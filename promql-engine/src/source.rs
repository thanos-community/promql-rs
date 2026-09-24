//! How a store hands series to the engine.
//!
//! [`SeriesSource`] mirrors Prometheus's `storage.Querier.Select`, which
//! every PromQL-on-something implementation has ended up writing. The
//! engine asks in PromQL terms — label matchers and a millisecond range —
//! and the store answers with a DataFusion [`ExecutionPlan`] in the
//! [canonical schema](crate::series). What happens inside `select` is the
//! store's business: a parquet scan, a vortex scan, a gRPC call, a
//! `GROUP BY` that nests samples into lists. This crate never looks.
//!
//! Three obligations come with the answer:
//!
//! 1. **Filter.** Every series matches every matcher; every sample lies
//!    inside the range.
//! 2. **Partition.** A row holds one chunk of one series' samples. A
//!    series may span any number of rows and batches, but its rows are
//!    consecutive and never cross a partition.
//! 3. **Order.** Series are sorted by label set in DataFusion's struct
//!    order: label fields by name, compared one at a time, an absent label
//!    as `""`. That is not Prometheus's `labels.Compare` (`{b="1"}`
//!    precedes `{a="1"}` here), because `labels ASC` is only a true
//!    declaration in the order DataFusion itself compares in. Within a
//!    series, rows ascend by first sample timestamp and samples by
//!    timestamp.
//!
//! The engine trusts the first and checks the other two, one label
//! comparison per row, in [`SeriesSetExec`]: an operator that closes a
//! series at the next label set would otherwise answer wrong without
//! noticing.
//!
//! [`SelectorTable`] then makes a `select` result look like an ordinary
//! table to DataFusion, so a selector is a `TableScan` leaf and everything
//! above it — `EXPLAIN`, the optimizer, later operators — is stock.

use std::cmp::Ordering;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{make_comparator, Array, AsArray, RecordBatch, StructArray};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{SchemaRef, TimestampMillisecondType};
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics,
};
use futures::{Stream, StreamExt};
use promql_parser::ast::LabelMatcher;

use crate::error::EngineError;
use crate::series::{self, LABELS, SAMPLES, TIMESTAMP};

/// What the engine wants from a scan, beyond the matchers: Prometheus's
/// `storage.SelectHints`, typed.
///
/// Only the range is binding. Both bounds are inclusive milliseconds and
/// already account for lookback, `offset`, `@` and range windows, so the
/// store does not need to know any PromQL to honour them. Everything else
/// is advisory: a store may use it to read less or to lay its output out
/// better, and may ignore it without changing the result.
///
/// Prometheus's `Limit` and `DisableTrimming` are left out: the first
/// serves its label-values API, the second its own chunk trimming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectHints {
    pub start_ms: i64,
    pub end_ms: i64,
    /// Step of the enclosing range query; `None` for an instant query. A
    /// store may read downsampled or step-aligned data.
    pub step_ms: Option<i64>,
    /// Window of a range selector such as `[5m]`; `None` for an instant
    /// selector. A store may prune chunks per window.
    pub range_ms: Option<i64>,
    /// The function or aggregation directly above the selector, by its
    /// PromQL name (`rate`, `sum`); `None` when there is none.
    /// `count_over_time` needs no values; `rate` needs whole windows.
    pub func: Option<String>,
    /// `by` of the aggregation directly above the selector. With
    /// `sum by (route)` only `route` decides the output, so a store may
    /// drop the other labels, and if it partitions its plan by the
    /// grouping labels the engine aggregates without a shuffle. Absent as
    /// soon as anything sits in between, as in `sum by (route)
    /// (rate(x[5m]))`, where the selector's own parent is `rate`.
    pub grouping: Option<Grouping>,
    /// Return only shard `index` of `count` of the matching series, for
    /// scanning the series space in parallel; `None` for all of them.
    pub shard: Option<Shard>,
}

impl SelectHints {
    /// Just the range; every advisory hint absent.
    pub fn range(start_ms: i64, end_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            step_ms: None,
            range_ms: None,
            func: None,
            grouping: None,
            shard: None,
        }
    }
}

/// `by (labels…)` or `without (labels…)` of an enclosing aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grouping {
    pub labels: Vec<String>,
    /// `true` for `by`, `false` for `without`. Carried for parity with
    /// upstream's `SelectHints.By`, which is likewise only ever set for
    /// `by`: `without` tells a store nothing it can act on.
    pub by: bool,
}

/// One of `count` equal parts of the series space, by series identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard {
    pub index: u64,
    pub count: u64,
}

/// A store of series, as the engine sees it.
#[async_trait]
pub trait SeriesSource: fmt::Debug + Send + Sync {
    /// Every series matching all of `matchers`, carrying only the samples
    /// within `hints`, as a plan whose schema passes [`series::validate`].
    ///
    /// `matchers` arrive verbatim from the parser, including `__name__`
    /// (synthesized from a bare metric name). An absent label compares as
    /// `""`, and regexes are anchored to the whole value; see
    /// [`crate::matcher`] for the reference semantics a store is held to.
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// One selector's scan, as a DataFusion table.
///
/// Built eagerly: `select` is called once at plan time and its result
/// held, because a [`TableProvider`] must report its schema before any
/// scan and the schema — the label names — is only known once the store
/// has found the matching series. That is also how Prometheus works: it
/// expands every selector's series before evaluating anything. A
/// consequence worth knowing is that the store's own planning happens
/// while the engine is planning, not while it is executing.
///
/// Public so that a store which implements [`SeriesSource`] can register
/// itself as a SQL table for debugging: `SELECT labels, samples FROM …`.
///
/// Nothing in this crate constructs one yet. The planner that stacks on
/// this PR is the first caller; it lives here because it is the other half
/// of what the trait promises — the shape a store returns, and how the
/// engine mounts that shape into a plan.
#[derive(Debug)]
pub struct SelectorTable {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl SelectorTable {
    /// Ask `source` for the selection and validate what comes back.
    pub async fn try_new(
        state: &dyn Session,
        source: &dyn SeriesSource,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> std::result::Result<Self, EngineError> {
        let plan = source.select(state, matchers, hints).await?;
        let schema = plan.schema();
        series::validate(&schema).map_err(EngineError::Schema)?;
        Ok(Self { plan, schema })
    }

    /// The label names this selection carries.
    pub fn label_names(&self) -> Vec<String> {
        series::label_names(&self.schema)
    }
}

#[async_trait]
impl TableProvider for SelectorTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(SeriesSetExec::new(Arc::clone(&self.plan)));
        let Some(cols) = projection else {
            return Ok(plan);
        };
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = cols
            .iter()
            .map(|&i| {
                let name = self.schema.field(i).name().clone();
                (
                    Arc::new(Column::new(&name, i)) as Arc<dyn PhysicalExpr>,
                    name,
                )
            })
            .collect();
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }
}

/// A store's plan, declared `labels ASC` and checked to be so: Prometheus's
/// `storage.SeriesSet`, the stream of series a `Select` returns.
///
/// The declaration is what lets an aggregate grouped by `labels` run in
/// `InputOrderMode::Sorted`, holding one open series per partition instead
/// of every series of the scan. A store cannot be trusted with that on its
/// word, because a wrong declaration does not fail, it splits a series in
/// two and answers twice. So every row, empty ones included, is compared
/// with the one before it in its partition: labels must not descend, which
/// also catches a closed series reappearing. An empty row still carries a
/// label set and still closes the series before it, even though it has
/// nothing to fold; skipping its label check would let DataFusion's grouped
/// aggregate close a series on a row this check never looked at. Only the
/// first-sample-timestamp check is skipped for an empty row, since it has
/// no first sample: within one label set the first sample timestamp of the
/// next non-empty row must not go backwards. Nothing is sorted or buffered
/// to repair a violation; that would be the whole-series concatenation
/// chunk rows exist to avoid. It is an [`EngineError::Source`] instead.
///
/// The check is per partition. Keeping a series inside one partition is
/// the plan's concern, not something a per-partition stream can see.
#[derive(Debug)]
pub struct SeriesSetExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SeriesSetExec {
    /// `input` must have a schema that passed [`series::validate`].
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = input.schema();
        let labels = Column::new(LABELS, schema.index_of(LABELS).expect("a canonical schema"));
        let ordering = [PhysicalSortExpr::new_default(Arc::new(labels))];
        let eq = EquivalenceProperties::new_with_orderings(schema, [ordering]);
        let properties = PlanProperties::clone(input.properties())
            .with_eq_properties(eq)
            .into();
        Self { input, properties }
    }
}

impl DisplayAs for SeriesSetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SeriesSetExec")
    }
}

impl ExecutionPlan for SeriesSetExec {
    fn name(&self) -> &str {
        "SeriesSetExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    /// Round-robin beneath this node would deal one series' rows to
    /// several partitions, each of which would then pass the check.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "SeriesSetExec takes exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let labels = input.schema().field_with_name(LABELS)?.data_type().clone();
        Ok(Box::pin(SeriesSetStream {
            input,
            converter: RowConverter::new(vec![SortField::new(labels)])?,
            prev: None,
            prev_first_t: None,
        }))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }
}

struct SeriesSetStream {
    input: SendableRecordBatchStream,
    /// One converter for the whole stream, so the first row of a batch
    /// compares with the last of the batch before.
    converter: RowConverter,
    prev: Option<OwnedRow>,
    /// The last non-empty row's first sample timestamp within the current
    /// series, `None` until one has been seen.
    prev_first_t: Option<i64>,
}

impl SeriesSetStream {
    /// Adjacent rows of one batch are compared on the label columns as
    /// they are. Converting every row, as the boundary row is, copies each
    /// label set into row format and allocates one per series; with a
    /// sample walk that could not vectorise, that was the 8 to 15% of a
    /// 10,000-series query that switching the check off saved.
    fn check(&mut self, batch: &RecordBatch) -> Result<()> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(());
        }
        let labels = batch.column_by_name(LABELS).expect("canonical");
        let samples = batch
            .column_by_name(SAMPLES)
            .expect("canonical")
            .as_list::<i32>();
        let timestamps = samples
            .values()
            .as_struct()
            .column_by_name(TIMESTAMP)
            .expect("canonical")
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let offsets = samples.value_offsets();
        let cmp = make_comparator(labels, labels, SortOptions::default())?;
        let first = self.converter.convert_columns(&[labels.slice(0, 1)])?;
        for r in 0..n {
            let (start, end) = (offsets[r] as usize, offsets[r + 1] as usize);
            // Whole rows at a time, so the scan vectorises; only a row that
            // fails is walked again for the message.
            if !timestamps[start..end].is_sorted() {
                let i = (start + 1..end)
                    .find(|&i| timestamps[i] < timestamps[i - 1])
                    .expect("an unsorted row has a descent");
                return Err(source_error(format!(
                    "series {}: sample at {} follows one at {}; samples within a row \
                     must ascend by timestamp",
                    format_labels(labels.as_struct(), r),
                    timestamps[i],
                    timestamps[i - 1],
                )));
            }
            let order = match r {
                0 => self.prev.as_ref().map(|p| p.row().cmp(&first.row(0))),
                _ => Some(cmp(r - 1, r)),
            };
            match order {
                Some(Ordering::Greater) => {
                    let prev = match r {
                        0 => self.prev_labels()?,
                        _ => format_labels(labels.as_struct(), r - 1),
                    };
                    return Err(source_error(format!(
                        "series {} arrived after {prev}; series must be sorted by labels in \
                         struct order, and the rows of a series consecutive",
                        format_labels(labels.as_struct(), r),
                    )));
                }
                Some(Ordering::Equal) => {
                    if start != end {
                        let first_t = timestamps[start];
                        if let Some(prev_first_t) = self.prev_first_t {
                            if first_t < prev_first_t {
                                return Err(source_error(format!(
                                    "series {}: a row starting at {first_t} follows one \
                                     starting at {prev_first_t}; the rows of a series must \
                                     ascend by first sample timestamp",
                                    format_labels(labels.as_struct(), r),
                                )));
                            }
                        }
                        self.prev_first_t = Some(first_t);
                    }
                }
                _ => self.prev_first_t = (start != end).then(|| timestamps[start]),
            }
        }
        let last = self.converter.convert_columns(&[labels.slice(n - 1, 1)])?;
        self.prev = Some(last.row(0).owned());
        Ok(())
    }

    /// The previous series' labels, decoded only for an error message.
    fn prev_labels(&self) -> Result<String> {
        let prev = self.prev.as_ref().expect("compared against");
        let columns = self.converter.convert_rows(std::iter::once(prev.row()))?;
        Ok(format_labels(columns[0].as_struct(), 0))
    }
}

impl Stream for SeriesSetStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.check(&batch).map(|()| batch))),
            other => other,
        }
    }
}

impl RecordBatchStream for SeriesSetStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

pub(crate) fn source_error(msg: String) -> DataFusionError {
    DataFusionError::External(Box::new(EngineError::Source(msg)))
}

/// `{name="value", …}` without the absent labels, as Prometheus prints a
/// label set.
fn format_labels(labels: &StructArray, row: usize) -> String {
    let pairs: Vec<String> = labels
        .fields()
        .iter()
        .zip(labels.columns())
        .filter_map(|(f, c)| {
            let v = c.as_string_view().value(row);
            (!v.is_empty()).then(|| format!("{}={v:?}", f.name()))
        })
        .collect();
    format!("{{{}}}", pairs.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, Float64Array, ListArray, RecordBatch, StringViewArray, StructArray,
        TimestampMillisecondArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use crate::series::{sample_fields, sample_item, schema};

    /// `(labels, timestamps)`; a label missing from a row is `""`, as in
    /// the canonical shape.
    type Row<'a> = (&'a [(&'a str, &'a str)], &'a [i64]);

    /// One batch of `rows` over the label `names`.
    fn batch(names: &[&str], rows: &[Row]) -> RecordBatch {
        let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        let schema = schema(&names);
        let columns: Vec<ArrayRef> = names
            .iter()
            .map(|n| {
                let values = rows
                    .iter()
                    .map(|(labels, _)| labels.iter().find(|(k, _)| k == n).map_or("", |(_, v)| *v));
                Arc::new(StringViewArray::from_iter_values(values)) as ArrayRef
            })
            .collect();
        let labels = match schema.field(0).data_type() {
            datafusion::arrow::datatypes::DataType::Struct(f) if f.is_empty() => {
                StructArray::new_empty_fields(rows.len(), None)
            }
            datafusion::arrow::datatypes::DataType::Struct(f) => {
                StructArray::new(f.clone(), columns, None)
            }
            _ => unreachable!("canonical"),
        };
        let ts: Vec<i64> = rows.iter().flat_map(|(_, t)| t.iter().copied()).collect();
        let entries = StructArray::new(
            sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts.clone())),
                Arc::new(Float64Array::from(vec![1.0; ts.len()])),
            ],
            None,
        );
        let offsets = OffsetBuffer::from_lengths(rows.iter().map(|(_, t)| t.len()));
        let samples = ListArray::new(sample_item(), offsets, Arc::new(entries), None);
        RecordBatch::try_new(schema, vec![Arc::new(labels), Arc::new(samples)]).unwrap()
    }

    fn series_set(partitions: Vec<Vec<RecordBatch>>) -> Arc<SeriesSetExec> {
        let schema = partitions
            .iter()
            .flatten()
            .next()
            .expect("at least one batch")
            .schema();
        let child = MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap();
        Arc::new(SeriesSetExec::new(child))
    }

    async fn run(
        partitions: Vec<Vec<RecordBatch>>,
    ) -> std::result::Result<Vec<RecordBatch>, EngineError> {
        let ctx = SessionContext::new();
        Ok(collect(series_set(partitions), ctx.task_ctx()).await?)
    }

    async fn refused(partitions: Vec<Vec<RecordBatch>>) -> String {
        match run(partitions).await {
            Err(EngineError::Source(msg)) => msg,
            other => panic!("expected EngineError::Source, got {other:?}"),
        }
    }

    const A: &[(&str, &str)] = &[("pod", "a")];
    const B: &[(&str, &str)] = &[("pod", "b")];
    const C: &[(&str, &str)] = &[("pod", "c")];

    #[tokio::test]
    async fn descending_labels_are_refused() {
        let msg = refused(vec![vec![batch(&["pod"], &[(B, &[1]), (A, &[1])])]]).await;
        assert!(msg.contains(r#"pod="a""#), "{msg}");
    }

    #[tokio::test]
    async fn a_closed_series_reappearing_is_refused() {
        let msg = refused(vec![vec![
            batch(&["pod"], &[(A, &[1, 2])]),
            batch(&["pod"], &[(B, &[1])]),
            batch(&["pod"], &[(A, &[3])]),
        ]])
        .await;
        assert!(msg.contains(r#"pod="a""#), "{msg}");
    }

    #[tokio::test]
    async fn a_first_timestamp_going_backwards_is_refused() {
        let msg = refused(vec![vec![
            batch(&["pod"], &[(A, &[100, 200])]),
            batch(&["pod"], &[(A, &[50])]),
        ]])
        .await;
        assert!(msg.contains(r#"pod="a""#) && msg.contains("50"), "{msg}");
    }

    #[tokio::test]
    async fn a_series_split_across_three_batches_passes_untouched() {
        let input = vec![
            batch(&["pod"], &[(A, &[1, 2])]),
            batch(&["pod"], &[(A, &[3])]),
            batch(&["pod"], &[(A, &[3, 4]), (B, &[1])]),
        ];
        let out = run(vec![input.clone()]).await.unwrap();
        assert_eq!(out, input);
    }

    /// An empty row has no first timestamp to check, so a run of empty
    /// rows inside one series never trips the "ascend by first sample
    /// timestamp" check.
    #[tokio::test]
    async fn empty_rows_skip_only_the_timestamp_check() {
        run(vec![vec![batch(
            &["pod"],
            &[(A, &[100, 200]), (A, &[]), (A, &[300])],
        )]])
        .await
        .unwrap();
    }

    /// An empty row still carries a label set and still closes the series
    /// before it: `c{}` closes `b`, so `b` reappearing in the next batch is
    /// a closed series reappearing, same as if `c{}` had samples.
    #[tokio::test]
    async fn an_empty_row_closes_the_series_before_it() {
        let msg = refused(vec![vec![
            batch(&["pod"], &[(B, &[0, 30_000]), (C, &[])]),
            batch(&["pod"], &[(B, &[60_000, 90_000])]),
        ]])
        .await;
        assert!(msg.contains(r#"pod="b""#), "{msg}");
    }

    /// The doc promises samples ascend within a row is checked, not just
    /// assumed; a row whose samples go backwards must be refused.
    #[tokio::test]
    async fn samples_out_of_order_within_a_row_are_refused() {
        let msg = refused(vec![vec![batch(&["pod"], &[(A, &[200, 100])])]]).await;
        assert!(msg.contains(r#"pod="a""#), "{msg}");
    }

    /// The check runs a row at a time, not the batch's samples as one
    /// sequence: a descent at a row boundary is two rows, one deep inside a
    /// long row of a later series is an error.
    #[tokio::test]
    async fn a_descent_deep_in_a_later_row_is_refused() {
        let long: Vec<i64> = (0..40).map(|i| if i == 37 { 0 } else { i * 10 }).collect();
        let msg = refused(vec![vec![batch(&["pod"], &[(A, &[500, 600]), (B, &long)])]]).await;
        assert!(
            msg.contains(r#"pod="b""#) && msg.contains("follows one at 360"),
            "{msg}"
        );
    }

    /// A batch with no rows carries no series and must not reset what the
    /// next batch is compared against.
    #[tokio::test]
    async fn an_empty_batch_between_series_changes_nothing() {
        run(vec![vec![
            batch(&["pod"], &[(A, &[1])]),
            batch(&["pod"], &[]),
            batch(&["pod"], &[(A, &[2]), (B, &[1])]),
        ]])
        .await
        .unwrap();
        let msg = refused(vec![vec![
            batch(&["pod"], &[(B, &[1])]),
            batch(&["pod"], &[]),
            batch(&["pod"], &[(A, &[1])]),
        ]])
        .await;
        assert!(
            msg.contains(r#"pod="a""#) && msg.contains(r#"pod="b""#),
            "{msg}"
        );
    }

    /// The first row of a batch is compared with the last of the one
    /// before, not with the first series of the stream.
    #[tokio::test]
    async fn a_batch_boundary_compares_against_the_last_series() {
        run(vec![vec![
            batch(&["pod"], &[(A, &[1]), (B, &[1])]),
            batch(&["pod"], &[(B, &[2]), (C, &[1])]),
        ]])
        .await
        .unwrap();
        let msg = refused(vec![vec![
            batch(&["pod"], &[(A, &[1]), (C, &[1])]),
            batch(&["pod"], &[(B, &[1])]),
        ]])
        .await;
        assert!(
            msg.contains(r#"pod="b""#) && msg.contains(r#"pod="c""#),
            "{msg}"
        );
    }

    /// `labels ASC` is DataFusion's struct order: fields by name, compared
    /// one at a time, an absent label as `""`. `{b="1"}` is `("", "1")`
    /// and sorts before `{a="1"}`, `("1", "")`, the reverse of Prometheus's
    /// `labels.Compare`; the declaration can only be true in the order
    /// DataFusion compares in.
    #[tokio::test]
    async fn sparse_label_sets_pass_in_struct_order() {
        run(vec![vec![batch(
            &["a", "b"],
            &[
                (&[("b", "1")], &[1]),
                (&[("a", "1")], &[1]),
                (&[("a", "1"), ("b", "1")], &[1]),
            ],
        )]])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn prometheus_order_is_refused_where_it_differs() {
        refused(vec![vec![batch(
            &["a", "b"],
            &[(&[("a", "1")], &[1]), (&[("b", "1")], &[1])],
        )]])
        .await;
    }

    #[tokio::test]
    async fn a_label_set_without_labels_is_one_series() {
        run(vec![vec![
            batch(&[], &[(&[], &[1, 2])]),
            batch(&[], &[(&[], &[3])]),
        ]])
        .await
        .unwrap();
        refused(vec![vec![batch(&[], &[(&[], &[2]), (&[], &[1])])]]).await;
    }

    /// Each partition is its own stream of series; the same label set in
    /// two partitions is the plan's problem, not the order check's.
    #[tokio::test]
    async fn partitions_are_checked_independently() {
        let out = run(vec![
            vec![batch(&["pod"], &[(B, &[1])])],
            vec![batch(&["pod"], &[(A, &[1])])],
        ])
        .await
        .unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn declares_labels_ascending_and_keeps_the_partitioning() {
        let exec = series_set(vec![
            vec![batch(&["pod"], &[(A, &[1])])],
            vec![batch(&["pod"], &[(B, &[1])])],
        ]);
        assert_eq!(exec.properties().output_partitioning().partition_count(), 2);
        let ordering = exec.properties().output_ordering().expect("an ordering");
        assert_eq!(ordering.to_string(), "labels@0 ASC");
        assert_eq!(exec.maintains_input_order(), [true]);
    }
}
