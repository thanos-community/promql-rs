//! `dedup.NewSeriesSet` as a DataFusion plan node and its operator.
//!
//! [`DedupNode`] sits directly above a selector's scan and keeps its
//! schema; [`DedupPlanner`] turns it into [`DedupExec`], which buffers the
//! scan's rows, merges those equal but for the replica labels, and emits
//! one batch in which the replica labels read `""`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{internal_err, DFSchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::{stream, TryStreamExt};
use promql_engine::series::{self, Series};

use super::{chain_samples, dedup_samples, DeduplicationFunc};

const NAME: &str = "ThanosDedup";

fn describe(
    f: &mut fmt::Formatter<'_>,
    replica_labels: &[String],
    func: DeduplicationFunc,
    is_counter: bool,
) -> fmt::Result {
    write!(
        f,
        "{NAME}: replica_labels={replica_labels:?}, func={}, counter={is_counter}",
        func.as_str()
    )
}

/// The plan node: merge the input's rows that are equal but for
/// `replica_labels`. The schema is the scan's, and the replica labels of
/// a merged row read `""`.
///
/// The schema is pinned when the node is made rather than read off the
/// input: DataFusion pushes struct field accesses such as
/// `get_field(labels, 'job')` towards the leaves through every node whose
/// output, rebuilt around the pushed projection, still shows the new
/// column. A node merging rows cannot pass such a column through, so it
/// owns its schema, as an `Aggregate` does, and the projection stays above
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DedupNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    /// The replica labels the input actually carries.
    replica_labels: Vec<String>,
    func: DeduplicationFunc,
    /// Whether the function above the selector is a counter function.
    is_counter: bool,
}

impl DedupNode {
    pub(crate) fn new(
        input: LogicalPlan,
        replica_labels: Vec<String>,
        func: DeduplicationFunc,
        is_counter: bool,
    ) -> Self {
        Self {
            schema: Arc::clone(input.schema()),
            input,
            replica_labels,
            func,
            is_counter,
        }
    }
}

/// `DFSchema` has no order; the schema follows from the input anyway.
impl PartialOrd for DedupNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            &self.input,
            &self.replica_labels,
            self.func,
            self.is_counter,
        )
            .partial_cmp(&(
                &other.input,
                &other.replica_labels,
                other.func,
                other.is_counter,
            ))
    }
}

impl UserDefinedLogicalNodeCore for DedupNode {
    fn name(&self) -> &str {
        NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        describe(f, &self.replica_labels, self.func, self.is_counter)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return internal_err!(
                "{NAME} takes one input and no expressions, got {} and {}",
                inputs.len(),
                exprs.len()
            );
        }
        Ok(Self {
            input: inputs.swap_remove(0),
            ..self.clone()
        })
    }
}

/// Plans a [`DedupNode`] as a [`DedupExec`]; for
/// `Engine::with_extension_planners`.
#[derive(Debug, Default)]
pub struct DedupPlanner;

#[async_trait]
impl ExtensionPlanner for DedupPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<DedupNode>() else {
            return Ok(None);
        };
        let [input] = physical_inputs else {
            return internal_err!("{NAME} takes one input, got {}", physical_inputs.len());
        };
        Ok(Some(Arc::new(DedupExec::new(
            Arc::clone(input),
            node.replica_labels.clone(),
            node.func,
            node.is_counter,
        ))))
    }
}

/// The operator. One partition: every row of a series has to be seen
/// before any is merged, and a selector's rows are one batch anyway.
#[derive(Debug)]
pub struct DedupExec {
    input: Arc<dyn ExecutionPlan>,
    replica_labels: Vec<String>,
    func: DeduplicationFunc,
    is_counter: bool,
    properties: Arc<PlanProperties>,
}

impl DedupExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        replica_labels: Vec<String>,
        func: DeduplicationFunc,
        is_counter: bool,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            replica_labels,
            func,
            is_counter,
            properties,
        }
    }
}

impl DisplayAs for DedupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        describe(f, &self.replica_labels, self.func, self.is_counter)
    }
}

impl ExecutionPlan for DedupExec {
    fn name(&self) -> &str {
        NAME
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("{NAME} takes one input, got {}", children.len());
        };
        Ok(Arc::new(Self::new(
            Arc::clone(input),
            self.replica_labels.clone(),
            self.func,
            self.is_counter,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("{NAME} has one partition, partition {partition} was asked for");
        }
        let input = self.input.execute(0, context)?;
        let schema = self.schema();
        let out = Arc::clone(&schema);
        let replica_labels = self.replica_labels.clone();
        let (func, is_counter) = (self.func, self.is_counter);
        let merged = async move {
            let batches: Vec<RecordBatch> = input.try_collect().await?;
            merge(&out, &batches, &replica_labels, func, is_counter)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(merged),
        )))
    }
}

/// The samples of one replica, drained.
fn samples_of(series: &Series) -> Vec<(i64, f64)> {
    series
        .timestamps()
        .iter()
        .copied()
        .zip(series.values().iter().copied())
        .collect()
}

/// One series' replicas: the values of the replica labels, for a fixed
/// order, and the row.
type Replicas<'a> = Vec<(Vec<&'a str>, &'a Series)>;

/// `dedupSeriesSet` drained: the rows of `batches` that are equal but for
/// `replica_labels`, merged into one row each, in `schema`. The rows of
/// one series are merged in the order of their first sample, so the
/// replica that scraped first leads, as Go's pseudo-replicas are ordered
/// by their first chunk; ties go by the replica labels' values.
fn merge(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    replica_labels: &[String],
    func: DeduplicationFunc,
    is_counter: bool,
) -> Result<RecordBatch> {
    let rows = series::decode(batches).map_err(DataFusionError::Internal)?;
    let is_replica = |name: &str| replica_labels.iter().any(|l| l == name);
    let mut groups: BTreeMap<Vec<(&str, &str)>, Replicas> = BTreeMap::new();
    for row in &rows {
        let kept: Vec<(&str, &str)> = row.labels().filter(|(n, _)| !is_replica(n)).collect();
        let replica: Vec<&str> = replica_labels.iter().map(|l| row.label(l)).collect();
        groups.entry(kept).or_default().push((replica, row));
    }

    let mut merged = Vec::with_capacity(groups.len());
    for (labels, mut replicas) in groups {
        replicas.sort_by(|a, b| {
            let first = |s: &Series| s.timestamps().first().copied();
            first(a.1).cmp(&first(b.1)).then_with(|| a.0.cmp(&b.0))
        });
        let samples = match replicas.as_slice() {
            [(_, only)] => samples_of(only),
            _ => {
                let lists = replicas.iter().map(|(_, s)| samples_of(s)).collect();
                match func {
                    DeduplicationFunc::Penalty => dedup_samples(lists, is_counter),
                    DeduplicationFunc::Chain => chain_samples(lists),
                }
            }
        };
        let (ts, vs): (Vec<i64>, Vec<f64>) = samples.into_iter().unzip();
        merged.push(Series::new(&labels, ts, vs).map_err(DataFusionError::Internal)?);
    }

    // `encode` writes `""` for the replica labels the merged rows lack and
    // builds the very schema the input has; adopt the input's so the
    // physical schema is the one the planner checked.
    let names = series::label_names(schema);
    let batch = series::encode(&names, &merged).map_err(DataFusionError::Internal)?;
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        batch.columns().to_vec(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> Series {
        let (ts, vs): (Vec<i64>, Vec<f64>) = samples.iter().copied().unzip();
        Series::new(labels, ts, vs).unwrap()
    }

    fn batch(rows: &[Series]) -> RecordBatch {
        series::encode(&series::label_names_of(rows), rows).unwrap()
    }

    fn merged(
        rows: &[Series],
        replica_labels: &[&str],
        func: DeduplicationFunc,
        is_counter: bool,
    ) -> (SchemaRef, RecordBatch) {
        let input = batch(rows);
        let labels: Vec<String> = replica_labels.iter().map(|l| l.to_string()).collect();
        let out = merge(
            &input.schema(),
            std::slice::from_ref(&input),
            &labels,
            func,
            is_counter,
        )
        .unwrap();
        (input.schema(), out)
    }

    #[test]
    fn replicas_of_one_series_become_one_row_in_the_input_schema() {
        let rows = [
            row(
                &[("__name__", "up"), ("job", "x"), ("replica", "a")],
                &[(0, 1.0), (10_000, 2.0), (20_000, 3.0)],
            ),
            row(
                &[("__name__", "up"), ("job", "x"), ("replica", "b")],
                &[(1_000, 10.0), (11_000, 20.0), (21_000, 30.0)],
            ),
            // The compacted, replica-less copy is a replica too.
            row(
                &[("__name__", "up"), ("job", "x")],
                &[(0, 100.0), (10_000, 200.0), (20_000, 300.0)],
            ),
            row(
                &[("__name__", "up"), ("job", "y"), ("replica", "a")],
                &[(0, 7.0), (10_000, 7.0)],
            ),
        ];
        let (schema, out) = merged(&rows, &["replica"], DeduplicationFunc::Penalty, false);
        assert!(
            Arc::ptr_eq(&schema, &out.schema()),
            "the schema is the input's"
        );

        let series = series::decode(&[out]).unwrap();
        assert_eq!(series.len(), 2, "{series:?}");
        // Both a and the compacted copy have their first sample at 0; the
        // absent replica label sorts first, so the compacted copy leads,
        // and neither a nor b ever gets a turn.
        assert_eq!(
            series[0].labels().collect::<Vec<_>>(),
            [("__name__", "up"), ("job", "x")]
        );
        assert_eq!(series[0].label("replica"), "");
        assert_eq!(series[0].values(), &[100.0, 200.0, 300.0]);
        // A series with one replica is untouched but for the label.
        assert_eq!(
            series[1].labels().collect::<Vec<_>>(),
            [("__name__", "up"), ("job", "y")]
        );
        assert_eq!(series[1].values(), &[7.0, 7.0]);
    }

    #[test]
    fn the_function_above_decides_the_counter_lift() {
        let rows = [
            row(
                &[("__name__", "c"), ("replica", "a")],
                &[(0, 100.0), (10_000, 110.0)],
            ),
            row(&[("__name__", "c"), ("replica", "b")], &[(35_000, 105.0)]),
        ];
        let (_, out) = merged(&rows, &["replica"], DeduplicationFunc::Penalty, true);
        let series = series::decode(&[out]).unwrap();
        assert_eq!(series[0].values(), &[100.0, 110.0, 110.0]);

        let (_, out) = merged(&rows, &["replica"], DeduplicationFunc::Penalty, false);
        let series = series::decode(&[out]).unwrap();
        assert_eq!(series[0].values(), &[100.0, 110.0, 105.0]);
    }

    #[test]
    fn chain_unions_the_replicas() {
        let rows = [
            row(
                &[("__name__", "g"), ("replica", "a")],
                &[(0, 1.0), (10, 1.0)],
            ),
            row(
                &[("__name__", "g"), ("replica", "b")],
                &[(10, 2.0), (20, 2.0)],
            ),
        ];
        let (_, out) = merged(&rows, &["replica"], DeduplicationFunc::Chain, false);
        let series = series::decode(&[out]).unwrap();
        assert_eq!(series[0].timestamps(), &[0, 10, 20]);
        assert_eq!(series[0].values(), &[1.0, 1.0, 2.0]);
    }

    #[test]
    fn several_replica_labels_and_nothing_else() {
        let rows = [
            row(&[("replica", "a"), ("shard", "1")], &[(0, 1.0)]),
            row(&[("replica", "b"), ("shard", "2")], &[(5_000, 2.0)]),
        ];
        let (_, out) = merged(
            &rows,
            &["replica", "shard"],
            DeduplicationFunc::Penalty,
            false,
        );
        let series = series::decode(&[out]).unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].labels().count(), 0);
        assert_eq!(series[0].timestamps(), &[0]);
    }
}
