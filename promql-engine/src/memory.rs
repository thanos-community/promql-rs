//! An in-memory [`SeriesSource`], and the reference implementation of a
//! store's three obligations.
//!
//! It exists for tests, the conformance suite seeds it from the corpus's
//! `load` blocks, but it is also the executable statement of what a real
//! store has to do: apply the matchers with [`crate::matcher`]'s
//! semantics, keep only samples inside the range, hand over each series'
//! rows consecutively, series in struct order and samples in timestamp
//! order, in the canonical schema. A
//! store implementer who wants to know "what exactly am I promising" can
//! read `select` below.
//!
//! The shape of that `select` is the part worth copying, not the fact
//! that the data happens to sit in memory: a mask per matcher over a
//! whole label column, one `filter`, one pass that copies the samples
//! inside the range, and nothing that walks a row. A store reading
//! Parquet or answering over gRPC has the same three steps with its own
//! scan underneath.
//!
//! Only the first of those steps is this module's: the last two are
//! [`crate::series::drop_unused_labels`] and [`crate::series::clip`],
//! which live with the shape they operate on so every store gets them
//! rather than writing them again.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, AsArray, ListArray, RecordBatch, UInt32Array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{filter_record_batch, take_record_batch};
use datafusion::arrow::datatypes::{SchemaRef, TimestampMillisecondType};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use promql_parser::ast::{LabelMatcher, SeriesDescription};

use crate::matcher::{mask_all, CompiledMatcher};
#[cfg(test)]
use crate::series::decode;
use crate::series::{
    clip, drop_unused_labels, encode, label_names_of, sample_item, Series, LABELS, SAMPLES,
    TIMESTAMP,
};
use crate::source::{SelectHints, SeriesSource};

#[derive(Debug)]
pub struct MemorySeriesSource {
    /// Sorted by labels in struct order, the order `SeriesSetExec`
    /// declares, so a selection is sorted by construction.
    batch: RecordBatch,
    /// Test mode: see [`Self::chunked`].
    chunk_ms: Option<i64>,
    /// Test mode: see [`Self::partitions`].
    partitions: usize,
}

impl Default for MemorySeriesSource {
    fn default() -> Self {
        Self::unique(Vec::new())
    }
}

impl MemorySeriesSource {
    /// Encode these series into the one batch every query is answered
    /// from. Each [`Series`] was checked when it was built, so the only
    /// thing left to reject is two of them sharing a label set: that is
    /// one series split in two, and it is caught here rather than on
    /// every query.
    pub fn try_new(series: Vec<Series>) -> std::result::Result<Self, String> {
        let batch = encode(&label_names_of(&series), &series)?;
        Ok(Self {
            batch: sort_by_labels(&batch).map_err(|e| e.to_string())?,
            chunk_ms: None,
            partitions: 1,
        })
    }

    /// Test mode: hand every selected series over as consecutive rows of
    /// at most `chunk_ms` span, each its own single-row `RecordBatch`, the
    /// way a chunked store does. Series order and time order are kept.
    /// `0` gives one sample per row.
    pub fn chunked(mut self, chunk_ms: i64) -> Self {
        self.chunk_ms = Some(chunk_ms);
        self
    }

    /// Test mode: spread the selected series over `n` partitions by a hash
    /// of their label set, each series whole in one partition and each
    /// partition in struct order, the way a store scanning shards in
    /// parallel does. Combines with [`Self::chunked`].
    pub fn partitions(mut self, n: usize) -> Self {
        assert!(n > 0, "a plan has at least one partition");
        self.partitions = n;
        self
    }

    /// The same, for callers that already merged by label set. They key
    /// that merge the way `Series::new` normalizes a label set, dropping
    /// `""` values before comparing, so no two entries can collapse into
    /// one set here. What remains is `i32` offset overflow, which no merge
    /// could have caused, so the panic surfaces a broken caller rather
    /// than a reachable condition.
    fn unique(series: Vec<Series>) -> Self {
        Self::try_new(series).expect("callers merge by Series::new's normalized label set")
    }

    /// Seed from promqltest series descriptions the way upstream's
    /// `load` does: value `i` sits at `i * interval` from the epoch, an
    /// omitted value (`_`) emits no sample, and `stale` is already a
    /// StaleNaN payload courtesy of the parser.
    ///
    /// The corpus repeats lines on purpose, so lines with equal labels
    /// merge, the later one winning any timestamp both define. That is
    /// obligation 2, partition, applied at load time, and it is what
    /// keeps [`encode`] from seeing one label set twice.
    pub fn from_descriptions(series: &[SeriesDescription], interval_secs: f64) -> Self {
        let interval_ms = (interval_secs * 1000.0).round() as i64;
        let mut merged: BTreeMap<Vec<(&str, &str)>, BTreeMap<i64, f64>> = BTreeMap::new();
        for sd in series {
            // The parser keeps a description's labels as written, a name
            // possibly repeated; sorted by name, the last value wins. The
            // key also drops `""` values because `Series::new` does:
            // otherwise `x{env=""}` and `x` key differently here and then
            // collide once built, which `unique` promises cannot happen.
            let mut pairs: Vec<(&str, &str)> = sd
                .labels
                .iter()
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .filter(|(_, v)| !v.is_empty())
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            let mut labels: Vec<(&str, &str)> = Vec::with_capacity(pairs.len());
            for p in pairs {
                match labels.last_mut() {
                    Some(last) if last.0 == p.0 => last.1 = p.1,
                    _ => labels.push(p),
                }
            }
            let samples = merged.entry(labels).or_default();
            for (i, v) in sd.values.iter().enumerate().filter(|(_, v)| !v.omitted) {
                samples.insert(i as i64 * interval_ms, v.value);
            }
        }
        let stored = merged
            .into_iter()
            .map(|(labels, samples)| {
                let (timestamps, values) = samples.into_iter().unzip();
                Series::new(&labels, timestamps, values)
                    .expect("a sorted map yields ascending timestamps and unique names")
            })
            .collect();
        // The merge above keyed on the label set, so no two survive equal.
        Self::unique(stored)
    }

    /// The stored series, read back out of the batch.
    ///
    /// Test-only: [`decode`] drops zero-sample rows, which is right for a
    /// query result but would hide a held series here, so this must never
    /// grow a non-test caller without a row iterator that keeps them.
    #[cfg(test)]
    fn series(&self) -> Vec<Series> {
        decode(std::slice::from_ref(&self.batch)).expect("encoded here")
    }
}

#[async_trait]
impl SeriesSource for MemorySeriesSource {
    async fn select(
        &self,
        _state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let compiled: Vec<CompiledMatcher> = matchers
            .iter()
            .map(CompiledMatcher::compile)
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Obligation 1, filter.
        let labels = self.batch.column_by_name(LABELS).expect("canonical");
        let mask = mask_all(&compiled, labels.as_struct())?;
        let selected = filter_record_batch(&self.batch, &mask)?;
        let selected = drop_unused_labels(&selected).map_err(DataFusionError::Execution)?;
        let selected =
            clip(&selected, hints.start_ms, hints.end_ms).map_err(DataFusionError::Execution)?;

        // Obligations 2 and 3, partition and order, come for free: the
        // stored batch already holds one row per series in struct order
        // with its samples ascending, and neither step here, nor the
        // partitioning below, reorders anything within a partition.
        let schema = selected.schema();
        let partitions = partition_by_labels(&selected, self.partitions)?
            .iter()
            .map(|part| match self.chunk_ms {
                Some(chunk_ms) => {
                    split_into_chunks(part, chunk_ms).map_err(DataFusionError::Execution)
                }
                None => Ok(vec![part.clone()]),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(MemorySourceConfig::try_new_exec(&partitions, schema, None)?)
    }
}

/// `batch`'s rows in struct order of their labels, the order DataFusion
/// compares `labels` in, which is what `SeriesSetExec` declares and
/// checks. It differs from the `(name, value)` order the rows may have
/// been merged in, so it cannot be skipped for series that "look" sorted.
fn sort_by_labels(batch: &RecordBatch) -> Result<RecordBatch> {
    let labels = batch.column_by_name(LABELS).expect("canonical");
    let converter = RowConverter::new(vec![SortField::new(labels.data_type().clone())])?;
    let rows = converter.convert_columns(std::slice::from_ref(labels))?;
    let mut order: Vec<u32> = (0..batch.num_rows() as u32).collect();
    order.sort_unstable_by_key(|&i| rows.row(i as usize));
    Ok(take_record_batch(batch, &UInt32Array::from(order))?)
}

/// `n` batches holding `batch`'s rows by a hash of their label set, each
/// keeping `batch`'s row order.
fn partition_by_labels(batch: &RecordBatch, n: usize) -> Result<Vec<RecordBatch>> {
    if n == 1 {
        return Ok(vec![batch.clone()]);
    }
    let labels = batch.column_by_name(LABELS).expect("canonical");
    let converter = RowConverter::new(vec![SortField::new(labels.data_type().clone())])?;
    let rows = converter.convert_columns(std::slice::from_ref(labels))?;
    let mut indices: Vec<Vec<u32>> = vec![Vec::new(); n];
    for (i, row) in rows.iter().enumerate() {
        let mut h = DefaultHasher::new();
        row.as_ref().hash(&mut h);
        indices[(h.finish() % n as u64) as usize].push(i as u32);
    }
    indices
        .into_iter()
        .map(|idx| Ok(take_record_batch(batch, &UInt32Array::from(idx))?))
        .collect()
}

/// Test mode: turn one canonical batch (one row per series) into
/// several, each holding one series' samples split into consecutive
/// chunks of at most `chunk_ms` span. Series order is preserved, and
/// within a series so is time order, so the result is what a chunked
/// `SeriesSource` would hand over for the same selection.
fn split_into_chunks(
    batch: &RecordBatch,
    chunk_ms: i64,
) -> std::result::Result<Vec<RecordBatch>, String> {
    let schema: SchemaRef = batch.schema();
    let labels = batch.column_by_name(LABELS).expect("canonical").as_struct();
    let samples = batch
        .column_by_name(SAMPLES)
        .expect("canonical")
        .as_list::<i32>();

    let mut out = Vec::new();
    for row in 0..batch.num_rows() {
        let label_row: ArrayRef = Arc::new(labels.slice(row, 1));
        let row_samples: ArrayRef = samples.value(row);
        let timestamps: Vec<i64> = row_samples
            .as_struct()
            .column_by_name(TIMESTAMP)
            .expect("canonical")
            .as_primitive::<TimestampMillisecondType>()
            .values()
            .to_vec();

        if timestamps.is_empty() {
            out.push(chunk_batch(&schema, label_row, row_samples.slice(0, 0))?);
            continue;
        }

        let mut start = 0usize;
        while start < timestamps.len() {
            let mut end = start + 1;
            while end < timestamps.len() && timestamps[end] - timestamps[start] <= chunk_ms {
                end += 1;
            }
            out.push(chunk_batch(
                &schema,
                label_row.clone(),
                row_samples.slice(start, end - start),
            )?);
            start = end;
        }
    }
    Ok(out)
}

/// One single-row batch: `label_row` and `chunk_samples` (already the
/// list's child slice) wrapped back into the canonical list-of-structs
/// column.
fn chunk_batch(
    schema: &SchemaRef,
    label_row: ArrayRef,
    chunk_samples: ArrayRef,
) -> std::result::Result<RecordBatch, String> {
    let len = i32::try_from(chunk_samples.len()).map_err(|_| "chunk longer than i32::MAX")?;
    let list = ListArray::new(
        sample_item(),
        OffsetBuffer::new(vec![0i32, len].into()),
        chunk_samples,
        None,
    );
    RecordBatch::try_new(schema.clone(), vec![label_row, Arc::new(list)]).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::{collect, ExecutionPlanProperties};
    use datafusion::prelude::SessionContext;
    use promql_parser::ast::MatchOp;
    use promql_parser::posrange::PositionRange;

    fn matcher(name: &str, op: MatchOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            name: name.into(),
            op,
            value: value.into(),
            pos_range: PositionRange::default(),
        }
    }

    /// A counter scraped every 30s.
    fn counter(labels: &[(&str, &str)], start: f64, step: f64, n: usize) -> Series {
        let timestamps = (0..n).map(|i| i as i64 * 30_000).collect();
        let values = (0..n).map(|i| start + i as f64 * step).collect();
        Series::new(labels, timestamps, values).unwrap()
    }

    fn source() -> MemorySeriesSource {
        MemorySeriesSource::try_new(vec![
            counter(
                &[("__name__", "http_requests_total"), ("pod", "envoy-1")],
                1.0,
                1.0,
                4,
            ),
            counter(
                &[
                    ("__name__", "http_requests_total"),
                    ("pod", "envoy-2"),
                    ("route", "/"),
                ],
                1.0,
                2.0,
                4,
            ),
            counter(&[("__name__", "other")], 5.0, 0.0, 1),
        ])
        .unwrap()
    }

    #[test]
    fn one_label_set_twice_is_refused_at_construction() {
        let err = MemorySeriesSource::try_new(vec![
            counter(&[("__name__", "up")], 1.0, 0.0, 1),
            counter(&[("__name__", "up")], 2.0, 0.0, 1),
        ])
        .unwrap_err();
        assert!(err.contains(r#"__name__="up""#), "{err}");
    }

    #[tokio::test]
    async fn selects_by_matchers_and_clips_the_range() {
        let ctx = SessionContext::new();
        let plan = source()
            .select(
                &ctx.state(),
                &[matcher("__name__", MatchOp::Equal, "http_requests_total")],
                SelectHints::range(30_000, 60_000),
            )
            .await
            .unwrap();
        crate::series::validate(&plan.schema()).unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let decoded = crate::series::decode(&batches).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].timestamps(), [30_000, 60_000]);
        assert_eq!(decoded[0].values(), [2.0, 3.0]);
        assert_eq!(decoded[1].timestamps(), [30_000, 60_000]);
        assert_eq!(decoded[1].values(), [3.0, 5.0]);
        // `route` is in the schema because envoy-2 has it, and absent from
        // envoy-1's label set because its row holds "".
        assert!(decoded[0].labels().all(|(n, _)| n != "route"));
        assert_eq!(decoded[0].label("route"), "");
        assert_eq!(decoded[1].label("route"), "/");
    }

    /// The clip filters the shared child array, so a row keeps its own
    /// samples only as long as the kept ranges stay in row order. The
    /// series selected here is not the first one stored, which is what
    /// makes the two disagree if they ever do.
    #[tokio::test]
    async fn a_clip_after_dropping_earlier_rows_keeps_each_row_its_samples() {
        let ctx = SessionContext::new();
        let plan = source()
            .select(
                &ctx.state(),
                &[matcher("pod", MatchOp::Equal, "envoy-2")],
                SelectHints::range(60_000, 90_000),
            )
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let decoded = crate::series::decode(&batches).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].label("pod"), "envoy-2");
        assert_eq!(decoded[0].timestamps(), [60_000, 90_000]);
        assert_eq!(decoded[0].values(), [5.0, 7.0]);
        // `other` is out of the selection, so its label set is out of the
        // schema too.
        assert_eq!(
            crate::series::label_names(&batches[0].schema()),
            ["__name__", "pod", "route"]
        );
    }

    #[tokio::test]
    async fn an_empty_selection_is_a_valid_empty_plan() {
        let ctx = SessionContext::new();
        let plan = source()
            .select(
                &ctx.state(),
                &[matcher("pod", MatchOp::Equal, "envoy-3")],
                SelectHints::range(0, 1_000_000),
            )
            .await
            .unwrap();
        crate::series::validate(&plan.schema()).unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert!(crate::series::decode(&batches).unwrap().is_empty());
    }

    #[test]
    fn repeated_lines_for_one_series_are_one_series() {
        let load = [
            r#"x{a="1"} 1 2 3"#,
            r#"x{a="1"} 1 2 3"#,
            r#"x{a="1"} _ _ _ 4"#,
        ];
        let series: Vec<SeriesDescription> = load
            .iter()
            .map(|l| promql_parser::parse_series_desc(l).unwrap())
            .collect();
        let src = MemorySeriesSource::from_descriptions(&series, 1.0);
        assert_eq!(src.series().len(), 1);
        assert_eq!(src.series()[0].timestamps(), [0, 1000, 2000, 3000]);
        assert_eq!(src.series()[0].values(), [1.0, 2.0, 3.0, 4.0]);
    }

    /// `parse_series_desc` already retains only non-empty-valued matchers
    /// (`actions::series_description`'s `labels.retain`), so this never
    /// sees `env=""` through the parser. The merge key still drops an
    /// empty value itself, defensively, so a `SeriesDescription` built
    /// any other way can't produce two entries that later collapse to one
    /// label set inside `Series::new`.
    #[test]
    fn an_empty_valued_label_merges_with_the_label_omitted() {
        let with_empty = promql_parser::parse_series_desc(r#"foo{env=""} 1"#).unwrap();
        assert_eq!(with_empty.labels.len(), 1, "{:?}", with_empty.labels);

        let load = [r#"foo{env=""} 1"#, r#"foo 2"#];
        let series: Vec<SeriesDescription> = load
            .iter()
            .map(|l| promql_parser::parse_series_desc(l).unwrap())
            .collect();
        let src = MemorySeriesSource::from_descriptions(&series, 1.0);
        assert_eq!(src.series().len(), 1);
        assert_eq!(src.series()[0].values(), [2.0]);
        assert_eq!(src.series()[0].label("env"), "");
    }

    async fn select_all(src: &MemorySeriesSource) -> Arc<dyn ExecutionPlan> {
        let ctx = SessionContext::new();
        src.select(&ctx.state(), &[], SelectHints::range(0, i64::MAX))
            .await
            .unwrap()
    }

    fn pods(batches: &[RecordBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                let labels = b.column_by_name(LABELS).unwrap().as_struct();
                (0..b.num_rows())
                    .map(|r| {
                        labels
                            .fields()
                            .iter()
                            .zip(labels.columns())
                            .map(|(f, c)| format!("{}={}", f.name(), c.as_string_view().value(r)))
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The store's order is DataFusion's struct order, the one
    /// `SeriesSetExec` declares: `{b="1"}` is `("", "1")` and precedes
    /// `{a="1"}`, `("1", "")`, although Prometheus sorts them the other
    /// way round and `from_descriptions` merges them in that order.
    #[tokio::test]
    async fn series_come_out_in_struct_order() {
        let series: Vec<SeriesDescription> =
            [r#"x{a="1"} 1"#, r#"x{b="1"} 1"#, r#"x{a="1",b="1"} 1"#]
                .iter()
                .map(|l| promql_parser::parse_series_desc(l).unwrap())
                .collect();
        let src = MemorySeriesSource::from_descriptions(&series, 1.0);
        let ctx = SessionContext::new();
        let plan = src
            .select(&ctx.state(), &[], SelectHints::range(0, 1_000))
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(
            pods(&batches),
            [
                "__name__=x,a=,b=1",
                "__name__=x,a=1,b=",
                "__name__=x,a=1,b=1",
            ]
        );
    }

    /// Every series lands whole in one partition, its chunk rows in time
    /// order, and the partitions together hold exactly the unpartitioned
    /// rows, each partition itself in struct order.
    #[tokio::test]
    async fn partitions_keep_each_series_whole_and_ordered() {
        let many: Vec<Series> = (0..16)
            .map(|i| {
                counter(
                    &[("__name__", "x"), ("pod", &format!("p{i:02}"))],
                    0.0,
                    1.0,
                    10,
                )
            })
            .collect();
        let whole = select_all(&MemorySeriesSource::try_new(many.clone()).unwrap()).await;
        let parted = select_all(
            &MemorySeriesSource::try_new(many)
                .unwrap()
                .partitions(4)
                .chunked(60_000),
        )
        .await;
        assert_eq!(parted.output_partitioning().partition_count(), 4);

        let ctx = SessionContext::new();
        let mut seen: Vec<String> = Vec::new();
        for p in 0..4 {
            let stream = parted.execute(p, ctx.task_ctx()).unwrap();
            let batches = datafusion::physical_plan::common::collect(stream)
                .await
                .unwrap();
            let rows = pods(&batches);
            assert!(
                !rows.is_empty(),
                "partition {p} is empty; 16 series should spread"
            );
            let mut series = rows.clone();
            series.dedup();
            assert!(
                series.windows(2).all(|w| w[0] < w[1]),
                "partition {p}: {rows:?}"
            );
            let mut runs: Vec<(String, Vec<i64>)> = Vec::new();
            for s in crate::series::decode(&batches).unwrap() {
                let key = format!("{:?}", s.labels().collect::<Vec<_>>());
                match runs.last_mut() {
                    Some((k, ts)) if *k == key => ts.extend_from_slice(s.timestamps()),
                    _ => runs.push((key, s.timestamps().to_vec())),
                }
            }
            for (key, ts) in &runs {
                assert_eq!(ts.len(), 10, "partition {p}: {key} lost samples");
                assert!(
                    ts.windows(2).all(|w| w[0] < w[1]),
                    "partition {p}: {key} {ts:?}"
                );
            }
            seen.extend(series);
        }
        seen.sort();
        assert_eq!(seen, pods(&collect(whole, ctx.task_ctx()).await.unwrap()));
    }

    #[tokio::test]
    async fn an_invalid_regex_fails_the_selection() {
        let ctx = SessionContext::new();
        let err = source()
            .select(
                &ctx.state(),
                &[matcher("pod", MatchOp::RegexEqual, "(")],
                SelectHints::range(0, 1_000_000),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("regular expression"), "{err}");
    }
}
