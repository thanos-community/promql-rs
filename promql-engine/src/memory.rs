//! An in-memory [`SeriesSource`], and the reference implementation of a
//! store's obligations.
//!
//! It exists for tests, the conformance suite seeds it from the corpus's
//! `load` blocks, but it is also the executable statement of what a real
//! store has to do: apply the matchers with [`crate::matcher`]'s
//! semantics, keep only samples inside the range, cut the range into
//! blocks and stamp every series with its block, hand over each series'
//! chunks consecutively, blocks ascending, series in struct order within
//! a block and samples in timestamp order, in the canonical schema. A
//! store implementer who wants to know "what exactly am I promising" can
//! read `select` below.
//!
//! One block per select is the smallest correct answer, and the one this
//! store gives by default: the block runs over the whole window-end
//! domain, so every series of the selection folds once, exactly as a
//! store that never heard of blocks would have it. [`MemorySeriesSource::blocks`]
//! is the test mode that cuts like a store with fixed block ranges.
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
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, AsArray, ListArray, RecordBatch, UInt32Array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{concat_batches, filter_record_batch, take, take_record_batch};
use datafusion::arrow::datatypes::TimestampMillisecondType;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use promql_parser::ast::{LabelMatcher, SeriesDescription};

use crate::error::{EngineError, SeriesError};
use crate::matcher::{mask_all, CompiledMatcher};
#[cfg(test)]
use crate::series::decode;
use crate::series::{
    clip, drop_empty, drop_unused_labels, encode, label_names_of, sample_item, with_block, Block,
    Series, BLOCK_END, BLOCK_START, LABELS, SAMPLES, TIMESTAMP,
};
use crate::source::{SelectHints, SeriesSource};

#[derive(Debug)]
pub struct MemorySeriesSource {
    /// Sorted by labels in struct order, the order `SeriesSetExec`
    /// declares, so a selection is sorted by construction.
    batch: RecordBatch,
    /// Test mode: see [`Self::chunked`].
    chunk_ms: Option<i64>,
    /// Test mode: see [`Self::blocks`].
    block_ms: Option<i64>,
    /// Test mode: see [`Self::partitions`].
    partitions: usize,
    /// See [`Self::rows_per_batch`].
    rows_per_batch: usize,
}

/// DataFusion's default `execution.batch_size`, the size a store scanning
/// through DataFusion hands over.
const ROWS_PER_BATCH: usize = 8192;

/// The block stamped on the stored batch, which no select hands out.
const WHOLE: Block = Block {
    start_ms: i64::MIN,
    end_ms: i64::MAX,
};

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
    /// every query. The block stamped here is a placeholder; `select`
    /// replaces it with the block of the select.
    pub fn try_new(series: Vec<Series>) -> std::result::Result<Self, SeriesError> {
        let batch = encode(&label_names_of(&series), &series, WHOLE)?;
        Ok(Self {
            batch: sort_by_labels(&batch)?,
            chunk_ms: None,
            block_ms: None,
            partitions: 1,
            rows_per_batch: ROWS_PER_BATCH,
        })
    }

    /// Test mode: hand every selected label set over as consecutive
    /// chunks of at most `chunk_ms` span, the way a chunked store does.
    /// Series order and time order are kept. `0` gives one sample per
    /// chunk.
    pub fn chunked(mut self, chunk_ms: i64) -> Self {
        self.chunk_ms = Some(chunk_ms);
        self
    }

    /// Test mode: cut the select's window-end domain at multiples of
    /// `block_ms`, the way a store with fixed block ranges does, rather
    /// than answering in one block. Each block's series hold the samples
    /// its windows reach, so a label set with samples in several blocks
    /// is handed over once per block, and one whose samples no window of
    /// a block reaches is absent from that block. Edges fall wherever the
    /// multiples do, between two steps included, so a block may answer
    /// no step at all. Combines with [`Self::chunked`] and
    /// [`Self::partitions`], which cut inside each block; a batch may
    /// still straddle a block edge.
    ///
    /// A width is unsigned and non-zero, so the zero or negative width a
    /// block cut cannot make sense of is a type error:
    ///
    /// ```compile_fail
    /// # use promql_engine::MemorySeriesSource;
    /// # use std::num::NonZeroU64;
    /// # fn f(source: MemorySeriesSource) {
    /// source.blocks(NonZeroU64::new(-5).unwrap());
    /// # }
    /// ```
    pub fn blocks(mut self, block_ms: NonZeroU64) -> Self {
        // Unsigned so a non-positive width cannot be written down. A width
        // past i64::MAX ms is wider than any timestamp, so saturating it
        // still gives one block, which is what the exact width would.
        self.block_ms = Some(i64::try_from(block_ms.get()).unwrap_or(i64::MAX));
        self
    }

    /// Hand rows over at most `n` to a `RecordBatch`, chunks or whole
    /// series, a label set free to straddle two batches. The default is
    /// DataFusion's batch size, what a store fills; `1` makes every row
    /// cross a batch boundary, which is what a test of carrying a series
    /// across batches wants. A bench at one row per batch measures
    /// DataFusion's per-batch cost, about half of a chunked 30-day query,
    /// not the engine.
    pub fn rows_per_batch(mut self, n: NonZeroUsize) -> Self {
        self.rows_per_batch = n.get();
        self
    }

    /// Test mode: spread the selected series over `n` partitions by a hash
    /// of their label set, each series whole in one partition and each
    /// partition in struct order, the way a store scanning shards in
    /// parallel does. Combines with [`Self::chunked`].
    pub fn partitions(mut self, n: NonZeroUsize) -> Self {
        self.partitions = n.get();
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
        let selected = drop_unused_labels(&selected).map_err(external)?;

        // Obligation 2, blocks: by default one over the whole window-end
        // domain, `[start_ms + window_ms, end_ms]`, its end exclusive.
        let blocks = match self.block_ms {
            Some(block_ms) => cut_into_blocks(&selected, &hints, block_ms),
            None => {
                let block = Block {
                    start_ms: hints.start_ms.saturating_add(hints.window_ms),
                    end_ms: hints.end_ms.saturating_add(1),
                };
                clip(&selected, hints.start_ms, hints.end_ms)
                    .and_then(|clipped| with_block(&clipped, block))
                    .map(|one| vec![one])
            }
        }
        .map_err(external)?;

        // Obligations 3 and 4, partition and order, come for free: the
        // stored batch already holds one series per label set in struct
        // order with its samples ascending, and neither step here, nor the
        // partitioning below, reorders anything within a partition. Blocks
        // ascend because they were cut in order, and each block hashes its
        // series to the same partition as every other.
        let schema = selected.schema();
        let mut partitions: Vec<Vec<RecordBatch>> = vec![Vec::new(); self.partitions];
        for block in &blocks {
            for (p, part) in partition_by_labels(block, self.partitions)?
                .into_iter()
                .enumerate()
            {
                partitions[p].push(part);
            }
        }
        let partitions = partitions
            .iter()
            .map(|parts| {
                let part = concat_batches(&schema, parts)?;
                match self.chunk_ms {
                    Some(chunk_ms) => split_into_chunks(&part, chunk_ms, self.rows_per_batch),
                    None => Ok(slice_rows(&part, self.rows_per_batch)),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(MemorySourceConfig::try_new_exec(&partitions, schema, None)?)
    }
}

/// Carry a series error out of `select` as an engine error, which
/// `EngineError::from` takes back out of DataFusion with its cause intact.
/// `DataFusionError::Execution` would flatten it to text first.
fn external(e: SeriesError) -> DataFusionError {
    DataFusionError::External(Box::new(EngineError::from(e)))
}

/// `batch`'s rows in struct order of their labels, the order DataFusion
/// compares `labels` in, which is what `SeriesSetExec` declares and
/// checks. It differs from the `(name, value)` order the rows may have
/// been merged in, so it cannot be skipped for series that "look" sorted.
fn sort_by_labels(batch: &RecordBatch) -> std::result::Result<RecordBatch, ArrowError> {
    let labels = batch.column_by_name(LABELS).expect("canonical");
    let converter = RowConverter::new(vec![SortField::new(labels.data_type().clone())])?;
    let rows = converter.convert_columns(std::slice::from_ref(labels))?;
    let mut order: Vec<u32> = (0..batch.num_rows() as u32).collect();
    order.sort_unstable_by_key(|&i| rows.row(i as usize));
    take_record_batch(batch, &UInt32Array::from(order))
}

/// Test mode: the select's window-end domain `[start_ms + window_ms,
/// end_ms]` cut at multiples of `block_ms`, the first block starting at
/// the domain's start so its reach-back is exactly the widened range.
/// Each block holds every series with a sample in `[block_start -
/// window_ms, block_end)` within the range, clipped to that, and no
/// other. Cut in `i128` so a domain ending at the end of time still
/// terminates; a block end past `i64::MAX` is clamped, which only loses
/// the window end at `i64::MAX` itself.
fn cut_into_blocks(
    batch: &RecordBatch,
    hints: &SelectHints,
    block_ms: i64,
) -> std::result::Result<Vec<RecordBatch>, SeriesError> {
    let (lo, hi) = (
        i128::from(hints.start_ms) + i128::from(hints.window_ms),
        i128::from(hints.end_ms),
    );
    let block_ms = i128::from(block_ms);
    let mut out = Vec::new();
    let mut start = lo;
    while start <= hi {
        let end = (start.div_euclid(block_ms) + 1) * block_ms;
        let block = Block {
            start_ms: start as i64,
            end_ms: end.min(i128::from(i64::MAX)) as i64,
        };
        let reach_lo = (start - i128::from(hints.window_ms)).max(i128::from(hints.start_ms));
        let reach_hi = (end - 1).min(hi);
        let reach = clip(batch, reach_lo as i64, reach_hi as i64)?;
        out.push(drop_empty(&with_block(&reach, block)?));
        start = end;
    }
    Ok(out)
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

/// `batch` in slices of at most `n` rows. A partition with no series is
/// still one batch, an empty one.
fn slice_rows(batch: &RecordBatch, n: usize) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    (0..rows.max(1))
        .step_by(n)
        .map(|k| batch.slice(k, n.min(rows - k)))
        .collect()
}

/// Test mode: turn one canonical batch (one series per label set) into
/// chunks of at most `chunk_ms` span, packed at most `rows_per_batch` to a
/// batch. Label set order is preserved, and within a label set so is time order,
/// so the result is what a chunked `SeriesSource` would hand over for the
/// same selection.
///
/// A label set's chunks are consecutive ranges of the samples child, and
/// the label sets are too, so a batch of chunks is one slice of that child
/// under new offsets: the samples are never copied, only the label rows.
fn split_into_chunks(
    batch: &RecordBatch,
    chunk_ms: i64,
    rows_per_batch: usize,
) -> Result<Vec<RecordBatch>> {
    let labels = batch.column_by_name(LABELS).expect("canonical");
    let samples = batch
        .column_by_name(SAMPLES)
        .expect("canonical")
        .as_list::<i32>();
    let offsets = samples.value_offsets();
    let timestamps = samples
        .values()
        .as_struct()
        .column_by_name(TIMESTAMP)
        .expect("canonical")
        .as_primitive::<TimestampMillisecondType>()
        .values();

    // Chunk `c` is series `rows[c]`'s samples `bounds[c]..bounds[c + 1]`.
    let mut rows: Vec<u32> = Vec::new();
    let mut bounds: Vec<i32> = vec![offsets[0]];
    for row in 0..batch.num_rows() {
        let (mut start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
        if start == end {
            rows.push(row as u32);
            bounds.push(end as i32);
        }
        while start < end {
            let mut next = start + 1;
            while next < end && timestamps[next] - timestamps[start] <= chunk_ms {
                next += 1;
            }
            rows.push(row as u32);
            bounds.push(next as i32);
            start = next;
        }
    }

    let mut out = Vec::new();
    for (k, rows) in rows.chunks(rows_per_batch).enumerate() {
        let bounds = &bounds[k * rows_per_batch..=k * rows_per_batch + rows.len()];
        let first = bounds[0];
        let list = ListArray::new(
            sample_item(),
            OffsetBuffer::new(bounds.iter().map(|b| b - first).collect::<Vec<_>>().into()),
            samples
                .values()
                .slice(first as usize, (bounds[rows.len()] - first) as usize),
            None,
        );
        let indices = UInt32Array::from(rows.to_vec());
        let column = |name: &str| {
            take(
                batch.column_by_name(name).expect("canonical"),
                &indices,
                None,
            )
        };
        out.push(RecordBatch::try_new(
            batch.schema(),
            vec![
                take(labels, &indices, None)?,
                Arc::new(list),
                column(BLOCK_START)?,
                column(BLOCK_END)?,
            ],
        )?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::{NonZeroU64, NonZeroUsize};

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("a count of at least one")
    }

    fn nz64(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).expect("a span of at least a millisecond")
    }
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
        assert!(err.to_string().contains(r#"__name__="up""#), "{err}");
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

    /// Every label set lands whole in one partition, its chunks in time
    /// order, and the partitions together hold exactly the unpartitioned
    /// series, each partition itself in struct order.
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
                .partitions(nz(4))
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

    /// A store fills its batches: rows pack up to the batch size whether
    /// or not they are chunks, and a series may straddle two batches.
    #[tokio::test]
    async fn rows_pack_into_batches_of_the_batch_size() {
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
        let ctx = SessionContext::new();
        let batches = |src: MemorySeriesSource| {
            let ctx = &ctx;
            async move {
                collect(select_all(&src).await, ctx.task_ctx())
                    .await
                    .unwrap()
            }
        };
        let sizes = |b: &[RecordBatch]| b.iter().map(RecordBatch::num_rows).collect::<Vec<_>>();
        let samples = |b: &[RecordBatch]| {
            crate::series::decode(b)
                .unwrap()
                .iter()
                .map(|s| (s.label("pod").to_string(), s.timestamps().to_vec()))
                .collect::<Vec<_>>()
        };
        let stored = || MemorySeriesSource::try_new(many.clone()).unwrap();

        // 30s apart, a 60s span is three samples: four chunks per series.
        let one = batches(stored().chunked(60_000).rows_per_batch(nz(1))).await;
        assert_eq!(sizes(&one), vec![1; 64]);
        let packed = batches(stored().chunked(60_000).rows_per_batch(nz(5))).await;
        assert_eq!(sizes(&packed), [vec![5; 12], vec![4]].concat());
        assert_eq!(pods(&packed), pods(&one));
        assert_eq!(samples(&packed), samples(&one));
        assert_eq!(sizes(&batches(stored().chunked(60_000)).await), [64]);

        let whole = batches(stored().rows_per_batch(nz(5))).await;
        assert_eq!(sizes(&whole), [5, 5, 5, 1]);
        assert_eq!(samples(&whole), samples(&batches(stored()).await));
    }

    /// Blocks cut at multiples of the block span from the first window
    /// end, each reaching back by the window and holding only the series
    /// that have a sample in that reach.
    #[tokio::test]
    async fn blocks_cut_the_window_end_domain_and_reach_back() {
        let ctx = SessionContext::new();
        let plan = source()
            .blocks(nz64(40_000))
            .select(
                &ctx.state(),
                &[matcher("__name__", MatchOp::Equal, "http_requests_total")],
                SelectHints {
                    window_ms: 30_000,
                    ..SelectHints::range(0, 90_000)
                },
            )
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let mut rows: Vec<(Block, String, Vec<i64>)> = Vec::new();
        for b in &batches {
            for (row, s) in crate::series::decode(std::slice::from_ref(b))
                .unwrap()
                .iter()
                .enumerate()
            {
                rows.push((
                    crate::series::block_of(b, row),
                    s.label("pod").to_string(),
                    s.timestamps().to_vec(),
                ));
            }
        }
        let block = |start_ms, end_ms| Block { start_ms, end_ms };
        assert_eq!(
            rows,
            [
                (block(30_000, 40_000), "envoy-1".into(), vec![0, 30_000]),
                (block(30_000, 40_000), "envoy-2".into(), vec![0, 30_000]),
                (
                    block(40_000, 80_000),
                    "envoy-1".into(),
                    vec![30_000, 60_000]
                ),
                (
                    block(40_000, 80_000),
                    "envoy-2".into(),
                    vec![30_000, 60_000]
                ),
                (
                    block(80_000, 120_000),
                    "envoy-1".into(),
                    vec![60_000, 90_000]
                ),
                (
                    block(80_000, 120_000),
                    "envoy-2".into(),
                    vec![60_000, 90_000]
                ),
            ]
        );

        // A block none of whose windows reach a series leaves it out: the
        // data ends at 90s, so the block from 120s still reaches its last
        // sample and the two after it hold nothing rather than an empty
        // series.
        let plan = source()
            .blocks(nz64(40_000))
            .select(
                &ctx.state(),
                &[matcher("pod", MatchOp::Equal, "envoy-1")],
                SelectHints {
                    window_ms: 30_000,
                    ..SelectHints::range(0, 200_000)
                },
            )
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let blocks: Vec<Block> = batches
            .iter()
            .flat_map(|b| (0..b.num_rows()).map(|r| crate::series::block_of(b, r)))
            .collect();
        assert_eq!(
            blocks,
            [
                block(30_000, 40_000),
                block(40_000, 80_000),
                block(80_000, 120_000),
                block(120_000, 160_000),
            ]
        );
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
