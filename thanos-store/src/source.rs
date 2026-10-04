//! The engine's view of the stores: a [`SeriesSource`] over a
//! [`ProxyStore`], built once per HTTP request so warnings have somewhere
//! to go.
//!
//! A select's whole range is fetched from the stores once, its chunks
//! kept encoded, and cut into blocks that are decoded one at a time as
//! the engine pulls them, one row per store chunk, so no series is ever
//! decoded over the whole range.

use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex, PoisonError};

use crate::dedup::{encode_firsts, is_counter, DedupNode, DeduplicationFunc, REPLICA_SLOT_LABEL};
use crate::labels::Matcher;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Float64Array, ListArray, RecordBatch, StringViewArray, StructArray,
    TimestampMillisecondArray, UInt32Array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Fields, SchemaRef};
use datafusion::catalog::Session;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::series::{sample_fields, sample_item, schema, Block, LABELS};
use promql_engine::{SelectHints, SelectorExtension, SeriesSource};
use promql_parser::ast::LabelMatcher;

use crate::labels::LabelSet;
use crate::proxy::{ChunkedSeries, ProxyStore, SeriesResult, WindowChunk};
use crate::storepb::thanos::PartialResponseStrategy;

/// Per-request knobs of the Prometheus API that reach the stores.
#[derive(Debug, Clone)]
pub struct SelectOptions {
    /// `partial_response`: whether a failing store costs the query or
    /// only earns a warning.
    pub partial_response: PartialResponseStrategy,
    /// `storeMatch[]`: query only stores whose `__address__` satisfies
    /// one of these selectors; empty means all of them.
    pub store_matchers: Vec<Vec<Matcher>>,
    /// Re-check every returned series against the selector and drop, with
    /// a warning, what does not match. Guards the engine's filter
    /// obligation against a misbehaving store.
    pub verify_matchers: bool,
    /// Replica deduplication; `None` leaves the replicas as separate
    /// series, Go's `dedup=false`.
    pub dedup: Option<Dedup>,
}

/// The query-time half of Thanos's `--query.replica-label` and
/// `--deduplication.func`.
#[derive(Debug, Clone)]
pub struct Dedup {
    pub replica_labels: Vec<String>,
    pub func: DeduplicationFunc,
}

impl SelectOptions {
    /// Go's `querier.isDedupEnabled`: asking for deduplication without a
    /// label to deduplicate along is asking for nothing.
    pub fn dedup_enabled(&self) -> bool {
        self.dedup
            .as_ref()
            .is_some_and(|d| !d.replica_labels.is_empty())
    }

    /// What the stores are asked to leave out: the replica labels when
    /// deduplicating, none otherwise.
    pub fn replica_labels(&self) -> &[String] {
        match &self.dedup {
            Some(d) => &d.replica_labels,
            None => &[],
        }
    }

    /// Whether one failing store fails the whole request.
    pub fn aborts(&self) -> bool {
        self.partial_response == PartialResponseStrategy::Abort
    }
}

impl Default for SelectOptions {
    fn default() -> Self {
        Self {
            partial_response: PartialResponseStrategy::Warn,
            store_matchers: Vec::new(),
            verify_matchers: true,
            dedup: None,
        }
    }
}

/// Prometheus TSDB's smallest block range, `--storage.tsdb.min-block-duration`.
pub const DEFAULT_BLOCK_MS: i64 = 2 * 60 * 60 * 1000;

/// The select's window-end domain, `[start_ms + window_ms, end_ms]`, cut
/// at multiples of `block_ms` from the Unix epoch, where Prometheus TSDB
/// cuts its block ranges, so a block's reach tends to fall in few store
/// blocks. Two choices break that alignment on purpose. The last block
/// ends at `end_ms + 1`, not the next `block_ms` boundary, and a domain
/// shorter than one block is a single block whatever edge it straddles;
/// either alignment would cost a short query a second store fetch for a
/// sliver of extra reach it does not need. The engine only requires the
/// blocks to be contiguous and ascending, not epoch-aligned, so losing
/// alignment here only costs cache locality between one query and the
/// next, not correctness. The edge arithmetic runs in `i128` so a domain
/// ending at the end of time still terminates.
pub fn cut_blocks(hints: &SelectHints, block_ms: i64) -> Vec<Block> {
    let (lo, hi) = (
        i128::from(hints.start_ms) + i128::from(hints.window_ms),
        i128::from(hints.end_ms),
    );
    let clamp = |t: i128| t.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
    if lo > hi {
        return Vec::new();
    }
    let block_ms = i128::from(block_ms.max(1));
    if hi - lo < block_ms {
        return vec![Block {
            start_ms: clamp(lo),
            end_ms: clamp(hi + 1),
        }];
    }
    let mut blocks = Vec::new();
    let mut start = lo;
    while start <= hi {
        let end = ((start.div_euclid(block_ms) + 1) * block_ms).min(hi + 1);
        blocks.push(Block {
            start_ms: clamp(start),
            end_ms: clamp(end),
        });
        start = end;
    }
    blocks
}

/// A [`SeriesSource`] that fans every selector out to the Thanos stores.
///
/// Warnings from the stores accumulate here across the selectors of one
/// query; the HTTP layer drains them with
/// [`Self::take_warnings`] for the response envelope.
#[derive(Debug)]
pub struct ThanosSeriesSource {
    proxy: Arc<ProxyStore>,
    options: SelectOptions,
    block_ms: i64,
    warnings: Warnings,
}

impl ThanosSeriesSource {
    pub fn new(proxy: Arc<ProxyStore>, options: SelectOptions) -> Self {
        Self {
            proxy,
            options,
            block_ms: DEFAULT_BLOCK_MS,
            warnings: Warnings::default(),
        }
    }

    /// Cut every select at multiples of `block_ms` instead of
    /// [`DEFAULT_BLOCK_MS`]; see [`cut_blocks`].
    pub fn blocks(mut self, block_ms: i64) -> Self {
        assert!(block_ms > 0, "a block spans at least a millisecond");
        self.block_ms = block_ms;
        self
    }

    pub fn options(&self) -> &SelectOptions {
        &self.options
    }

    pub fn proxy(&self) -> &ProxyStore {
        &self.proxy
    }

    /// The warnings gathered so far, leaving none behind.
    pub fn take_warnings(&self) -> Vec<String> {
        self.warnings.take()
    }
}

/// Shared with the block streams, which run after `select` returned.
/// Every selector of a query asks the same stores, so one store's
/// warning can arrive several times; like
/// Prometheus's annotations, each is kept once.
#[derive(Debug, Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

impl Warnings {
    fn extend(&self, warnings: Vec<String>) {
        let mut kept = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for warning in warnings {
            if !kept.contains(&warning) {
                kept.push(warning);
            }
        }
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

fn external(err: crate::StoreError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[async_trait]
impl SeriesSource for ThanosSeriesSource {
    async fn select(
        &self,
        _state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let blocks = cut_blocks(&hints, self.block_ms);
        // The plan's schema is due now and the label names are only known
        // once the stores have answered, so the whole range is fetched
        // here, one request per store however many blocks it is cut
        // into: a request per block costs a store round trip and an index
        // lookup each, which on a month at 2h blocks is hundreds of
        // sequential calls. A select without a window end has no rows to
        // fetch for.
        let fetched = match blocks.first() {
            None => SeriesResult::default(),
            Some(&first) => self
                .proxy
                .series_for_blocks(matchers, &hints, &self.options, Some(reach(&hints, first)))
                .await
                .map_err(external)?,
        };
        self.warnings.extend(fetched.warnings);
        tracing::debug!(
            series = fetched.series.len(),
            chunks = fetched.series.iter().map(|s| s.chunks.len()).sum::<usize>(),
            chunk_bytes = fetched
                .series
                .iter()
                .flat_map(|s| &s.chunks)
                .map(|c| c.data.len())
                .sum::<usize>(),
            "buffered select"
        );
        let mut names = fetched.names;
        let slotted = self.options.dedup_enabled();
        if slotted {
            // Sorted, as the schema's field order is.
            let at = names.partition_point(|n| n.as_str() < REPLICA_SLOT_LABEL);
            names.insert(at, REPLICA_SLOT_LABEL.to_string());
        }
        let schema = schema(&names);
        let stream = BlockStream {
            schema: Arc::clone(&schema),
            blocks: Mutex::new(Some(Blocks {
                schema: Arc::clone(&schema),
                slotted,
                names,
                series: fetched.series,
                hints,
                blocks,
            })),
        };
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            vec![Arc::new(stream)],
            None,
            [],
            false,
            None,
        )?))
    }

    fn scan_node(&self, scan: LogicalPlan, hints: &SelectHints) -> Result<LogicalPlan> {
        let Some(dedup) = self
            .options
            .dedup
            .as_ref()
            .filter(|_| self.options.dedup_enabled())
        else {
            return Ok(scan);
        };
        let node = DedupNode {
            replica_labels: dedup.replica_labels.clone(),
            algorithm: dedup.func.as_algorithm(),
            counter: is_counter(hints.func.as_deref()),
            start_ms: hints.start_ms,
            window_ms: hints.window_ms,
        };
        Ok(SelectorExtension::plan(Arc::new(node), scan))
    }
}

/// One select's fetched series, cut into blocks as the engine pulls.
#[derive(Debug)]
struct Blocks {
    /// Sorted: the schema's label names, the slot's among them when
    /// `slotted`.
    names: Vec<String>,
    /// Whether every row carries [`REPLICA_SLOT_LABEL`], its chunk's
    /// replica; set when deduplicating, so `DedupExec` can tell replicas
    /// apart after the stores dropped the labels that did.
    slotted: bool,
    schema: SchemaRef,
    /// In struct order; only the chunks a block still to come reaches.
    series: Vec<ChunkedSeries>,
    hints: SelectHints,
    blocks: Vec<Block>,
}

/// The one partition. The chunks move into the task that decodes them so
/// each can be dropped once past, which a second execution would then
/// miss, so there is none.
#[derive(Debug)]
struct BlockStream {
    schema: SchemaRef,
    blocks: Mutex<Option<Blocks>>,
}

impl PartitionStream for BlockStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let taken = self
            .blocks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(blocks) = taken else {
            let err = DataFusionError::Internal("select's blocks executed twice".into());
            return Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                futures::stream::once(async move { Err(err) }),
            ));
        };
        // Capacity one, and a slot reserved before a block is decoded: the
        // task works on the next block while the engine reads this one and
        // never further ahead, so decoded samples stay one block deep.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(blocks.emit(tx));
        let batches = futures::stream::unfold((rx, Some(task)), |(mut rx, task)| async move {
            if let Some(batch) = rx.recv().await {
                return Some((batch, (rx, task)));
            }
            // A closed channel is also what a panicked or cancelled task
            // leaves, which must not pass for the last block.
            match task?.await {
                Err(e) => Some((
                    Err(DataFusionError::Internal(format!(
                        "block decoding failed: {e}"
                    ))),
                    (rx, None),
                )),
                Ok(()) => None,
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            batches,
        ))
    }
}

impl Blocks {
    /// Every block with rows, in order, until the reader goes away or a
    /// block fails.
    async fn emit(mut self, tx: tokio::sync::mpsc::Sender<Result<RecordBatch>>) {
        for i in 0..self.blocks.len() {
            let Ok(permit) = tx.reserve().await else {
                return;
            };
            let batch = self.block(i);
            let failed = batch.is_err();
            if failed || batch.as_ref().is_ok_and(|b| b.num_rows() > 0) {
                permit.send(batch);
            }
            if failed {
                return;
            }
        }
    }

    /// Block `i`'s rows: one per chunk, every sample its windows reach.
    /// Afterwards drops the chunks no later block reaches.
    fn block(&mut self, i: usize) -> Result<RecordBatch> {
        let block = self.blocks[i];
        let window = reach(&self.hints, block);
        let mut decoded = Vec::with_capacity(self.series.len());
        for s in &self.series {
            let chunks = s.decode_window(window.clone()).map_err(external)?;
            if !chunks.is_empty() {
                decoded.push((&s.labels, encode_firsts(&s.firsts), chunks));
            }
        }
        let batch = block_batch(&self.schema, &self.names, self.slotted, &decoded, block)?;
        drop(decoded);
        // Reaches ascend with the blocks, so a chunk ending before the
        // next one starts is done. Samples decoded on arrival were only
        // for the first block; a later one decodes its own.
        match self.blocks.get(i + 1) {
            Some(&next) => {
                let from = *reach(&self.hints, next).start();
                for s in &mut self.series {
                    s.chunks.retain_mut(|c| {
                        c.decoded = None;
                        c.max_time >= from
                    });
                }
                self.series.retain(|s| !s.chunks.is_empty());
            }
            None => self.series.clear(),
        }
        Ok(batch)
    }
}

/// The samples `block`'s windows read, inside the select's range.
fn reach(hints: &SelectHints, block: Block) -> RangeInclusive<i64> {
    let window = i128::from(hints.window_ms);
    let start = (i128::from(block.start_ms) - window).max(i128::from(hints.start_ms));
    let end = (i128::from(block.end_ms) - 1).min(i128::from(hints.end_ms));
    start as i64..=end as i64
}

/// A label set, the part of its slot values naming its replicas' first
/// samples, and its chunks decoded for one block.
type BlockSeries<'a> = (&'a LabelSet, String, Vec<WindowChunk>);

/// `series`, already in the struct order `SeriesSetExec` checks, as one
/// canonical batch in `block`: a row per chunk, each label set's chunks
/// consecutive in the order given. With `slotted` each row's
/// [`REPLICA_SLOT_LABEL`] is its chunk's slot and the first samples of its
/// series' slots.
fn block_batch(
    schema: &SchemaRef,
    names: &[String],
    slotted: bool,
    series: &[BlockSeries<'_>],
    block: Block,
) -> Result<RecordBatch> {
    let DataType::Struct(fields) = schema.field_with_name(LABELS)?.data_type() else {
        unreachable!("a canonical schema");
    };
    let label_sets = label_array(fields, names, series.iter().map(|(labels, ..)| *labels));
    let mut rows: Vec<u32> = Vec::new();
    let mut slots: Vec<String> = Vec::new();
    let (mut ts, mut vs): (Vec<i64>, Vec<f64>) = (Vec::new(), Vec::new());
    let mut offsets: Vec<i32> = vec![0];
    for (i, (_, firsts, chunks)) in series.iter().enumerate() {
        for (slot, chunk) in chunks {
            rows.push(i as u32);
            if slotted {
                slots.push(format!("{slot}{firsts}"));
            }
            ts.extend(chunk.iter().map(|&(t, _)| t));
            vs.extend(chunk.iter().map(|&(_, v)| v));
            offsets.push(i32::try_from(ts.len()).map_err(|_| {
                DataFusionError::Execution("more than i32::MAX samples in one block".into())
            })?);
        }
    }
    let mut labels = take(&label_sets, &UInt32Array::from(rows), None)?;
    if slotted {
        labels = with_slots(fields, names, &labels, &slots);
    }
    let samples = ListArray::new(
        sample_item(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(StructArray::new(
            sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
            ],
            None,
        )),
        None,
    );
    let [block_start, block_end] = block.columns(labels.len());
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![labels, Arc::new(samples), block_start, block_end],
    )?)
}

/// `labels` with the slot column, blank until now, filled per row.
fn with_slots(fields: &Fields, names: &[String], labels: &ArrayRef, slots: &[String]) -> ArrayRef {
    let at = names
        .iter()
        .position(|n| n == REPLICA_SLOT_LABEL)
        .expect("a slotted schema has the slot label");
    let labels = labels
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("label_array builds a struct");
    let mut columns = labels.columns().to_vec();
    columns[at] = Arc::new(StringViewArray::from_iter_values(slots));
    Arc::new(StructArray::new(fields.clone(), columns, None))
}

/// One struct row per label set, `""` for a name it does not carry.
fn label_array<'a>(
    fields: &Fields,
    names: &[String],
    label_sets: impl ExactSizeIterator<Item = &'a LabelSet> + Clone,
) -> ArrayRef {
    if names.is_empty() {
        return Arc::new(StructArray::new_empty_fields(label_sets.len(), None));
    }
    let columns = names
        .iter()
        .map(|name| {
            let values = label_sets.clone().map(|l| l.get(name).unwrap_or(""));
            Arc::new(StringViewArray::from_iter_values(values)) as ArrayRef
        })
        .collect();
    Arc::new(StructArray::new(fields.clone(), columns, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkenc::encode_xor;
    use crate::proxy::RawChunk;

    const HOUR: i64 = 60 * 60 * 1000;

    fn hints(start_ms: i64, end_ms: i64, window_ms: i64) -> SelectHints {
        SelectHints::range(start_ms, end_ms).with_window_ms(window_ms)
    }

    fn block(start_ms: i64, end_ms: i64) -> Block {
        Block { start_ms, end_ms }
    }

    #[test]
    fn blocks_align_to_the_epoch_and_clamp_to_the_domain() {
        // Window ends from 01:30 to 06:10; the reach before 01:30 is the
        // first block's, not a block of its own.
        let cut = cut_blocks(&hints(HOUR, 6 * HOUR + 10 * 60_000, HOUR / 2), 2 * HOUR);
        assert_eq!(
            cut,
            [
                block(HOUR + HOUR / 2, 2 * HOUR),
                block(2 * HOUR, 4 * HOUR),
                block(4 * HOUR, 6 * HOUR),
                block(6 * HOUR, 6 * HOUR + 10 * 60_000 + 1),
            ]
        );
    }

    #[test]
    fn a_domain_ending_on_an_edge_ends_past_it() {
        // The window end at 04:00 exists, so the last block is
        // [04:00, 04:00 + 1ms) rather than nothing.
        assert_eq!(
            cut_blocks(&hints(0, 4 * HOUR, 0), 2 * HOUR),
            [
                block(0, 2 * HOUR),
                block(2 * HOUR, 4 * HOUR),
                block(4 * HOUR, 4 * HOUR + 1),
            ]
        );
    }

    #[test]
    fn a_domain_shorter_than_a_block_is_one_block() {
        // 01:30 to 02:30 straddles the 02:00 edge and still is one block.
        let (lo, hi) = (HOUR + HOUR / 2, 2 * HOUR + HOUR / 2);
        assert_eq!(
            cut_blocks(&hints(lo - 300_000, hi, 300_000), 2 * HOUR),
            [block(lo, hi + 1)]
        );
        // An instant query is one window end.
        assert_eq!(
            cut_blocks(&hints(lo - 300_000, lo, 300_000), 2 * HOUR),
            [block(lo, lo + 1)]
        );
        // One block's length exactly is two, because the end is inclusive.
        assert_eq!(cut_blocks(&hints(0, 2 * HOUR, 0), 2 * HOUR).len(), 2);
        assert_eq!(cut_blocks(&hints(0, 2 * HOUR - 1, 0), 2 * HOUR).len(), 1);
    }

    #[test]
    fn blocks_are_contiguous_before_the_epoch_and_at_the_end_of_time() {
        assert_eq!(
            cut_blocks(&hints(-3 * HOUR, HOUR, 0), 2 * HOUR),
            [
                block(-3 * HOUR, -2 * HOUR),
                block(-2 * HOUR, 0),
                block(0, HOUR + 1),
            ]
        );
        let cut = cut_blocks(&hints(i64::MAX - 3 * HOUR, i64::MAX, 0), HOUR);
        assert_eq!(cut.first().unwrap().start_ms, i64::MAX - 3 * HOUR);
        assert_eq!(cut.last().unwrap().end_ms, i64::MAX);
        assert!(cut.windows(2).all(|w| w[0].end_ms == w[1].start_ms));
    }

    #[test]
    fn a_window_wider_than_the_range_has_no_window_end() {
        assert!(cut_blocks(&hints(0, 1_000, 2_000), 2 * HOUR).is_empty());
    }

    const MIN: i64 = 60_000;

    /// One XOR chunk of a sample every 15m from `from_min` to `to_min`.
    fn raw(from_min: i64, to_min: i64) -> RawChunk {
        let samples: Vec<(i64, f64)> = (from_min..=to_min)
            .step_by(15)
            .map(|m| (m * MIN, m as f64))
            .collect();
        RawChunk {
            min_time: samples[0].0,
            max_time: samples[samples.len() - 1].0,
            addr: Arc::from("test"),
            data: encode_xor(&samples),
            decoded: None,
            slot: 0,
        }
    }

    type LabelledChunks<'a> = (&'a [(&'a str, &'a str)], Vec<RawChunk>);

    /// 00:00 to 06:00 with a 30m window at 2h blocks: blocks start at
    /// 00:30, 02:00, 04:00 and 06:00 and reach back to 00:00, 01:30,
    /// 03:30 and 05:30.
    fn six_hours(series: Vec<LabelledChunks<'_>>) -> Blocks {
        let hints = hints(0, 6 * HOUR, 30 * MIN);
        let names = vec!["__name__".to_string(), "job".to_string()];
        Blocks {
            slotted: false,
            schema: schema(&names),
            names,
            series: series
                .into_iter()
                .map(|(labels, chunks)| ChunkedSeries {
                    labels: LabelSet::from_strs(labels),
                    chunks,
                    firsts: Vec::new(),
                })
                .collect(),
            blocks: cut_blocks(&hints, 2 * HOUR),
            hints,
        }
    }

    fn chunks_left(blocks: &Blocks) -> usize {
        blocks.series.iter().map(|s| s.chunks.len()).sum()
    }

    #[test]
    fn a_chunk_is_freed_after_the_last_block_reaching_it() {
        let mut blocks = six_hours(vec![
            (
                &[("__name__", "up"), ("job", "a")],
                vec![raw(0, 60), raw(75, 180), raw(195, 360)],
            ),
            (&[("__name__", "up"), ("job", "b")], vec![raw(0, 60)]),
        ]);
        assert_eq!(blocks.blocks.len(), 4);
        // (rows, chunks left, series left) after each block. The next
        // block's reach decides, not the current one's: after the first
        // block, [01:15, 03:00] ends past 01:30 and stays, [00:00, 01:00]
        // does not, and job b goes with its only chunk.
        let mut got = Vec::new();
        for i in 0..blocks.blocks.len() {
            let rows = blocks.block(i).unwrap().num_rows();
            got.push((rows, chunks_left(&blocks), blocks.series.len()));
        }
        assert_eq!(got, [(3, 2, 1), (2, 1, 1), (1, 1, 1), (1, 0, 0)]);
    }

    #[test]
    fn samples_decoded_on_arrival_are_dropped_after_the_first_block() {
        let mut chunk = raw(75, 180);
        chunk.decoded = Some(crate::chunkenc::decode_xor(&chunk.data).unwrap());
        let mut blocks = six_hours(vec![(&[("__name__", "up"), ("job", "a")], vec![chunk])]);
        let first = blocks.block(0).unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(blocks.series[0].chunks[0].decoded, None);
    }

    /// A block no chunk reaches sends nothing, which the engine's check
    /// allows: it only needs each block to start at or after the end of
    /// the one before.
    #[tokio::test]
    async fn emit_skips_blocks_without_rows() {
        let blocks = six_hours(vec![(
            &[("__name__", "up"), ("job", "a")],
            vec![raw(0, 60), raw(330, 345)],
        )]);
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(blocks.emit(tx));
        let mut starts = Vec::new();
        while let Some(batch) = rx.recv().await {
            let batch = batch.unwrap();
            let column = batch
                .column_by_name(promql_engine::series::BLOCK_START)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0);
            starts.push(column);
        }
        task.await.unwrap();
        assert_eq!(starts, [30 * MIN, 4 * HOUR, 6 * HOUR]);
    }

    /// `cmp_struct` orders raw label sets before the schema exists; it
    /// must answer as Arrow's comparison on the encoded batch does, the
    /// one the engine's order check runs, including when the sets carry
    /// different names.
    #[test]
    fn struct_order_on_label_sets_agrees_with_arrow() {
        let sets: Vec<LabelSet> = [
            &[("__name__", "up")][..],
            &[("__name__", "up"), ("a", "1")],
            &[("__name__", "up"), ("b", "1")],
            &[("__name__", "up"), ("a", "1"), ("b", "2")],
            &[("__name__", "up"), ("a", "1"), ("c", "1")],
            &[("__name__", "up"), ("a", "2")],
            &[("__name__", "down"), ("z", "0")],
            &[("a", "1")],
            &[("b", "")],
            &[],
        ]
        .iter()
        .map(|pairs| {
            LabelSet::new(
                pairs
                    .iter()
                    .map(|(n, v)| (n.to_string(), v.to_string()))
                    .collect(),
            )
        })
        .collect();
        let mut names: Vec<String> = sets
            .iter()
            .flat_map(|l| l.pairs().iter().map(|(n, _)| n.clone()))
            .collect();
        names.sort();
        names.dedup();
        let schema = schema(&names);
        let DataType::Struct(fields) = schema.field_with_name(LABELS).unwrap().data_type() else {
            unreachable!("a canonical schema");
        };
        let array = label_array(fields, &names, sets.iter());
        let cmp = datafusion::arrow::array::make_comparator(
            &array,
            &array,
            datafusion::arrow::compute::SortOptions::default(),
        )
        .unwrap();
        for (i, a) in sets.iter().enumerate() {
            for (j, b) in sets.iter().enumerate() {
                assert_eq!(a.cmp_struct(b), cmp(i, j), "{a} vs {b}");
            }
        }
    }
}
