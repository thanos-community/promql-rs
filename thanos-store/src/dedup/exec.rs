//! [`DedupExec`], Thanos's `dedupSeriesSet` and `dedupSeries` over blocks.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, ListArray, RecordBatch, StringViewArray, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Fields, SchemaRef, TimestampMillisecondType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::TryStreamExt;
use promql_engine::series::{
    sample_fields, sample_item, BLOCK_END, BLOCK_START, LABELS, SAMPLES, TIMESTAMP, VALUE,
};

use super::{decode_slot, slot_index, Algorithm, First, ReplicaMerge, REPLICA_SLOT_LABEL};

type Samples = Vec<(i64, f64)>;

/// Label values in field order with the slot's blanked. Shared, so that
/// the rows of one series and the merge kept for it hold one allocation.
type Key = Arc<[String]>;

/// The first samples a row's slot value names, shared by the rows of a
/// series for the same reason as [`Key`].
type Firsts = Arc<[(usize, First)]>;

/// What a [`DedupExec`] needs besides its input; [`super::DedupNode`] owns
/// the same fields and says what each means.
#[derive(Debug, Clone)]
pub(crate) struct DedupConfig {
    pub algorithm: Algorithm,
    pub counter: bool,
    pub start_ms: i64,
    pub window_ms: i64,
}

impl DedupConfig {
    /// The last timestamp the next block's reach does not contain, which
    /// is where a [`ReplicaMerge`] must have its state after this block.
    ///
    /// Blocks are contiguous, so the next block starts at this block's
    /// `end_ms`, and `reach` in `source.rs` makes its samples begin at
    /// `max(next.start_ms - window_ms, start_ms)`. Everything before that
    /// is absent from the next call, everything from there to this block's
    /// last sample is present in both; one too high a checkpoint would
    /// fold samples the next call replays, one too low would drop state it
    /// needs, and either diverges from Go silently at the block edge.
    fn checkpoint(&self, block_end_ms: i64) -> i64 {
        let next_reach = i128::from(block_end_ms) - i128::from(self.window_ms);
        let next_reach = next_reach.max(i128::from(self.start_ms));
        (next_reach - 1).clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }
}

/// Merges the replicas of each series within a block, `dedupSeriesSet`.
///
/// The store strips the replica labels, so replicas of a series arrive
/// with equal label sets, told apart only by [`REPLICA_SLOT_LABEL`]. A
/// block is buffered whole: slot is the first label by name order, so the
/// input is sorted by slot first and a series' replicas are scattered
/// across it. Memory is one block's samples plus a [`ReplicaMerge`] per
/// series seen, kept for the whole query because the penalty and the
/// counter lift carry from one block to the next. Every row names the
/// first sample of each of its series' slots, so a series' merge is built
/// whole when its first row shows and nothing is read ahead: a block never
/// waits for a later one.
///
/// The output keeps the input schema, the slot column blank, and is sorted
/// by the labels without the slot, which is not the input's order.
#[derive(Debug)]
pub(crate) struct DedupExec {
    input: Arc<dyn ExecutionPlan>,
    config: DedupConfig,
    properties: Arc<PlanProperties>,
}

impl DedupExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, config: DedupConfig) -> Self {
        let schema = input.schema();
        let column = |name: &str| -> Option<Arc<dyn PhysicalExpr>> {
            Some(Arc::new(Column::new(name, schema.index_of(name).ok()?)))
        };
        let mut properties = PlanProperties::clone(input.properties());
        // The same ordering SeriesSetExec declares, restated here because
        // ours differs from the input's: without it EnforceSorting would
        // put a SortExec on top to be safe.
        if let (Some(start), Some(end), Some(labels)) =
            (column(BLOCK_START), column(BLOCK_END), column(LABELS))
        {
            let ordering = [start, end, labels].map(PhysicalSortExpr::new_default);
            properties = properties.with_eq_properties(EquivalenceProperties::new_with_orderings(
                schema,
                [ordering],
            ));
        }
        Self {
            input,
            config,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for DedupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DedupExec")
    }
}

impl ExecutionPlan for DedupExec {
    fn name(&self) -> &str {
        "DedupExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .pop()
            .filter(|_| children.is_empty())
            .ok_or_else(|| DataFusionError::Plan("DedupExec has exactly one child".into()))?;
        Ok(Arc::new(Self::new(input, self.config.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let state = State {
            input,
            schema: Arc::clone(&schema),
            config: self.config.clone(),
            rows: VecDeque::new(),
            ended: false,
            merges: HashMap::new(),
            cache: Cache::default(),
        };
        let out = futures::stream::try_unfold(state, |mut state| async move {
            Ok(state.next_block().await?.map(|batch| (batch, state)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            out.map_err(|e: DataFusionError| e),
        )))
    }
}

/// One input row, decoded.
struct Row {
    key: Key,
    slot: usize,
    firsts: Firsts,
    block: (i64, i64),
    samples: Samples,
}

struct State {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    config: DedupConfig,
    /// Decoded rows read ahead of the block being emitted.
    rows: VecDeque<Row>,
    ended: bool,
    merges: HashMap<Key, ReplicaMerge>,
    cache: Cache,
}

/// What the last decoded row had; rows of one series arrive next to each
/// other, so most rows reuse it instead of allocating their own.
#[derive(Default)]
struct Cache {
    key: Option<Key>,
    /// The slot value past the slot index, and what it names.
    firsts: Option<(String, Firsts)>,
}

impl State {
    /// Reads one input batch into `rows`.
    async fn pull(&mut self) -> Result<()> {
        match futures::StreamExt::next(&mut self.input).await {
            Some(batch) => self.rows.extend(decode(&batch?, &mut self.cache)?),
            None => self.ended = true,
        }
        Ok(())
    }

    /// Reads until a row of a later block shows, or the input ends, so
    /// that every row of `block` is in `rows`.
    async fn fill(&mut self, block: (i64, i64)) -> Result<()> {
        while !self.ended && self.rows.back().is_none_or(|r| r.block == block) {
            self.pull().await?;
        }
        Ok(())
    }

    async fn next_block(&mut self) -> Result<Option<RecordBatch>> {
        while self.rows.is_empty() && !self.ended {
            self.pull().await?;
        }
        let Some(block) = self.rows.front().map(|r| r.block) else {
            return Ok(None);
        };
        self.fill(block).await?;

        // slot -> samples per series; BTreeMap sorts by the label values,
        // which is DataFusion's struct order, and by slot, the fold order.
        type Slots = BTreeMap<usize, Samples>;
        let mut groups: BTreeMap<Key, (Firsts, Slots)> = BTreeMap::new();
        while self.rows.front().is_some_and(|r| r.block == block) {
            let Some(row) = self.rows.pop_front() else {
                break;
            };
            groups
                .entry(row.key)
                .or_insert_with(|| (row.firsts, Slots::new()))
                .1
                .entry(row.slot)
                .or_default()
                .extend(row.samples);
        }

        // The input ended with this block: no later block reaches back, so
        // nothing is left to hold for.
        let checkpoint = if self.ended && self.rows.is_empty() {
            i64::MAX
        } else {
            self.config.checkpoint(block.1)
        };
        let mut merged: Vec<(Key, Samples)> = Vec::with_capacity(groups.len());
        for (key, (firsts, slots)) in groups {
            let replicas: Vec<(usize, &[(i64, f64)])> =
                slots.iter().map(|(id, s)| (*id, s.as_slice())).collect();
            let merge = self.merges.entry(Arc::clone(&key)).or_insert_with(|| {
                // A source that names no first samples has one replica per
                // slot it sends, so what this block holds is all there is.
                let firsts: Vec<(usize, First)> = if firsts.is_empty() {
                    replicas
                        .iter()
                        .filter_map(|(id, s)| s.first().map(|f| (*id, *f)))
                        .collect()
                } else {
                    firsts.to_vec()
                };
                ReplicaMerge::new(self.config.algorithm, self.config.counter, &firsts)
            });
            if let Some((id, _)) = replicas
                .iter()
                .find(|(id, s)| !s.is_empty() && !merge.knows(*id))
            {
                return Err(internal(&format!(
                    "slot {id} has samples but no first sample was named for it"
                )));
            }
            let mut samples = merge.advance(&replicas, checkpoint);
            let tail = replicas
                .iter()
                .any(|(_, s)| s.last().is_some_and(|(t, _)| *t > checkpoint));
            if checkpoint != i64::MAX && tail {
                // The block's reach runs past the checkpoint into the next
                // block's, which decides those samples again with its own
                // data; this block still has to answer for them.
                samples.extend(merge.preview(&replicas, checkpoint));
            }
            if !samples.is_empty() {
                merged.push((key, samples));
            }
        }
        encode(&self.schema, &merged, block).map(Some)
    }
}

fn internal(what: &str) -> DataFusionError {
    DataFusionError::Execution(format!("DedupExec: {what}"))
}

fn decode(batch: &RecordBatch, cache: &mut Cache) -> Result<Vec<Row>> {
    let labels = batch
        .column_by_name(LABELS)
        .and_then(|c| c.as_struct_opt())
        .ok_or_else(|| internal("no labels struct column"))?;
    let samples = batch
        .column_by_name(SAMPLES)
        .and_then(|c| c.as_list_opt::<i32>())
        .ok_or_else(|| internal("no samples list column"))?;
    let block = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_primitive_opt::<TimestampMillisecondType>())
            .ok_or_else(|| internal("no block column"))
    };
    let (starts, ends) = (block(BLOCK_START)?, block(BLOCK_END)?);
    let fields = labels.fields();
    let columns: Vec<&StringViewArray> = labels
        .columns()
        .iter()
        .map(|c| {
            c.as_string_view_opt()
                .ok_or_else(|| internal("a label column is not Utf8View"))
        })
        .collect::<Result<_>>()?;
    let slot_field = fields.iter().position(|f| f.name() == REPLICA_SLOT_LABEL);
    // A null label reads as the empty string, as everywhere in the engine.
    let value = |j: usize, i: usize| {
        let col = columns[j];
        if col.is_null(i) {
            ""
        } else {
            col.value(i)
        }
    };

    (0..batch.num_rows())
        .map(|i| {
            let mut slot = 0;
            let mut firsts: Firsts = Arc::from([]);
            if let Some(j) = slot_field {
                let v = value(j, i);
                if !v.is_empty() {
                    let named = v.find(';').map_or("", |at| &v[at..]);
                    match &cache.firsts {
                        Some((same, shared)) if same == named => firsts = Arc::clone(shared),
                        _ => {
                            let (_, parsed) = decode_slot(v).map_err(|e| internal(&e))?;
                            firsts = parsed.into();
                            cache.firsts = Some((named.to_owned(), Arc::clone(&firsts)));
                        }
                    }
                    slot = slot_index(v).map_err(|e| internal(&e))?;
                }
            }
            let shown = |j: usize| {
                if Some(j) == slot_field {
                    ""
                } else {
                    value(j, i)
                }
            };
            let same = cache
                .key
                .as_ref()
                .is_some_and(|k| k.iter().enumerate().all(|(j, v)| v == shown(j)));
            if !same {
                cache.key = Some((0..columns.len()).map(|j| shown(j).to_owned()).collect());
            }
            let key = Arc::clone(cache.key.as_ref().expect("set above"));

            let values = samples.value(i);
            let pairs = values
                .as_struct_opt()
                .filter(|s| s.null_count() == 0)
                .and_then(|s| {
                    let ts = s
                        .column_by_name(TIMESTAMP)?
                        .as_primitive_opt::<TimestampMillisecondType>()?;
                    let vs = s
                        .column_by_name(VALUE)?
                        .as_primitive_opt::<datafusion::arrow::datatypes::Float64Type>()?;
                    // The schema forbids nulls in a sample; reading past one
                    // would pair a timestamp with an undefined value.
                    (ts.null_count() == 0 && vs.null_count() == 0).then_some((ts, vs))
                })
                .ok_or_else(|| internal("samples are not non-null (timestamp, value) structs"))?;
            Ok(Row {
                key,
                slot,
                firsts,
                block: (starts.value(i), ends.value(i)),
                samples: pairs
                    .0
                    .values()
                    .iter()
                    .copied()
                    .zip(pairs.1.values().iter().copied())
                    .collect(),
            })
        })
        .collect()
}

/// One row per series in the order given, in `schema`'s shape.
fn encode(schema: &SchemaRef, series: &[(Key, Samples)], block: (i64, i64)) -> Result<RecordBatch> {
    let DataType::Struct(fields) = schema.field_with_name(LABELS)?.data_type() else {
        return Err(internal("labels is not a struct"));
    };
    let columns: Vec<ArrayRef> = (0..fields.len())
        .map(|j| {
            Arc::new(StringViewArray::from_iter_values(
                series.iter().map(|(key, _)| key[j].as_str()),
            )) as ArrayRef
        })
        .collect();
    let labels = StructArray::new(Fields::clone(fields), columns, None);

    let mut offsets = vec![0i32];
    let (mut ts, mut vs) = (Vec::new(), Vec::new());
    for (_, samples) in series {
        ts.extend(samples.iter().map(|&(t, _)| t));
        vs.extend(samples.iter().map(|&(_, v)| v));
        offsets.push(
            i32::try_from(ts.len())
                .map_err(|_| internal("more than i32::MAX samples in one block"))?,
        );
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
    let [start, end] = promql_engine::series::Block {
        start_ms: block.0,
        end_ms: block.1,
    }
    .columns(series.len());
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(labels), Arc::new(samples), start, end],
    )?)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::dedup::{encode_firsts, is_counter, Algorithm};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use promql_engine::series::schema;

    /// A row to feed: label values by field name, the slot first.
    pub(crate) struct In {
        pub labels: Vec<(&'static str, String)>,
        pub samples: Samples,
        pub block: (i64, i64),
    }

    pub(crate) fn batch(names: &[&str], rows: &[In]) -> RecordBatch {
        let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        let schema = schema(&names);
        let DataType::Struct(fields) = schema.field_with_name(LABELS).unwrap().data_type().clone()
        else {
            unreachable!()
        };
        let columns: Vec<ArrayRef> = fields
            .iter()
            .map(|f| {
                Arc::new(StringViewArray::from_iter_values(rows.iter().map(|r| {
                    r.labels
                        .iter()
                        .find(|(n, _)| *n == f.name())
                        .map_or("", |(_, v)| v.as_str())
                }))) as ArrayRef
            })
            .collect();
        let mut offsets = vec![0i32];
        let (mut ts, mut vs) = (Vec::new(), Vec::new());
        for r in rows {
            ts.extend(r.samples.iter().map(|s| s.0));
            vs.extend(r.samples.iter().map(|s| s.1));
            offsets.push(ts.len() as i32);
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
        let col = |f: fn(&(i64, i64)) -> i64| {
            Arc::new(TimestampMillisecondArray::from(
                rows.iter().map(|r| f(&r.block)).collect::<Vec<_>>(),
            )) as ArrayRef
        };
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StructArray::new(fields.clone(), columns, None)),
                Arc::new(samples),
                col(|b| b.0),
                col(|b| b.1),
            ],
        )
        .unwrap()
    }

    pub(crate) fn config(algorithm: Algorithm, counter: bool, window_ms: i64) -> DedupConfig {
        DedupConfig {
            algorithm,
            counter,
            start_ms: 0,
            window_ms,
        }
    }

    /// Runs `batches` through the exec's stream, as `(labels, samples, block)`.
    pub(crate) async fn run(
        batches: Vec<RecordBatch>,
        config: DedupConfig,
    ) -> Vec<(Vec<String>, Samples, (i64, i64))> {
        let schema = batches[0].schema();
        let input = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        let mut state = State {
            input,
            schema,
            config,
            rows: VecDeque::new(),
            ended: false,
            merges: HashMap::new(),
            cache: Cache::default(),
        };
        let mut out = Vec::new();
        while let Some(b) = state.next_block().await.unwrap() {
            out.extend(
                decode(&b, &mut Cache::default())
                    .unwrap()
                    .into_iter()
                    .map(|r| (r.key.to_vec(), r.samples, r.block)),
            );
        }
        out
    }

    fn scrapes(offset_s: i64, from_s: i64, to_s: i64) -> Samples {
        (from_s..to_s)
            .step_by(10)
            .map(|s| ((s + offset_s) * 1000, ((s + offset_s) / 10) as f64))
            .collect()
    }

    /// A row of `slot`, its value naming the first sample of every replica
    /// in `replicas` as the source does.
    fn row(
        slot: usize,
        replicas: &[&Samples],
        job: &'static str,
        samples: Samples,
        block: (i64, i64),
    ) -> In {
        let firsts: Vec<Option<First>> = replicas.iter().map(|s| s.first().copied()).collect();
        In {
            labels: vec![
                (
                    REPLICA_SLOT_LABEL,
                    format!("{slot}{}", encode_firsts(&firsts)),
                ),
                ("job", job.to_string()),
            ],
            samples,
            block,
        }
    }

    const NAMES: [&str; 2] = [REPLICA_SLOT_LABEL, "job"];

    #[tokio::test]
    async fn two_slots_merge_as_the_kernel_does() {
        let (a, b) = (scrapes(0, 0, 100), scrapes(1, 0, 100));
        let block = (0, 100_000);
        let both = [&a, &b];
        let out = run(
            vec![batch(
                &NAMES,
                &[
                    row(0, &both, "x", a.clone(), block),
                    row(1, &both, "x", b.clone(), block),
                ],
            )],
            config(Algorithm::Penalty, false, 0),
        )
        .await;
        let want = kernel(Algorithm::Penalty, false, &[a, b]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, want);
        assert_eq!(out[0].0, ["", "x"]);
    }

    /// The merge of whole replicas, the one-block answer by definition.
    fn kernel(algorithm: Algorithm, counter: bool, replicas: &[Samples]) -> Samples {
        let all: Vec<(usize, &[(i64, f64)])> = replicas
            .iter()
            .enumerate()
            .map(|(i, s)| (i, s.as_slice()))
            .collect();
        let firsts: Vec<(usize, First)> = all
            .iter()
            .filter_map(|(i, s)| s.first().map(|f| (*i, *f)))
            .collect();
        ReplicaMerge::new(algorithm, counter, &firsts).merge(&all, i64::MAX)
    }

    #[tokio::test]
    async fn chain_unions_the_slots() {
        let a: Samples = vec![(0, 1.0), (20_000, 3.0)];
        let b: Samples = vec![(0, 9.0), (10_000, 2.0)];
        let block = (0, 100_000);
        let both = [&a, &b];
        let out = run(
            vec![batch(
                &NAMES,
                &[
                    row(0, &both, "x", a.clone(), block),
                    row(1, &both, "x", b.clone(), block),
                ],
            )],
            config(Algorithm::Chain, false, 0),
        )
        .await;
        assert_eq!(out[0].1, [(0, 1.0), (10_000, 2.0), (20_000, 3.0)]);
    }

    #[tokio::test]
    async fn a_slot_in_several_rows_is_one_replica() {
        let block = (0, 100_000);
        let whole = scrapes(0, 0, 100);
        let out = run(
            vec![batch(
                &NAMES,
                &[
                    row(0, &[&whole], "x", whole[..5].to_vec(), block),
                    row(0, &[&whole], "x", whole[5..].to_vec(), block),
                ],
            )],
            config(Algorithm::Penalty, false, 0),
        )
        .await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, whole);
    }

    #[tokio::test]
    async fn the_slot_label_goes_and_the_output_is_sorted_without_it() {
        // Input is sorted with the slot first: (0,b) (0,c) (1,a) (1,b).
        // Without it the order is a, b, c, and b folds two slots.
        let block = (0, 100_000);
        let s = |v: f64| -> Samples { vec![(0, v)] };
        let (b0, b1) = (s(1.0), s(4.0));
        let out = run(
            vec![batch(
                &NAMES,
                &[
                    row(0, &[&b0, &b1], "b", b0.clone(), block),
                    row(0, &[&s(2.0)], "c", s(2.0), block),
                    row(0, &[&s(3.0)], "a", s(3.0), block),
                    row(1, &[&b0, &b1], "b", b1.clone(), block),
                ],
            )],
            config(Algorithm::Penalty, false, 0),
        )
        .await;
        let keys: Vec<_> = out.iter().map(|o| o.0.clone()).collect();
        assert_eq!(keys, [["", "a"], ["", "b"], ["", "c"]]);
        assert_eq!(out[1].1, [(0, 1.0)]);
    }

    /// The rows of one block over `replicas`, each cut to `lo..hi` as
    /// `reach` in `source.rs` cuts them, a row only for a replica that has
    /// a sample there.
    fn block_rows(replicas: &[Samples], block: (i64, i64), lo: i64, hi: i64) -> Vec<In> {
        let all: Vec<&Samples> = replicas.iter().collect();
        replicas
            .iter()
            .enumerate()
            .filter_map(|(slot, s)| {
                let cut: Samples = s
                    .iter()
                    .filter(|(t, _)| (lo..hi).contains(t))
                    .copied()
                    .collect();
                (!cut.is_empty()).then(|| row(slot, &all, "x", cut, block))
            })
            .collect()
    }

    /// `edges` cut into blocks, each fed what `reach` in `source.rs` gives
    /// it: samples from `max(start - window, 0)` to just before its end.
    fn blocks_of(replicas: &[Samples], edges: &[i64], window: i64) -> Vec<RecordBatch> {
        edges
            .windows(2)
            .map(|e| {
                let rows = block_rows(replicas, (e[0], e[1]), (e[0] - window).max(0), e[1]);
                batch(&NAMES, &rows)
            })
            .collect()
    }

    fn bits(samples: &[(i64, f64)]) -> Vec<(i64, u64)> {
        samples.iter().map(|&(t, v)| (t, v.to_bits())).collect()
    }

    /// One block over everything, one block per consecutive pair of
    /// `edges`, and the kernel over the whole replicas must agree. A
    /// block's output begins at its own reach, past the previous
    /// checkpoint, so it must equal the one-block result cut to that
    /// reach.
    async fn blocks_match_one(
        replicas: &[Samples],
        edges: &[i64],
        window: i64,
        algorithm: Algorithm,
        counter: bool,
    ) {
        let end = *edges.last().unwrap();
        let want = kernel(algorithm, counter, replicas);
        let one = run(
            blocks_of(replicas, &[0, end], 0),
            config(algorithm, counter, window),
        )
        .await;
        let one = one.first().map(|o| o.1.clone()).unwrap_or_default();
        assert_eq!(bits(&one), bits(&want), "one block, edges {edges:?}");

        let several = run(
            blocks_of(replicas, edges, window),
            config(algorithm, counter, window),
        )
        .await;
        let reach_of = |lo: i64, hi: i64| -> Samples {
            want.iter()
                .filter(|(t, _)| (lo..hi).contains(t))
                .copied()
                .collect()
        };
        let mut seen = 0;
        for out in &several {
            let e = edges
                .windows(2)
                .find(|e| out.2 == (e[0], e[1]))
                .expect("an emitted block is one of the blocks");
            seen += 1;
            assert_eq!(
                bits(&out.1),
                bits(&reach_of((e[0] - window).max(0), e[1])),
                "block {e:?} of {edges:?}, window {window}"
            );
        }
        // A block with no output is one where nothing reaches it.
        let silent = edges.len() - 1 - seen;
        for e in edges.windows(2) {
            if !several.iter().any(|o| o.2 == (e[0], e[1])) {
                assert!(
                    reach_of((e[0] - window).max(0), e[1]).is_empty(),
                    "block {e:?} missing for {silent} blocks"
                );
            }
        }
    }

    fn resetting(off: i64, to_s: i64, reset_s: i64, skip: std::ops::Range<i64>) -> Samples {
        scrapes(off, 0, to_s)
            .into_iter()
            .filter(|(t, _)| !skip.contains(&(t / 1000)))
            .map(|(t, _)| {
                let s = t / 1000;
                (t, if s < reset_s { s } else { s - reset_s } as f64)
            })
            .collect()
    }

    /// Both replicas reset the counter at 100 s, replica 1 misses a
    /// stretch, and the block edge at 100 s cuts through both: the
    /// blocks together must say what one block over everything says.
    #[tokio::test]
    async fn two_overlapping_blocks_merge_as_one() {
        let r = [
            resetting(0, 200, 100, 60..90),
            resetting(1, 200, 100, 150..170),
        ];
        blocks_match_one(&r, &[0, 100_000, 200_000], 30_000, Algorithm::Penalty, true).await;
    }

    /// Edges that fall between scrapes, and a reach (window 25 s) that
    /// is no multiple of the step: `checkpoint` must still leave no
    /// sample in both the folded state and the next block's input.
    #[tokio::test]
    async fn three_blocks_off_the_step_merge_as_one() {
        let r = [
            resetting(0, 300, 140, 100..115),
            resetting(3, 300, 140, 200..230),
        ];
        let edges = [0, 95_000, 207_000, 300_000];
        blocks_match_one(&r, &edges, 25_000, Algorithm::Penalty, true).await;
        blocks_match_one(&r, &edges, 25_000, Algorithm::Penalty, false).await;
    }

    /// Slot 0 is silent in the second block while slot 1 is not.
    #[tokio::test]
    async fn a_silent_first_slot_in_a_later_block_merges_as_one() {
        let r = [resetting(0, 100, 1000, 0..0), resetting(1, 200, 120, 0..0)];
        blocks_match_one(&r, &[0, 100_000, 200_000], 30_000, Algorithm::Penalty, true).await;
        blocks_match_one(&r, &[0, 100_000, 200_000], 0, Algorithm::Penalty, true).await;
    }

    /// Replica 0 starts late and a counter lifts replica 1 to its first
    /// value, so the first samples of replica 1 all precede it. One block
    /// gives them all; several blocks must give each its reach of that
    /// same series, valued by a sample only a later block has.
    #[tokio::test]
    async fn a_late_first_slot_gives_the_same_over_one_two_and_three_blocks() {
        let r = [scrapes(0, 80, 100), scrapes(1, 0, 100)];
        let want = kernel(Algorithm::Penalty, true, &r);
        assert_eq!(want.len(), 10);
        assert_eq!(want[0], (1_000, 8.0), "lifted to slot 0's first value");
        for edges in [
            vec![0, 100_000],
            vec![0, 50_000, 100_000],
            vec![0, 40_000, 70_000, 100_000],
            vec![0, 85_000, 100_000],
        ] {
            blocks_match_one(&r, &edges, 30_000, Algorithm::Penalty, true).await;
        }
    }

    /// Deterministic cases of 2 or 3 replicas, gaps, jitter, counter resets
    /// and stale markers, replica 0 often starting late, a counter or not,
    /// penalty or chain, and one to four blocks with edges off the step
    /// and windows of different lengths: the blocks emit what one block
    /// does, which is what the kernel gives over whole replicas.
    #[tokio::test]
    async fn blocks_match_one_block_and_the_kernel_over_random_series() {
        use crate::dedup::iter::tests::{generate, Rng};
        let mut rng = Rng(0x5eed_0fb1_0c00);
        let mut cases = 0;
        for round in 0..640 {
            let n = 2 + round % 2;
            let mut replicas = generate(&mut rng, n);
            if round % 4 == 1 {
                // Slot 0 starts late, past where later replicas begin.
                let from = 60_000 + rng.below(250_000) as i64;
                replicas[0].retain(|(t, _)| *t >= from);
            }
            let end = replicas.iter().flatten().map(|s| s.0).max().unwrap_or(0);
            if replicas.iter().all(Vec::is_empty) {
                continue;
            }
            let mut edges = vec![0];
            for _ in 0..rng.below(4) {
                edges.push(1 + rng.below(end as u64) as i64);
            }
            edges.sort_unstable();
            edges.dedup();
            edges.push(end + 1);
            let window = [0, 15_000, 25_000, 30_000, 60_000][rng.below(5) as usize];
            let algorithm = if round % 7 == 0 {
                Algorithm::Chain
            } else {
                Algorithm::Penalty
            };
            blocks_match_one(&replicas, &edges, window, algorithm, round % 3 != 0).await;
            cases += 1;
        }
        assert!(cases >= 400, "{cases} cases");
    }

    #[tokio::test]
    async fn rows_without_the_slot_label_pass_through() {
        let block = (0, 100_000);
        let a = scrapes(0, 0, 50);
        let out = run(
            vec![batch(
                &["job"],
                &[
                    In {
                        labels: vec![("job", "b".into())],
                        samples: a.clone(),
                        block,
                    },
                    In {
                        labels: vec![("job", "a".into())],
                        samples: a.clone(),
                        block,
                    },
                ],
            )],
            config(Algorithm::Penalty, true, 0),
        )
        .await;
        // Input order is the store's concern; output is sorted.
        assert_eq!(
            out.iter().map(|o| o.0[0].as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert!(out.iter().all(|o| o.1 == a));
    }

    #[test]
    fn the_checkpoint_precedes_the_next_reach() {
        let c = config(Algorithm::Penalty, false, 30_000);
        assert_eq!(c.checkpoint(100_000), 69_999);
        // The first reach is clamped to the select's start.
        assert_eq!(c.checkpoint(10_000), -1);
        assert!(is_counter(Some("rate")));
    }

    #[test]
    fn a_bad_slot_is_an_error() {
        let b = batch(
            &NAMES,
            &[In {
                labels: vec![(REPLICA_SLOT_LABEL, "x".into()), ("job", "a".into())],
                samples: vec![],
                block: (0, 1),
            }],
        );
        assert!(decode(&b, &mut Cache::default()).is_err());
    }

    #[tokio::test]
    async fn samples_of_a_slot_nobody_named_are_an_error() {
        let a = scrapes(0, 0, 30);
        let block = (0, 100_000);
        // Slot 1 sends samples but the slot values only name slot 0.
        let mut rows = vec![row(0, &[&a], "x", a.clone(), block)];
        rows.push(In {
            labels: rows[0].labels.clone(),
            samples: a.clone(),
            block,
        });
        rows[1].labels[0].1 = format!("1{}", encode_firsts(&[Some(a[0])]));
        let b = batch(&NAMES, &rows);
        let schema = b.schema();
        let input = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(b)]),
        ));
        let mut state = State {
            input,
            schema,
            config: config(Algorithm::Penalty, false, 0),
            rows: VecDeque::new(),
            ended: false,
            merges: HashMap::new(),
            cache: Cache::default(),
        };
        // Rows of one series share the first row's names, so slot 1 is
        // the one the merge does not know.
        let err = state.next_block().await.unwrap_err().to_string();
        assert!(err.contains("slot 1"), "{err}");
    }
}
