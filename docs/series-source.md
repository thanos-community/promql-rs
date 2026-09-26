# The `SeriesSource` trait

*Status: draft for discussion.*

Every PromQL-on-something implementation ends up writing the thing
Prometheus calls `storage.Querier.Select`: the series matching these
matchers, in this time range. Each of us has a different store behind it:
TSDB blocks, Parquet, Iceberg, ClickHouse, remote read, columnar stores of
our own. For one shared engine we have to agree on exactly two things, and
this document proposes both:

1. **The trait a store implements.** `SeriesSource`, one method: a select
   the engine issues once per selector over the whole query range.
2. **The Arrow shape its data comes back in.** One row per chunk of one
   series, stamped with the block of the store's own layout it was cut
   into, and inside a block either label-sorted with the rows of a series
   consecutive, or keyed by an opaque series id the store provides. A
   `RecordBatch` in this shape is a *series batch*, and its boundaries mean
   nothing: a series' chunk rows may be split across any number of
   RecordBatches.

Everything PromQL, lookback, staleness, the steps, `offset`, `@`,
functions and aggregations, happens in the engine and is not the store's
business. Agreeing on the trait and the shape first lets the engine be
built in stacked PRs on top of them, and every store in parallel.

## The `SeriesSource` trait

```rust
#[async_trait]
pub trait SeriesSource: Debug + Send + Sync {
    /// Every series matching all of `matchers` with samples within `hints`,
    /// as a plan whose schema passes `series::validate`.
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Prometheus's `storage.SelectHints`, typed, plus the window.
pub struct SelectHints {
    pub start_ms: i64,              // inclusive; binding, widened by the window
    pub end_ms: i64,                // inclusive; binding
    pub window_ms: i64,             // lookback delta or `[5m]`; binding at every block
    pub step_ms: Option<i64>,       // step of the range query; None for instant
    pub range_ms: Option<i64>,      // window of a range selector, `[5m]`
    pub func: Option<String>,       // function or aggregation directly above
    pub grouping: Option<Grouping>, // `by`/`without` labels directly above
    pub shard: Option<Shard>,       // return shard `index` of `count` only
}

pub struct Grouping { pub labels: Vec<String>, pub by: bool }
pub struct Shard { pub index: u64, pub count: u64 }
```

**What the engine asks.** One `select` per selector, over the whole query
range. The matchers are the selector's exactly as parsed, including
`__name__` synthesised from a bare metric name. The inclusive millisecond
range is binding and is Prometheus's: it runs from the first step's window
end, `t − offset` or the `@` time, widened back by the window, the lookback
delta for an instant selector or the `[5m]` for a range one, to the last
step's, as Prometheus widens `SelectHints.Start` and `End`
(`promql/engine.go`, `getTimeRangesForSelector`). `window_ms` carries that
window, so the store can reach back by it at every block it cuts; it needs
no PromQL to honour it.

**Blocks are the store's.** The store cuts its answer into blocks along
its own time units: a TSDB store its block ranges, a columnar store
partitioned by hour its hours, a store that keeps whole series one block,
and an adapter over a label-sorted wire protocol the windows it chooses to
ask in, since the protocol forces one request per window. Every chunk row
carries its block's `block_start` and `block_end`, see *The series batch*.
Blocks ascend by `block_start`, do not overlap, and are contiguous over the
select's window ends, from `start_ms + window_ms` to `end_ms`, so the first
block's reach-back covers the widened start. A block's rows for a series
hold every sample the store has for it in `[block_start − window_ms,
block_end)` within the select's range; the block answers the steps of the
query whose window end, `t − offset` or the `@` time, lies in
`[block_start, block_end)`; its samples before `block_start` feed windows
only. A
series appears in every block whose `[block_start − window_ms, block_end)`
holds one of its samples, even when all of them lie before `block_start`.
Each block reaches back far enough to answer its steps alone, so nothing
per series crosses a block edge and the engine forgets every series there;
carrying a window per series across edges instead is the per-series state
blocks exist to avoid. Every partition
of a select must cut the same edges, which the store keeps trivially, since
the blocks are its layout.

The remaining hints mirror Prometheus's and are advisory: a store may use
them to read less or to lay its output out better, and may ignore them
without changing the result. A columnar store can get real work out of
them.

- `step_ms` and `range_ms`: read downsampled or aligned data, prune chunks
  per window.
- `func`: `count_over_time` and `present_over_time` need no values;
  `rate` needs whole windows.
- `grouping`: with `sum by (route)` directly above, only `route` decides
  the output, so a store may drop the other labels, and if it partitions
  its plan by the grouping labels the engine aggregates without a
  shuffle.
- `shard`: scan the series space in parallel, one shard per plan.

Prometheus's `Limit` and `DisableTrimming` are left out: the first serves
its label-values API, the second its own chunk trimming.

**What the store answers.** A DataFusion `ExecutionPlan` whose schema is
the series-batch schema below. A physical plan, rather than a stream of
batches, lets the store keep what it knows: its partitioning and ordering,
predicate pushdown, streaming, its own scan operators. The engine builds
everything above that plan and never looks inside it.

**When.** Once per selector, before the query's plan runs, as Prometheus
expands a selector's series before evaluating. The schema, meaning the
label names, is only known once the store has found the selection's
series, and DataFusion needs it before it can plan above it. The store's
own planning therefore happens while the engine plans the query, not while
it executes. A selection's schema is fixed for the whole select, every
block included; label names that only turn up during execution are not
supported.

**Four obligations.** `series::validate` checks only the schema; the
ordering is checked per row, see *Violations*.

1. *Filter.* Every series matches every matcher. A row may carry samples
   outside the range, see *Order*, but a row whose whole span misses it
   should not be sent. The store prunes those on the chunk min and max time
   it already keeps, as Thanos's `LoadSeriesForTime` (`pkg/store/bucket.go`)
   and Prometheus's chunk metadata filter (`tsdb/querier.go:559-565`) do,
   so the engine needs no column for it.
2. *Rows.* A chunk row holds one chunk of exactly one series and at least
   one sample, samples ascending by timestamp.
3. *Order.* Stated over a partition's stream of rows, never over a
   batch. A series may end mid-batch or span any number of RecordBatches;
   nothing requires it to be whole in one, because the engine does not look
   chunks up but folds rows as they arrive. All of a block's rows come
   before the next block's first row, and inside a block rows come in one
   of two modes. In **sorted mode**:
   - series sorted by their label set;
   - rows of one series consecutive and never crossing a partition: the
     first row with another label set closes the series for the block, and
     no later row of the block may carry it.

   In **keyed mode**, every row carries a `series_id`, see *The series
   batch*, and rows of one series may come in any order relative to other
   series' rows in the block, so the store need not sort by labels at all;
   they still never cross a partition. In both modes rows of one series
   ascend by first sample timestamp within a block, and the lookback
   and the steps a block answers are the same.

   A row may be a whole chunk that straddles either end of the range, as
   `rpc.proto:37-38` allows. A sample at or after `block_end` falls in no
   window of a step the block answers, and neither does one before
   `block_start − window_ms`, so the engine neither clips nor checks a
   sample against an edge, and the store never decodes a chunk to trim it.

   Sorted mode's label and consecutiveness rules, the partition bound
   aside, are Thanos's `Store.Series` frame rule
   (`pkg/store/storepb/rpc.proto:29-32`): "a single frame can contain
   partition of the single series, but once a new series is started to be
   streamed it means that no more data will be sent for previous one.
   Series has to be sorted." A `Series` request narrowed to one block and
   its reach-back answers that block as it stands. The time order of a
   series' rows is ours: the Store API has "no
   requirements on chunk sorting" (`rpc.proto:34`) and its proxy sorts
   chunks itself (`pkg/store/proxy_merge.go:180-182`), so a store adapted
   from it sorts each series' chunk metas before emitting rows, touching
   metas, not samples.
4. *Handed over in that order.* The store's obligation stops at emitting
   rows in this order. It picks the mode by its schema: a plan whose schema
   carries `series_id` is in keyed mode, one without it in sorted mode, and
   the engine declares the scan to match, the output ordering
   `(block_start, labels, first sample timestamp)` and hash partitioning on
   `labels`, the full label set, for sorted mode, the ordering
   `block_start` and hash partitioning on `series_id` for keyed mode. It
   checks the declaration against what actually arrives row by row, and
   refuses a sorted-mode plan that loses the ordering, because sorted
   mode's memory bound rests on DataFusion being able to prove it, see
   [`engine.md`](engine.md).

**Violations.** The engine never sorts or buffers to repair order. It always
checks, one label comparison per row, batch boundaries included. In sorted
mode it compares with the previous row of the block: labels non-decreasing
within a block of a partition's stream, which also catches a closed series
reappearing. In keyed mode it compares with the labels the row's
`series_id` was first seen with in the block, so an id collision or a store
that reuses an id cannot merge two series. In both, first sample timestamp
is non-decreasing within a series and block, `block_start` and `block_end`
are constant within a block, and blocks are non-decreasing and
non-overlapping across the rows of a partition. A violation is a query
error, where upstream the same mistake is a silent wrong result. An empty
row, though forbidden, is skipped.

**Overlap.** Adjacent rows of one series may overlap in time, which is
normal once one series' chunks arrive from several files or compaction
levels of the store. The engine skips samples at or before the last
timestamp it has already seen for that series, so the first row wins. Each
block starts from nothing, since its reach-back repeats samples the block
before held. That is Thanos's `chunkSeriesIterator`, which does the same
with `Seek(lastT + 1)` (`pkg/query/iter.go:277-281`). Prometheus's block
merge keeps the union instead; this document follows first-row-wins,
settled the same way in [`engine.md`](engine.md#ordering-integrity)'s
*Overlap*.

A store that keeps whole series is not asked to change anything beyond the
two block columns. It stamps the range as one block and sends one row per
series, which is the one-block, one-chunk case of this contract.

Matcher semantics are Prometheus's: a label a series lacks compares as
`""`, so `k!="v"` and `k=~".*"` match a series without `k` and `k=~".+"`
does not, and a regex is anchored to the whole value. `matcher.rs` holds
these rules in plain Rust. The in-memory source applies them directly, a
real store translates them into its own predicates, and the differential
tests against Prometheus decide whether it got that right.

## The series batch

```text
labels       Struct<{name}: Utf8View, …>    one field per label name, sorted
samples      List<Struct<timestamp: Timestamp(ms), value: Float64>>
block_start  Timestamp(ms)                  window ends the block answers, from here, inclusive
block_end    Timestamp(ms)                  to here, exclusive; both constant over the block
series_id    UInt64 | FixedSizeBinary(n)    optional; present only in keyed mode
```

One row is one chunk of one series in one block. Nothing is nullable.
`block_start` and `block_end` may be plain or run-end encoded, the store's
choice; at one row per chunk either costs next to nothing.

A series is identified by its label set: one value per label name, and
the set never changes for the life of the series. Three series that
differ only in `pod` are three rows, each with its complete label set and
its own samples. A batch is many such rows side by side and nothing more:
rows share only the schema and the union of their label names. Two rows with
the same label set in one block are two chunks of that series. In sorted
mode they must be consecutive in the partition's stream, whether or not a
batch boundary falls between them, and the same label set reappearing in
the block after another one has started is refused; in keyed mode they
carry one `series_id` and other series' rows may lie between them.
Merging series by the labels that survive an aggregation is an operator
above this shape, not part of it.

The `series_id` does not replace the label set as the identity. The engine
compares it for equality and hashes its bytes, and never interprets it; how
and when the store computes it is the store's business. The contract asks
only that within one store, and therefore within one block, one id maps to
one label set and one label set to one id. The engine buckets rows by the id
and confirms each by its labels, as Prometheus's head does with its
`seriesHashmap`, which checks every hash hit with `labels.Equal` (prom
`tsdb/head.go:2332-2362`), and not as its PromQL engine does when it trusts a
bare `metric.Hash()` in vector matching (prom `promql/engine.go:3371-3381`).
An id computed at ingestion can be finer than a PromQL series when it also
hashes, say, a unit or an instrumentation scope; such a store merges those
to one id per label set before emitting. Two ids with one label set
otherwise leave the selector as two rows with one label set, and the
engine refuses the block: caught by the selector's own label-set
uniqueness check at the block edge, or by the output check of
[`engine.md`](engine.md#the-correctness-net) otherwise.

| Choice | Why |
|---|---|
| A row is a chunk of one series, not a sample | Every PromQL operator works on one series' samples in order: lookback, staleness, `rate`, counter resets. With rows as samples, the first thing each operator does is find the series again. With chunk rows, a chunk is a zero-copy slice, a series is a run of consecutive rows folded in order, and parallelism over partitions needs no shuffle because no series crosses one. The regrouping has to happen somewhere; once, in the store that knows its own series boundaries and emits chunks as it keeps them, beats once per operator. |
| Rows of a series may span batches | The store is not asked to size batches around series: it cuts them where its scan or memory budget says, and a series continues into the next batch. The engine carries only window buffers across a batch boundary, the one open series' per partition in sorted mode, every series of the block in keyed mode, so its memory does not depend on where batches end. |
| An optional fixed-width `series_id` | TSDB blocks and the Store API deliver label order and carry no id on any read path: Prometheus's `storage.Series` exposes only labels (prom `storage/interface.go:633-636`), and Thanos's `storepb.Series` carries labels and chunks, whose per-chunk `hash` is a content checksum (thanos `pkg/store/storepb/types.proto:23-37`). A store that computes a fingerprint at ingestion, as a columnar store could, has an id for free and label order only at the cost of a sort per block. So the column is optional and its presence is the mode. `UInt64` or `FixedSizeBinary(n)`, whichever the store already keeps, because a fixed width compares and hashes without offsets and a 16-byte hash fits as well as a 64-bit one. |
| Labels are a struct with a field per name | The schema is the union of the selection's label names, so `by (route)` is a column reference and DataFusion's own grouping, sorting and `EXPLAIN` understand it. Fields are sorted so two producers build one schema. |
| An absent label is `""`, not NULL | That is PromQL's semantics, and non-nullable children remove the `get_field`-under-a-NULL-parent trap: there is no NULL parent to read a phantom value through. |
| `Utf8View` at the leaf | One 16-byte view per label per series, values up to 12 bytes inline, equality decided from length and prefix before any buffer is read. It is what DataFusion's Parquet reader hands over, and the only string type with DataFusion's group-by fast path. A dictionary would encode nothing within a series, where each value appears once, and the engine cannot use batch-level keys: DataFusion hydrates them at the first group-by. |
| Samples are one list of structs | One offsets buffer, so a timestamp and its value cannot drift apart. Equal lengths and positional alignment are structure, not a promise two parallel lists would have to keep. |
| Milliseconds, `Float64` | Prometheus's units. `Float64` carries StaleNaN as the exact bit pattern `0x7ff0_0000_0000_0002`; nothing may cast through `f64::NAN`. |
| `List`, not `LargeList` | i32 offsets cap the samples in one batch at 2³¹ − 1, about 2.1 billion. Even a whole-series row scraped every 5 s reaches that only after roughly 340 years, and after 68 years at 1 s; a series of many chunk rows may be split across batches anyway, and a batch is bounded by the store's batch size far below the cap. `encode` errors rather than overflowing. |
| Block columns, but no time-bound columns and no step | Samples ascend within a row, so the first and last sample timestamps are `timestamp[offsets[i]]` and `timestamp[offsets[i+1] - 1]`, two O(1) reads. A row that cannot reach the first step is still skipped without walking its samples, which is what Prometheus cannot do when it decodes through a chunk to find out (`tsdb/querier.go:779-791`). Columns carrying the same two values would be a second sort key that can disagree with the data. The block cannot be read off the data: the lookback puts samples before `block_start` into the block's rows, so no sample says where the block begins, and the engine needs `block_end` before any series closes in the block. So both edges travel on every row. The step stays the engine's. |

**In code.** `series.rs` is the one definition of the shape. `encode`
exists for stores that hold whole series as rows, and refuses two rows with
one label set because it cannot tell a second chunk from a duplicate; a
store that sends several chunk rows per series, or already has Arrow,
builds the batch itself and needs only `validate`.

## Implementing it

**In memory** (`memory.rs`). This is the contract's reference
implementation: the plainest store that can satisfy it, built to promise
exactly what the contract requires and nothing more, so the differential
and promqltest suites have a store to hold every other implementation
against. It defaults to one block and one row per series, but the tests
also cut it into blocks, realistic chunks and one sample per chunk, so
every kernel runs across row and block edges the same way a real store's
output would force it to.

**A columnar store** (Parquet, a lakehouse). Push the matchers into the
scan as predicates and the range into row-group or chunk pruning. If the
store keeps a sample per row, a group-by over the label columns with an
ordered `array_agg` produces the shape for one block. If it already keeps
chunks of one series per row, it emits them as they are, sorted by
`(labels, first sample timestamp)`, and concatenates nothing. A columnar
store partitioned by hour could declare its hours as blocks, sort one hour
by labels and time and emit it as a block, which bounds its sort by an hour
instead of the range. A store that keeps an ingestion-time series id beside
its rows skips that sort: it emits the hour as it lies, in keyed mode, with
each series' rows ascending by time. Either way the store does the
regrouping, because it knows its own series boundaries and the engine does
not.

**A remote store** (Prometheus remote read, a gRPC service). Cut the
window-end domain `[start_ms + window_ms, end_ms]` into blocks and request
each block widened back by `window_ms`. Group the reply's chunks by series
and build one `Series` per series and block from all of that series'
chunks, sorted by first sample timestamp where the protocol does not
promise it, never one `Series` per chunk, since `encode` refuses two rows
with one label set; then `encode`, stamping the block's edges on its rows.

**Streaming.** A plan may yield several batches, cut wherever suits the
store, even inside a series. They must share one schema, so the store has
to know the label-name union before the first batch. That is why `encode`
takes the names as a parameter instead of deriving them.

## Non-goals for this stage

The basics first: floats, one label encoding. Each of these
is deferred on purpose, not forgotten.

- **Native histograms, exemplars, metadata.** Floats only. A per-sample
  histogram needs its own encoding, which is left open, and every
  kernel would have to branch on it. That is a stacked change once the
  float path matches Prometheus. The shape leaves room: a third,
  per-sample nullable field in the sample struct, or a separate list
  column, both keep the chunk row.
- **A series crossing partitions.** Chunks of a series stream across batches,
  but never across DataFusion partitions: that would turn the selector into a
  distributed merge, and the hash partitioning in the fourth obligation exists
  to keep it out.
- **Downsampling and pre-aggregation.** The hints let a store answer from
  downsampled data when the step allows it, but nothing here specifies
  how, and the engine does not ask for it.
- **Dictionary-encoded labels.** A series carries each label value
  exactly once, so within a series a dictionary has nothing to encode and
  a view is simply the value. A value only recurs across the series of a
  batch, where a dictionary would save 12 bytes per label per series and
  nothing else, since DataFusion hydrates its keys back to strings at the
  first group-by. It stays a store-side optimisation for when label memory
  dominates, which only instant queries over very many series come near.
