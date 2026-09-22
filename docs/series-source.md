# The `SeriesSource` trait

*Status: draft for discussion. An alternative to #4 (table providers).*

Every PromQL-on-something implementation ends up writing the thing
Prometheus calls `storage.Querier.Select`: the series matching these
matchers, in this time range. Each of us has a different store behind it:
TSDB blocks, Parquet, Vortex, Great Lakes, Iceberg, ClickHouse, remote read.
For one shared engine we have to agree on exactly two things, and this document
proposes both:

1. **The trait a store implements.** `SeriesSource`, one method.
2. **The Arrow shape its data comes back in.** One row per chunk of one
   series, rows of a series consecutive. A `RecordBatch` in this shape is a
   *series batch*, and its boundaries mean nothing: a series' chunk rows may
   be split across any number of RecordBatches.

Everything PromQL, lookback, staleness, the step grid, `offset`, `@`,
functions and aggregations, happens in the engine and is not the store's
business. Agreeing on the trait and the shape first lets the engine be
built in stacked PRs on top of them, and every store in parallel.

## The `SeriesSource` trait

```rust
#[async_trait]
pub trait SeriesSource: Debug + Send + Sync {
    /// Every series matching all of `matchers`, carrying only the samples
    /// within `hints`, as a plan whose schema passes `series::validate`.
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Prometheus's `storage.SelectHints`, typed.
pub struct SelectHints {
    pub start_ms: i64,              // inclusive; binding
    pub end_ms: i64,                // inclusive; binding
    pub step_ms: Option<i64>,       // step of the range query; None for instant
    pub range_ms: Option<i64>,      // window of a range selector, `[5m]`
    pub func: Option<String>,       // function or aggregation directly above
    pub grouping: Option<Grouping>, // `by`/`without` labels directly above
    pub shard: Option<Shard>,       // return shard `index` of `count` only
}

pub struct Grouping { pub labels: Vec<String>, pub by: bool }
pub struct Shard { pub index: u64, pub count: u64 }
```

**What the engine asks.** The selector's label matchers exactly as parsed,
including `__name__` synthesised from a bare metric name, and the hints.
Only the matchers and the inclusive millisecond range are binding. The
range already accounts for lookback, `offset`, `@` and range-vector
windows, so a store needs no PromQL knowledge to honour it.

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

**When.** Once per selector, at plan time, as Prometheus expands every
selector's series before evaluating. The schema, meaning the label names,
is only known once the store has found the series, and DataFusion needs
it before it can plan above it. The store's own planning therefore happens
while the engine plans, not while it executes. This is deliberate: a
selection's schema is fixed at plan time, and label names that only turn
up during execution are not supported.

**Four obligations.** `series::validate` checks only the schema; the
ordering is checked per row, see *Violations*.

1. *Filter.* Every series matches every matcher. Chunks may reach outside
   the range, see *Clipping* below, but a row whose whole span misses it
   should not be sent.
2. *Rows.* A chunk row holds one chunk of exactly one series and at least
   one sample, samples ascending by timestamp.
3. *Order.* Stated over a partition's whole stream of rows, never over a
   batch. A series may end mid-batch or span any number of RecordBatches;
   nothing requires it to be whole in one, because the engine does not look
   chunks up but folds rows as they arrive. Three properties:
   - series label-sorted across the stream, without any replica labels the
     store strips;
   - rows of one series consecutive and never crossing a partition: once a
     row with a different label set appears the previous series is closed,
     and no later row may carry its label set;
   - rows of one series ascending by first sample timestamp.

   The first two, the partition bound aside, are Thanos's `Store.Series`
   frame rule (`pkg/store/storepb/rpc.proto:29-32`, `:107-108` for replica labels):
   "a single frame can contain partition of the single series, but once a
   new series is started to be streamed it means that no more data will be
   sent for previous one. Series has to be sorted." The third is ours: the
   Store API has "no requirements on chunk sorting" (`rpc.proto:34`) and its
   proxy sorts chunks itself (`pkg/store/proxy_merge.go:180-182`), so a
   store adapted from it sorts each series' chunk metas before emitting
   rows, touching metas, not samples.
4. *Declared.* The returned plan declares that ordering as its output
   ordering, `(labels, first sample timestamp)`, and, when it has more than
   one partition, hash partitioning on `labels`. The engine refuses a plan
   that loses it, because the whole memory bound rests on DataFusion being
   able to prove it, see [`engine.md`](engine.md).

**Violations.** The engine never sorts or buffers to repair order. It always
checks, one comparison per row: labels non-decreasing between adjacent rows
of a partition, batch boundaries included, which also catches a closed
series reappearing, and first sample timestamp non-decreasing within a
series. A violation is a query error, where upstream the same mistake is a
silent wrong result. An empty row, though forbidden, is skipped.

**Overlap.** Adjacent rows of one series may overlap in time, which is normal
once blocks come from different stores or compaction levels. The engine skips
samples at or before the last timestamp it has already seen for that series,
so the first row wins. That is Thanos's `chunkSeriesIterator`, which does the
same with `Seek(lastT + 1)` (`pkg/query/iter.go:277-281`). Prometheus's block
merge keeps the union instead; whether to follow it is open, see
[`engine.md`](engine.md). This rule is not replica deduplication: picking
between two replicas weighs staleness and penalties, and stays a plan node of
its own above the selector.

**Clipping.** A chunk may span beyond the requested range, as
`rpc.proto:37-38` allows, and the engine discards samples outside it. Rows
that fall entirely outside should be pruned by the store, on its own chunk
index and before a row exists: Thanos's `LoadSeriesForTime`
(`pkg/store/bucket.go`) and Prometheus's chunk metadata filter
(`tsdb/querier.go:559-565`) both prune on chunk min and max time they
already keep, so the engine needs no column for it.

A store that keeps whole series is not asked to change anything. It sends one
row per series, which is the one-chunk case of this contract.

Matcher semantics are Prometheus's: a label a series lacks compares as
`""`, so `k!="v"` and `k=~".*"` match a series without `k` and `k=~".+"`
does not, and a regex is anchored to the whole value. `matcher.rs` holds
these rules in plain Rust. The in-memory source applies them directly, a
real store translates them into its own predicates, and the differential
tests against Prometheus decide whether it got that right.

## The series batch

```text
labels    Struct<{name}: Utf8View, …>    one field per label name, sorted
samples   List<Struct<timestamp: Timestamp(ms), value: Float64>>
```

One row is one chunk of one series. Nothing is nullable.

A series is identified by its label set: one value per label name, and
the set never changes for the life of the series. Three series that
differ only in `pod` are three rows, each with its complete label set and
its own samples. A batch is many such rows side by side and nothing more:
rows share only the schema and the union of their label names. Two rows with
the same label set are two chunks of that series and must be consecutive in
the partition's stream, whether or not a batch boundary falls between them;
the same label set reappearing after another one has started is refused.
Merging series by the labels that survive an aggregation is an operator
above this shape, not part of it.

| Choice | Why |
|---|---|
| A row is a chunk of one series, not a sample | Every PromQL operator works on one series' samples in order: lookback, staleness, `rate`, counter resets. With rows as samples, the first thing each operator does is find the series again. With chunk rows, a chunk is a zero-copy slice, a series is a run of consecutive rows folded in order, and parallelism over partitions needs no shuffle because no series crosses one. The regrouping has to happen somewhere; once, in the store that knows its own series boundaries and emits chunks as it keeps them, beats once per operator. |
| Rows of a series may span batches | The store is not asked to size batches around series: it cuts them where its scan or memory budget says, and a series continues into the next batch. The engine carries only the lane state of the one open series per partition across a boundary, so its memory does not depend on where batches end. |
| Labels are a struct with a field per name | The schema is the union of the selection's label names, so `by (route)` is a column reference and DataFusion's own grouping, sorting and `EXPLAIN` understand it. Fields are sorted so two producers build one schema. |
| An absent label is `""`, not NULL | That is PromQL's semantics, and non-nullable children remove the `get_field`-under-a-NULL-parent trap: there is no NULL parent to read a phantom value through. |
| `Utf8View` at the leaf | One 16-byte view per label per series, values up to 12 bytes inline, equality decided from length and prefix before any buffer is read. It is what Vortex and DataFusion's Parquet reader hand over, and the only string type with DataFusion's group-by fast path. A dictionary would encode nothing within a series, where each value appears once, and the engine cannot use batch-level keys: DataFusion hydrates them at the first group-by. |
| Samples are one list of structs | One offsets buffer, so a timestamp and its value cannot drift apart. Equal lengths and positional alignment are structure, not a promise two parallel lists would have to keep. |
| Milliseconds, `Float64` | Prometheus's units. `Float64` carries StaleNaN as the exact bit pattern `0x7ff0_0000_0000_0002`; nothing may cast through `f64::NAN`. |
| `List`, not `LargeList` | i32 offsets cap the samples in one batch at 2³¹ − 1, about 2.1 billion. Even a whole-series row scraped every 5 s reaches that only after roughly 340 years, and after 68 years at 1 s; a series of many chunk rows may be split across batches anyway, and a batch is bounded by the store's batch size far below the cap. `encode` errors rather than overflowing. |
| No time-bound columns, and no step | Samples ascend within a row, so the first and last sample timestamps are `timestamp[offsets[i]]` and `timestamp[offsets[i+1] - 1]`, two O(1) reads. A row that cannot reach the first step is still skipped without walking its samples, which is what Prometheus cannot do when it decodes through a chunk to find out (`tsdb/querier.go:779-791`). Columns carrying the same two values would be a second sort key that can disagree with the data. The step stays the engine's; the hints already bound the range. |

**In code.** `series.rs` is the one definition of the shape: `schema(names)`,
`validate(&schema)`, `label_names(&schema)`. `Series` is one chunk row,
built from plain values with `Series::new(labels, timestamps, values)`, which
checks the lengths, the sample order and duplicate names once; a series of
several chunks is several `Series` with the same labels. `encode(names,
&[Series])` puts rows into a batch and refuses rows that break *Order*;
`decode(&[RecordBatch])` slices them back out without copying, one `Series`
per chunk row. A store that already has Arrow (Parquet, Vortex) builds the
batch directly and only needs `validate`; a store that has rows builds
`Series` and calls `encode`.

## Implementing it

**In memory** (`memory.rs`). Filter the stored series with `matcher.rs`,
clip each to the range with two binary searches and a slice, cut it into
chunk rows, `encode` them label-sorted, return DataFusion's memory source
with the ordering declared. One row per series by default; the tests also
cut realistic chunks and one sample per chunk, so every kernel is run
across row boundaries. It is the reference: what exactly a store promises,
and what the tests run against.

**A columnar store** (Parquet, Vortex, a lakehouse). Push the matchers
into the scan as predicates and the range into row-group or chunk pruning.
If the store keeps a sample per row, a group-by over the label columns
with an ordered `array_agg` produces the shape. If it already keeps chunks
of one series per row, such as 2h blocks, it emits them as they are, sorted
by `(labels, first sample timestamp)`, and concatenates nothing. Either way
the store does the regrouping, because it knows its own series boundaries
and the engine does not.

**A remote store** (Prometheus remote read, a gRPC service). Decode each
chunk into a `Series`, sort a series' chunks by first sample timestamp where
the protocol does not promise it, `encode`.

**Streaming.** A plan may yield several batches, cut wherever suits the
store, even inside a series. They must share one schema, so the store has
to know the label-name union before the first batch. That is why `encode`
takes the names as a parameter instead of deriving them.

## Relation to #4

#4 proposes a virtual table per metric, one sample per row and a column
per label, as the layout PromQL executes over. This document does not fix
a table layout at all. It fixes what a store hands the engine, chunk rows
of one series consecutive, and leaves how the store gets there to the
store. A store built along #4's lines implements `select` by regrouping its
rows into series once, at the boundary, where it knows its own series, and
the engine never has to rediscover series in a sample table. How the
engine folds chunk rows instead of running window functions over sample
rows, and why the selector and range functions are grouped aggregates
over chunk rows rather than scalar functions over one row, is
[`engine.md`](engine.md).

## Non-goals for this stage

The basics first: floats, one label encoding. Each of these
is deferred on purpose, not forgotten.

- **Native histograms, exemplars, metadata.** Floats only. A per-sample
  histogram needs its own encoding, which #4 leaves open too, and every
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
