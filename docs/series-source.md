# The `SeriesSource` trait

*Status: draft for discussion. Companion to #4 (table providers).*

Every PromQL-on-something implementation ends up writing the thing
Prometheus calls `storage.Querier.Select`: the series matching these
matchers, in this time range. Each of us has a different store behind it:
TSDB blocks, Parquet, Vortex, Great Lakes, Iceberg, ClickHouse, remote read.
For one shared engine we have to agree on exactly two things, and this document
proposes both:

1. **The trait a store implements.** `SeriesSource`, one method.
2. **The Arrow shape its data comes back in.** One row per series. A
   `RecordBatch` in this shape is a *series batch*.

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

**Three obligations.** The engine assumes them instead of checking per
sample; `series::validate` checks only the schema.

1. *Filter.* Every series matches every matcher; every sample lies inside
   the range.
2. *Partition.* A row holds samples of exactly one series, and within a
   batch a series has one row. Today a series is whole in that row;
   splitting a very long series across batches is left open, see the
   non-goals.
3. *Order.* Samples ascend by timestamp within a series.

Matcher semantics are Prometheus's: a label a series lacks compares as
`""`, so `k!="v"` and `k=~".*"` match a series without `k` and `k=~".+"`
does not, and a regex is anchored to the whole value. `matcher.rs` holds
these rules in plain Rust. The in-memory source applies them directly, a
real store translates them into its own predicates, and the differential
tests against Prometheus decide whether it got that right.

## The series batch

```text
labels   Struct<{name}: Utf8View, …>    one field per label name, sorted
samples  List<Struct<timestamp: Timestamp(ms), value: Float64>>
```

One row is one series. Nothing is nullable.

A series is identified by its label set: one value per label name, and
the set never changes for the life of the series. Three series that
differ only in `pod` are three rows, each with its complete label set and
its own samples. A batch is many such rows side by side and nothing more:
rows share only the schema, the union of their label names, and two rows
with one label set are refused as one series split in two. Merging series
by the labels that survive an aggregation is an operator above this shape,
not part of it.

| Choice | Why |
|---|---|
| A row is a series, not a sample | Every PromQL operator works on one series' samples in order: lookback, staleness, `rate`, counter resets. With rows as samples, the first thing each operator does is find the series again. With rows as series, a series is a zero-copy slice, operators are kernels over slices, and parallelism over rows needs no shuffle. The regrouping has to happen somewhere; once, in the store that knows its own series boundaries, beats once per operator. |
| Labels are a struct with a field per name | The schema is the union of the selection's label names, so `by (route)` is a column reference and DataFusion's own grouping, sorting and `EXPLAIN` understand it. Fields are sorted so two producers build one schema. |
| An absent label is `""`, not NULL | That is PromQL's semantics, and non-nullable children remove the `get_field`-under-a-NULL-parent trap: there is no NULL parent to read a phantom value through. |
| `Utf8View` at the leaf | One 16-byte view per label per series, values up to 12 bytes inline, equality decided from length and prefix before any buffer is read. It is what Vortex and DataFusion's Parquet reader hand over, and the only string type with DataFusion's group-by fast path. A dictionary would encode nothing within a series, where each value appears once, and the engine cannot use batch-level keys: DataFusion hydrates them at the first group-by. |
| Samples are one list of structs | One offsets buffer, so a timestamp and its value cannot drift apart. Equal lengths and positional alignment are structure, not a promise two parallel lists would have to keep. |
| Milliseconds, `Float64` | Prometheus's units. `Float64` carries StaleNaN as the exact bit pattern `0x7ff0_0000_0000_0002`; nothing may cast through `f64::NAN`. |
| `List`, not `LargeList` | i32 offsets cap the samples in one batch at 2³¹ − 1, about 2.1 billion. A single series scraped every 5 s reaches that after roughly 340 years, and after 68 years at 1 s, so no series comes close; a batch of many series is bounded by the store's batch size far below it. `encode` errors rather than overflowing. |
| No `min_time`, `max_time` or step | The first two are the first and last sample of a sorted series, and the hints already bound the range; the step is the engine's. A redundant field is one a store can get out of sync with the data. |

**In code.** `series.rs` is the one definition of the shape: `schema(names)`,
`validate(&schema)`, `label_names(&schema)`. `Series` is one row, built
from plain values with `Series::new(labels, timestamps, values)`, which
checks the lengths, the order and duplicate names once. `encode(names,
&[Series])` puts rows into a batch and `decode(&[RecordBatch])` slices
them back out without copying. A store that already has Arrow (Parquet,
Vortex) builds the batch directly and only needs `validate`; a store that
has rows builds `Series` and calls `encode`.

## Implementing it

**In memory** (`memory.rs`, in this PR). Filter the stored series with
`matcher.rs`, clip each to the range with two binary searches and a slice,
`encode`, return DataFusion's memory exec. It is the reference: what
exactly a store promises, in a hundred lines, and what the tests run
against.

**A columnar store** (Parquet, Vortex, a lakehouse). Push the matchers
into the scan as predicates and the range into row-group or chunk pruning.
If the store keeps a sample per row, a group-by over the label columns
with an ordered `array_agg` produces the shape. If it already keeps chunks
of one series per row, such as 2h blocks, concatenate the chunks of a
series and clip. Either way the store does the regrouping, because it
knows its own series boundaries and the engine does not.

**A remote store** (Prometheus remote read, a gRPC service). Decode the
response into `Series`, `encode`.

**Streaming.** A plan may yield several batches, and they must share one
schema, so the store has to know the label-name union before the first
batch. That is why `encode` takes the names as a parameter instead of
deriving them.

## Relation to #4

#4 proposes a virtual table per metric, one sample per row and a column
per label, as the layout PromQL executes over. This document does not fix
a table layout at all. It fixes what a store hands the engine, one row per
series, and leaves how the store gets there to the store. A store built
along #4's lines implements `select` by regrouping its rows into series
once, at the boundary, where it knows its own series. Range functions
then run over one series' samples instead of as window functions over
sample rows, and the engine never has to rediscover series in a sample
table.

## This PR, and what stacks on it

In this PR: this document, the trait and its hints, the schema and its
helpers, the matcher reference and the in-memory source. Nothing evaluates
PromQL yet.

Stacked next, already prototyped an engine matching
Prometheus on all 57 upstream range-query test cases whose expressions it
implements: planning a selector over the store's plan plus the
instant-vector kernel (lookback, staleness, step, `offset`, `@`);
aggregations as a user-defined aggregate over the samples list; range
functions (`rate`, `increase`, `*_over_time`) as scalar functions over
it. Each keeps the shape. An operator's output is again one row per
series with a list of samples, now at step timestamps, which is what lets
operators stack.

This was implemented to show that the `SeriesSource` trait and the decisions
made around it are actually decent and work with the overall
engine implementation.

## Non-goals for this stage

The basics first: floats, whole series, one label encoding. Each of these
is deferred on purpose, not forgotten.

- **Native histograms, exemplars, metadata.** Floats only. A per-sample
  histogram needs its own encoding, which #4 leaves open too, and every
  kernel would have to branch on it. That is a stacked change once the
  float path matches Prometheus. The shape leaves room: a third,
  per-sample nullable field in the sample struct, or a separate list
  column, both keep one row per series.
- **Splitting a series across batches.** Today a store returns each series
  whole in one row, and the engine helpers assume it. Streaming a very
  long series as time-ordered chunks, so that a store need not hold or
  wait for all of it, is an optimisation for when series are large enough
  to hurt. The shape allows it later: rows with one label set in
  consecutive batches, in order and without overlap, and merging them
  would be the engine's job, not the store's.
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
