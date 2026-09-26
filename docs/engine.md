# The engine

*Status: draft for discussion. Companion to [`series-source.md`](series-source.md),
which fixes what a store hands us; this document is what happens above it.*

A PromQL engine is judged by the query it cannot run. Correctness is table
stakes, and the differential tests settle it. What decides whether the engine
is usable is the query over a million series with a year of samples, where
the only interesting question is how much of it has to be in memory at once.
This document says how the engine keeps that bounded, and why the answer
falls out of the shape the store delivers rather than out of an operator we
bolt on later.

## Lineage

Prometheus never materialises a series. Evaluation is iterators all the way
down: `storage.BufferedSeriesIterator` keeps a ring buffer of exactly the
lookback delta (`storage/buffer.go:26-36`), `matrixIterSlice` is written
against timestamps and a reused sample slice rather than against a series
object (`promql/engine.go:2727-2886`), and below both,
`populateWithDelSeriesIterator` walks chunk after chunk so that a chunk
boundary is invisible to everything above it (`tsdb/querier.go:759-791`). The
memory an evaluation needs is a function of the window and the number of
series in flight, never of how long a series is.

Thanos keeps the same property one level up. Its proxy merges series from
many stores by chaining chunk descriptors, not samples
(`pkg/store/proxy_merge.go:145-188`), so a merge costs pointers rather than
decoded points. The Thanos engine writes Prometheus's buffer as an operator,
a ring buffer per series (promql-engine `ringbuffer/`), but opens one for
every selected series before the first step and keeps it to the end, which
its own design notes name the dominant memory weakness (promql-engine
`docs/architecture.md:198-213`).

DataFusion gives us streams of `RecordBatch`, which is the same discipline in
Arrow terms: an operator consumes batches and emits batches, and only what an
aggregation must hold is held. Our engine takes the property from Prometheus
and the proxy, and its form from DataFusion. **The unit of materialisation is
the window, never the series.**

## Chunk rows, and why the plan shape follows from them

Each selector gets one select, and the store answers it with one row per
chunk of one series, cut into blocks of its own layout that every row
declares; inside a block series are label-sorted with rows of a series
consecutive, or keyed by a store-provided series id. That contract is
`series-source.md`'s; what matters here is that it forbids the engine from
ever seeing a whole series at once, and therefore forbids an operator that
wants one.

A scalar UDF over the `samples` list cannot be a range function under it. A
scalar UDF is handed one row and must produce that row's answer, so a scalar
`rate` is correct only if the row already holds every sample of the window,
which is only true if the store concatenated the series first: an unbounded
copy pushed into every store to satisfy a signature, the memory problem the
contract exists to avoid.

The selector and the range functions are therefore **grouped aggregate UDFs
over the series of a block**, `(block_start, labels)` in sorted mode and
`(block_start, series_id)` in keyed mode. An aggregate sees rows of one
group in sequence and keeps state between them, which is exactly a
chunk-crossing iterator written as DataFusion. The query's plan is then one
`TableScan` per selector over the store's plan for that selector
(`SelectorTable` in `source.rs`), the selector or range aggregate directly
above it, and, for `sum by (…)` and friends, a second `AggregateExec` above
that (`aggregate.rs`), see *Session flags*. The steps the lower aggregate
folds into come from `Params` (`params.rs`), cut per block to the steps it
answers.

**Blocks.** The store cuts time. The engine issues one select per selector
over the whole query range, its start widened by the window and both ends
moved by the offset as Prometheus widens its select hints, and passes the
window as `SelectHints.window_ms`. The store cuts its answer into blocks of
its own layout and stamps every chunk row with its block's `block_start`
and `block_end`, reaching back by the window at each
([`series-source.md`](series-source.md#the-seriessource-trait)). A block
answers the steps of the query whose window end, `t − offset` or the `@`
time, lies in `[block_start, block_end)`; its samples before `block_start`
feed windows only. The engine learns a block's edges from its first row,
before any series closes in it, so a series that closes inside a block evaluates
all its steps for the block at once, and nothing per series survives the
block. An aggregate above holds one partial per group and step of the
block and emits when the block's last row has passed, at the next block's
first row or the end of the stream, and block results concatenate with no
merge between them. Every partition of one store cuts the same edges, the
store's obligation and trivially kept, since the blocks are its layout. Two
selectors may see different edges, from different stores or through
different offsets; an operator joining them matches by step, holding a
step until both sides have answered it, which bounds what it holds by one
block of each side.

In sorted mode the scan declares its output ordered by the group key, and
DataFusion runs that lower `AggregateExec` in `InputOrderMode::Sorted`
(`datafusion-physical-plan-54.1.0/src/aggregates/mod.rs:815-823`,
`order/full.rs:80-92`): a group is emitted the moment a row with a different
key arrives, and only the open group is held. In keyed mode the scan
declares only `block_start` ordered, and the same aggregate runs as a hash
aggregate keyed by `(block_start, series_id)` in
`InputOrderMode::PartiallySorted`, every series of the block open until the
block's last row has passed. The choice between them is the plan's, made
from the scan's schema, not a mapping trait every operator implements.
Losing a declared ordering between scan and selector would turn a
sorted-mode plan into a hash aggregate over labels, which is why *Ordering
integrity* refuses it.

**Batch boundaries mean nothing.** A store may split a series' chunk rows
across any number of RecordBatches, ending it mid-batch or spreading it over
several. The engine never gathers or looks up the chunks a `rate` window
needs: it folds rows into their series' window buffer in arrival order. In
sorted mode it closes the series at the first row with another label set,
in keyed mode when the block's last row has passed, as it does for the last
series of a block in sorted mode. There is no lookup, no join and no
buffered batch. A whole-series store sending the range as one block, one
row per series, is the one-block, one-chunk case.

## Kernel state

A **kernel** is the per-series function that reduces one step's window, a
slice of the window buffer, to that step's value: `range::evaluate` for a
`Func` over a `Window` (`range.rs`), or the instant selector's pick of the
last sample. The name is Arrow's: like the `arrow::compute` kernels, it takes
slices and returns values, and whatever carries from one step to the next is
the state this section is about. The lane kernels of `aggregate.rs`, which
combine per-step partials, are a different thing.

Semantically each step is a lane: the samples in `(t − range, t]`, reduced
by the function. Keeping a lane per step as the data structure would pay
every sample once per window it falls in, twenty times for `[5m]` at 15 s,
which is the refold the sweep in `range.rs` exists to remove. So the state
per open series is **one window buffer**, `BufferedSeriesIterator`
(`buffer.rs`), after Prometheus's `storage.BufferedSeriesIterator`
(`storage/buffer.go:26-36`), and it serves every function alike.

What a later step still reads of a pushed chunk row is copied into the
buffer, not held as a slice of its batch: a retained slice pins the batch's
child buffers, other series' samples included, and every kernel indexes one
contiguous slice. Usually only a row's tail need be copied; a full copy is
forced when `@`'s pinned window needs trimming, or when a stale marker must
survive the copy, since a range function must not see one in place. A step is
evaluated the moment its window cannot change any more, when its end is at
or before the last timestamp pushed, rather than at series close, and when
the series closes every step of the block still open is evaluated, since
the block's end came with its first row. Only the block's own steps are
evaluated, so the reach-back before its start feeds windows and never
answers one. The buffer
then drops what no later window reaches, once that is at least half of it,
as `ReduceDelta` (`storage/buffer.go:67-70`) shrinks Prometheus's. Without
the eager step a block would be buffered whole before its first answer:
172,800 samples per series for a store that sends 30 days at 15 s as one
block.

Over the buffer, `rate`, `increase`, `min_over_time` and `max_over_time`
carry a `Sweep` from step to step, the resets or the extremum candidates as
absolute sample ordinals, so a step costs the samples that entered and left
its window and trimming the front never rewrites them. Every other function
refolds its window through `range::evaluate`. Order-dependent functions
(`quantile_over_time`, `deriv`, `changes`, …) need nothing more than a
buffer already sorted by time.

**Above the selector.** `rate` keeps no lane per step: a step's value leaves
the moment it is final. Operators above the selector do hold state, and the
block bounds it. `sum by (…)` keeps one partial per group and step of the
block, and emits them when the block's last row has passed, since no other
block answers those steps. The bound is groups × steps per block, four
for a 2 h block at a 30-minute step, where holding every step of the range
would be groups × 1,441 for 30 days at that step; the result row a range aggregate
hands up per series is one block's steps long.
[`engine-chunks.md`](engine-chunks.md#4-an-aggregate-across-a-block-edge)
walks one edge with numbers.

The instant selector is the degenerate case. For each step it wants the last
sample at or before the step within the lookback delta, and a stale marker
that is that sample hides the series, so the selector keeps markers the
range functions filter on copy. `offset` and `@` shift the window back in
time, which is why the first step of a fold can still be looking for a
sample that lives in an earlier row. A step's window ends at `t − offset`,
or at the `@` time, and that end decides which block answers the step. Under
`@` every step shares one pinned window, so one block answers every step:
what falls outside the window is dropped on copy and the one answer is
repeated across the steps. That, and overlap between adjacent chunks, is why
rows must be folded in the order they arrive and can never be folded
independently and combined.

## Memory

The bound is **O(series in flight × window)** in the selector, and series in
flight is one per partition in sorted mode, plus **O(groups × steps per
block)** in an aggregate above it. In sorted mode nothing in the engine is
proportional to the number of series, a series' length, its number of
chunks, or how its rows fall across batches: at a batch boundary the finished
RecordBatch is released entirely and only the open series' window buffer
survives, and at a block edge nothing per series survives. Keyed mode keeps
every series of the block in flight, each with its window buffer and one
block's steps, so its state is on the order of one block of the selection
plus N · (240 B slot + 16 B per step of the block): about 38 MB for 10k
series at 15 s in a one-hour block and 384 MB at 100k
`[assumed 16 B samples]` before that per-series term, which varies per
query. The block edge still drops all of it.

The engine copies samples, but only a window's worth: each row's samples
enter the buffer once and leave it when no step reads them. That is the
price of releasing batches, and it replaces the pass the range kernels pay
anyway to drop stale markers. What it avoids is `arrow::compute::concat`
over a series' rows, which copies the series; a copy of a window is cheap,
a concat of a series is the cost the contract refuses to push into the store.

The offsets pay for themselves here. Samples ascend within a row, so its
first sample timestamp is `timestamp[offsets[i]]` and its last sample
timestamp is `timestamp[offsets[i+1] - 1]`, two O(1) reads into the child
buffer. A row whose last sample timestamp is before the first step's window
is skipped without walking its samples, where Prometheus decodes through the
chunk to find out (`tsdb/querier.go:779-791`). The one row to guard is an
empty one, `offsets[i] == offsets[i+1]`, where both reads land outside the
row. The contract forbids it; the engine skips one anyway, since it has
nothing to fold.

## Ordering integrity

**Upstream.** Prometheus has nothing to group: a `ChunkSeries` owns its
chunks (prom `tsdb/querier.go:1139-1146`), chunk time order is an error at
block write time (prom `tsdb/index/index.go:454-455`), and label order is
produced on request and trusted by every merge (prom
`storage/merge.go:331-332`). Only on the wire does a series span
consecutive frames, rebuilt by adjacent label equality. The Store API
requires sorted series but has "no requirements on chunk sorting" (thanos
`pkg/store/storepb/rpc.proto:29-34`); the proxy sorts chunks itself (thanos
`pkg/store/proxy_merge.go:127-182`). No reader checks any of it, so a
misbehaving store makes a series come out twice, unreported.

**Contract.** The properties are therefore requirements on the store, not
inherited guarantees. Inside each block the store delivers one of two
modes, keyed mode when its schema carries a `series_id` column and sorted
mode otherwise ([`series-source.md`](series-source.md#order)): in sorted
mode series are sorted by their label set within a block and rows of one
series are consecutive; in keyed mode every row carries the store's opaque
`series_id` and rows of one series may lie in any order relative to other
series of the block. In both modes every row carries its block's edges,
blocks come in order, rows of one series never cross a partition and ascend
by first sample timestamp within a block, batches may be cut anywhere, and
the lookback and the steps a block answers are the same. Sorted mode needs
label order and consecutiveness to close a group at the next key: the first
foreign row closes it without the engine knowing where batches end. Without
the time order the overlap rule drops an earlier row's samples. A store
adapted from the Store API sorts each series' chunk metas before emitting
rows, as the proxy does, touching metas, not samples. The store learns the
lookback from `SelectHints.window_ms` and includes it at every block it
cuts; where it finds those samples across its own files is its business.

**Why blocks.** Every order a store could promise puts a cost somewhere.
The figures here are estimates from comparing the query shapes below, not
benchmarks. The benchmark that would settle it: the same five query shapes
run against label order over the whole range, time order over the whole
range, blocks in sorted mode and blocks in keyed mode, over 30 days at
10k and 100k series, measuring peak resident memory and time to first
step. Label order across the
whole range dictates storage layout: a store that keeps its data by time
cannot stream it without a sort that holds the whole selection, 29 GB for
10k series over 30 days and 289 GB for 100k. Time order across all series
moves the cost into the engine, one window buffer per series in flight,
which thanos promql-engine documents as its dominant memory weakness
(`docs/architecture.md:198-213` there); at a 30-minute step that is 14 to
9,000 times more state than label order for four of five query shapes.
Label order inside blocks is native to TSDB blocks and to a Store API
request narrowed to one block; a columnar store partitioned by hour could
sort one hour at a time. Blocks keep the engine free of any term in the
number of series and emit each block's steps as the block ends. The store pays by
reading each block's reach-back again, window over block of the stream: 4%
for `[5m]` and 50% for `[1h]` over 2 h blocks, 8% and 100% over 1 h blocks,
and more where it reads whole chunks. Keyed mode is admissible only because
the block bounds it: its hash aggregate holds every series of one block and
drops them at the edge, where over a whole-range stream it would hold every
series with every step of the range until the stream ended.

**Checks.** The engine never sorts or buffers rows to repair order; that is
the concatenation problem again. It always checks, one label comparison per
row. In sorted mode that is labels non-decreasing against the previous row
of the block in a partition's stream, which also catches a closed series
reappearing. In keyed mode the row's labels must equal those its `series_id`
bucket was opened with, as Prometheus's head confirms every `seriesHashmap`
hit with `labels.Equal` (prom `tsdb/head.go:2332-2362`) rather than trusting
a bare hash the way its PromQL engine's vector matching does (prom
`promql/engine.go:3371-3381`); a mismatch is an id collision or a store bug.
In both, first sample timestamp is non-decreasing within a series and block.
The block columns are checked too: `block_start` and `block_end` constant
within a block, and blocks non-decreasing and non-overlapping across the
rows of a partition. A sample past either edge needs no check, since it
falls in no window the block answers. A violation is a query error. In keyed
mode the selector also checks label-set uniqueness across its groups when
the block ends, hashing each group's labels into a set on labels the group
table already holds; a duplicate means two ids for one label set, and the
block is refused. `reject_same_labelset` misses `sum(rate(x[5m]))` over a
split series because the labels do not survive the aggregation, as *The
correctness net* below states. The engine also refuses a sorted-mode plan
that loses the declared `labels` ordering, and any plan that keeps a series
from staying whole in one partition, which `CoalescePartitionsExec` and
`RepartitionExec` without `preserve_order` do between scan and selector.

**Session flags.** In sorted mode `SeriesSetExec` declares `Hash([labels],
n)` over the store's n partitions and the ordering `(block_start, labels,
first sample timestamp)` inside each; in keyed mode it declares
`Hash([series_id], n)` and the ordering `block_start`, and the selector
aggregate is planned as a hash aggregate on `(block_start, series_id)`, each
group holding the labels it was opened with for the check and the output.
That group table is keyed mode's whole memory shape: every series of the
block, its window buffer and one block's steps, and a probe per row. The
hash is what DataFusion means by the contract's "never crossing a
partition", and it is exactly what the selector aggregate needs. With
`repartition_aggregations` on, DataFusion finds that requirement already met
and plans the selector and range aggregates `SinglePartitioned`, Sorted in
sorted mode and PartiallySorted in keyed mode, one per store partition,
with no merge and no Final. Partitions meet only at the shuffle an upper
`sum by (…)` needs for its own key, `(block_start, …)` with `block_start`
leading, which moves one partial state per group and partition, not
samples. DataFusion keeps `preserve_order` on that repartition when it lets
the aggregate stream (`enforce_distribution.rs:941-967` in DataFusion 54),
so the Final runs `PartiallySorted` on the `block_start` prefix and emits a
block once every input partition has passed it, never holding more than
the open block's partials; `check_selector_plans` refuses a plan whose
Final would hold every block to the end. The engine also sets
`subset_repartition_threshold = 1`: below its default of four input
partitions, `add_hash_on_top`
(`datafusion-physical-optimizer-54.1.0/src/enforce_distribution.rs:897-902`)
re-hashes a satisfied input just to reach `target_partitions`, splitting the
selector back into Partial and FinalPartitioned; the threshold is
structural rather than a parallelism knob, since `Hash([labels])` already
satisfies the selector's `(block_start, labels)` key as a subset
(`partitioning.rs:252-256`). Round-robin and file-scan repartitioning stay
off, since a byte-range scan split cuts through a series; round-robin is
inserted under a partial aggregate (`enforce_distribution.rs:1309-1314`),
and only the Hash requirement keeps it above the selector, over finished
series. `check_selector_plans` (`engine.rs`) refuses, per plan, a selector
aggregate that is not Sorted over a sorted-mode `SeriesSetExec`, or not
PartiallySorted and grouped on `series_id` over a keyed-mode one.
Parallelism below the selector is the store's partition count and
`SelectHints.shard`, never `target_partitions`.

**Output order.** Results leave block by block, in block order, and inside
a block the plan's partitions finish in any order. A series' row carries
one block's steps ascending, so a series seen in 360 blocks leaves as 360
rows. Prometheus sorts a range query's matrix before returning it (prom
`promql/engine.go`, `sort.Sort(mat)`), and so does the engine, once, over
finished rows: by labels, then block, re-cut into slices of the collected
batches so that no sample is copied and a series' rows lie adjacent, steps
ascending. The price is batch count, about one per series and block, and
each row's labels: at 88 B per row, `rate(x[5m])` over 10k series and 30
days at 2 h blocks carries up to 317 MB of labels beside 231 MB of samples
at a 30-minute step, an estimate from the same query shapes above. The order is
`labels.Compare` over the labels a series has. The `labels` struct's own
order is not it: it compares field by field and an absent label is `""`,
so two series with different label names can sort the other way. A
`SortPreservingMergeExec` at the root would be that wrong order, and would
not survive the label rebuild above an aggregate anyway.

**Co-partitioning.** The declared hash is the store's bucketing, not
DataFusion's hash function, and DataFusion checks neither the function
nor the count (`datafusion-physical-expr-54.1.0/src/partitioning.rs:219-222`).
Aggregates and windows only need each key in one partition, never which
one. A partitioned join matches partition i with partition i and would
line a store's bucket up against DataFusion's, or another store's, and
drop matches silently; an `InterleaveExec`, which DataFusion builds from
a `UnionExec` whose children all declare the same partitioning, does the
same by zipping partition i of one child with partition i of another.
`check_selector_plans` therefore refuses a partitioned join or an
interleave with an input that still carries the declaration, looking
through `FilterExec`, `GlobalLimitExec`, `LocalLimitExec` and
`CoalesceBatchesExec` on the way down, since DataFusion reports each of
those as leaving its child's partitioning unchanged. Vector matching
will join on a rebuilt label key, which sheds it: `Partitioning::project`
turns a key the projection does not carry into an `UnKnownColumn`, which
equals nothing, itself included.

**Overlap.** One series' chunks may arrive from several files or
compaction levels of a store and overlap in time. The first row wins: the
engine skips samples at or before the last timestamp seen for the series in
the block, as Thanos's `chunkSeriesIterator` does (thanos
`pkg/query/iter.go:277-281`). Each block starts afresh, because its
reach-back repeats samples on purpose. Prometheus's block merge keeps the
union and drops only duplicate timestamps (prom `storage/merge.go:653-656`).
The two agree on exact duplicates and differ only for interleaved
timestamps. A stale marker claims its timestamp like any sample, before the
range functions filter it out, because a store's merge drops repeated
timestamps before PromQL sees staleness: a marker in the first row must not
let a real sample at the same timestamp through from the second.

## The correctness net

`labelset::reject_same_labelset` (`labelset.rs`) runs on each block's
output, where a label set may appear once; across blocks the same label set
is the same series continuing. It only sees a split series whose labels
survived to the output; the per-row checks in *Ordering integrity* are what
catch it before an aggregation hides it. In keyed mode, two ids with one
label set are caught earlier still, by the selector's own label-set
uniqueness check at the block edge (*Ordering integrity*, *Checks*), which
the per-row label confirmation cannot see on its own, since each id's
bucket agrees with itself.

Above it, the test suites run the same corpus against three source modes:
whole series in one row, realistic chunks, and one sample per chunk. The
third is the adversarial case, where every fold crosses a row boundary and
carry-over is exercised on every sample. All three must reach identical
promqltest gate counts, and each must also run cut into blocks, so every
step whose window straddles a block edge is answered from the lookback. A
kernel that is only correct when it sees a whole window in one row fails the
third mode immediately.

## Non-goals for this stage

- **A series crossing partitions.** It would make the selector aggregate a
  distributed merge. The hash partitioning in the contract exists to keep it
  out.
- **Native histograms.** Every kernel here branches on `f64`. The shape leaves
  room, the kernels do not yet.
