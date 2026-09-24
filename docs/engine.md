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
decoded points. The Thanos engine bounds evaluation by series-per-batch times
window with its ring buffer, which is the same bound written as an operator.

DataFusion gives us streams of `RecordBatch`, which is the same discipline in
Arrow terms: an operator consumes batches and emits batches, and only what an
aggregation must hold is held. Our engine inherits the property from all
three. **The unit of materialisation is the window, never the series.**

## Chunk rows, and why the plan shape follows from them

The store emits one row per chunk of one series, rows of a series
consecutive, series sorted by label set. That contract is
`series-source.md`'s; what matters here is that it forbids the engine from
ever seeing a whole series at once, and therefore forbids an operator that
wants one.

The old design made the range functions scalar UDFs over the `samples` list.
A scalar UDF is handed one row and must produce that row's answer, so a
scalar `rate` can only be correct if the row already holds every sample of
the window, which is only true if the store concatenated the series first.
That is the structural reason the earlier contract demanded concatenation,
and the reason reviewers were right to call it prohibitively expensive: it
pushed an unbounded copy into every store to satisfy a signature.

The fix is to move the selector and the range functions from scalar UDFs to
**grouped aggregate UDFs over the labels column**. An aggregate sees rows of
one group in sequence and keeps state between them, which is exactly a
chunk-crossing iterator written as DataFusion. Each plan is then one
`TableScan` per selector over the store's own plan (`SelectorTable` in
`source.rs`), the selector or range aggregate directly above it, and, for
`sum by (…)` and friends, a second `AggregateExec` above that, unchanged
(`aggregate.rs`). The step grid the lower aggregate folds into comes from
`Params` (`params.rs`) and is fixed at plan time.

Because the scan declares its output ordered by the group key, DataFusion
runs that lower `AggregateExec` in `InputOrderMode::Sorted`
(`datafusion-physical-plan-54.1.0/src/aggregates/mod.rs:815-823`,
`order/full.rs:80-92`): a group is emitted the moment a row with a different
key arrives, and only the open group is held. Sorted mode is the whole
memory argument. Without the declared ordering the same aggregate would hold
every series of the scan at once, which is why *Ordering integrity* refuses
such a plan.

**Batch boundaries mean nothing.** A store may split a series' chunk rows
across any number of RecordBatches, ending it mid-batch or spreading it over
several. The engine never gathers or looks up the chunks a `rate` window
needs: it folds rows into the open series' window buffer in arrival order,
and closes the series at the first row with another label set or at the end
of the stream. There is no lookup, no join and no buffered batch. A whole-series
store sending one row per series is the one-chunk case.

## Kernel state

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
contiguous slice. A row that arrives with the buffer empty is walked in
place and only its tail is copied, unless it needs `@`'s pinned window
trimmed or carries a stale marker a range function must not see in place —
either copies the row whole into the buffer instead. A step is
evaluated the moment its window can no longer change, when its end is at
or before the last timestamp pushed, rather than at series close. The
buffer then drops what no later window reaches, once that is at least half
of it, as `ReduceDelta` (`storage/buffer.go:67-70`) shrinks Prometheus's.
Without the eager step a 30-day range at 15 s would buffer 172,800 samples
per series before answering the first one.

Over the buffer, `rate`, `increase`, `min_over_time` and `max_over_time`
carry a `Sweep` from step to step, the resets or the extremum candidates as
absolute sample ordinals, so a step costs the samples that entered and left
its window and trimming the front never rewrites them. Every other function
refolds its window through `range::evaluate`; a sliding variant measured
no faster for `changes`/`resets` and a no-op one slowed the whole family.
Order-dependent functions (`quantile_over_time`, `deriv`, `changes`, …)
need nothing more than a buffer already sorted by time.

The instant selector is the degenerate case. For each step it wants the last
sample at or before the step within the lookback delta, and a stale marker
that is that sample hides the series, so the selector keeps markers the
range functions filter on copy. `offset` and `@` shift the window before
clipping, which is why the first step of a fold can still be looking for a
sample that lives in an earlier row. Under `@` the window is pinned: what
falls outside it is dropped on copy and the one answer is repeated across
the grid. That, and overlap between adjacent chunks, is why rows must be
folded in the order they arrive and can never be folded independently and
combined.

## Memory

The bound is **O(series in flight × window)**, and series in flight is one
per partition under sorted mode. Nothing in the engine is proportional to a
series' length, its number of chunks, or how its rows fall across batches: at
a batch boundary the previous RecordBatch is released entirely and only the
open series' window buffer survives.

The engine copies samples, but only a window's worth: each row's samples
enter the buffer once and leave it when no step reads them. That is the
price of releasing batches, and it replaces the pass the range kernels pay
anyway to drop stale markers. What it avoids is `arrow::compute::concat`
over a series' rows, which copies the series; a copy of a window is cheap,
a concat of a series is the cost we just refused to push into the store.

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

**Contract.** The three properties are therefore requirements on the store,
not inherited guarantees: series label-sorted across the stream, without any
replica labels the store strips; rows of one series consecutive and never
crossing a partition; rows of one series ascending by first sample
timestamp. Sorted mode needs the first two to close a group at the next key:
because rows of a series are consecutive across a partition's whole stream,
the first foreign row closes it without the engine knowing where batches end.
Without the third the overlap rule drops an earlier row's samples. A store
adapted from the Store API sorts each series' chunk metas before emitting
rows, as the proxy does, touching metas, not samples.

**Checks.** The engine never sorts or buffers to repair order; that is the
concatenation problem again. It always checks, one comparison per row:
labels non-decreasing between adjacent rows of a partition, which also
catches a closed series reappearing, and first sample timestamp
non-decreasing within a series. A violation is a query error.
`reject_same_labelset` sees final output only, so it misses
`sum(rate(x[5m]))` over a split series. The engine also refuses a plan that
loses the declared `labels` ordering or keeps a series from staying whole
in one partition, which `CoalescePartitionsExec` and `RepartitionExec`
without `preserve_order` do between scan and selector.

**Session flags.** The engine turns off the three DataFusion rewrites that
deal one series' rows to several partitions: round-robin repartitioning
(inserted under a partial aggregate,
`datafusion-physical-optimizer-54.1.0/src/enforce_distribution.rs:1309-1314`),
file-scan repartitioning, and hash repartitioning of aggregations.
`check_selector_plans` (`engine.rs`) then refuses, per plan, a selector
aggregate that is not Sorted over `SeriesSetExec`. Parallelism comes from
store partitions and `SelectHints.shard` instead. Hash repartitioning with
`prefer_existing_sort` would regain it for the upper aggregates, and keeps
both selector aggregates Sorted, but it is deferred: the plan then ends in
`target_partitions` partitions, series come back in arrival order, and
struct columns have no sort kernel to restore it.

**Overlap.** The first row wins: the engine skips samples at or before the
last timestamp seen for the series, as Thanos's `chunkSeriesIterator` does
(thanos `pkg/query/iter.go:277-281`). Prometheus's block merge keeps the
union and drops only duplicate timestamps (prom `storage/merge.go:653-656`).
The two agree on exact duplicates and differ only for interleaved
timestamps. A stale marker claims its timestamp
like any sample, before the range functions filter it out, because a
store's merge dedups before PromQL sees staleness: a marker in the first row
must not let a real sample at the same timestamp through from the second.

## The correctness net

`labelset::reject_same_labelset` (`labelset.rs`) still runs on the output,
but it only sees a split series whose labels survived to the output; the
per-row checks in *Ordering integrity* are what catch it before an
aggregation hides it.

Above it, the test suites run the same corpus against three source modes:
whole series in one row, realistic chunks, and one sample per chunk. The
third is the adversarial case, where every fold crosses a row boundary and
carry-over is exercised on every sample. All three must reach identical
promqltest gate counts. A kernel that is only correct when it sees a whole
window in one row fails the third mode immediately.

## Non-goals for this stage

- **Replica deduplication.** The overlap rule in *Ordering integrity*
  resolves chunks of one series, not replicas. Choosing between
  `prometheus_replica="a"` and `"b"` is a plan node above the selector, with
  its own penalty rules, and mixing the two would make the cheap rule
  silently decide a question it cannot see.
- **A series crossing partitions.** It would make the selector aggregate a
  distributed merge. The hash partitioning in the contract exists to keep it
  out.
- **Native histograms.** Every kernel here branches on `f64`. The shape leaves
  room, the kernels do not yet.
