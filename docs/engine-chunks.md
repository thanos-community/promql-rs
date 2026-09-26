# How `rate` crosses a chunk boundary

promql-engine internals · companion to [engine.md](engine.md) and [series-source.md](series-source.md) · [illustrated version](engine-chunks.html) of the single-block fixture, with the step-through fold and the Arrow buffer inspector

If a store streams one Arrow row per chunk, and may cut its `RecordBatch`es anywhere, how are two chunks aligned so that `rate` can reach the last value of one and the first value of the next?

`rate(x[5m])` · step 30 s · 420 s → 600 s · two batches, three rows, two series

**They are not aligned.** The two chunk rows of `x` are ranges in two different `RecordBatch`es, read in arrival order, and the samples of the first that a later window still reaches are already copied into the series' **window buffer** when the second is read. Alignment is only a question if the operator works on whole series. A grouped aggregate does not.

## 1. The shape everything happens in

```
labels       Struct<__name__: Utf8View, ...>
samples      List<Struct<timestamp: Timestamp(Millisecond), value: Float64>>
block_start  Timestamp(Millisecond)
block_end    Timestamp(Millisecond)
```

One row is one chunk of one series in one block the store cut its answer into, and `block_start` and `block_end` name that block on every row. The fixture is one block, so the two columns are the same on every row and stay out of view until section 4. The fixture is three rows in two batches: batch A holds the first chunk of series `x`; batch B holds its second chunk, then one chunk of series `y`, which is what closes `x`. The counter resets inside B.0, from 16 to 3.

```
batch A
row  labels.__name__  samples                  first / last sample timestamp, derived
 0   "x"              offsets[0]..offsets[1]   timestamp[0] = 300   timestamp[4] = 420

  offsets    [0                  5]
              |                  |
  timestamp  [300 330 360 390 420]
  value      [ 10  11  12  13  14]
              \______ A.0 _______/
```

```
batch B
row  labels.__name__  samples                  first / last sample timestamp, derived
 0   "x"              offsets[0]..offsets[1]   timestamp[0] = 450   timestamp[5] = 600
 1   "y"              offsets[1]..offsets[2]   timestamp[6] = 300   timestamp[8] = 360

  offsets    [0                       6            9]
              |                       |            |
  timestamp  [450 480 510 540 570 600 | 300 330 360]
  value      [ 15  16   3   4   5   6 |   7   8   9]
              \________ B.0 _________/  \__ B.1 ___/
```

Batch B's offsets and child buffers start again at 0; no index in batch B reaches batch A.

## 2. Batches carry no meaning

> **A store may split a series' chunk rows across any number of `RecordBatch`es.** A series may end mid-batch or span several; `x` here does both. The engine never gathers a series' chunks for `rate`: it folds rows in arrival order into one window buffer, closes a series when a row with another label set arrives or the block's last row has passed, and carries across a batch boundary only the window buffer of the one open series per partition in sorted mode; in keyed mode it carries every series of the block.

What crosses from A.0 to B.0 is a copy of the samples a later window reaches, never a reference into batch A's buffers, and the batch boundary is not an event the fold notices. Why the engine never joins chunk rows into one is in [engine.md](engine.md#memory).

That B.1 closes `x` holds only because the store keeps a series' rows consecutive and label-sorted within a block, a contract in [engine.md](engine.md#ordering-integrity). That is the sorted mode the fold here shows; in keyed mode each row finds its series' buffer by `series_id` instead of by adjacency, and nothing else in the fold changes. The engine checks it on every row: one label comparison per row, labels non-decreasing between adjacent rows of a block, and cheaper checks beside it, first sample timestamp non-decreasing within a series, `block_start` and `block_end` constant within a block, and blocks ascending and non-overlapping from one row to the next, turning a violation into a query error rather than a silently wrong result. The samples need no edge check: a sample past either edge falls in no window the block answers.

## 3. The fold

`rate(x[5m])` from t = 420 s to t = 600 s at step 30 s. Seven steps, and each step's answer is a function of its **lane**, the samples in its window `(t_step − 300 s, t_step]`. The lanes are the semantics, not `rate`'s data structure: `rate` keeps no lane per step. It copies each chunk row into one buffer per open series, `BufferedSeriesIterator` after Prometheus's `storage/buffer.go`, answers a step as soon as its window cannot change any more, and trims from the front what no later window reaches ([engine.md](engine.md#kernel-state)). A state per step would pay every sample once per window it falls in, ten times here, twenty for `[5m]` at 15 s. The aggregate above `rate` does keep one partial per group and step, for the steps of the block being read; [section 4](#4-an-aggregate-across-a-block-edge) shows what bounds it.

What `extrapolatedRate` needs from a lane is small: its first and last sample, the count, and `reset_sum`, the values lost to counter resets. The engine reads the endpoints and the count off the lane's slice of the buffer, and carries the resets from step to step in `Sweep::Counter`, as ordinals into the buffer, so a step costs the samples entering and leaving the window. A running `reset_sum` would not save the resets: when a reset leaves the window its value has to come out of the sum, so it is stored either way, and a float that is added to and subtracted from drifts besides (`range.rs:254-256`). What the resets cost is bounded by the resets inside one window, not by its samples. thanos promql-engine's [`ringbuffer/rate.go`](https://github.com/thanos-io/promql-engine/blob/main/ringbuffer/rate.go) makes the same choice, keeping every reset in the window as the pair of values either side of the drop. promql-engine uses that buffer only when the range overlaps five or fewer steps (`ringbuffer/overtime.go`); `[5m]` at 15 s overlaps twenty and falls back to `generic.go`, which keeps every sample (section 7). The tables below are those numbers per lane.

A.0 first. When it is pushed the last timestamp seen is 420 s, so the lane at t = 420 s is complete and answered at once; no later sample can land in `(120 s, 420 s]`. The other six stay open, and all five samples stay in the buffer because the next window, `(150 s, 450 s]`, still reaches 300 s. The lane at t = 600 s holds four of them: its window is half-open, so the sample exactly on 300 s is outside it.

| lane t | window | after A.0: first | last | count | reset_sum |
|---|---|---|---|---|---|
| 420 s | (120 s, 420 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 450 s | (150 s, 450 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 480 s | (180 s, 480 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 510 s | (210 s, 510 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 540 s | (240 s, 540 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 570 s | (270 s, 570 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 600 s | (300 s, 600 s] | (330 s, 11) | (420 s, 14) | 4 | 0 |

B.0 is copied into the same buffer, after A.0's five samples; nothing of batch A is referenced. Its first sample is the first thing read from batch B, and the buffer is all that connects it to A.0. Its last timestamp is 600 s, so all six open lanes are complete and answered during the push:

| lane t | after B.0: first | last | count | reset_sum |
|---|---|---|---|---|
| 420 s | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 450 s | (300 s, 10) | (450 s, 15) | 6 | 0 |
| 480 s | (300 s, 10) | (480 s, 16) | 7 | 0 |
| 510 s | (300 s, 10) | (510 s, 3) | 8 | 16 |
| 540 s | (300 s, 10) | (540 s, 4) | 9 | 16 |
| 570 s | (300 s, 10) | (570 s, 5) | 10 | 16 |
| 600 s | (330 s, 11) | (600 s, 6) | 10 | 16 |

The lane at t = 600 s holds `first = 11` from A.0 in batch A and `last = 6` from B.0 in batch B, and the only thing spanning the two is a buffer of eleven copied samples, bounded by the window rather than the series.

## 4. An aggregate across a block edge

`sum(rate(x[5m]))` over the same seven steps, now with two series and a block edge inside the range. `x{pod="a"}` is the series above; `x{pod="b"}` counts up by one every 30 s, from 10 at 150 s. The store cuts its blocks every 240 s to keep the tables short, where a real one is an hour or two, so the window ends 420 s to 600 s fall in two blocks it declares: block 1, `block_start` 240 s and `block_end` 480 s, answers 420 s and 450 s; block 2, `block_start` 480 s and `block_end` 720 s, answers 480 s to 600 s. The engine selects once, from 120 s to 600 s with `window_ms` 300 s, and the store reaches back by that window at each block: block 1's rows hold samples from 120 s to 479 s, block 2's from 180 s to 600 s, and all of block 1's rows come before block 2's.

```
block  labels.pod  samples                     first / last sample timestamp
  1    "a"         300 330 … 420 450    (6)    300 / 450
  1    "b"         150 180 … 420 450   (11)    150 / 450
  2    "a"         300 330 … 570 600   (11)    300 / 600    reach-back 300–450
  2    "b"         180 210 … 570 600   (15)    180 / 600    reach-back 180–450
```

The fixture's chunks happen to end and begin exactly at these edges: `a`'s
block-1 row ends at 450 s, and `b`'s block-2 row begins at 180 s, its
reach-back start; no row here is trimmed to a block edge, since the store
never decodes a chunk to trim it
([series-source.md](series-source.md#the-seriessource-trait)).

Say block 1's two rows come in one batch. Its rows span 150 s to 450 s, and that does not matter: `sum` never sees samples, only the step values `rate` emits, each keyed by its step, so there is no current step for a batch to be early or late for. The rows feed whichever windows they fall in.

`rate` folds `a`'s block-1 row first. Its last timestamp is 450 s, so `a`'s steps at 420 s and 450 s, the only ones block 1 answers, are final and leave with the rates of section 5; `sum` adds them into the partials for 420 s and 450 s. `b`'s row then closes `a` before any of `b`'s samples is read, and `a`'s window buffer is dropped. `b`'s two steps are final at 450 s too, at 10 / 300 s = 0.0333333333 each: ten samples in each window, nine increments, extrapolated by 10/9, since a first value of 10 or more keeps the zero clip from biting. Block 2's first row then arrives with `block_start` 480 s, so block 1's last row has passed, and with it its steps: `sum` emits 420 s and 450 s and drops their partials.

| step | block | partial after `a` | after `b` | end of block 1 |
|---|---|---|---|---|
| 420 s | 1 | 0.015 | 0.0483333333 | emitted |
| 450 s | 1 | 0.0183333333 | 0.0516666667 | emitted |
| 480 s … 600 s | 2 | – | – | opened by block 2 |

A store that hands over whole chunks could send `a`'s B.0 to block 1 as it is, samples up to 600 s. Block 1 answers no step past 450 s, so the samples from 480 s fall in no window it evaluates, and nothing is clipped.

Block 2 starts every series from an empty buffer. `a`'s samples from 300 s to 450 s are its reach-back: they fill the windows of 480 s onward and answer no step, not even 450 s, which block 1 answered. Its last timestamp is 600 s, so all five steps of block 2 are final during the push:

| step | partial after `a` | after `b`, end of block 2 |
|---|---|---|
| 480 s | 0.0216666667 | **0.055** |
| 510 s | 0.0321428571 | **0.0654761905** |
| 540 s | 0.0354166667 | **0.06875** |
| 570 s | 0.0407407407 | **0.0740740741** |
| 600 s | 0.0407407407 | **0.0740740741** |

`a`'s rates are section 5's to the last digit: the window at 480 s, `(180 s, 480 s]`, lies wholly inside what block 2's rows hold. `b`'s sample at 180 s is read and falls in no window, since the window is open at its start. Nothing per series crossed the edge, and for `sum` nothing at all, since block 1's partials were emitted before block 2's first row was folded. `sum` never held partials for more than one block's steps, five here, four for a 2 h block at a 30-minute step, and `rate` never more than one series' buffer. With several partitions each keeps its own partials for the block, and the block's final `sum` adds them once every partition has passed the block, since its group key leads with `block_start` and the repartition beneath the Final preserves block order ([engine.md](engine.md#ordering-integrity)).

## 5. Finalising

Each lane went through Prometheus's `extrapolatedRate` when it was answered: t = 420 s during A.0's push, the rest during B.0's. B.1 belongs to series `y`, so it only closes `x`'s output row, which DataFusion sees from then on. For the lane at t = 600 s, spelled out once:

```
result             = last_v - first_v + reset_sum = 6 - 11 + 16 = 11
sampled_interval   = 600 - 330                                  = 270 s
average_interval   = 270 / (10 - 1)                             = 30 s
threshold          = 1.1 * 30                                   = 33 s
duration_to_start  = 330 - 300 = 30 s   (< 33, kept as measured)
duration_to_zero   = 270 * (11 / 11) = 270 s   (30 < 270, zero clip does not bite)
duration_to_end    = 600 - 600 = 0 s    (< 33, kept)
factor             = (270 + 30 + 0) / 270                       = 10/9
increase           = 11 * 10/9                                  = 12.2222222
rate               = 12.2222222 / 300                           = 0.0407407407
```

All seven, same procedure:

| lane t | result | sampled interval | to_start | factor | increase | rate |
|---|---|---|---|---|---|---|
| 420 s | 4 | 120 s | 15 s (clamped) | 135/120 | 4.5 | **0.015** |
| 450 s | 5 | 150 s | 15 s (clamped) | 165/150 | 5.5 | **0.0183333333** |
| 480 s | 6 | 180 s | 15 s (clamped) | 195/180 | 6.5 | **0.0216666667** |
| 510 s | 9 | 210 s | 15 s (clamped) | 225/210 | 9.6428571 | **0.0321428571** |
| 540 s | 10 | 240 s | 15 s (clamped) | 255/240 | 10.625 | **0.0354166667** |
| 570 s | 11 | 270 s | 30 s | 300/270 | 12.2222222 | **0.0407407407** |
| 600 s | 11 | 270 s | 30 s | 300/270 | 12.2222222 | **0.0407407407** |

The clamped rows are the five whose window starts at least the 33 s threshold before the first sample, so `duration_to_start` gets the half-interval guess. The reset shows as `result` climbing from 6 to 9 while the raw last value fell from 16 to 3.

## 6. Overlapping chunks

Adjacent chunk rows of a series in one block may overlap in time, when its chunks arrive from several files or compaction levels. The engine remembers the last timestamp it copied for each series and skips any sample at or before it, so the first row wins, as in Thanos's `chunkSeriesIterator`; Prometheus's union differs only for interleaved timestamps ([engine.md](engine.md#ordering-integrity)). A stale marker claims its timestamp too, before `rate` filters it out of the buffer, so a marker in the first row is not replaced by a sample at the same timestamp in the second. The skip matters even for an exact duplicate, which moves neither endpoint and fakes no reset but would raise `count`, shortening the average interval, which can push `duration_to_start` through a clamp it would otherwise miss.

## 7. Functions that need every sample in the window

`quantile_over_time` and its kind cannot be reduced to a few numbers, because they need the set. They need no other structure either: each step refolds its lane's slice of the same buffer, which is already bounded by the window and sorted by time; see [engine.md](engine.md#kernel-state). thanos promql-engine's [`ringbuffer/generic.go`](https://github.com/thanos-io/promql-engine/blob/main/ringbuffer/generic.go) is the same structure as the window buffer: it keeps every sample in the window and hands the function the slice.

## Notes on the numbers

The fold and `extrapolatedRate` run in the interactive page, following Prometheus's `promql/functions.go` term for term, with the guards in the order that file runs them: threshold clamp, then counter zero clip. None of it is read back from the engine; the engine checks itself against it instead. `the_engine_chunks_fixture` in `buffer.rs` pins this fixture as a regression: A.0 must push before B.0, the lane at 420 s must answer before B.0 arrives, and all seven rates must match to 1e-10. Section 4 reuses those rates and adds `x{pod="b"}`'s, worked by hand through the same terms.
