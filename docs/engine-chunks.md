# How `rate` crosses a chunk boundary

Interactive version with the step-through fold and the Arrow buffer inspector: https://thanos-community.github.io/promql-rs/engine-chunks.html

promql-engine internals · branch `series-chunk-rows` · companion to [engine.md](engine.md) and [series-source.md](series-source.md)

If a store streams one Arrow row per chunk, and may cut its `RecordBatch`es anywhere, how are two chunks aligned so that `rate` can reach the last value of one and the first value of the next?

`rate(x[5m])` · step 30 s · 420 s → 600 s · two batches, three rows, two series

**They are not aligned.** The two chunk rows of `x` are ranges in two different `RecordBatch`es, read in arrival order, and the last value of the first is already in per-step **lane state** when the second is read. Alignment is only a question if the operator works on whole series. A grouped aggregate does not.

## 1. The shape everything happens in

```
labels     Struct<__name__: Utf8View, ...>
samples    List<Struct<timestamp: Timestamp(Millisecond), value: Float64>>
```

One row is one chunk of one series. The fixture is three rows in two batches: batch A holds the first chunk of series `x`; batch B holds its second chunk, then one chunk of series `y`, which is what closes `x`. The counter resets inside B.0, from 16 to 3.

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

> **A store may split a series' chunk rows across any number of `RecordBatch`es.** A series may end mid-batch or span several; `x` here does both. The engine never gathers a series' chunks for `rate`: it folds rows in arrival order into per-step lane state, closes a series when a row with another label set arrives or the stream ends, and carries across a batch boundary only the lane state of the one open series per partition.

What crosses from A.0 to B.0 is lane state, never a reference into batch A's buffers, and the batch boundary is not an event the fold notices. Why the engine never joins chunk rows into one is in [engine.md](engine.md#memory).

That B.1 closes `x` holds only because the store keeps a series' rows consecutive and label-sorted, a contract in [engine.md](engine.md#ordering-integrity). The engine checks it on every row at one comparison each, labels non-decreasing between adjacent rows and first sample timestamp non-decreasing within a series, and turns a violation into a query error rather than a silently wrong result.

## 3. The fold

`rate(x[5m])` from t = 420 s to t = 600 s at step 30 s. Seven steps, so seven lanes per series. A lane holds only what `extrapolatedRate` needs, so a sample can be forgotten once folded: the first and last sample, the count, and `reset_sum`, the values lost to counter resets.

A sample folds into every lane whose window `(t_step − 300 s, t_step]` holds it: set `first` if unset, add the old `last` to `reset_sum` if the new value is below it, overwrite `last`, bump `count`.

A.0 first. Six lanes take all five of its samples. The lane at t = 600 s takes four: its window is half-open, so the sample exactly on 300 s is outside it.

| lane t | window | after A.0: first | last | count | reset_sum |
|---|---|---|---|---|---|
| 420 s | (120 s, 420 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 450 s | (150 s, 450 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 480 s | (180 s, 480 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 510 s | (210 s, 510 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 540 s | (240 s, 540 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 570 s | (270 s, 570 s] | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 600 s | (300 s, 600 s] | (330 s, 11) | (420 s, 14) | 4 | 0 |

B.0 folds into the same lanes; nothing of A.0 is held except those numbers. Its first sample is the first thing read from batch B, and lane state is all that connects it to A.0. After it:

| lane t | after B.0: first | last | count | reset_sum |
|---|---|---|---|---|
| 420 s | (300 s, 10) | (420 s, 14) | 5 | 0 |
| 450 s | (300 s, 10) | (450 s, 15) | 6 | 0 |
| 480 s | (300 s, 10) | (480 s, 16) | 7 | 0 |
| 510 s | (300 s, 10) | (510 s, 3) | 8 | 16 |
| 540 s | (300 s, 10) | (540 s, 4) | 9 | 16 |
| 570 s | (300 s, 10) | (570 s, 5) | 10 | 16 |
| 600 s | (330 s, 11) | (600 s, 6) | 10 | 16 |

The lane at t = 600 s holds `first = 11` from A.0 in batch A and `last = 6` from B.0 in batch B, and nothing ever built a range spanning the two.

## 4. Finalising

B.1 belongs to series `y`, so series `x` is complete and each of its lanes goes through Prometheus's `extrapolatedRate`. For the lane at t = 600 s, spelled out once:

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

## 5. Overlapping chunks

Adjacent chunk rows of a series may overlap in time, after compaction for example. The engine remembers the last timestamp it folded for each series and skips any sample at or before it, so the first row wins, as in Thanos's `chunkSeriesIterator`; whether to take Prometheus's union instead is [open](engine.md#ordering-integrity). The skip matters even for an exact duplicate, which moves neither endpoint and fakes no reset but would raise `count`, shortening the average interval, which can push `duration_to_start` through a clamp it would otherwise miss.

## 6. Functions that need every sample in the window

`quantile_over_time` and its kind cannot be folded into a few numbers, because they need the set. They keep a buffer of each series' samples, bounded by the window rather than the series, in place of running lane state; see [engine.md](engine.md#kernel-state).

## Notes on the numbers

The fold and `extrapolatedRate` run in the interactive page, following Prometheus's `promql/functions.go` term for term, with the guards in the order that file runs them: threshold clamp, then counter zero clip. None of it is read back from the engine, whose kernel is still being written. When it lands, pin this fixture: three rows in two batches, the series boundary and the batch boundary in different places, a reset inside the second chunk, and one lane that closes before the second chunk opens.
