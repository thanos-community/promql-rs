# In-memory series representation for the PromQL layer

**The question is not how anyone stores series. It is what the PromQL layer is handed.**

```
┌─ phase 1 · scan ─────────────────┐       ┌─ phase 2 · PromQL ───────────────┐
│ your storage, your problem       │       │ this document                    │
│ out of scope                     │       │ the part we have to agree on     │
│                                  │       │                                  │
│ · apply matchers and time range  │  ──▶  │ · range vectors: rate, increase  │
│ · group samples by labelset      │       │ · instant collapse               │
│ · sort by timestamp within each  │       │ · aggregation across series      │
│                                  │       │                                  │
└──────────────────────────────────┘       └──────────────────────────────────┘
```

## Recommendation

Three decisions, taken together:

1. **The seam is one row per series, samples in aligned `List` columns.**
2. **Range-vector functions are kernels over slices**, with step bounds from a two-pointer sweep.
   Not window UDFs.
3. **Stores that keep rows as samples convert at the seam, in phase 1**, where they know their own
   series boundaries.

```
labels     Struct<code: Dictionary<UInt32, Utf8>, verb: …, …>     one row per series
timestamps List<Timestamp(ms)>
values     List<Float64>
```

The alternative is one row per sample with the labels run-end encoded inside the struct,
`Struct<RunEndEncoded<Int32, Dictionary<UInt32, Utf8>>, …>`. Both carry identical sample bytes and
the labelset once per series. They differ only in whether a DataFusion row is a sample or a series.

The recommendation rests on how the row axis behaves through the whole pipeline, not on one number.
Five reasons, in the order they matter:

1. **The first thing phase 2 does is find the series again.** Every range-vector function, and the
   staleness lookback that turns a bare selector into an instant vector, works on one series at a
   time. When rows are series the boundary is the list offsets. When rows are samples somebody has to
   write an operator that walks 18 run-end buffers, takes their union, and carries a series across
   batch boundaries. That operator is `support/rangevec.rs`, fed by `struct_ree.rs` or `list.rs`;
   the rows-as-samples file is twice the length of the rows-as-series one, and the difference is
   state.
2. **The series-aware operator emits the `list` layout.** Its output is `(labels: Struct, value)`
   or `(labels: Struct, grid: List)`, one row per series, whichever layout fed it. From the range
   vector onwards the engine runs in rows-as-series either way. Handing over rows-as-samples means
   phase 1 flattens what phase 2 immediately un-flattens.
3. **Parallelism falls out of the row.** Round-robin over series rows is partitioning over series,
   and every per-series stage is embarrassingly parallel with no shuffle. Rows-as-samples must not
   be round-robined at all, because a batch boundary is not a series boundary: the operator has to
   forbid it and rely on the provider's partitioning, or the engine has to hash-repartition every
   sample on `labels` first.
4. **The whole series is a slice.** `&[i64]` and `&[f64]` straight out of the list children, for
   any window length. Rows-as-samples arrive in batches of 8,192, so a two-week series spans five of
   them, and every per-series computation has to survive that cut: either the operator carries the
   partial series into the next batch or every kernel learns to walk a chain of slices. Any reader
   that bounds its batches puts that cut somewhere; one series per row never has it.
5. **Native window frames are not a route to PromQL semantics.** They are the one thing only
   rows-as-samples can drive. A frame evaluates once per input row, at sample timestamps, not once
   per step at step timestamps, and its left edge is not PromQL's `(t - range, t]`. What replacing
   them costs is [an 18-line sweep, once](#what-replacing-udwfs-costs).

The measurements agree, with the caveat that measurements of a hand-written operator are
measurements of that operator. Operator against operator, `list` is 1.0x to 3.5x faster on the
whole query and 1.4x to 4x faster on the range-vector stage alone, at equal or lower peak
allocation, and the gap grows with the window. That is close enough that performance alone would
not decide this; [the tradeoffs](#three-ways-to-build-the-range-vector) do.

**The price of `list`, to state as a price.** A series cannot span rows without merge semantics
this document does not define, so a provider emits each series in one row and caps its batches by
samples rather than rows. `List` has `i32` offsets, so `List` versus `LargeList` must be decided
rather than left to implementations. And a store that keeps rows as samples pays a conversion at
the seam. All three are in [Open](#open) or in the tradeoffs below.

This is the nested shape #4 proposed, with two changes: the labels are one struct column rather
than a column per label, and there is no `min_time` / `max_time`. [Shared requirements](#shared-requirements)
says why.

**If something forces rows to be samples,** run-end encode the labels inside the struct, exactly
as Polar Signals and Dash0 do. Do not reach for `RunEndEncoded<Int32, Struct<…>>`, which is the
tidier placement and does not work in DataFusion 55 (finding 1). And write the operator: DataFusion
has no notion of a series when rows are samples, so nothing finds them for you.

## Two phases

### Phase 1 · the scan layer

Out of scope, on purpose. Everyone's storage is different and nobody is going to agree on it. The
only thing that has to be agreed is what comes out the other side. Three obligations:

1. **Filter.** Apply the label matchers and the time range. The time range especially, because
   phase 2 assumes every sample it sees is one it asked for.
2. **Partition.** Group samples by labelset. A series' samples arrive contiguously and are never
   interleaved with another series'.
3. **Order.** Sort by timestamp, ascending, within each series.

How any of that happens is nobody else's business. Object storage, a gRPC call to something else,
an mmap'd file, all fine. None of it belongs in the spec.

### Phase 2 · the PromQL layer

Consumes exactly that and nothing else. Range vectors, instant collapse, aggregation across series.
This is the part written once and shared, so it is the part worth specifying precisely. If phase 1
delivers on those three obligations, the series grouping never has to be rediscovered by the
engine.

Everything below is worked through one real metric, `apiserver_request_total` on a production
Kubernetes cluster: 708 series, 18 label names, 30s scrape. The benchmark invents label values,
timestamps and counter values with that cardinality and nothing else. One cluster, so one data
point. Native histograms are out of scope.

## The constraint that drives everything

Phase 1 hands over one series' samples together. The labelset is therefore **constant across all
those samples**, and the representation must not pay for it per sample.

That is not a stylistic preference, it is a memory bound. Eighteen label values averaging ten bytes
plus a four-byte offset each, repeated alongside every 16-byte sample, is roughly 250 bytes of
labels per row. An order of magnitude more memory for the labels than for the data, and it grows
with the window.

Neither candidate does this, and it is the one thing they agree on. It is arithmetic rather than a
finding, so it is not benchmarked. The question is only **where to put the run length**, and there
are exactly two places.

## The two candidates

```
struct_ree · run-end encoded labels                list · listed samples

labels     Struct<REE<Int32, Dict<u32, Utf8>>, …>  labels     Struct<Dict<u32, Utf8>, …>
timestamp  Timestamp(ms)                           timestamps List<Timestamp(ms)>
value      Float64                                 values     List<Float64>

one row per sample                                 one row per series
```

Both dictionary encode the leaf, so a label value is stored once per batch in either. Nothing is
nullable in either, including the values child of every run-end encoded label, which arrow-rs's
`RunArray::try_new` cannot express and the benchmark builds from `ArrayData` instead.

Run-end encoding the labels has two possible placements, and only one of them works. Putting the
encoding *around* the struct, `RunEndEncoded<Int32, Struct<…>>`, is the tidier one: each labelset
is stored once and the `run_ends` buffer is literally the series boundary list. DataFusion cannot
read a field out of it. So `struct_ree` is the other placement, run-end encoding *inside* the
struct on each label, which gives every field its own independent run boundaries and makes the
series boundary the union of 18 buffers. That inner placement is what Polar Signals and Dash0 store
(`newREE` over `dictUTF8`, values child non-nullable), so the benchmark's `struct_ree` is the
production shape, not an approximation of it.

**These are near duals.** Same information, same contiguous sample buffers, and a run-length
buffer in both, attached to a different column, and for `struct_ree` to every label field
separately:

```
struct_ree                               list
  labels.code.run_ends  [10, 20, 30]       timestamps.offsets [0, 10, 20, 30]
  labels.code.values    3 dict keys        labels             3 struct rows
  … and so on, 18 fields, each with
    its own independent run boundaries
  timestamp             30 × int64         timestamps.values  30 × int64
  value                 30 × float64       values.values      30 × float64
```

In both, enumerating series is a walk over small integer buffers with no hashing: one buffer for
`list`, the union of 18 for `struct_ree`. DataFusion sees only rows, so when rows are samples that
walk has to be written as an operator.

The only real difference is **which column DataFusion treats as the row axis**:

| | `struct_ree` | `list` |
|---|---|---|
| row is | a sample | a series |
| labels encoding | run-end encoded, dictionary leaf | plain struct, dictionary leaf |
| series boundary | union of 18 `run_ends` buffers | one `offsets` buffer |
| a series longer than one batch | spans batches, operator carries it across | never, one row regardless |
| `timestamp` predicates, `RANGE … PRECEDING` frames | work as normal | need custom kernels |
| offset ceiling | none on the sample axis | i32, so `List` vs `LargeList` must be specified |
| 177 two-week series arriving | 7.1M rows in 885 batches | 177 rows in 177 batches |

## The row axis through the pipeline

What granularity each stage runs at, and what it costs to get there.

| stage | `struct_ree` | `list` |
|---|---|---|
| scan output | sample | series |
| finding series | operator: 18-way run-end union, carry across batches | operator: read offsets |
| range-vector kernel | `&[i64]`, `&[f64]` after the carry has copied the series together | `&[i64]`, `&[f64]` slices of the list children |
| range-vector output | series: `(labels, value \| grid)` | series: `(labels, value \| grid)`, identical |
| `sum by`, `without`, binary ops, `label_replace` | series | series |
| `topk`, `quantile`, anything ranking at a step | unnest to `(series, step)` | unnest to `(series, step)`, identical |
| the API matrix response | already the operator's output shape | already the layout |
| partitioning for parallelism | provider must partition on series boundaries; the operator forbids round-robin | any row split is a series split |

Everything below the range vector is the same plan for both layouts, and the benchmark runs it as
the same code. The comparison is therefore about the top three rows, and those rows are where the
three options below differ.

## Three ways to build the range vector

The range-vector step is where the layout question and the window-function question meet, so the
options are laid out together: what each one is, what it buys, what it costs. Two of them are
measured; the third is pinned by tests and reasoned about.

### A · rows as samples, a series-aware operator

`struct_ree` in. `RangeVectorExec` finds series from the union of 18 run-end buffers, carries a
series that crosses a batch boundary into the next batch, and hands the kernel a slice. This is
the `struct_ree · operator` row in every table below.

What it buys:

- **The production encoding crosses the seam untouched.** A provider over a Polar Signals or Dash0
  store hands over what it has.
- **No `List` versus `LargeList` question and no one-series-per-row rule.** A series may be as long
  as the scan.
- **Time predicates on `timestamp` and native frames stay available** to anything else in the plan.

What it costs:

- **The operator is mandatory.** DataFusion has no notion of a series when rows are samples, so
  nothing runs until the operator exists.
- **Everything in `struct_ree.rs` beyond the encoding is state that exists only for this layout**:
  the 18-way union in the coordinates of a sliced batch, the held-back last series, rebuilding
  label dictionaries for the output, refusing the planner's repartitioning.
  [Implementation complexity](#implementation-complexity) itemises it.
- **The held-back series is a copy, one per partition, kept until the next batch settles it.** Its
  memory grows with the window, 17.1 MB against 3.5 MB peak at two weeks, and the range-vector
  stage runs 1.4x to 4x behind `list`.
- **Every other per-series operation needs the same machinery.** The staleness lookback for a bare
  selector, every `*_over_time`, subqueries: each either shares this operator or repeats its
  boundary logic.
- **Parallelism depends on the provider.** Partitions must fall on series boundaries, and the engine
  cannot repartition without hashing every sample on `labels`.

### B · rows as samples, range-vector functions as window UDFs

`struct_ree` in. `rate(value) OVER (PARTITION BY labels ORDER BY timestamp RANGE 5m PRECEDING)`,
with a `PartitionEvaluator` that runs the kernel over the row range DataFusion hands it.
`support/frame.rs` has the placeholder, `tests/layouts.rs` pins what it does. It is not timed,
because what it computes is not a PromQL range vector; the reasons are the costs.

What it buys:

- **No custom `ExecutionPlan`.** DataFusion finds the bounds, partitions by labels, and parallelises
  with its own window operator.
- **Per function, a `WindowUDFImpl` and a `PartitionEvaluator`.** The kernel inside is still a fold
  over a slice.

What it costs:

- **The output cardinality is wrong.** A frame yields one result per input row, at sample
  timestamps: 7,080 samples in, 7,080 results out. A PromQL range query wants one result per step,
  at step timestamps, and at a 15s step on a 30s scrape most steps are not sample timestamps. To get
  steps out of a frame, inject synthetic step rows into every partition before it and filter the
  sample rows out after it. Three plan stages to correct what the sweep produces by construction.
- **The window edge is wrong.** A `RANGE 5m PRECEDING` frame is `[t - 5m, t]`; PromQL is
  `(t - 5m, t]`. An offset fixes it, per function.
- **`PARTITION BY labels` is a sort of every sample by the labels column.** Per-sample label work
  the operator never does. It runs on the production encoding, the characterisation test does, but
  nothing about it suggests it lands near the operator rows.
- **Per-row evaluation touches `window / scrape` samples per row**, ten at 5m and 30s, unless the
  evaluator keeps its own sliding state, at which point the frame has contributed nothing.
- **An instant query evaluates the frame at every sample to use one result.**
- **The boilerplate is larger than what it replaces.** The placeholder window UDF in
  `support/frame.rs` is about 80 lines, documentation included, around a one-line sum. The sweep
  it stands in for is 18 lines, written once for every function.

### C · rows as series, kernels over slices

`list` in. A row is a series, so the kernel takes slices of the two list children and the sweep
finds the step bounds. This is the `list · operator` row. In the benchmark it runs through the
same `RangeVectorExec` as A so that the comparison is operator against operator; for `list` alone
the operator is not needed. A range-vector function is a function of one row, two list columns in
and a `Float64` or a step grid out, which is a `ScalarUDF`. That form is not built here.

What it buys:

- **The boundary is structural.** No union, no carry, no rebuilt dictionaries, no partitioning
  constraint on the provider beyond one series per row.
- **Any row split is a series split.** Parallelism without a shuffle, using DataFusion's own
  repartitioning.
- **Output shape is input shape is API shape.** `(labels, grid)` per series is the matrix response.
- **Per function, a Rust `fn` over `&[i64]` and `&[f64]`**, plus the 18-line sweep once. No
  `ExecutionPlan`, no `WindowUDFImpl`, no plan surgery.
- **The range-vector stage is 1.4x to 4x faster and its memory is flat in the window.**

What it costs:

- **`List` versus `LargeList` must be decided.** i32 offsets cap the elements of one array.
- **One series per row.** A provider must never split a series across rows, and controls batch
  memory with a sample budget per batch instead, as the benchmark does at 8,192.
- **Native time predicates and frames over samples are unavailable inside phase 2.** Phase 1 does
  the time filtering, so nothing in the measured plans misses them.
- **A store that keeps rows as samples converts at the seam.** Polar Signals and Dash0 would. The
  conversion is the top half of option A's operator, done once in phase 1, where the store knows its
  boundaries from its own index and never needs the 18-way union. The cost does not vanish; it moves
  to the one place that has the information to make it cheap.

### The way forward

**C.** The measurements put A and C within a few milliseconds of each other on every shape but the
two-week one, so performance alone does not decide it. What decides it is that C's series boundary
is structural, and both A and B have to reconstruct it: A with the state in `struct_ree.rs`,
B with plan surgery around a frame whose semantics are wrong for PromQL in cardinality and edge.
B is also the only option that adds code per function rather than once.

A stays the right shape for a store that cannot convert at the seam, and everything above the
range vector is shared, so choosing C does not strand it.

## What we measured

`promql-layout-bench` builds each layout as real Arrow batches, splits them into 24 partitions of
whole series with about 8,192 samples per batch, registers them with DataFusion 55 and runs
`sum by (code) (rate(apiserver_request_total[5m]))` and its range-query relatives as DataFusion
logical plans, no SQL. `rate` is the real thing, with counter-reset crediting and extrapolation to
the window edges. Every candidate is checked against a reference that runs the same kernel over the
generator with no Arrow in sight. Full tables in [`RESULTS.md`](../promql-layout-bench/RESULTS.md);
this section is the reading.

Two candidates per query, both through the same series-aware `RangeVectorExec`:

- **`struct_ree · operator`**: fed rows-as-samples. Option A.
- **`list · operator`**: fed rows-as-series. Option C.

Above the range vector the plan is shared. `rate`, `+unnest` and `full` are cumulative cuts of it:
the range vector alone, then unnested to one row per `(series, step)`, then the whole query. Every
cell is the median of five runs after one warmup. Peak allocation comes from a counting global
allocator, because RSS hides a materialisation that gets freed again. The timings run under that
allocator too; its bookkeeping falls on every allocation, so it leans against the layout that
allocates more without changing which one leads.

### Raw Arrow footprint

Labels at rest, 18 labels, dictionary leaf in both:

| | `struct_ree` | `list` |
|---|---:|---:|
| labels, 708 series | 212.8 KB | **189.7 KB** |
| labels, 177 series | 177.2 KB | **141.8 KB** |

With a dictionary leaf in both, a run costs a run end plus a key where a series row costs a key, so
run-end encoding only saves memory when a run spans several series. At this cardinality, with
series sorted by labelset, it does not: `list` is smaller by 11% to 20%. Footprint at rest does
not decide anything, and neither layout grows its labels with the window.

### Equivalent series-aware pipelines

The fair comparison, options A and C: the same operator, the same kernel, the same plan above it,
and only the row axis differs. Median milliseconds for the whole query, then for the range-vector
cut alone.

| shape | samples | `struct_ree · operator` | `list · operator` | ratio |
|---|---:|---:|---:|---:|
| `rate[5m]`, instant | 7,080 | 4.8 ms | **3.1 ms** | 1.5x |
| `rate[5m]`, 1h at 15s, `sum by (code)` | 92,040 | 8.0 ms | **7.0 ms** | 1.1x |
| `rate[5m]`, 1h at 15s, `topk(3)` | 92,040 | 12.9 ms | **11.6 ms** | 1.1x |
| `rate[5m]`, 1d at 1m, `sum by (code)` | 2,044,704 | 16.7 ms | **14.0 ms** | 1.2x |
| `rate[5m]`, 1d at 1m, `topk(3)` | 2,044,704 | **44.9 ms** | 46.8 ms | 1.0x |
| `rate[1d]`, instant | 2,039,040 | 9.9 ms | **5.8 ms** | 1.7x |
| `increase[2w]`, instant | 7,136,640 | 17.6 ms | **5.1 ms** | 3.5x |

| shape, range-vector cut only | `struct_ree · operator` | `list · operator` | ratio |
|---|---:|---:|---:|
| `rate[5m]`, instant | 2.5 ms | **0.7 ms** | 3.6x |
| `rate[5m]`, 1h at 15s | 2.8 ms | **1.2 ms** | 2.3x |
| `rate[5m]`, 1d at 1m | 11.6 ms | **8.5 ms** | 1.4x |
| `rate[1d]`, instant | 12.2 ms | **7.5 ms** | 1.6x |
| `increase[2w]`, instant | 19.4 ms | **4.8 ms** | 4.0x |

Three things to read off this.

**The gap is in finding the series, not in the kernel.** On the range-vector cut `list` leads by
1.4x to 4x, and the lead grows with the window. The kernel is the same code over the same samples
for both, so the difference is what the `struct_ree` operator does around it: the 18-way run-end
union per batch, the held-back series and its copy, 18 dictionaries rebuilt per output batch. The
copy is not the part that costs. Fed the two-week scan as one batch per partition instead of 885,
so that almost nothing is carried, the `struct_ree` cut stays where it is while `list` gets faster
still. Where the rest of the `struct_ree` time goes is not profiled here.

**Above the range vector the layouts are indistinguishable**, by construction, and the numbers say
so: going from the `rate` cut to `full` costs both operator rows the same few milliseconds for a
`sum by`, and the same 35 ms for a `topk` over a million `(series, step)` rows. Where the query is
dominated by that part, as `topk` at 1d is, the layout stops mattering.

**Peak allocation follows the same shape.** Instant queries: 3.8 MB against 2.9 MB at 5m, 17.1 MB
against 3.5 MB at two weeks, and the two-week figure is the held-back series, one per partition,
24 copies of 40,320 samples. Range queries within 20% of each other either way, 8 to 16 MB for a
`sum by` and about 60 MB for a `topk`, because the unnested grid dominates both.

### Implementation complexity

What the operator has to do for each layout, `struct_ree.rs` against `list.rs`:

| | `struct_ree` | `list` |
|---|---|---|
| find boundaries | sorted, deduplicated union of 18 `run_ends` buffers, in the logical coordinates of a possibly sliced batch | read one `OffsetBuffer` |
| read a series' labels | follow each of 18 runs to its dictionary key at the series' first row, materialise strings | `take` the struct row |
| a series crossing a batch | hold the last series back, extend it when the next batch continues it, emit at end of partition | cannot happen |
| build the output labels | 18 dictionary builders per output batch | gather from the input struct |
| partitioning | must refuse round-robin repartitioning, series would be split | any split is fine |

`struct_ree.rs` is twice the length of `list.rs`, and the extra is the state. A better operator
could avoid the copy by teaching the kernel to walk a chain of slices, at the price of every
range-vector function learning about batch boundaries. The boundary itself does not go away: any
reader that bounds its batches cuts some series, and every per-series operator has to know.

And what each way of finding window bounds costs, options B against A and C:

| | window UDF (B) | kernel and sweep (A, C) |
|---|---|---|
| window bounds | DataFusion's frame, per row | `windows()`, 18 lines, once |
| per function | `WindowUDFImpl` + `PartitionEvaluator` around the kernel | a `fn` over slices |
| output | one row per sample; step rows must be injected and samples filtered out | one row per series, grid inside |
| window edge | `[t - r, t]`, offset needed | `(t - r, t]` |
| instant query | frame at every sample, keep the last | one window at `t` |
| execution | DataFusion's window operator over a sort by labels | `ScalarUDF` over lists (C) or `RangeVectorExec` (A) |

### Findings pinned by tests

Each of these is a characterisation test in `tests/layouts.rs`. If DataFusion changes, the test
fails and this note needs revisiting.

1. **`RunEndEncoded<Int32, Struct<…>>` cannot be read from.** DataFusion 55 rejects `labels.code`
   on it: `type RunEndEncoded(…) is not Struct, Map, or Null`. This is why `struct_ree` means the
   inner placement throughout.
2. **A native `RANGE` frame runs over rows-as-series, and DataFusion does not object.** Ordering
   by a `List<Timestamp>` column is planned and executed. The evaluator is simply handed whole
   series, `List<Float64>` per row, and the finest boundary it can express is "series 3 to 5".
   Nothing rejects it, so without the test the failure would be silent.
3. **A native `RANGE` frame over rows-as-samples emits one result per input row.** 7,080 samples in,
   7,080 results out, at sample timestamps. A PromQL range query wants one result per step, at step
   timestamps, with a `(t - range, t]` window. The frame is not that.

## Window frames and PromQL semantics

A frame is a range of **row** indices. When a row is a series, the frame is a range of series and
no arithmetic inside the evaluator gets a sample boundary back: finding 2. When a row is a sample,
the frame evaluates once per sample, at sample timestamps, over `[t - range, t]`: finding 3. PromQL
wants once per step, at step timestamps, over `(t - range, t]`. Neither layout gets PromQL
semantics from a frame; rows-as-samples gets something that can be corrected with more plan, and
rows-as-series gets nothing usable.

For a range query nobody should want the frame anyway. On a 5m window at a 30s scrape a per-row
frame touches ten samples per sample, against one pass over each series for the sweep, and its
results land at sample timestamps rather than step timestamps, so a second pass would still have to
pick out the steps. That closes the door on range-vector functions as window UDFs, which is where
#4 was heading, so the question is what replacing them costs.

### What replacing UDWFs costs

**A UDWF provides exactly one thing: the window bounds.** Side by side, with everything that is
identical in both left out:

```rust
// ---- as a UDWF -------------------------------------------------------------
// DataFusion parses RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING, partitions by
// labels, orders by timestamp, and hands each row the range it computed.

impl PartitionEvaluator for RateWindow {
    fn uses_window_frame(&self) -> bool { true }

    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
        let vs = values[0].as_primitive::<Float64Type>();
        rate(&vs.values()[range.start..range.end])   // <-- a slice
    }
}

// ---- our own kernel --------------------------------------------------------
// We compute the range instead. Everything after that line is the same.

fn windows(ts: &[i64], grid: Grid) -> Vec<Range<usize>> {
    let (mut lo, mut hi) = (0, 0);
    (0..grid.steps).map(|s| {
        let t = grid.at(s);
        while hi < ts.len() && ts[hi] <= t { hi += 1; }        // right edge, inclusive
        while lo < hi && ts[lo] <= t - grid.range { lo += 1; }  // left edge, exclusive
        lo..hi
    }).collect()
}

for (s, range) in windows(ts, grid).into_iter().enumerate() {
    out.push(rate(&values[range]));                  // <-- the same slice
}
```

Both arrive at `rate(&[f64])`. The bodies of every range-vector function sit below that line and
cannot tell the difference, which is why the choice does not multiply out across the function set.

`support/window.rs` is the real version of the lower half; the sweep itself is 18 lines. Both
pointers only move forward, so one pass over a series serves every step. `support/ratefn.rs` is
then a real `rate` and `increase`, with counter-reset crediting and extrapolation to the window
edges, and not one line of it knows which mechanism found its bounds. It is the kernel every timed
row above ran. A test asserts the swept bounds agree with bounds computed by filtering, which is
what a frame would have handed over.

That is the shape of the entire function set. PromQL has roughly 25 range-vector functions and
every one is a fold over a slice. The `*_over_time` family is a loop, the counter family shares
one extrapolation helper, `quantile_over_time` sorts. They are the same code under either plan, so
the cost of dropping UDWFs is the sweep, once, not per function.

What the sweep does not cover, and neither would a UDWF: subqueries, where a range vector is built
from an inner range query's step grid, and the `@` and `offset` modifiers. Both change *which*
timestamps you ask phase 1 for rather than how a window is found, so they are independent of this
decision.

## Aggregating across series

How `sum by (code) (rate(…[5m]))` rejoins the per-series world. This is the question after the
record format and it is independent of it, so everything here holds whichever layout wins, and the
benchmark runs it as one plan for both.

**Instant queries never leave series granularity.** The range-vector operator collapses each
series to one scalar, so the plan is 708 series in, 708 rows out, then a grouping over 708 rows.
That covers every recording rule, which is the entire continuous workload.

**Range queries have to materialise the step grid**, because `sum by (code)` combines different
series at the same step, so one value per `(series, step)` has to exist somewhere. The operator
emits the grid as a `List<Struct<step, value>>` per series and the plan unnests it:

```
Sort                                          1,680 rows
  Aggregate: sum by (code, step)              169,920 → 1,680
    Unnest grid                               708 → 169,920
      RangeVectorExec: rate_steps(…)          708 series → 708 rows
        Scan                                  92,040 samples
```

Measured, the unnest is cheap because its input is small: at 1h it takes `list` from 1.2 ms to
2.1 ms, and at 1d, unnesting to a million rows, it is within noise of the range-vector cut. The
`(series, step)` rows are the shape `topk`, `bottomk`, `quantile` and `count_values` need in any
case, since they rank across series at a step.

**Not built, and worth knowing about:** a fused `sum by` as an aggregate whose accumulator is a
`steps`-wide vector would keep `by` and `without` at series granularity and only unnest 7 groups
rather than 708 series. It cannot serve the ranking functions, and a dense accumulator over a 30d
range at a 15s step is 172,800 floats per group and wants a guard. The measured unnest cost above
is the ceiling on what it could save.

## Shared requirements

Whichever is chosen, these hold:

- **Samples are sorted.** Timestamps strictly ascending within a series, values positionally
  aligned, equal lengths. Reset detection in `increase` walks the window in order, the instant
  collapse is the last sample, and the sweep assumes it.
- **Series are contiguous** and never interleaved. With `list` this is structural. With
  `struct_ree` it is a promise the provider makes and the operator depends on; DataFusion has no
  property to declare it, and declaring a full sort order instead promises more than phase 1 owes.
- **Nothing is nullable.** An absent label is `""` in PromQL, not NULL. Non-nullable children remove
  the `get_field` phantom-value-under-a-NULL-parent trap, because there is no NULL parent to read
  through. A schema constraint instead of a two-layer matcher guard.
- **Dictionary at the leaf.** `Dictionary<UInt32, Utf8>` for every label value, in both layouts.
- **No `min_time` / `max_time`.** Given sorted input they are the first and last sample of a series,
  and given exact time-range filtering in phase 1 there is nothing left for a row-level time filter
  to prune. In a shared spec a redundant field is mainly a field an implementation can get out of
  sync with the data.
- **`Float64` carries StaleNaN** as the exact bit pattern `0x7ff0_0000_0000_0002`, so nothing may
  cast through `f64::NAN`.

## Open

- **One series, one row.** #4 allowed "2h of samples per row"; this note says one row per series
  per scan. Splitting a series across rows would need merge semantics in every per-series operator,
  which is the carry logic `struct_ree` needs and `list` avoids. A provider controls batch memory
  by capping batches at a sample budget, as the benchmark does at 8,192, not by splitting series.
  If a real provider cannot hold one series in one row, that is the moment to revisit this.
- **`List` vs `LargeList`.** i32 offsets cap total elements per array at about 2.1 billion. The 2w
  shape is 7.1M in one array; a batch capped by samples never gets near it, but the spec has to say
  which it is.
- **Range-vector functions as `ScalarUDF`s over the list columns.** Option C's simplest form is not
  built; the benchmark's operator is an `ExecutionPlan` so both layouts could share it. Building
  one function that way would confirm the operator is unnecessary for `list`.
- **Subqueries.** A range vector built from an inner range query's step grid. Independent of the
  layout choice, but nothing here has worked it through.
- **Nullability and StaleNaN** are specified above; nullability is exercised by the schema tests,
  StaleNaN is not yet.

If DataFusion later supports field access on `RunEndEncoded<Struct>`, finding 1 starts failing and
the outer placement is worth measuring: it would give `struct_ree` one run-length buffer instead of
eighteen, which is the part of it that is genuinely elegant.

## Reproduce

```
cargo test -p promql-layout-bench
cargo run -p promql-layout-bench --release -- --write-results
```

The crate is two files and a support folder. `src/list.rs` and `src/struct_ree.rs` hold everything
unique to producing and processing each layout, behind the `Layout` trait in `support/layout.rs`;
they are the code to review. `src/support/` is the rest: `scan.rs` invents series with the
cluster's cardinality, `format.rs` is the seam, `chunking.rs` hands a scan over in partitions of
batches the way a provider would, and `rangevec.rs` is the series-aware operator, written once and
generic over the layout, with `window.rs` finding its bounds and `ratefn.rs` as the real `rate`.
`promql.rs` is everything above it as DataFusion logical plans, `frame.rs` is the placeholder
window UDF, `query.rs` is the PromQL query as numbers, `dispatch.rs` picks a layout at runtime,
`alloc.rs` counts allocations, `harness.rs` runs the shapes, and `--write-results` regenerates
[`RESULTS.md`](../promql-layout-bench/RESULTS.md).

Both candidates are handed byte-identical samples from one generator, and every candidate is
checked against a reference that never touches Arrow, so a difference in the numbers can only be
the encoding and how the engine handles it.
