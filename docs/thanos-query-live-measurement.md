# Live measurement against Go Thanos

*Measured 2026-10-03.*

This records one live comparison of the Rust Thanos querier (`thanos-query-rs`)
against Go `thanos query`, both reading the same Store Gateway. It exists so the
ratios can be re-checked when the store is cold or the Go binary is rebuilt, and
so the numbers come with the reasons behind them.

## Setup

- `T` is now minus 6h, fixed for every query.
- Rust: release build at tree 96736ec, default 2h blocks.
- Go: `thanos query` revision 1d2b2ae7 (v0.42.4-197, go1.27, build tag
  `slicelabels`) with `dedup=false`, because this branch has no dedup.
- Both talk to the same production Store Gateway through a port-forward, which
  stayed up for every block.
- 10 measured runs per side, alternating Go and Rust. Each side is primed first
  (2 to 3 runs on Go, until two consecutive runs agree within about 20%; one on
  Rust) so the store's cache and the Go heap are warm. Priming runs are not
  statistics. No measured run was discarded; the rule was to drop one only above
  3x the median of the other nine on its side.
- Every measured run returned HTTP 200 with no warnings.

## Results

Median, with min to max in parentheses.

| Query | Rust | Go | Result |
|---|---|---|---|
| instant `sum(up)` | 83.7ms (75.8 to 92.8) | 76.0ms (74.3 to 90.1) | identical (1155) |
| instant `count(up)` | 82.7ms (75.4 to 98.1) | 78.4ms (71.0 to 124.6) | identical (1155) |
| range `sum(rate(process_cpu_seconds_total[5m]))`, 6h, step 60 | 284ms (263 to 314) | 299ms (280 to 334) | 361 points; 359 differ in the last digits, at most 6.5e-13 relative |
| range `count(up)`, 30d, step 3600 | 8.81s (8.32 to 13.74) | 6.62s (5.75 to 8.77) | 721 points identical |
| `/api/v1/labels`, 7d | 24.7ms (22.4 to 28.9) | 25.0ms (23.7 to 29.0) | identical (425 names) |
| `/api/v1/label/job/values`, 7d | 23.2ms (20.8 to 26.6) | 23.3ms (22.1 to 25.9) | identical (53) |

Equality is the hash of `data.result` with Go's `analysis` stripped.

Peak RSS during the 30d query, ten runs: Rust 534 to 611 MB (median 572), Go 824
to 825 MB. Go's figure is flat because its heap stays at the level the priming
runs reached, so it measures retained heap rather than what one query needs.

Benchstat says the Rust instant `sum(up)` is slower by 10% (p=0.04) and the 30d
query by 33% (p<0.001); every other query is within noise. The 30d Rust
spread is wide (25%); one run took 13.7s.

### Previous run

Against the old Go binary (revision 94f971bc, February 2024), median of 3 runs:
`sum(up)` 0.063s Rust, 0.066s Go; `count(up)` 0.061s, 0.064s; 6h rate 0.225s,
0.266s; 30d `count(up)` 8.70s, 6.34s; labels 0.023s, 0.023s; label values
0.020s, 0.022s. Peak RSS on the 30d query: Rust 455 / 544 / 516 MB, Go 1031 /
1398 / 1401 MB. That run found 38 of 361 rate points differing.

The session before, against a cold store: 6h queries took about twice Go's
time, and the 30d query at 2h blocks hit the 2m query timeout. At 24h blocks it
took 141s against Go's 114s, at 400 MB against 1.1 GB.

## Why the numbers moved

- **One request per store per query.** The old fetch sent one Series request per
  2h block, so a 30d query made about 360 requests and each re-ran the store's
  index lookups. Now there is one request per store for the whole range, as Go
  does. `--query-block-duration` only sizes engine blocks; it no longer shapes
  the wire traffic, which is why 2h blocks stopped timing out.
- **Memory is about 30% below Go's.** Frames stay encoded until a block needs
  them, a block decodes one ahead of the engine over a channel, and chunks that
  no later block reaches are freed. The engine still receives blocks, so
  nothing is materialised as a whole series. The first block decodes while
  frames are still arriving, which hides most of the fetch behind decode.
- **Instant queries are store-bound on both sides.** The 7ms gap on `sum(up)`
  is within one run's spread on Go (74 to 90ms) and is not explained by engine
  work; `count(up)` shows no significant gap.

## Caveats

- The store was warm. Go's 30d query went from 114s to 6s between sessions, so
  compare ratios across sessions, never absolute times.
- The 38 rate points that differed by up to 3.2e-6 came from the old Go binary,
  not from the rewrite. It vendored a January 2024 Prometheus whose
  `extrapolatedRate` clamps the start gap to zero before capping it at half the
  average interval; the pinned spec commit and current Thanos cap first, which
  is the order Rust uses. Against the rebuilt Go that difference is gone: all
  361 points agree to 6.5e-13 relative. The 359 hashes that still differ are
  last-digit float noise from summing in a different order, not a formula
  difference.

## Side findings

- The Rust instant endpoint rejects a bare range selector; Prometheus returns a
  matrix.
- The hint start is `T-300000` ms where Prometheus uses `T-299999` ms.
- The planner now sends no step for instant queries, so the Series request
  carries `step: 0` as Go Thanos's does. The fix is cherry-picked from the
  branch bound for main.

## Known divergences and follow-ups

Replica deduplication, measured at `T=1791009720` against Go Thanos `1d2b2ae7`
on :10903, both with `--query.replica-label prometheus_replica`.

`sum(up)`, `count(up)`, `count(up)` over 30d at step 3600 and instant
`count(up)` equal Go exactly, with `dedup=true` and with `dedup=false`. That
needed one fix: Go's proxy sorts chunks with equal bounds larger-data-first
(`AggrChunk.Compare`), and the penalty merge picks differently when ties arrive
in another order.

`sum(rate(process_cpu_seconds_total[5m]))` over 6h at step 60s still differs, by
up to 9e-4 relative with `dedup=true` and 2.1e-5 with `dedup=false`. Go asks
`Aggr_COUNTER` for counter functions and wraps each replica's chunks in
`downsample.NewApplyCounterResetsSeriesIterator` before deduplicating
(`pkg/query/iter.go` `newChunkSeries` / `aggrsFromFunc`). We ask `RAW`, so a
reset inside one replica is seen by the penalty merge as a drop.
Follow-up: port the reset application, with its state carried across blocks.

Go also lifts a counter's first pick of replica b to replica 0's first sample
(`counterErrAdjustSeriesIterator` / `adjustAtValue`). Our kernel holds picks
back until replica 0's first sample arrives, and `DedupExec` looks ahead into
later blocks to find it. A randomized review found two problems:

- **B, first follow-up (memory bound, not precision).** One held series makes
  `DedupExec` buffer the whole remaining input and delays every other series of
  that block.
- **A.** The first lift is wrong when replica 0's first sample lies beyond the
  current block and nothing is folded yet.

Proposed fix for both: the source decodes each replica slot's first sample
(within Go's bounds) when the query's chunks arrive, before any block decode,
and delivers it with the slot tag. The kernel seeds every level with it, and the
hold-back and the lookahead go away.

## Live measurement with deduplication 2026-10-03

The morning's protocol and queries, rerun in the afternoon with replica
deduplication on both sides. Go `thanos query` 1d2b2ae7 and the Rust release
build at 88eb064 both run with `--query.replica-label prometheus_replica` and
receive no `dedup` or `replicaLabels` parameter, so both deduplicate. `T` is
`1791027494`. Before each block no compile ran for 60 s. The one-minute load
average per block was 2.61 (`sum(up)`), 2.35 (`count(up)`), 1.87 (rate), 2.42
(30d), 4.63 (labels) and 3.61 (label values). No measured run was retried or
discarded, and none returned warnings.

| Query | Rust | Go | Result |
|---|---|---|---|
| instant `sum(up)` | 60.3ms (56.7 to 71.0) | 66.1ms (54.3 to 77.8) | identical (385) |
| instant `count(up)` | 77.4ms (64.1 to 91.0) | 75.3ms (65.9 to 78.3) | identical (389) |
| range `sum(rate(process_cpu_seconds_total[5m]))`, 6h, step 60 | not measured | not measured | 361 points; 11 differ by more than 1e-3 relative, worst Rust 243.1 where Go has 7.88 |
| range `count(up)`, 30d, step 3600 | 10.86s (10.42 to 17.56) | 10.07s (9.37 to 11.38) | 721 points identical |
| `/api/v1/labels`, 7d | 23.8ms (23.0 to 28.0) | 25.4ms (24.0 to 30.1) | identical (425 names) |
| `/api/v1/label/job/values`, 7d | 22.8ms (21.5 to 32.3) | 23.8ms (21.4 to 25.8) | identical (53) |

`count(up)` is 389 where `sum(up)` is 385 because four targets were down at `T`.
Without deduplication the morning counted 1155, three replicas of 385.

The rate query failed equality on all three attempts, 30 s apart, so it has no
timings and is missing from both files. A run counts only when its result
equals Go's. Both sides return 361 points at the same timestamps, 345 of which
differ, 11 of them by more than 1e-3 relative. At `1791018494` Rust returns
243.1 where Go returns 7.88. Two processes restarted inside that point's 5m
window, and every replica recorded the reset. Go applies counter resets to each
replica before it deduplicates, and we do not (see Known divergences). The 9e-4
recorded there was measured at another `T`; here Rust is off by a factor of 30,
so porting the reset application is a correctness fix, not a precision one.

Peak RSS during the 30d query, ten runs: Rust 712 to 779 MiB (median 753), Go
986 to 1020 MiB, 1020 in nine runs. As in the morning, Go's figure is its
retained heap. The morning's MB figures are MiB as well.

Compared with the morning, the 30d query took Rust 23% longer (8.81s to 10.86s,
p=0.009) and 32% more memory (572 to 753 MiB), and took Go 52% longer (6.62s to
10.07s). The store moves between sessions, so the ratio to Go is the number to
trust: Rust's 30d gap shrank from 33% to 8% (p=0.002), at 26% less peak RSS.
Instant `sum(up)` came out 28% faster than the morning on Rust and 13% on Go,
which is the store, not a gain from deduplication.

One Rust 30d run took 17.6s while Go took 11.4s in the same pair; the next
slowest Rust run took 12.0s.

`benchstat docs/measurements/2026-10-03-thanos-query-dedup-go.txt docs/measurements/2026-10-03-thanos-query-dedup-rust.txt`:

```
goos: darwin
goarch: arm64
pkg: thanos-query-live
                 │ docs/measurements/2026-10-03-thanos-query-dedup-go.txt │ docs/measurements/2026-10-03-thanos-query-dedup-rust.txt │
                 │                         sec/op                         │              sec/op                vs base               │
InstantSumUp                                                 66.11m ± 14%                        60.31m ± 17%       ~ (p=0.247 n=10)
InstantCountUp                                               75.34m ±  8%                        77.40m ± 15%       ~ (p=0.353 n=10)
RangeCountUp30d                                               10.07 ±  6%                         10.86 ± 10%  +7.87% (p=0.002 n=10)
Labels7d                                                     25.37m ± 14%                        23.84m ±  8%       ~ (p=0.075 n=10)
LabelValuesJob7d                                             23.78m ±  7%                        22.81m ±  6%       ~ (p=0.315 n=10)
geomean                                                      124.8m                              122.5m        -1.83%

                │ docs/measurements/2026-10-03-thanos-query-dedup-go.txt │ docs/measurements/2026-10-03-thanos-query-dedup-rust.txt │
                │                     peak-rss-bytes                     │          peak-rss-bytes           vs base                │
RangeCountUp30d                                            1019.9Mi ± 0%                       753.0Mi ± 2%  -26.17% (p=0.000 n=10)
```

`benchstat docs/measurements/2026-10-03-thanos-query-rust.txt docs/measurements/2026-10-03-thanos-query-dedup-rust.txt`
compares the morning's Rust run without deduplication against this one:

```
goos: darwin
goarch: arm64
pkg: thanos-query-live
                 │ docs/measurements/2026-10-03-thanos-query-rust.txt │ docs/measurements/2026-10-03-thanos-query-dedup-rust.txt │
                 │                       sec/op                       │             sec/op              vs base                  │
InstantSumUp                                             83.72m ±  8%                     60.31m ± 17%  -27.96% (p=0.000 n=10)
InstantCountUp                                           82.67m ± 12%                     77.40m ± 15%        ~ (p=0.075 n=10)
RangeRateCpu6h                                           284.4m ±  9%
RangeCountUp30d                                           8.811 ± 25%                     10.860 ± 10%  +23.26% (p=0.009 n=10)
Labels7d                                                 24.66m ± 13%                     23.84m ±  8%        ~ (p=0.853 n=10)
LabelValuesJob7d                                         23.22m ± 10%                     22.81m ±  6%        ~ (p=0.393 n=10)
geomean                                                  146.6m                           122.5m         -4.61%                ¹
¹ benchmark set differs from baseline; geomeans may not be comparable

                │ docs/measurements/2026-10-03-thanos-query-rust.txt │ docs/measurements/2026-10-03-thanos-query-dedup-rust.txt │
                │                   peak-rss-bytes                   │          peak-rss-bytes           vs base                │
RangeCountUp30d                                         571.6Mi ± 7%                       753.0Mi ± 2%  +31.74% (p=0.000 n=10)
```

## Comparing with benchstat

`go install golang.org/x/perf/cmd/benchstat@latest`, then
`benchstat docs/measurements/2026-10-03-thanos-query-go.txt docs/measurements/2026-10-03-thanos-query-rust.txt`.
The two files hold the 60 measured runs per side in Go benchmark format; later
reruns go into new dated files so benchstat can track the trend.

## Reproducing

1. Run Go `thanos query` with `dedup=false` on one port and the Rust release
   build on another, both pointed at the same store endpoint.
2. Pick `T` once and pass the same `time=` or `start`/`end` to every query.
3. Prime each side per query until two consecutive runs agree within about 20%,
   then run 10 measured runs per side, alternating which side goes first. Keep
   priming out of the statistics.
4. Sample peak RSS of each process with `ps` while the 30d query runs.
5. Compare result bodies, not just status codes.
