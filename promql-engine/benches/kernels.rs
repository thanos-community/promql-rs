//! Micro benchmarks of the engine's kernels, DataFusion not involved.
//!
//! Every group is named `area/function` and parameterized by input size,
//! so a tracker keys a stable history on ids like
//! `selector/eval_series/plain/16000`. Names are chosen once: renaming a
//! benchmark resets its history. Fixtures are deterministic and built
//! outside the timed closure. To compare two states of the code:
//!
//! ```sh
//! cargo bench -p promql-engine --bench kernels -- --save-baseline before
//! # change code
//! cargo bench -p promql-engine --bench kernels -- --baseline before
//! ```

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use datafusion::arrow::array::{ArrayRef, AsArray, BooleanBufferBuilder, ListArray};
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};
use promql_engine::aggregate::{Grouped, Op};
use promql_engine::math::{self, FlagLane, Welford};
use promql_engine::params::Params;
use promql_engine::range::{self, Func};
use promql_engine::selector::{self, STALE_NAN_BITS};
use promql_engine::series::{self, Series};

/// Scrape interval of every synthetic series.
const SCRAPE_MS: i64 = 15_000;
/// Step of every synthetic query grid.
const STEP_MS: i64 = 30_000;
const FIVE_MINUTES_MS: i64 = 5 * 60_000;
/// Series lengths the per-series kernels are measured at.
const LENGTHS: [usize; 2] = [1_000, 16_000];

/// A counter scraped every `SCRAPE_MS`, rising by a value that varies
/// per sample and resetting every 1000 samples, like a restarting pod.
fn counter(n: usize) -> (Vec<i64>, Vec<f64>) {
    let mut ts = Vec::with_capacity(n);
    let mut vs = Vec::with_capacity(n);
    let mut v = 0.0;
    for i in 0..n {
        if i % 1000 == 0 {
            v = 0.0;
        }
        v += 1.0 + (i % 7) as f64;
        ts.push(i as i64 * SCRAPE_MS);
        vs.push(v);
    }
    (ts, vs)
}

/// The same values with every `every`-th one replaced by Prometheus's
/// staleness marker.
fn with_stale(vs: &[f64], every: usize) -> Vec<f64> {
    vs.iter()
        .enumerate()
        .map(|(i, v)| {
            if i % every == every - 1 {
                f64::from_bits(STALE_NAN_BITS)
            } else {
                *v
            }
        })
        .collect()
}

/// The grid a range query over the whole series evaluates on. Selector
/// and range kernels share one `Params`, so `window_ms` reads as the
/// lookback delta here and as the `[5m]` in [`range_params`].
fn selector_params(n: usize) -> Params {
    Params {
        start_ms: 0,
        end_ms: (n as i64 - 1) * SCRAPE_MS,
        step_ms: STEP_MS,
        window_ms: FIVE_MINUTES_MS,
        offset_ms: 0,
        at_ms: None,
    }
}

fn range_params(n: usize) -> Params {
    selector_params(n)
}

/// `k` series with a sample on every one of `steps` grid points, the
/// shape the aggregate accumulator sees after the vector selector.
fn aligned(k: usize, steps: usize) -> Vec<(Vec<i64>, Vec<f64>)> {
    (0..k)
        .map(|i| {
            let ts = (0..steps).map(|j| j as i64 * STEP_MS).collect();
            let vs = (0..steps).map(|j| (i * 31 + j) as f64 * 0.25).collect();
            (ts, vs)
        })
        .collect()
}

/// The `samples` column of those series, which is what the aggregate's
/// `update_batch` is handed. Routed through the canonical encoder so the
/// list offsets and child layout match what the plan really produces.
fn samples_of(all: &[(Vec<i64>, Vec<f64>)]) -> ArrayRef {
    let series: Vec<Series> = all
        .iter()
        .enumerate()
        .map(|(i, (ts, vs))| {
            let id = format!("s-{i}");
            let labels = [("__name__", "aligned"), ("id", id.as_str())];
            Series::new(&labels, ts.clone(), vs.clone()).unwrap()
        })
        .collect();
    let batch = series::encode(&series::label_names_of(&series), &series).unwrap();
    Arc::clone(batch.column_by_name(series::SAMPLES).unwrap())
}

/// `k` series of `n` samples with five labels, four of them varying.
fn labelled(k: usize, n: usize) -> Vec<Series> {
    (0..k)
        .map(|i| {
            let pod = format!("nginx-{i}");
            let route = format!("/{}", i % 8);
            let zone = format!("eu-west-{}", i % 3);
            let code = if i % 16 == 0 { "500" } else { "200" };
            let labels = [
                ("__name__", "http_requests_total"),
                ("code", code),
                ("pod", pod.as_str()),
                ("route", route.as_str()),
                ("zone", zone.as_str()),
            ];
            let (ts, vs) = counter(n);
            Series::new(&labels, ts, vs).unwrap()
        })
        .collect()
}

/// The elementwise kernels behind `Steps::add_run`: one slice of values
/// folded into as many accumulator lanes. 240 lanes is an hour at 15s,
/// 1440 a day at 60s.
fn math_add_each(c: &mut Criterion) {
    let mut g = c.benchmark_group("math/add_each");
    for lanes in [240usize, 1440] {
        let values: Vec<f64> = (0..lanes).map(|i| 1.0 + (i % 7) as f64).collect();
        g.throughput(Throughput::Elements(lanes as u64));

        let (mut sums, mut comps) = (vec![0.0; lanes], vec![0.0; lanes]);
        g.bench_function(BenchmarkId::new("kahan", lanes), |b| {
            b.iter(|| {
                math::kahan_add_each(&mut sums, &mut comps, black_box(&values));
                black_box(&sums);
            })
        });

        let (mut acc, mut comps, mut counts) =
            (vec![0.0; lanes], vec![0.0; lanes], vec![0.0; lanes]);
        let mut incremental = BooleanBufferBuilder::new(lanes);
        incremental.append_n(lanes, false);
        g.bench_function(BenchmarkId::new("mean", lanes), |b| {
            b.iter(|| {
                math::mean_add_each(
                    &mut acc,
                    &mut comps,
                    &mut counts,
                    FlagLane::new(&mut incremental, 0, lanes),
                    black_box(&values),
                );
                black_box(&acc);
            })
        });

        let mut cur = vec![f64::NAN; lanes];
        g.bench_function(BenchmarkId::new("max", lanes), |b| {
            b.iter(|| {
                math::max_add_each(&mut cur, black_box(&values));
                black_box(&cur);
            })
        });

        let (mut means, mut m2s, mut counts) =
            (vec![0.0; lanes], vec![0.0; lanes], vec![0.0; lanes]);
        g.bench_function(BenchmarkId::new("welford", lanes), |b| {
            b.iter(|| {
                math::welford_add_each(&mut means, &mut m2s, &mut counts, black_box(&values));
                black_box(&means);
            })
        });
    }
    g.finish();
}

/// The sequential reductions behind the `*_over_time` functions.
fn math_reductions(c: &mut Criterion) {
    let mut g = c.benchmark_group("math/reductions");
    for n in LENGTHS {
        let (_, vs) = counter(n);
        g.throughput(Throughput::Elements(n as u64));
        g.bench_function(BenchmarkId::new("kahan_sum", n), |b| {
            b.iter(|| math::kahan_sum(black_box(&vs)).value())
        });
        g.bench_function(BenchmarkId::new("mean_of", n), |b| {
            b.iter(|| math::mean_of(black_box(&vs)))
        });
        // No slice-at-a-time welford reduction exists; `stddev_over_time`
        // folds the accumulator itself, so the bench folds it too.
        g.bench_function(BenchmarkId::new("welford_of", n), |b| {
            b.iter(|| {
                let mut w = Welford::default();
                for v in black_box(&vs) {
                    w.add(*v);
                }
                w.variance()
            })
        });
    }
    g.finish();
}

/// One series walked across the step grid: upstream's `evalSeries` loop.
fn selector_eval_series(c: &mut Criterion) {
    let mut g = c.benchmark_group("selector/eval_series");
    for n in LENGTHS {
        let (ts, vs) = counter(n);
        let stale = with_stale(&vs, 100);
        let p = selector_params(n);
        let pinned = Params {
            at_ms: Some(p.end_ms),
            ..p
        };
        g.throughput(Throughput::Elements(n as u64));
        for (name, vs, p) in [
            ("plain", &vs, &p),
            ("stale_every_100", &stale, &p),
            ("at_pinned", &vs, &pinned),
        ] {
            g.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| {
                    let mut acc = 0.0;
                    selector::eval_series(black_box(&ts), black_box(vs), p, |_, v| acc += v);
                    acc
                })
            });
        }
    }
    g.finish();
}

/// The vector selector over a whole Arrow column of series, allocation
/// of the output included.
fn selector_apply(c: &mut Criterion) {
    let (k, n) = (1_000usize, 1_000usize);
    let all = labelled(k, n);
    let batch = series::encode(&series::label_names_of(&all), &all).unwrap();
    let samples = batch
        .column_by_name(series::SAMPLES)
        .unwrap()
        .as_list::<i32>()
        .clone();
    let p = selector_params(n);
    let mut g = c.benchmark_group("selector/apply");
    g.throughput(Throughput::Elements((k * n) as u64));
    g.bench_function(BenchmarkId::from_parameter(format!("{k}x{n}")), |b| {
        b.iter(|| selector::apply(black_box(&samples), &p))
    });
    g.finish();
}

/// One series through a range function across the step grid. The sweep
/// itself is crate-private, so this drives it through the public column
/// entry point over a single row; the list wrapper is one row's worth of
/// overhead on top of the same walk.
fn range_function(c: &mut Criterion) {
    let mut g = c.benchmark_group("range/range_function");
    for n in LENGTHS {
        let (ts, vs) = counter(n);
        let stale = with_stale(&vs, 100);
        let plain = samples_of(&[(ts.clone(), vs)]);
        let stale = samples_of(&[(ts, stale)]);
        let p = range_params(n);
        g.throughput(Throughput::Elements(n as u64));
        for (name, func, column) in [
            ("rate", Func::Rate, &plain),
            ("increase", Func::Increase, &plain),
            ("count_over_time", Func::CountOverTime, &plain),
            ("max_over_time", Func::MaxOverTime, &plain),
            ("rate_stale_every_100", Func::Rate, &stale),
        ] {
            let column: &ListArray = column.as_list::<i32>();
            g.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| range::apply(func, black_box(column), &p))
            });
        }
    }
    g.finish();
}

/// `rate` over a whole Arrow column of series.
fn range_apply(c: &mut Criterion) {
    let (k, n) = (1_000usize, 1_000usize);
    let all = labelled(k, n);
    let batch = series::encode(&series::label_names_of(&all), &all).unwrap();
    let samples = batch
        .column_by_name(series::SAMPLES)
        .unwrap()
        .as_list::<i32>()
        .clone();
    let p = range_params(n);
    let mut g = c.benchmark_group("range/apply");
    g.throughput(Throughput::Elements((k * n) as u64));
    g.bench_function(BenchmarkId::new("rate", format!("{k}x{n}")), |b| {
        b.iter(|| range::apply(Func::Rate, black_box(&samples), &p))
    });
    g.finish();
}

/// One accumulator filled with a whole `samples` column, all of it in
/// `groups` groups assigned round-robin.
fn filled(op: Op, column: &ArrayRef, groups: &[usize], end_ms: i64, total: usize) -> Grouped {
    let mut acc = Grouped::new(op, 0, end_ms, STEP_MS).unwrap();
    acc.update_batch(std::slice::from_ref(column), groups, None, total)
        .unwrap();
    acc
}

/// Folding `k` aligned series into one accumulator, the partial phase of
/// a cross-series aggregation. The accumulator's allocation is included.
fn aggregate_add_series(c: &mut Criterion) {
    let steps = 1440usize;
    let end_ms = (steps as i64 - 1) * STEP_MS;
    let mut g = c.benchmark_group("aggregate/add_series");
    for k in [100usize, 1_000] {
        let column = samples_of(&aligned(k, steps));
        let one = vec![0usize; k];
        g.throughput(Throughput::Elements((k * steps) as u64));
        for (name, op) in [
            ("sum", Op::Sum),
            ("avg", Op::Avg),
            ("stddev", Op::Stddev),
            ("max", Op::Max),
        ] {
            g.bench_function(BenchmarkId::new(name, k), |b| {
                b.iter(|| filled(op, black_box(&column), &one, end_ms, 1))
            });
        }
    }
    g.finish();
}

/// The shuffle between the two aggregation phases: serializing one
/// accumulator's state and merging it into another. Emitting the state
/// consumes the accumulator's lanes, so each sample refills one outside
/// the timed closure.
fn aggregate_state_merge(c: &mut Criterion) {
    let steps = 1440usize;
    let end_ms = (steps as i64 - 1) * STEP_MS;
    let column = samples_of(&aligned(100, steps));
    let one = vec![0usize; 100];
    let mut g = c.benchmark_group("aggregate/state_merge");
    g.throughput(Throughput::Elements(steps as u64));
    for (name, op) in [("sum", Op::Sum), ("avg", Op::Avg), ("stddev", Op::Stddev)] {
        g.bench_function(BenchmarkId::new(format!("{name}/state"), steps), |b| {
            b.iter_batched(
                || filled(op, &column, &one, end_ms, 1),
                |mut acc| acc.state(EmitTo::All).unwrap(),
                BatchSize::SmallInput,
            )
        });
        let state: ArrayRef = filled(op, &column, &one, end_ms, 1)
            .state(EmitTo::All)
            .unwrap()
            .remove(0);
        g.bench_function(
            BenchmarkId::new(format!("{name}/merge_batch"), steps),
            |b| {
                b.iter(|| {
                    let mut acc = Grouped::new(op, 0, end_ms, STEP_MS).unwrap();
                    acc.merge_batch(std::slice::from_ref(black_box(&state)), &[0], None, 1)
                        .unwrap();
                    acc.evaluate(EmitTo::All).unwrap()
                })
            },
        );
    }
    g.finish();
}

/// The two edges of the seam: rows into a batch and back, plus clipping
/// one series to a range.
fn series_encode_decode(c: &mut Criterion) {
    let (k, n) = (1_000usize, 1_000usize);
    let all = labelled(k, n);
    let names = series::label_names_of(&all);
    let batch = series::encode(&names, &all).unwrap();
    let mut g = c.benchmark_group("series");
    g.throughput(Throughput::Elements((k * n) as u64));
    g.bench_function(BenchmarkId::new("encode", format!("{k}x{n}")), |b| {
        b.iter(|| series::encode(&names, black_box(&all)).unwrap())
    });
    g.bench_function(BenchmarkId::new("decode", format!("{k}x{n}")), |b| {
        b.iter(|| series::decode(std::slice::from_ref(black_box(&batch))).unwrap())
    });
    let (lo, hi) = (n as i64 * SCRAPE_MS / 4, n as i64 * SCRAPE_MS / 2);
    g.bench_function(BenchmarkId::new("clip", format!("{k}x{n}")), |b| {
        b.iter(|| series::clip(black_box(&batch), lo, hi).unwrap())
    });
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(50);
    targets =
        math_add_each,
        math_reductions,
        selector_eval_series,
        selector_apply,
        range_function,
        range_apply,
        aggregate_add_series,
        aggregate_state_merge,
        series_encode_decode
}
criterion_main!(benches);
