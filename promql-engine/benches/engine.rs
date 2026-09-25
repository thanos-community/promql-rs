//! End-to-end benchmarks: parse, plan, execute and decode a query over
//! the in-memory source, DataFusion included.
//!
//! One engine, with its Tokio runtime and UDF registrations, and one
//! source per shape are built outside the timed closure, so an iteration
//! is what a caller of `Engine::range_query` pays. `engine/plan` isolates
//! parsing and planning, which includes the store's `select`, from
//! execution. Ids are `engine/query/<query>/<range>/<series>x<samples>`
//! and are chosen once: renaming a benchmark resets its history.
//!
//! ```sh
//! cargo bench -p promql-engine --bench engine -- --save-baseline before
//! # change code
//! cargo bench -p promql-engine --bench engine -- --baseline before
//! ```

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use promql_engine::{Engine, MemorySeriesSource, RangeQuery, Series, SeriesSource};

#[path = "support/pprof.rs"]
mod profiler;

use profiler::PProfProfiler;

const SCRAPE_MS: i64 = 15_000;
const STEP_MS: i64 = 30_000;
const HOUR_MS: i64 = 60 * 60_000;

/// Cardinality and length of a synthetic source.
struct Shape {
    series: usize,
    samples: usize,
}

const SHAPES: [Shape; 3] = [
    Shape {
        series: 100,
        samples: 1_000,
    },
    Shape {
        series: 1_000,
        samples: 1_000,
    },
    Shape {
        series: 10_000,
        samples: 300,
    },
];

impl Shape {
    fn id(&self) -> String {
        format!("{}x{}", self.series, self.samples)
    }

    fn end_ms(&self) -> i64 {
        (self.samples as i64 - 1) * SCRAPE_MS
    }

    fn elements(&self) -> u64 {
        (self.series * self.samples) as u64
    }
}

/// Counters named `http_requests_total` with `pod`, `route` and `code`
/// labels, scraped every 15s, resetting every 1000 samples.
fn synthetic(shape: &Shape) -> Arc<dyn SeriesSource> {
    let all = (0..shape.series)
        .map(|i| {
            let pod = format!("nginx-{i}");
            let route = format!("/{}", i % 8);
            let code = if i % 16 == 0 { "500" } else { "200" };
            let labels = [
                ("__name__", "http_requests_total"),
                ("code", code),
                ("pod", pod.as_str()),
                ("route", route.as_str()),
            ];
            let mut v = 0.0;
            let (ts, vs): (Vec<i64>, Vec<f64>) = (0..shape.samples)
                .map(|j| {
                    if j % 1000 == 0 {
                        v = 0.0;
                    }
                    v += 1.0 + ((i + j) % 7) as f64;
                    (j as i64 * SCRAPE_MS, v)
                })
                .unzip();
            Series::new(&labels, ts, vs).unwrap()
        })
        .collect();
    Arc::new(MemorySeriesSource::try_new(all).unwrap())
}

/// The queries, from a bare selector to the SLO recording-rule shape.
const QUERIES: [(&str, &str); 5] = [
    ("selector", "http_requests_total"),
    ("sum", "sum(http_requests_total)"),
    ("sum_by_route", "sum by (route) (http_requests_total)"),
    ("rate_5m", "rate(http_requests_total[5m])"),
    (
        "sum_by_route_increase_5m",
        "sum by (route) (increase(http_requests_total[5m]))",
    ),
];

/// Every query at every shape, over the last hour of data at 30s and as
/// an instant query at the last sample.
fn query(c: &mut Criterion) {
    let engine = Engine::blocking().unwrap();
    let mut g = c.benchmark_group("engine/query");
    for shape in &SHAPES {
        let source = synthetic(shape);
        let end = shape.end_ms();
        let ranges = [
            ("range_1h", RangeQuery::new(end - HOUR_MS, end, STEP_MS)),
            ("instant", RangeQuery::new(end, end, STEP_MS)),
        ];
        g.throughput(Throughput::Elements(shape.elements()));
        for (qname, q) in QUERIES {
            for (rname, range) in &ranges {
                g.bench_function(
                    BenchmarkId::new(format!("{qname}/{rname}"), shape.id()),
                    |b| {
                        b.iter(|| {
                            engine
                                .range_query(source.as_ref(), black_box(q), range)
                                .unwrap()
                        })
                    },
                );
            }
        }
    }
    g.finish();
}

/// The elementwise operator's own cost, over the last hour at 30s.
///
/// Each case is a function wrapped round a query `engine/query` already
/// measures, so the difference is one more projection and one pass over
/// the values: `abs` is the cheapest of them, and `clamp` carries two
/// scalar arguments through the same pass. Ids are
/// `engine/elementwise/<query>/<series>x<samples>`.
fn elementwise(c: &mut Criterion) {
    let engine = Engine::blocking().unwrap();
    let shape = &SHAPES[2];
    let source = synthetic(shape);
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    let mut g = c.benchmark_group("engine/elementwise");
    g.throughput(Throughput::Elements(shape.elements()));
    for (qname, q) in [
        ("abs_selector", "abs(http_requests_total)"),
        ("clamp_selector", "clamp(http_requests_total, 0, 1000)"),
        ("abs_rate_5m", "abs(rate(http_requests_total[5m]))"),
    ] {
        g.bench_function(BenchmarkId::new(qname, shape.id()), |b| {
            b.iter(|| {
                engine
                    .range_query(source.as_ref(), black_box(q), &range)
                    .unwrap()
            })
        });
    }
    g.finish();
}

/// The two shapes a binary operator takes, over the same source.
///
/// A scalar operand is folded while planning, so `vector_scalar` is one
/// more pass over the values and nothing else — the same price as
/// `engine/elementwise`. `vector_vector` is what the pairing costs: both
/// sides widened to one schema, unioned, and grouped by the match
/// signature, which over this source is one group per series. Ids are
/// `engine/binary/<shape>/<series>x<samples>`.
fn binary(c: &mut Criterion) {
    let engine = Engine::blocking().unwrap();
    let shape = &SHAPES[2];
    let source = synthetic(shape);
    let end = shape.end_ms();
    let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
    let mut g = c.benchmark_group("engine/binary");
    g.throughput(Throughput::Elements(shape.elements()));
    for (qname, q) in [
        ("vector_scalar", "http_requests_total * 2"),
        // A comparison is the one that rebuilds the row boundaries
        // rather than only the values, and over this source it keeps
        // most of them, which is the expensive half of that path.
        ("vector_scalar_compare", "http_requests_total > 100"),
        ("vector_vector", "http_requests_total + http_requests_total"),
        // The fan-out: every series of a route matches the one sum of
        // it, so a match group here is a lane per series rather than
        // the single pair the cases above pay for.
        (
            "group_left",
            "http_requests_total / on(route) group_left() sum by (route) (http_requests_total)",
        ),
        // A set operator holds both sides of every match group whole,
        // and `or` is the one that then emits both — the widest a group
        // gets for the least arithmetic.
        (
            "or",
            "http_requests_total or on(route) sum by (route) (http_requests_total)",
        ),
        // A fill turns the sides that did not match from something to
        // skip into something to answer for, which is the per-step
        // bookkeeping at its widest.
        (
            "fill",
            "http_requests_total / on(route) group_left() fill(0) sum by (route) (http_requests_total)",
        ),
    ] {
        g.bench_function(BenchmarkId::new(qname, shape.id()), |b| {
            b.iter(|| {
                engine
                    .range_query(source.as_ref(), black_box(q), &range)
                    .unwrap()
            })
        });
    }
    g.finish();
}

/// Parsing and planning alone. The store's `select` runs here, so the gap
/// to `engine/query` is execution proper.
fn plan(c: &mut Criterion) {
    let engine = Engine::new();
    // Planning is only exposed as `plan_async`, and the runtime an
    // `Engine::blocking()` owns is private, so the bench drives its own.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (qname, q) = QUERIES[4];
    let mut g = c.benchmark_group("engine/plan");
    for shape in &SHAPES {
        let source = synthetic(shape);
        let end = shape.end_ms();
        let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
        g.throughput(Throughput::Elements(shape.series as u64));
        g.bench_function(
            BenchmarkId::new(format!("{qname}/range_1h"), shape.id()),
            |b| {
                b.iter(|| {
                    rt.block_on(engine.plan_async(source.as_ref(), black_box(q), &range))
                        .unwrap()
                })
            },
        );
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100))
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20);
    targets = query, elementwise, binary, plan
}
criterion_main!(benches);
