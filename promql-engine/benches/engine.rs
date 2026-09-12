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
    Arc::new(MemorySeriesSource::new(all))
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
                                .range_query(Arc::clone(&source), black_box(q), range)
                                .unwrap()
                        })
                    },
                );
            }
        }
    }
    g.finish();
}

/// Parsing and planning alone. The store's `select` runs here, so the gap
/// to `engine/query` is execution proper.
fn plan(c: &mut Criterion) {
    let engine = Engine::blocking().unwrap();
    let (qname, q) = QUERIES[4];
    let mut g = c.benchmark_group("engine/plan");
    for shape in &SHAPES {
        let source = synthetic(shape);
        let end = shape.end_ms();
        let range = RangeQuery::new(end - HOUR_MS, end, STEP_MS);
        g.throughput(Throughput::Elements(shape.series as u64));
        g.bench_function(
            BenchmarkId::new(format!("{qname}/range_1h"), shape.id()),
            |b| b.iter(|| engine.plan(source.as_ref(), black_box(q), &range).unwrap()),
        );
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20);
    targets = query, plan
}
criterion_main!(benches);
