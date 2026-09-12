//! Parser benchmarks: `parse_expr` over representative queries and
//! `parse_series_desc` over a promqltest `load` line. Ids are
//! `parser/parse_expr/<name>`, chosen once; throughput is bytes of input.
//!
//! ```sh
//! cargo bench -p promql-parser -- --save-baseline before
//! # change code
//! cargo bench -p promql-parser -- --baseline before
//! ```

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

const QUERIES: [(&str, &str); 10] = [
    ("selector", "http_requests_total"),
    (
        "matchers",
        r#"http_requests_total{job="api", instance=~"10\\.0\\..*:9090", code!="500", method=~"GET|POST"}"#,
    ),
    ("sum_by", "sum by (job, instance) (http_requests_total)"),
    ("rate", "rate(http_requests_total[5m])"),
    (
        "nested",
        "max(sum by (route) (increase(http_requests_total[5m])))",
    ),
    (
        "ratio",
        r#"sum(rate(http_requests_total{code=~"5.."}[5m])) / sum(rate(http_requests_total[5m])) > 0.01"#,
    ),
    (
        "subquery",
        "max_over_time(rate(http_requests_total[5m])[1h:1m])",
    ),
    ("offset_at", "http_requests_total offset 5m @ 1609459200"),
    (
        "histogram_quantile",
        "histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))",
    ),
    (
        "arithmetic",
        "(a + b) * (c - d) / (e + f) and (g or h) unless i",
    ),
];

fn parse_expr(c: &mut Criterion) {
    let mut g = c.benchmark_group("parser/parse_expr");
    for (name, q) in QUERIES {
        if let Err(e) = promql_parser::parse_expr(q) {
            panic!("{name}: {e:?}");
        }
        g.throughput(Throughput::Bytes(q.len() as u64));
        g.bench_function(name, |b| {
            b.iter(|| promql_parser::parse_expr(black_box(q)).unwrap())
        });
    }
    g.finish();
}

/// A `load` line whose values expand to a thousand samples, the cost of
/// seeding one series in the conformance suite.
fn parse_series_desc(c: &mut Criterion) {
    let line = r#"http_requests_total{pod="nginx-1", route="/api", code="200"} 0+10x1000"#;
    let mut g = c.benchmark_group("parser/parse_series_desc");
    g.throughput(Throughput::Bytes(line.len() as u64));
    g.bench_function("counter_1000", |b| {
        b.iter(|| promql_parser::parse_series_desc(black_box(line)).unwrap())
    });
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = parse_expr, parse_series_desc
}
criterion_main!(benches);
