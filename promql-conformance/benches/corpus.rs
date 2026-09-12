//! Replay of the Prometheus corpus through the engine, oracle not involved.
//!
//! Gated on `PROMQL_ENGINE_TESTCASES` like the differential suite; without
//! it the bench prints a line and registers nothing. One iteration seeds
//! the in-memory source from each evaluable case's `load` block and runs
//! its query, which is what a conformance run pays per case. Only cases
//! the engine answers today are replayed, so the number of cases behind
//! `corpus/replay` grows as the engine does; the throughput figure is
//! cases per second.
//!
//! ```sh
//! PROMQL_ENGINE_TESTCASES=~/src/github.com/thanos-io/promql-engine/testcases \
//!   cargo bench -p promql-conformance
//! ```

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use promql_conformance::{DataFusionEngine, Engine, QueryResult};
use promql_parser::SeriesDescription;
use promql_testcases::{range_queries_in, testcases_dir, Case};

fn seed(case: &Case) -> (Vec<SeriesDescription>, f64) {
    let series = case
        .load
        .as_ref()
        .map(|l| l.parsed().cloned().collect())
        .unwrap_or_default();
    let interval = case.load.as_ref().map(|l| l.interval_secs).unwrap_or(0.0);
    (series, interval)
}

fn replay(c: &mut Criterion) {
    let Some(dir) = testcases_dir() else {
        eprintln!(
            "skipping corpus/replay: {} is not set",
            promql_testcases::TESTCASES_DIR_ENV
        );
        return;
    };
    let cases = range_queries_in(&dir).expect("the corpus loads");
    let engine = DataFusionEngine::new().expect("the engine starts");

    // Everything else would time an error path.
    let evaluable: Vec<(Case, Vec<SeriesDescription>, f64)> = cases
        .into_iter()
        .filter_map(|case| {
            let (series, interval) = seed(&case);
            let result = engine.range_query(
                &series,
                interval,
                &case.query,
                case.start_ms,
                case.end_ms,
                case.step_ms,
            );
            match result {
                Ok(QueryResult::Matrix(_)) => Some((case, series, interval)),
                _ => None,
            }
        })
        .collect();

    let mut g = c.benchmark_group("corpus");
    g.throughput(Throughput::Elements(evaluable.len() as u64));
    g.bench_function("replay", |b| {
        b.iter(|| {
            for (case, series, interval) in &evaluable {
                let result = engine
                    .range_query(
                        black_box(series),
                        *interval,
                        &case.query,
                        case.start_ms,
                        case.end_ms,
                        case.step_ms,
                    )
                    .unwrap();
                black_box(result);
            }
        })
    });
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .sample_size(10);
    targets = replay
}
criterion_main!(benches);
