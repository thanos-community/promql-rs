use std::hint::black_box;
use std::time::Instant;

use promql_histogram::statistics::{fraction, quantile};
use promql_histogram::{FloatHistogram, Span, TrimDirection, CUSTOM_SCHEMA};

const CORE_ITERATIONS: usize = 50_000;
const ARITHMETIC_ITERATIONS: usize = 2_000;
const SIDE_BUCKETS: usize = 256;
const CUSTOM_BOUNDS: usize = 256;

fn bucket_counts(length: usize, seed: usize) -> Vec<f64> {
    (0..length)
        .map(|index| ((index * 17 + seed) % 23 + 1) as f64)
        .collect()
}

fn exponential_spans(offset: i32) -> Vec<Span> {
    (0..4)
        .map(|span| Span {
            offset: if span == 0 { offset } else { 8 },
            length: (SIDE_BUCKETS / 4) as u32,
        })
        .collect()
}

fn exponential_fixture(schema: i32, seed: usize, zero_threshold: f64) -> FloatHistogram {
    let negative_buckets = bucket_counts(SIDE_BUCKETS, seed);
    let positive_buckets = bucket_counts(SIDE_BUCKETS, seed + 7);
    let zero_count = 11.0;
    let count =
        negative_buckets.iter().sum::<f64>() + zero_count + positive_buckets.iter().sum::<f64>();
    FloatHistogram {
        schema,
        count,
        sum: count * 1.5,
        zero_threshold,
        zero_count,
        negative_spans: exponential_spans(-128),
        negative_buckets,
        positive_spans: exponential_spans(-128),
        positive_buckets,
        ..FloatHistogram::default()
    }
}

fn custom_fixture(step: f64, seed: usize) -> FloatHistogram {
    let custom_values = (0..CUSTOM_BOUNDS)
        .map(|index| -64.0 + index as f64 * step)
        .collect::<Vec<_>>();
    let positive_buckets = bucket_counts(custom_values.len() + 1, seed);
    let count = positive_buckets.iter().sum();
    FloatHistogram {
        schema: CUSTOM_SCHEMA,
        count,
        sum: count * 2.0,
        positive_spans: vec![Span {
            offset: 0,
            length: positive_buckets.len() as u32,
        }],
        positive_buckets,
        custom_values,
        ..FloatHistogram::default()
    }
}

fn benchmark<R>(name: &str, iterations: usize, mut operation: impl FnMut() -> R) {
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(operation());
    }
    let elapsed = started.elapsed();
    let seconds = elapsed.as_secs_f64();
    eprintln!(
        "{name}: {iterations} ops in {elapsed:?} ({:.0} ops/s, {:.1} ns/op)",
        iterations as f64 / seconds,
        seconds * 1e9 / iterations as f64,
    );
}

#[test]
#[ignore = "manual release-mode native histogram benchmark"]
fn benchmark_native_histogram_core_operations() {
    let exponential = exponential_fixture(4, 3, 0.0001);
    let custom = custom_fixture(0.5, 5);
    exponential.validate_ingress().unwrap();
    custom.validate_ingress().unwrap();

    benchmark("exponential validation", CORE_ITERATIONS, || {
        black_box(&exponential).validate_ingress().unwrap()
    });
    benchmark("custom validation", CORE_ITERATIONS, || {
        black_box(&custom).validate_ingress().unwrap()
    });
    benchmark("exponential traversal", CORE_ITERATIONS, || {
        black_box(&exponential)
            .all_buckets()
            .fold(0.0, |sum, bucket| sum + bucket.count)
    });
    benchmark("custom traversal", CORE_ITERATIONS, || {
        black_box(&custom)
            .all_buckets()
            .fold(0.0, |sum, bucket| sum + bucket.count)
    });
    benchmark("exponential quantile", CORE_ITERATIONS, || {
        quantile(black_box(0.73), black_box(&exponential))
    });
    benchmark("custom quantile", CORE_ITERATIONS, || {
        quantile(black_box(0.73), black_box(&custom))
    });
    benchmark("exponential fraction", CORE_ITERATIONS, || {
        fraction(black_box(-3.0), black_box(20.0), black_box(&exponential))
    });
    benchmark("custom fraction", CORE_ITERATIONS, || {
        fraction(black_box(-12.5), black_box(23.5), black_box(&custom))
    });

    let mut exponential_trims = vec![exponential.clone(); ARITHMETIC_ITERATIONS];
    let mut exponential_trims = exponential_trims.iter_mut();
    benchmark("exponential upper trim", ARITHMETIC_ITERATIONS, || {
        exponential_trims
            .next()
            .unwrap()
            .trim_buckets(black_box(3.0), TrimDirection::Upper)
            .count
    });

    let mut custom_trims = vec![custom.clone(); ARITHMETIC_ITERATIONS];
    let mut custom_trims = custom_trims.iter_mut();
    benchmark("custom lower trim", ARITHMETIC_ITERATIONS, || {
        custom_trims
            .next()
            .unwrap()
            .trim_buckets(black_box(3.0), TrimDirection::Lower)
            .count
    });

    let exponential_rhs = exponential_fixture(3, 11, 0.01);
    let mut exponential_results = vec![exponential.clone(); ARITHMETIC_ITERATIONS];
    let mut exponential_results = exponential_results.iter_mut();
    benchmark("exponential add/reconcile", ARITHMETIC_ITERATIONS, || {
        let result = exponential_results.next().unwrap();
        result.add(black_box(&exponential_rhs)).unwrap();
        result.count
    });

    let custom_rhs = custom_fixture(1.0, 13);
    let mut custom_results = vec![custom.clone(); ARITHMETIC_ITERATIONS];
    let mut custom_results = custom_results.iter_mut();
    benchmark("custom add/reconcile", ARITHMETIC_ITERATIONS, || {
        let result = custom_results.next().unwrap();
        result.add(black_box(&custom_rhs)).unwrap();
        result.count
    });
}
