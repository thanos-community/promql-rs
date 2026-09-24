//! Reproduction only: what happens when a `SeriesSource` hands the engine
//! one series as several rows (one per time chunk) instead of one.
//!
//! `MemorySeriesSource::chunked` is a test-only knob (see its doc in
//! `memory.rs`) that splits each selected series' samples into consecutive
//! rows of at most `CHUNK_MS` span before handing them to the plan, still
//! one partition, series order then time order preserved. Every test here
//! runs the same query against a chunked and an unchunked source built from
//! the same descriptions, and compares the two: any difference is a bug
//! this repository does not yet claim to fix.

use std::sync::Arc;

use promql_engine::{Engine, MemorySeriesSource, RangeQuery, Series};
use promql_parser::SeriesDescription;

/// Short enough that a 5m/10m window, and even a one-step selector's
/// lookback, always straddles at least one chunk boundary: 20 samples at
/// 30s apart span 570s, so 150s chunks give each series ~4 chunks.
const CHUNK_MS: i64 = 150_000;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two counters, still one series each, so any difference between the
/// chunked and unchunked runs comes from chunking rather than from the
/// data itself.
fn descriptions() -> Vec<SeriesDescription> {
    load(&[r#"x{pod="a"} 1+1x19"#, r#"x{pod="b"} 2+3x19"#])
}

fn plain() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(&descriptions(), 30.0))
}

fn chunked() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(&descriptions(), 30.0).chunked(CHUNK_MS))
}

fn query(source: &MemorySeriesSource, q: &str, range: RangeQuery) -> Vec<Series> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(source, q, &range)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    promql_engine::series::decode(&batches).unwrap()
}

fn key(s: &Series) -> String {
    s.labels()
        .map(|(n, v)| format!("{n}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Compare a chunked run against the same query on the unchunked source:
/// same series (by label set), same timestamps, same values.
fn assert_same_as_unchunked(q: &str, range: RangeQuery) {
    let mut expected = query(plain().as_ref(), q, range);
    let mut actual = query(chunked().as_ref(), q, range);
    expected.sort_by_key(key);
    actual.sort_by_key(key);

    let expected_keys: Vec<_> = expected.iter().map(key).collect();
    let actual_keys: Vec<_> = actual.iter().map(key).collect();
    assert_eq!(
        expected_keys, actual_keys,
        "{q}: chunked source returned a different set of series"
    );
    for (e, a) in expected.iter().zip(&actual) {
        assert_eq!(
            e.timestamps(),
            a.timestamps(),
            "{q} ({}): timestamps differ",
            key(e)
        );
        assert_eq!(
            e.values().len(),
            a.values().len(),
            "{q} ({}): value count differs",
            key(e)
        );
        for (ev, av) in e.values().iter().zip(a.values()) {
            assert!(
                (ev - av).abs() < 1e-9,
                "{q} ({}): expected {:?}, got {:?}",
                key(e),
                e.values(),
                a.values()
            );
        }
    }
}

#[test]
fn plain_selector_at_one_step() {
    assert_same_as_unchunked("x", RangeQuery::new(300_000, 300_000, 30_000));
}

#[test]
fn rate_across_a_chunk_boundary() {
    // A 5m window is twice CHUNK_MS, so it always spans several chunks.
    assert_same_as_unchunked("rate(x[5m])", RangeQuery::new(300_000, 300_000, 30_000));
}

#[test]
fn sum_of_the_series() {
    assert_same_as_unchunked("sum(x)", RangeQuery::new(300_000, 300_000, 30_000));
}

#[test]
fn count_over_time_across_chunks() {
    assert_same_as_unchunked(
        "count_over_time(x[10m])",
        RangeQuery::new(300_000, 300_000, 30_000),
    );
}

#[test]
fn binary_op_matching_the_selector_with_itself() {
    assert_same_as_unchunked("x + x", RangeQuery::new(300_000, 300_000, 30_000));
}
