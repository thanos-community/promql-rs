//! A `SeriesSource` that hands the engine one series as several rows, one
//! per time chunk, must get the same answer as one that hands it one row.
//!
//! `MemorySeriesSource::chunked` is the test mode (see its doc in
//! `memory.rs`) that splits each selected series' samples into consecutive
//! rows of at most `CHUNK_MS` span before handing them to the plan, series
//! order then time order preserved. Every test here runs the same query
//! against a chunked and an unchunked source built from the same
//! descriptions, and compares the two: any difference is a bug in how the
//! selector and range aggregates carry a series across rows.

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

/// One chunk row per batch, so every chunk boundary is a batch boundary
/// too, the harder case for carrying a series.
fn chunked() -> Arc<MemorySeriesSource> {
    Arc::new(
        MemorySeriesSource::from_descriptions(&descriptions(), 30.0)
            .chunked(CHUNK_MS)
            .rows_per_batch(1),
    )
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
    assert_same_on(chunked().as_ref(), q, range);
}

/// [`assert_same_as_unchunked`] for any source built from [`descriptions`].
fn assert_same_on(source: &MemorySeriesSource, q: &str, range: RangeQuery) {
    let mut expected = query(plain().as_ref(), q, range);
    let mut actual = query(source, q, range);
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

/// Both sides of the match read the same series across chunk boundaries,
/// so the carry has to be right twice over. Kept here rather than added
/// with the operator so that branch turns it on without rediscovering it.
#[test]
#[ignore = "binary operators are not supported yet"]
fn binary_op_matching_the_selector_with_itself() {
    assert_same_as_unchunked("x + x", RangeQuery::new(300_000, 300_000, 30_000));
}

/// Four partitions put the selector under a Partial and a Final aggregate
/// with a merge between them, a shape one partition never plans.
#[test]
fn chunks_over_four_partitions() {
    let source = MemorySeriesSource::from_descriptions(&descriptions(), 30.0)
        .chunked(CHUNK_MS)
        .rows_per_batch(1)
        .partitions(4);
    for q in ["x", "rate(x[5m])", "sum(x)", "count_over_time(x[10m])"] {
        assert_same_on(&source, q, RangeQuery::new(300_000, 600_000, 30_000));
    }
}

/// Many steps, so each step's window is finalised while later chunks are
/// still arriving rather than all at series close.
fn multi_step() -> RangeQuery {
    RangeQuery::new(0, 600_000, 30_000)
}

#[test]
fn offset_across_chunks() {
    for q in ["x offset 2m", "rate(x[5m] offset 2m)"] {
        assert_same_as_unchunked(q, multi_step());
    }
}

#[test]
fn at_across_chunks() {
    // One `@` inside the data, one on a chunk boundary, one after the end.
    for q in [
        "x @ 200",
        "rate(x[5m] @ 300)",
        "count_over_time(x[10m] @ 450)",
        "x @ 900",
    ] {
        assert_same_as_unchunked(q, multi_step());
    }
}

#[test]
fn extrema_across_chunks() {
    for q in ["min_over_time(x[5m])", "max_over_time(x[5m])"] {
        assert_same_as_unchunked(q, multi_step());
    }
}

#[test]
fn irate_across_chunks() {
    assert_same_as_unchunked("irate(x[1m])", multi_step());
}

#[test]
fn every_query_over_many_steps() {
    for q in ["x", "rate(x[5m])", "sum(x)", "count_over_time(x[10m])"] {
        assert_same_as_unchunked(q, multi_step());
    }
}

/// `chunked(0)`: one sample per row, so every window crosses rows.
#[test]
fn one_sample_per_row() {
    let source = MemorySeriesSource::from_descriptions(&descriptions(), 30.0)
        .chunked(0)
        .rows_per_batch(1);
    for q in [
        "x",
        "rate(x[5m])",
        "sum(x)",
        "count_over_time(x[10m])",
        "irate(x[1m])",
        "max_over_time(x[5m] offset 1m)",
    ] {
        assert_same_on(&source, q, multi_step());
    }
}

/// Partition counts below, at and above the number of series, alone and
/// under chunking: a series lands whole in one partition either way. Rows
/// one to a batch, three, which puts a batch boundary inside a series and
/// two series in one batch, and packed as a store packs them.
#[test]
fn partitions_match_unpartitioned() {
    for n in [1, 2, 4, 7] {
        for (chunk_ms, rows) in [
            (None, 8192),
            (Some(0), 1),
            (Some(0), 3),
            (Some(CHUNK_MS), 1),
            (Some(CHUNK_MS), 3),
            (Some(CHUNK_MS), 8192),
        ] {
            let mut source = MemorySeriesSource::from_descriptions(&descriptions(), 30.0);
            if let Some(ms) = chunk_ms {
                source = source.chunked(ms);
            }
            let source = source.rows_per_batch(rows).partitions(n);
            for q in [
                "x",
                "rate(x[5m])",
                "sum(x)",
                "sum by (pod) (rate(x[5m]))",
                "count_over_time(x[10m])",
            ] {
                assert_same_on(&source, q, multi_step());
            }
        }
    }
}
