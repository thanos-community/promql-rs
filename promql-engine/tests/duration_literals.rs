//! A float-seconds range, offset or `@` reaches the engine the way
//! Prometheus gets it there: a duration rounds to nanoseconds and then
//! truncates to milliseconds, `@` rounds `secs * 1000`, and what Go
//! cannot hold is an error instead of a clamped value.

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery};

/// One sample, `5`, at t=1000ms.
fn source() -> MemorySeriesSource {
    let desc = promql_parser::parse_series_desc("x _ 5").expect("series line parses");
    MemorySeriesSource::from_descriptions(&[desc], 1.0)
}

type Rows = Vec<(Vec<i64>, Vec<f64>)>;

fn at(query: &str, ts_ms: i64) -> Result<Rows, EngineError> {
    let engine = Engine::blocking().unwrap();
    let batches = engine.range_query(&source(), query, &RangeQuery::new(ts_ms, ts_ms, 1000))?;
    let out = promql_engine::series::decode(&batches).unwrap();
    Ok(out
        .iter()
        .map(|s| (s.timestamps().to_vec(), s.values().to_vec()))
        .collect())
}

fn at_1001(query: &str) -> Rows {
    at(query, 1001).unwrap()
}

#[test]
fn a_fractional_offset_truncates_to_milliseconds() {
    let one_ms = at_1001("x offset 1ms");
    assert_eq!(one_ms.len(), 1, "the sample at 1000ms is in reach");
    // Go: Round(0.0015 * 1e9) = 1_500_000ns, then / 1_000_000 = 1ms.
    // Rounding seconds * 1000 made it 2ms, which looks before 1000ms.
    assert_eq!(at_1001("x offset 0.0015"), one_ms);
    assert!(at_1001("x offset 2ms").is_empty());
}

#[test]
fn a_negative_offset_truncates_toward_zero() {
    // -1.5ms is -1ms: from 999 it looks at 1000 and finds the sample.
    let one_ms = at("x offset -1ms", 999).unwrap();
    assert_eq!(one_ms.len(), 1);
    assert_eq!(at("x offset -0.0015", 999).unwrap(), one_ms);
    // From 998 it looks at 999 and finds nothing; truncating away from
    // zero would be -2ms, which reaches 1000.
    assert!(at("x offset -1ms", 998).unwrap().is_empty());
    assert!(at("x offset -0.0015", 998).unwrap().is_empty());
    assert_eq!(at("x offset -2ms", 998).unwrap().len(), 1);
}

#[test]
fn a_fractional_range_truncates_to_milliseconds() {
    let one_ms = at_1001("count_over_time(x[1ms])");
    // The window is (t - range, t]; at 1001 a 1ms window misses 1000.
    assert!(one_ms.is_empty());
    assert_eq!(at_1001("count_over_time(x[0.0015])"), one_ms);
    assert_eq!(at_1001("count_over_time(x[2ms])").len(), 1);
}

#[test]
fn a_fractional_range_sets_the_rate_divisor() {
    let desc = promql_parser::parse_series_desc("y _ _ _ _ _ 0 1 3 6").unwrap();
    let source = MemorySeriesSource::from_descriptions(&[desc], 1.0);
    let rate = |q: &str| {
        let engine = Engine::blocking().unwrap();
        let batches = engine
            .range_query(&source, q, &RangeQuery::new(6000, 6000, 1000))
            .unwrap();
        let out = promql_engine::series::decode(&batches).unwrap();
        out[0].values()[0]
    };
    // 3.0015s is 3001ms; rounding seconds * 1000 made it 3002ms. The
    // window starts well before the first sample, so extrapolation stops
    // half an interval short and the divisor is not cancelled out.
    let truncated = rate("rate(y[3001ms])");
    assert_eq!(rate("rate(y[3.0015])"), truncated);
    assert_ne!(rate("rate(y[3002ms])"), truncated);
}

#[test]
fn the_at_modifier_rounds_to_milliseconds() {
    // Go: FromFloatSeconds rounds, so 1.0004 is 1000ms and 1.0006 is 1001ms.
    assert_eq!(at_1001("x @ 1.0004"), at_1001("x @ 1"));
    assert_eq!(at_1001("x @ 1.0004").len(), 1);
    // Truncating 999.6 would give 999ms, before the sample.
    assert_eq!(at_1001("x @ 0.9996"), at_1001("x @ 1"));
}

#[test]
fn an_at_the_parser_rejects_is_an_error() {
    for q in [
        "x @ 1e400",
        "x @ NaN",
        "x @ Inf",
        "x @ -Inf",
        "x @ 1e300",
        "x @ -1e300",
    ] {
        let err = at(q, 1001).unwrap_err();
        assert!(matches!(err, EngineError::Parse(_)), "{q}: {err}");
    }
}

#[test]
fn an_offset_or_range_go_cannot_hold_is_a_query_error() {
    for q in [
        "x offset Inf",
        "x offset NaN",
        "x offset -Inf",
        "x offset 1e10",
        "count_over_time(x[Inf])",
        "count_over_time(x[1e10])",
    ] {
        let err = at(q, 1001).unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{q}: {err}");
    }
}

#[test]
fn integer_millisecond_durations_are_unchanged() {
    assert_eq!(at_1001("x offset 1ms").len(), 1);
    assert_eq!(at_1001("x offset 1s").len(), 0);
    assert_eq!(at("x offset 1s", 2000).unwrap().len(), 1);
    assert_eq!(at_1001("x @ 1").len(), 1);
}
