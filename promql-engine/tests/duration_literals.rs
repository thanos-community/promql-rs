//! A float-seconds `@`, offset or range that does not fit `i64`
//! milliseconds is an error instead of a clamped or zeroed value.

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery};

fn run(query: &str) -> Result<(), EngineError> {
    let desc = promql_parser::parse_series_desc("x _ 5").expect("series line parses");
    let source = MemorySeriesSource::from_descriptions(&[desc], 1.0);
    let engine = Engine::blocking().unwrap();
    engine
        .range_query(&source, query, &RangeQuery::new(1001, 1001, 1000))
        .map(|_| ())
}

#[test]
fn an_at_the_parser_rejects_is_an_error() {
    // `1e400` overflows f64, so the number literal itself is the error.
    for q in ["x @ 1e400", "x @ NaN", "x @ Inf", "x @ -Inf", "x @ 1e300"] {
        let err = run(q).unwrap_err();
        assert!(matches!(err, EngineError::Parse(_)), "{q}: {err}");
    }
}

#[test]
fn an_offset_or_range_that_overflows_is_a_query_error() {
    // NaN is not past either bound, so it clears the parser's check and
    // the engine rejects it, as upstream's conversion cannot hold it.
    for q in ["x offset NaN", "count_over_time(x[NaN])"] {
        let err = run(q).unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{q}: {err}");
    }
}

/// Past `time.Duration` the grammar's own range check refuses the literal
/// (`duration out of range`), before any engine sees it.
#[test]
fn a_literal_past_the_longest_duration_is_a_parse_error() {
    for q in [
        "x offset Inf",
        "x offset -1e300",
        "count_over_time(x[Inf])",
        "count_over_time(x[1e300])",
    ] {
        let err = run(q).unwrap_err();
        assert!(matches!(err, EngineError::Parse(_)), "{q}: {err}");
    }
}
