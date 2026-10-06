//! The engine parses with the `ParserOptions` it was built with, on every
//! path a query string takes in. Which construct each gate guards, and
//! with what message, is `promql-parser/tests/feature_flags.rs`; this file
//! only pins that the engine hands its options to the parser and does not
//! keep a second set of defaults.

use promql_engine::{Engine, EngineError, EngineOptions, MemorySeriesSource, RangeQuery};
use promql_parser::ParserOptions;

const FUNCTIONS: ParserOptions = ParserOptions {
    enable_experimental_functions: true,
    experimental_duration_expr: false,
    enable_extended_range_selectors: false,
    enable_binop_fill_modifiers: false,
};

fn source() -> MemorySeriesSource {
    let descriptions: Vec<_> = ["x 1 2 3", "y 4 5 6"]
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .into();
    MemorySeriesSource::from_descriptions(&descriptions, 30.0)
}

fn range() -> RangeQuery {
    RangeQuery::new(0, 60_000, 30_000)
}

/// The refusal a stock engine gives, as the parser words it.
fn is_the_gate(err: &EngineError) -> bool {
    matches!(err, EngineError::Parse(e)
        if e.iter().any(|e| e.message == r#"function "mad_over_time" is not enabled"#))
}

const QUERY: &str = "mad_over_time(x[1m])";

#[test]
fn a_stock_engine_refuses_experimental_syntax_on_every_path() {
    let blocking = Engine::blocking().unwrap();
    let err = blocking
        .range_query(&source(), QUERY, &range())
        .unwrap_err();
    assert!(is_the_gate(&err), "range_query: {err}");

    let engine = Engine::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let err = rt
        .block_on(engine.range_query_async(&source(), QUERY, &range()))
        .unwrap_err();
    assert!(is_the_gate(&err), "range_query_async: {err}");
    let err = rt
        .block_on(engine.plan_async(&source(), QUERY, &range()))
        .unwrap_err();
    assert!(is_the_gate(&err), "plan_async: {err}");
    let err = rt
        .block_on(engine.physical_plan_async(&source(), QUERY, &range()))
        .map(|_| ())
        .unwrap_err();
    assert!(is_the_gate(&err), "physical_plan_async: {err}");

    let default = Engine::with_options(EngineOptions::default());
    let err = rt
        .block_on(default.plan_async(&source(), QUERY, &range()))
        .unwrap_err();
    assert!(is_the_gate(&err), "with_options(default): {err}");
}

/// With the gate open the query gets past the parser. This engine has no
/// `mad_over_time`, so it stops there, with its own error rather than the
/// parser's.
#[test]
fn an_engine_built_with_the_flag_parses_it_on_every_path() {
    let options = EngineOptions { parser: FUNCTIONS };
    let blocking = Engine::blocking_with_options(options.clone()).unwrap();
    let err = blocking
        .range_query(&source(), QUERY, &range())
        .unwrap_err();
    assert!(matches!(err, EngineError::Unsupported(_)), "{err}");

    let engine = Engine::with_options(options);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for (path, result) in [
        (
            "range_query_async",
            rt.block_on(engine.range_query_async(&source(), QUERY, &range()))
                .map(|_| ()),
        ),
        (
            "plan_async",
            rt.block_on(engine.plan_async(&source(), QUERY, &range()))
                .map(|_| ()),
        ),
        (
            "physical_plan_async",
            rt.block_on(engine.physical_plan_async(&source(), QUERY, &range()))
                .map(|_| ()),
        ),
    ] {
        let err = result.unwrap_err();
        assert!(matches!(err, EngineError::Unsupported(_)), "{path}: {err}");
    }
}

/// A flag does not leak into the others: this engine admits experimental
/// functions and still refuses fill.
#[test]
fn each_flag_opens_only_its_own_gate() {
    let engine = Engine::with_options(EngineOptions { parser: FUNCTIONS });
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let err = rt
        .block_on(engine.plan_async(&source(), "x + fill(0) y", &range()))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::Parse(e)
            if e.iter().any(|e| e.message == "binop fill modifiers are experimental and not enabled")),
        "{err}"
    );
}
