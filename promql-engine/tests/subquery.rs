//! End to end: `inner[range:step]`, with the expectations worked by hand
//! from the `SubqueryExpr` case in Prometheus's `eval`.
//!
//! The one thing worth testing hard is the inner grid's phase. Its points
//! are multiples of the subquery's step in absolute time, never offsets
//! from the outer step, so shifting the evaluation time by less than a
//! step must not move a single inner point — which is why several cases
//! below ask the same question at deliberately unaligned times and expect
//! the identical answer.

use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery, Series};
use promql_parser::SeriesDescription;

const S: i64 = 1000;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two counters scraped every 10s for 1000s: `a` is `1+1x100`, so its
/// sample at time `t` is `1 + t/10s`, and `b` is twice that.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"metric_total{pod="a"} 1+1x100"#,
            r#"metric_total{pod="b"} 2+2x100"#,
        ]),
        10.0,
    ))
}

fn query(q: &str, range: RangeQuery) -> Vec<Series> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), q, &range)
        .unwrap();
    promql_engine::series::decode(&batches).unwrap()
}

/// The single value one instant query produces.
fn at(q: &str, at_ms: i64) -> f64 {
    let out = query(q, RangeQuery::new(at_ms, at_ms, 30 * S));
    assert_eq!(out.len(), 1, "{q}: {out:?}");
    assert_eq!(out[0].values().len(), 1, "{q}: {out:?}");
    out[0].values()[0]
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

/// At 1000s the grid is 980s, 990s, 1000s — the multiples of 10s after
/// the strict lower bound 970s — carrying 99, 100 and 101.
#[test]
fn the_inner_grid_starts_at_the_first_step_after_the_window_opens() {
    assert_eq!(
        at(r#"sum_over_time(metric_total{pod="a"}[30s:10s])"#, 1000 * S),
        300.0
    );
    assert_eq!(
        at(
            r#"count_over_time(metric_total{pod="a"}[30s:10s])"#,
            1000 * S
        ),
        3.0
    );
    // A range that is not a whole number of steps opens the window
    // mid-step, and the grid takes the next point rather than rounding.
    assert_eq!(
        at(
            r#"count_over_time(metric_total{pod="a"}[35s:10s])"#,
            1000 * S
        ),
        4.0
    );
}

/// The grid is pinned to absolute multiples of the step, so an
/// evaluation time off the step lands on the same inner points.
#[test]
fn an_evaluation_time_off_the_step_does_not_move_the_grid() {
    // 480s, 490s and 500s carry 49, 50 and 51.
    for at_ms in [500 * S, 501 * S, 505 * S, 509 * S] {
        assert_eq!(
            at(r#"sum_over_time(metric_total{pod="a"}[30s:10s])"#, at_ms),
            150.0,
            "at {at_ms}ms"
        );
    }
    // One full step later the window has advanced by exactly one point.
    assert_eq!(
        at(r#"sum_over_time(metric_total{pod="a"}[30s:10s])"#, 510 * S),
        153.0
    );
}

/// An offset shifts the window, and the grid stays where it was: every
/// offset smaller than a step gives the same answer as the aligned one.
#[test]
fn an_offset_smaller_than_the_step_changes_nothing() {
    for offset in ["3s", "5s", "7s", "9s"] {
        let q = format!(r#"sum_over_time(metric_total{{pod="a"}}[30s:10s] offset {offset})"#);
        assert_eq!(at(&q, 1010 * S), 300.0, "offset {offset}");
    }
    assert_eq!(
        at(
            r#"sum_over_time(metric_total{pod="a"}[30s:10s] offset 10s)"#,
            1010 * S
        ),
        300.0
    );
}

/// Written without a step, a subquery takes the engine's default — 1m
/// here, as in `promqltest` — so a 5m range holds five points.
#[test]
fn a_subquery_without_a_step_takes_the_engines_default() {
    assert_eq!(
        at(r#"count_over_time(metric_total{pod="a"}[5m:])"#, 1000 * S),
        5.0
    );
    let mut range = RangeQuery::new(1000 * S, 1000 * S, 30 * S);
    range.no_step_subquery_interval_ms = 10 * S;
    let out = query(r#"count_over_time(metric_total{pod="a"}[5m:])"#, range);
    assert_eq!(out[0].values(), [30.0]);
}

/// `@` pins the outer window, so every step of the outer range query
/// reads the one grid it names.
#[test]
fn an_at_modifier_pins_the_window_for_every_step() {
    let out = query(
        r#"sum_over_time(metric_total{pod="a"}[30s:10s] @ 1000)"#,
        RangeQuery::new(0, 60 * S, 30 * S),
    );
    assert_eq!(out[0].timestamps(), [0, 30 * S, 60 * S]);
    assert_eq!(out[0].values(), [300.0, 300.0, 300.0]);

    // `end()` is the statement's end even though the subquery narrows
    // the grid beneath it.
    let out = query(
        r#"sum_over_time(metric_total{pod="a"}[30s:10s] @ end())"#,
        RangeQuery::new(0, 1000 * S, 500 * S),
    );
    assert_eq!(out[0].values(), [300.0, 300.0, 300.0]);
}

/// The corpus's nested case, at this fixture's values: the middle
/// subquery's five sums are 288, 291, 294, 297, 300, and `rate`
/// extrapolates the 12 they rise over its 50s window.
#[test]
fn a_subquery_nests_inside_a_subquery() {
    let v = at(
        r#"rate(sum_over_time(metric_total{pod="a"}[30s:10s])[50s:10s])"#,
        1000 * S,
    );
    assert!(close(v, 0.3), "{v}");
}

/// The inner expression is an arbitrary instant vector, aggregations
/// included: one grid point is one full evaluation of it.
#[test]
fn a_subquery_over_an_aggregation_evaluates_it_per_grid_point() {
    assert_eq!(
        at("max_over_time(sum(metric_total)[30s:10s])", 1000 * S),
        303.0
    );
    assert_eq!(
        at("min_over_time(sum(metric_total)[30s:10s])", 1000 * S),
        297.0
    );

    let out = query(
        "max_over_time(sum by (pod) (metric_total)[30s:10s])",
        RangeQuery::new(1000 * S, 1000 * S, 30 * S),
    );
    assert_eq!(out.len(), 2);
    // By label, not by index: a DataFusion aggregation does not promise
    // an order, and asserting one makes the test flaky rather than strict.
    let group = |pod: &str| {
        out.iter()
            .find(|s| s.label("pod") == pod)
            .unwrap_or_else(|| panic!("{pod} group: {out:?}"))
            .values()
            .to_vec()
    };
    assert_eq!(group("a"), [101.0]);
    assert_eq!(group("b"), [202.0]);
}

/// A bare subquery is a range vector: its matrix is the inner grid, and
/// only an instant query has the single window that makes one.
#[test]
fn a_bare_subquery_is_the_inner_grid() {
    let out = query(
        r#"metric_total{pod="a"}[30s:10s]"#,
        RangeQuery::new(1000 * S, 1000 * S, 30 * S),
    );
    assert_eq!(out[0].timestamps(), [980 * S, 990 * S, 1000 * S]);
    assert_eq!(out[0].values(), [99.0, 100.0, 101.0]);
    // `__name__` survives: nothing above it drops one.
    assert_eq!(out[0].label("__name__"), "metric_total");

    // Over more than one step it is a matrix, which is the user's
    // mistake rather than a gap, so it reads as upstream's
    // `validateQueryType` rejection.
    let err = Engine::blocking()
        .unwrap()
        .range_query(
            source().as_ref(),
            r#"metric_total{pod="a"}[30s:10s]"#,
            &RangeQuery::new(0, 1000 * S, 500 * S),
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::Query(_)), "{err:?}");
    assert_eq!(
        err.to_string(),
        "invalid expression type \"range vector\" for range query, \
         must be Scalar or instant Vector"
    );
}

/// A subquery whose step would outrun the grid cap is a query error
/// naming the limit, not an out-of-memory.
#[test]
fn a_subquery_grid_larger_than_the_cap_is_a_query_error() {
    let err = Engine::blocking()
        .unwrap()
        .range_query(
            source().as_ref(),
            "count_over_time(metric_total[1000d:1ms])",
            &RangeQuery::new(0, 0, 30 * S),
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::Query(_)), "{err:?}");
}

#[tokio::test]
async fn the_inner_range_query_is_a_plan_of_its_own_beneath_the_window() {
    let engine = Engine::new();
    let source = source();
    let plan = engine
        .plan_async(
            source.as_ref(),
            "rate(metric_total[5m:1m])",
            &RangeQuery::new(600 * S, 1200 * S, 30 * S),
        )
        .await
        .unwrap();
    let rendered = plan.display_indent().to_string();
    // The outer window reads the outer grid; the inner selector reads
    // the subquery's, which starts one minute after 300s.
    assert!(
        rendered
            .contains("Utf8(\"rate\"), Int64(600000), Int64(1200000), Int64(30000), Int64(300000)"),
        "{rendered}"
    );
    assert!(
        rendered.contains("promql_vector_selector(selector_0.samples, Int64(360000), Int64(1200000), Int64(60000)"),
        "{rendered}"
    );
}
