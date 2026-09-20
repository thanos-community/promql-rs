//! End to end: range functions over the in-memory source, expectations
//! worked by hand from the semantics.

use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery, Series};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two counters at 30s: +1 and +2 per step, 16 and 19 samples long.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"http_requests_total{pod="envoy-1"} 1+1x15"#,
            r#"http_requests_total{pod="envoy-2"} 1+2x18"#,
        ]),
        30.0,
    ))
}

fn query(q: &str, range: RangeQuery) -> Vec<Series> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), q, &range)
        .unwrap();
    promql_engine::series::decode(&batches).unwrap()
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
fn rate_drops_the_metric_name_and_gives_the_slope() {
    // At 300s the (0, 300s] window holds ten samples 30s apart; the
    // first is one interval from the boundary so extrapolation reaches
    // it: envoy-1 rises 1/30 per second, envoy-2 2/30.
    let out = query(
        "rate(http_requests_total[5m])",
        RangeQuery::new(300_000, 300_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    assert_eq!(
        out[0].labels().collect::<Vec<_>>(),
        vec![("pod", "envoy-1")]
    );
    assert!(close(out[0].values()[0], 1.0 / 30.0), "{out:?}");
    assert!(close(out[1].values()[0], 2.0 / 30.0), "{out:?}");
}

#[test]
fn increase_and_the_over_time_family_at_one_step() {
    let at = RangeQuery::new(300_000, 300_000, 30_000);
    let one = |q: &str| query(q, at)[0].values()[0];
    assert!(close(one("increase(http_requests_total[5m])"), 10.0));
    assert_eq!(one("sum_over_time(http_requests_total[5m])"), 65.0);
    assert_eq!(one("avg_over_time(http_requests_total[5m])"), 6.5);
    assert_eq!(one("min_over_time(http_requests_total[5m])"), 2.0);
    assert_eq!(one("max_over_time(http_requests_total[5m])"), 11.0);
    assert_eq!(one("count_over_time(http_requests_total[5m])"), 10.0);
    assert_eq!(one("last_over_time(http_requests_total[5m])"), 11.0);
    assert_eq!(one("present_over_time(http_requests_total[5m])"), 1.0);
    assert_eq!(one("changes(http_requests_total[5m])"), 9.0);
    assert_eq!(one("resets(http_requests_total[5m])"), 0.0);
    assert!(close(one("irate(http_requests_total[5m])"), 1.0 / 30.0));
    assert_eq!(one("idelta(http_requests_total[5m])"), 1.0);
}

#[test]
fn last_over_time_keeps_the_metric_name() {
    let out = query(
        "last_over_time(http_requests_total[5m])",
        RangeQuery::new(0, 0, 30_000),
    );
    assert_eq!(
        out[0].labels().collect::<Vec<_>>(),
        vec![("__name__", "http_requests_total"), ("pod", "envoy-1")]
    );
    assert_eq!(out[0].timestamps(), [0]);
    assert_eq!(out[0].values(), [1.0]);
}

#[test]
fn a_series_ends_when_its_window_empties() {
    // envoy-1's last sample is at 450s. A (t-1m, t] window is empty once
    // t - 60s >= 450s, i.e. from t = 510s on; at 480s it still holds 450s.
    let out = query(
        "count_over_time(http_requests_total[1m])",
        RangeQuery::new(0, 900_000, 30_000),
    );
    let envoy1 = &out[0];
    assert_eq!(envoy1.timestamps().last(), Some(&480_000));
    assert_eq!(envoy1.values().last(), Some(&1.0));
    assert_eq!(envoy1.timestamps()[..2], [0, 30_000]);
    assert_eq!(envoy1.values()[..2], [1.0, 2.0]);
}

#[test]
fn offset_and_at_shift_the_window() {
    let out = query(
        "count_over_time(http_requests_total[5m] offset 1m)",
        RangeQuery::new(360_000, 360_000, 30_000),
    );
    assert_eq!(out[0].timestamps(), [360_000]);
    assert_eq!(out[0].values(), [10.0]);

    let out = query(
        "count_over_time(http_requests_total[5m] @ 300)",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [10.0, 10.0, 10.0]);

    let out = query(
        "count_over_time(http_requests_total[5m] @ end())",
        RangeQuery::new(0, 60_000, 30_000),
    );
    // (−4m, 60s]: samples at 0, 30s, 60s.
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [3.0, 3.0, 3.0]);
}

#[test]
fn aggregations_compose_over_range_functions() {
    let out = query(
        "sum(rate(http_requests_total[5m]))",
        RangeQuery::new(300_000, 300_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels().count(), 0);
    assert!(close(out[0].values()[0], 3.0 / 30.0), "{out:?}");

    let out = query(
        "avg by (pod) (increase(http_requests_total[5m]))",
        RangeQuery::new(300_000, 300_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    let envoy1 = out
        .iter()
        .find(|s| s.label("pod") == "envoy-1")
        .expect("envoy-1 group");
    assert_eq!(envoy1.labels().count(), 1);
    assert!(close(envoy1.values()[0], 10.0), "{out:?}");
}

/// The two functions that answer at a step their vector never reached.
///
/// envoy-1's samples stop at 450s and the lookback carries it to 750s,
/// so 900s and 1200s are the steps with nothing in them — and they are
/// the only steps `absent` emits at, while `scalar` emits at all five.
#[test]
fn absent_and_scalar_answer_where_the_selector_stops() {
    let over = RangeQuery::new(0, 1_200_000, 300_000);
    let out = query(r#"absent(http_requests_total{pod="envoy-1"})"#, over);
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].labels().collect::<Vec<_>>(),
        vec![("pod", "envoy-1")]
    );
    assert_eq!(out[0].timestamps(), &[900_000, 1_200_000]);
    assert_eq!(out[0].values(), &[1.0, 1.0]);

    let out = query(r#"scalar(http_requests_total{pod="envoy-1"})"#, over);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels().count(), 0);
    assert_eq!(
        out[0].timestamps(),
        &[0, 300_000, 600_000, 900_000, 1_200_000]
    );
    assert_eq!(out[0].values()[..3], [1.0, 11.0, 16.0]);
    assert!(out[0].values()[3..].iter().all(|v| v.is_nan()));
}

/// `funcScalar`: the value of the one element, and a NaN for any other
/// count — two elements and none alike.
#[test]
fn scalar_is_the_value_of_a_vector_that_holds_exactly_one_series() {
    let at = RangeQuery::new(0, 0, 30_000);
    let one = |q: &str| query(q, at)[0].values()[0];
    assert_eq!(one(r#"scalar(http_requests_total{pod="envoy-1"})"#), 1.0);
    assert!(
        one("scalar(http_requests_total)").is_nan(),
        "two are not one"
    );
    assert!(one(r#"scalar(http_requests_total{pod="mars"})"#).is_nan());
    // An aggregation collapses the two into the one it wants.
    assert_eq!(one("scalar(sum(http_requests_total))"), 2.0);
}

/// `funcAbsent`: nothing where the vector has something, and one series
/// where it has not — the step no aggregation can answer for.
#[test]
fn absent_answers_only_where_its_vector_is_empty() {
    let at = RangeQuery::new(0, 0, 30_000);
    assert!(query("absent(http_requests_total)", at).is_empty());
    // The metric name is not among the labels it answers with.
    assert_eq!(label_set("absent(nonexistent)", at), Some(vec![]));
}

/// `absent_over_time` is the same answer over a window: 1 where the
/// window held nothing, and the same invented label set.
#[test]
fn absent_over_time_answers_where_the_window_held_nothing() {
    let at = |ms| RangeQuery::new(ms, ms, 30_000);
    assert!(query("absent_over_time(http_requests_total[5m])", at(0)).is_empty());
    assert_eq!(
        label_set(r#"absent_over_time(nonexistent{pod="mars"}[5m])"#, at(0)),
        Some(vec![("pod".to_string(), "mars".to_string())])
    );
    // The window is what separates the two: the store stops at 540s, so
    // at 900s a 5m window is empty while a 10m one still reaches back.
    assert_eq!(
        query("absent_over_time(http_requests_total[5m])", at(900_000))[0].values(),
        &[1.0]
    );
    assert!(query("absent_over_time(http_requests_total[10m])", at(900_000)).is_empty());
}

/// `createLabelsForAbsentFunction`: equality matchers only, the first
/// for a name only, and from a selector only.
#[test]
fn the_labels_absent_answers_with_are_the_selectors_equalities() {
    let at = RangeQuery::new(0, 0, 30_000);
    assert_eq!(
        label_set(r#"absent(nonexistent{pod="mars"})"#, at),
        Some(vec![("pod".to_string(), "mars".to_string())])
    );
    assert_eq!(
        label_set(r#"absent(nonexistent{pod="mars",route=~"a.*"})"#, at),
        Some(vec![("pod".to_string(), "mars".to_string())]),
        "a regex matcher names nothing"
    );
    assert_eq!(
        label_set(r#"absent(nonexistent{pod="mars",pod="venus"})"#, at),
        Some(vec![]),
        "two equalities on one name take it back out"
    );
    assert_eq!(
        label_set(r#"absent((nonexistent{pod="mars"}))"#, at),
        Some(vec![]),
        "a parenthesis is not a selector, upstream's type switch included"
    );
    assert_eq!(
        label_set("absent(sum(nonexistent))", at),
        Some(vec![]),
        "nor is anything else"
    );
}

/// The label set of the one series a query answers with, owned so the
/// batches it was decoded from can be dropped.
fn label_set(q: &str, range: RangeQuery) -> Option<Vec<(String, String)>> {
    query(q, range).first().map(|s| {
        s.labels()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    })
}

/// A call this engine can plan is still held to upstream's table first,
/// and says what upstream says: our parser types every call the same, so
/// nothing before the planner has looked at the arity or the types.
#[test]
fn a_call_of_the_wrong_shape_is_refused_in_upstreams_words() {
    let engine = Engine::blocking().unwrap();
    for (q, want) in [
        (
            "scalar()",
            r#"expected 1 argument(s) in call to "scalar", got 0"#,
        ),
        (
            "absent(up, 1)",
            r#"expected 1 argument(s) in call to "absent", got 2"#,
        ),
        (
            "scalar(1)",
            r#"expected type instant vector in call to function "scalar", got scalar"#,
        ),
    ] {
        match engine.range_query(source().as_ref(), q, &RangeQuery::new(0, 0, 30_000)) {
            Err(EngineError::Query(m)) => assert_eq!(m, want, "{q}"),
            other => panic!("{q}: {other:?}"),
        }
    }
}

#[test]
fn what_is_still_unsupported_is_named() {
    let engine = Engine::blocking().unwrap();
    for (q, what) in [
        ("rate(http_requests_total[5m:1m])", "a subquery"),
        ("abs(http_requests_total)", "the abs function"),
        (
            "quantile_over_time(0.5, http_requests_total[5m])",
            "the quantile_over_time function",
        ),
    ] {
        match engine.range_query(source().as_ref(), q, &RangeQuery::new(0, 0, 30_000)) {
            Err(EngineError::Unsupported(f)) => assert_eq!(f, what, "{q}"),
            other => panic!("{q}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn the_plan_is_a_projection_with_the_function_as_a_literal() {
    let engine = Engine::new();
    let source = source();
    let plan = engine
        .plan_async(
            source.as_ref(),
            "rate(http_requests_total[5m] offset 1m)",
            &RangeQuery::new(0, 600_000, 30_000),
        )
        .await
        .unwrap();
    let rendered = plan.display_indent().to_string();
    assert!(
        rendered.starts_with(
            "Projection: promql_labels(Utf8(\"pod\"), get_field(selector_0.labels, Utf8(\"pod\"))) AS labels, promql_range_function(selector_0.samples, Utf8(\"rate\"), Int64(0), Int64(600000), Int64(30000), Int64(300000), Int64(60000), Int64(NULL)) AS samples"
        ),
        "{rendered}"
    );
    assert!(rendered.contains("TableScan: selector_0"), "{rendered}");
}
