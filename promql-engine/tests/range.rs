//! End to end: range functions over the in-memory source, expectations
//! worked by hand from the semantics.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
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

#[test]
fn what_is_still_unsupported_is_named() {
    let engine = Engine::blocking().unwrap();
    for (q, what) in [
        (
            "absent_over_time(http_requests_total[5m])",
            "the absent_over_time function",
        ),
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

/// Two metrics with one label set but the name, plus a third pod so the
/// queries that are allowed still have rows.
fn two_metrics() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"http_requests_total{pod="envoy-1"} 1+1x15"#,
            r#"http_errors_total{pod="envoy-1"} 1+1x15"#,
            r#"http_requests_total{pod="envoy-2"} 1+2x18"#,
        ]),
        30.0,
    ))
}

#[test]
fn dropping_the_metric_name_may_not_leave_one_label_set_twice() {
    let engine = Engine::blocking().unwrap();
    let source = two_metrics();
    let at = RangeQuery::new(300_000, 300_000, 30_000);
    let run = |q: &str| engine.range_query(source.as_ref(), q, &at);

    // Both series lose `__name__` and become `{pod="envoy-1"}`.
    for q in [
        r#"rate({pod="envoy-1"}[1m])"#,
        // The check sits below the aggregation, so it still fires.
        r#"sum(rate({pod="envoy-1"}[1m]))"#,
        r#"sum by (pod)(rate({pod="envoy-1"}[1m]))"#,
    ] {
        let err = run(q).unwrap_err();
        assert_eq!(
            err.to_string(),
            "vector cannot contain metrics with the same labelset",
            "{q}"
        );
    }

    // A pinned name leaves one row, and a function that keeps the name
    // leaves two rows that are still distinct.
    assert!(run(r#"rate(http_requests_total{pod="envoy-1"}[1m])"#).is_ok());
    assert!(run(r#"last_over_time({pod="envoy-1"}[1m])"#).is_ok());
}

#[tokio::test]
async fn the_check_carries_the_projection_the_aggregation_needs() {
    let engine = Engine::new();
    let source = two_metrics();
    let range = RangeQuery::new(0, 600_000, 30_000);
    // The optimized plan, not the planner's: the rule this pins down runs
    // during optimization and leaves the raw plan alone.
    let plan = async |q: &str| {
        let plan = engine.plan_async(source.as_ref(), q, &range).await.unwrap();
        SessionContext::new()
            .state()
            .optimize(&plan)
            .unwrap()
            .display_indent()
            .to_string()
    };

    let rendered = plan(r#"sum by (pod)(rate({pod="envoy-1"}[1m]))"#).await;
    let check = rendered
        .find("ContainsSameLabelset")
        .unwrap_or_else(|| panic!("the check is in the plan: {rendered}"));
    // DataFusion extracts the aggregation's group key, `get_field(labels,
    // 'pod')`, into a projection of its own and sinks it towards the leaf
    // through every node that would pass the extracted column back up.
    // Were the check node to report its input's schema rather than its
    // own, it would be such a node, and the operator would end up hashing
    // whatever the store's rows became rather than the labels the call
    // actually emits.
    assert!(
        !rendered[check..].contains("__datafusion_extracted"),
        "nothing extracted below the check: {rendered}"
    );

    let rendered = plan("rate(http_requests_total[5m])").await;
    assert!(!rendered.contains("ContainsSameLabelset"), "{rendered}");
}

#[test]
fn dropping_the_metric_name_rejects_duplicate_label_sets() {
    let source = MemorySeriesSource::from_descriptions(&load(&["foo 1", "bar 2"]), 30.0);
    let engine = Engine::blocking().unwrap();

    // Both series contribute at t=0. Dropping their only distinguishing
    // label would produce two samples with the same empty label set.
    let err = engine
        .range_query(
            &source,
            r#"count_over_time({__name__=~"foo|bar"}[1m])"#,
            &RangeQuery::new(0, 0, 30_000),
        )
        .expect_err("overlapping series with the same output labels must be rejected");
    assert!(err.to_string().contains("same labelset"), "{err}");
}

#[test]
fn a_series_with_no_output_points_cannot_collide() {
    // Both series share `pod` and lose `__name__` under rate(). foo has
    // enough samples for rate to produce a slope at t=300s; bar has only
    // one sample ever, so rate over it never has two points to compare
    // and the row it contributes has no output points at any step.
    let source = MemorySeriesSource::from_descriptions(
        &load(&[r#"foo{pod="envoy-1"} 1+1x15"#, r#"bar{pod="envoy-1"} 5"#]),
        30.0,
    );
    let engine = Engine::blocking().unwrap();
    let out = engine
        .range_query(
            &source,
            r#"rate({pod="envoy-1"}[1m])"#,
            &RangeQuery::new(300_000, 300_000, 30_000),
        )
        .expect("bar's empty row must not be counted as a collision with foo's");
    let series = promql_engine::series::decode(&out).unwrap();
    assert_eq!(series.len(), 1, "{series:?}");
}
