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
            r#"http_requests_total{pod="nginx-1"} 1+1x15"#,
            r#"http_requests_total{pod="nginx-2"} 1+2x18"#,
        ]),
        30.0,
    ))
}

fn query(q: &str, range: RangeQuery) -> Vec<Series> {
    Engine::blocking()
        .unwrap()
        .range_query(source(), q, &range)
        .unwrap()
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
fn rate_drops_the_metric_name_and_gives_the_slope() {
    // At 300s the (0, 300s] window holds ten samples 30s apart; the
    // first is one interval from the boundary so extrapolation reaches
    // it: nginx-1 rises 1/30 per second, nginx-2 2/30.
    let out = query(
        "rate(http_requests_total[5m])",
        RangeQuery::new(300_000, 300_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    assert_eq!(
        out[0].labels().collect::<Vec<_>>(),
        vec![("pod", "nginx-1")]
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
        vec![("__name__", "http_requests_total"), ("pod", "nginx-1")]
    );
    assert_eq!(out[0].timestamps(), [0]);
    assert_eq!(out[0].values(), [1.0]);
}

#[test]
fn a_series_ends_when_its_window_empties() {
    // nginx-1's last sample is at 450s. A (t-1m, t] window is empty once
    // t - 60s >= 450s, i.e. from t = 510s on; at 480s it still holds 450s.
    let out = query(
        "count_over_time(http_requests_total[1m])",
        RangeQuery::new(0, 900_000, 30_000),
    );
    let nginx1 = &out[0];
    assert_eq!(nginx1.timestamps().last(), Some(&480_000));
    assert_eq!(nginx1.values().last(), Some(&1.0));
    assert_eq!(nginx1.timestamps()[..2], [0, 30_000]);
    assert_eq!(nginx1.values()[..2], [1.0, 2.0]);
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
    let nginx1 = out
        .iter()
        .find(|s| s.label("pod") == "nginx-1")
        .expect("nginx-1 group");
    assert_eq!(nginx1.labels().count(), 1);
    assert!(close(nginx1.values()[0], 10.0), "{out:?}");
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
        match engine.range_query(source(), q, &RangeQuery::new(0, 0, 30_000)) {
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
