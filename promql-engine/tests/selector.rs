//! End to end: parse, plan, execute, decode over the in-memory source.
//!
//! The expected numbers are worked by hand from the semantics, not copied
//! from a run, so this holds the engine to the specification rather than
//! to itself. The conformance suite then holds it to Prometheus.

use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// The corpus's staple: two series at 30s, 16 and 19 samples long.
fn nginx() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"http_requests_total{pod="nginx-1"} 1+1x15"#,
            r#"http_requests_total{pod="nginx-2"} 1+2x18"#,
        ]),
        30.0,
    ))
}

#[test]
fn a_selector_with_offset_expires_series_at_the_lookback_boundary() {
    let engine = Engine::blocking().unwrap();
    let out = engine
        .range_query(
            nginx(),
            "http_requests_total offset 30s",
            &RangeQuery::new(600_000, 1_200_000, 30_000),
        )
        .unwrap();

    assert_eq!(out.len(), 2);
    let nginx1 = &out[0];
    assert_eq!(
        nginx1.labels().collect::<Vec<_>>(),
        vec![("__name__", "http_requests_total"), ("pod", "nginx-1")]
    );
    // nginx-1's last sample is 16 at 450s. Looking back 30s from step t,
    // it is visible while t - 30s - 450s < 5m, i.e. t < 780s: six steps
    // from 600s to 750s. At 780s the sample is exactly 5m old and gone.
    assert_eq!(
        nginx1.timestamps(),
        (0..6).map(|i| 600_000 + i * 30_000).collect::<Vec<_>>()
    );
    assert_eq!(nginx1.values(), [16.0; 6]);
    // nginx-2 ends at 540s with 37, so it lasts until t < 870s: nine steps.
    let nginx2 = &out[1];
    assert_eq!(
        nginx2.timestamps(),
        (0..9).map(|i| 600_000 + i * 30_000).collect::<Vec<_>>()
    );
    assert_eq!(nginx2.values(), [37.0; 9]);
}

#[test]
fn at_end_beyond_all_data_yields_nothing() {
    let engine = Engine::blocking().unwrap();
    let out = engine
        .range_query(
            nginx(),
            "http_requests_total @ end()",
            &RangeQuery::new(0, 1_800_000, 30_000),
        )
        .unwrap();
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn at_start_repeats_the_first_sample_on_every_step() {
    let engine = Engine::blocking().unwrap();
    let out = engine
        .range_query(
            nginx(),
            "http_requests_total @ start()",
            &RangeQuery::new(0, 120_000, 30_000),
        )
        .unwrap();
    assert_eq!(out.len(), 2);
    for s in &out {
        assert_eq!(s.timestamps().len(), 5);
        assert!(s.values().iter().all(|v| *v == 1.0), "{s:?}");
        assert_eq!(s.timestamps()[4], 120_000);
    }
}

#[test]
fn matchers_reach_the_source() {
    let engine = Engine::blocking().unwrap();
    let out = engine
        .range_query(
            nginx(),
            r#"http_requests_total{pod=~"nginx-2|nginx-3"}"#,
            &RangeQuery::new(0, 60_000, 30_000),
        )
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].label("pod"), "nginx-2");
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [1.0, 3.0, 5.0]);
}

#[test]
fn anything_but_a_selector_is_unsupported_by_name() {
    let engine = Engine::blocking().unwrap();
    for (query, what) in [
        ("topk(2, http_requests_total)", "the topk aggregation"),
        (
            "absent_over_time(http_requests_total[5m])",
            "the absent_over_time function",
        ),
        ("http_requests_total + 1", "a binary operator"),
        ("http_requests_total[5m]", "a range selector"),
    ] {
        let err = engine
            .range_query(nginx(), query, &RangeQuery::new(0, 60_000, 30_000))
            .unwrap_err();
        match err {
            EngineError::Unsupported(f) => assert_eq!(f, what, "{query}"),
            other => panic!("{query}: expected Unsupported, got {other}"),
        }
    }
}

#[test]
fn a_parse_error_is_a_query_error() {
    let engine = Engine::blocking().unwrap();
    let err = engine
        .range_query(
            nginx(),
            "http_requests_total{",
            &RangeQuery::new(0, 0, 30_000),
        )
        .unwrap_err();
    assert!(matches!(err, EngineError::Query(_)), "{err}");
}

#[tokio::test]
async fn the_plan_is_a_projection_over_a_scan_with_literal_parameters() {
    // `new`, not `blocking`: this test runs inside Tokio already, and an
    // engine holding its own runtime cannot be dropped there.
    let engine = Engine::new();
    let source = nginx();
    let plan = engine
        .plan_async(
            source.as_ref(),
            "http_requests_total offset 30s",
            &RangeQuery::new(600_000, 1_200_000, 30_000),
        )
        .await
        .unwrap();

    let rendered = plan.display_indent().to_string();
    assert!(
        rendered.starts_with("Projection: selector_0.labels, promql_vector_selector(selector_0.samples, Int64(600000), Int64(1200000), Int64(30000), Int64(300000), Int64(30000), Int64(NULL)) AS samples"),
        "{rendered}"
    );
    assert!(rendered.contains("TableScan: selector_0"), "{rendered}");
    assert!(matches!(plan, LogicalPlan::Projection(_)));

    // A different offset is a different call, so the two can never be
    // folded into one by common-subexpression elimination.
    let other = engine
        .plan_async(
            source.as_ref(),
            "http_requests_total offset 1m",
            &RangeQuery::new(600_000, 1_200_000, 30_000),
        )
        .await
        .unwrap();
    assert_ne!(other.display_indent().to_string(), rendered);
}
