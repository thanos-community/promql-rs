//! End to end: aggregations over the in-memory source, expectations worked
//! by hand from the semantics.

use std::collections::BTreeMap;
use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Three series at 30s: two pods on route "/", one on "/api"; the "/api"
/// one is short (4 samples).
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"http_requests_total{pod="nginx-1", route="/"} 1+1x10"#,
            r#"http_requests_total{pod="nginx-2", route="/"} 1+2x10"#,
            r#"http_requests_total{pod="nginx-3", route="/api"} 100+0x3"#,
        ]),
        30.0,
    ))
}

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn query(q: &str, range: RangeQuery) -> Vec<promql_engine::DecodedSeries> {
    Engine::blocking()
        .unwrap()
        .range_query(source(), q, &range)
        .unwrap()
}

#[test]
fn sum_over_everything_yields_one_unlabelled_series() {
    let out = query(
        "sum(http_requests_total)",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert!(out[0].labels.is_empty());
    // t=0: 1+1+100, t=30s: 2+3+100, t=60s: 3+5+100.
    assert_eq!(
        out[0].samples,
        vec![(0, 102.0), (30_000, 105.0), (60_000, 108.0)]
    );
}

#[test]
fn by_keeps_only_the_listed_labels() {
    let out = query(
        "sum by (route) (http_requests_total)",
        RangeQuery::new(0, 30_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    let by_route: BTreeMap<_, _> = out
        .iter()
        .map(|s| (s.labels["route"].clone(), s.samples.clone()))
        .collect();
    assert_eq!(by_route["/"], vec![(0, 2.0), (30_000, 5.0)]);
    assert_eq!(by_route["/api"], vec![(0, 100.0), (30_000, 100.0)]);
    assert!(out.iter().all(|s| s.labels.len() == 1), "{out:?}");
}

#[test]
fn without_drops_the_listed_labels_and_the_metric_name() {
    let out = query(
        "max without (pod) (http_requests_total)",
        RangeQuery::new(0, 30_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    let slash = out.iter().find(|s| s.labels["route"] == "/").unwrap();
    assert_eq!(slash.labels, labels(&[("route", "/")]));
    assert_eq!(slash.samples, vec![(0, 1.0), (30_000, 3.0)]);
}

#[test]
fn a_group_lives_exactly_as_long_as_its_series() {
    // nginx-3 has samples at 0, 30, 60, 90s and is visible through the 5m
    // lookback until t < 90s + 5m = 390s. Its group ends where it ends;
    // the "/" group runs to the end of the query.
    let out = query(
        "count by (route) (http_requests_total)",
        RangeQuery::new(0, 600_000, 30_000),
    );
    let api = out.iter().find(|s| s.labels["route"] == "/api").unwrap();
    assert_eq!(api.samples.len(), 13);
    assert_eq!(api.samples.last(), Some(&(360_000, 1.0)));
    // The "/" series end at 300s and expire at 600s: 20 steps, 0..=570s.
    let slash = out.iter().find(|s| s.labels["route"] == "/").unwrap();
    assert_eq!(slash.samples.len(), 20);
    assert_eq!(slash.samples.last(), Some(&(570_000, 2.0)));
    assert!(slash.samples.iter().all(|(_, v)| *v == 2.0));
}

#[test]
fn avg_and_the_spread_statistics() {
    let out = query(
        "avg by (route) (http_requests_total)",
        RangeQuery::new(30_000, 30_000, 30_000),
    );
    let slash = out.iter().find(|s| s.labels["route"] == "/").unwrap();
    assert_eq!(slash.samples, vec![(30_000, 2.5)]);

    // Values at 30s on "/": 2 and 3. Population variance 0.25, stddev 0.5.
    let out = query(
        "stdvar by (route) (http_requests_total)",
        RangeQuery::new(30_000, 30_000, 30_000),
    );
    let slash = out.iter().find(|s| s.labels["route"] == "/").unwrap();
    assert_eq!(slash.samples, vec![(30_000, 0.25)]);
    let out = query(
        "stddev by (route) (http_requests_total)",
        RangeQuery::new(30_000, 30_000, 30_000),
    );
    let slash = out.iter().find(|s| s.labels["route"] == "/").unwrap();
    assert_eq!(slash.samples, vec![(30_000, 0.5)]);
}

#[test]
fn by_can_keep_the_metric_name() {
    let out = query(
        r#"count by (__name__) ({__name__=~".+"})"#,
        RangeQuery::new(0, 0, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].labels,
        labels(&[("__name__", "http_requests_total")])
    );
    assert_eq!(out[0].samples, vec![(0, 3.0)]);
}

#[test]
fn an_aggregation_over_nothing_is_nothing() {
    let out = query("sum(no_such_metric)", RangeQuery::new(0, 60_000, 30_000));
    assert!(out.is_empty(), "{out:?}");
    let out = query(
        "sum by (pod) (no_such_metric)",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn aggregations_nest() {
    let out = query(
        "max(sum by (route) (http_requests_total))",
        RangeQuery::new(0, 30_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].samples, vec![(0, 100.0), (30_000, 100.0)]);
}

#[test]
fn parameterized_aggregations_are_named_as_unsupported() {
    let engine = Engine::blocking().unwrap();
    for (q, what) in [
        ("topk(2, http_requests_total)", "the topk aggregation"),
        (
            "quantile(0.5, http_requests_total)",
            "the quantile aggregation",
        ),
        (
            "count_values(\"v\", http_requests_total)",
            "the count_values aggregation",
        ),
    ] {
        match engine.range_query(source(), q, &RangeQuery::new(0, 0, 30_000)) {
            Err(EngineError::Unsupported(f)) => assert_eq!(f, what, "{q}"),
            other => panic!("{q}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn the_plan_is_an_aggregate_over_string_group_keys() {
    let engine = Engine::new();
    let source = source();
    let plan = engine
        .plan_async(
            source.as_ref(),
            "sum by (route) (http_requests_total)",
            &RangeQuery::new(0, 60_000, 30_000),
        )
        .await
        .unwrap();
    let rendered = plan.display_indent().to_string();
    assert!(
        rendered.starts_with(
            "Projection: promql_labels(Utf8(\"route\"), route) AS labels, samples\n  Aggregate: groupBy=[[CAST(get_field(selector_0.labels, Utf8(\"route\")) AS Utf8) AS route]], aggr=[[promql_aggregate(samples, Utf8(\"sum\"), Int64(0), Int64(60000), Int64(30000)) AS samples]]"
        ),
        "{rendered}"
    );
    assert!(rendered.contains("TableScan: selector_0"), "{rendered}");
}
