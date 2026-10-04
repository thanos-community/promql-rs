//! The HTTP API over fake Thanos stores with the range cut into blocks:
//! the engine answers a row per label set and block, and the matrix still
//! lists every series once.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use promql_engine::Engine;
use thanos_query_rs::metrics::Metrics;
use thanos_query_rs::query::ThanosQueryableCreator;
use thanos_query_rs::v1::{QueryApi, QueryOptions};
use thanos_query_rs::{router, RouterOptions};
use thanos_store::testutil::{info, raw_chunk, series, series_frame, serve, FakeStore};
use thanos_store::{EndpointSet, EndpointSetConfig, ProxyStore};
use tower::ServiceExt;

/// 2023-11-14T22:13:20Z.
const T0: i64 = 1_700_000_000_000;
const STEP: i64 = 15_000;

/// Two counters, each in two chunks, 21 samples every 15s from `T0`.
fn store() -> Arc<FakeStore> {
    let counter = |inc: f64| -> Vec<(i64, f64)> {
        (0..21).map(|i| (T0 + i * STEP, i as f64 * inc)).collect()
    };
    let frames = [("api", 10.0), ("web", 5.0)]
        .into_iter()
        .map(|(job, inc)| {
            let samples = counter(inc);
            series_frame(series(
                &[("__name__", "http_requests_total"), ("job", job)],
                vec![raw_chunk(&samples[..9]), raw_chunk(&samples[9..])],
            ))
        })
        .collect();
    Arc::new(FakeStore::new(info("sidecar", &[], T0, T0 + 20 * STEP)).with_frames(frames))
}

/// `query_range` over the whole data through a querier cutting at
/// `block_ms`, and the fake store it asked.
async fn query_range(query: &str, block_ms: i64) -> (StatusCode, String, Arc<FakeStore>) {
    let fake = store();
    let (addr, _server) = serve(Arc::clone(&fake)).await;
    let endpoints = EndpointSet::new(&[addr], &EndpointSetConfig::default()).unwrap();
    endpoints.update().await;
    let creator = ThanosQueryableCreator::new(Arc::new(ProxyStore::new(Arc::new(endpoints))))
        .blocks(block_ms);
    let api = QueryApi::new(Engine::new(), Arc::new(creator), QueryOptions::default());
    let app = router(
        Arc::new(api),
        Arc::new(Metrics::new()),
        &RouterOptions::default(),
    );

    let params = form_urlencoded::Serializer::new(String::new())
        .append_pair("query", query)
        .append_pair("start", &((T0 + 4 * STEP) / 1000).to_string())
        .append_pair("end", &((T0 + 20 * STEP) / 1000).to_string())
        .append_pair("step", "15s")
        .finish();
    let request = Request::builder()
        .uri(format!("/api/v1/query_range?{params}"))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap(), fake)
}

#[tokio::test]
async fn a_range_over_several_blocks_lists_each_series_once() {
    for query in [
        "http_requests_total",
        "rate(http_requests_total[1m])",
        "sum(http_requests_total)",
    ] {
        let (status, whole, _) = query_range(query, thanos_store::DEFAULT_BLOCK_MS).await;
        assert_eq!(status, StatusCode::OK, "{query}: {whole}");
        let (status, cut, fake) = query_range(query, 60_000).await;
        assert_eq!(status, StatusCode::OK, "{query}: {cut}");
        assert_eq!(cut, whole, "{query}");
        // One request for the whole range, whatever the blocks.
        assert_eq!(fake.series_requests().len(), 1, "{query}");

        let v: serde_json::Value = serde_json::from_str(&cut).unwrap();
        let result = v["data"]["result"].as_array().unwrap();
        let mut metrics: Vec<String> = result.iter().map(|s| s["metric"].to_string()).collect();
        metrics.dedup();
        assert_eq!(metrics.len(), result.len(), "{query}: a series twice");
        for s in result {
            assert_eq!(s["values"].as_array().unwrap().len(), 17, "{query}: {s}");
        }
    }
}
