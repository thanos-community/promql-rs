//! The HTTP API end to end through the router, against an in-memory
//! source: bytes of the Prometheus JSON, parameter handling and every
//! error string a client can provoke.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use datafusion::catalog::Session;
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::{Engine, MemorySeriesSource, SelectHints, Series, SeriesSource};
use promql_parser::ast::LabelMatcher;
use thanos_query_rs::api::ApiError;
use thanos_query_rs::metrics::Metrics;
use thanos_query_rs::query::{MemoryQueryableCreator, Queryable, QueryableCreator};
use thanos_query_rs::v1::{QueryApi, QueryOptions};
use thanos_query_rs::{router, RouterOptions};
use thanos_store::{LabelsResult, SelectOptions};
use tower::ServiceExt;

/// 2023-11-14T22:13:20Z.
const T0_S: i64 = 1_700_000_000;
const T0: i64 = T0_S * 1000;
/// "Now" for instant queries without `time`.
const NOW: i64 = T0 + 60_000;

fn series(labels: &[(&str, &str)], values: &[f64]) -> Series {
    let timestamps = (0..values.len() as i64).map(|i| T0 + i * 15_000).collect();
    Series::new(labels, timestamps, values.to_vec()).unwrap()
}

/// Two `up` series, stored out of order, and a counter.
fn memory() -> MemorySeriesSource {
    MemorySeriesSource::new(vec![
        series(
            &[("__name__", "up"), ("instance", "b"), ("job", "web")],
            &[0.0, 1.0, 0.0, 1.0, 0.0],
        ),
        series(
            &[("__name__", "up"), ("instance", "a"), ("job", "api")],
            &[1.0, 1.0, 1.0, 1.0, 1.0],
        ),
        series(
            &[("__name__", "http_requests_total"), ("job", "api")],
            &[0.0, 10.0, 20.0, 30.0, 40.0],
        ),
    ])
}

fn app_with(
    creator: Arc<dyn QueryableCreator>,
    opts: QueryOptions,
    router_opts: RouterOptions,
) -> Router {
    let api = QueryApi::new(Engine::new(), creator, opts).with_now(|| NOW);
    router(Arc::new(api), Arc::new(Metrics::new()), &router_opts)
}

fn app() -> Router {
    app_with(
        Arc::new(MemoryQueryableCreator::new(memory())),
        QueryOptions::default(),
        RouterOptions::default(),
    )
}

fn query_string(pairs: &[(&str, &str)]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (k, v) in pairs {
        serializer.append_pair(k, v);
    }
    serializer.finish()
}

async fn call(app: Router, request: Request<Body>) -> (StatusCode, axum::http::HeaderMap, String) {
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, String::from_utf8(body.to_vec()).unwrap())
}

async fn get(
    app: Router,
    path: &str,
    pairs: &[(&str, &str)],
) -> (StatusCode, axum::http::HeaderMap, String) {
    let uri = if pairs.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{}", query_string(pairs))
    };
    call(
        app,
        Request::builder().uri(uri).body(Body::empty()).unwrap(),
    )
    .await
}

fn error_of(body: &str) -> (String, String) {
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["status"], "error", "{body}");
    (
        v["errorType"].as_str().unwrap().to_string(),
        v["error"].as_str().unwrap().to_string(),
    )
}

const UP_MATRIX: &str = r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"up","instance":"a","job":"api"},"values":[[1700000000,"1"],[1700000015,"1"],[1700000030,"1"],[1700000045,"1"],[1700000060,"1"]]},{"metric":{"__name__":"up","instance":"b","job":"web"},"values":[[1700000000,"0"],[1700000015,"1"],[1700000030,"0"],[1700000045,"1"],[1700000060,"0"]]}]}}"#;

#[tokio::test]
async fn query_range_returns_a_sorted_matrix() {
    let (status, headers, body) = get(
        app(),
        "/api/v1/query_range",
        &[
            ("query", "up"),
            ("start", "1700000000"),
            ("end", "1700000060"),
            ("step", "15"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    assert!(headers.get(header::CACHE_CONTROL).is_none());
    assert_eq!(body, UP_MATRIX);
}

#[tokio::test]
async fn query_range_accepts_rfc3339_and_duration_steps() {
    let (status, _, body) = get(
        app(),
        "/api/v1/query_range",
        &[
            ("query", "up"),
            ("start", "2023-11-14T22:13:20Z"),
            ("end", "2023-11-14T22:14:20.000Z"),
            ("step", "15s"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, UP_MATRIX);
}

#[tokio::test]
async fn query_range_evaluates_functions_and_aggregations() {
    let (status, _, body) = get(
        app(),
        "/api/v1/query_range",
        &[
            ("query", "sum by (job) (rate(http_requests_total[1m]))"),
            ("start", "1700000060"),
            ("end", "1700000060"),
            ("step", "15"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"api"},"values":[[1700000060,"0.6666666666666666"]]}]}}"#
    );
}

#[tokio::test]
async fn instant_query_returns_a_vector_at_time_or_now() {
    let (status, _, body) = get(
        app(),
        "/api/v1/query",
        &[("query", "up"), ("time", "1700000030.5")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"a","job":"api"},"value":[1700000030.500,"1"]},{"metric":{"__name__":"up","instance":"b","job":"web"},"value":[1700000030.500,"0"]}]}}"#
    );

    let (status, _, body) = get(app(), "/api/v1/query", &[("query", r#"up{job="api"}"#)]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"a","job":"api"},"value":[1700000060,"1"]}]}}"#
    );

    let (status, _, body) = get(app(), "/api/v1/query", &[("query", "nothing_here")]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#
    );
}

#[tokio::test]
async fn post_form_body_comes_before_the_query_string() {
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/query_range?step=30&query=http_requests_total")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(query_string(&[
            ("query", "up"),
            ("start", "1700000000"),
            ("end", "1700000060"),
            ("step", "15"),
        ])))
        .unwrap();
    let (status, _, body) = call(app(), request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, UP_MATRIX, "the body's query and step win");
}

/// A path, its parameters and the error message they must produce.
type Case<'a> = (&'a str, Vec<(&'a str, &'a str)>, &'a str);

#[tokio::test]
async fn every_bad_request_string_matches_thanos() {
    fn range<'a>(extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
        let mut pairs = vec![
            ("query", "up"),
            ("start", "1700000000"),
            ("end", "1700000060"),
            ("step", "15"),
        ];
        for &(k, v) in extra {
            pairs.retain(|(key, _)| *key != k);
            pairs.push((k, v));
        }
        pairs
    }
    let cases: Vec<Case> = vec![
        (
            "/api/v1/query_range",
            vec![("query", "up")],
            r#"cannot parse "" to a valid timestamp"#,
        ),
        (
            "/api/v1/query_range",
            range(&[("end", "1699999999")]),
            "end timestamp must not be before start time",
        ),
        (
            "/api/v1/query_range",
            range(&[("step", "0")]),
            "zero or negative query resolution step widths are not accepted. Try a positive integer",
        ),
        (
            "/api/v1/query_range",
            range(&[("step", "-1")]),
            "zero or negative query resolution step widths are not accepted. Try a positive integer",
        ),
        (
            "/api/v1/query_range",
            range(&[("step", "soon")]),
            r#"'step' parameter: cannot parse "soon" to a valid duration"#,
        ),
        (
            "/api/v1/query_range",
            range(&[("end", "1700100000"), ("step", "1")]),
            "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)",
        ),
        (
            "/api/v1/query_range",
            range(&[("end", "1700000000"), ("step", "0.0001")]),
            "query resolution step must be at least 1ms",
        ),
        (
            "/api/v1/query_range",
            range(&[("timeout", "later")]),
            r#"cannot parse "later" to a valid duration"#,
        ),
        (
            "/api/v1/query_range",
            range(&[("dedup", "maybe")]),
            r#"'dedup' parameter: strconv.ParseBool: parsing "maybe": invalid syntax"#,
        ),
        (
            "/api/v1/query_range",
            range(&[("partial_response", "2")]),
            r#"'partial_response' parameter: strconv.ParseBool: parsing "2": invalid syntax"#,
        ),
        (
            "/api/v1/query_range",
            range(&[("max_source_resolution", "-300")]),
            "negative 'max_source_resolution' is not accepted. Try a positive integer",
        ),
        (
            "/api/v1/query_range",
            range(&[("max_source_resolution", "coarse")]),
            r#"'max_source_resolution' parameter: cannot parse "coarse" to a valid duration"#,
        ),
        (
            "/api/v1/query_range",
            range(&[("engine", "foo")]),
            "'foo' bad engine",
        ),
        (
            "/api/v1/query_range",
            range(&[("lookback_delta", "x")]),
            r#"'lookback_delta' parameter: cannot parse "x" to a valid duration"#,
        ),
        (
            "/api/v1/query",
            vec![("query", "up"), ("time", "abc")],
            r#"Invalid time value for 'time': cannot parse "abc" to a valid timestamp"#,
        ),
        (
            "/api/v1/query",
            vec![("query", "up"), ("shard_info", "[1]")],
            "could not unmarshal parameter shard_info: json: cannot unmarshal array into Go value of type storepb.ShardInfo",
        ),
        (
            "/api/v1/labels",
            vec![("start", "10"), ("end", "5")],
            "end timestamp must not be before start time",
        ),
        (
            "/api/v1/labels",
            vec![("limit", "-1")],
            "limit must be non-negative",
        ),
        (
            "/api/v1/labels",
            vec![("limit", "many")],
            r#"cannot parse "many" to a valid limit"#,
        ),
        (
            "/api/v1/label/job/values",
            vec![("start", "then")],
            r#"Invalid time value for 'start': cannot parse "then" to a valid timestamp"#,
        ),
    ];
    for (path, pairs, want) in cases {
        let (status, headers, body) = get(app(), path, &pairs).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path} {pairs:?}: {body}");
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        let (typ, message) = error_of(&body);
        assert_eq!(typ, "bad_data", "{path} {pairs:?}");
        assert_eq!(message, want, "{path} {pairs:?}");
    }

    // Selector syntax errors carry the parser's message.
    for (path, pairs) in [
        ("/api/v1/query", vec![("query", "up{")]),
        (
            "/api/v1/query",
            vec![("query", "up"), ("storeMatch[]", "{")],
        ),
        ("/api/v1/labels", vec![("match[]", "up{")]),
    ] {
        let (status, _, body) = get(app(), path, &pairs).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path} {pairs:?}: {body}");
        assert_eq!(error_of(&body).0, "bad_data");
    }
}

#[tokio::test]
async fn what_the_engine_cannot_do_is_an_execution_error() {
    let (status, _, body) = get(
        app(),
        "/api/v1/query",
        &[("query", "up + 1"), ("time", "1700000030")],
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let (typ, message) = error_of(&body);
    assert_eq!(typ, "execution");
    assert!(message.contains("not supported yet"), "{message}");
}

#[tokio::test]
async fn preflight_and_cors_headers() {
    let (status, headers, body) = call(
        app(),
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/v1/query")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());
    assert_eq!(headers["access-control-allow-origin"], "*");
    assert_eq!(
        headers["access-control-allow-methods"],
        "GET, POST, OPTIONS"
    );
    assert_eq!(
        headers["access-control-allow-headers"],
        "Accept, Accept-Encoding, Authorization, Content-Type, Origin"
    );
    assert_eq!(headers["access-control-expose-headers"], "Date");

    let (_, headers, _) = get(app(), "/api/v1/status/buildinfo", &[]).await;
    assert_eq!(headers["access-control-allow-origin"], "*");

    let quiet = app_with(
        Arc::new(MemoryQueryableCreator::new(memory())),
        QueryOptions::default(),
        RouterOptions {
            disable_cors: true,
            ..Default::default()
        },
    );
    let (status, headers, _) = get(quiet, "/api/v1/status/buildinfo", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn labels_and_label_values() {
    let (status, _, body) = get(app(), "/api/v1/labels", &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        r#"{"status":"success","data":["__name__","instance","job"]}"#
    );

    let (_, _, body) = get(
        app(),
        "/api/v1/labels",
        &[
            ("match[]", "http_requests_total"),
            ("match[]", r#"up{job="web"}"#),
        ],
    )
    .await;
    assert_eq!(
        body, r#"{"status":"success","data":["__name__","instance","job"]}"#,
        "selector results are unioned"
    );

    let (_, _, body) = get(
        app(),
        "/api/v1/labels",
        &[("match[]", "http_requests_total")],
    )
    .await;
    assert_eq!(body, r#"{"status":"success","data":["__name__","job"]}"#);

    let (status, headers, body) = get(app(), "/api/v1/labels", &[("limit", "1")]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        body,
        r#"{"status":"success","data":["__name__"],"warnings":["results truncated due to limit"]}"#
    );

    let (_, _, body) = get(
        app(),
        "/api/v1/labels",
        &[("start", "1800000000"), ("end", "1900000000")],
    )
    .await;
    assert_eq!(
        body, r#"{"status":"success","data":[]}"#,
        "nothing in that range"
    );

    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/labels")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from("match[]=up"))
        .unwrap();
    let (status, _, body) = call(app(), request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        r#"{"status":"success","data":["__name__","instance","job"]}"#
    );

    let (status, _, body) = get(app(), "/api/v1/label/job/values", &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, r#"{"status":"success","data":["api","web"]}"#);

    let (_, _, body) = get(
        app(),
        "/api/v1/label/job/values",
        &[("match[]", r#"up{instance="b"}"#)],
    )
    .await;
    assert_eq!(body, r#"{"status":"success","data":["web"]}"#);

    let (_, _, body) = get(app(), "/api/v1/label/__name__/values", &[("limit", "1")]).await;
    assert_eq!(
        body,
        r#"{"status":"success","data":["http_requests_total"],"warnings":["results truncated due to limit"]}"#
    );

    let (_, _, body) = get(app(), "/api/v1/label/nope/values", &[]).await;
    assert_eq!(body, r#"{"status":"success","data":[]}"#);
}

#[tokio::test]
async fn buildinfo_and_metrics() {
    let (status, _, body) = get(app(), "/api/v1/status/buildinfo", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], "success");
    assert_eq!(v["data"]["version"], env!("CARGO_PKG_VERSION"));
    assert!(v["data"]["goVersion"].is_string());

    let app = app();
    let _ = get(app.clone(), "/api/v1/query", &[("query", "up")]).await;
    let _ = get(app.clone(), "/api/v1/query", &[("query", "up{")]).await;
    let (status, headers, body) = get(app, "/metrics", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("application/openmetrics-text"));
    assert!(
        body.contains(r#"http_requests_total{handler="/api/v1/query",method="GET",code="200"} 1"#),
        "{body}"
    );
    assert!(
        body.contains(r#"http_requests_total{handler="/api/v1/query",method="GET",code="400"} 1"#),
        "{body}"
    );
}

#[tokio::test]
async fn route_prefix_moves_everything() {
    let prefixed = app_with(
        Arc::new(MemoryQueryableCreator::new(memory())),
        QueryOptions::default(),
        RouterOptions {
            route_prefix: "/thanos/".into(),
            ..Default::default()
        },
    );
    let (status, _, _) = get(prefixed.clone(), "/thanos/api/v1/status/buildinfo", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = get(prefixed.clone(), "/thanos/metrics", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = get(prefixed, "/api/v1/status/buildinfo", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// A creator whose source is slow and chatty, for the timeout and the
/// warning plumbing.
#[derive(Debug)]
struct Wrapped {
    inner: MemoryQueryableCreator,
    delay: Duration,
    warnings: Vec<String>,
}

impl QueryableCreator for Wrapped {
    fn queryable(&self, options: SelectOptions) -> Box<dyn Queryable> {
        Box::new(WrappedQueryable {
            inner: self.inner.queryable(options),
            source: SlowSource {
                inner: Arc::new(memory()),
                delay: self.delay,
            },
            warnings: self.warnings.clone(),
        })
    }
}

struct WrappedQueryable {
    inner: Box<dyn Queryable>,
    source: SlowSource,
    warnings: Vec<String>,
}

#[async_trait]
impl Queryable for WrappedQueryable {
    fn source(&self) -> &dyn SeriesSource {
        &self.source
    }

    fn warnings(&self) -> Vec<String> {
        self.warnings.clone()
    }

    async fn label_names(
        &self,
        start_ms: i64,
        end_ms: i64,
        matchers: &[LabelMatcher],
    ) -> Result<LabelsResult, ApiError> {
        let mut result = self.inner.label_names(start_ms, end_ms, matchers).await?;
        result.warnings.extend(self.warnings.iter().cloned());
        Ok(result)
    }

    async fn label_values(
        &self,
        name: &str,
        start_ms: i64,
        end_ms: i64,
        matchers: &[LabelMatcher],
    ) -> Result<LabelsResult, ApiError> {
        self.inner
            .label_values(name, start_ms, end_ms, matchers)
            .await
    }
}

#[derive(Debug)]
struct SlowSource {
    inner: Arc<MemorySeriesSource>,
    delay: Duration,
}

#[async_trait]
impl SeriesSource for SlowSource {
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        tokio::time::sleep(self.delay).await;
        self.inner.select(state, matchers, hints).await
    }
}

#[tokio::test]
async fn a_slow_source_hits_the_timeout() {
    let slow = app_with(
        Arc::new(Wrapped {
            inner: MemoryQueryableCreator::new(memory()),
            delay: Duration::from_millis(300),
            warnings: vec![],
        }),
        QueryOptions::default(),
        RouterOptions::default(),
    );
    let (status, _, body) = get(
        slow.clone(),
        "/api/v1/query",
        &[("query", "up"), ("timeout", "0.05")],
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let (typ, message) = error_of(&body);
    assert_eq!(typ, "timeout");
    assert_eq!(message, "query timed out in query execution");

    // The flag caps the request's own timeout.
    let capped = app_with(
        Arc::new(Wrapped {
            inner: MemoryQueryableCreator::new(memory()),
            delay: Duration::from_millis(300),
            warnings: vec![],
        }),
        QueryOptions {
            query_timeout: Duration::from_millis(50),
            ..Default::default()
        },
        RouterOptions::default(),
    );
    let (status, _, _) = get(
        capped,
        "/api/v1/query",
        &[("query", "up"), ("timeout", "10s")],
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn source_warnings_reach_the_envelope() {
    let chatty = app_with(
        Arc::new(Wrapped {
            inner: MemoryQueryableCreator::new(memory()),
            delay: Duration::ZERO,
            warnings: vec!["receive series from store-1:10901: Unavailable: down".into()],
        }),
        QueryOptions::default(),
        RouterOptions::default(),
    );
    let (status, headers, body) = get(
        chatty.clone(),
        "/api/v1/query",
        &[("query", r#"up{job="api"}"#), ("time", "1700000030")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        body,
        r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","instance":"a","job":"api"},"value":[1700000030,"1"]}]},"warnings":["receive series from store-1:10901: Unavailable: down"]}"#
    );

    let (_, _, body) = get(chatty, "/api/v1/labels", &[("limit", "1")]).await;
    assert_eq!(
        body,
        r#"{"status":"success","data":["__name__"],"warnings":["receive series from store-1:10901: Unavailable: down","results truncated due to limit"]}"#
    );
}
