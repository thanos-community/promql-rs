//! Request metrics on `/metrics`, in the OpenMetrics text format, the way
//! Thanos's `extpromhttp` instruments its handlers.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use prometheus_client::encoding::{text, EncodeLabelSet};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RequestLabels {
    handler: String,
    method: String,
    code: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HandlerLabels {
    handler: String,
    method: String,
}

/// The registry and the request metrics.
#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    requests: Family<RequestLabels, Counter>,
    duration: Family<HandlerLabels, Histogram>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();
        let requests = Family::<RequestLabels, Counter>::default();
        registry.register(
            "http_requests",
            "HTTP requests served, by handler, method and status code",
            requests.clone(),
        );
        let duration = Family::<HandlerLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(0.005, 2.0, 13))
        });
        registry.register(
            "http_request_duration_seconds",
            "Time to serve an HTTP request, by handler and method",
            duration.clone(),
        );
        Self {
            registry,
            requests,
            duration,
        }
    }

    pub fn observe(&self, handler: &str, method: &str, code: StatusCode, seconds: f64) {
        self.requests
            .get_or_create(&RequestLabels {
                handler: handler.to_string(),
                method: method.to_string(),
                code: code.as_u16().to_string(),
            })
            .inc();
        self.duration
            .get_or_create(&HandlerLabels {
                handler: handler.to_string(),
                method: method.to_string(),
            })
            .observe(seconds);
    }

    /// The registry in OpenMetrics text.
    pub fn encode(&self) -> String {
        let mut out = String::new();
        text::encode(&mut out, &self.registry).expect("encoding into a String cannot fail");
        out
    }
}

/// Middleware: count and time every request under its route pattern.
pub async fn track(State(metrics): State<Arc<Metrics>>, request: Request, next: Next) -> Response {
    let handler = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_string(), |p| p.as_str().to_string());
    let method = request.method().to_string();
    let started = Instant::now();
    let response = next.run(request).await;
    metrics.observe(
        &handler,
        &method,
        response.status(),
        started.elapsed().as_secs_f64(),
    );
    response
}

/// `GET /metrics`.
pub async fn serve(State(metrics): State<Arc<Metrics>>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        metrics.encode(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_are_counted_and_timed() {
        let metrics = Metrics::new();
        metrics.observe("/api/v1/query", "GET", StatusCode::OK, 0.01);
        metrics.observe("/api/v1/query", "GET", StatusCode::OK, 0.02);
        metrics.observe("/api/v1/query", "POST", StatusCode::BAD_REQUEST, 0.001);
        let text = metrics.encode();
        assert!(
            text.contains(
                r#"http_requests_total{handler="/api/v1/query",method="GET",code="200"} 2"#
            ),
            "{text}"
        );
        assert!(
            text.contains(
                r#"http_requests_total{handler="/api/v1/query",method="POST",code="400"} 1"#
            ),
            "{text}"
        );
        assert!(
            text.contains(
                r#"http_request_duration_seconds_count{handler="/api/v1/query",method="GET"} 2"#
            ),
            "{text}"
        );
        assert!(text.ends_with("# EOF\n"));
    }
}
