//! `thanos-query-rs`: the Prometheus HTTP API of `thanos query`, evaluated
//! by promql-engine over the Thanos Store API.
//!
//! The binary in `main.rs` wires flags to [`router`]; everything else
//! lives here so the API can be tested in-process against an in-memory
//! source. Modules follow Thanos's layout: `api` is `pkg/api/api.go`,
//! `v1` and `params` are `pkg/api/query/v1.go`, `query` is
//! `pkg/query`'s queryable creator.

pub mod api;
pub mod args;
pub mod metrics;
pub mod params;
pub mod query;
pub mod v1;
pub mod value;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;

use crate::metrics::Metrics;
use crate::v1::QueryApi;

/// How the router is mounted.
#[derive(Debug, Clone, Default)]
pub struct RouterOptions {
    /// `--web-route-prefix`: everything is served under this path.
    pub route_prefix: String,
    /// `--web-disable-cors`.
    pub disable_cors: bool,
}

/// The whole HTTP surface: `/api/v1/*` and `/metrics`, with CORS and
/// request metrics on every response.
pub fn router(api: Arc<QueryApi>, metrics: Arc<Metrics>, opts: &RouterOptions) -> Router {
    let v1 = Router::new()
        .route("/query", get(v1::query).post(v1::query))
        .route("/query_range", get(v1::query_range).post(v1::query_range))
        .route("/labels", get(v1::label_names).post(v1::label_names))
        .route("/label/{name}/values", get(v1::label_values))
        .route("/status/buildinfo", get(v1::buildinfo))
        .with_state(api);

    let app = Router::new()
        .nest("/api/v1", v1)
        .route(
            "/metrics",
            get(metrics::serve).with_state(Arc::clone(&metrics)),
        )
        .layer(axum::middleware::from_fn_with_state(
            !opts.disable_cors,
            api::cors,
        ))
        .layer(axum::middleware::from_fn_with_state(
            metrics,
            metrics::track,
        ))
        .layer(TraceLayer::new_for_http());

    match route_prefix(&opts.route_prefix) {
        Some(prefix) => Router::new().nest(&prefix, app),
        None => app,
    }
}

/// `--web-route-prefix` as axum wants it: one leading slash, no trailing
/// one, and `None` when there is nothing to nest under.
fn route_prefix(prefix: &str) -> Option<String> {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(format!("/{trimmed}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_normalize() {
        assert_eq!(route_prefix(""), None);
        assert_eq!(route_prefix("/"), None);
        assert_eq!(route_prefix("thanos"), Some("/thanos".to_string()));
        assert_eq!(route_prefix("/thanos/"), Some("/thanos".to_string()));
    }
}
