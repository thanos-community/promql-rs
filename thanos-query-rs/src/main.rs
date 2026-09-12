//! `thanos-query-rs`: serve the Prometheus HTTP API over Thanos Store API
//! endpoints, evaluating PromQL with promql-engine.

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use promql_engine::Engine;
use thanos_query_rs::args::{Args, LogFormat};
use thanos_query_rs::metrics::Metrics;
use thanos_query_rs::query::ThanosQueryableCreator;
use thanos_query_rs::v1::{QueryApi, QueryOptions};
use thanos_query_rs::{router, RouterOptions};
use thanos_store::{EndpointSet, EndpointSetConfig, ProxyStore};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_tracing(&args.log.log_level, args.log.log_format)?;

    if args.endpoints.endpoints.is_empty() {
        tracing::warn!("no --endpoint given; every query will come back empty");
    }
    let endpoint_config = EndpointSetConfig {
        refresh_interval: args.endpoints.endpoint_info_interval,
        ..EndpointSetConfig::default()
    };
    let endpoints = Arc::new(
        EndpointSet::new(&args.endpoints.endpoints, &endpoint_config)
            .context("configure endpoints")?,
    );
    // Learn who the stores are before answering anything, then keep it
    // fresh in the background.
    endpoints.update().await;
    for endpoint in endpoints.endpoints() {
        match endpoint.metadata() {
            Some(m) => tracing::info!(
                addr = endpoint.addr(),
                component = m.component_type,
                label_sets = ?m.label_sets.iter().map(ToString::to_string).collect::<Vec<_>>(),
                min_time = m.min_time,
                max_time = m.max_time,
                "endpoint ready"
            ),
            None => tracing::warn!(addr = endpoint.addr(), "endpoint has not answered Info yet"),
        }
    }
    let _refresh = Arc::clone(&endpoints).spawn_refresh();

    let proxy = Arc::new(ProxyStore::new(endpoints));
    let api = Arc::new(QueryApi::new(
        Engine::new(),
        Arc::new(ThanosQueryableCreator::new(proxy)),
        QueryOptions {
            query_timeout: args.query.query_timeout,
            lookback_delta: args.query.query_lookback_delta,
            default_step: args.query.query_default_step,
            max_concurrent: args.query.query_max_concurrent,
            partial_response: args.query.query_partial_response,
        },
    ));
    let app = router(
        api,
        Arc::new(Metrics::new()),
        &RouterOptions {
            route_prefix: args.web.web_route_prefix.clone(),
            disable_cors: args.web.web_disable_cors,
        },
    );

    let listener = tokio::net::TcpListener::bind(args.http.http_address)
        .await
        .with_context(|| format!("listen on {}", args.http.http_address))?;
    tracing::info!(address = %args.http.http_address, "listening for HTTP requests");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve HTTP")?;
    tracing::info!("shut down");
    Ok(())
}

/// `--log-level` unless `RUST_LOG` is set, in text or JSON.
fn init_tracing(level: &str, format: LogFormat) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(level))
        .with_context(|| format!("invalid log level {level:?}"))?;
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
    Ok(())
}

/// SIGINT or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install the Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install the SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT, shutting down"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}
