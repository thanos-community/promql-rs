//! Command-line flags. Named after `thanos query`'s flags, spelled the way
//! Rust CLIs spell them: `--query-timeout` for `--query.timeout`.

use std::net::SocketAddr;
use std::time::Duration;

use clap::{ArgAction, Parser, ValueEnum};

use crate::params::parse_duration;

/// A Thanos Query look-alike that evaluates PromQL with promql-rs.
#[derive(Debug, Parser)]
#[command(name = "thanos-query-rs", version, about)]
pub struct Args {
    #[command(flatten)]
    pub http: HttpArgs,
    #[command(flatten)]
    pub endpoints: EndpointArgs,
    #[command(flatten)]
    pub query: QueryArgs,
    #[command(flatten)]
    pub web: WebArgs,
    #[command(flatten)]
    pub log: LogArgs,
}

#[derive(Debug, clap::Args)]
pub struct HttpArgs {
    #[arg(
        long,
        default_value = "0.0.0.0:10902",
        help = "Listen address for the HTTP API and /metrics"
    )]
    pub http_address: SocketAddr,
}

#[derive(Debug, clap::Args)]
pub struct EndpointArgs {
    #[arg(
        long = "endpoint",
        value_name = "HOST:PORT",
        help = "Address of a Store API endpoint to query; repeat for more than one"
    )]
    pub endpoints: Vec<String>,

    #[arg(
        long,
        default_value = "30s",
        value_parser = duration_arg,
        help = "How often to refresh the endpoints' Info metadata"
    )]
    pub endpoint_info_interval: Duration,
}

#[derive(Debug, clap::Args)]
pub struct QueryArgs {
    #[arg(
        long,
        default_value = "2m",
        value_parser = duration_arg,
        help = "Maximum time a query may take before it is aborted"
    )]
    pub query_timeout: Duration,

    #[arg(
        long,
        default_value = "5m",
        value_parser = duration_arg,
        help = "How far back a selector looks for the latest sample"
    )]
    pub query_lookback_delta: Duration,

    #[arg(
        long,
        default_value = "1s",
        value_parser = duration_arg,
        help = "Step of a range query that gives none; the UI's default"
    )]
    pub query_default_step: Duration,

    #[arg(
        long,
        default_value_t = 20,
        help = "Maximum number of queries evaluated at once"
    )]
    pub query_max_concurrent: usize,

    #[arg(
        long,
        default_value_t = true,
        action = ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true",
        help = "Answer with the healthy stores' data and a warning when a store fails, instead of an error"
    )]
    pub query_partial_response: bool,
}

#[derive(Debug, clap::Args)]
pub struct WebArgs {
    #[arg(
        long,
        default_value = "",
        help = "Prefix for the API endpoints, to serve under a sub-path"
    )]
    pub web_route_prefix: String,

    #[arg(long, help = "Do not set CORS headers on responses")]
    pub web_disable_cors: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, clap::Args)]
pub struct LogArgs {
    #[arg(
        long,
        default_value = "info",
        help = "Log level: error, warn, info, debug or trace; RUST_LOG overrides it"
    )]
    pub log_level: String,

    #[arg(long, default_value = "text", value_enum, help = "Log format")]
    pub log_format: LogFormat,
}

/// Durations as the API takes them: `30s`, `2m`, `1h30m`, or seconds.
fn duration_arg(s: &str) -> Result<Duration, String> {
    let ns = parse_duration(s)?;
    if ns < 0 {
        return Err(format!("{s:?} is negative"));
    }
    Ok(Duration::from_nanos(ns as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_thanos() {
        let args = Args::try_parse_from(["thanos-query-rs"]).unwrap();
        assert_eq!(args.http.http_address.port(), 10902);
        assert!(args.endpoints.endpoints.is_empty());
        assert_eq!(
            args.endpoints.endpoint_info_interval,
            Duration::from_secs(30)
        );
        assert_eq!(args.query.query_timeout, Duration::from_secs(120));
        assert_eq!(args.query.query_lookback_delta, Duration::from_secs(300));
        assert_eq!(args.query.query_default_step, Duration::from_secs(1));
        assert_eq!(args.query.query_max_concurrent, 20);
        assert!(args.query.query_partial_response);
        assert_eq!(args.web.web_route_prefix, "");
        assert!(!args.web.web_disable_cors);
        assert_eq!(args.log.log_level, "info");
        assert_eq!(args.log.log_format, LogFormat::Text);
    }

    #[test]
    fn flags_parse() {
        let args = Args::try_parse_from([
            "thanos-query-rs",
            "--endpoint",
            "a:10901",
            "--endpoint=b:10901",
            "--query-timeout",
            "30s",
            "--query-partial-response=false",
            "--web-disable-cors",
            "--log-format",
            "json",
            "--http-address",
            "127.0.0.1:0",
        ])
        .unwrap();
        assert_eq!(args.endpoints.endpoints, ["a:10901", "b:10901"]);
        assert_eq!(args.query.query_timeout, Duration::from_secs(30));
        assert!(!args.query.query_partial_response);
        assert!(args.web.web_disable_cors);
        assert_eq!(args.log.log_format, LogFormat::Json);

        let bare = Args::try_parse_from(["thanos-query-rs", "--query-partial-response"]).unwrap();
        assert!(bare.query.query_partial_response);
        assert!(Args::try_parse_from(["thanos-query-rs", "--query-timeout", "soon"]).is_err());
    }
}
