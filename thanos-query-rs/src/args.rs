//! Command-line flags. Named after `thanos query`'s flags, spelled the way
//! Rust CLIs spell them: `--query-timeout` for `--query.timeout`. The
//! replica flags keep Thanos's dots, because operators copy them from
//! existing Thanos deployments; the dashed spelling is an alias.

use std::net::SocketAddr;
use std::time::Duration;

use clap::builder::{PossibleValuesParser, TypedValueParser};
use clap::{ArgAction, Parser, ValueEnum};
use thanos_store::dedup::DeduplicationFunc;

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

    #[arg(
        long,
        default_value = "2h",
        value_parser = block_duration_arg,
        help = "Span of the blocks a selector's samples are decoded and handed to the engine in, aligned to the Unix epoch; the stores are asked for the whole range once regardless"
    )]
    pub query_block_duration: Duration,

    /// `--query.replica-label`; Thanos also takes a comma-separated list
    /// and sorts and deduplicates it (`strutil.ParseFlagLabels`), see
    /// [`QueryArgs::replica_labels`].
    #[arg(
        long = "query.replica-label",
        alias = "query-replica-label",
        value_name = "LABEL",
        value_delimiter = ',',
        help = "Labels to treat as a replica indicator along which data is deduplicated; `dedup=false` still queries without deduplication. May be repeated or comma separated"
    )]
    pub query_replica_labels: Vec<String>,

    #[arg(
        long = "deduplication.func",
        alias = "deduplication-func",
        default_value = "penalty",
        value_parser = PossibleValuesParser::new(["penalty", "chain"]).map(|s| match s.as_str() {
            "chain" => DeduplicationFunc::Chain,
            _ => DeduplicationFunc::Penalty,
        }),
        help = "Experimental. Deduplication algorithm for merging overlapping series: penalty or chain. Chain unions samples 1:1 and needs a replica label from --query.replica-label"
    )]
    pub deduplication_func: DeduplicationFunc,
}

impl QueryArgs {
    /// `strutil.ParseFlagLabels`: no empty names, sorted, each once.
    pub fn replica_labels(&self) -> Vec<String> {
        let mut labels: Vec<String> = self
            .query_replica_labels
            .iter()
            .filter(|l| !l.is_empty())
            .cloned()
            .collect();
        labels.sort();
        labels.dedup();
        labels
    }
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

/// A block holds at least one millisecond of window ends.
fn block_duration_arg(s: &str) -> Result<Duration, String> {
    let d = duration_arg(s)?;
    if d < Duration::from_millis(1) {
        return Err(format!("{s:?} is shorter than a millisecond"));
    }
    Ok(d)
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
        assert_eq!(
            args.query.query_block_duration,
            Duration::from_secs(2 * 60 * 60)
        );
        assert!(args.query.replica_labels().is_empty());
        assert_eq!(args.query.deduplication_func, DeduplicationFunc::Penalty);
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

        let blocks =
            Args::try_parse_from(["thanos-query-rs", "--query-block-duration", "30m"]).unwrap();
        assert_eq!(
            blocks.query.query_block_duration,
            Duration::from_secs(30 * 60)
        );
        assert!(Args::try_parse_from(["thanos-query-rs", "--query-block-duration", "0s"]).is_err());
    }

    #[test]
    fn replica_flags_follow_thanos() {
        let args = Args::try_parse_from([
            "thanos-query-rs",
            "--query.replica-label=replica,az",
            "--query.replica-label",
            "replica",
            "--query.replica-label=",
            "--deduplication.func=chain",
        ])
        .unwrap();
        assert_eq!(args.query.replica_labels(), ["az", "replica"]);
        assert_eq!(args.query.deduplication_func, DeduplicationFunc::Chain);
        assert!(Args::try_parse_from(["thanos-query-rs", "--deduplication.func=nope"]).is_err());
    }
}
