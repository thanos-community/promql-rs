//! The `/api/v1` handlers, a port of `pkg/api/query/v1.go` onto
//! promql-engine. Parameters are read in Go's order so the first error a
//! client sees is the same one Thanos reports.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::response::Response;
use datafusion::error::DataFusionError;
use promql_engine::{Engine, EngineError, RangeQuery, Series};
use promql_parser::ast::LabelMatcher;
use serde::Serialize;
use serde_json::value::RawValue;
use thanos_store::{LabelsResult, PartialResponseStrategy, SelectOptions, StoreError};
use tokio::sync::Semaphore;

use crate::api::{respond, ApiError};
use crate::params::{
    parse_bool_param, parse_duration, parse_limit, parse_metadata_time_range,
    parse_metric_selector, parse_step, parse_store_matchers, parse_time, parse_time_param,
    FormValues,
};
use crate::query::QueryableCreator;
use crate::value::{matrix_from_series, vector_from_series};

/// Prometheus's cap on points per series in a range query.
const MAX_POINTS_PER_SERIES: i128 = 11_000;

/// The flags that shape query evaluation.
#[derive(Debug, Clone)]
pub struct QueryOptions {
    /// `--query-timeout`: the most a query may take, whatever `timeout=`
    /// asks for.
    pub query_timeout: Duration,
    /// `--query-lookback-delta`, unless `lookback_delta=` is given.
    pub lookback_delta: Duration,
    /// `--query-default-step` for range queries without `step=`.
    pub default_step: Duration,
    /// `--query-max-concurrent`: queries evaluating at once.
    pub max_concurrent: usize,
    /// `--query-partial-response`, unless `partial_response=` is given.
    pub partial_response: bool,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(2 * 60),
            lookback_delta: Duration::from_secs(5 * 60),
            default_step: Duration::from_secs(1),
            max_concurrent: 20,
            partial_response: true,
        }
    }
}

/// The API's state: Thanos's `QueryAPI`.
pub struct QueryApi {
    engine: Engine,
    queryable_create: Arc<dyn QueryableCreator>,
    /// `gate.Gate`: caps concurrent evaluations.
    gate: Semaphore,
    now: Box<dyn Fn() -> i64 + Send + Sync>,
    opts: QueryOptions,
}

impl std::fmt::Debug for QueryApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryApi")
            .field("queryable_create", &self.queryable_create)
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

impl QueryApi {
    pub fn new(
        engine: Engine,
        queryable_create: Arc<dyn QueryableCreator>,
        opts: QueryOptions,
    ) -> Self {
        Self {
            engine,
            gate: Semaphore::new(opts.max_concurrent.max(1)),
            queryable_create,
            now: Box::new(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as i64)
            }),
            opts,
        }
    }

    /// Fix "now" for tests.
    pub fn with_now(mut self, now: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.now = Box::new(now);
        self
    }

    pub fn options(&self) -> &QueryOptions {
        &self.opts
    }

    /// The `timeout` parameter capped by `--query-timeout`.
    fn timeout(&self, form: &FormValues) -> Result<Duration, ApiError> {
        let value = form.get("timeout");
        if value.is_empty() {
            return Ok(self.opts.query_timeout);
        }
        let ns = parse_duration(value).map_err(ApiError::bad_data)?;
        let requested = Duration::from_nanos(ns.max(0) as u64);
        Ok(requested.min(self.opts.query_timeout))
    }

    /// The parameters both query handlers share, after the time ones.
    /// Order and messages follow `query`/`queryRange`; `range_query` picks
    /// the one difference between them.
    fn query_params(&self, form: &FormValues, range_query: bool) -> Result<QueryParams, ApiError> {
        // `dedup` and `replicaLabels[]` are validated and ignored: no
        // deduplication yet, replica labels stay in the result.
        parse_bool_param(form, "dedup", true)?;
        let store_matchers = parse_store_matchers(form)?;
        let mut partial_response = self.opts.partial_response;
        if !range_query {
            partial_response = parse_bool_param(form, "partial_response", partial_response)?;
        }
        parse_max_source_resolution(form)?;
        if range_query {
            partial_response = parse_bool_param(form, "partial_response", partial_response)?;
        }
        parse_shard_info(form)?;
        parse_engine(form)?;
        let query = form.get("query").to_string();
        let lookback_ms =
            parse_lookback_delta(form)?.unwrap_or(self.opts.lookback_delta.as_millis() as i64);
        Ok(QueryParams {
            query,
            lookback_ms,
            select: SelectOptions {
                partial_response: if partial_response {
                    PartialResponseStrategy::Warn
                } else {
                    PartialResponseStrategy::Abort
                },
                store_matchers: store_matchers
                    .iter()
                    .map(|set| {
                        set.iter()
                            .map(promql_engine::matcher::CompiledMatcher::compile)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| ApiError::bad_data(e.to_string()))?,
                verify_matchers: true,
            },
        })
    }

    /// Evaluate under the gate and the timeout; the source's warnings come
    /// back with the series.
    async fn exec(
        &self,
        query: &str,
        range: RangeQuery,
        timeout: Duration,
        select: SelectOptions,
    ) -> Result<(Vec<Series>, Vec<String>), ApiError> {
        let queryable = self.queryable_create.queryable(select);
        let run = async {
            let _permit = self
                .gate
                .acquire()
                .await
                .map_err(|_| ApiError::exec("query gate is closed"))?;
            self.engine
                .range_query_async(queryable.source(), query, &range)
                .await
                .map_err(engine_error)
        };
        let series = tokio::time::timeout(timeout, run)
            .await
            .map_err(|_| ApiError::timeout("query timed out in query execution"))??;
        Ok((series, queryable.warnings()))
    }
}

struct QueryParams {
    query: String,
    lookback_ms: i64,
    select: SelectOptions,
}

/// Prometheus's status codes for what the engine reports: a bad query is
/// the client's, an unsupported feature or a failing store is an
/// execution error, the rest is ours.
fn engine_error(err: EngineError) -> ApiError {
    match err {
        EngineError::Query(message) => ApiError::bad_data(message),
        EngineError::Unsupported(_) => ApiError::exec(err.to_string()),
        EngineError::DataFusion(ref df) => match store_error_in(df) {
            Some(store) => ApiError::exec(format!("expanding series: {store}")),
            None => ApiError::internal(err.to_string()),
        },
        EngineError::Schema(_) | EngineError::Runtime(_) => ApiError::internal(err.to_string()),
    }
}

/// The store error a DataFusion error wraps, if that is what it is.
fn store_error_in(err: &DataFusionError) -> Option<&StoreError> {
    match err.find_root() {
        DataFusionError::External(inner) => inner.downcast_ref::<StoreError>(),
        _ => None,
    }
}

/// `parseDownsamplingParamMillis`: validated, then ignored, as this
/// client fetches raw data only.
fn parse_max_source_resolution(form: &FormValues) -> Result<(), ApiError> {
    let value = form.get("max_source_resolution");
    if value.is_empty() || value == "auto" {
        return Ok(());
    }
    let ns = parse_duration(value)
        .map_err(|e| ApiError::bad_data(format!("'max_source_resolution' parameter: {e}")))?;
    if ns < 0 {
        return Err(ApiError::bad_data(
            "negative 'max_source_resolution' is not accepted. Try a positive integer",
        ));
    }
    Ok(())
}

/// `parseShardInfo`: must be a JSON object when given; ignored after that.
fn parse_shard_info(form: &FormValues) -> Result<(), ApiError> {
    let value = form.get("shard_info");
    if value.is_empty() {
        return Ok(());
    }
    match serde_json::from_str::<serde_json::Value>(value) {
        Ok(serde_json::Value::Object(_)) => Ok(()),
        Ok(other) => Err(ApiError::bad_data(format!(
            "could not unmarshal parameter shard_info: json: cannot unmarshal {} into Go value of type storepb.ShardInfo",
            json_kind(&other)
        ))),
        Err(e) => Err(ApiError::bad_data(format!(
            "could not unmarshal parameter shard_info: {e}"
        ))),
    }
}

fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// `parseEngineParam`: Thanos knows `prometheus` and `thanos`; both mean
/// promql-engine here.
fn parse_engine(form: &FormValues) -> Result<(), ApiError> {
    match form.get("engine") {
        "" | "prometheus" | "thanos" => Ok(()),
        other => Err(ApiError::bad_data(format!("'{other}' bad engine"))),
    }
}

/// `parseLookbackDeltaParam`: milliseconds when given and positive.
fn parse_lookback_delta(form: &FormValues) -> Result<Option<i64>, ApiError> {
    let value = form.get("lookback_delta");
    if value.is_empty() {
        return Ok(None);
    }
    let ns = parse_duration(value)
        .map_err(|e| ApiError::bad_data(format!("'lookback_delta' parameter: {e}")))?;
    Ok((ns > 0).then_some(ns / 1_000_000))
}

/// `GET|POST /api/v1/query`.
pub async fn query(
    State(api): State<Arc<QueryApi>>,
    form: FormValues,
) -> Result<Response, ApiError> {
    let ts = parse_time_param(&form, "time", (api.now)())?;
    let timeout = api.timeout(&form)?;
    let params = api.query_params(&form, false)?;

    // The engine has no instant-query entry point: one step at `ts`.
    let range = RangeQuery {
        start_ms: ts,
        end_ms: ts,
        step_ms: 1,
        lookback_ms: params.lookback_ms,
    };
    let (series, warnings) = api
        .exec(&params.query, range, timeout, params.select)
        .await?;
    Ok(respond(
        &vector_from_series(&series, ts).to_query_data(),
        &warnings,
    ))
}

/// `GET|POST /api/v1/query_range`.
pub async fn query_range(
    State(api): State<Arc<QueryApi>>,
    form: FormValues,
) -> Result<Response, ApiError> {
    let start_ms = parse_time(form.get("start")).map_err(ApiError::bad_data)?;
    let end_ms = parse_time(form.get("end")).map_err(ApiError::bad_data)?;
    if end_ms < start_ms {
        return Err(ApiError::bad_data(
            "end timestamp must not be before start time",
        ));
    }
    let range_seconds = (end_ms - start_ms) / 1000;
    let step_ns = parse_step(
        &form,
        api.opts.default_step.as_nanos() as i64,
        range_seconds,
    )?;
    if step_ns <= 0 {
        return Err(ApiError::bad_data(
            "zero or negative query resolution step widths are not accepted. Try a positive integer",
        ));
    }
    // For safety, limit the number of returned points per timeseries.
    if (i128::from(end_ms - start_ms) * 1_000_000) / i128::from(step_ns) > MAX_POINTS_PER_SERIES {
        return Err(ApiError::bad_data(
            "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)",
        ));
    }
    let step_ms = step_ns / 1_000_000;
    if step_ms == 0 {
        // Thanos accepts sub-millisecond steps; the engine's grid is
        // milliseconds.
        return Err(ApiError::bad_data(
            "query resolution step must be at least 1ms",
        ));
    }
    let timeout = api.timeout(&form)?;
    let params = api.query_params(&form, true)?;

    let range = RangeQuery {
        start_ms,
        end_ms,
        step_ms,
        lookback_ms: params.lookback_ms,
    };
    let (series, warnings) = api
        .exec(&params.query, range, timeout, params.select)
        .await?;
    Ok(respond(
        &matrix_from_series(&series).to_query_data(),
        &warnings,
    ))
}

/// The parameters of the two metadata endpoints, in Go's order.
struct LabelParams {
    start_ms: i64,
    end_ms: i64,
    limit: usize,
    matcher_sets: Vec<Vec<LabelMatcher>>,
    select: SelectOptions,
}

fn label_params(api: &QueryApi, form: &FormValues) -> Result<LabelParams, ApiError> {
    let (start_ms, end_ms) = parse_metadata_time_range(form)?;
    let partial_response = parse_bool_param(form, "partial_response", api.opts.partial_response)?;
    let store_matchers = parse_store_matchers(form)?;
    let limit = parse_limit(form.get("limit"))?;
    let matcher_sets = form
        .get_all("match[]")
        .into_iter()
        .map(parse_metric_selector)
        .collect::<Result<Vec<_>, _>>()?;
    let store_matchers = store_matchers
        .iter()
        .map(|set| {
            set.iter()
                .map(promql_engine::matcher::CompiledMatcher::compile)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ApiError::bad_data(e.to_string()))?;
    Ok(LabelParams {
        start_ms,
        end_ms,
        limit,
        matcher_sets,
        select: SelectOptions {
            partial_response: if partial_response {
                PartialResponseStrategy::Warn
            } else {
                PartialResponseStrategy::Abort
            },
            store_matchers,
            verify_matchers: true,
        },
    })
}

/// Union the per-selector results, sort, apply the limit with its
/// warning, and render the `[]string`.
fn respond_labels(mut results: Vec<LabelsResult>, limit: usize) -> Response {
    let mut values: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    for result in results.drain(..) {
        values.extend(result.values);
        warnings.extend(result.warnings);
    }
    values.sort();
    values.dedup();
    if limit > 0 && values.len() > limit {
        values.truncate(limit);
        warnings.push("results truncated due to limit".to_string());
    }
    let data = serde_json::to_string(&values).expect("strings serialize");
    respond(&RawValue::from_string(data).expect("valid JSON"), &warnings)
}

/// `GET|POST /api/v1/labels`.
pub async fn label_names(
    State(api): State<Arc<QueryApi>>,
    form: FormValues,
) -> Result<Response, ApiError> {
    let params = label_params(&api, &form)?;
    let queryable = api.queryable_create.queryable(params.select);
    let mut results = Vec::new();
    if params.matcher_sets.is_empty() {
        results.push(
            queryable
                .label_names(params.start_ms, params.end_ms, &[])
                .await?,
        );
    } else {
        for matchers in &params.matcher_sets {
            results.push(
                queryable
                    .label_names(params.start_ms, params.end_ms, matchers)
                    .await?,
            );
        }
    }
    Ok(respond_labels(results, params.limit))
}

/// `GET /api/v1/label/{name}/values`.
pub async fn label_values(
    State(api): State<Arc<QueryApi>>,
    Path(name): Path<String>,
    form: FormValues,
) -> Result<Response, ApiError> {
    if name.is_empty() {
        return Err(ApiError::bad_data(format!("invalid label name: {name:?}")));
    }
    let params = label_params(&api, &form)?;
    let queryable = api.queryable_create.queryable(params.select);
    let mut results = Vec::new();
    if params.matcher_sets.is_empty() {
        results.push(
            queryable
                .label_values(&name, params.start_ms, params.end_ms, &[])
                .await?,
        );
    } else {
        for matchers in &params.matcher_sets {
            results.push(
                queryable
                    .label_values(&name, params.start_ms, params.end_ms, matchers)
                    .await?,
            );
        }
    }
    Ok(respond_labels(results, params.limit))
}

/// Prometheus's `/api/v1/status/buildinfo`; Grafana's health check.
#[derive(Serialize)]
struct BuildInfo {
    version: &'static str,
    revision: &'static str,
    branch: &'static str,
    #[serde(rename = "buildUser")]
    build_user: &'static str,
    #[serde(rename = "buildDate")]
    build_date: &'static str,
    #[serde(rename = "goVersion")]
    go_version: &'static str,
}

/// `GET /api/v1/status/buildinfo`.
pub async fn buildinfo() -> Response {
    let info = BuildInfo {
        version: env!("CARGO_PKG_VERSION"),
        revision: option_env!("THANOS_QUERY_RS_REVISION").unwrap_or("unknown"),
        branch: option_env!("THANOS_QUERY_RS_BRANCH").unwrap_or("unknown"),
        build_user: "",
        build_date: "",
        go_version: "",
    };
    let data = serde_json::to_string(&info).expect("build info serializes");
    respond(&RawValue::from_string(data).expect("valid JSON"), &[])
}
