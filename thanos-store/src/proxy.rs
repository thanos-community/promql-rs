//! `ProxyStore`, the client half of `pkg/store/proxy.go`: fan a request
//! out to the endpoints that can hold data and merge what comes back.
//!
//! Series handling differs from Go in one deliberate way. Go merges the
//! stores' sorted streams lazily with a loser tree and hands Prometheus
//! chunk iterators; here every frame is drained into one
//! `BTreeMap<LabelSet, BTreeMap<i64, f64>>`. That single structure covers
//! a series split across frames, the same series from two stores, and
//! overlapping chunks, and its order is the order results must leave in.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use promql_engine::matcher::{matches_all, CompiledMatcher};
use promql_engine::{SelectHints, Series};
use promql_parser::ast::LabelMatcher;
use tokio::task::JoinSet;
use tonic::Status;

use crate::chunkenc::decode_xor;
use crate::endpointset::{EndpointRef, EndpointSet};
use crate::error::StoreError;
use crate::labels::{compile_matchers, to_proto_matchers, LabelSet};
use crate::source::SelectOptions;
use crate::storepb::thanos::chunk::Encoding;
use crate::storepb::thanos::{
    series_response, Aggr, LabelNamesRequest, LabelValuesRequest, Series as StoreSeries,
    SeriesRequest,
};

/// The merged answer to one selector.
#[derive(Debug, Default)]
pub struct SeriesResult {
    /// One per label set in `labels.Compare` order, samples ascending and
    /// within the requested range.
    pub series: Vec<Series>,
    pub warnings: Vec<String>,
}

/// Label names or label values from every store, merged.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LabelsResult {
    /// Sorted, without repeats.
    pub values: Vec<String>,
    pub warnings: Vec<String>,
}

/// The fan-out over an [`EndpointSet`].
#[derive(Debug)]
pub struct ProxyStore {
    endpoints: Arc<EndpointSet>,
}

impl ProxyStore {
    pub fn new(endpoints: Arc<EndpointSet>) -> Self {
        Self { endpoints }
    }

    pub fn endpoints(&self) -> &EndpointSet {
        &self.endpoints
    }

    /// `ProxyStore.Series`: every series matching `matchers` with its
    /// samples in `[hints.start_ms, hints.end_ms]`, from every store that
    /// can hold some.
    pub async fn series(
        &self,
        matchers: &[LabelMatcher],
        hints: &SelectHints,
        options: &SelectOptions,
    ) -> Result<SeriesResult, StoreError> {
        let compiled = compile_matchers(matchers)?;
        let abort = options.aborts();

        // Go raises this only for Series: not one endpoint has a Store API
        // and the caller wants everything or nothing.
        if abort
            && !self
                .endpoints
                .endpoints()
                .iter()
                .any(|e| e.metadata().is_some())
        {
            return Err(StoreError::NoStoresAvailable);
        }
        let stores = self.endpoints.endpoints_for(
            hints.start_ms,
            hints.end_ms,
            &compiled,
            &options.store_matchers,
        );
        if stores.is_empty() {
            tracing::debug!("no store matched the query");
            return Ok(SeriesResult::default());
        }

        let request = SeriesRequest {
            min_time: hints.start_ms,
            max_time: hints.end_ms,
            matchers: to_proto_matchers(matchers),
            max_resolution_window: 0,
            aggregates: vec![Aggr::Raw as i32],
            partial_response_disabled: abort,
            partial_response_strategy: options.partial_response as i32,
            skip_chunks: false,
            hints: None,
            step: hints.step_ms.unwrap_or(0),
            range: hints.range_ms.unwrap_or(0),
            query_hints: None,
            shard_info: None,
            without_replica_labels: Vec::new(),
            limit: 0,
            response_batch_size: 0,
        };
        tracing::debug!(
            stores = stores.len(),
            min_time = hints.start_ms,
            max_time = hints.end_ms,
            "fanning out Series"
        );

        let responses = fan_out(&stores, |store| {
            let request = request.clone();
            async move { receive_series(&store, request).await }
        })
        .await?;
        merge_series(&stores, responses, hints, &compiled, options)
    }

    /// `ProxyStore.LabelNames`: the union of the label names the stores
    /// overlapping `[start, end]` know for `matchers`.
    pub async fn label_names(
        &self,
        start: i64,
        end: i64,
        matchers: &[LabelMatcher],
        options: &SelectOptions,
    ) -> Result<LabelsResult, StoreError> {
        let compiled = compile_matchers(matchers)?;
        let stores = self
            .endpoints
            .endpoints_for(start, end, &compiled, &options.store_matchers);
        if stores.is_empty() {
            return Ok(LabelsResult::default());
        }
        let request = LabelNamesRequest {
            partial_response_disabled: options.aborts(),
            partial_response_strategy: options.partial_response as i32,
            start,
            end,
            hints: None,
            matchers: to_proto_matchers(matchers),
            without_replica_labels: Vec::new(),
            // Go does not forward the API's limit either; the union is
            // truncated after merging.
            limit: 0,
        };
        let responses = fan_out(&stores, |store| {
            let request = request.clone();
            async move {
                store
                    .store_client()
                    .label_names(request)
                    .await
                    .map(|r| r.into_inner())
                    .map(|r| (r.names, r.warnings))
            }
        })
        .await?;
        merge_labels(&stores, responses, "fetch label names from store", options)
    }

    /// `ProxyStore.LabelValues`: the union of the values of label `name`
    /// the stores overlapping `[start, end]` know for `matchers`.
    pub async fn label_values(
        &self,
        name: &str,
        start: i64,
        end: i64,
        matchers: &[LabelMatcher],
        options: &SelectOptions,
    ) -> Result<LabelsResult, StoreError> {
        let compiled = compile_matchers(matchers)?;
        let stores = self
            .endpoints
            .endpoints_for(start, end, &compiled, &options.store_matchers);
        if stores.is_empty() {
            return Ok(LabelsResult::default());
        }
        let request = LabelValuesRequest {
            label: name.to_string(),
            partial_response_disabled: options.aborts(),
            partial_response_strategy: options.partial_response as i32,
            start,
            end,
            hints: None,
            matchers: to_proto_matchers(matchers),
            without_replica_labels: Vec::new(),
            limit: 0,
        };
        let responses = fan_out(&stores, |store| {
            let request = request.clone();
            async move {
                store
                    .store_client()
                    .label_values(request)
                    .await
                    .map(|r| r.into_inner())
                    .map(|r| (r.values, r.warnings))
            }
        })
        .await?;
        merge_labels(&stores, responses, "fetch label values from store", options)
    }
}

/// Run `call` against every store at once. Results come back in store
/// order, so merging is deterministic whatever the network does.
async fn fan_out<T, F, Fut>(stores: &[Arc<EndpointRef>], call: F) -> Result<Vec<T>, StoreError>
where
    T: Send + 'static,
    F: Fn(Arc<EndpointRef>) -> Fut,
    Fut: Future<Output = T> + Send + 'static,
{
    let mut tasks = JoinSet::new();
    for (i, store) in stores.iter().enumerate() {
        let fut = call(Arc::clone(store));
        tasks.spawn(async move { (i, fut.await) });
    }
    let mut results: Vec<Option<T>> = (0..stores.len()).map(|_| None).collect();
    while let Some(joined) = tasks.join_next().await {
        let (i, result) =
            joined.map_err(|e| StoreError::Internal(format!("store task failed: {e}")))?;
        results[i] = Some(result);
    }
    Ok(results
        .into_iter()
        .map(|r| r.expect("every store task reported"))
        .collect())
}

/// Everything one store's `Series` stream produced.
#[derive(Debug, Default)]
struct StoreSeriesResponse {
    /// `series` frames and the contents of `batch` frames, in order.
    series: Vec<StoreSeries>,
    warnings: Vec<String>,
    /// The status that broke the stream, before or after some frames.
    error: Option<Status>,
}

async fn receive_series(store: &EndpointRef, request: SeriesRequest) -> StoreSeriesResponse {
    let mut out = StoreSeriesResponse::default();
    let mut stream = match store.store_client().series(request).await {
        Ok(response) => response.into_inner(),
        Err(status) => {
            out.error = Some(status);
            return out;
        }
    };
    loop {
        match stream.message().await {
            Ok(Some(frame)) => match frame.result {
                Some(series_response::Result::Series(series)) => out.series.push(series),
                Some(series_response::Result::Batch(batch)) => out.series.extend(batch.series),
                Some(series_response::Result::Warning(warning)) => out.warnings.push(warning),
                Some(series_response::Result::Hints(_)) | None => {}
            },
            Ok(None) => break,
            Err(status) => {
                out.error = Some(status);
                break;
            }
        }
    }
    out
}

/// Fold every store's frames into series the engine accepts.
fn merge_series(
    stores: &[Arc<EndpointRef>],
    responses: Vec<StoreSeriesResponse>,
    hints: &SelectHints,
    compiled: &[CompiledMatcher],
    options: &SelectOptions,
) -> Result<SeriesResult, StoreError> {
    let abort = options.aborts();
    let mut warnings = Vec::new();
    let mut merged: BTreeMap<LabelSet, BTreeMap<i64, f64>> = BTreeMap::new();
    let mut skipped_chunks = 0usize;

    for (store, response) in stores.iter().zip(responses) {
        for warning in response.warnings {
            if abort {
                return Err(StoreError::Aborted {
                    addr: store.addr().to_string(),
                    warning,
                });
            }
            warnings.push(warning);
        }
        if let Some(status) = response.error {
            let err = StoreError::rpc("receive series from", store.addr(), status);
            if abort {
                return Err(err);
            }
            // Frames received before the failure stay, as Go keeps what it
            // had buffered.
            warnings.push(err.to_string());
        }

        for series in response.series {
            let labels = LabelSet::from_proto(&series.labels);
            if !merged.contains_key(&labels) {
                merged.insert(labels.clone(), BTreeMap::new());
            }
            let samples = merged.get_mut(&labels).expect("just inserted");

            // Earlier chunks win on equal timestamps, as Prometheus's
            // chunk iterator takes the first chunk's sample.
            let mut chunks = series.chunks;
            chunks.sort_by_key(|c| (c.min_time, c.max_time));
            for chunk in &chunks {
                let Some(raw) = &chunk.raw else { continue };
                if raw.r#type != Encoding::Xor as i32 {
                    skipped_chunks += 1;
                    continue;
                }
                let decoded = decode_xor(&raw.data).map_err(|source| StoreError::Chunk {
                    addr: store.addr().to_string(),
                    labels: labels.clone(),
                    source,
                })?;
                for (t, v) in decoded {
                    if t < hints.start_ms || t > hints.end_ms {
                        continue;
                    }
                    samples.entry(t).or_insert(v);
                }
            }
        }
    }
    if skipped_chunks > 0 {
        tracing::debug!(skipped_chunks, "skipped chunks of non-float encodings");
    }

    let mut series = Vec::with_capacity(merged.len());
    for (labels, samples) in merged {
        if samples.is_empty() {
            continue;
        }
        let (timestamps, values): (Vec<i64>, Vec<f64>) = samples.into_iter().unzip();
        let built = Series::new(&labels.as_strs(), timestamps, values).map_err(|message| {
            StoreError::Series {
                labels: labels.clone(),
                message,
            }
        })?;
        if options.verify_matchers && !matches_all(compiled, &built) {
            warnings.push(format!(
                "dropped series {labels} that does not match the selector"
            ));
            continue;
        }
        series.push(built);
    }
    Ok(SeriesResult { series, warnings })
}

/// One store's label names or values, with its warnings.
type LabelsResponse = Result<(Vec<String>, Vec<String>), Status>;

/// Union the stores' label names or values, sorted and without repeats.
fn merge_labels(
    stores: &[Arc<EndpointRef>],
    responses: Vec<LabelsResponse>,
    rpc: &'static str,
    options: &SelectOptions,
) -> Result<LabelsResult, StoreError> {
    let mut values = Vec::new();
    let mut warnings = Vec::new();
    for (store, response) in stores.iter().zip(responses) {
        match response {
            Ok((found, store_warnings)) => {
                values.extend(found);
                warnings.extend(store_warnings);
            }
            Err(status) => {
                let err = StoreError::rpc(rpc, store.addr(), status);
                if options.aborts() {
                    return Err(err);
                }
                warnings.push(err.to_string());
            }
        }
    }
    values.sort();
    values.dedup();
    Ok(LabelsResult { values, warnings })
}

#[cfg(test)]
mod tests {
    use promql_parser::ast::MatchOp;
    use promql_parser::posrange::PositionRange;
    use tonic::Code;

    use super::*;
    use crate::endpointset::EndpointSetConfig;
    use crate::storepb::thanos::{label_matcher, LabelNamesResponse, LabelValuesResponse};
    use crate::testutil::{
        batch_frame, histogram_chunk, info, raw_chunk, series as store_series, series_frame, serve,
        warning_frame, FakeStore,
    };
    use crate::PartialResponseStrategy;

    fn matcher(name: &str, op: MatchOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            name: name.to_string(),
            op,
            value: value.to_string(),
            pos_range: PositionRange::default(),
        }
    }

    fn up() -> Vec<LabelMatcher> {
        vec![matcher("__name__", MatchOp::Equal, "up")]
    }

    fn abort() -> SelectOptions {
        SelectOptions {
            partial_response: PartialResponseStrategy::Abort,
            ..Default::default()
        }
    }

    fn samples(series: &Series) -> Vec<(i64, f64)> {
        series
            .timestamps()
            .iter()
            .copied()
            .zip(series.values().iter().copied())
            .collect()
    }

    /// Serve every fake, dial them all in order and fetch their Info.
    async fn proxy_over(fakes: &[Arc<FakeStore>]) -> ProxyStore {
        let mut addrs = Vec::new();
        for fake in fakes {
            let (addr, _server) = serve(Arc::clone(fake)).await;
            addrs.push(addr);
        }
        let set = EndpointSet::new(&addrs, &EndpointSetConfig::default()).unwrap();
        set.update().await;
        ProxyStore::new(Arc::new(set))
    }

    fn sidecar(min_time: i64, max_time: i64) -> FakeStore {
        FakeStore::new(info("sidecar", &[&[("replica", "a")]], min_time, max_time))
    }

    #[tokio::test]
    async fn merges_split_frames_batches_and_two_stores() {
        let a = Arc::new(sidecar(0, 10_000).with_frames(vec![
            series_frame(store_series(
                &[("__name__", "up"), ("job", "a")],
                vec![raw_chunk(&[(1_000, 1.0), (2_000, 1.0)])],
            )),
            // The same series continues in the next frame.
            series_frame(store_series(
                &[("__name__", "up"), ("job", "a")],
                vec![raw_chunk(&[(3_000, 1.0)])],
            )),
        ]));
        let b = Arc::new(sidecar(0, 10_000).with_frames(vec![batch_frame(vec![
            // Overlaps store a at 2000; a's sample wins, being first.
            store_series(
                &[("__name__", "up"), ("job", "a")],
                vec![raw_chunk(&[(2_000, 9.0), (4_000, 1.0)])],
            ),
            store_series(
                &[("__name__", "up"), ("job", "b")],
                vec![raw_chunk(&[(1_000, 0.0)])],
            ),
        ])]));
        let proxy = proxy_over(&[a, b]).await;

        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert!(result.warnings.is_empty(), "{:?}", result.warnings);
        assert_eq!(result.series.len(), 2);
        assert_eq!(result.series[0].label("job"), "a");
        assert_eq!(
            samples(&result.series[0]),
            vec![(1_000, 1.0), (2_000, 1.0), (3_000, 1.0), (4_000, 1.0)]
        );
        assert_eq!(result.series[1].label("job"), "b");
        assert_eq!(samples(&result.series[1]), vec![(1_000, 0.0)]);
    }

    #[tokio::test]
    async fn clips_samples_to_the_range_and_orders_chunks() {
        let store = Arc::new(
            sidecar(0, 100_000).with_frames(vec![series_frame(store_series(
                &[("__name__", "up")],
                vec![
                    // Out of order on the wire; sorted by bounds before merging.
                    raw_chunk(&[(5_000, 5.0), (6_000, 6.0), (9_000, 9.0)]),
                    raw_chunk(&[(1_000, 1.0), (2_000, 2.0), (5_000, 50.0)]),
                ],
            ))]),
        );
        let proxy = proxy_over(std::slice::from_ref(&store)).await;

        let result = proxy
            .series(
                &up(),
                &SelectHints::range(2_000, 6_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.series.len(), 1);
        assert_eq!(
            samples(&result.series[0]),
            vec![(2_000, 2.0), (5_000, 50.0), (6_000, 6.0)],
            "inclusive bounds, the earlier chunk wins at 5000"
        );

        let request = &store.series_requests()[0];
        assert_eq!((request.min_time, request.max_time), (2_000, 6_000));
        assert_eq!(request.aggregates, vec![Aggr::Raw as i32]);
        assert_eq!(request.matchers.len(), 1);
        assert_eq!(request.matchers[0].r#type, label_matcher::Type::Eq as i32);
        assert_eq!(request.matchers[0].value, "up");
        assert_eq!(
            request.partial_response_strategy,
            PartialResponseStrategy::Warn as i32
        );
        assert!(!request.partial_response_disabled);
    }

    #[tokio::test]
    async fn a_series_with_no_samples_in_range_is_dropped() {
        let store = Arc::new(
            sidecar(0, 100_000).with_frames(vec![series_frame(store_series(
                &[("__name__", "up")],
                vec![raw_chunk(&[(1_000, 1.0)])],
            ))]),
        );
        let proxy = proxy_over(&[store]).await;
        let result = proxy
            .series(
                &up(),
                &SelectHints::range(2_000, 6_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert!(result.series.is_empty());
    }

    #[tokio::test]
    async fn warning_frames_warn_or_abort() {
        let store = Arc::new(sidecar(0, 10_000).with_frames(vec![
            warning_frame("block 01ABC is missing"),
            series_frame(store_series(
                &[("__name__", "up")],
                vec![raw_chunk(&[(1_000, 1.0)])],
            )),
        ]));
        let proxy = proxy_over(&[store]).await;

        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.series.len(), 1);
        assert_eq!(result.warnings, vec!["block 01ABC is missing"]);

        let err = proxy
            .series(&up(), &SelectHints::range(0, 10_000), &abort())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, StoreError::Aborted { warning, .. } if warning == "block 01ABC is missing"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_failing_store_warns_or_aborts() {
        let healthy = Arc::new(
            sidecar(0, 10_000).with_frames(vec![series_frame(store_series(
                &[("__name__", "up")],
                vec![raw_chunk(&[(1_000, 1.0)])],
            ))]),
        );
        let down = Arc::new(
            sidecar(0, 10_000).with_series_status(Status::unavailable("connection refused")),
        );
        let proxy = proxy_over(&[healthy, down.clone()]).await;

        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.series.len(), 1, "the healthy store's data is kept");
        assert_eq!(result.warnings.len(), 1);
        let warning = &result.warnings[0];
        assert!(
            warning.starts_with("receive series from 127.0.0.1:"),
            "{warning}"
        );
        assert!(
            warning.ends_with("Unavailable: connection refused"),
            "{warning}"
        );

        let err = proxy
            .series(&up(), &SelectHints::range(0, 10_000), &abort())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, StoreError::Rpc { status, .. } if status.code() == Code::Unavailable),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_stream_that_breaks_keeps_its_earlier_frames() {
        let store = Arc::new(sidecar(0, 10_000).with_frames(vec![
            series_frame(store_series(
                &[("__name__", "up")],
                vec![raw_chunk(&[(1_000, 1.0)])],
            )),
            Err(Status::internal("disk read failed")),
        ]));
        let proxy = proxy_over(&[store]).await;
        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.series.len(), 1);
        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("Internal: disk read failed"));
    }

    #[tokio::test]
    async fn stores_outside_the_range_or_labels_are_not_asked() {
        let east = Arc::new(
            FakeStore::new(info("store", &[&[("cluster", "east")]], 0, 100_000)).with_frames(vec![
                series_frame(store_series(
                    &[("__name__", "up"), ("cluster", "east")],
                    vec![raw_chunk(&[(1_000, 1.0)])],
                )),
            ]),
        );
        let west = Arc::new(FakeStore::new(info(
            "store",
            &[&[("cluster", "west")]],
            0,
            100_000,
        )));
        let old = Arc::new(FakeStore::new(info("store", &[], 0, 500)));
        let proxy = proxy_over(&[east.clone(), west.clone(), old.clone()]).await;

        let matchers = vec![
            matcher("__name__", MatchOp::Equal, "up"),
            matcher("cluster", MatchOp::Equal, "east"),
        ];
        let result = proxy
            .series(
                &matchers,
                &SelectHints::range(1_000, 2_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.series.len(), 1);
        assert_eq!(east.series_requests().len(), 1);
        assert_eq!(
            west.series_requests().len(),
            0,
            "its external labels contradict the matchers"
        );
        assert_eq!(
            old.series_requests().len(),
            0,
            "its data ends before the range"
        );
    }

    #[tokio::test]
    async fn store_matchers_pick_stores_by_address() {
        let a = Arc::new(sidecar(0, 10_000));
        let b = Arc::new(sidecar(0, 10_000));
        let proxy = proxy_over(&[a.clone(), b.clone()]).await;
        let b_addr = proxy.endpoints().endpoints()[1].addr().to_string();

        let options = SelectOptions {
            store_matchers: vec![vec![CompiledMatcher::compile(&matcher(
                "__address__",
                MatchOp::Equal,
                &b_addr,
            ))
            .unwrap()]],
            ..Default::default()
        };
        proxy
            .series(&up(), &SelectHints::range(0, 10_000), &options)
            .await
            .unwrap();
        assert_eq!(a.series_requests().len(), 0);
        assert_eq!(b.series_requests().len(), 1);
    }

    #[tokio::test]
    async fn histogram_chunks_are_skipped() {
        let store = Arc::new(
            sidecar(0, 10_000).with_frames(vec![series_frame(store_series(
                &[("__name__", "up")],
                vec![histogram_chunk(0, 5_000), raw_chunk(&[(1_000, 1.0)])],
            ))]),
        );
        let proxy = proxy_over(&[store]).await;
        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(samples(&result.series[0]), vec![(1_000, 1.0)]);
        assert!(result.warnings.is_empty());
    }

    #[tokio::test]
    async fn a_corrupt_chunk_is_an_error() {
        let mut chunk = raw_chunk(&[(1_000, 1.0)]);
        chunk.raw.as_mut().unwrap().data.truncate(3);
        let store = Arc::new(
            sidecar(0, 10_000).with_frames(vec![series_frame(store_series(
                &[("__name__", "up"), ("job", "x")],
                vec![chunk],
            ))]),
        );
        let proxy = proxy_over(&[store]).await;
        let err = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Chunk { .. }), "{err:?}");
        assert!(
            err.to_string().contains(r#"{__name__="up", job="x"}"#),
            "{err}"
        );
    }

    #[tokio::test]
    async fn series_not_matching_the_selector_are_dropped_unless_told_otherwise() {
        let store =
            Arc::new(
                sidecar(0, 10_000)
                    .ignoring_matchers()
                    .with_frames(vec![series_frame(store_series(
                        &[("__name__", "down")],
                        vec![raw_chunk(&[(1_000, 1.0)])],
                    ))]),
            );
        let proxy = proxy_over(&[store]).await;

        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert!(result.series.is_empty());
        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("does not match the selector"));

        let trusting = SelectOptions {
            verify_matchers: false,
            ..Default::default()
        };
        let result = proxy
            .series(&up(), &SelectHints::range(0, 10_000), &trusting)
            .await
            .unwrap();
        assert_eq!(result.series.len(), 1);
    }

    #[tokio::test]
    async fn without_any_store_series_is_empty_or_aborts() {
        let proxy = proxy_over(&[]).await;
        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert!(result.series.is_empty() && result.warnings.is_empty());

        let err = proxy
            .series(&up(), &SelectHints::range(0, 10_000), &abort())
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::NoStoresAvailable));
        assert_eq!(err.to_string(), "No StoreAPIs available");
    }

    #[tokio::test]
    async fn label_names_and_values_are_unioned() {
        let a = Arc::new(
            sidecar(0, 10_000)
                .with_label_names(Ok(LabelNamesResponse {
                    names: vec!["job".into(), "__name__".into()],
                    warnings: vec!["a is slow".into()],
                    hints: None,
                }))
                .with_label_values(Ok(LabelValuesResponse {
                    values: vec!["api".into()],
                    warnings: vec![],
                    hints: None,
                })),
        );
        let b = Arc::new(
            sidecar(0, 10_000)
                .with_label_names(Ok(LabelNamesResponse {
                    names: vec!["instance".into(), "job".into()],
                    warnings: vec![],
                    hints: None,
                }))
                .with_label_values(Err(Status::unavailable("gone"))),
        );
        let proxy = proxy_over(&[a.clone(), b.clone()]).await;

        let names = proxy
            .label_names(0, 10_000, &[], &SelectOptions::default())
            .await
            .unwrap();
        assert_eq!(names.values, vec!["__name__", "instance", "job"]);
        assert_eq!(names.warnings, vec!["a is slow"]);
        let request = &a.label_names_requests()[0];
        assert_eq!((request.start, request.end), (0, 10_000));
        assert!(request.matchers.is_empty());

        let values = proxy
            .label_values("job", 0, 10_000, &up(), &SelectOptions::default())
            .await
            .unwrap();
        assert_eq!(values.values, vec!["api"]);
        assert_eq!(values.warnings.len(), 1);
        assert!(values.warnings[0].starts_with("fetch label values from store 127.0.0.1:"));
        assert_eq!(a.label_values_requests()[0].label, "job");
        assert_eq!(a.label_values_requests()[0].matchers[0].value, "up");

        let err = proxy
            .label_values("job", 0, 10_000, &up(), &abort())
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::Rpc { .. }), "{err:?}");

        // A store outside the range is not asked.
        assert!(proxy
            .label_names(20_000, 30_000, &[], &SelectOptions::default())
            .await
            .unwrap()
            .values
            .is_empty());
        assert_eq!(a.label_names_requests().len(), 1);
    }
}
