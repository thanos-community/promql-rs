//! `ProxyStore`, the client half of `pkg/store/proxy.go`: fan a request
//! out to the endpoints that can hold data and merge what comes back.
//!
//! Series handling differs from Go in one deliberate way. Go merges the
//! stores' sorted streams lazily with a loser tree; here each store's
//! frames are grouped by label set as they arrive and the stores' groups
//! merged once all have answered, which covers a series split across
//! frames and the same series from two stores. The groups are kept in
//! the engine's struct order rather than the stores' `labels.Compare`
//! order, since that is the order every block must be emitted in and
//! the two disagree wherever label names differ. Samples are not merged:
//! the chunks go on as they came, and the engine skips what an earlier
//! chunk of the label set already covered, as Thanos's
//! `chunkSeriesIterator` does.

use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};

use crate::labels::Matcher;
use promql_engine::SelectHints;
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

/// The stores' answer to one `Series` request, grouped by label set.
#[derive(Debug, Default)]
pub struct SeriesResult {
    /// One per label set with a chunk in range, in the engine's struct
    /// order ([`LabelSet::cmp_struct`]).
    pub series: Vec<ChunkedSeries>,
    /// Sorted: the names of every label set the stores sent that passed
    /// the selector, chunks in range or not, so the schema does not hinge
    /// on how a store trims chunks at the range's edges.
    pub names: Vec<String>,
    pub warnings: Vec<String>,
}

/// Every store's chunks of one label set, still encoded: a selection's
/// chunks are held for the whole query, and a decoded sample takes about
/// ten times the bytes of an XOR one.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkedSeries {
    pub labels: LabelSet,
    /// None entirely outside the request, sorted by `min_time`, then
    /// `max_time` and data, and without exact repeats, as the Go proxy
    /// hands them on. They may still overlap, as when two stores or two
    /// replicas hold the same data.
    pub chunks: Vec<RawChunk>,
    /// With deduplication, `firsts[slot]` is the first sample Go's iterator
    /// over that replica yields, see [`Self::seed_firsts`]; empty before.
    pub firsts: Vec<Option<(i64, f64)>>,
}

/// One XOR chunk with the bounds its meta claims.
#[derive(Debug, Clone, PartialEq)]
pub struct RawChunk {
    pub min_time: i64,
    pub max_time: i64,
    /// For the error when the chunk does not decode.
    pub addr: Arc<str>,
    pub data: Vec<u8>,
    /// All of `data`'s samples, when decoded while the frames were still
    /// arriving so the first block is ready when the last one lands.
    /// `None` also for a chunk that failed to decode then: the block that
    /// reads it decodes again and reports the error.
    pub decoded: Option<Vec<(i64, f64)>>,
    /// The replica [`ChunkedSeries::split_overlaps`] assigned the chunk
    /// to; 0 until then.
    pub slot: u32,
}

/// A chunk's samples clipped to a window, with the slot of the chunk.
pub type WindowChunk = (u32, Vec<(i64, f64)>);

impl ChunkedSeries {
    /// `NewOverlapSplit` in Thanos `pkg/dedup/iter.go`: give every chunk
    /// the replica it would be a part of once overlapping chunks are
    /// pulled apart. Each chunk goes to the first replica whose last chunk
    /// ends strictly before it starts, else to a new one.
    ///
    /// This runs once over the series' whole chunk list, not per block:
    /// on the samples a block's decode leaves, a chunk's neighbours
    /// change from block to block and with them its replica, and the
    /// merge downstream would follow a different replica in every block.
    /// Needs the chunks sorted by `min_time`, which the proxy guarantees.
    pub fn split_overlaps(&mut self) {
        let mut last_max: Vec<i64> = Vec::new();
        for chunk in &mut self.chunks {
            let slot = last_max.iter().position(|&max| max < chunk.min_time);
            let slot = slot.unwrap_or_else(|| {
                last_max.push(chunk.max_time);
                last_max.len() - 1
            });
            last_max[slot] = chunk.max_time;
            chunk.slot = slot as u32;
        }
    }

    /// Names, per slot, the first sample `NewBoundedSeriesIterator(mint, maxt)`
    /// over that replica's chunks yields, which `querier.go` builds for each
    /// split series: the first sample at or after `mint`, if it is not after
    /// `maxt`. The merge downstream reads these before any block holds the
    /// sample: a counter's first pick of a level is lifted by the first
    /// sample of the replicas below it, wherever in the query that lies.
    ///
    /// Needs [`Self::split_overlaps`] first. A chunk that does not decode
    /// names nothing; the block that reads it reports the error.
    pub fn seed_firsts(&mut self, mint: i64, maxt: i64) {
        let slots = self.chunks.iter().map(|c| c.slot as usize + 1).max();
        self.firsts = vec![None; slots.unwrap_or(0)];
        for chunk in &self.chunks {
            let first = &mut self.firsts[chunk.slot as usize];
            if first.is_some() || chunk.max_time < mint || chunk.min_time > maxt {
                continue;
            }
            let samples = match &chunk.decoded {
                Some(samples) => samples.clone(),
                None => decode_xor(&chunk.data).unwrap_or_default(),
            };
            *first = samples
                .into_iter()
                .find(|&(t, _)| t >= mint)
                .filter(|&(t, _)| t <= maxt);
        }
    }

    /// The samples in `reach`, inclusive, of every chunk whose bounds
    /// reach into it: decoded, clipped, none empty, ascending by first
    /// sample, each with its chunk's slot.
    pub fn decode_window(
        &self,
        reach: RangeInclusive<i64>,
    ) -> Result<Vec<WindowChunk>, StoreError> {
        let (min, max) = (*reach.start(), *reach.end());
        let mut chunks = Vec::new();
        for chunk in &self.chunks {
            if chunk.max_time < min || chunk.min_time > max {
                continue;
            }
            let mut samples = match &chunk.decoded {
                Some(samples) => samples.clone(),
                None => decode_xor(&chunk.data).map_err(|source| StoreError::Chunk {
                    addr: chunk.addr.to_string(),
                    labels: self.labels.clone(),
                    source,
                })?,
            };
            samples.truncate(samples.partition_point(|&(t, _)| t <= max));
            samples.drain(..samples.partition_point(|&(t, _)| t < min));
            if !samples.is_empty() {
                chunks.push((chunk.min_time, chunk.max_time, chunk.slot, samples));
            }
        }
        // By the first sample left after clipping, which is what the
        // engine checks, and clipping can put a chunk with the later
        // min_time first. Ties go by bounds, as the Go proxy sorts, then
        // by arrival: the engine keeps the first chunk's sample.
        chunks.sort_by_key(|(min, max, _, samples)| (samples[0].0, *min, *max));
        Ok(chunks
            .into_iter()
            .map(|(_, _, slot, samples)| (slot, samples))
            .collect())
    }
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
    /// Addresses already warned about lacking `without_replica_labels`: a
    /// store does not change that between calls, and a warning per `Series`
    /// call per store would flood the log.
    warned: Arc<Mutex<HashSet<String>>>,
}

impl ProxyStore {
    pub fn new(endpoints: Arc<EndpointSet>) -> Self {
        Self {
            endpoints,
            warned: Arc::default(),
        }
    }

    pub fn endpoints(&self) -> &EndpointSet {
        &self.endpoints
    }

    /// `ProxyStore.Series`: the chunks of every series matching
    /// `matchers` with samples in `[hints.start_ms, hints.end_ms]`, one
    /// request to every store that can hold some, all at once. A store's
    /// warnings and failure become warnings, or the error when partial
    /// responses are not allowed; a store that breaks off mid-stream keeps
    /// the frames it sent, as Go keeps what it had buffered.
    pub async fn series(
        &self,
        matchers: &[LabelMatcher],
        hints: &SelectHints,
        options: &SelectOptions,
    ) -> Result<SeriesResult, StoreError> {
        self.series_for_blocks(matchers, hints, options, None).await
    }

    /// [`Self::series`], also decoding on arrival the chunks reaching into
    /// `first_reach`, so decoding the first block overlaps the transfer.
    /// Only the first: every decoded sample is held until its block runs,
    /// at about ten times the encoded bytes.
    pub async fn series_for_blocks(
        &self,
        matchers: &[LabelMatcher],
        hints: &SelectHints,
        options: &SelectOptions,
        first_reach: Option<RangeInclusive<i64>>,
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
        tracing::debug!(
            stores = stores.len(),
            min_time = hints.start_ms,
            max_time = hints.end_ms,
            "fanning out Series"
        );

        let call = Arc::new(SeriesCall {
            request: series_request(matchers, hints, options),
            // No matchers verify nothing, which is what switching the
            // check off means. A matcher on a replica label is the
            // store's to apply: the answer no longer carries the label,
            // and checking it here would drop every series it selects.
            verify: if options.verify_matchers {
                compiled
                    .into_iter()
                    .filter(|m| !options.replica_labels().iter().any(|l| l == m.name()))
                    .collect()
            } else {
                Vec::new()
            },
            abort,
            first_reach,
            warned: Arc::clone(&self.warned),
        });
        let mut tasks = JoinSet::new();
        for (i, store) in stores.iter().enumerate() {
            let (store, call) = (Arc::clone(store), Arc::clone(&call));
            tasks.spawn(async move { (i, receive_series(&store, &call).await) });
        }
        let mut received: Vec<Option<Received>> = (0..stores.len()).map(|_| None).collect();
        while let Some(joined) = tasks.join_next().await {
            let (i, result) =
                joined.map_err(|e| StoreError::Internal(format!("store task failed: {e}")))?;
            // An early return drops the join set, which aborts the stores
            // still streaming: in abort mode their frames are moot.
            received[i] = Some(result?);
        }

        // In store order, so a label set's chunks, and with them the
        // engine's pick among overlapping ones, do not depend on which
        // store answered first.
        let mut merged: BTreeMap<StructOrdered, Vec<RawChunk>> = BTreeMap::new();
        let mut warnings = Vec::new();
        for store in received {
            let store = store.expect("every store task reported");
            warnings.extend(store.warnings);
            if merged.is_empty() {
                merged = store.series;
                continue;
            }
            for (labels, chunks) in store.series {
                merged.entry(labels).or_default().extend(chunks);
            }
        }
        let names: BTreeSet<&str> = merged
            .keys()
            .flat_map(|labels| labels.0.pairs().iter().map(|(name, _)| name.as_str()))
            .collect();
        let names = names.into_iter().map(str::to_string).collect();
        let series = merged
            .into_iter()
            .filter(|(_, chunks)| !chunks.is_empty())
            .map(|(labels, mut chunks)| {
                dedup_identical_chunks(&mut chunks);
                let mut series = ChunkedSeries {
                    labels: labels.0,
                    chunks,
                    firsts: Vec::new(),
                };
                if options.dedup_enabled() {
                    series.split_overlaps();
                    series.seed_firsts(hints.start_ms, hints.end_ms);
                }
                series
            })
            .collect();
        Ok(SeriesResult {
            series,
            names,
            warnings,
        })
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
            without_replica_labels: options.replica_labels().to_vec(),
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
            without_replica_labels: options.replica_labels().to_vec(),
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

/// `responseDeduplicator.chainSeriesAndRemIdenticalChunks` in Thanos
/// `pkg/store/proxy_merge.go`: drop a chunk whose bytes an earlier one of
/// the series has, and order what is left by `min_time`, `max_time`, then
/// data *descending*, `AggrChunk.Compare` under `sort.Slice(.. Compare > 0)`:
/// `Chunk.Compare` is `bytes.Compare(m, b)`, positive for the larger data,
/// where the bounds return positive for the smaller. So of two chunks with
/// equal bounds the one with the larger bytes goes first, which for XOR is
/// the one with more samples (the sample count leads the data). Ascending
/// swaps them, and with them the replica slots the overlap split hands out
/// when a compacted block's chunk and a replica's share bounds: the penalty
/// merge then walks other replicas, and a query's last points differ from
/// Go's. The overlap split needs that order, and the
/// proxy applies it with or without replica labels: Go's proxy always does
/// (`enableDedup` is true there), so with deduplication off the chunks are
/// still sorted and identical ones dropped. `bd8f170` kept arrival order
/// for that case; the order is Go parity now, not an accident.
///
/// Go identifies a chunk by an xxhash of its data and keeps the hash
/// alone; comparing the bytes drops exactly the identical chunks, none
/// to a collision. Equal data implies equal bounds, so after the sort
/// the identical ones are adjacent.
fn dedup_identical_chunks(chunks: &mut Vec<RawChunk>) {
    chunks.sort_by(|a, b| {
        (a.min_time, a.max_time)
            .cmp(&(b.min_time, b.max_time))
            .then_with(|| b.data.cmp(&a.data))
    });
    chunks.dedup_by(|later, earlier| later.data == earlier.data);
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

/// A label set as a key ordered by [`LabelSet::cmp_struct`]. Equality
/// stays `LabelSet`'s, which agrees with it.
#[derive(Debug, PartialEq, Eq)]
struct StructOrdered(LabelSet);

impl Ord for StructOrdered {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp_struct(&other.0)
    }
}

impl PartialOrd for StructOrdered {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// What every store task of one `Series` fan-out shares.
struct SeriesCall {
    request: SeriesRequest,
    verify: Vec<Matcher>,
    abort: bool,
    first_reach: Option<RangeInclusive<i64>>,
    warned: Arc<Mutex<HashSet<String>>>,
}

/// One store's frames, grouped by label set.
#[derive(Default)]
struct Received {
    /// Every label set that passed the selector, with the XOR chunks
    /// that reach into the request, possibly none.
    series: BTreeMap<StructOrdered, Vec<RawChunk>>,
    warnings: Vec<String>,
}

/// Stream one store's reply into [`Received`] as it arrives, so grouping
/// overlaps the transfer. `Err` only when partial responses are not
/// allowed.
async fn receive_series(store: &EndpointRef, call: &SeriesCall) -> Result<Received, StoreError> {
    let mut out = Received::default();
    let addr: Arc<str> = Arc::from(store.addr());
    let mut skipped_chunks = 0usize;
    // `newAsyncRespSet`: a store that cannot drop the labels itself has
    // them dropped here; Go then also re-sorts, which the grouping by
    // label set does for us.
    let strip: &[String] = match store.metadata() {
        Some(m) if m.supports_without_replica_labels => &[],
        _ => &call.request.without_replica_labels,
    };
    if !strip.is_empty()
        && call
            .warned
            .lock()
            .is_ok_and(|mut w| w.insert(addr.to_string()))
    {
        tracing::warn!(
            addr = store.addr(),
            "store does not support without_replica_labels, stripping them in the proxy"
        );
    }
    let failure = match store.store_client().series(call.request.clone()).await {
        Err(status) => Some(status),
        Ok(response) => {
            let mut stream = response.into_inner();
            loop {
                match stream.message().await {
                    Ok(Some(frame)) => match frame.result {
                        Some(series_response::Result::Series(series)) => {
                            skipped_chunks += out.add(series, &addr, call, strip);
                        }
                        Some(series_response::Result::Batch(batch)) => {
                            for series in batch.series {
                                skipped_chunks += out.add(series, &addr, call, strip);
                            }
                        }
                        Some(series_response::Result::Warning(warning)) => {
                            if call.abort {
                                return Err(StoreError::Aborted {
                                    addr: addr.to_string(),
                                    warning,
                                });
                            }
                            out.warnings.push(warning);
                        }
                        Some(series_response::Result::Hints(_)) | None => {}
                    },
                    Ok(None) => break None,
                    Err(status) => break Some(status),
                }
            }
        }
    };
    if skipped_chunks > 0 {
        tracing::debug!(
            addr = store.addr(),
            skipped_chunks,
            "skipped chunks of non-float encodings"
        );
    }
    if let Some(status) = failure {
        let err = StoreError::rpc("receive series from", store.addr(), status);
        if call.abort {
            return Err(err);
        }
        out.warnings.push(err.to_string());
    }
    Ok(out)
}

impl Received {
    /// File one frame's series under its label set; returns how many of
    /// its chunks were of an encoding this client skips.
    fn add(
        &mut self,
        series: StoreSeries,
        addr: &Arc<str>,
        call: &SeriesCall,
        strip: &[String],
    ) -> usize {
        let labels = LabelSet::new(
            series
                .labels
                .into_iter()
                .map(|label| (label.name, label.value))
                .collect(),
        );
        let labels = if strip.is_empty() {
            labels
        } else {
            labels.without(strip)
        };
        let chunks = match self.series.entry(StructOrdered(labels)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if !verified(&entry.key().0, &call.verify, &mut self.warnings) {
                    return 0;
                }
                entry.insert(Vec::new())
            }
        };
        let (min, max) = (call.request.min_time, call.request.max_time);
        let mut skipped = 0;
        for chunk in series.chunks {
            // A store may send chunks reaching past the request; one
            // entirely outside it is not worth keeping.
            if chunk.max_time < min || chunk.min_time > max {
                continue;
            }
            let Some(raw) = chunk.raw else { continue };
            if raw.r#type != Encoding::Xor as i32 {
                skipped += 1;
                continue;
            }
            let decoded = call
                .first_reach
                .as_ref()
                .filter(|r| chunk.max_time >= *r.start() && chunk.min_time <= *r.end())
                .and_then(|_| decode_xor(&raw.data).ok());
            chunks.push(RawChunk {
                min_time: chunk.min_time,
                max_time: chunk.max_time,
                addr: Arc::clone(addr),
                data: raw.data,
                decoded,
                slot: 0,
            });
        }
        skipped
    }
}

/// The `Series` request for `hints`' range, raw data only.
fn series_request(
    matchers: &[LabelMatcher],
    hints: &SelectHints,
    options: &SelectOptions,
) -> SeriesRequest {
    SeriesRequest {
        min_time: hints.start_ms,
        max_time: hints.end_ms,
        matchers: to_proto_matchers(matchers),
        max_resolution_window: 0,
        aggregates: vec![Aggr::Raw as i32],
        partial_response_disabled: options.aborts(),
        partial_response_strategy: options.partial_response as i32,
        skip_chunks: false,
        hints: None,
        step: hints.step_ms.unwrap_or(0),
        range: hints.range_ms.unwrap_or(0),
        query_hints: None,
        shard_info: None,
        // Only when deduplicating, as `querier.selectFn` does: raw replicas
        // keep the labels that tell them apart.
        without_replica_labels: options.replica_labels().to_vec(),
        limit: 0,
        response_batch_size: 0,
    }
}

/// Whether `labels` satisfies every matcher of `compiled`; a series that
/// fails is dropped with a warning. The caller passes none when
/// `verify_matchers` is off, since the scan walks every matcher of every
/// series.
///
/// A label a store never sent reads as "", which is what the matchers are
/// defined against, so an absent label needs no case of its own here.
fn verified(labels: &LabelSet, compiled: &[Matcher], warnings: &mut Vec<String>) -> bool {
    if compiled
        .iter()
        .all(|m| m.matches(labels.get(m.name()).unwrap_or("")))
    {
        return true;
    }
    warnings.push(format!(
        "dropped series {labels} that does not match the selector"
    ));
    false
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

    /// Decoded over the range the test asked for.
    fn chunks(series: &ChunkedSeries, reach: RangeInclusive<i64>) -> Vec<Vec<(i64, f64)>> {
        series
            .decode_window(reach)
            .unwrap()
            .into_iter()
            .map(|(_, samples)| samples)
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
    async fn groups_split_frames_batches_and_two_stores() {
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
            // Overlaps store a at 2000; kept, the engine picks a sample.
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
        assert_eq!(result.series[0].labels.get("job"), Some("a"));
        assert_eq!(
            chunks(&result.series[0], 0..=10_000),
            vec![
                vec![(1_000, 1.0), (2_000, 1.0)],
                vec![(2_000, 9.0), (4_000, 1.0)],
                vec![(3_000, 1.0)],
            ],
            "every store's chunks, by first sample"
        );
        assert_eq!(result.series[1].labels.get("job"), Some("b"));
        assert_eq!(
            chunks(&result.series[1], 0..=10_000),
            vec![vec![(1_000, 0.0)]]
        );
    }

    #[tokio::test]
    async fn clips_samples_to_the_range_and_orders_chunks() {
        let store = Arc::new(
            sidecar(0, 100_000).with_frames(vec![series_frame(store_series(
                &[("__name__", "up")],
                vec![
                    // Out of order on the wire.
                    raw_chunk(&[(5_000, 5.0), (6_000, 6.0), (9_000, 9.0)]),
                    raw_chunk(&[(1_000, 1.0), (1_900, 1.9), (5_000, 50.0)]),
                    // Starts after the chunk above but, clipped, before it.
                    raw_chunk(&[(1_500, 1.5), (3_000, 3.0)]),
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
            chunks(&result.series[0], 2_000..=6_000),
            vec![
                vec![(3_000, 3.0)],
                vec![(5_000, 50.0)],
                vec![(5_000, 5.0), (6_000, 6.0)],
            ],
            "inclusive bounds, chunks by their first sample in range, then by bounds"
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

    /// Only the chunks the first block reaches are decoded while the
    /// frames arrive, and a block reads the same samples either way.
    #[tokio::test]
    async fn chunks_reaching_the_first_block_are_decoded_on_arrival() {
        let store = Arc::new(
            sidecar(0, 10_000).with_frames(vec![series_frame(store_series(
                &[("__name__", "up")],
                vec![
                    raw_chunk(&[(1_000, 1.0), (2_000, 2.0)]),
                    raw_chunk(&[(5_000, 5.0), (6_000, 6.0)]),
                ],
            ))]),
        );
        let proxy = proxy_over(&[store]).await;
        let result = proxy
            .series_for_blocks(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
                Some(0..=1_500),
            )
            .await
            .unwrap();
        let series = &result.series[0];
        assert_eq!(
            series.chunks[0].decoded,
            Some(vec![(1_000, 1.0), (2_000, 2.0)])
        );
        assert_eq!(series.chunks[1].decoded, None);
        assert_eq!(chunks(series, 0..=1_500), vec![vec![(1_000, 1.0)]]);
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

    /// The schema's names come from the same fetch as the chunks: every
    /// label set that passes the selector, with chunks in range or not,
    /// including a name only some of them carry.
    #[tokio::test]
    async fn names_come_from_every_selected_label_set_of_the_fetch() {
        let store = Arc::new(sidecar(0, 10_000).ignoring_matchers().with_frames(vec![
            series_frame(store_series(
                &[("__name__", "up"), ("job", "a")],
                vec![raw_chunk(&[(1_000, 1.0)])],
            )),
            batch_frame(vec![
                store_series(&[("__name__", "up"), ("pod", "p"), ("job", "b")], vec![]),
                store_series(&[("__name__", "down"), ("zone", "z")], vec![]),
            ]),
        ]));
        let proxy = proxy_over(std::slice::from_ref(&store)).await;
        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.names, ["__name__", "job", "pod"]);
        assert_eq!(result.warnings.len(), 1, "{:?}", result.warnings);
        assert!(result.warnings[0].contains(r#"{__name__="down", zone="z"}"#));
        assert_eq!(
            result.series.len(),
            1,
            "only a label set with chunks is a row"
        );
        assert_eq!(result.series[0].labels.get("job"), Some("a"));

        let requests = store.series_requests();
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].skip_chunks);
    }

    /// Stores send `labels.Compare` order; the result is in struct order,
    /// whichever store a label set came from.
    #[tokio::test]
    async fn series_come_in_struct_order_across_stores() {
        let a = Arc::new(sidecar(0, 10_000).with_frames(vec![
            series_frame(store_series(
                &[("__name__", "up"), ("a", "1"), ("b", "2")],
                vec![raw_chunk(&[(1_000, 1.0)])],
            )),
            series_frame(store_series(
                &[("__name__", "up"), ("b", "1")],
                vec![raw_chunk(&[(1_000, 1.0)])],
            )),
        ]));
        let b = Arc::new(
            sidecar(0, 10_000).with_frames(vec![series_frame(store_series(
                &[("__name__", "up"), ("a", "1"), ("c", "1")],
                vec![raw_chunk(&[(1_000, 1.0)])],
            ))]),
        );
        let proxy = proxy_over(&[a, b]).await;
        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        let got: Vec<String> = result.series.iter().map(|s| s.labels.to_string()).collect();
        assert_eq!(
            got,
            [
                r#"{__name__="up", b="1"}"#,
                r#"{__name__="up", a="1", c="1"}"#,
                r#"{__name__="up", a="1", b="2"}"#,
            ]
        );
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
            store_matchers: vec![vec![Matcher::compile(&matcher(
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
        assert_eq!(
            chunks(&result.series[0], 0..=10_000),
            vec![vec![(1_000, 1.0)]]
        );
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
        let result = proxy
            .series(
                &up(),
                &SelectHints::range(0, 10_000),
                &SelectOptions::default(),
            )
            .await
            .unwrap();
        let err = result.series[0].decode_window(0..=10_000).unwrap_err();
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

    fn chunk(min_time: i64, max_time: i64, data: &[u8]) -> RawChunk {
        RawChunk {
            min_time,
            max_time,
            addr: Arc::from("test"),
            data: data.to_vec(),
            decoded: None,
            slot: 0,
        }
    }

    fn slots(chunks: Vec<RawChunk>) -> Vec<u32> {
        let mut series = ChunkedSeries {
            labels: LabelSet::default(),
            chunks,
            firsts: Vec::new(),
        };
        series.split_overlaps();
        series.chunks.iter().map(|c| c.slot).collect()
    }

    /// `overlapSplitSet`: a chunk joins the first replica that ended
    /// strictly before it starts. Touching at a millisecond is an overlap.
    #[test]
    fn overlaps_split_into_the_first_replica_that_is_free() {
        assert_eq!(
            slots(vec![
                chunk(0, 10, b"a"),
                chunk(5, 15, b"b"),
                chunk(11, 20, b"c"),
                chunk(16, 30, b"d"),
                chunk(20, 25, b"e"),
            ]),
            [0, 1, 0, 1, 2]
        );
        assert_eq!(slots(vec![chunk(0, 10, b"a"), chunk(10, 20, b"b")]), [0, 1]);
        assert_eq!(slots(Vec::new()), Vec::<u32>::new());
    }

    /// `AggrChunk.Compare` orders (larger data first on equal bounds), an
    /// identical chunk from another frame or store is dropped, and a
    /// different chunk of the same bounds stays.
    #[test]
    fn identical_chunks_go_and_the_rest_sort_by_bounds_then_data() {
        let mut chunks = vec![
            chunk(10, 20, b"c"),
            chunk(0, 20, b"b"),
            chunk(0, 10, b"z"),
            chunk(0, 20, b"a"),
            chunk(10, 20, b"c"),
        ];
        dedup_identical_chunks(&mut chunks);
        let got: Vec<_> = chunks
            .iter()
            .map(|c| (c.min_time, c.max_time, c.data.clone()))
            .collect();
        assert_eq!(
            got,
            [
                (0, 10, b"z".to_vec()),
                (0, 20, b"b".to_vec()),
                (0, 20, b"a".to_vec()),
                (10, 20, b"c".to_vec()),
            ]
        );
    }

    fn xor_chunk(samples: &[(i64, f64)]) -> RawChunk {
        let data = crate::chunkenc::encode_xor(samples);
        chunk(samples[0].0, samples[samples.len() - 1].0, &data)
    }

    /// `NewBoundedSeriesIterator`'s first sample per replica: the first at
    /// or after `mint`, none if that is past `maxt` or the replica has no
    /// chunk in range, however the other replicas run.
    #[test]
    fn firsts_are_the_first_samples_inside_the_bounds_of_each_slot() {
        let mut series = ChunkedSeries {
            labels: LabelSet::default(),
            chunks: vec![
                xor_chunk(&[(0, 1.0), (10, 2.0), (20, 3.0)]),
                xor_chunk(&[(5, 7.0), (15, 8.0)]),
                xor_chunk(&[(25, 4.0), (30, 5.0)]),
                xor_chunk(&[(60, 9.0)]),
            ],
            firsts: Vec::new(),
        };
        series.split_overlaps();
        assert_eq!(slots_of(&series), [0, 1, 0, 0]);
        series.seed_firsts(8, 50);
        // Slot 0 skips 0 before `mint`; slot 1's 5 as well, its 15 is first.
        assert_eq!(series.firsts, [Some((10, 2.0)), Some((15, 8.0))]);
        series.seed_firsts(21, 22);
        assert_eq!(series.firsts, [None, None]);
        series.seed_firsts(i64::MIN, i64::MAX);
        assert_eq!(series.firsts, [Some((0, 1.0)), Some((5, 7.0))]);
    }

    fn slots_of(series: &ChunkedSeries) -> Vec<u32> {
        series.chunks.iter().map(|c| c.slot).collect()
    }
}
