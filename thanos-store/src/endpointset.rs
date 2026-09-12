//! The set of Store API endpoints, a static-address port of
//! `pkg/query/endpointset.go`: one gRPC channel per endpoint, its `Info`
//! metadata refreshed on a ticker, and the per-query choice of endpoints
//! that can hold data (`storeMatches` in `pkg/store/proxy.go`).

use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use promql_engine::matcher::CompiledMatcher;
use tokio::task::{JoinHandle, JoinSet};
use tonic::transport::{Channel, Endpoint};

use crate::error::StoreError;
use crate::labels::{label_sets_match, matchers_match_address, LabelSet};
use crate::storepb::thanos::info::info_client::InfoClient;
use crate::storepb::thanos::info::{InfoRequest, InfoResponse};
use crate::storepb::thanos::store_client::StoreClient;

/// How endpoints are dialed and how often they are asked who they are.
#[derive(Debug, Clone)]
pub struct EndpointSetConfig {
    pub connect_timeout: Duration,
    /// HTTP/2 keepalive ping interval, Thanos's `grpc.WithKeepaliveParams`.
    pub keepalive_interval: Duration,
    pub keepalive_timeout: Duration,
    /// Deadline of one `Info` call.
    pub info_timeout: Duration,
    /// How often [`EndpointSet::spawn_refresh`] refreshes metadata,
    /// `--endpoint-info-interval`.
    pub refresh_interval: Duration,
    /// Largest `Series` frame accepted; Go uses `math.MaxInt32`.
    pub max_recv_message_size: usize,
}

impl Default for EndpointSetConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(10),
            keepalive_timeout: Duration::from_secs(5),
            info_timeout: Duration::from_secs(5),
            refresh_interval: Duration::from_secs(30),
            max_recv_message_size: i32::MAX as usize,
        }
    }
}

/// What an endpoint said about itself: the parts of `infopb.InfoResponse`
/// the proxy prunes stores by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointMetadata {
    /// `sidecar`, `store`, `receive`, `rule`, `query`.
    pub component_type: String,
    /// External label sets; a Query endpoint announces one per store
    /// behind it.
    pub label_sets: Vec<LabelSet>,
    pub min_time: i64,
    pub max_time: i64,
}

impl EndpointMetadata {
    /// From an `Info` reply; `None` when the endpoint has no Store API, a
    /// Ruler serving only rules, say.
    pub fn from_info(info: &InfoResponse) -> Option<Self> {
        let store = info.store.as_ref()?;
        Some(Self {
            component_type: info.component_type.clone(),
            label_sets: info
                .label_sets
                .iter()
                .map(LabelSet::from_zlabel_set)
                .collect(),
            min_time: store.min_time,
            max_time: store.max_time,
        })
    }

    /// Whether the store's range and `[min_time, max_time]` share a
    /// millisecond, both inclusive.
    pub fn overlaps(&self, min_time: i64, max_time: i64) -> bool {
        !(min_time > self.max_time || max_time < self.min_time)
    }
}

/// One endpoint: its address, a lazily connected channel shared by every
/// call, and the last metadata it reported.
#[derive(Debug)]
pub struct EndpointRef {
    addr: String,
    channel: Channel,
    metadata: RwLock<Option<EndpointMetadata>>,
    info_timeout: Duration,
    max_recv_message_size: usize,
}

impl EndpointRef {
    /// Dial `addr` lazily; nothing connects until the first call. Must run
    /// inside a tokio runtime, which the channel's reactor hooks into.
    pub fn new(addr: &str, cfg: &EndpointSetConfig) -> Result<Self, StoreError> {
        let endpoint = Endpoint::from_shared(dial_uri(addr)?)
            .map_err(|e| StoreError::Address {
                addr: addr.to_string(),
                reason: e.to_string(),
            })?
            .connect_timeout(cfg.connect_timeout)
            .http2_keep_alive_interval(cfg.keepalive_interval)
            .keep_alive_timeout(cfg.keepalive_timeout)
            .keep_alive_while_idle(true);
        Ok(Self {
            addr: addr.to_string(),
            channel: endpoint.connect_lazy(),
            metadata: RwLock::new(None),
            info_timeout: cfg.info_timeout,
            max_recv_message_size: cfg.max_recv_message_size,
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn store_client(&self) -> StoreClient<Channel> {
        StoreClient::new(self.channel.clone()).max_decoding_message_size(self.max_recv_message_size)
    }

    pub fn info_client(&self) -> InfoClient<Channel> {
        InfoClient::new(self.channel.clone())
    }

    /// The last metadata the endpoint reported, `None` until the first
    /// successful `Info`.
    pub fn metadata(&self) -> Option<EndpointMetadata> {
        self.metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replace the metadata by hand; tests build endpoints that never
    /// answer `Info`.
    pub fn set_metadata(&self, metadata: Option<EndpointMetadata>) {
        *self
            .metadata
            .write()
            .unwrap_or_else(PoisonError::into_inner) = metadata;
    }

    /// Ask the endpoint who it is. On failure the last metadata stays,
    /// like Go keeps a store around until it has been unhealthy a while.
    pub async fn refresh(&self) -> Result<(), StoreError> {
        let mut client = self.info_client();
        let response = tokio::time::timeout(self.info_timeout, client.info(InfoRequest {}))
            .await
            .map_err(|_| {
                StoreError::rpc(
                    "fetch info from",
                    &self.addr,
                    tonic::Status::deadline_exceeded(format!(
                        "no reply within {:?}",
                        self.info_timeout
                    )),
                )
            })?
            .map_err(|status| StoreError::rpc("fetch info from", &self.addr, status))?;
        self.set_metadata(EndpointMetadata::from_info(response.get_ref()));
        Ok(())
    }
}

/// `host:port` as Thanos's `--endpoint` takes it, or a full `http://`
/// URI. TLS is not supported yet, so `https://` is refused here instead
/// of failing on the first call.
fn dial_uri(addr: &str) -> Result<String, StoreError> {
    let refuse = |reason: &str| StoreError::Address {
        addr: addr.to_string(),
        reason: reason.to_string(),
    };
    if addr.is_empty() {
        return Err(refuse("empty address"));
    }
    if addr.starts_with("http://") {
        return Ok(addr.to_string());
    }
    if addr.starts_with("https://") {
        return Err(refuse("TLS is not supported yet"));
    }
    if addr.contains("://") {
        return Err(refuse(
            "unsupported scheme; use host:port or http://host:port",
        ));
    }
    Ok(format!("http://{addr}"))
}

/// Every configured endpoint.
#[derive(Debug)]
pub struct EndpointSet {
    endpoints: Vec<Arc<EndpointRef>>,
    refresh_interval: Duration,
}

impl EndpointSet {
    /// Dial each address once; repeated addresses count once.
    pub fn new(addrs: &[String], cfg: &EndpointSetConfig) -> Result<Self, StoreError> {
        let mut endpoints: Vec<Arc<EndpointRef>> = Vec::with_capacity(addrs.len());
        for addr in addrs {
            if endpoints.iter().any(|e| e.addr() == addr) {
                continue;
            }
            endpoints.push(Arc::new(EndpointRef::new(addr, cfg)?));
        }
        Ok(Self {
            endpoints,
            refresh_interval: cfg.refresh_interval,
        })
    }

    pub fn endpoints(&self) -> &[Arc<EndpointRef>] {
        &self.endpoints
    }

    /// Refresh every endpoint's metadata at once. A failure is logged and
    /// leaves that endpoint's last metadata in place.
    pub async fn update(&self) {
        let mut tasks = JoinSet::new();
        for endpoint in &self.endpoints {
            let endpoint = Arc::clone(endpoint);
            tasks.spawn(async move {
                let result = endpoint.refresh().await;
                (endpoint, result)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((endpoint, Ok(()))) => match endpoint.metadata() {
                    Some(m) => tracing::debug!(
                        addr = endpoint.addr(),
                        component = m.component_type,
                        min_time = m.min_time,
                        max_time = m.max_time,
                        label_sets = m.label_sets.len(),
                        "refreshed endpoint metadata"
                    ),
                    None => tracing::warn!(
                        addr = endpoint.addr(),
                        "endpoint has no Store API and will not be queried"
                    ),
                },
                Ok((endpoint, Err(err))) => tracing::warn!(
                    addr = endpoint.addr(),
                    error = %err,
                    "could not refresh endpoint metadata"
                ),
                Err(err) => tracing::error!(error = %err, "endpoint refresh task failed"),
            }
        }
    }

    /// Refresh on a ticker, forever; the first tick is one interval away
    /// because the caller has just run [`Self::update`].
    pub fn spawn_refresh(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.refresh_interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                self.update().await;
            }
        })
    }

    /// `storeMatches` for every endpoint: those with metadata whose time
    /// range overlaps the query's, whose address passes `store_matchers`,
    /// and whose external label sets do not contradict `matchers`.
    pub fn endpoints_for(
        &self,
        min_time: i64,
        max_time: i64,
        matchers: &[CompiledMatcher],
        store_matchers: &[Vec<CompiledMatcher>],
    ) -> Vec<Arc<EndpointRef>> {
        self.endpoints
            .iter()
            .filter(|e| {
                let Some(metadata) = e.metadata() else {
                    tracing::debug!(addr = e.addr(), "skipping endpoint without metadata");
                    return false;
                };
                if !metadata.overlaps(min_time, max_time) {
                    tracing::debug!(
                        addr = e.addr(),
                        "skipping endpoint: no data within [{min_time}, {max_time}], store has [{}, {}]",
                        metadata.min_time,
                        metadata.max_time
                    );
                    return false;
                }
                if !matchers_match_address(store_matchers, e.addr()) {
                    tracing::debug!(addr = e.addr(), "skipping endpoint: __address__ does not match storeMatch[]");
                    return false;
                }
                if !label_sets_match(matchers, &metadata.label_sets) {
                    tracing::debug!(addr = e.addr(), "skipping endpoint: external labels contradict the matchers");
                    return false;
                }
                true
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use promql_parser::ast::{LabelMatcher, MatchOp};
    use promql_parser::posrange::PositionRange;

    use super::*;

    fn compiled(name: &str, op: MatchOp, value: &str) -> CompiledMatcher {
        CompiledMatcher::compile(&LabelMatcher {
            name: name.to_string(),
            op,
            value: value.to_string(),
            pos_range: PositionRange::default(),
        })
        .unwrap()
    }

    fn metadata(cluster: &str, min_time: i64, max_time: i64) -> EndpointMetadata {
        EndpointMetadata {
            component_type: "sidecar".into(),
            label_sets: vec![LabelSet::from_strs(&[("cluster", cluster)])],
            min_time,
            max_time,
        }
    }

    #[test]
    fn dial_uris() {
        assert_eq!(
            dial_uri("localhost:10901").unwrap(),
            "http://localhost:10901"
        );
        assert_eq!(dial_uri("http://s:1").unwrap(), "http://s:1");
        assert!(dial_uri("").is_err());
        assert!(dial_uri("https://s:1")
            .unwrap_err()
            .to_string()
            .contains("TLS"));
        assert!(dial_uri("dns:///s:1").is_err());
    }

    #[test]
    fn metadata_from_info_needs_a_store() {
        use crate::storepb::thanos::info::{InfoResponse, StoreInfo};
        use crate::storepb::thanos::{Label, ZLabelSet};

        let without = InfoResponse {
            component_type: "rule".into(),
            ..Default::default()
        };
        assert_eq!(EndpointMetadata::from_info(&without), None);

        let with = InfoResponse {
            component_type: "sidecar".into(),
            label_sets: vec![ZLabelSet {
                labels: vec![Label {
                    name: "replica".into(),
                    value: "a".into(),
                }],
            }],
            store: Some(StoreInfo {
                min_time: 5,
                max_time: 10,
                ..Default::default()
            }),
            ..Default::default()
        };
        let got = EndpointMetadata::from_info(&with).unwrap();
        assert_eq!(got.component_type, "sidecar");
        assert_eq!(
            got.label_sets,
            vec![LabelSet::from_strs(&[("replica", "a")])]
        );
        assert!(got.overlaps(10, 20), "inclusive at both ends");
        assert!(got.overlaps(0, 5));
        assert!(!got.overlaps(11, 20));
        assert!(!got.overlaps(0, 4));
    }

    // Lazy channels still register with tokio's reactor, hence the runtime.
    #[tokio::test]
    async fn endpoints_for_prunes_by_range_labels_and_address() {
        let cfg = EndpointSetConfig::default();
        let set = EndpointSet::new(
            &["a:1".into(), "b:1".into(), "c:1".into(), "a:1".into()],
            &cfg,
        )
        .unwrap();
        assert_eq!(set.endpoints().len(), 3, "repeated addresses count once");
        set.endpoints()[0].set_metadata(Some(metadata("east", 0, 100)));
        set.endpoints()[1].set_metadata(Some(metadata("west", 50, 200)));
        // c never answered Info.

        let addrs = |eps: Vec<Arc<EndpointRef>>| -> Vec<String> {
            eps.iter().map(|e| e.addr().to_string()).collect()
        };
        let up = compiled("__name__", MatchOp::Equal, "up");

        assert_eq!(
            addrs(set.endpoints_for(0, 300, std::slice::from_ref(&up), &[])),
            ["a:1", "b:1"]
        );
        assert_eq!(
            addrs(set.endpoints_for(150, 300, std::slice::from_ref(&up), &[])),
            ["b:1"]
        );
        assert_eq!(
            addrs(set.endpoints_for(100, 100, std::slice::from_ref(&up), &[])),
            ["a:1", "b:1"]
        );
        assert!(set
            .endpoints_for(201, 300, std::slice::from_ref(&up), &[])
            .is_empty());

        let east = compiled("cluster", MatchOp::Equal, "east");
        assert_eq!(
            addrs(set.endpoints_for(0, 300, &[up.clone(), east], &[])),
            ["a:1"]
        );

        let only_b = vec![compiled("__address__", MatchOp::Equal, "b:1")];
        assert_eq!(addrs(set.endpoints_for(0, 300, &[up], &[only_b])), ["b:1"]);
    }
}
