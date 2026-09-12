//! What can go wrong between the engine and the stores.

use promql_engine::EngineError;

use crate::chunkenc::ChunkError;
use crate::labels::LabelSet;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// An `--endpoint` value that cannot be dialed.
    #[error("endpoint {addr:?}: {reason}")]
    Address { addr: String, reason: String },
    /// Not one endpoint has reported a Store API and partial responses
    /// are not allowed; Go's `ErrorNoStoresAvailable`.
    #[error("No StoreAPIs available")]
    NoStoresAvailable,
    /// A matcher the engine itself rejects, an invalid regex.
    #[error("{0}")]
    Matcher(EngineError),
    /// A gRPC call failed or timed out. `rpc` names the call the way the
    /// Go proxy phrases its warnings: "receive series from", "fetch label
    /// names from store", "fetch label values from store".
    #[error("{rpc} {addr}: {:?}: {}", .status.code(), .status.message())]
    Rpc {
        rpc: &'static str,
        addr: String,
        #[source]
        status: tonic::Status,
    },
    /// A store sent a warning frame while partial responses are not
    /// allowed.
    #[error("store {addr}: {warning}")]
    Aborted { addr: String, warning: String },
    /// A chunk that does not decode.
    #[error("decode chunk of {labels} from store {addr}: {source}")]
    Chunk {
        addr: String,
        labels: LabelSet,
        #[source]
        source: ChunkError,
    },
    /// Samples the engine's `Series` refuses, which after merging can only
    /// mean a bug on one side.
    #[error("build series {labels}: {message}")]
    Series { labels: LabelSet, message: String },
    /// A fan-out task died; a bug, not a store problem.
    #[error("{0}")]
    Internal(String),
}

impl StoreError {
    /// A named constructor instead of `From<tonic::Status>`, so every
    /// call site says which RPC and which store failed.
    pub fn rpc(rpc: &'static str, addr: &str, status: tonic::Status) -> Self {
        Self::Rpc {
            rpc,
            addr: addr.to_string(),
            status,
        }
    }
}
