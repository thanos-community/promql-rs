//! Every way a query fails before, during, or after DataFusion.
//!
//! The variants are kept apart because callers treat them differently: a
//! differential test counts [`EngineError::Unsupported`] as "not built
//! yet" and collapses it, treats [`EngineError::Query`] as an answer (the
//! reference engine also rejects some queries), and treats everything else
//! as a bug in this crate.

use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The expression parsed but uses something this engine does not
    /// implement yet. Named so that progress is countable per feature.
    #[error("{0} is not supported yet")]
    Unsupported(String),

    /// The query itself is invalid — a parse error, or a semantic error
    /// Prometheus would also report. This is a result, not a failure.
    #[error("{0}")]
    Query(String),

    /// A store handed back something other than the canonical series
    /// schema. Reported at plan time, before anything executes.
    #[error("series source schema: {0}")]
    Schema(String),

    /// DataFusion refused or failed the plan.
    #[error("datafusion: {0}")]
    DataFusion(#[from] DataFusionError),

    /// A blocking call on an engine without a runtime, or a runtime that
    /// could not be built.
    #[error("runtime: {0}")]
    Runtime(String),
}
