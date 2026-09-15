//! Every way a selection fails before, during, or after DataFusion.
//!
//! The variants are kept apart because callers treat them differently:
//! [`EngineError::Query`] is an answer (the reference engine also rejects
//! some queries), [`EngineError::Schema`] is a store's bug, and
//! [`EngineError::DataFusion`] is DataFusion's or this crate's.

use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
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
}
