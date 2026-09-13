//! Every way a query fails before, during, or after DataFusion.
//!
//! The variants are kept apart because callers treat them differently: a
//! differential test counts [`EngineError::Unsupported`] as "not built
//! yet" and collapses it, [`EngineError::Query`] is an answer (the
//! reference engine also rejects some queries), [`EngineError::Schema`] is
//! a store's bug, and everything else is DataFusion's or this crate's.

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
    DataFusion(DataFusionError),

    /// A blocking call on an engine without a runtime, or a runtime that
    /// could not be built.
    #[error("runtime: {0}")]
    Runtime(String),
}

/// A [`EngineError::Query`] raised from inside a DataFusion kernel.
///
/// A kernel can only fail with a [`DataFusionError`], but some of its
/// failures are Prometheus's own answers rather than bugs: "multiple
/// matches for labels" is what the reference engine tells the user, and
/// the differential suite has to count it as a match, not as a crash.
/// [`QueryError::raise`] wraps one so it survives the trip out through
/// DataFusion, and [`query_error`] recognises it on the way back.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct QueryError(pub String);

impl QueryError {
    /// This message, as a DataFusion error to return from a kernel.
    pub fn raise(message: impl Into<String>) -> DataFusionError {
        DataFusionError::External(Box::new(QueryError(message.into())))
    }
}

/// The [`QueryError`] inside a DataFusion error, if there is one.
///
/// The chain is walked rather than using `DataFusionError::find_root`,
/// which stops at the deepest *DataFusion* error and so never reaches
/// through `External` to the payload. Execution wraps errors in `Context`
/// and `ArrowError` on the way up, and all of those forward `source`.
pub fn query_error(e: &DataFusionError) -> Option<&QueryError> {
    let mut error: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(e) = error {
        if let Some(q) = e.downcast_ref::<QueryError>() {
            return Some(q);
        }
        error = e.source();
    }
    None
}

/// Every `?` on a DataFusion error passes through here, so a kernel's
/// [`QueryError`] becomes [`EngineError::Query`] wherever it surfaces —
/// planning, execution, or collection — without each call site knowing.
impl From<DataFusionError> for EngineError {
    fn from(e: DataFusionError) -> Self {
        match query_error(&e) {
            Some(q) => EngineError::Query(q.0.clone()),
            None => EngineError::DataFusion(e),
        }
    }
}
