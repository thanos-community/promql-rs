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

    /// A store broke the order it promised: rows of a series not
    /// consecutive, series not label-sorted, or a series' first sample
    /// timestamps going backwards. Found while executing, so it reaches
    /// the caller through DataFusion, and is unwrapped from it again.
    #[error("series source order: {0}")]
    Source(String),

    /// DataFusion refused or failed the plan.
    #[error("datafusion: {0}")]
    DataFusion(DataFusionError),

    /// A blocking call on an engine without a runtime, or a runtime that
    /// could not be built.
    #[error("runtime: {0}")]
    Runtime(String),
}

impl From<DataFusionError> for EngineError {
    /// An operator can only fail with a `DataFusionError`, so an engine
    /// error raised inside one travels as `External` and is taken back out
    /// here; otherwise every such error would reach callers as an opaque
    /// `DataFusion` string.
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::External(inner) => match inner.downcast::<EngineError>() {
                Ok(engine) => *engine,
                Err(inner) => EngineError::DataFusion(DataFusionError::External(inner)),
            },
            DataFusionError::Context(ctx, inner) => match EngineError::from(*inner) {
                EngineError::DataFusion(inner) => {
                    EngineError::DataFusion(DataFusionError::Context(ctx, Box::new(inner)))
                }
                engine => engine,
            },
            e => EngineError::DataFusion(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store-order violation is raised inside an `ExecutionPlan`, where
    /// only a `DataFusionError` can be returned; callers must still be
    /// able to match on the variant.
    #[test]
    fn a_source_error_survives_the_trip_through_datafusion() {
        let wrapped =
            DataFusionError::External(Box::new(EngineError::Source("out of order".into())));
        assert!(
            matches!(EngineError::from(wrapped), EngineError::Source(m) if m == "out of order")
        );

        let in_context = DataFusionError::Context(
            "while collecting".into(),
            Box::new(DataFusionError::External(Box::new(EngineError::Source(
                "x".into(),
            )))),
        );
        assert!(matches!(
            EngineError::from(in_context),
            EngineError::Source(_)
        ));
    }

    #[test]
    fn any_other_datafusion_error_stays_one() {
        let e = EngineError::from(DataFusionError::Execution("boom".into()));
        assert!(matches!(e, EngineError::DataFusion(_)), "{e:?}");
    }
}
