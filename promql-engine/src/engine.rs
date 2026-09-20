//! The engine: parse, plan, execute.
//!
//! Owns a DataFusion [`SessionContext`] with the PromQL functions
//! registered. A DataFusion query is async end to end; callers that are
//! not — the conformance harness runs plain closures — construct the
//! engine with [`Engine::blocking`], which adds a Tokio runtime to block
//! on. An engine built with [`Engine::new`] has none and is safe to hold
//! and drop inside someone else's runtime.
//!
//! A range query hands back Arrow batches in the canonical schema
//! ([`crate::series`]), not decoded [`Series`](crate::Series): this is a
//! DataFusion engine, and Arrow in, Arrow out lets a caller stay on
//! `RecordBatch` end to end instead of paying to materialize Rust values
//! it may only re-encode. The one thing applied before the batches come
//! back is [`series::drop_empty`] — see its doc for why that can't live
//! in the plan.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::error::EngineError;
pub use crate::plan::RangeQuery;
use crate::series;
use crate::source::SeriesSource;
use crate::{aggregate, histogram, labels, range, reduce, selector};

pub struct Engine {
    ctx: SessionContext,
    rt: Option<tokio::runtime::Runtime>,
}

impl Engine {
    /// An engine for async callers. Use the `*_async` methods.
    pub fn new() -> Self {
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        ctx.register_udf(selector::udf());
        ctx.register_udf(labels::udf());
        ctx.register_udaf(aggregate::udaf());
        ctx.register_udf(range::udf());
        ctx.register_udaf(reduce::udaf());
        ctx.register_udf(labels::replace_udf());
        ctx.register_udaf(histogram::udaf());
        Self { ctx, rt: None }
    }

    /// An engine with its own runtime, for synchronous callers. Must not
    /// be dropped from inside another Tokio runtime; Tokio forbids that.
    pub fn blocking() -> Result<Self, EngineError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Runtime(e.to_string()))?;
        Ok(Self {
            rt: Some(rt),
            ..Self::new()
        })
    }

    /// Plan a query without running it, for inspection.
    pub async fn plan_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<LogicalPlan, EngineError> {
        let expr =
            promql_parser::parse_expr(query).map_err(|e| EngineError::Query(e.to_string()))?;
        crate::plan::plan(&self.ctx.state(), source, &expr, range).await
    }

    /// Evaluate a range query. Batches are in the canonical schema
    /// ([`crate::series`]) with empty series already dropped; call
    /// [`series::decode`] to get [`Series`](crate::Series) instead.
    pub async fn range_query_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let plan = self.plan_async(source, query, range).await?;
        let df = self.ctx.execute_logical_plan(plan).await.map_err(lift)?;
        let batches = df.collect().await.map_err(lift)?;
        // One `collect()` shares a single output schema across all its
        // batches, so validating it once and comparing later batches by
        // pointer skips the redundant re-walk of the label fields.
        let mut validated = None;
        batches
            .iter()
            .map(|b| {
                let schema = b.schema();
                if !validated.as_ref().is_some_and(|s| Arc::ptr_eq(s, &schema)) {
                    series::validate(&schema).map_err(EngineError::Schema)?;
                }
                validated = Some(schema);
                Ok(series::drop_empty(b))
            })
            .collect()
    }

    /// [`Self::range_query_async`], blocking on the engine's own runtime.
    pub fn range_query(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        self.runtime()?
            .block_on(self.range_query_async(source, query, range))
    }

    fn runtime(&self) -> Result<&tokio::runtime::Runtime, EngineError> {
        self.rt.as_ref().ok_or_else(|| {
            EngineError::Runtime(
                "this engine has no runtime; build it with Engine::blocking() or use the *_async methods".into(),
            )
        })
    }
}

/// A PromQL error raised from inside an operator, lifted back out.
///
/// An [`Accumulator`](datafusion::logical_expr::Accumulator) can only
/// fail with a `DataFusionError`, so a failure that is the *query's*
/// fault — the colliding label sets a label function can produce, the
/// only one so far — arrives here wrapped, and a caller reading the
/// variants would call it the engine breaking rather than the answer
/// Prometheus gives too.
///
/// Recognized by its text, which is as unpleasant as it looks: the
/// message is the only part of the error DataFusion promises to carry
/// through an aggregate's execution unchanged, so a typed error would
/// have to be matched the same way at the other end.
fn lift(e: datafusion::error::DataFusionError) -> EngineError {
    if e.to_string().contains(reduce::SAME_LABELSET) {
        return EngineError::Query(reduce::SAME_LABELSET.to_string());
    }
    EngineError::DataFusion(e)
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}
