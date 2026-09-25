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
//! it may only re-encode. The two things applied before the batches come
//! back are [`series::drop_empty`] — see its doc for why that can't live
//! in the plan — and [`labelset::reject_same_labelset`], which is
//! Prometheus's own post-evaluation pass.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::error::EngineError;
use crate::labelset;
pub use crate::plan::RangeQuery;
use crate::series;
use crate::source::SeriesSource;
use crate::{aggregate, labels, range, selector};

pub struct Engine {
    ctx: SessionContext,
    rt: Option<tokio::runtime::Runtime>,
}

impl Engine {
    /// An engine for async callers. Use the `*_async` methods.
    pub fn new() -> Self {
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        ctx.register_udaf(selector::udaf());
        ctx.register_udf(labels::udf());
        ctx.register_udaf(aggregate::udaf());
        ctx.register_udaf(range::udaf());
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

    /// The optimized plan a range query runs, for a caller that wants to
    /// stream it directly instead of paying for [`Self::range_query_async`]'s
    /// buffering; the memory bench is the one such caller.
    pub async fn physical_plan_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Arc<dyn ExecutionPlan>, EngineError> {
        let plan = self.plan_async(source, query, range).await?;
        Ok(self
            .ctx
            .execute_logical_plan(plan)
            .await?
            .create_physical_plan()
            .await?)
    }

    /// Evaluate a range query. Batches are in the canonical schema
    /// ([`crate::series`]) with empty series already dropped, series in
    /// Prometheus's label-set order; call
    /// [`series::decode`] to get [`Series`](crate::Series) instead.
    pub async fn range_query_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let plan = self.plan_async(source, query, range).await?;
        let df = self.ctx.execute_logical_plan(plan).await?;
        let batches = df.collect().await?;
        // One `collect()` shares a single output schema across all its
        // batches, so validating it once and comparing later batches by
        // pointer skips the redundant re-walk of the label fields.
        let mut validated = None;
        let batches: Vec<RecordBatch> = batches
            .iter()
            .map(|b| {
                let schema = b.schema();
                if !validated.as_ref().is_some_and(|s| Arc::ptr_eq(s, &schema)) {
                    series::validate(&schema).map_err(EngineError::Schema)?;
                }
                validated = Some(schema);
                Ok(series::drop_empty(b))
            })
            .collect::<Result<_, EngineError>>()?;
        labelset::reject_same_labelset(&batches)?;
        labelset::sort_by_labelset(&batches)
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

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}
