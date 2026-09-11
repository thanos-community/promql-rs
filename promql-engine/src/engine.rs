//! The engine: parse, plan, execute, decode.
//!
//! Owns a DataFusion [`SessionContext`] with the PromQL functions
//! registered. A DataFusion query is async end to end; callers that are
//! not — the conformance harness runs plain closures — construct the
//! engine with [`Engine::blocking`], which adds a Tokio runtime to block
//! on. An engine built with [`Engine::new`] has none and is safe to hold
//! and drop inside someone else's runtime.

use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::error::EngineError;
pub use crate::plan::RangeQuery;
use crate::series::{self, DecodedSeries};
use crate::source::SeriesSource;
use crate::{aggregate, instant, labels};

pub struct Engine {
    ctx: SessionContext,
    rt: Option<tokio::runtime::Runtime>,
}

impl Engine {
    /// An engine for async callers. Use the `*_async` methods.
    pub fn new() -> Self {
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        ctx.register_udf(instant::udf());
        ctx.register_udf(labels::udf());
        ctx.register_udaf(aggregate::udaf());
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

    pub fn context(&self) -> &SessionContext {
        &self.ctx
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

    /// Evaluate a range query.
    pub async fn range_query_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Vec<DecodedSeries>, EngineError> {
        let plan = self.plan_async(source, query, range).await?;
        let df = self.ctx.execute_logical_plan(plan).await?;
        let batches = df.collect().await?;
        series::decode(&batches).map_err(EngineError::Schema)
    }

    /// [`Self::range_query_async`], blocking on the engine's own runtime.
    pub fn range_query(
        &self,
        source: Arc<dyn SeriesSource>,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Vec<DecodedSeries>, EngineError> {
        self.runtime()?
            .block_on(self.range_query_async(source.as_ref(), query, range))
    }

    /// [`Self::plan_async`], blocking on the engine's own runtime.
    pub fn plan(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<LogicalPlan, EngineError> {
        self.runtime()?
            .block_on(self.plan_async(source, query, range))
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
