//! The engine: parse, plan, execute, decode.
//!
//! Owns a DataFusion [`SessionContext`] with the PromQL functions
//! registered. A DataFusion query is async end to end; callers that are
//! not — the conformance harness runs plain closures — construct the
//! engine with [`Engine::blocking`], which adds a Tokio runtime to block
//! on. An engine built with [`Engine::new`] has none and is safe to hold
//! and drop inside someone else's runtime.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::error::EngineError;
pub use crate::plan::RangeQuery;
use crate::series::{self, Series};
use crate::source::SeriesSource;
use crate::{aggregate, labels, range, selector};

pub struct Engine {
    ctx: SessionContext,
    rt: Option<tokio::runtime::Runtime>,
}

impl Engine {
    /// An engine for async callers. Use the `*_async` methods.
    pub fn new() -> Self {
        Self::with_extension_planners(Vec::new())
    }

    /// An engine whose physical planner also knows the caller's own
    /// [`LogicalPlan::Extension`] nodes. A caller that puts a step of its
    /// own into a plan from [`Self::plan_async`] — a Thanos querier's
    /// replica deduplication, say; `docs/series-source.md` describes the
    /// contract — hands the [`ExtensionPlanner`] for it over here and runs
    /// the plan with [`Self::execute_async`]. DataFusion turns an extension
    /// node into an operator through no other route.
    pub fn with_extension_planners(planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>) -> Self {
        // `SessionContext::new_with_config` is this without the planner.
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features()
            .with_query_planner(Arc::new(ExtensionQueryPlanner { planners }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(selector::udf());
        ctx.register_udf(labels::udf());
        ctx.register_udaf(aggregate::udaf());
        ctx.register_udf(range::udf());
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
    ) -> Result<Vec<Series>, EngineError> {
        let plan = self.plan_async(source, query, range).await?;
        self.execute_async(plan).await
    }

    /// Run a plan from [`Self::plan_async`], as it came or with the
    /// caller's own steps in it, and decode the series.
    pub async fn execute_async(&self, plan: LogicalPlan) -> Result<Vec<Series>, EngineError> {
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
    ) -> Result<Vec<Series>, EngineError> {
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

/// DataFusion's default physical planner plus the extension planners the
/// engine was built with; the session's `QueryPlanner`.
struct ExtensionQueryPlanner {
    planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl fmt::Debug for ExtensionQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtensionQueryPlanner")
            .field("planners", &self.planners.len())
            .finish()
    }
}

#[async_trait]
impl QueryPlanner for ExtensionQueryPlanner {
    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
        state: &SessionState,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(self.planners.clone())
            .create_physical_plan(plan, state)
            .await
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}
