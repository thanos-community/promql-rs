//! The engine: parse, plan, execute.
//!
//! Owns a DataFusion [`SessionContext`] with the PromQL functions
//! registered. A DataFusion query is async end to end; callers that are
//! not — the conformance harness runs plain closures — construct the
//! engine with [`Engine::blocking`], which adds a Tokio runtime to block
//! on. An engine built with [`Engine::new`] has none and is safe to hold
//! and drop inside someone else's runtime.
//!
//! An instant query is the same evaluation over a range of one step,
//! which is how Prometheus runs one too; what it adds is reading the
//! result as the type the *expression* has. See [`instant_query_async`]
//! and [`crate::value_type`].
//!
//! [`instant_query_async`]: Engine::instant_query_async
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
use crate::value_type::{value_type, ValueType};
use crate::{aggregate, labels, range, selector};

/// The four things an instant query can answer with, Prometheus's
/// `promql.Value`. Which one it is follows from the expression's type,
/// never from the result: see [`crate::value_type`].
#[derive(Debug, Clone)]
pub enum InstantResult {
    Scalar(f64),
    String(String),
    /// One row per series in the [instant-vector
    /// shape](crate::series::vector_schema); read it with
    /// [`series::decode_vector`].
    Vector(RecordBatch),
    /// A top-level range selector: the canonical series shape with the
    /// store's own samples, ordered by label set as upstream orders a
    /// `Matrix` result. A `Vec` to match what a range query hands back,
    /// though the ordering leaves exactly one batch in it.
    Matrix(Vec<RecordBatch>),
}

/// The step an instant query is evaluated on.
///
/// Upstream runs an instant query as a range evaluation of one step with
/// `interval: 1` (`promql/engine.go:791-796` at 83962c35), and so does
/// this. One millisecond rather than none because the grid arithmetic
/// divides by the step: [`crate::params::step_count`] would answer zero
/// steps and the planner rejects a non-positive step outright.
const INSTANT_STEP_MS: i64 = 1;

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
        self.execute(plan).await
    }

    /// Plan an instant query without running it, for inspection.
    pub async fn plan_instant_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        at_ms: i64,
    ) -> Result<LogicalPlan, EngineError> {
        let expr =
            promql_parser::parse_expr(query).map_err(|e| EngineError::Query(e.to_string()))?;
        crate::plan::plan_instant(&self.ctx.state(), source, &expr, &instant_range(at_ms)).await
    }

    /// Evaluate a query at one instant, as Prometheus's
    /// `NewInstantQuery` does: a range evaluation of a single step whose
    /// result is then read as the *expression's* type
    /// (`execEvalStmt`, `promql/engine.go:791-849` at 83962c35). The
    /// result cannot say which type it is — a scalar and `sum(x)` are
    /// both one unlabelled series with one point — so the expression
    /// decides, before anything is planned.
    pub async fn instant_query_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        at_ms: i64,
    ) -> Result<InstantResult, EngineError> {
        let expr =
            promql_parser::parse_expr(query).map_err(|e| EngineError::Query(e.to_string()))?;
        let kind = value_type(&expr);
        if matches!(kind, ValueType::Scalar | ValueType::String) {
            // A literal, `time()` or `scalar(x)`: none of them is
            // planned yet, and each would need a plan with no selector
            // under it rather than a reshape of one.
            return Err(EngineError::Unsupported(format!(
                "a {}-typed instant query",
                kind.as_str()
            )));
        }

        let range = instant_range(at_ms);
        let plan = crate::plan::plan_instant(&self.ctx.state(), source, &expr, &range).await?;
        let batches = self.execute(plan).await?;
        match kind {
            ValueType::Vector => Ok(InstantResult::Vector(
                series::to_vector(&batches, at_ms).map_err(EngineError::Schema)?,
            )),
            // One batch, not one per input batch: the order upstream
            // gives a `Matrix` is over the whole result, so it cannot
            // be applied to DataFusion's partitions one at a time.
            ValueType::Matrix => Ok(InstantResult::Matrix(vec![series::sort_by_labels(
                &batches,
            )
            .map_err(EngineError::Schema)?])),
            ValueType::Scalar | ValueType::String => unreachable!("returned above"),
        }
    }

    /// [`Self::instant_query_async`], blocking on the engine's own
    /// runtime.
    pub fn instant_query(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        at_ms: i64,
    ) -> Result<InstantResult, EngineError> {
        self.runtime()?
            .block_on(self.instant_query_async(source, query, at_ms))
    }

    /// Run a planned query and hand back canonical batches: the schema
    /// checked once, empty series dropped.
    async fn execute(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>, EngineError> {
        let df = self.ctx.execute_logical_plan(plan).await?;
        let batches = df.collect().await?;
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

/// The one-step range an instant query at `at_ms` is evaluated over.
fn instant_range(at_ms: i64) -> RangeQuery {
    RangeQuery::new(at_ms, at_ms, INSTANT_STEP_MS)
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::ExecutionPlan;
    use promql_parser::ast::LabelMatcher;

    use super::*;
    use crate::series::{encode, label_names_of, Series};

    /// A store that answers in two partitions, so the engine sees two
    /// batches. `MemorySeriesSource` always answers in one, which is
    /// exactly the case that cannot catch a per-batch ordering.
    #[derive(Debug)]
    struct Split(Vec<Series>);

    #[async_trait]
    impl SeriesSource for Split {
        async fn select(
            &self,
            _state: &dyn Session,
            _matchers: &[LabelMatcher],
            _hints: crate::source::SelectHints,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            let names = label_names_of(&self.0);
            let partitions: Vec<Vec<RecordBatch>> = self
                .0
                .iter()
                .map(|s| vec![encode(&names, std::slice::from_ref(s)).expect("one series")])
                .collect();
            let schema = partitions[0][0].schema();
            Ok(MemorySourceConfig::try_new_exec(&partitions, schema, None)?)
        }
    }

    /// The order upstream gives a `Matrix` is over the result, so a
    /// store that answers in several batches must still come back
    /// sorted end to end — not sorted within each batch.
    ///
    /// Which partition arrives first is DataFusion's to decide and
    /// varies run to run, which is the point: only a sort over the
    /// whole result is deterministic here.
    #[test]
    fn a_matrix_from_several_batches_is_sorted_end_to_end() {
        // Reverse order on the way in, one series per batch.
        let source = Split(vec![
            Series::new(&[("__name__", "m"), ("pod", "z")], vec![0], vec![1.0]).unwrap(),
            Series::new(&[("__name__", "m"), ("pod", "a")], vec![0], vec![2.0]).unwrap(),
        ]);

        let engine = Engine::blocking().unwrap();
        let InstantResult::Matrix(batches) = engine.instant_query(&source, "m[5m]", 0).unwrap()
        else {
            panic!("a range selector is matrix-typed")
        };
        let got: Vec<String> = series::decode(&batches)
            .unwrap()
            .iter()
            .map(|s| s.label("pod").to_string())
            .collect();
        assert_eq!(got, ["a", "z"]);
    }
}
