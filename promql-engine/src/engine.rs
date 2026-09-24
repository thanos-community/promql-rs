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
//! in the plan — and `labelset::reject_same_labelset`, which is
//! Prometheus's own post-evaluation pass.
//!
//! Between planning and execution the physical plan goes through
//! [`check_selector_plans`]. The selector and range aggregates bound memory
//! only in DataFusion's Sorted mode, and DataFusion drops to Linear mode
//! without an error; the check turns that into a refused query.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{self, displayable, ExecutionPlan, InputOrderMode, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::error::EngineError;
use crate::labelset;
pub use crate::plan::RangeQuery;
use crate::series::{self, LABELS};
use crate::source::{SeriesSetExec, SeriesSource};
use crate::{aggregate, labels, range, selector};

pub struct Engine {
    ctx: SessionContext,
    rt: Option<tokio::runtime::Runtime>,
}

impl Engine {
    /// An engine for async callers. Use the `*_async` methods.
    pub fn new() -> Self {
        // A scan split by byte range cuts through a series. Round-robin is
        // off out of caution: with the selector SinglePartitioned over
        // Hash(labels) DataFusion only places it above the selector, over
        // finished series, but nothing but that requirement keeps it there.
        // Hash repartitioning for aggregations is what lets the selector run
        // SinglePartitioned per store partition. The price: the plan ends in
        // several partitions and series come back in arrival order, which
        // nothing in the plan can re-sort, since Arrow has no struct sort.
        let mut config = SessionConfig::new()
            .with_round_robin_repartition(false)
            .with_repartition_file_scans(false)
            .with_repartition_aggregations(true);
        // Below this many input partitions EnforceDistribution re-hashes a
        // satisfied Hash(labels) input anyway, to reach target_partitions,
        // which splits the selector into Partial and FinalPartitioned.
        config.options_mut().optimizer.subset_repartition_threshold = 1;
        let ctx = SessionContext::new_with_config(config);
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

    /// The optimized `ExecutionPlan` a range query runs, for inspection or
    /// for a caller that streams it instead of paying for
    /// [`Self::range_query_async`]'s buffering, as the memory bench does.
    /// It has passed [`check_selector_plans`], so it is exactly what
    /// [`Self::range_query_async`] executes.
    pub async fn physical_plan_async(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        range: &RangeQuery,
    ) -> Result<Arc<dyn ExecutionPlan>, EngineError> {
        let plan = self.plan_async(source, query, range).await?;
        let exec = self
            .ctx
            .execute_logical_plan(plan)
            .await?
            .create_physical_plan()
            .await?;
        check_selector_plans(&exec)?;
        Ok(exec)
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
        let exec = self.physical_plan_async(source, query, range).await?;
        let batches = physical_plan::collect(exec, self.ctx.task_ctx()).await?;
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

/// Refuse a plan in which a selector or range-function aggregate would not
/// hold exactly one open series per partition.
///
/// The session flags in [`Engine::new`] produce such plans today, but
/// nothing makes DataFusion keep doing so: an upgrade or a new optimizer
/// rule that loses `InputOrderMode::Sorted` does not fail, it buffers every
/// series of the scan and answers out of order. `EvalSeries`' contiguity
/// checks catch the rows that then arrive interleaved, but only for data
/// that happens to interleave, so the shape is checked here on every plan.
///
/// Accepted: a Partial, Single or SinglePartitioned aggregate, Sorted,
/// whose input reaches [`SeriesSetExec`] through nothing but projections
/// and cooperative yields; and a Final aggregate, Sorted, over such a Partial through an
/// order-preserving merge or hash repartition. Anything that deals rows
/// out anew below the Partial — round-robin, a hash split of chunk rows —
/// can put one series in two partitions, each of which folds half of it.
///
/// A sort on a `labels` column is refused wherever it sits: Arrow cannot
/// sort a struct, so the plan would otherwise fail inside the sort.
pub fn check_selector_plans(plan: &Arc<dyn ExecutionPlan>) -> Result<(), EngineError> {
    let refuse = |why: &str| {
        EngineError::Query(format!(
            "refusing a plan in which {why}:\n{}",
            displayable(plan.as_ref()).indent(true)
        ))
    };
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        stack.extend(node.children());
        if let Some(sort) = node.downcast_ref::<SortExec>() {
            if sort.expr().iter().any(|e| is_labels(e.expr.as_ref())) {
                return Err(refuse("a sort orders by labels"));
            }
            continue;
        }
        let Some(agg) = selector_aggregate(node) else {
            continue;
        };
        if agg.input_order_mode() != &InputOrderMode::Sorted {
            return Err(refuse("a selector aggregate does not run Sorted"));
        }
        let fed = match agg.mode() {
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned => {
                reaches(agg.input(), &|n| n.is::<SeriesSetExec>(), &|n| {
                    n.is::<ProjectionExec>() || n.is::<CooperativeExec>()
                })
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => reaches(
                agg.input(),
                &|n| selector_aggregate(n).is_some_and(|a| *a.mode() == AggregateMode::Partial),
                &|n| {
                    n.is::<ProjectionExec>()
                        || n.is::<CooperativeExec>()
                        || n.is::<SortPreservingMergeExec>()
                        || n.downcast_ref::<RepartitionExec>().is_some_and(|r| {
                            r.preserve_order() && matches!(r.partitioning(), Partitioning::Hash(..))
                        })
                },
            ),
            AggregateMode::PartialReduce => false,
        };
        if !fed {
            return Err(refuse(
                "a selector aggregate's input does not come straight from the store",
            ));
        }
    }
    Ok(())
}

fn selector_aggregate(node: &Arc<dyn ExecutionPlan>) -> Option<&AggregateExec> {
    node.downcast_ref::<AggregateExec>().filter(|agg| {
        agg.aggr_expr()
            .iter()
            .any(|e| [selector::NAME, range::NAME].contains(&e.fun().name()))
    })
}

fn is_labels(expr: &dyn datafusion::physical_expr::PhysicalExpr) -> bool {
    expr.downcast_ref::<Column>()
        .is_some_and(|c| c.name() == LABELS)
}

/// Whether `node`, or the single-child chain below it through `through`,
/// ends in a node that is `target`.
fn reaches(
    node: &Arc<dyn ExecutionPlan>,
    target: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool,
    through: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool,
) -> bool {
    let mut node = node;
    loop {
        if target(node) {
            return true;
        }
        match node.children().as_slice() {
            [child] if through(node) => node = child,
            _ => return false,
        }
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}
