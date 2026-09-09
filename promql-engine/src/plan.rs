//! From a parsed expression to a DataFusion [`LogicalPlan`].
//!
//! Only a bare instant-vector selector is planned today. Everything else
//! is [`EngineError::Unsupported`], **named**: the differential suite
//! counts those by feature, so a precise name is how progress becomes a
//! number rather than "red".
//!
//! For a selector the plan is two nodes, both stock:
//!
//! ```text
//! Projection: labels, promql_instant_vector(samples, …) AS samples
//!   TableScan: selector_0            ← SelectorTable over SeriesSource::select
//! ```

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{col, LogicalPlan, LogicalPlanBuilder};
use promql_parser::ast::{AtModifier, Expr, VectorSelector};

use crate::error::EngineError;
use crate::instant::{self, Params};
use crate::matcher::effective_matchers;
use crate::series::{LABELS, SAMPLES};
use crate::source::{SelectHints, SelectorTable, SeriesSource};

/// The range a query is evaluated over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeQuery {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    /// Prometheus's `--query.lookback-delta`, 5m by default.
    pub lookback_ms: i64,
}

impl RangeQuery {
    pub const DEFAULT_LOOKBACK_MS: i64 = 5 * 60 * 1000;

    pub fn new(start_ms: i64, end_ms: i64, step_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            step_ms,
            lookback_ms: Self::DEFAULT_LOOKBACK_MS,
        }
    }
}

/// Plan `expr` over `source` for `query`.
pub async fn plan(
    state: &dyn Session,
    source: &dyn SeriesSource,
    expr: &Expr,
    query: &RangeQuery,
) -> Result<LogicalPlan, EngineError> {
    if query.step_ms <= 0 {
        return Err(EngineError::Query(format!(
            "step must be positive, got {}ms",
            query.step_ms
        )));
    }
    match expr {
        Expr::VectorSelector(vs) => plan_selector(state, source, vs, query, 0).await,
        other => Err(EngineError::Unsupported(describe(other).to_string())),
    }
}

async fn plan_selector(
    state: &dyn Session,
    source: &dyn SeriesSource,
    vs: &VectorSelector,
    query: &RangeQuery,
    index: usize,
) -> Result<LogicalPlan, EngineError> {
    reject_unsupported_modifiers(vs)?;

    let params = Params {
        start_ms: query.start_ms,
        end_ms: query.end_ms,
        step_ms: query.step_ms,
        lookback_ms: query.lookback_ms,
        offset_ms: (vs.original_offset_secs * 1000.0).round() as i64,
        at_ms: resolve_at(vs, query),
    };
    let (start_ms, end_ms) = params.select_range();

    let table = SelectorTable::try_new(
        state,
        source,
        &effective_matchers(vs),
        SelectHints { start_ms, end_ms },
    )
    .await?;

    let plan = LogicalPlanBuilder::scan(
        format!("selector_{index}"),
        provider_as_source(Arc::new(table)),
        None,
    )?
    .project(vec![
        col(LABELS),
        instant::call(col(SAMPLES), &params).alias(SAMPLES),
    ])?
    .build()?;
    Ok(plan)
}

/// The `@` modifier's timestamp, with `start()`/`end()` resolved against
/// the query. Upstream does this in `preprocessExpr`; our parser leaves it
/// to the engine.
fn resolve_at(vs: &VectorSelector, query: &RangeQuery) -> Option<i64> {
    match (vs.timestamp, vs.start_or_end) {
        (Some(ts), _) => Some(ts),
        (None, Some(AtModifier::Start)) => Some(query.start_ms),
        (None, Some(AtModifier::End)) => Some(query.end_ms),
        (None, None) => None,
    }
}

fn reject_unsupported_modifiers(vs: &VectorSelector) -> Result<(), EngineError> {
    if vs.original_offset_expr.is_some() {
        return Err(EngineError::Unsupported(
            "an offset given as a duration expression".into(),
        ));
    }
    if vs.anchored || vs.smoothed {
        return Err(EngineError::Unsupported(
            "the anchored and smoothed modifiers".into(),
        ));
    }
    if vs.skip_histogram_buckets {
        return Err(EngineError::Unsupported(
            "the __ignore_histogram_buckets__ modifier".into(),
        ));
    }
    Ok(())
}

/// A name for what an expression is, for the "not supported yet" message.
/// Singular noun phrases, because the message is "{this} is not supported
/// yet".
pub fn describe(expr: &Expr) -> &'static str {
    match expr {
        Expr::Aggregate(_) => "an aggregation",
        Expr::Binary(_) => "a binary operator",
        Expr::Call(_) => "a function call",
        Expr::MatrixSelector(_) => "a range selector",
        Expr::Subquery(_) => "a subquery",
        Expr::NumberLiteral(_) | Expr::Duration(_) => "a scalar literal",
        Expr::StringLiteral(_) => "a string literal",
        Expr::Paren(_) => "a parenthesized expression",
        Expr::Unary(_) => "a unary operator",
        Expr::StepInvariant(_) => "a step-invariant expression",
        Expr::VectorSelector(_) => "a vector selector",
    }
}
