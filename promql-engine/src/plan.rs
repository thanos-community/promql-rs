//! From a parsed expression to a DataFusion [`LogicalPlan`].
//!
//! The planner walks the expression tree once, bottom up. Every node
//! yields a plan in the canonical schema plus the label names that schema
//! carries — the next node up needs those to decide its own labels.
//! Whatever is not planned is [`EngineError::Unsupported`], **named**: the
//! differential suite counts those by feature, so a precise name is how
//! progress becomes a number rather than "red".
//!
//! Every node is stock DataFusion with one of this crate's functions in it:
//!
//! ```text
//! sum by (pod) (rate(http_requests_total[5m]))
//!
//! Projection: promql_labels('pod', pod) AS labels, samples
//!   Aggregate: groupBy=[CAST(get_field(labels, 'pod') AS Utf8) AS pod],
//!              aggr=[promql_aggregate(samples, 'sum') AS samples]
//!     Projection: promql_labels(…without __name__) AS labels,
//!                 promql_range_function(samples, 'rate', …) AS samples
//!       TableScan: selector_0          ← SelectorTable over SeriesSource::select
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{col, LogicalPlan, LogicalPlanBuilder};
use promql_parser::ast::{AggregateExpr, AtModifier, Call, Expr, VectorSelector};

use crate::aggregate::{self, Op};
use crate::error::EngineError;
use crate::instant::{self, Params};
use crate::labels;
use crate::matcher::{effective_matchers, METRIC_NAME};
use crate::range::{self, Func};
use crate::series::{LABELS, SAMPLES};
use crate::source::{Grouping, SelectHints, SelectorTable, SeriesSource};

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
    let mut planner = Planner {
        state,
        source,
        query,
        selectors: 0,
    };
    Ok(planner.expr(expr, None).await?.plan)
}

/// A planned subexpression: the plan and the label names in its schema.
struct Planned {
    plan: LogicalPlan,
    label_names: Vec<String>,
}

struct Planner<'a> {
    state: &'a dyn Session,
    source: &'a dyn SeriesSource,
    query: &'a RangeQuery,
    /// Selectors seen so far; each becomes its own table `selector_N`.
    selectors: usize,
}

type Planning<'f> = Pin<Box<dyn Future<Output = Result<Planned, EngineError>> + Send + 'f>>;

impl Planner<'_> {
    /// Plan one node. `grouping` is the aggregation directly above, handed
    /// down to the selector as a hint for the store.
    fn expr<'f>(&'f mut self, expr: &'f Expr, grouping: Option<&'f Grouping>) -> Planning<'f> {
        Box::pin(async move {
            match expr {
                Expr::VectorSelector(vs) => self.selector(vs, grouping).await,
                Expr::Paren(p) => self.expr(&p.expr, grouping).await,
                Expr::Aggregate(a) => self.aggregate(a).await,
                Expr::Call(c) => self.call(c, grouping).await,
                other => Err(EngineError::Unsupported(describe(other))),
            }
        })
    }

    /// Ask the store for `vs` over `[start_ms, end_ms]` and start a plan
    /// on the resulting table.
    async fn scan(
        &mut self,
        vs: &VectorSelector,
        (start_ms, end_ms): (i64, i64),
        grouping: Option<&Grouping>,
    ) -> Result<(LogicalPlanBuilder, Vec<String>), EngineError> {
        reject_unsupported_modifiers(vs)?;
        let table = SelectorTable::try_new(
            self.state,
            self.source,
            &effective_matchers(vs),
            SelectHints {
                start_ms,
                end_ms,
                grouping: grouping.cloned(),
            },
        )
        .await?;
        let label_names = table.label_names();
        let index = self.selectors;
        self.selectors += 1;
        let builder = LogicalPlanBuilder::scan(
            format!("selector_{index}"),
            provider_as_source(Arc::new(table)),
            None,
        )?;
        Ok((builder, label_names))
    }

    async fn selector(
        &mut self,
        vs: &VectorSelector,
        grouping: Option<&Grouping>,
    ) -> Result<Planned, EngineError> {
        let params = Params {
            start_ms: self.query.start_ms,
            end_ms: self.query.end_ms,
            step_ms: self.query.step_ms,
            lookback_ms: self.query.lookback_ms,
            offset_ms: offset_ms(vs),
            at_ms: resolve_at(vs, self.query),
        };
        let (builder, label_names) = self.scan(vs, params.select_range(), grouping).await?;
        let plan = builder
            .project(vec![
                col(LABELS),
                instant::call(col(SAMPLES), &params).alias(SAMPLES),
            ])?
            .build()?;
        Ok(Planned { plan, label_names })
    }

    /// A function of one range selector: `rate(x[5m])` and its family.
    async fn call(
        &mut self,
        call: &Call,
        grouping: Option<&Grouping>,
    ) -> Result<Planned, EngineError> {
        let name = call.func.name.as_str();
        let func = Func::parse(name)
            .ok_or_else(|| EngineError::Unsupported(format!("the {name} function")))?;
        let (ms, vs) = match call.args.as_slice() {
            [Expr::MatrixSelector(ms)] => match ms.vector_selector.as_ref() {
                Expr::VectorSelector(vs) => (ms, vs),
                other => {
                    return Err(EngineError::Unsupported(format!(
                        "a range selector over {}",
                        describe(other)
                    )))
                }
            },
            [Expr::Subquery(_)] => return Err(EngineError::Unsupported("a subquery".into())),
            [other] => {
                return Err(EngineError::Unsupported(format!(
                    "the {name} function over {}",
                    describe(other)
                )))
            }
            _ => {
                return Err(EngineError::Query(format!(
                    "{name} expects exactly one argument, got {}",
                    call.args.len()
                )))
            }
        };
        if ms.range_expr.is_some() {
            return Err(EngineError::Unsupported(
                "a range given as a duration expression".into(),
            ));
        }

        let params = range::Params {
            start_ms: self.query.start_ms,
            end_ms: self.query.end_ms,
            step_ms: self.query.step_ms,
            range_ms: (ms.range_secs * 1000.0).round() as i64,
            offset_ms: offset_ms(vs),
            at_ms: resolve_at(vs, self.query),
        };
        let (builder, input_names) = self.scan(vs, params.select_range(), grouping).await?;
        let (labels_expr, label_names) = if func.drops_metric_name() {
            labels::keep(&input_names, |n| n != METRIC_NAME)
        } else {
            (col(LABELS), input_names)
        };
        let plan = builder
            .project(vec![
                labels_expr.alias(LABELS),
                range::call(col(SAMPLES), func, &params).alias(SAMPLES),
            ])?
            .build()?;
        Ok(Planned { plan, label_names })
    }

    async fn aggregate(&mut self, agg: &AggregateExpr) -> Result<Planned, EngineError> {
        let op = Op::parse(&agg.op.to_string())
            .ok_or_else(|| EngineError::Unsupported(format!("the {} aggregation", agg.op)))?;
        if agg.param.is_some() {
            return Err(EngineError::Unsupported(format!(
                "the {} aggregation with a parameter",
                agg.op
            )));
        }
        let grouping = Grouping {
            labels: agg.grouping.clone(),
            without: agg.without,
        };
        let input = self.expr(&agg.expr, Some(&grouping)).await?;

        let keys = labels::group_keys(&input.label_names, &agg.grouping, agg.without);
        let plan = LogicalPlanBuilder::from(input.plan)
            .aggregate(
                labels::group_exprs(&keys),
                vec![aggregate::call(col(SAMPLES), op).alias(SAMPLES)],
            )?
            .project(vec![labels::regroup(&keys).alias(LABELS), col(SAMPLES)])?
            .build()?;
        Ok(Planned {
            plan,
            label_names: keys,
        })
    }
}

fn offset_ms(vs: &VectorSelector) -> i64 {
    (vs.original_offset_secs * 1000.0).round() as i64
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
/// yet". Aggregations and functions are named individually so the count
/// per feature is a roadmap.
pub fn describe(expr: &Expr) -> String {
    match expr {
        Expr::Aggregate(a) => format!("the {} aggregation", a.op),
        Expr::Call(c) => format!("the {} function", c.func.name),
        Expr::Binary(_) => "a binary operator".into(),
        Expr::MatrixSelector(_) => "a range selector".into(),
        Expr::Subquery(_) => "a subquery".into(),
        Expr::NumberLiteral(_) | Expr::Duration(_) => "a scalar literal".into(),
        Expr::StringLiteral(_) => "a string literal".into(),
        Expr::Paren(_) => "a parenthesized expression".into(),
        Expr::Unary(_) => "a unary operator".into(),
        Expr::StepInvariant(_) => "a step-invariant expression".into(),
        Expr::VectorSelector(_) => "a vector selector".into(),
    }
}
