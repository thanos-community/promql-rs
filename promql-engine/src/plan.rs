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
//! Projection: promql_labels('pod', __group__pod) AS labels, samples
//!   Aggregate: groupBy=[get_field(labels, 'pod') AS __group__pod],
//!              aggr=[promql_aggregate(samples, 'sum') AS samples]
//!     Projection: promql_labels(…without __name__) AS labels,
//!                 promql_range_function(samples, 'rate', …) AS samples
//!       TableScan: selector_0          ← SelectorTable over SeriesSource::select
//! ```
//!
//! The `__group__` prefix on a group key keeps a label named `labels`
//! or `samples` from colliding with the engine's own columns; see
//! [`labels::group_exprs`]. [`labels::regroup`] restores the bare name.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use datafusion::catalog::Session;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{col, LogicalPlan, LogicalPlanBuilder};
use promql_parser::ast::{AggregateExpr, AtModifier, Call, Expr, MatrixSelector, VectorSelector};

use crate::aggregate::{self, Op};
use crate::error::EngineError;
use crate::labels;
use crate::matcher::{effective_matchers, METRIC_NAME};
use crate::params::{step_count, Params};
use crate::range::{self, Func};
use crate::selector;
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
    pub const DEFAULT_LOOKBACK: Duration = Duration::from_secs(5 * 60);

    pub fn new(start_ms: i64, end_ms: i64, step_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            step_ms,
            lookback_ms: millis(Self::DEFAULT_LOOKBACK),
        }
    }
}

/// Timestamps stay i64 milliseconds (Prometheus convention); this is the
/// one point where a `Duration` constant crosses into that world.
fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).expect("duration exceeds i64 millis")
}

pub async fn plan(
    state: &dyn Session,
    source: &dyn SeriesSource,
    expr: &Expr,
    query: &RangeQuery,
) -> Result<LogicalPlan, EngineError> {
    check_range(query)?;
    let mut planner = Planner::new(state, source, query);
    Ok(planner.expr(expr, Above::default()).await?.plan)
}

/// [`plan`], for a query whose range is a single instant.
///
/// The one thing it plans that [`plan`] cannot is a range selector as
/// the whole query, `x[5m]`, which is a matrix result. Upstream draws
/// the same line in the evaluator rather than the planner: a matrix
/// selector panics unless `startTimestamp == endTimestamp`
/// (`promql/engine.go:2366-2370` at 83962c35), and a range query is
/// rejected before that for having a type other than vector or scalar.
pub async fn plan_instant(
    state: &dyn Session,
    source: &dyn SeriesSource,
    expr: &Expr,
    query: &RangeQuery,
) -> Result<LogicalPlan, EngineError> {
    check_range(query)?;
    let mut planner = Planner::new(state, source, query);
    match top_level_matrix(expr) {
        Some(ms) => Ok(planner.matrix_selector(ms).await?.plan),
        None => Ok(planner.expr(expr, Above::default()).await?.plan),
    }
}

/// The range selector a query *is*, looking through the wrappers that
/// only pass a value along. Everything else that holds one — `rate`'s
/// argument — consumes it itself and never reaches here.
fn top_level_matrix(expr: &Expr) -> Option<&MatrixSelector> {
    match expr {
        Expr::MatrixSelector(ms) => Some(ms),
        Expr::Paren(p) => top_level_matrix(&p.expr),
        Expr::StepInvariant(e) => top_level_matrix(e),
        _ => None,
    }
}

fn check_range(query: &RangeQuery) -> Result<(), EngineError> {
    if query.step_ms <= 0 {
        return Err(EngineError::Query(format!(
            "step must be positive, got {}ms",
            query.step_ms
        )));
    }
    // `aggregate::Grid` checks the same constant; this half exists so
    // the user gets a query error naming the limit.
    let steps = step_count(query.start_ms, query.end_ms, query.step_ms);
    if steps > aggregate::MAX_STEPS as i128 {
        return Err(EngineError::Query(format!(
            "{}..{} every {}ms is {steps} steps, more than the {} this engine allows",
            query.start_ms,
            query.end_ms,
            query.step_ms,
            aggregate::MAX_STEPS
        )));
    }
    Ok(())
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

/// What stands directly above a selector, which is all the store gets
/// told about the query beyond its matchers and its range.
#[derive(Debug, Clone, Copy, Default)]
struct Above<'a> {
    /// The enclosing function or aggregation's PromQL name.
    func: Option<&'a str>,
    grouping: Option<&'a Grouping>,
}

impl<'a> Planner<'a> {
    fn new(state: &'a dyn Session, source: &'a dyn SeriesSource, query: &'a RangeQuery) -> Self {
        Self {
            state,
            source,
            query,
            selectors: 0,
        }
    }

    /// Plan one node. `above` is what the store will be told sits over
    /// the selector this subtree bottoms out in.
    fn expr<'f>(&'f mut self, expr: &'f Expr, above: Above<'f>) -> Planning<'f> {
        Box::pin(async move {
            match expr {
                Expr::VectorSelector(vs) => self.selector(vs, above).await,
                Expr::Paren(p) => self.expr(&p.expr, above).await,
                Expr::Aggregate(a) => self.aggregate(a).await,
                Expr::Call(c) => self.call(c).await,
                other => Err(EngineError::Unsupported(describe(other))),
            }
        })
    }

    /// The store's hints for one selector: the scan range it must honour
    /// and whatever else may let it read less.
    ///
    /// `range_ms` is the `[5m]` of a range selector, absent for an
    /// instant one. `step_ms` is always the step the caller passed: this
    /// engine has one entry point, so a query whose bounds happen to be
    /// equal is a one-step range query, not an instant one, and guessing
    /// otherwise would withhold a grid the store can still align to.
    fn hints(
        &self,
        (start_ms, end_ms): (i64, i64),
        range_ms: Option<i64>,
        above: Above,
    ) -> SelectHints {
        SelectHints {
            start_ms,
            end_ms,
            step_ms: Some(self.query.step_ms),
            range_ms,
            func: above.func.map(str::to_string),
            grouping: above.grouping.cloned(),
            shard: None,
        }
    }

    /// Ask the store for `vs` under `hints` and start a plan on the
    /// resulting table.
    async fn scan(
        &mut self,
        vs: &VectorSelector,
        hints: SelectHints,
    ) -> Result<(LogicalPlanBuilder, Vec<String>), EngineError> {
        reject_unsupported_modifiers(vs)?;
        let table =
            SelectorTable::try_new(self.state, self.source, &effective_matchers(vs), hints).await?;
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
        above: Above<'_>,
    ) -> Result<Planned, EngineError> {
        let params = Params {
            start_ms: self.query.start_ms,
            end_ms: self.query.end_ms,
            step_ms: self.query.step_ms,
            window_ms: self.query.lookback_ms,
            offset_ms: offset_ms(vs),
            at_ms: resolve_at(vs, self.query),
        };
        check_selector_bounds(params.at_ms, params.offset_ms)?;
        let hints = self.hints(params.select_range(), None, above);
        let (builder, label_names) = self.scan(vs, hints).await?;
        let plan = builder
            .project(vec![
                col(LABELS),
                selector::call(col(SAMPLES), &params).alias(SAMPLES),
            ])?
            .build()?;
        Ok(Planned { plan, label_names })
    }

    /// A range selector as the whole query: the samples themselves, as
    /// upstream's `matrixSelector` hands them back.
    ///
    /// No kernel sits over the samples, and that is the point: the
    /// window *is* the scan range the store was asked for, strict lower
    /// bound and all (`Params::select_range`, mirroring
    /// `matrixIterSlice`'s `mint < t <= maxt`), and a store owes the
    /// engine only the samples inside the range it was given. The
    /// timestamps stay the samples' own — upstream does not move them
    /// for an `offset` either.
    async fn matrix_selector(&mut self, ms: &MatrixSelector) -> Result<Planned, EngineError> {
        let Expr::VectorSelector(vs) = ms.vector_selector.as_ref() else {
            return Err(EngineError::Unsupported(format!(
                "a range selector over {}",
                describe(&ms.vector_selector)
            )));
        };
        if ms.range_expr.is_some() {
            return Err(EngineError::Unsupported(
                "a range given as a duration expression".into(),
            ));
        }

        let params = Params {
            start_ms: self.query.start_ms,
            end_ms: self.query.end_ms,
            step_ms: self.query.step_ms,
            window_ms: (ms.range_secs * 1000.0).round() as i64,
            offset_ms: offset_ms(vs),
            at_ms: resolve_at(vs, self.query),
        };
        check_selector_bounds(params.at_ms, params.offset_ms)?;
        check_time_bound("the range", params.window_ms)?;
        let hints = self.hints(
            params.select_range(),
            Some(params.window_ms),
            Above::default(),
        );
        let (builder, label_names) = self.scan(vs, hints).await?;
        Ok(Planned {
            plan: builder.build()?,
            label_names,
        })
    }

    /// A function of one range selector: `rate(x[5m])` and its family.
    ///
    /// Takes no `Above`: this call is itself what stands directly over the
    /// selector, so nothing higher reaches the store.
    async fn call(&mut self, call: &Call) -> Result<Planned, EngineError> {
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

        let params = Params {
            start_ms: self.query.start_ms,
            end_ms: self.query.end_ms,
            step_ms: self.query.step_ms,
            window_ms: (ms.range_secs * 1000.0).round() as i64,
            offset_ms: offset_ms(vs),
            at_ms: resolve_at(vs, self.query),
        };
        check_selector_bounds(params.at_ms, params.offset_ms)?;
        check_time_bound("the range", params.window_ms)?;
        // The store hears about this function, not the aggregation over
        // it: `rate` is what decides which samples it may skip, and the
        // grouping above it no longer describes the selector's parent, so
        // it is dropped rather than threaded through.
        let hints = self.hints(
            params.select_range(),
            Some(params.window_ms),
            Above {
                func: Some(func.as_str()),
                grouping: None,
            },
        );
        let (builder, input_names) = self.scan(vs, hints).await?;
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
        let op = Op::from_token(agg.op)
            .ok_or_else(|| EngineError::Unsupported(format!("the {} aggregation", agg.op)))?;
        if agg.param.is_some() {
            return Err(EngineError::Unsupported(format!(
                "the {} aggregation with a parameter",
                agg.op
            )));
        }
        // `without` names what to drop, so it says nothing about which
        // labels a store may stop reading; only `by` does, and upstream's
        // `extractGroupsFromPath` hints on that alone.
        let grouping = (!agg.without).then(|| Grouping {
            labels: agg.grouping.clone(),
            by: true,
        });
        let input = self
            .expr(
                &agg.expr,
                Above {
                    func: Some(op.as_str()),
                    grouping: grouping.as_ref(),
                },
            )
            .await?;

        let keys = labels::group_keys(&input.label_names, &agg.grouping, agg.without);
        let plan = LogicalPlanBuilder::from(input.plan)
            .aggregate(
                labels::group_exprs(&keys),
                vec![aggregate::call(
                    col(SAMPLES),
                    op,
                    self.query.start_ms,
                    self.query.end_ms,
                    self.query.step_ms,
                )
                .alias(SAMPLES)],
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

/// The widest `@` timestamp, offset or range this engine will plan.
///
/// Three of these terms meet in one `i64` where `select_range` subtracts
/// the window and the offset, so the bound is a quarter of the range.
/// Prometheus catches an out-of-bounds `@` while parsing (`setTimestamp`
/// in `parser.go`); ours saturates the float instead.
const MAX_TIME_MS: i64 = i64::MAX / 4;

/// One term of the scan-range arithmetic, named for the message.
fn check_time_bound(what: &str, ms: i64) -> Result<(), EngineError> {
    if (-MAX_TIME_MS..=MAX_TIME_MS).contains(&ms) {
        return Ok(());
    }
    Err(EngineError::Query(format!(
        "{what} {ms}ms is out of range, the limit is {MAX_TIME_MS}ms"
    )))
}

/// The `@` and the offset of one selector, rejected here rather than in
/// the kernel: `select_range` and the kernels below it saturate rather
/// than panic, which would answer an absurd query with an empty result.
fn check_selector_bounds(at_ms: Option<i64>, offset_ms: i64) -> Result<(), EngineError> {
    if let Some(at) = at_ms {
        check_time_bound("the @ modifier timestamp", at)?;
    }
    check_time_bound("the offset", offset_ms)
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use promql_parser::ast::{Expr, LabelMatcher};

    use super::*;
    use crate::memory::MemorySeriesSource;

    /// A store that keeps what it was asked, so a test can read the
    /// hints the planner built.
    #[derive(Debug, Default)]
    struct Recorder {
        inner: MemorySeriesSource,
        seen: Mutex<Vec<SelectHints>>,
    }

    #[async_trait]
    impl SeriesSource for Recorder {
        async fn select(
            &self,
            state: &dyn Session,
            matchers: &[LabelMatcher],
            hints: SelectHints,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            self.seen.lock().unwrap().push(hints.clone());
            self.inner.select(state, matchers, hints).await
        }
    }

    /// The hints one query's selectors reach the store with.
    async fn hints_of(query: &str, range: RangeQuery) -> Vec<SelectHints> {
        let ctx = SessionContext::new();
        let source = Recorder::default();
        let expr: Expr = promql_parser::parse_expr(query).expect("query parses");
        plan(&ctx.state(), &source, &expr, &range)
            .await
            .expect("query plans");
        source.seen.into_inner().unwrap()
    }

    #[tokio::test]
    async fn the_store_hears_the_step_the_window_and_what_is_above_the_selector() {
        let range = RangeQuery::new(0, 600_000, 30_000);

        let hints = hints_of("sum by (pod)(rate(x[5m]))", range).await;
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].step_ms, Some(30_000));
        assert_eq!(hints[0].range_ms, Some(300_000));
        // `rate` stands between the aggregation and the selector, so it
        // is the function the store is told about, and the grouping two
        // levels up does not reach it.
        assert_eq!(hints[0].func.as_deref(), Some("rate"));
        assert_eq!(hints[0].grouping, None);

        // A plain selector has no window and nothing above it.
        let hints = hints_of("x", range).await;
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].step_ms, Some(30_000));
        assert_eq!(hints[0].range_ms, None);
        assert_eq!(hints[0].func, None);
        assert_eq!(hints[0].grouping, None);

        // Directly under a `by` aggregation, that is what it hears.
        let hints = hints_of("sum by (pod)(x)", range).await;
        assert_eq!(hints[0].func.as_deref(), Some("sum"));
        assert_eq!(
            hints[0].grouping,
            Some(Grouping {
                labels: vec!["pod".to_string()],
                by: true,
            })
        );

        // `without` names what to drop, which is no help to a store.
        let hints = hints_of("count without (pod)(x)", range).await;
        assert_eq!(hints[0].func.as_deref(), Some("count"));
        assert_eq!(hints[0].grouping, None);

        // Equal bounds are a one-step range query, not an instant one:
        // the step is still the grid the caller asked for.
        let one_step = RangeQuery::new(600_000, 600_000, 30_000);
        let hints = hints_of("x", one_step).await;
        assert_eq!(hints[0].step_ms, Some(30_000));
    }

    /// Plan `query` against an empty store: every guard here runs before
    /// the store is asked for anything, and each node carries its own
    /// functions, so none need registering.
    async fn plan_of(query: &str, range: RangeQuery) -> Result<LogicalPlan, EngineError> {
        let ctx = SessionContext::new();
        let source = MemorySeriesSource::default();
        let expr: Expr = promql_parser::parse_expr(query).expect("query parses");
        plan(&ctx.state(), &source, &expr, &range).await
    }

    #[tokio::test]
    async fn a_query_with_more_steps_than_the_cap_is_a_query_error() {
        let range = RangeQuery::new(0, 1000 * 24 * 60 * 60 * 1000, 1);
        let err = plan_of("up", range).await.unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
        let message = err.to_string();
        assert!(
            message.contains(&aggregate::MAX_STEPS.to_string()),
            "the message names the limit: {message}"
        );
    }

    #[tokio::test]
    async fn a_query_at_the_step_cap_is_planned() {
        let end = aggregate::MAX_STEPS as i64 - 1;
        assert!(plan_of("up", RangeQuery::new(0, end, 1)).await.is_ok());
        assert!(plan_of("up", RangeQuery::new(0, end + 1, 1)).await.is_err());
    }

    #[tokio::test]
    async fn an_at_modifier_out_of_range_is_a_query_error() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        let err = plan_of("up @ -1e30", range).await.unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
        assert!(err.to_string().contains("@ modifier"), "{err}");

        let err = plan_of("rate(up[5m] @ 1e30)", range).await.unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
        assert!(err.to_string().contains("@ modifier"), "{err}");
    }

    #[tokio::test]
    async fn an_at_modifier_at_a_real_timestamp_is_planned() {
        let range = RangeQuery::new(1_600_000_000_000, 1_600_000_060_000, 30_000);
        assert!(plan_of("up @ 1600000000", range).await.is_ok());
        assert!(plan_of("up @ start()", range).await.is_ok());
        assert!(plan_of("up offset 5m", range).await.is_ok());
    }

    #[test]
    fn a_term_the_scan_range_cannot_hold_is_rejected() {
        assert!(check_selector_bounds(None, 0).is_ok());
        assert!(check_selector_bounds(Some(0), 0).is_ok());
        assert!(check_selector_bounds(Some(MAX_TIME_MS), -MAX_TIME_MS).is_ok());

        assert!(check_selector_bounds(Some(i64::MIN), 0).is_err());
        assert!(check_selector_bounds(Some(i64::MAX), 0).is_err());
        assert!(check_selector_bounds(None, i64::MAX).is_err());
        assert!(check_time_bound("the range", i64::MAX).is_err());

        let message = check_selector_bounds(Some(i64::MIN), 0)
            .unwrap_err()
            .to_string();
        assert!(message.contains("@ modifier timestamp"), "{message}");
        assert!(message.contains(&MAX_TIME_MS.to_string()), "{message}");
    }

    #[tokio::test]
    async fn every_supported_aggregation_plans() {
        let range = RangeQuery::new(0, 60_000, 30_000);
        for op in [
            "sum", "avg", "count", "min", "max", "group", "stddev", "stdvar",
        ] {
            let query = format!("{op} by (pod) (up)");
            assert!(plan_of(&query, range).await.is_ok(), "{query}");
        }
        for op in [
            "topk",
            "bottomk",
            "quantile",
            "count_values",
            "limitk",
            "limit_ratio",
        ] {
            let query = format!("{op}(1, up)");
            let err = plan_of(&query, range).await.unwrap_err();
            assert!(matches!(err, EngineError::Unsupported(_)), "{query}: {err}");
            assert!(err.to_string().contains(op), "{query}: {err}");
        }
    }

    #[tokio::test]
    async fn a_step_that_is_not_positive_is_a_query_error() {
        let err = plan_of("up", RangeQuery::new(0, 60_000, 0))
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
    }
}
