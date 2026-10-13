//! The planner's output shape, pinned as insta snapshots in `plan/snapshots/`.
//!
//! Every other suite here checks numbers, which a planner change can keep
//! right while quietly moving work between nodes: a projection that stops
//! pruning labels, an aggregation that loses its grouping expression, a
//! literal that stops being folded in. Rendering the `LogicalPlan` and
//! comparing it to an expected text makes that visible as a reviewable
//! diff instead. The rendering is `promql_engine::explain_plan`, not
//! `LogicalPlan::display_indent()`: see `src/explain.rs` for why a plan
//! pin needs its own reader-facing text and what makes that text safe to
//! trust.
//!
//! The snapshots are descriptive, not normative: a deliberate planner
//! change re-blesses them with `cargo insta review` (or `cargo insta
//! accept`, or `INSTA_UPDATE=always cargo test`), in a commit of its own
//! whose message says why the shape moved, and that diff is the review.
//! The corpus is meant to grow one query at a time as shapes are added.
//!
//! # One module per upstream topic
//!
//! Each module under `plan/` is named after the promqltest file in
//! `promql-conformance/testdata/prometheus/` whose topic it covers, so a
//! shape sits next to the evaluations that check its numbers. A module's
//! `RANGE` is the query range its cases plan with, so the literals in a
//! plan never differ within a module unless a case passes bounds of its
//! own, for a shape that only exists at other bounds.
//!
//! Modules are code, so none can be left out of a run the way a data file
//! could. What can go stale is the other direction, a `.snap` file whose
//! test was renamed or deleted: `cargo insta test --unreferenced=reject`
//! names those.
//!
//! # Adding a case
//!
//! Add a test to the module for its topic, or start a module named after
//! the upstream `.test` file, with a doc comment saying what shape it pins,
//! the series the query runs over, and an `assert_snapshot!` named after
//! the test whose third argument is the query, so the `.snap` header shows
//! it. Run `cargo insta test --review`: a case without a snapshot fails and
//! the review prints the rendered plan. Read it before accepting. The
//! point is to notice a shape you did not expect, not to record one.
//!
//! [`Case::physical`], asserted as a second snapshot named `<test>_physical`,
//! also pins the `ExecutionPlan`, rendered by
//! `promql_engine::explain_physical_plan`: the calls read as in the logical
//! plan, and every column printed keeps the `@index` the node evaluates.
//! Only small cases can carry one: partition counts and repartitioning make
//! larger physical plans too fragile to hold as text.
//!
//! [`Case::chunked`] and [`Case::partitions`] run a case over
//! [`MemorySeriesSource::chunked`] and [`MemorySeriesSource::partitions`],
//! the store shapes the Sorted selector exists for.
//!
//! # Why the series are per case
//!
//! A projection over the source names the label fields one by one, and
//! those names come from the source's data. So the label set is part of
//! the shape being pinned, and a case that shares someone else's fixture
//! would pin someone else's schema. Only the label set reaches the
//! planner, so one sample per series is enough and a second adds
//! nothing; [`Case::source`] rejects any series carrying more, so the
//! corpus cannot drift into looking like a data-driven evaluation suite.
//! A chunked case is the exception: there the samples decide how many
//! batches the store hands over, which the physical plan shows.
//!
//! # Why no Filter appears
//!
//! Matchers are the store's to apply. `SeriesSource::select` takes them,
//! so they never become a plan node, and two queries differing only in
//! their matchers render the same text.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::{lit, Column, UnKnownColumn};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::{displayable, ExecutionPlan, InputOrderMode, Partitioning};
use promql_engine::check_selector_plans;
use promql_engine::series::{encode, label_names_of, Block};
use promql_engine::{
    range, selector, Engine, EngineError, EngineOptions, MemorySeriesSource, RangeQuery,
    SelectHints, Series, SeriesSource,
};
use promql_parser::ast::LabelMatcher;
use std::num::{NonZeroU64, NonZeroUsize};

// A test crate's root resolves `mod` against `tests/`, where each file is
// a test binary of its own and `functions.rs` already is one.
#[path = "plan/aggregators.rs"]
mod aggregators;
#[path = "plan/at_modifier.rs"]
mod at_modifier;
#[path = "plan/functions.rs"]
mod functions;
#[path = "plan/operators.rs"]
mod operators;
#[path = "plan/range_queries.rs"]
mod range_queries;
#[path = "plan/selectors.rs"]
mod selectors;

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("a count of at least one")
}

fn nz64(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("a span of at least a millisecond")
}

#[derive(Debug, Clone, Copy)]
struct Bounds {
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
}

/// The series a pinned query plans over, and the store shape that hands
/// them over.
#[derive(Debug)]
struct Case {
    range: Bounds,
    load: &'static [&'static str],
    chunked_ms: Option<i64>,
    blocks_ms: Option<u64>,
    partitions: Option<usize>,
    instant: bool,
}

impl Case {
    fn new(range: Bounds, load: &'static [&'static str]) -> Self {
        Case {
            range,
            load,
            chunked_ms: None,
            blocks_ms: None,
            partitions: None,
            instant: false,
        }
    }

    /// [`MemorySeriesSource::chunked`]: the store hands each series over
    /// as chunks of at most this span, which only the physical plan shows.
    fn chunked(mut self, ms: i64) -> Self {
        self.chunked_ms = Some(ms);
        self
    }

    /// [`MemorySeriesSource::blocks`].
    fn blocks(mut self, ms: u64) -> Self {
        self.blocks_ms = Some(ms);
        self
    }

    /// [`MemorySeriesSource::partitions`].
    fn partitions(mut self, n: usize) -> Self {
        self.partitions = Some(n);
        self
    }

    /// Plan as an instant query at the range's start, the only way to
    /// plan a range selector, which a range query refuses.
    fn instant(mut self) -> Self {
        self.instant = true;
        self
    }

    fn source(&self) -> MemorySeriesSource {
        let series: Vec<_> = self
            .load
            .iter()
            .map(|l| {
                let desc = promql_parser::parse_series_desc(l).expect("series line parses");
                // Chunking is the one thing that turns the sample count into
                // plan shape, as the number of batches the store hands over.
                assert!(
                    desc.values.len() == 1 || self.chunked_ms.is_some() || self.blocks_ms.is_some(),
                    "series {l:?} has {} samples; an unchunked plan case carries exactly one, \
                     because nothing but the label set reaches the planner",
                    desc.values.len(),
                );
                desc
            })
            .collect();
        // The interval only spaces samples out, and no sample is ever read.
        let mut source = MemorySeriesSource::from_descriptions(&series, 30.0);
        // One chunk per batch: the cases show a label set crossing batches
        // as the batch count under SeriesSetExec.
        if let Some(ms) = self.chunked_ms {
            source = source.chunked(ms).rows_per_batch(nz(1));
        }
        if let Some(ms) = self.blocks_ms {
            source = source.blocks(nz64(ms));
        }
        if let Some(n) = self.partitions {
            source = source.partitions(nz(n));
        }
        source
    }

    fn range_query(&self) -> RangeQuery {
        RangeQuery::new(self.range.start_ms, self.range.end_ms, self.range.step_ms)
    }

    /// The `LogicalPlan`, rendered by `promql_engine::explain_plan`.
    fn plan(&self, query: &str) -> String {
        let source = self.source();
        let engine = engine();
        let plan = runtime()
            .block_on(async {
                if self.instant {
                    engine
                        .plan_instant_async(&source, query, self.range.start_ms)
                        .await
                } else {
                    engine.plan_async(&source, query, &self.range_query()).await
                }
            })
            .expect("the query plans");
        promql_engine::explain_plan(&plan)
    }

    /// The `ExecutionPlan`, for shapes the logical plan cannot show: the
    /// store-order wrapper and the physical operators DataFusion picks.
    /// Pinned only where the text does not depend on the machine.
    fn physical(&self, query: &str) -> String {
        assert!(
            !self.instant,
            "the engine lowers only the plan a range query runs"
        );
        let source = self.source();
        // The engine's own physical plan, not one lowered here, so the text is
        // what the engine runs.
        let exec = runtime()
            .block_on(engine().physical_plan_async(&source, query, &self.range_query()))
            .expect("the plan lowers");
        let rendered = promql_engine::explain_physical_plan(exec.as_ref());
        // A shuffle above the selector is as wide as the session's
        // target_partitions, DataFusion's default of one per core.
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        rendered.replace(
            &format!("], {cores}), input_partitions="),
            "], target_partitions), input_partitions=",
        )
    }
}

/// Every gate is open: a pin is about the plan a query gets, and the
/// corpus spans the experimental surface (fill in `operators`, and
/// whatever the next topic adds). Whether stock options refuse that
/// syntax is `tests/feature_flags.rs`'s question.
///
/// Not `Engine::blocking`: an engine that owns a runtime cannot be
/// dropped from inside one, and the cases drive [`runtime`]'s.
fn engine() -> Engine {
    Engine::with_options(EngineOptions {
        parser: promql_parser::ParserOptions::all(),
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

// The physical plan decides what the logical plan cannot: whether the
// selector aggregate holds one open series per partition (Sorted) or every
// series of the scan (Linear). The rest of this file checks that choice
// directly rather than as text, because partition counts and the upper
// aggregate's split make the larger plans too fragile to pin.

/// Every selector and range-function aggregate in `plan`, Partial and Final
/// alike.
fn selector_aggregates(plan: &Arc<dyn ExecutionPlan>) -> Vec<&AggregateExec> {
    let mut found = Vec::new();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if let Some(agg) = node.downcast_ref::<AggregateExec>() {
            if agg
                .aggr_expr()
                .iter()
                .any(|e| [selector::NAME, range::NAME].contains(&e.fun().name()))
            {
                found.push(agg);
            }
        }
        stack.extend(node.children());
    }
    found
}

fn two_counters() -> Vec<promql_parser::SeriesDescription> {
    [r#"x{pod="a"} 1+1x19"#, r#"x{pod="b"} 2+3x19"#]
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

fn physical_plan(source: &dyn SeriesSource, query: &str) -> Arc<dyn ExecutionPlan> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(Engine::new().physical_plan_async(
        source,
        query,
        &RangeQuery::new(300_000, 600_000, 30_000),
    ))
    .unwrap_or_else(|e| panic!("{query}: {e}"))
}

#[test]
fn the_selector_aggregate_runs_sorted_at_one_and_four_partitions() {
    for n in [1, 4] {
        let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0)
            .chunked(150_000)
            .partitions(nz(n));
        for query in ["x", "rate(x[5m])", "sum by (pod) (rate(x[5m]))"] {
            let plan = physical_plan(&source, query);
            let shown = displayable(plan.as_ref()).indent(true).to_string();
            let aggregates = selector_aggregates(&plan);
            assert!(
                !aggregates.is_empty(),
                "{query} at {n} partitions has no selector aggregate:\n{shown}"
            );
            for agg in aggregates {
                assert_eq!(
                    agg.input_order_mode(),
                    &InputOrderMode::Sorted,
                    "{query} at {n} partitions, {:?} aggregate:\n{shown}",
                    agg.mode(),
                );
            }
        }
    }
}

/// `plan` with `wrap` applied to the input of every selector aggregate,
/// the aggregate rebuilt over it so it re-derives its input order.
fn under_the_selector(
    plan: Arc<dyn ExecutionPlan>,
    wrap: impl Fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    plan.transform_up(|node| {
        let is_selector = node.downcast_ref::<AggregateExec>().is_some_and(|agg| {
            agg.aggr_expr()
                .iter()
                .any(|e| e.fun().name() == selector::NAME)
        });
        if !is_selector {
            return Ok(Transformed::no(node));
        }
        let input = wrap(Arc::clone(node.children()[0]));
        Ok(Transformed::yes(node.with_new_children(vec![input])?))
    })
    .expect("the plan rebuilds")
    .data
}

#[test]
fn the_engine_accepts_its_own_plans() {
    for n in [1, 4] {
        let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0).partitions(nz(n));
        for query in ["x", "rate(x[5m])", "sum(x)"] {
            let plan = physical_plan(&source, query);
            check_selector_plans(&plan).unwrap_or_else(|e| {
                panic!(
                    "{query} at {n} partitions: {e}\n{}",
                    displayable(plan.as_ref()).indent(true)
                )
            });
        }
    }
}

#[test]
fn a_repartition_under_the_selector_is_refused() {
    let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0);
    let plan = under_the_selector(physical_plan(&source, "x"), |input| {
        Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(4)).unwrap())
    });
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    match check_selector_plans(&plan) {
        Err(EngineError::Query(_)) => {}
        other => panic!("expected the plan refused, got {other:?}:\n{shown}"),
    }
}

/// `sort_to_indices` has no Struct arm, so this plan would fail inside
/// Arrow at execution; refusing it names the cause instead.
#[test]
fn a_sort_on_labels_under_the_selector_is_refused() {
    let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0);
    let plan = under_the_selector(physical_plan(&source, "x"), |input| {
        let labels = Column::new_with_schema("labels", &input.schema()).unwrap();
        let ordering = LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(labels))]).unwrap();
        Arc::new(SortExec::new(ordering, input))
    });
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    match check_selector_plans(&plan) {
        Err(EngineError::Query(_)) => {}
        other => panic!("expected the plan refused, got {other:?}:\n{shown}"),
    }
}

/// A store emitting Prometheus's `labels.Compare` order rather than struct
/// order: `{a="1"}` before `{b="1"}`, where struct order compares the `a`
/// column first and puts `("", "1")` ahead of `("1", "")`.
#[derive(Debug)]
struct PrometheusOrdered;

#[async_trait]
impl SeriesSource for PrometheusOrdered {
    async fn select(
        &self,
        _state: &dyn Session,
        _matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let series = vec![
            Series::new(&[("__name__", "x"), ("a", "1")], vec![300_000], vec![1.0]).unwrap(),
            Series::new(&[("__name__", "x"), ("b", "1")], vec![300_000], vec![2.0]).unwrap(),
        ];
        let block = Block {
            start_ms: hints.start_ms + hints.window_ms,
            end_ms: hints.end_ms + 1,
        };
        let batch = encode(&label_names_of(&series), &series, block).unwrap();
        let schema = batch.schema();
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            None,
        )?)
    }
}

#[test]
fn prometheus_ordered_input_is_rejected() {
    let engine = Engine::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let result = rt.block_on(engine.range_query_async(
        &PrometheusOrdered,
        "x",
        &RangeQuery::new(300_000, 300_000, 30_000),
    ));
    match result {
        Err(EngineError::Source(_)) => {}
        other => panic!("expected a source order error, got {other:?}"),
    }
}

/// Hash(labels) from SeriesSetExec must not survive the projection that
/// computes the group key above the range function: the key is a new
/// expression, and an upper aggregate that believed its input already
/// hash-partitioned by it would skip the shuffle and answer a group once
/// per store partition.
#[test]
fn the_label_projection_drops_the_hash_partitioning() {
    let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0)
        .chunked(150_000)
        .partitions(nz(4));
    let plan = physical_plan(&source, "sum by (pod) (rate(x[5m]))");
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    // The ProjectionExec that feeds the upper Partial, right above the
    // range function's aggregate.
    let mut node = &plan;
    while !node.children()[0]
        .downcast_ref::<AggregateExec>()
        .is_some_and(|agg| {
            agg.aggr_expr()
                .iter()
                .any(|e| e.fun().name() == range::NAME)
        })
    {
        node = node.children()[0];
    }
    let projection = node;
    assert!(projection.is::<ProjectionExec>(), "{shown}");
    assert_eq!(
        projection.children()[0]
            .properties()
            .partitioning
            .to_string(),
        "Hash([labels@2], 4)",
        "{shown}"
    );
    // It still prints as Hash([labels@2], 4): Partitioning::project turns an
    // expression the projection does not carry into an UnKnownColumn named
    // after it. That placeholder equals nothing, itself included, so nothing
    // above can find a requirement satisfied by it.
    let Partitioning::Hash(exprs, 4) = &projection.properties().partitioning else {
        panic!("{shown}");
    };
    assert!(exprs[0].is::<UnKnownColumn>(), "{exprs:?}\n{shown}");
}

/// `x` joined with itself partition by partition: the planner cannot
/// produce this yet, binary operators are unsupported, but nothing stops
/// DataFusion from trusting SeriesSetExec's Hash(labels) once they are.
/// A hash repartition on each side replaces that trust with DataFusion's
/// own hash, and is accepted.
#[test]
fn a_partitioned_join_over_the_store_partitioning_is_refused() {
    let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0).partitions(nz(4));
    let join = |wrap: &dyn Fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan>| {
        let (left, right) = (
            wrap(physical_plan(&source, "x")),
            wrap(physical_plan(&source, "x")),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("labels", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("labels", &right.schema()).unwrap()) as _,
        )];
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );
        plan
    };
    let trusted = join(&|input| input);
    let shown = displayable(trusted.as_ref()).indent(true).to_string();
    match check_selector_plans(&trusted) {
        Err(EngineError::Query(_)) => {}
        other => panic!("expected the plan refused, got {other:?}:\n{shown}"),
    }
    let rehashed = join(&|input| {
        let labels = Arc::new(Column::new_with_schema("labels", &input.schema()).unwrap());
        Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(vec![labels], 4)).unwrap())
    });
    check_selector_plans(&rehashed).unwrap_or_else(|e| panic!("{e}"));
}

/// `x or x` partition by partition: DataFusion turns a `UnionExec` into an
/// `InterleaveExec` whenever every child declares the same partitioning
/// (`enforce_distribution.rs:1458-1485`, `union.rs:664-676`), and two
/// selectors over the same store both declare `Hash([labels], n)`. The
/// interleave would then zip partition i of one scan with partition i of
/// the other, handing a Sorted consumer above it the same label set twice.
#[test]
fn an_interleave_over_the_store_partitioning_is_refused() {
    let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0).partitions(nz(4));
    let inputs = vec![physical_plan(&source, "x"), physical_plan(&source, "x")];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(InterleaveExec::try_new(inputs).unwrap());
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    match check_selector_plans(&plan) {
        Err(EngineError::Query(_)) => {}
        other => panic!("expected the plan refused, got {other:?}:\n{shown}"),
    }
}

/// A plain `UnionExec` does not pair partitions by index — each child keeps
/// contributing its own partitions — so coalescing both children first,
/// which DataFusion cannot interleave because it no longer sees a shared
/// Hash partitioning, must stay accepted.
#[test]
fn a_union_over_coalesced_children_is_accepted() {
    let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0).partitions(nz(4));
    let inputs = vec![
        Arc::new(CoalescePartitionsExec::new(physical_plan(&source, "x")))
            as Arc<dyn ExecutionPlan>,
        Arc::new(CoalescePartitionsExec::new(physical_plan(&source, "x")))
            as Arc<dyn ExecutionPlan>,
    ];
    let plan = UnionExec::try_new(inputs).unwrap();
    assert!(
        plan.is::<UnionExec>(),
        "{}",
        displayable(plan.as_ref()).indent(true)
    );
    check_selector_plans(&plan).unwrap_or_else(|e| panic!("{e}"));
}

/// `x` joined with itself, partitioned, with a `FilterExec` between each
/// side and the selector: `FilterExec` reports the same
/// `output_partitioning()` as its input (`filter.rs`'s
/// `compute_properties`), so `Hash([labels], n)` reaches the join
/// unchanged and the join must still be refused.
#[test]
fn a_partitioned_join_over_a_filter_over_the_store_partitioning_is_refused() {
    let source = MemorySeriesSource::from_descriptions(&two_counters(), 30.0).partitions(nz(4));
    let filtered = |input: Arc<dyn ExecutionPlan>| -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(lit(true), input).unwrap())
    };
    let (left, right) = (
        filtered(physical_plan(&source, "x")),
        filtered(physical_plan(&source, "x")),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("labels", &left.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("labels", &right.schema()).unwrap()) as _,
    )];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    match check_selector_plans(&plan) {
        Err(EngineError::Query(_)) => {}
        other => panic!("expected the plan refused, got {other:?}:\n{shown}"),
    }
}
