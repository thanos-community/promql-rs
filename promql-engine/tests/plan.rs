//! The planner's output shape, pinned as text in `testdata/plans.yaml`.
//!
//! Every other suite here checks numbers, which a planner change can keep
//! right while quietly moving work between nodes: a projection that stops
//! pruning labels, an aggregation that loses its grouping expression, a
//! literal that stops being folded in. Rendering the `LogicalPlan` and
//! comparing it to an expected text makes that visible as a reviewable
//! diff instead.
//!
//! The expectations are descriptive, not normative: a deliberate planner
//! change updates the YAML, and that diff is the review. Three queries
//! with three shapes is the whole corpus today; it is meant to grow one
//! query at a time as shapes are added.
//!
//! # Adding a case
//!
//! Append an entry to `testdata/plans.yaml` with a `name`, a
//! `description` saying what shape it pins, the `load` series the query
//! runs over, the `query: |`, and an empty `plan: |` block. Both of those
//! are block scalars even for a one-line query, so every entry has one
//! shape and a long query can be wrapped. Run the test:
//! the failure prints the rendered plan, and pasting it in is the whole
//! edit. Read it before pasting — the point is to notice a shape you did
//! not expect, not to record one.
//!
//! A `physical: |` block, added the same way, also pins the
//! `ExecutionPlan`. Only small cases can carry one: partition counts and
//! repartitioning make larger physical plans too fragile to hold as text.
//!
//! # Why the series are per case
//!
//! A projection over the source names the label fields one by one, and
//! those names come from the source's data. So the label set is part of
//! the shape being pinned, and a case that shares someone else's fixture
//! would pin someone else's schema. Only the label set reaches the
//! planner, so one sample per series is enough and a second adds
//! nothing; the loader rejects any series carrying more, so the corpus
//! cannot drift into looking like a data-driven evaluation suite.
//!
//! # Conventions borrowed
//!
//! The file layout — `defaults` holding the query range, `tests` holding
//! `name`/`description`/`load`/`query` — is the shared engine test-case
//! format that `promql-testcases` binds (thanos-io/promql-engine's
//! `testcases/range_queries.yaml`), read with the same `serde_norway`,
//! with `plan` added and `load` as a list of series rather than a
//! promqltest block. It sits under `testdata/` and resolves against
//! `CARGO_MANIFEST_DIR`, like the conformance corpus, so the suite does
//! not care where it was invoked from.
//!
//! # Why no Filter appears
//!
//! Matchers are the store's to apply. `SeriesSource::select` takes them,
//! so they never become a plan node, and two queries differing only in
//! their matchers render the same text.

use std::path::{Path, PathBuf};

use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use promql_engine::{Engine, MemorySeriesSource, RangeQuery};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Suite {
    defaults: Defaults,
    tests: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Defaults {
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    #[allow(dead_code)]
    description: Option<String>,
    load: Vec<String>,
    query: String,
    plan: String,
    /// The `ExecutionPlan`, for shapes the logical plan cannot show: the
    /// store-order wrapper and the physical operators DataFusion picks.
    /// Pinned only where the text does not depend on the machine.
    physical: Option<String>,
}

fn plans_file() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/testdata/plans.yaml")
}

/// The logical plan's text, and the physical plan's when `case` pins one.
fn plan_of(case: &Case, range: &RangeQuery) -> (String, Option<String>) {
    let series: Vec<_> = case
        .load
        .iter()
        .map(|l| {
            let desc = promql_parser::parse_series_desc(l).expect("series line parses");
            assert_eq!(
                desc.values.len(),
                1,
                "case {:?}: series {l:?} has {} samples; plan cases carry exactly one, \
                 because nothing but the label set reaches the planner",
                case.name,
                desc.values.len(),
            );
            desc
        })
        .collect();
    // The interval only spaces samples out, and no sample is ever read.
    let source = MemorySeriesSource::from_descriptions(&series, 30.0);

    // `Engine::new`, not `blocking`: an engine that owns a runtime cannot
    // be dropped from inside one, and the runtime here is this function's.
    let engine = Engine::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    // Both `query` and `plan` are `|` block scalars, so both shed the
    // newline it appends; a query wrapped over several lines keeps its
    // own newlines, which PromQL treats as whitespace.
    let plan = rt
        .block_on(engine.plan_async(&source, case.query.trim_end(), range))
        .expect("the query plans");
    // The renderer borrows the plan, so it cannot be the tail expression.
    let logical = plan.display_indent().to_string();
    let physical = case.physical.as_ref().map(|_| {
        // Target partitions are fixed because DataFusion defaults them to
        // the core count, which would put the machine into the text. The
        // context is otherwise stock: the logical plan already carries its
        // functions, so nothing needs registering.
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let exec = rt
            .block_on(async { ctx.execute_logical_plan(plan).await?.create_physical_plan().await })
            .expect("the plan lowers");
        // The renderer borrows the plan, so it cannot be the tail expression.
        let rendered = displayable(exec.as_ref()).indent(true).to_string();
        rendered
    });
    (logical, physical)
}

#[test]
fn every_case_plans_to_its_expected_shape() {
    let path = plans_file();
    let body = std::fs::read_to_string(&path).expect("the plans file is readable");
    let suite: Suite = serde_norway::from_str(&body).expect("the plans file parses");
    assert!(!suite.tests.is_empty(), "{} has no cases", path.display());

    let range = RangeQuery::new(
        suite.defaults.start_ms,
        suite.defaults.end_ms,
        suite.defaults.step_ms,
    );

    // Every case is reported, not just the first: one planner change
    // usually moves several shapes, and seeing all of them is what makes
    // the update a single reviewable edit.
    let mut failures = Vec::new();
    for case in &suite.tests {
        let (logical, physical) = plan_of(case, &range);
        let pairs = [
            ("plan", Some(case.plan.as_str()), Some(logical)),
            ("physical", case.physical.as_deref(), physical),
        ];
        for (block, expected, actual) in pairs {
            let (Some(expected), Some(actual)) = (expected, actual) else {
                continue;
            };
            let expected = expected.trim_end();
            let actual = actual.trim_end();
            if actual != expected {
                failures.push(format!(
                    "case {:?}, {block}:\n  query:    {}\n  expected: {}\n  actual:   {}",
                    case.name,
                    case.query.trim_end(),
                    expected.replace('\n', "\n            "),
                    actual.replace('\n', "\n            "),
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} plans changed in {}\n\n{}",
        failures.len(),
        suite.tests.len(),
        path.display(),
        failures.join("\n\n"),
    );
}
