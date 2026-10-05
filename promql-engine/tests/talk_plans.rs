//! The plans the PromCon 2026 lightning talk shows, exported as JSON for the
//! deck in `docs/talks/promcon-2026-lightning/deck` by its
//! `scripts/gen-data.sh`.
//!
//! A slide that pastes a plan by hand drifts from the planner the first time
//! a shape moves; one generated here is what the engine plans on the day the
//! deck is built. Nothing is written unless `PROMQL_TALK_PLANS_OUT` names a
//! path, so `cargo test --workspace` stays free of side effects.
//!
//! This is an export, not a pin: `tests/plan.rs` pins these shapes, and the
//! store setup below copies its harness so the two show the same plans. The
//! one difference is the shuffle width. `plan.rs` prints the session's
//! `target_partitions` as a name to keep its pins machine independent; a
//! slide shows the number, which is DataFusion's default of one per core on
//! the machine that generated it.

use std::num::NonZeroUsize;

use datafusion::physical_plan::displayable;
use promql_engine::{Engine, MemorySeriesSource, RangeQuery};
use serde_json::json;

/// In the order the slides walk through them: a selector, a range function
/// over it, and an aggregation over that.
const QUERIES: [&str; 3] = [
    "http_requests_total",
    "rate(http_requests_total[5m])",
    "sum by (job) (rate(http_requests_total[5m]))",
];

const LOAD: [&str; 2] = [
    r#"http_requests_total{job="api", pod="api-1"} 0+10x19"#,
    r#"http_requests_total{job="web", pod="web-1"} 0+20x19"#,
];

/// Chunks and partitions make the store hand over several rows per series
/// across partitions, which is what splits the aggregation into a Partial,
/// a shuffle on the block columns and the group key, and a FinalPartitioned.
const CHUNKED_MS: i64 = 150_000;
const PARTITIONS: usize = 4;

#[test]
fn export_talk_plans() {
    let Some(out) = std::env::var_os("PROMQL_TALK_PLANS_OUT") else {
        return;
    };

    let series: Vec<_> = LOAD
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect();
    let source = MemorySeriesSource::from_descriptions(&series, 30.0)
        .chunked(CHUNKED_MS)
        .rows_per_batch(NonZeroUsize::MIN)
        .partitions(NonZeroUsize::new(PARTITIONS).expect("a partition count of at least one"));
    let range = RangeQuery::new(600_000, 1_200_000, 30_000);

    // `Engine::new`, not `blocking`: an engine that owns a runtime cannot be
    // dropped from inside one, and the runtime here is this function's.
    let engine = Engine::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let entries: Vec<_> = QUERIES
        .iter()
        .map(|&query| {
            let plan = rt
                .block_on(engine.plan_async(&source, query, &range))
                .unwrap_or_else(|e| panic!("{query} plans: {e}"));
            let exec = rt
                .block_on(engine.physical_plan_async(&source, query, &range))
                .unwrap_or_else(|e| panic!("{query} lowers: {e}"));
            let physical = displayable(exec.as_ref()).indent(true).to_string();
            json!({
                "query": query,
                "logical": promql_engine::explain_plan(&plan).trim_end(),
                "physical": physical.trim_end(),
            })
        })
        .collect();

    // The aggregation slide narrates this split; a planner change that
    // moves it has to fail the export rather than ship a slide that
    // contradicts its own plan.
    let aggregation = entries[2]["physical"].as_str().expect("a string");
    for shown in [
        "AggregateExec: mode=SinglePartitioned",
        "AggregateExec: mode=Partial",
        "RepartitionExec: partitioning=Hash([block_start@0, block_end@1, __group__job@2]",
        "AggregateExec: mode=FinalPartitioned",
    ] {
        assert!(
            aggregation.contains(shown),
            "{} no longer shows {shown:?}:\n{aggregation}",
            QUERIES[2],
        );
    }

    let body = serde_json::to_string_pretty(&entries).expect("plans serialize");
    std::fs::write(&out, body + "\n")
        .unwrap_or_else(|e| panic!("writing {}: {e}", out.to_string_lossy()));
}
