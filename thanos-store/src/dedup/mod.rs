//! Replica deduplication, Thanos's `pkg/dedup`, as a step in the
//! DataFusion plan.
//!
//! The same series scraped by two Prometheus replicas reaches the querier
//! twice, the rows differing only in the labels that tell replicas apart
//! (`--query-replica-label`). Go's querier merges them with the penalty
//! iterator: whichever replica has the next sample is followed, and the
//! other is not consulted again until twice the last interval has passed,
//! so the merged series keeps the scrape frequency instead of doubling it.
//! When the function above the selector is a counter function, a replica
//! that lags behind is lifted to the last value handed out so PromQL sees
//! no false reset. [`iter`] is that algorithm, ported from `iter.go` onto
//! drained samples.
//!
//! Here the merge is a [`DedupNode`] the querier puts into the engine's
//! finished plan, directly above each selector's scan, with
//! [`Dedup::inject`]; its operator, [`DedupExec`], merges the rows that
//! are equal but for the replica labels. The engine knows nothing of it
//! beyond the [`ExtensionPlanner`] it is built with, [`Dedup::planner`].
//! The node keeps the scan's schema and writes `""`, PromQL's absent
//! label, into the replica labels, so everything the engine planned above
//! the scan runs unchanged and the label is gone from the result, as it
//! is in Go.
//!
//! Unlike Go, stores are not asked to drop the replica labels
//! (`without_replica_labels`). A store only strips them, it does not merge
//! samples, and the rows of one series would then share a label set,
//! which the engine's batch refuses as one series split in two. Keeping
//! the labels keeps the replicas apart by identity, so Go's
//! `overlapSplit`, which reconstructs replicas from overlapping chunks, is
//! not needed either.

mod iter;
mod plan;

use std::str::FromStr;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_planner::ExtensionPlanner;
use promql_engine::SelectorTable;

pub use iter::{chain_samples, dedup_samples};
pub use plan::{DedupExec, DedupNode, DedupPlanner};

/// `--query-deduplication-func`: how the samples of replicas are merged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeduplicationFunc {
    /// Follow one replica and penalise switching: `dedup.AlgorithmPenalty`.
    #[default]
    Penalty,
    /// Union the samples one to one, the first replica winning a shared
    /// timestamp: `dedup.AlgorithmChain`, Prometheus's `ChainedSeriesMerge`.
    Chain,
}

impl DeduplicationFunc {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Penalty => "penalty",
            Self::Chain => "chain",
        }
    }
}

impl FromStr for DeduplicationFunc {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "penalty" => Ok(Self::Penalty),
            "chain" => Ok(Self::Chain),
            other => Err(format!(
                "unknown deduplication function {other:?}; want penalty or chain"
            )),
        }
    }
}

/// `isCounter`: the functions whose input must never go backwards.
pub fn is_counter(func: Option<&str>) -> bool {
    matches!(func, Some("increase" | "rate" | "irate" | "resets"))
}

/// One request's deduplication: what `dedup.NewSeriesSet` is given, and
/// where in the plan it goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dedup {
    replica_labels: Vec<String>,
    func: DeduplicationFunc,
}

impl Dedup {
    pub fn new(replica_labels: Vec<String>, func: DeduplicationFunc) -> Self {
        Self {
            replica_labels,
            func,
        }
    }

    /// For `Engine::with_extension_planners`: turns a [`DedupNode`] into a
    /// [`DedupExec`].
    pub fn planner() -> Arc<dyn ExtensionPlanner + Send + Sync> {
        Arc::new(DedupPlanner)
    }

    /// Put a [`DedupNode`] above every selector scan of `plan` whose labels
    /// include a replica label. Bottom up, so the new node's own child is
    /// not visited again. The scans are found by their table, so any
    /// `SeriesSource` is fine, and the function above each selector comes
    /// from the hints the scan was asked with.
    pub fn inject(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if self.replica_labels.is_empty() {
            return Ok(plan);
        }
        let transformed = plan.transform_up(|node| {
            let LogicalPlan::TableScan(scan) = &node else {
                return Ok(Transformed::no(node));
            };
            let provider = source_as_provider(&scan.source)?;
            let Some(table) = provider.downcast_ref::<SelectorTable>() else {
                return Ok(Transformed::no(node));
            };
            let names = table.label_names();
            let replica_labels: Vec<String> = self
                .replica_labels
                .iter()
                .filter(|l| names.contains(l))
                .cloned()
                .collect();
            if replica_labels.is_empty() {
                return Ok(Transformed::no(node));
            }
            let is_counter = is_counter(table.hints().func.as_deref());
            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(DedupNode::new(node, replica_labels, self.func, is_counter)),
            })))
        })?;
        Ok(transformed.data)
    }
}

#[cfg(test)]
mod tests {
    use promql_engine::{Engine, MemorySeriesSource, RangeQuery, Series};

    use super::*;

    fn series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> Series {
        let (ts, vs): (Vec<i64>, Vec<f64>) = samples.iter().copied().unzip();
        Series::new(labels, ts, vs).unwrap()
    }

    /// `up` from two replicas, `other` from none.
    fn replicated() -> MemorySeriesSource {
        MemorySeriesSource::new(vec![
            series(
                &[("__name__", "up"), ("job", "x"), ("replica", "a")],
                &[(0, 1.0), (10_000, 1.0), (20_000, 1.0)],
            ),
            series(
                &[("__name__", "up"), ("job", "x"), ("replica", "b")],
                &[(1_000, 1.0), (11_000, 1.0), (21_000, 1.0)],
            ),
            series(
                &[("__name__", "other"), ("job", "x")],
                &[(0, 5.0), (10_000, 5.0), (20_000, 5.0)],
            ),
        ])
    }

    #[test]
    fn deduplication_func_parses_like_the_flag() {
        assert_eq!("penalty".parse(), Ok(DeduplicationFunc::Penalty));
        assert_eq!("chain".parse(), Ok(DeduplicationFunc::Chain));
        assert!("first".parse::<DeduplicationFunc>().is_err());
        assert_eq!(DeduplicationFunc::default().as_str(), "penalty");
        assert!(is_counter(Some("rate")) && is_counter(Some("resets")));
        assert!(!is_counter(Some("sum")) && !is_counter(None));
    }

    #[tokio::test]
    async fn inject_puts_the_node_directly_above_the_scan() {
        let source = replicated();
        let engine = Engine::new();
        let range = RangeQuery::new(0, 20_000, 10_000);
        let dedup = Dedup::new(vec!["replica".into()], DeduplicationFunc::Penalty);

        let plan = engine
            .plan_async(&source, "rate(up[1m])", &range)
            .await
            .unwrap();
        let injected = dedup.inject(plan).unwrap();
        let shown = injected.display_indent().to_string();
        let node = shown
            .lines()
            .position(|l| l.contains("ThanosDedup"))
            .unwrap_or_else(|| panic!("no node in\n{shown}"));
        let lines: Vec<&str> = shown.lines().collect();
        assert!(
            lines[node]
                .contains(r#"ThanosDedup: replica_labels=["replica"], func=penalty, counter=true"#),
            "{shown}"
        );
        assert!(lines[node + 1].contains("TableScan: selector_0"), "{shown}");

        let plan = engine.plan_async(&source, "sum(up)", &range).await.unwrap();
        let shown = dedup.inject(plan).unwrap().display_indent().to_string();
        assert!(shown.contains("counter=false"), "{shown}");
    }

    #[tokio::test]
    async fn scans_without_a_replica_label_are_left_alone() {
        let source = replicated();
        let engine = Engine::new();
        let range = RangeQuery::new(0, 20_000, 10_000);
        let plan = engine.plan_async(&source, "other", &range).await.unwrap();

        let dedup = Dedup::new(vec!["replica".into()], DeduplicationFunc::Penalty);
        assert_eq!(dedup.inject(plan.clone()).unwrap(), plan);

        let none = Dedup::new(Vec::new(), DeduplicationFunc::Penalty);
        let plan = engine.plan_async(&source, "up", &range).await.unwrap();
        assert_eq!(none.inject(plan.clone()).unwrap(), plan);
    }

    #[tokio::test]
    async fn the_engine_runs_the_injected_plan() {
        let source = replicated();
        let engine = Engine::with_extension_planners(vec![Dedup::planner()]);
        let range = RangeQuery::new(0, 20_000, 10_000);
        let dedup = Dedup::new(vec!["replica".into()], DeduplicationFunc::Penalty);

        let plan = engine.plan_async(&source, "up", &range).await.unwrap();
        let result = engine
            .execute_async(dedup.inject(plan).unwrap())
            .await
            .unwrap();
        assert_eq!(result.len(), 1, "{result:?}");
        assert_eq!(result[0].label("replica"), "");
        assert_eq!(result[0].label("job"), "x");
        assert_eq!(result[0].timestamps(), &[0, 10_000, 20_000]);

        // The engine's aggregation above the node sees one series.
        let plan = engine
            .plan_async(&source, "count(up)", &range)
            .await
            .unwrap();
        let result = engine
            .execute_async(dedup.inject(plan).unwrap())
            .await
            .unwrap();
        assert_eq!(result[0].values(), &[1.0, 1.0, 1.0]);

        // Label accesses above the node, which DataFusion likes to push
        // down to the scan, stay above it: the group and the kept labels
        // are read off the merged row.
        for query in [
            "sum by (job) (up)",
            "rate(up[1m])",
            "count by (replica) (up)",
        ] {
            let plan = engine.plan_async(&source, query, &range).await.unwrap();
            let result = engine
                .execute_async(dedup.inject(plan).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            assert_eq!(result.len(), 1, "{query}: {result:?}");
            assert_eq!(result[0].label("replica"), "", "{query}");
        }

        // And without the node, two, once b has its first sample.
        let plan = engine
            .plan_async(&source, "count(up)", &range)
            .await
            .unwrap();
        let result = engine.execute_async(plan).await.unwrap();
        assert_eq!(result[0].values(), &[1.0, 2.0, 2.0]);

        // An engine without the planner cannot run the node.
        let plan = Engine::new()
            .plan_async(&source, "up", &range)
            .await
            .unwrap();
        let err = Engine::new()
            .execute_async(dedup.inject(plan).unwrap())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("No installed planner"), "{err}");
    }
}
