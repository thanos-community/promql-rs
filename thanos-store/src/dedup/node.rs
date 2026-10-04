//! [`DedupNode`], the logical side of [`DedupExec`].

use std::fmt;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::SelectorNode;

use super::exec::{DedupConfig, DedupExec};
use super::Algorithm;

/// Deduplicates replicas between a source's scan and `SeriesSetExec`, so
/// a series reaches the engine once however many replicas stored it.
///
/// `start_ms` and `window_ms` are the select's [`SelectHints`]; the exec
/// cannot derive where the next block's reach starts from a block's own
/// columns, and it needs that to checkpoint its merge state.
///
/// [`SelectHints`]: promql_engine::SelectHints
#[derive(Debug, Clone)]
pub(crate) struct DedupNode {
    pub replica_labels: Vec<String>,
    pub algorithm: Algorithm,
    /// Whether the query's function needs the counter lift, from
    /// [`is_counter`](super::is_counter).
    pub counter: bool,
    pub start_ms: i64,
    pub window_ms: i64,
}

impl SelectorNode for DedupNode {
    fn wrap(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DedupExec::new(
            input,
            DedupConfig {
                algorithm: self.algorithm,
                counter: self.counter,
                start_ms: self.start_ms,
                window_ms: self.window_ms,
            },
        )))
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let func = match self.algorithm {
            Algorithm::Penalty => "penalty",
            Algorithm::Chain => "chain",
        };
        write!(
            f,
            "Dedup: replica_labels=[{}], func={func}, counter={}",
            self.replica_labels.join(", "),
            self.counter
        )
    }

    /// Everything the exec behaves by, since two nodes with equal keys are
    /// taken for the same plan and one replaces the other.
    fn node_key(&self) -> String {
        format!(
            "Dedup({:?},{:?},{},{},{})",
            self.replica_labels, self.algorithm, self.counter, self.start_ms, self.window_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::physical_plan::displayable;
    use promql_engine::{
        explain_plan, Engine, MemorySeriesSource, RangeQuery, SelectHints, SelectorExtension,
        SeriesSource,
    };
    use promql_parser::ast::LabelMatcher;

    fn node(algorithm: Algorithm, counter: bool) -> DedupNode {
        DedupNode {
            replica_labels: vec!["prometheus_replica".into(), "rule_replica".into()],
            algorithm,
            counter,
            start_ms: 0,
            window_ms: 300_000,
        }
    }

    #[test]
    fn explain_names_labels_function_and_counter() {
        struct Show(DedupNode);
        impl fmt::Display for Show {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                self.0.fmt_for_explain(f)
            }
        }
        assert_eq!(
            Show(node(Algorithm::Penalty, true)).to_string(),
            "Dedup: replica_labels=[prometheus_replica, rule_replica], func=penalty, counter=true"
        );
        assert!(Show(node(Algorithm::Chain, false))
            .to_string()
            .ends_with("func=chain, counter=false"));
    }

    #[test]
    fn the_key_tells_apart_every_field() {
        let base = node(Algorithm::Penalty, true);
        let keys = [
            base.node_key(),
            node(Algorithm::Chain, true).node_key(),
            node(Algorithm::Penalty, false).node_key(),
            DedupNode {
                start_ms: 1,
                ..base.clone()
            }
            .node_key(),
            DedupNode {
                window_ms: 1,
                ..base.clone()
            }
            .node_key(),
            DedupNode {
                replica_labels: vec![],
                ..base
            }
            .node_key(),
        ];
        let distinct: std::collections::HashSet<_> = keys.iter().collect();
        assert_eq!(distinct.len(), keys.len());
    }

    /// The in-memory source with a Dedup node on top of every scan; its
    /// series carry no slot, so they must come through as they are.
    #[derive(Debug)]
    struct Toy(MemorySeriesSource);

    #[async_trait]
    impl SeriesSource for Toy {
        async fn select(
            &self,
            state: &dyn Session,
            matchers: &[LabelMatcher],
            hints: SelectHints,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            self.0.select(state, matchers, hints).await
        }

        fn scan_node(
            &self,
            scan: LogicalPlan,
            hints: &SelectHints,
        ) -> datafusion::error::Result<LogicalPlan> {
            let node = DedupNode {
                window_ms: hints.window_ms,
                start_ms: hints.start_ms,
                ..node(Algorithm::Penalty, false)
            };
            Ok(SelectorExtension::plan(Arc::new(node), scan))
        }
    }

    #[tokio::test]
    async fn the_node_lowers_to_a_dedup_exec_beneath_the_series_set() {
        let desc = promql_parser::parse_series_desc(r#"up{job="a"} 1+1x15"#).unwrap();
        let toy = Toy(MemorySeriesSource::from_descriptions(&[desc], 30.0));
        let engine = Engine::new();
        let q = RangeQuery::new(300_000, 300_000, 30_000);

        let plan = engine.plan_async(&toy, "sum(up)", &q).await.unwrap();
        let logical = explain_plan(&plan);
        assert!(logical.contains("Dedup: replica_labels="), "{logical}");

        let exec = engine
            .physical_plan_async(&toy, "sum(up)", &q)
            .await
            .unwrap();
        let text = displayable(exec.as_ref()).indent(true).to_string();
        let lines: Vec<&str> = text.lines().map(str::trim_start).collect();
        let at = lines
            .iter()
            .position(|l| l.starts_with("SeriesSetExec"))
            .unwrap();
        assert!(lines[at + 1].starts_with("DedupExec"), "{text}");
        assert!(!text.contains("SortExec"), "{text}");
    }
}
