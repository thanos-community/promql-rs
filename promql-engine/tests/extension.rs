//! The seam for a caller's own plan step: a plan from `plan_async` with a
//! `LogicalPlan::Extension` node put above a selector's scan, run by an
//! engine built with the node's `ExtensionPlanner`, and the hints a scan
//! keeps for such a caller. The node here does nothing; what a real one
//! does is the caller's business.

use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::DFSchemaRef;
use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use promql_engine::{
    Engine, MemorySeriesSource, RangeQuery, SelectHints, SelectorTable, Series, SeriesSource,
};
use promql_parser::ast::LabelMatcher;

/// A node that changes nothing: the input's schema, the input's rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
struct Passthrough {
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for Passthrough {
    fn name(&self) -> &str {
        "Passthrough"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Passthrough")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            input: inputs.swap_remove(0),
        })
    }
}

#[derive(Debug)]
struct PassthroughPlanner;

#[async_trait]
impl ExtensionPlanner for PassthroughPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(node
            .as_any()
            .downcast_ref::<Passthrough>()
            .map(|_| Arc::clone(&physical_inputs[0])))
    }
}

/// Put a `Passthrough` above every scan.
fn wrap(plan: LogicalPlan) -> LogicalPlan {
    plan.transform_up(|node| {
        Ok(match node {
            LogicalPlan::TableScan(_) => Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(Passthrough { input: node }),
            })),
            other => Transformed::no(other),
        })
    })
    .unwrap()
    .data
}

/// A source that remembers the hints of every selection.
#[derive(Debug)]
struct Recording {
    inner: MemorySeriesSource,
    hints: Mutex<Vec<SelectHints>>,
}

#[async_trait]
impl SeriesSource for Recording {
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.hints.lock().unwrap().push(hints.clone());
        self.inner.select(state, matchers, hints).await
    }
}

fn series(labels: &[(&str, &str)], values: &[f64]) -> Series {
    let timestamps = (0..values.len() as i64).map(|i| i * 15_000).collect();
    Series::new(labels, timestamps, values.to_vec()).unwrap()
}

fn requests() -> MemorySeriesSource {
    MemorySeriesSource::try_new(vec![
        series(
            &[("__name__", "http_requests_total"), ("pod", "a")],
            &[0.0, 10.0, 20.0, 30.0, 40.0],
        ),
        series(
            &[("__name__", "http_requests_total"), ("pod", "b")],
            &[0.0, 5.0, 10.0, 15.0, 20.0],
        ),
    ])
    .unwrap()
}

type Flat = Vec<(Vec<(String, String)>, Vec<i64>, Vec<f64>)>;

/// Labels, timestamps and values, sorted by labels: the engine's output
/// order is not part of its contract and a changed plan may change it.
fn flatten(batches: &[RecordBatch]) -> Flat {
    let mut flat: Flat = promql_engine::series::decode(batches)
        .unwrap()
        .iter()
        .map(|s| {
            (
                s.labels()
                    .map(|(n, v)| (n.to_string(), v.to_string()))
                    .collect(),
                s.timestamps().to_vec(),
                s.values().to_vec(),
            )
        })
        .collect();
    flat.sort_by(|a, b| a.0.cmp(&b.0));
    flat
}

#[tokio::test]
async fn a_callers_node_runs_through_the_planner_the_engine_was_built_with() {
    let source = requests();
    let range = RangeQuery::new(0, 60_000, 15_000);
    let query = "sum by (pod) (rate(http_requests_total[1m]))";
    let engine = Engine::with_extension_planners(vec![Arc::new(PassthroughPlanner)]);

    let plain = engine
        .range_query_async(&source, query, &range)
        .await
        .unwrap();
    let plan = wrap(engine.plan_async(&source, query, &range).await.unwrap());
    let shown = plan.display_indent().to_string();
    assert!(shown.contains("Passthrough"), "{shown}");
    let wrapped = engine.execute_async(plan).await.unwrap();
    assert_eq!(flatten(&plain), flatten(&wrapped));

    // Without the planner, DataFusion refuses the node.
    let bare = Engine::new();
    let plan = wrap(bare.plan_async(&source, query, &range).await.unwrap());
    let err = bare.execute_async(plan).await.unwrap_err();
    assert!(err.to_string().contains("No installed planner"), "{err}");
}

#[tokio::test]
async fn a_scan_keeps_the_hints_it_was_asked_with() {
    let source = Recording {
        inner: requests(),
        hints: Mutex::new(Vec::new()),
    };
    let engine = Engine::new();
    let range = RangeQuery::new(0, 60_000, 15_000);

    let plan = engine
        .plan_async(
            &source,
            "sum by (pod) (rate(http_requests_total[1m]))",
            &range,
        )
        .await
        .unwrap();
    let recorded = source.hints.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].func.as_deref(), Some("rate"));
    assert_eq!(recorded[0].range_ms, Some(60_000));
    // A range function between the aggregation and the selector makes
    // the grouping describe something other than the selector's parent,
    // so the planner drops it and a store must not rely on it there.
    assert!(recorded[0].grouping.is_none());

    let mut kept = None;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let provider = source_as_provider(&scan.source)?;
            let table = provider.downcast_ref::<SelectorTable>().unwrap();
            kept = Some(table.hints().clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(kept.as_ref(), Some(&recorded[0]));

    engine
        .plan_async(&source, "sum(http_requests_total)", &range)
        .await
        .unwrap();
    let recorded = source.hints.lock().unwrap().clone();
    assert_eq!(recorded[1].func.as_deref(), Some("sum"));
    assert_eq!(recorded[1].range_ms, None);
}
