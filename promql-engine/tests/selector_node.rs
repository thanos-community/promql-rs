//! A source's node on top of its selector scan, end to end.
//!
//! The toy node drops the series `pod="envoy-2"`, which a pass-through
//! would not show: the answers change only if its exec really sits in the
//! executed plan. The plan texts are pinned here, not in `plans.yaml`,
//! because a source without the override must keep every pin there as it
//! was, and this file is where the override is exercised.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{AsArray, BooleanArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::catalog::Session;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    displayable, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::StreamExt;
use promql_engine::{
    check_selector_plans, explain_plan, Engine, MemorySeriesSource, RangeQuery, SelectHints,
    SelectorExtension, SelectorNode, SeriesSource,
};
use promql_parser::ast::LabelMatcher;
use promql_parser::SeriesDescription;

/// Keeps every series but `pod="envoy-2"`.
#[derive(Debug)]
struct DropEnvoy2;

impl SelectorNode for DropEnvoy2 {
    fn wrap(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DropEnvoy2Exec { input }))
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DropEnvoy2")
    }

    fn node_key(&self) -> String {
        "DropEnvoy2".into()
    }
}

#[derive(Debug)]
struct DropEnvoy2Exec {
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for DropEnvoy2Exec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DropEnvoy2Exec")
    }
}

impl ExecutionPlan for DropEnvoy2Exec {
    fn name(&self) -> &str {
        "DropEnvoy2Exec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children.swap_remove(0),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let kept = input.map(|batch| -> Result<RecordBatch> {
            let batch = batch?;
            let mask: BooleanArray = {
                let labels = batch.column_by_name("labels").unwrap().as_struct();
                let pod = labels.column_by_name("pod").unwrap().as_string_view();
                pod.iter().map(|p| Some(p != Some("envoy-2"))).collect()
            };
            Ok(filter_record_batch(&batch, &mask)?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, kept)))
    }
}

/// The in-memory source, with the node on top of every scan or without.
#[derive(Debug)]
struct Toy {
    inner: MemorySeriesSource,
    with_node: bool,
}

#[async_trait]
impl SeriesSource for Toy {
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.select(state, matchers, hints).await
    }

    fn scan_node(&self, scan: LogicalPlan, _hints: &SelectHints) -> Result<LogicalPlan> {
        Ok(if self.with_node {
            SelectorExtension::plan(Arc::new(DropEnvoy2), scan)
        } else {
            scan
        })
    }
}

fn toy(with_node: bool) -> Toy {
    let lines = [
        r#"http_requests_total{pod="envoy-1"} 1+1x15"#,
        r#"http_requests_total{pod="envoy-2"} 1+2x18"#,
    ];
    let desc: Vec<SeriesDescription> = lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).unwrap())
        .collect();
    Toy {
        inner: MemorySeriesSource::from_descriptions(&desc, 30.0),
        with_node,
    }
}

fn physical_text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

/// Whether `child` is the first line after `parent`, indented one level.
fn directly_beneath(text: &str, parent: &str, child: &str) -> bool {
    let lines: Vec<&str> = text.lines().collect();
    lines.windows(2).any(|w| {
        w[0].trim_start().starts_with(parent) && w[1].trim_start().starts_with(child) && {
            let indent = |l: &str| l.len() - l.trim_start().len();
            indent(w[1]) == indent(w[0]) + 2
        }
    })
}

#[tokio::test]
async fn the_node_shows_above_the_scan_in_logical_explain() {
    let engine = Engine::new();
    let q = RangeQuery::new(300_000, 300_000, 30_000);
    let plan = engine
        .plan_async(&toy(true), "sum(http_requests_total)", &q)
        .await
        .unwrap();
    let text = explain_plan(&plan);
    let lines: Vec<&str> = text.lines().collect();
    let node = lines.iter().position(|l| l.trim() == "DropEnvoy2").unwrap();
    assert!(
        lines[node + 1]
            .trim_start()
            .starts_with("TableScan: http_requests_total"),
        "{text}"
    );
}

#[tokio::test]
async fn the_node_lowers_beneath_series_set_exec_without_a_sort() {
    let engine = Engine::new();
    let q = RangeQuery::new(300_000, 300_000, 30_000);
    let exec = engine
        .physical_plan_async(&toy(true), "sum(http_requests_total)", &q)
        .await
        .unwrap();
    let text = physical_text(&exec);
    assert!(
        directly_beneath(&text, "SeriesSetExec", "DropEnvoy2Exec"),
        "{text}"
    );
    assert!(!text.contains("SortExec"), "{text}");
    check_selector_plans(&exec).unwrap();
}

/// A range function builds its scan on a different path from a bare
/// selector, and the node must be there too.
#[tokio::test]
async fn the_node_is_on_a_range_function_selector_too() {
    let engine = Engine::new();
    let q = RangeQuery::new(300_000, 300_000, 30_000);
    let query = "sum(rate(http_requests_total[2m]))";
    let plan = engine.plan_async(&toy(true), query, &q).await.unwrap();
    assert!(explain_plan(&plan).contains("DropEnvoy2"));
    let exec = engine
        .physical_plan_async(&toy(true), query, &q)
        .await
        .unwrap();
    let text = physical_text(&exec);
    assert!(
        directly_beneath(&text, "SeriesSetExec", "DropEnvoy2Exec"),
        "{text}"
    );
}

/// The values of each series, in output order; `sum` leaves one.
fn values(batches: &[RecordBatch]) -> Vec<Vec<f64>> {
    promql_engine::series::decode(batches)
        .unwrap()
        .iter()
        .map(|s| s.values().to_vec())
        .collect()
}

#[tokio::test]
async fn an_instant_sum_sees_only_the_surviving_series() {
    let engine = Engine::new();
    // envoy-1 holds 11 at 300s and envoy-2 holds 21.
    let q = RangeQuery::new(300_000, 300_000, 30_000);
    let with = values(
        &engine
            .range_query_async(&toy(true), "sum(http_requests_total)", &q)
            .await
            .unwrap(),
    );
    let without = values(
        &engine
            .range_query_async(&toy(false), "sum(http_requests_total)", &q)
            .await
            .unwrap(),
    );
    assert_eq!(with, vec![vec![11.0]]);
    assert_eq!(without, vec![vec![32.0]]);
}

#[tokio::test]
async fn a_range_query_sees_only_the_surviving_series() {
    let engine = Engine::new();
    let q = RangeQuery::new(300_000, 360_000, 30_000);
    let with = values(
        &engine
            .range_query_async(&toy(true), "sum(http_requests_total)", &q)
            .await
            .unwrap(),
    );
    assert_eq!(with, vec![vec![11.0, 12.0, 13.0]]);
}

/// No override, no node: the plans are what they were before the hook.
#[tokio::test]
async fn a_source_without_the_override_plans_as_before() {
    let engine = Engine::new();
    let q = RangeQuery::new(300_000, 300_000, 30_000);
    let plan = engine
        .plan_async(&toy(false), "sum(http_requests_total)", &q)
        .await
        .unwrap();
    let plain = engine
        .plan_async(&toy(false).inner, "sum(http_requests_total)", &q)
        .await
        .unwrap();
    assert_eq!(explain_plan(&plan), explain_plan(&plain));
    assert!(!explain_plan(&plan).contains("DropEnvoy2"));
    let a = physical_text(
        &engine
            .physical_plan_async(&toy(false), "sum(http_requests_total)", &q)
            .await
            .unwrap(),
    );
    let b = physical_text(
        &engine
            .physical_plan_async(&toy(false).inner, "sum(http_requests_total)", &q)
            .await
            .unwrap(),
    );
    assert_eq!(a, b);
    assert!(!a.contains("DropEnvoy2Exec"), "{a}");
}
