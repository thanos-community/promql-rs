//! `Matrix.ContainsSameLabelset` as a DataFusion plan node and its operator.
//!
//! Prometheus checks this once, on the result of a call over a range
//! selector (`promql/engine.go`, the `rangeEval` of a `Call` over a
//! `MatrixSelector`): dropping `__name__` from a selector that matched
//! several metric names leaves two rows with one label set, and rather
//! than merge them it errors. A plain projection would leave them side by
//! side, so the plan carries this node above the projection instead.
//!
//! It relies on the store's obligation to put one row per series in a
//! batch ([`crate::source`]); a store that split one series across two
//! batches would trip the check, which is the same thing that already
//! breaks every operator above it.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::{internal_err, DFSchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::session_state::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;

use crate::error::EngineError;
use crate::series::{LABELS, SAMPLES};

const NAME: &str = "ContainsSameLabelset";

/// Prometheus's own sentence, word for word: the conformance suite
/// compares error text, not error kinds.
pub const SAME_LABELSET: &str = "vector cannot contain metrics with the same labelset";

/// Fail the query if `plan` ever emits two rows with one label set.
pub fn contains_same_labelset(plan: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(ContainsSameLabelset::new(plan)),
    })
}

/// The plan node. Rows pass through untouched; only the error is new.
///
/// The schema is pinned when the node is made rather than read off the
/// input, and carried through `with_exprs_and_inputs`. DataFusion pushes
/// struct field accesses such as `get_field(labels, 'pod')` towards the
/// leaves through any single-input node whose rebuilt output schema shows
/// the extracted column, and `sum by (pod) (rate({…}[5m]))` has exactly
/// such an access above this node. Owning the schema means the rebuild
/// never shows it, so the projection stays above and the operator still
/// gets the `labels` struct it hashes rather than loose scalars.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ContainsSameLabelset {
    input: LogicalPlan,
    schema: DFSchemaRef,
}

impl ContainsSameLabelset {
    fn new(input: LogicalPlan) -> Self {
        Self {
            schema: Arc::clone(input.schema()),
            input,
        }
    }
}

/// `DFSchema` has no order; the schema follows from the input anyway.
impl PartialOrd for ContainsSameLabelset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl UserDefinedLogicalNodeCore for ContainsSameLabelset {
    fn name(&self) -> &str {
        NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{NAME}")
    }

    // `necessary_children_exprs` and `prevent_predicate_push_down_columns`
    // stay at their defaults, which report "cannot say" and "no predicate
    // may pass": the check has to see every row the projection above it
    // would emit, so nothing below it may drop or filter rows.

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return internal_err!(
                "{NAME} takes one input and no expressions, got {} and {}",
                inputs.len(),
                exprs.len()
            );
        }
        Ok(Self {
            input: inputs.swap_remove(0),
            ..self.clone()
        })
    }
}

/// Plans a [`ContainsSameLabelset`] as a [`ContainsSameLabelsetExec`].
#[derive(Debug, Default)]
pub struct ContainsSameLabelsetPlanner;

#[async_trait]
impl ExtensionPlanner for ContainsSameLabelsetPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if node
            .as_any()
            .downcast_ref::<ContainsSameLabelset>()
            .is_none()
        {
            return Ok(None);
        }
        let [input] = physical_inputs else {
            return internal_err!("{NAME} takes one input, got {}", physical_inputs.len());
        };
        Ok(Some(Arc::new(ContainsSameLabelsetExec::new(Arc::clone(
            input,
        )))))
    }
}

/// The operator: pass every batch through, and fail on the first label
/// set seen twice.
#[derive(Debug)]
pub struct ContainsSameLabelsetExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl ContainsSameLabelsetExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        // Unlike `CoalescePartitionsExec`, this node never merges more
        // than one input partition, so unlike that node it keeps the
        // input's orderings rather than clearing them.
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self { input, properties }
    }
}

impl DisplayAs for ContainsSameLabelsetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{NAME}")
    }
}

impl ExecutionPlan for ContainsSameLabelsetExec {
    fn name(&self) -> &str {
        NAME
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// One partition, so one set sees every row: two rows with the same
    /// label set are a duplicate wherever the store put them.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("{NAME} takes one input, got {}", children.len());
        };
        Ok(Arc::new(Self::new(Arc::clone(input))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("{NAME} has one partition, partition {partition} was asked for");
        }
        // `required_input_distribution` asks `EnforceDistribution` for a
        // coalesce below this node; without it rows in partitions >= 1
        // would vanish from both the check and the output.
        if self.input.output_partitioning().partition_count() != 1 {
            return internal_err!("{NAME} requires a single input partition");
        }
        let schema = self.schema();
        let labels = schema
            .field_with_name(LABELS)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        // Row format rather than a hash: `create_hashes` would report a
        // collision as a duplicate label set, and this error is an answer
        // the user sees, not a heuristic.
        let converter = RowConverter::new(vec![SortField::new(labels.data_type().clone())])?;
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let input = self.input.execute(0, context)?;
        let stream = input.map(move |batch| {
            let batch = batch?;
            let column = batch
                .column_by_name(LABELS)
                .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: no {LABELS} column")))?;
            let samples = batch
                .column_by_name(SAMPLES)
                .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: no {SAMPLES} column")))?
                .as_list::<i32>();
            let rows = converter.convert_columns(std::slice::from_ref(column))?;
            for (i, row) in rows.iter().enumerate() {
                // A series with no points is not in Prometheus's output
                // matrix, so it cannot collide with anything in it. This
                // predicate must match `series::drop_empty` exactly, or a
                // row skipped here but kept there lets a real duplicate
                // through unchecked.
                if samples.value_length(i) == 0 {
                    continue;
                }
                if !seen.insert(row.as_ref().to_vec()) {
                    return Err(DataFusionError::External(Box::new(EngineError::Query(
                        SAME_LABELSET.into(),
                    ))));
                }
            }
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::compute::concat_batches;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::series::{encode, label_names_of, Series};

    fn row(pod: &str, name: &str) -> Series {
        Series::new(&[("__name__", name), ("pod", pod)], vec![0], vec![1.0]).unwrap()
    }

    /// Run `batches`, each its own batch of one partition, through the
    /// operator.
    ///
    /// Rows are encoded one at a time and concatenated: [`encode`]
    /// refuses a duplicate label set, which is the very input under test
    /// here — a store that hands one over is what the operator catches.
    async fn check(batches: Vec<Vec<Series>>) -> Result<Vec<RecordBatch>> {
        let names = label_names_of(&batches.concat());
        let one = |row| encode(&names, std::slice::from_ref(row)).unwrap();
        let schema = one(&batches[0][0]).schema();
        let batches: Vec<RecordBatch> = batches
            .iter()
            .map(|rows| concat_batches(&schema, rows.iter().map(one).collect::<Vec<_>>().iter()))
            .collect::<std::result::Result<_, _>>()?;
        let input = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
        let exec = Arc::new(ContainsSameLabelsetExec::new(input));
        collect(exec, SessionContext::new().task_ctx()).await
    }

    #[tokio::test]
    async fn a_label_set_repeated_across_batches_is_an_error() {
        let err = check(vec![
            vec![row("envoy-1", "a"), row("envoy-2", "a")],
            vec![row("envoy-3", "a"), row("envoy-1", "a")],
        ])
        .await
        .unwrap_err();
        assert!(err.to_string().contains(SAME_LABELSET), "{err}");
    }

    #[tokio::test]
    async fn a_label_set_repeated_within_one_batch_is_an_error() {
        let err = check(vec![vec![row("envoy-1", "a"), row("envoy-1", "a")]])
            .await
            .unwrap_err();
        assert!(err.to_string().contains(SAME_LABELSET), "{err}");
    }

    #[tokio::test]
    async fn distinct_label_sets_pass_through_unchanged() {
        let batches = check(vec![
            vec![row("envoy-1", "a"), row("envoy-2", "a")],
            vec![row("envoy-3", "a")],
        ])
        .await
        .unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
    }
}
