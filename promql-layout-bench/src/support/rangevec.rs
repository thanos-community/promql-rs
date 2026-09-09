//! The range-vector operator, written once and fed by either layout.
//!
//! This is the operator an engine has to write anyway: DataFusion has no notion of a series, so
//! something has to find them before a range-vector function can run. Per batch it does three
//! things:
//!
//! 1. **Find the series.** That is the layout's job, through [`Layout::series`]; this module never
//!    looks inside a batch.
//! 2. **Run the kernel.** The real `rate` from [`super::ratefn`], over `&[i64]` and `&[f64]`
//!    slices straight out of the sample buffers. Identical code for both layouts.
//! 3. **Emit one row per series**, labels alongside a scalar or a step grid, so everything above
//!    is the same plan whichever layout fed it.

use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Float64Array, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, RecordBatchStream,
};
use futures::{Stream, StreamExt};

use super::format::{output_schema, step_grid_array};
use super::layout::Layout;
use super::query::Query;
use super::ratefn::{extrapolated_rate, rate_steps};

// ---------------------------------------------------------------------------------------------
// Table provider
// ---------------------------------------------------------------------------------------------

/// `rate(m[w])` as a table: the raw scan wrapped in a [`RangeVectorExec`].
#[derive(Debug)]
pub struct RateTable<L: Layout> {
    scan: Arc<dyn TableProvider>,
    query: Query,
    schema: SchemaRef,
    layout: PhantomData<L>,
}

/// Wrap the table holding `L` batches.
pub fn rate_table<L: Layout>(scan: Arc<dyn TableProvider>, query: Query) -> Arc<dyn TableProvider> {
    Arc::new(RateTable::<L> {
        scan,
        query,
        schema: output_schema(query.is_instant()),
        layout: PhantomData,
    })
}

#[async_trait]
impl<L: Layout> TableProvider for RateTable<L> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = self.scan.scan(state, None, &[], None).await?;
        let exec: Arc<dyn ExecutionPlan> = Arc::new(RangeVectorExec::<L>::new(input, self.query));
        let Some(cols) = projection else {
            return Ok(exec);
        };
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = cols
            .iter()
            .map(|&i| {
                let name = self.schema.field(i).name().clone();
                (
                    Arc::new(Column::new(&name, i)) as Arc<dyn PhysicalExpr>,
                    name,
                )
            })
            .collect();
        Ok(Arc::new(ProjectionExec::try_new(exprs, exec)?))
    }
}

// ---------------------------------------------------------------------------------------------
// Execution plan
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct RangeVectorExec<L: Layout> {
    input: Arc<dyn ExecutionPlan>,
    query: Query,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    layout: PhantomData<L>,
}

impl<L: Layout> RangeVectorExec<L> {
    pub fn new(input: Arc<dyn ExecutionPlan>, query: Query) -> Self {
        let schema = output_schema(query.is_instant());
        let props = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            input,
            query,
            schema,
            props: Arc::new(props),
            layout: PhantomData,
        }
    }
}

impl<L: Layout> DisplayAs for RangeVectorExec<L> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RangeVectorExec: layout={}", L::NAME)
    }
}

impl<L: Layout> ExecutionPlan for RangeVectorExec<L> {
    fn name(&self) -> &str {
        "RangeVectorExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    /// Series are contiguous within a partition and the operator depends on it, so the planner
    /// must not round-robin the input's batches across more partitions.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(Arc::clone(&children[0]), self.query)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(RangeVectorStream::<L> {
            input: self.input.execute(partition, context)?,
            query: self.query,
            schema: Arc::clone(&self.schema),
            layout: L::default(),
            done: false,
        }))
    }
}

// ---------------------------------------------------------------------------------------------
// Kernel and output
// ---------------------------------------------------------------------------------------------

enum Outcome {
    Scalar(f64),
    Grid(Vec<Option<f64>>),
}

/// The range-vector function over one series. `None` when PromQL would drop the series.
fn compute(query: &Query, ts: &[i64], vs: &[f64]) -> Option<Outcome> {
    if query.is_instant() {
        return extrapolated_rate(ts, vs, query.at, query.range_ms, query.per_second)
            .map(Outcome::Scalar);
    }
    let steps = rate_steps(ts, vs, query.grid(), query.per_second);
    steps
        .iter()
        .any(Option::is_some)
        .then_some(Outcome::Grid(steps))
}

fn output_batch<L: Layout>(
    schema: &SchemaRef,
    query: &Query,
    input: &RecordBatch,
    rows: Vec<(L::Labels, Outcome)>,
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    let (labels, outcomes): (Vec<L::Labels>, Vec<Outcome>) = rows.into_iter().unzip();
    let labels = L::labels_column(input, labels)?;
    let result: ArrayRef = if query.is_instant() {
        let values: Vec<f64> = outcomes
            .into_iter()
            .map(|o| match o {
                Outcome::Scalar(v) => v,
                Outcome::Grid(_) => unreachable!(),
            })
            .collect();
        Arc::new(Float64Array::from(values))
    } else {
        let grids: Vec<Vec<Option<f64>>> = outcomes
            .into_iter()
            .map(|o| match o {
                Outcome::Grid(g) => g,
                Outcome::Scalar(_) => unreachable!(),
            })
            .collect();
        Arc::new(step_grid_array(&grids, query.grid()))
    };
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![labels, result],
    )?)
}

// ---------------------------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------------------------

struct RangeVectorStream<L: Layout> {
    input: SendableRecordBatchStream,
    query: Query,
    schema: SchemaRef,
    /// Per-partition layout state: whatever a series that crosses batches needs carried.
    layout: L,
    done: bool,
}

impl<L: Layout> RangeVectorStream<L> {
    fn process(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let rows: Vec<(L::Labels, Outcome)> = self
            .layout
            .series(batch)
            .into_iter()
            .filter_map(|s| compute(&self.query, &s.ts, &s.vs).map(|o| (s.labels, o)))
            .collect();
        output_batch::<L>(&self.schema, &self.query, batch, rows)
    }

    /// End of the partition: whatever the layout still holds is complete.
    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        let Some(s) = self.layout.finish() else {
            return Ok(None);
        };
        let Some(o) = compute(&self.query, &s.ts, &s.vs) else {
            return Ok(None);
        };
        let empty = RecordBatch::new_empty(Arc::clone(&self.schema));
        let out = output_batch::<L>(&self.schema, &self.query, &empty, vec![(s.labels, o)])?;
        Ok(Some(out))
    }
}

impl<L: Layout> Stream for RangeVectorStream<L> {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.done {
                return Poll::Ready(None);
            }
            match this.input.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(batch))) => match this.process(&batch) {
                    Ok(out) if out.num_rows() == 0 => continue,
                    other => return Poll::Ready(Some(other)),
                },
                Poll::Ready(None) => {
                    this.done = true;
                    return Poll::Ready(this.finish().transpose());
                }
            }
        }
    }
}

impl<L: Layout> RecordBatchStream for RangeVectorStream<L> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
