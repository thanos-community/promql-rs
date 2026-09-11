//! The seam: how a store hands series to the engine.
//!
//! [`SeriesSource`] mirrors Prometheus's `storage.Querier.Select`, which
//! every PromQL-on-something implementation has ended up writing. The
//! engine asks in PromQL terms — label matchers and a millisecond range —
//! and the store answers with a DataFusion [`ExecutionPlan`] in the
//! [canonical schema](crate::series). What happens inside `select` is the
//! store's business: a parquet scan, a vortex scan, a gRPC call, a
//! `GROUP BY` that nests samples into lists. This crate never looks.
//!
//! Three obligations come with the answer, and the engine assumes all of
//! them rather than checking per sample:
//!
//! 1. **Filter.** Every series matches every matcher; every sample lies
//!    inside the range.
//! 2. **Partition.** One row is one whole series. A series is never split
//!    across rows.
//! 3. **Order.** Samples ascend by timestamp within a series.
//!
//! [`SelectorTable`] then makes a `select` result look like an ordinary
//! table to DataFusion, so a selector is a `TableScan` leaf and everything
//! above it — `EXPLAIN`, the optimizer, later operators — is stock.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use promql_parser::ast::LabelMatcher;

use crate::error::EngineError;
use crate::series;

/// What the engine wants from a scan, beyond the matchers.
///
/// Both bounds are inclusive milliseconds and already account for
/// lookback, `offset`, `@` and range windows: the store does not need to
/// know any PromQL to honour them. Named after Prometheus's `SelectHints`;
/// more fields (step, function) arrive as operators need them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectHints {
    pub start_ms: i64,
    pub end_ms: i64,
    /// The aggregation directly above this selector, if any. Advisory: a
    /// store that partitions its output by these labels and declares
    /// `Partitioning::Hash` on its plan lets DataFusion aggregate in one
    /// phase without a shuffle. A store may ignore it entirely.
    pub grouping: Option<Grouping>,
}

impl SelectHints {
    pub fn range(start_ms: i64, end_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            grouping: None,
        }
    }
}

/// `by (labels…)` or `without (labels…)` of an enclosing aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grouping {
    pub labels: Vec<String>,
    pub without: bool,
}

/// A store of series, as the engine sees it.
#[async_trait]
pub trait SeriesSource: fmt::Debug + Send + Sync {
    /// Every series matching all of `matchers`, carrying only the samples
    /// within `hints`, as a plan whose schema passes [`series::validate`].
    ///
    /// `matchers` arrive verbatim from the parser, including `__name__`
    /// (synthesized from a bare metric name). An absent label compares as
    /// `""`, and regexes are anchored to the whole value; see
    /// [`crate::matcher`] for the reference semantics a store is held to.
    async fn select(
        &self,
        state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// One selector's scan, as a DataFusion table.
///
/// Built eagerly: `select` is called once at plan time and its result
/// held, because a [`TableProvider`] must report its schema before any
/// scan and the schema — the label names — is only known once the store
/// has found the matching series. That is also how Prometheus works: it
/// expands every selector's series before evaluating anything. A
/// consequence worth knowing is that the store's own planning happens
/// while the engine is planning, not while it is executing.
///
/// Public so that a store which implements [`SeriesSource`] can register
/// itself as a SQL table for debugging: `SELECT labels, samples FROM …`.
#[derive(Debug)]
pub struct SelectorTable {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl SelectorTable {
    /// Ask `source` for the selection and validate what comes back.
    pub async fn try_new(
        state: &dyn Session,
        source: &dyn SeriesSource,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> std::result::Result<Self, EngineError> {
        let plan = source.select(state, matchers, hints).await?;
        let schema = plan.schema();
        series::validate(&schema).map_err(EngineError::Schema)?;
        Ok(Self { plan, schema })
    }

    /// The label names this selection carries.
    pub fn label_names(&self) -> Vec<String> {
        series::label_names(&self.schema)
    }
}

#[async_trait]
impl TableProvider for SelectorTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(cols) = projection else {
            return Ok(Arc::clone(&self.plan));
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
        Ok(Arc::new(ProjectionExec::try_new(
            exprs,
            Arc::clone(&self.plan),
        )?))
    }
}
