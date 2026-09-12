//! How a store hands series to the engine.
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
//! 2. **Partition.** A row holds one series' samples, and within a batch a
//!    series has one row. Today a series is whole in its row; splitting a
//!    long series across batches is left open, see the non-goals in
//!    `docs/series-source.md`.
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

/// What the engine wants from a scan, beyond the matchers: Prometheus's
/// `storage.SelectHints`, typed.
///
/// Only the range is binding. Both bounds are inclusive milliseconds and
/// already account for lookback, `offset`, `@` and range windows, so the
/// store does not need to know any PromQL to honour them. Everything else
/// is advisory: a store may use it to read less or to lay its output out
/// better, and may ignore it without changing the result.
///
/// Prometheus's `Limit` and `DisableTrimming` are left out: the first
/// serves its label-values API, the second its own chunk trimming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectHints {
    pub start_ms: i64,
    pub end_ms: i64,
    /// Step of the enclosing range query; `None` for an instant query. A
    /// store may read downsampled or step-aligned data.
    pub step_ms: Option<i64>,
    /// Window of a range selector such as `[5m]`; `None` for an instant
    /// selector. A store may prune chunks per window.
    pub range_ms: Option<i64>,
    /// The function or aggregation directly above the selector, by its
    /// PromQL name (`rate`, `sum`); `None` when there is none.
    /// `count_over_time` needs no values; `rate` needs whole windows.
    pub func: Option<String>,
    /// `by`/`without` of the aggregation directly above the selector. With
    /// `sum by (route)` only `route` decides the output, so a store may
    /// drop the other labels, and if it partitions its plan by the
    /// grouping labels the engine aggregates without a shuffle.
    pub grouping: Option<Grouping>,
    /// Return only shard `index` of `count` of the matching series, for
    /// scanning the series space in parallel; `None` for all of them.
    pub shard: Option<Shard>,
}

impl SelectHints {
    /// Just the range; every advisory hint absent.
    pub fn range(start_ms: i64, end_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            step_ms: None,
            range_ms: None,
            func: None,
            grouping: None,
            shard: None,
        }
    }
}

/// `by (labels…)` or `without (labels…)` of an enclosing aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grouping {
    pub labels: Vec<String>,
    /// `true` for `by`, `false` for `without`.
    pub by: bool,
}

/// One of `count` equal parts of the series space, by series identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard {
    pub index: u64,
    pub count: u64,
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
