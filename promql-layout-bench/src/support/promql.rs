//! Phase 2, the PromQL layer above the range vector, as DataFusion logical plans.
//!
//! Nothing here is SQL. The eventual engine parses PromQL and lowers it to logical plans, so the
//! benchmark builds the same plans with the DataFrame API and nothing else.
//!
//! The range-vector step, `rate(m[w])`, produces a frame with one row per series and the labels
//! alongside either a scalar or a step grid; [`super::dispatch::rate_frame`] builds it. Everything
//! above that frame is here and is shared: [`plan`] is the same code whichever layout produced the
//! frame. That is the whole comparison in one sentence: **the only thing that varies is how the
//! series were found.**

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::datasource::MemTable;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{col, lit, ExprFunctionExt};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};

use super::chunking::Built;
use super::format::{
    COL_GRID, COL_LABELS, COL_VALUE, GROUP_LABEL, STEP_TS, STEP_VALUE, UNIQUE_LABEL,
};

/// A session that plans for `target_partitions` cores.
pub fn session(target_partitions: usize) -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(target_partitions.max(1)),
    )
}

/// The scan as a table, one DataFusion partition per built partition. No ordering is declared:
/// phase 1 owes contiguity, not order, and DataFusion has no property for "clustered but unsorted".
pub fn scan_table(built: &Built) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(MemTable::try_new(
        Arc::clone(&built.schema),
        built.partitions.clone(),
    )?))
}

/// What PromQL asks for on top of the range vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    /// `sum by (code) (rate(m[w]))` at one instant. One value per code, no unnest.
    InstantSum,
    /// The same across a step grid, one value per `(code, step)`. `sum by (code)` combines
    /// different series *at the same step*, so the grid has to become rows first.
    RangeSum,
    /// `topk(k, rate(m[w]))` across a step grid. Ranks series at each step, so it cannot be fused
    /// into a per-series accumulator and the unnest is unavoidable.
    RangeTopk { k: usize },
}

impl Plan {
    pub fn is_instant(&self) -> bool {
        matches!(self, Plan::InstantSum)
    }

    pub fn label(&self) -> String {
        match self {
            Plan::InstantSum => "sum by (code)".into(),
            Plan::RangeSum => "sum by (code), range".into(),
            Plan::RangeTopk { k } => format!("topk({k}), range"),
        }
    }
}

/// One row per `(series, step)`: the step grid unnested, with the labels the plans need.
pub fn unnested(rate: DataFrame) -> Result<DataFrame> {
    rate.select(vec![
        col(COL_LABELS).field(GROUP_LABEL).alias(GROUP_LABEL),
        col(COL_LABELS).field(UNIQUE_LABEL).alias(UNIQUE_LABEL),
        col(COL_GRID),
    ])?
    .unnest_columns(&[COL_GRID])?
    .select(vec![
        col(GROUP_LABEL),
        col(UNIQUE_LABEL),
        col(COL_GRID).field(STEP_TS).alias(STEP_TS),
        col(COL_GRID).field(STEP_VALUE).alias(STEP_VALUE),
    ])
}

/// The rest of the query, above the range vector. Identical for every layout.
pub fn plan(rate: DataFrame, plan: Plan) -> Result<DataFrame> {
    let asc = |name: &str| col(name).sort(true, true);
    match plan {
        Plan::InstantSum => rate
            .aggregate(
                vec![col(COL_LABELS).field(GROUP_LABEL).alias(GROUP_LABEL)],
                vec![sum(col(COL_VALUE)).alias("total")],
            )?
            .sort(vec![asc(GROUP_LABEL)]),
        Plan::RangeSum => unnested(rate)?
            .aggregate(
                vec![col(GROUP_LABEL), col(STEP_TS)],
                vec![sum(col(STEP_VALUE)).alias("total")],
            )?
            .sort(vec![asc(GROUP_LABEL), asc(STEP_TS)]),
        Plan::RangeTopk { k } => {
            let rank = row_number()
                .partition_by(vec![col(STEP_TS)])
                .order_by(vec![col(STEP_VALUE).sort(false, true), asc(UNIQUE_LABEL)])
                .build()?
                .alias("rn");
            unnested(rate)?
                .window(vec![rank])?
                .filter(col("rn").lt_eq(lit(k as u64)))?
                .sort(vec![asc(STEP_TS), asc("rn")])?
                .select(vec![col(UNIQUE_LABEL), col(STEP_TS), col(STEP_VALUE)])
        }
    }
}

/// Read result rows as `(key, total)` pairs, where the key is every column but the last joined
/// together and the total is the last column. Keys come back dictionary encoded or plain depending
/// on the plan, so both are normalised to strings.
pub fn read_groups(batches: &[RecordBatch]) -> Vec<(String, f64)> {
    let mut rows = Vec::new();
    for b in batches {
        let last = b.num_columns() - 1;
        let keys: Vec<ArrayRef> = (0..last)
            .map(|c| cast(b.column(c), &DataType::Utf8).expect("cast group key to Utf8"))
            .collect();
        let totals = b.column(last).as_primitive::<Float64Type>();
        for i in 0..b.num_rows() {
            let key = keys
                .iter()
                .map(|k| k.as_string::<i32>().value(i).to_string())
                .collect::<Vec<_>>()
                .join("/");
            rows.push((key, totals.value(i)));
        }
    }
    rows
}
