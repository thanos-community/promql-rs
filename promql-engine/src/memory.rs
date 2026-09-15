//! An in-memory [`SeriesSource`], and the reference implementation of a
//! store's three obligations.
//!
//! It exists for tests, but it is also the executable statement of what a
//! real store has to do: apply the matchers with [`crate::matcher`]'s
//! semantics, keep only samples inside the range, hand over one row per
//! series with samples in timestamp order, in the canonical schema. A
//! store implementer who wants to know "what exactly am I promising" can
//! read `select` below.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use promql_parser::ast::LabelMatcher;

use crate::matcher::{matches_all, CompiledMatcher};
use crate::series::{encode, label_names_of, Series};
use crate::source::{SelectHints, SeriesSource};

#[derive(Debug, Default)]
pub struct MemorySeriesSource {
    series: Vec<Series>,
}

impl MemorySeriesSource {
    /// Hold these series. Each [`Series`] was checked when it was built,
    /// so there is nothing to validate here; two series with the same
    /// label set are the caller's mistake and would select as two rows.
    pub fn new(series: Vec<Series>) -> Self {
        Self { series }
    }

    pub fn series(&self) -> &[Series] {
        &self.series
    }
}

#[async_trait]
impl SeriesSource for MemorySeriesSource {
    async fn select(
        &self,
        _state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let compiled: Vec<CompiledMatcher> = matchers
            .iter()
            .map(CompiledMatcher::compile)
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Obligation 1, filter: every matcher on every series, then the
        // range on every sample. Clipping yields a new series that shares
        // the stored buffers.
        let selected: Vec<Series> = self
            .series
            .iter()
            .filter(|s| matches_all(&compiled, s))
            .map(|s| s.clip(hints.start_ms, hints.end_ms))
            .collect();

        // Obligations 2 and 3, partition and order: one row per series,
        // samples ascending as they were stored. The schema is the union
        // of the selected series' label names.
        let batch =
            encode(&label_names_of(&selected), &selected).map_err(DataFusionError::Internal)?;
        let schema = batch.schema();
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            None,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use promql_parser::ast::MatchOp;
    use promql_parser::posrange::PositionRange;

    fn matcher(name: &str, op: MatchOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            name: name.into(),
            op,
            value: value.into(),
            pos_range: PositionRange::default(),
        }
    }

    /// A counter scraped every 30s.
    fn counter(labels: &[(&str, &str)], start: f64, step: f64, n: usize) -> Series {
        let timestamps = (0..n).map(|i| i as i64 * 30_000).collect();
        let values = (0..n).map(|i| start + i as f64 * step).collect();
        Series::new(labels, timestamps, values).unwrap()
    }

    fn source() -> MemorySeriesSource {
        MemorySeriesSource::new(vec![
            counter(
                &[("__name__", "http_requests_total"), ("pod", "nginx-1")],
                1.0,
                1.0,
                4,
            ),
            counter(
                &[
                    ("__name__", "http_requests_total"),
                    ("pod", "nginx-2"),
                    ("route", "/"),
                ],
                1.0,
                2.0,
                4,
            ),
            counter(&[("__name__", "other")], 5.0, 0.0, 1),
        ])
    }

    #[tokio::test]
    async fn selects_by_matchers_and_clips_the_range() {
        let ctx = SessionContext::new();
        let plan = source()
            .select(
                &ctx.state(),
                &[matcher("__name__", MatchOp::Equal, "http_requests_total")],
                SelectHints::range(30_000, 60_000),
            )
            .await
            .unwrap();
        crate::series::validate(&plan.schema()).unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let decoded = crate::series::decode(&batches).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].timestamps(), [30_000, 60_000]);
        assert_eq!(decoded[0].values(), [2.0, 3.0]);
        assert_eq!(decoded[1].timestamps(), [30_000, 60_000]);
        assert_eq!(decoded[1].values(), [3.0, 5.0]);
        // `route` is in the schema because nginx-2 has it, and absent from
        // nginx-1's label set because its row holds "".
        assert!(decoded[0].labels().all(|(n, _)| n != "route"));
        assert_eq!(decoded[0].label("route"), "");
        assert_eq!(decoded[1].label("route"), "/");
    }

    #[tokio::test]
    async fn an_empty_selection_is_a_valid_empty_plan() {
        let ctx = SessionContext::new();
        let plan = source()
            .select(
                &ctx.state(),
                &[matcher("pod", MatchOp::Equal, "nginx-3")],
                SelectHints::range(0, 1_000_000),
            )
            .await
            .unwrap();
        crate::series::validate(&plan.schema()).unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert!(crate::series::decode(&batches).unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_invalid_regex_fails_the_selection() {
        let ctx = SessionContext::new();
        let err = source()
            .select(
                &ctx.state(),
                &[matcher("pod", MatchOp::RegexEqual, "(")],
                SelectHints::range(0, 1_000_000),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("regular expression"), "{err}");
    }
}
