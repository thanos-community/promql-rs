//! An in-memory [`SeriesSource`], and the reference implementation of a
//! store's three obligations.
//!
//! It exists for tests — the conformance suite seeds it from the corpus's
//! `load` blocks — but it is also the executable statement of what a real
//! store has to do: apply the matchers with [`crate::matcher`]'s
//! semantics, keep only samples inside the range, hand over one row per
//! series with samples in timestamp order, in the canonical schema. A
//! store implementer who wants to know "what exactly am I promising" can
//! read `select` below.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use promql_parser::ast::{LabelMatcher, SeriesDescription};

use crate::matcher::{matches_all, CompiledMatcher};
use crate::series::SeriesBatchBuilder;
use crate::source::{SelectHints, SeriesSource};

/// One stored series.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredSeries {
    pub labels: BTreeMap<String, String>,
    /// Sorted ascending by timestamp.
    pub samples: Vec<(i64, f64)>,
}

#[derive(Debug, Default)]
pub struct MemorySeriesSource {
    series: Vec<StoredSeries>,
}

impl MemorySeriesSource {
    pub fn new(series: Vec<StoredSeries>) -> Self {
        Self { series }
    }

    /// Seed from promqltest series descriptions the way upstream's
    /// `load` does: value `i` sits at `i * interval` from the epoch, an
    /// omitted value (`_`) emits no sample, and `stale` is already a
    /// StaleNaN payload courtesy of the parser.
    pub fn from_descriptions(series: &[SeriesDescription], interval_secs: f64) -> Self {
        let interval_ms = (interval_secs * 1000.0).round() as i64;
        let stored = series
            .iter()
            .map(|sd| StoredSeries {
                labels: sd
                    .labels
                    .iter()
                    .map(|l| (l.name.clone(), l.value.clone()))
                    .collect(),
                samples: sd
                    .values
                    .iter()
                    .enumerate()
                    .filter(|(_, v)| !v.omitted)
                    .map(|(i, v)| (i as i64 * interval_ms, v.value))
                    .collect(),
            })
            .collect();
        Self::new(stored)
    }

    pub fn series(&self) -> &[StoredSeries] {
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
        // range on every sample.
        let selected: Vec<(&StoredSeries, Vec<(i64, f64)>)> = self
            .series
            .iter()
            .filter(|s| matches_all(&compiled, &s.labels))
            .map(|s| {
                let samples = s
                    .samples
                    .iter()
                    .copied()
                    .filter(|(t, _)| hints.start_ms <= *t && *t <= hints.end_ms)
                    .collect();
                (s, samples)
            })
            .collect();

        // The schema is the union of the selected series' label names.
        let names: BTreeSet<String> = selected
            .iter()
            .flat_map(|(s, _)| s.labels.keys().cloned())
            .collect();
        let names: Vec<String> = names.into_iter().collect();

        // Obligations 2 and 3, partition and order: the builder takes one
        // whole series per push, and the stored samples are already sorted.
        let mut builder = SeriesBatchBuilder::new(&names);
        for (s, samples) in &selected {
            builder
                .push(&s.labels, samples)
                .map_err(DataFusionError::Internal)?;
        }
        let schema = builder.schema();
        let batch = builder.finish();
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

    fn source() -> MemorySeriesSource {
        let load = [
            r#"http_requests_total{pod="nginx-1"} 1+1x3"#,
            r#"http_requests_total{pod="nginx-2", route="/"} 1+2x3"#,
            r#"other 5"#,
        ];
        let series: Vec<SeriesDescription> = load
            .iter()
            .map(|l| promql_parser::parse_series_desc(l).unwrap())
            .collect();
        MemorySeriesSource::from_descriptions(&series, 30.0)
    }

    #[tokio::test]
    async fn selects_by_matchers_and_clips_the_range() {
        let ctx = SessionContext::new();
        let plan = source()
            .select(
                &ctx.state(),
                &[matcher("__name__", MatchOp::Equal, "http_requests_total")],
                SelectHints {
                    start_ms: 30_000,
                    end_ms: 60_000,
                },
            )
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let decoded = crate::series::decode(&batches).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].samples, vec![(30_000, 2.0), (60_000, 3.0)]);
        assert_eq!(decoded[1].samples, vec![(30_000, 3.0), (60_000, 5.0)]);
        // `route` is in the schema because nginx-2 has it, and absent from
        // nginx-1's decoded labels because it decodes as "".
        assert!(!decoded[0].labels.contains_key("route"));
        assert_eq!(decoded[1].labels["route"], "/");
    }

    #[tokio::test]
    async fn an_empty_selection_is_a_valid_empty_plan() {
        let ctx = SessionContext::new();
        let plan = source()
            .select(
                &ctx.state(),
                &[matcher("pod", MatchOp::Equal, "nginx-3")],
                SelectHints {
                    start_ms: 0,
                    end_ms: 1_000_000,
                },
            )
            .await
            .unwrap();
        crate::series::validate(&plan.schema()).unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert!(crate::series::decode(&batches).unwrap().is_empty());
    }
}
