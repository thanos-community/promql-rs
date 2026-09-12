//! The engine's view of the stores: a [`SeriesSource`] over a
//! [`ProxyStore`], built once per HTTP request so warnings have somewhere
//! to go.

use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::matcher::CompiledMatcher;
use promql_engine::series::{encode, label_names_of};
use promql_engine::{SelectHints, SeriesSource};
use promql_parser::ast::LabelMatcher;

use crate::proxy::ProxyStore;
use crate::storepb::thanos::PartialResponseStrategy;

/// Per-request knobs of the Prometheus API that reach the stores.
#[derive(Debug, Clone)]
pub struct SelectOptions {
    /// `partial_response`: whether a failing store costs the query or
    /// only earns a warning.
    pub partial_response: PartialResponseStrategy,
    /// `storeMatch[]`: query only stores whose `__address__` satisfies
    /// one of these selectors; empty means all of them.
    pub store_matchers: Vec<Vec<CompiledMatcher>>,
    /// Re-check every returned series against the selector and drop, with
    /// a warning, what does not match. Guards the engine's filter
    /// obligation against a misbehaving store.
    pub verify_matchers: bool,
}

impl SelectOptions {
    /// Whether one failing store fails the whole request.
    pub fn aborts(&self) -> bool {
        self.partial_response == PartialResponseStrategy::Abort
    }
}

impl Default for SelectOptions {
    fn default() -> Self {
        Self {
            partial_response: PartialResponseStrategy::Warn,
            store_matchers: Vec::new(),
            verify_matchers: true,
        }
    }
}

/// A [`SeriesSource`] that fans every selector out to the Thanos stores.
///
/// Warnings from the stores accumulate here across the selectors of one
/// query; the HTTP layer drains them with [`Self::take_warnings`] for the
/// response envelope.
#[derive(Debug)]
pub struct ThanosSeriesSource {
    proxy: Arc<ProxyStore>,
    options: SelectOptions,
    warnings: Mutex<Vec<String>>,
}

impl ThanosSeriesSource {
    pub fn new(proxy: Arc<ProxyStore>, options: SelectOptions) -> Self {
        Self {
            proxy,
            options,
            warnings: Mutex::new(Vec::new()),
        }
    }

    pub fn options(&self) -> &SelectOptions {
        &self.options
    }

    /// The warnings gathered so far, leaving none behind.
    pub fn take_warnings(&self) -> Vec<String> {
        std::mem::take(&mut *self.warnings.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

#[async_trait]
impl SeriesSource for ThanosSeriesSource {
    async fn select(
        &self,
        _state: &dyn Session,
        matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = self
            .proxy
            .series(matchers, &hints, &self.options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        self.warnings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(result.warnings);

        // The proxy has already filtered, merged into one row per label
        // set and ordered the samples, so the batch is the plan.
        let batch = encode(&label_names_of(&result.series), &result.series)
            .map_err(DataFusionError::Internal)?;
        let schema = batch.schema();
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            None,
        )?)
    }
}
