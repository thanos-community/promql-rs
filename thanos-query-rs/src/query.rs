//! What the handlers query: a per-request [`Queryable`] over a
//! `SeriesSource`, made by a [`QueryableCreator`], Thanos's
//! `query.QueryableCreator`. One implementation fans out to the stores,
//! one answers from memory for tests.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use promql_engine::matcher::{matches_all, CompiledMatcher};
use promql_engine::{MemorySeriesSource, SeriesSource};
use promql_parser::ast::LabelMatcher;
use thanos_store::{LabelsResult, ProxyStore, SelectOptions, StoreError, ThanosSeriesSource};

use crate::api::ApiError;

/// One request's view of the data.
#[async_trait]
pub trait Queryable: Send + Sync {
    /// The source the engine evaluates against.
    fn source(&self) -> &dyn SeriesSource;

    /// Warnings the source gathered so far, drained.
    fn warnings(&self) -> Vec<String>;

    /// The label names of the series matching `matchers` (all series when
    /// empty) with samples in `[start_ms, end_ms]`, sorted.
    async fn label_names(
        &self,
        start_ms: i64,
        end_ms: i64,
        matchers: &[LabelMatcher],
    ) -> Result<LabelsResult, ApiError>;

    /// The values of label `name` over the same selection, sorted.
    async fn label_values(
        &self,
        name: &str,
        start_ms: i64,
        end_ms: i64,
        matchers: &[LabelMatcher],
    ) -> Result<LabelsResult, ApiError>;
}

/// Makes a [`Queryable`] per request from the request's options.
pub trait QueryableCreator: Send + Sync + fmt::Debug {
    fn queryable(&self, options: SelectOptions) -> Box<dyn Queryable>;
}

/// The real thing: every request fans out to the Thanos stores.
#[derive(Debug)]
pub struct ThanosQueryableCreator {
    proxy: Arc<ProxyStore>,
}

impl ThanosQueryableCreator {
    pub fn new(proxy: Arc<ProxyStore>) -> Self {
        Self { proxy }
    }
}

impl QueryableCreator for ThanosQueryableCreator {
    fn queryable(&self, options: SelectOptions) -> Box<dyn Queryable> {
        Box::new(ThanosQueryable {
            source: ThanosSeriesSource::new(Arc::clone(&self.proxy), options),
        })
    }
}

struct ThanosQueryable {
    source: ThanosSeriesSource,
}

/// Go returns `ErrorExec` for a failing label lookup; an invalid regex is
/// the client's fault.
fn store_error(err: StoreError) -> ApiError {
    match err {
        StoreError::Matcher(e) => ApiError::bad_data(e.to_string()),
        other => ApiError::exec(other.to_string()),
    }
}

#[async_trait]
impl Queryable for ThanosQueryable {
    fn source(&self) -> &dyn SeriesSource {
        &self.source
    }

    fn warnings(&self) -> Vec<String> {
        self.source.take_warnings()
    }

    async fn label_names(
        &self,
        start_ms: i64,
        end_ms: i64,
        matchers: &[LabelMatcher],
    ) -> Result<LabelsResult, ApiError> {
        self.source
            .proxy()
            .label_names(start_ms, end_ms, matchers, self.source.options())
            .await
            .map_err(store_error)
    }

    async fn label_values(
        &self,
        name: &str,
        start_ms: i64,
        end_ms: i64,
        matchers: &[LabelMatcher],
    ) -> Result<LabelsResult, ApiError> {
        self.source
            .proxy()
            .label_values(name, start_ms, end_ms, matchers, self.source.options())
            .await
            .map_err(store_error)
    }
}

/// Answers from a fixed set of series; for the API tests.
#[derive(Debug)]
pub struct MemoryQueryableCreator {
    source: Arc<MemorySeriesSource>,
}

impl MemoryQueryableCreator {
    pub fn new(source: MemorySeriesSource) -> Self {
        Self {
            source: Arc::new(source),
        }
    }
}

impl QueryableCreator for MemoryQueryableCreator {
    fn queryable(&self, _options: SelectOptions) -> Box<dyn Queryable> {
        Box::new(MemoryQueryable {
            source: Arc::clone(&self.source),
        })
    }
}

struct MemoryQueryable {
    source: Arc<MemorySeriesSource>,
}

impl MemoryQueryable {
    /// The stored series matching `matchers` with a sample in range.
    fn select(
        &self,
        start_ms: i64,
        end_ms: i64,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<&promql_engine::Series>, ApiError> {
        let compiled: Vec<CompiledMatcher> = matchers
            .iter()
            .map(CompiledMatcher::compile)
            .collect::<Result<_, _>>()
            .map_err(|e| ApiError::bad_data(e.to_string()))?;
        Ok(self
            .source
            .series()
            .iter()
            .filter(|s| matches_all(&compiled, s))
            .filter(|s| s.timestamps().iter().any(|&t| t >= start_ms && t <= end_ms))
            .collect())
    }
}

#[async_trait]
impl Queryable for MemoryQueryable {
    fn source(&self) -> &dyn SeriesSource {
        self.source.as_ref()
    }

    fn warnings(&self) -> Vec<String> {
        Vec::new()
    }

    async fn label_names(
        &self,
        start_ms: i64,
        end_ms: i64,
        matchers: &[LabelMatcher],
    ) -> Result<LabelsResult, ApiError> {
        let names: BTreeSet<String> = self
            .select(start_ms, end_ms, matchers)?
            .iter()
            .flat_map(|s| s.labels().map(|(n, _)| n.to_string()))
            .collect();
        Ok(LabelsResult {
            values: names.into_iter().collect(),
            warnings: Vec::new(),
        })
    }

    async fn label_values(
        &self,
        name: &str,
        start_ms: i64,
        end_ms: i64,
        matchers: &[LabelMatcher],
    ) -> Result<LabelsResult, ApiError> {
        let values: BTreeSet<String> = self
            .select(start_ms, end_ms, matchers)?
            .iter()
            .map(|s| s.label(name))
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .collect();
        Ok(LabelsResult {
            values: values.into_iter().collect(),
            warnings: Vec::new(),
        })
    }
}
