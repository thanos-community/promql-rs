//! The DataFusion engine behind the harness's [`Engine`] seam.
//!
//! Each case seeds a fresh [`MemorySeriesSource`] from its `load` block —
//! the same series descriptions the oracle parses from the raw text — and
//! runs the query through `promql_engine`. Error mapping is the whole of
//! the logic here, and it matters: a query the engine rejects as invalid
//! is an *answer* (Prometheus rejects queries too, and both erroring is a
//! match), an unsupported feature is "not built yet", and anything else
//! is a bug to be reported as one.

use std::sync::Arc;

use promql_engine::{MemorySeriesSource, RangeQuery};
use promql_parser::SeriesDescription;

use crate::result::{Engine, EngineError, Point, QueryResult, Series};

pub struct DataFusionEngine {
    inner: promql_engine::Engine,
}

impl DataFusionEngine {
    pub fn new() -> Result<Self, EngineError> {
        Ok(Self {
            inner: promql_engine::Engine::blocking()
                .map_err(|e| EngineError::Other(e.to_string()))?,
        })
    }
}

impl Engine for DataFusionEngine {
    fn range_query(
        &self,
        series: &[SeriesDescription],
        interval_secs: f64,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<QueryResult, EngineError> {
        let source = Arc::new(MemorySeriesSource::from_descriptions(series, interval_secs));
        let range = RangeQuery::new(start_ms, end_ms, step_ms);
        match self.inner.range_query(source, query, &range) {
            Ok(decoded) => Ok(QueryResult::Matrix(
                decoded
                    .iter()
                    .map(|s| Series {
                        labels: s
                            .labels()
                            .map(|(k, v)| (k.to_owned(), v.to_owned()))
                            .collect(),
                        floats: s
                            .timestamps()
                            .iter()
                            .zip(s.values())
                            .map(|(&t, &v)| Point { t, v })
                            .collect(),
                        histograms: 0,
                    })
                    .collect(),
            )),
            Err(promql_engine::EngineError::Unsupported(feature)) => {
                Err(EngineError::Unsupported(feature))
            }
            Err(promql_engine::EngineError::Query(msg)) => Ok(QueryResult::Error(msg)),
            Err(other) => Err(EngineError::Other(other.to_string())),
        }
    }
}
