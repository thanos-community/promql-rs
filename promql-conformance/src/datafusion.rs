//! The DataFusion engine behind the harness's [`Engine`] seam.
//!
//! Each case seeds a fresh [`MemorySeriesSource`] from its `load` blocks
//! — the same series descriptions the oracle parses from the raw text —
//! and runs the query through `promql_engine`. Error mapping is the whole
//! of the logic here, and it matters: a query the engine rejects as
//! invalid is an *answer* (Prometheus rejects queries too, and both
//! erroring is a match), an unsupported feature is "not built yet", and
//! anything else is a bug to be reported as one.

use std::collections::BTreeMap;
use std::sync::Arc;

use promql_engine::{MemorySeriesSource, RangeQuery};

use crate::result::{Engine, EngineError, LoadedSeries, Point, QueryResult, Series};

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

/// Flatten several `load` blocks into one store.
///
/// `MemorySeriesSource::from_descriptions` takes a single interval, so
/// the cross-block merge happens here rather than in `promql-engine`:
/// this is test-fixture shaping, and the engine has no business knowing
/// that a script can load twice at different resolutions.
///
/// Sample *i* of a block is at `i * interval_ms(block)`, matching what
/// `from_descriptions` does within one block. Where two blocks write the
/// same label set at the same timestamp the later one wins, which is the
/// order an appender would have applied them in.
fn merge(load: &[LoadedSeries<'_>]) -> MemorySeriesSource {
    let mut merged: BTreeMap<Vec<(&str, &str)>, BTreeMap<i64, f64>> = BTreeMap::new();

    for block in load {
        let interval_ms = (block.interval_secs * 1000.0).round() as i64;
        for sd in block.series {
            // The parser keeps labels as written, possibly with a name
            // repeated; sorted by name, the last value wins.
            let mut pairs: Vec<(&str, &str)> = sd
                .labels
                .iter()
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            let mut labels: Vec<(&str, &str)> = Vec::with_capacity(pairs.len());
            for p in pairs {
                match labels.last_mut() {
                    Some(last) if last.0 == p.0 => last.1 = p.1,
                    _ => labels.push(p),
                }
            }

            let samples = merged.entry(labels).or_default();
            for (i, v) in sd.values.iter().enumerate().filter(|(_, v)| !v.omitted) {
                samples.insert(i as i64 * interval_ms, v.value);
            }
        }
    }

    let stored = merged
        .into_iter()
        .map(|(labels, samples)| {
            let (timestamps, values) = samples.into_iter().unzip();
            promql_engine::Series::new(&labels, timestamps, values)
                .expect("a sorted map yields ascending timestamps and unique names")
        })
        .collect();
    MemorySeriesSource::new(stored)
}

impl Engine for DataFusionEngine {
    fn range_query(
        &self,
        load: &[LoadedSeries<'_>],
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<QueryResult, EngineError> {
        let source = Arc::new(merge(load));
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

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::SeriesDescription;

    fn desc(line: &str) -> SeriesDescription {
        promql_parser::parse_series_desc(line).expect("parses")
    }

    fn find<'a>(source: &'a MemorySeriesSource, name: &str) -> &'a promql_engine::Series {
        source
            .series()
            .iter()
            .find(|s| s.labels().any(|(k, v)| k == "__name__" && v == name))
            .unwrap_or_else(|| panic!("no series named {name}"))
    }

    /// The reason the trait takes a slice rather than one block: a
    /// script can load at several resolutions, and each block's samples
    /// are spaced by its own interval.
    #[test]
    fn each_block_keeps_its_own_interval() {
        let slow = vec![desc("slow 1 2 3")];
        let fast = vec![desc("fast 1 2 3")];
        let source = merge(&[
            LoadedSeries {
                series: &slow,
                interval_secs: 60.0,
            },
            LoadedSeries {
                series: &fast,
                interval_secs: 10.0,
            },
        ]);

        assert_eq!(find(&source, "slow").timestamps(), [0, 60_000, 120_000]);
        assert_eq!(find(&source, "fast").timestamps(), [0, 10_000, 20_000]);
    }

    #[test]
    fn blocks_accumulate_rather_than_replace() {
        let first = vec![desc("a 1")];
        let second = vec![desc("b 2")];
        let source = merge(&[
            LoadedSeries {
                series: &first,
                interval_secs: 60.0,
            },
            LoadedSeries {
                series: &second,
                interval_secs: 60.0,
            },
        ]);
        assert_eq!(source.series().len(), 2);
    }

    /// Two blocks writing the same series at the same timestamp: the
    /// later one wins, as it would through an appender.
    #[test]
    fn a_later_block_overwrites_the_same_timestamp() {
        let first = vec![desc("a 1 1 1")];
        let second = vec![desc("a 9")];
        let source = merge(&[
            LoadedSeries {
                series: &first,
                interval_secs: 60.0,
            },
            LoadedSeries {
                series: &second,
                interval_secs: 60.0,
            },
        ]);
        assert_eq!(source.series().len(), 1);
        assert_eq!(find(&source, "a").values(), [9.0, 1.0, 1.0]);
    }

    #[test]
    fn no_blocks_is_an_empty_store() {
        assert!(merge(&[]).series().is_empty());
    }
}
