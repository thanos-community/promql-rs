//! The shape both implementations produce, and the seam the Rust engine
//! will plug into.
//!
//! Both sides of a differential test have to speak one vocabulary before
//! anything can be compared, so [`QueryResult`] is deliberately the
//! oracle's wire format decoded into Rust terms rather than anything
//! engine-specific.

use std::collections::BTreeMap;

use promql_parser::SeriesDescription;
use serde::Deserialize;

/// A label set. `BTreeMap` because comparison must be order-insensitive
/// and upstream's `labels.New` sorts by name anyway, so this matches Go
/// without a separate sorting step.
pub type Labels = BTreeMap<String, String>;

/// One float sample.
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    pub t: i64,
    pub v: f64,
}

/// One matrix row.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub labels: Labels,
    pub floats: Vec<Point>,
    /// Native-histogram points, which the protocol does not encode yet.
    /// Non-zero means the case is out of reach rather than mismatched.
    pub histograms: usize,
}

/// One vector element.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub labels: Labels,
    pub t: i64,
    pub v: f64,
    pub histogram: bool,
}

/// The result of a range query.
///
/// A range query always yields a matrix in practice; the other variants
/// exist because the comparer must handle whatever the oracle reports
/// rather than assume.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Matrix(Vec<Series>),
    Vector(Vec<Sample>),
    Scalar {
        v: f64,
        t: i64,
    },
    Str {
        v: String,
        t: i64,
    },
    /// The query failed. Comparison counts two errors as a match
    /// regardless of message, matching Go.
    Error(String),
}

impl QueryResult {
    /// Whether any part of this result is a native histogram, which the
    /// protocol cannot carry yet.
    pub fn has_histograms(&self) -> bool {
        match self {
            QueryResult::Matrix(series) => series.iter().any(|s| s.histograms > 0),
            QueryResult::Vector(samples) => samples.iter().any(|s| s.histogram),
            _ => false,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            QueryResult::Matrix(_) => "matrix",
            QueryResult::Vector(_) => "vector",
            QueryResult::Scalar { .. } => "scalar",
            QueryResult::Str { .. } => "string",
            QueryResult::Error(_) => "error",
        }
    }
}

/// A PromQL range-query engine.
///
/// The oracle receives the raw `load` block so that Go performs its own
/// parsing; an implementation of this trait receives the series already
/// parsed, which is what a real engine gets. Turning a
/// [`SeriesDescription`] into timestamped samples is the storage layer's
/// job, not the harness's.
///
/// # Contract
///
/// The oracle configures Prometheus a particular way, and an
/// implementation has to match it or diverge for reasons that have
/// nothing to do with being wrong. From `scripts/promql-oracle`:
///
/// - **Sample timestamps.** Value `i` of a series sits at
///   `i * interval_secs * 1000` ms, counting from 0 — upstream's
///   `testStartTime` is the Unix epoch. An omitted value (`_`) emits no
///   sample at all, and `stale` emits `StaleNaN`
///   (`0x7ff0000000000002`). Note `1+1x40` is 41 points, not 40; see
///   `promql-parser/tests/series_desc.rs`.
/// - **Lookback delta 5m**, Prometheus's default.
/// - **`@` modifier and negative offsets enabled.** 32 and 8 corpus
///   cases respectively need them.
/// - **Experimental functions enabled**, matching what
///   `testcases_test.go` sets before parsing corpus queries.
/// - **`MaxSamples` effectively unlimited** (1e10), so no case should
///   fail on a sample limit.
/// - **Default subquery step 1m**, but unobservable: the corpus contains
///   no stepless subquery, so this cannot be exercised.
/// - **A range query always yields a matrix.** The other
///   [`QueryResult`] variants exist for completeness, not because a
///   range query produces them.
/// - **Error text is not compared**, only the fact of erroring; 11
///   corpus cases legitimately error.
///
/// # Async
///
/// This trait is synchronous because the harness is: `libtest-mimic`
/// trials are plain closures. A DataFusion-backed engine is async, so
/// the implementation should own a runtime and `block_on` inside
/// `range_query` rather than the trait becoming async and forcing every
/// caller to acquire one.
pub trait Engine {
    fn range_query(
        &self,
        series: &[SeriesDescription],
        interval_secs: f64,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<QueryResult, EngineError>;
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The expected state until an execution layer exists. Every
    /// differential test fails with this, and that failure is the
    /// engine's specification.
    #[error("not implemented: no Rust execution engine exists yet")]
    NotImplemented,
    #[error("{0}")]
    Other(String),
}

/// Stand-in engine that fails every query.
///
/// Replace with the real engine once PR #4's `TableProvider` and an
/// execution layer land; the differential suite then starts going green
/// case by case.
pub struct Unimplemented;

impl Engine for Unimplemented {
    fn range_query(
        &self,
        _series: &[SeriesDescription],
        _interval_secs: f64,
        _query: &str,
        _start_ms: i64,
        _end_ms: i64,
        _step_ms: i64,
    ) -> Result<QueryResult, EngineError> {
        Err(EngineError::NotImplemented)
    }
}

// ---------------- oracle wire format ----------------
//
// Floats arrive as strings because JSON numbers cannot represent NaN,
// +Inf or -Inf, all of which the corpus produces. Go writes the shortest
// round-tripping decimal, so parsing recovers the identical bit pattern,
// and Rust's f64 parser accepts Go's "NaN"/"+Inf"/"-Inf" spellings.

#[derive(Debug, Deserialize)]
pub(crate) struct WireResponse {
    pub id: i64,
    pub kind: String,
    #[serde(default)]
    pub series: Vec<WireSeries>,
    #[serde(default)]
    pub samples: Vec<WireSample>,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub t: i64,
    #[serde(default)]
    pub err: String,
    /// Decoded but not yet compared. Go's own comparer currently
    /// discards `PromQLInfo`/`PromQLWarning` annotations
    /// (`discardPromqlAnnotations`, `engine_test.go:4531`), so matching
    /// on them would be stricter than the reference. Carried so that
    /// tightening this later needs no protocol change.
    #[serde(default)]
    #[allow(dead_code)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WireSeries {
    pub labels: Labels,
    #[serde(default)]
    pub floats: Vec<WirePoint>,
    #[serde(default)]
    pub histograms: usize,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WireSample {
    pub labels: Labels,
    pub t: i64,
    pub v: String,
    #[serde(default)]
    pub histogram: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WirePoint {
    pub t: i64,
    pub v: String,
}

/// Parse a float the oracle emitted.
///
/// Rejecting rather than defaulting matters: a silently-zeroed value
/// would turn a protocol bug into a plausible-looking mismatch, which is
/// exactly the confusion the harness self-checks exist to prevent.
pub(crate) fn parse_float(s: &str) -> Result<f64, String> {
    s.parse::<f64>()
        .map_err(|e| format!("cannot parse float {s:?}: {e}"))
}

impl WireResponse {
    pub(crate) fn into_result(self) -> Result<QueryResult, String> {
        match self.kind.as_str() {
            "matrix" => {
                let mut out = Vec::with_capacity(self.series.len());
                for s in self.series {
                    let mut floats = Vec::with_capacity(s.floats.len());
                    for p in s.floats {
                        floats.push(Point {
                            t: p.t,
                            v: parse_float(&p.v)?,
                        });
                    }
                    out.push(Series {
                        labels: s.labels,
                        floats,
                        histograms: s.histograms,
                    });
                }
                Ok(QueryResult::Matrix(out))
            }
            "vector" => {
                let mut out = Vec::with_capacity(self.samples.len());
                for s in self.samples {
                    out.push(Sample {
                        labels: s.labels,
                        t: s.t,
                        v: parse_float(&s.v)?,
                        histogram: s.histogram,
                    });
                }
                Ok(QueryResult::Vector(out))
            }
            "scalar" => Ok(QueryResult::Scalar {
                v: parse_float(&self.value)?,
                t: self.t,
            }),
            "string" => Ok(QueryResult::Str {
                v: self.value,
                t: self.t,
            }),
            "error" => Ok(QueryResult::Error(self.err)),
            other => Err(format!("unknown result kind {other:?}")),
        }
    }
}
