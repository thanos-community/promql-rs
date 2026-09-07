//! Loader for the shared PromQL engine test cases.
//!
//! `thanos-io/promql-engine` keeps its range-query cases in
//! `testcases/range_queries.yaml` in a language-agnostic format so that
//! engine implementations outside Go can run the same suite. This crate
//! is the Rust binding for those files, mirroring the Go binding in
//! `testcases/testcases.go`. The YAML is the source of truth; nothing
//! here reinterprets it.
//!
//! Each case's `load` field is a Prometheus test-script `load` block:
//!
//! ```text
//! load 30s
//! http_requests_total{pod="nginx-1", route="/"} 46.00+13.00x40
//! http_requests_total{pod="nginx-2", route="/"}  2+5.25x40
//! ```
//!
//! The directive line is matched the way upstream's promqltest matches
//! it, and every remaining line goes through
//! [`promql_parser::parse_series_desc`].
//!
//! # Locating the YAML
//!
//! The files live in the other repository and are not vendored here
//! yet, so the directory holding them is supplied at runtime through
//! `PROMQL_ENGINE_TESTCASES`:
//!
//! ```console
//! $ PROMQL_ENGINE_TESTCASES=~/src/github.com/thanos-io/promql-engine/testcases \
//!     cargo test -p promql-testcases
//! ```
//!
//! [`testcases_dir`] returns `None` when it is unset, which is what
//! lets the corpus tests skip rather than fail on a machine without a
//! `promql-engine` checkout. This is a stopgap until the upstream PR
//! lands and the files can be vendored or pulled in as a submodule.
//!
//! # Scope
//!
//! Parsing only. Cases are loaded and their series descriptions parsed;
//! turning the parsed sequences into timestamped samples (`interval` ×
//! index) is left to whatever consumes them.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use promql_parser::SeriesDescription;
use regex::Regex;
use serde::Deserialize;

/// Environment variable naming the directory that holds the YAML files.
pub const TESTCASES_DIR_ENV: &str = "PROMQL_ENGINE_TESTCASES";

/// The range-query suite's filename within that directory.
pub const RANGE_QUERIES_FILE: &str = "range_queries.yaml";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse {path}: {source}")]
    Yaml {
        path: PathBuf,
        #[source]
        source: serde_norway::Error,
    },
    #[error("defaults.step_ms must be positive, got {0}")]
    NonPositiveStep(i64),
    #[error("test {0} has no name")]
    MissingName(usize),
    #[error("test {0:?} has no query")]
    MissingQuery(String),
    #[error("test {case:?}: load block does not start with a `load <interval>` directive")]
    MissingLoadDirective { case: String },
    #[error("test {case:?}: invalid load interval {interval:?}")]
    InvalidInterval { case: String, interval: String },
}

/// A single range-query test case, with defaults applied. Mirrors
/// `testcases.Case` in the Go binding, except that `load` arrives
/// parsed rather than as raw text.
#[derive(Debug, Clone, PartialEq)]
pub struct Case {
    /// Identifies the case; used as the Go subtest name.
    pub name: String,
    /// Optional prose explaining what the case covers.
    pub description: Option<String>,
    /// The input series. `None` for cases that need no data.
    pub load: Option<LoadBlock>,
    /// The PromQL expression to execute.
    pub query: String,
    /// Query range, in milliseconds. `start_ms` and `end_ms` are Unix
    /// timestamps and may be negative; `step_ms` is a duration.
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
}

impl Case {
    /// Whether every series line in this case's load block parsed.
    /// A case with no load block counts as supported.
    pub fn is_supported(&self) -> bool {
        self.load.as_ref().is_none_or(LoadBlock::is_fully_supported)
    }
}

/// A parsed `load <interval>` block.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadBlock {
    /// The sample interval from the directive line, in seconds.
    pub interval_secs: f64,
    /// Whether the directive was `load_with_nhcb` rather than `load`.
    /// Upstream uses it to also derive classic-histogram buckets.
    pub with_nhcb: bool,
    /// One entry per series line, in source order.
    pub series: Vec<SeriesLine>,
}

impl LoadBlock {
    /// The series that parsed, skipping anything unsupported.
    pub fn parsed(&self) -> impl Iterator<Item = &SeriesDescription> {
        self.series.iter().filter_map(|s| match s {
            SeriesLine::Parsed(sd) => Some(sd),
            SeriesLine::Unsupported { .. } => None,
        })
    }

    /// The series lines this parser cannot handle yet.
    pub fn unsupported(&self) -> impl Iterator<Item = &UnsupportedLine> {
        self.series.iter().filter_map(|s| match s {
            SeriesLine::Unsupported(u) => Some(u),
            SeriesLine::Parsed(_) => None,
        })
    }

    pub fn is_fully_supported(&self) -> bool {
        self.unsupported().next().is_none()
    }
}

/// One line of a load block.
#[derive(Debug, Clone, PartialEq)]
pub enum SeriesLine {
    Parsed(SeriesDescription),
    /// Kept rather than dropped so callers can assert on how much of
    /// the corpus is out of reach, and watch that number shrink.
    Unsupported(UnsupportedLine),
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnsupportedLine {
    /// The line as it appeared, trimmed.
    pub text: String,
    pub reason: Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsupported {
    /// A `{{schema:1 …}}` native-histogram descriptor. The grammar
    /// rules behind those are still on `promql-sync`'s skip list.
    NativeHistogram,
    /// Anything else the parser rejected. Treated as a real failure by
    /// the corpus tests rather than an accepted gap.
    ParseError,
}

/// The directory named by `PROMQL_ENGINE_TESTCASES`, if it is set.
pub fn testcases_dir() -> Option<PathBuf> {
    std::env::var_os(TESTCASES_DIR_ENV).map(PathBuf::from)
}

/// Load the range-query suite from `<dir>/range_queries.yaml`.
pub fn range_queries_in(dir: &Path) -> Result<Vec<Case>, Error> {
    load_file(&dir.join(RANGE_QUERIES_FILE))
}

/// Load a suite from an explicit path.
pub fn load_file(path: &Path) -> Result<Vec<Case>, Error> {
    let body = std::fs::read_to_string(path).map_err(|source| Error::Read {
        path: path.to_path_buf(),
        source,
    })?;
    parse_suite(&body).map_err(|e| match e {
        Error::Yaml { source, .. } => Error::Yaml {
            path: path.to_path_buf(),
            source,
        },
        other => other,
    })
}

/// Parse a suite from YAML text. Applies `defaults` and validates the
/// same invariants the Go binding does.
pub fn parse_suite(yaml: &str) -> Result<Vec<Case>, Error> {
    let suite: Suite = serde_norway::from_str(yaml).map_err(|source| Error::Yaml {
        path: PathBuf::new(),
        source,
    })?;

    if suite.defaults.step_ms <= 0 {
        return Err(Error::NonPositiveStep(suite.defaults.step_ms));
    }

    let mut cases = Vec::with_capacity(suite.tests.len());
    for (i, t) in suite.tests.into_iter().enumerate() {
        if t.name.is_empty() {
            return Err(Error::MissingName(i));
        }
        if t.query.trim().is_empty() {
            return Err(Error::MissingQuery(t.name));
        }
        let load = match t.load {
            Some(ref raw) if !raw.trim().is_empty() => Some(parse_load_block(&t.name, raw)?),
            _ => None,
        };
        cases.push(Case {
            load,
            query: t.query.trim_end().to_string(),
            description: t.description.filter(|d| !d.trim().is_empty()),
            start_ms: t.start_ms.unwrap_or(suite.defaults.start_ms),
            end_ms: t.end_ms.unwrap_or(suite.defaults.end_ms),
            step_ms: t.step_ms.unwrap_or(suite.defaults.step_ms),
            name: t.name,
        });
    }
    Ok(cases)
}

/// Matches the `load` directive that opens a block. Same shape as
/// upstream promqltest's `patLoad`.
fn load_directive() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^load(?:_(with_nhcb))?\s+(.+?)\s*$").expect("valid regex"))
}

/// Split a load block into its directive and its series lines.
///
/// A line that fails to parse is recorded rather than raised: a native
/// histogram is a known gap, not a broken corpus. Anything else is
/// flagged as [`Unsupported::ParseError`] so the tests can tell the two
/// apart.
fn parse_load_block(case: &str, raw: &str) -> Result<LoadBlock, Error> {
    let mut lines = raw.lines().map(str::trim).filter(|l| !l.is_empty());

    let directive = lines.next().unwrap_or_default();
    let caps = load_directive()
        .captures(directive)
        .ok_or_else(|| Error::MissingLoadDirective {
            case: case.to_string(),
        })?;
    let with_nhcb = caps.get(1).is_some();
    let interval = caps.get(2).expect("group 2 always matches").as_str();
    let interval_secs =
        promql_parser::parse_duration_seconds(interval).map_err(|()| Error::InvalidInterval {
            case: case.to_string(),
            interval: interval.to_string(),
        })?;

    let series = lines
        .map(|line| match promql_parser::parse_series_desc(line) {
            Ok(sd) => SeriesLine::Parsed(sd),
            Err(_) => SeriesLine::Unsupported(UnsupportedLine {
                text: line.to_string(),
                reason: if line.contains("{{") {
                    Unsupported::NativeHistogram
                } else {
                    Unsupported::ParseError
                },
            }),
        })
        .collect();

    Ok(LoadBlock {
        interval_secs,
        with_nhcb,
        series,
    })
}

// ---------------- YAML shapes ----------------

#[derive(Debug, Deserialize)]
struct Suite {
    #[serde(default)]
    defaults: Defaults,
    #[serde(default)]
    tests: Vec<RawCase>,
}

#[derive(Debug, Default, Deserialize)]
struct Defaults {
    #[serde(default)]
    start_ms: i64,
    #[serde(default)]
    end_ms: i64,
    #[serde(default)]
    step_ms: i64,
}

#[derive(Debug, Deserialize)]
struct RawCase {
    #[serde(default)]
    name: String,
    description: Option<String>,
    load: Option<String>,
    #[serde(default)]
    query: String,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    step_ms: Option<i64>,
}
