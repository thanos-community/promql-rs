//! Conformance testing for the Rust PromQL engine.
//!
//! Two suites live here, split by where their notion of "correct" comes
//! from. The split is the point: they rot in opposite directions, so
//! running both is worth more than running either twice.
//!
//! - [`thanos`] — `thanos-io/promql-engine`'s shared corpus, checked
//!   against a **live Prometheus**. Upstream's own suite is differential
//!   for the same reason (`engine/engine_test.go:209`), which is why
//!   `testcases/range_queries.yaml` carries no expected values at all.
//!   Recording Go's current output as fixtures would rot silently as
//!   Prometheus evolves; asking it afresh every run cannot. The cost is
//!   a Go toolchain and a corpus checkout, so this suite skips when
//!   either is missing.
//! - [`prometheus`] — Prometheus's **own promqltest corpus**, vendored
//!   under `testdata/prometheus/`, where the expected values ship with
//!   the questions. Nothing to ask, so nothing to be unavailable: no
//!   oracle, no Go, no network. This is the suite that runs in CI.
//!
//! # Current state
//!
//! [`datafusion::DataFusionEngine`] fills the [`result::Engine`] seam
//! with the `promql-engine` crate. It evaluates vector selectors,
//! aggregations and range functions; every other expression fails with
//! [`result::EngineError::Unsupported`], and those failures are the
//! specification for what to build next. The passing count is the
//! progress meter.
//!
//! A red test is only useful when it is red for the right reason, so
//! harness breakage is always reported as such rather than as a failing
//! engine. `tests/selfcheck.rs` proves the oracle is reachable, the
//! protocol round-trips and the comparer is correct; if those fail,
//! no differential failure means anything.

pub mod compare;
pub mod datafusion;
pub mod prometheus;
pub mod result;
pub mod thanos;

pub use compare::{compare, floats_equal, Mismatch};
pub use datafusion::DataFusionEngine;
pub use result::{Engine, EngineError, Labels, Point, QueryResult, Sample, Series, Unimplemented};
pub use thanos::oracle::{self, Oracle, OracleError};
