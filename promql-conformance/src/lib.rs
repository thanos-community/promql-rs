//! Differential conformance testing against a Prometheus oracle.
//!
//! `promql-engine`'s conformance suite is differential:
//! `TestQueriesAgainstOldEngine` (`engine/engine_test.go:209`) runs each
//! case against both the Thanos engine and Prometheus's own and asserts
//! the results match, which is why `testcases/range_queries.yaml`
//! carries no expected values at all. Recording Go's current output as
//! fixtures would rot silently as upstream Prometheus evolves; asking
//! the reference implementation on every run cannot.
//!
//! This crate does the same in Rust. A small Go binary
//! (`scripts/promql-oracle`) answers "what does Prometheus return for
//! this load block and query" over a line-based JSON protocol;
//! [`compare`] then applies Go's own comparison rules.
//!
//! # Current state
//!
//! [`datafusion::DataFusionEngine`] fills the [`result::Engine`] seam
//! with the `promql-engine` crate. It evaluates bare instant-vector
//! selectors; every other expression fails with
//! [`result::EngineError::Unsupported`], and those failures are the
//! specification for what to build next. The passing count is the
//! progress meter.
//!
//! The suite is split in two, and the split is the point:
//!
//! - **Harness self-checks** (`tests/selfcheck.rs`) must pass. They
//!   prove the oracle is reachable, the protocol round-trips, and the
//!   comparer is correct.
//! - **Differential cases** (`tests/differential.rs`) pass where the
//!   engine agrees with Prometheus, fail where it disagrees, and are
//!   collapsed into one failure where the engine has nothing to say yet.
//!
//! A red test is only useful when it is red for the right reason. If a
//! self-check fails, the plumbing is broken and no differential failure
//! means anything.

pub mod compare;
pub mod datafusion;
pub mod oracle;
pub mod result;

pub use compare::{compare, floats_equal, Mismatch};
pub use datafusion::DataFusionEngine;
pub use oracle::{Oracle, OracleError};
pub use result::{Engine, EngineError, Labels, Point, QueryResult, Sample, Series, Unimplemented};
