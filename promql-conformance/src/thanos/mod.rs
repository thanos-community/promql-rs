//! The differential suite: `thanos-io/promql-engine`'s shared corpus,
//! checked against a live Prometheus.
//!
//! `testcases/range_queries.yaml` carries no expected values at all —
//! upstream's `TestQueriesAgainstOldEngine` runs each case through both
//! the Thanos engine and Prometheus's own and asserts they agree. We do
//! the same, asking [`oracle`] rather than recording Go's answers,
//! because recorded fixtures rot silently as Prometheus evolves and a
//! question asked afresh every run cannot.
//!
//! Contrast [`crate::prometheus`], where the expected values ship with
//! the corpus. The two rot in opposite directions, which is why both
//! are worth having: a disagreement between them is a Prometheus
//! behaviour change worth knowing about.

pub mod oracle;
