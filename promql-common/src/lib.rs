//! Structural hand-port of `github.com/prometheus/common` (v0.67.5), the
//! `model` package specifically. Prometheus pins this version at the commit
//! named in `promql-conformance/testdata/prometheus/UPSTREAM.md`
//! (`83962c35a4ab0c9988bc469aa8165014fc065d34`), which is the spec: a local
//! checkout's HEAD is not.

pub mod model;
