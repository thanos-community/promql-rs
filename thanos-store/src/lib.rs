//! A Thanos Store API client that speaks `promql-engine`'s `SeriesSource`.
//!
//! Thanos components (Sidecar, Store Gateway, Receive, Ruler, Query)
//! all serve one gRPC service, `thanos.Store`, which returns series as
//! label sets plus XOR-compressed chunks for a set of matchers and a time
//! range. This crate dials a static list of those endpoints, learns
//! their time ranges and external labels through `thanos.info.Info`,
//! fans a `Series` call out to the endpoints that can have data, decodes
//! the chunks and hands the engine one canonical batch per selector.
//!
//! Names follow the Go packages they port: `storepb` for the generated
//! types, `chunkenc` for the chunk codec, `EndpointSet` and `ProxyStore`
//! for the fan-out.

pub mod chunkenc;
pub mod storepb;
