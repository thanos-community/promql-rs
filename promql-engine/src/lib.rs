//! PromQL evaluation as DataFusion plans.
//!
//! The engine sits between two things it does not own: a parser
//! ([`promql_parser`]) and a store. The store is anyone's — parquet,
//! vortex, a remote service — so the one thing this crate specifies about
//! it is the seam: a Rust trait, [`SeriesSource`], and an Arrow shape,
//! [`series`]. Everything PromQL — lookback, staleness, the step grid,
//! `offset`, `@`, and later every function and aggregation — happens on
//! this side of that seam as ordinary DataFusion plans and functions.
//!
//! ```text
//! PromQL text ─► parser ─► Expr ─► plan::plan ─► LogicalPlan ─► DataFusion
//!                                      │                          ▲
//!                                      └── SeriesSource::select ──┘
//!                                          (the store's ExecutionPlan,
//!                                           in the canonical schema)
//! ```
//!
//! # Arrow through DataFusion only
//!
//! This crate reaches Arrow exclusively through `datafusion::arrow`. A
//! direct `arrow` dependency would pin a second Arrow version for any
//! consumer that patches DataFusion to a fork, and every `RecordBatch`
//! crossing the seam would then be a type mismatch.

pub mod aggregate;
pub mod engine;
pub mod error;
pub mod labels;
pub mod matcher;
pub mod math;
pub mod memory;
pub mod plan;
pub mod range;
pub mod selector;
pub mod series;
pub mod source;

pub use engine::{Engine, RangeQuery};
pub use error::EngineError;
pub use memory::MemorySeriesSource;
pub use series::DecodedSeries;
pub use source::{Grouping, SelectHints, SelectorTable, SeriesSource};
