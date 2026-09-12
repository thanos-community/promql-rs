//! PromQL evaluation as DataFusion plans.
//!
//! The engine sits between two things it does not own: a parser
//! ([`promql_parser`]) and a store. The store is anyone's — parquet,
//! vortex, a remote service — so the one thing this crate specifies about
//! it is a Rust trait, [`SeriesSource`], and the Arrow shape its data
//! comes back in, [`series`]. Everything PromQL — lookback, staleness, the
//! step grid, `offset`, `@`, functions and aggregations — happens on the
//! engine's side of that trait as ordinary DataFusion plans and functions.
//!
//! `docs/series-source.md` is the design note for the trait and the shape.
//! Above them, [`plan`] turns a parsed expression into a `LogicalPlan`
//! over the store's plan, with [`selector`], [`aggregate`] and [`range`]
//! as DataFusion functions over the samples list, and [`engine`] runs it.
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
//! consumer that patches DataFusion to a fork, and every `RecordBatch` a
//! store hands over would then be a type mismatch.

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
pub use series::Series;
pub use source::{Grouping, SelectHints, SelectorTable, SeriesSource, Shard};
