//! Everything that is not one of the two layouts.
//!
//! Split the way `docs/series-representation.md` splits the problem:
//!
//! - [`scan`] is phase 1. Out of scope for the design, so here it is faked: it invents series
//!   with `apiserver_request_total`'s cardinality and turns them into the Arrow arrays both
//!   layouts are built from.
//! - [`format`] is the seam. Column names, leaf types and the operator's output schema, the only
//!   things the two phases agree on.
//! - [`layout`] is the contract a layout implements; [`chunking`] hands a scan over as partitions
//!   of batches the way a provider would.
//! - [`rangevec`] is phase 2's range-vector operator, generic over the layout. [`window`] finds
//!   its window bounds and [`ratefn`] is the real `rate` it runs.
//! - [`promql`] is the rest of phase 2, as DataFusion logical plans, the same for every layout.
//! - [`frame`] is the native window frame, kept for one characterisation and never timed.
//! - [`dispatch`] picks a layout at runtime, [`harness`] runs the benchmark, [`query`] is the
//!   PromQL query as numbers and [`alloc`] counts allocations.

pub mod alloc;
pub mod chunking;
pub mod dispatch;
pub mod format;
pub mod frame;
pub mod harness;
pub mod layout;
pub mod promql;
pub mod query;
pub mod rangevec;
pub mod ratefn;
pub mod scan;
pub mod window;
