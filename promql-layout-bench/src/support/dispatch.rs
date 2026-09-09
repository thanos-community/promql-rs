//! Runtime keys for the harness and the tests.
//!
//! [`Layout`] is not object-safe, so anything that loops over layouts at runtime goes through
//! [`LayoutKind`], which forwards to the generic functions. This is the only file in `support/`
//! that names the two layout types.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::prelude::{DataFrame, SessionContext};

use super::chunking::{build, build_single, Built, Chunking};
use super::layout::Layout;
use super::query::Query;
use super::rangevec::rate_table;
use super::scan::Spec;
use crate::list::List;
use crate::struct_ree::StructRee;

/// The two candidates, as a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutKind {
    /// Rows are samples, labels run-end encoded inside the struct. See `src/struct_ree.rs`.
    StructRee,
    /// Rows are series, samples in aligned lists. See `src/list.rs`.
    List,
}

impl LayoutKind {
    /// Both candidates, in the order `docs/series-representation.md` compares them.
    pub const ALL: [LayoutKind; 2] = [LayoutKind::StructRee, LayoutKind::List];

    pub fn name(self) -> &'static str {
        match self {
            Self::StructRee => StructRee::NAME,
            Self::List => List::NAME,
        }
    }

    pub fn build(self, spec: &Spec, chunking: Chunking) -> Built {
        match self {
            Self::StructRee => build::<StructRee>(spec, chunking),
            Self::List => build::<List>(spec, chunking),
        }
    }

    pub fn build_single(self, spec: &Spec) -> RecordBatch {
        match self {
            Self::StructRee => build_single::<StructRee>(spec),
            Self::List => build_single::<List>(spec),
        }
    }

    pub fn rows_per_batch(self, samples_per_batch: usize, samples_per_series: usize) -> usize {
        match self {
            Self::StructRee => StructRee::rows_per_batch(samples_per_batch, samples_per_series),
            Self::List => List::rows_per_batch(samples_per_batch, samples_per_series),
        }
    }

    /// The scan wrapped in the series-aware operator for this layout.
    pub fn rate_table(self, scan: Arc<dyn TableProvider>, query: Query) -> Arc<dyn TableProvider> {
        match self {
            Self::StructRee => rate_table::<StructRee>(scan, query),
            Self::List => rate_table::<List>(scan, query),
        }
    }
}

/// `rate(m[w])` as a frame of `(labels, value)` for an instant query or `(labels, grid)` for a
/// range query: the scan wrapped in the series-aware operator. Same schema whichever layout fed it.
pub fn rate_frame(
    ctx: &SessionContext,
    scan: Arc<dyn TableProvider>,
    layout: LayoutKind,
    query: Query,
) -> Result<DataFrame> {
    ctx.read_table(layout.rate_table(scan, query))
}
