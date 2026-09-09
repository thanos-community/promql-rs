//! The contract both layouts implement.
//!
//! Everything the shared operator in [`super::rangevec`] needs from a layout, and nothing else.
//! `src/list.rs` and `src/struct_ree.rs` are the two implementations, and they are the only files
//! in the crate that know how a batch of either layout is put together.

use std::borrow::Cow;
use std::fmt;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;

use super::scan::Spec;

/// One way of laying series out in Arrow, and how to get them back out.
///
/// `Default` because the operator creates one per partition to hold cross-batch state; `Unpin`
/// because the stream that holds it is polled through `get_mut`.
pub trait Layout: Default + fmt::Debug + Send + Sync + Unpin + 'static {
    /// The name the benchmark and the results tables use.
    const NAME: &'static str;

    /// A series' labels while its batch is being processed. What this is depends on whether the
    /// labels can be gathered from the input at the end or have to be read out on the way.
    type Labels;

    fn schema() -> SchemaRef;

    /// Encode the given series of the fake scan as one batch, series contiguous.
    fn encode(spec: &Spec, series: &[usize]) -> RecordBatch;

    /// DataFusion rows per batch for a budget of samples per batch.
    fn rows_per_batch(samples_per_batch: usize, samples_per_series: usize) -> usize;

    /// The series completed by seeing `batch`, in order. `self` is the per-partition state a
    /// layout needs when a series can continue into the next batch.
    fn series<'a>(&mut self, batch: &'a RecordBatch) -> Vec<Series<'a, Self::Labels>>;

    /// End of the partition: whatever is still held back.
    fn finish(&mut self) -> Option<Series<'static, Self::Labels>>;

    /// The output labels column for the series found in `input`.
    fn labels_column(input: &RecordBatch, labels: Vec<Self::Labels>) -> Result<ArrayRef>;
}

/// One series, ready for the kernel. Samples are borrowed from the batch when the series lies
/// within it and owned when it had to be stitched together from more than one.
pub struct Series<'a, L> {
    pub labels: L,
    pub ts: Cow<'a, [i64]>,
    pub vs: Cow<'a, [f64]>,
}
