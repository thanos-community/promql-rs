//! How a scan is split into partitions and batches before it crosses the seam.
//!
//! A scan is handed over as partitions of batches, the way a real provider emits it, so
//! [`build`] takes a [`Chunking`] and returns a [`Built`]. Which layout the batches are in is the
//! type parameter; nothing here knows what a batch looks like.

use std::ops::Range;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;

use super::format::{labels_bytes, total_bytes};
use super::layout::Layout;
use super::scan::{series_order, Spec};

/// How a scan is split into partitions and batches before it crosses the seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chunking {
    /// Partitions the scan is split into. Each holds a contiguous run of whole series, which is
    /// what a provider that knows its series boundaries would do.
    pub partitions: usize,
    /// DataFusion rows per batch within a partition, `0` for one batch per partition. Rows are
    /// samples or series depending on the layout, so for the row-per-sample layout a series may
    /// cross a batch boundary, exactly as it does coming out of a Parquet reader.
    pub rows_per_batch: usize,
}

impl Chunking {
    /// Everything in one batch.
    pub const SINGLE: Chunking = Chunking {
        partitions: 1,
        rows_per_batch: 0,
    };

    pub fn new(partitions: usize, rows_per_batch: usize) -> Self {
        Self {
            partitions: partitions.max(1),
            rows_per_batch,
        }
    }
}

/// A scan encoded in one layout, ready to register.
pub struct Built {
    pub schema: SchemaRef,
    /// Partitions of batches, series contiguous and never interleaved.
    pub partitions: Vec<Vec<RecordBatch>>,
    /// DataFusion rows across all partitions.
    pub rows: usize,
    /// Bytes held by the labels, measured before slicing so shared buffers are counted once.
    pub labels_bytes: usize,
    /// Bytes held by everything, measured the same way.
    pub total_bytes: usize,
}

impl Built {
    pub fn batches(&self) -> usize {
        self.partitions.iter().map(Vec::len).sum()
    }
}

/// Which positions of the sorted series order land in each partition.
fn partition_ranges(series: usize, partitions: usize) -> Vec<Range<usize>> {
    let partitions = partitions.max(1).min(series.max(1));
    (0..partitions)
        .map(|p| (p * series / partitions)..((p + 1) * series / partitions))
        .collect()
}

/// Slice one partition's batch into `rows_per_batch` pieces, `0` for no slicing.
fn slice(batch: RecordBatch, rows_per_batch: usize) -> Vec<RecordBatch> {
    if rows_per_batch == 0 || batch.num_rows() <= rows_per_batch {
        return vec![batch];
    }
    (0..batch.num_rows())
        .step_by(rows_per_batch)
        .map(|off| batch.slice(off, rows_per_batch.min(batch.num_rows() - off)))
        .collect()
}

/// Encode a scan in layout `L`, as partitions of batches.
pub fn build<L: Layout>(spec: &Spec, chunking: Chunking) -> Built {
    let order = series_order(spec.series);
    let mut partitions = Vec::new();
    let mut rows = 0;
    let mut labels = 0;
    let mut total = 0;

    for range in partition_ranges(spec.series, chunking.partitions) {
        let batch = L::encode(spec, &order[range]);
        rows += batch.num_rows();
        labels += labels_bytes(&batch);
        total += total_bytes(&batch);
        partitions.push(slice(batch, chunking.rows_per_batch));
    }

    Built {
        schema: L::schema(),
        partitions,
        rows,
        labels_bytes: labels,
        total_bytes: total,
    }
}

/// Encode a scan as one batch. Convenience for tests and footprint checks.
pub fn build_single<L: Layout>(spec: &Spec) -> RecordBatch {
    let mut built = build::<L>(spec, Chunking::SINGLE);
    built
        .partitions
        .pop()
        .and_then(|mut p| p.pop())
        .expect("one partition, one batch")
}
