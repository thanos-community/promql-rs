//! A [`SeriesSource`] that computes its series from a seed-free
//! description instead of storing them, for the memory bench.
//!
//! `MemorySeriesSource` keeps every input batch resident, so a query over
//! it can never show what the engine saves by not holding a series: the
//! input is already all there. Streamed, each batch is built when the
//! plan polls for it and freed once the engine lets go, which is what a
//! store reading chunks off disk or a socket does. Resident, the same
//! batches are built up front and served from memory, the equivalent of
//! `MemorySeriesSource` with its rows packed into full batches.
//!
//! The data is the long bench's: counters `http_requests_total` with
//! `code`, `pod` and `route`, resetting every 1000 samples.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Float64Array, ListArray, RecordBatch, StringViewBuilder, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use promql_engine::series::{labels_type, sample_fields, sample_item, schema};
use promql_engine::{SelectHints, SeriesSource};
use promql_parser::ast::LabelMatcher;

const NAMES: [&str; 4] = ["__name__", "code", "pod", "route"];

#[derive(Debug, Clone)]
pub struct Shape {
    pub series: usize,
    pub samples: usize,
    pub scrape_ms: i64,
    /// Samples per row; `None` is one row per series.
    pub chunk: Option<usize>,
    pub rows_per_batch: usize,
}

#[derive(Debug)]
pub struct Generated {
    shape: Shape,
    schema: SchemaRef,
    /// Series indices in the struct order of their label sets, which is
    /// not index order (`nginx-10` sorts before `nginx-2`).
    order: Arc<Vec<usize>>,
    /// Built up front when resident; `None` streams.
    resident: Option<Vec<RecordBatch>>,
}

impl Generated {
    pub fn streamed(shape: Shape) -> Self {
        let names: Vec<String> = NAMES.iter().map(|n| n.to_string()).collect();
        let mut order: Vec<usize> = (0..shape.series).collect();
        order.sort_by_key(|&i| (code(i), pod(i)));
        Self {
            shape,
            schema: schema(&names),
            order: Arc::new(order),
            resident: None,
        }
    }

    pub fn resident(shape: Shape) -> Self {
        let mut s = Self::streamed(shape);
        let all = s.partition(0, i64::MAX).batches().collect::<Result<_>>();
        s.resident = Some(all.expect("generated batches are canonical"));
        s
    }

    fn partition(&self, start_ms: i64, end_ms: i64) -> Partition {
        Partition {
            shape: self.shape.clone(),
            schema: self.schema.clone(),
            order: self.order.clone(),
            start_ms,
            end_ms,
        }
    }
}

#[async_trait]
impl SeriesSource for Generated {
    /// Every series matches: the bench only asks for `http_requests_total`.
    async fn select(
        &self,
        _state: &dyn Session,
        _matchers: &[LabelMatcher],
        hints: SelectHints,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(batches) = &self.resident {
            let last = (self.shape.samples as i64 - 1) * self.shape.scrape_ms;
            assert!(
                hints.start_ms <= 0 && hints.end_ms >= last,
                "a resident source is not clipped; query its whole range"
            );
            return Ok(MemorySourceConfig::try_new_exec(
                std::slice::from_ref(batches),
                self.schema.clone(),
                None,
            )?);
        }
        let part: Arc<dyn PartitionStream> = Arc::new(self.partition(hints.start_ms, hints.end_ms));
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema.clone(),
            vec![part],
            None,
            [],
            false,
            None,
        )?))
    }
}

#[derive(Debug)]
struct Partition {
    shape: Shape,
    schema: SchemaRef,
    order: Arc<Vec<usize>>,
    start_ms: i64,
    end_ms: i64,
}

impl Partition {
    /// Samples `lo..hi` of every series in label order, cut into rows of
    /// at most `chunk`, packed `rows_per_batch` rows to a batch. Each
    /// batch is built only when the iterator is advanced.
    fn batches(&self) -> impl Iterator<Item = Result<RecordBatch>> + Send + 'static {
        let s = &self.shape;
        let lo = (self.start_ms.max(0) + s.scrape_ms - 1) / s.scrape_ms;
        let hi = ((self.end_ms / s.scrape_ms) + 1).min(s.samples as i64);
        let (lo, hi) = (lo as usize, (hi.max(lo)) as usize);
        let chunk = s.chunk.unwrap_or(usize::MAX).max(1);
        let order = self.order.clone();
        let mut rows = (0..order.len()).map(move |k| order[k]).flat_map(move |i| {
            (lo..hi)
                .step_by(chunk)
                .map(move |from| (i, from, (from + chunk).min(hi)))
        });
        let schema = self.schema.clone();
        let scrape_ms = s.scrape_ms;
        let per_batch = s.rows_per_batch;
        std::iter::from_fn(move || {
            let rows: Vec<(usize, usize, usize)> = rows.by_ref().take(per_batch).collect();
            (!rows.is_empty()).then(|| build(&schema, &rows, scrape_ms))
        })
    }
}

impl PartitionStream for Partition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(self.batches()),
        ))
    }
}

fn code(i: usize) -> &'static str {
    if i.is_multiple_of(16) {
        "500"
    } else {
        "200"
    }
}

fn pod(i: usize) -> String {
    format!("nginx-{i}")
}

/// The value of series `i` at sample `j`: a counter that resets every
/// 1000 samples, computed in closed form so a chunk needs no history.
fn value(i: usize, j: usize) -> f64 {
    let base = j - j % 1000;
    (base..=j).map(|k| 1.0 + ((i + k) % 7) as f64).sum()
}

fn build(
    schema: &SchemaRef,
    rows: &[(usize, usize, usize)],
    scrape_ms: i64,
) -> Result<RecordBatch> {
    let mut labels: Vec<StringViewBuilder> =
        NAMES.iter().map(|_| StringViewBuilder::new()).collect();
    let n: usize = rows.iter().map(|(_, from, to)| to - from).sum();
    let mut ts: Vec<i64> = Vec::with_capacity(n);
    let mut vs: Vec<f64> = Vec::with_capacity(n);
    let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    offsets.push(0);
    for &(i, from, to) in rows {
        labels[0].append_value("http_requests_total");
        labels[1].append_value(code(i));
        labels[2].append_value(pod(i));
        labels[3].append_value(format!("/{}", i % 8));
        // One running sum per row rather than `value` per sample.
        let mut v = value(i, from);
        for j in from..to {
            if j > from {
                v = if j % 1000 == 0 { 0.0 } else { v } + 1.0 + ((i + j) % 7) as f64;
            }
            ts.push(j as i64 * scrape_ms);
            vs.push(v);
        }
        offsets.push(ts.len() as i32);
    }
    let DataType::Struct(label_fields) = labels_type(&NAMES.map(String::from)) else {
        unreachable!("labels are a struct")
    };
    let labels = StructArray::new(
        label_fields,
        labels
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as ArrayRef)
            .collect(),
        None,
    );
    let entries = StructArray::new(
        sample_fields(),
        vec![
            Arc::new(TimestampMillisecondArray::from(ts)),
            Arc::new(Float64Array::from(vs)),
        ],
        None,
    );
    let samples = ListArray::new(
        sample_item(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(entries),
        None,
    );
    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(labels), Arc::new(samples)],
    )?)
}
