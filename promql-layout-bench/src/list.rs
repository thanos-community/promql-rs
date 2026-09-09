//! Rows are series.
//!
//! One row per series: the labels once, the samples as two `List` columns that share one
//! `OffsetBuffer`. The series boundary *is* the offsets, so finding a series is reading two
//! integers, a series can never cross a batch, and its samples are a slice of the list children.
//! That is the whole layout, which is the point of the comparison.
//!
//! Any row split is a series split, so the planner could repartition this layout freely. The
//! shared operator keeps repartitioning off for both layouts so the comparison stays like for like.

use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, ListArray, RecordBatch, UInt32Array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{
    DataType, Field, Float64Type, Schema, SchemaRef, TimestampMillisecondType,
};
use datafusion::common::Result;

use crate::support::format::{
    label_fields, list_item, timestamp_type, COL_LABELS, COL_TIMESTAMPS, COL_VALUES,
};
use crate::support::layout::{Layout, Series};
use crate::support::scan::{flat_samples, labels_per_series, Spec};

/// Rows as series. No state: a series never crosses a batch, so there is nothing to carry.
#[derive(Debug, Default)]
pub struct List;

impl Layout for List {
    const NAME: &'static str = "list";

    /// The row of the input's labels column the series came from, gathered with one `take` when
    /// the output batch is assembled.
    type Labels = usize;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(COL_LABELS, DataType::Struct(label_fields()), false),
            Field::new(
                COL_TIMESTAMPS,
                DataType::List(list_item(timestamp_type())),
                false,
            ),
            Field::new(
                COL_VALUES,
                DataType::List(list_item(DataType::Float64)),
                false,
            ),
        ]))
    }

    /// The two list columns deliberately share one `OffsetBuffer`, which is the positional
    /// alignment the format requires: sample `k` of a series is `timestamps[k]` and `values[k]`
    /// in the same slot.
    fn encode(spec: &Spec, series: &[usize]) -> RecordBatch {
        let labels = labels_per_series(series);
        let (ts, vs) = flat_samples(spec, series);
        let offsets = OffsetBuffer::new(
            (0..=series.len())
                .map(|s| (s * spec.samples) as i32)
                .collect::<Vec<_>>()
                .into(),
        );
        let timestamps = ListArray::new(
            list_item(timestamp_type()),
            offsets.clone(),
            Arc::new(ts),
            None,
        );
        let values = ListArray::new(list_item(DataType::Float64), offsets, Arc::new(vs), None);
        RecordBatch::try_new(
            Self::schema(),
            vec![Arc::new(labels), Arc::new(timestamps), Arc::new(values)],
        )
        .expect("list batch")
    }

    /// Rows are series, so a sample budget caps how many whole series a batch holds.
    fn rows_per_batch(samples_per_batch: usize, samples_per_series: usize) -> usize {
        (samples_per_batch / samples_per_series).max(1)
    }

    /// Series `i` is offsets `i..i+1` of the list children. Nothing is copied.
    fn series<'a>(&mut self, batch: &'a RecordBatch) -> Vec<Series<'a, usize>> {
        let ts = batch
            .column_by_name(COL_TIMESTAMPS)
            .unwrap()
            .as_list::<i32>();
        let vs = batch.column_by_name(COL_VALUES).unwrap().as_list::<i32>();
        let ts_values: &[i64] = ts
            .values()
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let vs_values: &[f64] = vs.values().as_primitive::<Float64Type>().values();
        let offsets = ts.offsets();

        (0..batch.num_rows())
            .map(|i| {
                let (a, b) = (offsets[i] as usize, offsets[i + 1] as usize);
                Series {
                    labels: i,
                    ts: Cow::Borrowed(&ts_values[a..b]),
                    vs: Cow::Borrowed(&vs_values[a..b]),
                }
            })
            .collect()
    }

    /// A row is a whole series, so nothing is ever held back.
    fn finish(&mut self) -> Option<Series<'static, usize>> {
        None
    }

    fn labels_column(input: &RecordBatch, rows: Vec<usize>) -> Result<ArrayRef> {
        let src = input.column_by_name(COL_LABELS).unwrap();
        let idx = UInt32Array::from(rows.into_iter().map(|i| i as u32).collect::<Vec<_>>());
        Ok(take(src.as_ref(), &idx, None)?)
    }
}
