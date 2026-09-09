//! Rows are samples.
//!
//! One row per sample, the labels run-end encoded inside the struct: each of the 18 label fields
//! is `RunEndEncoded<Int32, Dictionary<UInt32, Utf8>>` with its own runs. Cheapest labels at rest,
//! because a constant label such as `job` collapses to one run for the whole partition.
//!
//! Nothing in the batch says where a series ends, so the operator has to work it out. A series
//! ends wherever *any* label's run ends, a series may continue into the next batch and has to be
//! carried across, and its labels have to be read back out of 18 dictionaries. Everything in this
//! file beyond the encoding exists because of that.

use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, DictionaryArray, Int32Array, RecordBatch, RunArray,
    StringDictionaryBuilder, StructArray,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Float64Type, Int32Type, SchemaRef, TimestampMillisecondType,
    UInt32Type,
};
use datafusion::common::Result;

use crate::support::format::{
    dict, dict_type, flat_schema, label_fields, COL_LABELS, COL_TIMESTAMP, COL_VALUE, LABEL_NAMES,
};
use crate::support::layout::{Layout, Series};
use crate::support::scan::{flat_samples, labels_for, Spec};

/// A run-end encoded label column, `RunEndEncoded<Int32, Dictionary<UInt32, Utf8>>`, with the
/// values child stated non-nullable.
///
/// arrow-rs's `RunArray::try_new` hardcodes the values child as nullable, so the type is spelled
/// out here and the array built from `ArrayData` against it. An absent label is `""` in PromQL,
/// never NULL, and the format says so in the schema rather than in a comment.
pub fn ree_type() -> DataType {
    DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int32, false)),
        Arc::new(Field::new("values", dict_type(), false)),
    )
}

/// Labels as one struct field per label, each run-end encoded.
pub fn ree_label_fields() -> Fields {
    LABEL_NAMES
        .iter()
        .map(|n| Field::new(*n, ree_type(), false))
        .collect()
}

/// Collapse one label across the given series into runs, one entry per distinct adjacent value.
fn runs_for_field(
    spec: &Spec,
    series: &[usize],
    field: usize,
) -> (Int32Array, DictionaryArray<UInt32Type>) {
    let mut ends: Vec<i32> = Vec::new();
    let mut vals: Vec<String> = Vec::new();
    if spec.samples > 0 {
        for (n, &i) in series.iter().enumerate() {
            let v = labels_for(i)[field].clone();
            let end = ((n + 1) * spec.samples) as i32;
            if vals.last() == Some(&v) {
                *ends.last_mut().unwrap() = end;
            } else {
                ends.push(end);
                vals.push(v);
            }
        }
    }
    (Int32Array::from(ends), dict(vals))
}

/// A run-end encoded label built against [`ree_type`] directly, which is the only way to get the
/// non-nullable values child.
fn ree_array(ends: Int32Array, values: DictionaryArray<UInt32Type>) -> ArrayRef {
    let len = ends.values().last().copied().unwrap_or(0) as usize;
    let data = ArrayData::builder(ree_type())
        .len(len)
        .add_child_data(ends.into_data())
        .add_child_data(values.into_data())
        .build()
        .expect("run-end encoded label");
    Arc::new(RunArray::<Int32Type>::from(data))
}

/// A series held back because the next batch may continue it.
#[derive(Debug)]
struct Carry {
    labels: Vec<String>,
    ts: Vec<i64>,
    vs: Vec<f64>,
}

/// Rows as samples. The state is the series held back at the end of the previous batch.
#[derive(Debug, Default)]
pub struct StructRee {
    carry: Option<Carry>,
}

impl Layout for StructRee {
    const NAME: &'static str = "struct_ree";

    /// The label values, read out as strings at the series' first row. They cannot be gathered
    /// from the input later because an output row may span more than one input batch.
    type Labels = Vec<String>;

    fn schema() -> SchemaRef {
        flat_schema(DataType::Struct(ree_label_fields()))
    }

    /// Labels a plain struct whose 18 children are each run-end encoded, samples flat alongside.
    fn encode(spec: &Spec, series: &[usize]) -> RecordBatch {
        let columns: Vec<ArrayRef> = (0..LABEL_NAMES.len())
            .map(|f| {
                let (ends, vals) = runs_for_field(spec, series, f);
                ree_array(ends, vals)
            })
            .collect();
        let labels =
            StructArray::try_new(ree_label_fields(), columns, None).expect("struct of runs");
        let (ts, vs) = flat_samples(spec, series);
        RecordBatch::try_new(
            Self::schema(),
            vec![Arc::new(labels), Arc::new(ts), Arc::new(vs)],
        )
        .expect("struct_ree batch")
    }

    /// Rows are samples, so the budget is the row count. A series longer than it spans batches.
    fn rows_per_batch(samples_per_batch: usize, _samples_per_series: usize) -> usize {
        samples_per_batch
    }

    fn series<'a>(&mut self, batch: &'a RecordBatch) -> Vec<Series<'a, Vec<String>>> {
        let mut series = split(batch);
        let mut out = Vec::with_capacity(series.len() + 1);

        // Does the held-back series continue into this batch?
        if let Some(mut carry) = self.carry.take() {
            let continues = matches!(series.first(), Some(s) if s.labels == carry.labels);
            if continues {
                let first = series.remove(0);
                carry.ts.extend_from_slice(&first.ts);
                carry.vs.extend_from_slice(&first.vs);
                if series.is_empty() {
                    // The whole batch was that one series, and it may go on.
                    self.carry = Some(carry);
                    return out;
                }
            }
            out.push(Series {
                labels: carry.labels,
                ts: Cow::Owned(carry.ts),
                vs: Cow::Owned(carry.vs),
            });
        }

        // Hold back the last series; the next batch decides whether it is complete.
        if let Some(last) = series.pop() {
            self.carry = Some(Carry {
                labels: last.labels,
                ts: last.ts.to_vec(),
                vs: last.vs.to_vec(),
            });
        }
        out.extend(series);
        out
    }

    /// End of the partition: the held-back series is complete.
    fn finish(&mut self) -> Option<Series<'static, Vec<String>>> {
        self.carry.take().map(|c| Series {
            labels: c.labels,
            ts: Cow::Owned(c.ts),
            vs: Cow::Owned(c.vs),
        })
    }

    /// Labels arrive as strings, so they are dictionary encoded again, one builder per field.
    /// Sized to this batch's series, so no builder slack leaks into `peak alloc`.
    fn labels_column(_input: &RecordBatch, labels: Vec<Vec<String>>) -> Result<ArrayRef> {
        let mut builders: Vec<StringDictionaryBuilder<UInt32Type>> = (0..LABEL_NAMES.len())
            .map(|f| {
                let bytes: usize = labels.iter().map(|l| l[f].len()).sum();
                StringDictionaryBuilder::with_capacity(labels.len(), labels.len(), bytes)
            })
            .collect();
        for l in &labels {
            for (b, v) in builders.iter_mut().zip(l) {
                b.append_value(v);
            }
        }
        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|mut b| Arc::new(b.finish()) as ArrayRef)
            .collect();
        Ok(Arc::new(StructArray::try_new(
            label_fields(),
            arrays,
            None,
        )?))
    }
}

/// Every series in one batch. A series ends wherever *any* label's run ends, so the boundaries
/// are the sorted union of 18 run-end buffers, in the logical coordinates of this (possibly
/// sliced) batch. The labels of a series are read once, at its first row, by following each
/// field's run to its dictionary key.
fn split(batch: &RecordBatch) -> Vec<Series<'_, Vec<String>>> {
    let labels = batch.column_by_name(COL_LABELS).unwrap().as_struct();
    let runs: Vec<&RunArray<Int32Type>> = labels
        .columns()
        .iter()
        .map(|c| {
            c.as_any()
                .downcast_ref()
                .expect("RunEndEncoded<Int32> label")
        })
        .collect();
    let ts_values: &[i64] = batch
        .column_by_name(COL_TIMESTAMP)
        .unwrap()
        .as_primitive::<TimestampMillisecondType>()
        .values();
    let vs_values: &[f64] = batch
        .column_by_name(COL_VALUE)
        .unwrap()
        .as_primitive::<Float64Type>()
        .values();

    let mut bounds: Vec<usize> = Vec::new();
    for r in &runs {
        let ends = r.run_ends();
        for p in ends.get_start_physical_index()..=ends.get_end_physical_index() {
            let end = (ends.values()[p] as usize - ends.offset()).min(ends.len());
            bounds.push(end);
        }
    }
    bounds.sort_unstable();
    bounds.dedup();

    let mut out = Vec::with_capacity(bounds.len());
    let mut start = 0;
    for end in bounds {
        let labels = runs
            .iter()
            .map(|r| {
                let dict = r.values().as_dictionary::<UInt32Type>();
                let key = dict.keys().value(r.get_physical_index(start)) as usize;
                dict.values().as_string::<i32>().value(key).to_string()
            })
            .collect();
        out.push(Series {
            labels,
            ts: Cow::Borrowed(&ts_values[start..end]),
            vs: Cow::Borrowed(&vs_values[start..end]),
        });
        start = end;
    }
    out
}
