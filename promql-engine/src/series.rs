//! The canonical series shape: what crosses the seam in both directions.
//!
//! One row is one series. Its labels sit in a struct with a field per
//! label name, its samples in a list of `(timestamp, value)` structs:
//!
//! ```text
//! labels   Struct<{name}: Dictionary<UInt32, Utf8>, …>   fields sorted by name
//! samples  List<Struct<timestamp: Timestamp(ms), value: Float64>>
//! ```
//!
//! Nothing is nullable. An absent label is `""`, as it is in PromQL, so
//! there is no NULL parent to read a phantom child through. Timestamps
//! are milliseconds because that is Prometheus's unit. A single list of
//! structs rather than two aligned lists means one offsets buffer, so a
//! timestamp and its value cannot drift apart by construction.
//!
//! Every producer builds the shape through [`SeriesBatchBuilder`] and
//! every consumer checks it through [`validate`], so there is exactly one
//! definition of the shape in code, here.
//!
//! The same shape comes out of every operator: an instant vector over a
//! range query is again one row per series with a list of samples, now at
//! step timestamps. That is what lets operators stack.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, ListArray, RecordBatch, StringDictionaryBuilder,
    StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float64Type, Schema, SchemaRef, TimeUnit,
    TimestampMillisecondType, UInt32Type,
};

pub const LABELS: &str = "labels";
pub const SAMPLES: &str = "samples";
pub const TIMESTAMP: &str = "timestamp";
pub const VALUE: &str = "value";
/// Arrow's conventional name for a list's element field.
pub const LIST_ITEM: &str = "item";

/// The leaf type of every label value.
pub fn label_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
}

pub fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, None)
}

/// The two fields of one sample.
pub fn sample_fields() -> Fields {
    Fields::from(vec![
        Field::new(TIMESTAMP, timestamp_type(), false),
        Field::new(VALUE, DataType::Float64, false),
    ])
}

pub fn sample_item() -> FieldRef {
    Arc::new(Field::new(
        LIST_ITEM,
        DataType::Struct(sample_fields()),
        false,
    ))
}

pub fn samples_type() -> DataType {
    DataType::List(sample_item())
}

/// The labels struct for a given set of names. Sorted here so that two
/// producers given the same names in different orders build the same
/// schema.
pub fn labels_type(names: &[String]) -> DataType {
    let mut names: Vec<&String> = names.iter().collect();
    names.sort();
    names.dedup();
    DataType::Struct(
        names
            .into_iter()
            .map(|n| Field::new(n, label_type(), false))
            .collect(),
    )
}

pub fn schema(label_names: &[String]) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(LABELS, labels_type(label_names), false),
        Field::new(SAMPLES, samples_type(), false),
    ]))
}

/// Check a schema against the canonical shape, naming the first thing
/// wrong with it.
///
/// Strict on purpose. A store that gets this slightly wrong — a nullable
/// child, nanoseconds, plain `Utf8` — would otherwise fail somewhere
/// inside a kernel with a downcast panic, far from the code that produced
/// the batch.
pub fn validate(schema: &Schema) -> Result<(), String> {
    if schema.fields().len() != 2 {
        return Err(format!(
            "expected exactly the columns `{LABELS}` and `{SAMPLES}`, got {}",
            schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let labels = schema
        .field_with_name(LABELS)
        .map_err(|_| format!("no `{LABELS}` column"))?;
    if labels.is_nullable() {
        return Err(format!("`{LABELS}` must not be nullable"));
    }
    match labels.data_type() {
        DataType::Struct(fields) => {
            let mut prev: Option<&str> = None;
            for f in fields {
                if f.is_nullable() {
                    return Err(format!("label `{}` must not be nullable", f.name()));
                }
                if f.data_type() != &label_type() {
                    return Err(format!(
                        "label `{}` is {}, expected {}",
                        f.name(),
                        f.data_type(),
                        label_type()
                    ));
                }
                if let Some(p) = prev {
                    if p >= f.name().as_str() {
                        return Err(format!(
                            "label fields must be sorted by name and unique; `{p}` precedes `{}`",
                            f.name()
                        ));
                    }
                }
                prev = Some(f.name());
            }
        }
        other => return Err(format!("`{LABELS}` is {other}, expected a struct")),
    }

    let samples = schema
        .field_with_name(SAMPLES)
        .map_err(|_| format!("no `{SAMPLES}` column"))?;
    if samples.is_nullable() {
        return Err(format!("`{SAMPLES}` must not be nullable"));
    }
    if samples.data_type() != &samples_type() {
        return Err(format!(
            "`{SAMPLES}` is {}, expected {}",
            samples.data_type(),
            samples_type()
        ));
    }
    Ok(())
}

/// The label names a canonical schema carries, in field order.
pub fn label_names(schema: &Schema) -> Vec<String> {
    match schema.field_with_name(LABELS).map(|f| f.data_type()) {
        Ok(DataType::Struct(fields)) => fields.iter().map(|f| f.name().clone()).collect(),
        _ => Vec::new(),
    }
}

/// Builds one canonical batch, one series at a time.
///
/// The label names are fixed up front because they are the schema. A
/// series lacking one of them gets `""`; a series carrying a name not in
/// the set is an error, since silently dropping a label would merge two
/// series into one.
pub struct SeriesBatchBuilder {
    names: Vec<String>,
    labels: Vec<StringDictionaryBuilder<UInt32Type>>,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    offsets: Vec<i32>,
}

impl SeriesBatchBuilder {
    pub fn new(label_names: &[String]) -> Self {
        let mut names = label_names.to_vec();
        names.sort();
        names.dedup();
        let labels = names
            .iter()
            .map(|_| StringDictionaryBuilder::<UInt32Type>::new())
            .collect();
        Self {
            names,
            labels,
            timestamps: Vec::new(),
            values: Vec::new(),
            offsets: vec![0],
        }
    }

    /// Append one series. `samples` must be sorted ascending by timestamp;
    /// that is a promise the store makes and this builder trusts.
    pub fn push(
        &mut self,
        labels: &BTreeMap<String, String>,
        samples: &[(i64, f64)],
    ) -> Result<(), String> {
        for name in labels.keys() {
            if self.names.binary_search(name).is_err() {
                return Err(format!(
                    "label `{name}` is not among the builder's label names"
                ));
            }
        }
        for (name, b) in self.names.iter().zip(self.labels.iter_mut()) {
            b.append_value(labels.get(name).map(String::as_str).unwrap_or(""));
        }
        for (t, v) in samples {
            self.timestamps.push(*t);
            self.values.push(*v);
        }
        self.offsets.push(
            i32::try_from(self.timestamps.len())
                .map_err(|_| "more than i32::MAX samples in one batch".to_string())?,
        );
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn schema(&self) -> SchemaRef {
        schema(&self.names)
    }

    pub fn finish(mut self) -> RecordBatch {
        let n = self.len();
        let label_fields: Fields = self
            .names
            .iter()
            .map(|n| Field::new(n, label_type(), false))
            .collect();
        let children: Vec<ArrayRef> = self
            .labels
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as ArrayRef)
            .collect();
        let labels = if label_fields.is_empty() {
            StructArray::new_empty_fields(n, None)
        } else {
            StructArray::new(label_fields, children, None)
        };

        let entries = StructArray::new(
            sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(std::mem::take(
                    &mut self.timestamps,
                ))),
                Arc::new(Float64Array::from(std::mem::take(&mut self.values))),
            ],
            None,
        );
        let samples = ListArray::new(
            sample_item(),
            OffsetBuffer::new(std::mem::take(&mut self.offsets).into()),
            Arc::new(entries),
            None,
        );

        RecordBatch::try_new(self.schema(), vec![Arc::new(labels), Arc::new(samples)])
            .expect("builder output matches its own schema")
    }
}

/// One series read back out of a canonical batch.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedSeries {
    /// Labels with the empty-string placeholders removed, so this is the
    /// label set as Prometheus would print it.
    pub labels: BTreeMap<String, String>,
    pub samples: Vec<(i64, f64)>,
}

/// Read canonical batches back into plain Rust.
///
/// Series with no samples are dropped here rather than filtered in the
/// plan, because the plan-side filter would sit above the projection that
/// computes the samples and the optimizer may push it below, evaluating
/// the kernel twice. An empty series is inert for every operator anyway.
pub fn decode(batches: &[RecordBatch]) -> Result<Vec<DecodedSeries>, String> {
    let mut out = Vec::new();
    for batch in batches {
        validate(&batch.schema())?;
        let labels = batch.column_by_name(LABELS).expect("validated").as_struct();
        let samples = batch
            .column_by_name(SAMPLES)
            .expect("validated")
            .as_list::<i32>();

        // One cast per label column per batch, not per value: dictionaries
        // may have been re-keyed by an operator, and `cast` to Utf8 handles
        // every encoding uniformly.
        let mut label_columns = Vec::with_capacity(labels.num_columns());
        for (field, column) in labels.fields().iter().zip(labels.columns()) {
            let utf8 = cast(column, &DataType::Utf8).map_err(|e| e.to_string())?;
            label_columns.push((field.name().clone(), utf8));
        }

        let entries = samples.values().as_struct();
        let ts = entries
            .column_by_name(TIMESTAMP)
            .expect("validated")
            .as_primitive::<TimestampMillisecondType>();
        let vs = entries
            .column_by_name(VALUE)
            .expect("validated")
            .as_primitive::<Float64Type>();
        let offsets = samples.offsets();

        for row in 0..batch.num_rows() {
            let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
            if a == b {
                continue;
            }
            let mut label_set = BTreeMap::new();
            for (name, column) in &label_columns {
                let s = column.as_string::<i32>();
                if s.is_null(row) {
                    continue;
                }
                let v = s.value(row);
                if !v.is_empty() {
                    label_set.insert(name.clone(), v.to_string());
                }
            }
            let points = (a..b).map(|i| (ts.value(i), vs.value(i))).collect();
            out.push(DecodedSeries {
                labels: label_set,
                samples: points,
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn builder_output_validates_and_round_trips() {
        let names = vec!["pod".to_string(), "__name__".to_string()];
        let mut b = SeriesBatchBuilder::new(&names);
        b.push(
            &labels(&[("__name__", "up"), ("pod", "a")]),
            &[(0, 1.0), (30_000, 2.0)],
        )
        .unwrap();
        // A series lacking `pod` gets "" and decodes without it.
        b.push(&labels(&[("__name__", "up")]), &[(0, 3.0)]).unwrap();
        // An empty series is built but not decoded.
        b.push(&labels(&[("__name__", "up"), ("pod", "z")]), &[])
            .unwrap();
        let batch = b.finish();

        validate(&batch.schema()).unwrap();
        assert_eq!(label_names(&batch.schema()), vec!["__name__", "pod"]);

        let decoded = decode(&[batch]).unwrap();
        assert_eq!(
            decoded,
            vec![
                DecodedSeries {
                    labels: labels(&[("__name__", "up"), ("pod", "a")]),
                    samples: vec![(0, 1.0), (30_000, 2.0)],
                },
                DecodedSeries {
                    labels: labels(&[("__name__", "up")]),
                    samples: vec![(0, 3.0)],
                },
            ]
        );
    }

    #[test]
    fn a_label_outside_the_schema_is_rejected() {
        let mut b = SeriesBatchBuilder::new(&["a".to_string()]);
        let err = b.push(&labels(&[("b", "x")]), &[]).unwrap_err();
        assert!(err.contains("`b`"), "{err}");
    }

    #[test]
    fn no_labels_at_all_is_a_valid_shape() {
        let mut b = SeriesBatchBuilder::new(&[]);
        b.push(&BTreeMap::new(), &[(1, 1.0)]).unwrap();
        let batch = b.finish();
        validate(&batch.schema()).unwrap();
        assert_eq!(decode(&[batch]).unwrap().len(), 1);
    }

    #[test]
    fn validate_names_the_deviation() {
        let ok = schema(&["a".to_string()]);
        validate(&ok).unwrap();

        let nullable = Schema::new(vec![
            Field::new(LABELS, labels_type(&["a".to_string()]), true),
            Field::new(SAMPLES, samples_type(), false),
        ]);
        assert!(validate(&nullable).unwrap_err().contains("nullable"));

        let ns_item = Arc::new(Field::new(
            LIST_ITEM,
            DataType::Struct(Fields::from(vec![
                Field::new(
                    TIMESTAMP,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new(VALUE, DataType::Float64, false),
            ])),
            false,
        ));
        let nanos = Schema::new(vec![
            Field::new(LABELS, labels_type(&[]), false),
            Field::new(SAMPLES, DataType::List(ns_item), false),
        ]);
        assert!(validate(&nanos).unwrap_err().contains("expected"));

        let plain = Schema::new(vec![
            Field::new(
                LABELS,
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Utf8, false)])),
                false,
            ),
            Field::new(SAMPLES, samples_type(), false),
        ]);
        assert!(validate(&plain).unwrap_err().contains("Dictionary"));

        let unsorted = Schema::new(vec![
            Field::new(
                LABELS,
                DataType::Struct(Fields::from(vec![
                    Field::new("b", label_type(), false),
                    Field::new("a", label_type(), false),
                ])),
                false,
            ),
            Field::new(SAMPLES, samples_type(), false),
        ]);
        assert!(validate(&unsorted).unwrap_err().contains("sorted"));

        let extra = Schema::new(vec![
            Field::new(LABELS, labels_type(&[]), false),
            Field::new(SAMPLES, samples_type(), false),
            Field::new("x", DataType::Int64, false),
        ]);
        assert!(validate(&extra).unwrap_err().contains("exactly"));
    }
}
