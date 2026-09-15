//! The canonical series shape: what a store hands the engine, and what
//! every operator hands the next.
//!
//! One row is one series. Its labels sit in a struct with a field per
//! label name, its samples in a list of `(timestamp, value)` structs:
//!
//! ```text
//! labels   Struct<{name}: Utf8View, …>   fields sorted by name
//! samples  List<Struct<timestamp: Timestamp(ms), value: Float64>>
//! ```
//!
//! Label values are `Utf8View`, one 16-byte view per label per series with
//! values up to 12 bytes inline. Within one series a value appears exactly
//! once, so a dictionary would encode nothing there, and across a batch
//! the engine cannot use dictionary keys: DataFusion hydrates them at the
//! first group-by, while `Utf8View` has its fast path and is what Vortex
//! and Parquet hand over.
//!
//! A series is identified by its label set: one value per label name, and
//! the set never changes. A batch is many such series side by side and
//! nothing more. Each row keeps its own complete label set and its own
//! samples; rows share only the schema, the union of their label names.
//! Two rows with one label set would be one series split in two, which
//! [`encode`] refuses. Merging series by the labels that survive an
//! aggregation is an operator above this shape, not part of it.
//!
//! Nothing is nullable. An absent label is `""`, as it is in PromQL, so
//! there is no NULL parent to read a phantom child through. Timestamps
//! are milliseconds because that is Prometheus's unit. A single list of
//! structs rather than two aligned lists means one offsets buffer, so a
//! timestamp and its value cannot drift apart by construction.
//!
//! A [`Series`] is one row of it, held as its two column values. A store
//! puts rows together with [`encode`], the engine reads them back with
//! [`decode`], and every consumer checks the shape through [`validate`],
//! so there is exactly one definition of the shape in code, here.
//!
//! The same shape comes out of every operator: an instant vector over a
//! range query is again one row per series with a list of samples, now at
//! step timestamps. That is what lets operators stack.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, Float64Array, ListArray, RecordBatch,
    StringViewArray, StringViewBuilder, StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float64Type, Schema, SchemaRef, TimeUnit,
    TimestampMillisecondType,
};

pub const LABELS: &str = "labels";
pub const SAMPLES: &str = "samples";
pub const TIMESTAMP: &str = "timestamp";
pub const VALUE: &str = "value";
/// Arrow's conventional name for a list's element field.
pub const LIST_ITEM: &str = "item";

/// The leaf type of every label value: one 16-byte view per row, values
/// up to 12 bytes inline. The module doc says why not a dictionary.
pub fn label_type() -> DataType {
    DataType::Utf8View
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

/// One series: one row of the canonical shape, held as its two column
/// values.
///
/// A series is its label set. A different label set is a different
/// series, and the label set never changes, so a `Series` is immutable:
/// deriving one from another, as [`Series::clip`] does, yields a new
/// `Series` sharing the buffers. It is Prometheus's `promql.Series`, the
/// pair `Metric` and `Floats`, in Arrow.
#[derive(Debug, Clone)]
pub struct Series {
    /// One row: one field per label name, names sorted, values strings.
    labels: StructArray,
    /// The `(timestamp, value)` structs, timestamps strictly ascending.
    samples: StructArray,
}

impl Series {
    /// Build one from plain values, checking everything once: the two
    /// vectors have one length, the timestamps ascend, no label name is
    /// given twice. Labels are sorted by name, and a `""` value is not
    /// stored because that is how PromQL spells an absent label. The
    /// vectors become Arrow arrays without a copy.
    pub fn new(
        labels: &[(&str, &str)],
        timestamps: Vec<i64>,
        values: Vec<f64>,
    ) -> Result<Series, String> {
        if timestamps.len() != values.len() {
            return Err(format!(
                "{} timestamps but {} values",
                timestamps.len(),
                values.len()
            ));
        }
        if let Some(w) = timestamps.windows(2).find(|w| w[0] >= w[1]) {
            return Err(format!(
                "timestamps must be strictly ascending; {} is followed by {}",
                w[0], w[1]
            ));
        }
        let mut pairs: Vec<(&str, &str)> = labels
            .iter()
            .copied()
            .filter(|(_, v)| !v.is_empty())
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        if let Some(w) = pairs.windows(2).find(|w| w[0].0 == w[1].0) {
            return Err(format!("label `{}` is given twice", w[0].0));
        }

        let fields: Fields = pairs
            .iter()
            .map(|(n, _)| Field::new(*n, label_type(), false))
            .collect();
        let columns: Vec<ArrayRef> = pairs
            .iter()
            .map(|(_, v)| {
                Arc::new(StringViewArray::from_iter_values(std::iter::once(*v))) as ArrayRef
            })
            .collect();
        let labels = if fields.is_empty() {
            StructArray::new_empty_fields(1, None)
        } else {
            StructArray::new(fields, columns, None)
        };
        let samples = StructArray::new(
            sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                Arc::new(Float64Array::from(values)),
            ],
            None,
        );
        Ok(Series { labels, samples })
    }

    /// The value of one label, `""` when the series does not have it, as
    /// in PromQL.
    pub fn label(&self, name: &str) -> &str {
        self.labels
            .column_by_name(name)
            .map_or("", |c| view_value(c, 0))
    }

    /// The label set as Prometheus prints it: sorted by name, without the
    /// `""` placeholders a batch stores for labels a series lacks.
    pub fn labels(&self) -> impl Iterator<Item = (&str, &str)> {
        self.labels
            .fields()
            .iter()
            .zip(self.labels.columns())
            .map(|(f, c)| (f.name().as_str(), view_value(c, 0)))
            .filter(|(_, v)| !v.is_empty())
    }

    /// Milliseconds, strictly ascending.
    pub fn timestamps(&self) -> &[i64] {
        self.samples
            .column_by_name(TIMESTAMP)
            .expect("canonical")
            .as_primitive::<TimestampMillisecondType>()
            .values()
    }

    /// One per timestamp.
    pub fn values(&self) -> &[f64] {
        self.samples
            .column_by_name(VALUE)
            .expect("canonical")
            .as_primitive::<Float64Type>()
            .values()
    }

    /// The same series with only the samples in `[start_ms, end_ms]`,
    /// sharing the buffers: two binary searches and a slice.
    pub fn clip(&self, start_ms: i64, end_ms: i64) -> Series {
        let ts = self.timestamps();
        let lo = ts.partition_point(|t| *t < start_ms);
        let hi = ts.partition_point(|t| *t <= end_ms).max(lo);
        Series {
            labels: self.labels.clone(),
            samples: self.samples.slice(lo, hi - lo),
        }
    }
}

/// The string at `row` of one label column. The schema forbids nulls; a
/// null still reads as `""` rather than panicking.
fn view_value(column: &ArrayRef, row: usize) -> &str {
    let views = column.as_string_view();
    if views.is_null(row) {
        ""
    } else {
        views.value(row)
    }
}

/// The sorted union of the series' label names: the schema a batch of
/// them needs.
pub fn label_names_of(series: &[Series]) -> Vec<String> {
    let mut names: Vec<String> = series
        .iter()
        .flat_map(|s| s.labels().map(|(n, _)| n.to_string()))
        .collect();
    names.sort();
    names.dedup();
    names
}

/// One batch, one row per series, in the schema for `names`.
///
/// The names are a parameter rather than derived here because a store
/// that streams several batches for one scan must give them all the same
/// schema. A series lacking one of the names gets `""`; a series carrying
/// a label outside them is an error, since silently dropping a label
/// would merge two series into one. Two series with the same label set
/// are an error too: a series is its label set, so that is one series
/// split in two.
pub fn encode(names: &[String], series: &[Series]) -> Result<RecordBatch, String> {
    let mut names = names.to_vec();
    names.sort();
    names.dedup();

    // A value longer than 12 bytes that repeats across series is stored once.
    let mut columns: Vec<StringViewBuilder> = names
        .iter()
        .map(|_| StringViewBuilder::new().with_deduplicate_strings())
        .collect();
    let mut seen: HashSet<Vec<&str>> = HashSet::with_capacity(series.len());
    let mut offsets: Vec<i32> = Vec::with_capacity(series.len() + 1);
    offsets.push(0);
    let mut total = 0usize;
    for s in series {
        for (name, _) in s.labels() {
            if names.binary_search_by(|n| n.as_str().cmp(name)).is_err() {
                return Err(format!(
                    "label `{name}` is not among the batch's label names"
                ));
            }
        }
        let values: Vec<&str> = names.iter().map(|n| s.label(n)).collect();
        for (column, value) in columns.iter_mut().zip(&values) {
            column.append_value(value);
        }
        if !seen.insert(values) {
            let set: Vec<String> = s.labels().map(|(n, v)| format!("{n}={v:?}")).collect();
            return Err(format!(
                "two series in one batch share the label set {{{}}}",
                set.join(", ")
            ));
        }
        total += s.samples.len();
        offsets.push(
            i32::try_from(total)
                .map_err(|_| "more than i32::MAX samples in one batch".to_string())?,
        );
    }

    let label_fields: Fields = names
        .iter()
        .map(|n| Field::new(n, label_type(), false))
        .collect();
    let labels = if label_fields.is_empty() {
        StructArray::new_empty_fields(series.len(), None)
    } else {
        let columns: Vec<ArrayRef> = columns
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as ArrayRef)
            .collect();
        StructArray::new(label_fields, columns, None)
    };
    let entries = if series.is_empty() {
        new_empty_array(&DataType::Struct(sample_fields()))
    } else {
        let parts: Vec<&dyn Array> = series.iter().map(|s| &s.samples as &dyn Array).collect();
        concat(&parts).map_err(|e| e.to_string())?
    };
    let samples = ListArray::new(
        sample_item(),
        OffsetBuffer::new(offsets.into()),
        entries,
        None,
    );
    RecordBatch::try_new(schema(&names), vec![Arc::new(labels), Arc::new(samples)])
        .map_err(|e| e.to_string())
}

/// The rows of canonical batches, as zero-copy slices.
///
/// Series with no samples are dropped here rather than filtered in the
/// plan, because the plan-side filter would sit above the projection that
/// computes the samples and the optimizer may push it below, evaluating
/// the kernel twice. An empty series is inert for every operator anyway.
pub fn decode(batches: &[RecordBatch]) -> Result<Vec<Series>, String> {
    let mut out = Vec::new();
    for batch in batches {
        validate(&batch.schema())?;
        let labels = batch.column_by_name(LABELS).expect("validated").as_struct();
        let samples = batch
            .column_by_name(SAMPLES)
            .expect("validated")
            .as_list::<i32>();
        for row in 0..batch.num_rows() {
            if samples.value_length(row) == 0 {
                continue;
            }
            out.push(Series {
                labels: labels.slice(row, 1),
                samples: samples.value(row).as_struct().clone(),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> Series {
        let (ts, vs) = samples.iter().copied().unzip();
        Series::new(labels, ts, vs).unwrap()
    }

    #[test]
    fn a_batch_validates_and_round_trips() {
        let all = [
            series(
                &[("__name__", "up"), ("pod", "a")],
                &[(0, 1.0), (30_000, 2.0)],
            ),
            // A series lacking `pod` gets "" in the batch and reads back
            // without it.
            series(&[("__name__", "up")], &[(0, 3.0)]),
            // An empty series is encoded but not decoded.
            series(&[("__name__", "up"), ("pod", "z")], &[]),
        ];
        let names = label_names_of(&all);
        assert_eq!(names, vec!["__name__", "pod"]);
        let batch = encode(&names, &all).unwrap();
        assert_eq!(batch.num_rows(), 3);
        validate(&batch.schema()).unwrap();
        assert_eq!(label_names(&batch.schema()), vec!["__name__", "pod"]);

        let decoded = decode(&[batch]).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(
            decoded[0].labels().collect::<Vec<_>>(),
            vec![("__name__", "up"), ("pod", "a")]
        );
        assert_eq!(decoded[0].timestamps(), [0, 30_000]);
        assert_eq!(decoded[0].values(), [1.0, 2.0]);
        assert_eq!(
            decoded[1].labels().collect::<Vec<_>>(),
            vec![("__name__", "up")]
        );
        assert_eq!(decoded[1].label("pod"), "");
        assert_eq!(decoded[1].timestamps(), [0]);
        assert_eq!(decoded[1].values(), [3.0]);
    }

    /// Three series that differ in one label are three series, each with
    /// its own complete label set and its own samples, before and after
    /// they share a batch.
    #[test]
    fn three_series_that_differ_in_one_label_are_three_series() {
        let common = [("status", "200"), ("method", "GET"), ("job", "api")];
        let with_pod = |pod: &'static str| {
            let mut labels = common.to_vec();
            labels.push(("pod", pod));
            labels
        };
        let all = [
            series(&with_pod("a"), &[(0, 1.0), (1, 1.0), (2, 1.0)]),
            series(&with_pod("b"), &[(0, 2.0), (1, 2.0), (2, 2.0)]),
            series(&with_pod("c"), &[(0, 0.0), (1, 1.0), (2, 1.0)]),
        ];
        for (s, pod) in all.iter().zip(["a", "b", "c"]) {
            assert_eq!(
                s.labels().collect::<Vec<_>>(),
                vec![
                    ("job", "api"),
                    ("method", "GET"),
                    ("pod", pod),
                    ("status", "200")
                ]
            );
        }

        let batch = encode(&label_names_of(&all), &all).unwrap();
        assert_eq!(batch.num_rows(), 3);
        let decoded = decode(&[batch]).unwrap();
        assert_eq!(decoded.len(), 3);
        for (before, after) in all.iter().zip(&decoded) {
            assert_eq!(
                before.labels().collect::<Vec<_>>(),
                after.labels().collect::<Vec<_>>()
            );
            assert_eq!(before.timestamps(), after.timestamps());
            assert_eq!(before.values(), after.values());
        }
        assert_eq!(decoded[1].label("pod"), "b");
        assert_eq!(decoded[2].values(), [0.0, 1.0, 1.0]);
    }

    #[test]
    fn two_series_with_one_label_set_are_rejected() {
        let twice = [
            series(&[("a", "1")], &[(0, 1.0)]),
            series(&[("a", "1")], &[(5, 2.0)]),
        ];
        let err = encode(&label_names_of(&twice), &twice).unwrap_err();
        assert!(err.contains(r#"a="1""#), "{err}");

        // The empty label set is a label set too.
        let bare = [series(&[], &[(0, 1.0)]), series(&[], &[(1, 1.0)])];
        assert!(encode(&[], &bare).is_err());
    }

    #[test]
    fn a_label_outside_the_schema_is_rejected() {
        let err = encode(&["a".to_string()], &[series(&[("b", "x")], &[])]).unwrap_err();
        assert!(err.contains("`b`"), "{err}");
    }

    #[test]
    fn no_labels_at_all_is_a_valid_shape() {
        let batch = encode(&[], &[series(&[], &[(1, 1.0)])]).unwrap();
        validate(&batch.schema()).unwrap();
        assert_eq!(decode(&[batch]).unwrap().len(), 1);

        let empty = encode(&[], &[]).unwrap();
        validate(&empty.schema()).unwrap();
        assert_eq!(empty.num_rows(), 0);
    }

    #[test]
    fn a_series_is_checked_once_when_built() {
        let err = Series::new(&[], vec![0, 1], vec![1.0]).unwrap_err();
        assert!(err.contains("values"), "{err}");
        let err = Series::new(&[], vec![1, 1], vec![1.0, 2.0]).unwrap_err();
        assert!(err.contains("ascending"), "{err}");
        let err = Series::new(&[("a", "1"), ("a", "2")], vec![], vec![]).unwrap_err();
        assert!(err.contains("`a`"), "{err}");

        // Sorted by name, and "" is how PromQL spells an absent label.
        let s = series(&[("b", "2"), ("a", ""), ("c", "3")], &[]);
        assert_eq!(s.labels().collect::<Vec<_>>(), vec![("b", "2"), ("c", "3")]);
        assert_eq!(s.label("a"), "");
        assert_eq!(s.label("b"), "2");
        assert_eq!(s.label("nope"), "");
    }

    #[test]
    fn clip_keeps_the_closed_range_and_shares_the_buffers() {
        let s = series(&[("a", "1")], &[(0, 1.0), (10, 2.0), (20, 3.0), (30, 4.0)]);
        let c = s.clip(10, 20);
        assert_eq!(c.timestamps(), [10, 20]);
        assert_eq!(c.values(), [2.0, 3.0]);
        assert_eq!(c.label("a"), "1");
        assert_eq!(c.values().as_ptr(), s.values()[1..].as_ptr());
        assert!(s.clip(31, 40).timestamps().is_empty());
        assert_eq!(s.clip(-5, 40).timestamps().len(), 4);
        assert!(s.clip(20, 10).timestamps().is_empty());
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
        assert!(validate(&plain).unwrap_err().contains("expected Utf8View"));

        let dictionary = Schema::new(vec![
            Field::new(
                LABELS,
                DataType::Struct(Fields::from(vec![Field::new(
                    "a",
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                    false,
                )])),
                false,
            ),
            Field::new(SAMPLES, samples_type(), false),
        ]);
        assert!(validate(&dictionary)
            .unwrap_err()
            .contains("expected Utf8View"));

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
