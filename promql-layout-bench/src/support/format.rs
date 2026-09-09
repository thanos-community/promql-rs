//! The record format: the seam between the scan layer and the PromQL layer.
//!
//! This is the only thing the two phases have to agree on. Phase 1 promises to produce it,
//! phase 2 promises to consume it and nothing else.
//!
//! Two shapes cross the seam. Input is what the scan hands over, in one of the two layouts in
//! `src/list.rs` and `src/struct_ree.rs`. Output is what the range-vector operator emits, and is
//! the same whichever layout fed it: one row per series with either a scalar or a step grid
//! alongside the labels. Every column name lives here so that nothing in `support/` has to know
//! which layout it is looking at.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, Int64Array, ListArray, RecordBatch,
    StringDictionaryBuilder, StructArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit, UInt32Type,
};

use super::window::Grid;

/// Column names of the record format. Every layout uses the same names, so the queries, the
/// footprint report and the tests never have to know which layout they are looking at.
pub const COL_LABELS: &str = "labels";
/// Sample column of the row-per-sample layout.
pub const COL_TIMESTAMP: &str = "timestamp";
/// Value column of the row-per-sample layout.
pub const COL_VALUE: &str = "value";
/// Sample column of the row-per-series layout, one list per series.
pub const COL_TIMESTAMPS: &str = "timestamps";
/// Value column of the row-per-series layout, one list per series.
pub const COL_VALUES: &str = "values";
/// Step grid column of a range-query result, one list per series.
pub const COL_GRID: &str = "grid";
/// Arrow's conventional name for a list's element field.
pub const LIST_ITEM: &str = "item";
/// The label `sum by (code)` groups on.
pub const GROUP_LABEL: &str = "code";
/// A label unique to every series, used to break ties deterministically in `topk`.
pub const UNIQUE_LABEL: &str = "instance";

/// Field naming the step timestamp inside a step grid entry, in milliseconds.
pub const STEP_TS: &str = "step";
/// Field naming the value at that step.
pub const STEP_VALUE: &str = "value";

/// Millisecond timestamps, the only time type the record format uses.
pub fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, None)
}

/// The leaf type of every label value: dictionary encoded strings, `Dictionary<UInt32, Utf8>`.
/// This is what Polar Signals and Dash0 store, and what both candidates use at the leaf.
pub fn dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
}

/// Dictionary encode one column of label values. Sized exactly, because Arrow reports buffer
/// capacity as memory and a default builder would put kilobytes of slack in the footprint report.
pub fn dict(values: Vec<String>) -> DictionaryArray<UInt32Type> {
    let distinct: HashSet<&str> = values.iter().map(String::as_str).collect();
    let bytes: usize = distinct.iter().map(|s| s.len()).sum();
    let mut b =
        StringDictionaryBuilder::<UInt32Type>::with_capacity(values.len(), distinct.len(), bytes);
    for v in &values {
        b.append_value(v);
    }
    b.finish()
}

/// Labels as one struct field per label, dictionary encoded at the leaf. The labels column of the
/// row-per-series layout and of every operator output.
pub fn label_fields() -> Fields {
    LABEL_NAMES
        .iter()
        .map(|n| Field::new(*n, dict_type(), false))
        .collect()
}

/// Schema of a row-per-sample table that keeps its labels in one column. Only the labels
/// column's encoding varies.
pub fn flat_schema(labels_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_LABELS, labels_type, false),
        Field::new(COL_TIMESTAMP, timestamp_type(), false),
        Field::new(COL_VALUE, DataType::Float64, false),
    ]))
}

/// The element field of a non-nullable list column.
pub fn list_item(item: DataType) -> FieldRef {
    Arc::new(Field::new(LIST_ITEM, item, false))
}

/// One entry of a step grid. Carrying the step timestamp alongside the value means the unnest
/// yields a usable `GROUP BY` key without relying on two lists staying positionally aligned.
pub fn step_fields() -> Fields {
    Fields::from(vec![
        Field::new(STEP_TS, DataType::Int64, false),
        Field::new(STEP_VALUE, DataType::Float64, false),
    ])
}

/// A step grid: one list of `(step, value)` per series.
pub fn step_grid_type() -> DataType {
    DataType::List(list_item(DataType::Struct(step_fields())))
}

/// Pack per-series step results into a step grid column. A step with no value is left out of the
/// series' list, the way PromQL leaves a series out of a step it has no rate at.
pub fn step_grid_array(results: &[Vec<Option<f64>>], grid: Grid) -> ListArray {
    let mut steps: Vec<i64> = Vec::new();
    let mut vals: Vec<f64> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(results.len() + 1);
    offsets.push(0);
    for r in results {
        for (s, v) in r.iter().enumerate() {
            if let Some(v) = v {
                steps.push(grid.at(s));
                vals.push(*v);
            }
        }
        offsets.push(steps.len() as i32);
    }
    let entries = StructArray::try_new(
        step_fields(),
        vec![
            Arc::new(Int64Array::from(steps)) as ArrayRef,
            Arc::new(Float64Array::from(vals)) as ArrayRef,
        ],
        None,
    )
    .expect("step entries");
    ListArray::new(
        list_item(DataType::Struct(step_fields())),
        OffsetBuffer::new(offsets.into()),
        Arc::new(entries),
        None,
    )
}

/// What the range-vector operator emits, whichever layout fed it. One row per series; a scalar
/// for an instant query, a step grid for a range query.
pub fn output_schema(instant: bool) -> SchemaRef {
    let result = if instant {
        Field::new(COL_VALUE, DataType::Float64, false)
    } else {
        Field::new(COL_GRID, step_grid_type(), false)
    };
    Arc::new(Schema::new(vec![
        Field::new(COL_LABELS, DataType::Struct(label_fields()), false),
        result,
    ]))
}

/// The label names carried by `apiserver_request_total` on the cluster the design note is based
/// on. The scan layer invents values for these; the PromQL layer only ever reads `code`.
pub const LABEL_NAMES: [&str; 18] = [
    "code",
    "component",
    "container",
    "dry_run",
    "endpoint",
    "group",
    "instance",
    "job",
    "namespace",
    "pod",
    "prometheus",
    "prometheus_replica",
    "resource",
    "scope",
    "service",
    "subresource",
    "verb",
    "version",
];

/// Position of a label in [`LABEL_NAMES`].
pub fn label_index(name: &str) -> usize {
    LABEL_NAMES
        .iter()
        .position(|n| *n == name)
        .unwrap_or_else(|| panic!("unknown label {name}"))
}

/// Per-column memory footprint of a batch, in bytes.
pub fn footprint(batch: &RecordBatch) -> Vec<(String, usize)> {
    batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, c)| (f.name().clone(), c.get_array_memory_size()))
        .collect()
}

/// Bytes held by the labels column.
pub fn labels_bytes(batch: &RecordBatch) -> usize {
    batch
        .column_by_name(COL_LABELS)
        .map_or(0, |c| c.get_array_memory_size())
}

/// Bytes held by the whole batch.
pub fn total_bytes(batch: &RecordBatch) -> usize {
    footprint(batch).into_iter().map(|(_, bytes)| bytes).sum()
}
