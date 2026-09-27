//! The canonical series shape: what a store hands the engine, and what
//! every operator hands the next.
//!
//! One row is one series: a label set, one chunk of its samples, and
//! the block the store cut it into. The labels sit in a struct with a
//! field per label name, the samples in a list of `(timestamp, value)`
//! structs, and the block is two timestamps:
//!
//! ```text
//! labels       Struct<{name}: Utf8View, …>   fields sorted by name
//! samples      List<Struct<timestamp: Timestamp(ms), value: Float64>>
//! block_start  Timestamp(ms)                 window ends the block answers, from here
//! block_end    Timestamp(ms)                 to here, exclusive
//! ```
//!
//! A store cuts its answer into blocks along its own time units and
//! stamps every series with its block, constant over the block. A block
//! answers the steps whose window end lies in `[block_start, block_end)`
//! and its series hold every sample those windows reach, so nothing per
//! series has to survive a block edge; `docs/series-source.md` is the
//! contract. A store that keeps whole series stamps the whole select as
//! one block. The columns are plain timestamps rather than run-end
//! encoded because the fold groups on them, and DataFusion's group
//! values do not take a run-end array.
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
//! A label set with several chunks in a block arrives once per chunk;
//! [`encode`] is for a store that holds each series whole, so it refuses
//! two series with one label set, unable to tell a second chunk from a
//! duplicate. Merging series by the labels that survive an aggregation is
//! an operator above this shape, not part of it.
//!
//! Nothing is nullable. An absent label is `""`, as it is in PromQL, so
//! there is no NULL parent to read a phantom child through. Timestamps
//! are milliseconds because that is Prometheus's unit. A single list of
//! structs rather than two aligned lists means one offsets buffer, so a
//! timestamp and its value cannot drift apart by construction.
//!
//! A [`Series`] is one row of it, held as its label set and samples. A
//! store puts rows together with [`encode`], the engine reads them back
//! with [`decode`], and every consumer checks the shape through
//! [`validate`], so there is exactly one definition of the shape in
//! code, here.
//!
//! The same shape comes out of every operator: an instant vector over a
//! range query is again one row per series with a list of samples, now at
//! step timestamps, in the block that answered them. That is what lets
//! operators stack, and why a label set seen in several blocks leaves as
//! several rows.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, Float64Array,
    ListArray, RecordBatch, StringViewArray, StringViewBuilder, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{concat, filter, filter_record_batch};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float64Type, Schema, SchemaRef, TimeUnit,
    TimestampMillisecondType,
};

pub const LABELS: &str = "labels";
pub const SAMPLES: &str = "samples";
pub const TIMESTAMP: &str = "timestamp";
pub const VALUE: &str = "value";
pub const BLOCK_START: &str = "block_start";
pub const BLOCK_END: &str = "block_end";
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
        Field::new(BLOCK_START, timestamp_type(), false),
        Field::new(BLOCK_END, timestamp_type(), false),
    ]))
}

/// The block a series belongs to: it answers the steps whose window end
/// lies in `[start_ms, end_ms)`, and holds every sample those windows
/// reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Block {
    pub start_ms: i64,
    pub end_ms: i64,
}

impl Block {
    /// The two block columns for `rows` series of this block.
    pub fn columns(&self, rows: usize) -> [ArrayRef; 2] {
        [
            Arc::new(TimestampMillisecondArray::from(vec![self.start_ms; rows])),
            Arc::new(TimestampMillisecondArray::from(vec![self.end_ms; rows])),
        ]
    }
}

/// `batch` with its block columns set to `block`, for a store that cuts
/// a batch it already holds into blocks.
pub fn with_block(batch: &RecordBatch, block: Block) -> Result<RecordBatch, String> {
    let [start, end] = block.columns(batch.num_rows());
    let columns: Vec<ArrayRef> = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, c)| match f.name().as_str() {
            BLOCK_START => Arc::clone(&start),
            BLOCK_END => Arc::clone(&end),
            _ => Arc::clone(c),
        })
        .collect();
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| e.to_string())
}

/// The block of row `row` of a canonical batch.
pub fn block_of(batch: &RecordBatch, row: usize) -> Block {
    let at = |name: &str| {
        batch
            .column_by_name(name)
            .expect("canonical")
            .as_primitive::<TimestampMillisecondType>()
            .value(row)
    };
    Block {
        start_ms: at(BLOCK_START),
        end_ms: at(BLOCK_END),
    }
}

/// Check a schema against the canonical shape, naming the first thing
/// wrong with it.
///
/// Strict on purpose. A store that gets this slightly wrong — a nullable
/// child, nanoseconds, plain `Utf8` — would otherwise fail somewhere
/// inside a kernel with a downcast panic, far from the code that produced
/// the batch.
pub fn validate(schema: &Schema) -> Result<(), String> {
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    if names != [LABELS, SAMPLES, BLOCK_START, BLOCK_END] {
        return Err(format!(
            "expected exactly the columns `{LABELS}`, `{SAMPLES}`, `{BLOCK_START}` and \
             `{BLOCK_END}`, in that order, got {}",
            names.join(", ")
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
    for name in [BLOCK_START, BLOCK_END] {
        let field = schema.field_with_name(name).expect("checked above");
        if field.is_nullable() {
            return Err(format!("`{name}` must not be nullable"));
        }
        if field.data_type() != &timestamp_type() {
            return Err(format!(
                "`{name}` is {}, expected {}",
                field.data_type(),
                timestamp_type()
            ));
        }
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
/// Prometheus's `promql.Series`, the pair `Metric` and `Floats`, in
/// Arrow. Immutable, because a different label set is a different series.
/// This is the row-at-a-time view that tests and [`decode`] hand back; a
/// store holds a whole batch and works on it with kernels instead. The
/// block is not part of it: a whole series is one block's worth by
/// definition, and a result decoded from several blocks is several of
/// these per label set, one per block.
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

/// One batch, one row per series, in the schema for `names`, every row
/// in `block`.
///
/// For a store that holds each series whole: the one-block, one-chunk
/// case of the contract. The names are a parameter rather than derived
/// here because a store that streams several batches for one scan must
/// give them all the same schema. A series lacking one of the names gets
/// `""`; a series carrying a label outside them is an error, since
/// silently dropping a label would merge two series into one. Two series
/// with the same label set are an error too: with whole series this
/// cannot be a second chunk, so it is one series split in two.
pub fn encode(names: &[String], series: &[Series], block: Block) -> Result<RecordBatch, String> {
    let mut names = names.to_vec();
    names.sort();
    names.dedup();

    // A value longer than 12 bytes that repeats across series is stored once.
    let mut columns: Vec<StringViewBuilder> = names
        .iter()
        .map(|_| StringViewBuilder::new().with_deduplicate_strings())
        .collect();
    // A row is hashed as it is written rather than collected into a key,
    // so encoding allocates nothing per series; a collision costs only the
    // label-by-label comparison against the rows already in that bucket.
    let mut seen: HashMap<u64, Vec<usize>> = HashMap::with_capacity(series.len());
    let mut offsets: Vec<i32> = Vec::with_capacity(series.len() + 1);
    offsets.push(0);
    let mut total = 0usize;
    for (row, s) in series.iter().enumerate() {
        // Both `s.labels()` and `names` are sorted by name, so a merge
        // walk finds each column's value, or reports a label the schema
        // doesn't carry, in one linear pass instead of a `column_by_name`
        // scan per label per series.
        let mut labels = s.labels().peekable();
        let mut hasher = DefaultHasher::new();
        for (column, name) in columns.iter_mut().zip(&names) {
            if let Some((n, _)) = labels.peek() {
                if *n < name.as_str() {
                    return Err(format!("label `{n}` is not among the batch's label names"));
                }
            }
            let value = match labels.peek() {
                Some((n, v)) if *n == name.as_str() => {
                    let v = *v;
                    labels.next();
                    v
                }
                _ => "",
            };
            column.append_value(value);
            value.hash(&mut hasher);
        }
        if let Some((n, _)) = labels.next() {
            return Err(format!("label `{n}` is not among the batch's label names"));
        }
        let bucket = seen.entry(hasher.finish()).or_default();
        if bucket
            .iter()
            .any(|&other| names.iter().all(|n| series[other].label(n) == s.label(n)))
        {
            let set: Vec<String> = s.labels().map(|(n, v)| format!("{n}={v:?}")).collect();
            return Err(format!(
                "two series in one batch share the label set {{{}}}",
                set.join(", ")
            ));
        }
        bucket.push(row);
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
    let [block_start, block_end] = block.columns(series.len());
    RecordBatch::try_new(
        schema(&names),
        vec![Arc::new(labels), Arc::new(samples), block_start, block_end],
    )
    .map_err(|e| e.to_string())
}

/// The rows of canonical batches, as zero-copy slices, one [`Series`]
/// per row: a label set that arrived in several blocks is several.
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

/// A canonical batch with every empty-samples row removed.
///
/// Pulled out of [`decode`] so the engine's public API can hand back
/// Arrow batches with this one piece of decode's semantics already
/// applied: the module doc on [`decode`] explains why dropping empty
/// series can't be a plan-side filter instead.
pub fn drop_empty(batch: &RecordBatch) -> RecordBatch {
    let samples = batch
        .column_by_name(SAMPLES)
        .expect("validated")
        .as_list::<i32>();
    if (0..batch.num_rows()).all(|row| samples.value_length(row) > 0) {
        return batch.clone();
    }
    let mask = BooleanArray::from_iter(
        (0..batch.num_rows()).map(|row| Some(samples.value_length(row) > 0)),
    );
    filter_record_batch(batch, &mask).expect("mask has one entry per row")
}

/// Drop the label columns no row in `batch` carries.
///
/// The store's schema is the union over everything it holds, and a
/// selection is usually much narrower. Keeping an all-`""` column would
/// be correct — that is how the shape spells an absent label — but it
/// would make the schema depend on what else is in the store, so two
/// stores holding the same series would answer one query with different
/// schemas.
pub fn drop_unused_labels(batch: &RecordBatch) -> Result<RecordBatch, String> {
    let labels = batch.column_by_name(LABELS).expect("canonical").as_struct();
    let used: Vec<usize> = (0..labels.num_columns())
        .filter(|&i| {
            // A `Utf8View`'s low 32 bits are its length, so a non-empty
            // value is visible in the view word without materializing
            // the `&str` behind it.
            let values = labels.column(i).as_string_view();
            values.views().iter().any(|v| *v as u32 != 0)
        })
        .collect();
    if used.len() == labels.num_columns() {
        return Ok(batch.clone());
    }

    let names: Vec<String> = used
        .iter()
        .map(|&i| labels.fields()[i].name().clone())
        .collect();
    let kept: ArrayRef = if used.is_empty() {
        Arc::new(StructArray::new_empty_fields(batch.num_rows(), None))
    } else {
        Arc::new(StructArray::new(
            used.iter().map(|&i| labels.fields()[i].clone()).collect(),
            used.iter().map(|&i| Arc::clone(labels.column(i))).collect(),
            None,
        ))
    };
    replace_column(
        batch,
        LABELS,
        Field::new(LABELS, labels_type(&names), false),
        kept,
    )
}

/// `batch` with the column `name` swapped for `column`, typed `field`;
/// the other columns, the block's included, stay as they are.
fn replace_column(
    batch: &RecordBatch,
    name: &str,
    field: Field,
    column: ArrayRef,
) -> Result<RecordBatch, String> {
    let fields: Vec<FieldRef> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            if f.name() == name {
                Arc::new(field.clone())
            } else {
                Arc::clone(f)
            }
        })
        .collect();
    let columns: Vec<ArrayRef> = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, c)| {
            if f.name() == name {
                Arc::clone(&column)
            } else {
                Arc::clone(c)
            }
        })
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| e.to_string())
}

/// Keep only the samples in `[start_ms, end_ms]`.
///
/// Timestamps ascend within a row, so two binary searches per row give the
/// kept range without looking at a sample. Those ranges then drive one
/// `filter` over the shared child array, so the samples are copied once
/// and the ones outside the range are never touched. A batch whose rows
/// all lie inside the range — the common case for a store that already
/// pruned by time — is handed back untouched.
pub fn clip(batch: &RecordBatch, start_ms: i64, end_ms: i64) -> Result<RecordBatch, String> {
    let samples = batch
        .column_by_name(SAMPLES)
        .expect("canonical")
        .as_list::<i32>();
    let entries = samples.values().as_struct();
    let timestamps = entries
        .column_by_name(TIMESTAMP)
        .expect("canonical")
        .as_primitive::<TimestampMillisecondType>()
        .values();

    // A first pass finds each row's kept range and whether any row needs
    // clipping at all; a second, only entered when one does, builds the
    // mask run-wise (the gap before the range, then the range itself) in
    // one pass over each row rather than a `set_bit` per kept sample.
    let offsets = samples.offsets();
    let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(batch.num_rows());
    let mut untouched = true;
    for row in 0..batch.num_rows() {
        let (from, to) = (offsets[row] as usize, offsets[row + 1] as usize);
        let window = &timestamps[from..to];
        let lo = from + window.partition_point(|t| *t < start_ms);
        let hi = (from + window.partition_point(|t| *t <= end_ms)).max(lo);
        untouched &= lo == from && hi == to;
        ranges.push((lo, hi));
    }
    if untouched {
        return Ok(batch.clone());
    }

    let mut mask = BooleanBufferBuilder::new(entries.len());
    let mut clipped: Vec<i32> = Vec::with_capacity(ranges.len() + 1);
    clipped.push(0);
    let mut total = 0i32;
    let mut cursor = 0usize;
    for (lo, hi) in ranges {
        mask.append_n(lo - cursor, false);
        mask.append_n(hi - lo, true);
        cursor = hi;
        total += (hi - lo) as i32;
        clipped.push(total);
    }
    mask.append_n(entries.len() - cursor, false);

    let entries =
        filter(entries, &BooleanArray::new(mask.finish(), None)).map_err(|e| e.to_string())?;
    let samples = ListArray::new(
        sample_item(),
        OffsetBuffer::new(clipped.into()),
        entries,
        None,
    );
    replace_column(
        batch,
        SAMPLES,
        Field::new(SAMPLES, samples_type(), false),
        Arc::new(samples),
    )
}

/// The timestamp and value children of a samples list's entries. Callers
/// hold a column whose type the signature or [`validate`] already checked.
pub(crate) fn sample_slices(s: &StructArray) -> (&[i64], &[f64]) {
    let ts = s
        .column_by_name(TIMESTAMP)
        .expect("a canonical samples entry")
        .as_primitive::<TimestampMillisecondType>()
        .values();
    let vs = s
        .column_by_name(VALUE)
        .expect("a canonical samples entry")
        .as_primitive::<Float64Type>()
        .values();
    (ts, vs)
}

/// Rows of a samples column under construction. Arrow in, Arrow out: the
/// buffers are those of the canonical samples column, handed over without
/// a copy, so a kernel writes its output once.
///
/// The row after the last finished one is open: samples pushed since, not
/// yet visible to [`take_first`](Self::take_first).
#[derive(Debug)]
pub(crate) struct SamplesBuilder {
    ts: Vec<i64>,
    vs: Vec<f64>,
    offsets: Vec<i32>,
}

impl Default for SamplesBuilder {
    fn default() -> Self {
        Self {
            ts: Vec::new(),
            vs: Vec::new(),
            offsets: vec![0],
        }
    }
}

impl SamplesBuilder {
    pub(crate) fn reserve(&mut self, samples: usize) {
        self.ts.reserve(samples);
        self.vs.reserve(samples);
    }

    pub(crate) fn push(&mut self, t: i64, v: f64) {
        self.ts.push(t);
        self.vs.push(v);
    }

    /// Past `i32::MAX` samples the offsets cannot say where a row ends, and
    /// a wrapped offset would hand DataFusion a corrupt list.
    pub(crate) fn finish_row(&mut self) {
        let end = i32::try_from(self.ts.len()).expect("more than i32::MAX samples in one column");
        self.offsets.push(end);
    }

    /// Rows finished and not yet taken.
    pub(crate) fn rows(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Splits the first n finished rows off as the canonical samples ListArray.
    ///
    /// The head keeps the vectors' allocations and becomes the Arrow
    /// buffers; what is copied is the tail, the rows after `n` plus the
    /// open one, which in sorted mode is a single series.
    ///
    /// `split_off` leaves the head's capacity exactly as it was before the
    /// split: `reserve`'s room for the still-open series behind it, which
    /// an emit hands to Arrow uncounted otherwise. Only shrunk past a 2x
    /// slack, so the one head that is genuinely most of the buffer (an
    /// `EmitTo::All` with nothing left open) skips a copy of the whole
    /// thing for a percent-scale reservation remainder.
    pub(crate) fn take_first(&mut self, n: usize) -> ListArray {
        let end = self.offsets[n];
        let ts = self.ts.split_off(end as usize);
        let vs = self.vs.split_off(end as usize);
        let mut ts = std::mem::replace(&mut self.ts, ts);
        let mut vs = std::mem::replace(&mut self.vs, vs);
        if ts.capacity() > ts.len().saturating_mul(2) {
            ts.shrink_to_fit();
        }
        if vs.capacity() > vs.len().saturating_mul(2) {
            vs.shrink_to_fit();
        }
        let rest: Vec<i32> = self.offsets[n..].iter().map(|o| o - end).collect();
        let mut offsets = std::mem::replace(&mut self.offsets, rest);
        offsets.truncate(n + 1);
        ListArray::new(
            sample_item(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(StructArray::new(
                sample_fields(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(ts)),
                    Arc::new(Float64Array::from(vs)),
                ],
                None,
            )),
            None,
        )
    }

    /// Every finished row. An open row stays behind.
    pub(crate) fn take_all(&mut self) -> ListArray {
        self.take_first(self.rows())
    }

    pub(crate) fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.ts.capacity() * std::mem::size_of::<i64>()
            + self.vs.capacity() * std::mem::size_of::<f64>()
            + self.offsets.capacity() * std::mem::size_of::<i32>()
    }
}

/// One block over everything, the shape a whole-series store hands over;
/// the block every test fixture outside a block test sits in.
#[cfg(test)]
pub(crate) const ONE_BLOCK: Block = Block {
    start_ms: 0,
    end_ms: i64::MAX,
};

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
        let batch = encode(&names, &all, ONE_BLOCK).unwrap();
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

        let batch = encode(&label_names_of(&all), &all, ONE_BLOCK).unwrap();
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
        let err = encode(&label_names_of(&twice), &twice, ONE_BLOCK).unwrap_err();
        assert!(err.contains(r#"a="1""#), "{err}");

        // The empty label set is a label set too.
        let bare = [series(&[], &[(0, 1.0)]), series(&[], &[(1, 1.0)])];
        assert!(encode(&[], &bare, ONE_BLOCK).is_err());
    }

    #[test]
    fn a_label_outside_the_schema_is_rejected() {
        let err = encode(&["a".to_string()], &[series(&[("b", "x")], &[])], ONE_BLOCK).unwrap_err();
        assert!(err.contains("`b`"), "{err}");
    }

    #[test]
    fn no_labels_at_all_is_a_valid_shape() {
        let batch = encode(&[], &[series(&[], &[(1, 1.0)])], ONE_BLOCK).unwrap();
        validate(&batch.schema()).unwrap();
        assert_eq!(decode(&[batch]).unwrap().len(), 1);

        let empty = encode(&[], &[], ONE_BLOCK).unwrap();
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

    /// Both helpers keep the canonical shape, which is why they live
    /// here: the batch that comes back still validates and still decodes
    /// to the series the store meant to hand over.
    #[test]
    fn the_store_helpers_narrow_a_batch_without_leaving_the_shape() {
        let all = [
            series(
                &[("__name__", "up"), ("pod", "a")],
                &[(0, 1.0), (1000, 2.0)],
            ),
            series(&[("__name__", "up")], &[(1000, 3.0), (2000, 4.0)]),
        ];
        // `pod` is in the schema, but no row below carries it.
        let batch = encode(
            &["__name__".to_string(), "pod".to_string()],
            &all[1..],
            ONE_BLOCK,
        )
        .unwrap();

        let narrowed = drop_unused_labels(&batch).unwrap();
        validate(&narrowed.schema()).unwrap();
        assert_eq!(label_names(&narrowed.schema()), ["__name__"]);

        let clipped = clip(&narrowed, 2000, 9000).unwrap();
        validate(&clipped.schema()).unwrap();
        let decoded = decode(&[clipped]).unwrap();
        assert_eq!(decoded[0].timestamps(), [2000]);
        assert_eq!(decoded[0].values(), [4.0]);

        // Nothing to narrow and nothing to clip hands the batch back.
        assert_eq!(clip(&narrowed, 0, 9000).unwrap().num_rows(), 1);
    }

    #[test]
    fn validate_names_the_deviation() {
        let ok = schema(&["a".to_string()]);
        validate(&ok).unwrap();

        let block = |nullable: bool| {
            vec![
                Field::new(BLOCK_START, timestamp_type(), nullable),
                Field::new(BLOCK_END, timestamp_type(), nullable),
            ]
        };
        let with_block = |labels: Field, samples: Field| {
            Schema::new([vec![labels, samples], block(false)].concat())
        };

        let nullable = with_block(
            Field::new(LABELS, labels_type(&["a".to_string()]), true),
            Field::new(SAMPLES, samples_type(), false),
        );
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
        let nanos = with_block(
            Field::new(LABELS, labels_type(&[]), false),
            Field::new(SAMPLES, DataType::List(ns_item), false),
        );
        assert!(validate(&nanos).unwrap_err().contains("expected"));

        let plain = with_block(
            Field::new(
                LABELS,
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Utf8, false)])),
                false,
            ),
            Field::new(SAMPLES, samples_type(), false),
        );
        assert!(validate(&plain).unwrap_err().contains("expected Utf8View"));

        let dictionary = with_block(
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
        );
        assert!(validate(&dictionary)
            .unwrap_err()
            .contains("expected Utf8View"));

        let unsorted = with_block(
            Field::new(
                LABELS,
                DataType::Struct(Fields::from(vec![
                    Field::new("b", label_type(), false),
                    Field::new("a", label_type(), false),
                ])),
                false,
            ),
            Field::new(SAMPLES, samples_type(), false),
        );
        assert!(validate(&unsorted).unwrap_err().contains("sorted"));

        let bare = || {
            vec![
                Field::new(LABELS, labels_type(&[]), false),
                Field::new(SAMPLES, samples_type(), false),
            ]
        };
        let without_block = Schema::new(bare());
        assert!(validate(&without_block).unwrap_err().contains("exactly"));

        let extra = Schema::new(
            [
                bare(),
                block(false),
                vec![Field::new("x", DataType::Int64, false)],
            ]
            .concat(),
        );
        assert!(validate(&extra).unwrap_err().contains("exactly"));

        let nullable_block = Schema::new([bare(), block(true)].concat());
        assert!(validate(&nullable_block)
            .unwrap_err()
            .contains("must not be nullable"));

        let seconds = Schema::new(
            [
                bare(),
                vec![
                    Field::new(
                        BLOCK_START,
                        DataType::Timestamp(TimeUnit::Second, None),
                        false,
                    ),
                    Field::new(BLOCK_END, timestamp_type(), false),
                ],
            ]
            .concat(),
        );
        assert!(validate(&seconds).unwrap_err().contains("expected"));
    }

    /// The block columns travel with the rows through the store helpers
    /// and come back off the batch as they went in.
    #[test]
    fn a_block_is_stamped_on_every_row_and_read_back() {
        let all = [
            series(&[("pod", "a")], &[(0, 1.0), (1000, 2.0)]),
            series(&[("pod", "b")], &[(0, 3.0)]),
        ];
        let block = Block {
            start_ms: 500,
            end_ms: 2000,
        };
        let batch = encode(&label_names_of(&all), &all, block).unwrap();
        validate(&batch.schema()).unwrap();
        assert_eq!(block_of(&batch, 0), block);
        assert_eq!(block_of(&batch, 1), block);

        let later = Block {
            start_ms: 2000,
            end_ms: 3000,
        };
        let restamped = with_block(&clip(&batch, 1000, 1000).unwrap(), later).unwrap();
        validate(&restamped.schema()).unwrap();
        assert_eq!(block_of(&restamped, 1), later);
        assert_eq!(decode(&[restamped]).unwrap().len(), 1);
    }

    fn builder_rows(list: &ListArray) -> Vec<Vec<(i64, f64)>> {
        (0..list.len())
            .map(|r| {
                let row = list.value(r);
                let (ts, vs) = sample_slices(row.as_struct());
                ts.iter().copied().zip(vs.iter().copied()).collect()
            })
            .collect()
    }

    #[test]
    fn take_first_leaves_later_rows_and_the_open_one() {
        let mut b = SamplesBuilder::default();
        b.push(0, 1.0);
        b.finish_row();
        b.finish_row();
        b.push(10, 2.0);
        b.push(20, 3.0);
        b.finish_row();
        b.push(30, 4.0);

        let first = b.take_first(2);
        assert_eq!(first.data_type(), &samples_type());
        assert_eq!(builder_rows(&first), vec![vec![(0, 1.0)], vec![]]);

        b.finish_row();
        assert_eq!(
            builder_rows(&b.take_all()),
            vec![vec![(10, 2.0), (20, 3.0)], vec![(30, 4.0)]]
        );
        assert_eq!(b.take_all().len(), 0);
    }

    /// The head split off by `take_first` must not carry the open series'
    /// reservation: an emit that hands a small row to Arrow while a large
    /// grid is still reserved behind it must not report the grid's size.
    #[test]
    fn take_first_does_not_emit_the_open_series_reservation() {
        let mut b = SamplesBuilder::default();
        b.reserve(100_000);
        b.push(0, 1.0);
        b.finish_row();
        b.push(10, 2.0); // the open row, still being grown

        let first = b.take_first(1);
        let row = first.value(0);
        let (ts, vs) = sample_slices(row.as_struct());
        assert_eq!(ts.len(), 1);
        assert_eq!(vs.len(), 1);
        assert!(
            first.get_array_memory_size() < 4096,
            "emitted row of {} sample(s) reports {} bytes, still counting the open \
             series' reservation",
            ts.len(),
            first.get_array_memory_size()
        );
    }
}
