//! `topk`, `bottomk`, `limitk` and `limit_ratio` as a DataFusion aggregate
//! function: `promql_aggregate_k(labels, samples, '<op>', k, start, end,
//! step) … GROUP BY <label columns>`.
//!
//! Ports upstream's `aggregationK` (`promql/engine.go` at 83962c35). These
//! four select series instead of folding them, so their output keeps the
//! input's labels and a series survives at the steps where it was chosen.
//! A group therefore answers with a *list of series*, one list row per
//! group, and the planner unnests it back to rows (`plan.rs`'s
//! `aggregate_k`). Doing it as an aggregate rather than an operator of its
//! own keeps the plan DataFusion's: the groups, the partitions and the
//! block edge are the ones `sum by` already has.
//!
//! What a group holds is, per step, a heap of at most `k` `(value, series)`
//! entries. The heaps are `container/heap`'s, ported with their `Less`
//! (NaN is the smallest, even against another NaN), so that which of two
//! equal values stays is upstream's answer for the same arrival order, and
//! `topk(1, x)` over a tie keeps the first series, not the last. A series
//! is held once, behind an `Arc`, however many steps its entries are in:
//! when no heap holds it any more it is gone, so the labels a group keeps
//! are those of the series it can still return, not of every series it saw.
//!
//! `k` is a scalar expression and is its own value at every step, passed as
//! one list literal ([`crate::aggregate::params_literal`]). The checks upstream
//! makes before the loop, NaN and the `int64` overflow, are the planner's:
//! they refuse the query, which a kernel cannot.
//!
//! The partial state is the result itself: for each series still held, the
//! steps it was chosen at and its value there. Merging another partition's
//! state adds each of those pairs the way a sample is added, which is
//! correct for all four. A series in the top `k` of the union is in the top
//! `k` of its own partition, `limit_ratio` decides by the series' labels
//! alone, and `limitk` keeps the `k` smallest label sets, so which `k` it
//! returns does not depend on how the store split the series. Upstream
//! takes the first `k` the input hands it, which is the same thing once the
//! input is in label order, as a Prometheus select is.
//!
//! Ties are the one place the partitions can disagree with a single
//! Prometheus: of two equal values at the `k`th place, the one that arrived
//! first stays, and arrival order across partitions is not label order.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, ListArray, StringViewArray, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use promql_parser::token::ItemType;

use crate::aggregate::{int_arg, params_arg, params_literal, Grid};
use crate::math::nan_first;
use crate::series;
use crate::sort::{labels_key, read_string};

pub const NAME: &str = "promql_aggregate_k";

/// The aggregations this function implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Topk,
    Bottomk,
    Limitk,
    LimitRatio,
}

impl Op {
    /// Dispatch is on the variant, not the text, for the reason
    /// [`crate::aggregate::Op::from_token`] gives.
    pub fn from_token(op: ItemType) -> Option<Op> {
        Some(match op {
            ItemType::Topk => Op::Topk,
            ItemType::Bottomk => Op::Bottomk,
            ItemType::Limitk => Op::Limitk,
            ItemType::LimitRatio => Op::LimitRatio,
            _ => return None,
        })
    }

    pub fn parse(s: &str) -> Option<Op> {
        Some(match s {
            "topk" => Op::Topk,
            "bottomk" => Op::Bottomk,
            "limitk" => Op::Limitk,
            "limit_ratio" => Op::LimitRatio,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Topk => "topk",
            Op::Bottomk => "bottomk",
            Op::Limitk => "limitk",
            Op::LimitRatio => "limit_ratio",
        }
    }
}

/// What one step takes from a group, from that step's parameter.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Selection {
    /// Nothing: `k < 1`, or a ratio of zero. Upstream returns early on
    /// the first series it meets, discarding the step.
    None,
    /// At most this many series.
    K(usize),
    /// The series whose offset falls inside this ratio, in `[-1, 1]`.
    Ratio(f64),
}

impl Selection {
    fn of(op: Op, param: f64) -> Selection {
        match op {
            // `int64(fParam)`. Go's conversion of a value out of range is
            // undefined; the planner has already refused NaN and every
            // value that would overflow, so this only sees what fits and
            // saturates for a caller that did not check.
            Op::Topk | Op::Bottomk | Op::Limitk => match param as i64 {
                k if k < 1 => Selection::None,
                k => Selection::K(usize::try_from(k).unwrap_or(usize::MAX)),
            },
            Op::LimitRatio if param == 0.0 => Selection::None,
            // NaN passes through both comparisons and selects nothing;
            // the planner refuses it before it gets here.
            Op::LimitRatio => Selection::Ratio(param.clamp(-1.0, 1.0)),
        }
    }
}

/// One input series as a group holds it. The labels are copied out of the
/// batch: a view into it would keep every other series' label buffers
/// alive for as long as this one is held.
#[derive(Debug)]
struct Held {
    /// The values of the `labels` struct's fields, in field order.
    labels: Box<[Box<str>]>,
    /// The label set as `promql_labels_key` orders it, for `limitk`. Empty
    /// for the others, which never compare label sets.
    key: Box<[u8]>,
}

/// A value chosen at one step, and the series it belongs to.
#[derive(Debug)]
struct Entry {
    value: f64,
    series: Arc<Held>,
}

/// One input row on its way in: its labels in the batch, and the two
/// things the selection reads off them, computed once per series rather
/// than once per step.
struct Incoming<'a> {
    labels: &'a StructArray,
    row: usize,
    /// `limit_ratio`: where the label set falls in `[0, 1)`.
    offset: f64,
    /// `limitk`: the label set's sort key.
    key: Vec<u8>,
    held: Option<Arc<Held>>,
}

impl<'a> Incoming<'a> {
    fn new(op: Op, labels: &'a StructArray, row: usize, key: Vec<u8>) -> Result<Self> {
        let mut incoming = Self {
            labels,
            row,
            offset: 0.0,
            key,
            held: None,
        };
        incoming.key.clear();
        match op {
            Op::Limitk => labels_key(labels, row, &mut incoming.key)?,
            Op::LimitRatio => incoming.offset = sample_offset(labels, row)?,
            Op::Topk | Op::Bottomk => {}
        }
        Ok(incoming)
    }

    /// The series as a group holds it, copied out on first use: a series
    /// that no step chooses is never copied.
    fn held(&mut self) -> Result<Arc<Held>> {
        if let Some(held) = &self.held {
            return Ok(Arc::clone(held));
        }
        let mut values = Vec::with_capacity(self.labels.num_columns());
        for column in self.labels.columns() {
            values.push(Box::<str>::from(read_string(column, self.row)?));
        }
        let held = Arc::new(Held {
            labels: values.into_boxed_slice(),
            key: self.key.clone().into_boxed_slice(),
        });
        self.held = Some(Arc::clone(&held));
        Ok(held)
    }
}

/// Upstream's `HashRatioSampler.SampleOffset`: the label set's `Hash()` as
/// a fraction of `MaxUint64`.
///
/// `Labels.Hash` is the `stringlabels` one (`model/labels/
/// labels_stringlabels.go` at 83962c35): xxhash64, seed 0, of the label
/// set's own encoding, `ls.data`. That file is built under
/// `!slicelabels && !dedupelabels`, so an untagged build, which is what
/// Prometheus ships and the corpus is run against, uses it, and the
/// `name 0xFF value 0xFF` hash of `labels_slicelabels.go` (tag
/// `slicelabels`) would sample other series. Per label, in name order,
/// the encoding is the name's length, the name, the value's length and
/// the value, a length being one byte below 255 and otherwise `0xFF` and
/// the length as three bytes, little-endian (`encodeSize`). A label whose
/// value is empty is not in the set, as everywhere else in the engine.
fn sample_offset(labels: &StructArray, row: usize) -> Result<f64> {
    fn size(n: usize, out: &mut Vec<u8>) {
        if n < 255 {
            out.push(n as u8);
        } else {
            // `sizeWhenEncoded` panics past 1<<24; a label that long is
            // not one a store holds.
            out.extend_from_slice(&[0xFF, n as u8, (n >> 8) as u8, (n >> 16) as u8]);
        }
    }
    let mut bytes = Vec::new();
    for (field, column) in labels.fields().iter().zip(labels.columns()) {
        let value = read_string(column, row)?;
        if value.is_empty() {
            continue;
        }
        size(field.name().len(), &mut bytes);
        bytes.extend_from_slice(field.name().as_bytes());
        size(value.len(), &mut bytes);
        bytes.extend_from_slice(value.as_bytes());
    }
    let hash = twox_hash::XxHash64::oneshot(0, &bytes);
    Ok(hash as f64 / u64::MAX as f64)
}

/// `AddRatioSampleWithOffset`: for a ratio of `r >= 0` the series whose
/// offset is below `r`, for a negative one those at or above `1 + r`, so
/// that `r` and `r - 1` split the series into two sets that together are
/// all of them.
fn within_ratio(ratio: f64, offset: f64) -> bool {
    (ratio >= 0.0 && offset < ratio) || (ratio < 0.0 && offset >= 1.0 + ratio)
}

// The heaps below are `container/heap`'s `up`, `down`, `Push` and `Fix`
// over a slice, with the comparison passed in, because a Rust `BinaryHeap`
// would pop in an order that differs from upstream's on ties.

fn up(h: &mut [Entry], mut j: usize, less: &impl Fn(&Entry, &Entry) -> bool) {
    while j > 0 {
        let i = (j - 1) / 2;
        if !less(&h[j], &h[i]) {
            break;
        }
        h.swap(i, j);
        j = i;
    }
}

fn down(h: &mut [Entry], i0: usize, less: &impl Fn(&Entry, &Entry) -> bool) -> bool {
    let n = h.len();
    let mut i = i0;
    loop {
        let j1 = 2 * i + 1;
        if j1 >= n {
            break;
        }
        let mut j = j1;
        if j1 + 1 < n && less(&h[j1 + 1], &h[j1]) {
            j = j1 + 1;
        }
        if !less(&h[j], &h[i]) {
            break;
        }
        h.swap(i, j);
        i = j;
    }
    i > i0
}

fn push(h: &mut Vec<Entry>, e: Entry, less: &impl Fn(&Entry, &Entry) -> bool) {
    h.push(e);
    let last = h.len() - 1;
    up(h, last, less);
}

/// Replace the root and restore the invariant, `heap.Fix(h, 0)`.
fn replace_root(h: &mut [Entry], e: Entry, less: &impl Fn(&Entry, &Entry) -> bool) {
    h[0] = e;
    if !down(h, 0, less) {
        up(h, 0, less);
    }
}

/// `vectorByValueHeap.Less`: NaN is least, so a NaN is the first to go
/// from the top-`k` heap.
fn less_by_value(a: &Entry, b: &Entry) -> bool {
    a.value.is_nan() || a.value < b.value
}

/// `vectorByReverseValueHeap.Less`: the same with the numbers reversed,
/// so the root of the bottom-`k` heap is the largest value held.
fn less_by_reverse_value(a: &Entry, b: &Entry) -> bool {
    a.value.is_nan() || a.value > b.value
}

/// For `limitk`'s heap, whose root is the label set that sorts last of
/// those held and so the one to give way to a smaller.
fn greater_key(a: &Entry, b: &Entry) -> bool {
    a.series.key > b.series.key
}

/// Every group's state is one such set of heaps.
#[derive(Debug)]
pub struct SelectK {
    op: Op,
    grid: Grid,
    selection: Vec<Selection>,
    /// The labels struct's fields, for the output's.
    label_fields: Fields,
    /// One heap per step, or none for a step nothing was chosen at.
    steps: Vec<Vec<Entry>>,
    /// Entries held in all the heaps, for [`Accumulator::size`].
    held: usize,
    /// Scratch for the label key of the series being added.
    key: Vec<u8>,
}

impl SelectK {
    pub fn new(
        op: Op,
        params: &[f64],
        label_fields: Fields,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Self> {
        let grid = Grid::new(NAME, start_ms, end_ms, step_ms)?;
        if params.len() != grid.len() {
            return Err(DataFusionError::Execution(format!(
                "{NAME}: {} parameter values for a grid of {} steps",
                params.len(),
                grid.len()
            )));
        }
        Ok(Self {
            op,
            selection: params.iter().map(|p| Selection::of(op, *p)).collect(),
            grid,
            label_fields,
            steps: (0..grid.len()).map(|_| Vec::new()).collect(),
            held: 0,
            key: Vec::new(),
        })
    }

    /// `aggregationK`'s per-series step: offer `value` at `step` to the
    /// group. The arms are the `switch op` of the loop body.
    fn add(&mut self, step: usize, value: f64, row: &mut Incoming) -> Result<()> {
        let heap = &mut self.steps[step];
        match (self.op, self.selection[step]) {
            (_, Selection::None) => {}
            (Op::Topk, Selection::K(k)) => {
                if heap.len() < k {
                    let series = row.held()?;
                    push(heap, Entry { value, series }, &less_by_value);
                    self.held += 1;
                } else if heap[0].value < value || (heap[0].value.is_nan() && !value.is_nan()) {
                    let series = row.held()?;
                    // `if k > 1 { heap.Fix }`: a heap of one has nothing
                    // to restore.
                    if k > 1 {
                        replace_root(heap, Entry { value, series }, &less_by_value);
                    } else {
                        heap[0] = Entry { value, series };
                    }
                }
            }
            (Op::Bottomk, Selection::K(k)) => {
                if heap.len() < k {
                    let series = row.held()?;
                    push(heap, Entry { value, series }, &less_by_reverse_value);
                    self.held += 1;
                } else if heap[0].value > value || (heap[0].value.is_nan() && !value.is_nan()) {
                    let series = row.held()?;
                    if k > 1 {
                        replace_root(heap, Entry { value, series }, &less_by_reverse_value);
                    } else {
                        heap[0] = Entry { value, series };
                    }
                }
            }
            (Op::Limitk, Selection::K(k)) => {
                if heap.len() < k {
                    let series = row.held()?;
                    push(heap, Entry { value, series }, &greater_key);
                    self.held += 1;
                } else if row.key.as_slice() < &*heap[0].series.key {
                    let series = row.held()?;
                    replace_root(heap, Entry { value, series }, &greater_key);
                }
            }
            (Op::LimitRatio, Selection::Ratio(r)) => {
                if within_ratio(r, row.offset) {
                    let series = row.held()?;
                    // Order is unspecified for `limit_ratio`, and nothing
                    // reads the heap but the output, so a plain append.
                    heap.push(Entry { value, series });
                    self.held += 1;
                }
            }
            // `Selection::of` pairs `K` with the three and `Ratio` with
            // the fourth.
            _ => unreachable!("a selection that does not belong to {:?}", self.op),
        }
        Ok(())
    }

    /// Offer one series' samples, which must lie on the grid.
    fn add_series(
        &mut self,
        labels: &StructArray,
        row: usize,
        ts: &[i64],
        vs: &[f64],
    ) -> Result<()> {
        let key = std::mem::take(&mut self.key);
        let mut incoming = Incoming::new(self.op, labels, row, key)?;
        let mut failure = Ok(());
        let grid = self.grid;
        grid.runs(ts, |index, from, len| {
            for k in 0..len {
                if failure.is_ok() {
                    failure = self.add(index + k, vs[from + k], &mut incoming);
                }
            }
        })?;
        self.key = incoming.key;
        failure
    }

    /// The groups' series with the steps each was chosen at, as upstream
    /// emits them: steps in order, and within a step `topk`'s heap
    /// descending and `bottomk`'s ascending, a NaN last either way
    /// (`sort.Sort(sort.Reverse(heap))`). A series is placed where it is
    /// first chosen, so an instant query, which has one step, is in
    /// upstream's order. `sorted` is off for the partial state, which no
    /// one reads in order.
    fn chosen(&self, sorted: bool) -> Vec<(Arc<Held>, Vec<i64>, Vec<f64>)> {
        let mut index: HashMap<*const Held, usize> = HashMap::new();
        let mut out: Vec<(Arc<Held>, Vec<i64>, Vec<f64>)> = Vec::new();
        for (step, heap) in self.steps.iter().enumerate() {
            if heap.is_empty() {
                continue;
            }
            let ts = self.grid.timestamp(step);
            let mut order: Vec<&Entry> = heap.iter().collect();
            if sorted {
                match self.op {
                    Op::Topk => order.sort_by(|a, b| nan_first(b.value, a.value)),
                    Op::Bottomk => order.sort_by(|a, b| {
                        a.value
                            .is_nan()
                            .cmp(&b.value.is_nan())
                            .then_with(|| a.value.partial_cmp(&b.value).unwrap_or(Ordering::Equal))
                    }),
                    Op::Limitk | Op::LimitRatio => {}
                }
            }
            for e in order {
                let at = *index.entry(Arc::as_ptr(&e.series)).or_insert_with(|| {
                    out.push((Arc::clone(&e.series), Vec::new(), Vec::new()));
                    out.len() - 1
                });
                out[at].1.push(ts);
                out[at].2.push(e.value);
            }
        }
        out
    }

    /// The chosen series as one list row of `(labels, samples)`.
    fn list(&self, sorted: bool) -> ScalarValue {
        let chosen = self.chosen(sorted);
        let labels = if self.label_fields.is_empty() {
            StructArray::new_empty_fields(chosen.len(), None)
        } else {
            let columns: Vec<ArrayRef> = (0..self.label_fields.len())
                .map(|i| {
                    Arc::new(StringViewArray::from_iter_values(
                        chosen.iter().map(|(held, _, _)| &*held.labels[i]),
                    )) as ArrayRef
                })
                .collect();
            StructArray::new(self.label_fields.clone(), columns, None)
        };
        let mut offsets = vec![0i32];
        let (mut ts, mut vs) = (Vec::new(), Vec::new());
        for (_, t, v) in &chosen {
            ts.extend_from_slice(t);
            vs.extend_from_slice(v);
            offsets.push(ts.len() as i32);
        }
        let samples = ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(StructArray::new(
                series::sample_fields(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(ts)),
                    Arc::new(Float64Array::from(vs)),
                ],
                None,
            )),
            None,
        );
        let entries = StructArray::new(
            entry_fields(&self.label_fields),
            vec![Arc::new(labels), Arc::new(samples)],
            None,
        );
        let len = entries.len() as i32;
        ScalarValue::List(Arc::new(ListArray::new(
            entry_item(&self.label_fields),
            OffsetBuffer::new(vec![0, len].into()),
            Arc::new(entries),
            None,
        )))
    }
}

/// The two fields of one chosen series: its labels and its samples.
fn entry_fields(label_fields: &Fields) -> Fields {
    Fields::from(vec![
        Field::new(
            series::LABELS,
            DataType::Struct(label_fields.clone()),
            false,
        ),
        Field::new(series::SAMPLES, series::samples_type(), false),
    ])
}

fn entry_item(label_fields: &Fields) -> FieldRef {
    Arc::new(Field::new(
        series::LIST_ITEM,
        DataType::Struct(entry_fields(label_fields)),
        false,
    ))
}

/// What a group evaluates to, and its partial state: the chosen series.
fn result_type(label_fields: &Fields) -> DataType {
    DataType::List(entry_item(label_fields))
}

/// The label fields of the input's `labels` struct.
fn label_fields_of(labels: &DataType) -> Result<Fields> {
    match labels {
        DataType::Struct(fields) => Ok(fields.clone()),
        other => plan_err!("{NAME}: first argument must be a labels struct, got {other}"),
    }
}

/// The label fields of a [`result_type`].
fn label_fields_of_result(result: &DataType) -> Result<Fields> {
    let DataType::List(item) = result else {
        return plan_err!("{NAME}: {result} is not a list of chosen series");
    };
    let DataType::Struct(entry) = item.data_type() else {
        return plan_err!("{NAME}: {} is not a chosen series", item.data_type());
    };
    match entry.iter().find(|f| f.name() == series::LABELS) {
        Some(f) => label_fields_of(f.data_type()),
        None => plan_err!("{NAME}: a chosen series has no labels"),
    }
}

impl Accumulator for SelectK {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let labels = values[0].as_struct();
        let list = values[1].as_list::<i32>();
        let (ts, vs) = series::sample_slices(list.values().as_struct());
        let offsets = list.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            self.add_series(labels, row, &ts[lo..hi], &vs[lo..hi])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.list(true))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.steps.capacity() * std::mem::size_of::<Vec<Entry>>()
            + self.held * std::mem::size_of::<Entry>()
            // Each series held once at least; its labels and key are
            // what it costs beyond the `Arc`.
            + self.held * std::mem::size_of::<Held>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.list(false)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let list = states[0].as_list::<i32>();
        let entries = list.values().as_struct();
        let labels = entries
            .column_by_name(series::LABELS)
            .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: state has no labels")))?
            .as_struct_opt()
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: state labels are not a struct"))
            })?;
        let samples = entries
            .column_by_name(series::SAMPLES)
            .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: state has no samples")))?
            .as_list_opt::<i32>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: state samples are not a list"))
            })?;
        let (ts, vs) = series::sample_slices(samples.values().as_struct());
        let offsets = samples.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            for series in list.offsets()[row] as usize..list.offsets()[row + 1] as usize {
                let (lo, hi) = (offsets[series] as usize, offsets[series + 1] as usize);
                self.add_series(labels, series, &ts[lo..hi], &vs[lo..hi])?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SelectKUdaf {
    signature: Signature,
}

impl Default for SelectKUdaf {
    fn default() -> Self {
        Self {
            // The labels struct is the plan's own, whatever names it
            // carries, so only the arity is fixed here and `return_type`
            // checks the rest.
            signature: Signature::any(7, Volatility::Immutable),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(SelectKUdaf::default())
}

/// `promql_aggregate_k(labels, samples, '<op>', k, start, end, step)`,
/// `k` being the value of the parameter expression at each step.
pub fn call(
    labels: Expr,
    samples: Expr,
    op: Op,
    k: &[f64],
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Expr {
    udaf().call(vec![
        labels,
        samples,
        lit(op.as_str()),
        params_literal(k),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

impl AggregateUDFImpl for SelectKUdaf {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let fields = label_fields_of(&arg_types[0])?;
        if arg_types[1] != series::samples_type() {
            return plan_err!(
                "{NAME}: second argument must be {}, got {}",
                series::samples_type(),
                arg_types[1]
            );
        }
        Ok(result_type(&fields))
    }

    /// Every group yields a list, possibly empty; never NULL.
    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        let op = match crate::aggregate::literal_arg(&args, 2) {
            Some(ScalarValue::Utf8(Some(s))) => Op::parse(s),
            _ => None,
        }
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{NAME}: third argument must be one of topk, bottomk, limitk, limit_ratio as a string literal"
            ))
        })?;
        let (start, end, step) = (
            int_arg(&args, 4, "start", NAME)?,
            int_arg(&args, 5, "end", NAME)?,
            int_arg(&args, 6, "step", NAME)?,
        );
        let grid = Grid::new(NAME, start, end, step)?;
        let params = params_arg(&args, 3, &grid, NAME)?;
        let fields = label_fields_of_result(args.return_field.data_type())?;
        Ok(Box::new(SelectK::new(
            op, &params, fields, start, end, step,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "chosen"),
            args.return_field.data_type().clone(),
            false,
        ))])
    }

    /// What an aggregation over no rows at all yields: nothing chosen.
    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        let fields = label_fields_of_result(data_type)?;
        Ok(SelectK::new(Op::Topk, &[0.0], fields, 0, 0, 1)?.list(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringViewArray;

    fn label_fields() -> Fields {
        Fields::from(vec![Field::new("pod", series::label_type(), false)])
    }

    /// One batch of `(pod, samples)` rows.
    fn batch(rows: &[(&str, &[(i64, f64)])]) -> Vec<ArrayRef> {
        let labels = StructArray::new(
            label_fields(),
            vec![Arc::new(StringViewArray::from_iter_values(
                rows.iter().map(|r| r.0),
            ))],
            None,
        );
        let ts: Vec<i64> = rows.iter().flat_map(|r| r.1.iter().map(|s| s.0)).collect();
        let vs: Vec<f64> = rows.iter().flat_map(|r| r.1.iter().map(|s| s.1)).collect();
        let mut offsets = vec![0i32];
        for r in rows {
            offsets.push(offsets.last().unwrap() + r.1.len() as i32);
        }
        let samples = ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(StructArray::new(
                series::sample_fields(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(ts)),
                    Arc::new(Float64Array::from(vs)),
                ],
                None,
            )),
            None,
        );
        vec![Arc::new(labels), Arc::new(samples)]
    }

    fn select(op: Op, k: &[f64]) -> SelectK {
        SelectK::new(op, k, label_fields(), 0, (k.len() as i64 - 1) * 10, 10).unwrap()
    }

    /// What a group answers: each chosen series, in the order emitted,
    /// with its `(timestamp, value)` pairs.
    type Chosen = Vec<(String, Vec<(i64, f64)>)>;

    fn chosen(acc: &SelectK, sorted: bool) -> Chosen {
        acc.chosen(sorted)
            .into_iter()
            .map(|(held, ts, vs)| (held.labels[0].to_string(), ts.into_iter().zip(vs).collect()))
            .collect()
    }

    fn at(rows: &Chosen, pod: &str) -> Vec<(i64, f64)> {
        rows.iter()
            .find(|r| r.0 == pod)
            .expect("a chosen series")
            .1
            .clone()
    }

    #[test]
    fn the_heap_keeps_k_and_the_first_of_two_equal_values() {
        let mut acc = select(Op::Topk, &[2.0]);
        acc.update_batch(&batch(&[
            ("a", &[(0, 5.0)]),
            ("b", &[(0, 7.0)]),
            ("c", &[(0, 5.0)]),
            ("d", &[(0, 7.0)]),
            ("e", &[(0, 6.0)]),
        ]))
        .unwrap();
        // `c` ties `a` and does not displace it, upstream's strict `<`, so
        // `a` is what `d` then evicts. `b` and `d` tie, and come out in
        // the heap's order: the root `d` first, as upstream's stable-for-
        // short-slices sort leaves them.
        let rows = chosen(&acc, true);
        let pods: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
        assert_eq!(pods, ["d", "b"]);
        assert_eq!(acc.held, 2);
    }

    #[test]
    fn bottomk_is_the_reversed_heap() {
        let mut acc = select(Op::Bottomk, &[3.0]);
        acc.update_batch(&batch(&[
            ("a", &[(0, 5.0)]),
            ("b", &[(0, f64::NAN)]),
            ("c", &[(0, 1.0)]),
            ("d", &[(0, 9.0)]),
            ("e", &[(0, 2.0)]),
        ]))
        .unwrap();
        // The NaN was held first and is the first thing a number evicts.
        let rows = chosen(&acc, true);
        let pods: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
        assert_eq!(pods, ["c", "e", "a"]);
    }

    /// A series no step still holds is released with its labels.
    #[test]
    fn a_displaced_series_is_not_kept() {
        let mut acc = select(Op::Topk, &[1.0]);
        acc.update_batch(&batch(&[("a", &[(0, 1.0)]), ("b", &[(0, 2.0)])]))
            .unwrap();
        assert_eq!(chosen(&acc, true).len(), 1);
        assert_eq!(Arc::strong_count(&acc.steps[0][0].series), 1);
    }

    #[test]
    fn a_step_with_no_selection_takes_nothing_and_the_others_still_do() {
        let mut acc = select(Op::Topk, &[0.0, 1.0]);
        acc.update_batch(&batch(&[
            ("a", &[(0, 1.0), (10, 1.0)]),
            ("b", &[(0, 2.0), (10, 0.5)]),
        ]))
        .unwrap();
        let rows = chosen(&acc, true);
        assert_eq!(at(&rows, "a"), [(10, 1.0)]);
        assert!(!rows.iter().any(|r| r.0 == "b"));
    }

    #[test]
    fn limitk_keeps_the_smallest_label_sets_whatever_the_arrival_order() {
        let mut acc = select(Op::Limitk, &[2.0]);
        acc.update_batch(&batch(&[
            ("d", &[(0, 1.0)]),
            ("b", &[(0, 1.0)]),
            ("c", &[(0, 1.0)]),
            ("a", &[(0, 1.0)]),
        ]))
        .unwrap();
        let mut pods: Vec<String> = chosen(&acc, true).into_iter().map(|r| r.0).collect();
        pods.sort();
        assert_eq!(pods, ["a", "b"]);
    }

    #[test]
    fn limit_ratio_decides_by_the_labels_alone() {
        let mut all = select(Op::LimitRatio, &[1.0]);
        let mut none = select(Op::LimitRatio, &[0.0]);
        let mut half = select(Op::LimitRatio, &[0.5]);
        let mut rest = select(Op::LimitRatio, &[-0.5]);
        let rows: Vec<(String, Vec<(i64, f64)>)> = (0..40)
            .map(|i| (format!("p{i}"), vec![(0, i as f64)]))
            .collect();
        let input: Vec<(&str, &[(i64, f64)])> = rows
            .iter()
            .map(|(p, s)| (p.as_str(), s.as_slice()))
            .collect();
        for acc in [&mut all, &mut none, &mut half, &mut rest] {
            acc.update_batch(&batch(&input)).unwrap();
        }
        assert_eq!(chosen(&all, true).len(), 40);
        assert_eq!(chosen(&none, true).len(), 0);
        let (h, r) = (chosen(&half, true).len(), chosen(&rest, true).len());
        assert_eq!(h + r, 40);
        assert!(h > 0 && r > 0, "40 hashes fall on both sides: {h} {r}");
    }

    /// `Labels.Hash` of no labels is `xxhash.Sum64(nil)`, XXH64 of the
    /// empty string at seed 0, in either build.
    #[test]
    fn the_sampling_offset_is_the_xxhash64_of_the_label_set() {
        let empty = StructArray::new_empty_fields(1, None);
        assert_eq!(
            sample_offset(&empty, 0).unwrap(),
            0xEF46_DB37_51D8_E999_u64 as f64 / u64::MAX as f64
        );
        // A label with no value is not in the set.
        let blank = StructArray::new(
            label_fields(),
            vec![Arc::new(StringViewArray::from_iter_values([""]))],
            None,
        );
        assert_eq!(
            sample_offset(&blank, 0).unwrap(),
            sample_offset(&empty, 0).unwrap()
        );
    }

    /// The label set as `stringlabels` stores it, and its XXH64, worked
    /// outside this crate from `encodeSize` and `marshalLabelToSizedBuffer`
    /// with an independent XXH64 (checked against the reference vectors
    /// for "", "a" and "abc").
    #[test]
    fn the_sampling_offset_hashes_the_stringlabels_encoding() {
        let two = StructArray::new(
            Fields::from(vec![
                Field::new("job", series::label_type(), false),
                Field::new("pod", series::label_type(), false),
            ]),
            vec![
                Arc::new(StringViewArray::from_iter_values(["api"])),
                Arc::new(StringViewArray::from_iter_values(["a"])),
            ],
            None,
        );
        // 03 'job' 03 'api' 03 'pod' 01 'a'
        let bytes = b"\x03job\x03api\x03pod\x01a";
        assert_eq!(
            twox_hash::XxHash64::oneshot(0, bytes),
            0x37C9_2B91_FDAC_ADAE
        );
        assert_eq!(
            sample_offset(&two, 0).unwrap(),
            0x37C9_2B91_FDAC_ADAE_u64 as f64 / u64::MAX as f64
        );
    }

    /// A length of 255 or more is `0xFF` and three little-endian bytes,
    /// so 254 is the last one-byte length.
    #[test]
    fn a_long_label_value_is_sized_with_four_bytes() {
        let offset = |len: usize, hash: u64| {
            let big = StructArray::new(
                Fields::from(vec![Field::new("big", series::label_type(), false)]),
                vec![Arc::new(StringViewArray::from_iter_values([
                    "x".repeat(len)
                ]))],
                None,
            );
            assert_eq!(
                sample_offset(&big, 0).unwrap(),
                hash as f64 / u64::MAX as f64,
                "{len}"
            );
        };
        // 03 'big' FE 'x'*254
        offset(254, 0xA1AC_FBEF_D2F8_71F3);
        // 03 'big' FF FF 00 00 'x'*255
        offset(255, 0xA600_F638_ABCD_B0F5);
        // 03 'big' FF 2C 01 00 'x'*300
        offset(300, 0xEE47_A4DE_FE81_1E01);
    }

    #[test]
    fn a_ratio_and_its_negative_complement_split_the_unit_interval() {
        for offset in [0.0, 0.2999, 0.3001, 0.7, 0.9999] {
            assert_ne!(within_ratio(0.3, offset), within_ratio(0.3 - 1.0, offset));
        }
        assert!(within_ratio(1.0, 0.9999));
        assert!(within_ratio(-1.0, 0.0));
        assert!(!within_ratio(0.0, 0.0));
    }

    /// The partial state is the result: merging the states of two
    /// partitions is the answer over both, whichever came first.
    #[test]
    fn merging_partial_states_is_the_selection_over_everything() {
        let rows: [(&str, &[(i64, f64)]); 4] = [
            ("a", &[(0, 1.0), (10, 9.0)]),
            ("b", &[(0, 4.0), (10, 2.0)]),
            ("c", &[(0, 3.0), (10, 8.0)]),
            ("d", &[(0, 2.0), (10, 7.0)]),
        ];
        for op in [Op::Topk, Op::Bottomk, Op::Limitk, Op::LimitRatio] {
            let k = [2.0, 2.0];
            let mut whole = select(op, &k);
            whole.update_batch(&batch(&rows)).unwrap();

            let (mut left, mut right) = (select(op, &k), select(op, &k));
            left.update_batch(&batch(&rows[..2])).unwrap();
            right.update_batch(&batch(&rows[2..])).unwrap();
            let mut merged = select(op, &k);
            for part in [&mut right, &mut left] {
                let ScalarValue::List(state) = part.state().unwrap().remove(0) else {
                    panic!("a list")
                };
                assert_eq!(state.data_type(), &result_type(&label_fields()));
                merged.merge_batch(&[state as ArrayRef]).unwrap();
            }
            let canon = |acc: &SelectK| {
                let mut rows = chosen(acc, true);
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                rows
            };
            assert_eq!(canon(&merged), canon(&whole), "{op:?}");
        }
    }

    #[test]
    fn the_result_is_a_list_of_labels_and_samples() {
        let mut acc = select(Op::Topk, &[1.0, 1.0]);
        acc.update_batch(&batch(&[
            ("a", &[(0, 1.0), (10, 5.0)]),
            ("b", &[(0, 2.0), (10, 3.0)]),
        ]))
        .unwrap();
        let ScalarValue::List(list) = acc.evaluate().unwrap() else {
            panic!("a list")
        };
        assert_eq!(list.data_type(), &result_type(&label_fields()));
        let entries = list.value(0);
        let entries = entries.as_struct();
        assert_eq!(entries.len(), 2);
        let pods = entries.column(0).as_struct().column(0).as_string_view();
        assert_eq!((pods.value(0), pods.value(1)), ("b", "a"));
    }

    #[test]
    fn a_sample_off_the_grid_is_an_error() {
        let mut acc = select(Op::Topk, &[1.0]);
        assert!(acc.update_batch(&batch(&[("a", &[(3, 1.0)])])).is_err());
    }

    #[test]
    fn selection_follows_upstreams_clamps() {
        assert_eq!(Selection::of(Op::Topk, 0.9), Selection::None);
        assert_eq!(Selection::of(Op::Topk, 2.9), Selection::K(2));
        assert_eq!(Selection::of(Op::Bottomk, -1.0), Selection::None);
        assert_eq!(
            Selection::of(Op::Limitk, 9999999999.0),
            Selection::K(9_999_999_999)
        );
        assert_eq!(Selection::of(Op::LimitRatio, 0.0), Selection::None);
        assert_eq!(Selection::of(Op::LimitRatio, 7.0), Selection::Ratio(1.0));
        assert_eq!(Selection::of(Op::LimitRatio, -7.0), Selection::Ratio(-1.0));
    }
}
