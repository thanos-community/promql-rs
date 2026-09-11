//! PromQL aggregation as a DataFusion aggregate function:
//! `promql_aggregate(samples, 'sum') … GROUP BY <label columns>`.
//!
//! An aggregation reduces the series of a group to one series, step by
//! step: at each step, every series that has a point contributes it. On
//! the row-per-series shape that is an aggregate function over the
//! `samples` list, with the grouping labels as ordinary `GROUP BY`
//! columns. DataFusion supplies the hash grouping, the two-phase
//! partial/final split across partitions and the repartitioning; this
//! module supplies the per-step arithmetic, which is the part Prometheus
//! is particular about (see [`crate::math`]).
//!
//! Every input series is already on the query's step grid — it came
//! through `promql_vector_selector` or a range function, both of which
//! emit at `start + i * step` — so a timestamp *is* an array index, and
//! the group's state is a flat array with one slot per step rather than
//! a map. That is what lets this work on slices: Prometheus sees one
//! sample at a time through an iterator and has no choice but to fold
//! per sample, whereas here a whole series arrives as a contiguous
//! `&[f64]` along time, and folding it into the group is one pass over
//! two slices with an independent accumulator per step. No loop-carried
//! dependency, so the compiler can vectorize it; see [`crate::math`].
//!
//! A step no series contributed to is absent from the output, which is
//! what Prometheus does too, so the array is paired with a `seen` bitmap
//! rather than relying on a sentinel value.
//!
//! The operator is a literal argument rather than eight registered
//! functions: one name to register, one plan to serialize, and the
//! same reasoning as the parameters of `promql_vector_selector`.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, ListArray, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float64Type, TimestampMillisecondType,
};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

use crate::math::{self, max_nan_loses, min_nan_loses, KahanSum, Mean, Welford};
use crate::series;

pub const NAME: &str = "promql_aggregate";

/// The aggregation operators this function implements. The rest of
/// PromQL's (`topk`, `quantile`, `count_values`, …) produce per-series or
/// per-value output and get their own treatment later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    Group,
    Stddev,
    Stdvar,
}

impl Op {
    pub fn parse(s: &str) -> Option<Op> {
        Some(match s {
            "sum" => Op::Sum,
            "avg" => Op::Avg,
            "count" => Op::Count,
            "min" => Op::Min,
            "max" => Op::Max,
            "group" => Op::Group,
            "stddev" => Op::Stddev,
            "stdvar" => Op::Stdvar,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Sum => "sum",
            Op::Avg => "avg",
            Op::Count => "count",
            Op::Min => "min",
            Op::Max => "max",
            Op::Group => "group",
            Op::Stddev => "stddev",
            Op::Stdvar => "stdvar",
        }
    }
}

/// One group's running value at one step. The arms mirror
/// `groupedAggregation` in upstream's `engine.go`; each is the port of
/// that struct's fields the operator actually uses.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    Sum(KahanSum),
    Avg(Mean),
    Count(f64),
    Min(f64),
    Max(f64),
    Group,
    Var(Welford),
}

impl State {
    /// A fresh state for `op`, with nothing seen yet.
    pub fn new(op: Op) -> State {
        match op {
            Op::Sum => State::Sum(KahanSum::default()),
            Op::Avg => State::Avg(Mean::default()),
            Op::Count => State::Count(0.0),
            Op::Min => State::Min(f64::NAN),
            Op::Max => State::Max(f64::NAN),
            Op::Group => State::Group,
            Op::Stddev | Op::Stdvar => State::Var(Welford::default()),
        }
    }

    /// One more series' value at this step.
    pub fn add(&mut self, f: f64) {
        match self {
            State::Sum(k) => k.add(f),
            State::Avg(m) => m.add(f),
            State::Count(n) => *n += 1.0,
            State::Min(v) => *v = min_nan_loses(*v, f),
            State::Max(v) => *v = max_nan_loses(*v, f),
            State::Group => {}
            State::Var(w) => w.add(f),
        }
    }

    /// Another partial state for the same step, from another partition.
    pub fn merge(&mut self, other: &State) {
        match (self, other) {
            (State::Sum(a), State::Sum(b)) => a.merge(b),
            (State::Avg(a), State::Avg(b)) => a.merge(b),
            (State::Count(a), State::Count(b)) => *a += b,
            (State::Min(a), State::Min(b)) => *a = min_nan_loses(*a, *b),
            (State::Max(a), State::Max(b)) => *a = max_nan_loses(*a, *b),
            (State::Group, State::Group) => {}
            (State::Var(a), State::Var(b)) => a.merge(b),
            _ => unreachable!("one accumulator, one operator"),
        }
    }

    /// The aggregated value at this step.
    pub fn result(&self, op: Op) -> f64 {
        match (self, op) {
            (State::Sum(k), _) => k.value(),
            (State::Avg(m), _) => m.result(),
            (State::Count(n), _) => *n,
            (State::Min(v), _) | (State::Max(v), _) => *v,
            (State::Group, _) => 1.0,
            (State::Var(w), Op::Stddev) => w.variance().sqrt(),
            (State::Var(w), _) => w.variance(),
        }
    }

    /// The four numbers that serialize any arm, for the partial state.
    fn to_row(self) -> (f64, f64, f64, bool) {
        match self {
            State::Sum(k) => (k.sum, k.c, 0.0, false),
            State::Avg(m) => (m.value, m.c, m.count, m.incremental),
            State::Count(n) => (0.0, 0.0, n, false),
            State::Min(v) | State::Max(v) => (v, 0.0, 0.0, false),
            State::Group => (0.0, 0.0, 0.0, false),
            State::Var(w) => (w.mean, w.m2, w.count, false),
        }
    }

    fn from_row(op: Op, a: f64, b: f64, n: f64, m: bool) -> State {
        match op {
            Op::Sum => State::Sum(KahanSum::new(a, b)),
            Op::Avg => State::Avg(Mean {
                value: a,
                c: b,
                count: n,
                incremental: m,
            }),
            Op::Count => State::Count(n),
            Op::Min => State::Min(a),
            Op::Max => State::Max(a),
            Op::Group => State::Group,
            Op::Stddev | Op::Stdvar => State::Var(Welford {
                mean: a,
                m2: b,
                count: n,
            }),
        }
    }
}

/// The query's step grid, which is what makes a timestamp an index.
///
/// Everything an aggregation can sit on top of — a selector, a range
/// function, another aggregation — emits at `start + i * step` and
/// nowhere else, so a step is an array position and the accumulator can
/// be a flat array rather than a map.
#[derive(Debug, Clone, Copy)]
struct Grid {
    start_ms: i64,
    step_ms: i64,
    len: usize,
}

impl Grid {
    fn new(start_ms: i64, end_ms: i64, step_ms: i64) -> Result<Self> {
        if step_ms <= 0 {
            return Err(DataFusionError::Execution(format!(
                "{NAME}: step must be positive, got {step_ms}ms"
            )));
        }
        let len = if end_ms < start_ms {
            0
        } else {
            usize::try_from((end_ms - start_ms) / step_ms + 1).map_err(|_| {
                DataFusionError::Execution(format!(
                    "{NAME}: {start_ms}..{end_ms} has too many steps"
                ))
            })?
        };
        Ok(Self {
            start_ms,
            step_ms,
            len,
        })
    }

    fn timestamp(&self, index: usize) -> i64 {
        self.start_ms + index as i64 * self.step_ms
    }

    fn index(&self, ts: i64) -> Result<usize> {
        let offset = ts - self.start_ms;
        let index = offset / self.step_ms;
        if offset < 0 || offset % self.step_ms != 0 || index as usize >= self.len {
            return Err(DataFusionError::Execution(format!(
                "{NAME}: sample at {ts}ms is not on the step grid {}..{} every {}ms",
                self.start_ms,
                self.timestamp(self.len.saturating_sub(1)),
                self.step_ms
            )));
        }
        Ok(index as usize)
    }
}

/// One group's running state, held as parallel `f64` arrays rather than
/// an array of [`State`]s.
///
/// This is the shape that lets a whole series be folded in with slice
/// arithmetic: adding series `s` to the group means running the op's
/// kernel over `lanes[i0..i0+n]` and `s.values[..n]` in lockstep. Only
/// the lanes the operator actually uses are allocated.
#[derive(Debug)]
enum Lanes {
    Sum {
        sum: Vec<f64>,
        c: Vec<f64>,
    },
    Avg {
        value: Vec<f64>,
        c: Vec<f64>,
        count: Vec<f64>,
        incremental: Vec<bool>,
    },
    Count {
        n: Vec<f64>,
    },
    Min(Vec<f64>),
    Max(Vec<f64>),
    Group,
    Var {
        mean: Vec<f64>,
        m2: Vec<f64>,
        count: Vec<f64>,
    },
}

impl Lanes {
    /// Lanes for `len` steps, every position holding [`State::new`].
    fn new(op: Op, len: usize) -> Self {
        let zeros = || vec![0.0; len];
        match op {
            Op::Sum => Lanes::Sum {
                sum: zeros(),
                c: zeros(),
            },
            Op::Avg => Lanes::Avg {
                value: zeros(),
                c: zeros(),
                count: zeros(),
                incremental: vec![false; len],
            },
            Op::Count => Lanes::Count { n: zeros() },
            Op::Min => Lanes::Min(vec![f64::NAN; len]),
            Op::Max => Lanes::Max(vec![f64::NAN; len]),
            Op::Group => Lanes::Group,
            Op::Stddev | Op::Stdvar => Lanes::Var {
                mean: zeros(),
                m2: zeros(),
                count: zeros(),
            },
        }
    }

    /// Fold one series' values into the steps starting at `index`. The
    /// values are consecutive steps, so this is the slice work: one
    /// pass, one accumulator per position, no interaction between them.
    fn add_run(&mut self, index: usize, values: &[f64]) {
        let r = index..index + values.len();
        match self {
            Lanes::Sum { sum, c } => math::kahan_add_each(&mut sum[r.clone()], &mut c[r], values),
            Lanes::Avg {
                value,
                c,
                count,
                incremental,
            } => math::mean_add_each(
                &mut value[r.clone()],
                &mut c[r.clone()],
                &mut count[r.clone()],
                &mut incremental[r],
                values,
            ),
            Lanes::Count { n } => math::count_add_each(&mut n[r]),
            Lanes::Min(cur) => math::min_add_each(&mut cur[r], values),
            Lanes::Max(cur) => math::max_add_each(&mut cur[r], values),
            Lanes::Group => {}
            Lanes::Var { mean, m2, count } => math::welford_add_each(
                &mut mean[r.clone()],
                &mut m2[r.clone()],
                &mut count[r],
                values,
            ),
        }
    }

    /// One step's state as a value, for merging and for the result.
    fn get(&self, i: usize) -> State {
        match self {
            Lanes::Sum { sum, c } => State::Sum(KahanSum::new(sum[i], c[i])),
            Lanes::Avg {
                value,
                c,
                count,
                incremental,
            } => State::Avg(Mean {
                value: value[i],
                c: c[i],
                count: count[i],
                incremental: incremental[i],
            }),
            Lanes::Count { n } => State::Count(n[i]),
            Lanes::Min(cur) => State::Min(cur[i]),
            Lanes::Max(cur) => State::Max(cur[i]),
            Lanes::Group => State::Group,
            Lanes::Var { mean, m2, count } => State::Var(Welford {
                mean: mean[i],
                m2: m2[i],
                count: count[i],
            }),
        }
    }

    fn set(&mut self, i: usize, s: State) {
        match (self, s) {
            (Lanes::Sum { sum, c }, State::Sum(k)) => {
                sum[i] = k.sum;
                c[i] = k.c;
            }
            (
                Lanes::Avg {
                    value,
                    c,
                    count,
                    incremental,
                },
                State::Avg(m),
            ) => {
                value[i] = m.value;
                c[i] = m.c;
                count[i] = m.count;
                incremental[i] = m.incremental;
            }
            (Lanes::Count { n }, State::Count(v)) => n[i] = v,
            (Lanes::Min(cur), State::Min(v)) | (Lanes::Max(cur), State::Max(v)) => cur[i] = v,
            (Lanes::Group, State::Group) => {}
            (Lanes::Var { mean, m2, count }, State::Var(w)) => {
                mean[i] = w.mean;
                m2[i] = w.m2;
                count[i] = w.count;
            }
            _ => unreachable!("one accumulator, one operator"),
        }
    }

    /// Bytes of lane storage, for DataFusion's memory accounting.
    fn size(&self) -> usize {
        let f = |v: &Vec<f64>| v.capacity() * std::mem::size_of::<f64>();
        match self {
            Lanes::Sum { sum, c } => f(sum) + f(c),
            Lanes::Avg {
                value,
                c,
                count,
                incremental,
            } => f(value) + f(c) + f(count) + incremental.capacity(),
            Lanes::Count { n } => f(n),
            Lanes::Min(cur) | Lanes::Max(cur) => f(cur),
            Lanes::Group => 0,
            Lanes::Var { mean, m2, count } => f(mean) + f(m2) + f(count),
        }
    }
}

/// The accumulator for one group.
#[derive(Debug)]
pub struct Steps {
    op: Op,
    grid: Grid,
    /// Whether any series contributed at each step. A step nobody
    /// reached is absent from the result, which is not the same as a
    /// step that summed to zero.
    seen: Vec<bool>,
    lanes: Lanes,
}

impl Steps {
    pub fn new(op: Op, start_ms: i64, end_ms: i64, step_ms: i64) -> Result<Self> {
        let grid = Grid::new(start_ms, end_ms, step_ms)?;
        Ok(Self {
            op,
            seen: vec![false; grid.len],
            lanes: Lanes::new(op, grid.len),
            grid,
        })
    }

    /// Fold one whole series in. Its timestamps are ascending and on the
    /// grid, so they form one contiguous run of steps unless the series
    /// has gaps; each maximal run is one slice operation.
    pub fn add_series(&mut self, timestamps: &[i64], values: &[f64]) -> Result<()> {
        debug_assert_eq!(timestamps.len(), values.len());
        if timestamps.is_empty() {
            return Ok(());
        }
        let grid = self.grid;
        let first = grid.index(timestamps[0])?;
        let last = grid.index(timestamps[timestamps.len() - 1])?;

        // The common case: no gaps, so the whole series is one run.
        if last - first + 1 == timestamps.len() {
            self.add_run(first, values);
            return Ok(());
        }

        let mut run_start = 0;
        let mut run_index = first;
        let mut previous = first;
        for k in 1..timestamps.len() {
            let index = grid.index(timestamps[k])?;
            if index != previous + 1 {
                self.add_run(run_index, &values[run_start..k]);
                run_start = k;
                run_index = index;
            }
            previous = index;
        }
        self.add_run(run_index, &values[run_start..]);
        Ok(())
    }

    fn add_run(&mut self, index: usize, values: &[f64]) {
        self.seen[index..index + values.len()].fill(true);
        self.lanes.add_run(index, values);
    }

    fn merge_at(&mut self, index: usize, incoming: &State) {
        if self.seen[index] {
            let mut current = self.lanes.get(index);
            current.merge(incoming);
            self.lanes.set(index, current);
        } else {
            self.lanes.set(index, *incoming);
            self.seen[index] = true;
        }
    }

    /// The steps that were reached, in order.
    fn occupied(&self) -> impl Iterator<Item = usize> + '_ {
        self.seen
            .iter()
            .enumerate()
            .filter_map(|(i, seen)| seen.then_some(i))
    }

    /// The group's series, in step order.
    pub fn samples(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.occupied()
            .map(move |i| (self.grid.timestamp(i), self.lanes.get(i).result(self.op)))
    }
}

/// Fields of the partial state's list element.
fn state_fields() -> Fields {
    Fields::from(vec![
        Field::new(series::TIMESTAMP, series::timestamp_type(), false),
        Field::new("a", DataType::Float64, false),
        Field::new("b", DataType::Float64, false),
        Field::new("n", DataType::Float64, false),
        Field::new("m", DataType::Boolean, false),
    ])
}

fn state_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        series::LIST_ITEM,
        DataType::Struct(state_fields()),
        false,
    )))
}

/// A one-row list holding `entries`.
fn single_row_list(item: FieldRef, entries: StructArray) -> ScalarValue {
    let n = entries.len() as i32;
    ScalarValue::List(Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(vec![0, n].into()),
        Arc::new(entries),
        None,
    )))
}

fn empty_samples() -> ScalarValue {
    single_row_list(
        series::sample_item(),
        StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new())),
                Arc::new(Float64Array::from(Vec::<f64>::new())),
            ],
            None,
        ),
    )
}

impl Accumulator for Steps {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let list = values[0].as_list::<i32>();
        let entries = list.values().as_struct();
        let ts = entries
            .column_by_name(series::TIMESTAMP)
            .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: no timestamp column")))?
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let vs = entries
            .column_by_name(series::VALUE)
            .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: no value column")))?
            .as_primitive::<Float64Type>()
            .values();
        let offsets = list.offsets();
        // One row is one whole series, so each iteration hands a
        // contiguous slice of timestamps and values to the kernels.
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
            self.add_series(&ts[a..b], &vs[a..b])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let (ts, vs): (Vec<i64>, Vec<f64>) = self.samples().unzip();
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
            ],
            None,
        );
        Ok(single_row_list(series::sample_item(), entries))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.seen.capacity() + self.lanes.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let n = self.seen.iter().filter(|s| **s).count();
        let mut ts = Vec::with_capacity(n);
        let (mut a, mut b, mut c) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let mut m = Vec::with_capacity(n);
        for i in 0..self.seen.len() {
            if !self.seen[i] {
                continue;
            }
            let (ra, rb, rn, rm) = self.lanes.get(i).to_row();
            ts.push(self.grid.timestamp(i));
            a.push(ra);
            b.push(rb);
            c.push(rn);
            m.push(rm);
        }
        let entries = StructArray::new(
            state_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(a)),
                Arc::new(Float64Array::from(b)),
                Arc::new(Float64Array::from(c)),
                Arc::new(BooleanArray::from(m)),
            ],
            None,
        );
        let item = match state_type() {
            DataType::List(item) => item,
            _ => unreachable!(),
        };
        Ok(vec![single_row_list(item, entries)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let list = states[0].as_list::<i32>();
        let entries = list.values().as_struct();
        let ts = entries
            .column(0)
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let a = entries.column(1).as_primitive::<Float64Type>().values();
        let b = entries.column(2).as_primitive::<Float64Type>().values();
        let n = entries.column(3).as_primitive::<Float64Type>().values();
        let m = entries.column(4).as_boolean();
        let offsets = list.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            for i in offsets[row] as usize..offsets[row + 1] as usize {
                let s = State::from_row(self.op, a[i], b[i], n[i], m.value(i));
                let index = self.grid.index(ts[i])?;
                self.merge_at(index, &s);
            }
        }
        Ok(())
    }
}

/// The DataFusion function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Aggregate {
    signature: Signature,
}

impl Default for Aggregate {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    series::samples_type(),
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Aggregate::default())
}

/// `promql_aggregate(samples, '<op>', start, end, step)`.
///
/// The step grid is passed in because it is what turns a timestamp into
/// an array index; see [`Grid`].
pub fn call(samples: Expr, op: Op, start_ms: i64, end_ms: i64, step_ms: i64) -> Expr {
    udaf().call(vec![
        samples,
        lit(op.as_str()),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

impl AggregateUDFImpl for Aggregate {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.first() != Some(&series::samples_type()) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::samples_type(),
                arg_types.first()
            );
        }
        Ok(series::samples_type())
    }

    /// Every group yields a list, possibly empty; never NULL.
    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let literal = |i: usize| {
            args.exprs
                .get(i)
                .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
                .map(Literal::value)
        };
        let op = literal(1)
            .and_then(|v| match v {
                ScalarValue::Utf8(Some(s)) => Op::parse(s.as_str()),
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "{NAME}: second argument must be one of sum, avg, count, min, max, group, stddev, stdvar as a string literal"
                ))
            })?;
        let grid = |i: usize, what: &str| {
            literal(i)
                .and_then(|v| match v {
                    ScalarValue::Int64(Some(n)) => Some(*n),
                    _ => None,
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("{NAME}: {what} must be an Int64 literal"))
                })
        };
        Ok(Box::new(Steps::new(
            op,
            grid(2, "start")?,
            grid(3, "end")?,
            grid(4, "step")?,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "steps"),
            state_type(),
            false,
        ))])
    }

    /// What an aggregation over no rows at all yields: no samples.
    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(empty_samples())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fold `series` in through the slice path, on a grid wide enough
    /// for every timestamp used.
    fn accumulate(op: Op, series: &[&[(i64, f64)]], start: i64, end: i64, step: i64) -> Steps {
        let mut acc = Steps::new(op, start, end, step).unwrap();
        for s in series {
            let (ts, vs): (Vec<i64>, Vec<f64>) = s.iter().copied().unzip();
            acc.add_series(&ts, &vs).unwrap();
        }
        acc
    }

    fn run(op: Op, series: &[&[(i64, f64)]]) -> Vec<(i64, f64)> {
        accumulate(op, series, 0, 3, 1).samples().collect()
    }

    #[test]
    fn each_step_is_aggregated_over_the_series_present_at_it() {
        let a: &[(i64, f64)] = &[(0, 1.0), (1, 2.0), (2, 3.0)];
        let b: &[(i64, f64)] = &[(1, 10.0), (2, 20.0), (3, 30.0)];
        assert_eq!(
            run(Op::Sum, &[a, b]),
            vec![(0, 1.0), (1, 12.0), (2, 23.0), (3, 30.0)]
        );
        assert_eq!(
            run(Op::Count, &[a, b]),
            vec![(0, 1.0), (1, 2.0), (2, 2.0), (3, 1.0)]
        );
        assert_eq!(
            run(Op::Avg, &[a, b]),
            vec![(0, 1.0), (1, 6.0), (2, 11.5), (3, 30.0)]
        );
        assert_eq!(run(Op::Min, &[a, b])[1], (1, 2.0));
        assert_eq!(run(Op::Max, &[a, b])[1], (1, 10.0));
        assert_eq!(run(Op::Group, &[a, b])[3], (3, 1.0));
    }

    #[test]
    fn stddev_and_stdvar_are_population_statistics() {
        let series: Vec<&[(i64, f64)]> = vec![
            &[(0, 2.0)],
            &[(0, 4.0)],
            &[(0, 4.0)],
            &[(0, 4.0)],
            &[(0, 5.0)],
            &[(0, 5.0)],
            &[(0, 7.0)],
            &[(0, 9.0)],
        ];
        assert!((run(Op::Stdvar, &series)[0].1 - 4.0).abs() < 1e-12);
        assert!((run(Op::Stddev, &series)[0].1 - 2.0).abs() < 1e-12);
    }

    /// The lane kernels and the one-value-at-a-time [`State`] must agree
    /// bit for bit. [`State`] is the readable definition of each
    /// operator's arithmetic; the lanes are the fast restatement of it,
    /// and this is what pins them together.
    #[test]
    fn the_slice_path_matches_the_sample_at_a_time_path_exactly() {
        let values: Vec<Vec<f64>> = vec![
            vec![1.0, 2.0, 3.0, 4.0],
            vec![1e16, 1.0, -1e16, 0.5],
            vec![f64::NAN, 7.0, -0.0, 1e308],
            vec![-3.5, f64::INFINITY, 2.0, 1e-320],
            vec![0.1, 0.2, 0.3, 0.4],
        ];
        for op in [
            Op::Sum,
            Op::Avg,
            Op::Count,
            Op::Min,
            Op::Max,
            Op::Group,
            Op::Stddev,
            Op::Stdvar,
        ] {
            let series: Vec<Vec<(i64, f64)>> = values
                .iter()
                .map(|vs| {
                    vs.iter()
                        .copied()
                        .enumerate()
                        .map(|(i, v)| (i as i64, v))
                        .collect()
                })
                .collect();
            let refs: Vec<&[(i64, f64)]> = series.iter().map(|s| s.as_slice()).collect();
            let lanes: Vec<(i64, f64)> = accumulate(op, &refs, 0, 3, 1).samples().collect();

            // The same numbers, one at a time, through `State`.
            let mut scalar: Vec<State> = (0..4).map(|_| State::new(op)).collect();
            for vs in &values {
                for (i, v) in vs.iter().enumerate() {
                    scalar[i].add(*v);
                }
            }
            for (i, s) in scalar.iter().enumerate() {
                assert_eq!(
                    lanes[i].1.to_bits(),
                    s.result(op).to_bits(),
                    "{op:?} step {i}: {} vs {}",
                    lanes[i].1,
                    s.result(op)
                );
            }
        }
    }

    #[test]
    fn a_gap_in_a_series_splits_it_into_runs() {
        // Steps 0 and 2 present, 1 missing: two runs, and step 1 stays
        // unreached rather than being counted as a zero contribution.
        let a: &[(i64, f64)] = &[(0, 1.0), (2, 3.0)];
        let b: &[(i64, f64)] = &[(0, 10.0), (1, 20.0), (2, 30.0)];
        assert_eq!(
            accumulate(Op::Count, &[a], 0, 3, 1)
                .samples()
                .collect::<Vec<_>>(),
            vec![(0, 1.0), (2, 1.0)]
        );
        assert_eq!(
            accumulate(Op::Sum, &[a, b], 0, 3, 1)
                .samples()
                .collect::<Vec<_>>(),
            vec![(0, 11.0), (1, 20.0), (2, 33.0)]
        );
    }

    #[test]
    fn a_sample_off_the_step_grid_is_an_error() {
        let mut acc = Steps::new(Op::Sum, 0, 60_000, 30_000).unwrap();
        assert!(acc.add_series(&[15_000], &[1.0]).is_err());
        assert!(acc.add_series(&[90_000], &[1.0]).is_err());
        assert!(acc.add_series(&[-30_000], &[1.0]).is_err());
        assert!(acc
            .add_series(&[0, 30_000, 60_000], &[1.0, 2.0, 3.0])
            .is_ok());
    }

    #[test]
    fn partial_states_round_trip_and_merge() {
        for op in [
            Op::Sum,
            Op::Avg,
            Op::Count,
            Op::Min,
            Op::Max,
            Op::Group,
            Op::Stddev,
            Op::Stdvar,
        ] {
            let left_series: &[&[(i64, f64)]] = &[&[(0, 1.0), (1, 5.0)], &[(0, 3.0)]];
            let right_series: &[&[(i64, f64)]] = &[&[(0, 8.0), (1, 4.0), (2, 2.0)]];
            let all: Vec<&[(i64, f64)]> = left_series.iter().chain(right_series).copied().collect();
            let whole = accumulate(op, &all, 0, 2, 1);
            let mut left = accumulate(op, left_series, 0, 2, 1);
            let mut right = accumulate(op, right_series, 0, 2, 1);
            let mut merged = Steps::new(op, 0, 2, 1).unwrap();
            let mut states: Vec<ArrayRef> = Vec::new();
            for acc in [&mut left, &mut right] {
                let s = acc.state().unwrap().remove(0);
                states.push(s.to_array().unwrap());
            }
            for s in &states {
                merged.merge_batch(std::slice::from_ref(s)).unwrap();
            }
            let expect: Vec<(i64, f64)> = whole.samples().collect();
            let got: Vec<(i64, f64)> = merged.samples().collect();
            assert_eq!(got.len(), expect.len(), "{op:?}");
            for ((t1, a), (t2, b)) in got.iter().zip(&expect) {
                assert_eq!(t1, t2, "{op:?}");
                assert!((a - b).abs() < 1e-12, "{op:?}: {a} vs {b}");
            }
        }
    }

    #[test]
    fn evaluate_is_a_canonical_samples_list() {
        let mut acc = Steps::new(Op::Sum, 0, 60_000, 30_000).unwrap();
        acc.add_series(&[30_000], &[1.0]).unwrap();
        acc.add_series(&[0], &[2.0]).unwrap();
        let out = acc.evaluate().unwrap();
        let arr = out.to_array().unwrap();
        assert_eq!(arr.data_type(), &series::samples_type());
        let list = arr.as_list::<i32>();
        assert_eq!(list.len(), 1);
        assert_eq!(list.value(0).len(), 2);
        assert_eq!(
            empty_samples().to_array().unwrap().data_type(),
            &series::samples_type()
        );
    }
}
