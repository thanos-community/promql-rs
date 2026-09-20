//! `scalar(v)` and `absent(v)` as one aggregate function:
//! `promql_reduce(samples, 'scalar', start, end, step)`.
//!
//! Both read a whole instant vector at one step and answer with a single
//! series, and both have something to say about a step no series reached
//! at all: `scalar` a NaN, `absent` a 1. That is what separates them from
//! [`crate::aggregate`], whose `seen` bitmap leaves such a step out of
//! the result entirely, and it is why they plan as an aggregation over no
//! grouping columns — a global aggregate yields its one row even over an
//! empty input, so "nothing was there" is still a row to fill.
//!
//! The state is two lanes over the step grid: how many samples a step
//! saw, and the value of the last of them. Nothing more is needed —
//! `scalar` reads the value only where the count is exactly one, and
//! `absent` reads only whether the count is zero.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

use crate::aggregate::Grid;
use crate::series;

pub const NAME: &str = "promql_reduce";

/// The two functions that reduce a whole instant vector per step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Func {
    Scalar,
    Absent,
}

impl Func {
    pub fn parse(s: &str) -> Option<Func> {
        Some(match s {
            "scalar" => Func::Scalar,
            "absent" => Func::Absent,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Func::Scalar => "scalar",
            Func::Absent => "absent",
        }
    }

    /// The step's answer, or `None` where the function emits no sample.
    ///
    /// `funcScalar` (`promql/functions.go:763-785` at 83962c35) answers
    /// the one value a step holds and a NaN for any other count, so it
    /// always emits; `funcAbsent` (`promql/functions.go:1302-1311`)
    /// emits only where the vector was empty.
    fn value(&self, count: i64, value: f64) -> Option<f64> {
        match self {
            Func::Scalar if count == 1 => Some(value),
            Func::Scalar => Some(f64::NAN),
            Func::Absent if count == 0 => Some(1.0),
            Func::Absent => None,
        }
    }
}

/// How many samples each step of the grid saw, and the last value it saw.
#[derive(Debug)]
struct Steps {
    func: Func,
    grid: Grid,
    counts: Vec<i64>,
    values: Vec<f64>,
}

impl Steps {
    fn new(func: Func, grid: Grid) -> Self {
        Self {
            func,
            grid,
            counts: vec![0; grid.len()],
            values: vec![f64::NAN; grid.len()],
        }
    }

    /// The samples list column, row by row, onto the lanes.
    fn fold(&mut self, list: &ListArray) -> Result<()> {
        let entries = list.values().as_struct();
        let timestamps = child::<TimestampMillisecondArray>(entries, series::TIMESTAMP)?.values();
        let values = child::<Float64Array>(entries, series::VALUE)?.values();
        let offsets = list.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            // `self.grid` is `Copy`, so the closure below can hold the
            // lanes mutably while it reads the grid.
            let grid = self.grid;
            let (counts, lanes) = (&mut self.counts, &mut self.values);
            grid.runs(&timestamps[lo..hi], |index, from, len| {
                for k in 0..len {
                    counts[index + k] += 1;
                    lanes[index + k] = values[lo + from + k];
                }
            })?;
        }
        Ok(())
    }
}

impl Accumulator for Steps {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let list = values[0].as_list_opt::<i32>().ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: first argument is not a samples list"))
        })?;
        self.fold(list)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for step in 0..self.grid.len() {
            if let Some(v) = self.func.value(self.counts[step], self.values[step]) {
                timestamps.push(self.grid.timestamp(step));
                values.push(v);
            }
        }
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                Arc::new(Float64Array::from(values)),
            ],
            None,
        );
        Ok(one_row(series::sample_item(), Arc::new(entries)))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            one_row(
                state_item(STATE_COUNTS, DataType::Int64),
                Arc::new(Int64Array::from(self.counts.clone())),
            ),
            one_row(
                state_item(STATE_VALUES, DataType::Float64),
                Arc::new(Float64Array::from(self.values.clone())),
            ),
        ])
    }

    /// Two lanes of the same grid, added position by position. A value
    /// only ever matters where the total count is one, and then exactly
    /// one partial saw that sample, so taking the value of any partial
    /// that saw something is enough.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = lanes::<Int64Type>(states.first(), STATE_COUNTS)?;
        let values = lanes::<Float64Type>(states.get(1), STATE_VALUES)?;
        // The two columns are one state read row by row, so a peer that
        // sent them different lengths is a query error here rather than
        // an index out of bounds below.
        if counts.len() != values.len() {
            return Err(DataFusionError::Internal(format!(
                "{NAME}: partial state has {} {STATE_COUNTS} rows and {} {STATE_VALUES} rows",
                counts.len(),
                values.len()
            )));
        }
        for row in 0..counts.len() {
            let (count_row, value_row) = (counts.value(row), values.value(row));
            let c = count_row.as_primitive::<Int64Type>();
            let v = value_row.as_primitive::<Float64Type>();
            if c.len() != self.grid.len() || v.len() != self.grid.len() {
                return Err(DataFusionError::Internal(format!(
                    "{NAME}: partial state is {} steps, not the {} of this grid",
                    c.len(),
                    self.grid.len()
                )));
            }
            for step in 0..self.grid.len() {
                if c.value(step) > 0 {
                    self.counts[step] += c.value(step);
                    self.values[step] = v.value(step);
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.counts.capacity() * std::mem::size_of::<i64>()
            + self.values.capacity() * std::mem::size_of::<f64>()
    }
}

/// Field names of the partial state, named once so that a rename fails
/// to compile at both ends rather than mismatching across a
/// partial/final plan boundary.
const STATE_COUNTS: &str = "counts";
const STATE_VALUES: &str = "values";

fn state_item(name: &str, of: DataType) -> FieldRef {
    Arc::new(Field::new(name, of, false))
}

fn state_type(name: &str, of: DataType) -> DataType {
    DataType::List(state_item(name, of))
}

/// One list value holding `entries` whole: the single row an
/// [`Accumulator`] hands back.
fn one_row(item: FieldRef, entries: ArrayRef) -> ScalarValue {
    let len = entries.len() as i32;
    ScalarValue::List(Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(vec![0, len].into()),
        entries,
        None,
    )))
}

fn child<'a, T: 'static>(entries: &'a StructArray, name: &str) -> Result<&'a T> {
    entries
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: samples have no {name} column of its type"))
        })
}

/// One state column as the list of per-step lanes it must be.
fn lanes<T: datafusion::arrow::datatypes::ArrowPrimitiveType>(
    state: Option<&ArrayRef>,
    name: &str,
) -> Result<ListArray> {
    let list = state
        .and_then(|s| s.as_list_opt::<i32>())
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: partial state column {name} is not a list"))
        })?
        .clone();
    if list.values().as_primitive_opt::<T>().is_none() {
        return Err(DataFusionError::Internal(format!(
            "{NAME}: partial state column {name} holds {}",
            list.values().data_type()
        )));
    }
    Ok(list)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Reduce {
    signature: Signature,
}

impl Default for Reduce {
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
    AggregateUDF::new_from_impl(Reduce::default())
}

/// `promql_reduce(samples, '<func>', start, end, step)`. The grid is an
/// argument for the same reason [`crate::aggregate`] takes one: it is
/// what turns a timestamp into a lane index, and here also what says
/// which steps must be answered when no sample arrives at all.
pub fn call(samples: Expr, func: Func, start_ms: i64, end_ms: i64, step_ms: i64) -> Expr {
    udaf().call(vec![
        samples,
        lit(func.as_str()),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

/// The function and the grid, read back off the planned call.
fn from_args(args: &AccumulatorArgs) -> Result<Steps> {
    let literal = |i: usize| {
        args.exprs
            .get(i)
            .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
            .map(Literal::value)
    };
    let func = literal(1)
        .and_then(|v| match v {
            ScalarValue::Utf8(Some(s)) => Func::parse(s.as_str()),
            _ => None,
        })
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{NAME}: second argument must be scalar or absent as a string literal"
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
    let grid = Grid::new(NAME, grid(2, "start")?, grid(3, "end")?, grid(4, "step")?)?;
    Ok(Steps::new(func, grid))
}

impl AggregateUDFImpl for Reduce {
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

    /// The one series is always there, even when it holds no samples.
    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        Ok(Box::new(from_args(&args)?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, STATE_COUNTS),
                state_type(STATE_COUNTS, DataType::Int64),
                false,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, STATE_VALUES),
                state_type(STATE_VALUES, DataType::Float64),
                false,
            )),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn steps(func: Func) -> Steps {
        Steps::new(func, Grid::new(NAME, 0, 30_000, 10_000).unwrap())
    }

    /// The rule both functions are: a step is empty, singular, or
    /// crowded, and each of the three has its own answer.
    #[test]
    fn the_answer_at_a_step_follows_its_count() {
        assert_eq!(Func::Scalar.value(1, 7.5), Some(7.5));
        assert!(Func::Scalar.value(0, 7.5).unwrap().is_nan());
        assert!(Func::Scalar.value(2, 7.5).unwrap().is_nan());
        assert_eq!(Func::Absent.value(0, 7.5), Some(1.0));
        assert_eq!(Func::Absent.value(1, 7.5), None);
        assert_eq!(Func::Absent.value(2, 7.5), None);
    }

    /// A grid no series reached is where these two differ from every
    /// aggregation: both still have a row to hand back.
    #[test]
    fn an_empty_input_still_answers_at_every_step() {
        let ScalarValue::List(list) = steps(Func::Scalar).evaluate().unwrap() else {
            panic!("a samples list");
        };
        assert_eq!(list.value(0).len(), 4, "a NaN at each of four steps");

        let ScalarValue::List(list) = steps(Func::Absent).evaluate().unwrap() else {
            panic!("a samples list");
        };
        let entries = list.value(0);
        let entries = entries.as_struct();
        assert_eq!(entries.len(), 4);
        let values = entries
            .column_by_name(series::VALUE)
            .unwrap()
            .as_primitive::<Float64Type>();
        assert!((0..4).all(|i| values.value(i) == 1.0));
    }

    /// Two partitions of the same step reach the same answer as one:
    /// a step two series touched is crowded however the rows were split.
    #[test]
    fn partial_states_merge_to_the_same_counts() {
        let mut whole = steps(Func::Scalar);
        whole.counts = vec![0, 1, 2, 0];
        whole.values = vec![f64::NAN, 4.0, 9.0, f64::NAN];

        let mut left = steps(Func::Scalar);
        left.counts = vec![0, 1, 1, 0];
        left.values = vec![f64::NAN, 4.0, 5.0, f64::NAN];
        let mut right = steps(Func::Scalar);
        right.counts = vec![0, 0, 1, 0];
        right.values = vec![f64::NAN, f64::NAN, 9.0, f64::NAN];

        let mut merged = steps(Func::Scalar);
        for mut partial in [left, right] {
            let state = partial.state().unwrap();
            let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
            merged.merge_batch(&arrays).unwrap();
        }
        assert_eq!(merged.counts, whole.counts);
        assert_eq!(merged.evaluate().unwrap(), whole.evaluate().unwrap());
    }
}
