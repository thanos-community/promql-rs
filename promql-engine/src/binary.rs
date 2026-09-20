//! The arithmetic binary operators, and one-to-one vector matching as
//! an aggregate function:
//! `promql_binary(samples, __rhs, labels, '+', start, end, step)`.
//!
//! [`Op`] is the arithmetic itself, upstream's `vectorElemBinop`
//! (`promql/engine.go:3257` at 83962c35) restricted to the float/float
//! arms. It is shared by all three shapes the operators take: a scalar
//! on both sides folds through [`crate::scalar`], a scalar on one side
//! rides the elementwise kernel, and two vectors come here.
//!
//! # Why an aggregation and not a join
//!
//! Default matching pairs a left series with a right one when their
//! label sets agree apart from `__name__`, and the result carries
//! exactly those labels (`resultMetric` deletes nothing else for
//! one-to-one `ignoring()`). So the match signature *is* the output
//! label set, and grouping both sides by it gives the pairing, the
//! output labels and the duplicate detection in one node — where a join
//! would give the pairing and still need an aggregation to find the
//! duplicates, which upstream reports per step rather than per series.
//!
//! The state is four lanes over the step grid, a count and a value per
//! side. One-to-one means a step may hold at most one sample per side,
//! so the counts are what the two matching errors are raised from and
//! the values are only ever read where the count is one.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray, StringArray, StructArray,
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
use promql_parser::token::ItemType;

use crate::aggregate::Grid;
use crate::matcher::METRIC_NAME;
use crate::series;

pub const NAME: &str = "promql_binary";

/// The column telling the aggregation which operand a row came from.
///
/// Both sides are unioned into one table so that a single grouping
/// finds the match groups; this is the only thing that survives of
/// which side a series was on.
pub const SIDE: &str = "__rhs";

/// Lane index per side, so that `counts[LHS]` reads as what it is.
const LHS: usize = 0;
const RHS: usize = 1;

/// The binary operators that compute a value rather than filter one.
///
/// Comparisons are absent on purpose: they keep the left value and drop
/// the sample instead, which is a different operator shape and not
/// implemented yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
}

impl Op {
    pub fn from_token(op: ItemType) -> Option<Op> {
        Some(match op {
            ItemType::Add => Op::Add,
            ItemType::Sub => Op::Sub,
            ItemType::Mul => Op::Mul,
            ItemType::Div => Op::Div,
            ItemType::Mod => Op::Mod,
            ItemType::Pow => Op::Pow,
            ItemType::Atan2 => Op::Atan2,
            _ => return None,
        })
    }

    /// The operator back from the literal a plan carries, which is its
    /// PromQL spelling.
    pub fn parse(s: &str) -> Option<Op> {
        Some(match s {
            "+" => Op::Add,
            "-" => Op::Sub,
            "*" => Op::Mul,
            "/" => Op::Div,
            "%" => Op::Mod,
            "^" => Op::Pow,
            "atan2" => Op::Atan2,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Add => "+",
            Op::Sub => "-",
            Op::Mul => "*",
            Op::Div => "/",
            Op::Mod => "%",
            Op::Pow => "^",
            Op::Atan2 => "atan2",
        }
    }

    /// Whether the operator rewrites the metric's schema, which for
    /// every operator here it does: upstream's `changesMetricSchema`
    /// (`promql/engine.go:4208` at 83962c35) names exactly this set, and
    /// a result whose `__name__` survived would claim to be a metric it
    /// is no longer.
    pub fn drops_metric_name(&self) -> bool {
        true
    }

    /// One pair of floats, upstream's `vectorElemBinop`.
    ///
    /// Rust's `%` is Go's `math.Mod` — the remainder takes the sign of
    /// the dividend — and `powf` is `math.Pow`; the division by zero
    /// that yields an infinity or a NaN is IEEE in both languages, so
    /// none of these needs a Go-shaped wrapper the way `clamp`'s
    /// `math.Min` did.
    pub fn value(&self, lhs: f64, rhs: f64) -> f64 {
        match self {
            Op::Add => lhs + rhs,
            Op::Sub => lhs - rhs,
            Op::Mul => lhs * rhs,
            Op::Div => lhs / rhs,
            Op::Mod => lhs % rhs,
            Op::Pow => lhs.powf(rhs),
            Op::Atan2 => lhs.atan2(rhs),
        }
    }
}

/// One match group: what each side put at each step of the grid.
#[derive(Debug)]
struct Pairing {
    op: Op,
    grid: Grid,
    counts: [Vec<i64>; 2],
    values: [Vec<f64>; 2],
    /// The match group's own labels, for the message a duplicate has to
    /// name. Every row of the group renders the same text, so the first
    /// one to arrive settles it.
    group: Option<String>,
    /// Up to two label sets per side, again only for that message:
    /// upstream prints the pair that collided and there is nothing to
    /// say about a third.
    metrics: [Vec<String>; 2],
}

impl Pairing {
    fn new(op: Op, grid: Grid) -> Self {
        Self {
            op,
            grid,
            counts: [vec![0; grid.len()], vec![0; grid.len()]],
            values: [vec![f64::NAN; grid.len()], vec![f64::NAN; grid.len()]],
            group: None,
            metrics: [Vec::new(), Vec::new()],
        }
    }

    /// Keep what a failure would have to quote, before the row's
    /// samples are folded away into the lanes.
    fn remember(&mut self, side: usize, labels: &StructArray, row: usize) {
        if self.group.is_none() {
            self.group = Some(render(labels, row, true));
        }
        if self.metrics[side].len() < 2 {
            let metric = render(labels, row, false);
            if self.metrics[side].first() != Some(&metric) {
                self.metrics[side].push(metric);
            }
        }
    }

    fn fold(&mut self, side: usize, timestamps: &[i64], values: &[f64]) -> Result<()> {
        // `self.grid` is `Copy`, so the closure below can hold the lanes
        // mutably while it reads the grid.
        let grid = self.grid;
        let (counts, lanes) = (&mut self.counts[side], &mut self.values[side]);
        grid.runs(timestamps, |index, from, len| {
            for count in &mut counts[index..index + len] {
                *count += 1;
            }
            lanes[index..index + len].copy_from_slice(&values[from..from + len]);
        })
    }

    /// Upstream's message for two series on the "one" side of a match
    /// (`promql/engine.go:2999` at 83962c35). For one-to-one matching
    /// that side is always the right one.
    ///
    /// The pair inside the brackets is the first two label sets this
    /// group saw on that side, which is arrival order and not
    /// upstream's pair: upstream quotes the series that collided and
    /// the one it collided with at that step, and once three or more
    /// share a match group those need not be these two. It is the
    /// message text and nothing else, so the tests assert the sentence
    /// around the brackets rather than what is in them — DataFusion is
    /// free to hand the rows over in any order, and an assertion on the
    /// pair would be a flake waiting to happen.
    fn duplicate(&self) -> DataFusionError {
        let group = self.group.as_deref().unwrap_or("{}");
        let metrics = &self.metrics[RHS];
        // Upstream names the series it collided on first and the one
        // already held second; a side with fewer than two is out of
        // reach here, and an empty string is all there would be to say.
        let second = metrics.get(1).map(String::as_str).unwrap_or_default();
        let first = metrics.first().map(String::as_str).unwrap_or_default();
        DataFusionError::Execution(format!(
            "found duplicate series for the match group {group} on the right hand-side of the \
             operation: [{second}, {first}];many-to-many matching not allowed: matching labels \
             must be unique on one side"
        ))
    }
}

impl Accumulator for Pairing {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let list = values
            .first()
            .and_then(|v| v.as_list_opt::<i32>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: first argument is not a samples list"))
            })?;
        let side = values
            .get(1)
            .and_then(|v| v.as_boolean_opt())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: second argument is not a boolean"))
            })?;
        let labels = values
            .get(2)
            .and_then(|v| v.as_struct_opt())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: third argument is not a label struct"))
            })?;

        let entries = list.values().as_struct();
        let timestamps = child::<TimestampMillisecondArray>(entries, series::TIMESTAMP)?.values();
        let samples = child::<Float64Array>(entries, series::VALUE)?.values();
        let offsets = list.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let which = if side.is_valid(row) && side.value(row) {
                RHS
            } else {
                LHS
            };
            self.remember(which, labels, row);
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            self.fold(which, &timestamps[lo..hi], &samples[lo..hi])?;
        }
        Ok(())
    }

    /// The step grid walked once, in upstream's order: a duplicate on
    /// the "one" side is reported before a left side that matched twice,
    /// and both before any value is emitted.
    ///
    /// A step whose left side is empty is skipped rather than checked,
    /// which is where this parts company with upstream: there the
    /// short-circuit is on the whole vector, so a duplicate in *this*
    /// match group still fails the query as long as some other group
    /// had a left-hand sample at that step. Seeing that would take a
    /// second pass over every group.
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for step in 0..self.grid.len() {
            let (left, right) = (self.counts[LHS][step], self.counts[RHS][step]);
            if left == 0 {
                continue;
            }
            if right > 1 {
                return Err(self.duplicate());
            }
            if right == 0 {
                continue;
            }
            if left > 1 {
                return Err(DataFusionError::Execution(
                    "multiple matches for labels: many-to-one matching must be explicit \
                     (group_left/group_right)"
                        .into(),
                ));
            }
            timestamps.push(self.grid.timestamp(step));
            values.push(
                self.op
                    .value(self.values[LHS][step], self.values[RHS][step]),
            );
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
        let mut out = Vec::with_capacity(STATE_NAMES.len());
        for side in [LHS, RHS] {
            out.push(one_row(
                state_item(STATE_NAMES[side].counts, DataType::Int64),
                Arc::new(Int64Array::from(self.counts[side].clone())),
            ));
            out.push(one_row(
                state_item(STATE_NAMES[side].values, DataType::Float64),
                Arc::new(Float64Array::from(self.values[side].clone())),
            ));
            out.push(one_row(
                state_item(STATE_NAMES[side].metrics, DataType::Utf8),
                Arc::new(StringArray::from(self.metrics[side].clone())),
            ));
        }
        out.push(ScalarValue::Utf8(self.group.clone()));
        Ok(out)
    }

    /// Lanes added position by position, as in [`crate::aggregate`]. A
    /// value is only read where the total count is one, and then exactly
    /// one partial saw that sample, so any partial that saw something
    /// carries the value.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for side in [LHS, RHS] {
            let counts = lanes::<Int64Type>(states.get(side * 3), STATE_NAMES[side].counts)?;
            let values = lanes::<Float64Type>(states.get(side * 3 + 1), STATE_NAMES[side].values)?;
            if counts.len() != values.len() {
                return Err(DataFusionError::Internal(format!(
                    "{NAME}: partial state has {} count rows and {} value rows",
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
                        self.counts[side][step] += c.value(step);
                        self.values[side][step] = v.value(step);
                    }
                }
            }
            let metrics = states
                .get(side * 3 + 2)
                .and_then(|s| s.as_list_opt::<i32>());
            if let Some(metrics) = metrics {
                for row in 0..metrics.len() {
                    let held = metrics.value(row);
                    let held = held.as_string::<i32>();
                    for i in 0..held.len() {
                        if self.metrics[side].len() < 2
                            && self.metrics[side].first().map(String::as_str) != Some(held.value(i))
                        {
                            self.metrics[side].push(held.value(i).to_string());
                        }
                    }
                }
            }
        }
        if let Some(group) = states.get(6).and_then(|s| s.as_string_opt::<i32>()) {
            for row in 0..group.len() {
                if self.group.is_none() && group.is_valid(row) {
                    self.group = Some(group.value(row).to_string());
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let lanes: usize = self
            .counts
            .iter()
            .map(|c| c.capacity() * std::mem::size_of::<i64>())
            .sum::<usize>()
            + self
                .values
                .iter()
                .map(|v| v.capacity() * std::mem::size_of::<f64>())
                .sum::<usize>();
        std::mem::size_of::<Self>() + lanes
    }
}

/// One side's partial-state field names.
struct StateNames {
    counts: &'static str,
    values: &'static str,
    metrics: &'static str,
}

/// Named once so that a rename fails to compile at both ends rather
/// than mismatching across a partial/final plan boundary.
const STATE_NAMES: [StateNames; 2] = [
    StateNames {
        counts: "lhs_counts",
        values: "lhs_values",
        metrics: "lhs_metrics",
    },
    StateNames {
        counts: "rhs_counts",
        values: "rhs_values",
        metrics: "rhs_metrics",
    },
];

const STATE_GROUP: &str = "group";

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

/// One row of a label struct as Prometheus prints a label set.
///
/// `without_name` gives the match group, which is the same label set
/// with `__name__` taken out — upstream's `MatchLabels(false)` for
/// default matching. An empty value is how the canonical shape spells a
/// label that is not there, so it prints as nothing at all rather than
/// as `l=""`.
fn render(labels: &StructArray, row: usize, without_name: bool) -> String {
    let mut out = String::from("{");
    for (index, name) in labels.column_names().into_iter().enumerate() {
        if without_name && name == METRIC_NAME {
            continue;
        }
        let column = labels.column(index);
        let value = match column.data_type() {
            DataType::Utf8View => column.as_string_view().value(row),
            DataType::Utf8 => column.as_string::<i32>().value(row),
            _ => continue,
        };
        if value.is_empty() {
            continue;
        }
        if out.len() > 1 {
            out.push_str(", ");
        }
        out.push_str(name);
        out.push_str("=\"");
        out.push_str(value);
        out.push('"');
    }
    out.push('}');
    out
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Binary {
    signature: Signature,
}

impl Default for Binary {
    fn default() -> Self {
        // Not `exact`: the label struct's type is the query's own label
        // set, so only the count of arguments is fixed.
        Self {
            signature: Signature::any(7, Volatility::Immutable),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Binary::default())
}

/// `promql_binary(samples, <side>, labels, '<op>', start, end, step)`.
///
/// The grid is an argument for the same reason [`crate::aggregate`]
/// takes one: it turns a timestamp into a lane index. `labels` is read
/// only to quote a match group in a failure, so it is the *input's*
/// label set, `__name__` included, not the grouping's.
pub fn call(
    samples: Expr,
    side: Expr,
    labels: Expr,
    op: Op,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Expr {
    udaf().call(vec![
        samples,
        side,
        labels,
        lit(op.as_str()),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

/// The operator and the grid, read back off the planned call.
fn from_args(args: &AccumulatorArgs) -> Result<Pairing> {
    let literal = |i: usize| {
        args.exprs
            .get(i)
            .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
            .map(Literal::value)
    };
    let op = literal(3)
        .and_then(|v| match v {
            ScalarValue::Utf8(Some(s)) => Op::parse(s.as_str()),
            _ => None,
        })
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{NAME}: fourth argument must be an arithmetic operator as a string literal"
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
    let grid = Grid::new(NAME, grid(4, "start")?, grid(5, "end")?, grid(6, "step")?)?;
    Ok(Pairing::new(op, grid))
}

impl AggregateUDFImpl for Binary {
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
        if !matches!(arg_types.get(1), Some(DataType::Boolean)) {
            return plan_err!("{NAME}: second argument must say which side a row is on");
        }
        if !matches!(arg_types.get(2), Some(DataType::Struct(_))) {
            return plan_err!("{NAME}: third argument must be a label struct");
        }
        Ok(series::samples_type())
    }

    /// A match group with nothing to pair still yields its row; it holds
    /// no samples, and `series::drop_empty` takes it out of the result.
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
        let mut fields = Vec::with_capacity(7);
        for names in &STATE_NAMES {
            fields.push(Arc::new(Field::new(
                format_state_name(args.name, names.counts),
                state_type(names.counts, DataType::Int64),
                false,
            )));
            fields.push(Arc::new(Field::new(
                format_state_name(args.name, names.values),
                state_type(names.values, DataType::Float64),
                false,
            )));
            fields.push(Arc::new(Field::new(
                format_state_name(args.name, names.metrics),
                state_type(names.metrics, DataType::Utf8),
                false,
            )));
        }
        fields.push(Arc::new(Field::new(
            format_state_name(args.name, STATE_GROUP),
            DataType::Utf8,
            true,
        )));
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringViewArray;
    use datafusion::arrow::datatypes::Fields;

    use super::*;

    fn pairing(op: Op) -> Pairing {
        Pairing::new(op, Grid::new(NAME, 0, 30_000, 10_000).unwrap())
    }

    fn samples_of(value: ScalarValue) -> Vec<(i64, f64)> {
        let ScalarValue::List(list) = value else {
            panic!("a samples list")
        };
        let entries = list.value(0);
        let entries = entries.as_struct();
        let timestamps = entries
            .column_by_name(series::TIMESTAMP)
            .unwrap()
            .as_primitive::<datafusion::arrow::datatypes::TimestampMillisecondType>();
        let values = entries
            .column_by_name(series::VALUE)
            .unwrap()
            .as_primitive::<Float64Type>();
        (0..entries.len())
            .map(|i| (timestamps.value(i), values.value(i)))
            .collect()
    }

    /// Upstream's `vectorElemBinop`, including the two Go functions the
    /// operators are: `math.Mod` takes the dividend's sign, and division
    /// by zero is an infinity rather than an error.
    #[test]
    fn the_arithmetic_is_upstreams_vector_elem_binop() {
        assert_eq!(Op::Add.value(1.0, 2.0), 3.0);
        assert_eq!(Op::Sub.value(1.0, 2.0), -1.0);
        assert_eq!(Op::Mul.value(3.0, 4.0), 12.0);
        assert_eq!(Op::Div.value(1.0, 4.0), 0.25);
        assert_eq!(Op::Pow.value(2.0, 10.0), 1024.0);
        assert_eq!(Op::Atan2.value(1.0, 1.0), std::f64::consts::FRAC_PI_4);

        assert_eq!(Op::Mod.value(7.0, 3.0), 1.0);
        assert_eq!(Op::Mod.value(-7.0, 3.0), -1.0);
        assert_eq!(Op::Mod.value(7.0, -3.0), 1.0);
        assert!(Op::Mod.value(1.0, 0.0).is_nan());

        assert_eq!(Op::Div.value(1.0, 0.0), f64::INFINITY);
        assert_eq!(Op::Div.value(-1.0, 0.0), f64::NEG_INFINITY);
        assert!(Op::Div.value(0.0, 0.0).is_nan());
    }

    #[test]
    fn every_operator_round_trips_through_its_promql_spelling() {
        for op in [
            Op::Add,
            Op::Sub,
            Op::Mul,
            Op::Div,
            Op::Mod,
            Op::Pow,
            Op::Atan2,
        ] {
            assert_eq!(Op::parse(op.as_str()), Some(op));
        }
        assert_eq!(Op::from_token(ItemType::Add), Some(Op::Add));
        assert_eq!(Op::from_token(ItemType::Gtr), None);
        assert_eq!(Op::from_token(ItemType::Land), None);
    }

    /// A step only one side reached is not in the result: upstream's
    /// loop over the left side skips a sample with no match, and a right
    /// sample nothing matched is never looked at.
    #[test]
    fn only_the_steps_both_sides_reached_are_paired() {
        let mut pairing = pairing(Op::Add);
        pairing
            .fold(LHS, &[0, 10_000, 20_000], &[1.0, 2.0, 3.0])
            .unwrap();
        pairing.fold(RHS, &[10_000, 30_000], &[10.0, 30.0]).unwrap();
        assert_eq!(samples_of(pairing.evaluate().unwrap()), [(10_000, 12.0)]);
    }

    /// Two series on the same side of one match group are the two
    /// matching errors, and only where their samples meet at a step.
    #[test]
    fn two_series_in_a_match_group_fail_where_they_overlap() {
        let mut both = pairing(Op::Add);
        both.fold(LHS, &[0], &[1.0]).unwrap();
        both.fold(LHS, &[0], &[2.0]).unwrap();
        both.fold(RHS, &[0], &[3.0]).unwrap();
        let err = both.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("many-to-one matching must be explicit"),
            "{err}"
        );

        let mut right = pairing(Op::Add);
        right.fold(LHS, &[0], &[1.0]).unwrap();
        right.fold(RHS, &[0], &[2.0]).unwrap();
        right.fold(RHS, &[0], &[3.0]).unwrap();
        let err = right.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("found duplicate series for the match group"),
            "{err}"
        );
        assert!(err.contains("on the right hand-side"), "{err}");

        // Apart in time is not a duplicate at all: upstream checks the
        // vector at one step, not the series over the range.
        let mut apart = pairing(Op::Add);
        apart.fold(LHS, &[0], &[1.0]).unwrap();
        apart.fold(LHS, &[10_000], &[2.0]).unwrap();
        apart.fold(RHS, &[0, 10_000], &[3.0, 4.0]).unwrap();
        assert_eq!(
            samples_of(apart.evaluate().unwrap()),
            [(0, 4.0), (10_000, 6.0)]
        );
    }

    /// Two partitions of one match group reach the same answer as one.
    #[test]
    fn partial_states_merge_to_the_same_pairing() {
        let mut whole = pairing(Op::Mul);
        whole.fold(LHS, &[0, 10_000], &[2.0, 3.0]).unwrap();
        whole.fold(RHS, &[0, 10_000], &[5.0, 7.0]).unwrap();

        let mut left = pairing(Op::Mul);
        left.fold(LHS, &[0, 10_000], &[2.0, 3.0]).unwrap();
        let mut right = pairing(Op::Mul);
        right.fold(RHS, &[0, 10_000], &[5.0, 7.0]).unwrap();

        let mut merged = pairing(Op::Mul);
        for mut partial in [left, right] {
            let state = partial.state().unwrap();
            let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
            merged.merge_batch(&arrays).unwrap();
        }
        assert_eq!(merged.evaluate().unwrap(), whole.evaluate().unwrap());
    }

    /// The label set as Prometheus prints it, with the canonical
    /// shape's empty string read as a label that is not there.
    #[test]
    fn a_label_set_prints_as_prometheus_prints_it() {
        let fields = Fields::from(vec![
            Field::new(METRIC_NAME, series::label_type(), false),
            Field::new("job", series::label_type(), false),
            Field::new("pod", series::label_type(), false),
        ]);
        let labels = StructArray::new(
            fields,
            vec![
                Arc::new(StringViewArray::from(vec!["up"])),
                Arc::new(StringViewArray::from(vec!["api"])),
                Arc::new(StringViewArray::from(vec![""])),
            ],
            None,
        );
        assert_eq!(render(&labels, 0, false), r#"{__name__="up", job="api"}"#);
        assert_eq!(render(&labels, 0, true), r#"{job="api"}"#);
    }
}
