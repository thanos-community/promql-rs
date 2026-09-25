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
//!
//! # Why the result is a list of series
//!
//! A comparison answers with the left sample as it stood, `__name__` and
//! all, and `__name__` is exactly what the match signature forgets. Two
//! left series that differ only in it therefore share a match group
//! while upstream's `resultMetric` keeps them apart — and they are not a
//! duplicate as long as their samples never meet at a step. So the
//! aggregation carries a fifth lane, the name the left sample at each
//! step wore, and hands back one `(name, samples)` pair per distinct
//! name; the planner unnests that into the rows a group splits into.
//! The shapes that drop the name intern nothing and leave through the
//! single unnamed pair.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray, StringArray, StringViewArray,
    StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, Int64Type};
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

/// The binary operators that take a value on each side.
///
/// The set operators are not here: `and`, `or` and `unless` never look
/// at a value, so they are a different operator shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eql,
    Neq,
    Gtr,
    Lss,
    Gte,
    Lte,
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
            ItemType::EqlC => Op::Eql,
            ItemType::Neq => Op::Neq,
            ItemType::Gtr => Op::Gtr,
            ItemType::Lss => Op::Lss,
            ItemType::Gte => Op::Gte,
            ItemType::Lte => Op::Lte,
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
            Op::Eql => "==",
            Op::Neq => "!=",
            Op::Gtr => ">",
            Op::Lss => "<",
            Op::Gte => ">=",
            Op::Lte => "<=",
        }
    }

    /// The operators upstream's `IsComparisonOperator` answers for.
    /// They filter rather than compute: the sample keeps its own value
    /// and is dropped where the comparison does not hold.
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Op::Eql | Op::Neq | Op::Gtr | Op::Lss | Op::Gte | Op::Lte
        )
    }

    /// Whether the result stops being the metric it was computed from.
    ///
    /// Upstream's `changesMetricSchema` (`promql/engine.go:4208` at
    /// 83962c35) names the arithmetic operators and nothing else, so a
    /// comparison keeps `__name__` — it answers with a sample that was
    /// already there. `bool` replaces the value with a 1 or a 0, which
    /// is no longer that metric either, and drops it too.
    pub fn drops_metric_name(&self, return_bool: bool) -> bool {
        !self.is_comparison() || return_bool
    }

    /// One pair of floats, upstream's `vectorElemBinop` with the
    /// `returnBool` wrapper its callers apply: `None` is the sample
    /// upstream leaves out of the result.
    ///
    /// Rust's `%` is Go's `math.Mod` — the remainder takes the sign of
    /// the dividend — and `powf` is `math.Pow`; the division by zero
    /// that yields an infinity or a NaN is IEEE in both languages, so
    /// none of these needs a Go-shaped wrapper the way `clamp`'s
    /// `math.Min` did. A NaN compares false to everything, in Go and in
    /// Rust alike, so it is simply filtered away.
    pub fn value(&self, lhs: f64, rhs: f64, return_bool: bool) -> Option<f64> {
        if !self.is_comparison() {
            return Some(match self {
                Op::Add => lhs + rhs,
                Op::Sub => lhs - rhs,
                Op::Mul => lhs * rhs,
                Op::Div => lhs / rhs,
                Op::Mod => lhs % rhs,
                Op::Pow => lhs.powf(rhs),
                Op::Atan2 => lhs.atan2(rhs),
                _ => unreachable!("every comparison is handled below"),
            });
        }
        let keep = self.compare(lhs, rhs);
        match (return_bool, keep) {
            (true, keep) => Some(if keep { 1.0 } else { 0.0 }),
            (false, true) => Some(lhs),
            (false, false) => None,
        }
    }

    /// Whether the comparison holds. Meaningless for the arithmetic
    /// operators, which never ask.
    pub fn compare(&self, lhs: f64, rhs: f64) -> bool {
        match self {
            Op::Eql => lhs == rhs,
            Op::Neq => lhs != rhs,
            Op::Gtr => lhs > rhs,
            Op::Lss => lhs < rhs,
            Op::Gte => lhs >= rhs,
            Op::Lte => lhs <= rhs,
            _ => unreachable!("only a comparison compares"),
        }
    }
}

/// The `__name__` an output series kept, and the samples that came out
/// under it. Field names of the struct [`udaf`] answers with.
pub const PAIR_NAME: &str = "name";
pub const PAIR_SAMPLES: &str = "samples";

fn pair_fields() -> Fields {
    Fields::from(vec![
        Field::new(PAIR_NAME, series::label_type(), false),
        Field::new(PAIR_SAMPLES, series::samples_type(), false),
    ])
}

fn pair_item() -> FieldRef {
    Arc::new(Field::new("item", DataType::Struct(pair_fields()), false))
}

/// What one match group answers with: the series it splits into.
pub fn output_type() -> DataType {
    DataType::List(pair_item())
}

/// The operator as a plan carries it: its PromQL spelling, with the
/// modifier written in where the query had one. One literal rather than
/// two arguments, so a plan text reads back as the query it came from.
pub fn literal(op: Op, return_bool: bool) -> &'static str {
    if !return_bool {
        return op.as_str();
    }
    match op {
        Op::Eql => "== bool",
        Op::Neq => "!= bool",
        Op::Gtr => "> bool",
        Op::Lss => "< bool",
        Op::Gte => ">= bool",
        Op::Lte => "<= bool",
        // `bool` is only ever written on a comparison; upstream's
        // parser refuses it anywhere else.
        other => other.as_str(),
    }
}

/// [`literal`] read back.
pub fn parse_literal(s: &str) -> Option<(Op, bool)> {
    let (spelling, return_bool) = match s.strip_suffix(" bool") {
        Some(spelling) => (spelling, true),
        None => (s, false),
    };
    let op = [
        Op::Add,
        Op::Sub,
        Op::Mul,
        Op::Div,
        Op::Mod,
        Op::Pow,
        Op::Atan2,
        Op::Eql,
        Op::Neq,
        Op::Gtr,
        Op::Lss,
        Op::Gte,
        Op::Lte,
    ]
    .into_iter()
    .find(|op| op.as_str() == spelling)?;
    // `1 + bool 2` is not a query upstream's parser accepts, so it is
    // not a plan this reads back either.
    if return_bool && !op.is_comparison() {
        return None;
    }
    Some((op, return_bool))
}

/// The samples one output series ends up with, timestamps beside
/// values as the canonical shape wants them.
type Bucket = (Vec<i64>, Vec<f64>);

/// One match group: what each side put at each step of the grid.
#[derive(Debug)]
struct Pairing {
    op: Op,
    return_bool: bool,
    /// Whether the left sample's `__name__` reaches the result, so that
    /// the group has to split by it. Only a comparison without `bool`
    /// answers yes; the rest never intern a name.
    named: bool,
    grid: Grid,
    counts: [Vec<i64>; 2],
    values: [Vec<f64>; 2],
    /// The name the left sample at each step wore, as an index into
    /// [`Pairing::pool`]. Index 0 is the empty name, which is how the
    /// canonical shape spells a label that is not there.
    names: Vec<u32>,
    pool: Vec<String>,
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
    fn new(op: Op, return_bool: bool, grid: Grid) -> Self {
        Self {
            op,
            return_bool,
            named: !op.drops_metric_name(return_bool),
            grid,
            counts: [vec![0; grid.len()], vec![0; grid.len()]],
            values: [vec![f64::NAN; grid.len()], vec![f64::NAN; grid.len()]],
            names: vec![0; grid.len()],
            pool: vec![String::new()],
            group: None,
            metrics: [Vec::new(), Vec::new()],
        }
    }

    /// A name's lane index, added to the pool the first time it is
    /// seen. A match group holds a handful of names at most, so the
    /// scan is cheaper than a map would be to build.
    fn intern(&mut self, name: &str) -> u32 {
        if let Some(index) = self.pool.iter().position(|held| held == name) {
            return index as u32;
        }
        self.pool.push(name.to_string());
        (self.pool.len() - 1) as u32
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

    fn fold(&mut self, side: usize, timestamps: &[i64], values: &[f64], name: u32) -> Result<()> {
        // `self.grid` is `Copy`, so the closure below can hold the lanes
        // mutably while it reads the grid.
        let grid = self.grid;
        // Only the left side's name survives, and only where the
        // operator keeps one.
        let wears_a_name = self.named && side == LHS;
        let counts = &mut self.counts[side];
        let lanes = &mut self.values[side];
        let names = &mut self.names;
        grid.runs(timestamps, |index, from, len| {
            for count in &mut counts[index..index + len] {
                *count += 1;
            }
            lanes[index..index + len].copy_from_slice(&values[from..from + len]);
            if wears_a_name {
                names[index..index + len].fill(name);
            }
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
            let name = if self.named && which == LHS {
                self.intern(name_of(labels, row))
            } else {
                0
            };
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            self.fold(which, &timestamps[lo..hi], &samples[lo..hi], name)?;
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
        // One bucket per name the left side wore. The shapes that drop
        // the name never intern, so they all land in bucket 0 and the
        // group answers with the single series it always did.
        let mut buckets: Vec<Bucket> = (0..self.pool.len())
            .map(|_| (Vec::new(), Vec::new()))
            .collect();
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
            // A comparison that does not hold leaves the step out
            // entirely, which is what upstream's `keep` decides.
            if let Some(value) = self.op.value(
                self.values[LHS][step],
                self.values[RHS][step],
                self.return_bool,
            ) {
                let bucket = &mut buckets[self.names[step] as usize];
                bucket.0.push(self.grid.timestamp(step));
                bucket.1.push(value);
            }
        }

        // Sorted by name, because DataFusion is free to hand the rows
        // of a group over in any order and the answer must not be.
        let mut out: Vec<(&str, &Bucket)> = self
            .pool
            .iter()
            .map(String::as_str)
            .zip(buckets.iter())
            .filter(|(_, bucket)| !bucket.0.is_empty())
            .collect();
        out.sort_by_key(|(name, _)| *name);

        let mut offsets = Vec::with_capacity(out.len() + 1);
        offsets.push(0i32);
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for (_, bucket) in &out {
            timestamps.extend_from_slice(&bucket.0);
            values.extend_from_slice(&bucket.1);
            offsets.push(timestamps.len() as i32);
        }
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                Arc::new(Float64Array::from(values)),
            ],
            None,
        );
        let samples = ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(entries),
            None,
        );
        let names: Vec<&str> = out.iter().map(|(name, _)| *name).collect();
        let pairs = StructArray::new(
            pair_fields(),
            vec![Arc::new(StringViewArray::from(names)), Arc::new(samples)],
            None,
        );
        Ok(one_row(pair_item(), Arc::new(pairs)))
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
        // The lane spelled out, rather than pool plus indices: the
        // indices of two partials mean different things, and a string
        // per step needs no remapping on the way back in. Empty when
        // no name can reach the result, so the common shapes carry no
        // lane at all.
        let lane: Vec<&str> = match self.named {
            true => self
                .names
                .iter()
                .map(|i| self.pool[*i as usize].as_str())
                .collect(),
            false => Vec::new(),
        };
        out.push(one_row(
            state_item(STATE_LHS_NAMES, DataType::Utf8),
            Arc::new(StringArray::from(lane)),
        ));
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
        if let Some(lanes) = states.get(7).and_then(|s| s.as_list_opt::<i32>()) {
            for row in 0..lanes.len() {
                let lane = lanes.value(row);
                let lane = lane.as_string::<i32>();
                // A partial that carried no name says so by carrying no
                // lane; anything else is a state from another grid.
                if lane.is_empty() {
                    continue;
                }
                if lane.len() != self.grid.len() {
                    return Err(DataFusionError::Internal(format!(
                        "{NAME}: partial name lane is {} steps, not the {} of this grid",
                        lane.len(),
                        self.grid.len()
                    )));
                }
                for step in 0..lane.len() {
                    if !lane.value(step).is_empty() {
                        let id = self.intern(lane.value(step));
                        self.names[step] = id;
                    }
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
                .sum::<usize>()
            + self.names.capacity() * std::mem::size_of::<u32>()
            + self.pool.iter().map(String::capacity).sum::<usize>();
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

/// Only the left side has one, so it sits outside [`STATE_NAMES`].
const STATE_LHS_NAMES: &str = "lhs_names";

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

/// One row's `__name__`, or the empty string where the operand has no
/// such label at all — which is the same thing in the canonical shape.
fn name_of(labels: &StructArray, row: usize) -> &str {
    let Some(column) = labels.column_by_name(METRIC_NAME) else {
        return "";
    };
    match column.data_type() {
        DataType::Utf8View => column.as_string_view().value(row),
        DataType::Utf8 => column.as_string::<i32>().value(row),
        _ => "",
    }
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
/// takes one: it turns a timestamp into a lane index. `labels` is the
/// *input's* label set, `__name__` included, not the grouping's: the
/// name a comparison keeps is read off it, and so is the label set a
/// failure has to quote.
#[allow(clippy::too_many_arguments)]
pub fn call(
    samples: Expr,
    side: Expr,
    labels: Expr,
    op: Op,
    return_bool: bool,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Expr {
    udaf().call(vec![
        samples,
        side,
        labels,
        lit(literal(op, return_bool)),
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
    let (op, return_bool) = literal(3)
        .and_then(|v| match v {
            ScalarValue::Utf8(Some(s)) => parse_literal(s.as_str()),
            _ => None,
        })
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{NAME}: fourth argument must be a binary operator as a string literal"
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
    Ok(Pairing::new(op, return_bool, grid))
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
        Ok(output_type())
    }

    /// A match group with nothing to pair answers with an empty list,
    /// which the planner's `Unnest` drops on its own.
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
        let mut fields = Vec::with_capacity(8);
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
        fields.push(Arc::new(Field::new(
            format_state_name(args.name, STATE_LHS_NAMES),
            state_type(STATE_LHS_NAMES, DataType::Utf8),
            false,
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
        Pairing::new(op, false, Grid::new(NAME, 0, 30_000, 10_000).unwrap())
    }

    /// Every output series of one match group: the name it kept and the
    /// samples under it, in the order the accumulator promised.
    fn pairs_of(value: ScalarValue) -> Vec<(String, Vec<(i64, f64)>)> {
        let ScalarValue::List(list) = value else {
            panic!("a list of output series")
        };
        let pairs = list.value(0);
        let pairs = pairs.as_struct();
        let names = pairs.column_by_name(PAIR_NAME).unwrap().as_string_view();
        let samples = pairs.column_by_name(PAIR_SAMPLES).unwrap().as_list::<i32>();
        (0..pairs.len())
            .map(|row| {
                let entries = samples.value(row);
                let entries = entries.as_struct();
                let timestamps = entries
                    .column_by_name(series::TIMESTAMP)
                    .unwrap()
                    .as_primitive::<datafusion::arrow::datatypes::TimestampMillisecondType>(
                );
                let values = entries
                    .column_by_name(series::VALUE)
                    .unwrap()
                    .as_primitive::<Float64Type>();
                let samples = (0..entries.len())
                    .map(|i| (timestamps.value(i), values.value(i)))
                    .collect();
                (names.value(row).to_string(), samples)
            })
            .collect()
    }

    /// The samples of a group that answers with one series, which is
    /// every shape but a comparison over two differently named metrics.
    fn samples_of(value: ScalarValue) -> Vec<(i64, f64)> {
        match pairs_of(value).as_slice() {
            [] => Vec::new(),
            [(_, samples)] => samples.clone(),
            many => panic!("{} output series, not one", many.len()),
        }
    }

    /// Upstream's `vectorElemBinop`, including the two Go functions the
    /// operators are: `math.Mod` takes the dividend's sign, and division
    /// by zero is an infinity rather than an error.
    #[test]
    fn the_arithmetic_is_upstreams_vector_elem_binop() {
        let v = |op: Op, l, r| op.value(l, r, false).expect("arithmetic always keeps");
        assert_eq!(v(Op::Add, 1.0, 2.0), 3.0);
        assert_eq!(v(Op::Sub, 1.0, 2.0), -1.0);
        assert_eq!(v(Op::Mul, 3.0, 4.0), 12.0);
        assert_eq!(v(Op::Div, 1.0, 4.0), 0.25);
        assert_eq!(v(Op::Pow, 2.0, 10.0), 1024.0);
        assert_eq!(v(Op::Atan2, 1.0, 1.0), std::f64::consts::FRAC_PI_4);

        assert_eq!(v(Op::Mod, 7.0, 3.0), 1.0);
        assert_eq!(v(Op::Mod, -7.0, 3.0), -1.0);
        assert_eq!(v(Op::Mod, 7.0, -3.0), 1.0);
        assert!(v(Op::Mod, 1.0, 0.0).is_nan());

        assert_eq!(v(Op::Div, 1.0, 0.0), f64::INFINITY);
        assert_eq!(v(Op::Div, -1.0, 0.0), f64::NEG_INFINITY);
        assert!(v(Op::Div, 0.0, 0.0).is_nan());
    }

    /// A comparison answers with the sample it was given or with
    /// nothing at all; `bool` turns that into a 1 or a 0 and always
    /// answers.
    #[test]
    fn a_comparison_filters_and_bool_scores() {
        assert_eq!(Op::Gtr.value(3.0, 2.0, false), Some(3.0));
        assert_eq!(Op::Gtr.value(2.0, 3.0, false), None);
        assert_eq!(Op::Gtr.value(3.0, 2.0, true), Some(1.0));
        assert_eq!(Op::Gtr.value(2.0, 3.0, true), Some(0.0));

        assert_eq!(Op::Eql.value(2.0, 2.0, false), Some(2.0));
        assert_eq!(Op::Neq.value(2.0, 2.0, false), None);
        assert_eq!(Op::Gte.value(2.0, 2.0, false), Some(2.0));
        assert_eq!(Op::Lte.value(2.0, 2.0, false), Some(2.0));
        assert_eq!(Op::Lss.value(2.0, 2.0, false), None);

        // A NaN compares false to everything, `!=` included by
        // negation, so it survives only that one.
        for op in [Op::Eql, Op::Gtr, Op::Lss, Op::Gte, Op::Lte] {
            assert_eq!(op.value(f64::NAN, 1.0, false), None, "{}", op.as_str());
            assert_eq!(op.value(f64::NAN, 1.0, true), Some(0.0), "{}", op.as_str());
        }
        assert!(Op::Neq.value(f64::NAN, 1.0, false).unwrap().is_nan());
    }

    /// Only a comparison without `bool` answers with the metric it was
    /// given, so only that one keeps its name.
    #[test]
    fn what_drops_the_metric_name_is_changes_metric_schema_plus_bool() {
        assert!(Op::Add.drops_metric_name(false));
        assert!(Op::Atan2.drops_metric_name(false));
        assert!(!Op::Gtr.drops_metric_name(false));
        assert!(Op::Gtr.drops_metric_name(true));
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
            Op::Eql,
            Op::Neq,
            Op::Gtr,
            Op::Lss,
            Op::Gte,
            Op::Lte,
        ] {
            assert_eq!(parse_literal(literal(op, false)), Some((op, false)));
            if op.is_comparison() {
                assert_eq!(parse_literal(literal(op, true)), Some((op, true)));
                assert_eq!(literal(op, true), format!("{} bool", op.as_str()));
            }
        }
        assert_eq!(Op::from_token(ItemType::Add), Some(Op::Add));
        assert_eq!(Op::from_token(ItemType::Gtr), Some(Op::Gtr));
        assert_eq!(Op::from_token(ItemType::Land), None);
        assert_eq!(parse_literal("and"), None);
        // `bool` belongs to a comparison and to nothing else, so a plan
        // that spells it anywhere else is not one this wrote.
        assert_eq!(parse_literal("+ bool"), None);
        assert_eq!(parse_literal("atan2 bool"), None);
    }

    /// A step only one side reached is not in the result: upstream's
    /// loop over the left side skips a sample with no match, and a right
    /// sample nothing matched is never looked at.
    #[test]
    fn only_the_steps_both_sides_reached_are_paired() {
        let mut pairing = pairing(Op::Add);
        pairing
            .fold(LHS, &[0, 10_000, 20_000], &[1.0, 2.0, 3.0], 0)
            .unwrap();
        pairing
            .fold(RHS, &[10_000, 30_000], &[10.0, 30.0], 0)
            .unwrap();
        assert_eq!(samples_of(pairing.evaluate().unwrap()), [(10_000, 12.0)]);
    }

    /// A comparison between two vectors answers with the left sample
    /// where it holds and leaves the step out where it does not — the
    /// filter upstream's `keep` is, one step at a time.
    #[test]
    fn a_comparison_between_vectors_filters_step_by_step() {
        let mut filtered = pairing(Op::Gtr);
        filtered.fold(LHS, &[0, 10_000], &[5.0, 1.0], 0).unwrap();
        filtered.fold(RHS, &[0, 10_000], &[2.0, 9.0], 0).unwrap();
        assert_eq!(samples_of(filtered.evaluate().unwrap()), [(0, 5.0)]);

        let mut scored = Pairing::new(Op::Gtr, true, Grid::new(NAME, 0, 30_000, 10_000).unwrap());
        scored.fold(LHS, &[0, 10_000], &[5.0, 1.0], 0).unwrap();
        scored.fold(RHS, &[0, 10_000], &[2.0, 9.0], 0).unwrap();
        assert_eq!(
            samples_of(scored.evaluate().unwrap()),
            [(0, 1.0), (10_000, 0.0)]
        );
    }

    /// Two series on the same side of one match group are the two
    /// matching errors, and only where their samples meet at a step.
    #[test]
    fn two_series_in_a_match_group_fail_where_they_overlap() {
        let mut both = pairing(Op::Add);
        both.fold(LHS, &[0], &[1.0], 0).unwrap();
        both.fold(LHS, &[0], &[2.0], 0).unwrap();
        both.fold(RHS, &[0], &[3.0], 0).unwrap();
        let err = both.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("many-to-one matching must be explicit"),
            "{err}"
        );

        let mut right = pairing(Op::Add);
        right.fold(LHS, &[0], &[1.0], 0).unwrap();
        right.fold(RHS, &[0], &[2.0], 0).unwrap();
        right.fold(RHS, &[0], &[3.0], 0).unwrap();
        let err = right.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("found duplicate series for the match group"),
            "{err}"
        );
        assert!(err.contains("on the right hand-side"), "{err}");

        // Apart in time is not a duplicate at all: upstream checks the
        // vector at one step, not the series over the range.
        let mut apart = pairing(Op::Add);
        apart.fold(LHS, &[0], &[1.0], 0).unwrap();
        apart.fold(LHS, &[10_000], &[2.0], 0).unwrap();
        apart.fold(RHS, &[0, 10_000], &[3.0, 4.0], 0).unwrap();
        assert_eq!(
            samples_of(apart.evaluate().unwrap()),
            [(0, 4.0), (10_000, 6.0)]
        );
    }

    /// Two left series that differ only in `__name__` share a match
    /// group, because the signature drops the name — but a comparison
    /// gives that name back, so the group has to answer with both.
    #[test]
    fn a_kept_name_splits_the_match_group_back_up() {
        let mut pairing = Pairing::new(Op::Gtr, false, Grid::new(NAME, 0, 30_000, 10_000).unwrap());
        let b = pairing.intern("b");
        let a = pairing.intern("a");
        pairing.fold(LHS, &[0], &[5.0], b).unwrap();
        pairing.fold(LHS, &[10_000], &[7.0], a).unwrap();
        pairing.fold(RHS, &[0, 10_000], &[1.0, 1.0], 0).unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [
                ("a".to_string(), vec![(10_000, 7.0)]),
                ("b".to_string(), vec![(0, 5.0)]),
            ]
        );

        // `bool` takes the name away again, so the same two series are
        // one result — which is what upstream's `resultMetric` does
        // once `changesMetricSchema` holds.
        let mut scored = Pairing::new(Op::Gtr, true, Grid::new(NAME, 0, 30_000, 10_000).unwrap());
        assert_eq!(scored.intern("b"), b);
        scored.fold(LHS, &[0], &[5.0], b).unwrap();
        scored.fold(LHS, &[10_000], &[7.0], a).unwrap();
        scored.fold(RHS, &[0, 10_000], &[1.0, 1.0], 0).unwrap();
        assert_eq!(
            pairs_of(scored.evaluate().unwrap()),
            [(String::new(), vec![(0, 1.0), (10_000, 1.0)])]
        );
    }

    /// Two partitions of one match group reach the same answer as one.
    #[test]
    fn partial_states_merge_to_the_same_pairing() {
        let mut whole = pairing(Op::Mul);
        whole.fold(LHS, &[0, 10_000], &[2.0, 3.0], 0).unwrap();
        whole.fold(RHS, &[0, 10_000], &[5.0, 7.0], 0).unwrap();

        let mut left = pairing(Op::Mul);
        left.fold(LHS, &[0, 10_000], &[2.0, 3.0], 0).unwrap();
        let mut right = pairing(Op::Mul);
        right.fold(RHS, &[0, 10_000], &[5.0, 7.0], 0).unwrap();

        let mut merged = pairing(Op::Mul);
        for mut partial in [left, right] {
            let state = partial.state().unwrap();
            let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
            merged.merge_batch(&arrays).unwrap();
        }
        assert_eq!(merged.evaluate().unwrap(), whole.evaluate().unwrap());
    }

    /// The name lane survives the same round trip, and the pool indices
    /// of one partial mean nothing in another — which is why the lane
    /// travels as the names themselves.
    #[test]
    fn partial_states_merge_the_names_they_carried() {
        let grid = Grid::new(NAME, 0, 30_000, 10_000).unwrap();
        let mut left = Pairing::new(Op::Gtr, false, grid);
        let name = left.intern("a");
        left.fold(LHS, &[0], &[5.0], name).unwrap();

        let mut right = Pairing::new(Op::Gtr, false, grid);
        // A different pool, so "b" is index 1 here and would collide
        // with "a" if the index were what crossed over.
        let name = right.intern("b");
        right.fold(LHS, &[10_000], &[7.0], name).unwrap();
        right.fold(RHS, &[0, 10_000], &[1.0, 1.0], 0).unwrap();

        let mut merged = Pairing::new(Op::Gtr, false, grid);
        for mut partial in [left, right] {
            let state = partial.state().unwrap();
            let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
            merged.merge_batch(&arrays).unwrap();
        }
        assert_eq!(
            pairs_of(merged.evaluate().unwrap()),
            [
                ("a".to_string(), vec![(0, 5.0)]),
                ("b".to_string(), vec![(10_000, 7.0)]),
            ]
        );
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
