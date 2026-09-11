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
//! through `promql_instant_vector` or a range function — so a group's
//! state is simply a map from step timestamp to a running value, and
//! merging two partial states is merging two maps. A step no series
//! contributed to is absent from the map and therefore from the output,
//! which is what Prometheus does too.
//!
//! The operator is a literal argument rather than eight registered
//! functions: one name to register, one plan to serialize, and the
//! same reasoning as the parameters of `promql_instant_vector`.

use std::any::Any;
use std::collections::BTreeMap;
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

use crate::math::{max_nan_loses, min_nan_loses, KahanSum, Mean, Welford};
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

/// The accumulator for one group.
#[derive(Debug)]
pub struct Steps {
    op: Op,
    steps: BTreeMap<i64, State>,
}

impl Steps {
    pub fn new(op: Op) -> Self {
        Self {
            op,
            steps: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, ts: i64, f: f64) {
        self.steps
            .entry(ts)
            .or_insert_with(|| State::new(self.op))
            .add(f);
    }

    fn merge(&mut self, ts: i64, other: &State) {
        match self.steps.get_mut(&ts) {
            Some(s) => s.merge(other),
            None => {
                self.steps.insert(ts, *other);
            }
        }
    }

    /// The group's series, in step order.
    pub fn samples(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.steps.iter().map(|(t, s)| (*t, s.result(self.op)))
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
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
            for i in a..b {
                self.add(ts[i], vs[i]);
            }
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
        std::mem::size_of::<Self>()
            + self.steps.len() * (std::mem::size_of::<i64>() + std::mem::size_of::<State>())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let n = self.steps.len();
        let mut ts = Vec::with_capacity(n);
        let (mut a, mut b, mut c) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let mut m = Vec::with_capacity(n);
        for (t, s) in &self.steps {
            let (ra, rb, rn, rm) = s.to_row();
            ts.push(*t);
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
                self.merge(ts[i], &s);
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
                vec![series::samples_type(), DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Aggregate::default())
}

/// `promql_aggregate(samples, '<op>')`.
pub fn call(samples: Expr, op: Op) -> Expr {
    udaf().call(vec![samples, lit(op.as_str())])
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
        let op = args
            .exprs
            .get(1)
            .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
            .and_then(|l| match l.value() {
                ScalarValue::Utf8(Some(s)) => Op::parse(s.as_str()),
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "{NAME}: second argument must be one of sum, avg, count, min, max, group, stddev, stdvar as a string literal"
                ))
            })?;
        Ok(Box::new(Steps::new(op)))
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

    fn run(op: Op, series: &[&[(i64, f64)]]) -> Vec<(i64, f64)> {
        let mut acc = Steps::new(op);
        for s in series {
            for (t, v) in *s {
                acc.add(*t, *v);
            }
        }
        acc.samples().collect()
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
            let mut whole = Steps::new(op);
            let mut left = Steps::new(op);
            let mut right = Steps::new(op);
            for (t, v) in [(0, 1.0), (1, 5.0), (0, 3.0)] {
                whole.add(t, v);
                left.add(t, v);
            }
            for (t, v) in [(0, 8.0), (2, 2.0)] {
                whole.add(t, v);
                right.add(t, v);
            }
            let mut merged = Steps::new(op);
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
        let mut acc = Steps::new(Op::Sum);
        acc.add(30_000, 1.0);
        acc.add(0, 2.0);
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
