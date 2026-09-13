//! The value-level kernels of a binary operator.
//!
//! Five scalar functions, all of them a row loop over a samples list,
//! all of them stateless with every parameter a literal argument so the
//! plan says what it does:
//!
//! | function | in | out |
//! |---|---|---|
//! | `promql_binary_match` | two aligned samples lists | [`matches_type`] |
//! | `promql_binary_scalar` | a series and a scalar | samples |
//! | `promql_binary_filter` | a series and a presence mask | [`matches_type`] |
//! | `promql_scalar_literal` | nothing | samples |
//! | `promql_unique_series` | a series and how many collapsed into it | samples |
//!
//! Both operands of an operator are already on the same step grid, so
//! lining them up is a merge of two ascending timestamp runs rather than
//! any kind of search. A step where only one side has a sample is not a
//! match and produces nothing, which is `VectorBinop` iterating the
//! instant vector at that step.
//!
//! # Why a match is not a sample
//!
//! [`matches_type`] is a samples list with a `keep` flag. Prometheus
//! checks for duplicate matches *before* it decides whether a comparison
//! filters the sample away, so `foo > bar` with two `foo` series matching
//! one `bar` is an error even at a step where neither comparison is true.
//! The flag carries "this pair matched" separately from "this sample
//! survives", so [`super::group`] can count matches and still emit only
//! the survivors.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, ListArray, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Float64Type, Int64Type, TimestampMillisecondType,
};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    lit, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};

use crate::error::QueryError;
use crate::grid::Grid;
use crate::series;

use super::Op;

pub const MATCH_NAME: &str = "promql_binary_match";
pub const SCALAR_NAME: &str = "promql_binary_scalar";
pub const FILTER_NAME: &str = "promql_binary_filter";
pub const LITERAL_NAME: &str = "promql_scalar_literal";
pub const UNIQUE_NAME: &str = "promql_unique_series";

/// Marks a matched pair whose sample a comparison filtered away.
pub const KEEP: &str = "keep";

pub fn match_fields() -> Fields {
    Fields::from(vec![
        Field::new(series::TIMESTAMP, series::timestamp_type(), false),
        Field::new(series::VALUE, DataType::Float64, false),
        Field::new(KEEP, DataType::Boolean, false),
    ])
}

pub fn match_item() -> FieldRef {
    Arc::new(Field::new(
        series::LIST_ITEM,
        DataType::Struct(match_fields()),
        false,
    ))
}

/// One operator's matched pairs per series: the canonical samples list
/// with a flag for whether each match survived a comparison.
pub fn matches_type() -> DataType {
    DataType::List(match_item())
}

/// `vectorElemBinop` for floats: the value and whether it survives.
///
/// A comparison yields the left value and keeps it only when the
/// comparison holds; everything else always keeps. `bool` is applied by
/// the caller, because a vector-scalar comparison overrides the value
/// first.
pub fn elem_binop(op: Op, lhs: f64, rhs: f64) -> (f64, bool) {
    match op {
        Op::Add => (lhs + rhs, true),
        Op::Sub => (lhs - rhs, true),
        Op::Mul => (lhs * rhs, true),
        Op::Div => (lhs / rhs, true),
        // Go's math.Mod and math.Pow, which are Rust's operators on f64.
        Op::Mod => (lhs % rhs, true),
        Op::Pow => (lhs.powf(rhs), true),
        Op::Atan2 => (lhs.atan2(rhs), true),
        Op::Eq => (lhs, lhs == rhs),
        Op::Ne => (lhs, lhs != rhs),
        Op::Gt => (lhs, lhs > rhs),
        Op::Lt => (lhs, lhs < rhs),
        Op::Ge => (lhs, lhs >= rhs),
        Op::Le => (lhs, lhs <= rhs),
        // Set operators never reach a value kernel; they are presence
        // only, and the planner routes them to `promql_binary_filter`.
        Op::And | Op::Or | Op::Unless => (lhs, true),
    }
}

/// `bool`: the comparison's outcome as a value, and nothing filtered.
fn apply_bool(value: f64, keep: bool, return_bool: bool) -> (f64, bool) {
    if return_bool {
        (if keep { 1.0 } else { 0.0 }, true)
    } else {
        (value, keep)
    }
}

/// One row's samples, as the two slices the kernels walk.
struct Rows<'a> {
    ts: &'a [i64],
    vs: &'a [f64],
    offsets: &'a [i32],
    list: &'a ListArray,
}

impl<'a> Rows<'a> {
    fn new(list: &'a ListArray, what: &str) -> Result<Self> {
        let entries = list.values().as_struct();
        let ts = entries
            .column_by_name(series::TIMESTAMP)
            .ok_or_else(|| DataFusionError::Internal(format!("{what}: no timestamp column")))?
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let vs = entries
            .column_by_name(series::VALUE)
            .ok_or_else(|| DataFusionError::Internal(format!("{what}: no value column")))?
            .as_primitive::<Float64Type>()
            .values();
        Ok(Self {
            ts,
            vs,
            offsets: list.offsets(),
            list,
        })
    }

    /// Row `row`'s samples, empty for a NULL row: a mask that did not
    /// join is a mask with nothing in it.
    fn row(&self, row: usize) -> (&'a [i64], &'a [f64]) {
        if self.list.is_null(row) {
            return (&[], &[]);
        }
        let (a, b) = (self.offsets[row] as usize, self.offsets[row + 1] as usize);
        (&self.ts[a..b], &self.vs[a..b])
    }
}

/// Builds a list column one row at a time.
#[derive(Default)]
struct Lists {
    ts: Vec<i64>,
    vs: Vec<f64>,
    keep: Vec<bool>,
    offsets: Vec<i32>,
}

impl Lists {
    fn new(rows: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
        Self {
            offsets,
            ..Default::default()
        }
    }

    fn push(&mut self, ts: i64, value: f64, keep: bool) {
        self.ts.push(ts);
        self.vs.push(value);
        self.keep.push(keep);
    }

    fn end_row(&mut self) {
        self.offsets.push(self.ts.len() as i32);
    }

    fn finish(self, with_keep: bool) -> ListArray {
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMillisecondArray::from(self.ts)),
            Arc::new(Float64Array::from(self.vs)),
        ];
        let (fields, item) = if with_keep {
            columns.push(Arc::new(BooleanArray::from(self.keep)));
            (match_fields(), match_item())
        } else {
            (series::sample_fields(), series::sample_item())
        };
        ListArray::new(
            item,
            OffsetBuffer::new(self.offsets.into()),
            Arc::new(StructArray::new(fields, columns, None)),
            None,
        )
    }
}

/// The literal arguments the kernels take, read back at execution time.
fn str_arg(args: &ScalarFunctionArgs, i: usize, name: &str) -> Result<String> {
    match &args.args[i] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s.clone()),
        other => Err(DataFusionError::Execution(format!(
            "{name}: argument {i} must be a string literal, got {:?}",
            other.data_type()
        ))),
    }
}

fn bool_arg(args: &ScalarFunctionArgs, i: usize, name: &str) -> Result<bool> {
    match &args.args[i] {
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => Ok(*b),
        other => Err(DataFusionError::Execution(format!(
            "{name}: argument {i} must be a boolean literal, got {:?}",
            other.data_type()
        ))),
    }
}

fn int_arg(args: &ScalarFunctionArgs, i: usize, name: &str) -> Result<i64> {
    match &args.args[i] {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v),
        other => Err(DataFusionError::Execution(format!(
            "{name}: argument {i} must be an Int64 literal, got {:?}",
            other.data_type()
        ))),
    }
}

fn op_arg(args: &ScalarFunctionArgs, i: usize, name: &str) -> Result<Op> {
    let s = str_arg(args, i, name)?;
    Op::parse_str(&s)
        .ok_or_else(|| DataFusionError::Execution(format!("{name}: unknown operator {s}")))
}

fn list_arg(args: &ScalarFunctionArgs, i: usize) -> Result<ArrayRef> {
    match &args.args[i] {
        ColumnarValue::Array(a) => Ok(Arc::clone(a)),
        ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows),
    }
}

/// Non-nullable, always: DataFusion's default would mark the field
/// nullable and the next operator's shape check would reject it.
fn non_null_field(name: &'static str, data_type: DataType) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(name, data_type, false)))
}

fn expect_samples(name: &str, arg: Option<&DataType>, which: &str) -> Result<()> {
    if arg != Some(&series::samples_type()) {
        return plan_err!(
            "{name}: {which} argument must be {}, got {arg:?}",
            series::samples_type()
        );
    }
    Ok(())
}

// ---------------------------------------------------------------- match

/// `promql_binary_match(lhs, rhs, '<op>', <bool>, <swap>)`.
///
/// `swap` is `group_right`: the many side is the operator's right-hand
/// operand, so the arithmetic evaluates the other way round while the
/// row still carries the many side first.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Match {
    signature: Signature,
}

impl Default for Match {
    fn default() -> Self {
        Self {
            signature: Signature::any(5, Volatility::Immutable),
        }
    }
}

pub fn match_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Match::default())
}

pub fn match_call(lhs: Expr, rhs: Expr, op: Op, return_bool: bool, swap: bool) -> Expr {
    match_udf().call(vec![
        lhs,
        rhs,
        lit(op.as_str()),
        lit(return_bool),
        lit(swap),
    ])
}

impl ScalarUDFImpl for Match {
    fn name(&self) -> &str {
        MATCH_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        expect_samples(MATCH_NAME, arg_types.first(), "first")?;
        expect_samples(MATCH_NAME, arg_types.get(1), "second")?;
        Ok(matches_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        non_null_field(MATCH_NAME, matches_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (op, return_bool, swap) = (
            op_arg(&args, 2, MATCH_NAME)?,
            bool_arg(&args, 3, MATCH_NAME)?,
            bool_arg(&args, 4, MATCH_NAME)?,
        );
        let (left, right) = (list_arg(&args, 0)?, list_arg(&args, 1)?);
        let (left, right) = (left.as_list::<i32>(), right.as_list::<i32>());
        let (a, b) = (Rows::new(left, MATCH_NAME)?, Rows::new(right, MATCH_NAME)?);

        let mut out = Lists::new(left.len());
        for row in 0..left.len() {
            let ((a_ts, a_vs), (b_ts, b_vs)) = (a.row(row), b.row(row));
            let (mut i, mut j) = (0, 0);
            while i < a_ts.len() && j < b_ts.len() {
                match a_ts[i].cmp(&b_ts[j]) {
                    std::cmp::Ordering::Less => i += 1,
                    std::cmp::Ordering::Greater => j += 1,
                    std::cmp::Ordering::Equal => {
                        let (l, r) = if swap {
                            (b_vs[j], a_vs[i])
                        } else {
                            (a_vs[i], b_vs[j])
                        };
                        let (value, keep) = elem_binop(op, l, r);
                        let (value, keep) = apply_bool(value, keep, return_bool);
                        out.push(a_ts[i], value, keep);
                        i += 1;
                        j += 1;
                    }
                }
            }
            out.end_row();
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish(true))))
    }
}

// --------------------------------------------------------------- scalar

/// `promql_binary_scalar(samples, scalar, '<op>', <bool>, <swap>)`.
///
/// `VectorscalarBinop`. The first argument is always the vector side, so
/// `swap` says the scalar was written on the left. A comparison with the
/// scalar on the left still yields the vector's value.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Scalar {
    signature: Signature,
}

impl Default for Scalar {
    fn default() -> Self {
        Self {
            signature: Signature::any(5, Volatility::Immutable),
        }
    }
}

pub fn scalar_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Scalar::default())
}

pub fn scalar_call(vector: Expr, scalar: Expr, op: Op, return_bool: bool, swap: bool) -> Expr {
    scalar_udf().call(vec![
        vector,
        scalar,
        lit(op.as_str()),
        lit(return_bool),
        lit(swap),
    ])
}

impl ScalarUDFImpl for Scalar {
    fn name(&self) -> &str {
        SCALAR_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        expect_samples(SCALAR_NAME, arg_types.first(), "first")?;
        expect_samples(SCALAR_NAME, arg_types.get(1), "second")?;
        Ok(series::samples_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        non_null_field(SCALAR_NAME, series::samples_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (op, return_bool, swap) = (
            op_arg(&args, 2, SCALAR_NAME)?,
            bool_arg(&args, 3, SCALAR_NAME)?,
            bool_arg(&args, 4, SCALAR_NAME)?,
        );
        let (vector, scalar) = (list_arg(&args, 0)?, list_arg(&args, 1)?);
        let (vector, scalar) = (vector.as_list::<i32>(), scalar.as_list::<i32>());
        let (v, s) = (
            Rows::new(vector, SCALAR_NAME)?,
            Rows::new(scalar, SCALAR_NAME)?,
        );

        let mut out = Lists::new(vector.len());
        for row in 0..vector.len() {
            let ((v_ts, v_vs), (s_ts, s_vs)) = (v.row(row), s.row(row));
            let (mut i, mut j) = (0, 0);
            while i < v_ts.len() && j < s_ts.len() {
                match v_ts[i].cmp(&s_ts[j]) {
                    std::cmp::Ordering::Less => i += 1,
                    std::cmp::Ordering::Greater => j += 1,
                    std::cmp::Ordering::Equal => {
                        let (l, r) = if swap {
                            (s_vs[j], v_vs[i])
                        } else {
                            (v_vs[i], s_vs[j])
                        };
                        let (mut value, keep) = elem_binop(op, l, r);
                        // The vector's value is the result of a
                        // comparison whichever side it was written on.
                        if op.is_comparison() && swap {
                            value = v_vs[i];
                        }
                        let (value, keep) = apply_bool(value, keep, return_bool);
                        if keep {
                            out.push(v_ts[i], value, true);
                        }
                        i += 1;
                        j += 1;
                    }
                }
            }
            out.end_row();
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish(false))))
    }
}

// --------------------------------------------------------------- filter

/// `promql_binary_filter(samples)` or
/// `promql_binary_filter(samples, mask, <keep_present>)`.
///
/// The set operators. `mask` is the other side's presence per step, one
/// row per match group, and a NULL mask is an empty one: nothing on the
/// other side had this signature at all. The one-argument form keeps
/// everything, which is the left side of `or`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Filter {
    signature: Signature,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

pub fn filter_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Filter::default())
}

pub fn filter_call(samples: Expr, mask: Option<(Expr, bool)>) -> Expr {
    match mask {
        Some((mask, keep_present)) => filter_udf().call(vec![samples, mask, lit(keep_present)]),
        None => filter_udf().call(vec![samples]),
    }
}

impl ScalarUDFImpl for Filter {
    fn name(&self) -> &str {
        FILTER_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        expect_samples(FILTER_NAME, arg_types.first(), "first")?;
        if arg_types.len() > 1 {
            expect_samples(FILTER_NAME, arg_types.get(1), "second")?;
        }
        Ok(matches_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        non_null_field(FILTER_NAME, matches_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let samples = list_arg(&args, 0)?;
        let samples = samples.as_list::<i32>();
        let rows = Rows::new(samples, FILTER_NAME)?;
        let masked = if args.args.len() > 1 {
            let mask = list_arg(&args, 1)?;
            Some((mask, bool_arg(&args, 2, FILTER_NAME)?))
        } else {
            None
        };
        let mask = masked.as_ref().map(|(mask, _)| mask.as_list::<i32>());
        let mask_rows = match mask {
            Some(m) => Some(Rows::new(m, FILTER_NAME)?),
            None => None,
        };
        let keep_present = masked.as_ref().map(|(_, k)| *k).unwrap_or(false);

        let mut out = Lists::new(samples.len());
        for row in 0..samples.len() {
            let (ts, vs) = rows.row(row);
            match &mask_rows {
                None => {
                    for (t, v) in ts.iter().zip(vs) {
                        out.push(*t, *v, true);
                    }
                }
                Some(m) => {
                    let (m_ts, _) = m.row(row);
                    let mut j = 0;
                    for (t, v) in ts.iter().zip(vs) {
                        while j < m_ts.len() && m_ts[j] < *t {
                            j += 1;
                        }
                        let present = j < m_ts.len() && m_ts[j] == *t;
                        if present == keep_present {
                            out.push(*t, *v, true);
                        }
                    }
                }
            }
            out.end_row();
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish(true))))
    }
}

// -------------------------------------------------------------- literal

/// `promql_scalar_literal(<value>, start, end, step)`: a scalar as a
/// series, one sample at every step. A scalar in PromQL has a value at
/// every instant, which in this shape is a full grid.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Literal {
    signature: Signature,
}

impl Default for Literal {
    fn default() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

pub fn literal_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Literal::default())
}

pub fn literal_call(value: f64, start_ms: i64, end_ms: i64, step_ms: i64) -> Expr {
    literal_udf().call(vec![lit(value), lit(start_ms), lit(end_ms), lit(step_ms)])
}

impl ScalarUDFImpl for Literal {
    fn name(&self) -> &str {
        LITERAL_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(series::samples_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        non_null_field(LITERAL_NAME, series::samples_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let value = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => *v,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "{LITERAL_NAME}: argument 0 must be a Float64 literal, got {:?}",
                    other.data_type()
                )))
            }
        };
        let grid = Grid::new(
            LITERAL_NAME,
            int_arg(&args, 1, LITERAL_NAME)?,
            int_arg(&args, 2, LITERAL_NAME)?,
            int_arg(&args, 3, LITERAL_NAME)?,
        )?;
        let mut out = Lists::new(args.number_rows);
        for _ in 0..args.number_rows {
            for ts in grid.timestamps() {
                out.push(ts, value, true);
            }
            out.end_row();
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish(false))))
    }
}

// --------------------------------------------------------------- unique

/// `promql_unique_series(samples, count)`.
///
/// Unary minus drops `__name__`, and Prometheus refuses the result if
/// that leaves two series sharing one label set: `eval`'s `UnaryExpr`
/// arm ends in `ContainsSameLabelset`, the only place in the language
/// that looks. `count` is how many series the regroup underneath
/// collapsed into this one, so anything above one is that collision.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Unique {
    signature: Signature,
}

impl Default for Unique {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

pub fn unique_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Unique::default())
}

pub fn unique_call(samples: Expr, count: Expr) -> Expr {
    unique_udf().call(vec![samples, count])
}

impl ScalarUDFImpl for Unique {
    fn name(&self) -> &str {
        UNIQUE_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        expect_samples(UNIQUE_NAME, arg_types.first(), "first")?;
        Ok(series::samples_type())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        non_null_field(UNIQUE_NAME, series::samples_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let counts = list_arg(&args, 1)?;
        let counts = counts.as_primitive::<Int64Type>();
        if (0..counts.len()).any(|row| !counts.is_null(row) && counts.value(row) > 1) {
            return Err(QueryError::raise(
                "vector cannot contain metrics with the same labelset",
            ));
        }
        let samples = list_arg(&args, 0)?;
        Ok(ColumnarValue::Array(samples))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_always_keeps_its_value() {
        assert_eq!(elem_binop(Op::Add, 2.0, 3.0), (5.0, true));
        assert_eq!(elem_binop(Op::Sub, 2.0, 3.0), (-1.0, true));
        assert_eq!(elem_binop(Op::Mul, 2.0, 3.0), (6.0, true));
        assert_eq!(elem_binop(Op::Div, 3.0, 2.0), (1.5, true));
    }

    /// `%` is Go's `math.Mod` and `^` is `math.Pow`, which are Rust's
    /// remainder operator and `powf` down to the sign of zero.
    #[test]
    fn modulo_and_power_follow_the_float_operators() {
        assert_eq!(elem_binop(Op::Mod, 5.0, 3.0), (5.0f64 % 3.0, true));
        assert_eq!(elem_binop(Op::Mod, -5.0, 3.0), (-5.0f64 % 3.0, true));
        assert_eq!(elem_binop(Op::Pow, 2.0, 10.0), (2.0f64.powf(10.0), true));
        assert_eq!(elem_binop(Op::Pow, 9.0, 0.5), (3.0, true));
    }

    /// A comparison yields the *left* value, kept only when it holds.
    #[test]
    fn a_comparison_yields_the_left_value_and_may_drop_it() {
        assert_eq!(elem_binop(Op::Gt, 5.0, 3.0), (5.0, true));
        assert_eq!(elem_binop(Op::Gt, 3.0, 5.0), (3.0, false));
        assert_eq!(elem_binop(Op::Eq, 3.0, 3.0), (3.0, true));
        assert_eq!(elem_binop(Op::Ne, 3.0, 3.0), (3.0, false));
    }

    #[test]
    fn bool_turns_a_comparison_into_one_or_zero_and_keeps_everything() {
        let (v, keep) = elem_binop(Op::Gt, 3.0, 5.0);
        assert_eq!(apply_bool(v, keep, true), (0.0, true));
        let (v, keep) = elem_binop(Op::Gt, 5.0, 3.0);
        assert_eq!(apply_bool(v, keep, true), (1.0, true));
    }

    #[test]
    fn nan_never_compares_equal() {
        let (_, keep) = elem_binop(Op::Eq, f64::NAN, f64::NAN);
        assert!(!keep);
        let (_, keep) = elem_binop(Op::Ne, f64::NAN, f64::NAN);
        assert!(keep);
    }
}
