//! Instant-vector functions as one DataFusion scalar function:
//! `promql_elementwise(samples, 'abs', a, b)`.
//!
//! These are the functions that look at one sample at a time and change
//! nothing else: same series, same timestamps, a new value. Upstream
//! writes them through `simpleFloatFunc`, `clamp` and `funcRound`
//! (`promql/functions.go` at 83962c35), each mapping one `Sample` to one
//! `Sample` with `DropName: true`.
//!
//! So the kernel here rebuilds only the value array and hands back the
//! timestamps, offsets and validity it was given: the shape of the input
//! *is* the shape of the output, and a row is still a series.
//!
//! The scalar arguments — `round`'s `to_nearest`, `clamp`'s bounds —
//! arrive already folded to numbers. Upstream evaluates them per step,
//! which this cannot do while planning, so a scalar argument that is not
//! the same at every step is refused by name in [`crate::plan`] rather
//! than quietly taken at its first value.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, ListArray, StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    lit, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};

use crate::{date, series};

pub const NAME: &str = "promql_elementwise";

/// The instant-vector functions that map a value to a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Func {
    Abs,
    Ceil,
    Floor,
    Round,
    Exp,
    Ln,
    Log2,
    Log10,
    Sqrt,
    Sgn,
    Clamp,
    ClampMin,
    ClampMax,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    Deg,
    Rad,
    Year,
    Month,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
}

impl Func {
    pub fn parse(s: &str) -> Option<Func> {
        Some(match s {
            "abs" => Func::Abs,
            "ceil" => Func::Ceil,
            "floor" => Func::Floor,
            "round" => Func::Round,
            "exp" => Func::Exp,
            "ln" => Func::Ln,
            "log2" => Func::Log2,
            "log10" => Func::Log10,
            "sqrt" => Func::Sqrt,
            "sgn" => Func::Sgn,
            "clamp" => Func::Clamp,
            "clamp_min" => Func::ClampMin,
            "clamp_max" => Func::ClampMax,
            "sin" => Func::Sin,
            "cos" => Func::Cos,
            "tan" => Func::Tan,
            "asin" => Func::Asin,
            "acos" => Func::Acos,
            "atan" => Func::Atan,
            "sinh" => Func::Sinh,
            "cosh" => Func::Cosh,
            "tanh" => Func::Tanh,
            "asinh" => Func::Asinh,
            "acosh" => Func::Acosh,
            "atanh" => Func::Atanh,
            "deg" => Func::Deg,
            "rad" => Func::Rad,
            "year" => Func::Year,
            "month" => Func::Month,
            "day_of_month" => Func::DayOfMonth,
            "day_of_week" => Func::DayOfWeek,
            "day_of_year" => Func::DayOfYear,
            "days_in_month" => Func::DaysInMonth,
            "hour" => Func::Hour,
            "minute" => Func::Minute,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Func::Abs => "abs",
            Func::Ceil => "ceil",
            Func::Floor => "floor",
            Func::Round => "round",
            Func::Exp => "exp",
            Func::Ln => "ln",
            Func::Log2 => "log2",
            Func::Log10 => "log10",
            Func::Sqrt => "sqrt",
            Func::Sgn => "sgn",
            Func::Clamp => "clamp",
            Func::ClampMin => "clamp_min",
            Func::ClampMax => "clamp_max",
            Func::Sin => "sin",
            Func::Cos => "cos",
            Func::Tan => "tan",
            Func::Asin => "asin",
            Func::Acos => "acos",
            Func::Atan => "atan",
            Func::Sinh => "sinh",
            Func::Cosh => "cosh",
            Func::Tanh => "tanh",
            Func::Asinh => "asinh",
            Func::Acosh => "acosh",
            Func::Atanh => "atanh",
            Func::Deg => "deg",
            Func::Rad => "rad",
            Func::Year => "year",
            Func::Month => "month",
            Func::DayOfMonth => "day_of_month",
            Func::DayOfWeek => "day_of_week",
            Func::DayOfYear => "day_of_year",
            Func::DaysInMonth => "days_in_month",
            Func::Hour => "hour",
            Func::Minute => "minute",
        }
    }

    /// Whether the function reads a timestamp rather than a
    /// measurement: upstream's `dateWrapper` family, which may be
    /// called with no vector at all and then answers for the step.
    pub fn is_date(&self) -> bool {
        matches!(
            self,
            Func::Year
                | Func::Month
                | Func::DayOfMonth
                | Func::DayOfWeek
                | Func::DayOfYear
                | Func::DaysInMonth
                | Func::Hour
                | Func::Minute
        )
    }

    /// Every one of these sets `DropName: true` on the sample it emits,
    /// so none of them keeps `__name__`.
    pub fn drops_metric_name(&self) -> bool {
        true
    }

    /// The function with its scalar arguments folded in, ready to run
    /// over values. `a` and `b` are the call's arguments after the
    /// vector, absent where the call did not give them.
    pub fn bind(self, a: Option<f64>, b: Option<f64>) -> Bound {
        match self {
            // `round(v)` is `round(v, 1)`: the default is in upstream's
            // signature, not in a branch.
            Func::Round => Bound::Round(a.unwrap_or(1.0)),
            Func::Clamp => Bound::clamp(a.unwrap_or(f64::NAN), b.unwrap_or(f64::NAN)),
            Func::ClampMin => Bound::clamp(a.unwrap_or(f64::NAN), f64::INFINITY),
            Func::ClampMax => Bound::clamp(f64::NEG_INFINITY, a.unwrap_or(f64::NAN)),
            other => Bound::Map(other.map_fn()),
        }
    }

    /// The `func(float64) float64` upstream hands `simpleFloatFunc`.
    fn map_fn(self) -> fn(f64) -> f64 {
        match self {
            Func::Abs => f64::abs,
            Func::Ceil => f64::ceil,
            Func::Floor => f64::floor,
            Func::Exp => f64::exp,
            Func::Ln => f64::ln,
            Func::Log2 => f64::log2,
            Func::Log10 => f64::log10,
            Func::Sqrt => f64::sqrt,
            Func::Sgn => sgn,
            Func::Sin => f64::sin,
            Func::Cos => f64::cos,
            Func::Tan => f64::tan,
            Func::Asin => f64::asin,
            Func::Acos => f64::acos,
            Func::Atan => f64::atan,
            Func::Sinh => f64::sinh,
            Func::Cosh => f64::cosh,
            Func::Tanh => f64::tanh,
            Func::Asinh => f64::asinh,
            Func::Acosh => f64::acosh,
            Func::Atanh => f64::atanh,
            Func::Deg => |v| v * 180.0 / std::f64::consts::PI,
            Func::Rad => |v| v * std::f64::consts::PI / 180.0,
            Func::Year => |v| date_field(v, |c| c.year),
            Func::Month => |v| date_field(v, |c| c.month),
            Func::DayOfMonth => |v| date_field(v, |c| c.day),
            Func::DayOfWeek => |v| date_field(v, |c| c.weekday),
            Func::DayOfYear => |v| date_field(v, |c| c.yearday),
            Func::DaysInMonth => |v| date_field(v, |c| date::days_in_month(c.year, c.month)),
            Func::Hour => |v| date_field(v, |c| c.hour),
            Func::Minute => |v| date_field(v, |c| c.minute),
            Func::Round | Func::Clamp | Func::ClampMin | Func::ClampMax => {
                unreachable!("{} takes its arguments through bind", self.as_str())
            }
        }
    }
}

/// One field of the UTC calendar, from a sample holding a Unix time in
/// seconds.
///
/// Upstream truncates the float to an `int64` and hands it to
/// `time.Unix` (`dateWrapper`, `promql/functions.go:2093` at 83962c35).
/// Go leaves that conversion undefined for a NaN or a value past the
/// integer range and picks whatever the machine does; Rust's `as`
/// defines it, so the answers here are the same everywhere even where
/// upstream's are not: a NaN becomes 0, the epoch, and an infinity
/// becomes the end of the `i64` range.
pub fn date_field(secs: f64, field: fn(&date::Civil) -> i64) -> f64 {
    let civil = date::civil(secs as i64);
    field(&civil) as f64
}

/// `funcSgn`: zero and NaN come back as themselves, so the sign of `-0`
/// survives and a NaN does not become a 1.
fn sgn(v: f64) -> f64 {
    if v < 0.0 {
        -1.0
    } else if v > 0.0 {
        1.0
    } else {
        v
    }
}

/// A function with its arguments applied, one value in, one out.
///
/// Not `PartialEq`: a variant holds a function pointer, and comparing
/// those compares addresses the linker is free to merge or duplicate.
#[derive(Debug, Clone, Copy)]
pub enum Bound {
    Map(fn(f64) -> f64),
    Round(f64),
    Clamp {
        min: f64,
        max: f64,
    },
    /// `clamp` with `max < min` drops every sample it is given
    /// (`clamp`, `promql/functions.go:705-707` at 83962c35), so the
    /// answer is the empty vector however many series came in.
    Nothing,
}

impl Bound {
    fn clamp(min: f64, max: f64) -> Bound {
        if max < min {
            Bound::Nothing
        } else {
            Bound::Clamp { min, max }
        }
    }

    pub fn value(&self, v: f64) -> f64 {
        match self {
            Bound::Map(f) => f(v),
            // Inverted as upstream inverts it: dividing by `to_nearest`
            // and multiplying back is a different float from multiplying
            // by its reciprocal, and upstream took the reciprocal.
            Bound::Round(to_nearest) => {
                let inverse = 1.0 / to_nearest;
                (v * inverse + 0.5).floor() / inverse
            }
            Bound::Clamp { min, max } => go_max(*min, go_min(*max, v)),
            Bound::Nothing => unreachable!("an empty result has no values"),
        }
    }
}

/// Go's `math.Min` and `math.Max`, which answer NaN when either operand
/// is one. Rust's `f64::min`/`f64::max` return the *other* operand
/// instead, which would make `clamp` turn a NaN sample into a bound.
fn go_min(x: f64, y: f64) -> f64 {
    if x.is_nan() || y.is_nan() {
        f64::NAN
    } else {
        x.min(y)
    }
}

fn go_max(x: f64, y: f64) -> f64 {
    if x.is_nan() || y.is_nan() {
        f64::NAN
    } else {
        x.max(y)
    }
}

/// The DataFusion function. Stateless: the operator and its arguments
/// are literals, as in [`crate::selector`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Elementwise {
    signature: Signature,
}

impl Default for Elementwise {
    fn default() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

pub fn udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Elementwise::default())
}

/// `promql_elementwise(samples, 'abs', a, b)`. The two scalar arguments
/// are always present in the plan, NULL where the call had none, so one
/// signature serves every function.
pub fn call(samples: Expr, func: Func, a: Option<f64>, b: Option<f64>) -> Expr {
    udf().call(vec![
        samples,
        lit(func.as_str()),
        lit(ScalarValue::Float64(a)),
        lit(ScalarValue::Float64(b)),
    ])
}

impl ScalarUDFImpl for Elementwise {
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
        if !matches!(arg_types.get(1), Some(DataType::Utf8)) {
            return plan_err!("{NAME}: the function name must be a string");
        }
        for (i, t) in arg_types.iter().enumerate().skip(2) {
            if !matches!(t, DataType::Float64 | DataType::Null) {
                return plan_err!("{NAME}: argument {i} must be Float64, got {t}");
            }
        }
        Ok(series::samples_type())
    }

    /// As in [`crate::selector`]: a null row maps to a null row, and
    /// anything else would leave the canonical shape.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let data_type = self.return_type(&types)?;
        let nullable = args.arg_fields[0].is_nullable();
        Ok(Arc::new(Field::new(NAME, data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let samples: ArrayRef = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        let name = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "{NAME}: the function name must be a string literal, got {:?}",
                    other.data_type()
                )))
            }
        };
        let func = Func::parse(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("{NAME}: unknown function {name}"))
        })?;
        let bound = func.bind(float_arg(&args, 2)?, float_arg(&args, 3)?);
        Ok(ColumnarValue::Array(Arc::new(apply(
            samples.as_list::<i32>(),
            bound,
        ))))
    }
}

/// Read literal argument `i` as a `Float64`, `None` for SQL NULL.
fn float_arg(args: &ScalarFunctionArgs, i: usize) -> Result<Option<f64>> {
    match &args.args[i] {
        ColumnarValue::Scalar(ScalarValue::Float64(v)) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "{NAME}: argument {i} must be a Float64 literal, got {:?}",
            other.data_type()
        ))),
    }
}

/// Run `bound` over every value of a samples column.
///
/// The timestamps and the row boundaries come back untouched: an
/// elementwise function moves no sample between series and drops none,
/// so only the values are rebuilt.
pub fn apply(samples: &ListArray, bound: Bound) -> ListArray {
    let entries = samples.values().as_struct();
    let timestamps = entries
        .column_by_name(series::TIMESTAMP)
        .expect("validated by return_type");
    let values = entries
        .column_by_name(series::VALUE)
        .expect("validated by return_type")
        .as_primitive::<Float64Type>();

    if matches!(bound, Bound::Nothing) {
        return empty_rows(samples.len(), samples.nulls().cloned());
    }

    let mapped: Float64Array = values.values().iter().map(|v| bound.value(*v)).collect();
    let entries = StructArray::new(
        series::sample_fields(),
        vec![Arc::clone(timestamps), Arc::new(mapped)],
        None,
    );
    ListArray::new(
        series::sample_item(),
        samples.offsets().clone(),
        Arc::new(entries),
        samples.nulls().cloned(),
    )
}

/// `len` rows, every one of them an empty series.
fn empty_rows(len: usize, nulls: Option<datafusion::arrow::buffer::NullBuffer>) -> ListArray {
    let entries = StructArray::new(
        series::sample_fields(),
        vec![
            Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new())),
            Arc::new(Float64Array::from(Vec::<f64>::new())),
        ],
        None,
    );
    ListArray::new(
        series::sample_item(),
        OffsetBuffer::new(vec![0i32; len + 1].into()),
        Arc::new(entries),
        nulls,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(name: &str, v: f64) -> f64 {
        Func::parse(name).expect(name).bind(None, None).value(v)
    }

    #[test]
    fn the_simple_functions_are_gos_math() {
        assert_eq!(value("abs", -1.5), 1.5);
        assert_eq!(value("ceil", 1.2), 2.0);
        assert_eq!(value("floor", 1.8), 1.0);
        assert_eq!(value("exp", 0.0), 1.0);
        assert_eq!(value("ln", 1.0), 0.0);
        assert_eq!(value("log2", 8.0), 3.0);
        assert_eq!(value("log10", 1000.0), 3.0);
        assert_eq!(value("sqrt", 9.0), 3.0);
        assert_eq!(value("deg", std::f64::consts::PI), 180.0);
        assert_eq!(value("rad", 180.0), std::f64::consts::PI);
        assert_eq!(value("sin", 0.0), 0.0);
        assert_eq!(value("cos", 0.0), 1.0);

        // The edges Go's math is specified at, and Rust's agrees.
        assert_eq!(value("ln", 0.0), f64::NEG_INFINITY);
        assert!(value("ln", -1.0).is_nan());
        assert!(value("sqrt", -1.0).is_nan());
        assert!(value("asin", 2.0).is_nan());
        assert_eq!(value("exp", f64::INFINITY), f64::INFINITY);
        assert_eq!(value("exp", f64::NEG_INFINITY), 0.0);
    }

    /// `funcSgn` returns the value itself for zero and NaN, so `-0`
    /// stays negative and a NaN never becomes a number.
    #[test]
    fn sgn_keeps_the_values_that_have_no_sign() {
        assert_eq!(value("sgn", -7.0), -1.0);
        assert_eq!(value("sgn", 7.0), 1.0);
        assert_eq!(value("sgn", 0.0).to_bits(), 0.0f64.to_bits());
        assert_eq!(value("sgn", -0.0).to_bits(), (-0.0f64).to_bits());
        assert!(value("sgn", f64::NAN).is_nan());
    }

    /// A NaN in, a NaN out, for every function that has nothing else to
    /// say about one.
    #[test]
    fn a_nan_propagates() {
        for name in [
            "abs", "ceil", "floor", "exp", "ln", "log2", "log10", "sqrt", "sin", "cos", "tan",
            "asin", "acos", "atan", "sinh", "cosh", "tanh", "asinh", "acosh", "atanh", "deg",
            "rad",
        ] {
            assert!(value(name, f64::NAN).is_nan(), "{name}");
        }
    }

    /// `funcRound` ties round up — away from zero for a positive value
    /// and towards zero for a negative one, which is what
    /// `floor(v + 0.5)` does and what `math.Round` does not.
    #[test]
    fn round_settles_ties_upwards() {
        let round = |v: f64, to: Option<f64>| Func::Round.bind(to, None).value(v);
        assert_eq!(round(2.5, None), 3.0);
        assert_eq!(round(-2.5, None), -2.0);
        assert_eq!(round(1.4, None), 1.0);
        assert_eq!(round(-1.4, None), -1.0);
        // to_nearest is a step, not a digit count.
        assert_eq!(round(2.6, Some(5.0)), 5.0);
        assert_eq!(round(2.4, Some(5.0)), 0.0);
        assert_eq!(round(1.234, Some(0.1)), 1.2);
        // A to_nearest of zero is an infinite inverse: upstream answers
        // NaN rather than refusing the call.
        assert!(round(1.0, Some(0.0)).is_nan());
    }

    #[test]
    fn clamp_is_max_of_min_and_bounds_that_cross_give_nothing() {
        let clamp = |v: f64, min, max| Func::Clamp.bind(Some(min), Some(max)).value(v);
        assert_eq!(clamp(5.0, 0.0, 1.0), 1.0);
        assert_eq!(clamp(-5.0, 0.0, 1.0), 0.0);
        assert_eq!(clamp(0.5, 0.0, 1.0), 0.5);
        assert!(matches!(
            Func::Clamp.bind(Some(1.0), Some(0.0)),
            Bound::Nothing
        ));
        // Equal bounds still clamp; only max < min is empty.
        assert_eq!(clamp(5.0, 1.0, 1.0), 1.0);
        // Go's math.Min and math.Max propagate a NaN; Rust's return the
        // other operand, which would clamp a NaN to a bound.
        assert!(clamp(f64::NAN, 0.0, 1.0).is_nan());

        assert_eq!(Func::ClampMin.bind(Some(3.0), None).value(1.0), 3.0);
        assert_eq!(Func::ClampMin.bind(Some(3.0), None).value(9.0), 9.0);
        assert_eq!(Func::ClampMax.bind(Some(3.0), None).value(9.0), 3.0);
        assert_eq!(Func::ClampMax.bind(Some(3.0), None).value(1.0), 1.0);
        // `min.max(max.min(v))` with a NaN bound answers as Go's
        // math.Max(min, math.Min(max, v)) does.
        assert!(Func::ClampMin
            .bind(Some(f64::NAN), None)
            .value(1.0)
            .is_nan());
    }

    /// The two samples Go's `int64(v)` leaves to the machine. Rust
    /// defines both, so these answers are the engine's everywhere:
    /// a NaN is zero and reads as the epoch, and an infinity saturates
    /// instead of wrapping into some other year.
    #[test]
    fn a_date_function_answers_for_a_nan_and_an_infinity() {
        let year = |v| date_field(v, |c| c.year);
        assert_eq!(year(f64::NAN), 1970.0);
        assert_eq!(date_field(f64::NAN, |c| c.month), 1.0);
        assert_eq!(date_field(f64::NAN, |c| c.day), 1.0);
        assert_eq!(date_field(f64::NAN, |c| c.hour), 0.0);

        assert_eq!(year(f64::INFINITY), date::civil(i64::MAX).year as f64);
        assert_eq!(year(f64::NEG_INFINITY), date::civil(i64::MIN).year as f64);
        assert!(year(f64::INFINITY) > 1970.0 && year(f64::NEG_INFINITY) < 0.0);
    }

    #[test]
    fn every_name_round_trips() {
        for func in [
            Func::Abs,
            Func::Ceil,
            Func::Floor,
            Func::Round,
            Func::Exp,
            Func::Ln,
            Func::Log2,
            Func::Log10,
            Func::Sqrt,
            Func::Sgn,
            Func::Clamp,
            Func::ClampMin,
            Func::ClampMax,
            Func::Sin,
            Func::Cos,
            Func::Tan,
            Func::Asin,
            Func::Acos,
            Func::Atan,
            Func::Sinh,
            Func::Cosh,
            Func::Tanh,
            Func::Asinh,
            Func::Acosh,
            Func::Atanh,
            Func::Deg,
            Func::Rad,
        ] {
            assert_eq!(Func::parse(func.as_str()), Some(func));
            assert!(crate::function::signature(func.as_str()).is_some());
        }
        assert_eq!(Func::parse("rate"), None);
    }

    fn samples() -> ListArray {
        let batch = series::encode(
            &["s".to_string()],
            &[
                series::Series::new(&[("s", "a")], vec![0, 1], vec![-1.0, 2.5]).unwrap(),
                series::Series::new(&[("s", "b")], vec![], vec![]).unwrap(),
                series::Series::new(&[("s", "c")], vec![7], vec![-9.0]).unwrap(),
            ],
        )
        .unwrap();
        batch
            .column_by_name(series::SAMPLES)
            .unwrap()
            .as_list::<i32>()
            .clone()
    }

    fn values_of(list: &ListArray) -> Vec<f64> {
        list.values()
            .as_struct()
            .column_by_name(series::VALUE)
            .unwrap()
            .as_primitive::<Float64Type>()
            .values()
            .to_vec()
    }

    #[test]
    fn apply_keeps_the_rows_and_the_timestamps() {
        let input = samples();
        let out = apply(&input, Func::Abs.bind(None, None));
        assert_eq!(out.len(), 3);
        assert_eq!(out.offsets(), input.offsets());
        assert_eq!(values_of(&out), [1.0, 2.5, 9.0]);
        let ts = out
            .values()
            .as_struct()
            .column_by_name(series::TIMESTAMP)
            .unwrap();
        assert_eq!(
            ts.as_ref(),
            input
                .values()
                .as_struct()
                .column_by_name(series::TIMESTAMP)
                .unwrap()
                .as_ref()
        );
    }

    #[test]
    fn a_clamp_that_cannot_be_satisfied_empties_every_row() {
        let out = apply(&samples(), Func::Clamp.bind(Some(1.0), Some(0.0)));
        assert_eq!(out.len(), 3);
        assert_eq!(out.offsets().to_vec(), vec![0, 0, 0, 0]);
        assert!(values_of(&out).is_empty());
    }
}
