//! The instant-vector selector as a DataFusion scalar function.
//!
//! `promql_instant_vector(samples, start, end, step, lookback, offset, at)`
//! takes one series' samples and returns that series' values on the step
//! grid: for every step, the most recent sample no older than `lookback`,
//! stamped with the step's timestamp. This is the operation that turns a
//! bare selector into what a range query returns, and the one every
//! PromQL engine has to get exactly right, boundaries included.
//!
//! It is a scalar function because a row is a series: the function sees
//! one series' slice, runs the sweep, and emits one list. DataFusion then
//! parallelizes over rows, pushes projections around it, and — because
//! the parameters are literal arguments rather than state on the function
//! — serializes the plan with no custom codec and never confuses two
//! selectors with different parameters for one.
//!
//! The semantics are `vectorSelectorSingle` in Prometheus's
//! `promql/engine.go`, and the tests below are the boundary cases that
//! function's `if`s encode.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, ListArray, StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Float64Type, TimestampMillisecondType,
};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::series;

pub const NAME: &str = "promql_instant_vector";

/// Prometheus's staleness marker: a NaN with this exact payload. It must
/// be compared by bits, since every NaN compares unequal to everything.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

pub fn is_stale(v: f64) -> bool {
    v.to_bits() == STALE_NAN_BITS
}

/// Everything the kernel needs besides the samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Params {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    pub lookback_ms: i64,
    /// The selector's offset. Positive looks into the past.
    pub offset_ms: i64,
    /// An `@` modifier, already resolved from `start()`/`end()`.
    pub at_ms: Option<i64>,
}

impl Params {
    /// The scan range a store has to be asked for so that every step can
    /// be answered: `getTimeRangesForSelector` in upstream. The `- 1` is
    /// the strict lower bound of the lookback window, so that a sample
    /// exactly `lookback` old is not even read.
    pub fn select_range(&self) -> (i64, i64) {
        let (lo, hi) = match self.at_ms {
            Some(at) => (at, at),
            None => (self.start_ms, self.end_ms),
        };
        (
            lo - (self.lookback_ms - 1) - self.offset_ms,
            hi - self.offset_ms,
        )
    }

    fn steps(&self) -> impl Iterator<Item = i64> {
        let (start, end, step) = (self.start_ms, self.end_ms, self.step_ms);
        (0..)
            .map(move |i| start + i * step)
            .take_while(move |ts| *ts <= end)
    }
}

/// The value the selector yields at one lookup time, if any.
///
/// The last sample at or before `ref_time`, provided it is younger than
/// `lookback`: `ref_time - lookback < t <= ref_time`, half-open. A
/// StaleNaN there means the series was marked stale, so nothing.
fn lookup(ts: &[i64], vs: &[f64], ref_time: i64, lookback_ms: i64) -> Option<f64> {
    // Index of the first sample after ref_time; the candidate is the one
    // before it.
    let i = ts.partition_point(|t| *t <= ref_time);
    if i == 0 {
        return None;
    }
    let (t, v) = (ts[i - 1], vs[i - 1]);
    if t <= ref_time - lookback_ms || is_stale(v) {
        return None;
    }
    Some(v)
}

/// Evaluate one series on the step grid.
///
/// `ts` and `vs` are one series' samples, sorted ascending. `emit` is
/// called with `(step_timestamp, value)` for every step that has a value,
/// in step order.
pub fn instant_vector(ts: &[i64], vs: &[f64], p: &Params, mut emit: impl FnMut(i64, f64)) {
    debug_assert_eq!(ts.len(), vs.len());
    if p.step_ms <= 0 || p.end_ms < p.start_ms {
        return;
    }

    // `@` pins the lookup time, so the answer is the same at every step:
    // one lookup, repeated across the grid at step timestamps.
    if let Some(at) = p.at_ms {
        if let Some(v) = lookup(ts, vs, at - p.offset_ms, p.lookback_ms) {
            for step in p.steps() {
                emit(step, v);
            }
        }
        return;
    }

    // Without `@`, the lookup time advances with the step, so `hi` — the
    // count of samples at or before it — only ever moves forward. One pass
    // over the series serves every step.
    let mut hi = 0usize;
    for step in p.steps() {
        let ref_time = step - p.offset_ms;
        while hi < ts.len() && ts[hi] <= ref_time {
            hi += 1;
        }
        if hi == 0 {
            continue;
        }
        let (t, v) = (ts[hi - 1], vs[hi - 1]);
        if t <= ref_time - p.lookback_ms || is_stale(v) {
            continue;
        }
        emit(step, v);
    }
}

/// The DataFusion function. Stateless: every parameter is an argument.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct InstantVector {
    signature: Signature,
}

impl Default for InstantVector {
    fn default() -> Self {
        Self {
            signature: Signature::any(7, Volatility::Immutable),
        }
    }
}

/// The registered function, ready to `call`.
pub fn udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(InstantVector::default())
}

/// Build the call expression for a samples column and parameters.
pub fn call(samples: Expr, p: &Params) -> Expr {
    use datafusion::logical_expr::lit;
    udf().call(vec![
        samples,
        lit(p.start_ms),
        lit(p.end_ms),
        lit(p.step_ms),
        lit(p.lookback_ms),
        lit(p.offset_ms),
        match p.at_ms {
            Some(at) => lit(at),
            None => lit(ScalarValue::Int64(None)),
        },
    ])
}

impl ScalarUDFImpl for InstantVector {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Same shape out as in. The check here is what makes a mistyped
    /// samples column a plan error rather than a downcast panic.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.first() != Some(&series::samples_type()) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::samples_type(),
                arg_types.first()
            );
        }
        for (i, t) in arg_types.iter().enumerate().skip(1) {
            if !matches!(t, DataType::Int64 | DataType::Null) {
                return plan_err!("{NAME}: argument {i} must be Int64, got {t}");
            }
        }
        Ok(series::samples_type())
    }

    /// Every row gets a list, possibly empty. DataFusion's default marks a
    /// function's output nullable, which would make the result no longer
    /// the canonical shape it came in as.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        Ok(Arc::new(Field::new(NAME, self.return_type(&types)?, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let samples: ArrayRef = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        let p = Params {
            start_ms: int_arg(&args, 1)?.ok_or_else(|| missing("start"))?,
            end_ms: int_arg(&args, 2)?.ok_or_else(|| missing("end"))?,
            step_ms: int_arg(&args, 3)?.ok_or_else(|| missing("step"))?,
            lookback_ms: int_arg(&args, 4)?.ok_or_else(|| missing("lookback"))?,
            offset_ms: int_arg(&args, 5)?.ok_or_else(|| missing("offset"))?,
            at_ms: int_arg(&args, 6)?,
        };
        Ok(ColumnarValue::Array(Arc::new(apply(
            samples.as_list::<i32>(),
            &p,
        ))))
    }
}

fn missing(what: &str) -> DataFusionError {
    DataFusionError::Execution(format!("{NAME}: {what} must not be NULL"))
}

/// Read literal argument `i` as an `Int64`, `None` for SQL NULL.
pub(crate) fn int_arg(args: &ScalarFunctionArgs, i: usize) -> Result<Option<i64>> {
    match &args.args[i] {
        ColumnarValue::Scalar(ScalarValue::Int64(v)) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "{NAME}: argument {i} must be an Int64 literal, got {:?}",
            other.data_type()
        ))),
    }
}

/// Run the kernel over every row of a samples column.
pub fn apply(samples: &ListArray, p: &Params) -> ListArray {
    let entries = samples.values().as_struct();
    let ts: &[i64] = entries
        .column_by_name(series::TIMESTAMP)
        .expect("validated by return_type")
        .as_primitive::<TimestampMillisecondType>()
        .values();
    let vs: &[f64] = entries
        .column_by_name(series::VALUE)
        .expect("validated by return_type")
        .as_primitive::<Float64Type>()
        .values();
    let offsets = samples.offsets();

    let mut out_ts: Vec<i64> = Vec::new();
    let mut out_vs: Vec<f64> = Vec::new();
    let mut out_offsets: Vec<i32> = Vec::with_capacity(samples.len() + 1);
    out_offsets.push(0);
    for row in 0..samples.len() {
        let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
        instant_vector(&ts[a..b], &vs[a..b], p, |t, v| {
            out_ts.push(t);
            out_vs.push(v);
        });
        out_offsets.push(out_ts.len() as i32);
    }

    let entries = StructArray::new(
        series::sample_fields(),
        vec![
            Arc::new(TimestampMillisecondArray::from(out_ts)),
            Arc::new(Float64Array::from(out_vs)),
        ],
        None,
    );
    ListArray::new(
        series::sample_item(),
        OffsetBuffer::new(out_offsets.into()),
        Arc::new(entries),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const M: i64 = 60_000;

    fn run(ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        let mut out = Vec::new();
        instant_vector(ts, vs, &p, |t, v| out.push((t, v)));
        out
    }

    fn params() -> Params {
        Params {
            start_ms: 0,
            end_ms: 10 * M,
            step_ms: M,
            lookback_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        }
    }

    /// One sample at t=0, steps every minute. Alive for steps 0..=4
    /// (ages 0m..4m) and gone at step 5 (age exactly 5m, the half-open
    /// boundary), never to return.
    #[test]
    fn the_lookback_window_is_half_open() {
        let out = run(&[0], &[7.0], params());
        assert_eq!(
            out,
            vec![(0, 7.0), (M, 7.0), (2 * M, 7.0), (3 * M, 7.0), (4 * M, 7.0)]
        );
    }

    #[test]
    fn samples_are_stamped_with_the_step_not_their_own_time() {
        // A sample 10s after the step boundary is seen from the next step.
        let out = run(
            &[10_000],
            &[1.0],
            Params {
                end_ms: M,
                ..params()
            },
        );
        assert_eq!(out, vec![(M, 1.0)]);
    }

    #[test]
    fn the_latest_sample_wins() {
        let out = run(
            &[0, 30_000, 90_000],
            &[1.0, 2.0, 3.0],
            Params {
                end_ms: 2 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(0, 1.0), (M, 2.0), (2 * M, 3.0)]);
    }

    #[test]
    fn a_stale_marker_hides_the_series_until_a_fresh_sample() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let out = run(
            &[0, M, 2 * M],
            &[1.0, stale, 3.0],
            Params {
                end_ms: 2 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(0, 1.0), (2 * M, 3.0)]);
        // An ordinary NaN is a value, not a marker.
        let out = run(
            &[0],
            &[f64::NAN],
            Params {
                end_ms: 0,
                ..params()
            },
        );
        assert_eq!(out.len(), 1);
        assert!(out[0].1.is_nan());
    }

    #[test]
    fn offset_shifts_the_lookup_not_the_emission() {
        // With offset 1m, step t looks at t-1m. The sample at 0 is seen
        // from step 1m onward and expires at step 6m.
        let out = run(
            &[0],
            &[1.0],
            Params {
                offset_ms: M,
                ..params()
            },
        );
        assert_eq!(
            out,
            vec![
                (M, 1.0),
                (2 * M, 1.0),
                (3 * M, 1.0),
                (4 * M, 1.0),
                (5 * M, 1.0)
            ]
        );
        // Negative offset looks into the future.
        let out = run(
            &[3 * M],
            &[1.0],
            Params {
                offset_ms: -M,
                end_ms: 3 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(2 * M, 1.0), (3 * M, 1.0)]);
    }

    #[test]
    fn at_pins_the_lookup_and_repeats_it_on_every_step() {
        let out = run(
            &[0, M, 2 * M],
            &[1.0, 2.0, 3.0],
            Params {
                at_ms: Some(M + 1),
                end_ms: 2 * M,
                ..params()
            },
        );
        assert_eq!(out, vec![(0, 2.0), (M, 2.0), (2 * M, 2.0)]);
        // `@` too far after the data: nothing at any step.
        let out = run(
            &[0],
            &[1.0],
            Params {
                at_ms: Some(10 * M),
                ..params()
            },
        );
        assert!(out.is_empty());
    }

    #[test]
    fn the_select_range_reflects_the_strict_lower_bound() {
        let p = Params {
            offset_ms: 30_000,
            ..params()
        };
        assert_eq!(p.select_range(), (-(5 * M) + 1 - 30_000, 10 * M - 30_000));
        let p = Params {
            at_ms: Some(M),
            ..params()
        };
        assert_eq!(p.select_range(), (M - 5 * M + 1, M));
    }

    #[test]
    fn apply_runs_the_kernel_row_by_row() {
        let names: Vec<String> = vec![];
        let mut b = series::SeriesBatchBuilder::new(&names);
        b.push(&Default::default(), &[(0, 1.0)]).unwrap();
        b.push(&Default::default(), &[]).unwrap();
        b.push(&Default::default(), &[(0, 1.0), (M, 2.0)]).unwrap();
        let batch = b.finish();
        let samples = batch
            .column_by_name(series::SAMPLES)
            .unwrap()
            .as_list::<i32>();
        let out = apply(
            samples,
            &Params {
                end_ms: M,
                ..params()
            },
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out.offsets().to_vec(), vec![0, 2, 2, 4]);
    }
}
