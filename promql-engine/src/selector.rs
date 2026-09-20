//! The vector selector as a DataFusion scalar function.
//!
//! `promql_vector_selector(samples, start, end, step, lookback, offset,
//! at, mode, extend_after)` takes one series' samples and returns that
//! series' values on the step grid: for every step, the most recent
//! sample no older than `lookback`, stamped with the step's timestamp.
//! Under `smoothed` it interpolates that value instead, which is why it
//! also reads `extend_after` milliseconds past the step.
//!
//! A scalar function, because a row is a series: DataFusion parallelizes
//! over rows and pushes projections around it. Keeping the parameters as
//! literal arguments rather than state on the function means the plan
//! serializes with no custom codec and two selectors with different
//! parameters are never taken for one.
//!
//! The names follow Prometheus's `promql/engine.go`, where a
//! `VectorSelector` expands the series set and runs `evalSeries` over
//! `vectorSelectorSingle`: the expansion is [`crate::source::SelectorTable`],
//! [`eval_series`] is one series' walk across the grid, and [`apply`] runs
//! it over a batch. The tests below are the boundary cases
//! `vectorSelectorSingle`'s `if`s encode.

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

use crate::params::{Params, RangeMode};
use crate::series;

pub const NAME: &str = "promql_vector_selector";

/// Prometheus's staleness marker: a NaN with this exact payload. It must
/// be compared by bits, since every NaN compares unequal to everything.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

pub fn is_stale(v: f64) -> bool {
    v.to_bits() == STALE_NAN_BITS
}

/// The value the selector yields at one lookup time, if any.
///
/// The last sample at or before `ref_time`, provided it is younger than
/// `lookback`: `ref_time - lookback < t <= ref_time`, half-open. A
/// StaleNaN there means the series was marked stale, so nothing.
fn lookup(ts: &[i64], vs: &[f64], ref_time: i64, window_ms: i64) -> Option<f64> {
    // Index of the first sample after ref_time; the candidate is the one
    // before it.
    let i = ts.partition_point(|t| *t <= ref_time);
    if i == 0 {
        return None;
    }
    let (t, v) = (ts[i - 1], vs[i - 1]);
    if t <= ref_time - window_ms || is_stale(v) {
        return None;
    }
    Some(v)
}

/// `smoothSeries` for one step: the value the series is taken to have at
/// `data_ts`, read from the samples `ts[lo..hi]`, which the caller has
/// already narrowed to `(data_ts - lb, data_ts + lb]`.
///
/// Staleness markers are not samples, so they are stepped over on both
/// sides rather than interpolated through; upstream gets this for free
/// because `matrixIterSlice` has already dropped them.
fn smoothed(ts: &[i64], vs: &[f64], lo: usize, hi: usize, data_ts: i64) -> Option<f64> {
    let i = lo + ts[lo..hi].partition_point(|t| *t < data_ts);
    if i < hi && ts[i] == data_ts && !is_stale(vs[i]) {
        return Some(vs[i]);
    }
    let next = (i..hi).find(|&j| ts[j] > data_ts && !is_stale(vs[j]));
    let prev = (lo..i).rev().find(|&j| !is_stale(vs[j]));
    match (prev, next) {
        (Some(a), Some(b)) => Some(crate::range::interpolate(
            (ts[a], vs[a]),
            (ts[b], vs[b]),
            data_ts,
            // A smoothed selector has no window to detect a reset in, so
            // upstream interpolates through one until sample metadata
            // says the series is a counter.
            false,
        )),
        // Nothing after it yet: the last value carries forward.
        (Some(a), None) => Some(vs[a]),
        // Nothing before it: the series has not started.
        _ => None,
    }
}

/// Every step's [`smoothed`] value, sweeping the two window edges
/// forward with `data_ts` so the series is read once.
fn eval_smoothed(ts: &[i64], vs: &[f64], p: &Params, emit: &mut impl FnMut(i64, f64)) {
    let lb = p.window_ms;
    let mut at_step = |step: i64, data_ts: i64, lo: &mut usize, hi: &mut usize| {
        while *lo < ts.len() && ts[*lo] <= data_ts - lb {
            *lo += 1;
        }
        *hi = (*hi).max(*lo);
        while *hi < ts.len() && ts[*hi] <= data_ts + lb {
            *hi += 1;
        }
        if let Some(v) = smoothed(ts, vs, *lo, *hi, data_ts) {
            emit(step, v);
        }
    };
    let (mut lo, mut hi) = (0usize, 0usize);
    if let Some(at) = p.at_ms {
        at_step(at, at - p.offset_ms, &mut lo, &mut hi);
        return;
    }
    for step in p.steps() {
        at_step(step, step - p.offset_ms, &mut lo, &mut hi);
    }
}

/// Evaluate one series on the step grid: the body of upstream's
/// `evalSeries` loop for one series, with `vectorSelectorSingle` inlined
/// as a single forward sweep.
///
/// `ts` and `vs` are one series' samples, sorted ascending. `emit` is
/// called with `(step_timestamp, value)` for every step that has a value,
/// in step order.
pub fn eval_series(ts: &[i64], vs: &[f64], p: &Params, mut emit: impl FnMut(i64, f64)) {
    debug_assert_eq!(ts.len(), vs.len());
    if p.step_ms <= 0 || p.end_ms < p.start_ms {
        return;
    }

    if p.mode == RangeMode::Smoothed {
        // `@` still pins one lookup, but the value is then repeated over
        // the grid, so the pinned case collects it first.
        if p.at_ms.is_some() {
            let mut pinned = None;
            eval_smoothed(ts, vs, p, &mut |_, v| pinned = Some(v));
            if let Some(v) = pinned {
                for step in p.steps() {
                    emit(step, v);
                }
            }
        } else {
            eval_smoothed(ts, vs, p, &mut emit);
        }
        return;
    }

    // `@` pins the lookup time, so the answer is the same at every step:
    // one lookup, repeated across the grid at step timestamps.
    if let Some(at) = p.at_ms {
        if let Some(v) = lookup(ts, vs, at - p.offset_ms, p.window_ms) {
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
        if t <= ref_time - p.window_ms || is_stale(v) {
            continue;
        }
        emit(step, v);
    }
}

/// The DataFusion function. Stateless: every parameter is an argument.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorSelector {
    signature: Signature,
}

impl Default for VectorSelector {
    fn default() -> Self {
        Self {
            signature: Signature::any(9, Volatility::Immutable),
        }
    }
}

pub fn udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(VectorSelector::default())
}

/// `promql_vector_selector(samples, start, end, step, lookback, offset,
/// at, '<mode>', extend_after)`.
pub fn call(samples: Expr, p: &Params) -> Expr {
    use datafusion::logical_expr::lit;
    udf().call(vec![
        samples,
        lit(p.start_ms),
        lit(p.end_ms),
        lit(p.step_ms),
        lit(p.window_ms),
        lit(p.offset_ms),
        match p.at_ms {
            Some(at) => lit(at),
            None => lit(ScalarValue::Int64(None)),
        },
        lit(p.mode.as_str()),
        // An instant selector never reaches back beyond its own
        // lookback, so only the forward half crosses the plan.
        lit(p.extension.1),
    ])
}

impl ScalarUDFImpl for VectorSelector {
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
        if arg_types.get(7) != Some(&DataType::Utf8) {
            return plan_err!("{NAME}: argument 7 must be the selector mode as Utf8");
        }
        for (i, t) in arg_types.iter().enumerate().skip(1) {
            if i == 7 {
                continue;
            }
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
        let data_type = self.return_type(&types)?;
        // A null row maps to a null row, so nullability mirrors the input.
        let nullable = args.arg_fields[0].is_nullable();
        Ok(Arc::new(Field::new(NAME, data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let samples: ArrayRef = match &args.args[0] {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        let p = Params {
            start_ms: int_arg(&args, 1, NAME)?.ok_or_else(|| missing("start"))?,
            end_ms: int_arg(&args, 2, NAME)?.ok_or_else(|| missing("end"))?,
            step_ms: int_arg(&args, 3, NAME)?.ok_or_else(|| missing("step"))?,
            window_ms: int_arg(&args, 4, NAME)?.ok_or_else(|| missing("lookback"))?,
            offset_ms: int_arg(&args, 5, NAME)?.ok_or_else(|| missing("offset"))?,
            at_ms: int_arg(&args, 6, NAME)?,
            mode: match &args.args[7] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) => RangeMode::parse(name)
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!("{NAME}: unknown selector mode {name}"))
                    })?,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "{NAME}: selector mode must be a string literal, got {:?}",
                        other.data_type()
                    )))
                }
            },
            extension: (
                0,
                int_arg(&args, 8, NAME)?.ok_or_else(|| missing("extension"))?,
            ),
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
///
/// The range kernel shares this helper, so `caller` names the function
/// the query called rather than this module.
pub(crate) fn int_arg(args: &ScalarFunctionArgs, i: usize, caller: &str) -> Result<Option<i64>> {
    match &args.args[i] {
        ColumnarValue::Scalar(ScalarValue::Int64(v)) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "{caller}: argument {i} must be an Int64 literal, got {:?}",
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

    // A starting size, not a ceiling: one sample can serve many steps.
    let mut out_ts: Vec<i64> = Vec::with_capacity(ts.len());
    let mut out_vs: Vec<f64> = Vec::with_capacity(ts.len());
    let mut out_offsets: Vec<i32> = Vec::with_capacity(samples.len() + 1);
    out_offsets.push(0);
    for row in 0..samples.len() {
        // A null row is an absent series, not an empty one, and its
        // offsets may still span samples.
        if !samples.is_null(row) {
            let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
            eval_series(&ts[a..b], &vs[a..b], p, |t, v| {
                out_ts.push(t);
                out_vs.push(v);
            });
        }
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
        // One output row per input row, in order, so the input's
        // validity is the output's.
        samples.nulls().cloned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::buffer::NullBuffer;

    const M: i64 = 60_000;

    fn run(ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        let mut out = Vec::new();
        eval_series(ts, vs, &p, |t, v| out.push((t, v)));
        out
    }

    fn params() -> Params {
        Params {
            start_ms: 0,
            end_ms: 10 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
            mode: RangeMode::Plain,
            extension: (0, 0),
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
        // Three series, so three distinct label sets.
        let batch = series::encode(
            &["s".to_string()],
            &[
                series::Series::new(&[("s", "a")], vec![0], vec![1.0]).unwrap(),
                series::Series::new(&[("s", "b")], vec![], vec![]).unwrap(),
                series::Series::new(&[("s", "c")], vec![0, M], vec![1.0, 2.0]).unwrap(),
            ],
        )
        .unwrap();
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

    /// Three one-sample rows; the middle is null but still spans a sample.
    fn with_a_null_row() -> ListArray {
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, 0, 0])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
            None,
        );
        ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            Arc::new(entries),
            Some(NullBuffer::from(vec![true, false, true])),
        )
    }

    #[test]
    fn a_null_row_yields_a_null_row() {
        let out = apply(
            &with_a_null_row(),
            &Params {
                end_ms: 0,
                ..params()
            },
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out.null_count(), 1);
        assert!(!out.is_null(0) && out.is_null(1) && !out.is_null(2));
        // The sample the null row spans stays out of the output.
        assert_eq!(out.offsets().to_vec(), vec![0, 1, 1, 2]);
        let vs = out
            .values()
            .as_struct()
            .column(1)
            .as_primitive::<Float64Type>();
        assert_eq!(vs.values(), &[1.0, 3.0]);
    }

    /// `smoothSeries`: the value at a step between two samples is the
    /// straight line between them, the value before the first sample is
    /// nothing, and the value after the last one carries forward.
    #[test]
    fn a_smoothed_selector_interpolates_between_the_samples() {
        let p = Params {
            start_ms: 0,
            end_ms: 4 * M,
            step_ms: 30_000,
            window_ms: M,
            offset_ms: 0,
            at_ms: None,
            mode: RangeMode::Smoothed,
            extension: (0, M),
        };
        let out = run(&[M, 2 * M], &[10.0, 20.0], p);
        assert_eq!(
            out,
            vec![
                // Steps 0 and 30s see only samples ahead of them, and
                // the line has no left end there.
                (M, 10.0),
                (90_000, 15.0),
                (2 * M, 20.0),
                // 150s still has the last sample in its window; from
                // 180s on the window is empty again.
                (150_000, 20.0),
            ]
        );
    }

    /// A staleness marker is not a sample, so it is neither returned nor
    /// interpolated through.
    #[test]
    fn a_smoothed_selector_steps_over_a_staleness_marker() {
        let p = Params {
            start_ms: 2 * M,
            end_ms: 2 * M,
            step_ms: M,
            window_ms: 2 * M,
            offset_ms: 0,
            at_ms: None,
            mode: RangeMode::Smoothed,
            extension: (0, 2 * M),
        };
        let stale = f64::from_bits(STALE_NAN_BITS);
        let out = run(&[M, 2 * M, 3 * M], &[10.0, stale, 30.0], p);
        assert_eq!(out, vec![(2 * M, 20.0)]);
    }
}
