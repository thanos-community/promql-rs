//! Range-vector functions as one DataFusion scalar function:
//! `promql_range_function(samples, 'rate', start, end, step, range, offset, at)`.
//!
//! `rate(x[5m])` is, per series and per step, a function of the samples
//! in a window ending at the step: the same shape as the instant vector,
//! with a window instead of a lookback and real arithmetic instead of
//! "the last one". So it is the same kind of function: one series' slice
//! in, that series' values on the step grid out, DataFusion parallel over
//! rows. The function name is a literal argument, like every parameter,
//! so one registration serves all of them and the plan says which it is.
//!
//! Semantics are `matrixIterSlice`, `extrapolatedRate`, `instantValue`
//! and the `*_over_time` functions in Prometheus's `promql/functions.go`
//! and `engine.go`. Floats only; native histograms are a later slice.

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
    lit, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};

use crate::instant::{int_arg, is_stale};
use crate::math;
use crate::series;

pub const NAME: &str = "promql_range_function";

/// The functions this kernel implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Func {
    Rate,
    Increase,
    Delta,
    Irate,
    Idelta,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    CountOverTime,
    LastOverTime,
    PresentOverTime,
    Changes,
    Resets,
}

impl Func {
    pub fn parse(s: &str) -> Option<Func> {
        Some(match s {
            "rate" => Func::Rate,
            "increase" => Func::Increase,
            "delta" => Func::Delta,
            "irate" => Func::Irate,
            "idelta" => Func::Idelta,
            "sum_over_time" => Func::SumOverTime,
            "avg_over_time" => Func::AvgOverTime,
            "min_over_time" => Func::MinOverTime,
            "max_over_time" => Func::MaxOverTime,
            "count_over_time" => Func::CountOverTime,
            "last_over_time" => Func::LastOverTime,
            "present_over_time" => Func::PresentOverTime,
            "changes" => Func::Changes,
            "resets" => Func::Resets,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Func::Rate => "rate",
            Func::Increase => "increase",
            Func::Delta => "delta",
            Func::Irate => "irate",
            Func::Idelta => "idelta",
            Func::SumOverTime => "sum_over_time",
            Func::AvgOverTime => "avg_over_time",
            Func::MinOverTime => "min_over_time",
            Func::MaxOverTime => "max_over_time",
            Func::CountOverTime => "count_over_time",
            Func::LastOverTime => "last_over_time",
            Func::PresentOverTime => "present_over_time",
            Func::Changes => "changes",
            Func::Resets => "resets",
        }
    }

    /// Every range function drops `__name__` except `last_over_time`,
    /// which "acts like offset" (`dropName` in `evalCall`).
    pub fn drops_metric_name(&self) -> bool {
        !matches!(self, Func::LastOverTime)
    }
}

/// Everything the kernel needs besides the samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Params {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    /// The `[5m]`.
    pub range_ms: i64,
    pub offset_ms: i64,
    pub at_ms: Option<i64>,
}

impl Params {
    /// The scan range: `getTimeRangesForSelector` with `evalRange != 0`.
    /// The `- 1` is the strict lower bound of the window.
    pub fn select_range(&self) -> (i64, i64) {
        let (lo, hi) = match self.at_ms {
            Some(at) => (at, at),
            None => (self.start_ms, self.end_ms),
        };
        (
            lo - (self.range_ms - 1) - self.offset_ms,
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

/// One step's window: the samples with `range_start < t <= range_end`,
/// StaleNaN already removed, plus the bounds the extrapolation needs.
pub struct Window<'a> {
    pub ts: &'a [i64],
    pub vs: &'a [f64],
    pub range_start: i64,
    pub range_end: i64,
    pub range_ms: i64,
}

/// Apply `func` to one window. `None` is "no sample at this step".
pub fn evaluate(func: Func, w: &Window) -> Option<f64> {
    if w.ts.is_empty() {
        return None;
    }
    match func {
        Func::Rate => extrapolated_rate(w, true, true),
        Func::Increase => extrapolated_rate(w, true, false),
        Func::Delta => extrapolated_rate(w, false, false),
        Func::Irate => instant_value(w, true),
        Func::Idelta => instant_value(w, false),
        Func::SumOverTime => {
            let k = math::kahan_sum(w.vs);
            Some(if k.sum.is_infinite() {
                k.sum
            } else {
                k.value()
            })
        }
        Func::AvgOverTime => Some(math::mean_of(w.vs)),
        Func::MinOverTime => math::min_of(w.vs),
        Func::MaxOverTime => math::max_of(w.vs),
        Func::CountOverTime => Some(w.vs.len() as f64),
        Func::LastOverTime => Some(w.vs[w.vs.len() - 1]),
        Func::PresentOverTime => Some(1.0),
        Func::Changes => Some(
            w.vs.windows(2)
                .filter(|p| p[1] != p[0] && !(p[1].is_nan() && p[0].is_nan()))
                .count() as f64,
        ),
        Func::Resets => Some(w.vs.windows(2).filter(|p| p[1] < p[0]).count() as f64),
    }
}

/// `extrapolatedRate` for floats without start timestamps.
fn extrapolated_rate(w: &Window, is_counter: bool, is_rate: bool) -> Option<f64> {
    let n = w.ts.len();
    let (first_t, last_t) = (w.ts[0], w.ts[n - 1]);
    let mut result = w.vs[n - 1] - w.vs[0];
    if is_counter {
        for p in w.vs.windows(2) {
            if p[1] < p[0] {
                result += p[0];
            }
        }
    }

    let mut duration_to_start = (first_t - w.range_start) as f64 / 1000.0;
    let mut duration_to_end = (w.range_end - last_t) as f64 / 1000.0;
    let sampled_interval = (last_t - first_t) as f64 / 1000.0;
    let average_between_samples = if n > 1 {
        sampled_interval / (n - 1) as f64
    } else {
        0.0
    };
    let extrapolation_threshold = average_between_samples * 1.1;

    if n == 1 {
        // A single sample and no start timestamp to anchor it: nothing.
        return None;
    }
    // Samples close to the boundary extrapolate all the way to it;
    // further away, only half an average interval, our guess for where
    // the series really starts or ends.
    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_between_samples / 2.0;
    }
    if is_counter {
        // Never extrapolate a counter below zero.
        let mut duration_to_zero = duration_to_start;
        if result > 0.0 && w.vs[0] >= 0.0 {
            duration_to_zero = sampled_interval * (w.vs[0] / result);
        }
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_between_samples / 2.0;
    }

    let mut factor = if sampled_interval != 0.0 {
        (sampled_interval + duration_to_start + duration_to_end) / sampled_interval
    } else {
        1.0
    };
    if is_rate {
        factor /= w.range_ms as f64 / 1000.0;
    }
    Some(result * factor)
}

/// `instantValue`: the last two samples.
fn instant_value(w: &Window, is_rate: bool) -> Option<f64> {
    let n = w.ts.len();
    if n < 2 {
        return None;
    }
    let (t0, v0, t1, v1) = (w.ts[n - 2], w.vs[n - 2], w.ts[n - 1], w.vs[n - 1]);
    let sampled_interval = t1 - t0;
    if sampled_interval == 0 {
        return None;
    }
    // A counter reset leaves the result at the newer value.
    let reset = is_rate && v1 < v0;
    let mut result = if reset { v1 } else { v1 - v0 };
    if is_rate {
        result /= sampled_interval as f64 / 1000.0;
    }
    Some(result)
}

/// Evaluate one series on the step grid.
///
/// `ts`/`vs` are one series' samples, sorted ascending; StaleNaN entries
/// are skipped as `matrixIterSlice` skips them. `emit` receives
/// `(step_timestamp, value)` in step order.
pub fn range_function(
    func: Func,
    ts: &[i64],
    vs: &[f64],
    p: &Params,
    mut emit: impl FnMut(i64, f64),
) {
    debug_assert_eq!(ts.len(), vs.len());
    if p.step_ms <= 0 || p.end_ms < p.start_ms || p.range_ms <= 0 {
        return;
    }
    // Staleness markers are not samples. Filter only when there are any,
    // which is rare, so the common path borrows the slices as they are.
    let filtered: Option<(Vec<i64>, Vec<f64>)> = vs.iter().any(|v| is_stale(*v)).then(|| {
        ts.iter()
            .zip(vs)
            .filter(|(_, v)| !is_stale(**v))
            .map(|(t, v)| (*t, *v))
            .unzip()
    });
    let (ts, vs): (&[i64], &[f64]) = match &filtered {
        Some((t, v)) => (t, v),
        None => (ts, vs),
    };

    let window = |range_end: i64| -> Window<'_> {
        let range_start = range_end - p.range_ms;
        let lo = ts.partition_point(|t| *t <= range_start);
        let hi = ts.partition_point(|t| *t <= range_end);
        Window {
            ts: &ts[lo..hi],
            vs: &vs[lo..hi],
            range_start,
            range_end,
            range_ms: p.range_ms,
        }
    };

    // `@` pins the window; evaluate once and repeat across the grid.
    if let Some(at) = p.at_ms {
        if let Some(v) = evaluate(func, &window(at - p.offset_ms)) {
            for step in p.steps() {
                emit(step, v);
            }
        }
        return;
    }

    // Both window edges only move forward with the step.
    let (mut lo, mut hi) = (0usize, 0usize);
    for step in p.steps() {
        let range_end = step - p.offset_ms;
        let range_start = range_end - p.range_ms;
        while lo < ts.len() && ts[lo] <= range_start {
            lo += 1;
        }
        if hi < lo {
            hi = lo;
        }
        while hi < ts.len() && ts[hi] <= range_end {
            hi += 1;
        }
        let w = Window {
            ts: &ts[lo..hi],
            vs: &vs[lo..hi],
            range_start,
            range_end,
            range_ms: p.range_ms,
        };
        if let Some(v) = evaluate(func, &w) {
            emit(step, v);
        }
    }
}

/// The DataFusion function. Stateless: every parameter is an argument.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RangeFunction {
    signature: Signature,
}

impl Default for RangeFunction {
    fn default() -> Self {
        Self {
            signature: Signature::any(8, Volatility::Immutable),
        }
    }
}

pub fn udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RangeFunction::default())
}

/// `promql_range_function(samples, '<func>', start, end, step, range, offset, at)`.
pub fn call(samples: Expr, func: Func, p: &Params) -> Expr {
    udf().call(vec![
        samples,
        lit(func.as_str()),
        lit(p.start_ms),
        lit(p.end_ms),
        lit(p.step_ms),
        lit(p.range_ms),
        lit(p.offset_ms),
        match p.at_ms {
            Some(at) => lit(at),
            None => lit(ScalarValue::Int64(None)),
        },
    ])
}

impl ScalarUDFImpl for RangeFunction {
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
        if arg_types.get(1) != Some(&DataType::Utf8) {
            return plan_err!("{NAME}: second argument must be the function name as Utf8");
        }
        for (i, t) in arg_types.iter().enumerate().skip(2) {
            if !matches!(t, DataType::Int64 | DataType::Null) {
                return plan_err!("{NAME}: argument {i} must be Int64, got {t}");
            }
        }
        Ok(series::samples_type())
    }

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
        let func = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) => {
                Func::parse(name).ok_or_else(|| {
                    DataFusionError::Execution(format!("{NAME}: unknown function {name}"))
                })?
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "{NAME}: function name must be a string literal, got {:?}",
                    other.data_type()
                )))
            }
        };
        let need = |i: usize, what: &str| -> Result<i64> {
            int_arg(&args, i)?.ok_or_else(|| {
                DataFusionError::Execution(format!("{NAME}: {what} must not be NULL"))
            })
        };
        let p = Params {
            start_ms: need(2, "start")?,
            end_ms: need(3, "end")?,
            step_ms: need(4, "step")?,
            range_ms: need(5, "range")?,
            offset_ms: need(6, "offset")?,
            at_ms: int_arg(&args, 7)?,
        };
        Ok(ColumnarValue::Array(Arc::new(apply(
            func,
            samples.as_list::<i32>(),
            &p,
        ))))
    }
}

/// Run the kernel over every row of a samples column.
pub fn apply(func: Func, samples: &ListArray, p: &Params) -> ListArray {
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
        range_function(func, &ts[a..b], &vs[a..b], p, |t, v| {
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
    use crate::instant::STALE_NAN_BITS;

    const S: i64 = 1000;
    const M: i64 = 60 * S;

    fn run(func: Func, ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        let mut out = Vec::new();
        range_function(func, ts, vs, &p, |t, v| out.push((t, v)));
        out
    }

    /// One step at 5m over a 5m window.
    fn at_5m() -> Params {
        Params {
            start_ms: 5 * M,
            end_ms: 5 * M,
            step_ms: 30 * S,
            range_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        }
    }

    /// A counter incrementing by one every 30s: samples 0..=10 at 0..=300s.
    fn counter() -> (Vec<i64>, Vec<f64>) {
        (0..=10).map(|i| (i * 30 * S, i as f64 + 1.0)).unzip()
    }

    #[test]
    fn rate_of_a_clean_counter_is_its_slope() {
        // Window (0, 300s]: ten samples 2..=11 at 30s..=300s. The first
        // is 30s from the range start, within 1.1× the 30s spacing, so
        // extrapolation reaches the boundary: increase 9 × 300/270 = 10.
        let (ts, vs) = counter();
        let out = run(Func::Rate, &ts, &vs, at_5m());
        assert_eq!(out.len(), 1);
        assert!((out[0].1 - 1.0 / 30.0).abs() < 1e-12, "{out:?}");
        let out = run(Func::Increase, &ts, &vs, at_5m());
        assert!((out[0].1 - 10.0).abs() < 1e-12, "{out:?}");
        let out = run(Func::Delta, &ts, &vs, at_5m());
        assert!((out[0].1 - 10.0).abs() < 1e-12, "{out:?}");
    }

    #[test]
    fn a_counter_reset_adds_the_value_before_it() {
        // 2, 3, 1, 2 at 30s..120s in a (0, 120s] window: raw 0, plus the
        // 3 lost at the reset. Extrapolated to the boundary: 3 × 120/90.
        let ts = [30 * S, 60 * S, 90 * S, 120 * S];
        let vs = [2.0, 3.0, 1.0, 2.0];
        let p = Params {
            start_ms: 120 * S,
            end_ms: 120 * S,
            range_ms: 120 * S,
            ..at_5m()
        };
        let out = run(Func::Increase, &ts, &vs, p);
        assert!((out[0].1 - 4.0).abs() < 1e-12, "{out:?}");
        assert_eq!(run(Func::Resets, &ts, &vs, p), vec![(120 * S, 1.0)]);
        // A gauge does not see resets.
        assert!((run(Func::Delta, &ts, &vs, p)[0].1 - 0.0).abs() < 1e-12);
    }

    #[test]
    fn far_from_the_boundary_extrapolation_is_half_an_interval() {
        // Samples at 0, 30s, 60s in a (0, 300s] window; the one at 0 is
        // excluded, leaving 30s and 60s. The start is one interval away
        // (30s < 1.1 × 30s) so extrapolation reaches it; the end is 240s
        // away, so only half an interval, 15s, is added there. Increase =
        // 1 × (30 + 30 + 15)/30 = 2.5. The counter's zero clip does not
        // bite: 30s × (2/1) = 60s > 30s.
        let ts = [0, 30 * S, 60 * S];
        let vs = [1.0, 2.0, 3.0];
        let out = run(Func::Increase, &ts, &vs, at_5m());
        assert!((out[0].1 - 2.5).abs() < 1e-12, "{out:?}");
    }

    #[test]
    fn a_single_sample_yields_nothing_for_rates_but_counts() {
        let ts = [60 * S];
        let vs = [5.0];
        assert!(run(Func::Rate, &ts, &vs, at_5m()).is_empty());
        assert!(run(Func::Irate, &ts, &vs, at_5m()).is_empty());
        assert_eq!(
            run(Func::CountOverTime, &ts, &vs, at_5m()),
            vec![(5 * M, 1.0)]
        );
        assert_eq!(
            run(Func::PresentOverTime, &ts, &vs, at_5m()),
            vec![(5 * M, 1.0)]
        );
    }

    #[test]
    fn the_window_is_open_at_the_start_and_closed_at_the_end() {
        // Samples at exactly range_start and range_end: only the latter.
        let ts = [0, 5 * M];
        let vs = [1.0, 2.0];
        assert_eq!(
            run(Func::CountOverTime, &ts, &vs, at_5m()),
            vec![(5 * M, 1.0)]
        );
        // A step with an empty window emits nothing.
        let p = Params {
            start_ms: 20 * M,
            end_ms: 20 * M,
            ..at_5m()
        };
        assert!(run(Func::CountOverTime, &ts, &vs, p).is_empty());
    }

    #[test]
    fn the_over_time_family() {
        let (ts, vs) = counter();
        let one = |f: Func| run(f, &ts, &vs, at_5m())[0].1;
        assert_eq!(one(Func::SumOverTime), 65.0);
        assert_eq!(one(Func::AvgOverTime), 6.5);
        assert_eq!(one(Func::MinOverTime), 2.0);
        assert_eq!(one(Func::MaxOverTime), 11.0);
        assert_eq!(one(Func::CountOverTime), 10.0);
        assert_eq!(one(Func::LastOverTime), 11.0);
        assert_eq!(one(Func::Changes), 9.0);
        assert_eq!(one(Func::Resets), 0.0);
        assert!((one(Func::Irate) - 1.0 / 30.0).abs() < 1e-12);
        assert_eq!(one(Func::Idelta), 1.0);
    }

    #[test]
    fn irate_on_a_reset_keeps_the_new_value() {
        let ts = [0, 30 * S];
        let vs = [10.0, 2.0];
        let p = Params {
            start_ms: 30 * S,
            end_ms: 30 * S,
            range_ms: M,
            ..at_5m()
        };
        assert!((run(Func::Irate, &ts, &vs, p)[0].1 - 2.0 / 30.0).abs() < 1e-12);
        assert_eq!(run(Func::Idelta, &ts, &vs, p)[0].1, -8.0);
    }

    #[test]
    fn nan_rules_in_min_max_and_changes() {
        let ts = [30 * S, 60 * S, 90 * S];
        let vs = [f64::NAN, 3.0, f64::NAN];
        let p = Params {
            start_ms: 90 * S,
            end_ms: 90 * S,
            range_ms: 2 * M,
            ..at_5m()
        };
        assert_eq!(run(Func::MinOverTime, &ts, &vs, p)[0].1, 3.0);
        assert_eq!(run(Func::MaxOverTime, &ts, &vs, p)[0].1, 3.0);
        assert_eq!(run(Func::Changes, &ts, &vs, p)[0].1, 2.0);
        let vs = [f64::NAN, f64::NAN, 1.0];
        assert_eq!(run(Func::Changes, &ts, &vs, p)[0].1, 1.0);
    }

    #[test]
    fn stale_markers_are_not_samples() {
        let (ts, mut vs) = counter();
        vs[5] = f64::from_bits(STALE_NAN_BITS);
        assert_eq!(run(Func::CountOverTime, &ts, &vs, at_5m())[0].1, 9.0);
        assert_eq!(run(Func::SumOverTime, &ts, &vs, at_5m())[0].1, 65.0 - 6.0);
    }

    #[test]
    fn offset_and_at_move_the_window_not_the_emission() {
        let (ts, vs) = counter();
        let p = Params {
            start_ms: 6 * M,
            end_ms: 6 * M,
            offset_ms: M,
            ..at_5m()
        };
        let out = run(Func::CountOverTime, &ts, &vs, p);
        assert_eq!(out, vec![(6 * M, 10.0)]);

        let p = Params {
            start_ms: 0,
            end_ms: M,
            step_ms: 30 * S,
            at_ms: Some(5 * M),
            ..at_5m()
        };
        let out = run(Func::CountOverTime, &ts, &vs, p);
        assert_eq!(out, vec![(0, 10.0), (30 * S, 10.0), (M, 10.0)]);
    }

    #[test]
    fn the_sweep_matches_a_fresh_window_per_step() {
        let (ts, vs) = counter();
        let p = Params {
            start_ms: 0,
            end_ms: 10 * M,
            step_ms: 30 * S,
            range_ms: 2 * M,
            offset_ms: 0,
            at_ms: None,
        };
        let swept = run(Func::SumOverTime, &ts, &vs, p);
        let mut expected = Vec::new();
        for step in p.steps() {
            let one = run(
                Func::SumOverTime,
                &ts,
                &vs,
                Params {
                    start_ms: step,
                    end_ms: step,
                    ..p
                },
            );
            expected.extend(one);
        }
        assert_eq!(swept, expected);
    }

    #[test]
    fn the_select_range_reflects_the_strict_lower_bound() {
        let p = Params {
            start_ms: 0,
            end_ms: 10 * M,
            step_ms: M,
            range_ms: 5 * M,
            offset_ms: 30 * S,
            at_ms: None,
        };
        assert_eq!(p.select_range(), (-(5 * M) + 1 - 30 * S, 10 * M - 30 * S));
    }
}
