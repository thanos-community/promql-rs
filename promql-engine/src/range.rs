//! Range-vector functions as one DataFusion grouped aggregate function:
//! `promql_range_function(samples, 'rate', start, end, step, range, offset, at)`
//! grouped by `labels`.
//!
//! A range function is the vector selector's shape with a window instead
//! of a lookback, so it runs in the selector's accumulator, [`EvalSeries`],
//! with [`advance_range`] as the walk: one series' chunk rows in, that
//! series' values on the step grid out. The function name is a literal
//! argument, like every parameter, so one registration serves all of them
//! and the plan says which it is.
//!
//! Semantics are `matrixIterSlice`, `extrapolatedRate`, `instantValue`
//! and the `*_over_time` functions in Prometheus's `promql/functions.go`
//! and `engine.go`. Floats only; native histograms are a later slice.

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, GroupsAccumulator, Signature,
    Volatility,
};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::PhysicalExpr;

use crate::buffer::{BufferedSeriesIterator, Kernel};
use crate::math;
use crate::params::{step_count, Params};
use crate::selector::{empty_samples, EvalSeries};
use crate::series::{self, SamplesBuilder};

pub const NAME: &str = "promql_range_function";

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
        Func::Rate => extrapolated_rate(w, raw_increase(w.vs, true), true, true),
        Func::Increase => extrapolated_rate(w, raw_increase(w.vs, true), true, false),
        Func::Delta => extrapolated_rate(w, raw_increase(w.vs, false), false, false),
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

/// `result` in `extrapolatedRate`: the span of the window plus, for a
/// counter, the value lost at every reset.
///
/// The sweep reproduces this sum from the reset positions it tracks
/// rather than calling this, so the additions must stay in sample order
/// and keep starting from the span: a differently associated sum is a
/// different float, and the differential suite holds 1e-10 relative.
fn raw_increase(vs: &[f64], is_counter: bool) -> f64 {
    let mut result = vs[vs.len() - 1] - vs[0];
    if is_counter {
        for p in vs.windows(2) {
            if p[1] < p[0] {
                result += p[0];
            }
        }
    }
    result
}

/// `extrapolatedRate` for floats without start timestamps, from
/// `result` and the window's endpoints alone.
fn extrapolated_rate(w: &Window, result: f64, is_counter: bool, is_rate: bool) -> Option<f64> {
    let n = w.ts.len();
    let (first_t, last_t) = (w.ts[0], w.ts[n - 1]);

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

/// What a step costs once the window's edges have moved.
///
/// A 5m window on a 15s step overlaps its predecessor twenty to one, so a
/// kernel that refolds the window pays for the same samples twenty times.
/// These variants carry just enough across a step that the work is the
/// samples that entered and left it instead.
///
/// Only the functions where that trade measured out are here: removing
/// the sweep cost these 60-78%, while carrying a sliding pair count for
/// `changes`/`resets` was indistinguishable from refolding, and a no-op
/// variant for the rest cost the whole family 16-19% in `enter`, `leave`
/// and the dispatch in `value`. Everything else refolds through
/// [`evaluate`], with no `Sweep` at all.
pub(crate) enum Sweep {
    /// Where the counter reset, oldest first, so `raw_increase`'s sum
    /// can be replayed in sample order instead of carried as a running
    /// float that drifts as pairs leave.
    Counter {
        resets: VecDeque<usize>,
        /// `rate` divides by the window; `increase` does not.
        is_rate: bool,
    },
    /// Candidates for `min_over_time`/`max_over_time`, extreme at the
    /// front. NaNs never enter, since any number beats one; a window
    /// with only NaNs is recognised by the queue being empty.
    Extremum {
        candidates: VecDeque<usize>,
        max: bool,
    },
}

impl Sweep {
    /// Between series, so that one `Sweep` can be hoisted above a row
    /// loop and keep the `VecDeque`s' capacity.
    pub(crate) fn reset(&mut self) {
        match self {
            Sweep::Counter { resets, .. } => resets.clear(),
            Sweep::Extremum { candidates, .. } => candidates.clear(),
        }
    }

    pub(crate) fn new(func: Func) -> Option<Sweep> {
        Some(match func {
            Func::Rate | Func::Increase => Sweep::Counter {
                resets: VecDeque::new(),
                is_rate: func == Func::Rate,
            },
            Func::MinOverTime => Sweep::Extremum {
                candidates: VecDeque::new(),
                max: false,
            },
            Func::MaxOverTime => Sweep::Extremum {
                candidates: VecDeque::new(),
                max: true,
            },
            _ => return None,
        })
    }

    /// Sample `i` joins the window's end; `lo` is its start, so a pair
    /// arrives with `i` only once its left half is inside. `vs` holds
    /// ordinals from `base` on: the tracked indices stay absolute so that
    /// trimming the buffer's front never has to rewrite them.
    fn enter(&mut self, i: usize, lo: usize, base: usize, vs: &[f64]) {
        let at = |j: usize| vs[j - base];
        match self {
            Sweep::Counter { resets, .. } => {
                if i > lo && at(i) < at(i - 1) {
                    resets.push_back(i - 1);
                }
            }
            Sweep::Extremum { candidates, max } => {
                if at(i).is_nan() {
                    return;
                }
                // A strict comparison keeps the earliest of equal
                // values, which is the one `min_of`'s fold settles on
                // and the only way `-0.0` against `0.0` agrees.
                while let Some(&back) = candidates.back() {
                    let beaten = if *max {
                        at(back) < at(i)
                    } else {
                        at(back) > at(i)
                    };
                    if !beaten {
                        break;
                    }
                    candidates.pop_back();
                }
                candidates.push_back(i);
            }
        }
    }

    fn leave(&mut self, i: usize) {
        match self {
            Sweep::Counter { resets, .. } => {
                if resets.front() == Some(&i) {
                    resets.pop_front();
                }
            }
            Sweep::Extremum { candidates, .. } => {
                if candidates.front() == Some(&i) {
                    candidates.pop_front();
                }
            }
        }
    }

    /// The step's value, `None` where `evaluate` gives `None`. `lo`
    /// rebases the tracked indices onto the window's slices.
    fn value(&self, w: &Window, lo: usize) -> Option<f64> {
        if w.ts.is_empty() {
            return None;
        }
        match self {
            Sweep::Counter { resets, is_rate } => {
                let mut result = w.vs[w.vs.len() - 1] - w.vs[0];
                for i in resets {
                    result += w.vs[i - lo];
                }
                extrapolated_rate(w, result, true, *is_rate)
            }
            Sweep::Extremum { candidates, .. } => Some(match candidates.front() {
                Some(i) => w.vs[i - lo],
                // Every sample is NaN, and `min_of` folds those to the
                // last one rather than the first.
                None => w.vs[w.vs.len() - 1],
            }),
        }
    }
}

/// Evaluate the steps of `it`'s open series that are ready.
///
/// Without `done`, a step is ready once its window's end is at or before
/// `last_t`: the samples still to come are all later, so they cannot
/// land in it. With `done` there are no samples to come, and every step
/// left is ready. Evaluating as the chunks arrive, rather than at close,
/// is what lets the buffer drop everything below `lo` between chunks.
///
/// `it.ts`/`it.vs` must already be free of StaleNaN entries, and a
/// `Sweep`, if any, must have seen only this series. `emit` receives
/// `(step_timestamp, value)` in step order, each step at most once.
pub(crate) fn advance_range(
    it: &mut BufferedSeriesIterator,
    func: Func,
    done: bool,
    mut emit: impl FnMut(i64, f64),
) {
    let BufferedSeriesIterator {
        sweep,
        params: p,
        ts,
        vs,
        base,
        lo,
        hi,
        next_step,
        last_t,
        ..
    } = it;
    debug_assert_eq!(ts.len(), vs.len());
    let steps = step_count(p.start_ms, p.end_ms, p.step_ms);
    if p.window_ms <= 0 || *next_step >= steps {
        return;
    }
    // A window ending after this may still gain a sample.
    let ready = match (done, *last_t) {
        (true, _) => i64::MAX,
        (false, Some(t)) => t,
        (false, None) => return,
    };
    let base = *base;
    let len = base + ts.len();
    let slice = |lo: usize, hi: usize, range_end: i64| -> Window<'_> {
        Window {
            ts: &ts[lo - base..hi - base],
            vs: &vs[lo - base..hi - base],
            range_start: range_end - p.window_ms,
            range_end,
            range_ms: p.window_ms,
        }
    };

    // `@` pins the window: it is evaluated once, when it is ready, and
    // repeated across the whole grid.
    if let Some(at) = p.at_ms {
        let range_end = at - p.offset_ms;
        if range_end > ready {
            return;
        }
        let lo = base + ts.partition_point(|t| *t <= range_end - p.window_ms);
        let hi = base + ts.partition_point(|t| *t <= range_end);
        if let Some(v) = evaluate(func, &slice(lo, hi, range_end)) {
            for step in p.steps() {
                emit(step, v);
            }
        }
        *next_step = steps;
        return;
    }

    // Both window edges only move forward with the step, so a sweep sees
    // every sample enter once and leave once, across chunks too. The two
    // loops are one `match` above the step loop rather than a branch
    // inside it, so the refolding path pays neither the calls nor the
    // dispatch in them.
    let step_ms = i128::from(p.step_ms);
    match sweep {
        None => {
            while *next_step < steps {
                let step = (i128::from(p.start_ms) + *next_step * step_ms) as i64;
                let range_end = step - p.offset_ms;
                if range_end > ready {
                    return;
                }
                let range_start = range_end - p.window_ms;
                while *lo < len && ts[*lo - base] <= range_start {
                    *lo += 1;
                }
                *hi = (*hi).max(*lo);
                while *hi < len && ts[*hi - base] <= range_end {
                    *hi += 1;
                }
                if let Some(v) = evaluate(func, &slice(*lo, *hi, range_end)) {
                    emit(step, v);
                }
                *next_step += 1;
            }
        }
        Some(sweep) => {
            while *next_step < steps {
                let step = (i128::from(p.start_ms) + *next_step * step_ms) as i64;
                let range_end = step - p.offset_ms;
                if range_end > ready {
                    return;
                }
                let range_start = range_end - p.window_ms;
                while *lo < len && ts[*lo - base] <= range_start {
                    if *lo < *hi {
                        sweep.leave(*lo);
                    }
                    *lo += 1;
                }
                // A gap wider than the window leaves `hi` behind `lo`;
                // the samples it skips never entered, and the state is
                // empty.
                *hi = (*hi).max(*lo);
                while *hi < len && ts[*hi - base] <= range_end {
                    sweep.enter(*hi, *lo, base, vs);
                    *hi += 1;
                }
                if let Some(v) = sweep.value(&slice(*lo, *hi, range_end), *lo) {
                    emit(step, v);
                }
                *next_step += 1;
            }
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
        let mut args = vec![series::samples_type(), DataType::Utf8];
        args.extend(std::iter::repeat_n(DataType::Int64, 6));
        Self {
            signature: Signature::exact(args, Volatility::Immutable),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(RangeFunction::default())
}

/// `promql_range_function(samples, '<func>', start, end, step, range, offset, at)`.
pub fn call(samples: Expr, func: Func, p: &Params) -> Expr {
    udaf().call(vec![
        samples,
        lit(func.as_str()),
        lit(p.start_ms),
        lit(p.end_ms),
        lit(p.step_ms),
        lit(p.window_ms),
        lit(p.offset_ms),
        lit(ScalarValue::Int64(p.at_ms)),
    ])
}

/// The function name and the grid, read off a planned call's literals.
/// Every error names this function: the grid literals are read by a
/// helper shared with the selector, whose own message cannot say which
/// of the two it was reading for.
fn read_args(exprs: &[Arc<dyn PhysicalExpr>]) -> Result<(Func, Params)> {
    let name = exprs
        .get(1)
        .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
        .map(Literal::value);
    let func = match name {
        Some(ScalarValue::Utf8(Some(name))) => match Func::parse(name) {
            Some(func) => func,
            None => return plan_err!("{NAME}: unknown function {name}"),
        },
        other => {
            return plan_err!("{NAME}: the function name must be a Utf8 literal, got {other:?}")
        }
    };
    let params = Params::from_literals(exprs, 2).map_err(|e| e.context(NAME))?;
    Ok((func, params))
}

impl AggregateUDFImpl for RangeFunction {
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
        Ok(series::samples_type())
    }

    /// Every series yields a list, possibly empty; never NULL, so the
    /// output is the canonical shape it came in as.
    fn is_nullable(&self) -> bool {
        false
    }

    /// Without a group key there is no series to fold rows into.
    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        plan_err!("{NAME} must be grouped by labels")
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "samples"),
            series::samples_type(),
            false,
        ))])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    /// `DISTINCT` would deduplicate chunk rows, which is the overlap rule's
    /// job; answering without it would be silently different.
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        let (func, params) = read_args(args.exprs)?;
        Ok(Box::new(EvalSeries::new(Kernel::Range(func), params)))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(empty_samples())
    }
}

/// Run the kernel over every row of a samples column, each row being a
/// whole series in one chunk.
///
/// This is the cursor fed one chunk per series: no plan calls it any more,
/// it stays as the one-row oracle until the scalar path is deleted.
pub fn apply(func: Func, samples: &ListArray, p: &Params) -> ListArray {
    // Offsets index the child as it is, so a sliced `ListArray`, whose
    // offsets need not start at zero and whose child may still hold the
    // dropped rows' samples, needs no rebasing.
    let offsets = samples.offsets();
    let (ts, vs) = series::sample_slices(samples.values().as_struct());

    let mut out = SamplesBuilder::default();
    // One cursor for the column, so the buffers and the sweep's queues
    // keep their capacity from series to series.
    let mut it = BufferedSeriesIterator::new(Kernel::Range(func), *p);
    for row in 0..samples.len() {
        // A null row is an absent series, not an empty one, and its
        // offsets may still span samples.
        if !samples.is_null(row) {
            let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
            it.push(&ts[a..b], &vs[a..b], &mut out);
        }
        it.close(&mut out);
    }

    let (field, offsets, values, _) = out.take_all().into_parts();
    // One output row per input row, in order, so the input's validity is
    // the output's.
    ListArray::new(field, offsets, values, samples.nulls().cloned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selector::{is_stale, STALE_NAN_BITS};
    use datafusion::arrow::array::{Float64Array, StructArray, TimestampMillisecondArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::{Float64Type, TimestampMillisecondType};

    const S: i64 = 1000;
    const M: i64 = 60 * S;

    /// Routes through `apply` (a one-row samples column) rather than
    /// calling `advance_range` directly, so the staleness filter, which
    /// lives in `apply` now, is exercised by every test that uses `run`.
    fn run(func: Func, ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts.to_vec())),
                Arc::new(Float64Array::from(vs.to_vec())),
            ],
            None,
        );
        let samples = ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(vec![0, ts.len() as i32].into()),
            Arc::new(entries),
            None,
        );
        let out = apply(func, &samples, &p);
        let row = out.value(0);
        let row = row.as_struct();
        let out_ts = row
            .column_by_name(series::TIMESTAMP)
            .unwrap()
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let out_vs = row
            .column_by_name(series::VALUE)
            .unwrap()
            .as_primitive::<Float64Type>()
            .values();
        out_ts.iter().copied().zip(out_vs.iter().copied()).collect()
    }

    /// One step at 5m over a 5m window.
    fn at_5m() -> Params {
        Params {
            start_ms: 5 * M,
            end_ms: 5 * M,
            step_ms: 30 * S,
            window_ms: 5 * M,
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
            window_ms: 120 * S,
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
            window_ms: M,
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
            window_ms: 2 * M,
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

    /// Every function, because both branches of `advance_range` are
    /// sliding walks that carry `lo`/`hi` across steps: the swept one
    /// carries kernel state too, the other only the bounds. `fresh`
    /// recomputes both bounds per step with `partition_point`, so it
    /// pins each walk against an independent oracle rather than
    /// against itself.
    const ALL: [Func; 14] = [
        Func::Rate,
        Func::Increase,
        Func::Delta,
        Func::Irate,
        Func::Idelta,
        Func::SumOverTime,
        Func::AvgOverTime,
        Func::MinOverTime,
        Func::MaxOverTime,
        Func::CountOverTime,
        Func::LastOverTime,
        Func::PresentOverTime,
        Func::Changes,
        Func::Resets,
    ];

    /// Counter resets, NaN runs, a staleness marker, signed zeros and a
    /// gap wider than the window: one series that reaches every branch
    /// of every kernel as a 2m window slides over it.
    fn a_rough_series() -> (Vec<i64>, Vec<f64>) {
        let stale = f64::from_bits(STALE_NAN_BITS);
        [
            (0, 1.0),
            (30 * S, 2.0),
            (60 * S, 2.0),
            (90 * S, 1.0),
            (120 * S, f64::NAN),
            (150 * S, 5.0),
            (180 * S, stale),
            (210 * S, 4.0),
            (240 * S, -0.0),
            (270 * S, 0.0),
            (300 * S, 9.0),
            (330 * S, 9.0),
            (360 * S, 8.0),
            // Nothing for nine minutes, then two samples that are only
            // ever seen together with nothing else: an all-NaN window.
            (900 * S, f64::NAN),
            (930 * S, f64::NAN),
            (960 * S, 3.0),
            (990 * S, 3.0),
            (1020 * S, 2.0),
        ]
        .into_iter()
        .unzip()
    }

    /// The oracle: one window built from scratch per step, handed to
    /// `evaluate`, with the sweep's staleness filter in front of it.
    fn fresh(func: Func, ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        let (ts, vs): (Vec<i64>, Vec<f64>) = ts
            .iter()
            .zip(vs)
            .filter(|(_, v)| !is_stale(**v))
            .map(|(t, v)| (*t, *v))
            .unzip();
        let mut out = Vec::new();
        for step in p.steps() {
            let range_end = step - p.offset_ms;
            let range_start = range_end - p.window_ms;
            let lo = ts.partition_point(|t| *t <= range_start);
            let hi = ts.partition_point(|t| *t <= range_end);
            let w = Window {
                ts: &ts[lo..hi],
                vs: &vs[lo..hi],
                range_start,
                range_end,
                range_ms: p.window_ms,
            };
            if let Some(v) = evaluate(func, &w) {
                out.push((step, v));
            }
        }
        out
    }

    #[test]
    fn the_sliding_walks_match_a_fresh_window_per_step() {
        let (ts, vs) = a_rough_series();
        let p = Params {
            start_ms: 0,
            end_ms: 20 * M,
            step_ms: 15 * S,
            window_ms: 2 * M,
            offset_ms: 45 * S,
            at_ms: None,
        };
        for func in ALL {
            let name = func.as_str();
            let swept = run(func, &ts, &vs, p);
            let expected = fresh(func, &ts, &vs, p);
            assert_eq!(swept.len(), expected.len(), "{name}");
            for (a, b) in swept.iter().zip(&expected) {
                assert_eq!(a.0, b.0, "{name}");
                assert_eq!(
                    a.1.to_bits(),
                    b.1.to_bits(),
                    "{name} at {}: {} vs {}",
                    a.0,
                    a.1,
                    b.1
                );
            }
        }
    }

    #[test]
    fn fuzz_the_sliding_walks_against_fresh() {
        let mut seed: u64 = 0x2545F4914F6CDD1D;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for case in 0..400 {
            let n = (next() % 25) as usize;
            let mut ts: Vec<i64> = Vec::new();
            let mut vs: Vec<f64> = Vec::new();
            let mut t = 0i64;
            let mut v = 0.0f64;
            for _ in 0..n {
                t += ((next() % 6) as i64) * 30 * S;
                if ts.last() == Some(&t) {
                    continue;
                }
                ts.push(t);
                v = match next() % 10 {
                    0 => f64::NAN,
                    1 => f64::from_bits(STALE_NAN_BITS),
                    2 => -0.0,
                    3 => 0.0,
                    4 => v - (next() % 5) as f64,
                    _ => v + (next() % 5) as f64,
                };
                vs.push(v);
            }
            let p = Params {
                start_ms: 0,
                end_ms: (t + 5 * M).max(M),
                step_ms: 15 * S,
                window_ms: (1 + next() % 8) as i64 * 30 * S,
                offset_ms: (next() % 4) as i64 * 15 * S,
                at_ms: None,
            };
            for func in ALL {
                let name = func.as_str();
                let swept = run(func, &ts, &vs, p);
                let expected = fresh(func, &ts, &vs, p);
                assert_eq!(swept.len(), expected.len(), "case {case} {name}");
                for (a, b) in swept.iter().zip(&expected) {
                    assert_eq!(a.0, b.0, "case {case} {name}");
                    assert_eq!(
                        a.1.to_bits(),
                        b.1.to_bits(),
                        "case {case} {name} at {}: {} vs {} | ts={ts:?} vs={vs:?} p={p:?}",
                        a.0,
                        a.1,
                        b.1
                    );
                }
            }
        }
    }

    /// Feeds `ts`/`vs` through the cursor `k` samples at a time, as if
    /// `base` samples had already been trimmed off the front, and trims
    /// below `lo` after every chunk the way `BufferedSeriesIterator::push`
    /// does, so that every ordinal the sweep holds is shifted off its
    /// buffer index.
    fn cursor(
        func: Func,
        ts: &[i64],
        vs: &[f64],
        p: Params,
        base: usize,
        k: usize,
    ) -> Vec<(i64, f64)> {
        let mut it = BufferedSeriesIterator::new(Kernel::Range(func), p);
        (it.base, it.lo, it.hi) = (base, base, base);
        let mut out = Vec::new();
        for (cts, cvs) in ts.chunks(k).zip(vs.chunks(k)) {
            for (t, v) in cts.iter().zip(cvs) {
                if !is_stale(*v) {
                    it.ts.push(*t);
                    it.vs.push(*v);
                }
            }
            it.last_t = cts.last().copied();
            advance_range(&mut it, func, false, |t, v| out.push((t, v)));
            let dead = it.lo - it.base;
            it.ts.drain(..dead);
            it.vs.drain(..dead);
            it.base += dead;
        }
        advance_range(&mut it, func, true, |t, v| out.push((t, v)));
        out
    }

    #[test]
    fn advance_range_matches_apply_across_a_base_shift() {
        let (ts, vs) = a_rough_series();
        let plain = Params {
            start_ms: 0,
            end_ms: 20 * M,
            step_ms: 15 * S,
            window_ms: 2 * M,
            offset_ms: 45 * S,
            at_ms: None,
        };
        let params = [
            plain,
            Params {
                offset_ms: 0,
                ..plain
            },
            Params {
                at_ms: Some(5 * M),
                ..plain
            },
            Params {
                at_ms: Some(17 * M),
                ..plain
            },
        ];
        for p in params {
            for func in ALL {
                let expected = run(func, &ts, &vs, p);
                for base in [1, 7, 1000] {
                    for k in [1, 2, 3, 5, ts.len()] {
                        let got = cursor(func, &ts, &vs, p, base, k);
                        let name = func.as_str();
                        assert_eq!(
                            got.len(),
                            expected.len(),
                            "{name} base={base} k={k} p={p:?}"
                        );
                        for (a, b) in got.iter().zip(&expected) {
                            assert_eq!(a.0, b.0, "{name} base={base} k={k} p={p:?}");
                            assert_eq!(
                                a.1.to_bits(),
                                b.1.to_bits(),
                                "{name} base={base} k={k} p={p:?} at {}: {} vs {}",
                                a.0,
                                a.1,
                                b.1
                            );
                        }
                    }
                }
            }
        }
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
            Func::CountOverTime,
            &with_a_null_row(),
            &Params {
                start_ms: 0,
                end_ms: 0,
                ..at_5m()
            },
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out.null_count(), 1);
        assert!(!out.is_null(0) && out.is_null(1) && !out.is_null(2));
        // The sample the null row spans stays out of the output.
        assert_eq!(out.offsets().to_vec(), vec![0, 1, 1, 2]);
    }

    /// A sliced `ListArray` shares its child and offsets verbatim with
    /// the original: the first offset is not zero, and the child still
    /// holds the dropped row's samples, including a stale one. The mask
    /// and the recomputed offsets must line up against that shared
    /// child, not against a zero-based view of it.
    #[test]
    fn a_stale_marker_in_a_dropped_row_does_not_shift_a_sliced_rows_offsets() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, 0, 30 * S])),
                Arc::new(Float64Array::from(vec![stale, 1.0, 2.0])),
            ],
            None,
        );
        // Row 0: one stale sample. Row 1: two clean samples.
        let two_rows = ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(vec![0, 1, 3].into()),
            Arc::new(entries),
            None,
        );
        let sliced = two_rows.slice(1, 1);

        let p = Params {
            start_ms: 30 * S,
            end_ms: 30 * S,
            window_ms: M,
            ..at_5m()
        };
        let from_sliced = apply(Func::CountOverTime, &sliced, &p);

        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, 30 * S])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
            None,
        );
        let one_row = ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(entries),
            None,
        );
        let from_unsliced = apply(Func::CountOverTime, &one_row, &p);

        assert_eq!(
            from_sliced.offsets().to_vec(),
            from_unsliced.offsets().to_vec()
        );
        let sliced_values = from_sliced.values().as_struct();
        let unsliced_values = from_unsliced.values().as_struct();
        assert_eq!(
            sliced_values
                .column_by_name(series::VALUE)
                .unwrap()
                .as_primitive::<Float64Type>()
                .values(),
            unsliced_values
                .column_by_name(series::VALUE)
                .unwrap()
                .as_primitive::<Float64Type>()
                .values()
        );
        // Both count the window's two samples.
        assert_eq!(
            from_unsliced
                .values()
                .as_struct()
                .column_by_name(series::VALUE)
                .unwrap()
                .as_primitive::<Float64Type>()
                .value(0),
            2.0
        );
    }

    /// The grid literals are read by a helper shared with the vector
    /// selector, so the name has to be added on the way out.
    #[test]
    fn an_argument_error_names_the_range_function() {
        let lit = |v: ScalarValue| Arc::new(Literal::new(v)) as Arc<dyn PhysicalExpr>;
        let exprs = vec![
            lit(ScalarValue::Null),
            lit(ScalarValue::Utf8(Some("rate".into()))),
            // `start`, which is not an Int64 literal.
            lit(ScalarValue::Utf8(Some("noon".into()))),
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(Some(30 * S))),
            lit(ScalarValue::Int64(Some(5 * M))),
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(None)),
        ];
        let err = read_args(&exprs).unwrap_err().to_string();
        assert!(err.contains(NAME), "{err}");
        assert!(!err.contains(crate::selector::NAME), "{err}");

        let mut unknown = exprs;
        unknown[1] = lit(ScalarValue::Utf8(Some("rote".into())));
        unknown[2] = lit(ScalarValue::Int64(Some(0)));
        let err = read_args(&unknown).unwrap_err().to_string();
        assert!(err.contains(NAME) && err.contains("rote"), "{err}");
    }
}
