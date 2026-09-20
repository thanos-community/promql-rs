//! `histogram_quantile(φ, v)` over classic histograms:
//! `promql_histogram_quantile(samples, le, φ, start, end, step)
//!  … GROUP BY <every label but le>`.
//!
//! A classic histogram is not one series but a family of them, one per
//! `le` bucket, and the quantile is read across the family at each step.
//! So this is an aggregation like [`crate::aggregate`]'s, with the
//! grouping doing what upstream's `resetHistograms` does with a label
//! signature: gather the buckets of one histogram under one key
//! (`promql/engine.go:1291-1332` at 83962c35). What is left per group is
//! `BucketQuantile` (`promql/quantile.go:105-168`), step by step.
//!
//! The state cannot be flat lanes the way an aggregation's are: a group
//! holds one lane *per bucket*, and how many buckets there are is the
//! data's business. It is a short vector of (upper bound, lane) instead,
//! searched linearly — a histogram has a handful of buckets, and the
//! quantile has to sort them anyway.
//!
//! Native histograms are not here, because no sample in this engine is
//! one yet; [`crate::plan`] names that gap rather than answering for it.

use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, ListArray, StringViewArray, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

use crate::aggregate::Grid;
use crate::selector::is_stale;
use crate::series;

pub const NAME: &str = "promql_histogram_quantile";

/// The label a classic histogram's upper bound is written in.
pub const BUCKET_LABEL: &str = "le";

/// What upstream calls a difference too small to be a real decrease
/// (`smallDeltaTolerance`, `promql/quantile.go:45` at 83962c35).
const SMALL_DELTA_TOLERANCE: f64 = 1e-12;

/// One bucket of a classic histogram: the `le` it was labelled with and
/// the cumulative count at one step.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    pub upper_bound: f64,
    pub count: f64,
}

/// `BucketQuantile` (`promql/quantile.go:105-168` at 83962c35), without
/// the annotations it also reports and without `coalesceBuckets`, whose
/// work [`Lanes::lane`] has already done: two series whose `le` parses
/// to one bound share one lane and their counts are summed there, so no
/// bound reaches here twice.
///
/// Takes the buckets by value because it sorts and rewrites counts,
/// both of which upstream does to its caller's slice.
pub fn bucket_quantile(q: f64, mut buckets: Vec<Bucket>) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    if buckets.is_empty() {
        return f64::NAN;
    }
    // `partial_cmp`, not `total_cmp`: upstream's comparator calls a NaN
    // bound neither less nor greater, which leaves it wherever it was
    // and `+Inf` last. `total_cmp` would order NaN past `+Inf` and fail
    // the check below over a histogram that does have an `+Inf` bucket.
    buckets.sort_by(|a, b| {
        a.upper_bound
            .partial_cmp(&b.upper_bound)
            .unwrap_or(Ordering::Equal)
    });
    if buckets[buckets.len() - 1].upper_bound != f64::INFINITY {
        return f64::NAN;
    }

    ensure_monotonic(&mut buckets);

    if buckets.len() < 2 {
        return f64::NAN;
    }
    let observations = buckets[buckets.len() - 1].count;
    if observations == 0.0 {
        return f64::NAN;
    }
    let mut rank = q * observations;
    // Upstream's `sort.Search` over all but the last bucket: the first
    // bucket whose count reaches the rank, or one past the end.
    let searched = buckets.len() - 1;
    let b = (0..searched)
        .find(|&i| buckets[i].count >= rank)
        .unwrap_or(searched);

    if b == buckets.len() - 1 {
        return buckets[buckets.len() - 2].upper_bound;
    }
    // The lowest bucket with a non-positive upper bound has no room to
    // interpolate in: there is no natural lower bound below it.
    if b == 0 && buckets[0].upper_bound <= 0.0 {
        return buckets[0].upper_bound;
    }
    let mut bucket_start = 0.0;
    let bucket_end = buckets[b].upper_bound;
    let mut count = buckets[b].count;
    if b > 0 {
        bucket_start = buckets[b - 1].upper_bound;
        count -= buckets[b - 1].count;
        rank -= buckets[b - 1].count;
    }
    bucket_start + (bucket_end - bucket_start) * (rank / count)
}

/// `ensureMonotonicAndIgnoreSmallDeltas`: a count that drops is raised
/// back to its predecessor, and a difference small enough to be a
/// rounding artifact is flattened the same way.
///
/// Upstream also reports whether it had to, which feeds an annotation
/// this engine has no channel for; the counts it leaves behind are the
/// part the quantile reads.
fn ensure_monotonic(buckets: &mut [Bucket]) {
    let mut prev = buckets[0].count;
    for bucket in buckets.iter_mut().skip(1) {
        let curr = bucket.count;
        if curr == prev {
            continue;
        }
        if almost_equal(prev, curr, SMALL_DELTA_TOLERANCE) || curr < prev {
            bucket.count = prev;
            continue;
        }
        prev = curr;
    }
}

/// The smallest positive normal `f64`, which upstream's `almost.Equal`
/// uses as the scale below which a relative comparison stops meaning
/// anything.
const MIN_NORMAL: f64 = f64::MIN_POSITIVE;

/// `almost.Equal` (`util/almost/almost.go` at 83962c35). A staleness
/// marker is only ever equal to another one, which cannot reach here —
/// the selector drops stale samples — but the rule is cheap to keep and
/// expensive to rediscover.
fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    if is_stale(a) || is_stale(b) {
        return is_stale(a) && is_stale(b);
    }
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < MIN_NORMAL {
        return diff < epsilon * MIN_NORMAL;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

/// One group's buckets: an upper bound and the counts it had at each
/// step of the grid.
#[derive(Debug)]
struct Lanes {
    grid: Grid,
    phi: f64,
    /// One entry per distinct `le`, in the order first seen. A handful
    /// per histogram, so a linear search beats anything with a hash.
    buckets: Vec<(f64, Vec<Option<f64>>)>,
}

impl Lanes {
    fn new(phi: f64, grid: Grid) -> Self {
        Self {
            grid,
            phi,
            buckets: Vec::new(),
        }
    }

    /// The lane for one upper bound, created empty on first sight. This
    /// is where `coalesceBuckets` happens: `le="0.2"` and `le="2e-1"`
    /// are one bound and so one lane, whose counts [`Lanes::add`] sums.
    fn lane(&mut self, upper_bound: f64) -> &mut Vec<Option<f64>> {
        // `total_cmp` rather than `==`, so that the NaN a bucket label
        // can parse to lands in one lane instead of a new one each time.
        let at = self
            .buckets
            .iter()
            .position(|(b, _)| b.total_cmp(&upper_bound).is_eq());
        match at {
            Some(i) => &mut self.buckets[i].1,
            None => {
                self.buckets
                    .push((upper_bound, vec![None; self.grid.len()]));
                &mut self.buckets.last_mut().expect("just pushed").1
            }
        }
    }

    /// One row: a bucket series and the `le` it was labelled with. The
    /// count is added to whatever the lane already held, which is
    /// `coalesceBuckets` for two spellings of one bound and the only
    /// correct merge of two partitions' states.
    fn add(&mut self, upper_bound: f64, timestamps: &[i64], values: &[f64]) -> Result<()> {
        let grid = self.grid;
        let lane = self.lane(upper_bound);
        grid.runs(timestamps, |index, from, len| {
            for k in 0..len {
                let at = &mut lane[index + k];
                *at = Some(at.unwrap_or(0.0) + values[from + k]);
            }
        })
    }

    /// The buckets present at one step. Upstream evaluates a step at a
    /// time and only sees the samples that exist there, so a bucket
    /// series with a gap is simply not part of that step's histogram.
    fn at(&self, step: usize) -> Vec<Bucket> {
        self.buckets
            .iter()
            .filter_map(|(upper_bound, lane)| {
                lane[step].map(|count| Bucket {
                    upper_bound: *upper_bound,
                    count,
                })
            })
            .collect()
    }
}

impl Accumulator for Lanes {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let list = values[0].as_list_opt::<i32>().ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: first argument is not a samples list"))
        })?;
        let labels = values[1].as_ref();
        let labels: &StringViewArray = labels.as_any().downcast_ref().ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: second argument is not a label value"))
        })?;
        let entries = list.values().as_struct();
        let timestamps = child::<TimestampMillisecondArray>(entries, series::TIMESTAMP)?.values();
        let counts = child::<Float64Array>(entries, series::VALUE)?.values();
        let offsets = list.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            // A bucket label that is not a number is not a bucket:
            // upstream warns and drops the series (`resetHistograms`,
            // `promql/engine.go:1311-1321`), which for a histogram with
            // no other buckets leaves nothing to answer with.
            let Ok(upper_bound) = labels.value(row).parse::<f64>() else {
                continue;
            };
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            self.add(upper_bound, &timestamps[lo..hi], &counts[lo..hi])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for step in 0..self.grid.len() {
            let buckets = self.at(step);
            // No bucket at this step is no histogram at this step, and
            // upstream emits nothing for it.
            if buckets.is_empty() {
                continue;
            }
            timestamps.push(self.grid.timestamp(step));
            values.push(bucket_quantile(self.phi, buckets));
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

    /// The partial state is the buckets themselves: `(le, timestamp,
    /// count)` for every sample this accumulator has seen. There is no
    /// smaller summary — a quantile needs the whole histogram, and
    /// which bucket a partition happened to hold says nothing about the
    /// others.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let (mut bounds, mut timestamps, mut counts) = (Vec::new(), Vec::new(), Vec::new());
        for (upper_bound, lane) in &self.buckets {
            for (step, count) in lane.iter().enumerate() {
                if let Some(count) = count {
                    bounds.push(*upper_bound);
                    timestamps.push(self.grid.timestamp(step));
                    counts.push(*count);
                }
            }
        }
        let entries = StructArray::new(
            state_fields(),
            vec![
                Arc::new(Float64Array::from(bounds)),
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                Arc::new(Float64Array::from(counts)),
            ],
            None,
        );
        Ok(vec![one_row(state_item(), Arc::new(entries))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let list = states[0].as_list_opt::<i32>().ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: partial state is not a list"))
        })?;
        let entries = list.values().as_struct();
        let bounds = child::<Float64Array>(entries, STATE_BOUND)?.values();
        let timestamps = child::<TimestampMillisecondArray>(entries, series::TIMESTAMP)?.values();
        let counts = child::<Float64Array>(entries, series::VALUE)?.values();
        let offsets = list.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            for i in offsets[row] as usize..offsets[row + 1] as usize {
                self.add(bounds[i], &timestamps[i..i + 1], &counts[i..i + 1])?;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .buckets
                .iter()
                .map(|(_, lane)| {
                    std::mem::size_of::<(f64, Vec<Option<f64>>)>()
                        + lane.capacity() * std::mem::size_of::<Option<f64>>()
                })
                .sum::<usize>()
    }
}

/// The `le` of the partial state's rows; the other two columns are the
/// canonical sample's own names.
const STATE_BOUND: &str = "le";

fn state_fields() -> Fields {
    Fields::from(vec![
        Field::new(STATE_BOUND, DataType::Float64, false),
        Field::new(series::TIMESTAMP, series::timestamp_type(), false),
        Field::new(series::VALUE, DataType::Float64, false),
    ])
}

fn state_item() -> FieldRef {
    Arc::new(Field::new(
        series::LIST_ITEM,
        DataType::Struct(state_fields()),
        false,
    ))
}

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
            DataFusionError::Internal(format!("{NAME}: no {name} column of the expected type"))
        })
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HistogramQuantile {
    signature: Signature,
}

impl Default for HistogramQuantile {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    series::samples_type(),
                    series::label_type(),
                    DataType::Float64,
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
    AggregateUDF::new_from_impl(HistogramQuantile::default())
}

/// `promql_histogram_quantile(samples, le, φ, start, end, step)`.
///
/// φ is a literal because upstream reads it once per evaluation and
/// this engine folds it while planning; `le` is a column because it is
/// the one label the operator reads, and it reads it per series.
pub fn call(samples: Expr, le: Expr, phi: f64, start_ms: i64, end_ms: i64, step_ms: i64) -> Expr {
    udaf().call(vec![
        samples,
        le,
        lit(phi),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

fn from_args(args: &AccumulatorArgs) -> Result<Lanes> {
    let literal = |i: usize| {
        args.exprs
            .get(i)
            .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
            .map(Literal::value)
    };
    let phi = literal(2)
        .and_then(|v| match v {
            ScalarValue::Float64(Some(q)) => Some(*q),
            _ => None,
        })
        .ok_or_else(|| {
            DataFusionError::Plan(format!("{NAME}: the quantile must be a Float64 literal"))
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
    let grid = Grid::new(NAME, grid(3, "start")?, grid(4, "end")?, grid(5, "step")?)?;
    Ok(Lanes::new(phi, grid))
}

impl AggregateUDFImpl for HistogramQuantile {
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
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "buckets"),
            DataType::List(state_item()),
            false,
        ))])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buckets(pairs: &[(f64, f64)]) -> Vec<Bucket> {
        pairs
            .iter()
            .map(|(upper_bound, count)| Bucket {
                upper_bound: *upper_bound,
                count: *count,
            })
            .collect()
    }

    const INF: f64 = f64::INFINITY;

    /// The interpolation upstream documents: linear inside the bucket
    /// the rank falls in, with a natural lower bound of zero under the
    /// lowest one.
    #[test]
    fn a_quantile_interpolates_inside_its_bucket() {
        let b = buckets(&[(1.0, 1.0), (2.0, 2.0), (INF, 3.0)]);
        // Three observations: the median's rank is 1.5, inside (1, 2].
        assert_eq!(bucket_quantile(0.5, b.clone()), 1.5);
        // A rank inside the lowest bucket interpolates from 0.
        assert_eq!(bucket_quantile(0.25, b.clone()), 0.75);
        // And one in the +Inf bucket answers with the bound below it,
        // since +Inf is not a useful number to report.
        assert_eq!(bucket_quantile(1.0, b), 2.0);
    }

    /// A lowest bucket at or below zero has nothing to interpolate
    /// from, so its own bound is the answer.
    #[test]
    fn a_non_positive_lowest_bucket_is_its_own_answer() {
        let b = buckets(&[(-1.0, 2.0), (0.0, 4.0), (INF, 4.0)]);
        assert_eq!(bucket_quantile(0.25, b.clone()), -1.0);
        assert_eq!(bucket_quantile(0.5, b), -1.0);
    }

    /// Every case upstream's doc comment lists as special.
    #[test]
    fn the_special_cases_answer_as_upstream_documents_them() {
        let b = buckets(&[(1.0, 1.0), (INF, 2.0)]);
        assert!(bucket_quantile(f64::NAN, b.clone()).is_nan());
        assert_eq!(bucket_quantile(-0.1, b.clone()), f64::NEG_INFINITY);
        assert_eq!(bucket_quantile(1.1, b.clone()), INF);
        // Fewer than two buckets, no +Inf bucket, and no observations.
        assert!(bucket_quantile(0.5, buckets(&[(INF, 1.0)])).is_nan());
        assert!(bucket_quantile(0.5, buckets(&[(1.0, 1.0), (2.0, 2.0)])).is_nan());
        assert!(bucket_quantile(0.5, buckets(&[(1.0, 0.0), (INF, 0.0)])).is_nan());
        assert!(bucket_quantile(0.5, Vec::new()).is_nan());
    }

    /// `coalesceBuckets` where this engine does it: two series whose
    /// `le` spells one bound are summed into one lane, so the quantile
    /// never sees the bound twice.
    #[test]
    fn two_spellings_of_one_bound_are_one_bucket() {
        let grid = Grid::new(NAME, 0, 0, 1000).unwrap();
        let mut lanes = Lanes::new(0.5, grid);
        // `le="1"` and `le="1.0"`, one observation each.
        lanes.add(1.0, &[0], &[1.0]).unwrap();
        lanes.add(1.0, &[0], &[1.0]).unwrap();
        lanes.add(INF, &[0], &[4.0]).unwrap();
        assert_eq!(lanes.at(0), buckets(&[(1.0, 2.0), (INF, 4.0)]));
        // Two of the four observations are at or below 1, so the median
        // sits exactly on that shared bound.
        assert_eq!(bucket_quantile(0.5, lanes.at(0)), 1.0);
    }

    /// A NaN bound sorts nowhere, so `+Inf` is still last and the
    /// histogram still answers. `total_cmp` would put NaN past `+Inf`
    /// and turn this into the no-`+Inf`-bucket NaN.
    #[test]
    fn a_nan_bound_does_not_displace_the_infinity_bucket() {
        let b = buckets(&[(1.0, 1.0), (f64::NAN, 1.0), (INF, 2.0)]);
        assert_eq!(bucket_quantile(0.5, b), 1.0);
    }

    /// A count that goes down is pulled back up, and a difference too
    /// small to be real is flattened rather than forced.
    #[test]
    fn a_decreasing_count_is_forced_upwards() {
        let mut b = buckets(&[(1.0, 10.0), (2.0, 5.0), (INF, 10.0)]);
        ensure_monotonic(&mut b);
        assert_eq!(b[1].count, 10.0);

        let mut tiny = buckets(&[(1.0, 1.0), (2.0, 1.0 + 1e-15), (INF, 2.0)]);
        ensure_monotonic(&mut tiny);
        assert_eq!(tiny[1].count, 1.0, "a rounding artifact is not a rise");
    }

    /// `almost.Equal`'s own rules, including the two NaNs.
    #[test]
    fn almost_equal_is_upstreams_relative_comparison() {
        assert!(almost_equal(1.0, 1.0, SMALL_DELTA_TOLERANCE));
        assert!(almost_equal(f64::NAN, f64::NAN, SMALL_DELTA_TOLERANCE));
        assert!(!almost_equal(1.0, 2.0, SMALL_DELTA_TOLERANCE));
        assert!(!almost_equal(0.0, 1e-300, SMALL_DELTA_TOLERANCE));
        let stale = f64::from_bits(crate::selector::STALE_NAN_BITS);
        assert!(almost_equal(stale, stale, SMALL_DELTA_TOLERANCE));
        assert!(!almost_equal(stale, f64::NAN, SMALL_DELTA_TOLERANCE));
    }
}
