//! The vector selector as a DataFusion grouped aggregate function.
//!
//! `promql_vector_selector(samples, start, end, step, lookback, offset, at)`
//! grouped by `labels` folds one series' chunk rows, in arrival order, into
//! that series' values on the step grid: for every step, the most recent
//! sample no older than `lookback`, stamped with the step's timestamp.
//!
//! An aggregate, because a row is a chunk, not a series: only an aggregate
//! sees a group's rows in sequence and keeps state between them. Keeping the
//! parameters as literal arguments rather than state on the function means
//! the plan serializes with no custom codec and two selectors with different
//! parameters are never taken for one.
//!
//! The names follow Prometheus's `promql/engine.go`, where a
//! `VectorSelector` expands the series set and runs `evalSeries` over
//! `vectorSelectorSingle`: the expansion is [`crate::source::SelectorTable`],
//! `EvalSeries` is the accumulator both the selector and the range
//! functions run in, and `advance_selector` is one series' walk across the
//! grid.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, Expr, GroupsAccumulator, Signature,
    Volatility,
};

use crate::buffer::{BufferedSeriesIterator, Kernel};
use crate::params::Params;
use crate::series::{self, SamplesBuilder};
use crate::source::source_error;

pub const NAME: &str = "promql_vector_selector";

/// Prometheus's staleness marker: a NaN with this exact payload. It must
/// be compared by bits, since every NaN compares unequal to everything.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

pub fn is_stale(v: f64) -> bool {
    v.to_bits() == STALE_NAN_BITS
}

/// `vectorSelectorSingle` for every step the buffered samples can answer
/// for good: those whose lookup time is at or before `last_t`, since a
/// later sample is later than that.
///
/// Stale markers stay in the buffer: one that is the latest sample hides
/// the series, which dropping it would undo.
pub(crate) fn advance_selector(it: &mut BufferedSeriesIterator, out: &mut SamplesBuilder) {
    let Some(last_t) = it.last_t else {
        return;
    };
    let p = it.params;
    let steps = it.steps();

    // `@` pins the lookup, so the answer is one value repeated across the
    // grid. The buffer holds only the pinned window's latest sample.
    if let Some(at) = p.at_ms {
        if it.next_step >= steps || at - p.offset_ms > last_t {
            return;
        }
        if let Some(&v) = it.vs.last() {
            if !is_stale(v) {
                for step in p.steps() {
                    out.push(step, v);
                }
            }
        }
        it.next_step = steps;
        return;
    }

    let end = it.base + it.ts.len();
    while it.next_step < steps {
        let step = it.step_at(it.next_step);
        let ref_time = step - p.offset_ms;
        if ref_time > last_t {
            break;
        }
        while it.hi < end && it.ts[it.hi - it.base] <= ref_time {
            it.hi += 1;
        }
        if it.hi > it.base {
            let i = it.hi - 1 - it.base;
            let (t, v) = (it.ts[i], it.vs[i]);
            if t > ref_time - p.window_ms && !is_stale(v) {
                out.push(step, v);
            }
        }
        it.next_step += 1;
    }
    // The candidate for the next step is the last sample read, or a later one.
    it.lo = it.hi.saturating_sub(1).max(it.base);
}

/// `evalSeries` as a DataFusion accumulator, one per selector or range
/// function per partition, grouped by the whole `labels` struct.
///
/// It relies on a series' rows arriving consecutively: that is what lets one
/// [`BufferedSeriesIterator`] serve every group, and it is what DataFusion's
/// Sorted mode, one open group at a time, needs to bound memory. A group
/// seen again after a later one opened is an error, not a second fold,
/// because folding it would answer from half a series.
pub(crate) struct EvalSeries {
    series: BufferedSeriesIterator,
    out: SamplesBuilder,
    /// Group index of the open series, always `finished` when set.
    open: Option<usize>,
    /// Rows finished in `out`, all before `open`.
    finished: usize,
    /// The groups DataFusion has handed out, so an emit can give trailing
    /// groups that only had null or filtered rows their empty row.
    groups: usize,
}

impl EvalSeries {
    pub(crate) fn new(kernel: Kernel, params: Params) -> Self {
        Self {
            series: BufferedSeriesIterator::new(kernel, params),
            out: SamplesBuilder::default(),
            open: None,
            finished: 0,
            groups: 0,
        }
    }

    /// Finishes every row before group `g`: the open series, then an empty
    /// row for each group whose rows were all null, filtered or empty.
    fn close_until(&mut self, g: usize) {
        if self.open.take().is_some() {
            self.series.close(&mut self.out);
            self.finished += 1;
        }
        while self.finished < g {
            self.out.finish_row();
            self.finished += 1;
        }
    }

    /// In Sorted mode `First(n)` never reaches the open group. In any other
    /// mode it may, and the group is closed with what it has; a row for it
    /// afterwards is a new group DataFusion cannot tell apart.
    fn emit(&mut self, emit_to: EmitTo) -> ListArray {
        let n = match emit_to {
            EmitTo::All => self
                .groups
                .max(self.finished + usize::from(self.open.is_some())),
            EmitTo::First(n) => n,
        };
        if self.open.is_none_or(|o| o < n) {
            self.close_until(n);
        }
        let list = self.out.take_first(n);
        self.finished -= n;
        self.open = self.open.map(|o| o - n);
        self.groups = self.groups.saturating_sub(n);
        list
    }
}

impl std::fmt::Debug for EvalSeries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvalSeries")
            .field("open", &self.open)
            .field("finished", &self.finished)
            .field("groups", &self.groups)
            .finish_non_exhaustive()
    }
}

/// Rows a `FILTER` clause dropped, and rows with no series at all,
/// contribute nothing to their group.
fn skipped(list: &ListArray, filter: Option<&BooleanArray>, row: usize) -> bool {
    list.is_null(row) || filter.is_some_and(|f| f.is_null(row) || !f.value(row))
}

impl GroupsAccumulator for EvalSeries {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.groups = self.groups.max(total_num_groups);
        let list = values[0].as_list::<i32>();
        let (ts, vs) = series::sample_slices(list.values().as_struct());
        let offsets = list.offsets();
        for (row, &g) in group_indices.iter().enumerate() {
            let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
            if a == b || skipped(list, opt_filter, row) {
                continue;
            }
            if self.open != Some(g) {
                // The group key never reaches an accumulator, so unlike
                // `SeriesSetExec` this cannot name the series. It is the
                // backstop for a plan that reorders rows above that check.
                if g < self.finished {
                    return Err(source_error(format!(
                        "{NAME}: rows of a series are not consecutive: group {g} came back \
                         after group {} opened",
                        self.finished
                    )));
                }
                self.close_until(g);
                self.open = Some(g);
            }
            self.series.push(&ts[a..b], &vs[a..b], &mut self.out);
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        Ok(Arc::new(self.emit(emit_to)))
    }

    /// The finished series itself: a partial holding a whole series has
    /// nothing left to combine, so the final side only has to pass it on.
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![Arc::new(self.emit(emit_to))])
    }

    /// Group indices are handed out in first-seen order, so a state for a
    /// group at or before the last one merged is that series' second state,
    /// from a second partition, and no bitset is needed to see it.
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.groups = self.groups.max(total_num_groups);
        let list = values[0].as_list::<i32>();
        let (ts, vs) = series::sample_slices(list.values().as_struct());
        let offsets = list.offsets();
        for (row, &g) in group_indices.iter().enumerate() {
            if skipped(list, opt_filter, row) {
                continue;
            }
            if g < self.finished || self.open.is_some() {
                return Err(source_error(format!(
                    "{NAME}: a series arrived from more than one partition (group {g})"
                )));
            }
            self.close_until(g);
            for i in offsets[row] as usize..offsets[row + 1] as usize {
                self.out.push(ts[i], vs[i]);
            }
            self.out.finish_row();
            self.finished += 1;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.series.size() + self.out.size()
    }
}

/// The DataFusion function. Stateless: every parameter is an argument.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorSelector {
    signature: Signature,
}

impl Default for VectorSelector {
    fn default() -> Self {
        let mut args = vec![series::samples_type()];
        args.extend(std::iter::repeat_n(DataType::Int64, 6));
        Self {
            signature: Signature::exact(args, Volatility::Immutable),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(VectorSelector::default())
}

/// `promql_vector_selector(samples, start, end, step, lookback, offset, at)`.
pub fn call(samples: Expr, p: &Params) -> Expr {
    udaf().call(vec![
        samples,
        lit(p.start_ms),
        lit(p.end_ms),
        lit(p.step_ms),
        lit(p.window_ms),
        lit(p.offset_ms),
        lit(ScalarValue::Int64(p.at_ms)),
    ])
}

/// What a group with no rows yields: no samples.
pub(crate) fn empty_samples() -> ScalarValue {
    let mut b = SamplesBuilder::default();
    b.finish_row();
    ScalarValue::List(Arc::new(b.take_all()))
}

impl AggregateUDFImpl for VectorSelector {
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
        Ok(Box::new(EvalSeries::new(
            Kernel::Selector,
            Params::from_literals(args.exprs, 1)?,
        )))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(empty_samples())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::EngineError;
    use datafusion::logical_expr::EmitTo;

    const M: i64 = 60_000;

    fn run(ts: &[i64], vs: &[f64], p: Params) -> Vec<(i64, f64)> {
        select(&[(ts, vs)], p)
    }

    /// One series pushed as `chunks`, then closed.
    fn select(chunks: &[(&[i64], &[f64])], p: Params) -> Vec<(i64, f64)> {
        let mut it = BufferedSeriesIterator::new(Kernel::Selector, p);
        let mut out = SamplesBuilder::default();
        for (ts, vs) in chunks {
            it.push(ts, vs, &mut out);
        }
        it.close(&mut out);
        let mut rows = rows(&out.take_all());
        assert_eq!(rows.len(), 1);
        rows.pop().unwrap()
    }

    fn rows(list: &ListArray) -> Vec<Vec<(i64, f64)>> {
        (0..list.len())
            .map(|r| {
                let row = list.value(r);
                let (ts, vs) = series::sample_slices(row.as_struct());
                ts.iter().copied().zip(vs.iter().copied()).collect()
            })
            .collect()
    }

    fn params() -> Params {
        Params {
            start_ms: 0,
            end_ms: 10 * M,
            step_ms: M,
            window_ms: 5 * M,
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

    /// Gaps longer than the lookback, a stale marker, a NaN and repeated
    /// values: every branch `advance_selector` has, across the grids below.
    fn a_rough_series() -> (Vec<i64>, Vec<f64>) {
        let stale = f64::from_bits(STALE_NAN_BITS);
        [
            (0, 1.0),
            (30_000, 2.0),
            (90_000, 2.0),
            (120_000, f64::NAN),
            (150_000, 5.0),
            (180_000, stale),
            (240_000, 4.0),
            (270_000, -0.0),
            (300_000, 9.0),
            (900_000, 3.0),
            (930_000, stale),
            (960_000, 3.0),
            (1_020_000, 2.0),
        ]
        .into_iter()
        .unzip()
    }

    fn grids() -> Vec<Params> {
        let base = Params {
            start_ms: 0,
            end_ms: 20 * M,
            step_ms: 30_000,
            window_ms: 2 * M,
            offset_ms: 0,
            at_ms: None,
        };
        vec![
            base,
            Params {
                step_ms: 7_000,
                ..base
            },
            Params {
                start_ms: 16 * M,
                ..base
            },
            Params {
                offset_ms: 90_000,
                ..base
            },
            Params {
                offset_ms: -M,
                ..base
            },
            Params {
                window_ms: 5 * M,
                ..base
            },
            Params {
                at_ms: Some(150_000),
                ..base
            },
            Params {
                at_ms: Some(160_000),
                offset_ms: -M,
                ..base
            },
            Params {
                at_ms: Some(185_000),
                ..base
            },
            Params {
                at_ms: Some(20 * M),
                ..base
            },
        ]
    }

    fn close_enough(a: &[(i64, f64)], b: &[(i64, f64)]) -> bool {
        a.len() == b.len()
            && a.iter()
                .zip(b)
                .all(|(x, y)| x.0 == y.0 && x.1.to_bits() == y.1.to_bits())
    }

    #[test]
    fn chunk_splits_select_what_one_row_selects() {
        let (ts, vs) = a_rough_series();
        for p in grids() {
            let expected = run(&ts, &vs, p);
            for k in 1..=ts.len() {
                let chunks: Vec<(&[i64], &[f64])> = ts.chunks(k).zip(vs.chunks(k)).collect();
                let got = select(&chunks, p);
                assert!(
                    close_enough(&got, &expected),
                    "{p:?}, chunks of {k}: {got:?} != {expected:?}"
                );
            }
        }
    }

    #[test]
    fn chunks_repeating_the_previous_tail_select_the_same() {
        let (ts, vs) = a_rough_series();
        for p in grids() {
            let expected = run(&ts, &vs, p);
            for k in 2..=ts.len() {
                // Each chunk starts one sample back, a duplicate of the
                // previous chunk's last one.
                let chunks: Vec<(&[i64], &[f64])> = (0..ts.len())
                    .step_by(k - 1)
                    .map(|a| {
                        let b = (a + k).min(ts.len());
                        (&ts[a..b], &vs[a..b])
                    })
                    .collect();
                let got = select(&chunks, p);
                assert!(
                    close_enough(&got, &expected),
                    "{p:?}, overlapping chunks of {k}: {got:?} != {expected:?}"
                );
            }
        }
    }

    /// A samples column, one row per entry.
    fn column(rows: &[&[(i64, f64)]]) -> ArrayRef {
        let mut b = SamplesBuilder::default();
        for row in rows {
            for &(t, v) in *row {
                b.push(t, v);
            }
            b.finish_row();
        }
        Arc::new(b.take_all())
    }

    fn accumulator() -> EvalSeries {
        EvalSeries::new(
            Kernel::Selector,
            Params {
                end_ms: 2 * M,
                ..params()
            },
        )
    }

    fn emitted(a: ArrayRef) -> Vec<Vec<(i64, f64)>> {
        rows(a.as_list::<i32>())
    }

    #[test]
    fn a_series_whose_rows_are_not_consecutive_is_an_error() {
        let mut acc = accumulator();
        let rows = column(&[&[(0, 1.0)], &[(0, 2.0)], &[(M, 3.0)]]);
        let err = acc.update_batch(&[rows], &[0, 1, 0], None, 2).unwrap_err();
        assert!(
            matches!(EngineError::from(err), EngineError::Source(m) if m.contains("not consecutive"))
        );
    }

    #[test]
    fn a_series_carries_across_batches_and_emits_once_closed() {
        let mut acc = accumulator();
        acc.update_batch(&[column(&[&[(0, 1.0)], &[(0, 5.0)]])], &[0, 1], None, 2)
            .unwrap();
        // Group 1 is open; only group 0 is closed.
        assert_eq!(
            emitted(acc.evaluate(EmitTo::First(1)).unwrap()),
            vec![vec![(0, 1.0), (M, 1.0), (2 * M, 1.0)]]
        );
        // The open series is now group 0.
        acc.update_batch(&[column(&[&[(M, 6.0)]])], &[0], None, 1)
            .unwrap();
        assert_eq!(
            emitted(acc.evaluate(EmitTo::All).unwrap()),
            vec![vec![(0, 5.0), (M, 6.0), (2 * M, 6.0)]]
        );
    }

    #[test]
    fn a_group_with_nothing_to_select_is_an_empty_row() {
        let mut acc = accumulator();
        let rows = column(&[&[(0, 1.0)], &[(0, 2.0)], &[], &[(0, 4.0)]]);
        let skip = BooleanArray::from(vec![true, false, true, true]);
        acc.update_batch(&[rows], &[0, 1, 2, 3], Some(&skip), 5)
            .unwrap();
        let out = emitted(acc.evaluate(EmitTo::All).unwrap());
        assert_eq!(out.len(), 5);
        assert!(out[1].is_empty() && out[2].is_empty() && out[4].is_empty());
        assert_eq!(out[3].len(), 3);
    }

    #[test]
    fn the_state_is_the_finished_series() {
        let mut acc = accumulator();
        acc.update_batch(&[column(&[&[(0, 1.0)], &[(0, 2.0)]])], &[0, 1], None, 2)
            .unwrap();
        let state = acc.state(EmitTo::All).unwrap();
        assert_eq!(state.len(), 1);

        let mut last = accumulator();
        last.merge_batch(&state, &[0, 1], None, 2).unwrap();
        assert_eq!(emitted(last.evaluate(EmitTo::All).unwrap()).len(), 2);

        // A second state for one series means it crossed partitions.
        let mut last = accumulator();
        last.merge_batch(&state, &[0, 1], None, 2).unwrap();
        let err = last.merge_batch(&state, &[1, 2], None, 3).unwrap_err();
        assert!(
            matches!(EngineError::from(err), EngineError::Source(m) if m.contains("partition"))
        );
    }
}
