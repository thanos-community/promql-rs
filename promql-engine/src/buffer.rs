//! One series' samples as they cross chunk rows: Prometheus's
//! `storage.BufferedSeriesIterator` (`storage/buffer.go`), fed row by row
//! instead of pulling from a chunk iterator.
//!
//! What a later chunk row's steps still read is copied into one contiguous
//! buffer rather than kept as a slice of its batch. A retained slice would
//! pin the whole batch's child buffers, other series' samples included, and
//! every kernel indexes one contiguous slice; the copy replaces the
//! staleness filter the range kernel pays anyway. A row that arrives with
//! the buffer empty, every row of a store that does not chunk, is walked
//! where it lies and only its tail is copied: copying it first would copy
//! the whole series.
//!
//! Steps are evaluated as soon as their window can no longer change, not at
//! series close. Otherwise the buffer would hold the whole series, which is
//! 172,800 samples for a 30-day range at 15 s; evaluated eagerly it holds a
//! window. The output row is still only visible once the series closes.

use crate::params::{step_count, Params};
use crate::range::{advance_range, Func, Sweep};
use crate::selector::{advance_selector, is_stale};
use crate::series::SamplesBuilder;

pub(crate) enum Kernel {
    Selector,
    Range(Func),
}

pub(crate) struct BufferedSeriesIterator {
    pub(crate) kernel: Kernel,
    pub(crate) sweep: Option<Sweep>,
    pub(crate) params: Params,
    pub(crate) ts: Vec<i64>,
    pub(crate) vs: Vec<f64>,
    /// Absolute ordinal `i` lives at `[i - base]`.
    pub(crate) base: usize,
    /// Absolute ordinals: the first sample a later step can still read, and
    /// one past the last sample the current step has read.
    pub(crate) lo: usize,
    pub(crate) hi: usize,
    /// Index into the step grid of the first step not yet evaluated.
    pub(crate) next_step: i128,
    /// Overlap rule; stale markers count.
    pub(crate) last_t: Option<i64>,
}

impl BufferedSeriesIterator {
    pub(crate) fn new(kernel: Kernel, params: Params) -> Self {
        let sweep = match kernel {
            Kernel::Range(func) => Sweep::new(func),
            Kernel::Selector => None,
        };
        Self {
            kernel,
            sweep,
            params,
            ts: Vec::new(),
            vs: Vec::new(),
            base: 0,
            lo: 0,
            hi: 0,
            next_step: 0,
            last_t: None,
        }
    }

    /// Folds one chunk row. Evaluates into `out`'s open row every step whose
    /// window can no longer change, then trims the buffer below `lo`.
    ///
    /// Chunks of one series must ascend by first timestamp; `SeriesSetExec`
    /// checks that with the labels at hand for the message, so this cannot
    /// fail.
    pub(crate) fn push(&mut self, ts: &[i64], vs: &[f64], out: &mut SamplesBuilder) {
        debug_assert_eq!(ts.len(), vs.len());
        let Some(&last) = ts.last() else {
            return;
        };
        if self.skip_chunk(ts) {
            self.last_t = self.last_t.max(Some(last));
            return;
        }
        if self.ts.is_empty() && self.in_place(vs) {
            self.push_in_place(ts, vs, out);
            return;
        }
        self.append(ts, vs);
        self.advance_buffered(false, out);
        self.reduce_delta();
    }

    /// Evaluates the remaining steps, finishes `out`'s row, resets for the
    /// next series while keeping capacity.
    pub(crate) fn close(&mut self, out: &mut SamplesBuilder) {
        // With nothing buffered, no step left has a sample to read.
        if !self.ts.is_empty() {
            // Every sample is in: no window can change any more.
            self.last_t = Some(i64::MAX);
            self.advance_buffered(true, out);
        }
        out.finish_row();
        self.ts.clear();
        self.vs.clear();
        self.base = 0;
        self.lo = 0;
        self.hi = 0;
        self.next_step = 0;
        self.last_t = None;
        if let Some(sweep) = self.sweep.as_mut() {
            sweep.reset();
        }
    }

    pub(crate) fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.ts.capacity() * std::mem::size_of::<i64>()
            + self.vs.capacity() * std::mem::size_of::<f64>()
    }

    pub(crate) fn steps(&self) -> i128 {
        let p = &self.params;
        step_count(p.start_ms, p.end_ms, p.step_ms)
    }

    /// The timestamp of step `i` on the grid.
    pub(crate) fn step_at(&self, i: i128) -> i64 {
        (i128::from(self.params.start_ms) + i * i128::from(self.params.step_ms)) as i64
    }

    /// `ts`/`vs` are the series' samples from ordinal `base` on, the
    /// buffer's or a chunk's.
    fn advance(&mut self, ts: &[i64], vs: &[f64], done: bool, out: &mut SamplesBuilder) {
        match self.kernel {
            Kernel::Selector => advance_selector(self, ts, vs, out),
            Kernel::Range(func) => advance_range(self, ts, vs, func, done, |t, v| out.push(t, v)),
        }
    }

    /// The kernels take the samples as slices so that a chunk can be walked
    /// in place; the buffer is lent to them for the call, which moves no
    /// samples.
    fn advance_buffered(&mut self, done: bool, out: &mut SamplesBuilder) {
        let (ts, vs) = (std::mem::take(&mut self.ts), std::mem::take(&mut self.vs));
        self.advance(&ts, &vs, done, out);
        (self.ts, self.vs) = (ts, vs);
    }

    /// Whether a chunk can be walked as it arrived. A pinned window is
    /// trimmed on copy, and the range kernels must not see a stale marker,
    /// so either goes through [`append`](Self::append).
    fn in_place(&self, vs: &[f64]) -> bool {
        self.params.at_ms.is_none()
            && (matches!(self.kernel, Kernel::Selector) || !vs.iter().any(|v| is_stale(*v)))
    }

    /// Walks the chunk's steps off the chunk itself, then copies what a
    /// later step still reads. With nothing buffered the chunk's first
    /// sample is ordinal `base`, so the kernels' ordinals, a sweep's
    /// included, hold across the copy exactly as across `reduce_delta`.
    fn push_in_place(&mut self, ts: &[i64], vs: &[f64], out: &mut SamplesBuilder) {
        debug_assert!(self.lo == self.base && self.hi == self.base);
        let from = self
            .last_t
            .map_or(0, |last| ts.partition_point(|t| *t <= last));
        let (ts, vs) = (&ts[from..], &vs[from..]);
        let Some(&last) = ts.last() else {
            return;
        };
        self.last_t = Some(last);
        self.advance(ts, vs, false, out);
        let keep = self.lo - self.base;
        self.ts.extend_from_slice(&ts[keep..]);
        self.vs.extend_from_slice(&vs[keep..]);
        self.base = self.lo;
    }

    /// A chunk that ends at or before every window's start, or arrives after
    /// every step is answered, can only move the overlap reference. Two O(1)
    /// reads decide it, so its samples are never walked.
    fn skip_chunk(&self, ts: &[i64]) -> bool {
        let p = &self.params;
        let first_end = p.at_ms.unwrap_or(p.start_ms);
        let first_start = first_end
            .saturating_sub(p.offset_ms)
            .saturating_sub(p.window_ms);
        ts[ts.len() - 1] <= first_start || self.next_step >= self.steps()
    }

    /// Copies the chunk's samples that can still matter.
    ///
    /// The first row wins an overlap, as in Thanos's `chunkSeriesIterator`:
    /// a sample at or before `last_t` is dropped. A stale marker claims its
    /// timestamp before the range kernel filters it out, the way a store's
    /// merge dedups before PromQL sees staleness.
    fn append(&mut self, ts: &[i64], vs: &[f64]) {
        let from = self
            .last_t
            .map_or(0, |last| ts.partition_point(|t| *t <= last));
        let (ts, vs) = (&ts[from..], &vs[from..]);
        let Some(&last) = ts.last() else {
            return;
        };
        self.last_t = Some(last);

        let p = &self.params;
        let (ts, vs) = match p.at_ms {
            // A pinned window never moves, so what lies outside it is dead
            // on arrival.
            Some(at) => {
                let end = at - p.offset_ms;
                let a = ts.partition_point(|t| *t <= end - p.window_ms);
                let b = ts.partition_point(|t| *t <= end);
                (&ts[a..b], &vs[a..b])
            }
            None => (ts, vs),
        };
        match self.kernel {
            // Pinned, the selector reads only the latest sample of its window.
            Kernel::Selector if p.at_ms.is_some() => {
                if let (Some(&t), Some(&v)) = (ts.last(), vs.last()) {
                    self.ts.clear();
                    self.vs.clear();
                    self.ts.push(t);
                    self.vs.push(v);
                }
            }
            Kernel::Selector => {
                self.ts.extend_from_slice(ts);
                self.vs.extend_from_slice(vs);
            }
            Kernel::Range(_) => {
                for (&t, &v) in ts.iter().zip(vs) {
                    if !is_stale(v) {
                        self.ts.push(t);
                        self.vs.push(v);
                    }
                }
            }
        }
    }

    /// `ReduceDelta`: drop what no later step reads, once it is at least
    /// half the buffer, so the shift is amortised over the samples it frees.
    fn reduce_delta(&mut self) {
        let cut = self.lo - self.base;
        if cut > 0 && cut >= self.ts.len() / 2 {
            self.ts.drain(..cut);
            self.vs.drain(..cut);
            self.base = self.lo;
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray, ListArray};

    use super::*;
    use crate::range::Func;
    use crate::selector::STALE_NAN_BITS;
    use crate::series;

    const M: i64 = 60_000;

    fn params() -> Params {
        Params {
            start_ms: 0,
            end_ms: 4 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        }
    }

    /// Every finished row of `list`, as `(timestamp, value)` pairs.
    fn rows(list: &ListArray) -> Vec<Vec<(i64, f64)>> {
        (0..list.len())
            .map(|r| {
                let row = list.value(r);
                let (ts, vs) = series::sample_slices(row.as_struct());
                ts.iter().copied().zip(vs.iter().copied()).collect()
            })
            .collect()
    }

    /// One series pushed as `chunks`, then closed.
    fn select(p: Params, chunks: &[(&[i64], &[f64])]) -> Vec<(i64, f64)> {
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

    #[test]
    fn a_window_split_across_chunks_selects_as_one() {
        let out = select(
            params(),
            &[
                (&[0, 30_000], &[1.0, 2.0]),
                (&[90_000, 150_000], &[3.0, 4.0]),
            ],
        );
        assert_eq!(
            out,
            vec![(0, 1.0), (M, 2.0), (2 * M, 3.0), (3 * M, 4.0), (4 * M, 4.0)]
        );
    }

    #[test]
    fn a_step_is_answered_before_the_series_closes() {
        let mut it = BufferedSeriesIterator::new(Kernel::Selector, params());
        let mut out = SamplesBuilder::default();
        it.push(&[0, 90_000], &[1.0, 2.0], &mut out);
        // 0 and 1m can no longer change once 90s is seen; 2m can.
        assert_eq!(it.next_step, 2);
        it.push(&[150_000], &[3.0], &mut out);
        assert_eq!(it.next_step, 3);
        it.close(&mut out);
        assert_eq!(
            rows(&out.take_all()),
            vec![vec![
                (0, 1.0),
                (M, 1.0),
                (2 * M, 2.0),
                (3 * M, 3.0),
                (4 * M, 3.0)
            ]]
        );
    }

    #[test]
    fn an_overlapping_chunk_loses_to_the_first() {
        let out = select(
            params(),
            &[
                (&[0, M, 2 * M], &[1.0, 2.0, 3.0]),
                (&[M, 90_000, 3 * M], &[9.0, 9.0, 4.0]),
            ],
        );
        assert_eq!(
            out,
            vec![(0, 1.0), (M, 2.0), (2 * M, 3.0), (3 * M, 4.0), (4 * M, 4.0)]
        );
    }

    #[test]
    fn a_stale_marker_claims_its_timestamp() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        let out = select(
            params(),
            &[(&[0, M], &[1.0, stale]), (&[M, 2 * M], &[5.0, 6.0])],
        );
        assert_eq!(
            out,
            vec![(0, 1.0), (2 * M, 6.0), (3 * M, 6.0), (4 * M, 6.0)]
        );
    }

    #[test]
    fn a_chunk_before_every_window_is_never_buffered() {
        let p = Params {
            start_ms: 10 * M,
            end_ms: 12 * M,
            ..params()
        };
        let mut it = BufferedSeriesIterator::new(Kernel::Selector, p);
        let mut out = SamplesBuilder::default();
        it.push(&[0, 5 * M], &[1.0, 2.0], &mut out);
        assert!(it.ts.is_empty());
        assert_eq!(it.last_t, Some(5 * M));
        // Still the overlap reference: 5m again is a duplicate.
        it.push(&[5 * M, 6 * M], &[7.0, 3.0], &mut out);
        it.close(&mut out);
        assert_eq!(rows(&out.take_all()), vec![vec![(10 * M, 3.0)]],);
    }

    #[test]
    fn at_pins_the_lookup_across_chunks() {
        let p = Params {
            at_ms: Some(M + 1),
            ..params()
        };
        let out = select(p, &[(&[0], &[1.0]), (&[M], &[2.0]), (&[2 * M], &[3.0])]);
        assert_eq!(
            out,
            vec![(0, 2.0), (M, 2.0), (2 * M, 2.0), (3 * M, 2.0), (4 * M, 2.0)]
        );
    }

    #[test]
    fn close_starts_the_next_series_from_nothing() {
        let mut it = BufferedSeriesIterator::new(Kernel::Selector, params());
        let mut out = SamplesBuilder::default();
        it.push(&[0, 3 * M], &[1.0, 2.0], &mut out);
        it.close(&mut out);
        // Earlier than the first series' last sample: not an overlap.
        it.push(&[2 * M], &[5.0], &mut out);
        it.close(&mut out);
        it.close(&mut out);
        assert_eq!(
            rows(&out.take_all()),
            vec![
                vec![(0, 1.0), (M, 1.0), (2 * M, 1.0), (3 * M, 2.0), (4 * M, 2.0)],
                vec![(2 * M, 5.0), (3 * M, 5.0), (4 * M, 5.0)],
                vec![],
            ]
        );
    }

    #[test]
    fn the_buffer_holds_the_window_not_the_series() {
        let mut it = BufferedSeriesIterator::new(
            Kernel::Selector,
            Params {
                end_ms: 1000 * M,
                ..params()
            },
        );
        let mut out = SamplesBuilder::default();
        for i in 0..1000 {
            it.push(&[i * M], &[i as f64], &mut out);
            assert!(it.ts.len() <= 4, "{} samples buffered at {i}", it.ts.len());
        }
        it.close(&mut out);
        assert_eq!(rows(&out.take_all())[0].len(), 1001);
    }

    /// The kernels step by addition, and the step after the last one lies
    /// past `i64::MAX` here.
    #[test]
    fn a_grid_ending_at_the_end_of_time_does_not_overflow() {
        let p = Params {
            start_ms: i64::MAX - 2 * M,
            end_ms: i64::MAX,
            ..params()
        };
        let (ts, vs) = ([i64::MAX - 2 * M, i64::MAX - M], [1.0, 2.0]);
        for func in kernels() {
            let got = eval(func, p, &[(&ts, &vs)]);
            assert!(!got.is_empty(), "{}", name(func));
            assert!(got.iter().all(|(t, _)| *t >= p.start_ms), "{got:?}");
        }
    }

    /// A 30-day series is one row from a store that does not chunk it, so
    /// the buffer must not take a copy of a chunk to walk it.
    #[test]
    fn a_chunk_is_walked_in_place_and_only_its_tail_copied() {
        let p = Params {
            end_ms: 1000 * M,
            ..params()
        };
        let (ts, vs): (Vec<i64>, Vec<f64>) = (0..1000).map(|i| (i * M, i as f64)).unzip();
        for func in kernels() {
            let mut it = BufferedSeriesIterator::new(kernel(func), p);
            let mut out = SamplesBuilder::default();
            it.push(&ts, &vs, &mut out);
            assert!(
                it.ts.capacity() <= 5,
                "{}: {} samples of capacity after one chunk",
                name(func),
                it.ts.capacity()
            );
            // A second chunk with the tail still buffered is appended to it.
            it.push(&[1000 * M], &[1000.0], &mut out);
            it.close(&mut out);
            let whole = eval(
                func,
                p,
                &[(
                    &[ts.as_slice(), &[1000 * M]].concat(),
                    &[vs.as_slice(), &[1000.0]].concat(),
                )],
            );
            assert_bits(&rows(&out.take_all())[0], &whole, &name(func));
        }
    }

    // Range kernels and the chunk_ms sweep: whatever the cut points, a series
    // must answer bit for bit what it answers as one chunk.

    const S: i64 = 1000;

    const FUNCS: [Func; 14] = [
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

    /// `None` is the selector: `Kernel` is not `Copy`, and every sweep below
    /// needs a fresh one per run.
    fn kernel(func: Option<Func>) -> Kernel {
        func.map_or(Kernel::Selector, Kernel::Range)
    }

    fn name(func: Option<Func>) -> &'static str {
        func.map_or("selector", |f| f.as_str())
    }

    /// Every kernel: the selector, then each range function.
    fn kernels() -> impl Iterator<Item = Option<Func>> {
        std::iter::once(None).chain(FUNCS.map(Some))
    }

    /// One series pushed as `chunks`, then closed, through any kernel.
    fn eval(func: Option<Func>, p: Params, chunks: &[(&[i64], &[f64])]) -> Vec<(i64, f64)> {
        let mut it = BufferedSeriesIterator::new(kernel(func), p);
        let mut out = SamplesBuilder::default();
        for (ts, vs) in chunks {
            it.push(ts, vs, &mut out);
        }
        it.close(&mut out);
        let mut rows = rows(&out.take_all());
        assert_eq!(rows.len(), 1);
        rows.pop().unwrap()
    }

    /// Compared by bits: a cut point that changes the order of a float sum
    /// changes the answer, and `1e-9` would not see it.
    fn assert_bits(got: &[(i64, f64)], want: &[(i64, f64)], ctx: &dyn std::fmt::Display) {
        let bits = |s: &[(i64, f64)]| s.iter().map(|(t, v)| (*t, v.to_bits())).collect::<Vec<_>>();
        assert_eq!(
            bits(got),
            bits(want),
            "{ctx}\n got: {got:?}\nwant: {want:?}"
        );
    }

    /// Counter resets, NaN runs, a staleness marker, signed zeros and a gap
    /// wider than the window, as range.rs's suite uses: every branch of
    /// every kernel as a 2m window slides over it.
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
            (900 * S, f64::NAN),
            (930 * S, f64::NAN),
            (960 * S, 3.0),
            (990 * S, 3.0),
            (1020 * S, 2.0),
        ]
        .into_iter()
        .unzip()
    }

    /// Grids that put window edges on samples, between them, pinned before
    /// the data ends and pinned after it.
    fn rough_grids() -> Vec<Params> {
        let plain = Params {
            start_ms: 0,
            end_ms: 20 * M,
            step_ms: 15 * S,
            window_ms: 2 * M,
            offset_ms: 45 * S,
            at_ms: None,
        };
        vec![
            plain,
            Params {
                offset_ms: 0,
                ..plain
            },
            Params {
                window_ms: 5 * M,
                step_ms: M,
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
        ]
    }

    fn split<'a>(ts: &'a [i64], vs: &'a [f64], k: usize) -> Vec<(&'a [i64], &'a [f64])> {
        ts.chunks(k).zip(vs.chunks(k)).collect()
    }

    #[test]
    fn chunk_size_sweep() {
        let (ts, vs) = a_rough_series();
        for p in rough_grids() {
            for func in kernels() {
                let whole = eval(func, p, &[(&ts, &vs)]);
                for k in 1..=ts.len() {
                    let got = eval(func, p, &split(&ts, &vs, k));
                    assert_bits(&got, &whole, &format!("{} k={k} {p:?}", name(func)));
                }
            }
        }
    }

    /// One step at 10m over a 5m window: `(5m, 10m]`.
    fn at_10m() -> Params {
        Params {
            start_ms: 10 * M,
            end_ms: 10 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        }
    }

    #[test]
    fn boundary_on_range_start() {
        let mut it = BufferedSeriesIterator::new(Kernel::Range(Func::CountOverTime), at_10m());
        let mut out = SamplesBuilder::default();
        it.push(&[3 * M, 4 * M, 5 * M], &[1.0, 2.0, 3.0], &mut out);
        // The window is open at its start, so 5m is outside it.
        assert!(it.ts.is_empty(), "buffered {:?}", it.ts);
        it.push(&[6 * M], &[4.0], &mut out);
        it.close(&mut out);
        assert_eq!(rows(&out.take_all()), vec![vec![(10 * M, 1.0)]]);
    }

    #[test]
    fn boundary_on_range_end() {
        let p = Params {
            end_ms: 11 * M,
            ..at_10m()
        };
        let mut it = BufferedSeriesIterator::new(Kernel::Range(Func::CountOverTime), p);
        let mut out = SamplesBuilder::default();
        it.push(&[9 * M, 10 * M], &[1.0, 2.0], &mut out);
        // 10m's window is closed at its end: nothing later can enter it.
        assert_eq!(it.next_step, 1);
        it.push(&[10 * M + 30 * S], &[3.0], &mut out);
        assert_eq!(it.next_step, 1);
        it.push(&[11 * M], &[4.0], &mut out);
        assert_eq!(it.next_step, 2);
        it.close(&mut out);
        assert_eq!(
            rows(&out.take_all()),
            vec![vec![(10 * M, 2.0), (11 * M, 4.0)]]
        );
    }

    #[test]
    fn boundary_on_step_with_duplicate_head() {
        let ts = [6 * M, 7 * M, 8 * M, 9 * M, 10 * M, 11 * M];
        let vs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let p = Params {
            end_ms: 11 * M,
            ..at_10m()
        };
        for func in [Func::CountOverTime, Func::Rate] {
            let whole = eval(Some(func), p, &[(&ts, &vs)]);
            // The next chunk repeats 10m, the first step's range end, with a
            // value that would read as a counter reset.
            let got = eval(
                Some(func),
                p,
                &[(&ts[..5], &vs[..5]), (&[10 * M, 11 * M], &[0.0, 6.0])],
            );
            assert_bits(&got, &whole, &func.as_str());
        }
    }

    #[test]
    fn counter_reset_on_the_chunk_boundary() {
        let ts: Vec<i64> = (0..=10).map(|i| i * 30 * S).collect();
        let vs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        let p = Params {
            start_ms: 5 * M,
            end_ms: 5 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        };
        for func in [Func::Rate, Func::Increase, Func::Resets, Func::Irate] {
            let whole = eval(Some(func), p, &[(&ts, &vs)]);
            // The reset is the first sample of the second chunk, so the
            // value it compares against lives only in the first.
            let got = eval(Some(func), p, &[(&ts[..6], &vs[..6]), (&ts[6..], &vs[6..])]);
            assert_bits(&got, &whole, &func.as_str());
        }
        assert_eq!(
            eval(
                Some(Func::Resets),
                p,
                &[(&ts[..6], &vs[..6]), (&ts[6..], &vs[6..])]
            ),
            vec![(5 * M, 1.0)]
        );
        // Increase over (0, 5m]: 2 → 6, reset, 1 → 5, extrapolated.
        let increase = eval(
            Some(Func::Increase),
            p,
            &[(&ts[..6], &vs[..6]), (&ts[6..], &vs[6..])],
        );
        assert!((increase[0].1 - 10.0).abs() < 1e-9, "{increase:?}");
    }

    /// A.0 and B.0 of docs/engine-chunks.md §1, and the seven rates of §4.
    #[test]
    fn the_engine_chunks_fixture() {
        let p = Params {
            start_ms: 420 * S,
            end_ms: 600 * S,
            step_ms: 30 * S,
            window_ms: 300 * S,
            offset_ms: 0,
            at_ms: None,
        };
        let a0: (Vec<i64>, Vec<f64>) = [300, 330, 360, 390, 420]
            .into_iter()
            .map(|t| t * S)
            .zip([10.0, 11.0, 12.0, 13.0, 14.0])
            .unzip();
        let b0: (Vec<i64>, Vec<f64>) = [450, 480, 510, 540, 570, 600]
            .into_iter()
            .map(|t| t * S)
            .zip([15.0, 16.0, 3.0, 4.0, 5.0, 6.0])
            .unzip();

        let mut it = BufferedSeriesIterator::new(Kernel::Range(Func::Rate), p);
        let mut out = SamplesBuilder::default();
        it.push(&a0.0, &a0.1, &mut out);
        // The lane at 420s closes before B.0 opens.
        assert_eq!(it.next_step, 1);
        it.push(&b0.0, &b0.1, &mut out);
        it.close(&mut out);
        let got = rows(&out.take_all()).pop().unwrap();

        let want = [
            (420, 0.015),
            (450, 0.0183333333333),
            (480, 0.0216666666667),
            (510, 0.0321428571429),
            (540, 0.0354166666667),
            (570, 0.0407407407407),
            (600, 0.0407407407407),
        ];
        assert_eq!(got.len(), want.len(), "{got:?}");
        for ((t, v), (wt, wv)) in got.iter().zip(want) {
            assert_eq!(*t, wt * S);
            assert!((v - wv).abs() < 1e-10, "at {wt}s: {v} vs {wv}");
        }
    }

    #[test]
    fn overlap_keeps_the_first_row() {
        let p = Params {
            start_ms: 3 * M,
            end_ms: 3 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        };
        // Interleaved: the union would be six samples, the first row keeps
        // its three and only 150s of the second survives.
        let chunks: [(&[i64], &[f64]); 2] = [
            (&[0, 60 * S, 120 * S], &[1.0, 2.0, 3.0]),
            (&[30 * S, 90 * S, 150 * S], &[7.0, 8.0, 9.0]),
        ];
        assert_eq!(
            eval(Some(Func::CountOverTime), p, &chunks),
            vec![(3 * M, 4.0)]
        );
        assert_eq!(
            eval(Some(Func::SumOverTime), p, &chunks),
            vec![(3 * M, 15.0)]
        );

        // A stale marker is not a sample, but it still claims its timestamp:
        // the second row's 120s is a duplicate, not a value.
        let stale = f64::from_bits(STALE_NAN_BITS);
        let chunks: [(&[i64], &[f64]); 2] = [
            (&[0, 60 * S, 120 * S], &[1.0, 2.0, stale]),
            (&[120 * S, 150 * S], &[5.0, 6.0]),
        ];
        assert_eq!(
            eval(Some(Func::CountOverTime), p, &chunks),
            vec![(3 * M, 3.0)]
        );
        assert_eq!(
            eval(Some(Func::SumOverTime), p, &chunks),
            vec![(3 * M, 9.0)]
        );
    }

    /// range.rs's 400-case fuzz, split at random cut points with empty
    /// chunks and repeated heads thrown in, against the same series as one
    /// chunk. A repeated head is the previous chunk's last timestamp with a
    /// different value, so answering from it would show.
    #[test]
    fn fuzz_random_cuts_empty_chunks_and_duplicated_heads() {
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
                at_ms: (next() % 5 == 0).then(|| (next() % 20) as i64 * 30 * S),
            };

            let mut chunks: Vec<(Vec<i64>, Vec<f64>)> = Vec::new();
            let mut from = 0;
            while from < ts.len() {
                let to = (from + (next() % 5) as usize).min(ts.len());
                let (mut cts, mut cvs) = (ts[from..to].to_vec(), vs[from..to].to_vec());
                if from > 0 && next() % 3 == 0 {
                    cts.insert(0, ts[from - 1]);
                    cvs.insert(0, vs[from - 1] + 100.0);
                }
                chunks.push((cts, cvs));
                from = to;
            }
            let chunks: Vec<(&[i64], &[f64])> = chunks
                .iter()
                .map(|(t, v)| (t.as_slice(), v.as_slice()))
                .collect();

            for func in kernels() {
                let whole = eval(func, p, &[(&ts, &vs)]);
                let got = eval(func, p, &chunks);
                assert_bits(
                    &got,
                    &whole,
                    &format!("case {case} {} ts={ts:?} vs={vs:?} p={p:?}", name(func)),
                );
            }
        }
    }

    #[test]
    fn steps_finalise_before_close() {
        let p = Params {
            start_ms: 0,
            end_ms: 1000 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        };
        let (ts, vs): (Vec<i64>, Vec<f64>) = (0..1000).map(|i| (i * M, i as f64)).unzip();
        // A 5m window at 1m spacing holds five samples.
        let window = 5;
        for chunk in [1, 3, 10, 50] {
            let mut it = BufferedSeriesIterator::new(Kernel::Range(Func::Rate), p);
            let mut out = SamplesBuilder::default();
            let empty = out.size();
            for (i, (cts, cvs)) in split(&ts, &vs, chunk).into_iter().enumerate() {
                let steps = it.next_step;
                it.push(cts, cvs, &mut out);
                assert!(
                    it.next_step > steps,
                    "chunk {chunk}: no step finalised at {i}"
                );
                // Every finalised step but the first, whose window holds one
                // sample, has a rate, and `size` counts at least the samples
                // held: they are in `out` before the row is finished.
                let held = (it.next_step - 1) as usize * 16;
                assert!(
                    out.size() >= empty + held,
                    "chunk {chunk}: out holds fewer than {} steps after chunk {i}",
                    it.next_step - 1
                );
                // What survives a push is the window of the first step not
                // yet answered, plus a dead prefix `reduce_delta` keeps while
                // it is under half the buffer: under two windows, whatever
                // the chunk size. "Window plus one chunk" is the peak inside
                // `push`, before the steps are walked.
                assert!(
                    it.ts.len() < 2 * window,
                    "chunk {chunk}: {} samples buffered after chunk {i}",
                    it.ts.len()
                );
            }
            it.close(&mut out);
            assert_eq!(rows(&out.take_all())[0].len(), 1000);
        }
    }
}
