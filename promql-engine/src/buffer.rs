//! One series' samples as they cross chunk rows: Prometheus's
//! `storage.BufferedSeriesIterator` (`storage/buffer.go`), fed row by row
//! instead of pulling from a chunk iterator.
//!
//! A chunk row's samples are copied into one contiguous buffer rather than
//! kept as slices of their batch. A retained slice would pin the whole
//! batch's child buffers, other series' samples included, and every kernel
//! indexes one contiguous slice; the copy replaces the staleness filter the
//! range kernel pays anyway.
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
        self.append(ts, vs);
        self.advance(false, out);
        self.reduce_delta();
    }

    /// Evaluates the remaining steps, finishes `out`'s row, resets for the
    /// next series while keeping capacity.
    pub(crate) fn close(&mut self, out: &mut SamplesBuilder) {
        // With nothing buffered, no step left has a sample to read.
        if !self.ts.is_empty() {
            // Every sample is in: no window can change any more.
            self.last_t = Some(i64::MAX);
            self.advance(true, out);
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

    fn advance(&mut self, done: bool, out: &mut SamplesBuilder) {
        match self.kernel {
            Kernel::Selector => advance_selector(self, out),
            Kernel::Range(func) => advance_range(self, func, done, |t, v| out.push(t, v)),
        }
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
}
