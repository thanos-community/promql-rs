//! One series' samples as they cross chunk rows: Prometheus's
//! `storage.BufferedSeriesIterator` (`storage/buffer.go`), fed by rows
//! instead of pulling from a chunk iterator.

use crate::params::Params;
use crate::range::{Func, Sweep};
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
    pub(crate) lo: usize,
    pub(crate) hi: usize,
    pub(crate) next_step: i128,
    /// Overlap rule; stale markers count.
    pub(crate) last_t: Option<i64>,
}

impl BufferedSeriesIterator {
    pub(crate) fn new(kernel: Kernel, params: Params) -> Self {
        todo!("{:?}", params)
    }

    /// Folds one chunk row. Evaluates into `out`'s open row every step whose
    /// window can no longer change, then trims the buffer below `lo`.
    pub(crate) fn push(&mut self, ts: &[i64], vs: &[f64], out: &mut SamplesBuilder) {
        todo!()
    }

    /// Evaluates the remaining steps, finishes `out`'s row, resets for the
    /// next series while keeping capacity.
    pub(crate) fn close(&mut self, out: &mut SamplesBuilder) {
        todo!()
    }

    pub(crate) fn size(&self) -> usize {
        todo!()
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
            &[(&[0, 30_000], &[1.0, 2.0]), (&[90_000, 150_000], &[3.0, 4.0])],
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
            vec![vec![(0, 1.0), (M, 2.0), (2 * M, 3.0), (3 * M, 3.0), (4 * M, 3.0)]]
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
        assert_eq!(out, vec![(0, 1.0), (2 * M, 6.0), (3 * M, 6.0), (4 * M, 6.0)]);
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
        assert_eq!(
            rows(&out.take_all()),
            vec![vec![(10 * M, 3.0)]],
        );
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
