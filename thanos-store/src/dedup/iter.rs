//! `pkg/dedup/iter.go` over drained samples: the penalty merge of the
//! replicas of one series, and the chain merge.

use std::collections::BTreeMap;

/// `dedupSeries.Iterator` drained: the replicas' samples, each ascending
/// and already clipped to the range, merged into one ascending run with
/// the penalty algorithm. `is_counter` says whether the function above the
/// selector is one whose input must never go backwards.
pub fn dedup_samples(replicas: Vec<Vec<(i64, f64)>>, is_counter: bool) -> Vec<(i64, f64)> {
    let mut replicas = replicas.into_iter();
    let Some(first) = replicas.next() else {
        return Vec::new();
    };
    let mut it: Box<dyn AdjustableIterator> = Box::new(ReplicaIterator::new(first, is_counter));
    for replica in replicas {
        it = Box::new(DedupSeriesIterator::new(
            it,
            Box::new(ReplicaIterator::new(replica, is_counter)),
        ));
    }
    let mut out = Vec::new();
    while it.next() {
        out.push(it.at());
    }
    out
}

/// `storage.ChainedSeriesMerge`: every replica's samples by timestamp, the
/// first replica winning a timestamp they share.
pub fn chain_samples(replicas: Vec<Vec<(i64, f64)>>) -> Vec<(i64, f64)> {
    let mut merged = BTreeMap::new();
    for replica in replicas {
        for (t, v) in replica {
            merged.entry(t).or_insert(v);
        }
    }
    merged.into_iter().collect()
}

/// The slice of `chunkenc.Iterator` the algorithm uses plus
/// `adjustableSeriesIterator.adjustAtValue`. `next` and `seek` say whether
/// a sample is at hand, Go's `ValFloat` against `ValNone`.
trait AdjustableIterator {
    fn next(&mut self) -> bool;
    /// Move to the first sample at or after `t`; no move when the current
    /// one already is.
    fn seek(&mut self, t: i64) -> bool;
    fn at(&self) -> (i64, f64);
    fn at_t(&self) -> i64;
    fn adjust_at_value(&mut self, last: f64);
}

/// One replica's samples, as `counterErrAdjustSeriesIterator` for a
/// counter and `noopAdjustableSeriesIterator` otherwise.
struct ReplicaIterator {
    samples: Vec<(i64, f64)>,
    /// Index of the current sample; `None` before the first `next`, the
    /// length once exhausted.
    pos: Option<usize>,
    is_counter: bool,
    /// `errAdjust`: lifted onto every value once this replica was found
    /// behind the other at a switch.
    err_adjust: f64,
}

impl ReplicaIterator {
    fn new(samples: Vec<(i64, f64)>, is_counter: bool) -> Self {
        Self {
            samples,
            pos: None,
            is_counter,
            err_adjust: 0.0,
        }
    }

    /// The current sample, or the last one once exhausted, as a Prometheus
    /// iterator keeps reporting the last it decoded.
    fn current(&self) -> (i64, f64) {
        let last = self.samples.len().saturating_sub(1);
        self.samples
            .get(self.pos.unwrap_or(0).min(last))
            .copied()
            .unwrap_or((i64::MIN, f64::NAN))
    }
}

impl AdjustableIterator for ReplicaIterator {
    fn next(&mut self) -> bool {
        let next = self.pos.map_or(0, |p| p + 1).min(self.samples.len());
        self.pos = Some(next);
        next < self.samples.len()
    }

    fn seek(&mut self, t: i64) -> bool {
        let from = self.pos.unwrap_or(0).min(self.samples.len());
        let i = from + self.samples[from..].partition_point(|(ts, _)| *ts < t);
        self.pos = Some(i);
        i < self.samples.len()
    }

    fn at(&self) -> (i64, f64) {
        let (t, v) = self.current();
        // A staleness marker is a NaN with one fixed payload, and PromQL
        // knows it by its bits. Adding the adjustment, even 0, would
        // quieten it into a NaN taken for a sample, so it passes as is.
        if self.is_counter && !v.is_nan() {
            (t, v + self.err_adjust)
        } else {
            (t, v)
        }
    }

    fn at_t(&self) -> i64 {
        self.current().0
    }

    fn adjust_at_value(&mut self, last: f64) {
        if !self.is_counter {
            return;
        }
        let (_, v) = self.at();
        if last > v {
            // This replica has an obsolete value: it missed the end of the
            // counter before the app restarted. Lift it.
            self.err_adjust += last - v;
        }
    }
}

/// `initialPenalty`: before any interval is known, timestamps being in
/// milliseconds and scrapes seconds apart.
const INITIAL_PENALTY: i64 = 5000;

/// `dedupSeriesIterator`: two replicas merged by following the one with
/// the earlier next sample and holding the other back by twice the last
/// interval.
struct DedupSeriesIterator {
    a: Box<dyn AdjustableIterator>,
    b: Box<dyn AdjustableIterator>,
    a_ok: bool,
    b_ok: bool,
    last_t: i64,
    /// `lastIter`: whether `a` handed out the last sample.
    last_is_a: bool,
    pen_a: i64,
    pen_b: i64,
    use_a: bool,
}

impl DedupSeriesIterator {
    fn new(mut a: Box<dyn AdjustableIterator>, mut b: Box<dyn AdjustableIterator>) -> Self {
        let a_ok = a.next();
        let b_ok = b.next();
        Self {
            a,
            b,
            a_ok,
            b_ok,
            last_t: i64::MIN,
            last_is_a: true,
            pen_a: 0,
            pen_b: 0,
            use_a: true,
        }
    }

    /// `lastFloatVal`: the value last handed out, while the replica it
    /// came from still has samples.
    fn last_float_val(&self) -> Option<f64> {
        if (self.use_a && self.a_ok) || (!self.use_a && self.b_ok) {
            Some(self.at().1)
        } else {
            None
        }
    }

    /// `Next` without its deferred adjustment.
    fn advance(&mut self) -> bool {
        // Advance both replicas to at least the last timestamp plus the
        // penalty each carries.
        if self.a_ok {
            self.a_ok = self.a.seek(self.last_t + 1 + self.pen_a);
        }
        if self.b_ok {
            self.b_ok = self.b.seek(self.last_t + 1 + self.pen_b);
        }

        // One replica exhausted: follow the other.
        if !self.a_ok {
            self.use_a = false;
            if self.b_ok {
                self.last_t = self.b.at_t();
                self.last_is_a = false;
                self.pen_b = 0;
            }
            return self.b_ok;
        }
        if !self.b_ok {
            self.use_a = true;
            self.last_t = self.a.at_t();
            self.last_is_a = true;
            self.pen_a = 0;
            return true;
        }

        // Both have data: take the earlier sample. The replica not taken
        // gets a penalty of twice the last interval, so a sample close to
        // this one does not raise the frequency, and clock drift between
        // replicas does not matter.
        let ta = self.a.at_t();
        let tb = self.b.at_t();
        self.use_a = ta <= tb;
        if self.use_a {
            self.pen_b = if self.last_t != i64::MIN {
                2 * (ta - self.last_t)
            } else {
                INITIAL_PENALTY
            };
            self.pen_a = 0;
            self.last_t = ta;
            self.last_is_a = true;
        } else {
            self.pen_a = if self.last_t != i64::MIN {
                2 * (tb - self.last_t)
            } else {
                INITIAL_PENALTY
            };
            self.pen_b = 0;
            self.last_t = tb;
            self.last_is_a = false;
        }
        true
    }
}

impl AdjustableIterator for DedupSeriesIterator {
    fn next(&mut self) -> bool {
        let last_float = self.last_float_val();
        let last_use_a = self.use_a;
        let ok = self.advance();
        if self.use_a != last_use_a {
            // Switched replicas: make the new one agree with the value
            // handed out before.
            if let Some(last) = last_float {
                self.adjust_at_value(last);
            }
        }
        ok
    }

    fn seek(&mut self, t: i64) -> bool {
        // Step with `next` rather than seeking the replicas, so gaps are
        // not skipped over.
        loop {
            let ts = self.at_t();
            if ts >= t {
                return if self.use_a {
                    self.a.seek(ts)
                } else {
                    self.b.seek(ts)
                };
            }
            if !self.next() {
                return false;
            }
        }
    }

    fn at(&self) -> (i64, f64) {
        if self.last_is_a {
            self.a.at()
        } else {
            self.b.at()
        }
    }

    fn at_t(&self) -> i64 {
        if self.use_a {
            self.a.at_t()
        } else {
            self.b.at_t()
        }
    }

    fn adjust_at_value(&mut self, last: f64) {
        if self.a_ok {
            self.a.adjust_at_value(last);
        }
        if self.b_ok {
            self.b.adjust_at_value(last);
        }
    }
}

#[cfg(test)]
mod tests {
    use promql_engine::selector::STALE_NAN_BITS;

    use super::*;

    /// Samples every `step` seconds from `start` seconds, values 1, 2, 3…
    fn scrapes(start: i64, step: i64, n: i64) -> Vec<(i64, f64)> {
        (0..n)
            .map(|i| ((start + i * step) * 1000, (i + 1) as f64))
            .collect()
    }

    fn seconds(samples: &[(i64, f64)]) -> Vec<i64> {
        samples.iter().map(|(t, _)| t / 1000).collect()
    }

    #[test]
    fn one_replica_passes_through() {
        let samples = scrapes(0, 10, 4);
        assert_eq!(dedup_samples(vec![samples.clone()], false), samples);
        assert_eq!(dedup_samples(vec![samples.clone()], true), samples);
        assert_eq!(dedup_samples(vec![], false), vec![]);
        assert_eq!(dedup_samples(vec![vec![], vec![]], false), vec![]);
    }

    #[test]
    fn the_replica_scraping_first_is_followed() {
        // Replica b scrapes one second after a; its samples are never
        // needed, so the merged series keeps a's frequency.
        let merged = dedup_samples(vec![scrapes(0, 10, 4), scrapes(1, 10, 4)], false);
        assert_eq!(seconds(&merged), [0, 10, 20, 30]);

        let merged = dedup_samples(vec![scrapes(1, 10, 4), scrapes(0, 10, 4)], false);
        assert_eq!(
            seconds(&merged),
            [0, 10, 20, 30],
            "whichever is given first"
        );
    }

    #[test]
    fn a_gap_switches_replicas_after_the_penalty() {
        // a stops after 20s and returns at 60s; b keeps scraping. After
        // a's 20s sample, b is held back by twice the interval, so the
        // switch lands at 41s and a's 60s sample is then out-penalised.
        let a: Vec<(i64, f64)> = [0, 10, 20, 60]
            .iter()
            .map(|s| (s * 1000, *s as f64))
            .collect();
        let b = scrapes(1, 10, 7);
        let merged = dedup_samples(vec![a, b], false);
        assert_eq!(seconds(&merged), [0, 10, 20, 41, 51, 61]);
    }

    #[test]
    fn a_lagging_counter_replica_is_lifted_at_the_switch() {
        let a = vec![(0, 100.0), (10_000, 110.0)];
        let b = vec![(35_000, 105.0)];
        assert_eq!(
            dedup_samples(vec![a.clone(), b.clone()], true),
            vec![(0, 100.0), (10_000, 110.0), (35_000, 110.0)],
            "b never saw the counter reach 110, so it is lifted by 5"
        );
        assert_eq!(
            dedup_samples(vec![a, b], false),
            vec![(0, 100.0), (10_000, 110.0), (35_000, 105.0)],
            "a gauge is taken as is"
        );
    }

    #[test]
    fn a_staleness_marker_keeps_its_bits_through_the_counter_lift() {
        // Prometheus ends a series with a NaN of one fixed payload, a
        // signalling NaN. Adding the counter adjustment to it, even 0,
        // quietens it into a NaN the engine takes for a value, and every
        // rate over the window turns NaN.
        let stale = f64::from_bits(STALE_NAN_BITS);
        let a = vec![(0, 10.0), (10_000, 20.0), (20_000, stale)];
        let b = vec![(1_000, 10.0), (11_000, 20.0), (21_000, stale)];
        let merged = dedup_samples(vec![a, b], true);
        assert_eq!(seconds(&merged), [0, 10, 20]);
        assert_eq!(merged[2].1.to_bits(), STALE_NAN_BITS);
    }

    #[test]
    fn three_replicas_fold_pairwise() {
        // The first two fold to the first's samples, 3s being inside the
        // initial 5s penalty. The third, 6s behind, is past it, so it is
        // switched to after the first sample and then followed: the merge
        // holds the pair back by twice the interval each time.
        let merged = dedup_samples(
            vec![scrapes(0, 10, 3), scrapes(3, 10, 3), scrapes(6, 10, 3)],
            false,
        );
        assert_eq!(seconds(&merged), [0, 6, 16, 26]);
    }

    #[test]
    fn chain_unions_samples_first_replica_first() {
        let merged = chain_samples(vec![
            vec![(0, 1.0), (10, 1.0)],
            vec![(10, 2.0), (20, 2.0)],
            vec![(5, 3.0)],
        ]);
        assert_eq!(merged, vec![(0, 1.0), (5, 3.0), (10, 1.0), (20, 2.0)]);
    }
}
