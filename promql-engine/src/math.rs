//! The float arithmetic PromQL is particular about.
//!
//! Prometheus does not sum floats naively: `sum` and `avg` are
//! Kahan-compensated, `avg` switches to an incremental mean when the
//! running sum would overflow, `min`/`max` let a number beat a NaN but
//! never the other way round, and `stddev`/`stdvar` are Welford's online
//! algorithm. The differential suite compares against the Go engine to
//! `1e-6` absolute or `1e-10` relative, so these rules are ported, not
//! approximated. Each is a small value type here, shared by the
//! aggregation and the `*_over_time` functions.

/// Kahan–Neumaier summation, `kahansum.Inc` in Prometheus.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct KahanSum {
    pub sum: f64,
    pub c: f64,
}

impl KahanSum {
    pub fn new(sum: f64, c: f64) -> Self {
        Self { sum, c }
    }

    /// Add one term.
    pub fn add(&mut self, inc: f64) {
        let (sum, c) = kahan_inc(inc, self.sum, self.c);
        self.sum = sum;
        self.c = c;
    }

    /// Fold another partial sum in: its sum and its compensation are both
    /// terms.
    pub fn merge(&mut self, other: &KahanSum) {
        self.add(other.sum);
        self.add(other.c);
    }

    pub fn value(&self) -> f64 {
        self.sum + self.c
    }
}

/// `kahansum.Inc(inc, sum, c) -> (sum, c)`.
pub fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;
    let c = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };
    (t, c)
}

/// Prometheus's mean: a Kahan sum divided by the count until that sum
/// would overflow, then an incremental mean from that point on. The
/// `floatIncrementalMean` branch of `aggregation` in `engine.go`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Mean {
    /// The running sum, or the running mean once `incremental`.
    pub value: f64,
    pub c: f64,
    pub count: f64,
    pub incremental: bool,
}

impl Mean {
    pub fn add(&mut self, f: f64) {
        self.count += 1.0;
        if !self.incremental {
            let (v, c) = kahan_inc(f, self.value, self.c);
            if !v.is_infinite() {
                self.value = v;
                self.c = c;
                return;
            }
            // The sum would overflow: from here on keep a mean instead.
            self.incremental = true;
            self.value /= self.count - 1.0;
            self.c /= self.count - 1.0;
        }
        let q = (self.count - 1.0) / self.count;
        let (v, c) = kahan_inc(f / self.count, q * self.value, q * self.c);
        self.value = v;
        self.c = c;
    }

    /// Combine two partial means. Exact when either side is empty, which is
    /// the common case (a state merged into a fresh accumulator); otherwise
    /// a count-weighted combination.
    pub fn merge(&mut self, other: &Mean) {
        if other.count == 0.0 {
            return;
        }
        if self.count == 0.0 {
            *self = *other;
            return;
        }
        let total = self.count + other.count;
        if !self.incremental && !other.incremental {
            let mut s = KahanSum::new(self.value, self.c);
            s.merge(&KahanSum::new(other.value, other.c));
            if !s.sum.is_infinite() {
                self.value = s.sum;
                self.c = s.c;
                self.count = total;
                return;
            }
        }
        let a = self.result();
        let b = other.result();
        self.value = a * (self.count / total) + b * (other.count / total);
        self.c = 0.0;
        self.count = total;
        self.incremental = true;
    }

    pub fn result(&self) -> f64 {
        if self.incremental {
            self.value + self.c
        } else {
            self.value / self.count + self.c / self.count
        }
    }
}

/// The `min` rule: a smaller value wins, and any number beats a NaN.
pub fn min_nan_loses(current: f64, f: f64) -> f64 {
    if current > f || current.is_nan() {
        f
    } else {
        current
    }
}

/// The `max` rule, mirror image.
pub fn max_nan_loses(current: f64, f: f64) -> f64 {
    if current < f || current.is_nan() {
        f
    } else {
        current
    }
}

/// Welford's online variance, as `stddev`/`stdvar` accumulate it. `m2` is
/// the sum of squared deviations; a NaN or infinite input poisons it, as
/// upstream's does through arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Welford {
    pub mean: f64,
    pub m2: f64,
    pub count: f64,
}

impl Welford {
    pub fn add(&mut self, f: f64) {
        self.count += 1.0;
        let delta = f - self.mean;
        self.mean += delta / self.count;
        self.m2 += delta * (f - self.mean);
    }

    /// Chan et al.'s parallel combination.
    pub fn merge(&mut self, other: &Welford) {
        if other.count == 0.0 {
            return;
        }
        if self.count == 0.0 {
            *self = *other;
            return;
        }
        let total = self.count + other.count;
        let delta = other.mean - self.mean;
        self.mean += delta * (other.count / total);
        self.m2 += other.m2 + delta * delta * (self.count * other.count / total);
        self.count = total;
    }

    pub fn variance(&self) -> f64 {
        self.m2 / self.count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kahan_recovers_what_naive_summation_loses() {
        let mut k = KahanSum::default();
        for v in [1e16, 1.0, -1e16] {
            k.add(v);
        }
        assert_eq!(k.value(), 1.0);
        assert_eq!(1e16 + 1.0 - 1e16, 0.0);
    }

    #[test]
    fn kahan_merge_is_a_sum_of_terms() {
        let mut a = KahanSum::default();
        a.add(1e16);
        let mut b = KahanSum::default();
        b.add(1.0);
        b.add(-1e16);
        a.merge(&b);
        assert_eq!(a.value(), 1.0);
    }

    #[test]
    fn mean_is_the_kahan_sum_over_the_count_until_overflow() {
        let mut m = Mean::default();
        for v in [1.0, 2.0, 3.0, 4.0] {
            m.add(v);
        }
        assert!(!m.incremental);
        assert_eq!(m.result(), 2.5);

        let mut m = Mean::default();
        m.add(f64::MAX);
        m.add(f64::MAX);
        assert!(m.incremental);
        assert_eq!(m.result(), f64::MAX);
        m.add(0.0);
        assert!((m.result() - f64::MAX / 1.5).abs() <= f64::MAX * 1e-15);
    }

    #[test]
    fn mean_merge_into_empty_is_exact() {
        let mut m = Mean::default();
        for v in [1.0, 2.0, 4.0] {
            m.add(v);
        }
        let mut fresh = Mean::default();
        fresh.merge(&m);
        assert_eq!(fresh, m);
        let mut other = Mean::default();
        other.add(9.0);
        fresh.merge(&other);
        assert_eq!(fresh.result(), 4.0);
    }

    #[test]
    fn a_number_beats_a_nan_but_a_nan_beats_nothing() {
        assert_eq!(min_nan_loses(f64::NAN, 3.0), 3.0);
        assert_eq!(min_nan_loses(3.0, f64::NAN), 3.0);
        assert_eq!(min_nan_loses(3.0, 1.0), 1.0);
        assert_eq!(max_nan_loses(f64::NAN, 3.0), 3.0);
        assert_eq!(max_nan_loses(3.0, f64::NAN), 3.0);
        assert_eq!(max_nan_loses(3.0, 5.0), 5.0);
    }

    #[test]
    fn welford_matches_the_two_pass_variance_and_merges() {
        let xs = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let mut w = Welford::default();
        xs.iter().for_each(|x| w.add(*x));
        assert!((w.variance() - 4.0).abs() < 1e-12);

        let (mut a, mut b) = (Welford::default(), Welford::default());
        xs[..3].iter().for_each(|x| a.add(*x));
        xs[3..].iter().for_each(|x| b.add(*x));
        a.merge(&b);
        assert!((a.variance() - 4.0).abs() < 1e-12);
        assert_eq!(a.count, 8.0);

        let mut w = Welford::default();
        w.add(1.0);
        w.add(f64::INFINITY);
        assert!(w.variance().is_nan());
    }
}
