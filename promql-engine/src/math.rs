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
//!
//! # Two shapes of slice work
//!
//! Every kernel takes slices, but only one of the two shapes may go fast.
//!
//! **Reductions** ([`kahan_sum`], [`mean_of`], …) must stay sequential:
//! splitting the slice across several accumulators changes the result
//! whenever an intermediate sum overflows. `[1e308, 1e308, -1e308,
//! -1e308]` sums to `+Inf` in one pass, because Kahan drops the
//! compensation on overflow, but to `NaN` in two halves merged at the
//! end. Taking the slice still pays: the accumulator stays in registers
//! for the whole run instead of being reloaded per call.
//!
//! **Elementwise updates** ([`kahan_add_each`], …) update one independent
//! accumulator per position, which is the shape of a cross-series
//! aggregation, where position `i` is step `i` and steps never interact.
//! These vectorize and must stay bit-identical to updating each
//! accumulator on its own.

use datafusion::arrow::array::BooleanBufferBuilder;
use datafusion::arrow::buffer::BooleanBuffer;

/// A run of `len` flags inside a bit-packed lane, from `at`.
///
/// `avg`'s overflow switch stays packed, so the kernels cannot take it as
/// the `&mut [f64]` they take for floats. The bit setters do not
/// bounds-check the way a slice does, so the run carries its length and
/// the debug build checks it.
pub struct FlagLane<'a> {
    bits: &'a mut BooleanBufferBuilder,
    at: usize,
    len: usize,
}

impl<'a> FlagLane<'a> {
    pub fn new(bits: &'a mut BooleanBufferBuilder, at: usize, len: usize) -> Self {
        debug_assert!(
            at + len <= bits.len(),
            "flag lane past the end of the bitmap"
        );
        Self { bits, at, len }
    }

    fn get(&self, i: usize) -> bool {
        debug_assert!(i < self.len);
        self.bits.get_bit(self.at + i)
    }

    fn set(&mut self, i: usize, v: bool) {
        debug_assert!(i < self.len);
        self.bits.set_bit(self.at + i, v);
    }
}

/// The read side of [`FlagLane`]: a run of incoming flags, still packed
/// in the Arrow buffer they arrived in.
pub struct Flags<'a> {
    bits: &'a BooleanBuffer,
    at: usize,
    len: usize,
}

impl<'a> Flags<'a> {
    pub fn new(bits: &'a BooleanBuffer, at: usize, len: usize) -> Self {
        debug_assert!(at + len <= bits.len(), "flags past the end of the buffer");
        Self { bits, at, len }
    }

    pub fn get(&self, i: usize) -> bool {
        debug_assert!(i < self.len);
        self.bits.value(self.at + i)
    }
}

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

    #[inline]
    pub fn add(&mut self, inc: f64) {
        let (sum, c) = kahan_inc(inc, self.sum, self.c);
        self.sum = sum;
        self.c = c;
    }

    /// Its sum and its compensation are both terms, or the compensation
    /// is lost and the merged answer differs from a sequential one.
    #[inline]
    pub fn merge(&mut self, other: &KahanSum) {
        self.add(other.sum);
        self.add(other.c);
    }

    pub fn value(&self) -> f64 {
        self.sum + self.c
    }
}

/// `kahansum.Inc(inc, sum, c) -> (sum, c)`.
#[inline]
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
    #[inline]
    pub fn add(&mut self, f: f64) {
        self.count += 1.0;
        if !self.incremental {
            let (v, c) = kahan_inc(f, self.value, self.c);
            // The first sample seeds the group the way Prometheus's
            // `groupedAggregation` literal does (`floatValue: f`), so an
            // infinite first sample must not trip the overflow switch
            // below: doing so would divide 0.0 by `count - 1.0` (0.0)
            // and poison the mean with NaN instead of keeping the ±Inf.
            if !v.is_infinite() || self.count == 1.0 {
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

    /// Combine two partial means. Exact when either side is empty (a
    /// state merged into a fresh accumulator, the common case) or when
    /// both sides still hold a direct sum and their Kahan-merged total
    /// stays finite; otherwise falls back to a count-weighted combination
    /// of each side's `result()`.
    #[inline]
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

    #[inline]
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
    #[inline]
    pub fn add(&mut self, f: f64) {
        self.count += 1.0;
        let delta = f - self.mean;
        self.mean += delta / self.count;
        self.m2 += delta * (f - self.mean);
    }

    /// Chan et al.'s parallel combination.
    #[inline]
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

// ── Reductions: a whole slice into one value ─────────────────────────
//
// Sequential by necessity, see the module comment.

/// Kahan-compensated sum of a whole series' values.
pub fn kahan_sum(values: &[f64]) -> KahanSum {
    let (mut sum, mut c) = (0.0, 0.0);
    for v in values {
        (sum, c) = kahan_inc(*v, sum, c);
    }
    KahanSum { sum, c }
}

/// The mean of a whole series' values, with the overflow switch.
pub fn mean_of(values: &[f64]) -> f64 {
    let mut m = Mean::default();
    for v in values {
        m.add(*v);
    }
    m.result()
}

/// The smallest value, where any number beats a NaN. `None` if empty.
pub fn min_of(values: &[f64]) -> Option<f64> {
    values
        .split_first()
        .map(|(first, _)| values.iter().copied().fold(*first, min_nan_loses))
}

/// The largest value, same NaN rule. `None` if empty.
pub fn max_of(values: &[f64]) -> Option<f64> {
    values
        .split_first()
        .map(|(first, _)| values.iter().copied().fold(*first, max_nan_loses))
}

// ── Elementwise: a slice of values into a slice of accumulators ──────
//
// Each takes the accumulator's lanes as parallel slices rather than a
// slice of structs, so the loop is over `f64` arrays and vectorizes.
// Truncating every slice to `values.len()` up front also drops the bounds
// checks from the loop body.

/// `sums[i], comps[i] += values[i]`, Kahan-compensated per position.
pub fn kahan_add_each(sums: &mut [f64], comps: &mut [f64], values: &[f64]) {
    let n = values.len();
    let (sums, comps) = (&mut sums[..n], &mut comps[..n]);
    for ((sum, c), v) in sums.iter_mut().zip(comps.iter_mut()).zip(values) {
        (*sum, *c) = kahan_inc(*v, *sum, *c);
    }
}

/// One more value in each position's running mean.
pub fn mean_add_each(
    acc: &mut [f64],
    comps: &mut [f64],
    counts: &mut [f64],
    mut incremental: FlagLane<'_>,
    values: &[f64],
) {
    let n = values.len();
    let (acc, comps, counts) = (&mut acc[..n], &mut comps[..n], &mut counts[..n]);
    for (i, f) in values.iter().enumerate() {
        let mut m = Mean {
            value: acc[i],
            c: comps[i],
            count: counts[i],
            incremental: incremental.get(i),
        };
        m.add(*f);
        acc[i] = m.value;
        comps[i] = m.c;
        counts[i] = m.count;
        incremental.set(i, m.incremental);
    }
}

/// One more value in every position's count, whatever that value was:
/// a count is over samples present, so no lane is skipped.
pub fn count_add_each(counts: &mut [f64]) {
    for n in counts.iter_mut() {
        *n += 1.0;
    }
}

/// One more value in each position's running minimum. NaN loses: a
/// number always beats it, so a group is NaN only if every value was.
pub fn min_add_each(cur: &mut [f64], values: &[f64]) {
    let cur = &mut cur[..values.len()];
    for (c, v) in cur.iter_mut().zip(values) {
        *c = min_nan_loses(*c, *v);
    }
}

/// One more value in each position's running maximum. NaN loses: a
/// number always beats it, so a group is NaN only if every value was.
pub fn max_add_each(cur: &mut [f64], values: &[f64]) {
    let cur = &mut cur[..values.len()];
    for (c, v) in cur.iter_mut().zip(values) {
        *c = max_nan_loses(*c, *v);
    }
}

/// One more value in each position's running variance.
pub fn welford_add_each(means: &mut [f64], m2s: &mut [f64], counts: &mut [f64], values: &[f64]) {
    let n = values.len();
    let (means, m2s, counts) = (&mut means[..n], &mut m2s[..n], &mut counts[..n]);
    for (((mean, m2), count), v) in means
        .iter_mut()
        .zip(m2s.iter_mut())
        .zip(counts.iter_mut())
        .zip(values)
    {
        *count += 1.0;
        let delta = *v - *mean;
        *mean += delta / *count;
        *m2 += delta * (*v - *mean);
    }
}

// ── Elementwise: a slice of partial states into a slice of accumulators
//
// What a two-phase aggregation does when a partition's states reach the
// final accumulator. Each is the per-position restatement of the
// corresponding `merge` above, so the two must stay in step.

/// `sums[i], comps[i]` folded with the partial sum at `i`.
pub fn kahan_merge_each(
    sums: &mut [f64],
    comps: &mut [f64],
    other_sums: &[f64],
    other_comps: &[f64],
) {
    let n = other_sums.len();
    let (sums, comps) = (&mut sums[..n], &mut comps[..n]);
    for (((sum, c), os), oc) in sums
        .iter_mut()
        .zip(comps.iter_mut())
        .zip(other_sums)
        .zip(&other_comps[..n])
    {
        // The two terms [`KahanSum::merge`] adds, inlined rather than
        // built as a `KahanSum` per position that the loop would then
        // have to prove is dead.
        let (s, k) = kahan_inc(*os, *sum, *c);
        (*sum, *c) = kahan_inc(*oc, s, k);
    }
}

/// Each position's running mean folded with the partial mean at the same
/// position. Both groups are a [`Mean`] spread over four lanes, value,
/// compensation, count and incremental flag, in that order: the running
/// lanes first, then the incoming states.
// Four lanes in and four out is what a [`Mean`] is; grouping them into
// argument structs only renames the fields at every call site, and the
// add kernels next door already take their lanes flat.
#[allow(clippy::too_many_arguments)]
pub fn mean_merge_each(
    value: &mut [f64],
    c: &mut [f64],
    count: &mut [f64],
    mut incremental: FlagLane<'_>,
    other_value: &[f64],
    other_c: &[f64],
    other_count: &[f64],
    other_incremental: Flags<'_>,
) {
    let n = other_value.len();
    let (value, c, count) = (&mut value[..n], &mut c[..n], &mut count[..n]);
    for i in 0..n {
        let mut m = Mean {
            value: value[i],
            c: c[i],
            count: count[i],
            incremental: incremental.get(i),
        };
        m.merge(&Mean {
            value: other_value[i],
            c: other_c[i],
            count: other_count[i],
            incremental: other_incremental.get(i),
        });
        value[i] = m.value;
        c[i] = m.c;
        count[i] = m.count;
        incremental.set(i, m.incremental);
    }
}

/// Each position's count plus the partial count at the same position.
pub fn count_merge_each(counts: &mut [f64], others: &[f64]) {
    let counts = &mut counts[..others.len()];
    for (n, o) in counts.iter_mut().zip(others) {
        *n += o;
    }
}

/// Each position's running variance folded with the partial state at the
/// same position: the running lanes first, then the incoming ones.
pub fn welford_merge_each(
    means: &mut [f64],
    m2s: &mut [f64],
    counts: &mut [f64],
    other_means: &[f64],
    other_m2s: &[f64],
    other_counts: &[f64],
) {
    let n = other_means.len();
    let (means, m2s, counts) = (&mut means[..n], &mut m2s[..n], &mut counts[..n]);
    for i in 0..n {
        let mut w = Welford {
            mean: means[i],
            m2: m2s[i],
            count: counts[i],
        };
        w.merge(&Welford {
            mean: other_means[i],
            m2: other_m2s[i],
            count: other_counts[i],
        });
        means[i] = w.mean;
        m2s[i] = w.m2;
        counts[i] = w.count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The slice kernels must be bit-identical to feeding the value
    /// types one element at a time — that is the whole claim.
    #[test]
    fn reductions_match_the_one_at_a_time_loop_bit_for_bit() {
        let cases: &[&[f64]] = &[
            &[],
            &[1.0],
            &[1e16, 1.0, -1e16],
            &[0.1; 100],
            &[1e308, 1e308, -1e308, -1e308],
            &[f64::NAN, 3.0, -7.5, f64::NAN],
            &[f64::INFINITY, 1.0, f64::NEG_INFINITY],
            &[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0],
        ];
        for values in cases {
            let mut k = KahanSum::default();
            let mut m = Mean::default();
            for v in *values {
                k.add(*v);
                m.add(*v);
            }
            // Bit comparison, not `==`: several cases carry NaN, which
            // is never equal to itself.
            assert_eq!(kahan_sum(values).value().to_bits(), k.value().to_bits());
            if !values.is_empty() {
                assert_eq!(mean_of(values).to_bits(), m.result().to_bits());
                let expect_min = values.iter().copied().fold(values[0], min_nan_loses);
                let expect_max = values.iter().copied().fold(values[0], max_nan_loses);
                assert_eq!(min_of(values).unwrap().to_bits(), expect_min.to_bits());
                assert_eq!(max_of(values).unwrap().to_bits(), expect_max.to_bits());
            } else {
                assert!(mean_of(values).is_nan());
                assert_eq!(min_of(values), None);
                assert_eq!(max_of(values), None);
            }
        }
    }

    #[test]
    fn elementwise_updates_are_independent_per_position() {
        // Three positions, three contributors each, added as slices.
        let rows: [[f64; 3]; 3] = [[1e16, 2.0, f64::NAN], [1.0, 4.0, 5.0], [-1e16, 6.0, 3.0]];
        let mut sums = [0.0; 3];
        let mut comps = [0.0; 3];
        let mut counts = [0.0; 3];
        let mut mins = [f64::NAN; 3];
        for row in &rows {
            kahan_add_each(&mut sums, &mut comps, row);
            count_add_each(&mut counts);
            min_add_each(&mut mins, row);
        }
        // Position 0 keeps the compensation naive summation would lose,
        // and the NaN in position 2 poisons nothing but position 2.
        assert_eq!(sums[0] + comps[0], 1.0);
        assert_eq!(sums[1] + comps[1], 12.0);
        assert!((sums[2] + comps[2]).is_nan());
        assert_eq!(counts, [3.0; 3]);
        assert_eq!(mins, [-1e16, 2.0, 3.0]);

        // And each position equals its own column reduced on its own.
        for i in 0..3 {
            let column: Vec<f64> = rows.iter().map(|r| r[i]).collect();
            assert_eq!(
                (sums[i] + comps[i]).to_bits(),
                kahan_sum(&column).value().to_bits()
            );
        }
    }

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

    /// An infinite first sample seeds the group directly, the way
    /// Prometheus's `groupedAggregation` literal does, instead of
    /// dividing zero by zero in the overflow switch.
    #[test]
    fn an_infinite_first_sample_does_not_poison_the_mean() {
        let mut m = Mean::default();
        m.add(f64::INFINITY);
        m.add(2.0);
        assert_eq!(m.result(), f64::INFINITY);

        let mut m = Mean::default();
        m.add(f64::NEG_INFINITY);
        m.add(2.0);
        assert_eq!(m.result(), f64::NEG_INFINITY);

        // Two infinities of opposite sign is a legitimate NaN, same as
        // Prometheus: this is not the bug, it is Inf + -Inf.
        let mut m = Mean::default();
        m.add(f64::INFINITY);
        m.add(f64::NEG_INFINITY);
        assert!(m.result().is_nan());

        let mut m = Mean::default();
        m.add(f64::NAN);
        m.add(1.0);
        assert!(m.result().is_nan());
    }

    /// The same four sequences through the lane kernel `mean_add_each`,
    /// which is what the grouped accumulator actually calls: the fix
    /// lives in [`Mean::add`], which `mean_add_each` calls per position,
    /// so this is the same guard reached through the other entry point.
    #[test]
    fn mean_add_each_keeps_an_infinite_first_sample_out_of_the_overflow_switch() {
        let run = |values: &[f64]| -> f64 {
            let mut acc = [0.0];
            let mut comps = [0.0];
            let mut counts = [0.0];
            let mut flags = BooleanBufferBuilder::new(1);
            flags.append(false);
            for v in values {
                mean_add_each(
                    &mut acc,
                    &mut comps,
                    &mut counts,
                    FlagLane::new(&mut flags, 0, 1),
                    std::slice::from_ref(v),
                );
            }
            Mean {
                value: acc[0],
                c: comps[0],
                count: counts[0],
                incremental: flags.get_bit(0),
            }
            .result()
        };
        assert_eq!(run(&[f64::INFINITY, 2.0]), f64::INFINITY);
        assert_eq!(run(&[f64::NEG_INFINITY, 2.0]), f64::NEG_INFINITY);
        assert!(run(&[f64::INFINITY, f64::NEG_INFINITY]).is_nan());
        assert!(run(&[f64::NAN, 1.0]).is_nan());
    }

    /// A partial state seeded from a lone `+Inf` merged into a partial
    /// state seeded from a plain value must stay `+Inf`, not poison to
    /// NaN: the two-phase path hits the same first-sample seeding as the
    /// single-accumulator path.
    #[test]
    fn merging_an_infinite_partial_state_stays_infinite() {
        let mut inf = Mean::default();
        inf.add(f64::INFINITY);
        let mut two = Mean::default();
        two.add(2.0);
        inf.merge(&two);
        assert_eq!(inf.result(), f64::INFINITY);
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
