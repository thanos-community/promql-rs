//! The step grid, shared by every kernel that walks one.
//!
//! An instant selector and a range function differ in what they do with
//! a window, not in how they find one: same grid, same `offset`, same
//! `@`, same strict lower bound on the scan range. They are one struct
//! so that a change to the grid arithmetic cannot land in one kernel and
//! miss the other.

/// Everything a per-series kernel needs besides the samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Params {
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    /// How far back of each step the kernel reads: the lookback delta
    /// for an instant selector, the `[5m]` for a range function.
    pub window_ms: i64,
    /// The selector's offset. Positive looks into the past.
    pub offset_ms: i64,
    /// An `@` modifier, already resolved from `start()`/`end()`.
    pub at_ms: Option<i64>,
}

impl Params {
    /// The scan range a store has to be asked for so that every step can
    /// be answered: `getTimeRangesForSelector` in upstream. The `- 1` is
    /// the strict lower bound of the window, so that a sample exactly
    /// `window_ms` old is not even read.
    ///
    /// The saturating arithmetic is the lower half of a two-part defence
    /// against an absurd timestamp: the planner rejects an out-of-range
    /// `@` or offset, and this clamps whatever still reaches it.
    pub fn select_range(&self) -> (i64, i64) {
        let (lo, hi) = match self.at_ms {
            Some(at) => (at, at),
            None => (self.start_ms, self.end_ms),
        };
        (
            lo.saturating_sub(self.window_ms.saturating_sub(1))
                .saturating_sub(self.offset_ms),
            hi.saturating_sub(self.offset_ms),
        )
    }

    /// The grid this evaluates on, whose length is [`step_count`].
    pub(crate) fn steps(&self) -> impl Iterator<Item = i64> {
        let (start, step) = (i128::from(self.start_ms), i128::from(self.step_ms));
        // The count is derived up front, the way `Grid` derives its length,
        // so the last position is <= end by construction: no per-step bound
        // check, and so no probe one step past the end that could overflow.
        let count = step_count(self.start_ms, self.end_ms, self.step_ms);
        (0..count).map(move |i| (start + i * step) as i64)
    }
}

/// How many steps `start_ms..=end_ms` every `step_ms` has.
///
/// In `i128` so that a range as wide as `i64` is a number to compare
/// against a cap rather than an overflow. The planner and
/// [`Grid`](crate::aggregate) both measure with this, so their guards
/// agree by construction; both reject a non-positive step first, with a
/// message of their own.
pub fn step_count(start_ms: i64, end_ms: i64, step_ms: i64) -> i128 {
    if step_ms <= 0 || end_ms < start_ms {
        return 0;
    }
    (i128::from(end_ms) - i128::from(start_ms)) / i128::from(step_ms) + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1000;
    const M: i64 = 60 * S;

    #[test]
    fn the_select_range_reflects_the_strict_lower_bound() {
        let p = Params {
            start_ms: 0,
            end_ms: 10 * M,
            step_ms: M,
            window_ms: 5 * M,
            offset_ms: 30 * S,
            at_ms: None,
        };
        assert_eq!(p.select_range(), (-(5 * M) + 1 - 30 * S, 10 * M - 30 * S));
    }

    /// `rate(up[5m] @ -1e30)` is where an extreme timestamp comes from.
    #[test]
    fn an_extreme_at_timestamp_clamps_the_select_range() {
        let base = Params {
            start_ms: 5 * M,
            end_ms: 5 * M,
            step_ms: 30 * S,
            window_ms: 5 * M,
            offset_ms: 0,
            at_ms: None,
        };
        let p = Params {
            at_ms: Some(i64::MIN),
            offset_ms: i64::MAX,
            ..base
        };
        assert_eq!(p.select_range(), (i64::MIN, i64::MIN));
        let p = Params {
            at_ms: Some(i64::MAX),
            offset_ms: i64::MIN,
            ..base
        };
        assert_eq!(p.select_range(), (i64::MAX, i64::MAX));
        let p = Params {
            start_ms: i64::MIN,
            end_ms: i64::MAX,
            window_ms: i64::MAX,
            ..base
        };
        let (lo, hi) = p.select_range();
        assert_eq!((lo, hi), (i64::MIN, i64::MAX));
        assert!(lo <= hi);
    }

    #[test]
    fn the_step_count_of_the_widest_possible_range_is_a_number() {
        assert_eq!(step_count(0, 9, 1), 10);
        assert_eq!(step_count(0, 9, 10), 1);
        assert_eq!(step_count(10, 0, 1), 0);
        assert_eq!(step_count(0, 10, 0), 0);
        assert_eq!(
            step_count(i64::MIN, i64::MAX, 1),
            i128::from(u64::MAX) + 1,
            "the widest range does not overflow"
        );
    }

    #[test]
    fn the_steps_are_as_many_as_the_count_says() {
        let p = Params {
            start_ms: 0,
            end_ms: 100,
            step_ms: 30,
            window_ms: 0,
            offset_ms: 0,
            at_ms: None,
        };
        assert_eq!(p.steps().collect::<Vec<_>>(), vec![0, 30, 60, 90]);
        assert_eq!(p.steps().count() as i128, step_count(0, 100, 30));
    }

    #[test]
    fn the_step_grid_stops_at_the_end_of_time() {
        let p = Params {
            start_ms: i64::MAX,
            end_ms: i64::MAX,
            step_ms: 1,
            window_ms: 0,
            offset_ms: 0,
            at_ms: None,
        };
        assert_eq!(step_count(p.start_ms, p.end_ms, p.step_ms), 1);

        // Ask for the end explicitly: collecting an unbounded iterator
        // could hang if overflow wraps instead of panicking in release.
        let mut steps = p.steps();
        assert_eq!(steps.next(), Some(i64::MAX));
        assert_eq!(steps.next(), None);
    }
}
