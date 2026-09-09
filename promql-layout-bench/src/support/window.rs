//! The whole of what UDWFs would have done for us.
//!
//! Dropping native window frames means computing window boundaries ourselves. This module is
//! that, in full: given one series' sorted timestamps and a range query's `(start, step, steps,
//! range)`, it yields the sample index range each step covers. Everything else a range-vector
//! function needs, the samples themselves, is a slice either way, which is why the function
//! bodies in [`super::ratefn`] are the same code whether a frame or this module found the bounds.
//!
//! It is a monotonic two-pointer sweep, so it is also strictly cheaper than a frame: a
//! `RANGE BETWEEN … PRECEDING` frame re-derives its bounds per row, which is the `window / step`
//! waste measured in `docs/series-representation.md`. Here both pointers only move forward, so
//! one pass over a series serves every step.

use std::ops::Range;

/// A range query's time grid, in milliseconds.
#[derive(Debug, Clone, Copy)]
pub struct Grid {
    /// Timestamp of the first step.
    pub start: i64,
    /// Distance between steps.
    pub step: i64,
    /// Number of steps. An instant query is `steps == 1`.
    pub steps: usize,
    /// Window width, the `[5m]` in `rate(x[5m])`.
    pub range: i64,
}

impl Grid {
    /// Timestamp of step `s`.
    pub fn at(&self, s: usize) -> i64 {
        self.start + s as i64 * self.step
    }
}

/// For each step of `grid`, the half-open index range of `ts` covering `(t - range, t]`.
///
/// `ts` must be sorted ascending, which is phase 1's third obligation. The left bound is
/// exclusive and the right inclusive, matching PromQL's range selector.
///
/// Both pointers advance monotonically across steps, so the whole sweep is `O(samples + steps)`
/// per series however many steps are asked for.
pub fn windows(ts: &[i64], grid: Grid) -> Vec<Range<usize>> {
    let mut out = Vec::with_capacity(grid.steps);
    let mut lo = 0usize;
    let mut hi = 0usize;

    for s in 0..grid.steps {
        let t = grid.at(s);
        while hi < ts.len() && ts[hi] <= t {
            hi += 1;
        }
        // Left bound is exclusive: a sample exactly `range` old is outside the window.
        while lo < hi && ts[lo] <= t - grid.range {
            lo += 1;
        }
        out.push(lo..hi);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 30s scrape, so sample `i` is at `i * 30_000`.
    fn scrape(n: usize) -> Vec<i64> {
        (0..n as i64).map(|i| i * 30_000).collect()
    }

    #[test]
    fn a_5m_window_at_a_30s_scrape_holds_ten_samples() {
        // 21 samples reaches 600_000 exactly, so the window's right edge lands on a sample.
        let ts = scrape(21);
        let w = windows(
            &ts,
            Grid {
                start: 600_000, // 10m in, so the window is fully populated
                step: 15_000,
                steps: 1,
                range: 300_000,
            },
        );
        // (5m, 10m] over a 30s scrape: samples at 5m30s through 10m inclusive.
        assert_eq!(w[0], 11..21);
        assert_eq!(w[0].len(), 10, "a 5m window at 30s holds 10 samples");
        assert_eq!(ts[w[0].start], 330_000);
        assert_eq!(ts[w[0].end - 1], 600_000);
    }

    #[test]
    fn the_left_bound_is_exclusive_and_the_right_inclusive() {
        // Samples exactly on both edges of the window.
        let ts = vec![0, 100, 200, 300];
        let w = windows(
            &ts,
            Grid {
                start: 300,
                step: 1,
                steps: 1,
                range: 300,
            },
        );
        // (0, 300]: drops the sample at 0, keeps the one at 300.
        assert_eq!(&ts[w[0].clone()], &[100, 200, 300]);
    }

    #[test]
    fn windows_slide_without_rescanning() {
        let ts = scrape(240);
        let grid = Grid {
            start: 300_000,
            step: 15_000,
            steps: 200,
            range: 300_000,
        };
        let w = windows(&ts, grid);

        assert_eq!(w.len(), 200);
        for (s, r) in w.iter().enumerate() {
            let t = grid.at(s);
            // Every sample in the range is inside the window, and the neighbours are not.
            for &v in &ts[r.clone()] {
                assert!(v > t - grid.range && v <= t, "step {s}: {v} outside window");
            }
            if r.start > 0 {
                assert!(
                    ts[r.start - 1] <= t - grid.range,
                    "step {s}: left bound loose"
                );
            }
            if r.end < ts.len() {
                assert!(ts[r.end] > t, "step {s}: right bound loose");
            }
        }
    }

    #[test]
    fn a_window_before_any_sample_is_empty() {
        let ts = scrape(10);
        let w = windows(
            &ts,
            Grid {
                start: -1_000_000,
                step: 15_000,
                steps: 2,
                range: 300_000,
            },
        );
        assert!(w.iter().all(|r| r.is_empty()));
    }

    #[test]
    fn a_window_past_the_last_sample_keeps_what_is_in_range() {
        let ts = scrape(10); // last sample at 270_000
        let w = windows(
            &ts,
            Grid {
                start: 400_000,
                step: 15_000,
                steps: 1,
                range: 300_000,
            },
        );
        // (100_000, 400_000] catches samples from 120_000 to 270_000.
        assert_eq!(
            &ts[w[0].clone()],
            &[120_000, 150_000, 180_000, 210_000, 240_000, 270_000]
        );
    }

    #[test]
    fn no_samples_at_all_is_not_a_panic() {
        assert_eq!(
            windows(
                &[],
                Grid {
                    start: 0,
                    step: 1,
                    steps: 3,
                    range: 10
                }
            ),
            vec![0..0, 0..0, 0..0]
        );
    }
}
