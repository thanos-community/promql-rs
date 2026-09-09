//! A real `rate`, to show what the layout choice does and does not change.
//!
//! This is the arithmetic the benchmark fakes. It is here for one reason: to demonstrate that a
//! range-vector function takes two slices and a window, and never learns whether those bounds came
//! from a DataFusion `RANGE` frame or from [`super::window::windows`]. Giving up UDWFs does not
//! cost a line of this file.
//!
//! Faithful to Prometheus's `extrapolatedRate`, including the extrapolation to the window edges
//! that makes `rate` over a partially-covered window not simply `delta / elapsed`.

use super::window::Grid;

/// Counter resets summed as increases, per PromQL. A counter that drops is assumed to have
/// restarted from zero, so the pre-drop value is credited in full.
fn delta_with_resets(values: &[f64]) -> f64 {
    let mut total = 0.0;
    for w in values.windows(2) {
        if w[1] < w[0] {
            // Reset: the counter restarted, so everything up to the drop counts.
            total += w[1];
        } else {
            total += w[1] - w[0];
        }
    }
    total
}

/// `rate` / `increase` over one window.
///
/// `ts` and `values` are the window's samples, positionally aligned and sorted, which is the
/// guarantee phase 1 owes and the schema encodes. `per_second` distinguishes `rate` from
/// `increase`.
///
/// Returns `None` for fewer than two samples, which is PromQL's behaviour: a rate needs an
/// interval to divide by.
pub fn extrapolated_rate(
    ts: &[i64],
    values: &[f64],
    window_end: i64,
    range: i64,
    per_second: bool,
) -> Option<f64> {
    if ts.len() < 2 || ts.len() != values.len() {
        return None;
    }

    let window_start = window_end - range;
    let mut result = delta_with_resets(values);

    let sampled_first = ts[0];
    let sampled_last = ts[ts.len() - 1];
    let sampled_span = (sampled_last - sampled_first) as f64;
    if sampled_span <= 0.0 {
        return None;
    }

    // Average distance between samples, used to decide how far it is safe to extrapolate.
    let mean_interval = sampled_span / (ts.len() - 1) as f64;
    let extrapolation_limit = mean_interval * 1.1;

    // How much of the window sits outside the samples we actually have, at each end.
    let mut to_start = (sampled_first - window_start) as f64;
    let mut to_end = (window_end - sampled_last) as f64;

    // Extrapolate at most a sample-and-a-bit past the edge; beyond that assume the series simply
    // does not extend, rather than inventing counter increase that was never observed.
    if to_start > extrapolation_limit {
        to_start = mean_interval / 2.0;
    }
    if to_end > extrapolation_limit {
        to_end = mean_interval / 2.0;
    }

    result *= (sampled_span + to_start + to_end) / sampled_span;

    if per_second {
        result /= range as f64 / 1000.0;
    }
    Some(result)
}

/// `rate(x[range])` across a whole step grid, one pass over the series.
///
/// This is the shape the row-per-series layout wants: a series in, a step grid out, the window
/// bounds found by a sweep rather than a frame. Note that nothing below reaches for DataFusion,
/// so the layout decision is invisible from here.
pub fn rate_steps(ts: &[i64], values: &[f64], grid: Grid, per_second: bool) -> Vec<Option<f64>> {
    super::window::windows(ts, grid)
        .into_iter()
        .enumerate()
        .map(|(s, r)| {
            extrapolated_rate(
                &ts[r.clone()],
                &values[r],
                grid.at(s),
                grid.range,
                per_second,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A counter climbing by 10 every 30s, so the true rate is 1/3 per second.
    fn counter(n: usize) -> (Vec<i64>, Vec<f64>) {
        (
            (0..n as i64).map(|i| i * 30_000).collect(),
            (0..n).map(|i| i as f64 * 10.0).collect(),
        )
    }

    #[test]
    fn a_fully_covered_window_gives_the_true_rate() {
        let (ts, vs) = counter(21);
        // Window (5m, 10m] is fully inside the samples, so no extrapolation applies.
        let got = extrapolated_rate(&ts[11..21], &vs[11..21], 600_000, 300_000, true).unwrap();
        // 9 intervals of 10 over 300s, extrapolated across the full window.
        assert!((got - 1.0 / 3.0).abs() < 0.02, "expected ~0.333, got {got}");
    }

    #[test]
    fn increase_over_a_full_window_is_the_counter_delta() {
        let (ts, vs) = counter(21);
        let got = extrapolated_rate(&ts[11..21], &vs[11..21], 600_000, 300_000, false).unwrap();
        // 10 samples, 9 gaps of 10 each, extrapolated to the window edges.
        assert!((got - 100.0).abs() < 5.0, "expected ~100, got {got}");
    }

    #[test]
    fn a_counter_reset_is_credited_not_subtracted() {
        // Climbs, resets to 0, climbs again. A naive last-minus-first would go negative.
        let ts: Vec<i64> = (0..5).map(|i| i * 30_000).collect();
        let vs = vec![100.0, 200.0, 5.0, 10.0, 15.0];
        let got = extrapolated_rate(&ts, &vs, 120_000, 120_000, false).unwrap();
        assert!(got > 0.0, "a reset must not produce a negative increase");
        // 100 climbed before the reset, 5 credited at it, 10 after.
        assert!((got - 115.0).abs() < 20.0, "expected ~115, got {got}");
    }

    #[test]
    fn fewer_than_two_samples_has_no_rate() {
        assert!(extrapolated_rate(&[0], &[1.0], 0, 300_000, true).is_none());
        assert!(extrapolated_rate(&[], &[], 0, 300_000, true).is_none());
    }

    #[test]
    fn misaligned_slices_are_rejected_rather_than_read_past() {
        assert!(extrapolated_rate(&[0, 1, 2], &[1.0, 2.0], 2, 10, true).is_none());
    }

    #[test]
    fn every_step_of_a_grid_sees_the_same_rate_for_a_steady_counter() {
        let (ts, vs) = counter(240);
        let grid = Grid {
            start: 600_000,
            step: 15_000,
            steps: 100,
            range: 300_000,
        };
        let got = rate_steps(&ts, &vs, grid, true);

        assert_eq!(got.len(), 100);
        for (s, v) in got.iter().enumerate() {
            let v = v.expect("a steady counter has a rate at every step");
            assert!((v - 1.0 / 3.0).abs() < 0.02, "step {s}: got {v}");
        }
    }

    /// The point of the whole exercise: the value does not depend on how the bounds were found.
    #[test]
    fn a_swept_window_agrees_with_one_computed_by_hand() {
        let (ts, vs) = counter(60);
        let grid = Grid {
            start: 900_000,
            step: 30_000,
            steps: 10,
            range: 300_000,
        };

        let swept = rate_steps(&ts, &vs, grid, true);
        assert_eq!(swept.len(), grid.steps);
        for (s, got) in swept.iter().enumerate() {
            let t = grid.at(s);
            // The bounds a RANGE frame would have handed us, found by filtering instead.
            let idx: Vec<usize> = (0..ts.len())
                .filter(|&i| ts[i] > t - grid.range && ts[i] <= t)
                .collect();
            let hand_ts: Vec<i64> = idx.iter().map(|&i| ts[i]).collect();
            let hand_vs: Vec<f64> = idx.iter().map(|&i| vs[i]).collect();
            let expected = extrapolated_rate(&hand_ts, &hand_vs, t, grid.range, true);
            assert_eq!(
                *got, expected,
                "step {s} disagrees with hand-computed bounds"
            );
        }
    }
}
