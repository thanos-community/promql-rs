//! The PromQL query being asked, as numbers.
//!
//! `rate(apiserver_request_total[5m])` at one instant, or across a step grid. The shape decides
//! how many samples a series has to carry and which window bounds the operator computes; nothing
//! else in the pipeline depends on it.

use super::scan::SCRAPE_MS;
use super::window::Grid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Query {
    /// Window width, the `[5m]`.
    pub range_ms: i64,
    /// Steps in the grid, `1` for an instant query.
    pub steps: usize,
    /// Distance between steps, `0` for an instant query.
    pub step_ms: i64,
    /// Evaluation time, which is the last step of a range query.
    pub at: i64,
    /// `rate` rather than `increase`.
    pub per_second: bool,
}

impl Query {
    /// `rate(m[range])` or `increase(m[range])` at one instant.
    pub fn instant(range_ms: i64, per_second: bool) -> Self {
        Self {
            range_ms,
            steps: 1,
            step_ms: 0,
            at: 0,
            per_second,
        }
    }

    /// `rate(m[range])` across `steps` steps `step_ms` apart.
    pub fn range(range_ms: i64, steps: usize, step_ms: i64, per_second: bool) -> Self {
        Self {
            range_ms,
            steps,
            step_ms,
            at: 0,
            per_second,
        }
    }

    /// Evaluate at `at`.
    pub fn at(mut self, at: i64) -> Self {
        self.at = at;
        self
    }

    pub fn is_instant(&self) -> bool {
        self.steps == 1
    }

    /// The step grid, ending at `at`.
    pub fn grid(&self) -> Grid {
        Grid {
            start: self.at - (self.steps as i64 - 1) * self.step_ms,
            step: self.step_ms,
            steps: self.steps,
            range: self.range_ms,
        }
    }

    /// Samples per series a scan at [`SCRAPE_MS`] has to hold for every window of this query to
    /// be fully covered, so no step sees a partial window.
    pub fn samples(&self) -> usize {
        let span = self.range_ms + (self.steps as i64 - 1) * self.step_ms;
        ((span + SCRAPE_MS - 1) / SCRAPE_MS) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_5m_instant_query_needs_ten_samples() {
        assert_eq!(Query::instant(300_000, true).samples(), 10);
    }

    #[test]
    fn a_range_query_needs_its_span_plus_the_window() {
        // 1h at 15s is 240 steps; the first window starts 5m before the first step.
        assert_eq!(Query::range(300_000, 240, 15_000, true).samples(), 130);
    }

    #[test]
    fn the_grid_ends_at_the_evaluation_time() {
        let g = Query::range(300_000, 4, 15_000, true).at(1_000_000).grid();
        assert_eq!(g.at(3), 1_000_000);
        assert_eq!(g.start, 955_000);
    }
}
