//! The step grid: the one definition of where a sample may sit.
//!
//! Everything the engine emits — a selector, a range function, an
//! aggregation, a binary operator — produces samples at
//! `start + i * step` and nowhere else. That makes a timestamp an array
//! index, which is what lets an accumulator be a flat array rather than a
//! map, and what lets two operands be lined up by position.
//!
//! The grid carries the name of the function using it, so an off-grid
//! timestamp — a bug in whatever produced the input — is reported against
//! the operator that noticed.

use datafusion::error::{DataFusionError, Result};

#[derive(Debug, Clone, Copy)]
pub struct Grid {
    pub start_ms: i64,
    pub step_ms: i64,
    pub len: usize,
    name: &'static str,
}

impl Grid {
    pub fn new(name: &'static str, start_ms: i64, end_ms: i64, step_ms: i64) -> Result<Self> {
        if step_ms <= 0 {
            return Err(DataFusionError::Execution(format!(
                "{name}: step must be positive, got {step_ms}ms"
            )));
        }
        let len = if end_ms < start_ms {
            0
        } else {
            usize::try_from((end_ms - start_ms) / step_ms + 1).map_err(|_| {
                DataFusionError::Execution(format!(
                    "{name}: {start_ms}..{end_ms} has too many steps"
                ))
            })?
        };
        Ok(Self {
            start_ms,
            step_ms,
            len,
            name,
        })
    }

    pub fn timestamp(&self, index: usize) -> i64 {
        self.start_ms + index as i64 * self.step_ms
    }

    pub fn index(&self, ts: i64) -> Result<usize> {
        let offset = ts - self.start_ms;
        let index = offset / self.step_ms;
        if offset < 0 || offset % self.step_ms != 0 || index as usize >= self.len {
            return Err(DataFusionError::Execution(format!(
                "{}: sample at {ts}ms is not on the step grid {}..{} every {}ms",
                self.name,
                self.start_ms,
                self.timestamp(self.len.saturating_sub(1)),
                self.step_ms
            )));
        }
        Ok(index as usize)
    }

    /// Every step timestamp, in order.
    pub fn timestamps(&self) -> impl Iterator<Item = i64> + '_ {
        (0..self.len).map(|i| self.timestamp(i))
    }
}
