//! Byte-offset position ranges, mirroring `upstream/posrange/posrange.go`.
//!
//! Upstream defines a `Pos = int` type (signed, but with only non-negative
//! values used in practice) and a `PositionRange { Start, End Pos }` struct.
//! The Rust mirror uses `u32` for byte offsets — matching grmtools' span
//! representation — and keeps the struct shape identical.

use std::ops::Range;

/// Byte offset into the source query string.
pub type Pos = u32;

/// Half-open byte range into the source string, `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PositionRange {
    pub start: Pos,
    pub end: Pos,
}

impl PositionRange {
    pub const fn new(start: Pos, end: Pos) -> Self {
        Self { start, end }
    }

    /// Construct a range starting at `start` with byte-length `len`.
    pub const fn at(start: Pos, len: usize) -> Self {
        Self {
            start,
            end: start + len as Pos,
        }
    }

    /// Merge the leftmost start and rightmost end.
    pub fn merge(first: Self, last: Self) -> Self {
        Self {
            start: first.start,
            end: last.end,
        }
    }

    pub fn as_range(&self) -> Range<usize> {
        self.start as usize..self.end as usize
    }
}
