//! Port of `github.com/prometheus/common/model`.

pub mod time;

pub use time::{timestamp_from_float_seconds, Duration, ParseDurationError, TimestampOutOfBounds};
