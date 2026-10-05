//! Port of `github.com/prometheus/common/model`.

pub mod time;

pub use time::{secs_to_millis, Duration, ParseDurationError};
