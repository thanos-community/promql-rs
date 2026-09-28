//! Port of `model/time.go`'s `Duration` type from
//! `github.com/prometheus/common` v0.67.5 (the version
//! `promql-conformance`'s pinned Prometheus commit vendors, per its
//! `go.mod`). Only `Duration`, `ParseDuration` and `Duration.String` are
//! ported; the rest of `time.go` (the `Time` type, JSON/YAML marshaling)
//! has no PromQL-parsing caller here.
//!
//! Stored as nanoseconds (`i64`), not milliseconds, because Go's overflow
//! check runs in nanosecond units (`unitMap`'s multipliers are
//! `time.Duration` values, and the accumulator is compared against
//! `1<<63-1` nanoseconds). A millisecond-denominated accumulator would
//! reject and accept a different set of large durations than upstream
//! does (e.g. `294y` overflows in ns but not in ms), so nanoseconds is
//! the only representation that reproduces Go's rejection boundary.

use std::fmt;
use std::str::FromStr;
use std::time::Duration as StdDuration;

use thiserror::Error;

/// Port of `model.Duration` (`type Duration time.Duration`). Wraps a
/// count of nanoseconds; negative values only arise via
/// [`Duration::parse_allow_negative`], mirroring
/// `model.ParseDurationAllowNegative`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Duration(i64);

/// Port of the error strings `model.ParseDuration` returns. Upstream
/// returns plain `fmt.Errorf`/`errors.New` values; we keep the same
/// messages so error text a reviewer diffs against the Go source still
/// matches.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseDurationError {
    #[error("not a valid duration string: {0:?}")]
    NotADuration(String),
    #[error("empty duration string")]
    Empty,
    #[error("unknown unit {unit:?} in duration {orig:?}")]
    UnknownUnit { unit: String, orig: String },
    #[error("duration out of range")]
    OutOfRange,
}

/// One entry of Go's `unitMap`: `pos` orders units from biggest (1) to
/// smallest (7) so parsing can reject out-of-order units (`1m1h`), and
/// `mult_nanos` is the unit's length in nanoseconds.
struct UnitInfo {
    pos: u8,
    mult_nanos: u64,
}

/// Port of `unitMap`. Units must appear in strictly descending
/// significance (`pos` strictly increasing as the scan moves through the
/// string) — this is what makes `1m1h` an error instead of silently
/// meaning "1 minute + 1 hour": without the order check, a typo like
/// swapping `h` and `m` would parse instead of failing.
fn unit_info(unit: &str) -> Option<UnitInfo> {
    Some(match unit {
        "ms" => UnitInfo { pos: 7, mult_nanos: 1_000_000 },
        "s" => UnitInfo { pos: 6, mult_nanos: 1_000_000_000 },
        "m" => UnitInfo { pos: 5, mult_nanos: 60 * 1_000_000_000 },
        "h" => UnitInfo { pos: 4, mult_nanos: 3600 * 1_000_000_000 },
        "d" => UnitInfo { pos: 3, mult_nanos: 24 * 3600 * 1_000_000_000 },
        "w" => UnitInfo { pos: 2, mult_nanos: 7 * 24 * 3600 * 1_000_000_000 },
        "y" => UnitInfo { pos: 1, mult_nanos: 365 * 24 * 3600 * 1_000_000_000 },
        _ => return None,
    })
}

fn is_digit(b: u8) -> bool {
    b.is_ascii_digit()
}

impl Duration {
    /// Port of `model.ParseDuration`. Assumes a year is always 365d, a
    /// week always 7d, a day always 24h. Negative durations are not
    /// supported here (see [`Duration::parse_allow_negative`]).
    pub fn parse(s: &str) -> Result<Duration, ParseDurationError> {
        match s {
            // Allow 0 without a unit.
            "0" => return Ok(Duration(0)),
            "" => return Err(ParseDurationError::Empty),
            _ => {}
        }

        let orig = s;
        let bytes = s.as_bytes();
        let mut i = 0usize;
        let mut dur: u64 = 0;
        let mut last_unit_pos: u8 = 0;

        while i < bytes.len() {
            if !is_digit(bytes[i]) {
                return Err(ParseDurationError::NotADuration(orig.to_string()));
            }
            // Consume [0-9]*.
            let num_start = i;
            while i < bytes.len() && is_digit(bytes[i]) {
                i += 1;
            }
            let v: u64 = s[num_start..i]
                .parse()
                .map_err(|_| ParseDurationError::NotADuration(orig.to_string()))?;

            // Consume the unit: everything up to the next digit.
            let unit_start = i;
            while i < bytes.len() && !is_digit(bytes[i]) {
                i += 1;
            }
            if i == unit_start {
                return Err(ParseDurationError::NotADuration(orig.to_string()));
            }
            let unit = &s[unit_start..i];
            let info = unit_info(unit).ok_or_else(|| ParseDurationError::UnknownUnit {
                unit: unit.to_string(),
                orig: orig.to_string(),
            })?;
            // Units must go in order from biggest to smallest.
            if info.pos <= last_unit_pos {
                return Err(ParseDurationError::NotADuration(orig.to_string()));
            }
            last_unit_pos = info.pos;

            // Check if the provided duration overflows time.Duration
            // (> ~290 years), same two-step check as upstream: first the
            // per-unit multiply, then the running total.
            if v > (1u64 << 63) / info.mult_nanos {
                return Err(ParseDurationError::OutOfRange);
            }
            dur += v * info.mult_nanos;
            if dur > (1u64 << 63) - 1 {
                return Err(ParseDurationError::OutOfRange);
            }
        }

        Ok(Duration(dur as i64))
    }

    /// Port of `model.ParseDurationAllowNegative`.
    pub fn parse_allow_negative(s: &str) -> Result<Duration, ParseDurationError> {
        match s.strip_prefix('-') {
            None => Duration::parse(s),
            Some(rest) => Duration::parse(rest).map(|d| Duration(-d.0)),
        }
    }

    /// Nanoseconds, the type's internal unit (see the module doc comment
    /// for why nanoseconds and not milliseconds).
    pub fn as_nanos_i64(&self) -> i64 {
        self.0
    }

    /// Milliseconds, truncating any sub-millisecond remainder. `String()`
    /// itself only ever operates at ms resolution or coarser (the
    /// smallest parseable unit is `ms`), so this loses nothing a duration
    /// produced by [`Duration::parse`] carries.
    pub fn as_millis(&self) -> i64 {
        self.0 / 1_000_000
    }

    /// The inverse of [`Duration::as_millis`], for a caller that only has
    /// a raw millisecond count — the engine's UDF arguments are `i64`
    /// milliseconds, not a `Duration`, and this is how their planner
    /// renderer turns one back into `5m` instead of `300000`.
    pub fn from_millis(ms: i64) -> Duration {
        Duration(ms * 1_000_000)
    }

    /// Port of `time.Duration.Seconds()`, the unit `promql-parser`'s
    /// grammar needs at its duration-literal and number-literal actions
    /// (upstream's pinned `generated_parser.y:1101,1120` feed
    /// `dur.Seconds()` straight into those). Go splits into whole
    /// seconds plus a nanosecond remainder divided by 1e9 rather than
    /// dividing the full nanosecond count by 1e9 in one step; the two
    /// give different rounding on some inputs (e.g. `1s118ms` is
    /// `1.1179999999999999` split, `1.118` undivided), so matching Go's
    /// float bit pattern means matching its arithmetic, not just its
    /// intent.
    pub fn as_secs_f64(&self) -> f64 {
        let sec = self.0 / 1_000_000_000;
        let nsec = self.0 % 1_000_000_000;
        sec as f64 + nsec as f64 / 1_000_000_000.0
    }
}

impl FromStr for Duration {
    type Err = ParseDurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Duration::parse(s)
    }
}

impl fmt::Display for Duration {
    /// Port of `Duration.String()`. Years and weeks print only when the
    /// remainder divides exactly — upstream's comment: "it is often
    /// easier to read 90d than 12w6d" — so `90d` stays `90d`, not folded
    /// into weeks.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut ms = self.0 / 1_000_000;

        if ms == 0 {
            return write!(f, "0s");
        }

        let sign = if ms < 0 {
            ms = -ms;
            "-"
        } else {
            ""
        };

        let mut out = String::new();
        // `exact`: only emit this unit if it divides the remainder with
        // no leftover, matching Go's `f("y", ..., exact: true)` closures.
        let mut emit = |unit: &str, mult: i64, exact: bool, ms: &mut i64| {
            if exact && *ms % mult != 0 {
                return;
            }
            let v = *ms / mult;
            if v > 0 {
                out.push_str(&v.to_string());
                out.push_str(unit);
                *ms -= v * mult;
            }
        };

        emit("y", 1000 * 60 * 60 * 24 * 365, true, &mut ms);
        emit("w", 1000 * 60 * 60 * 24 * 7, true, &mut ms);
        emit("d", 1000 * 60 * 60 * 24, false, &mut ms);
        emit("h", 1000 * 60 * 60, false, &mut ms);
        emit("m", 1000 * 60, false, &mut ms);
        emit("s", 1000, false, &mut ms);
        emit("ms", 1, false, &mut ms);

        write!(f, "{sign}{out}")
    }
}

impl TryFrom<Duration> for StdDuration {
    type Error = std::num::TryFromIntError;

    /// `std::time::Duration` has no sign; a negative `Duration` (only
    /// reachable via `parse_allow_negative`) does not fit.
    fn try_from(d: Duration) -> Result<Self, Self::Error> {
        let nanos: u64 = u64::try_from(d.0)?;
        Ok(StdDuration::from_nanos(nanos))
    }
}

impl TryFrom<StdDuration> for Duration {
    type Error = std::num::TryFromIntError;

    /// `std::time::Duration` can exceed `i64` nanoseconds (its range goes
    /// to ~584 years); this rejects what doesn't fit rather than
    /// silently truncating.
    fn try_from(d: StdDuration) -> Result<Self, Self::Error> {
        let nanos = i64::try_from(d.as_nanos())?;
        Ok(Duration(nanos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim port of `TestParseDuration`'s `baseCases` +
    /// `negativeCases` table (time_test.go), collapsed to
    /// (input, expected_nanos, expected_string) since we split
    /// positive/negative parsing into two functions rather than a
    /// boolean flag.
    #[test]
    fn parse_duration_base_cases() {
        let cases: &[(&str, i64, &str)] = &[
            ("0", 0, "0s"),
            ("0w", 0, "0s"),
            ("0s", 0, "0s"),
            ("324ms", 324 * 1_000_000, "324ms"),
            ("3s", 3 * 1_000_000_000, "3s"),
            ("5m", 5 * 60 * 1_000_000_000, "5m"),
            ("1h", 3600 * 1_000_000_000, "1h"),
            ("4d", 4 * 24 * 3600 * 1_000_000_000, "4d"),
            ("4d1h", 4 * 24 * 3600 * 1_000_000_000 + 3600 * 1_000_000_000, "4d1h"),
            ("14d", 14 * 24 * 3600 * 1_000_000_000, "2w"),
            ("3w", 3 * 7 * 24 * 3600 * 1_000_000_000, "3w"),
            (
                "3w2d1h",
                3 * 7 * 24 * 3600 * 1_000_000_000 + 2 * 24 * 3600 * 1_000_000_000 + 3600 * 1_000_000_000,
                "23d1h",
            ),
            ("10y", 10 * 365 * 24 * 3600 * 1_000_000_000, "10y"),
        ];

        for &(input, want_nanos, want_string) in cases {
            let d = Duration::parse(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(d.as_nanos_i64(), want_nanos, "{input}");
            assert_eq!(d.to_string(), want_string, "{input}");

            // Round-trip: the negative-parser must agree on magnitude too.
            let d_neg = Duration::parse_allow_negative(input).unwrap();
            assert_eq!(d_neg, d, "{input} via parse_allow_negative");
        }
    }

    /// Port of `TestParseDuration`'s `negativeCases` table.
    #[test]
    fn parse_duration_negative_cases() {
        let cases: &[(&str, i64, &str)] = &[
            ("-3s", -3 * 1_000_000_000, "-3s"),
            ("-5m", -5 * 60 * 1_000_000_000, "-5m"),
            ("-1h", -3600 * 1_000_000_000, "-1h"),
            ("-2d", -2 * 24 * 3600 * 1_000_000_000, "-2d"),
            ("-1w", -7 * 24 * 3600 * 1_000_000_000, "-1w"),
            (
                "-3w2d1h",
                -(3 * 7 * 24 * 3600 * 1_000_000_000 + 2 * 24 * 3600 * 1_000_000_000 + 3600 * 1_000_000_000),
                "-23d1h",
            ),
            ("-10y", -10 * 365 * 24 * 3600 * 1_000_000_000, "-10y"),
        ];

        for &(input, want_nanos, want_string) in cases {
            let d = Duration::parse_allow_negative(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(d.as_nanos_i64(), want_nanos, "{input}");
            assert_eq!(d.to_string(), want_string, "{input}");
        }
    }

    /// Verbatim port of `TestParseBadDuration`'s case list.
    #[test]
    fn parse_bad_duration() {
        let cases = [
            "1",
            "1y1m1d",
            "1.5d",
            "d",
            "294y",
            "200y10400w",
            "107675d",
            "2584200h",
            "",
        ];
        for c in cases {
            assert!(Duration::parse(c).is_err(), "expected error for {c:?}");
        }
    }

    /// `289y` sits just under the overflow boundary; port of the
    /// `TestDuration_UnmarshalJSON` case that exercises it (accepted,
    /// unlike `294y` above).
    #[test]
    fn parse_duration_near_overflow_boundary_accepted() {
        let d = Duration::parse("289y").unwrap();
        assert_eq!(d.as_nanos_i64(), 289 * 365 * 24 * 3600 * 1_000_000_000);
    }

    #[test]
    fn round_trip_parse_format() {
        let cases = [
            "0s", "324ms", "3s", "5m", "1h", "4d", "4d1h", "2w", "3w", "23d1h", "10y",
            "-3s", "-23d1h",
        ];
        for c in cases {
            let d = Duration::parse_allow_negative(c).unwrap();
            assert_eq!(d.to_string(), c, "round trip for {c:?}");
            let d2 = Duration::parse_allow_negative(&d.to_string()).unwrap();
            assert_eq!(d2, d);
        }
    }

    #[test]
    fn from_str_matches_parse() {
        assert_eq!("5m".parse::<Duration>().unwrap(), Duration::parse("5m").unwrap());
    }

    #[test]
    fn std_duration_conversion() {
        let d = Duration::parse("1h30m").unwrap();
        let std_d: StdDuration = d.try_into().unwrap();
        assert_eq!(std_d, StdDuration::from_secs(5400));
        let back: Duration = std_d.try_into().unwrap();
        assert_eq!(back, d);

        let neg = Duration::parse_allow_negative("-1h").unwrap();
        assert!(StdDuration::try_from(neg).is_err());
    }

    #[test]
    fn from_millis_round_trips_through_as_millis() {
        assert_eq!(Duration::from_millis(300_000).to_string(), "5m");
        assert_eq!(Duration::from_millis(90_000).to_string(), "1m30s");
        assert_eq!(Duration::from_millis(300_000).as_millis(), 300_000);
    }

    #[test]
    fn as_secs_f64() {
        assert_eq!(Duration::parse("30s").unwrap().as_secs_f64(), 30.0);
        assert_eq!(Duration::parse("500ms").unwrap().as_secs_f64(), 0.5);
    }

    /// `time.Duration.Seconds()` splits into whole seconds plus a
    /// nanosecond remainder divided by 1e9, rather than dividing the
    /// full nanosecond count by 1e9 in one step; the two disagree by one
    /// ulp on this input. Computed the Go way here (not hardcoded) so the
    /// assertion documents the arithmetic, not just a magic literal.
    #[test]
    fn as_secs_f64_matches_go_split_not_undivided() {
        let d = Duration::parse("1s118ms").unwrap();
        let nanos = d.as_nanos_i64();
        let go_way = (nanos / 1_000_000_000) as f64 + (nanos % 1_000_000_000) as f64 / 1_000_000_000.0;
        let undivided = nanos as f64 / 1_000_000_000.0;
        assert_ne!(go_way, undivided, "test input must actually exercise the rounding difference");
        assert_eq!(d.as_secs_f64(), go_way);
        assert_eq!(d.as_secs_f64(), 1.1179999999999999);
    }
}
