//! Port of `model/time.go`'s `Duration` type from
//! `github.com/prometheus/common` v0.67.5 (the version
//! `promql-conformance`'s pinned Prometheus commit vendors, per its
//! `go.mod`). Only `Duration`, `ParseDuration` and `Duration.String` are
//! ported; the rest of `time.go` (the `Time` type, JSON/YAML marshaling)
//! has no PromQL-parsing caller here.
//!
//! Wraps `chrono::TimeDelta` rather than a bare `i64` nanosecond count:
//! `TimeDelta` already carries the accumulator, the arithmetic and the
//! `Send + Sync + Copy` newtype semantics this type needs, and chrono is
//! already in the dependency tree via arrow/DataFusion. `TimeDelta`'s own
//! range is wider than Go's (it stores seconds and sub-second nanos
//! separately, so it can hold millisecond-scale durations `i64` nanos
//! cannot), so every constructor here still enforces Go's boundary,
//! ±(1<<63−1) nanoseconds — the same bound `model.ParseDuration`'s
//! overflow check compares against (`unitMap`'s multipliers are
//! `time.Duration` values, and the accumulator is compared against
//! `1<<63-1` nanoseconds). Letting a `TimeDelta` outside that range in
//! would make [`Duration::as_nanos_i64`] lossy for a value this port
//! itself could never have produced by parsing.

use std::fmt;
use std::str::FromStr;
use std::time::Duration as StdDuration;

use chrono::TimeDelta;
use thiserror::Error;

/// Port of `model.Duration` (`type Duration time.Duration`). Every
/// constructor enforces Go's range, ±(1<<63−1) nanoseconds, even though
/// the wrapped `TimeDelta` could hold more (see the module doc comment);
/// negative values only arise via [`Duration::parse_allow_negative`],
/// mirroring `model.ParseDurationAllowNegative`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Duration(TimeDelta);

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

/// One entry of a unit table: `pos` orders units from biggest to
/// smallest so parsing can reject out-of-order units (`1m1h`), and
/// `mult_nanos` is the unit's length in nanoseconds.
struct UnitInfo {
    pos: u8,
    mult_nanos: u64,
}

/// Port of `unitMap`, restricted to the units `model.ParseDuration`
/// itself accepts (`y w d h m s ms`). `Duration::parse` — and so the
/// PromQL grammar, which calls it — uses only this table: upstream's
/// parser rejects `rate(x[5us])` the same way, and matching that means
/// not accepting a finer unit here that Go's grammar never had.
fn unit_info_basic(unit: &str) -> Option<UnitInfo> {
    Some(match unit {
        "ms" => UnitInfo {
            pos: 7,
            mult_nanos: 1_000_000,
        },
        "s" => UnitInfo {
            pos: 6,
            mult_nanos: 1_000_000_000,
        },
        "m" => UnitInfo {
            pos: 5,
            mult_nanos: 60 * 1_000_000_000,
        },
        "h" => UnitInfo {
            pos: 4,
            mult_nanos: 3600 * 1_000_000_000,
        },
        "d" => UnitInfo {
            pos: 3,
            mult_nanos: 24 * 3600 * 1_000_000_000,
        },
        "w" => UnitInfo {
            pos: 2,
            mult_nanos: 7 * 24 * 3600 * 1_000_000_000,
        },
        "y" => UnitInfo {
            pos: 1,
            mult_nanos: 365 * 24 * 3600 * 1_000_000_000,
        },
        _ => return None,
    })
}

/// Port of Go's full `time.ParseDuration` `unitMap`: `unit_info_basic`'s
/// table plus the sub-millisecond units and their accepted spellings
/// (`us`, the U+00B5 MICRO SIGN and U+03BC GREEK SMALL LETTER MU
/// spellings of `µs`/`μs`, and `ns`). `Duration::parse_nanos` uses this
/// table for `Display`'s round trip and for callers that need
/// nanosecond-precision input (never the PromQL grammar — see
/// `unit_info_basic`).
fn unit_info_nanos(unit: &str) -> Option<UnitInfo> {
    match unit {
        "us" | "µs" | "μs" => Some(UnitInfo {
            pos: 8,
            mult_nanos: 1_000,
        }),
        "ns" => Some(UnitInfo {
            pos: 9,
            mult_nanos: 1,
        }),
        _ => unit_info_basic(unit),
    }
}

fn is_digit(c: char) -> bool {
    c.is_ascii_digit()
}

impl Duration {
    /// Shared scanner behind [`Duration::parse`] and
    /// [`Duration::parse_nanos`]: one unit table parameterizes the same
    /// digit/unit walk so the two entry points can't drift on the rules
    /// (descending significance, each unit at most once, digits only,
    /// bare `0`, empty rejected, overflow rejected). Iterates by `char`,
    /// not by byte, because the nanosecond table's `µs`/`μs` spellings
    /// are multi-byte UTF-8 — indexing by byte offset the way a
    /// byte-only scanner would risks splitting one of those units mid
    /// character.
    fn parse_with(
        s: &str,
        unit_info: fn(&str) -> Option<UnitInfo>,
    ) -> Result<Duration, ParseDurationError> {
        match s {
            // Allow 0 without a unit.
            "0" => return Ok(Duration(TimeDelta::zero())),
            "" => return Err(ParseDurationError::Empty),
            _ => {}
        }

        let orig = s;
        let mut chars = s.char_indices().peekable();
        let mut dur: u64 = 0;
        let mut last_unit_pos: u8 = 0;

        while let Some(&(num_start, c)) = chars.peek() {
            if !is_digit(c) {
                return Err(ParseDurationError::NotADuration(orig.to_string()));
            }
            // Consume [0-9]*.
            let mut num_end = num_start;
            while let Some(&(idx, c)) = chars.peek() {
                if !is_digit(c) {
                    break;
                }
                num_end = idx + c.len_utf8();
                chars.next();
            }
            let v: u64 = s[num_start..num_end]
                .parse()
                .map_err(|_| ParseDurationError::NotADuration(orig.to_string()))?;

            // Consume the unit: everything up to the next digit.
            let unit_start = num_end;
            let mut unit_end = unit_start;
            while let Some(&(idx, c)) = chars.peek() {
                if is_digit(c) {
                    break;
                }
                unit_end = idx + c.len_utf8();
                chars.next();
            }
            if unit_end == unit_start {
                return Err(ParseDurationError::NotADuration(orig.to_string()));
            }
            let unit = &s[unit_start..unit_end];
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

        Ok(Duration(TimeDelta::nanoseconds(dur as i64)))
    }

    /// Port of `model.ParseDuration`. Assumes a year is always 365d, a
    /// week always 7d, a day always 24h. Negative durations are not
    /// supported here (see [`Duration::parse_allow_negative`]). Units
    /// stop at `ms`, matching upstream's grammar — see
    /// `unit_info_basic`.
    pub fn parse(s: &str) -> Result<Duration, ParseDurationError> {
        Self::parse_with(s, unit_info_basic)
    }

    /// Same grammar and rules as [`Duration::parse`], extended below
    /// `ms` with `us`/`µs`/`μs` and `ns` — the spellings Go's
    /// `time.ParseDuration` accepts beyond `model.ParseDuration`'s more
    /// restricted table. Not used by the PromQL grammar (see
    /// `unit_info_basic`); it exists so [`Duration::to_string`]'s
    /// sub-millisecond output round-trips through parsing.
    pub fn parse_nanos(s: &str) -> Result<Duration, ParseDurationError> {
        Self::parse_with(s, unit_info_nanos)
    }

    /// Port of `model.ParseDurationAllowNegative`.
    pub fn parse_allow_negative(s: &str) -> Result<Duration, ParseDurationError> {
        match s.strip_prefix('-') {
            None => Duration::parse(s),
            Some(rest) => Duration::parse(rest).map(|d| Duration(-d.0)),
        }
    }

    /// Nanoseconds (see the module doc comment for why the range this can
    /// return is narrower than `TimeDelta`'s own). Every constructor in
    /// this module keeps the wrapped `TimeDelta` inside Go's
    /// ±(1<<63−1)-nanosecond range, so `num_nanoseconds` returning `None`
    /// here would mean an invariant this module itself broke.
    pub fn as_nanos_i64(&self) -> i64 {
        self.0
            .num_nanoseconds()
            .expect("Duration invariant: value always fits Go's i64-nanosecond range")
    }

    /// Milliseconds, truncating any sub-millisecond remainder — lossy for
    /// a `Duration` built via [`Duration::parse_nanos`] (or a converted
    /// `TimeDelta`) that carries a genuine sub-ms remainder; a `Duration`
    /// produced by [`Duration::parse`] never has one, since `ms` is its
    /// smallest unit.
    pub fn as_millis(&self) -> i64 {
        self.as_nanos_i64() / 1_000_000
    }

    /// The inverse of [`Duration::as_millis`], for a caller that only has
    /// a raw millisecond count — the engine's UDF arguments are `i64`
    /// milliseconds, not a `Duration`, and this is how their planner
    /// renderer turns one back into `5m` instead of `300000`. Fallible for
    /// the same reason every other constructor here is: an `i64`
    /// millisecond count can name a span outside Go's ±(1<<63−1)-
    /// nanosecond range (`ms * 1_000_000` overflowing `i64` nanoseconds),
    /// and this input is reachable straight from a query's step/lookback/
    /// window/offset literals (`promql-engine/src/explain.rs`), not just
    /// from a trusted internal caller, so it must reject rather than
    /// panic or silently wrap.
    pub fn from_millis(ms: i64) -> Result<Duration, ParseDurationError> {
        let td = TimeDelta::try_milliseconds(ms).ok_or(ParseDurationError::OutOfRange)?;
        Duration::try_from(td)
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
        let nanos = self.as_nanos_i64();
        let sec = nanos / 1_000_000_000;
        let nsec = nanos % 1_000_000_000;
        sec as f64 + nsec as f64 / 1_000_000_000.0
    }
}

/// Float seconds as the milliseconds the engine plans with, rounded as
/// `(secs * 1000.0).round()`. `None` for NaN, ±Inf and a result outside
/// `i64`: `as i64` would saturate those or map NaN to 0, which turns a
/// bad `@`, offset or range into a plausible timestamp.
pub fn secs_to_millis(secs: f64) -> Option<i64> {
    let ms = (secs * 1000.0).round();
    // `i64::MAX as f64` is 2^63, itself out of range, hence `<`.
    (ms >= i64::MIN as f64 && ms < i64::MAX as f64).then_some(ms as i64)
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
    ///
    /// Go's `String()` never sees below `ms` because `time.Duration`'s
    /// own textual round trip lives in `unitMap`'s minimum, `ms`; this
    /// port's `Duration` can hold true nanoseconds (via
    /// [`Duration::parse_nanos`] or a converted `TimeDelta`), so
    /// truncating at `ms` here the way Go does would print two different
    /// `Duration` values — `1ms` and `1ms500ns` — identically. Instead,
    /// once the ms-and-above digits are emitted the same way Go would,
    /// any sub-ms remainder continues in `us`/`ns`, non-zero units only,
    /// so a value that Go's `unitMap` never has to represent still
    /// prints losslessly and round-trips through [`Duration::parse_nanos`].
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_nanos = self.as_nanos_i64();

        if total_nanos == 0 {
            return write!(f, "0s");
        }

        let neg = total_nanos < 0;
        // Safe: `total_nanos != i64::MIN`, since Go's range excludes it
        // (the invariant `as_nanos_i64` documents).
        let total_nanos_abs = total_nanos.unsigned_abs();
        let mut ms = (total_nanos_abs / 1_000_000) as i64;
        let sub_ms_nanos = total_nanos_abs % 1_000_000;

        let sign = if neg { "-" } else { "" };

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

        if sub_ms_nanos > 0 {
            let us = sub_ms_nanos / 1_000;
            let ns = sub_ms_nanos % 1_000;
            if us > 0 {
                out.push_str(&us.to_string());
                out.push_str("us");
            }
            if ns > 0 {
                out.push_str(&ns.to_string());
                out.push_str("ns");
            }
        }

        write!(f, "{sign}{out}")
    }
}

impl From<Duration> for TimeDelta {
    fn from(d: Duration) -> Self {
        d.0
    }
}

impl TryFrom<TimeDelta> for Duration {
    type Error = ParseDurationError;

    /// A `TimeDelta` outside Go's ±(1<<63−1)-nanosecond range is
    /// rejected rather than silently clamped or truncated — see the
    /// module doc comment. `num_nanoseconds` already returns `None`
    /// exactly at that boundary (it overflows `i64` at `i64::MAX + 1`
    /// nanoseconds and `TimeDelta` is otherwise symmetric), except for
    /// `i64::MIN`: that fits in `i64` and is a valid `time.Duration` in
    /// Go, but it is one magnitude past what `ParseDurationAllowNegative`
    /// (the only Go path that ever produces a negative `Duration`) can
    /// reach — it negates a magnitude `ParseDuration` already bounded to
    /// `i64::MAX` — so no value this port constructs by parsing is ever
    /// `i64::MIN`, and this rejects it explicitly rather than accept a
    /// `TimeDelta` no parse path could hand back.
    fn try_from(td: TimeDelta) -> Result<Self, Self::Error> {
        match td.num_nanoseconds() {
            Some(n) if n != i64::MIN => Ok(Duration(td)),
            _ => Err(ParseDurationError::OutOfRange),
        }
    }
}

impl TryFrom<Duration> for StdDuration {
    type Error = std::num::TryFromIntError;

    /// `std::time::Duration` has no sign; a negative `Duration` (only
    /// reachable via `parse_allow_negative`) does not fit.
    fn try_from(d: Duration) -> Result<Self, Self::Error> {
        let nanos: u64 = u64::try_from(d.as_nanos_i64())?;
        Ok(StdDuration::from_nanos(nanos))
    }
}

impl TryFrom<StdDuration> for Duration {
    type Error = std::num::TryFromIntError;

    /// `std::time::Duration` can exceed `i64` nanoseconds (its range goes
    /// to ~584 years); this rejects what doesn't fit rather than
    /// silently truncating. `i64::try_from` succeeding is exactly Go's
    /// positive bound (`i64::MAX` nanoseconds), so no separate range
    /// check is needed here the way [`TryFrom<TimeDelta>`] needs one.
    fn try_from(d: StdDuration) -> Result<Self, Self::Error> {
        let nanos = i64::try_from(d.as_nanos())?;
        Ok(Duration(TimeDelta::nanoseconds(nanos)))
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
            (
                "4d1h",
                4 * 24 * 3600 * 1_000_000_000 + 3600 * 1_000_000_000,
                "4d1h",
            ),
            ("14d", 14 * 24 * 3600 * 1_000_000_000, "2w"),
            ("3w", 3 * 7 * 24 * 3600 * 1_000_000_000, "3w"),
            (
                "3w2d1h",
                3 * 7 * 24 * 3600 * 1_000_000_000
                    + 2 * 24 * 3600 * 1_000_000_000
                    + 3600 * 1_000_000_000,
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
                -(3 * 7 * 24 * 3600 * 1_000_000_000
                    + 2 * 24 * 3600 * 1_000_000_000
                    + 3600 * 1_000_000_000),
                "-23d1h",
            ),
            ("-10y", -10 * 365 * 24 * 3600 * 1_000_000_000, "-10y"),
        ];

        for &(input, want_nanos, want_string) in cases {
            let d =
                Duration::parse_allow_negative(input).unwrap_or_else(|e| panic!("{input}: {e}"));
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
            "0s", "324ms", "3s", "5m", "1h", "4d", "4d1h", "2w", "3w", "23d1h", "10y", "-3s",
            "-23d1h",
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
        assert_eq!(
            "5m".parse::<Duration>().unwrap(),
            Duration::parse("5m").unwrap()
        );
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
        assert_eq!(Duration::from_millis(300_000).unwrap().to_string(), "5m");
        assert_eq!(Duration::from_millis(90_000).unwrap().to_string(), "1m30s");
        assert_eq!(Duration::from_millis(300_000).unwrap().as_millis(), 300_000);
    }

    /// `from_millis` takes a raw `i64` millisecond count straight from a
    /// query's step/lookback/window/offset literals (see its doc
    /// comment), so it must reject what falls outside Go's ±(1<<63−1)-
    /// nanosecond range instead of panicking (`i64::MIN`, inside
    /// `TimeDelta::try_milliseconds` itself) or building a `Duration`
    /// whose `as_nanos_i64` would then panic on its own invariant
    /// (`i64::MAX` milliseconds, which is in range for `TimeDelta` but
    /// overflows `i64` nanoseconds).
    #[test]
    fn from_millis_rejects_out_of_range() {
        assert!(Duration::from_millis(i64::MIN).is_err());
        assert!(Duration::from_millis(i64::MAX).is_err());
        assert!(Duration::from_millis(9_223_372_036_854).is_ok());
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
        let go_way =
            (nanos / 1_000_000_000) as f64 + (nanos % 1_000_000_000) as f64 / 1_000_000_000.0;
        let undivided = nanos as f64 / 1_000_000_000.0;
        assert_ne!(
            go_way, undivided,
            "test input must actually exercise the rounding difference"
        );
        assert_eq!(d.as_secs_f64(), go_way);
        assert_eq!(d.as_secs_f64(), 1.1179999999999999);
    }

    /// Port of Go's `time.ParseDuration`'s sub-millisecond unit table
    /// (`unitMap` in `time/format.go`), which `model.ParseDuration` does
    /// not expose — see [`Duration::parse_nanos`].
    #[test]
    fn parse_nanos_units() {
        let cases: &[(&str, i64)] = &[
            ("1ms500us", 1_500_000),
            ("1us", 1_000),
            ("1µs", 1_000),
            ("1μs", 1_000),
            ("1ns", 1),
            ("1s1ns", 1_000_000_001),
            ("0", 0),
        ];
        for &(input, want_nanos) in cases {
            let d = Duration::parse_nanos(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(d.as_nanos_i64(), want_nanos, "{input}");
        }
    }

    /// `1ns1us` breaks the descending-significance rule (ns is smaller
    /// than us but appears first); `1us1us` repeats a unit. Both are
    /// errors under the same ordering check `parse_bad_duration` proves
    /// for the basic table.
    #[test]
    fn parse_nanos_rejects_bad_order_and_repeats() {
        assert!(Duration::parse_nanos("1ns1us").is_err());
        assert!(Duration::parse_nanos("1us1us").is_err());
    }

    /// `model.ParseDuration`'s grammar stops at `ms`; the PromQL parser
    /// calls `Duration::parse`, never `parse_nanos`, so a query duration
    /// literal with `us` or `ns` is rejected exactly as upstream rejects
    /// it (see `unit_info_basic`).
    #[test]
    fn parse_rejects_sub_millisecond_units() {
        for c in ["5us", "5ns", "1000000ns"] {
            assert!(Duration::parse(c).is_err(), "expected error for {c:?}");
        }
    }

    /// [`Duration::to_string`]'s sub-millisecond continuation (see its
    /// doc comment): `1ms500us` and `1ns` exercise the `us`/`ns` tail,
    /// `1s1ns` exercises a whole-second value with a bare nanosecond
    /// remainder, and `1500ms` proves the ms-and-above rendering is
    /// unchanged from Go's.
    #[test]
    fn display_sub_millisecond_remainder() {
        assert_eq!(
            Duration::parse_nanos("1ms500us").unwrap().to_string(),
            "1ms500us"
        );
        assert_eq!(Duration::parse_nanos("1ns").unwrap().to_string(), "1ns");
        assert_eq!(Duration::parse_nanos("1s1ns").unwrap().to_string(), "1s1ns");
        assert_eq!(Duration::from_millis(1500).unwrap().to_string(), "1s500ms");
    }

    /// `Duration::parse_nanos(d.to_string())` must recover `d` exactly,
    /// for both the basic (ms-and-above) and nanosecond-extended
    /// vocabularies — `Display` only ever emits units both tables
    /// recognize.
    #[test]
    fn round_trip_parse_nanos_format() {
        // Basic-table cases, built with `parse_allow_negative` (signed).
        let basic = [
            "0s", "324ms", "3s", "5m", "1h", "4d", "4d1h", "2w", "3w", "23d1h", "10y", "-3s",
            "-23d1h",
        ];
        // Nanosecond-table-only cases (unsigned; `parse_nanos` has no
        // negative variant).
        let nanos = ["1ms500us", "1ns", "1s1ns", "1s500ms"];

        for c in basic {
            let d = Duration::parse_allow_negative(c).unwrap_or_else(|e| panic!("{c}: {e}"));
            let formatted = d.to_string();
            // `parse_nanos` has no sign handling of its own (only
            // `parse_allow_negative` does, and it stays on the basic
            // table); strip the sign here the same way that wrapper does.
            let (neg, unsigned) = match formatted.strip_prefix('-') {
                Some(rest) => (true, rest),
                None => (false, formatted.as_str()),
            };
            let mut d2 =
                Duration::parse_nanos(unsigned).unwrap_or_else(|e| panic!("{formatted}: {e}"));
            if neg {
                d2 = Duration(-TimeDelta::from(d2));
            }
            assert_eq!(d2, d, "round trip for {c:?} -> {formatted:?}");
        }
        for c in nanos {
            let d = Duration::parse_nanos(c).unwrap_or_else(|e| panic!("{c}: {e}"));
            let formatted = d.to_string();
            let d2 =
                Duration::parse_nanos(&formatted).unwrap_or_else(|e| panic!("{formatted}: {e}"));
            assert_eq!(d2, d, "round trip for {c:?} -> {formatted:?}");
        }
    }

    #[test]
    fn time_delta_conversions() {
        let d = Duration::parse("1h30m").unwrap();
        let td: TimeDelta = d.into();
        assert_eq!(td, TimeDelta::seconds(5400));
        let back = Duration::try_from(td).unwrap();
        assert_eq!(back, d);

        let neg = Duration::parse_allow_negative("-1h").unwrap();
        let neg_td: TimeDelta = neg.into();
        assert_eq!(Duration::try_from(neg_td).unwrap(), neg);

        // Beyond Go's i64-nanosecond range: `TimeDelta::MAX` is bounded
        // by `i64::MAX` *milliseconds*, far past `i64::MAX` nanoseconds.
        assert!(Duration::try_from(TimeDelta::MAX).is_err());
    }

    #[test]
    fn secs_to_millis_rejects_what_as_i64_would_clamp() {
        assert_eq!(secs_to_millis(1.0015), Some(1002));
        assert_eq!(secs_to_millis(-1.5), Some(-1500));
        for v in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1e300,
            -1e300,
            1e16,
        ] {
            assert_eq!(secs_to_millis(v), None, "{v}");
        }
    }
}
