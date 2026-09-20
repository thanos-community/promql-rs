//! The UTC calendar the date functions read, from a Unix timestamp.
//!
//! Upstream's `dateWrapper` builds a `time.Time` with
//! `time.Unix(sec, 0).UTC()` and asks it for one field
//! (`promql/functions.go:2079-2160` at 83962c35). Go's calendar is the
//! proleptic Gregorian one, so the conversion is arithmetic and needs no
//! table and no dependency: this is Howard Hinnant's `civil_from_days`,
//! which is what every such library does underneath.
//!
//! Seconds are `i64` and the arithmetic is exact over the whole range,
//! so a timestamp far outside any plausible query is answered rather
//! than saturated or panicked on.

/// The fields of one instant, as Go's `time.Time` reports them in UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Civil {
    pub year: i64,
    /// 1..=12, as Go's `Month`.
    pub month: i64,
    pub day: i64,
    /// 1..=366, as Go's `YearDay`.
    pub yearday: i64,
    /// 0 is Sunday, as Go's `Weekday`.
    pub weekday: i64,
    pub hour: i64,
    pub minute: i64,
}

/// Split a Unix timestamp in seconds into its UTC fields.
pub fn civil(secs: i64) -> Civil {
    // Floor division, not truncation: the day before the epoch is day
    // -1 for every second of it, and `-1 / 86400` would answer 0.
    let days = secs.div_euclid(SECS_PER_DAY);
    let time_of_day = secs.rem_euclid(SECS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    Civil {
        year,
        month,
        day,
        yearday: days - days_from_civil(year, 1, 1) + 1,
        // 1970-01-01 was a Thursday, and Go counts Sunday as 0.
        weekday: (days + 4).rem_euclid(7),
        hour: time_of_day / 3600,
        minute: (time_of_day / 60) % 60,
    }
}

/// The length of `month` in `year`, upstream's `32 - Date(y, m, 32).Day()`.
pub fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        // Only reachable from a hand-built `Civil`; the conversion
        // above never produces one.
        _ => 0,
    }
}

fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

const SECS_PER_DAY: i64 = 86_400;

/// Days since 1970-01-01 to (year, month, day). The era arithmetic
/// shifts the year to start in March so that the leap day lands at the
/// end of it and the month lengths become a linear formula.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The inverse, for the start of a year.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_is_a_thursday() {
        let c = civil(0);
        assert_eq!((c.year, c.month, c.day), (1970, 1, 1));
        assert_eq!(c.yearday, 1);
        assert_eq!(c.weekday, 4);
        assert_eq!((c.hour, c.minute), (0, 0));
    }

    /// The timestamps `functions.test` uses, read off `date -u`.
    #[test]
    fn a_handful_of_dates_read_as_they_do_in_utc() {
        for (secs, want) in [
            (
                1_500_000_000,
                Civil {
                    year: 2017,
                    month: 7,
                    day: 14,
                    yearday: 195,
                    weekday: 5,
                    hour: 2,
                    minute: 40,
                },
            ),
            // A leap day, and the last minute of the year after it.
            (
                1_582_934_400,
                Civil {
                    year: 2020,
                    month: 2,
                    day: 29,
                    yearday: 60,
                    weekday: 6,
                    hour: 0,
                    minute: 0,
                },
            ),
            (
                1_609_459_199,
                Civil {
                    year: 2020,
                    month: 12,
                    day: 31,
                    yearday: 366,
                    weekday: 4,
                    hour: 23,
                    minute: 59,
                },
            ),
        ] {
            assert_eq!(civil(secs), want, "{secs}");
        }
    }

    /// Before the epoch the day number goes down a whole day at a time,
    /// which truncating division would get wrong on both fields.
    #[test]
    fn a_second_before_the_epoch_is_the_last_second_of_1969() {
        let c = civil(-1);
        assert_eq!((c.year, c.month, c.day), (1969, 12, 31));
        assert_eq!((c.hour, c.minute), (23, 59));
        assert_eq!(c.weekday, 3);
        assert_eq!(c.yearday, 365);
    }

    #[test]
    fn february_is_the_only_month_that_moves() {
        assert_eq!(days_in_month(2020, 2), 29);
        assert_eq!(days_in_month(2021, 2), 28);
        assert_eq!(days_in_month(2000, 2), 29);
        assert_eq!(days_in_month(1900, 2), 28);
        assert_eq!(days_in_month(2021, 1), 31);
        assert_eq!(days_in_month(2021, 4), 30);
    }

    /// Every day of four centuries round-trips through both directions,
    /// which is the only way to be sure of the era arithmetic.
    #[test]
    fn the_conversion_is_its_own_inverse() {
        for days in -50_000..50_000 {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days, "{days}");
            assert!((1..=12).contains(&m), "{days}: month {m}");
            assert!(d >= 1 && d <= days_in_month(y, m), "{days}: day {d}");
        }
    }
}
