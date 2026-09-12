//! Request parameters, ported from `pkg/api/query/v1.go` and the Go
//! standard library pieces it leans on: `net/http`'s form handling,
//! `strconv`, and Prometheus's `model.ParseDuration`. Error strings are
//! kept verbatim so clients see what they see from Thanos.

use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use axum::http::{header, Method};
use promql_parser::ast::LabelMatcher;

use crate::api::ApiError;

/// Largest request body read for form parameters.
const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// Prometheus's `MinTime` and `MaxTime` in milliseconds, the default
/// range of the metadata endpoints.
pub const MIN_TIME_MS: i64 = (i64::MIN / 1000 + 62_135_596_801) * 1000;
pub const MAX_TIME_MS: i64 = (i64::MAX / 1000 - 62_135_596_801) * 1000 + 999;

/// Go's `r.Form` after `ParseForm`: the URL-encoded body of a POST first,
/// then the query string, every value of a repeated key kept in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FormValues(Vec<(String, String)>);

impl FormValues {
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        Self(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    /// `ParseForm`: a body is read for `POST`, `PUT` and `PATCH` with an
    /// `application/x-www-form-urlencoded` content type; any other body is
    /// ignored, never an error.
    pub fn parse(
        query: Option<&str>,
        method: &Method,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Self {
        let mut values = Vec::new();
        let body_method =
            matches!(*method, Method::POST | Method::PUT | Method::PATCH) && is_form(content_type);
        if body_method {
            values.extend(form_urlencoded::parse(body).into_owned());
        }
        if let Some(query) = query {
            values.extend(form_urlencoded::parse(query.as_bytes()).into_owned());
        }
        Self(values)
    }

    /// `r.FormValue`: the first value, `""` when absent.
    pub fn get(&self, key: &str) -> &str {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map_or("", |(_, v)| v.as_str())
    }

    /// `r.Form[key]`: every value, in order.
    pub fn get_all(&self, key: &str) -> Vec<&str> {
        self.0
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect()
    }
}

fn is_form(content_type: Option<&str>) -> bool {
    content_type
        .and_then(|ct| ct.split(';').next())
        .is_some_and(|media| {
            media
                .trim()
                .eq_ignore_ascii_case("application/x-www-form-urlencoded")
        })
}

impl<S: Send + Sync> FromRequest<S> for FormValues {
    type Rejection = ApiError;

    async fn from_request(request: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let method = request.method().clone();
        let query = request.uri().query().map(str::to_owned);
        let content_type = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body: Bytes = axum::body::to_bytes(request.into_body(), MAX_BODY_BYTES)
            .await
            .map_err(|e| ApiError::bad_data(format!("read request body: {e}")))?;
        Ok(Self::parse(
            query.as_deref(),
            &method,
            content_type.as_deref(),
            &body,
        ))
    }
}

/// `parseTime`: Unix seconds with an optional fraction, or RFC 3339, as
/// milliseconds.
pub fn parse_time(s: &str) -> Result<i64, String> {
    if let Ok(t) = s.parse::<f64>() {
        if t.is_finite() {
            // Go: `s, ns := math.Modf(t); ns = math.Round(ns*1000) / 1000;
            // time.Unix(int64(s), int64(ns*float64(time.Second)))`, then
            // `timestamp.FromTime`.
            let whole = t.trunc();
            let fraction = ((t - whole) * 1000.0).round() / 1000.0;
            let mut sec = whole as i64;
            let mut nsec = (fraction * 1e9) as i64;
            if nsec < 0 {
                nsec += 1_000_000_000;
                sec -= 1;
            } else if nsec >= 1_000_000_000 {
                nsec -= 1_000_000_000;
                sec += 1;
            }
            return Ok(sec.saturating_mul(1000).saturating_add(nsec / 1_000_000));
        }
    }
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(t.timestamp_millis());
    }
    Err(format!("cannot parse {s:?} to a valid timestamp"))
}

/// `parseTimeParam`: absent means `default`.
pub fn parse_time_param(form: &FormValues, name: &str, default: i64) -> Result<i64, ApiError> {
    let value = form.get(name);
    if value.is_empty() {
        return Ok(default);
    }
    parse_time(value)
        .map_err(|e| ApiError::bad_data(format!("Invalid time value for '{name}': {e}")))
}

/// `parseDuration`: seconds as a float, or Prometheus's `1h30m` syntax,
/// as nanoseconds like Go's `time.Duration`.
pub fn parse_duration(s: &str) -> Result<i64, String> {
    if let Ok(d) = s.parse::<f64>() {
        let ns = d * 1e9;
        if ns.is_nan() || ns > i64::MAX as f64 || ns < i64::MIN as f64 {
            return Err(format!(
                "cannot parse {s:?} to a valid duration. It overflows int64"
            ));
        }
        return Ok(ns as i64);
    }
    if let Some(d) = parse_prometheus_duration(s) {
        return Ok(d);
    }
    Err(format!("cannot parse {s:?} to a valid duration"))
}

/// `model.ParseDuration`: `[Ny][Nw][Nd][Nh][Nm][Ns][Nms]`, units in that
/// order, each at most once, at least one present; `"0"` alone is fine.
/// `None` for anything else, including overflow.
fn parse_prometheus_duration(s: &str) -> Option<i64> {
    if s == "0" {
        return Some(0);
    }
    const UNITS: [(&str, i64); 7] = [
        ("y", 365 * 24 * 60 * 60 * 1000),
        ("w", 7 * 24 * 60 * 60 * 1000),
        ("d", 24 * 60 * 60 * 1000),
        ("h", 60 * 60 * 1000),
        ("m", 60 * 1000),
        ("s", 1000),
        ("ms", 1),
    ];
    let mut rest = s;
    let mut next_unit = 0;
    let mut total_ms: i64 = 0;
    let mut components = 0;
    while !rest.is_empty() {
        let digits_end = rest.find(|c: char| !c.is_ascii_digit())?;
        if digits_end == 0 {
            return None;
        }
        let n: i64 = rest[..digits_end].parse().ok()?;
        rest = &rest[digits_end..];
        // `m` must not swallow the `m` of `ms`.
        let (unit, mult) = UNITS[next_unit..].iter().find(|(unit, _)| {
            rest.starts_with(unit) && !(*unit == "m" && rest.starts_with("ms"))
        })?;
        rest = &rest[unit.len()..];
        next_unit = UNITS.iter().position(|(u, _)| u == unit)? + 1;
        total_ms = total_ms.checked_add(n.checked_mul(*mult)?)?;
        components += 1;
    }
    if components == 0 {
        return None;
    }
    total_ms.checked_mul(1_000_000)
}

/// `strconv.ParseBool`, with its error text.
pub fn parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {s:?}: invalid syntax")),
    }
}

/// A boolean parameter that overrides `default` when present, with the
/// `'<name>' parameter: ...` wrapping of the Thanos parsers.
pub fn parse_bool_param(form: &FormValues, name: &str, default: bool) -> Result<bool, ApiError> {
    let value = form.get(name);
    if value.is_empty() {
        return Ok(default);
    }
    parse_bool(value).map_err(|e| ApiError::bad_data(format!("'{name}' parameter: {e}")))
}

/// `parseLimitParam`: `""` means no limit.
pub fn parse_limit(s: &str) -> Result<usize, ApiError> {
    if s.is_empty() {
        return Ok(0);
    }
    let limit: i64 = s
        .parse()
        .map_err(|_| ApiError::bad_data(format!("cannot parse {s:?} to a valid limit")))?;
    if limit < 0 {
        return Err(ApiError::bad_data("limit must be non-negative"));
    }
    Ok(limit as usize)
}

/// `extpromql.ParseMetricSelector`: the matchers of one selector such as
/// `up{job="api"}` or `{__name__=~"http_.*"}`.
pub fn parse_metric_selector(s: &str) -> Result<Vec<LabelMatcher>, ApiError> {
    promql_parser::parse_metric_selector(s).map_err(|e| ApiError::bad_data(e.to_string()))
}

/// `parseStoreDebugMatchersParam`: every `storeMatch[]` selector.
pub fn parse_store_matchers(form: &FormValues) -> Result<Vec<Vec<LabelMatcher>>, ApiError> {
    form.get_all("storeMatch[]")
        .into_iter()
        .map(parse_metric_selector)
        .collect()
}

/// `parseStep`: the `step` parameter, or `max(range/250, default)` whole
/// seconds like the Thanos UI. Nanoseconds.
pub fn parse_step(
    form: &FormValues,
    default_step_ns: i64,
    range_seconds: i64,
) -> Result<i64, ApiError> {
    let value = form.get("step");
    if !value.is_empty() {
        return parse_duration(value)
            .map_err(|e| ApiError::bad_data(format!("'step' parameter: {e}")));
    }
    let seconds = (range_seconds / 250).max(default_step_ns / 1_000_000_000);
    Ok(seconds * 1_000_000_000)
}

/// `parseMetadataTimeRange` with Thanos's default of the whole of time:
/// `start` and `end` in milliseconds.
pub fn parse_metadata_time_range(form: &FormValues) -> Result<(i64, i64), ApiError> {
    let start = parse_time_param(form, "start", MIN_TIME_MS)?;
    let end = parse_time_param(form, "end", MAX_TIME_MS)?;
    if end < start {
        return Err(ApiError::bad_data(
            "end timestamp must not be before start time",
        ));
    }
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_values_follow_go() {
        let form = FormValues::parse(
            Some("a=1&match%5B%5D=x&a=2"),
            &Method::POST,
            Some("application/x-www-form-urlencoded; charset=utf-8"),
            b"a=body&match[]=y",
        );
        assert_eq!(form.get("a"), "body", "the body comes first");
        assert_eq!(form.get_all("a"), ["body", "1", "2"]);
        assert_eq!(form.get_all("match[]"), ["y", "x"]);
        assert_eq!(form.get("missing"), "");

        let get = FormValues::parse(
            Some("a=1"),
            &Method::GET,
            Some("application/x-www-form-urlencoded"),
            b"a=body",
        );
        assert_eq!(get.get_all("a"), ["1"], "GET bodies are ignored");
        let json = FormValues::parse(None, &Method::POST, Some("application/json"), b"a=body");
        assert_eq!(json.get("a"), "", "other content types are ignored");
    }

    #[test]
    fn times() {
        assert_eq!(parse_time("1700000000"), Ok(1_700_000_000_000));
        assert_eq!(parse_time("1700000000.5"), Ok(1_700_000_000_500));
        assert_eq!(parse_time("1700000000.123"), Ok(1_700_000_000_123));
        assert_eq!(
            parse_time("1700000000.1234"),
            Ok(1_700_000_000_123),
            "rounded to ms"
        );
        assert_eq!(
            parse_time("1700000000.9996"),
            Ok(1_700_000_001_000),
            "rounds up into the next second"
        );
        assert_eq!(parse_time("-1.5"), Ok(-1_500));
        assert_eq!(parse_time("0"), Ok(0));
        assert_eq!(parse_time("2023-11-14T22:13:20Z"), Ok(1_700_000_000_000));
        assert_eq!(
            parse_time("2023-11-14T23:13:20.250+01:00"),
            Ok(1_700_000_000_250)
        );
        assert_eq!(
            parse_time("yesterday"),
            Err(r#"cannot parse "yesterday" to a valid timestamp"#.to_string())
        );
        assert_eq!(
            parse_time(""),
            Err(r#"cannot parse "" to a valid timestamp"#.to_string())
        );

        let form = FormValues::from_pairs(&[("time", "x")]);
        assert_eq!(
            parse_time_param(&form, "time", 7).unwrap_err().message,
            r#"Invalid time value for 'time': cannot parse "x" to a valid timestamp"#
        );
        assert_eq!(parse_time_param(&form, "start", 7).unwrap(), 7);
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("15"), Ok(15_000_000_000));
        assert_eq!(parse_duration("0.5"), Ok(500_000_000));
        assert_eq!(parse_duration("-1"), Ok(-1_000_000_000));
        assert_eq!(parse_duration("15s"), Ok(15_000_000_000));
        assert_eq!(parse_duration("1h30m"), Ok(5_400_000_000_000));
        assert_eq!(
            parse_duration("1y2w3d4h5m6s7ms"),
            Some(((((365 + 14 + 3) * 24 + 4) * 60 + 5) * 60 + 6) * 1000 + 7,)
                .map(|ms: i64| ms * 1_000_000)
                .ok_or(String::new())
        );
        assert_eq!(parse_duration("250ms"), Ok(250_000_000));
        assert_eq!(parse_duration("0"), Ok(0));
        for bad in ["", "1x", "1s1m", "1m1m", "s", "1.5s", "1 s", "1ms1s"] {
            assert_eq!(
                parse_duration(bad),
                Err(format!("cannot parse {bad:?} to a valid duration")),
                "{bad:?}"
            );
        }
        assert_eq!(
            parse_duration("1e300"),
            Err(r#"cannot parse "1e300" to a valid duration. It overflows int64"#.to_string())
        );
        assert!(parse_duration("9999999999999y").is_err(), "overflows");
    }

    #[test]
    fn bools_limits_and_steps() {
        for (s, want) in [
            ("1", true),
            ("t", true),
            ("TRUE", true),
            ("True", true),
            ("0", false),
            ("False", false),
        ] {
            assert_eq!(parse_bool(s), Ok(want));
        }
        assert_eq!(
            parse_bool("yes"),
            Err(r#"strconv.ParseBool: parsing "yes": invalid syntax"#.to_string())
        );
        let form = FormValues::from_pairs(&[("dedup", "nope")]);
        assert_eq!(
            parse_bool_param(&form, "dedup", true).unwrap_err().message,
            r#"'dedup' parameter: strconv.ParseBool: parsing "nope": invalid syntax"#
        );
        assert!(parse_bool_param(&form, "partial_response", true).unwrap());

        assert_eq!(parse_limit("").unwrap(), 0);
        assert_eq!(parse_limit("10").unwrap(), 10);
        assert_eq!(
            parse_limit("x").unwrap_err().message,
            r#"cannot parse "x" to a valid limit"#
        );
        assert_eq!(
            parse_limit("-1").unwrap_err().message,
            "limit must be non-negative"
        );

        let none = FormValues::default();
        assert_eq!(
            parse_step(&none, 1_000_000_000, 100).unwrap(),
            1_000_000_000
        );
        assert_eq!(
            parse_step(&none, 1_000_000_000, 3600).unwrap(),
            14_000_000_000,
            "3600/250 = 14s"
        );
        assert_eq!(
            parse_step(&none, 60_000_000_000, 3600).unwrap(),
            60_000_000_000
        );
        let explicit = FormValues::from_pairs(&[("step", "15")]);
        assert_eq!(
            parse_step(&explicit, 1_000_000_000, 3600).unwrap(),
            15_000_000_000
        );
        let bad = FormValues::from_pairs(&[("step", "soon")]);
        assert_eq!(
            parse_step(&bad, 1, 1).unwrap_err().message,
            r#"'step' parameter: cannot parse "soon" to a valid duration"#
        );
    }

    #[test]
    fn selectors_and_metadata_ranges() {
        let matchers = parse_metric_selector(r#"up{job=~"a.*"}"#).unwrap();
        assert_eq!(matchers.len(), 2);
        assert!(parse_metric_selector("up +").is_err());
        let form = FormValues::from_pairs(&[
            ("storeMatch[]", r#"{__address__="a:1"}"#),
            ("storeMatch[]", "{x=\"y\"}"),
        ]);
        assert_eq!(parse_store_matchers(&form).unwrap().len(), 2);

        assert_eq!(
            parse_metadata_time_range(&FormValues::default()).unwrap(),
            (MIN_TIME_MS, MAX_TIME_MS)
        );
        let form = FormValues::from_pairs(&[("start", "10"), ("end", "5")]);
        assert_eq!(
            parse_metadata_time_range(&form).unwrap_err().message,
            "end timestamp must not be before start time"
        );
        assert_eq!(MIN_TIME_MS, -9_223_309_901_257_974_000);
        assert_eq!(MAX_TIME_MS, 9_223_309_901_257_974_999);
    }
}
