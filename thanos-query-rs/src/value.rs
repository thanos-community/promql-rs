//! Query results as Prometheus's JSON, a port of `web/api/v1/json_codec.go`
//! and `util/jsonutil/marshal.go`: values are strings formatted like Go's
//! `strconv.FormatFloat`, timestamps are seconds with a three-digit
//! millisecond fraction, and series are sorted by label set.

use promql_engine::Series;
use serde_json::value::RawValue;

/// One element of an instant vector.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub labels: Vec<(String, String)>,
    pub t_ms: i64,
    pub f: f64,
}

/// One element of a range matrix.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesValue {
    pub labels: Vec<(String, String)>,
    pub points: Vec<(i64, f64)>,
}

/// `parser.Value` for the two result types this API produces.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Vector(Vec<Sample>),
    Matrix(Vec<SeriesValue>),
}

impl Value {
    /// `resultType`.
    pub fn result_type(&self) -> &'static str {
        match self {
            Self::Vector(_) => "vector",
            Self::Matrix(_) => "matrix",
        }
    }

    /// Prometheus's `queryData`: `{"resultType":..,"result":[..]}`,
    /// rendered here because the value formatting is not serde's.
    pub fn to_query_data(&self) -> Box<RawValue> {
        let mut out = String::new();
        out.push_str("{\"resultType\":\"");
        out.push_str(self.result_type());
        out.push_str("\",\"result\":[");
        match self {
            Self::Vector(samples) => {
                for (i, sample) in samples.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"metric\":");
                    write_metric(&mut out, &sample.labels);
                    out.push_str(",\"value\":");
                    write_point(&mut out, sample.t_ms, sample.f);
                    out.push('}');
                }
            }
            Self::Matrix(series) => {
                for (i, s) in series.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"metric\":");
                    write_metric(&mut out, &s.labels);
                    // Go writes "values" only when there are float points.
                    if !s.points.is_empty() {
                        out.push_str(",\"values\":[");
                        for (j, &(t, f)) in s.points.iter().enumerate() {
                            if j > 0 {
                                out.push(',');
                            }
                            write_point(&mut out, t, f);
                        }
                        out.push(']');
                    }
                    out.push('}');
                }
            }
        }
        out.push_str("]}");
        RawValue::from_string(out).expect("hand-written JSON is valid")
    }
}

/// `labels.Labels` as JSON: an object with the labels in name order.
fn write_metric(out: &mut String, labels: &[(String, String)]) {
    out.push('{');
    for (i, (name, value)) in labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&serde_json::to_string(name).expect("strings serialize"));
        out.push(':');
        out.push_str(&serde_json::to_string(value).expect("strings serialize"));
    }
    out.push('}');
}

/// `marshalFPointJSON`: `[timestamp,"value"]`.
fn write_point(out: &mut String, t_ms: i64, f: f64) {
    out.push('[');
    out.push_str(&marshal_timestamp(t_ms));
    out.push_str(",\"");
    out.push_str(&marshal_float(f));
    out.push_str("\"]");
}

/// `jsonutil.MarshalFloat` minus the quotes: `strconv.FormatFloat(f, 'f',
/// -1, 64)`, or the `'e'` form when the magnitude is below `1e-6` or at
/// least `1e21`; `NaN`, `+Inf`, `-Inf` spelled Go's way.
pub fn marshal_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    let abs = f.abs();
    if abs != 0.0 && !(1e-6..1e21).contains(&abs) {
        // Rust prints the same shortest round-trip mantissa; Go's exponent
        // carries a sign and at least two digits.
        let text = format!("{f:e}");
        let (mantissa, exp) = text.split_once('e').expect("exponent form");
        let exp: i32 = exp.parse().expect("exponent digits");
        let sign = if exp < 0 { '-' } else { '+' };
        return format!("{mantissa}e{sign}{:02}", exp.abs());
    }
    // Rust's `Display` is the shortest round-trip decimal without an
    // exponent, which is exactly `'f', -1`.
    format!("{f}")
}

/// `jsonutil.MarshalTimestamp`: milliseconds as `sec` or `sec.mmm`.
pub fn marshal_timestamp(t_ms: i64) -> String {
    let mut out = String::with_capacity(20);
    let mut t = t_ms;
    if t < 0 {
        out.push('-');
        t = -t;
    }
    out.push_str(&(t / 1000).to_string());
    let fraction = t % 1000;
    if fraction != 0 {
        out.push('.');
        if fraction < 100 {
            out.push('0');
        }
        if fraction < 10 {
            out.push('0');
        }
        out.push_str(&fraction.to_string());
    }
    out
}

/// A series' labels in name order, as `labels.Labels` keeps them.
fn labels_of(series: &Series) -> Vec<(String, String)> {
    let mut labels: Vec<(String, String)> = series
        .labels()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect();
    labels.sort();
    labels
}

/// A range result: every series with points, sorted by label set as
/// Prometheus sorts a `Matrix`.
pub fn matrix_from_series(series: &[Series]) -> Value {
    let mut out: Vec<SeriesValue> = series
        .iter()
        .filter(|s| !s.timestamps().is_empty())
        .map(|s| SeriesValue {
            labels: labels_of(s),
            points: s
                .timestamps()
                .iter()
                .copied()
                .zip(s.values().iter().copied())
                .collect(),
        })
        .collect();
    out.sort_by(|a, b| a.labels.cmp(&b.labels));
    Value::Matrix(out)
}

/// An instant result from a one-step range evaluation: each series
/// contributes its single sample, stamped with the query time as
/// Prometheus stamps vector elements. Sorted by label set.
pub fn vector_from_series(series: &[Series], t_ms: i64) -> Value {
    let mut out: Vec<Sample> = series
        .iter()
        .filter_map(|s| {
            s.values().first().map(|&f| Sample {
                labels: labels_of(s),
                t_ms,
                f,
            })
        })
        .collect();
    out.sort_by(|a, b| a.labels.cmp(&b.labels));
    Value::Vector(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats_format_like_go() {
        for (f, want) in [
            (1.0, "1"),
            (0.5, "0.5"),
            (-0.0, "-0"),
            (0.0, "0"),
            (2.0 / 3.0, "0.6666666666666666"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (1.5e-7, "1.5e-07"),
            (1e-6, "0.000001"),
            (9.99e-7, "9.99e-07"),
            (-2.5e300, "-2.5e+300"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "+Inf"),
            (f64::NEG_INFINITY, "-Inf"),
            (123456789.125, "123456789.125"),
            (0.1 + 0.2, "0.30000000000000004"),
        ] {
            assert_eq!(marshal_float(f), want, "{f:?}");
        }
    }

    #[test]
    fn timestamps_format_like_go() {
        assert_eq!(marshal_timestamp(1_700_000_000_000), "1700000000");
        assert_eq!(marshal_timestamp(1_700_000_000_500), "1700000000.500");
        assert_eq!(marshal_timestamp(1_700_000_000_050), "1700000000.050");
        assert_eq!(marshal_timestamp(1_700_000_000_007), "1700000000.007");
        assert_eq!(marshal_timestamp(0), "0");
        assert_eq!(marshal_timestamp(-1_500), "-1.500");
    }

    fn series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> Series {
        let (ts, vs) = samples.iter().copied().unzip();
        Series::new(labels, ts, vs).unwrap()
    }

    #[test]
    fn matrix_json_is_prometheus_shaped_and_sorted() {
        let value = matrix_from_series(&[
            series(
                &[("__name__", "up"), ("job", "b")],
                &[(1_700_000_000_000, 1.0)],
            ),
            series(
                &[("job", "a"), ("__name__", "up")],
                &[(1_700_000_000_000, 0.0), (1_700_000_015_000, 1.5)],
            ),
        ]);
        assert_eq!(value.result_type(), "matrix");
        assert_eq!(
            value.to_query_data().get(),
            r#"{"resultType":"matrix","result":[{"metric":{"__name__":"up","job":"a"},"values":[[1700000000,"0"],[1700000015,"1.5"]]},{"metric":{"__name__":"up","job":"b"},"values":[[1700000000,"1"]]}]}"#
        );
    }

    #[test]
    fn vector_json_stamps_the_query_time() {
        let value = vector_from_series(
            &[
                series(&[("__name__", "up")], &[(1_700_000_000_000, 1.0)]),
                series(&[("__name__", "down")], &[]),
            ],
            1_700_000_000_250,
        );
        assert_eq!(value.result_type(), "vector");
        assert_eq!(
            value.to_query_data().get(),
            r#"{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1700000000.250,"1"]}]}"#
        );
        assert_eq!(
            Value::Vector(vec![]).to_query_data().get(),
            r#"{"resultType":"vector","result":[]}"#
        );
    }
}
