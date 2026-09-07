//! Series-description parsing (`parse_series_desc`), the promqltest
//! load-block line format.
//!
//! Expected values were cross-checked against Go's
//! `parser.ParseSeriesDesc` over the full
//! `promql/promqltest/testdata` corpus: of 1015 distinct series lines,
//! all 787 non-histogram lines produce identical labels and values.
//! The 228 native-histogram lines are the known gap (see
//! `rejects_native_histograms`).

use promql_parser::{parse_series_desc, SeriesDescription};

fn parse(input: &str) -> SeriesDescription {
    parse_series_desc(input).expect("parses")
}

/// Label name/value pairs, in source order.
fn labels(sd: &SeriesDescription) -> Vec<(&str, &str)> {
    sd.labels
        .iter()
        .map(|m| (m.name.as_str(), m.value.as_str()))
        .collect()
}

/// The value sequence with the omitted flag dropped. Only meaningful
/// for inputs without `_` gaps.
fn floats(sd: &SeriesDescription) -> Vec<f64> {
    sd.values.iter().map(|v| v.value).collect()
}

#[test]
fn arithmetic_run() {
    let sd = parse(r#"http_requests_total{pod="nginx-1", route="/"} 46.00+13.00x40"#);

    assert_eq!(
        labels(&sd),
        vec![
            ("__name__", "http_requests_total"),
            ("pod", "nginx-1"),
            ("route", "/"),
        ]
    );

    // Upstream's loop is `i <= count`, adding "an additional value for
    // time 0, which we ignore in tests" — so x40 is 41 points.
    let vals = floats(&sd);
    assert_eq!(vals.len(), 41);
    assert_eq!(vals[0], 46.0);
    assert_eq!(vals[1], 59.0);
    assert_eq!(vals[40], 566.0);
}

#[test]
fn fractional_step() {
    let vals = floats(&parse(
        r#"http_requests_total{pod="nginx-2", route="/"}  2+5.25x40"#,
    ));
    assert_eq!(vals.len(), 41);
    assert_eq!(vals[0], 2.0);
    assert_eq!(vals[1], 7.25);
    assert_eq!(vals[40], 212.0);
}

#[test]
fn repeat_without_step() {
    assert_eq!(floats(&parse("metric{} 1x2")), vec![1.0, 1.0, 1.0]);
}

#[test]
fn plain_sequence() {
    assert_eq!(floats(&parse("metric 1 2 3")), vec![1.0, 2.0, 3.0]);
}

#[test]
fn negative_step() {
    assert_eq!(floats(&parse("metric 10-2x3")), vec![10.0, 8.0, 6.0, 4.0]);
}

#[test]
fn blank_is_a_gap() {
    let vals = parse("metric 1 _ 3").values;
    assert_eq!(vals.len(), 3);
    assert!(!vals[0].omitted);
    assert!(vals[1].omitted);
    assert!(!vals[2].omitted);
}

#[test]
fn blank_run_has_no_extra_point() {
    // `BLANK TIMES uint` loops `i < count` upstream, unlike the value
    // forms which loop `i <= count`. `_x3` is exactly 3 gaps.
    let vals = parse("metric _x3 1").values;
    assert_eq!(vals.len(), 4);
    assert!(vals[..3].iter().all(|v| v.omitted));
    assert!(!vals[3].omitted);
}

#[test]
fn stale_marker() {
    let vals = parse("metric 1 stale").values;
    assert_eq!(vals.len(), 2);
    assert!(vals[1].value.is_nan());
    // Upstream uses value.StaleNaN, a NaN with a distinguishing low bit.
    assert_eq!(vals[1].value.to_bits(), 0x7ff0_0000_0000_0002);
}

#[test]
fn bare_metric_without_label_set() {
    let sd = parse("metric 1");
    assert_eq!(labels(&sd), vec![("__name__", "metric")]);
    assert_eq!(floats(&sd), vec![1.0]);
}

#[test]
fn empty_value_label_is_dropped() {
    // `metric : metric_identifier label_set` runs through a
    // labels.Builder upstream, and Builder.Reset treats an empty value
    // as a deletion.
    let sd = parse(r#"metric{__unit__="", job="app"} 1"#);
    assert_eq!(labels(&sd), vec![("__name__", "metric"), ("job", "app")]);
}

#[test]
fn tabs_separate_values() {
    let sd = parse("metric{job=\"api\"}\t\t1 2");
    assert_eq!(labels(&sd), vec![("__name__", "metric"), ("job", "api")]);
    assert_eq!(floats(&sd), vec![1.0, 2.0]);
}

#[test]
fn trailing_space_is_allowed() {
    assert_eq!(floats(&parse("metric 1 2 ")), vec![1.0, 2.0]);
}

#[test]
fn metric_with_no_values() {
    let sd = parse("metric");
    assert_eq!(labels(&sd), vec![("__name__", "metric")]);
    assert!(sd.values.is_empty());
}

#[test]
fn special_floats() {
    let vals = floats(&parse("metric NaN Inf -Inf"));
    assert!(vals[0].is_nan());
    assert_eq!(vals[1], f64::INFINITY);
    assert_eq!(vals[2], f64::NEG_INFINITY);
}

#[test]
fn rejects_native_histograms() {
    // The `{{...}}` alternatives of `series_item` reference rules still
    // on the sidecar's skip list, so they are not parsed yet.
    assert!(parse_series_desc("metric {{schema:1 sum:3 count:2}}").is_err());
}

#[test]
fn rejects_non_stale_identifier() {
    assert!(parse_series_desc("metric 1 bogus").is_err());
}

#[test]
fn expression_parsing_is_unaffected() {
    // The series lexer must not leak into the expression entry point:
    // `x` and `_` are ordinary identifier characters there, and space
    // is insignificant.
    assert!(promql_parser::parse_expr("x + 1").is_ok());
    assert!(promql_parser::parse_expr("x_total{a=\"b\"} offset 5m").is_ok());
    assert!(promql_parser::parse_expr("sum by (x) (rate(foo[5m]))").is_ok());
    // Conversely, an expression is not a series description.
    assert!(parse_series_desc("rate(foo[5m])").is_err());
}
