//! Loader tests against inline YAML. These do not need a
//! `promql-engine` checkout; see `corpus.rs` for the ones that do.

use promql_testcases::{parse_suite, Unsupported};

const SUITE: &str = r#"
defaults:
  start_ms: 0
  end_ms: 1800000
  step_ms: 30000
tests:
  - name: fuzz parser crash
    load: |
      load 30s
      http_requests_total{pod="nginx-1", route="/"} 46.00+13.00x40
      http_requests_total{pod="nginx-2", route="/"}  2+5.25x40
    query: sum(http_requests_total)
  - name: no data
    query: vector(1)
    end_ms: 120000
"#;

#[test]
fn applies_defaults_and_overrides() {
    let cases = parse_suite(SUITE).expect("parses");
    assert_eq!(cases.len(), 2);

    assert_eq!(cases[0].name, "fuzz parser crash");
    assert_eq!(cases[0].start_ms, 0);
    assert_eq!(cases[0].end_ms, 1_800_000);
    assert_eq!(cases[0].step_ms, 30_000);

    // end_ms is set on the case and wins over defaults.
    assert_eq!(cases[1].end_ms, 120_000);
    assert_eq!(cases[1].step_ms, 30_000);
}

#[test]
fn parses_the_load_block() {
    let cases = parse_suite(SUITE).expect("parses");
    let load = cases[0].load.as_ref().expect("has a load block");

    assert_eq!(load.interval_secs, 30.0);
    assert!(!load.with_nhcb);
    assert_eq!(load.series.len(), 2);
    assert!(load.is_fully_supported());

    let first = load.parsed().next().expect("first series");
    let names: Vec<&str> = first.labels.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(names, vec!["__name__", "pod", "route"]);
    // 46.00+13.00x40 expands to 41 points, per upstream's `i <= count`.
    assert_eq!(first.values.len(), 41);
    assert_eq!(first.values[0].value, 46.0);
    assert_eq!(first.values[40].value, 566.0);
}

#[test]
fn case_without_load_block_has_none() {
    let cases = parse_suite(SUITE).expect("parses");
    assert!(cases[1].load.is_none());
    assert!(cases[1].is_supported());
}

#[test]
fn native_histograms_are_marked_unsupported() {
    let yaml = r#"
defaults: {start_ms: 0, end_ms: 1000, step_ms: 1000}
tests:
  - name: native histogram
    load: |
      load 30s
      metric{a="b"} {{schema:1 sum:3 count:2}}x2
      other 1 2 3
    query: sum(metric)
"#;
    let cases = parse_suite(yaml).expect("parses");
    let load = cases[0].load.as_ref().expect("has a load block");

    // The block still loads; only the histogram line is set aside.
    assert_eq!(load.series.len(), 2);
    assert_eq!(load.parsed().count(), 1);
    assert!(!load.is_fully_supported());
    assert!(!cases[0].is_supported());

    let unsupported: Vec<_> = load.unsupported().collect();
    assert_eq!(unsupported.len(), 1);
    assert_eq!(unsupported[0].reason, Unsupported::NativeHistogram);
    assert!(unsupported[0].text.contains("{{schema:1"));
}

#[test]
fn a_broken_line_is_not_mistaken_for_a_histogram() {
    let yaml = r#"
defaults: {start_ms: 0, end_ms: 1000, step_ms: 1000}
tests:
  - name: broken
    load: |
      load 30s
      metric 1 bogus 3
    query: sum(metric)
"#;
    let cases = parse_suite(yaml).expect("parses");
    let load = cases[0].load.as_ref().unwrap();
    let unsupported: Vec<_> = load.unsupported().collect();
    assert_eq!(unsupported.len(), 1);
    assert_eq!(unsupported[0].reason, Unsupported::ParseError);
}

#[test]
fn load_directive_variants() {
    for (directive, secs, nhcb) in [
        ("load 10s", 10.0, false),
        ("load 1m", 60.0, false),
        ("load 2m", 120.0, false),
        ("load 60s", 60.0, false),
        ("load_with_nhcb 30s", 30.0, true),
        ("load    30s", 30.0, false),
    ] {
        let yaml = format!(
            "defaults: {{start_ms: 0, end_ms: 1000, step_ms: 1000}}\n\
             tests:\n  - name: t\n    load: |\n      {directive}\n      metric 1\n    query: metric\n"
        );
        let cases = parse_suite(&yaml).unwrap_or_else(|e| panic!("{directive:?}: {e}"));
        let load = cases[0].load.as_ref().unwrap();
        assert_eq!(load.interval_secs, secs, "{directive:?}");
        assert_eq!(load.with_nhcb, nhcb, "{directive:?}");
    }
}

#[test]
fn rejects_a_block_without_a_load_directive() {
    let yaml = r#"
defaults: {start_ms: 0, end_ms: 1000, step_ms: 1000}
tests:
  - name: t
    load: |
      metric 1 2 3
    query: metric
"#;
    assert!(parse_suite(yaml).is_err());
}

#[test]
fn rejects_an_invalid_interval() {
    let yaml = r#"
defaults: {start_ms: 0, end_ms: 1000, step_ms: 1000}
tests:
  - name: t
    load: |
      load 30q
      metric 1
    query: metric
"#;
    assert!(parse_suite(yaml).is_err());
}

#[test]
fn validates_the_same_invariants_as_the_go_binding() {
    let no_step = "defaults: {start_ms: 0, end_ms: 1000, step_ms: 0}\ntests: []\n";
    assert!(parse_suite(no_step).is_err());

    let no_query =
        "defaults: {start_ms: 0, end_ms: 1000, step_ms: 1000}\ntests:\n  - name: t\n    query: ''\n";
    assert!(parse_suite(no_query).is_err());

    let no_name = "defaults: {start_ms: 0, end_ms: 1000, step_ms: 1000}\ntests:\n  - query: up\n";
    assert!(parse_suite(no_name).is_err());
}
