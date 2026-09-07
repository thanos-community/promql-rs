//! Tests against the real `range_queries.yaml`.
//!
//! These need a `promql-engine` checkout, pointed at by
//! `PROMQL_ENGINE_TESTCASES`. Without it they skip, so a plain `cargo
//! test` still passes on a machine that only has this repository:
//!
//! ```console
//! $ PROMQL_ENGINE_TESTCASES=~/src/github.com/thanos-io/promql-engine/testcases \
//!     cargo test -p promql-testcases
//! ```
//!
//! Once the upstream PR lands and the YAML is vendored or brought in as
//! a submodule, the skip goes away and these become unconditional.

use promql_testcases::{range_queries_in, testcases_dir, Case, Unsupported};

/// Number of cases whose load block uses a native-histogram
/// descriptor. Asserted exactly so that porting the `histogram_*`
/// grammar rules shows up here as a failing count rather than passing
/// silently.
const NATIVE_HISTOGRAM_CASES: usize = 7;

fn corpus() -> Option<Vec<Case>> {
    let dir = testcases_dir()?;
    Some(range_queries_in(&dir).expect("load range_queries.yaml"))
}

macro_rules! corpus_or_skip {
    () => {
        match corpus() {
            Some(c) => c,
            None => {
                eprintln!("skipping: PROMQL_ENGINE_TESTCASES is not set");
                return;
            }
        }
    };
}

#[test]
fn loads_every_case() {
    let cases = corpus_or_skip!();
    assert!(!cases.is_empty());
    for c in &cases {
        assert!(!c.name.is_empty());
        assert!(!c.query.trim().is_empty());
        assert!(c.step_ms > 0, "{}: step_ms must be positive", c.name);
    }
}

#[test]
fn every_series_line_parses_except_native_histograms() {
    let cases = corpus_or_skip!();

    let mut unsupported_cases = Vec::new();
    for c in &cases {
        let Some(load) = &c.load else { continue };
        for u in load.unsupported() {
            assert_eq!(
                u.reason,
                Unsupported::NativeHistogram,
                "{}: unexpected parse failure on {:?}",
                c.name,
                u.text
            );
        }
        if !load.is_fully_supported() {
            unsupported_cases.push(c.name.as_str());
        }
    }

    assert_eq!(
        unsupported_cases.len(),
        NATIVE_HISTOGRAM_CASES,
        "cases blocked on native histograms: {unsupported_cases:?}"
    );
}

#[test]
fn load_intervals_all_parse() {
    let cases = corpus_or_skip!();
    for c in &cases {
        let Some(load) = &c.load else { continue };
        assert!(
            load.interval_secs > 0.0,
            "{}: interval must be positive",
            c.name
        );
    }
}

#[test]
fn supported_cases_have_series_with_values() {
    let cases = corpus_or_skip!();
    for c in cases.iter().filter(|c| c.is_supported()) {
        let Some(load) = &c.load else { continue };
        for sd in load.parsed() {
            assert!(
                !sd.labels.is_empty(),
                "{}: series with no labels at all",
                c.name
            );
        }
    }
}
