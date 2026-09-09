//! Harness self-checks. **These must pass.**
//!
//! The differential suite in `differential.rs` is expected to fail
//! wholesale right now, because no Rust engine exists. That is only
//! useful if the failures mean "the engine is missing" rather than "the
//! plumbing is broken", and these tests are what separates the two.
//!
//! If anything here goes red, no differential failure can be trusted.
//!
//! They skip rather than fail when the Go toolchain or the corpus is
//! absent: an honest skip cannot be confused with a real failure, which
//! is the whole point.

use promql_conformance::{compare, oracle, QueryResult};
use promql_testcases::{range_queries_in, testcases_dir, Case};

/// Fetch the shared oracle, or skip when it cannot be built.
macro_rules! oracle_or_skip {
    () => {
        match oracle::shared() {
            Ok(o) => o,
            Err(e) => {
                eprintln!("skipping: oracle unavailable: {e}");
                return;
            }
        }
    };
}

fn matrix(result: &QueryResult) -> &[promql_conformance::Series] {
    match result {
        QueryResult::Matrix(series) => series,
        other => panic!("expected a matrix, got {}: {other:?}", other.kind()),
    }
}

/// The one assertion in the whole suite not derived from the oracle's
/// own output, which makes it the test that actually validates the
/// transport and the float encoding.
///
/// `46.00+13.00x40` at `load 30s` is an arithmetic run: value `46 + 13i`
/// at `t = 30000i`. Worked out by hand, not recorded from a run.
#[test]
fn oracle_answers_a_known_query() {
    let oracle = oracle_or_skip!();

    let result = oracle
        .query(
            "load 30s\nhttp_requests_total{pod=\"nginx-1\", route=\"/\"} 46.00+13.00x40\n",
            "http_requests_total",
            0,
            120_000,
            30_000,
        )
        .expect("oracle answers");

    let series = matrix(&result);
    assert_eq!(series.len(), 1, "one series expected: {series:?}");

    let labels: Vec<(&str, &str)> = series[0]
        .labels
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    assert_eq!(
        labels,
        vec![
            ("__name__", "http_requests_total"),
            ("pod", "nginx-1"),
            ("route", "/"),
        ]
    );

    let got: Vec<(i64, f64)> = series[0].floats.iter().map(|p| (p.t, p.v)).collect();
    assert_eq!(
        got,
        vec![
            (0, 46.0),
            (30_000, 59.0),
            (60_000, 72.0),
            (90_000, 85.0),
            (120_000, 98.0),
        ]
    );
}

/// JSON numbers cannot represent these at all, which is why the protocol
/// carries every float as a string. If that encoding regresses, this is
/// where it shows up rather than as a puzzling mismatch later.
#[test]
fn special_floats_round_trip() {
    let oracle = oracle_or_skip!();

    let result = oracle
        .query(
            "load 30s\nspecial NaN Inf -Inf\n",
            "special",
            0,
            60_000,
            30_000,
        )
        .expect("oracle answers");

    let series = matrix(&result);
    assert_eq!(series.len(), 1);
    let values: Vec<f64> = series[0].floats.iter().map(|p| p.v).collect();
    assert_eq!(values.len(), 3, "got {values:?}");
    assert!(values[0].is_nan(), "expected NaN, got {}", values[0]);
    assert_eq!(values[1], f64::INFINITY);
    assert_eq!(values[2], f64::NEG_INFINITY);
}

/// A query that does not compile is a legitimate result -- some cases
/// assert both implementations reject the same expression -- so it must
/// come back as an error result, not as a protocol failure.
#[test]
fn query_errors_are_results_not_transport_failures() {
    let oracle = oracle_or_skip!();

    let result = oracle
        .query("load 30s\nm 1\n", "sum(", 0, 30_000, 30_000)
        .expect("oracle answers rather than failing the transport");

    match result {
        QueryResult::Error(msg) => assert!(
            msg.contains("parse error"),
            "expected a parse error, got {msg:?}"
        ),
        other => panic!("expected an error result, got {}", other.kind()),
    }
}

/// The lazy loader skips the NHCB conversion that `loadCmd.append`
/// performs, so such a block would be seeded incompletely. The oracle
/// must refuse it rather than return a confidently wrong answer.
#[test]
fn rejects_load_with_nhcb() {
    let oracle = oracle_or_skip!();

    let result = oracle
        .query(
            "load_with_nhcb 30s\nm{le=\"1\"} 1\n",
            "m",
            0,
            30_000,
            30_000,
        )
        .expect("oracle answers");

    match result {
        QueryResult::Error(msg) => assert!(
            msg.contains("load_with_nhcb"),
            "expected an NHCB refusal, got {msg:?}"
        ),
        other => panic!("expected an error result, got {}", other.kind()),
    }
}

/// Native histograms cannot be encoded by the protocol yet. The oracle
/// reports a count so the Rust side can mark the case out of reach
/// instead of comparing a truncated result and calling it a mismatch.
#[test]
fn native_histogram_results_are_flagged() {
    let oracle = oracle_or_skip!();

    let result = oracle
        .query(
            "load 30s\nh {{schema:0 sum:5 count:4}}\n",
            "h",
            0,
            30_000,
            30_000,
        )
        .expect("oracle answers");

    assert!(
        result.has_histograms(),
        "expected the histogram flag to be set: {result:?}"
    );
}

/// Sends every corpus case through the oracle and asserts each comes
/// back well formed -- a result or an explicit error, never a transport
/// failure.
///
/// This is what licenses trusting the differential suite: it establishes
/// that all 251 answers are real before any of them is used to judge an
/// engine.
#[test]
fn oracle_handles_every_corpus_case() {
    let oracle = oracle_or_skip!();

    let Some(dir) = testcases_dir() else {
        eprintln!("skipping: PROMQL_ENGINE_TESTCASES is not set");
        return;
    };
    let cases = range_queries_in(&dir).expect("load range_queries.yaml");
    assert!(!cases.is_empty());

    let mut answered = 0usize;
    let mut errored = Vec::new();
    for case in &cases {
        let load = case.load.as_ref().map(|l| l.raw.as_str()).unwrap_or("");
        match oracle.query(load, &case.query, case.start_ms, case.end_ms, case.step_ms) {
            Ok(QueryResult::Error(msg)) => {
                answered += 1;
                errored.push((case.name.clone(), msg));
            }
            Ok(_) => answered += 1,
            Err(e) => panic!("{}: transport failure: {e}", case.name),
        }
    }

    eprintln!(
        "oracle: {answered}/{} cases answered, {} returned a query error",
        cases.len(),
        errored.len()
    );
    for (name, msg) in errored.iter().take(10) {
        eprintln!("  query error: {name}: {msg}");
    }

    assert_eq!(answered, cases.len(), "every case must get an answer");
}

/// 21 corpus cases have no load block -- `pi`, `vector(1)`,
/// `number literal`, `scalar binary op == true` -- and they must run
/// against empty storage and produce a real result.
///
/// This asserts a computed answer rather than merely "not a crash",
/// because the failure it guards against is subtle: handing an empty
/// string to the lazy loader fails with `no "load" command found`, which
/// surfaces as a query error, and since comparison counts two errors as
/// a match, such a case could pass without either implementation having
/// computed anything.
#[test]
fn load_less_cases_query_empty_storage() {
    let oracle = oracle_or_skip!();

    let result = oracle
        .query("", "vector(1)", 0, 60_000, 30_000)
        .expect("oracle answers");

    let series = matrix(&result);
    assert_eq!(series.len(), 1, "expected one series, got {series:?}");
    assert!(
        series[0].labels.is_empty(),
        "vector(1) has no labels, got {:?}",
        series[0].labels
    );
    let got: Vec<(i64, f64)> = series[0].floats.iter().map(|p| (p.t, p.v)).collect();
    assert_eq!(got, vec![(0, 1.0), (30_000, 1.0), (60_000, 1.0)]);
}

/// Asking the oracle the same thing twice must give the same answer,
/// except for queries that have no deterministic answer at all.
///
/// Two things ride on this. It exercises the success path — oracle →
/// `QueryResult` → `compare` → `Ok` — which no differential case reaches
/// today, since they all short-circuit at `NotImplemented`; without it,
/// the first person to implement an engine would be the first to run
/// that code. And it runs the comparer over real results rather than
/// hand-built pairs, covering shapes the unit tests never construct:
/// empty matrices, empty label sets, NaN and infinities inside a series,
/// the stale-series case, 1501-step ranges.
///
/// The bound is a fraction rather than a fixed set of cases, and that is
/// deliberate. Which order-dependent cases actually disagree varies per
/// run — of ten `limitk`/`limit_ratio` cases a typical run sees two —
/// so both an exact count and a name list would be flaky. An earlier
/// version asserted the unstable query used a known order-dependent
/// *function*, and that broke as soon as `topk by (route) (1, ...)` over
/// two NaN series turned up: the instability is about ties and NaNs, not
/// about which function was called.
#[test]
fn the_oracle_is_deterministic_apart_from_queries_with_no_single_answer() {
    let oracle = oracle_or_skip!();
    let Some(cases) = corpus_or_skip() else {
        return;
    };

    let mut unstable = Vec::new();
    for case in &cases {
        let load = case.load.as_ref().map(|l| l.raw.as_str()).unwrap_or("");
        let (_, stable) = oracle
            .query_probing_stability(load, &case.query, case.start_ms, case.end_ms, case.step_ms)
            .unwrap_or_else(|e| panic!("{}: {e}", case.name));
        if !stable {
            unstable.push(case.name.as_str());
        }
    }

    eprintln!(
        "{}/{} cases deterministic; {} with no single answer: {unstable:?}",
        cases.len() - unstable.len(),
        cases.len(),
        unstable.len(),
    );

    let fraction = unstable.len() as f64 / cases.len() as f64;
    assert!(
        fraction <= MAX_UNSTABLE_FRACTION,
        "{:.1}% of the corpus gave a different answer when asked twice, above the \
         {:.0}% ceiling. Either the oracle became nondeterministic or the harness \
         is reusing state across requests; the differential suite means little \
         until this is explained.\n  unstable: {unstable:?}",
        fraction * 100.0,
        MAX_UNSTABLE_FRACTION * 100.0,
    );
}

/// The counterpart to the test above: perturbing one value must be
/// caught, for every case.
///
/// Reflexivity alone is worthless as evidence — a comparer hardwired to
/// return `Ok` would satisfy it. This proves the comparer would actually
/// catch a wrong engine, across the whole corpus and not just on the
/// examples the unit tests happen to pick.
#[test]
fn a_single_perturbed_value_is_caught_in_every_case() {
    let oracle = oracle_or_skip!();
    let Some(cases) = corpus_or_skip() else {
        return;
    };

    let mut caught = 0usize;
    let mut unperturbable = Vec::new();
    for case in &cases {
        let truth = ask(oracle, case);
        match perturb(&truth) {
            Some(wrong) => {
                assert!(
                    compare(&truth, &wrong).is_err(),
                    "{}: a perturbed value went undetected",
                    case.name
                );
                caught += 1;
            }
            // No finite float to move: an empty result, or one made
            // only of NaN/infinities, where every perturbation either
            // compares equal by design or is not a value change at all.
            None => unperturbable.push(case.name.as_str()),
        }
    }

    eprintln!(
        "detected a perturbation in {caught}/{} cases; {} had no finite value to perturb",
        cases.len(),
        unperturbable.len()
    );
    for name in unperturbable.iter().take(10) {
        eprintln!("  no finite value: {name}");
    }

    // Most of the corpus must be perturbable, otherwise this test is
    // asserting far less than it appears to.
    assert!(
        caught * 2 > cases.len(),
        "only {caught} of {} cases could be perturbed; this test is not proving much",
        cases.len()
    );
}

/// Ceiling on how much of the corpus may have no deterministic answer.
///
/// A handful of cases genuinely do not: `limitk` and `limit_ratio`
/// return an arbitrary subset, and `topk`/`bottomk` fall back on input
/// order when values tie or are NaN. Around 12 cases are candidates. The
/// bound is loose because which of them actually disagree varies per
/// run; what it catches is the oracle turning broadly nondeterministic,
/// which would invalidate the whole suite.
const MAX_UNSTABLE_FRACTION: f64 = 0.10;

fn corpus_or_skip() -> Option<Vec<Case>> {
    let Some(dir) = testcases_dir() else {
        eprintln!("skipping: PROMQL_ENGINE_TESTCASES is not set");
        return None;
    };
    Some(range_queries_in(&dir).expect("load range_queries.yaml"))
}

fn ask(oracle: &oracle::Oracle, case: &Case) -> QueryResult {
    let load = case.load.as_ref().map(|l| l.raw.as_str()).unwrap_or("");
    oracle
        .query(load, &case.query, case.start_ms, case.end_ms, case.step_ms)
        .unwrap_or_else(|e| panic!("{}: {e}", case.name))
}

/// Move the first finite float in a result, by enough to exceed the
/// comparer's tolerance at any magnitude.
///
/// `v + 1 + |v|` is the perturbation: it shifts by at least 1, and
/// scales with the value so it also clears the relative tolerance
/// (`1e-10 * min(|x|,|y|)`), which a fixed `+1.0` would not do at
/// `1e12`. It also avoids the `v * 2 + 1` trap, which is a no-op at
/// `v = -1`.
///
/// Returns `None` when there is no finite float to move.
fn perturb(result: &QueryResult) -> Option<QueryResult> {
    let QueryResult::Matrix(series) = result else {
        return None;
    };
    let mut series = series.clone();
    for s in &mut series {
        for p in &mut s.floats {
            if p.v.is_finite() {
                p.v = p.v + 1.0 + p.v.abs();
                return Some(QueryResult::Matrix(series));
            }
        }
    }
    None
}

/// A selector against empty storage returns an empty matrix, not an
/// error.
#[test]
fn selector_against_empty_storage_is_empty_not_an_error() {
    let oracle = oracle_or_skip!();

    let result = oracle
        .query("", "absent_metric", 0, 30_000, 30_000)
        .expect("oracle answers");

    assert!(
        matrix(&result).is_empty(),
        "expected an empty matrix, got {result:?}"
    );
}
