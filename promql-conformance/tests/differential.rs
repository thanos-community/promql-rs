//! The differential suite: one test per corpus case.
//!
//! Each case asks the Go oracle what Prometheus returns, asks the Rust
//! engine the same thing, and compares. The engine is young, so most
//! cases cannot be evaluated yet; those are the specification for what
//! to build next, and the passing count is the progress meter.
//!
//! Because the corpus drives the tests, they are registered at runtime
//! with `libtest-mimic` rather than written as `#[test]` functions. That
//! buys per-case names, so `cargo test -p promql-conformance topk`
//! selects exactly the cases you are working on.
//!
//! # Output
//!
//! A case the engine cannot evaluate fails with
//! `EngineError::Unsupported`, naming the missing feature. Hundreds of
//! those, each saying "an aggregation is not supported yet", are pages of
//! output conveying one table, so the suite collapses them into a single
//! failing trial that prints the table: how many cases each missing
//! feature blocks. Cases the engine *does* evaluate get their own trial
//! and their own verdict. As features land, cases move from the table to
//! named trials on their own.
//!
//! Set `PROMQL_CONFORMANCE_PER_CASE=1` to get one test per case
//! regardless.
//!
//! Whether the plumbing itself works is `selfcheck.rs`'s job. If those
//! fail, nothing here means anything.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::OnceLock;

use libtest_mimic::{Arguments, Failed, Trial};
use promql_conformance::{
    compare, oracle, result::Engine, DataFusionEngine, EngineError, QueryResult,
};
use promql_testcases::{range_queries_in, testcases_dir, Case, SeriesLine};

fn main() -> ExitCode {
    let args = Arguments::from_args();

    let Some(dir) = testcases_dir() else {
        eprintln!(
            "skipping the differential suite: {} is not set.\n\
             Point it at a promql-engine checkout's testcases directory:\n  \
             {}=~/src/github.com/thanos-io/promql-engine/testcases cargo test -p promql-conformance",
            promql_testcases::TESTCASES_DIR_ENV,
            promql_testcases::TESTCASES_DIR_ENV,
        );
        return ExitCode::SUCCESS;
    };

    // Fail loudly here rather than turning an unavailable oracle into
    // 244 indistinguishable failures.
    if let Err(e) = oracle::shared() {
        eprintln!("skipping the differential suite: oracle unavailable: {e}");
        return ExitCode::SUCCESS;
    }

    let cases = match range_queries_in(&dir) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cannot load the corpus: {e}");
            return ExitCode::FAILURE;
        }
    };

    let mut trials = Vec::new();
    let mut unsupported: BTreeMap<String, Vec<String>> = BTreeMap::new();
    // The corpus reuses a few names (`abs`, `count_over_time`); trials
    // need distinct ones, and `#2` still matches a substring filter.
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();

    for mut case in cases {
        let n = seen.entry(case.name.clone()).or_insert(0);
        *n += 1;
        if *n > 1 {
            case.name = format!("{} #{n}", case.name);
        }
        // The engine is cheap to ask; the oracle is not, and does not need
        // to be asked for a case the engine cannot evaluate anyway.
        match (!per_case_requested())
            .then(|| unsupported_feature(&case))
            .flatten()
        {
            Some(feature) => unsupported.entry(feature).or_default().push(case.name),
            None => {
                let name = case.name.clone();
                trials.push(Trial::test(name, move || run(case)));
            }
        }
    }

    if !unsupported.is_empty() {
        let total: usize = unsupported.values().map(Vec::len).sum();
        let mut table = String::new();
        for (feature, names) in &unsupported {
            table.push_str(&format!("\n  {:>4}  {feature}", names.len()));
        }
        trials.push(Trial::test("unsupported_expressions", move || {
            Err(Failed::from(format!(
                "{total} corpus cases use expressions the engine does not implement yet:\
                 {table}\n\n  \
                 Each becomes its own test as soon as the engine evaluates it.\n  \
                 Set {PER_CASE_ENV}=1 to list them individually now."
            )))
        }));
    }

    libtest_mimic::run(&args, trials).exit_code()
}

/// Opt back into one test per case, for when the collapsed summary is
/// not what you want.
const PER_CASE_ENV: &str = "PROMQL_CONFORMANCE_PER_CASE";

fn per_case_requested() -> bool {
    std::env::var_os(PER_CASE_ENV).is_some_and(|v| !v.is_empty() && v != "0")
}

fn engine() -> &'static DataFusionEngine {
    static ENGINE: OnceLock<DataFusionEngine> = OnceLock::new();
    ENGINE.get_or_init(|| DataFusionEngine::new().expect("the engine constructs"))
}

/// Ask the engine about a case and report the missing feature if it has
/// one. Any other outcome — a result, an error, a bug — is a case worth
/// its own trial.
fn unsupported_feature(case: &Case) -> Option<String> {
    let (series, interval) = seed(case);
    match engine().range_query(
        &series,
        interval,
        &case.query,
        case.start_ms,
        case.end_ms,
        case.step_ms,
    ) {
        Err(EngineError::Unsupported(feature)) => Some(feature),
        _ => None,
    }
}

fn seed(case: &Case) -> (Vec<promql_parser::SeriesDescription>, f64) {
    let series = case
        .load
        .as_ref()
        .map(|l| l.parsed().cloned().collect())
        .unwrap_or_default();
    let interval = case.load.as_ref().map(|l| l.interval_secs).unwrap_or(0.0);
    (series, interval)
}

fn run(case: Case) -> Result<(), Failed> {
    // A load block whose series the Rust parser cannot represent is not
    // part of the engine's specification. Skipping keeps the failure
    // count meaningful: every remaining failure is a missing engine, not
    // a missing grammar rule.
    if let Some(load) = &case.load {
        if let Some(unsupported) = load.series.iter().find_map(|s| match s {
            SeriesLine::Unsupported(u) => Some(u),
            SeriesLine::Parsed(_) => None,
        }) {
            eprintln!(
                "{}: skipped, unsupported series line ({:?}): {}",
                case.name, unsupported.reason, unsupported.text
            );
            return Ok(());
        }
    }

    let oracle = oracle::shared().map_err(|e| Failed::from(e.to_string()))?;
    let raw_load = case.load.as_ref().map(|l| l.raw.as_str()).unwrap_or("");

    let expected = oracle
        .query(
            raw_load,
            &case.query,
            case.start_ms,
            case.end_ms,
            case.step_ms,
        )
        .map_err(|e| {
            // A transport failure is a broken harness, not a failing
            // engine. Say so explicitly so it cannot be mistaken for a
            // specification failure.
            Failed::from(format!("HARNESS FAILURE (not an engine failure): {e}"))
        })?;

    // Native histograms cannot cross the protocol yet, so there is
    // nothing to hold the engine to.
    if expected.has_histograms() {
        eprintln!("{}: skipped, native-histogram result", case.name);
        return Ok(());
    }

    let (series, interval) = seed(&case);
    let actual = match engine().range_query(
        &series,
        interval,
        &case.query,
        case.start_ms,
        case.end_ms,
        case.step_ms,
    ) {
        Ok(r) => r,
        // Deliberately terse. The query and range are already in the
        // YAML, and repeating them for every case buys nothing when the
        // reason is identical throughout. Detail is worth printing for a
        // real mismatch, below, where the two sides actually differ.
        Err(e) => return Err(Failed::from(e.to_string())),
    };

    let Err(mismatch) = compare(&expected, &actual) else {
        return Ok(());
    };

    // Before blaming the engine, check the question has an answer.
    //
    // Some queries have none: `limitk` and `limit_ratio` return an
    // arbitrary subset, and `topk`/`bottomk` fall back on input order
    // when values tie or are NaN. See `Oracle::query_probing_stability`
    // for why an out-of-process oracle sees this and Go's own
    // differential test does not.
    //
    // Probing costs nothing on the happy path, since we only reach here
    // once something has already disagreed.
    let (_, stable) = oracle
        .query_probing_stability(
            raw_load,
            &case.query,
            case.start_ms,
            case.end_ms,
            case.step_ms,
        )
        .map_err(|e| Failed::from(format!("HARNESS FAILURE (not an engine failure): {e}")))?;

    if !stable {
        eprintln!(
            "{}: skipped, the reference gives a different answer each time it is asked",
            case.name
        );
        return Ok(());
    }

    Err(Failed::from(format!(
        "{mismatch}\n  query:  {}\n  range:  {}..{} step {}ms\n  \
         oracle: {}\n  ours:   {}",
        case.query,
        case.start_ms,
        case.end_ms,
        case.step_ms,
        describe(&expected),
        describe(&actual),
    )))
}

/// A one-line summary of a result, for failure messages.
fn describe(result: &QueryResult) -> String {
    match result {
        QueryResult::Matrix(series) => {
            let samples: usize = series.iter().map(|s| s.floats.len()).sum();
            format!("matrix, {} series, {samples} samples", series.len())
        }
        QueryResult::Vector(samples) => format!("vector, {} samples", samples.len()),
        QueryResult::Scalar { v, t } => format!("scalar {v} @{t}"),
        QueryResult::Str { v, t } => format!("string {v:?} @{t}"),
        QueryResult::Error(e) => format!("error: {e}"),
    }
}
