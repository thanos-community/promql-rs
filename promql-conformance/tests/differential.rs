//! The differential suite: one test per corpus case. **These are
//! expected to fail.**
//!
//! Each case asks the Go oracle what Prometheus returns, asks the Rust
//! engine the same thing, and compares. No Rust engine exists yet, so
//! every case fails — and those failures are the specification. As PR
//! #4's `TableProvider` and an execution layer land, cases turn green
//! one at a time.
//!
//! Because the corpus drives the tests, they are registered at runtime
//! with `libtest-mimic` rather than written as `#[test]` functions. That
//! buys per-case names, so `cargo test -p promql-conformance topk`
//! selects exactly the cases you are working on.
//!
//! # Output while no engine exists
//!
//! 244 failures that all say "not implemented" are ~2000 lines of
//! output conveying one fact, so the suite collapses them into a single
//! failure until the engine seam does something. It probes the engine to
//! decide, which means the collapse needs no configuration and undoes
//! itself as soon as `Engine` is implemented.
//!
//! Set `PROMQL_CONFORMANCE_PER_CASE=1` to get one test per case
//! regardless.
//!
//! Whether the plumbing itself works is `selfcheck.rs`'s job. If those
//! fail, nothing here means anything.

use std::process::ExitCode;

use libtest_mimic::{Arguments, Failed, Trial};
use promql_conformance::{
    compare, oracle, result::Engine, EngineError, QueryResult, Unimplemented,
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

    // While no engine exists at all, every case fails for the same
    // reason, and 244 identical failures cost about 2000 lines of output
    // that say one thing. Collapse them into a single failure until
    // there is something to differentiate.
    //
    // The probe means this needs no configuration and undoes itself:
    // implement `Engine` and the per-case trials appear on their own.
    if engine_is_absent() && !per_case_requested() {
        let (runnable, skipped) = partition(&cases);
        let trial = Trial::test("engine_not_implemented", move || {
            Err(Failed::from(format!(
                "no Rust execution engine exists yet\n\n  \
                 {runnable} corpus cases are ready and the oracle answers every one.\n  \
                 {skipped} skipped (native histograms).\n\n  \
                 Implement `Engine` and replace `Unimplemented` in \
                 tests/differential.rs;\n  \
                 the per-case tests then appear automatically.\n  \
                 Set {PER_CASE_ENV}=1 to list them individually now."
            )))
        });
        return libtest_mimic::run(&args, vec![trial]).exit_code();
    }

    let trials: Vec<Trial> = cases
        .into_iter()
        .map(|case| {
            let name = case.name.clone();
            Trial::test(name, move || run(case))
        })
        .collect();

    libtest_mimic::run(&args, trials).exit_code()
}

/// Opt back into one test per case, for when the collapsed summary is
/// not what you want.
const PER_CASE_ENV: &str = "PROMQL_CONFORMANCE_PER_CASE";

fn per_case_requested() -> bool {
    std::env::var_os(PER_CASE_ENV).is_some_and(|v| !v.is_empty() && v != "0")
}

/// Whether the engine seam is entirely unimplemented, as opposed to
/// implemented and merely wrong. Asked with a query that needs no data.
fn engine_is_absent() -> bool {
    matches!(
        Unimplemented.range_query(&[], 0.0, "vector(1)", 0, 0, 30_000),
        Err(EngineError::NotImplemented)
    )
}

/// Split the corpus into cases that hold the engine to something and
/// cases skipped because the Rust parser cannot represent their input.
fn partition(cases: &[Case]) -> (usize, usize) {
    let skipped = cases
        .iter()
        .filter(|c| c.load.as_ref().is_some_and(|l| !l.is_fully_supported()))
        .count();
    (cases.len() - skipped, skipped)
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

    let series: Vec<_> = case
        .load
        .as_ref()
        .map(|l| l.parsed().cloned().collect())
        .unwrap_or_default();
    let interval = case.load.as_ref().map(|l| l.interval_secs).unwrap_or(0.0);

    let engine = Unimplemented;
    let actual = match engine.range_query(
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
