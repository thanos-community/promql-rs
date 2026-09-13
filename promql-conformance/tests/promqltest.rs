//! Prometheus's promqltest corpus, replayed against the Rust engine.
//!
//! 2,098 evals across 20 vendored `.test` files, each carrying its own
//! expected values. Unlike [the differential suite](differential.rs)
//! there is nothing to ask at runtime — no oracle, no Go, no network —
//! so this is the conformance suite that actually runs in CI.
//!
//! # What green means
//!
//! Not "the engine passes PromQL". It means *exactly* the evals recorded
//! in `testdata/prometheus/BASELINE.toml` are still the ones not
//! passing. A case that starts passing, stops passing, changes how it
//! fails, or stops existing all turn this red — see
//! [`promql_conformance::prometheus::baseline`] for why each direction
//! has to fail closed.
//!
//! Every run prints a census regardless, so the real numbers are never
//! hidden behind a green tick.
//!
//! # Reading the output
//!
//! Trials are registered for evals that **pass** and for **violations**
//! of the baseline. Evals that fail exactly as recorded are counted, not
//! registered — several hundred permanently-red trials would bury the
//! few that mean something.
//!
//! - `PROMQL_PROMQLTEST_PER_CASE=1` ignores the baseline and gives every
//!   eval its own trial, failing as it really is. This is the view for
//!   working on the engine.
//! - `PROMQL_PROMQLTEST_BLESS=1` rewrites the baseline from this run.
//!
//! # What is not asserted
//!
//! `expect warn` / `info` / `no_warn` / `no_info` — 555 evals — need an
//! annotation channel the engine does not have. They are parsed, counted
//! and reported, but not checked; the count is printed so the gap stays
//! visible rather than silently passing.

use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::OnceLock;

use libtest_mimic::{Arguments, Failed, Trial};
use promql_conformance::prometheus::baseline::{self, Baseline, Violation};
use promql_conformance::prometheus::{load_corpus, run_script, Outcome, Verdict};
use promql_conformance::DataFusionEngine;

fn main() -> ExitCode {
    let args = Arguments::from_args();

    let corpus = match load_corpus() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cannot load the promqltest corpus: {e}");
            return ExitCode::FAILURE;
        }
    };

    let outcomes: Vec<Outcome> = corpus
        .iter()
        .flat_map(|script| run_script(engine(), script))
        .collect();

    census(corpus.len(), &outcomes);

    if baseline::bless_requested() {
        let baseline = Baseline::from_outcomes(&outcomes);
        return match baseline.save(&baseline::path()) {
            Ok(()) => {
                eprintln!(
                    "\nwrote {} entries to {}",
                    baseline.len(),
                    baseline::path().display()
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("\ncannot write the baseline: {e}");
                ExitCode::FAILURE
            }
        };
    }

    let trials = if per_case_requested() {
        per_case_trials(&outcomes)
    } else {
        match Baseline::load(&baseline::path()) {
            Ok(b) => gated_trials(&b, &outcomes),
            Err(e) => {
                eprintln!("\ncannot read the baseline: {e}");
                return ExitCode::FAILURE;
            }
        }
    };

    libtest_mimic::run(&args, trials).exit_code()
}

/// One trial per passing eval, plus one per baseline violation. Evals
/// that do not pass exactly as recorded produce no trial at all.
fn gated_trials(baseline: &Baseline, outcomes: &[Outcome]) -> Vec<Trial> {
    let violations = baseline::check(baseline, outcomes);

    // An unexpected pass is both a passing eval and a violation, and two
    // trials cannot share a name — the violation is the one to report.
    let flagged: BTreeSet<(&str, &str)> = violations
        .iter()
        .filter_map(|v| match v {
            Violation::UnexpectedPass { file, id } => Some((file.as_str(), id.as_str())),
            _ => None,
        })
        .collect();

    let mut trials: Vec<Trial> = outcomes
        .iter()
        .filter(|o| o.verdict.is_pass() && !flagged.contains(&(o.file.as_str(), o.id.as_str())))
        .map(|o| {
            let name = format!("{}/{}", o.file, o.id);
            Trial::test(name, || Ok(()))
        })
        .collect();

    trials.extend(violations.into_iter().map(|v| {
        let name = match &v {
            Violation::UnexpectedPass { file, id }
            | Violation::Unbaselined { file, id, .. }
            | Violation::Changed { file, id, .. }
            | Violation::Orphaned { file, id, .. } => format!("BASELINE {file}/{id}"),
        };
        Trial::test(name, move || Err(Failed::from(v.to_string())))
    }));

    trials
}

/// Every eval on its own, baseline ignored: the view for working on the
/// engine rather than for guarding it.
fn per_case_trials(outcomes: &[Outcome]) -> Vec<Trial> {
    outcomes
        .iter()
        .filter(|o| !matches!(o.verdict, Verdict::Skipped(_)))
        .map(|o| {
            let name = format!("{}/{}", o.file, o.id);
            let at = format!("{}.test:{}", o.file, o.line);
            let query = o.query.clone();
            let verdict = o.verdict.clone();
            Trial::test(name, move || match verdict {
                Verdict::Pass => Ok(()),
                Verdict::Fail(detail) => Err(Failed::from(format!(
                    "{detail}\n  query: {query}\n  at:    {at}"
                ))),
                Verdict::Unsupported(feature) => Err(Failed::from(format!(
                    "{feature} is not supported yet\n  query: {query}\n  at:    {at}"
                ))),
                Verdict::Skipped(_) => Ok(()),
            })
        })
        .collect()
}

/// The real numbers, printed on every run so a green tick never stands
/// in for "the engine passes PromQL".
fn census(files: usize, outcomes: &[Outcome]) {
    let mut passed = 0;
    let mut empty_passes = 0;
    let mut failed = 0;
    let mut skipped = 0;
    let mut unchecked_annotations = 0;
    let mut unsupported: BTreeMap<&str, usize> = BTreeMap::new();

    for o in outcomes {
        if o.unchecked_annotations {
            unchecked_annotations += 1;
        }
        match &o.verdict {
            Verdict::Pass => {
                passed += 1;
                if o.expected_rows == 0 {
                    empty_passes += 1;
                }
            }
            Verdict::Fail(_) => failed += 1,
            Verdict::Skipped(_) => skipped += 1,
            Verdict::Unsupported(feature) => *unsupported.entry(feature).or_default() += 1,
        }
    }

    let unsupported_total: usize = unsupported.values().sum();
    eprintln!(
        "promqltest: {files} files, {} evals — {passed} pass, {failed} fail, \
         {unsupported_total} unsupported, {skipped} skipped\n  \
         {} of those passes assert a non-empty result; {empty_passes} assert only that \
         nothing came back.\n  \
         {unchecked_annotations} evals carry warn/info assertions that are parsed but \
         not checked.",
        outcomes.len(),
        passed - empty_passes,
    );

    if !unsupported.is_empty() {
        eprintln!("\nmissing engine features, by evals blocked:");
        for (feature, count) in &unsupported {
            eprintln!("  {count:>4}  {feature}");
        }
    }
    eprintln!();
}

/// Ignore the baseline and give every eval its own trial.
const PER_CASE_ENV: &str = "PROMQL_PROMQLTEST_PER_CASE";

fn per_case_requested() -> bool {
    std::env::var_os(PER_CASE_ENV).is_some_and(|v| !v.is_empty() && v != "0")
}

fn engine() -> &'static DataFusionEngine {
    static ENGINE: OnceLock<DataFusionEngine> = OnceLock::new();
    ENGINE.get_or_init(|| DataFusionEngine::new().expect("the engine constructs"))
}
