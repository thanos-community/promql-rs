//! Prometheus's promqltest corpus, replayed against the Rust engine.
//!
//! 2,098 evals across 20 vendored `.test` files, each carrying its own
//! expected values. Unlike [the differential suite](differential.rs)
//! there is nothing to ask at runtime — no oracle, no Go, no network —
//! so this is the conformance suite that actually runs in CI.
//!
//! # Reading the output
//!
//! The engine is young, so most of the corpus is out of reach. Saying so
//! 2,000 times is not information, so the suite sorts each eval into one
//! of four buckets and only gives the interesting ones their own trial:
//!
//! - **unsupported** — the engine named a feature it does not have.
//!   Collapsed into one trial printing a feature→count table, exactly as
//!   `differential.rs` does. These become named trials as features land.
//! - **skipped** — nothing to hold the engine to: a native-histogram
//!   load line or expected row we cannot read. Printed, not failed.
//! - **failed** — the engine answered, and the answer is wrong. One
//!   trial each. This is the bucket worth reading.
//! - **passed** — one trial each, so `cargo test … rate` selects them.
//!
//! Set `PROMQL_PROMQLTEST_PER_CASE=1` to give the unsupported cases
//! individual trials too.
//!
//! # What is not asserted
//!
//! `expect warn` / `info` / `no_warn` / `no_info` — 617 lines — need an
//! annotation channel the engine does not have. They are parsed, counted
//! and reported, but not checked; the count is printed so the gap stays
//! visible rather than silently passing.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::OnceLock;

use libtest_mimic::{Arguments, Failed, Trial};
use promql_conformance::prometheus::{load_corpus, run_script, Verdict};
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

    let mut trials = Vec::new();
    let mut unsupported: BTreeMap<String, usize> = BTreeMap::new();
    let mut skipped = 0usize;
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut empty_passes = 0usize;
    let mut unchecked_annotations = 0usize;

    for script in &corpus {
        for outcome in run_script(engine(), script) {
            if outcome.unchecked_annotations {
                unchecked_annotations += 1;
            }
            let name = outcome.id;
            let at = format!("{}.test:{}", outcome.file, outcome.line);
            match outcome.verdict {
                Verdict::Pass => {
                    passed += 1;
                    if outcome.expected_rows == 0 {
                        empty_passes += 1;
                    }
                    trials.push(Trial::test(name, || Ok(())));
                }
                Verdict::Fail(detail) => {
                    failed += 1;
                    let query = outcome.query;
                    trials.push(Trial::test(name, move || {
                        Err(Failed::from(format!(
                            "{detail}\n  query: {query}\n  at:    {at}"
                        )))
                    }));
                }
                Verdict::Unsupported(feature) => {
                    if per_case_requested() {
                        trials.push(Trial::test(name, move || {
                            Err(Failed::from(format!("{feature} is not supported yet")))
                        }));
                    } else {
                        *unsupported.entry(feature).or_default() += 1;
                    }
                }
                Verdict::Skipped(reason) => {
                    skipped += 1;
                    eprintln!("{name}: skipped, {reason}");
                }
            }
        }
    }

    if !unsupported.is_empty() {
        let total: usize = unsupported.values().sum();
        let mut table = String::new();
        for (feature, count) in &unsupported {
            table.push_str(&format!("\n  {count:>4}  {feature}"));
        }
        trials.push(Trial::test("unsupported_expressions", move || {
            Err(Failed::from(format!(
                "{total} promqltest evals use expressions the engine does not \
                 implement yet:{table}\n\n  \
                 Each becomes its own test as soon as the engine evaluates it.\n  \
                 Set {PER_CASE_ENV}=1 to list them individually now."
            )))
        }));
    }

    let unsupported_total: usize = unsupported.values().sum();
    eprintln!(
        "promqltest: {} files, {} evals — {passed} pass, {failed} fail, \
         {unsupported_total} unsupported, {skipped} skipped\n  \
         {} of those passes assert a non-empty result; {empty_passes} assert only that \
         nothing came back.\n  \
         {unchecked_annotations} evals carry warn/info assertions that are parsed but \
         not checked.",
        corpus.len(),
        passed + failed + skipped + unsupported_total,
        passed - empty_passes,
    );

    libtest_mimic::run(&args, trials).exit_code()
}

/// Opt into one trial per unsupported eval, for when the collapsed
/// summary is not what you want.
const PER_CASE_ENV: &str = "PROMQL_PROMQLTEST_PER_CASE";

fn per_case_requested() -> bool {
    std::env::var_os(PER_CASE_ENV).is_some_and(|v| !v.is_empty() && v != "0")
}

fn engine() -> &'static DataFusionEngine {
    static ENGINE: OnceLock<DataFusionEngine> = OnceLock::new();
    ENGINE.get_or_init(|| DataFusionEngine::new().expect("the engine constructs"))
}
