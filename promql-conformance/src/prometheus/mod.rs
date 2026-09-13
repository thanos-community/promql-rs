//! Prometheus's own promqltest corpus, replayed against our engine.
//!
//! The 20 `.test` files under `testdata/prometheus/` are the largest
//! specification of PromQL behaviour that exists: 2,098 assertions, each
//! carrying its expected values inline. That last part is what makes
//! this suite different from [`crate::thanos`] — there is nothing to
//! ask, so it needs no oracle, no Go toolchain and no network, and it
//! therefore runs everywhere, including CI.
//!
//! Upstream parses these files in two layers (`promql/promqltest/
//! test.go`): a hand-written line scanner for the directives, and the
//! real PromQL grammar for everything inside a block. There is no
//! grammar file for the script format itself. We mirror that split —
//! [`script`] is the line scanner, and the series lines and expected
//! rows go through `promql_parser::parse_series_desc`, which is already
//! cross-checked against this very corpus.

//! [`run`] walks a parsed script against an [`crate::result::Engine`]
//! and reports one [`run::Outcome`] per eval; [`baseline`] records which
//! of those are already known not to pass, so CI can be green about a
//! corpus the engine mostly cannot answer yet.

pub mod baseline;
pub mod run;
pub mod script;

pub use baseline::Baseline;
pub use run::{almost_equal, run_script, Outcome, Verdict};
pub use script::{load_corpus, Command, Eval, Expect, Expected, Script, Timing};
