//! The recorded state of every eval that does not pass.
//!
//! Most of the corpus is out of reach today, so a green CI run cannot
//! mean "everything passes". It means "exactly the evals we already knew
//! about are still the ones not passing" — and the baseline is that
//! record. Progress is a file that shrinks.
//!
//! # Why it fails closed in all four directions
//!
//! | | |
//! |---|---|
//! | recorded, still not passing | fine |
//! | recorded, now passes | **fail** — a win to bless, not to ignore |
//! | not recorded, not passing | **fail** — a regression |
//! | recorded, no such eval | **fail** — an upstream re-pin moved it |
//!
//! The last two are the point. A `promql-sync pull` that changes the
//! corpus mechanically forces a re-bless and a human look at what moved,
//! which is the only thing standing between a re-pin and a silent loss
//! of coverage.
//!
//! # Keys
//!
//! Grouped by file stem, then keyed by [`Outcome::id`] — the query text
//! and the evaluation time, never the line number. Upstream reflows
//! comments constantly; a line-keyed baseline would invalidate wholesale
//! on every bump and teach everyone to bless without reading.
//!
//! # Values
//!
//! `"fail"`, `"skipped"`, or `"unsupported: <feature>"`. The feature
//! name is part of the value because it is stable and useful — the file
//! doubles as a greppable inventory of what the engine is missing, and
//! implementing a feature shows up as a diff rather than as silence.
//! Failure *messages* are deliberately not recorded: they are ours to
//! word, and rewording one should not turn CI red.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use super::run::{Outcome, Verdict};
use super::script::corpus_dir;

/// The generated file, next to the corpus it describes.
pub const FILE_NAME: &str = "BASELINE.toml";

/// Set to any non-empty value other than `0` to regenerate the file.
pub const BLESS_ENV: &str = "PROMQL_PROMQLTEST_BLESS";

pub fn path() -> PathBuf {
    corpus_dir().join(FILE_NAME)
}

pub fn bless_requested() -> bool {
    std::env::var_os(BLESS_ENV).is_some_and(|v| !v.is_empty() && v != "0")
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{path}: {source}\n\nRegenerate it with {BLESS_ENV}=1 cargo test -p promql-conformance --test promqltest")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("{path}: {source}")]
    Write {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("serialising the baseline: {0}")]
    Serialise(#[from] toml::ser::Error),
}

/// File stem → case id → recorded state.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Baseline(BTreeMap<String, BTreeMap<String, String>>);

impl Baseline {
    /// The state to record for an outcome, or `None` if it passes.
    /// Passing evals are absent from the file by construction, so a
    /// regression shows up as "not recorded" rather than as a value
    /// change.
    pub fn state_of(verdict: &Verdict) -> Option<String> {
        match verdict {
            Verdict::Pass => None,
            Verdict::Fail(_) => Some("fail".to_string()),
            Verdict::Skipped(_) => Some("skipped".to_string()),
            Verdict::Unsupported(feature) => Some(format!("unsupported: {feature}")),
        }
    }

    pub fn from_outcomes(outcomes: &[Outcome]) -> Self {
        let mut files: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for o in outcomes {
            if let Some(state) = Self::state_of(&o.verdict) {
                files
                    .entry(o.file.clone())
                    .or_default()
                    .insert(o.id.clone(), state);
            }
        }
        Self(files)
    }

    pub fn len(&self) -> usize {
        self.0.values().map(BTreeMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn load(path: &Path) -> Result<Self, Error> {
        let body = std::fs::read_to_string(path).map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&body)
            .map(Self)
            .map_err(|source| Error::Parse {
                path: path.to_path_buf(),
                source,
            })
    }

    pub fn save(&self, path: &Path) -> Result<(), Error> {
        let mut out = String::new();
        out.push_str(
            "# Every promqltest eval that does not pass, and why.\n\
             #\n\
             # Generated — do not edit by hand. Regenerate with:\n\
             #\n\
             #   PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest\n\
             #\n\
             # A case that starts passing, stops passing, or stops existing all turn\n\
             # CI red, so every change to this file is a deliberate one. Shrinking it\n\
             # is the point; `unsupported:` values name the missing engine feature and\n\
             # are worth grepping.\n\n",
        );
        out.push_str(&toml::to_string_pretty(&self.0)?);
        std::fs::write(path, out).map_err(|source| Error::Write {
            path: path.to_path_buf(),
            source,
        })
    }

    fn get(&self, file: &str, id: &str) -> Option<&str> {
        self.0.get(file)?.get(id).map(String::as_str)
    }
}

/// A disagreement between the baseline and this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// Recorded as not passing, and it passes now. A win — bless it.
    UnexpectedPass { file: String, id: String },
    /// Not recorded, and not passing. A regression.
    Unbaselined {
        file: String,
        id: String,
        state: String,
        detail: String,
    },
    /// Recorded as one thing, now another. The interesting one is
    /// `unsupported: …` → `fail`: a feature landed and is wrong.
    Changed {
        file: String,
        id: String,
        was: String,
        now: String,
        detail: String,
    },
    /// Recorded, but no such eval ran. An upstream re-pin moved or
    /// deleted it, and whatever it was testing is no longer covered.
    Orphaned {
        file: String,
        id: String,
        was: String,
    },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Violation::UnexpectedPass { file, id } => write!(
                f,
                "{file}: this now PASSES and the baseline says it should not.\n  \
                 {id}\n\n  \
                 That is a win. Re-bless to record it:\n    \
                 {BLESS_ENV}=1 cargo test -p promql-conformance --test promqltest"
            ),
            Violation::Unbaselined {
                file,
                id,
                state,
                detail,
            } => write!(
                f,
                "{file}: this stopped passing and is not in the baseline.\n  \
                 {id}\n  now: {state}\n  {detail}"
            ),
            Violation::Changed {
                file,
                id,
                was,
                now,
                detail,
            } => write!(
                f,
                "{file}: this changed how it does not pass.\n  \
                 {id}\n  was: {was}\n  now: {now}\n  {detail}"
            ),
            Violation::Orphaned { file, id, was } => write!(
                f,
                "{file}: the baseline records an eval that no longer exists.\n  \
                 {id}\n  was: {was}\n\n  \
                 An upstream re-pin moved or deleted it. Check what it covered before \
                 re-blessing."
            ),
        }
    }
}

/// Compare a run against the baseline. An empty result means CI is green.
pub fn check(baseline: &Baseline, outcomes: &[Outcome]) -> Vec<Violation> {
    let mut violations = Vec::new();
    let mut live: BTreeMap<(&str, &str), ()> = BTreeMap::new();

    for o in outcomes {
        live.insert((&o.file, &o.id), ());
        let recorded = baseline.get(&o.file, &o.id);
        match (Baseline::state_of(&o.verdict), recorded) {
            (None, None) => {}
            (None, Some(_)) => violations.push(Violation::UnexpectedPass {
                file: o.file.clone(),
                id: o.id.clone(),
            }),
            (Some(state), None) => violations.push(Violation::Unbaselined {
                file: o.file.clone(),
                id: o.id.clone(),
                state,
                detail: detail_of(&o.verdict),
            }),
            (Some(state), Some(was)) if state != was => violations.push(Violation::Changed {
                file: o.file.clone(),
                id: o.id.clone(),
                was: was.to_string(),
                now: state,
                detail: detail_of(&o.verdict),
            }),
            (Some(_), Some(_)) => {}
        }
    }

    for (file, cases) in &baseline.0 {
        for (id, was) in cases {
            if !live.contains_key(&(file.as_str(), id.as_str())) {
                violations.push(Violation::Orphaned {
                    file: file.clone(),
                    id: id.clone(),
                    was: was.clone(),
                });
            }
        }
    }

    violations
}

fn detail_of(verdict: &Verdict) -> String {
    match verdict {
        Verdict::Fail(detail) | Verdict::Skipped(detail) => detail.clone(),
        Verdict::Unsupported(_) | Verdict::Pass => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(file: &str, id: &str, verdict: Verdict) -> Outcome {
        Outcome {
            file: file.to_string(),
            id: id.to_string(),
            line: 1,
            query: id.to_string(),
            verdict,
            expected_rows: 1,
            unchecked_annotations: false,
        }
    }

    fn baseline(entries: &[(&str, &str, &str)]) -> Baseline {
        let mut files: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for (file, id, state) in entries {
            files
                .entry(file.to_string())
                .or_default()
                .insert(id.to_string(), state.to_string());
        }
        Baseline(files)
    }

    #[test]
    fn a_recorded_failure_that_still_fails_is_green() {
        let b = baseline(&[("ops", "q @ 0", "fail")]);
        let run = [outcome("ops", "q @ 0", Verdict::Fail("still wrong".into()))];
        assert_eq!(check(&b, &run), []);
    }

    #[test]
    fn a_pass_is_green_only_when_it_is_not_recorded() {
        let run = [outcome("ops", "q @ 0", Verdict::Pass)];
        assert_eq!(check(&Baseline::default(), &run), []);

        let b = baseline(&[("ops", "q @ 0", "fail")]);
        assert_eq!(
            check(&b, &run),
            [Violation::UnexpectedPass {
                file: "ops".into(),
                id: "q @ 0".into(),
            }]
        );
    }

    #[test]
    fn a_regression_is_caught_because_it_is_not_recorded() {
        let run = [outcome("ops", "q @ 0", Verdict::Fail("broke".into()))];
        let v = check(&Baseline::default(), &run);
        assert!(
            matches!(&v[..], [Violation::Unbaselined { state, detail, .. }]
                if state == "fail" && detail == "broke"),
            "{v:?}"
        );
    }

    /// The transition worth the most: someone implemented a feature and
    /// it produces the wrong answer. Neither entry alone would catch it.
    #[test]
    fn a_feature_landing_wrong_is_caught_as_a_change() {
        let b = baseline(&[("ops", "topk(1, x) @ 0", "unsupported: the topk aggregation")]);
        let run = [outcome(
            "ops",
            "topk(1, x) @ 0",
            Verdict::Fail("expected 1 sample, got 2".into()),
        )];
        let v = check(&b, &run);
        assert!(
            matches!(&v[..], [Violation::Changed { was, now, .. }]
                if was == "unsupported: the topk aggregation" && now == "fail"),
            "{v:?}"
        );
    }

    /// What makes an upstream re-pin visible instead of silent.
    #[test]
    fn an_entry_with_no_matching_eval_is_orphaned() {
        let b = baseline(&[("ops", "deleted upstream @ 0", "fail")]);
        assert_eq!(
            check(&b, &[]),
            [Violation::Orphaned {
                file: "ops".into(),
                id: "deleted upstream @ 0".into(),
                was: "fail".into(),
            }]
        );
    }

    #[test]
    fn the_same_id_in_two_files_is_two_cases() {
        let b = baseline(&[("ops", "q @ 0", "fail")]);
        let run = [
            outcome("ops", "q @ 0", Verdict::Fail("x".into())),
            outcome("funcs", "q @ 0", Verdict::Fail("x".into())),
        ];
        let v = check(&b, &run);
        assert!(
            matches!(&v[..], [Violation::Unbaselined { file, .. }] if file == "funcs"),
            "{v:?}"
        );
    }

    #[test]
    fn an_unsupported_feature_is_recorded_by_name() {
        let run = [
            outcome(
                "ops",
                "a @ 0",
                Verdict::Unsupported("the frob function".into()),
            ),
            outcome("ops", "b @ 0", Verdict::Skipped("histogram".into())),
            outcome("ops", "c @ 0", Verdict::Pass),
        ];
        let b = Baseline::from_outcomes(&run);
        assert_eq!(
            b.get("ops", "a @ 0"),
            Some("unsupported: the frob function")
        );
        assert_eq!(b.get("ops", "b @ 0"), Some("skipped"));
        assert_eq!(b.get("ops", "c @ 0"), None);
        assert_eq!(b.len(), 2);
    }

    /// A round trip has to survive the quoting, because query text is
    /// full of quotes and backslashes.
    #[test]
    fn awkward_query_text_round_trips() {
        let run = [
            outcome(
                "ops",
                r#"label_replace(x, "d", "$1", "src", "(\\d+)") @ 0"#,
                Verdict::Fail("x".into()),
            ),
            outcome(
                "ops",
                "sum(x) by (job) @ 0..60000/30000",
                Verdict::Fail("x".into()),
            ),
        ];
        let written = Baseline::from_outcomes(&run);
        let body = toml::to_string_pretty(&written.0).expect("serialises");
        let read: BTreeMap<String, BTreeMap<String, String>> =
            toml::from_str(&body).expect("parses back");
        assert_eq!(Baseline(read), written);
    }
}
