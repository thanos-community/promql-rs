//! Where the engine stands against the corpus.
//!
//! Separate from [`super::supported`] because these answer different
//! questions and only one of them is a gate. The supported list says
//! what must not break; this says how much of PromQL is left, which is
//! the number you want when deciding what to build next rather than
//! whether to merge.
//!
//! Both views are printed on every run, gated or not. The whole corpus
//! evaluates in well under a second with no oracle and no network, so
//! there is nothing to save by not measuring, and a green tick that
//! hides "12% of PromQL works" would be its own kind of lie.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use super::run::{Outcome, Verdict};
use super::supported::Supported;

/// One `.test` file's standing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStats {
    pub file: String,
    pub evals: usize,
    pub passed: usize,
    /// Evals we cannot even ask — a native histogram in the data. Worth
    /// separating from failures: a file at 0/521 because the parser
    /// cannot read histograms is not the same finding as one at 0/213
    /// because the engine gets everything wrong.
    pub skipped: usize,
}

impl FileStats {
    pub fn percent(&self) -> f64 {
        if self.evals == 0 {
            0.0
        } else {
            100.0 * self.passed as f64 / self.evals as f64
        }
    }

    pub fn is_fully_green(&self) -> bool {
        self.evals > 0 && self.passed == self.evals
    }
}

pub fn per_file(outcomes: &[Outcome]) -> Vec<FileStats> {
    let mut by_file: BTreeMap<&str, FileStats> = BTreeMap::new();
    for o in outcomes {
        let stats = by_file.entry(&o.file).or_insert_with(|| FileStats {
            file: o.file.clone(),
            evals: 0,
            passed: 0,
            skipped: 0,
        });
        stats.evals += 1;
        match o.verdict {
            Verdict::Pass => stats.passed += 1,
            Verdict::Skipped(_) => stats.skipped += 1,
            _ => {}
        }
    }
    let mut stats: Vec<FileStats> = by_file.into_values().collect();
    // Best first: the question is "what is closest to done".
    stats.sort_by(|a, b| {
        b.percent()
            .total_cmp(&a.percent())
            .then_with(|| b.evals.cmp(&a.evals))
    });
    stats
}

/// Missing engine features, and how many evals each blocks.
pub fn missing_features(outcomes: &[Outcome]) -> BTreeMap<&str, usize> {
    let mut features: BTreeMap<&str, usize> = BTreeMap::new();
    for o in outcomes {
        if let Verdict::Unsupported(feature) = &o.verdict {
            *features.entry(feature.as_str()).or_default() += 1;
        }
    }
    features
}

/// The per-file table printed on every run.
///
/// Nudges toward `"all"` where a file is fully green, but never acts on
/// it: promoting a file to the stronger form binds evals that do not
/// exist yet, and that should be somebody's decision.
pub fn scoreboard(outcomes: &[Outcome], supported: &Supported) -> String {
    let stats = per_file(outcomes);
    let passed: usize = stats.iter().map(|s| s.passed).sum();
    let total: usize = stats.iter().map(|s| s.evals).sum();

    let mut out = String::new();
    let _ = writeln!(
        out,
        "\npromqltest: {} files, {total} evals — {passed} pass ({:.1}%)\n",
        stats.len(),
        if total == 0 {
            0.0
        } else {
            100.0 * passed as f64 / total as f64
        },
    );

    let width = stats.iter().map(|s| s.file.len()).max().unwrap_or(0);
    for s in &stats {
        let ratio = format!("{}/{}", s.passed, s.evals);
        let _ = write!(
            out,
            "  {:<width$}  {ratio:>9}  {:>5.1}%",
            s.file,
            s.percent()
        );
        if s.is_fully_green() && !supported.batch(&s.file).is_some_and(|b| b.is_all()) {
            let _ = write!(out, "   ← fully green; consider {} = \"all\"", s.file);
        } else if s.skipped == s.evals {
            let _ = write!(out, "   (all skipped)");
        } else if s.skipped > 0 {
            let _ = write!(out, "   ({} skipped)", s.skipped);
        }
        out.push('\n');
    }

    out
}

/// `UNSUPPORTED.md` — the roadmap, regenerated on bless.
///
/// Committed but never gating. It exists so "what should I implement
/// next" is answerable by reading the repo, and so a feature landing
/// shows up as a diff someone can be pleased about, rather than only as
/// a number in a CI log nobody scrolls back to.
pub fn inventory_markdown(outcomes: &[Outcome]) -> String {
    let features = missing_features(outcomes);
    let blocked: usize = features.values().sum();
    let stats = per_file(outcomes);
    let passed: usize = stats.iter().map(|s| s.passed).sum();
    let total: usize = stats.iter().map(|s| s.evals).sum();

    let mut out = String::new();
    out.push_str(
        "# What the engine is missing\n\n\
         Generated — do not edit by hand. Regenerated alongside `SUPPORTED.toml` by:\n\n\
         ```sh\n\
         PROMQL_PROMQLTEST_BLESS=1 cargo test -p promql-conformance --test promqltest\n\
         ```\n\n\
         Nothing here gates CI. It is a roadmap: every row is a count of Prometheus's \
         own promqltest evals that one missing feature blocks, so the top of the table \
         is the cheapest coverage available.\n\n",
    );

    let _ = writeln!(
        out,
        "Against the vendored corpus: **{passed} of {total} evals pass** ({:.1}%), \
         and **{blocked}** are blocked on the features below.\n",
        if total == 0 {
            0.0
        } else {
            100.0 * passed as f64 / total as f64
        },
    );

    out.push_str("## Missing features\n\n| evals blocked | feature |\n|---:|---|\n");
    let mut rows: Vec<(&str, usize)> = features.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    for (feature, count) in rows {
        let _ = writeln!(out, "| {count} | {feature} |");
    }

    out.push_str("\n## By file\n\n| file | evals | passing | |\n|---|---:|---:|---|\n");
    for s in &stats {
        let note = if s.is_fully_green() {
            "fully green"
        } else if s.skipped == s.evals {
            "all skipped — native histograms"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} ({:.0}%) | {note} |",
            s.file,
            s.evals,
            s.passed,
            s.percent()
        );
    }

    out.push_str(
        "\n---\n\n\
         Some failures are one cause wearing many hats — a single lexer gap can account \
         for hundreds of rows above. Read a handful before picking, with:\n\n\
         ```sh\n\
         PROMQL_PROMQLTEST_ALL=1 cargo test -p promql-conformance --test promqltest -- <file>/\n\
         ```\n",
    );

    out
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

    fn sample() -> Vec<Outcome> {
        vec![
            outcome("green", "a @ 0", Verdict::Pass),
            outcome("green", "b @ 0", Verdict::Pass),
            outcome("half", "a @ 0", Verdict::Pass),
            outcome("half", "b @ 0", Verdict::Fail("wrong".into())),
            outcome(
                "none",
                "a @ 0",
                Verdict::Unsupported("the frob function".into()),
            ),
            outcome(
                "none",
                "b @ 0",
                Verdict::Unsupported("the frob function".into()),
            ),
            outcome(
                "none",
                "c @ 0",
                Verdict::Unsupported("the baz function".into()),
            ),
            outcome("hist", "a @ 0", Verdict::Skipped("histogram".into())),
        ]
    }

    #[test]
    fn per_file_counts_and_orders_by_how_close_to_done() {
        let stats = per_file(&sample());
        let names: Vec<&str> = stats.iter().map(|s| s.file.as_str()).collect();
        assert_eq!(names, ["green", "half", "none", "hist"]);
        assert_eq!(stats[0].passed, 2);
        assert_eq!(stats[0].evals, 2);
        assert!(stats[0].is_fully_green());
        assert_eq!(stats[1].percent(), 50.0);
        assert_eq!(stats[3].skipped, 1);
    }

    #[test]
    fn features_are_counted_by_how_many_evals_they_block() {
        let outcomes = sample();
        let features = missing_features(&outcomes);
        assert_eq!(features["the frob function"], 2);
        assert_eq!(features["the baz function"], 1);
        assert_eq!(features.len(), 2);
    }

    #[test]
    fn a_fully_green_file_is_nudged_toward_all_until_it_says_all() {
        let out = scoreboard(&sample(), &Supported::default());
        assert!(out.contains("green = \"all\""), "{out}");

        // Once declared, the nudge goes away rather than repeating.
        let declared = Supported::load_from_str("green = \"all\"\n").expect("parses");
        let out = scoreboard(&sample(), &declared);
        assert!(!out.contains("consider"), "{out}");
    }

    #[test]
    fn the_scoreboard_totals_match_the_outcomes() {
        let out = scoreboard(&sample(), &Supported::default());
        assert!(out.contains("8 evals — 3 pass (37.5%)"), "{out}");
    }

    #[test]
    fn the_inventory_leads_with_the_most_blocking_feature() {
        let md = inventory_markdown(&sample());
        let frob = md.find("the frob function").expect("listed");
        let baz = md.find("the baz function").expect("listed");
        assert!(frob < baz, "the bigger win should come first");
        assert!(md.contains("**3 of 8 evals pass**"), "{md}");
        assert!(md.contains("all skipped"), "{md}");
    }
}
