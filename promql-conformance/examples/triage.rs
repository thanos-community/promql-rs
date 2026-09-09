//! Triage the corpus by how much engine machinery each case needs.
//!
//! Picking what to implement next is otherwise guesswork over 251
//! queries. This walks each case's AST and reports the *features* it
//! uses — selectors, functions, aggregations, binary operators,
//! subqueries, `@`, offsets — so cases can be grouped by the smallest
//! engine that would satisfy them.
//!
//! ```console
//! $ PROMQL_ENGINE_TESTCASES=.../testcases \
//!     cargo run -p promql-conformance --example triage
//! ```
//!
//! Pass `-v` to list every case in a tier rather than a sample.

use std::collections::{BTreeMap, BTreeSet};

use promql_conformance::QueryResult;
use promql_parser::ast::Expr;
use promql_testcases::{range_queries_in, testcases_dir, Case};

/// One capability a query needs from the engine. Ordered roughly by
/// how much machinery it implies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Feature {
    /// An instant vector selector: `metric{label="v"}`.
    Selector,
    /// A bare number, as in `vector(1)` or `1 + 1`.
    NumberLiteral,
    StringLiteral,
    Paren,
    /// Unary minus.
    Unary,
    /// `metric[5m]`, needed by every range function.
    MatrixSelector,
    /// Any function call, recorded by name.
    Call,
    /// `sum`, `topk`, ... recorded by operator.
    Aggregation,
    /// Arithmetic, comparison or set operator.
    BinaryOp,
    /// Vector matching: `on`, `ignoring`, `group_left`, `group_right`.
    VectorMatching,
    /// `expr[5m:30s]`.
    Subquery,
    /// `offset 5m`, including negative offsets.
    Offset,
    /// `@ 1234`, `@ start()`, `@ end()`.
    AtModifier,
    /// Experimental duration arithmetic.
    DurationExpr,
    /// Parsed as a step-invariant subtree.
    StepInvariant,
}

impl Feature {
    fn label(self) -> &'static str {
        match self {
            Feature::Selector => "selector",
            Feature::NumberLiteral => "number",
            Feature::StringLiteral => "string",
            Feature::Paren => "paren",
            Feature::Unary => "unary",
            Feature::MatrixSelector => "range-selector",
            Feature::Call => "function-call",
            Feature::Aggregation => "aggregation",
            Feature::BinaryOp => "binary-op",
            Feature::VectorMatching => "vector-matching",
            Feature::Subquery => "subquery",
            Feature::Offset => "offset",
            Feature::AtModifier => "@-modifier",
            Feature::DurationExpr => "duration-expr",
            Feature::StepInvariant => "step-invariant",
        }
    }
}

#[derive(Debug, Default)]
struct Profile {
    features: BTreeSet<Feature>,
    /// Function and aggregation names, for reporting which builtins a
    /// tier would need.
    functions: BTreeSet<String>,
}

fn main() {
    let verbose = std::env::args().any(|a| a == "-v");

    let Some(dir) = testcases_dir() else {
        eprintln!(
            "set {} to a promql-engine checkout's testcases directory",
            promql_testcases::TESTCASES_DIR_ENV
        );
        std::process::exit(1);
    };
    let cases = range_queries_in(&dir).expect("load range_queries.yaml");

    let mut profiled: Vec<(&Case, Profile)> = Vec::new();
    let mut unparsed = Vec::new();
    for case in &cases {
        match promql_parser::parse_expr(&case.query) {
            Ok(expr) => {
                let mut p = Profile::default();
                walk(&expr, &mut p);
                profiled.push((case, p));
            }
            // Our own parser failing is worth knowing about here: such a
            // case cannot be attempted at all yet, whatever the engine
            // does.
            Err(_) => unparsed.push(case),
        }
    }

    report_tiers(&profiled, verbose);
    report_simplest(&profiled);
    report_marginal_value(&profiled);
    report_feature_frequency(&profiled);
    report_builtins(&profiled);

    if !unparsed.is_empty() {
        println!("\n=== queries our parser rejects ({}) ===", unparsed.len());
        for case in &unparsed {
            println!("  {}: {}", case.name, one_line(&case.query));
        }
    }
}

/// Group cases by the smallest feature set that covers them.
fn report_tiers(profiled: &[(&Case, Profile)], verbose: bool) {
    // Tiers are cumulative: each admits everything the previous one did
    // plus its own additions, which is how an engine actually grows.
    let tiers: &[(&str, &[Feature])] = &[
        ("1. bare selectors", &[Feature::Selector]),
        (
            "2. + literals and parens",
            &[
                Feature::Selector,
                Feature::NumberLiteral,
                Feature::StringLiteral,
                Feature::Paren,
                Feature::Unary,
            ],
        ),
        (
            "3. + aggregations",
            &[
                Feature::Selector,
                Feature::NumberLiteral,
                Feature::StringLiteral,
                Feature::Paren,
                Feature::Unary,
                Feature::Aggregation,
            ],
        ),
        (
            "4. + range selectors and functions",
            &[
                Feature::Selector,
                Feature::NumberLiteral,
                Feature::StringLiteral,
                Feature::Paren,
                Feature::Unary,
                Feature::Aggregation,
                Feature::MatrixSelector,
                Feature::Call,
            ],
        ),
        (
            "5. + binary operators",
            &[
                Feature::Selector,
                Feature::NumberLiteral,
                Feature::StringLiteral,
                Feature::Paren,
                Feature::Unary,
                Feature::Aggregation,
                Feature::MatrixSelector,
                Feature::Call,
                Feature::BinaryOp,
                Feature::VectorMatching,
            ],
        ),
    ];

    println!("=== cumulative tiers ===");
    let mut previous = 0usize;
    for (name, allowed) in tiers {
        let allowed: BTreeSet<Feature> = allowed.iter().copied().collect();
        let members: Vec<&Case> = profiled
            .iter()
            .filter(|(_, p)| p.features.is_subset(&allowed))
            .map(|(c, _)| *c)
            .collect();
        let added = members.len().saturating_sub(previous);
        println!("{name}: {} cases (+{added})", members.len());
        if verbose {
            for c in &members {
                println!("      {}", c.name);
            }
        }
        previous = members.len();
    }

    // Cases that touch no series at all. These need no storage, no
    // selector and no lookback -- only stepping the range and shaping a
    // result -- so they isolate engine plumbing from storage, which is
    // where the bulk of the work is.
    let storage_free: BTreeSet<Feature> = [
        Feature::NumberLiteral,
        Feature::StringLiteral,
        Feature::Paren,
        Feature::Unary,
        Feature::Call,
    ]
    .into_iter()
    .collect();
    let no_storage: Vec<&(&Case, Profile)> = profiled
        .iter()
        .filter(|(_, p)| {
            p.features.is_subset(&storage_free) && !p.features.contains(&Feature::Selector)
        })
        .collect();

    println!(
        "\n=== needs no storage at all: {} cases ===",
        no_storage.len()
    );
    for (case, profile) in &no_storage {
        println!(
            "  {:<44} {:<22} {}",
            truncate(&case.name, 44),
            profile
                .functions
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(","),
            one_line(&case.query),
        );
    }

    let only_selectors: BTreeSet<Feature> = [Feature::Selector].into_iter().collect();
    let starters: Vec<&(&Case, Profile)> = profiled
        .iter()
        .filter(|(_, p)| p.features.is_subset(&only_selectors))
        .collect();

    println!("\n=== needs only a selector: {} cases ===", starters.len());
    for (case, _) in &starters {
        println!(
            "  {:<44} {:>3} series loaded  {}",
            truncate(&case.name, 44),
            case.load.as_ref().map(|l| l.series.len()).unwrap_or(0),
            one_line(&case.query),
        );
    }

    // Selector plus one elementwise function: the smallest step that
    // actually reads data and transforms it.
    let elementwise: BTreeSet<Feature> = [
        Feature::Selector,
        Feature::NumberLiteral,
        Feature::Paren,
        Feature::Unary,
        Feature::Call,
    ]
    .into_iter()
    .collect();
    let with_data: Vec<&(&Case, Profile)> = profiled
        .iter()
        .filter(|(_, p)| {
            p.features.is_subset(&elementwise) && p.features.contains(&Feature::Selector)
        })
        .collect();
    println!(
        "\n=== selector + literals + a function call: {} cases ===",
        with_data.len()
    );
    let mut fns: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, p) in &with_data {
        for f in &p.functions {
            *fns.entry(f.as_str()).or_default() += 1;
        }
    }
    let mut fn_rows: Vec<(&str, usize)> = fns.into_iter().collect();
    fn_rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    println!(
        "  functions involved: {}",
        fn_rows
            .iter()
            .map(|(n, c)| format!("{n}({c})"))
            .collect::<Vec<_>>()
            .join(" ")
    );
}

/// The simplest cases by feature count, with what the oracle actually
/// returns.
///
/// Feature count alone is a poor guide to a first milestone: the two
/// selector-only cases in this corpus both return an empty matrix, so
/// passing them would prove almost nothing. Showing the result size
/// separates cases that exercise real data from cases that only check
/// emptiness.
fn report_simplest(profiled: &[(&Case, Profile)]) {
    let oracle = match promql_conformance::oracle::shared() {
        Ok(o) => o,
        Err(e) => {
            println!("\n(skipping result sizes: oracle unavailable: {e})");
            return;
        }
    };

    let mut rows: Vec<(&Case, &Profile)> = profiled.iter().map(|(c, p)| (*c, p)).collect();
    rows.sort_by_key(|(c, p)| (p.features.len(), c.name.clone()));

    println!("\n=== 25 simplest cases, with what the oracle returns ===");
    println!("  {:<44} series  samples  features", "case");
    for (case, profile) in rows.iter().take(25) {
        let load = case.load.as_ref().map(|l| l.raw.as_str()).unwrap_or("");
        let summary =
            match oracle.query(load, &case.query, case.start_ms, case.end_ms, case.step_ms) {
                Ok(QueryResult::Matrix(series)) => {
                    let samples: usize = series.iter().map(|s| s.floats.len()).sum();
                    format!("{:>6} {:>8}", series.len(), samples)
                }
                Ok(QueryResult::Error(_)) => format!("{:>15}", "(errors)"),
                Ok(other) => format!("{:>15}", other.kind()),
                Err(_) => format!("{:>15}", "(unavailable)"),
            };
        let features: Vec<&str> = profile.features.iter().map(|f| f.label()).collect();
        println!(
            "  {:<44} {summary}  {}",
            truncate(&case.name, 44),
            features.join(",")
        );
    }
}

/// For each feature not yet implemented, how many further cases adding
/// it would unlock on top of a chosen baseline.
///
/// This is the question "what should I build next" stated directly,
/// rather than inferred from a frequency table: a feature can be common
/// yet unlock nothing on its own because every case using it also needs
/// something else.
fn report_marginal_value(profiled: &[(&Case, Profile)]) {
    let baseline: BTreeSet<Feature> = [
        Feature::Selector,
        Feature::NumberLiteral,
        Feature::StringLiteral,
        Feature::Paren,
        Feature::Unary,
    ]
    .into_iter()
    .collect();

    let covered = |set: &BTreeSet<Feature>| {
        profiled
            .iter()
            .filter(|(_, p)| p.features.is_subset(set))
            .count()
    };

    let base_count = covered(&baseline);
    println!("\n=== marginal value on top of selectors + literals ({base_count} cases) ===");

    let candidates = [
        Feature::Offset,
        Feature::AtModifier,
        Feature::MatrixSelector,
        Feature::Call,
        Feature::Aggregation,
        Feature::BinaryOp,
        Feature::VectorMatching,
        Feature::Subquery,
        Feature::StepInvariant,
        Feature::DurationExpr,
    ];

    let mut rows: Vec<(Feature, usize)> = candidates
        .iter()
        .map(|f| {
            let mut set = baseline.clone();
            set.insert(*f);
            (*f, covered(&set) - base_count)
        })
        .collect();
    rows.sort_by_key(|(_, n)| std::cmp::Reverse(*n));

    for (f, gained) in rows {
        println!("  + {:<18} unlocks {gained:>3} more", f.label());
    }

    // Pairs, because the single-feature view understates combinations
    // that only pay off together.
    let mut pairs: Vec<(Feature, Feature, usize)> = Vec::new();
    for (i, a) in candidates.iter().enumerate() {
        for b in &candidates[i + 1..] {
            let mut set = baseline.clone();
            set.insert(*a);
            set.insert(*b);
            pairs.push((*a, *b, covered(&set) - base_count));
        }
    }
    pairs.sort_by_key(|(_, _, n)| std::cmp::Reverse(*n));
    println!("  -- best pairs --");
    for (a, b, gained) in pairs.iter().take(5) {
        println!(
            "  + {:<16} + {:<16} unlocks {gained:>3} more",
            a.label(),
            b.label()
        );
    }
}

fn report_feature_frequency(profiled: &[(&Case, Profile)]) {
    let mut counts: BTreeMap<Feature, usize> = BTreeMap::new();
    for (_, p) in profiled {
        for f in &p.features {
            *counts.entry(*f).or_default() += 1;
        }
    }
    println!("\n=== how many cases use each feature ===");
    let mut rows: Vec<(Feature, usize)> = counts.into_iter().collect();
    rows.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    for (f, n) in rows {
        println!("  {:<18} {n:>3}", f.label());
    }
}

fn report_builtins(profiled: &[(&Case, Profile)]) {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, p) in profiled {
        for name in &p.functions {
            *counts.entry(name.as_str()).or_default() += 1;
        }
    }
    let mut rows: Vec<(&str, usize)> = counts.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    println!(
        "\n=== functions and aggregations by case count ({} distinct) ===",
        rows.len()
    );
    for chunk in rows.chunks(6) {
        let line: Vec<String> = chunk.iter().map(|(n, c)| format!("{n}({c})")).collect();
        println!("  {}", line.join("  "));
    }
}

fn walk(expr: &Expr, p: &mut Profile) {
    match expr {
        Expr::VectorSelector(v) => {
            p.features.insert(Feature::Selector);
            // Upstream folds `offset` into offset_secs and records an
            // explicit timestamp for `@`, so check both representations.
            if v.offset_secs != 0.0 || v.original_offset_secs != 0.0 {
                p.features.insert(Feature::Offset);
            }
            if v.timestamp.is_some() || v.start_or_end.is_some() {
                p.features.insert(Feature::AtModifier);
            }
        }
        Expr::MatrixSelector(m) => {
            p.features.insert(Feature::MatrixSelector);
            walk(&m.vector_selector, p);
        }
        Expr::NumberLiteral(_) => {
            p.features.insert(Feature::NumberLiteral);
        }
        Expr::StringLiteral(_) => {
            p.features.insert(Feature::StringLiteral);
        }
        Expr::Paren(e) => {
            p.features.insert(Feature::Paren);
            walk(&e.expr, p);
        }
        Expr::Unary(e) => {
            p.features.insert(Feature::Unary);
            walk(&e.expr, p);
        }
        Expr::Call(c) => {
            p.features.insert(Feature::Call);
            p.functions.insert(c.func.name.clone());
            for a in &c.args {
                walk(a, p);
            }
        }
        Expr::Aggregate(a) => {
            p.features.insert(Feature::Aggregation);
            p.functions.insert(format!("{:?}", a.op).to_lowercase());
            walk(&a.expr, p);
            if let Some(param) = &a.param {
                walk(param, p);
            }
        }
        Expr::Binary(b) => {
            p.features.insert(Feature::BinaryOp);
            if b.vector_matching.is_some() {
                p.features.insert(Feature::VectorMatching);
            }
            walk(&b.lhs, p);
            walk(&b.rhs, p);
        }
        Expr::Subquery(s) => {
            p.features.insert(Feature::Subquery);
            walk(&s.expr, p);
        }
        Expr::StepInvariant(e) => {
            p.features.insert(Feature::StepInvariant);
            walk(e, p);
        }
        Expr::Duration(_) => {
            p.features.insert(Feature::DurationExpr);
        }
    }
}

fn one_line(s: &str) -> String {
    let joined = s.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate(&joined, 70)
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let head: String = s.chars().take(n.saturating_sub(1)).collect();
        format!("{head}…")
    }
}
