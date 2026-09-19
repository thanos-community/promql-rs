//! Executing a parsed `.test` script against an engine.
//!
//! A script is a sequence, not a set: `load` blocks accumulate into
//! storage, an `eval` reads whatever is there, `clear` wipes it. So this
//! walks the commands in order and hands each eval exactly the blocks
//! above it since the last clear — not every block in the file, which
//! would feed cases data that does not exist yet at that point.
//!
//! # Comparison
//!
//! Deliberately *not* [`crate::compare`]. That implements the
//! differential suite's rules, which are Go's
//! `cmp.EquateApprox(1e-10, 1e-6)`. promqltest uses `util/almost.Equal`:
//! a single relative epsilon of `1e-6`, and — the part that matters —
//! Prometheus's stale marker compares equal only to another stale
//! marker, never to an ordinary NaN. Two comparers because there are
//! genuinely two specifications; sharing one would mean quietly
//! applying the wrong tolerances to one of the suites.
//!
//! # Instant queries
//!
//! 1,801 of the 2,098 evals are instant, and they go to
//! `Engine::instant_query`, which answers with the type the query has.
//! So the script's shape and the answer's type are compared directly: a
//! vector eval answered with a matrix is a failure naming both, not
//! something to reshape. The range path still emulates, because a range
//! query is always a matrix and a script can still expect a scalar from
//! one.

use std::collections::BTreeMap;

use promql_parser::SeriesDescription;

use super::script::{Command, Eval, Expected, Range, Script, Timing};
use crate::result::{
    Engine, EngineError, Labels, LoadedSeries, Point, QueryResult, Sample, Series,
};

/// Relative tolerance, from upstream's `defaultEpsilon`.
pub const EPSILON: f64 = 0.000001;

/// Prometheus's stale marker: a NaN with a specific payload.
pub fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == 0x7ff0_0000_0000_0002
}

/// Upstream's `util/almost.Equal`.
///
/// Note the two NaN rules, which are not the same rule: a stale marker
/// equals only another stale marker, while any two ordinary NaNs are
/// equal to each other. Collapsing them would let a genuine NaN satisfy
/// an assertion about staleness.
pub fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    if is_stale_nan(a) || is_stale_nan(b) {
        return is_stale_nan(a) && is_stale_nan(b);
    }
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    // Near zero, or when both are denormal, the relative form is
    // meaningless; fall back to an absolute comparison scaled the same
    // way Go scales it.
    if a == 0.0 || b == 0.0 || abs_sum < f64::MIN_POSITIVE {
        return diff < epsilon * f64::MIN_POSITIVE;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

/// What happened to one eval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    /// The engine and the script disagree. This is the interesting one.
    Fail(String),
    /// The engine has not implemented something the query needs. Named
    /// so the suite can count cases per missing feature.
    Unsupported(String),
    /// Nothing to hold the engine to — an unreadable load block or
    /// expected row, which today means native histograms.
    Skipped(String),
}

impl Verdict {
    pub fn is_pass(&self) -> bool {
        matches!(self, Verdict::Pass)
    }
}

/// One eval and how it went.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// The `.test` file's stem, e.g. `operators`.
    pub file: String,
    /// Identifies the eval within its file, and stays stable across an
    /// upstream re-pin: the query and the evaluation time, plus an
    /// ordinal only when a file repeats the identical query at the
    /// identical time. Line numbers deliberately play no part — comments
    /// reflow, and a baseline keyed on them would invalidate wholesale
    /// on every bump.
    pub id: String,
    /// 1-based line of the `eval`, for humans reading a failure.
    pub line: usize,
    pub query: String,
    pub verdict: Verdict,
    /// How many rows the script expects. Zero means the assertion is
    /// "this returns nothing", which passes against an engine that
    /// returns nothing for any reason — worth counting separately so the
    /// pass total cannot be read as more coverage than it is.
    pub expected_rows: usize,
    /// Whether the eval carried `expect warn`/`info` assertions that
    /// were parsed but not checked.
    pub unchecked_annotations: bool,
}

/// One `load` block's data, as an eval below it sees it.
struct Block {
    series: Vec<SeriesDescription>,
    interval_secs: f64,
    /// Metric names of series lines in this block that did not parse —
    /// native-histogram descriptors, today. `None` for a line whose name
    /// could not be recovered at all.
    ///
    /// An eval reading one of these is being asked a different question
    /// than the script wrote, because the data it needs is missing. But
    /// most are not: `operators.test` seeds one histogram series in a
    /// block that 165 float evals also read from, and skipping all of
    /// them would throw away most of the file.
    missing: Vec<Option<String>>,
}

/// Whether a query could be reading one of the series we failed to load.
///
/// A deliberate over-approximation, and a textual one: it answers "might
/// this touch it?", never "does it?". Two ways to say yes — the query
/// names the metric, or it uses a selector with no metric name at all
/// (`max({job="api-server"})`), which can match anything. A name we
/// could not recover also counts, since there is nothing to compare.
///
/// Over-approximating is the safe direction. A false yes costs one
/// skipped eval; a false no reports a missing-data failure as though the
/// engine were wrong, and step 7 would then bake that into the baseline.
fn might_read(query: &str, missing: &[Option<String>]) -> bool {
    if missing.is_empty() {
        return false;
    }
    if missing.iter().any(Option::is_none) {
        return true;
    }
    if missing
        .iter()
        .flatten()
        .any(|name| query.contains(name.as_str()))
    {
        return true;
    }
    has_nameless_selector(query)
}

fn has_nameless_selector(query: &str) -> bool {
    let bytes = query.as_bytes();
    bytes.iter().enumerate().any(|(i, &b)| {
        b == b'{'
            && !bytes[..i]
                .iter()
                .rev()
                .find(|c| !c.is_ascii_whitespace())
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b':')
    })
}

/// The metric name a series line declares, from either spelling:
/// `foo{bar="baz"} …` or `{__name__="foo", bar="baz"} …`.
fn metric_name(line: &str) -> Option<String> {
    let line = line.trim();
    if let Some(rest) = line.strip_prefix('{') {
        let at = rest.find("__name__")?;
        let eq = rest[at..].find('=')? + at;
        let open = rest[eq..].find('"')? + eq + 1;
        let close = rest[open..].find('"')? + open;
        return Some(rest[open..close].to_string());
    }
    let name: String = line
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == ':')
        .collect();
    (!name.is_empty()).then_some(name)
}

/// Run every eval in a script, in source order.
pub fn run_script(engine: &dyn Engine, script: &Script) -> Vec<Outcome> {
    let mut outcomes = Vec::new();
    // Blocks visible to the next eval: those above it, since the last
    // `clear`. Not every block in the file.
    let mut active: Vec<Block> = Vec::new();
    // Disambiguates a file that asks the identical question twice.
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();

    for command in &script.commands {
        match command {
            Command::Clear => active.clear(),
            Command::Load(load) => active.push(Block {
                series: load.block.parsed().cloned().collect(),
                interval_secs: load.block.interval_secs,
                missing: load
                    .block
                    .unsupported()
                    .map(|u| metric_name(&u.text))
                    .collect(),
            }),
            Command::Eval(eval) => {
                let key = case_key(eval);
                let n = seen.entry(key.clone()).or_insert(0);
                *n += 1;
                let id = if *n > 1 { format!("{key} #{n}") } else { key };
                outcomes.push(Outcome {
                    file: script.name.clone(),
                    id,
                    line: eval.line,
                    query: eval.query.clone(),
                    verdict: run_eval(engine, eval, &active),
                    expected_rows: match &eval.expected {
                        Expected::Series(rows) => rows.len(),
                        Expected::Scalar(_) | Expected::Str(_) => 1,
                        Expected::Nothing => 0,
                    },
                    unchecked_annotations: eval.expect.has_annotation_assertions(),
                });
            }
        }
    }

    outcomes
}

fn case_key(eval: &Eval) -> String {
    let query = eval.query.split_whitespace().collect::<Vec<_>>().join(" ");
    match eval.timing {
        Timing::Instant { at_ms } => format!("{query} @ {at_ms}"),
        Timing::Range(r) => format!("{query} @ {}..{}/{}", r.start_ms, r.end_ms, r.step_ms),
    }
}

fn run_eval(engine: &dyn Engine, eval: &Eval, active: &[Block]) -> Verdict {
    if !eval.is_supported() {
        return Verdict::Skipped(format!(
            "{} expected row(s) we cannot read: {}",
            eval.unsupported.len(),
            eval.unsupported[0].text
        ));
    }
    if active.iter().any(|b| might_read(&eval.query, &b.missing)) {
        return Verdict::Skipped(
            "a series it may read did not load (native histogram)".to_string(),
        );
    }

    let load: Vec<LoadedSeries<'_>> = active
        .iter()
        .filter(|b| !b.series.is_empty())
        .map(|b| LoadedSeries {
            series: &b.series,
            interval_secs: b.interval_secs,
        })
        .collect();

    let answer = match eval.timing {
        Timing::Instant { at_ms } => engine.instant_query(&load, &eval.query, at_ms),
        Timing::Range(r) => engine.range_query(&load, &eval.query, r.start_ms, r.end_ms, r.step_ms),
    };
    let actual = match answer {
        Ok(r) => r,
        Err(EngineError::Unsupported(feature)) => return Verdict::Unsupported(feature),
        Err(e) => return Verdict::Fail(e.to_string()),
    };

    match verify(eval, actual) {
        Ok(()) => Verdict::Pass,
        Err(detail) => Verdict::Fail(detail),
    }
}

/// The shape the script says to expect. Drives normalisation, because
/// our own output cannot distinguish a scalar from a one-series vector.
enum Shape {
    Vector,
    Matrix(Range),
    Scalar,
    Str,
}

fn shape(eval: &Eval) -> Shape {
    match (&eval.expected, eval.timing, eval.range_vector) {
        (Expected::Str(_), _, _) => Shape::Str,
        (Expected::Scalar(_), _, _) => Shape::Scalar,
        (_, _, Some(r)) => Shape::Matrix(r),
        (_, Timing::Range(r), _) => Shape::Matrix(r),
        (_, Timing::Instant { .. }, None) => Shape::Vector,
    }
}

fn verify(eval: &Eval, actual: QueryResult) -> Result<(), String> {
    // `expect fail` is checked before anything else: a failing query has
    // no result to compare.
    if eval.expect.expects_failure() {
        return match actual {
            QueryResult::Error(_) => Ok(()),
            other => Err(format!(
                "expected the query to fail, got {}",
                describe(&other)
            )),
        };
    }
    if let QueryResult::Error(msg) = &actual {
        return Err(format!("query failed: {msg}"));
    }

    let at_ms = match eval.timing {
        Timing::Instant { at_ms } => at_ms,
        Timing::Range(r) => r.start_ms,
    };

    match shape(eval) {
        Shape::Str => {
            let Expected::Str(want) = &eval.expected else {
                unreachable!("shape() returns Str only for Expected::Str")
            };
            match actual {
                QueryResult::Str { v, .. } if &v == want => Ok(()),
                QueryResult::Str { v, .. } => Err(format!("expected string {want:?}, got {v:?}")),
                other => Err(wrong_type("a string", &other)),
            }
        }
        Shape::Scalar => {
            let Expected::Scalar(want) = eval.expected else {
                unreachable!("shape() returns Scalar only for Expected::Scalar")
            };
            let got = match actual {
                QueryResult::Scalar { v, .. } => v,
                // A range eval is still emulated, and a range query is
                // always a matrix, so there the scalar has to be dug
                // back out of one.
                other if matches!(eval.timing, Timing::Range(_)) => normalise_scalar(other)?,
                other => return Err(wrong_type("a scalar", &other)),
            };
            if almost_equal(want, got, EPSILON) {
                Ok(())
            } else {
                Err(format!("expected scalar {want}, got {got}"))
            }
        }
        Shape::Vector => {
            // Only an instant eval has this shape, and an instant query
            // answers with the type its expression has, so anything but
            // a vector is the engine getting the type wrong rather than
            // something to reshape.
            let QueryResult::Vector(got) = actual else {
                return Err(wrong_type("a vector", &actual));
            };
            let want = expected_rows(eval, at_ms, 0);
            compare_vector(&want, &got, eval.expect.ordered)
        }
        Shape::Matrix(r) => {
            let want = expected_rows(eval, r.start_ms, r.step_ms);
            match actual {
                QueryResult::Matrix(series) => compare_matrix(&want, &series),
                other => Err(wrong_type("a matrix", &other)),
            }
        }
    }
}

/// One expected series, already resolved to timestamps.
struct Expected1 {
    labels: Labels,
    points: Vec<Point>,
    pos: usize,
}

fn expected_rows(eval: &Eval, start_ms: i64, step_ms: i64) -> Vec<Expected1> {
    let Expected::Series(rows) = &eval.expected else {
        return Vec::new();
    };
    rows.iter()
        .map(|row| Expected1 {
            labels: labels_of(&row.desc),
            points: row
                .desc
                .values
                .iter()
                .enumerate()
                .filter(|(_, v)| !v.omitted)
                .map(|(i, v)| Point {
                    t: start_ms + i as i64 * step_ms,
                    v: v.value,
                })
                .collect(),
            pos: row.pos,
        })
        .collect()
}

fn labels_of(desc: &SeriesDescription) -> Labels {
    desc.labels
        .iter()
        .map(|l| (l.name.clone(), l.value.clone()))
        .collect()
}

/// The result is of a different type than the script asserts, which is
/// a failure in itself: an instant query's type is part of its answer.
fn wrong_type(want: &str, got: &QueryResult) -> String {
    format!("expected {want}, got {} ({})", got.kind(), describe(got))
}

fn normalise_scalar(actual: QueryResult) -> Result<f64, String> {
    match actual {
        QueryResult::Scalar { v, .. } => Ok(v),
        QueryResult::Matrix(series) => match series.as_slice() {
            [s] if s.floats.len() == 1 => Ok(s.floats[0].v),
            [] => Err("expected a scalar, got an empty result".to_string()),
            _ => Err(format!("expected a scalar, got {} series", series.len())),
        },
        other => Err(format!("expected a scalar, got {}", describe(&other))),
    }
}

fn compare_vector(want: &[Expected1], got: &[Sample], ordered: bool) -> Result<(), String> {
    if want.len() != got.len() {
        return Err(format!(
            "expected {} sample(s), got {}{}",
            want.len(),
            got.len(),
            unexpected_labels(
                want.iter().map(|e| &e.labels),
                got.iter().map(|s| &s.labels)
            ),
        ));
    }

    if ordered {
        // `ordered` is the one case where output order is the
        // assertion, so it cannot go through the label-keyed path
        // below: sorting first would make the check vacuous.
        let mut want: Vec<&Expected1> = want.iter().collect();
        want.sort_by_key(|e| e.pos);
        for (i, (w, g)) in want.iter().zip(got).enumerate() {
            if w.labels != g.labels {
                return Err(format!(
                    "position {}: expected {:?}, got {:?}",
                    i + 1,
                    w.labels,
                    g.labels
                ));
            }
            let wv = w.points.first().map(|p| p.v).unwrap_or(f64::NAN);
            if !almost_equal(wv, g.v, EPSILON) {
                return Err(format!("position {}: expected {wv}, got {}", i + 1, g.v));
            }
        }
        return Ok(());
    }

    let by_labels: BTreeMap<&Labels, &Sample> = got.iter().map(|s| (&s.labels, s)).collect();
    for w in want {
        let Some(g) = by_labels.get(&w.labels) else {
            return Err(format!("expected metric {:?} not in the result", w.labels));
        };
        let wv = w.points.first().map(|p| p.v).unwrap_or(f64::NAN);
        if !almost_equal(wv, g.v, EPSILON) {
            return Err(format!("{:?}: expected {wv}, got {}", w.labels, g.v));
        }
    }
    Ok(())
}

fn compare_matrix(want: &[Expected1], got: &[Series]) -> Result<(), String> {
    if want.len() != got.len() {
        return Err(format!(
            "expected {} series, got {}{}",
            want.len(),
            got.len(),
            unexpected_labels(
                want.iter().map(|e| &e.labels),
                got.iter().map(|s| &s.labels)
            ),
        ));
    }

    let by_labels: BTreeMap<&Labels, &Series> = got.iter().map(|s| (&s.labels, s)).collect();
    for w in want {
        let Some(g) = by_labels.get(&w.labels) else {
            return Err(format!("expected metric {:?} not in the result", w.labels));
        };
        if w.points.len() != g.floats.len() {
            return Err(format!(
                "{:?}: expected {} point(s), got {}",
                w.labels,
                w.points.len(),
                g.floats.len()
            ));
        }
        for (wp, gp) in w.points.iter().zip(&g.floats) {
            if wp.t != gp.t {
                return Err(format!(
                    "{:?}: expected a point at {}, got one at {}",
                    w.labels, wp.t, gp.t
                ));
            }
            if !almost_equal(wp.v, gp.v, EPSILON) {
                return Err(format!(
                    "{:?} @{}: expected {}, got {}",
                    w.labels, wp.t, wp.v, gp.v
                ));
            }
        }
    }
    Ok(())
}

/// Name the label sets that differ, so a count mismatch says which
/// series is missing rather than only how many.
fn unexpected_labels<'a>(
    want: impl Iterator<Item = &'a Labels>,
    got: impl Iterator<Item = &'a Labels>,
) -> String {
    let want: Vec<&Labels> = want.collect();
    let got: Vec<&Labels> = got.collect();
    let missing: Vec<_> = want.iter().filter(|l| !got.contains(l)).collect();
    let extra: Vec<_> = got.iter().filter(|l| !want.contains(l)).collect();
    let mut out = String::new();
    if !missing.is_empty() {
        out.push_str(&format!("\n  missing: {missing:?}"));
    }
    if !extra.is_empty() {
        out.push_str(&format!("\n  unexpected: {extra:?}"));
    }
    out
}

fn describe(result: &QueryResult) -> String {
    match result {
        QueryResult::Matrix(series) => {
            let samples: usize = series.iter().map(|s| s.floats.len()).sum();
            format!("a matrix of {} series, {samples} samples", series.len())
        }
        QueryResult::Vector(samples) => format!("a vector of {} samples", samples.len()),
        QueryResult::Scalar { v, .. } => format!("scalar {v}"),
        QueryResult::Str { v, .. } => format!("string {v:?}"),
        QueryResult::Error(e) => format!("error: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_and_ordinary_nan_are_different_things() {
        let stale = f64::from_bits(0x7ff0_0000_0000_0002);
        assert!(almost_equal(stale, stale, EPSILON));
        assert!(almost_equal(f64::NAN, f64::NAN, EPSILON));
        assert!(!almost_equal(stale, f64::NAN, EPSILON));
        assert!(!almost_equal(f64::NAN, stale, EPSILON));
    }

    /// Note the denominator is the *sum* of the magnitudes, not either
    /// one, so the tolerance on the difference is ~2ε rather than ε.
    /// That is upstream's formula, quirk included.
    #[test]
    fn tolerance_is_relative() {
        assert!(almost_equal(1e9, 1e9 + 1.0, EPSILON));
        assert!(almost_equal(1.0, 1.0000000001, EPSILON));
        assert!(almost_equal(1.0, 1.000001, EPSILON));
        assert!(!almost_equal(1.0, 1.00001, EPSILON));
    }

    #[test]
    fn infinities_compare_by_identity() {
        assert!(almost_equal(f64::INFINITY, f64::INFINITY, EPSILON));
        assert!(!almost_equal(f64::INFINITY, f64::NEG_INFINITY, EPSILON));
        assert!(!almost_equal(f64::INFINITY, 1e308, EPSILON));
    }

    #[test]
    fn zero_uses_the_absolute_fallback() {
        assert!(almost_equal(0.0, 0.0, EPSILON));
        assert!(!almost_equal(0.0, 1e-30, EPSILON));
    }

    #[test]
    fn a_metric_name_is_read_from_either_spelling() {
        assert_eq!(metric_name("foo{a=\"b\"} 1 2 3").as_deref(), Some("foo"));
        assert_eq!(metric_name("  foo 1 2 3").as_deref(), Some("foo"));
        assert_eq!(
            metric_name("{__name__=\"testhistogram\", id=\"1\"} {{schema:-53}}").as_deref(),
            Some("testhistogram")
        );
        assert_eq!(metric_name("{a=\"b\"} 1").as_deref(), None);
    }

    /// The case that motivates the whole heuristic: `operators.test`
    /// loads one histogram series into a block that 165 float evals also
    /// read. Poisoning the block wholesale would skip all of them.
    #[test]
    fn a_query_naming_other_metrics_is_not_poisoned() {
        let missing = vec![Some("http_requests_histogram".to_string())];
        assert!(!might_read("sum(http_requests) by (job)", &missing));
        assert!(might_read("sum(http_requests_histogram)", &missing));
    }

    #[test]
    fn a_selector_without_a_metric_name_matches_anything() {
        let missing = vec![Some("http_requests_histogram".to_string())];
        assert!(might_read("max({job=\"api-server\"})", &missing));
        assert!(might_read("count({__name__=~\".+\"})", &missing));
    }

    #[test]
    fn a_named_selector_is_not_mistaken_for_a_nameless_one() {
        assert!(!has_nameless_selector("http_requests{job=\"x\"}"));
        assert!(!has_nameless_selector("rate(foo{a=\"b\"}[5m])"));
        // A space between name and braces is still a named selector.
        assert!(!has_nameless_selector("http_requests {job=\"x\"}"));
        assert!(!has_nameless_selector("sum by (job) (foo)"));
        assert!(!has_nameless_selector("foo[5m:1m]"));
        assert!(has_nameless_selector("{job=\"x\"}"));
        assert!(has_nameless_selector("sum({job=\"x\"})"));
    }

    #[test]
    fn a_block_that_loaded_cleanly_poisons_nothing() {
        assert!(!might_read("{job=\"x\"}", &[]));
    }

    /// A name we could not recover means we cannot reason about it, so
    /// everything downstream is suspect.
    #[test]
    fn an_unrecoverable_name_poisons_conservatively() {
        assert!(might_read("sum(unrelated)", &[None]));
    }

    // ---------------- the runner ----------------
    //
    // These go through `run_script` on hand-written snippets rather than
    // calling the comparison helpers directly, because the parts most
    // likely to be wrong are the ones joining them up: which load blocks
    // an eval sees, and which comparison a script's shape selects.
    //
    // They also cover paths the corpus cannot reach yet. All 24 `expect
    // ordered` cases are `topk`/`sort`, which the engine does not
    // implement, so the ordered comparison would otherwise ship with no
    // test at all and start gating results the day sorting lands.

    use std::cell::RefCell;

    use crate::result::EngineError;

    use super::super::script::parse;

    /// An engine that answers from a closure, so a test can state the
    /// engine's reply and check only what the runner does with it.
    struct Stub<F>(F);

    impl<F: Fn(&str) -> Result<QueryResult, EngineError>> Engine for Stub<F> {
        fn range_query(
            &self,
            _load: &[LoadedSeries<'_>],
            query: &str,
            _start_ms: i64,
            _end_ms: i64,
            _step_ms: i64,
        ) -> Result<QueryResult, EngineError> {
            (self.0)(query)
        }

        fn instant_query(
            &self,
            _load: &[LoadedSeries<'_>],
            query: &str,
            _at_ms: i64,
        ) -> Result<QueryResult, EngineError> {
            (self.0)(query)
        }
    }

    /// One vector element, as a test writes it inline.
    fn vector(rows: &[Row<'_>]) -> QueryResult {
        QueryResult::Vector(
            rows.iter()
                .map(|(labels, points)| Sample {
                    labels: labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    t: points[0].0,
                    v: points[0].1,
                    histogram: false,
                })
                .collect(),
        )
    }

    /// A label set and its points, as a test writes them inline.
    type Row<'a> = (&'a [(&'a str, &'a str)], &'a [(i64, f64)]);

    fn matrix(rows: &[Row<'_>]) -> QueryResult {
        QueryResult::Matrix(
            rows.iter()
                .map(|(labels, points)| Series {
                    labels: labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    floats: points.iter().map(|&(t, v)| Point { t, v }).collect(),
                    histograms: 0,
                })
                .collect(),
        )
    }

    fn verdicts(input: &str, engine: &dyn Engine) -> Vec<Verdict> {
        let script = parse("t", input).expect("the snippet parses");
        run_script(engine, &script)
            .into_iter()
            .map(|o| o.verdict)
            .collect()
    }

    /// The rule that would poison many cases at once if wrong: an eval
    /// sees the loads above it since the last `clear`, and no others.
    #[test]
    fn an_eval_sees_only_the_loads_above_it_since_the_last_clear() {
        let counts = RefCell::new(Vec::new());
        let script = parse(
            "t",
            "load 1m\n  a 1\n\neval instant at 0 first\n\nload 1m\n  b 2\n\n\
             eval instant at 0 second\n\nclear\n\nload 1m\n  c 3\n\n\
             eval instant at 0 third\n",
        )
        .expect("the snippet parses");

        let _ = run_script(&CountingStub(&counts), &script);
        assert_eq!(counts.borrow().as_slice(), &[1, 2, 1]);
    }

    /// Records how many series each eval was seeded with.
    struct CountingStub<'a>(&'a RefCell<Vec<usize>>);

    impl Engine for CountingStub<'_> {
        fn range_query(
            &self,
            load: &[LoadedSeries<'_>],
            _query: &str,
            _start_ms: i64,
            _end_ms: i64,
            _step_ms: i64,
        ) -> Result<QueryResult, EngineError> {
            self.0
                .borrow_mut()
                .push(load.iter().map(|b| b.series.len()).sum());
            Ok(matrix(&[]))
        }

        fn instant_query(
            &self,
            load: &[LoadedSeries<'_>],
            _query: &str,
            _at_ms: i64,
        ) -> Result<QueryResult, EngineError> {
            self.0
                .borrow_mut()
                .push(load.iter().map(|b| b.series.len()).sum());
            Ok(vector(&[]))
        }
    }

    #[test]
    fn an_instant_eval_compares_the_vector_the_engine_returned() {
        let engine = Stub(|_: &str| Ok(vector(&[(&[("job", "a")], &[(0, 7.0)])])));
        assert_eq!(
            verdicts("eval instant at 0 q\n  {job=\"a\"} 7\n", &engine),
            [Verdict::Pass]
        );
        assert!(!verdicts("eval instant at 0 q\n  {job=\"a\"} 8\n", &engine)[0].is_pass());
    }

    /// The type is part of the answer, so an engine that returns the
    /// wrong one fails rather than being reshaped into the right one.
    #[test]
    fn an_instant_eval_answered_with_the_wrong_type_fails_naming_both() {
        let engine = Stub(|_: &str| Ok(matrix(&[(&[("job", "a")], &[(0, 7.0)])])));
        let Verdict::Fail(detail) = &verdicts("eval instant at 0 q\n  {job=\"a\"} 7\n", &engine)[0]
        else {
            panic!("expected a failure")
        };
        assert!(detail.contains("expected a vector"), "{detail}");
        assert!(detail.contains("matrix"), "{detail}");

        // And the same the other way round, for a range-vector eval.
        let engine = Stub(|_: &str| Ok(vector(&[(&[("job", "a")], &[(0, 7.0)])])));
        let script = "eval instant at 0 q\n  expect range vector from 0 to 0 step 1m\n  \
                      {job=\"a\"} 7\n";
        let Verdict::Fail(detail) = &verdicts(script, &engine)[0] else {
            panic!("expected a failure")
        };
        assert!(detail.contains("expected a matrix"), "{detail}");
    }

    #[test]
    fn an_unordered_eval_ignores_the_order_the_engine_returned() {
        let engine = Stub(|_: &str| {
            Ok(vector(&[
                (&[("job", "b")], &[(0, 2.0)]),
                (&[("job", "a")], &[(0, 1.0)]),
            ]))
        });
        let script = "eval instant at 0 q\n  {job=\"a\"} 1\n  {job=\"b\"} 2\n";
        assert_eq!(verdicts(script, &engine), [Verdict::Pass]);
    }

    /// The same reply and the same expectation as above, plus `expect
    /// ordered` — which must now reject it.
    #[test]
    fn an_ordered_eval_holds_the_engine_to_the_order() {
        let engine = Stub(|_: &str| {
            Ok(vector(&[
                (&[("job", "b")], &[(0, 2.0)]),
                (&[("job", "a")], &[(0, 1.0)]),
            ]))
        });
        let script = "eval instant at 0 q\n  expect ordered\n  {job=\"a\"} 1\n  {job=\"b\"} 2\n";
        let Verdict::Fail(detail) = &verdicts(script, &engine)[0] else {
            panic!("expected the wrong order to be caught")
        };
        assert!(detail.contains("position 1"), "{detail}");

        let script = "eval instant at 0 q\n  expect ordered\n  {job=\"b\"} 2\n  {job=\"a\"} 1\n";
        assert_eq!(verdicts(script, &engine), [Verdict::Pass]);
    }

    #[test]
    fn a_range_eval_compares_every_point_and_its_timestamp() {
        let engine = Stub(|_: &str| Ok(matrix(&[(&[("job", "a")], &[(0, 1.0), (60_000, 2.0)])])));
        let script = "eval range from 0 to 1m step 1m q\n  {job=\"a\"} 1 2\n";
        assert_eq!(verdicts(script, &engine), [Verdict::Pass]);

        let script = "eval range from 0 to 1m step 1m q\n  {job=\"a\"} 1 3\n";
        assert!(!verdicts(script, &engine)[0].is_pass());
    }

    #[test]
    fn expect_fail_wants_an_error_and_nothing_else() {
        let erroring = Stub(|_: &str| Ok(QueryResult::Error("bad".into())));
        let answering = Stub(|_: &str| Ok(vector(&[(&[], &[(0, 1.0)])])));
        let script = "eval instant at 0 q\n  expect fail\n";
        assert_eq!(verdicts(script, &erroring), [Verdict::Pass]);
        assert!(!verdicts(script, &answering)[0].is_pass());
    }

    #[test]
    fn a_bare_number_is_compared_as_a_scalar() {
        let engine = Stub(|_: &str| Ok(QueryResult::Scalar { v: 42.0, t: 0 }));
        assert_eq!(
            verdicts("eval instant at 0 q\n  42\n", &engine),
            [Verdict::Pass]
        );
        assert!(!verdicts("eval instant at 0 q\n  43\n", &engine)[0].is_pass());

        // A range eval expecting a scalar still digs it out of the
        // matrix a range query always answers with.
        let engine = Stub(|_: &str| Ok(matrix(&[(&[], &[(0, 42.0)])])));
        let script = "eval range from 0 to 0 step 1m q\n  42\n";
        assert_eq!(verdicts(script, &engine), [Verdict::Pass]);
    }

    #[test]
    fn an_unsupported_feature_is_reported_as_such_not_as_a_failure() {
        let engine = Stub(|_: &str| Err(EngineError::Unsupported("the frob function".into())));
        assert_eq!(
            verdicts("eval instant at 0 frob(x)\n  1\n", &engine),
            [Verdict::Unsupported("the frob function".into())]
        );
    }
}
