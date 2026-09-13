//! End to end: binary operators over the in-memory source, expectations
//! worked by hand from the semantics.
//!
//! The shared corpus covers arithmetic and the matching modifiers well,
//! so the cases written out here are the ones it does not reach: the set
//! operators, which it has no clean case for, and the errors, where the
//! corpus only records *that* Prometheus refuses and not what it says.

use std::collections::BTreeMap;
use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery, Series};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

fn source(lines: &[&str]) -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(&load(lines), 30.0))
}

/// Two metrics that share `code` and `method`, so they match one to one.
fn two_metrics() -> Arc<MemorySeriesSource> {
    source(&[
        r#"foo{code="200", method="get"} 1+1x10"#,
        r#"foo{code="200", method="post"} 1+2x10"#,
        r#"bar{code="200", method="get"} 2+0x10"#,
    ])
}

fn run(source: Arc<MemorySeriesSource>, query: &str, range: RangeQuery) -> Vec<Series> {
    Engine::blocking()
        .unwrap()
        .range_query(source, query, &range)
        .unwrap()
}

fn query(q: &str) -> Vec<Series> {
    run(two_metrics(), q, RangeQuery::new(0, 60_000, 30_000))
}

fn error(source: Arc<MemorySeriesSource>, q: &str) -> String {
    let range = RangeQuery::new(0, 60_000, 30_000);
    match Engine::blocking().unwrap().range_query(source, q, &range) {
        Err(EngineError::Query(message)) => message,
        other => panic!("{q}: expected a query error, got {other:?}"),
    }
}

fn by_label<'a>(out: &'a [Series], name: &str) -> BTreeMap<&'a str, &'a Series> {
    out.iter().map(|s| (s.label(name), s)).collect()
}

// ------------------------------------------------------------ arithmetic

/// Only `foo{method="get"}` has a partner; `foo{method="post"}` matches
/// nothing and is simply absent. The metric name goes because addition
/// no longer describes either metric.
#[test]
fn arithmetic_matches_on_every_shared_label_and_drops_the_name() {
    let out = query("foo + bar");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].label("__name__"), "");
    assert_eq!(out[0].label("code"), "200");
    assert_eq!(out[0].label("method"), "get");
    // foo is 1, 2, 3 and bar is 2 at every step.
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [3.0, 4.0, 5.0]);
}

#[test]
fn ignoring_widens_the_match_and_on_narrows_it() {
    // Ignoring `method`, both foo series match the one bar series, which
    // is two matches for one output series.
    let out = error(two_metrics(), "foo + ignoring (method) bar");
    assert!(out.contains("multiple matches"), "{out}");

    // Naming only `code` has the same effect, and keeps only `code`.
    let out = error(two_metrics(), "foo + on (code) bar");
    assert!(out.contains("multiple matches"), "{out}");
}

#[test]
fn a_scalar_applies_to_every_series_and_keeps_its_labels() {
    let out = query("foo * 2");
    assert_eq!(out.len(), 2);
    let by_method = by_label(&out, "method");
    assert_eq!(by_method["get"].values(), [2.0, 4.0, 6.0]);
    assert_eq!(by_method["post"].values(), [2.0, 6.0, 10.0]);
    assert_eq!(by_method["get"].label("__name__"), "");
    assert_eq!(by_method["get"].label("code"), "200");
}

#[test]
fn modulo_and_power_are_the_float_operators() {
    let out = run(
        source(&[r#"x{} 5 7 9"#]),
        "x % 3",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out[0].values(), [5.0f64 % 3.0, 7.0f64 % 3.0, 9.0f64 % 3.0]);

    let out = run(
        source(&[r#"x{} 2 3 4"#]),
        "x ^ 2",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(
        out[0].values(),
        [2.0f64.powf(2.0), 3.0f64.powf(2.0), 4.0f64.powf(2.0)]
    );
}

// ------------------------------------------------------------ comparison

/// A comparison without `bool` is a filter: the series keeps its name and
/// its own values, and loses the steps where it does not hold.
#[test]
fn a_comparison_filters_and_keeps_the_metric_name() {
    let out = run(
        source(&[r#"x{} 1 5 9"#]),
        "x > 4",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].label("__name__"), "x");
    assert_eq!(out[0].timestamps(), [30_000, 60_000]);
    assert_eq!(out[0].values(), [5.0, 9.0]);
}

/// With `bool` nothing is filtered: every step gets a one or a zero, and
/// the name goes, because the result is no longer that metric.
#[test]
fn bool_answers_every_step_and_drops_the_name() {
    let out = run(
        source(&[r#"x{} 1 5 9"#]),
        "x > bool 4",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].label("__name__"), "");
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [0.0, 1.0, 1.0]);
}

/// The vector's value is the result whichever side it was written on.
#[test]
fn a_scalar_on_the_left_still_yields_the_vectors_value() {
    let out = run(
        source(&[r#"x{} 1 5 9"#]),
        "4 < x",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out[0].timestamps(), [30_000, 60_000]);
    assert_eq!(out[0].values(), [5.0, 9.0]);
}

#[test]
fn comparing_two_vectors_filters_the_left_one() {
    let out = query("foo > bar");
    assert_eq!(out.len(), 1);
    // foo is 1, 2, 3 against bar's 2: only the last step holds.
    assert_eq!(out[0].label("__name__"), "foo");
    assert_eq!(out[0].timestamps(), [60_000]);
    assert_eq!(out[0].values(), [3.0]);
}

// -------------------------------------------------------------- scalars

#[test]
fn a_scalar_expression_is_an_unlabelled_series_at_every_step() {
    let out = run(
        source(&[r#"x{} 1"#]),
        "2 ^ 3",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels().count(), 0);
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [8.0, 8.0, 8.0]);

    let out = run(
        source(&[r#"x{} 1"#]),
        "1 == bool 1",
        RangeQuery::new(0, 1_800_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].timestamps().len(), 61);
    assert!(out[0].values().iter().all(|v| *v == 1.0));
}

#[test]
fn a_bare_number_is_a_series_too() {
    let out = run(
        source(&[r#"x{} 1"#]),
        "42",
        RangeQuery::new(0, 30_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels().count(), 0);
    assert_eq!(out[0].values(), [42.0, 42.0]);
}

/// `-x` negates and drops the name, exactly as multiplying by minus one.
#[test]
fn unary_minus_negates_and_drops_the_name() {
    let out = run(
        source(&[r#"x{pod="a"} 1 2 3"#]),
        "-x",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].label("__name__"), "");
    assert_eq!(out[0].label("pod"), "a");
    assert_eq!(out[0].values(), [-1.0, -2.0, -3.0]);
}

/// Negation is the one place Prometheus objects to two series landing
/// on one label set: `x` and `y` differ only in the name it drops.
#[test]
fn unary_minus_refuses_to_merge_two_series_into_one() {
    let message = error(
        source(&[r#"x{pod="a"} 1 2 3"#, r#"y{pod="a"} 4 5 6"#]),
        r#"-{__name__=~"x|y"}"#,
    );
    assert_eq!(
        message,
        "vector cannot contain metrics with the same labelset"
    );
}

/// The sign belongs to the operand, so this is `(-x) + y` and not
/// `-(x + y)`. Upstream gives the unary rule `*`'s precedence.
#[test]
fn unary_minus_binds_tighter_than_the_operator_after_it() {
    let out = run(
        source(&[r#"x{pod="a"} 1 2 3"#, r#"y{pod="a"} 10 10 10"#]),
        "-x + y",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].values(), [9.0, 8.0, 7.0]);
}

// ------------------------------------------------------- set operators

/// Presence, not values: `and` keeps the left sample wherever the right
/// side has one for the same label set, and the left series is otherwise
/// untouched — name included.
#[test]
fn and_keeps_the_left_side_where_the_right_side_reaches() {
    let out = run(
        source(&[
            r#"a{pod="x"} 1 2 3"#,
            r#"b{pod="x"} 9 _ _"#,
            r#"b{pod="y"} 9 9 9"#,
        ]),
        "a and b",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].label("__name__"), "a");
    assert_eq!(out[0].label("pod"), "x");
    // The value is the left side's, at every step b{pod="x"} reaches.
    // Its one sample is at t=0 and the lookback carries it forward.
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [1.0, 2.0, 3.0]);
}

#[test]
fn and_drops_a_series_the_other_side_never_has() {
    let out = run(
        source(&[r#"a{pod="x"} 1 2 3"#, r#"b{pod="y"} 9 9 9"#]),
        "a and b",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert!(out.is_empty());
}

#[test]
fn unless_is_the_other_half_of_and() {
    let out = run(
        source(&[
            r#"a{pod="x"} 1 2 3"#,
            r#"a{pod="y"} 4 5 6"#,
            r#"b{pod="y"} 9 9 9"#,
        ]),
        "a unless b",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].label("pod"), "x");
    assert_eq!(out[0].values(), [1.0, 2.0, 3.0]);
}

/// `or` is every left series plus the right ones the left does not
/// already cover, decided per step and per match group.
#[test]
fn or_fills_in_what_the_left_side_does_not_cover() {
    let out = run(
        source(&[r#"a{pod="x"} 1 2 3"#, r#"b{pod="y"} 7 8 9"#]),
        "a or b",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    let by_pod = by_label(&out, "pod");
    assert_eq!(by_pod["x"].label("__name__"), "a");
    assert_eq!(by_pod["x"].values(), [1.0, 2.0, 3.0]);
    assert_eq!(by_pod["y"].label("__name__"), "b");
    assert_eq!(by_pod["y"].values(), [7.0, 8.0, 9.0]);
}

/// Two halves of one series: the steps the left side answers are its own,
/// and the rest come from the right. They are one output series, not two
/// rows with one label set.
#[test]
fn or_merges_two_branches_into_one_series() {
    let out = run(
        source(&[r#"x{pod="a"} 1 5 9"#]),
        "(x > 4) or (x < 4)",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].label("__name__"), "x");
    assert_eq!(out[0].timestamps(), [0, 30_000, 60_000]);
    assert_eq!(out[0].values(), [1.0, 5.0, 9.0]);
}

// ----------------------------------------------------------- group_left

/// The included label comes from the low-cardinality side, and every
/// label of the high-cardinality side stays.
#[test]
fn group_left_splices_a_label_from_the_one_side() {
    let out = run(
        source(&[
            r#"a{pod="x"} 1 2 3"#,
            r#"a{pod="y"} 4 5 6"#,
            r#"info{pod="x", ns="prod"} 1 1 1"#,
            r#"info{pod="y", ns="dev"} 1 1 1"#,
        ]),
        "a * on (pod) group_left (ns) info",
        RangeQuery::new(0, 60_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    let by_pod = by_label(&out, "pod");
    assert_eq!(by_pod["x"].label("ns"), "prod");
    assert_eq!(by_pod["x"].values(), [1.0, 2.0, 3.0]);
    assert_eq!(by_pod["y"].label("ns"), "dev");
    assert_eq!(by_pod["y"].values(), [4.0, 5.0, 6.0]);
}

/// `group_right` is the same match with the sides swapped, and the
/// arithmetic still runs left to right.
#[test]
fn group_right_reads_the_other_way_round() {
    let out = run(
        source(&[
            r#"a{pod="x"} 10 10 10"#,
            r#"b{pod="x", shard="1"} 1 2 3"#,
            r#"b{pod="x", shard="2"} 4 5 6"#,
        ]),
        "a - on (pod) group_right () b",
        RangeQuery::new(0, 30_000, 30_000),
    );
    assert_eq!(out.len(), 2);
    let by_shard = by_label(&out, "shard");
    assert_eq!(by_shard["1"].values(), [9.0, 8.0]);
    assert_eq!(by_shard["2"].values(), [6.0, 5.0]);
}

/// The low-cardinality side changing its included label mid-window is
/// not two series: the lookback keeps the old one alive, so both are
/// present at once and the match group is no longer unique. Prometheus
/// rejects this, and it is the one collision the result grouping alone
/// would not see, because the two land in different output series.
#[test]
fn a_changing_included_label_is_a_duplicate_match_group() {
    let message = error(
        source(&[
            r#"a{pod="x"} 1 1 1 1 1"#,
            r#"info{pod="x", ns="a"} 1 1 _ _ _"#,
            r#"info{pod="x", ns="b"} _ _ 1 1 1"#,
        ]),
        "a * on (pod) group_left (ns) info",
    );
    assert_eq!(
        message,
        r#"found duplicate series for the match group {pod="x"} on the right hand-side of the operation: [{__name__="info", ns="a", pod="x"}, {__name__="info", ns="b", pod="x"}];many-to-many matching not allowed: matching labels must be unique on one side"#
    );
}

// -------------------------------------------------------------- errors

/// Two series on each side of a one-to-one match: neither side is the
/// "one" side, so there is no match to make.
#[test]
fn many_to_many_names_both_offending_series() {
    let message = error(
        source(&[
            r#"foo{code="200", method="get"} 1+1x5"#,
            r#"foo{code="200", method="post"} 1+1x5"#,
            r#"bar{code="200", method="get"} 1+1x5"#,
            r#"bar{code="200", method="post"} 1+1x5"#,
        ]),
        "foo + on (code) bar",
    );
    assert_eq!(
        message,
        r#"found duplicate series for the match group {code="200"} on the right hand-side of the operation: [{__name__="bar", code="200", method="get"}, {__name__="bar", code="200", method="post"}];many-to-many matching not allowed: matching labels must be unique on one side"#
    );
}

/// Two on the left and one on the right is a match Prometheus will make,
/// but only if it is asked for explicitly.
#[test]
fn two_on_one_side_must_be_declared() {
    let message = error(
        source(&[
            r#"foo{code="200", method="get"} 1+1x5"#,
            r#"foo{code="200", method="post"} 1+1x5"#,
            r#"bar{code="200"} 1+1x5"#,
        ]),
        "foo + on (code) bar",
    );
    assert_eq!(
        message,
        "multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"
    );
}

/// Declared, but the labels kept do not tell the two apart. The many
/// side keeps everything it has, so the only way two of its series can
/// land on one result is for them to differ in `__name__` alone — which
/// arithmetic then drops.
#[test]
fn group_left_still_needs_to_produce_distinct_series() {
    let message = error(
        source(&[
            r#"foo{code="200"} 1+1x5"#,
            r#"baz{code="200"} 1+1x5"#,
            r#"bar{code="200"} 1+1x5"#,
        ]),
        r#"{__name__=~"foo|baz"} + on (code) group_left () bar"#,
    );
    assert_eq!(
        message,
        "multiple matches for labels: grouping labels must ensure unique matches"
    );
}

/// A filtered comparison still counts as a match. Prometheus looks for
/// duplicates before it decides whether to keep the sample, so this is
/// an error even though nothing would have survived the filter.
#[test]
fn a_comparison_that_keeps_nothing_still_reports_the_duplicate() {
    let message = error(
        source(&[
            r#"foo{code="200", method="get"} 0 0 0"#,
            r#"foo{code="200", method="post"} 0 0 0"#,
            r#"bar{code="200"} 5 5 5"#,
        ]),
        "foo > on (code) bar",
    );
    assert!(message.contains("multiple matches"), "{message}");
}

#[test]
fn the_parsers_missing_checks_are_query_errors() {
    for (q, expected) in [
        (
            "foo + bool bar",
            "bool modifier can only be used on comparison operators",
        ),
        (
            "1 == 1",
            "comparisons between scalars must use BOOL modifier",
        ),
        (
            "foo and 1",
            "set operator \"and\" not allowed in binary scalar expression",
        ),
    ] {
        assert_eq!(error(two_metrics(), q), expected, "{q}");
    }
}

// ---------------------------------------------------------- plan shape

#[test]
fn the_plan_joins_on_the_matching_labels_and_groups_by_the_result() {
    let plan = Engine::blocking()
        .unwrap()
        .plan(
            two_metrics().as_ref(),
            "foo + on (code) group_left () bar",
            &RangeQuery::new(0, 60_000, 30_000),
        )
        .unwrap();
    let text = format!("{}", plan.display_indent());
    assert!(text.contains("Inner Join: __many_k0 = __one_k0"), "{text}");
    assert!(text.contains("promql_binary_group"), "{text}");
    assert!(text.contains("Aggregate: groupBy="), "{text}");
}
