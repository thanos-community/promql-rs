//! Duration expressions (`generated_parser.y`'s `duration_expr` family):
//! what the parser keeps as a literal, what it keeps as a `DurationExpr`
//! for the engine to evaluate, and what it refuses.

use promql_parser::ast::{DurationExpr, Expr};
use promql_parser::parse_expr;
use promql_parser::token::ItemType;

fn must_parse(q: &str) -> Expr {
    parse_expr(q).unwrap_or_else(|e| panic!("failed to parse {q:?}: {e}"))
}

fn range_of(q: &str) -> (f64, Option<DurationExpr>) {
    match must_parse(q) {
        Expr::Call(c) => match &c.args[0] {
            Expr::MatrixSelector(ms) => (ms.range_secs, ms.range_expr.as_deref().cloned()),
            other => panic!("expected a matrix selector, got {other:?}"),
        },
        other => panic!("expected a call, got {other:?}"),
    }
}

fn offset_of(q: &str) -> (f64, Option<DurationExpr>) {
    match must_parse(q) {
        Expr::VectorSelector(vs) => (
            vs.original_offset_secs,
            vs.original_offset_expr.as_deref().cloned(),
        ),
        other => panic!("expected a vector selector, got {other:?}"),
    }
}

#[test]
fn arithmetic_in_a_range_is_kept_as_an_expression() {
    let (secs, expr) = range_of("changes(x[26m+4m])");
    assert_eq!(secs, 0.0);
    assert_eq!(expr.expect("a DurationExpr").op, ItemType::Add);
}

#[test]
fn a_plain_range_stays_a_literal() {
    assert_eq!(range_of("changes(x[30m])"), (1800.0, None));
    // A parenthesised literal is still a literal.
    assert_eq!(range_of("changes(x[(30m)])"), (1800.0, None));
}

#[test]
fn step_and_range_calls_parse_in_brackets_and_offsets() {
    let (_, expr) = range_of("count_over_time(x[step()+1])");
    assert_eq!(expr.unwrap().op, ItemType::Add);
    let (_, expr) = range_of("count_over_time(x[1+(STep()-5)*2])");
    assert_eq!(expr.unwrap().op, ItemType::Add);
    let (secs, expr) = offset_of("x offset range()");
    assert_eq!(secs, 0.0);
    assert_eq!(expr.unwrap().op, ItemType::Range);
}

#[test]
fn min_and_max_take_two_durations() {
    let (_, expr) = range_of("count_over_time(x[max(min(step()+1,1h),1ms)])");
    assert_eq!(expr.unwrap().op, ItemType::Max);
}

/// `foo offset -2 ^ 2` is `(foo offset -2) ^ 2`, `foo offset (-2 ^ 2)` is
/// an offset of -4: the reason `offset_duration_expr` exists upstream.
#[test]
fn offset_binds_a_bare_literal_tighter_than_an_operator() {
    match must_parse("x offset -2 ^ 2") {
        Expr::Binary(b) => assert_eq!(b.op, ItemType::Pow),
        other => panic!("expected a binary expression, got {other:?}"),
    }
    let (secs, expr) = offset_of("x offset -4");
    assert_eq!((secs, expr), (-4.0, None));
    // Unary minus binds looser than `^`: the offset is -(2 ^ 2).
    let (_, expr) = offset_of("x offset (-2 ^ 2)");
    let d = expr.unwrap();
    assert_eq!(d.op, ItemType::Sub);
    assert!(d.lhs.is_none());
    match d.rhs.as_deref() {
        Some(Expr::Duration(inner)) => assert_eq!(inner.op, ItemType::Pow),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_sign_before_a_call_wraps_the_call() {
    let (_, expr) = offset_of("x offset -step()");
    let d = expr.unwrap();
    assert_eq!(d.op, ItemType::Sub);
    assert!(d.lhs.is_none());
    let (_, expr) = offset_of("x offset -min(step(), 1s)");
    assert_eq!(expr.unwrap().op, ItemType::Sub);
}

#[test]
fn at_takes_a_duration_as_seconds() {
    for (q, ms) in [("x @ 100s", 100_000), ("x @ 1m40s", 100_000), ("x @ -1m", -60_000)] {
        match must_parse(q) {
            Expr::VectorSelector(vs) => assert_eq!(vs.timestamp, Some(ms), "{q}"),
            other => panic!("{q}: {other:?}"),
        }
    }
}

#[test]
fn a_literal_range_must_be_positive() {
    for q in ["x[0s]", "x[-5m]", "x[5m:0s]", "x[0:1m]"] {
        assert!(parse_expr(q).is_err(), "{q} should not parse");
    }
}

#[test]
fn division_and_modulo_by_a_literal_zero_are_errors() {
    for q in ["x[5m/0]", "x[5m%0]", "x offset (5/0)"] {
        assert!(parse_expr(q).is_err(), "{q} should not parse");
    }
}

#[test]
fn a_literal_past_the_longest_duration_is_an_error() {
    assert!(parse_expr("x[1e11]").is_err());
    assert!(parse_expr("x offset 1e11").is_err());
}

#[test]
fn subquery_range_and_step_take_expressions() {
    match must_parse("x[29s+1s:5s+5s]") {
        Expr::Subquery(sq) => {
            assert_eq!(sq.range_expr.unwrap().op, ItemType::Add);
            assert_eq!(sq.step_expr.unwrap().op, ItemType::Add);
        }
        other => panic!("{other:?}"),
    }
}

/// The binary-expression grammar still owns an expression that is not in
/// a duration position.
#[test]
fn outside_brackets_arithmetic_is_still_a_binary_expression() {
    assert!(matches!(must_parse("1 + 2"), Expr::Binary(_)));
    assert!(matches!(must_parse("x offset 100 + 2"), Expr::Binary(_)));
}
