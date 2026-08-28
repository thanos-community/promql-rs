//! End-to-end smoke tests. Exercise a handful of representative PromQL
//! expressions through the full parser pipeline (lrlex lexer + grmtools
//! parser + action helpers) and assert on the resulting AST.

use promql_parser::ast::{Expr, MatchOp};
use promql_parser::{parse_expr, parse_metric_selector};

fn must_parse(q: &str) -> Expr {
    parse_expr(q).unwrap_or_else(|e| panic!("failed to parse {q:?}: {e}"))
}

#[test]
fn bare_vector_selector() {
    match must_parse("up") {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.name, "up");
            assert!(vs.label_matchers.is_empty());
        }
        other => panic!("expected VectorSelector, got {other:?}"),
    }
}

#[test]
fn vector_selector_with_matchers() {
    match must_parse(r#"http_requests_total{job="api", status!~"5.."}"#) {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.name, "http_requests_total");
            assert_eq!(vs.label_matchers.len(), 2);
            assert_eq!(vs.label_matchers[0].name, "job");
            assert_eq!(vs.label_matchers[0].op, MatchOp::Equal);
            assert_eq!(vs.label_matchers[0].value, "api");
            assert_eq!(vs.label_matchers[1].name, "status");
            assert_eq!(vs.label_matchers[1].op, MatchOp::RegexNotEqual);
            assert_eq!(vs.label_matchers[1].value, "5..");
        }
        other => panic!("expected VectorSelector, got {other:?}"),
    }
}

#[test]
fn arithmetic_precedence() {
    // 2 + 3 * 4 parses as 2 + (3 * 4)
    match must_parse("2 + 3 * 4") {
        Expr::Binary(b) => {
            assert_eq!(b.op, promql_parser::token::ItemType::Add);
            match *b.rhs {
                Expr::Binary(ref inner) => {
                    assert_eq!(inner.op, promql_parser::token::ItemType::Mul);
                }
                other => panic!("expected nested Mul, got {other:?}"),
            }
        }
        other => panic!("expected Binary, got {other:?}"),
    }
}

#[test]
fn right_associative_pow() {
    // 2 ^ 3 ^ 4 parses as 2 ^ (3 ^ 4)
    match must_parse("2 ^ 3 ^ 4") {
        Expr::Binary(b) => {
            assert_eq!(b.op, promql_parser::token::ItemType::Pow);
            match *b.rhs {
                Expr::Binary(ref inner) => {
                    assert_eq!(inner.op, promql_parser::token::ItemType::Pow);
                }
                other => panic!("expected nested Pow, got {other:?}"),
            }
        }
        other => panic!("expected Binary, got {other:?}"),
    }
}

#[test]
fn rate_over_matrix() {
    match must_parse("rate(http_requests_total[5m])") {
        Expr::Call(c) => {
            assert_eq!(c.func.name, "rate");
            assert_eq!(c.args.len(), 1);
            match &c.args[0] {
                Expr::MatrixSelector(ms) => {
                    assert_eq!(ms.range_secs, 300.0);
                    assert!(matches!(&*ms.vector_selector, Expr::VectorSelector(_)));
                }
                other => panic!("expected MatrixSelector, got {other:?}"),
            }
        }
        other => panic!("expected Call, got {other:?}"),
    }
}

#[test]
fn aggregate_by() {
    match must_parse(r#"sum by (service) (rate(http_requests_total[5m]))"#) {
        Expr::Aggregate(a) => {
            assert_eq!(a.op, promql_parser::token::ItemType::Sum);
            assert_eq!(a.grouping, vec!["service"]);
            assert!(!a.without);
        }
        other => panic!("expected Aggregate, got {other:?}"),
    }
}

#[test]
fn aggregate_without() {
    match must_parse(r#"max without (instance) (cpu_usage)"#) {
        Expr::Aggregate(a) => {
            assert_eq!(a.op, promql_parser::token::ItemType::Max);
            assert_eq!(a.grouping, vec!["instance"]);
            assert!(a.without);
        }
        other => panic!("expected Aggregate, got {other:?}"),
    }
}

#[test]
fn offset_modifier() {
    match must_parse(r#"foo offset 5m"#) {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.name, "foo");
            assert_eq!(vs.original_offset_secs, 300.0);
        }
        other => panic!("expected VectorSelector with offset, got {other:?}"),
    }
}

#[test]
fn offset_negative() {
    match must_parse(r#"foo offset -1h"#) {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.original_offset_secs, -3600.0);
        }
        other => panic!("expected VectorSelector, got {other:?}"),
    }
}

#[test]
fn at_modifier_timestamp() {
    match must_parse(r#"foo @ 1700000000"#) {
        Expr::VectorSelector(vs) => {
            assert_eq!(vs.timestamp, Some(1_700_000_000_000));
        }
        other => panic!("expected VectorSelector with @ modifier, got {other:?}"),
    }
}

#[test]
fn subquery_with_step() {
    match must_parse(r#"rate(foo[5m])[30m:1m]"#) {
        Expr::Subquery(sq) => {
            assert_eq!(sq.range_secs, 1800.0);
            assert_eq!(sq.step_secs, 60.0);
        }
        other => panic!("expected Subquery, got {other:?}"),
    }
}

#[test]
fn paren_preserves_structure() {
    match must_parse("(1 + 2) * 3") {
        Expr::Binary(b) => {
            assert_eq!(b.op, promql_parser::token::ItemType::Mul);
            match *b.lhs {
                Expr::Paren(_) => {}
                other => panic!("expected Paren on LHS, got {other:?}"),
            }
        }
        other => panic!("expected Binary, got {other:?}"),
    }
}

#[test]
fn unary_negation_of_literal_collapses() {
    match must_parse("-3.14") {
        Expr::NumberLiteral(n) => assert!((n.val - -3.14).abs() < 1e-12),
        other => panic!("expected NumberLiteral (collapsed), got {other:?}"),
    }
}

#[test]
fn metric_selector_api() {
    let matchers = parse_metric_selector(r#"up{job="api"}"#).expect("parse");
    // Expect 2: job="api" and the synthesised __name__="up".
    assert_eq!(matchers.len(), 2);
    assert!(matchers
        .iter()
        .any(|m| m.name == "__name__" && m.value == "up"));
    assert!(matchers.iter().any(|m| m.name == "job" && m.value == "api"));
}
