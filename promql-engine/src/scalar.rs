//! Scalar-typed expressions, folded to a value per step.
//!
//! A scalar in PromQL is not a constant: `time()` changes with the step.
//! But every scalar-typed expression this engine plans is a function of
//! the step alone — no series reaches it — so the whole of it can be
//! evaluated while planning, one `f64` per step, and needs no kernel and
//! no DataFusion expression of its own.
//!
//! `scalar(v)` is the exception, and the reason [`fold`] returns a
//! result rather than a value: it reads a vector, so it cannot be
//! answered here and is reported as the unsupported feature it is.
//!
//! The arithmetic is upstream's `scalarBinop` (`promql/engine.go:3224`
//! at 83962c35), comparisons included: between two scalars a comparison
//! is 1 or 0. Writing one without `bool` is a query error, raised by
//! [`crate::plan`]'s tree check before any of this runs, so a
//! comparison that reaches here always had the modifier.

use promql_parser::ast::{Call, Expr};
use promql_parser::token::ItemType;

use crate::error::EngineError;
use crate::plan::{describe, RangeQuery};

/// The value a scalar-typed expression takes at one step.
///
/// `ts_ms` is the step's timestamp, which only `time()` reads; `query`
/// is what `start()`, `end()`, `step()` and `range()` read.
pub fn fold(expr: &Expr, ts_ms: i64, query: &RangeQuery) -> Result<f64, EngineError> {
    match expr {
        Expr::NumberLiteral(n) => Ok(n.val),
        Expr::Paren(p) => fold(&p.expr, ts_ms, query),
        Expr::StepInvariant(e) => fold(e, ts_ms, query),
        Expr::Unary(u) => {
            let v = fold(&u.expr, ts_ms, query)?;
            match u.op {
                ItemType::Sub => Ok(-v),
                ItemType::Add => Ok(v),
                op => Err(EngineError::Unsupported(format!(
                    "the unary {op} operator on a scalar"
                ))),
            }
        }
        Expr::Binary(b) => {
            let (lhs, rhs) = (fold(&b.lhs, ts_ms, query)?, fold(&b.rhs, ts_ms, query)?);
            binop(b.op, lhs, rhs)
        }
        Expr::Call(c) => call(c, ts_ms, query),
        // A duration expression is scalar-typed but its value comes from
        // the query's own step and range, which this fold does not see.
        other => Err(EngineError::Unsupported(describe(other))),
    }
}

/// A scalar-returning call at one step. Its own entry point because the
/// planner meets such a call as a `Call`, not as the `Expr` around it.
pub(crate) fn call(c: &Call, ts_ms: i64, query: &RangeQuery) -> Result<f64, EngineError> {
    // Before the name is matched, not after: `time(1)` takes no
    // arguments upstream and must say so rather than quietly answering
    // the time.
    crate::function::check_call(c)?;
    match c.func.name.as_str() {
        "time" => Ok(time(ts_ms)),
        "pi" => Ok(std::f64::consts::PI),
        "start" | "end" | "step" | "range" => Ok(query_context(&c.func.name, query)),
        name => Err(EngineError::Unsupported(format!("the {name} function"))),
    }
}

/// Upstream's `foldQueryContextFunctions` (`promql/engine.go`, 83962c35),
/// which rewrites `start()`, `end()`, `range()` and `step()` into number
/// literals before evaluation: `funcQueryContext` panics if one reaches
/// the evaluator. The values are seconds, and `step()` is 0 when start
/// equals end, which is how upstream spells an instant query; the
/// conformance harness passes an instant as a one-millisecond step, so
/// reading `step_ms` alone would answer 0.001.
fn query_context(name: &str, query: &RangeQuery) -> f64 {
    match name {
        "start" => query.start_ms as f64 / 1000.0,
        "end" => query.end_ms as f64 / 1000.0,
        // `end.Sub(start).Seconds()`: the difference in integer
        // nanoseconds, then a float, so the subtraction is exact.
        "range" => (query.end_ms - query.start_ms) as f64 / 1000.0,
        _ if query.start_ms == query.end_ms => 0.0,
        _ => query.step_ms as f64 / 1000.0,
    }
}

/// Upstream's `funcTime`: the step in seconds, as a float.
pub(crate) fn time(ts_ms: i64) -> f64 {
    ts_ms as f64 / 1000.0
}

/// Upstream's `scalarBinop`, which is [`crate::binary::Op`] with the
/// `bool` its comparisons always carry: between two scalars the modifier
/// is mandatory, checked by [`crate::plan`] before any of this runs, so
/// a comparison here is Go's `btos` and never filters.
///
/// Shared rather than written out again: `scalarBinop` and
/// `vectorElemBinop` agree operator for operator upstream, and one of
/// them here would drift.
fn binop(op: ItemType, lhs: f64, rhs: f64) -> Result<f64, EngineError> {
    match crate::binary::Op::from_token(op) {
        Some(op) => Ok(op
            .value(lhs, rhs, true)
            .expect("bool keeps every comparison")),
        // `and`, `or`, `unless`: upstream's parser rejects them between
        // scalars, so reaching here means our parser was laxer.
        None => Err(EngineError::Query(format!(
            "set operator {op} not allowed in binary scalar expression"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(query: &str, ts_ms: i64) -> f64 {
        within(query, ts_ms, &RangeQuery::new(0, 0, 1))
    }

    fn within(query: &str, ts_ms: i64, range: &RangeQuery) -> f64 {
        fold(
            &promql_parser::parse_expr(query).expect("the query parses"),
            ts_ms,
            range,
        )
        .expect("the expression folds")
    }

    fn value(query: &str) -> f64 {
        at(query, 0)
    }

    /// The corpus's `literals.test`, which is what this exists for.
    #[test]
    fn the_literal_corpus_folds_to_its_expected_values() {
        assert_eq!(value("12.34e6"), 12_340_000.0);
        assert_eq!(value("12.34e-6"), 0.00001234);
        assert_eq!(value("1+1"), 2.0);
        assert_eq!(value("1-1"), 0.0);
        assert_eq!(value("1 - -1"), 2.0);
        assert_eq!(value(".2"), 0.2);
        assert_eq!(value("+0.2"), 0.2);
        assert_eq!(value("-0.2e-6"), -0.0000002);
        assert_eq!(value("+Inf"), f64::INFINITY);
        assert_eq!(value("-Inf"), f64::NEG_INFINITY);
        assert!(value("NaN").is_nan());
        assert_eq!(value("1 / 0"), f64::INFINITY);
        assert_eq!(value("((1) / (0))"), f64::INFINITY);
        assert_eq!(value("-1 / 0"), f64::NEG_INFINITY);
        assert!(value("0 / 0").is_nan());
        assert!(value("1 % 0").is_nan());
        assert_eq!(value("2."), 2.0);
    }

    /// Every arm of upstream's `scalarBinop`, including the sign rule
    /// `%` inherits from Go's `math.Mod`.
    #[test]
    fn the_arithmetic_is_upstreams_scalar_binop() {
        assert_eq!(value("3 * 4"), 12.0);
        assert_eq!(value("2 ^ 10"), 1024.0);
        assert_eq!(value("7 % 3"), 1.0);
        assert_eq!(value("-7 % 3"), -1.0);
        assert_eq!(value("7 % -3"), 1.0);
        // `atan2` is an operator, not a function, upstream and here.
        assert_eq!(value("1 atan2 1"), std::f64::consts::FRAC_PI_4);
    }

    /// `foldQueryContextFunctions`: seconds, from the query's own bounds
    /// and not from the step being evaluated, which is what separates
    /// them from `time()`.
    #[test]
    fn the_query_context_functions_read_the_query_not_the_step() {
        let range = RangeQuery::new(10_000, 50_000, 10_000);
        for (query, want) in [
            ("start()", 10.0),
            ("end()", 50.0),
            ("step()", 10.0),
            ("range()", 40.0),
            ("end() - start() == bool range()", 1.0),
        ] {
            // The value is the same at every step.
            for ts in [10_000, 30_000, 50_000] {
                assert_eq!(within(query, ts, &range), want, "{query} at {ts}");
            }
        }
        assert_eq!(
            within("start() + 0.5", 0, &RangeQuery::new(1_500, 9_000, 500)),
            2.0
        );
    }

    /// Upstream spells an instant query as `start == end` and answers 0
    /// for `step()` there, whatever step the caller passed (the
    /// conformance harness passes 1ms).
    #[test]
    fn step_is_zero_when_start_equals_end() {
        for step_ms in [1, 5_000] {
            let range = RangeQuery::new(100_000, 100_000, step_ms);
            assert_eq!(within("step()", 100_000, &range), 0.0);
            assert_eq!(within("range()", 100_000, &range), 0.0);
            assert_eq!(within("start()", 100_000, &range), 100.0);
            assert_eq!(within("end()", 100_000, &range), 100.0);
        }
    }

    #[test]
    fn a_comparison_between_scalars_is_one_or_zero() {
        for (query, want) in [
            ("1 == bool 1", 1.0),
            ("1 == bool 2", 0.0),
            ("1 != bool 2", 1.0),
            ("2 > bool 1", 1.0),
            ("1 > bool 2", 0.0),
            ("1 < bool 2", 1.0),
            ("1 >= bool 1", 1.0),
            ("1 <= bool 0", 0.0),
        ] {
            assert_eq!(value(query), want, "{query}");
        }
    }

    /// The one scalar that is not constant.
    #[test]
    fn time_is_the_step_in_seconds() {
        assert_eq!(at("time()", 0), 0.0);
        assert_eq!(at("time()", 60_000), 60.0);
        assert_eq!(at("time() - 30", 60_000), 30.0);
        assert_eq!(at("pi()", 12_345), std::f64::consts::PI);
    }

    /// A call the fold knows the name of is still held to its
    /// signature: `time()` takes nothing, and an argument makes the
    /// query wrong rather than the time different.
    #[test]
    fn a_call_is_checked_before_its_name_is_matched() {
        let err = fold(
            &promql_parser::parse_expr("time(1)").unwrap(),
            0,
            &RangeQuery::new(0, 0, 1),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "expected 0 argument(s) in call to \"time\", got 1"
        );
        assert!(matches!(err, EngineError::Query(_)), "{err}");

        let err = fold(
            &promql_parser::parse_expr("pi(1)").unwrap(),
            0,
            &RangeQuery::new(0, 0, 1),
        )
        .unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
        assert_eq!(at("time()", 60_000), 60.0);
    }

    /// What cannot be folded is named, not guessed at: `scalar(v)`
    /// reads a vector, which this fold never sees.
    #[test]
    fn an_expression_that_needs_data_is_unsupported_by_name() {
        let err = fold(
            &promql_parser::parse_expr("scalar(up)").unwrap(),
            0,
            &RangeQuery::new(0, 0, 1),
        )
        .unwrap_err();
        assert!(
            matches!(&err, EngineError::Unsupported(f) if f == "the scalar function"),
            "{err}"
        );

        let err = fold(
            &promql_parser::parse_expr("scalar(up) + 1").unwrap(),
            0,
            &RangeQuery::new(0, 0, 1),
        )
        .unwrap_err();
        assert!(matches!(err, EngineError::Unsupported(_)), "{err}");
    }
}
