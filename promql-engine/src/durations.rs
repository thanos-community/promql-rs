//! Folding a parsed duration expression into milliseconds.
//!
//! Mirrors Prometheus's `promql/durations.go` at 83962c35. The parser
//! builds the tree but cannot finish it: `step()` and `range()` are
//! properties of the query, so `foo[step()+1s]` has no value until a
//! query is in hand. Everything else could have been folded earlier;
//! keeping one evaluator for both halves is what makes the folded
//! result identical whichever operands appear.

use promql_parser::ast::{DurationExpr, Expr};
use promql_parser::token::ItemType;

use crate::error::EngineError;
use crate::plan::RangeQuery;

/// Fold `expr` to milliseconds. `allow_negative` is upstream's flag: an
/// offset may run backwards, a range or a step may not.
pub fn duration_ms(
    expr: &DurationExpr,
    query: &RangeQuery,
    allow_negative: bool,
) -> Result<i64, EngineError> {
    let secs = eval(expr, query)?;
    if secs <= 0.0 && !allow_negative {
        return Err(EngineError::Query(format!(
            "{}:{}: duration must be greater than 0",
            expr.start_pos, expr.end_pos
        )));
    }
    // Upstream compares the seconds count against the i64 nanosecond
    // range (`duration > 1<<63-1 || duration < -1<<63`) rather than
    // against the nanoseconds it is about to produce. Mirrored as
    // written: the looser bound is what decides which queries are
    // rejected, and the cast below saturates rather than wrapping.
    if secs > i64::MAX as f64 || secs < i64::MIN as f64 {
        return Err(EngineError::Query(format!(
            "{}:{}: duration is out of range",
            expr.start_pos, expr.end_pos
        )));
    }
    // Upstream truncates to whole milliseconds here, and the corpus
    // depends on it: `[step()/10]` over a 5s step is 500ms exactly.
    Ok((secs * 1000.0) as i64)
}

fn eval(expr: &DurationExpr, query: &RangeQuery) -> Result<f64, EngineError> {
    let lhs = match &expr.lhs {
        Some(e) => Some(operand(e, query)?),
        None => None,
    };
    let rhs = match &expr.rhs {
        Some(e) => Some(operand(e, query)?),
        None => None,
    };
    let both = |name: &str| -> Result<(f64, f64), EngineError> {
        match (lhs, rhs) {
            (Some(l), Some(r)) => Ok((l, r)),
            _ => Err(EngineError::Query(format!(
                "duration expression operator {name} wants two operands"
            ))),
        }
    };
    match expr.op {
        ItemType::Step => Ok(step_ms(query) as f64 / 1000.0),
        ItemType::Range => Ok((query.end_ms - query.start_ms) as f64 / 1000.0),
        ItemType::Min => both("min").map(|(l, r)| l.min(r)),
        ItemType::Max => both("max").map(|(l, r)| l.max(r)),
        // A missing left-hand side is how the parser spells a unary
        // sign, following upstream's DurationExpr.
        ItemType::Add => Ok(lhs.unwrap_or(0.0) + rhs.unwrap_or(0.0)),
        ItemType::Sub => match lhs {
            Some(l) => Ok(l - rhs.unwrap_or(0.0)),
            None => Ok(-rhs.unwrap_or(0.0)),
        },
        ItemType::Mul => both("*").map(|(l, r)| l * r),
        ItemType::Div => {
            let (l, r) = both("/")?;
            if r == 0.0 {
                return Err(EngineError::Query(format!(
                    "{}:{}: division by zero",
                    expr.start_pos, expr.end_pos
                )));
            }
            Ok(l / r)
        }
        ItemType::Mod => {
            let (l, r) = both("%")?;
            if r == 0.0 {
                return Err(EngineError::Query(format!(
                    "{}:{}: modulo by zero",
                    expr.start_pos, expr.end_pos
                )));
            }
            Ok(l % r)
        }
        ItemType::Pow => both("^").map(|(l, r)| l.powf(r)),
        other => Err(EngineError::Query(format!(
            "unexpected duration expression operator {other:?}"
        ))),
    }
}

/// What `step()` answers. An instant query has no step: upstream's
/// `EvalStmt.Interval` is 0 there, so `foo[step()]` fails the
/// greater-than-zero check. The 1ms step the evaluator runs an instant
/// query with (`engine.go:796`) is an internal detail of executing it
/// as a one-step range, and must not leak into the expression.
fn step_ms(query: &RangeQuery) -> i64 {
    if query.start_ms == query.end_ms {
        return 0;
    }
    query.step_ms
}

fn operand(e: &Expr, query: &RangeQuery) -> Result<f64, EngineError> {
    match e {
        Expr::NumberLiteral(nl) => Ok(nl.val),
        Expr::Duration(d) => eval(d, query),
        other => Err(EngineError::Query(format!(
            "unexpected duration expression operand {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use promql_parser::ast::NumberLiteral;
    use promql_parser::posrange::PositionRange;

    use super::*;

    fn lit(val: f64) -> Expr {
        Expr::NumberLiteral(NumberLiteral {
            val,
            duration: false,
            pos_range: PositionRange::default(),
        })
    }

    fn node(op: ItemType, lhs: Option<Expr>, rhs: Option<Expr>) -> DurationExpr {
        DurationExpr {
            op,
            lhs: lhs.map(Box::new),
            rhs: rhs.map(Box::new),
            wrapped: false,
            start_pos: 0,
            end_pos: 1,
        }
    }

    #[test]
    fn a_product_wider_than_an_i64_of_nanoseconds_is_out_of_range() {
        let e = node(ItemType::Mul, Some(lit(4e9)), Some(lit(4e9)));
        let err = duration_ms(&e, &RangeQuery::new(0, 10_000, 1_000), false).unwrap_err();
        assert!(
            err.to_string().contains("duration is out of range"),
            "{err}"
        );
    }

    #[test]
    fn step_is_zero_on_an_instant_query() {
        let e = node(ItemType::Step, None, None);
        // The harness runs an instant query with a 1ms step; step()
        // must not see it.
        let instant = RangeQuery::new(50_000, 50_000, 1);
        let err = duration_ms(&e, &instant, false).unwrap_err();
        assert!(
            err.to_string().contains("duration must be greater than 0"),
            "{err}"
        );
        assert_eq!(duration_ms(&e, &instant, true).unwrap(), 0);

        let ranged = RangeQuery::new(50_000, 60_000, 5_000);
        assert_eq!(duration_ms(&e, &ranged, false).unwrap(), 5_000);
    }

    #[test]
    fn range_is_the_query_span() {
        let e = node(ItemType::Range, None, None);
        let q = RangeQuery::new(50_000, 60_000, 5_000);
        assert_eq!(duration_ms(&e, &q, false).unwrap(), 10_000);
    }

    /// The parser only rejects a divisor that is literally zero, so the
    /// folded case has to be caught here.
    #[test]
    fn division_by_a_folded_zero_is_rejected() {
        let zero = Expr::Duration(node(ItemType::Mul, Some(lit(0.0)), Some(lit(3.0))));
        let e = node(ItemType::Div, Some(lit(60.0)), Some(zero));
        let err = duration_ms(&e, &RangeQuery::new(0, 10_000, 1_000), false).unwrap_err();
        assert!(err.to_string().contains("division by zero"), "{err}");
    }
}
