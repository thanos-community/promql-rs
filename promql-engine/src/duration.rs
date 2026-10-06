//! Duration expressions, folded to a number before planning.
//!
//! `x[step()*4]`, `x offset (range()/2)` and `x[1m+30s]` parse to a
//! `DurationExpr` because a range or an offset may name the query's own
//! step or range. Nothing about the plan depends on the value until the
//! planner reads it, so the fold happens there, once per selector, and
//! the kernels only ever see milliseconds.
//!
//! Ports upstream's `durationVisitor` (`promql/durations.go` at
//! 83962c35): `evaluateDoubleExpr`, `evaluateDurationExpr` and
//! `calculateDuration`. Upstream mutates the AST before evaluation; the
//! planner already reads each selector's duration at one point, so it
//! asks for the value there instead of rewriting the tree.

use promql_parser::ast::{DurationExpr, Expr};
use promql_parser::token::ItemType;

use crate::error::EngineError;
use crate::plan::RangeQuery;

/// What `step()` and `range()` mean for one query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct DurationCtx {
    step_secs: f64,
    range_secs: f64,
}

impl DurationCtx {
    /// `step()` is the query's step, even when `start == end`: this engine
    /// treats equal bounds as a one-step range query (see the store-hints
    /// test in `plan`), and callers that mean an instant query pass a
    /// nominal step because `Grid` rejects zero. Upstream's instant
    /// queries evaluate durations with a zero step, so `[step()]` there is
    /// "duration must be greater than 0"; here it is the nominal step.
    /// `range()` is `end - start` either way.
    pub(crate) fn new(query: &RangeQuery) -> Self {
        Self {
            step_secs: query.step_ms as f64 / 1000.0,
            range_secs: (query.end_ms - query.start_ms) as f64 / 1000.0,
        }
    }

    /// Upstream's `calculateDuration`, in milliseconds. A range or step
    /// must be above zero; an offset may be negative. Go truncates
    /// `duration*1000` to whole milliseconds, which `as i64` does too, so
    /// `step()/3` of 10s is 3333ms, not 3334.
    pub(crate) fn millis(
        &self,
        e: &DurationExpr,
        allow_negative: bool,
    ) -> Result<i64, EngineError> {
        let secs = self.eval(e)?;
        let pos = e.position_range();
        if secs <= 0.0 && !allow_negative {
            return Err(EngineError::Query(format!(
                "{}:{}: duration must be greater than 0",
                pos.start, pos.end
            )));
        }
        // The negated comparison also catches NaN (`0 ^ -1`-style results
        // are Inf, `inf - inf` is NaN), which Go's `>`/`<` pair lets through
        // to an undefined `time.Duration` conversion.
        let limit = i64::MAX as f64 / 1e9;
        if !(-limit..=limit).contains(&secs) {
            return Err(EngineError::Query(format!(
                "{}:{}: duration out of range",
                pos.start, pos.end
            )));
        }
        Ok((secs * 1000.0) as i64)
    }

    /// Upstream's `evaluateDoubleExpr`.
    fn eval_operand(&self, e: &Expr) -> Result<f64, EngineError> {
        match e {
            Expr::NumberLiteral(nl) => Ok(nl.val),
            Expr::Paren(p) => self.eval_operand(&p.expr),
            Expr::Duration(d) => self.eval(d),
            other => Err(EngineError::Query(format!(
                "unexpected expression type in a duration: {}",
                crate::plan::describe(other)
            ))),
        }
    }

    /// Upstream's `evaluateDurationExpr`. A missing left operand is the
    /// unary form: `-step()` is `SUB` with only a right-hand side.
    fn eval(&self, e: &DurationExpr) -> Result<f64, EngineError> {
        let lhs = e.lhs.as_deref().map(|l| self.eval_operand(l)).transpose()?;
        let rhs = e.rhs.as_deref().map(|r| self.eval_operand(r)).transpose()?;
        let (l, r) = (lhs.unwrap_or(0.0), rhs.unwrap_or(0.0));
        // Not `Expr::position_range`: upstream reports the right operand's
        // own span for a zero divisor.
        let rhs_pos = || {
            e.rhs
                .as_deref()
                .map(|r| r.position_range())
                .unwrap_or_default()
        };
        Ok(match e.op {
            ItemType::Step => self.step_secs,
            ItemType::Range => self.range_secs,
            // Go's `math.Min`/`math.Max` return NaN if either is NaN;
            // `f64::min` would return the other operand.
            ItemType::Min => go_min(l, r),
            ItemType::Max => go_max(l, r),
            ItemType::Add if lhs.is_none() => r,
            ItemType::Add => l + r,
            ItemType::Sub if lhs.is_none() => -r,
            ItemType::Sub => l - r,
            ItemType::Mul => l * r,
            ItemType::Div => {
                if r == 0.0 {
                    let p = rhs_pos();
                    return Err(EngineError::Query(format!(
                        "{}:{}: division by zero",
                        p.start, p.end
                    )));
                }
                l / r
            }
            ItemType::Mod => {
                if r == 0.0 {
                    let p = rhs_pos();
                    return Err(EngineError::Query(format!(
                        "{}:{}: modulo by zero",
                        p.start, p.end
                    )));
                }
                // Go's `math.Mod` keeps the dividend's sign, like `%` on
                // f64 does.
                l % r
            }
            ItemType::Pow => l.powf(r),
            op => {
                return Err(EngineError::Query(format!(
                    "unexpected duration expression operator {op:?}"
                )))
            }
        })
    }
}

fn go_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.min(b)
    }
}

fn go_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::parse_expr;

    /// The `[...]` of `x[<d>]` evaluated under `query`.
    fn range_ms(d: &str, query: &RangeQuery) -> Result<i64, String> {
        let Ok(Expr::MatrixSelector(ms)) = parse_expr(&format!("x[{d}]")) else {
            panic!("{d} should parse as a range");
        };
        let ctx = DurationCtx::new(query);
        match ms.range_expr {
            Some(e) => ctx.millis(&e, false),
            None => Ok((ms.range_secs * 1000.0) as i64),
        }
        .map_err(|e| e.to_string())
    }

    fn offset_ms(d: &str, query: &RangeQuery) -> Result<i64, String> {
        let Ok(Expr::VectorSelector(vs)) = parse_expr(&format!("x offset {d}")) else {
            panic!("{d} should parse as an offset");
        };
        let ctx = DurationCtx::new(query);
        match vs.original_offset_expr {
            Some(e) => ctx.millis(&e, true),
            None => Ok((vs.original_offset_secs * 1000.0) as i64),
        }
        .map_err(|e| e.to_string())
    }

    const RANGE: RangeQuery = RangeQuery {
        start_ms: 50_000,
        end_ms: 60_000,
        step_ms: 5_000,
        lookback_ms: 300_000,
    };

    #[test]
    fn arithmetic_follows_operator_precedence() {
        assert_eq!(range_ms("26m+4m", &RANGE), Ok(1_800_000));
        assert_eq!(range_ms("2m*(10+5)", &RANGE), Ok(1_800_000));
        assert_eq!(range_ms("1h30m % 1h", &RANGE), Ok(1_800_000));
        assert_eq!(range_ms("-5m+35m", &RANGE), Ok(1_800_000));
        assert_eq!(range_ms("1+(2*3)^2", &RANGE), Ok(37_000));
    }

    #[test]
    fn step_and_range_come_from_the_query() {
        assert_eq!(range_ms("step()", &RANGE), Ok(5_000));
        assert_eq!(range_ms("range()", &RANGE), Ok(10_000));
        assert_eq!(range_ms("min(step()+1,1h)", &RANGE), Ok(6_000));
        assert_eq!(range_ms("max(step(),1h)", &RANGE), Ok(3_600_000));
    }

    #[test]
    fn an_instant_query_has_no_range() {
        let instant = RangeQuery::new(50_000, 50_000, 1);
        assert_eq!(offset_ms("range()", &instant), Ok(0));
        assert_eq!(
            range_ms("range()", &instant),
            Err("2:9: duration must be greater than 0".into())
        );
    }

    #[test]
    fn the_sign_binds_to_the_call_in_an_offset() {
        assert_eq!(offset_ms("-step()", &RANGE), Ok(-5_000));
        assert_eq!(offset_ms("(-step()*2)", &RANGE), Ok(-10_000));
        assert_eq!(offset_ms("-min(step(), 1s)", &RANGE), Ok(-1_000));
    }

    #[test]
    fn a_range_must_be_positive_but_an_offset_may_be_negative() {
        assert!(range_ms("step()-5", &RANGE)
            .unwrap_err()
            .ends_with("duration must be greater than 0"));
        assert_eq!(offset_ms("(step()-10)", &RANGE), Ok(-5_000));
    }

    #[test]
    fn a_computed_zero_divisor_is_a_query_error() {
        let err = range_ms("1m/(step()-5)", &RANGE).unwrap_err();
        assert!(err.ends_with("division by zero"), "{err}");
        let err = range_ms("1m%(step()-5)", &RANGE).unwrap_err();
        assert!(err.ends_with("modulo by zero"), "{err}");
    }

    #[test]
    fn a_result_past_the_longest_duration_is_an_error() {
        let err = range_ms("step()*2e9", &RANGE).unwrap_err();
        assert!(err.ends_with("duration out of range"), "{err}");
    }

    /// Go converts `duration*1000` to `time.Duration` by truncation.
    #[test]
    fn sub_millisecond_results_truncate() {
        assert_eq!(range_ms("(step()+0.0019)", &RANGE), Ok(5_001));
        assert_eq!(range_ms("step()/3", &RANGE), Ok(1_666));
    }
}
