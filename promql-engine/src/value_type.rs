//! What an expression evaluates to, decided before it is evaluated.
//!
//! Prometheus's `parser.Expr.Type()`. An instant query needs it because
//! the evaluator always produces a matrix and the *expression's* type is
//! what says whether that matrix is really a vector, a scalar, a string
//! or a matrix — `execEvalStmt` switches on it (`promql/engine.go:828`
//! at 83962c35). Nothing in the result can be inspected instead: a
//! scalar comes back as one series with no labels and one point, exactly
//! like `sum(x)`.
//!
//! [`promql_parser::ast::Expr::value_type`] is the same walk, but its
//! `Call` arm reads `FunctionRef::return_type`, which the parser fills in
//! as `Vector` for every function — it carries no function registry yet.
//! So the table below is this crate's stand-in for `functions.go`, and
//! when the parser grows the real registry this module collapses into a
//! call to the parser's own walk.

use promql_parser::ast::Expr;

/// Prometheus's `parser.ValueType`, minus `none`: that one classifies
/// statements, and an expression never has it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Scalar,
    String,
    Vector,
    Matrix,
}

impl ValueType {
    /// Upstream's `parser.DocumentedType`, the spelling used in error
    /// messages.
    pub fn as_str(self) -> &'static str {
        match self {
            ValueType::Scalar => "scalar",
            ValueType::String => "string",
            ValueType::Vector => "instant vector",
            ValueType::Matrix => "range vector",
        }
    }
}

/// The type `expr` evaluates to.
pub fn value_type(expr: &Expr) -> ValueType {
    match expr {
        Expr::Aggregate(_) | Expr::VectorSelector(_) => ValueType::Vector,
        Expr::Binary(b) => {
            // Upstream's rule verbatim: two scalars make a scalar, any
            // vector operand makes the result a vector.
            match (value_type(&b.lhs), value_type(&b.rhs)) {
                (ValueType::Scalar, ValueType::Scalar) => ValueType::Scalar,
                _ => ValueType::Vector,
            }
        }
        Expr::Call(c) => function_return_type(&c.func.name),
        Expr::MatrixSelector(_) | Expr::Subquery(_) => ValueType::Matrix,
        Expr::NumberLiteral(_) | Expr::Duration(_) => ValueType::Scalar,
        Expr::StringLiteral(_) => ValueType::String,
        Expr::Paren(p) => value_type(&p.expr),
        Expr::Unary(u) => value_type(&u.expr),
        Expr::StepInvariant(e) => value_type(e),
    }
}

/// The `ReturnType` column of [`crate::function`]'s transcription of
/// `functions.go`. A name that table does not know is a vector here
/// rather than an error, because this walk runs on expressions
/// [`crate::function::check_call`] has not reached yet — and it is the
/// only type an unknown name could sensibly be.
fn function_return_type(name: &str) -> ValueType {
    crate::function::signature(name).map_or(ValueType::Vector, |s| s.return_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn type_of(query: &str) -> ValueType {
        value_type(&promql_parser::parse_expr(query).expect("the query parses"))
    }

    #[test]
    fn every_kind_of_expression_has_its_upstream_type() {
        assert_eq!(type_of("up"), ValueType::Vector);
        assert_eq!(type_of(r#"{job="api"}"#), ValueType::Vector);
        assert_eq!(type_of("sum by (pod) (up)"), ValueType::Vector);
        assert_eq!(type_of("up[5m]"), ValueType::Matrix);
        assert_eq!(type_of("up[5m:1m]"), ValueType::Matrix);
        assert_eq!(type_of("42"), ValueType::Scalar);
        assert_eq!(type_of(r#""hello""#), ValueType::String);
        assert_eq!(type_of("(up)"), ValueType::Vector);
        assert_eq!(type_of("(42)"), ValueType::Scalar);
        assert_eq!(type_of("-up"), ValueType::Vector);
        assert_eq!(type_of("-42"), ValueType::Scalar);
    }

    #[test]
    fn a_binary_operator_is_scalar_only_when_both_sides_are() {
        assert_eq!(type_of("1 + 1"), ValueType::Scalar);
        assert_eq!(type_of("1 + up"), ValueType::Vector);
        assert_eq!(type_of("up + 1"), ValueType::Vector);
        assert_eq!(type_of("up + up"), ValueType::Vector);
        assert_eq!(type_of("(1 + 1) * 2"), ValueType::Scalar);
        assert_eq!(type_of("(1 + up) * 2"), ValueType::Vector);
        assert_eq!(type_of("time() - 1"), ValueType::Scalar);
        assert_eq!(type_of("scalar(up) > 1"), ValueType::Scalar);
    }

    /// The table is the part that can silently drift from upstream, so
    /// it is enumerated rather than spot-checked: every scalar-returning
    /// function in `functions.go` at 83962c35, and a sample of the
    /// vector-returning ones that look like they might not be.
    ///
    /// `start`, `end`, `range` and `step` are the experimental duration
    /// functions; they count because the corpus parses with experimental
    /// functions enabled.
    #[test]
    fn exactly_the_scalar_returning_functions_are_scalar() {
        for name in ["end", "pi", "range", "scalar", "start", "step", "time"] {
            assert_eq!(
                function_return_type(name),
                ValueType::Scalar,
                "{name} returns a scalar upstream"
            );
        }
        for name in [
            "day_of_month",
            "day_of_week",
            "day_of_year",
            "days_in_month",
            "hour",
            "minute",
            "month",
            "year",
            "timestamp",
            "vector",
            "rate",
            "histogram_quantile",
            "label_replace",
            "label_join",
            "absent",
            "absent_over_time",
            "sort",
            "abs",
            // A function this engine has never heard of is a vector too,
            // which is what upstream's zero value for an unknown name
            // could never be: the parser rejects it long before here.
            "no_such_function",
        ] {
            assert_eq!(
                function_return_type(name),
                ValueType::Vector,
                "{name} returns a vector upstream"
            );
        }
    }

    #[test]
    fn a_function_call_takes_the_functions_type_not_its_arguments() {
        assert_eq!(type_of("rate(up[5m])"), ValueType::Vector);
        assert_eq!(type_of("scalar(up)"), ValueType::Scalar);
        assert_eq!(type_of("vector(1)"), ValueType::Vector);
        assert_eq!(type_of("time()"), ValueType::Scalar);
        assert_eq!(type_of("pi()"), ValueType::Scalar);
        assert_eq!(type_of("day_of_month()"), ValueType::Vector);
        assert_eq!(type_of("timestamp(up)"), ValueType::Vector);
    }

    /// `@` wraps its operand in a step-invariant node upstream; whether
    /// our parser produces one or not, the type is the operand's.
    #[test]
    fn a_step_invariant_expression_has_its_operands_type() {
        assert_eq!(type_of("up @ 100"), ValueType::Vector);
        assert_eq!(type_of("up[5m] @ 100"), ValueType::Matrix);
        assert_eq!(type_of("rate(up[5m] @ 100)"), ValueType::Vector);
    }

    #[test]
    fn the_documented_spelling_is_upstreams() {
        assert_eq!(ValueType::Vector.as_str(), "instant vector");
        assert_eq!(ValueType::Matrix.as_str(), "range vector");
        assert_eq!(ValueType::Scalar.as_str(), "scalar");
        assert_eq!(ValueType::String.as_str(), "string");
    }
}
