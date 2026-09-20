//! Prometheus's `parser.Functions` table, and the call check that reads
//! it.
//!
//! Our parser fills a stub `FunctionRef` — every call comes out typed
//! `Vector` with no argument types (`actions.rs::function_call`) — so
//! the arity and argument types upstream settles while parsing are
//! settled here instead, in the same words, before anything is planned.
//! The table is transcribed from `promql/parser/functions.go` at
//! 83962c35 and covers every function upstream knows, not only the ones
//! this engine can plan: an argument this engine would reject for being
//! the wrong shape is a query error either way, and saying so with
//! upstream's message keeps a harness comparing error text from
//! diverging on wording.

use promql_parser::ast::Call;

use crate::error::EngineError;
use crate::value_type::{value_type, ValueType as T};

/// One row of upstream's table.
pub struct Signature {
    pub arg_types: &'static [T],
    /// Upstream `Variadic`: 0 for a fixed arity, `n > 0` for up to `n`
    /// extra arguments of the last type, `-1` for unboundedly many.
    pub variadic: i32,
    pub return_type: T,
}

/// The signature of the function `name`, or `None` for a name upstream
/// does not know — which upstream rejects while parsing and this engine
/// rejects in [`check_call`].
pub fn signature(name: &str) -> Option<Signature> {
    let (arg_types, variadic, return_type): (&'static [T], i32, T) = match name {
        "abs" => (&[T::Vector], 0, T::Vector),
        "absent" => (&[T::Vector], 0, T::Vector),
        "absent_over_time" => (&[T::Matrix], 0, T::Vector),
        "acos" => (&[T::Vector], 0, T::Vector),
        "acosh" => (&[T::Vector], 0, T::Vector),
        "asin" => (&[T::Vector], 0, T::Vector),
        "asinh" => (&[T::Vector], 0, T::Vector),
        "atan" => (&[T::Vector], 0, T::Vector),
        "atanh" => (&[T::Vector], 0, T::Vector),
        "avg_over_time" => (&[T::Matrix], 0, T::Vector),
        "ceil" => (&[T::Vector], 0, T::Vector),
        "changes" => (&[T::Matrix], 0, T::Vector),
        "clamp" => (&[T::Vector, T::Scalar, T::Scalar], 0, T::Vector),
        "clamp_max" => (&[T::Vector, T::Scalar], 0, T::Vector),
        "clamp_min" => (&[T::Vector, T::Scalar], 0, T::Vector),
        "cos" => (&[T::Vector], 0, T::Vector),
        "cosh" => (&[T::Vector], 0, T::Vector),
        "count_over_time" => (&[T::Matrix], 0, T::Vector),
        "days_in_month" => (&[T::Vector], 1, T::Vector),
        "day_of_month" => (&[T::Vector], 1, T::Vector),
        "day_of_week" => (&[T::Vector], 1, T::Vector),
        "day_of_year" => (&[T::Vector], 1, T::Vector),
        "deg" => (&[T::Vector], 0, T::Vector),
        "end" => (&[], 0, T::Scalar),
        "delta" => (&[T::Matrix], 0, T::Vector),
        "deriv" => (&[T::Matrix], 0, T::Vector),
        "exp" => (&[T::Vector], 0, T::Vector),
        "first_over_time" => (&[T::Matrix], 0, T::Vector),
        "floor" => (&[T::Vector], 0, T::Vector),
        "histogram_avg" => (&[T::Vector], 0, T::Vector),
        "histogram_count" => (&[T::Vector], 0, T::Vector),
        "histogram_sum" => (&[T::Vector], 0, T::Vector),
        "histogram_stddev" => (&[T::Vector], 0, T::Vector),
        "histogram_stdvar" => (&[T::Vector], 0, T::Vector),
        "histogram_fraction" => (&[T::Scalar, T::Scalar, T::Vector], 0, T::Vector),
        "histogram_quantile" => (&[T::Scalar, T::Vector], 0, T::Vector),
        "histogram_quantiles" => (&[T::Vector, T::String, T::Scalar, T::Scalar], 9, T::Vector),
        "double_exponential_smoothing" => (&[T::Matrix, T::Scalar, T::Scalar], 0, T::Vector),
        "hour" => (&[T::Vector], 1, T::Vector),
        "idelta" => (&[T::Matrix], 0, T::Vector),
        "increase" => (&[T::Matrix], 0, T::Vector),
        "info" => (&[T::Vector, T::Vector], 1, T::Vector),
        "irate" => (&[T::Matrix], 0, T::Vector),
        "label_replace" => (
            &[T::Vector, T::String, T::String, T::String, T::String],
            0,
            T::Vector,
        ),
        "label_join" => (&[T::Vector, T::String, T::String, T::String], -1, T::Vector),
        "last_over_time" => (&[T::Matrix], 0, T::Vector),
        "ln" => (&[T::Vector], 0, T::Vector),
        "log10" => (&[T::Vector], 0, T::Vector),
        "log2" => (&[T::Vector], 0, T::Vector),
        "mad_over_time" => (&[T::Matrix], 0, T::Vector),
        "max_over_time" => (&[T::Matrix], 0, T::Vector),
        "min_over_time" => (&[T::Matrix], 0, T::Vector),
        "ts_of_first_over_time" => (&[T::Matrix], 0, T::Vector),
        "ts_of_max_over_time" => (&[T::Matrix], 0, T::Vector),
        "ts_of_min_over_time" => (&[T::Matrix], 0, T::Vector),
        "ts_of_last_over_time" => (&[T::Matrix], 0, T::Vector),
        "minute" => (&[T::Vector], 1, T::Vector),
        "month" => (&[T::Vector], 1, T::Vector),
        "pi" => (&[], 0, T::Scalar),
        "predict_linear" => (&[T::Matrix, T::Scalar], 0, T::Vector),
        "present_over_time" => (&[T::Matrix], 0, T::Vector),
        "quantile_over_time" => (&[T::Scalar, T::Matrix], 0, T::Vector),
        "rad" => (&[T::Vector], 0, T::Vector),
        "range" => (&[], 0, T::Scalar),
        "rate" => (&[T::Matrix], 0, T::Vector),
        "resets" => (&[T::Matrix], 0, T::Vector),
        "round" => (&[T::Vector, T::Scalar], 1, T::Vector),
        "scalar" => (&[T::Vector], 0, T::Scalar),
        "sgn" => (&[T::Vector], 0, T::Vector),
        "sin" => (&[T::Vector], 0, T::Vector),
        "sinh" => (&[T::Vector], 0, T::Vector),
        "sort" => (&[T::Vector], 0, T::Vector),
        "sort_desc" => (&[T::Vector], 0, T::Vector),
        "sort_by_label" => (&[T::Vector, T::String], -1, T::Vector),
        "sort_by_label_desc" => (&[T::Vector, T::String], -1, T::Vector),
        "sqrt" => (&[T::Vector], 0, T::Vector),
        "start" => (&[], 0, T::Scalar),
        "step" => (&[], 0, T::Scalar),
        "stddev_over_time" => (&[T::Matrix], 0, T::Vector),
        "stdvar_over_time" => (&[T::Matrix], 0, T::Vector),
        "sum_over_time" => (&[T::Matrix], 0, T::Vector),
        "tan" => (&[T::Vector], 0, T::Vector),
        "tanh" => (&[T::Vector], 0, T::Vector),
        "time" => (&[], 0, T::Scalar),
        "timestamp" => (&[T::Vector], 0, T::Vector),
        "vector" => (&[T::Scalar], 0, T::Vector),
        "year" => (&[T::Vector], 1, T::Vector),
        _ => return None,
    };
    Some(Signature {
        arg_types,
        variadic,
        return_type,
    })
}

/// Upstream's `checkAST` for a `Call` (`promql/parser/parse.go:830-873`
/// at 83962c35): the arity first, then every argument's type, the
/// variadic tail all held to the last declared type.
pub fn check_call(call: &Call) -> Result<(), EngineError> {
    let name = call.func.name.as_str();
    let Some(sig) = signature(name) else {
        // PromQL's function names are case-insensitive, and the table
        // holds them as upstream spells them. A sibling parser branch
        // resolves the spelling; until it lands, `ABS(x)` is a function
        // this engine has not reached rather than one nobody has heard
        // of, and the two deserve different answers.
        if signature(&name.to_lowercase()).is_some() {
            return Err(EngineError::Unsupported(format!("the {name} function")));
        }
        return Err(EngineError::Query(format!(
            "unknown function with name {name:?}"
        )));
    };

    let declared = sig.arg_types.len();
    let given = call.args.len();
    if sig.variadic == 0 {
        if declared != given {
            return Err(arity("", declared, name, given));
        }
    } else {
        let least = declared - 1;
        if least > given {
            return Err(arity("at least ", least, name, given));
        }
        // A negative `Variadic` is upstream's "as many as you like", so
        // there is no upper bound to compute.
        if sig.variadic > 0 {
            let most = least + sig.variadic as usize;
            if most < given {
                return Err(arity("at most ", most, name, given));
            }
        }
    }

    for (i, arg) in call.args.iter().enumerate() {
        // Past the declared types, a variadic tail repeats the last one;
        // a fixed-arity call cannot get here at all.
        let want = sig.arg_types[i.min(declared - 1)];
        let got = value_type(arg);
        if got != want {
            return Err(EngineError::Query(format!(
                "expected type {} in call to function {name:?}, got {}",
                want.as_str(),
                got.as_str()
            )));
        }
    }
    Ok(())
}

fn arity(qualifier: &str, want: usize, name: &str, got: usize) -> EngineError {
    EngineError::Query(format!(
        "expected {qualifier}{want} argument(s) in call to {name:?}, got {got}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(query: &str) -> Result<(), EngineError> {
        let expr = promql_parser::parse_expr(query).expect("query parses");
        match expr {
            promql_parser::ast::Expr::Call(c) => check_call(&c),
            other => panic!("{query} is {other:?}, not a call"),
        }
    }

    /// Upstream's messages, verbatim: the harness compares error text.
    #[test]
    fn the_arity_is_upstreams_and_so_is_its_complaint() {
        assert!(check("time()").is_ok());
        assert!(check("abs(up)").is_ok());
        assert!(check("round(up)").is_ok());
        assert!(check("round(up, 5)").is_ok());
        assert!(check("clamp(up, 0, 1)").is_ok());
        // A variadic tail with no upper bound.
        assert!(check("label_join(up, \"a\", \",\", \"b\", \"c\")").is_ok());

        assert_eq!(
            check("abs(up, 1)").unwrap_err().to_string(),
            "expected 1 argument(s) in call to \"abs\", got 2"
        );
        assert_eq!(
            check("round(up, 1, 2)").unwrap_err().to_string(),
            "expected at most 2 argument(s) in call to \"round\", got 3"
        );
        assert_eq!(
            check("clamp(up, 0)").unwrap_err().to_string(),
            "expected 3 argument(s) in call to \"clamp\", got 2"
        );
        assert_eq!(
            check("label_replace(up, \"a\", \"b\", \"c\")")
                .unwrap_err()
                .to_string(),
            "expected 5 argument(s) in call to \"label_replace\", got 4"
        );
    }

    #[test]
    fn an_argument_of_the_wrong_type_is_named_by_the_type_it_should_be() {
        assert_eq!(
            check("abs(up[5m])").unwrap_err().to_string(),
            "expected type instant vector in call to function \"abs\", got range vector"
        );
        assert_eq!(
            check("rate(up)").unwrap_err().to_string(),
            "expected type range vector in call to function \"rate\", got instant vector"
        );
        assert_eq!(
            check("vector(up)").unwrap_err().to_string(),
            "expected type scalar in call to function \"vector\", got instant vector"
        );
        assert_eq!(
            check("clamp_min(up, \"x\")").unwrap_err().to_string(),
            "expected type scalar in call to function \"clamp_min\", got string"
        );
    }

    #[test]
    fn a_name_upstream_does_not_know_is_not_a_function() {
        assert_eq!(
            check("nosuchfunc(up)").unwrap_err().to_string(),
            "unknown function with name \"nosuchfunc\""
        );
    }

    /// Upstream matches a function name whatever its case, so a
    /// spelling this table misses is still a function — the engine has
    /// not reached it, which is not the same as never having heard of
    /// it. The message keeps the spelling the query used.
    #[test]
    fn a_known_function_under_another_spelling_is_a_gap_not_a_typo() {
        for query in ["ABS(up)", "Rate(up[5m])", "cLaMp_MiN(up, 1)"] {
            let name = query.split('(').next().unwrap();
            let err = check(query).unwrap_err();
            assert!(
                matches!(&err, EngineError::Unsupported(f) if f == &format!("the {name} function")),
                "{query}: {err}"
            );
        }
    }

    /// The table decides the result type, so the two must agree for
    /// every name in it; [`crate::value_type`] reads it for exactly that.
    #[test]
    fn the_table_is_where_the_scalar_returning_functions_come_from() {
        for name in ["end", "pi", "range", "scalar", "start", "step", "time"] {
            assert_eq!(signature(name).unwrap().return_type, T::Scalar, "{name}");
        }
        for name in ["abs", "rate", "vector", "label_replace", "year"] {
            assert_eq!(signature(name).unwrap().return_type, T::Vector, "{name}");
        }
    }
}
