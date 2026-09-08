//! Public parser entry points. Mirrors upstream `parse.go`.
//!
//! The grammar and lrlex lexer are compiled by `build.rs`. This module
//! wires the two together and surfaces parse errors in a shape that
//! consumers can attach to HTTP responses.

use crate::ast::{Expr, LabelMatcher};
use crate::error::{ParseError, ParseErrors};
use crate::posrange::{Pos, PositionRange};

/// Parse a PromQL expression. Upstream: `parser.ParseExpr`.
pub fn parse_expr(input: &str) -> Result<Expr, ParseErrors> {
    let lexerdef = crate::lexer_l::lexerdef();
    let lexer = lexerdef.lexer(input);
    let (ast, errs) = crate::grammar::parse(&lexer);
    if !errs.is_empty() {
        let mut out = Vec::with_capacity(errs.len());
        for e in errs {
            out.push(ParseError::new(format!("{e}"), PositionRange::default()));
        }
        return Err(ParseErrors(out));
    }
    match ast {
        Some(Ok(expr)) => Ok(expr),
        Some(Err(())) => Err(ParseErrors(vec![ParseError::new(
            "parse produced an error node",
            PositionRange::default(),
        )])),
        None => Err(ParseErrors(vec![ParseError::new(
            "empty parse",
            PositionRange::default(),
        )])),
    }
}

/// Parse a metric selector, returning its label matchers.
/// Upstream: `parser.ParseMetricSelector`.
pub fn parse_metric_selector(input: &str) -> Result<Vec<LabelMatcher>, ParseErrors> {
    let expr = parse_expr(input)?;
    match expr {
        Expr::VectorSelector(mut vs) => {
            if !vs.name.is_empty() {
                vs.label_matchers.push(LabelMatcher {
                    name: "__name__".to_string(),
                    op: crate::ast::MatchOp::Equal,
                    value: std::mem::take(&mut vs.name),
                    pos_range: vs.pos_range,
                });
            }
            Ok(vs.label_matchers)
        }
        _ => Err(ParseErrors(vec![ParseError::new(
            "input is not a metric selector",
            PositionRange::new(0, input.len() as Pos),
        )])),
    }
}
