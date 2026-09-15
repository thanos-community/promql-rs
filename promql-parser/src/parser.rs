//! Public parser entry points. Mirrors upstream `parse.go`.
//!
//! The grammar and lrlex lexer are compiled by `build.rs`. This module
//! wires the two together and surfaces parse errors in a shape that
//! consumers can attach to HTTP responses.

use lrpar::{LexError, Lexeme};

use crate::ast::{Expr, LabelMatcher};
use crate::error::{ParseError, ParseErrors};
use crate::posrange::{Pos, PositionRange};

type LexerTypes = lrlex::DefaultLexerTypes<u32>;

/// The byte range an lrpar error points at.
///
/// Both error kinds carry a span: a lex error its own, a parse error
/// that of the lexeme it stopped on. Every `ParseError` used to be
/// built with `PositionRange::default()`, i.e. `0..0`, so the position
/// survived only inside the `Display` text — which a consumer would
/// have had to parse back out. This module's whole stated purpose is to
/// "surface parse errors in a shape that consumers can attach to HTTP
/// responses", and a range that always says `0..0` cannot do that.
///
/// Note a lex error's span is the zero-width point where matching
/// failed, not a range covering the offending text.
fn error_range(e: &lrpar::LexParseError<u32, LexerTypes>) -> PositionRange {
    let span = match e {
        lrpar::LexParseError::LexError(e) => e.span(),
        lrpar::LexParseError::ParseError(e) => e.lexeme().span(),
    };
    PositionRange::new(span.start() as Pos, span.end() as Pos)
}

/// Parse a PromQL expression. Upstream: `parser.ParseExpr`.
pub fn parse_expr(input: &str) -> Result<Expr, ParseErrors> {
    let lexerdef = crate::lexer_l::lexerdef();
    let lexer = lexerdef.lexer(input);
    let (ast, errs) = crate::grammar::parse(&lexer);
    if !errs.is_empty() {
        let mut out = Vec::with_capacity(errs.len());
        for e in errs {
            out.push(ParseError::new(format!("{e}"), error_range(&e)));
        }
        return Err(ParseErrors(out));
    }
    // Neither of these carries a span of its own, so they cover the
    // whole input rather than claiming a misleading `0..0`.
    let whole = PositionRange::new(0, input.len() as Pos);
    match ast {
        Some(Ok(expr)) => Ok(expr),
        Some(Err(())) => Err(ParseErrors(vec![ParseError::new(
            "parse produced an error node",
            whole,
        )])),
        None => Err(ParseErrors(vec![ParseError::new("empty parse", whole)])),
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
