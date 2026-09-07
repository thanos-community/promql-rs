//! Public parser entry points. Mirrors upstream `parse.go`.
//!
//! The grammar and both lrlex lexers are compiled by `build.rs`. This
//! module wires them together and surfaces parse errors in a shape that
//! consumers can attach to HTTP responses.
//!
//! # Parse modes
//!
//! `grammar.y` has a single `start` rule that dispatches on a leading
//! `START_*` pseudo-token, exactly as upstream's does. Those tokens
//! have no lexer rule; upstream prepends one to the token stream in
//! `parser.Lex` (guarded by `InjectItem`), and [`InjectingLexer`] below
//! is the same trick against grmtools' `Lexer` trait. That is what lets
//! one grammar serve expressions, metric selectors and series
//! descriptions without duplicating the shared `metric` / `label_set`
//! rules.
//!
//! # Lexer modes
//!
//! Upstream lexes all of those with one lexer carrying a `seriesDesc`
//! bool (`upstream/lex.go`): the flag makes SPACE significant, turns
//! `x` and `_` into TIMES/BLANK, and disables hex literals (`0x…` is
//! ambiguous with the `x` repeat operator). lrlex always starts in
//! state 0, so the mode can't be set by the caller and lives in a
//! second lexer file instead. `series.l`'s start conditions map 1:1
//! onto upstream's state functions:
//!
//! | `series.l` | `upstream/lex.go`   |
//! |------------|---------------------|
//! | `INITIAL`  | `lexStatements`     |
//! | `BRACES`   | `lexInsideBraces`   |
//! | `SERIES`   | `lexValueSequence`  |
//!
//! The `<BRACES>\}` and `<SERIES>\{` transitions stand in for
//! upstream's `if l.seriesDesc` checks at the end of `lexInsideBraces`
//! and `lexKeywordOrIdentifier`. Because start conditions are chosen
//! before the match rather than after, `series.l` needs no equivalent
//! of upstream's `l.peek() != '{'` lookahead.
//!
//! One known divergence: in a value sequence, upstream lexes any
//! identifier and lets the `series_value` action reject anything that
//! isn't `stale`. A longest-match DFA can't do that — an identifier
//! rule would swallow the `x40` in `13.00x40` — so `series.l` matches
//! `stale` literally and any other word is a lex error rather than a
//! parse error. Both reject; only the message differs.

use lrpar::{Lexeme, NonStreamingLexer};

use crate::actions::ParseResult;
use crate::ast::{Expr, LabelMatcher, SeriesDescription};
use crate::error::{ParseError, ParseErrors};
use crate::posrange::{Pos, PositionRange};

mod start_tokens {
    include!(concat!(env!("OUT_DIR"), "/start_tokens.rs"));
}

type LexerTypes = lrlex::DefaultLexerTypes<u32>;
type Lexeme_ = lrlex::DefaultLexeme<u32>;

/// Wraps a lexer so the stream begins with a synthetic `START_*`
/// lexeme. Mirrors upstream's `parser.InjectItem` / the `p.injecting`
/// branch of `parser.Lex`, which exist for the same reason: yacc allows
/// one start symbol, so extra ones are selected by a leading token.
///
/// The injected lexeme has a zero-width span at offset 0, so it never
/// perturbs the position ranges the actions compute.
struct InjectingLexer<'lexer, 'input> {
    inner: &'lexer dyn NonStreamingLexer<'input, LexerTypes>,
    inject: u32,
}

impl lrpar::Lexer<LexerTypes> for InjectingLexer<'_, '_> {
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Result<Lexeme_, lrlex::LRLexError>> + 'a> {
        Box::new(std::iter::once(Ok(Lexeme_::new(self.inject, 0, 0))).chain(self.inner.iter()))
    }
}

impl<'input> NonStreamingLexer<'input, LexerTypes> for InjectingLexer<'_, 'input> {
    fn span_str(&self, span: cfgrammar::Span) -> &'input str {
        self.inner.span_str(span)
    }

    fn span_lines_str(&self, span: cfgrammar::Span) -> &'input str {
        self.inner.span_lines_str(span)
    }

    fn line_col(&self, span: cfgrammar::Span) -> ((usize, usize), (usize, usize)) {
        self.inner.line_col(span)
    }
}

/// Run the grammar over `input` in the given parse mode. `start_token`
/// selects the `start` alternative; upstream's `parseGenerated`.
fn parse_generated(
    input: &str,
    start_token: u32,
    series_mode: bool,
) -> Result<ParseResult, ParseErrors> {
    let expr_def;
    let series_def;
    let lexer = if series_mode {
        series_def = crate::series_l::lexerdef();
        Box::new(series_def.lexer(input)) as Box<dyn NonStreamingLexer<'_, LexerTypes>>
    } else {
        expr_def = crate::lexer_l::lexerdef();
        Box::new(expr_def.lexer(input)) as Box<dyn NonStreamingLexer<'_, LexerTypes>>
    };
    let injecting = InjectingLexer {
        inner: lexer.as_ref(),
        inject: start_token,
    };

    let (ast, errs) = crate::grammar::parse(&injecting);
    if !errs.is_empty() {
        let mut out = Vec::with_capacity(errs.len());
        for e in errs {
            out.push(ParseError::new(format!("{e}"), PositionRange::default()));
        }
        return Err(ParseErrors(out));
    }
    match ast {
        Some(Ok(result)) => Ok(result),
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

fn unexpected_mode(what: &str, input: &str) -> ParseErrors {
    ParseErrors(vec![ParseError::new(
        format!("input is not {what}"),
        PositionRange::new(0, input.len() as Pos),
    )])
}

/// Parse a PromQL expression. Upstream: `parser.ParseExpr`.
pub fn parse_expr(input: &str) -> Result<Expr, ParseErrors> {
    match parse_generated(input, start_tokens::START_EXPRESSION, false)? {
        ParseResult::Expr(e) => Ok(e),
        _ => Err(unexpected_mode("an expression", input)),
    }
}

/// Parse a metric selector, returning its label matchers.
/// Upstream: `parser.ParseMetricSelector`.
pub fn parse_metric_selector(input: &str) -> Result<Vec<LabelMatcher>, ParseErrors> {
    let expr = match parse_generated(input, start_tokens::START_METRIC_SELECTOR, false)? {
        ParseResult::Expr(e) => e,
        _ => return Err(unexpected_mode("a metric selector", input)),
    };
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
        _ => Err(unexpected_mode("a metric selector", input)),
    }
}

/// Parse a series description — one `metric{...} <values>` line of a
/// promqltest load block. Upstream: `parser.ParseSeriesDesc`.
///
/// Value sequences are expanded at parse time, as upstream does: a
/// `<value>+<step>x<count>` run yields `count + 1` points, the extra
/// one being "time 0, which we ignore in tests".
///
/// Native-histogram descriptors (`{{schema:1 …}}`) are not supported
/// yet; those alternatives of `series_item` reference rules that are
/// still on the sidecar's skip list, so they are rejected as parse
/// errors.
pub fn parse_series_desc(input: &str) -> Result<SeriesDescription, ParseErrors> {
    match parse_generated(input, start_tokens::START_SERIES_DESCRIPTION, true)? {
        ParseResult::SeriesDescription(sd) => Ok(sd),
        _ => Err(unexpected_mode("a series description", input)),
    }
}

/// Parse a bare label set (`{foo="bar"}`). Upstream's `START_METRIC`
/// mode, used by promtool's unit-test loader.
pub fn parse_metric(input: &str) -> Result<Vec<LabelMatcher>, ParseErrors> {
    match parse_generated(input, start_tokens::START_METRIC, true)? {
        ParseResult::Metric(m) => Ok(m),
        _ => Err(unexpected_mode("a metric", input)),
    }
}
