//! Per-production Rust action helpers. Mirrors upstream `parse.go`
//! helpers (`newBinaryExpression`, `newAggregateExpr`, `unquoteString`,
//! `addOffset`, `setTimestamp`, etc.).
//!
//! The grammar file calls into these so `src/grammar.y` stays
//! one-line-per-action and structurally diffable against upstream.
//!
//! Lifetime convention: every action takes
//! `&'l dyn NonStreamingLexer<'i, DefaultLexerTypes<u32>>` with
//! `'i: 'l` (the input string outlives the lexer borrow). This matches
//! the `__gt_lexer` / `__gt_input` lifetimes in grmtools' generated
//! parser.
//!
//! Current scope: the subset invoked by the core expression grammar
//! (see `src/grammar.y` for the matching rule coverage). Helpers for
//! series descriptions, histogram descriptors, fill modifiers, and
//! duration-expression arithmetic are not yet implemented.

use cfgrammar::Span;
use lrlex::{DefaultLexeme, DefaultLexerTypes};
use lrpar::{Lexeme, NonStreamingLexer};

use crate::ast::{
    AggregateExpr, AtModifier, BinaryExpr, Call, Expr, FunctionRef, LabelMatcher, MatchOp,
    MatrixSelector, NumberLiteral, ParenExpr, StringLiteral, SubqueryExpr, UnaryExpr, ValueType,
    VectorMatchCardinality, VectorMatching, VectorSelector,
};
use crate::posrange::{Pos, PositionRange};
use crate::token::ItemType;

/// Two-lifetime trait-object alias for the lexer trait grmtools passes
/// into every action. `'l` is the borrow lifetime (the `&'l dyn …`),
/// `'i` is the source string lifetime.
type L<'l, 'i> = dyn NonStreamingLexer<'i, DefaultLexerTypes<u32>> + 'l;

/// Short alias for the lexeme type the grammar's terminal actions see.
type Lx = DefaultLexeme<u32>;

/// Helper: source substring for a span.
pub fn span_str<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, span: Span) -> &'i str {
    lexer.span_str(span)
}

fn to_pos_range(span: Span) -> PositionRange {
    PositionRange::new(span.start() as Pos, span.end() as Pos)
}

// -------- binary modifiers --------

/// Intermediate struct for building up binary modifiers through the
/// grammar's left-recursive modifier rules. Mirrors upstream's approach
/// of threading a partial BinaryExpr through bool_modifier →
/// on_or_ignoring → group_modifiers → fill_modifiers.
#[derive(Debug, Clone, PartialEq)]
pub struct BinModifiers {
    pub vector_matching: VectorMatching,
    pub return_bool: bool,
}

impl Default for BinModifiers {
    fn default() -> Self {
        BinModifiers {
            vector_matching: VectorMatching::default(),
            return_bool: false,
        }
    }
}

// -------- literals --------

pub fn number_literal<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<Expr, ()> {
    let span = lx.span();
    let raw = lexer.span_str(span);
    let val = parse_number_literal(raw).unwrap_or(0.0);
    Ok(Expr::NumberLiteral(NumberLiteral {
        val,
        duration: false,
        pos_range: to_pos_range(span),
    }))
}

pub fn duration_literal<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<Expr, ()> {
    let span = lx.span();
    let raw = lexer.span_str(span);
    let val = parse_duration_seconds(raw).unwrap_or(0.0);
    Ok(Expr::NumberLiteral(NumberLiteral {
        val,
        duration: true,
        pos_range: to_pos_range(span),
    }))
}

pub fn string_literal<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<Expr, ()> {
    let span = lx.span();
    let raw = lexer.span_str(span);
    let val = unquote_string(raw).unwrap_or_else(|_| raw.to_string());
    Ok(Expr::StringLiteral(StringLiteral {
        val,
        pos_range: to_pos_range(span),
    }))
}

/// Return just the string content of a STRING token (with outer
/// quotes stripped and escapes processed), useful when the grammar
/// wants the raw value rather than a full `StringLiteral` expression.
pub fn string_literal_value<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, span: Span) -> Result<String, ()> {
    let raw = lexer.span_str(span);
    Ok(unquote_string(raw).unwrap_or_else(|_| raw.to_string()))
}

pub fn paren<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    inner: Result<Expr, ()>,
) -> Result<Expr, ()> {
    Ok(Expr::Paren(ParenExpr {
        expr: Box::new(inner?),
        pos_range: to_pos_range(span),
    }))
}

// -------- unary / binary --------

/// Grammar-facing binary helper. The operator arrives as a captured
/// token (`$2` in the emitted grammar) rather than a constant, so a
/// single helper covers every operator alt.
pub fn binary<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    op_lx: Lx,
    modifiers: Result<Option<BinModifiers>, ()>,
    lhs: Result<Expr, ()>,
    rhs: Result<Expr, ()>,
) -> Result<Expr, ()> {
    let op = op_item_type_from_lex(lexer, op_lx)?;
    let (vector_matching, return_bool) = match modifiers? {
        Some(mods) => (Some(mods.vector_matching), mods.return_bool),
        None => (None, false),
    };
    Ok(Expr::Binary(BinaryExpr {
        op,
        lhs: Box::new(lhs?),
        rhs: Box::new(rhs?),
        vector_matching,
        return_bool,
    }))
}

/// Grammar-facing unary helper. Same shape as [`binary`] — op comes in
/// as a captured token.
pub fn unary_from_token<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    op_ty: Result<ItemType, ()>,
    inner: Result<Expr, ()>,
) -> Result<Expr, ()> {
    let _ = lexer;
    let op = op_ty?;
    let e = inner?;
    let start_pos = span.start() as Pos;
    if let Expr::NumberLiteral(mut nl) = e {
        if op == ItemType::Sub {
            nl.val = -nl.val;
        }
        nl.pos_range.start = start_pos;
        return Ok(Expr::NumberLiteral(nl));
    }
    Ok(Expr::Unary(UnaryExpr {
        op,
        expr: Box::new(e),
        start_pos,
    }))
}

/// Map an operator token's source text back to its [`ItemType`]. Used
/// by [`binary`] to convert a captured `$2` token into the enum value.
/// Panics on an unknown operator — grmtools' grammar shape restricts
/// the input to the operators enumerated below.
fn op_item_type_from_lex<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<ItemType, ()> {
    let raw = lexer.span_str(lx.span());
    let ty = match raw {
        "+" => ItemType::Add,
        "-" => ItemType::Sub,
        "*" => ItemType::Mul,
        "/" => ItemType::Div,
        "%" => ItemType::Mod,
        "^" => ItemType::Pow,
        "==" => ItemType::EqlC,
        "!=" => ItemType::Neq,
        "<" => ItemType::Lss,
        "<=" => ItemType::Lte,
        ">" => ItemType::Gtr,
        ">=" => ItemType::Gte,
        "and" => ItemType::Land,
        "or" => ItemType::Lor,
        "unless" => ItemType::Lunless,
        "atan2" => ItemType::Atan2,
        _ => return Err(()),
    };
    Ok(ty)
}

// -------- binary modifier helpers --------

/// Empty bool_modifier: no modifier, no allocation.
pub fn bool_modifier_empty() -> Result<Option<BinModifiers>, ()> {
    Ok(None)
}

/// Bool modifier: sets return_bool flag.
pub fn bool_modifier_bool() -> Result<Option<BinModifiers>, ()> {
    Ok(Some(BinModifiers {
        vector_matching: VectorMatching::default(),
        return_bool: true,
    }))
}

/// on_or_ignoring with IGNORING: takes bool_modifier and adds matching labels.
pub fn on_or_ignoring_ignoring(
    mods: Result<Option<BinModifiers>, ()>,
    labels: Result<Vec<String>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.matching_labels = labels?;
    m.vector_matching.on = false;
    Ok(Some(m))
}

/// on_or_ignoring with ON: takes bool_modifier and adds matching labels + on flag.
pub fn on_or_ignoring_on(
    mods: Result<Option<BinModifiers>, ()>,
    labels: Result<Vec<String>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.matching_labels = labels?;
    m.vector_matching.on = true;
    Ok(Some(m))
}

/// group_modifiers: pass through bool_modifier or on_or_ignoring unchanged.
pub fn group_modifiers_pass(
    mods: Result<Option<BinModifiers>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    mods
}

/// group_modifiers with GROUP_LEFT: sets ManyToOne cardinality and include labels.
pub fn group_modifiers_left(
    mods: Result<Option<BinModifiers>, ()>,
    labels: Result<Vec<String>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.card = VectorMatchCardinality::ManyToOne;
    m.vector_matching.include = labels?;
    Ok(Some(m))
}

/// group_modifiers with GROUP_RIGHT: sets OneToMany cardinality and include labels.
pub fn group_modifiers_right(
    mods: Result<Option<BinModifiers>, ()>,
    labels: Result<Vec<String>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.card = VectorMatchCardinality::OneToMany;
    m.vector_matching.include = labels?;
    Ok(Some(m))
}

/// fill_modifiers: pass through group_modifiers unchanged.
pub fn fill_modifiers_pass(
    mods: Result<Option<BinModifiers>, ()>,
) -> Result<Option<BinModifiers>, ()> {
    mods
}

/// fill_modifiers with FILL: sets both LHS and RHS to the same fill value.
pub fn fill_modifiers_fill(
    mods: Result<Option<BinModifiers>, ()>,
    value: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    let v = value?;
    m.vector_matching.fill_values.lhs = Some(v);
    m.vector_matching.fill_values.rhs = Some(v);
    Ok(Some(m))
}

/// fill_modifiers with FILL_LEFT: sets only LHS fill value.
pub fn fill_modifiers_fill_left(
    mods: Result<Option<BinModifiers>, ()>,
    value: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.fill_values.lhs = Some(value?);
    Ok(Some(m))
}

/// fill_modifiers with FILL_RIGHT: sets only RHS fill value.
pub fn fill_modifiers_fill_right(
    mods: Result<Option<BinModifiers>, ()>,
    value: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.fill_values.rhs = Some(value?);
    Ok(Some(m))
}

/// fill_modifiers with FILL_LEFT then FILL_RIGHT: sets both fill values.
pub fn fill_modifiers_fill_left_right(
    mods: Result<Option<BinModifiers>, ()>,
    left_val: Result<f64, ()>,
    right_val: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.fill_values.lhs = Some(left_val?);
    m.vector_matching.fill_values.rhs = Some(right_val?);
    Ok(Some(m))
}

/// fill_modifiers with FILL_RIGHT then FILL_LEFT: sets both fill values.
pub fn fill_modifiers_fill_right_left(
    mods: Result<Option<BinModifiers>, ()>,
    right_val: Result<f64, ()>,
    left_val: Result<f64, ()>,
) -> Result<Option<BinModifiers>, ()> {
    let mut m = mods?.unwrap_or_default();
    m.vector_matching.fill_values.lhs = Some(left_val?);
    m.vector_matching.fill_values.rhs = Some(right_val?);
    Ok(Some(m))
}

/// Extract the numeric value from a fill_value production (number_duration_literal wrapped in parens).
pub fn fill_value_extract(expr: Result<Expr, ()>) -> Result<f64, ()> {
    match expr? {
        Expr::NumberLiteral(nl) => Ok(nl.val),
        _ => Err(()),
    }
}

/// Extract negated numeric value from a fill_value production with unary minus.
pub fn fill_value_unary(op: Result<ItemType, ()>, expr: Result<Expr, ()>) -> Result<f64, ()> {
    let op_ty = op?;
    match expr? {
        Expr::NumberLiteral(nl) => {
            if op_ty == ItemType::Sub {
                Ok(-nl.val)
            } else {
                Ok(nl.val)
            }
        }
        _ => Err(()),
    }
}

// -------- selectors --------

pub fn vector_selector<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    metric_name: Option<String>,
    matchers: Option<Vec<LabelMatcher>>,
) -> Result<Expr, ()> {
    let name = metric_name.unwrap_or_default();
    let label_matchers = matchers.unwrap_or_default();
    Ok(Expr::VectorSelector(VectorSelector {
        name,
        original_offset_secs: 0.0,
        original_offset_expr: None,
        offset_secs: 0.0,
        timestamp: None,
        skip_histogram_buckets: false,
        start_or_end: None,
        label_matchers,
        bypass_empty_matcher_check: false,
        anchored: false,
        smoothed: false,
        pos_range: to_pos_range(span),
    }))
}

pub fn label_matcher<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    name_span: Span,
    op: MatchOp,
    value_lx: Lx,
) -> Result<LabelMatcher, ()> {
    let name = lexer.span_str(name_span).to_string();
    let raw_value = lexer.span_str(value_lx.span());
    let value = unquote_string(raw_value).unwrap_or_else(|_| raw_value.to_string());
    Ok(LabelMatcher {
        name,
        op,
        value,
        pos_range: to_pos_range(span),
    })
}

pub fn quoted_metric_name_matcher<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    value_lx: Lx,
) -> Result<LabelMatcher, ()> {
    let raw = lexer.span_str(value_lx.span());
    let value = unquote_string(raw).unwrap_or_else(|_| raw.to_string());
    Ok(LabelMatcher {
        name: "__name__".to_string(),
        op: MatchOp::Equal,
        value,
        pos_range: to_pos_range(span),
    })
}

/// Variant of [`quoted_metric_name_matcher`] where the string has
/// already been unquoted (e.g. via `actions::string_literal_value`).
pub fn quoted_metric_name_matcher_from_ident<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    value: String,
) -> Result<LabelMatcher, ()> {
    Ok(LabelMatcher {
        name: "__name__".to_string(),
        op: MatchOp::Equal,
        value,
        pos_range: to_pos_range(span),
    })
}

/// Matcher where the label *name* is a `string_identifier` (quoted
/// UTF-8 label) and the value is a `STRING` token. Upstream's
/// `label_matcher : string_identifier match_op STRING` form.
pub fn label_matcher_quoted_name<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    name: String,
    op: MatchOp,
    value_lx: Lx,
) -> Result<LabelMatcher, ()> {
    let raw = lexer.span_str(value_lx.span());
    let value = unquote_string(raw).unwrap_or_else(|_| raw.to_string());
    Ok(LabelMatcher {
        name,
        op,
        value,
        pos_range: to_pos_range(span),
    })
}

// -------- matrix / subquery / offset / @ --------

pub fn matrix_selector<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    selector: Result<Expr, ()>,
    duration_lx: Lx,
) -> Result<Expr, ()> {
    let raw = lexer.span_str(duration_lx.span());
    let range_secs = parse_duration_seconds(raw).unwrap_or(0.0);
    Ok(Expr::MatrixSelector(MatrixSelector {
        vector_selector: Box::new(selector?),
        range_secs,
        range_expr: None,
        end_pos: span.end() as Pos,
    }))
}

pub fn subquery<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    inner: Result<Expr, ()>,
    range_lx: Lx,
    step_lx: Option<Lx>,
) -> Result<Expr, ()> {
    let range_secs = parse_duration_seconds(lexer.span_str(range_lx.span())).unwrap_or(0.0);
    let step_secs = step_lx
        .map(|l| parse_duration_seconds(lexer.span_str(l.span())).unwrap_or(0.0))
        .unwrap_or(0.0);
    Ok(Expr::Subquery(SubqueryExpr {
        expr: Box::new(inner?),
        range_secs,
        range_expr: None,
        original_offset_secs: 0.0,
        original_offset_expr: None,
        offset_secs: 0.0,
        timestamp: None,
        start_or_end: None,
        step_secs,
        step_expr: None,
        end_pos: span.end() as Pos,
    }))
}

/// Variant of [`matrix_selector`] for the upstream-shaped grammar,
/// where the range arrives as a full Expr (produced by the
/// `positive_duration_expr → duration_expr → number_duration_literal`
/// chain) rather than a raw DURATION token. Currently only the literal
/// form is supported; its f64 value is extracted directly.
pub fn matrix_selector_from_expr<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    selector: Result<Expr, ()>,
    range: Result<Expr, ()>,
) -> Result<Expr, ()> {
    let range_secs = duration_from_expr(&range?)?;
    Ok(Expr::MatrixSelector(MatrixSelector {
        vector_selector: Box::new(selector?),
        range_secs,
        range_expr: None,
        end_pos: span.end() as Pos,
    }))
}

/// Variant of [`subquery`] for the upstream-shaped grammar. Both the
/// range and the optional step are Exprs built by the duration
/// chain; each is currently expected to be a numeric literal.
pub fn subquery_from_exprs<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    inner: Result<Expr, ()>,
    range: Result<Expr, ()>,
    step: Option<Result<Expr, ()>>,
) -> Result<Expr, ()> {
    let range_secs = duration_from_expr(&range?)?;
    let step_secs = match step {
        Some(r) => duration_from_expr(&r?)?,
        None => 0.0,
    };
    Ok(Expr::Subquery(SubqueryExpr {
        expr: Box::new(inner?),
        range_secs,
        range_expr: None,
        original_offset_secs: 0.0,
        original_offset_expr: None,
        offset_secs: 0.0,
        timestamp: None,
        start_or_end: None,
        step_secs,
        step_expr: None,
        end_pos: span.end() as Pos,
    }))
}

/// Variant of [`offset`] for the upstream-shaped grammar, where the
/// offset value flows through `offset_duration_expr → duration_expr`
/// rather than a raw DURATION token. Currently only the literal form
/// is supported; extracts the f64 and applies it to the subject.
pub fn offset_from_expr<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    inner: Result<Expr, ()>,
    offset: Result<Expr, ()>,
) -> Result<Expr, ()> {
    let offset_secs = duration_from_expr(&offset?)?;
    Ok(apply_offset(inner?, offset_secs))
}

/// Extract a seconds-valued f64 from an Expr whose shape is a
/// `NumberLiteral` (either a `DURATION` token or a numeric literal).
/// Other shapes — duration arithmetic, function calls — aren't
/// yet supported and produce `Err(())`.
fn duration_from_expr(e: &Expr) -> Result<f64, ()> {
    match e {
        Expr::NumberLiteral(nl) => Ok(nl.val),
        Expr::Paren(p) => duration_from_expr(&p.expr),
        _ => Err(()),
    }
}

pub fn offset<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    inner: Result<Expr, ()>,
    negate: bool,
    duration_lx: Lx,
) -> Result<Expr, ()> {
    let secs = parse_duration_seconds(lexer.span_str(duration_lx.span())).unwrap_or(0.0);
    let offset_secs = if negate { -secs } else { secs };
    Ok(apply_offset(inner?, offset_secs))
}

fn apply_offset(mut e: Expr, offset_secs: f64) -> Expr {
    match &mut e {
        Expr::VectorSelector(vs) => {
            vs.original_offset_secs = offset_secs;
            vs.offset_secs = offset_secs;
        }
        Expr::MatrixSelector(ms) => {
            if let Expr::VectorSelector(vs) = ms.vector_selector.as_mut() {
                vs.original_offset_secs = offset_secs;
                vs.offset_secs = offset_secs;
            }
        }
        Expr::Subquery(sq) => {
            sq.original_offset_secs = offset_secs;
            sq.offset_secs = offset_secs;
        }
        _ => {}
    }
    e
}

pub fn at_timestamp<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    inner: Result<Expr, ()>,
    negate: bool,
    number_lx: Lx,
) -> Result<Expr, ()> {
    let raw = lexer.span_str(number_lx.span());
    let secs = parse_number_literal(raw).unwrap_or(0.0);
    let secs = if negate { -secs } else { secs };
    Ok(apply_at_timestamp(inner?, secs))
}

fn apply_at_timestamp(mut e: Expr, secs: f64) -> Expr {
    let ts_ms = (secs * 1000.0) as i64;
    match &mut e {
        Expr::VectorSelector(vs) => vs.timestamp = Some(ts_ms),
        Expr::MatrixSelector(ms) => {
            if let Expr::VectorSelector(vs) = ms.vector_selector.as_mut() {
                vs.timestamp = Some(ts_ms);
            }
        }
        Expr::Subquery(sq) => sq.timestamp = Some(ts_ms),
        _ => {}
    }
    e
}

pub fn at_modifier<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    inner: Result<Expr, ()>,
    modifier: AtModifier,
) -> Result<Expr, ()> {
    let mut e = inner?;
    match &mut e {
        Expr::VectorSelector(vs) => vs.start_or_end = Some(modifier),
        Expr::MatrixSelector(ms) => {
            if let Expr::VectorSelector(vs) = ms.vector_selector.as_mut() {
                vs.start_or_end = Some(modifier);
            }
        }
        Expr::Subquery(sq) => sq.start_or_end = Some(modifier),
        _ => {}
    }
    Ok(e)
}

/// Variant of [`at_timestamp`] for the upstream-shaped grammar, where
/// the `@ <number>` arrives as a pre-parsed f64 (from the
/// `signed_or_unsigned_number → number` rule chain).
pub fn at_timestamp_val<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    inner: Result<Expr, ()>,
    secs: f64,
) -> Result<Expr, ()> {
    Ok(apply_at_timestamp(inner?, secs))
}

/// Just the f64 value of a NUMBER token — used by upstream's `number`
/// rule. Different return shape from `number_literal` (which wraps in
/// an Expr).
pub fn number_value<'l, 'i: 'l>(lexer: &'l L<'l, 'i>, lx: Lx) -> Result<f64, ()> {
    let raw = lexer.span_str(lx.span());
    parse_number_literal(raw)
}

/// `unary_op number_duration_literal` — apply a +/- sign to a duration
/// Expr. Used by `offset_duration_expr` and `signed_number` chains.
pub fn signed_duration(op: Result<ItemType, ()>, inner: Result<Expr, ()>) -> Result<Expr, ()> {
    let e = inner?;
    let op = op?;
    if let Expr::NumberLiteral(mut nl) = e {
        if op == ItemType::Sub {
            nl.val = -nl.val;
        }
        return Ok(Expr::NumberLiteral(nl));
    }
    Ok(e)
}

// -------- function calls & aggregates --------

pub fn function_call<'l, 'i: 'l>(
    lexer: &'l L<'l, 'i>,
    span: Span,
    name_lx: Lx,
    args: Vec<Expr>,
) -> Result<Expr, ()> {
    let name = lexer.span_str(name_lx.span()).to_string();
    Ok(Expr::Call(Call {
        func: FunctionRef {
            name,
            arg_types: Vec::new(),
            return_type: ValueType::Vector,
            variadic: 0,
            experimental: false,
        },
        args,
        pos_range: to_pos_range(span),
    }))
}

pub fn aggregate<'l, 'i: 'l>(
    _lexer: &'l L<'l, 'i>,
    span: Span,
    op: ItemType,
    modifier: Option<(bool, Vec<String>)>,
    mut args: Vec<Expr>,
) -> Result<Expr, ()> {
    let (without, grouping) = modifier.unwrap_or((false, Vec::new()));
    let (param, expr) = if aggregator_takes_param(op) && args.len() >= 2 {
        let p = args.remove(0);
        (Some(Box::new(p)), args.pop().expect("checked len"))
    } else {
        let e = args.pop().unwrap_or_else(|| {
            Expr::NumberLiteral(NumberLiteral {
                val: 0.0,
                duration: false,
                pos_range: to_pos_range(span),
            })
        });
        (None, e)
    };
    Ok(Expr::Aggregate(AggregateExpr {
        op,
        expr: Box::new(expr),
        param,
        grouping,
        without,
        pos_range: to_pos_range(span),
    }))
}

fn aggregator_takes_param(op: ItemType) -> bool {
    matches!(
        op,
        ItemType::Topk
            | ItemType::Bottomk
            | ItemType::Quantile
            | ItemType::CountValues
            | ItemType::Limitk
            | ItemType::LimitRatio
    )
}

// -------- shared parsers (number, duration, string) --------

fn parse_number_literal(raw: &str) -> Result<f64, ()> {
    if raw.eq_ignore_ascii_case("nan") {
        return Ok(f64::NAN);
    }
    if raw.eq_ignore_ascii_case("inf") || raw.eq_ignore_ascii_case("+inf") {
        return Ok(f64::INFINITY);
    }
    if raw.eq_ignore_ascii_case("-inf") {
        return Ok(f64::NEG_INFINITY);
    }
    let cleaned: String = raw.chars().filter(|&c| c != '_').collect();
    let s = cleaned.as_str();
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16)
            .map(|n| n as f64)
            .map_err(|_| ());
    }
    if let Some(oct) = s.strip_prefix("0o").or_else(|| s.strip_prefix("0O")) {
        return u64::from_str_radix(oct, 8)
            .map(|n| n as f64)
            .map_err(|_| ());
    }
    if let Some(bin) = s.strip_prefix("0b").or_else(|| s.strip_prefix("0B")) {
        return u64::from_str_radix(bin, 2)
            .map(|n| n as f64)
            .map_err(|_| ());
    }
    s.parse::<f64>().map_err(|_| ())
}

fn parse_duration_seconds(raw: &str) -> Result<f64, ()> {
    let mut total = 0f64;
    let mut chars = raw.chars().peekable();
    let mut had_any = false;
    while chars.peek().is_some() {
        let mut num = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '.' {
                num.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if num.is_empty() {
            return Err(());
        }
        let n: f64 = num.parse().map_err(|_| ())?;
        let mut unit = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_alphabetic() {
                unit.push(c);
                chars.next();
            } else {
                break;
            }
        }
        let multiplier = match unit.as_str() {
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            "d" => 86400.0,
            "w" => 604_800.0,
            "y" => 31_536_000.0,
            _ => return Err(()),
        };
        total += n * multiplier;
        had_any = true;
    }
    if !had_any {
        return Err(());
    }
    Ok(total)
}

fn unquote_string(raw: &str) -> Result<String, ()> {
    if raw.len() < 2 {
        return Err(());
    }
    let bytes = raw.as_bytes();
    let open = bytes[0];
    let close = bytes[raw.len() - 1];
    if open != close {
        return Err(());
    }
    let inner = &raw[1..raw.len() - 1];
    if open == b'`' {
        return Ok(inner.to_string());
    }
    let mut out = String::with_capacity(inner.len());
    let mut it = inner.chars();
    while let Some(c) = it.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some('"') => out.push('"'),
            Some('`') => out.push('`'),
            Some('0') => out.push('\0'),
            Some('a') => out.push('\x07'),
            Some('b') => out.push('\x08'),
            Some('f') => out.push('\x0c'),
            Some('v') => out.push('\x0b'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => return Err(()),
        }
    }
    Ok(out)
}
