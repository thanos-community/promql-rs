//! PromQL AST node types. Structural mirror of `upstream/ast.go`.
//!
//! Upstream uses a `Node` interface with `Expr` / `Statement`
//! sub-interfaces and one struct per node kind. In Rust we collapse
//! expression types into a single [`Expr`] enum for exhaustive matching;
//! statements are a separate enum. Field names are preserved, case-shifted
//! to Rust's `snake_case`. A non-trivial field-name change would cost us
//! diff review against future upstream.

use crate::posrange::{Pos, PositionRange};
use crate::token::ItemType;

/// Value classification, mirroring upstream `ValueType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueType {
    None,
    Scalar,
    Vector,
    Matrix,
    String,
}

/// Label-matcher operator. Mirrors `labels.MatchType` upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
}

impl MatchOp {
    pub fn from_item_type(it: ItemType) -> Option<Self> {
        Some(match it {
            ItemType::Eql => MatchOp::Equal,
            ItemType::Neq => MatchOp::NotEqual,
            ItemType::EqlRegex => MatchOp::RegexEqual,
            ItemType::NeqRegex => MatchOp::RegexNotEqual,
            _ => return None,
        })
    }
}

/// Single label matcher from a `{…}` selector.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
    pub pos_range: PositionRange,
}

/// Vector-matching cardinality. Mirrors upstream `VectorMatchCardinality`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VectorMatchCardinality {
    #[default]
    OneToOne,
    ManyToOne,
    OneToMany,
    ManyToMany,
}

/// Fill values for vector matching with `fill` / `fill_left` /
/// `fill_right`. Upstream `VectorMatchFillValues`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VectorMatchFillValues {
    pub lhs: Option<f64>,
    pub rhs: Option<f64>,
}

/// Vector-matching shape for a binary expression. Upstream
/// `VectorMatching`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VectorMatching {
    pub card: VectorMatchCardinality,
    pub matching_labels: Vec<String>,
    pub on: bool,
    pub include: Vec<String>,
    pub fill_values: VectorMatchFillValues,
}

/// `@ start()` / `@ end()` preprocessor. Upstream represents this via
/// `ItemType`; we use a dedicated enum for type safety in Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AtModifier {
    Start,
    End,
}

/// All expression node types. Upstream `Expr` interface flattened.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Aggregate(AggregateExpr),
    Binary(BinaryExpr),
    Call(Call),
    MatrixSelector(MatrixSelector),
    NumberLiteral(NumberLiteral),
    Paren(ParenExpr),
    StringLiteral(StringLiteral),
    Subquery(SubqueryExpr),
    Unary(UnaryExpr),
    VectorSelector(VectorSelector),
    StepInvariant(Box<Expr>),
    Duration(DurationExpr),
}

impl Expr {
    pub fn value_type(&self) -> ValueType {
        match self {
            Expr::Aggregate(_) | Expr::VectorSelector(_) => ValueType::Vector,
            Expr::Binary(e) => {
                if matches!(e.lhs.value_type(), ValueType::Scalar)
                    && matches!(e.rhs.value_type(), ValueType::Scalar)
                {
                    ValueType::Scalar
                } else {
                    ValueType::Vector
                }
            }
            Expr::Call(c) => c.func.return_type,
            Expr::MatrixSelector(_) | Expr::Subquery(_) => ValueType::Matrix,
            Expr::NumberLiteral(_) | Expr::Duration(_) => ValueType::Scalar,
            Expr::Paren(p) => p.expr.value_type(),
            Expr::StringLiteral(_) => ValueType::String,
            Expr::Unary(u) => u.expr.value_type(),
            Expr::StepInvariant(e) => e.value_type(),
        }
    }

    pub fn position_range(&self) -> PositionRange {
        match self {
            Expr::Aggregate(e) => e.pos_range,
            Expr::Binary(e) => PositionRange::merge(e.lhs.position_range(), e.rhs.position_range()),
            Expr::Call(e) => e.pos_range,
            Expr::MatrixSelector(e) => PositionRange {
                start: e.vector_selector.position_range().start,
                end: e.end_pos,
            },
            Expr::NumberLiteral(e) => e.pos_range,
            Expr::Paren(e) => e.pos_range,
            Expr::StringLiteral(e) => e.pos_range,
            Expr::Subquery(e) => PositionRange {
                start: e.expr.position_range().start,
                end: e.end_pos,
            },
            Expr::Unary(e) => PositionRange {
                start: e.start_pos,
                end: e.expr.position_range().end,
            },
            Expr::VectorSelector(e) => e.pos_range,
            Expr::StepInvariant(e) => e.position_range(),
            Expr::Duration(e) => e.position_range(),
        }
    }
}

/// Upstream `AggregateExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub op: ItemType,
    pub expr: Box<Expr>,
    pub param: Option<Box<Expr>>,
    pub grouping: Vec<String>,
    pub without: bool,
    pub pos_range: PositionRange,
}

/// Upstream `BinaryExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub op: ItemType,
    pub lhs: Box<Expr>,
    pub rhs: Box<Expr>,
    pub vector_matching: Option<VectorMatching>,
    pub return_bool: bool,
}

/// Reference to a built-in function. Upstream `*Function` is a pointer
/// into a global registry; we carry the resolved metadata in the AST so
/// consumers don't need the registry at query time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionRef {
    pub name: String,
    pub arg_types: Vec<ValueType>,
    pub return_type: ValueType,
    /// Upstream `Variadic`: index from which args are variadic, or 0/−1
    /// encoding per upstream convention.
    pub variadic: i32,
    pub experimental: bool,
}

/// Upstream `Call`.
#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub func: FunctionRef,
    pub args: Vec<Expr>,
    pub pos_range: PositionRange,
}

/// Upstream `MatrixSelector`.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSelector {
    pub vector_selector: Box<Expr>,
    /// Upstream `Range time.Duration` — stored as seconds. Zero when the
    /// range comes from a `DurationExpr` (step()/range() experimental).
    pub range_secs: f64,
    pub range_expr: Option<Box<DurationExpr>>,
    pub end_pos: Pos,
}

/// Upstream `SubqueryExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryExpr {
    pub expr: Box<Expr>,
    pub range_secs: f64,
    pub range_expr: Option<Box<DurationExpr>>,
    pub original_offset_secs: f64,
    pub original_offset_expr: Option<Box<DurationExpr>>,
    pub offset_secs: f64,
    pub timestamp: Option<i64>,
    pub start_or_end: Option<AtModifier>,
    pub step_secs: f64,
    pub step_expr: Option<Box<DurationExpr>>,
    pub end_pos: Pos,
}

/// Upstream `NumberLiteral`.
#[derive(Debug, Clone, PartialEq)]
pub struct NumberLiteral {
    pub val: f64,
    pub duration: bool,
    pub pos_range: PositionRange,
}

/// Upstream `ParenExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct ParenExpr {
    pub expr: Box<Expr>,
    pub pos_range: PositionRange,
}

/// Upstream `StringLiteral`.
#[derive(Debug, Clone, PartialEq)]
pub struct StringLiteral {
    pub val: String,
    pub pos_range: PositionRange,
}

/// Upstream `UnaryExpr`.
#[derive(Debug, Clone, PartialEq)]
pub struct UnaryExpr {
    pub op: ItemType,
    pub expr: Box<Expr>,
    pub start_pos: Pos,
}

/// Upstream `VectorSelector`. Fields that upstream populates during query
/// execution (`UnexpandedSeriesSet`, `Series`) are intentionally omitted:
/// the parser crate has no storage dependency.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSelector {
    pub name: String,
    pub original_offset_secs: f64,
    pub original_offset_expr: Option<Box<DurationExpr>>,
    pub offset_secs: f64,
    pub timestamp: Option<i64>,
    pub skip_histogram_buckets: bool,
    pub start_or_end: Option<AtModifier>,
    pub label_matchers: Vec<LabelMatcher>,
    pub bypass_empty_matcher_check: bool,
    pub anchored: bool,
    pub smoothed: bool,
    pub pos_range: PositionRange,
}

/// Upstream `DurationExpr`. Used for experimental duration arithmetic
/// (`step()`, `range()`, `min()`, `max()`, and operator-joined
/// durations).
#[derive(Debug, Clone, PartialEq)]
pub struct DurationExpr {
    pub op: ItemType,
    pub lhs: Option<Box<Expr>>,
    pub rhs: Option<Box<Expr>>,
    pub wrapped: bool,
    pub start_pos: Pos,
    pub end_pos: Pos,
}

impl DurationExpr {
    pub fn position_range(&self) -> PositionRange {
        PositionRange {
            start: self.start_pos,
            end: self.end_pos,
        }
    }
}
