//! Binary operators: arithmetic, comparison, and the set operators.
//!
//! Prometheus evaluates a binary operator once per step, over the two
//! instant vectors at that step. Two facts follow, and they decide the
//! whole shape of this module:
//!
//! * **Matching is per step.** Two series that share a match signature
//!   but never carry a sample at the same instant never collide, so a
//!   low-cardinality side whose included label changes mid-window is two
//!   output series rather than an error.
//! * **Output identity is the result label set.** Several matched pairs
//!   can feed one output series, and that is only wrong when two of them
//!   want the same step.
//!
//! So a vector-vector operator is a join on the matching labels followed
//! by a group-by on the result labels:
//!
//! ```text
//! Projection: promql_labels(…result labels…) AS labels, samples
//!   Aggregate: groupBy=[get_field(labels, …result labels…)],
//!              aggr=[promql_binary_group(matches, …) AS samples]
//!     Projection: promql_labels(…) AS labels,
//!                 promql_binary_match(many.samples, one.samples, '+', …) AS matches
//!       Inner Join: many.__match_0 = one.__match_0
//!         Projection: labels, samples, get_field(labels, 'pod') AS __match_0
//!         Projection: labels, samples, get_field(labels, 'pod') AS __match_0
//! ```
//!
//! The group-by is the same node [`crate::plan::Planner::aggregate`]
//! builds for `sum by (…)`, which is what keeps the canonical shape:
//! nothing here unnests, so nothing here has to repair the nullability
//! that DataFusion's `Unnest` imposes on a list's element.
//!
//! Scalars are series too. A number literal becomes a one-row plan with
//! an empty label set and a sample at every step, so `foo + 1` is a cross
//! join rather than a special case, and `1 + 2` is the same code with two
//! one-row plans.
//!
//! # What the parser leaves to us
//!
//! Upstream rejects a good deal of nonsense in `checkAST` — `bool` on a
//! non-comparison, grouping on a set operator, a label in both `on` and
//! `group_left`. Our parser does not, so [`normalize`] ports those rules
//! and reports them as [`EngineError::Query`]: they are answers the
//! reference engine also gives, not gaps in this engine.

pub mod combine;
pub mod group;

use datafusion::common::Column;
use datafusion::common::ScalarValue;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::functions_aggregate::expr_fn::{count, first_value};
use datafusion::logical_expr::{
    col, lit, Expr as DfExpr, JoinType, LogicalPlan, LogicalPlanBuilder,
};
use promql_parser::ast::{
    BinaryExpr, UnaryExpr, ValueType, VectorMatchCardinality, VectorMatching,
};
use promql_parser::token::ItemType;

use crate::aggregate;
use crate::error::EngineError;
use crate::labels;
use crate::matcher::METRIC_NAME;
use crate::plan::{Planned, Planner};
use crate::series::{LABELS, SAMPLES};

/// The column a join's matched pairs arrive in.
const MATCHES: &str = "__matches";
/// The "one" side's label set, for the duplicate-match message.
const ONE_LABELS: &str = "__pair_labels";
/// The "one" side's match group, likewise.
const MATCH_GROUP: &str = "__pair_group";
/// The other side's presence per step, for a set operator.
const MASK: &str = "__mask";
/// The "one" side's uniqueness check.
const CHECK: &str = "__check";
/// How many series a regroup collapsed into one.
const COUNT: &str = "__count";

/// The operators a binary expression can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    And,
    Or,
    Unless,
}

impl Op {
    pub fn parse(it: ItemType) -> Option<Op> {
        Some(match it {
            ItemType::Add => Op::Add,
            ItemType::Sub => Op::Sub,
            ItemType::Mul => Op::Mul,
            ItemType::Div => Op::Div,
            ItemType::Mod => Op::Mod,
            ItemType::Pow => Op::Pow,
            ItemType::Atan2 => Op::Atan2,
            ItemType::EqlC => Op::Eq,
            ItemType::Neq => Op::Ne,
            ItemType::Gtr => Op::Gt,
            ItemType::Lss => Op::Lt,
            ItemType::Gte => Op::Ge,
            ItemType::Lte => Op::Le,
            ItemType::Land => Op::And,
            ItemType::Lor => Op::Or,
            ItemType::Lunless => Op::Unless,
            _ => return None,
        })
    }

    pub fn parse_str(s: &str) -> Option<Op> {
        Some(match s {
            "+" => Op::Add,
            "-" => Op::Sub,
            "*" => Op::Mul,
            "/" => Op::Div,
            "%" => Op::Mod,
            "^" => Op::Pow,
            "atan2" => Op::Atan2,
            "==" => Op::Eq,
            "!=" => Op::Ne,
            ">" => Op::Gt,
            "<" => Op::Lt,
            ">=" => Op::Ge,
            "<=" => Op::Le,
            "and" => Op::And,
            "or" => Op::Or,
            "unless" => Op::Unless,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Add => "+",
            Op::Sub => "-",
            Op::Mul => "*",
            Op::Div => "/",
            Op::Mod => "%",
            Op::Pow => "^",
            Op::Atan2 => "atan2",
            Op::Eq => "==",
            Op::Ne => "!=",
            Op::Gt => ">",
            Op::Lt => "<",
            Op::Ge => ">=",
            Op::Le => "<=",
            Op::And => "and",
            Op::Or => "or",
            Op::Unless => "unless",
        }
    }

    pub fn is_comparison(&self) -> bool {
        matches!(self, Op::Eq | Op::Ne | Op::Gt | Op::Lt | Op::Ge | Op::Le)
    }

    pub fn is_set(&self) -> bool {
        matches!(self, Op::And | Op::Or | Op::Unless)
    }

    /// `changesMetricSchema`: the operators whose result is no longer the
    /// metric it came from, so it loses its name. `bool` drops the name
    /// too, but that is the modifier's doing, not the operator's.
    pub fn changes_metric_schema(&self) -> bool {
        matches!(
            self,
            Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod | Op::Pow | Op::Atan2
        )
    }
}

/// How many series on each side one match may join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Card {
    OneToOne,
    ManyToOne,
    ManyToMany,
}

impl Card {
    pub fn as_str(&self) -> &'static str {
        match self {
            Card::OneToOne => "one-to-one",
            Card::ManyToOne => "many-to-one",
            Card::ManyToMany => "many-to-many",
        }
    }

    pub fn parse(s: &str) -> Option<Card> {
        Some(match s {
            "one-to-one" => Card::OneToOne,
            "many-to-one" => Card::ManyToOne,
            "many-to-many" => Card::ManyToMany,
            _ => return None,
        })
    }
}

/// What the two operands are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    VectorVector,
    /// One side is a scalar; `scalar_is_lhs` is which.
    VectorScalar {
        scalar_is_lhs: bool,
    },
    ScalarScalar,
}

/// A binary expression with every default filled in and every rule
/// upstream's `checkAST` enforces already checked.
#[derive(Debug, Clone)]
pub struct Binop {
    pub op: Op,
    pub kind: Kind,
    pub return_bool: bool,
    pub card: Card,
    /// The `on(…)` or `ignoring(…)` names.
    pub labels: Vec<String>,
    pub on: bool,
    /// The `group_left(…)` / `group_right(…)` names, taken from the one
    /// side.
    pub include: Vec<String>,
    /// `group_right`: the high-cardinality side is the right operand.
    pub many_is_rhs: bool,
}

impl Binop {
    /// Whether the result keeps `__name__`.
    fn drops_metric_name(&self) -> bool {
        self.op.changes_metric_schema() || self.return_bool
    }

    /// Which operand the "one" side was written on, as the duplicate
    /// message names it.
    fn one_side(&self) -> &'static str {
        if self.many_is_rhs {
            "left"
        } else {
            "right"
        }
    }
}

fn query<S: Into<String>>(message: S) -> EngineError {
    EngineError::Query(message.into())
}

/// Fill in the defaults the parser leaves out and apply upstream's
/// `checkAST` rules for a binary expression, in upstream's order.
pub fn normalize(be: &BinaryExpr) -> Result<Binop, EngineError> {
    let op = Op::parse(be.op).ok_or_else(|| {
        query(format!(
            "binary expression does not support operator \"{}\"",
            be.op
        ))
    })?;
    let (lt, rt) = (be.lhs.value_type(), be.rhs.value_type());

    if be.return_bool && !op.is_comparison() {
        return Err(query(
            "bool modifier can only be used on comparison operators",
        ));
    }
    if op.is_comparison() && !be.return_bool && lt == ValueType::Scalar && rt == ValueType::Scalar {
        return Err(query("comparisons between scalars must use BOOL modifier"));
    }

    let mut matching = be.vector_matching.clone().unwrap_or_default();
    // A set operator written without a modifier is many-to-many, which is
    // the only cardinality it may have.
    if op.is_set() && matching.card == VectorMatchCardinality::OneToOne {
        matching.card = VectorMatchCardinality::ManyToMany;
    }
    if matching.on {
        for name in &matching.matching_labels {
            if matching.include.contains(name) {
                return Err(query(format!(
                    "label \"{name}\" must not occur in ON and GROUP clause at once"
                )));
            }
        }
    }
    for t in [lt, rt] {
        if t != ValueType::Scalar && t != ValueType::Vector {
            return Err(query(
                "binary expression must contain only scalar and instant vector types",
            ));
        }
    }

    let both_vectors = lt == ValueType::Vector && rt == ValueType::Vector;
    if !both_vectors {
        if !matching.matching_labels.is_empty() {
            return Err(query(
                "vector matching only allowed between instant vectors",
            ));
        }
        if has_fill(&matching) {
            return Err(query(
                "filling in missing series only allowed between instant vectors",
            ));
        }
        matching = VectorMatching::default();
    } else if op.is_set() {
        if matches!(
            matching.card,
            VectorMatchCardinality::OneToMany | VectorMatchCardinality::ManyToOne
        ) {
            return Err(query(format!(
                "no grouping allowed for \"{}\" operation",
                be.op
            )));
        }
        if matching.card != VectorMatchCardinality::ManyToMany {
            return Err(query("set operations must always be many-to-many"));
        }
        if has_fill(&matching) {
            return Err(query(
                "filling in missing series not allowed for set operators",
            ));
        }
    }
    if (lt == ValueType::Scalar || rt == ValueType::Scalar) && op.is_set() {
        return Err(query(format!(
            "set operator \"{}\" not allowed in binary scalar expression",
            be.op
        )));
    }
    if has_fill(&matching) {
        return Err(EngineError::Unsupported(
            "the fill, fill_left and fill_right modifiers".into(),
        ));
    }

    let kind = match (lt, rt) {
        (ValueType::Vector, ValueType::Vector) => Kind::VectorVector,
        (ValueType::Scalar, ValueType::Vector) => Kind::VectorScalar {
            scalar_is_lhs: true,
        },
        (ValueType::Vector, ValueType::Scalar) => Kind::VectorScalar {
            scalar_is_lhs: false,
        },
        _ => Kind::ScalarScalar,
    };
    // Upstream swaps the operands for `group_right` so that the "one"
    // side is always the right one, and accounts for the swap when it
    // evaluates the values.
    let (card, many_is_rhs) = match matching.card {
        VectorMatchCardinality::OneToOne => (Card::OneToOne, false),
        VectorMatchCardinality::ManyToOne => (Card::ManyToOne, false),
        VectorMatchCardinality::OneToMany => (Card::ManyToOne, true),
        VectorMatchCardinality::ManyToMany => (Card::ManyToMany, false),
    };
    Ok(Binop {
        op,
        kind,
        return_bool: be.return_bool,
        card,
        labels: matching.matching_labels,
        on: matching.on,
        include: matching.include,
        many_is_rhs,
    })
}

fn has_fill(matching: &VectorMatching) -> bool {
    matching.fill_values.lhs.is_some() || matching.fill_values.rhs.is_some()
}

fn sorted(mut names: Vec<String>) -> Vec<String> {
    names.sort();
    names.dedup();
    names
}

/// The labels two series must agree on to match: `signatureFunc`.
///
/// `on` names them; otherwise they are everything both sides carry except
/// the ignored names and `__name__`. A name only one side has still
/// counts, because absent is `""` on the other and the two only match
/// when both are absent.
pub fn matching_labels(binop: &Binop, many: &[String], one: &[String]) -> Vec<String> {
    if binop.on {
        return sorted(binop.labels.clone());
    }
    sorted(
        many.iter()
            .chain(one)
            .filter(|n| n.as_str() != METRIC_NAME && !binop.labels.contains(n))
            .cloned()
            .collect(),
    )
}

/// The labels the result carries: `resultMetric`.
///
/// One-to-one keeps the matching labels and nothing else — literally so
/// for `on`, by subtraction for `ignoring`. A `group_x` keeps the whole
/// many side and splices in the included labels from the one side.
pub fn result_labels(binop: &Binop, many: &[String], one: &[String]) -> Vec<String> {
    let mut names: Vec<String> = match binop.card {
        Card::OneToOne if binop.on => binop.labels.clone(),
        Card::OneToOne => many
            .iter()
            .filter(|n| !binop.labels.contains(n))
            .cloned()
            .collect(),
        // Set operators pass both sides' labels through untouched.
        Card::ManyToMany => many.iter().chain(one).cloned().collect(),
        Card::ManyToOne => many.to_vec(),
    };
    if binop.drops_metric_name() {
        names.retain(|n| n != METRIC_NAME);
    }
    names.extend(binop.include.iter().cloned());
    sorted(names)
}

/// An absent label, in the leaf type the join keys compare in.
fn absent() -> DfExpr {
    lit(ScalarValue::Utf8View(Some(String::new())))
}

/// One label out of a plan's `labels` struct, or `""` if that plan has no
/// such label.
fn label_of(source: &DfExpr, have: &[String], name: &str) -> DfExpr {
    if have.iter().any(|h| h == name) {
        get_field(source.clone(), name)
    } else {
        absent()
    }
}

/// A `labels` struct over exactly `want`, read out of `source`.
fn labels_over(source: &DfExpr, have: &[String], want: &[String]) -> DfExpr {
    labels::call(
        want.iter()
            .map(|n| (n.clone(), label_of(source, have, n)))
            .collect(),
    )
}

/// What a side of a join calls its columns.
///
/// A side is both aliased and renamed, and it needs both. The alias
/// qualifies what the side is built on; the names distinguish what the
/// side projects. DataFusion's leaf-expression pushdown merges a
/// struct-field access into the projection below it and, doing so, adds
/// that projection's own source columns alongside the aliased ones
/// (`extract_leaf_expressions.rs:653`). So both sides' sources surface in
/// the join schema, and without the alias they surface as two columns
/// called `samples`.
struct Side(&'static str);

const MANY: Side = Side("many");
const ONE: Side = Side("one");
const CHECK_SIDE: Side = Side("check");

impl Side {
    fn labels(&self) -> String {
        format!("__{}_labels", self.0)
    }

    fn samples(&self) -> String {
        format!("__{}_samples", self.0)
    }

    /// The `index`th matching label. Numbered rather than named so that
    /// a label called `labels` cannot collide with one.
    fn key(&self, index: usize) -> String {
        format!("__{}_k{index}", self.0)
    }

    fn keys(&self, count: usize) -> Vec<Column> {
        (0..count).map(|i| Column::from_name(self.key(i))).collect()
    }

    fn label_column(&self) -> DfExpr {
        col(self.labels())
    }
}

/// The uniqueness check, carried into the group so that pruning an unused
/// column cannot delete the check with it. The accumulator never reads
/// the value.
fn check_column(present: bool) -> DfExpr {
    if present {
        // Bare, not `col(CHECK).alias(CHECK)`: an alias onto its own
        // name is a different expression to DataFusion than the column
        // is, and the leaf-expression merge adds the column back beside
        // it (`extract_leaf_expressions.rs:653`), duplicating the field.
        col(CHECK)
    } else {
        lit(ScalarValue::Null).alias(CHECK)
    }
}

impl Planner<'_> {
    /// A binary expression, dispatched on what its operands are.
    pub(crate) async fn binary(&mut self, be: &BinaryExpr) -> Result<Planned, EngineError> {
        let binop = normalize(be)?;
        match binop.kind {
            Kind::VectorVector if binop.op.is_set() => self.set_operator(be, &binop).await,
            Kind::VectorVector => self.vector_binop(be, &binop).await,
            _ => self.scalar_binop(be, &binop).await,
        }
    }

    /// A number literal is a series: one row, no labels, a sample at
    /// every step. `NumberLiteral` in upstream's `eval`.
    pub(crate) fn number(&self, value: f64) -> Result<Planned, EngineError> {
        let q = self.query;
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                labels::call(vec![]).alias(LABELS),
                combine::literal_call(value, q.start_ms, q.end_ms, q.step_ms).alias(SAMPLES),
            ])?
            .build()?;
        Ok(Planned {
            plan,
            label_names: Vec::new(),
        })
    }

    /// `-x` negates and drops `__name__`, which is exactly `x * -1`.
    /// `+x` is the operand.
    pub(crate) async fn unary(&mut self, ue: &UnaryExpr) -> Result<Planned, EngineError> {
        match ue.op {
            ItemType::Add => self.expr(&ue.expr, None).await,
            ItemType::Sub => {
                let operand = self.expr(&ue.expr, None).await?;
                let binop = Binop {
                    op: Op::Mul,
                    kind: match ue.expr.value_type() {
                        ValueType::Scalar => Kind::ScalarScalar,
                        _ => Kind::VectorScalar {
                            scalar_is_lhs: false,
                        },
                    },
                    return_bool: false,
                    card: Card::OneToOne,
                    labels: Vec::new(),
                    on: false,
                    include: Vec::new(),
                    many_is_rhs: false,
                };
                let minus_one = self.number(-1.0)?;
                let dropped_name = operand.label_names.iter().any(|n| n == METRIC_NAME);
                let negated = self.combine_with_scalar(operand, minus_one, &binop, false)?;
                if dropped_name {
                    self.unique_series(negated)
                } else {
                    Ok(negated)
                }
            }
            other => Err(EngineError::Unsupported(format!(
                "the unary {other} operator"
            ))),
        }
    }

    /// Negation is the one operation Prometheus checks for collisions:
    /// it drops `__name__` and then refuses a result holding two series
    /// with the same label set. Regrouping by what is left finds them —
    /// a group of more than one is the collision — and the rest of the
    /// language is happy to merge such series silently, so this runs
    /// nowhere else, and not even here unless there was a `__name__` to
    /// drop.
    fn unique_series(&self, negated: Planned) -> Result<Planned, EngineError> {
        let keys = negated.label_names;
        let plan = LogicalPlanBuilder::from(negated.plan)
            .aggregate(
                labels::group_exprs(&keys),
                vec![
                    first_value(col(SAMPLES), Vec::new()).alias(SAMPLES),
                    count(lit(1i64)).alias(COUNT),
                ],
            )?
            .project(vec![
                labels::regroup(&keys).alias(LABELS),
                combine::unique_call(col(SAMPLES), col(COUNT)).alias(SAMPLES),
            ])?
            .build()?;
        Ok(Planned {
            plan,
            label_names: keys,
        })
    }

    /// A scalar on one side: a cross join against the one-row plan, and
    /// the vector's labels carried through. `VectorscalarBinop`.
    async fn scalar_binop(
        &mut self,
        be: &BinaryExpr,
        binop: &Binop,
    ) -> Result<Planned, EngineError> {
        let lhs = self.expr(&be.lhs, None).await?;
        let rhs = self.expr(&be.rhs, None).await?;
        let scalar_is_lhs = matches!(
            binop.kind,
            Kind::VectorScalar {
                scalar_is_lhs: true
            }
        );
        let (vector, scalar) = if scalar_is_lhs {
            (rhs, lhs)
        } else {
            (lhs, rhs)
        };
        self.combine_with_scalar(vector, scalar, binop, scalar_is_lhs)
    }

    fn combine_with_scalar(
        &self,
        vector: Planned,
        scalar: Planned,
        binop: &Binop,
        swap: bool,
    ) -> Result<Planned, EngineError> {
        let vector_side = rename(vector.plan, &MANY)?;
        let scalar_side = rename(scalar.plan, &ONE)?;
        let joined = LogicalPlanBuilder::from(vector_side).cross_join(scalar_side)?;
        let source = MANY.label_column();
        let (labels_expr, label_names) = if binop.drops_metric_name() {
            let names: Vec<String> = vector
                .label_names
                .iter()
                .filter(|n| n.as_str() != METRIC_NAME)
                .cloned()
                .collect();
            (labels_over(&source, &vector.label_names, &names), names)
        } else {
            (source, vector.label_names)
        };
        let plan = joined
            .project(vec![
                labels_expr.alias(LABELS),
                combine::scalar_call(
                    col(MANY.samples()),
                    col(ONE.samples()),
                    binop.op,
                    binop.return_bool,
                    swap,
                )
                .alias(SAMPLES),
            ])?
            .build()?;
        Ok(Planned { plan, label_names })
    }

    /// Two instant vectors: join on the matching labels, then group the
    /// matched pairs by the labels the result carries.
    async fn vector_binop(
        &mut self,
        be: &BinaryExpr,
        binop: &Binop,
    ) -> Result<Planned, EngineError> {
        let lhs = self.expr(&be.lhs, None).await?;
        let rhs = self.expr(&be.rhs, None).await?;
        let (many, one) = if binop.many_is_rhs {
            (rhs, lhs)
        } else {
            (lhs, rhs)
        };
        let keys = matching_labels(binop, &many.label_names, &one.label_names);
        let result = result_labels(binop, &many.label_names, &one.label_names);
        let (many_names, one_names) = (many.label_names.clone(), one.label_names.clone());

        let one_keyed = with_keys(one, &ONE, &keys)?;
        let check = self.unique_one_side(&one_keyed, &keys, binop, &one_names)?;
        let check_present = check.is_some();
        let joined = self.join_on(
            with_keys(many, &MANY, &keys)?,
            one_keyed,
            &keys,
            JoinType::Inner,
        )?;
        let joined = match check {
            Some(check) => self.join_check(joined, check, &keys)?,
            None => joined,
        };

        let (many_labels, one_labels) = (MANY.label_column(), ONE.label_column());
        let result_expr = labels::call(
            result
                .iter()
                .map(|n| {
                    let value = if binop.include.contains(n) {
                        label_of(&one_labels, &one_names, n)
                    } else {
                        label_of(&many_labels, &many_names, n)
                    };
                    (n.clone(), value)
                })
                .collect(),
        );
        let plan = joined
            .project(vec![
                result_expr.alias(LABELS),
                combine::match_call(
                    col(MANY.samples()),
                    col(ONE.samples()),
                    binop.op,
                    binop.return_bool,
                    binop.many_is_rhs,
                )
                .alias(MATCHES),
                one_labels.alias(ONE_LABELS),
                labels_over(&ONE.label_column(), &one_names, &keys).alias(MATCH_GROUP),
                check_column(check_present),
            ])?
            .build()?;
        self.regroup(plan, &result, binop)
    }

    /// The "one" side must hold at most one series per match group per
    /// step, and Prometheus says so while indexing that side, before it
    /// looks at the many side at all.
    ///
    /// Grouping the result already catches this whenever the colliding
    /// pairs share an output series, which is every case except one: a
    /// `group_left (l)` whose duplicates disagree about `l` lands them in
    /// *different* output series, and nothing there would ever compare
    /// them. So the check is built only when there are included labels,
    /// which is also the only time the "one" side is read twice.
    ///
    /// It is the same regrouping aggregate, run over the one side alone
    /// and keyed by the match group: two series with one signature that
    /// both reach a step is exactly the collision it reports.
    fn unique_one_side(
        &self,
        one: &LogicalPlan,
        keys: &[String],
        binop: &Binop,
        one_names: &[String],
    ) -> Result<Option<LogicalPlan>, EngineError> {
        if binop.include.is_empty() {
            return Ok(None);
        }
        let q = self.query;
        let mut exprs = vec![
            combine::filter_call(col(ONE.samples()), None).alias(MATCHES),
            ONE.label_column().alias(ONE_LABELS),
            labels_over(&ONE.label_column(), one_names, keys).alias(MATCH_GROUP),
        ];
        exprs.extend((0..keys.len()).map(|i| col(ONE.key(i)).alias(CHECK_SIDE.key(i))));
        Ok(Some(
            LogicalPlanBuilder::from(one.clone())
                .project(exprs)?
                .aggregate(
                    (0..keys.len())
                        .map(|i| col(CHECK_SIDE.key(i)))
                        .collect::<Vec<_>>(),
                    vec![group::call(
                        col(MATCHES),
                        col(ONE_LABELS),
                        col(MATCH_GROUP),
                        lit(ScalarValue::Null),
                        Card::ManyToOne,
                        binop.one_side(),
                        q.start_ms,
                        q.end_ms,
                        q.step_ms,
                    )
                    .alias(CHECK)],
                )?
                .build()?,
        ))
    }

    /// Hang the uniqueness check off the join, so the plan cannot be
    /// optimised into one that never runs it.
    fn join_check(
        &self,
        joined: LogicalPlanBuilder,
        check: LogicalPlan,
        keys: &[String],
    ) -> Result<LogicalPlanBuilder, EngineError> {
        if keys.is_empty() {
            return Ok(joined.cross_join(check)?);
        }
        Ok(joined.join(
            check,
            JoinType::Inner,
            (MANY.keys(keys.len()), CHECK_SIDE.keys(keys.len())),
            None,
        )?)
    }

    /// `and`, `or`, `unless`: no values cross over, only presence. The
    /// other side collapses to one row per match group holding the steps
    /// it covers, and the join decides what that absence means.
    async fn set_operator(
        &mut self,
        be: &BinaryExpr,
        binop: &Binop,
    ) -> Result<Planned, EngineError> {
        let lhs = self.expr(&be.lhs, None).await?;
        let rhs = self.expr(&be.rhs, None).await?;
        let keys = matching_labels(binop, &lhs.label_names, &rhs.label_names);
        let result = result_labels(binop, &lhs.label_names, &rhs.label_names);

        let plan = match binop.op {
            Op::And | Op::Unless => {
                let keep_present = binop.op == Op::And;
                let join = if keep_present {
                    JoinType::Inner
                } else {
                    JoinType::Left
                };
                let names = lhs.label_names.clone();
                let mask = self.mask(rhs, &keys)?;
                let joined = self.join_on(with_keys(lhs, &MANY, &keys)?, mask, &keys, join)?;
                joined
                    .project(self.set_projection(
                        labels_over(&MANY.label_column(), &names, &result),
                        combine::filter_call(col(MANY.samples()), Some((col(MASK), keep_present))),
                    ))?
                    .build()?
            }
            _ => {
                // `or` is every left series, plus the right ones at the
                // steps no left series with their signature covers.
                let (lhs_names, rhs_names) = (lhs.label_names.clone(), rhs.label_names.clone());
                let mask = self.mask(lhs.clone(), &keys)?;
                let left = LogicalPlanBuilder::from(lhs.plan)
                    .project(self.set_projection(
                        labels_over(&col(LABELS), &lhs_names, &result),
                        combine::filter_call(col(SAMPLES), None),
                    ))?
                    .build()?;
                let right = self
                    .join_on(with_keys(rhs, &MANY, &keys)?, mask, &keys, JoinType::Left)?
                    .project(self.set_projection(
                        labels_over(&MANY.label_column(), &rhs_names, &result),
                        combine::filter_call(col(MANY.samples()), Some((col(MASK), false))),
                    ))?
                    .build()?;
                LogicalPlanBuilder::from(left).union(right)?.build()?
            }
        };
        self.regroup(plan, &result, binop)
    }

    /// What every branch of a set operator hands the regrouping: the
    /// labels it keeps and the steps it covers. The last two columns are
    /// the duplicate-match machinery, which set operators have no use
    /// for; both sides of a `union` must agree on the shape.
    fn set_projection(&self, labels_expr: DfExpr, matches: DfExpr) -> Vec<DfExpr> {
        vec![
            labels_expr.alias(LABELS),
            matches.alias(MATCHES),
            labels::call(vec![]).alias(ONE_LABELS),
            labels::call(vec![]).alias(MATCH_GROUP),
            check_column(false),
        ]
    }

    /// One row per match group on `side`, holding the steps it has a
    /// sample at. `group` is exactly that: one at every step the group
    /// reaches, nothing where it does not.
    fn mask(&self, side: Planned, keys: &[String]) -> Result<LogicalPlan, EngineError> {
        let q = self.query;
        let mut exprs: Vec<DfExpr> = keys
            .iter()
            .enumerate()
            .map(|(i, n)| label_of(&col(LABELS), &side.label_names, n).alias(ONE.key(i)))
            .collect();
        exprs.push(col(SAMPLES));
        Ok(LogicalPlanBuilder::from(side.plan)
            .project(exprs)?
            .aggregate(
                (0..keys.len()).map(|i| col(ONE.key(i))).collect::<Vec<_>>(),
                vec![aggregate::call(
                    col(SAMPLES),
                    aggregate::Op::Group,
                    q.start_ms,
                    q.end_ms,
                    q.step_ms,
                )
                .alias(MASK)],
            )?
            .build()?)
    }

    /// The join that lines up two sides on their matching labels. With no
    /// matching labels at all — `on ()` — every row matches every row.
    fn join_on(
        &self,
        many: LogicalPlan,
        one: LogicalPlan,
        keys: &[String],
        join_type: JoinType,
    ) -> Result<LogicalPlanBuilder, EngineError> {
        let many = LogicalPlanBuilder::from(many);
        if keys.is_empty() {
            return Ok(many.cross_join(one)?);
        }
        Ok(many.join(
            one,
            join_type,
            (MANY.keys(keys.len()), ONE.keys(keys.len())),
            None,
        )?)
    }

    /// Matched pairs into series, and the cardinality rules with them.
    fn regroup(
        &self,
        plan: LogicalPlan,
        result: &[String],
        binop: &Binop,
    ) -> Result<Planned, EngineError> {
        let q = self.query;
        let plan = LogicalPlanBuilder::from(plan)
            .aggregate(
                labels::group_exprs(result),
                vec![group::call(
                    col(MATCHES),
                    col(ONE_LABELS),
                    col(MATCH_GROUP),
                    col(CHECK),
                    binop.card,
                    binop.one_side(),
                    q.start_ms,
                    q.end_ms,
                    q.step_ms,
                )
                .alias(SAMPLES)],
            )?
            .project(vec![labels::regroup(result).alias(LABELS), col(SAMPLES)])?
            .build()?;
        Ok(Planned {
            plan,
            label_names: result.to_vec(),
        })
    }
}

/// A plan's two columns under one side's names, so that a join carries
/// both sides without a qualifier to tell them apart.
fn rename(plan: LogicalPlan, side: &Side) -> Result<LogicalPlan, EngineError> {
    Ok(LogicalPlanBuilder::from(plan)
        .alias(side.0)?
        .project(vec![
            col(LABELS).alias(side.labels()),
            col(SAMPLES).alias(side.samples()),
        ])?
        .build()?)
}

/// A side of a join: its two columns renamed, plus its matching labels
/// lifted out as the columns the join compares.
fn with_keys(side: Planned, name: &Side, keys: &[String]) -> Result<LogicalPlan, EngineError> {
    let mut exprs = vec![
        col(LABELS).alias(name.labels()),
        col(SAMPLES).alias(name.samples()),
    ];
    for (i, label) in keys.iter().enumerate() {
        exprs.push(label_of(&col(LABELS), &side.label_names, label).alias(name.key(i)));
    }
    Ok(LogicalPlanBuilder::from(side.plan)
        .alias(name.0)?
        .project(exprs)?
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::ast::Expr;

    fn parse(query: &str) -> BinaryExpr {
        match promql_parser::parse_expr(query).expect("parses") {
            Expr::Binary(b) => b,
            other => panic!("not a binary expression: {other:?}"),
        }
    }

    fn normalized(query: &str) -> Binop {
        normalize(&parse(query)).expect("accepted")
    }

    fn rejected(query: &str) -> String {
        match normalize(&parse(query)) {
            Err(EngineError::Query(message)) => message,
            other => panic!("{query}: expected a query error, got {other:?}"),
        }
    }

    fn names(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    #[test]
    fn a_plain_operator_is_one_to_one_ignoring_the_name() {
        let b = normalized("foo + bar");
        assert_eq!(b.op, Op::Add);
        assert_eq!(b.card, Card::OneToOne);
        assert!(!b.on);
        assert!(b.labels.is_empty());
        assert_eq!(b.kind, Kind::VectorVector);
        // Nothing is named, so the signature is every label both sides
        // carry except the metric name.
        assert_eq!(
            matching_labels(
                &b,
                &names(&["__name__", "pod"]),
                &names(&["__name__", "pod"])
            ),
            names(&["pod"])
        );
    }

    #[test]
    fn a_set_operator_defaults_to_many_to_many() {
        assert_eq!(normalized("foo and bar").card, Card::ManyToMany);
        assert_eq!(normalized("foo or bar").card, Card::ManyToMany);
        assert_eq!(normalized("foo unless bar").card, Card::ManyToMany);
    }

    #[test]
    fn group_right_makes_the_right_side_the_many_side() {
        let b = normalized("foo * on (code) group_right (path) bar");
        assert_eq!(b.card, Card::ManyToOne);
        assert!(b.many_is_rhs);
        assert_eq!(b.one_side(), "left");
        assert_eq!(b.include, names(&["path"]));

        let b = normalized("foo * on (code) group_left (path) bar");
        assert_eq!(b.card, Card::ManyToOne);
        assert!(!b.many_is_rhs);
        assert_eq!(b.one_side(), "right");
    }

    /// Arithmetic and `bool` lose the metric name; a plain comparison
    /// keeps it, because it is a filter over the series it came from.
    #[test]
    fn only_some_operators_drop_the_metric_name() {
        assert!(normalized("foo + bar").drops_metric_name());
        assert!(normalized("foo == bool bar").drops_metric_name());
        assert!(!normalized("foo == bar").drops_metric_name());
        assert!(!normalized("foo and bar").drops_metric_name());
    }

    #[test]
    fn one_to_one_keeps_the_matching_labels_and_nothing_else() {
        let b = normalized("foo + on (code) bar");
        assert_eq!(
            result_labels(&b, &names(&["__name__", "code", "pod"]), &names(&["code"])),
            names(&["code"])
        );
        let b = normalized("foo + ignoring (pod) bar");
        assert_eq!(
            result_labels(&b, &names(&["__name__", "code", "pod"]), &names(&["code"])),
            names(&["code"])
        );
    }

    /// `group_left` keeps the whole many side and splices in the labels
    /// named after it, taken from the one side.
    #[test]
    fn group_left_keeps_the_many_side_plus_the_included_labels() {
        let b = normalized("foo * on (pod) group_left (ns) bar");
        assert_eq!(
            result_labels(&b, &names(&["__name__", "pod"]), &names(&["ns", "pod"])),
            names(&["ns", "pod"])
        );
    }

    /// A comparison keeps `__name__`, so a one-to-one comparison's result
    /// is the matching labels plus the name it came from.
    #[test]
    fn a_comparison_keeps_the_name_it_filters() {
        let b = normalized("foo > ignoring (pod) bar");
        assert_eq!(
            result_labels(&b, &names(&["__name__", "code", "pod"]), &names(&["code"])),
            names(&["__name__", "code"])
        );
    }

    #[test]
    fn set_operators_pass_both_sides_labels_through() {
        let b = normalized("foo or bar");
        assert_eq!(
            result_labels(
                &b,
                &names(&["__name__", "pod"]),
                &names(&["__name__", "ns"])
            ),
            names(&["__name__", "ns", "pod"])
        );
    }

    #[test]
    fn upstreams_rules_are_query_errors() {
        assert_eq!(
            rejected("foo + bool bar"),
            "bool modifier can only be used on comparison operators"
        );
        assert_eq!(
            rejected("1 == 1"),
            "comparisons between scalars must use BOOL modifier"
        );
        assert_eq!(
            rejected("foo * on (code) group_left (code) bar"),
            "label \"code\" must not occur in ON and GROUP clause at once"
        );
        assert_eq!(
            rejected("foo and on (code) group_left (path) bar"),
            "no grouping allowed for \"and\" operation"
        );
        assert_eq!(
            rejected("foo and 1"),
            "set operator \"and\" not allowed in binary scalar expression"
        );
        assert_eq!(
            rejected("foo + on (code) 1"),
            "vector matching only allowed between instant vectors"
        );
    }

    /// A modifier on a scalar operand is dropped rather than carried, so
    /// nothing downstream has to ask whether it applies.
    #[test]
    fn a_scalar_operand_has_no_matching() {
        let b = normalized("foo + 1");
        assert_eq!(
            b.kind,
            Kind::VectorScalar {
                scalar_is_lhs: false
            }
        );
        assert!(b.labels.is_empty());
        let b = normalized("1 + foo");
        assert_eq!(
            b.kind,
            Kind::VectorScalar {
                scalar_is_lhs: true
            }
        );
        assert_eq!(normalized("1 + 2").kind, Kind::ScalarScalar);
    }
}
