//! The binary operators between two vectors, and the matching that
//! decides which series pair, as an aggregate function:
//! `promql_binary(samples, __rhs, labels, '+', 'on(job) group_left()', start, end, step)`.
//!
//! [`Op`] is the arithmetic itself, upstream's `vectorElemBinop`
//! (`promql/engine.go:3257` at 83962c35) restricted to the float/float
//! arms. It is shared by all three shapes the operators take: a scalar
//! on both sides folds through [`crate::scalar`], a scalar on one side
//! rides the elementwise kernel, and two vectors come here.
//!
//! # Why an aggregation and not a join
//!
//! Matching pairs a left series with a right one when their labels agree
//! on the match signature — the `on` labels, or everything but
//! `__name__` and the `ignoring` ones. Grouping both sides by that
//! signature gives the pairing, the duplicate detection and the
//! `group_left` fan-out in a single node, where a join would give the
//! pairing and still need an aggregation to find the duplicates, which
//! upstream reports per step rather than per series.
//!
//! # What a group holds
//!
//! One side of a match is the "one": at most one of its series may be
//! in a group at any step, and [`Pairing`] keeps it as lanes over the
//! step grid, a count and a value per step. The other side is the
//! "many" — for `group_left` it really is many — so its series are kept
//! one lane pair each. That is the cost of this shape: a group as wide
//! as a `group_left` fan-out holds a lane per series of it.
//!
//! # Why the result is a list of series
//!
//! A match group is not one output series. `group_left` answers with a
//! series per left-hand one, and even plain one-to-one splits when the
//! operator keeps `__name__`, which is exactly the label the signature
//! forgets. So the aggregation builds the result label set upstream's
//! `resultMetric` would, groups the samples under it, and hands back a
//! `(labels, samples)` pair per distinct one; the planner unnests that
//! into a row each. Two series of the "many" side reaching the same
//! result labels at one step is upstream's matching error, in its own
//! words — which words depends on the cardinality.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray, StringArray, StringViewArray,
    StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, Int64Type};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;
use promql_parser::token::ItemType;

use crate::aggregate::Grid;
use crate::matcher::METRIC_NAME;
use crate::series;

pub const NAME: &str = "promql_binary";

/// The column telling the aggregation which operand a row came from.
///
/// Both sides are unioned into one table so that a single grouping
/// finds the match groups; this is the only thing that survives of
/// which side a series was on.
pub const SIDE: &str = "__rhs";

/// The two sides, as the [`SIDE`] flag spells them.
const LHS: usize = 0;
const RHS: usize = 1;

/// Every binary operator between two vectors.
///
/// The set operators sit here with the rest even though they never read
/// a value: they match on the same signature, so they are the same
/// grouping with a different rule for which samples come out of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eql,
    Neq,
    Gtr,
    Lss,
    Gte,
    Lte,
    And,
    Or,
    Unless,
}

/// Every operator, for the round trips that have to name them all.
const ALL: [Op; 16] = [
    Op::Add,
    Op::Sub,
    Op::Mul,
    Op::Div,
    Op::Mod,
    Op::Pow,
    Op::Atan2,
    Op::Eql,
    Op::Neq,
    Op::Gtr,
    Op::Lss,
    Op::Gte,
    Op::Lte,
    Op::And,
    Op::Or,
    Op::Unless,
];

impl Op {
    pub fn from_token(op: ItemType) -> Option<Op> {
        Some(match op {
            ItemType::Add => Op::Add,
            ItemType::Sub => Op::Sub,
            ItemType::Mul => Op::Mul,
            ItemType::Div => Op::Div,
            ItemType::Mod => Op::Mod,
            ItemType::Pow => Op::Pow,
            ItemType::Atan2 => Op::Atan2,
            ItemType::EqlC => Op::Eql,
            ItemType::Neq => Op::Neq,
            ItemType::Gtr => Op::Gtr,
            ItemType::Lss => Op::Lss,
            ItemType::Gte => Op::Gte,
            ItemType::Lte => Op::Lte,
            ItemType::Land => Op::And,
            ItemType::Lor => Op::Or,
            ItemType::Lunless => Op::Unless,
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
            Op::Eql => "==",
            Op::Neq => "!=",
            Op::Gtr => ">",
            Op::Lss => "<",
            Op::Gte => ">=",
            Op::Lte => "<=",
            Op::And => "and",
            Op::Or => "or",
            Op::Unless => "unless",
        }
    }

    /// The operators upstream's `IsSetOperator` answers for. They read
    /// no value at all: which samples come out is decided by which
    /// signatures the other side has at that step.
    pub fn is_set(&self) -> bool {
        matches!(self, Op::And | Op::Or | Op::Unless)
    }

    /// The operators upstream's `IsComparisonOperator` answers for.
    /// They filter rather than compute: the sample keeps its own value
    /// and is dropped where the comparison does not hold.
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Op::Eql | Op::Neq | Op::Gtr | Op::Lss | Op::Gte | Op::Lte
        )
    }

    /// Whether the result stops being the metric it was computed from.
    ///
    /// Upstream's `changesMetricSchema` (`promql/engine.go:4208` at
    /// 83962c35) names the arithmetic operators and nothing else, so a
    /// comparison keeps `__name__` — it answers with a sample that was
    /// already there. `bool` replaces the value with a 1 or a 0, which
    /// is no longer that metric either, and drops it too.
    pub fn drops_metric_name(&self, return_bool: bool) -> bool {
        match self {
            // A set operator hands a sample back untouched, so there is
            // nothing for `changesMetricSchema` to be true of.
            op if op.is_set() => false,
            op if op.is_comparison() => return_bool,
            _ => true,
        }
    }

    /// One pair of floats, upstream's `vectorElemBinop` with the
    /// `returnBool` wrapper its callers apply: `None` is the sample
    /// upstream leaves out of the result.
    ///
    /// Rust's `%` is Go's `math.Mod` — the remainder takes the sign of
    /// the dividend — and `powf` is `math.Pow`; the division by zero
    /// that yields an infinity or a NaN is IEEE in both languages, so
    /// none of these needs a Go-shaped wrapper the way `clamp`'s
    /// `math.Min` did. A NaN compares false to everything, in Go and in
    /// Rust alike, so it is simply filtered away.
    pub fn value(&self, lhs: f64, rhs: f64, return_bool: bool) -> Option<f64> {
        debug_assert!(!self.is_set(), "a set operator never combines two values");
        if !self.is_comparison() {
            return Some(match self {
                Op::Add => lhs + rhs,
                Op::Sub => lhs - rhs,
                Op::Mul => lhs * rhs,
                Op::Div => lhs / rhs,
                Op::Mod => lhs % rhs,
                Op::Pow => lhs.powf(rhs),
                Op::Atan2 => lhs.atan2(rhs),
                _ => unreachable!("every comparison is handled below, every set operator above"),
            });
        }
        let keep = self.compare(lhs, rhs);
        match (return_bool, keep) {
            (true, keep) => Some(if keep { 1.0 } else { 0.0 }),
            (false, true) => Some(lhs),
            (false, false) => None,
        }
    }

    /// Whether the comparison holds. Meaningless for the arithmetic
    /// operators, which never ask.
    pub fn compare(&self, lhs: f64, rhs: f64) -> bool {
        match self {
            Op::Eql => lhs == rhs,
            Op::Neq => lhs != rhs,
            Op::Gtr => lhs > rhs,
            Op::Lss => lhs < rhs,
            Op::Gte => lhs >= rhs,
            Op::Lte => lhs <= rhs,
            _ => unreachable!("only a comparison compares"),
        }
    }
}

/// How many series on each side one match may pair, upstream's
/// `VectorMatchCardinality` without the many-to-many the set operators
/// alone are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Card {
    #[default]
    OneToOne,
    /// `group_left`: many on the left, one on the right.
    ManyToOne,
    /// `group_right`: one on the left, many on the right.
    OneToMany,
}

/// The `on`/`ignoring` and `group_left`/`group_right` modifiers, which
/// together decide the match signature and the result's labels.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Matching {
    pub card: Card,
    /// `on(…)` rather than `ignoring(…)`.
    pub on: bool,
    /// The labels named by whichever of the two was written.
    pub labels: Vec<String>,
    /// The `group_left(…)` labels, copied from the "one" side.
    pub include: Vec<String>,
}

impl Matching {
    /// The modifiers as the query spelled them, so that a plan reads
    /// back as PromQL. Default matching writes nothing at all.
    pub fn literal(&self) -> String {
        let mut out = String::new();
        if self.on || !self.labels.is_empty() {
            out.push_str(if self.on { "on(" } else { "ignoring(" });
            out.push_str(&self.labels.join(", "));
            out.push(')');
        }
        let group = match self.card {
            Card::OneToOne => return out,
            Card::ManyToOne => "group_left(",
            Card::OneToMany => "group_right(",
        };
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(group);
        out.push_str(&self.include.join(", "));
        out.push(')');
        out
    }

    /// [`Matching::literal`] read back. Deliberately strict: anything
    /// this cannot spell is a plan it did not write.
    pub fn parse(s: &str) -> Option<Matching> {
        let mut out = Matching::default();
        let mut rest = s.trim();
        for (prefix, on) in [("on(", true), ("ignoring(", false)] {
            if let Some(tail) = rest.strip_prefix(prefix) {
                let (names, tail) = tail.split_once(')')?;
                out.on = on;
                out.labels = split_names(names);
                rest = tail.trim_start();
                break;
            }
        }
        if rest.is_empty() {
            return Some(out);
        }
        for (prefix, card) in [
            ("group_left(", Card::ManyToOne),
            ("group_right(", Card::OneToMany),
        ] {
            if let Some(tail) = rest.strip_prefix(prefix) {
                let (names, tail) = tail.split_once(')')?;
                if !tail.trim().is_empty() {
                    return None;
                }
                out.card = card;
                out.include = split_names(names);
                return Some(out);
            }
        }
        None
    }

    /// Which side of the union holds at most one series per group.
    fn one_side(&self) -> usize {
        match self.card {
            Card::OneToMany => LHS,
            _ => RHS,
        }
    }

    /// The word upstream puts in the duplicate message for that side.
    fn one_side_word(&self) -> &'static str {
        match self.card {
            Card::OneToMany => "left",
            _ => "right",
        }
    }
}

fn split_names(names: &str) -> Vec<String> {
    names
        .split(',')
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .map(str::to_string)
        .collect()
}

/// The three ways a match fails, in upstream's words.
///
/// Exported because a UDAF can only fail with a `DataFusionError`, so
/// the engine has to recognise these on the way out to report them as
/// what they are: a query Prometheus also refuses, not a bug in here.
/// [`DUPLICATE_SERIES`] is only the fixed head of its message — the
/// rest names the group and the two series that collided.
pub const DUPLICATE_SERIES: &str = "found duplicate series for the match group";
pub const MULTIPLE_MATCHES: &str =
    "multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)";
pub const GROUPING_NOT_UNIQUE: &str =
    "multiple matches for labels: grouping labels must ensure unique matches";

/// Whether a failure out of [`udaf`] is one of them.
pub fn is_matching_error(message: &str) -> bool {
    message.starts_with(DUPLICATE_SERIES)
        || message == MULTIPLE_MATCHES
        || message == GROUPING_NOT_UNIQUE
}

/// The result label set and the samples under it, the shape [`udaf`]
/// answers with.
pub const PAIR_LABELS: &str = "labels";
pub const PAIR_SAMPLES: &str = "samples";

fn pair_fields(names: &[String]) -> Fields {
    Fields::from(vec![
        Field::new(PAIR_LABELS, series::labels_type(names), false),
        Field::new(PAIR_SAMPLES, series::samples_type(), false),
    ])
}

fn pair_item(names: &[String]) -> FieldRef {
    Arc::new(Field::new(
        "item",
        DataType::Struct(pair_fields(names)),
        false,
    ))
}

/// What one match group answers with: the series it splits into.
pub fn output_type(names: &[String]) -> DataType {
    DataType::List(pair_item(names))
}

/// The operator as a plan carries it: its PromQL spelling, with the
/// modifier written in where the query had one. One literal rather than
/// two arguments, so a plan text reads back as the query it came from.
pub fn literal(op: Op, return_bool: bool) -> &'static str {
    if !return_bool {
        return op.as_str();
    }
    match op {
        Op::Eql => "== bool",
        Op::Neq => "!= bool",
        Op::Gtr => "> bool",
        Op::Lss => "< bool",
        Op::Gte => ">= bool",
        Op::Lte => "<= bool",
        // `bool` is only ever written on a comparison; upstream's
        // parser refuses it anywhere else.
        other => other.as_str(),
    }
}

/// [`literal`] read back.
pub fn parse_literal(s: &str) -> Option<(Op, bool)> {
    let (spelling, return_bool) = match s.strip_suffix(" bool") {
        Some(spelling) => (spelling, true),
        None => (s, false),
    };
    let op = ALL.into_iter().find(|op| op.as_str() == spelling)?;
    // `1 + bool 2` is not a query upstream's parser accepts, so it is
    // not a plan this reads back either.
    if return_bool && !op.is_comparison() {
        return None;
    }
    Some((op, return_bool))
}

/// One series held whole: its labels, and what it put at each step of
/// the grid.
///
/// For the operators that combine two values this is the "many" side
/// and `side` is always the one the modifier allows to be many. A set
/// operator has no "one" side at all, so both operands land here and
/// `side` is what tells them apart.
#[derive(Debug)]
struct Many {
    side: usize,
    labels: Vec<String>,
    counts: Vec<i64>,
    values: Vec<f64>,
}

/// One output series under construction.
#[derive(Debug)]
struct Bucket {
    labels: Vec<String>,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    /// The step this last took a sample at, which is how two series of
    /// the "many" side reaching it at once are caught.
    claimed: Option<usize>,
}

/// One match group: the "one" side as lanes, the "many" side as a
/// series each.
#[derive(Debug)]
struct Pairing {
    op: Op,
    return_bool: bool,
    matching: Matching,
    grid: Grid,
    /// The label columns, in the order the input struct carries them,
    /// which is the order every label vector here is in.
    schema: Vec<String>,
    one_counts: Vec<i64>,
    one_values: Vec<f64>,
    /// Which of [`Pairing::one_pool`] the "one" side's sample at each
    /// step came from, `-1` where it put none. Only the `group_x`
    /// labels are read off it, but they are read per step: two series
    /// may hold the "one" side at different steps without being a
    /// duplicate of each other.
    one_rows: Vec<i32>,
    one_pool: Vec<Vec<String>>,
    many: Vec<Many>,
    /// The match group's own labels, for the message a duplicate has to
    /// name. Every row of the group renders the same text, so the first
    /// one to arrive settles it.
    group: Option<String>,
    /// Up to two label sets from the "one" side, again only for that
    /// message: upstream prints the pair that collided and there is
    /// nothing to say about a third.
    one_metrics: Vec<String>,
}

impl Pairing {
    fn new(op: Op, return_bool: bool, matching: Matching, grid: Grid, schema: Vec<String>) -> Self {
        Self {
            op,
            return_bool,
            matching,
            grid,
            schema,
            one_counts: vec![0; grid.len()],
            one_values: vec![f64::NAN; grid.len()],
            one_rows: vec![-1; grid.len()],
            one_pool: Vec::new(),
            many: Vec::new(),
            group: None,
            one_metrics: Vec::new(),
        }
    }

    fn intern_one(&mut self, labels: &[String]) -> i32 {
        if let Some(index) = self.one_pool.iter().position(|held| held == labels) {
            return index as i32;
        }
        self.one_pool.push(labels.to_vec());
        (self.one_pool.len() - 1) as i32
    }

    fn intern_many(&mut self, side: usize, labels: &[String]) -> usize {
        if let Some(index) = self
            .many
            .iter()
            .position(|held| held.side == side && held.labels == labels)
        {
            return index;
        }
        self.many.push(Many {
            side,
            labels: labels.to_vec(),
            counts: vec![0; self.grid.len()],
            values: vec![f64::NAN; self.grid.len()],
        });
        self.many.len() - 1
    }

    /// One input series folded into the group's lanes.
    fn absorb(
        &mut self,
        side: usize,
        labels: &[String],
        timestamps: &[i64],
        values: &[f64],
    ) -> Result<()> {
        let grid = self.grid;
        if !self.op.is_set() && side == self.matching.one_side() {
            self.remember(labels);
            let row = self.intern_one(labels);
            let (counts, lanes, rows) = (
                &mut self.one_counts,
                &mut self.one_values,
                &mut self.one_rows,
            );
            return grid.runs(timestamps, |index, from, len| {
                for count in &mut counts[index..index + len] {
                    *count += 1;
                }
                lanes[index..index + len].copy_from_slice(&values[from..from + len]);
                rows[index..index + len].fill(row);
            });
        }
        let many = self.intern_many(side, labels);
        let series = &mut self.many[many];
        let (counts, lanes) = (&mut series.counts, &mut series.values);
        grid.runs(timestamps, |index, from, len| {
            for count in &mut counts[index..index + len] {
                *count += 1;
            }
            lanes[index..index + len].copy_from_slice(&values[from..from + len]);
        })
    }

    /// Keep what a failure would have to quote.
    fn remember(&mut self, labels: &[String]) {
        if self.group.is_none() {
            self.group = Some(self.signature_text(labels));
        }
        if self.one_metrics.len() < 2 {
            let metric = print(&self.schema, labels, |_| true);
            if self.one_metrics.first() != Some(&metric) {
                self.one_metrics.push(metric);
            }
        }
    }

    /// The match group as upstream's `MatchLabels` renders it: the `on`
    /// labels, or everything but `__name__` and the `ignoring` ones.
    fn signature_text(&self, labels: &[String]) -> String {
        let m = &self.matching;
        print(&self.schema, labels, |name| {
            if m.on {
                m.labels.iter().any(|l| l == name)
            } else {
                name != METRIC_NAME && !m.labels.iter().any(|l| l == name)
            }
        })
    }

    /// Upstream's `resultMetric` (`promql/engine.go:3132` at 83962c35),
    /// in its order: the metadata labels go first where the operator
    /// changed what the metric means, then one-to-one narrows to the
    /// match labels, then the `group_x` labels are copied over from the
    /// "one" side — or deleted where it has none.
    fn result_labels(&self, many: usize, one: i32) -> Vec<String> {
        let mut out = self.many[many].labels.clone();
        // A set operator answers with the sample it was handed, so its
        // labels are the ones it arrived with -- `resultMetric` is not
        // on that path at all (`promql/engine.go:2888-2961` at
        // 83962c35).
        if self.op.is_set() {
            return out;
        }
        let m = &self.matching;
        for (index, name) in self.schema.iter().enumerate() {
            // `on` keeps only what it names and `ignoring` drops only
            // what it names, which is one test: the label goes where
            // being named is not what the modifier wanted.
            let drop = (name == METRIC_NAME && self.op.drops_metric_name(self.return_bool))
                || (m.card == Card::OneToOne && m.on != m.labels.iter().any(|l| l == name));
            if drop {
                out[index].clear();
            }
        }
        for name in &m.include {
            if let Some(index) = self.schema.iter().position(|held| held == name) {
                out[index] = match one {
                    -1 => String::new(),
                    one => self.one_pool[one as usize][index].clone(),
                };
            }
        }
        out
    }

    /// Upstream's message for two series on the "one" side of a match
    /// (`promql/engine.go:2999` at 83962c35).
    ///
    /// The pair inside the brackets is the first two label sets this
    /// group saw on that side, which is arrival order and not
    /// upstream's pair: upstream quotes the series that collided and
    /// the one it collided with at that step, and once three or more
    /// share a match group those need not be these two. It is the
    /// message text and nothing else, so the tests assert the sentence
    /// around the brackets rather than what is in them — DataFusion is
    /// free to hand the rows over in any order, and an assertion on the
    /// pair would be a flake waiting to happen.
    fn duplicate(&self) -> DataFusionError {
        let group = self.group.as_deref().unwrap_or("{}");
        let side = self.matching.one_side_word();
        // Upstream names the series it collided on first and the one
        // already held second; a side with fewer than two is out of
        // reach here, and an empty string is all there would be to say.
        let second = self.one_metrics.get(1).map(String::as_str).unwrap_or("");
        let first = self.one_metrics.first().map(String::as_str).unwrap_or("");
        DataFusionError::Execution(format!(
            "{DUPLICATE_SERIES} {group} on the {side} hand-side of the \
             operation: [{second}, {first}];many-to-many matching not allowed: matching labels \
             must be unique on one side"
        ))
    }

    /// `and`, `or` and `unless` over the same grid: upstream's
    /// `VectorAnd` / `VectorOr` / `VectorUnless` (`promql/engine.go:2888`
    /// at 83962c35), which ask nothing of a sample but whether the other
    /// side has this signature at this step.
    ///
    /// Every sample that survives keeps its own labels and its own
    /// value, so two of them can only ever land in one output series if
    /// their label sets are equal -- and equal labels are the same
    /// signature, which for `or` means the right-hand one was dropped.
    /// That is why upstream's "vector cannot contain metrics with the
    /// same labelset" has no way to happen here, and why merging by
    /// label set is safe.
    fn sets(&self) -> Vec<Bucket> {
        let mut buckets: Vec<Bucket> = Vec::new();
        let mut by_labels: HashMap<&Vec<String>, usize> = HashMap::new();

        for step in 0..self.grid.len() {
            let present = |side: usize| {
                self.many
                    .iter()
                    .any(|series| series.side == side && series.counts[step] > 0)
            };
            let (left, right) = (present(LHS), present(RHS));
            for series in &self.many {
                if series.counts[step] == 0 {
                    continue;
                }
                let keep = match self.op {
                    Op::And => series.side == LHS && right,
                    Op::Unless => series.side == LHS && !right,
                    // Everything on the left, and the right only where
                    // the left put nothing under this signature.
                    Op::Or => series.side == LHS || !left,
                    _ => unreachable!("only a set operator walks this grid"),
                };
                if !keep {
                    continue;
                }
                let bucket = *by_labels.entry(&series.labels).or_insert_with(|| {
                    buckets.push(Bucket {
                        labels: series.labels.clone(),
                        timestamps: Vec::new(),
                        values: Vec::new(),
                        claimed: None,
                    });
                    buckets.len() - 1
                });
                buckets[bucket].timestamps.push(self.grid.timestamp(step));
                buckets[bucket].values.push(series.values[step]);
            }
        }

        buckets.sort_by(|a, b| a.labels.cmp(&b.labels));
        buckets
    }

    /// The step grid walked once, in upstream's order: a duplicate on
    /// the "one" side is reported before a "many" side that matched
    /// twice, and both before any value is emitted.
    ///
    /// A step whose "many" side is empty is skipped rather than checked,
    /// which is where this parts company with upstream: there the
    /// short-circuit is on the whole vector, so a duplicate in *this*
    /// match group still fails the query as long as some other group
    /// had a sample on the "many" side at that step. Seeing that would
    /// take a second pass over every group.
    fn pairs(&self) -> Result<Vec<Bucket>> {
        if self.op.is_set() {
            return Ok(self.sets());
        }
        let mut buckets: Vec<Bucket> = Vec::new();
        let mut by_labels: HashMap<Vec<String>, usize> = HashMap::new();
        let mut by_pair: HashMap<(usize, i32), usize> = HashMap::new();
        let one_to_one = self.matching.card == Card::OneToOne;

        for step in 0..self.grid.len() {
            let total: i64 = self.many.iter().map(|series| series.counts[step]).sum();
            if total == 0 {
                continue;
            }
            if self.one_counts[step] > 1 {
                return Err(self.duplicate());
            }
            if self.one_counts[step] == 0 {
                continue;
            }
            // One to one is the cardinality that says there is nothing
            // to fan out over, so a second series here is the query
            // asking for a fan-out without saying so.
            if one_to_one && total > 1 {
                return Err(DataFusionError::Execution(MULTIPLE_MATCHES.into()));
            }
            let one = self.one_rows[step];
            for many in 0..self.many.len() {
                if self.many[many].counts[step] == 0 {
                    continue;
                }
                let bucket = match by_pair.get(&(many, one)) {
                    Some(bucket) => *bucket,
                    None => {
                        let labels = self.result_labels(many, one);
                        let bucket = *by_labels.entry(labels.clone()).or_insert_with(|| {
                            buckets.push(Bucket {
                                labels,
                                timestamps: Vec::new(),
                                values: Vec::new(),
                                claimed: None,
                            });
                            buckets.len() - 1
                        });
                        by_pair.insert((many, one), bucket);
                        bucket
                    }
                };
                // The claim happens before the comparison is applied,
                // because upstream checks the match before it decides
                // whether the sample survives the operator.
                if buckets[bucket].claimed == Some(step) {
                    return Err(DataFusionError::Execution(GROUPING_NOT_UNIQUE.into()));
                }
                buckets[bucket].claimed = Some(step);

                let (lhs, rhs) = match self.matching.one_side() {
                    LHS => (self.one_values[step], self.many[many].values[step]),
                    _ => (self.many[many].values[step], self.one_values[step]),
                };
                if let Some(value) = self.op.value(lhs, rhs, self.return_bool) {
                    buckets[bucket].timestamps.push(self.grid.timestamp(step));
                    buckets[bucket].values.push(value);
                }
            }
        }

        buckets.retain(|bucket| !bucket.timestamps.is_empty());
        // Sorted, because DataFusion is free to hand the rows of a
        // group over in any order and the answer must not be.
        buckets.sort_by(|a, b| a.labels.cmp(&b.labels));
        Ok(buckets)
    }
}

impl Accumulator for Pairing {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let list = values
            .first()
            .and_then(|v| v.as_list_opt::<i32>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: first argument is not a samples list"))
            })?;
        let side = values
            .get(1)
            .and_then(|v| v.as_boolean_opt())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: second argument is not a boolean"))
            })?;
        let labels = values
            .get(2)
            .and_then(|v| v.as_struct_opt())
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{NAME}: third argument is not a label struct"))
            })?;

        let entries = list.values().as_struct();
        let timestamps = child::<TimestampMillisecondArray>(entries, series::TIMESTAMP)?.values();
        let samples = child::<Float64Array>(entries, series::VALUE)?.values();
        let offsets = list.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let which = if side.is_valid(row) && side.value(row) {
                RHS
            } else {
                LHS
            };
            let labels = read_labels(&self.schema, labels, row);
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            self.absorb(which, &labels, &timestamps[lo..hi], &samples[lo..hi])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let buckets = self.pairs()?;

        let mut offsets = Vec::with_capacity(buckets.len() + 1);
        offsets.push(0i32);
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for bucket in &buckets {
            timestamps.extend_from_slice(&bucket.timestamps);
            values.extend_from_slice(&bucket.values);
            offsets.push(timestamps.len() as i32);
        }
        let entries = StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                Arc::new(Float64Array::from(values)),
            ],
            None,
        );
        let samples = ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(entries),
            None,
        );

        // Rebuilt through `series::labels_type` rather than from the
        // input's own fields, so that the struct this hands back is the
        // one `promql_labels` would have built for the same names.
        let DataType::Struct(fields) = series::labels_type(&self.schema) else {
            unreachable!("a labels type is a struct")
        };
        let columns: Vec<ArrayRef> = fields
            .iter()
            .map(|field| {
                let index = self
                    .schema
                    .iter()
                    .position(|name| name == field.name())
                    .expect("every field of the labels type is a column of the schema");
                let values: Vec<&str> = buckets
                    .iter()
                    .map(|bucket| bucket.labels[index].as_str())
                    .collect();
                Arc::new(StringViewArray::from(values)) as ArrayRef
            })
            .collect();
        let labels = match fields.is_empty() {
            true => StructArray::new_empty_fields(buckets.len(), None),
            false => StructArray::new(fields, columns, None),
        };

        let pairs = StructArray::new(
            pair_fields(&self.schema),
            vec![Arc::new(labels), Arc::new(samples)],
            None,
        );
        Ok(one_row(pair_item(&self.schema), Arc::new(pairs)))
    }

    /// Flat lists throughout: the "many" side is a rectangle of rows by
    /// steps, and the label sets a rectangle of rows by columns, so a
    /// partial travels as the same handful of primitive lanes whatever
    /// the query's label schema is.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let width = self.schema.len();
        let mut many_labels: Vec<&str> = Vec::with_capacity(self.many.len() * width);
        let mut many_counts: Vec<i64> = Vec::with_capacity(self.many.len() * self.grid.len());
        let mut many_values: Vec<f64> = Vec::with_capacity(many_counts.capacity());
        for series in &self.many {
            many_labels.extend(series.labels.iter().map(String::as_str));
            many_counts.extend_from_slice(&series.counts);
            many_values.extend_from_slice(&series.values);
        }
        let one_pool: Vec<&str> = self
            .one_pool
            .iter()
            .flat_map(|labels| labels.iter().map(String::as_str))
            .collect();

        Ok(vec![
            int_lane(STATE_ONE_COUNTS, self.one_counts.clone()),
            float_lane(STATE_ONE_VALUES, self.one_values.clone()),
            int_lane(
                STATE_ONE_ROWS,
                self.one_rows.iter().map(|row| *row as i64).collect(),
            ),
            text_lane(STATE_ONE_POOL, one_pool),
            text_lane(
                STATE_ONE_METRICS,
                self.one_metrics.iter().map(String::as_str).collect(),
            ),
            text_lane(STATE_MANY_LABELS, many_labels),
            int_lane(STATE_MANY_COUNTS, many_counts),
            float_lane(STATE_MANY_VALUES, many_values),
            int_lane(
                STATE_MANY_SIDES,
                self.many.iter().map(|s| s.side as i64).collect(),
            ),
            ScalarValue::Utf8(self.group.clone()),
        ])
    }

    /// Lanes added position by position, as in [`crate::aggregate`]. A
    /// value is only read where the total count is one, and then exactly
    /// one partial saw that sample, so any partial that saw something
    /// carries the value.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let steps = self.grid.len();
        let width = self.schema.len();

        let one_counts = lanes::<Int64Type>(states.first(), STATE_ONE_COUNTS)?;
        let one_values = lanes::<Float64Type>(states.get(1), STATE_ONE_VALUES)?;
        let one_rows = lanes::<Int64Type>(states.get(2), STATE_ONE_ROWS)?;
        let one_pool = texts(states.get(3), STATE_ONE_POOL)?;
        for row in 0..one_counts.len() {
            let counts = one_counts.value(row);
            let counts = counts.as_primitive::<Int64Type>();
            let values = one_values.value(row);
            let values = values.as_primitive::<Float64Type>();
            let rows = one_rows.value(row);
            let rows = rows.as_primitive::<Int64Type>();
            if counts.len() != steps || values.len() != steps || rows.len() != steps {
                return Err(DataFusionError::Internal(format!(
                    "{NAME}: partial state is {} steps, not the {steps} of this grid",
                    counts.len()
                )));
            }
            let pool = rectangle(&one_pool.value(row), width, NO_ROWS)?;
            let mut seen: Vec<i32> = Vec::with_capacity(pool.len());
            for labels in &pool {
                seen.push(self.intern_one(labels));
            }
            for step in 0..steps {
                if counts.value(step) > 0 {
                    self.one_counts[step] += counts.value(step);
                    self.one_values[step] = values.value(step);
                    let held = rows.value(step);
                    if held >= 0 {
                        self.one_rows[step] = *seen
                            .get(held as usize)
                            .ok_or_else(|| self.corrupt("a one-side row out of its pool"))?;
                    }
                }
            }
        }

        let many_labels = texts(states.get(5), STATE_MANY_LABELS)?;
        let many_counts = lanes::<Int64Type>(states.get(6), STATE_MANY_COUNTS)?;
        let many_values = lanes::<Float64Type>(states.get(7), STATE_MANY_VALUES)?;
        let many_sides = lanes::<Int64Type>(states.get(8), STATE_MANY_SIDES)?;
        for row in 0..many_counts.len() {
            let counts = many_counts.value(row);
            let counts = counts.as_primitive::<Int64Type>();
            let values = many_values.value(row);
            let values = values.as_primitive::<Float64Type>();
            let sides = many_sides.value(row);
            let sides = sides.as_primitive::<Int64Type>();
            let rows = counts.len() / steps.max(1);
            if counts.len() != rows * steps || values.len() != counts.len() {
                return Err(self.corrupt("a many-side rectangle that is not rows by steps"));
            }
            if sides.len() != rows {
                return Err(self.corrupt("a side per series that is not one per series"));
            }
            let labels = rectangle(&many_labels.value(row), width, rows)?;
            for (index, labels) in labels.iter().enumerate() {
                let many = self.intern_many(sides.value(index) as usize, labels);
                for step in 0..steps {
                    let at = index * steps + step;
                    if counts.value(at) > 0 {
                        self.many[many].counts[step] += counts.value(at);
                        self.many[many].values[step] = values.value(at);
                    }
                }
            }
        }

        if let Some(metrics) = states.get(4).and_then(|s| s.as_list_opt::<i32>()) {
            for row in 0..metrics.len() {
                let held = metrics.value(row);
                let held = held.as_string::<i32>();
                for i in 0..held.len() {
                    if self.one_metrics.len() < 2
                        && self.one_metrics.first().map(String::as_str) != Some(held.value(i))
                    {
                        self.one_metrics.push(held.value(i).to_string());
                    }
                }
            }
        }
        if let Some(group) = states.get(9).and_then(|s| s.as_string_opt::<i32>()) {
            for row in 0..group.len() {
                if self.group.is_none() && group.is_valid(row) {
                    self.group = Some(group.value(row).to_string());
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let lane = self.grid.len() * (std::mem::size_of::<i64>() + std::mem::size_of::<f64>());
        std::mem::size_of::<Self>()
            + lane
            + self.grid.len() * std::mem::size_of::<i32>()
            + self.many.len() * lane
            + (self.many.len() + self.one_pool.len())
                * self.schema.len()
                * std::mem::size_of::<String>()
    }
}

impl Pairing {
    fn corrupt(&self, what: &str) -> DataFusionError {
        DataFusionError::Internal(format!("{NAME}: partial state holds {what}"))
    }
}

/// Row count of a label rectangle is derived from its length where the
/// caller does not already know it, which a zero-width schema makes
/// impossible — so the caller says which case it is.
const NO_ROWS: usize = usize::MAX;

/// A flat run of label values read back as one label set per row.
fn rectangle(flat: &ArrayRef, width: usize, rows: usize) -> Result<Vec<Vec<String>>> {
    let flat = flat.as_string::<i32>();
    if width == 0 {
        // Every label set is the empty one, so a pool of them holds
        // exactly one entry and a rectangle holds as many as the
        // caller counted.
        return Ok(vec![Vec::new(); if rows == NO_ROWS { 1 } else { rows }]);
    }
    if !flat.len().is_multiple_of(width) {
        return Err(DataFusionError::Internal(format!(
            "{NAME}: {} label values are not a multiple of the {width} columns",
            flat.len()
        )));
    }
    Ok((0..flat.len() / width)
        .map(|row| {
            (0..width)
                .map(|column| flat.value(row * width + column).to_string())
                .collect()
        })
        .collect())
}

/// One row of a label struct as the vector of values this module works
/// in, in the schema's order.
fn read_labels(schema: &[String], labels: &StructArray, row: usize) -> Vec<String> {
    schema
        .iter()
        .map(|name| match labels.column_by_name(name) {
            Some(column) => match column.data_type() {
                DataType::Utf8View => column.as_string_view().value(row).to_string(),
                DataType::Utf8 => column.as_string::<i32>().value(row).to_string(),
                _ => String::new(),
            },
            None => String::new(),
        })
        .collect()
}

/// A label set as Prometheus prints one, keeping the names `wanted`
/// answers for. An empty value is how the canonical shape spells a
/// label that is not there, so it prints as nothing at all.
fn print(schema: &[String], labels: &[String], mut wanted: impl FnMut(&str) -> bool) -> String {
    let mut out = String::from("{");
    for (name, value) in schema.iter().zip(labels) {
        if value.is_empty() || !wanted(name) {
            continue;
        }
        if out.len() > 1 {
            out.push_str(", ");
        }
        out.push_str(name);
        out.push_str("=\"");
        out.push_str(value);
        out.push('"');
    }
    out.push('}');
    out
}

const STATE_ONE_COUNTS: &str = "one_counts";
const STATE_ONE_VALUES: &str = "one_values";
const STATE_ONE_ROWS: &str = "one_rows";
const STATE_ONE_POOL: &str = "one_pool";
const STATE_ONE_METRICS: &str = "one_metrics";
const STATE_MANY_LABELS: &str = "many_labels";
const STATE_MANY_COUNTS: &str = "many_counts";
const STATE_MANY_VALUES: &str = "many_values";
const STATE_MANY_SIDES: &str = "many_sides";
const STATE_GROUP: &str = "group";

/// Named once so that a rename fails to compile at both ends rather
/// than mismatching across a partial/final plan boundary.
const STATE_LANES: [(&str, DataType); 9] = [
    (STATE_ONE_COUNTS, DataType::Int64),
    (STATE_ONE_VALUES, DataType::Float64),
    (STATE_ONE_ROWS, DataType::Int64),
    (STATE_ONE_POOL, DataType::Utf8),
    (STATE_ONE_METRICS, DataType::Utf8),
    (STATE_MANY_LABELS, DataType::Utf8),
    (STATE_MANY_COUNTS, DataType::Int64),
    (STATE_MANY_VALUES, DataType::Float64),
    (STATE_MANY_SIDES, DataType::Int64),
];

fn state_item(name: &str, of: DataType) -> FieldRef {
    Arc::new(Field::new(name, of, false))
}

fn state_type(name: &str, of: DataType) -> DataType {
    DataType::List(state_item(name, of))
}

fn int_lane(name: &str, values: Vec<i64>) -> ScalarValue {
    one_row(
        state_item(name, DataType::Int64),
        Arc::new(Int64Array::from(values)),
    )
}

fn float_lane(name: &str, values: Vec<f64>) -> ScalarValue {
    one_row(
        state_item(name, DataType::Float64),
        Arc::new(Float64Array::from(values)),
    )
}

fn text_lane(name: &str, values: Vec<&str>) -> ScalarValue {
    one_row(
        state_item(name, DataType::Utf8),
        Arc::new(StringArray::from(values)),
    )
}

/// One list value holding `entries` whole: the single row an
/// [`Accumulator`] hands back.
fn one_row(item: FieldRef, entries: ArrayRef) -> ScalarValue {
    let len = entries.len() as i32;
    ScalarValue::List(Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(vec![0, len].into()),
        entries,
        None,
    )))
}

fn child<'a, T: 'static>(entries: &'a StructArray, name: &str) -> Result<&'a T> {
    entries
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: samples have no {name} column of its type"))
        })
}

/// One state column as the list of lanes it must be.
fn lanes<T: datafusion::arrow::datatypes::ArrowPrimitiveType>(
    state: Option<&ArrayRef>,
    name: &str,
) -> Result<ListArray> {
    let list = list_state(state, name)?;
    if list.values().as_primitive_opt::<T>().is_none() {
        return Err(DataFusionError::Internal(format!(
            "{NAME}: partial state column {name} holds {}",
            list.values().data_type()
        )));
    }
    Ok(list)
}

fn texts(state: Option<&ArrayRef>, name: &str) -> Result<ListArray> {
    let list = list_state(state, name)?;
    if list.values().as_string_opt::<i32>().is_none() {
        return Err(DataFusionError::Internal(format!(
            "{NAME}: partial state column {name} holds {}",
            list.values().data_type()
        )));
    }
    Ok(list)
}

fn list_state(state: Option<&ArrayRef>, name: &str) -> Result<ListArray> {
    Ok(state
        .and_then(|s| s.as_list_opt::<i32>())
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{NAME}: partial state column {name} is not a list"))
        })?
        .clone())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Binary {
    signature: Signature,
}

impl Default for Binary {
    fn default() -> Self {
        // Not `exact`: the label struct's type is the query's own label
        // set, so only the count of arguments is fixed.
        Self {
            signature: Signature::any(8, Volatility::Immutable),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(Binary::default())
}

/// `promql_binary(samples, <side>, labels, '<op>', '<matching>', …grid)`.
///
/// The grid is an argument for the same reason [`crate::aggregate`]
/// takes one: it turns a timestamp into a lane index. `labels` is the
/// *input's* label set, `__name__` included, not the grouping's: the
/// result's labels are built from it, and so is the label set a failure
/// has to quote.
#[allow(clippy::too_many_arguments)]
pub fn call(
    samples: Expr,
    side: Expr,
    labels: Expr,
    op: Op,
    return_bool: bool,
    matching: &Matching,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Expr {
    udaf().call(vec![
        samples,
        side,
        labels,
        lit(literal(op, return_bool)),
        lit(matching.literal()),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

/// The label column names of a labels struct, in schema order.
fn names_of(labels: &DataType) -> Option<Vec<String>> {
    match labels {
        DataType::Struct(fields) => Some(fields.iter().map(|f| f.name().clone()).collect()),
        _ => None,
    }
}

/// The operator, the matching and the grid, read back off the planned
/// call.
fn from_args(args: &AccumulatorArgs) -> Result<Pairing> {
    let literal = |i: usize| {
        args.exprs
            .get(i)
            .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
            .map(Literal::value)
    };
    let text = |i: usize| match literal(i) {
        Some(ScalarValue::Utf8(Some(s))) => Some(s.as_str()),
        _ => None,
    };
    let (op, return_bool) = text(3).and_then(parse_literal).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{NAME}: fourth argument must be a binary operator as a string literal"
        ))
    })?;
    let matching = text(4).and_then(Matching::parse).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{NAME}: fifth argument must be the matching modifiers as a string literal"
        ))
    })?;
    let number = |i: usize, what: &str| {
        literal(i)
            .and_then(|v| match v {
                ScalarValue::Int64(Some(n)) => Some(*n),
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!("{NAME}: {what} must be an Int64 literal"))
            })
    };
    let grid = Grid::new(
        NAME,
        number(5, "start")?,
        number(6, "end")?,
        number(7, "step")?,
    )?;
    let schema = args
        .exprs
        .get(2)
        .ok_or_else(|| DataFusionError::Plan(format!("{NAME}: no labels argument")))?
        .data_type(args.schema)
        .ok()
        .as_ref()
        .and_then(names_of)
        .ok_or_else(|| {
            DataFusionError::Plan(format!("{NAME}: third argument must be a label struct"))
        })?;
    Ok(Pairing::new(op, return_bool, matching, grid, schema))
}

impl AggregateUDFImpl for Binary {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.first() != Some(&series::samples_type()) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::samples_type(),
                arg_types.first()
            );
        }
        if !matches!(arg_types.get(1), Some(DataType::Boolean)) {
            return plan_err!("{NAME}: second argument must say which side a row is on");
        }
        let Some(names) = arg_types.get(2).and_then(names_of) else {
            return plan_err!("{NAME}: third argument must be a label struct");
        };
        Ok(output_type(&names))
    }

    /// A match group with nothing to pair answers with an empty list,
    /// which the planner's `Unnest` drops on its own.
    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        Ok(Box::new(from_args(&args)?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let mut fields: Vec<FieldRef> = STATE_LANES
            .iter()
            .map(|(name, of)| {
                Arc::new(Field::new(
                    format_state_name(args.name, name),
                    state_type(name, of.clone()),
                    false,
                )) as FieldRef
            })
            .collect();
        fields.push(Arc::new(Field::new(
            format_state_name(args.name, STATE_GROUP),
            DataType::Utf8,
            true,
        )));
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The label schema every pairing below works in, sorted as the
    /// canonical struct sorts it, so an expected label vector reads in
    /// the order the output carries.
    const SCHEMA: [&str; 4] = [METRIC_NAME, "path", "pod", "zone"];

    fn names(of: &[&str]) -> Vec<String> {
        of.iter().map(|n| n.to_string()).collect()
    }

    /// One label set in [`SCHEMA`]'s order; `""` is a label the series
    /// does not carry.
    fn lset(name: &str, path: &str, pod: &str, zone: &str) -> Vec<String> {
        names(&[name, path, pod, zone])
    }

    fn grid() -> Grid {
        Grid::new(NAME, 0, 30_000, 10_000).unwrap()
    }

    fn pairing(op: Op, return_bool: bool, matching: Matching) -> Pairing {
        Pairing::new(op, return_bool, matching, grid(), names(&SCHEMA))
    }

    fn plain(op: Op) -> Pairing {
        pairing(op, false, Matching::default())
    }

    fn on(labels: &[&str]) -> Matching {
        Matching {
            on: true,
            labels: names(labels),
            ..Matching::default()
        }
    }

    /// One output series as a test reads it: its labels in [`SCHEMA`]'s
    /// order, then its samples.
    type Out = (Vec<String>, Vec<(i64, f64)>);

    /// Every output series of one match group.
    fn pairs_of(value: ScalarValue) -> Vec<Out> {
        let ScalarValue::List(list) = value else {
            panic!("a list of output series")
        };
        let pairs = list.value(0);
        let pairs = pairs.as_struct();
        let labels = pairs.column_by_name(PAIR_LABELS).unwrap().as_struct();
        let samples = pairs.column_by_name(PAIR_SAMPLES).unwrap().as_list::<i32>();
        (0..pairs.len())
            .map(|row| {
                let entries = samples.value(row);
                let entries = entries.as_struct();
                let timestamps = entries
                    .column_by_name(series::TIMESTAMP)
                    .unwrap()
                    .as_primitive::<datafusion::arrow::datatypes::TimestampMillisecondType>(
                );
                let values = entries
                    .column_by_name(series::VALUE)
                    .unwrap()
                    .as_primitive::<Float64Type>();
                let samples = (0..entries.len())
                    .map(|i| (timestamps.value(i), values.value(i)))
                    .collect();
                let labels = (0..labels.num_columns())
                    .map(|c| labels.column(c).as_string_view().value(row).to_string())
                    .collect();
                (labels, samples)
            })
            .collect()
    }

    /// The samples of a group that answers with one series.
    fn samples_of(value: ScalarValue) -> Vec<(i64, f64)> {
        match pairs_of(value).as_slice() {
            [] => Vec::new(),
            [(_, samples)] => samples.clone(),
            many => panic!("{} output series, not one", many.len()),
        }
    }

    /// Upstream's `vectorElemBinop`, including the two Go functions the
    /// operators are: `math.Mod` takes the dividend's sign, and division
    /// by zero is an infinity rather than an error.
    #[test]
    fn the_arithmetic_is_upstreams_vector_elem_binop() {
        let v = |op: Op, l, r| op.value(l, r, false).expect("arithmetic always keeps");
        assert_eq!(v(Op::Add, 1.0, 2.0), 3.0);
        assert_eq!(v(Op::Sub, 1.0, 2.0), -1.0);
        assert_eq!(v(Op::Mul, 3.0, 4.0), 12.0);
        assert_eq!(v(Op::Div, 1.0, 4.0), 0.25);
        assert_eq!(v(Op::Pow, 2.0, 10.0), 1024.0);
        assert_eq!(v(Op::Atan2, 1.0, 1.0), std::f64::consts::FRAC_PI_4);

        assert_eq!(v(Op::Mod, 7.0, 3.0), 1.0);
        assert_eq!(v(Op::Mod, -7.0, 3.0), -1.0);
        assert_eq!(v(Op::Mod, 7.0, -3.0), 1.0);
        assert!(v(Op::Mod, 1.0, 0.0).is_nan());

        assert_eq!(v(Op::Div, 1.0, 0.0), f64::INFINITY);
        assert_eq!(v(Op::Div, -1.0, 0.0), f64::NEG_INFINITY);
        assert!(v(Op::Div, 0.0, 0.0).is_nan());
    }

    /// A comparison answers with the sample it was given or with
    /// nothing at all; `bool` turns that into a 1 or a 0 and always
    /// answers.
    #[test]
    fn a_comparison_filters_and_bool_scores() {
        assert_eq!(Op::Gtr.value(3.0, 2.0, false), Some(3.0));
        assert_eq!(Op::Gtr.value(2.0, 3.0, false), None);
        assert_eq!(Op::Gtr.value(3.0, 2.0, true), Some(1.0));
        assert_eq!(Op::Gtr.value(2.0, 3.0, true), Some(0.0));

        assert_eq!(Op::Eql.value(2.0, 2.0, false), Some(2.0));
        assert_eq!(Op::Neq.value(2.0, 2.0, false), None);
        assert_eq!(Op::Gte.value(2.0, 2.0, false), Some(2.0));
        assert_eq!(Op::Lte.value(2.0, 2.0, false), Some(2.0));
        assert_eq!(Op::Lss.value(2.0, 2.0, false), None);

        // A NaN compares false to everything, `!=` included by
        // negation, so it survives only that one.
        for op in [Op::Eql, Op::Gtr, Op::Lss, Op::Gte, Op::Lte] {
            assert_eq!(op.value(f64::NAN, 1.0, false), None, "{}", op.as_str());
            assert_eq!(op.value(f64::NAN, 1.0, true), Some(0.0), "{}", op.as_str());
        }
        assert!(Op::Neq.value(f64::NAN, 1.0, false).unwrap().is_nan());
    }

    /// Only a comparison without `bool` answers with the metric it was
    /// given, so only that one keeps its name.
    #[test]
    fn what_drops_the_metric_name_is_changes_metric_schema_plus_bool() {
        assert!(Op::Add.drops_metric_name(false));
        assert!(Op::Atan2.drops_metric_name(false));
        assert!(!Op::Gtr.drops_metric_name(false));
        assert!(Op::Gtr.drops_metric_name(true));
        // A set operator hands the sample back as it came, name and all.
        for op in [Op::And, Op::Or, Op::Unless] {
            assert!(!op.drops_metric_name(false), "{}", op.as_str());
        }
    }

    #[test]
    fn every_operator_round_trips_through_its_promql_spelling() {
        for op in ALL {
            assert_eq!(parse_literal(literal(op, false)), Some((op, false)));
            if op.is_comparison() {
                assert_eq!(parse_literal(literal(op, true)), Some((op, true)));
                assert_eq!(literal(op, true), format!("{} bool", op.as_str()));
            }
        }
        assert_eq!(Op::from_token(ItemType::Add), Some(Op::Add));
        assert_eq!(Op::from_token(ItemType::Gtr), Some(Op::Gtr));
        assert_eq!(Op::from_token(ItemType::Land), Some(Op::And));
        assert_eq!(Op::from_token(ItemType::Lunless), Some(Op::Unless));
        assert_eq!(parse_literal("and"), Some((Op::And, false)));
        assert_eq!(parse_literal("and bool"), None);
        // `bool` belongs to a comparison and to nothing else, so a plan
        // that spells it anywhere else is not one this wrote.
        assert_eq!(parse_literal("+ bool"), None);
        assert_eq!(parse_literal("atan2 bool"), None);
    }

    /// The modifiers go into a plan as the query wrote them and come
    /// back the same.
    #[test]
    fn the_matching_modifiers_round_trip_through_promql() {
        let cases = [
            (Matching::default(), ""),
            (on(&["job", "pod"]), "on(job, pod)"),
            (
                Matching {
                    labels: names(&["zone"]),
                    ..Matching::default()
                },
                "ignoring(zone)",
            ),
            (
                Matching {
                    card: Card::ManyToOne,
                    on: true,
                    labels: names(&["job"]),
                    include: names(&["tier"]),
                },
                "on(job) group_left(tier)",
            ),
            (
                Matching {
                    card: Card::OneToMany,
                    on: true,
                    labels: names(&["job"]),
                    include: Vec::new(),
                },
                "on(job) group_right()",
            ),
            (
                Matching {
                    card: Card::ManyToOne,
                    ..Matching::default()
                },
                "group_left()",
            ),
        ];
        for (matching, spelling) in cases {
            assert_eq!(matching.literal(), spelling);
            assert_eq!(Matching::parse(spelling), Some(matching), "{spelling}");
        }
        assert_eq!(Matching::parse("on(job"), None);
        assert_eq!(Matching::parse("group_sideways()"), None);
    }

    /// A step only one side reached is not in the result: upstream's
    /// loop over the left side skips a sample with no match, and a right
    /// sample nothing matched is never looked at.
    #[test]
    fn only_the_steps_both_sides_reached_are_paired() {
        let mut pairing = plain(Op::Add);
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");
        pairing
            .absorb(LHS, &left, &[0, 10_000, 20_000], &[1.0, 2.0, 3.0])
            .unwrap();
        pairing
            .absorb(RHS, &right, &[10_000, 30_000], &[10.0, 30.0])
            .unwrap();
        assert_eq!(samples_of(pairing.evaluate().unwrap()), [(10_000, 12.0)]);
    }

    /// A comparison between two vectors answers with the left sample
    /// where it holds and leaves the step out where it does not — the
    /// filter upstream's `keep` is, one step at a time.
    #[test]
    fn a_comparison_between_vectors_filters_step_by_step() {
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");

        let mut filtered = pairing(Op::Gtr, false, Matching::default());
        filtered
            .absorb(LHS, &left, &[0, 10_000], &[5.0, 1.0])
            .unwrap();
        filtered
            .absorb(RHS, &right, &[0, 10_000], &[2.0, 9.0])
            .unwrap();
        assert_eq!(samples_of(filtered.evaluate().unwrap()), [(0, 5.0)]);

        let mut scored = pairing(Op::Gtr, true, Matching::default());
        scored
            .absorb(LHS, &left, &[0, 10_000], &[5.0, 1.0])
            .unwrap();
        scored
            .absorb(RHS, &right, &[0, 10_000], &[2.0, 9.0])
            .unwrap();
        assert_eq!(
            samples_of(scored.evaluate().unwrap()),
            [(0, 1.0), (10_000, 0.0)]
        );
    }

    /// Two series on the same side of one match group are the two
    /// matching errors, and only where their samples meet at a step.
    #[test]
    fn two_series_in_a_match_group_fail_where_they_overlap() {
        let a = lset("requests", "", "a", "");
        let b = lset("retries", "", "a", "");
        let c = lset("errors", "", "a", "");

        let mut both = plain(Op::Add);
        both.absorb(LHS, &a, &[0], &[1.0]).unwrap();
        both.absorb(LHS, &b, &[0], &[2.0]).unwrap();
        both.absorb(RHS, &c, &[0], &[3.0]).unwrap();
        let err = both.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("many-to-one matching must be explicit"),
            "{err}"
        );

        let mut right = plain(Op::Add);
        right.absorb(LHS, &c, &[0], &[1.0]).unwrap();
        right.absorb(RHS, &a, &[0], &[2.0]).unwrap();
        right.absorb(RHS, &b, &[0], &[3.0]).unwrap();
        let err = right.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("found duplicate series for the match group {pod=\"a\"} on the right"),
            "{err}"
        );
        assert!(
            err.contains(
                ";many-to-many matching not allowed: matching labels must be unique on one side"
            ),
            "{err}"
        );

        // Apart in time is not a duplicate at all: upstream checks the
        // vector at one step, not the series over the range.
        let mut apart = plain(Op::Add);
        apart.absorb(LHS, &a, &[0], &[1.0]).unwrap();
        apart.absorb(LHS, &b, &[10_000], &[2.0]).unwrap();
        apart.absorb(RHS, &c, &[0, 10_000], &[3.0, 4.0]).unwrap();
        assert_eq!(
            samples_of(apart.evaluate().unwrap()),
            [(0, 4.0), (10_000, 6.0)]
        );
    }

    /// Two left series that differ only in `__name__` share a match
    /// group, because the signature drops the name — but a comparison
    /// gives that name back, so the group has to answer with both.
    #[test]
    fn a_kept_name_splits_the_match_group_back_up() {
        let a = lset("a", "", "one", "");
        let b = lset("b", "", "one", "");
        let c = lset("c", "", "one", "");

        let mut filtered = pairing(Op::Gtr, false, Matching::default());
        filtered.absorb(LHS, &b, &[0], &[5.0]).unwrap();
        filtered.absorb(LHS, &a, &[10_000], &[7.0]).unwrap();
        filtered.absorb(RHS, &c, &[0, 10_000], &[1.0, 1.0]).unwrap();
        assert_eq!(
            pairs_of(filtered.evaluate().unwrap()),
            [
                (lset("a", "", "one", ""), vec![(10_000, 7.0)]),
                (lset("b", "", "one", ""), vec![(0, 5.0)]),
            ]
        );

        // `bool` takes the name away again, so the same two series are
        // one result — which is what upstream's `resultMetric` does
        // once `changesMetricSchema` holds.
        let mut scored = pairing(Op::Gtr, true, Matching::default());
        scored.absorb(LHS, &b, &[0], &[5.0]).unwrap();
        scored.absorb(LHS, &a, &[10_000], &[7.0]).unwrap();
        scored.absorb(RHS, &c, &[0, 10_000], &[1.0, 1.0]).unwrap();
        assert_eq!(
            pairs_of(scored.evaluate().unwrap()),
            [(lset("", "", "one", ""), vec![(0, 1.0), (10_000, 1.0)])]
        );
    }

    /// `on(…)` narrows the result to the labels it names, `ignoring(…)`
    /// takes only those away — upstream's `Keep` and `Del`.
    #[test]
    fn one_to_one_keeps_what_the_modifier_says() {
        let left = lset("requests", "", "a", "eu");
        let right = lset("errors", "", "a", "us");

        let mut kept = pairing(Op::Add, false, on(&["pod"]));
        kept.absorb(LHS, &left, &[0], &[6.0]).unwrap();
        kept.absorb(RHS, &right, &[0], &[2.0]).unwrap();
        assert_eq!(
            pairs_of(kept.evaluate().unwrap()),
            [(lset("", "", "a", ""), vec![(0, 8.0)])]
        );

        let mut ignoring = pairing(
            Op::Add,
            false,
            Matching {
                labels: names(&["zone"]),
                ..Matching::default()
            },
        );
        ignoring.absorb(LHS, &left, &[0], &[6.0]).unwrap();
        ignoring.absorb(RHS, &right, &[0], &[2.0]).unwrap();
        assert_eq!(
            pairs_of(ignoring.evaluate().unwrap()),
            [(lset("", "", "a", ""), vec![(0, 8.0)])]
        );
    }

    /// `group_left` keeps every label of the many side — no `Keep` and
    /// no `Del` — and copies the named ones over from the one side, so
    /// a match group answers with a series per left-hand one.
    #[test]
    fn group_left_fans_out_and_carries_the_included_labels() {
        let mut pairing = pairing(
            Op::Div,
            false,
            Matching {
                card: Card::ManyToOne,
                on: true,
                labels: names(&["pod"]),
                include: names(&["zone"]),
            },
        );
        pairing
            .absorb(LHS, &lset("requests", "/a", "a", ""), &[0], &[10.0])
            .unwrap();
        pairing
            .absorb(LHS, &lset("requests", "/b", "a", ""), &[0], &[20.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", "eu"), &[0], &[2.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [
                (lset("", "/a", "a", "eu"), vec![(0, 5.0)]),
                (lset("", "/b", "a", "eu"), vec![(0, 10.0)]),
            ]
        );
    }

    /// `group_right` is the same match with the sides swapped: the
    /// right is the many, and the operands keep their written order.
    #[test]
    fn group_right_swaps_which_side_may_be_many() {
        let mut pairing = pairing(
            Op::Sub,
            false,
            Matching {
                card: Card::OneToMany,
                on: true,
                labels: names(&["pod"]),
                include: Vec::new(),
            },
        );
        pairing
            .absorb(LHS, &lset("total", "", "a", ""), &[0], &[10.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "/a", "a", ""), &[0], &[2.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "/b", "a", ""), &[0], &[3.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [
                (lset("", "/a", "a", ""), vec![(0, 8.0)]),
                (lset("", "/b", "a", ""), vec![(0, 7.0)]),
            ]
        );
    }

    /// A duplicate on the one side of a `group_right` is quoted as the
    /// left-hand side, because that is where the one now sits.
    #[test]
    fn group_right_names_the_left_hand_side_in_a_duplicate() {
        let mut pairing = pairing(
            Op::Add,
            false,
            Matching {
                card: Card::OneToMany,
                on: true,
                labels: names(&["pod"]),
                include: Vec::new(),
            },
        );
        pairing
            .absorb(LHS, &lset("total", "", "a", ""), &[0], &[1.0])
            .unwrap();
        pairing
            .absorb(LHS, &lset("count", "", "a", ""), &[0], &[2.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", ""), &[0], &[3.0])
            .unwrap();
        let err = pairing.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("found duplicate series for the match group {pod=\"a\"} on the left"),
            "{err}"
        );
    }

    /// The grouping labels have to leave the result unique: two series
    /// of the many side reaching the same labels is upstream's other
    /// matching error.
    #[test]
    fn a_fan_out_that_collides_names_the_grouping_labels() {
        let mut pairing = pairing(
            Op::Add,
            false,
            Matching {
                card: Card::ManyToOne,
                on: true,
                labels: names(&["pod"]),
                include: Vec::new(),
            },
        );
        // Both left series lose `__name__` to the arithmetic and agree
        // on everything else, so they are one result label set.
        pairing
            .absorb(LHS, &lset("requests", "", "a", "eu"), &[0], &[1.0])
            .unwrap();
        pairing
            .absorb(LHS, &lset("retries", "", "a", "eu"), &[0], &[2.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", ""), &[0], &[3.0])
            .unwrap();
        let err = pairing.evaluate().unwrap_err().to_string();
        assert!(
            err.contains("multiple matches for labels: grouping labels must ensure unique matches"),
            "{err}"
        );
    }

    /// An `on` that names `__name__` matches on it and keeps it, save
    /// where the operator has taken it away first.
    #[test]
    fn on_the_metric_name_matches_on_it() {
        let mut pairing = pairing(Op::Gtr, false, on(&[METRIC_NAME]));
        pairing
            .absorb(LHS, &lset("up", "", "a", ""), &[0], &[5.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("up", "", "b", ""), &[0], &[2.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [(lset("up", "", "", ""), vec![(0, 5.0)])]
        );
    }

    /// Two partitions of one match group reach the same answer as one,
    /// down to the many side's labels and the one side's `group_x`
    /// values — whose pool indices mean nothing across a partial.
    #[test]
    fn partial_states_merge_to_the_same_pairing() {
        let matching = Matching {
            card: Card::ManyToOne,
            on: true,
            labels: names(&["pod"]),
            include: names(&["zone"]),
        };
        let of = || pairing(Op::Mul, false, matching.clone());
        let many_a = lset("requests", "/a", "a", "");
        let many_b = lset("requests", "/b", "a", "");
        let one = lset("errors", "", "a", "eu");

        let mut whole = of();
        whole
            .absorb(LHS, &many_a, &[0, 10_000], &[2.0, 3.0])
            .unwrap();
        whole.absorb(LHS, &many_b, &[10_000], &[4.0]).unwrap();
        whole.absorb(RHS, &one, &[0, 10_000], &[5.0, 7.0]).unwrap();

        let mut left = of();
        left.absorb(LHS, &many_a, &[0, 10_000], &[2.0, 3.0])
            .unwrap();
        let mut right = of();
        right.absorb(LHS, &many_b, &[10_000], &[4.0]).unwrap();
        right.absorb(RHS, &one, &[0, 10_000], &[5.0, 7.0]).unwrap();

        let mut merged = of();
        for mut partial in [left, right] {
            let state = partial.state().unwrap();
            let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
            merged.merge_batch(&arrays).unwrap();
        }
        assert_eq!(merged.evaluate().unwrap(), whole.evaluate().unwrap());
        assert_eq!(
            pairs_of(merged.evaluate().unwrap()),
            [
                (lset("", "/a", "a", "eu"), vec![(0, 10.0), (10_000, 21.0)]),
                (lset("", "/b", "a", "eu"), vec![(10_000, 28.0)]),
            ]
        );
    }

    /// The three set operators over one match group, step by step: the
    /// right side is present at the first two steps and gone at the
    /// third, so each operator changes its mind exactly there.
    #[test]
    fn a_set_operator_asks_only_whether_the_other_side_is_there() {
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");
        let of = |op: Op| {
            let mut pairing = plain(op);
            pairing
                .absorb(LHS, &left, &[0, 10_000, 20_000], &[1.0, 2.0, 3.0])
                .unwrap();
            pairing
                .absorb(RHS, &right, &[0, 10_000], &[9.0, 9.0])
                .unwrap();
            pairs_of(pairing.evaluate().unwrap())
        };

        // `and` keeps the left sample, its value and its name.
        assert_eq!(of(Op::And), [(left.clone(), vec![(0, 1.0), (10_000, 2.0)])]);
        assert_eq!(of(Op::Unless), [(left.clone(), vec![(20_000, 3.0)])]);
        // `or` is every left sample plus the right ones the left did
        // not already answer for, each under its own labels.
        assert_eq!(
            of(Op::Or),
            [(left.clone(), vec![(0, 1.0), (10_000, 2.0), (20_000, 3.0)])]
        );
    }

    /// `or` answers with the right-hand series, name and all, at the
    /// steps the left side left empty — and with nothing of it where
    /// the left side was there.
    #[test]
    fn or_falls_back_to_the_right_hand_series() {
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");
        let mut pairing = plain(Op::Or);
        pairing.absorb(LHS, &left, &[10_000], &[1.0]).unwrap();
        pairing
            .absorb(RHS, &right, &[0, 10_000, 20_000], &[7.0, 8.0, 9.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [
                (right, vec![(0, 7.0), (20_000, 9.0)]),
                (left, vec![(10_000, 1.0)]),
            ]
        );
    }

    /// Two left series in one group are a fan-out error for an
    /// arithmetic operator and nothing at all for a set one: every
    /// signature may hold as many series as it likes on both sides.
    #[test]
    fn a_set_operator_lets_both_sides_be_many() {
        let mut pairing = pairing(Op::And, false, on(&["pod"]));
        let a = lset("requests", "/a", "a", "");
        let b = lset("requests", "/b", "a", "");
        pairing.absorb(LHS, &a, &[0], &[1.0]).unwrap();
        pairing.absorb(LHS, &b, &[0], &[2.0]).unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", "eu"), &[0], &[9.0])
            .unwrap();
        pairing
            .absorb(RHS, &lset("errors", "", "a", "us"), &[0], &[9.0])
            .unwrap();
        assert_eq!(
            pairs_of(pairing.evaluate().unwrap()),
            [(a, vec![(0, 1.0)]), (b, vec![(0, 2.0)])]
        );
    }

    /// A set operator's partial state carries which side each series was
    /// on, which is the only thing the merged group cannot work out for
    /// itself.
    #[test]
    fn partial_states_keep_the_side_a_series_was_on() {
        let left = lset("requests", "", "a", "");
        let right = lset("errors", "", "a", "");
        let mut whole = plain(Op::Unless);
        whole.absorb(LHS, &left, &[0, 10_000], &[1.0, 2.0]).unwrap();
        whole.absorb(RHS, &right, &[10_000], &[9.0]).unwrap();

        let mut a = plain(Op::Unless);
        a.absorb(LHS, &left, &[0, 10_000], &[1.0, 2.0]).unwrap();
        let mut b = plain(Op::Unless);
        b.absorb(RHS, &right, &[10_000], &[9.0]).unwrap();

        let mut merged = plain(Op::Unless);
        for mut partial in [a, b] {
            let state = partial.state().unwrap();
            let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
            merged.merge_batch(&arrays).unwrap();
        }
        assert_eq!(merged.evaluate().unwrap(), whole.evaluate().unwrap());
        assert_eq!(
            pairs_of(merged.evaluate().unwrap()),
            [(left, vec![(0, 1.0)])]
        );
    }

    /// The label set as Prometheus prints it, with the canonical
    /// shape's empty string read as a label that is not there.
    #[test]
    fn a_label_set_prints_as_prometheus_prints_it() {
        let schema = names(&[METRIC_NAME, "job", "pod"]);
        let labels = names(&["up", "api", ""]);
        assert_eq!(
            print(&schema, &labels, |_| true),
            r#"{__name__="up", job="api"}"#
        );
        assert_eq!(
            print(&schema, &labels, |n| n != METRIC_NAME),
            r#"{job="api"}"#
        );
    }
}
