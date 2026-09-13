//! Matched pairs back into series:
//! `promql_binary_group(matches, one_labels, match_group, check, card, side, start, end, step)`.
//!
//! A join produces one row per matched *pair*. Several pairs can land on
//! the same output series — that is what `group_left` is for — and the
//! same pair can produce different output series at different steps, when
//! an included label changes value across the window. So the output
//! identity is the result label set, and turning pairs into series is a
//! group-by over exactly those labels, the same shape `sum by (…)` uses.
//!
//! Grouping is also where Prometheus's cardinality rules live, because
//! they are per step: two pairs may share an output series as long as
//! they never want the same step. When they do collide, which of the
//! three errors applies depends on whether the colliding pairs came from
//! one "one"-side series (the many side is ambiguous) or two (the one
//! side was not unique to begin with).

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, ListArray, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Float64Type, TimestampMillisecondType,
};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

use crate::error::QueryError;
use crate::grid::Grid;
use crate::series;

use super::combine::{match_item, matches_type, KEEP};
use super::Card;

pub const NAME: &str = "promql_binary_group";

/// One label set as Prometheus prints it: `{a="1", b="2"}`, absent
/// labels left out, `__name__` in the braces like any other.
fn render(labels: &StructArray, row: usize) -> String {
    let mut out = String::from("{");
    let mut first = true;
    for (i, field) in labels.fields().iter().enumerate() {
        let value = labels.column(i).as_string_view().value(row);
        if value.is_empty() {
            continue;
        }
        if !first {
            out.push_str(", ");
        }
        first = false;
        out.push_str(field.name());
        out.push_str("=\"");
        for c in value.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                _ => out.push(c),
            }
        }
        out.push('"');
    }
    out.push('}');
    out
}

/// A label set's identity, without building the string for it. Only two
/// distinct values per group are ever rendered, for the error message.
fn fingerprint(labels: &StructArray, row: usize) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for i in 0..labels.num_columns() {
        labels
            .column(i)
            .as_string_view()
            .value(row)
            .hash(&mut hasher);
    }
    hasher.finish()
}

/// The accumulator for one output series.
#[derive(Debug)]
pub struct Regroup {
    grid: Grid,
    card: Card,
    /// Which side of the operator the "one" side was written on, for the
    /// duplicate message. `group_right` swaps them.
    side: String,
    seen: Vec<bool>,
    value: Vec<f64>,
    keep: Vec<bool>,
    /// Two pairs wanted the same step.
    duplicate: bool,
    /// Up to two distinct "one"-side series that fed this group, as the
    /// message prints them. Two of them plus a collision is a one side
    /// that was not unique.
    ones: Vec<String>,
    one_prints: Vec<u64>,
    /// The match group of the first "one"-side series, for the message.
    match_group: Option<String>,
}

impl Regroup {
    pub fn new(card: Card, side: String, start_ms: i64, end_ms: i64, step_ms: i64) -> Result<Self> {
        let grid = Grid::new(NAME, start_ms, end_ms, step_ms)?;
        Ok(Self {
            card,
            side,
            seen: vec![false; grid.len],
            value: vec![0.0; grid.len],
            keep: vec![false; grid.len],
            duplicate: false,
            ones: Vec::new(),
            one_prints: Vec::new(),
            match_group: None,
            grid,
        })
    }

    /// One pair's contribution at one step. The first writer wins; a
    /// second is a collision unless the operator is a set operator,
    /// where two rows meeting in one group is the point.
    fn contribute(&mut self, ts: i64, value: f64, keep: bool) -> Result<()> {
        let index = self.grid.index(ts)?;
        if self.seen[index] {
            if self.card != Card::ManyToMany {
                self.duplicate = true;
            }
            return Ok(());
        }
        self.seen[index] = true;
        self.value[index] = value;
        self.keep[index] = keep;
        Ok(())
    }

    /// Remember a "one"-side series, up to the two the message needs.
    fn saw_one(&mut self, labels: &StructArray, row: usize) {
        if self.card == Card::ManyToMany || self.ones.len() >= 2 {
            return;
        }
        let print = fingerprint(labels, row);
        if self.one_prints.contains(&print) {
            return;
        }
        self.one_prints.push(print);
        self.ones.push(render(labels, row));
    }

    fn remember_one(&mut self, printed: String) {
        if self.ones.len() >= 2 || self.ones.contains(&printed) {
            return;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        printed.hash(&mut hasher);
        self.one_prints.push(hasher.finish());
        self.ones.push(printed);
    }

    /// The error this group ended up in, if any.
    ///
    /// Two distinct "one"-side series in a group that also collided is
    /// Prometheus's many-to-many case: it detects that while indexing the
    /// one side, before it ever looks at the many side. Everything else
    /// is the cardinality of the match itself.
    fn conflict(&self) -> Option<String> {
        if !self.duplicate {
            return None;
        }
        if self.ones.len() >= 2 {
            let (mut a, mut b) = (self.ones[0].clone(), self.ones[1].clone());
            if a > b {
                std::mem::swap(&mut a, &mut b);
            }
            let group = self.match_group.clone().unwrap_or_else(|| "{}".into());
            return Some(format!(
                "found duplicate series for the match group {group} on the {} hand-side of the operation: [{a}, {b}]\
;many-to-many matching not allowed: matching labels must be unique on one side",
                self.side
            ));
        }
        Some(match self.card {
            Card::OneToOne => {
                "multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"
            }
            _ => "multiple matches for labels: grouping labels must ensure unique matches",
        }
        .to_string())
    }

    fn samples(&self) -> (Vec<i64>, Vec<f64>) {
        let mut ts = Vec::new();
        let mut vs = Vec::new();
        for i in 0..self.seen.len() {
            if self.seen[i] && self.keep[i] {
                ts.push(self.grid.timestamp(i));
                vs.push(self.value[i]);
            }
        }
        (ts, vs)
    }
}

/// A one-row list holding `entries`.
fn single_row_list(item: FieldRef, entries: StructArray) -> ScalarValue {
    let n = entries.len() as i32;
    ScalarValue::List(Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(vec![0, n].into()),
        Arc::new(entries),
        None,
    )))
}

fn empty_samples() -> ScalarValue {
    single_row_list(
        series::sample_item(),
        StructArray::new(
            series::sample_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new())),
                Arc::new(Float64Array::from(Vec::<f64>::new())),
            ],
            None,
        ),
    )
}

impl Accumulator for Regroup {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let list = values[0].as_list::<i32>();
        let entries = list.values().as_struct();
        let ts = entries
            .column_by_name(series::TIMESTAMP)
            .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: no timestamp column")))?
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let vs = entries
            .column_by_name(series::VALUE)
            .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: no value column")))?
            .as_primitive::<Float64Type>()
            .values();
        let keep = entries
            .column_by_name(KEEP)
            .ok_or_else(|| DataFusionError::Internal(format!("{NAME}: no keep column")))?
            .as_boolean();
        let one = values[1].as_struct();
        let group = values[2].as_struct();
        let offsets = list.offsets();

        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let (a, b) = (offsets[row] as usize, offsets[row + 1] as usize);
            if a == b {
                continue;
            }
            self.saw_one(one, row);
            if self.match_group.is_none() && self.card != Card::ManyToMany {
                self.match_group = Some(render(group, row));
            }
            for i in a..b {
                self.contribute(ts[i], vs[i], keep.value(i))?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if let Some(message) = self.conflict() {
            return Err(QueryError::raise(message));
        }
        let (ts, vs) = self.samples();
        Ok(single_row_list(
            series::sample_item(),
            StructArray::new(
                series::sample_fields(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(ts)),
                    Arc::new(Float64Array::from(vs)),
                ],
                None,
            ),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.seen.capacity()
            + self.keep.capacity()
            + self.value.capacity() * std::mem::size_of::<f64>()
            + self.ones.iter().map(String::len).sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let n = self.seen.iter().filter(|s| **s).count();
        let mut ts = Vec::with_capacity(n);
        let mut vs = Vec::with_capacity(n);
        let mut keep = Vec::with_capacity(n);
        for i in 0..self.seen.len() {
            if !self.seen[i] {
                continue;
            }
            ts.push(self.grid.timestamp(i));
            vs.push(self.value[i]);
            keep.push(self.keep[i]);
        }
        let entries = StructArray::new(
            super::combine::match_fields(),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
                Arc::new(BooleanArray::from(keep)),
            ],
            None,
        );
        let one = |i: usize| ScalarValue::Utf8(self.ones.get(i).cloned());
        Ok(vec![
            single_row_list(match_item(), entries),
            ScalarValue::Boolean(Some(self.duplicate)),
            one(0),
            one(1),
            ScalarValue::Utf8(self.match_group.clone()),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let list = states[0].as_list::<i32>();
        let entries = list.values().as_struct();
        let ts = entries
            .column(0)
            .as_primitive::<TimestampMillisecondType>()
            .values();
        let vs = entries.column(1).as_primitive::<Float64Type>().values();
        let keep = entries.column(2).as_boolean();
        let duplicate = states[1].as_boolean();
        let offsets = list.offsets();

        let text = |array: &ArrayRef, row: usize| -> Option<String> {
            let a = array.as_string::<i32>();
            (!a.is_null(row)).then(|| a.value(row).to_string())
        };
        for row in 0..list.len() {
            if !duplicate.is_null(row) && duplicate.value(row) {
                self.duplicate = true;
            }
            for state in &states[2..4] {
                if let Some(one) = text(state, row) {
                    self.remember_one(one);
                }
            }
            if self.match_group.is_none() {
                self.match_group = text(&states[4], row);
            }
            if list.is_null(row) {
                continue;
            }
            for i in offsets[row] as usize..offsets[row + 1] as usize {
                self.contribute(ts[i], vs[i], keep.value(i))?;
            }
        }
        Ok(())
    }
}

/// The DataFusion function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BinaryGroup {
    signature: Signature,
}

impl Default for BinaryGroup {
    fn default() -> Self {
        Self {
            signature: Signature::any(9, Volatility::Immutable),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(BinaryGroup::default())
}

/// `promql_binary_group(matches, one_labels, match_group, check, '<card>', '<side>', start, end, step)`.
///
/// `check` is the "one" side's uniqueness check. Nothing ever reads its
/// value; it is an argument so that the plan depends on it and the check
/// cannot be optimised away as an unused column.
#[allow(clippy::too_many_arguments)]
pub fn call(
    matches: Expr,
    one_labels: Expr,
    match_group: Expr,
    check: Expr,
    card: Card,
    side: &str,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Expr {
    udaf().call(vec![
        matches,
        one_labels,
        match_group,
        check,
        lit(card.as_str()),
        lit(side),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

impl AggregateUDFImpl for BinaryGroup {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.first() != Some(&matches_type()) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                matches_type(),
                arg_types.first()
            );
        }
        Ok(series::samples_type())
    }

    /// Every group yields a list, possibly empty; never NULL.
    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let literal = |i: usize| {
            args.exprs
                .get(i)
                .and_then(|e| (e.as_ref() as &dyn Any).downcast_ref::<Literal>())
                .map(Literal::value)
        };
        let text = |i: usize, what: &str| {
            literal(i)
                .and_then(|v| match v {
                    ScalarValue::Utf8(Some(s)) => Some(s.clone()),
                    _ => None,
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("{NAME}: {what} must be a string literal"))
                })
        };
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
        let card = text(4, "the cardinality")?;
        let card = Card::parse(&card).ok_or_else(|| {
            DataFusionError::Plan(format!("{NAME}: unknown match cardinality {card}"))
        })?;
        Ok(Box::new(Regroup::new(
            card,
            text(5, "the one side")?,
            number(6, "start")?,
            number(7, "end")?,
            number(8, "step")?,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let field = |name: &str, data_type: DataType, nullable: bool| {
            Arc::new(Field::new(
                format_state_name(args.name, name),
                data_type,
                nullable,
            ))
        };
        Ok(vec![
            field("steps", DataType::List(match_item()), false),
            field("duplicate", DataType::Boolean, false),
            field("one_a", DataType::Utf8, true),
            field("one_b", DataType::Utf8, true),
            field("match_group", DataType::Utf8, true),
        ])
    }

    /// What a group over no rows at all yields: no samples.
    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(empty_samples())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringViewArray;
    use datafusion::arrow::datatypes::Fields;

    fn labels(pairs: &[(&str, &str)]) -> StructArray {
        if pairs.is_empty() {
            return StructArray::new_empty_fields(1, None);
        }
        let fields: Vec<FieldRef> = pairs
            .iter()
            .map(|(n, _)| Arc::new(Field::new(*n, series::label_type(), false)))
            .collect();
        let columns: Vec<ArrayRef> = pairs
            .iter()
            .map(|(_, v)| Arc::new(StringViewArray::from(vec![*v])) as ArrayRef)
            .collect();
        StructArray::try_new(Fields::from(fields), columns, None).unwrap()
    }

    fn group(card: Card) -> Regroup {
        Regroup::new(card, "right".into(), 0, 2, 1).unwrap()
    }

    #[test]
    fn a_label_set_prints_the_way_prometheus_prints_it() {
        let l = labels(&[("__name__", "bar"), ("code", "200"), ("method", "get")]);
        assert_eq!(
            render(&l, 0),
            r#"{__name__="bar", code="200", method="get"}"#
        );
    }

    /// An absent label is `""` in this shape, and absent in the message.
    #[test]
    fn an_empty_value_is_an_absent_label() {
        assert_eq!(render(&labels(&[("a", "1"), ("b", "")]), 0), r#"{a="1"}"#);
        assert_eq!(render(&labels(&[]), 0), "{}");
    }

    #[test]
    fn a_quote_in_a_value_is_escaped() {
        assert_eq!(render(&labels(&[("a", "x\"y")]), 0), r#"{a="x\"y"}"#);
    }

    #[test]
    fn one_contribution_per_step_is_no_conflict() {
        let mut g = group(Card::OneToOne);
        g.saw_one(&labels(&[("__name__", "bar")]), 0);
        g.contribute(0, 1.0, true).unwrap();
        g.contribute(1, 2.0, true).unwrap();
        assert_eq!(g.conflict(), None);
        assert_eq!(g.samples(), (vec![0, 1], vec![1.0, 2.0]));
    }

    /// A filtered comparison still counts as a match: Prometheus checks
    /// for duplicates before it decides whether to keep the sample.
    #[test]
    fn a_filtered_match_still_collides() {
        let mut g = group(Card::OneToOne);
        g.saw_one(&labels(&[("__name__", "bar")]), 0);
        g.contribute(0, 1.0, false).unwrap();
        g.contribute(0, 2.0, false).unwrap();
        assert!(g.conflict().unwrap().contains("must be explicit"));
        assert_eq!(g.samples(), (vec![], vec![]));
    }

    #[test]
    fn two_many_side_rows_on_one_step_is_a_cardinality_error() {
        let mut g = group(Card::ManyToOne);
        g.saw_one(&labels(&[("__name__", "bar")]), 0);
        g.contribute(1, 1.0, true).unwrap();
        g.contribute(1, 2.0, true).unwrap();
        assert_eq!(
            g.conflict().unwrap(),
            "multiple matches for labels: grouping labels must ensure unique matches"
        );
    }

    /// Two different low-cardinality series feeding one output series,
    /// colliding on a step: the one side was never unique.
    #[test]
    fn two_one_side_rows_on_one_step_is_many_to_many() {
        let mut g = group(Card::OneToOne);
        g.match_group = Some(r#"{code="200"}"#.into());
        g.saw_one(&labels(&[("__name__", "bar"), ("method", "post")]), 0);
        g.saw_one(&labels(&[("__name__", "bar"), ("method", "get")]), 0);
        g.contribute(0, 1.0, true).unwrap();
        g.contribute(0, 2.0, true).unwrap();
        assert_eq!(
            g.conflict().unwrap(),
            r#"found duplicate series for the match group {code="200"} on the right hand-side of the operation: [{__name__="bar", method="get"}, {__name__="bar", method="post"}];many-to-many matching not allowed: matching labels must be unique on one side"#
        );
    }

    /// Two low-cardinality series that never share a step are two output
    /// series, not an error: the included label simply changed.
    #[test]
    fn two_one_side_rows_on_different_steps_are_fine() {
        let mut g = group(Card::ManyToOne);
        g.saw_one(&labels(&[("ns", "a")]), 0);
        g.contribute(0, 1.0, true).unwrap();
        g.saw_one(&labels(&[("ns", "b")]), 0);
        g.contribute(1, 2.0, true).unwrap();
        assert_eq!(g.conflict(), None);
    }

    /// Set operators put many rows in one group on purpose.
    #[test]
    fn many_to_many_never_conflicts() {
        let mut g = group(Card::ManyToMany);
        g.contribute(0, 1.0, true).unwrap();
        g.contribute(0, 2.0, true).unwrap();
        assert_eq!(g.conflict(), None);
        assert_eq!(g.samples(), (vec![0], vec![1.0]));
    }
}
