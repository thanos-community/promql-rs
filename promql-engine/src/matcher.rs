//! Label matcher semantics in plain Rust.
//!
//! This is the reference a [`SeriesSource`](crate::source::SeriesSource)
//! is held to. The in-memory source applies it directly; a real store
//! translates it into its own predicates, and the differential tests
//! decide whether it got that right. The rules, from Prometheus's
//! `labels.Matcher`:
//!
//! - A label a series does not have compares as `""`. So `k!="v"` matches
//!   a series without `k`, and `k=~".*"` does too, while `k=~".+"` does not.
//! - Regexes match the **whole** value. The pattern is wrapped as
//!   `^(?:…)$`; the non-capturing group keeps a top-level `|` from binding
//!   looser than the anchors.
//!
//! The rules are written twice: [`CompiledMatcher::matches`] decides one
//! value and is the reference, `CompiledMatcher::mask` decides a whole
//! label column at once and is what a store should copy. A test holds the
//! two to the same answers.

use datafusion::arrow::array::{
    Array, AsArray, BooleanArray, BooleanBufferBuilder, Scalar, StringViewArray, StructArray,
};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::compute::and;
use datafusion::arrow::compute::kernels::cmp::{eq, neq};
use datafusion::arrow::error::ArrowError;
use promql_parser::ast::{LabelMatcher, MatchOp, VectorSelector};
use promql_parser::posrange::PositionRange;
use regex::Regex;

use crate::error::EngineError;

pub const METRIC_NAME: &str = "__name__";

/// A matcher with its regex compiled once.
#[derive(Debug, Clone)]
pub struct CompiledMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
    regex: Option<Regex>,
}

impl CompiledMatcher {
    pub fn compile(m: &LabelMatcher) -> Result<Self, EngineError> {
        let regex = match m.op {
            MatchOp::RegexEqual | MatchOp::RegexNotEqual => {
                let anchored = format!("^(?:{})$", m.value);
                Some(Regex::new(&anchored).map_err(|e| {
                    EngineError::Query(format!(
                        "invalid regular expression {:?} for label {:?}: {e}",
                        m.value, m.name
                    ))
                })?)
            }
            MatchOp::Equal | MatchOp::NotEqual => None,
        };
        Ok(Self {
            name: m.name.clone(),
            op: m.op,
            value: m.value.clone(),
            regex,
        })
    }

    /// Whether one label value satisfies this matcher.
    pub fn matches(&self, value: &str) -> bool {
        match self.op {
            MatchOp::Equal => value == self.value,
            MatchOp::NotEqual => value != self.value,
            MatchOp::RegexEqual => self.regex.as_ref().expect("compiled").is_match(value),
            MatchOp::RegexNotEqual => !self.regex.as_ref().expect("compiled").is_match(value),
        }
    }

    /// The same decision for every row of a canonical `labels` struct, as
    /// a mask a store can hand to `filter`.
    ///
    /// A label the struct does not carry is `""` for every row, so the
    /// matcher is constant and no column is read at all: the column-wise
    /// reading of "an absent label compares as `""`".
    ///
    /// The returned mask carries no nulls, and callers rely on it:
    /// [`mask_all`] reads the raw buffer as a selection and `filter` drops
    /// a null row instead of keeping it. Only a null in a label column
    /// could produce one, and that row would then fall out of the
    /// selection rather than compare as `""`, so
    /// [`crate::series::validate`] is what keeps both this guarantee and
    /// the agreement with [`Self::matches`].
    ///
    /// `selection` is a hint, not a promise about the returned mask: only
    /// the regex branch ANDs it in, to skip regexes on rows an earlier
    /// matcher already excluded. The compare kernels are vectorised over
    /// the whole column, where a gather would cost more than the compares
    /// it skips, so they answer full width. The mask is therefore only
    /// meaningful ANDed against `selection`, which [`mask_all`] does
    /// for every branch.
    ///
    /// Precondition: `labels` is already [`crate::series::validate`]d. A
    /// wrong-typed column panics in `as_string_view` rather than erroring,
    /// hence `pub(crate)` rather than an API a store could reach before
    /// validating.
    pub(crate) fn mask(
        &self,
        labels: &StructArray,
        selection: Option<&BooleanBuffer>,
    ) -> Result<BooleanArray, ArrowError> {
        let rows = labels.len();
        let Some(column) = labels.column_by_name(&self.name) else {
            let set = if self.matches("") {
                BooleanBuffer::new_set(rows)
            } else {
                BooleanBuffer::new_unset(rows)
            };
            return Ok(BooleanArray::new(set, None));
        };
        let values = column.as_string_view();
        match self.op {
            MatchOp::Equal | MatchOp::NotEqual => {
                let needle = Scalar::new(StringViewArray::from(vec![self.value.as_str()]));
                let cmp = if self.op == MatchOp::Equal { eq } else { neq };
                cmp(&values, &needle)
            }
            // Arrow's `regexp_is_match_scalar` takes a view array but
            // recompiles the pattern from a string, and we hold the
            // anchored `Regex` already, compiled once with the error
            // message the query needs.
            MatchOp::RegexEqual | MatchOp::RegexNotEqual => {
                let regex = self.regex.as_ref().expect("compiled");
                let negated = self.op == MatchOp::RegexNotEqual;
                let mut mask = BooleanBufferBuilder::new(rows);
                for row in 0..rows {
                    let keep = selection.is_none_or(|s| s.value(row))
                        && regex.is_match(values.value(row)) != negated;
                    mask.append(keep);
                }
                Ok(BooleanArray::new(mask.finish(), None))
            }
        }
    }
}

/// The rows of a canonical `labels` struct that satisfy every matcher.
///
/// Stops as soon as nothing is left, which is the common shape of a
/// selector: one narrow `__name__` matcher and then refinements. Seeded
/// with the first matcher's own mask rather than an all-true one, which
/// leaves an empty matcher list as the one case still needing its own
/// all-true answer. Each matcher after the first is handed the running
/// mask as its selection, so a regex only tests the rows still in play.
///
/// Precondition: same as [`CompiledMatcher::mask`], `labels` must already
/// be validated.
pub(crate) fn mask_all(
    matchers: &[CompiledMatcher],
    labels: &StructArray,
) -> Result<BooleanArray, ArrowError> {
    let Some((first, rest)) = matchers.split_first() else {
        return Ok(BooleanArray::new(
            BooleanBuffer::new_set(labels.len()),
            None,
        ));
    };
    let mut kept = first.mask(labels, None)?;
    for m in rest {
        if kept.true_count() == 0 {
            break;
        }
        let next = m.mask(labels, Some(kept.values()))?;
        kept = and(&kept, &next)?;
    }
    Ok(kept)
}

/// The selector's matchers with `__name__` synthesized from a bare name.
///
/// Upstream's `newVectorSelector` folds a bare metric name into a
/// `__name__` matcher at parse time; our parser keeps it in `vs.name`, so
/// this is where the two forms meet. A quoted name (`{"up"}`) already
/// arrives as a `__name__` matcher and needs nothing.
pub fn effective_matchers(vs: &VectorSelector) -> Vec<LabelMatcher> {
    let mut out = vs.label_matchers.clone();
    if !vs.name.is_empty() {
        out.push(LabelMatcher {
            name: METRIC_NAME.to_string(),
            op: MatchOp::Equal,
            value: vs.name.clone(),
            pos_range: PositionRange::default(),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::{encode, label_names_of, Series};

    fn m(name: &str, op: MatchOp, value: &str) -> CompiledMatcher {
        CompiledMatcher::compile(&LabelMatcher {
            name: name.into(),
            op,
            value: value.into(),
            pos_range: PositionRange::default(),
        })
        .unwrap()
    }

    /// One batch of series, as a store holds them.
    fn batch(rows: &[&[(&str, &str)]]) -> StructArray {
        let series: Vec<Series> = rows
            .iter()
            .map(|r| Series::new(r, vec![], vec![]).unwrap())
            .collect();
        let batch = encode(&label_names_of(&series), &series).unwrap();
        batch
            .column_by_name(crate::series::LABELS)
            .unwrap()
            .as_struct()
            .clone()
    }

    fn masked(matcher: &CompiledMatcher, labels: &StructArray) -> Vec<bool> {
        matcher
            .mask(labels, None)
            .unwrap()
            .iter()
            .flatten()
            .collect()
    }

    /// The value a row carries for `name`, by the same rule the masks
    /// follow: a label the row omits is `""`.
    fn value_of(row: &[(&str, &str)], name: &str) -> String {
        row.iter()
            .find(|(n, _)| *n == name)
            .map_or("", |(_, v)| *v)
            .to_string()
    }

    #[test]
    fn an_absent_label_is_the_empty_string() {
        let l = batch(&[&[("a", "x")]]);
        assert_eq!(masked(&m("k", MatchOp::Equal, "v"), &l), [false]);
        assert_eq!(masked(&m("k", MatchOp::Equal, ""), &l), [true]);
        assert_eq!(masked(&m("k", MatchOp::NotEqual, "v"), &l), [true]);
        assert_eq!(masked(&m("k", MatchOp::RegexEqual, ".*"), &l), [true]);
        assert_eq!(masked(&m("k", MatchOp::RegexEqual, ".+"), &l), [false]);
        assert_eq!(masked(&m("k", MatchOp::RegexNotEqual, ".+"), &l), [true]);
    }

    /// The column-wise form is held to the row-wise reference, over the
    /// cases that separate them: a label no row carries, one only some
    /// rows carry, and anchored regexes.
    #[test]
    fn the_mask_and_the_reference_agree() {
        let l = batch(&[
            &[("__name__", "up"), ("pod", "envoy-1")],
            &[("__name__", "up"), ("pod", "envoy-2"), ("route", "/")],
            &[("__name__", "other")],
        ]);
        let values: Vec<Vec<&str>> = ["__name__", "pod", "route", "zone"]
            .iter()
            .map(|name| {
                (0..l.len())
                    .map(|row| {
                        l.column_by_name(name)
                            .map_or("", |c| c.as_string_view().value(row))
                    })
                    .collect()
            })
            .collect();

        for (name, column) in ["__name__", "pod", "route", "zone"].iter().zip(&values) {
            for value in ["up", "", "envoy-1", "/", "envoy-.*", ".+", "envoy-1|other"] {
                for op in [
                    MatchOp::Equal,
                    MatchOp::NotEqual,
                    MatchOp::RegexEqual,
                    MatchOp::RegexNotEqual,
                ] {
                    let matcher = m(name, op, value);
                    let expected: Vec<bool> = column.iter().map(|v| matcher.matches(v)).collect();
                    assert_eq!(masked(&matcher, &l), expected, "{name}{op:?}{value}");
                }
            }
        }
    }

    /// A regex only tests the rows still in play, so the result has to
    /// stay what the two matchers give row-wise — including for a row the
    /// regex is never run on.
    #[test]
    fn a_regex_behind_a_narrow_equality_agrees_with_the_reference() {
        let rows: [&[(&str, &str)]; 4] = [
            &[("__name__", "up"), ("pod", "envoy-1")],
            &[("__name__", "up"), ("pod", "sidecar")],
            // Excluded by `__name__`, and would match the regex.
            &[("__name__", "other"), ("pod", "envoy-2")],
            &[("__name__", "up")],
        ];
        let l = batch(&rows);
        let ms = [
            m("__name__", MatchOp::Equal, "up"),
            m("pod", MatchOp::RegexEqual, "envoy-.+"),
        ];
        let expected: Vec<bool> = rows
            .iter()
            .map(|row| ms.iter().all(|mm| mm.matches(&value_of(row, &mm.name))))
            .collect();
        let got: Vec<bool> = mask_all(&ms, &l).unwrap().iter().flatten().collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn regexes_are_anchored_to_the_whole_value() {
        let re = m("k", MatchOp::RegexEqual, "pyth.*|brave");
        assert!(re.matches("python3"));
        assert!(re.matches("brave"));
        assert!(!re.matches("bravery"));
        assert!(!re.matches("xbrave"));
        assert!(!m("k", MatchOp::RegexEqual, "pyth").matches("python"));
    }

    #[test]
    fn a_bare_name_becomes_a_name_matcher() {
        let vs = match promql_parser::parse_expr(r#"up{job="a"}"#).unwrap() {
            promql_parser::ast::Expr::VectorSelector(vs) => vs,
            other => panic!("{other:?}"),
        };
        let ms: Vec<_> = effective_matchers(&vs)
            .iter()
            .map(|m| CompiledMatcher::compile(m).unwrap())
            .collect();
        assert_eq!(ms.len(), 2);
        let l = batch(&[
            &[("__name__", "up"), ("job", "a")],
            &[("__name__", "down"), ("job", "a")],
            &[("__name__", "up"), ("job", "b")],
        ]);
        assert_eq!(
            mask_all(&ms, &l)
                .unwrap()
                .iter()
                .flatten()
                .collect::<Vec<_>>(),
            [true, false, false]
        );
    }

    #[test]
    fn an_invalid_regex_is_a_query_error() {
        let err = CompiledMatcher::compile(&LabelMatcher {
            name: "k".into(),
            op: MatchOp::RegexEqual,
            value: "(".into(),
            pos_range: PositionRange::default(),
        })
        .unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
    }
}
