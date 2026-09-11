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

use promql_parser::ast::{LabelMatcher, MatchOp, VectorSelector};
use promql_parser::posrange::PositionRange;
use regex::Regex;

use crate::error::EngineError;
use crate::series::Series;

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

    /// Whether a series' label set satisfies this matcher; an absent label
    /// is `""`.
    pub fn matches_labels(&self, series: &Series) -> bool {
        self.matches(series.label(&self.name))
    }
}

/// Compile every matcher of a selector, including the one for its metric
/// name.
///
/// Upstream's `newVectorSelector` folds a bare metric name into a
/// `__name__` matcher at parse time; our parser keeps it in `vs.name`, so
/// this is where the two forms meet. A quoted name (`{"up"}`) already
/// arrives as a `__name__` matcher and needs nothing.
pub fn compile_selector(vs: &VectorSelector) -> Result<Vec<CompiledMatcher>, EngineError> {
    effective_matchers(vs)
        .iter()
        .map(CompiledMatcher::compile)
        .collect()
}

/// The selector's matchers with `__name__` synthesized from a bare name.
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

/// Whether a series' label set satisfies every matcher.
pub fn matches_all(matchers: &[CompiledMatcher], series: &Series) -> bool {
    matchers.iter().all(|m| m.matches_labels(series))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(name: &str, op: MatchOp, value: &str) -> CompiledMatcher {
        CompiledMatcher::compile(&LabelMatcher {
            name: name.into(),
            op,
            value: value.into(),
            pos_range: PositionRange::default(),
        })
        .unwrap()
    }

    fn labels(pairs: &[(&str, &str)]) -> Series {
        Series::new(pairs, vec![], vec![]).unwrap()
    }

    #[test]
    fn an_absent_label_is_the_empty_string() {
        let l = labels(&[("a", "x")]);
        assert!(!m("k", MatchOp::Equal, "v").matches_labels(&l));
        assert!(m("k", MatchOp::Equal, "").matches_labels(&l));
        assert!(m("k", MatchOp::NotEqual, "v").matches_labels(&l));
        assert!(m("k", MatchOp::RegexEqual, ".*").matches_labels(&l));
        assert!(!m("k", MatchOp::RegexEqual, ".+").matches_labels(&l));
        assert!(m("k", MatchOp::RegexNotEqual, ".+").matches_labels(&l));
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
        let ms = compile_selector(&vs).unwrap();
        assert_eq!(ms.len(), 2);
        assert!(matches_all(
            &ms,
            &labels(&[("__name__", "up"), ("job", "a")])
        ));
        assert!(!matches_all(
            &ms,
            &labels(&[("__name__", "down"), ("job", "a")])
        ));
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
