//! Label sets and matchers: the slice of Prometheus's `model/labels` and
//! Thanos's `pkg/store` matcher handling that a client needs.

use std::fmt;

use promql_engine::matcher::CompiledMatcher;
use promql_parser::ast::{LabelMatcher, MatchOp};
use promql_parser::posrange::PositionRange;

use crate::error::StoreError;
use crate::storepb::thanos::{label_matcher, Label, LabelMatcher as StoreLabelMatcher, ZLabelSet};

/// The label `storeMatch[]` selectors are checked against.
pub const ADDRESS_LABEL: &str = "__address__";

/// A label set sorted by name. The derived ordering is Prometheus's
/// `labels.Compare`: pair by pair, name then value, and a strict prefix
/// sorts first. Stores send series in this order and results leave in it.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelSet(Vec<(String, String)>);

impl LabelSet {
    /// From pairs in any order. Should a store repeat a name, the first
    /// value wins.
    pub fn new(mut pairs: Vec<(String, String)>) -> Self {
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|later, earlier| later.0 == earlier.0);
        Self(pairs)
    }

    pub fn from_strs(pairs: &[(&str, &str)]) -> Self {
        Self::new(
            pairs
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect(),
        )
    }

    pub fn from_proto(labels: &[Label]) -> Self {
        Self::new(
            labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
        )
    }

    pub fn from_zlabel_set(set: &ZLabelSet) -> Self {
        Self::from_proto(&set.labels)
    }

    /// The value of `name`, `None` when the set does not carry it.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .binary_search_by(|(n, _)| n.as_str().cmp(name))
            .ok()
            .map(|i| self.0[i].1.as_str())
    }

    pub fn pairs(&self) -> &[(String, String)] {
        &self.0
    }

    /// Borrowed pairs, the shape `Series::new` takes.
    pub fn as_strs(&self) -> Vec<(&str, &str)> {
        self.0
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for LabelSet {
    /// `{a="1", b="2"}`, as `labels.Labels.String()` prints.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for (i, (name, value)) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{name}={value:?}")?;
        }
        write!(f, "}}")
    }
}

/// The engine's matchers with their regexes compiled, or the first
/// invalid regex as the engine reports it.
pub fn compile_matchers(matchers: &[LabelMatcher]) -> Result<Vec<CompiledMatcher>, StoreError> {
    matchers
        .iter()
        .map(|m| CompiledMatcher::compile(m).map_err(StoreError::Matcher))
        .collect()
}

/// The engine's matchers on the wire, the inverse of
/// `storepb.MatchersToPromMatchers`. Regex patterns go as written; the
/// server anchors them to the whole value like Prometheus does.
pub fn to_proto_matchers(matchers: &[LabelMatcher]) -> Vec<StoreLabelMatcher> {
    matchers
        .iter()
        .map(|m| StoreLabelMatcher {
            r#type: match m.op {
                MatchOp::Equal => label_matcher::Type::Eq,
                MatchOp::NotEqual => label_matcher::Type::Neq,
                MatchOp::RegexEqual => label_matcher::Type::Re,
                MatchOp::RegexNotEqual => label_matcher::Type::Nre,
            } as i32,
            name: m.name.clone(),
            value: m.value.clone(),
        })
        .collect()
}

/// `storepb.MatchersToPromMatchers`: wire matchers back into the engine's.
pub fn from_proto_matchers(
    matchers: &[StoreLabelMatcher],
) -> Result<Vec<LabelMatcher>, StoreError> {
    matchers
        .iter()
        .map(|m| {
            let op = match label_matcher::Type::try_from(m.r#type) {
                Ok(label_matcher::Type::Eq) => MatchOp::Equal,
                Ok(label_matcher::Type::Neq) => MatchOp::NotEqual,
                Ok(label_matcher::Type::Re) => MatchOp::RegexEqual,
                Ok(label_matcher::Type::Nre) => MatchOp::RegexNotEqual,
                Err(_) => {
                    return Err(StoreError::Internal(format!(
                        "unknown matcher type {} for label {:?}",
                        m.r#type, m.name
                    )))
                }
            };
            Ok(LabelMatcher {
                name: m.name.clone(),
                op,
                value: m.value.clone(),
                pos_range: PositionRange::default(),
            })
        })
        .collect()
}

/// `LabelSetsMatch` from `pkg/store/proxy.go`: whether a store announcing
/// `label_sets` can hold series for `matchers`. The sets are OR-ed, one
/// compatible set is enough, and a store announcing none may hold
/// anything.
pub fn label_sets_match(matchers: &[CompiledMatcher], label_sets: &[LabelSet]) -> bool {
    label_sets.is_empty()
        || label_sets
            .iter()
            .any(|set| label_set_matches(matchers, set))
}

/// One announced set against every matcher. Only labels the set carries
/// take part: `__name__` or a pod label says nothing about a store whose
/// external labels are `{cluster, replica}`.
fn label_set_matches(matchers: &[CompiledMatcher], label_set: &LabelSet) -> bool {
    matchers
        .iter()
        .all(|m| label_set.get(&m.name).is_none_or(|v| m.matches(v)))
}

/// `storeMatchDebugMetadata`: the `storeMatch[]` selectors against
/// `{__address__="host:port"}`. No selectors means every store.
pub fn matchers_match_address(store_matchers: &[Vec<CompiledMatcher>], addr: &str) -> bool {
    if store_matchers.is_empty() {
        return true;
    }
    let address = [LabelSet::from_strs(&[(ADDRESS_LABEL, addr)])];
    store_matchers
        .iter()
        .any(|set| label_sets_match(set, &address))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matcher(name: &str, op: MatchOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            name: name.to_string(),
            op,
            value: value.to_string(),
            pos_range: PositionRange::default(),
        }
    }

    fn compiled(name: &str, op: MatchOp, value: &str) -> CompiledMatcher {
        CompiledMatcher::compile(&matcher(name, op, value)).unwrap()
    }

    #[test]
    fn ordering_is_labels_compare() {
        let a1 = LabelSet::from_strs(&[("a", "1")]);
        let a1b2 = LabelSet::from_strs(&[("b", "2"), ("a", "1")]);
        let a2 = LabelSet::from_strs(&[("a", "2")]);
        let b1 = LabelSet::from_strs(&[("b", "1")]);
        assert!(a1 < a1b2, "a strict prefix sorts first");
        assert!(a1b2 < a2, "values decide before the length does");
        assert!(a2 < b1, "names decide before values");
        assert_eq!(a1b2.pairs()[0].0, "a", "pairs are sorted by name");
    }

    #[test]
    fn get_and_display() {
        let set = LabelSet::from_strs(&[("job", "api"), ("__name__", "up"), ("job", "ignored")]);
        assert_eq!(set.get("__name__"), Some("up"));
        assert_eq!(
            set.get("job"),
            Some("api"),
            "the first value of a repeated name wins"
        );
        assert_eq!(set.get("instance"), None);
        assert_eq!(set.len(), 2);
        assert_eq!(set.to_string(), r#"{__name__="up", job="api"}"#);
        assert_eq!(LabelSet::default().to_string(), "{}");
    }

    #[test]
    fn proto_matchers_carry_the_raw_regex() {
        let out = to_proto_matchers(&[
            matcher("a", MatchOp::Equal, "1"),
            matcher("b", MatchOp::NotEqual, "2"),
            matcher("c", MatchOp::RegexEqual, "x.*"),
            matcher("d", MatchOp::RegexNotEqual, "(y|z)"),
        ]);
        let types: Vec<i32> = out.iter().map(|m| m.r#type).collect();
        assert_eq!(types, vec![0, 1, 2, 3]);
        assert_eq!(out[2].value, "x.*", "no anchors added on the client");
        assert_eq!(out[3].name, "d");

        let back = from_proto_matchers(&out).unwrap();
        let ops: Vec<MatchOp> = back.iter().map(|m| m.op).collect();
        assert_eq!(
            ops,
            vec![
                MatchOp::Equal,
                MatchOp::NotEqual,
                MatchOp::RegexEqual,
                MatchOp::RegexNotEqual
            ]
        );
        assert_eq!(back[2].value, "x.*");
    }

    #[test]
    fn compile_reports_a_bad_regex() {
        let err = compile_matchers(&[matcher("a", MatchOp::RegexEqual, "(")]).unwrap_err();
        assert!(matches!(err, StoreError::Matcher(_)), "{err:?}");
        assert!(err.to_string().contains("invalid regular expression"));
    }

    #[test]
    fn label_sets_match_truth_table() {
        let cluster_a = LabelSet::from_strs(&[("cluster", "a"), ("replica", "0")]);
        let cluster_b = LabelSet::from_strs(&[("cluster", "b"), ("replica", "0")]);
        let name_up = compiled("__name__", MatchOp::Equal, "up");
        let is_a = compiled("cluster", MatchOp::Equal, "a");
        let not_a = compiled("cluster", MatchOp::NotEqual, "a");
        let re_ab = compiled("cluster", MatchOp::RegexEqual, "a|b");
        let re_c = compiled("cluster", MatchOp::RegexEqual, "c");

        assert!(
            label_sets_match(std::slice::from_ref(&is_a), &[]),
            "no announced sets: may hold anything"
        );
        assert!(
            label_sets_match(
                std::slice::from_ref(&name_up),
                std::slice::from_ref(&cluster_a)
            ),
            "absent label: no verdict"
        );
        assert!(label_sets_match(
            std::slice::from_ref(&is_a),
            std::slice::from_ref(&cluster_a)
        ));
        assert!(!label_sets_match(
            std::slice::from_ref(&is_a),
            std::slice::from_ref(&cluster_b)
        ));
        assert!(
            label_sets_match(
                std::slice::from_ref(&is_a),
                &[cluster_b.clone(), cluster_a.clone()]
            ),
            "sets are OR-ed"
        );
        assert!(label_sets_match(&[not_a], std::slice::from_ref(&cluster_b)));
        assert!(label_sets_match(&[re_ab], std::slice::from_ref(&cluster_b)));
        assert!(!label_sets_match(
            &[re_c],
            &[cluster_a.clone(), cluster_b.clone()]
        ));
        assert!(
            !label_sets_match(&[name_up, is_a], &[cluster_b]),
            "one contradiction rules a set out"
        );
    }

    #[test]
    fn store_matchers_select_by_address() {
        let sidecar = compiled(ADDRESS_LABEL, MatchOp::Equal, "sidecar:10901");
        let any_store = compiled(ADDRESS_LABEL, MatchOp::RegexEqual, "store-.*");
        assert!(matchers_match_address(&[], "anything"));
        assert!(matchers_match_address(
            &[vec![sidecar.clone()]],
            "sidecar:10901"
        ));
        assert!(!matchers_match_address(
            &[vec![sidecar.clone()]],
            "store-0:10901"
        ));
        assert!(
            matchers_match_address(&[vec![sidecar], vec![any_store]], "store-0:10901"),
            "selectors are OR-ed"
        );
    }
}
