//! Result comparison, ported from `promql-engine`'s `comparer`
//! (`engine/engine_test.go:4531`).
//!
//! This decides what "passing" means for the whole differential suite,
//! so it is written to match Go's semantics rather than to be obvious.
//! It is not equality:
//!
//! - Floats compare approximately, but only when both are finite.
//! - NaN equals NaN.
//! - Series are order-insensitive.
//! - Two errors count as a match whatever the messages say.

use crate::result::{Labels, QueryResult, Sample, Series};

/// Absolute tolerance. `epsilon` in `engine_test.go:4449`.
const EPSILON: f64 = 1e-6;
/// Relative tolerance. `fraction` in `engine_test.go:4450`.
const FRACTION: f64 = 1e-10;

/// Why two results differ. Carries enough context to act on without
/// re-running anything.
#[derive(Debug, Clone, PartialEq)]
pub struct Mismatch(pub String);

impl std::fmt::Display for Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

macro_rules! mismatch {
    ($($arg:tt)*) => { Err(Mismatch(format!($($arg)*))) };
}

/// Compare a result against the oracle's.
///
/// `Ok(())` means the two agree under Go's own comparison rules.
pub fn compare(oracle: &QueryResult, ours: &QueryResult) -> Result<(), Mismatch> {
    // Go's compareErrors: if both sides errored they are equal, because
    // error text legitimately differs between engines (wrappers,
    // series ordering). Only agreement on *failing* is asserted.
    match (oracle, ours) {
        (QueryResult::Error(_), QueryResult::Error(_)) => return Ok(()),
        (QueryResult::Error(e), other) => {
            return mismatch!("oracle errored ({e}) but we returned {}", other.kind());
        }
        (other, QueryResult::Error(e)) => {
            return mismatch!("we errored ({e}) but oracle returned {}", other.kind());
        }
        _ => {}
    }

    match (oracle, ours) {
        (QueryResult::Matrix(a), QueryResult::Matrix(b)) => compare_matrix(a, b),
        (QueryResult::Vector(a), QueryResult::Vector(b)) => compare_vector(a, b),
        (QueryResult::Scalar { v: av, t: at }, QueryResult::Scalar { v: bv, t: bt }) => {
            if at != bt {
                return mismatch!("scalar timestamp: oracle {at}, ours {bt}");
            }
            if !floats_equal(*av, *bv) {
                return mismatch!("scalar value: oracle {av}, ours {bv}");
            }
            Ok(())
        }
        (QueryResult::Str { v: av, t: at }, QueryResult::Str { v: bv, t: bt }) => {
            if at != bt || av != bv {
                return mismatch!("string: oracle {av:?}@{at}, ours {bv:?}@{bt}");
            }
            Ok(())
        }
        (a, b) => mismatch!("result kind: oracle {}, ours {}", a.kind(), b.kind()),
    }
}

fn compare_matrix(oracle: &[Series], ours: &[Series]) -> Result<(), Mismatch> {
    if oracle.len() != ours.len() {
        return mismatch!(
            "series count: oracle {}, ours {}\n  oracle: {}\n  ours:   {}",
            oracle.len(),
            ours.len(),
            label_list(oracle.iter().map(|s| &s.labels)),
            label_list(ours.iter().map(|s| &s.labels)),
        );
    }

    // Go sorts both sides before comparing, so series order is not part
    // of the contract.
    let mut a: Vec<&Series> = oracle.iter().collect();
    let mut b: Vec<&Series> = ours.iter().collect();
    a.sort_by(|x, y| label_key(&x.labels).cmp(&label_key(&y.labels)));
    b.sort_by(|x, y| label_key(&x.labels).cmp(&label_key(&y.labels)));

    for (x, y) in a.iter().zip(b.iter()) {
        if x.labels != y.labels {
            return mismatch!(
                "series labels: oracle {}, ours {}",
                fmt_labels(&x.labels),
                fmt_labels(&y.labels)
            );
        }
        if x.floats.len() != y.floats.len() {
            return mismatch!(
                "{}: sample count: oracle {}, ours {}",
                fmt_labels(&x.labels),
                x.floats.len(),
                y.floats.len()
            );
        }
        for (p, q) in x.floats.iter().zip(y.floats.iter()) {
            if p.t != q.t {
                return mismatch!(
                    "{}: timestamp: oracle {}, ours {}",
                    fmt_labels(&x.labels),
                    p.t,
                    q.t
                );
            }
            if !floats_equal(p.v, q.v) {
                return mismatch!(
                    "{} @{}: oracle {}, ours {}",
                    fmt_labels(&x.labels),
                    p.t,
                    p.v,
                    q.v
                );
            }
        }
    }
    Ok(())
}

fn compare_vector(oracle: &[Sample], ours: &[Sample]) -> Result<(), Mismatch> {
    if oracle.len() != ours.len() {
        return mismatch!(
            "vector length: oracle {}, ours {}",
            oracle.len(),
            ours.len()
        );
    }

    let mut a: Vec<&Sample> = oracle.iter().collect();
    let mut b: Vec<&Sample> = ours.iter().collect();
    a.sort_by(|x, y| label_key(&x.labels).cmp(&label_key(&y.labels)));
    b.sort_by(|x, y| label_key(&x.labels).cmp(&label_key(&y.labels)));

    for (x, y) in a.iter().zip(b.iter()) {
        if x.labels != y.labels {
            return mismatch!(
                "sample labels: oracle {}, ours {}",
                fmt_labels(&x.labels),
                fmt_labels(&y.labels)
            );
        }
        if x.t != y.t {
            return mismatch!(
                "{}: timestamp: oracle {}, ours {}",
                fmt_labels(&x.labels),
                x.t,
                y.t
            );
        }
        if !floats_equal(x.v, y.v) {
            return mismatch!("{}: oracle {}, ours {}", fmt_labels(&x.labels), x.v, y.v);
        }
    }
    Ok(())
}

/// Float comparison matching `cmp.Equal(l, r, EquateNaNs(), EquateApprox(FRACTION, EPSILON))`.
///
/// The three-way split is not decoration. go-cmp guards `EquateApprox`
/// with `FilterValues(areRealF64s, ...)` (`cmpopts/equate.go:63`), so
/// the approximate comparer applies *only* when both values are finite
/// and non-NaN. Everything else falls through to `EquateNaNs` and then
/// exact equality.
///
/// Applying the approximate formula to infinities would be actively
/// wrong: `|inf - inf|` is NaN, and `NaN <= x` is false, so `+Inf` would
/// not equal itself.
pub fn floats_equal(a: f64, b: f64) -> bool {
    if a.is_nan() || b.is_nan() {
        // EquateNaNs: equal exactly when both are NaN.
        return a.is_nan() && b.is_nan();
    }
    if a.is_infinite() || b.is_infinite() {
        // Exact equality, so +Inf == +Inf but +Inf != -Inf.
        return a == b;
    }
    // approximator.compareF64: |x-y| <= max(marg, frac*min(|x|,|y|))
    let rel_margin = FRACTION * a.abs().min(b.abs());
    (a - b).abs() <= EPSILON.max(rel_margin)
}

fn label_key(labels: &Labels) -> String {
    let mut s = String::new();
    for (name, value) in labels {
        s.push_str(name);
        s.push('=');
        s.push_str(value);
        s.push(',');
    }
    s
}

fn fmt_labels(labels: &Labels) -> String {
    let inner: Vec<String> = labels.iter().map(|(n, v)| format!("{n}={v:?}")).collect();
    format!("{{{}}}", inner.join(", "))
}

fn label_list<'a>(sets: impl Iterator<Item = &'a Labels>) -> String {
    let all: Vec<String> = sets.map(fmt_labels).collect();
    if all.is_empty() {
        "<none>".to_string()
    } else {
        all.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::Point;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    fn series(pairs: &[(&str, &str)], points: &[(i64, f64)]) -> Series {
        Series {
            labels: labels(pairs),
            floats: points.iter().map(|(t, v)| Point { t: *t, v: *v }).collect(),
            histograms: 0,
        }
    }

    #[test]
    fn identical_matrices_match() {
        let m = QueryResult::Matrix(vec![series(&[("a", "1")], &[(0, 1.0), (30000, 2.0)])]);
        assert!(compare(&m, &m.clone()).is_ok());
    }

    #[test]
    fn nan_equals_nan() {
        assert!(floats_equal(f64::NAN, f64::NAN));
        assert!(!floats_equal(f64::NAN, 1.0));
        assert!(!floats_equal(1.0, f64::NAN));
    }

    #[test]
    fn infinities_compare_exactly() {
        assert!(floats_equal(f64::INFINITY, f64::INFINITY));
        assert!(floats_equal(f64::NEG_INFINITY, f64::NEG_INFINITY));
        assert!(!floats_equal(f64::INFINITY, f64::NEG_INFINITY));
        assert!(!floats_equal(f64::INFINITY, 1e308));
        // NaN takes precedence over the infinity branch.
        assert!(!floats_equal(f64::INFINITY, f64::NAN));
    }

    #[test]
    fn absolute_tolerance_applies_to_small_values() {
        // Inside the 1e-6 margin.
        assert!(floats_equal(1.0, 1.0 + 9e-7));
        // Outside it.
        assert!(!floats_equal(1.0, 1.0 + 2e-6));
    }

    #[test]
    fn relative_tolerance_applies_to_large_values() {
        // frac*min(|x|,|y|) = 1e-10 * 1e12 = 100, which beats the 1e-6
        // margin, so a difference of 50 is within tolerance.
        assert!(floats_equal(1e12, 1e12 + 50.0));
        assert!(!floats_equal(1e12, 1e12 + 500.0));
    }

    #[test]
    fn series_order_does_not_matter() {
        let a = QueryResult::Matrix(vec![
            series(&[("pod", "a")], &[(0, 1.0)]),
            series(&[("pod", "b")], &[(0, 2.0)]),
        ]);
        let b = QueryResult::Matrix(vec![
            series(&[("pod", "b")], &[(0, 2.0)]),
            series(&[("pod", "a")], &[(0, 1.0)]),
        ]);
        assert!(compare(&a, &b).is_ok());
    }

    #[test]
    fn both_errored_counts_as_a_match() {
        let a = QueryResult::Error("something went wrong".into());
        let b = QueryResult::Error("a totally different message".into());
        assert!(compare(&a, &b).is_ok());
    }

    #[test]
    fn one_sided_error_is_a_mismatch() {
        let err = QueryResult::Error("boom".into());
        let ok = QueryResult::Matrix(vec![]);
        assert!(compare(&err, &ok).is_err());
        assert!(compare(&ok, &err).is_err());
    }

    #[test]
    fn differing_labels_are_a_mismatch() {
        let a = QueryResult::Matrix(vec![series(&[("pod", "a")], &[(0, 1.0)])]);
        let b = QueryResult::Matrix(vec![series(&[("pod", "z")], &[(0, 1.0)])]);
        assert!(compare(&a, &b).is_err());
    }

    #[test]
    fn differing_timestamps_are_a_mismatch() {
        let a = QueryResult::Matrix(vec![series(&[("a", "1")], &[(0, 1.0)])]);
        let b = QueryResult::Matrix(vec![series(&[("a", "1")], &[(30000, 1.0)])]);
        assert!(compare(&a, &b).is_err());
    }

    #[test]
    fn differing_sample_counts_are_a_mismatch() {
        let a = QueryResult::Matrix(vec![series(&[("a", "1")], &[(0, 1.0), (30000, 1.0)])]);
        let b = QueryResult::Matrix(vec![series(&[("a", "1")], &[(0, 1.0)])]);
        assert!(compare(&a, &b).is_err());
    }

    #[test]
    fn differing_kinds_are_a_mismatch() {
        let a = QueryResult::Matrix(vec![]);
        let b = QueryResult::Scalar { v: 1.0, t: 0 };
        assert!(compare(&a, &b).is_err());
    }

    #[test]
    fn mismatch_message_names_the_series_and_timestamp() {
        let a = QueryResult::Matrix(vec![series(&[("pod", "nginx-1")], &[(30000, 1.0)])]);
        let b = QueryResult::Matrix(vec![series(&[("pod", "nginx-1")], &[(30000, 9.0)])]);
        let err = compare(&a, &b).expect_err("values differ");
        assert!(err.0.contains("nginx-1"), "{}", err.0);
        assert!(err.0.contains("30000"), "{}", err.0);
    }
}
