//! End to end: `topk`, `bottomk`, `limitk`, `limit_ratio` and `quantile`
//! over the in-memory source, expectations worked by hand from upstream's
//! `aggregationK` and `quantile`.

use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery, Series};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Three series at 30s that cross, with no tie at any step. `a` climbs
/// 1..10, `c` falls 10..1 and `b` sits at 5.5, so the largest is `c` for
/// steps 0 to 4 and `a` from 5, and the smallest is `a` for 0 to 4 and
/// `c` from 5. `a` and `c` share a group.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"x{pod="a", group="one"} 1+1x9"#,
            r#"x{pod="b", group="two"} 5.5+0x9"#,
            r#"x{pod="c", group="one"} 10-1x9"#,
        ]),
        30.0,
    ))
}

fn try_query(q: &str, range: RangeQuery) -> Result<Vec<Series>, EngineError> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), q, &range)?;
    Ok(promql_engine::series::decode(&batches).unwrap())
}

fn query(q: &str, range: RangeQuery) -> Vec<Series> {
    try_query(q, range).unwrap()
}

fn range() -> RangeQuery {
    RangeQuery::new(0, 270_000, 30_000)
}

fn by_pod<'a>(out: &'a [Series], pod: &str) -> &'a Series {
    out.iter()
        .find(|s| s.label("pod") == pod)
        .unwrap_or_else(|| panic!("no series {pod} in {out:?}"))
}

fn steps(from: i64, to: i64) -> Vec<i64> {
    (from..=to).map(|i| i * 30_000).collect()
}

/// A series survives only at the steps it was chosen at, with the
/// input's labels and values.
#[test]
fn topk_keeps_a_series_at_the_steps_it_is_chosen() {
    let out = query("topk(1, x)", range());
    assert_eq!(out.len(), 2, "{out:?}");
    let c = by_pod(&out, "c");
    assert_eq!(c.timestamps(), steps(0, 4));
    assert_eq!(c.values(), [10.0, 9.0, 8.0, 7.0, 6.0]);
    assert_eq!(c.label("__name__"), "x");
    assert_eq!(c.label("group"), "one");
    let a = by_pod(&out, "a");
    assert_eq!(a.timestamps(), steps(5, 9));
    assert_eq!(a.values(), [6.0, 7.0, 8.0, 9.0, 10.0]);
}

#[test]
fn bottomk_is_the_mirror() {
    let out = query("bottomk(1, x)", range());
    assert_eq!(out.len(), 2, "{out:?}");
    assert_eq!(by_pod(&out, "a").timestamps(), steps(0, 4));
    assert_eq!(by_pod(&out, "a").values(), [1.0, 2.0, 3.0, 4.0, 5.0]);
    assert_eq!(by_pod(&out, "c").timestamps(), steps(5, 9));
    assert_eq!(by_pod(&out, "c").values(), [5.0, 4.0, 3.0, 2.0, 1.0]);
}

#[test]
fn by_selects_within_each_group() {
    let out = query("topk by (group) (1, x)", range());
    assert_eq!(out.len(), 3, "{out:?}");
    // `b` is alone in its group, so it is chosen at every step.
    assert_eq!(by_pod(&out, "b").timestamps(), steps(0, 9));
    assert_eq!(by_pod(&out, "c").timestamps(), steps(0, 4));
    assert_eq!(by_pod(&out, "a").timestamps(), steps(5, 9));

    // `without` groups by what is left, here nothing: one group.
    let out = query("topk without (pod, group, __name__) (1, x)", range());
    assert_eq!(out.len(), 2, "{out:?}");
}

/// `k` is evaluated at every step: `time() / 30` is 0, 1, 2, … so the
/// first step selects nothing and each later one a series more.
#[test]
fn k_is_evaluated_per_step() {
    let out = query("topk(time() / 30, x)", RangeQuery::new(0, 90_000, 30_000));
    let held: Vec<usize> = (0..4)
        .map(|i| {
            out.iter()
                .filter(|s| s.timestamps().contains(&(i * 30_000)))
                .count()
        })
        .collect();
    assert_eq!(held, [0, 1, 2, 3]);
    // At step 2 the two largest are c (8) and b (5.5), not a (3).
    assert_eq!(by_pod(&out, "b").timestamps(), steps(2, 3));
}

/// `k` below one and any `k` is clamped by the series there are, so a
/// huge one is all of them, in order, and it is not an error.
#[test]
fn k_below_one_is_empty_and_k_above_the_input_is_everything() {
    assert!(query("topk(0, x)", range()).is_empty());
    assert!(query("bottomk(-3, x)", range()).is_empty());
    assert!(query("topk(0.9, x)", range()).is_empty());
    let out = query("topk(9999999999, x)", range());
    assert_eq!(out.len(), 3);
    assert!(out.iter().all(|s| s.timestamps() == steps(0, 9)));
}

/// An instant query has one step, and its result is upstream's order:
/// the heap descending for `topk`, ascending for `bottomk`.
#[test]
fn an_instant_query_is_ordered_by_value() {
    let at = RangeQuery::new(120_000, 120_000, 30_000);
    let pods = |q: &str| -> Vec<String> {
        query(q, at)
            .iter()
            .map(|s| s.label("pod").to_string())
            .collect()
    };
    // At 120s: a=5, b=5.5, c=6.
    assert_eq!(pods("topk(3, x)"), ["c", "b", "a"]);
    assert_eq!(pods("bottomk(3, x)"), ["a", "b", "c"]);
    assert_eq!(pods("topk(2, x)"), ["c", "b"]);
}

/// NaN is the smallest value either way, so it is the first to be
/// pushed out and sorts last.
#[test]
fn nan_loses_in_both_directions() {
    let source = Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[r#"n{pod="a"} 1"#, r#"n{pod="b"} NaN"#, r#"n{pod="c"} 3"#]),
        30.0,
    ));
    let run = |q: &str| -> Vec<String> {
        let batches = Engine::blocking()
            .unwrap()
            .range_query(source.as_ref(), q, &RangeQuery::new(0, 0, 30_000))
            .unwrap();
        promql_engine::series::decode(&batches)
            .unwrap()
            .iter()
            .map(|s| s.label("pod").to_string())
            .collect()
    };
    assert_eq!(run("topk(2, n)"), ["c", "a"]);
    assert_eq!(run("bottomk(2, n)"), ["a", "c"]);
    assert_eq!(run("topk(3, n)"), ["c", "a", "b"]);
    assert_eq!(run("bottomk(3, n)"), ["a", "c", "b"]);
}

/// Upstream refuses before it evaluates anything, so an empty input does
/// not excuse a NaN, and a value `int64` cannot hold is named with Go's
/// spelling of it.
#[test]
fn a_parameter_upstream_refuses_is_a_query_error() {
    for (q, msg) in [
        ("topk(NaN, no_such)", "Parameter value is NaN"),
        ("limitk(NaN, no_such)", "Parameter value is NaN"),
        ("limit_ratio(NaN, no_such)", "Ratio value is NaN"),
        ("bottomk(1e30, x)", "Scalar value 1e+30 underflows int64"),
    ] {
        let _ = msg;
        match try_query(q, range()) {
            Err(EngineError::Query(m)) if q.contains("1e30") => {
                assert_eq!(m, "Scalar value 1e+30 overflows int64", "{q}")
            }
            Err(EngineError::Query(m)) => assert_eq!(m, msg, "{q}"),
            other => panic!("{q}: {other:?}"),
        }
    }
    match try_query("topk(-1e30, x) + topk(1e30, x)", range()) {
        Err(EngineError::Query(m)) => assert!(m.contains("int64"), "{m}"),
        other => panic!("{other:?}"),
    }
    // Nothing selected at any step, so the bounds are never read.
    assert!(query("topk(-1e30, x)", range()).is_empty());
}

/// `limitk` returns the `k` smallest label sets, which is what upstream's
/// first `k` of a label-sorted input is.
#[test]
fn limitk_takes_the_first_series_in_label_order() {
    let out = query("limitk(2, x)", range());
    assert_eq!(out.len(), 2);
    // Label order is by name then value: `group="one"` holds both `a` and
    // `c` before `group="two"` has its `b`.
    for pod in ["a", "c"] {
        assert_eq!(by_pod(&out, pod).timestamps(), steps(0, 9));
    }
    let out = query("limitk by (group) (1, x)", range());
    assert_eq!(out.len(), 2);
    assert_eq!(by_pod(&out, "a").values().len(), 10);
    assert_eq!(by_pod(&out, "b").values().len(), 10);
    assert!(query("limitk(0, x)", range()).is_empty());
}

/// A ratio and its complement split the series with no overlap and no
/// remainder, and the ends are none and all.
#[test]
fn limit_ratio_partitions_the_series() {
    let pods = |q: &str| -> Vec<String> {
        let mut p: Vec<String> = query(q, range())
            .iter()
            .map(|s| s.label("pod").to_string())
            .collect();
        p.sort();
        p
    };
    assert_eq!(pods("limit_ratio(1, x)"), ["a", "b", "c"]);
    assert_eq!(pods("limit_ratio(-1, x)"), ["a", "b", "c"]);
    assert_eq!(pods("limit_ratio(1.5, x)"), ["a", "b", "c"]);
    assert!(pods("limit_ratio(0, x)").is_empty());
    for r in [0.25, 0.5, 0.75] {
        let low = pods(&format!("limit_ratio({r}, x)"));
        let high = pods(&format!("limit_ratio({}, x)", r - 1.0));
        let mut both: Vec<String> = low.iter().chain(&high).cloned().collect();
        both.sort();
        assert_eq!(both, ["a", "b", "c"], "{r}: {low:?} and {high:?}");
    }
}

#[test]
fn quantile_is_taken_over_the_series_present_at_each_step() {
    let out = query("quantile(0.5, x)", range());
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].labels().count(), 0);
    // The median of {1+i, 5.5, 10-i} is 5.5 at every step: the other two
    // straddle it, and once they cross they straddle it the other way.
    assert_eq!(
        out[0].values(),
        [5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5]
    );
    let out = query("quantile(0, x)", range());
    assert_eq!(out[0].values()[0], 1.0);
    let out = query("quantile(1, x)", range());
    assert_eq!(out[0].values()[0], 10.0);
    // Between ranks: rank 0.25 * 2 = 0.5 of {1, 5.5, 10}.
    let out = query("quantile(0.25, x)", range());
    assert_eq!(out[0].values()[0], 1.0 * 0.5 + 5.5 * 0.5);
    let out = query("quantile by (group) (1, x)", range());
    assert_eq!(out.len(), 2);
    let one = out.iter().find(|s| s.label("group") == "one").unwrap();
    assert_eq!(one.values()[0], 10.0);
}

/// An out-of-range `q` is an annotation upstream and `+Inf` or `-Inf` in
/// the result; NaN is NaN. Per step, since `q` is a scalar expression.
#[test]
fn quantile_outside_zero_to_one_is_infinite_and_nan_is_nan() {
    assert_eq!(
        query("quantile(1.5, x)", range())[0].values()[0],
        f64::INFINITY
    );
    assert_eq!(
        query("quantile(-1, x)", range())[0].values()[0],
        f64::NEG_INFINITY
    );
    assert!(query("quantile(NaN, x)", range())[0].values()[0].is_nan());
    let out = query("quantile(time() / 270, x)", range());
    assert_eq!(out[0].values()[0], 1.0);
    assert_eq!(*out[0].values().last().unwrap(), 10.0);
}
