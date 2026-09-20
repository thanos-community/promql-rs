//! End to end: `histogram_quantile` over classic histograms.
//!
//! The arithmetic is pinned value by value in `histogram.rs`; what is
//! under test here is the seam around it — which series form one
//! histogram, which labels the answer carries, and what a bucket family
//! that is not one does. Every query is a one-step range at 150s.

use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery, Series};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Five bucket families under one metric name, each a `path` of its own:
///
/// - `/a` is an ordinary histogram of four observations.
/// - `/b` has everything in its `+Inf` bucket.
/// - `/c` has no `+Inf` bucket at all.
/// - `/d` has one bucket.
/// - `/e` has a bucket label that is not a number.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"rq_bucket{path="/a",le="0.1"} 1+0x10"#,
            r#"rq_bucket{path="/a",le="0.2"} 3+0x10"#,
            r#"rq_bucket{path="/a",le="+Inf"} 4+0x10"#,
            r#"rq_bucket{path="/b",le="1"} 0+0x10"#,
            r#"rq_bucket{path="/b",le="+Inf"} 10+0x10"#,
            r#"rq_bucket{path="/c",le="1"} 1+0x10"#,
            r#"rq_bucket{path="/c",le="2"} 2+0x10"#,
            r#"rq_bucket{path="/d",le="+Inf"} 5+0x10"#,
            r#"rq_bucket{path="/e",le="none"} 1+0x10"#,
            r#"rq_bucket{path="/e",le="+Inf"} 2+0x10"#,
        ]),
        30.0,
    ))
}

/// The one step every query here is evaluated at.
const AT: i64 = 150_000;

fn answer(store: &MemorySeriesSource, query: &str) -> Vec<Series> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(store, query, &RangeQuery::new(AT, AT, 30_000))
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    promql_engine::series::decode(&batches).expect("the canonical shape decodes")
}

/// The result of `query` at 150s, as `path` to value. A result has no
/// promised order, and every series in [`source`] has a `path`.
fn by_path(query: &str) -> Vec<(String, f64)> {
    let mut rows: Vec<(String, f64)> = answer(source().as_ref(), query)
        .iter()
        .map(|s| {
            let path = s
                .labels()
                .find(|(k, _)| *k == "path")
                .map(|(_, v)| v.to_string())
                .unwrap_or_default();
            (path, s.values()[0])
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn at(path: &str, query: &str) -> f64 {
    by_path(query)
        .into_iter()
        .find(|(p, _)| p == path)
        .unwrap_or_else(|| panic!("{query} has no {path}"))
        .1
}

fn error(store: &MemorySeriesSource, query: &str) -> EngineError {
    Engine::blocking()
        .unwrap()
        .range_query(store, query, &RangeQuery::new(AT, AT, 30_000))
        .expect_err(query)
}

/// One row per bucket family, labelled with everything but `le` and the
/// metric name.
#[test]
fn every_bucket_family_answers_once_without_le_or_the_metric_name() {
    let series = answer(source().as_ref(), "histogram_quantile(0.5, rq_bucket)");
    assert_eq!(series.len(), 5, "one per path, not one per bucket");
    for s in &series {
        let labels: Vec<_> = s.labels().map(|(k, _)| k).collect();
        assert_eq!(labels, vec!["path"], "le and __name__ are both gone");
    }
}

/// The interpolation, read through the engine rather than the kernel:
/// `/a` has four observations, so the median's rank is 2, which lands
/// halfway through the (0.1, 0.2] bucket.
#[test]
fn a_quantile_is_interpolated_across_the_family() {
    // Upstream's own `0.1 + (0.2-0.1)*0.5`, digit for digit — not 0.15.
    assert_eq!(
        at("/a", "histogram_quantile(0.5, rq_bucket)"),
        0.15000000000000002
    );
    assert_eq!(at("/a", "histogram_quantile(0.25, rq_bucket)"), 0.1);
    // A rank inside the lowest bucket interpolates from a natural zero.
    assert_eq!(at("/a", "histogram_quantile(0.125, rq_bucket)"), 0.05);
    // Everything in `+Inf` answers with the bound below it.
    assert_eq!(at("/b", "histogram_quantile(0.5, rq_bucket)"), 1.0);
}

/// The families that are not histograms enough to answer: the value is
/// a NaN, and the series is still there carrying it.
#[test]
fn a_family_that_cannot_be_read_answers_nan() {
    let q = "histogram_quantile(0.5, rq_bucket)";
    assert!(at("/c", q).is_nan(), "no +Inf bucket");
    assert!(at("/d", q).is_nan(), "fewer than two buckets");
    // `/e`'s unparseable `le` is dropped as upstream drops it, leaving
    // one bucket, which is also not enough.
    assert!(at("/e", q).is_nan());
}

/// φ's own rules, which are the quantile's before any bucket is read.
#[test]
fn a_quantile_outside_the_unit_interval_is_an_infinity() {
    assert_eq!(
        at("/a", "histogram_quantile(1.5, rq_bucket)"),
        f64::INFINITY
    );
    assert_eq!(
        at("/a", "histogram_quantile(-0.5, rq_bucket)"),
        f64::NEG_INFINITY
    );
    assert!(at("/a", "histogram_quantile(NaN, rq_bucket)").is_nan());
    // The bounds themselves are inside it.
    assert_eq!(at("/a", "histogram_quantile(1, rq_bucket)"), 0.2);
    assert_eq!(at("/a", "histogram_quantile(0, rq_bucket)"), 0.0);
}

/// The buckets do not have to be a selector: anything that yields one
/// series per `le` will do, which is how the function is really used.
#[test]
fn the_buckets_may_be_a_rate_of_the_counters() {
    let rate = at("/a", "histogram_quantile(0.5, rate(rq_bucket[2m]))");
    // The counters are flat, so every rate is zero and the histogram
    // has no observations left to find a quantile in.
    assert!(rate.is_nan(), "{rate}");

    // Summed over the one path each bucket has, the histogram is the
    // same one and so is its median.
    assert_eq!(
        at(
            "/a",
            "histogram_quantile(0.5, sum by (path, le) (rq_bucket))"
        ),
        0.15000000000000002
    );
}

/// One number written four ways is one bucket, holding every count
/// that reached it. The shape is histograms.test:63-67 at the pin,
/// whose `histogram_quantile(0.5, ...)` the corpus expects to be 0.15.
#[test]
fn bucket_bounds_that_spell_one_number_are_one_bucket() {
    let store = MemorySeriesSource::from_descriptions(
        &load(&[
            r#"mixed_bucket{le="0.1"} 1+0x10"#,
            r#"mixed_bucket{le="0.2"} 1+0x10"#,
            r#"mixed_bucket{le="2e-1"} 1+0x10"#,
            r#"mixed_bucket{le="2.0e-1"} 1+0x10"#,
            r#"mixed_bucket{le="+Inf"} 4+0x10"#,
        ]),
        30.0,
    );
    let series = answer(&store, "histogram_quantile(0.5, mixed_bucket)");
    assert_eq!(series.len(), 1);
    // Three of the four observations are at or below 0.2, so the median
    // is halfway through (0.1, 0.2] — not the 0.2 that overwriting the
    // bucket instead of summing it would give.
    assert_eq!(series[0].values()[0], 0.15000000000000002);
}

/// Two metric names over one bucket family are one label set once
/// `__name__` is dropped, which upstream refuses (histograms.test:1078
/// at the pin, itself prometheus/prometheus#9910).
#[test]
fn two_histograms_that_become_one_label_set_are_refused() {
    let store = MemorySeriesSource::from_descriptions(
        &load(&[
            r#"rq_bucket{job="j",le="0.1"} 1+0x10"#,
            r#"rq_bucket{job="j",le="+Inf"} 4+0x10"#,
            r#"rq2_bucket{job="j",le="0.1"} 1+0x10"#,
            r#"rq2_bucket{job="j",le="+Inf"} 4+0x10"#,
        ]),
        30.0,
    );
    let err = error(
        &store,
        r#"histogram_quantile(0.99, {__name__=~"rq\\d*_bucket"})"#,
    );
    assert!(
        matches!(&err, EngineError::Query(m)
            if m == "vector cannot contain metrics with the same labelset"),
        "{err}"
    );
}

/// φ is folded while planning, so one that moves over the range is
/// named rather than taken at its first value.
#[test]
fn a_quantile_that_moves_over_the_range_is_unsupported_by_name() {
    let err = Engine::blocking()
        .unwrap()
        .range_query(
            source().as_ref(),
            "histogram_quantile(time() / 1000, rq_bucket)",
            &RangeQuery::new(0, 120_000, 60_000),
        )
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::Unsupported(f)
            if f == "the histogram_quantile function with a scalar argument that changes between steps"),
        "{err}"
    );
}
