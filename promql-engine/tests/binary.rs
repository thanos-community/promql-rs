//! End to end: the arithmetic binary operators over the in-memory
//! source.
//!
//! What is under test is the matching — which series pair up, which
//! labels survive, what an unmatched side leaves behind — rather than
//! the arithmetic, which `binary.rs`'s unit tests hold to Go's `math`
//! value by value.
//!
//! A one-step range stands in for an instant query, which this engine
//! does not have its own entry point for yet: with `start == end` every
//! series carries exactly one value, so a row reads as a vector element.

use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two metrics over the same two pods, plus one pod only `requests`
/// knows about — the series with nothing to match against.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"requests{pod="a"} 10+10x10"#,
            r#"requests{pod="b"} 20+20x10"#,
            r#"requests{pod="c"} 30+30x10"#,
            r#"errors{pod="a"} 1+1x10"#,
            r#"errors{pod="b"} 2+2x10"#,
        ]),
        30.0,
    ))
}

/// The one step every assertion below is read at.
fn at_150s() -> RangeQuery {
    RangeQuery::new(150_000, 150_000, 30_000)
}

/// One vector element: its labels, then its value.
type Row = (Vec<(String, String)>, f64);

/// The result of `query` over `source` at one step, sorted by label set
/// — a vector result has no promised order, and DataFusion's grouping
/// order is not one either.
fn vector_of(
    source: &dyn promql_engine::SeriesSource,
    query: &str,
    range: &RangeQuery,
) -> Vec<Row> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(source, query, range)
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<Row> = promql_engine::series::decode(&batches)
        .expect("the canonical shape decodes")
        .iter()
        .map(|s| {
            assert_eq!(s.values().len(), 1, "{query}: one step, one value");
            (
                s.labels()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                s.values()[0],
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn vector(query: &str) -> Vec<Row> {
    vector_of(source().as_ref(), query, &at_150s())
}

fn error(query: &str) -> EngineError {
    Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), query, &at_150s())
        .expect_err(query)
}

fn pod(name: &str) -> Vec<(String, String)> {
    vec![("pod".to_string(), name.to_string())]
}

/// At 150s: requests are 60, 120, 180 and errors 6, 12.
#[test]
fn a_scalar_on_either_side_applies_to_every_sample() {
    assert_eq!(
        vector("requests * 2"),
        [(pod("a"), 120.0), (pod("b"), 240.0), (pod("c"), 360.0),]
    );
    // The operands keep their order when the scalar is on the left,
    // which is the whole of upstream's `swap`.
    assert_eq!(
        vector("600 / requests"),
        [(pod("a"), 10.0), (pod("b"), 5.0), (pod("c"), 10.0 / 3.0)]
    );
    assert_eq!(
        vector("requests - 600"),
        [(pod("a"), -540.0), (pod("b"), -480.0), (pod("c"), -420.0),]
    );
}

/// `changesMetricSchema` holds for every arithmetic operator, so the
/// result is never the metric it was computed from.
#[test]
fn arithmetic_drops_the_metric_name() {
    for query in ["requests * 2", "2 * requests", "requests / errors"] {
        let names: Vec<_> = vector(query)
            .iter()
            .flat_map(|(labels, _)| labels.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>())
            .collect();
        assert!(
            !names.contains(&"__name__".to_string()),
            "{query}: {names:?}"
        );
    }
}

/// Default matching pairs on every label but `__name__`, so `pod="c"`
/// — which only one side has — is simply not in the result.
#[test]
fn one_to_one_matching_drops_the_series_with_no_partner() {
    assert_eq!(
        vector("requests / errors"),
        [(pod("a"), 10.0), (pod("b"), 10.0)]
    );
    assert_eq!(
        vector("requests + errors"),
        [(pod("a"), 66.0), (pod("b"), 132.0)]
    );
}

/// A label one side does not carry is part of the signature all the
/// same: `{pod="a"}` and `{pod="a", zone="eu"}` are different match
/// groups, so neither of them pairs.
#[test]
fn an_extra_label_on_one_side_is_a_different_match_group() {
    let source = MemorySeriesSource::from_descriptions(
        &load(&[r#"requests{pod="a"} 10"#, r#"errors{pod="a", zone="eu"} 1"#]),
        30.0,
    );
    let range = RangeQuery::new(0, 0, 30_000);
    assert!(vector_of(&source, "requests / errors", &range).is_empty());
}

/// An operand that selects nothing has nothing to pair with, and the
/// answer is the empty vector rather than the other side.
#[test]
fn an_empty_side_yields_an_empty_result() {
    for query in ["requests * missing", "missing * requests"] {
        assert!(vector(query).is_empty(), "{query}");
    }
}

/// Two series in one match group are upstream's two matching errors,
/// in upstream's words.
#[test]
fn a_match_group_with_two_series_on_a_side_fails() {
    let source = MemorySeriesSource::from_descriptions(
        &load(&[
            r#"requests{pod="a"} 10"#,
            r#"retries{pod="a"} 5"#,
            r#"errors{pod="a"} 1"#,
        ]),
        30.0,
    );
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(0, 0, 30_000);

    let err = engine
        .range_query(
            &source,
            r#"{__name__=~"requests|retries"} + errors"#,
            &range,
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "multiple matches for labels: many-to-one matching must be explicit \
             (group_left/group_right)"
        ),
        "{err}"
    );

    let err = engine
        .range_query(
            &source,
            r#"errors + {__name__=~"requests|retries"}"#,
            &range,
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "found duplicate series for the match group {pod=\"a\"} on the right hand-side of \
             the operation: ["
        ),
        "{err}"
    );
    assert!(
        err.contains(
            ";many-to-many matching not allowed: matching labels must be unique on one side"
        ),
        "{err}"
    );
}

/// The operators this commit does not implement are named one by one:
/// the count per feature is what says which is worth doing next.
#[test]
fn the_rest_of_the_operators_are_unsupported_by_name() {
    for (query, feature) in [
        ("requests > errors", "the > comparison operator"),
        ("requests == bool errors", "the == comparison operator"),
        ("requests and errors", "the and set operator"),
        ("requests or errors", "the or set operator"),
        ("requests unless errors", "the unless set operator"),
        ("requests / on(pod) errors", "the on modifier"),
        ("requests / ignoring(zone) errors", "the ignoring modifier"),
        (
            "requests / on(pod) group_left errors",
            "the group_left modifier",
        ),
        (
            "requests / on(pod) group_right errors",
            "the group_right modifier",
        ),
    ] {
        let err = error(query);
        assert!(
            matches!(&err, EngineError::Unsupported(f) if f == feature),
            "{query}: {err}"
        );
    }
}

/// A scalar operand is folded while planning, so one that moves with
/// the step is a gap rather than a wrong answer.
#[test]
fn a_step_varying_scalar_operand_is_unsupported() {
    let range = RangeQuery::new(0, 300_000, 30_000);
    let err = Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), "requests * time()", &range)
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::Unsupported(f)
            if f == "the * operator with a scalar argument that changes between steps"),
        "{err}"
    );
}

/// A range query pairs step by step: a step only one side reached is
/// not in the result, which is what a series that starts late shows.
#[test]
fn matching_is_per_step_not_per_series() {
    let source = MemorySeriesSource::from_descriptions(
        &load(&[r#"requests{pod="a"} 10 20 30"#, r#"errors{pod="a"} _ _ 3"#]),
        30.0,
    );
    // A 30s step over the three scrapes, with the default lookback
    // carrying each sample forward; only the last step has both.
    let range = RangeQuery::new(0, 60_000, 30_000);
    let batches = Engine::blocking()
        .unwrap()
        .range_query(&source, "requests / errors", &range)
        .expect("the query runs");
    let series = promql_engine::series::decode(&batches).expect("the canonical shape decodes");
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].timestamps(), [60_000]);
    assert_eq!(series[0].values(), [10.0]);
}
