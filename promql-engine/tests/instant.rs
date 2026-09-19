//! End to end: instant queries over the in-memory source.
//!
//! What is under test is the seam an instant query adds — the result
//! type read off the expression, and the reshape into the vector shape —
//! rather than the evaluation, which `range.rs` and `selector.rs`
//! already hold to its numbers.
//!
//! # The one guard with no end-to-end case
//!
//! `to_vector` rejects a series with other than one point. Nothing
//! `instant_query` can plan produces one: the grid has a single step,
//! and every kernel emits one point per step, so the guard is against a
//! future caller rather than a reachable state. The nearest thing to an
//! end-to-end case is below — real engine output, from a two-step range
//! query, pushed through the same public reshape.

use std::sync::Arc;

use promql_engine::series::{decode, decode_vector, to_vector};
use promql_engine::{Engine, EngineError, InstantResult, MemorySeriesSource};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two counters at 30s: +1 and +2 per step, 16 and 19 samples long.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"http_requests_total{pod="envoy-1"} 1+1x15"#,
            r#"http_requests_total{pod="envoy-2"} 1+2x18"#,
        ]),
        30.0,
    ))
}

fn at(query: &str, at_ms: i64) -> Result<InstantResult, EngineError> {
    Engine::blocking()
        .unwrap()
        .instant_query(source().as_ref(), query, at_ms)
}

/// One vector element as the assertions below read it.
type Row = (Vec<(String, String)>, i64, f64);

/// The vector's rows, sorted by label set. A vector result is not
/// ordered — upstream sorts only a matrix, and an aggregation's rows
/// come out in DataFusion's grouping order — so the assertions below
/// order them rather than asserting on an order nothing promises.
fn vector(query: &str, at_ms: i64) -> Vec<Row> {
    let InstantResult::Vector(batch) = at(query, at_ms).expect("the query runs") else {
        panic!("{query} is a vector-typed expression")
    };
    let mut rows: Vec<_> = decode_vector(&batch)
        .expect("the vector shape decodes")
        .iter()
        .map(|s| {
            (
                s.labels()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<Vec<_>>(),
                s.timestamp(),
                s.value(),
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

#[test]
fn a_selector_is_one_row_per_series_at_the_evaluation_time() {
    let got = vector("http_requests_total", 300_000);
    assert_eq!(got.len(), 2);
    assert_eq!(
        got[0].0,
        vec![
            ("__name__".to_string(), "http_requests_total".to_string()),
            ("pod".to_string(), "envoy-1".to_string()),
        ]
    );
    // Sample 10 of each series, both timestamped with the eval time.
    assert_eq!((got[0].1, got[0].2), (300_000, 11.0));
    assert_eq!((got[1].1, got[1].2), (300_000, 21.0));
}

#[test]
fn an_aggregation_is_one_row_with_the_grouped_labels() {
    let got = vector("sum by (pod) (rate(http_requests_total[5m]))", 300_000);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].0, vec![("pod".to_string(), "envoy-1".to_string())]);
    assert!((got[0].2 - 1.0 / 30.0).abs() < 1e-9, "{got:?}");

    let got = vector("sum(http_requests_total)", 300_000);
    assert_eq!(got.len(), 1);
    assert!(got[0].0.is_empty(), "sum without `by` keeps no labels");
    assert_eq!(got[0].2, 32.0);
}

/// A series with no sample in the lookback window is simply not in the
/// result, rather than a row with a missing point.
#[test]
fn a_series_outside_the_lookback_is_not_in_the_vector() {
    // envoy-1 ends at 450s, envoy-2 at 540s; at 900s only envoy-2 is
    // still within the 5m lookback.
    let got = vector("http_requests_total", 800_000);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0[1].1, "envoy-2");

    assert!(vector("http_requests_total", 2_000_000).is_empty());
}

/// A top-level range selector is the one expression whose instant
/// result is a matrix: the samples themselves, not a point per series.
#[test]
fn a_range_selector_is_a_matrix_of_the_raw_samples() {
    let InstantResult::Matrix(batches) = at("http_requests_total[2m]", 300_000).expect("runs")
    else {
        panic!("a range selector is matrix-typed")
    };
    let series = decode(&batches).expect("canonical batches");
    assert_eq!(series.len(), 2);
    // (180s, 300s]: four samples, keeping their own timestamps.
    assert_eq!(series[0].timestamps(), [210_000, 240_000, 270_000, 300_000]);
    assert_eq!(series[0].values(), [8.0, 9.0, 10.0, 11.0]);
    assert_eq!(series[1].values(), [15.0, 17.0, 19.0, 21.0]);

    // An offset moves the window, not the timestamps.
    let InstantResult::Matrix(batches) =
        at("http_requests_total[1m] offset 1m", 300_000).expect("runs")
    else {
        panic!("still matrix-typed")
    };
    let series = decode(&batches).expect("canonical batches");
    assert_eq!(series[0].timestamps(), [210_000, 240_000]);
}

/// The types nothing plans yet are named as such, so the conformance
/// suite counts them as a missing feature rather than a wrong answer.
#[test]
fn a_scalar_or_string_typed_query_is_unsupported_by_type() {
    for (query, want) in [
        ("42", "a scalar-typed instant query"),
        ("1 + 1", "a scalar-typed instant query"),
        ("time()", "a scalar-typed instant query"),
        (
            "scalar(http_requests_total)",
            "a scalar-typed instant query",
        ),
        (r#""hello""#, "a string-typed instant query"),
    ] {
        let err = at(query, 0).unwrap_err();
        assert!(
            matches!(&err, EngineError::Unsupported(f) if f == want),
            "{query}: {err}"
        );
    }
}

/// The reshape's own guard, over batches a real query produced: two
/// steps means two points per series, which is not a vector and is
/// refused rather than silently read as its first point. See the module
/// doc for why `instant_query` itself cannot get here.
#[test]
fn reshaping_a_multi_step_result_into_a_vector_is_refused() {
    let engine = Engine::blocking().unwrap();
    let two_steps = promql_engine::RangeQuery::new(300_000, 330_000, 30_000);
    let batches = engine
        .range_query(source().as_ref(), "http_requests_total", &two_steps)
        .expect("runs");
    assert_eq!(
        decode(&batches).expect("canonical")[0].timestamps().len(),
        2
    );

    let err = to_vector(&batches, 330_000).unwrap_err();
    assert!(err.contains("has 2"), "{err}");
    assert!(err.contains("one point per series"), "{err}");
}

/// Same expression, same store, both entry points: the instant query is
/// the range query at one step, so the numbers cannot differ.
#[test]
fn an_instant_query_agrees_with_the_one_step_range_query() {
    let engine = Engine::blocking().unwrap();
    let source = source();
    let query = "sum by (pod) (increase(http_requests_total[5m]))";
    let range = promql_engine::RangeQuery::new(300_000, 300_000, 1);
    let ranged = decode(
        &engine
            .range_query(source.as_ref(), query, &range)
            .expect("runs"),
    )
    .expect("canonical");

    let mut by_labels: Vec<(Vec<(String, String)>, f64)> = ranged
        .iter()
        .map(|s| {
            assert_eq!(s.timestamps(), [300_000]);
            (
                s.labels()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                s.values()[0],
            )
        })
        .collect();
    by_labels.sort_by(|a, b| a.0.cmp(&b.0));

    let instant = vector(query, 300_000);
    assert_eq!(instant.len(), by_labels.len());
    for (got, (labels, value)) in instant.iter().zip(&by_labels) {
        assert_eq!((&got.0, got.2), (labels, *value));
    }
}
