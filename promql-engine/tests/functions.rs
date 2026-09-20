//! End to end: the instant-vector functions over the in-memory source.
//!
//! What is under test is the operator's seam — one row in, one row out,
//! `__name__` gone, the scalar arguments folded — rather than the
//! arithmetic, which the unit tests in `elementwise.rs` hold to Go's
//! `math` value by value.
//!
//! A one-step range at 150s stands in for an instant query, which this
//! engine does not have its own entry point for yet: with `start == end`
//! every series carries exactly one value, so a row reads as a vector
//! element.

use std::sync::Arc;

use promql_engine::{Engine, EngineError, MemorySeriesSource, RangeQuery};
use promql_parser::SeriesDescription;

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// Two series 30s apart, one climbing and one falling through zero.
/// The values are fractional so that rounding has something to do:
/// at 150s they are oslo 9.25 and lima -9.75.
fn source() -> Arc<MemorySeriesSource> {
    Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[
            r#"temperature{city="oslo"} 0.5+1.75x10"#,
            r#"temperature{city="lima"} 1.5-2.25x10"#,
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

/// The result of `query` at 150s, sorted by label set — a vector
/// result has no promised order.
fn vector(query: &str) -> Vec<Row> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), query, &at_150s())
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

fn error(query: &str) -> EngineError {
    Engine::blocking()
        .unwrap()
        .range_query(source().as_ref(), query, &at_150s())
        .expect_err(query)
}

#[test]
fn the_store_is_what_the_assertions_below_assume() {
    assert_eq!(
        vector("temperature"),
        vec![
            (
                vec![
                    ("__name__".into(), "temperature".into()),
                    ("city".into(), "lima".into())
                ],
                -9.75
            ),
            (
                vec![
                    ("__name__".into(), "temperature".into()),
                    ("city".into(), "oslo".into())
                ],
                9.25
            ),
        ]
    );
}

/// The label set comes back without `__name__`: every one of these
/// functions emits its sample with `DropName: true`.
#[test]
fn an_elementwise_function_keeps_the_series_and_drops_the_metric_name() {
    let got = vector("abs(temperature)");
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].0, vec![("city".to_string(), "lima".to_string())]);
    assert_eq!(got[0].1, 9.75);
    assert_eq!(got[1].1, 9.25);
}

#[test]
fn each_function_answers_for_each_series() {
    for (query, lima, oslo) in [
        ("abs(temperature)", 9.75, 9.25),
        ("ceil(temperature)", -9.0, 10.0),
        ("floor(temperature)", -10.0, 9.0),
        ("sgn(temperature)", -1.0, 1.0),
        ("clamp_min(temperature, 0)", 0.0, 9.25),
        ("clamp_max(temperature, 0)", -9.75, 0.0),
        ("clamp(temperature, -1, 1)", -1.0, 1.0),
        // Ties round up, so -9.75 goes to -10 and 9.25 to 9.
        ("round(temperature)", -10.0, 9.0),
        ("round(temperature, 4)", -8.0, 8.0),
        ("exp(sgn(temperature))", (-1f64).exp(), 1f64.exp()),
        ("deg(rad(temperature))", -9.75, 9.25),
        ("sqrt(abs(temperature))", 9.75f64.sqrt(), 9.25f64.sqrt()),
    ] {
        let got = vector(query);
        assert_eq!(got.len(), 2, "{query}");
        assert!((got[0].1 - lima).abs() < 1e-12, "{query} for lima: {got:?}");
        assert!((got[1].1 - oslo).abs() < 1e-12, "{query} for oslo: {got:?}");
    }
}

/// A value a function has nothing to say about is still a sample: the
/// series stays in the result carrying a NaN, as upstream's
/// `simpleFloatFunc` leaves it.
#[test]
fn a_nan_is_a_value_and_not_a_missing_sample() {
    let got = vector("ln(temperature)");
    assert_eq!(got.len(), 2);
    assert!(got[0].1.is_nan(), "ln of a negative value is NaN");
    assert!((got[1].1 - 9.25f64.ln()).abs() < 1e-12, "{got:?}");
}

/// `clamp` with the bounds the wrong way round answers with nothing at
/// all — not with the series unchanged, and not with an error.
#[test]
fn a_clamp_whose_bounds_cross_is_the_empty_vector() {
    assert!(vector("clamp(temperature, 1, 0)").is_empty());
    // clamp_min and clamp_max each have one bound at infinity, so
    // neither can ever cross.
    assert_eq!(vector("clamp_min(temperature, 100)").len(), 2);
}

/// A function over a function: the inner one has already dropped the
/// metric name, and the outer runs on the same rows.
#[test]
fn the_operator_stacks_on_a_range_function_and_an_aggregation() {
    // Held against the same rate without the `abs`, so the assertion
    // is about the operator and not about the extrapolation.
    let rate = vector("rate(temperature[2m])");
    let absolute = vector("abs(rate(temperature[2m]))");
    assert_eq!(rate.len(), 2);
    assert!(rate[0].1 < 0.0 && rate[1].1 > 0.0, "{rate:?}");
    for (with, without) in absolute.iter().zip(&rate) {
        assert_eq!(with.0, without.0);
        assert_eq!(with.1, without.1.abs());
    }

    let got = vector("floor(sum(temperature))");
    assert_eq!(got.len(), 1);
    assert!(got[0].0.is_empty());
    assert_eq!(got[0].1, -1.0);

    let got = vector("sum(abs(temperature))");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, 19.0);
}

/// `vector(s)` is the scalar's value as one series with no labels, and
/// so it needs no store at all.
#[test]
fn vector_of_a_scalar_is_one_unlabelled_series() {
    let got = vector("vector(1)");
    assert_eq!(got.len(), 1);
    assert!(got[0].0.is_empty());
    assert_eq!(got[0].1, 1.0);

    // At 150s, `time()` is 150.
    assert_eq!(vector("vector(time())")[0].1, 150.0);
    // And it stays a vector when a function is wrapped round it.
    assert_eq!(vector("abs(vector(-3))")[0].1, 3.0);
}

/// Upstream evaluates a scalar argument per step; this engine folds it
/// while planning, so one that moves is named rather than guessed at.
/// A one-step query has nothing that can move — this is the range path.
#[test]
fn a_scalar_argument_that_moves_over_the_range_is_unsupported_by_name() {
    let engine = Engine::blocking().unwrap();
    let range = RangeQuery::new(0, 120_000, 60_000);
    let err = engine
        .range_query(source().as_ref(), "clamp_max(temperature, time())", &range)
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::Unsupported(f)
            if f == "the clamp_max function with a scalar argument that changes between steps"),
        "{err}"
    );

    // One that does not move is fine over the same range.
    assert!(engine
        .range_query(source().as_ref(), "clamp_max(temperature, 2 * 3)", &range)
        .is_ok());
    // And over one step, even `time()` does not move.
    assert_eq!(vector("clamp_max(temperature, time())")[1].1, 9.25);
}

/// The arity and the argument types are the parser's job upstream, so
/// they are query errors here, in upstream's words.
#[test]
fn a_call_prometheus_would_not_parse_is_a_query_error() {
    for (query, message) in [
        (
            "abs(temperature, 1)",
            "expected 1 argument(s) in call to \"abs\", got 2",
        ),
        (
            "abs(temperature[5m])",
            "expected type instant vector in call to function \"abs\", got range vector",
        ),
        (
            "clamp(temperature, 1)",
            "expected 3 argument(s) in call to \"clamp\", got 2",
        ),
        (
            "vector(temperature)",
            "expected type scalar in call to function \"vector\", got instant vector",
        ),
        ("nope(temperature)", "unknown function with name \"nope\""),
    ] {
        let err = error(query);
        assert_eq!(err.to_string(), message, "{query}");
        assert!(matches!(err, EngineError::Query(_)), "{query}: {err}");
    }
}

/// The values of `query` at `at_ms`, sorted — for the assertions that
/// need an instant other than the 150s [`vector`] uses.
fn values_at(query: &str, at_ms: i64) -> Vec<f64> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(
            source().as_ref(),
            query,
            &RangeQuery::new(at_ms, at_ms, 30_000),
        )
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut vs: Vec<f64> = promql_engine::series::decode(&batches)
        .unwrap()
        .iter()
        .map(|s| s.values()[0])
        .collect();
    vs.sort_by(f64::total_cmp);
    vs
}

/// `timestamp()` over a bare selector reports when the sample was
/// taken, not when it was asked for — the one place lookback is
/// visible in a value.
#[test]
fn timestamp_over_a_selector_is_the_samples_own_time() {
    // Samples land every 30s, so at 150s the sample is the step.
    assert_eq!(values_at("timestamp(temperature)", 150_000), [150.0, 150.0]);
    // At 160s lookback reaches back to the 150s sample, and that is
    // the time reported — the step's 160 would be upstream's answer
    // only if the selector had been stepped first.
    assert_eq!(values_at("timestamp(temperature)", 160_000), [150.0, 150.0]);
    // `__name__` goes, as it does for every DropName function.
    assert_eq!(
        vector("timestamp(temperature)"),
        vec![
            (vec![("city".into(), "lima".into())], 150.0),
            (vec![("city".into(), "oslo".into())], 150.0),
        ]
    );
}

/// Over anything but a bare selector the samples have already been
/// restamped to their step, so that is what `timestamp()` reports —
/// upstream restamps in `rangeEval` for the same reason.
#[test]
fn timestamp_over_a_stepped_expression_is_the_step() {
    assert_eq!(
        values_at("timestamp(abs(temperature))", 160_000),
        [160.0, 160.0]
    );
    assert_eq!(values_at("timestamp(sum(temperature))", 160_000), [160.0]);
    // Parentheses are not an expression, so this is still the selector.
    assert_eq!(
        values_at("timestamp((temperature))", 160_000),
        [150.0, 150.0]
    );
}

/// With `@` the selector is pinned, so every step reports the pinned
/// sample's time rather than its own.
#[test]
fn timestamp_under_an_at_modifier_is_the_pinned_time() {
    assert_eq!(
        values_at("timestamp(temperature @ 60)", 150_000),
        [60.0, 60.0]
    );
    // An offset beside the `@` is discarded rather than stacked with
    // it: upstream overwrites the offset to reach the pinned instant,
    // so this reads 60s and not 30s.
    assert_eq!(
        values_at("timestamp(temperature @ 60 offset 30s)", 150_000),
        [60.0, 60.0]
    );
    // Without the `@` the offset still counts, which is what makes the
    // line above a special case rather than the rule.
    assert_eq!(
        values_at("timestamp(temperature offset 30s)", 150_000),
        [120.0, 120.0]
    );
}

/// `timestamp` takes an instant vector, so a range selector is the
/// type error upstream's own signature makes it.
#[test]
fn timestamp_of_a_range_selector_is_a_type_error() {
    let err = error("timestamp(temperature[5m])");
    assert!(
        matches!(&err, EngineError::Query(m) if m.contains("expected type instant vector")),
        "{err}"
    );
}

/// What the elementwise operator cannot express is still named as the
/// feature it is, not as a bad query.
#[test]
fn the_functions_that_are_not_elementwise_are_still_unsupported() {
    for (query, what) in [
        ("scalar(temperature)", "the scalar function"),
        ("sort(temperature)", "the sort function"),
        ("year(temperature)", "the year function"),
        ("absent(temperature)", "the absent function"),
    ] {
        let err = error(query);
        assert!(
            matches!(&err, EngineError::Unsupported(f) if f == what),
            "{query}: {err}"
        );
    }
}
