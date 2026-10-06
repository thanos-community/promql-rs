//! End to end: the four ordering functions over the in-memory source.
//!
//! Every assertion here is on the *order* of the rows, which is the whole
//! of what these functions do, so each case reads the result back as a
//! list of series in the order the engine handed them over.

use promql_engine::{Engine, EngineOptions, MemorySeriesSource, RangeQuery, Series};
use promql_parser::ParserOptions;
use promql_parser::SeriesDescription;

const INSTANT: RangeQuery = RangeQuery {
    start_ms: 0,
    end_ms: 0,
    step_ms: 30_000,
    lookback_ms: 300_000,
};

/// `sort_by_label` and `sort_by_label_desc` are experimental functions.
fn engine() -> Engine {
    Engine::blocking_with_options(EngineOptions {
        parser: ParserOptions {
            enable_experimental_functions: true,
            ..Default::default()
        },
    })
    .unwrap()
}

fn load(lines: &[&str]) -> Vec<SeriesDescription> {
    lines
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect()
}

/// The rows a query answers with, in order, each rendered as the labels
/// the case cares about plus its one value.
fn order(lines: &[&str], query: &str, show: &[&str], range: RangeQuery) -> Vec<String> {
    let source = MemorySeriesSource::from_descriptions(&load(lines), 30.0);
    let batches = engine().range_query(&source, query, &range).unwrap();
    promql_engine::series::decode(&batches)
        .expect("the result is canonical")
        .iter()
        .map(|s: &Series| {
            let labels: Vec<&str> = show.iter().map(|l| s.label(l)).collect();
            format!("{}={}", labels.join("/"), s.values()[0])
        })
        .collect()
}

const VALUES: [&str; 5] = [
    r#"x{i="a"} 3"#,
    r#"x{i="b"} 1"#,
    r#"x{i="c"} NaN"#,
    r#"x{i="d"} 2"#,
    r#"x{i="e"} 1"#,
];

/// `funcSort` reverses a NaN-first heap, so NaN lands at the bottom —
/// and `funcSortDesc` reverses the other NaN-first heap, so it lands at
/// the bottom there too, rather than at the top where a plain reversal
/// would put it.
#[test]
fn nan_sorts_last_in_both_directions() {
    let asc = order(&VALUES, "sort(x)", &["i"], INSTANT);
    assert_eq!(asc.last().unwrap(), "c=NaN");
    assert_eq!(&asc[2..4], ["d=2", "a=3"]);

    let desc = order(&VALUES, "sort_desc(x)", &["i"], INSTANT);
    assert_eq!(desc.last().unwrap(), "c=NaN");
    assert_eq!(&desc[0..2], ["a=3", "d=2"]);
}

/// Two series with one value may come back either way round — Go's
/// `sort.Sort` is not stable either — but both must be adjacent and in
/// the block the value puts them in.
#[test]
fn ties_stay_together_at_their_value() {
    let mut tied = order(&VALUES, "sort(x)", &["i"], INSTANT)[0..2].to_vec();
    tied.sort();
    assert_eq!(tied, ["b=1", "e=1"]);
}

/// A range query's result is a matrix, which Prometheus orders by label
/// set whatever the query said, so the function does nothing there. The
/// harness's own ordering assertions only ever ask about instant results.
#[test]
fn a_range_query_is_not_reordered() {
    let range = RangeQuery::new(0, 30_000, 30_000);
    let sorted = order(&VALUES, "sort(x)", &["i"], range);
    let plain = order(&VALUES, "x", &["i"], range);
    assert_eq!(sorted, plain);
}

const LABELS: [&str; 6] = [
    r#"x{cpu="2", host="b"} 1"#,
    r#"x{cpu="10", host="a"} 1"#,
    r#"x{cpu="2", host="a"} 1"#,
    r#"x{cpu="1", host="b"} 1"#,
    r#"x{host="z"} 1"#,
    r#"x{cpu="1", host="a"} 1"#,
];

/// `natsort.Compare`: `cpu="10"` follows `cpu="2"`, not `cpu="1"`.
#[test]
fn label_values_compare_as_natural_numbers() {
    let out = order(&LABELS, r#"sort_by_label(x, "cpu")"#, &["cpu"], INSTANT);
    assert_eq!(out, ["=1", "1=1", "1=1", "2=1", "2=1", "10=1"]);
}

/// Each label in turn, then the full label set as the tiebreak, which is
/// what makes the order of two series equal in every named label
/// reproducible.
#[test]
fn later_labels_and_then_the_whole_set_break_a_tie() {
    let by_cpu = order(
        &LABELS,
        r#"sort_by_label(x, "cpu", "host")"#,
        &["cpu", "host"],
        INSTANT,
    );
    assert_eq!(
        by_cpu,
        ["/z=1", "1/a=1", "1/b=1", "2/a=1", "2/b=1", "10/a=1"]
    );

    // Named no labels at all, the tiebreak is the entire order, and it
    // is `labels.Compare`: name then value, bytewise, so "10" precedes
    // "2" and the series carrying no `cpu` at all comes last, its
    // second pair being `host` where the others' is `cpu`.
    let by_set = order(&LABELS, "sort_by_label(x)", &["cpu", "host"], INSTANT);
    assert_eq!(
        by_set,
        ["1/a=1", "1/b=1", "10/a=1", "2/a=1", "2/b=1", "/z=1"]
    );
}

/// A label no series carries, and a series missing a label others have,
/// both read as `""` — which is what `Metric.Get` answers in Prometheus.
#[test]
fn a_missing_label_sorts_as_the_empty_string() {
    let out = order(&LABELS, r#"sort_by_label(x, "cpu")"#, &["host"], INSTANT);
    assert_eq!(out[0], "z=1", "the series without `cpu` comes first");

    // Nobody has `nope`, so every key ties and only the full label set
    // decides.
    let unknown = order(&LABELS, r#"sort_by_label(x, "nope")"#, &["cpu"], INSTANT);
    let by_set = order(&LABELS, "sort_by_label(x)", &["cpu"], INSTANT);
    assert_eq!(unknown, by_set);
}

/// The `_desc` variants negate the whole comparator, tiebreak included.
#[test]
fn desc_reverses_the_tiebreak_too() {
    let asc = order(
        &LABELS,
        r#"sort_by_label(x, "cpu")"#,
        &["cpu", "host"],
        INSTANT,
    );
    let mut desc = order(
        &LABELS,
        r#"sort_by_label_desc(x, "cpu")"#,
        &["cpu", "host"],
        INSTANT,
    );
    desc.reverse();
    assert_eq!(asc, desc);
}

#[test]
fn the_argument_list_is_checked() {
    let source = MemorySeriesSource::from_descriptions(&load(&VALUES), 30.0);
    let engine = engine();
    for query in ["sort(x, x)", r#"sort_by_label(x, 1)"#] {
        let err = engine
            .range_query(&source, query, &INSTANT)
            .expect_err(query);
        assert!(
            matches!(err, promql_engine::EngineError::Query(_)),
            "{query}: {err}"
        );
    }
}

/// The one row `absent` makes is a synthetic series under labels the
/// planner derived, so the sort keys have to find a value in it and a
/// label field on it like on any selected row. A label it does not
/// carry is `""` for the key, as for any other input.
#[test]
fn the_absent_row_sorts_like_any_other() {
    for query in [
        r#"sort(absent(x{i="z"}))"#,
        r#"sort_desc(absent(x{i="z"}))"#,
        r#"sort_by_label(absent(x{i="z"}), "i")"#,
        r#"sort_by_label_desc(absent(x{i="z"}), "job")"#,
    ] {
        assert_eq!(order(&VALUES, query, &["i"], INSTANT), ["z=1"], "{query}");
    }
    for query in ["sort(absent(x))", "absent(sort(x))"] {
        assert!(order(&VALUES, query, &["i"], INSTANT).is_empty(), "{query}");
    }
}
