//! End to end: the two functions that rewrite a series' label set.
//!
//! What is under test is the seam — one row in, one row out, the label
//! set built by the projection and the collisions settled above it —
//! so every query here is a one-step range at 150s and only the labels
//! and the one value are read back.

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

/// One element of the answer: its labels, then its value.
type Row = (Vec<(String, String)>, f64);

/// `query` at 150s, sorted by label set — nothing promises an order.
fn rows(query: &str) -> Vec<Row> {
    let batches = Engine::blocking()
        .unwrap()
        .range_query(
            source().as_ref(),
            query,
            &RangeQuery::new(150_000, 150_000, 30_000),
        )
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut out: Vec<Row> = promql_engine::series::decode(&batches)
        .expect("the canonical shape decodes")
        .iter()
        .map(|s| {
            (
                s.labels()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                s.values()[0],
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// The label set of each series, for a query that only rewrites labels.
fn label_sets(query: &str) -> Vec<Vec<(String, String)>> {
    rows(query).into_iter().map(|row| row.0).collect()
}

fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn error(query: &str) -> EngineError {
    Engine::blocking()
        .unwrap()
        .range_query(
            source().as_ref(),
            query,
            &RangeQuery::new(150_000, 150_000, 30_000),
        )
        .expect_err(query)
}

/// `evalLabelReplace`: the destination takes the expanded replacement
/// wherever the regex matches the source, and keeps what it had
/// wherever it does not.
#[test]
fn label_replace_writes_the_destination_from_the_source() {
    assert_eq!(
        label_sets(
            r#"label_replace(temperature{city="oslo"}, "region", "north-$1", "city", "(.*)")"#
        ),
        vec![pairs(&[
            ("__name__", "temperature"),
            ("city", "oslo"),
            ("region", "north-oslo"),
        ])]
    );
    // No match, no write: the series comes through as it was.
    assert_eq!(
        label_sets(r#"label_replace(temperature{city="oslo"}, "region", "north", "city", "lima")"#),
        vec![pairs(&[("__name__", "temperature"), ("city", "oslo")])]
    );
    // A source the series does not carry reads as "", which the
    // anchored regex still matches.
    assert_eq!(
        label_sets(
            r#"label_replace(temperature{city="oslo"}, "region", "any-$1", "nowhere", "(.*)")"#
        ),
        vec![pairs(&[
            ("__name__", "temperature"),
            ("city", "oslo"),
            ("region", "any-"),
        ])],
        "a source the series does not carry expands as empty"
    );
    // An expansion that comes out empty is a label the series does not
    // have: upstream's `Builder.Set("")` deletes rather than writes.
    assert_eq!(
        label_sets(r#"label_replace(temperature{city="oslo"}, "city", "", "city", ".*")"#),
        vec![pairs(&[("__name__", "temperature")])]
    );
    // The metric name is a destination like any other.
    assert_eq!(
        label_sets(
            r#"label_replace(temperature{city="oslo"}, "__name__", "t-$1", "city", "(.*)")"#
        ),
        vec![pairs(&[("__name__", "t-oslo"), ("city", "oslo")])]
    );
    // The value is untouched by any of it.
    assert_eq!(
        rows(r#"label_replace(temperature{city="oslo"}, "region", "n", "city", ".*")"#)[0].1,
        9.25
    );
}

/// `evalLabelJoin`: the sources in order, separated, into one label.
#[test]
fn label_join_concatenates_its_sources() {
    assert_eq!(
        label_sets(r#"label_join(temperature{city="oslo"}, "key", "-", "__name__", "city")"#),
        vec![pairs(&[
            ("__name__", "temperature"),
            ("city", "oslo"),
            ("key", "temperature-oslo"),
        ])]
    );
    // A missing source is "", separator and all.
    assert_eq!(
        label_sets(r#"label_join(temperature{city="oslo"}, "key", "-", "city", "nowhere")"#),
        vec![pairs(&[
            ("__name__", "temperature"),
            ("city", "oslo"),
            ("key", "oslo-"),
        ])]
    );
    // One source and an empty separator is a rename.
    assert_eq!(
        label_sets(r#"label_join(temperature{city="oslo"}, "town", "", "city")"#),
        vec![pairs(&[
            ("__name__", "temperature"),
            ("city", "oslo"),
            ("town", "oslo"),
        ])]
    );
    // No source at all is allowed — the variadic tail may be empty —
    // and joins to "", which removes the destination again.
    assert_eq!(
        label_sets(r#"label_join(temperature{city="oslo"}, "city", ", ")"#),
        vec![pairs(&[("__name__", "temperature")])]
    );
}

/// The three things upstream refuses, in its words: it panics with
/// these, which reaches the user as the query's error.
#[test]
fn the_label_functions_refuse_what_upstream_refuses() {
    for (query, want) in [
        (
            r#"label_replace(temperature, "dst", "", "src", "(.*")"#,
            "invalid regular expression in label_replace(): (.*",
        ),
        (
            r#"label_replace(temperature, "", "", "src", ".*")"#,
            "invalid destination label name in label_replace(): ",
        ),
        (
            r#"label_join(temperature, "", ",", "src")"#,
            "invalid destination label name in label_join(): ",
        ),
        (
            r#"label_join(temperature, "dst", ",", "")"#,
            "invalid source label name in label_join(): ",
        ),
    ] {
        let err = error(query);
        assert!(
            matches!(&err, EngineError::Query(m) if m == want),
            "{query}: {err}"
        );
    }
}

/// Writing a label can give two series one label set. Upstream merges
/// them and refuses only where two of them hold a sample at the same
/// step; both halves of that are here.
#[test]
fn series_that_collide_after_a_write_merge_or_are_refused() {
    // Raised inside the operator and lifted back to a query error, so
    // that it reads as the answer Prometheus gives and not as a broken
    // engine.
    let err = error(r#"label_replace(temperature, "city", "same", "city", ".*")"#);
    assert!(
        matches!(&err, EngineError::Query(m)
            if m == "vector cannot contain metrics with the same labelset"),
        "{err}"
    );

    // The same collision over steps that do not overlap is a merge, not
    // an error: one series holds 1 at 0s, the other 2 at 600s, and the
    // 5m lookback reaches neither across the gap.
    let apart = Arc::new(MemorySeriesSource::from_descriptions(
        &load(&[r#"m{k="a"} 1 _"#, r#"m{k="b"} _ 2"#]),
        600.0,
    ));
    let batches = Engine::blocking()
        .unwrap()
        .range_query(
            apart.as_ref(),
            r#"label_replace(m, "k", "one", "k", ".*")"#,
            &RangeQuery::new(0, 600_000, 600_000),
        )
        .expect("the merge is allowed");
    let out = promql_engine::series::decode(&batches).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].labels().collect::<Vec<_>>(),
        vec![("__name__", "m"), ("k", "one")]
    );
    assert_eq!(out[0].timestamps(), &[0, 600_000]);
    assert_eq!(out[0].values(), &[1.0, 2.0]);
}
