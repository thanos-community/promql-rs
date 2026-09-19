//! End to end: the four ordering functions over the in-memory source.
//!
//! Every assertion here is on the *order* of the rows, which is the whole
//! of what these functions do, so each case reads the result back as a
//! list of series in the order the engine handed them over.

use promql_engine::{Engine, MemorySeriesSource, RangeQuery, Series};
use promql_parser::SeriesDescription;

const INSTANT: RangeQuery = RangeQuery {
    start_ms: 0,
    end_ms: 0,
    step_ms: 30_000,
    lookback_ms: 300_000,
};

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
    let batches = Engine::blocking()
        .unwrap()
        .range_query(&source, query, &range)
        .unwrap();
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

/// A store that hands its series over one partition per series.
///
/// `SeriesSource` says nothing about how many partitions a store's plan
/// has, and a real one — a file per block, a shard per store node —
/// will have several. `MemorySeriesSource` has exactly one, which is
/// the case that cannot catch [the property below].
///
/// [the property below]: the_root_merges_the_parallel_sorts_into_one_partition
#[derive(Debug)]
struct Split(Vec<promql_engine::Series>);

#[async_trait::async_trait]
impl promql_engine::SeriesSource for Split {
    async fn select(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _matchers: &[promql_parser::ast::LabelMatcher],
        _hints: promql_engine::SelectHints,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        let names = promql_engine::series::label_names_of(&self.0);
        let partitions: Vec<Vec<_>> = self
            .0
            .iter()
            .map(|s| {
                vec![
                    promql_engine::series::encode(&names, std::slice::from_ref(s))
                        .expect("one series is one batch"),
                ]
            })
            .collect();
        let schema = promql_engine::series::schema(&names);
        Ok(
            datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
                &partitions,
                schema,
                None,
            )?,
        )
    }
}

/// The load-bearing property behind every assertion above: the plan's
/// root must hand back **one** output partition.
///
/// `Engine` configures nothing, so DataFusion is free to sort each input
/// partition on its own. What makes the result ordered is that the
/// physical planner then caps the root with a
/// `SortPreservingMergeExec` — everything downstream (`collect`,
/// `drop_empty`, `decode`, the conformance harness) only preserves the
/// order it is given, it never establishes one. Two partitions reaching
/// the caller and the rows would interleave by whichever finished
/// first, silently and only on some machines and some stores.
///
/// Five input partitions from [`Split`] and eight target partitions,
/// because a single-partition store or a one-core machine would satisfy
/// this for the wrong reason.
#[test]
fn the_root_merges_the_parallel_sorts_into_one_partition() {
    use datafusion::physical_plan::{collect, ExecutionPlanProperties};
    use datafusion::prelude::{SessionConfig, SessionContext};

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    // `VALUES` again, one series per partition and out of value order.
    let source = Split(
        [
            ("a", 3.0),
            ("b", 1.0),
            ("c", f64::NAN),
            ("d", 2.0),
            ("e", 1.0),
        ]
        .into_iter()
        .map(|(i, v)| Series::new(&[("__name__", "x"), ("i", i)], vec![0], vec![v]).unwrap())
        .collect(),
    );
    let expr = promql_parser::parse_expr("sort(x)").expect("the query parses");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Through `plan::plan` rather than `Engine`, which owns its session
    // and so cannot be told to use more than one partition. The UDFs
    // travel inside the expressions, so none need registering.
    let (partitions, rendered, values) = rt.block_on(async {
        let plan = promql_engine::plan::plan(&ctx.state(), &source, &expr, &INSTANT)
            .await
            .expect("the query plans");
        let physical = ctx
            .state()
            .create_physical_plan(&plan)
            .await
            .expect("the plan lowers");
        let partitions = physical.output_partitioning().partition_count();
        let rendered = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(false)
            .to_string();
        let batches = collect(physical, ctx.task_ctx()).await.expect("it runs");
        let values: Vec<f64> = promql_engine::series::decode(&batches)
            .expect("the result is canonical")
            .iter()
            .map(|s| s.values()[0])
            .collect();
        (partitions, rendered, values)
    });

    assert_eq!(partitions, 1, "the root must not fan out to the caller");
    // Without this the count above would hold for the boring reason
    // that nothing was parallelised in the first place.
    assert!(
        rendered.contains("SortPreservingMergeExec"),
        "the sort ran on one partition, so the merge was never exercised:\n{rendered}"
    );
    assert_eq!(values[0..4], [1.0, 1.0, 2.0, 3.0]);
    assert!(values[4].is_nan(), "{values:?}");
}

#[test]
fn the_argument_list_is_checked() {
    let source = MemorySeriesSource::from_descriptions(&load(&VALUES), 30.0);
    let engine = Engine::blocking().unwrap();
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
