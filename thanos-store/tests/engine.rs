//! End to end: fake Thanos stores, `ProxyStore`, `ThanosSeriesSource`, and
//! `promql_engine::Engine` evaluating PromQL over them.
//!
//! The reference is `MemorySeriesSource` holding the same samples: the
//! engine must not be able to tell the two sources apart.

use std::sync::Arc;

use promql_engine::{Engine, MemorySeriesSource, RangeQuery, Series, SeriesSource};
use thanos_store::dedup::dedup_samples;
use thanos_store::testutil::{
    batch_frame, info, raw_chunk, series as store_series, series_frame, serve, warning_frame,
    FakeStore,
};
use thanos_store::{
    Dedup, DeduplicationFunc, EndpointSet, EndpointSetConfig, PartialResponseStrategy, ProxyStore,
    SelectOptions, ThanosSeriesSource,
};
use tonic::Status;

const T0: i64 = 1_700_000_000_000;
const STEP: i64 = 15_000;

/// `n` samples every 15s from `T0`, a counter growing by `inc` each.
fn counter(n: i64, inc: f64) -> Vec<(i64, f64)> {
    (0..n).map(|i| (T0 + i * STEP, i as f64 * inc)).collect()
}

async fn source_over(fakes: Vec<FakeStore>, options: SelectOptions) -> ThanosSeriesSource {
    let mut addrs = Vec::new();
    for fake in fakes {
        let (addr, _server) = serve(Arc::new(fake)).await;
        addrs.push(addr);
    }
    let set = EndpointSet::new(&addrs, &EndpointSetConfig::default()).unwrap();
    set.update().await;
    ThanosSeriesSource::new(Arc::new(ProxyStore::new(Arc::new(set))), options)
}

fn memory_series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> Series {
    let (ts, vs): (Vec<i64>, Vec<f64>) = samples.iter().copied().unzip();
    Series::new(labels, ts, vs).unwrap()
}

/// Labels and samples of every series, sorted by labels so that the
/// engine's output order, which is not part of its contract, does not
/// count.
type Flat = Vec<(Vec<(String, String)>, Vec<(i64, f64)>)>;

fn flatten(series: &[Series]) -> Flat {
    let mut flat: Flat = series
        .iter()
        .map(|s| {
            (
                s.labels()
                    .map(|(n, v)| (n.to_string(), v.to_string()))
                    .collect(),
                s.timestamps()
                    .iter()
                    .copied()
                    .zip(s.values().iter().copied())
                    .collect(),
            )
        })
        .collect();
    flat.sort_by(|a, b| a.0.cmp(&b.0));
    flat
}

/// Two clusters, each behind its own store, one metric with two jobs; the
/// west store splits one series over a plain frame and a batch frame.
fn two_clusters() -> (Vec<FakeStore>, MemorySeriesSource) {
    let east_api = counter(21, 10.0);
    let east_web = counter(21, 5.0);
    let west_api = counter(21, 30.0);
    let east = FakeStore::new(info(
        "sidecar",
        &[&[("cluster", "east")]],
        T0,
        T0 + 20 * STEP,
    ))
    .with_frames(vec![
        series_frame(store_series(
            &[
                ("__name__", "http_requests_total"),
                ("cluster", "east"),
                ("job", "api"),
            ],
            vec![raw_chunk(&east_api)],
        )),
        series_frame(store_series(
            &[
                ("__name__", "http_requests_total"),
                ("cluster", "east"),
                ("job", "web"),
            ],
            vec![raw_chunk(&east_web)],
        )),
    ]);
    let west = FakeStore::new(info(
        "sidecar",
        &[&[("cluster", "west")]],
        T0,
        T0 + 20 * STEP,
    ))
    .with_frames(vec![
        series_frame(store_series(
            &[
                ("__name__", "http_requests_total"),
                ("cluster", "west"),
                ("job", "api"),
            ],
            vec![raw_chunk(&west_api[..10])],
        )),
        batch_frame(vec![store_series(
            &[
                ("__name__", "http_requests_total"),
                ("cluster", "west"),
                ("job", "api"),
            ],
            vec![raw_chunk(&west_api[10..])],
        )]),
    ]);
    let memory = MemorySeriesSource::new(vec![
        memory_series(
            &[
                ("__name__", "http_requests_total"),
                ("cluster", "east"),
                ("job", "api"),
            ],
            &east_api,
        ),
        memory_series(
            &[
                ("__name__", "http_requests_total"),
                ("cluster", "east"),
                ("job", "web"),
            ],
            &east_web,
        ),
        memory_series(
            &[
                ("__name__", "http_requests_total"),
                ("cluster", "west"),
                ("job", "api"),
            ],
            &west_api,
        ),
    ]);
    (vec![east, west], memory)
}

#[tokio::test]
async fn the_engine_cannot_tell_the_stores_from_memory() {
    let (fakes, memory) = two_clusters();
    let thanos = source_over(fakes, SelectOptions::default()).await;
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, 60_000);

    for query in [
        "http_requests_total",
        r#"http_requests_total{cluster="west"}"#,
        r#"http_requests_total{job=~"a.*"}"#,
        "rate(http_requests_total[1m])",
        "sum by (cluster) (rate(http_requests_total[1m]))",
        "sum(http_requests_total)",
        "count_over_time(http_requests_total[2m])",
        r#"http_requests_total{job="nobody"}"#,
    ] {
        let from_thanos = engine
            .range_query_async(&thanos, query, &range)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let from_memory = engine
            .range_query_async(&memory, query, &range)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(
            flatten(&from_thanos),
            flatten(&from_memory),
            "{query} differs between the stores and memory"
        );
        assert!(thanos.take_warnings().is_empty(), "{query}");
    }
}

#[tokio::test]
async fn rate_has_the_expected_value() {
    let (fakes, _) = two_clusters();
    let thanos = source_over(fakes, SelectOptions::default()).await;
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 8 * STEP, T0 + 8 * STEP, 60_000);

    let result = engine
        .range_query_async(
            &thanos,
            r#"rate(http_requests_total{job="api"}[1m])"#,
            &range,
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2, "{result:?}");
    // A counter growing by 10 every 15s rates at 2/3 per second; the
    // west one grows by 30 per 15s.
    assert_eq!(result[0].label("cluster"), "east");
    assert!(
        (result[0].values()[0] - 10.0 / 15.0).abs() < 1e-9,
        "{result:?}"
    );
    assert_eq!(result[1].label("cluster"), "west");
    assert!((result[1].values()[0] - 2.0).abs() < 1e-9, "{result:?}");
}

#[tokio::test]
async fn only_the_stores_that_can_answer_are_asked() {
    let (fakes, _) = two_clusters();
    let thanos = source_over(fakes, SelectOptions::default()).await;
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, 60_000);

    let result = engine
        .range_query_async(
            &thanos,
            r#"sum(http_requests_total{cluster="east"})"#,
            &range,
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    // Both east series, at 60s resolution over four minutes: 5 points.
    assert_eq!(result[0].timestamps().len(), 5);
    assert_eq!(result[0].values()[0], (4.0 * 10.0) + (4.0 * 5.0));
}

#[tokio::test]
async fn warnings_reach_the_caller_and_abort_fails_the_query() {
    let samples = counter(5, 1.0);
    let noisy = || {
        FakeStore::new(info("store", &[], T0, T0 + 4 * STEP)).with_frames(vec![
            warning_frame("block 01X is corrupted"),
            series_frame(store_series(
                &[("__name__", "up")],
                vec![raw_chunk(&samples)],
            )),
        ])
    };
    let engine = Engine::new();
    let range = RangeQuery::new(T0, T0 + 4 * STEP, STEP);

    let warning = source_over(vec![noisy()], SelectOptions::default()).await;
    let result = engine
        .range_query_async(&warning, "up", &range)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(warning.take_warnings(), vec!["block 01X is corrupted"]);
    assert!(warning.take_warnings().is_empty(), "taken means gone");

    let abort = source_over(
        vec![noisy()],
        SelectOptions {
            partial_response: PartialResponseStrategy::Abort,
            ..Default::default()
        },
    )
    .await;
    let err = engine
        .range_query_async(&abort, "up", &range)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("block 01X is corrupted"), "{err}");
}

#[tokio::test]
async fn a_dead_store_is_a_warning_with_partial_data() {
    let samples = counter(5, 1.0);
    let alive = FakeStore::new(info("store", &[&[("replica", "a")]], T0, T0 + 4 * STEP))
        .with_frames(vec![series_frame(store_series(
            &[("__name__", "up"), ("replica", "a")],
            vec![raw_chunk(&samples)],
        ))]);
    let dead = FakeStore::new(info("store", &[&[("replica", "b")]], T0, T0 + 4 * STEP))
        .with_series_status(Status::unavailable("transport is closing"));
    let thanos = source_over(vec![alive, dead], SelectOptions::default()).await;
    let engine = Engine::new();

    let result = engine
        .range_query_async(&thanos, "up", &RangeQuery::new(T0, T0 + 4 * STEP, STEP))
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].label("replica"), "a");
    let warnings = thanos.take_warnings();
    assert_eq!(warnings.len(), 1);
    assert!(
        warnings[0].starts_with("receive series from 127.0.0.1:"),
        "{warnings:?}"
    );
    assert!(
        warnings[0].ends_with("Unavailable: transport is closing"),
        "{warnings:?}"
    );
}

/// One series behind two replica stores and a compacted, replica-less
/// copy of its start, plus a series only the first replica has.
struct Replicated {
    fakes: Vec<FakeStore>,
    api: Vec<Vec<(i64, f64)>>,
    web: Vec<(i64, f64)>,
}

fn replicated() -> Replicated {
    let api_a = counter(21, 10.0);
    // Replica b scrapes five seconds later and sees the counter three ahead.
    let api_b: Vec<(i64, f64)> = (0..21)
        .map(|i| (T0 + 5_000 + i * STEP, i as f64 * 10.0 + 3.0))
        .collect();
    let compacted = api_a[..5].to_vec();
    let web = counter(21, 5.0);
    let store = |replica: &[(&str, &str)], frames: Vec<_>| {
        FakeStore::new(info("sidecar", &[replica], T0, T0 + 20 * STEP)).with_frames(frames)
    };
    let a = store(
        &[("prometheus_replica", "a")],
        vec![
            series_frame(store_series(
                &[
                    ("__name__", "http_requests_total"),
                    ("job", "api"),
                    ("prometheus_replica", "a"),
                ],
                vec![raw_chunk(&api_a)],
            )),
            series_frame(store_series(
                &[
                    ("__name__", "http_requests_total"),
                    ("job", "web"),
                    ("prometheus_replica", "a"),
                ],
                vec![raw_chunk(&web)],
            )),
        ],
    );
    let b = store(
        &[("prometheus_replica", "b")],
        vec![series_frame(store_series(
            &[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("prometheus_replica", "b"),
            ],
            vec![raw_chunk(&api_b)],
        ))],
    );
    let c = store(
        &[],
        vec![series_frame(store_series(
            &[("__name__", "http_requests_total"), ("job", "api")],
            vec![raw_chunk(&compacted)],
        ))],
    );
    Replicated {
        fakes: vec![a, b, c],
        // In the order the step merges them: by first sample, then by the
        // replica label, which the compacted copy lacks.
        api: vec![compacted, api_a, api_b],
        web,
    }
}

/// What the step is expected to produce, as a memory source: the `api`
/// replicas merged for `is_counter`, `web` as is, no replica label.
fn merged_reference(replicated: &Replicated, is_counter: bool) -> MemorySeriesSource {
    MemorySeriesSource::new(vec![
        memory_series(
            &[("__name__", "http_requests_total"), ("job", "api")],
            &dedup_samples(replicated.api.clone(), is_counter),
        ),
        memory_series(
            &[("__name__", "http_requests_total"), ("job", "web")],
            &replicated.web,
        ),
    ])
}

async fn deduplicated(
    engine: &Engine,
    source: &dyn SeriesSource,
    dedup: &Dedup,
    query: &str,
    range: &RangeQuery,
) -> Vec<Series> {
    let plan = engine
        .plan_async(source, query, range)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let plan = dedup.inject(plan).unwrap();
    engine
        .execute_async(plan)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
}

#[tokio::test]
async fn replicas_are_merged_in_the_plan() {
    let replicated = replicated();
    let thanos = source_over(replicated.fakes, SelectOptions::default()).await;
    let replicated = Replicated {
        fakes: Vec::new(),
        ..replicated
    };
    let engine = Engine::with_extension_planners(vec![Dedup::planner()]);
    let dedup = Dedup::new(
        vec!["prometheus_replica".into()],
        DeduplicationFunc::Penalty,
    );
    let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, 60_000);

    for (query, is_counter) in [
        ("http_requests_total", false),
        (r#"http_requests_total{job="api"}"#, false),
        ("sum by (job) (http_requests_total)", false),
        ("count(http_requests_total)", false),
        ("max_over_time(http_requests_total[1m])", false),
        ("rate(http_requests_total[1m])", true),
        ("sum by (job) (increase(http_requests_total[1m]))", true),
        // Grouping by the replica label finds none.
        ("count by (prometheus_replica) (http_requests_total)", false),
    ] {
        let from_thanos = deduplicated(&engine, &thanos, &dedup, query, &range).await;
        let reference = merged_reference(&replicated, is_counter);
        let from_memory = engine
            .range_query_async(&reference, query, &range)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(
            flatten(&from_thanos),
            flatten(&from_memory),
            "{query} differs from the merged reference"
        );
        assert!(
            from_thanos
                .iter()
                .all(|s| s.label("prometheus_replica").is_empty()),
            "{query} kept a replica label"
        );
    }

    // Two series, not four.
    let counted = deduplicated(
        &engine,
        &thanos,
        &dedup,
        "count(http_requests_total)",
        &range,
    )
    .await;
    assert_eq!(counted.len(), 1);
    assert!(counted[0].values().iter().all(|v| *v == 2.0), "{counted:?}");

    // Without the step, every replica is its own series.
    let plain = engine
        .range_query_async(&thanos, "count(http_requests_total)", &range)
        .await
        .unwrap();
    assert!(plain[0].values().iter().all(|v| *v == 4.0), "{plain:?}");
    let plain = engine
        .range_query_async(&thanos, "http_requests_total", &range)
        .await
        .unwrap();
    assert_eq!(plain.len(), 4);
    assert!(thanos.take_warnings().is_empty());
}

#[tokio::test]
async fn a_lagging_replica_is_lifted_under_a_counter_function() {
    let a = vec![(T0, 100.0), (T0 + 10_000, 110.0)];
    let b = vec![(T0 + 35_000, 105.0)];
    let store = |replica: &str, samples: &[(i64, f64)]| {
        FakeStore::new(info("sidecar", &[&[("replica", replica)]], T0, T0 + 60_000)).with_frames(
            vec![series_frame(store_series(
                &[("__name__", "restarts_total"), ("replica", replica)],
                vec![raw_chunk(samples)],
            ))],
        )
    };
    let thanos = source_over(
        vec![store("a", &a), store("b", &b)],
        SelectOptions::default(),
    )
    .await;
    let engine = Engine::with_extension_planners(vec![Dedup::planner()]);
    let dedup = Dedup::new(vec!["replica".into()], DeduplicationFunc::Penalty);
    let range = RangeQuery::new(T0 + 35_000, T0 + 35_000, 1_000);

    // Under `rate` the replica found behind at the switch is lifted to 110,
    // so there is no counter reset to add back in.
    let lifted = MemorySeriesSource::new(vec![memory_series(
        &[("__name__", "restarts_total")],
        &[(T0, 100.0), (T0 + 10_000, 110.0), (T0 + 35_000, 110.0)],
    )]);
    let query = "rate(restarts_total[1m])";
    let from_thanos = deduplicated(&engine, &thanos, &dedup, query, &range).await;
    let expected = engine
        .range_query_async(&lifted, query, &range)
        .await
        .unwrap();
    assert_eq!(flatten(&from_thanos), flatten(&expected));
    assert!(from_thanos[0].values()[0] < 1.0, "{from_thanos:?}");

    // A function that is not a counter function takes the values as they are.
    let raw = MemorySeriesSource::new(vec![memory_series(
        &[("__name__", "restarts_total")],
        &[(T0, 100.0), (T0 + 10_000, 110.0), (T0 + 35_000, 105.0)],
    )]);
    let query = "min_over_time(restarts_total[1m])";
    let from_thanos = deduplicated(&engine, &thanos, &dedup, query, &range).await;
    let expected = engine.range_query_async(&raw, query, &range).await.unwrap();
    assert_eq!(flatten(&from_thanos), flatten(&expected));
    assert_eq!(from_thanos[0].values(), &[100.0]);
}

#[tokio::test]
async fn the_source_can_be_shared_as_a_trait_object() {
    let (fakes, _) = two_clusters();
    let thanos: Arc<dyn SeriesSource> =
        Arc::new(source_over(fakes, SelectOptions::default()).await);
    let engine = Engine::new();
    let result = engine
        .range_query_async(
            thanos.as_ref(),
            "sum(http_requests_total)",
            &RangeQuery::new(T0 + 20 * STEP, T0 + 20 * STEP, 1),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].values(),
        &[20.0 * 10.0 + 20.0 * 5.0 + 20.0 * 30.0]
    );
}
