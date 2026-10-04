//! End to end: fake Thanos stores, `ProxyStore`, `ThanosSeriesSource`, and
//! `promql_engine::Engine` evaluating PromQL over them.
//!
//! The reference is `MemorySeriesSource` holding the same samples: the
//! engine must not be able to tell the two sources apart.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use promql_engine::{Engine, MemorySeriesSource, RangeQuery, Series, SeriesSource};
use thanos_store::testutil::{
    batch_frame, info, raw_chunk, series as store_series, series_frame, serve, warning_frame,
    FakeStore,
};
use thanos_store::{
    EndpointSet, EndpointSetConfig, LabelSet, PartialResponseStrategy, ProxyStore, SelectOptions,
    ThanosSeriesSource,
};
use tonic::Status;

const T0: i64 = 1_700_000_000_000;
const STEP: i64 = 15_000;

/// `n` samples every 15s from `T0`, a counter growing by `inc` each.
fn counter(n: i64, inc: f64) -> Vec<(i64, f64)> {
    (0..n).map(|i| (T0 + i * STEP, i as f64 * inc)).collect()
}

async fn source_over(fakes: Vec<FakeStore>, options: SelectOptions) -> ThanosSeriesSource {
    source_over_shared(fakes.into_iter().map(Arc::new).collect(), options).await
}

/// [`source_over`] for a test that inspects the requests afterwards.
async fn source_over_shared(
    fakes: Vec<Arc<FakeStore>>,
    options: SelectOptions,
) -> ThanosSeriesSource {
    let mut addrs = Vec::new();
    for fake in fakes {
        let (addr, _server) = serve(fake).await;
        addrs.push(addr);
    }
    let set = EndpointSet::new(&addrs, &EndpointSetConfig::default()).unwrap();
    set.update().await;
    ThanosSeriesSource::new(Arc::new(ProxyStore::new(Arc::new(set))), options)
}

/// The engine hands back Arrow batches; these assertions are written
/// against series, so every result goes through here.
fn decode(batches: &[RecordBatch]) -> Vec<Series> {
    promql_engine::series::decode(batches).expect("canonical schema")
}

/// [`decode`] with each label set's blocks joined, Prometheus's matrix.
fn coalesced(batches: &[RecordBatch]) -> Vec<Series> {
    promql_engine::series::coalesce(decode(batches)).expect("one label set's rows are adjacent")
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
    let memory = MemorySeriesSource::try_new(vec![
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
    ])
    .unwrap();
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
        let from_thanos = decode(&from_thanos);
        let from_memory = engine
            .range_query_async(&memory, query, &range)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let from_memory = decode(&from_memory);
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
    let result = decode(&result);
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
    let result = decode(&result);
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
    assert_eq!(decode(&result).len(), 1);
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
    let result = decode(&result);
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

/// A store may send the same series once with an empty-valued label and
/// once without it, e.g. two scrapes of the same target where one send
/// omits an optional label. `LabelSet` treats the two as one series.
#[tokio::test]
async fn an_empty_valued_label_merges_with_the_series_without_it() {
    let samples = counter(21, 10.0);
    let store = FakeStore::new(info("store", &[], T0, T0 + 20 * STEP)).with_frames(vec![
        series_frame(store_series(
            &[("__name__", "up"), ("pod", "")],
            vec![raw_chunk(&samples[..10])],
        )),
        series_frame(store_series(
            &[("__name__", "up")],
            vec![raw_chunk(&samples[10..])],
        )),
    ]);
    let thanos = source_over(vec![store], SelectOptions::default()).await;
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP);

    let result = engine
        .range_query_async(&thanos, "up", &range)
        .await
        .unwrap();
    let result = coalesced(&result);
    assert_eq!(
        result.len(),
        1,
        "the empty-valued label merges with the series lacking it"
    );
    assert!(
        result[0].labels().all(|(name, _)| name != "pod"),
        "{result:?}"
    );
    assert!(thanos.take_warnings().is_empty());
}

/// A store that breaks off mid-stream: with partial responses its frames
/// before the break still answer, next to the healthy store's, and the
/// break is a warning; without, the select fails.
#[tokio::test]
async fn a_store_failing_mid_stream_keeps_its_frames_or_fails_the_select() {
    let samples = counter(5, 1.0);
    let stores = || {
        let healthy = FakeStore::new(info("store", &[&[("replica", "a")]], T0, T0 + 4 * STEP))
            .with_frames(vec![series_frame(store_series(
                &[("__name__", "up"), ("replica", "a")],
                vec![raw_chunk(&samples)],
            ))]);
        let breaking = FakeStore::new(info("store", &[&[("replica", "b")]], T0, T0 + 4 * STEP))
            .with_frames(vec![
                series_frame(store_series(
                    &[("__name__", "up"), ("replica", "b")],
                    vec![raw_chunk(&samples)],
                )),
                Err(Status::internal("disk read failed")),
                series_frame(store_series(
                    &[("__name__", "up"), ("replica", "c")],
                    vec![raw_chunk(&samples)],
                )),
            ]);
        vec![healthy, breaking]
    };
    let engine = Engine::new();
    let range = RangeQuery::new(T0, T0 + 4 * STEP, STEP);

    let warn = source_over(stores(), SelectOptions::default()).await;
    let result = engine.range_query_async(&warn, "up", &range).await.unwrap();
    let replicas: Vec<String> = coalesced(&result)
        .iter()
        .map(|s| s.label("replica").to_string())
        .collect();
    assert_eq!(replicas, ["a", "b"], "the frame after the break never came");
    let warnings = warn.take_warnings();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(
        warnings[0].ends_with("Internal: disk read failed"),
        "{warnings:?}"
    );

    let abort = source_over(
        stores(),
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
    assert!(err.to_string().contains("disk read failed"), "{err}");
}

/// However many blocks the range is cut into, every store is asked
/// once, for exactly the select's range.
#[tokio::test]
async fn each_store_is_asked_once_for_the_whole_range() {
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP);
    for block_ms in [STEP, 60_000, thanos_store::DEFAULT_BLOCK_MS] {
        let (fakes, _) = two_clusters();
        let fakes: Vec<Arc<FakeStore>> = fakes.into_iter().map(Arc::new).collect();
        let thanos = source_over_shared(fakes.clone(), SelectOptions::default())
            .await
            .blocks(block_ms);
        engine
            .range_query_async(&thanos, "rate(http_requests_total[1m])", &range)
            .await
            .unwrap();
        for fake in &fakes {
            let requests = fake.series_requests();
            assert_eq!(requests.len(), 1, "{block_ms}ms blocks: {requests:?}");
            assert!(!requests[0].skip_chunks);
            assert_eq!(
                (requests[0].min_time, requests[0].max_time),
                (T0 + 4 * STEP - 60_000, T0 + 20 * STEP),
                "{block_ms}ms blocks"
            );
        }
    }
}

/// Clipped to a block's reach, a chunk that starts first can have its
/// first remaining sample after another chunk's: `x` holds T0, T0 + 1
/// step and T0 + 40 steps, `y` the dense run between. Every block of the
/// range reaches back past T0 + 1 step no further, so `x` keeps only its
/// last sample; emitting by `min_time` would put it first and fail the
/// engine's order check. The two overlap, so a block reaching T0 would
/// let `x` win and drop `y`: the range stays clear of that.
/// Go Thanos sends `step: 0` for an instant query, and the planner's
/// `None` is what makes the request say so; a range query sends its step.
#[tokio::test]
async fn an_instant_query_sends_no_step_and_a_range_query_its_own() {
    let engine = Engine::new();
    for (range, step) in [
        (RangeQuery::new(T0 + 8 * STEP, T0 + 8 * STEP, 60_000), 0),
        (RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP), STEP),
    ] {
        let (fakes, _) = two_clusters();
        let fakes: Vec<Arc<FakeStore>> = fakes.into_iter().map(Arc::new).collect();
        let thanos = source_over_shared(fakes.clone(), SelectOptions::default()).await;
        engine
            .range_query_async(&thanos, "rate(http_requests_total[1m])", &range)
            .await
            .unwrap();
        for fake in &fakes {
            let requests = fake.series_requests();
            assert_eq!(requests.len(), 1, "{requests:?}");
            assert_eq!(requests[0].step, step, "{range:?}");
        }
    }
}

#[tokio::test]
async fn clipping_that_reorders_chunks_still_ascends() {
    let all = counter(41, 1.0);
    let x: Vec<(i64, f64)> = [all[0], all[1], all[40]].to_vec();
    let y: Vec<(i64, f64)> = all[2..40].to_vec();
    let store =
        FakeStore::new(info("store", &[], T0, T0 + 40 * STEP)).with_frames(vec![series_frame(
            store_series(&[("__name__", "up")], vec![raw_chunk(&x), raw_chunk(&y)]),
        )]);
    let memory =
        MemorySeriesSource::try_new(vec![memory_series(&[("__name__", "up")], &all)]).unwrap();
    let thanos = source_over(vec![store], SelectOptions::default())
        .await
        .blocks(60_000);
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 25 * STEP, T0 + 40 * STEP, STEP);
    for query in ["up", "rate(up[1m])", "count_over_time(up[2m])"] {
        let from_thanos = engine
            .range_query_async(&thanos, query, &range)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let from_memory = engine
            .range_query_async(&memory, query, &range)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(
            flatten(&coalesced(&from_thanos)),
            flatten(&coalesced(&from_memory)),
            "{query}"
        );
    }
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
    let result = decode(&result);
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].values(),
        &[20.0 * 10.0 + 20.0 * 5.0 + 20.0 * 30.0]
    );
}

const QUERIES: &[&str] = &[
    "http_requests_total",
    r#"http_requests_total{cluster="west"}"#,
    "rate(http_requests_total[1m])",
    "sum by (cluster) (rate(http_requests_total[1m]))",
    "sum(http_requests_total)",
    "count_over_time(http_requests_total[2m])",
    "increase(http_requests_total[5m])",
];

/// Blocks are the source's business: however the range is cut, the
/// engine answers what one block over the whole range answers. Edges fall
/// on steps, between them and inside the rate windows.
#[tokio::test]
async fn several_blocks_answer_as_one_block_does() {
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP);
    let (fakes, _) = two_clusters();
    let whole = source_over(fakes, SelectOptions::default())
        .await
        .blocks(i64::MAX);
    for block_ms in [STEP, 40_000, 60_000, 7 * 60_000 / 2] {
        let (fakes, _) = two_clusters();
        let cut = source_over(fakes, SelectOptions::default())
            .await
            .blocks(block_ms);
        for query in QUERIES {
            let one = engine
                .range_query_async(&whole, query, &range)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let many = engine
                .range_query_async(&cut, query, &range)
                .await
                .unwrap_or_else(|e| panic!("{query} in {block_ms}ms blocks: {e}"));
            assert_eq!(
                flatten(&coalesced(&many)),
                flatten(&coalesced(&one)),
                "{query} in {block_ms}ms blocks"
            );
        }
        let rows = engine
            .range_query_async(&cut, "http_requests_total", &range)
            .await
            .unwrap();
        assert!(
            decode(&rows).len() > 3,
            "{block_ms}ms blocks should give each of the 3 series several rows"
        );
        assert!(cut.take_warnings().is_empty());
    }
}

/// The same series from two stores, one sending a duplicate of a chunk
/// and a chunk overlapping two others: the engine keeps the first chunk's
/// samples, so the answer is the single store's.
#[tokio::test]
async fn overlapping_and_duplicate_chunks_from_two_stores_answer_as_one_store() {
    let samples = counter(21, 10.0);
    let labels = [("__name__", "up"), ("job", "api")];
    let one = || {
        FakeStore::new(info("store", &[], T0, T0 + 20 * STEP)).with_frames(vec![series_frame(
            store_series(
                &labels,
                vec![raw_chunk(&samples[..11]), raw_chunk(&samples[11..])],
            ),
        )])
    };
    let again = || {
        FakeStore::new(info("store", &[], T0, T0 + 20 * STEP)).with_frames(vec![batch_frame(vec![
            store_series(
                &labels,
                vec![raw_chunk(&samples[5..16]), raw_chunk(&samples[..11])],
            ),
        ])])
    };
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP);
    for block_ms in [thanos_store::DEFAULT_BLOCK_MS, 60_000] {
        let single = source_over(vec![one()], SelectOptions::default())
            .await
            .blocks(block_ms);
        let both = source_over(vec![one(), again()], SelectOptions::default())
            .await
            .blocks(block_ms);
        for query in ["up", "rate(up[1m])", "count_over_time(up[2m])", "sum(up)"] {
            let want = engine
                .range_query_async(&single, query, &range)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let got = engine
                .range_query_async(&both, query, &range)
                .await
                .unwrap_or_else(|e| panic!("{query} from two stores: {e}"));
            assert_eq!(
                flatten(&coalesced(&got)),
                flatten(&coalesced(&want)),
                "{query} in {block_ms}ms blocks"
            );
        }
    }
}

/// Thanos sends series in `labels.Compare` order, `LabelSet`'s; the
/// engine checks DataFusion's struct order, which compares field by field
/// with `""` for an absent label. Over the fields `__name__, a, b, c`,
/// `{a="1", c="1"}` is `(x, 1, "", 1)` and precedes `{a="1", b="2"}`,
/// `(x, 1, 2, "")`, where `labels.Compare` has it the other way round;
/// `{b="1"}` goes from last to first. Only a source that sorts each block
/// itself gets through the engine's order check.
#[tokio::test]
async fn label_sets_with_different_names_pass_the_order_check() {
    let sets: [&[(&str, &str)]; 4] = [
        &[("__name__", "x"), ("a", "1")],
        &[("__name__", "x"), ("a", "1"), ("b", "2")],
        &[("__name__", "x"), ("a", "1"), ("c", "1")],
        &[("__name__", "x"), ("b", "1")],
    ];
    assert!(
        sets.windows(2)
            .all(|w| LabelSet::from_strs(w[0]) < LabelSet::from_strs(w[1])),
        "the store sends them in labels.Compare order"
    );
    let samples: Vec<Vec<(i64, f64)>> = (0..sets.len())
        .map(|i| counter(21, i as f64 + 1.0))
        .collect();
    let store = FakeStore::new(info("store", &[], T0, T0 + 20 * STEP)).with_frames(
        sets.iter()
            .zip(&samples)
            .map(|(labels, samples)| {
                series_frame(store_series(
                    labels,
                    vec![raw_chunk(&samples[..8]), raw_chunk(&samples[8..])],
                ))
            })
            .collect(),
    );
    let memory = MemorySeriesSource::try_new(
        sets.iter()
            .zip(&samples)
            .map(|(labels, samples)| memory_series(labels, samples))
            .collect(),
    )
    .unwrap();
    let thanos = source_over(vec![store], SelectOptions::default())
        .await
        .blocks(60_000);
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP);
    for query in ["x", "rate(x[1m])", "sum by (a) (x)"] {
        let from_thanos = engine
            .range_query_async(&thanos, query, &range)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let from_memory = engine
            .range_query_async(&memory, query, &range)
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(
            flatten(&coalesced(&from_thanos)),
            flatten(&coalesced(&from_memory)),
            "{query}"
        );
    }
}

/// One chunk spanning five one-minute blocks is decoded for each and
/// clipped to each block's reach, so the rate windows that cross an edge
/// still see every sample they need.
#[tokio::test]
async fn a_chunk_straddling_block_edges_feeds_every_block_it_reaches() {
    let samples = counter(21, 10.0);
    let store = Arc::new(
        FakeStore::new(info("store", &[], T0, T0 + 20 * STEP)).with_frames(vec![series_frame(
            store_series(&[("__name__", "up")], vec![raw_chunk(&samples)]),
        )]),
    );
    let thanos = source_over_shared(vec![Arc::clone(&store)], SelectOptions::default())
        .await
        .blocks(60_000);
    let engine = Engine::new();
    let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP);

    let result = engine
        .range_query_async(&thanos, "rate(up[1m])", &range)
        .await
        .unwrap();
    assert!(decode(&result).len() > 1, "one row per block");
    let result = coalesced(&result);
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].timestamps(),
        (4..=20).map(|i| T0 + i * STEP).collect::<Vec<_>>()
    );
    // 10 every 15s, whichever block the window end fell in.
    for v in result[0].values() {
        assert!((v - 10.0 / 15.0).abs() < 1e-9, "{:?}", result[0].values());
    }

    assert_eq!(store.series_requests().len(), 1);
}

// Replica deduplication.

/// One block of replicas of `x` that jitter by a few seconds: the first
/// replica lost its samples 8 to 11, the second kept them.
mod replicas {
    use super::*;
    use datafusion::physical_plan::displayable;
    use promql_engine::explain_plan;
    use thanos_store::dedup::DeduplicationFunc;
    use thanos_store::source::Dedup;

    const JITTER: i64 = 3_000;
    const STALE_NAN: u64 = 0x7ff0000000000002;

    fn a() -> Vec<(i64, f64)> {
        counter(21, 10.0)
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !(8..=11).contains(i))
            .map(|(_, s)| s)
            .collect()
    }

    fn b() -> Vec<(i64, f64)> {
        counter(21, 10.0)
            .into_iter()
            .map(|(t, v)| (t + JITTER, v))
            .collect()
    }

    /// What the penalty merge hands out: replica a until its gap, then b,
    /// which stays ahead of a by its jitter and so is never left again.
    fn penalty() -> Vec<(i64, f64)> {
        let (a, b) = (a(), b());
        let mut merged: Vec<_> = a[..8].to_vec();
        merged.extend(&b[9..]);
        merged
    }

    /// The union, there being no shared timestamp.
    fn chain() -> Vec<(i64, f64)> {
        let mut merged = a();
        merged.extend(b());
        merged.sort_by_key(|&(t, _)| t);
        merged
    }

    fn labelled(replica: &str, with_replica: bool) -> Vec<(&str, &str)> {
        let mut labels = vec![("__name__", "x"), ("job", "api")];
        if with_replica {
            labels.push(("replica", replica));
        }
        labels
    }

    /// A store holding both replicas; `strips` is what its Info says about
    /// `without_replica_labels`, and a store that does not strip sends the
    /// label as it is.
    fn store(strips: bool) -> Arc<FakeStore> {
        let mut info = info("sidecar", &[], T0, T0 + 20 * STEP + JITTER);
        info.store.as_mut().unwrap().supports_without_replica_labels = strips;
        let a = a();
        let split = a.iter().position(|&(t, _)| t > T0 + 8 * STEP).unwrap();
        Arc::new(FakeStore::new(info).with_frames(vec![
            series_frame(store_series(
                &labelled("a", !strips),
                vec![raw_chunk(&a[..split]), raw_chunk(&a[split..])],
            )),
            series_frame(store_series(&labelled("b", !strips), vec![raw_chunk(&b())])),
        ]))
    }

    fn dedup(func: DeduplicationFunc) -> SelectOptions {
        SelectOptions {
            dedup: Some(Dedup {
                replica_labels: vec!["replica".into()],
                func,
            }),
            ..Default::default()
        }
    }

    fn memory(samples: &[(i64, f64)]) -> MemorySeriesSource {
        MemorySeriesSource::try_new(vec![memory_series(&labelled("", false), samples)]).unwrap()
    }

    const QUERIES: [&str; 4] = [
        "x",
        "rate(x[1m])",
        "sum_over_time(x[1m])",
        "count_over_time(x[400s])",
    ];

    /// Whichever way the replicas reach the proxy and however the range
    /// is cut, the engine sees the merged series and nothing else.
    async fn answers_as(fake: Arc<FakeStore>, options: SelectOptions, merged: &[(i64, f64)]) {
        let engine = Engine::new();
        let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP);
        let want = memory(merged);
        for block_ms in [i64::MAX, 60_000, 40_000] {
            let thanos = source_over_shared(vec![Arc::clone(&fake)], options.clone())
                .await
                .blocks(block_ms);
            for query in QUERIES {
                let got = engine
                    .range_query_async(&thanos, query, &range)
                    .await
                    .unwrap_or_else(|e| panic!("{query} in {block_ms}ms blocks: {e}"));
                let expected = engine
                    .range_query_async(&want, query, &range)
                    .await
                    .unwrap();
                let got = flatten(&coalesced(&got));
                assert!(
                    got.iter().all(|(labels, _)| labels
                        == &[
                            ("__name__".to_string(), "x".to_string()),
                            ("job".to_string(), "api".to_string())
                        ]
                        || query != "x"),
                    "{query}: {got:?}"
                );
                assert_eq!(
                    got,
                    flatten(&coalesced(&expected)),
                    "{query} in {block_ms}ms blocks"
                );
                assert!(!got.is_empty(), "{query}");
            }
            assert!(thanos.take_warnings().is_empty());
        }
    }

    #[tokio::test]
    async fn a_store_that_strips_is_asked_to_and_the_penalty_merge_follows() {
        let fake = store(true);
        answers_as(
            Arc::clone(&fake),
            dedup(DeduplicationFunc::Penalty),
            &penalty(),
        )
        .await;
        for request in fake.series_requests() {
            assert_eq!(request.without_replica_labels, ["replica"]);
        }
    }

    #[tokio::test]
    async fn the_proxy_strips_for_a_store_that_does_not() {
        let fake = store(false);
        answers_as(
            Arc::clone(&fake),
            dedup(DeduplicationFunc::Penalty),
            &penalty(),
        )
        .await;
        for request in fake.series_requests() {
            assert_eq!(request.without_replica_labels, ["replica"]);
        }
    }

    #[tokio::test]
    async fn the_chain_merge_unions_the_replicas() {
        answers_as(store(true), dedup(DeduplicationFunc::Chain), &chain()).await;
    }

    /// `dedup=false`: the replicas stay series of their own with the
    /// labels that tell them apart, and the stores are not asked to drop
    /// any. The same goes for dedup without a label to dedup along.
    #[tokio::test]
    async fn without_dedup_the_replicas_stay_apart() {
        let engine = Engine::new();
        let range = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP);
        let no_labels = SelectOptions {
            dedup: Some(Dedup {
                replica_labels: Vec::new(),
                func: DeduplicationFunc::Penalty,
            }),
            ..Default::default()
        };
        for options in [SelectOptions::default(), no_labels] {
            let fake = store(false);
            let thanos = source_over_shared(vec![Arc::clone(&fake)], options)
                .await
                .blocks(60_000);
            let result = engine
                .range_query_async(&thanos, "x", &range)
                .await
                .unwrap();
            let result = coalesced(&result);
            let replicas: Vec<_> = result.iter().map(|s| s.label("replica")).collect();
            assert_eq!(replicas, ["a", "b"]);
            for request in fake.series_requests() {
                assert!(request.without_replica_labels.is_empty());
            }
            assert!(result
                .iter()
                .all(|s| s.labels().all(|(name, _)| name != "__replica_slot__")));
        }
    }

    /// A stale marker one replica ends its series with is not a value to
    /// skip over: the engine sees it and the series ends there, though the
    /// other replica has a sample before it.
    #[tokio::test]
    async fn a_stale_marker_survives_the_merge() {
        let stale = f64::from_bits(STALE_NAN);
        let mut a: Vec<_> = counter(10, 10.0);
        a.push((T0 + 10 * STEP, stale));
        let b: Vec<_> = counter(10, 10.0)
            .into_iter()
            .map(|(t, v)| (t + JITTER, v))
            .collect();
        let mut info = info("sidecar", &[], T0, T0 + 20 * STEP);
        info.store.as_mut().unwrap().supports_without_replica_labels = true;
        let fake = Arc::new(FakeStore::new(info).with_frames(vec![
            series_frame(store_series(&labelled("", false), vec![raw_chunk(&a)])),
            series_frame(store_series(&labelled("", false), vec![raw_chunk(&b)])),
        ]));
        let engine = Engine::new();
        for block_ms in [i64::MAX, 15_000] {
            let thanos =
                source_over_shared(vec![Arc::clone(&fake)], dedup(DeduplicationFunc::Penalty))
                    .await
                    .blocks(block_ms);
            // Just before the marker the merge follows a, whose last
            // sample then is 90, not b's at +3s.
            let before = engine
                .range_query_async(
                    &thanos,
                    "x",
                    &RangeQuery::new(T0 + 10 * STEP - 1, T0 + 10 * STEP - 1, 1),
                )
                .await
                .unwrap();
            assert_eq!(decode(&before)[0].values(), [90.0], "{block_ms}ms");
            let after = engine
                .range_query_async(
                    &thanos,
                    "x",
                    &RangeQuery::new(T0 + 10 * STEP + 1_000, T0 + 10 * STEP + 1_000, 1),
                )
                .await
                .unwrap();
            assert!(decode(&after).is_empty(), "{block_ms}ms: {after:?}");
        }
    }

    /// The reset is the first sample of the second block: its window
    /// reaches back into the first, and the merge state and the counter
    /// lift have to be the ones an unbroken run would have there.
    #[tokio::test]
    async fn a_counter_reset_at_a_block_edge_rates_as_in_one_block() {
        let edge = (T0 / 120_000 + 1) * 120_000;
        let grid = |shift: i64| -> Vec<(i64, f64)> {
            (-6..=6)
                .map(|i| {
                    let value = if i < 0 {
                        100.0 + i as f64 * 10.0
                    } else {
                        i as f64
                    };
                    (edge + i * STEP + shift, value)
                })
                .collect()
        };
        let mut info = info("sidecar", &[], edge - 6 * STEP, edge + 6 * STEP + JITTER);
        info.store.as_mut().unwrap().supports_without_replica_labels = true;
        let fake = Arc::new(FakeStore::new(info).with_frames(vec![
            series_frame(store_series(
                &labelled("", false),
                vec![raw_chunk(&grid(0))],
            )),
            series_frame(store_series(
                &labelled("", false),
                vec![raw_chunk(&grid(JITTER))],
            )),
        ]));
        let engine = Engine::new();
        let range = RangeQuery::new(edge - 60_000, edge + 60_000, STEP);
        let whole = source_over_shared(vec![Arc::clone(&fake)], dedup(DeduplicationFunc::Penalty))
            .await
            .blocks(i64::MAX);
        let cut = source_over_shared(vec![Arc::clone(&fake)], dedup(DeduplicationFunc::Penalty))
            .await
            .blocks(120_000);
        let reference = memory(&grid(0));
        let rows = engine.range_query_async(&cut, "x", &range).await.unwrap();
        assert!(decode(&rows).len() > 1, "two blocks, two rows");
        for query in ["rate(x[1m])", "increase(x[1m])", "resets(x[1m])", "x"] {
            let one = engine
                .range_query_async(&whole, query, &range)
                .await
                .unwrap();
            let two = engine.range_query_async(&cut, query, &range).await.unwrap();
            let want = engine
                .range_query_async(&reference, query, &range)
                .await
                .unwrap();
            assert_eq!(
                flatten(&coalesced(&two)),
                flatten(&coalesced(&one)),
                "{query}"
            );
            assert_eq!(
                flatten(&coalesced(&two)),
                flatten(&coalesced(&want)),
                "{query} against the followed replica alone"
            );
        }
    }

    /// The Dedup node sits between the scan and `SeriesSetExec` when the
    /// source deduplicates, and nowhere otherwise.
    #[tokio::test]
    async fn the_plan_shows_the_dedup_node_only_when_deduplicating() {
        let engine = Engine::new();
        let q = RangeQuery::new(T0 + 4 * STEP, T0 + 20 * STEP, STEP);
        for (options, on) in [
            (dedup(DeduplicationFunc::Penalty), true),
            (Default::default(), false),
        ] {
            let thanos = source_over_shared(vec![store(true)], options).await;
            let plan = engine.plan_async(&thanos, "rate(x[1m])", &q).await.unwrap();
            let logical = explain_plan(&plan);
            let exec = engine
                .physical_plan_async(&thanos, "rate(x[1m])", &q)
                .await
                .unwrap();
            let physical = displayable(exec.as_ref()).indent(true).to_string();
            assert_eq!(logical.contains("Dedup: "), on, "{logical}");
            assert_eq!(physical.contains("DedupExec"), on, "{physical}");
            assert!(!physical.contains("SortExec"), "{physical}");
            if on {
                assert!(
                    logical.contains("Dedup: replica_labels=[replica], func=penalty, counter=true"),
                    "{logical}"
                );
                let lines: Vec<&str> = logical.lines().map(str::trim_start).collect();
                let at = lines.iter().position(|l| l.starts_with("Dedup: ")).unwrap();
                assert!(
                    lines[at + 1].contains("Scan") || lines[at + 1].contains("TableScan"),
                    "{logical}"
                );
                let lines: Vec<&str> = physical.lines().map(str::trim_start).collect();
                let at = lines
                    .iter()
                    .position(|l| l.starts_with("SeriesSetExec"))
                    .unwrap();
                assert!(lines[at + 1].starts_with("DedupExec"), "{physical}");
            }
        }
    }

    /// Real samples of a series a compacted block and two replicas all
    /// hold, with the compacted block's chunk and one replica's chunk of
    /// the same bounds; the 6h range around them is Go's, merged by
    /// `dedup.NewSeriesSet` over the chunks `NewOverlapSplit` split, which
    /// the expected samples are the output of. Of the two equal-bounds
    /// chunks the store sends the replica's first, and Go's proxy puts the
    /// one with the larger data (more samples) first, so the merge starts
    /// on the compacted chunk and ends with both replicas' staleness
    /// markers; in the other order it follows one replica and the series
    /// is still alive after them.
    #[tokio::test]
    async fn chunks_of_equal_bounds_split_into_replicas_as_go_orders_them() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("testdata/equal_bounds_replicas.json")).unwrap();
        let samples = |v: &serde_json::Value| -> Vec<(i64, f64)> {
            v.as_array()
                .unwrap()
                .iter()
                .map(|p| {
                    let value = match p[2].as_i64().unwrap() {
                        2 => f64::from_bits(STALE_NAN),
                        _ => p[1].as_f64().unwrap(),
                    };
                    (p[0].as_i64().unwrap(), value)
                })
                .collect()
        };
        let chunks: Vec<_> = fixture["chunks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| raw_chunk(&samples(&c["samples"])))
            .collect();
        let (start, end) = (
            fixture["start_s"].as_i64().unwrap() * 1000,
            fixture["end_s"].as_i64().unwrap() * 1000,
        );
        let mut info = info("sidecar", &[], start - 3_600_000, end);
        info.store.as_mut().unwrap().supports_without_replica_labels = true;
        let fake = Arc::new(
            FakeStore::new(info).with_frames(vec![series_frame(store_series(
                &labelled("", false),
                chunks,
            ))]),
        );
        let want = memory(&samples(&fixture["merged"]));
        let range = RangeQuery::new(start, end, 60_000);
        let engine = Engine::new();
        for block_ms in [i64::MAX, 7_200_000] {
            let thanos =
                source_over_shared(vec![Arc::clone(&fake)], dedup(DeduplicationFunc::Penalty))
                    .await
                    .blocks(block_ms);
            for query in ["x", "count_over_time(x[5m])"] {
                let got = engine
                    .range_query_async(&thanos, query, &range)
                    .await
                    .unwrap();
                let expected = engine
                    .range_query_async(&want, query, &range)
                    .await
                    .unwrap();
                assert_eq!(
                    flatten(&coalesced(&got)),
                    flatten(&coalesced(&expected)),
                    "{query} in {block_ms}ms blocks"
                );
            }
        }
    }
}
