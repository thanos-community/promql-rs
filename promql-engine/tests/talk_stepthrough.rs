//! The numbers slide 6 of the PromCon 2026 lightning talk shows, computed
//! by the engine and exported as JSON for the deck in
//! `docs/talks/promcon-2026-lightning/deck` by its `scripts/gen-data.sh`.
//!
//! The slide steps through `sum(rate(x[5m]))` over the fixture of
//! `docs/engine-blocks.html`, whose page computes its rates with its own
//! JavaScript port of `extrapolatedRate`. The deck takes its numbers from
//! here instead, and the asserts below hold the engine to the page's, so
//! the page, `docs/engine-blocks.md` and the slide cannot drift apart
//! without this export failing. Nothing runs unless
//! `PROMQL_TALK_STEPTHROUGH_OUT` names a path, as in `talk_plans.rs`.

use std::num::{NonZeroU64, NonZeroUsize};

use promql_engine::{Engine, MemorySeriesSource, RangeQuery, Series};
use serde_json::json;

const QUERY: &str = "sum(rate(x[5m]))";

/// The page's two series at 30 s from the epoch: `a` from 300 s with its
/// counter reset at 510 s, `b` from 150 s counting up by one.
const LOAD: [&str; 2] = [
    r#"x{pod="a"} _x10 10 11 12 13 14 15 16 3 4 5 6"#,
    r#"x{pod="b"} _x5 10+1x15"#,
];

/// 240 s blocks put the edge at 480 s, between the page's two blocks, and
/// three rows per batch cut the store's four rows into the page's batch C
/// (1·a, 1·b, 2·a) and batch D (2·b).
///
/// The store starts its first block at the first window end, 420 s, where
/// the page's store with fixed ranges says 240 s. Both answer 420 s and
/// 450 s from the same samples, so the numbers agree and the slide keeps
/// the page's 240.
const BLOCK_MS: u64 = 240_000;
const ROWS_PER_BATCH: usize = 3;

const S: i64 = 1000;
const STEPS: [i64; 7] = [420, 450, 480, 510, 540, 570, 600];
/// The steps each block answers, in the order the engine emits them.
const BLOCKS: [&[i64]; 2] = [&[420, 450], &[480, 510, 540, 570, 600]];

/// `docs/engine-blocks.md` §5: each increase is `result × factor`, and the
/// rate is the increase over the 300 s range.
fn want_a() -> [f64; 7] {
    [
        4.0 * 135.0 / 120.0,
        5.0 * 165.0 / 150.0,
        6.0 * 195.0 / 180.0,
        9.0 * 225.0 / 210.0,
        10.0 * 255.0 / 240.0,
        11.0 * 300.0 / 270.0,
        11.0 * 300.0 / 270.0,
    ]
    .map(|increase| increase / 300.0)
}

/// `docs/engine-blocks.md` §4: ten samples a window, nine increments,
/// extrapolated by 10/9 at every step.
fn want_b() -> [f64; 7] {
    [9.0 * 10.0 / 9.0 / 300.0; 7]
}

#[test]
fn export_talk_stepthrough() {
    let Some(out) = std::env::var_os("PROMQL_TALK_STEPTHROUGH_OUT") else {
        return;
    };

    let series: Vec<_> = LOAD
        .iter()
        .map(|l| promql_parser::parse_series_desc(l).expect("series line parses"))
        .collect();
    let source = MemorySeriesSource::from_descriptions(&series, 30.0)
        .blocks(NonZeroU64::new(BLOCK_MS).expect("a non-zero block width"))
        .rows_per_batch(NonZeroUsize::new(ROWS_PER_BATCH).expect("a non-zero batch size"));
    let range = RangeQuery::new(STEPS[0] * S, STEPS[6] * S, 30 * S);
    let engine = Engine::blocking().expect("an engine with its own runtime");
    let run = |query: &str| -> Vec<Series> {
        let batches = engine
            .range_query(&source, query, &range)
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        promql_engine::series::decode(&batches).expect("the canonical shape decodes")
    };

    // One output row per series and block: the slide sends each one up to
    // `sum` as its own Arrow row, so a block cut anywhere else would make
    // the slide show rows the engine never emits.
    let rates = run("rate(x[5m])");
    let rate_of = |pod: &str| -> Vec<(i64, f64)> {
        let rows: Vec<&Series> = rates.iter().filter(|s| s.label("pod") == pod).collect();
        let steps: Vec<Vec<i64>> = rows
            .iter()
            .map(|s| s.timestamps().iter().map(|t| t / S).collect())
            .collect();
        assert_eq!(steps, BLOCKS, "rate rows of x{{pod={pod:?}}}");
        rows.iter()
            .flat_map(|s| s.timestamps().iter().zip(s.values()))
            .map(|(t, v)| (t / S, *v))
            .collect()
    };
    let sum = run(QUERY);
    let sum_steps: Vec<Vec<i64>> = sum
        .iter()
        .map(|s| s.timestamps().iter().map(|t| t / S).collect())
        .collect();
    assert_eq!(sum_steps, BLOCKS, "{QUERY} rows");
    let sum: Vec<(i64, f64)> = sum
        .iter()
        .flat_map(|s| s.timestamps().iter().zip(s.values()))
        .map(|(t, v)| (t / S, *v))
        .collect();

    let (a, b) = (rate_of("a"), rate_of("b"));
    let want_sum: Vec<f64> = want_a().iter().zip(want_b()).map(|(a, b)| a + b).collect();
    for (name, got, want) in [
        ("rate a", &a, want_a().to_vec()),
        ("rate b", &b, want_b().to_vec()),
        ("sum", &sum, want_sum),
    ] {
        assert_eq!(got.len(), STEPS.len(), "{name}: {got:?}");
        for ((t, v), (step, w)) in got.iter().zip(STEPS.iter().zip(want)) {
            assert_eq!(t, step, "{name}");
            assert!((v - w).abs() < 1e-10, "{name} at {t}s: {v} vs {w}");
        }
    }

    let body = serde_json::to_string_pretty(&json!({
        "query": QUERY,
        "rate": { "a": a, "b": b },
        "sum": sum,
    }))
    .expect("numbers serialize");
    std::fs::write(&out, body + "\n")
        .unwrap_or_else(|e| panic!("writing {}: {e}", out.to_string_lossy()));
}
