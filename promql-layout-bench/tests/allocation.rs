//! What a layout costs while being queried, which is not what it costs sitting in memory.
//!
//! The constraint behind both layouts is that the labelset is constant across a series' samples
//! and must not be paid for per sample. That has to hold at query time too: the operator's labels
//! work is per series, so widening the window must not move its peak the way an expansion of the
//! labels to one value per sample would.
//!
//! Measuring this needs an allocator that counts, because resident set size hides it: freed pages
//! get reused and RSS stays flat even if a query materialises a gigabyte.

// Each test holds `ONE_AT_A_TIME` across its awaits on purpose; see the comment there.
#![allow(clippy::await_holding_lock)]

use std::sync::Mutex;

use datafusion::error::Result;
use promql_layout_bench::support::alloc::Counting;
use promql_layout_bench::support::chunking::Chunking;
use promql_layout_bench::support::dispatch::{rate_frame, LayoutKind};
use promql_layout_bench::support::promql::{plan, read_groups, scan_table, session, Plan};
use promql_layout_bench::support::query::Query;
use promql_layout_bench::support::scan::Spec;

#[global_allocator]
static ALLOC: Counting = Counting;

/// The allocator counts every thread, and the test harness runs tests on several. One test's
/// `build` landing inside another's measured window would be counted, so measurements take turns.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// Peak bytes allocated above the starting level while running the instant query.
async fn peak_alloc_of(layout: LayoutKind, spec: &Spec) -> Result<usize> {
    let _turn = ONE_AT_A_TIME.lock().unwrap();

    let query = Query::instant(spec.samples as i64 * 30_000, true).at(spec.at());
    let ctx = session(1);
    let built = layout.build(spec, Chunking::SINGLE);
    let full = plan(
        rate_frame(&ctx, scan_table(&built)?, layout, query)?,
        Plan::InstantSum,
    )?;

    let before = Counting::in_use();
    Counting::reset_peak();
    let batches = full.collect().await?;
    let peak = Counting::peak_above(before);

    // Keep the results alive until after the measurement.
    assert!(!read_groups(&batches).is_empty());
    Ok(peak)
}

/// Ten times the samples, but the labels work is identical, so the peak must not scale with the
/// window the way an expansion would.
async fn assert_labels_work_is_flat_in_the_window(layout: LayoutKind) -> Result<()> {
    let small = peak_alloc_of(layout, &Spec::new(256, 200)).await?;
    let large = peak_alloc_of(layout, &Spec::new(256, 2_000)).await?;
    eprintln!(
        "{}: peak {small} bytes at 200 samples, {large} at 2,000",
        layout.name()
    );
    assert!(
        large < small * 4,
        "{}: labels work should not scale with samples: {small} then {large}",
        layout.name()
    );
    Ok(())
}

/// The row-per-series layout has nothing to expand: there is one labels row per series and the
/// group key is read straight off it.
#[tokio::test]
async fn row_per_series_does_not_expand_its_labels() -> Result<()> {
    assert_labels_work_is_flat_in_the_window(LayoutKind::List).await
}

/// The series-aware operator reads the run ends instead of expanding them, so rows-as-samples
/// stays flat too. The one thing that does grow with the window is the carry, which copies the
/// samples of a series that crosses a batch, not its labels.
#[tokio::test]
async fn row_per_sample_operator_does_not_expand_its_labels() -> Result<()> {
    assert_labels_work_is_flat_in_the_window(LayoutKind::StructRee).await
}
