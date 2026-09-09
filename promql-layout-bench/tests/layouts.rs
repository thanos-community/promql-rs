//! Both layouts must answer `sum by (code) (rate(apiserver_request_total[5m]))` identically, or
//! the comparison in `docs/series-representation.md` is meaningless. The answer they are held to
//! is computed without Arrow or DataFusion: the kernel run straight over the generator's series.
//!
//! Two things cannot be done at all: `RunEndEncoded<Struct>` cannot be read from, and the
//! row-per-series layout cannot drive a native window frame. Those are findings rather than bugs
//! in this crate, so they are pinned by characterisation tests:
//! if DataFusion's behaviour changes, these fail and the design note needs revisiting.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, RecordBatch, RunArray};
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{col, lit};
use promql_layout_bench::list::List;
use promql_layout_bench::struct_ree::{ree_type, StructRee};
use promql_layout_bench::support::chunking::{build_single, Chunking};
use promql_layout_bench::support::dispatch::{rate_frame, LayoutKind};
use promql_layout_bench::support::format::{
    dict_type, flat_schema, label_index, COL_LABELS, COL_TIMESTAMP, COL_TIMESTAMPS, COL_VALUE,
    COL_VALUES, GROUP_LABEL, UNIQUE_LABEL,
};
use promql_layout_bench::support::frame::window_frame;
use promql_layout_bench::support::promql::{plan, read_groups, scan_table, session, Plan};
use promql_layout_bench::support::query::Query;
use promql_layout_bench::support::ratefn::{extrapolated_rate, rate_steps};
use promql_layout_bench::support::scan::{
    flat_samples, labels_for, labels_per_series, timestamps_for, values_for, Spec,
};

const TARGET: usize = 4;
const K: usize = 3;

/// How each layout is handed over in tests: several partitions, and batches small enough that
/// the row-per-sample layout's series cross batch boundaries.
fn chunking(layout: LayoutKind) -> Chunking {
    match layout {
        LayoutKind::List => Chunking::new(TARGET, 16),
        LayoutKind::StructRee => Chunking::new(TARGET, 64),
    }
}

/// A `rate` over the whole scan, at the last sample.
fn instant(spec: &Spec) -> Query {
    Query::instant(spec.samples as i64 * 30_000, true).at(spec.at())
}

/// A 5m `rate` over four steps 15s apart, ending at the last sample.
fn range(spec: &Spec) -> Query {
    Query::range(300_000, 4, 15_000, true).at(spec.at())
}

async fn run(
    layout: LayoutKind,
    spec: &Spec,
    query: Query,
    kind: Plan,
    chunking: Chunking,
) -> Result<Vec<(String, f64)>> {
    let ctx = session(TARGET);
    let built = layout.build(spec, chunking);
    let rate = rate_frame(&ctx, scan_table(&built)?, layout, query)?;
    Ok(read_groups(&plan(rate, kind)?.collect().await?))
}

/// The answer, computed by hand: the kernel over the generator, no Arrow in sight.
fn reference(spec: &Spec, query: Query, kind: Plan) -> Vec<(String, f64)> {
    let code_idx = label_index(GROUP_LABEL);
    let instance_idx = label_index(UNIQUE_LABEL);
    let ts = timestamps_for(spec);
    let grid = query.grid();

    match kind {
        Plan::InstantSum => {
            let mut sums: BTreeMap<String, f64> = BTreeMap::new();
            for i in 0..spec.series {
                let vs = values_for(i, spec);
                if let Some(r) =
                    extrapolated_rate(&ts, &vs, query.at, query.range_ms, query.per_second)
                {
                    *sums.entry(labels_for(i)[code_idx].clone()).or_default() += r;
                }
            }
            sums.into_iter().collect()
        }
        Plan::RangeSum => {
            let mut sums: BTreeMap<(String, i64), f64> = BTreeMap::new();
            for i in 0..spec.series {
                let vs = values_for(i, spec);
                let code = labels_for(i)[code_idx].clone();
                for (s, v) in rate_steps(&ts, &vs, grid, query.per_second)
                    .into_iter()
                    .enumerate()
                {
                    if let Some(v) = v {
                        *sums.entry((code.clone(), grid.at(s))).or_default() += v;
                    }
                }
            }
            sums.into_iter()
                .map(|((code, step), v)| (format!("{code}/{step}"), v))
                .collect()
        }
        Plan::RangeTopk { k } => {
            let mut per_step: BTreeMap<i64, Vec<(String, f64)>> = BTreeMap::new();
            for i in 0..spec.series {
                let vs = values_for(i, spec);
                let instance = labels_for(i)[instance_idx].clone();
                for (s, v) in rate_steps(&ts, &vs, grid, query.per_second)
                    .into_iter()
                    .enumerate()
                {
                    if let Some(v) = v {
                        per_step
                            .entry(grid.at(s))
                            .or_default()
                            .push((instance.clone(), v));
                    }
                }
            }
            let mut out = Vec::new();
            for (step, mut rows) in per_step {
                rows.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                for (instance, v) in rows.into_iter().take(k) {
                    out.push((format!("{instance}/{step}"), v));
                }
            }
            out
        }
    }
}

fn assert_close(got: &[(String, f64)], want: &[(String, f64)], what: &str) {
    assert_eq!(
        got.len(),
        want.len(),
        "{what}: {} rows, wanted {}",
        got.len(),
        want.len()
    );
    for ((kg, vg), (kw, vw)) in got.iter().zip(want) {
        assert_eq!(kg, kw, "{what}: group keys differ");
        assert!(
            (vg - vw).abs() <= 1e-9 * vw.abs().max(1.0),
            "{what}: {kg} is {vg}, wanted {vw}"
        );
    }
}

/// Both layouts, against the reference.
async fn assert_both_layouts_agree(spec: &Spec, query: Query, kind: Plan) -> Result<()> {
    let want = reference(spec, query, kind);
    assert!(!want.is_empty(), "the reference should have rows");

    for layout in LayoutKind::ALL {
        let got = run(layout, spec, query, kind, chunking(layout)).await?;
        assert_close(&got, &want, &format!("{}, {kind:?}", layout.name()));
    }
    Ok(())
}

#[tokio::test]
async fn instant_query_agrees_with_the_reference_on_both_layouts() -> Result<()> {
    let spec = Spec::new(708, 10);
    assert_both_layouts_agree(&spec, instant(&spec), Plan::InstantSum).await?;
    // The scan layer invents 7 distinct codes.
    assert_eq!(reference(&spec, instant(&spec), Plan::InstantSum).len(), 7);
    Ok(())
}

#[tokio::test]
async fn range_sum_agrees_with_the_reference_on_both_layouts() -> Result<()> {
    let spec = Spec::new(708, 12);
    assert_both_layouts_agree(&spec, range(&spec), Plan::RangeSum).await?;
    // One row per (code, step): this is the cardinality the unnest exists to produce.
    assert_eq!(reference(&spec, range(&spec), Plan::RangeSum).len(), 7 * 4);
    Ok(())
}

#[tokio::test]
async fn topk_agrees_with_the_reference_on_both_layouts() -> Result<()> {
    let spec = Spec::new(64, 12);
    let kind = Plan::RangeTopk { k: K };
    assert_both_layouts_agree(&spec, range(&spec), kind).await?;
    assert_eq!(reference(&spec, range(&spec), kind).len(), K * 4);
    Ok(())
}

/// Rows-as-samples arrive in fixed-size batches, so a long series spans several of them and the
/// operator has to carry it across. Batches far smaller than a series, both query shapes.
#[tokio::test]
async fn a_series_spanning_many_batches_is_reassembled() -> Result<()> {
    let spec = Spec::new(16, 100);
    let tiny = Chunking::new(3, 7);
    for (query, kind) in [
        (instant(&spec), Plan::InstantSum),
        (range(&spec), Plan::RangeSum),
    ] {
        let want = reference(&spec, query, kind);
        let got = run(LayoutKind::StructRee, &spec, query, kind, tiny).await?;
        assert_close(&got, &want, &format!("struct_ree across batches, {kind:?}"));
    }
    Ok(())
}

/// The operator depends on series staying contiguous within a partition. With fewer partitions
/// than `target_partitions` the planner would like to round-robin batches to fill the cores; the
/// operator says no, and this checks it was heard.
#[tokio::test]
async fn fewer_partitions_than_target_are_not_redistributed_under_the_operator() -> Result<()> {
    let spec = Spec::new(64, 40);
    let query = instant(&spec);
    let want = reference(&spec, query, Plan::InstantSum);
    for layout in LayoutKind::ALL {
        let two = Chunking::new(2, 8);
        let got = run(layout, &spec, query, Plan::InstantSum, two).await?;
        assert_close(&got, &want, &format!("{} with 2 partitions", layout.name()));
    }
    Ok(())
}

/// Not a candidate: one row per sample with the run-end encoding *around* the struct, so each
/// labelset is stored once and `run_ends` is literally the series boundary list. Built here, not
/// in `src/`, because nothing in the benchmark can use it.
fn outer_ree_table(spec: &Spec) -> Result<Arc<dyn TableProvider>> {
    let series: Vec<usize> = (0..spec.series).collect();
    let values = labels_per_series(&series);
    let run_ends = Int32Array::from(
        (1..=series.len())
            .map(|s| (s * spec.samples) as i32)
            .collect::<Vec<_>>(),
    );
    let labels = RunArray::<Int32Type>::try_new(&run_ends, &values)?;
    let (ts, vs) = flat_samples(spec, &series);
    let schema = flat_schema(labels.data_type().clone());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(labels), Arc::new(ts), Arc::new(vs)],
    )?;
    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

/// `RunEndEncoded<Int32, Struct<..>>` stores each labelset exactly once and hands you series
/// boundaries in its run-ends buffer, which makes it the tidiest encoding on paper. DataFusion 55
/// cannot read a field out of it, so `labels.code` fails and it cannot serve `sum by (code)`.
///
/// If this starts failing, DataFusion gained support and the encoding is worth re-measuring.
#[tokio::test]
async fn run_end_encoded_struct_is_not_supported_by_datafusion_55() -> Result<()> {
    let spec = Spec::new(8, 4);
    let ctx = session(1);
    let raw = ctx.read_table(outer_ree_table(&spec)?)?;
    let outcome = match raw.aggregate(
        vec![col(COL_LABELS).field(GROUP_LABEL).alias(GROUP_LABEL)],
        vec![count(lit(1))],
    ) {
        Ok(df) => df.collect().await,
        Err(e) => Err(e),
    };
    let err = outcome.expect_err("expected field access on RunEndEncoded<Struct> to fail");
    assert!(
        err.to_string().contains("is not Struct, Map, or Null"),
        "unexpected failure mode: {err}"
    );
    Ok(())
}

/// A native `RANGE BETWEEN ... PRECEDING` window frame works when rows are samples: there is a
/// scalar timestamp column to order by, and the row indices the evaluator receives line up with
/// samples. Note what it yields: one row per *sample*, not per step.
#[tokio::test]
async fn sample_per_row_layout_can_drive_a_native_window_frame() -> Result<()> {
    let spec = Spec::new(8, 20);
    let ctx = session(1);
    let built = LayoutKind::StructRee.build(&spec, Chunking::SINGLE);
    let raw = ctx.read_table(scan_table(&built)?)?;
    let batches = window_frame(raw, COL_VALUE, COL_TIMESTAMP)?
        .collect()
        .await?;
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        rows,
        spec.total(),
        "a window function emits one row per input row"
    );
    Ok(())
}

/// The row-per-series layout cannot drive one, and DataFusion does not say so. A `RANGE` frame
/// ordered by this layout's `List<Timestamp>` column is planned and executed. The frame handed to
/// the evaluator is a range of *rows*, and a row here is a whole series, so the evaluator receives
/// `List<Float64>` per row and the finest boundary expressible is "series 3 to 5". The fake window
/// function reports that instead of pretending to compute something.
#[tokio::test]
async fn row_per_series_cannot_drive_a_native_window_frame() -> Result<()> {
    let spec = Spec::new(8, 20);
    let ctx = session(1);
    let built = LayoutKind::List.build(&spec, Chunking::SINGLE);
    let raw = ctx.read_table(scan_table(&built)?)?;
    let outcome = match window_frame(raw, COL_VALUES, COL_TIMESTAMPS) {
        Ok(df) => df.collect().await,
        Err(e) => Err(e),
    };
    let err = outcome.expect_err("a frame over series rows should not yield a per-sample result");
    assert!(
        err.to_string().contains("whole series"),
        "unexpected failure mode: {err}"
    );
    Ok(())
}

#[test]
fn row_counts_match_the_layout_claim() {
    let spec = Spec::new(708, 10);
    assert_eq!(build_single::<StructRee>(&spec).num_rows(), spec.total());
    assert_eq!(build_single::<List>(&spec).num_rows(), spec.series);
}

/// What Polar Signals and Dash0 store, and what the doc specifies: run-end encoding inside the
/// struct, a dictionary at the leaf, and nothing nullable anywhere.
#[test]
fn struct_ree_labels_are_run_end_encoded_dictionaries_with_nothing_nullable() {
    let batch = build_single::<StructRee>(&Spec::new(8, 4));
    let DataType::Struct(fields) = batch.schema().field(0).data_type().clone() else {
        panic!("labels should be a struct");
    };
    assert_eq!(fields.len(), 18);
    for f in fields.iter() {
        assert!(!f.is_nullable(), "{} should not be nullable", f.name());
        assert_eq!(
            f.data_type(),
            &ree_type(),
            "{} should be REE<Int32, Dict>",
            f.name()
        );
        let DataType::RunEndEncoded(run_ends, values) = f.data_type() else {
            unreachable!()
        };
        assert!(!run_ends.is_nullable());
        assert!(
            !values.is_nullable(),
            "the values child must be non-nullable"
        );
        assert_eq!(values.data_type(), &dict_type());
    }
}

#[test]
fn list_labels_are_dictionary_encoded_at_the_leaf() {
    let batch = build_single::<List>(&Spec::new(8, 4));
    let DataType::Struct(fields) = batch.schema().field(0).data_type().clone() else {
        panic!("labels should be a struct");
    };
    for f in fields.iter() {
        assert!(!f.is_nullable());
        assert_eq!(f.data_type(), &dict_type());
    }
}

/// The constraint that drives the whole design: the labelset is constant across a series' samples,
/// so neither candidate may pay for it per sample. Widening the window must not move the labels
/// column at all.
#[test]
fn labels_do_not_grow_with_the_window() {
    let short = Spec::new(64, 10);
    let long = Spec::new(64, 2_880);
    for layout in LayoutKind::ALL {
        let a = layout.build(&short, Chunking::SINGLE).labels_bytes;
        let b = layout.build(&long, Chunking::SINGLE).labels_bytes;
        assert_eq!(
            a,
            b,
            "{} labels should not grow with the window: {a} then {b}",
            layout.name()
        );
    }
}
