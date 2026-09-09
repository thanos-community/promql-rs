//! Measures the candidate layouts against `sum by (code) (rate(apiserver_request_total[5m]))` and
//! its range-query relatives, with real Arrow batches, real DataFusion plans and the real `rate`.
//!
//! Two candidates per query, both running the same series-aware `RangeVectorExec`: fed rows as
//! samples by `struct_ree`, fed rows as series by `list`. Above the range vector every candidate
//! runs the identical plan, so the only thing that varies is how the series were found.
//!
//!   cargo run -p promql-layout-bench --release
//!   cargo run -p promql-layout-bench --release -- --write-results   # also writes RESULTS.md

use std::fs;
use std::time::{Duration, Instant};

use datafusion::error::Result;
use datafusion::prelude::DataFrame;

use super::alloc::Counting;
use super::chunking::Chunking;
use super::dispatch::{rate_frame, LayoutKind};
use super::promql::{plan, read_groups, scan_table, session, unnested, Plan};
use super::query::Query;
use super::scan::{Spec, SCRAPE_MS};

const RUNS: usize = 5;
/// Samples per batch, for both layouts: DataFusion's default batch size and a Parquet reader's.
/// Each layout turns it into rows its own way; see `Layout::rows_per_batch`.
const SAMPLES_PER_BATCH: usize = 8192;
const TOPK: usize = 3;

const MINUTE: i64 = 60_000;
const HOUR: i64 = 60 * MINUTE;
const DAY: i64 = 24 * HOUR;

struct Shape {
    name: &'static str,
    series: usize,
    query: Query,
}

/// Instant shapes from the cluster's rule set, range shapes from its dashboards.
fn shapes() -> Vec<Shape> {
    vec![
        Shape {
            name: "rate[5m], instant",
            series: 708,
            query: Query::instant(5 * MINUTE, true),
        },
        Shape {
            name: "rate[5m], 1h range at 15s",
            series: 708,
            query: Query::range(5 * MINUTE, 240, 15_000, true),
        },
        Shape {
            name: "rate[5m], 1d range at 1m",
            series: 708,
            query: Query::range(5 * MINUTE, 1_440, MINUTE, true),
        },
        Shape {
            name: "rate[1d], instant",
            series: 708,
            query: Query::instant(DAY, true),
        },
        Shape {
            name: "increase[2w], instant",
            series: 177,
            query: Query::instant(14 * DAY, false),
        },
    ]
}

struct Candidate {
    label: &'static str,
    layout: LayoutKind,
}

const CANDIDATES: [Candidate; 2] = [
    Candidate {
        label: "struct_ree · operator",
        layout: LayoutKind::StructRee,
    },
    Candidate {
        label: "list · operator",
        layout: LayoutKind::List,
    },
];

struct Measured {
    df_rows: usize,
    batches: usize,
    labels_bytes: usize,
    /// Cumulative cuts of the pipeline: the rate frame, then unnested, then the full plan.
    stages: Vec<(&'static str, Duration)>,
    peak: usize,
    rows: Vec<(String, f64)>,
}

async fn median_of(frame: &DataFrame) -> Result<Duration> {
    let mut times = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let t = Instant::now();
        let batches = frame.clone().collect().await?;
        times.push(t.elapsed());
        drop(batches);
    }
    times.sort();
    Ok(times[RUNS / 2])
}

async fn measure(
    c: &Candidate,
    spec: &Spec,
    query: Query,
    kind: Plan,
    target: usize,
) -> Result<Measured> {
    let rows_per_batch = c.layout.rows_per_batch(SAMPLES_PER_BATCH, spec.samples);
    let built = c.layout.build(spec, Chunking::new(target, rows_per_batch));

    let ctx = session(target);
    let rate = rate_frame(&ctx, scan_table(&built)?, c.layout, query)?;
    let mut cuts = vec![("rate", rate.clone())];
    if !kind.is_instant() {
        cuts.push(("unnest", unnested(rate.clone())?));
    }
    let full = plan(rate, kind)?;
    cuts.push(("full", full.clone()));

    // Warm up, then time every cut.
    let batches = full.clone().collect().await?;
    let mut stages = Vec::new();
    for (name, frame) in &cuts {
        stages.push((*name, median_of(frame).await?));
    }

    let before = Counting::in_use();
    Counting::reset_peak();
    let held = full.collect().await?;
    let peak = Counting::peak_above(before);
    drop(held);

    Ok(Measured {
        df_rows: built.rows,
        batches: built.batches(),
        labels_bytes: built.labels_bytes,
        stages,
        peak,
        rows: read_groups(&batches),
    })
}

// ---------------------------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------------------------

struct Report(String);

impl Report {
    fn line(&mut self, s: impl AsRef<str>) {
        println!("{}", s.as_ref());
        self.0.push_str(s.as_ref());
        self.0.push('\n');
    }
}

fn human(bytes: usize) -> String {
    let mut b = bytes as f64;
    for unit in ["B", "KB", "MB", "GB"] {
        if b < 1024.0 {
            return format!("{b:.1} {unit}");
        }
        b /= 1024.0;
    }
    format!("{b:.1} TB")
}

fn ms(d: Duration) -> String {
    format!("{:.1} ms", d.as_secs_f64() * 1000.0)
}

fn commas(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn same(a: &[(String, f64)], b: &[(String, f64)]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|((ka, va), (kb, vb))| ka == kb && (va - vb).abs() <= 1e-9 * va.abs().max(1.0))
}

/// Run every shape against both candidates and print the report; `--write-results` also writes
/// it to `RESULTS.md`.
pub async fn run() -> Result<()> {
    let write = std::env::args().any(|a| a == "--write-results");
    let target = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut r = Report(String::new());

    r.line("# promql-layout-bench results");
    r.line("");
    r.line(format!(
        "DataFusion {}. {target} target partitions, each holding a contiguous run of whole \
         series. Both layouts arrive in batches of about {} samples: that many rows when rows are \
         samples, as many whole series as fit when rows are series. Scrape interval {}s. Every \
         cell is the median of {RUNS} runs after one warmup.",
        datafusion::DATAFUSION_VERSION,
        commas(SAMPLES_PER_BATCH),
        SCRAPE_MS / 1000,
    ));
    r.line("");
    r.line(
        "Both candidates run the same series-aware `RangeVectorExec`: `struct_ree · operator` \
         feeds it rows as samples, `list · operator` rows as series. `rate`, `+unnest` and `full` \
         are cumulative cuts of the same plan: the range vector alone, then unnested to one row per \
         (series, step), then the whole query. Peak alloc is allocation above the level before the \
         query, from a counting allocator.",
    );

    for shape in shapes() {
        let spec = Spec::new(shape.series, shape.query.samples());
        let query = shape.query.at(spec.at());
        r.line("");
        r.line(format!(
            "## {} · {} series × {} samples = {} samples",
            shape.name,
            commas(spec.series),
            commas(spec.samples),
            commas(spec.total())
        ));

        let kinds: Vec<Plan> = if query.is_instant() {
            vec![Plan::InstantSum]
        } else {
            vec![Plan::RangeSum, Plan::RangeTopk { k: TOPK }]
        };

        for kind in kinds {
            let range = !kind.is_instant();
            r.line("");
            r.line(format!("### {}", kind.label()));
            r.line("");
            r.line(if range {
                "| candidate | df rows | batches | labels | rate | +unnest | full | peak alloc | out rows |"
            } else {
                "| candidate | df rows | batches | labels | rate | full | peak alloc | out rows |"
            });
            r.line(if range {
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|"
            } else {
                "|---|---:|---:|---:|---:|---:|---:|---:|"
            });

            let mut baseline: Option<Vec<(String, f64)>> = None;
            for c in &CANDIDATES {
                let m = measure(c, &spec, query, kind, target).await?;
                let cuts: Vec<String> = m.stages.iter().map(|(_, d)| ms(*d)).collect();
                r.line(format!(
                    "| {} | {} | {} | {} | {} | {} | {} |",
                    c.label,
                    commas(m.df_rows),
                    commas(m.batches),
                    human(m.labels_bytes),
                    cuts.join(" | "),
                    human(m.peak),
                    commas(m.rows.len()),
                ));
                match &baseline {
                    None => baseline = Some(m.rows),
                    Some(expected) => {
                        if !same(expected, &m.rows) {
                            r.line(format!(
                                "| | **{} differs from {}** |",
                                c.label, CANDIDATES[0].label
                            ));
                        }
                    }
                }
            }
        }
    }

    if write {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/RESULTS.md");
        fs::write(path, &r.0).expect("write RESULTS.md");
        println!("\nwrote {path}");
    }
    Ok(())
}
