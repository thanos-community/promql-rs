//! `cleanupMetricLabels` on the query's result: two series that ended up
//! with one label set.
//!
//! Prometheus runs with delayed `__name__` removal (`promql/engine.go`,
//! `evaluator.cleanupMetricLabels`, and `EnableDelayedNameRemoval: true`
//! in `promql/promqltest`), so dropping the name never errors where it
//! happens. The name stays on the series for the whole evaluation and is
//! removed once, on the way out; only then does upstream look for a label
//! set twice. Every check inside the evaluation — above a call, above a
//! binary operator, in `rangeEval` — is behind `!enableDelayedNameRemoval`
//! and dead. Checking earlier would answer with an error where upstream
//! answers with a number: `sum(rate({env="1"}[10m])) by (env)` adds two
//! rows that lost their names into one series and is not a duplicate at
//! all.
//!
//! Upstream errors only when two such series share a timestamp, and
//! merges them into one series when they do not. Two metrics alike in
//! every label but the name, sampled over windows that do not overlap,
//! are exactly that under a name-dropping call, so it is reachable here
//! and the merge is a known gap: the result then carries both rows where
//! upstream carries one series. Closing it needs an Arrow concat pass
//! over the colliding rows' points, so only the error is mirrored.

use std::cmp::Ordering;
use std::collections::HashMap;

use datafusion::arrow::array::{AsArray, RecordBatch, StringViewArray};
use datafusion::arrow::datatypes::TimestampMillisecondType;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::error::DataFusionError;

use crate::error::EngineError;
use crate::series::{LABELS, SAMPLES, TIMESTAMP};

/// Prometheus's own sentence, word for word: the conformance suite
/// compares error text, not error kinds.
pub const SAME_LABELSET: &str = "vector cannot contain metrics with the same labelset";

/// Reject a result that carries one label set on two series.
///
/// Called on the final batches, with empty rows already dropped: a series
/// that produced no points is not in upstream's result matrix and cannot
/// collide with anything in it.
pub fn reject_same_labelset(batches: &[RecordBatch]) -> Result<(), EngineError> {
    let Some(first) = batches.first() else {
        return Ok(());
    };
    let labels = first
        .schema()
        .field_with_name(LABELS)
        .map_err(|e| EngineError::Schema(e.to_string()))?
        .data_type()
        .clone();
    // Row format rather than a hash: `create_hashes` would report a
    // collision as a duplicate label set, and this error is an answer the
    // user sees, not a heuristic.
    let converter = RowConverter::new(vec![SortField::new(labels)]).map_err(arrow_error)?;
    // Label sets first, and timestamps only for a label set that turned
    // up twice: upstream reaches for the timestamps from behind a
    // `ContainsSameLabelset` guard as well, and a result whose rows are
    // all distinct is every query that answers with a number.
    let mut seen: HashMap<Vec<u8>, Vec<Location>> = HashMap::new();
    for (batch, b) in batches.iter().enumerate() {
        let column = b
            .column_by_name(LABELS)
            .ok_or_else(|| EngineError::Schema(format!("no {LABELS} column")))?;
        let rows = converter
            .convert_columns(std::slice::from_ref(column))
            .map_err(arrow_error)?;
        for (row, bytes) in rows.iter().enumerate() {
            seen.entry(bytes.as_ref().to_vec())
                .or_default()
                .push(Location { batch, row });
        }
    }

    for rows in seen.values().filter(|rows| rows.len() > 1) {
        let mut timestamps = Vec::new();
        for at in rows {
            timestamps.extend(timestamps_of(&batches[at.batch], at.row)?);
        }
        timestamps.sort_unstable();
        if timestamps.windows(2).any(|w| w[0] == w[1]) {
            return Err(EngineError::Query(SAME_LABELSET.into()));
        }
    }
    Ok(())
}

/// The result's rows in the order Prometheus returns a range query's
/// matrix: `sort.Sort(mat)` in `promql/engine.go`, which compares label
/// sets pair by pair, `labels.Compare`. The plan ends in as many
/// partitions as the store has and they finish in any order, so without
/// this the order would change from run to run.
///
/// The `labels` struct's own order, which `SeriesSetExec` declares, is
/// not that order: it compares field by field with `""` for an absent
/// label, so `{app="q"}` sorts after `{i="0"}` there and before it here.
pub fn sort_by_labelset(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>, EngineError> {
    let mut columns = Vec::with_capacity(batches.len());
    for batch in batches {
        let labels = batch
            .column_by_name(LABELS)
            .ok_or_else(|| EngineError::Schema(format!("no {LABELS} column")))?
            .as_struct();
        // `compare` pairs fields by position across batches.
        if columns.is_empty() || batch.schema_ref() == batches[0].schema_ref() {
            columns.push(
                labels
                    .columns()
                    .iter()
                    .map(|c| c.as_string_view())
                    .collect(),
            );
        } else {
            return Err(EngineError::Schema(
                "result batches differ in schema".into(),
            ));
        }
    }
    let mut order: Vec<(usize, usize)> = batches
        .iter()
        .enumerate()
        .flat_map(|(b, batch)| (0..batch.num_rows()).map(move |row| (b, row)))
        .collect();
    let cmp = |x: &(usize, usize), y: &(usize, usize)| compare(&columns, *x, *y);
    // A single store partition, and label sets that all carry the same
    // names, come out of the plan in this order already.
    if order.is_sorted_by(|x, y| cmp(x, y) != Ordering::Greater) {
        return Ok(batches.to_vec());
    }
    order.sort_by(cmp);
    // Slices of the input rather than `interleave_record_batch`: the
    // samples are nearly the whole result (a 30d selector over 1000
    // series is ~650 MB of them) and interleave copies every one, while
    // `RecordBatch::slice` only bumps the Arc'd buffers. The cost is more,
    // shorter batches, one per run of rows that stayed adjacent. A store
    // that hashes series across partitions interleaves them row by row,
    // so expect about one batch per series; consumers already take any
    // number of batches.
    let mut runs: Vec<RecordBatch> = Vec::new();
    let mut run: Option<(usize, usize, usize)> = None;
    for (b, row) in order {
        run = match run {
            Some((rb, start, len)) if rb == b && start + len == row => Some((rb, start, len + 1)),
            Some((rb, start, len)) => {
                runs.push(batches[rb].slice(start, len));
                Some((b, row, 1))
            }
            None => Some((b, row, 1)),
        };
    }
    runs.extend(run.map(|(b, start, len)| batches[b].slice(start, len)));
    Ok(runs)
}

/// `labels.Compare` of two rows, read off the columns without collecting
/// either label set. Where the rows first differ and one has the label and
/// the other not, the one without it continues with a later name, or ends.
fn compare(columns: &[Vec<&StringViewArray>], x: (usize, usize), y: (usize, usize)) -> Ordering {
    let (a, b) = (&columns[x.0], &columns[y.0]);
    let has_more = |c: &[&StringViewArray], row: usize, from: usize| {
        c[from..].iter().any(|v| !v.value(row).is_empty())
    };
    for f in 0..a.len() {
        let (va, vb) = (a[f].value(x.1), b[f].value(y.1));
        if va == vb {
            continue;
        }
        return match (va.is_empty(), vb.is_empty()) {
            (false, true) if has_more(b, y.1, f + 1) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (true, false) if has_more(a, x.1, f + 1) => Ordering::Greater,
            (true, false) => Ordering::Less,
            _ => va.cmp(vb),
        };
    }
    Ordering::Equal
}

/// Where one row of the result sits, so the second pass can go back to it.
struct Location {
    batch: usize,
    row: usize,
}

/// The timestamps of one row's points.
fn timestamps_of(batch: &RecordBatch, row: usize) -> Result<Vec<i64>, EngineError> {
    let points = batch
        .column_by_name(SAMPLES)
        .ok_or_else(|| EngineError::Schema(format!("no {SAMPLES} column")))?
        .as_list::<i32>()
        .value(row);
    Ok(points
        .as_struct()
        .column_by_name(TIMESTAMP)
        .ok_or_else(|| EngineError::Schema(format!("no {TIMESTAMP} field")))?
        .as_primitive::<TimestampMillisecondType>()
        .values()
        .to_vec())
}

fn arrow_error(e: ArrowError) -> EngineError {
    EngineError::DataFusion(DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::{encode, label_names_of, Series};

    fn row(pod: &str, name: &str, timestamps: Vec<i64>) -> Series {
        let values = vec![1.0; timestamps.len()];
        Series::new(&[("__name__", name), ("pod", pod)], timestamps, values).unwrap()
    }

    /// Encode `batches`, one batch per inner vec, and check them.
    ///
    /// Rows are encoded one at a time and concatenated: [`encode`]
    /// refuses a duplicate label set, which is the very input under test
    /// here — a plan that emits one is what this catches.
    fn check(batches: Vec<Vec<Series>>) -> Result<(), EngineError> {
        let names = label_names_of(&batches.concat());
        let one = |row| encode(&names, std::slice::from_ref(row)).unwrap();
        let schema = one(&batches[0][0]).schema();
        let batches: Vec<RecordBatch> = batches
            .iter()
            .map(|rows| {
                datafusion::arrow::compute::concat_batches(
                    &schema,
                    rows.iter().map(one).collect::<Vec<_>>().iter(),
                )
                .unwrap()
            })
            .collect();
        reject_same_labelset(&batches)
    }

    #[test]
    fn a_label_set_repeated_across_batches_is_an_error() {
        let err = check(vec![
            vec![row("envoy-1", "a", vec![0]), row("envoy-2", "a", vec![0])],
            vec![row("envoy-3", "a", vec![0]), row("envoy-1", "a", vec![0])],
        ])
        .unwrap_err();
        assert!(err.to_string().contains(SAME_LABELSET), "{err}");
    }

    #[test]
    fn a_label_set_repeated_within_one_batch_is_an_error() {
        let err = check(vec![vec![
            row("envoy-1", "a", vec![0]),
            row("envoy-1", "a", vec![0]),
        ]])
        .unwrap_err();
        assert!(err.to_string().contains(SAME_LABELSET), "{err}");
    }

    #[test]
    fn distinct_label_sets_are_fine() {
        check(vec![
            vec![row("envoy-1", "a", vec![0]), row("envoy-2", "a", vec![0])],
            vec![row("envoy-3", "a", vec![0])],
        ])
        .unwrap();
    }

    /// Upstream merges these rather than erroring; the error is what is
    /// mirrored, so at least it must not fire.
    #[test]
    fn one_label_set_on_rows_that_share_no_timestamp_is_not_an_error() {
        check(vec![vec![
            row("envoy-1", "a", vec![0, 30_000]),
            row("envoy-1", "a", vec![60_000]),
        ]])
        .unwrap();
    }

    /// Every ordering of label sets that differ in which names they carry,
    /// split over two batches, against `labels.Compare` written the obvious
    /// way: pair lists compared lexicographically.
    #[test]
    fn sort_by_labelset_is_labels_compare() {
        let sets: Vec<Vec<(&str, &str)>> = vec![
            vec![("a", "1")],
            vec![("a", "1"), ("b", "1")],
            vec![("a", "1"), ("c", "1")],
            vec![("a", "2")],
            vec![("b", "1")],
            vec![("b", "2"), ("c", "1")],
            vec![("c", "1")],
            vec![],
        ];
        let series: Vec<Series> = sets
            .iter()
            .rev()
            .map(|l| Series::new(l, vec![0], vec![1.0]).unwrap())
            .collect();
        let names = label_names_of(&series);
        let (front, back) = series.split_at(4);
        let batches = [
            encode(&names, front).unwrap(),
            encode(&names, back).unwrap(),
        ];
        let sorted = crate::series::decode(&sort_by_labelset(&batches).unwrap()).unwrap();
        let got: Vec<Vec<(&str, &str)>> = sorted.iter().map(|s| s.labels().collect()).collect();
        let mut want = sets.clone();
        want.sort();
        assert_eq!(got, want);
    }

    /// The value buffer behind a batch's samples: shared with the input
    /// when the output is a slice of it, fresh when the rows were copied.
    fn values_buffer(batch: &RecordBatch) -> *const u8 {
        batch
            .column_by_name(SAMPLES)
            .unwrap()
            .as_list::<i32>()
            .values()
            .as_struct()
            .column_by_name(crate::series::VALUE)
            .unwrap()
            .to_data()
            .buffers()[0]
            .as_ptr()
    }

    /// Two partitions whose rows alternate in label-set order: the output
    /// is one slice per row, and every slice still points at its input's
    /// samples.
    #[test]
    fn sort_by_labelset_slices_without_copying_samples() {
        let series: Vec<Series> = (0..6)
            .map(|i| row(&format!("p{i}"), "x", vec![0, 1, 2]))
            .collect();
        let names = label_names_of(&series);
        let even: Vec<Series> = series.iter().step_by(2).cloned().collect();
        let odd: Vec<Series> = series.iter().skip(1).step_by(2).cloned().collect();
        let batches = [
            encode(&names, &odd).unwrap(),
            encode(&names, &even).unwrap(),
        ];
        let inputs: Vec<*const u8> = batches.iter().map(values_buffer).collect();

        let sorted = sort_by_labelset(&batches).unwrap();
        assert_eq!(sorted.len(), 6);
        for batch in &sorted {
            assert!(
                inputs.contains(&values_buffer(batch)),
                "samples were copied"
            );
        }
        let pods: Vec<String> = crate::series::decode(&sorted)
            .unwrap()
            .iter()
            .map(|s| s.label("pod").to_string())
            .collect();
        assert_eq!(pods, ["p0", "p1", "p2", "p3", "p4", "p5"]);
    }
}
