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

use std::collections::HashMap;

use datafusion::arrow::array::{AsArray, RecordBatch};
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
}
