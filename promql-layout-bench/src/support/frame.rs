//! The window form: a native `RANGE BETWEEN ... PRECEDING` frame.
//!
//! This answers one question only, can a layout drive a native DataFusion window frame at all,
//! and is pinned by characterisation tests rather than timed. Nothing in the benchmark's numbers
//! depends on it.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    col, ExprFunctionExt, PartitionEvaluator, Signature, Volatility, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowUDF, WindowUDFImpl,
};
use datafusion::prelude::DataFrame;

use super::format::COL_LABELS;

/// A native window frame over the samples of one series:
/// `fake_rate_window(value) OVER (PARTITION BY labels ORDER BY time RANGE 5m PRECEDING)`.
///
/// Meaningful when rows are samples. When rows are series DataFusion still plans and runs it, but
/// the frame is a range of rows and each row is a whole series, so the evaluator is handed
/// `List<Float64>` per row and reports that. Both outcomes are pinned by characterisation tests.
pub fn window_frame(raw: DataFrame, value: &str, time: &str) -> Result<DataFrame> {
    let five_minutes = ScalarValue::new_interval_mdn(0, 0, 5 * 60 * 1_000_000_000);
    let frame = WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::Preceding(five_minutes),
        WindowFrameBound::CurrentRow,
    );
    let rate = WindowUDF::from(FakeRateWindow::new())
        .call(vec![col(value)])
        .partition_by(vec![col(COL_LABELS)])
        .order_by(vec![col(time).sort(true, true)])
        .window_frame(frame)
        .build()?
        .alias("r");
    raw.window(vec![rate])
}

/// Window form, the one that needs a real `RANGE BETWEEN ... PRECEDING` frame.
pub const FAKE_RATE_WINDOW: &str = "fake_rate_window";

/// `fake_rate_window(value) OVER (PARTITION BY labels ORDER BY timestamp RANGE ...)`.
///
/// The frame handed to the evaluator is a range of **row** indices, which assumes rows are samples.
/// When rows are series the evaluator is handed whole series, `List<Float64>` per row, and says
/// so rather than pretending. The arithmetic is a placeholder because the frame is what is under
/// test, and a frame yields one result per input row, not per step, so a real `rate` here would
/// not answer a PromQL range query anyway.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FakeRateWindow {
    signature: Signature,
}

impl Default for FakeRateWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeRateWindow {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl WindowUDFImpl for FakeRateWindow {
    fn name(&self) -> &str {
        FAKE_RATE_WINDOW
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(WindowEvaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }
}

#[derive(Debug)]
struct WindowEvaluator;

impl PartitionEvaluator for WindowEvaluator {
    fn uses_window_frame(&self) -> bool {
        true
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &std::ops::Range<usize>,
    ) -> Result<ScalarValue> {
        // The frame is row indices, which is only meaningful when a row is a sample.
        let Some(vs) = values[0].as_primitive_opt::<Float64Type>() else {
            return exec_err!(
                "the frame is a range of rows and each row here is a whole series ({} per row), \
                 not a sample",
                values[0].data_type()
            );
        };
        let sum: f64 = vs.values()[range.start..range.end].iter().sum();
        Ok(ScalarValue::Float64(Some(sum)))
    }
}
