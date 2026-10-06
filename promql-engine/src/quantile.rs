//! The `quantile` aggregation as a DataFusion aggregate function:
//! `promql_quantile(samples, q, start, end, step) … GROUP BY <label columns>`.
//!
//! Ports the `QUANTILE` arms of upstream's `aggregation` (`promql/engine.go`
//! at 83962c35): every value of a group at a step is collected, and
//! [`math::quantile`] answers over them when the group closes. `q` is a
//! scalar expression, so it is its own value at each step, passed as one
//! list literal ([`aggregate::params_literal`]).
//!
//! It is not a lane of [`crate::aggregate`]: `sum` and its siblings fold a
//! value into a fixed-size state, and an exact quantile has no such state.
//! What this holds is every sample of the group in the block, `(step,
//! value)` pairs in one flat vector, 16 bytes each. That is as much as
//! Prometheus holds, and it is the one aggregation whose memory is
//! proportional to the series under it rather than to groups times steps.
//! A sketch would bound it and would also be a different answer.
//!
//! The partial state is the same pairs, so two partitions merge by
//! concatenation, and the final sort makes the order they arrived in
//! irrelevant.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, ListArray, StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};

use crate::aggregate::{int_arg, params_arg, params_literal, params_type, Grid};
use crate::math;
use crate::series;

pub const NAME: &str = "promql_quantile";

/// The pairs, as the rows of a one-row list: the partial state, and
/// what [`Quantile::result`] builds its answer from.
fn one_row(item: FieldRef, entries: StructArray) -> ScalarValue {
    let len = entries.len() as i32;
    ScalarValue::List(Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(vec![0, len].into()),
        Arc::new(entries),
        None,
    )))
}

/// One group's values, collected.
#[derive(Debug)]
pub struct Quantile {
    grid: Grid,
    /// `q` at each step.
    q: Vec<f64>,
    /// `(step index, value)` in arrival order.
    pairs: Vec<(u32, f64)>,
}

impl Quantile {
    pub fn new(q: Vec<f64>, start_ms: i64, end_ms: i64, step_ms: i64) -> Result<Self> {
        let grid = Grid::new(NAME, start_ms, end_ms, step_ms)?;
        if q.len() != grid.len() {
            return Err(DataFusionError::Execution(format!(
                "{NAME}: {} values of q for a grid of {} steps",
                q.len(),
                grid.len()
            )));
        }
        Ok(Self {
            grid,
            q,
            pairs: Vec::new(),
        })
    }

    /// The quantile at each step something reached, in step order.
    ///
    /// A counting sort by step lays each step's values side by side, so
    /// that [`math::quantile`] sorts a short slice per step rather than
    /// one long vector by `(step, value)`.
    fn result(&self) -> Vec<(i64, f64)> {
        let mut starts = vec![0usize; self.grid.len() + 1];
        for (step, _) in &self.pairs {
            starts[*step as usize + 1] += 1;
        }
        for i in 0..self.grid.len() {
            starts[i + 1] += starts[i];
        }
        let mut next = starts.clone();
        let mut values = vec![0.0; self.pairs.len()];
        for (step, v) in &self.pairs {
            let slot = &mut next[*step as usize];
            values[*slot] = *v;
            *slot += 1;
        }
        (0..self.grid.len())
            .filter(|i| starts[*i] < starts[*i + 1])
            .map(|i| {
                let at = &mut values[starts[i]..starts[i + 1]];
                (self.grid.timestamp(i), math::quantile(self.q[i], at))
            })
            .collect()
    }

    fn entries(fields: Fields, ts: Vec<i64>, vs: Vec<f64>) -> StructArray {
        StructArray::new(
            fields,
            vec![
                Arc::new(TimestampMillisecondArray::from(ts)),
                Arc::new(Float64Array::from(vs)),
            ],
            None,
        )
    }
}

impl Accumulator for Quantile {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let list = values[0].as_list::<i32>();
        let entries = list.values().as_struct();
        let (ts, vs) = series::sample_slices(entries);
        let offsets = list.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let (lo, hi) = (offsets[row] as usize, offsets[row + 1] as usize);
            let (pairs, grid) = (&mut self.pairs, self.grid);
            grid.runs(&ts[lo..hi], |index, from, len| {
                for k in 0..len {
                    pairs.push(((index + k) as u32, vs[lo + from + k]));
                }
            })?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let (ts, vs): (Vec<i64>, Vec<f64>) = self.result().into_iter().unzip();
        Ok(one_row(
            series::sample_item(),
            Self::entries(series::sample_fields(), ts, vs),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.q.capacity() * std::mem::size_of::<f64>()
            + self.pairs.capacity() * std::mem::size_of::<(u32, f64)>()
    }

    /// The pairs themselves, timestamped by their step. Unlike a sample
    /// column the timestamps repeat, which is why the merge below reads
    /// them one at a time instead of through [`Grid::runs`].
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let ts = self
            .pairs
            .iter()
            .map(|(step, _)| self.grid.timestamp(*step as usize))
            .collect();
        let vs = self.pairs.iter().map(|(_, v)| *v).collect();
        Ok(vec![one_row(
            series::sample_item(),
            Self::entries(series::sample_fields(), ts, vs),
        )])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let list = states[0].as_list::<i32>();
        let entries = list.values().as_struct();
        let (ts, vs) = series::sample_slices(entries);
        let offsets = list.offsets();
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            for i in offsets[row] as usize..offsets[row + 1] as usize {
                self.pairs.push((self.grid.index(ts[i])? as u32, vs[i]));
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct QuantileUdaf {
    signature: Signature,
}

impl Default for QuantileUdaf {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    series::samples_type(),
                    params_type(),
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

pub fn udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(QuantileUdaf::default())
}

/// `promql_quantile(samples, q, start, end, step)`, `q` being the value of
/// the parameter expression at each step of the grid.
pub fn call(samples: Expr, q: &[f64], start_ms: i64, end_ms: i64, step_ms: i64) -> Expr {
    udaf().call(vec![
        samples,
        params_literal(q),
        lit(start_ms),
        lit(end_ms),
        lit(step_ms),
    ])
}

impl AggregateUDFImpl for QuantileUdaf {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.first() != Some(&series::samples_type()) {
            return plan_err!(
                "{NAME}: first argument must be {}, got {:?}",
                series::samples_type(),
                arg_types.first()
            );
        }
        Ok(series::samples_type())
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return plan_err!("{NAME}: DISTINCT is not supported");
        }
        let (start, end, step) = (
            int_arg(&args, 2, "start", NAME)?,
            int_arg(&args, 3, "end", NAME)?,
            int_arg(&args, 4, "step", NAME)?,
        );
        let grid = Grid::new(NAME, start, end, step)?;
        let q = params_arg(&args, 1, &grid, NAME)?;
        Ok(Box::new(Quantile::new(q, start, end, step)?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "pairs"),
            series::samples_type(),
            false,
        ))])
    }

    /// What an aggregation over no rows at all yields: no samples.
    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        let mut empty = Quantile::new(vec![0.0], 0, 0, 1)?;
        empty.evaluate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples(rows: &[&[(i64, f64)]]) -> ArrayRef {
        let ts: Vec<i64> = rows.iter().flat_map(|r| r.iter().map(|s| s.0)).collect();
        let vs: Vec<f64> = rows.iter().flat_map(|r| r.iter().map(|s| s.1)).collect();
        let mut offsets = vec![0i32];
        for r in rows {
            offsets.push(offsets.last().unwrap() + r.len() as i32);
        }
        Arc::new(ListArray::new(
            series::sample_item(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(Quantile::entries(series::sample_fields(), ts, vs)),
            None,
        ))
    }

    fn pairs(value: ScalarValue) -> Vec<(i64, f64)> {
        let ScalarValue::List(list) = value else {
            panic!("a list")
        };
        let entries = list.value(0);
        let entries = entries.as_struct();
        let (ts, vs) = series::sample_slices(entries);
        ts.iter().copied().zip(vs.iter().copied()).collect()
    }

    fn grid_of_three(q: [f64; 3]) -> Quantile {
        Quantile::new(q.to_vec(), 0, 2, 1).unwrap()
    }

    #[test]
    fn each_step_takes_its_own_q_over_the_values_present_at_it() {
        let a: &[(i64, f64)] = &[(0, 1.0), (1, 5.0), (2, 9.0)];
        let b: &[(i64, f64)] = &[(0, 3.0), (2, 1.0)];
        let mut acc = grid_of_three([0.0, 1.0, 0.5]);
        acc.update_batch(&[samples(&[a, b])]).unwrap();
        // Step 0 holds {1, 3} at q=0, step 1 holds {5} at q=1, step 2 holds
        // {9, 1} at q=0.5.
        assert_eq!(
            pairs(acc.evaluate().unwrap()),
            vec![(0, 1.0), (1, 5.0), (2, 5.0)]
        );
    }

    #[test]
    fn a_step_nothing_reached_is_absent() {
        let mut acc = grid_of_three([0.5; 3]);
        acc.update_batch(&[samples(&[&[(1, 2.0)]])]).unwrap();
        assert_eq!(pairs(acc.evaluate().unwrap()), vec![(1, 2.0)]);
    }

    /// A partition's partial state is its values, so merging two is
    /// the same answer as one accumulator that saw both, whichever
    /// arrived first.
    #[test]
    fn merging_partials_is_the_quantile_over_all_the_values() {
        let a: &[(i64, f64)] = &[(0, 1.0), (1, f64::NAN), (2, 3.0)];
        let b: &[(i64, f64)] = &[(0, 2.0), (1, 4.0), (2, 8.0)];
        let q = [0.5, 0.5, 0.25];
        let mut whole = grid_of_three(q);
        whole.update_batch(&[samples(&[a, b])]).unwrap();

        let mut left = grid_of_three(q);
        left.update_batch(&[samples(&[a])]).unwrap();
        let mut right = grid_of_three(q);
        right.update_batch(&[samples(&[b])]).unwrap();
        let mut merged = grid_of_three(q);
        for part in [&mut right, &mut left] {
            let ScalarValue::List(state) = part.state().unwrap().remove(0) else {
                panic!("a list")
            };
            merged.merge_batch(&[state as ArrayRef]).unwrap();
        }
        // NaN is never equal to itself, so compare the bits.
        let bits = |v: ScalarValue| -> Vec<(i64, u64)> {
            pairs(v)
                .into_iter()
                .map(|(t, v)| (t, v.to_bits()))
                .collect()
        };
        assert_eq!(
            bits(merged.evaluate().unwrap()),
            bits(whole.evaluate().unwrap())
        );
    }

    #[test]
    fn a_sample_off_the_grid_is_an_error() {
        let mut acc = grid_of_three([0.5; 3]);
        assert!(acc.update_batch(&[samples(&[&[(7, 1.0)]])]).is_err());
    }

    #[test]
    fn q_must_cover_the_grid() {
        assert!(Quantile::new(vec![0.5], 0, 2, 1).is_err());
    }
}
