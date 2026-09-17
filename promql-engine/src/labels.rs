//! Writing the `labels` struct: `promql_labels(name, value, name, value, …)`.
//!
//! Every operator that changes a series' labels — an aggregation keeping
//! only its grouping labels, a range function dropping `__name__` —
//! has to build a new `labels` struct. DataFusion's `named_struct` would,
//! but it marks every field nullable, so the result would no longer be
//! the canonical shape and the next operator's schema check would reject
//! it. This function is `named_struct` with the canonical field types
//! promised up front.
//!
//! Values arrive as `Utf8View`, read straight out of an existing struct
//! with `get_field` or back out of a group-by's key columns, so usually
//! there is nothing to convert; a plain `Utf8` value is cast. NULLs,
//! should any appear, become `""`, which is how the shape spells
//! "absent".
//!
//! The planner-side helpers below decide *which* labels an operator keeps
//! and produce the expressions; they are pure functions over name lists.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringViewBuilder, StructArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::{
    col, lit, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};

use crate::matcher::METRIC_NAME;
use crate::series::{self, LABELS};

pub const NAME: &str = "promql_labels";

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Labels {
    signature: Signature,
}

impl Default for Labels {
    fn default() -> Self {
        Self {
            // Zero pairs is the empty label set, which `sum(x)` produces.
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
        }
    }
}

pub fn udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(Labels::default())
}

/// `promql_labels('a', <a>, 'b', <b>, …)` for `(name, value)` pairs. The
/// names must be sorted and unique; the planner helpers guarantee that.
pub fn call(pairs: Vec<(String, Expr)>) -> Expr {
    let mut args = Vec::with_capacity(pairs.len() * 2);
    for (name, value) in pairs {
        args.push(lit(name));
        args.push(value);
    }
    udf().call(args)
}

/// The names given as literal arguments, validated.
fn names(scalar_arguments: &[Option<&ScalarValue>]) -> Result<Vec<String>> {
    if !scalar_arguments.len().is_multiple_of(2) {
        return plan_err!(
            "{NAME}: expects (name, value) pairs, got {} arguments",
            scalar_arguments.len()
        );
    }
    let mut out = Vec::with_capacity(scalar_arguments.len() / 2);
    for (i, pair) in scalar_arguments.chunks(2).enumerate() {
        match pair[0] {
            Some(ScalarValue::Utf8(Some(n))) if !n.is_empty() => out.push(n.clone()),
            other => {
                return plan_err!(
                    "{NAME}: argument {} must be a non-empty string literal, got {other:?}",
                    2 * i
                )
            }
        }
    }
    if out.windows(2).any(|w| w[0] >= w[1]) {
        return plan_err!("{NAME}: label names must be sorted and unique, got {out:?}");
    }
    Ok(out)
}

fn value_type_ok(t: &DataType) -> bool {
    *t == series::label_type() || matches!(t, DataType::Utf8 | DataType::Null)
}

/// One value column as the canonical leaf, NULLs as `""`.
fn canonical(values: &ArrayRef) -> Result<ArrayRef> {
    let views = if values.data_type() == &series::label_type() {
        Arc::clone(values)
    } else {
        cast(values.as_ref(), &series::label_type())?
    };
    if views.null_count() == 0 {
        return Ok(views);
    }
    let mut b = StringViewBuilder::with_capacity(views.len());
    for v in views.as_string_view() {
        b.append_value(v.unwrap_or(""));
    }
    Ok(Arc::new(b.finish()))
}

impl ScalarUDFImpl for Labels {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(format!(
            "{NAME}: return_field_from_args should be called instead"
        )))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let names = names(args.scalar_arguments)?;
        for (i, f) in args.arg_fields.iter().enumerate().skip(1).step_by(2) {
            if !value_type_ok(f.data_type()) {
                return plan_err!(
                    "{NAME}: argument {i} must be {} or Utf8, got {}",
                    series::label_type(),
                    f.data_type()
                );
            }
        }
        Ok(Arc::new(Field::new(
            NAME,
            series::labels_type(&names),
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let mut fields: Vec<FieldRef> = Vec::with_capacity(args.args.len() / 2);
        let mut children: Vec<ArrayRef> = Vec::with_capacity(args.args.len() / 2);
        for pair in args.args.chunks(2) {
            let name = match &pair[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) => name.clone(),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "{NAME}: label name must be a string literal, got {:?}",
                        other.data_type()
                    )))
                }
            };
            let values = match &pair[1] {
                ColumnarValue::Array(a) => Arc::clone(a),
                ColumnarValue::Scalar(s) => s.to_array_of_size(n)?,
            };
            fields.push(Arc::new(Field::new(name, series::label_type(), false)));
            children.push(canonical(&values)?);
        }
        let labels = if fields.is_empty() {
            StructArray::new_empty_fields(n, None)
        } else {
            StructArray::try_new(Fields::from(fields), children, None)?
        };
        Ok(ColumnarValue::Array(Arc::new(labels)))
    }
}

/// Which labels an aggregation groups by, given the input's label names.
///
/// `by (l…)` keeps the listed labels that exist in the input — including
/// `__name__` if listed. `without (l…)` keeps everything else except the
/// listed labels and `__name__`. Neither keeps nothing. Sorted, unique.
/// This is `generateGroupingLabels` in upstream's `engine.go`.
pub fn group_keys(input: &[String], grouping: &[String], without: bool) -> Vec<String> {
    let mut keys: Vec<String> = if without {
        input
            .iter()
            .filter(|n| n.as_str() != METRIC_NAME && !grouping.contains(n))
            .cloned()
            .collect()
    } else {
        input
            .iter()
            .filter(|n| grouping.contains(n))
            .cloned()
            .collect()
    };
    keys.sort();
    keys.dedup();
    keys
}

/// The grouping expressions for `LogicalPlanBuilder::aggregate`:
/// `get_field(labels, k) AS k`. The keys stay `Utf8View`, which
/// DataFusion's vectorized group-by takes as it is.
pub fn group_exprs(keys: &[String]) -> Vec<Expr> {
    keys.iter()
        .map(|k| get_field(col(LABELS), k.as_str()).alias(k))
        .collect()
}

/// The `labels` struct of an aggregation's output, from its group columns.
pub fn regroup(keys: &[String]) -> Expr {
    call(keys.iter().map(|k| (k.clone(), col(k))).collect())
}

/// The `labels` struct with only the names passing `keep`, read from the
/// existing struct. Returns the expression and the names it carries.
pub fn keep(input: &[String], mut keep: impl FnMut(&str) -> bool) -> (Expr, Vec<String>) {
    let mut names: Vec<String> = input.iter().filter(|n| keep(n)).cloned().collect();
    names.sort();
    names.dedup();
    let expr = call(
        names
            .iter()
            .map(|n| (n.clone(), get_field(col(LABELS), n.as_str())))
            .collect(),
    );
    (expr, names)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn by_keeps_listed_labels_that_exist() {
        let input = s(&["__name__", "pod", "route"]);
        assert_eq!(
            group_keys(&input, &s(&["route", "job"]), false),
            s(&["route"])
        );
        assert_eq!(
            group_keys(&input, &s(&["__name__"]), false),
            s(&["__name__"])
        );
        assert!(group_keys(&input, &[], false).is_empty());
    }

    #[test]
    fn without_drops_listed_labels_and_the_metric_name() {
        let input = s(&["__name__", "pod", "route"]);
        assert_eq!(group_keys(&input, &s(&["pod"]), true), s(&["route"]));
        assert_eq!(group_keys(&input, &[], true), s(&["pod", "route"]));
    }

    #[test]
    fn keep_drops_and_sorts() {
        let (_, names) = keep(&s(&["route", "__name__", "pod"]), |n| n != METRIC_NAME);
        assert_eq!(names, s(&["pod", "route"]));
    }

    #[test]
    fn names_must_be_sorted_unique_literals() {
        let a = ScalarValue::Utf8(Some("a".into()));
        let b = ScalarValue::Utf8(Some("b".into()));
        assert_eq!(
            names(&[Some(&a), None, Some(&b), None]).unwrap(),
            s(&["a", "b"])
        );
        assert!(names(&[Some(&b), None, Some(&a), None]).is_err());
        assert!(names(&[Some(&a), None, Some(&a), None]).is_err());
        assert!(names(&[Some(&a)]).is_err());
        assert!(names(&[None, None]).is_err());
    }

    #[test]
    fn canonical_views_strings_and_fills_nulls() {
        use datafusion::arrow::array::StringArray;
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None, Some("x")]));
        let out = canonical(&arr).unwrap();
        assert_eq!(out.data_type(), &series::label_type());
        assert_eq!(out.null_count(), 0);
        let back = cast(out.as_ref(), &DataType::Utf8).unwrap();
        let back = back.as_string::<i32>();
        assert_eq!(back.value(0), "x");
        assert_eq!(back.value(1), "");
    }
}
