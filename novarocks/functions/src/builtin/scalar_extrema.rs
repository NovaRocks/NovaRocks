// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//! Scalar extrema select one original row in the bound common value domain.
use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallContract, ScalarCallInput, SelectedValues, Selection,
    aggregate_scalar::ExtremaDirection,
    comparison::ExtremaOrder,
    control_values::{AssemblyPlan, assemble_values},
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    math_numeric::{
        MathNumericError, MathNumericObservation, NumericArrayView, cast_output_observed,
        value_at_f64,
    },
};
use arrow_array::{Array, ArrayRef, Float64Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::NaiveDateTime;
use std::{alloc::Layout, cmp::Ordering, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub enum ExtremaOperation {
    Greatest,
    Least,
}
type ErrorBoundary<'a> = Option<&'a dyn Fn(&str) -> Result<(), KernelFailure>>;
fn raw_error(message: String, boundary: ErrorBoundary<'_>) -> KernelFailure {
    if let Some(boundary) = boundary
        && let Err(failure) = boundary(&message)
    {
        return failure;
    }
    internal(&message)
}
fn reserve<T>(count: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}
fn temporal(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date32 | DataType::Timestamp(_, _) | DataType::Utf8 | DataType::Null
    )
}
/// The original short temporal argument reads NULL, while len-one broadcasts.
pub fn datetime_value_at(
    values: &[Option<NaiveDateTime>],
    row: usize,
    len: usize,
) -> Option<NaiveDateTime> {
    let idx = if values.len() == 1 && len > 1 { 0 } else { row };
    values.get(idx).copied().flatten()
}
pub(super) fn prepare(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<ExtremaOrder, KernelFailure> {
    let arguments = &contract.selected().argument_types;
    if arguments.is_empty() {
        return Err(invalid("extrema requires at least one value argument"));
    }
    let result = contract.result_type();
    let mut nullable = false;
    for argument in arguments.iter() {
        observe()?;
        let FunctionArgumentType::Value(value) = argument else {
            return Err(invalid("extrema requires value arguments"));
        };
        if !value.same_physical_binding_ignoring_root_nullability_observed(result, &mut observe)? {
            return Err(invalid(
                "extrema argument differs from the common value domain",
            ));
        }
        nullable |= value.nullable() || value.data_type() == &DataType::Null;
    }
    if nullable != result.nullable() {
        return Err(invalid(
            "extrema result nullability differs from its arguments",
        ));
    }
    ExtremaOrder::prepare(result.value_type(), result.data_type(), observe)?
        .ok_or_else(|| invalid("extrema has no ordering for the frozen value domain"))
}

pub(super) fn evaluate<'a>(
    operation: ExtremaOperation,
    order: &ExtremaOrder,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let direction = match operation {
        ExtremaOperation::Greatest => ExtremaDirection::Maximum,
        ExtremaOperation::Least => ExtremaDirection::Minimum,
    };
    let preferred = match direction {
        ExtremaDirection::Minimum => Ordering::Less,
        ExtremaDirection::Maximum => Ordering::Greater,
    };
    let result = (|| {
        let arguments = input.arguments();
        let selection = input.selection();
        let ty = input.contract().result_type().data_type();
        let mut sources = reserve(arguments.len(), &mut work)?;
        for argument in arguments {
            work.step()?;
            sources.push(Some(argument.array()));
        }
        let mut choices = reserve(selection.len(), &mut work)?;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            work.step()?;
            let mut chosen: Option<(usize, usize)> = None;
            for (index, argument) in arguments.iter().enumerate() {
                work.step()?;
                let row = argument.value_row(ordinal, batch_row);
                let candidate = argument.array().as_ref();
                if candidate.data_type() == &DataType::Null || candidate.is_null(row) {
                    chosen = None;
                    break;
                }
                let choose = if let Some((current_index, current_row)) = chosen {
                    let current: &EvaluatedArgument<'_> = &arguments[current_index];
                    order.compare(
                        (candidate, row),
                        (current.array().as_ref(), current_row),
                        direction,
                        &mut work,
                    )? == preferred
                } else {
                    true
                };
                if choose {
                    chosen = Some((index, row));
                }
            }
            choices.push(chosen);
        }
        work.flush()?;
        let values = assemble_values(
            AssemblyPlan::Indexed {
                result_type: ty,
                sources: &sources,
                choices: &choices,
            },
            control,
        )
        .map_err(crate::control_values::AssemblyFailure::into_kernel)?;
        SelectedValues::try_new_observed(selection, ty, values, Box::default(), || work.step())
    })();
    work.finish_result(result)
}

/// The caller owns eager child evaluation. This boundary receives values only
/// and preserves domain causes at the bounded owner boundary.
pub fn evaluate_legacy(
    operation: ExtremaOperation,
    arrays: &[ArrayRef],
    rows: usize,
    target: Option<&DataType>,
) -> Result<ArrayRef, KernelFailure> {
    let arguments = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    evaluate_values(
        operation,
        &arguments,
        Selection::all(rows),
        target,
        true,
        &mut work,
        None,
    )
}
fn evaluate_values(
    operation: ExtremaOperation,
    args: &[EvaluatedArgument<'_>],
    selection: Selection<'_>,
    target: Option<&DataType>,
    legacy: bool,
    work: &mut EvaluationCheckpoints<'_>,
    boundary: ErrorBoundary<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let greatest = matches!(operation, ExtremaOperation::Greatest);
    let mut temporal_arguments = !args.is_empty();
    for arg in args {
        work.step()?;
        if !temporal(arg.array().data_type()) {
            temporal_arguments = false;
            break;
        }
    }
    if temporal_arguments {
        let mut values_per_arg = reserve(args.len(), work)?;
        for arg in args {
            work.step()?;
            let array = arg.array();
            let count = if legacy { array.len() } else { selection.len() };
            let mut decoded = reserve(count, work)?;
            if matches!(array.data_type(), DataType::Null) {
                for _ in 0..count {
                    work.step()?;
                    decoded.push(None);
                }
            } else {
                let reader = super::calendar_input::DateInput::raw(array.as_ref())
                    .map_err(|e| raw_error(e.legacy_message(), boundary))?;
                for ordinal in 0..count {
                    work.step()?;
                    let row = if legacy {
                        ordinal
                    } else {
                        arg.value_row(ordinal, selection.row(ordinal).expect("selection ordinal"))
                    };
                    decoded.push(if array.is_null(row) {
                        None
                    } else {
                        reader.read(row, work)?
                    });
                }
            }
            values_per_arg.push(decoded);
        }
        let mut values = reserve(selection.len(), work)?;
        for ordinal in 0..selection.len() {
            work.step()?;
            let row = selection.row(ordinal).expect("selection ordinal");
            let mut acc: Option<NaiveDateTime> = None;
            for decoded in &values_per_arg {
                work.step()?;
                let value = if legacy {
                    datetime_value_at(decoded, row, selection.batch_rows())
                } else {
                    decoded.get(ordinal).copied().flatten()
                };
                match (acc, value) {
                    (Some(a), Some(b)) => {
                        acc = Some(if greatest {
                            if a >= b { a } else { b }
                        } else if a <= b {
                            a
                        } else {
                            b
                        })
                    }
                    (None, Some(b)) => acc = Some(b),
                    (_, None) => {
                        acc = None;
                        break;
                    }
                }
            }
            values.push(acc.map(crate::datetime_value::naive_to_timestamp_micros));
        }
        work.flush()?;
        let ts = Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef;
        work.flush()?;
        let fallback = DataType::Timestamp(TimeUnit::Microsecond, None);
        let output_type = target.unwrap_or(&fallback);
        if matches!(output_type, DataType::Utf8) {
            let ts = ts
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    raw_error(
                        "failed to downcast datetime extrema result".to_string(),
                        boundary,
                    )
                })?;
            let mut out = reserve(ts.len(), work)?;
            for row in 0..ts.len() {
                work.step()?;
                let value = if ts.is_null(row) {
                    None
                } else {
                    work.flush()?;
                    let value = crate::datetime_value::timestamp_to_naive(
                        &TimeUnit::Microsecond,
                        ts.value(row),
                    )
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string());
                    work.flush()?;
                    value
                };
                out.push(value);
            }
            work.flush()?;
            let out = Arc::new(StringArray::from(out)) as ArrayRef;
            work.flush()?;
            return Ok(out);
        }
        return cast(ts, Some(output_type), work);
    }
    let mut views = reserve(args.len(), work)?;
    for arg in args {
        work.step()?;
        views.push(NumericArrayView::new(arg.array()).map_err(KernelFailure::from_domain)?);
    }
    let mut values = reserve(selection.len(), work)?;
    for ordinal in 0..selection.len() {
        work.step()?;
        let row = selection.row(ordinal).expect("selection ordinal");
        let mut acc: Option<f64> = None;
        for (arg, view) in args.iter().zip(&views) {
            work.step()?;
            work.flush()?;
            let v = if legacy {
                value_at_f64(view, row, selection.batch_rows())
            } else {
                view.value_f64(arg.value_row(ordinal, row))
            }
            .filter(|v| v.is_finite());
            work.flush()?;
            match (acc, v) {
                (Some(a), Some(b)) => acc = Some(if greatest { a.max(b) } else { a.min(b) }),
                (None, Some(b)) => acc = Some(b),
                (_, None) => {
                    acc = None;
                    break;
                }
            }
        }
        values.push(acc);
    }
    work.flush()?;
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    work.flush()?;
    cast(out, target, work)
}
fn cast(
    out: ArrayRef,
    target: Option<&DataType>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    work.flush()?;
    let out = cast_output_observed(out, target, &mut |event| match event {
        MathNumericObservation::Step => work.step(),
        MathNumericObservation::OpaqueBoundary => work.flush(),
    })
    .map_err(MathNumericError::into_kernel)?;
    work.flush()?;
    Ok(out)
}
struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn wait(&self, _duration: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("extrema evaluation must not request a wait")
    }

    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "scalar_extrema_tests.rs"]
mod tests;
