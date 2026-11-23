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
//! The original greatest/least values shared by evaluated legacy and selected calls.
use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallContract, ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    math_numeric::{
        MathNumericError, MathNumericObservation, NumericArrayView, cast_output,
        cast_output_observed, value_at_f64,
    },
};
use arrow_array::{
    Array, ArrayRef, Date32Array, Float64Array, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::NaiveDateTime;
use novarocks_type_contract::{TemporalType, ValueLogicalType};
use novarocks_value_arrays::decode::PreparedTemporalArray;
use std::{alloc::Layout, sync::Arc};

use super::temporal_output::TemporalOutputBuilder;
use crate::temporal_cast::Type;
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
fn numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Null
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
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let arguments = &contract.selected().argument_types;
    if arguments.is_empty()
        || !contract.result_type().nullable()
        || contract.result_type().logical_type() != ValueLogicalType::Physical
    {
        return Err(invalid(
            "extrema requires a nonempty exact nullable Physical result profile",
        ));
    }
    let result = contract.result_type();
    if result.value_type().temporal().is_some() || *result.data_type() == DataType::Date32 {
        for argument in arguments.iter() {
            observe()?;
            let FunctionArgumentType::Value(value) = argument else {
                return Err(invalid("extrema requires value arguments"));
            };
            if !value.same_value_domain(result) || value.data_type() != result.data_type() {
                return Err(invalid(
                    "extrema argument is outside its selected temporal domain",
                ));
            }
        }
        return Ok(());
    }
    let mut is_temporal = true;
    let mut is_numeric = true;
    for argument in arguments.iter() {
        observe()?;
        let FunctionArgumentType::Value(value) = argument else {
            return Err(invalid("extrema requires value arguments"));
        };
        if value.logical_type() != ValueLogicalType::Physical
            && !(value.logical_type() == ValueLogicalType::Json
                && value.data_type() == &DataType::Utf8)
        {
            return Err(invalid(
                "extrema has no value-preserving implementation for this logical domain",
            ));
        }
        is_temporal &= temporal(value.data_type());
        is_numeric &= numeric(value.data_type());
    }
    let target = contract.result_type().data_type();
    if *target == DataType::Null {
        return Err(invalid(
            "greatest/least has no v1 value result for an exact Null target",
        ));
    }
    let target_admitted = if is_temporal {
        matches!(
            target,
            DataType::Utf8 | DataType::Date32 | DataType::Timestamp(_, _) | DataType::Null
        )
    } else if is_numeric {
        numeric(target) || matches!(target, DataType::Decimal256(_, _))
    } else {
        false
    };
    if !target_admitted {
        return Err(invalid(
            "extrema source/result carriers are not an installed v1 calculation profile",
        ));
    }
    // Target zone adjustment belongs to the static Arrow conversion, not a session clock.
    // The owner validates library support before admitting that exact target.
    Ok(())
}
/// Check a temporal target's exact static conversion, including timezone support.
pub(super) fn validate_target(contract: &ScalarCallContract) -> Result<(), KernelFailure> {
    if matches!(
        contract.result_type().data_type(),
        DataType::Timestamp(_, Some(_))
    ) {
        let empty =
            Arc::new(TimestampMicrosecondArray::from(Vec::<Option<i64>>::new())) as ArrayRef;
        cast_output(empty, Some(contract.result_type().data_type()))
            .map_err(MathNumericError::into_kernel)?;
    }
    Ok(())
}
pub(super) fn evaluate<'a>(
    operation: ExtremaOperation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        validate_profile(input.contract(), || work.step())?;
        let out = if let Some(ty) = input.contract().result_type().value_type().temporal() {
            evaluate_temporal(operation, &input, ty, &mut work)?
        } else if *input.contract().result_type().data_type() == DataType::Date32 {
            evaluate_date(operation, &input, &mut work)?
        } else {
            evaluate_values(
                operation,
                input.arguments(),
                input.selection(),
                Some(input.contract().result_type().data_type()),
                false,
                &mut work,
                None,
            )?
        };
        SelectedValues::try_new_observed(
            input.selection(),
            input.contract().result_type().data_type(),
            out,
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

fn prefer<T: Ord>(operation: ExtremaOperation, candidate: T, current: T) -> bool {
    match operation {
        ExtremaOperation::Greatest => candidate > current,
        ExtremaOperation::Least => candidate < current,
    }
}

fn evaluate_date(
    operation: ExtremaOperation,
    input: &ScalarCallInput<'_, '_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let mut sources = reserve(input.arguments().len(), work)?;
    for argument in input.arguments() {
        let array = argument
            .array()
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| invalid("extrema DATE argument violates its frozen carrier"))?;
        sources.push((*argument, array));
        work.step()?;
    }
    let selection = input.selection();
    work.flush()?;
    let mut output = TemporalOutputBuilder::new(Type::Date, selection.len())?;
    work.flush()?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let mut chosen = None;
        for &(argument, array) in &sources {
            work.step()?;
            let row = argument.value_row(ordinal, row);
            if array.is_null(row) {
                chosen = None;
                break;
            }
            let candidate = array.value(row);
            if chosen.is_none_or(|current| prefer(operation, candidate, current)) {
                chosen = Some(candidate);
            }
        }
        match chosen {
            Some(day) => output.date(day)?,
            None => output.null(),
        }
    }
    work.flush()?;
    Ok(output.finish())
}

fn evaluate_temporal(
    operation: ExtremaOperation,
    input: &ScalarCallInput<'_, '_>,
    ty: TemporalType,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let arguments = &input.contract().selected().argument_types;
    let mut sources = reserve(arguments.len(), work)?;
    for (argument, binding) in input.arguments().iter().zip(arguments.iter()) {
        let FunctionArgumentType::Value(binding) = binding else {
            return Err(invalid("extrema requires value arguments"));
        };
        let source = PreparedTemporalArray::bind(ty, argument.array().as_ref(), binding.nullable())
            .map_err(|_| invalid("extrema temporal argument violates its frozen carrier"))?;
        sources.push((*argument, source));
        work.step()?;
    }
    let selection = input.selection();
    work.flush()?;
    let mut output = TemporalOutputBuilder::new(Type::Temporal(ty), selection.len())?;
    work.flush()?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let mut chosen = None;
        for (argument, source) in &sources {
            work.step()?;
            let Some(value) = source
                .value(argument.value_row(ordinal, row))
                .map_err(|_| invalid("extrema has an invalid frozen temporal operand"))?
            else {
                chosen = None;
                break;
            };
            let key = value
                .relation_key(ty)
                .map_err(|_| invalid("extrema temporal value violates its declared ordering"))?;
            if chosen.is_none_or(|(current, _)| prefer(operation, key, current)) {
                chosen = Some((key, value));
            }
        }
        match chosen {
            Some((_, value)) => output.temporal(ty, value)?,
            None => output.null(),
        }
    }
    work.flush()?;
    Ok(output.finish())
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
