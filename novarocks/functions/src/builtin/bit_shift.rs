// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Selected shifts preserve the original widened arithmetic and safe narrowing.
//! Arrow builder allocation still requires formal host memory admission.

#[cfg(test)]
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, PrimitiveArray,
    types::{ArrowPrimitiveType, Int8Type, Int16Type, Int32Type, Int64Type},
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

#[cfg(test)]
use arrow_array::builder::FixedSizeBinaryBuilder;

use crate::{
    EvaluatedArgument, FunctionArgumentType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};

pub(super) use crate::bit_numeric::ShiftOp;

pub(super) fn evaluate_shift<'a>(
    operation: ShiftOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [
        FunctionArgumentType::Value(source),
        FunctionArgumentType::Value(count),
    ] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid("shift requires two exact value arguments"));
    };
    let [left, right] = input.arguments() else {
        return Err(invalid("shift requires two evaluated arguments"));
    };
    let target = input.contract().result_type();
    if count.logical_type() != ValueLogicalType::Physical
        || count.data_type() != &DataType::Int64
        || target.logical_type() != source.logical_type()
        || target.data_type() != source.data_type()
        || !target.nullable()
    {
        return Err(invalid(
            "shift types differ from their exact installed profile",
        ));
    }
    if left.array().data_type() != source.data_type()
        || right.array().data_type() != count.data_type()
    {
        return Err(internal("shift carrier differs from its checked argument"));
    }
    let counts = right
        .array()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| internal("shift count carrier cannot be downcast"))?;
    let arguments = ShiftArguments {
        left: *left,
        right: *right,
        source,
        count,
        counts,
    };
    let values = match (&source.logical_type(), source.data_type()) {
        (ValueLogicalType::Physical, DataType::Int8) => {
            primitive::<Int8Type>(operation, &input, arguments, control)?
        }
        (ValueLogicalType::Physical, DataType::Int16) => {
            primitive::<Int16Type>(operation, &input, arguments, control)?
        }
        (ValueLogicalType::Physical, DataType::Int32) => {
            primitive::<Int32Type>(operation, &input, arguments, control)?
        }
        (ValueLogicalType::Physical, DataType::Int64) => {
            primitive::<Int64Type>(operation, &input, arguments, control)?
        }
        (ValueLogicalType::LargeInt, DataType::FixedSizeBinary(16)) => {
            largeint(operation, &input, arguments, control)?
        }
        _ => {
            return Err(invalid(
                "shift source is not an exact installed integer domain",
            ));
        }
    };
    SelectedValues::try_new(
        input.selection(),
        target.data_type(),
        values,
        Box::default(),
    )
    .map_err(|_| internal("shift compact output violates its selected contract"))
}

/// Each side owns its mapping and SQL NULL contract. Check both addresses and
/// both selected NULLs before strict propagation, so NULL cannot hide a broken
/// carrier or a non-nullable argument's selected SQL NULL.
#[derive(Clone, Copy)]
struct ShiftArguments<'a, 'b> {
    left: EvaluatedArgument<'a>,
    right: EvaluatedArgument<'a>,
    source: &'b FunctionValueType,
    count: &'b FunctionValueType,
    counts: &'a Int64Array,
}
impl ShiftArguments<'_, '_> {
    fn selected_pair(
        self,
        ordinal: usize,
        batch_row: usize,
    ) -> Result<Option<(usize, usize)>, KernelFailure> {
        let left_row = self.left.value_row(ordinal, batch_row);
        let right_row = self.right.value_row(ordinal, batch_row);
        if left_row >= self.left.array().len() || right_row >= self.right.array().len() {
            return Err(internal(
                "shift selected row is outside its checked carrier",
            ));
        }
        let left_null = self.left.array().is_null(left_row);
        let right_null = self.right.array().is_null(right_row);
        if (left_null && !self.source.nullable()) || (right_null && !self.count.nullable()) {
            return Err(internal(
                "shift non-null argument contains selected SQL NULL",
            ));
        }
        if left_null || right_null {
            Ok(None)
        } else {
            Ok(Some((left_row, right_row)))
        }
    }
}

fn primitive<T: ArrowPrimitiveType>(
    operation: ShiftOp,
    input: &ScalarCallInput<'_, '_>,
    arguments: ShiftArguments<'_, '_>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure>
where
    T::Native: Into<i64> + TryFrom<i64>,
{
    let values = arguments
        .left
        .array()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| internal("shift selected primitive carrier cannot be downcast"))?;
    let selection = input.selection();
    output_capacity(selection.len(), std::mem::size_of::<T::Native>())?;
    // The original loop allocates a BIGINT intermediate before Arrow safe
    // narrowing. Representability is checked for both real carriers.
    output_capacity(selection.len(), std::mem::size_of::<Option<i64>>())?;
    let mut work = EvaluationCheckpoints::new(control);
    let mut observe = |event| match event {
        crate::bit_array::BitArrayObservation::Step => work.step(),
        crate::bit_array::BitArrayObservation::OpaqueBoundary => work.flush(),
    };
    let values = crate::bit_array::shift_values_observed(
        selection,
        |ordinal, batch_row| {
            Ok(arguments
                .selected_pair(ordinal, batch_row)?
                .map(|(left_row, right_row)| {
                    (
                        values.value(left_row).into(),
                        arguments.counts.value(right_row),
                    )
                }))
        },
        |value, count| operation.apply_i64(value, i64::from(count)),
        &mut observe,
    )?;
    let values = crate::bit_array::finish_i64_observed(
        values,
        Some(input.contract().result_type().data_type()),
        &mut observe,
    )?
    .map_err(crate::bit_array::BitArrayError::into_kernel)?;
    work.finish()?;
    Ok(values)
}

fn largeint(
    operation: ShiftOp,
    input: &ScalarCallInput<'_, '_>,
    arguments: ShiftArguments<'_, '_>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    let values = arguments
        .left
        .array()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| internal("shift selected LargeInt carrier cannot be downcast"))?;
    if values.value_length() != 16 {
        return Err(internal(
            "shift LargeInt carrier has an incorrect byte width",
        ));
    }
    let selection = input.selection();
    output_capacity(selection.len(), 16)?;
    output_capacity(selection.len(), std::mem::size_of::<Option<i128>>())?;
    let mut work = EvaluationCheckpoints::new(control);
    let mut observe = |event| match event {
        crate::bit_array::BitArrayObservation::Step => work.step(),
        crate::bit_array::BitArrayObservation::OpaqueBoundary => work.flush(),
    };
    let values = crate::bit_array::shift_values_observed(
        selection,
        |ordinal, batch_row| {
            arguments
                .selected_pair(ordinal, batch_row)?
                .map(|(left_row, right_row)| {
                    crate::largeint::value_at(values, left_row)
                        .map(|value| (value, arguments.counts.value(right_row)))
                        .map_err(|_| internal("shift LargeInt value has an incorrect byte width"))
                })
                .transpose()
        },
        |value, count| operation.apply_i128(value, i64::from(count)),
        &mut observe,
    )?;
    let values = crate::bit_array::cast_largeint_output_observed(
        &values,
        Some(input.contract().result_type().data_type()),
        &mut observe,
    )?
    .map_err(|_| internal("shift LargeInt output has an incorrect byte width"))?;
    work.finish()?;
    Ok(values)
}

/// Allocation representability only; it does not authorize host memory.
fn output_capacity(rows: usize, width: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(width)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(values).map_err(|_| KernelFailure::ResourceExhausted)?;
    isize::try_from(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

#[cfg(test)]
mod tests;
