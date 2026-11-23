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

//! Selected shifts use the declared signed width and an INTEGER distance.
//! Arrow builder allocation still requires formal host memory admission.

use arrow_array::{
    Array, ArrayRef, Int32Array, PrimitiveArray,
    types::{ArrowPrimitiveType, Int8Type, Int16Type, Int32Type, Int64Type},
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;

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
        || count.data_type() != &DataType::Int32
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
        .downcast_ref::<Int32Array>()
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
    counts: &'a Int32Array,
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
                .map(|(left, right)| {
                    (
                        values.value(left).into(),
                        i64::from(arguments.counts.value(right)),
                    )
                }))
        },
        |value, count| {
            apply_shift(
                operation,
                value,
                count as i32,
                (std::mem::size_of::<T::Native>() * 8) as u32,
            )
        },
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

fn apply_shift(operation: ShiftOp, value: i64, count: i32, width: u32) -> i64 {
    if count >= 64 {
        return if operation == ShiftOp::Right {
            value >> 63
        } else {
            0
        };
    }
    let count = count as u32;
    let padding = 64 - width;
    match operation {
        ShiftOp::Left => {
            // Sign-extend after narrowing the shifted bits to the source width.
            value.wrapping_shl(count).wrapping_shl(padding) >> padding
        }
        ShiftOp::Right => value.wrapping_shr(count),
        ShiftOp::RightLogical if width < 64 && count != 0 => {
            let mask = u64::MAX >> padding;
            ((value as u64 & mask).wrapping_shr(count) as i64).wrapping_shl(padding) >> padding
        }
        ShiftOp::RightLogical => (value as u64).wrapping_shr(count) as i64,
    }
}

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
