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

//! Exact selected bitwise computation for the installed integer domains.
//! Arrow builder allocation still requires formal host memory admission.

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, PrimitiveArray,
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

pub(super) use crate::bit_numeric::BitwiseOp;

pub(super) fn evaluate_bitwise<'a>(
    operation: BitwiseOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    if input.arguments().len() != operation.arity()
        || input.contract().selected().argument_types.len() != operation.arity()
    {
        return Err(invalid(
            "bitwise arguments differ from their exact operation arity",
        ));
    }
    let arguments = match (
        operation,
        input.contract().selected().argument_types.as_ref(),
        input.arguments(),
    ) {
        (BitwiseOp::Not, [FunctionArgumentType::Value(source)], [left]) => BitwiseArguments {
            left: *left,
            right: None,
            source,
            right_type: None,
        },
        (
            BitwiseOp::And | BitwiseOp::Or | BitwiseOp::Xor,
            [
                FunctionArgumentType::Value(source),
                FunctionArgumentType::Value(other),
            ],
            [left, right],
        ) if source.logical_type() == other.logical_type()
            && source.data_type() == other.data_type() =>
        {
            BitwiseArguments {
                left: *left,
                right: Some(*right),
                source,
                right_type: Some(other),
            }
        }
        _ => {
            return Err(invalid(
                "bitwise arguments differ from their exact operation profile",
            ));
        }
    };
    let source = arguments.source;
    let target = input.contract().result_type();
    if target.logical_type() != source.logical_type()
        || target.data_type() != source.data_type()
        || !target.nullable()
    {
        return Err(invalid(
            "bitwise result differs from its exact installed profile",
        ));
    }
    if arguments.left.array().data_type() != source.data_type()
        || arguments
            .right
            .is_some_and(|right| right.array().data_type() != source.data_type())
    {
        return Err(internal(
            "bitwise carrier differs from its checked argument",
        ));
    }
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
                "bitwise source is not an exact installed integer domain",
            ));
        }
    };
    SelectedValues::try_new(
        input.selection(),
        target.data_type(),
        values,
        Box::default(),
    )
    .map_err(|_| internal("bitwise compact output violates its selected contract"))
}

#[derive(Clone, Copy)]
struct BitwiseArguments<'a, 'b> {
    left: EvaluatedArgument<'a>,
    right: Option<EvaluatedArgument<'a>>,
    source: &'b FunctionValueType,
    right_type: Option<&'b FunctionValueType>,
}
impl BitwiseArguments<'_, '_> {
    /// Validate both mappings and NULL contracts before strict propagation.
    /// Unary NOT has no second argument to evaluate or silently ignore.
    fn selected_pair(
        self,
        ordinal: usize,
        batch_row: usize,
    ) -> Result<Option<(usize, Option<usize>)>, KernelFailure> {
        let left_row = self.left.value_row(ordinal, batch_row);
        if left_row >= self.left.array().len() {
            return Err(internal("bitwise left row is outside its checked carrier"));
        }
        let left_null = self.left.array().is_null(left_row);
        if left_null && !self.source.nullable() {
            return Err(internal("bitwise non-null left contains selected SQL NULL"));
        }
        let (right_row, right_null) = match (self.right, self.right_type) {
            (Some(right), Some(right_type)) => {
                let row = right.value_row(ordinal, batch_row);
                if row >= right.array().len() {
                    return Err(internal("bitwise right row is outside its checked carrier"));
                }
                let is_null = right.array().is_null(row);
                if is_null && !right_type.nullable() {
                    return Err(internal(
                        "bitwise non-null right contains selected SQL NULL",
                    ));
                }
                (Some(row), is_null)
            }
            (None, None) => (None, false),
            _ => return Err(internal("bitwise right argument lacks its exact type")),
        };
        if left_null || right_null {
            Ok(None)
        } else {
            Ok(Some((left_row, right_row)))
        }
    }
}

fn primitive<'a, T: ArrowPrimitiveType>(
    operation: BitwiseOp,
    input: &ScalarCallInput<'_, '_>,
    arguments: BitwiseArguments<'a, '_>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure>
where
    T::Native: Into<i64>,
{
    let downcast = |argument: EvaluatedArgument<'a>| {
        argument
            .array()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| internal("bitwise primitive carrier cannot be downcast"))
    };
    let left = downcast(arguments.left)?;
    let right = arguments.right.map(downcast).transpose()?;
    let selection = input.selection();
    output_capacity(selection.len(), std::mem::size_of::<T::Native>())?;
    output_capacity(selection.len(), std::mem::size_of::<Option<i64>>())?;
    let mut work = EvaluationCheckpoints::new(control);
    let mut observe = |event| match event {
        crate::bit_array::BitArrayObservation::Step => work.step(),
        crate::bit_array::BitArrayObservation::OpaqueBoundary => work.flush(),
    };
    let values = crate::bit_array::map_values_observed(
        selection,
        |ordinal, batch_row| {
            let Some((left_row, right_row)) = arguments.selected_pair(ordinal, batch_row)? else {
                return Ok(None);
            };
            let right = match (right, right_row) {
                (Some(values), Some(row)) => values.value(row).into(),
                (None, None) => 0,
                _ => return Err(internal("bitwise right primitive mapping is inconsistent")),
            };
            Ok(Some((left.value(left_row).into(), right)))
        },
        |(left, right)| operation.apply_i64(left, right),
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

fn largeint<'a>(
    operation: BitwiseOp,
    input: &ScalarCallInput<'_, '_>,
    arguments: BitwiseArguments<'a, '_>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    let downcast = |argument: EvaluatedArgument<'a>| {
        argument
            .array()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| internal("bitwise LargeInt carrier cannot be downcast"))
    };
    let left = downcast(arguments.left)?;
    let right = arguments.right.map(downcast).transpose()?;
    if left.value_length() != 16 || right.is_some_and(|array| array.value_length() != 16) {
        return Err(internal(
            "bitwise LargeInt carrier has an incorrect byte width",
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
    let values = crate::bit_array::map_values_observed(
        selection,
        |ordinal, batch_row| {
            let Some((left_row, right_row)) = arguments.selected_pair(ordinal, batch_row)? else {
                return Ok(None);
            };
            let right = match (right, right_row) {
                (Some(values), Some(row)) => largeint_value(values, row)?,
                (None, None) => 0,
                _ => return Err(internal("bitwise right LargeInt mapping is inconsistent")),
            };
            Ok(Some((largeint_value(left, left_row)?, right)))
        },
        |(left, right)| operation.apply_i128(left, right),
        &mut observe,
    )?;
    let values = crate::bit_array::cast_largeint_output_observed(
        &values,
        Some(input.contract().result_type().data_type()),
        &mut observe,
    )?
    .map_err(crate::bit_array::BitArrayError::into_kernel)?;
    work.finish()?;
    Ok(values)
}

fn largeint_value(values: &FixedSizeBinaryArray, row: usize) -> Result<i128, KernelFailure> {
    crate::largeint::i128_from_be_bytes(values.value(row))
        .map_err(|_| internal("bitwise LargeInt value has an incorrect byte width"))
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
