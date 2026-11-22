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

//! Selected registered UTF-8 concatenation over borrowed input spans.
//! Requested-layout representability is checked before output allocation.
//! Arrow construction and allocator internals are opaque, not a host grant.

use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::{FunctionIntrinsicRowError, ValueLogicalType};
use std::{alloc::Layout, sync::Arc};

use super::string_concat_core::{ConcatLength, concat_length_observed, emit_concat_observed};

fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    let offsets = rows
        .checked_add(1)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let offsets = Layout::array::<i32>(offsets)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let bitmap = rows
        .checked_add(63)
        .map(|bits| bits / 64)
        .and_then(|words| words.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|value| value.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

fn value_bytes<'a>(argument: EvaluatedArgument<'a>, row: usize) -> Result<&'a [u8], KernelFailure> {
    let array = argument.array().as_any();
    if let Some(array) = array.downcast_ref::<StringArray>() {
        Ok(array.value(row).as_bytes())
    } else if let Some(array) = array.downcast_ref::<BinaryArray>() {
        Ok(array.value(row))
    } else {
        Err(internal("concat selected carrier is not Utf8 or Binary"))
    }
}

fn row_length(
    input: &ScalarCallInput<'_, '_>,
    ordinal: usize,
    batch_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ConcatLength, KernelFailure> {
    let mut context = (input, ordinal, batch_row, work);
    concat_length_observed(input.arguments().len(), &mut context, |context, index| {
        let argument = context.0.arguments()[index];
        let FunctionArgumentType::Value(source) =
            &context.0.contract().selected().argument_types[index]
        else {
            return Err(invalid("concat requires only checked value arguments"));
        };
        let values = argument.array();
        let row = argument.value_row(context.1, context.2);
        context.3.step()?;
        if row >= values.len() {
            return Err(internal("concat selected row is outside its carrier"));
        }
        if values.is_null(row) {
            if !source.nullable() {
                return Err(internal(
                    "concat non-null source contains selected SQL NULL",
                ));
            }
            Ok(None)
        } else {
            value_bytes(argument, row).map(Some)
        }
    })
}

pub(super) fn evaluate_string_concat<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let selected = input.contract().selected();
        let target = input.contract().result_type();
        let trino = input.contract().effects().own_row_error == FunctionIntrinsicRowError::MayRaise;
        let min_arity = if trino { 2 } else { 1 };
        if input.arguments().len() < min_arity
            || selected.argument_types.len() != input.arguments().len()
            || target.logical_type() != ValueLogicalType::Physical
            || !matches!(target.data_type(), DataType::Utf8 | DataType::Binary)
            || !target.nullable()
        {
            return Err(invalid(
                "concat differs from its installed variadic Utf8 profile",
            ));
        }
        for (argument, ty) in input.arguments().iter().zip(selected.argument_types.iter()) {
            let FunctionArgumentType::Value(source) = ty else {
                return Err(invalid("concat requires only checked value arguments"));
            };
            if source.logical_type() != ValueLogicalType::Physical
                || source.data_type() != target.data_type()
            {
                return Err(invalid(
                    "concat selected source is outside its installed profile",
                ));
            }
            if argument.array().data_type() != source.data_type() {
                return Err(internal(
                    "concat selected carrier differs from its checked type",
                ));
            }
            work.step()?;
        }
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut output_bytes = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let ConcatLength::Value(length) = row_length(&input, ordinal, batch_row, &mut work)?
            {
                output_bytes = output_bytes
                    .checked_add(length)
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
            work.step()?;
        }
        output_capacity(selection.len(), output_bytes)?;
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(output_bytes)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(selection.len() + 1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0i32);
        let mut has_null = false;
        let mut errors = Vec::new();
        for (ordinal, batch_row) in selection.iter().enumerate() {
            match row_length(&input, ordinal, batch_row, &mut work)? {
                length @ (ConcatLength::Null | ConcatLength::TooLong) => {
                    if trino && let Some(error) = length.data_error(ordinal) {
                        errors
                            .try_reserve(1)
                            .map_err(|_| KernelFailure::ResourceExhausted)?;
                        errors.push(error);
                    }
                    has_null = true;
                    validity.append(false);
                }
                ConcatLength::Value(length) => {
                    if length > output_bytes - bytes.len() {
                        return Err(internal("concat exceeded its measured output extent"));
                    }
                    let mut context = (&input, ordinal, batch_row, &mut bytes, &mut work);
                    emit_concat_observed(
                        input.arguments().len(),
                        &mut context,
                        |context, index| {
                            let argument = context.0.arguments()[index];
                            value_bytes(argument, argument.value_row(context.1, context.2))
                        },
                        |context, text| {
                            for byte in text {
                                context.3.push(*byte);
                                context.4.step()?;
                            }
                            Ok(())
                        },
                        |context, _| context.4.step(),
                    )?;
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != output_bytes {
            return Err(internal("concat differs from its measured output extent"));
        }
        work.flush()?;
        let offsets = OffsetBuffer::new(offsets.into());
        let bytes = Buffer::from(bytes);
        let nulls = has_null.then(|| NullBuffer::new(validity.finish()));
        let array: ArrayRef = if target.data_type() == &DataType::Binary {
            Arc::new(BinaryArray::new(offsets, bytes, nulls))
        } else {
            Arc::new(StringArray::new(offsets, bytes, nulls))
        };
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            target.data_type(),
            array,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    if result.as_ref().is_err_and(KernelFailure::is_interruption) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
#[path = "string_concat_tests.rs"]
mod tests;
