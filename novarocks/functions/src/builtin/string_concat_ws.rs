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

//! Selected CONCAT_WS over exact borrowed Utf8 argument addresses.
//! Two passes measure and directly write the original capped row result.
//! Output Layout checks and opaque Arrow construction are not a host grant.

use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

use super::string_concat_core::{concat_ws_length_observed, emit_concat_ws_observed};

fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    let offsets = Layout::array::<i32>(
        rows.checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?,
    )
    .map_err(|_| KernelFailure::ResourceExhausted)?
    .size();
    let bitmap = rows
        .checked_add(63)
        .map(|n| n / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

fn carrier<'a>(argument: EvaluatedArgument<'a>) -> Result<&'a StringArray, KernelFailure> {
    argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| internal("concat_ws selected carrier is not Utf8"))
}

// This header/address check never reads a NULL payload. It remains required
// for every argument even when separator NULL or the row cap masks its value.
fn selected_text<'a>(
    input: &ScalarCallInput<'_, 'a>,
    index: usize,
    ordinal: usize,
    batch_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<&'a str>, KernelFailure> {
    let argument = input.arguments()[index];
    let FunctionArgumentType::Value(source) = &input.contract().selected().argument_types[index]
    else {
        return Err(invalid("concat_ws requires only checked value arguments"));
    };
    let values = carrier(argument)?;
    let row = argument.value_row(ordinal, batch_row);
    work.step()?;
    if row >= values.len() {
        return Err(internal("concat_ws selected row is outside its carrier"));
    }
    let is_null = values.is_null(row);
    work.step()?;
    if is_null {
        if !source.nullable {
            return Err(internal(
                "concat_ws non-null source contains selected SQL NULL",
            ));
        }
        Ok(None)
    } else {
        Ok(Some(values.value(row)))
    }
}

fn row_length(
    input: &ScalarCallInput<'_, '_>,
    ordinal: usize,
    batch_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    let mut context = (input, ordinal, batch_row, work);
    concat_ws_length_observed(
        input.arguments().len(),
        &mut context,
        |context, index| selected_text(context.0, index, context.1, context.2, context.3),
        |context, index| {
            selected_text(context.0, index, context.1, context.2, context.3)?;
            Ok(())
        },
        |context, _| context.3.step(),
    )
}

fn copy_span(
    text: &str,
    bytes: &mut Vec<u8>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    for byte in text.bytes() {
        bytes.push(byte);
        work.step()?;
    }
    Ok(())
}

pub(super) fn evaluate_string_concat_ws<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let selected = input.contract().selected();
        let target = input.contract().result_type();
        if input.arguments().len() < 2
            || selected.argument_types.len() != input.arguments().len()
            || target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "concat_ws differs from its installed variadic Utf8 profile",
            ));
        }
        for (argument, ty) in input.arguments().iter().zip(selected.argument_types.iter()) {
            let FunctionArgumentType::Value(source) = ty else {
                return Err(invalid("concat_ws requires only checked value arguments"));
            };
            let exact = source.logical_type == ValueLogicalType::Physical
                && source.data_type == DataType::Utf8;
            work.step()?;
            if !exact {
                return Err(invalid(
                    "concat_ws selected source is outside its installed profile",
                ));
            }
            let checked = carrier(*argument);
            work.step()?;
            checked?;
        }
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, row) in selection.iter().enumerate() {
            if let Some(length) = row_length(&input, ordinal, row, &mut work)? {
                total = total
                    .checked_add(length)
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
            work.step()?;
        }
        output_capacity(selection.len(), total)?;
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total)
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
        for (ordinal, row) in selection.iter().enumerate() {
            match row_length(&input, ordinal, row, &mut work)? {
                None => {
                    has_null = true;
                    validity.append(false);
                }
                Some(length) => {
                    let available = total.checked_sub(bytes.len()).ok_or_else(|| {
                        internal("concat_ws destination exceeds its measured extent")
                    })?;
                    if length > available {
                        return Err(internal("concat_ws exceeded its measured output extent"));
                    }
                    let start = bytes.len();
                    let mut context = (&input, ordinal, row, &mut bytes, &mut work);
                    emit_concat_ws_observed(
                        input.arguments().len(),
                        &mut context,
                        |context| {
                            selected_text(context.0, 0, context.1, context.2, context.4)?
                                .ok_or_else(|| {
                                    internal("concat_ws changed its measured separator NULL")
                                })
                        },
                        |context, index| {
                            selected_text(context.0, index, context.1, context.2, context.4)
                        },
                        |context, text| copy_span(text, context.3, context.4),
                        |context, _| context.4.step(),
                    )?;
                    if bytes.len() - start != length {
                        return Err(internal("concat_ws changed its measured row extent"));
                    }
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "concat_ws differs from its measured output extent",
            ));
        }
        work.flush()?;
        let array = Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            has_null.then(|| NullBuffer::new(validity.finish())),
        )) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target.data_type,
            array,
            Box::default(),
            || work.step(),
        )
    })();
    if matches!(
        &result,
        Err(KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
#[path = "string_concat_ws_tests.rs"]
mod tests;
