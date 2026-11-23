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

//! Selected split_part spans preserve the original directional search algorithm.
//! SQL NULL inputs produce a non-null empty string; no payload is read for NULL.
//! Layout checks cover requested representability, not formal host memory grants.

use super::string_substring::StringIntegerArray;
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    let offsets = Layout::array::<i32>(
        rows.checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?,
    )
    .map_err(|_| KernelFailure::ResourceExhausted)?
    .size();
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

struct SelectedProjection<'work, 'control> {
    work: &'work mut EvaluationCheckpoints<'control>,
}
impl<'a> crate::string_split_part_core::SplitProjection<'a> for SelectedProjection<'_, '_> {
    type Error = KernelFailure;
    type Output = Option<&'a str>;
    fn empty(&mut self) -> Result<Option<&'a str>, KernelFailure> {
        Ok(None)
    }
    fn text(&mut self, text: &'a str) -> Result<Option<&'a str>, KernelFailure> {
        Ok(Some(text))
    }
    fn character(&mut self, tail: &'a str, ch: char) -> Result<Option<&'a str>, KernelFailure> {
        Ok(Some(&tail[..ch.len_utf8()]))
    }
    fn observe_character(&mut self, ch: char) -> Result<(), KernelFailure> {
        for _ in 0..ch.len_utf8() {
            self.work.step()?;
        }
        Ok(())
    }
    fn step(&mut self) -> Result<(), KernelFailure> {
        self.work.step()
    }
    fn flush(&mut self) -> Result<(), KernelFailure> {
        self.work.flush()
    }
}
fn split_part_span<'a>(
    text: &'a str,
    delimiter: &str,
    index: i64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<&'a str>, KernelFailure> {
    crate::string_split_part_core::project(text, delimiter, index, &mut SelectedProjection { work })
}
pub(super) fn evaluate_string_split_part<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 3 || arguments.len() != 3 {
            return Err(invalid(
                "split_part requires its exact three checked arguments",
            ));
        }
        for (index, ty) in types.iter().enumerate() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("split_part requires value arguments"));
            };
            let expected = if index < 2 {
                ty.data_type() == &DataType::Utf8
            } else {
                matches!(ty.data_type(), DataType::Int32 | DataType::Int64)
            };
            if ty.logical_type() != ValueLogicalType::Physical || !expected {
                return Err(invalid(
                    "split_part differs from its exact installed argument profile",
                ));
            }
            work.step()?;
        }
        let target = input.contract().result_type();
        if target.logical_type() != ValueLogicalType::Physical
            || target.data_type() != &DataType::Utf8
            || !target.nullable()
        {
            return Err(invalid(
                "split_part differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("split_part selected source is not Utf8"))?;
        let delimiters = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("split_part selected delimiter is not Utf8"))?;
        let indices = StringIntegerArray::read(arguments[2].array().as_ref())?;
        let trino = matches!(indices, StringIntegerArray::Bigint(_));
        let row_span = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<Result<Option<&str>, &'static str>, KernelFailure> {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal(
                        "split_part selected argument row is out of bounds",
                    ));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("split_part requires value arguments"));
                    };
                    if !ty.nullable() {
                        return Err(internal(
                            "split_part non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                return Ok(Ok(if trino { None } else { Some("") }));
            }
            let index = indices.value(rows[2]);
            if trino && index <= 0 {
                return Ok(Err("Index must be greater than zero"));
            }
            let span = split_part_span(
                strings.value(rows[0]),
                delimiters.value(rows[1]),
                index,
                work,
            )?;
            Ok(Ok(if trino {
                span
            } else {
                Some(span.unwrap_or(""))
            }))
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Ok(Some(span)) = row_span(ordinal, batch_row, &mut work)? {
                total = total
                    .checked_add(span.len())
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
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
        offsets.push(0i32);
        let mut validity = trino.then(|| BooleanBufferBuilder::new(selection.len()));
        let mut has_null = false;
        let mut errors = Vec::new();
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let span = match row_span(ordinal, batch_row, &mut work)? {
                Ok(span) => span,
                Err(message) => {
                    errors
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    errors.push(crate::RowDataError::with_reason(
                        ordinal,
                        crate::DataFailureReason::Domain(
                            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                        ),
                        message,
                    ));
                    None
                }
            };
            if let Some(validity) = &mut validity {
                validity.append(span.is_some());
                has_null |= span.is_none();
            }
            if let Some(span) = span {
                if span.len() > total - bytes.len() {
                    return Err(internal("split_part exceeded its measured output extent"));
                }
                for byte in span.bytes() {
                    bytes.push(byte);
                    work.step()?;
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "split_part differs from its measured output extent",
            ));
        }
        work.flush()?;
        let array = Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            validity
                .filter(|_| has_null)
                .map(|mut validity| NullBuffer::new(validity.finish())),
        )) as ArrayRef;
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
#[path = "string_split_part_tests.rs"]
mod tests;
