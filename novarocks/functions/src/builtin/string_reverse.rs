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

//! Selected original Unicode string reversal over borrowed UTF-8 spans.
//! Checks cover requested-layout representability. Arrow construction and
//! allocator internals remain opaque; this is not a formal host memory grant.

use super::string_measure::{fixed_char_padding, fixed_char_width};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringReverseOp {
    Reverse,
}

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

enum ReverseRow<'a> {
    Text {
        text: &'a str,
        leading_spaces: usize,
    },
    Binary(&'a [u8]),
}

impl ReverseRow<'_> {
    fn byte_count(&self) -> usize {
        match self {
            Self::Text {
                text,
                leading_spaces,
            } => text.len() + leading_spaces,
            Self::Binary(bytes) => bytes.len(),
        }
    }
}

fn fixed_reverse_span<'a>(
    text: &'a str,
    width: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ReverseRow<'a>, KernelFailure> {
    let mut chars = 0;
    let mut start = 0;
    for character in text.chars() {
        if character == ' ' && start == chars {
            start += 1;
        }
        chars += 1;
        work.step()?;
    }
    let padding = fixed_char_padding(width, chars)?;
    // Reversal moves storage padding to the front and the original leading
    // ASCII spaces to the trailing CHAR storage suffix, which is omitted.
    if start == chars {
        Ok(ReverseRow::Text {
            text: "",
            leading_spaces: 0,
        })
    } else {
        Ok(ReverseRow::Text {
            text: &text[start..],
            leading_spaces: padding,
        })
    }
}

pub(super) fn evaluate_string_reverse<'a>(
    _op: StringReverseOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let ([FunctionArgumentType::Value(source)], [argument]) = (
            input.contract().selected().argument_types.as_ref(),
            input.arguments(),
        ) else {
            return Err(invalid(
                "string reversal requires one checked value argument",
            ));
        };
        let target = input.contract().result_type();
        if source.logical_type() != ValueLogicalType::Physical
            || !matches!(source.data_type(), DataType::Utf8 | DataType::Binary)
            || target.logical_type() != ValueLogicalType::Physical
            || target.data_type() != source.data_type()
            || fixed_char_width(source) != fixed_char_width(target)
        {
            return Err(invalid(
                "string reversal differs from its exact installed profile",
            ));
        }
        let values = argument.array();
        let strings = values.as_any().downcast_ref::<StringArray>();
        let binaries = values.as_any().downcast_ref::<BinaryArray>();
        let binary = source.data_type() == &DataType::Binary;
        if (binary && binaries.is_none()) || (!binary && strings.is_none()) {
            return Err(internal(
                "reversal selected carrier differs from its checked type",
            ));
        }
        let width = fixed_char_width(source);
        let reverse_row = |row, work: &mut EvaluationCheckpoints<'_>| {
            if let Some(strings) = strings {
                let text = strings.value(row);
                if let Some(width) = width {
                    fixed_reverse_span(text, width, work)
                } else {
                    Ok(ReverseRow::Text {
                        text,
                        leading_spaces: 0,
                    })
                }
            } else if let Some(binaries) = binaries {
                Ok(ReverseRow::Binary(binaries.value(row)))
            } else {
                Err(internal("reversal has no selected carrier"))
            }
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut output_bytes = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            work.step()?;
            if row >= values.len() {
                return Err(internal(
                    "string reversal selected row is outside its carrier",
                ));
            }
            if values.is_null(row) {
                if !source.nullable() {
                    return Err(internal(
                        "string reversal non-null source contains selected SQL NULL",
                    ));
                }
                continue;
            }
            let selected = reverse_row(row, &mut work)?;
            output_bytes = output_bytes
                .checked_add(selected.byte_count())
                .ok_or(KernelFailure::ResourceExhausted)?;
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
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            if values.is_null(row) {
                has_null = true;
                validity.append(false);
            } else {
                let selected = reverse_row(row, &mut work)?;
                if selected.byte_count() > output_bytes - bytes.len() {
                    return Err(internal(
                        "string reversal exceeded its measured output extent",
                    ));
                }
                match selected {
                    ReverseRow::Text {
                        text,
                        leading_spaces,
                    } => {
                        for _ in 0..leading_spaces {
                            bytes.push(b' ');
                            work.step()?;
                        }
                        for character in text.chars().rev() {
                            let mut encoded = [0; 4];
                            for byte in character.encode_utf8(&mut encoded).bytes() {
                                bytes.push(byte);
                                work.step()?;
                            }
                        }
                    }
                    ReverseRow::Binary(value) => {
                        for byte in value.iter().rev() {
                            bytes.push(*byte);
                            work.step()?;
                        }
                    }
                }
                validity.append(true);
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != output_bytes {
            return Err(internal(
                "string reversal differs from its measured output extent",
            ));
        }
        work.flush()?;
        let offsets = OffsetBuffer::new(offsets.into());
        let bytes = Buffer::from(bytes);
        let nulls = has_null.then(|| NullBuffer::new(validity.finish()));
        let array: ArrayRef = if binary {
            Arc::new(BinaryArray::new(offsets, bytes, nulls))
        } else {
            Arc::new(StringArray::new(offsets, bytes, nulls))
        };
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            target.data_type(),
            array,
            Box::default(),
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
#[path = "string_reverse_tests.rs"]
mod tests;
