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

//! Selected Unicode scalar substring over the original borrowed UTF-8 values.
//! Layout checks cover requested representability, not formal host memory grants.

use super::string_measure::{fixed_char_padding, fixed_char_width};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::{CharacterKind, ValueKind, ValueLogicalType};
use std::{alloc::Layout, ops::Range, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringSubstringOp {
    Substring,
}

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
        .map(|bits| bits / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}

fn observe_char(
    character: char,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    for _ in 0..character.len_utf8() {
        work.step()?;
    }
    Ok(())
}

fn substring_span<'a>(
    text: &'a str,
    position: i64,
    length: Option<i64>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<&'a str, KernelFailure> {
    if position == 0 || length.is_some_and(|n| n <= 0) {
        return Ok("");
    }
    let start = if position > 0 {
        (position - 1) as usize
    } else {
        let mut count = 0usize;
        for character in text.chars() {
            count += 1;
            observe_char(character, work)?;
        }
        let distance = position.unsigned_abs() as usize;
        if distance > count {
            return Ok("");
        }
        count - distance
    };
    let end = length.map(|n| start.saturating_add(n as usize));
    let mut start_byte = None;
    for (ordinal, (byte, character)) in text.char_indices().enumerate() {
        observe_char(character, work)?;
        if ordinal == start {
            start_byte = Some(byte);
        }
        if end == Some(ordinal) {
            return Ok(&text[start_byte.unwrap_or(byte)..byte]);
        }
    }
    Ok(start_byte.map_or("", |byte| &text[byte..]))
}

fn binary_span(bytes: &[u8], position: i64, length: Option<i64>) -> &[u8] {
    value_range(bytes.len(), position, length).map_or(&[], |range| &bytes[range])
}

fn value_range(width: usize, position: i64, length: Option<i64>) -> Option<Range<usize>> {
    if position == 0 || length.is_some_and(|n| n <= 0) {
        return None;
    }
    let start = if position > 0 {
        usize::try_from(position - 1).ok()?
    } else {
        width.checked_sub(usize::try_from(position.unsigned_abs()).ok()?)?
    };
    let available = width.checked_sub(start)?;
    let count = length.map_or(available, |length| {
        usize::try_from(length).unwrap_or(available).min(available)
    });
    Some(start..start + count)
}

struct SubstringSpan<'a> {
    bytes: &'a [u8],
    spaces: usize,
}

impl<'a> SubstringSpan<'a> {
    fn borrowed(bytes: &'a [u8]) -> Self {
        Self { bytes, spaces: 0 }
    }

    fn byte_count(&self) -> usize {
        // CHAR spaces are bounded by the declared maximum CHAR width and a
        // borrowed Arrow value is bounded by i32 offsets.
        self.bytes.len() + self.spaces
    }
}

fn fixed_substring_span<'a>(
    text: &'a str,
    width: usize,
    position: i64,
    length: Option<i64>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<SubstringSpan<'a>, KernelFailure> {
    let Some(range) = value_range(width, position, length) else {
        return Ok(SubstringSpan::borrowed(&[]));
    };
    let mut stored = 0;
    let mut start_byte = text.len();
    let mut end_byte = text.len();
    for (ordinal, (byte, character)) in text.char_indices().enumerate() {
        if ordinal == range.start {
            start_byte = byte;
        }
        if ordinal == range.end {
            end_byte = byte;
        }
        stored += 1;
        observe_char(character, work)?;
    }
    fixed_char_padding(width, stored)?;
    Ok(SubstringSpan {
        bytes: &text.as_bytes()[start_byte..end_byte],
        spaces: range.end.saturating_sub(range.start.max(stored)),
    })
}

pub(super) enum StringIntegerArray<'a> {
    Integer(&'a Int32Array),
    Bigint(&'a Int64Array),
}

impl<'a> StringIntegerArray<'a> {
    pub(super) fn read(array: &'a dyn Array) -> Result<Self, KernelFailure> {
        let array = array.as_any();
        if let Some(values) = array.downcast_ref::<Int64Array>() {
            Ok(Self::Bigint(values))
        } else if let Some(values) = array.downcast_ref::<Int32Array>() {
            Ok(Self::Integer(values))
        } else {
            Err(internal(
                "selected string argument is not an integer carrier",
            ))
        }
    }

    pub(super) fn value(&self, row: usize) -> i64 {
        match self {
            Self::Integer(array) => i64::from(array.value(row)),
            Self::Bigint(array) => array.value(row),
        }
    }
}

pub(super) fn evaluate_string_substring<'a>(
    _op: StringSubstringOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if !matches!(types.len(), 2 | 3) || types.len() != arguments.len() {
            return Err(invalid(
                "substring requires its exact two or three checked arguments",
            ));
        }
        let [
            FunctionArgumentType::Value(source),
            FunctionArgumentType::Value(start),
            ..,
        ] = types
        else {
            return Err(invalid("substring requires checked source and position"));
        };
        let binary = source.data_type() == &DataType::Binary;
        let char_width = fixed_char_width(source);
        let index_type = start.data_type();
        if !matches!(index_type, DataType::Int32 | DataType::Int64)
            || ((binary || char_width.is_some()) && index_type != &DataType::Int64)
        {
            return Err(invalid(
                "substring position must match the selected integer profile",
            ));
        }
        for (index, ty) in types.iter().enumerate() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("substring requires value arguments"));
            };
            if ty.logical_type() != ValueLogicalType::Physical
                || ty.data_type()
                    != if index == 0 && binary {
                        &DataType::Binary
                    } else if index == 0 {
                        &DataType::Utf8
                    } else {
                        index_type
                    }
            {
                return Err(invalid(
                    "substring differs from its exact installed argument profile",
                ));
            }
            work.step()?;
        }
        let target = input.contract().result_type();
        if target.logical_type() != ValueLogicalType::Physical
            || target.data_type() != source.data_type()
            || match char_width {
                Some(width) => !matches!(
                    target.value_type().kind(),
                    ValueKind::BoundedCharacter(ty)
                        if ty.kind() == CharacterKind::Varchar && ty.length() as usize == width
                ),
                None => fixed_char_width(target).is_some(),
            }
        {
            return Err(invalid(
                "substring differs from its exact installed result profile",
            ));
        }
        let source = arguments[0].array().as_any();
        let strings = if binary {
            None
        } else {
            Some(
                source
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("substring selected source is not Utf8"))?,
            )
        };
        let binaries = if binary {
            Some(
                source
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| internal("substring selected source is not Binary"))?,
            )
        } else {
            None
        };
        let starts = StringIntegerArray::read(arguments[1].array().as_ref())?;
        let lengths = arguments
            .get(2)
            .map(|arg| StringIntegerArray::read(arg.array().as_ref()))
            .transpose()?;
        let row_span = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<SubstringSpan<'_>>, KernelFailure> {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal("substring selected argument row is out of bounds"));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("substring requires value arguments"));
                    };
                    if !ty.nullable() {
                        return Err(internal(
                            "substring non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                return Ok(None);
            }
            let start = starts.value(rows[1]);
            let length = lengths.as_ref().map(|array| array.value(rows[2]));
            if let Some(strings) = strings {
                let source = strings.value(rows[0]);
                if let Some(width) = char_width {
                    fixed_substring_span(source, width, start, length, work).map(Some)
                } else {
                    substring_span(source, start, length, work)
                        .map(|text| Some(SubstringSpan::borrowed(text.as_bytes())))
                }
            } else if let Some(binaries) = binaries {
                Ok(Some(SubstringSpan::borrowed(binary_span(
                    binaries.value(rows[0]),
                    start,
                    length,
                ))))
            } else {
                Err(internal("substring has no prepared source carrier"))
            }
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some(span) = row_span(ordinal, batch_row, &mut work)? {
                total = total
                    .checked_add(span.byte_count())
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
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0i32);
        let mut has_null = false;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            match row_span(ordinal, batch_row, &mut work)? {
                None => {
                    has_null = true;
                    validity.append(false);
                }
                Some(span) => {
                    if span.byte_count() > total - bytes.len() {
                        return Err(internal("substring exceeded its measured output extent"));
                    }
                    for byte in span
                        .bytes
                        .iter()
                        .copied()
                        .chain(std::iter::repeat_n(b' ', span.spaces))
                    {
                        bytes.push(byte);
                        work.step()?;
                    }
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "substring differs from its measured output extent",
            ));
        }
        work.flush()?;
        let offsets = OffsetBuffer::new(offsets.into());
        let bytes = Buffer::from(bytes);
        let nulls = has_null.then(|| NullBuffer::new(validity.finish()));
        let array = if binary {
            Arc::new(BinaryArray::new(offsets, bytes, nulls)) as ArrayRef
        } else {
            Arc::new(StringArray::new(offsets, bytes, nulls)) as ArrayRef
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
#[path = "string_substring_tests.rs"]
mod tests;
