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

//! Borrowed selected Unicode-scalar lpad/rpad with both original size gates.
//! Two output passes measure and directly write without row String/char buffers.
//! Layout gates are representational facts, not formal host memory grants.

use super::string_measure::{fixed_char_padding, fixed_char_width};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{check_i32_output_extent, internal, invalid, reserve_local_output},
    kernel_input::EvaluationCheckpoints,
    string_repeat_pad_core::{self, CyclicCharacters, PadProjection, SourceCharacters},
};
use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::{FunctionIntrinsicRowError, ValueKind, ValueLogicalType};
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringPadOp {
    Left,
    Right,
}

fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    check_i32_output_extent(bytes)?;
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

// The original string::common owner applies this independently to target
// Unicode scalar count and resulting UTF-8 bytes. Oversize is successful NULL.
#[cfg(test)]
const MAX_PAD_LENGTH: usize = string_repeat_pad_core::MAX_STRING_BYTES;

struct BorrowedCharacters<'a> {
    text: &'a str,
    len: usize,
    spaces: usize,
}
impl SourceCharacters for BorrowedCharacters<'_> {
    fn len(&self) -> usize {
        self.len
    }
    fn iter(&self) -> impl Iterator<Item = char> {
        self.text
            .chars()
            .chain(std::iter::repeat_n(' ', self.spaces))
    }
}
struct BorrowedCycle<'a> {
    text: &'a str,
    len: usize,
    cursor: std::str::Chars<'a>,
    next: usize,
}
impl CyclicCharacters for BorrowedCycle<'_> {
    type Error = KernelFailure;
    fn len(&self) -> usize {
        self.len
    }
    fn get(&mut self, index: usize) -> Result<char, KernelFailure> {
        if index == 0 {
            self.cursor = self.text.chars();
            self.next = 0;
        }
        if index != self.next {
            return Err(internal(
                "lpad/rpad cyclic projection differs from its source order",
            ));
        }
        let ch = self
            .cursor
            .next()
            .ok_or_else(|| internal("lpad/rpad cyclic projection has no original character"))?;
        self.next += 1;
        Ok(ch)
    }
}
/// Inline storage projection: no per-row String, Vec<char>, or scratch grant.
/// The shared original program alone selects prefix, fill count, side, and caps.
struct CompactPadProjection<'a, 'control> {
    bytes: Option<&'a mut Vec<u8>>,
    total: usize,
    extent: usize,
    char_width: Option<usize>,
    source_spaces: usize,
    work: &'a mut EvaluationCheckpoints<'control>,
}
impl CompactPadProjection<'_, '_> {
    fn push(&mut self, byte: u8) -> Result<(), KernelFailure> {
        self.extent = self
            .extent
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        if let Some(bytes) = &mut self.bytes {
            if bytes.len() >= self.total {
                return Err(internal("lpad/rpad exceeded its measured output extent"));
            }
            bytes.push(byte);
        }
        self.work.step()
    }
    fn copy(&mut self, source: &str) -> Result<(), KernelFailure> {
        for byte in source.bytes() {
            self.push(byte)?;
        }
        for _ in 0..self.source_spaces {
            self.push(b' ')?;
        }
        Ok(())
    }
    fn characters(&mut self, text: &str) -> Result<usize, KernelFailure> {
        let mut count = 0;
        for _ in text.chars() {
            count += 1;
            self.work.step()?;
        }
        Ok(count)
    }
}
impl<'source> PadProjection<'source> for CompactPadProjection<'_, '_> {
    type Error = KernelFailure;
    type Source = BorrowedCharacters<'source>;
    type Pad = BorrowedCycle<'source>;
    type Fill = ();
    type Output = usize;
    type Emission = Option<usize>;
    fn source(&mut self, text: &'source str) -> Result<Self::Source, KernelFailure> {
        let stored = self.characters(text)?;
        let len = self.char_width.unwrap_or(stored);
        self.source_spaces = fixed_char_padding(len, stored)?;
        Ok(BorrowedCharacters {
            text,
            len,
            spaces: self.source_spaces,
        })
    }
    fn pad(&mut self, text: &'source str) -> Result<Self::Pad, KernelFailure> {
        Ok(BorrowedCycle {
            text,
            len: self.characters(text)?,
            cursor: text.chars(),
            next: 0,
        })
    }
    fn prefix(&mut self, chars: impl Iterator<Item = char>) -> Result<usize, KernelFailure> {
        for ch in chars {
            self.push_fill(&mut (), ch)?;
        }
        Ok(self.extent)
    }
    fn new_fill(&mut self, source: &str, left: bool) -> Result<(), KernelFailure> {
        // Direct compact output stores the right-prefix before its fill; this
        // is storage projection only. The original owned projection is unchanged.
        if !left {
            self.copy(source)?;
        }
        Ok(())
    }
    fn push_fill(&mut self, _fill: &mut (), ch: char) -> Result<(), KernelFailure> {
        let mut utf8 = [0u8; 4];
        for byte in ch.encode_utf8(&mut utf8).bytes() {
            self.push(byte)?;
        }
        Ok(())
    }
    fn compose_left(&mut self, _fill: (), source: &str) -> Result<usize, KernelFailure> {
        self.copy(source)?;
        Ok(self.extent)
    }
    fn compose_right(&mut self, _source: &str, _fill: ()) -> Result<usize, KernelFailure> {
        Ok(self.extent)
    }
    fn result_len(&self, result: &usize) -> usize {
        *result
    }
    fn emit(&mut self, result: Option<usize>) -> Result<Option<usize>, KernelFailure> {
        Ok(result)
    }
}

/// Original static failures are shared without constructing CPU diagnostics during admission.
#[derive(Clone, Copy)]
pub(super) enum StaticProfileFailure {
    Count,
    Value,
    Source,
    Result,
}
impl StaticProfileFailure {
    fn message(self) -> &'static str {
        match self {
            Self::Count => "lpad/rpad requires its exact three checked arguments",
            Self::Value => "lpad/rpad requires value arguments",
            Self::Source => "lpad/rpad differs from its exact installed argument profile",
            Self::Result => "lpad/rpad differs from its exact installed result profile",
        }
    }
}
pub(super) fn check_count(types: usize, arguments: usize) -> Result<(), StaticProfileFailure> {
    let arity = 3;
    if types != arity || types != arguments {
        return Err(StaticProfileFailure::Count);
    }
    Ok(())
}
pub(super) fn check_argument(
    index: usize,
    ty: &FunctionArgumentType,
    value_carrier: &DataType,
) -> Result<(), StaticProfileFailure> {
    let FunctionArgumentType::Value(ty) = ty else {
        return Err(StaticProfileFailure::Value);
    };
    if ty.logical_type() != ValueLogicalType::Physical
        || ty.data_type()
            != if index != 1 {
                value_carrier
            } else {
                &DataType::Int64
            }
    {
        return Err(StaticProfileFailure::Source);
    }
    Ok(())
}
pub(super) fn check_result(target: &crate::FunctionValueType) -> Result<(), StaticProfileFailure> {
    if target.logical_type() != ValueLogicalType::Physical
        || !matches!(target.data_type(), DataType::Utf8 | DataType::Binary)
        || !target.nullable()
    {
        return Err(StaticProfileFailure::Result);
    }
    Ok(())
}

pub(super) fn check_character_profile(
    op: StringPadOp,
    types: &[FunctionArgumentType],
    target: &crate::FunctionValueType,
    trino: bool,
) -> Result<Option<usize>, StaticProfileFailure> {
    let [
        FunctionArgumentType::Value(source),
        _,
        FunctionArgumentType::Value(fill),
    ] = types
    else {
        return Err(StaticProfileFailure::Value);
    };
    let width = fixed_char_width(source);
    if fixed_char_width(fill).is_some() || (width.is_some() && (op != StringPadOp::Left || !trino))
    {
        return Err(StaticProfileFailure::Source);
    }
    if fixed_char_width(target).is_some()
        || width.is_some() && target.value_type().kind() != &ValueKind::String
    {
        return Err(StaticProfileFailure::Result);
    }
    Ok(width)
}

enum PadRow<'a> {
    Text {
        source: &'a str,
        fill: &'a str,
        char_width: Option<usize>,
    },
    Binary {
        source: &'a [u8],
        fill: &'a [u8],
    },
}

impl PadRow<'_> {
    fn empty_fill(&self) -> bool {
        match self {
            Self::Text { fill, .. } => fill.is_empty(),
            Self::Binary { fill, .. } => fill.is_empty(),
        }
    }

    fn render(
        &self,
        length: i64,
        left: bool,
        trino: bool,
        mut projection: CompactPadProjection<'_, '_>,
    ) -> Result<Option<usize>, KernelFailure> {
        match *self {
            Self::Text {
                source,
                fill,
                char_width,
            } => {
                projection.char_width = char_width;
                string_repeat_pad_core::pad_into_with_limit(
                    source,
                    length,
                    || fill,
                    left,
                    (!trino).then_some(string_repeat_pad_core::MAX_STRING_BYTES),
                    projection,
                )
            }
            Self::Binary { source, fill } => {
                let length = usize::try_from(length)
                    .map_err(|_| internal("binary padding received an invalid checked length"))?;
                if length <= source.len() {
                    for byte in &source[..length] {
                        projection.push(*byte)?;
                    }
                } else {
                    let padding = fill.iter().cycle().take(length - source.len());
                    if left {
                        for byte in padding.chain(source) {
                            projection.push(*byte)?;
                        }
                    } else {
                        for byte in source.iter().chain(padding) {
                            projection.push(*byte)?;
                        }
                    }
                }
                Ok(Some(projection.extent))
            }
        }
    }
}

type SelectedPadRow<'a> = Result<Option<(PadRow<'a>, i64)>, &'static str>;

pub(super) fn evaluate_string_pad<'a>(
    op: StringPadOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        check_count(types.len(), arguments.len()).map_err(|error| invalid(error.message()))?;
        let target = input.contract().result_type();
        check_result(target).map_err(|error| invalid(error.message()))?;
        for (index, ty) in types.iter().enumerate() {
            check_argument(index, ty, target.data_type())
                .map_err(|error| invalid(error.message()))?;
            work.step()?;
        }
        let trino = input.contract().effects().own_row_error == FunctionIntrinsicRowError::MayRaise;
        let char_width = check_character_profile(op, types, target, trino)
            .map_err(|error| invalid(error.message()))?;
        let binary = target.data_type() == &DataType::Binary;
        let lengths = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal("lpad/rpad selected length is not Int64"))?;
        let source = arguments[0].array().as_any();
        let fill = arguments[2].array().as_any();
        let texts = source
            .downcast_ref::<StringArray>()
            .zip(fill.downcast_ref::<StringArray>());
        let binaries = source
            .downcast_ref::<BinaryArray>()
            .zip(fill.downcast_ref::<BinaryArray>());
        if (binary && (!trino || binaries.is_none())) || (!binary && texts.is_none()) {
            return Err(internal(
                "lpad/rpad selected carriers differ from their checked types",
            ));
        }
        let left = op == StringPadOp::Left;
        let selected_plan = |ordinal,
                             batch_row,
                             work: &mut EvaluationCheckpoints<'_>|
         -> Result<SelectedPadRow<'_>, KernelFailure> {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal("lpad/rpad selected argument row is out of bounds"));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("lpad/rpad requires value arguments"));
                    };
                    if !ty.nullable() {
                        return Err(internal(
                            "lpad/rpad non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                return Ok(Ok(None));
            }
            let length = lengths.value(rows[1]);
            let row = if let Some((strings, pads)) = texts {
                PadRow::Text {
                    source: strings.value(rows[0]),
                    fill: pads.value(rows[2]),
                    char_width,
                }
            } else if let Some((binaries, pads)) = binaries {
                PadRow::Binary {
                    source: binaries.value(rows[0]),
                    fill: pads.value(rows[2]),
                }
            } else {
                return Err(internal("lpad/rpad did not prepare a value carrier"));
            };
            if trino {
                if !(0..=i64::from(i32::MAX)).contains(&length) {
                    return Ok(Err("Target length must be in the range [0..2147483647]"));
                }
                if row.empty_fill() {
                    return Ok(Err("Padding must not be empty"));
                }
            }
            Ok(Ok(Some((row, length))))
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Ok(Some((row, count))) = selected_plan(ordinal, batch_row, &mut work)?
                && let Some(extent) = row.render(
                    count,
                    left,
                    trino,
                    CompactPadProjection {
                        bytes: None,
                        total: 0,
                        extent: 0,
                        char_width: None,
                        source_spaces: 0,
                        work: &mut work,
                    },
                )?
            {
                total = total
                    .checked_add(extent)
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
        }
        output_capacity(selection.len(), total)?;
        work.flush()?;
        let mut bytes = reserve_local_output(total, "lpad/rpad output bytes")?;
        work.flush()?;
        let mut offsets = reserve_local_output(selection.len() + 1, "lpad/rpad output offsets")?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0i32);
        let mut has_null = false;
        let mut errors = Vec::new();
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let measured = match selected_plan(ordinal, batch_row, &mut work)? {
                Ok(None) => None,
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
                Ok(Some((row, count))) => row
                    .render(
                        count,
                        left,
                        trino,
                        CompactPadProjection {
                            bytes: None,
                            total: 0,
                            extent: 0,
                            char_width: None,
                            source_spaces: 0,
                            work: &mut work,
                        },
                    )?
                    .map(|extent| (row, count, extent)),
            };
            match measured {
                None => {
                    has_null = true;
                    validity.append(false);
                }
                Some((row, count, extent)) => {
                    let written = row.render(
                        count,
                        left,
                        trino,
                        CompactPadProjection {
                            bytes: Some(&mut bytes),
                            total,
                            extent: 0,
                            char_width: None,
                            source_spaces: 0,
                            work: &mut work,
                        },
                    )?;
                    if written != Some(extent) {
                        return Err(internal(
                            "lpad/rpad differs from its measured output extent",
                        ));
                    }
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "lpad/rpad differs from its measured output extent",
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
#[path = "string_pad_tests.rs"]
mod tests;
