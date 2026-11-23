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

//! Selected character translation with first-source-index duplicate semantics.
//! A fixed ASCII table or sorted Unicode index lives only for the current row.
//! Two output passes measure and emit without per-row output Strings.
//! Sorting and Arrow construction are opaque before/after controlled calls.
//! Layout checks describe requests, not a formal host memory grant.

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringTranslateOp {
    Translate,
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

fn span_length(total: usize, length: usize) -> Result<usize, KernelFailure> {
    total
        .checked_add(length)
        .ok_or(KernelFailure::ResourceExhausted)
}

const UNMAPPED: i16 = -1;
const DELETED: i16 = -2;
type MappingEntry = (char, Option<char>, usize);

// Keep the finite ASCII table inline: boxing it would introduce an extra
// per-row allocation merely to shrink a transient stack discriminator.
#[allow(clippy::large_enum_variant)]
enum TranslateMap {
    Ascii([i16; 256]),
    Unicode(Vec<MappingEntry>),
}

fn mapping_capacity(count: usize) -> Result<(), KernelFailure> {
    Layout::array::<MappingEntry>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}

fn ascii_observed(text: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<bool, KernelFailure> {
    for byte in text.bytes() {
        let ascii = byte.is_ascii();
        work.step()?;
        if !ascii {
            return Ok(false);
        }
    }
    Ok(true)
}

impl TranslateMap {
    fn prepare(
        from: &str,
        to: &str,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        // The original branch depends on both mapping strings, not source text.
        if ascii_observed(from, work)? && ascii_observed(to, work)? {
            work.flush()?;
            let mut map = [UNMAPPED; 256];
            work.flush()?;
            let mut targets = to.bytes();
            for byte in from.bytes() {
                let target = targets.next().map(i16::from).unwrap_or(DELETED);
                if map[byte as usize] == UNMAPPED {
                    map[byte as usize] = target;
                }
                work.step()?;
            }
            return Ok(Self::Ascii(map));
        }
        let mut count = 0usize;
        for _ in from.chars() {
            count = count
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?;
            work.step()?;
        }
        mapping_capacity(count)?;
        work.flush()?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut targets = to.chars();
        for (ordinal, source) in from.chars().enumerate() {
            entries.push((source, targets.next(), ordinal));
            work.step()?;
        }
        // In-place standard sort owns its internal comparisons. Bracket the
        // actual call without claiming to meter those opaque comparisons.
        work.flush()?;
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0).then(left.2.cmp(&right.2)));
        work.flush()?;
        Ok(Self::Unicode(entries))
    }

    fn mapped(
        &self,
        ch: char,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<char>, KernelFailure> {
        match self {
            Self::Ascii(map) => {
                let mapped = if ch.is_ascii() {
                    map[ch as usize]
                } else {
                    UNMAPPED
                };
                work.step()?;
                Ok(match mapped {
                    UNMAPPED => Some(ch),
                    DELETED => None,
                    byte => Some(char::from(byte as u8)),
                })
            }
            Self::Unicode(entries) => {
                // Lower bound selects the first original ordinal for duplicates.
                let mut lo = 0usize;
                let mut hi = entries.len();
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let precedes = entries[mid].0 < ch;
                    work.step()?;
                    if precedes {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                let result = match entries.get(lo) {
                    Some((source, target, _)) if *source == ch => *target,
                    _ => Some(ch),
                };
                work.step()?;
                Ok(result)
            }
        }
    }

    fn length(
        &self,
        text: &str,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<usize, KernelFailure> {
        let mut bytes = 0usize;
        for ch in text.chars() {
            work.step()?;
            if let Some(mapped) = self.mapped(ch, work)? {
                bytes = span_length(bytes, mapped.len_utf8())?;
                work.step()?;
            }
        }
        Ok(bytes)
    }

    fn emit(
        &self,
        text: &str,
        bytes: &mut Vec<u8>,
        total: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        for ch in text.chars() {
            work.step()?;
            if let Some(mapped) = self.mapped(ch, work)? {
                let mut encoded = [0u8; 4];
                let span = mapped.encode_utf8(&mut encoded).as_bytes();
                if span.len() > total - bytes.len() {
                    return Err(internal("translate exceeded its measured output extent"));
                }
                for byte in span {
                    bytes.push(*byte);
                    work.step()?;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn evaluate_string_translate<'a>(
    op: StringTranslateOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let StringTranslateOp::Translate = op;
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 3 || types.len() != arguments.len() {
            return Err(invalid(
                "translate requires its exact three checked arguments",
            ));
        }
        for ty in types {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("translate requires value arguments"));
            };
            let exact = ty.logical_type() == ValueLogicalType::Physical
                && ty.data_type() == &DataType::Utf8;
            work.step()?;
            if !exact {
                return Err(invalid(
                    "translate differs from its exact installed argument profile",
                ));
            }
        }
        let target = input.contract().result_type();
        if target.logical_type() != ValueLogicalType::Physical
            || target.data_type() != &DataType::Utf8
            || !target.nullable()
        {
            return Err(invalid(
                "translate differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("translate selected source is not Utf8"))?;
        let from_strings = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("translate selected from is not Utf8"))?;
        let to_strings = arguments[2]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("translate selected to is not Utf8"))?;
        let selected_texts = |ordinal,
                              batch_row,
                              work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<[&str; 3]>, KernelFailure> {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal("translate selected argument row is out of bounds"));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("translate requires value arguments"));
                    };
                    if !ty.nullable() {
                        return Err(internal(
                            "translate non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                return Ok(None);
            }
            Ok(Some([
                strings.value(rows[0]),
                from_strings.value(rows[1]),
                to_strings.value(rows[2]),
            ]))
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some([text, from, to]) = selected_texts(ordinal, batch_row, &mut work)? {
                let map = TranslateMap::prepare(from, to, &mut work)?;
                total = span_length(total, map.length(text, &mut work)?)?;
                work.step()?;
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
            match selected_texts(ordinal, batch_row, &mut work)? {
                None => {
                    has_null = true;
                    validity.append(false);
                }
                Some([text, from, to]) => {
                    let map = TranslateMap::prepare(from, to, &mut work)?;
                    map.emit(text, &mut bytes, total, &mut work)?;
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "translate differs from its measured output extent",
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
#[path = "string_translate_tests.rs"]
mod tests;
