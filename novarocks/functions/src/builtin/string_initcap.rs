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

//! Selected original per-character Unicode initcap, without runtime name lookup.
//! Two passes measure and write into one output backing; no row String is built.
//! Layout checks prove representation only, not a formal host memory grant.

use super::string_measure::fixed_char_width;
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
pub(super) enum StringInitcapOp {
    Initcap,
    TitleCase,
}

fn visit_mapped(
    operation: StringInitcapOp,
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
    mut visit: impl FnMut(char, &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let mut new_word = true;
    for ch in text.chars() {
        if operation == StringInitcapOp::TitleCase {
            let space = super::string_trim_core::trino_whitespace(ch);
            let mapped = if space {
                ch
            } else if new_word {
                super::string_case::unicode::titlecase(ch)
            } else {
                super::string_case::unicode::lowercase(ch)
            };
            new_word = space;
            work.step()?;
            visit(mapped, work)?;
            continue;
        }
        // Each UTF-8 scalar decode and Unicode classification has completed.
        // Per-char mapping preserves the legacy non-contextual sigma rule.
        let alphanumeric = ch.is_alphanumeric();
        let alphabetic = ch.is_alphabetic();
        work.step()?;
        if !alphanumeric {
            new_word = true;
            visit(ch, work)?;
        } else if alphabetic && new_word {
            new_word = false;
            for mapped in ch.to_uppercase() {
                visit(mapped, work)?;
            }
        } else if alphabetic {
            for mapped in ch.to_lowercase() {
                visit(mapped, work)?;
            }
        } else {
            new_word = false;
            visit(ch, work)?;
        }
    }
    Ok(())
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

pub(super) fn evaluate_string_initcap<'a>(
    op: StringInitcapOp,
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
            return Err(invalid("initcap requires one checked value argument"));
        };
        let target = input.contract().result_type();
        let source_width = fixed_char_width(source);
        if source.logical_type() != ValueLogicalType::Physical
            || source.data_type() != &DataType::Utf8
            || target.logical_type() != ValueLogicalType::Physical
            || target.data_type() != &DataType::Utf8
            || source_width != fixed_char_width(target)
            || (op == StringInitcapOp::Initcap && (!target.nullable() || source_width.is_some()))
        {
            return Err(invalid("initcap differs from its exact installed profile"));
        }
        let values = argument
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("initcap selected carrier is not Utf8"))?;
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut output_bytes = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let row = argument.value_row(ordinal, batch_row);
            work.step()?;
            if row >= values.len() {
                return Err(internal("initcap selected row is outside its carrier"));
            }
            if values.is_null(row) {
                if !source.nullable() {
                    return Err(internal(
                        "initcap non-null source contains selected SQL NULL",
                    ));
                }
                continue;
            }
            let text = values.value(row);
            let mut length = 0usize;
            visit_mapped(op, text, &mut work, |mapped, work| {
                length = length
                    .checked_add(mapped.len_utf8())
                    .ok_or(KernelFailure::ResourceExhausted)?;
                work.step()
            })?;
            output_bytes = output_bytes
                .checked_add(length)
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
                visit_mapped(op, values.value(row), &mut work, |mapped, work| {
                    let mut encoded = [0u8; 4];
                    let mapped = mapped.encode_utf8(&mut encoded).as_bytes();
                    if mapped.len() > output_bytes - bytes.len() {
                        return Err(internal("initcap exceeded its measured output extent"));
                    }
                    for byte in mapped {
                        bytes.push(*byte);
                        work.step()?;
                    }
                    Ok(())
                })?;
                validity.append(true);
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != output_bytes {
            return Err(internal("initcap differs from its measured output extent"));
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
#[path = "string_initcap_tests.rs"]
mod tests;
