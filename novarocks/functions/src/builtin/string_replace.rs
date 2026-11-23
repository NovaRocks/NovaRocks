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

//! Selected original Rust non-overlapping replace over borrowed UTF-8 spans.
//! Two passes measure and write; there is no per-row String or size-policy cap.
//! Standard search and Arrow construction have opaque before/after checkpoints.
//! Layout gates are representation checks, not a formal host memory grant.

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
pub(super) enum StringReplaceOp {
    Replace,
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

fn visit_replaced(
    text: &str,
    needle: &str,
    replacement: &str,
    work: &mut EvaluationCheckpoints<'_>,
    mut visit: impl FnMut(&str, &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    // Rust's original str::replace uses this same non-overlapping searcher.
    // Both constructing it and each next-match search are opaque library calls;
    // no synthetic scan claims to meter their internal byte comparisons.
    work.flush()?;
    let mut matches = text.match_indices(needle);
    work.flush()?;
    let mut last = 0usize;
    loop {
        work.flush()?;
        let matched = matches.next();
        work.flush()?;
        let Some((start, part)) = matched else {
            break;
        };
        visit(&text[last..start], work)?;
        visit(replacement, work)?;
        last = start + part.len();
        work.step()?;
    }
    visit(&text[last..], work)?;
    work.step()?;
    Ok(())
}

pub(super) fn evaluate_string_replace<'a>(
    op: StringReplaceOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let StringReplaceOp::Replace = op;
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if !matches!(types.len(), 2 | 3) || types.len() != arguments.len() {
            return Err(invalid(
                "replace requires two checked arguments and an optional replacement",
            ));
        }
        for ty in types {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("replace requires value arguments"));
            };
            let exact = ty.logical_type() == ValueLogicalType::Physical
                && ty.data_type() == &DataType::Utf8;
            work.step()?;
            if !exact {
                return Err(invalid(
                    "replace differs from its exact installed argument profile",
                ));
            }
        }
        let target = input.contract().result_type();
        if target.logical_type() != ValueLogicalType::Physical
            || target.data_type() != &DataType::Utf8
        {
            return Err(invalid(
                "replace differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("replace selected source is not Utf8"))?;
        let needles = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("replace selected needle is not Utf8"))?;
        let replacements = arguments
            .get(2)
            .map(|argument| {
                argument
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("replace selected replacement is not Utf8"))
            })
            .transpose()?;
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
                    return Err(internal("replace selected argument row is out of bounds"));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("replace requires value arguments"));
                    };
                    if !ty.nullable() {
                        return Err(internal(
                            "replace non-null argument contains selected SQL NULL",
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
                needles.value(rows[1]),
                replacements.map_or("", |values| values.value(rows[2])),
            ]))
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some([text, needle, replacement]) =
                selected_texts(ordinal, batch_row, &mut work)?
            {
                visit_replaced(text, needle, replacement, &mut work, |span, work| {
                    total = span_length(total, span.len())?;
                    work.step()
                })?;
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
                Some([text, needle, replacement]) => {
                    visit_replaced(text, needle, replacement, &mut work, |span, work| {
                        if span.len() > total - bytes.len() {
                            return Err(internal("replace exceeded its measured output extent"));
                        }
                        for byte in span.bytes() {
                            bytes.push(byte);
                            work.step()?;
                        }
                        Ok(())
                    })?;
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal("replace differs from its measured output extent"));
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
#[path = "string_replace_tests.rs"]
mod tests;
