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

//! Selected original Unicode scalar positions using the borrowed std searcher.
//! Rust 1.92 str::find uses inline StrSearcher/TwoWaySearcher state, with no
//! dynamic scratch. The search remains opaque between original control checks;
//! this author does not promise a checkpoint within that standard-library call.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringLocateOp {
    Locate,
    Instr,
    Strpos,
}

fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = Layout::array::<i32>(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let bitmap = rows
        .checked_add(63)
        .map(|n| n / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
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
fn locate(
    haystack: &str,
    needle: &str,
    start: i64,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<i32, KernelFailure> {
    if start <= 0 {
        work.step()?;
        return Ok(0);
    }
    let Ok(start_index) = usize::try_from(start) else {
        work.step()?;
        return Ok(0);
    };
    let target = start_index - 1;
    let mut characters = 0usize;
    let mut start_byte = None;
    for (ordinal, (byte, character)) in haystack.char_indices().enumerate() {
        if ordinal == target {
            start_byte = Some(byte);
        }
        characters += 1;
        observe_char(character, work)?;
    }
    if needle.is_empty() {
        let position = if start_index <= characters.max(1) {
            start
        } else {
            0
        };
        work.step()?;
        return i32::try_from(position)
            .map_err(|_| internal("locate empty-needle position exceeds Utf8 representation"));
    }
    if start_index > characters {
        work.step()?;
        return Ok(0);
    }
    let Some(start_byte) = start_byte else {
        work.step()?;
        return Ok(0);
    };
    // Same borrowed standard search as execution/string/locate_ops.rs. No
    // synthetic work loop substitutes for its opaque internal byte comparisons.
    work.flush()?;
    let relative = haystack[start_byte..].find(needle);
    work.flush()?;
    let Some(relative) = relative else {
        return Ok(0);
    };
    let absolute = start_byte + relative;
    let mut prefix_chars = 0usize;
    for character in haystack[..absolute].chars() {
        prefix_chars += 1;
        observe_char(character, work)?;
    }
    i32::try_from(prefix_chars + 1)
        .map_err(|_| internal("locate result exceeds Utf8 representation"))
}

fn strpos(
    haystack: &str,
    needle: &str,
    instance: u64,
    reverse: bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<i64, KernelFailure> {
    if needle.is_empty() {
        return Ok(1);
    }
    if instance > haystack.len() as u64 {
        return Ok(0);
    }
    // Every nonempty match starts at a distinct code point, even when
    // matches overlap. Observe the source while deriving that upper bound.
    let mut characters = 0;
    for character in haystack.chars() {
        observe_char(character, work)?;
        characters += 1;
    }
    if instance > characters {
        return Ok(0);
    }
    let mut range = 0..haystack.len();
    let mut matched = 0;
    while matched < instance {
        let part = &haystack[range.clone()];
        work.flush()?;
        let found = if reverse {
            part.rfind(needle)
        } else {
            part.find(needle)
        };
        work.flush()?;
        let Some(found) = found else {
            return Ok(0);
        };
        let start = range.start + found;
        matched += 1;
        if matched == instance {
            let mut position = 1;
            for character in haystack[..start].chars() {
                observe_char(character, work)?;
                position += 1;
            }
            return Ok(position);
        }
        // Resume at the next candidate boundary, including overlapping matches.
        let remaining = if reverse {
            let end = start + needle.len();
            haystack[..end]
                .char_indices()
                .next_back()
                .map(|(byte, _)| byte)
        } else {
            haystack[start..]
                .char_indices()
                .nth(1)
                .map(|(byte, _)| start + byte)
        };
        let Some(boundary) = remaining else {
            return Ok(0);
        };
        if reverse {
            range.end = boundary;
        } else {
            range.start = boundary;
        }
        work.step()?;
    }
    Ok(0)
}

pub(super) fn evaluate_string_locate<'a>(
    op: StringLocateOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let args = input.arguments();
        let valid_arity = match op {
            StringLocateOp::Instr => types.len() == 2,
            StringLocateOp::Locate | StringLocateOp::Strpos => matches!(types.len(), 2 | 3),
        };
        if !valid_arity || types.len() != args.len() {
            return Err(invalid(
                "locate/instr differs from its installed argument count",
            ));
        }
        for (index, ty) in types.iter().enumerate() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("locate/instr requires checked values"));
            };
            if ty.logical_type() != ValueLogicalType::Physical
                || *ty.data_type()
                    != if index < 2 {
                        DataType::Utf8
                    } else {
                        DataType::Int64
                    }
            {
                return Err(invalid(
                    "locate/instr differs from its installed argument profile",
                ));
            }
            work.step()?;
        }
        let target = input.contract().result_type();
        let is_strpos = op == StringLocateOp::Strpos;
        let result_carrier = if is_strpos {
            DataType::Int64
        } else {
            DataType::Int32
        };
        if target.logical_type() != ValueLogicalType::Physical
            || target.data_type() != &result_carrier
            || (!is_strpos && !target.nullable())
        {
            return Err(invalid(
                "locate/instr differs from its installed result profile",
            ));
        }
        let (haystack_index, needle_index) = match op {
            StringLocateOp::Locate => (1, 0),
            StringLocateOp::Instr | StringLocateOp::Strpos => (0, 1),
        };
        let haystacks = args[haystack_index]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("locate/instr haystack carrier is not Utf8"))?;
        let needles = args[needle_index]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("locate/instr needle carrier is not Utf8"))?;
        let starts = if args.len() == 3 {
            Some(
                args[2]
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| internal("locate start carrier is not Int64"))?,
            )
        } else {
            None
        };
        let selection = input.selection();
        output_capacity(selection.len())?;
        if is_strpos {
            Layout::array::<i64>(selection.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
        }
        work.flush()?;
        let mut values = Vec::new();
        let mut wide_values = Vec::new();
        if is_strpos {
            wide_values
                .try_reserve_exact(selection.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
        } else {
            values
                .try_reserve_exact(selection.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
        }
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        let mut errors = Vec::new();
        work.flush()?;
        let mut has_null = false;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, arg) in args.iter().enumerate() {
                let row = arg.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= arg.array().len() {
                    return Err(internal("locate/instr selected row is out of bounds"));
                }
                if arg.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("locate/instr requires checked values"));
                    };
                    if !ty.nullable() {
                        return Err(internal(
                            "locate/instr non-null source contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            let value = if is_null {
                None
            } else {
                let start = starts.map_or(1, |array| array.value(rows[2]));
                let haystack = haystacks.value(rows[haystack_index]);
                let needle = needles.value(rows[needle_index]);
                if !is_strpos {
                    Some(i64::from(locate(haystack, needle, start, &mut work)?))
                } else if let Some(instance) = start.checked_abs().filter(|value| *value != 0) {
                    Some(strpos(
                        haystack,
                        needle,
                        instance as u64,
                        start < 0,
                        &mut work,
                    )?)
                } else {
                    errors
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    errors.push(crate::RowDataError::with_reason(
                        ordinal,
                        crate::DataFailureReason::Domain(
                            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                        ),
                        "'instance' must be a positive or negative number.",
                    ));
                    None
                }
            };
            if is_strpos {
                wide_values.push(value.unwrap_or(0));
            } else {
                values.push(value.unwrap_or(0) as i32);
            }
            validity.append(value.is_some());
            has_null |= value.is_none();
            work.step()?;
        }
        work.flush()?;
        let nulls = has_null.then(|| NullBuffer::new(validity.finish()));
        let array = if is_strpos {
            Arc::new(Int64Array::new(wide_values.into(), nulls)) as ArrayRef
        } else {
            Arc::new(Int32Array::new(values.into(), nulls)) as ArrayRef
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
#[path = "string_locate_tests.rs"]
mod tests;
