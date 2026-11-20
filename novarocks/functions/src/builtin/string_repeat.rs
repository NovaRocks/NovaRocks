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

//! Selected byte repetition and spaces with the original successful-NULL row cap.
//! Layout checks cover requested representability, not formal host memory grants.

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    string_repeat_pad_core::{self, RepeatPlan, RepeatWriter},
};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StringRepeatOp {
    Repeat,
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

// Existing execution string::common::OLAP_STRING_MAX_LENGTH. This is a
// semantic per-row successful-NULL cap, not a resource budget or default.
#[cfg(test)]
const MAX_ROW_BYTES: usize = crate::string_repeat_pad_core::MAX_STRING_BYTES;

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
            Self::Count => "repeat/space requires its exact checked argument count",
            Self::Value => "repeat/space requires value arguments",
            Self::Source => "repeat/space differs from its exact installed argument profile",
            Self::Result => "repeat/space differs from its exact installed result profile",
        }
    }
}
pub(super) fn check_count(
    op: StringRepeatOp,
    types: usize,
    arguments: usize,
) -> Result<usize, StaticProfileFailure> {
    let arity = match op {
        StringRepeatOp::Repeat => 2,
    };
    if types != arity || types != arguments {
        return Err(StaticProfileFailure::Count);
    }
    Ok(arity)
}
pub(super) fn check_argument(
    op: StringRepeatOp,
    index: usize,
    ty: &FunctionArgumentType,
) -> Result<(), StaticProfileFailure> {
    let FunctionArgumentType::Value(ty) = ty else {
        return Err(StaticProfileFailure::Value);
    };
    if ty.logical_type() != ValueLogicalType::Physical
        || *ty.data_type()
            != if op == StringRepeatOp::Repeat && index == 0 {
                DataType::Utf8
            } else {
                DataType::Int64
            }
    {
        return Err(StaticProfileFailure::Source);
    }
    Ok(())
}
pub(super) fn check_result(target: &crate::FunctionValueType) -> Result<(), StaticProfileFailure> {
    if target.logical_type() != ValueLogicalType::Physical
        || target.data_type() != &DataType::Utf8
        || !target.nullable()
    {
        return Err(StaticProfileFailure::Result);
    }
    Ok(())
}

struct CompactRepeatWriter<'a, 'control> {
    bytes: &'a mut Vec<u8>,
    total: usize,
    work: &'a mut EvaluationCheckpoints<'control>,
}
impl RepeatWriter for CompactRepeatWriter<'_, '_> {
    type Error = KernelFailure;
    type Output = ();
    fn empty(&mut self) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn repeat(&mut self, span: &str, repeats: usize, extent: usize) -> Result<(), KernelFailure> {
        if extent > self.total - self.bytes.len() {
            return Err(internal("repeat/space exceeded its measured output extent"));
        }
        for _ in 0..repeats {
            for byte in span.bytes() {
                self.bytes.push(byte);
                self.work.step()?;
            }
            self.work.step()?;
        }
        Ok(())
    }
}

pub(super) fn evaluate_string_repeat<'a>(
    op: StringRepeatOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        let arity = check_count(op, types.len(), arguments.len())
            .map_err(|error| invalid(error.message()))?;
        for (index, ty) in types.iter().enumerate() {
            check_argument(op, index, ty).map_err(|error| invalid(error.message()))?;
            work.step()?;
        }
        let target = input.contract().result_type();
        check_result(target).map_err(|error| invalid(error.message()))?;
        let strings = if op == StringRepeatOp::Repeat {
            Some(
                arguments[0]
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("repeat selected source is not Utf8"))?,
            )
        } else {
            None
        };
        let count_index = arity - 1;
        let counts = arguments[count_index]
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal("repeat/space selected count is not Int64"))?;
        let row_output = |ordinal,
                          batch_row,
                          work: &mut EvaluationCheckpoints<'_>|
         -> Result<RepeatPlan<'_>, KernelFailure> {
            let mut rows = [0usize; 2];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal(
                        "repeat/space selected argument row is out of bounds",
                    ));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("repeat/space requires value arguments"));
                    };
                    if !ty.nullable() {
                        return Err(internal(
                            "repeat/space non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                work.step()?;
                return Ok(RepeatPlan::Null);
            }
            let output = (|| -> Result<RepeatPlan<'_>, KernelFailure> {
                let count = counts.value(rows[count_index]);
                Ok(match op {
                    StringRepeatOp::Repeat => {
                        let strings = strings
                            .ok_or_else(|| internal("repeat has no checked Utf8 carrier"))?;
                        string_repeat_pad_core::repeat_plan(|| strings.value(rows[0]), count)
                    }
                })
            })()?;
            work.step()?;
            Ok(output)
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some(bytes) = row_output(ordinal, batch_row, &mut work)?.bytes() {
                total = total
                    .checked_add(bytes)
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
            let plan = row_output(ordinal, batch_row, &mut work)?;
            let result = string_repeat_pad_core::render_repeat(
                plan,
                &mut CompactRepeatWriter {
                    bytes: &mut bytes,
                    total,
                    work: &mut work,
                },
            )?;
            match result {
                None => {
                    has_null = true;
                    validity.append(false);
                }
                Some(()) => validity.append(true),
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal(
                "repeat/space differs from its measured output extent",
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
#[path = "string_repeat_tests.rs"]
mod tests;
