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

//! Selected substring_index borrows the original forward, non-overlapping match spans.
//! Layout checks cover requested representability, not formal host memory grants.

use super::string_substring_index_core::{
    ForwardMatchAccess, ForwardMatchObserver, scan_forward_matches, substring_index_span,
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int32Array, StringArray};
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

impl ForwardMatchObserver for EvaluationCheckpoints<'_> {
    type Error = KernelFailure;
    fn flush(&mut self) -> Result<(), KernelFailure> {
        EvaluationCheckpoints::flush(self)
    }
    fn step(&mut self) -> Result<(), KernelFailure> {
        EvaluationCheckpoints::step(self)
    }
}

struct PureMatchAccess<'text, 'work, 'control> {
    strings: &'text StringArray,
    source_row: usize,
    work: &'work mut EvaluationCheckpoints<'control>,
    cut: Option<usize>,
}
impl<'text> ForwardMatchAccess<'text> for PureMatchAccess<'text, '_, '_> {
    type Error = KernelFailure;
    fn text(&self) -> &'text str {
        self.strings.value(self.source_row)
    }
    fn first_pass(
        &mut self,
        text: &str,
        delimiter: &str,
        target: Option<usize>,
    ) -> Result<usize, KernelFailure> {
        let scan = scan_forward_matches(text, delimiter, target, self.work)?;
        self.cut = scan.cut;
        Ok(scan.count)
    }
    fn prefix_cut(&mut self, _: &str, _: &str, _: usize) -> Result<usize, KernelFailure> {
        self.cut
            .ok_or_else(|| internal("substring_index forward match extent changed"))
    }
    fn suffix_cut(
        &mut self,
        text: &str,
        delimiter: &str,
        ordinal: usize,
    ) -> Result<usize, KernelFailure> {
        scan_forward_matches(text, delimiter, Some(ordinal), self.work)?
            .cut
            .ok_or_else(|| internal("substring_index forward match extent changed"))
    }
}

pub(super) fn evaluate_string_substring_index<'a>(
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
                "substring_index requires its exact three checked arguments",
            ));
        }
        for (index, ty) in types.iter().enumerate() {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("substring_index requires value arguments"));
            };
            if ty.logical_type != ValueLogicalType::Physical
                || ty.data_type
                    != if index < 2 {
                        DataType::Utf8
                    } else {
                        DataType::Int32
                    }
            {
                return Err(invalid(
                    "substring_index differs from its exact installed argument profile",
                ));
            }
            work.step()?;
        }
        let target = input.contract().result_type();
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "substring_index differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("substring_index selected source is not Utf8"))?;
        let delimiters = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("substring_index selected delimiter is not Utf8"))?;
        let counts = arguments[2]
            .array()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| internal("substring_index selected count is not Int32"))?;
        let row_span = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<&str>, KernelFailure> {
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for (index, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[index] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal(
                        "substring_index selected argument row is out of bounds",
                    ));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[index] else {
                        return Err(invalid("substring_index requires value arguments"));
                    };
                    if !ty.nullable {
                        return Err(internal(
                            "substring_index non-null argument contains selected SQL NULL",
                        ));
                    }
                    is_null = true;
                }
            }
            if is_null {
                return Ok(None);
            }
            let delimiter = delimiters.value(rows[1]);
            let count = counts.value(rows[2]);
            let mut access = PureMatchAccess {
                strings,
                source_row: rows[0],
                work,
                cut: None,
            };
            substring_index_span(&mut access, delimiter, i64::from(count))
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, batch_row) in selection.iter().enumerate() {
            if let Some(span) = row_span(ordinal, batch_row, &mut work)? {
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
                    if span.len() > total - bytes.len() {
                        return Err(internal(
                            "substring_index exceeded its measured output extent",
                        ));
                    }
                    for byte in span.bytes() {
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
                "substring_index differs from its measured output extent",
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
#[path = "string_substring_index_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "string_substring_index_shared_tests.rs"]
mod shared_tests;
