// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//! Selected-row implementation of the Trino URL functions.

use super::{
    string_parse_url_shared::{TrinoUri, TrinoUrlPart},
    string_url_decode::TrinoUrlDecode,
};
use crate::{
    DataFailureReason, EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{
    Array, ArrayRef, StringArray,
    builder::{Int64Builder, StringBuilder},
};
use novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT;
use std::{alloc::Layout, fmt::Write, sync::Arc};
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
        .map(|n| n / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}
#[cfg(test)]
#[path = "string_parse_url_tests.rs"]
mod tests;

pub(super) fn evaluate_trino_url<'a>(
    part: TrinoUrlPart,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let mut work = EvaluationCheckpoints::new(control);
    work.flush()?;
    let (source, key) = match input.arguments() {
        [source] if part.arity() == 1 => (UrlArgument::new(*source)?, None),
        [source, key] if part.arity() == 2 => {
            (UrlArgument::new(*source)?, Some(UrlArgument::new(*key)?))
        }
        _ => return Err(invalid("URL arguments differ from the selected overload")),
    };
    let selection = input.selection();
    let mut errors = Vec::new();
    let array = if part.is_port() {
        let mut ports = Int64Builder::with_capacity(selection.len());
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let port = match source.get(ordinal, row)? {
                Some(source) => TrinoUri::parse(source, &mut work)?.and_then(|uri| uri.port),
                None => None,
            };
            ports.append_option(port);
        }
        Arc::new(ports.finish()) as ArrayRef
    } else {
        output_capacity(selection.len(), 0)?;
        let mut strings = StringBuilder::new();
        let mut offset = 0;
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let value = match (source.get(ordinal, row)?, &key) {
                (Some(source), None) => url_text_part(part, source, None, &mut work)?,
                (Some(source), Some(key)) => match key.get(ordinal, row)? {
                    Some(key) => url_text_part(part, source, Some(key), &mut work)?,
                    None => Ok(None),
                },
                (None, _) => Ok(None),
            };
            match value {
                Ok(Some(value)) => {
                    offset =
                        crate::aggregate_scalar::checked_utf8_output_offset(offset, value.len())?;
                    for ch in value.chars() {
                        work.step()?;
                        strings
                            .write_char(ch)
                            .map_err(|_| internal("cannot append the URL result"))?;
                    }
                    strings.append_value("");
                }
                Ok(None) => strings.append_null(),
                Err(message) => {
                    errors.push(RowDataError::with_reason(
                        ordinal,
                        DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
                        message,
                    ));
                    strings.append_null();
                }
            }
        }
        Arc::new(strings.finish()) as ArrayRef
    };
    let result = SelectedValues::try_new_observed(
        selection,
        input.contract().result_type().data_type(),
        array,
        errors.into_boxed_slice(),
        || work.step(),
    );
    work.finish_result(result)
}

struct UrlArgument<'a> {
    evaluation: EvaluatedArgument<'a>,
    strings: &'a StringArray,
}

impl<'a> UrlArgument<'a> {
    fn new(evaluation: EvaluatedArgument<'a>) -> Result<Self, KernelFailure> {
        let strings = evaluation
            .array()
            .as_any()
            .downcast_ref()
            .ok_or_else(|| internal("URL selected input is not Utf8"))?;
        Ok(Self {
            evaluation,
            strings,
        })
    }

    fn get(&self, ordinal: usize, batch_row: usize) -> Result<Option<&'a str>, KernelFailure> {
        let row = self.evaluation.value_row(ordinal, batch_row);
        if row >= self.strings.len() {
            return Err(internal("URL selected input row is out of bounds"));
        }
        Ok((!self.strings.is_null(row)).then(|| self.strings.value(row)))
    }
}

fn url_text_part(
    part: TrinoUrlPart,
    source: &str,
    key: Option<&str>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<Option<String>, &'static str>, KernelFailure> {
    if matches!(part, TrinoUrlPart::Decode) {
        return Ok(TrinoUrlDecode::Form.decode(source, work)?.map(Some));
    }
    let Some(uri) = TrinoUri::parse(source, work)? else {
        return Ok(Ok(None));
    };
    if let Some(key) = key {
        return match uri.raw_parameter(key, work)? {
            Some(value) => Ok(TrinoUrlDecode::Form.decode(value, work)?.map(Some)),
            None => Ok(Ok(None)),
        };
    }
    let value = uri.text(part);
    if matches!(part, TrinoUrlPart::Host | TrinoUrlPart::Protocol) {
        let mut result = String::new();
        result
            .try_reserve_exact(value.len())
            .map_err(|source| super::string_failure::reservation::<u8>(value.len(), source))?;
        for ch in value.chars() {
            work.step()?;
            result.push(ch);
        }
        Ok(Ok(Some(result)))
    } else {
        Ok(TrinoUrlDecode::UriComponent.decode(value, work)?.map(Some))
    }
}
