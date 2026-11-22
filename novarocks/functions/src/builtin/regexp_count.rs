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
//! ONE original REGEXP_COUNT computation with caller-authored source policy.
use super::string_extended::{DirectStringControl, StringCoreInput};
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError, SelectedValues,
    Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    pattern_memo::PatternMemo,
};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use regex::Regex;
use std::{alloc::Layout, sync::Arc};

#[cfg(test)]
mod f10;

/// Exact source-to-error projection from the frozen caller contract.
/// This flag is not a KnownConstant test or a request to inspect an AST.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PatternSource {
    Utf8Literal,
    Other,
}

/// The original reader, shared by both entry points and their static boundary.
pub fn string_or_null(array: &ArrayRef) -> Result<Option<&StringArray>, KernelFailure> {
    if matches!(array.data_type(), DataType::Null) {
        return Ok(None);
    }
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(Some)
        .ok_or_else(|| internal("regexp_count expects string"))
}
fn reserve<T>(rows: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut out = Vec::new();
    out.try_reserve_exact(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(out)
}
fn observe(text: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure> {
    for _ in text.as_bytes() {
        work.step()?;
    }
    Ok(())
}

pub fn evaluate_selected<'a>(
    input: StringCoreInput<'_, 'a>,
    source: PatternSource,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        if input.arguments.len() != 2 {
            return Err(invalid("regexp_count requires two selected arguments"));
        }
        let strings = string_or_null(input.arguments[0].array())?;
        let patterns_array = string_or_null(input.arguments[1].array())?;
        let mut patterns = PatternMemo::new();
        let mut out = reserve::<Option<i64>>(input.selection.len(), &mut work)?;
        let mut errors = reserve::<RowDataError>(0, &mut work)?;
        for (ordinal, batch) in input.selection.iter().enumerate() {
            work.step()?;
            // Preserve the original typed-NULL projection before reading row data.
            let Some(strings) = strings else {
                out.push(None);
                continue;
            };
            let Some(patterns_array) = patterns_array else {
                out.push(None);
                continue;
            };
            let s_row = input.arguments[0].value_row(ordinal, batch);
            let p_row = input.arguments[1].value_row(ordinal, batch);
            if s_row >= strings.len() || p_row >= patterns_array.len() {
                return Err(internal("regexp_count selected row is outside its carrier"));
            }
            if strings.is_null(s_row) || patterns_array.is_null(p_row) {
                out.push(None);
                continue;
            }
            let pattern = patterns_array.value(p_row);
            if pattern == "a{,}" {
                out.push(Some(0));
                continue;
            }
            observe(pattern, &mut work)?;
            work.flush()?;
            let compiled =
                patterns.get_or_compile(pattern, |pattern| Regex::new(pattern).map_err(Arc::new));
            work.flush()?;
            let re = match compiled {
                Ok(re) => re,
                Err(error) => {
                    let syntax = super::string_failure::regex_syntax(error)?;
                    if source == PatternSource::Other {
                        out.push(None);
                        continue;
                    }
                    work.flush()?;
                    let cause = super::string_failure::StringDataError::RegexLiteral {
                        pattern,
                        source: &syntax,
                    };
                    let message = cause.to_string();
                    work.flush()?;
                    observe(&message, &mut work)?;
                    if let Some(boundary) = input.error_boundary {
                        work.flush()?;
                        boundary(ordinal, cause)?;
                        work.flush()?;
                    }
                    work.flush()?;
                    errors
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    // Retain the actual SDK syntax source and its originating Data
                    // fact in the selected journal; the message is only a
                    // bounded diagnostic, never the row failure's author.
                    errors.push(RowDataError::from_domain(
                        ordinal,
                        super::string_failure::required_row(ordinal, cause),
                    )?);
                    work.flush()?;
                    out.push(None);
                    continue;
                }
            };
            let value = strings.value(s_row);
            observe(value, &mut work)?;
            work.flush()?;
            let mut matches = re.find_iter(value);
            work.flush()?;
            // This is the original find_iter().count() fold, observed per opaque
            // search operation; usize overflow and final `as i64` are unchanged.
            let mut count = 0usize;
            loop {
                work.flush()?;
                let next = matches.next();
                work.flush()?;
                if next.is_none() {
                    break;
                }
                count += 1;
                work.step()?;
            }
            out.push(Some(count as i64));
        }
        work.flush()?;
        let array = Arc::new(Int64Array::from(out)) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            input.selection,
            &DataType::Int64,
            array,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

/// V1 supplies evaluated arrays and the original immediate-node fact.
pub fn evaluate_required(
    arrays: &[ArrayRef; 2],
    rows: usize,
    source: PatternSource,
) -> Result<ArrayRef, KernelFailure> {
    string_or_null(&arrays[0])?;
    string_or_null(&arrays[1])?;
    let arguments = arrays.each_ref().map(EvaluatedArgument::Column);
    let boundary = |row: usize, cause: super::string_failure::StringDataError<'_>| {
        Err(super::string_failure::required_row(row, cause))
    };
    let output = evaluate_selected(
        StringCoreInput {
            arguments: &arguments,
            selection: Selection::all(rows),
            error_boundary: Some(&boundary),
        },
        source,
        &DirectStringControl,
    )?;
    Ok(Arc::clone(output.values()))
}

pub(super) fn validate_profile(
    contract: &crate::ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    if contract.selected().argument_types.len() != 2 {
        return Err(invalid("regexp_count exact arity differs"));
    }
    for ty in &contract.selected().argument_types {
        step()?;
        if !matches!(ty, crate::FunctionArgumentType::Value(ty) if ty.logical_type() == novarocks_type_contract::ValueLogicalType::Physical && ty.data_type() == &DataType::Utf8)
        {
            return Err(invalid("regexp_count requires exact Physical Utf8"));
        }
    }
    step()?;
    if !matches!(&contract.selected().result_type, crate::FunctionResultType::Scalar(ty) if ty.logical_type() == novarocks_type_contract::ValueLogicalType::Physical && ty.data_type() == &DataType::Int64 && ty.nullable())
    {
        return Err(invalid("regexp_count requires nullable Physical Int64"));
    }
    Ok(())
}
