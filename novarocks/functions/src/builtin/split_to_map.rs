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

//! The installed TRUNCATE bindings, effects and preparation share each exact owner.

//! Selected Trino split_to_map with per-row parsing and failure attribution.
use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    Array, ArrayRef, StringArray,
    builder::{MapBuilder, MapFieldNames, StringBuilder},
};
use arrow_schema::{DataType, FieldRef, Fields};

use super::collection_arrow::{ArrowSite, arrow_failure};
use crate::{
    DataFailureReason, KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallInput,
    SelectedValues, kernel_control::invalid, kernel_input::EvaluationCheckpoints,
};

pub(super) fn result_fields(result: &DataType) -> Result<(&FieldRef, &Fields), KernelFailure> {
    let DataType::Map(entries, false) = result else {
        return Err(invalid("split_to_map requires its declared unsorted MAP"));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(invalid("split_to_map result requires Struct entries"));
    };
    if fields.len() != 2
        || fields
            .iter()
            .any(|field| field.data_type() != &DataType::Utf8)
    {
        return Err(invalid(
            "split_to_map requires VARCHAR key and value carriers",
        ));
    }
    Ok((entries, fields))
}

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let [string, entries, pairs] = input.arguments() else {
        return Err(invalid("split_to_map requires STRING and two delimiters"));
    };
    let arguments = [*string, *entries, *pairs];
    let arrays = arguments.map(|argument| argument.array().as_any().downcast_ref::<StringArray>());
    let [Some(strings), Some(entry_delimiters), Some(pair_delimiters)] = arrays else {
        return Err(invalid("split_to_map received a non-VARCHAR carrier"));
    };
    let arrays = [strings, entry_delimiters, pair_delimiters];
    let target = input.contract().result_type().data_type();
    let (entries, fields) = result_fields(target)?;
    let selection = input.selection();
    let mut work = EvaluationCheckpoints::new(control);
    work.flush()?;
    let names = MapFieldNames {
        entry: entries.name().clone(),
        key: fields[0].name().clone(),
        value: fields[1].name().clone(),
    };
    super::collection_arrow::list_offset_entries(selection.len())?;
    let mut result = MapBuilder::with_capacity(
        Some(names),
        StringBuilder::new(),
        StringBuilder::new(),
        selection.len(),
    )
    .with_keys_field(Arc::clone(&fields[0]))
    .with_values_field(Arc::clone(&fields[1]));
    let mut errors = Vec::new();
    let mut entry_count = 0;
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let indices = arguments.map(|argument| argument.value_row(ordinal, row));
        let row_valid = match arrays
            .into_iter()
            .zip(indices)
            .all(|(array, index)| array.is_valid(index))
        {
            false => false,
            true => {
                let [string, entry_delimiter, pair_delimiter] =
                    std::array::from_fn(|index| arrays[index].value(indices[index]));
                match split_row(string, entry_delimiter, pair_delimiter, &mut work)? {
                    Ok(pairs) => {
                        entry_count = crate::aggregate_scalar::checked_map_output_offset(
                            entry_count,
                            pairs.len(),
                        )?;
                        for (key, value) in pairs {
                            work.step()?;
                            append_pair(&mut result, key, value)?;
                        }
                        true
                    }
                    Err(message) => {
                        errors
                            .try_reserve(1)
                            .map_err(|_| KernelFailure::ResourceExhausted)?;
                        errors.push(RowDataError::with_reason(
                            ordinal,
                            DataFailureReason::Domain(
                                novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                            ),
                            message,
                        ));
                        false
                    }
                }
            }
        };
        result
            .append(row_valid)
            .map_err(|source| arrow_failure("split_to_map", ArrowSite::Construct, source))?;
    }
    work.flush()?;
    let result: ArrayRef = Arc::new(result.finish());
    let output = SelectedValues::try_new_observed::<KernelFailure>(
        selection,
        target,
        result,
        errors.into_boxed_slice(),
        || work.step(),
    )?;
    work.flush()?;
    Ok(output)
}

type SplitPairs<'a> = Vec<(&'a str, &'a str)>;

fn split_row<'a>(
    input: &'a str,
    entry_delimiter: &str,
    pair_delimiter: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<SplitPairs<'a>, &'static str>, KernelFailure> {
    if entry_delimiter.is_empty() {
        return Ok(Err("entryDelimiter is empty"));
    }
    if pair_delimiter.is_empty() {
        return Ok(Err("keyValueDelimiter is empty"));
    }
    if entry_delimiter == pair_delimiter {
        return Ok(Err(
            "entryDelimiter and keyValueDelimiter must not be the same",
        ));
    }
    let mut pairs = Vec::new();
    let mut keys = HashSet::new();
    let invalid_entry = "Key-value delimiter must appear exactly once in each entry";
    for entry in input.split_terminator(entry_delimiter) {
        work.step()?;
        let Some((key, value)) = entry.split_once(pair_delimiter) else {
            return Ok(Err(invalid_entry));
        };
        if value.contains(pair_delimiter) {
            return Ok(Err(invalid_entry));
        }
        keys.try_reserve(1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        if !keys.insert(key) {
            return Ok(Err("Duplicate keys are not allowed"));
        }
        pairs
            .try_reserve(1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        pairs.push((key, value));
    }
    Ok(Ok(pairs))
}

fn append_pair(
    output: &mut MapBuilder<StringBuilder, StringBuilder>,
    key: &str,
    value: &str,
) -> Result<(), KernelFailure> {
    let (keys, values) = output.entries();
    for (used, extra) in [
        (keys.values_slice().len(), key.len()),
        (values.values_slice().len(), value.len()),
    ] {
        let offset = i32::try_from(used)
            .map_err(|_| invalid("split_to_map exceeded its checked VARCHAR extent"))?;
        crate::aggregate_scalar::checked_utf8_output_offset(offset, extra)?;
    }
    keys.append_value(key);
    values.append_value(value);
    Ok(())
}
