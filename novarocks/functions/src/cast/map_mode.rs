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

//! A selected MAP conversion, with the whole output row as the error boundary.

use super::*;
use crate::{DataFailureReason, Selection};
use arrow_array::{ArrayRef, MapArray, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use novarocks_type_contract::{KeyComparison, KeyRelation, ValueKind};
use novarocks_value_arrays::{
    ValueFailure,
    hash_eq::{KeyPreparationFailure, KeyRows, PreparedKeyOperations},
};
use std::{alloc::Layout, cell::RefCell, collections::HashMap, ops::Range, sync::Arc};

pub(super) fn evaluate_row(
    recipe: &PreparedCastRecipe,
    input: &ArrayRef,
    row: usize,
    ordinal: usize,
    control: &dyn KernelEvaluationControl,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<ArrayRef, RowDataError>, KernelFailure> {
    let map = input
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| invalid("map CAST has a foreign concrete carrier"))?;
    let offsets = map.value_offsets();
    let start = usize::try_from(offsets[row]).map_err(|_| invalid("negative map CAST offset"))?;
    let end = usize::try_from(offsets[row + 1]).map_err(|_| invalid("negative map CAST offset"))?;
    if start > end || end > map.entries().len() {
        return Err(invalid("map CAST offsets exceed its entries"));
    }
    let CastChildren::Map(children) = &recipe.children else {
        return Err(internal("map CAST lost the prepared child recipes"));
    };
    let count = end - start;
    work.flush()?;
    if count == 0 {
        return assemble_map(
            recipe.result.data_type(),
            arrow_array::new_empty_array(children[0].result.data_type()),
            arrow_array::new_empty_array(children[1].result.data_type()),
            work,
        )
        .map(Ok);
    }
    let keys = map.keys().slice(start, count);
    let key_output = children[0].evaluate_selected(
        EvaluatedArgument::Column(&keys),
        Selection::all(count),
        &[],
        control,
    )?;
    if let Some(error) = key_output.errors().first() {
        return Ok(Err(error.with_selected_ordinal(ordinal)));
    }
    for position in 0..count {
        if logical_is_null(key_output.values().as_ref(), position, 1, work)? {
            return Ok(Err(row_error(ordinal, "map key is null")));
        }
    }
    work.flush()?;
    let source_values = map.values().slice(start, count);
    let value_output = children[1].evaluate_selected(
        EvaluatedArgument::Column(&source_values),
        Selection::all(count),
        &[],
        control,
    )?;
    if let Some(error) = value_output.errors().first() {
        return Ok(Err(error.with_selected_ordinal(ordinal)));
    }
    if converted_keys_have_duplicate(&children[0].result, key_output.values(), 0..count, work)? {
        return Ok(Err(duplicate_map_key_error(ordinal)));
    }
    assemble_map(
        recipe.result.data_type(),
        Arc::clone(key_output.values()),
        Arc::clone(value_output.values()),
        work,
    )
    .map(Ok)
}

fn assemble_map(
    output: &DataType,
    keys: ArrayRef,
    values: ArrayRef,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let DataType::Map(entry_field, sorted) = output else {
        return Err(internal("map CAST lost its frozen target layout"));
    };
    let DataType::Struct(fields) = entry_field.data_type() else {
        return Err(internal("map CAST target has foreign entries"));
    };
    let count = i32::try_from(keys.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
    let arrow_failure = |cause| {
        crate::control_values::AssemblyFailure::arrow("map CAST child assembly", cause)
            .into_kernel()
    };
    work.flush()?;
    let entries =
        StructArray::try_new(fields.clone(), vec![keys, values], None).map_err(arrow_failure)?;
    let result = MapArray::try_new(
        Arc::clone(entry_field),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, count])),
        entries,
        None,
        *sorted,
    )
    .map_err(arrow_failure)?;
    work.flush()?;
    Ok(Arc::new(result))
}

pub(crate) fn duplicate_map_key_error(ordinal: usize) -> RowDataError {
    row_error(ordinal, "duplicate keys")
}

fn row_error(ordinal: usize, detail: &'static str) -> RowDataError {
    RowDataError::with_reason(
        ordinal,
        DataFailureReason::Domain(
            novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT,
        ),
        detail,
    )
}

pub(crate) fn converted_keys_have_duplicate(
    declaration: &FunctionValueType,
    keys: &ArrayRef,
    rows: Range<usize>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    if rows.is_empty() {
        return Ok(false);
    }
    if matches!(
        declaration.value_type().kind(),
        ValueKind::Scalar(DataType::Null)
    ) {
        return Err(invalid("nonempty map CAST has unknown keys"));
    }
    let mut latest = HashMap::new();
    let mut previous = Vec::new();
    Layout::array::<Option<usize>>(rows.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    latest
        .try_reserve(rows.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    previous
        .try_reserve_exact(rows.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;

    let work = RefCell::new(work);
    let mut observe = |units: usize| {
        for _ in 0..units {
            work.borrow_mut().step()?;
        }
        Ok(())
    };
    let mut growth = |_| work.borrow_mut().flush();
    let operations =
        PreparedKeyOperations::prepare_observed(declaration, KeyRelation::Identical, &mut observe)
            .map_err(|error| match error {
                KeyPreparationFailure::Caller(failure) => failure,
                KeyPreparationFailure::Unsupported(_) => {
                    internal("map CAST key relation differs from prepared type")
                }
            })?;
    let bound = operations
        .bind_observed(keys, KeyRows::Range(rows.clone()), &mut observe)
        .map_err(value_failure)?;
    let first = rows.start;
    for row in rows {
        let hash = operations
            .hash_observed(&bound, row, &mut observe)
            .map_err(value_failure)?;
        let head = latest.insert(hash, row);
        let mut candidate = head;
        while let Some(prior) = candidate {
            match operations
                .compare_observed(&bound, prior, &bound, row, &mut growth, &mut observe)
                .map_err(value_failure)?
            {
                KeyComparison::True => return Ok(true),
                KeyComparison::False => {}
                _ => return Err(internal("map CAST key identity is not total")),
            }
            candidate = previous[prior - first];
        }
        previous.push(head);
    }
    Ok(false)
}

fn value_failure(error: ValueFailure<KernelFailure>) -> KernelFailure {
    match error {
        ValueFailure::Value(error) => KernelFailure::from_domain(error),
        ValueFailure::Caller(error) => error,
    }
}
