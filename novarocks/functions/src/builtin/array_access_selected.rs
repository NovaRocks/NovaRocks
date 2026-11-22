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

//! Exact selected mapping for the ORIGINAL array element access author.
use super::{
    array_access_core::{AccessFailure, AccessRows, ArrayAccessInputs, lookup_observed},
    array_literal_core::CollectionObservation,
    collection_selected::{compact, copy_error, nullable_take_type, reserve},
};
use crate::{
    DataFailureReason, EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl,
    KernelFailure, RowDataError, ScalarCallContract, ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    selected_copy,
};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, ListArray};
use arrow_schema::DataType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::{alloc::Layout, cell::RefCell, sync::Arc};
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let [
        FunctionArgumentType::Value(list),
        FunctionArgumentType::Value(index),
    ] = contract.selected().argument_types.as_ref()
    else {
        return Err(invalid(
            "array element access requires its two exact value arguments",
        ));
    };
    observe()?;
    if list.logical_type() != ValueLogicalType::Physical
        || index.logical_type() != ValueLogicalType::Physical
        || !matches!(index.data_type(), DataType::Int32 | DataType::Int64)
    {
        return Err(invalid(
            "array element access requires a Physical List and the selected Int32/Int64 index profile",
        ));
    }
    let DataType::List(field) = list.data_type() else {
        return Err(invalid(
            "array element access requires its frozen List input",
        ));
    };
    let item = list
        .value_type()
        .child_type_at(0)
        .ok_or_else(|| invalid("array element access has an invalid frozen child identity"))?;
    let target = contract.result_type();
    if !target.nullable()
        || !FunctionValueType::borrowed_value_domains_equal_observed(
            target.value_type(),
            item,
            &mut observe,
        )?
        || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            target.data_type(),
            field.data_type(),
            &mut observe,
        )?
    {
        return Err(invalid(
            "array element access result differs from its real item field",
        ));
    }
    nullable_take_type(field.data_type(), &mut observe)
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    evaluate_with_index_policy(input, false, control)
}

pub(super) fn evaluate_with_index_policy<'a>(
    input: ScalarCallInput<'_, 'a>,
    trino_element_at: bool,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = RefCell::new(EvaluationCheckpoints::new(control));
    let result = (|| {
        validate_profile(input.contract(), || work.borrow_mut().step())?;
        let target = input.contract().result_type();
        let selection = input.selection();
        let [list, index] = input.arguments() else {
            return Err(invalid(
                "array element access requires two evaluated arguments",
            ));
        };
        Layout::array::<Option<u32>>(selection.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        let (normalized_index, errors) = if trino_element_at {
            normalize_element_at(*list, *index, selection, &work)?
        } else {
            (compact(*index, selection, &work)?, Box::default())
        };
        let mut observer = |event| match event {
            CollectionObservation::Step => work.borrow_mut().step(),
            CollectionObservation::OpaqueBoundary => work.borrow_mut().flush(),
        };
        let inputs =
            ArrayAccessInputs::new(list.array().clone(), normalized_index, None, &mut observer)
                .map_err(|error| match error {
                    AccessFailure::Control(cause) => cause,
                    AccessFailure::Operation(cause) => cause,
                })?;
        let values = lookup_observed(
            &inputs,
            selection,
            |ordinal, row| {
                let row = list.value_row(ordinal, row);
                if row >= list.array().len() {
                    return Err(internal(
                        "array element list mapping is outside its checked carrier",
                    ));
                }
                Ok(AccessRows {
                    list: row,
                    subscript: ordinal,
                    check: None,
                })
            },
            Some(target.data_type()),
            |values, indices| {
                selected_copy::preflight_original_take(
                    values,
                    selected_copy::CopyIndexView::UInt32(indices),
                    |boundary| {
                        if boundary {
                            work.borrow_mut().flush()
                        } else {
                            work.borrow_mut().step()
                        }
                    },
                )
                .map_err(copy_error)
            },
            &mut observer,
        )
        .map_err(|error| match error {
            AccessFailure::Control(cause) => cause,
            AccessFailure::Operation(cause) => cause,
        })?;
        SelectedValues::try_new(selection, target.data_type(), values, errors)
            .map_err(|_| internal("array element compact output violates its frozen contract"))
    })();
    work.into_inner().finish_result(result)
}
fn normalize_element_at(
    array: EvaluatedArgument<'_>,
    index: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    work: &RefCell<EvaluationCheckpoints<'_>>,
) -> Result<(ArrayRef, Box<[RowDataError]>), KernelFailure> {
    let list = array
        .array()
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| internal("element_at source is outside its prepared List profile"))?;
    let indices = index
        .array()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| internal("element_at index is outside its prepared BIGINT profile"))?;
    let mut normalized = reserve::<Option<i32>>(selection.len(), work)?;
    let mut errors = reserve::<RowDataError>(selection.len(), work)?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.borrow_mut().step()?;
        let array_row = array.value_row(ordinal, row);
        let index_row = index.value_row(ordinal, row);
        if array_row >= list.len() || index_row >= indices.len() {
            return Err(internal(
                "element_at selection is outside its prepared carriers",
            ));
        }
        if list.is_null(array_row) || indices.is_null(index_row) {
            normalized.push(None);
            continue;
        }
        let index = indices.value(index_row);
        let len = i64::from(list.value_length(array_row));
        let position = match index {
            0 => {
                errors.push(RowDataError::with_reason(
                    ordinal,
                    DataFailureReason::Domain(
                        novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                    ),
                    "SQL array indices start at 1",
                ));
                None
            }
            value if value > len || value < -len => None,
            value if value < 0 => i32::try_from(len + 1 + value).ok(),
            value => i32::try_from(value).ok(),
        };
        normalized.push(position);
    }
    work.borrow_mut().flush()?;
    Ok((
        Arc::new(Int32Array::from(normalized)),
        errors.into_boxed_slice(),
    ))
}

#[cfg(test)]
#[path = "array_access_selected_tests.rs"]
mod tests;
