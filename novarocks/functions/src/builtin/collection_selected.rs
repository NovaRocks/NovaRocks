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
//! Exact selected adapters for ONE original constructor and map lookup cores.
use super::{
    array_literal_core::{ArrayLiteralInputs, CollectionObservation, ConstructionFailure},
    map_lookup_core::{LookupRows, MapLookupFailure, MapLookupInputs, lookup_observed},
};
use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallContract, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    selected_copy::{self, CopyError},
};
use arrow_array::{ArrayRef, UInt64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::{alloc::Layout, cell::RefCell};
#[derive(Clone, Copy, Debug)]
pub(super) enum Operation {
    ArrayLiteral,
    MapLookup,
}
pub(super) fn copy_error(error: CopyError) -> KernelFailure {
    match error {
        CopyError::Control(cause) => cause,
        CopyError::Extent => KernelFailure::ResourceExhausted,
        other => internal(&format!("selected collection copy: {other}")),
    }
}
fn value<'a>(arg: &'a FunctionArgumentType) -> Result<&'a FunctionValueType, KernelFailure> {
    match arg {
        FunctionArgumentType::Value(value) => Ok(value),
        _ => Err(invalid("collection call requires exact value arguments")),
    }
}
pub(super) fn nullable_take_type(
    ty: &DataType,
    observe: &mut dyn FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    observe()?;
    match ty {
        DataType::Union(..) | DataType::RunEndEncoded(..) => Err(invalid(
            "map lookup has no installed nullable-take profile for Union or RunEndEncoded values",
        )),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => nullable_take_type(field.data_type(), observe),
        DataType::Struct(fields) => {
            for field in fields {
                nullable_take_type(field.data_type(), observe)?;
            }
            Ok(())
        }
        DataType::Dictionary(_, field) => nullable_take_type(field, observe),
        _ => Ok(()),
    }
}
pub(super) fn validate_profile(
    operation: Operation,
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let target = contract.result_type();
    let args = &contract.selected().argument_types;
    match operation {
        Operation::ArrayLiteral => {
            if target.logical_type() != ValueLogicalType::Physical || target.nullable() {
                return Err(invalid(
                    "array literal requires its exact non-null List result",
                ));
            }
            let DataType::List(field) = target.data_type() else {
                return Err(invalid(
                    "array literal result must be its frozen List field",
                ));
            };
            let item = target
                .value_type()
                .child_type_at(0)
                .ok_or_else(|| invalid("array literal has no frozen semantic item"))?;
            for argument in args.iter() {
                observe()?;
                let source = value(argument)?;
                if !FunctionValueType::borrowed_value_domains_equal_observed(
                    source.value_type(),
                    item,
                    &mut observe,
                )? || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                    source.data_type(),
                    field.data_type(),
                    &mut observe,
                )? {
                    return Err(invalid(
                        "array literal requires already-materialized exact item arguments",
                    ));
                }
            }
        }
        Operation::MapLookup => {
            let [map, key] = args.as_ref() else {
                return Err(invalid(
                    "map element_at has exactly two selected value arguments",
                ));
            };
            let map = value(map)?;
            let key = value(key)?;
            if map.logical_type() != ValueLogicalType::Physical || !target.nullable() {
                return Err(invalid(
                    "map element_at requires a Physical Map and nullable result",
                ));
            }
            let DataType::Map(entries, _) = map.data_type() else {
                return Err(invalid("map element_at requires its frozen Map input"));
            };
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(invalid("map element_at requires frozen Struct entries"));
            };
            if fields.len() != 2 {
                return Err(invalid("map element_at requires exactly two entry fields"));
            }
            let key_semantic = map
                .value_type()
                .child_type_at(0)
                .ok_or_else(|| invalid("map element_at has no frozen semantic key"))?;
            let value_semantic = map
                .value_type()
                .child_type_at(1)
                .ok_or_else(|| invalid("map element_at has no frozen semantic value"))?;
            let independent_probe = super::map_lookup_core::has_uninhabited_map_key(map);
            observe()?;
            let key_matches = independent_probe
                || (FunctionValueType::borrowed_value_domains_equal_observed(
                    key.value_type(),
                    key_semantic,
                    &mut observe,
                )? && novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                    key.data_type(),
                    fields[0].data_type(),
                    &mut observe,
                )?);
            if !key_matches
                || !FunctionValueType::borrowed_value_domains_equal_observed(
                    target.value_type(),
                    value_semantic,
                    &mut observe,
                )?
                || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                    target.data_type(),
                    fields[1].data_type(),
                    &mut observe,
                )?
            {
                return Err(invalid(
                    "map element_at selected domains differ from its real key/value fields",
                ));
            }
            nullable_take_type(fields[1].data_type(), &mut observe)?;
        }
    }
    Ok(())
}
pub(super) fn reserve<T>(
    len: usize,
    work: &RefCell<EvaluationCheckpoints<'_>>,
) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(len).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.borrow_mut().flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.borrow_mut().flush()?;
    Ok(values)
}
pub(super) fn compact(
    argument: EvaluatedArgument<'_>,
    selection: crate::Selection<'_>,
    work: &RefCell<EvaluationCheckpoints<'_>>,
) -> Result<ArrayRef, KernelFailure> {
    let mut indices = reserve::<Option<u64>>(selection.len(), work)?;
    for (ordinal, row) in selection.iter().enumerate() {
        work.borrow_mut().step()?;
        let row = argument.value_row(ordinal, row);
        if row >= argument.array().len() {
            return Err(internal(
                "collection argument mapping is outside its checked carrier",
            ));
        }
        indices.push(Some(
            u64::try_from(row).map_err(|_| KernelFailure::ResourceExhausted)?,
        ));
    }
    selected_copy::preflight_take(argument.array().as_ref(), &indices, |boundary| {
        if boundary {
            work.borrow_mut().flush()
        } else {
            work.borrow_mut().step()
        }
    })
    .map_err(copy_error)?;
    work.borrow_mut().flush()?;
    let indices = UInt64Array::from(indices);
    let values =
        arrow_select::take::take(argument.array().as_ref(), &indices, None).map_err(|cause| {
            internal(&format!(
                "collection exact selected projection failed: {cause}"
            ))
        })?;
    work.borrow_mut().flush()?;
    Ok(values)
}
pub(super) fn evaluate<'a>(
    operation: Operation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = RefCell::new(EvaluationCheckpoints::new(control));
    let result = (|| {
        validate_profile(operation, input.contract(), || work.borrow_mut().step())?;
        let mut observer = |event| match event {
            CollectionObservation::Step => work.borrow_mut().step(),
            CollectionObservation::OpaqueBoundary => work.borrow_mut().flush(),
        };
        let target = input.contract().result_type();
        let selection = input.selection();
        match operation {
            Operation::ArrayLiteral => {
                let rows_plus_one = selection
                    .len()
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                Layout::array::<i32>(rows_plus_one)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                let slots = selection
                    .len()
                    .checked_mul(input.arguments().len())
                    .ok_or(KernelFailure::ResourceExhausted)?;
                i32::try_from(slots).map_err(|_| KernelFailure::ResourceExhausted)?;
                let mut inputs = ArrayLiteralInputs::from_output(
                    Some(target.data_type()),
                    selection.len(),
                    input.arguments().len(),
                    &mut observer,
                )
                .map_err(|failure| match failure {
                    ConstructionFailure::Control(cause) => cause,
                    ConstructionFailure::Data(error) => internal(&error.legacy_message()),
                })?;
                for argument in input.arguments() {
                    work.borrow_mut().step()?;
                    let array = compact(*argument, selection, &work)?;
                    inputs.push_evaluated(array, &mut observer).map_err(
                        |failure| match failure {
                            ConstructionFailure::Control(cause) => cause,
                            ConstructionFailure::Data(error) => internal(&error.legacy_message()),
                        },
                    )?;
                }
                let values = inputs
                    .finish(
                        |_, _| {
                            Err(
                                "exact selected array literal cannot invoke an implicit coercion"
                                    .to_string(),
                            )
                        },
                        &mut observer,
                    )
                    .map_err(|failure| match failure {
                        ConstructionFailure::Control(cause) => cause,
                        ConstructionFailure::Data(error) => internal(&error.legacy_message()),
                    })?;
                SelectedValues::try_new(selection, target.data_type(), values, Box::default())
                    .map_err(|_| {
                        internal("array literal compact result violates its checked contract")
                    })
            }
            Operation::MapLookup => {
                let [map, key] = input.arguments() else {
                    return Err(invalid("map lookup requires two evaluated arguments"));
                };
                let inputs = MapLookupInputs::new(map.array(), key.array(), None)
                    .map_err(|error| internal(&error))?;
                Layout::array::<Option<u32>>(selection.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                let mut errors = reserve::<RowDataError>(selection.len(), &work)?;
                let values = lookup_observed(
                    &inputs,
                    selection,
                    |ordinal, row| {
                        let map_row = map.value_row(ordinal, row);
                        let key_row = key.value_row(ordinal, row);
                        if map_row >= map.array().len() || key_row >= key.array().len() {
                            return Err(internal(
                                "map lookup argument mapping is outside its checked carrier",
                            ));
                        }
                        Ok(LookupRows {
                            map: map_row,
                            key: key_row,
                            check: None,
                        })
                    },
                    Some(target.data_type()),
                    |values, indices| {
                        let mut exact_indices = reserve::<Option<u64>>(indices.len(), &work)?;
                        for index in indices {
                            work.borrow_mut().step()?;
                            exact_indices.push(index.map(u64::from));
                        }
                        selected_copy::preflight_take(values, &exact_indices, |boundary| {
                            if boundary {
                                work.borrow_mut().flush()
                            } else {
                                work.borrow_mut().step()
                            }
                        })
                        .map_err(copy_error)
                    },
                    |ordinal, message| {
                        errors.push(RowDataError::new(ordinal, &message));
                        Ok(())
                    },
                    &mut observer,
                )
                .map_err(|failure| match failure {
                    MapLookupFailure::Control(cause) => cause,
                    MapLookupFailure::Data(message) => internal(&message),
                })?;
                SelectedValues::try_new(
                    selection,
                    target.data_type(),
                    values,
                    errors.into_boxed_slice(),
                )
                .map_err(|_| {
                    internal("map lookup compact values/errors violate its checked contract")
                })
            }
        }
    })();
    work.into_inner().finish_result(result)
}

#[cfg(test)]
#[path = "collection_selected_tests.rs"]
mod tests;
