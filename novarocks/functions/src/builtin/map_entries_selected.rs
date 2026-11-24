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
//! Exact selected projection over the ONE original MAP_ENTRIES author.
use super::{
    array_literal_core::CollectionObservation,
    collection_selected::{copy_error, reserve},
    map_entries_core::{MapEntriesFailure, project_observed},
};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallContract,
    ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    selected_copy,
};
use arrow_array::{Array, UInt64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::cell::RefCell;
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let [FunctionArgumentType::Value(source)] = contract.selected().argument_types.as_ref() else {
        return Err(invalid("map_entries requires its one exact value argument"));
    };
    observe()?;
    let DataType::Map(entries, _) = source.data_type() else {
        return Err(invalid("map_entries requires its real Map source"));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(invalid("map_entries requires its real Struct entries"));
    };
    let result = contract.result_type();
    let DataType::List(item) = result.data_type() else {
        return Err(invalid("map_entries requires its real List result"));
    };
    let DataType::Struct(output) = item.data_type() else {
        return Err(invalid("map_entries requires its real Struct item"));
    };
    observe()?;
    if source.logical_type() != ValueLogicalType::Physical
        || result.logical_type() != ValueLogicalType::Physical
        || !result.nullable()
        || fields.len() != 2
        || output.len() != 2
    {
        return Err(invalid(
            "map_entries differs from its exact Physical Map to nullable List profile",
        ));
    }
    let output_semantic = result
        .value_type()
        .child_type_at(0)
        .ok_or_else(|| invalid("map_entries has no semantic result item"))?;
    let novarocks_type_contract::ValueKind::Struct(declared) = output_semantic.kind() else {
        return Err(invalid("map_entries result item is not a semantic ROW"));
    };
    if item.is_nullable() || declared.iter().any(|field| field.row_name().is_some()) {
        return Err(invalid(
            "map_entries requires an anonymous, nonnull ROW item",
        ));
    }
    for (index, (source_field, target)) in fields.iter().zip(output.iter()).enumerate() {
        let a = source
            .value_type()
            .child_type_at(index)
            .ok_or_else(|| invalid("map_entries source semantic child is absent"))?;
        let b = output_semantic
            .child_type_at(index)
            .ok_or_else(|| invalid("map_entries result semantic child is absent"))?;
        if !FunctionValueType::borrowed_value_domains_equal_observed(a, b, &mut observe)?
            || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                source_field.data_type(),
                target.data_type(),
                &mut observe,
            )?
        {
            return Err(invalid(
                "map_entries result differs from its actual key/value domains",
            ));
        }
    }
    Ok(())
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = RefCell::new(EvaluationCheckpoints::new(control));
    let result = (|| {
        validate_profile(input.contract(), || work.borrow_mut().step())?;
        let [argument] = input.arguments() else {
            return Err(invalid("map_entries requires one evaluated Map argument"));
        };
        let target = input.contract().result_type();
        let selection = input.selection();
        let mut observer = |event| match event {
            CollectionObservation::Step => work.borrow_mut().step(),
            CollectionObservation::OpaqueBoundary => work.borrow_mut().flush(),
        };
        // Preserve the original complete carrier/target conversion before exact selected copying.
        let projection =
            project_observed(argument.array(), Some(target.data_type()), &mut observer).map_err(
                |e| match e {
                    MapEntriesFailure::Control(cause) => cause,
                    MapEntriesFailure::Operation(cause) => cause,
                },
            )?;
        let mut indices = reserve::<Option<u64>>(selection.len(), &work)?;
        let mut contiguous_start = None::<usize>;
        let mut contiguous = true;
        for (ordinal, row) in selection.iter().enumerate() {
            work.borrow_mut().step()?;
            let row = argument.value_row(ordinal, row);
            if row >= projection.len() {
                return Err(internal(
                    "map_entries address is outside its checked carrier",
                ));
            }
            match contiguous_start {
                None => contiguous_start = Some(row),
                Some(start) => contiguous &= start.checked_add(ordinal) == Some(row),
            }
            indices.push(Some(
                u64::try_from(row).map_err(|_| KernelFailure::ResourceExhausted)?,
            ));
        }
        let values = if contiguous {
            // This is the actual checked row-address recipe, not a type/name fallback.
            // A slice has exactly selection.len rows and retains original nested backing.
            work.borrow_mut().flush()?;
            let values = projection.slice(contiguous_start.unwrap_or(0), selection.len());
            work.borrow_mut().flush()?;
            values
        } else {
            selected_copy::preflight_take(projection.as_ref(), &indices, |boundary| {
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
                arrow_select::take::take(projection.as_ref(), &indices, None).map_err(|cause| {
                    internal(&format!(
                        "map_entries exact selected projection failed: {cause}"
                    ))
                })?;
            work.borrow_mut().flush()?;
            values
        };
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            target.data_type(),
            values,
            Box::default(),
            || work.borrow_mut().step(),
        )
    })();
    work.into_inner().finish_result(result)
}
