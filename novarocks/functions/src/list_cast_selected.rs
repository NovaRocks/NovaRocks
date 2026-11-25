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

//! Selected adapters for the observed original List CAST profiles. COPY remains
//! the sole representation author; no capacity grant is inferred from preflight.
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, RowDataError, SelectedValues,
    Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    list_cast_core::{self, ListCastError, ListCastObservation},
    selected_copy::{self, CopyError},
};
use arrow_array::{Array, ListArray, UInt64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::{alloc::Layout, cell::RefCell};

/// Admit the observed carrier profiles and the explicit NULL-to-JSON list
/// conversion. The complete target Field authors the output layout. Lifting
/// to LargeBinary requires a nullable nominal JSON child in the target.
pub(crate) fn profile(source: &FunctionValueType, result: &FunctionValueType) -> bool {
    if novarocks_type_contract::is_null_list_to_json_cast(source, result) {
        return true;
    }
    let (DataType::List(source_item), DataType::List(target_item)) =
        (source.data_type(), result.data_type())
    else {
        return false;
    };
    let (Some(source_semantic), Some(target_semantic)) = (
        source.value_type().child_type_at(0),
        result.value_type().child_type_at(0),
    ) else {
        return false;
    };
    source.logical_type() == ValueLogicalType::Physical
        && result.logical_type() == ValueLogicalType::Physical
        && source_semantic.domain().is_none()
        && target_semantic.domain().is_none()
        && target_semantic.nullable()
        && matches!(
            (source_item.data_type(), target_item.data_type()),
            (DataType::Null, DataType::Int32 | DataType::Utf8) | (DataType::Utf8, DataType::Utf8)
        )
}
fn copy_error(error: CopyError) -> KernelFailure {
    error.into_kernel()
}
fn reserve<T>(
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
pub(crate) fn evaluate<'a>(
    source: &FunctionValueType,
    result: &FunctionValueType,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'a>,
    inherited: &[RowDataError],
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = RefCell::new(EvaluationCheckpoints::new(control));
    let outcome = (|| {
        match argument {
            EvaluatedArgument::SelectedColumn(values) => {
                if !values
                    .selection()
                    .same_rows_observed(selection, || work.borrow_mut().step())?
                    || values.values().len() != selection.len()
                    || !std::ptr::eq(inherited, values.errors())
                {
                    return Err(invalid(
                        "List CAST requires its exact selected address and inherited journal",
                    ));
                }
            }
            _ => {
                if !inherited.is_empty() {
                    return Err(invalid("List CAST has a foreign inherited journal"));
                }
                argument.validate_shape_observed::<KernelFailure>(selection, || {
                    work.borrow_mut().step()
                })?;
            }
        }
        if let Some(value) = argument.constant_value() {
            if !value
                .value_type()
                .fits_value_type_observed(source, || work.borrow_mut().step())?
            {
                return Err(invalid(
                    "List CAST constant differs from its frozen logical identity",
                ));
            }
            work.borrow_mut().step()?;
        }
        if !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            argument.array().data_type(),
            source.data_type(),
            || work.borrow_mut().step(),
        )? {
            return Err(invalid(
                "List CAST argument differs from its frozen Field metadata",
            ));
        }
        let list = argument
            .array()
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("List CAST has a foreign concrete List carrier"))?;
        let mut indices = reserve::<Option<u64>>(selection.len(), &work)?;
        let mut inherited_cursor = inherited.iter().peekable();
        for (ordinal, logical_row) in selection.iter().enumerate() {
            work.borrow_mut().step()?;
            if inherited_cursor
                .peek()
                .is_some_and(|error| error.selected_ordinal() == ordinal)
            {
                inherited_cursor.next();
                indices.push(None);
                continue;
            }
            let row = argument.value_row(ordinal, logical_row);
            if row >= list.len() {
                return Err(invalid("List CAST address is outside its carrier"));
            }
            if !source.nullable() && list.is_null(row) {
                return Err(invalid(
                    "non-null List CAST argument contains a selected NULL",
                ));
            }
            indices.push(Some(
                u64::try_from(row).map_err(|_| KernelFailure::ResourceExhausted)?,
            ));
        }
        if inherited_cursor.next().is_some() {
            return Err(invalid("List CAST journal is outside its selected domain"));
        }
        selected_copy::preflight_take(list, &indices, |boundary| {
            if boundary {
                work.borrow_mut().flush()
            } else {
                work.borrow_mut().step()
            }
        })
        .map_err(copy_error)?;
        work.borrow_mut().flush()?;
        let indices = UInt64Array::from(indices);
        let compact = arrow_select::take::take(list, &indices, None)
            .map_err(|cause| internal(&format!("checked List CAST projection failed: {cause}")))?;
        work.borrow_mut().flush()?;
        let DataType::List(target_field) = result.data_type() else {
            return Err(internal("prepared List CAST target is not a List"));
        };
        let null_lift_profile = profile(source, result);
        let mut observe = |event| match event {
            ListCastObservation::Step => work.borrow_mut().step(),
            ListCastObservation::OpaqueBoundary => work.borrow_mut().flush(),
        };
        let out =
            list_cast_core::cast_observed(
                &compact,
                target_field,
                &mut |child, target| {
                    if child.data_type() != &DataType::Null
                        || target != target_field.data_type()
                        || !null_lift_profile
                    {
                        return Err(ListCastError::Host(internal(
                            "prepared List CAST has a foreign child operation",
                        )));
                    }
                    list_cast_core::null_source_observed(child.len(), target, &mut |event| {
                        match event {
                            ListCastObservation::Step => work.borrow_mut().step(),
                            ListCastObservation::OpaqueBoundary => work.borrow_mut().flush(),
                        }
                    })
                },
                &mut observe,
            )
            .map_err(|cause| match cause {
                ListCastError::Host(cause) => cause,
                ListCastError::Domain(cause) => KernelFailure::from_domain(cause),
            })?;
        let mut errors = reserve::<RowDataError>(inherited.len(), &work)?;
        for error in inherited {
            work.borrow_mut().flush()?;
            errors.push(error.clone());
            work.borrow_mut().flush()?;
        }
        work.borrow_mut().flush()?;
        SelectedValues::try_new_observed(
            selection,
            result.data_type(),
            out,
            errors.into_boxed_slice(),
            || work.borrow_mut().step(),
        )
    })();
    // A refusal latched by the actual control is returned without a footer.
    work.into_inner().finish_result(outcome)
}
