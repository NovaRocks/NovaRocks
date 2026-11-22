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

//! Original SPLIT computation shared by full-array v1 and selected calls.
use super::string_substring::StringIntegerArray;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError, ScalarCallContract,
    ScalarCallInput, SelectedValues, Selection,
};
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::{DataType, FieldRef};
use novarocks_type_contract::{FunctionIntrinsicRowError, ValueLogicalType};
use std::sync::Arc;
#[derive(Clone, Copy, Debug)]
pub enum Observation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
enum SplitContract {
    FirstStringRequired,
    SecondStringRequired,
    LengthMismatch,
}
impl std::fmt::Display for SplitContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FirstStringRequired => {
                f.write_str("split: first argument must be a string array")
            }
            Self::SecondStringRequired => {
                f.write_str("split: second argument must be a string array")
            }
            Self::LengthMismatch => f.write_str("split: argument length mismatch"),
        }
    }
}
impl std::error::Error for SplitContract {}
impl novarocks_failure_contract::FailureProjection for SplitContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(368).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for SplitContract {}
// Check the actual List and child Utf8 formats before either builder appends.
// No representable input prefix or memory grant is inferred from these facts.
fn append_piece(
    builder: &mut ListBuilder<StringBuilder>,
    piece: &str,
    row_children: &mut usize,
    child_offset: &mut i32,
    byte_offset: &mut i32,
) -> Result<(), KernelFailure> {
    // Every previously successful append bounded this count by i32::MAX.
    let next_row = *row_children + 1;
    crate::aggregate_scalar::checked_list_output_offset(0, next_row)?;
    let next_child = crate::aggregate_scalar::checked_list_output_offset(*child_offset, 1)?;
    let next_byte = crate::aggregate_scalar::checked_utf8_output_offset(*byte_offset, piece.len())?;
    builder.values().append_value(piece);
    *row_children = next_row;
    *child_offset = next_child;
    *byte_offset = next_byte;
    Ok(())
}
fn sources<'a>(
    left: &'a dyn Array,
    right: &'a dyn Array,
) -> Result<(&'a StringArray, &'a StringArray), KernelFailure> {
    let left = left
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| KernelFailure::from_domain(SplitContract::FirstStringRequired))?;
    let right = right
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| KernelFailure::from_domain(SplitContract::SecondStringRequired))?;
    Ok((left, right))
}
fn evaluate_rows(
    left: &StringArray,
    right: &StringArray,
    selection: Selection<'_>,
    mut address: impl FnMut(usize, usize) -> (usize, usize),
    limit: impl Fn(usize, usize) -> Result<Option<i64>, KernelFailure>,
    trino: bool,
    result_field: Option<&FieldRef>,
    observe: &mut impl FnMut(Observation) -> Result<(), KernelFailure>,
) -> Result<(ArrayRef, Box<[RowDataError]>), KernelFailure> {
    observe(Observation::OpaqueBoundary)?;
    let value_builder = StringBuilder::new();
    let mut list_builder = ListBuilder::new(value_builder);
    if let Some(field) = result_field {
        list_builder = list_builder.with_field(Arc::clone(field));
    }
    let mut child_offset = 0i32;
    let mut byte_offset = 0i32;
    let mut errors = Vec::new();
    observe(Observation::OpaqueBoundary)?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        let (row, delimiter_row) = address(ordinal, batch_row);
        observe(Observation::Step)?;
        if row >= left.len() || delimiter_row >= right.len() {
            return Err(internal("split selected address is out of bounds"));
        }
        if left.is_null(row) || right.is_null(delimiter_row) {
            observe(Observation::OpaqueBoundary)?;
            list_builder.append(false);
            observe(Observation::OpaqueBoundary)?;
            continue;
        }
        let mut row_children = 0usize;
        let haystack = left.value(row);
        let delimiter = right.value(delimiter_row);
        let Some(limit) = limit(ordinal, batch_row)? else {
            observe(Observation::OpaqueBoundary)?;
            list_builder.append(false);
            observe(Observation::OpaqueBoundary)?;
            continue;
        };
        if trino {
            let error = if limit <= 0 {
                Some("Limit must be positive")
            } else if limit > i64::from(i32::MAX) {
                Some("Limit is too large")
            } else if delimiter.is_empty() {
                Some("The delimiter may not be the empty string")
            } else {
                None
            };
            if let Some(error) = error {
                observe(Observation::OpaqueBoundary)?;
                errors
                    .try_reserve(1)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                errors.push(RowDataError::with_reason(
                    ordinal,
                    crate::DataFailureReason::Domain(
                        novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                    ),
                    error,
                ));
                list_builder.append(false);
                observe(Observation::OpaqueBoundary)?;
                continue;
            }
        }
        if delimiter.is_empty() {
            for ch in haystack.chars() {
                observe(Observation::Step)?;
                let mut buf = [0u8; 4];
                let s = ch.encode_utf8(&mut buf);
                observe(Observation::OpaqueBoundary)?;
                append_piece(
                    &mut list_builder,
                    s,
                    &mut row_children,
                    &mut child_offset,
                    &mut byte_offset,
                )?;
                observe(Observation::OpaqueBoundary)?;
            }
            observe(Observation::OpaqueBoundary)?;
            list_builder.append(true);
            observe(Observation::OpaqueBoundary)?;
            continue;
        }
        let mut start = 0usize;
        while row_children + 1 < limit as usize {
            observe(Observation::OpaqueBoundary)?;
            let found = haystack[start..].find(delimiter);
            observe(Observation::OpaqueBoundary)?;
            let Some(pos) = found else {
                break;
            };
            let end = start + pos;
            observe(Observation::OpaqueBoundary)?;
            append_piece(
                &mut list_builder,
                &haystack[start..end],
                &mut row_children,
                &mut child_offset,
                &mut byte_offset,
            )?;
            observe(Observation::OpaqueBoundary)?;
            start = end + delimiter.len();
            observe(Observation::Step)?;
        }
        observe(Observation::OpaqueBoundary)?;
        append_piece(
            &mut list_builder,
            &haystack[start..],
            &mut row_children,
            &mut child_offset,
            &mut byte_offset,
        )?;
        list_builder.append(true);
        observe(Observation::OpaqueBoundary)?;
    }
    observe(Observation::OpaqueBoundary)?;
    let result = Arc::new(list_builder.finish()) as ArrayRef;
    observe(Observation::OpaqueBoundary)?;
    Ok((result, errors.into_boxed_slice()))
}
pub fn evaluate_required(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, KernelFailure> {
    let (left, right) = sources(left.as_ref(), right.as_ref())?;
    if right.len() != left.len() {
        return Err(KernelFailure::from_domain(SplitContract::LengthMismatch));
    }
    evaluate_rows(
        left,
        right,
        Selection::all(left.len()),
        |_, row| (row, row),
        |_, _| Ok(Some(i64::from(i32::MAX))),
        false,
        None,
        &mut |_| Ok(()),
    )
    .map(|(result, _)| result)
}
pub fn evaluate_observed(
    left: &dyn Array,
    right: &dyn Array,
    selection: Selection<'_>,
    address: impl FnMut(usize, usize) -> (usize, usize),
    observe: &mut impl FnMut(Observation) -> Result<(), KernelFailure>,
) -> Result<ArrayRef, KernelFailure> {
    let (left, right) = sources(left, right)?;
    evaluate_rows(
        left,
        right,
        selection,
        address,
        |_, _| Ok(Some(i64::from(i32::MAX))),
        false,
        None,
        observe,
    )
    .map(|(result, _)| result)
}
pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let types = &contract.selected().argument_types;
    if !matches!(types.len(), 2 | 3) {
        return Err(invalid(
            "split requires two text arguments and an optional limit",
        ));
    }
    let mut nullable = false;
    for (index, ty) in types.iter().enumerate() {
        step()?;
        let FunctionArgumentType::Value(source) = ty else {
            return Err(invalid("split requires value arguments"));
        };
        if source.logical_type() != ValueLogicalType::Physical
            || source.data_type()
                != if index < 2 {
                    &DataType::Utf8
                } else {
                    &DataType::Int64
                }
        {
            return Err(invalid("split requires its exact Physical Utf8 arguments"));
        }
        nullable |= source.nullable();
    }
    let target = contract.result_type();
    step()?;
    // Both sources were checked above against the original Physical Utf8
    // profile. Borrow that complete declaration for the canonical item; do
    // not allocate a Field/name only to compare the result's immutable layout.
    let FunctionArgumentType::Value(source) = &types[0] else {
        unreachable!("checked split value argument")
    };
    let item_matches = if target.logical_type() != ValueLogicalType::Physical {
        false
    } else {
        match (target.value_type().kind(), target.data_type()) {
            (novarocks_type_contract::ValueKind::List(_), DataType::List(item)) => {
                step()?;
                if item.name() != "item" || !item.is_nullable() {
                    false
                } else if let Some(child) = target.typed_binding().borrowed().child_at(0) {
                    step()?;
                    child.value_type().nullable()
                        && source
                            .typed_binding()
                            .borrowed()
                            .same_physical_binding_ignoring_root_nullability_observed(
                                child, &mut step,
                            )?
                } else {
                    false
                }
            }
            _ => false,
        }
    };
    if !item_matches || (nullable && !target.nullable()) {
        return Err(invalid(
            "split differs from its exact List Utf8 result contract",
        ));
    }
    Ok(())
}
pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        validate_profile(input.contract(), || work.step())?;
        if input.arguments().len() != input.contract().selected().argument_types.len() {
            return Err(invalid("split selected argument arity differs"));
        }
        let selection = input.selection();
        for (arg, ty) in input
            .arguments()
            .iter()
            .zip(input.contract().selected().argument_types.iter())
        {
            let FunctionArgumentType::Value(source) = ty else {
                unreachable!("validated values")
            };
            work.flush()?;
            let nulls = arg.array().logical_nulls();
            work.flush()?;
            for (ordinal, row) in selection.iter().enumerate() {
                let row = arg.value_row(ordinal, row);
                work.step()?;
                if row >= arg.array().len() {
                    return Err(internal("split selected address is out of bounds"));
                }
                if !source.nullable() && nulls.as_ref().is_some_and(|n| n.is_null(row)) {
                    return Err(internal("split non-null selected source contains SQL NULL"));
                }
            }
        }
        let (left, right) = sources(
            input.arguments()[0].array().as_ref(),
            input.arguments()[1].array().as_ref(),
        )?;
        let limit = input.arguments().get(2);
        let limits = limit
            .map(|arg| StringIntegerArray::read(arg.array().as_ref()))
            .transpose()?;
        let trino = input.contract().effects().own_row_error == FunctionIntrinsicRowError::MayRaise;
        let DataType::List(field) = input.contract().result_type().data_type() else {
            return Err(invalid("split requires a checked list result"));
        };
        let (out, errors) = evaluate_rows(
            left,
            right,
            selection,
            |ordinal, row| {
                (
                    input.arguments()[0].value_row(ordinal, row),
                    input.arguments()[1].value_row(ordinal, row),
                )
            },
            |ordinal, row| {
                let Some((limit, limits)) = limit.zip(limits.as_ref()) else {
                    return Ok(Some(i64::from(i32::MAX)));
                };
                let row = limit.value_row(ordinal, row);
                Ok(limit.array().is_valid(row).then(|| limits.value(row)))
            },
            trino,
            Some(field),
            &mut |event| match event {
                Observation::Step => work.step(),
                Observation::OpaqueBoundary => work.flush(),
            },
        )?;
        SelectedValues::try_new_observed(
            selection,
            input.contract().result_type().data_type(),
            out,
            errors,
            || work.step(),
        )
    })();
    work.finish_result(result)
}
#[cfg(test)]
#[path = "string_split_tests.rs"]
mod tests;
