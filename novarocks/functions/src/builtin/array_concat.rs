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

//! Selected evaluation of the frozen array concatenation overload.

use super::{
    collection_arrow::{ArrowSite, arrow_failure},
    collection_selected::{copy_error, reserve},
};
use crate::{
    DataFailureReason, FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallContract, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    selected_copy::{ExtendSegment, preflight_extend_multi},
};
use arrow_array::{Array, ListArray, make_array};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::{ArrayData, transform::MutableArrayData};
use arrow_schema::DataType;
use novarocks_type_contract::{
    FunctionValueType, ValueLogicalType, arrow_data_types_exact_observed,
};
use std::{cell::RefCell, sync::Arc};

pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    element: Option<usize>,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let result = contract.result_type();
    let (ValueLogicalType::Physical, DataType::List(output), Some(output_item)) = (
        result.logical_type(),
        result.data_type(),
        result.value_type().child_type_at(0),
    ) else {
        return Err(invalid("array_concat requires a frozen List result"));
    };
    for (position, argument) in contract.selected().argument_types.iter().enumerate() {
        observe()?;
        let FunctionArgumentType::Value(source) = argument else {
            return Err(invalid("array_concat requires value arguments"));
        };
        let (item, carrier, nullable) = if Some(position) == element {
            (source.value_type(), source.data_type(), source.nullable())
        } else {
            let (ValueLogicalType::Physical, DataType::List(field), Some(item)) = (
                source.logical_type(),
                source.data_type(),
                source.value_type().child_type_at(0),
            ) else {
                return Err(invalid("array concat requires frozen List arguments"));
            };
            if source.nullable() && !result.nullable() {
                return Err(invalid("array concat cannot discard a List root NULL"));
            }
            (item, field.data_type(), field.is_nullable())
        };
        if !FunctionValueType::borrowed_value_domains_equal_observed(
            item,
            output_item,
            &mut observe,
        )? || !arrow_data_types_exact_observed::<KernelFailure>(
            carrier,
            output.data_type(),
            &mut observe,
        )? || (nullable && !output.is_nullable())
        {
            return Err(invalid(
                "array_concat argument differs from the frozen item or NULL contract",
            ));
        }
    }
    Ok(())
}

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    element: Option<usize>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = RefCell::new(EvaluationCheckpoints::new(control));
    let result =
        (|| {
            let selection = input.selection();
            let arguments = input.arguments();
            let target = input.contract().result_type().data_type();
            let DataType::List(field) = target else {
                return Err(internal(
                    "array_concat result is outside its prepared List profile",
                ));
            };
            let mut arrays = reserve::<Option<&ListArray>>(arguments.len(), &work)?;
            let mut sources = reserve::<&dyn Array>(arguments.len(), &work)?;
            let mut data = reserve::<ArrayData>(arguments.len(), &work)?;
            for (position, argument) in arguments.iter().enumerate() {
                work.borrow_mut().step()?;
                let list =
                    if Some(position) == element {
                        None
                    } else {
                        Some(argument.array().as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    internal("array concat argument is outside its prepared List profile")
                })?)
                    };
                arrays.push(list);
                sources.push(list.map_or(argument.array().as_ref(), |list| list.values().as_ref()));
            }
            let offset_count = selection
                .len()
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?;
            let segment_count = selection
                .len()
                .checked_mul(arrays.len())
                .ok_or(KernelFailure::ResourceExhausted)?;
            let mut offsets = reserve::<i32>(offset_count, &work)?;
            let mut segments = reserve::<ExtendSegment>(segment_count, &work)?;
            let mut input_rows = reserve::<usize>(arrays.len(), &work)?;
            let mut nulls = NullBufferBuilder::new(selection.len());
            let unary = arguments.len() == 1;
            let mut errors =
                reserve::<RowDataError>(if unary { selection.len() } else { 0 }, &work)?;
            let mut offset = 0;
            offsets.push(offset);

            for (ordinal, row) in selection.iter().enumerate() {
                work.borrow_mut().step()?;
                input_rows.clear();
                let mut valid = true;
                for argument in arguments {
                    work.borrow_mut().step()?;
                    let source_row = argument.value_row(ordinal, row);
                    if source_row >= argument.array().len() {
                        return Err(internal(
                            "array_concat selection is outside its frozen argument",
                        ));
                    }
                    input_rows.push(source_row);
                    valid &= argument.array().data_type() != &DataType::Null
                        && argument.array().is_valid(source_row);
                }
                if valid && unary {
                    work.borrow_mut().flush()?;
                    errors.push(RowDataError::with_reason(
                        ordinal,
                        DataFailureReason::Domain(
                            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                        ),
                        "There must be two or more arguments to concat",
                    ));
                }
                nulls.append(valid && !unary);
                if valid && !unary {
                    for (source, (array, source_row)) in arrays.iter().zip(&input_rows).enumerate()
                    {
                        work.borrow_mut().step()?;
                        let (start, end) = if let Some(array) = array {
                            let ranges = array.value_offsets();
                            (
                                usize::try_from(ranges[*source_row]).map_err(|_| {
                                    internal("array concat input has a negative child offset")
                                })?,
                                usize::try_from(ranges[*source_row + 1]).map_err(|_| {
                                    internal("array concat input has a negative child offset")
                                })?,
                            )
                        } else {
                            (*source_row, *source_row + 1)
                        };
                        let len = end.checked_sub(start).ok_or_else(|| {
                            internal("array_concat input child offsets are reversed")
                        })?;
                        if len != 0 {
                            offset =
                                crate::aggregate_scalar::checked_list_output_offset(offset, len)?;
                            segments.push(ExtendSegment {
                                source,
                                start,
                                len,
                                repeats: 1,
                                nulls: 0,
                            });
                        }
                    }
                }
                offsets.push(offset);
            }

            preflight_extend_multi(&sources, &segments, 0, |boundary| {
                if boundary {
                    work.borrow_mut().flush()
                } else {
                    work.borrow_mut().step()
                }
            })
            .map_err(copy_error)?;
            for source in sources {
                work.borrow_mut().flush()?;
                data.push(source.to_data());
            }
            let mut references = reserve::<&ArrayData>(data.len(), &work)?;
            references.extend(data.iter());
            work.borrow_mut().flush()?;
            let mut values = MutableArrayData::new(references, false, 0);
            for segment in segments {
                work.borrow_mut().flush()?;
                values
                    .try_extend(segment.source, segment.start, segment.start + segment.len)
                    .map_err(|error| arrow_failure("array_concat", ArrowSite::Extend, error))?;
            }
            work.borrow_mut().flush()?;
            let array = ListArray::try_new(
                field.clone(),
                OffsetBuffer::new(offsets.into()),
                make_array(values.freeze()),
                nulls.finish(),
            )
            .map_err(|error| arrow_failure("array_concat", ArrowSite::Construct, error))?;
            work.borrow_mut().flush()?;
            SelectedValues::try_new(
                selection,
                target,
                Arc::new(array),
                errors.into_boxed_slice(),
            )
            .map_err(|_| internal("array_concat compact output differs from its frozen result"))
        })();
    work.into_inner().finish_result(result)
}
