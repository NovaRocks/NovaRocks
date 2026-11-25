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

use std::sync::Arc;

use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ListArray, StringArray};
use arrow_buffer::{Buffer, NullBufferBuilder, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::{
    FunctionValueType, PureCompileControl, TrinoCastMode, ValueLogicalType,
};

use super::array_scalar_output::scratch;
use super::json_sql_selected::{RowFailure, RowWork};
use super::value_conversion_temporal::TemporalTextCast;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::cast::{CastPrepareError, CastRowResult, PreparedCastRecipe};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, logical_is_null};
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::scalar_output_operation::with_scalar_output_peak;
use crate::selected_copy::checked_utf8_output_offset;
use crate::{
    AggregateStateAllocator, DataFailureReason, EvaluatedArgument, KernelEvaluationControl,
    KernelFailure, RowDataError, ScalarCallContract, ScalarCallInput, SelectedValues,
};

#[derive(Debug)]
pub(super) enum ElementCast {
    Varchar,
    Standard(PreparedCastRecipe),
    JsonVarchar(PreparedCastRecipe),
    Temporal {
        cast: TemporalTextCast,
        nullable: bool,
    },
}

impl ElementCast {
    pub(super) fn bind(
        element: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<Self, CastPrepareError> {
        let target = FunctionValueType::new(DataType::Utf8, false);
        if let Some(cast) = TemporalTextCast::bind(element, &target) {
            return Ok(Self::Temporal {
                cast,
                nullable: element.nullable(),
            });
        }
        let nullable = PreparedCastRecipe::trino_result_nullable(
            element,
            &target,
            TrinoCastMode::Explicit,
            control,
        )?;
        let target = target
            .with_nullable(nullable)
            .map_err(|_| CastPrepareError::TypeMismatch)?;
        let prepared =
            PreparedCastRecipe::try_new_trino(element, &target, TrinoCastMode::Explicit, control)?;
        Ok(
            if prepared.is_identity() && element.data_type() == &DataType::Utf8 {
                Self::Varchar
            } else if element.logical_type() == ValueLogicalType::Json {
                Self::JsonVarchar(prepared)
            } else {
                Self::Standard(prepared)
            },
        )
    }

    pub(super) fn prepare(
        contract: &ScalarCallContract,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let mut arguments = contract.value_argument_types();
        let array = arguments
            .next()
            .ok_or_else(|| invalid("array_join requires an array"))?;
        if !matches!(array.data_type(), DataType::List(_))
            || !matches!(arguments.len(), 1 | 2)
            || arguments.any(|ty| ty.data_type() != &DataType::Utf8)
            || contract.result_type().data_type() != &DataType::Utf8
        {
            return Err(invalid(
                "array_join requires its frozen List and VARCHAR declarations",
            ));
        }
        let element = array
            .child_at(0)
            .ok_or_else(|| invalid("array_join has no declared element"))?;
        Self::bind(&element, control).map_err(|error| match error {
            CastPrepareError::Control(cause) => compile_failure(cause),
            CastPrepareError::Kernel(cause) => cause,
            _ => invalid("array_join preparation requires its selected element CAST"),
        })
    }
}

fn append(
    output: &mut HostVec<u8, HostAggregateAllocator>,
    text: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let total = output
        .len()
        .checked_add(text.len())
        .ok_or(KernelFailure::ResourceExhausted)?;
    checked_utf8_output_offset(total)?;
    work.flush()?;
    output.try_reserve(text.len()).map_err(|_| {
        output
            .allocator()
            .take_recorded_failure()
            .unwrap_or(KernelFailure::ResourceExhausted)
    })?;
    for bytes in text.as_bytes().chunks(32 * 1024) {
        work.step()?;
        output.extend_from_slice(bytes);
    }
    Ok(())
}

fn append_part(
    output: &mut HostVec<u8, HostAggregateAllocator>,
    separator: &str,
    part: &str,
    emitted: &mut bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    if *emitted {
        append(output, separator, work)?;
    }
    append(output, part, work)?;
    *emitted = true;
    Ok(())
}

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
    cast: &ElementCast,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let arguments = input.arguments();
    let source = arguments[0]
        .array()
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| internal("array_join lost its List input"))?;
    let text = |arg: EvaluatedArgument<'a>| {
        arg.array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("array_join lost a VARCHAR input"))
    };
    let separator = text(arguments[1])?;
    let replacement = arguments.get(2).copied().map(text).transpose()?;
    let selection = input.selection();
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let count = selection
            .len()
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut payload = scratch(0, allocator)?;
        let mut offsets = scratch(count, allocator)?;
        let mut valid = scratch(selection.len(), allocator)?;
        let mut row_errors = scratch(selection.len(), allocator)?;
        let mut cast_charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
        let mut diagnostic_bytes = 0usize;
        let child = source.values();
        let utf8 = child.as_any().downcast_ref::<StringArray>();
        offsets.push(0i32);
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let src = arguments[0].value_row(ordinal, row);
            let sep = arguments[1].value_row(ordinal, row);
            let null_text = replacement.zip(arguments.get(2)).map(|(values, arg)| {
                let row = arg.value_row(ordinal, row);
                (!values.is_null(row)).then(|| values.value(row))
            });
            if source.is_null(src) || separator.is_null(sep) || null_text == Some(None) {
                valid.push(false);
                offsets.push(checked_utf8_output_offset(payload.len())?);
                continue;
            }
            let sep = separator.value(sep);
            let fallback = null_text.flatten();
            let start = payload.len();
            let mut emitted = false;
            let mut failed = false;
            let child_offsets = source.value_offsets();
            for item in child_offsets[src] as usize..child_offsets[src + 1] as usize {
                work.step()?;
                let mut _workspace = None;
                let owned;
                let part = if logical_is_null(child.as_ref(), item, 1, &mut work)? {
                    fallback
                } else {
                    match cast {
                        ElementCast::Varchar => Some(
                            utf8.ok_or_else(|| {
                                internal("array_join identity lost its VARCHAR carrier")
                            })?
                            .value(item),
                        ),
                        ElementCast::JsonVarchar(recipe) => {
                            let result = recipe.with_json_varchar_row(
                                EvaluatedArgument::Column(child),
                                ordinal,
                                item,
                                &mut RowWork::new(&mut work, &cast_charge),
                                |part, row| {
                                    append_part(&mut payload, sep, part, &mut emitted, row.work)
                                },
                            );
                            match result {
                                Ok(Some(())) => continue,
                                Ok(None) => fallback,
                                Err(RowFailure::Outer(cause)) => return Err(cause),
                                Err(RowFailure::Data { reason, message }) => {
                                    let bytes =
                                        message.len().min(crate::MAX_ROW_ERROR_MESSAGE_BYTES);
                                    work.flush()?;
                                    let mut grant = cast_charge.reserve_operation(bytes)?;
                                    row_errors.push(RowDataError::with_reason(
                                        ordinal,
                                        DataFailureReason::Domain(reason),
                                        message,
                                    ));
                                    diagnostic_bytes = diagnostic_bytes
                                        .checked_add(bytes)
                                        .ok_or(KernelFailure::ResourceExhausted)?;
                                    cast_charge.reconcile_under_reservation(
                                        diagnostic_bytes,
                                        &mut grant,
                                    )?;
                                    failed = true;
                                    break;
                                }
                            }
                        }
                        ElementCast::Standard(recipe) => {
                            // A single original CAST may materialize text while
                            // the growing join payload is still live.
                            let bytes = crate::cast::varchar_row_temporary_extent(child, item)?;
                            work.flush()?;
                            _workspace = Some(cast_charge.reserve_operation(bytes)?);
                            match recipe.evaluate_row(
                                EvaluatedArgument::Column(child),
                                ordinal,
                                item,
                                control,
                            )? {
                                CastRowResult::Text(text) => {
                                    owned = text;
                                    Some(owned.as_str())
                                }
                                CastRowResult::Null => fallback,
                                CastRowResult::RowError(error) => {
                                    row_errors.push(error);
                                    failed = true;
                                    break;
                                }
                                _ => return Err(internal("element CAST did not produce VARCHAR")),
                            }
                        }
                        ElementCast::Temporal { cast, nullable } => {
                            work.flush()?;
                            _workspace = Some(cast_charge.reserve_operation(
                                super::value_conversion_temporal::TEMPORAL_CAST_TEXT_SCRATCH,
                            )?);
                            match cast.evaluate(child.as_ref(), item, *nullable, &mut work)? {
                                Some(text) => {
                                    owned = text;
                                    Some(owned.as_str())
                                }
                                None => fallback,
                            }
                        }
                    }
                };
                if let Some(part) = part {
                    append_part(&mut payload, sep, part, &mut emitted, &mut work)?;
                }
            }
            if failed {
                payload.truncate(start);
            }
            valid.push(!failed);
            offsets.push(checked_utf8_output_offset(payload.len())?);
        }

        let output =
            crate::scalar_output_resources::flat_utf8_output_peak(payload.len(), selection.len())?;
        let values =
            with_scalar_output_peak(output, Arc::clone(host), allocator, &mut work, |work| {
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(payload.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                for chunk in payload.chunks(32 * 1024) {
                    work.step()?;
                    bytes.extend_from_slice(chunk);
                }
                work.flush()?;
                let mut result_offsets = Vec::new();
                result_offsets
                    .try_reserve_exact(offsets.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                let mut nulls = NullBufferBuilder::new(valid.len());
                result_offsets.push(0);
                for (offset, valid) in offsets[1..].iter().zip(valid.iter()) {
                    work.step()?;
                    result_offsets.push(*offset);
                    nulls.append(*valid);
                }
                let result = StringArray::try_new(
                    OffsetBuffer::new(result_offsets.into()),
                    Buffer::from(bytes),
                    nulls.finish(),
                )
                .map_err(|error| {
                    super::collection_arrow::arrow_failure(
                        "array_join",
                        super::collection_arrow::ArrowSite::Construct,
                        error,
                    )
                })?;
                Ok(Arc::new(result))
            })?
            .values;
        let error_bytes = std::alloc::Layout::array::<RowDataError>(row_errors.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?
            .size();
        work.flush()?;
        let _error_reservation = cast_charge.reserve_operation(error_bytes)?;
        let mut errors = Vec::new();
        errors
            .try_reserve_exact(row_errors.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for error in row_errors {
            work.step()?;
            errors.push(error);
        }
        SelectedValues::try_new_observed(
            selection,
            &DataType::Utf8,
            values,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}
