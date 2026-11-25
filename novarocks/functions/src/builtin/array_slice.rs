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

use arrow_array::{Array, Int64Array, ListArray};
use arrow_schema::DataType;
use novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT;

use super::array_scalar_output::scratch;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::selected_copy::{ExtendSegment, list_from_segments_in};
use crate::{
    AggregateStateAllocator, DataFailureReason, KernelEvaluationControl, KernelFailure,
    RowDataError, ScalarCallContract, ScalarCallInput, SelectedValues,
};

pub(super) fn validate_profile(
    contract: &ScalarCallContract,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let mut types = contract.value_argument_types();
    let (Some(array), Some(start), Some(length), None) =
        (types.next(), types.next(), types.next(), types.next())
    else {
        return Err(invalid("array slice requires exactly three arguments"));
    };
    if !matches!(array.data_type(), DataType::List(_))
        || start.data_type() != &DataType::Int64
        || length.data_type() != &DataType::Int64
        || !novarocks_type_contract::arrow_data_types_exact_observed(
            array.data_type(),
            contract.result_type().data_type(),
            &mut observe,
        )?
    {
        return Err(invalid(
            "array slice requires its frozen List and BIGINT argument types",
        ));
    }
    Ok(())
}

fn range(size: usize, start: i64, length: i64) -> Result<(usize, usize), &'static str> {
    if start == 0 {
        return Err("SQL array indices start at 1");
    }
    if length < 0 {
        return Err("length must be greater than or equal to 0");
    }
    let size_wide = size as i128;
    let start = if start < 0 {
        size_wide + i128::from(start)
    } else {
        i128::from(start) - 1
    };
    if !(0..size_wide).contains(&start) {
        return Ok((0, 0));
    }
    let remaining = size_wide - start;
    Ok((start as usize, remaining.min(i128::from(length)) as usize))
}

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    host: &Arc<dyn AggregateStateAllocator>,
    allocator: &HostAggregateAllocator,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let arguments = input.arguments();
    let array = arguments[0]
        .array()
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| internal("array slice received a non-List carrier"))?;
    let starts = arguments[1]
        .array()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| internal("array slice received a non-BIGINT start carrier"))?;
    let lengths = arguments[2]
        .array()
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| internal("array slice received a non-BIGINT length carrier"))?;
    let target = input.contract().result_type().data_type();
    let DataType::List(field) = target else {
        return Err(internal("array slice has a non-List result"));
    };
    let selection = input.selection();
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        work.flush()?;
        let mut row_sizes = scratch(selection.len(), allocator)?;
        let mut segments = scratch::<ExtendSegment>(selection.len(), allocator)?;
        let mut row_errors = scratch(selection.len(), allocator)?;
        let offsets = array.value_offsets();
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let array_row = arguments[0].value_row(ordinal, row);
            let start_row = arguments[1].value_row(ordinal, row);
            let length_row = arguments[2].value_row(ordinal, row);
            if array.is_null(array_row) || starts.is_null(start_row) || lengths.is_null(length_row)
            {
                row_sizes.push(None);
                continue;
            }
            let begin = offsets[array_row] as usize;
            let size = offsets[array_row + 1] as usize - begin;
            match range(size, starts.value(start_row), lengths.value(length_row)) {
                Ok((start, length)) => {
                    row_sizes.push(Some(length));
                    if length != 0 {
                        let start = begin + start;
                        if let Some(last) = segments.last_mut()
                            && last.start + last.len == start
                        {
                            last.len += length;
                        } else {
                            segments.push(ExtendSegment {
                                source: 0,
                                start,
                                len: length,
                                repeats: 1,
                                nulls: 0,
                            });
                        }
                    }
                }
                Err(message) => {
                    row_sizes.push(None);
                    row_errors.push((ordinal, message));
                }
            }
        }
        let output = list_from_segments_in(
            field,
            std::slice::from_ref(array.values()),
            &segments,
            &row_sizes,
            host,
            allocator,
            &mut work,
        )?;
        let errors_charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
        let capacity = std::alloc::Layout::array::<RowDataError>(row_errors.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let _reservation = errors_charge.reserve_operation(capacity.size())?;
        let mut errors = Vec::new();
        errors
            .try_reserve_exact(row_errors.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for &(ordinal, message) in row_errors.iter() {
            work.step()?;
            errors.push(RowDataError::with_reason(
                ordinal,
                DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
                message,
            ));
        }
        SelectedValues::try_new_observed(
            selection,
            target,
            output,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

#[cfg(test)]
mod tests {
    use super::range;

    #[test]
    fn array_slice_extreme_arguments_clip_without_overflow() {
        assert_eq!(range(3, 2, i64::MAX), Ok((1, 2)));
        assert_eq!(range(3, -2, i64::MAX), Ok((1, 2)));
        assert_eq!(range(3, i64::MIN, 2), Ok((0, 0)));
        assert_eq!(range(3, i64::MAX, 2), Ok((0, 0)));
        assert!(range(0, 0, 0).is_err());
        assert!(range(0, 1, i64::MIN).is_err());
    }
}
