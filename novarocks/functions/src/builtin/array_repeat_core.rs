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
//! ARRAY_REPEAT retains one original MutableArrayData author. Its exact ordered
//! constructor/extension plan is checked before any output copy or allocation.
use super::collection_arrow::{self, ArrowSite};
use crate::KernelFailure;
use crate::selected_copy::{CopyError, ExtendSegment, preflight_extend_multi};
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, make_array, new_null_array};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{DataType, FieldRef};
use std::sync::Arc;
fn reserve<T>(count: usize) -> Result<Vec<T>, KernelFailure> {
    let mut out = Vec::new();
    out.try_reserve_exact(count).map_err(|source| {
        KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            "array_repeat plan",
            source,
        ))
    })?;
    Ok(out)
}
pub fn cast_repeat_count(array: ArrayRef) -> Result<ArrayRef, KernelFailure> {
    if array.data_type() == &DataType::Int64 {
        return Ok(array);
    }
    arrow_cast::cast(&array, &DataType::Int64).map_err(|source| {
        collection_arrow::arrow_failure("array_repeat count", ArrowSite::Cast, source)
    })
}
pub fn repeat_values(
    source: ArrayRef,
    counts: &Int64Array,
    rows: usize,
    output_field: FieldRef,
) -> Result<ArrayRef, KernelFailure> {
    if (source.len() != 1 && source.len() != rows) || (counts.len() != 1 && counts.len() != rows) {
        return Err(collection_arrow::contract(
            "array_repeat prepared argument lengths differ from their invocation",
        ));
    }
    let target = output_field.data_type();
    let values = if source.data_type() == &DataType::Null && target != &DataType::Null {
        new_null_array(target, source.len())
    } else if source.data_type() == target {
        source
    } else {
        arrow_cast::cast(&source, target).map_err(|source| {
            collection_arrow::arrow_failure("array_repeat source", ArrowSite::Cast, source)
        })?
    };
    let mut segments = Vec::new();
    let mut current = 0i32;
    let offset_count = collection_arrow::list_offset_entries(rows)?;
    let mut offsets = reserve::<i32>(offset_count)?;
    offsets.push(0);
    let mut nulls = NullBufferBuilder::new(rows);
    for row in 0..rows {
        let source_row = if values.len() == 1 { 0 } else { row };
        let count_row = if counts.len() == 1 { 0 } else { row };
        if counts.is_null(count_row) {
            nulls.append_null();
            offsets.push(current);
            continue;
        }
        let count = counts.value(count_row);
        if count > 0 {
            current =
                crate::aggregate_scalar::checked_list_output_offset_u128(current, count as u128)?;
            segments.try_reserve(1).map_err(|source| {
                KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
                    "array_repeat extension plan",
                    source,
                ))
            })?;
            segments.push(ExtendSegment {
                source: 0,
                start: source_row,
                len: 1,
                repeats: count as usize,
                nulls: 0,
            });
        }
        nulls.append_non_null();
        offsets.push(current);
    }
    preflight_extend_multi(&[values.as_ref()], &segments, 0, |_| Ok(()))
        .map_err(CopyError::into_kernel)?;
    let data = values.to_data();
    let mut out = MutableArrayData::new(vec![&data], false, 0);
    for segment in segments {
        for _ in 0..segment.repeats {
            out.try_extend(0, segment.start, segment.start + 1)
                .map_err(|source| {
                    collection_arrow::arrow_failure("array_repeat", ArrowSite::Extend, source)
                })?;
        }
    }
    let result = ListArray::try_new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        make_array(out.freeze()),
        nulls.finish(),
    )
    .map_err(|source| {
        collection_arrow::arrow_failure("array_repeat", ArrowSite::Construct, source)
    })?;
    Ok(Arc::new(result))
}
#[cfg(test)]
#[path = "array_repeat_core_tests.rs"]
mod tests;
