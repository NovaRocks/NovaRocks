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
//! Utf8 membership compares the recipe's logical domain on its original host.
use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBuilder, StringArray};
use novarocks_functions::native_inlist::{InError, InObservation};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl, KernelFailure};
use std::sync::Arc;

/// Arrow backing is destroyed before its original reservation is returned.
pub(crate) struct Utf8Equalities {
    array: BooleanArray,
    _reservation: Option<novarocks_functions::opaque_memory::OpaqueReservation>,
}
impl Utf8Equalities {
    pub(crate) fn without_reservation(array: BooleanArray) -> Self {
        Self {
            array,
            _reservation: None,
        }
    }
    pub(crate) fn array(&self) -> &BooleanArray {
        &self.array
    }
}
fn boolean_buffer_bytes(rows: usize, has_nulls: bool) -> Result<usize, KernelFailure> {
    use novarocks_functions::selected_copy::{CopyError, fixed_interleave_extent};
    fixed_interleave_extent(&arrow::datatypes::DataType::Boolean, rows)
        .map_err(CopyError::into_kernel)?;
    let one = rows
        .div_ceil(8)
        .checked_next_multiple_of(64)
        .ok_or(KernelFailure::ResourceExhausted)?;
    one.checked_mul(1 + usize::from(has_nulls))
        .ok_or(KernelFailure::ResourceExhausted)
}
pub(crate) fn utf8_equality_observed<E>(
    array: &ArrayRef,
    candidate: &ArrayRef,
    json: bool,
    host: Option<Arc<dyn AggregateStateAllocator>>,
    control: &dyn KernelEvaluationControl,
    observe: &mut dyn FnMut(InObservation) -> Result<(), E>,
) -> Result<Utf8Equalities, InError<E>> {
    let input = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            InError::Operation(novarocks_functions::native_inlist::contract(
                "IN input is not Utf8",
            ))
        })?;
    let values = candidate
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            InError::Operation(novarocks_functions::native_inlist::contract(
                "IN candidate is not Utf8",
            ))
        })?;
    if values.len() != 1 && values.len() != input.len() {
        return Err(InError::Operation(
            novarocks_functions::native_inlist::contract("IN candidate length differs from input"),
        ));
    }
    control.checkpoint(0).map_err(InError::Operation)?;
    observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
    let bytes = boolean_buffer_bytes(
        input.len(),
        !input.is_empty() && (input.null_count() != 0 || values.null_count() != 0),
    )
    .map_err(InError::Operation)?;
    let reservation = match host.as_ref().filter(|_| bytes != 0) {
        Some(host) => Some(
            novarocks_functions::opaque_memory::OpaqueRetainedCharge::try_new(Arc::clone(host))
                .and_then(|charge| charge.reserve_operation(bytes))
                .map_err(InError::Operation)?,
        ),
        None => None,
    };
    // Direct non-runtime evaluation uses the same original process heap policy.
    // Runtime callers must pass their installed Frame allocator.
    let host =
        host.unwrap_or_else(|| Arc::new(novarocks_functions::UnaccountedAggregateStateAllocator));
    let mut builder = BooleanBuilder::with_capacity(input.len());
    for row in 0..input.len() {
        control.checkpoint(1).map_err(InError::Operation)?;
        observe(InObservation::Step).map_err(InError::Host)?;
        let rhs = if values.len() == 1 { 0 } else { row };
        if json {
            // Every demanded non-NULL JSON value is normalized, even beside
            // SQL NULL. UNKNOWN cannot authorize hiding an invalid JSON value.
            let left = if input.is_null(row) {
                None
            } else {
                Some(
                    novarocks_functions::json_canonical::normalize_json_key(
                        input.value(row),
                        Arc::clone(&host),
                        control,
                    )
                    .map_err(InError::Operation)?,
                )
            };
            let right = if values.is_null(rhs) {
                None
            } else {
                Some(
                    novarocks_functions::json_canonical::normalize_json_key(
                        values.value(rhs),
                        Arc::clone(&host),
                        control,
                    )
                    .map_err(InError::Operation)?,
                )
            };
            match (left, right) {
                (Some(left), Some(right)) => {
                    builder.append_value(left.as_bytes() == right.as_bytes())
                }
                _ => builder.append_null(),
            }
        } else if input.is_null(row) || values.is_null(rhs) {
            builder.append_null();
        } else {
            builder.append_value(input.value(row) == values.value(rhs));
        }
    }
    control.checkpoint(0).map_err(InError::Operation)?;
    observe(InObservation::OpaqueBoundary).map_err(InError::Host)?;
    Ok(Utf8Equalities {
        array: builder.finish(),
        _reservation: reservation,
    })
}
