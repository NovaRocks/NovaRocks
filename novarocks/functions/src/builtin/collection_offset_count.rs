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
//! ONE original root-NULL and i32-offset counting body shared by CARDINALITY and MAP_SIZE.
use super::array_literal_core::CollectionObservation;
use arrow_array::{Array, ArrayRef, Int64Array};
use std::sync::Arc;
#[derive(Debug)]
pub enum OffsetCountFailure<E> {
    Operation(crate::KernelFailure),
    Control(E),
}
/// The offset supplier preserves each raw shell's original classification/allocation order.
/// Arrays and offsets are exact borrowed facts; this function does not decode children.
pub(super) fn count_observed<'a, E>(
    array: &dyn Array,
    offsets: impl FnOnce() -> Result<&'a [i32], crate::KernelFailure>,
    selection: crate::Selection<'_>,
    mut row: impl FnMut(usize, usize) -> Result<usize, E>,
    observer: &mut dyn FnMut(CollectionObservation) -> Result<(), E>,
) -> Result<ArrayRef, OffsetCountFailure<E>> {
    observer(CollectionObservation::OpaqueBoundary).map_err(OffsetCountFailure::Control)?;
    output_capacity(selection.len()).map_err(OffsetCountFailure::Operation)?;
    let mut out = Vec::new();
    out.try_reserve_exact(selection.len()).map_err(|source| {
        OffsetCountFailure::Operation(crate::KernelFailure::from_domain(
            crate::allocation_failure::AllocationRefusal::new("collection offset count", source),
        ))
    })?;
    observer(CollectionObservation::OpaqueBoundary).map_err(OffsetCountFailure::Control)?;
    let offsets = offsets().map_err(OffsetCountFailure::Operation)?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        observer(CollectionObservation::Step).map_err(OffsetCountFailure::Control)?;
        let row = row(ordinal, batch_row).map_err(OffsetCountFailure::Control)?;
        if row >= array.len() || row.checked_add(1).is_none_or(|end| end >= offsets.len()) {
            return Err(OffsetCountFailure::Operation(
                super::collection_arrow::contract(
                    "collection offset count row is outside its prepared carrier",
                ),
            ));
        }
        if array.is_null(row) {
            out.push(None);
        } else {
            let count = offsets[row + 1]
                .checked_sub(offsets[row])
                .filter(|n| *n >= 0)
                .ok_or_else(|| {
                    OffsetCountFailure::Operation(super::collection_arrow::contract(
                        "collection offset count requires monotone representable offsets",
                    ))
                })?;
            out.push(Some(i64::from(count)));
        }
    }
    observer(CollectionObservation::OpaqueBoundary).map_err(OffsetCountFailure::Control)?;
    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    observer(CollectionObservation::OpaqueBoundary).map_err(OffsetCountFailure::Control)?;
    Ok(out)
}

#[derive(Debug)]
struct CountOutputLayout {
    rows: usize,
    width: usize,
    source: std::alloc::LayoutError,
}
impl std::fmt::Display for CountOutputLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "collection count output requires {} rows of {} bytes: {}",
            self.rows, self.width, self.source
        )
    }
}
impl std::error::Error for CountOutputLayout {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for CountOutputLayout {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let code = std::num::NonZeroU16::new(389).unwrap();
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, code),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::KERNEL,
                code,
            },
            required: u64::try_from(self.rows as u128 * self.width as u128).ok(),
            extent_limit: Some(isize::MAX as u64),
            representable_prefix: None,
        })
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CountOutputLayout {}
/// Validate the actual temporary Vec, final values and bitmap geometry.
/// This observes format addressability; it does not reserve any memory budget.
pub(super) fn output_capacity(rows: usize) -> Result<(), crate::KernelFailure> {
    fn array<T>(rows: usize) -> Result<(), crate::KernelFailure> {
        std::alloc::Layout::array::<T>(rows).map_err(|source| {
            crate::KernelFailure::from_domain(CountOutputLayout {
                rows,
                width: std::mem::size_of::<T>(),
                source,
            })
        })?;
        Ok(())
    }
    array::<Option<i64>>(rows)?;
    array::<i64>(rows)?;
    // The checked output extent proves this ceil operation fits usize.
    array::<u8>((rows / 64 + usize::from(!rows.is_multiple_of(64))) * 8)
}

#[cfg(test)]
#[path = "collection_offset_count_tests.rs"]
mod tests;
