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

//! Ordinary-array zip preflight for the admitted flat carrier profiles.
//! This checks representation and allocation layouts, not a host memory grant.

use super::{
    CopyError, CopyObservation, add, buffer_extent, byte_interleave_payload_extent, downcast,
    guarded_interleave_extent, interleave_bitmap_extent, mul, mutable_capacity, offset_value,
};
use crate::KernelFailure;
use arrow_array::types::{BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type};
use arrow_array::{Array, BooleanArray, GenericByteArray};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_schema::DataType;

// Arrow 60.0 MutableBuffer rounds every requested capacity to 64 bytes.
pub(super) fn rounded_capacity(bytes: usize) -> Result<usize, CopyError> {
    let blocks = add(bytes / 64, usize::from(!bytes.is_multiple_of(64)))?;
    buffer_extent(blocks, 64)?;
    mul(blocks, 64)
}

// The byte buffer starts with truthy.len() bytes, not the selected payload
// size. Its actual mask-run extends can double an already allocated capacity.
struct ByteBufferExtent {
    len: usize,
    capacity: usize,
    exact_interleave: bool,
}
impl ByteBufferExtent {
    fn new(capacity: usize, exact_interleave: bool) -> Result<Self, CopyError> {
        Ok(Self {
            len: 0,
            capacity: if exact_interleave {
                0
            } else {
                rounded_capacity(capacity)?
            },
            exact_interleave,
        })
    }
    fn extend(&mut self, bytes: usize, large: bool) -> Result<(), CopyError> {
        let required = add(self.len, bytes)?;
        byte_interleave_payload_extent(required, large)?;
        if self.exact_interleave {
            buffer_extent(required, 1)?;
        } else if required > self.capacity {
            let capacity = rounded_capacity(required)?.max(mul(self.capacity, 2)?);
            buffer_extent(capacity, 1)?;
            self.capacity = capacity;
        }
        self.len = required;
        Ok(())
    }
}

fn flat_extent(ty: &DataType, rows: usize) -> Result<(), CopyError> {
    let bytes = if let Some(width) = ty.primitive_width() {
        mul(rows, width)?
    } else {
        match ty {
            DataType::Boolean => add(rows / 8, usize::from(!rows.is_multiple_of(8)))?,
            DataType::FixedSizeBinary(width) => {
                let width = usize::try_from(*width)
                    .map_err(|_| CopyError::Invalid("copy frozen carrier has a negative width"))?;
                mul(rows, width)?
            }
            DataType::Utf8 | DataType::Binary => mul(add(rows, 1)?, 4)?,
            DataType::LargeUtf8 | DataType::LargeBinary => mul(add(rows, 1)?, 8)?,
            other => return Err(CopyError::unsupported(other)),
        }
    };
    if !matches!(ty, DataType::FixedSizeBinary(_)) {
        guarded_interleave_extent(ty, rows)?;
    } else {
        buffer_extent(bytes, 1)?;
        interleave_bitmap_extent(rows)?;
    }
    rounded_capacity(bytes)?;
    // Covers both the destination validity and the nullable mask's cleaned bits.
    interleave_bitmap_extent(rows)
}

fn byte_payload<T: ByteArrayType>(
    mask: &BooleanArray,
    truthy: &dyn Array,
    falsy: &dyn Array,
    large: bool,
    exact_interleave: bool,
    work: &mut CopyObservation<'_>,
) -> Result<(), CopyError> {
    let truthy: &GenericByteArray<T> = downcast(truthy)?;
    let falsy: &GenericByteArray<T> = downcast(falsy)?;
    let mut buffer = ByteBufferExtent::new(truthy.len(), exact_interleave)?;
    let mut start = 0;
    let mut previous = None;
    // This is the actual mask walk. NULL mask bits select falsy, even if their
    // hidden physical value bit is true. Each contiguous run is one zip extend.
    for row in 0..mask.len() {
        let choose_truthy = mask.is_valid(row) && mask.value(row);
        work.step()?;
        if let Some(previous) = previous
            && previous != choose_truthy
        {
            let source = if previous { truthy } else { falsy };
            extend_byte_span(&mut buffer, source, start, row, large)?;
            work.step()?;
            start = row;
        }
        previous = Some(choose_truthy);
    }
    if let Some(previous) = previous {
        let source = if previous { truthy } else { falsy };
        extend_byte_span(&mut buffer, source, start, mask.len(), large)?;
        work.step()?;
    }
    Ok(())
}

fn extend_byte_span<T: ByteArrayType>(
    buffer: &mut ByteBufferExtent,
    source: &GenericByteArray<T>,
    start: usize,
    end: usize,
    large: bool,
) -> Result<(), CopyError> {
    let offsets = source.value_offsets();
    let start = offset_value(offsets[start])?;
    let end = offset_value(offsets[end])?;
    // MutableArrayData::extend copies the whole offset span, including payload
    // under NULL source rows. Validity cannot reduce this combined byte extent.
    buffer.extend(
        end.checked_sub(start)
            .ok_or(CopyError::Invalid("copy source offsets are not monotonic"))?,
        large,
    )
}

// Matches Arrow 60's source dispatch for two non-scalar Datum arrays. The
// cleaned nullable mask chooses falsy at NULL rows; no source value is cast.
fn uses_interleave(mask: &BooleanArray, work: &mut CopyObservation<'_>) -> Result<bool, CopyError> {
    if mask.len() < 1024 {
        return Ok(false);
    }
    let mut runs = 0usize;
    let mut previous = false;
    for row in 0..mask.len() {
        let current = mask.is_valid(row) && mask.value(row);
        if current && !previous {
            runs = add(runs, 1)?;
        }
        previous = current;
        work.step()?;
    }
    Ok(runs > mask.len() / 8)
}

/// Preflight Arrow zip for two ordinary arrays, not scalar Datum fast paths.
/// The arrays must already have the same admitted complete carrier and exactly
/// mask.len() rows. NULL mask rows select falsy. The callback uses the caller's
/// original control: false is completed owned work, true an opaque boundary.
/// Encoded, nested and view carriers remain explicitly unsupported here.
/// Controls and numerical resource refusals are primary with no later callback;
/// ordinary errors and success observe their finish boundary. The caller still
/// owns host admission and boundaries around the actual Arrow copy.
pub fn preflight_zip(
    mask: &BooleanArray,
    truthy: &dyn Array,
    falsy: &dyn Array,
    mut observe: impl FnMut(bool) -> Result<(), KernelFailure>,
) -> Result<(), CopyError> {
    let mut work = CopyObservation(
        super::WorkObservation::Legacy(&mut observe),
        None,
        super::ScratchCoverage::ChildTables,
        None,
        None,
    );
    work.boundary()?;
    let result = (|| {
        let shape = truthy.len() == mask.len() && falsy.len() == mask.len();
        work.step()?;
        if !shape {
            return Err(CopyError::Invalid(
                "zip ordinary arrays must match mask length",
            ));
        }
        let ty = truthy.data_type();
        flat_extent(ty, truthy.len())?;
        work.step()?;
        let equal = work.exact_types(ty, falsy.data_type())?;
        work.step()?;
        if !equal {
            return Err(CopyError::Invalid(
                "zip sources differ from their complete carrier",
            ));
        }
        // Both flat to_data headers have at most two Buffer entries, no child
        // ArrayData, and zip's constructor borrows exactly two source headers.
        // These layout checks do not claim all Arrow closure/Arc allocations.
        buffer_extent(2, size_of::<Buffer>())?;
        buffer_extent(2, size_of::<&ArrayData>())?;
        work.step()?;
        let exact_interleave = uses_interleave(mask, &mut work)?;
        if exact_interleave {
            // Arrow's mask iterator collects this exact pair Vec before its
            // ordinary interleave author. It is distinct from copied payload.
            buffer_extent(mask.len(), size_of::<(usize, usize)>())?;
        }
        match ty {
            DataType::Utf8 => {
                byte_payload::<Utf8Type>(mask, truthy, falsy, false, exact_interleave, &mut work)?
            }
            DataType::Binary => {
                byte_payload::<BinaryType>(mask, truthy, falsy, false, exact_interleave, &mut work)?
            }
            DataType::LargeUtf8 => {
                byte_payload::<LargeUtf8Type>(
                    mask,
                    truthy,
                    falsy,
                    true,
                    exact_interleave,
                    &mut work,
                )?;
            }
            DataType::LargeBinary => {
                byte_payload::<LargeBinaryType>(
                    mask,
                    truthy,
                    falsy,
                    true,
                    exact_interleave,
                    &mut work,
                )?;
            }
            _ => {}
        }
        // Match zip's actual source to_data order and its truthy.len hint.
        work.boundary()?;
        let falsy_data = falsy.to_data();
        work.boundary()?;
        let truthy_data = truthy.to_data();
        work.boundary()?;
        if !exact_interleave {
            mutable_capacity(&truthy_data, truthy.len(), &mut work)?;
        }
        // Flat sources have no constructor children or combined dictionaries.
        // The second source contributes to selected payload, not a second hint.
        let same_data = work.exact_types(truthy_data.data_type(), falsy_data.data_type())?;
        work.step()?;
        if !same_data {
            return Err(CopyError::Invalid(
                "zip source data differs from its complete carrier",
            ));
        }
        Ok(())
    })();
    if matches!(&result, Err(CopyError::Control(_) | CopyError::Extent(_))) {
        return result;
    }
    work.boundary()?;
    result
}

#[cfg(test)]
#[path = "zip_tests.rs"]
mod tests;
