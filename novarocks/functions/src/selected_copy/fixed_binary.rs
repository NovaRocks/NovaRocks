// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original selected-copy constructor for a fixed-size byte carrier.
//! Source custody and result lifetime remain with the caller. This writer's
//! actual payload, bitmap and metadata requests precede their allocations;
//! MutableArrayData invoices do not describe this constructor.

use super::{CopyConstructionError, CopyError, take_host};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelControlObservation, KernelEvaluationControl};
use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueSchemaCreation;
use std::{alloc::Layout, sync::Arc};

// A control-work segment, never a value-width or output-size restriction.
const COPY_BYTES: usize = 64 * 1024;

fn output_extent(width: usize, rows: usize) -> Result<usize, CopyError> {
    let required = width as u128 * rows as u128;
    // Arrow 60.1 uses usize for value offsets; i32 constrains each value's
    // width, not the complete payload. This writer owns a Vec<u8>, so check
    // its actual Rust backing layout before requesting destination storage.
    let bytes = usize::try_from(required).map_err(|_| CopyError::backing_bytes(required))?;
    Layout::array::<u8>(bytes).map_err(|source| CopyError::layout::<u8>(bytes, source))?;
    Ok(bytes)
}

/// Check the fixed-byte writer's actual Rust payload backing extent.
/// This proves representation only; it never authorizes destination storage.
pub fn preflight_fixed_binary_constructor_extent(width: i32, rows: usize) -> Result<(), CopyError> {
    let width = usize::try_from(width)
        .map_err(|_| CopyError::Invalid("copy frozen carrier has a negative width"))?;
    output_extent(width, rows).map(|_| ())
}

fn reserve<T, H>(
    count: usize,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<T>, CopyConstructionError<H>> {
    work.flush().map_err(CopyError::Control)?;
    let mut result = Vec::new();
    if count != 0 {
        let layout =
            Layout::array::<T>(count).map_err(|source| CopyError::layout::<T>(count, source))?;
        creation
            .request_layout(layout)
            .map_err(CopyConstructionError::Creator)?;
        result.try_reserve_exact(count).map_err(|source| {
            CopyError::Control(crate::KernelFailure::from_domain(
                crate::allocation_failure::AllocationRefusal::new(
                    "selected fixed-byte construction",
                    source,
                ),
            ))
        })?;
        work.flush().map_err(CopyError::Control)?;
    }
    Ok(result)
}

fn append_value(
    output: &mut Vec<u8>,
    value: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), CopyError> {
    // The complete output capacity was requested and reserved once. These
    // bounded appends neither grow a Vec nor invoke an unbounded Arrow copy.
    for bytes in value.chunks(COPY_BYTES) {
        work.flush().map_err(CopyError::Control)?;
        output.extend_from_slice(bytes);
        work.flush().map_err(CopyError::Control)?;
    }
    Ok(())
}

pub(super) fn copy_selected<H>(
    sources: &[ArrayRef],
    choices: &[(usize, usize)],
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, CopyConstructionError<H>> {
    let observed = KernelControlObservation::new(control);
    let mut work = EvaluationCheckpoints::new(&observed);
    work.flush().map_err(CopyError::Control)?;
    let first = sources
        .first()
        .ok_or(CopyError::Invalid("fixed-byte construction has no source"))?;
    let DataType::FixedSizeBinary(width) = first.data_type() else {
        return Err(CopyError::Invalid("fixed-byte construction needs its exact carrier").into());
    };
    let width = *width;
    let value_bytes = usize::try_from(width)
        .map_err(|_| CopyError::Invalid("copy frozen carrier has a negative width"))?;
    let payload_bytes = output_extent(value_bytes, choices.len())?;
    for source in sources {
        work.step().map_err(CopyError::Control)?;
        if source.data_type() != first.data_type() {
            return Err(CopyError::Invalid("fixed-byte construction source types differ").into());
        }
    }
    let mut has_nulls = false;
    for &(source, row) in choices {
        work.step().map_err(CopyError::Control)?;
        let source = sources.get(source).ok_or(CopyError::Invalid(
            "fixed-byte construction source index is out of bounds",
        ))?;
        if row >= source.len() {
            return Err(CopyError::Invalid("fixed-byte construction row is out of bounds").into());
        }
        has_nulls |= source.is_null(row);
    }
    // A zero-width carrier needs this exact-length bitmap even when all rows
    // are valid: Arrow cannot infer its row count from the empty payload.
    let has_bitmap = has_nulls || width == 0;
    let mut typed = reserve::<&FixedSizeBinaryArray, H>(sources.len(), creation, &mut work)?;
    for source in sources {
        work.step().map_err(CopyError::Control)?;
        typed.push(source.as_any().downcast_ref().ok_or(CopyError::Invalid(
            "fixed-byte construction source does not match its carrier",
        ))?);
    }
    work.flush().map_err(CopyError::Control)?;
    // The existing flat-node envelope includes the private immutable Buffer
    // owners and concrete Array Arc. Do not charge adoption headers or another
    // Array wrapper in addition. Payload and bitmap Vec backings are separate.
    let metadata = take_host::result_node_metadata_upper(first.data_type(), 1, has_bitmap, 0)?;
    creation
        .observe_opaque_upper_bound(metadata)
        .map_err(CopyConstructionError::Creator)?;
    let mut payload = reserve::<u8, H>(payload_bytes, creation, &mut work)?;
    let bitmap_bytes = if has_bitmap {
        choices.len().div_ceil(8)
    } else {
        0
    };
    let mut validity = reserve::<u8, H>(bitmap_bytes, creation, &mut work)?;
    let mut byte = 0u8;
    let mut null_count = 0usize;
    for (ordinal, &(source, row)) in choices.iter().enumerate() {
        work.step().map_err(CopyError::Control)?;
        let source = typed[source];
        // Preserve the selected physical bytes of NULL slots as the original
        // MutableArrayData copy did. NULL does not authorize dropping payload.
        append_value(&mut payload, source.value(row), &mut work)?;
        if has_bitmap {
            if source.is_valid(row) {
                byte |= 1 << (ordinal % 8);
            } else {
                null_count += 1;
            }
            if ordinal % 8 == 7 {
                validity.push(byte);
                byte = 0;
            }
        }
    }
    if has_bitmap && !choices.len().is_multiple_of(8) {
        validity.push(byte);
    }
    work.flush().map_err(CopyError::Control)?;
    let nulls = if has_bitmap {
        let bits = BooleanBuffer::new(Buffer::from_vec(validity), 0, choices.len());
        // SAFETY: the loop writes exactly one validity bit per selected row,
        // counts exactly the zero bits in that same row domain, and leaves the
        // unused final-byte tail outside BooleanBuffer's explicit bit length.
        // This avoids an opaque whole-bitmap popcount after controlled copying.
        Some(unsafe { NullBuffer::new_unchecked(bits, null_count) })
    } else {
        None
    };
    let array =
        FixedSizeBinaryArray::try_new(width, Buffer::from_vec(payload), nulls).map_err(|_| {
            CopyError::Invalid("fixed-byte construction disagrees with its checked extent")
        })?;
    let output = Arc::new(array) as ArrayRef;
    work.finish().map_err(CopyError::Control)?;
    Ok(output)
}

#[cfg(test)]
#[path = "fixed_binary_tests.rs"]
mod tests;
