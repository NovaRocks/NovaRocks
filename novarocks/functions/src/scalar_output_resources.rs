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

//! Resource facts for the ONE original aggregate scalar output builder.
//! Only already-read scalar graph capacities and exact output type structure
//! are inspected here. This module does not decode, compare or cast a value.

use crate::KernelFailure;
use crate::aggregate_scalar::{
    AggScalarValue as V, ScalarStateAllocator, ScalarWork, TrackedAggScalarValue as T,
    tracked_scalar_heap_capacity,
};
use arrow_schema::DataType;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScalarOutputResources {
    pub(crate) slots: usize,
    pub(crate) heap_bytes: usize,
    pub(crate) rows: usize,
    pub(crate) type_nodes: usize,
    pub(crate) type_metadata_bytes: usize,
}
fn add(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_mul(b).ok_or(KernelFailure::ResourceExhausted)
}
fn type_nodes(ty: &DataType, work: &mut ScalarWork<'_, '_>) -> Result<usize, KernelFailure> {
    work.step()?;
    let mut count = 1;
    match ty {
        DataType::List(field) | DataType::Map(field, _) => {
            count = add(count, type_nodes(field.data_type(), work)?)?;
        }
        DataType::Struct(fields) => {
            for field in fields {
                count = add(count, type_nodes(field.data_type(), work)?)?;
            }
        }
        _ => {}
    }
    Ok(count)
}
fn owned_graph(value: &V, work: &mut ScalarWork<'_, '_>) -> Result<(usize, usize), KernelFailure> {
    work.step()?;
    let mut slots = 1;
    let mut heap = match value {
        V::Utf8(bytes) => bytes.capacity(),
        V::Binary(bytes) => bytes.capacity(),
        V::List(items) | V::Struct(items) => mul(items.capacity(), size_of::<Option<V>>())?,
        V::Map(items) => mul(items.capacity(), size_of::<(Option<V>, Option<V>)>())?,
        _ => 0,
    };
    let mut child = |value: &Option<V>| -> Result<(), KernelFailure> {
        match value {
            None => {
                slots = add(slots, 1)?;
                work.step()?;
            }
            Some(value) => {
                let (child_slots, child_heap) = owned_graph(value, work)?;
                slots = add(slots, child_slots)?;
                heap = add(heap, child_heap)?;
            }
        }
        Ok(())
    };
    match value {
        V::List(items) | V::Struct(items) => {
            for value in items {
                child(value)?;
            }
        }
        V::Map(items) => {
            for (key, value) in items {
                child(key)?;
                child(value)?;
            }
        }
        _ => {}
    }
    Ok((slots, heap))
}
fn tracked_slots<A: ScalarStateAllocator>(
    value: &T<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<usize, KernelFailure> {
    work.step()?;
    let mut slots = 1;
    let mut child = |value: &Option<T<A>>| -> Result<(), KernelFailure> {
        slots = add(
            slots,
            match value {
                None => {
                    work.step()?;
                    1
                }
                Some(value) => tracked_slots(value, work)?,
            },
        )?;
        Ok(())
    };
    match value {
        T::List(items) | T::Struct(items) => {
            for value in items {
                child(value)?;
            }
        }
        T::Map(items) => {
            for (key, value) in items {
                child(key)?;
                child(value)?;
            }
        }
        _ => {}
    }
    Ok(slots)
}
impl ScalarOutputResources {
    pub(crate) fn from_owned(
        ty: &DataType,
        values: &[Option<V>],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, KernelFailure> {
        let mut slots = 0;
        let mut heap_bytes = 0;
        for value in values {
            let (nodes, heap) = match value {
                None => {
                    work.step()?;
                    (1, 0)
                }
                Some(value) => owned_graph(value, work)?,
            };
            slots = add(slots, nodes)?;
            heap_bytes = add(heap_bytes, heap)?;
        }
        Ok(Self {
            slots,
            heap_bytes,
            rows: values.len(),
            type_nodes: type_nodes(ty, work)?,
            // ONE Arrow metadata fact includes field names, exact nested
            // metadata map capacities, timezone bytes, and physical dtype.
            type_metadata_bytes: ty.size(),
        })
    }
    pub(crate) fn from_tracked<A: ScalarStateAllocator>(
        ty: &DataType,
        value: Option<&T<A>>,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Self, KernelFailure> {
        let (slots, heap_bytes) = match value {
            None => {
                work.step()?;
                (1, 0)
            }
            Some(value) => (
                tracked_slots(value, work)?,
                tracked_scalar_heap_capacity(value, work)?,
            ),
        };
        Ok(Self {
            slots,
            heap_bytes,
            rows: 1,
            type_nodes: type_nodes(ty, work)?,
            // ONE Arrow metadata fact includes field names, exact nested
            // metadata map capacities, timezone bytes, and physical dtype.
            type_metadata_bytes: ty.size(),
        })
    }
    /// Includes conversion copies, nested flattening scratch, original default
    /// Arrow builders, their growth peak, and the custody transition. The 1024
    /// element/byte initial capacities are the pinned original Arrow builders'
    /// real defaults; all variable byte payload comes from the actual graph.
    pub(crate) fn operation_upper_bound(self) -> Result<usize, KernelFailure> {
        if self.rows == 0 {
            // The original analytic empty branch constructs a zero-length null
            // carrier; none of the default scalar builders is invoked here.
            return add(
                crate::arrow_result_custody::custody_type_metadata_upper_bound(self.type_nodes)?,
                add(
                    mul(self.type_metadata_bytes, 4)?,
                    mul(self.type_nodes, size_of::<arrow_buffer::i256>())?,
                )?,
            );
        }

        let expanded_slots = mul(add(self.slots, self.rows)?, self.type_nodes)?;
        // Struct NULL rows materialize one child slot per exact target field;
        // recursive List/Map moves and Struct scratch can coexist with input.
        let scratch = mul(expanded_slots, mul(size_of::<Option<V>>(), 4)?)?;
        // Native output width is at most Decimal256's 32 bytes. Four copies
        // cover Vec capacity growth, old+new realloc peak and Arrow conversion.
        let native = mul(expanded_slots, 4 * size_of::<arrow_buffer::i256>())?;
        let default_buffers = mul(self.type_nodes, 4 * 1024 * size_of::<arrow_buffer::i256>())?;
        let variable = mul(self.heap_bytes, 4)?;
        let metadata =
            crate::arrow_result_custody::custody_type_metadata_upper_bound(self.type_nodes)?;

        add(
            add(add(scratch, native)?, add(default_buffers, variable)?)?,
            add(metadata, mul(self.type_metadata_bytes, 4)?)?,
        )
    }
    pub(crate) fn owned_heap_bytes(
        values: &[Option<V>],
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<usize, KernelFailure> {
        let mut bytes = 0;
        for value in values {
            work.step()?;
            if let Some(value) = value {
                bytes = add(bytes, owned_graph(value, work)?.1)?;
            }
        }
        Ok(bytes)
    }
}

/// Independently built flat Arrow values reserve their actual buffer demand,
/// Arrow's rounded validity capacity, and the shared custody metadata envelope.
pub(crate) fn flat_arrow_output_peak(buffers: usize, rows: usize) -> Result<usize, KernelFailure> {
    let valid_bytes = add(rows / 8, usize::from(!rows.is_multiple_of(8)))?;
    let validity = add(valid_bytes, 63)? / 64 * 64;
    let metadata = crate::arrow_result_custody::custody_type_metadata_upper_bound(1)?;
    add(add(buffers, validity)?, metadata)
}

pub(crate) fn flat_utf8_output_peak(payload: usize, rows: usize) -> Result<usize, KernelFailure> {
    let offsets = mul(add(rows, 1)?, size_of::<i32>())?;
    flat_arrow_output_peak(add(payload, offsets)?, rows)
}
