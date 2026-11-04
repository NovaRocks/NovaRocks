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

//! The original single-field codec's schema request arithmetic, shared with
//! the standard stream writer. Private FlatBuffer field-location geometry is
//! supplied by its locked caller and remains an operation model, not an exact
//! allocator fact. No field, metadata key or logical domain is interpreted.

use super::IpcGeometryError;
use arrow_schema::DataType;
use std::alloc::Layout;

fn add(a: usize, b: usize) -> Result<usize, IpcGeometryError> {
    a.checked_add(b).ok_or(IpcGeometryError::Overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, IpcGeometryError> {
    a.checked_mul(b).ok_or(IpcGeometryError::Overflow)
}
#[derive(Clone, Copy, Debug)]
pub struct SchemaGeometry {
    pub backing: usize,
    pub tables: usize,
    pub metadata_entries: usize,
    pub string_bytes: usize,
    pub field_occurrences: usize,
    pub type_occurrences: usize,
    pub metadata_nonempty_fields: usize,
    pub child_offset_items: usize,
    pub child_offset_requests: usize,
    pub union_id_items: usize,
    pub union_id_requests: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct SchemaResourceSource {
    /// Bound on the original source walker's pending entries.
    pub walker_node_bound: usize,
    pub source_retained_bytes: usize,
    pub scratch_work: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct SchemaAllocationMetrics {
    pub request_bytes: usize,
    pub request_count: usize,
    pub work_upper_bound: usize,
}
#[derive(Default)]
pub struct SchemaRequestAccumulator {
    pub bytes: usize,
    pub count: usize,
}
impl SchemaRequestAccumulator {
    pub fn layout<E: From<IpcGeometryError>>(
        &mut self,
        layout: Layout,
        copies: usize,
        observe: &mut dyn FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        let result: Result<(), IpcGeometryError> = (|| {
            if layout.size() != 0 && copies != 0 {
                self.bytes = add(self.bytes, mul(layout.size(), copies)?)?;
                self.count = add(self.count, copies)?;
            }
            Ok(())
        })();
        observe()?;
        result.map_err(E::from)
    }
    pub fn exact<T, E: From<IpcGeometryError>>(
        &mut self,
        capacity: usize,
        copies: usize,
        observe: &mut dyn FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        let result: Result<(), IpcGeometryError> = (|| {
            let layout = Layout::array::<T>(capacity).map_err(|_| IpcGeometryError::Overflow)?;
            if layout.size() == 0 || copies == 0 {
                return Ok(());
            }
            self.bytes = add(self.bytes, mul(layout.size(), copies)?)?;
            self.count = add(self.count, copies)?;
            Ok(())
        })();
        observe()?;
        result.map_err(E::from)
    }
    pub fn geometric<T, E: From<IpcGeometryError>>(
        &mut self,
        target: usize,
        initial: usize,
        copies: usize,
        observe: &mut dyn FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        if target == 0 {
            return Ok(());
        }
        let mut capacity = initial;
        self.exact::<T, E>(capacity, copies, observe)?;
        while capacity < target {
            let next = if capacity < 4 {
                Ok(4)
            } else {
                mul(capacity, 2)
            };
            observe()?;
            capacity = next.map_err(E::from)?;
            self.exact::<T, E>(capacity, copies, observe)?;
        }
        Ok(())
    }
}

/// The same FlatBuffer schema/message backing arithmetic as the original
/// codec author. Table slots/widths are from Arrow's supported schema/message
/// tables; include padding and undeduplicated vtables before builder allocation.
pub fn schema_backing_upper_bound(
    fields: usize,
    types: usize,
    metadata: usize,
    strings: usize,
    string_bytes: usize,
) -> Result<usize, IpcGeometryError> {
    const TABLE_SLOTS: usize = 7;
    const TABLE_BYTES: usize = TABLE_SLOTS * (8 + 7) + (4 + 7) + (4 + 2 * TABLE_SLOTS);
    let tables = add(add(fields, types)?, add(metadata, 2)?)?;
    let vectors = add(add(mul(types, 2)?, fields)?, 1)?;
    let payload = add(add(mul(fields, 8)?, mul(metadata, 4)?)?, 4)?;
    let strings = add(string_bytes, mul(strings, 8)?)?;
    add(
        add(mul(tables, TABLE_BYTES)?, strings)?,
        add(add(payload, mul(vectors, 7)?)?, 11)?,
    )
}

pub fn schema_writer_allocation_metrics<E: From<IpcGeometryError>>(
    schema: SchemaGeometry,
    source: SchemaResourceSource,
    field_location: Layout,
    observe: &mut dyn FnMut() -> Result<(), E>,
) -> Result<SchemaAllocationMetrics, E> {
    let mut requests = SchemaRequestAccumulator::default();
    requests.exact::<u8, E>(schema.backing, 2, observe)?;
    if source.walker_node_bound == 0 {
        return Err(IpcGeometryError::SourceModel.into());
    }
    requests.geometric::<novarocks_type_contract::TypeTraversalFrame<'_>, E>(
        source.walker_node_bound,
        1,
        4,
        observe,
    )?;
    requests.exact::<(&str, &str), E>(schema.metadata_entries, 1, observe)?;
    // WIPOffset is the standard four-byte relative wire offset. Its original
    // caller verifies the locked concrete offset/location layouts separately.
    requests.exact::<u32, E>(schema.metadata_entries, 1, observe)?;
    requests.count = add(
        requests.count,
        mul(2, schema.metadata_nonempty_fields)?
            .checked_sub(if schema.metadata_entries == 0 { 0 } else { 2 })
            .ok_or(IpcGeometryError::Overflow)?,
    )?;
    requests.exact::<u32, E>(schema.child_offset_items, 1, observe)?;
    requests.count = add(
        requests.count,
        schema
            .child_offset_requests
            .checked_sub(usize::from(schema.child_offset_items != 0))
            .ok_or(IpcGeometryError::Overflow)?,
    )?;
    requests.exact::<i32, E>(schema.union_id_items, 1, observe)?;
    requests.count = add(
        requests.count,
        schema
            .union_id_requests
            .checked_sub(usize::from(schema.union_id_items != 0))
            .ok_or(IpcGeometryError::Overflow)?,
    )?;
    for capacity in [4, 8] {
        let layout = Layout::from_size_align(
            mul(field_location.size(), capacity)?,
            field_location.align(),
        )
        .map_err(|_| IpcGeometryError::Overflow)?;
        requests.layout(layout, 1, observe)?;
    }
    requests.geometric::<u32, E>(schema.tables, 4, 1, observe)?;
    let associations = mul(
        mul(4, mul(schema.metadata_entries, schema.metadata_entries)?)?,
        add(schema.string_bytes, 1)?,
    )?;
    let vtables = mul(40, mul(schema.tables, schema.tables)?)?;
    let storage = add(mul(4, requests.bytes)?, requests.count)?;
    let source_bytes = mul(
        source.source_retained_bytes,
        add(schema.metadata_entries, mul(5, schema.field_occurrences)?)?,
    )?;
    let structure = add(
        mul(32, add(schema.field_occurrences, schema.type_occurrences)?)?,
        source.scratch_work,
    )?;
    let work_upper_bound = add(
        add(add(source_bytes, associations)?, vtables)?,
        add(storage, structure)?,
    )?;
    Ok(SchemaAllocationMetrics {
        request_bytes: requests.bytes,
        request_count: requests.count,
        work_upper_bound,
    })
}

/// Allocation arities of the original Arrow IPC type-emission branches. This
/// does not author a type or widen a caller's carrier support. Both source
/// visitors borrow the type-contract structural walker.
#[derive(Clone, Copy, Debug)]
pub struct TypeHeaderGeometry {
    pub children: usize,
    pub union_ids: usize,
    pub timezone_bytes: Option<usize>,
}
pub fn type_header_geometry(ty: &DataType) -> TypeHeaderGeometry {
    let (children, union_ids) = match ty {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => (1, 0),
        DataType::Struct(fields) => (fields.len(), 0),
        DataType::Union(fields, _) => (fields.len(), fields.len()),
        DataType::RunEndEncoded(_, _) => (2, 0),
        _ => (0, 0),
    };
    TypeHeaderGeometry {
        children,
        union_ids,
        timezone_bytes: match ty {
            DataType::Timestamp(_, Some(zone)) => Some(zone.len()),
            _ => None,
        },
    }
}

// The ONE FlatBuffers 25.9.23 builder.rs FieldLoc source mirror, extracted
// unchanged from the original codec allocation model. This is a private
// implementation operation layout, not a wire grammar or allocator fact.
#[doc(hidden)]
#[expect(
    dead_code,
    reason = "The pinned FlatBuffers field-location model is used only for its allocation layout"
)]
pub struct LockedFieldLocation {
    offset: u32,
    slot: u16,
}
pub fn locked_field_location_layout() -> Layout {
    Layout::new::<LockedFieldLocation>()
}
