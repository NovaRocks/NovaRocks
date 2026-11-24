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
//! Original MAP_AGG computation, parameterized only by its actual state allocator.
use super::aggregate_map_source_failure as failure;
use crate::KernelFailure;
use crate::aggregate_scalar::checked_map_output_offset;
use crate::aggregate_scalar::{
    AggScalarValue, ScalarStateAllocator, ScalarWork, TrackedAggScalarValue,
    build_typed_scalar_array, tracked_scalar_from_typed_array, tracked_scalar_to_output,
};
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{Array, ArrayRef, MapArray, StructArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Fields};
use hashbrown::{HashMap, hash_map::DefaultHashBuilder};
#[cfg(test)]
use novarocks_type_contract::FunctionValueType;
use novarocks_type_contract::ValueTypeRef;
use std::sync::Arc;

mod key;
pub(super) use key::KeyIdentity;

#[derive(Debug)]
pub struct MapAggState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub(super) key_heads: HashMap<u64, usize, DefaultHashBuilder, A>,
    pub(super) entries: ScalarVec<MapEntry<A>, A>,
}

#[derive(Debug)]
pub(super) struct MapEntry<A: ScalarStateAllocator> {
    pub(super) key: TrackedAggScalarValue<A>,
    pub(super) value: Option<TrackedAggScalarValue<A>>,
    next_key: Option<usize>,
}

impl<A: ScalarStateAllocator> MapAggState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            key_heads: HashMap::with_hasher_in(DefaultHashBuilder::default(), allocator.clone()),
            entries: ScalarVec::new_in(allocator.clone()),
            allocator,
        }
    }
}

/// The same first-wins computation used by both update and partial-state merge.
fn append_entry<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    identity: &KeyIdentity,
    key: TrackedAggScalarValue<A>,
    value: Option<TrackedAggScalarValue<A>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    let hash = identity.hash(&key, work)?;
    append_entry_with_hash(state, identity, key, value, hash, work)
}

fn append_entry_with_hash<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    identity: &KeyIdentity,
    key: TrackedAggScalarValue<A>,
    value: Option<TrackedAggScalarValue<A>>,
    hash: u64,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    let head = state.key_heads.get(&hash).copied();
    let mut candidate = head;
    while let Some(row) = candidate {
        work.step()?;
        let prior = &state.entries[row];
        if identity.matches(&key, &prior.key, &state.allocator, work)? {
            return Ok(());
        }
        candidate = prior.next_key;
    }
    work.flush()?;
    if head.is_none() {
        state.key_heads.try_reserve(1).map_err(|source| {
            state
                .allocator
                .scalar_allocation_error("reserve map_agg key index", source)
        })?;
        work.flush()?;
    }
    state.entries.try_reserve(1).map_err(|source| {
        state
            .allocator
            .scalar_allocation_error("reserve map_agg entries", source)
    })?;
    work.flush()?;
    let row = state.entries.len();
    state.entries.push(MapEntry {
        key,
        value,
        next_key: head,
    });
    state.key_heads.insert(hash, row);
    Ok(())
}

/// Original packed-input fixture, kept to verify which rows the body reads.
#[cfg(test)]
pub struct MapUpdateInput {
    pub keys: ArrayRef,
    pub values: ArrayRef,
}
#[cfg(test)]
pub fn update_input(array: &ArrayRef) -> Result<MapUpdateInput, KernelFailure> {
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| failure::contract("map_agg expects struct input"))?;
    if struct_arr.num_columns() != 2 {
        return Err(failure::contract("map_agg expects 2 arguments"));
    }
    Ok(MapUpdateInput {
        keys: struct_arr.column(0).clone(),
        values: struct_arr.column(1).clone(),
    })
}

#[cfg(test)]
fn physical_test_identity(carrier: &DataType) -> KeyIdentity {
    KeyIdentity::prepare(&FunctionValueType::new(carrier.clone(), true), &mut |_| {
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap()
}

#[cfg(test)]
pub fn update_row<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    input: &MapUpdateInput,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    update_from_arrays(
        state,
        &physical_test_identity(input.keys.data_type()),
        (&input.keys, row, None),
        (&input.values, row, None),
        work,
    )
}

/// Distinct actual carrier addresses preserve Column, Selected and Constant inputs.
pub(super) fn update_from_arrays<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    identity: &KeyIdentity,
    (keys, key_row, key_declaration): (&ArrayRef, usize, Option<&ValueTypeRef>),
    (values, value_row, value_declaration): (&ArrayRef, usize, Option<&ValueTypeRef>),
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    work.step()?;
    let Some(key) =
        tracked_scalar_from_typed_array(keys, key_row, key_declaration, &state.allocator, work)?
    else {
        return Ok(());
    };
    // Preserve the original failure order even for a duplicate key.
    let value = tracked_scalar_from_typed_array(
        values,
        value_row,
        value_declaration,
        &state.allocator,
        work,
    )?;
    append_entry(state, identity, key, value, work)
}
pub struct MapMergeInput<'a> {
    map: &'a MapArray,
    keys: ArrayRef,
    values: ArrayRef,
}
pub fn merge_input(array: &ArrayRef) -> Result<MapMergeInput<'_>, KernelFailure> {
    let map = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| failure::contract("map_agg merge input must be MapArray"))?;
    Ok(MapMergeInput {
        map,
        keys: map.keys().clone(),
        values: map.values().clone(),
    })
}
#[cfg(test)]
pub fn merge_row<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    input: &MapMergeInput<'_>,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    merge_row_typed(
        state,
        &physical_test_identity(input.keys.data_type()),
        input,
        row,
        None,
        work,
    )
}
pub(super) fn merge_row_typed<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    identity: &KeyIdentity,
    input: &MapMergeInput<'_>,
    row: usize,
    declaration: Option<&ValueTypeRef>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), KernelFailure> {
    work.step()?;
    if input.map.is_null(row) {
        return Ok(());
    }
    let offsets = input.map.value_offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let key_declaration = declaration.and_then(|value| value.child_type_at(0));
    let value_declaration = declaration.and_then(|value| value.child_type_at(1));
    for idx in start..end {
        work.step()?;
        update_from_arrays(
            state,
            identity,
            (&input.keys, idx, key_declaration),
            (&input.values, idx, value_declaration),
            work,
        )?;
    }
    Ok(())
}
pub fn build_array<'state, A: ScalarStateAllocator + 'state, I>(
    target_type: &DataType,
    group_states: I,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, KernelFailure>
where
    I: ExactSizeIterator<Item = &'state MapAggState<A>>,
{
    build_array_checked_states(target_type, None, group_states.map(Ok), work)
}
pub fn build_array_checked_states<'state, A: ScalarStateAllocator + 'state, I>(
    target_type: &DataType,
    declaration: Option<&ValueTypeRef>,
    group_states: I,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, KernelFailure>
where
    I: ExactSizeIterator<Item = Result<&'state MapAggState<A>, KernelFailure>>,
{
    let (map_field, field_defs, ordered) = parse_map_type(target_type)?;
    let key_type = field_defs[0].data_type();
    let value_type = field_defs[1].data_type();
    work.flush()?;
    let mut key_values = Vec::<Option<AggScalarValue>>::new();
    let mut value_values = Vec::<Option<AggScalarValue>>::new();
    let mut offsets = Vec::with_capacity(group_states.len() + 1);
    work.flush()?;
    let mut validity = NullBufferBuilder::new(group_states.len());
    offsets.push(0_i32);
    let mut current: i32 = 0;
    for state in group_states {
        let state = state?;
        work.step()?;
        validity.append(!state.entries.is_empty());
        let next = checked_map_output_offset(current, state.entries.len())?;
        for MapEntry { key, value, .. } in &state.entries {
            work.step()?;
            key_values.push(Some(tracked_scalar_to_output(key, work)?));
            value_values.push(
                value
                    .as_ref()
                    .map(|value| tracked_scalar_to_output(value, work))
                    .transpose()?,
            );
        }
        current = next;
        offsets.push(current);
    }
    let mut out_keys = build_typed_scalar_array(
        key_type,
        declaration.and_then(|value| value.child_type_at(0)),
        key_values,
        work,
    )?;
    let mut out_values = build_typed_scalar_array(
        value_type,
        declaration.and_then(|value| value.child_type_at(1)),
        value_values,
        work,
    )?;
    if out_keys.data_type() != field_defs[0].data_type() {
        work.flush()?;
        out_keys = cast(&out_keys, field_defs[0].data_type()).map_err(failure::constructor)?;
        work.flush()?;
    }
    if out_values.data_type() != field_defs[1].data_type() {
        work.flush()?;
        out_values = cast(&out_values, field_defs[1].data_type()).map_err(failure::constructor)?;
        work.flush()?;
    }
    work.flush()?;
    let entries = StructArray::try_new(field_defs, vec![out_keys, out_values], None)
        .map_err(failure::constructor)?;
    let out = MapArray::try_new(
        map_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        validity.finish(),
        ordered,
    )
    .map_err(failure::constructor)?;
    work.flush()?;
    Ok(Arc::new(out))
}
pub fn parse_map_type(ty: &DataType) -> Result<(Arc<Field>, Fields, bool), KernelFailure> {
    let DataType::Map(field, ordered) = ty else {
        return Err(failure::contract(format_args!(
            "map_agg output type must be MAP, got {:?}",
            ty
        )));
    };
    let DataType::Struct(fields) = field.data_type().clone() else {
        return Err(failure::contract("map_agg map entries type must be STRUCT"));
    };
    if fields.len() != 2 {
        return Err(failure::contract(
            "map_agg map entries type must have 2 fields",
        ));
    }
    Ok((field.clone(), fields, *ordered))
}

#[cfg(test)]
#[path = "aggregate_map_core_tests.rs"]
mod tests;
