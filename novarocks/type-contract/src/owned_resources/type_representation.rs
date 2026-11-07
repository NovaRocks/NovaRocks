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

//! Structural representation counts for existing protocol payload budgets.
//! Shared backing is counted per retained occurrence, without deduplication.
//! These are neither allocator measurements nor capacity grants; policy and
//! the current-exchange lifetime remain owned by each protocol caller.

use arrow_schema::{DataType, FieldRef};
use std::alloc::Layout;

fn sum(values: impl IntoIterator<Item = Option<usize>>) -> Option<usize> {
    values
        .into_iter()
        .try_fold(0usize, |total, value| total.checked_add(value?))
}
fn mul(left: usize, right: usize) -> Option<usize> {
    left.checked_mul(right)
}
fn add(left: usize, right: usize) -> Option<usize> {
    left.checked_add(right)
}
fn type_layout_bytes(layout: Result<Layout, super::layout::LayoutResourceError>) -> Option<usize> {
    layout.ok().map(|layout| layout.size())
}
fn arc_slice_bytes<T>(len: usize) -> Option<usize> {
    Layout::array::<T>(len)
        .ok()
        .and_then(|layout| super::layout::arc_layout(layout).ok())
        .map(|layout| layout.size())
}

pub fn function_value_type_retained_bytes(value: &crate::FunctionValueType) -> Option<usize> {
    sum([
        semantic_value_bytes(value.value_type()),
        value_layout_bytes(value.typed_binding().layout()),
    ])
}
fn semantic_value_bytes(value: &crate::ValueTypeRef) -> Option<usize> {
    use crate::ValueKind;
    let own = type_layout_bytes(crate::ValueTypeRef::storage_allocation_layout())?;
    let shape = match value.kind() {
        ValueKind::Scalar(data_type) => arrow_type_dynamic_bytes(data_type),
        ValueKind::Struct(fields) => sum([
            mul(fields.len(), std::mem::size_of::<crate::ValueField>()),
            sum(fields.iter().map(|field| {
                sum([
                    type_layout_bytes(crate::ValueField::storage_allocation_layout()),
                    Some(field.name().len()),
                    semantic_value_bytes(field.value_type()),
                ])
            })),
        ]),
        ValueKind::Union(fields) => sum([
            mul(fields.len(), std::mem::size_of::<crate::ValueUnionField>()),
            sum(fields.iter().map(|field| {
                sum([
                    type_layout_bytes(crate::ValueField::storage_allocation_layout()),
                    Some(field.field.name().len()),
                    semantic_value_bytes(field.field.value_type()),
                ])
            })),
        ]),
        _ => sum((0..value.child_count()).map(|index| {
            semantic_value_bytes(value.child_type_at(index).expect("checked semantic child"))
        })),
    }?;
    sum([Some(own), Some(shape)])
}
fn value_layout_bytes(value: &crate::ValueLayout) -> Option<usize> {
    let mut bytes = sum([
        type_layout_bytes(crate::ValueLayout::storage_allocation_layout()),
        arrow_type_dynamic_bytes(value.data_type()),
    ])?;
    let mut index = 0;
    while let Some(child) = value.materialized_child_at(index) {
        bytes = add(
            bytes,
            sum([
                Some(std::mem::size_of::<crate::ValueLayout>()),
                value_layout_bytes(child),
            ])?,
        )?;
        index += 1;
    }
    Some(bytes)
}

/// Dynamic backing of the actual Arrow metadata representation; inline
/// DataType storage is already part of its enclosing semantic/layout owner.
pub fn arrow_type_dynamic_bytes(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::Timestamp(_, timezone) => timezone
            .as_ref()
            .map_or(Some(0), |timezone| arc_slice_bytes::<u8>(timezone.len())),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => arrow_field_retained_bytes(field),
        DataType::Struct(fields) => sum([
            arc_slice_bytes::<FieldRef>(fields.len()),
            sum(fields.iter().map(arrow_field_retained_bytes)),
        ]),
        DataType::Union(fields, _) => sum([
            arc_slice_bytes::<(i8, FieldRef)>(fields.len()),
            sum(fields
                .iter()
                .map(|(_, field)| arrow_field_retained_bytes(field))),
        ]),
        DataType::Dictionary(key, value) => sum([
            mul(2, std::mem::size_of::<DataType>()),
            arrow_type_dynamic_bytes(key),
            arrow_type_dynamic_bytes(value),
        ]),
        DataType::RunEndEncoded(run_ends, values) => sum([
            arrow_field_retained_bytes(run_ends),
            arrow_field_retained_bytes(values),
        ]),
        _ => Some(0),
    }
}

/// Count the original Arc<Field> backing and dynamic metadata text. Spare
/// HashMap/String capacity does not become serialized protocol payload.
pub fn arrow_field_retained_bytes(field: &FieldRef) -> Option<usize> {
    sum([
        arc_slice_bytes::<arrow_schema::Field>(1),
        Some(field.name().len()),
        arrow_type_dynamic_bytes(field.data_type()),
        super::arrow_metadata::retained_backing_upper_bound(field.metadata().len()).ok(),
        sum(field
            .metadata()
            .iter()
            .map(|(key, value)| key.len().checked_add(value.len()))),
    ])
}
