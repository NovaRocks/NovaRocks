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

//! Recursive adaptation from physical Iceberg field IDs to the table schema.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBufferBuilder, FixedSizeListArray, GenericListArray, MapArray,
    OffsetSizeTrait, StructArray, make_array, new_null_array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use novarocks_spi::connector::ConnectorError;
use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType};

use crate::default_value::materialize_initial_default;
use crate::iceberg::spec::{NestedField, Schema};
use crate::schema_mapping::field_id_for_arrow_field;

use super::super::column_handle::corrupt;
use super::{
    IcebergPhysicalAdaptation, apply_adaptation, initial_default_field, physical_adaptation,
};

#[derive(Debug)]
pub(super) enum AdaptationError {
    Invalid(ConnectorError),
    TypeMismatch {
        field: String,
        source: DataType,
        target: DataType,
        read_error: ConnectorError,
    },
}

impl AdaptationError {
    pub(super) fn into_read_error(self) -> ConnectorError {
        match self {
            Self::Invalid(error)
            | Self::TypeMismatch {
                read_error: error, ..
            } => error,
        }
    }
}

impl From<ConnectorError> for AdaptationError {
    fn from(error: ConnectorError) -> Self {
        Self::Invalid(error)
    }
}

#[derive(Clone, Debug)]
pub(super) struct FieldAdaptation {
    target: FieldRef,
    kind: Adaptation,
    has_required_missing: bool,
}

#[derive(Clone, Debug)]
enum Adaptation {
    Leaf(IcebergPhysicalAdaptation),
    Struct(Vec<StructMember>),
    List(Box<FieldAdaptation>),
    Map(Box<FieldAdaptation>),
    InitialDefault(Arc<NestedField>),
    TypedNull,
    RequiredMissing,
}

#[derive(Clone, Debug)]
struct StructMember {
    source_slot: Option<usize>,
    adaptation: FieldAdaptation,
}

#[derive(Clone, Copy)]
enum BindingLayout {
    Canonical,
    ScanRoot,
    ScanChild,
}

impl BindingLayout {
    fn child(self) -> Self {
        match self {
            Self::Canonical => Self::Canonical,
            Self::ScanRoot | Self::ScanChild => Self::ScanChild,
        }
    }

    fn retain_leaf(self, source: &Field, target: &Field) -> bool {
        if matches!(self, Self::Canonical)
            || target
                .metadata()
                .get(NR_LOGICAL_TYPE_KEY)
                .and_then(|value| ValueLogicalType::from_metadata_value(value).ok())
                == Some(ValueLogicalType::Variant)
        {
            return false;
        }
        match (source.data_type(), target.data_type()) {
            (DataType::Utf8View, DataType::Utf8 | DataType::LargeUtf8)
            | (DataType::BinaryView, DataType::Binary | DataType::LargeBinary) => true,
            (DataType::Dictionary(keys, values), DataType::Utf8 | DataType::LargeUtf8) => {
                matches!(self, Self::ScanRoot)
                    && **keys == DataType::Int32
                    && matches!(values.as_ref(), DataType::Utf8 | DataType::Utf8View)
            }
            _ => false,
        }
    }
}

fn visit_field_leaf_ids(field: &Field, visit: &mut impl FnMut(i32)) -> Result<(), ConnectorError> {
    match field.data_type() {
        DataType::Struct(children) => {
            for child in children {
                visit_field_leaf_ids(child, visit)?;
            }
        }
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::FixedSizeList(child, _)
        | DataType::Map(child, _) => visit_field_leaf_ids(child, visit)?,
        _ => {
            if let Some(id) = field_id_for_arrow_field(field).map_err(corrupt)? {
                visit(id);
            }
        }
    }
    Ok(())
}

impl FieldAdaptation {
    pub(super) fn bind(
        source: Option<&Field>,
        target: &FieldRef,
        table_schema: &Schema,
    ) -> Result<Self, AdaptationError> {
        Self::bind_with_layout(source, target, BindingLayout::Canonical, table_schema)
    }

    pub(super) fn bind_for_scan(
        source: Option<&Field>,
        target: &FieldRef,
        table_schema: &Schema,
    ) -> Result<Self, AdaptationError> {
        Self::bind_with_layout(source, target, BindingLayout::ScanRoot, table_schema)
    }

    fn bind_with_layout(
        source: Option<&Field>,
        target: &FieldRef,
        layout: BindingLayout,
        table_schema: &Schema,
    ) -> Result<Self, AdaptationError> {
        // Resolve the physical field by ID before choosing its byte layout.
        // This preserves decoder storage even when struct members were reordered.
        let target = match source {
            Some(source) if layout.retain_leaf(source, target) => Arc::new(
                target
                    .as_ref()
                    .clone()
                    .with_data_type(source.data_type().clone()),
            ),
            _ => Arc::clone(target),
        };
        let kind = match source {
            None => match initial_default_field(table_schema, target_id(&target)?) {
                Some(field) => Adaptation::InitialDefault(field),
                None if target.is_nullable() => Adaptation::TypedNull,
                None => Adaptation::RequiredMissing,
            },
            Some(source) => Self::bind_present(source, &target, layout.child(), table_schema)?,
        };
        let has_required_missing = match &kind {
            Adaptation::Struct(children) => children
                .iter()
                .any(|child| child.adaptation.has_required_missing),
            Adaptation::List(child) | Adaptation::Map(child) => child.has_required_missing,
            Adaptation::RequiredMissing => true,
            _ => false,
        };
        Ok(Self {
            target,
            kind,
            has_required_missing,
        })
    }

    fn bind_present(
        source: &Field,
        target: &Field,
        child_layout: BindingLayout,
        table_schema: &Schema,
    ) -> Result<Adaptation, AdaptationError> {
        let physical = source.data_type();
        let desired = target.data_type();
        if physical == desired {
            return Ok(Adaptation::Leaf(IcebergPhysicalAdaptation::Identity));
        }
        Ok(match (physical, desired) {
            (DataType::Struct(source), DataType::Struct(target)) => Adaptation::Struct(
                bind_struct_members(source, target, child_layout, table_schema)?,
            ),
            (DataType::List(source), DataType::List(target))
            | (DataType::LargeList(source), DataType::LargeList(target)) => {
                Adaptation::List(Box::new(Self::bind_with_layout(
                    same_id(source, target)?,
                    target,
                    child_layout,
                    table_schema,
                )?))
            }
            (DataType::FixedSizeList(source, n), DataType::FixedSizeList(target, m)) if n == m => {
                Adaptation::List(Box::new(Self::bind_with_layout(
                    same_id(source, target)?,
                    target,
                    child_layout,
                    table_schema,
                )?))
            }
            (DataType::Map(source, s), DataType::Map(target, t)) if s == t => {
                // Entries carry two Iceberg fields, the key and the value.
                Adaptation::Map(Box::new(Self::bind_with_layout(
                    Some(source),
                    target,
                    child_layout,
                    table_schema,
                )?))
            }
            _ => {
                let logical_variant = target
                    .metadata()
                    .get(NR_LOGICAL_TYPE_KEY)
                    .and_then(|value| ValueLogicalType::from_metadata_value(value).ok())
                    == Some(ValueLogicalType::Variant);
                let adaptation = physical_adaptation(physical, desired, logical_variant).map_err(
                    |read_error| AdaptationError::TypeMismatch {
                        field: target.name().clone(),
                        source: physical.clone(),
                        target: desired.clone(),
                        read_error,
                    },
                )?;
                Adaptation::Leaf(adaptation)
            }
        })
    }

    /// Visit only leaf identities for which the scan consumes this file's
    /// values. A known ID under an unrelated physical parent is not a bound
    /// field and its counts cannot describe the target's NULL/default values.
    pub(super) fn visit_bound_leaf_ids(
        &self,
        visit: &mut impl FnMut(i32),
    ) -> Result<(), ConnectorError> {
        match &self.kind {
            Adaptation::Leaf(_) => visit_field_leaf_ids(&self.target, visit),
            Adaptation::Struct(children) => {
                for child in children {
                    child.adaptation.visit_bound_leaf_ids(visit)?;
                }
                Ok(())
            }
            Adaptation::List(child) | Adaptation::Map(child) => child.visit_bound_leaf_ids(visit),
            Adaptation::InitialDefault(_) | Adaptation::TypedNull | Adaptation::RequiredMissing => {
                Ok(())
            }
        }
    }

    pub(super) fn retained_heap_bytes(&self) -> usize {
        match &self.kind {
            Adaptation::Struct(children) => {
                children.capacity() * size_of::<StructMember>()
                    + children
                        .iter()
                        .map(|child| child.adaptation.retained_heap_bytes())
                        .sum::<usize>()
            }
            Adaptation::List(child) | Adaptation::Map(child) => {
                size_of::<Self>() + child.retained_heap_bytes()
            }
            _ => 0,
        }
    }

    pub(super) fn missing_required(&self) -> Option<&str> {
        match &self.kind {
            Adaptation::RequiredMissing => Some(self.target.name()),
            Adaptation::Struct(children) => children
                .iter()
                .find_map(|child| child.adaptation.missing_required()),
            Adaptation::List(child) | Adaptation::Map(child) => child.missing_required(),
            _ => None,
        }
    }

    pub(super) fn materialize(
        &self,
        source: &ArrayRef,
        path: &str,
    ) -> Result<ArrayRef, ConnectorError> {
        self.apply(Some(source), source.len(), None, path)
    }

    fn apply(
        &self,
        source: Option<&ArrayRef>,
        len: usize,
        reachable: Option<&NullBuffer>,
        path: &str,
    ) -> Result<ArrayRef, ConnectorError> {
        let source = match &self.kind {
            Adaptation::InitialDefault(field) => {
                return materialize_initial_default(field, self.target.data_type(), len)
                    .map_err(|error| corrupt(format!("iceberg initial default: {error}")));
            }
            Adaptation::RequiredMissing => {
                if len != 0 && reachable.is_none_or(|mask| mask.null_count() < len) {
                    return Err(corrupt(format!(
                        "iceberg data file {path} is missing required field {} without an initial default",
                        self.target.name()
                    )));
                }
                return Ok(new_null_array(self.target.data_type(), len));
            }
            Adaptation::TypedNull => return Ok(new_null_array(self.target.data_type(), len)),
            _ => source.ok_or_else(|| corrupt("iceberg reader lost a bound field"))?,
        };
        let reachable = self
            .has_required_missing
            .then(|| NullBuffer::union(reachable, source.nulls()))
            .flatten();
        match &self.kind {
            Adaptation::Leaf(adaptation) => {
                apply_adaptation(source, self.target.as_ref(), path, *adaptation)
            }
            Adaptation::Struct(children) => {
                let structure = source
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| corrupt("iceberg reader lost a bound struct carrier"))?;
                let mut fields = Vec::with_capacity(children.len());
                let mut values = Vec::with_capacity(children.len());
                for member in children {
                    let value = member.adaptation.apply(
                        member.source_slot.map(|slot| structure.column(slot)),
                        len,
                        reachable.as_ref(),
                        path,
                    )?;
                    fields.push(runtime_field(&member.adaptation.target, &value));
                    values.push(value);
                }
                StructArray::try_new_with_length(
                    fields.into(),
                    values,
                    structure.nulls().cloned(),
                    len,
                )
                .map(|array| Arc::new(array) as ArrayRef)
                .map_err(|error| corrupt(format!("iceberg nested struct in {path}: {error}")))
            }
            Adaptation::List(item) => self.apply_list(item, source, reachable.as_ref(), path),
            Adaptation::Map(entries) => {
                let map = source
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| corrupt("iceberg reader lost a bound map carrier"))?;
                let values = Arc::new(map.entries().clone()) as ArrayRef;
                let mask = entries.offset_reachability(
                    values.len(),
                    map.value_offsets(),
                    reachable.as_ref(),
                );
                let output = entries.apply(Some(&values), values.len(), mask.as_ref(), path)?;
                let DataType::Map(_, sorted) = self.target.data_type() else {
                    unreachable!("map adaptation has a map target");
                };
                rebuild_container(
                    source,
                    DataType::Map(runtime_field(&entries.target, &output), *sorted),
                    output,
                    path,
                )
            }
            Adaptation::InitialDefault(_) | Adaptation::TypedNull | Adaptation::RequiredMissing => {
                unreachable!("absent fields have no physical carrier")
            }
        }
    }

    fn apply_list(
        &self,
        item: &FieldAdaptation,
        source: &ArrayRef,
        reachable: Option<&NullBuffer>,
        path: &str,
    ) -> Result<ArrayRef, ConnectorError> {
        let values;
        let mask;
        let wrap: fn(FieldRef) -> DataType;
        match self.target.data_type() {
            DataType::List(_) => {
                let list = downcast_list::<i32>(source)?;
                values = list.values();
                mask = item.offset_reachability(values.len(), list.value_offsets(), reachable);
                wrap = DataType::List;
            }
            DataType::LargeList(_) => {
                let list = downcast_list::<i64>(source)?;
                values = list.values();
                mask = item.offset_reachability(values.len(), list.value_offsets(), reachable);
                wrap = DataType::LargeList;
            }
            DataType::FixedSizeList(_, size) => {
                let list = source
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        corrupt("iceberg reader lost a bound fixed-size list carrier")
                    })?;
                values = list.values();
                let mask = reachable.map(|reachable| {
                    let mut bits = BooleanBufferBuilder::new(values.len());
                    for row in 0..list.len() {
                        bits.append_n(*size as usize, reachable.is_valid(row));
                    }
                    NullBuffer::new(bits.finish())
                });
                let output = item.apply(
                    item.has_source().then_some(values),
                    values.len(),
                    mask.as_ref(),
                    path,
                )?;
                return rebuild_container(
                    source,
                    DataType::FixedSizeList(runtime_field(&item.target, &output), *size),
                    output,
                    path,
                );
            }
            _ => unreachable!("list adaptation has a list target"),
        }
        let output = item.apply(
            item.has_source().then_some(values),
            values.len(),
            mask.as_ref(),
            path,
        )?;
        rebuild_container(
            source,
            wrap(runtime_field(&item.target, &output)),
            output,
            path,
        )
    }

    fn has_source(&self) -> bool {
        !matches!(
            self.kind,
            Adaptation::InitialDefault(_) | Adaptation::TypedNull | Adaptation::RequiredMissing
        )
    }

    /// Entries outside a sliced list or under a NULL parent are storage
    /// slots only. They cannot make a missing required value observable.
    fn offset_reachability<O: OffsetSizeTrait>(
        &self,
        values_len: usize,
        offsets: &[O],
        reachable: Option<&NullBuffer>,
    ) -> Option<NullBuffer> {
        if !self.has_required_missing {
            return None;
        }
        let start = offsets[0].as_usize();
        let end = offsets[offsets.len() - 1].as_usize();
        if reachable.is_none() && start == 0 && end == values_len {
            return None;
        }
        let mut bits = BooleanBufferBuilder::new(values_len);
        bits.append_n(start, false);
        for (row, window) in offsets.windows(2).enumerate() {
            bits.append_n(
                window[1].as_usize() - window[0].as_usize(),
                reachable.is_none_or(|mask| mask.is_valid(row)),
            );
        }
        bits.append_n(values_len - end, false);
        Some(NullBuffer::new(bits.finish()))
    }
}

fn bind_struct_members(
    source: &Fields,
    target: &Fields,
    child_layout: BindingLayout,
    table_schema: &Schema,
) -> Result<Vec<StructMember>, AdaptationError> {
    let mut slots = HashMap::with_capacity(source.len());
    for (slot, field) in source.iter().enumerate() {
        if let Some(id) = field_id_for_arrow_field(field).map_err(corrupt)?
            && slots.insert(id, slot).is_some()
        {
            return Err(corrupt(format!("iceberg struct declares field id {id} twice")).into());
        }
    }
    target
        .iter()
        .map(|field| {
            let id = target_id(field)?;
            let source_slot = slots.get(&id).copied();
            let adaptation = FieldAdaptation::bind_with_layout(
                source_slot.map(|slot| source[slot].as_ref()),
                field,
                child_layout,
                table_schema,
            )?;
            Ok(StructMember {
                source_slot,
                adaptation,
            })
        })
        .collect()
}

fn target_id(target: &Field) -> Result<i32, ConnectorError> {
    field_id_for_arrow_field(target)
        .map_err(corrupt)?
        .ok_or_else(|| corrupt(format!("iceberg target field {} has no id", target.name())))
}

fn same_id<'a>(source: &'a Field, target: &Field) -> Result<Option<&'a Field>, ConnectorError> {
    Ok(
        (field_id_for_arrow_field(source).map_err(corrupt)? == Some(target_id(target)?))
            .then_some(source),
    )
}

fn runtime_field(target: &FieldRef, array: &ArrayRef) -> FieldRef {
    Arc::new(
        target
            .as_ref()
            .clone()
            .with_data_type(array.data_type().clone())
            .with_nullable(target.is_nullable() || array.null_count() > 0),
    )
}

fn rebuild_container(
    source: &ArrayRef,
    data_type: DataType,
    child: ArrayRef,
    path: &str,
) -> Result<ArrayRef, ConnectorError> {
    source
        .to_data()
        .into_builder()
        .data_type(data_type)
        .child_data(vec![child.to_data()])
        .build()
        .map(make_array)
        .map_err(|error| corrupt(format!("iceberg nested container in {path}: {error}")))
}

fn downcast_list<O: OffsetSizeTrait>(
    source: &ArrayRef,
) -> Result<&GenericListArray<O>, ConnectorError> {
    source
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .ok_or_else(|| corrupt("iceberg reader lost a bound list carrier"))
}
