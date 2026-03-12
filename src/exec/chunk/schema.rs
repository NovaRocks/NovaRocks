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
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::ids::SlotId;
use crate::lower::type_lowering::primitive_type_from_desc;
use crate::types;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkFieldSchema {
    name: String,
    nullable: bool,
    type_desc: Option<types::TTypeDesc>,
    children: Vec<ChunkFieldSchema>,
}

impl ChunkFieldSchema {
    pub fn new(
        name: impl Into<String>,
        nullable: bool,
        type_desc: Option<types::TTypeDesc>,
    ) -> Self {
        Self::try_new(name, nullable, type_desc).unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_new(
        name: impl Into<String>,
        nullable: bool,
        type_desc: Option<types::TTypeDesc>,
    ) -> Result<Self, String> {
        let name = name.into();
        match type_desc {
            Some(desc) => Self::try_from_type_desc(name, nullable, desc),
            None => Ok(Self {
                name,
                nullable,
                type_desc: None,
                children: Vec::new(),
            }),
        }
    }

    pub fn try_from_type_desc(
        name: impl Into<String>,
        nullable: bool,
        desc: types::TTypeDesc,
    ) -> Result<Self, String> {
        let name = name.into();
        let nodes = desc
            .types
            .as_ref()
            .ok_or_else(|| format!("field '{}' type desc missing nodes", name))?;
        let (field_schema, next) = Self::from_desc_nodes(nodes, 0, name, nullable)?;
        if next != nodes.len() {
            return Err(format!(
                "field '{}' type desc has trailing nodes: consumed={} total={}",
                field_schema.name(),
                next,
                nodes.len()
            ));
        }
        Ok(field_schema)
    }

    pub fn from_field(field: &Field) -> Result<Self, String> {
        Self::from_arrow_field(field)
    }

    fn from_arrow_field(field: &Field) -> Result<Self, String> {
        let children = match field.data_type() {
            DataType::Struct(fields) => fields
                .iter()
                .map(|child| Self::from_field(child.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
            DataType::List(item) | DataType::LargeList(item) => {
                vec![Self::from_field(item.as_ref())?]
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(entry_fields) = entries.data_type() else {
                    return Err(format!(
                        "map field '{}' entries is not struct: {:?}",
                        field.name(),
                        entries.data_type()
                    ));
                };
                if entry_fields.len() != 2 {
                    return Err(format!(
                        "map field '{}' expected 2 entry fields, got {}",
                        field.name(),
                        entry_fields.len()
                    ));
                }
                vec![
                    Self::from_field(entry_fields[0].as_ref())?,
                    Self::from_field(entry_fields[1].as_ref())?,
                ]
            }
            _ => Vec::new(),
        };
        Ok(Self {
            name: field.name().to_string(),
            nullable: field.is_nullable(),
            type_desc: None,
            children,
        })
    }

    fn from_desc_nodes(
        nodes: &[types::TTypeNode],
        start: usize,
        name: String,
        nullable: bool,
    ) -> Result<(Self, usize), String> {
        let node = nodes.get(start).ok_or_else(|| {
            format!(
                "field '{}' type desc ended unexpectedly at node {}",
                name, start
            )
        })?;

        match node.type_ {
            t if t == types::TTypeNodeType::SCALAR => Ok((
                Self {
                    name,
                    nullable,
                    type_desc: Some(types::TTypeDesc::new(vec![node.clone()])),
                    children: Vec::new(),
                },
                start + 1,
            )),
            t if t == types::TTypeNodeType::STRUCT => {
                let struct_fields = node.struct_fields.as_ref().ok_or_else(|| {
                    format!("struct field '{}' type desc missing struct_fields", name)
                })?;
                let mut cursor = start + 1;
                let mut children = Vec::with_capacity(struct_fields.len());
                for (idx, struct_field) in struct_fields.iter().enumerate() {
                    let child_name = struct_field
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("field_{idx}"));
                    let (child, next) = Self::from_desc_nodes(nodes, cursor, child_name, true)?;
                    cursor = next;
                    children.push(child);
                }
                Ok((
                    Self {
                        name,
                        nullable,
                        type_desc: Some(types::TTypeDesc::new(nodes[start..cursor].to_vec())),
                        children,
                    },
                    cursor,
                ))
            }
            t if t == types::TTypeNodeType::ARRAY => {
                let (item, next) =
                    Self::from_desc_nodes(nodes, start + 1, "item".to_string(), true)?;
                Ok((
                    Self {
                        name,
                        nullable,
                        type_desc: Some(types::TTypeDesc::new(nodes[start..next].to_vec())),
                        children: vec![item],
                    },
                    next,
                ))
            }
            t if t == types::TTypeNodeType::MAP => {
                let (key, next) = Self::from_desc_nodes(nodes, start + 1, "key".to_string(), true)?;
                let (value, next) = Self::from_desc_nodes(nodes, next, "value".to_string(), true)?;
                Ok((
                    Self {
                        name,
                        nullable,
                        type_desc: Some(types::TTypeDesc::new(nodes[start..next].to_vec())),
                        children: vec![key, value],
                    },
                    next,
                ))
            }
            other => Err(format!(
                "unsupported type desc node {:?} for field '{}'",
                other, name
            )),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn type_desc(&self) -> Option<&types::TTypeDesc> {
        self.type_desc.as_ref()
    }

    pub fn primitive_type(&self) -> Option<types::TPrimitiveType> {
        self.type_desc.as_ref().and_then(primitive_type_from_desc)
    }

    pub fn json_semantic(&self) -> bool {
        self.primitive_type() == Some(types::TPrimitiveType::JSON)
    }

    pub fn children(&self) -> &[ChunkFieldSchema] {
        &self.children
    }

    pub fn struct_child(&self, idx: usize) -> Option<&ChunkFieldSchema> {
        self.children.get(idx)
    }

    pub fn list_item(&self) -> Option<&ChunkFieldSchema> {
        self.children.first()
    }

    pub fn map_key(&self) -> Option<&ChunkFieldSchema> {
        self.children.first()
    }

    pub fn map_value(&self) -> Option<&ChunkFieldSchema> {
        self.children.get(1)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkSlotSchema {
    slot_id: SlotId,
    field_schema: ChunkFieldSchema,
    unique_id: Option<i32>,
}

impl ChunkSlotSchema {
    pub fn new(
        slot_id: SlotId,
        name: impl Into<String>,
        nullable: bool,
        type_desc: Option<types::TTypeDesc>,
        unique_id: Option<i32>,
    ) -> Self {
        Self::try_new(slot_id, name, nullable, type_desc, unique_id)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_new(
        slot_id: SlotId,
        name: impl Into<String>,
        nullable: bool,
        type_desc: Option<types::TTypeDesc>,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        Ok(Self {
            slot_id,
            field_schema: ChunkFieldSchema::try_new(name, nullable, type_desc)?,
            unique_id,
        })
    }

    pub fn from_field(
        slot_id: SlotId,
        field: &Field,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        Ok(Self {
            slot_id,
            field_schema: ChunkFieldSchema::from_field(field)?,
            unique_id,
        })
    }

    pub fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub fn name(&self) -> &str {
        self.field_schema.name()
    }

    pub fn nullable(&self) -> bool {
        self.field_schema.nullable()
    }

    pub fn type_desc(&self) -> Option<&types::TTypeDesc> {
        self.field_schema.type_desc()
    }

    pub fn unique_id(&self) -> Option<i32> {
        self.unique_id
    }

    pub fn primitive_type(&self) -> Option<types::TPrimitiveType> {
        self.field_schema.primitive_type()
    }

    pub fn field_schema(&self) -> &ChunkFieldSchema {
        &self.field_schema
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkSchema {
    slots: Vec<ChunkSlotSchema>,
    index_by_slot: HashMap<SlotId, usize>,
}

pub type ChunkSchemaRef = Arc<ChunkSchema>;

impl ChunkSchema {
    pub fn try_new(slots: Vec<ChunkSlotSchema>) -> Result<Self, String> {
        let mut index_by_slot = HashMap::with_capacity(slots.len());
        for (idx, slot) in slots.iter().enumerate() {
            if index_by_slot.insert(slot.slot_id(), idx).is_some() {
                return Err(format!(
                    "duplicate slot id {} in chunk schema sidecar at index {}",
                    slot.slot_id(),
                    idx
                ));
            }
        }
        Ok(Self {
            slots,
            index_by_slot,
        })
    }

    pub fn empty() -> Self {
        Self {
            slots: Vec::new(),
            index_by_slot: HashMap::new(),
        }
    }

    pub fn slots(&self) -> &[ChunkSlotSchema] {
        &self.slots
    }

    pub fn slot_schema_from_arrow_field(
        slot_id: SlotId,
        field: &Field,
    ) -> Result<ChunkSlotSchema, String> {
        ChunkSlotSchema::from_field(slot_id, field, None)
    }

    pub fn try_from_schema_and_slot_ids(
        schema: &arrow::datatypes::Schema,
        slot_ids: &[SlotId],
    ) -> Result<Self, String> {
        if schema.fields().len() != slot_ids.len() {
            return Err(format!(
                "chunk schema slot id length mismatch: schema_fields={} slot_ids={}",
                schema.fields().len(),
                slot_ids.len()
            ));
        }
        let slots = schema
            .fields()
            .iter()
            .zip(slot_ids.iter().copied())
            .map(|(field, slot_id)| Self::slot_schema_from_arrow_field(slot_id, field.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(slots)
    }

    pub fn slot(&self, slot_id: SlotId) -> Option<&ChunkSlotSchema> {
        self.index_by_slot
            .get(&slot_id)
            .and_then(|idx| self.slots.get(*idx))
    }

    pub fn index_by_slot(&self) -> &HashMap<SlotId, usize> {
        &self.index_by_slot
    }
}

pub(super) fn validate_chunk_schema_against_batch(
    batch: &RecordBatch,
    chunk_schema: &ChunkSchema,
) -> Result<(), String> {
    if batch.num_columns() != chunk_schema.slots().len() {
        return Err(format!(
            "chunk schema sidecar length mismatch: batch_columns={} sidecar_slots={}",
            batch.num_columns(),
            chunk_schema.slots().len()
        ));
    }
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let slot = chunk_schema
            .slots()
            .get(idx)
            .ok_or_else(|| format!("missing chunk schema sidecar entry at index {}", idx))?;
        if field.is_nullable() != slot.nullable() {
            return Err(format!(
                "chunk schema nullable mismatch at index {}: batch={} sidecar={}",
                idx,
                field.is_nullable(),
                slot.nullable()
            ));
        }
    }
    Ok(())
}
