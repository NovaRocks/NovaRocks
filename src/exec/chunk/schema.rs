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
use crate::lower::type_lowering::{arrow_type_from_desc, primitive_type_from_desc};
use crate::types;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkFieldSchema {
    type_desc: Option<types::TTypeDesc>,
    children: Vec<ChunkFieldSchema>,
}

impl ChunkFieldSchema {
    pub fn new(
        _name: impl Into<String>,
        _nullable: bool,
        type_desc: Option<types::TTypeDesc>,
    ) -> Self {
        Self::try_new(type_desc).unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_new(type_desc: Option<types::TTypeDesc>) -> Result<Self, String> {
        match type_desc {
            Some(desc) => Self::try_from_type_desc("", false, desc),
            None => Ok(Self {
                type_desc: None,
                children: Vec::new(),
            }),
        }
    }

    pub fn try_from_type_desc(
        _name: impl Into<String>,
        _nullable: bool,
        desc: types::TTypeDesc,
    ) -> Result<Self, String> {
        let nodes = desc
            .types
            .as_ref()
            .ok_or_else(|| "field type desc missing nodes".to_string())?;
        let (field_schema, next) = Self::from_desc_nodes(nodes, 0)?;
        if next != nodes.len() {
            return Err(format!(
                "field type desc has trailing nodes: consumed={} total={}",
                next,
                nodes.len()
            ));
        }
        Ok(field_schema)
    }

    pub fn from_field(field: &Field) -> Result<Self, String> {
        Self::from_arrow_data_type(field.data_type())
    }

    fn from_arrow_data_type(data_type: &DataType) -> Result<Self, String> {
        let children = match data_type {
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
                        "map entries is not struct: {:?}",
                        entries.data_type()
                    ));
                };
                if entry_fields.len() != 2 {
                    return Err(format!(
                        "map entries expected 2 struct fields, got {}",
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
            type_desc: None,
            children,
        })
    }

    fn from_desc_nodes(nodes: &[types::TTypeNode], start: usize) -> Result<(Self, usize), String> {
        let node = nodes
            .get(start)
            .ok_or_else(|| format!("field type desc ended unexpectedly at node {}", start))?;

        match node.type_ {
            t if t == types::TTypeNodeType::SCALAR => Ok((
                Self {
                    type_desc: Some(types::TTypeDesc::new(vec![node.clone()])),
                    children: Vec::new(),
                },
                start + 1,
            )),
            t if t == types::TTypeNodeType::STRUCT => {
                let struct_fields = node
                    .struct_fields
                    .as_ref()
                    .ok_or_else(|| "struct type desc missing struct_fields".to_string())?;
                let mut cursor = start + 1;
                let mut children = Vec::with_capacity(struct_fields.len());
                for _ in struct_fields {
                    let (child, next) = Self::from_desc_nodes(nodes, cursor)?;
                    cursor = next;
                    children.push(child);
                }
                Ok((
                    Self {
                        type_desc: Some(types::TTypeDesc::new(nodes[start..cursor].to_vec())),
                        children,
                    },
                    cursor,
                ))
            }
            t if t == types::TTypeNodeType::ARRAY => {
                let (item, next) = Self::from_desc_nodes(nodes, start + 1)?;
                Ok((
                    Self {
                        type_desc: Some(types::TTypeDesc::new(nodes[start..next].to_vec())),
                        children: vec![item],
                    },
                    next,
                ))
            }
            t if t == types::TTypeNodeType::MAP => {
                let (key, next) = Self::from_desc_nodes(nodes, start + 1)?;
                let (value, next) = Self::from_desc_nodes(nodes, next)?;
                Ok((
                    Self {
                        type_desc: Some(types::TTypeDesc::new(nodes[start..next].to_vec())),
                        children: vec![key, value],
                    },
                    next,
                ))
            }
            other => Err(format!("unsupported type desc node {:?}", other)),
        }
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
    field: Field,
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
        let Some(type_desc) = type_desc else {
            return Err(format!(
                "chunk slot {} missing type_desc; use try_new_with_field for runtime fields",
                slot_id
            ));
        };
        Self::try_from_type_desc(slot_id, name, nullable, type_desc, unique_id)
    }

    pub fn new_with_field(
        slot_id: SlotId,
        field: Field,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Self {
        Self::try_new_with_field(slot_id, field, field_schema, unique_id)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    pub fn try_new_with_field(
        slot_id: SlotId,
        field: Field,
        field_schema: Option<ChunkFieldSchema>,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        Ok(Self {
            slot_id,
            field_schema: match field_schema {
                Some(schema) => schema,
                None => ChunkFieldSchema::from_field(&field)?,
            },
            field,
            unique_id,
        })
    }

    pub fn try_from_type_desc(
        slot_id: SlotId,
        name: impl Into<String>,
        nullable: bool,
        type_desc: types::TTypeDesc,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        let field_schema = ChunkFieldSchema::try_from_type_desc("", false, type_desc.clone())?;
        let data_type = arrow_type_from_desc(&type_desc).ok_or_else(|| {
            format!(
                "chunk slot {} has unsupported type desc for arrow conversion",
                slot_id
            )
        })?;
        Self::try_new_with_field(
            slot_id,
            Field::new(name, data_type, nullable),
            Some(field_schema),
            unique_id,
        )
    }

    pub fn from_field(
        slot_id: SlotId,
        field: &Field,
        unique_id: Option<i32>,
    ) -> Result<Self, String> {
        Self::try_new_with_field(slot_id, field.clone(), None, unique_id)
    }

    pub fn with_field(&self, field: Field) -> Result<Self, String> {
        Self::try_new_with_field(
            self.slot_id,
            field,
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    pub fn with_slot_id(&self, slot_id: SlotId) -> Result<Self, String> {
        Self::try_new_with_field(
            slot_id,
            self.field.clone(),
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    pub fn with_field_and_slot_id(&self, slot_id: SlotId, field: Field) -> Result<Self, String> {
        Self::try_new_with_field(
            slot_id,
            field,
            Some(self.field_schema.clone()),
            self.unique_id,
        )
    }

    pub fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn name(&self) -> &str {
        self.field.name()
    }

    pub fn nullable(&self) -> bool {
        self.field.is_nullable()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
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
    arrow_schema: SchemaRef,
    slot_ids: Vec<SlotId>,
    index_by_slot: HashMap<SlotId, usize>,
}

pub type ChunkSchemaRef = Arc<ChunkSchema>;

impl ChunkSchema {
    pub fn try_new(slots: Vec<ChunkSlotSchema>) -> Result<Self, String> {
        let mut index_by_slot = HashMap::with_capacity(slots.len());
        let mut slot_ids = Vec::with_capacity(slots.len());
        let mut fields = Vec::with_capacity(slots.len());
        for (idx, slot) in slots.iter().enumerate() {
            if index_by_slot.insert(slot.slot_id(), idx).is_some() {
                return Err(format!(
                    "duplicate slot id {} in chunk schema contract at index {}",
                    slot.slot_id(),
                    idx
                ));
            }
            slot_ids.push(slot.slot_id());
            fields.push(Arc::new(slot.field().clone()));
        }
        Ok(Self {
            slots,
            arrow_schema: Arc::new(arrow::datatypes::Schema::new(fields)),
            slot_ids,
            index_by_slot,
        })
    }

    pub fn empty() -> Self {
        Self {
            slots: Vec::new(),
            arrow_schema: Arc::new(arrow::datatypes::Schema::empty()),
            slot_ids: Vec::new(),
            index_by_slot: HashMap::new(),
        }
    }

    pub fn slots(&self) -> &[ChunkSlotSchema] {
        &self.slots
    }

    pub fn arrow_schema_ref(&self) -> SchemaRef {
        Arc::clone(&self.arrow_schema)
    }

    pub fn slot_ids(&self) -> &[SlotId] {
        &self.slot_ids
    }

    pub fn field(&self, idx: usize) -> Option<&Field> {
        self.slots.get(idx).map(ChunkSlotSchema::field)
    }

    pub fn field_by_slot(&self, slot_id: SlotId) -> Option<&Field> {
        self.slot(slot_id).map(ChunkSlotSchema::field)
    }

    pub fn slot_schema_from_arrow_field(
        slot_id: SlotId,
        field: &Field,
    ) -> Result<ChunkSlotSchema, String> {
        ChunkSlotSchema::from_field(slot_id, field, None)
    }

    pub fn try_ref_from_schema_and_slot_ids(
        schema: &Schema,
        slot_ids: &[SlotId],
    ) -> Result<ChunkSchemaRef, String> {
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
        Self::try_new(slots).map(Arc::new)
    }

    pub fn slot(&self, slot_id: SlotId) -> Option<&ChunkSlotSchema> {
        self.index_by_slot
            .get(&slot_id)
            .and_then(|idx| self.slots.get(*idx))
    }

    pub fn index_of(&self, slot_id: SlotId) -> Option<usize> {
        self.index_by_slot.get(&slot_id).copied()
    }

    pub fn index_by_slot(&self) -> &HashMap<SlotId, usize> {
        &self.index_by_slot
    }

    pub fn project_by_slots(&self, slot_ids: &[SlotId]) -> Result<Self, String> {
        let mut slots = Vec::with_capacity(slot_ids.len());
        for slot_id in slot_ids {
            let slot = self.slot(*slot_id).cloned().ok_or_else(|| {
                format!(
                    "chunk schema projection references missing slot {} (available={:?})",
                    slot_id,
                    self.slot_ids()
                )
            })?;
            slots.push(slot);
        }
        Self::try_new(slots)
    }

    pub fn with_fields_in_order(&self, fields: Vec<Field>) -> Result<Self, String> {
        if fields.len() != self.slots.len() {
            return Err(format!(
                "chunk schema field length mismatch: fields={} slots={}",
                fields.len(),
                self.slots.len()
            ));
        }
        let slots = self
            .slots
            .iter()
            .cloned()
            .zip(fields.into_iter())
            .map(|(slot, field)| slot.with_field(field))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(slots)
    }

    pub fn concat(parts: &[ChunkSchemaRef]) -> Result<Self, String> {
        let mut slots = Vec::new();
        for part in parts {
            slots.extend_from_slice(part.slots());
        }
        Self::try_new(slots)
    }
}

pub(super) fn validate_chunk_schema_against_batch(
    batch: &RecordBatch,
    chunk_schema: &ChunkSchema,
) -> Result<(), String> {
    if batch.num_columns() != chunk_schema.slots().len() {
        return Err(format!(
            "chunk schema contract length mismatch: batch_columns={} contract_slots={}",
            batch.num_columns(),
            chunk_schema.slots().len()
        ));
    }
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let expected = chunk_schema
            .field(idx)
            .ok_or_else(|| format!("missing chunk schema slot at index {}", idx))?;
        // Allow nullable batch fields to satisfy a non-nullable contract.
        // Source operators (e.g. FILE_SCAN for CSV) always produce nullable
        // columns; the actual NOT-NULL constraint is enforced downstream
        // (e.g. by the sink's row validation or the storage layer).
        let nullable_ok =
            field.is_nullable() == expected.is_nullable() || field.is_nullable();
        if field.name() != expected.name()
            || field.data_type() != expected.data_type()
            || !nullable_ok
        {
            return Err(format!(
                "chunk schema field mismatch at index {}: batch=({}, {:?}, {}) contract=({}, {:?}, {})",
                idx,
                field.name(),
                field.data_type(),
                field.is_nullable(),
                expected.name(),
                expected.data_type(),
                expected.is_nullable()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::BinaryArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{ChunkSchema, ChunkSlotSchema};
    use crate::common::ids::SlotId;
    use crate::exec::chunk::Chunk;
    use crate::lower::type_lowering::scalar_type_desc;
    use crate::types::TPrimitiveType;

    #[test]
    fn strict_rejects_duplicate_slot_id() {
        let err = ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("a", DataType::Int32, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("b", DataType::Int32, true),
                None,
                None,
            ),
        ])
        .expect_err("duplicate slot ids should fail");
        assert!(err.contains("duplicate slot id"), "err={}", err);
    }

    #[test]
    fn chunk_schema_sidecar_recovers_type_desc_and_unique_id() {
        let desc = scalar_type_desc(TPrimitiveType::HLL);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Binary, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from(vec![Some(b"x".as_slice())]))],
        )
        .expect("record batch");
        let chunk = Chunk::try_new_with_chunk_schema(
            batch,
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new(
                    SlotId::new(7),
                    "a",
                    true,
                    Some(desc.clone()),
                    Some(77),
                )])
                .expect("chunk schema"),
            ),
        )
        .expect("chunk");
        let slot = chunk
            .chunk_schema()
            .slot(SlotId::new(7))
            .expect("slot schema");
        assert_eq!(slot.type_desc(), Some(&desc));
        assert_eq!(slot.primitive_type(), Some(TPrimitiveType::HLL));
        assert_eq!(slot.name(), "a");
        assert_eq!(slot.unique_id(), Some(77));
    }
}
