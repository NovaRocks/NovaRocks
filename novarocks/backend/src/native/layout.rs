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

//! Backend-owned output layout decoding for native wire plans.

use std::collections::HashMap;
use std::sync::Arc;

use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks_protocol::common;

use super::type_decode::decode_field_type;

pub(crate) fn chunk_schema_from_output_columns(
    columns: &[common::OutputColumn],
    path: FieldPath,
) -> Result<ChunkSchemaRef, ProtocolError> {
    decode_output_layout(columns, path).map(|layout| layout.chunk_schema())
}

#[derive(Clone, Debug)]
pub(crate) struct NativeOutputLayout {
    slot_ids: Vec<SlotId>,
    chunk_schema: ChunkSchemaRef,
    slot_schemas: Vec<novarocks::exec::chunk::ChunkSlotSchema>,
}

impl NativeOutputLayout {
    fn new(
        slot_ids: Vec<SlotId>,
        chunk_schema: ChunkSchemaRef,
        slot_schemas: Vec<novarocks::exec::chunk::ChunkSlotSchema>,
    ) -> Self {
        Self {
            slot_ids,
            chunk_schema,
            slot_schemas,
        }
    }

    pub(crate) fn slot_ids(&self) -> &[SlotId] {
        &self.slot_ids
    }
    pub(crate) fn chunk_schema(&self) -> ChunkSchemaRef {
        self.chunk_schema.clone()
    }
    pub(crate) fn slot_schemas(&self) -> &[novarocks::exec::chunk::ChunkSlotSchema] {
        &self.slot_schemas
    }
}

pub(crate) fn decode_output_layout(
    columns: &[common::OutputColumn],
    path: FieldPath,
) -> Result<NativeOutputLayout, ProtocolError> {
    let mut slots = Vec::with_capacity(columns.len());
    let mut seen = HashMap::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let column_path = path.clone().index(index);
        let slot_id = SlotId::new(column.column_id);
        if let Some(first_index) = seen.insert(slot_id, index) {
            return Err(error(
                column_path.field("column_id"),
                ProtocolErrorKind::InconsistentFields,
                format!(
                    "duplicate OutputColumn.column_id {} at index {} (first seen at index {})",
                    column.column_id, index, first_index
                ),
            ));
        }
        let type_desc = column.r#type.as_ref().ok_or_else(|| {
            error(
                column_path.clone().field("type"),
                ProtocolErrorKind::MissingField,
                format!(
                    "OutputColumn.type missing for column_id={} name='{}' at index {}",
                    column.column_id, column.name, index
                ),
            )
        })?;
        let field = decode_field_type(&column.name, column.nullable, type_desc).map_err(|detail| {
            error(
                column_path.field("type"),
                ProtocolErrorKind::InvalidValue,
                format!(
                    "OutputColumn.type decode failed for column_id={} name='{}' at index {}: {}",
                    column.column_id, column.name, index, detail
                ),
            )
        })?;
        slots.push(
            ChunkSchema::slot_schema_from_arrow_field(slot_id, &field)
                .map_err(|detail| error(path.clone(), ProtocolErrorKind::InvalidValue, detail))?,
        );
    }
    let chunk_schema = ChunkSchema::try_new(slots.clone())
        .map(Arc::new)
        .map_err(|detail| error(path, ProtocolErrorKind::InvalidValue, detail))?;
    Ok(NativeOutputLayout::new(
        slots.iter().map(|slot| slot.slot_id()).collect(),
        chunk_schema,
        slots,
    ))
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ProtocolFamily::Native, path, kind, detail)
}

#[cfg(test)]
mod tests {
    use super::decode_output_layout;
    use novarocks::common::ids::SlotId;
    use novarocks::protocol::FieldPath;
    use novarocks_protocol::common;

    fn int_column(column_id: u32) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: "id".to_string(),
            nullable: true,
            r#type: Some(common::TypeDesc {
                kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                    r#type: common::PrimitiveType::Int as i32,
                    ..Default::default()
                })),
            }),
            is_internal: false,
        }
    }

    #[test]
    fn decodes_layout_slots_schema_and_slot_schemas_together() {
        let layout = decode_output_layout(&[int_column(3)], FieldPath::root("columns"))
            .expect("layout decodes");

        assert_eq!(layout.slot_ids(), &[SlotId::new(3)]);
        assert_eq!(layout.chunk_schema().slots().len(), 1);
        assert_eq!(layout.slot_schemas()[0].slot_id(), SlotId::new(3));
    }

    #[test]
    fn preserves_duplicate_slot_error_path() {
        let error =
            decode_output_layout(&[int_column(3), int_column(3)], FieldPath::root("columns"))
                .expect_err("duplicate slots must fail");

        assert_eq!(
            error.to_string(),
            "native protocol error at columns[1].column_id (inconsistent fields): duplicate OutputColumn.column_id 3 at index 1 (first seen at index 0)"
        );
    }
}
