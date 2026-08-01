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

//! Proto layout lowering placeholder.

use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use arrow::datatypes::{Field, Schema, SchemaRef};

use super::error::NativeFragmentLeafDecodeError;
#[cfg(test)]
use crate::native::type_decode::decode_field_type;
use novarocks::common::ids::SlotId;
#[cfg(test)]
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks::protocol::ProtocolErrorKind;
use novarocks_protocol::common;

#[derive(Clone, Debug, Default)]
pub(crate) struct Layout {
    order: Vec<SlotId>,
    index: HashMap<SlotId, usize>,
}

impl Layout {
    #[allow(dead_code)]
    pub(crate) fn for_slots(slots: impl IntoIterator<Item = SlotId>) -> Self {
        let mut order = Vec::new();
        let mut index = HashMap::new();
        for slot in slots {
            index.entry(slot).or_insert_with(|| {
                let idx = order.len();
                order.push(slot);
                idx
            });
        }
        Self { order, index }
    }

    pub(crate) fn order(&self) -> &[SlotId] {
        &self.order
    }

    #[allow(dead_code)]
    pub(crate) fn slot_ids(&self) -> &[SlotId] {
        self.order()
    }

    pub(crate) fn contains_slot(&self, slot: SlotId) -> bool {
        self.index.contains_key(&slot)
    }

    pub(crate) fn index_of_slot(&self, slot: SlotId) -> Option<usize> {
        self.index.get(&slot).copied()
    }

    pub(crate) fn index_of_column_id(&self, column_id: u32) -> Option<usize> {
        self.index_of_slot(SlotId::new(column_id))
    }

    pub(crate) fn resolve_column_id(
        &self,
        column_id: u32,
    ) -> Result<SlotId, NativeFragmentLeafDecodeError> {
        let slot = SlotId::new(column_id);
        if self.contains_slot(slot)
            && let Some(index) = self.index.get(&slot)
            && self.order.get(*index) == Some(&slot)
        {
            Ok(slot)
        } else {
            Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "column_id",
                format!(
                    "ColumnRef column_id={} not found in input layout",
                    column_id
                ),
            ))
        }
    }
}

#[cfg(test)]
pub(crate) fn layout_from_output_columns(
    cols: &[common::OutputColumn],
) -> Result<Layout, NativeFragmentLeafDecodeError> {
    let decoded = decode_output_columns(cols)?;
    Ok(Layout::for_slots(decoded.slot_ids))
}

#[cfg(test)]
pub(crate) fn schema_from_output_columns(
    cols: &[common::OutputColumn],
) -> Result<SchemaRef, NativeFragmentLeafDecodeError> {
    let decoded = decode_output_columns(cols)?;
    Ok(schema_from_fields(decoded.fields))
}

#[cfg(test)]
pub(crate) fn chunk_schema_from_output_columns(
    cols: &[common::OutputColumn],
) -> Result<ChunkSchemaRef, NativeFragmentLeafDecodeError> {
    let decoded = decode_output_columns(cols)?;
    if decoded.fields.len() != decoded.slot_ids.len() {
        return Err(NativeFragmentLeafDecodeError::at_collection(
            ProtocolErrorKind::InconsistentFields,
            format!(
                "OutputColumn schema/slot length mismatch: fields={} slot_ids={}",
                decoded.fields.len(),
                decoded.slot_ids.len()
            ),
        ));
    }
    let slots = decoded
        .fields
        .iter()
        .zip(decoded.slot_ids.iter().copied())
        .map(|(field, slot_id)| ChunkSchema::slot_schema_from_arrow_field(slot_id, field))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_collection(ProtocolErrorKind::InvalidValue, error)
        })?;
    ChunkSchema::try_new(slots).map(Arc::new).map_err(|error| {
        NativeFragmentLeafDecodeError::at_collection(ProtocolErrorKind::InvalidValue, error)
    })
}

#[cfg(test)]
pub(crate) fn slot_schemas_from_output_columns(
    cols: &[common::OutputColumn],
) -> Result<Vec<ChunkSlotSchema>, NativeFragmentLeafDecodeError> {
    let decoded = decode_output_columns(cols)?;
    if decoded.fields.len() != decoded.slot_ids.len() {
        return Err(NativeFragmentLeafDecodeError::at_collection(
            ProtocolErrorKind::InconsistentFields,
            format!(
                "OutputColumn schema/slot length mismatch: fields={} slot_ids={}",
                decoded.fields.len(),
                decoded.slot_ids.len()
            ),
        ));
    }
    decoded
        .fields
        .iter()
        .zip(decoded.slot_ids.iter().copied())
        .map(|(field, slot_id)| ChunkSchema::slot_schema_from_arrow_field(slot_id, field))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_collection(ProtocolErrorKind::InvalidValue, error)
        })
}

#[cfg(test)]
pub(crate) fn slot_ids_from_output_columns(
    cols: &[common::OutputColumn],
) -> Result<Vec<SlotId>, NativeFragmentLeafDecodeError> {
    Ok(decode_output_columns(cols)?.slot_ids)
}

#[cfg(test)]
fn schema_from_fields(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(
        fields.into_iter().map(Arc::new).collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
struct DecodedOutputColumns {
    slot_ids: Vec<SlotId>,
    fields: Vec<Field>,
}

#[cfg(test)]
fn decode_output_columns(
    cols: &[common::OutputColumn],
) -> Result<DecodedOutputColumns, NativeFragmentLeafDecodeError> {
    let mut slot_ids = Vec::with_capacity(cols.len());
    let mut fields = Vec::with_capacity(cols.len());
    let mut seen = HashMap::with_capacity(cols.len());

    for (idx, col) in cols.iter().enumerate() {
        let slot_id = SlotId::new(col.column_id);
        if let Some(first_idx) = seen.insert(slot_id, idx) {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "column_id",
                format!(
                    "duplicate OutputColumn.column_id {} at index {} (first seen at index {})",
                    col.column_id, idx, first_idx
                ),
            )
            .prepend_index(idx));
        }
        let type_desc = col.r#type.as_ref().ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "type",
                format!(
                    "OutputColumn.type missing for column_id={} name='{}' at index {}",
                    col.column_id, col.name, idx
                ),
            )
            .prepend_index(idx)
        })?;
        let field = decode_field_type(&col.name, col.nullable, type_desc).map_err(|err| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "type",
                format!(
                    "OutputColumn.type decode failed for column_id={} name='{}' at index {}: {}",
                    col.column_id, col.name, idx, err
                ),
            )
            .prepend_index(idx)
        })?;

        slot_ids.push(slot_id);
        fields.push(field);
    }

    Ok(DecodedOutputColumns { slot_ids, fields })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields};

    use super::*;
    use novarocks::common::ids::SlotId;
    use novarocks::proto::common;
    use novarocks_types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

    fn scalar_type(primitive: common::PrimitiveType) -> common::TypeDesc {
        common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: primitive as i32,
                ..Default::default()
            })),
        }
    }

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        let primitive = match data_type {
            DataType::Int32 => common::PrimitiveType::Int,
            DataType::Int64 => common::PrimitiveType::Bigint,
            DataType::Utf8 => common::PrimitiveType::Varchar,
            other => panic!("test fixture type is not scalar: {other:?}"),
        };
        scalar_type(primitive)
    }

    fn output_column(
        column_id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable,
            is_internal: false,
        }
    }

    #[test]
    fn builds_schema_chunk_schema_and_layout_in_output_column_order() {
        let cols = vec![
            output_column(7, "id", DataType::Int64, false),
            output_column(3, "name", DataType::Utf8, true),
        ];

        let layout = layout_from_output_columns(&cols).expect("layout");
        assert_eq!(layout.order(), &[SlotId::new(7), SlotId::new(3)]);
        assert_eq!(layout.index_of_slot(SlotId::new(7)), Some(0));
        assert_eq!(layout.index_of_slot(SlotId::new(3)), Some(1));
        assert_eq!(layout.index_of_column_id(3), Some(1));
        assert_eq!(
            layout.resolve_column_id(7).expect("resolve slot"),
            SlotId::new(7)
        );

        let schema = schema_from_output_columns(&cols).expect("arrow schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0), &Field::new("id", DataType::Int64, false));
        assert_eq!(schema.field(1), &Field::new("name", DataType::Utf8, true));

        let chunk_schema = chunk_schema_from_output_columns(&cols).expect("chunk schema");
        assert_eq!(chunk_schema.slot_ids(), &[SlotId::new(7), SlotId::new(3)]);
        assert_eq!(chunk_schema.index_of(SlotId::new(3)), Some(1));
        assert_eq!(chunk_schema.field(0), Some(schema.field(0)));
        assert_eq!(
            chunk_schema.field_by_slot(SlotId::new(3)),
            Some(schema.field(1))
        );
    }

    #[test]
    fn decodes_decimal_list_struct_and_logical_field_metadata() {
        let payload_field = field_with_logical_type(
            Field::new("payload", DataType::Utf8, true),
            LogicalType::Json,
        );
        let complex_field = Field::new(
            "complex",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("price", DataType::Decimal128(18, 4), false)),
                Arc::new(Field::new(
                    "tags",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                )),
                Arc::new(payload_field.clone()),
            ])),
            true,
        );
        let cols = vec![common::OutputColumn {
            column_id: 99,
            name: "complex".to_string(),
            r#type: Some(common::TypeDesc {
                kind: Some(common::type_desc::Kind::Strct(common::StructType {
                    fields: vec![
                        common::StructField {
                            name: "price".to_string(),
                            r#type: Some(common::TypeDesc {
                                kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                                    r#type: common::PrimitiveType::Decimal128 as i32,
                                    precision: Some(18),
                                    scale: Some(4),
                                    ..Default::default()
                                })),
                            }),
                        },
                        common::StructField {
                            name: "tags".to_string(),
                            r#type: Some(common::TypeDesc {
                                kind: Some(common::type_desc::Kind::List(Box::new(
                                    common::ListType {
                                        element: Some(Box::new(scalar_type(
                                            common::PrimitiveType::Varchar,
                                        ))),
                                    },
                                ))),
                            }),
                        },
                        common::StructField {
                            name: "payload".to_string(),
                            r#type: Some(scalar_type(common::PrimitiveType::Json)),
                        },
                    ],
                })),
            }),
            nullable: true,
            is_internal: true,
        }];

        let schema = schema_from_output_columns(&cols).expect("arrow schema");
        assert_eq!(schema.field(0).name(), "complex");
        assert!(schema.field(0).is_nullable());
        let DataType::Struct(fields) = schema.field(0).data_type() else {
            panic!(
                "expected struct field, got {:?}",
                schema.field(0).data_type()
            );
        };
        assert_eq!(fields[0].data_type(), &DataType::Decimal128(18, 4));
        assert_eq!(
            fields[1].data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
        assert_eq!(fields[2].name(), "payload");
        assert_eq!(fields[2].data_type(), &DataType::Utf8);
        assert_eq!(
            logical_type_of_field(fields[2].as_ref()),
            Some(LogicalType::Json)
        );

        let slot_schemas = slot_schemas_from_output_columns(&cols).expect("slot schemas");
        assert_eq!(slot_schemas.len(), 1);
        assert_eq!(slot_schemas[0].slot_id(), SlotId::new(99));
        assert_eq!(
            slot_schemas[0]
                .field_schema()
                .struct_child(2)
                .expect("payload child")
                .logical_type(),
            Some(LogicalType::Json)
        );
    }

    #[test]
    fn missing_type_fails_fast() {
        let cols = vec![common::OutputColumn {
            column_id: 1,
            name: "missing".to_string(),
            r#type: None,
            nullable: true,
            is_internal: false,
        }];

        let err = schema_from_output_columns(&cols).expect_err("missing type should fail");
        assert!(err.contains("OutputColumn.type missing"), "err={err}");
    }

    #[test]
    fn duplicate_column_id_fails_fast() {
        let cols = vec![
            output_column(5, "left", DataType::Int32, true),
            output_column(5, "right", DataType::Int64, true),
        ];

        let err = layout_from_output_columns(&cols).expect_err("duplicate column id should fail");
        assert!(
            err.contains("duplicate OutputColumn.column_id 5"),
            "err={err}"
        );

        let err =
            chunk_schema_from_output_columns(&cols).expect_err("duplicate column id should fail");
        assert!(
            err.contains("duplicate OutputColumn.column_id 5"),
            "err={err}"
        );
    }
}
