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

use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkFieldSchema, ChunkSlotSchema};
use crate::lower::type_lowering::arrow_field_from_desc;
use crate::thrift::types;

pub(crate) fn chunk_field_schema_from_type_desc(
    name: impl Into<String>,
    nullable: bool,
    desc: types::TTypeDesc,
) -> Result<ChunkFieldSchema, String> {
    let name = name.into();
    validate_type_desc_nodes(&desc, "field")?;
    let field = arrow_field_from_desc(&name, nullable, &desc)
        .ok_or_else(|| "field type desc has unsupported arrow mapping".to_string())?;
    ChunkFieldSchema::from_field(&field)
}

pub(crate) fn chunk_slot_schema_from_type_desc(
    slot_id: SlotId,
    name: impl Into<String>,
    nullable: bool,
    desc: types::TTypeDesc,
    unique_id: Option<i32>,
) -> Result<ChunkSlotSchema, String> {
    let name = name.into();
    validate_type_desc_nodes(&desc, "slot")?;
    let field = arrow_field_from_desc(&name, nullable, &desc).ok_or_else(|| {
        format!(
            "chunk slot {} has unsupported type desc for arrow conversion",
            slot_id
        )
    })?;
    ChunkSlotSchema::try_new_with_field(slot_id, field, None, unique_id)
}

pub(crate) fn chunk_slot_schema_from_optional_type_desc(
    slot_id: SlotId,
    name: impl Into<String>,
    nullable: bool,
    desc: Option<types::TTypeDesc>,
    unique_id: Option<i32>,
) -> Result<ChunkSlotSchema, String> {
    let Some(desc) = desc else {
        return Err(format!(
            "chunk slot {} missing type_desc; use try_new_with_field for runtime fields",
            slot_id
        ));
    };
    chunk_slot_schema_from_type_desc(slot_id, name, nullable, desc, unique_id)
}

fn validate_type_desc_nodes(desc: &types::TTypeDesc, label: &str) -> Result<(), String> {
    let nodes = desc
        .types
        .as_ref()
        .ok_or_else(|| format!("{label} type desc missing nodes"))?;
    let next = type_desc_node_span(nodes, 0)?;
    if next != nodes.len() {
        return Err(format!(
            "{label} type desc has trailing nodes: consumed={} total={}",
            next,
            nodes.len()
        ));
    }
    Ok(())
}

fn type_desc_node_span(nodes: &[types::TTypeNode], start: usize) -> Result<usize, String> {
    let node = nodes
        .get(start)
        .ok_or_else(|| format!("field type desc ended unexpectedly at node {}", start))?;

    match node.type_ {
        t if t == types::TTypeNodeType::SCALAR => Ok(start + 1),
        t if t == types::TTypeNodeType::STRUCT => {
            let struct_fields = node
                .struct_fields
                .as_ref()
                .ok_or_else(|| "struct type desc missing struct_fields".to_string())?;
            let mut cursor = start + 1;
            for _ in struct_fields {
                cursor = type_desc_node_span(nodes, cursor)?;
            }
            Ok(cursor)
        }
        t if t == types::TTypeNodeType::ARRAY => type_desc_node_span(nodes, start + 1),
        t if t == types::TTypeNodeType::MAP => {
            let next = type_desc_node_span(nodes, start + 1)?;
            type_desc_node_span(nodes, next)
        }
        other => Err(format!("unsupported type desc node {:?}", other)),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{
        chunk_field_schema_from_type_desc, chunk_slot_schema_from_optional_type_desc,
        chunk_slot_schema_from_type_desc,
    };
    use crate::common::ids::SlotId;
    use crate::thrift::types::{
        TPrimitiveType, TScalarType, TStructField, TTypeDesc, TTypeNode, TTypeNodeType,
    };
    use crate::types::arrow_thrift::thrift_type_desc_from_primitive;
    use crate::types::logical::{LogicalType, logical_type_of_field};

    fn scalar_node(primitive: TPrimitiveType) -> TTypeNode {
        TTypeNode::new(
            TTypeNodeType::SCALAR,
            TScalarType::new(primitive, None, None, None, None),
            None,
            None,
        )
    }

    fn struct_field(name: &str) -> TStructField {
        TStructField::new(Some(name.to_string()), None::<String>, None, None::<String>)
    }

    #[test]
    fn chunk_schema_adapter_tags_logical_field_metadata() {
        let cases = [
            (TPrimitiveType::JSON, DataType::Utf8, LogicalType::Json),
            (TPrimitiveType::HLL, DataType::Binary, LogicalType::Hll),
            (
                TPrimitiveType::OBJECT,
                DataType::Binary,
                LogicalType::Object,
            ),
            (
                TPrimitiveType::PERCENTILE,
                DataType::Binary,
                LogicalType::Percentile,
            ),
        ];

        for (primitive, expected_type, expected_logical) in cases {
            let field_schema = chunk_field_schema_from_type_desc(
                "payload",
                true,
                thrift_type_desc_from_primitive(primitive),
            )
            .expect("field schema from type desc");
            let slot = chunk_slot_schema_from_type_desc(
                SlotId::new(9),
                "payload",
                true,
                thrift_type_desc_from_primitive(primitive),
                Some(77),
            )
            .expect("slot schema from type desc");

            assert_eq!(slot.data_type(), &expected_type);
            assert_eq!(field_schema.logical_type(), Some(expected_logical));
            assert_eq!(logical_type_of_field(slot.field()), Some(expected_logical));
            assert_eq!(slot.unique_id(), Some(77));
        }
    }

    #[test]
    fn chunk_schema_adapter_rejects_scalar_descriptor_with_trailing_node() {
        let desc = TTypeDesc::new(vec![
            scalar_node(TPrimitiveType::INT),
            scalar_node(TPrimitiveType::BIGINT),
        ]);

        let err = chunk_field_schema_from_type_desc("payload", true, desc)
            .expect_err("trailing type desc node should fail");

        assert!(
            err.contains("field type desc has trailing nodes: consumed=1 total=2"),
            "err={err}"
        );
    }

    #[test]
    fn chunk_schema_adapter_accepts_valid_nested_descriptor_spans() {
        let desc = TTypeDesc::new(vec![
            TTypeNode {
                type_: TTypeNodeType::STRUCT,
                scalar_type: None,
                struct_fields: Some(vec![struct_field("items"), struct_field("attrs")]),
                is_named: None,
            },
            TTypeNode {
                type_: TTypeNodeType::ARRAY,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            },
            scalar_node(TPrimitiveType::INT),
            TTypeNode {
                type_: TTypeNodeType::MAP,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            },
            scalar_node(TPrimitiveType::VARCHAR),
            scalar_node(TPrimitiveType::BIGINT),
        ]);

        let slot = chunk_slot_schema_from_type_desc(SlotId::new(11), "nested", true, desc, None)
            .expect("valid nested descriptor should produce a slot schema");

        let DataType::Struct(fields) = slot.data_type() else {
            panic!("expected struct data type, got {:?}", slot.data_type());
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "items");
        assert!(matches!(fields[0].data_type(), DataType::List(_)));
        assert_eq!(fields[1].name(), "attrs");
        assert!(matches!(fields[1].data_type(), DataType::Map(_, _)));
        assert_eq!(slot.field_schema().children().len(), 2);
        assert!(slot.field_schema().struct_child(0).is_some());
        assert!(slot.field_schema().struct_child(1).is_some());
    }

    #[test]
    fn chunk_schema_adapter_rejects_too_short_nested_descriptor() {
        let desc = TTypeDesc::new(vec![TTypeNode {
            type_: TTypeNodeType::ARRAY,
            scalar_type: None,
            struct_fields: None,
            is_named: None,
        }]);

        let err = chunk_field_schema_from_type_desc("items", true, desc)
            .expect_err("array descriptor without item node should fail");

        assert!(
            err.contains("field type desc ended unexpectedly at node 1"),
            "err={err}"
        );
    }

    #[test]
    fn chunk_schema_adapter_optional_descriptor_reports_missing_type_desc() {
        let err =
            chunk_slot_schema_from_optional_type_desc(SlotId::new(42), "missing", true, None, None)
                .expect_err("missing optional descriptor should fail");

        assert!(
            err.contains(
                "chunk slot 42 missing type_desc; use try_new_with_field for runtime fields"
            ),
            "err={err}"
        );
    }
}
