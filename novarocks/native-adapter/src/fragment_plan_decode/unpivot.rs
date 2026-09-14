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

//! Native Unpivot decoder integration coverage.

mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::super::tests::{
        column_ref, one_col_values_node, output_column_with_nullable, physical_node, string_literal,
    };
    use super::super::{NativePlanDecodeContext, decode_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_proto_codec::{FieldPath, arrow_physical};
    use novarocks_proto_models::plan;

    fn valid_unpivot(literal: novarocks_proto_models::expr::Expr) -> plan::DistributedNode {
        let mut node = physical_node(
            20,
            plan::plan_node::Kind::Unpivot(plan::UnpivotNode {
                passthrough_columns: Vec::new(),
                value_output_column_id: 11,
                literal_output_column_ids: vec![10],
                value_mappings: vec![plan::UnpivotValueMapping {
                    input_value_column_id: 1,
                    constants: vec![plan::UnpivotConstant {
                        value: Some(plan::unpivot_constant::Value::ScalarLiteral(literal)),
                    }],
                }],
                max_output_rows: 1024,
                max_output_bytes: 1024 * 1024,
                output_schema: None,
            }),
            vec![
                output_column_with_nullable(10, "label", DataType::Utf8, false),
                output_column_with_nullable(11, "value", DataType::Int64, true),
            ],
            vec![one_col_values_node(10)],
        );
        install_exact_output_schema(
            &mut node,
            vec![
                Field::new("label", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ],
            &[10, 11],
        );
        node
    }

    fn install_exact_output_schema(
        node: &mut plan::DistributedNode,
        fields: Vec<Field>,
        slot_ids: &[u32],
    ) {
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
        else {
            panic!("physical node");
        };
        physical.output_columns.clear();
        let Some(plan::plan_node::Kind::Unpivot(unpivot)) = physical.kind.as_mut() else {
            panic!("Unpivot node");
        };
        let schema = Schema::new(fields);
        let (columns, schema_metadata) = arrow_physical::encode_schema(
            &schema,
            slot_ids,
            false,
            FieldPath::root("unpivot.output_schema"),
        )
        .expect("exact output schema");
        unpivot.output_schema = Some(plan::ArrowPhysicalSchema {
            columns,
            schema_metadata,
        });
    }

    #[test]
    fn lowers_typed_unpivot_contract() {
        let mut arena = ExprArena::default();
        let decoded = decode_node(
            &valid_unpivot(string_literal("first")),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let ExecNodeKind::Unpivot(unpivot) = decoded.node.kind else {
            panic!("expected Unpivot");
        };
        assert_eq!(unpivot.value_mappings.len(), 1);
        assert_eq!(unpivot.literal_output_slot_ids.len(), 1);
        assert_eq!(unpivot.max_output_rows, 1024);
    }

    #[test]
    fn rejects_non_literal_mapping_tag() {
        let mut arena = ExprArena::default();
        let error = decode_node(
            &valid_unpivot(column_ref(1, DataType::Utf8)),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap_err();
        assert!(error.contains("not found in input layout") || error.contains("not a literal"));
    }

    #[test]
    fn rejects_value_nullability_drift() {
        let mut node = valid_unpivot(string_literal("first"));
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
        else {
            panic!("physical node");
        };
        let Some(plan::plan_node::Kind::Unpivot(unpivot)) = physical.kind.as_mut() else {
            panic!("Unpivot node");
        };
        unpivot
            .output_schema
            .as_mut()
            .expect("output schema")
            .columns[1]
            .field
            .as_mut()
            .expect("field")
            .nullable = false;
        let mut arena = ExprArena::default();
        let error =
            decode_node(&node, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
        assert!(
            error.contains("value output nullability mismatch"),
            "{error}"
        );
    }

    #[test]
    fn preserves_non_nullable_list_element_shape() {
        let mut node = physical_node(
            20,
            plan::plan_node::Kind::Unpivot(plan::UnpivotNode {
                passthrough_columns: Vec::new(),
                value_output_column_id: 11,
                literal_output_column_ids: vec![10],
                value_mappings: vec![plan::UnpivotValueMapping {
                    input_value_column_id: 1,
                    constants: vec![plan::UnpivotConstant {
                        value: Some(plan::unpivot_constant::Value::Int32List(plan::Int32List {
                            values: vec![1, 2],
                        })),
                    }],
                }],
                max_output_rows: 1024,
                max_output_bytes: 1024 * 1024,
                output_schema: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let list_type = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Int32,
            false,
        )));
        install_exact_output_schema(
            &mut node,
            vec![
                Field::new("field_ids", list_type.clone(), false),
                Field::new("value", DataType::Int64, true),
            ],
            &[10, 11],
        );

        let mut arena = ExprArena::default();
        let decoded = decode_node(&node, &mut arena, &NativePlanDecodeContext::default()).unwrap();
        let ExecNodeKind::Unpivot(unpivot) = decoded.node.kind else {
            panic!("expected Unpivot");
        };
        assert_eq!(
            unpivot
                .output_chunk_schema
                .slot(novarocks_types::SlotId::new(10))
                .expect("list output")
                .data_type(),
            &list_type
        );
    }
}
