//! Native project decoder integration coverage.

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::{DecodedNode, NativePlanDecodeContext, decode_node};
    use crate::fragment_error::NativeFragmentDecodeError;
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_plan_codec::encode_native_type as encode_type;
    use novarocks_proto_codec::ProtocolErrorKind;
    use novarocks_proto_models::{common, expr, plan};
    use novarocks_types::SlotId;

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn two_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn lower(node: &plan::DistributedNode) -> DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    fn lower_error(node: &plan::DistributedNode) -> NativeFragmentDecodeError {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default())
            .expect_err("invalid Project node must fail")
    }

    #[test]
    fn missing_project_item_expr_uses_exact_indexed_path_and_kind() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: None,
                    output_name: "missing".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let error = lower_error(&project);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.project.items[0].expr"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn missing_project_item_expr_type_uses_exact_indexed_path_and_kind() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(expr::Expr {
                        r#type: None,
                        ..int_literal(1)
                    }),
                    output_name: "missing_type".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let error = lower_error(&project);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.project.items[0].expr.type"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn invalid_project_item_expr_type_uses_incoming_wire_path_and_kind() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(expr::Expr {
                        r#type: Some(common::TypeDesc::default()),
                        ..int_literal(1)
                    }),
                    output_name: "invalid_type".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let error = lower_error(&project);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.root.payload.physical.project.items[0].expr.type"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
    }

    #[test]
    fn project_arity_uses_distributed_node_children_path_and_kind() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode::default()),
            Vec::new(),
            Vec::new(),
        );

        let error = lower_error(&project);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.root.children");
        assert_eq!(protocol.kind(), ProtocolErrorKind::InconsistentFields);
    }

    #[test]
    fn lowers_project_items_to_output_slots_and_schema() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(1, DataType::Int64)),
                    output_name: "projected_id".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&project);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.node_id, 20);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(1)]);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(7)]);
        assert_eq!(
            project.output_chunk_schema.field(0).unwrap().name(),
            "projected_id"
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7)]);
        assert!(matches!(project.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn wraps_project_distributed_limit_as_limit_node() {
        let mut project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(1, DataType::Int64)),
                    output_name: "projected_id".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        project.limit = 1;

        let lowered = lower(&project);
        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected Limit");
        };
        assert_eq!(limit.node_id, 20);
        assert_eq!(limit.limit, Some(1));
        assert_eq!(limit.offset, 0);
        assert!(matches!(limit.input.kind, ExecNodeKind::Project(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(7)]);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(7)]);
    }

    #[test]
    fn parent_project_can_reference_child_project_output_column_id() {
        let inner = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(1, DataType::Int64)),
                    output_name: "projected_id".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let outer = physical_node(
            21,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(7, DataType::Int64)),
                    output_name: "outer_id".to_string(),
                    output_column_id: 9,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![inner],
        );

        let lowered = lower(&outer);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(7)]);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(9)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(9)]);
    }

    #[test]
    fn lowers_project_reused_input_slots_with_output_indices_when_output_ids_change() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![
                    plan::ProjectItem {
                        expr: Some(column_ref(1, DataType::Int64)),
                        output_name: "left_out".to_string(),
                        output_column_id: 7,
                    },
                    plan::ProjectItem {
                        expr: Some(column_ref(2, DataType::Int64)),
                        output_name: "right_out".to_string(),
                        output_column_id: 8,
                    },
                ],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![two_col_values_node(10)],
        );

        let lowered = lower(&project);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(1), SlotId::new(2)]);
        assert_eq!(project.output_indices, Some(vec![0, 1]));
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[SlotId::new(7), SlotId::new(8)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7), SlotId::new(8)]);
    }

    #[test]
    fn lowers_project_duplicate_output_ids_with_output_indices() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![
                    plan::ProjectItem {
                        expr: Some(column_ref(1, DataType::Int64)),
                        output_name: "left_copy".to_string(),
                        output_column_id: 7,
                    },
                    plan::ProjectItem {
                        expr: Some(column_ref(1, DataType::Int64)),
                        output_name: "right_copy".to_string(),
                        output_column_id: 7,
                    },
                ],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&project);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.exprs.len(), 1);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(1)]);
        assert_eq!(project.output_indices, Some(vec![0, 0]));
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[SlotId::new(7), SlotId::new(8)]
        );
        assert_eq!(
            project.output_chunk_schema.field(0).unwrap().name(),
            "left_copy"
        );
        assert_eq!(
            project.output_chunk_schema.field(1).unwrap().name(),
            "right_copy"
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7), SlotId::new(8)]);
    }
}
