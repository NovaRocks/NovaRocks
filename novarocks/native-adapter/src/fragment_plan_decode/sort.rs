//! Native sort decoder integration coverage.

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::{NativePlanDecodeContext, decode_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_execution::exec::node::sort::SortTopNType;
    use novarocks_plan_codec::encode_native_type as encode_type;
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

    fn sort_item(column_id: u32) -> expr::SortItem {
        expr::SortItem {
            expr: Some(column_ref(column_id, DataType::Int64)),
            asc: true,
            nulls_first: false,
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

    fn lower(node: &plan::DistributedNode) -> super::super::DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    #[test]
    fn lowers_sort_and_topn_shapes() {
        let mut sort = physical_node(
            20,
            plan::plan_node::Kind::Sort(plan::SortNode {
                items: vec![sort_item(1)],
                analytic_partition_by: Vec::new(),
                output_columns: vec![output_column(1, "id", DataType::Int64)],
                offset: Some(2),
                partition_limit: None,
                topn_type: None,
            }),
            vec![output_column(1, "id", DataType::Int64)],
            vec![one_col_values_node(10)],
        );
        sort.limit = 9;
        let lowered_sort = lower(&sort);
        let ExecNodeKind::Sort(sort) = lowered_sort.node.kind else {
            panic!("expected Sort");
        };
        assert!(!sort.use_top_n);
        assert_eq!(sort.limit, Some(9));
        assert_eq!(sort.offset, 2);
        assert_eq!(sort.order_by.len(), 1);

        let topn = physical_node(
            30,
            plan::plan_node::Kind::Topn(plan::TopNNode {
                items: vec![sort_item(1)],
                limit: Some(3),
                offset: Some(0),
                phase: plan::TopNPhase::TopnPhaseFinal as i32,
                is_split: false,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered_topn = lower(&topn);
        let ExecNodeKind::Sort(topn) = lowered_topn.node.kind else {
            panic!("expected TopN as Sort");
        };
        assert!(topn.use_top_n);
        assert_eq!(topn.limit, Some(3));
        assert_eq!(topn.offset, 0);
        assert_eq!(topn.topn_type, SortTopNType::RowNumber);
    }

    #[test]
    fn lowers_sort_output_reorder_as_subordinate_project() {
        let sort = physical_node(
            20,
            plan::plan_node::Kind::Sort(plan::SortNode {
                items: vec![sort_item(1)],
                analytic_partition_by: Vec::new(),
                output_columns: vec![
                    output_column(2, "b", DataType::Int64),
                    output_column(1, "a", DataType::Int64),
                ],
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            vec![
                output_column(2, "b", DataType::Int64),
                output_column(1, "a", DataType::Int64),
            ],
            vec![two_col_values_node(10)],
        );

        let lowered = lower(&sort);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected reorder project");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.node_id, 20);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(2), SlotId::new(1)]);
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[SlotId::new(2), SlotId::new(1)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(2), SlotId::new(1)]);
        let ExecNodeKind::Sort(sort) = project.input.kind else {
            panic!("expected Sort below reorder project");
        };
        assert_eq!(sort.order_by.len(), 1);
        assert!(matches!(sort.input.kind, ExecNodeKind::Values(_)));
    }
}
