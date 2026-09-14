//! Native table-function decoder integration coverage.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};

    use super::super::{DecodedNode, NativePlanDecodeContext, decode_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_execution::exec::node::table_function::TableFunctionOutputSlot;
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

    fn lower(node: &plan::DistributedNode) -> DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    #[test]
    fn lowers_native_table_function_with_outer_and_result_slots() {
        let array_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let child_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "arr", array_type.clone()),
        ];
        let child = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: Vec::new(),
                columns: child_columns.clone(),
            }),
            child_columns,
            Vec::new(),
        );
        let result_columns = vec![output_column(3, "unnest", DataType::Int64)];
        let node = physical_node(
            20,
            plan::plan_node::Kind::TableFunction(plan::TableFunctionNode {
                function_name: "unnest".to_string(),
                args: vec![column_ref(2, array_type.clone())],
                output_columns: result_columns.clone(),
                alias: Some("u".to_string()),
                is_left_join: false,
            }),
            result_columns,
            vec![child],
        );

        let lowered = lower(&node);
        assert_eq!(
            lowered.layout.order(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );

        let ExecNodeKind::TableFunction(table_function) = lowered.node.kind else {
            panic!("expected TableFunction");
        };
        assert_eq!(table_function.node_id, 20);
        assert_eq!(table_function.function_name, "unnest");
        assert_eq!(table_function.param_types, vec![array_type]);
        assert_eq!(table_function.ret_types, vec![DataType::Int64]);
        assert_eq!(
            table_function.outer_slots,
            vec![SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(table_function.fn_result_slots, vec![SlotId::new(3)]);
        assert!(table_function.fn_result_required);
        assert!(!table_function.is_left_join);
        assert_eq!(table_function.param_slots.len(), 1);
        assert_ne!(table_function.param_slots[0], SlotId::new(1));
        assert_ne!(table_function.param_slots[0], SlotId::new(2));
        assert_ne!(table_function.param_slots[0], SlotId::new(3));
        assert_eq!(
            table_function.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );
        assert_eq!(table_function.output_slot_sources.len(), 3);
        match &table_function.output_slot_sources[0] {
            TableFunctionOutputSlot::Outer { slot } => assert_eq!(*slot, SlotId::new(1)),
            other => panic!("expected first outer slot, got {other:?}"),
        }
        match &table_function.output_slot_sources[1] {
            TableFunctionOutputSlot::Outer { slot } => assert_eq!(*slot, SlotId::new(2)),
            other => panic!("expected second outer slot, got {other:?}"),
        }
        match &table_function.output_slot_sources[2] {
            TableFunctionOutputSlot::Result { index } => assert_eq!(*index, 0),
            other => panic!("expected result slot, got {other:?}"),
        }

        let ExecNodeKind::Project(project) = table_function.input.kind else {
            panic!("expected derived Project input");
        };
        assert!(project.is_subordinate);
        assert_eq!(
            project.expr_slot_ids,
            vec![
                SlotId::new(1),
                SlotId::new(2),
                table_function.param_slots[0],
            ]
        );
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[
                SlotId::new(1),
                SlotId::new(2),
                table_function.param_slots[0],
            ]
        );
        assert!(matches!(project.input.kind, ExecNodeKind::Values(_)));
    }
}
