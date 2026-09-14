//! Native nested-loop decoder integration coverage.

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::tests::{
        bool_literal, lower, one_col_values_node_with, one_col_values_node_with_nullable,
        output_column, output_column_with_nullable, physical_node,
    };
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_proto_models::plan;
    use novarocks_types::SlotId;

    #[test]
    fn nested_loop_join_output_schema_uses_plan_output_nullability() {
        let output_columns = vec![
            output_column_with_nullable(1, "lhs", DataType::Int64, false),
            output_column_with_nullable(2, "rhs", DataType::Int64, true),
        ];
        let join = physical_node(
            30,
            plan::plan_node::Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: plan::JoinKind::LeftOuter as i32,
                condition: Some(bool_literal(true)),
            }),
            output_columns,
            vec![
                one_col_values_node_with_nullable(10, 1, "lhs", 10, false),
                one_col_values_node_with_nullable(11, 2, "rhs", 10, false),
            ],
        );

        let lowered = lower(&join);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert!(!lowered.output_schema.slots()[0].nullable());
        assert!(lowered.output_schema.slots()[1].nullable());
        let ExecNodeKind::NestedLoopJoin(join) = lowered.node.kind else {
            panic!("expected NestedLoopJoin");
        };
        assert!(!join.join_scope_chunk_schema.slots()[0].nullable());
        assert!(join.join_scope_chunk_schema.slots()[1].nullable());
    }

    #[test]
    fn nested_loop_right_semi_swaps_inputs_for_left_semi_execution() {
        let right_output = vec![output_column(2, "rhs", DataType::Int64)];
        let join = physical_node(
            30,
            plan::plan_node::Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: plan::JoinKind::RightSemi as i32,
                condition: Some(bool_literal(true)),
            }),
            right_output,
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 20),
            ],
        );

        let lowered = lower(&join);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(2)]);
        let ExecNodeKind::NestedLoopJoin(join) = lowered.node.kind else {
            panic!("expected NestedLoopJoin");
        };
        assert!(matches!(
            join.join_type,
            novarocks_execution::exec::node::nljoin::NestedLoopJoinType::LeftSemi
        ));
        assert_eq!(join.left_chunk_schema.slot_ids(), &[SlotId::new(2)]);
        assert_eq!(join.right_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(
            join.join_scope_chunk_schema.slot_ids(),
            &[SlotId::new(2), SlotId::new(1)]
        );
    }
}
