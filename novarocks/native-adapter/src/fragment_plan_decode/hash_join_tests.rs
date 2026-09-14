//! Native hash-join decoder integration coverage.

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::tests::{
        column_ref, lower, one_col_values_node_with, one_col_values_node_with_nullable,
        output_column_with_nullable, physical_node,
    };
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_proto_models::plan;
    use novarocks_types::SlotId;

    #[test]
    fn hash_join_output_schema_uses_plan_output_nullability() {
        let output_columns = vec![
            output_column_with_nullable(1, "lhs", DataType::Int64, false),
            output_column_with_nullable(2, "rhs", DataType::Int64, true),
        ];
        let join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::LeftOuter as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
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
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert!(!join.join_scope_chunk_schema.slots()[0].nullable());
        assert!(join.join_scope_chunk_schema.slots()[1].nullable());
    }

    #[test]
    fn hash_join_execution_mode_overrides_distribution_and_unknown_defaults_broadcast() {
        let mut join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: Some(plan::JoinExecutionMode::Partitioned as i32),
            }),
            Vec::new(),
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 10),
            ],
        );
        let lowered = lower(&join);
        let ExecNodeKind::Join(join_node) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(
            join_node.distribution_mode,
            novarocks_execution::exec::node::join::JoinDistributionMode::Partitioned
        );

        let plan::distributed_node::Payload::Physical(physical) =
            join.payload.as_mut().expect("physical")
        else {
            panic!("expected physical");
        };
        let Some(plan::plan_node::Kind::HashJoin(hash_join)) = physical.kind.as_mut() else {
            panic!("expected hash join");
        };
        hash_join.distribution = plan::JoinDistribution::Unknown as i32;
        hash_join.execution_mode = None;
        let lowered = lower(&join);
        let ExecNodeKind::Join(join_node) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(
            join_node.distribution_mode,
            novarocks_execution::exec::node::join::JoinDistributionMode::Broadcast
        );
    }
}
