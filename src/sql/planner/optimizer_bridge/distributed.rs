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

use crate::sql::optimizer::OptimizerPhysicalNode;
use crate::sql::planner::DistributedPlan;

use super::id_binding::verify_optimizer_id_binding;
use super::physical::optimizer_physical_to_plan;

pub(crate) fn optimizer_physical_to_distributed_plan(
    plan: &OptimizerPhysicalNode,
) -> Result<DistributedPlan, String> {
    verify_optimizer_id_binding(plan)?;
    let mut physical = optimizer_physical_to_plan(plan)?;
    crate::sql::planner::runtime_filter_placement::place_runtime_filters(&mut physical);
    crate::sql::planner::build_distributed_plan(&physical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, ProjectItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::{JoinKind, OutputColumn};
    use crate::sql::optimizer::operator::{
        JoinDistribution, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp, ProjectOp,
        ValuesOp,
    };
    use crate::sql::optimizer::physical_tree::{
        JoinExecutionDistribution, OptimizerPhysicalNode, PlanExecutionProps, attach_scalar_arena,
    };
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::DistributedNode;
    use crate::sql::planner::optimizer_bridge::scalar::{intern_project_items, intern_typed};
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    fn int_col(column_id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref(column_id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn values_node(columns: Vec<OutputColumn>) -> OptimizerPhysicalNode {
        OptimizerPhysicalNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }),
            children: vec![],
            output_columns: columns,
            stats: Statistics::default(),
            explain_stats: Default::default(),
            execution_props: PlanExecutionProps::default(),
        }
    }

    fn has_build_rf(node: &DistributedNode) -> bool {
        !node.build_runtime_filters.is_empty() || node.children.iter().any(has_build_rf)
    }

    fn has_probe_rf(node: &DistributedNode) -> bool {
        !node.probe_runtime_filters.is_empty() || node.children.iter().any(has_probe_rf)
    }

    fn broadcast_hash_join_without_optimizer_rf_annotations() -> OptimizerPhysicalNode {
        let probe_id = ColumnId::new_for_test(1);
        let build_id = ColumnId::new_for_test(2);
        let probe_col = int_col(probe_id, "probe_key");
        let build_col = int_col(build_id, "build_key");
        let mut scalars = ScalarArena::new();
        let left = intern_typed(&mut scalars, &column_ref(probe_id, "probe_key"));
        let right = intern_typed(&mut scalars, &column_ref(build_id, "build_key"));
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left,
                    right,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![
                values_node(vec![probe_col.clone()]),
                values_node(vec![build_col.clone()]),
            ],
            output_columns: vec![probe_col, build_col],
            stats: Statistics::default(),
            explain_stats: Default::default(),
            execution_props: PlanExecutionProps {
                join_distribution: Some(JoinExecutionDistribution::Broadcast),
                ..PlanExecutionProps::default()
            },
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    #[test]
    fn bridge_builds_distributed_plan_from_optimizer_values() {
        let mut plan = values_node(vec![int_col(ColumnId::new_for_test(1), "v")]);
        attach_scalar_arena(&mut plan, Arc::new(ScalarArena::new()));

        let dp = optimizer_physical_to_distributed_plan(&plan).expect("build DistributedPlan");
        assert_eq!(dp.fragments.len(), 1);
        assert_eq!(dp.root_fragment_id, 0);
    }

    #[test]
    fn bridge_rejects_unbound_project_column_before_distributed_build() {
        let input_id = ColumnId::new_for_test(1);
        let missing_id = ColumnId::new_for_test(99);
        let output_id = ColumnId::new_for_test(2);
        let mut scalars = ScalarArena::new();
        let items = intern_project_items(
            &mut scalars,
            &[ProjectItem {
                expr: column_ref(missing_id, "missing"),
                output_name: "p".to_string(),
                output_column_id: output_id,
            }],
        );
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalProject(ProjectOp {
                items,
                output_qualifier: None,
            }),
            children: vec![values_node(vec![int_col(input_id, "v")])],
            output_columns: vec![int_col(output_id, "p")],
            stats: Statistics::default(),
            explain_stats: Default::default(),
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        let err = optimizer_physical_to_distributed_plan(&plan)
            .expect_err("unbound project ColumnId must fail");
        assert!(
            err.contains("not produced by child scope"),
            "unexpected err={err}"
        );
    }

    #[test]
    fn bridge_runs_planner_runtime_filter_placement_before_distributed_build() {
        let plan = broadcast_hash_join_without_optimizer_rf_annotations();

        let dp = optimizer_physical_to_distributed_plan(&plan).expect("build DistributedPlan");
        let root = &dp.fragments[dp.root_fragment_id as usize].root;

        assert!(
            has_build_rf(root),
            "distributed plan should contain build RF"
        );
        assert!(
            has_probe_rf(root),
            "distributed plan should contain probe RF"
        );
    }
}
