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

use super::*;
use crate::runtime_filter::model::graph::RuntimeFilterBindingRole;
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::{ChangeStreamBranchKind, JoinKind, OutputColumn};
use crate::sql::optimizer::operator::{
    JoinDistribution, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp, ValuesOp,
};
use crate::sql::optimizer::optimized_tree::{
    JoinExecutionDistribution, OptimizedOperatorNode, PlanExecutionProps, attach_scalar_arena,
};
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::statistics::Statistics;
use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
use crate::sql::planner::payload::{PlanSortNode, PlanValuesNode};
use crate::sql::planner::physical::{
    DistributedChangeEventExpandNode, DistributedChangeEventOutputExpr, DistributedChangeEventSpec,
    PhysicalPlanKind, PhysicalPlanStats, PlannerConfidence, PreExpandKeyedAssertSpec,
};
use arrow::datatypes::DataType;
use std::collections::HashMap;
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

fn values_node(columns: Vec<OutputColumn>) -> OptimizedOperatorNode {
    OptimizedOperatorNode {
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

fn physical_stats() -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: 0.0,
        row_count_confidence: PlannerConfidence::Exact,
        column_statistics: HashMap::new(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn physical_values_node(columns: Vec<OutputColumn>) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: columns.clone(),
        }),
        children: vec![],
        output_columns: columns,
        stats: physical_stats(),
        probe_runtime_filters: vec![],
    }
}

fn keyed_assert_spec() -> PreExpandKeyedAssertSpec {
    PreExpandKeyedAssertSpec {
        key_column_name: "__nr_row_id".to_string(),
        key_label: "_row_id".to_string(),
        message_prefix: "MOR UPDATE matched target row".to_string(),
    }
}

fn change_event_expand_node(
    child: PhysicalPlanNode,
    output_columns: Vec<OutputColumn>,
    events: Vec<DistributedChangeEventSpec>,
) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::ChangeEventExpand(DistributedChangeEventExpandNode {
            events,
            output_columns: output_columns.clone(),
            change_op_column_id: ColumnId::new_for_test(200),
            data_route_column_id: None,
        }),
        children: vec![child],
        output_columns,
        stats: physical_stats(),
        probe_runtime_filters: vec![],
    }
}

fn physical_sort_node(
    child: PhysicalPlanNode,
    physical_output_columns: Vec<OutputColumn>,
    sort_output_columns: Vec<OutputColumn>,
) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Sort(PlanSortNode {
            items: vec![],
            analytic_partition_by: vec![],
            output_columns: sort_output_columns,
            offset: None,
            partition_limit: None,
            topn_type: None,
        }),
        children: vec![child],
        output_columns: physical_output_columns,
        stats: physical_stats(),
        probe_runtime_filters: vec![],
    }
}

fn plan_snapshot(plan: &PhysicalPlanNode) -> String {
    format!("{plan:#?}")
}

fn broadcast_hash_join_without_optimizer_rf_annotations() -> OptimizedOperatorNode {
    let probe_id = ColumnId::new_for_test(1);
    let build_id = ColumnId::new_for_test(2);
    let probe_col = int_col(probe_id, "probe_key");
    let build_col = int_col(build_id, "build_key");
    let mut scalars = ScalarArena::new();
    let left = intern_typed(&mut scalars, &column_ref(probe_id, "probe_key"));
    let right = intern_typed(&mut scalars, &column_ref(build_id, "build_key"));
    let mut plan = OptimizedOperatorNode {
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
fn pipeline_builds_distributed_plan_from_physical_values() {
    let physical = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![],
        }),
        children: vec![],
        output_columns: vec![],
        stats: PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Exact,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        probe_runtime_filters: vec![],
    };

    let distributed = build_distributed_plan(physical).expect("build DistributedPlan");
    assert_eq!(distributed.fragments().len(), 1);
    assert_eq!(distributed.root_fragment_id(), 0);
}

fn assert_sealed_plan(_: &crate::sql::planner::distributed::DistributedPlan) {}

#[test]
fn statistics_entrypoint_seals_a_typed_internal_root_sink() {
    let metrics = novarocks_spi::connector::StatisticsMetricRequest::try_new(vec![
        novarocks_spi::connector::StatisticsMetric::RowCount,
    ])
    .expect("statistics metrics");
    let distributed = build_statistics_distributed_plan_with_settings(
        physical_values_node(vec![int_col(ColumnId::new_for_test(17), "value")]),
        metrics.clone(),
        &crate::sql::optimizer::options::SessionOptimizerSettings::default(),
    )
    .expect("statistics entrypoint seals");

    assert_sealed_plan(&distributed);
    assert!(matches!(
        &distributed.fragments()[0].sink,
        crate::sql::planner::distributed::DataSink::Statistics(actual)
            if actual.metrics() == metrics.metrics()
    ));
    assert_eq!(distributed.topology().result_fragment_id(), None);
}

#[test]
fn plain_write_and_change_stream_entrypoints_return_sealed_plans() {
    let id = OutputColumn {
        column_id: ColumnId::new_for_test(1),
        name: "id".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_internal: false,
    };

    let plain = build_distributed_plan(physical_values_node(vec![id.clone()]))
        .expect("plain entrypoint seals");
    assert_sealed_plan(&plain);
    assert!(matches!(
        plain.fragments()[0].sink,
        crate::sql::planner::distributed::DataSink::Result
    ));

    let write = build_iceberg_write_distributed_plan(
        physical_values_node(vec![id.clone()]),
        crate::sql::planner::distributed::write::sink::IcebergWritePlanInput {
            descriptor_database: "test_db".to_string(),
            spec: crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec(),
            input: crate::sql::planner::distributed::write::sink::ConnectorWriteInputBinding::RootOutputByOrdinal,
        },
    )
    .expect("write entrypoint seals after sink decoration");
    assert_sealed_plan(&write);
    assert!(matches!(
        write.fragments()[0].sink,
        crate::sql::planner::distributed::DataSink::ConnectorWrite(_)
    ));

    let change = build_iceberg_change_stream_distributed_plan(
        physical_values_node(vec![id]),
        "test_db",
        crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec::for_test(
            Some(0),
            None,
            vec![crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![0])],
        ),
        None,
    )
    .expect("change-stream entrypoint seals after router decoration");
    assert_sealed_plan(&change.distributed_plan);
    assert_eq!(change.distributed_plan.fragments().len(), 2);
    let root = change
        .distributed_plan
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == change.distributed_plan.root_fragment_id())
        .expect("change-stream root");
    assert!(matches!(
        root.sink,
        crate::sql::planner::distributed::DataSink::ChangeStreamRouter(_)
    ));
}

#[test]
fn pipeline_places_runtime_filters_before_distributed_build() {
    let optimizer = broadcast_hash_join_without_optimizer_rf_annotations();
    let physical = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimizer)
        .expect("convert optimizer physical plan");
    let distributed = build_distributed_plan(physical).expect("build DistributedPlan");
    let graph = distributed.runtime_filter_graph();

    assert!(
        graph
            .bindings()
            .any(|binding| matches!(binding.role, RuntimeFilterBindingRole::Producer(_)))
    );
    assert!(
        graph
            .bindings()
            .any(|binding| matches!(binding.role, RuntimeFilterBindingRole::Consumer(_)))
    );
}

#[test]
fn keyed_change_stream_assert_is_planned_before_expand_and_distributed_normally() {
    let key = int_col(ColumnId::new_for_test(1), "__nr_row_id");
    let payload = int_col(ColumnId::new_for_test(2), "payload");
    let child = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![key.clone(), payload.clone()],
        }),
        children: vec![],
        output_columns: vec![key.clone(), payload.clone()],
        stats: physical_stats(),
        probe_runtime_filters: vec![],
    };
    let expanded_columns = vec![
        key.clone(),
        payload,
        int_col(ColumnId::new_for_test(3), "__change_op"),
    ];
    let physical = PhysicalPlanNode {
        kind: PhysicalPlanKind::ChangeEventExpand(DistributedChangeEventExpandNode {
            events: vec![DistributedChangeEventSpec {
                predicate: None,
                branch_kind: ChangeStreamBranchKind::ReuseData,
                assignments: vec![],
            }],
            output_columns: expanded_columns.clone(),
            change_op_column_id: ColumnId::new_for_test(3),
            data_route_column_id: None,
        }),
        children: vec![child],
        output_columns: expanded_columns.clone(),
        stats: physical_stats(),
        probe_runtime_filters: vec![],
    };

    let planned = build_iceberg_change_stream_distributed_plan(
        physical,
        "test_db",
        crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec::for_test(
            Some(2),
            None,
            vec![crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![0])],
        ),
        Some(PreExpandKeyedAssertSpec {
            key_column_name: "__nr_row_id".to_string(),
            key_label: "_row_id".to_string(),
            message_prefix: "MOR UPDATE matched target row".to_string(),
        }),
    )
    .expect("plan keyed assertion through the real change-stream entrypoint");

    let distributed = &planned.distributed_plan;
    let root = &distributed.fragments()[distributed.root_fragment_id() as usize].root;
    assert!(matches!(
        root.payload,
        crate::sql::planner::distributed::DistributedNodeKind::ChangeEventExpand(_)
    ));
    let assertion = &root.children[0];
    let crate::sql::planner::distributed::DistributedNodeKind::AssertOneRow(assertion_payload) =
        &assertion.payload
    else {
        panic!("PhysicalAssertOneRow must be the direct ChangeEventExpand child");
    };
    assert_eq!(assertion_payload.group_key_column_ids, vec![key.column_id]);
    assert_eq!(assertion_payload.group_key_labels, vec!["_row_id"]);
    assert_eq!(assertion.children[0].node_id, 1);
    assert_eq!(assertion.node_id, 2);
    assert_eq!(root.node_id, 3);
    assert_eq!(assertion.tuple_ids, assertion.children[0].tuple_ids);
    assert_eq!(root.children[0].children[0].tuple_ids, vec![1]);
    let crate::sql::planner::distributed::DistributedNodeKind::ChangeEventExpand(expand) =
        &root.payload
    else {
        unreachable!();
    };
    assert_eq!(expand.output_columns.len(), expanded_columns.len());
    assert_eq!(
        expand
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        expanded_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>()
    );
    assert_eq!(planned.topology.writer_branches.len(), 1);
    assert_eq!(distributed.fragments().len(), 2);
}

#[test]
fn pre_expand_keyed_assert_rejects_missing_expand_without_mutation() {
    let mut physical = physical_values_node(vec![int_col(ColumnId::new_for_test(1), "k")]);
    let before = plan_snapshot(&physical);

    let err = insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect_err("missing ChangeEventExpand must fail");

    assert!(err.contains("found 0"), "{err}");
    assert_eq!(plan_snapshot(&physical), before);
}

#[test]
fn pre_expand_keyed_assert_rejects_multiple_expands_atomically() {
    let key = int_col(ColumnId::new_for_test(1), "__nr_row_id");
    let inner = change_event_expand_node(
        physical_values_node(vec![key.clone()]),
        vec![key.clone()],
        vec![],
    );
    let mut physical = change_event_expand_node(inner, vec![key], vec![]);
    let before = plan_snapshot(&physical);

    let err = insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect_err("multiple ChangeEventExpand nodes must fail");

    assert!(err.contains("found 2"), "{err}");
    assert_eq!(plan_snapshot(&physical), before);
}

#[test]
fn pre_expand_keyed_assert_rejects_expand_arity_without_mutation() {
    let key = int_col(ColumnId::new_for_test(1), "__nr_row_id");
    let mut physical =
        change_event_expand_node(physical_values_node(vec![key.clone()]), vec![key], vec![]);
    physical.children.clear();
    let before = plan_snapshot(&physical);

    let err = insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect_err("ChangeEventExpand without one child must fail");

    assert!(err.contains("expected one child, got 0"), "{err}");
    assert_eq!(plan_snapshot(&physical), before);
}

#[test]
fn pre_expand_keyed_assert_rejects_missing_and_ambiguous_key_without_mutation() {
    for columns in [
        vec![int_col(ColumnId::new_for_test(2), "payload")],
        vec![
            int_col(ColumnId::new_for_test(1), "__nr_row_id"),
            int_col(ColumnId::new_for_test(2), "__NR_ROW_ID"),
        ],
    ] {
        let mut physical = change_event_expand_node(physical_values_node(columns), vec![], vec![]);
        let before = plan_snapshot(&physical);

        let err = insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
            .expect_err("missing or ambiguous key must fail");

        assert!(
            err.contains("not found") || err.contains("ambiguous"),
            "{err}"
        );
        assert_eq!(plan_snapshot(&physical), before);
    }
}

#[test]
fn pre_expand_keyed_assert_derives_row_id_from_change_event_assignment() {
    let source_row_id = int_col(ColumnId::new_for_test(99), "source_row_id");
    let output_row_id = int_col(ColumnId::new_for_test(10), "_row_id");
    let assignment = DistributedChangeEventOutputExpr {
        output_column_id: output_row_id.column_id,
        expr: Some(column_ref(source_row_id.column_id, "source_row_id")),
    };
    let mut physical = change_event_expand_node(
        physical_values_node(vec![source_row_id.clone()]),
        vec![output_row_id],
        vec![DistributedChangeEventSpec {
            predicate: None,
            branch_kind: ChangeStreamBranchKind::ReuseData,
            assignments: vec![assignment],
        }],
    );

    insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect("derive keyed assertion from row-id assignment");

    let PhysicalPlanKind::AssertOneRow(assertion) = &physical.children[0].kind else {
        panic!("keyed assertion must be inserted before ChangeEventExpand");
    };
    assert_eq!(
        assertion.group_key_column_ids,
        vec![source_row_id.column_id]
    );
    assert_eq!(physical.children[0].output_columns.len(), 1);
    assert_eq!(
        physical.children[0].output_columns[0].column_id,
        source_row_id.column_id
    );
}

#[test]
fn pre_expand_keyed_assert_rejects_assignment_outside_direct_child_scope_atomically() {
    let output_row_id = int_col(ColumnId::new_for_test(10), "_row_id");
    let mut physical = change_event_expand_node(
        physical_values_node(vec![int_col(ColumnId::new_for_test(11), "payload")]),
        vec![output_row_id.clone()],
        vec![DistributedChangeEventSpec {
            predicate: None,
            branch_kind: ChangeStreamBranchKind::ReuseData,
            assignments: vec![DistributedChangeEventOutputExpr {
                output_column_id: output_row_id.column_id,
                expr: Some(column_ref(ColumnId::new_for_test(99), "source_row_id")),
            }],
        }],
    );
    let before = plan_snapshot(&physical);

    let err = insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect_err("assignment outside direct child scope must fail");

    assert!(err.contains("not in direct child output scope"), "{err}");
    assert_eq!(plan_snapshot(&physical), before);
}

#[test]
fn pre_expand_keyed_assert_rejects_ambiguous_assignment_in_direct_child_scope() {
    let source_left = int_col(ColumnId::new_for_test(99), "left_row_id");
    let source_right = int_col(ColumnId::new_for_test(99), "right_row_id");
    let output_row_id = int_col(ColumnId::new_for_test(10), "_row_id");
    let mut physical = change_event_expand_node(
        physical_values_node(vec![source_left, source_right]),
        vec![output_row_id.clone()],
        vec![DistributedChangeEventSpec {
            predicate: None,
            branch_kind: ChangeStreamBranchKind::ReuseData,
            assignments: vec![DistributedChangeEventOutputExpr {
                output_column_id: output_row_id.column_id,
                expr: Some(column_ref(ColumnId::new_for_test(99), "source_row_id")),
            }],
        }],
    );

    let err = insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect_err("ambiguous direct child ColumnId must fail");

    assert!(
        err.contains("ambiguous in direct child output scope"),
        "{err}"
    );
}

#[test]
fn pre_expand_keyed_assert_rejects_assignment_pruned_by_sort_output_scope_atomically() {
    let source_row_id = int_col(ColumnId::new_for_test(99), "source_row_id");
    let payload = int_col(ColumnId::new_for_test(11), "payload");
    let sorted_only = int_col(ColumnId::new_for_test(12), "sorted_only");
    let output_row_id = int_col(ColumnId::new_for_test(10), "_row_id");
    let child = physical_sort_node(
        physical_values_node(vec![source_row_id.clone(), payload]),
        vec![source_row_id.clone(), sorted_only.clone()],
        vec![sorted_only],
    );
    let mut physical = change_event_expand_node(
        child,
        vec![output_row_id.clone()],
        vec![DistributedChangeEventSpec {
            predicate: None,
            branch_kind: ChangeStreamBranchKind::ReuseData,
            assignments: vec![DistributedChangeEventOutputExpr {
                output_column_id: output_row_id.column_id,
                expr: Some(column_ref(source_row_id.column_id, "source_row_id")),
            }],
        }],
    );
    let before = plan_snapshot(&physical);

    let err = insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect_err("Sort output scope must hide pruned child columns");

    assert!(err.contains("not in direct child output scope"), "{err}");
    assert_eq!(plan_snapshot(&physical), before);
}

#[test]
fn pre_expand_keyed_assert_rejects_direct_name_pruned_by_sort_output_scope_atomically() {
    let merge_key = int_col(ColumnId::new_for_test(99), "__nr_merge_assert_key");
    let visible = int_col(ColumnId::new_for_test(12), "visible");
    let child = physical_sort_node(
        physical_values_node(vec![merge_key.clone(), visible.clone()]),
        vec![merge_key, visible.clone()],
        vec![visible],
    );
    let mut physical = change_event_expand_node(child, vec![], vec![]);
    let before = plan_snapshot(&physical);
    let keyed_assert = PreExpandKeyedAssertSpec {
        key_column_name: "__nr_merge_assert_key".to_string(),
        key_label: "merge matched target key".to_string(),
        message_prefix: "MERGE matched target row".to_string(),
    };

    let err = insert_pre_expand_keyed_assert(&mut physical, &keyed_assert)
        .expect_err("Sort output scope must hide a pruned direct-name key");

    assert!(err.contains("not found in native child"), "{err}");
    assert_eq!(plan_snapshot(&physical), before);
}

#[test]
fn pre_expand_keyed_assert_accepts_assignment_in_reordered_sort_output_scope() {
    let source_row_id = int_col(ColumnId::new_for_test(99), "source_row_id");
    let payload = int_col(ColumnId::new_for_test(12), "payload");
    let output_row_id = int_col(ColumnId::new_for_test(10), "_row_id");
    let child = physical_sort_node(
        physical_values_node(vec![source_row_id.clone(), payload.clone()]),
        vec![source_row_id.clone(), payload.clone()],
        vec![payload, source_row_id.clone()],
    );
    let mut physical = change_event_expand_node(
        child,
        vec![output_row_id.clone()],
        vec![DistributedChangeEventSpec {
            predicate: None,
            branch_kind: ChangeStreamBranchKind::ReuseData,
            assignments: vec![DistributedChangeEventOutputExpr {
                output_column_id: output_row_id.column_id,
                expr: Some(column_ref(source_row_id.column_id, "source_row_id")),
            }],
        }],
    );

    insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect("reordered Sort output retains the assignment column");
}

#[test]
fn pre_expand_keyed_assert_accepts_empty_sort_output_as_passthrough_scope() {
    let source_row_id = int_col(ColumnId::new_for_test(99), "source_row_id");
    let output_row_id = int_col(ColumnId::new_for_test(10), "_row_id");
    let child = physical_sort_node(
        physical_values_node(vec![source_row_id.clone()]),
        vec![source_row_id.clone()],
        vec![],
    );
    let mut physical = change_event_expand_node(
        child,
        vec![output_row_id.clone()],
        vec![DistributedChangeEventSpec {
            predicate: None,
            branch_kind: ChangeStreamBranchKind::ReuseData,
            assignments: vec![DistributedChangeEventOutputExpr {
                output_column_id: output_row_id.column_id,
                expr: Some(column_ref(source_row_id.column_id, "source_row_id")),
            }],
        }],
    );

    insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect("empty Sort output is a passthrough scope");
}

#[test]
fn pre_expand_keyed_assert_inserts_below_unchanged_non_root_parent() {
    let key = int_col(ColumnId::new_for_test(1), "__nr_row_id");
    let expand = change_event_expand_node(
        physical_values_node(vec![key.clone()]),
        vec![key.clone()],
        vec![],
    );
    let mut physical = physical_sort_node(expand, vec![key], vec![]);

    insert_pre_expand_keyed_assert(&mut physical, &keyed_assert_spec())
        .expect("insert keyed assertion into nested ChangeEventExpand");

    assert!(matches!(physical.kind, PhysicalPlanKind::Sort(_)));
    assert_eq!(physical.children.len(), 1);
    assert!(matches!(
        physical.children[0].kind,
        PhysicalPlanKind::ChangeEventExpand(_)
    ));
    assert!(matches!(
        physical.children[0].children[0].kind,
        PhysicalPlanKind::AssertOneRow(_)
    ));
    assert!(matches!(
        physical.children[0].children[0].children[0].kind,
        PhysicalPlanKind::Values(_)
    ));
}
