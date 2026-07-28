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

use super::super::relational::encoded_physical_variant_names_for_test;
use super::*;
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::planner::physical::PhysicalPlanKind;

#[test]
fn physical_plan_encoder_variant_guard_tracks_rust_enum_not_proto_arms() {
    assert_eq!(
        encoded_physical_variant_names_for_test(),
        PhysicalPlanKind::variant_names_for_test()
    );
    assert!(
        !encoded_physical_variant_names_for_test().contains(&"Decode"),
        "Decode exists only as a proto arm; Rust PhysicalPlanKind is the source of truth"
    );
}

#[test]
fn hash_aggregate_payload_maps_group_layout_and_mode() {
    let group = output_column(1, "group_key", DataType::Int64);
    let group_expr = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: crate::sql::column_id::ColumnId::new_for_test(1),
            qualifier: None,
            column: "group_key".to_string(),
        },
        data_type: DataType::Int64,
        nullable: false,
    };
    let aggregate = DistributedNode {
        node_id: 7,
        fragment_id: 0,
        tuple_ids: vec![7],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![DistributedNode {
            node_id: 8,
            fragment_id: 0,
            tuple_ids: vec![8],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(crate::sql::planner::payload::PlanValuesNode {
                rows: Vec::new(),
                columns: vec![group.clone()],
            }),
        }],
        stats: stats(),
        payload: DistributedNodeKind::HashAggregate(Box::new(
            crate::sql::planner::physical::PhysicalHashAggregateNode {
                mode: crate::sql::planner::physical::AggMode::Local,
                group_by: vec![group_expr],
                aggregates: Vec::new(),
                is_merge: Vec::new(),
                output_layout: crate::sql::planner::physical::AggregateOutputLayout::new(
                    vec![group.clone()],
                    Vec::new(),
                ),
                output_columns: vec![group.clone()],
                topn_runtime_filter_builds: Vec::new(),
            },
        )),
    };
    let distributed = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: aggregate,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: vec![group],
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    };

    let encoded =
        encode_distributed_plan(&distributed, empty_scan_bindings()).expect("encode aggregate");
    let root = encoded.fragments[0].root.as_ref().expect("aggregate root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical aggregate payload");
    };
    let Some(plan::plan_node::Kind::HashAggregate(aggregate)) = physical.kind.as_ref() else {
        panic!("expected HashAggregate payload");
    };
    assert_eq!(aggregate.mode, i32::from(plan::AggMode::Local));
    assert_eq!(aggregate.group_by.len(), 1);
    assert_eq!(
        aggregate
            .output_layout
            .as_ref()
            .expect("aggregate output layout")
            .group_key_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(
        aggregate
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
fn encoded_join_output_maps_reconciled_children_not_stale_payload() {
    // The join payload lists a stale id (999) that neither child produces. The
    // sealed node-output contract reconciles the join against its children, and
    // the encoder must map that contract instead of the stale payload columns.
    let left = output_column(1, "l_k", DataType::Int64);
    let right = output_column(2, "r_k", DataType::Int64);
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root: DistributedNode {
                node_id: 1,
                fragment_id: 0,
                tuple_ids: vec![1],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: vec![
                    DistributedNode {
                        node_id: 2,
                        fragment_id: 0,
                        tuple_ids: vec![2],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Values(
                            crate::sql::planner::payload::PlanValuesNode {
                                rows: Vec::new(),
                                columns: vec![left.clone()],
                            },
                        ),
                    },
                    DistributedNode {
                        node_id: 3,
                        fragment_id: 0,
                        tuple_ids: vec![3],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
                        runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Values(
                            crate::sql::planner::payload::PlanValuesNode {
                                rows: Vec::new(),
                                columns: vec![right.clone()],
                            },
                        ),
                    },
                ],
                stats: stats(),
                payload: DistributedNodeKind::HashJoin(Box::new(
                    crate::sql::planner::physical::PhysicalHashJoinNode {
                        join_type: JoinKind::Inner,
                        eq_conditions: Vec::new(),
                        other_condition: None,
                        distribution: JoinDistribution::Unknown,
                        execution_mode: None,
                        build_runtime_filters: Vec::new(),
                        output_columns: vec![
                            left.clone(),
                            right.clone(),
                            output_column(999, "stale", DataType::Int64),
                        ],
                    },
                )),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: vec![left.clone(), right.clone()],
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    };

    let encoded =
        encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");
    let root = encoded.fragments[0].root.as_ref().expect("root");
    let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref() else {
        panic!("expected physical join root");
    };
    assert_eq!(
        physical
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "l_k"), (2, "r_k")],
        "the encoder maps the reconciled contract, dropping the stale id 999"
    );
}
