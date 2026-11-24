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
use crate::planner::payload::{PlanCTEAnchorNode, PlanCTEConsumeNode, PlanCTEProduceNode};

fn cte_produce(
    cte_id: CteId,
    output_columns: Vec<OutputColumn>,
    child: PhysicalPlanNode,
) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEProduce(PlanCTEProduceNode {
            cte_id,
            output_columns: output_columns.clone(),
        }),
        children: vec![child],
        output_columns,
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    }
}

fn cte_consume(
    cte_id: CteId,
    alias: &str,
    output_columns: Vec<OutputColumn>,
    producer_column_ids: Vec<ColumnId>,
) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEConsume(PlanCTEConsumeNode {
            cte_id,
            alias: alias.to_string(),
            output_columns: output_columns.clone(),
            producer_column_ids,
        }),
        children: Vec::new(),
        output_columns,
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    }
}

fn cte_anchor(
    cte_id: CteId,
    produce: PhysicalPlanNode,
    body: PhysicalPlanNode,
) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id }),
        children: vec![produce, body.clone()],
        output_columns: body.output_columns,
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    }
}

fn hash_distribution(child: PhysicalPlanNode) -> PhysicalPlanNode {
    let key = child.output_columns[0].column_id;
    redistribute(
        child,
        RedistributeMode::Hash {
            cols: vec![key],
            source: crate::planner::physical::HashSource::ShuffleJoin,
        },
    )
}

#[test]
fn an_adjacent_broadcast_preserves_single_copy_at_the_cte_producer() {
    let cte_id = 11;
    let source = column(1, "source", DataType::Int64, false);
    let producer = cte_produce(
        cte_id,
        vec![source.clone()],
        values(vec![source.clone()], vec![vec![literal_int(3)]]),
    );
    let probe = column(10, "probe", DataType::Int64, false);
    let build = column(20, "build", DataType::Int64, false);
    let join = hash_join(
        crate::common::JoinKind::Inner,
        PhysicalHashJoinBuildSide::Right,
        SqlJoinDistribution::Broadcast,
        Some(JoinExecutionMode::Broadcast),
        values(vec![probe.clone()], vec![vec![literal_int(3)]]),
        redistribute(
            cte_consume(cte_id, "build", vec![build.clone()], vec![source.column_id]),
            RedistributeMode::Broadcast,
        ),
        vec![probe, build],
    );
    let plan = finish_for_test(&cte_anchor(cte_id, producer, join)).unwrap();
    assert_eq!(plan.fragments().len(), 2);
    assert_eq!(plan.edges().len(), 1);
    let edge = plan.edges().values().next().unwrap();
    assert_eq!(edge.kind, EdgeKind::CteMulticast);
    assert_eq!(edge.partitioning.source, Distribution::Broadcast);
    assert_eq!(edge.partitioning.destination, Distribution::Broadcast);
    assert_eq!(
        edge.partitioning.source_multiplicity,
        RowMultiplicity::SingleCopy
    );
    assert_eq!(
        edge.partitioning.destination_multiplicity,
        RowMultiplicity::Replicated
    );
    let fragment = &plan.fragments()[&edge.destination.fragment];
    let join = &fragment.nodes()[&fragment.root()];
    assert_eq!(join.inputs[1], edge.destination.node);
}

#[test]
fn fused_join_exchange_uses_the_other_hash_input_partition_space() {
    let cte_id = 12;
    let source = column(1, "source", DataType::Int64, false);
    let producer = cte_produce(
        cte_id,
        vec![source.clone()],
        values(vec![source.clone()], vec![vec![literal_int(3)]]),
    );
    let probe = column(10, "probe", DataType::Int64, false);
    let build = column(20, "build", DataType::Int64, false);
    let join = hash_join(
        crate::common::JoinKind::Inner,
        PhysicalHashJoinBuildSide::Right,
        SqlJoinDistribution::Shuffle,
        Some(JoinExecutionMode::Partitioned),
        hash_distribution(cte_consume(
            cte_id,
            "probe",
            vec![probe.clone()],
            vec![source.column_id],
        )),
        hash_distribution(values(vec![build.clone()], vec![vec![literal_int(3)]])),
        vec![probe, build],
    );
    let plan = finish_for_test(&cte_anchor(cte_id, producer, join)).unwrap();
    assert_eq!(plan.fragments().len(), 3);
    assert_eq!(plan.edges().len(), 2);
    let mut shared_scheme = None;
    for edge in plan.edges().values() {
        let Distribution::Hash { keys, scheme } = &edge.partitioning.source else {
            panic!("join input must be hashed at its original producer");
        };
        let Distribution::Hash {
            keys: receive_keys,
            scheme: receive_scheme,
        } = &edge.partitioning.destination
        else {
            panic!("join input must import the same hash space");
        };
        assert_eq!(scheme, receive_scheme);
        assert_eq!(keys.as_ref(), &[edge.destination.receive_mapping[0].0]);
        assert_eq!(
            receive_keys.as_ref(),
            &[edge.destination.receive_mapping[0].1]
        );
        assert!(shared_scheme.is_none_or(|other| other == scheme));
        shared_scheme = Some(scheme);
    }
}

#[test]
fn comparison_exchange_fuses_only_when_the_original_cte_value_is_aligned() {
    for needs_conversion in [false, true] {
        let cte_id = 13;
        let source = column(1, "source", DataType::Int32, false);
        let producer = cte_produce(
            cte_id,
            vec![source.clone()],
            values(vec![source.clone()], vec![vec![literal_int32(3)]]),
        );
        let left = hash_distribution(cte_consume(
            cte_id,
            "left",
            vec![column(10, "left", DataType::Int32, false)],
            vec![source.column_id],
        ));
        let (comparison_type, right_literal) = if needs_conversion {
            (DataType::Int64, literal_int(3))
        } else {
            (DataType::Int32, literal_int32(3))
        };
        let right = hash_distribution(values(
            vec![column(20, "right", comparison_type.clone(), false)],
            vec![vec![right_literal]],
        ));
        let output = column(30, "comparison", comparison_type, false);
        let comparison = PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(crate::planner::physical::PhysicalSetOpNode {
                kind: PlanSetOpKind::Except,
                output_columns: vec![output.clone()],
                child_output_columns: vec![
                    left.output_columns.clone(),
                    right.output_columns.clone(),
                ],
            }),
            children: vec![left, right],
            output_columns: vec![output],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let plan = finish_for_test(&cte_anchor(cte_id, producer, comparison)).unwrap();
        let cte_edge = plan
            .edges()
            .values()
            .find(|edge| edge.kind == EdgeKind::CteMulticast)
            .unwrap();
        if needs_conversion {
            assert_eq!(cte_edge.partitioning.destination, Distribution::RoundRobin);
            let stage = &plan.fragments()[&cte_edge.destination.fragment];
            assert!(matches!(
                &stage.nodes()[&stage.root()].kind,
                NodeKind::Project { .. }
            ));
            assert!(matches!(
                stage.stage_placement(),
                novarocks_physical_plan::StagePlacementPolicy::FollowSources { sources }
                if sources.as_ref() == &[cte_edge.source.fragment]
            ));
            let hash_edge = plan
                .edges()
                .values()
                .find(|edge| edge.source.fragment == stage.id())
                .unwrap();
            let Distribution::Hash { keys, .. } = &hash_edge.partitioning.source else {
                panic!("conversion must occur before the comparison exchange");
            };
            assert_eq!(stage.values()[&keys[0]].ty.data_type(), &DataType::Int64);
        } else {
            assert!(matches!(
                &cte_edge.partitioning.destination,
                Distribution::Hash { .. }
            ));
            let stage = &plan.fragments()[&cte_edge.destination.fragment];
            let node = &stage.nodes()[&stage.root()];
            assert!(matches!(
                &node.kind,
                NodeKind::SetOp {
                    kind: SetOperationKind::Except,
                    ..
                }
            ));
            assert_eq!(node.inputs[0], cte_edge.destination.node);
        }
    }
}

#[test]
fn cte_consume_preserves_aliases_and_repeated_producer_occurrences() {
    let cte_id = 7;
    let producer_key = column(1, "producer_key", DataType::Int64, false);
    let producer_value = column(2, "producer_value", DataType::Utf8, false);
    let producer_columns = vec![producer_key, producer_value.clone()];
    let produce = cte_produce(
        cte_id,
        producer_columns.clone(),
        values(
            producer_columns,
            vec![vec![
                literal_int(3),
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String("payload".into())),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Utf8,
                        false,
                    ),
                },
            ]],
        ),
    );
    let first_alias = column(10, "first_alias", DataType::Utf8, false);
    let second_alias = column(11, "second_alias", DataType::Utf8, false);
    let consume = cte_consume(
        cte_id,
        "renamed_cte",
        vec![first_alias.clone(), second_alias.clone()],
        vec![producer_value.column_id, producer_value.column_id],
    );

    let final_plan = finish_for_test(&cte_anchor(cte_id, produce, consume)).unwrap();
    assert_eq!(final_plan.fragments().len(), 2);
    assert_eq!(final_plan.edges().len(), 1);
    let producer = final_plan.fragments().get(&FragmentId::new(1)).unwrap();
    let FragmentSink::Multicast { edges } = producer.sink() else {
        panic!("expected CTE producer multicast sink");
    };
    assert_eq!(edges.len(), 1);
    let producer_root = producer.nodes().get(&producer.root()).unwrap();
    let repeated_value = producer_root.output.columns[1];
    let edge = final_plan.edges().get(&edges[0]).unwrap();
    assert_eq!(edge.kind, EdgeKind::CteMulticast);
    assert_eq!(edge.partitioning.source, Distribution::RoundRobin);
    assert_eq!(edge.partitioning.destination, Distribution::RoundRobin);
    assert_eq!(
        edge.source.projection.as_ref(),
        &[repeated_value, repeated_value]
    );
    assert_eq!(edge.destination.receive_mapping[0].0, repeated_value);
    assert_eq!(edge.destination.receive_mapping[1].0, repeated_value);
    assert_ne!(
        edge.destination.receive_mapping[0].1,
        edge.destination.receive_mapping[1].1
    );
    let consumer = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
    for (_, imported) in &edge.destination.receive_mapping {
        assert!(matches!(
            consumer.values().get(imported).unwrap().origin,
            ValueOrigin::CteImport {
                edge: import_edge,
                producer_fragment,
                producer_value,
            } if import_edge == edge.id
                && producer_fragment == FragmentId::new(1)
                && producer_value == repeated_value
        ));
    }
    let fields = &final_plan.result_port().unwrap().fields;
    assert_eq!(fields[0].name.as_ref(), first_alias.name);
    assert_eq!(fields[1].name.as_ref(), second_alias.name);
}

#[test]
fn cte_self_join_rejects_the_data_wait_cycle_before_publication() {
    for build_side in [
        PhysicalHashJoinBuildSide::Left,
        PhysicalHashJoinBuildSide::Right,
    ] {
        let cte_id = 80;
        let shared = column(1, "shared", DataType::Int64, false);
        let produce = cte_produce(
            cte_id,
            vec![shared.clone()],
            values(vec![shared.clone()], vec![vec![literal_int(9)]]),
        );
        let left = column(10, "left_alias", DataType::Int64, false);
        let right = column(20, "right_alias", DataType::Int64, false);
        let branch = |column: &OutputColumn, alias| {
            redistribute(
                cte_consume(cte_id, alias, vec![column.clone()], vec![shared.column_id]),
                RedistributeMode::Gather,
            )
        };
        let join = hash_join(
            crate::common::JoinKind::Inner,
            build_side,
            SqlJoinDistribution::Singleton,
            Some(JoinExecutionMode::Singleton),
            branch(&left, "left_cte"),
            branch(&right, "right_cte"),
            vec![left.clone(), right.clone()],
        );
        let error = finish_for_test(&cte_anchor(cte_id, produce, join)).unwrap_err();
        let ContractLoweringError::UnsupportedWaitCycle(cause) = &error else {
            panic!("shared producer must be rejected for its data wait cycle: {error}");
        };
        assert!(cause.to_string().contains("decoupled retained output (B4)"));
        let expected = cause.clone();
        let crate::compiler::SqlCompileError::CompilationSource(source) = error.into() else {
            panic!("the planning capability must retain its original source");
        };
        assert_eq!(
            source.downcast_ref::<crate::compiler::StructuralWaitCycle>(),
            Some(&expected)
        );
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&source).reason(),
            novarocks_failure_contract::sql_reason::STRUCTURAL_WAIT_CYCLE
        );
    }
}

#[test]
fn cte_multicast_has_one_exact_edge_per_consumer() {
    let cte_id = 8;
    let producer_column = column(1, "producer", DataType::Int64, false);
    let produce = cte_produce(
        cte_id,
        vec![producer_column.clone()],
        values(vec![producer_column.clone()], vec![vec![literal_int(9)]]),
    );
    let left_column = column(10, "left_alias", DataType::Int64, false);
    let right_column = column(20, "right_alias", DataType::Int64, false);
    let left = cte_consume(
        cte_id,
        "left_cte",
        vec![left_column.clone()],
        vec![producer_column.column_id],
    );
    let right = cte_consume(
        cte_id,
        "right_cte",
        vec![right_column.clone()],
        vec![producer_column.column_id],
    );
    let result_column = column(30, "combined", DataType::Int64, false);
    let body = PhysicalPlanNode {
        kind: PhysicalPlanKind::SetOp(crate::planner::physical::PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns: vec![result_column.clone()],
            child_output_columns: vec![vec![left_column], vec![right_column]],
        }),
        children: vec![left, right],
        output_columns: vec![result_column],
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    };

    let final_plan = finish_for_test(&cte_anchor(cte_id, produce, body)).unwrap();
    let producer = final_plan.fragments().get(&FragmentId::new(1)).unwrap();
    let FragmentSink::Multicast { edges } = producer.sink() else {
        panic!("expected CTE producer multicast sink");
    };
    assert_eq!(edges.len(), 2);
    assert_eq!(final_plan.edges().len(), 2);
    let producer_value = producer
        .nodes()
        .get(&producer.root())
        .unwrap()
        .output
        .columns[0];
    let mut destination_nodes = BTreeSet::new();
    for edge_id in edges {
        let edge = final_plan.edges().get(edge_id).unwrap();
        assert_eq!(edge.kind, EdgeKind::CteMulticast);
        assert_eq!(edge.partitioning.source, Distribution::RoundRobin);
        assert_eq!(edge.partitioning.destination, Distribution::RoundRobin);
        assert_eq!(edge.source.fragment, FragmentId::new(1));
        assert_eq!(edge.source.projection.as_ref(), &[producer_value]);
        assert!(destination_nodes.insert(edge.destination.node));
    }
}

#[test]
fn multicast_with_independent_builds_keeps_blocking_runtime_filters() {
    let cte_id = 81;
    let producer_column = column(1, "shared_key", DataType::Int64, false);
    let produce = cte_produce(
        cte_id,
        vec![producer_column.clone()],
        values(vec![producer_column.clone()], vec![vec![literal_int(9)]]),
    );
    let mut branches = Vec::new();
    let mut branch_outputs = Vec::new();
    for (filter_id, raw, shared) in [(81, 10, true), (82, 20, true), (83, 30, false)] {
        let probe = column(raw, "probe", DataType::Int64, false);
        let build = column(raw + 1, "build", DataType::Int64, false);
        let probe_plan = if shared {
            // A CTE import preserves single-copy rows but does not promise
            // singleton distribution. Gather establishes the Join's input
            // requirement without changing the shared multicast ancestry.
            redistribute(
                cte_consume(
                    cte_id,
                    "shared",
                    vec![probe.clone()],
                    vec![producer_column.column_id],
                ),
                RedistributeMode::Gather,
            )
        } else {
            values(vec![probe.clone()], vec![vec![literal_int(9)]])
        };
        let mut branch = hash_join(
            crate::common::JoinKind::Inner,
            PhysicalHashJoinBuildSide::Right,
            SqlJoinDistribution::Singleton,
            Some(JoinExecutionMode::Singleton),
            probe_plan,
            values(vec![build], vec![vec![literal_int(9)]]),
            vec![probe.clone()],
        );
        attach_singleton_join_runtime_filter(&mut branch, filter_id, false, true);
        if shared {
            let gather = &mut branch.children[0];
            gather.children[0].probe_runtime_filters =
                std::mem::take(&mut gather.probe_runtime_filters);
        }
        branches.push(branch);
        branch_outputs.push(vec![probe]);
    }
    let output = column(40, "key", DataType::Int64, false);
    let body = PhysicalPlanNode {
        kind: PhysicalPlanKind::SetOp(crate::planner::physical::PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns: vec![output.clone()],
            child_output_columns: branch_outputs,
        }),
        children: branches,
        output_columns: vec![output],
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    };
    let plan = finish_for_test(&cte_anchor(cte_id, produce, body))
        .expect("independent RF builds must not leave the shared multicast blocked");
    assert_runtime_filter_attachments_match_endpoints(&plan);
    assert!(plan.fragments().values().any(|fragment| {
        matches!(fragment.sink(), FragmentSink::Multicast { edges } if edges.len() == 2)
    }));
    for id in [82, 83] {
        let filter = &plan.runtime_filters()[&RuntimeFilterId::new(id)];
        let producer = &filter.producers[0];
        let fragment = &plan.fragments()[&producer.endpoint.fragment];
        let join = &fragment.nodes()[&producer.endpoint.node];
        assert!(
            matches!(
                fragment.nodes()[&join.inputs[1]].kind,
                NodeKind::Values { .. }
            ),
            "the filter build is independent of the shared multicast"
        );
        assert_eq!(filter.consumers.len(), 1);
        assert_eq!(
            filter.consumers[0].activation,
            RuntimeFilterConsumerActivation::BlockingSnapshot
        );
        let endpoint = &filter.consumers[0].endpoint;
        assert_eq!(endpoint.node, join.id);
        let edge = plan
            .edges()
            .values()
            .find(|edge| {
                edge.kind == EdgeKind::CteMulticast
                    && edge.destination.fragment == endpoint.fragment
                    && edge.destination.node == join.inputs[0]
            })
            .expect("the runtime-filter probe reads from the fused CTE receiver");
        assert_eq!(edge.partitioning.destination, Distribution::Singleton);
        let import = edge.destination.receive_mapping[0].1;
        assert_eq!(endpoint.values.as_ref(), &[import]);
        assert!(plan.annotations().iter().any(|annotation| {
            annotation.subject
                == novarocks_physical_plan::AnnotationSubject::Value(endpoint.fragment, import)
                && annotation.key.as_ref() == "sql.display_name"
                && annotation.value.as_ref() == "probe"
        }));
    }
    assert_eq!(
        plan.runtime_filters()[&RuntimeFilterId::new(84)].consumers[0].activation,
        RuntimeFilterConsumerActivation::BlockingSnapshot
    );
}

#[test]
fn cte_consume_rejects_unknown_duplicate_and_inexact_mappings() {
    let unknown = cte_consume(
        99,
        "missing",
        vec![column(10, "x", DataType::Int64, false)],
        vec![ColumnId(1)],
    );
    assert!(
        finish_for_test(&unknown)
            .unwrap_err()
            .to_string()
            .contains("unknown or out-of-scope producer 99")
    );

    let cte_id = 9;
    let producer_column = column(1, "producer", DataType::Int64, false);
    let produce = || {
        cte_produce(
            cte_id,
            vec![producer_column.clone()],
            values(vec![producer_column.clone()], vec![vec![literal_int(1)]]),
        )
    };
    let arity = cte_consume(
        cte_id,
        "arity",
        vec![column(10, "x", DataType::Int64, false)],
        Vec::new(),
    );
    assert!(
        finish_for_test(&cte_anchor(cte_id, produce(), arity))
            .unwrap_err()
            .to_string()
            .contains("CTEConsume producer mapping expected 1 item(s), got 0")
    );

    let duplicate = cte_consume(
        cte_id,
        "duplicate",
        vec![
            column(10, "x", DataType::Int64, false),
            column(10, "y", DataType::Int64, false),
        ],
        vec![producer_column.column_id, producer_column.column_id],
    );
    assert!(
        finish_for_test(&cte_anchor(cte_id, produce(), duplicate))
            .unwrap_err()
            .to_string()
            .contains("repeats output column c10")
    );

    let missing_column = cte_consume(
        cte_id,
        "missing_column",
        vec![column(10, "x", DataType::Int64, false)],
        vec![ColumnId(404)],
    );
    assert!(
        finish_for_test(&cte_anchor(cte_id, produce(), missing_column))
            .unwrap_err()
            .to_string()
            .contains("missing producer column c404")
    );

    let wrong_type = cte_consume(
        cte_id,
        "wrong_type",
        vec![column(10, "x", DataType::Utf8, false)],
        vec![producer_column.column_id],
    );
    assert!(
        finish_for_test(&cte_anchor(cte_id, produce(), wrong_type))
            .unwrap_err()
            .to_string()
            .contains("producer column c1 has type")
    );
}

#[test]
fn cte_anchor_rejects_duplicate_active_definition() {
    let cte_id = 10;
    let outer_column = column(1, "outer", DataType::Int64, false);
    let inner_column = column(2, "inner", DataType::Int64, false);
    let inner_consume = cte_consume(
        cte_id,
        "inner",
        vec![column(20, "value", DataType::Int64, false)],
        vec![inner_column.column_id],
    );
    let inner = cte_anchor(
        cte_id,
        cte_produce(
            cte_id,
            vec![inner_column.clone()],
            values(vec![inner_column], vec![vec![literal_int(2)]]),
        ),
        inner_consume,
    );
    let outer = cte_anchor(
        cte_id,
        cte_produce(
            cte_id,
            vec![outer_column.clone()],
            values(vec![outer_column], vec![vec![literal_int(1)]]),
        ),
        inner,
    );

    assert!(
        finish_for_test(&outer)
            .unwrap_err()
            .to_string()
            .contains("duplicate active CTE definition 10")
    );
}
