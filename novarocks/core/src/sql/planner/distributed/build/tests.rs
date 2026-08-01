use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroU32;

use arrow::datatypes::DataType;

use super::fragment_cut::cut;
use super::fragment_cut::stream_exchange_output_columns;
use super::lowering::{NodeIdAllocator, lower_fragment_local_node};
use super::runtime_filter_binding::{
    RuntimeFilterBindings, RuntimeFilterBuildBinding as BuildBinding,
    RuntimeFilterProbeBinding as ProbeBinding, populate_runtime_filter_graph,
};
use super::{build_distributed_plan, union_distinct_must_be_rewritten_error};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
    ConsumerActivation, ContributionKind, CoverageWitnessId, NullOrder, NullSemantics,
    ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
    RuntimeFilterPolicyRequirement, SortDirection,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::model::graph::{
    ConsumerBindingTarget, ProducerBindingTarget, RuntimeFilterBindingRole,
    RuntimeFilterBindingRoleData, RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
    RuntimeFilterGraph, RuntimeFilterGraphData,
};
use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::activation_decision::{
    ActivationConstraint, ActivationFallback, DraftRuntimeFilterGraph,
};
use crate::sql::planner::distributed::runtime_filter_progress::build_join_progress_proof_catalog;
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, ExchangeFlavor,
    FragmentEdgeKind, FragmentStreamKind, FrontierEdge, PartitionKind, PlanFragment,
};
use crate::sql::planner::payload::{
    AggregateCall, PlanAssertOneRowNode, PlanCTEAnchorNode, PlanCTEConsumeNode, PlanCTEProduceNode,
    PlanFilterNode, PlanGenerateSeriesNode, PlanLimitNode, PlanProjectNode, PlanRepeatNode,
    PlanScanNode, PlanSortNode, PlanTableFunctionNode, PlanValuesNode, PlanWindowNode, WindowExpr,
};
use crate::sql::planner::physical::runtime_filter::{
    AggregateTopNRuntimeFilterBuildIntent, RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
use crate::sql::planner::physical::{
    AggMode, AggregateOutputLayout, HashSource, JoinDistribution, JoinExecutionMode,
    PhysicalPlanStats, PlannerConfidence, PlannerCostEstimate, TopNPhase,
};
use crate::sql::planner::physical::{
    DistributedChangeEventExpandNode, PhysicalHashAggregateNode, PhysicalHashJoinEqCondition,
    PhysicalHashJoinNode, PhysicalNestLoopJoinNode, PhysicalPlanKind, PhysicalPlanNode,
    PhysicalSetOpNode, PhysicalTopNNode, PlanSetOpKind, RedistributeMode, RedistributeNode,
};
use crate::sql::planner::table::{ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActivationNeutralGraphStructure {
    channels: Vec<ActivationNeutralChannelStructure>,
    bindings: Vec<ActivationNeutralBindingStructure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActivationNeutralChannelStructure {
    channel_id: ChannelId,
    logical_domain: RuntimeFilterLogicalDomain,
    lifecycle: RuntimeFilterLifecycle,
    availability_coverage: Coverage,
    terminal_coverage: Coverage,
    reduction_requirement: ReductionRequirement,
    allowed_contribution_kinds: BTreeSet<ContributionKind>,
    required_consumer_capabilities: BTreeSet<ArtifactCapability>,
    policy: RuntimeFilterPolicyRequirement,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActivationNeutralBindingStructure {
    binding_id: BindingId,
    channel_id: ChannelId,
    coverage_witness_id: Option<CoverageWitnessId>,
    location: crate::runtime_filter::model::graph::PlanLocation,
    expression: String,
    apply_point: crate::runtime_filter::model::graph::ApplyPoint,
    role: ActivationNeutralBindingRoleStructure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ActivationNeutralBindingRoleStructure {
    Producer {
        contribution_kinds: BTreeSet<ContributionKind>,
        completion_requirement: CompletionRequirement,
        target: ProducerBindingTarget,
    },
    Consumer {
        capabilities: BTreeSet<ArtifactCapability>,
        target: ConsumerBindingTarget,
    },
}

fn activation_neutral_graph_structure<A>(
    graph: &RuntimeFilterGraphData<A>,
) -> ActivationNeutralGraphStructure {
    ActivationNeutralGraphStructure {
        channels: graph
            .channels()
            .map(|channel| ActivationNeutralChannelStructure {
                channel_id: channel.channel_id,
                logical_domain: channel.logical_domain.clone(),
                lifecycle: channel.lifecycle,
                availability_coverage: channel.availability_coverage.clone(),
                terminal_coverage: channel.terminal_coverage.clone(),
                reduction_requirement: channel.reduction_requirement,
                allowed_contribution_kinds: channel.allowed_contribution_kinds.clone(),
                required_consumer_capabilities: channel.required_consumer_capabilities.clone(),
                policy: channel.policy,
            })
            .collect(),
        bindings: graph
            .bindings()
            .map(|binding| ActivationNeutralBindingStructure {
                binding_id: binding.binding_id,
                channel_id: binding.channel_id,
                coverage_witness_id: binding.coverage_witness_id,
                location: binding.location,
                expression: format!("{:?}", binding.expression),
                apply_point: binding.apply_point,
                role: match &binding.role {
                    RuntimeFilterBindingRoleData::Producer(producer) => {
                        ActivationNeutralBindingRoleStructure::Producer {
                            contribution_kinds: producer.contribution_kinds.clone(),
                            completion_requirement: producer.completion_requirement,
                            target: producer.target,
                        }
                    }
                    RuntimeFilterBindingRoleData::Consumer(consumer) => {
                        ActivationNeutralBindingRoleStructure::Consumer {
                            capabilities: consumer.capabilities.clone(),
                            target: consumer.target,
                        }
                    }
                },
            })
            .collect(),
    }
}

fn copy_graph_with_structural_drift(
    graph: &RuntimeFilterGraph,
    mutate_channel: impl Fn(&mut RuntimeFilterChannelSpec),
    mutate_binding: impl Fn(&mut RuntimeFilterBindingSpec),
) -> RuntimeFilterGraph {
    let mut copy = RuntimeFilterGraph::default();
    for channel in graph.channels() {
        let mut channel = channel.clone();
        mutate_channel(&mut channel);
        copy.insert_channel(channel).expect("copy channel");
    }
    for binding in graph.bindings() {
        let mut binding = binding.clone();
        mutate_binding(&mut binding);
        copy.insert_binding(binding).expect("copy binding");
    }
    copy
}

fn assert_snapshot_detects_channel_mutation(
    graph: &RuntimeFilterGraph,
    field: &str,
    mutate: impl Fn(&mut RuntimeFilterChannelSpec),
) {
    let drift = copy_graph_with_structural_drift(graph, mutate, |_| {});
    assert_ne!(
        activation_neutral_graph_structure(graph),
        activation_neutral_graph_structure(&drift),
        "activation-neutral snapshot must include channel.{field}"
    );
}

fn assert_snapshot_detects_binding_mutation(
    graph: &RuntimeFilterGraph,
    field: &str,
    mutate: impl Fn(&mut RuntimeFilterBindingSpec),
) {
    let drift = copy_graph_with_structural_drift(graph, |_| {}, mutate);
    assert_ne!(
        activation_neutral_graph_structure(graph),
        activation_neutral_graph_structure(&drift),
        "activation-neutral snapshot must include binding.{field}"
    );
}

#[test]
fn build_distributed_plan_values_shapes_root_fragment() {
    let output_columns = vec![output_col(1, "k", DataType::Int64, false)];
    let plan = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: output_columns.clone(),
        }),
        children: vec![],
        output_columns: output_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&plan).expect("build_distributed_plan");

    assert!(dp.runtime_filter_graph().is_empty());
    assert_eq!(dp.fragments().len(), 1);
    assert_eq!(dp.root_fragment_id(), 0);
    assert!(dp.edges().is_empty());

    let fragment = &dp.fragments()[0];
    assert_eq!(fragment.fragment_id, 0);
    assert!(matches!(fragment.sink, DataSink::Result));
    assert!(matches!(
        fragment.data_partition.kind,
        PartitionKind::Unpartitioned
    ));
    assert!(matches!(
        fragment.output_partition.kind,
        PartitionKind::Unpartitioned
    ));
    assert!(fragment.output_exprs.is_none());
    assert_eq!(fragment.output_columns.len(), output_columns.len());
    assert_eq!(
        fragment.output_columns[0].column_id,
        output_columns[0].column_id
    );
    assert_eq!(fragment.output_columns[0].name, output_columns[0].name);
    assert!(fragment.cte_id.is_none());
    assert!(fragment.cte_exchange_nodes.is_empty());

    assert!(matches!(
        &fragment.root.payload,
        DistributedNodeKind::Values(_)
    ));
    assert_eq!(fragment.root.node_id, 1);
    assert_eq!(fragment.root.tuple_ids, vec![1]);
    assert_eq!(fragment.root.fragment_id, 0);
    assert!(fragment.root.children.is_empty());
}

#[test]
fn lowering_materializes_fragment_local_node_without_topology() {
    let child_columns = vec![output_col(1, "k", DataType::Int64, false)];
    let child = DistributedNode {
        node_id: 3,
        fragment_id: 9,
        tuple_ids: vec![4],
        nullable_tuple_ids: vec![],
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![],
        stats: stats_with_row_count(10.0),
        payload: DistributedNodeKind::Values(PlanValuesNode {
            rows: vec![],
            columns: child_columns.clone(),
        }),
    };
    let project = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: column_ref_expr(1, "k", DataType::Int64, false),
                output_name: "k_alias".to_string(),
                output_column_id: ColumnId::new_for_test(2),
            }],
            output_qualifier: None,
        }),
        children: vec![],
        output_columns: vec![output_col(2, "k_alias", DataType::Int64, false)],
        stats: stats_with_row_count(5.0),
        probe_runtime_filters: vec![],
    };
    let mut ids = NodeIdAllocator::new(7, 11);

    let lowered = lower_fragment_local_node(&project, 9, vec![child], &mut ids)
        .expect("lower fragment-local Project");

    assert_eq!(lowered.node_id, 7);
    assert_eq!(lowered.fragment_id, 9);
    assert_eq!(lowered.tuple_ids, vec![11]);
    assert_eq!(lowered.children.len(), 1);
    assert!(matches!(lowered.payload, DistributedNodeKind::Project(_)));
    assert_eq!(lowered.stats.output_row_count, 5.0);
}

#[test]
fn build_distributed_plan_scan_project_shapes_one_fragment() {
    let scan_columns = vec![output_col(1, "k", DataType::Int64, false)];
    let project_columns = vec![output_col(2, "k_alias", DataType::Int64, false)];
    let scan = PhysicalPlanNode {
        kind: PhysicalPlanKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table: table_def(),
            alias: Some("t".to_string()),
            columns: scan_columns.clone(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }),
        children: vec![],
        output_columns: scan_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let project = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: column_ref_expr(1, "k", DataType::Int64, false),
                output_name: "k_alias".to_string(),
                output_column_id: ColumnId::new_for_test(2),
            }],
            output_qualifier: None,
        }),
        children: vec![scan],
        output_columns: project_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&project).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 1);
    let root = &dp.fragments()[0].root;
    assert!(matches!(&root.payload, DistributedNodeKind::Project(_)));
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![2]);
    assert_eq!(root.children.len(), 1);

    let child = &root.children[0];
    assert!(matches!(&child.payload, DistributedNodeKind::Scan(_)));
    assert_eq!(child.node_id, 1);
    assert_eq!(child.tuple_ids, vec![1]);
}

#[test]
fn build_distributed_plan_preserves_filter_over_scan_without_mutating_scan() {
    let scan_columns = vec![
        output_col(1, "k", DataType::Int64, false),
        output_col(2, "predicate_only", DataType::Int64, false),
    ];
    let scan = PhysicalPlanNode {
        kind: PhysicalPlanKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table: table_def_with_columns(&scan_columns),
            alias: Some("t".to_string()),
            columns: scan_columns.clone(),
            predicates: vec![bool_lit(true)],
            required_columns: Some(vec!["k".to_string(), "predicate_only".to_string()]),
            variant_columns: vec![],
            mv_rewritten_from: None,
        }),
        children: vec![],
        output_columns: scan_columns.clone(),
        stats: stats_with_row_count(100.0),
        probe_runtime_filters: vec![],
    };
    let filter = PhysicalPlanNode {
        kind: PhysicalPlanKind::Filter(PlanFilterNode {
            predicate: cmp_expr(2, "predicate_only", BinOp::Gt, 10),
        }),
        children: vec![scan],
        output_columns: scan_columns,
        stats: stats_with_row_count(5.0),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&filter).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    let root_filter = match &root.payload {
        DistributedNodeKind::Filter(filter) => filter,
        other => panic!("expected Filter root, got {other:?}"),
    };
    assert_cmp_expr(&root_filter.predicate, 2, "predicate_only", BinOp::Gt, 10);
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![1]);
    assert_eq!(root.stats.output_row_count, 5.0);
    assert_eq!(root.children.len(), 1);

    let scan = match &root.children[0].payload {
        DistributedNodeKind::Scan(scan) => scan,
        other => panic!("expected Scan child, got {other:?}"),
    };
    assert_eq!(scan.predicates.len(), 1);
    assert_bool_lit(&scan.predicates[0], true);
    assert_eq!(
        scan.required_columns.as_ref(),
        Some(&vec!["k".to_string(), "predicate_only".to_string()])
    );
}

#[test]
fn build_distributed_plan_preserves_filter_over_project() {
    let scan_columns = vec![output_col(1, "k", DataType::Int64, false)];
    let project_columns = vec![output_col(2, "k_alias", DataType::Int64, false)];
    let scan = PhysicalPlanNode {
        kind: PhysicalPlanKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table: table_def(),
            alias: Some("t".to_string()),
            columns: scan_columns.clone(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }),
        children: vec![],
        output_columns: scan_columns.clone(),
        stats: stats_with_row_count(100.0),
        probe_runtime_filters: vec![],
    };
    let project = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: column_ref_expr(1, "k", DataType::Int64, false),
                output_name: "k_alias".to_string(),
                output_column_id: ColumnId::new_for_test(2),
            }],
            output_qualifier: None,
        }),
        children: vec![scan],
        output_columns: project_columns.clone(),
        stats: stats_with_row_count(10.0),
        probe_runtime_filters: vec![],
    };
    let filter = PhysicalPlanNode {
        kind: PhysicalPlanKind::Filter(PlanFilterNode {
            predicate: cmp_expr(2, "k_alias", BinOp::Gt, 10),
        }),
        children: vec![project],
        output_columns: project_columns,
        stats: stats_with_row_count(5.0),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&filter).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    let root_filter = match &root.payload {
        DistributedNodeKind::Filter(filter) => filter,
        other => panic!("expected Filter root, got {other:?}"),
    };
    assert_cmp_expr(&root_filter.predicate, 2, "k_alias", BinOp::Gt, 10);
    assert_eq!(root.node_id, 3);
    assert_eq!(root.tuple_ids, vec![2]);
    assert_eq!(root.stats.output_row_count, 5.0);
    assert_eq!(root.children.len(), 1);

    let child = &root.children[0];
    assert!(matches!(&child.payload, DistributedNodeKind::Project(_)));
    assert_eq!(child.node_id, 2);
    assert_eq!(child.tuple_ids, vec![2]);
}

#[test]
fn build_distributed_plan_sort_reuses_child_tuple() {
    let scan = scan_node(1, "k");
    let sort = PhysicalPlanNode {
        kind: PhysicalPlanKind::Sort(PlanSortNode {
            items: vec![],
            analytic_partition_by: vec![],
            output_columns: scan.output_columns.clone(),
            offset: None,
            partition_limit: None,
            topn_type: None,
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&sort).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(&root.payload, DistributedNodeKind::Sort(_)));
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![1]);
    assert_eq!(root.children.len(), 1);
    assert_eq!(root.children[0].node_id, 1);
    assert!(matches!(
        &root.children[0].payload,
        DistributedNodeKind::Scan(_)
    ));
}

#[test]
fn build_distributed_plan_hash_aggregate_allocates_new_tuple() {
    let scan = scan_node(1, "k");
    // A minimal `COUNT(*)` aggregate: a real aggregate always produces at least one
    // output column, which the sealed node-output catalog now requires (this test
    // only asserts tuple allocation).
    let count_col = output_col(20, "count", DataType::Int64, false);
    let aggregate = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: "count".to_string(),
                args: vec![],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: ColumnId::new_for_test(20),
            }],
            is_merge: vec![false],
            output_layout: AggregateOutputLayout::new(vec![], vec![count_col.clone()]),
            output_columns: vec![count_col.clone()],
            topn_runtime_filter_builds: Vec::new(),
        })),
        children: vec![scan],
        output_columns: vec![count_col],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&aggregate).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(
        &root.payload,
        DistributedNodeKind::HashAggregate(_)
    ));
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![2]);
    assert_eq!(root.children.len(), 1);
    assert_eq!(root.children[0].tuple_ids, vec![1]);
}

#[test]
fn build_distributed_plan_hash_join_combines_child_tuples() {
    let left = scan_node(1, "l_k");
    let right = scan_node(2, "r_k");
    let output_columns = vec![
        output_col(1, "l_k", DataType::Int64, false),
        output_col(2, "r_k", DataType::Int64, false),
    ];
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
            execution_mode: None,
            build_runtime_filters: vec![],
            output_columns: output_columns.clone(),
        })),
        children: vec![left, right],
        output_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&join).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(&root.payload, DistributedNodeKind::HashJoin(_)));
    assert_eq!(root.node_id, 3);
    assert_eq!(root.tuple_ids, vec![1, 2]);
    assert_eq!(root.children.len(), 2);
    assert_eq!(root.children[0].node_id, 1);
    assert_eq!(root.children[0].tuple_ids, vec![1]);
    assert_eq!(root.children[1].node_id, 2);
    assert_eq!(root.children[1].tuple_ids, vec![2]);
}

#[test]
fn stream_exchange_output_columns_uses_source_metadata_for_requested_ids() {
    let source = vec![output_col(20, "avg(v)", DataType::Utf8, true)];
    let requested = vec![output_col(20, "avg(v)", DataType::Float64, true)];

    let output = stream_exchange_output_columns(&source, &requested);

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].column_id, ColumnId::new_for_test(20));
    assert_eq!(output[0].data_type, DataType::Utf8);
}

#[test]
fn stream_exchange_output_columns_falls_back_to_requested_when_source_is_empty() {
    let requested = vec![output_col(20, "avg(v)", DataType::Float64, true)];

    let output = stream_exchange_output_columns(&[], &requested);

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].column_id, requested[0].column_id);
    assert_eq!(output[0].name, requested[0].name);
    assert_eq!(output[0].data_type, requested[0].data_type);
    assert_eq!(output[0].nullable, requested[0].nullable);
    assert_eq!(output[0].is_internal, requested[0].is_internal);
}

#[test]
fn build_distributed_plan_seals_partitioned_join_progress_certificate() {
    let filter_id = 77;
    let probe_expr = column_ref_expr(1, "l_k", DataType::Int64, false);
    let build_expr = column_ref_expr(2, "r_k", DataType::Int64, false);
    let duplicate_probe_intent = RuntimeFilterProbeIntent {
        filter_id,
        probe_expr: probe_expr.clone(),
    };
    let mut left_scan = scan_node(1, "l_k");
    left_scan.probe_runtime_filters = vec![duplicate_probe_intent.clone()];
    let left_project = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: probe_expr.clone(),
                output_name: "l_k".to_string(),
                output_column_id: ColumnId::new_for_test(1),
            }],
            output_qualifier: None,
        }),
        children: vec![left_scan],
        output_columns: vec![output_col(1, "l_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![duplicate_probe_intent],
    };
    let left_redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Hash {
                cols: vec![ColumnId::new_for_test(1)],
                source: HashSource::ShuffleJoin,
            },
            partition_exprs: vec![],
            output_columns: left_project.output_columns.clone(),
        }),
        children: vec![left_project],
        output_columns: vec![output_col(1, "l_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let right_scan = scan_node(2, "r_k");
    let right = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Hash {
                cols: vec![ColumnId::new_for_test(2)],
                source: HashSource::ShuffleJoin,
            },
            partition_exprs: vec![build_expr.clone()],
            output_columns: right_scan.output_columns.clone(),
        }),
        children: vec![right_scan],
        output_columns: vec![output_col(2, "r_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: probe_expr.clone(),
                right: build_expr.clone(),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
            execution_mode: Some(JoinExecutionMode::Partitioned),
            build_runtime_filters: vec![RuntimeFilterBuildIntent {
                filter_id,
                build_expr: build_expr.clone(),
                probe_expr: probe_expr.clone(),
                expr_order: 3,
                execution_mode: JoinExecutionMode::Partitioned,
            }],
            output_columns: vec![
                output_col(1, "l_k", DataType::Int64, false),
                output_col(2, "r_k", DataType::Int64, false),
            ],
        })),
        children: vec![left_redistribute, right],
        output_columns: vec![
            output_col(1, "l_k", DataType::Int64, false),
            output_col(2, "r_k", DataType::Int64, false),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&join).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 3);
    let root_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id())
        .expect("root fragment");
    let graph = dp.runtime_filter_graph();
    let producer = graph
        .bindings()
        .find(|binding| matches!(binding.role, RuntimeFilterBindingRole::Producer(_)))
        .expect("producer binding");
    let probe_fragment_id = graph
        .bindings()
        .find(|binding| matches!(binding.role, RuntimeFilterBindingRole::Consumer(_)))
        .expect("consumer binding")
        .location
        .fragment_id
        .get();
    let probe_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == probe_fragment_id)
        .expect("probe fragment");
    let join_node = &root_fragment.root;
    assert_eq!(join_node.fragment_id, dp.root_fragment_id());
    assert!(matches!(
        &join_node.payload,
        DistributedNodeKind::HashJoin(_)
    ));
    assert_eq!(graph.channel_count(), 1);
    assert_eq!(graph.binding_count(), 3);
    assert_eq!(producer.location.fragment_id.get(), join_node.fragment_id);
    assert_eq!(producer.location.node_id.get(), join_node.node_id);
    assert_column_ref(&producer.expression, 2, "r_k");
    assert_eq!(
        join_node.runtime_filter_binding_ids,
        vec![producer.binding_id]
    );
    let proof = dp
        .runtime_filter_join_progress()
        .get(&(
            producer.channel_id,
            producer.binding_id,
            join_node.fragment_id,
        ))
        .expect("partitioned join progress proof");
    assert_eq!(proof.producer_fragment, join_node.fragment_id);
    assert_eq!(proof.non_build_inputs.len(), 1);
    assert_eq!(proof.build_frontier.len(), 1);
    assert_eq!(
        proof.non_build_inputs[0].source_fragment,
        probe_fragment.fragment_id
    );
    assert_ne!(
        proof.non_build_inputs[0].source_fragment,
        proof.build_frontier[0].source_fragment
    );

    let probe_project = &probe_fragment.root;
    assert_eq!(probe_project.fragment_id, probe_fragment.fragment_id);
    assert_eq!(probe_project.runtime_filter_binding_ids.len(), 1);
    let probe_scan = &probe_project.children[0];
    assert!(matches!(&probe_scan.payload, DistributedNodeKind::Scan(_)));
    assert_eq!(probe_scan.runtime_filter_binding_ids.len(), 1);
    for binding_id in probe_project
        .runtime_filter_binding_ids
        .iter()
        .chain(&probe_scan.runtime_filter_binding_ids)
    {
        let binding = graph.binding(*binding_id).expect("consumer binding");
        assert!(matches!(
            binding.role,
            RuntimeFilterBindingRole::Consumer(_)
        ));
        assert_column_ref(&binding.expression, 1, "l_k");
        assert_eq!(
            binding.location.fragment_id.get(),
            probe_fragment.fragment_id
        );
    }
}

#[test]
fn rfd_5a_join_population_is_deterministic_and_node_carried_only_by_binding_id() {
    let filter_id = 41;
    let probe_expr = column_ref_expr(1, "probe", DataType::Int64, false);
    let build_expr = column_ref_expr(2, "build", DataType::Int64, false);
    let probe_output = vec![output_col(1, "probe", DataType::Int64, false)];
    let probe = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: probe_output.clone(),
        }),
        children: vec![],
        output_columns: probe_output,
        stats: stats(),
        probe_runtime_filters: vec![RuntimeFilterProbeIntent {
            filter_id,
            probe_expr: probe_expr.clone(),
        }],
    };
    let build_output = vec![output_col(2, "build", DataType::Int64, false)];
    let build = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: build_output.clone(),
        }),
        children: vec![],
        output_columns: build_output,
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: probe_expr.clone(),
                right: build_expr.clone(),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
            execution_mode: Some(JoinExecutionMode::Broadcast),
            build_runtime_filters: vec![RuntimeFilterBuildIntent {
                filter_id,
                build_expr,
                probe_expr,
                expr_order: 0,
                execution_mode: JoinExecutionMode::Broadcast,
            }],
            output_columns: vec![
                output_col(1, "probe", DataType::Int64, false),
                output_col(2, "build", DataType::Int64, false),
            ],
        })),
        children: vec![probe, build],
        output_columns: vec![
            output_col(1, "probe", DataType::Int64, false),
            output_col(2, "build", DataType::Int64, false),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let distributed = build_distributed_plan(&join).expect("build RFD-5A join graph");
    let graph = distributed.runtime_filter_graph();
    assert_eq!(graph.channel_count(), 1);
    assert_eq!(graph.binding_count(), 2);
    let channel = graph.channels().next().expect("runtime-filter channel");
    assert_ne!(channel.channel_id.get(), 0);
    let producer = graph
        .bindings()
        .find(|binding| matches!(binding.role, RuntimeFilterBindingRole::Producer(_)))
        .expect("producer binding");
    let consumer = graph
        .bindings()
        .find(|binding| matches!(binding.role, RuntimeFilterBindingRole::Consumer(_)))
        .expect("consumer binding");
    assert_ne!(producer.binding_id.get(), 0);
    assert_ne!(consumer.binding_id.get(), 0);
    assert_ne!(producer.binding_id, consumer.binding_id);
    assert_eq!(producer.channel_id, channel.channel_id);
    assert_eq!(consumer.channel_id, channel.channel_id);
    assert!(distributed.fragments().iter().any(|fragment| {
        fn has_binding(node: &DistributedNode, binding_id: BindingId) -> bool {
            node.runtime_filter_binding_ids.contains(&binding_id)
                || node
                    .children
                    .iter()
                    .any(|child| has_binding(child, binding_id))
        }
        has_binding(&fragment.root, producer.binding_id)
    }));
    assert!(distributed.fragments().iter().any(|fragment| {
        fn has_binding(node: &DistributedNode, binding_id: BindingId) -> bool {
            node.runtime_filter_binding_ids.contains(&binding_id)
                || node
                    .children
                    .iter()
                    .any(|child| has_binding(child, binding_id))
        }
        has_binding(&fragment.root, consumer.binding_id)
    }));
}

#[test]
fn rfd_5a_graph_disambiguates_duplicate_build_expressions_by_binding_order() {
    let probe_expr_1 = column_ref_expr(1, "probe_1", DataType::Int64, false);
    let probe_expr_2 = column_ref_expr(2, "probe_2", DataType::Int64, false);
    let build_expr = column_ref_expr(3, "build", DataType::Int64, false);
    let probe_output = vec![
        output_col(1, "probe_1", DataType::Int64, false),
        output_col(2, "probe_2", DataType::Int64, false),
    ];
    let probe = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: probe_output.clone(),
        }),
        children: vec![],
        output_columns: probe_output,
        stats: stats(),
        probe_runtime_filters: vec![
            RuntimeFilterProbeIntent {
                filter_id: 41,
                probe_expr: probe_expr_1.clone(),
            },
            RuntimeFilterProbeIntent {
                filter_id: 42,
                probe_expr: probe_expr_2.clone(),
            },
        ],
    };
    let build_output = vec![output_col(3, "build", DataType::Int64, false)];
    let build = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: build_output.clone(),
        }),
        children: vec![],
        output_columns: build_output,
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![
                PhysicalHashJoinEqCondition {
                    left: probe_expr_1.clone(),
                    right: build_expr.clone(),
                    null_safe: false,
                },
                PhysicalHashJoinEqCondition {
                    left: probe_expr_2.clone(),
                    right: build_expr.clone(),
                    null_safe: false,
                },
            ],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
            execution_mode: Some(JoinExecutionMode::Broadcast),
            build_runtime_filters: vec![
                RuntimeFilterBuildIntent {
                    filter_id: 41,
                    build_expr: build_expr.clone(),
                    probe_expr: probe_expr_1,
                    expr_order: 0,
                    execution_mode: JoinExecutionMode::Broadcast,
                },
                RuntimeFilterBuildIntent {
                    filter_id: 42,
                    build_expr,
                    probe_expr: probe_expr_2,
                    expr_order: 1,
                    execution_mode: JoinExecutionMode::Broadcast,
                },
            ],
            output_columns: vec![
                output_col(1, "probe_1", DataType::Int64, false),
                output_col(2, "probe_2", DataType::Int64, false),
                output_col(3, "build", DataType::Int64, false),
            ],
        })),
        children: vec![probe, build],
        output_columns: vec![
            output_col(1, "probe_1", DataType::Int64, false),
            output_col(2, "probe_2", DataType::Int64, false),
            output_col(3, "build", DataType::Int64, false),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let distributed = build_distributed_plan(&join).expect("build duplicate-key RF graph");
    let graph = distributed.runtime_filter_graph();
    let join_node = &distributed.fragments()[0].root;
    let producers = graph
        .bindings()
        .filter(|binding| matches!(binding.role, RuntimeFilterBindingRole::Producer(_)))
        .collect::<Vec<_>>();
    let consumers = graph
        .bindings()
        .filter(|binding| matches!(binding.role, RuntimeFilterBindingRole::Consumer(_)))
        .collect::<Vec<_>>();

    assert_eq!((producers.len(), consumers.len()), (2, 2));
    assert_eq!(join_node.runtime_filter_binding_ids.len(), 2);
    let producer_key_ordinals = producers
        .iter()
        .map(|binding| match &binding.role {
            RuntimeFilterBindingRole::Producer(requirement) => match requirement.target {
                ProducerBindingTarget::JoinBuildKey { ordinal } => ordinal,
                ProducerBindingTarget::AggregateTopNKey { .. } => {
                    panic!("HashJoin planner must emit JoinBuildKey producer targets")
                }
            },
            RuntimeFilterBindingRole::Consumer(_) => unreachable!("filtered above"),
        })
        .collect::<Vec<_>>();
    assert_eq!(producer_key_ordinals, vec![0, 1]);
    assert_column_ref(&consumers[0].expression, 1, "probe_1");
    assert_column_ref(&consumers[1].expression, 2, "probe_2");
}

#[test]
fn aggregate_topn_runtime_filter_materializes_ordered_live_graph() {
    let filter_id = 71;
    let plan = aggregate_topn_runtime_filter_plan(filter_id);

    let distributed = build_distributed_plan(&plan).expect("build aggregate TopN graph");
    let graph = distributed.runtime_filter_graph();
    let channels = graph.channels().collect::<Vec<_>>();
    assert_eq!(channels.len(), 1);
    let channel = channels[0];
    assert!(matches!(
        channel.logical_domain,
        RuntimeFilterLogicalDomain::OrderedBound(_)
    ));
    assert_eq!(channel.lifecycle, RuntimeFilterLifecycle::MonotonicUpdates);
    assert_eq!(
        channel.reduction_requirement,
        ReductionRequirement::TightenOrderedBound
    );
    assert_eq!(
        channel.allowed_contribution_kinds,
        BTreeSet::from([
            ContributionKind::OrderedBoundUpdate,
            ContributionKind::ProducerClosed,
        ])
    );
    assert_eq!(
        channel.required_consumer_capabilities,
        BTreeSet::from([ArtifactCapability::OrderedRange])
    );

    let producer = graph
        .bindings()
        .find(|binding| matches!(binding.role, RuntimeFilterBindingRole::Producer(_)))
        .expect("aggregate TopN producer");
    assert!(matches!(
        producer.role,
        RuntimeFilterBindingRole::Producer(ref requirement)
            if matches!(
                requirement.target,
                ProducerBindingTarget::AggregateTopNKey {
                    group_key_ordinal: 0,
                    limit,
                } if limit == NonZeroU32::new(2).unwrap()
            )
    ));
    let consumer = graph
        .bindings()
        .find(|binding| matches!(binding.role, RuntimeFilterBindingRole::Consumer(_)))
        .expect("source-boundary consumer");
    let RuntimeFilterBindingRole::Consumer(requirement) = &consumer.role else {
        unreachable!("filtered to a consumer")
    };
    assert_eq!(requirement.target, ConsumerBindingTarget::SourceBoundary);
    assert_eq!(
        requirement.activation,
        ConsumerActivation::NonBlockingLive {
            late_apply: crate::runtime_filter::model::contract::LateApplyGranularity::Batch,
        }
    );

    let aggregate_node = find_distributed_node(distributed.fragments(), |node| {
        matches!(node.payload, DistributedNodeKind::HashAggregate(_))
    })
    .expect("aggregate node");
    let scan_node = find_distributed_node(distributed.fragments(), |node| {
        matches!(node.payload, DistributedNodeKind::Scan(_))
    })
    .expect("scan node");
    assert_eq!(
        aggregate_node.runtime_filter_binding_ids,
        vec![producer.binding_id]
    );
    assert_eq!(
        scan_node.runtime_filter_binding_ids,
        vec![consumer.binding_id]
    );
    crate::sql::planner::distributed::validation::validate_runtime_filter_graph_against_plan(
        graph,
        distributed.fragments(),
    )
    .expect("runtime-filter graph must remain attached to the plan");
}

#[test]
fn aggregate_topn_runtime_filter_keeps_ordered_bound_contract_live_in_draft() {
    let draft = super::build_distributed_plan_draft(&aggregate_topn_runtime_filter_plan(75))
        .expect("build aggregate TopN draft");
    let consumer = draft
        .runtime_filter_graph
        .bindings()
        .find(|binding| matches!(binding.role, RuntimeFilterBindingRoleData::Consumer(_)))
        .expect("draft consumer binding");
    let RuntimeFilterBindingRoleData::Consumer(requirement) = &consumer.role else {
        unreachable!("filtered to a consumer")
    };
    assert_eq!(
        requirement.activation,
        ActivationConstraint::LiveOnly {
            late_apply: crate::runtime_filter::model::contract::LateApplyGranularity::Batch,
            reason: crate::sql::planner::distributed::activation_decision::RequiredLiveReason::OrderedBoundContract,
        }
    );
}

#[test]
fn aggregate_topn_runtime_filter_rejects_missing_probe_after_intent_exists() {
    let filter_id = 72;
    let mut plan = aggregate_topn_runtime_filter_plan(filter_id);
    clear_runtime_filter_probes(&mut plan);

    let error = build_distributed_plan(&plan).expect_err("TopN intent without a probe must fail");
    assert!(error.contains(&filter_id.to_string()), "{error}");
    assert!(error.contains("probe"), "{error}");
}

#[test]
fn aggregate_topn_runtime_filter_rejects_duplicate_aggregate_owners() {
    let filter_id = 73;
    let plan = duplicate_aggregate_topn_runtime_filter_plan(filter_id);

    let error = build_distributed_plan(&plan).expect_err("duplicate TopN owners must fail");
    assert!(error.contains(&filter_id.to_string()), "{error}");
    assert!(error.contains("duplicate"), "{error}");
}

#[test]
fn aggregate_topn_runtime_filter_rejects_probe_that_cannot_exactly_resolve() {
    let filter_id = 74;
    let plan = aggregate_topn_runtime_filter_plan_with_unresolvable_project_probe(filter_id);

    let error =
        build_distributed_plan(&plan).expect_err("unresolvable TopN probe must fail the build");
    assert!(error.contains(&filter_id.to_string()), "{error}");
    assert!(error.contains("resolve"), "{error}");
}

#[test]
fn build_distributed_plan_keeps_runtime_filter_probe_on_filter() {
    let filter_id = 78;
    let probe_expr = column_ref_expr(1, "l_k", DataType::Int64, false);
    let build_expr = column_ref_expr(2, "r_k", DataType::Int64, false);
    let left_scan = scan_node(1, "l_k");
    let left_filter = PhysicalPlanNode {
        kind: PhysicalPlanKind::Filter(PlanFilterNode {
            predicate: cmp_expr(1, "l_k", BinOp::Gt, 10),
        }),
        children: vec![left_scan],
        output_columns: vec![output_col(1, "l_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![RuntimeFilterProbeIntent {
            filter_id,
            probe_expr: probe_expr.clone(),
        }],
    };
    let right = scan_node(2, "r_k");
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: probe_expr.clone(),
                right: build_expr.clone(),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
            execution_mode: Some(JoinExecutionMode::Partitioned),
            build_runtime_filters: vec![RuntimeFilterBuildIntent {
                filter_id,
                build_expr: build_expr.clone(),
                probe_expr: probe_expr.clone(),
                expr_order: 0,
                execution_mode: JoinExecutionMode::Partitioned,
            }],
            output_columns: vec![
                output_col(1, "l_k", DataType::Int64, false),
                output_col(2, "r_k", DataType::Int64, false),
            ],
        })),
        children: vec![left_filter, right],
        output_columns: vec![
            output_col(1, "l_k", DataType::Int64, false),
            output_col(2, "r_k", DataType::Int64, false),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&join).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 1);
    let join_node = &dp.fragments()[0].root;
    let graph = dp.runtime_filter_graph();
    assert_eq!(graph.channel_count(), 1);
    assert_eq!(graph.binding_count(), 2);
    assert_eq!(join_node.runtime_filter_binding_ids.len(), 1);
    let filter = &join_node.children[0];
    assert!(matches!(&filter.payload, DistributedNodeKind::Filter(_)));
    assert_eq!(filter.runtime_filter_binding_ids.len(), 1);
    let binding = graph
        .binding(filter.runtime_filter_binding_ids[0])
        .expect("filter consumer binding");
    let RuntimeFilterBindingRole::Consumer(requirement) = &binding.role else {
        panic!("filter binding must be a consumer");
    };
    assert_eq!(
        requirement.target,
        crate::runtime_filter::model::graph::ConsumerBindingTarget::DirectInput {
            input_ordinal: 0,
        }
    );
    assert_column_ref(&binding.expression, 1, "l_k");
    assert_eq!(binding.location.fragment_id.get(), join_node.fragment_id);
    let scan = &filter.children[0];
    assert!(matches!(&scan.payload, DistributedNodeKind::Scan(_)));
    assert!(scan.runtime_filter_binding_ids.is_empty());
}

#[test]
fn populate_runtime_filter_graph_deduplicates_and_skips_incomplete_channels() {
    let mut fragments = vec![
        test_fragment(distributed_values_node(1, 0)),
        test_fragment(distributed_values_node(2, 2)),
        test_fragment(distributed_values_node(3, 1)),
        test_fragment(distributed_values_node(4, 3)),
    ];
    let build_bindings = vec![test_build_binding(1, 0, 10), test_build_binding(1, 0, 11)];
    let probe_bindings = vec![
        test_probe_binding(2, 2, 10),
        test_probe_binding(2, 2, 10),
        test_probe_binding(3, 1, 10),
        test_probe_binding(4, 3, 99),
    ];

    let mut graph = DraftRuntimeFilterGraph::default();
    populate_runtime_filter_graph(
        &mut fragments,
        &mut graph,
        &RuntimeFilterBindings {
            builds: build_bindings,
            probes: probe_bindings,
            topn_builds: Vec::new(),
            node_input_columns: std::collections::BTreeMap::new(),
        },
    )
    .expect("populate graph");

    assert_eq!(graph.channel_count(), 1);
    assert_eq!(graph.binding_count(), 3);
    assert_eq!(fragments[0].root.runtime_filter_binding_ids.len(), 1);
    assert_eq!(fragments[1].root.runtime_filter_binding_ids.len(), 1);
    assert_eq!(fragments[2].root.runtime_filter_binding_ids.len(), 1);
    assert!(fragments[3].root.runtime_filter_binding_ids.is_empty());
}

#[test]
fn draft_runtime_filter_population_preserves_join_structure_without_sealed_activation() {
    let bindings = RuntimeFilterBindings {
        builds: vec![test_build_binding(1, 0, 10)],
        probes: vec![test_probe_binding(2, 1, 10)],
        topn_builds: Vec::new(),
        node_input_columns: std::collections::BTreeMap::new(),
    };
    let fragments = vec![
        test_fragment(distributed_values_node(1, 0)),
        test_fragment(distributed_values_node(2, 1)),
    ];

    let mut draft_fragments = fragments;
    let mut draft_graph = DraftRuntimeFilterGraph::default();
    populate_runtime_filter_graph(&mut draft_fragments, &mut draft_graph, &bindings)
        .expect("populate draft graph");
    draft_graph.validate().expect("draft graph validates");
    let sealed_fragments = draft_fragments.clone();
    let sealed_graph = draft_graph
        .clone()
        .map_consumer_activations(|_, _, _, _| {
            Ok::<_, std::convert::Infallible>(ConsumerActivation::BlockingSnapshot)
        })
        .expect("infallible test-only materialization");
    sealed_graph
        .validate()
        .expect("materialized graph validates");

    assert_eq!(draft_graph.channel_count(), sealed_graph.channel_count());
    assert_eq!(draft_graph.binding_count(), sealed_graph.binding_count());
    assert_eq!(
        activation_neutral_graph_structure(&draft_graph),
        activation_neutral_graph_structure(&sealed_graph),
        "Draft and sealed population must differ only in consumer activation"
    );
    // This is deliberately a one-field-at-a-time mutation list. Consumer activation is the
    // only intentionally excluded field from `activation_neutral_graph_structure`.
    assert_snapshot_detects_channel_mutation(&sealed_graph, "channel_id", |channel| {
        channel.channel_id = ChannelId::new(99);
    });
    assert_snapshot_detects_channel_mutation(&sealed_graph, "logical_domain", |channel| {
        channel.logical_domain = match &channel.logical_domain {
            RuntimeFilterLogicalDomain::Membership {
                value_type,
                null_semantics,
            } => RuntimeFilterLogicalDomain::Membership {
                value_type: value_type.clone(),
                null_semantics: match null_semantics {
                    NullSemantics::NeverMatches => NullSemantics::NullSafeEqual,
                    NullSemantics::NullSafeEqual => NullSemantics::NeverMatches,
                },
            },
            RuntimeFilterLogicalDomain::OrderedBound(_) => RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Utf8,
                null_semantics: NullSemantics::NeverMatches,
            },
        };
    });
    assert_snapshot_detects_channel_mutation(&sealed_graph, "lifecycle", |channel| {
        channel.lifecycle = match channel.lifecycle {
            RuntimeFilterLifecycle::CompleteOnce => RuntimeFilterLifecycle::MonotonicUpdates,
            RuntimeFilterLifecycle::MonotonicUpdates => RuntimeFilterLifecycle::CompleteOnce,
        };
    });
    assert_snapshot_detects_channel_mutation(&sealed_graph, "availability_coverage", |channel| {
        channel.availability_coverage = Coverage::AnyOf(vec![
            channel.availability_coverage.clone(),
            Coverage::Leaf(CoverageWitnessId::new(99)),
        ]);
    });
    assert_snapshot_detects_channel_mutation(&sealed_graph, "terminal_coverage", |channel| {
        channel.terminal_coverage = Coverage::AllOf(vec![
            channel.terminal_coverage.clone(),
            Coverage::Leaf(CoverageWitnessId::new(98)),
        ]);
    });
    assert_snapshot_detects_channel_mutation(&sealed_graph, "reduction_requirement", |channel| {
        channel.reduction_requirement = match channel.reduction_requirement {
            ReductionRequirement::SetUnion => ReductionRequirement::TightenOrderedBound,
            ReductionRequirement::TightenOrderedBound
            | ReductionRequirement::MergeTopKSummary(_) => ReductionRequirement::SetUnion,
        };
    });
    assert_snapshot_detects_channel_mutation(
        &sealed_graph,
        "allowed_contribution_kinds",
        |channel| {
            if !channel
                .allowed_contribution_kinds
                .insert(ContributionKind::TopKSummary)
            {
                channel
                    .allowed_contribution_kinds
                    .remove(&ContributionKind::TopKSummary);
            }
        },
    );
    assert_snapshot_detects_channel_mutation(
        &sealed_graph,
        "required_consumer_capabilities",
        |channel| {
            if !channel
                .required_consumer_capabilities
                .insert(ArtifactCapability::OrderedRange)
            {
                channel
                    .required_consumer_capabilities
                    .remove(&ArtifactCapability::OrderedRange);
            }
        },
    );
    assert_snapshot_detects_channel_mutation(&sealed_graph, "policy", |channel| {
        channel.policy.deadline_ms = if channel.policy.deadline_ms == 0 {
            1
        } else {
            0
        };
    });

    assert_snapshot_detects_binding_mutation(&sealed_graph, "binding_id", |binding| {
        if matches!(&binding.role, RuntimeFilterBindingRole::Producer(_)) {
            binding.binding_id = BindingId::new(99);
        }
    });
    assert_snapshot_detects_binding_mutation(&sealed_graph, "channel_id", |binding| {
        if matches!(&binding.role, RuntimeFilterBindingRole::Producer(_)) {
            binding.channel_id = ChannelId::new(99);
        }
    });
    assert_snapshot_detects_binding_mutation(&sealed_graph, "coverage_witness_id", |binding| {
        if matches!(&binding.role, RuntimeFilterBindingRole::Producer(_)) {
            binding.coverage_witness_id = Some(CoverageWitnessId::new(99));
        }
    });
    assert_snapshot_detects_binding_mutation(&sealed_graph, "location", |binding| {
        if matches!(&binding.role, RuntimeFilterBindingRole::Producer(_)) {
            binding.location.node_id = crate::runtime_filter::model::contract::PlanNodeId::new(99);
        }
    });
    assert_snapshot_detects_binding_mutation(&sealed_graph, "expression", |binding| {
        if matches!(&binding.role, RuntimeFilterBindingRole::Producer(_)) {
            binding.expression = column_ref_expr(99, "changed", DataType::Int64, false);
        }
    });
    assert_snapshot_detects_binding_mutation(&sealed_graph, "apply_point", |binding| {
        if matches!(&binding.role, RuntimeFilterBindingRole::Producer(_)) {
            binding.apply_point = crate::runtime_filter::model::graph::ApplyPoint::NodeInput;
        }
    });
    assert_snapshot_detects_binding_mutation(
        &sealed_graph,
        "producer.contribution_kinds",
        |binding| {
            if let RuntimeFilterBindingRole::Producer(producer) = &mut binding.role {
                if !producer
                    .contribution_kinds
                    .insert(ContributionKind::FinalDomainShard)
                {
                    producer
                        .contribution_kinds
                        .remove(&ContributionKind::FinalDomainShard);
                }
            }
        },
    );
    assert_snapshot_detects_binding_mutation(
        &sealed_graph,
        "producer.completion_requirement",
        |binding| {
            if let RuntimeFilterBindingRole::Producer(producer) = &mut binding.role {
                producer.completion_requirement = match producer.completion_requirement {
                    CompletionRequirement::ProducerClosed => {
                        CompletionRequirement::FencedFinalDomain(
                            CompletionFenceKind::CommittedDomainFrozen,
                        )
                    }
                    CompletionRequirement::FencedFinalDomain(_) => {
                        CompletionRequirement::ProducerClosed
                    }
                };
            }
        },
    );
    assert_snapshot_detects_binding_mutation(&sealed_graph, "producer.target", |binding| {
        if let RuntimeFilterBindingRole::Producer(producer) = &mut binding.role {
            producer.target = ProducerBindingTarget::JoinBuildKey { ordinal: 99 };
        }
    });

    let consumer_target = sealed_graph
        .bindings()
        .find_map(|binding| match &binding.role {
            RuntimeFilterBindingRole::Consumer(consumer) => Some(consumer.target),
            RuntimeFilterBindingRole::Producer(_) => None,
        })
        .expect("sealed consumer target");
    let changed_consumer_target = ConsumerBindingTarget::DirectInput { input_ordinal: 99 };
    assert_ne!(
        consumer_target, changed_consumer_target,
        "the consumer target mutation must not be a no-op"
    );
    assert_snapshot_detects_binding_mutation(&sealed_graph, "consumer.capabilities", |binding| {
        if let RuntimeFilterBindingRole::Consumer(consumer) = &mut binding.role {
            if !consumer
                .capabilities
                .insert(ArtifactCapability::OrderedRange)
            {
                consumer
                    .capabilities
                    .remove(&ArtifactCapability::OrderedRange);
            }
        }
    });
    assert_snapshot_detects_binding_mutation(&sealed_graph, "consumer.target", |binding| {
        if let RuntimeFilterBindingRole::Consumer(consumer) = &mut binding.role {
            consumer.target = changed_consumer_target;
        }
    });
    assert_eq!(
        draft_fragments
            .iter()
            .map(|fragment| fragment.root.runtime_filter_binding_ids.clone())
            .collect::<Vec<_>>(),
        sealed_fragments
            .iter()
            .map(|fragment| fragment.root.runtime_filter_binding_ids.clone())
            .collect::<Vec<_>>()
    );
    let draft_consumer = draft_graph
        .bindings()
        .find_map(|binding| match &binding.role {
            crate::runtime_filter::model::graph::RuntimeFilterBindingRoleData::Consumer(
                consumer,
            ) => Some(consumer),
            crate::runtime_filter::model::graph::RuntimeFilterBindingRoleData::Producer(_) => None,
        })
        .expect("draft consumer");
    assert_eq!(
        draft_consumer.activation,
        ActivationConstraint::BlockingOrBatchLive {
            fallback: ActivationFallback::BlockingSnapshot,
        }
    );
    let sealed_consumer = sealed_graph
        .bindings()
        .find_map(|binding| match &binding.role {
            RuntimeFilterBindingRole::Consumer(consumer) => Some(consumer),
            RuntimeFilterBindingRole::Producer(_) => None,
        })
        .expect("sealed consumer");
    assert_eq!(
        sealed_consumer.activation,
        ConsumerActivation::BlockingSnapshot
    );
}

#[test]
fn build_distributed_plan_nest_loop_join_combines_child_tuples() {
    let left = scan_node(1, "l_k");
    let right = scan_node(2, "r_k");
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::NestLoopJoin(PhysicalNestLoopJoinNode {
            join_type: JoinKind::Inner,
            condition: None,
            output_columns: vec![
                output_col(1, "l_k", DataType::Int64, false),
                output_col(2, "r_k", DataType::Int64, false),
            ],
        }),
        children: vec![left, right],
        output_columns: vec![
            output_col(1, "l_k", DataType::Int64, false),
            output_col(2, "r_k", DataType::Int64, false),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&join).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(
        &root.payload,
        DistributedNodeKind::NestLoopJoin(_)
    ));
    assert_eq!(root.node_id, 3);
    assert_eq!(root.tuple_ids, vec![1, 2]);
    assert_eq!(root.children[0].tuple_ids, vec![1]);
    assert_eq!(root.children[1].tuple_ids, vec![2]);
}

#[test]
fn build_distributed_plan_assert_one_row_reuses_child_tuple() {
    let scan = scan_node(1, "k");
    let assert_one_row = PhysicalPlanNode {
        kind: PhysicalPlanKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one(
            "select k from t",
        )),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&assert_one_row).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(
        &root.payload,
        DistributedNodeKind::AssertOneRow(_)
    ));
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![1]);
    assert_eq!(root.children[0].node_id, 1);
}

#[test]
fn build_distributed_plan_change_event_expand_allocates_new_tuple() {
    let scan = scan_node(1, "k");
    let expand = PhysicalPlanNode {
        kind: PhysicalPlanKind::ChangeEventExpand(DistributedChangeEventExpandNode {
            events: vec![],
            output_columns: vec![
                output_col(2, "payload", DataType::Int64, false),
                output_col(3, "change_op", DataType::Int64, false),
            ],
            change_op_column_id: ColumnId::new_for_test(3),
            data_route_column_id: None,
        }),
        children: vec![scan],
        output_columns: vec![
            output_col(2, "payload", DataType::Int64, false),
            output_col(3, "change_op", DataType::Int64, false),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&expand).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(
        &root.payload,
        DistributedNodeKind::ChangeEventExpand(_)
    ));
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![2]);
    assert_eq!(root.children[0].tuple_ids, vec![1]);
}

#[test]
fn build_distributed_plan_repeat_appends_virtual_tuple_only_when_grouping_fn_args_present() {
    let scan = scan_node(1, "k");
    let repeat = PhysicalPlanNode {
        kind: PhysicalPlanKind::Repeat(repeat_node(false)),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&repeat).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    let repeat = match &root.payload {
        DistributedNodeKind::Repeat(repeat) => repeat,
        other => panic!("expected Repeat root, got {other:?}"),
    };
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![1]);
    assert_eq!(repeat.virtual_tuple_id, Some(2));

    let scan = scan_node(1, "k");
    let repeat = PhysicalPlanNode {
        kind: PhysicalPlanKind::Repeat(repeat_node(true)),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&repeat).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    let repeat = match &root.payload {
        DistributedNodeKind::Repeat(repeat) => repeat,
        other => panic!("expected Repeat root, got {other:?}"),
    };
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![1, 2]);
    assert_eq!(repeat.virtual_tuple_id, Some(2));
}

#[test]
fn build_distributed_plan_generate_series_replicates_dummy_allocations() {
    let output_columns = vec![output_col(1, "x", DataType::Int64, false)];
    let generate_series = PhysicalPlanNode {
        kind: PhysicalPlanKind::GenerateSeries(PlanGenerateSeriesNode {
            start: 1,
            end: 3,
            step: 1,
            column_name: "x".to_string(),
            alias: None,
            output_column_id: ColumnId::new_for_test(1),
        }),
        children: vec![],
        output_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&generate_series).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(
        &root.payload,
        DistributedNodeKind::GenerateSeries(_)
    ));
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![2]);
    assert!(root.children.is_empty());
}

#[test]
fn build_distributed_plan_table_function_replicates_dummy_allocations() {
    let scan = scan_node(1, "k");
    let output_columns = vec![output_col(2, "item", DataType::Int64, false)];
    let table_function = PhysicalPlanNode {
        kind: PhysicalPlanKind::TableFunction(PlanTableFunctionNode {
            function_name: "unnest".to_string(),
            args: vec![],
            output_columns: output_columns.clone(),
            alias: None,
            is_left_join: false,
        }),
        children: vec![scan],
        output_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&table_function).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(
        &root.payload,
        DistributedNodeKind::TableFunction(_)
    ));
    assert_eq!(root.node_id, 3);
    assert_eq!(root.tuple_ids, vec![3]);
    assert_eq!(root.children.len(), 1);
    assert_eq!(root.children[0].node_id, 1);
    assert_eq!(root.children[0].tuple_ids, vec![1]);
}

#[test]
fn build_distributed_plan_window_single_group_allocates_analytic_ids() {
    let scan = scan_node(1, "k");
    let rn = output_col(2, "rn", DataType::Int64, false);
    let output_columns = vec![output_col(1, "k", DataType::Int64, false), rn.clone()];
    let window = PhysicalPlanNode {
        kind: PhysicalPlanKind::Window(PlanWindowNode {
            window_exprs: vec![window_expr(rn, vec![], vec![])],
            output_columns: output_columns.clone(),
        }),
        children: vec![scan],
        output_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&window).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(&root.payload, DistributedNodeKind::Window(_)));
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![1, 3]);
    assert_eq!(root.children.len(), 1);
    assert_eq!(root.children[0].node_id, 1);
    assert_eq!(root.children[0].tuple_ids, vec![1]);
}

#[test]
fn build_distributed_plan_window_multi_group_allocates_sort_when_ordering_changes() {
    let scan = scan_node_with_columns(vec![
        output_col(1, "k", DataType::Int64, false),
        output_col(2, "v", DataType::Int64, true),
    ]);
    let rn_by_k = output_col(3, "rn_by_k", DataType::Int64, false);
    let rn_by_v = output_col(4, "rn_by_v", DataType::Int64, false);
    let output_columns = vec![
        output_col(1, "k", DataType::Int64, false),
        output_col(2, "v", DataType::Int64, true),
        rn_by_k.clone(),
        rn_by_v.clone(),
    ];
    let window = PhysicalPlanNode {
        kind: PhysicalPlanKind::Window(PlanWindowNode {
            window_exprs: vec![
                window_expr(
                    rn_by_k,
                    vec![],
                    vec![sort_item(column_ref_expr(1, "k", DataType::Int64, false))],
                ),
                window_expr(
                    rn_by_v,
                    vec![],
                    vec![sort_item(column_ref_expr(2, "v", DataType::Int64, true))],
                ),
            ],
            output_columns: output_columns.clone(),
        }),
        children: vec![scan],
        output_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let project_columns = vec![output_col(5, "rn_alias", DataType::Int64, false)];
    let project = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: column_ref_expr(4, "rn_by_v", DataType::Int64, false),
                output_name: "rn_alias".to_string(),
                output_column_id: ColumnId::new_for_test(5),
            }],
            output_qualifier: None,
        }),
        children: vec![window],
        output_columns: project_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&project).expect("build_distributed_plan");

    let root = &dp.fragments()[0].root;
    assert!(matches!(&root.payload, DistributedNodeKind::Project(_)));
    assert_eq!(root.node_id, 6);
    assert_eq!(root.tuple_ids, vec![6]);
    assert_eq!(root.children.len(), 1);
    let window = &root.children[0];
    assert!(matches!(&window.payload, DistributedNodeKind::Window(_)));
    assert_eq!(window.node_id, 2);
    assert_eq!(window.tuple_ids, vec![1, 3, 5]);
    assert_eq!(window.children.len(), 1);
    assert_eq!(window.children[0].node_id, 1);
    assert_eq!(window.children[0].tuple_ids, vec![1]);
}

#[test]
fn build_distributed_plan_window_rejects_empty_window_exprs() {
    let scan = scan_node(1, "k");
    let window = PhysicalPlanNode {
        kind: PhysicalPlanKind::Window(PlanWindowNode {
            window_exprs: vec![],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err = build_distributed_plan(&window).expect_err("empty Window expressions are invalid");

    assert!(
        err.contains("PhysicalWindow has no window expressions"),
        "unexpected error: {err}"
    );
}

#[test]
fn build_distributed_plan_hash_redistribute_creates_exchange_edge() {
    let scan = scan_node(1, "k");
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Hash {
                cols: vec![ColumnId::new_for_test(1)],
                source: HashSource::ShuffleJoin,
            },
            partition_exprs: vec![column_ref_expr(1, "qualified_k", DataType::Int64, false)],
            output_columns: scan.output_columns.clone(),
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let project = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: column_ref_expr(1, "k", DataType::Int64, false),
                output_name: "k_alias".to_string(),
                output_column_id: ColumnId::new_for_test(2),
            }],
            output_qualifier: None,
        }),
        children: vec![redistribute],
        output_columns: vec![output_col(2, "k_alias", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&project).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 2);
    assert_eq!(dp.root_fragment_id(), 0);
    assert_eq!(dp.fragments()[0].fragment_id, 1);
    assert_eq!(dp.fragments()[1].fragment_id, 0);
    assert_eq!(dp.edges().len(), 1);

    let root = &dp.fragments()[1].root;
    assert!(matches!(&root.payload, DistributedNodeKind::Project(_)));
    assert_eq!(root.fragment_id, 0);
    assert_eq!(root.children.len(), 1);

    let exchange = &root.children[0];
    let exchange_receiver = match &exchange.payload {
        DistributedNodeKind::Exchange(exchange_receiver) => exchange_receiver,
        other => panic!("expected Exchange child, got {other:?}"),
    };
    assert_eq!(exchange.fragment_id, 0);
    assert_eq!(exchange_receiver.source_fragment_id, 1);
    assert!(matches!(
        exchange_receiver.partition.kind,
        PartitionKind::Hash
    ));
    assert_eq!(exchange_receiver.partition.exprs.len(), 1);
    assert_column_ref(&exchange_receiver.partition.exprs[0], 1, "qualified_k");
    assert!(matches!(
        exchange_receiver.flavor,
        crate::sql::planner::distributed::ExchangeFlavor::Distribution
    ));
    assert_eq!(exchange_receiver.output_columns.len(), 1);
    assert_eq!(
        exchange_receiver.output_columns[0].column_id,
        ColumnId::new_for_test(1)
    );
    assert_eq!(exchange_receiver.output_columns[0].name, "k");

    let edge = &dp.edges()[0];
    assert_eq!(edge.source_fragment_id, 1);
    assert_eq!(edge.target_fragment_id, 0);
    assert_eq!(edge.target_exchange_node_id, exchange.node_id);
    assert_eq!(edge.stream_kind, FragmentStreamKind::Partitioned);
    assert!(matches!(edge.edge_kind, FragmentEdgeKind::Stream));
    assert_eq!(edge.output_slot_ids, vec![1]);

    let child_fragment = &dp.fragments()[0];
    assert_ne!(exchange.tuple_ids, child_fragment.root.tuple_ids);
    assert_eq!(exchange.tuple_ids.len(), 1);
    assert!(matches!(child_fragment.sink, DataSink::Noop));
    assert!(matches!(
        child_fragment.output_partition.kind,
        PartitionKind::Hash
    ));
    assert_eq!(
        child_fragment.output_partition.explain_label(),
        "HASH_PARTITIONED (t.qualified_k)"
    );
    assert_eq!(
        child_fragment.output_columns[0].column_id,
        ColumnId::new_for_test(1)
    );
    assert!(matches!(edge.output_partition.kind, PartitionKind::Hash));
    assert_eq!(edge.output_partition.exprs.len(), 1);
    assert!(matches!(
        &child_fragment.root.payload,
        DistributedNodeKind::Scan(_)
    ));
}

#[test]
fn fragment_cut_seam_preserves_exchange_topology_before_rf_binding() {
    let filter_id = 91;
    let probe_expr = column_ref_expr(1, "l_k", DataType::Int64, false);
    let build_expr = column_ref_expr(2, "r_k", DataType::Int64, false);
    let mut left_scan = scan_node(1, "l_k");
    left_scan.probe_runtime_filters = vec![RuntimeFilterProbeIntent {
        filter_id,
        probe_expr: probe_expr.clone(),
    }];
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Hash {
                cols: vec![ColumnId::new_for_test(1)],
                source: HashSource::ShuffleJoin,
            },
            partition_exprs: vec![probe_expr.clone()],
            output_columns: left_scan.output_columns.clone(),
        }),
        children: vec![left_scan],
        output_columns: vec![output_col(1, "l_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let right_scan = scan_node(2, "r_k");
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: probe_expr.clone(),
                right: build_expr.clone(),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
            execution_mode: Some(JoinExecutionMode::Partitioned),
            build_runtime_filters: vec![RuntimeFilterBuildIntent {
                filter_id,
                build_expr,
                probe_expr,
                expr_order: 0,
                execution_mode: JoinExecutionMode::Partitioned,
            }],
            output_columns: vec![
                output_col(1, "l_k", DataType::Int64, false),
                output_col(2, "r_k", DataType::Int64, false),
            ],
        })),
        children: vec![redistribute, right_scan],
        output_columns: vec![
            output_col(1, "l_k", DataType::Int64, false),
            output_col(2, "r_k", DataType::Int64, false),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let cut = cut(&join).expect("cut HashJoin and Redistribute topology");

    assert_eq!(cut.plan.root_fragment_id, Some(0));
    assert_eq!(cut.plan.fragments.len(), 2);
    assert_eq!(cut.plan.fragments[0].fragment_id, 1);
    assert_eq!(cut.plan.fragments[1].fragment_id, 0);
    assert_eq!(cut.plan.edges.len(), 1);
    let edge = &cut.plan.edges[0];
    assert_eq!(edge.source_fragment_id, 1);
    assert_eq!(edge.target_fragment_id, 0);
    assert_eq!(edge.target_exchange_node_id, 2);
    assert_eq!(edge.stream_kind, FragmentStreamKind::Partitioned);
    let root = &cut.plan.fragments[1].root;
    assert!(matches!(root.payload, DistributedNodeKind::HashJoin(_)));
    assert!(matches!(
        root.children[0].payload,
        DistributedNodeKind::Exchange(_)
    ));

    assert_eq!(cut.bindings.builds.len(), 1);
    assert_eq!(cut.bindings.builds[0].intent.filter_id, filter_id);
    assert_eq!(cut.bindings.builds[0].fragment_id, 0);
    assert_eq!(cut.bindings.probes.len(), 1);
    assert_eq!(cut.bindings.probes[0].intent.filter_id, filter_id);
    assert_eq!(cut.bindings.probes[0].fragment_id, 1);

    fn assert_runtime_filters_unbound(node: &DistributedNode) {
        assert!(node.runtime_filter_binding_ids.is_empty());
        for child in &node.children {
            assert_runtime_filters_unbound(child);
        }
    }
    for fragment in &cut.plan.fragments {
        assert_runtime_filters_unbound(&fragment.root);
    }
}

#[test]
fn build_distributed_plan_stream_edge_carries_exchange_output_slot_order() {
    let source_columns = vec![
        output_col(1, "old", DataType::Int64, false),
        output_col(2, "delta", DataType::Int64, false),
    ];
    let exchange_columns = vec![source_columns[1].clone(), source_columns[0].clone()];
    let scan = scan_node_with_columns(source_columns.clone());
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Hash {
                cols: vec![ColumnId::new_for_test(2)],
                source: HashSource::ShuffleJoin,
            },
            partition_exprs: vec![],
            output_columns: exchange_columns.clone(),
        }),
        children: vec![scan],
        output_columns: source_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

    let edge = &dp.edges()[0];
    assert_eq!(edge.output_slot_ids, vec![2, 1]);

    let root = &dp.fragments()[1].root;
    let receiver = match &root.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected Exchange root, got {other:?}"),
    };
    assert_eq!(
        receiver
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![ColumnId::new_for_test(2), ColumnId::new_for_test(1)]
    );

    let child_fragment = &dp.fragments()[0];
    assert_eq!(
        child_fragment
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![ColumnId::new_for_test(1), ColumnId::new_for_test(2)]
    );
}

#[test]
fn build_distributed_plan_redistribute_drops_non_child_output_columns() {
    let source_columns = vec![
        output_col(1, "old", DataType::Int64, false),
        output_col(2, "delta", DataType::Int64, false),
    ];
    let predicate_only = output_col(3, "predicate_only", DataType::Int64, false);
    let exchange_columns = vec![
        source_columns[1].clone(),
        predicate_only,
        source_columns[0].clone(),
    ];
    let mut scan = scan_node_with_columns(source_columns.clone());
    scan.output_columns = exchange_columns.clone();
    let filter = PhysicalPlanNode {
        kind: PhysicalPlanKind::Filter(PlanFilterNode {
            predicate: column_ref_expr(3, "predicate_only", DataType::Int64, false),
        }),
        children: vec![scan],
        output_columns: exchange_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Broadcast,
            partition_exprs: vec![],
            output_columns: exchange_columns.clone(),
        }),
        children: vec![filter],
        output_columns: source_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

    let edge = &dp.edges()[0];
    assert_eq!(edge.output_slot_ids, vec![2, 1]);

    let root = &dp.fragments()[1].root;
    let receiver = match &root.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected Exchange root, got {other:?}"),
    };
    assert_eq!(
        receiver
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![ColumnId::new_for_test(2), ColumnId::new_for_test(1)]
    );

    let child_fragment = &dp.fragments()[0];
    assert_eq!(
        child_fragment
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![ColumnId::new_for_test(1), ColumnId::new_for_test(2)]
    );
}

#[test]
fn build_distributed_plan_stream_edge_drops_scan_columns_pruned_from_required() {
    let source_columns = vec![
        output_col(1, "c0", DataType::Int64, false),
        output_col(2, "c1", DataType::Utf8, true),
        output_col(3, "c2", DataType::Utf8, true),
        output_col(4, "c3", DataType::Int64, true),
    ];
    let mut scan = scan_node_with_columns(source_columns.clone());
    if let PhysicalPlanKind::Scan(scan) = &mut scan.kind {
        scan.required_columns = Some(vec!["c0".to_string(), "c1".to_string(), "c3".to_string()]);
    }
    let materialized_scan_columns = vec![
        source_columns[0].clone(),
        source_columns[1].clone(),
        source_columns[3].clone(),
    ];
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Broadcast,
            partition_exprs: vec![],
            output_columns: source_columns.clone(),
        }),
        children: vec![scan],
        output_columns: materialized_scan_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

    let edge = &dp.edges()[0];
    assert_eq!(edge.output_slot_ids, vec![1, 2, 4]);

    let child_fragment = &dp.fragments()[0];
    assert_eq!(
        child_fragment
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![
            ColumnId::new_for_test(1),
            ColumnId::new_for_test(2),
            ColumnId::new_for_test(4)
        ]
    );
}

#[test]
fn build_distributed_plan_stream_edge_drops_join_columns_pruned_from_child_source() {
    let left_columns = vec![
        output_col(1, "l_c0", DataType::Int64, false),
        output_col(2, "l_c1", DataType::Utf8, true),
        output_col(3, "l_c2", DataType::Utf8, true),
    ];
    let right_columns = vec![output_col(4, "r_c0", DataType::Int64, false)];
    let mut left = scan_node_with_columns(left_columns.clone());
    if let PhysicalPlanKind::Scan(scan) = &mut left.kind {
        scan.required_columns = Some(vec!["l_c0".to_string(), "l_c1".to_string()]);
    }
    let right = scan_node_with_columns(right_columns.clone());
    let join_output_columns = vec![
        left_columns[0].clone(),
        left_columns[2].clone(),
        right_columns[0].clone(),
    ];
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
            execution_mode: None,
            build_runtime_filters: vec![],
            output_columns: join_output_columns.clone(),
        })),
        children: vec![left, right],
        output_columns: join_output_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Broadcast,
            partition_exprs: vec![],
            output_columns: join_output_columns,
        }),
        children: vec![join],
        output_columns: vec![left_columns[0].clone(), right_columns[0].clone()],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

    assert_eq!(dp.edges()[0].output_slot_ids, vec![1, 4]);
    let receiver = match &dp.fragments()[1].root.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected Exchange root, got {other:?}"),
    };
    assert_eq!(
        receiver
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![ColumnId::new_for_test(1), ColumnId::new_for_test(4)]
    );
    assert_eq!(
        dp.fragments()[0]
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![ColumnId::new_for_test(1), ColumnId::new_for_test(4)]
    );
}

#[test]
fn build_distributed_plan_stream_edge_drops_redistribute_child_columns_pruned_from_source() {
    let left_columns = vec![
        output_col(1, "l_c0", DataType::Int64, false),
        output_col(2, "l_c1", DataType::Utf8, true),
    ];
    let right_columns = vec![
        output_col(9, "r_c0", DataType::Int64, false),
        output_col(10, "r_c1", DataType::Utf8, true),
        output_col(11, "r_c2", DataType::Utf8, true),
        output_col(12, "r_c3", DataType::Int64, true),
    ];
    let left = scan_node_with_columns(left_columns.clone());
    let mut right = scan_node_with_columns(right_columns.clone());
    if let PhysicalPlanKind::Scan(scan) = &mut right.kind {
        scan.required_columns = Some(vec![
            "r_c0".to_string(),
            "r_c1".to_string(),
            "r_c3".to_string(),
        ]);
    }
    let right_redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Gather,
            partition_exprs: vec![],
            output_columns: right_columns.clone(),
        }),
        children: vec![right],
        output_columns: vec![
            right_columns[0].clone(),
            right_columns[1].clone(),
            right_columns[3].clone(),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let join_output_columns = vec![
        left_columns[0].clone(),
        right_columns[1].clone(),
        right_columns[2].clone(),
    ];
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
            execution_mode: None,
            build_runtime_filters: vec![],
            output_columns: join_output_columns.clone(),
        })),
        children: vec![left, right_redistribute],
        output_columns: join_output_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Broadcast,
            partition_exprs: vec![],
            output_columns: join_output_columns,
        }),
        children: vec![join],
        output_columns: vec![left_columns[0].clone(), right_columns[1].clone()],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

    assert_eq!(dp.edges().len(), 2);
    assert_eq!(dp.edges()[0].output_slot_ids, vec![9, 10, 12]);
    assert_eq!(dp.edges()[1].output_slot_ids, vec![1, 10]);
    let receiver = match &dp.fragments()[2].root.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected Exchange root, got {other:?}"),
    };
    assert_eq!(
        receiver
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![ColumnId::new_for_test(1), ColumnId::new_for_test(10)]
    );
}

#[test]
fn build_distributed_plan_stream_edge_uses_project_item_outputs() {
    let scan_columns = vec![
        output_col(1, "c0", DataType::Int64, false),
        output_col(2, "c1", DataType::Utf8, true),
        output_col(3, "c2", DataType::Utf8, true),
    ];
    let scan = scan_node_with_columns(scan_columns.clone());
    let project_output_columns = vec![scan_columns[0].clone(), scan_columns[2].clone()];
    let stale_node_output_columns = vec![
        scan_columns[0].clone(),
        scan_columns[1].clone(),
        scan_columns[2].clone(),
    ];
    let project = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![
                ProjectItem {
                    expr: column_ref_expr(1, "c0", DataType::Int64, false),
                    output_name: "c0".to_string(),
                    output_column_id: ColumnId::new_for_test(1),
                },
                ProjectItem {
                    expr: column_ref_expr(3, "c2", DataType::Utf8, true),
                    output_name: "c2".to_string(),
                    output_column_id: ColumnId::new_for_test(3),
                },
            ],
            output_qualifier: None,
        }),
        children: vec![scan],
        output_columns: project_output_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Broadcast,
            partition_exprs: vec![],
            output_columns: stale_node_output_columns,
        }),
        children: vec![project],
        output_columns: project_output_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

    assert_eq!(dp.edges()[0].output_slot_ids, vec![1, 3]);
    let receiver = match &dp.fragments()[1].root.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected Exchange root, got {other:?}"),
    };
    assert_eq!(
        receiver
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![ColumnId::new_for_test(1), ColumnId::new_for_test(3)]
    );
    assert_eq!(
        dp.fragments()[0]
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        project_output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>()
    );
}

#[test]
fn build_distributed_plan_local_aggregate_stream_uses_layout_output_types() {
    let input_columns = vec![output_col(1, "v", DataType::Int64, true)];
    let scan = scan_node_with_columns(input_columns);
    let final_avg = output_col(20, "avg(v)", DataType::Float64, true);
    let local_avg_state = output_col(20, "avg(v)", DataType::Utf8, true);
    let aggregate = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Local,
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: "avg".to_string(),
                args: vec![column_ref_expr(1, "v", DataType::Int64, true)],
                distinct: false,
                result_type: DataType::Float64,
                order_by: vec![],
                output_column_id: ColumnId::new_for_test(20),
            }],
            is_merge: vec![false],
            output_layout: AggregateOutputLayout::new(vec![], vec![local_avg_state.clone()]),
            output_columns: vec![final_avg.clone()],
            topn_runtime_filter_builds: Vec::new(),
        })),
        children: vec![scan],
        output_columns: vec![final_avg.clone()],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Hash {
                cols: vec![ColumnId::new_for_test(20)],
                source: HashSource::ShuffleAgg,
            },
            partition_exprs: vec![column_ref_expr(20, "avg(v)", DataType::Utf8, true)],
            output_columns: vec![final_avg],
        }),
        children: vec![aggregate],
        output_columns: vec![local_avg_state.clone()],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

    assert_eq!(dp.edges()[0].output_slot_ids, vec![20]);
    let receiver = match &dp.fragments()[1].root.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected Exchange root, got {other:?}"),
    };
    assert_eq!(receiver.output_columns.len(), 1);
    assert_eq!(
        receiver.output_columns[0].column_id,
        local_avg_state.column_id
    );
    assert_eq!(receiver.output_columns[0].data_type, DataType::Utf8);
    assert_eq!(dp.fragments()[0].output_columns.len(), 1);
    assert_eq!(
        dp.fragments()[0].output_columns[0].column_id,
        local_avg_state.column_id
    );
    assert_eq!(
        dp.fragments()[0].output_columns[0].data_type,
        DataType::Utf8
    );
}

#[test]
fn build_distributed_plan_hash_redistribute_rejects_missing_partition_column() {
    let scan = scan_node(1, "k");
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Hash {
                cols: vec![ColumnId::new_for_test(1), ColumnId::new_for_test(99)],
                source: HashSource::ShuffleJoin,
            },
            partition_exprs: vec![],
            output_columns: scan.output_columns.clone(),
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err =
        build_distributed_plan(&redistribute).expect_err("missing hash column should be rejected");

    assert!(
        err.contains("missing hash partition columns"),
        "unexpected error: {err}"
    );
    assert!(err.contains("c99"), "unexpected error: {err}");
    assert!(err.contains("available"), "unexpected error: {err}");
}

#[test]
fn build_distributed_plan_broadcast_redistribute_creates_broadcast_edge() {
    let scan = scan_node(1, "k");
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Broadcast,
            partition_exprs: vec![],
            output_columns: scan.output_columns.clone(),
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let project = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: column_ref_expr(1, "k", DataType::Int64, false),
                output_name: "k_alias".to_string(),
                output_column_id: ColumnId::new_for_test(2),
            }],
            output_qualifier: None,
        }),
        children: vec![redistribute],
        output_columns: vec![output_col(2, "k_alias", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&project).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 2);
    assert_eq!(dp.edges().len(), 1);
    let root = &dp.fragments()[1].root;
    let exchange = &root.children[0];
    let exchange_receiver = match &exchange.payload {
        DistributedNodeKind::Exchange(exchange_receiver) => exchange_receiver,
        other => panic!("expected Exchange child, got {other:?}"),
    };
    assert!(matches!(
        exchange_receiver.partition.kind,
        PartitionKind::Unpartitioned
    ));
    assert!(exchange_receiver.partition.exprs.is_empty());
    assert_eq!(dp.edges()[0].stream_kind, FragmentStreamKind::Broadcast);
    assert!(matches!(dp.edges()[0].edge_kind, FragmentEdgeKind::Stream));
    assert!(matches!(
        dp.edges()[0].output_partition.kind,
        PartitionKind::Unpartitioned
    ));
}

#[test]
fn build_distributed_plan_root_gather_is_skipped() {
    let scan = scan_node(1, "k");
    let redistribute = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Gather,
            partition_exprs: vec![],
            output_columns: scan.output_columns.clone(),
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 1);
    assert!(dp.edges().is_empty());
    let root = &dp.fragments()[0].root;
    assert!(matches!(&root.payload, DistributedNodeKind::Scan(_)));
}

#[test]
fn build_distributed_plan_cte_anchor_splits_produce_fragment_and_consume_exchange() {
    let cte_id: CteId = 7;
    let producer_columns = vec![output_col(1, "p_k", DataType::Int64, false)];
    let consumer_columns = vec![output_col(2, "c_k", DataType::Int64, false)];
    let scan = scan_node_with_columns(producer_columns.clone());
    let produce = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEProduce(PlanCTEProduceNode {
            cte_id,
            output_columns: producer_columns.clone(),
        }),
        children: vec![scan],
        output_columns: producer_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let consume = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEConsume(PlanCTEConsumeNode {
            cte_id,
            alias: "cte_alias".to_string(),
            output_columns: consumer_columns.clone(),
            producer_column_ids: vec![producer_columns[0].column_id],
        }),
        children: vec![],
        output_columns: consumer_columns.clone(),
        stats: stats_with_cost(),
        probe_runtime_filters: vec![],
    };
    let anchor = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id }),
        children: vec![produce, consume],
        output_columns: consumer_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&anchor).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 2);
    assert_eq!(dp.root_fragment_id(), 0);
    assert_eq!(dp.edges().len(), 1);

    let produce_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.cte_id == Some(cte_id))
        .expect("produce fragment");
    assert_eq!(produce_fragment.fragment_id, 1);
    assert!(matches!(produce_fragment.sink, DataSink::Noop));
    assert_eq!(
        produce_fragment.output_columns.len(),
        producer_columns.len()
    );
    assert_eq!(
        produce_fragment.output_columns[0].column_id,
        producer_columns[0].column_id
    );
    assert!(produce_fragment.cte_exchange_nodes.is_empty());
    assert!(matches!(
        &produce_fragment.root.payload,
        DistributedNodeKind::Scan(_)
    ));

    let root_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id())
        .expect("root fragment");
    assert!(matches!(root_fragment.sink, DataSink::Result));
    assert_eq!(root_fragment.cte_id, None);

    let project = &root_fragment.root;
    let project_payload = match &project.payload {
        DistributedNodeKind::Project(project) => project,
        other => panic!("expected CTE consume remap Project root, got {other:?}"),
    };
    assert_eq!(
        project_payload.output_qualifier.as_deref(),
        Some("cte_alias")
    );
    assert_eq!(project_payload.items.len(), consumer_columns.len());
    assert_eq!(
        project_payload.items[0].output_column_id,
        consumer_columns[0].column_id
    );
    match &project_payload.items[0].expr.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            assert_eq!(*column_id, producer_columns[0].column_id);
        }
        other => panic!("expected producer ColumnRef, got {other:?}"),
    }

    let exchange = project.children.first().expect("project exchange child");
    let receiver = match &exchange.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected CTE consume Exchange child, got {other:?}"),
    };
    assert_eq!(exchange.fragment_id, dp.root_fragment_id());
    assert_eq!(exchange.tuple_ids.len(), 1);
    assert!(
        exchange.stats.cost_estimate.is_none(),
        "synthetic CTE Exchange must not inherit CTEConsume cost"
    );
    assert_eq!(receiver.source_fragment_id, produce_fragment.fragment_id);
    assert!(matches!(
        receiver.partition.kind,
        PartitionKind::Unpartitioned
    ));
    assert!(receiver.partition.exprs.is_empty());
    assert_eq!(receiver.output_columns.len(), producer_columns.len());
    assert_eq!(
        receiver.output_columns[0].column_id,
        producer_columns[0].column_id
    );
    assert_eq!(receiver.output_qualifier.as_deref(), Some("cte_alias"));
    let receive_producer_column_ids = match &receiver.flavor {
        ExchangeFlavor::CteMulticast {
            cte_id: flavor_cte_id,
            receive_producer_column_ids,
        } => {
            assert_eq!(*flavor_cte_id, cte_id);
            receive_producer_column_ids
        }
        other => panic!("expected CteMulticast exchange flavor, got {other:?}"),
    };
    assert_eq!(
        receive_producer_column_ids,
        &vec![producer_columns[0].column_id]
    );

    let edge = &dp.edges()[0];
    assert_eq!(edge.source_fragment_id, produce_fragment.fragment_id);
    assert_eq!(edge.target_fragment_id, dp.root_fragment_id());
    assert_eq!(edge.target_exchange_node_id, exchange.node_id);
    assert_eq!(edge.stream_kind, FragmentStreamKind::Broadcast);
    assert!(matches!(
        edge.output_partition.kind,
        PartitionKind::Unpartitioned
    ));
    assert_eq!(edge.output_slot_ids, vec![1]);
    match &edge.edge_kind {
        FragmentEdgeKind::CteMulticast {
            cte_id: edge_cte_id,
            receive_producer_column_ids,
        } => {
            assert_eq!(*edge_cte_id, cte_id);
            assert_eq!(
                receive_producer_column_ids,
                &vec![producer_columns[0].column_id]
            );
        }
        other => panic!("expected CteMulticast edge, got {other:?}"),
    }
    assert_eq!(
        root_fragment.cte_exchange_nodes,
        vec![(
            cte_id,
            exchange.node_id,
            vec![producer_columns[0].column_id]
        )]
    );
}

#[test]
fn build_distributed_plan_cte_consume_remaps_pruned_producer_columns_with_project() {
    let cte_id: CteId = 8;
    let producer_columns = vec![
        output_col(1, "k", DataType::Int64, false),
        output_col(2, "v", DataType::Int64, false),
        output_col(3, "payload", DataType::Int64, false),
    ];
    let consumer_columns = vec![
        output_col(11, "k", DataType::Int64, false),
        output_col(13, "payload", DataType::Int64, false),
    ];
    let scan = scan_node_with_columns(producer_columns.clone());
    let produce = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEProduce(PlanCTEProduceNode {
            cte_id,
            output_columns: producer_columns.clone(),
        }),
        children: vec![scan],
        output_columns: producer_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let consume = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEConsume(PlanCTEConsumeNode {
            cte_id,
            alias: "cte_alias".to_string(),
            output_columns: consumer_columns.clone(),
            producer_column_ids: vec![producer_columns[0].column_id, producer_columns[2].column_id],
        }),
        children: vec![],
        output_columns: consumer_columns.clone(),
        stats: stats_with_cost(),
        probe_runtime_filters: vec![],
    };
    let anchor = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id }),
        children: vec![produce, consume],
        output_columns: consumer_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&anchor).expect("build_distributed_plan");
    let produce_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.cte_id == Some(cte_id))
        .expect("produce fragment");
    let root_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id())
        .expect("root fragment");

    let project = &root_fragment.root;
    let DistributedNodeKind::Project(project_payload) = &project.payload else {
        panic!("expected CTE consume remap Project root");
    };
    assert_eq!(
        project_payload.output_qualifier.as_deref(),
        Some("cte_alias")
    );
    assert_eq!(
        project_payload
            .items
            .iter()
            .map(|item| item.output_column_id)
            .collect::<Vec<_>>(),
        consumer_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        project_payload
            .items
            .iter()
            .map(|item| match &item.expr.kind {
                ExprKind::ColumnRef { column_id, .. } => *column_id,
                other => panic!("expected producer ColumnRef, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![producer_columns[0].column_id, producer_columns[2].column_id]
    );

    let exchange = project.children.first().expect("project exchange child");
    let receiver = match &exchange.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected CTE consume Exchange child, got {other:?}"),
    };
    assert_eq!(receiver.source_fragment_id, produce_fragment.fragment_id);
    assert_eq!(receiver.output_qualifier.as_deref(), Some("cte_alias"));
    assert_eq!(
        receiver
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        vec![producer_columns[0].column_id, producer_columns[2].column_id]
    );

    let edge = &dp.edges()[0];
    assert_eq!(edge.target_exchange_node_id, exchange.node_id);
    assert_eq!(edge.output_slot_ids, vec![1, 3]);
    match &edge.edge_kind {
        FragmentEdgeKind::CteMulticast {
            cte_id: edge_cte_id,
            receive_producer_column_ids,
        } => {
            assert_eq!(*edge_cte_id, cte_id);
            assert_eq!(
                receive_producer_column_ids,
                &vec![producer_columns[0].column_id, producer_columns[2].column_id]
            );
        }
        other => panic!("expected CteMulticast edge, got {other:?}"),
    }
    assert_eq!(
        root_fragment.cte_exchange_nodes,
        vec![(
            cte_id,
            exchange.node_id,
            vec![producer_columns[0].column_id, producer_columns[2].column_id]
        )]
    );
}

#[test]
fn build_distributed_plan_cte_consume_rejects_producer_slot_id_overflow() {
    let cte_id: CteId = 9;
    let producer_column = output_col(i32::MAX as u32 + 1, "p_k", DataType::Int64, false);
    let producer_columns = vec![producer_column.clone()];
    let produce = cte_produce_node(
        cte_id,
        producer_columns.clone(),
        scan_node_with_columns(producer_columns),
    );
    let consume = cte_consume_node(cte_id, 2, vec![producer_column.column_id]);
    let anchor = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id }),
        children: vec![produce, consume],
        output_columns: vec![output_col(2, "c_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err = build_distributed_plan(&anchor).expect_err("producer slot id overflow");

    assert!(
        err.contains("output column id c2147483648 cannot be encoded as stream output slot id"),
        "unexpected error: {err}"
    );
}

#[test]
fn build_distributed_plan_cte_produce_root_fails_without_visiting_child() {
    let cte_id: CteId = 7;
    let scan = scan_node(1, "k");
    let limit = PhysicalPlanNode {
        kind: PhysicalPlanKind::Limit(PlanLimitNode {
            limit: Some(1),
            offset: None,
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let produce = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEProduce(PlanCTEProduceNode {
            cte_id,
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        }),
        children: vec![limit],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err = build_distributed_plan(&produce)
        .expect_err("direct CTEProduce must fail before visiting child");

    assert!(
        err.contains("PhysicalCTEProduce emits no DistributedPlan node outside CTEAnchor"),
        "unexpected error: {err}"
    );
    assert!(
        !err.contains("PhysicalPlanKind::Limit"),
        "direct CTEProduce should fail before visiting unsupported child: {err}"
    );
}

#[test]
fn build_distributed_plan_cte_anchor_rejects_non_produce_first_child() {
    let cte_id: CteId = 7;
    let scan = scan_node(1, "k");
    let consume = cte_consume_node(cte_id, 2, vec![ColumnId::new_for_test(1)]);
    let anchor = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id }),
        children: vec![scan, consume],
        output_columns: vec![output_col(2, "c_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err =
        build_distributed_plan(&anchor).expect_err("CTEAnchor first child must be CTEProduce");

    assert!(
        err.contains("PhysicalCTEAnchor first child must be PhysicalCTEProduce"),
        "unexpected error: {err}"
    );
}

#[test]
fn build_distributed_plan_cte_consume_rejects_unknown_cte_id() {
    let consume = cte_consume_node(7, 2, vec![ColumnId::new_for_test(1)]);

    let err = build_distributed_plan(&consume).expect_err("unknown CTE id should be rejected");

    assert!(
        err.contains("CTE consume references unknown cte_id=7"),
        "unexpected error: {err}"
    );
}

#[test]
fn build_distributed_plan_cte_consume_rejects_bad_mapping() {
    let cte_id: CteId = 7;
    let producer_columns = vec![output_col(1, "p_k", DataType::Int64, false)];
    let produce = cte_produce_node(cte_id, producer_columns.clone(), scan_node(1, "p_k"));
    let bad_arity_consume = cte_consume_node(cte_id, 2, vec![]);
    let anchor = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id }),
        children: vec![produce.clone(), bad_arity_consume],
        output_columns: vec![output_col(2, "c_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err = build_distributed_plan(&anchor).expect_err("bad CTE mapping should be rejected");

    assert!(
        err.contains("CTEConsume output/producers arity mismatch for cte_id=7"),
        "unexpected error: {err}"
    );

    let duplicate_output_consume = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEConsume(PlanCTEConsumeNode {
            cte_id,
            alias: "cte_alias".to_string(),
            output_columns: vec![
                output_col(2, "c_k", DataType::Int64, false),
                output_col(2, "c_k_dup", DataType::Int64, false),
            ],
            producer_column_ids: vec![producer_columns[0].column_id, producer_columns[0].column_id],
        }),
        children: vec![],
        output_columns: vec![output_col(2, "c_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let anchor = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id }),
        children: vec![produce, duplicate_output_consume],
        output_columns: vec![output_col(2, "c_k", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err = build_distributed_plan(&anchor)
        .expect_err("duplicate CTE consume output should be rejected");

    assert!(
        err.contains("CTEConsume duplicate output column 2 for cte_id=7"),
        "unexpected error: {err}"
    );
}

#[test]
fn build_distributed_plan_collects_multiple_cte_exchange_nodes_in_root_tree() {
    let cte_id: CteId = 7;
    let producer_columns = vec![output_col(1, "p_k", DataType::Int64, false)];
    let produce = cte_produce_node(cte_id, producer_columns.clone(), scan_node(1, "p_k"));
    let left_consume = cte_consume_node(cte_id, 2, vec![producer_columns[0].column_id]);
    let right_consume = cte_consume_node(cte_id, 3, vec![producer_columns[0].column_id]);
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
            execution_mode: None,
            build_runtime_filters: vec![],
            output_columns: vec![
                output_col(2, "c_k", DataType::Int64, false),
                output_col(3, "c_k", DataType::Int64, false),
            ],
        })),
        children: vec![left_consume, right_consume],
        output_columns: vec![
            output_col(2, "c_k", DataType::Int64, false),
            output_col(3, "c_k", DataType::Int64, false),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let anchor = PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id }),
        children: vec![produce, join],
        output_columns: vec![
            output_col(2, "c_k", DataType::Int64, false),
            output_col(3, "c_k", DataType::Int64, false),
        ],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&anchor).expect("build_distributed_plan");
    let root_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id())
        .expect("root fragment");

    assert_eq!(root_fragment.cte_exchange_nodes.len(), 2);
    assert_eq!(dp.edges().len(), 2);
    assert!(
        root_fragment
            .cte_exchange_nodes
            .iter()
            .all(|(exchange_cte_id, _, producer_ids)| {
                *exchange_cte_id == cte_id && producer_ids == &vec![producer_columns[0].column_id]
            })
    );
}

#[test]
fn build_distributed_plan_limit_offset_over_scan_creates_gather_exchange() {
    let scan = scan_node(1, "k");
    let limit = PhysicalPlanNode {
        kind: PhysicalPlanKind::Limit(PlanLimitNode {
            limit: Some(5),
            offset: Some(2),
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats_with_row_count_and_cost(5.0),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&limit).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 2);
    assert_eq!(dp.edges().len(), 1);

    let root_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id())
        .expect("root fragment");
    let exchange = &root_fragment.root;
    let receiver = match &exchange.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected LimitOffset Exchange root, got {other:?}"),
    };
    assert_eq!(exchange.limit, 5);
    assert_eq!(exchange.stats.output_row_count, 5.0);
    assert!(
        exchange.stats.cost_estimate.is_none(),
        "synthetic LimitOffset Exchange must not inherit Limit cost"
    );
    assert!(
        exchange.stats.broadcast_decision.is_none(),
        "synthetic LimitOffset Exchange must not inherit Limit broadcast decision"
    );
    assert!(matches!(
        receiver.partition.kind,
        PartitionKind::Unpartitioned
    ));
    assert!(receiver.partition.exprs.is_empty());
    assert!(receiver.output_columns.is_empty());
    assert_eq!(receiver.output_qualifier, None);
    match &receiver.flavor {
        ExchangeFlavor::LimitOffset { limit, offset } => {
            assert_eq!(*limit, Some(5));
            assert_eq!(*offset, Some(2));
        }
        other => panic!("expected LimitOffset exchange flavor, got {other:?}"),
    }

    let edge = &dp.edges()[0];
    assert_eq!(edge.source_fragment_id, receiver.source_fragment_id);
    assert_eq!(edge.target_fragment_id, dp.root_fragment_id());
    assert_eq!(edge.target_exchange_node_id, exchange.node_id);
    assert_eq!(edge.stream_kind, FragmentStreamKind::Gather);
    assert!(matches!(edge.edge_kind, FragmentEdgeKind::Stream));
    assert!(matches!(
        edge.output_partition.kind,
        PartitionKind::Unpartitioned
    ));
    assert!(edge.output_slot_ids.is_empty());

    let child_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == receiver.source_fragment_id)
        .expect("child fragment");
    assert!(matches!(child_fragment.sink, DataSink::Noop));
    assert!(matches!(
        child_fragment.output_partition.kind,
        PartitionKind::Unpartitioned
    ));
    assert!(matches!(
        &child_fragment.root.payload,
        DistributedNodeKind::Scan(_)
    ));
    assert_eq!(exchange.tuple_ids, child_fragment.root.tuple_ids);
}

#[test]
fn build_distributed_plan_topn_final_split_creates_topn_exchange() {
    let scan = scan_node(1, "k");
    let sort_key = sort_item(column_ref_expr(1, "k", DataType::Int64, false));
    let topn = PhysicalPlanNode {
        kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
            items: vec![sort_key.clone()],
            limit: Some(10),
            offset: Some(3),
            phase: TopNPhase::Final,
            is_split: true,
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats_with_row_count_and_cost(10.0),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&topn).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 2);
    assert_eq!(dp.edges().len(), 1);

    let root_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id())
        .expect("root fragment");
    let exchange = &root_fragment.root;
    let receiver = match &exchange.payload {
        DistributedNodeKind::Exchange(receiver) => receiver,
        other => panic!("expected TopNSplit Exchange root, got {other:?}"),
    };
    assert_eq!(exchange.limit, 10);
    assert_eq!(exchange.stats.output_row_count, 10.0);
    assert!(
        exchange.stats.cost_estimate.is_none(),
        "synthetic TopNSplit Exchange must not inherit TopN cost"
    );
    assert!(
        exchange.stats.broadcast_decision.is_none(),
        "synthetic TopNSplit Exchange must not inherit TopN broadcast decision"
    );
    assert!(matches!(
        receiver.partition.kind,
        PartitionKind::Unpartitioned
    ));
    assert!(receiver.partition.exprs.is_empty());
    assert!(receiver.output_columns.is_empty());
    assert_eq!(receiver.output_qualifier, None);
    match &receiver.flavor {
        ExchangeFlavor::TopNSplit {
            items,
            limit,
            offset,
        } => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].asc, sort_key.asc);
            assert_eq!(items[0].nulls_first, sort_key.nulls_first);
            assert_column_ref(&items[0].expr, 1, "k");
            assert_eq!(*limit, Some(10));
            assert_eq!(*offset, Some(3));
        }
        other => panic!("expected TopNSplit exchange flavor, got {other:?}"),
    }

    let edge = &dp.edges()[0];
    assert_eq!(edge.source_fragment_id, receiver.source_fragment_id);
    assert_eq!(edge.target_fragment_id, dp.root_fragment_id());
    assert_eq!(edge.target_exchange_node_id, exchange.node_id);
    assert_eq!(edge.stream_kind, FragmentStreamKind::Gather);
    assert!(matches!(edge.edge_kind, FragmentEdgeKind::Stream));
    assert!(matches!(
        edge.output_partition.kind,
        PartitionKind::Unpartitioned
    ));
    let child_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == receiver.source_fragment_id)
        .expect("child fragment");
    assert!(matches!(child_fragment.sink, DataSink::Noop));
    assert_eq!(
        child_fragment.output_columns[0].column_id,
        ColumnId::new_for_test(1)
    );
}

#[test]
fn build_distributed_plan_topn_final_split_uses_child_producer_columns() {
    let child = scan_node(1, "left_key");
    let topn = PhysicalPlanNode {
        kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
            items: vec![sort_item(column_ref_expr(
                1,
                "left_key",
                DataType::Int64,
                false,
            ))],
            limit: Some(4),
            offset: Some(0),
            phase: TopNPhase::Final,
            is_split: true,
        }),
        children: vec![child],
        // This models a TopN pushed under an outer join: the enclosing
        // expression can still expose a right-side column, but the child
        // fragment only produces the preserved-side key.
        output_columns: vec![
            output_col(1, "left_key", DataType::Int64, false),
            output_col(2, "right_payload", DataType::Utf8, true),
        ],
        stats: stats_with_row_count_and_cost(4.0),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&topn).expect("build_distributed_plan");
    let root_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id())
        .expect("root fragment");
    let DistributedNodeKind::Exchange(receiver) = &root_fragment.root.payload else {
        panic!("expected TopNSplit exchange root");
    };
    let child_fragment = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == receiver.source_fragment_id)
        .expect("child fragment");

    assert_eq!(child_fragment.output_columns.len(), 1);
    assert_eq!(
        child_fragment.output_columns[0].column_id,
        ColumnId::new_for_test(1)
    );
    assert_eq!(dp.edges()[0].output_slot_ids, vec![1]);
}

#[test]
fn build_distributed_plan_limit_over_sort_collapses_into_local_sort() {
    let scan = scan_node(1, "k");
    let sort_stats = stats_with_cost();
    let sort = PhysicalPlanNode {
        kind: PhysicalPlanKind::Sort(PlanSortNode {
            items: vec![sort_item(column_ref_expr(1, "k", DataType::Int64, false))],
            analytic_partition_by: vec![],
            output_columns: scan.output_columns.clone(),
            offset: None,
            partition_limit: None,
            topn_type: None,
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: sort_stats.clone(),
        probe_runtime_filters: vec![],
    };
    let limit = PhysicalPlanNode {
        kind: PhysicalPlanKind::Limit(PlanLimitNode {
            limit: Some(7),
            offset: Some(4),
        }),
        children: vec![sort],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats_with_row_count(7.0),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&limit).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 1);
    assert!(dp.edges().is_empty());
    let root = &dp.fragments()[0].root;
    let sort = match &root.payload {
        DistributedNodeKind::Sort(sort) => sort,
        other => panic!("expected Sort root, got {other:?}"),
    };
    assert_eq!(root.limit, 7);
    assert_eq!(sort.offset, Some(4));
    assert_eq!(root.stats.output_row_count, 7.0);
    assert_eq!(root.stats.cost_estimate, sort_stats.cost_estimate);
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![1]);
    assert_eq!(root.children.len(), 1);
}

#[test]
fn build_distributed_plan_limit_over_topn_collapses_into_local_topn() {
    let scan = scan_node(1, "k");
    let topn_stats = stats_with_cost();
    let topn = PhysicalPlanNode {
        kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
            items: vec![sort_item(column_ref_expr(1, "k", DataType::Int64, false))],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: topn_stats.clone(),
        probe_runtime_filters: vec![],
    };
    let limit = PhysicalPlanNode {
        kind: PhysicalPlanKind::Limit(PlanLimitNode {
            limit: Some(7),
            offset: Some(4),
        }),
        children: vec![topn],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats_with_row_count(7.0),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&limit).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 1);
    assert!(dp.edges().is_empty());
    let root = &dp.fragments()[0].root;
    let topn = match &root.payload {
        DistributedNodeKind::TopN(topn) => topn,
        other => panic!("expected TopN root, got {other:?}"),
    };
    assert_eq!(root.limit, 7);
    assert_eq!(topn.limit, Some(7));
    assert_eq!(topn.offset, Some(4));
    assert_eq!(root.stats.output_row_count, 7.0);
    assert_eq!(root.stats.cost_estimate, topn_stats.cost_estimate);
}

#[test]
fn build_distributed_plan_topn_non_split_stays_in_fragment() {
    let scan = scan_node(1, "k");
    let topn = PhysicalPlanNode {
        kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
            items: vec![sort_item(column_ref_expr(1, "k", DataType::Int64, false))],
            limit: Some(3),
            offset: Some(1),
            phase: TopNPhase::Final,
            is_split: false,
        }),
        children: vec![scan],
        output_columns: vec![output_col(1, "k", DataType::Int64, false)],
        stats: stats_with_row_count(3.0),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&topn).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 1);
    assert!(dp.edges().is_empty());
    let root = &dp.fragments()[0].root;
    assert_eq!(root.limit, 3);
    assert_eq!(root.node_id, 2);
    assert_eq!(root.tuple_ids, vec![1]);
    assert_eq!(root.stats.output_row_count, 3.0);
    assert!(matches!(&root.payload, DistributedNodeKind::TopN(_)));
}

#[test]
fn build_distributed_plan_union_distinct_rejects_residual_distinct() {
    let output_columns = vec![output_col(1, "u_k", DataType::Int64, false)];
    let set_op = PhysicalPlanNode {
        kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionDistinct,
            output_columns: output_columns.clone(),
            child_output_columns: vec![output_columns.clone(), output_columns.clone()],
        }),
        children: vec![
            values_node(output_columns.clone()),
            values_node(output_columns.clone()),
        ],
        output_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err = build_distributed_plan(&set_op)
        .expect_err("residual UnionDistinct must fail before distributed build");

    assert_eq!(err, union_distinct_must_be_rewritten_error());
}

#[test]
fn build_distributed_plan_set_op_rejects_empty_inputs() {
    let output_columns = vec![output_col(1, "u_k", DataType::Int64, false)];
    let set_op = PhysicalPlanNode {
        kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns: output_columns.clone(),
            child_output_columns: vec![],
        }),
        children: vec![],
        output_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err = build_distributed_plan(&set_op).expect_err("SetOp without children must be rejected");

    assert_eq!(err, "set operation node has no inputs");
}

#[test]
fn build_distributed_plan_union_all_passes_through_same_fragment() {
    let output_columns = vec![output_col(1, "u_k", DataType::Int64, false)];
    let left_columns = vec![output_col(11, "l_k", DataType::Int64, false)];
    let right_columns = vec![output_col(21, "r_k", DataType::Int64, false)];
    let set_op = PhysicalPlanNode {
        kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns: output_columns.clone(),
            child_output_columns: vec![left_columns.clone(), right_columns.clone()],
        }),
        children: vec![values_node(left_columns), values_node(right_columns)],
        output_columns: output_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&set_op).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 1);
    assert!(dp.edges().is_empty());
    let root = &dp.fragments()[0].root;
    let union_all = match &root.payload {
        DistributedNodeKind::SetOp(set_op) => set_op,
        other => panic!("expected SetOp root, got {other:?}"),
    };
    assert_eq!(union_all.kind, PlanSetOpKind::UnionAll);
    assert_eq!(union_all.output_columns.len(), output_columns.len());
    assert_eq!(
        union_all.output_columns[0].column_id,
        output_columns[0].column_id
    );
    assert_eq!(root.node_id, 3);
    assert_eq!(root.tuple_ids, vec![3]);
    assert_eq!(root.fragment_id, dp.root_fragment_id());
    assert_eq!(root.children.len(), 2);
    assert_eq!(root.children[0].fragment_id, dp.root_fragment_id());
    assert_eq!(root.children[1].fragment_id, dp.root_fragment_id());
    assert!(matches!(
        &root.children[0].payload,
        DistributedNodeKind::Values(_)
    ));
    assert!(matches!(
        &root.children[1].payload,
        DistributedNodeKind::Values(_)
    ));
}

#[test]
fn build_distributed_plan_intersect_passes_through_same_fragment() {
    let output_columns = vec![output_col(1, "u_k", DataType::Int64, false)];
    let left_columns = vec![output_col(11, "l_k", DataType::Int64, false)];
    let right_columns = vec![output_col(21, "r_k", DataType::Int64, false)];
    let set_op = PhysicalPlanNode {
        kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
            kind: PlanSetOpKind::Intersect,
            output_columns: output_columns.clone(),
            child_output_columns: vec![left_columns.clone(), right_columns.clone()],
        }),
        children: vec![values_node(left_columns), values_node(right_columns)],
        output_columns: output_columns.clone(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let dp = build_distributed_plan(&set_op).expect("build_distributed_plan");

    assert_eq!(dp.fragments().len(), 1);
    assert!(dp.edges().is_empty());
    let root = &dp.fragments()[0].root;
    let intersect = match &root.payload {
        DistributedNodeKind::SetOp(set_op) => set_op,
        other => panic!("expected SetOp root, got {other:?}"),
    };
    assert_eq!(intersect.kind, PlanSetOpKind::Intersect);
    assert_eq!(intersect.output_columns.len(), output_columns.len());
    assert_eq!(
        intersect.output_columns[0].column_id,
        output_columns[0].column_id
    );
    assert_eq!(root.children.len(), 2);
    assert_eq!(root.children[0].fragment_id, dp.root_fragment_id());
    assert_eq!(root.children[1].fragment_id, dp.root_fragment_id());
}

#[test]
fn build_distributed_plan_rejects_project_without_child() {
    let project = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![],
            output_qualifier: None,
        }),
        children: vec![],
        output_columns: vec![output_col(2, "k_alias", DataType::Int64, false)],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err = build_distributed_plan(&project).expect_err("Project with 0 children is malformed");

    assert!(err.contains("Project"), "unexpected error: {err}");
    assert!(
        err.contains("expected 1 children"),
        "unexpected error: {err}"
    );
    assert!(err.contains("got 0"), "unexpected error: {err}");
}

#[test]
fn build_distributed_plan_local_arity_error_precedes_malformed_child_error() {
    let malformed_child = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![],
            output_qualifier: None,
        }),
        children: vec![],
        output_columns: vec![],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    let parent = PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![],
            output_qualifier: None,
        }),
        children: vec![malformed_child, values_node(vec![])],
        output_columns: vec![],
        stats: stats(),
        probe_runtime_filters: vec![],
    };

    let err = build_distributed_plan(&parent).expect_err("parent Project arity must win");

    assert_eq!(
        err,
        "build_distributed_plan: PhysicalPlanKind::Project expected 1 children, got 2"
    );
}

fn stats() -> PhysicalPlanStats {
    stats_with_row_count(0.0)
}

fn stats_with_row_count(output_row_count: f64) -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count,
        row_count_confidence: PlannerConfidence::Fallback,
        column_statistics: HashMap::new(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn stats_with_cost() -> PhysicalPlanStats {
    stats_with_row_count_and_cost(0.0)
}

fn stats_with_row_count_and_cost(output_row_count: f64) -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count,
        cost_estimate: Some(PlannerCostEstimate {
            cpu_cost: 1.0,
            memory_cost: 2.0,
            network_cost: 3.0,
        }),
        broadcast_decision: Some(crate::sql::planner::physical::PlannerBroadcastDecision {
            feasible: true,
            forced: false,
            build_bytes: 10.0,
            hash_table_bytes: 20.0,
            effective_backend_count: 3.0,
            risk_adj_fanout_bytes: 30.0,
            per_node_budget_bytes: 40.0,
            cluster_network_budget_bytes: 50.0,
            risk_multiplier: 1.0,
            reject_reason: None,
        }),
        ..stats()
    }
}

fn table_def() -> TableDef {
    TableDef {
        name: "t".to_string(),
        columns: vec![column_def("k", DataType::Int64, false)],
        iceberg_row_lineage_metadata_columns: vec![],
        source: ScanSource::StarRocks {
            db_id: 1,
            table_id: 2,
        },
    }
}

fn table_def_with_columns(columns: &[OutputColumn]) -> TableDef {
    TableDef {
        name: "t".to_string(),
        columns: columns
            .iter()
            .map(|column| column_def(&column.name, column.data_type.clone(), column.nullable))
            .collect(),
        iceberg_row_lineage_metadata_columns: vec![],
        source: ScanSource::StarRocks {
            db_id: 1,
            table_id: 2,
        },
    }
}

fn scan_node(column_id: u32, column_name: &str) -> PhysicalPlanNode {
    let scan_columns = vec![output_col(column_id, column_name, DataType::Int64, false)];
    scan_node_with_columns(scan_columns)
}

fn aggregate_topn_runtime_filter_plan(filter_id: i32) -> PhysicalPlanNode {
    let key = output_col(1, "key", DataType::Int64, false);
    let key_expr = column_ref_expr(1, "key", DataType::Int64, false);
    let mut scan = scan_node(1, "key");
    scan.probe_runtime_filters = vec![RuntimeFilterProbeIntent {
        filter_id,
        probe_expr: key_expr.clone(),
    }];
    let aggregate = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Local,
            group_by: vec![key_expr.clone()],
            aggregates: Vec::new(),
            is_merge: Vec::new(),
            output_layout: AggregateOutputLayout::new(vec![key.clone()], Vec::new()),
            output_columns: vec![key.clone()],
            topn_runtime_filter_builds: vec![AggregateTopNRuntimeFilterBuildIntent {
                filter_id,
                group_key_expr: key_expr.clone(),
                group_key_ordinal: 0,
                limit: NonZeroU32::new(2).unwrap(),
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }],
        })),
        children: vec![scan],
        output_columns: vec![key.clone()],
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    };
    PhysicalPlanNode {
        kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
            items: vec![SortItem {
                expr: key_expr,
                asc: true,
                nulls_first: false,
            }],
            limit: Some(2),
            offset: Some(0),
            phase: TopNPhase::Partial,
            is_split: true,
        }),
        children: vec![aggregate],
        output_columns: vec![key],
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    }
}

fn duplicate_aggregate_topn_runtime_filter_plan(filter_id: i32) -> PhysicalPlanNode {
    let left = aggregate_topn_runtime_filter_plan(filter_id);
    let mut right = aggregate_topn_runtime_filter_plan(filter_id);
    clear_runtime_filter_probes(&mut right);
    let output_columns = left
        .output_columns
        .iter()
        .chain(right.output_columns.iter())
        .cloned()
        .collect::<Vec<_>>();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::NestLoopJoin(PhysicalNestLoopJoinNode {
            join_type: JoinKind::Inner,
            condition: None,
            output_columns: output_columns.clone(),
        }),
        children: vec![left, right],
        output_columns,
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    }
}

fn aggregate_topn_runtime_filter_plan_with_unresolvable_project_probe(
    filter_id: i32,
) -> PhysicalPlanNode {
    let mut plan = aggregate_topn_runtime_filter_plan(filter_id);
    let PhysicalPlanKind::TopN(topn) = &mut plan.kind else {
        unreachable!("fixture root is TopN")
    };
    let projected_key = output_col(2, "projected_key", DataType::Int64, false);
    let projected_key_expr = column_ref_expr(2, "projected_key", DataType::Int64, false);
    topn.items[0].expr = projected_key_expr.clone();
    plan.output_columns = vec![projected_key.clone()];

    let aggregate = &mut plan.children[0];
    let PhysicalPlanKind::HashAggregate(aggregate_node) = &mut aggregate.kind else {
        unreachable!("TopN child is aggregate")
    };
    aggregate_node.group_by = vec![projected_key_expr.clone()];
    aggregate_node.output_layout =
        AggregateOutputLayout::new(vec![projected_key.clone()], Vec::new());
    aggregate_node.output_columns = vec![projected_key.clone()];
    aggregate_node.topn_runtime_filter_builds[0].group_key_expr = projected_key_expr.clone();
    aggregate.output_columns = vec![projected_key.clone()];

    let mut source = aggregate.children.remove(0);
    source.probe_runtime_filters.clear();
    aggregate.children = vec![PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![ProjectItem {
                expr: column_ref_expr(1, "key", DataType::Int64, false),
                output_name: "projected_key".to_string(),
                output_column_id: ColumnId::new_for_test(2),
            }],
            output_qualifier: None,
        }),
        children: vec![source],
        output_columns: vec![projected_key],
        stats: stats(),
        probe_runtime_filters: vec![RuntimeFilterProbeIntent {
            filter_id,
            probe_expr: column_ref_expr(2, "projected_key", DataType::Int32, false),
        }],
    }];
    plan
}

fn clear_runtime_filter_probes(node: &mut PhysicalPlanNode) {
    node.probe_runtime_filters.clear();
    for child in &mut node.children {
        clear_runtime_filter_probes(child);
    }
}

fn find_distributed_node(
    fragments: &[PlanFragment],
    predicate: impl Fn(&DistributedNode) -> bool + Copy,
) -> Option<&DistributedNode> {
    fn find(
        node: &DistributedNode,
        predicate: impl Fn(&DistributedNode) -> bool + Copy,
    ) -> Option<&DistributedNode> {
        predicate(node).then_some(node).or_else(|| {
            node.children
                .iter()
                .find_map(|child| find(child, predicate))
        })
    }
    fragments
        .iter()
        .find_map(|fragment| find(&fragment.root, predicate))
}

fn scan_node_with_columns(scan_columns: Vec<OutputColumn>) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Scan(PlanScanNode {
            database: "db".to_string(),
            table: table_def_with_columns(&scan_columns),
            alias: Some("t".to_string()),
            columns: scan_columns.clone(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }),
        children: vec![],
        output_columns: scan_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}

fn values_node(columns: Vec<OutputColumn>) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: columns.clone(),
        }),
        children: vec![],
        output_columns: columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}

fn distributed_values_node(node_id: i32, fragment_id: u32) -> DistributedNode {
    DistributedNode {
        node_id,
        fragment_id,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: vec![],
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![],
        stats: stats(),
        payload: DistributedNodeKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![],
        }),
    }
}

fn test_fragment(root: DistributedNode) -> PlanFragment {
    PlanFragment {
        fragment_id: root.fragment_id,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: vec![],
        cte_id: None,
        cte_exchange_nodes: vec![],
    }
}

fn test_build_binding(node_id: i32, fragment_id: u32, filter_id: i32) -> BuildBinding {
    BuildBinding {
        node_id,
        fragment_id,
        intent: RuntimeFilterBuildIntent {
            filter_id,
            build_expr: column_ref_expr(2, "build", DataType::Int64, false),
            probe_expr: column_ref_expr(1, "probe", DataType::Int64, false),
            expr_order: 0,
            execution_mode: JoinExecutionMode::Partitioned,
        },
    }
}

fn test_probe_binding(node_id: i32, fragment_id: u32, filter_id: i32) -> ProbeBinding {
    ProbeBinding {
        node_id,
        fragment_id,
        intent: RuntimeFilterProbeIntent {
            filter_id,
            probe_expr: column_ref_expr(1, "probe", DataType::Int64, false),
        },
    }
}

fn distributed_hash_join_node_with_exchanges(
    node_id: i32,
    fragment_id: u32,
    (probe_exchange_node_id, probe_source_fragment_id): (i32, u32),
    (build_exchange_node_id, build_source_fragment_id): (i32, u32),
) -> DistributedNode {
    let exchange = |node_id, source_fragment_id| DistributedNode {
        node_id,
        fragment_id,
        tuple_ids: vec![],
        nullable_tuple_ids: vec![],
        limit: -1,
        runtime_filter_binding_ids: vec![],
        children: vec![],
        stats: stats(),
        payload: DistributedNodeKind::Exchange(
            crate::sql::planner::distributed::ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id,
                output_columns: vec![],
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            },
        ),
    };
    DistributedNode {
        node_id,
        fragment_id,
        tuple_ids: vec![],
        nullable_tuple_ids: vec![],
        limit: -1,
        runtime_filter_binding_ids: vec![],
        children: vec![
            exchange(probe_exchange_node_id, probe_source_fragment_id),
            exchange(build_exchange_node_id, build_source_fragment_id),
        ],
        stats: stats(),
        payload: DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
            execution_mode: Some(JoinExecutionMode::Partitioned),
            build_runtime_filters: vec![],
            output_columns: vec![],
        })),
    }
}

fn cte_produce_node(
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
        probe_runtime_filters: vec![],
    }
}

fn cte_consume_node(
    cte_id: CteId,
    output_column_id: u32,
    producer_column_ids: Vec<ColumnId>,
) -> PhysicalPlanNode {
    let output_columns = vec![output_col(output_column_id, "c_k", DataType::Int64, false)];
    PhysicalPlanNode {
        kind: PhysicalPlanKind::CTEConsume(PlanCTEConsumeNode {
            cte_id,
            alias: "cte_alias".to_string(),
            output_columns: output_columns.clone(),
            producer_column_ids,
        }),
        children: vec![],
        output_columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}

fn window_expr(
    output_column: OutputColumn,
    partition_by: Vec<TypedExpr>,
    order_by: Vec<SortItem>,
) -> WindowExpr {
    WindowExpr {
        name: "row_number".to_string(),
        args: vec![],
        distinct: false,
        partition_by,
        order_by,
        window_frame: None,
        result_type: output_column.data_type,
        output_name: output_column.name,
        output_column_id: output_column.column_id,
        ignore_nulls: false,
    }
}

fn sort_item(expr: TypedExpr) -> SortItem {
    SortItem {
        expr,
        asc: true,
        nulls_first: false,
    }
}

fn repeat_node(with_grouping_fn_arg: bool) -> PlanRepeatNode {
    let grouping_fn_args = if with_grouping_fn_arg {
        vec![("grouping_k".to_string(), vec!["k".to_string()])]
    } else {
        vec![]
    };
    let grouping_fn_arg_ids = if with_grouping_fn_arg {
        vec![vec![ColumnId::new_for_test(1)]]
    } else {
        vec![]
    };
    let grouping_fn_ids = if with_grouping_fn_arg {
        vec![("grouping_k".to_string(), ColumnId::new_for_test(2))]
    } else {
        vec![]
    };

    PlanRepeatNode {
        repeat_column_ref_list: vec![],
        repeat_column_ref_ids: vec![],
        grouping_ids: vec![],
        all_rollup_columns: vec![],
        all_rollup_column_ids: vec![],
        grouping_key_aliases: vec![],
        grouping_fn_args,
        grouping_fn_arg_ids,
        grouping_fn_ids,
        virtual_tuple_id: None,
    }
}

fn column_def(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type,
        nullable,
        write_default: None,
        logical_type: None,
    }
}

fn output_col(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId::new_for_test(id),
        name: name.to_string(),
        data_type,
        nullable,
        is_internal: false,
    }
}

fn column_ref_expr(id: u32, column: &str, data_type: DataType, nullable: bool) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId::new_for_test(id),
            qualifier: Some("t".to_string()),
            column: column.to_string(),
        },
        data_type,
        nullable,
    }
}

fn bool_lit(value: bool) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Bool(value)),
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn int_lit(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        data_type: DataType::Int64,
        nullable: false,
    }
}

fn cmp_expr(column_id: u32, column: &str, op: BinOp, value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(column_ref_expr(column_id, column, DataType::Int64, false)),
            op,
            right: Box::new(int_lit(value)),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: BinOp::And,
            right: Box::new(right),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn assert_bool_lit(expr: &TypedExpr, expected: bool) {
    match &expr.kind {
        ExprKind::Literal(LiteralValue::Bool(value)) => assert_eq!(*value, expected),
        other => panic!("expected Bool literal, got {other:?}"),
    }
}

fn assert_cmp_expr(
    expr: &TypedExpr,
    expected_column_id: u32,
    expected_column: &str,
    expected_op: BinOp,
    expected_value: i64,
) {
    let (left, op, right) = match &expr.kind {
        ExprKind::BinaryOp { left, op, right } => (left, op, right),
        other => panic!("expected comparison expression, got {other:?}"),
    };
    assert_eq!(*op, expected_op);
    match &left.kind {
        ExprKind::ColumnRef {
            column_id, column, ..
        } => {
            assert_eq!(*column_id, ColumnId::new_for_test(expected_column_id));
            assert_eq!(column, expected_column);
        }
        other => panic!("expected column ref, got {other:?}"),
    }
    match &right.kind {
        ExprKind::Literal(LiteralValue::Int(value)) => assert_eq!(*value, expected_value),
        other => panic!("expected Int literal, got {other:?}"),
    }
}

fn assert_column_ref(expr: &TypedExpr, expected_column_id: u32, expected_column: &str) {
    match &expr.kind {
        ExprKind::ColumnRef {
            column_id, column, ..
        } => {
            assert_eq!(*column_id, ColumnId::new_for_test(expected_column_id));
            assert_eq!(column, expected_column);
        }
        other => panic!("expected ColumnRef, got {other:?}"),
    }
}

#[test]
fn join_progress_proof_catalog_partitions_fragment_inputs() {
    // Real binding path: hash-join node 10 in fragment 1 with probe exchange
    // (node 20 <- frag 2) and build exchange (node 30 <- frag 3); the probe
    // consumer lives on a Values node 40 in fragment 2.
    let join_root = distributed_hash_join_node_with_exchanges(10, 1, (20, 2), (30, 3));
    let mut fragments = vec![
        test_fragment(join_root),
        test_fragment(distributed_values_node(40, 2)),
        test_fragment(distributed_values_node(50, 3)),
    ];
    let build_bindings = vec![test_build_binding(10, 1, 7)];
    let probe_bindings = vec![test_probe_binding(40, 2, 7)];

    let mut graph = DraftRuntimeFilterGraph::default();
    populate_runtime_filter_graph(
        &mut fragments,
        &mut graph,
        &RuntimeFilterBindings {
            builds: build_bindings,
            probes: probe_bindings,
            topn_builds: Vec::new(),
            node_input_columns: std::collections::BTreeMap::new(),
        },
    )
    .expect("populate graph");

    let catalog = build_join_progress_proof_catalog(&fragments, &graph);
    assert_eq!(catalog.len(), 1);
    let proof = catalog.values().next().expect("one proof");
    assert_eq!(proof.producer_fragment, 1);
    assert_eq!(proof.join_node_id, 10);
    assert_eq!(
        proof.build_frontier,
        vec![FrontierEdge {
            source_fragment: 3,
            target_exchange_node: 30,
        }]
    );
    assert_eq!(
        proof.non_build_inputs,
        vec![FrontierEdge {
            source_fragment: 2,
            target_exchange_node: 20,
        }]
    );
}
