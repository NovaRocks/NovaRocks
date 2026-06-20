use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::exprs;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::partitions;
use crate::plan_nodes;
use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn as AnalysisOutputColumn, TypedExpr,
};
use crate::sql::catalog::{CatalogProvider, ScanSource, TableDef};
use crate::sql::codegen::boundary_schema::{
    BoundaryKind, BoundarySchemaReport, output_columns_to_boundary_columns,
};
use crate::sql::codegen::descriptors::DescriptorTableBuilder;
use crate::sql::codegen::expr_compiler::{self, ExprCompiler};
use crate::sql::codegen::fragment_builder::{
    add_iceberg_equality_delete_required_columns, build_noop_sink, build_result_sink,
    effective_iceberg_scan_column_names, iceberg_scan_table_handle_for_codegen, iceberg_table_info,
    output_columns_for_boundary, result_root_boundary_schema_report, synthetic_iceberg_table_id,
};
use crate::sql::codegen::helpers::{
    agg_call_display_name, agg_call_display_name_without_qualifiers, group_win_exprs_by_sig,
    join_kind_to_op, split_and_conjuncts_typed, typed_expr_display_name,
    typed_expr_display_name_without_qualifiers,
};
use crate::sql::codegen::nodes;
use crate::sql::codegen::resolve::{ColumnBinding, ExprScope, ResolvedTable};
use crate::sql::codegen::runtime_filter_lowering::{
    RfProbeTarget, join_distribution_mode_from_execution, legacy_rf_distribution_to_execution,
    remap_rf_expr_order, rf_build_expr_matches_join_build_expr,
    rf_layout_for_execution_distribution, rf_pipeline_dop,
};
use crate::sql::codegen::scalar_materialize::materialize;
use crate::sql::codegen::type_infer;
use crate::sql::codegen::{
    FragmentBuildResult, FragmentId, MultiFragmentBuildResult, OutputColumn,
};
use crate::sql::optimizer::operator::{
    AggMode, AssertOneRowOp, DecodeOp, GenerateSeriesOp, RepeatOp, ScanDictionaryColumn, TopNPhase,
};
use crate::sql::optimizer::physical_plan::JoinExecutionDistribution;
use crate::sql::optimizer::property::OrderingSpec;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::planner::optimizer_bridge::property::{
    ordering_spec_from_sort_items, window_ordering_spec,
};
use crate::sql::planner::plan::{AggregateCall, WindowExpr};
use crate::types;

pub(crate) fn lower_distributed_plan(
    dp: &crate::sql::planner::DistributedPlan,
    catalog: &dyn CatalogProvider,
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<MultiFragmentBuildResult, String> {
    let _ = catalog;
    validate_distributed_plan(dp)?;

    let mut state = OwnedLoweringState::new_with_fragments(
        connectors,
        mv_refresh_ctx,
        dp.root_fragment_id,
        &dp.fragments,
        Arc::clone(&dp.scalar_arena),
    );
    state.lower_fragment_by_id(dp.root_fragment_id)?;
    state.fragment_stack.clear();

    let edges = lower_fragment_edges(dp, &mut state)?;
    let lowered_fragments = std::mem::take(&mut state.lowered_fragments);

    let desc_tbl =
        std::mem::replace(&mut state.desc_builder, DescriptorTableBuilder::new()).build();
    let exec_params = nodes::build_exec_params_multi_with_refresh_context(
        connectors,
        &state.scan_tables,
        mv_refresh_ctx,
    )?;

    let mut fragment_results = Vec::with_capacity(lowered_fragments.len());
    for (fragment, lowered) in lowered_fragments {
        let output_columns = output_columns_for_boundary(&fragment.output_columns);
        let root_node_id = lowered
            .plan_nodes
            .first()
            .map(|node| node.node_id)
            .unwrap_or(-1);
        let boundary_schemas = vec![result_root_boundary_schema_report(
            fragment.fragment_id,
            root_node_id,
            &output_columns,
        )];
        let query_global_dicts = state
            .query_global_dicts_per_fragment
            .remove(&fragment.fragment_id)
            .filter(|dicts| !dicts.is_empty());
        let is_root = fragment.fragment_id == dp.root_fragment_id;
        let output_exprs = if is_root {
            result_output_exprs_for_columns(&lowered.scope, &fragment.output_columns)?
        } else {
            None
        };
        let output_sink = if is_root {
            build_result_sink()
        } else {
            build_noop_sink()
        };

        fragment_results.push(FragmentBuildResult {
            fragment_id: fragment.fragment_id,
            plan: plan_nodes::TPlan::new(lowered.plan_nodes),
            desc_tbl: desc_tbl.clone(),
            exec_params: exec_params.clone(),
            output_sink,
            output_exprs,
            output_columns,
            direct_exec: None,
            boundary_schemas,
            cte_id: fragment.cte_id,
            cte_exchange_nodes: fragment.cte_exchange_nodes.clone(),
            query_global_dicts,
            query_global_dict_exprs: None,
        });
    }

    let mut boundary_schemas = Vec::new();
    for fragment in &fragment_results {
        boundary_schemas.extend(fragment.boundary_schemas.clone());
    }
    boundary_schemas.extend(edge_boundary_schemas(dp)?);

    Ok(MultiFragmentBuildResult {
        fragment_results,
        root_fragment_id: dp.root_fragment_id,
        edges,
        boundary_schemas,
        rf_plan: if state.rf_all_filters.is_empty() {
            None
        } else {
            Some(crate::sql::codegen::RuntimeFilterPlanResult {
                all_filters: state.rf_all_filters,
                build_side_filters: state.rf_build_side_filters,
                probe_side_filters: state.rf_probe_side_filters,
            })
        },
    })
}

fn validate_distributed_plan(
    dp: &crate::sql::planner::DistributedPlan,
) -> Result<Vec<&crate::sql::planner::PlanFragment>, String> {
    if dp.fragments.is_empty() {
        return Err("lower_distributed_plan requires at least one fragment".to_string());
    }

    let mut fragments_by_id = BTreeMap::new();
    let mut input_index_by_id = BTreeMap::new();
    for (idx, fragment) in dp.fragments.iter().enumerate() {
        if fragments_by_id
            .insert(fragment.fragment_id, fragment)
            .is_some()
        {
            return Err(format!(
                "lower_distributed_plan duplicate fragment id={}",
                fragment.fragment_id
            ));
        }
        input_index_by_id.insert(fragment.fragment_id, idx);
    }

    for fragment in &dp.fragments {
        ensure_unpartitioned("data_partition", &fragment.data_partition)?;
        if fragment.output_exprs.is_some() {
            return Err(format!(
                "lower_distributed_plan does not support fragment output_exprs for fragment id={}",
                fragment.fragment_id
            ));
        }
        validate_node_fragment_ownership(fragment.fragment_id, &fragment.root)?;

        if fragment.fragment_id == dp.root_fragment_id {
            if !matches!(fragment.sink, crate::sql::planner::DataSink::Result) {
                return Err(format!(
                    "lower_distributed_plan root fragment id={} must use result sink",
                    fragment.fragment_id
                ));
            }
            ensure_unpartitioned("root output_partition", &fragment.output_partition)?;
        } else {
            if !matches!(fragment.sink, crate::sql::planner::DataSink::Noop) {
                return Err(format!(
                    "lower_distributed_plan non-root fragment id={} must use noop sink",
                    fragment.fragment_id
                ));
            }
        }
    }

    if !fragments_by_id.contains_key(&dp.root_fragment_id) {
        return Err(format!(
            "lower_distributed_plan root fragment id={} was not found",
            dp.root_fragment_id
        ));
    }

    let ordered_ids = topological_fragment_order(dp, &fragments_by_id, &input_index_by_id)?;
    Ok(ordered_ids
        .into_iter()
        .map(|fragment_id| {
            *fragments_by_id
                .get(&fragment_id)
                .expect("topological order references validated fragment id")
        })
        .collect())
}

fn validate_node_fragment_ownership(
    fragment_id: FragmentId,
    node: &crate::sql::planner::DistributedPlanNode,
) -> Result<(), String> {
    if node.fragment_id != fragment_id {
        return Err(format!(
            "lower_distributed_plan fragment id={} contains node_id={} with fragment_id={}",
            fragment_id, node.node_id, node.fragment_id
        ));
    }
    for child in &node.children {
        validate_node_fragment_ownership(fragment_id, child)?;
    }
    Ok(())
}

fn topological_fragment_order(
    dp: &crate::sql::planner::DistributedPlan,
    fragments_by_id: &BTreeMap<FragmentId, &crate::sql::planner::PlanFragment>,
    input_index_by_id: &BTreeMap<FragmentId, usize>,
) -> Result<Vec<FragmentId>, String> {
    let mut adjacency: BTreeMap<FragmentId, Vec<FragmentId>> = fragments_by_id
        .keys()
        .map(|fragment_id| (*fragment_id, Vec::new()))
        .collect();
    let mut indegree: BTreeMap<FragmentId, usize> = fragments_by_id
        .keys()
        .map(|fragment_id| (*fragment_id, 0))
        .collect();
    let mut reverse_adjacency: BTreeMap<FragmentId, Vec<FragmentId>> = fragments_by_id
        .keys()
        .map(|fragment_id| (*fragment_id, Vec::new()))
        .collect();

    for edge in &dp.edges {
        if !fragments_by_id.contains_key(&edge.source_fragment_id) {
            return Err(format!(
                "lower_distributed_plan edge references missing source fragment id={}",
                edge.source_fragment_id
            ));
        }
        let target = fragments_by_id
            .get(&edge.target_fragment_id)
            .ok_or_else(|| {
                format!(
                    "lower_distributed_plan edge references missing target fragment id={}",
                    edge.target_fragment_id
                )
            })?;
        validate_edge_target_node(target, edge)?;
        adjacency
            .get_mut(&edge.source_fragment_id)
            .expect("validated source fragment id")
            .push(edge.target_fragment_id);
        reverse_adjacency
            .get_mut(&edge.target_fragment_id)
            .expect("validated target fragment id")
            .push(edge.source_fragment_id);
        *indegree
            .get_mut(&edge.target_fragment_id)
            .expect("validated target fragment id") += 1;
    }

    validate_non_root_connectivity(
        dp.root_fragment_id,
        fragments_by_id,
        &adjacency,
        &reverse_adjacency,
    )?;

    let non_root_count = fragments_by_id
        .keys()
        .filter(|fragment_id| **fragment_id != dp.root_fragment_id)
        .count();
    let mut emitted = BTreeSet::new();
    let mut order = Vec::with_capacity(fragments_by_id.len());
    while order.len() < non_root_count {
        let next = fragments_by_id
            .keys()
            .filter(|fragment_id| **fragment_id != dp.root_fragment_id)
            .filter(|fragment_id| !emitted.contains(*fragment_id))
            .filter(|fragment_id| indegree.get(fragment_id).copied().unwrap_or_default() == 0)
            .min_by_key(|fragment_id| {
                input_index_by_id
                    .get(fragment_id)
                    .copied()
                    .unwrap_or(usize::MAX)
            })
            .copied()
            .ok_or_else(|| {
                "lower_distributed_plan cycle in DistributedPlan fragment edges".to_string()
            })?;

        emitted.insert(next);
        order.push(next);
        for target in adjacency.get(&next).into_iter().flatten() {
            let target_indegree = indegree
                .get_mut(target)
                .expect("adjacency references validated target fragment id");
            *target_indegree -= 1;
        }
    }

    if indegree
        .get(&dp.root_fragment_id)
        .copied()
        .unwrap_or_default()
        != 0
    {
        return Err("lower_distributed_plan cycle in DistributedPlan fragment edges".to_string());
    }
    order.push(dp.root_fragment_id);
    Ok(order)
}

fn validate_non_root_connectivity(
    root_fragment_id: FragmentId,
    fragments_by_id: &BTreeMap<FragmentId, &crate::sql::planner::PlanFragment>,
    adjacency: &BTreeMap<FragmentId, Vec<FragmentId>>,
    reverse_adjacency: &BTreeMap<FragmentId, Vec<FragmentId>>,
) -> Result<(), String> {
    for fragment_id in fragments_by_id
        .keys()
        .copied()
        .filter(|fragment_id| *fragment_id != root_fragment_id)
    {
        if adjacency
            .get(&fragment_id)
            .map(|targets| targets.is_empty())
            .unwrap_or(true)
        {
            return Err(format!(
                "lower_distributed_plan disconnected non-root fragment id={} has no outgoing edge toward root fragment id={}",
                fragment_id, root_fragment_id
            ));
        }
    }

    let mut reaches_root = BTreeSet::new();
    let mut stack = vec![root_fragment_id];
    while let Some(fragment_id) = stack.pop() {
        if !reaches_root.insert(fragment_id) {
            continue;
        }
        for source in reverse_adjacency.get(&fragment_id).into_iter().flatten() {
            stack.push(*source);
        }
    }

    for fragment_id in fragments_by_id
        .keys()
        .copied()
        .filter(|fragment_id| *fragment_id != root_fragment_id)
    {
        if !reaches_root.contains(&fragment_id) {
            return Err(format!(
                "lower_distributed_plan disconnected non-root fragment id={} is not connected to root fragment id={}",
                fragment_id, root_fragment_id
            ));
        }
    }
    Ok(())
}

fn validate_edge_target_node(
    target_fragment: &crate::sql::planner::PlanFragment,
    edge: &crate::sql::codegen::FragmentEdge,
) -> Result<(), String> {
    let target_node = find_node_by_id(&target_fragment.root, edge.target_exchange_node_id)
        .ok_or_else(|| {
            format!(
            "lower_distributed_plan edge target_exchange_node_id={} not found in target fragment id={}",
            edge.target_exchange_node_id, target_fragment.fragment_id
        )
        })?;

    let crate::sql::planner::PlanNodeKind::Exchange(exchange) = &target_node.kind else {
        return Err(format!(
            "lower_distributed_plan edge target_exchange_node_id={} in target fragment id={} must target Exchange",
            edge.target_exchange_node_id, target_fragment.fragment_id
        ));
    };

    match (&edge.edge_kind, &exchange.flavor) {
        (
            crate::sql::codegen::FragmentEdgeKind::Stream,
            super::kind::ExchangeFlavor::Distribution,
        )
        | (
            crate::sql::codegen::FragmentEdgeKind::Stream,
            super::kind::ExchangeFlavor::LimitOffset { .. },
        )
        | (
            crate::sql::codegen::FragmentEdgeKind::Stream,
            super::kind::ExchangeFlavor::TopNSplit { .. },
        ) => {
            if edge.source_fragment_id != exchange.source_fragment_id {
                return Err(format!(
                    "lower_distributed_plan stream edge source_fragment_id={} does not match Exchange source_fragment_id={} for target_exchange_node_id={} in target fragment id={}",
                    edge.source_fragment_id,
                    exchange.source_fragment_id,
                    edge.target_exchange_node_id,
                    target_fragment.fragment_id
                ));
            }
            Ok(())
        }
        (
            crate::sql::codegen::FragmentEdgeKind::CteMulticast { cte_id },
            super::kind::ExchangeFlavor::CteMulticast {
                cte_id: exchange_cte_id,
            },
        ) => {
            if cte_id != exchange_cte_id {
                return Err(format!(
                    "lower_distributed_plan CTE multicast edge cte_id={} does not match Exchange cte_id={} for target_exchange_node_id={} in target fragment id={}",
                    cte_id,
                    exchange_cte_id,
                    edge.target_exchange_node_id,
                    target_fragment.fragment_id
                ));
            }
            if edge.source_fragment_id != exchange.source_fragment_id {
                return Err(format!(
                    "lower_distributed_plan CTE multicast edge source_fragment_id={} does not match Exchange source_fragment_id={} for target_exchange_node_id={} in target fragment id={}",
                    edge.source_fragment_id,
                    exchange.source_fragment_id,
                    edge.target_exchange_node_id,
                    target_fragment.fragment_id
                ));
            }
            Ok(())
        }
        (crate::sql::codegen::FragmentEdgeKind::Stream, _) => Err(format!(
            "lower_distributed_plan stream edge target_exchange_node_id={} in target fragment id={} must target stream Exchange",
            edge.target_exchange_node_id, target_fragment.fragment_id
        )),
        (crate::sql::codegen::FragmentEdgeKind::CteMulticast { .. }, _) => Err(format!(
            "lower_distributed_plan CTE multicast edge target_exchange_node_id={} in target fragment id={} must target Exchange(CteMulticast)",
            edge.target_exchange_node_id, target_fragment.fragment_id
        )),
    }
}

fn find_node_by_id(
    node: &crate::sql::planner::DistributedPlanNode,
    node_id: i32,
) -> Option<&crate::sql::planner::DistributedPlanNode> {
    if node.node_id == node_id {
        return Some(node);
    }
    node.children
        .iter()
        .find_map(|child| find_node_by_id(child, node_id))
}

fn ensure_unpartitioned(
    label: &str,
    partition: &crate::sql::planner::DataPartition,
) -> Result<(), String> {
    if !matches!(
        partition.kind,
        crate::sql::planner::PartitionKind::Unpartitioned
    ) || !partition.exprs.is_empty()
    {
        return Err(format!(
            "lower_distributed_plan supports only unpartitioned {label}"
        ));
    }
    Ok(())
}

fn lower_fragment_edges(
    dp: &crate::sql::planner::DistributedPlan,
    state: &mut OwnedLoweringState<'_>,
) -> Result<Vec<crate::sql::codegen::FragmentEdge>, String> {
    let fragments_by_id: BTreeMap<FragmentId, &crate::sql::planner::PlanFragment> = dp
        .fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect();
    let mut lowered_edges = Vec::with_capacity(dp.edges.len());

    for edge in &dp.edges {
        let mut lowered_edge = edge.clone();
        if matches!(
            edge.edge_kind,
            crate::sql::codegen::FragmentEdgeKind::Stream
                | crate::sql::codegen::FragmentEdgeKind::CteMulticast { .. }
        ) {
            let exchange = target_exchange_for_edge(&fragments_by_id, edge)?;
            if state
                .lowered_fragment_output(edge.source_fragment_id)
                .is_none()
            {
                state.ensure_fragment_lowered(edge.source_fragment_id)?;
            }
            let source = state
                .lowered_fragment_output(edge.source_fragment_id)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "lower_distributed_plan edge references source fragment id={} before it was lowered",
                        edge.source_fragment_id
                    )
                })?;
            lowered_edge.output_partition =
                lower_exchange_output_partition(exchange, &source.scope, state.slot_allocator())?;
        }
        lowered_edges.push(lowered_edge);
    }

    Ok(lowered_edges)
}

fn lower_exchange_output_partition(
    exchange: &super::kind::DistributedExchangeNode,
    source_scope: &ExprScope,
    slot_allocator: expr_compiler::SlotAllocator,
) -> Result<partitions::TDataPartition, String> {
    if exchange.partition_type == partitions::TPartitionType::UNPARTITIONED
        || exchange.partition_type == partitions::TPartitionType::RANDOM
    {
        return Ok(partitions::TDataPartition::new(
            exchange.partition_type,
            None::<Vec<exprs::TExpr>>,
            None::<Vec<partitions::TRangePartition>>,
            None::<Vec<partitions::TBucketProperty>>,
        ));
    }

    if exchange.partition_type != partitions::TPartitionType::HASH_PARTITIONED {
        return Ok(partitions::TDataPartition::new(
            exchange.partition_type,
            None::<Vec<exprs::TExpr>>,
            None::<Vec<partitions::TRangePartition>>,
            None::<Vec<partitions::TBucketProperty>>,
        ));
    }

    if exchange.partition_exprs.is_empty() {
        return Err("DistributedPlan HASH Exchange has no partition expressions".to_string());
    }

    let mut partition_exprs = Vec::with_capacity(exchange.partition_exprs.len());
    for expr in &exchange.partition_exprs {
        let mut compiler = ExprCompiler::new(Rc::clone(&slot_allocator), source_scope);
        partition_exprs.push(compiler.compile_typed(expr)?);
    }

    Ok(partitions::TDataPartition::new(
        partitions::TPartitionType::HASH_PARTITIONED,
        Some(partition_exprs),
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    ))
}

fn edge_boundary_schemas(
    dp: &crate::sql::planner::DistributedPlan,
) -> Result<Vec<BoundarySchemaReport>, String> {
    let fragments_by_id: BTreeMap<FragmentId, &crate::sql::planner::PlanFragment> = dp
        .fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect();
    let mut reports = Vec::with_capacity(dp.edges.len() * 2);
    for edge in &dp.edges {
        let source = fragments_by_id
            .get(&edge.source_fragment_id)
            .ok_or_else(|| {
                format!(
                    "lower_distributed_plan edge references missing source fragment id={}",
                    edge.source_fragment_id
                )
            })?;
        if !fragments_by_id.contains_key(&edge.target_fragment_id) {
            return Err(format!(
                "lower_distributed_plan edge references missing target fragment id={}",
                edge.target_fragment_id
            ));
        }
        let exchange = target_exchange_for_edge(&fragments_by_id, edge)?;
        let edge_output_columns = match edge.edge_kind {
            crate::sql::codegen::FragmentEdgeKind::CteMulticast { .. } => {
                if exchange.output_columns.is_empty() {
                    &source.output_columns
                } else {
                    &exchange.output_columns
                }
            }
            crate::sql::codegen::FragmentEdgeKind::Stream => &source.output_columns,
        };
        let output_columns = output_columns_for_boundary(edge_output_columns);
        let columns = output_columns_to_boundary_columns(&output_columns);
        reports.push(BoundarySchemaReport {
            fragment_id: Some(edge.source_fragment_id as i32),
            node_id: edge.target_exchange_node_id,
            boundary_kind: BoundaryKind::ExchangeSender,
            columns: columns.clone(),
        });
        reports.push(BoundarySchemaReport {
            fragment_id: Some(edge.target_fragment_id as i32),
            node_id: edge.target_exchange_node_id,
            boundary_kind: BoundaryKind::ExchangeReceiver,
            columns,
        });
    }
    Ok(reports)
}

fn target_exchange_for_edge<'a>(
    fragments_by_id: &'a BTreeMap<FragmentId, &crate::sql::planner::PlanFragment>,
    edge: &crate::sql::codegen::FragmentEdge,
) -> Result<&'a super::kind::DistributedExchangeNode, String> {
    let target_fragment = fragments_by_id
        .get(&edge.target_fragment_id)
        .ok_or_else(|| {
            format!(
                "lower_distributed_plan edge references missing target fragment id={}",
                edge.target_fragment_id
            )
        })?;
    let target_node = find_node_by_id(&target_fragment.root, edge.target_exchange_node_id)
        .ok_or_else(|| {
            format!(
                "lower_distributed_plan edge target_exchange_node_id={} not found in target fragment id={}",
                edge.target_exchange_node_id, target_fragment.fragment_id
            )
        })?;
    let crate::sql::planner::PlanNodeKind::Exchange(exchange) = &target_node.kind else {
        return Err(format!(
            "lower_distributed_plan edge target_exchange_node_id={} in target fragment id={} must target Exchange",
            edge.target_exchange_node_id, target_fragment.fragment_id
        ));
    };
    match (&edge.edge_kind, &exchange.flavor) {
        (
            crate::sql::codegen::FragmentEdgeKind::Stream,
            super::kind::ExchangeFlavor::Distribution,
        )
        | (
            crate::sql::codegen::FragmentEdgeKind::Stream,
            super::kind::ExchangeFlavor::LimitOffset { .. },
        )
        | (
            crate::sql::codegen::FragmentEdgeKind::Stream,
            super::kind::ExchangeFlavor::TopNSplit { .. },
        )
        | (
            crate::sql::codegen::FragmentEdgeKind::CteMulticast { .. },
            super::kind::ExchangeFlavor::CteMulticast { .. },
        ) => Ok(exchange),
        (crate::sql::codegen::FragmentEdgeKind::Stream, _) => Err(format!(
            "lower_distributed_plan stream edge target_exchange_node_id={} in target fragment id={} must target stream Exchange",
            edge.target_exchange_node_id, target_fragment.fragment_id
        )),
        (crate::sql::codegen::FragmentEdgeKind::CteMulticast { .. }, _) => Err(format!(
            "lower_distributed_plan CTE multicast edge target_exchange_node_id={} in target fragment id={} must target Exchange(CteMulticast)",
            edge.target_exchange_node_id, target_fragment.fragment_id
        )),
    }
}

#[derive(Clone, Debug, PartialEq)]
struct AggregateSlotContract {
    data_type: DataType,
    type_desc: types::TTypeDesc,
}

#[derive(Clone)]
pub(in crate::sql::codegen) struct LoweredFragmentOutput {
    scope: ExprScope,
    tuple_ids: Vec<i32>,
    output_columns: Vec<AnalysisOutputColumn>,
    root_node_id: Option<i32>,
    root_node_type: Option<plan_nodes::TPlanNodeType>,
    root_sort_info: Option<plan_nodes::TSortInfo>,
}

fn aggregate_slot_contract_for_phase(
    need_finalize: bool,
    result_type: &DataType,
    intermediate_type: Option<&DataType>,
    display_name: &str,
) -> Result<AggregateSlotContract, String> {
    let data_type = if need_finalize {
        result_type.clone()
    } else {
        intermediate_type
            .cloned()
            .unwrap_or_else(|| result_type.clone())
    };
    let type_desc = type_infer::arrow_type_to_type_desc(&data_type)
        .map_err(|e| format!("aggregate `{display_name}` output type descriptor failed: {e}"))?;
    Ok(AggregateSlotContract {
        data_type,
        type_desc,
    })
}

pub(in crate::sql::codegen) trait LoweringStateAccess<'a> {
    fn connectors(&self) -> &'a crate::connector::ConnectorRegistry;
    fn mv_refresh_ctx(
        &self,
    ) -> Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext>;
    fn desc_builder(&mut self) -> &mut DescriptorTableBuilder;
    fn scan_tables(&mut self) -> &mut Vec<nodes::PlannedScanTable>;
    fn fragment_stack(&self) -> &[FragmentId];
    fn query_global_dicts_per_fragment(
        &mut self,
    ) -> &mut HashMap<FragmentId, Vec<crate::data::TGlobalDict>>;
    fn slot_to_global_dict(&self) -> &HashMap<i32, crate::data::TGlobalDict>;
    fn slot_to_global_dict_mut(&mut self) -> &mut HashMap<i32, crate::data::TGlobalDict>;
    fn rf_probe_targets(&mut self) -> &mut HashMap<i32, RfProbeTarget>;
    fn rf_all_filters(
        &mut self,
    ) -> &mut HashMap<i32, crate::runtime_filter::TRuntimeFilterDescription>;
    fn rf_build_side_filters(&mut self) -> &mut HashMap<FragmentId, Vec<i32>>;
    fn rf_probe_side_filters(&mut self) -> &mut HashMap<FragmentId, Vec<(i32, i32)>>;
    fn scalar_arena(&self) -> &ScalarArena;
    fn alloc_slot(&mut self) -> i32;
    fn slot_allocator(&self) -> expr_compiler::SlotAllocator;
    fn lowered_fragment_output(&self, _fragment_id: FragmentId) -> Option<&LoweredFragmentOutput> {
        None
    }
    fn remember_lowered_fragment_output(
        &mut self,
        _fragment_id: FragmentId,
        _output: LoweredFragmentOutput,
    ) {
    }
    fn ensure_fragment_lowered(&mut self, fragment_id: FragmentId) -> Result<(), String> {
        Err(format!(
            "DistributedPlan Exchange cannot lower source fragment id={} in this lowering context",
            fragment_id
        ))
    }

    fn current_fragment_id(&self) -> Result<FragmentId, String> {
        self.fragment_stack()
            .last()
            .copied()
            .ok_or_else(|| "no active fragment id in lowering state".to_string())
    }

    fn refresh_scan_table_for_codegen(&self, table: &TableDef) -> Result<TableDef, String> {
        refresh_scan_table_for_codegen(self.mv_refresh_ctx(), table)
    }

    fn propagate_dict_to_slot(&mut self, source_slot_id: i32, new_slot_id: i32) {
        if source_slot_id == new_slot_id {
            return;
        }
        let Some(source_dict) = self.slot_to_global_dict().get(&source_slot_id).cloned() else {
            return;
        };
        let new_dict = crate::data::TGlobalDict::new(
            Some(new_slot_id),
            source_dict.strings.clone(),
            source_dict.ids.clone(),
            source_dict.version,
        );
        let fragments: Vec<FragmentId> = if self.fragment_stack().is_empty() {
            self.current_fragment_id()
                .ok()
                .map(|fragment_id| vec![fragment_id])
                .unwrap_or_default()
        } else {
            self.fragment_stack().to_vec()
        };
        for fragment_id in fragments {
            self.query_global_dicts_per_fragment()
                .entry(fragment_id)
                .or_default()
                .push(new_dict.clone());
        }
        self.slot_to_global_dict_mut().insert(new_slot_id, new_dict);
    }
}

pub(crate) struct OwnedLoweringState<'a> {
    connectors: &'a crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    desc_builder: DescriptorTableBuilder,
    scan_tables: Vec<nodes::PlannedScanTable>,
    next_slot_id: Rc<RefCell<i32>>,
    fragment_stack: Vec<FragmentId>,
    query_global_dicts_per_fragment: HashMap<FragmentId, Vec<crate::data::TGlobalDict>>,
    slot_to_global_dict: HashMap<i32, crate::data::TGlobalDict>,
    rf_probe_targets: HashMap<i32, RfProbeTarget>,
    rf_all_filters: HashMap<i32, crate::runtime_filter::TRuntimeFilterDescription>,
    rf_build_side_filters: HashMap<FragmentId, Vec<i32>>,
    rf_probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>>,
    lowered_fragment_outputs: HashMap<FragmentId, LoweredFragmentOutput>,
    fragments_by_id: HashMap<FragmentId, crate::sql::planner::PlanFragment>,
    lowered_fragments: Vec<(crate::sql::planner::PlanFragment, LoweredDistributedNode)>,
    lowering_fragments: BTreeSet<FragmentId>,
    scalar_arena: Arc<ScalarArena>,
}

impl<'a> OwnedLoweringState<'a> {
    #[cfg(test)]
    fn new(
        connectors: &'a crate::connector::ConnectorRegistry,
        mv_refresh_ctx: Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
        _root_fragment_id: FragmentId,
    ) -> Self {
        Self::new_with_fragments(
            connectors,
            mv_refresh_ctx,
            _root_fragment_id,
            &[],
            Arc::new(ScalarArena::new()),
        )
    }

    fn new_with_fragments(
        connectors: &'a crate::connector::ConnectorRegistry,
        mv_refresh_ctx: Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
        _root_fragment_id: FragmentId,
        fragments: &[crate::sql::planner::PlanFragment],
        scalar_arena: Arc<ScalarArena>,
    ) -> Self {
        Self {
            connectors,
            mv_refresh_ctx,
            desc_builder: DescriptorTableBuilder::new(),
            scan_tables: Vec::new(),
            next_slot_id: Rc::new(RefCell::new(1)),
            fragment_stack: Vec::new(),
            query_global_dicts_per_fragment: HashMap::new(),
            slot_to_global_dict: HashMap::new(),
            rf_probe_targets: HashMap::new(),
            rf_all_filters: HashMap::new(),
            rf_build_side_filters: HashMap::new(),
            rf_probe_side_filters: HashMap::new(),
            lowered_fragment_outputs: HashMap::new(),
            fragments_by_id: fragments
                .iter()
                .cloned()
                .map(|fragment| (fragment.fragment_id, fragment))
                .collect(),
            lowered_fragments: Vec::new(),
            lowering_fragments: BTreeSet::new(),
            scalar_arena,
        }
    }

    fn lower_fragment_by_id(&mut self, fragment_id: FragmentId) -> Result<(), String> {
        if self.lowered_fragment_outputs.contains_key(&fragment_id) {
            return Ok(());
        }
        if !self.lowering_fragments.insert(fragment_id) {
            return Err(format!(
                "lower_distributed_plan cycle while lowering fragment id={}",
                fragment_id
            ));
        }

        let fragment = self
            .fragments_by_id
            .get(&fragment_id)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "lower_distributed_plan cannot lower missing fragment id={}",
                    fragment_id
                )
            })?;
        self.fragment_stack.push(fragment_id);
        let lowered_result = {
            let mut ctx = LoweringCtx::new(self);
            ctx.lower_node(&fragment.root)
        };
        self.fragment_stack.pop();
        let lowered = match lowered_result {
            Ok(lowered) => lowered,
            Err(err) => {
                self.lowering_fragments.remove(&fragment_id);
                return Err(err);
            }
        };

        let root_plan_node = lowered.plan_nodes.first();
        self.remember_lowered_fragment_output(
            fragment_id,
            LoweredFragmentOutput {
                scope: lowered.scope.clone(),
                tuple_ids: lowered.tuple_ids.clone(),
                output_columns: lowered.output_columns.clone(),
                root_node_id: root_plan_node.map(|node| node.node_id),
                root_node_type: root_plan_node.map(|node| node.node_type),
                root_sort_info: root_plan_node
                    .and_then(|node| node.sort_node.as_ref())
                    .map(|sort| sort.sort_info.clone()),
            },
        );
        self.lowered_fragments.push((fragment, lowered));
        self.lowering_fragments.remove(&fragment_id);
        Ok(())
    }
}

impl<'a> LoweringStateAccess<'a> for OwnedLoweringState<'a> {
    fn connectors(&self) -> &'a crate::connector::ConnectorRegistry {
        self.connectors
    }

    fn mv_refresh_ctx(
        &self,
    ) -> Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext> {
        self.mv_refresh_ctx
    }

    fn desc_builder(&mut self) -> &mut DescriptorTableBuilder {
        &mut self.desc_builder
    }

    fn scan_tables(&mut self) -> &mut Vec<nodes::PlannedScanTable> {
        &mut self.scan_tables
    }

    fn fragment_stack(&self) -> &[FragmentId] {
        &self.fragment_stack
    }

    fn query_global_dicts_per_fragment(
        &mut self,
    ) -> &mut HashMap<FragmentId, Vec<crate::data::TGlobalDict>> {
        &mut self.query_global_dicts_per_fragment
    }

    fn slot_to_global_dict(&self) -> &HashMap<i32, crate::data::TGlobalDict> {
        &self.slot_to_global_dict
    }

    fn slot_to_global_dict_mut(&mut self) -> &mut HashMap<i32, crate::data::TGlobalDict> {
        &mut self.slot_to_global_dict
    }

    fn rf_probe_targets(&mut self) -> &mut HashMap<i32, RfProbeTarget> {
        &mut self.rf_probe_targets
    }

    fn rf_all_filters(
        &mut self,
    ) -> &mut HashMap<i32, crate::runtime_filter::TRuntimeFilterDescription> {
        &mut self.rf_all_filters
    }

    fn rf_build_side_filters(&mut self) -> &mut HashMap<FragmentId, Vec<i32>> {
        &mut self.rf_build_side_filters
    }

    fn rf_probe_side_filters(&mut self) -> &mut HashMap<FragmentId, Vec<(i32, i32)>> {
        &mut self.rf_probe_side_filters
    }

    fn scalar_arena(&self) -> &ScalarArena {
        &self.scalar_arena
    }

    fn alloc_slot(&mut self) -> i32 {
        let mut next = self.next_slot_id.borrow_mut();
        let slot_id = *next;
        *next += 1;
        slot_id
    }

    fn slot_allocator(&self) -> expr_compiler::SlotAllocator {
        Rc::clone(&self.next_slot_id)
    }

    fn lowered_fragment_output(&self, fragment_id: FragmentId) -> Option<&LoweredFragmentOutput> {
        self.lowered_fragment_outputs.get(&fragment_id)
    }

    fn remember_lowered_fragment_output(
        &mut self,
        fragment_id: FragmentId,
        output: LoweredFragmentOutput,
    ) {
        self.lowered_fragment_outputs.insert(fragment_id, output);
    }

    fn ensure_fragment_lowered(&mut self, fragment_id: FragmentId) -> Result<(), String> {
        self.lower_fragment_by_id(fragment_id)
    }
}

pub(in crate::sql::codegen) struct LoweringCtx<'s, 'a, S: LoweringStateAccess<'a> + ?Sized> {
    state: &'s mut S,
    _marker: std::marker::PhantomData<&'a ()>,
}

struct LoweredDistributedNode {
    plan_nodes: Vec<plan_nodes::TPlanNode>,
    scope: ExprScope,
    tuple_ids: Vec<i32>,
    #[allow(dead_code)]
    output_columns: Vec<AnalysisOutputColumn>,
    ordering: OrderingSpec,
}

impl<'s, 'a, S: LoweringStateAccess<'a> + ?Sized> LoweringCtx<'s, 'a, S> {
    pub(crate) fn new(state: &'s mut S) -> Self {
        Self {
            state,
            _marker: std::marker::PhantomData,
        }
    }

    fn lower_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
    ) -> Result<LoweredDistributedNode, String> {
        let mut lowered = match &node.kind {
            crate::sql::planner::PlanNodeKind::Scan(scan) => self.lower_scan_node(node, scan)?,
            crate::sql::planner::PlanNodeKind::Project(project) => {
                self.lower_project_node(node, project)?
            }
            crate::sql::planner::PlanNodeKind::Filter(filter) => {
                self.lower_filter_node(node, filter)?
            }
            crate::sql::planner::PlanNodeKind::Sort(sort) => self.lower_sort_node(node, sort)?,
            crate::sql::planner::PlanNodeKind::TopN(topn) => self.lower_topn_node(node, topn)?,
            crate::sql::planner::PlanNodeKind::Exchange(exchange) => {
                self.lower_exchange_node(node, exchange)?
            }
            crate::sql::planner::PlanNodeKind::HashAggregate(agg) => {
                self.lower_hash_aggregate_node(node, agg.as_ref())?
            }
            crate::sql::planner::PlanNodeKind::HashJoin(hash_join) => {
                self.lower_hash_join_node(node, hash_join.as_ref())?
            }
            crate::sql::planner::PlanNodeKind::NestLoopJoin(nest_loop) => {
                self.lower_nest_loop_join_node(node, nest_loop)?
            }
            crate::sql::planner::PlanNodeKind::Values(values) => {
                self.lower_values_node(node, values)?
            }
            crate::sql::planner::PlanNodeKind::AssertOneRow(assert_one_row) => {
                self.lower_assert_one_row_node(node, assert_one_row)?
            }
            crate::sql::planner::PlanNodeKind::Decode(decode) => {
                self.lower_decode_node(node, decode)?
            }
            crate::sql::planner::PlanNodeKind::Repeat(repeat) => {
                self.lower_repeat_node(node, repeat)?
            }
            crate::sql::planner::PlanNodeKind::SetOp(set_op) => {
                self.lower_set_op_node(node, set_op)?
            }
            crate::sql::planner::PlanNodeKind::Window(window) => {
                self.lower_window_node(node, window)?
            }
            crate::sql::planner::PlanNodeKind::GenerateSeries(generate_series) => {
                self.lower_generate_series_node(node, generate_series)?
            }
            crate::sql::planner::PlanNodeKind::TableFunction(table_function) => {
                self.lower_table_function_node(node, table_function)?
            }
            crate::sql::planner::PlanNodeKind::Limit(_)
            | crate::sql::planner::PlanNodeKind::Aggregate(_)
            | crate::sql::planner::PlanNodeKind::Join(_)
            | crate::sql::planner::PlanNodeKind::Union(_)
            | crate::sql::planner::PlanNodeKind::Intersect(_)
            | crate::sql::planner::PlanNodeKind::Except(_)
            | crate::sql::planner::PlanNodeKind::CTEAnchor(_)
            | crate::sql::planner::PlanNodeKind::CTEProduce(_)
            | crate::sql::planner::PlanNodeKind::CTEConsume(_)
            | crate::sql::planner::PlanNodeKind::AggregateStateMerge(_)
            | crate::sql::planner::PlanNodeKind::Apply(_)
            | crate::sql::planner::PlanNodeKind::ImvDelta(_)
            | crate::sql::planner::PlanNodeKind::ImvVersion(_) => {
                return Err(format!(
                    "logical plan node {} leaked into distributed lowering",
                    node.kind.variant_name()
                ));
            }
        };
        if let Some(root) = lowered.plan_nodes.first_mut() {
            root.limit = node.limit;
        }
        self.record_probe_targets(node, &lowered);
        Ok(lowered)
    }

    fn lower_scan_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        scan: &super::kind::DistributedScanNode,
    ) -> Result<LoweredDistributedNode, String> {
        if !node.children.is_empty() {
            return Err(format!(
                "DistributedPlan Scan node_id={} expected 0 children, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let scan_tuple_id = first_tuple_id(node, "Scan")?;
        let (scan_plan_node, scope) = self.lower_scan(node.node_id, scan_tuple_id, scan)?;
        Ok(LoweredDistributedNode {
            plan_nodes: vec![scan_plan_node],
            scope,
            tuple_ids: vec![scan_tuple_id],
            output_columns: scan.columns.clone(),
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_project_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        project: &super::kind::DistributedProjectNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan Project node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let child = self.lower_node(&node.children[0])?;
        let project_tuple_id = first_tuple_id(node, "Project")?;
        let (project_plan_node, scope, _output_columns) =
            self.lower_project(node.node_id, project_tuple_id, project, &child.scope)?;
        let mut plan_nodes = vec![project_plan_node];
        plan_nodes.extend(child.plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids: vec![project_tuple_id],
            output_columns: project_node_output_columns(project),
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_filter_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        filter: &super::kind::DistributedFilterNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan Filter node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let mut child = self.lower_node(&node.children[0])?;
        let conjunct_refs = split_and_conjuncts_typed(&filter.predicate);
        let mut conjuncts = Vec::with_capacity(conjunct_refs.len());
        let mut compiler = ExprCompiler::new(self.state.slot_allocator(), &child.scope);
        for conjunct in conjunct_refs {
            conjuncts.push(compiler.compile_typed(conjunct)?);
        }

        if !conjuncts.is_empty() {
            if let Some(first_node) = child.plan_nodes.first_mut() {
                let node_id = first_node.node_id;
                let extra_conjuncts = conjuncts.clone();
                first_node
                    .conjuncts
                    .get_or_insert_with(Vec::new)
                    .extend(conjuncts);
                nodes::append_hdfs_scan_min_max_conjuncts(first_node, &extra_conjuncts);
                if let Some(planned) = self
                    .state
                    .scan_tables()
                    .iter_mut()
                    .find(|planned| planned.scan_node_id == node_id)
                {
                    planned.min_max_conjuncts.extend(extra_conjuncts);
                }
            }
        }

        Ok(LoweredDistributedNode {
            plan_nodes: child.plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            output_columns: child.output_columns,
            ordering: child.ordering,
        })
    }

    fn lower_sort_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        sort: &super::kind::DistributedSortNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan Sort node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let child = self.lower_node(&node.children[0])?;
        let sort_plan_node = self.lower_sort(
            node.node_id,
            sort,
            &child.scope,
            &child.tuple_ids,
            &sort.output_columns,
            sort.offset,
        )?;
        let mut plan_nodes = vec![sort_plan_node];
        plan_nodes.extend(child.plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            output_columns: sort.output_columns.clone(),
            ordering: ordering_spec_from_sort_items(&sort.items),
        })
    }

    fn lower_topn_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        topn: &super::kind::DistributedTopNNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan TopN node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let child = self.lower_node(&node.children[0])?;
        let top_n_plan_node =
            self.lower_top_n_single_or_partial(node.node_id, topn, &child.scope, &child.tuple_ids)?;
        let mut plan_nodes = vec![top_n_plan_node];
        plan_nodes.extend(child.plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            output_columns: child.output_columns,
            ordering: ordering_spec_from_sort_items(&topn.items),
        })
    }

    fn lower_exchange_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        exchange: &super::kind::DistributedExchangeNode,
    ) -> Result<LoweredDistributedNode, String> {
        if !node.children.is_empty() {
            return Err(format!(
                "DistributedPlan Exchange node_id={} expected 0 children, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        match &exchange.flavor {
            super::kind::ExchangeFlavor::Distribution => {
                let source = self.lower_stream_exchange_source(node, exchange)?;
                Ok(LoweredDistributedNode {
                    plan_nodes: vec![nodes::build_exchange_node(
                        node.node_id,
                        source.tuple_ids.clone(),
                        exchange.partition_type,
                    )],
                    scope: source.scope,
                    tuple_ids: source.tuple_ids,
                    output_columns: source.output_columns,
                    ordering: OrderingSpec::Any,
                })
            }
            super::kind::ExchangeFlavor::LimitOffset { limit, offset } => {
                let source = self.lower_stream_exchange_source(node, exchange)?;
                Ok(LoweredDistributedNode {
                    plan_nodes: vec![nodes::build_limit_exchange_node(
                        node.node_id,
                        source.tuple_ids.clone(),
                        exchange.partition_type,
                        *limit,
                        *offset,
                    )],
                    scope: source.scope,
                    tuple_ids: source.tuple_ids,
                    output_columns: source.output_columns,
                    ordering: OrderingSpec::Any,
                })
            }
            super::kind::ExchangeFlavor::TopNSplit {
                items,
                limit,
                offset,
            } => {
                let source = self.lower_stream_exchange_source(node, exchange)?;
                let partial_sort_info = source.root_sort_info.clone().ok_or_else(|| {
                    let got = source
                        .root_node_type
                        .map(|node_type| format!("{node_type:?}"))
                        .unwrap_or_else(|| "<empty>".to_string());
                    format!(
                        "FINAL+split TopN (node_id={}): expected PARTIAL child's root to be SORT_NODE, got {}",
                        source.root_node_id.unwrap_or(-1),
                        got
                    )
                })?;
                Ok(LoweredDistributedNode {
                    plan_nodes: vec![nodes::build_merging_exchange_node(
                        node.node_id,
                        source.tuple_ids.clone(),
                        exchange.partition_type,
                        partial_sort_info,
                        *limit,
                        *offset,
                    )],
                    scope: source.scope,
                    tuple_ids: source.tuple_ids,
                    output_columns: source.output_columns,
                    ordering: ordering_spec_from_sort_items(items),
                })
            }
            super::kind::ExchangeFlavor::CteMulticast { .. } => {
                if self
                    .state
                    .lowered_fragment_output(exchange.source_fragment_id)
                    .is_none()
                {
                    self.state
                        .ensure_fragment_lowered(exchange.source_fragment_id)?;
                }
                let exchange_tuple_id = first_tuple_id(node, "Exchange")?;
                let (scope, output_columns) = self.lower_cte_multicast_exchange_scope(
                    exchange_tuple_id,
                    node.node_id,
                    exchange,
                )?;
                Ok(LoweredDistributedNode {
                    plan_nodes: vec![nodes::build_exchange_node(
                        node.node_id,
                        vec![exchange_tuple_id],
                        exchange.partition_type,
                    )],
                    scope,
                    tuple_ids: vec![exchange_tuple_id],
                    output_columns,
                    ordering: OrderingSpec::Any,
                })
            }
        }
    }

    fn lower_hash_aggregate_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        agg: &super::kind::DistributedHashAggregateNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan HashAggregate node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let child = self.lower_node(&node.children[0])?;
        let agg_tuple_id = first_tuple_id(node, "HashAggregate")?;
        let (agg_plan_node, scope) =
            self.lower_hash_aggregate(node.node_id, agg_tuple_id, agg, &child.scope)?;
        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids: vec![agg_tuple_id],
            output_columns: agg.output_columns.clone(),
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_hash_join_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        hash_join: &super::kind::DistributedHashJoinNode,
    ) -> Result<LoweredDistributedNode, String> {
        let (left_node, right_node) = binary_children(node, "HashJoin")?;
        let left = self.lower_node(left_node)?;
        let right = self.lower_node(right_node)?;
        let LoweredDistributedNode {
            plan_nodes: left_plan_nodes,
            scope: left_scope,
            tuple_ids: left_tuple_ids,
            ..
        } = left;
        let LoweredDistributedNode {
            plan_nodes: right_plan_nodes,
            scope: right_scope,
            tuple_ids: right_tuple_ids,
            ..
        } = right;
        let (join_plan_node, scope, tuple_ids) = self.lower_hash_join(
            node.node_id,
            &left_tuple_ids,
            &right_tuple_ids,
            hash_join,
            left_scope,
            right_scope,
            node.execution_join_distribution,
            &node.build_runtime_filters,
        )?;
        let mut plan_nodes = vec![join_plan_node];
        plan_nodes.extend(left_plan_nodes);
        plan_nodes.extend(right_plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids,
            output_columns: Vec::new(),
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_nest_loop_join_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        nest_loop: &super::kind::DistributedNestLoopJoinNode,
    ) -> Result<LoweredDistributedNode, String> {
        let (left_node, right_node) = binary_children(node, "NestLoopJoin")?;
        let left = self.lower_node(left_node)?;
        let right = self.lower_node(right_node)?;
        let LoweredDistributedNode {
            plan_nodes: left_plan_nodes,
            scope: left_scope,
            tuple_ids: left_tuple_ids,
            ..
        } = left;
        let LoweredDistributedNode {
            plan_nodes: right_plan_nodes,
            scope: right_scope,
            tuple_ids: right_tuple_ids,
            ..
        } = right;
        let (join_plan_node, scope, tuple_ids) = self.lower_nest_loop_join(
            node.node_id,
            &left_tuple_ids,
            &right_tuple_ids,
            nest_loop,
            left_scope,
            right_scope,
        )?;
        let mut plan_nodes = vec![join_plan_node];
        plan_nodes.extend(left_plan_nodes);
        plan_nodes.extend(right_plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids,
            output_columns: Vec::new(),
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_values_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        values: &super::kind::DistributedValuesNode,
    ) -> Result<LoweredDistributedNode, String> {
        if !node.children.is_empty() {
            return Err(format!(
                "DistributedPlan Values node_id={} expected 0 children, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let tuple_id = first_tuple_id(node, "Values")?;
        let (plan_node, scope) = self.lower_values(node.node_id, tuple_id, values)?;
        Ok(LoweredDistributedNode {
            plan_nodes: vec![plan_node],
            scope,
            tuple_ids: vec![tuple_id],
            output_columns: values.columns.clone(),
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_assert_one_row_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        assert_one_row: &super::kind::DistributedAssertOneRowNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan AssertOneRow node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let child = self.lower_node(&node.children[0])?;
        let op = assert_one_row_node_to_physical_op(assert_one_row);
        let plan_node = self.lower_assert_one_row(node.node_id, &op, &child.tuple_ids);
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            output_columns: child.output_columns,
            ordering: child.ordering,
        })
    }

    fn lower_decode_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        decode: &super::kind::DistributedDecodeNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan Decode node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let child = self.lower_node(&node.children[0])?;
        let tuple_id = first_tuple_id(node, "Decode")?;
        let op = decode_node_to_physical_op(decode);
        let (plan_node, scope) = self.lower_decode(node.node_id, tuple_id, &op, &child.scope)?;
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids: vec![tuple_id],
            output_columns: decode.output_columns.clone(),
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_repeat_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        repeat: &super::kind::DistributedRepeatNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan Repeat node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let child = self.lower_node(&node.children[0])?;
        let op = repeat_node_to_physical_op(repeat);
        let virtual_tuple_id = repeat.virtual_tuple_id.ok_or_else(|| {
            "distributed Repeat node missing virtual_tuple_id during lowering".to_string()
        })?;
        let (plan_node, scope, tuple_ids, output_columns) = self.lower_repeat(
            node.node_id,
            virtual_tuple_id,
            &op,
            child.scope,
            child.tuple_ids,
            child.output_columns,
        )?;
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids,
            output_columns,
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_set_op_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        set_op: &super::kind::DistributedSetOpNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.is_empty() {
            return Err("DistributedPlan SetOp has no inputs".to_string());
        }
        if !set_op.child_output_columns.is_empty()
            && set_op.child_output_columns.len() != node.children.len()
        {
            return Err(format!(
                "DistributedPlan SetOp node_id={} child_output_columns has {}, children has {}",
                node.node_id,
                set_op.child_output_columns.len(),
                node.children.len()
            ));
        }
        let mut children = Vec::with_capacity(node.children.len());
        for child_node in &node.children {
            children.push(self.lower_node(child_node)?);
        }
        let tuple_id = first_tuple_id(node, "SetOp")?;
        let (plan_node, scope) = self.lower_set_op(
            node.node_id,
            tuple_id,
            set_op.kind,
            &set_op.output_columns,
            &set_op.child_output_columns,
            &children,
        )?;
        let mut plan_nodes = vec![plan_node];
        for child in children {
            plan_nodes.extend(child.plan_nodes);
        }
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids: vec![tuple_id],
            output_columns: set_op.output_columns.clone(),
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_window_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        window: &super::kind::DistributedWindowNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan Window node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let child = self.lower_node(&node.children[0])?;
        let (mut plan_nodes, scope, tuple_ids, ordering) = self.lower_window(
            node.node_id,
            &node.tuple_ids,
            window,
            &child.scope,
            &child.tuple_ids,
            &child.ordering,
        )?;
        plan_nodes.extend(child.plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids,
            output_columns: window.output_columns.clone(),
            ordering,
        })
    }

    fn lower_generate_series_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        generate_series: &super::kind::DistributedGenerateSeriesNode,
    ) -> Result<LoweredDistributedNode, String> {
        if !node.children.is_empty() {
            return Err(format!(
                "DistributedPlan GenerateSeries node_id={} expected 0 children, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let output_tuple_id = first_tuple_id(node, "GenerateSeries")?;
        let op = generate_series_node_to_physical_op(generate_series);
        let (plan_nodes, scope) = self.lower_generate_series(node.node_id, output_tuple_id, &op)?;
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids: vec![output_tuple_id],
            output_columns: vec![AnalysisOutputColumn {
                column_id: generate_series.output_column_id,
                name: generate_series.column_name.clone(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_table_function_node(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        table_function: &super::kind::DistributedTableFunctionNode,
    ) -> Result<LoweredDistributedNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "DistributedPlan TableFunction node_id={} expected 1 child, got {}",
                node.node_id,
                node.children.len()
            ));
        }
        let child = self.lower_node(&node.children[0])?;
        let output_tuple_id = first_tuple_id(node, "TableFunction")?;
        let (table_fn_nodes, scope) =
            self.lower_table_function(node.node_id, output_tuple_id, table_function, &child.scope)?;
        let mut plan_nodes = table_fn_nodes;
        plan_nodes.extend(child.plan_nodes);
        Ok(LoweredDistributedNode {
            plan_nodes,
            scope,
            tuple_ids: vec![output_tuple_id],
            output_columns: table_function.output_columns.clone(),
            ordering: OrderingSpec::Any,
        })
    }

    fn lower_stream_exchange_source(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        exchange: &super::kind::DistributedExchangeNode,
    ) -> Result<LoweredFragmentOutput, String> {
        if self
            .state
            .lowered_fragment_output(exchange.source_fragment_id)
            .is_none()
        {
            self.state
                .ensure_fragment_lowered(exchange.source_fragment_id)?;
        }
        let source = self
            .state
            .lowered_fragment_output(exchange.source_fragment_id)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "DistributedPlan Exchange node_id={} references source fragment id={} before it was lowered",
                    node.node_id, exchange.source_fragment_id
                )
            })?;
        if source.tuple_ids != node.tuple_ids {
            return Err(format!(
                "DistributedPlan Exchange node_id={} tuple_ids {:?} do not match source fragment id={} tuple_ids {:?}",
                node.node_id, node.tuple_ids, exchange.source_fragment_id, source.tuple_ids
            ));
        }
        Ok(source)
    }

    fn lower_cte_multicast_exchange_scope(
        &mut self,
        exchange_tuple_id: i32,
        exchange_node_id: i32,
        exchange: &super::kind::DistributedExchangeNode,
    ) -> Result<(ExprScope, Vec<AnalysisOutputColumn>), String> {
        if exchange.output_columns.is_empty() {
            return Err(format!(
                "DistributedPlan CTE multicast Exchange node_id={} has no output columns",
                exchange_node_id
            ));
        }
        let mut scope = ExprScope::new();
        for (idx, col) in exchange.output_columns.iter().enumerate() {
            let slot_id = self.state.alloc_slot();
            self.state.desc_builder().add_slot(
                slot_id,
                exchange_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: exchange_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: None,
                nullable: col.nullable,
            };
            scope.add_column_with_id(
                col.column_id,
                exchange.output_qualifier.clone(),
                col.name.clone(),
                binding,
            );
        }
        self.state.desc_builder().add_tuple(exchange_tuple_id, None);
        Ok((scope, exchange.output_columns.clone()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_hash_join(
        &mut self,
        join_node_id: i32,
        left_tuple_ids: &[i32],
        right_tuple_ids: &[i32],
        op: &super::kind::DistributedHashJoinNode,
        left_scope: ExprScope,
        right_scope: ExprScope,
        execution_distribution: Option<JoinExecutionDistribution>,
        build_runtime_filters: &[crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc],
    ) -> Result<(plan_nodes::TPlanNode, ExprScope, Vec<i32>), String> {
        let join_op = join_kind_to_op(op.join_type);

        let mut eq_join_conjuncts = Vec::new();
        let mut demoted_eq_exprs: Vec<TypedExpr> = Vec::new();
        let mut surviving_eq_origin: Vec<usize> = Vec::new();
        let mut surviving_eq_build_exprs: Vec<TypedExpr> = Vec::new();
        for (eq_index, eq) in op.eq_conditions.iter().enumerate() {
            let expr_a = &eq.left;
            let expr_b = &eq.right;
            let natural = ExprCompiler::new_strict_id(self.state.slot_allocator(), &left_scope)
                .compile_typed(expr_a)
                .ok()
                .and_then(|lt| {
                    ExprCompiler::new_strict_id(self.state.slot_allocator(), &right_scope)
                        .compile_typed(expr_b)
                        .ok()
                        .map(|rt| (lt, rt, expr_b.clone()))
                });
            let result = natural.or_else(|| {
                ExprCompiler::new_strict_id(self.state.slot_allocator(), &left_scope)
                    .compile_typed(expr_b)
                    .ok()
                    .and_then(|lt| {
                        ExprCompiler::new_strict_id(self.state.slot_allocator(), &right_scope)
                            .compile_typed(expr_a)
                            .ok()
                            .map(|rt| (lt, rt, expr_a.clone()))
                    })
            });
            if let Some((lt, rt, build_expr)) = result {
                eq_join_conjuncts.push(plan_nodes::TEqJoinCondition {
                    left: lt,
                    right: rt,
                    opcode: Some(if eq.null_safe {
                        crate::opcodes::TExprOpcode::EQ_FOR_NULL
                    } else {
                        crate::opcodes::TExprOpcode::EQ
                    }),
                });
                surviving_eq_origin.push(eq_index);
                surviving_eq_build_exprs.push(build_expr);
            } else {
                demoted_eq_exprs.push(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(expr_a.clone()),
                        op: if eq.null_safe {
                            BinOp::EqForNull
                        } else {
                            BinOp::Eq
                        },
                        right: Box::new(expr_b.clone()),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                });
            }
        }

        let mut other_join_conjuncts = Vec::new();
        {
            let mut merged = ExprScope::new();
            merged.merge(&left_scope);
            merged.merge(&right_scope);
            let mut compiler = ExprCompiler::new(self.state.slot_allocator(), &merged);
            for demoted in &demoted_eq_exprs {
                other_join_conjuncts.push(compiler.compile_typed(demoted)?);
            }
            if let Some(ref cond) = op.other_condition {
                other_join_conjuncts.push(compiler.compile_typed(cond)?);
            }
        }

        let distribution_mode =
            join_distribution_mode_from_execution(execution_distribution, &op.distribution);
        let mut join_plan_node = nodes::build_hash_join_node(
            join_node_id,
            left_tuple_ids,
            right_tuple_ids,
            join_op,
            distribution_mode,
            eq_join_conjuncts,
            other_join_conjuncts,
        );

        let rf_descs = self.build_rf_descriptors(
            build_runtime_filters,
            join_node_id,
            &right_scope,
            &surviving_eq_origin,
            &surviving_eq_build_exprs,
            execution_distribution,
        )?;
        if !rf_descs.is_empty()
            && let Some(hj) = join_plan_node.hash_join_node.as_mut()
        {
            hj.build_runtime_filters = Some(rf_descs);
        }

        self.widen_hash_join_nullable_tuples(op.join_type, left_tuple_ids, right_tuple_ids);

        let mut merged_tuple_ids = left_tuple_ids.to_vec();
        merged_tuple_ids.extend_from_slice(right_tuple_ids);

        let merged_scope = match op.join_type {
            JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left_scope,
            JoinKind::RightSemi | JoinKind::RightAnti => right_scope,
            _ => {
                let mut scope = left_scope;
                scope.merge(&right_scope);
                scope
            }
        };

        Ok((join_plan_node, merged_scope, merged_tuple_ids))
    }

    pub(crate) fn lower_nest_loop_join(
        &mut self,
        join_node_id: i32,
        left_tuple_ids: &[i32],
        right_tuple_ids: &[i32],
        op: &super::kind::DistributedNestLoopJoinNode,
        left_scope: ExprScope,
        right_scope: ExprScope,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope, Vec<i32>), String> {
        let join_op = join_kind_to_op(op.join_type);

        let join_conjuncts = if let Some(ref cond) = op.condition {
            let mut merged = ExprScope::new();
            merged.merge(&left_scope);
            merged.merge(&right_scope);
            let conjuncts = split_and_conjuncts_typed(cond);
            let mut results = Vec::new();
            for conj in conjuncts {
                let mut compiler = ExprCompiler::new(self.state.slot_allocator(), &merged);
                results.push(compiler.compile_typed(conj)?);
            }
            results
        } else {
            Vec::new()
        };

        let join_plan_node = nodes::build_nestloop_join_node(
            join_node_id,
            left_tuple_ids,
            right_tuple_ids,
            join_op,
            join_conjuncts,
        );

        self.widen_nest_loop_join_nullable_tuples(op.join_type, left_tuple_ids, right_tuple_ids);

        let mut merged_tuple_ids = left_tuple_ids.to_vec();
        merged_tuple_ids.extend_from_slice(right_tuple_ids);

        let merged_scope = match op.join_type {
            JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left_scope,
            JoinKind::RightSemi | JoinKind::RightAnti => right_scope,
            _ => {
                let mut scope = left_scope;
                scope.merge(&right_scope);
                scope
            }
        };

        Ok((join_plan_node, merged_scope, merged_tuple_ids))
    }

    fn widen_hash_join_nullable_tuples(
        &mut self,
        join_type: JoinKind,
        left_tuple_ids: &[i32],
        right_tuple_ids: &[i32],
    ) {
        match join_type {
            JoinKind::LeftOuter
            | JoinKind::LeftAnti
            | JoinKind::LeftSemi
            | JoinKind::NullAwareLeftAnti => {
                for &tid in right_tuple_ids {
                    self.state.desc_builder().widen_tuple_nullable(tid);
                }
            }
            JoinKind::RightOuter | JoinKind::RightAnti | JoinKind::RightSemi => {
                for &tid in left_tuple_ids {
                    self.state.desc_builder().widen_tuple_nullable(tid);
                }
            }
            JoinKind::FullOuter => {
                for &tid in left_tuple_ids {
                    self.state.desc_builder().widen_tuple_nullable(tid);
                }
                for &tid in right_tuple_ids {
                    self.state.desc_builder().widen_tuple_nullable(tid);
                }
            }
            _ => {}
        }
    }

    fn widen_nest_loop_join_nullable_tuples(
        &mut self,
        join_type: JoinKind,
        left_tuple_ids: &[i32],
        right_tuple_ids: &[i32],
    ) {
        match join_type {
            JoinKind::LeftOuter | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
                for &tid in right_tuple_ids {
                    self.state.desc_builder().widen_tuple_nullable(tid);
                }
            }
            JoinKind::RightOuter | JoinKind::RightAnti => {
                for &tid in left_tuple_ids {
                    self.state.desc_builder().widen_tuple_nullable(tid);
                }
            }
            JoinKind::FullOuter => {
                for &tid in left_tuple_ids {
                    self.state.desc_builder().widen_tuple_nullable(tid);
                }
                for &tid in right_tuple_ids {
                    self.state.desc_builder().widen_tuple_nullable(tid);
                }
            }
            _ => {}
        }
    }

    fn record_probe_targets(
        &mut self,
        node: &crate::sql::planner::DistributedPlanNode,
        result: &LoweredDistributedNode,
    ) {
        if node.probe_runtime_filters.is_empty() {
            return;
        }
        let Some(target_node) = result.plan_nodes.first() else {
            return;
        };
        let thrift_node_id = target_node.node_id;
        let Ok(fragment_id) = self.state.current_fragment_id() else {
            return;
        };
        for probe in &node.probe_runtime_filters {
            let probe_expr = materialize(self.state.scalar_arena(), probe.probe_expr);
            let mut compiler = ExprCompiler::new(self.state.slot_allocator(), &result.scope);
            let Ok(probe_texpr) = compiler.compile_typed(&probe_expr) else {
                continue;
            };
            self.state.rf_probe_targets().insert(
                probe.filter_id,
                RfProbeTarget {
                    thrift_node_id,
                    probe_texpr,
                    fragment_id,
                },
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_rf_descriptors(
        &mut self,
        build_runtime_filters: &[crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc],
        join_node_id: i32,
        build_scope: &ExprScope,
        surviving_eq_origin: &[usize],
        surviving_eq_build_exprs: &[TypedExpr],
        execution_distribution: Option<JoinExecutionDistribution>,
    ) -> Result<Vec<crate::runtime_filter::TRuntimeFilterDescription>, String> {
        use crate::runtime_filter;

        if build_runtime_filters.is_empty() {
            return Ok(Vec::new());
        }

        let pipeline_dop = rf_pipeline_dop();
        let join_fragment = self.state.current_fragment_id()?;
        let mut descs: Vec<runtime_filter::TRuntimeFilterDescription> =
            Vec::with_capacity(build_runtime_filters.len());

        for rf in build_runtime_filters {
            let filter_id = rf.filter_id;
            let Some(post_demote_expr_order) =
                remap_rf_expr_order(surviving_eq_origin, rf.expr_order)
            else {
                continue;
            };
            if post_demote_expr_order >= surviving_eq_origin.len() {
                continue;
            }
            let Some(expected_build_expr) = surviving_eq_build_exprs.get(post_demote_expr_order)
            else {
                continue;
            };
            let build_expr = materialize(self.state.scalar_arena(), rf.build_expr);
            if !rf_build_expr_matches_join_build_expr(&build_expr, expected_build_expr) {
                tracing::debug!(
                    "skip runtime filter {filter_id}: build expr does not match join build key"
                );
                continue;
            }

            let build_texpr = match ExprCompiler::new(self.state.slot_allocator(), build_scope)
                .compile_typed(&build_expr)
            {
                Ok(t) => t,
                Err(err) => {
                    tracing::debug!(
                        "skip runtime filter {filter_id}: build expr does not bind build scope: {err}"
                    );
                    continue;
                }
            };

            let probe_target = self.state.rf_probe_targets().get(&filter_id).cloned();
            let has_remote_targets = probe_target
                .as_ref()
                .map(|t| t.fragment_id != join_fragment)
                .unwrap_or(false);

            let effective_distribution = execution_distribution
                .unwrap_or_else(|| legacy_rf_distribution_to_execution(&rf.distribution));
            let (build_join_mode, local_layout, global_layout) =
                rf_layout_for_execution_distribution(effective_distribution);

            let layout = runtime_filter::TRuntimeFilterLayout::new(
                filter_id,
                local_layout,
                global_layout,
                false,
                1_i32,
                pipeline_dop,
                None::<Vec<i32>>,
                None::<Vec<i32>>,
                None::<Vec<i32>>,
                None::<Vec<crate::partitions::TBucketProperty>>,
            );

            let mut target_map = BTreeMap::new();
            if let Some(target) = &probe_target {
                target_map.insert(target.thrift_node_id, target.probe_texpr.clone());
            }

            let desc = runtime_filter::TRuntimeFilterDescription::new(
                filter_id,
                build_texpr,
                post_demote_expr_order as i32,
                target_map,
                has_remote_targets,
                None::<i64>,
                None::<Vec<crate::types::TNetworkAddress>>,
                build_join_mode,
                None::<crate::types::TUniqueId>,
                join_node_id,
                None::<Vec<crate::types::TUniqueId>>,
                None::<Vec<runtime_filter::TRuntimeFilterDestination>>,
                None::<Vec<i32>>,
                None::<BTreeMap<i32, Vec<exprs::TExpr>>>,
                runtime_filter::TRuntimeFilterBuildType::JOIN_FILTER,
                layout,
                None::<bool>,
                None::<bool>,
                None::<i32>,
                None::<bool>,
                None::<bool>,
                None::<i64>,
            );

            descs.push(desc.clone());
            self.state.rf_all_filters().insert(filter_id, desc);
            self.state
                .rf_build_side_filters()
                .entry(join_fragment)
                .or_default()
                .push(filter_id);
            if let Some(target) = &probe_target {
                self.state
                    .rf_probe_side_filters()
                    .entry(target.fragment_id)
                    .or_default()
                    .push((filter_id, target.thrift_node_id));
            }
        }

        Ok(descs)
    }

    pub(crate) fn lower_scan(
        &mut self,
        scan_node_id: i32,
        scan_tuple_id: i32,
        op: &super::kind::DistributedScanNode,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope), String> {
        let state = &mut *self.state;
        let table = state.refresh_scan_table_for_codegen(&op.table)?;

        let mut scope = ExprScope::new();
        let qualifier = op.alias.as_deref().or(Some(&table.name));
        let mut slot_to_column = HashMap::new();
        let mut iceberg_metadata_pseudo_column_slots = BTreeSet::new();

        // Determine which columns to emit
        let planned_scan = match &table.source {
            crate::sql::catalog::ScanSource::StarRocks { db_id, table_id } => {
                let planner = state.connectors().scan_planner("starrocks")?;
                let table_handle =
                    crate::connector::starrocks::table::StarRocksTableScanPlanner::table_handle_from_source(
                        &op.database,
                        &table.name,
                        *db_id,
                        *table_id,
                    );
                let scan = planner.begin_scan(
                    table_handle,
                    crate::connector::scan_planning::BeginScanContext,
                )?;
                let splits = planner
                    .plan_splits(&scan, crate::connector::scan_planning::SplitPlanningContext)?;
                Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
            }
            crate::sql::catalog::ScanSource::IcebergDataFiles {
                table: iceberg_table,
                files,
                ..
            } => {
                let planner = state.connectors().scan_planner("iceberg")?;
                let column_names = effective_iceberg_scan_column_names(&table);
                let table_handle = iceberg_scan_table_handle_for_codegen(
                    &op.table.source,
                    iceberg_table,
                    files.clone(),
                    column_names,
                );
                let scan = planner.begin_scan(
                    table_handle,
                    crate::connector::scan_planning::BeginScanContext,
                )?;
                let splits = planner
                    .plan_splits(&scan, crate::connector::scan_planning::SplitPlanningContext)?;
                Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
            }
            _ => None,
        };
        let mut required: Option<std::collections::HashSet<String>> = op
            .required_columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.to_lowercase()).collect());
        if let Some(required) = required.as_mut() {
            add_iceberg_equality_delete_required_columns(required, &table, planned_scan.as_ref())?;
            for variant_column in &op.variant_columns {
                required.insert(variant_column.source_column.to_lowercase());
            }
        }
        let scan_table_id = match &table.source {
            crate::sql::catalog::ScanSource::StarRocks { table_id, .. } => Some(*table_id),
            _ => iceberg_table_info(&table.source)
                .is_some()
                .then_some(synthetic_iceberg_table_id(scan_node_id)),
        };
        if let Some(table_id) = scan_table_id {
            state
                .desc_builder()
                .add_table_for_scan(table_id, &op.database, &table);
        }

        // Build a quick lookup so the column registration loop below can
        // recognise base-table columns that the dict rewriter retargeted
        // to a hidden `__nr_dict_<t>_<c>` Int32 slot. For those columns
        // we allocate ONE slot (at the source column's storage position)
        // named after the dict column and typed Int32 — keeping the
        // single-slot-per-column contract the StarRocks lake scan
        // expects (see `src/lower/node/lake_scan.rs`'s
        // `dict_int_to_string` self-map handling).
        let dict_source_to_target: HashMap<String, &ScanDictionaryColumn> = op
            .dict_columns
            .iter()
            .map(|dc| (dc.source_column.to_ascii_lowercase(), dc))
            .collect();
        // Track dict slot ids by source column so the second loop over
        // `op.dict_columns` doesn't re-allocate a slot for the same
        // column. Also accumulates the `(dict_slot_id, dict_col)` pairs
        // that feed the TGlobalDict / dict_string_id_to_int_ids payload
        // construction further down.
        let mut dict_slot_for_source: HashMap<String, i32> = HashMap::new();
        let mut dict_slot_to_dict: Vec<(i32, &ScanDictionaryColumn)> = Vec::new();
        let mut physical_slot_by_column: HashMap<String, i32> = HashMap::new();
        for (idx, col) in table.columns.iter().enumerate() {
            // The dict rewriter renames the source string column to the
            // dict column name in `op.columns` / `op.required_columns`,
            // so check membership using BOTH names when a dict mapping
            // exists for this base column.
            let dict_target = dict_source_to_target.get(&col.name.to_lowercase());
            if let Some(ref req) = required {
                let keep = req.contains(&col.name.to_lowercase())
                    || dict_target
                        .map(|dc| req.contains(&dc.dict_column.to_lowercase()))
                        .unwrap_or(false);
                if !keep {
                    continue;
                }
            }
            let slot_id = state.alloc_slot();
            // Bug B contract: slot keeps the SOURCE column's storage
            // name (so the lake scan finds the column by name in the
            // tablet schema) and Int32 type (the BE reads it as Utf8 via
            // `build_scan_schema_for_global_dict_encoding` when a
            // TGlobalDict is registered for the slot, then encodes
            // string -> dict id). The dict_column NAME is exposed only
            // in the FE codegen scope below, NOT in the slot descriptor
            // — the BE never sees `__nr_dict_t_s` as a column name.
            let slot_type = match dict_target {
                Some(_) => DataType::Int32,
                None => col.data_type.clone(),
            };
            let nullable = col.nullable;
            state.desc_builder().add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &slot_type,
                nullable,
                idx as i32,
            );
            slot_to_column.insert(slot_id, col.name.clone());
            physical_slot_by_column.insert(col.name.to_lowercase(), slot_id);
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: slot_type.clone(),
                type_desc: None,
                nullable,
            };
            // G1: pick up the per-column ColumnId from `op.columns` so the
            // scope's by-id index is populated for base-table reads. This is
            // what lets the optimizer's `DistributionSpec::HashPartitioned`
            // (which is now a `Vec<ColumnId>`) resolve directly against the
            // scan's child scope without having to round-trip through the
            // display name. The dict-renamed `OutputColumn` carries the
            // source column's id so the lookup still hits.
            let col_id = op
                .columns
                .iter()
                .find(|oc| {
                    let lc = oc.name.to_ascii_lowercase();
                    lc == col.name.to_ascii_lowercase()
                        || dict_target
                            .map(|dc| lc == dc.dict_column.to_ascii_lowercase())
                            .unwrap_or(false)
                })
                .map(|oc| oc.column_id)
                .unwrap_or(crate::sql::column_id::ColumnId::UNSET);
            scope.add_column_with_id(
                col_id,
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding.clone(),
            );
            // Also register the dict column name in the scope so the
            // post-rewrite `ColumnRef("__nr_dict_t_s")` resolves to this
            // same slot. The scan tuple holds a SINGLE slot for this
            // column; both names refer to it.
            if let Some(dict_col) = dict_target {
                scope.add_column_with_id(
                    col_id,
                    qualifier.map(|s| s.to_string()),
                    dict_col.dict_column.clone(),
                    binding.clone(),
                );
            }
            if let Some(dict_col) = dict_target {
                dict_slot_for_source.insert(col.name.to_ascii_lowercase(), slot_id);
                dict_slot_to_dict.push((slot_id, *dict_col));
            }
        }

        // Iceberg metadata pseudo-columns: register in ExprScope and emit as
        // output slots so SELECT _file/_pos and v3 row-lineage references
        // resolve in codegen and flow through to the HDFS_SCAN_NODE tuple
        // descriptor. Lowering picks up the slot by name to populate
        // IcebergVirtualSpec.
        //
        // Note: these pseudo-columns are NOT in `scan.columns`, so the column
        // pruning rule never adds them to `required_columns`. Always register
        // them regardless of `required`; the lowering layer only synthesises
        // the values for slots that are actually in the tuple descriptor.
        let meta_col_offset = table.columns.len();
        for (meta_idx, col) in table
            .iceberg_row_lineage_metadata_columns
            .iter()
            .enumerate()
        {
            let col_pos = (meta_col_offset + meta_idx) as i32;
            let slot_id = state.alloc_slot();
            state.desc_builder().add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                col_pos,
            );
            slot_to_column.insert(slot_id, col.name.clone());
            iceberg_metadata_pseudo_column_slots.insert(slot_id);
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: None,
                nullable: col.nullable,
            };
            let col_id = op
                .columns
                .iter()
                .find(|oc| oc.name.eq_ignore_ascii_case(&col.name))
                .map(|oc| oc.column_id)
                .unwrap_or(crate::sql::column_id::ColumnId::UNSET);
            scope.add_column_with_id(
                col_id,
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding,
            );
        }

        let variant_col_offset =
            table.columns.len() + table.iceberg_row_lineage_metadata_columns.len();
        let mut variant_path_columns = Vec::with_capacity(op.variant_columns.len());
        for (variant_idx, variant_column) in op.variant_columns.iter().enumerate() {
            let source_slot_id = physical_slot_by_column
                .get(&variant_column.source_column.to_lowercase())
                .copied()
                .ok_or_else(|| {
                    format!(
                        "scan `{}.{}` variant_columns references unknown source column `{}`",
                        op.database, table.name, variant_column.source_column
                    )
                })?;
            let output_slot_id = state.alloc_slot();
            let requested_type = type_infer::arrow_type_to_type_desc(
                &variant_column.requested_type,
            )
            .map_err(|err| {
                format!(
                    "scan `{}.{}` variant column `{}` has unsupported requested type {:?}: {err}",
                    op.database,
                    table.name,
                    variant_column.synthetic_column,
                    variant_column.requested_type
                )
            })?;
            let nullable = op
                .columns
                .iter()
                .find(|column| column.column_id == variant_column.synthetic_column_id)
                .map(|column| column.nullable)
                .unwrap_or(true);
            state.desc_builder().add_slot_with_type_desc(
                output_slot_id,
                scan_tuple_id,
                &variant_column.synthetic_column,
                requested_type.clone(),
                nullable,
                (variant_col_offset + variant_idx) as i32,
            );
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id: output_slot_id,
                data_type: variant_column.requested_type.clone(),
                type_desc: Some(requested_type.clone()),
                nullable,
            };
            scope.add_column_with_id(
                variant_column.synthetic_column_id,
                qualifier.map(|s| s.to_string()),
                variant_column.synthetic_column.clone(),
                binding,
            );
            variant_path_columns.push(plan_nodes::TVariantPathColumn::new(
                Some(source_slot_id),
                Some(output_slot_id),
                Some(variant_column.source_column.clone()),
                Some(variant_column.synthetic_column.clone()),
                Some(variant_column.canonical_path.clone()),
                Some(requested_type),
                Some(variant_column.strict),
            ));
        }

        // Compile predicates pushed down by the optimizer
        let pushed_conjuncts = if op.predicates.is_empty() {
            vec![]
        } else {
            let mut conjuncts = Vec::new();
            for pred in &op.predicates {
                let mut compiler = ExprCompiler::new(state.slot_allocator(), &scope);
                conjuncts.push(compiler.compile_typed(pred)?);
            }
            conjuncts
        };

        // Dict-encoded scan columns (Task 5/7/8 plan hints). The slot for
        // each dict column was already allocated in the table-column loop
        // above (where its storage `col_pos` is recorded). Here we just
        // build the BE-facing payload: a self-map `dict_slot → dict_slot`
        // for `TLakeScanNode.dict_string_id_to_int_ids` (the BE replaces
        // the dict slot in the scan layout with this id, so the storage
        // reader keeps the same slot) and a TGlobalDict for each. The
        // dict_columns hint is still consulted later to detect a planning
        // bug on non-StarRocks scans. `dict_columns` is empty in all
        // production paths today.
        let mut string_to_dict_slot: BTreeMap<i32, i32> = BTreeMap::new();
        for dict_col in &op.dict_columns {
            let dict_slot_id = dict_slot_for_source
                .get(&dict_col.source_column.to_ascii_lowercase())
                .copied()
                .ok_or_else(|| {
                    format!(
                        "scan `{}.{}` dict_columns references unknown source column `{}`",
                        op.database, table.name, dict_col.source_column
                    )
                })?;
            // Self-map: the BE's `lake_scan.rs` rewrites every dict int
            // slot in the layout to its mapped string slot before issuing
            // the storage read. With the FE-only fix the FE no longer
            // emits a separate string slot — the dict slot itself is the
            // storage slot, declared Int32 in desc_tbl but read as Utf8
            // via the query global dict path (see
            // `build_scan_schema_for_global_dict_encoding`). A self-map
            // keeps the BE's layout swap a no-op while preserving the
            // "dict-encoded" semantics on the FE/wire contract.
            string_to_dict_slot.insert(dict_slot_id, dict_slot_id);
        }

        let resolved = ResolvedTable {
            database: op.database.clone(),
            table: table.clone(),
            planned_scan,
            alias: op.alias.clone(),
        };
        state.desc_builder().add_tuple(scan_tuple_id, scan_table_id);

        let min_max_predicates =
            nodes::scan_file_min_max_predicates_from_state(&pushed_conjuncts, &slot_to_column);
        let change_op_slot = nodes::planned_change_op_slot_from_state(
            &iceberg_metadata_pseudo_column_slots,
            &slot_to_column,
        );
        let mut scan_plan_node = nodes::build_scan_node(
            state.connectors(),
            scan_node_id,
            scan_tuple_id,
            &resolved,
            pushed_conjuncts.clone(),
            min_max_predicates,
            change_op_slot,
        )?;

        if !variant_path_columns.is_empty() {
            if let Some(hdfs) = scan_plan_node.hdfs_scan_node.as_mut() {
                hdfs.variant_path_columns = Some(variant_path_columns);
            } else {
                return Err(format!(
                    "scan `{}.{}` has variant_columns but is not an iceberg/HDFS scan",
                    op.database, table.name
                ));
            }
        }

        // StarRocks lake scans carry the dict slot self-map on the wire via
        // `TLakeScanNode.dict_string_id_to_int_ids`. Iceberg/HDFS scans have no
        // such thrift field and don't need one: the dict slot is already an
        // Int32 storage slot, and the per-fragment `TGlobalDict` payloads
        // emitted below feed `lower_hdfs_scan_node`'s encode map directly
        // (the parquet reader reads Utf8 and encodes to dict ids). So for an
        // iceberg `hdfs_scan_node` we leave the thrift node untouched. Any
        // other scan kind receiving dict_columns is a planning bug.
        if !string_to_dict_slot.is_empty() {
            if let Some(lake) = scan_plan_node.lake_scan_node.as_mut() {
                lake.dict_string_id_to_int_ids = Some(string_to_dict_slot);
            } else if scan_plan_node.hdfs_scan_node.is_some() {
                // iceberg/HDFS: dicts flow via query_global_dicts in lowering.
            } else {
                return Err(format!(
                    "scan `{}.{}` has dict_columns but is neither a StarRocks lake scan nor an iceberg/HDFS scan",
                    op.database, op.table.name,
                ));
            }
        }

        // Emit per-dict-column TGlobalDict payloads onto EVERY fragment in
        // the current stack (the leaf scan's fragment plus every parent
        // fragment that consumes its output through an exchange). The
        // dict slot id is consistent across fragments, so a Decode
        // operator inserted above the exchange — which lives in a parent
        // fragment — must also receive the TGlobalDict via its own
        // fragment's `query_global_dicts`. Without this, the BE's
        // `lower_decode_node` fails with `missing query global dict for
        // encoded slot_id=<N>` (each fragment builds its own
        // QueryGlobalDictMap from its own TGlobalDict list).
        let current_frag = state.current_fragment_id()?;
        let dict_fragments: Vec<FragmentId> = if state.fragment_stack().is_empty() {
            vec![current_frag]
        } else {
            state.fragment_stack().to_vec()
        };
        for (dict_slot_id, dict_col) in &dict_slot_to_dict {
            let snapshot = dict_col.dictionary.as_ref();
            let mut ids = Vec::with_capacity(snapshot.values.len());
            let mut strings = Vec::with_capacity(snapshot.values.len());
            for value in &snapshot.values {
                ids.push(value.id);
                strings.push(value.bytes.clone());
            }
            let global_dict = crate::data::TGlobalDict::new(
                Some(*dict_slot_id),
                Some(strings),
                Some(ids),
                Some(snapshot.version),
            );
            for fragment_id in &dict_fragments {
                state
                    .query_global_dicts_per_fragment()
                    .entry(*fragment_id)
                    .or_default()
                    .push(global_dict.clone());
            }
            // Track the slot -> dict association so any operator that
            // allocates a new slot inheriting this slot's values
            // (Aggregate group-by, Project column ref, etc.) can
            // re-register the dict on the new slot id. The downstream
            // `Decode` resolves by NAME against the new slot, then the
            // BE's `lower_decode_node` needs a TGlobalDict keyed by that
            // new slot id — registered via `propagate_dict_to_slot`.
            state
                .slot_to_global_dict_mut()
                .insert(*dict_slot_id, global_dict);
        }

        state.scan_tables().push(nodes::PlannedScanTable {
            scan_node_id,
            scan_tuple_id,
            resolved,
            min_max_conjuncts: pushed_conjuncts,
            slot_to_column,
            iceberg_metadata_pseudo_column_slots,
        });

        Ok((scan_plan_node, scope))
    }

    pub(crate) fn lower_project(
        &mut self,
        project_node_id: i32,
        project_tuple_id: i32,
        op: &super::kind::DistributedProjectNode,
        child_scope: &ExprScope,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope, Vec<OutputColumn>), String> {
        let state = &mut *self.state;
        let mut output_columns = Vec::new();
        let mut slot_map = BTreeMap::new();
        let mut project_scope = ExprScope::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            let data_type = item.expr.data_type.clone();
            let nullable = item.expr.nullable;
            let name = item.output_name.clone();
            let slot_id = state.alloc_slot();
            let slot_type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("project expr `{name}` compiled to empty TExpr"))?;
            state.desc_builder().add_slot_with_type_desc(
                slot_id,
                project_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                output_columns.len() as i32,
            );
            slot_map.insert(slot_id, texpr);
            output_columns.push(OutputColumn {
                name: name.clone(),
                data_type: data_type.clone(),
                nullable,
            });

            let binding = ColumnBinding {
                tuple_id: project_tuple_id,
                slot_id,
                data_type: data_type.clone(),
                type_desc: Some(slot_type_desc.clone()),
                nullable,
            };
            project_scope.add_column_with_id(
                item.output_column_id,
                op.output_qualifier.clone(),
                name.clone(),
                binding.clone(),
            );

            let unqualified_display = typed_expr_display_name_without_qualifiers(&item.expr);
            if !unqualified_display.eq_ignore_ascii_case(&name) {
                let _ = unqualified_display;
            }

            // Propagate the dict registration on a ColumnRef passthrough:
            // the new slot inherits the source slot's dict, so a parent
            // fragment's Decode (post-exchange) finds the matching dict
            // in its own `query_global_dicts`.
            if let ExprKind::ColumnRef { column_id, .. } = item.expr.kind
                && let Some(child_binding) = child_scope.resolve_by_id(column_id)
            {
                let source_slot_id = child_binding.slot_id;
                state.propagate_dict_to_slot(source_slot_id, slot_id);
            }
        }

        state.desc_builder().add_tuple(project_tuple_id, None);
        let project_plan_node =
            nodes::build_project_node(project_node_id, project_tuple_id, slot_map);

        Ok((project_plan_node, project_scope, output_columns))
    }

    pub(crate) fn lower_hash_aggregate(
        &mut self,
        agg_node_id: i32,
        agg_tuple_id: i32,
        op: &super::kind::DistributedHashAggregateNode,
        child_scope: &ExprScope,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope), String> {
        let state = &mut *self.state;
        let need_finalize = matches!(op.mode, AggMode::Single | AggMode::Global);

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        // Compile GROUP BY expressions (same for all modes — the child scope
        // has the correct columns for both scan-level and Local-output contexts).
        for (idx, gb_expr) in op.group_by.iter().enumerate() {
            let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
            let texpr = compiler.compile_typed(gb_expr)?;
            let data_type = gb_expr.data_type.clone();
            let nullable = gb_expr.nullable;
            let name = typed_expr_display_name(gb_expr);
            let slot_id = state.alloc_slot();
            let slot_type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("group by expr `{name}` compiled to empty TExpr"))?;
            state.desc_builder().add_slot_with_type_desc(
                slot_id,
                agg_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: agg_tuple_id,
                slot_id,
                data_type: data_type.clone(),
                type_desc: Some(slot_type_desc),
                nullable,
            };
            let gb_column_id = op
                .output_columns
                .get(idx)
                .map(|col| col.column_id)
                .unwrap_or_else(|| match &gb_expr.kind {
                    ExprKind::ColumnRef { column_id, .. } => *column_id,
                    _ => crate::sql::column_id::ColumnId::UNSET,
                });
            agg_scope.add_column_with_id(gb_column_id, None, name, binding.clone());
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
                ..
            } = gb_expr.kind
            {
                let _ = (q, column, binding);
            }
            // Propagate dict registration through the aggregate's group-
            // by output: when the group-by is a passthrough ColumnRef of
            // a dict-encoded source slot, the new agg output slot also
            // carries dict ids. Re-register the TGlobalDict on the new
            // slot so a downstream Decode (in this or a parent fragment
            // post-exchange) resolves its `dict_id_to_string_ids` key.
            if let ExprKind::ColumnRef { column_id, .. } = gb_expr.kind
                && let Some(child_binding) = child_scope.resolve_by_id(column_id)
            {
                let source_slot_id = child_binding.slot_id;
                state.propagate_dict_to_slot(source_slot_id, slot_id);
            }
            grouping_exprs.push(texpr);
        }

        // Compile aggregate function expressions — mode-dependent.
        let agg_start_col = op.group_by.len();
        let mut aggregate_functions = Vec::new();

        debug_assert_eq!(
            op.is_merge.len(),
            op.aggregates.len(),
            "PhysicalHashAggregate (node_id={}): is_merge.len() = {}, aggregates.len() = {}",
            agg_node_id,
            op.is_merge.len(),
            op.aggregates.len(),
        );

        for (idx, agg_call) in op.aggregates.iter().enumerate() {
            let texpr = if op.is_merge[idx] {
                // Global (merge) phase: the child scope contains the Local's
                // output.  Each intermediate aggregate column sits at position
                // group_by.len() + idx in the child scope's ordered columns.
                let child_columns: Vec<_> = child_scope.iter_columns().collect();
                let child_col_idx = agg_start_col + idx;
                let (_, binding) = child_columns.get(child_col_idx).ok_or_else(|| {
                    format!(
                        "Global agg: child scope missing intermediate column at index {}",
                        child_col_idx
                    )
                })?;
                let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
                compiler.compile_merge_aggregate_call(
                    agg_call,
                    binding.slot_id,
                    binding.tuple_id,
                    &binding.data_type,
                )?
            } else {
                // Single or Local: compile against child scope normally.
                let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
                compiler.compile_aggregate_call_typed(agg_call).map_err(|err| {
                    let available = child_scope
                        .iter_columns()
                        .map(|(name, _)| name.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(
                        "failed to compile aggregate `{}` in {:?} mode against child scope [{}]: {}",
                        agg_call_display_name(agg_call),
                        op.mode,
                        available,
                        err
                    )
                })?
            };

            let nullable = true;
            let name = agg_call_display_name(agg_call);
            let intermediate_type = texpr
                .nodes
                .first()
                .and_then(|root| root.fn_.as_ref())
                .and_then(|func| func.aggregate_fn.as_ref())
                .and_then(|agg_fn| arrow_type_from_desc(&agg_fn.intermediate_type));
            let slot_contract = aggregate_slot_contract_for_phase(
                need_finalize,
                &agg_call.result_type,
                intermediate_type.as_ref(),
                &name,
            )?;
            let data_type = slot_contract.data_type.clone();
            let slot_type_desc = slot_contract.type_desc.clone();
            let slot_id = state.alloc_slot();
            let col_pos = (agg_start_col + idx) as i32;
            state.desc_builder().add_slot_with_type_desc(
                slot_id,
                agg_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                col_pos,
            );
            let binding = ColumnBinding {
                tuple_id: agg_tuple_id,
                slot_id,
                data_type,
                type_desc: Some(slot_type_desc),
                nullable,
            };
            agg_scope.add_column_with_id(
                agg_call.output_column_id,
                None,
                name.clone(),
                binding.clone(),
            );
            let unqualified_name = agg_call_display_name_without_qualifiers(agg_call);
            if !unqualified_name.eq_ignore_ascii_case(&name) {
                let _ = unqualified_name;
            }
            aggregate_functions.push(texpr);
        }

        state.desc_builder().add_tuple(agg_tuple_id, None);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            aggregate_functions,
            need_finalize,
        );

        Ok((agg_plan_node, agg_scope))
    }

    pub(crate) fn lower_sort(
        &mut self,
        sort_node_id: i32,
        op: &super::kind::DistributedSortNode,
        child_scope: &ExprScope,
        child_tuple_ids: &[i32],
        output_columns: &[AnalysisOutputColumn],
        offset: Option<i64>,
    ) -> Result<plan_nodes::TPlanNode, String> {
        let state = &mut *self.state;

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        // Compile analytic-partition exprs (set when this Sort precedes a
        // Window). Emitting them as TSortNode.analytic_partition_exprs tells
        // the pipeline engine to run sort locally per partition instead of
        // doing a global merge — matching StarRocks's parallel analytic
        // sort behaviour. Empty for plain ORDER BY.
        let analytic_partition_exprs = if op.analytic_partition_by.is_empty() {
            None
        } else {
            let mut out = Vec::with_capacity(op.analytic_partition_by.len());
            for expr in &op.analytic_partition_by {
                let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
                out.push(compiler.compile_typed(expr)?);
            }
            Some(out)
        };

        // Per-partition TopN fields (set by RankingWindowPredicatePushdown). For a
        // ranking-window pushdown the partition keys ARE the analytic partition keys,
        // so reuse analytic_partition_exprs as the key source. CONTRACT: partition-topn
        // is DECOUPLED from the global limit — `limit` stays -1 (None) and `use_top_n`
        // is set true ONLY via partition_limit.
        let (partition_exprs_t, partition_limit_t, topn_type_t, use_top_n_for_partition) =
            if let Some(limit) = op.partition_limit {
                let mut keys = Vec::with_capacity(op.analytic_partition_by.len());
                for expr in &op.analytic_partition_by {
                    let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
                    keys.push(compiler.compile_typed(expr)?);
                }
                let tt = match op.topn_type {
                    Some(crate::exec::node::sort::SortTopNType::RowNumber) => {
                        plan_nodes::TTopNType::ROW_NUMBER
                    }
                    Some(crate::exec::node::sort::SortTopNType::Rank) => {
                        plan_nodes::TTopNType::RANK
                    }
                    Some(crate::exec::node::sort::SortTopNType::DenseRank) => {
                        plan_nodes::TTopNType::DENSE_RANK
                    }
                    None => plan_nodes::TTopNType::ROW_NUMBER,
                };
                (Some(keys), Some(limit as i64), Some(tt), true)
            } else {
                (None, None, None, false)
            };

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            slot_ref_exprs_for_columns(child_scope, output_columns, "Sort")?,
        );
        let sort_tuple_slot_exprs = sort_info.sort_tuple_slot_exprs.clone();

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = sort_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = -1;
        sort_plan_node.row_tuples = child_tuple_ids.to_vec();
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: use_top_n_for_partition,
            offset,
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs,
            partition_exprs: partition_exprs_t,
            partition_limit: partition_limit_t,
            topn_type: topn_type_t,
            build_runtime_filters: None,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            late_materialization: None,
            enable_parallel_merge: None,
            analytic_partition_skewed: None,
            pre_agg_exprs: None,
            pre_agg_output_slot_id: None,
            pre_agg_insert_local_shuffle: None,
            parallel_merge_late_materialize_mode: None,
            per_pipeline: None,
        });

        Ok(sort_plan_node)
    }

    pub(crate) fn lower_top_n_single_or_partial(
        &mut self,
        top_n_node_id: i32,
        op: &super::kind::DistributedTopNNode,
        child_scope: &ExprScope,
        child_tuple_ids: &[i32],
    ) -> Result<plan_nodes::TPlanNode, String> {
        match (op.phase, op.is_split) {
            (TopNPhase::Final, true) => return Err("TopN split is Phase 2".to_string()),
            (TopNPhase::Final, false) | (TopNPhase::Partial, _) => {}
        }

        let state = &mut *self.state;
        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(state.slot_allocator(), child_scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            None::<Vec<exprs::TExpr>>,
        );

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = top_n_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = op.limit.unwrap_or(-1);
        sort_plan_node.row_tuples = child_tuple_ids.to_vec();
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: true,
            offset: op.offset,
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs: None,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs: None,
            partition_exprs: None,
            partition_limit: None,
            topn_type: None,
            build_runtime_filters: None,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            late_materialization: None,
            enable_parallel_merge: None,
            analytic_partition_skewed: None,
            pre_agg_exprs: None,
            pre_agg_output_slot_id: None,
            pre_agg_insert_local_shuffle: None,
            parallel_merge_late_materialize_mode: None,
            per_pipeline: None,
        });

        Ok(sort_plan_node)
    }

    pub(crate) fn lower_values(
        &mut self,
        values_node_id: i32,
        output_tuple_id: i32,
        op: &super::kind::DistributedValuesNode,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope), String> {
        let state = &mut *self.state;

        let mut scope = ExprScope::new();
        for (idx, col) in op.columns.iter().enumerate() {
            let slot_id = state.alloc_slot();
            state.desc_builder().add_slot(
                slot_id,
                output_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            scope.add_column_with_id(
                col.column_id,
                None,
                col.name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: col.data_type.clone(),
                    type_desc: None,
                    nullable: col.nullable,
                },
            );
        }
        state.desc_builder().add_tuple(output_tuple_id, None);

        let empty_scope = ExprScope::new();
        let mut const_expr_lists = Vec::with_capacity(op.rows.len());
        for row in &op.rows {
            if row.len() != op.columns.len() {
                return Err(format!(
                    "VALUES row column count mismatch: expected {}, got {}",
                    op.columns.len(),
                    row.len()
                ));
            }
            let mut exprs = Vec::with_capacity(row.len());
            for expr in row {
                let mut compiler = ExprCompiler::new(state.slot_allocator(), &empty_scope);
                exprs.push(compiler.compile_typed(expr)?);
            }
            const_expr_lists.push(exprs);
        }

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = values_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::UNION_NODE;
        plan_node.num_children = 0;
        plan_node.row_tuples = vec![output_tuple_id];
        plan_node.nullable_tuples = vec![];
        plan_node.union_node = Some(plan_nodes::TUnionNode {
            tuple_id: output_tuple_id,
            result_expr_lists: vec![],
            const_expr_lists,
            first_materialized_child_idx: 0,
            pass_through_slot_maps: None,
            local_exchanger_type: None,
            local_partition_by_exprs: None,
        });

        Ok((plan_node, scope))
    }

    fn lower_window(
        &mut self,
        window_node_id: i32,
        node_tuple_ids: &[i32],
        kind: &super::kind::DistributedWindowNode,
        child_scope: &ExprScope,
        child_tuple_ids: &[i32],
        child_ordering: &OrderingSpec,
    ) -> Result<
        (
            Vec<plan_nodes::TPlanNode>,
            ExprScope,
            Vec<i32>,
            OrderingSpec,
        ),
        String,
    > {
        let groups = group_win_exprs_by_sig(&kind.window_exprs);
        if groups.is_empty() {
            return Err("DistributedPlan Window has no window expressions".to_string());
        }
        let expected_tuple_count = child_tuple_ids.len() + groups.len();
        if node_tuple_ids.len() != expected_tuple_count {
            return Err(format!(
                "DistributedPlan Window tuple count mismatch: expected {}, got {}",
                expected_tuple_count,
                node_tuple_ids.len()
            ));
        }

        let mut op_nodes = Vec::new();
        let mut current_scope = ExprScope::new();
        current_scope.merge(child_scope);
        let mut current_tuple_ids = child_tuple_ids.to_vec();
        let mut current_ordering = child_ordering.clone();
        let mut next_node_id = window_node_id;

        for (group_idx, group_indices) in groups.iter().enumerate() {
            let output_tuple_id = node_tuple_ids[child_tuple_ids.len() + group_idx];
            let intermediate_tuple_id = output_tuple_id.checked_sub(1).ok_or_else(|| {
                format!(
                    "DistributedPlan Window output tuple id {} cannot derive intermediate tuple id",
                    output_tuple_id
                )
            })?;
            let group_exprs: Vec<_> = group_indices
                .iter()
                .map(|&i| kind.window_exprs[i].clone())
                .collect();
            let first_win = group_exprs
                .first()
                .ok_or_else(|| "DistributedPlan Window group is empty".to_string())?;

            if groups.len() > 1 {
                let required_ordering =
                    window_ordering_spec(&first_win.partition_by, &first_win.order_by);
                let has_sort_keys =
                    !first_win.partition_by.is_empty() || !first_win.order_by.is_empty();
                let ordering_is_representable = !matches!(required_ordering, OrderingSpec::Any);
                let needs_sort = has_sort_keys
                    && (!ordering_is_representable
                        || !current_ordering.satisfies(&required_ordering));
                if needs_sort {
                    let sort_node_id = next_node_id;
                    next_node_id += 1;
                    let mut sort_ordering = Vec::new();
                    let mut sort_is_asc = Vec::new();
                    let mut sort_nulls_first_list = Vec::new();
                    for expr in &first_win.partition_by {
                        let mut compiler =
                            ExprCompiler::new(self.state.slot_allocator(), &current_scope);
                        sort_ordering.push(compiler.compile_typed(expr)?);
                        sort_is_asc.push(true);
                        sort_nulls_first_list.push(true);
                    }
                    for item in &first_win.order_by {
                        let mut compiler =
                            ExprCompiler::new(self.state.slot_allocator(), &current_scope);
                        sort_ordering.push(compiler.compile_typed(&item.expr)?);
                        sort_is_asc.push(item.asc);
                        sort_nulls_first_list.push(item.nulls_first);
                    }
                    let sort_plan = nodes::build_sort_node_raw(
                        sort_node_id,
                        current_tuple_ids.clone(),
                        sort_ordering,
                        sort_is_asc,
                        sort_nulls_first_list,
                        -1,
                        None,
                    );
                    op_nodes.insert(0, sort_plan);
                    current_ordering = required_ordering;
                }
            }

            let analytic_node_id = next_node_id;
            next_node_id += 1;

            let mut partition_exprs = Vec::new();
            for expr in &first_win.partition_by {
                let mut compiler = ExprCompiler::new(self.state.slot_allocator(), &current_scope);
                partition_exprs.push(compiler.compile_typed(expr)?);
            }
            let mut order_by_exprs = Vec::new();
            for item in &first_win.order_by {
                let mut compiler = ExprCompiler::new(self.state.slot_allocator(), &current_scope);
                order_by_exprs.push(compiler.compile_typed(&item.expr)?);
            }

            let mut analytic_functions = Vec::new();
            for win_expr in &group_exprs {
                let mut compiler = ExprCompiler::new(self.state.slot_allocator(), &current_scope);
                let agg_call = AggregateCall {
                    name: win_expr.name.clone(),
                    args: win_expr.args.clone(),
                    distinct: win_expr.distinct,
                    result_type: win_expr.result_type.clone(),
                    order_by: vec![],
                    output_column_id: crate::sql::column_id::ColumnId::UNSET,
                };
                let mut texpr = compiler.compile_aggregate_call_typed(&agg_call)?;
                apply_ignore_nulls_to_root_fn(&mut texpr, win_expr.ignore_nulls);
                analytic_functions.push(texpr);
            }

            for (idx, win_expr) in group_exprs.iter().enumerate() {
                let slot_id = self.state.alloc_slot();
                self.state.desc_builder().add_slot(
                    slot_id,
                    intermediate_tuple_id,
                    &format!("__win_intermediate_{idx}"),
                    &win_expr.result_type,
                    true,
                    idx as i32,
                );
            }
            self.state
                .desc_builder()
                .add_tuple(intermediate_tuple_id, None);

            let mut output_scope = ExprScope::new();
            output_scope.merge(&current_scope);
            for (idx, win_expr) in group_exprs.iter().enumerate() {
                let slot_id = self.state.alloc_slot();
                self.state.desc_builder().add_slot(
                    slot_id,
                    output_tuple_id,
                    &win_expr.output_name,
                    &win_expr.result_type,
                    true,
                    idx as i32,
                );
                output_scope.add_column_with_id(
                    win_expr.output_column_id,
                    None,
                    win_expr.output_name.clone(),
                    ColumnBinding {
                        tuple_id: output_tuple_id,
                        slot_id,
                        data_type: win_expr.result_type.clone(),
                        type_desc: None,
                        nullable: true,
                    },
                );
            }
            self.state.desc_builder().add_tuple(output_tuple_id, None);

            let analytic_tnode = plan_nodes::TAnalyticNode {
                partition_exprs,
                order_by_exprs,
                analytic_functions,
                window: analytic_window_from_expr(first_win),
                intermediate_tuple_id,
                output_tuple_id,
                buffered_tuple_id: None,
                partition_by_eq: None,
                order_by_eq: None,
                sql_partition_keys: None,
                sql_aggregate_functions: None,
                has_outer_join_child: None,
                use_hash_based_partition: None,
                is_skewed: None,
            };

            let mut plan_node = nodes::default_plan_node();
            plan_node.node_id = analytic_node_id;
            plan_node.node_type = plan_nodes::TPlanNodeType::ANALYTIC_EVAL_NODE;
            plan_node.num_children = 1;
            plan_node.limit = -1;
            let mut new_tuple_ids = current_tuple_ids.clone();
            new_tuple_ids.push(output_tuple_id);
            plan_node.row_tuples = new_tuple_ids.clone();
            plan_node.nullable_tuples = vec![];
            plan_node.analytic_node = Some(analytic_tnode);

            op_nodes.insert(0, plan_node);
            current_scope = output_scope;
            current_tuple_ids = new_tuple_ids;
        }

        Ok((op_nodes, current_scope, current_tuple_ids, current_ordering))
    }

    fn lower_generate_series(
        &mut self,
        table_fn_node_id: i32,
        output_tuple_id: i32,
        op: &GenerateSeriesOp,
    ) -> Result<(Vec<plan_nodes::TPlanNode>, ExprScope), String> {
        let derived_param_values_node = table_fn_node_id - 1;
        let derived_param_tuple = output_tuple_id - 1;
        if op.step == 0 {
            return Err("generate_series step size cannot equal zero".to_string());
        }

        let int64_type_desc = type_infer::arrow_type_to_type_desc(&DataType::Int64)?;
        let empty_scope = ExprScope::new();
        let mut param_slots = Vec::with_capacity(3);
        let mut param_exprs = Vec::with_capacity(3);
        for (idx, (name, value)) in [
            ("__gs_start", op.start),
            ("__gs_end", op.end),
            ("__gs_step", op.step),
        ]
        .into_iter()
        .enumerate()
        {
            let slot_id = self.state.alloc_slot();
            self.state.desc_builder().add_slot_with_type_desc(
                slot_id,
                derived_param_tuple,
                name,
                int64_type_desc.clone(),
                false,
                idx as i32,
            );
            param_slots.push(slot_id);
            param_exprs.push(self.compile_int64_literal(value, &empty_scope)?);
        }
        self.state
            .desc_builder()
            .add_tuple(derived_param_tuple, None);

        let mut param_values_node = nodes::default_plan_node();
        param_values_node.node_id = derived_param_values_node;
        param_values_node.node_type = plan_nodes::TPlanNodeType::UNION_NODE;
        param_values_node.num_children = 0;
        param_values_node.row_tuples = vec![derived_param_tuple];
        param_values_node.nullable_tuples = vec![];
        param_values_node.union_node = Some(plan_nodes::TUnionNode {
            tuple_id: derived_param_tuple,
            result_expr_lists: vec![],
            const_expr_lists: vec![param_exprs],
            first_materialized_child_idx: 0,
            pass_through_slot_maps: None,
            local_exchanger_type: None,
            local_partition_by_exprs: None,
        });

        let slot_id = self.state.alloc_slot();
        self.state.desc_builder().add_slot_with_type_desc(
            slot_id,
            output_tuple_id,
            &op.column_name,
            int64_type_desc.clone(),
            false,
            0,
        );
        self.state.desc_builder().add_tuple(output_tuple_id, None);

        let mut scope = ExprScope::new();
        let qualifier = op
            .alias
            .clone()
            .unwrap_or_else(|| "generate_series".to_string());
        scope.add_column_with_id(
            op.output_column_id,
            Some(qualifier),
            op.column_name.clone(),
            ColumnBinding {
                tuple_id: output_tuple_id,
                slot_id,
                data_type: DataType::Int64,
                type_desc: Some(int64_type_desc.clone()),
                nullable: false,
            },
        );

        let table_function_expr = exprs::TExpr::new(vec![exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: int64_type_desc.clone(),
            num_children: 0,
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: "generate_series".to_string(),
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: vec![
                    int64_type_desc.clone(),
                    int64_type_desc.clone(),
                    int64_type_desc.clone(),
                ],
                ret_type: int64_type_desc.clone(),
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: None,
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: Some(types::TTableFunction::new(
                    vec![int64_type_desc.clone()],
                    None::<String>,
                    Some(false),
                )),
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..expr_compiler::default_expr_node()
        }]);

        let mut table_fn_plan_node = nodes::default_plan_node();
        table_fn_plan_node.node_id = table_fn_node_id;
        table_fn_plan_node.node_type = plan_nodes::TPlanNodeType::TABLE_FUNCTION_NODE;
        table_fn_plan_node.num_children = 1;
        table_fn_plan_node.limit = -1;
        table_fn_plan_node.row_tuples = vec![output_tuple_id];
        table_fn_plan_node.nullable_tuples = vec![];
        table_fn_plan_node.compact_data = true;
        table_fn_plan_node.table_function_node = Some(plan_nodes::TTableFunctionNode::new(
            Some(table_function_expr),
            Some(param_slots),
            Some(Vec::new()),
            Some(vec![slot_id]),
            Some(true),
        ));

        Ok((vec![table_fn_plan_node, param_values_node], scope))
    }

    fn compile_int64_literal(
        &mut self,
        value: i64,
        empty_scope: &ExprScope,
    ) -> Result<exprs::TExpr, String> {
        let typed = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        };
        let mut compiler = ExprCompiler::new(self.state.slot_allocator(), empty_scope);
        compiler.compile_typed(&typed)
    }

    fn lower_table_function(
        &mut self,
        table_fn_node_id: i32,
        output_tuple_id: i32,
        op: &super::kind::DistributedTableFunctionNode,
        child_scope: &ExprScope,
    ) -> Result<(Vec<plan_nodes::TPlanNode>, ExprScope), String> {
        let derived_project_node = table_fn_node_id - 1;
        let derived_project_tuple = output_tuple_id - 1;
        if !op.function_name.eq_ignore_ascii_case("unnest") {
            return Err(format!(
                "unsupported standalone table function: {}",
                op.function_name
            ));
        }
        if op.args.len() != op.output_columns.len() {
            return Err(format!(
                "table function output column count mismatch: args={} outputs={}",
                op.args.len(),
                op.output_columns.len()
            ));
        }

        let mut slot_map = BTreeMap::new();
        let mut project_scope = ExprScope::new();
        let mut remapped_child_bindings = HashMap::new();
        let mut outer_columns = Vec::new();
        let mut outer_slots = Vec::new();

        let child_cols: Vec<(String, ColumnBinding)> = child_scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();
        for (idx, (name, binding)) in child_cols.iter().enumerate() {
            let slot_id = self.state.alloc_slot();
            let type_desc = expr_compiler::binding_type_desc(binding)?;
            self.state.desc_builder().add_slot_with_type_desc(
                slot_id,
                derived_project_tuple,
                name,
                type_desc.clone(),
                binding.nullable,
                idx as i32,
            );
            slot_map.insert(
                slot_id,
                expr_compiler::build_slot_ref_texpr(
                    binding.slot_id,
                    binding.tuple_id,
                    type_desc.clone(),
                ),
            );
            let new_binding = ColumnBinding {
                tuple_id: derived_project_tuple,
                slot_id,
                data_type: binding.data_type.clone(),
                type_desc: Some(type_desc),
                nullable: binding.nullable,
            };
            project_scope.add_column(None, name.clone(), new_binding.clone());
            remapped_child_bindings
                .insert((binding.tuple_id, binding.slot_id), new_binding.clone());
            outer_slots.push(slot_id);
            outer_columns.push((name.clone(), new_binding));
        }
        for (column_id, binding) in child_scope.iter_id_bindings() {
            if let Some(new_binding) =
                remapped_child_bindings.get(&(binding.tuple_id, binding.slot_id))
            {
                project_scope.add_id_alias(*column_id, new_binding.clone());
            }
        }

        let mut param_slots = Vec::with_capacity(op.args.len());
        let mut param_type_descs = Vec::with_capacity(op.args.len());
        for (idx, arg) in op.args.iter().enumerate() {
            let mut compiler = ExprCompiler::new(self.state.slot_allocator(), child_scope);
            let texpr = compiler.compile_typed(arg)?;
            let type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("table function arg {idx} compiled to empty TExpr"))?;
            let slot_id = self.state.alloc_slot();
            self.state.desc_builder().add_slot_with_type_desc(
                slot_id,
                derived_project_tuple,
                &format!("__tf_arg_{idx}"),
                type_desc.clone(),
                arg.nullable,
                (child_cols.len() + idx) as i32,
            );
            slot_map.insert(slot_id, texpr);
            param_slots.push(slot_id);
            param_type_descs.push(type_desc);
        }
        self.state
            .desc_builder()
            .add_tuple(derived_project_tuple, None);
        let project_plan_node =
            nodes::build_project_node(derived_project_node, derived_project_tuple, slot_map);

        let mut output_scope = ExprScope::new();
        let mut output_outer_by_project_slot = HashMap::new();
        for (idx, (name, binding)) in outer_columns.iter().enumerate() {
            let type_desc = expr_compiler::binding_type_desc(binding)?;
            self.state.desc_builder().add_slot_with_type_desc(
                binding.slot_id,
                output_tuple_id,
                name,
                type_desc.clone(),
                binding.nullable,
                idx as i32,
            );
            let output_binding = ColumnBinding {
                tuple_id: output_tuple_id,
                slot_id: binding.slot_id,
                data_type: binding.data_type.clone(),
                type_desc: Some(type_desc),
                nullable: binding.nullable,
            };
            output_scope.add_column(None, name.clone(), output_binding.clone());
            output_outer_by_project_slot.insert(binding.slot_id, output_binding);
        }
        let output_id_aliases: Vec<_> = project_scope
            .iter_id_bindings()
            .filter_map(|(column_id, binding)| {
                output_outer_by_project_slot
                    .get(&binding.slot_id)
                    .map(|output_binding| (*column_id, output_binding.clone()))
            })
            .collect();
        for (column_id, output_binding) in output_id_aliases {
            output_scope.add_id_alias(column_id, output_binding);
        }

        let mut fn_result_slots = Vec::with_capacity(op.output_columns.len());
        let mut ret_type_descs = Vec::with_capacity(op.output_columns.len());
        let result_qualifier = op.alias.clone().or_else(|| Some(op.function_name.clone()));
        for (idx, col) in op.output_columns.iter().enumerate() {
            let DataType::List(item_field) = &op.args[idx].data_type else {
                return Err(format!(
                    "UNNEST argument {} must be ARRAY, got {:?}",
                    idx + 1,
                    op.args[idx].data_type
                ));
            };
            if item_field.data_type() != &col.data_type {
                return Err(format!(
                    "UNNEST result type mismatch for column {}: arg item={:?} output={:?}",
                    col.name,
                    item_field.data_type(),
                    col.data_type
                ));
            }
            let slot_id = self.state.alloc_slot();
            let type_desc = type_infer::arrow_type_to_type_desc(&col.data_type)?;
            self.state.desc_builder().add_slot_with_type_desc(
                slot_id,
                output_tuple_id,
                &col.name,
                type_desc.clone(),
                true,
                (outer_columns.len() + idx) as i32,
            );
            let binding = ColumnBinding {
                tuple_id: output_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: Some(type_desc.clone()),
                nullable: true,
            };
            output_scope.add_column_with_id(
                col.column_id,
                result_qualifier.clone(),
                col.name.clone(),
                binding,
            );
            fn_result_slots.push(slot_id);
            ret_type_descs.push(type_desc);
        }
        self.state.desc_builder().add_tuple(output_tuple_id, None);

        let ret_type = ret_type_descs
            .first()
            .cloned()
            .ok_or_else(|| "table function requires at least one return type".to_string())?;
        let table_function_expr = exprs::TExpr::new(vec![exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: ret_type.clone(),
            num_children: 0,
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: op.function_name.clone(),
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: param_type_descs,
                ret_type,
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: None,
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: Some(types::TTableFunction::new(
                    ret_type_descs,
                    None::<String>,
                    Some(op.is_left_join),
                )),
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..expr_compiler::default_expr_node()
        }]);

        let mut table_fn_plan_node = nodes::default_plan_node();
        table_fn_plan_node.node_id = table_fn_node_id;
        table_fn_plan_node.node_type = plan_nodes::TPlanNodeType::TABLE_FUNCTION_NODE;
        table_fn_plan_node.num_children = 1;
        table_fn_plan_node.limit = -1;
        table_fn_plan_node.row_tuples = vec![output_tuple_id];
        table_fn_plan_node.nullable_tuples = vec![];
        table_fn_plan_node.compact_data = true;
        table_fn_plan_node.table_function_node = Some(plan_nodes::TTableFunctionNode::new(
            Some(table_function_expr),
            Some(param_slots),
            Some(outer_slots),
            Some(fn_result_slots),
            Some(true),
        ));

        Ok((vec![table_fn_plan_node, project_plan_node], output_scope))
    }

    pub(crate) fn lower_assert_one_row(
        &mut self,
        assert_node_id: i32,
        op: &AssertOneRowOp,
        child_tuple_ids: &[i32],
    ) -> plan_nodes::TPlanNode {
        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = assert_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::ASSERT_NUM_ROWS_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        plan_node.row_tuples = child_tuple_ids.to_vec();
        plan_node.nullable_tuples = vec![];
        plan_node.compact_data = true;
        plan_node.assert_num_rows_node = Some(plan_nodes::TAssertNumRowsNode {
            desired_num_rows: Some(1),
            subquery_string: Some(op.subquery_text.clone()),
            assertion: Some(plan_nodes::TAssertion::LE),
        });
        plan_node
    }

    pub(crate) fn lower_decode(
        &mut self,
        decode_node_id: i32,
        decode_tuple_id: i32,
        op: &DecodeOp,
        child_scope: &ExprScope,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope), String> {
        let child_columns: Vec<(String, ColumnBinding)> = child_scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();
        let child_ids_by_slot: Vec<(crate::sql::column_id::ColumnId, ColumnBinding)> = child_scope
            .iter_id_bindings()
            .map(|(column_id, binding)| (*column_id, binding.clone()))
            .collect();

        let dict_target_meta: BTreeMap<
            i32,
            (String, DataType, bool, crate::sql::column_id::ColumnId),
        > = op
            .mappings
            .iter()
            .map(|item| {
                let dict_binding = child_scope
                    .resolve_by_id(item.source_column_id)
                    .ok_or_else(|| {
                        format!(
                            "decode source ColumnId({}) for `{}` is not in child scope",
                            item.source_column_id.0, item.dict_column
                        )
                    })?;
                let declared = op
                    .output_columns
                    .iter()
                    .find(|c| c.column_id == item.output_column_id);
                let data_type = declared
                    .map(|c| c.data_type.clone())
                    .unwrap_or(DataType::Utf8);
                let nullable = declared.map(|c| c.nullable).unwrap_or(true);
                let output_name = declared
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| item.string_column.clone());
                Ok::<_, String>((
                    dict_binding.slot_id,
                    (output_name, data_type, nullable, item.output_column_id),
                ))
            })
            .collect::<Result<_, _>>()?;

        let state = &mut *self.state;
        let mut decode_scope = ExprScope::new();
        let mut mapping: BTreeMap<i32, i32> = BTreeMap::new();
        let mut materialized_dict_slots: BTreeMap<i32, ColumnBinding> = BTreeMap::new();

        let mut col_pos: i32 = 0;
        for (child_name, child_binding) in &child_columns {
            if let Some((string_name, data_type, nullable, output_column_id)) =
                dict_target_meta.get(&child_binding.slot_id)
            {
                if let Some(binding) = materialized_dict_slots.get(&child_binding.slot_id) {
                    decode_scope.add_id_alias(*output_column_id, binding.clone());
                    continue;
                }
                let string_slot_id = state.alloc_slot();
                state.desc_builder().add_slot(
                    string_slot_id,
                    decode_tuple_id,
                    string_name,
                    data_type,
                    *nullable,
                    col_pos,
                );
                mapping.insert(child_binding.slot_id, string_slot_id);
                let output_binding = ColumnBinding {
                    tuple_id: decode_tuple_id,
                    slot_id: string_slot_id,
                    data_type: data_type.clone(),
                    type_desc: None,
                    nullable: *nullable,
                };
                materialized_dict_slots.insert(child_binding.slot_id, output_binding.clone());
                decode_scope.add_column_with_id(
                    *output_column_id,
                    None,
                    string_name.clone(),
                    output_binding,
                );
                col_pos += 1;
            } else {
                state.desc_builder().add_slot(
                    child_binding.slot_id,
                    decode_tuple_id,
                    child_name,
                    &child_binding.data_type,
                    child_binding.nullable,
                    col_pos,
                );
                let output_binding = ColumnBinding {
                    tuple_id: decode_tuple_id,
                    slot_id: child_binding.slot_id,
                    data_type: child_binding.data_type.clone(),
                    type_desc: child_binding.type_desc.clone(),
                    nullable: child_binding.nullable,
                };
                decode_scope.add_column(None, child_name.clone(), output_binding.clone());
                for (column_id, id_binding) in &child_ids_by_slot {
                    if id_binding.tuple_id == child_binding.tuple_id
                        && id_binding.slot_id == child_binding.slot_id
                    {
                        decode_scope.add_id_alias(*column_id, output_binding.clone());
                    }
                }
                col_pos += 1;
            }
        }

        if mapping.len() != op.mappings.len() {
            return Err(format!(
                "decode mappings unresolved: declared {} entries, materialized {}",
                op.mappings.len(),
                mapping.len()
            ));
        }

        state.desc_builder().add_tuple(decode_tuple_id, None);
        let decode_node = nodes::build_decode_node(decode_node_id, vec![decode_tuple_id], mapping);

        Ok((decode_node, decode_scope))
    }

    pub(crate) fn lower_repeat(
        &mut self,
        repeat_node_id: i32,
        virtual_tuple_id: i32,
        op: &RepeatOp,
        child_scope: ExprScope,
        child_tuple_ids: Vec<i32>,
        child_output_columns: Vec<AnalysisOutputColumn>,
    ) -> Result<
        (
            plan_nodes::TPlanNode,
            ExprScope,
            Vec<i32>,
            Vec<AnalysisOutputColumn>,
        ),
        String,
    > {
        let state = &mut *self.state;
        let has_grouping_fns = !op.grouping_fn_args.is_empty();
        let mut output_scope = child_scope;
        let mut output_columns = child_output_columns;

        let num_virtual = 1 + op.grouping_fn_args.len();
        let mut virtual_slot_ids = Vec::with_capacity(num_virtual);

        let grouping_id_slot = state.alloc_slot();
        state.desc_builder().add_slot(
            grouping_id_slot,
            virtual_tuple_id,
            "__grouping_id",
            &DataType::Int64,
            false,
            0,
        );
        if !op.grouping_fn_args.is_empty() {
            output_scope.add_column(
                None,
                "__grouping_id".to_string(),
                ColumnBinding {
                    tuple_id: virtual_tuple_id,
                    slot_id: grouping_id_slot,
                    data_type: DataType::Int64,
                    type_desc: None,
                    nullable: false,
                },
            );
        }
        virtual_slot_ids.push(grouping_id_slot);

        for (fn_idx, (fn_name, _)) in op.grouping_fn_args.iter().enumerate() {
            let slot = state.alloc_slot();
            state.desc_builder().add_slot(
                slot,
                virtual_tuple_id,
                fn_name,
                &DataType::Int64,
                false,
                1 + fn_idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: virtual_tuple_id,
                slot_id: slot,
                data_type: DataType::Int64,
                type_desc: None,
                nullable: false,
            };
            if let Some((_, column_id)) = op.grouping_fn_ids.get(fn_idx) {
                output_scope.add_column_with_id(*column_id, None, fn_name.clone(), binding);
                output_columns.push(AnalysisOutputColumn {
                    column_id: *column_id,
                    name: fn_name.clone(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                });
            } else {
                output_scope.add_internal_column(fn_name.clone(), binding);
            }
            virtual_slot_ids.push(slot);
        }

        state.desc_builder().add_tuple(virtual_tuple_id, None);

        let mut all_rollup_slot_ids = BTreeSet::new();
        for column_id in &op.all_rollup_column_ids {
            let binding = output_scope.resolve_by_id(*column_id).ok_or_else(|| {
                format!(
                    "Repeat rollup ColumnId({}) is not in output scope",
                    column_id.0
                )
            })?;
            all_rollup_slot_ids.insert(binding.slot_id);
        }

        let slot_id_set_list: Vec<BTreeSet<i32>> = op
            .repeat_column_ref_ids
            .iter()
            .map(|non_null_cols| {
                let mut slot_ids = BTreeSet::new();
                for column_id in non_null_cols {
                    let binding = output_scope.resolve_by_id(*column_id).ok_or_else(|| {
                        format!(
                            "Repeat non-null ColumnId({}) is not in output scope",
                            column_id.0
                        )
                    })?;
                    slot_ids.insert(binding.slot_id);
                }
                Ok(slot_ids)
            })
            .collect::<Result<_, String>>()?;

        let repeat_times = op.grouping_ids.len();
        let mut grouping_list: Vec<Vec<i64>> = Vec::with_capacity(num_virtual);
        grouping_list.push(op.grouping_ids.iter().map(|g| *g as i64).collect());

        for fn_args in &op.grouping_fn_arg_ids {
            let mut values = Vec::with_capacity(repeat_times);
            for non_null_cols in &op.repeat_column_ref_ids {
                let mut bits: u64 = 0;
                for (bit_pos, arg_col) in fn_args.iter().enumerate() {
                    let is_null = !non_null_cols.iter().any(|column_id| column_id == arg_col);
                    if is_null {
                        let reverse_bit_pos = fn_args.len() - 1 - bit_pos;
                        bits |= 1 << reverse_bit_pos;
                    }
                }
                values.push(bits as i64);
            }
            grouping_list.push(values);
        }

        let repeat_id_list: Vec<i64> = op.grouping_ids.iter().map(|g| *g as i64).collect();

        let mut row_tuples = child_tuple_ids.clone();
        if has_grouping_fns {
            row_tuples.push(virtual_tuple_id);
        }

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = repeat_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::REPEAT_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        plan_node.row_tuples = row_tuples;
        plan_node.nullable_tuples = vec![];
        plan_node.compact_data = true;
        plan_node.repeat_node = Some(plan_nodes::TRepeatNode {
            output_tuple_id: virtual_tuple_id,
            slot_id_set_list,
            repeat_id_list,
            grouping_list,
            all_slot_ids: all_rollup_slot_ids,
        });

        let mut output_tuple_ids = child_tuple_ids;
        if has_grouping_fns {
            output_tuple_ids.push(virtual_tuple_id);
        }

        Ok((plan_node, output_scope, output_tuple_ids, output_columns))
    }

    fn lower_set_op(
        &mut self,
        set_op_node_id: i32,
        output_tuple_id: i32,
        kind: super::kind::SetOpKind,
        explicit_output_columns: &[AnalysisOutputColumn],
        child_output_columns: &[Vec<AnalysisOutputColumn>],
        child_results: &[LoweredDistributedNode],
    ) -> Result<(plan_nodes::TPlanNode, ExprScope), String> {
        if child_results.is_empty() {
            return Err("set operation node has no inputs".to_string());
        }

        let state = &mut *self.state;
        let output_columns: Vec<AnalysisOutputColumn> = if !explicit_output_columns.is_empty() {
            explicit_output_columns.to_vec()
        } else {
            child_results[0]
                .scope
                .iter_columns()
                .map(|(name, binding)| AnalysisOutputColumn {
                    column_id: crate::sql::column_id::ColumnId::UNSET,
                    name: name.clone(),
                    data_type: binding.data_type.clone(),
                    nullable: binding.nullable,
                    is_internal: false,
                })
                .collect()
        };

        let mut output_scope = ExprScope::new();
        let first_child_cols: Vec<(String, ColumnBinding)> = child_results[0]
            .scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();

        if first_child_cols.len() != output_columns.len() {
            return Err(format!(
                "set operation column count mismatch during codegen: child has {}, output has {}",
                first_child_cols.len(),
                output_columns.len()
            ));
        }

        for (idx, output_col) in output_columns.iter().enumerate() {
            let slot_id = state.alloc_slot();
            state.desc_builder().add_slot(
                slot_id,
                output_tuple_id,
                &output_col.name,
                &output_col.data_type,
                output_col.nullable,
                idx as i32,
            );
            output_scope.add_column_with_id(
                output_col.column_id,
                None,
                output_col.name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: output_col.data_type.clone(),
                    type_desc: None,
                    nullable: output_col.nullable,
                },
            );
        }
        state.desc_builder().add_tuple(output_tuple_id, None);

        if !child_output_columns.is_empty() && child_output_columns.len() != child_results.len() {
            return Err(format!(
                "set operation child_output_columns has {}, inputs has {}",
                child_output_columns.len(),
                child_results.len()
            ));
        }

        let mut result_expr_lists = Vec::with_capacity(child_results.len());
        for (child_idx, child_result) in child_results.iter().enumerate() {
            let fallback_child_columns: Vec<AnalysisOutputColumn>;
            let expected_child_columns = if child_output_columns.is_empty() {
                fallback_child_columns = child_result
                    .scope
                    .iter_columns()
                    .map(|(name, binding)| AnalysisOutputColumn {
                        column_id: crate::sql::column_id::ColumnId::UNSET,
                        name: name.clone(),
                        data_type: binding.data_type.clone(),
                        nullable: binding.nullable,
                        is_internal: false,
                    })
                    .collect();
                &fallback_child_columns
            } else {
                &child_output_columns[child_idx]
            };
            if expected_child_columns.len() != output_columns.len() {
                return Err(format!(
                    "set operation child {} column count mismatch during codegen: child has {}, output has {}",
                    child_idx,
                    expected_child_columns.len(),
                    output_columns.len()
                ));
            }
            let ordered_child_bindings: Vec<_> = child_result.scope.iter_columns().collect();
            let mut expr_list = Vec::new();
            for (col_idx, expected_child_col) in expected_child_columns.iter().enumerate() {
                let output_col = &output_columns[col_idx];
                let child_binding = if expected_child_col.column_id
                    != crate::sql::column_id::ColumnId::UNSET
                {
                    child_result
                        .scope
                        .resolve_by_id(expected_child_col.column_id)
                        .ok_or_else(|| {
                            format!(
                                "set operation child {} output column `{}` id={} is not in child scope",
                                child_idx,
                                expected_child_col.name,
                                expected_child_col.column_id.0
                            )
                        })?
                } else {
                    ordered_child_bindings
                        .get(col_idx)
                        .map(|(_, binding)| *binding)
                        .ok_or_else(|| {
                            format!(
                                "set operation child {} missing positional column {}",
                                child_idx, col_idx
                            )
                        })?
                };
                let needs_cast = child_binding.data_type != output_col.data_type;
                if needs_cast {
                    let target_desc = type_infer::arrow_type_to_type_desc(&output_col.data_type)?;
                    let child_desc = expr_compiler::binding_type_desc(child_binding)?;
                    let slot_ref = expr_compiler::build_slot_ref_texpr(
                        child_binding.slot_id,
                        child_binding.tuple_id,
                        child_desc,
                    );
                    expr_list.push(expr_compiler::build_cast_texpr(slot_ref, target_desc));
                } else {
                    let type_desc = expr_compiler::binding_type_desc(child_binding)?;
                    expr_list.push(expr_compiler::build_slot_ref_texpr(
                        child_binding.slot_id,
                        child_binding.tuple_id,
                        type_desc,
                    ));
                }
            }
            result_expr_lists.push(expr_list);
        }

        let tnode = plan_nodes::TUnionNode {
            tuple_id: output_tuple_id,
            result_expr_lists,
            const_expr_lists: vec![],
            first_materialized_child_idx: 0,
            pass_through_slot_maps: None,
            local_exchanger_type: None,
            local_partition_by_exprs: None,
        };

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = set_op_node_id;
        plan_node.node_type = match kind {
            super::kind::SetOpKind::UnionAll => plan_nodes::TPlanNodeType::UNION_NODE,
            super::kind::SetOpKind::Intersect => plan_nodes::TPlanNodeType::INTERSECT_NODE,
            super::kind::SetOpKind::Except => plan_nodes::TPlanNodeType::EXCEPT_NODE,
        };
        plan_node.row_tuples = vec![output_tuple_id];
        plan_node.nullable_tuples = vec![];
        plan_node.num_children = child_results.len() as i32;

        match kind {
            super::kind::SetOpKind::UnionAll => {
                plan_node.union_node = Some(tnode);
            }
            super::kind::SetOpKind::Intersect => {
                plan_node.intersect_node = Some(plan_nodes::TIntersectNode {
                    tuple_id: tnode.tuple_id,
                    result_expr_lists: tnode.result_expr_lists,
                    const_expr_lists: tnode.const_expr_lists,
                    first_materialized_child_idx: tnode.first_materialized_child_idx,
                    has_outer_join_child: None,
                    local_partition_by_exprs: None,
                });
            }
            super::kind::SetOpKind::Except => {
                plan_node.except_node = Some(plan_nodes::TExceptNode {
                    tuple_id: tnode.tuple_id,
                    result_expr_lists: tnode.result_expr_lists,
                    const_expr_lists: tnode.const_expr_lists,
                    first_materialized_child_idx: tnode.first_materialized_child_idx,
                    local_partition_by_exprs: None,
                });
            }
        }

        Ok((plan_node, output_scope))
    }
}

fn first_tuple_id(
    node: &crate::sql::planner::DistributedPlanNode,
    operator_name: &str,
) -> Result<i32, String> {
    node.tuple_ids.first().copied().ok_or_else(|| {
        format!(
            "DistributedPlan {operator_name} node_id={} has no output tuple id",
            node.node_id
        )
    })
}

fn binary_children<'a>(
    node: &'a crate::sql::planner::DistributedPlanNode,
    operator_name: &str,
) -> Result<
    (
        &'a crate::sql::planner::DistributedPlanNode,
        &'a crate::sql::planner::DistributedPlanNode,
    ),
    String,
> {
    if node.children.len() != 2 {
        return Err(format!(
            "DistributedPlan {operator_name} node_id={} expected 2 children, got {}",
            node.node_id,
            node.children.len()
        ));
    }
    Ok((&node.children[0], &node.children[1]))
}

fn analytic_window_from_expr(win_expr: &WindowExpr) -> Option<plan_nodes::TAnalyticWindow> {
    use crate::sql::analysis::{WindowBound, WindowFrameType};

    win_expr.window_frame.as_ref().map(|frame| {
        let window_type = match frame.frame_type {
            WindowFrameType::Rows => plan_nodes::TAnalyticWindowType::ROWS,
            WindowFrameType::Range => plan_nodes::TAnalyticWindowType::RANGE,
        };
        let window_start = match &frame.start {
            WindowBound::UnboundedPreceding => None,
            WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                range_offset_predicate: None,
                rows_offset_value: None,
            }),
            WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                range_offset_predicate: None,
                rows_offset_value: Some(*n),
            }),
            WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                range_offset_predicate: None,
                rows_offset_value: Some(*n),
            }),
            WindowBound::UnboundedFollowing => None,
        };
        let window_end = match &frame.end {
            WindowBound::UnboundedFollowing => None,
            WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                range_offset_predicate: None,
                rows_offset_value: None,
            }),
            WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                range_offset_predicate: None,
                rows_offset_value: Some(*n),
            }),
            WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                range_offset_predicate: None,
                rows_offset_value: Some(*n),
            }),
            WindowBound::UnboundedPreceding => None,
        };
        plan_nodes::TAnalyticWindow {
            type_: window_type,
            window_start,
            window_end,
        }
    })
}

fn apply_ignore_nulls_to_root_fn(texpr: &mut exprs::TExpr, ignore_nulls: bool) {
    if !ignore_nulls {
        return;
    }
    if let Some(root) = texpr.nodes.first_mut()
        && let Some(fn_) = root.fn_.as_mut()
    {
        fn_.ignore_nulls = Some(true);
    }
}

fn assert_one_row_node_to_physical_op(
    kind: &super::kind::DistributedAssertOneRowNode,
) -> AssertOneRowOp {
    AssertOneRowOp {
        subquery_text: kind.subquery_text.clone(),
    }
}

fn decode_node_to_physical_op(kind: &super::kind::DistributedDecodeNode) -> DecodeOp {
    DecodeOp {
        mappings: kind.mappings.clone(),
        output_columns: kind.output_columns.clone(),
    }
}

fn repeat_node_to_physical_op(kind: &super::kind::DistributedRepeatNode) -> RepeatOp {
    RepeatOp {
        repeat_column_ref_list: kind.repeat_column_ref_list.clone(),
        repeat_column_ref_ids: kind.repeat_column_ref_ids.clone(),
        grouping_ids: kind.grouping_ids.clone(),
        all_rollup_columns: kind.all_rollup_columns.clone(),
        all_rollup_column_ids: kind.all_rollup_column_ids.clone(),
        grouping_key_aliases: kind.grouping_key_aliases.clone(),
        grouping_fn_args: kind.grouping_fn_args.clone(),
        grouping_fn_arg_ids: kind.grouping_fn_arg_ids.clone(),
        grouping_fn_ids: kind.grouping_fn_ids.clone(),
    }
}

fn generate_series_node_to_physical_op(
    kind: &super::kind::DistributedGenerateSeriesNode,
) -> GenerateSeriesOp {
    GenerateSeriesOp {
        start: kind.start,
        end: kind.end,
        step: kind.step,
        column_name: kind.column_name.clone(),
        alias: kind.alias.clone(),
        output_column_id: kind.output_column_id,
    }
}

fn project_node_output_columns(
    kind: &super::kind::DistributedProjectNode,
) -> Vec<AnalysisOutputColumn> {
    kind.items
        .iter()
        .map(|item| AnalysisOutputColumn {
            column_id: item.output_column_id,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        })
        .collect()
}

fn result_output_exprs_for_columns(
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
) -> Result<Option<Vec<exprs::TExpr>>, String> {
    slot_ref_exprs_for_columns(scope, output_columns, "result sink")
}

pub(in crate::sql::codegen) fn slot_ref_exprs_for_columns(
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
    context: &str,
) -> Result<Option<Vec<exprs::TExpr>>, String> {
    if output_columns.is_empty() {
        return Ok(None);
    }

    let mut exprs = Vec::with_capacity(output_columns.len());
    for column in output_columns {
        let binding = scope.resolve_by_id(column.column_id).ok_or_else(|| {
            format!(
                "{} cannot resolve output column `{}` id={}",
                context, column.name, column.column_id.0
            )
        })?;
        let type_desc = expr_compiler::binding_type_desc(binding)?;
        exprs.push(expr_compiler::build_slot_ref_texpr(
            binding.slot_id,
            binding.tuple_id,
            type_desc,
        ));
    }
    Ok(Some(exprs))
}

fn refresh_scan_table_for_codegen(
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    table: &TableDef,
) -> Result<TableDef, String> {
    match &table.source {
        ScanSource::IcebergVersionTable {
            table: iceberg_table,
            snapshot_id,
        } => {
            let refresh_ctx = mv_refresh_ctx
                .ok_or_else(|| "Iceberg version scan requires MV refresh context".to_string())?;
            let mut out = table.clone();
            out.source = refresh_ctx.version_scan_source(iceberg_table, *snapshot_id)?;
            Ok(out)
        }
        ScanSource::IcebergMvTargetState(scan) => {
            let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "Iceberg target-state scan requires MV refresh context".to_string()
            })?;
            let mut out = table.clone();
            let projected = nodes::projected_target_state_column_names(scan);
            out.columns.retain(|column| {
                projected
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&column.name))
            });
            out.iceberg_row_lineage_metadata_columns.retain(|column| {
                projected
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&column.name))
            });
            if projected
                .iter()
                .any(|name| name.eq_ignore_ascii_case("_row_id"))
                && !out
                    .columns
                    .iter()
                    .chain(out.iceberg_row_lineage_metadata_columns.iter())
                    .any(|column| column.name.eq_ignore_ascii_case("_row_id"))
            {
                out.iceberg_row_lineage_metadata_columns
                    .push(crate::sql::catalog::ColumnDef {
                        name: "_row_id".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    });
            }
            out.source = refresh_ctx.target_state_scan_source(scan)?;
            nodes::reject_target_state_equality_deletes(&out.source)?;
            Ok(out)
        }
        _ => Ok(table.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::connector::ConnectorRegistry;
    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::lower::type_lowering::arrow_type_from_desc;
    use crate::plan_nodes::TPlanNodeType;
    use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::codegen::boundary_schema::BoundaryKind;
    use crate::sql::codegen::expr_compiler::infer_agg_function_types;
    use crate::sql::codegen::fragment_builder::PlanFragmentBuilder;
    use crate::sql::codegen::ir::kind::{
        DistributedExchangeNode, DistributedHashJoinEqCondition, DistributedHashJoinNode,
        DistributedNestLoopJoinNode, DistributedValuesNode, ExchangeFlavor,
    };
    use crate::sql::codegen::ir::{
        DataPartition, DataSink, DistributedPlan, DistributedPlanNode, PartitionKind, PlanFragment,
        PlanNodeKind, PlanNodeStats, build_distributed_plan,
    };
    use crate::sql::codegen::resolve::{ColumnBinding, ExprScope};
    use crate::sql::codegen::{FragmentEdge, FragmentEdgeKind, FragmentStreamKind};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{JoinDistribution, Operator, ProjectOp, ScanOp};
    use crate::sql::optimizer::physical_plan::{
        PhysicalPlanNode, PlanExecutionProps, attach_scalar_arena,
    };
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::optimizer_bridge::scalar::intern_project_items;

    #[test]
    fn aggregate_slot_contract_uses_intermediate_only_for_non_finalize() {
        let (_, avg_intermediate) =
            infer_agg_function_types("avg", &[DataType::Int64], false).expect("avg types");
        let avg_intermediate = avg_intermediate.expect("avg intermediate");
        let contract = super::aggregate_slot_contract_for_phase(
            false,
            &DataType::Float64,
            Some(&avg_intermediate),
            "avg",
        )
        .expect("local avg contract");
        assert_eq!(contract.data_type, DataType::Utf8);
        assert_eq!(
            arrow_type_from_desc(&contract.type_desc),
            Some(DataType::Utf8)
        );

        let final_contract = super::aggregate_slot_contract_for_phase(
            true,
            &DataType::Float64,
            Some(&avg_intermediate),
            "avg",
        )
        .expect("final avg contract");
        assert_eq!(final_contract.data_type, DataType::Float64);
        assert_eq!(
            arrow_type_from_desc(&final_contract.type_desc),
            Some(DataType::Float64)
        );
    }

    #[test]
    fn build_via_distributed_plan_lowers_project_over_scan() {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let result = PlanFragmentBuilder::build_via_distributed_plan(
            &project_over_metadata_scan_plan(),
            &catalog,
            &connectors,
            "test_db",
        )
        .expect("build_via_distributed_plan");

        assert_eq!(result.root_fragment_id, 0);
        assert_eq!(result.fragment_results.len(), 1);
        let root = result
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == result.root_fragment_id)
            .expect("root fragment");
        let node_types: Vec<TPlanNodeType> =
            root.plan.nodes.iter().map(|node| node.node_type).collect();
        assert_eq!(
            node_types,
            vec![TPlanNodeType::PROJECT_NODE, TPlanNodeType::HDFS_SCAN_NODE]
        );
        assert!(
            root.desc_tbl.tuple_descriptors.len() >= 2,
            "project and scan tuples should be registered"
        );
        assert!(
            root.desc_tbl
                .slot_descriptors
                .as_ref()
                .expect("slot descriptors")
                .len()
                >= 2,
            "project and scan slots should be registered"
        );
    }

    #[test]
    fn null_aware_left_anti_hash_join_exposes_left_scope_and_widens_build_side() {
        let connectors = ConnectorRegistry::new();
        let mut state = super::OwnedLoweringState::new(&connectors, None, 0);
        let (left_column_id, right_column_id, left_key, right_key, left_scope, right_scope) =
            hash_join_test_inputs(&mut state);
        let op = DistributedHashJoinNode {
            join_type: JoinKind::NullAwareLeftAnti,
            eq_conditions: vec![DistributedHashJoinEqCondition {
                left: left_key,
                right: right_key,
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
        };

        let (plan_node, scope, tuple_ids) = {
            let mut ctx = super::LoweringCtx::new(&mut state);
            ctx.lower_hash_join(10, &[1], &[2], &op, left_scope, right_scope, None, &[])
                .expect("lower hash join")
        };

        assert_eq!(tuple_ids, vec![1, 2]);
        assert!(
            scope.resolve_by_id(left_column_id).is_some(),
            "left side should remain visible"
        );
        assert!(
            scope.resolve_by_id(right_column_id).is_none(),
            "build side should not be visible"
        );
        assert_eq!(plan_node.nullable_tuples, vec![false, true]);

        let desc_tbl = state.desc_builder.build();
        let slots = desc_tbl.slot_descriptors.expect("slot descriptors");
        let left_slot = slots
            .iter()
            .find(|slot| slot.id == Some(11))
            .expect("left slot descriptor");
        let right_slot = slots
            .iter()
            .find(|slot| slot.id == Some(22))
            .expect("right slot descriptor");
        assert_eq!(left_slot.is_nullable, Some(false));
        assert_eq!(right_slot.is_nullable, Some(true));
    }

    #[test]
    fn hash_join_demotes_unbound_eq_to_other_conjunct() {
        let connectors = ConnectorRegistry::new();
        let mut state = super::OwnedLoweringState::new(&connectors, None, 0);
        let (left_column_id, _, _, _, left_scope, right_scope) = hash_join_test_inputs(&mut state);
        let op = DistributedHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![DistributedHashJoinEqCondition {
                left: qualified_column_ref(left_column_id, "l", "k", false),
                right: qualified_column_ref(left_column_id, "l", "k", false),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
        };

        let plan_node = {
            let mut ctx = super::LoweringCtx::new(&mut state);
            ctx.lower_hash_join(10, &[1], &[2], &op, left_scope, right_scope, None, &[])
                .expect("lower hash join")
                .0
        };
        let hash_join = plan_node.hash_join_node.expect("hash join node");
        assert!(hash_join.eq_join_conjuncts.is_empty());
        assert_eq!(
            hash_join
                .other_join_conjuncts
                .as_ref()
                .expect("demoted conjunct")
                .len(),
            1
        );
    }

    #[test]
    fn hash_join_compiles_swapped_eq_as_join_conjunct() {
        let connectors = ConnectorRegistry::new();
        let mut state = super::OwnedLoweringState::new(&connectors, None, 0);
        let (_, _, left_key, right_key, left_scope, right_scope) =
            hash_join_test_inputs(&mut state);

        let op = DistributedHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![DistributedHashJoinEqCondition {
                left: right_key,
                right: left_key,
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
        };

        let plan_node = {
            let mut ctx = super::LoweringCtx::new(&mut state);
            ctx.lower_hash_join(10, &[1], &[2], &op, left_scope, right_scope, None, &[])
                .expect("lower hash join")
                .0
        };
        let hash_join = plan_node.hash_join_node.expect("hash join node");
        assert_eq!(hash_join.eq_join_conjuncts.len(), 1);
        assert!(hash_join.other_join_conjuncts.is_none());
    }

    #[test]
    fn lower_distributed_plan_accepts_multi_fragment_result_and_noop_children() {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let dp = distributed_values_multi_fragment_plan();

        let result = super::lower_distributed_plan(&dp, &catalog, &connectors, None)
            .expect("multi fragment lower");

        assert_eq!(result.root_fragment_id, 1);
        assert_eq!(result.fragment_results.len(), 2);
        let root = result
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("root fragment");
        let child = result
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("child fragment");
        assert_eq!(
            root.output_sink.type_,
            crate::data_sinks::TDataSinkType::RESULT_SINK
        );
        assert_eq!(
            child.output_sink.type_,
            crate::data_sinks::TDataSinkType::NOOP_SINK
        );
        assert_eq!(root.desc_tbl, child.desc_tbl);
        assert_eq!(root.exec_params, child.exec_params);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].source_fragment_id, 0);
        assert_eq!(result.edges[0].target_fragment_id, 1);
        assert_eq!(result.edges[0].target_exchange_node_id, 20);
        assert_eq!(result.boundary_schemas.len(), 4);
        assert_eq!(result.boundary_schemas[0].fragment_id, Some(0));
        assert_eq!(result.boundary_schemas[0].node_id, 10);
        assert_eq!(
            result.boundary_schemas[0].boundary_kind,
            BoundaryKind::ResultRoot
        );
        assert_eq!(result.boundary_schemas[1].fragment_id, Some(1));
        assert_eq!(result.boundary_schemas[1].node_id, 20);
        assert_eq!(
            result.boundary_schemas[1].boundary_kind,
            BoundaryKind::ResultRoot
        );
        assert_eq!(result.boundary_schemas[2].fragment_id, Some(0));
        assert_eq!(result.boundary_schemas[2].node_id, 20);
        assert_eq!(
            result.boundary_schemas[2].boundary_kind,
            BoundaryKind::ExchangeSender
        );
        assert_eq!(result.boundary_schemas[2].columns.len(), 1);
        assert_eq!(result.boundary_schemas[2].columns[0].name, "child_k");
        assert_eq!(result.boundary_schemas[3].fragment_id, Some(1));
        assert_eq!(result.boundary_schemas[3].node_id, 20);
        assert_eq!(
            result.boundary_schemas[3].boundary_kind,
            BoundaryKind::ExchangeReceiver
        );
        assert_eq!(result.boundary_schemas[3].columns.len(), 1);
        assert_eq!(result.boundary_schemas[3].columns[0].name, "child_k");
    }

    #[test]
    fn lower_distributed_plan_lowers_fragments_in_edge_topological_order() {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let dp = distributed_values_three_fragment_chain_reverse_input();

        let result = super::lower_distributed_plan(&dp, &catalog, &connectors, None)
            .expect("multi fragment lower");
        let order: Vec<u32> = result
            .fragment_results
            .iter()
            .map(|fragment| fragment.fragment_id)
            .collect();

        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn null_aware_left_anti_nest_loop_join_exposes_left_scope_and_widens_build_side() {
        let connectors = ConnectorRegistry::new();
        let mut state = super::OwnedLoweringState::new(&connectors, None, 0);
        let (left_column_id, right_column_id, _, _, left_scope, right_scope) =
            hash_join_test_inputs(&mut state);
        let op = DistributedNestLoopJoinNode {
            join_type: JoinKind::NullAwareLeftAnti,
            condition: None,
        };

        let (plan_node, scope, tuple_ids) = {
            let mut ctx = super::LoweringCtx::new(&mut state);
            ctx.lower_nest_loop_join(10, &[1], &[2], &op, left_scope, right_scope)
                .expect("lower nest loop join")
        };

        assert_eq!(tuple_ids, vec![1, 2]);
        assert!(
            scope.resolve_by_id(left_column_id).is_some(),
            "left side should remain visible"
        );
        assert!(
            scope.resolve_by_id(right_column_id).is_none(),
            "build side should not be visible"
        );
        assert_eq!(plan_node.nullable_tuples, vec![false, true]);

        let desc_tbl = state.desc_builder.build();
        let slots = desc_tbl.slot_descriptors.expect("slot descriptors");
        let left_slot = slots
            .iter()
            .find(|slot| slot.id == Some(11))
            .expect("left slot descriptor");
        let right_slot = slots
            .iter()
            .find(|slot| slot.id == Some(22))
            .expect("right slot descriptor");
        assert_eq!(left_slot.is_nullable, Some(false));
        assert_eq!(right_slot.is_nullable, Some(true));
    }

    fn hash_join_test_inputs(
        state: &mut super::OwnedLoweringState<'_>,
    ) -> (
        ColumnId,
        ColumnId,
        TypedExpr,
        TypedExpr,
        ExprScope,
        ExprScope,
    ) {
        state.desc_builder.add_tuple(1, None);
        state
            .desc_builder
            .add_slot(11, 1, "left_k", &DataType::Int64, false, 0);
        state.desc_builder.add_tuple(2, None);
        state
            .desc_builder
            .add_slot(22, 2, "right_k", &DataType::Int64, false, 0);

        let left_column_id = ColumnId::new_for_test(1);
        let right_column_id = ColumnId::new_for_test(3);
        let left_key = qualified_column_ref(left_column_id, "l", "k", false);
        let right_key = qualified_column_ref(right_column_id, "r", "k", false);
        let mut left_scope = ExprScope::new();
        left_scope.add_column_with_id(
            left_column_id,
            Some("l".to_string()),
            "k".to_string(),
            column_binding(1, 11, false),
        );
        let mut right_scope = ExprScope::new();
        right_scope.add_column_with_id(
            right_column_id,
            Some("r".to_string()),
            "k".to_string(),
            column_binding(2, 22, false),
        );
        (
            left_column_id,
            right_column_id,
            left_key,
            right_key,
            left_scope,
            right_scope,
        )
    }

    #[test]
    fn lower_distributed_plan_rejects_non_m0_fragment_shape() {
        let mut root_mismatch = distributed_project_scan_plan();
        root_mismatch.root_fragment_id = 99;
        root_mismatch.fragments[0].sink = DataSink::Noop;
        assert_lowering_err(
            &root_mismatch,
            "lower_distributed_plan root fragment id=99 was not found",
        );

        let mut noop_sink = distributed_project_scan_plan();
        noop_sink.fragments[0].sink = DataSink::Noop;
        assert_lowering_err(
            &noop_sink,
            "lower_distributed_plan root fragment id=0 must use result sink",
        );

        let mut random_partition = distributed_project_scan_plan();
        random_partition.fragments[0].data_partition = DataPartition {
            kind: PartitionKind::Random,
            exprs: vec![],
        };
        assert_lowering_err(
            &random_partition,
            "lower_distributed_plan supports only unpartitioned data_partition",
        );

        let mut output_exprs = distributed_project_scan_plan();
        output_exprs.fragments[0].output_exprs = Some(vec![]);
        assert_lowering_err(
            &output_exprs,
            "lower_distributed_plan does not support fragment output_exprs",
        );

        let mut wrong_owner = distributed_project_scan_plan();
        wrong_owner.fragments[0].root.children[0].fragment_id = 42;
        assert_lowering_err(
            &wrong_owner,
            "fragment id=0 contains node_id=1 with fragment_id=42",
        );

        let mut missing_target_node = distributed_values_multi_fragment_plan();
        missing_target_node.edges[0].target_exchange_node_id = 99;
        assert_lowering_err(
            &missing_target_node,
            "edge target_exchange_node_id=99 not found in target fragment id=1",
        );

        let mut mismatched_source = distributed_values_multi_fragment_plan();
        let PlanNodeKind::Exchange(exchange) = &mut mismatched_source.fragments[1].root.kind else {
            panic!("root should be exchange");
        };
        exchange.source_fragment_id = 42;
        assert_lowering_err(
            &mismatched_source,
            "stream edge source_fragment_id=0 does not match Exchange source_fragment_id=42",
        );

        let mut cyclic = distributed_values_multi_fragment_plan();
        cyclic.fragments[0].root = distributed_exchange_node(10, 0, 10, 1);
        cyclic.edges.push(fragment_edge(1, 0, 10));
        assert_lowering_err(&cyclic, "cycle in DistributedPlan fragment edges");

        let mut disconnected_noop = distributed_values_multi_fragment_plan();
        disconnected_noop.edges.clear();
        assert_lowering_err(
            &disconnected_noop,
            "disconnected non-root fragment id=0 has no outgoing edge toward root fragment id=1",
        );
    }

    struct DummyCatalog;

    impl CatalogProvider for DummyCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used by distributed-plan lowering test".to_string())
        }
    }

    fn distributed_project_scan_plan() -> DistributedPlan {
        build_distributed_plan(&project_over_metadata_scan_plan()).expect("build DistributedPlan")
    }

    fn distributed_values_multi_fragment_plan() -> DistributedPlan {
        let child_columns = vec![output_col(1, "child_k", DataType::Int64, false)];
        DistributedPlan {
            fragments: vec![
                PlanFragment {
                    fragment_id: 0,
                    root: distributed_values_node(10, 0, 10, child_columns.clone()),
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition {
                        kind: PartitionKind::Random,
                        exprs: vec![],
                    },
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: child_columns.clone(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 1,
                    root: distributed_exchange_node(20, 1, 10, 0),
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: child_columns.clone(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 1,
            edges: vec![fragment_edge(0, 1, 20)],
            scalar_arena: Arc::new(ScalarArena::new()),
        }
    }

    fn distributed_values_three_fragment_chain_reverse_input() -> DistributedPlan {
        let source_columns = vec![output_col(1, "source_k", DataType::Int64, false)];
        DistributedPlan {
            fragments: vec![
                PlanFragment {
                    fragment_id: 2,
                    root: distributed_exchange_node(20, 2, 10, 1),
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: source_columns.clone(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 1,
                    root: distributed_exchange_node(30, 1, 10, 0),
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition {
                        kind: PartitionKind::Random,
                        exprs: vec![],
                    },
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: source_columns.clone(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: distributed_values_node(10, 0, 10, source_columns.clone()),
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition {
                        kind: PartitionKind::Random,
                        exprs: vec![],
                    },
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: source_columns,
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 2,
            edges: vec![fragment_edge(0, 1, 30), fragment_edge(1, 2, 20)],
            scalar_arena: Arc::new(ScalarArena::new()),
        }
    }

    fn fragment_edge(
        source_fragment_id: u32,
        target_fragment_id: u32,
        target_exchange_node_id: i32,
    ) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id,
            target_fragment_id,
            target_exchange_node_id,
            output_partition: crate::partitions::TDataPartition::new(
                crate::partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::exprs::TExpr>>,
                None::<Vec<crate::partitions::TRangePartition>>,
                None::<Vec<crate::partitions::TBucketProperty>>,
            ),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
        }
    }

    fn distributed_values_node(
        node_id: i32,
        fragment_id: u32,
        tuple_id: i32,
        columns: Vec<OutputColumn>,
    ) -> DistributedPlanNode {
        DistributedPlanNode {
            node_id,
            fragment_id,
            tuple_ids: vec![tuple_id],
            nullable_tuple_ids: vec![],
            limit: -1,
            execution_join_distribution: None,
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
            children: vec![],
            stats: PlanNodeStats::from_statistics(&Statistics::default()),
            kind: PlanNodeKind::Values(DistributedValuesNode {
                rows: vec![],
                columns,
            }),
        }
    }

    fn distributed_exchange_node(
        node_id: i32,
        fragment_id: u32,
        source_tuple_id: i32,
        source_fragment_id: u32,
    ) -> DistributedPlanNode {
        DistributedPlanNode {
            node_id,
            fragment_id,
            tuple_ids: vec![source_tuple_id],
            nullable_tuple_ids: vec![],
            limit: -1,
            execution_join_distribution: None,
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
            children: vec![],
            stats: PlanNodeStats::from_statistics(&Statistics::default()),
            kind: PlanNodeKind::Exchange(DistributedExchangeNode {
                partition_type: crate::partitions::TPartitionType::UNPARTITIONED,
                partition_exprs: vec![],
                source_fragment_id,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            }),
        }
    }

    fn assert_lowering_err(dp: &DistributedPlan, expected: &str) {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let err = match super::lower_distributed_plan(dp, &catalog, &connectors, None) {
            Ok(_) => panic!("expected lowering error containing `{expected}`"),
            Err(err) => err,
        };
        assert!(
            err.contains(expected),
            "expected `{expected}` in lowering error `{err}`"
        );
    }

    fn column_binding(tuple_id: i32, slot_id: i32, nullable: bool) -> ColumnBinding {
        ColumnBinding {
            tuple_id,
            slot_id,
            data_type: DataType::Int64,
            type_desc: None,
            nullable,
        }
    }

    fn qualified_column_ref(
        column_id: ColumnId,
        qualifier: &str,
        column: &str,
        nullable: bool,
    ) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: Some(qualifier.to_string()),
                column: column.to_string(),
            },
            data_type: DataType::Int64,
            nullable,
        }
    }

    fn project_over_metadata_scan_plan() -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let scan = physical_node(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: metadata_table_def(),
                alias: Some("t".to_string()),
                columns: vec![k.clone()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k],
        );

        let project_output = output_col(1, "k", DataType::Int64, false);
        let mut scalars = scan
            .execution_props
            .scalar_arena
            .as_deref()
            .cloned()
            .unwrap_or_else(ScalarArena::new);
        let items = vec![ProjectItem {
            expr: column_ref_expr(1, "k", DataType::Int64, false),
            output_name: "k".to_string(),
            output_column_id: ColumnId::new_for_test(1),
        }];
        let mut plan = physical_node(
            Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &items),
                output_qualifier: None,
            }),
            vec![scan],
            vec![project_output],
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn physical_node(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
    ) -> PhysicalPlanNode {
        let scalars = children
            .iter()
            .find_map(|child| child.execution_props.scalar_arena.as_deref().cloned())
            .unwrap_or_else(ScalarArena::new);
        let mut plan = PhysicalPlanNode {
            op,
            children,
            stats: Statistics::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn metadata_table_def() -> TableDef {
        TableDef {
            name: "t$snapshots".to_string(),
            columns: vec![column_def("k", DataType::Int64, false)],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergMetadataTable {
                table: iceberg_table_info(),
                metadata_table_type: IcebergMetadataTableType::Snapshots,
                serialized_table: "{}".to_string(),
                cloud_properties: Default::default(),
                metadata_payload: None,
            },
        }
    }

    fn iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "t".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///warehouse/t".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
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
}
