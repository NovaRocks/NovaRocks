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
mod aggregate;
mod analytic;
mod assert;
mod cross_join;
mod decode;
mod empty_set;
mod exchange;
mod fetch;
mod file_scan;
mod hash_join;
pub(crate) mod hdfs_scan;
mod jdbc_scan;
mod lake_meta_scan;
mod lake_scan;
mod lookup;
mod mysql_scan;
mod nestloop_join;
mod project;
mod raw_values;
mod repeat;
mod schema_scan;
mod select;
mod set_op;
mod sort;
mod starrocks_scan;
mod table_function;
mod union;

use crate::common::ids::SlotId;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::node::filter::FilterNode;
use crate::exec::node::limit::LimitNode;
use crate::exec::node::{ExecNode, ExecNodeKind, RuntimeFilterProbeSpec};
use crate::novarocks_connectors::ConnectorRegistry;
use crate::novarocks_logging::warn;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::lower::expr::lower_t_expr_with_common_slot_map;
use crate::lower::layout::{Layout, layout_for_row_tuples};
use crate::{data, descriptors, exprs, internal_service, plan_nodes, runtime_filter, types};

pub(crate) use aggregate::lower_aggregate_node;
pub(crate) use analytic::lower_analytic_node;
pub(crate) use assert::lower_assert_num_rows_node;
pub(crate) use cross_join::lower_cross_join_node;
pub(crate) use decode::{QueryGlobalDictMap, build_query_global_dict_map, lower_decode_node};
pub(crate) use empty_set::lower_empty_set_node;
pub(crate) use exchange::lower_exchange_node;
pub(crate) use fetch::lower_fetch_node;
pub(crate) use file_scan::lower_file_scan_node;
pub(crate) use hash_join::lower_hash_join_node;
pub(crate) use hdfs_scan::lower_hdfs_scan_node;
pub(crate) use jdbc_scan::lower_jdbc_scan_node;
pub(crate) use lake_meta_scan::lower_lake_meta_scan_node;
pub(crate) use lake_scan::lower_lake_scan_node;
pub(crate) use lookup::lower_lookup_node;
pub(crate) use mysql_scan::lower_mysql_scan_node;
pub(crate) use nestloop_join::lower_nestloop_join_node;
pub(crate) use project::lower_project_node;
pub(crate) use raw_values::lower_raw_values_node;
pub(crate) use repeat::lower_repeat_node;
pub(crate) use schema_scan::lower_schema_scan_node;
pub(crate) use select::lower_select_node;
pub(crate) use set_op::{lower_except_node, lower_intersect_node};
pub(crate) use sort::lower_sort_node;
pub(crate) use starrocks_scan::lower_starrocks_scan_node;
pub(crate) use table_function::lower_table_function_node;
pub(crate) use union::lower_union_node;

#[derive(Clone, Debug)]
pub(crate) struct Lowered {
    pub(crate) node: ExecNode,
    pub(crate) layout: Layout,
}

fn collect_global_common_slot_map(
    nodes: &[plan_nodes::TPlanNode],
) -> BTreeMap<types::TSlotId, exprs::TExpr> {
    let mut merged = BTreeMap::new();
    for node in nodes {
        let mut maps: Vec<&BTreeMap<types::TSlotId, exprs::TExpr>> = Vec::new();
        if let Some(map) = node
            .select_node
            .as_ref()
            .and_then(|n| n.common_slot_map.as_ref())
        {
            maps.push(map);
        }
        if let Some(map) = node
            .hash_join_node
            .as_ref()
            .and_then(|n| n.common_slot_map.as_ref())
        {
            maps.push(map);
        }
        if let Some(map) = node
            .nestloop_join_node
            .as_ref()
            .and_then(|n| n.common_slot_map.as_ref())
        {
            maps.push(map);
        }
        if let Some(map) = node
            .project_node
            .as_ref()
            .and_then(|n| n.common_slot_map.as_ref())
        {
            maps.push(map);
        }
        if let Some(map) = node.common.as_ref().and_then(|n| n.heavy_exprs.as_ref()) {
            maps.push(map);
        }
        for map in maps {
            for (&slot_id, expr) in map {
                merged.entry(slot_id).or_insert_with(|| expr.clone());
            }
        }
    }
    merged
}

pub(crate) fn lower_plan(
    plan: &plan_nodes::TPlan,
    arena: &mut ExprArena,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    query_global_dicts: Option<&[data::TGlobalDict]>,
    query_global_dict_exprs: Option<&BTreeMap<i32, exprs::TExpr>>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    query_opts: Option<&internal_service::TQueryOptions>,
    db_name: Option<&str>,
    connectors: &ConnectorRegistry,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let mut idx = 0usize;
    let global_common_slot_map = collect_global_common_slot_map(&plan.nodes);
    let query_global_dict_map =
        build_query_global_dict_map(query_global_dicts, query_global_dict_exprs)?;
    let mut arena_query_global_dicts = HashMap::new();
    for (slot_id, dict) in &query_global_dict_map {
        let slot_id = SlotId::try_from(*slot_id)?;
        arena_query_global_dicts.insert(slot_id, dict.clone());
    }
    arena.set_query_global_dicts(arena_query_global_dicts);
    let lowered = lower_node(
        &plan.nodes,
        &mut idx,
        arena,
        tuple_slots,
        desc_tbl,
        &query_global_dict_map,
        exec_params,
        query_opts,
        db_name,
        connectors,
        layout_hints,
        &global_common_slot_map,
        last_query_id,
        fe_addr,
    )?;
    if idx != plan.nodes.len() {
        // best-effort: ignore trailing nodes
    }
    Ok({
        // Apply limit at root if present.
        lowered
    })
}

struct LowerNodeFrame {
    node_index: usize,
    expected_children: usize,
    children: Vec<Lowered>,
}

impl LowerNodeFrame {
    fn new(node_index: usize, expected_children: usize) -> Self {
        Self {
            node_index,
            expected_children,
            children: Vec::with_capacity(expected_children),
        }
    }
}

fn expected_children(node: &plan_nodes::TPlanNode) -> Result<usize, String> {
    if node.num_children < 0 {
        return Err(format!(
            "invalid plan node: node_id={} has negative num_children={}",
            node.node_id, node.num_children
        ));
    }
    Ok(node.num_children as usize)
}

fn lower_node(
    nodes: &[plan_nodes::TPlanNode],
    idx: &mut usize,
    arena: &mut ExprArena,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    query_global_dict_map: &QueryGlobalDictMap,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    query_opts: Option<&internal_service::TQueryOptions>,
    db_name: Option<&str>,
    connectors: &ConnectorRegistry,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    global_common_slot_map: &BTreeMap<types::TSlotId, exprs::TExpr>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let root_index = *idx;
    let root_node = nodes
        .get(root_index)
        .ok_or_else(|| "invalid plan node index".to_string())?;
    *idx += 1;

    let mut stack = vec![LowerNodeFrame::new(
        root_index,
        expected_children(root_node)?,
    )];

    while let Some(frame) = stack.last_mut() {
        if frame.children.len() < frame.expected_children {
            let child_index = *idx;
            let child_node = nodes
                .get(child_index)
                .ok_or_else(|| "invalid plan node index".to_string())?;
            *idx += 1;
            stack.push(LowerNodeFrame::new(
                child_index,
                expected_children(child_node)?,
            ));
            continue;
        }

        let frame = stack.pop().expect("stack frame");
        let node = &nodes[frame.node_index];
        let lowered = lower_node_with_children(
            node,
            frame.children,
            arena,
            tuple_slots,
            desc_tbl,
            query_global_dict_map,
            exec_params,
            query_opts,
            db_name,
            connectors,
            layout_hints,
            global_common_slot_map,
            last_query_id,
            fe_addr,
        )?;
        if let Some(parent) = stack.last_mut() {
            parent.children.push(lowered);
        } else {
            return Ok(lowered);
        }
    }

    unreachable!("lower_node traversal must return root node")
}

fn lower_node_with_children(
    node: &plan_nodes::TPlanNode,
    children: Vec<Lowered>,
    arena: &mut ExprArena,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    query_global_dict_map: &QueryGlobalDictMap,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    query_opts: Option<&internal_service::TQueryOptions>,
    db_name: Option<&str>,
    connectors: &ConnectorRegistry,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    global_common_slot_map: &BTreeMap<types::TSlotId, exprs::TExpr>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let mut out_layout = layout_for_row_tuples(&node.row_tuples, tuple_slots);
    // Some plan nodes carry multiple tuples in `row_tuples` (e.g. aggregate intermediate vs output).
    // For execution output layouts we should align with the node's declared output tuple id when available.
    if node.node_type == plan_nodes::TPlanNodeType::AGGREGATION_NODE {
        if let Some(agg) = node.agg_node.as_ref() {
            let output_tuple_id = agg.output_tuple_id;
            out_layout = layout_for_row_tuples(&[output_tuple_id], tuple_slots);
        }
    }
    let mut lowered = match node.node_type {
        t if t == plan_nodes::TPlanNodeType::EXCHANGE_NODE => lower_exchange_node(
            children,
            node,
            exec_params,
            arena,
            &out_layout,
            last_query_id,
            fe_addr,
        )?,
        t if t == plan_nodes::TPlanNodeType::SELECT_NODE => lower_select_node(children)?,
        t if t == plan_nodes::TPlanNodeType::REPEAT_NODE => {
            if children.len() != 1 {
                return Err(format!(
                    "REPEAT_NODE expected 1 child, got {}",
                    children.len()
                ));
            }
            let child = children.into_iter().next().expect("child");
            lower_repeat_node(child, node, out_layout, tuple_slots)?
        }
        t if t == plan_nodes::TPlanNodeType::PROJECT_NODE => {
            if children.len() != 1 {
                return Err(format!(
                    "PROJECT_NODE expected 1 child, got {}",
                    children.len()
                ));
            }
            let child = children.into_iter().next().expect("child");
            lower_project_node(
                child,
                node,
                out_layout,
                arena,
                global_common_slot_map,
                last_query_id,
                fe_addr,
            )?
        }
        t if t == plan_nodes::TPlanNodeType::DECODE_NODE => {
            if children.len() != 1 {
                return Err(format!(
                    "DECODE_NODE expected 1 child, got {}",
                    children.len()
                ));
            }
            let child = children.into_iter().next().expect("child");
            lower_decode_node(
                child,
                node,
                out_layout,
                arena,
                desc_tbl,
                query_global_dict_map,
            )?
        }
        t if t == plan_nodes::TPlanNodeType::UNION_NODE => {
            lower_union_node(children, node, out_layout, arena, last_query_id, fe_addr)?
        }
        t if t == plan_nodes::TPlanNodeType::INTERSECT_NODE => {
            lower_intersect_node(children, node, out_layout, arena, last_query_id, fe_addr)?
        }
        t if t == plan_nodes::TPlanNodeType::EXCEPT_NODE => {
            lower_except_node(children, node, out_layout, arena, last_query_id, fe_addr)?
        }
        t if t == plan_nodes::TPlanNodeType::EMPTY_SET_NODE => {
            lower_empty_set_node(node, &out_layout, desc_tbl)?
        }
        t if t == plan_nodes::TPlanNodeType::RAW_VALUES_NODE => {
            lower_raw_values_node(node, &mut out_layout)?
        }
        t if t == plan_nodes::TPlanNodeType::LOOKUP_NODE => {
            lower_lookup_node(children, node, out_layout)?
        }
        t if t == plan_nodes::TPlanNodeType::SCHEMA_SCAN_NODE => {
            if !children.is_empty() {
                return Err(format!(
                    "SCHEMA_SCAN_NODE expected 0 children, got {}",
                    children.len()
                ));
            }
            lower_schema_scan_node(node, &out_layout, desc_tbl)?
        }
        t if t == plan_nodes::TPlanNodeType::FETCH_NODE => {
            lower_fetch_node(children, node, out_layout)?
        }
        t if t == plan_nodes::TPlanNodeType::MYSQL_SCAN_NODE => {
            lower_mysql_scan_node(node, desc_tbl, tuple_slots, query_opts, connectors)?
        }
        t if t == plan_nodes::TPlanNodeType::FILE_SCAN_NODE => lower_file_scan_node(
            node,
            desc_tbl,
            tuple_slots,
            layout_hints,
            exec_params,
            arena,
            out_layout,
        )?,
        t if t == plan_nodes::TPlanNodeType::JDBC_SCAN_NODE => lower_jdbc_scan_node(
            node,
            desc_tbl,
            tuple_slots,
            layout_hints,
            query_opts,
            connectors,
            db_name,
        )?,
        t if t == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE => lower_hdfs_scan_node(
            node,
            desc_tbl,
            tuple_slots,
            layout_hints,
            exec_params,
            query_opts,
            connectors,
            out_layout,
        )?,
        t if t == plan_nodes::TPlanNodeType::STARROCKS_SCAN_NODE => lower_starrocks_scan_node(
            node,
            desc_tbl,
            tuple_slots,
            layout_hints,
            exec_params,
            query_opts,
            connectors,
        )?,
        t if t == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE => lower_lake_scan_node(
            node,
            desc_tbl,
            tuple_slots,
            layout_hints,
            exec_params,
            query_opts,
            arena,
            connectors,
            query_global_dict_map,
            db_name,
            fe_addr,
        )?,
        t if t == plan_nodes::TPlanNodeType::LAKE_META_SCAN_NODE => lower_lake_meta_scan_node(
            node,
            desc_tbl,
            tuple_slots,
            layout_hints,
            exec_params,
            db_name,
            fe_addr,
        )?,
        t if t == plan_nodes::TPlanNodeType::OLAP_SCAN_NODE => {
            return Err(
                "OLAP_SCAN_NODE is not supported in novarocks yet. Phase 1 only supports shared-data LAKE_SCAN_NODE queries"
                    .to_string(),
            );
        }
        t if t == plan_nodes::TPlanNodeType::AGGREGATION_NODE => {
            if children.len() != 1 {
                return Err(format!(
                    "AGGREGATION_NODE expected 1 child, got {}",
                    children.len()
                ));
            }
            let child = children.into_iter().next().expect("child");
            lower_aggregate_node(
                child,
                node,
                arena,
                query_opts,
                &out_layout,
                last_query_id,
                fe_addr,
            )?
        }
        t if t == plan_nodes::TPlanNodeType::HASH_JOIN_NODE => {
            lower_hash_join_node(children, node, arena, desc_tbl, last_query_id, fe_addr)?
        }
        t if t == plan_nodes::TPlanNodeType::CROSS_JOIN_NODE => {
            lower_cross_join_node(children, node, desc_tbl)?
        }
        t if t == plan_nodes::TPlanNodeType::NESTLOOP_JOIN_NODE => {
            lower_nestloop_join_node(children, node, arena, desc_tbl, last_query_id, fe_addr)?
        }
        t if t == plan_nodes::TPlanNodeType::ASSERT_NUM_ROWS_NODE => {
            lower_assert_num_rows_node(children, node, &mut out_layout)?
        }
        t if t == plan_nodes::TPlanNodeType::SORT_NODE => {
            lower_sort_node(children, node, arena, out_layout, last_query_id, fe_addr)?
        }
        t if t == plan_nodes::TPlanNodeType::ANALYTIC_EVAL_NODE => {
            if children.len() != 1 {
                return Err(format!(
                    "ANALYTIC_EVAL_NODE expected 1 child, got {}",
                    children.len()
                ));
            }
            let child = children.into_iter().next().expect("child");
            lower_analytic_node(
                child,
                node,
                arena,
                &out_layout,
                tuple_slots,
                last_query_id,
                fe_addr,
            )?
        }
        t if t == plan_nodes::TPlanNodeType::TABLE_FUNCTION_NODE => {
            if children.len() != 1 {
                return Err(format!(
                    "TABLE_FUNCTION_NODE expected 1 child, got {}",
                    children.len()
                ));
            }
            let child = children.into_iter().next().expect("child");
            lower_table_function_node(child, node, out_layout, desc_tbl)?
        }
        t => {
            return Err(format!("unsupported plan node type: {:?}", t));
        }
    };

    if node.node_type == plan_nodes::TPlanNodeType::EXCHANGE_NODE
        || is_scan_node_type(node.node_type)
    {
        if let Some(specs) = node
            .probe_runtime_filters
            .as_ref()
            .filter(|v| !v.is_empty())
            .map(|_| {
                lower_probe_runtime_filter_specs(
                    node,
                    arena,
                    &lowered.layout,
                    last_query_id,
                    fe_addr,
                )
            })
        {
            if !specs.is_empty() {
                if node.node_type == plan_nodes::TPlanNodeType::EXCHANGE_NODE {
                    attach_probe_runtime_filter_specs_to_exchange(&mut lowered.node, &specs);
                } else {
                    attach_probe_runtime_filter_specs_to_scan(&mut lowered.node, &specs);
                }
            }
        }
    }

    // Apply conjuncts (predicates/filters) if present
    if let Some(conjuncts) = node.conjuncts.as_ref() {
        if !conjuncts.is_empty() {
            let common_slot_map = node
                .select_node
                .as_ref()
                .and_then(|n| n.common_slot_map.as_ref())
                .or_else(|| {
                    node.hash_join_node
                        .as_ref()
                        .and_then(|n| n.common_slot_map.as_ref())
                })
                .or_else(|| {
                    node.nestloop_join_node
                        .as_ref()
                        .and_then(|n| n.common_slot_map.as_ref())
                })
                .or_else(|| {
                    node.project_node
                        .as_ref()
                        .and_then(|n| n.common_slot_map.as_ref())
                })
                .or_else(|| node.common.as_ref().and_then(|n| n.heavy_exprs.as_ref()));

            // Combine multiple conjuncts with AND logic
            let mut conjunct_ids = Vec::new();
            for conj in conjuncts {
                let conj_id = lower_t_expr_with_common_slot_map(
                    conj,
                    arena,
                    &lowered.layout,
                    last_query_id,
                    fe_addr,
                    common_slot_map,
                )?;
                conjunct_ids.push(conj_id);
            }

            // If multiple conjuncts, combine with AND
            let predicate = if conjunct_ids.len() == 1 {
                conjunct_ids[0]
            } else {
                // Build AND expression: conjunct1 AND conjunct2 AND ...
                let mut result = conjunct_ids[0];
                for conj_id in &conjunct_ids[1..] {
                    result = arena.push(ExprNode::And(result, *conj_id));
                }
                result
            };

            let mut pushed_to_scan = false;
            if let ExecNodeKind::Scan(scan) = &mut lowered.node.kind {
                let combined = if let Some(existing) = scan.conjunct_predicate() {
                    arena.push(ExprNode::And(existing, predicate))
                } else {
                    predicate
                };
                scan.set_conjunct_predicate(Some(combined));
                pushed_to_scan = true;
            }
            if !pushed_to_scan {
                lowered = Lowered {
                    node: ExecNode {
                        kind: ExecNodeKind::Filter(FilterNode {
                            input: Box::new(lowered.node),
                            node_id: node.node_id,
                            predicate,
                        }),
                    },
                    layout: lowered.layout,
                };
            }
        }
    }

    if node.limit >= 0 {
        // Sort/Exchange lowering may have already embedded LIMIT/OFFSET semantics.
        let is_sort = matches!(lowered.node.kind, ExecNodeKind::Sort(_));
        let is_limit = matches!(lowered.node.kind, ExecNodeKind::Limit(_));
        if !is_sort && !is_limit {
            lowered = Lowered {
                node: ExecNode {
                    kind: ExecNodeKind::Limit(LimitNode {
                        input: Box::new(lowered.node),
                        node_id: node.node_id,
                        limit: Some(node.limit as usize),
                        offset: 0,
                    }),
                },
                layout: lowered.layout,
            };
        }
    }
    Ok(lowered)
}

fn is_scan_node_type(t: plan_nodes::TPlanNodeType) -> bool {
    matches!(
        t,
        plan_nodes::TPlanNodeType::MYSQL_SCAN_NODE
            | plan_nodes::TPlanNodeType::FILE_SCAN_NODE
            | plan_nodes::TPlanNodeType::JDBC_SCAN_NODE
            | plan_nodes::TPlanNodeType::HDFS_SCAN_NODE
            | plan_nodes::TPlanNodeType::STARROCKS_SCAN_NODE
            | plan_nodes::TPlanNodeType::LAKE_SCAN_NODE
            | plan_nodes::TPlanNodeType::SCHEMA_SCAN_NODE
    )
}

fn common_slot_map_for_node(
    node: &plan_nodes::TPlanNode,
) -> Option<&BTreeMap<types::TSlotId, crate::exprs::TExpr>> {
    node.select_node
        .as_ref()
        .and_then(|n| n.common_slot_map.as_ref())
        .or_else(|| {
            node.hash_join_node
                .as_ref()
                .and_then(|n| n.common_slot_map.as_ref())
        })
        .or_else(|| {
            node.nestloop_join_node
                .as_ref()
                .and_then(|n| n.common_slot_map.as_ref())
        })
        .or_else(|| {
            node.project_node
                .as_ref()
                .and_then(|n| n.common_slot_map.as_ref())
        })
        .or_else(|| node.common.as_ref().and_then(|n| n.heavy_exprs.as_ref()))
}

pub(crate) fn local_rf_waiting_set(node: &plan_nodes::TPlanNode) -> Vec<i32> {
    let Some(set) = node.local_rf_waiting_set.as_ref() else {
        return Vec::new();
    };
    let mut ids: Vec<i32> = set.iter().map(|id| *id as i32).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn lower_probe_runtime_filter_specs(
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Vec<RuntimeFilterProbeSpec> {
    let Some(descs) = node
        .probe_runtime_filters
        .as_ref()
        .filter(|v| !v.is_empty())
    else {
        return Vec::new();
    };
    let common_slot_map = common_slot_map_for_node(node);
    let mut specs = Vec::new();
    let mut seen = HashSet::new();
    for desc in descs {
        let Some(filter_id) = desc.filter_id else {
            warn!(
                "probe runtime filter missing filter_id: node_id={}",
                node.node_id
            );
            continue;
        };
        if let Some(filter_type) = desc.filter_type {
            if filter_type != runtime_filter::TRuntimeFilterBuildType::JOIN_FILTER {
                continue;
            }
        }
        let Some(targets) = desc.plan_node_id_to_target_expr.as_ref() else {
            continue;
        };
        let Some(expr) = targets.get(&node.node_id) else {
            continue;
        };
        let expr_id = match lower_t_expr_with_common_slot_map(
            expr,
            arena,
            layout,
            last_query_id,
            fe_addr,
            common_slot_map,
        ) {
            Ok(id) => id,
            Err(err) => {
                warn!(
                    "probe runtime filter expr lowering failed: node_id={} filter_id={} err={}",
                    node.node_id, filter_id, err
                );
                continue;
            }
        };
        let slot_id = match arena.node(expr_id) {
            Some(ExprNode::SlotId(slot_id)) => *slot_id,
            _ => {
                let slot_id = find_first_slot_id(arena, expr_id).unwrap_or_else(|| SlotId::new(0));
                warn!(
                    "probe runtime filter expr is not a slot ref; will eval expression: node_id={} filter_id={}",
                    node.node_id, filter_id
                );
                slot_id
            }
        };
        if seen.insert(filter_id) {
            specs.push(RuntimeFilterProbeSpec {
                filter_id,
                expr_id,
                slot_id,
            });
        }
    }
    specs
}

fn find_first_slot_id(arena: &ExprArena, expr_id: ExprId) -> Option<SlotId> {
    let mut stack = vec![expr_id];
    while let Some(id) = stack.pop() {
        let Some(node) = arena.node(id) else {
            continue;
        };
        match node {
            ExprNode::SlotId(slot_id) => return Some(*slot_id),
            ExprNode::ArrayExpr { elements } => {
                for child in elements {
                    stack.push(*child);
                }
            }
            ExprNode::StructExpr { fields } => {
                for child in fields {
                    stack.push(*child);
                }
            }
            ExprNode::LambdaFunction { .. } => {
                // Do not descend into nested lambdas.
            }
            ExprNode::DictDecode { child, .. } => {
                stack.push(*child);
            }
            ExprNode::Cast(child)
            | ExprNode::CastTime(child)
            | ExprNode::CastTimeFromDatetime(child)
            | ExprNode::Not(child)
            | ExprNode::IsNull(child)
            | ExprNode::IsNotNull(child)
            | ExprNode::Clone(child) => {
                stack.push(*child);
            }
            ExprNode::Add(a, b)
            | ExprNode::Sub(a, b)
            | ExprNode::Mul(a, b)
            | ExprNode::Div(a, b)
            | ExprNode::Mod(a, b)
            | ExprNode::Eq(a, b)
            | ExprNode::EqForNull(a, b)
            | ExprNode::Ne(a, b)
            | ExprNode::Lt(a, b)
            | ExprNode::Le(a, b)
            | ExprNode::Gt(a, b)
            | ExprNode::Ge(a, b)
            | ExprNode::And(a, b)
            | ExprNode::Or(a, b) => {
                stack.push(*a);
                stack.push(*b);
            }
            ExprNode::In { child, values, .. } => {
                stack.push(*child);
                for value in values {
                    stack.push(*value);
                }
            }
            ExprNode::Case { children, .. } => {
                for child in children {
                    stack.push(*child);
                }
            }
            ExprNode::FunctionCall { args, .. } => {
                for arg in args {
                    stack.push(*arg);
                }
            }
            ExprNode::Literal(_) => {}
        }
    }
    None
}

fn attach_probe_runtime_filter_specs_to_exchange(
    node: &mut ExecNode,
    specs: &[RuntimeFilterProbeSpec],
) {
    if specs.is_empty() {
        return;
    }
    match &mut node.kind {
        ExecNodeKind::ExchangeSource(exchange) => exchange.add_runtime_filter_specs(specs),
        ExecNodeKind::Sort(sort) => {
            attach_probe_runtime_filter_specs_to_exchange(&mut sort.input, specs)
        }
        _ => {}
    }
}

fn attach_probe_runtime_filter_specs_to_scan(
    node: &mut ExecNode,
    specs: &[RuntimeFilterProbeSpec],
) {
    if specs.is_empty() {
        return;
    }
    if let ExecNodeKind::Scan(scan) = &mut node.kind {
        scan.add_runtime_filter_specs(specs);
    }
}
