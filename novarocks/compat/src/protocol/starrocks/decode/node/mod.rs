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
mod change_event_expand;
mod cross_join;
mod decode;
mod empty_set;
mod exchange;
mod fetch;
mod file_scan;
mod hash_join;
pub(crate) mod hdfs_scan;
mod iceberg_delta_scan;
mod lake_meta_scan;
mod lake_scan;
mod lookup;
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

use novarocks::common::ids::SlotId;
use novarocks::exec::expr::{ExprArena, ExprNode};
use novarocks::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind};
use novarocks::exec::node::filter::FilterNode;
use novarocks::exec::node::limit::LimitNode;
use novarocks::exec::node::scan::BoundScanRanges;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::runtime::scan_range::ScanRangeParams;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::protocol::starrocks::decode::StarRocksFragmentDecodeError;
use crate::protocol::starrocks::decode::expr::lower_t_expr_with_common_slot_map_at;
use crate::protocol::starrocks::decode::layout::{Layout, layout_for_row_tuples};
use crate::thrift::{data, descriptors, exprs, plan_nodes, types};
use novarocks::protocol::FieldPath;

/// Transient scan-range access handed to compat scan decoders.
///
/// It bundles the two roles that the instance's old `ScanAssignments` used to
/// serve during decode: the pre-enrichment INPUT (`(ScanAssignmentKind,
/// Vec<ScanRangeParams>)` per node, for the decoders' kind guards + range
/// enrichment) and a capture slot for the enriched `BoundScanRanges` OUTPUT
/// (routed into the instance's scan assignments; `bind` is deferred to
/// materialize time). Range-less scans (jdbc/mysql) only use `capture`.
#[derive(Clone, Copy)]
pub(crate) struct ScanRangeCarrier<'a> {
    raw: &'a BTreeMap<FragmentNodeId, (ScanAssignmentKind, Vec<ScanRangeParams>)>,
    captured: &'a RefCell<BTreeMap<FragmentNodeId, BoundScanRanges>>,
}

impl<'a> ScanRangeCarrier<'a> {
    pub(crate) fn new(
        raw: &'a BTreeMap<FragmentNodeId, (ScanAssignmentKind, Vec<ScanRangeParams>)>,
        captured: &'a RefCell<BTreeMap<FragmentNodeId, BoundScanRanges>>,
    ) -> Self {
        Self { raw, captured }
    }

    /// Pre-enrichment input for a scan node: its assignment kind (for the
    /// decoder's kind guard) plus the FE-decoded `ScanRangeParams`.
    pub(crate) fn get(&self, node_id: i32) -> Option<(ScanAssignmentKind, &'a [ScanRangeParams])> {
        self.raw
            .get(&FragmentNodeId::new(node_id))
            .map(|(kind, ranges)| (*kind, ranges.as_slice()))
    }

    /// Record a scan node's enriched connector ranges; drained after decode
    /// into the instance's scan assignments.
    pub(crate) fn capture(&self, node_id: i32, ranges: BoundScanRanges) {
        self.captured
            .borrow_mut()
            .insert(FragmentNodeId::new(node_id), ranges);
    }
}

pub(crate) struct StarRocksPlanDecodeContext<'a> {
    query_id: Option<novarocks_types::QueryId>,
    fragment_instance_id: Option<novarocks_types::UniqueId>,
    scan_ranges: Option<ScanRangeCarrier<'a>>,
    broker_file_program_facts: Option<&'a BTreeMap<i32, BrokerFileProgramFacts>>,
    lake_scan_program_facts:
        Option<&'a BTreeMap<i32, crate::protocol::starrocks::decode::LakeScanProgramFacts>>,
    lake_meta_scan_range_facts:
        Option<&'a BTreeMap<i32, Vec<crate::protocol::starrocks::decode::LakeMetaScanRangeFact>>>,
    per_exchange_sender_counts: Option<&'a BTreeMap<i32, i32>>,
    batch_sender_counts: &'a HashMap<i32, usize>,
    query_options: novarocks::runtime::query_options::QueryOptions,
    decode_facts: &'a crate::protocol::starrocks::decode::instance::StarRocksDecodeFacts,
    compat_iceberg_execution: Option<&'a Arc<novarocks_spi::connector::ConnectorExecutionBinding>>,
}

impl<'a> StarRocksPlanDecodeContext<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        query_id: Option<novarocks_types::QueryId>,
        fragment_instance_id: Option<novarocks_types::UniqueId>,
        scan_ranges: Option<ScanRangeCarrier<'a>>,
        broker_file_program_facts: Option<&'a BTreeMap<i32, BrokerFileProgramFacts>>,
        lake_scan_program_facts: Option<
            &'a BTreeMap<i32, crate::protocol::starrocks::decode::LakeScanProgramFacts>,
        >,
        lake_meta_scan_range_facts: Option<
            &'a BTreeMap<i32, Vec<crate::protocol::starrocks::decode::LakeMetaScanRangeFact>>,
        >,
        per_exchange_sender_counts: Option<&'a BTreeMap<i32, i32>>,
        batch_sender_counts: &'a HashMap<i32, usize>,
        query_options: novarocks::runtime::query_options::QueryOptions,
        decode_facts: &'a crate::protocol::starrocks::decode::instance::StarRocksDecodeFacts,
        compat_iceberg_execution: Option<
            &'a Arc<novarocks_spi::connector::ConnectorExecutionBinding>,
        >,
    ) -> Self {
        Self {
            query_id,
            fragment_instance_id,
            scan_ranges,
            broker_file_program_facts,
            lake_scan_program_facts,
            lake_meta_scan_range_facts,
            per_exchange_sender_counts,
            batch_sender_counts,
            query_options,
            decode_facts,
            compat_iceberg_execution,
        }
    }
}

pub(crate) use aggregate::lower_aggregate_node;
pub(crate) use analytic::lower_analytic_node;
pub(crate) use assert::lower_assert_num_rows_node;
pub(crate) use change_event_expand::lower_change_event_expand_node;
pub(crate) use cross_join::lower_cross_join_node;
pub(crate) use decode::{QueryGlobalDictMap, build_query_global_dict_map, lower_decode_node};
pub(crate) use empty_set::lower_empty_set_node;
pub(crate) use exchange::lower_exchange_node;
pub(crate) use exchange::resolve_exchange_sender_count;
pub(crate) use fetch::lower_fetch_node;
pub(crate) use file_scan::{
    BrokerFileProgramFacts, decode_broker_file_program_facts, lower_file_scan_node,
};
pub(crate) use hash_join::lower_hash_join_node;
pub(crate) use hdfs_scan::lower_hdfs_scan_node;
pub(crate) use iceberg_delta_scan::lower_iceberg_delta_scan_node;
pub(crate) use lake_meta_scan::{LakeMetaValuesPatch, lower_lake_meta_scan_node};
pub(crate) use lake_scan::{lower_lake_scan_node, reject_lake_late_materialization};
pub(crate) use lookup::{lower_lookup_node, lower_row_pos_descs};
pub(crate) use nestloop_join::lower_nestloop_join_node;
pub(crate) use project::lower_project_node;
pub(crate) use raw_values::lower_raw_values_node;
pub(crate) use repeat::lower_repeat_node;
pub(crate) use schema_scan::lower_schema_scan_node;
pub(crate) use schema_scan::supported_schema_scan_requires_ranges;
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
    context: &StarRocksPlanDecodeContext<'_>,
    db_name: Option<&str>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    query_global_dicts_path: FieldPath,
    query_global_dict_exprs_path: FieldPath,
    plan_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let mut idx = 0usize;
    let global_common_slot_map = collect_global_common_slot_map(&plan.nodes);
    let query_global_dict_map = build_query_global_dict_map(
        query_global_dicts,
        query_global_dict_exprs,
        query_global_dicts_path.clone(),
        query_global_dict_exprs_path,
    )?;
    let mut arena_query_global_dicts = HashMap::new();
    for (slot_id, dict) in &query_global_dict_map {
        let slot_id = SlotId::try_from(*slot_id).map_err(|detail| {
            StarRocksFragmentDecodeError::invalid_value(query_global_dicts_path.clone(), detail)
        })?;
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
        context,
        db_name,
        layout_hints,
        &global_common_slot_map,
        last_query_id,
        fe_addr,
        &plan_path,
    )?;
    if idx != plan.nodes.len() {
        return Err(StarRocksFragmentDecodeError::invalid_value(
            plan_path.field("nodes").index(idx),
            "trailing plan node is not reachable from the root",
        ));
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

enum NodeLeafDecodeError {
    Legacy(String),
    Typed(StarRocksFragmentDecodeError),
}

impl NodeLeafDecodeError {
    fn into_fragment(self, node_path: FieldPath) -> StarRocksFragmentDecodeError {
        match self {
            Self::Legacy(detail) => StarRocksFragmentDecodeError::invalid_value(node_path, detail),
            Self::Typed(error) => error,
        }
    }
}

impl From<String> for NodeLeafDecodeError {
    fn from(detail: String) -> Self {
        Self::Legacy(detail)
    }
}

impl From<StarRocksFragmentDecodeError> for NodeLeafDecodeError {
    fn from(error: StarRocksFragmentDecodeError) -> Self {
        Self::Typed(error)
    }
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

fn expected_children(
    node: &plan_nodes::TPlanNode,
    node_path: &FieldPath,
) -> Result<usize, StarRocksFragmentDecodeError> {
    if node.num_children < 0 {
        return Err(StarRocksFragmentDecodeError::invalid_value(
            node_path.clone().field("num_children"),
            format!(
                "node_id={} has negative num_children={}",
                node.node_id, node.num_children
            ),
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
    context: &StarRocksPlanDecodeContext<'_>,
    db_name: Option<&str>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    global_common_slot_map: &BTreeMap<types::TSlotId, exprs::TExpr>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    plan_path: &FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let root_index = *idx;
    let root_node = nodes.get(root_index).ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            plan_path.clone().field("nodes").index(root_index),
            "missing root plan node",
        )
    })?;
    *idx += 1;

    let mut stack = vec![LowerNodeFrame::new(
        root_index,
        expected_children(
            root_node,
            &plan_path.clone().field("nodes").index(root_index),
        )?,
    )];

    while let Some(frame) = stack.last_mut() {
        if frame.children.len() < frame.expected_children {
            let child_index = *idx;
            let child_node = nodes.get(child_index).ok_or_else(|| {
                StarRocksFragmentDecodeError::invalid_value(
                    plan_path
                        .clone()
                        .field("nodes")
                        .index(frame.node_index)
                        .field("num_children"),
                    format!(
                        "declared child {} is missing at flat node index {}",
                        frame.children.len(),
                        child_index
                    ),
                )
            })?;
            *idx += 1;
            stack.push(LowerNodeFrame::new(
                child_index,
                expected_children(
                    child_node,
                    &plan_path.clone().field("nodes").index(child_index),
                )?,
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
            context,
            db_name,
            layout_hints,
            global_common_slot_map,
            last_query_id,
            fe_addr,
            plan_path.clone().field("nodes").index(frame.node_index),
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
    context: &StarRocksPlanDecodeContext<'_>,
    db_name: Option<&str>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    global_common_slot_map: &BTreeMap<types::TSlotId, exprs::TExpr>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    node_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    lower_node_with_children_typed(
        node,
        children,
        arena,
        tuple_slots,
        desc_tbl,
        query_global_dict_map,
        context,
        db_name,
        layout_hints,
        global_common_slot_map,
        last_query_id,
        fe_addr,
        node_path,
    )
}

#[allow(clippy::too_many_arguments)]
fn lower_node_with_children_typed(
    node: &plan_nodes::TPlanNode,
    children: Vec<Lowered>,
    arena: &mut ExprArena,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    query_global_dict_map: &QueryGlobalDictMap,
    context: &StarRocksPlanDecodeContext<'_>,
    db_name: Option<&str>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    global_common_slot_map: &BTreeMap<types::TSlotId, exprs::TExpr>,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    node_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let mut out_layout = layout_for_row_tuples(&node.row_tuples, tuple_slots);
    // Some plan nodes carry multiple tuples in `row_tuples` (e.g. aggregate intermediate vs output).
    // For execution output layouts we should align with the node's declared output tuple id when available.
    if node.node_type == plan_nodes::TPlanNodeType::AGGREGATION_NODE
        && let Some(agg) = node.agg_node.as_ref()
    {
        let tuple_id = if agg.need_finalize {
            agg.output_tuple_id
        } else {
            agg.intermediate_tuple_id
        };
        out_layout = layout_for_row_tuples(&[tuple_id], tuple_slots);
    }
    if node.node_type == plan_nodes::TPlanNodeType::EXCHANGE_NODE
        && let Some(exchange) = node.exchange_node.as_ref()
    {
        out_layout = layout_for_row_tuples(&exchange.input_row_tuples, tuple_slots);
    }
    let mut lowered = (|| -> Result<Lowered, NodeLeafDecodeError> {
        Ok(match node.node_type {
            t if t == plan_nodes::TPlanNodeType::EXCHANGE_NODE => lower_exchange_node(
                children,
                node,
                desc_tbl.ok_or_else(|| {
                    format!(
                        "EXCHANGE_NODE missing descriptor table for node_id={}",
                        node.node_id
                    )
                })?,
                context.fragment_instance_id,
                context
                    .per_exchange_sender_counts
                    .and_then(|counts| counts.get(&node.node_id).copied()),
                context.batch_sender_counts,
                arena,
                &out_layout,
                last_query_id,
                fe_addr,
                node_path.clone(),
            )?,
            t if t == plan_nodes::TPlanNodeType::SELECT_NODE => lower_select_node(children)?,
            t if t == plan_nodes::TPlanNodeType::REPEAT_NODE => {
                if children.len() != 1 {
                    return Err(
                        format!("REPEAT_NODE expected 1 child, got {}", children.len()).into(),
                    );
                }
                let child = children.into_iter().next().expect("child");
                lower_repeat_node(child, node, out_layout, tuple_slots)?
            }
            t if t == plan_nodes::TPlanNodeType::CHANGE_EVENT_EXPAND_NODE => {
                let desc_tbl = desc_tbl.ok_or_else(|| {
                    format!(
                        "CHANGE_EVENT_EXPAND_NODE node_id={} requires descriptor table",
                        node.node_id
                    )
                })?;
                lower_change_event_expand_node(
                    children,
                    node,
                    out_layout,
                    arena,
                    desc_tbl,
                    last_query_id,
                    fe_addr,
                    node_path.clone(),
                )?
            }
            t if t == plan_nodes::TPlanNodeType::PROJECT_NODE => {
                if children.len() != 1 {
                    return Err(
                        format!("PROJECT_NODE expected 1 child, got {}", children.len()).into(),
                    );
                }
                let child = children.into_iter().next().expect("child");
                let desc_tbl =
                    desc_tbl.ok_or_else(|| "PROJECT_NODE requires descriptor table".to_string())?;
                lower_project_node(
                    child,
                    node,
                    out_layout,
                    arena,
                    desc_tbl,
                    global_common_slot_map,
                    last_query_id,
                    fe_addr,
                    node_path.clone(),
                )?
            }
            t if t == plan_nodes::TPlanNodeType::DECODE_NODE => {
                if children.len() != 1 {
                    return Err(
                        format!("DECODE_NODE expected 1 child, got {}", children.len()).into(),
                    );
                }
                let child = children.into_iter().next().expect("child");
                lower_decode_node(
                    child,
                    node,
                    out_layout,
                    arena,
                    desc_tbl,
                    query_global_dict_map,
                    node_path.clone(),
                )?
            }
            t if t == plan_nodes::TPlanNodeType::UNION_NODE => lower_union_node(
                children,
                node,
                out_layout,
                arena,
                desc_tbl,
                last_query_id,
                fe_addr,
                node_path.clone(),
            )?,
            t if t == plan_nodes::TPlanNodeType::INTERSECT_NODE => lower_intersect_node(
                children,
                node,
                out_layout,
                arena,
                desc_tbl,
                last_query_id,
                fe_addr,
                node_path.clone(),
            )?,
            t if t == plan_nodes::TPlanNodeType::EXCEPT_NODE => lower_except_node(
                children,
                node,
                out_layout,
                arena,
                desc_tbl,
                last_query_id,
                fe_addr,
                node_path.clone(),
            )?,
            t if t == plan_nodes::TPlanNodeType::EMPTY_SET_NODE => {
                lower_empty_set_node(node, &out_layout, desc_tbl)?
            }
            t if t == plan_nodes::TPlanNodeType::RAW_VALUES_NODE => {
                lower_raw_values_node(node, &mut out_layout)?
            }
            t if t == plan_nodes::TPlanNodeType::LOOKUP_NODE => {
                lower_lookup_node(children, node, out_layout, desc_tbl)?
            }
            t if t == plan_nodes::TPlanNodeType::SCHEMA_SCAN_NODE => {
                if !children.is_empty() {
                    return Err(format!(
                        "SCHEMA_SCAN_NODE expected 0 children, got {}",
                        children.len()
                    )
                    .into());
                }
                lower_schema_scan_node(node, &out_layout, desc_tbl, context.scan_ranges, fe_addr)?
            }
            t if t == plan_nodes::TPlanNodeType::FETCH_NODE => {
                lower_fetch_node(children, node, out_layout, desc_tbl)?
            }
            t if t == plan_nodes::TPlanNodeType::MYSQL_SCAN_NODE => {
                return Err("MYSQL_SCAN_NODE is unsupported; compat supports only internal tables and explicit Iceberg descriptors".to_string().into());
            }
            t if t == plan_nodes::TPlanNodeType::FILE_SCAN_NODE => lower_file_scan_node(
                node,
                desc_tbl,
                tuple_slots,
                layout_hints,
                context.scan_ranges,
                context
                    .broker_file_program_facts
                    .and_then(|facts| facts.get(&node.node_id)),
                arena,
                out_layout,
                node_path.clone(),
            )?,
            t if t == plan_nodes::TPlanNodeType::JDBC_SCAN_NODE => {
                return Err("JDBC_SCAN_NODE is unsupported; compat supports only internal tables and explicit Iceberg descriptors".to_string().into());
            }
            t if t == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE => lower_hdfs_scan_node(
                node,
                desc_tbl,
                tuple_slots,
                layout_hints,
                context.scan_ranges,
                &context.query_options,
                context.compat_iceberg_execution.cloned(),
                query_global_dict_map,
                out_layout,
                context.decode_facts,
                context.query_id,
            )?,
            t if t == plan_nodes::TPlanNodeType::ICEBERG_DELTA_SCAN_NODE => {
                if !children.is_empty() {
                    return Err(format!(
                        "ICEBERG_DELTA_SCAN_NODE expected 0 children, got {}",
                        children.len()
                    )
                    .into());
                }
                lower_iceberg_delta_scan_node(
                    node,
                    desc_tbl,
                    out_layout,
                    context.compat_iceberg_execution.cloned(),
                    context.query_id,
                    context.scan_ranges,
                    &context.query_options,
                )?
            }
            t if t == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE => {
                reject_lake_late_materialization(
                    node,
                    desc_tbl,
                    tuple_slots,
                    layout_hints,
                    node_path.clone(),
                )?;
                lower_lake_scan_node(
                    node,
                    desc_tbl,
                    tuple_slots,
                    layout_hints,
                    context.scan_ranges,
                    context
                        .lake_scan_program_facts
                        .and_then(|facts| facts.get(&node.node_id)),
                    context.query_id,
                    &context.query_options,
                    arena,
                    query_global_dict_map,
                    db_name,
                    fe_addr,
                )?
            }
            t if t == plan_nodes::TPlanNodeType::LAKE_META_SCAN_NODE => {
                lower_lake_meta_scan_node(
                    node,
                    desc_tbl,
                    tuple_slots,
                    layout_hints,
                    context
                        .lake_meta_scan_range_facts
                        .and_then(|facts| facts.get(&node.node_id))
                        .map(Vec::as_slice),
                    context.query_id,
                    db_name,
                    fe_addr,
                )?
            }
            t if t == plan_nodes::TPlanNodeType::OLAP_SCAN_NODE => {
                lower_starrocks_scan_node(
                    node,
                    desc_tbl,
                    tuple_slots,
                    layout_hints,
                    &context.query_options,
                    query_global_dict_map,
                )?
            }
            t if t == plan_nodes::TPlanNodeType::AGGREGATION_NODE => {
                if children.len() != 1 {
                    return Err(format!(
                        "AGGREGATION_NODE expected 1 child, got {}",
                        children.len()
                    )
                    .into());
                }
                let child = children.into_iter().next().expect("child");
                lower_aggregate_node(
                    child,
                    node,
                    arena,
                    desc_tbl,
                    &context.query_options,
                    &out_layout,
                    last_query_id,
                    fe_addr,
                    node_path.clone(),
                )?
            }
            t if t == plan_nodes::TPlanNodeType::HASH_JOIN_NODE => lower_hash_join_node(
                children,
                node,
                arena,
                desc_tbl,
                last_query_id,
                fe_addr,
                node_path.clone(),
            )?,
            t if t == plan_nodes::TPlanNodeType::CROSS_JOIN_NODE => {
                lower_cross_join_node(children, node, desc_tbl)?
            }
            t if t == plan_nodes::TPlanNodeType::NESTLOOP_JOIN_NODE => lower_nestloop_join_node(
                children,
                node,
                arena,
                desc_tbl,
                last_query_id,
                fe_addr,
                node_path.clone(),
            )?,
            t if t == plan_nodes::TPlanNodeType::ASSERT_NUM_ROWS_NODE => {
                lower_assert_num_rows_node(children, node, &mut out_layout)?
            }
            t if t == plan_nodes::TPlanNodeType::SORT_NODE => lower_sort_node(
                children,
                node,
                arena,
                out_layout,
                desc_tbl,
                last_query_id,
                fe_addr,
                node_path.clone(),
            )?,
            t if t == plan_nodes::TPlanNodeType::ANALYTIC_EVAL_NODE => {
                if children.len() != 1 {
                    return Err(format!(
                        "ANALYTIC_EVAL_NODE expected 1 child, got {}",
                        children.len()
                    )
                    .into());
                }
                let child = children.into_iter().next().expect("child");
                lower_analytic_node(
                    child,
                    node,
                    arena,
                    &out_layout,
                    desc_tbl.ok_or_else(|| {
                        format!(
                            "ANALYTIC_EVAL_NODE node_id={} requires descriptor table",
                            node.node_id
                        )
                    })?,
                    tuple_slots,
                    last_query_id,
                    fe_addr,
                    node_path.clone(),
                )?
            }
            t if t == plan_nodes::TPlanNodeType::TABLE_FUNCTION_NODE => {
                if children.len() != 1 {
                    return Err(format!(
                        "TABLE_FUNCTION_NODE expected 1 child, got {}",
                        children.len()
                    )
                    .into());
                }
                let child = children.into_iter().next().expect("child");
                lower_table_function_node(child, node, out_layout, desc_tbl)?
            }
            t => {
                return Err(format!("unsupported plan node type: {:?}", t).into());
            }
        })
    })()
    .map_err(|error| error.into_fragment(node_path.clone()))?;

    // Apply conjuncts (predicates/filters) if present
    if let Some(conjuncts) = node.conjuncts.as_ref()
        && !conjuncts.is_empty()
    {
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
        for (index, conj) in conjuncts.iter().enumerate() {
            let conj_id = lower_t_expr_with_common_slot_map_at(
                conj,
                arena,
                &lowered.layout,
                last_query_id,
                fe_addr,
                common_slot_map,
                node_path.clone().field("conjuncts").index(index),
                Some(node_path.clone()),
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
