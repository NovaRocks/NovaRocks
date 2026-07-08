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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::engine::StandaloneState;
use crate::engine::query_options::StandaloneQueryOptions;
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::sql::analysis::OutputColumn;
use crate::sql::common::ChangeStreamBranchKind;
use crate::sql::optimizer::OptimizerPhysicalNode;
use crate::sql::planner::{
    ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec, IcebergWriteSinkSpec,
};

pub(crate) const DML_CHANGE_STREAM_DATA_ROUTE_COLUMN: &str = "__change_data_route";

pub(crate) struct DmlChangeStreamWritePlan {
    pub(crate) producer: OptimizerPhysicalNode,
    pub(crate) dag: ChangeStreamWriteDagSpec,
    pub(crate) pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
}

#[derive(Clone, Debug)]
pub(crate) struct DmlPreExpandKeyedAssert {
    pub(crate) key_column_name: String,
    pub(crate) key_label: String,
    pub(crate) message_prefix: String,
}

#[derive(Debug)]
pub(crate) struct DmlChangeStreamWriteExecution {
    pub(crate) result: CoordinatedQueryResult,
    pub(crate) commit_plan:
        crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DmlChangeStreamBranchSet {
    UpdateMor,
    Merge {
        matched_update: bool,
        matched_delete: bool,
        not_matched_insert: bool,
    },
}

#[derive(Clone, Debug, Default)]
struct DmlChangeStreamWriteBranchSinkSpecs {
    delete_dv: Option<IcebergWriteSinkSpec>,
    reuse_data: Option<IcebergWriteSinkSpec>,
    fresh_data: Option<IcebergWriteSinkSpec>,
    target_partition_source_columns: Vec<String>,
}

impl DmlChangeStreamBranchSet {
    fn branch_kinds(self) -> Vec<ChangeStreamBranchKind> {
        match self {
            Self::UpdateMor => vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ],
            Self::Merge {
                matched_update,
                matched_delete,
                not_matched_insert,
            } => {
                let mut branches = Vec::with_capacity(3);
                if matched_update || matched_delete {
                    branches.push(ChangeStreamBranchKind::DeleteDv);
                }
                if matched_update {
                    branches.push(ChangeStreamBranchKind::ReuseData);
                }
                if not_matched_insert {
                    branches.push(ChangeStreamBranchKind::FreshData);
                }
                branches
            }
        }
    }
}

pub(crate) fn build_dml_change_stream_write_plan(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    producer: OptimizerPhysicalNode,
    branch_set: DmlChangeStreamBranchSet,
    target_ref: &str,
) -> Result<DmlChangeStreamWritePlan, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = build_iceberg_catalog(&entry)?;
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };

    let branch_kinds = branch_set.branch_kinds();
    if branch_kinds.is_empty() {
        return Err("DML change-stream write requires at least one branch".to_string());
    }
    let mut sink_specs = DmlChangeStreamWriteBranchSinkSpecs {
        target_partition_source_columns: target_partition_source_column_names(table.metadata())?,
        ..Default::default()
    };
    if branch_kinds.contains(&ChangeStreamBranchKind::DeleteDv) {
        sink_specs.delete_dv = Some(
            crate::engine::mutation_flow::build_mor_deletion_vector_sink_spec(
                target, &resolved, &table, &entry, target_ref,
            )?,
        );
    }
    if branch_kinds.contains(&ChangeStreamBranchKind::ReuseData) {
        sink_specs.reuse_data = Some(
            crate::engine::iceberg_writer::build_row_lineage_data_sink_spec(
                target, &resolved, &table, &entry,
            )?,
        );
    }
    if branch_kinds.contains(&ChangeStreamBranchKind::FreshData) {
        let write_columns = crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(
            table.metadata().current_schema(),
        )?;
        sink_specs.fresh_data = Some(crate::engine::iceberg_writer::build_insert_write_sink_spec(
            target,
            &resolved,
            &table,
            &entry,
            &write_columns,
        )?);
    }

    let dag = build_dml_change_stream_dag_from_sink_specs(
        branch_set,
        &producer.output_columns,
        sink_specs,
    )?;
    Ok(DmlChangeStreamWritePlan {
        producer,
        dag,
        pre_expand_keyed_assert: None,
    })
}

pub(crate) fn inject_dml_pre_expand_keyed_assert(
    build_result: &mut crate::sql::codegen::MultiFragmentBuildResult,
    keyed_assert: Option<&DmlPreExpandKeyedAssert>,
) -> Result<(), String> {
    let Some(keyed_assert) = keyed_assert else {
        return Ok(());
    };

    let expand_nodes = change_event_expand_positions(build_result);
    if expand_nodes.len() != 1 {
        return Err(format!(
            "DML change-stream keyed assert requires exactly one ChangeEventExpand node, found {}",
            expand_nodes.len()
        ));
    }
    let (fragment_idx, expand_idx) = expand_nodes[0];
    inject_keyed_assert_before_expand_node(
        &mut build_result.fragment_results[fragment_idx],
        expand_idx,
        keyed_assert,
    )?;
    renumber_plan_node_ids_preserving_preorder(build_result)?;
    Ok(())
}

fn change_event_expand_positions(
    build_result: &crate::sql::codegen::MultiFragmentBuildResult,
) -> Vec<(usize, usize)> {
    build_result
        .fragment_results
        .iter()
        .enumerate()
        .flat_map(|(fragment_idx, fragment)| {
            fragment
                .plan
                .nodes
                .iter()
                .enumerate()
                .filter_map(move |(node_idx, node)| {
                    (node.node_type
                        == crate::thrift::plan_nodes::TPlanNodeType::CHANGE_EVENT_EXPAND_NODE)
                        .then_some((fragment_idx, node_idx))
                })
        })
        .collect()
}

fn inject_keyed_assert_before_expand_node(
    fragment: &mut crate::sql::codegen::FragmentBuildResult,
    expand_idx: usize,
    keyed_assert: &DmlPreExpandKeyedAssert,
) -> Result<(), String> {
    let expand = fragment
        .plan
        .nodes
        .get(expand_idx)
        .ok_or_else(|| "DML change-stream ChangeEventExpand index out of range".to_string())?;
    if expand.num_children != 1 {
        return Err(format!(
            "DML change-stream ChangeEventExpand node_id={} expected one child, got {}",
            expand.node_id, expand.num_children
        ));
    }
    let child = fragment.plan.nodes.get(expand_idx + 1).ok_or_else(|| {
        format!(
            "DML change-stream ChangeEventExpand node_id={} missing child node",
            expand.node_id
        )
    })?;
    let key_slot =
        find_key_slot_for_pre_expand_assert(&fragment.desc_tbl, expand, child, keyed_assert)?;
    let mut assert_node = crate::sql::codegen::nodes::default_plan_node();
    assert_node.node_id = -1;
    assert_node.node_type = crate::thrift::plan_nodes::TPlanNodeType::ASSERT_NUM_ROWS_NODE;
    assert_node.num_children = 1;
    assert_node.limit = -1;
    assert_node.row_tuples = child.row_tuples.clone();
    assert_node.nullable_tuples = child.nullable_tuples.clone();
    assert_node.compact_data = true;
    assert_node.assert_num_rows_node = Some(crate::thrift::plan_nodes::TAssertNumRowsNode {
        desired_num_rows: Some(1),
        subquery_string: Some("DML change-stream matched row uniqueness".to_string()),
        assertion: Some(crate::thrift::plan_nodes::TAssertion::LE),
        group_key_slots: Some(vec![key_slot]),
        group_key_labels: Some(vec![keyed_assert.key_label.clone()]),
        keyed_message_prefix: Some(keyed_assert.message_prefix.clone()),
    });
    fragment.plan.nodes.insert(expand_idx + 1, assert_node);
    Ok(())
}

fn find_key_slot_for_pre_expand_assert(
    desc_tbl: &crate::thrift::descriptors::TDescriptorTable,
    expand: &crate::thrift::plan_nodes::TPlanNode,
    child: &crate::thrift::plan_nodes::TPlanNode,
    keyed_assert: &DmlPreExpandKeyedAssert,
) -> Result<i32, String> {
    if can_derive_key_from_row_id_assignment(keyed_assert)
        && let Some(slot_id) =
            find_key_slot_from_change_event_assignment(desc_tbl, expand, child, keyed_assert)?
    {
        return Ok(slot_id);
    }
    find_slot_id_in_row_tuples(desc_tbl, &child.row_tuples, &keyed_assert.key_column_name)
}

fn can_derive_key_from_row_id_assignment(keyed_assert: &DmlPreExpandKeyedAssert) -> bool {
    keyed_assert
        .key_column_name
        .eq_ignore_ascii_case("__nr_row_id")
        && keyed_assert
            .key_label
            .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
}

fn find_key_slot_from_change_event_assignment(
    desc_tbl: &crate::thrift::descriptors::TDescriptorTable,
    expand: &crate::thrift::plan_nodes::TPlanNode,
    child: &crate::thrift::plan_nodes::TPlanNode,
    keyed_assert: &DmlPreExpandKeyedAssert,
) -> Result<Option<i32>, String> {
    let Some(expand_payload) = expand.change_event_expand_node.as_ref() else {
        return Ok(None);
    };
    let Some(output_slot_id) = find_slot_id_by_name(
        desc_tbl,
        &expand_payload.output_slot_ids,
        &keyed_assert.key_label,
    )?
    else {
        return Ok(None);
    };

    let mut key_slot_id = None;
    for event in &expand_payload.events {
        for assignment in &event.assignments {
            if assignment.output_slot_id != output_slot_id {
                continue;
            }
            let Some(expr) = assignment.expr.as_ref() else {
                continue;
            };
            let Some(slot_id) = direct_slot_ref_slot_id(expr) else {
                continue;
            };
            if !slot_id_belongs_to_row_tuples(desc_tbl, &child.row_tuples, slot_id)? {
                return Err(format!(
                    "DML change-stream keyed assert assignment slot {slot_id} is not produced by the ChangeEventExpand child"
                ));
            }
            if let Some(previous) = key_slot_id {
                if previous != slot_id {
                    return Err(format!(
                        "DML change-stream keyed assert output `{}` is assigned from multiple child slots: {previous} and {slot_id}",
                        keyed_assert.key_label
                    ));
                }
            } else {
                key_slot_id = Some(slot_id);
            }
        }
    }
    Ok(key_slot_id)
}

fn find_slot_id_by_name(
    desc_tbl: &crate::thrift::descriptors::TDescriptorTable,
    slot_ids: &[i32],
    column_name: &str,
) -> Result<Option<i32>, String> {
    let slots = desc_tbl.slot_descriptors.as_ref().ok_or_else(|| {
        "DML change-stream keyed assert descriptor table has no slots".to_string()
    })?;
    let mut matches = slots.iter().filter(|slot| {
        slot.id.is_some_and(|slot_id| slot_ids.contains(&slot_id))
            && slot
                .col_name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(column_name))
    });
    let Some(slot) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(format!(
            "DML change-stream keyed assert output column `{column_name}` is ambiguous"
        ));
    }
    Ok(slot.id)
}

fn direct_slot_ref_slot_id(expr: &crate::thrift::exprs::TExpr) -> Option<i32> {
    let [node] = expr.nodes.as_slice() else {
        return None;
    };
    if node.node_type != crate::thrift::exprs::TExprNodeType::SLOT_REF {
        return None;
    }
    node.slot_ref.as_ref().map(|slot_ref| slot_ref.slot_id)
}

fn slot_id_belongs_to_row_tuples(
    desc_tbl: &crate::thrift::descriptors::TDescriptorTable,
    row_tuples: &[i32],
    slot_id: i32,
) -> Result<bool, String> {
    let slots = desc_tbl.slot_descriptors.as_ref().ok_or_else(|| {
        "DML change-stream keyed assert descriptor table has no slots".to_string()
    })?;
    Ok(slots.iter().any(|slot| {
        slot.id == Some(slot_id)
            && slot
                .parent
                .is_some_and(|tuple_id| row_tuples.contains(&tuple_id))
    }))
}

fn find_slot_id_in_row_tuples(
    desc_tbl: &crate::thrift::descriptors::TDescriptorTable,
    row_tuples: &[i32],
    column_name: &str,
) -> Result<i32, String> {
    let slots = desc_tbl.slot_descriptors.as_ref().ok_or_else(|| {
        "DML change-stream keyed assert descriptor table has no slots".to_string()
    })?;
    let mut matches = slots.iter().filter(|slot| {
        slot.parent
            .is_some_and(|tuple_id| row_tuples.contains(&tuple_id))
            && slot
                .col_name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(column_name))
    });
    let slot = matches.next().ok_or_else(|| {
        format!("DML change-stream keyed assert column `{column_name}` not found in child layout")
    })?;
    if matches.next().is_some() {
        return Err(format!(
            "DML change-stream keyed assert column `{column_name}` is ambiguous in child layout"
        ));
    }
    slot.id.ok_or_else(|| {
        format!("DML change-stream keyed assert column `{column_name}` has no slot id")
    })
}

fn renumber_plan_node_ids_preserving_preorder(
    build_result: &mut crate::sql::codegen::MultiFragmentBuildResult,
) -> Result<(), String> {
    let mut next_node_id = 1;
    let mut node_id_map = HashMap::new();
    for fragment in &mut build_result.fragment_results {
        if fragment.plan.nodes.is_empty() {
            continue;
        }
        let consumed = assign_preorder_invariant_node_ids(
            &mut fragment.plan.nodes,
            0,
            &mut next_node_id,
            &mut node_id_map,
        )?;
        if consumed != fragment.plan.nodes.len() {
            return Err(format!(
                "DML change-stream keyed assert cannot renumber fragment {}: plan contains {} nodes but first pre-order tree consumed {}",
                fragment.fragment_id,
                fragment.plan.nodes.len(),
                consumed
            ));
        }
    }
    remap_plan_node_references(build_result, &node_id_map)?;
    build_result.refresh_fragment_schedules();
    Ok(())
}

fn assign_preorder_invariant_node_ids(
    nodes: &mut [crate::thrift::plan_nodes::TPlanNode],
    root_idx: usize,
    next_node_id: &mut i32,
    node_id_map: &mut HashMap<i32, i32>,
) -> Result<usize, String> {
    if root_idx >= nodes.len() {
        return Err(format!(
            "DML change-stream keyed assert cannot renumber missing TPlan subtree root at index {root_idx}"
        ));
    }
    let old_root_id = nodes[root_idx].node_id;
    let child_count = nodes[root_idx].num_children;
    if child_count < 0 {
        return Err(format!(
            "DML change-stream keyed assert cannot renumber node {old_root_id}: negative child count {child_count}"
        ));
    }
    let mut next_idx = root_idx + 1;
    for child_ordinal in 0..child_count {
        if next_idx >= nodes.len() {
            return Err(format!(
                "DML change-stream keyed assert cannot renumber node {old_root_id}: missing child {child_ordinal}"
            ));
        }
        next_idx = assign_preorder_invariant_node_ids(nodes, next_idx, next_node_id, node_id_map)?;
    }
    let new_root_id = *next_node_id;
    *next_node_id += 1;
    if node_id_map.insert(old_root_id, new_root_id).is_some() {
        return Err(format!(
            "DML change-stream keyed assert cannot renumber duplicate TPlan node id {old_root_id}"
        ));
    }
    nodes[root_idx].node_id = new_root_id;
    Ok(next_idx)
}

fn remap_plan_node_references(
    build_result: &mut crate::sql::codegen::MultiFragmentBuildResult,
    node_id_map: &HashMap<i32, i32>,
) -> Result<(), String> {
    for fragment in &mut build_result.fragment_results {
        for node in &mut fragment.plan.nodes {
            remap_plan_node_payload_references(node, node_id_map)?;
        }
        remap_btree_map_keys(&mut fragment.exec_params.per_node_scan_ranges, node_id_map)?;
        remap_btree_map_keys(&mut fragment.native_scan_ranges, node_id_map)?;
        remap_btree_map_keys(&mut fragment.exec_params.per_exch_num_senders, node_id_map)?;
        if let Some(ranges) = fragment
            .exec_params
            .node_to_per_driver_seq_scan_ranges
            .as_mut()
        {
            remap_btree_map_keys(ranges, node_id_map)?;
        }
        for (_, exchange_node_id, _) in &mut fragment.cte_exchange_nodes {
            *exchange_node_id = remap_node_id(*exchange_node_id, node_id_map)?;
        }
        for boundary in &mut fragment.boundary_schemas {
            remap_boundary_schema_node_id(boundary, node_id_map)?;
        }
    }
    for edge in &mut build_result.edges {
        edge.target_exchange_node_id = remap_node_id(edge.target_exchange_node_id, node_id_map)?;
    }
    for lowered_edge in &mut build_result.lowered_edges {
        lowered_edge.edge.target_exchange_node_id =
            remap_node_id(lowered_edge.edge.target_exchange_node_id, node_id_map)?;
    }
    for boundary in &mut build_result.boundary_schemas {
        remap_boundary_schema_node_id(boundary, node_id_map)?;
    }
    if let Some(rf_plan) = build_result.rf_plan.as_mut() {
        for desc in rf_plan.all_filters.values_mut() {
            desc.build_plan_node_id = remap_node_id(desc.build_plan_node_id, node_id_map)?;
            for target_node_id in &mut desc.probe_target_node_ids {
                *target_node_id = remap_node_id(*target_node_id, node_id_map)?;
            }
            desc.probe_target_node_ids.sort_unstable();
            desc.probe_target_node_ids.dedup();
        }
        for probes in rf_plan.probe_side_filters.values_mut() {
            for (_, probe_node_id) in probes {
                *probe_node_id = remap_node_id(*probe_node_id, node_id_map)?;
            }
        }
    }
    Ok(())
}

fn remap_plan_node_payload_references(
    node: &mut crate::thrift::plan_nodes::TPlanNode,
    node_id_map: &HashMap<i32, i32>,
) -> Result<(), String> {
    if let Some(filters) = node.probe_runtime_filters.as_mut() {
        for filter in filters {
            remap_runtime_filter_description(filter, node_id_map)?;
        }
    }
    if let Some(waiting_set) = node.local_rf_waiting_set.as_mut() {
        let mut remapped = std::collections::BTreeSet::new();
        for build_node_id in std::mem::take(waiting_set) {
            remapped.insert(remap_node_id(build_node_id, node_id_map)?);
        }
        *waiting_set = remapped;
    }
    if let Some(hdfs_scan) = node.hdfs_scan_node.as_mut()
        && let Some(scan_node_id) = hdfs_scan.scan_node_id
    {
        hdfs_scan.scan_node_id = Some(i64::from(remap_node_id(
            i32::try_from(scan_node_id).map_err(|_| {
                format!(
                    "DML change-stream keyed assert cannot remap HDFS scan_node_id {scan_node_id}: out of i32 range"
                )
            })?,
            node_id_map,
        )?));
    }
    if let Some(fetch) = node.fetch_node.as_mut()
        && let Some(target_node_id) = fetch.target_node_id
    {
        fetch.target_node_id = Some(remap_node_id(target_node_id, node_id_map)?);
    }
    Ok(())
}

fn remap_runtime_filter_description(
    desc: &mut crate::thrift::runtime_filter::TRuntimeFilterDescription,
    node_id_map: &HashMap<i32, i32>,
) -> Result<(), String> {
    if let Some(build_node_id) = desc.build_plan_node_id {
        desc.build_plan_node_id = Some(remap_node_id(build_node_id, node_id_map)?);
    }
    if let Some(target_exprs) = desc.plan_node_id_to_target_expr.as_mut() {
        remap_btree_map_keys(target_exprs, node_id_map)?;
    }
    if let Some(partition_exprs) = desc.plan_node_id_to_partition_by_exprs.as_mut() {
        remap_btree_map_keys(partition_exprs, node_id_map)?;
    }
    Ok(())
}

fn remap_boundary_schema_node_id(
    boundary: &mut crate::sql::codegen::boundary_schema::BoundarySchemaReport,
    node_id_map: &HashMap<i32, i32>,
) -> Result<(), String> {
    if boundary.node_id >= 0 {
        boundary.node_id = remap_node_id(boundary.node_id, node_id_map)?;
    }
    Ok(())
}

fn remap_btree_map_keys<V>(
    map: &mut BTreeMap<i32, V>,
    node_id_map: &HashMap<i32, i32>,
) -> Result<(), String> {
    let old = std::mem::take(map);
    for (old_key, value) in old {
        let new_key = remap_node_id(old_key, node_id_map)?;
        if map.insert(new_key, value).is_some() {
            return Err(format!(
                "DML change-stream keyed assert remapped multiple node-id references to {new_key}"
            ));
        }
    }
    Ok(())
}

fn remap_node_id(node_id: i32, node_id_map: &HashMap<i32, i32>) -> Result<i32, String> {
    node_id_map.get(&node_id).copied().ok_or_else(|| {
        format!("DML change-stream keyed assert cannot remap unknown TPlan node id {node_id}")
    })
}

fn build_dml_change_stream_dag_from_sink_specs(
    branch_set: DmlChangeStreamBranchSet,
    producer_output_columns: &[OutputColumn],
    mut sink_specs: DmlChangeStreamWriteBranchSinkSpecs,
) -> Result<ChangeStreamWriteDagSpec, String> {
    let branch_kinds = branch_set.branch_kinds();
    if branch_kinds.is_empty() {
        return Err("DML change-stream write requires at least one branch".to_string());
    }
    let has_data_branch = branch_kinds.iter().any(|kind| {
        matches!(
            kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        )
    });
    let change_op_output_ordinal = output_ordinal_by_name(
        producer_output_columns,
        crate::exec::change_op::CHANGE_OP_COLUMN,
        "change-op column",
        OutputBindingKind::Internal,
    )?;
    let data_route_output_ordinal = if has_data_branch {
        Some(output_ordinal_by_name(
            producer_output_columns,
            DML_CHANGE_STREAM_DATA_ROUTE_COLUMN,
            "data-route column",
            OutputBindingKind::Internal,
        )?)
    } else {
        None
    };
    let data_partition_ordinals = if has_data_branch {
        target_partition_source_ordinals(
            producer_output_columns,
            &sink_specs.target_partition_source_columns,
        )?
    } else {
        Vec::new()
    };

    let mut branches = Vec::with_capacity(branch_kinds.len());
    for (idx, branch_kind) in branch_kinds.into_iter().enumerate() {
        let (sink_spec, output_partition_ordinals) = match branch_kind {
            ChangeStreamBranchKind::DeleteDv => {
                let sink_spec = sink_specs
                    .delete_dv
                    .take()
                    .ok_or_else(|| "DML change-stream DeleteDv sink spec is missing".to_string())?;
                let file_ordinal = output_ordinal_by_name(
                    producer_output_columns,
                    crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                    "delete file column",
                    OutputBindingKind::Internal,
                )?;
                (sink_spec, vec![file_ordinal])
            }
            ChangeStreamBranchKind::ReuseData => {
                let sink_spec = sink_specs.reuse_data.take().ok_or_else(|| {
                    "DML change-stream ReuseData sink spec is missing".to_string()
                })?;
                (sink_spec, data_partition_ordinals.clone())
            }
            ChangeStreamBranchKind::FreshData => {
                let sink_spec = sink_specs.fresh_data.take().ok_or_else(|| {
                    "DML change-stream FreshData sink spec is missing".to_string()
                })?;
                (sink_spec, data_partition_ordinals.clone())
            }
        };
        let stream_output_ordinals =
            output_ordinals_for_sink_columns(producer_output_columns, &sink_spec.target_columns)?;
        branches.push(ChangeStreamWriteBranchSpec {
            branch_id: i32::try_from(idx).map_err(|_| {
                "DML change-stream branch id overflow while building DAG".to_string()
            })?,
            branch_kind,
            stream_output_ordinals,
            output_partition_ordinals,
            sink_spec,
        });
    }

    let dag = ChangeStreamWriteDagSpec {
        change_op_output_ordinal: Some(change_op_output_ordinal),
        data_route_output_ordinal,
        branches,
    };
    dag.validate()?;
    Ok(dag)
}

pub(crate) fn execute_dml_change_stream_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    mut plan: DmlChangeStreamWritePlan,
    query_opts: Option<&StandaloneQueryOptions>,
) -> Result<DmlChangeStreamWriteExecution, String> {
    let crate::engine::PlannedIcebergChangeStreamWrite {
        build_result,
        native_sidecars,
        commit_plan,
        #[cfg(test)]
        topology,
    } = plan_dml_change_stream_write(state, target, &mut plan)?;
    #[cfg(test)]
    if let Some(result) = crate::engine::observe_change_stream_write_build_for_test(&topology) {
        return dml_change_stream_write_execution(result, commit_plan);
    }
    let result = crate::engine::execute_planned_iceberg_change_stream_write(
        build_result,
        native_sidecars,
        query_opts.cloned(),
    )?;
    dml_change_stream_write_execution(result, commit_plan)
}

pub(crate) fn plan_dml_change_stream_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    plan: &mut DmlChangeStreamWritePlan,
) -> Result<crate::engine::PlannedIcebergChangeStreamWrite, String> {
    let native_keyed_assert = plan.pre_expand_keyed_assert.clone();
    let planned =
        crate::engine::build_physical_plan_as_iceberg_change_stream_write_with_native_plan_mutation(
        state,
        Some(&target.catalog),
        &target.namespace,
        &plan.producer,
        &mut plan.dag,
        None,
        native_keyed_assert.map(|keyed_assert| {
            Box::new(move |native_plan: &mut crate::sql::planner::DistributedPlan| {
                inject_dml_pre_expand_keyed_assert_into_native_plan(native_plan, &keyed_assert)
            })
                as Box<
                    dyn FnOnce(&mut crate::sql::planner::DistributedPlan) -> Result<(), String>,
                >
        }),
    )?;
    let crate::engine::PlannedIcebergChangeStreamWrite {
        mut build_result,
        native_sidecars,
        commit_plan,
        #[cfg(test)]
        topology,
    } = planned;
    inject_dml_pre_expand_keyed_assert(&mut build_result, plan.pre_expand_keyed_assert.as_ref())?;
    Ok(crate::engine::PlannedIcebergChangeStreamWrite {
        build_result,
        native_sidecars,
        commit_plan,
        #[cfg(test)]
        topology,
    })
}

pub(crate) fn inject_dml_pre_expand_keyed_assert_into_native_plan(
    plan: &mut crate::sql::planner::DistributedPlan,
    keyed_assert: &DmlPreExpandKeyedAssert,
) -> Result<(), String> {
    let mut next_node_id = next_native_node_id(plan);
    let mut expand_count = 0usize;
    for fragment in &mut plan.fragments {
        inject_native_keyed_assert_before_expand_node(
            &mut fragment.root,
            keyed_assert,
            &mut next_node_id,
            &mut expand_count,
        )?;
    }
    if expand_count != 1 {
        return Err(format!(
            "DML change-stream keyed assert requires exactly one native ChangeEventExpand node, found {expand_count}"
        ));
    }
    renumber_native_plan_node_ids_preserving_preorder(plan)?;
    Ok(())
}

fn inject_native_keyed_assert_before_expand_node(
    node: &mut crate::sql::planner::DistributedNode,
    keyed_assert: &DmlPreExpandKeyedAssert,
    next_node_id: &mut i32,
    expand_count: &mut usize,
) -> Result<(), String> {
    for child in &mut node.children {
        inject_native_keyed_assert_before_expand_node(
            child,
            keyed_assert,
            next_node_id,
            expand_count,
        )?;
    }

    if !matches!(
        node.payload,
        crate::sql::planner::DistributedPayload::Physical(
            crate::sql::planner::plan::PhysicalPlanKind::ChangeEventExpand(_)
        )
    ) {
        return Ok(());
    }

    *expand_count += 1;
    if node.children.len() != 1 {
        return Err(format!(
            "DML change-stream native ChangeEventExpand node_id={} expected one child, got {}",
            node.node_id,
            node.children.len()
        ));
    }

    let key_column_id = find_native_key_column_id_for_pre_expand_assert(node, keyed_assert)?;
    let original_child = node.children.pop().expect("validated single child");
    let assert_node = crate::sql::planner::DistributedNode {
        node_id: *next_node_id,
        fragment_id: original_child.fragment_id,
        tuple_ids: original_child.tuple_ids.clone(),
        nullable_tuple_ids: original_child.nullable_tuple_ids.clone(),
        limit: -1,
        build_runtime_filters: vec![],
        probe_runtime_filters: vec![],
        children: vec![original_child],
        stats: node.stats.clone(),
        payload: crate::sql::planner::DistributedPayload::Physical(
            crate::sql::planner::plan::PhysicalPlanKind::AssertOneRow(
                crate::sql::planner::plan::PlanAssertOneRowNode::per_key_at_most_one(
                    "DML change-stream matched row uniqueness",
                    vec![key_column_id],
                    vec![keyed_assert.key_label.clone()],
                    keyed_assert.message_prefix.clone(),
                ),
            ),
        ),
    };
    *next_node_id += 1;
    node.children.push(assert_node);
    Ok(())
}

fn find_native_key_column_id_for_pre_expand_assert(
    expand_node: &crate::sql::planner::DistributedNode,
    keyed_assert: &DmlPreExpandKeyedAssert,
) -> Result<crate::sql::column_id::ColumnId, String> {
    let child = expand_node.children.first().ok_or_else(|| {
        format!(
            "DML change-stream native ChangeEventExpand node_id={} missing child",
            expand_node.node_id
        )
    })?;
    match find_output_column_id_by_name(child, &keyed_assert.key_column_name) {
        Ok(column_id) => Ok(column_id),
        Err(name_err) if can_derive_key_from_row_id_assignment(keyed_assert) => {
            if let Some(column_id) =
                find_native_key_column_id_from_change_event_assignment(expand_node, keyed_assert)?
            {
                Ok(column_id)
            } else {
                Err(name_err)
            }
        }
        Err(err) => Err(err),
    }
}

fn find_native_key_column_id_from_change_event_assignment(
    expand_node: &crate::sql::planner::DistributedNode,
    keyed_assert: &DmlPreExpandKeyedAssert,
) -> Result<Option<crate::sql::column_id::ColumnId>, String> {
    let crate::sql::planner::DistributedPayload::Physical(
        crate::sql::planner::plan::PhysicalPlanKind::ChangeEventExpand(expand),
    ) = &expand_node.payload
    else {
        return Ok(None);
    };
    let mut output_columns = expand
        .output_columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(&keyed_assert.key_label));
    let Some(output_column) = output_columns.next() else {
        return Ok(None);
    };
    if output_columns.next().is_some() {
        return Err(format!(
            "DML change-stream native keyed assert output column `{}` is ambiguous",
            keyed_assert.key_label
        ));
    }

    let mut key_column_id = None;
    for event in &expand.events {
        for assignment in &event.assignments {
            if assignment.output_column_id != output_column.column_id {
                continue;
            }
            let Some(expr) = assignment.expr.as_ref() else {
                continue;
            };
            let crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
                continue;
            };
            if let Some(previous) = key_column_id {
                if previous != *column_id {
                    return Err(format!(
                        "DML change-stream native keyed assert output `{}` is assigned from multiple child columns: {:?} and {:?}",
                        keyed_assert.key_label, previous, column_id
                    ));
                }
            } else {
                key_column_id = Some(*column_id);
            }
        }
    }
    Ok(key_column_id)
}

fn next_native_node_id(plan: &crate::sql::planner::DistributedPlan) -> i32 {
    plan.fragments
        .iter()
        .flat_map(|fragment| native_node_ids(&fragment.root))
        .max()
        .unwrap_or_default()
        + 1
}

fn native_node_ids(node: &crate::sql::planner::DistributedNode) -> Vec<i32> {
    let mut ids = vec![node.node_id];
    for child in &node.children {
        ids.extend(native_node_ids(child));
    }
    ids
}

fn renumber_native_plan_node_ids_preserving_preorder(
    plan: &mut crate::sql::planner::DistributedPlan,
) -> Result<(), String> {
    let mut next_node_id = 1;
    let mut node_id_map = HashMap::new();
    for fragment in &mut plan.fragments {
        assign_native_preorder_invariant_node_ids(
            &mut fragment.root,
            &mut next_node_id,
            &mut node_id_map,
        )?;
    }
    remap_native_plan_node_references(plan, &node_id_map)?;
    Ok(())
}

fn assign_native_preorder_invariant_node_ids(
    node: &mut crate::sql::planner::DistributedNode,
    next_node_id: &mut i32,
    node_id_map: &mut HashMap<i32, i32>,
) -> Result<(), String> {
    for child in &mut node.children {
        assign_native_preorder_invariant_node_ids(child, next_node_id, node_id_map)?;
    }
    let old_node_id = node.node_id;
    let new_node_id = *next_node_id;
    *next_node_id += 1;
    if node_id_map.insert(old_node_id, new_node_id).is_some() {
        return Err(format!(
            "DML change-stream keyed assert cannot renumber duplicate native node id {old_node_id}"
        ));
    }
    node.node_id = new_node_id;
    Ok(())
}

fn remap_native_plan_node_references(
    plan: &mut crate::sql::planner::DistributedPlan,
    node_id_map: &HashMap<i32, i32>,
) -> Result<(), String> {
    for fragment in &mut plan.fragments {
        for (_, exchange_node_id, _) in &mut fragment.cte_exchange_nodes {
            *exchange_node_id = remap_native_node_id(*exchange_node_id, node_id_map)?;
        }
        if let crate::sql::planner::DataSink::IcebergChangeStreamRouter(router) = &mut fragment.sink
        {
            for branch in &mut router.branches {
                branch.target_exchange_node_id =
                    remap_native_node_id(branch.target_exchange_node_id, node_id_map)?;
            }
        }
    }
    for edge in &mut plan.edges {
        edge.target_exchange_node_id =
            remap_native_node_id(edge.target_exchange_node_id, node_id_map)?;
    }
    Ok(())
}

fn remap_native_node_id(node_id: i32, node_id_map: &HashMap<i32, i32>) -> Result<i32, String> {
    node_id_map.get(&node_id).copied().ok_or_else(|| {
        format!("DML change-stream keyed assert cannot remap unknown native node id {node_id}")
    })
}

fn find_output_column_id_by_name(
    node: &crate::sql::planner::DistributedNode,
    column_name: &str,
) -> Result<crate::sql::column_id::ColumnId, String> {
    if let crate::sql::planner::DistributedPayload::Physical(
        crate::sql::planner::plan::PhysicalPlanKind::Project(project),
    ) = &node.payload
    {
        let mut matches = project
            .items
            .iter()
            .filter(|item| item.output_name.eq_ignore_ascii_case(column_name));
        let item = matches.next().ok_or_else(|| {
            format!(
                "DML change-stream keyed assert column `{column_name}` not found in native Project child"
            )
        })?;
        if matches.next().is_some() {
            return Err(format!(
                "DML change-stream keyed assert column `{column_name}` is ambiguous in native Project child"
            ));
        }
        return Ok(item.output_column_id);
    }

    let columns = native_node_output_columns(node).ok_or_else(|| {
        format!(
            "DML change-stream keyed assert cannot infer output columns for native node {}",
            node.node_id
        )
    })?;
    let mut matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(column_name));
    let column = matches.next().ok_or_else(|| {
        format!("DML change-stream keyed assert column `{column_name}` not found in native child")
    })?;
    if matches.next().is_some() {
        return Err(format!(
            "DML change-stream keyed assert column `{column_name}` is ambiguous in native child"
        ));
    }
    Ok(column.column_id)
}

fn native_node_output_columns(
    node: &crate::sql::planner::DistributedNode,
) -> Option<&[crate::sql::analysis::OutputColumn]> {
    match &node.payload {
        crate::sql::planner::DistributedPayload::Exchange(exchange) => {
            Some(&exchange.output_columns)
        }
        crate::sql::planner::DistributedPayload::Physical(kind) => match kind {
            crate::sql::planner::plan::PhysicalPlanKind::Scan(scan) => Some(&scan.columns),
            crate::sql::planner::plan::PhysicalPlanKind::Sort(sort) => Some(&sort.output_columns),
            crate::sql::planner::plan::PhysicalPlanKind::Values(values) => Some(&values.columns),
            crate::sql::planner::plan::PhysicalPlanKind::Window(window) => {
                Some(&window.output_columns)
            }
            crate::sql::planner::plan::PhysicalPlanKind::TableFunction(table_function) => {
                Some(&table_function.output_columns)
            }
            crate::sql::planner::plan::PhysicalPlanKind::HashAggregate(aggregate) => {
                Some(&aggregate.output_columns)
            }
            crate::sql::planner::plan::PhysicalPlanKind::SetOp(set_op) => {
                Some(&set_op.output_columns)
            }
            crate::sql::planner::plan::PhysicalPlanKind::ChangeEventExpand(expand) => {
                Some(&expand.output_columns)
            }
            crate::sql::planner::plan::PhysicalPlanKind::CTEProduce(produce) => {
                Some(&produce.output_columns)
            }
            crate::sql::planner::plan::PhysicalPlanKind::CTEConsume(consume) => {
                Some(&consume.output_columns)
            }
            crate::sql::planner::plan::PhysicalPlanKind::Redistribute(redistribute) => {
                Some(&redistribute.output_columns)
            }
            crate::sql::planner::plan::PhysicalPlanKind::Filter(_)
            | crate::sql::planner::plan::PhysicalPlanKind::Project(_)
            | crate::sql::planner::plan::PhysicalPlanKind::Limit(_)
            | crate::sql::planner::plan::PhysicalPlanKind::AssertOneRow(_)
            | crate::sql::planner::plan::PhysicalPlanKind::TopN(_)
            | crate::sql::planner::plan::PhysicalPlanKind::HashJoin(_)
            | crate::sql::planner::plan::PhysicalPlanKind::NestLoopJoin(_)
            | crate::sql::planner::plan::PhysicalPlanKind::Repeat(_)
            | crate::sql::planner::plan::PhysicalPlanKind::GenerateSeries(_)
            | crate::sql::planner::plan::PhysicalPlanKind::CTEAnchor(_) => {
                node.children.first().and_then(native_node_output_columns)
            }
        },
    }
}

fn dml_change_stream_write_execution(
    result: CoordinatedQueryResult,
    commit_plan: crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan,
) -> Result<DmlChangeStreamWriteExecution, String> {
    if let Some(abort) = result.write_abort.as_ref() {
        return Err(abort.reason.clone());
    }
    if result.write_commit.is_none() {
        return Err("DML change-stream write completed without writer commit".to_string());
    }
    Ok(DmlChangeStreamWriteExecution {
        result,
        commit_plan,
    })
}

fn target_partition_source_column_names(
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<String>, String> {
    let schema = metadata.current_schema();
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| {
            let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                format!(
                    "DML change-stream partition source field id {} not found in target schema",
                    field.source_id
                )
            })?;
            Ok(source.name.clone())
        })
        .collect()
}

fn target_partition_source_ordinals(
    output_columns: &[OutputColumn],
    source_columns: &[String],
) -> Result<Vec<usize>, String> {
    source_columns
        .iter()
        .map(|name| {
            output_ordinal_by_name(
                output_columns,
                name,
                "target partition source column",
                OutputBindingKind::UserVisible,
            )
        })
        .collect()
}

fn output_ordinals_for_sink_columns(
    output_columns: &[OutputColumn],
    sink_columns: &[crate::engine::catalog::ColumnDef],
) -> Result<Vec<usize>, String> {
    sink_columns
        .iter()
        .map(|column| {
            output_ordinal_by_name(
                output_columns,
                &column.name,
                "sink input column",
                binding_kind_for_sink_column(&column.name),
            )
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputBindingKind {
    Internal,
    UserVisible,
}

fn binding_kind_for_sink_column(name: &str) -> OutputBindingKind {
    if is_reserved_internal_output_name(name) {
        OutputBindingKind::Internal
    } else {
        OutputBindingKind::UserVisible
    }
}

fn is_reserved_internal_output_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
        || name.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        || name.eq_ignore_ascii_case(DML_CHANGE_STREAM_DATA_ROUTE_COLUMN)
}

fn output_ordinal_by_name(
    output_columns: &[OutputColumn],
    name: &str,
    label: &str,
    binding_kind: OutputBindingKind,
) -> Result<usize, String> {
    let mut matches = output_columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name.eq_ignore_ascii_case(name));
    let (ordinal, column) = matches
        .next()
        .ok_or_else(|| format!("DML change-stream {label} `{name}` not found in plan output"))?;
    if matches.next().is_some() {
        return Err(format!(
            "DML change-stream {label} `{name}` is ambiguous in plan output"
        ));
    }
    match binding_kind {
        OutputBindingKind::Internal if !column.is_internal => {
            return Err(format!(
                "DML change-stream {label} `{name}` must be marked internal in plan output"
            ));
        }
        OutputBindingKind::UserVisible if column.is_internal => {
            return Err(format!(
                "DML change-stream {label} `{name}` must be user-visible in plan output"
            ));
        }
        OutputBindingKind::Internal | OutputBindingKind::UserVisible => {}
    }
    Ok(ordinal)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::sql::common::ChangeStreamBranchKind;

    fn output_column(name: &str, ordinal: u32) -> crate::sql::analysis::OutputColumn {
        output_column_with_internal(name, ordinal, name.starts_with('_'))
    }

    fn output_column_with_internal(
        name: &str,
        ordinal: u32,
        is_internal: bool,
    ) -> crate::sql::analysis::OutputColumn {
        crate::sql::analysis::OutputColumn {
            column_id: crate::sql::column_id::ColumnId::new_for_test(ordinal + 1),
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal,
        }
    }

    fn producer_output_columns() -> Vec<crate::sql::analysis::OutputColumn> {
        vec![
            output_column(crate::exec::row_position::ICEBERG_FILE_PATH_COL, 0),
            output_column(crate::exec::row_position::ICEBERG_ROW_POS_COL, 1),
            output_column("region", 2),
            output_column("id", 3),
            output_column(crate::exec::row_position::ICEBERG_ROW_ID_COL, 4),
            output_column(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL, 5),
            output_column(crate::exec::change_op::CHANGE_OP_COLUMN, 6),
            output_column("__change_data_route", 7),
        ]
    }

    fn column(name: &str) -> crate::engine::catalog::ColumnDef {
        crate::engine::catalog::ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn sink_specs_for_partitioned_target() -> DmlChangeStreamWriteBranchSinkSpecs {
        let mut delete_dv = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        delete_dv.mode = crate::sql::planner::write_sink::IcebergWriteSinkMode::DeletionVectors;
        delete_dv.target_columns = vec![
            column(crate::exec::row_position::ICEBERG_FILE_PATH_COL),
            column(crate::exec::row_position::ICEBERG_ROW_POS_COL),
            column("region"),
        ];

        let mut reuse_data = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        reuse_data.mode = crate::sql::planner::write_sink::IcebergWriteSinkMode::RowLineageData;
        reuse_data.target_columns = vec![
            column("id"),
            column("region"),
            column(crate::exec::row_position::ICEBERG_ROW_ID_COL),
            column(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL),
        ];

        let mut fresh_data = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        fresh_data.mode = crate::sql::planner::write_sink::IcebergWriteSinkMode::Data;
        fresh_data.target_columns = vec![column("id"), column("region")];

        DmlChangeStreamWriteBranchSinkSpecs {
            delete_dv: Some(delete_dv),
            reuse_data: Some(reuse_data),
            fresh_data: Some(fresh_data),
            target_partition_source_columns: vec!["region".to_string()],
        }
    }

    fn sink_specs_for_unpartitioned_target() -> DmlChangeStreamWriteBranchSinkSpecs {
        DmlChangeStreamWriteBranchSinkSpecs {
            target_partition_source_columns: Vec::new(),
            ..sink_specs_for_partitioned_target()
        }
    }

    fn branch_kinds(
        dag: &crate::sql::planner::ChangeStreamWriteDagSpec,
    ) -> Vec<ChangeStreamBranchKind> {
        dag.branches
            .iter()
            .map(|branch| branch.branch_kind)
            .collect()
    }

    fn physical_values_plan_for_execution_test() -> crate::sql::optimizer::OptimizerPhysicalNode {
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        use crate::sql::optimizer::physical_tree::{
            OptimizerExplainStats, OptimizerPhysicalNode, PlanExecutionProps, attach_scalar_arena,
        };
        use crate::sql::optimizer::scalar::ScalarArena;
        use crate::sql::optimizer::statistics::Statistics;

        let output_columns = vec![
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: true,
            },
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "__change_data_route".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: true,
            },
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(3),
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            },
        ];
        let mut physical_plan = OptimizerPhysicalNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: output_columns.clone(),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 0.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut physical_plan, Arc::new(ScalarArena::new()));
        physical_plan
    }

    fn execution_test_plan() -> DmlChangeStreamWritePlan {
        let mut branch =
            crate::sql::planner::ChangeStreamWriteBranchSpec::reuse_data_for_test(vec![2]);
        branch.output_partition_ordinals = Vec::new();
        DmlChangeStreamWritePlan {
            producer: physical_values_plan_for_execution_test(),
            dag: crate::sql::planner::ChangeStreamWriteDagSpec::for_test(
                Some(0),
                Some(1),
                vec![branch],
            ),
            pre_expand_keyed_assert: None,
        }
    }

    fn target_for_execution_test() -> crate::engine::backend_resolver::TargetBackend {
        crate::engine::backend_resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: "test_catalog".to_string(),
            namespace: "default".to_string(),
            table: "target_orders".to_string(),
        }
    }

    fn keyed_assert_for_test() -> DmlPreExpandKeyedAssert {
        DmlPreExpandKeyedAssert {
            key_column_name: "__nr_row_id".to_string(),
            key_label: "_row_id".to_string(),
            message_prefix: "MOR UPDATE matched target row".to_string(),
        }
    }

    fn change_event_expand_plan_node(node_id: i32) -> crate::thrift::plan_nodes::TPlanNode {
        let mut node = crate::sql::codegen::nodes::default_plan_node();
        node.node_id = node_id;
        node.node_type = crate::thrift::plan_nodes::TPlanNodeType::CHANGE_EVENT_EXPAND_NODE;
        node.num_children = 1;
        node.row_tuples = vec![2];
        node.change_event_expand_node = Some(crate::thrift::plan_nodes::TChangeEventExpandNode {
            events: Vec::new(),
            output_slot_ids: Vec::new(),
            change_op_slot_id: 4,
            data_route_slot_id: Some(5),
        });
        node.nullable_tuples = vec![false];
        node
    }

    fn exchange_plan_node(node_id: i32, row_tuple: i32) -> crate::thrift::plan_nodes::TPlanNode {
        let mut node = crate::sql::codegen::nodes::default_plan_node();
        node.node_id = node_id;
        node.node_type = crate::thrift::plan_nodes::TPlanNodeType::EXCHANGE_NODE;
        node.num_children = 0;
        node.row_tuples = vec![row_tuple];
        node
    }

    fn keyed_assert_fragment(
        nodes: Vec<crate::thrift::plan_nodes::TPlanNode>,
    ) -> crate::sql::codegen::FragmentBuildResult {
        crate::sql::codegen::FragmentBuildResult {
            fragment_id: 0,
            has_scan_nodes: false,
            output_kind: crate::sql::codegen::FragmentOutputKind::Result,
            plan: crate::thrift::plan_nodes::TPlan::new(nodes),
            desc_tbl: crate::thrift::descriptors::TDescriptorTable::new(
                Some(vec![crate::thrift::descriptors::TSlotDescriptor::new(
                    Some(7),
                    Some(1),
                    None,
                    Some(0),
                    Some(0),
                    Some(0),
                    Some(0),
                    Some("__nr_row_id".to_string()),
                    Some(0),
                    Some(true),
                    Some(true),
                    Some(false),
                    None,
                    None::<String>,
                    None::<bool>,
                )]),
                Vec::new(),
                None::<Vec<crate::thrift::descriptors::TTableDescriptor>>,
                None::<bool>,
            ),
            exec_params: crate::thrift::internal_service::TPlanFragmentExecParams::new(
                crate::thrift::types::TUniqueId::new(1, 1),
                crate::thrift::types::TUniqueId::new(2, 2),
                std::collections::BTreeMap::new(),
                std::collections::BTreeMap::new(),
                None::<Vec<crate::thrift::data_sinks::TPlanFragmentDestination>>,
                None::<i32>,
                None::<i32>,
                None::<bool>,
                None::<bool>,
                None::<crate::thrift::runtime_filter::TRuntimeFilterParams>,
                None::<i32>,
                None::<bool>,
                None::<
                    std::collections::BTreeMap<
                        crate::thrift::types::TPlanNodeId,
                        std::collections::BTreeMap<
                            i32,
                            Vec<crate::thrift::internal_service::TScanRangeParams>,
                        >,
                    >,
                >,
                None::<bool>,
                None::<i32>,
                None::<bool>,
                None::<Vec<crate::thrift::internal_service::TExecDebugOption>>,
            ),
            native_scan_ranges: std::collections::BTreeMap::new(),
            output_sink: crate::thrift::data_sinks::TDataSink::new(
                crate::thrift::data_sinks::TDataSinkType::RESULT_SINK,
                None::<crate::thrift::data_sinks::TDataStreamSink>,
                None::<crate::thrift::data_sinks::TResultSink>,
                None::<crate::thrift::data_sinks::TMysqlTableSink>,
                None::<crate::thrift::data_sinks::TExportSink>,
                None::<crate::thrift::data_sinks::TOlapTableSink>,
                None::<crate::thrift::data_sinks::TMemoryScratchSink>,
                None::<crate::thrift::data_sinks::TMultiCastDataStreamSink>,
                None::<crate::thrift::data_sinks::TSchemaTableSink>,
                None::<crate::thrift::data_sinks::TIcebergTableSink>,
                None::<crate::thrift::data_sinks::THiveTableSink>,
                None::<crate::thrift::data_sinks::TTableFunctionTableSink>,
                None::<crate::thrift::data_sinks::TDictionaryCacheSink>,
                None::<Vec<Box<crate::thrift::data_sinks::TDataSink>>>,
                None::<i64>,
                None::<crate::thrift::data_sinks::TSplitDataStreamSink>,
                None::<crate::thrift::data_sinks::TIcebergChangeStreamRouterSink>,
            ),
            output_exprs: None,
            output_columns: Vec::new(),
            boundary_schemas: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
            query_global_dicts: None,
            query_global_dict_exprs: Some(std::collections::BTreeMap::from([(
                99,
                crate::thrift::exprs::TExpr::new(Vec::new()),
            )])),
        }
    }

    fn keyed_assert_build_result(
        nodes: Vec<crate::thrift::plan_nodes::TPlanNode>,
    ) -> crate::sql::codegen::MultiFragmentBuildResult {
        let fragment_results = vec![keyed_assert_fragment(nodes)];
        let fragment_schedules = fragment_results
            .iter()
            .map(crate::sql::codegen::FragmentBuildResult::scheduling_metadata)
            .collect();
        crate::sql::codegen::MultiFragmentBuildResult {
            fragment_results,
            fragment_schedules,
            root_fragment_id: 0,
            edges: Vec::new(),
            lowered_edges: Vec::new(),
            boundary_schemas: Vec::new(),
            rf_plan: None,
        }
    }

    fn assert_plan_node_ids_follow_preorder_for_test(plan: &crate::thrift::plan_nodes::TPlan) {
        fn assert_subtree(
            nodes: &[crate::thrift::plan_nodes::TPlanNode],
            root_idx: usize,
        ) -> usize {
            let root = &nodes[root_idx];
            let mut next_idx = root_idx + 1;
            let mut previous_child_root_id = None;
            for child_ordinal in 0..root.num_children {
                let child = nodes.get(next_idx).unwrap_or_else(|| {
                    panic!("missing child {child_ordinal} for node {}", root.node_id)
                });
                assert!(
                    root.node_id > child.node_id,
                    "parent node id {} must be greater than child root id {}",
                    root.node_id,
                    child.node_id
                );
                if let Some(previous_child_root_id) = previous_child_root_id {
                    assert!(
                        previous_child_root_id < child.node_id,
                        "sibling child root ids must increase: {} before {}",
                        previous_child_root_id,
                        child.node_id
                    );
                }
                previous_child_root_id = Some(child.node_id);
                next_idx = assert_subtree(nodes, next_idx);
            }
            next_idx
        }

        assert_eq!(
            assert_subtree(&plan.nodes, 0),
            plan.nodes.len(),
            "TPlan must contain exactly one pre-order tree"
        );
    }

    #[test]
    fn execution_return_type_carries_commit_plan() {
        let execution = DmlChangeStreamWriteExecution {
            result: CoordinatedQueryResult {
                query_result: crate::runtime::query_result::QueryResult::empty(),
                write_commit: Some(crate::runtime::write_coordinator::WriteCommitInput {
                    write_id: crate::thrift::types::TUniqueId::new(1, 2),
                    writers: Vec::new(),
                }),
                write_abort: None,
                fragment_profiles: Vec::new(),
            },
            commit_plan:
                crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan::new(
                    BTreeMap::new(),
                ),
        };

        assert!(execution.result.write_commit.is_some());
        assert!(execution.commit_plan.is_empty());
    }

    fn empty_writer_commit_for_test() -> crate::runtime::write_coordinator::WriteCommitInput {
        crate::runtime::write_coordinator::WriteCommitInput {
            write_id: crate::thrift::types::TUniqueId::new(1, 2),
            writers: Vec::new(),
        }
    }

    fn empty_writer_result_for_test() -> CoordinatedQueryResult {
        CoordinatedQueryResult {
            query_result: crate::runtime::query_result::QueryResult::empty(),
            write_commit: Some(empty_writer_commit_for_test()),
            write_abort: None,
            fragment_profiles: Vec::new(),
        }
    }

    fn commit_plan_for_branches(
        branches: &[(i32, ChangeStreamBranchKind)],
    ) -> crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan {
        crate::engine::iceberg_change_stream_write::ChangeStreamWriterCommitPlan::new(
            branches.iter().copied().collect(),
        )
    }

    #[test]
    fn update_mor_zero_rows_accepts_eos_without_branch_writer_reports() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("update MOR change-stream DAG");
        assert_eq!(
            branch_kinds(&dag),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ]
        );

        let execution = dml_change_stream_write_execution(
            empty_writer_result_for_test(),
            commit_plan_for_branches(&[
                (10, ChangeStreamBranchKind::DeleteDv),
                (11, ChangeStreamBranchKind::ReuseData),
            ]),
        )
        .expect("zero-row UPDATE should complete with query-level EOS");

        assert_eq!(
            execution.result.write_commit.expect("commit").writers.len(),
            0
        );
    }

    #[test]
    fn merge_matched_update_zero_rows_accepts_eos_without_branch_writer_reports() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: true,
                matched_delete: false,
                not_matched_insert: false,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("matched update DAG");
        assert_eq!(
            branch_kinds(&dag),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ]
        );

        let execution = dml_change_stream_write_execution(
            empty_writer_result_for_test(),
            commit_plan_for_branches(&[
                (10, ChangeStreamBranchKind::DeleteDv),
                (11, ChangeStreamBranchKind::ReuseData),
            ]),
        )
        .expect("zero-row MERGE matched UPDATE should complete with query-level EOS");

        assert_eq!(
            execution.result.write_commit.expect("commit").writers.len(),
            0
        );
    }

    #[test]
    fn merge_empty_not_matched_insert_commits_no_writer_files() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: false,
                matched_delete: false,
                not_matched_insert: true,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("not matched insert DAG");
        assert_eq!(branch_kinds(&dag), vec![ChangeStreamBranchKind::FreshData]);

        let execution = dml_change_stream_write_execution(
            empty_writer_result_for_test(),
            commit_plan_for_branches(&[(12, ChangeStreamBranchKind::FreshData)]),
        )
        .expect("empty MERGE not-matched INSERT should not require writer files");

        assert_eq!(
            execution.result.write_commit.expect("commit").writers.len(),
            0
        );
    }

    #[test]
    fn pre_expand_keyed_assert_inserts_assert_num_rows_before_expand() {
        let mut build_result = keyed_assert_build_result(vec![
            change_event_expand_plan_node(20),
            exchange_plan_node(10, 1),
        ]);
        build_result.fragment_results[0].plan.nodes[1].local_rf_waiting_set =
            Some(std::collections::BTreeSet::from([20]));
        build_result.fragment_results[0]
            .native_scan_ranges
            .insert(20, Vec::new());

        inject_dml_pre_expand_keyed_assert(&mut build_result, Some(&keyed_assert_for_test()))
            .expect("inject assert");

        let nodes = &build_result.fragment_results[0].plan.nodes;
        assert_plan_node_ids_follow_preorder_for_test(&build_result.fragment_results[0].plan);
        assert_eq!(
            nodes[0].node_type,
            crate::thrift::plan_nodes::TPlanNodeType::CHANGE_EVENT_EXPAND_NODE
        );
        assert_eq!(
            nodes[1].node_type,
            crate::thrift::plan_nodes::TPlanNodeType::ASSERT_NUM_ROWS_NODE
        );
        assert_eq!(
            nodes[2].node_type,
            crate::thrift::plan_nodes::TPlanNodeType::EXCHANGE_NODE
        );
        let assert_payload = nodes[1]
            .assert_num_rows_node
            .as_ref()
            .expect("assert payload");
        assert_eq!(
            nodes[2]
                .local_rf_waiting_set
                .as_ref()
                .map(|set| set.iter().copied().collect::<Vec<_>>()),
            Some(vec![nodes[0].node_id])
        );
        assert!(
            build_result.fragment_results[0]
                .native_scan_ranges
                .contains_key(&nodes[0].node_id),
            "native scan range map must be remapped with TPlan node ids"
        );
        assert!(
            !build_result.fragment_results[0]
                .native_scan_ranges
                .contains_key(&20),
            "native scan range map must not retain stale TPlan node ids"
        );
        let schedule_scan_ranges = build_result.fragment_schedules[0]
            .native_scan_ranges
            .iter()
            .map(|(node_id, ranges)| (*node_id, ranges.len()))
            .collect::<Vec<_>>();
        let fragment_scan_ranges = build_result.fragment_results[0]
            .native_scan_ranges
            .iter()
            .map(|(node_id, ranges)| (*node_id, ranges.len()))
            .collect::<Vec<_>>();
        assert_eq!(
            schedule_scan_ranges, fragment_scan_ranges,
            "fragment schedule native scan ranges must be refreshed after remapping TPlan node ids"
        );
        assert!(
            build_result.fragment_results[0]
                .query_global_dict_exprs
                .as_ref()
                .expect("dict exprs")
                .contains_key(&99)
        );
        assert_eq!(assert_payload.desired_num_rows, Some(1));
        assert_eq!(
            assert_payload.assertion,
            Some(crate::thrift::plan_nodes::TAssertion::LE)
        );
        assert_eq!(assert_payload.group_key_slots.as_deref(), Some(&[7][..]));
        assert_eq!(
            assert_payload.group_key_labels.as_deref(),
            Some(&["_row_id".to_string()][..])
        );
        assert_eq!(
            assert_payload.keyed_message_prefix.as_deref(),
            Some("MOR UPDATE matched target row")
        );
    }

    #[test]
    fn pre_expand_keyed_assert_rejects_multiple_expands_in_one_fragment() {
        let mut build_result = keyed_assert_build_result(vec![
            change_event_expand_plan_node(40),
            exchange_plan_node(30, 1),
            change_event_expand_plan_node(20),
            exchange_plan_node(10, 1),
        ]);

        let err =
            inject_dml_pre_expand_keyed_assert(&mut build_result, Some(&keyed_assert_for_test()))
                .expect_err("multiple ChangeEventExpand nodes must fail");

        assert!(
            err.contains("exactly one ChangeEventExpand"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn pre_expand_keyed_assert_rejects_unknown_local_rf_waiting_node_id() {
        let mut build_result = keyed_assert_build_result(vec![
            change_event_expand_plan_node(20),
            exchange_plan_node(10, 1),
        ]);
        build_result.fragment_results[0].plan.nodes[1].local_rf_waiting_set =
            Some(std::collections::BTreeSet::from([999]));

        let err =
            inject_dml_pre_expand_keyed_assert(&mut build_result, Some(&keyed_assert_for_test()))
                .expect_err("unknown local runtime-filter dependency node id must fail");

        assert!(
            err.contains("cannot remap unknown TPlan node id 999"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn execute_dml_change_stream_write_applies_keyed_assert_before_observer() {
        let _test_guard = crate::engine::acquire_standalone_test_guard();
        let _observer = crate::engine::install_change_stream_write_test_observer(true);
        let state = Arc::new(StandaloneState::default());
        let mut plan = execution_test_plan();
        plan.pre_expand_keyed_assert = Some(keyed_assert_for_test());

        let err = execute_dml_change_stream_write(&state, &target_for_execution_test(), plan, None)
            .expect_err("assert-bearing plan must be processed before the observer short-circuit");

        assert!(
            err.contains("requires exactly one native ChangeEventExpand node, found 0"),
            "unexpected error: {err}"
        );
    }

    fn native_change_event_expand_plan_for_test() -> crate::sql::planner::DistributedPlan {
        use crate::sql::planner::plan::{
            DistributedChangeEventExpandNode, PhysicalPlanKind, PlanValuesNode,
        };
        use crate::sql::planner::{
            DataPartition, DataSink, DistributedNode, DistributedPayload, PhysicalPlanStats,
            PlanFragment, PlannerConfidence,
        };

        let child = DistributedNode {
            node_id: 1,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
            children: Vec::new(),
            stats: PhysicalPlanStats {
                output_row_count: 1.0,
                row_count_confidence: PlannerConfidence::Exact,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedPayload::Physical(PhysicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![crate::sql::analysis::OutputColumn {
                    column_id: crate::sql::column_id::ColumnId::new_for_test(1),
                    name: "__nr_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    is_internal: true,
                }],
            })),
        };
        let expand = DistributedNode {
            node_id: 2,
            fragment_id: 0,
            tuple_ids: vec![1],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
            children: vec![child],
            stats: PhysicalPlanStats {
                output_row_count: 1.0,
                row_count_confidence: PlannerConfidence::Exact,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedPayload::Physical(PhysicalPlanKind::ChangeEventExpand(
                DistributedChangeEventExpandNode {
                    events: Vec::new(),
                    output_columns: vec![crate::sql::analysis::OutputColumn {
                        column_id: crate::sql::column_id::ColumnId::new_for_test(1),
                        name: "__nr_row_id".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        is_internal: true,
                    }],
                    change_op_column_id: crate::sql::column_id::ColumnId::new_for_test(2),
                    data_route_column_id: Some(crate::sql::column_id::ColumnId::new_for_test(3)),
                },
            )),
        };
        crate::sql::planner::DistributedPlan {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: expand,
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: Vec::new(),
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            edges: Vec::new(),
        }
    }

    fn native_change_event_expand_router_plan_for_test() -> crate::sql::planner::DistributedPlan {
        let mut plan = native_change_event_expand_plan_for_test();
        let writer_exchange = crate::sql::planner::DistributedNode {
            node_id: 30,
            fragment_id: 1,
            tuple_ids: vec![30],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
            children: Vec::new(),
            stats: crate::sql::planner::PhysicalPlanStats {
                output_row_count: 1.0,
                row_count_confidence: crate::sql::planner::PlannerConfidence::Exact,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: crate::sql::planner::DistributedPayload::Exchange(
                crate::sql::planner::ExchangeReceiver {
                    partition: crate::sql::planner::DataPartition::unpartitioned(),
                    source_fragment_id: 0,
                    output_columns: vec![crate::sql::analysis::OutputColumn {
                        column_id: crate::sql::column_id::ColumnId::new_for_test(1),
                        name: "id".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    }],
                    output_qualifier: None,
                    flavor: crate::sql::planner::plan::ExchangeFlavor::Distribution,
                },
            ),
        };
        plan.fragments.push(crate::sql::planner::PlanFragment {
            fragment_id: 1,
            root: writer_exchange,
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::IcebergWrite(
                crate::sql::planner::IcebergWriteFragmentSink {
                    descriptor_database: "default".to_string(),
                    spec: crate::sql::planner::write_sink::test_support::simple_sink_spec(),
                    input: crate::sql::planner::IcebergWriteInputBinding::RootOutputByOrdinal,
                },
            ),
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        });
        plan.edges.push(crate::sql::codegen::FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 30,
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            stream_kind: crate::sql::codegen::FragmentStreamKind::Gather,
            edge_kind: crate::sql::codegen::FragmentEdgeKind::IcebergChangeStreamRouter {
                router_group_id: 0,
                branch_id: 0,
                branch_kind: ChangeStreamBranchKind::ReuseData,
            },
            output_slot_ids: Vec::new(),
        });
        plan
    }

    fn native_plan_has_exchange_node(
        plan: &crate::sql::planner::DistributedPlan,
        fragment_id: crate::sql::codegen::FragmentId,
        node_id: i32,
    ) -> bool {
        fn node_has_exchange(node: &crate::sql::planner::DistributedNode, node_id: i32) -> bool {
            node.node_id == node_id
                && matches!(
                    node.payload,
                    crate::sql::planner::DistributedPayload::Exchange(_)
                )
                || node
                    .children
                    .iter()
                    .any(|child| node_has_exchange(child, node_id))
        }

        plan.fragments
            .iter()
            .find(|fragment| fragment.fragment_id == fragment_id)
            .is_some_and(|fragment| node_has_exchange(&fragment.root, node_id))
    }

    #[test]
    fn pre_expand_keyed_assert_wraps_native_change_event_expand_child() {
        let mut plan = native_change_event_expand_plan_for_test();
        inject_dml_pre_expand_keyed_assert_into_native_plan(&mut plan, &keyed_assert_for_test())
            .expect("inject native keyed assert");

        let root = &plan.fragments[0].root;
        let crate::sql::planner::DistributedPayload::Physical(
            crate::sql::planner::plan::PhysicalPlanKind::ChangeEventExpand(_),
        ) = &root.payload
        else {
            panic!("expected native ChangeEventExpand root");
        };
        let assert_node = &root.children[0];
        let crate::sql::planner::DistributedPayload::Physical(
            crate::sql::planner::plan::PhysicalPlanKind::AssertOneRow(assert),
        ) = &assert_node.payload
        else {
            panic!("expected native AssertOneRow below ChangeEventExpand");
        };
        assert_eq!(
            assert.group_key_column_ids,
            vec![crate::sql::column_id::ColumnId::new_for_test(1)]
        );
        assert_eq!(assert.group_key_labels, vec!["_row_id".to_string()]);
        assert_eq!(
            assert.keyed_message_prefix.as_deref(),
            Some("MOR UPDATE matched target row")
        );
        assert!(matches!(
            assert_node.children[0].payload,
            crate::sql::planner::DistributedPayload::Physical(
                crate::sql::planner::plan::PhysicalPlanKind::Values(_)
            )
        ));
    }

    #[test]
    fn pre_expand_keyed_assert_keeps_native_router_edge_node_ids_in_sync() {
        let mut build_result = keyed_assert_build_result(vec![
            change_event_expand_plan_node(20),
            exchange_plan_node(10, 1),
        ]);
        build_result
            .fragment_results
            .push(keyed_assert_fragment(vec![exchange_plan_node(30, 30)]));
        let edge = crate::sql::codegen::FragmentEdge {
            source_fragment_id: 0,
            target_fragment_id: 1,
            target_exchange_node_id: 30,
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            stream_kind: crate::sql::codegen::FragmentStreamKind::Gather,
            edge_kind: crate::sql::codegen::FragmentEdgeKind::IcebergChangeStreamRouter {
                router_group_id: 0,
                branch_id: 0,
                branch_kind: ChangeStreamBranchKind::ReuseData,
            },
            output_slot_ids: Vec::new(),
        };
        build_result.edges.push(edge.clone());
        build_result
            .lowered_edges
            .push(crate::sql::codegen::LoweredFragmentEdge {
                edge,
                compat_partition: crate::thrift::partitions::TDataPartition::new(
                    crate::thrift::partitions::TPartitionType::UNPARTITIONED,
                    None::<Vec<crate::thrift::exprs::TExpr>>,
                    None::<Vec<crate::thrift::partitions::TRangePartition>>,
                    None::<Vec<crate::thrift::partitions::TBucketProperty>>,
                ),
            });
        let mut native_plan = native_change_event_expand_router_plan_for_test();

        inject_dml_pre_expand_keyed_assert(&mut build_result, Some(&keyed_assert_for_test()))
            .expect("inject thrift keyed assert");
        inject_dml_pre_expand_keyed_assert_into_native_plan(
            &mut native_plan,
            &keyed_assert_for_test(),
        )
        .expect("inject native keyed assert");

        for edge in &build_result.edges {
            assert!(
                native_plan_has_exchange_node(
                    &native_plan,
                    edge.target_fragment_id,
                    edge.target_exchange_node_id,
                ),
                "native sidecar missing exchange node {} for fragment {}",
                edge.target_exchange_node_id,
                edge.target_fragment_id
            );
        }
        assert_eq!(build_result.edges.len(), 1);
        assert_eq!(build_result.lowered_edges.len(), 1);
        assert_eq!(
            build_result.edges[0].target_exchange_node_id,
            build_result.lowered_edges[0].edge.target_exchange_node_id,
            "native edge and lowered sidecar edge must stay keyed by the same exchange node"
        );
    }

    #[test]
    fn execute_dml_change_stream_write_rejects_missing_writer_commit() {
        let _test_guard = crate::engine::acquire_standalone_test_guard();
        let _observer = crate::engine::install_change_stream_write_test_observer(true);
        let state = Arc::new(StandaloneState::default());

        let err = execute_dml_change_stream_write(
            &state,
            &target_for_execution_test(),
            execution_test_plan(),
            None,
        )
        .expect_err("missing writer commit must fail");

        assert!(err.contains("DML change-stream write completed without writer commit"));
    }

    #[test]
    fn update_mor_change_stream_plan_declares_delete_and_reuse_branches() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("update MOR change-stream DAG");

        assert_eq!(
            branch_kinds(&dag),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ]
        );
        assert_eq!(dag.change_op_output_ordinal, Some(6));
        assert_eq!(dag.data_route_output_ordinal, Some(7));

        let delete_dv = dag
            .branches
            .iter()
            .find(|branch| branch.branch_kind == ChangeStreamBranchKind::DeleteDv)
            .expect("delete branch");
        assert_eq!(delete_dv.output_partition_ordinals.as_slice(), &[0][..]);

        let reuse_data = dag
            .branches
            .iter()
            .find(|branch| branch.branch_kind == ChangeStreamBranchKind::ReuseData)
            .expect("reuse branch");
        assert_eq!(reuse_data.output_partition_ordinals.as_slice(), &[2][..]);
    }

    #[test]
    fn merge_change_stream_plan_declares_only_reachable_branches() {
        let output_columns = producer_output_columns();

        let matched_delete = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: false,
                matched_delete: true,
                not_matched_insert: false,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("matched delete DAG");
        assert_eq!(
            branch_kinds(&matched_delete),
            vec![ChangeStreamBranchKind::DeleteDv]
        );

        let matched_update = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: true,
                matched_delete: false,
                not_matched_insert: false,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("matched update DAG");
        assert_eq!(
            branch_kinds(&matched_update),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ]
        );

        let insert_only = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: false,
                matched_delete: false,
                not_matched_insert: true,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("not matched insert DAG");
        assert_eq!(
            branch_kinds(&insert_only),
            vec![ChangeStreamBranchKind::FreshData]
        );

        let update_and_insert = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: true,
                matched_delete: false,
                not_matched_insert: true,
            },
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect("matched update plus not matched insert DAG");
        assert_eq!(
            branch_kinds(&update_and_insert),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
                ChangeStreamBranchKind::FreshData,
            ]
        );
    }

    #[test]
    fn unpartitioned_data_branch_has_empty_partition_ordinals() {
        let output_columns = producer_output_columns();
        let dag = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::Merge {
                matched_update: false,
                matched_delete: false,
                not_matched_insert: true,
            },
            &output_columns,
            sink_specs_for_unpartitioned_target(),
        )
        .expect("unpartitioned insert-only DAG");

        let fresh_data = dag
            .branches
            .iter()
            .find(|branch| branch.branch_kind == ChangeStreamBranchKind::FreshData)
            .expect("fresh branch");
        assert_eq!(
            fresh_data.output_partition_ordinals.as_slice(),
            &[] as &[usize]
        );
    }

    #[test]
    fn data_branch_requires_data_route_output_column() {
        let output_columns = producer_output_columns()
            .into_iter()
            .filter(|column| column.name != DML_CHANGE_STREAM_DATA_ROUTE_COLUMN)
            .collect::<Vec<_>>();
        let err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &output_columns,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("missing data route column must fail");

        assert!(err.contains("data-route column"));
        assert!(err.contains(DML_CHANGE_STREAM_DATA_ROUTE_COLUMN));
    }

    #[test]
    fn internal_route_and_file_columns_must_be_marked_internal() {
        let mut route_outputs = producer_output_columns();
        route_outputs[7] =
            output_column_with_internal(DML_CHANGE_STREAM_DATA_ROUTE_COLUMN, 7, false);
        let route_err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &route_outputs,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("non-internal data route column must fail");
        assert!(route_err.contains("data-route column"));
        assert!(route_err.contains("must be marked internal"));

        let mut file_outputs = producer_output_columns();
        file_outputs[0] =
            output_column_with_internal(crate::exec::row_position::ICEBERG_FILE_PATH_COL, 0, false);
        let file_err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &file_outputs,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("non-internal file column must fail");
        assert!(file_err.contains("delete file column"));
        assert!(file_err.contains("must be marked internal"));
    }

    #[test]
    fn user_target_sink_columns_must_not_bind_internal_outputs() {
        let mut outputs = producer_output_columns();
        outputs[3] = output_column_with_internal("id", 3, true);
        let err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &outputs,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("internal user target column must fail");

        assert!(err.contains("sink input column"));
        assert!(err.contains("must be user-visible"));
    }

    #[test]
    fn ambiguous_output_name_fails_fast() {
        let mut outputs = producer_output_columns();
        outputs.push(output_column("region", 8));
        let err = build_dml_change_stream_dag_from_sink_specs(
            DmlChangeStreamBranchSet::UpdateMor,
            &outputs,
            sink_specs_for_partitioned_target(),
        )
        .expect_err("duplicate output name must fail");

        assert!(err.contains("target partition source column"));
        assert!(err.contains("ambiguous"));
    }
}
