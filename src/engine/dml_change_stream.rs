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
    let key_slot = find_slot_id_in_row_tuples(
        &fragment.desc_tbl,
        &child.row_tuples,
        &keyed_assert.key_column_name,
    )?;
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
    for boundary in &mut build_result.boundary_schemas {
        remap_boundary_schema_node_id(boundary, node_id_map)?;
    }
    if let Some(rf_plan) = build_result.rf_plan.as_mut() {
        for desc in rf_plan.all_filters.values_mut() {
            remap_runtime_filter_description(desc, node_id_map)?;
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
        query_opts.cloned(),
    )?;
    dml_change_stream_write_execution(result, commit_plan)
}

pub(crate) fn plan_dml_change_stream_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    plan: &mut DmlChangeStreamWritePlan,
) -> Result<crate::engine::PlannedIcebergChangeStreamWrite, String> {
    let planned = crate::engine::build_physical_plan_as_iceberg_change_stream_write(
        state,
        Some(&target.catalog),
        &target.namespace,
        &plan.producer,
        &mut plan.dag,
        None,
    )?;
    let crate::engine::PlannedIcebergChangeStreamWrite {
        mut build_result,
        commit_plan,
        #[cfg(test)]
        topology,
    } = planned;
    inject_dml_pre_expand_keyed_assert(&mut build_result, plan.pre_expand_keyed_assert.as_ref())?;
    Ok(crate::engine::PlannedIcebergChangeStreamWrite {
        build_result,
        commit_plan,
        #[cfg(test)]
        topology,
    })
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
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
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
        crate::sql::codegen::MultiFragmentBuildResult {
            fragment_results: vec![keyed_assert_fragment(nodes)],
            root_fragment_id: 0,
            edges: Vec::new(),
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
            err.contains("requires exactly one ChangeEventExpand node, found 0"),
            "unexpected error: {err}"
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
