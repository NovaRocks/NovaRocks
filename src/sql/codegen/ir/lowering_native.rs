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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::datatypes::DataType;

use crate::connector::scan_planning::{BeginScanContext, SplitPlanningContext};
use crate::runtime::scan_range;
use crate::sql::analysis::OutputColumn as AnalysisOutputColumn;
use crate::sql::catalog::{IcebergDataFileBinding, ScanSource, TableDef};
use crate::sql::codegen::boundary_schema::{
    BoundaryKind, BoundarySchemaReport, output_columns_to_boundary_columns,
};
use crate::sql::codegen::connector_scan_wire::{ThriftScanContext, to_native_file_scan};
use crate::sql::codegen::{
    FragmentBuildResult, FragmentEdgeKind, FragmentId, FragmentOutputKind,
    FragmentSchedulingMetadata, FragmentStreamKind, MultiFragmentBuildResult, OutputColumn,
    RuntimeFilterPlanResult,
};
use crate::sql::planner::plan::{ExchangeFlavor, PhysicalPlanKind, PlanScanNode};
use crate::sql::planner::{
    DataPartition, DistributedNode, DistributedPayload, DistributedPlan, PartitionKind,
    PlanFragment, PlannedRuntimeFilter,
};

pub(crate) fn lower_distributed_plan(
    dp: &DistributedPlan,
    catalog: &dyn crate::sql::catalog::CatalogProvider,
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<MultiFragmentBuildResult, String> {
    let _ = catalog;
    validate_distributed_plan(dp)?;

    let mut refreshed = refresh_distributed_plan_for_native_sidecar(dp, mv_refresh_ctx)?;
    lower_native_cte_multicast_edge_output_slot_ids(&mut refreshed)?;
    let native_scan_ranges = build_native_scan_ranges(&refreshed, connectors, mv_refresh_ctx)?;

    let mut fragment_results = Vec::with_capacity(refreshed.fragments.len());
    let mut fragment_schedules = Vec::with_capacity(refreshed.fragments.len());
    for fragment in &refreshed.fragments {
        let output_columns = fragment
            .output_columns
            .iter()
            .map(output_column_for_boundary)
            .collect::<Vec<_>>();
        let boundary_schemas = vec![result_root_boundary_schema_report(
            fragment.fragment_id,
            fragment.root.node_id,
            &output_columns,
        )];
        let has_scan_nodes = distributed_node_has_scan(&fragment.root);
        let output_kind = fragment_output_kind(&fragment.sink);
        let native_scan_ranges = native_scan_ranges
            .get(&fragment.fragment_id)
            .cloned()
            .unwrap_or_default();

        let result = FragmentBuildResult {
            fragment_id: fragment.fragment_id,
            has_scan_nodes,
            output_kind,
            native_scan_ranges: native_scan_ranges.clone(),
            output_columns: output_columns.clone(),
            boundary_schemas: boundary_schemas.clone(),
            cte_id: fragment.cte_id,
            cte_exchange_nodes: fragment.cte_exchange_nodes.clone(),
        };
        fragment_schedules.push(FragmentSchedulingMetadata {
            fragment_id: fragment.fragment_id,
            has_scan_nodes,
            output_kind,
            native_scan_ranges,
            output_columns,
            boundary_schemas,
            cte_id: fragment.cte_id,
            cte_exchange_nodes: fragment.cte_exchange_nodes.clone(),
        });
        fragment_results.push(result);
    }

    let mut boundary_schemas = fragment_results
        .iter()
        .flat_map(|fragment| fragment.boundary_schemas.clone())
        .collect::<Vec<_>>();
    boundary_schemas.extend(edge_boundary_schemas(&refreshed)?);

    Ok(MultiFragmentBuildResult {
        fragment_results,
        fragment_schedules,
        root_fragment_id: refreshed.root_fragment_id,
        edges: refreshed.edges.clone(),
        boundary_schemas,
        rf_plan: runtime_filter_plan(&refreshed),
    })
}

pub(crate) fn refresh_distributed_plan_for_native_sidecar(
    dp: &DistributedPlan,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<DistributedPlan, String> {
    let mut out = dp.clone();
    for fragment in &mut out.fragments {
        refresh_distributed_node_scan_tables_for_native(&mut fragment.root, mv_refresh_ctx)?;
    }
    Ok(out)
}

fn lower_native_cte_multicast_edge_output_slot_ids(dp: &mut DistributedPlan) -> Result<(), String> {
    let fragments_by_id: BTreeMap<FragmentId, &PlanFragment> = dp
        .fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect();
    for edge in &mut dp.edges {
        let FragmentEdgeKind::CteMulticast {
            cte_id,
            receive_producer_column_ids,
        } = &edge.edge_kind
        else {
            continue;
        };
        let exchange = target_exchange_for_edge(&fragments_by_id, edge)?;
        if receive_producer_column_ids.len() != exchange.output_columns.len() {
            return Err(format!(
                "lower_distributed_plan CTE multicast receive/output arity mismatch for cte_id={}",
                cte_id
            ));
        }
        edge.output_slot_ids = receive_producer_column_ids
            .iter()
            .map(|column_id| {
                i32::try_from(column_id.0).map_err(|_| {
                    format!(
                        "native CTE multicast producer column {} cannot convert to output slot id",
                        column_id.0
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
    }
    Ok(())
}

fn refresh_distributed_node_scan_tables_for_native(
    node: &mut DistributedNode,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<(), String> {
    if let DistributedPayload::Physical(PhysicalPlanKind::Scan(scan)) = &mut node.payload {
        let refresh_only_source = is_refresh_only_scan_source(&scan.table.source);
        let native_projected_names = native_refresh_scan_projected_names(&scan.table.source);
        let refreshed_table = refresh_scan_table_for_codegen(mv_refresh_ctx, &scan.table)?;
        if let Some(projected_names) = native_projected_names {
            scan.required_columns = Some(merge_required_columns_with_projected(
                scan.required_columns.take(),
                &projected_names,
            ));
        } else if refresh_only_source {
            scan.columns = scan_output_columns_for_refreshed_table(scan, &refreshed_table);
        }
        scan.table = refreshed_table;
    }
    for child in &mut node.children {
        refresh_distributed_node_scan_tables_for_native(child, mv_refresh_ctx)?;
    }
    Ok(())
}

fn is_refresh_only_scan_source(source: &ScanSource) -> bool {
    matches!(
        source,
        ScanSource::IcebergVersionTable { .. }
            | ScanSource::IcebergMvTargetState(_)
            | ScanSource::IcebergMvTargetLocator(_)
    )
}

fn native_refresh_scan_projected_names(source: &ScanSource) -> Option<Vec<String>> {
    match source {
        ScanSource::IcebergMvTargetState(scan) => Some(projected_target_state_column_names(scan)),
        ScanSource::IcebergMvTargetLocator(scan) => {
            Some(projected_target_locator_column_names(scan))
        }
        _ => None,
    }
}

fn scan_output_columns_for_refreshed_table(
    scan: &PlanScanNode,
    table: &TableDef,
) -> Vec<AnalysisOutputColumn> {
    let mut out = Vec::new();
    for column in table
        .columns
        .iter()
        .chain(table.iceberg_row_lineage_metadata_columns.iter())
    {
        if let Some(output_column) = scan
            .columns
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(&column.name))
        {
            out.push(output_column.clone());
        }
    }
    for variant_column in &scan.variant_columns {
        if let Some(output_column) = scan
            .columns
            .iter()
            .find(|column| column.column_id == variant_column.synthetic_column_id)
        {
            out.push(output_column.clone());
        }
    }
    out
}

fn merge_required_columns_with_projected(
    existing: Option<Vec<String>>,
    projected_names: &[String],
) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for name in projected_names
        .iter()
        .cloned()
        .chain(existing.unwrap_or_default())
    {
        if seen.insert(name.to_lowercase()) {
            out.push(name);
        }
    }
    out
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
            let projected = projected_target_state_column_names(scan);
            retain_projected_iceberg_columns(&mut out, &projected);
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
                DataType::Int64,
                true,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
                false,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
                false,
            );
            reorder_refresh_table_columns_by_projected_names(&mut out, &projected)?;
            out.source = refresh_ctx.target_state_scan_source(scan)?;
            reject_target_state_equality_deletes(&out.source)?;
            Ok(out)
        }
        ScanSource::IcebergMvTargetLocator(scan) => {
            let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "Iceberg target-locator scan requires MV refresh context".to_string()
            })?;
            let mut out = table.clone();
            let projected = projected_target_locator_column_names(scan);
            retain_projected_iceberg_columns(&mut out, &projected);
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
                DataType::Int64,
                true,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
                false,
            );
            ensure_iceberg_metadata_column(
                &mut out,
                &projected,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
                false,
            );
            reorder_refresh_table_columns_by_projected_names(&mut out, &projected)?;
            out.source = refresh_ctx.target_locator_scan_source(scan)?;
            reject_target_state_equality_deletes(&out.source)?;
            Ok(out)
        }
        _ => Ok(table.clone()),
    }
}

fn retain_projected_iceberg_columns(table: &mut TableDef, projected: &[String]) {
    table.columns.retain(|column| {
        projected
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&column.name))
    });
    table.iceberg_row_lineage_metadata_columns.retain(|column| {
        projected
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&column.name))
    });
}

fn ensure_iceberg_metadata_column(
    table: &mut TableDef,
    projected: &[String],
    name: &str,
    data_type: DataType,
    nullable: bool,
) {
    if !projected
        .iter()
        .any(|projected_name| projected_name.eq_ignore_ascii_case(name))
    {
        return;
    }
    if table
        .columns
        .iter()
        .chain(table.iceberg_row_lineage_metadata_columns.iter())
        .any(|column| column.name.eq_ignore_ascii_case(name))
    {
        return;
    }
    table
        .iceberg_row_lineage_metadata_columns
        .push(crate::sql::catalog::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        });
}

fn reorder_refresh_table_columns_by_projected_names(
    table: &mut TableDef,
    projected: &[String],
) -> Result<(), String> {
    let physical = table.columns.clone();
    let metadata = table.iceberg_row_lineage_metadata_columns.clone();
    let mut next_physical = Vec::new();
    let mut next_metadata = Vec::new();
    let mut seen = BTreeSet::new();

    for name in projected {
        let key = name.to_lowercase();
        if !seen.insert(key) {
            continue;
        }
        if let Some(column) = physical
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
        {
            next_physical.push(column.clone());
            continue;
        }
        if let Some(column) = metadata
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
        {
            next_metadata.push(column.clone());
            continue;
        }
        return Err(format!(
            "refresh-only scan table `{}` cannot resolve projected column `{}`",
            table.name, name
        ));
    }

    table.columns = next_physical;
    table.iceberg_row_lineage_metadata_columns = next_metadata;
    Ok(())
}

fn build_native_scan_ranges(
    dp: &DistributedPlan,
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<BTreeMap<FragmentId, BTreeMap<i32, Vec<scan_range::ScanRangeParams>>>, String> {
    let mut out = BTreeMap::new();
    for fragment in &dp.fragments {
        let mut per_node = BTreeMap::new();
        collect_native_scan_ranges(
            fragment.fragment_id,
            &fragment.root,
            connectors,
            mv_refresh_ctx,
            &mut per_node,
        )?;
        out.insert(fragment.fragment_id, per_node);
    }
    Ok(out)
}

fn collect_native_scan_ranges(
    fragment_id: FragmentId,
    node: &DistributedNode,
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    out: &mut BTreeMap<i32, Vec<scan_range::ScanRangeParams>>,
) -> Result<(), String> {
    if let DistributedPayload::Physical(PhysicalPlanKind::Scan(scan)) = &node.payload {
        let ranges = native_scan_ranges_for_scan(node.node_id, scan, connectors, mv_refresh_ctx)?;
        out.insert(node.node_id, ranges);
    }
    for child in &node.children {
        if child.fragment_id == fragment_id {
            collect_native_scan_ranges(fragment_id, child, connectors, mv_refresh_ctx, out)?;
        }
    }
    Ok(())
}

fn native_scan_ranges_for_scan(
    scan_node_id: i32,
    scan: &PlanScanNode,
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<Vec<scan_range::ScanRangeParams>, String> {
    match &scan.table.source {
        ScanSource::StarRocks { .. } => {
            Err("StarRocks scan ranges require feature compat".to_string())
        }
        ScanSource::IcebergDataFiles { .. } => {
            build_iceberg_scan_ranges_from_source(scan_node_id, scan, &scan.table.source, None)
                .and_then(|handle| plan_iceberg_scan_ranges(connectors, scan_node_id, scan, handle))
        }
        ScanSource::IcebergMetadataTable { .. } | ScanSource::IcebergDeltaTable { .. } => {
            Ok(vec![build_iceberg_metadata_scan_range_params()])
        }
        ScanSource::IcebergVersionTable { table, snapshot_id } => {
            let refresh_ctx = mv_refresh_ctx
                .ok_or_else(|| "Iceberg version scan requires MV refresh context".to_string())?;
            let source = refresh_ctx.version_scan_source(table, *snapshot_id)?;
            let handle = build_iceberg_scan_ranges_from_source(scan_node_id, scan, &source, None)?;
            plan_iceberg_scan_ranges(connectors, scan_node_id, scan, handle)
        }
        ScanSource::IcebergMvTargetState(target_scan) => {
            let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "Iceberg target-state scan requires MV refresh context".to_string()
            })?;
            let source = refresh_ctx.target_state_scan_source(target_scan)?;
            reject_target_state_equality_deletes(&source)?;
            let handle = build_iceberg_scan_ranges_from_source(
                scan_node_id,
                scan,
                &source,
                Some(projected_target_state_column_names(target_scan)),
            )?;
            plan_iceberg_scan_ranges(connectors, scan_node_id, scan, handle)
        }
        ScanSource::IcebergMvTargetLocator(target_scan) => {
            let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                "Iceberg target-locator scan requires MV refresh context".to_string()
            })?;
            let source = refresh_ctx.target_locator_scan_source(target_scan)?;
            reject_target_state_equality_deletes(&source)?;
            let handle = build_iceberg_scan_ranges_from_source(
                scan_node_id,
                scan,
                &source,
                Some(projected_target_locator_column_names(target_scan)),
            )?;
            plan_iceberg_scan_ranges(connectors, scan_node_id, scan, handle)
        }
    }
}

fn build_iceberg_scan_ranges_from_source(
    scan_node_id: i32,
    scan: &PlanScanNode,
    source: &ScanSource,
    column_names: Option<Vec<String>>,
) -> Result<crate::connector::scan_planning::TableHandle, String> {
    let ScanSource::IcebergDataFiles {
        table,
        files,
        binding,
        ..
    } = source
    else {
        return Err("refresh-only scan source did not resolve to Iceberg data files".to_string());
    };
    let column_names = column_names.unwrap_or_else(|| effective_scan_column_names(scan));
    let handle = match binding {
        IcebergDataFileBinding::ExplicitFiles => {
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
                &table.catalog,
                &table.namespace,
                &table.table,
                table.current_snapshot_id,
                table.clone(),
                files.clone(),
                column_names,
            )
        }
        IcebergDataFileBinding::CurrentSnapshot => {
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
                &table.catalog,
                &table.namespace,
                &table.table,
                table.clone(),
                column_names,
            )
        }
    };
    let _ = scan_node_id;
    Ok(handle)
}

fn plan_iceberg_scan_ranges(
    connectors: &crate::connector::ConnectorRegistry,
    scan_node_id: i32,
    scan: &PlanScanNode,
    table_handle: crate::connector::scan_planning::TableHandle,
) -> Result<Vec<scan_range::ScanRangeParams>, String> {
    let ScanSource::IcebergDataFiles {
        cloud_properties, ..
    } = &scan.table.source
    else {
        return Err("Iceberg scan range source must be Iceberg data files".to_string());
    };
    let planner = connectors.scan_planner("iceberg")?;
    let scan_handle = planner.begin_scan(table_handle, BeginScanContext::default())?;
    let splits = planner.plan_splits(&scan_handle, SplitPlanningContext::default())?;
    let plan = to_native_file_scan(
        planner.name(),
        &scan_handle,
        &splits,
        ThriftScanContext {
            database: scan.database.clone(),
            table: scan.table.name.clone(),
            node_id: scan_node_id,
            scan_tuple_id: scan_node_id,
            min_max_predicates: Vec::new(),
            change_op_slot: None,
            cloud_properties: cloud_properties.clone(),
            columns: scan.table.columns.clone(),
            ..ThriftScanContext::default()
        },
    )?;
    Ok(plan.scan_ranges)
}

fn effective_scan_column_names(scan: &PlanScanNode) -> Vec<String> {
    scan.required_columns.clone().unwrap_or_else(|| {
        scan.table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    })
}

fn build_iceberg_metadata_scan_range_params() -> scan_range::ScanRangeParams {
    scan_range::ScanRangeParams::file(scan_range::FileScanRange {
        file_format: scan_range::FileFormat::Parquet,
        full_path: Some("iceberg-metadata".to_string()),
        relative_path: None,
        table_id: None,
        offset: 0,
        length: 0,
        file_length: 0,
        delete_files: Vec::new(),
        deletion_vector_descriptor: None,
        first_row_id: None,
        data_sequence_number: None,
        modification_time: None,
        datacache_options: None,
        included_positions: Vec::new(),
        serialized_split: Some(String::new()),
        use_iceberg_jni_metadata_reader: true,
        ivm_change_op: None,
        file_pruning_min_max_values: None,
        compat_change_op_slot_id: None,
    })
}

fn projected_target_state_column_names(
    scan: &crate::sql::catalog::IcebergMvTargetStateScan,
) -> Vec<String> {
    let mut names = Vec::new();
    push_unique_projected_name(&mut names, &scan.row_id_column_name);
    for name in scan
        .group_key_names
        .iter()
        .chain(scan.aggregate_state_names.iter())
    {
        push_unique_projected_name(&mut names, name);
    }
    if let crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
        branch_scope: Some(scope),
        ..
    } = &scan.row_filter
    {
        push_unique_projected_name(&mut names, &scope.branch_id_column_name);
    }
    for name in [
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
    ] {
        push_unique_projected_name(&mut names, name);
    }
    names
}

fn projected_target_locator_column_names(
    scan: &crate::sql::catalog::IcebergMvTargetLocatorScan,
) -> Vec<String> {
    let mut names = vec![scan.apply_key_column.clone()];
    if let Some(branch_id_column) = &scan.branch_id_column
        && !names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(branch_id_column))
    {
        names.push(branch_id_column.clone());
    }
    for name in [
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
    ] {
        push_unique_projected_name(&mut names, name);
    }
    names
}

fn push_unique_projected_name(names: &mut Vec<String>, name: &str) {
    if !names
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(name))
    {
        names.push(name.to_string());
    }
}

fn reject_target_state_equality_deletes(source: &ScanSource) -> Result<(), String> {
    let ScanSource::IcebergDataFiles { files, .. } = source else {
        return Ok(());
    };
    let has_equality_delete = files.iter().any(|file| {
        file.delete_files.iter().any(|delete_file| {
            delete_file.file_content == crate::sql::catalog::IcebergDeleteFileContent::Equality
        })
    });
    if has_equality_delete {
        return Err("Iceberg target-state scan does not support equality deletes yet".to_string());
    }
    Ok(())
}

fn output_column_for_boundary(column: &AnalysisOutputColumn) -> OutputColumn {
    OutputColumn {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn result_root_boundary_schema_report(
    fragment_id: FragmentId,
    root_node_id: i32,
    output_columns: &[OutputColumn],
) -> BoundarySchemaReport {
    BoundarySchemaReport {
        fragment_id: Some(fragment_id as i32),
        node_id: root_node_id,
        boundary_kind: BoundaryKind::ResultRoot,
        columns: output_columns_to_boundary_columns(output_columns),
    }
}

fn edge_boundary_schemas(dp: &DistributedPlan) -> Result<Vec<BoundarySchemaReport>, String> {
    let fragments_by_id: BTreeMap<FragmentId, &PlanFragment> = dp
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
            FragmentEdgeKind::CteMulticast { .. } | FragmentEdgeKind::Stream => {
                if exchange.output_columns.is_empty() {
                    &source.output_columns
                } else {
                    &exchange.output_columns
                }
            }
            FragmentEdgeKind::IcebergChangeStreamRouter { .. } => &exchange.output_columns,
        };
        let output_columns = edge_output_columns
            .iter()
            .map(output_column_for_boundary)
            .collect::<Vec<_>>();
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

fn runtime_filter_plan(dp: &DistributedPlan) -> Option<RuntimeFilterPlanResult> {
    let mut all_filters = HashMap::new();
    let mut build_side_filters: HashMap<FragmentId, Vec<i32>> = HashMap::new();
    let mut probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>> = HashMap::new();
    let mut probe_targets: HashMap<i32, Vec<(FragmentId, i32)>> = HashMap::new();

    for fragment in &dp.fragments {
        collect_runtime_filter_probe_targets(
            fragment.fragment_id,
            &fragment.root,
            &mut probe_targets,
        );
    }
    for fragment in &dp.fragments {
        collect_runtime_filter_builds(
            fragment.fragment_id,
            &fragment.root,
            &probe_targets,
            &mut all_filters,
            &mut build_side_filters,
            &mut probe_side_filters,
        );
    }

    if all_filters.is_empty() {
        None
    } else {
        Some(RuntimeFilterPlanResult {
            all_filters,
            build_side_filters,
            probe_side_filters,
        })
    }
}

fn collect_runtime_filter_probe_targets(
    fragment_id: FragmentId,
    node: &DistributedNode,
    out: &mut HashMap<i32, Vec<(FragmentId, i32)>>,
) {
    for probe in &node.probe_runtime_filters {
        out.entry(probe.filter_id)
            .or_default()
            .push((fragment_id, node.node_id));
    }
    for child in &node.children {
        collect_runtime_filter_probe_targets(fragment_id, child, out);
    }
}

fn collect_runtime_filter_builds(
    fragment_id: FragmentId,
    node: &DistributedNode,
    probe_targets: &HashMap<i32, Vec<(FragmentId, i32)>>,
    all_filters: &mut HashMap<i32, PlannedRuntimeFilter>,
    build_side_filters: &mut HashMap<FragmentId, Vec<i32>>,
    probe_side_filters: &mut HashMap<FragmentId, Vec<(i32, i32)>>,
) {
    for build in &node.build_runtime_filters {
        let targets = probe_targets
            .get(&build.filter_id)
            .cloned()
            .unwrap_or_default();
        let probe_target_node_ids = targets.iter().map(|(_, node_id)| *node_id).collect();
        let has_remote_targets = targets
            .iter()
            .any(|(target_fragment_id, _)| *target_fragment_id != fragment_id);
        all_filters.insert(
            build.filter_id,
            PlannedRuntimeFilter {
                filter_id: build.filter_id,
                build_plan_node_id: node.node_id,
                probe_target_node_ids,
                has_remote_targets,
                execution_mode: build.execution_mode,
                expr_order: i32::try_from(build.expr_order).unwrap_or(i32::MAX),
            },
        );
        build_side_filters
            .entry(fragment_id)
            .or_default()
            .push(build.filter_id);
        for (target_fragment_id, target_node_id) in targets {
            probe_side_filters
                .entry(target_fragment_id)
                .or_default()
                .push((build.filter_id, target_node_id));
        }
    }
    for child in &node.children {
        collect_runtime_filter_builds(
            fragment_id,
            child,
            probe_targets,
            all_filters,
            build_side_filters,
            probe_side_filters,
        );
    }
}

fn validate_distributed_plan(dp: &DistributedPlan) -> Result<(), String> {
    if dp.fragments.is_empty() {
        return Err("lower_distributed_plan requires at least one fragment".to_string());
    }

    let mut fragments_by_id = BTreeMap::new();
    for fragment in &dp.fragments {
        if fragments_by_id
            .insert(fragment.fragment_id, fragment)
            .is_some()
        {
            return Err(format!(
                "lower_distributed_plan duplicate fragment id={}",
                fragment.fragment_id
            ));
        }
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
            if !matches!(
                fragment.sink,
                crate::sql::planner::DataSink::Result
                    | crate::sql::planner::DataSink::IcebergWrite(_)
                    | crate::sql::planner::DataSink::IcebergChangeStreamRouter(_)
            ) {
                return Err(format!(
                    "lower_distributed_plan root fragment id={} must use result, Iceberg write, or Iceberg change-stream router sink",
                    fragment.fragment_id
                ));
            }
            ensure_unpartitioned("root output_partition", &fragment.output_partition)?;
        } else if !matches!(
            fragment.sink,
            crate::sql::planner::DataSink::Noop | crate::sql::planner::DataSink::IcebergWrite(_)
        ) {
            return Err(format!(
                "lower_distributed_plan non-root fragment id={} must use noop or Iceberg write sink",
                fragment.fragment_id
            ));
        }
    }

    if !fragments_by_id.contains_key(&dp.root_fragment_id) {
        return Err(format!(
            "lower_distributed_plan root fragment id={} was not found",
            dp.root_fragment_id
        ));
    }

    for edge in &dp.edges {
        if !fragments_by_id.contains_key(&edge.source_fragment_id) {
            return Err(format!(
                "lower_distributed_plan edge references missing source fragment id={}",
                edge.source_fragment_id
            ));
        }
        if !fragments_by_id.contains_key(&edge.target_fragment_id) {
            return Err(format!(
                "lower_distributed_plan edge references missing target fragment id={}",
                edge.target_fragment_id
            ));
        }
        target_exchange_for_edge(&fragments_by_id, edge)?;
    }
    Ok(())
}

fn validate_node_fragment_ownership(
    fragment_id: FragmentId,
    node: &DistributedNode,
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

fn ensure_unpartitioned(label: &str, partition: &DataPartition) -> Result<(), String> {
    if !matches!(partition.kind, PartitionKind::Unpartitioned) || !partition.exprs.is_empty() {
        return Err(format!(
            "lower_distributed_plan supports only unpartitioned {label}"
        ));
    }
    Ok(())
}

fn target_exchange_for_edge<'a>(
    fragments_by_id: &BTreeMap<FragmentId, &'a PlanFragment>,
    edge: &crate::sql::codegen::FragmentEdge,
) -> Result<&'a crate::sql::planner::ExchangeReceiver, String> {
    let target = fragments_by_id
        .get(&edge.target_fragment_id)
        .ok_or_else(|| {
            format!(
                "lower_distributed_plan edge references missing target fragment id={}",
                edge.target_fragment_id
            )
        })?;
    let exchange = find_exchange_node(&target.root, edge.target_exchange_node_id).ok_or_else(|| {
        format!(
            "lower_distributed_plan edge target_exchange_node_id={} not found in target fragment id={}",
            edge.target_exchange_node_id, edge.target_fragment_id
        )
    })?;
    let DistributedPayload::Exchange(exchange) = &exchange.payload else {
        return Err(format!(
            "lower_distributed_plan edge target_exchange_node_id={} in target fragment id={} must target Exchange",
            edge.target_exchange_node_id, edge.target_fragment_id
        ));
    };
    match (&edge.edge_kind, &exchange.flavor) {
        (FragmentEdgeKind::Stream, ExchangeFlavor::Distribution) => {}
        (
            FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            },
            ExchangeFlavor::CteMulticast {
                cte_id: exchange_cte_id,
                receive_producer_column_ids: exchange_ids,
            },
        ) => {
            if cte_id != exchange_cte_id || receive_producer_column_ids != exchange_ids {
                return Err(format!(
                    "lower_distributed_plan CTE multicast edge metadata does not match Exchange metadata for target_exchange_node_id={} in target fragment id={}",
                    edge.target_exchange_node_id, edge.target_fragment_id
                ));
            }
        }
        (FragmentEdgeKind::IcebergChangeStreamRouter { .. }, ExchangeFlavor::Distribution) => {}
        (FragmentEdgeKind::Stream, _) => {
            return Err(format!(
                "lower_distributed_plan stream edge target_exchange_node_id={} in target fragment id={} must target stream Exchange",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }
        (FragmentEdgeKind::CteMulticast { .. }, _) => {
            return Err(format!(
                "lower_distributed_plan CTE multicast edge target_exchange_node_id={} in target fragment id={} must target Exchange(CteMulticast)",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }
        (FragmentEdgeKind::IcebergChangeStreamRouter { .. }, _) => {
            return Err(format!(
                "lower_distributed_plan Iceberg change-stream router edge target_exchange_node_id={} in target fragment id={} must target Exchange(Distribution)",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }
    }
    Ok(exchange)
}

fn find_exchange_node(node: &DistributedNode, node_id: i32) -> Option<&DistributedNode> {
    if node.node_id == node_id {
        return Some(node);
    }
    for child in &node.children {
        if let Some(found) = find_exchange_node(child, node_id) {
            return Some(found);
        }
    }
    None
}

fn distributed_node_has_scan(node: &DistributedNode) -> bool {
    matches!(
        node.payload,
        DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
    ) || node.children.iter().any(distributed_node_has_scan)
}

fn fragment_output_kind(sink: &crate::sql::planner::DataSink) -> FragmentOutputKind {
    match sink {
        crate::sql::planner::DataSink::Result => FragmentOutputKind::Result,
        crate::sql::planner::DataSink::IcebergWrite(_) => FragmentOutputKind::TerminalWrite,
        crate::sql::planner::DataSink::Noop
        | crate::sql::planner::DataSink::IcebergChangeStreamRouter(_) => {
            FragmentOutputKind::NonTerminal
        }
    }
}

#[allow(dead_code)]
fn fragment_stream_kind(partition: &DataPartition) -> FragmentStreamKind {
    match partition.kind {
        PartitionKind::Unpartitioned => FragmentStreamKind::Gather,
        PartitionKind::Random => FragmentStreamKind::Broadcast,
        PartitionKind::Hash => FragmentStreamKind::Partitioned,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::ConnectorRegistry;
    use crate::sql::analysis::OutputColumn as AnalysisOutputColumn;
    use crate::sql::analysis::cte::CteId;
    use crate::sql::catalog::{CatalogProvider, TableDef};
    use crate::sql::codegen::{FragmentEdge, FragmentEdgeKind};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::ExchangeReceiver;
    use crate::sql::planner::plan::{ExchangeFlavor, PhysicalPlanKind, PlanValuesNode};
    use crate::sql::planner::{PhysicalPlanStats, PlannerConfidence};

    struct EmptyCatalog;

    impl CatalogProvider for EmptyCatalog {
        fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
            Err(format!("unexpected table lookup {database}.{table}"))
        }
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn output_col(id: u32, name: &str) -> AnalysisOutputColumn {
        AnalysisOutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn physical_values_node(
        fragment_id: FragmentId,
        node_id: i32,
        columns: Vec<AnalysisOutputColumn>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns,
            })),
        }
    }

    #[test]
    fn lower_distributed_plan_lowers_cte_multicast_edge_output_slots_to_requested_producer_columns()
    {
        let cte_id: CteId = 7;
        let producer_columns = vec![
            output_col(1, "k"),
            output_col(2, "v"),
            output_col(3, "payload"),
        ];
        let receive_columns = vec![producer_columns[0].clone(), producer_columns[2].clone()];
        let receive_producer_column_ids =
            vec![producer_columns[0].column_id, producer_columns[2].column_id];

        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer_fragment = PlanFragment {
            fragment_id: producer_fragment_id,
            root: physical_values_node(producer_fragment_id, 10, producer_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Noop,
            output_exprs: None,
            output_columns: producer_columns,
            cte_id: Some(cte_id),
            cte_exchange_nodes: Vec::new(),
        };
        let consumer_fragment = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedPayload::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: receive_columns.clone(),
                    output_qualifier: Some("c".to_string()),
                    flavor: ExchangeFlavor::CteMulticast {
                        cte_id,
                        receive_producer_column_ids: receive_producer_column_ids.clone(),
                    },
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Result,
            output_exprs: None,
            output_columns: receive_columns,
            cte_id: None,
            cte_exchange_nodes: vec![(
                cte_id,
                exchange_node_id,
                receive_producer_column_ids.clone(),
            )],
        };
        let dp = DistributedPlan {
            fragments: vec![producer_fragment, consumer_fragment],
            root_fragment_id: consumer_fragment_id,
            edges: vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids,
                },
                output_slot_ids: Vec::new(),
            }],
        };

        let result = lower_distributed_plan(&dp, &EmptyCatalog, &ConnectorRegistry::new(), None)
            .expect("native lower plan");

        assert_eq!(result.edges[0].output_slot_ids, vec![1, 3]);
    }
}
