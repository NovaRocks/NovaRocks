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

use crate::connector::ConnectorRegistry;
use crate::connector::scan_model::starrocks::PlannedNativeStarRocksScan;
use crate::query_execution::preparation::scan::{
    ResolvedIcebergFileScan, ResolvedScanBinding, ResolvedScanExecution, ScanBindingResolver,
    ScanExecutionBindings,
};
use crate::sql::planner::distributed::{
    DistributedNode, DistributedNodeKind, DistributedPlan, FragmentId,
};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

mod iceberg;
mod projection;
mod pruning;
mod static_predicate;

pub(crate) use iceberg::build_iceberg_metadata_scan_range_params;
use iceberg::{plan_iceberg_connector_read, plan_iceberg_delta_connector_read};
use projection::{resolve_effective_required_reads, resolve_physical_columns};
use static_predicate::lower_static_connector_predicates;

/// Immutable scan-planning choices derived from the session before connector
/// negotiation begins. Keeping this outside the native carrier makes an
/// explicit disabled setting a safe FE-side rollback.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScanPreparationOptions {
    pub(crate) enable_connector_static_predicate_pushdown: bool,
}

impl Default for ScanPreparationOptions {
    fn default() -> Self {
        Self {
            enable_connector_static_predicate_pushdown: true,
        }
    }
}

pub(super) fn prepare_scan_bindings(
    plan: &DistributedPlan,
    connectors: &ConnectorRegistry,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    resolver: Option<&dyn ScanBindingResolver>,
    options: ScanPreparationOptions,
) -> Result<ScanExecutionBindings, String> {
    let mut bindings = ScanExecutionBindings::default();
    let mut seen_scan_node_ids = std::collections::BTreeSet::new();
    for fragment in plan.fragments() {
        collect_scan_bindings(
            fragment.fragment_id,
            &fragment.root,
            connectors,
            controls,
            context,
            resolver,
            options,
            &mut seen_scan_node_ids,
            &mut bindings,
        )?;
    }
    Ok(bindings)
}

fn collect_scan_bindings(
    fragment_id: FragmentId,
    node: &DistributedNode,
    connectors: &ConnectorRegistry,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    resolver: Option<&dyn ScanBindingResolver>,
    options: ScanPreparationOptions,
    seen_scan_node_ids: &mut std::collections::BTreeSet<i32>,
    bindings: &mut ScanExecutionBindings,
) -> Result<(), String> {
    if let DistributedNodeKind::Scan(scan) = &node.payload {
        if !seen_scan_node_ids.insert(node.node_id) {
            return Err(format!("duplicate scan node_id={}", node.node_id));
        }
        prepare_scan_node(
            fragment_id,
            node.node_id,
            scan,
            connectors,
            controls,
            context,
            resolver,
            options,
            bindings,
        )?;
    }
    for child in &node.children {
        if child.fragment_id == fragment_id {
            collect_scan_bindings(
                fragment_id,
                child,
                connectors,
                controls,
                context,
                resolver,
                options,
                seen_scan_node_ids,
                bindings,
            )?;
        }
    }
    Ok(())
}

fn prepare_scan_node(
    fragment_id: FragmentId,
    node_id: i32,
    scan: &PlanScanNode,
    connectors: &ConnectorRegistry,
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    resolver: Option<&dyn ScanBindingResolver>,
    options: ScanPreparationOptions,
    bindings: &mut ScanExecutionBindings,
) -> Result<(), String> {
    let execution = match &scan.table.source {
        ScanSource::IcebergDataFiles {
            table,
            files,
            binding,
            ..
        } => ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
            table: table.clone(),
            files: files.clone(),
            binding: *binding,
        }),
        ScanSource::IcebergMetadataTable { .. } => {
            return bindings.insert_scan_ranges(
                fragment_id,
                node_id,
                vec![build_iceberg_metadata_scan_range_params()],
            );
        }
        ScanSource::StarRocks { .. } => {
            return Err("native StarRocks catalog scan planning is unavailable".to_string());
        }
        source if scan_source_requires_resolver(source) => {
            let source_context = scan_source_context(source);
            let resolver = resolver.ok_or_else(|| {
                format!(
                    "scan source {source_context} node_id={node_id} requires scan binding resolver"
                )
            })?;
            resolver
                .resolve_scan(node_id, scan)
                .map_err(|err| {
                    format!(
                        "scan binding resolver failed for required source {source_context} node_id={node_id}: {err}"
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "scan binding resolver returned no binding for required source {source_context} node_id={node_id}"
                    )
                })?
        }
        source => {
            return Err(format!(
                "scan preparation does not yet support source {source:?} for node_id={node_id}"
            ));
        }
    };
    validate_resolved_execution_kind(node_id, &scan.table.source, &execution)?;
    reject_target_equality_deletes(node_id, &scan.table.source, &execution)?;
    let physical_columns = resolve_physical_columns(node_id, scan)?;
    let (ranges, equality_required, connector_read) = match &execution {
        ResolvedScanExecution::ConnectorRead => {
            let resolver = resolver.ok_or_else(|| {
                format!("connector-pinned scan node_id={node_id} requires a scan binding resolver")
            })?;
            let read = resolver
                .resolve_connector_read(node_id, scan)
                .map_err(|error| {
                    format!(
                        "scan binding resolver failed to provide connector read for node_id={node_id}: {error}"
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "scan binding resolver returned no connector read for connector-pinned node_id={node_id}"
                    )
                })?;
            (Vec::new(), Vec::new(), Some(read))
        }
        ResolvedScanExecution::IcebergFiles(files) => {
            // Design: ADR-0018 (docs/adr/ADR-0018-static-connector-predicate-disposition.md)
            let static_predicates = options
                .enable_connector_static_predicate_pushdown
                .then(|| {
                    let connector_schema_fields = files
                        .table
                        .schema
                        .fields
                        .iter()
                        .map(|field| field.name.as_str())
                        .collect::<Vec<_>>();
                    lower_static_connector_predicates(scan, &connector_schema_fields)
                })
                .unwrap_or_default();
            let planned = plan_iceberg_connector_read(
                controls,
                context.clone(),
                scan,
                &execution,
                static_predicates,
            )
            .map_err(|err| format!("scan preparation node_id={node_id}: {err}"))?;
            // The provider reader projects physical equality keys internally
            // and drops them before delivery. Core therefore never owns a
            // hidden Iceberg delete column or file range.
            (Vec::new(), Vec::new(), Some(planned))
        }
        ResolvedScanExecution::IcebergDelta(_) => {
            let planned =
                plan_iceberg_delta_connector_read(controls, context.clone(), scan, &execution)
                    .map_err(|err| format!("scan preparation node_id={node_id}: {err}"))?;
            (Vec::new(), Vec::new(), Some(planned))
        }
    };
    let required_reads = resolve_effective_required_reads(node_id, scan, &equality_required)?;
    bindings.insert_binding(ResolvedScanBinding {
        node_id,
        execution,
        physical_columns,
        required_reads,
    })?;
    if let Some(connector_read) = connector_read {
        bindings.insert_connector_read(fragment_id, node_id, connector_read)?;
    }
    bindings.insert_scan_ranges(fragment_id, node_id, ranges)
}

fn store_planned_starrocks_scan(
    fragment_id: FragmentId,
    node_id: i32,
    planned: PlannedNativeStarRocksScan,
    bindings: &mut ScanExecutionBindings,
) -> Result<(), String> {
    if bindings.scan_ranges(fragment_id, node_id).is_some()
        || bindings.starrocks_source(node_id).is_some()
    {
        return Err(format!(
            "duplicate StarRocks scan planning fragment_id={fragment_id} node_id={node_id}"
        ));
    }
    bindings.insert_starrocks_source(node_id, planned.source)?;
    bindings.insert_scan_ranges(fragment_id, node_id, planned.ranges)
}

fn validate_resolved_execution_kind(
    node_id: i32,
    source: &ScanSource,
    execution: &ResolvedScanExecution,
) -> Result<(), String> {
    let valid = match source {
        ScanSource::ConnectorPinned => matches!(execution, ResolvedScanExecution::ConnectorRead),
        ScanSource::IcebergDeltaTable { .. } => {
            matches!(execution, ResolvedScanExecution::IcebergDelta(_))
        }
        ScanSource::IcebergDataFiles { .. }
        | ScanSource::IcebergVersionTable { .. }
        | ScanSource::IcebergMvTargetState(_)
        | ScanSource::IcebergMvTargetLocator(_) => {
            matches!(execution, ResolvedScanExecution::IcebergFiles(_))
        }
        ScanSource::StarRocks { .. } | ScanSource::IcebergMetadataTable { .. } => true,
    };
    if valid {
        return Ok(());
    }
    let required = if matches!(source, ScanSource::ConnectorPinned) {
        "ConnectorRead"
    } else if matches!(source, ScanSource::IcebergDeltaTable { .. }) {
        "IcebergDelta"
    } else {
        "IcebergFiles"
    };
    Err(format!(
        "scan source {} node_id={node_id} requires {required} execution",
        scan_source_kind(source)
    ))
}

fn reject_target_equality_deletes(
    node_id: i32,
    source: &ScanSource,
    execution: &ResolvedScanExecution,
) -> Result<(), String> {
    let target_kind = match source {
        ScanSource::IcebergMvTargetState(_) => "target-state",
        ScanSource::IcebergMvTargetLocator(_) => "target-locator",
        _ => return Ok(()),
    };
    let ResolvedScanExecution::IcebergFiles(files) = execution else {
        return Err(format!(
            "Iceberg {target_kind} scan node_id={node_id} requires IcebergFiles execution"
        ));
    };
    if files.files.iter().any(|file| {
        file.delete_files.iter().any(|delete| {
            delete.file_content
                == crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Equality
        })
    }) {
        return Err(format!(
            "Iceberg {target_kind} scan node_id={node_id} does not support equality deletes yet"
        ));
    }
    Ok(())
}

fn scan_source_requires_resolver(source: &ScanSource) -> bool {
    matches!(
        source,
        ScanSource::ConnectorPinned
            | ScanSource::IcebergVersionTable { .. }
            | ScanSource::IcebergMvTargetState(_)
            | ScanSource::IcebergMvTargetLocator(_)
            | ScanSource::IcebergDeltaTable { .. }
    )
}

fn scan_source_kind(source: &ScanSource) -> &'static str {
    match source {
        ScanSource::ConnectorPinned => "ConnectorPinned",
        ScanSource::StarRocks { .. } => "StarRocks",
        ScanSource::IcebergDataFiles { .. } => "IcebergDataFiles",
        ScanSource::IcebergMetadataTable { .. } => "IcebergMetadataTable",
        ScanSource::IcebergDeltaTable { .. } => "IcebergDeltaTable",
        ScanSource::IcebergVersionTable { .. } => "IcebergVersionTable",
        ScanSource::IcebergMvTargetState(_) => "IcebergMvTargetState",
        ScanSource::IcebergMvTargetLocator(_) => "IcebergMvTargetLocator",
    }
}

fn scan_source_context(source: &ScanSource) -> String {
    match source {
        ScanSource::IcebergDeltaTable {
            from_snapshot_id,
            to_snapshot_id,
            ..
        } => format!(
            "IcebergDeltaTable from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}"
        ),
        _ => scan_source_kind(source).to_string(),
    }
}

#[cfg(test)]
mod tests;
