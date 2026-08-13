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

//! Connector-facing table-maintenance execution.
//!
//! SQL parsing, application dispatch, and result encoding belong to
//! `novarocks-frontend`. Catalog, snapshot, file and commit truth belongs to
//! the Connector; this module only routes maintenance intents to it and shapes
//! the neutral outcome the frontend reports.

use crate::connector::metadata_maintenance::MetadataMaintenanceCacheFinalizer;
use crate::engine::table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget,
};

/// Execute a non-rewrite maintenance action through explicit frontend-owned
/// connector and cache-finalization ports. The caller owns the request context;
/// this helper never captures a process-local cancellation or topology view.
pub(crate) fn execute_action_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    request: MaintenanceActionRequest,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MaintenanceActionOutcome, String> {
    match request {
        MaintenanceActionRequest::RewriteDataFiles { .. }
        | MaintenanceActionRequest::RewritePositionDeleteFiles { .. } => Err(
            "distributed rewrite must be dispatched by the frontend table-maintenance owner"
                .to_string(),
        ),
        MaintenanceActionRequest::RewriteManifests {
            target,
            use_caching,
            spec_id,
        } => run_rewrite_manifests_action_with_ports(
            connector_control,
            cache_finalizer,
            target,
            use_caching,
            spec_id,
            connector_context,
        ),
        MaintenanceActionRequest::ExpireSnapshots {
            target,
            older_than_ms,
            retain_last,
        } => run_expire_snapshots_action_with_ports(
            connector_control,
            cache_finalizer,
            target,
            older_than_ms,
            retain_last,
            connector_context,
        ),
        MaintenanceActionRequest::RemoveOrphanFiles { .. } => Err(
            "remove orphan files must be dispatched by the frontend durable cleanup owner"
                .to_string(),
        ),
    }
}

/// Read the current snapshot on a caller-supplied exact request context.
pub(crate) fn current_snapshot_id_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    target: &MaintenanceTarget,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<i64, String> {
    let exact_lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &target.catalog)?;
    let facts = crate::connector::metadata_read_reference_facts_with_planning_lease(
        exact_lease,
        context,
        &target.namespace,
        &target.table,
    )?;
    facts.current_snapshot_id().ok_or_else(|| {
        format!(
            "iceberg table {} has no current snapshot",
            action_target(target)
        )
    })
}

fn run_rewrite_manifests_action_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    target: MaintenanceTarget,
    use_caching: Option<bool>,
    spec_id: Option<i32>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MaintenanceActionOutcome, String> {
    if use_caching.is_some() {
        return Err(
            "rewrite_manifests `use_caching` is not implemented in NovaRocks yet".to_string(),
        );
    }
    if spec_id.is_some() {
        return Err("rewrite_manifests `spec_id` is not implemented in NovaRocks yet".to_string());
    }
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| error.to_string())?;
    let completed = crate::connector::metadata_maintenance::execute_metadata_maintenance(
        connector_control,
        cache_finalizer,
        &instance_id,
        novarocks_spi::connector::ConnectorMutationOperationId::new(),
        novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: target.namespace.clone().into(),
            table: target.table.clone().into(),
        },
        crate::connector::metadata_maintenance::MetadataMaintenanceIntent::rewrite_metadata_layout(
        ),
        connector_context,
    )?;
    let summary = completed.receipt.summary();

    Ok(MaintenanceActionOutcome::RewriteManifests {
        rewritten_manifests_count: i32::try_from(summary.rewritten_items)
            .map_err(|_| "rewrite manifest count exceeds Spark result range".to_string())?,
        added_manifests_count: i32::try_from(summary.added_items)
            .map_err(|_| "added manifest count exceeds Spark result range".to_string())?,
    })
}

fn run_expire_snapshots_action_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    cache_finalizer: &dyn MetadataMaintenanceCacheFinalizer,
    target: MaintenanceTarget,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MaintenanceActionOutcome, String> {
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| error.to_string())?;
    crate::connector::metadata_maintenance::execute_metadata_maintenance(
        connector_control,
        cache_finalizer,
        &instance_id,
        novarocks_spi::connector::ConnectorMutationOperationId::new(),
        novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: target.namespace.clone().into(),
            table: target.table.clone().into(),
        },
        crate::connector::metadata_maintenance::MetadataMaintenanceIntent::expire_table_versions(
            older_than_ms,
            retain_last,
        ),
        connector_context,
    )?;

    tracing::info!(
        catalog = %target.catalog,
        namespace = %target.namespace,
        table = %target.table,
        "expire_snapshots: completed"
    );

    Ok(MaintenanceActionOutcome::ExpireSnapshots {
        deleted_data_files_count: None,
        deleted_position_delete_files_count: None,
        deleted_equality_delete_files_count: None,
        deleted_manifest_files_count: None,
        deleted_manifest_lists_count: None,
        deleted_statistics_files_count: None,
    })
}

fn action_target(target: &MaintenanceTarget) -> String {
    format!("{}.{}.{}", target.catalog, target.namespace, target.table)
}
