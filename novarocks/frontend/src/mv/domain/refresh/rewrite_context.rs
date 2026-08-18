// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Frozen refresh rewrite inputs shared by domain planning and query assembly.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::mv::domain::persistence::definition::StoredMvDefinition;
use crate::mv::domain::refresh::pin::RefreshSnapshotPin;
use crate::mv::domain::refresh::target::{IcebergMvTarget, load_iceberg_mv_target_binding};
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use novarocks_catalog::identifier::TableIdentity;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{
    ConnectorChangeWindow, ConnectorChangeWindowAdmission, ConnectorControlRegistry,
    ConnectorRequestContext, ConnectorScanAdmission, ConnectorTableResolution,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdmittedChangeFacts {
    pub has_inserts: bool,
    pub has_deletes: bool,
}

pub fn admitted_change_facts(
    admission: &ConnectorChangeWindowAdmission,
) -> Result<AdmittedChangeFacts, String> {
    match admission {
        ConnectorChangeWindowAdmission::MetadataOnly => Ok(AdmittedChangeFacts::default()),
        ConnectorChangeWindowAdmission::Incremental {
            has_inserts,
            has_deletes,
            ..
        } => Ok(AdmittedChangeFacts {
            has_inserts: *has_inserts,
            has_deletes: *has_deletes,
        }),
        ConnectorChangeWindowAdmission::FullRebuild(reason) => Err(
            crate::mv::domain::refresh::non_join_incremental::full_rebuild_reason_message(*reason),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_neutral_refresh_rewrite_context(
    connector_control: &dyn ConnectorControlRegistry,
    storage_observation: &dyn MvStorageObservationPort,
    target: &IcebergMvTarget,
    mv_id: i64,
    current_catalog: Option<&str>,
    current_database: &str,
    definition: Arc<StoredMvDefinition>,
    canonical_query: Arc<sqlparser::ast::Query>,
    base_refs: Arc<[TableIdentity]>,
    pin: Arc<RefreshSnapshotPin>,
    previous_snapshot_ids: BTreeMap<String, i64>,
    previous_table_uuids: BTreeMap<String, String>,
    target_snapshot_id: Option<i64>,
    target_table_uuid: String,
    retained_target_binding: Option<&crate::mv::domain::refresh::target_binding::MvTargetBinding>,
    connector_context: &ConnectorRequestContext,
) -> Result<Arc<crate::mv::domain::rewrite::context::IcebergMvRewriteContext>, String> {
    let loaded_target_binding;
    let binding = match retained_target_binding {
        Some(binding) => binding,
        None => {
            loaded_target_binding = load_iceberg_mv_target_binding(
                connector_control,
                storage_observation,
                target,
                connector_context,
            )?;
            &loaded_target_binding
        }
    };
    if binding.table_uuid() != target_table_uuid {
        return Err(format!(
            "MV refresh target UUID drifted after planning for {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    }
    if binding.current_snapshot_id() != target_snapshot_id {
        return Err(format!(
            "MV refresh target snapshot drifted after planning for {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    }
    let schema_contract = definition.schema_contract.clone().map(Arc::new);
    crate::mv::domain::rewrite::context::IcebergMvRewriteContext::from_parts(
        TableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        mv_id,
        current_catalog.map(str::to_string),
        current_database.to_string(),
        definition,
        canonical_query,
        base_refs,
        pin,
        previous_snapshot_ids,
        previous_table_uuids,
        target_snapshot_id,
        target_table_uuid,
        binding.physical_write_schema()?,
        Arc::from(binding.observation().field_ids().to_vec()),
        schema_contract,
    )
    .map(Arc::new)
}

pub(crate) fn observe_and_admit_change_window_for_table(
    connector_control: &dyn ConnectorControlRegistry,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    connector_context: &ConnectorRequestContext,
) -> Result<
    (
        ConnectorChangeWindowAdmission,
        MvSchemaValidationObservation,
    ),
    String,
> {
    let exact_lease =
        novarocks::connector::acquire_metadata_planning_lease(connector_control, &table.catalog)?;
    let metadata = novarocks::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    let window = ConnectorChangeWindow::new(from_snapshot_id, to_snapshot_id);
    let scan = novarocks::connector::scan_admission::admit_connector_change_window(
        &metadata.table,
        &metadata.schema,
        &exact_lease,
        connector_context.clone(),
        window,
    )?;
    let ConnectorScanAdmission::ChangeWindow(admission) = scan.admission() else {
        return Err("connector returned a snapshot admission for a change-window scan".to_string());
    };
    let observation = crate::mv::domain::storage_observation::observe_schema_validation(
        storage_observation,
        &exact_lease,
        &metadata,
        connector_context.clone(),
    )
    .map_err(|error| {
        format!(
            "observe MV schema validation facts for {}: {error}",
            table.fqn()
        )
    })?;
    Ok((admission.clone(), observation))
}
