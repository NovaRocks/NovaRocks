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

//! Iceberg control-plane implementation of metadata-maintenance SPI.
//!
//! This module deliberately owns catalog objects only on the FE.  Its plan is
//! opaque to generic core and its execute path never opens a BE writer or
//! emits a fragment carrier.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorMaxCompactableDataFiles, ConnectorMaxCompactableDataFilesRequest,
    ConnectorMetadataMaintenance, ConnectorMetadataMaintenanceExecuteRequest,
    ConnectorMetadataMaintenanceOperation, ConnectorMetadataMaintenancePlan,
    ConnectorMetadataMaintenancePlanSummary, ConnectorMetadataMaintenancePlanningRequest,
    ConnectorMetadataMaintenanceReceipt, ConnectorMetadataMaintenanceReceiptSummary,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ConnectorProviderBindingKey, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::catalog::CatalogTableName;
use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};
use crate::commit::snapshot_lifecycle_helpers::expire_snapshots::{
    ExpireParams, run_expire_snapshots_once_with_marker,
};
use crate::commit::snapshot_lifecycle_helpers::rewrite_manifests::run_rewrite_manifests_once_with_marker;
use crate::metadata::IcebergTablePayload;
use crate::metadata_context::IcebergMetadataContext;

const PAYLOAD_VERSION: u16 = 1;
const MARKER_VERSION: u16 = 1;
const MARKER_PROPERTY: &str = "novarocks.connector.maintenance.v1";
const STATE_DOMAIN: &[u8] = b"novarocks.iceberg.metadata-maintenance.state.v1\0";
const IDENTITY_DOMAIN: &[u8] = b"novarocks.iceberg.metadata-maintenance.identity.v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenancePlanPayloadV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    metadata_location_digest_hex: String,
    base_snapshot_id: Option<i64>,
    schema_id: i32,
    default_spec_id: i32,
    operation_kind: String,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
    artifact_location: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenanceMarkerV1 {
    version: u16,
    identity_digest_hex: String,
    incarnation_hex: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    base_snapshot_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenanceReceiptV1 {
    version: u16,
    snapshot_id: Option<i64>,
    artifact_location: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenanceEvidenceV1 {
    version: u16,
    namespace: String,
    table: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    identity_digest_hex: String,
}

#[derive(Clone)]
struct CachedPlan {
    request_digest: [u8; 32],
    plan: ConnectorMetadataMaintenancePlan,
    payload: IcebergMetadataMaintenancePlanPayloadV1,
}

#[derive(Clone)]
struct TerminalRecord {
    plan_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>,
}

/// FE-only Iceberg adapter.  The registry contains process-local catalog
/// clients and credentials; none cross the SPI payload boundary.
pub(crate) struct IcebergMetadataMaintenanceAdapter {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergMetadataContext>,
    plans: Mutex<HashMap<ConnectorMutationOperationId, CachedPlan>>,
    terminal: Mutex<HashMap<ConnectorMutationOperationId, TerminalRecord>>,
}

impl IcebergMetadataMaintenanceAdapter {
    pub(crate) fn new(
        key: ConnectorProviderBindingKey,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        Ok(Self {
            key,
            descriptor,
            runtime,
            plans: Mutex::new(HashMap::new()),
            terminal: Mutex::new(HashMap::new()),
        })
    }

    fn ensure_owner(&self, owner: &ConnectorProviderBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg metadata maintenance does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    fn plan_payload(
        &self,
        request: &ConnectorMetadataMaintenancePlanningRequest,
    ) -> Result<
        (
            IcebergMetadataMaintenancePlanPayloadV1,
            [u8; 32],
            ConnectorMetadataMaintenancePlanSummary,
        ),
        ConnectorError,
    > {
        let (namespace, table_name) = decode_base_table_target(request.operation().table())?;
        self.runtime
            .control_state()
            .invalidate_table_cache(&namespace, &table_name);
        let loaded = self
            .runtime
            .load_table_for_request(&namespace, &table_name, &request.context)
            .map_err(|error| unavailable(format!("load Iceberg table for maintenance: {error}")))?;
        let table = loaded.into_table();
        let metadata = table.metadata();
        let (older_than_ms, retain_last) = match request.operation() {
            ConnectorMetadataMaintenanceOperation::RewriteMetadataLayout { .. } => (None, None),
            ConnectorMetadataMaintenanceOperation::ExpireTableVersions {
                older_than_ms,
                retain_last,
                ..
            } => (*older_than_ms, *retain_last),
        };
        let state_digest = state_digest(
            metadata.uuid().to_string().as_bytes(),
            table.metadata_location(),
            metadata
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
            metadata.current_schema_id(),
            metadata.default_partition_spec_id(),
        );
        let payload_seed = format!(
            "{}:{}:{}",
            hex_encode(request.operation_id().to_bytes()),
            request.operation().kind(),
            hex_encode(state_digest)
        );
        let artifact_location = maintenance_artifact_location(
            table.metadata_location(),
            request.operation().kind(),
            &payload_seed,
        )?;
        let payload = IcebergMetadataMaintenancePlanPayloadV1 {
            version: PAYLOAD_VERSION,
            namespace,
            table: table_name,
            table_uuid: metadata.uuid().to_string(),
            metadata_location_digest_hex: hex_encode(metadata_location_digest(
                table.metadata_location(),
            )),
            base_snapshot_id: metadata
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
            schema_id: metadata.current_schema_id(),
            default_spec_id: metadata.default_partition_spec_id(),
            operation_kind: request.operation().kind().to_string(),
            older_than_ms,
            retain_last,
            artifact_location,
        };
        let artifact = canonical_json(&payload, "Iceberg metadata maintenance artifact")?;
        if artifact.len() > 1024 * 1024 {
            return Err(resource_exhausted(
                "Iceberg maintenance artifact part exceeds 1 MiB",
            ));
        }
        let output = table
            .file_io()
            .new_output(&payload.artifact_location)
            .map_err(|error| unavailable(format!("create maintenance artifact: {error}")))?;
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { output.write(artifact).await })
            .map_err(unavailable)?
            .map_err(|error| unavailable(format!("write maintenance artifact: {error}")))?;
        let summary = ConnectorMetadataMaintenancePlanSummary::new(
            u64::from(payload.base_snapshot_id.is_some()),
            0,
            0,
            0,
            0,
        );
        Ok((payload, state_digest, summary))
    }

    fn payload_for_plan(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
    ) -> Result<IcebergMetadataMaintenancePlanPayloadV1, ConnectorError> {
        if let Some(cached) = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg maintenance plan lock: {error}")))?
            .get(&plan.operation_id())
            .cloned()
        {
            if cached.plan.plan_digest() == plan.plan_digest() {
                return Ok(cached.payload);
            }
            return Err(invalid(
                "Iceberg metadata maintenance plan digest conflicts",
            ));
        }
        let payload: IcebergMetadataMaintenancePlanPayloadV1 =
            decode_canonical_json(plan.provider_payload(), "Iceberg metadata maintenance plan")?;
        validate_payload_for_plan(plan, &payload)?;
        Ok(payload)
    }

    fn marker(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
    ) -> IcebergMetadataMaintenanceMarkerV1 {
        IcebergMetadataMaintenanceMarkerV1 {
            version: MARKER_VERSION,
            identity_digest_hex: hex_encode(identity_digest(&self.descriptor, &self.key, plan)),
            incarnation_hex: hex_encode(self.key.incarnation.to_bytes()),
            operation_id_hex: hex_encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex_encode(plan.request_digest()),
            plan_digest_hex: hex_encode(plan.plan_digest()),
            state_digest_hex: hex_encode(plan.state_digest()),
            base_snapshot_id: payload.base_snapshot_id,
        }
    }

    fn evidence(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let evidence = IcebergMetadataMaintenanceEvidenceV1 {
            version: PAYLOAD_VERSION,
            namespace: payload.namespace.clone(),
            table: payload.table.clone(),
            operation_id_hex: hex_encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex_encode(plan.request_digest()),
            plan_digest_hex: hex_encode(plan.plan_digest()),
            state_digest_hex: hex_encode(plan.state_digest()),
            identity_digest_hex: hex_encode(identity_digest(&self.descriptor, &self.key, plan)),
        };
        ExternalMutationEvidence::try_new(
            PAYLOAD_VERSION,
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            canonical_json(&evidence, "Iceberg metadata maintenance evidence")?,
        )
    }

    fn lookup_marker(
        &self,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        marker: &IcebergMetadataMaintenanceMarkerV1,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<MarkerLookup, ConnectorError> {
        self.runtime
            .control_state()
            .invalidate_table_cache(&payload.namespace, &payload.table);
        let loaded = self
            .runtime
            .load_table_for_request(&payload.namespace, &payload.table, context)
            .map_err(|error| {
                unavailable(format!("load Iceberg table for marker lookup: {error}"))
            })?;
        let metadata = loaded.table.metadata();
        if let Some(raw) = metadata.properties().get(MARKER_PROPERTY) {
            let stored: IcebergMetadataMaintenanceMarkerV1 =
                decode_canonical_json(raw.as_bytes(), "Iceberg metadata maintenance marker")?;
            if stored.operation_id_hex == marker.operation_id_hex {
                return Ok(
                    if stored.identity_digest_hex == marker.identity_digest_hex {
                        MarkerLookup::Matching {
                            snapshot_id: metadata
                                .current_snapshot()
                                .map(|snapshot| snapshot.snapshot_id()),
                        }
                    } else {
                        MarkerLookup::Conflicting
                    },
                );
            }
        }
        for snapshot in metadata.snapshots() {
            if let Some(raw) = snapshot
                .summary()
                .additional_properties
                .get(MARKER_PROPERTY)
            {
                let stored: IcebergMetadataMaintenanceMarkerV1 =
                    decode_canonical_json(raw.as_bytes(), "Iceberg metadata maintenance marker")?;
                if stored.operation_id_hex == marker.operation_id_hex {
                    return Ok(
                        if stored.identity_digest_hex == marker.identity_digest_hex {
                            MarkerLookup::Matching {
                                snapshot_id: Some(snapshot.snapshot_id()),
                            }
                        } else {
                            MarkerLookup::Conflicting
                        },
                    );
                }
            }
        }
        Ok(MarkerLookup::Missing)
    }

    fn execute_once(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        marker: &IcebergMetadataMaintenanceMarkerV1,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorMetadataMaintenanceReceipt, ExecFailure> {
        self.runtime
            .control_state()
            .invalidate_table_cache(&payload.namespace, &payload.table);
        let catalog = self.runtime.novarocks_catalog().vendored_client();
        let loaded = self
            .runtime
            .load_table_for_request(&payload.namespace, &payload.table, context)
            .map_err(|error| ExecFailure::KnownUncommitted(unavailable(error.to_string())))?;
        validate_frozen_state(&loaded.table, plan, payload)
            .map_err(ExecFailure::KnownUncommitted)?;
        if plan.operation_kind() == novarocks_spi::connector::REWRITE_METADATA_LAYOUT_KIND {
            crate::document_storage::observation::validate_managed_rewrite_attachment(
                loaded.table.metadata(),
            )
            .map_err(ExecFailure::KnownUncommitted)?;
        }
        let table = loaded.into_table();
        let file_io = table.file_io().clone();
        let marker_bytes = canonical_json(marker, "Iceberg metadata maintenance marker")
            .map_err(ExecFailure::KnownUncommitted)?;
        let marker_text = String::from_utf8(marker_bytes.to_vec()).map_err(|error| {
            ExecFailure::KnownUncommitted(internal(format!("marker UTF-8: {error}")))
        })?;
        let summary = match plan.operation_kind() {
            novarocks_spi::connector::REWRITE_METADATA_LAYOUT_KIND => self
                .runtime
                .resources()
                .catalog_runtime()
                .block_on(run_rewrite_manifests_once_with_marker(
                    catalog,
                    table,
                    file_io,
                    Some(marker_text),
                ))
                .map_err(|error| ExecFailure::Unknown(unavailable(error)))
                .and_then(|result| {
                    result
                        .map(|outcome| ConnectorMetadataMaintenanceReceiptSummary {
                            rewritten_items: outcome.rewritten_manifests_count as u64,
                            added_items: outcome.added_manifests_count as u64,
                            ..Default::default()
                        })
                        .map_err(classify_iceberg_error)
                }),
            novarocks_spi::connector::EXPIRE_TABLE_VERSIONS_KIND => self
                .runtime
                .resources()
                .catalog_runtime()
                .block_on(run_expire_snapshots_once_with_marker(
                    catalog,
                    table,
                    file_io,
                    ExpireParams {
                        older_than_ms: payload.older_than_ms,
                        retain_last: payload.retain_last,
                    },
                    Some(marker_text),
                ))
                .map_err(|error| ExecFailure::Unknown(unavailable(error)))
                .and_then(|result| {
                    result
                        .map(|outcome| ConnectorMetadataMaintenanceReceiptSummary {
                            affected_versions: outcome.expired_snapshot_count as u64,
                            cleanup_succeeded: outcome.deleted_file_count as u64,
                            ..Default::default()
                        })
                        .map_err(classify_iceberg_error)
                }),
            _ => Err(ExecFailure::KnownUncommitted(invalid(
                "unsupported metadata maintenance operation",
            ))),
        };
        let summary = summary?;
        self.runtime
            .control_state()
            .invalidate_table_cache(&payload.namespace, &payload.table);
        let committed = self
            .runtime
            .load_table_for_request(&payload.namespace, &payload.table, context)
            .map_err(|error| {
                ExecFailure::Unknown(unavailable(format!(
                    "reload committed Iceberg table: {error}"
                )))
            })?;
        let snapshot_id = committed
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        ConnectorMetadataMaintenanceReceipt::try_new(
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            summary,
            canonical_json(
                &IcebergMetadataMaintenanceReceiptV1 {
                    version: PAYLOAD_VERSION,
                    snapshot_id,
                    artifact_location: payload.artifact_location.clone(),
                },
                "Iceberg metadata maintenance receipt",
            )
            .map_err(ExecFailure::KnownUncommitted)?,
        )
        .map_err(ExecFailure::KnownUncommitted)
    }
}

impl ConnectorMetadataMaintenance for IcebergMetadataMaintenanceAdapter {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn plan_maintenance(
        &self,
        request: ConnectorMetadataMaintenancePlanningRequest,
    ) -> Result<ConnectorMetadataMaintenancePlan, ConnectorError> {
        request.validate()?;
        self.ensure_owner(request.owner())?;
        let (namespace, table) = decode_base_table_target(request.operation().table())?;
        let operation = match request.operation() {
            ConnectorMetadataMaintenanceOperation::ExpireTableVersions { .. } => {
                CatalogOperation::ExpireSnapshots
            }
            ConnectorMetadataMaintenanceOperation::RewriteMetadataLayout { .. } => {
                CatalogOperation::RewriteManifests
            }
        };
        self.runtime
            .novarocks_catalog()
            .admit(&CatalogAdmissionRequest::new(
                operation,
                CatalogTableName::new(namespace, table),
                request.context.initiation(),
            ))
            .map_err(connector_unsupported)?;
        let mut plans = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg maintenance plan lock: {error}")))?;
        if let Some(existing) = plans.get(&request.operation_id()) {
            if existing.request_digest == request.request_digest() {
                return Ok(existing.plan.clone());
            }
            return Err(invalid(
                "Iceberg metadata maintenance operation was replayed with a different request",
            ));
        }
        let (payload, state_digest, summary) = self.plan_payload(&request)?;
        let provider_payload = canonical_json(&payload, "Iceberg metadata maintenance plan")?;
        let plan = ConnectorMetadataMaintenancePlan::try_new(
            &request,
            state_digest,
            summary,
            provider_payload,
        )?;
        plans.insert(
            request.operation_id(),
            CachedPlan {
                request_digest: request.request_digest(),
                plan: plan.clone(),
                payload,
            },
        );
        Ok(plan)
    }

    fn execute(
        &self,
        request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        if let Some(terminal) = self
            .terminal
            .lock()
            .map_err(|error| internal(format!("Iceberg maintenance terminal lock: {error}")))?
            .get(&request.plan.operation_id())
            .cloned()
        {
            if terminal.plan_digest == request.plan.plan_digest() {
                return Ok(terminal.outcome);
            }
            return Err(invalid(
                "Iceberg metadata maintenance execute conflicts with terminal plan",
            ));
        }
        let payload = self.payload_for_plan(&request.plan)?;
        let marker = self.marker(&request.plan, &payload);
        let outcome = match self.lookup_marker(&payload, &marker, &request.context)? {
            MarkerLookup::Matching { snapshot_id } => ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt_from_marker(&request.plan, &payload, snapshot_id)?,
                finalization: ExternalMutationFinalization::Complete,
            },
            MarkerLookup::Conflicting => ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg maintenance marker conflicts with operation",
                ),
                evidence: self.evidence(&request.plan, &payload)?,
            },
            MarkerLookup::Missing => {
                match self.execute_once(&request.plan, &payload, &marker, &request.context) {
                    Ok(receipt) => ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        receipt,
                        finalization: ExternalMutationFinalization::Complete,
                    },
                    Err(ExecFailure::KnownUncommitted(error)) => {
                        ExternalMutationOutcome::KnownUncommitted {
                            failure: failure(
                                ConnectorMutationFailureKind::Conflict,
                                error.to_string(),
                            ),
                            cleanup:
                                novarocks_spi::connector::ExternalMutationFinalization::Complete,
                        }
                    }
                    Err(ExecFailure::Unknown(error)) => ExternalMutationOutcome::CommitUnknown {
                        failure: failure(
                            ConnectorMutationFailureKind::Unavailable,
                            error.to_string(),
                        ),
                        evidence: self.evidence(&request.plan, &payload)?,
                    },
                }
            }
        };
        self.terminal
            .lock()
            .map_err(|error| internal(format!("Iceberg maintenance terminal lock: {error}")))?
            .insert(
                request.plan.operation_id(),
                TerminalRecord {
                    plan_digest: request.plan.plan_digest(),
                    outcome: outcome.clone(),
                },
            );
        Ok(outcome)
    }

    fn read_max_compactable_data_files(
        &self,
        request: ConnectorMaxCompactableDataFilesRequest,
    ) -> Result<ConnectorMaxCompactableDataFiles, ConnectorError> {
        let (namespace, table_name) = decode_base_table_target(&request.table)?;
        // The observation feeds a maintenance decision, so it must see the
        // current table state rather than a cached generation snapshot.
        self.runtime
            .control_state()
            .invalidate_table_cache(&namespace, &table_name);
        let physical = self
            .runtime
            .load_table_for_request(&namespace, &table_name, &request.context)
            .map_err(|error| unavailable(format!("load Iceberg table for observation: {error}")))?;
        let preserve_row_lineage =
            crate::schema_facts::row_lineage_enabled(physical.table.metadata());
        let table = physical.into_table();
        let stats = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::commit::current_live_data_file_compaction_stats(
                    &table,
                    table.file_io(),
                    preserve_row_lineage,
                )
                .await
            })
            .map_err(unavailable)?
            .map_err(|error| {
                unavailable(format!(
                    "observe Iceberg table {namespace}.{table_name} compaction groups: {error}"
                ))
            })?;
        let value = u64::try_from(stats.max_compactable_data_files).map_err(|_| {
            internal(format!(
                "Iceberg table {namespace}.{table_name} compactable file count overflow"
            ))
        })?;
        Ok(ConnectorMaxCompactableDataFiles::new(Some(value)))
    }
}

impl IcebergMetadataMaintenanceAdapter {
    fn receipt_from_marker(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        snapshot_id: Option<i64>,
    ) -> Result<ConnectorMetadataMaintenanceReceipt, ConnectorError> {
        ConnectorMetadataMaintenanceReceipt::try_new(
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            ConnectorMetadataMaintenanceReceiptSummary::default(),
            canonical_json(
                &IcebergMetadataMaintenanceReceiptV1 {
                    version: PAYLOAD_VERSION,
                    snapshot_id,
                    artifact_location: payload.artifact_location.clone(),
                },
                "Iceberg metadata maintenance receipt",
            )?,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerLookup {
    Matching { snapshot_id: Option<i64> },
    Conflicting,
    Missing,
}

enum ExecFailure {
    KnownUncommitted(ConnectorError),
    Unknown(ConnectorError),
}

fn decode_base_table_target(
    handle: &novarocks_spi::connector::ConnectorTableHandle,
) -> Result<(String, String), ConnectorError> {
    let target: IcebergTablePayload = serde_json::from_slice(handle.payload())
        .map_err(|error| invalid(format!("decode Iceberg maintenance table handle: {error}")))?;
    if target.metadata_table_type.is_some() {
        return Err(invalid(
            "Iceberg metadata maintenance requires a base table handle",
        ));
    }
    Ok((target.namespace, target.table))
}

fn validate_payload_for_plan(
    plan: &ConnectorMetadataMaintenancePlan,
    payload: &IcebergMetadataMaintenancePlanPayloadV1,
) -> Result<(), ConnectorError> {
    if payload.version != PAYLOAD_VERSION
        || payload.operation_kind != plan.operation_kind()
        || payload.metadata_location_digest_hex.len() != 64
        || payload.artifact_location.len()
            > novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_PATH_BYTES
    {
        return Err(invalid(
            "Iceberg metadata maintenance plan payload does not match SPI plan",
        ));
    }
    Ok(())
}

fn validate_frozen_state(
    table: &crate::iceberg::table::Table,
    plan: &ConnectorMetadataMaintenancePlan,
    payload: &IcebergMetadataMaintenancePlanPayloadV1,
) -> Result<(), ConnectorError> {
    let metadata = table.metadata();
    let actual = state_digest(
        metadata.uuid().to_string().as_bytes(),
        table.metadata_location(),
        metadata
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id()),
        metadata.current_schema_id(),
        metadata.default_partition_spec_id(),
    );
    if actual != plan.state_digest()
        || metadata.uuid().to_string() != payload.table_uuid
        || metadata_location_digest(table.metadata_location())
            != decode_digest(&payload.metadata_location_digest_hex)?
    {
        return Err(invalid(
            "Iceberg metadata maintenance plan base state has changed",
        ));
    }
    Ok(())
}

fn state_digest(
    table_uuid: &[u8],
    metadata_location: Option<&str>,
    snapshot_id: Option<i64>,
    schema_id: i32,
    spec_id: i32,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(STATE_DOMAIN);
    digest_bytes(&mut hasher, table_uuid);
    digest_bytes(
        &mut hasher,
        metadata_location.unwrap_or_default().as_bytes(),
    );
    hasher.update(snapshot_id.unwrap_or_default().to_be_bytes());
    hasher.update([u8::from(snapshot_id.is_some())]);
    hasher.update(schema_id.to_be_bytes());
    hasher.update(spec_id.to_be_bytes());
    hasher.finalize().into()
}

fn metadata_location_digest(value: Option<&str>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(STATE_DOMAIN);
    digest_bytes(&mut hasher, value.unwrap_or_default().as_bytes());
    hasher.finalize().into()
}

fn identity_digest(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorProviderBindingKey,
    plan: &ConnectorMetadataMaintenancePlan,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(IDENTITY_DOMAIN);
    digest_bytes(&mut hasher, descriptor.provider_id.as_str().as_bytes());
    digest_bytes(&mut hasher, descriptor.instance_id.as_str().as_bytes());
    hasher.update(key.incarnation.to_bytes());
    hasher.update(plan.operation_id().to_bytes());
    digest_bytes(&mut hasher, plan.operation_kind().as_bytes());
    hasher.update(plan.request_digest());
    hasher.update(plan.plan_digest());
    hasher.finalize().into()
}

fn maintenance_artifact_location(
    metadata_location: Option<&str>,
    operation_kind: &str,
    seed: &str,
) -> Result<String, ConnectorError> {
    let location =
        metadata_location.ok_or_else(|| invalid("Iceberg table has no metadata location"))?;
    let (root, _) = location
        .rsplit_once("/metadata/")
        .ok_or_else(|| invalid("Iceberg metadata location has no table metadata directory"))?;
    let digest = Sha256::digest(seed.as_bytes());
    let path = format!(
        "{root}/_novarocks/maintenance/v1/{operation_kind}/{}/plan.json",
        hex_encode(digest)
    );
    if path.len() > novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_PATH_BYTES {
        return Err(resource_exhausted(
            "Iceberg maintenance artifact path exceeds hard limit",
        ));
    }
    Ok(path)
}

fn classify_iceberg_error(error: crate::iceberg::Error) -> ExecFailure {
    match error.kind() {
        crate::iceberg::ErrorKind::CatalogCommitConflicts
        | crate::iceberg::ErrorKind::PreconditionFailed
        | crate::iceberg::ErrorKind::DataInvalid
        | crate::iceberg::ErrorKind::TableNotFound => {
            ExecFailure::KnownUncommitted(invalid(error.to_string()))
        }
        crate::iceberg::ErrorKind::Unexpected => {
            ExecFailure::Unknown(unavailable(error.to_string()))
        }
        _ => ExecFailure::KnownUncommitted(invalid(error.to_string())),
    }
}

fn canonical_json<T: Serialize>(value: &T, label: &str) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| internal(format!("encode {label}: {error}")))
}

fn decode_canonical_json<T>(payload: &[u8], label: &str) -> Result<T, ConnectorError>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let decoded: T = serde_json::from_slice(payload)
        .map_err(|error| invalid(format!("decode {label}: {error}")))?;
    if canonical_json(&decoded, label)?.as_ref() != payload {
        return Err(invalid(format!("{label} is not canonical JSON v1")));
    }
    Ok(decoded)
}

fn decode_digest(value: &str) -> Result<[u8; 32], ConnectorError> {
    let bytes = hex_decode(value).ok_or_else(|| invalid("maintenance digest is not hex"))?;
    bytes
        .try_into()
        .map_err(|_| invalid("maintenance digest has invalid length"))
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(ALPHABET[(byte >> 4) as usize] as char);
        encoded.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let high = (digits[0] as char).to_digit(16)? as u8;
            let low = (digits[1] as char).to_digit(16)? as u8;
            Some((high << 4) | low)
        })
        .collect()
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn failure(
    kind: ConnectorMutationFailureKind,
    message: impl Into<String>,
) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(kind, message.into())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

fn resource_exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorMetadataMaintenanceExecuteRequest,
        ConnectorMetadataMaintenanceOperation, ConnectorMetadataMaintenancePlanningRequest,
        ConnectorProviderId, ConnectorTableHandle, ProviderBindingEpoch,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;

    fn context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .expect("context")
    }

    fn adapter() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergMetadataMaintenanceAdapter,
        ConnectorProviderBindingKey,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        let key = ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        };
        let adapter =
            IcebergMetadataMaintenanceAdapter::new(key.clone(), runtime).expect("adapter");
        (executor, warehouse, adapter, key)
    }

    fn plan(
        key: ConnectorProviderBindingKey,
        operation_id: ConnectorMutationOperationId,
        table_payload: &'static [u8],
    ) -> ConnectorMetadataMaintenancePlan {
        let table = ConnectorTableHandle::try_new(
            key.instance_id.clone(),
            Bytes::from_static(table_payload),
        )
        .expect("table handle");
        let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
            operation_id,
            key,
            ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(table)
                .expect("operation"),
            context(),
        )
        .expect("planning request");
        ConnectorMetadataMaintenancePlan::try_new(
            &request,
            [3; 32],
            ConnectorMetadataMaintenancePlanSummary::new(1, 2, 3, 4, 5),
            Bytes::from_static(b"provider-plan"),
        )
        .expect("plan")
    }

    fn receipt(
        adapter: &IcebergMetadataMaintenanceAdapter,
        plan: &ConnectorMetadataMaintenancePlan,
    ) -> ConnectorMetadataMaintenanceReceipt {
        ConnectorMetadataMaintenanceReceipt::try_new(
            adapter.descriptor.clone(),
            adapter.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            ConnectorMetadataMaintenanceReceiptSummary::default(),
            Bytes::from_static(b"receipt"),
        )
        .expect("receipt")
    }

    #[test]
    fn maintenance_hex_codec_is_canonical_and_strict() {
        let digest = Sha256::digest(b"maintenance");
        let encoded = hex_encode(digest);
        assert_eq!(encoded.len(), 64);
        assert_eq!(hex_decode(&encoded).as_deref(), Some(digest.as_slice()));
        assert!(hex_decode("0").is_none());
        assert!(hex_decode("xy").is_none());
    }

    #[test]
    fn maintenance_artifact_location_stays_under_table_root() {
        let location = maintenance_artifact_location(
            Some("s3://warehouse/db/table/metadata/v7.metadata.json"),
            "expire-table-versions",
            "operation",
        )
        .expect("artifact location");
        assert!(location.starts_with("s3://warehouse/db/table/_novarocks/maintenance/v1/"));
        assert!(location.ends_with("/plan.json"));
    }

    #[test]
    fn terminal_replay_preserves_failed_finalization() {
        let (_executor, _warehouse, adapter, key) = adapter();
        let plan = plan(key, ConnectorMutationOperationId::new(), b"table-a");
        let expected_receipt = receipt(&adapter, &plan);
        let expected_failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "cleanup response was lost",
        );
        adapter.terminal.lock().expect("terminal").insert(
            plan.operation_id(),
            TerminalRecord {
                plan_digest: plan.plan_digest(),
                outcome: ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: expected_receipt.clone(),
                    finalization: ExternalMutationFinalization::Failed(expected_failure.clone()),
                },
            },
        );

        let outcome = adapter
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(plan, context())
                    .expect("execute request"),
            )
            .expect("terminal replay");
        let ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization: ExternalMutationFinalization::Failed(failure),
            ..
        } = outcome
        else {
            panic!("terminal replay must preserve failed finalization")
        };
        assert_eq!(receipt, expected_receipt);
        assert_eq!(failure.kind(), expected_failure.kind());
        assert_eq!(failure.message(), expected_failure.message());
    }

    #[test]
    fn response_loss_terminal_replay_is_idempotent_and_plan_bound() {
        let (_executor, _warehouse, adapter, key) = adapter();
        let operation_id = ConnectorMutationOperationId::new();
        let exact_plan = plan(key.clone(), operation_id, b"table-a");
        let evidence = ExternalMutationEvidence::try_new(
            PAYLOAD_VERSION,
            adapter.descriptor.clone(),
            adapter.key.incarnation,
            operation_id,
            exact_plan.operation_kind(),
            Bytes::from_static(b"response-loss-evidence"),
        )
        .expect("evidence");
        adapter.terminal.lock().expect("terminal").insert(
            operation_id,
            TerminalRecord {
                plan_digest: exact_plan.plan_digest(),
                outcome: ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "commit response was lost",
                    ),
                    evidence: evidence.clone(),
                },
            },
        );

        for _ in 0..2 {
            let outcome = adapter
                .execute(
                    ConnectorMetadataMaintenanceExecuteRequest::try_new(
                        exact_plan.clone(),
                        context(),
                    )
                    .expect("execute request"),
                )
                .expect("unknown replay");
            let ExternalMutationOutcome::CommitUnknown {
                evidence: replayed, ..
            } = outcome
            else {
                panic!("response-loss replay must remain unknown")
            };
            assert_eq!(replayed.digest(), evidence.digest());
        }

        let conflicting = plan(key, operation_id, b"table-b");
        let error = adapter
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(conflicting, context())
                    .expect("execute request"),
            )
            .expect_err("same operation with a different plan must conflict");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("conflicts with terminal plan"));
    }

    #[test]
    fn foreign_generation_is_rejected_before_catalog_access() {
        let (_executor, _warehouse, adapter, key) = adapter();
        let foreign_key = ConnectorProviderBindingKey {
            instance_id: key.instance_id,
            incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
        };
        let foreign = plan(
            foreign_key,
            ConnectorMutationOperationId::new(),
            b"foreign-table",
        );
        let error = adapter
            .execute(
                ConnectorMetadataMaintenanceExecuteRequest::try_new(foreign, context())
                    .expect("execute request"),
            )
            .expect_err("foreign generation must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("exact connector generation"));
    }

    #[test]
    fn provider_descriptor_remains_exact_for_terminal_receipts() {
        let (_executor, _warehouse, adapter, key) = adapter();
        assert_eq!(
            adapter.descriptor.provider_id,
            ConnectorProviderId::parse("iceberg").expect("provider")
        );
        assert_eq!(adapter.descriptor.instance_id, key.instance_id);
    }

    fn admission_runtime(
        catalog_type: &str,
    ) -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        Arc<IcebergMetadataContext>,
        std::net::TcpListener,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HMS probe listener");
        listener.set_nonblocking(true).expect("nonblocking probe");
        let mut properties = vec![
            ("iceberg.catalog.type".to_string(), catalog_type.to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            ),
        ];
        if catalog_type == "hive" {
            properties.push((
                "hive.metastore.uris".to_string(),
                format!(
                    "thrift://{}",
                    listener.local_addr().expect("listener address")
                ),
            ));
        }
        let configuration = crate::catalog_config::parse_catalog_configuration("ice", &properties)
            .expect("configuration");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                crate::resources::IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        (executor, warehouse, runtime, listener)
    }

    fn admission_context(catalog_type: &str) -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .expect("context")
        .with_initiation(if catalog_type == "hive" {
            novarocks_spi::connector::ConnectorRequestInitiation::Statement
        } else {
            novarocks_spi::connector::ConnectorRequestInitiation::Background
        })
    }

    fn admission_table(
        instance: novarocks_spi::connector::ConnectorInstanceId,
    ) -> ConnectorTableHandle {
        ConnectorTableHandle::try_new(instance, Bytes::from_static(br#"{"namespace":"db","table":"absent","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#)).expect("table handle")
    }

    fn assert_admission_left_no_io(
        warehouse: &tempfile::TempDir,
        listener: &std::net::TcpListener,
    ) {
        assert_eq!(
            std::fs::read_dir(warehouse.path())
                .expect("warehouse inventory")
                .count(),
            0
        );
        let error = listener
            .accept()
            .expect_err("admission must not connect to HMS");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn planning_refuses_hms_and_background_hadoop_before_maintenance_artifacts() {
        for catalog_type in ["hive", "hadoop"] {
            let (_executor, warehouse, runtime, listener) = admission_runtime(catalog_type);
            let key = ConnectorProviderBindingKey {
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
                incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
            };
            let adapter =
                IcebergMetadataMaintenanceAdapter::new(key.clone(), runtime).expect("adapter");
            for operation in [
                ConnectorMetadataMaintenanceOperation::rewrite_metadata_layout(admission_table(
                    key.instance_id.clone(),
                ))
                .expect("rewrite manifests"),
                ConnectorMetadataMaintenanceOperation::expire_table_versions(
                    admission_table(key.instance_id.clone()),
                    None,
                    Some(1),
                )
                .expect("expire snapshots"),
            ] {
                let request = ConnectorMetadataMaintenancePlanningRequest::try_new(
                    ConnectorMutationOperationId::new(),
                    key.clone(),
                    operation,
                    admission_context(catalog_type),
                )
                .expect("request");
                let error = adapter
                    .plan_maintenance(request)
                    .expect_err("catalog admission must refuse planning");
                assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
                assert!(error.to_string().contains(if catalog_type == "hive" {
                    "read-only compatibility entry"
                } else {
                    "background"
                }));
                assert!(adapter.plans.lock().expect("plans").is_empty());
                assert_admission_left_no_io(&warehouse, &listener);
            }
        }
    }
}
