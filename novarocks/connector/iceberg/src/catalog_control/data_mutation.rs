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

//! Generation-local Iceberg implementation of the connector data-mutation contract.
//!
//! Planning, commit dispatch, and reconciliation use only the exact provider
//! runtime supplied by the control factory. No catalog-name registry or
//! process-global async runtime participates in this path.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::iceberg::{NamespaceIdent, TableIdent};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorDataMutation, ConnectorDataMutationExecuteRequest, ConnectorDataMutationOperation,
    ConnectorDataMutationPlan, ConnectorDataMutationPlanSummary,
    ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
    ConnectorDataMutationReconcileRequest, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorOperationControl, ConnectorProviderBindingKey,
    ConnectorRequestContext, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use super::add_files::{
    AddFilesManifest, plan_manifest_for_table, preflight_caller_managed_source_domain,
    revalidate_manifest_for_table,
};
use crate::catalog::CatalogTableName;
use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};
use crate::commit::model::{OperationToken, StartSnapshot};
use crate::commit::recovery::FrozenPublicationFacts;
use crate::iceberg::spec::TableMetadata;
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;

#[path = "data_mutation/publication.rs"]
mod publication;

const PLAN_PAYLOAD_VERSION: u16 = 2;
const RECEIPT_PAYLOAD_VERSION: u16 = 1;
const EVIDENCE_PAYLOAD_VERSION: u16 = 2;
const MARKER_VALUE_VERSION: u16 = 1;
const TRUNCATE_OPERATION_KIND: &str = "truncate";
const MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES: usize = 16 * 1024;
pub(crate) const MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES: usize =
    MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES / 2;
const MAX_DURABLE_ICEBERG_TRUNCATE_RECEIPT_PROVIDER_PAYLOAD_BYTES: usize = 64;
const MARKER_PROPERTY: &str = "novarocks.connector.data-mutation.v1";
const IDENTITY_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.data-mutation.identity.v1\0";
const TRUNCATE_STATE_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.data-mutation.truncate-state.v1\0";
const METADATA_VERSION_DIGEST_DOMAIN: &[u8] =
    b"novarocks.iceberg.data-mutation.metadata-version.v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationPlanPayloadV2 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: Option<i64>,
    schema_id: i32,
    default_spec_id: i32,
    metadata_version_digest_hex: String,
    source_location: Option<String>,
    name_mapping_digest_hex: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationReceiptV1 {
    version: u16,
    snapshot_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationEvidenceV2 {
    version: u16,
    namespace: String,
    table: String,
    target_ref: String,
    table_uuid: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: Option<i64>,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    identity_digest_hex: String,
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
    publication: RecoveryPublication,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum RecoveryPublication {
    MarkerOnly {},
    Dispatched { facts: FrozenPublicationFacts },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationMarkerV1 {
    version: u16,
    identity_digest_hex: String,
    incarnation_hex: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
}

#[derive(Clone)]
enum PlannedIcebergMutation {
    RegisterExistingFiles {
        payload: IcebergDataMutationPlanPayloadV2,
        source_metadata: TableMetadata,
        manifest: AddFilesManifest,
        domain: novarocks_spi::connector::ConnectorDataMutationAddFilesDomain,
    },
    Truncate {
        payload: IcebergDataMutationPlanPayloadV2,
        source_metadata: TableMetadata,
    },
}

impl PlannedIcebergMutation {
    fn payload(&self) -> &IcebergDataMutationPlanPayloadV2 {
        match self {
            Self::RegisterExistingFiles { payload, .. } | Self::Truncate { payload, .. } => payload,
        }
    }

    fn source_metadata(&self) -> &TableMetadata {
        match self {
            Self::RegisterExistingFiles {
                source_metadata, ..
            }
            | Self::Truncate {
                source_metadata, ..
            } => source_metadata,
        }
    }
}

#[derive(Clone)]
struct CachedPlan {
    request_digest: [u8; 32],
    plan: ConnectorDataMutationPlan,
    private: PlannedIcebergMutation,
}

#[derive(Clone)]
struct TerminalRecord {
    plan_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorDataMutationReceipt>,
}

#[derive(Clone)]
struct MutationRecoveryTemplate {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorProviderBindingKey,
    plan: ConnectorDataMutationPlan,
    payload: IcebergDataMutationPlanPayloadV2,
}

impl MutationRecoveryTemplate {
    fn new(
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorProviderBindingKey,
        plan: ConnectorDataMutationPlan,
        payload: IcebergDataMutationPlanPayloadV2,
    ) -> Self {
        Self {
            descriptor,
            key,
            plan,
            payload,
        }
    }

    fn receipt(&self, snapshot_id: i64) -> Result<ConnectorDataMutationReceipt, ConnectorError> {
        ConnectorDataMutationReceipt::try_new(
            self.descriptor.clone(),
            self.key.incarnation,
            self.plan.operation_id(),
            self.plan.operation_kind(),
            self.plan.request_digest(),
            self.plan.plan_digest(),
            self.plan.state_digest(),
            self.plan.summary(),
            durable_receipt_payload(snapshot_id)?,
        )
    }

    fn evidence(
        &self,
        publication: RecoveryPublication,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let summary = self.plan.summary();
        let payload = IcebergDataMutationEvidenceV2 {
            version: EVIDENCE_PAYLOAD_VERSION,
            namespace: self.payload.namespace.clone(),
            table: self.payload.table.clone(),
            target_ref: self.payload.target_ref.clone(),
            table_uuid: self.payload.table_uuid.clone(),
            base_snapshot_id: self.payload.base_snapshot_id,
            base_sequence_number: self.payload.base_sequence_number,
            operation_id_hex: hex_encode(self.plan.operation_id().to_bytes()),
            operation_kind: self.plan.operation_kind().into(),
            request_digest_hex: hex_encode(self.plan.request_digest()),
            plan_digest_hex: hex_encode(self.plan.plan_digest()),
            state_digest_hex: hex_encode(self.plan.state_digest()),
            identity_digest_hex: hex_encode(identity_digest(
                &self.descriptor,
                &self.key,
                &self.plan,
            )),
            file_count: summary.file_count(),
            row_count: summary.row_count(),
            total_bytes: summary.total_bytes(),
            publication,
        };
        validate_recovery_facts(&payload, self.plan.operation_id())?;
        let evidence = ExternalMutationEvidence::try_new(
            EVIDENCE_PAYLOAD_VERSION,
            self.descriptor.clone(),
            self.key.incarnation,
            self.plan.operation_id(),
            self.plan.operation_kind(),
            canonical_json(&payload, "Iceberg data mutation evidence")?,
        )?;
        validate_durable_evidence(&evidence)?;
        Ok(evidence)
    }
}

fn validate_durable_evidence(evidence: &ExternalMutationEvidence) -> Result<(), ConnectorError> {
    if evidence.operation_kind() == TRUNCATE_OPERATION_KIND
        && evidence.try_to_wire_v1()?.len() > MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!(
                "Iceberg TRUNCATE evidence wire exceeds durable {} byte cap for a {} byte lowercase-hex journal field",
                MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES,
                MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES
            ),
        ));
    }
    Ok(())
}

fn validate_recovery_facts(
    evidence: &IcebergDataMutationEvidenceV2,
    operation: ConnectorMutationOperationId,
) -> Result<(), ConnectorError> {
    let uuid = uuid::Uuid::parse_str(&evidence.table_uuid)
        .map_err(|_| invalid("Iceberg mutation recovery has an invalid table UUID"))?;
    if evidence.base_snapshot_id.is_some() != evidence.base_sequence_number.is_some()
        || evidence
            .base_sequence_number
            .is_some_and(|sequence| sequence < 0)
        || evidence.namespace.is_empty()
        || evidence.table.is_empty()
        || evidence.target_ref.is_empty()
    {
        return Err(invalid(
            "Iceberg mutation recovery has inconsistent source or target facts",
        ));
    }
    if let RecoveryPublication::Dispatched { facts } = &evidence.publication {
        let ident = TableIdent::new(
            NamespaceIdent::new(evidence.namespace.clone()),
            evidence.table.clone(),
        );
        facts
            .validate_existing_target(
                OperationToken::from_mutation(operation),
                &ident,
                uuid,
                &evidence.target_ref,
                crate::commit::model::RequestShape::SnapshotProducing,
            )
            .map_err(|error| invalid(error.to_string()))?;
        facts
            .validate_no_session_data()
            .map_err(|error| invalid(error.to_string()))?;
    }
    Ok(())
}

trait IcebergDataMutationBackend: Send + Sync {
    fn admit(&self, request: &ConnectorDataMutationPlanningRequest) -> Result<(), ConnectorError>;

    fn plan(
        &self,
        request: &ConnectorDataMutationPlanningRequest,
    ) -> Result<
        (
            PlannedIcebergMutation,
            [u8; 32],
            ConnectorDataMutationPlanSummary,
        ),
        ConnectorError,
    >;

    #[allow(clippy::result_large_err)]
    fn execute(
        &self,
        planned: &PlannedIcebergMutation,
        marker: &IcebergDataMutationMarkerV1,
        recovery: &MutationRecoveryTemplate,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError>;

    fn lookup_marker(
        &self,
        namespace: &str,
        table: &str,
        target_ref: &str,
        operation_id_hex: &str,
        identity_digest_hex: &str,
        context: &ConnectorRequestContext,
    ) -> Result<MarkerLookup, ConnectorError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerLookup {
    Matching { snapshot_id: i64 },
    Conflicting,
    Missing,
}

struct RegisteredIcebergDataMutationBackend {
    provider: Arc<IcebergMetadata>,
    runtime: Arc<IcebergMetadataContext>,
}

impl RegisteredIcebergDataMutationBackend {
    fn new(provider: Arc<IcebergMetadata>) -> Self {
        Self {
            runtime: Arc::clone(provider.runtime()),
            provider,
        }
    }

    fn reload_table(
        &self,
        namespace: &str,
        table: &str,
        request_context: &ConnectorRequestContext,
    ) -> Result<crate::iceberg::table::Table, ConnectorError> {
        self.runtime
            .control_state()
            .invalidate_table_cache(namespace, table);
        self.runtime
            .load_table_for_request(namespace, table, request_context)
            .map(|loaded| loaded.into_table())
            .map_err(map_provider_error)
    }
}

impl IcebergDataMutationBackend for RegisteredIcebergDataMutationBackend {
    fn admit(&self, request: &ConnectorDataMutationPlanningRequest) -> Result<(), ConnectorError> {
        let target = self.provider.table_payload(request.operation().table())?;
        if target.metadata_table_type.is_some() {
            return Err(invalid(
                "Iceberg data mutation requires a base table handle",
            ));
        }
        let operation = match request.operation() {
            ConnectorDataMutationOperation::Truncate { .. } => CatalogOperation::Truncate,
            ConnectorDataMutationOperation::RegisterExistingFiles { .. } => {
                CatalogOperation::RegisterFiles
            }
        };
        self.runtime
            .novarocks_catalog()
            .admit(&CatalogAdmissionRequest::new(
                operation,
                CatalogTableName::new(target.namespace, target.table),
                request.context.initiation(),
            ))
            .map_err(connector_unsupported)?;
        Ok(())
    }

    fn plan(
        &self,
        request: &ConnectorDataMutationPlanningRequest,
    ) -> Result<
        (
            PlannedIcebergMutation,
            [u8; 32],
            ConnectorDataMutationPlanSummary,
        ),
        ConnectorError,
    > {
        let table_payload = self.provider.table_payload(request.operation().table())?;
        if table_payload.metadata_table_type.is_some() {
            return Err(invalid(
                "Iceberg data mutation requires a base table handle",
            ));
        }
        let namespace = table_payload.namespace;
        let table_name = table_payload.table;
        let table = self.reload_table(&namespace, &table_name, &request.context)?;
        let binding = self
            .runtime
            .resources()
            .planning_binding()
            .for_request(request.context.clone());
        let metadata = table.metadata();
        let table_uuid = metadata.uuid().to_string();
        let schema_id = metadata.current_schema_id();
        let default_spec_id = metadata.default_partition_spec_id();
        let metadata_version_digest = metadata_version_digest(table.metadata_location());

        match request.operation() {
            ConnectorDataMutationOperation::RegisterExistingFiles {
                source_location, ..
            } => {
                let domain = preflight_caller_managed_source_domain(
                    source_location,
                    &self.runtime.control_state().configuration().warehouse_uri,
                    table.metadata().location(),
                    &binding,
                )
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unsupported, error))?;
                let manifest = plan_manifest_for_table(
                    &table,
                    source_location,
                    &binding,
                    self.runtime.resources().catalog_runtime(),
                    self.runtime.novarocks_catalog().listing_admission(),
                )?;
                let mapping_digest = manifest
                    .canonical_name_mapping
                    .as_deref()
                    .map(|mapping| hex_encode(Sha256::digest(mapping.as_bytes())));
                let payload = IcebergDataMutationPlanPayloadV2 {
                    version: PLAN_PAYLOAD_VERSION,
                    namespace,
                    table: table_name,
                    table_uuid,
                    target_ref: "main".to_string(),
                    base_snapshot_id: target_snapshot_id(metadata, "main")?,
                    base_sequence_number: source_snapshot(metadata, "main")?
                        .map(|s| s.sequence_number),
                    schema_id,
                    default_spec_id,
                    metadata_version_digest_hex: hex_encode(metadata_version_digest),
                    source_location: Some(source_location.to_string()),
                    name_mapping_digest_hex: mapping_digest,
                };
                let summary = ConnectorDataMutationPlanSummary::try_new(
                    u32::try_from(manifest.records.len()).map_err(|_| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "ADD FILES manifest count exceeds u32",
                        )
                    })?,
                    manifest.total_rows,
                    manifest.total_bytes,
                )?;
                Ok((
                    PlannedIcebergMutation::RegisterExistingFiles {
                        payload,
                        source_metadata: metadata.clone(),
                        manifest: manifest.clone(),
                        domain,
                    },
                    manifest.digest,
                    summary,
                ))
            }
            ConnectorDataMutationOperation::Truncate { target_ref, .. } => {
                if target_ref.as_ref() != "main"
                    && metadata.format_version() != crate::iceberg::spec::FormatVersion::V3
                {
                    return Err(invalid(
                        "Iceberg branch TRUNCATE requires a format-v3 table",
                    ));
                }
                let base_snapshot_id = target_snapshot_id(metadata, target_ref)?;
                let payload = IcebergDataMutationPlanPayloadV2 {
                    version: PLAN_PAYLOAD_VERSION,
                    namespace,
                    table: table_name,
                    table_uuid,
                    target_ref: target_ref.to_string(),
                    base_snapshot_id,
                    base_sequence_number: source_snapshot(metadata, target_ref)?
                        .map(|s| s.sequence_number),
                    schema_id,
                    default_spec_id,
                    metadata_version_digest_hex: hex_encode(metadata_version_digest),
                    source_location: None,
                    name_mapping_digest_hex: None,
                };
                let state_digest = truncate_state_digest(&payload);
                Ok((
                    PlannedIcebergMutation::Truncate {
                        payload,
                        source_metadata: metadata.clone(),
                    },
                    state_digest,
                    ConnectorDataMutationPlanSummary::default(),
                ))
            }
        }
    }

    fn execute(
        &self,
        planned: &PlannedIcebergMutation,
        marker: &IcebergDataMutationMarkerV1,
        recovery: &MutationRecoveryTemplate,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        self.execute_publication(planned, marker, recovery, context)
    }

    fn lookup_marker(
        &self,
        namespace: &str,
        table: &str,
        target_ref: &str,
        operation_id_hex: &str,
        identity_digest_hex: &str,
        context: &ConnectorRequestContext,
    ) -> Result<MarkerLookup, ConnectorError> {
        let table = self.reload_table(namespace, table, context)?;
        let metadata = table.metadata();
        let target_snapshot = target_snapshot_id(metadata, target_ref)?;
        let mut by_id = HashMap::new();
        for snapshot in metadata.snapshots() {
            by_id.insert(snapshot.snapshot_id(), snapshot);
        }
        let mut cursor = target_snapshot;
        let mut visited = HashSet::new();
        while let Some(snapshot_id) = cursor {
            if !visited.insert(snapshot_id) {
                return Err(corrupt("Iceberg snapshot ancestry contains a cycle"));
            }
            let Some(snapshot) = by_id.get(&snapshot_id) else {
                break;
            };
            if let Some(raw) = snapshot
                .summary()
                .additional_properties
                .get(MARKER_PROPERTY)
            {
                let marker: IcebergDataMutationMarkerV1 =
                    decode_canonical_json(raw.as_bytes(), "Iceberg data mutation marker")?;
                if marker.operation_id_hex == operation_id_hex {
                    return Ok(if marker.identity_digest_hex == identity_digest_hex {
                        MarkerLookup::Matching { snapshot_id }
                    } else {
                        MarkerLookup::Conflicting
                    });
                }
            }
            cursor = snapshot.parent_snapshot_id();
        }
        Ok(MarkerLookup::Missing)
    }
}

pub struct IcebergDataMutationAdapter {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    backend: Arc<dyn IcebergDataMutationBackend>,
    plans: Mutex<HashMap<ConnectorMutationOperationId, CachedPlan>>,
    terminal: Mutex<HashMap<ConnectorMutationOperationId, TerminalRecord>>,
}

impl IcebergDataMutationAdapter {
    pub(crate) fn try_new(provider: Arc<IcebergMetadata>) -> Result<Self, ConnectorError> {
        let key = ConnectorProviderBindingKey {
            instance_id: provider.descriptor().instance_id.clone(),
            incarnation: provider.incarnation(),
        };
        Self::new_with_backend(
            key,
            Arc::new(RegisteredIcebergDataMutationBackend::new(provider)),
        )
    }

    fn new_with_backend(
        key: ConnectorProviderBindingKey,
        backend: Arc<dyn IcebergDataMutationBackend>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        Ok(Self {
            key,
            descriptor,
            backend,
            plans: Mutex::new(HashMap::new()),
            terminal: Mutex::new(HashMap::new()),
        })
    }

    fn ensure_owner(&self, owner: &ConnectorProviderBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg data mutation does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    fn marker(
        &self,
        plan: &ConnectorDataMutationPlan,
        payload: &IcebergDataMutationPlanPayloadV2,
    ) -> IcebergDataMutationMarkerV1 {
        let summary = plan.summary();
        IcebergDataMutationMarkerV1 {
            version: MARKER_VALUE_VERSION,
            identity_digest_hex: hex_encode(identity_digest(&self.descriptor, &self.key, plan)),
            incarnation_hex: hex_encode(self.key.incarnation.to_bytes()),
            operation_id_hex: hex_encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex_encode(plan.request_digest()),
            plan_digest_hex: hex_encode(plan.plan_digest()),
            state_digest_hex: hex_encode(plan.state_digest()),
            target_ref: payload.target_ref.clone(),
            base_snapshot_id: payload.base_snapshot_id,
            file_count: summary.file_count(),
            row_count: summary.row_count(),
            total_bytes: summary.total_bytes(),
        }
    }

    fn receipt(
        &self,
        plan: &ConnectorDataMutationPlan,
        snapshot_id: i64,
    ) -> Result<ConnectorDataMutationReceipt, ConnectorError> {
        ConnectorDataMutationReceipt::try_new(
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            plan.summary(),
            durable_receipt_payload(snapshot_id)?,
        )
    }

    fn evidence(
        &self,
        plan: &ConnectorDataMutationPlan,
        payload: &IcebergDataMutationPlanPayloadV2,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        MutationRecoveryTemplate::new(
            self.descriptor.clone(),
            self.key.clone(),
            plan.clone(),
            payload.clone(),
        )
        .evidence(RecoveryPublication::MarkerOnly {})
    }

    fn preflight_durable_truncate_evidence(
        &self,
        plan: &ConnectorDataMutationPlan,
        payload: &IcebergDataMutationPlanPayloadV2,
    ) -> Result<(), ConnectorError> {
        validate_durable_evidence(&self.evidence(plan, payload)?)
    }

    fn committed(
        &self,
        plan: &ConnectorDataMutationPlan,
        snapshot_id: i64,
        finalization: ExternalMutationFinalization,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: self.receipt(plan, snapshot_id)?,
            finalization,
        })
    }

    fn committed_from_reconcile(
        &self,
        request: &ConnectorDataMutationReconcileRequest,
        evidence: &IcebergDataMutationEvidenceV2,
        snapshot_id: i64,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        let summary = ConnectorDataMutationPlanSummary::try_new(
            evidence.file_count,
            evidence.row_count,
            evidence.total_bytes,
        )?;
        let receipt = ConnectorDataMutationReceipt::try_new(
            self.descriptor.clone(),
            self.key.incarnation,
            request.operation_id,
            request.operation_kind.clone(),
            request.request_digest,
            request.plan_digest,
            request.state_digest,
            summary,
            durable_receipt_payload(snapshot_id)?,
        )?;
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        })
    }
}

impl ConnectorDataMutation for IcebergDataMutationAdapter {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn plan_mutation(
        &self,
        request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        request.validate()?;
        self.ensure_owner(request.owner())?;
        self.backend.admit(&request)?;
        let mut plans = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation plan lock: {error}")))?;
        if let Some(cached) = plans.get(&request.operation_id()) {
            if cached.request_digest == request.request_digest() {
                return Ok(cached.plan.clone());
            }
            return Err(invalid(
                "Iceberg data mutation operation was replayed with a different request",
            ));
        }
        let (private, state_digest, summary) = self.backend.plan(&request)?;
        let provider_payload = canonical_json(private.payload(), "Iceberg data mutation plan")?;
        let source_scope = match &private {
            PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } => {
                Some(manifest.source_scope)
            }
            PlannedIcebergMutation::Truncate { .. } => None,
        };
        let add_files_domain = match &private {
            PlannedIcebergMutation::RegisterExistingFiles { domain, .. } => Some(*domain),
            PlannedIcebergMutation::Truncate { .. } => None,
        };
        let plan = ConnectorDataMutationPlan::try_new(
            &request,
            state_digest,
            summary,
            source_scope,
            add_files_domain,
            provider_payload,
        )?;
        self.preflight_durable_truncate_evidence(&plan, private.payload())?;
        plans.insert(
            request.operation_id(),
            CachedPlan {
                request_digest: request.request_digest(),
                plan: plan.clone(),
                private,
            },
        );
        Ok(plan)
    }

    fn execute(
        &self,
        request: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        if let Some(record) = self
            .terminal
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation terminal lock: {error}")))?
            .get(&request.plan.operation_id())
            .cloned()
        {
            if record.plan_digest == request.plan.plan_digest() {
                return Ok(record.outcome);
            }
            return Err(invalid(
                "Iceberg data mutation operation was executed with a different plan",
            ));
        }
        let cached = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation plan lock: {error}")))?
            .get(&request.plan.operation_id())
            .cloned()
            .ok_or_else(|| invalid("Iceberg data mutation plan is not registered"))?;
        if cached.plan.plan_digest() != request.plan.plan_digest() {
            return Err(invalid(
                "Iceberg data mutation execute request conflicts with the planned operation",
            ));
        }
        let marker = self.marker(&request.plan, cached.private.payload());
        let outcome = match self.backend.lookup_marker(
            &marker_target(&cached.private).0,
            &marker_target(&cached.private).1,
            &marker.target_ref,
            &marker.operation_id_hex,
            &marker.identity_digest_hex,
            &request.context,
        ) {
            Ok(MarkerLookup::Matching { snapshot_id }) => self.committed(
                &request.plan,
                snapshot_id,
                ExternalMutationFinalization::Complete,
            )?,
            Ok(MarkerLookup::Conflicting) => ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg data mutation marker conflicts with this operation",
                ),
                evidence: self.evidence(&request.plan, cached.private.payload())?,
            },
            Ok(MarkerLookup::Missing) => self.backend.execute(
                &cached.private,
                &marker,
                &MutationRecoveryTemplate::new(
                    self.descriptor.clone(),
                    self.key.clone(),
                    request.plan.clone(),
                    cached.private.payload().clone(),
                ),
                &request.context,
            )?,
            Err(error) => ExternalMutationOutcome::KnownUncommitted {
                failure: crate::commit::attempt::before_dispatch_failure(
                    &publication::format_error(error),
                ),
                cleanup: ExternalMutationFinalization::Complete,
            },
        };
        self.terminal
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation terminal lock: {error}")))?
            .insert(
                request.plan.operation_id(),
                TerminalRecord {
                    plan_digest: request.plan.plan_digest(),
                    outcome: outcome.clone(),
                },
            );
        Ok(outcome)
    }

    fn reconcile(
        &self,
        request: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        self.ensure_owner(&request.owner)?;
        let evidence: IcebergDataMutationEvidenceV2 = decode_canonical_json(
            request.evidence.provider_payload(),
            "Iceberg data mutation evidence",
        )?;
        validate_evidence_request(&self.descriptor, &self.key, &request, &evidence)?;
        match self.backend.lookup_marker(
            &evidence.namespace,
            &evidence.table,
            &evidence.target_ref,
            &evidence.operation_id_hex,
            &evidence.identity_digest_hex,
            &request.context,
        )? {
            MarkerLookup::Matching { snapshot_id } => {
                self.committed_from_reconcile(&request, &evidence, snapshot_id)
            }
            MarkerLookup::Conflicting => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg data mutation marker conflicts with reconciliation evidence",
                ),
                evidence: request.evidence,
            }),
            MarkerLookup::Missing => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Unavailable,
                    "Iceberg data mutation marker is not yet visible",
                ),
                evidence: request.evidence,
            }),
        }
    }
}

fn durable_receipt_payload(snapshot_id: i64) -> Result<Bytes, ConnectorError> {
    let payload = canonical_json(
        &IcebergDataMutationReceiptV1 {
            version: RECEIPT_PAYLOAD_VERSION,
            snapshot_id,
        },
        "Iceberg data mutation receipt",
    )?;
    if payload.len() > MAX_DURABLE_ICEBERG_TRUNCATE_RECEIPT_PROVIDER_PAYLOAD_BYTES {
        return Err(internal(format!(
            "Iceberg TRUNCATE receipt provider payload exceeds fixed {} byte durable bound",
            MAX_DURABLE_ICEBERG_TRUNCATE_RECEIPT_PROVIDER_PAYLOAD_BYTES
        )));
    }
    Ok(payload)
}

fn marker_target(planned: &PlannedIcebergMutation) -> (String, String) {
    let payload = planned.payload();
    (payload.namespace.clone(), payload.table.clone())
}

fn validate_evidence_request(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorProviderBindingKey,
    request: &ConnectorDataMutationReconcileRequest,
    evidence: &IcebergDataMutationEvidenceV2,
) -> Result<(), ConnectorError> {
    if request.evidence.schema_version() != EVIDENCE_PAYLOAD_VERSION
        || request.evidence.descriptor() != descriptor
        || request.evidence.incarnation() != key.incarnation
        || request.evidence.operation_id() != request.operation_id
        || request.evidence.operation_kind() != request.operation_kind.as_ref()
        || evidence.version != EVIDENCE_PAYLOAD_VERSION
        || evidence.operation_id_hex != hex_encode(request.operation_id.to_bytes())
        || evidence.operation_kind != request.operation_kind.as_ref()
        || evidence.request_digest_hex != hex_encode(request.request_digest)
        || evidence.plan_digest_hex != hex_encode(request.plan_digest)
        || evidence.state_digest_hex != hex_encode(request.state_digest)
        || evidence.identity_digest_hex
            != hex_encode(identity_digest_parts(
                descriptor,
                key,
                request.operation_id,
                &request.operation_kind,
                request.request_digest,
                request.plan_digest,
                request.state_digest,
            ))
    {
        return Err(invalid(
            "Iceberg data mutation evidence does not match its reconcile request",
        ));
    }
    validate_durable_evidence(&request.evidence)?;
    validate_recovery_facts(evidence, request.operation_id)
}

/// Fail closed unless the table is still the exact base state this plan froze.
fn validate_frozen_table(
    table: &crate::iceberg::table::Table,
    payload: &IcebergDataMutationPlanPayloadV2,
) -> Result<(), ConnectorError> {
    let metadata = table.metadata();
    if metadata.uuid().to_string() != payload.table_uuid
        || metadata.current_schema_id() != payload.schema_id
        || metadata.default_partition_spec_id() != payload.default_spec_id
        || target_snapshot_id(metadata, &payload.target_ref)? != payload.base_snapshot_id
    {
        return Err(conflict(
            "Iceberg data mutation table state advanced after planning",
        ));
    }
    if hex_encode(metadata_version_digest(table.metadata_location()))
        != payload.metadata_version_digest_hex
    {
        return Err(conflict(
            "Iceberg data mutation table state advanced after planning",
        ));
    }
    Ok(())
}

/// ADD FILES intentionally permits a data-ref OCC refresh. Its immutable
/// contract is table identity/schema/spec plus the complete frozen manifest;
/// the attempt guard re-runs the latter on every refreshed base.
fn validate_add_files_target_shape(
    table: &crate::iceberg::table::Table,
    payload: &IcebergDataMutationPlanPayloadV2,
) -> Result<(), ConnectorError> {
    let metadata = table.metadata();
    if metadata.uuid().to_string() != payload.table_uuid
        || metadata.current_schema_id() != payload.schema_id
        || metadata.default_partition_spec_id() != payload.default_spec_id
    {
        return Err(conflict(
            "Iceberg ADD FILES target identity, schema, or partition spec changed after planning",
        ));
    }
    Ok(())
}

fn anchor_committed_write(
    runtime: &IcebergMetadataContext,
    table: &crate::iceberg::table::Table,
) -> Result<(), ConnectorError> {
    let metadata_location = table
        .metadata_location()
        .ok_or_else(|| corrupt("Iceberg table has no metadata location"))?
        .to_string();
    let ident = table.identifier();
    let name = crate::catalog::CatalogTableName::new(
        ident.namespace().to_url_string(),
        ident.name().to_string(),
    );
    let catalog = Arc::clone(runtime.novarocks_catalog());
    let outcome = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            catalog
                .anchor_written_metadata(name, Arc::from(metadata_location))
                .await
        })
        .map_err(|error| internal(format!("Iceberg catalog runtime bridge: {error}")))?;
    match outcome {
        crate::catalog::error::CatalogOutcome::KnownCommitted { .. } => Ok(()),
        crate::catalog::error::CatalogOutcome::Unsupported(reason) => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            reason.message().to_string(),
        )),
        crate::catalog::error::CatalogOutcome::KnownUncommitted { failure } => {
            Err(map_provider_error(failure.to_string()))
        }
        crate::catalog::error::CatalogOutcome::CommitUnknown { failure, .. } => Err(
            ConnectorError::new(ConnectorErrorKind::Unavailable, failure.to_string()),
        ),
    }
}

fn target_snapshot_id(
    metadata: &crate::iceberg::spec::TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, ConnectorError> {
    if target_ref == "main" {
        return Ok(metadata
            .refs()
            .get("main")
            .map(|reference| reference.snapshot_id)
            .or_else(|| metadata.current_snapshot_id()));
    }
    metadata
        .refs()
        .get(target_ref)
        .map(|reference| Some(reference.snapshot_id))
        .ok_or_else(|| ConnectorError::new(ConnectorErrorKind::NotFound, "Iceberg ref not found"))
}

fn identity_digest(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorProviderBindingKey,
    plan: &ConnectorDataMutationPlan,
) -> [u8; 32] {
    identity_digest_parts(
        descriptor,
        key,
        plan.operation_id(),
        plan.operation_kind(),
        plan.request_digest(),
        plan.plan_digest(),
        plan.state_digest(),
    )
}

fn identity_digest_parts(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorProviderBindingKey,
    operation_id: ConnectorMutationOperationId,
    operation_kind: &str,
    request_digest: [u8; 32],
    plan_digest: [u8; 32],
    state_digest: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(IDENTITY_DIGEST_DOMAIN);
    digest_bytes(&mut hasher, descriptor.provider_id.as_str().as_bytes());
    digest_bytes(&mut hasher, descriptor.instance_id.as_str().as_bytes());
    digest_bytes(&mut hasher, &key.incarnation.to_bytes());
    digest_bytes(&mut hasher, &operation_id.to_bytes());
    digest_bytes(&mut hasher, operation_kind.as_bytes());
    digest_bytes(&mut hasher, &request_digest);
    digest_bytes(&mut hasher, &plan_digest);
    digest_bytes(&mut hasher, &state_digest);
    hasher.finalize().into()
}

fn source_snapshot(
    metadata: &TableMetadata,
    target_ref: &str,
) -> Result<Option<StartSnapshot>, ConnectorError> {
    target_snapshot_id(metadata, target_ref)?
        .map(|id| {
            let snapshot = metadata
                .snapshot_by_id(id)
                .ok_or_else(|| corrupt("Data mutation source ref snapshot is absent"))?;
            Ok(StartSnapshot {
                snapshot_id: id,
                sequence_number: snapshot.sequence_number(),
            })
        })
        .transpose()
}

fn truncate_state_digest(payload: &IcebergDataMutationPlanPayloadV2) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(TRUNCATE_STATE_DIGEST_DOMAIN);
    digest_bytes(&mut hasher, payload.table_uuid.as_bytes());
    digest_bytes(&mut hasher, payload.target_ref.as_bytes());
    digest_bytes(
        &mut hasher,
        &payload.base_snapshot_id.unwrap_or_default().to_be_bytes(),
    );
    digest_bytes(&mut hasher, &payload.schema_id.to_be_bytes());
    digest_bytes(&mut hasher, &payload.default_spec_id.to_be_bytes());
    digest_bytes(&mut hasher, payload.metadata_version_digest_hex.as_bytes());
    hasher.finalize().into()
}

fn metadata_version_digest(metadata_location: Option<&str>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(METADATA_VERSION_DIGEST_DOMAIN);
    digest_bytes(
        &mut hasher,
        metadata_location.unwrap_or_default().as_bytes(),
    );
    hasher.finalize().into()
}

fn digest_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
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

fn failure(
    kind: ConnectorMutationFailureKind,
    message: impl Into<Arc<str>>,
) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(kind, message)
}

pub(super) fn map_provider_error(message: impl ToString) -> ConnectorError {
    let message = message.to_string();
    let lower = message.to_ascii_lowercase();
    let kind = if lower.contains("not found") || lower.contains("unknown table") {
        ConnectorErrorKind::NotFound
    } else if lower.contains("exceed") || lower.contains("too many") {
        ConnectorErrorKind::ResourceExhausted
    } else if lower.contains("unsupported") || lower.contains("supports only") {
        ConnectorErrorKind::Unsupported
    } else if lower.contains("changed") || lower.contains("conflict") {
        ConnectorErrorKind::InvalidRequest
    } else {
        ConnectorErrorKind::Internal
    };
    ConnectorError::new(kind, message)
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn conflict(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorDataMutationExecuteRequest, ConnectorDataMutationPlanningRequest,
        ConnectorDataMutationReconcileRequest, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorMetadata, ConnectorProviderId, ConnectorRequestContext, ConnectorTableHandle,
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
        ProviderBindingEpoch,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };
    use crate::iceberg::{NamespaceIdent, TableCreation};
    use crate::resources::IcebergMetadataResources;

    struct FakeBackend {
        lookup: Mutex<MarkerLookup>,
        execute_count: AtomicUsize,
        context_calls: AtomicUsize,
        namespace: String,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                lookup: Mutex::new(MarkerLookup::Missing),
                execute_count: AtomicUsize::new(0),
                context_calls: AtomicUsize::new(0),
                namespace: "db".to_string(),
            }
        }

        fn with_namespace(namespace: impl Into<String>) -> Self {
            Self {
                namespace: namespace.into(),
                ..Self::new()
            }
        }
    }

    impl IcebergDataMutationBackend for FakeBackend {
        // This backend tests mutation protocol replay independently of catalog policy.
        fn admit(
            &self,
            _request: &ConnectorDataMutationPlanningRequest,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn plan(
            &self,
            _request: &ConnectorDataMutationPlanningRequest,
        ) -> Result<
            (
                PlannedIcebergMutation,
                [u8; 32],
                ConnectorDataMutationPlanSummary,
            ),
            ConnectorError,
        > {
            self.context_calls.fetch_add(1, Ordering::SeqCst);
            let source = fake_source_metadata();
            Ok((
                PlannedIcebergMutation::Truncate {
                    payload: IcebergDataMutationPlanPayloadV2 {
                        version: PLAN_PAYLOAD_VERSION,
                        namespace: self.namespace.clone(),
                        table: "orders".to_string(),
                        table_uuid: source.uuid().to_string(),
                        target_ref: "main".to_string(),
                        base_snapshot_id: None,
                        base_sequence_number: None,
                        schema_id: source.current_schema_id(),
                        default_spec_id: source.default_partition_spec_id(),
                        metadata_version_digest_hex: "aa".repeat(32),
                        source_location: None,
                        name_mapping_digest_hex: None,
                    },
                    source_metadata: source,
                },
                [9; 32],
                ConnectorDataMutationPlanSummary::default(),
            ))
        }

        fn execute(
            &self,
            _planned: &PlannedIcebergMutation,
            _marker: &IcebergDataMutationMarkerV1,
            recovery: &MutationRecoveryTemplate,
            _context: &ConnectorRequestContext,
        ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
            self.execute_count.fetch_add(1, Ordering::SeqCst);
            self.context_calls.fetch_add(1, Ordering::SeqCst);
            Ok(ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Unavailable,
                    "marker response unresolved",
                ),
                evidence: recovery.evidence(RecoveryPublication::MarkerOnly {})?,
            })
        }

        fn lookup_marker(
            &self,
            _namespace: &str,
            _table: &str,
            _target_ref: &str,
            _operation_id_hex: &str,
            _identity_digest_hex: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<MarkerLookup, ConnectorError> {
            self.context_calls.fetch_add(1, Ordering::SeqCst);
            Ok(*self.lookup.lock().expect("lookup"))
        }
    }

    fn fake_source_metadata() -> TableMetadata {
        TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "file:///mutation-fixture".into(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn test_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .expect("context")
    }

    pub(super) fn table_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            64 * 1024,
            256 * 1024,
        )
        .expect("table context")
    }

    pub(super) fn exact_provider_with_empty_table() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        Arc<IcebergMetadata>,
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
        let resources = IcebergMetadataResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let catalog = runtime.novarocks_catalog().vendored_client();
        executor.block_on(async move {
            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .expect("create namespace");
            let schema = Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .expect("schema");
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("t".to_string())
                        .schema(schema)
                        .format_version(FormatVersion::V2)
                        .build(),
                )
                .await
                .expect("create table");
        });
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = Arc::new(IcebergMetadata::new(
            descriptor,
            ProviderBindingEpoch::from_bytes([8; 16]),
            runtime,
        ));
        (executor, warehouse, provider)
    }

    #[test]
    fn add_files_duplicate_precheck_preserves_request_cancellation() {
        let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
        let runtime = provider.runtime();
        let table = runtime
            .load_table_for_request("db", "t", &table_context())
            .expect("table")
            .into_table();
        let manifest = AddFilesManifest {
            source_scope:
                novarocks_spi::connector::ConnectorDataMutationSourceScope::try_new_directory(
                    [1; 32],
                )
                .expect("scope"),
            records: Vec::new(),
            digest: [2; 32],
            total_bytes: 0,
            total_rows: 0,
            total_footer_bytes: 0,
            schema_identity_mode:
                super::super::add_files::AddFilesSchemaIdentityMode::EmbeddedFieldIds,
            canonical_name_mapping: None,
        };
        let cancelled = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            {
                let stop = novarocks_spi::connector::ConnectorStopOwner::new();
                stop.request_stop();
                stop.view()
            },
            1024,
            4096,
        )
        .expect("cancelled context");
        let operation = crate::commit::operation::IcebergCommitOperation::new(
            OperationToken::from_mutation(ConnectorMutationOperationId::new()),
            table.metadata().location(),
            runtime.resources().planning_binding().clone(),
            cancelled,
            runtime.resources().catalog_runtime().clone(),
            crate::commit::operation::OperationLimits::default(),
        )
        .expect("operation");
        let error = match operation.begin_attempt() {
            Ok(_) => panic!("cancelled duplicate validation must stop before I/O"),
            Err(error) => error,
        };
        assert_eq!(
            crate::commit::attempt::before_dispatch_failure(&error).kind(),
            ConnectorMutationFailureKind::Cancelled
        );
        assert!(operation.artifacts().expect("ledger").is_empty());
        assert!(manifest.records.is_empty());
    }

    fn test_adapter(
        backend: Arc<FakeBackend>,
    ) -> (
        IcebergDataMutationAdapter,
        ConnectorProviderBindingKey,
        ConnectorInstanceId,
    ) {
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance");
        let key = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([3; 16]),
        };
        (
            IcebergDataMutationAdapter::new_with_backend(key.clone(), backend).expect("adapter"),
            key,
            instance_id,
        )
    }

    fn truncate_request(
        key: ConnectorProviderBindingKey,
        instance_id: ConnectorInstanceId,
        operation_id: ConnectorMutationOperationId,
        target_ref: &str,
    ) -> ConnectorDataMutationPlanningRequest {
        let handle = ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
            .expect("handle");
        ConnectorDataMutationPlanningRequest::try_new(
            operation_id,
            key,
            ConnectorDataMutationOperation::truncate(handle, target_ref).expect("operation"),
            test_context(),
        )
        .expect("request")
    }

    #[test]
    fn exact_runtime_truncate_commits_and_replays_without_a_catalog_registry() {
        let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
        let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).expect("adapter");
        let metadata = provider
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: table_context(),
            })
            .expect("load table");
        let operation_id = ConnectorMutationOperationId::new();
        let planning = ConnectorDataMutationPlanningRequest::try_new(
            operation_id,
            adapter.binding_key().clone(),
            ConnectorDataMutationOperation::truncate(metadata.table, "main")
                .expect("truncate operation"),
            table_context(),
        )
        .expect("planning request");
        let plan = adapter.plan_mutation(planning).expect("plan truncate");
        let request = ConnectorDataMutationExecuteRequest::try_new(plan, table_context())
            .expect("execute request");
        let first = adapter.execute(request.clone()).expect("execute truncate");
        let replay = adapter.execute(request).expect("replay truncate");
        assert!(matches!(
            first,
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert_eq!(first, replay);
    }

    pub(super) fn write_external_parquet(
        directory: &std::path::Path,
        rows: Vec<i64>,
    ) -> std::path::PathBuf {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        let path = directory.join("external.parquet");
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("value", DataType::Int64, true)
                .with_metadata(HashMap::from([("PARQUET:field_id".into(), "1".into())])),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(rows))]).unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    pub(super) fn register_plan(
        adapter: &IcebergDataMutationAdapter,
        provider: &IcebergMetadata,
        directory: &std::path::Path,
    ) -> ConnectorDataMutationPlan {
        let metadata = provider
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: table_context(),
            })
            .unwrap();
        adapter
            .plan_mutation(
                ConnectorDataMutationPlanningRequest::try_new(
                    ConnectorMutationOperationId::new(),
                    adapter.binding_key().clone(),
                    ConnectorDataMutationOperation::register_existing_files(
                        metadata.table,
                        format!("file://{}", directory.display()),
                    )
                    .unwrap(),
                    table_context(),
                )
                .unwrap(),
            )
            .unwrap()
    }

    #[test]
    fn add_files_duplicate_on_refreshed_parent_preserves_external_parquet() {
        let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
        let source = tempfile::tempdir().unwrap();
        let file = write_external_parquet(source.path(), vec![1, 2, 3]);
        let bytes = std::fs::read(&file).unwrap();
        let adapter = IcebergDataMutationAdapter::try_new(provider.clone()).unwrap();
        let first = register_plan(&adapter, &provider, source.path());
        let concurrent = register_plan(&adapter, &provider, source.path());
        assert!(matches!(
            adapter
                .execute(
                    ConnectorDataMutationExecuteRequest::try_new(first, table_context()).unwrap()
                )
                .unwrap(),
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        let refusal = adapter
            .execute(
                ConnectorDataMutationExecuteRequest::try_new(concurrent, table_context()).unwrap(),
            )
            .unwrap();
        assert!(
            matches!(refusal, ExternalMutationOutcome::KnownUncommitted { failure, cleanup: ExternalMutationFinalization::Complete }
            if failure.kind() == ConnectorMutationFailureKind::Conflict)
        );
        assert_eq!(std::fs::read(&file).unwrap(), bytes);
        let table = provider
            .runtime()
            .load_table_for_request("db", "t", &table_context())
            .unwrap()
            .into_table();
        let active = provider
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::extract_data_files_with_stats_with_control(&table, None).await
            })
            .unwrap()
            .unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].record_count, Some(3));
    }

    #[test]
    fn add_files_changed_frozen_source_is_rejected_without_deleting_it() {
        let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
        let source = tempfile::tempdir().unwrap();
        write_external_parquet(source.path(), vec![1]);
        let adapter = IcebergDataMutationAdapter::try_new(provider.clone()).unwrap();
        let plan = register_plan(&adapter, &provider, source.path());
        let file = write_external_parquet(source.path(), vec![1, 2]);
        let changed_bytes = std::fs::read(&file).unwrap();
        let outcome = adapter
            .execute(ConnectorDataMutationExecuteRequest::try_new(plan, table_context()).unwrap())
            .unwrap();
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        assert_eq!(std::fs::read(file).unwrap(), changed_bytes);
        assert!(
            provider
                .runtime()
                .load_table_for_request("db", "t", &table_context())
                .unwrap()
                .table
                .metadata()
                .current_snapshot_id()
                .is_none()
        );
    }

    #[test]
    fn mutation_recovery_rejects_changed_source_sequence_and_nested_unknown_fields() {
        let backend = Arc::new(FakeBackend::new());
        let (adapter, key, instance) = test_adapter(backend);
        let plan = adapter
            .plan_mutation(truncate_request(
                key,
                instance,
                ConnectorMutationOperationId::new(),
                "main",
            ))
            .unwrap();
        let cached = adapter
            .plans
            .lock()
            .unwrap()
            .get(&plan.operation_id())
            .unwrap()
            .clone();
        let evidence = adapter.evidence(&plan, cached.private.payload()).unwrap();
        let mut decoded: IcebergDataMutationEvidenceV2 =
            decode_canonical_json(evidence.provider_payload(), "recovery").unwrap();
        decoded.base_sequence_number = Some(5);
        assert!(validate_recovery_facts(&decoded, plan.operation_id()).is_err());
        let mut value = serde_json::to_value(&decoded).unwrap();
        value["publication"]["cleanup_authority"] = serde_json::json!(true);
        assert!(serde_json::from_value::<IcebergDataMutationEvidenceV2>(value).is_err());
    }

    #[tokio::test]
    async fn mutation_source_freeze_uses_snapshot_sequence_when_another_ref_advances() {
        use crate::commit::fast_append::FastAppendPreparer;
        use crate::commit::overwrite::preparer_tests::Fixture;
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let main_intent = fixture.intent(&base, "main", vec![]);
        let (main, _) = fixture.stage(base, &main_intent, &FastAppendPreparer).await;
        let initial = source_snapshot(&main, "main").unwrap().unwrap();
        let dev_intent = fixture.intent(&main, "dev", vec![]);
        let (after, _) = fixture.stage(main, &dev_intent, &FastAppendPreparer).await;
        assert!(after.last_sequence_number() > initial.sequence_number);
        assert_eq!(source_snapshot(&after, "main").unwrap(), Some(initial));
        assert_eq!(
            source_snapshot(&after, "dev")
                .unwrap()
                .unwrap()
                .sequence_number,
            after.last_sequence_number()
        );
    }

    #[test]
    fn marker_codec_is_canonical_and_rejects_unknown_fields() {
        let marker = IcebergDataMutationMarkerV1 {
            version: 1,
            identity_digest_hex: "11".repeat(32),
            incarnation_hex: "22".repeat(16),
            operation_id_hex: "33".repeat(16),
            operation_kind: "truncate".to_string(),
            request_digest_hex: "44".repeat(32),
            plan_digest_hex: "55".repeat(32),
            state_digest_hex: "66".repeat(32),
            target_ref: "main".to_string(),
            base_snapshot_id: Some(7),
            file_count: 0,
            row_count: 0,
            total_bytes: 0,
        };
        let encoded = canonical_json(&marker, "marker").expect("encode");
        assert_eq!(
            decode_canonical_json::<IcebergDataMutationMarkerV1>(&encoded, "marker")
                .expect("decode"),
            marker
        );
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("json");
        value["credential"] = serde_json::Value::String("secret".to_string());
        assert!(
            decode_canonical_json::<IcebergDataMutationMarkerV1>(
                &serde_json::to_vec(&value).expect("json"),
                "marker"
            )
            .is_err()
        );
    }

    #[test]
    fn truncate_state_digest_binds_ref_and_base() {
        let mut payload = IcebergDataMutationPlanPayloadV2 {
            version: 1,
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: "uuid".to_string(),
            target_ref: "main".to_string(),
            base_snapshot_id: Some(7),
            base_sequence_number: Some(3),
            schema_id: 1,
            default_spec_id: 0,
            metadata_version_digest_hex: "aa".repeat(32),
            source_location: None,
            name_mapping_digest_hex: None,
        };
        let first = truncate_state_digest(&payload);
        payload.target_ref = "dev".to_string();
        assert_ne!(first, truncate_state_digest(&payload));
        payload.target_ref = "main".to_string();
        payload.base_snapshot_id = Some(8);
        assert_ne!(first, truncate_state_digest(&payload));
    }

    #[test]
    fn truncate_evidence_wire_fits_exact_durable_hex_boundary_and_rejects_one_over() {
        fn planned_evidence_wire_len(
            adapter: &IcebergDataMutationAdapter,
            plan: &ConnectorDataMutationPlan,
        ) -> usize {
            let plans = adapter.plans.lock().expect("plans");
            let cached = plans.get(&plan.operation_id()).expect("cached plan");
            adapter
                .evidence(plan, cached.private.payload())
                .expect("evidence")
                .try_to_wire_v1()
                .expect("wire")
                .len()
        }

        assert_eq!(
            MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
                .checked_mul(2)
                .expect("hex size"),
            MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES
        );

        let empty_backend = Arc::new(FakeBackend::with_namespace("n"));
        let (empty_adapter, key, instance_id) = test_adapter(empty_backend);
        let base_plan = empty_adapter
            .plan_mutation(truncate_request(
                key,
                instance_id,
                ConnectorMutationOperationId::from_bytes([11; 16]),
                "main",
            ))
            .expect("base plan");
        let base_wire_len = planned_evidence_wire_len(&empty_adapter, &base_plan);
        let boundary_namespace_len = MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
            .checked_sub(base_wire_len)
            .and_then(|length| length.checked_add(1))
            .expect("evidence base must fit durable cap");

        let boundary_backend = Arc::new(FakeBackend::with_namespace(
            "n".repeat(boundary_namespace_len),
        ));
        let (boundary_adapter, key, instance_id) = test_adapter(Arc::clone(&boundary_backend));
        let boundary_plan = boundary_adapter
            .plan_mutation(truncate_request(
                key,
                instance_id,
                ConnectorMutationOperationId::from_bytes([12; 16]),
                "main",
            ))
            .expect("evidence exactly at durable cap must plan");
        assert_eq!(
            planned_evidence_wire_len(&boundary_adapter, &boundary_plan),
            MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES
        );
        assert_eq!(boundary_backend.execute_count.load(Ordering::SeqCst), 0);

        let over_backend = Arc::new(FakeBackend::with_namespace(
            "n".repeat(boundary_namespace_len + 1),
        ));
        let (over_adapter, key, instance_id) = test_adapter(Arc::clone(&over_backend));
        let error = over_adapter
            .plan_mutation(truncate_request(
                key,
                instance_id,
                ConnectorMutationOperationId::from_bytes([13; 16]),
                "main",
            ))
            .expect_err("over-budget evidence must fail during planning");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert!(over_adapter.plans.lock().expect("plans").is_empty());
        assert_eq!(over_backend.execute_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn truncate_receipt_provider_payload_has_a_fixed_small_durable_bound() {
        for snapshot_id in [i64::MIN, -1, 0, 1, i64::MAX] {
            let payload = durable_receipt_payload(snapshot_id).expect("receipt payload");
            assert!(payload.len() <= MAX_DURABLE_ICEBERG_TRUNCATE_RECEIPT_PROVIDER_PAYLOAD_BYTES);
        }
    }

    #[test]
    fn operation_replay_is_idempotent_and_conflicting_request_is_rejected() {
        let backend = Arc::new(FakeBackend::new());
        let (adapter, key, instance_id) = test_adapter(backend);
        let operation_id = ConnectorMutationOperationId::from_bytes([8; 16]);
        let request = truncate_request(key.clone(), instance_id.clone(), operation_id, "main");
        let first = adapter.plan_mutation(request.clone()).expect("first plan");
        let replay = adapter.plan_mutation(request).expect("replay plan");
        assert_eq!(first.plan_digest(), replay.plan_digest());
        let conflict = truncate_request(key, instance_id, operation_id, "dev");
        assert!(adapter.plan_mutation(conflict).is_err());
    }

    #[test]
    fn adapter_threads_the_current_context_to_plan_execute_and_reconcile() {
        let backend = Arc::new(FakeBackend::new());
        let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
        let plan = adapter
            .plan_mutation(truncate_request(
                key,
                instance_id,
                ConnectorMutationOperationId::from_bytes([14; 16]),
                "main",
            ))
            .expect("plan");
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } = adapter
            .execute(
                ConnectorDataMutationExecuteRequest::try_new(plan.clone(), test_context())
                    .expect("execute request"),
            )
            .expect("unknown execution")
        else {
            panic!("expected commit unknown");
        };
        adapter
            .reconcile(
                ConnectorDataMutationReconcileRequest::try_new(&plan, evidence, test_context())
                    .expect("reconcile request"),
            )
            .expect("reconcile");
        assert_eq!(backend.context_calls.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn unknown_is_not_reexecuted_and_reconcile_survives_adapter_restart() {
        let backend = Arc::new(FakeBackend::new());
        let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
        let operation_id = ConnectorMutationOperationId::from_bytes([9; 16]);
        let plan = adapter
            .plan_mutation(truncate_request(
                key.clone(),
                instance_id,
                operation_id,
                "main",
            ))
            .expect("plan");
        let execute = ConnectorDataMutationExecuteRequest::try_new(plan.clone(), test_context())
            .expect("execute");
        let first = adapter.execute(execute.clone()).expect("unknown");
        let evidence = match first {
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => evidence,
            other => panic!("expected unknown, got {other:?}"),
        };
        assert!(matches!(
            adapter.execute(execute).expect("cached unknown"),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        assert_eq!(backend.execute_count.load(Ordering::SeqCst), 1);

        *backend.lookup.lock().expect("lookup") = MarkerLookup::Matching { snapshot_id: 42 };
        let restarted =
            IcebergDataMutationAdapter::new_with_backend(key, backend).expect("restart adapter");
        let reconcile =
            ConnectorDataMutationReconcileRequest::try_new(&plan, evidence, test_context())
                .expect("reconcile request");
        assert!(matches!(
            restarted.reconcile(reconcile).expect("reconciled"),
            ExternalMutationOutcome::KnownCommitted { receipt, .. }
                if receipt.summary() == ConnectorDataMutationPlanSummary::default()
        ));
    }

    #[test]
    fn unknown_outcome_reconciliation_uses_a_fresh_owner_after_client_cancellation() {
        let backend = Arc::new(FakeBackend::new());
        let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
        let plan = adapter
            .plan_mutation(truncate_request(
                key.clone(),
                instance_id,
                ConnectorMutationOperationId::from_bytes([19; 16]),
                "main",
            ))
            .expect("plan");
        let original_cancellation = Arc::new(novarocks_spi::connector::ConnectorStopOwner::new());
        let original_context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            original_cancellation.view(),
            1024,
            4096,
        )
        .expect("original context");
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } = adapter
            .execute(
                ConnectorDataMutationExecuteRequest::try_new(plan.clone(), original_context)
                    .expect("execute request"),
            )
            .expect("unknown result")
        else {
            panic!("expected unknown result");
        };
        original_cancellation.request_stop();
        *backend.lookup.lock().expect("lookup") = MarkerLookup::Matching { snapshot_id: 42 };
        let restarted = IcebergDataMutationAdapter::new_with_backend(key, backend.clone())
            .expect("restart adapter");
        let recovery =
            ConnectorDataMutationReconcileRequest::try_new(&plan, evidence, test_context())
                .expect("independent reconciliation context");
        assert!(matches!(
            restarted.reconcile(recovery).expect("reconcile"),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert_eq!(backend.execute_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn reconcile_marker_matrix_is_typed_and_never_reexecutes() {
        let backend = Arc::new(FakeBackend::new());
        let (adapter, key, instance_id) = test_adapter(Arc::clone(&backend));
        let plan = adapter
            .plan_mutation(truncate_request(
                key.clone(),
                instance_id,
                ConnectorMutationOperationId::from_bytes([10; 16]),
                "main",
            ))
            .expect("plan");
        let execute = ConnectorDataMutationExecuteRequest::try_new(plan.clone(), test_context())
            .expect("execute");
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
            adapter.execute(execute).expect("unknown")
        else {
            panic!("expected unknown");
        };

        let reconcile = || {
            ConnectorDataMutationReconcileRequest::try_new(&plan, evidence.clone(), test_context())
                .expect("reconcile request")
        };
        let restarted = IcebergDataMutationAdapter::new_with_backend(
            key.clone(),
            Arc::clone(&backend) as Arc<dyn IcebergDataMutationBackend>,
        )
        .expect("restart adapter");
        assert!(matches!(
            restarted.reconcile(reconcile()).expect("missing marker"),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));

        *backend.lookup.lock().expect("lookup") = MarkerLookup::Conflicting;
        assert!(matches!(
            restarted
                .reconcile(reconcile())
                .expect("conflicting marker"),
            ExternalMutationOutcome::CommitUnknown { failure, .. }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
        ));

        *backend.lookup.lock().expect("lookup") = MarkerLookup::Matching { snapshot_id: 43 };
        assert!(matches!(
            restarted.reconcile(reconcile()).expect("matching marker"),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert_eq!(backend.execute_count.load(Ordering::SeqCst), 1);
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

    fn admission_context(catalog_type: &str) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
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
    fn planning_refuses_hms_and_background_hadoop_before_backend_io() {
        for catalog_type in ["hive", "hadoop"] {
            let (_executor, warehouse, runtime, listener) = admission_runtime(catalog_type);
            let descriptor = ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            };
            let provider = Arc::new(IcebergMetadata::new(
                descriptor,
                ProviderBindingEpoch::from_bytes([8; 16]),
                runtime,
            ));
            let adapter = IcebergDataMutationAdapter::try_new(provider).expect("adapter");
            for operation in [
                ConnectorDataMutationOperation::truncate(
                    admission_table(adapter.key.instance_id.clone()),
                    "main",
                )
                .expect("truncate"),
                ConnectorDataMutationOperation::register_existing_files(
                    admission_table(adapter.key.instance_id.clone()),
                    warehouse.path().join("source").display().to_string(),
                )
                .expect("register files"),
            ] {
                let request = ConnectorDataMutationPlanningRequest::try_new(
                    ConnectorMutationOperationId::new(),
                    adapter.key.clone(),
                    operation,
                    admission_context(catalog_type),
                )
                .expect("request");
                let error = adapter
                    .plan_mutation(request)
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

    #[test]
    fn cached_plan_cannot_bypass_background_hadoop_admission() {
        let (_executor, _warehouse, provider) = exact_provider_with_empty_table();
        let adapter = IcebergDataMutationAdapter::try_new(Arc::clone(&provider)).expect("adapter");
        let metadata = provider
            .load_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("t"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: table_context(),
            })
            .expect("table");
        let mut request = ConnectorDataMutationPlanningRequest::try_new(
            ConnectorMutationOperationId::new(),
            adapter.key.clone(),
            ConnectorDataMutationOperation::truncate(metadata.table, "main").expect("truncate"),
            table_context(),
        )
        .expect("request");
        adapter
            .plan_mutation(request.clone())
            .expect("statement admission");
        request.context = request
            .context
            .with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::Background);
        let error = adapter
            .plan_mutation(request)
            .expect_err("cached plan must not authorize a background request");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(error.to_string().contains("background"));
        assert_eq!(adapter.plans.lock().expect("plans").len(), 1);
    }
}
