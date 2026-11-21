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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::iceberg::{NamespaceIdent, TableIdent};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[cfg(test)]
use novarocks_spi::connector::ExternalMutationEffect;
use novarocks_spi::connector::{
    ConnectorAddFilesFormat, ConnectorAddFilesRecursion, ConnectorDataMutation,
    ConnectorDataMutationExecuteRequest, ConnectorDataMutationOperation, ConnectorDataMutationPlan,
    ConnectorDataMutationPlanSummary, ConnectorDataMutationPlanningRequest,
    ConnectorDataMutationReceipt, ConnectorDataMutationReconcileRequest, ConnectorError,
    ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationOperationId, ConnectorOperationControl,
    ConnectorProviderBindingKey, ConnectorRequestContext, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use super::add_files::procedure_error;
use crate::catalog::CatalogTableName;
use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};
use crate::commit::model::{OperationToken, StartSnapshot};
use crate::commit::recovery::FrozenPublicationFacts;
use crate::iceberg::spec::TableMetadata;
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;
use novarocks_failure_contract::{FailureClass, connector_reason};

mod publication;

const PLAN_PAYLOAD_VERSION: u16 = 2;
const RECEIPT_PAYLOAD_VERSION: u16 = 1;
const EVIDENCE_PAYLOAD_VERSION: u16 = 3;
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationReceiptV1 {
    version: u16,
    snapshot_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergDataMutationEvidenceV3 {
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
    Dispatched { facts: Box<FrozenPublicationFacts> },
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

#[derive(Clone, Debug, Serialize)]
struct RegistrationPlanPayload {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    format: &'static str,
    recursive_directory: &'static str,
}

#[derive(Clone)]
enum PlannedIcebergMutation {
    RegisterExistingFiles {
        payload: RegistrationPlanPayload,
        location: Arc<str>,
        recursion: ConnectorAddFilesRecursion,
    },
    Truncate {
        payload: IcebergDataMutationPlanPayloadV2,
        source_metadata: Arc<TableMetadata>,
    },
}

impl PlannedIcebergMutation {
    fn provider_payload(&self) -> Result<Bytes, ConnectorError> {
        match self {
            Self::RegisterExistingFiles { payload, .. } => {
                canonical_json(payload, "Iceberg registration plan")
            }
            Self::Truncate { payload, .. } => canonical_json(payload, "Iceberg truncate plan"),
        }
    }

    fn truncate_parts(&self) -> Option<(&IcebergDataMutationPlanPayloadV2, &TableMetadata)> {
        match self {
            Self::Truncate {
                payload,
                source_metadata,
            } => Some((payload, source_metadata)),
            Self::RegisterExistingFiles { .. } => None,
        }
    }
}

fn missing_registration_argument(name: &str) -> ConnectorError {
    procedure_error(
        ConnectorErrorKind::InvalidRequest,
        FailureClass::Data,
        connector_reason::PROCEDURE_ARGUMENT_INVALID,
        format!("Required procedure argument '{name}' is missing"),
    )
}

fn validate_registration_target(
    namespace: &str,
    table: &str,
    version: crate::iceberg::spec::FormatVersion,
    format: ConnectorAddFilesFormat,
) -> Result<(), ConnectorError> {
    if version > crate::iceberg::spec::FormatVersion::V2 {
        let namespace = namespace.replace('"', "\"\"");
        let table = table.replace('"', "\"\"");
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "ADD_FILES is not supported for Iceberg table format version > 2. Table \"{namespace}\".\"{table}\" format version is {}.",
                version as u8,
            ),
        ).with_public_failure_detail());
    }
    if format != ConnectorAddFilesFormat::Parquet {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "NovaRocks ADD FILES only reads Parquet data files",
        )
        .with_public_failure_detail());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DataFinalizationFacts {
    namespace: String,
    table: String,
    table_uuid: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct DataPublication {
    receipt: ConnectorDataMutationReceipt,
    finalization: DataFinalizationFacts,
}

#[derive(Clone)]
struct CachedPlan {
    request_digest: [u8; 32],
    plan: ConnectorDataMutationPlan,
    private: PlannedIcebergMutation,
    execution: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct TerminalRecord {
    plan_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorDataMutationReceipt>,
}

#[derive(Clone)]
struct MutationRecoverySource {
    target: DataFinalizationFacts,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: Option<i64>,
}

impl From<&IcebergDataMutationPlanPayloadV2> for MutationRecoverySource {
    fn from(plan: &IcebergDataMutationPlanPayloadV2) -> Self {
        Self {
            target: DataFinalizationFacts {
                namespace: plan.namespace.clone(),
                table: plan.table.clone(),
                table_uuid: plan.table_uuid.clone(),
            },
            target_ref: plan.target_ref.clone(),
            base_snapshot_id: plan.base_snapshot_id,
            base_sequence_number: plan.base_sequence_number,
        }
    }
}

impl MutationRecoverySource {
    fn registration(plan: &RegistrationPlanPayload, start: Option<StartSnapshot>) -> Self {
        Self {
            target: DataFinalizationFacts {
                namespace: plan.namespace.clone(),
                table: plan.table.clone(),
                table_uuid: plan.table_uuid.clone(),
            },
            target_ref: "main".into(),
            base_snapshot_id: start.map(|s| s.snapshot_id),
            base_sequence_number: start.map(|s| s.sequence_number),
        }
    }
}

#[derive(Clone)]
struct MutationRecoveryTemplate {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorProviderBindingKey,
    plan: ConnectorDataMutationPlan,
    source: MutationRecoverySource,
    summary: ConnectorDataMutationPlanSummary,
}

impl MutationRecoveryTemplate {
    fn new(
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorProviderBindingKey,
        plan: ConnectorDataMutationPlan,
        source: MutationRecoverySource,
    ) -> Self {
        let summary = plan.summary();
        Self {
            descriptor,
            key,
            plan,
            source,
            summary,
        }
    }

    fn marker(&self) -> IcebergDataMutationMarkerV1 {
        IcebergDataMutationMarkerV1 {
            version: MARKER_VALUE_VERSION,
            identity_digest_hex: hex_encode(identity_digest(
                &self.descriptor,
                &self.key,
                &self.plan,
            )),
            incarnation_hex: hex_encode(self.key.incarnation.to_bytes()),
            operation_id_hex: hex_encode(self.plan.operation_id().to_bytes()),
            operation_kind: self.plan.operation_kind().to_string(),
            request_digest_hex: hex_encode(self.plan.request_digest()),
            plan_digest_hex: hex_encode(self.plan.plan_digest()),
            state_digest_hex: hex_encode(self.plan.state_digest()),
            target_ref: self.source.target_ref.clone(),
            base_snapshot_id: self.source.base_snapshot_id,
            file_count: self.summary.file_count(),
            row_count: self.summary.row_count(),
            total_bytes: self.summary.total_bytes(),
        }
    }

    fn finalization(&self) -> DataFinalizationFacts {
        self.source.target.clone()
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
            self.summary,
            durable_receipt_payload(snapshot_id)?,
        )
    }

    fn publication_receipt(&self, snapshot_id: i64) -> Result<DataPublication, ConnectorError> {
        Ok(DataPublication {
            receipt: self.receipt(snapshot_id)?,
            finalization: self.finalization(),
        })
    }

    fn evidence(
        &self,
        publication: RecoveryPublication,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let summary = self.summary;
        let payload = IcebergDataMutationEvidenceV3 {
            version: EVIDENCE_PAYLOAD_VERSION,
            namespace: self.source.target.namespace.clone(),
            table: self.source.target.table.clone(),
            target_ref: self.source.target_ref.clone(),
            table_uuid: self.source.target.table_uuid.clone(),
            base_snapshot_id: self.source.base_snapshot_id,
            base_sequence_number: self.source.base_sequence_number,
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
    evidence: &IcebergDataMutationEvidenceV3,
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

    fn execute(
        &self,
        planned: &PlannedIcebergMutation,
        marker: &IcebergDataMutationMarkerV1,
        recovery: &MutationRecoveryTemplate,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError>;

    fn execute_registration(
        &self,
        planned: &PlannedIcebergMutation,
        request: &ConnectorDataMutationExecuteRequest,
        descriptor: &ConnectorInstanceDescriptor,
        key: &ConnectorProviderBindingKey,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError>;

    fn evidence_policy(&self) -> crate::catalog::publication::CatalogEvidencePolicy;
    fn positive_attribution_finalization(&self) -> ExternalMutationFinalization;

    fn finalize_reconciled(
        &self,
        facts: &DataFinalizationFacts,
        context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorError>;

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

#[derive(Clone, Debug, Eq, PartialEq)]
enum MarkerLookup {
    Matching {
        observation: crate::commit::publication_descriptor::PositiveAttributionObservation,
    },
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
            .load_table_classified_for_request(namespace, table, request_context)
            .map(|loaded| loaded.into_table())
            .map_err(|(kind, message)| ConnectorError::new(kind, message))
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
        if let ConnectorDataMutationOperation::RegisterExistingFiles {
            location, format, ..
        } = request.operation()
        {
            if !self
                .runtime
                .control_state()
                .configuration()
                .add_files_enabled
            {
                return Err(procedure_error(
                    ConnectorErrorKind::PermissionDenied,
                    FailureClass::Data,
                    connector_reason::ADD_FILES_DISABLED,
                    "add_files procedure is disabled",
                ));
            }
            if location.is_none() {
                return Err(missing_registration_argument("location"));
            }
            if format.is_none() {
                return Err(missing_registration_argument("format"));
            }
        }
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
        let metadata = table.metadata();
        let table_uuid = metadata.uuid().to_string();
        let schema_id = metadata.current_schema_id();
        let default_spec_id = metadata.default_partition_spec_id();
        let metadata_version_digest = metadata_version_digest(table.metadata_location());

        match request.operation() {
            ConnectorDataMutationOperation::RegisterExistingFiles {
                location,
                format,
                recursion,
                ..
            } => {
                let location = location
                    .as_ref()
                    .ok_or_else(|| missing_registration_argument("location"))?;
                let format = format.ok_or_else(|| missing_registration_argument("format"))?;
                validate_registration_target(
                    &namespace,
                    &table_name,
                    metadata.format_version(),
                    format,
                )?;
                let payload = RegistrationPlanPayload {
                    version: 1,
                    namespace,
                    table: table_name,
                    table_uuid,
                    format: "PARQUET",
                    recursive_directory: match recursion {
                        ConnectorAddFilesRecursion::Include => "TRUE",
                        ConnectorAddFilesRecursion::Skip => "FALSE",
                        ConnectorAddFilesRecursion::Fail => "FAIL",
                    },
                };
                let mut state = Sha256::new();
                state.update(b"novarocks.iceberg.data-mutation.registration-state.v1\0");
                digest_bytes(&mut state, metadata.uuid().as_bytes());
                Ok((
                    PlannedIcebergMutation::RegisterExistingFiles {
                        payload,
                        location: Arc::clone(location),
                        recursion: *recursion,
                    },
                    state.finalize().into(),
                    ConnectorDataMutationPlanSummary::default(),
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
                };
                let state_digest = truncate_state_digest(&payload);
                Ok((
                    PlannedIcebergMutation::Truncate {
                        payload,
                        source_metadata: table.metadata_ref(),
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

    fn execute_registration(
        &self,
        planned: &PlannedIcebergMutation,
        request: &ConnectorDataMutationExecuteRequest,
        descriptor: &ConnectorInstanceDescriptor,
        key: &ConnectorProviderBindingKey,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        self.publish_registration(planned, request, descriptor, key)
    }

    fn evidence_policy(&self) -> crate::catalog::publication::CatalogEvidencePolicy {
        self.runtime.novarocks_catalog().evidence_policy()
    }
    fn positive_attribution_finalization(&self) -> ExternalMutationFinalization {
        self.runtime
            .novarocks_catalog()
            .positive_attribution_finalization()
    }

    fn finalize_reconciled(
        &self,
        facts: &DataFinalizationFacts,
        context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        self.finalize_publication_facts(facts, context)
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
        find_marker(
            table.metadata(),
            target_ref,
            operation_id_hex,
            identity_digest_hex,
        )
    }
}

fn find_marker(
    metadata: &TableMetadata,
    target_ref: &str,
    operation_id_hex: &str,
    identity_digest_hex: &str,
) -> Result<MarkerLookup, ConnectorError> {
    let target_snapshot = target_snapshot_id(metadata, target_ref)?;
    let mut cursor = target_snapshot;
    let mut remaining = metadata.snapshots().len();
    while let Some(snapshot_id) = cursor {
        let Some(snapshot) = metadata.snapshot_by_id(snapshot_id) else {
            break;
        };
        remaining = remaining
            .checked_sub(1)
            .ok_or_else(|| corrupt("Iceberg snapshot ancestry contains a cycle"))?;
        if let Some(raw) = snapshot
            .summary()
            .additional_properties
            .get(MARKER_PROPERTY)
        {
            let marker: IcebergDataMutationMarkerV1 =
                decode_canonical_json(raw.as_bytes(), "Iceberg data mutation marker")?;
            if marker.operation_id_hex == operation_id_hex {
                return Ok(if marker.identity_digest_hex == identity_digest_hex {
                    MarkerLookup::Matching { observation: crate::commit::publication_descriptor::PositiveAttributionObservation::SnapshotMarker {
                            table_uuid: metadata.uuid(), snapshot_id,
                            marker_key: MARKER_PROPERTY.to_owned(), marker_value: raw.clone(),
                        } }
                } else {
                    MarkerLookup::Conflicting
                });
            }
        }
        cursor = snapshot.parent_snapshot_id();
    }
    Ok(MarkerLookup::Missing)
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

    #[cfg(test)]
    fn marker(
        &self,
        plan: &ConnectorDataMutationPlan,
        payload: &IcebergDataMutationPlanPayloadV2,
    ) -> IcebergDataMutationMarkerV1 {
        MutationRecoveryTemplate::new(
            self.descriptor.clone(),
            self.key.clone(),
            plan.clone(),
            payload.into(),
        )
        .marker()
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
            payload.into(),
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
        let provider_payload = private.provider_payload()?;
        let plan =
            ConnectorDataMutationPlan::try_new(&request, state_digest, summary, provider_payload)?;
        if let Some((payload, _)) = private.truncate_parts() {
            self.preflight_durable_truncate_evidence(&plan, payload)?;
        }
        plans.insert(
            request.operation_id(),
            CachedPlan {
                request_digest: request.request_digest(),
                plan: plan.clone(),
                private,
                execution: Arc::new(Mutex::new(())),
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
        let _execution = cached
            .execution
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation execution lock: {error}")))?;
        if let Some(record) = self
            .terminal
            .lock()
            .map_err(|error| internal(format!("Iceberg data mutation terminal lock: {error}")))?
            .get(&request.plan.operation_id())
            .cloned()
        {
            if record.plan_digest == request.plan.plan_digest() {
                if let ExternalMutationOutcome::CommitUnknown { evidence, .. } = &record.outcome
                    && let Some((journal, entry)) =
                        crate::commit::attempt::observation::PublicationJournal::<DataPublication>::from_evidence(evidence)?
                    {
                        return Ok(publication::project_for_operation(
                            journal.catalog_entry_outcome(entry), request.plan.operation_kind(),
                        ));
                    }
                return Ok(record.outcome);
            }
            return Err(invalid(
                "Iceberg data mutation operation was executed with a different plan",
            ));
        }
        let outcome = if let Some((payload, _)) = cached.private.truncate_parts() {
            let template = MutationRecoveryTemplate::new(
                self.descriptor.clone(),
                self.key.clone(),
                request.plan.clone(),
                payload.into(),
            );
            let marker = template.marker();
            match self.backend.lookup_marker(
                &payload.namespace,
                &payload.table,
                &marker.target_ref,
                &marker.operation_id_hex,
                &marker.identity_digest_hex,
                &request.context,
            ) {
                Ok(MarkerLookup::Matching { .. }) => ExternalMutationOutcome::CommitUnknown {
                    failure: failure(
                        ConnectorMutationFailureKind::Unavailable,
                        "An existing data mutation marker has no original local dispatch journal",
                    ),
                    evidence: self.evidence(&request.plan, payload)?,
                },
                Ok(MarkerLookup::Conflicting) => ExternalMutationOutcome::CommitUnknown {
                    failure: failure(
                        ConnectorMutationFailureKind::Conflict,
                        "Iceberg data mutation marker conflicts with this operation",
                    ),
                    evidence: self.evidence(&request.plan, payload)?,
                },
                Ok(MarkerLookup::Missing) => {
                    self.backend
                        .execute(&cached.private, &marker, &template, &request.context)?
                }
                Err(error) => ExternalMutationOutcome::KnownUncommitted {
                    failure: crate::commit::attempt::before_dispatch_failure(
                        &publication::format_error(error),
                    ),
                    cleanup: ExternalMutationFinalization::Complete,
                },
            }
        } else {
            self.backend.execute_registration(
                &cached.private,
                &request,
                &self.descriptor,
                &self.key,
            )?
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
        let evidence: IcebergDataMutationEvidenceV3 = decode_canonical_json(
            request.evidence.provider_payload(),
            "Iceberg data mutation evidence",
        )?;
        validate_evidence_request(&self.descriptor, &self.key, &request, &evidence)?;
        let Some((journal, entry)) = crate::commit::attempt::observation::PublicationJournal::<
            DataPublication,
        >::from_evidence(&request.evidence)?
        else {
            return Ok(ExternalMutationOutcome::CommitUnknown {
                failure: unknown_reconciliation_failure(
                    &request,
                    ConnectorMutationFailureKind::Unavailable,
                    "Read-only data mutation reconciliation has no original in-process journal owner",
                ),
                evidence: request.evidence,
            });
        };
        if let committed @ ExternalMutationOutcome::KnownCommitted { .. } =
            journal.catalog_entry_outcome(entry)
        {
            return Ok(publication::project_for_operation(
                committed,
                &request.operation_kind,
            ));
        }
        let fresh_context;
        let observation_context = if request.operation_kind.as_ref()
            == novarocks_spi::connector::REGISTER_EXISTING_FILES_KIND
        {
            fresh_context = request.context.clone().after_external_effect();
            &fresh_context
        } else {
            &request.context
        };
        match self.backend.lookup_marker(
            &evidence.namespace,
            &evidence.table,
            &evidence.target_ref,
            &evidence.operation_id_hex,
            &evidence.identity_digest_hex,
            observation_context,
        )? {
            MarkerLookup::Matching { observation } => {
                let descriptor = journal
                    .entry_descriptor(entry)
                    .ok_or_else(|| corrupt("Data mutation journal entry is unavailable"))?;
                if journal
                    .append_positive_proof_with_finalization(
                        descriptor.key,
                        observation,
                        self.backend.positive_attribution_finalization(),
                    )
                    .is_err()
                {
                    if let committed @ ExternalMutationOutcome::KnownCommitted { .. } =
                        journal.catalog_entry_outcome(entry)
                    {
                        return Ok(publication::project_for_operation(
                            committed,
                            &request.operation_kind,
                        ));
                    }
                    return Ok(ExternalMutationOutcome::CommitUnknown {
                        failure: unknown_reconciliation_failure(
                            &request,
                            ConnectorMutationFailureKind::Conflict,
                            "Observed data mutation marker does not prove the exact original request",
                        ),
                        evidence: request.evidence,
                    });
                }
                let mut result = journal.catalog_entry_outcome(entry);
                if let ExternalMutationOutcome::KnownCommitted { receipt, .. } = &result {
                    let finalized = journal
                        .finalization_context(entry, self.backend.evidence_policy())
                        .and_then(|grant| grant.for_io(&request.context))
                        .and_then(|context| {
                            let _finalizing = journal.finalization_activity(entry);
                            self.backend
                                .finalize_reconciled(&receipt.finalization, &context)
                        });
                    if let Err(error) = finalized {
                        journal.record_entry_finalization(
                            entry,
                            ExternalMutationFinalization::Failed(failure(
                                ConnectorMutationFailureKind::Internal,
                                error.message(),
                            )),
                        );
                    }
                    result = journal.catalog_entry_outcome(entry);
                }
                Ok(publication::project_for_operation(
                    result,
                    &request.operation_kind,
                ))
            }
            MarkerLookup::Conflicting => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: unknown_reconciliation_failure(
                    &request,
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg data mutation marker conflicts with reconciliation evidence",
                ),
                evidence: request.evidence,
            }),
            MarkerLookup::Missing => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: unknown_reconciliation_failure(
                    &request,
                    ConnectorMutationFailureKind::Unavailable,
                    "Iceberg data mutation marker is not yet visible",
                ),
                evidence: request.evidence,
            }),
        }
    }
}

fn unknown_reconciliation_failure(
    request: &ConnectorDataMutationReconcileRequest,
    kind: ConnectorMutationFailureKind,
    detail: &'static str,
) -> ConnectorMutationFailure {
    if request.operation_kind.as_ref() == novarocks_spi::connector::REGISTER_EXISTING_FILES_KIND {
        publication::registration_unknown(detail)
    } else {
        failure(kind, detail)
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

fn validate_evidence_request(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorProviderBindingKey,
    request: &ConnectorDataMutationReconcileRequest,
    evidence: &IcebergDataMutationEvidenceV3,
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
mod tests;
