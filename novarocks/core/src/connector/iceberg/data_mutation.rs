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

//! FE-only Iceberg implementation of the connector data-mutation contract.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use novarocks_connector_iceberg::iceberg::{Catalog, NamespaceIdent, TableIdent};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorDataMutation, ConnectorDataMutationExecuteRequest, ConnectorDataMutationOperation,
    ConnectorDataMutationPlan, ConnectorDataMutationPlanSummary,
    ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
    ConnectorDataMutationReconcileRequest, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorInstanceDescriptor, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationOperationId, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::common::types::UniqueId;

use super::catalog::add_files::{
    AddFilesManifest, plan_manifest_for_table, revalidate_manifest_for_table,
};
use super::catalog::registry::{
    IcebergCatalogEntry, IcebergCatalogRegistry, block_on_iceberg, build_iceberg_catalog,
    data_file_to_written_file, load_table,
};
use super::commit::{
    CleanupAttempt, CleanupPathMapper, CommitServiceError, IcebergCommitCollector,
    RecoveryEvidence, RunInput, run_iceberg_commit,
};
use super::provider::decode_data_mutation_table_target;
use novarocks_connector_iceberg::commit::{CommitOpKind, CommitOutcome};
use novarocks_connector_iceberg::fs_io;

const PLAN_PAYLOAD_VERSION: u16 = 1;
const RECEIPT_PAYLOAD_VERSION: u16 = 1;
const EVIDENCE_PAYLOAD_VERSION: u16 = 1;
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
struct IcebergDataMutationPlanPayloadV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
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
struct IcebergDataMutationEvidenceV1 {
    version: u16,
    namespace: String,
    table: String,
    target_ref: String,
    operation_id_hex: String,
    operation_kind: String,
    request_digest_hex: String,
    plan_digest_hex: String,
    state_digest_hex: String,
    identity_digest_hex: String,
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
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
        payload: IcebergDataMutationPlanPayloadV1,
        manifest: AddFilesManifest,
    },
    Truncate {
        payload: IcebergDataMutationPlanPayloadV1,
    },
}

impl PlannedIcebergMutation {
    fn payload(&self) -> &IcebergDataMutationPlanPayloadV1 {
        match self {
            Self::RegisterExistingFiles { payload, .. } | Self::Truncate { payload } => payload,
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

trait IcebergDataMutationBackend: Send + Sync {
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
    ) -> Result<CommitOutcome, CommitServiceError>;

    fn lookup_marker(
        &self,
        namespace: &str,
        table: &str,
        target_ref: &str,
        operation_id_hex: &str,
        identity_digest_hex: &str,
    ) -> Result<MarkerLookup, ConnectorError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarkerLookup {
    Matching { snapshot_id: i64 },
    Conflicting,
    Missing,
}

struct RegisteredIcebergDataMutationBackend {
    instance_id: novarocks_spi::connector::ConnectorInstanceId,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}

impl RegisteredIcebergDataMutationBackend {
    fn new(
        instance_id: novarocks_spi::connector::ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Self {
        Self {
            instance_id,
            registry,
        }
    }

    fn entry(&self) -> Result<IcebergCatalogEntry, ConnectorError> {
        self.registry
            .read()
            .map_err(|error| internal(format!("Iceberg data mutation registry lock: {error}")))?
            .get(self.instance_id.as_str())
            .map_err(map_provider_error)
    }

    fn reload_table(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<
        (
            IcebergCatalogEntry,
            novarocks_connector_iceberg::iceberg::table::Table,
        ),
        ConnectorError,
    > {
        let entry = self.entry()?;
        entry.invalidate_table_cache(namespace, table);
        let loaded = load_table(&entry, namespace, table).map_err(map_provider_error)?;
        Ok((entry, loaded.into_table()))
    }
}

impl IcebergDataMutationBackend for RegisteredIcebergDataMutationBackend {
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
        let (namespace, table_name) =
            decode_data_mutation_table_target(request.operation().table())?;
        let (entry, table) = self.reload_table(&namespace, &table_name)?;
        let metadata = table.metadata();
        let table_uuid = metadata.uuid().to_string();
        let schema_id = metadata.current_schema_id();
        let default_spec_id = metadata.default_partition_spec_id();
        let metadata_version_digest = metadata_version_digest(table.metadata_location());

        match request.operation() {
            ConnectorDataMutationOperation::RegisterExistingFiles {
                source_location, ..
            } => {
                let manifest = plan_manifest_for_table(
                    &table,
                    source_location,
                    novarocks_fs::object_store_config_from_aws_s3_catalog_property_pairs(
                        &entry.properties,
                    )
                    .map_err(map_provider_error)?
                    .as_ref(),
                )
                .map_err(map_provider_error)?;
                let mapping_digest = manifest
                    .canonical_name_mapping
                    .as_deref()
                    .map(|mapping| hex::encode(Sha256::digest(mapping.as_bytes())));
                let payload = IcebergDataMutationPlanPayloadV1 {
                    version: PLAN_PAYLOAD_VERSION,
                    namespace,
                    table: table_name,
                    table_uuid,
                    target_ref: "main".to_string(),
                    base_snapshot_id: metadata.current_snapshot_id(),
                    schema_id,
                    default_spec_id,
                    metadata_version_digest_hex: hex::encode(metadata_version_digest),
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
                        manifest: manifest.clone(),
                    },
                    manifest.digest,
                    summary,
                ))
            }
            ConnectorDataMutationOperation::Truncate { target_ref, .. } => {
                if target_ref.as_ref() != "main"
                    && metadata.format_version()
                        != novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3
                {
                    return Err(invalid(
                        "Iceberg branch TRUNCATE requires a format-v3 table",
                    ));
                }
                let base_snapshot_id = target_snapshot_id(metadata, target_ref)?;
                let payload = IcebergDataMutationPlanPayloadV1 {
                    version: PLAN_PAYLOAD_VERSION,
                    namespace,
                    table: table_name,
                    table_uuid,
                    target_ref: target_ref.to_string(),
                    base_snapshot_id,
                    schema_id,
                    default_spec_id,
                    metadata_version_digest_hex: hex::encode(metadata_version_digest),
                    source_location: None,
                    name_mapping_digest_hex: None,
                };
                let state_digest = truncate_state_digest(&payload);
                Ok((
                    PlannedIcebergMutation::Truncate { payload },
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
    ) -> Result<CommitOutcome, CommitServiceError> {
        let payload = planned.payload();
        let (entry, table) = self
            .reload_table(&payload.namespace, &payload.table)
            .map_err(connector_error_as_pre_dispatch)?;
        validate_frozen_table(&table, payload).map_err(connector_error_as_pre_dispatch)?;
        match self.lookup_marker(
            &payload.namespace,
            &payload.table,
            &payload.target_ref,
            &marker.operation_id_hex,
            &marker.identity_digest_hex,
        ) {
            Ok(MarkerLookup::Matching { snapshot_id }) => {
                return Ok(CommitOutcome {
                    new_snapshot_id: snapshot_id,
                    written_manifest_paths: Vec::new(),
                });
            }
            Ok(MarkerLookup::Conflicting) => {
                return Err(CommitServiceError::unknown(
                    "Iceberg data mutation marker conflicted before dispatch".to_string(),
                    recovery_evidence(payload, mutation_op_kind(planned)),
                ));
            }
            Ok(MarkerLookup::Missing) => {}
            Err(error) => return Err(connector_error_as_pre_dispatch(error)),
        }

        let table_ident = TableIdent::new(
            NamespaceIdent::new(payload.namespace.clone()),
            payload.table.clone(),
        );
        let op_kind = match planned {
            PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } => {
                let object_store =
                    novarocks_fs::object_store_config_from_aws_s3_catalog_property_pairs(
                        &entry.properties,
                    )
                    .map_err(|error| connector_error_as_pre_dispatch(map_provider_error(error)))?;
                revalidate_manifest_for_table(
                    &table,
                    payload
                        .source_location
                        .as_deref()
                        .expect("ADD FILES plan has source location"),
                    object_store.as_ref(),
                    manifest,
                )
                .map_err(|error| connector_error_as_pre_dispatch(map_provider_error(error)))?;
                validate_no_duplicate_data_files(&table, manifest)
                    .map_err(connector_error_as_pre_dispatch)?;
                CommitOpKind::FastAppend
            }
            PlannedIcebergMutation::Truncate { .. } => CommitOpKind::Truncate,
        };
        let metadata = table.metadata();
        let staging_dir = format!(
            "{}/data/_staging/data-mutation-{}",
            metadata.location(),
            marker.operation_id_hex
        );
        let collector = Arc::new(
            IcebergCommitCollector::new(
                op_kind,
                table_ident,
                payload.base_snapshot_id,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staging_dir,
                UniqueId::new(0, 0),
            )
            .with_table_metadata(metadata.clone()),
        );
        if let PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } = planned {
            for data_file in manifest
                .to_data_files()
                .map_err(|error| connector_error_as_pre_dispatch(map_provider_error(error)))?
            {
                collector.inject_written_file(
                    data_file_to_written_file(&data_file, payload.default_spec_id).map_err(
                        |error| connector_error_as_pre_dispatch(map_provider_error(error)),
                    )?,
                );
            }
        }
        let catalog = build_iceberg_catalog(&entry)
            .map_err(|error| connector_error_as_pre_dispatch(map_provider_error(error)))?;
        ensure_hadoop_registration(&entry, catalog.as_ref(), &table)
            .map_err(connector_error_as_pre_dispatch)?;
        let marker_value = canonical_json(marker, "Iceberg data mutation marker")
            .map_err(connector_error_as_pre_dispatch)?;
        let snapshot_properties = BTreeMap::from([(
            MARKER_PROPERTY.to_string(),
            String::from_utf8(marker_value.to_vec()).expect("canonical JSON is UTF-8"),
        )]);
        let file_io = table.file_io().clone();
        let (fs, cleanup_path_mapper) =
            build_abort_cleanup(&entry).map_err(connector_error_as_pre_dispatch)?;
        let outcome = block_on_iceberg(async {
            run_iceberg_commit(RunInput {
                collector,
                catalog,
                table,
                fs,
                file_io,
                cleanup_path_mapper,
                cow_update_rewrite: None,
                selected_rewrite: None,
                target_ref: payload.target_ref.clone(),
                snapshot_properties,
            })
            .await
        })
        .map_err(|error| {
            CommitServiceError::invalid_input(format!("runtime failure: {error}"))
        })??;

        entry.invalidate_table_cache(&payload.namespace, &payload.table);
        let reloaded = load_table(&entry, &payload.namespace, &payload.table).map_err(|error| {
            CommitServiceError::finalize_failed_known_committed(
                Some(outcome.clone()),
                format!("reload committed Iceberg data mutation: {error}"),
                recovery_evidence(payload, op_kind),
            )
        })?;
        if let PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } = planned {
            let actual_mapping = reloaded
                .table
                .metadata()
                .properties()
                .get(novarocks_connector_iceberg::iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
                .map(|mapping| {
                    novarocks_connector_iceberg::schema_mapping::canonical_name_mapping(mapping)
                })
                .transpose()
                .map_err(|error| {
                    CommitServiceError::finalize_failed_known_committed(
                        Some(outcome.clone()),
                        format!("validate committed schema name mapping: {error}"),
                        recovery_evidence(payload, op_kind),
                    )
                })?;
            if actual_mapping.as_deref() != manifest.canonical_name_mapping.as_deref() {
                return Err(CommitServiceError::finalize_failed_known_committed(
                    Some(outcome),
                    "schema.name-mapping.default changed after ADD FILES commit".to_string(),
                    recovery_evidence(payload, op_kind),
                ));
            }
        }
        Ok(outcome)
    }

    fn lookup_marker(
        &self,
        namespace: &str,
        table: &str,
        target_ref: &str,
        operation_id_hex: &str,
        identity_digest_hex: &str,
    ) -> Result<MarkerLookup, ConnectorError> {
        let (_, table) = self.reload_table(namespace, table)?;
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

pub(crate) struct IcebergDataMutationAdapter {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    backend: Arc<dyn IcebergDataMutationBackend>,
    plans: Mutex<HashMap<ConnectorMutationOperationId, CachedPlan>>,
    terminal: Mutex<HashMap<ConnectorMutationOperationId, TerminalRecord>>,
}

impl IcebergDataMutationAdapter {
    pub(crate) fn new_registered(
        key: ConnectorExecutionBindingKey,
        instance_id: novarocks_spi::connector::ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Result<Self, ConnectorError> {
        Self::new(
            key,
            Arc::new(RegisteredIcebergDataMutationBackend::new(
                instance_id,
                registry,
            )),
        )
    }

    fn new(
        key: ConnectorExecutionBindingKey,
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

    fn ensure_owner(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
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
        payload: &IcebergDataMutationPlanPayloadV1,
    ) -> IcebergDataMutationMarkerV1 {
        let summary = plan.summary();
        IcebergDataMutationMarkerV1 {
            version: MARKER_VALUE_VERSION,
            identity_digest_hex: hex::encode(identity_digest(&self.descriptor, &self.key, plan)),
            incarnation_hex: hex::encode(self.key.incarnation.to_bytes()),
            operation_id_hex: hex::encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex::encode(plan.request_digest()),
            plan_digest_hex: hex::encode(plan.plan_digest()),
            state_digest_hex: hex::encode(plan.state_digest()),
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
        payload: &IcebergDataMutationPlanPayloadV1,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let marker = self.marker(plan, payload);
        ExternalMutationEvidence::try_new(
            EVIDENCE_PAYLOAD_VERSION,
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            canonical_json(
                &IcebergDataMutationEvidenceV1 {
                    version: EVIDENCE_PAYLOAD_VERSION,
                    namespace: payload.namespace.clone(),
                    table: payload.table.clone(),
                    target_ref: payload.target_ref.clone(),
                    operation_id_hex: marker.operation_id_hex,
                    operation_kind: marker.operation_kind,
                    request_digest_hex: marker.request_digest_hex,
                    plan_digest_hex: marker.plan_digest_hex,
                    state_digest_hex: marker.state_digest_hex,
                    identity_digest_hex: marker.identity_digest_hex,
                    file_count: marker.file_count,
                    row_count: marker.row_count,
                    total_bytes: marker.total_bytes,
                },
                "Iceberg data mutation evidence",
            )?,
        )
    }

    fn preflight_durable_truncate_evidence(
        &self,
        plan: &ConnectorDataMutationPlan,
        payload: &IcebergDataMutationPlanPayloadV1,
    ) -> Result<(), ConnectorError> {
        if plan.operation_kind() != TRUNCATE_OPERATION_KIND {
            return Ok(());
        }
        let wire = self.evidence(plan, payload)?.try_to_wire_v1()?;
        let hex_bytes = wire.len().checked_mul(2).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg TRUNCATE evidence hex size overflow",
            )
        })?;
        if hex_bytes > MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                format!(
                    "Iceberg TRUNCATE evidence wire exceeds durable {} byte cap for a {} byte lowercase-hex journal field",
                    MAX_DURABLE_ICEBERG_TRUNCATE_EVIDENCE_WIRE_BYTES,
                    MAX_DURABLE_TRUNCATE_EVIDENCE_HEX_BYTES,
                ),
            ));
        }
        Ok(())
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
        evidence: &IcebergDataMutationEvidenceV1,
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

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_mutation(
        &self,
        request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        request.validate()?;
        self.ensure_owner(request.owner())?;
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
        let plan = ConnectorDataMutationPlan::try_new(
            &request,
            state_digest,
            summary,
            source_scope,
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
        )? {
            MarkerLookup::Matching { snapshot_id } => self.committed(
                &request.plan,
                snapshot_id,
                ExternalMutationFinalization::Complete,
            )?,
            MarkerLookup::Conflicting => ExternalMutationOutcome::CommitUnknown {
                failure: failure(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg data mutation marker conflicts with this operation",
                ),
                evidence: self.evidence(&request.plan, cached.private.payload())?,
            },
            MarkerLookup::Missing => match self.backend.execute(&cached.private, &marker) {
                Ok(commit) => self.committed(
                    &request.plan,
                    commit.new_snapshot_id,
                    ExternalMutationFinalization::Complete,
                )?,
                Err(CommitServiceError::KnownUncommitted { message, .. })
                | Err(CommitServiceError::InvalidInput { message }) => {
                    ExternalMutationOutcome::KnownUncommitted {
                        failure: failure(ConnectorMutationFailureKind::Conflict, message),
                    }
                }
                Err(CommitServiceError::Unknown { message, .. }) => {
                    ExternalMutationOutcome::CommitUnknown {
                        failure: failure(ConnectorMutationFailureKind::Unavailable, message),
                        evidence: self.evidence(&request.plan, cached.private.payload())?,
                    }
                }
                Err(CommitServiceError::FinalizeFailedKnownCommitted {
                    outcome,
                    finalize_error,
                    ..
                }) => self.committed(
                    &request.plan,
                    outcome
                        .map(|outcome| outcome.new_snapshot_id)
                        .unwrap_or_default(),
                    ExternalMutationFinalization::Failed(failure(
                        ConnectorMutationFailureKind::Internal,
                        finalize_error,
                    )),
                )?,
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
        let evidence: IcebergDataMutationEvidenceV1 = decode_canonical_json(
            request.evidence.provider_payload(),
            "Iceberg data mutation evidence",
        )?;
        validate_evidence_request(&request, &evidence)?;
        match self.backend.lookup_marker(
            &evidence.namespace,
            &evidence.table,
            &evidence.target_ref,
            &evidence.operation_id_hex,
            &evidence.identity_digest_hex,
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

fn mutation_op_kind(planned: &PlannedIcebergMutation) -> CommitOpKind {
    match planned {
        PlannedIcebergMutation::RegisterExistingFiles { .. } => CommitOpKind::FastAppend,
        PlannedIcebergMutation::Truncate { .. } => CommitOpKind::Truncate,
    }
}

fn validate_evidence_request(
    request: &ConnectorDataMutationReconcileRequest,
    evidence: &IcebergDataMutationEvidenceV1,
) -> Result<(), ConnectorError> {
    if evidence.version != EVIDENCE_PAYLOAD_VERSION
        || evidence.operation_id_hex != hex::encode(request.operation_id.to_bytes())
        || evidence.operation_kind != request.operation_kind.as_ref()
        || evidence.request_digest_hex != hex::encode(request.request_digest)
        || evidence.plan_digest_hex != hex::encode(request.plan_digest)
        || evidence.state_digest_hex != hex::encode(request.state_digest)
    {
        return Err(invalid(
            "Iceberg data mutation evidence does not match its reconcile request",
        ));
    }
    Ok(())
}

fn validate_frozen_table(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    payload: &IcebergDataMutationPlanPayloadV1,
) -> Result<(), ConnectorError> {
    let metadata = table.metadata();
    if metadata.uuid().to_string() != payload.table_uuid
        || metadata.current_schema_id() != payload.schema_id
        || metadata.default_partition_spec_id() != payload.default_spec_id
        || target_snapshot_id(metadata, &payload.target_ref)? != payload.base_snapshot_id
        || hex::encode(metadata_version_digest(table.metadata_location()))
            != payload.metadata_version_digest_hex
    {
        return Err(conflict(
            "Iceberg data mutation table state advanced after planning",
        ));
    }
    Ok(())
}

fn validate_no_duplicate_data_files(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    manifest: &AddFilesManifest,
) -> Result<(), ConnectorError> {
    let live = super::catalog::registry::extract_data_files_with_stats(table)
        .map_err(map_provider_error)?
        .into_iter()
        .map(|file| file.path)
        .collect::<HashSet<_>>();
    if let Some(duplicate) = manifest
        .records
        .iter()
        .find(|record| live.contains(&record.location))
    {
        return Err(conflict(format!(
            "ADD FILES source already exists in the target table: {}",
            duplicate.location
        )));
    }
    Ok(())
}

fn build_abort_cleanup(
    entry: &IcebergCatalogEntry,
) -> Result<
    (
        novarocks_connector_iceberg::opendal::Operator,
        Option<CleanupPathMapper>,
    ),
    ConnectorError,
> {
    if let Some(s3_config) = entry.object_store_config() {
        let access = fs_io::resolve_access_for_location(&entry.warehouse_uri, Some(s3_config))
            .map_err(|error| {
                internal(format!(
                    "resolve Iceberg warehouse for data mutation cleanup: {error}"
                ))
            })?;
        let bucket = access
            .handle()
            .authority()
            .ok_or_else(|| corrupt("Iceberg warehouse URI has no object-store bucket"))?
            .to_string();
        let mapper: CleanupPathMapper = Arc::new(move |path| {
            novarocks_fs::parse_object_store_path_parse_only(path)
                .ok()
                .and_then(|(actual_bucket, key)| (actual_bucket == bucket).then_some(key))
                .unwrap_or_else(|| path.to_string())
        });
        return Ok((access.operator(), Some(mapper)));
    }
    let fs = novarocks_fs::FsAccessResolver::new()
        .resolve_location("/__novarocks_local_root__", None)
        .map_err(|error| internal(format!("build local cleanup operator: {error}")))?
        .operator();
    let mapper: CleanupPathMapper =
        Arc::new(|path: &str| path.strip_prefix("file://").unwrap_or(path).to_string());
    Ok((fs, Some(mapper)))
}

fn ensure_hadoop_registration(
    entry: &IcebergCatalogEntry,
    catalog: &dyn Catalog,
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<(), ConnectorError> {
    if entry.uses_remote_catalog() {
        return Ok(());
    }
    let namespace = table.identifier().namespace().clone();
    let ident = table.identifier().clone();
    let metadata_location = table
        .metadata_location()
        .ok_or_else(|| corrupt("Iceberg table has no metadata location"))?
        .to_string();
    block_on_iceberg(async {
        let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
        catalog.register_table(&ident, metadata_location).await
    })
    .map_err(|error| internal(format!("Iceberg registration runtime: {error}")))?
    .map(|_| ())
    .map_err(|error| map_provider_error(error.to_string()))
}

fn target_snapshot_id(
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
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
    key: &ConnectorExecutionBindingKey,
    plan: &ConnectorDataMutationPlan,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(IDENTITY_DIGEST_DOMAIN);
    digest_bytes(&mut hasher, descriptor.provider_id.as_str().as_bytes());
    digest_bytes(&mut hasher, descriptor.instance_id.as_str().as_bytes());
    digest_bytes(&mut hasher, &key.incarnation.to_bytes());
    digest_bytes(&mut hasher, &plan.operation_id().to_bytes());
    digest_bytes(&mut hasher, plan.operation_kind().as_bytes());
    digest_bytes(&mut hasher, &plan.request_digest());
    digest_bytes(&mut hasher, &plan.plan_digest());
    digest_bytes(&mut hasher, &plan.state_digest());
    hasher.finalize().into()
}

fn truncate_state_digest(payload: &IcebergDataMutationPlanPayloadV1) -> [u8; 32] {
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

fn recovery_evidence(
    payload: &IcebergDataMutationPlanPayloadV1,
    op_kind: CommitOpKind,
) -> RecoveryEvidence {
    RecoveryEvidence {
        table_ident: format!("{}.{}", payload.namespace, payload.table),
        op_kind,
        base_snapshot_id: payload.base_snapshot_id,
        base_sequence_number: 0,
        staging_dir: String::new(),
        manifest_cleanup_token: None,
    }
}

fn connector_error_as_pre_dispatch(error: ConnectorError) -> CommitServiceError {
    CommitServiceError::known_uncommitted(error.to_string(), CleanupAttempt::not_attempted())
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

fn map_provider_error(message: impl ToString) -> ConnectorError {
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
        ConnectorCancellation, ConnectorDataMutationExecuteRequest,
        ConnectorDataMutationPlanningRequest, ConnectorDataMutationReconcileRequest,
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorRequestContext,
        ConnectorTableHandle,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct FakeBackend {
        lookup: Mutex<MarkerLookup>,
        execute_count: AtomicUsize,
        namespace: String,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                lookup: Mutex::new(MarkerLookup::Missing),
                execute_count: AtomicUsize::new(0),
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
            Ok((
                PlannedIcebergMutation::Truncate {
                    payload: IcebergDataMutationPlanPayloadV1 {
                        version: PLAN_PAYLOAD_VERSION,
                        namespace: self.namespace.clone(),
                        table: "orders".to_string(),
                        table_uuid: "table-uuid".to_string(),
                        target_ref: "main".to_string(),
                        base_snapshot_id: Some(7),
                        schema_id: 1,
                        default_spec_id: 0,
                        metadata_version_digest_hex: "aa".repeat(32),
                        source_location: None,
                        name_mapping_digest_hex: None,
                    },
                },
                [9; 32],
                ConnectorDataMutationPlanSummary::default(),
            ))
        }

        fn execute(
            &self,
            planned: &PlannedIcebergMutation,
            _marker: &IcebergDataMutationMarkerV1,
        ) -> Result<CommitOutcome, CommitServiceError> {
            self.execute_count.fetch_add(1, Ordering::SeqCst);
            Err(CommitServiceError::unknown(
                "response lost".to_string(),
                recovery_evidence(planned.payload(), CommitOpKind::Truncate),
            ))
        }

        fn lookup_marker(
            &self,
            _namespace: &str,
            _table: &str,
            _target_ref: &str,
            _operation_id_hex: &str,
            _identity_digest_hex: &str,
        ) -> Result<MarkerLookup, ConnectorError> {
            Ok(*self.lookup.lock().expect("lookup"))
        }
    }

    fn test_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("context")
    }

    fn test_adapter(
        backend: Arc<FakeBackend>,
    ) -> (
        IcebergDataMutationAdapter,
        ConnectorExecutionBindingKey,
        ConnectorInstanceId,
    ) {
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance");
        let key = ConnectorExecutionBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([3; 16]),
        };
        (
            IcebergDataMutationAdapter::new(key.clone(), backend).expect("adapter"),
            key,
            instance_id,
        )
    }

    fn truncate_request(
        key: ConnectorExecutionBindingKey,
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
        let mut payload = IcebergDataMutationPlanPayloadV1 {
            version: 1,
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: "uuid".to_string(),
            target_ref: "main".to_string(),
            base_snapshot_id: Some(7),
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

        let empty_backend = Arc::new(FakeBackend::with_namespace(""));
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
        let restarted = IcebergDataMutationAdapter::new(key, backend).expect("restart adapter");
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
        let restarted = IcebergDataMutationAdapter::new(
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
}
