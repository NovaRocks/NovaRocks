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

use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::prepared::CatalogPublisher;
use crate::commit::attempt::{self, RetryPolicy};
use crate::commit::model::{
    AttemptToken, BaseIdentity, Dependency, FileChanges, FrozenRequest, IsolationLevel,
    OperationIntent, OperationIntentParts, OperationToken, RequestShape, TableTarget,
};
use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
use crate::commit::snapshot_lifecycle_helpers::reclamation::ReclamationReport;
use crate::commit::snapshot_lifecycle_helpers::retention::RetentionPlan;
use crate::commit::staging::PreparedChange;
use novarocks_spi::connector::{ConnectorOperationControl, ConnectorRequestContext};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorMaxCompactableDataFiles, ConnectorMaxCompactableDataFilesRequest,
    ConnectorMetadataMaintenance, ConnectorMetadataMaintenanceExecuteRequest,
    ConnectorMetadataMaintenanceExecution, ConnectorMetadataMaintenanceExecutionSource,
    ConnectorMetadataMaintenanceFuture, ConnectorMetadataMaintenanceOperation,
    ConnectorMetadataMaintenanceOutcome, ConnectorMetadataMaintenancePlan,
    ConnectorMetadataMaintenancePlanSummary, ConnectorMetadataMaintenancePlanningRequest,
    ConnectorMetadataMaintenanceReceipt, ConnectorMetadataMaintenanceReceiptSummary,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ConnectorProviderBindingKey, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use super::maintenance_admission::{RetentionProcedure, validate_retention_procedure};
use super::maintenance_error::{
    CommitFailureDisposition, MaintenanceCommitProcedure, commit_failure,
};
use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};
use crate::catalog::{CatalogTableName, SnapshotLoading};
use crate::commit::snapshot_lifecycle_helpers::rewrite_manifests::{
    RewriteManifestsPreparer, execute_metrics,
};
use crate::metadata::IcebergTablePayload;
use crate::metadata_context::IcebergMetadataContext;

#[path = "metadata_maintenance/expiry_metadata.rs"]
pub(super) mod expiry_metadata;

#[path = "metadata_maintenance/observation.rs"]
pub(crate) mod observation;

const PLAN_PAYLOAD_VERSION: u16 = 3;
const PAYLOAD_VERSION: u16 = 2;
const MARKER_VERSION: u16 = 2;
const MARKER_PROPERTY: &str = "novarocks.connector.maintenance.v2";
const STATE_DOMAIN: &[u8] = b"novarocks.iceberg.metadata-maintenance.intent.v2\0";
const IDENTITY_DOMAIN: &[u8] = b"novarocks.iceberg.metadata-maintenance.identity.v2\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenancePlanPayloadV1 {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    evaluation_time_ms: i64,
    operation_kind: String,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    sql_execute: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    clean_expired_metadata: bool,
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
    intent_digest_hex: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergMetadataMaintenanceReceiptV1 {
    version: u16,
    snapshot_id: Option<i64>,
    attempt_id_hex: String,
    attempt_ordinal: u32,
    base_snapshot_id: Option<i64>,
    metadata_location_digest_hex: String,
    frozen_request_digest_hex: String,
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
    intent_digest_hex: String,
    identity_digest_hex: String,
}

#[derive(Clone)]
struct TerminalRecord {
    plan_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>,
}

/// FE-only Iceberg adapter.  The registry contains process-local catalog
/// clients and credentials; none cross the SPI payload boundary.
pub(crate) struct IcebergMetadataMaintenanceAdapter {
    engine: Arc<MaintenanceEngine>,
    terminal: Arc<Mutex<HashMap<ConnectorMutationOperationId, TerminalRecord>>>,
    active: Arc<Mutex<HashMap<ConnectorMutationOperationId, Arc<MaintenanceExecution>>>>,
}

pub(crate) struct MaintenanceEngine {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergMetadataContext>,
}

impl std::ops::Deref for IcebergMetadataMaintenanceAdapter {
    type Target = MaintenanceEngine;
    fn deref(&self) -> &Self::Target {
        &self.engine
    }
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
            engine: Arc::new(MaintenanceEngine {
                key,
                descriptor,
                runtime,
            }),
            terminal: Arc::new(Mutex::new(HashMap::new())),
            active: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    #[cfg(test)]
    async fn execute(
        &self,
        request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ConnectorMetadataMaintenanceOutcome, ConnectorError> {
        let mut execution = self.begin_execute(request)?;
        execution.wait().await
    }
}

impl MaintenanceEngine {
    fn ensure_owner(&self, owner: &ConnectorProviderBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg metadata maintenance does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    async fn plan_payload(
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
            .observe_table_classified_for_request_with_snapshots_async(
                &namespace,
                &table_name,
                &request.context,
                maintenance_snapshot_loading(request.operation().kind()),
            )
            .await
            .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
        let table = loaded.into_table();
        let metadata = table.metadata();
        let (older_than_ms, retain_last, sql_execute, clean_expired_metadata) = match request
            .operation()
        {
            ConnectorMetadataMaintenanceOperation::RewriteMetadataLayout {
                sql_execute, ..
            } => (None, None, *sql_execute, false),
            ConnectorMetadataMaintenanceOperation::ExpireTableVersions {
                older_than_ms,
                retain_last,
                sql_execute,
                ..
            } => {
                if let Some(inputs) = sql_execute {
                    validate_retention_procedure(
                        RetentionProcedure::ExpireSnapshots,
                        metadata.format_version() as u8,
                        metadata
                            .properties()
                            .get("write.location-provider.impl")
                            .map(String::as_str),
                        &format!("{namespace}.{table_name}"),
                        inputs.retention,
                        self.runtime.control_state().configuration().maintenance,
                    )?;
                }
                let retain_last = match (retain_last, sql_execute) {
                    (None, Some(_)) => {
                        let default =
                            crate::iceberg::spec::TableProperties::new(metadata.properties())
                                .min_snapshots_to_keep()
                                .map_err(iceberg_error)?;
                        let default = i32::try_from(default)
                            .ok()
                            .filter(|count| *count > 0)
                            .ok_or_else(|| {
                                invalid(
                                    "Snapshot retention count must be positive and fit an integer",
                                )
                            })?;
                        Some(default as u32)
                    }
                    (retain_last, _) => *retain_last,
                };
                (
                    *older_than_ms,
                    retain_last,
                    sql_execute.is_some(),
                    sql_execute.is_some_and(|inputs| inputs.clean_expired_metadata),
                )
            }
        };
        let payload = IcebergMetadataMaintenancePlanPayloadV1 {
            version: PLAN_PAYLOAD_VERSION,
            namespace,
            table: table_name,
            table_uuid: metadata.uuid().to_string(),
            evaluation_time_ms: now_ms()?,
            operation_kind: request.operation().kind().to_string(),
            older_than_ms,
            retain_last,
            sql_execute,
            clean_expired_metadata,
        };
        // Only immutable operation intent belongs to the plan digest. Current
        // schema, spec, metadata location and ref head belong to each attempt.
        let intent_digest = intent_digest(&payload)?;
        let summary = ConnectorMetadataMaintenancePlanSummary::new(0, 0, 0, 0, 0);
        Ok((payload, intent_digest, summary))
    }

    fn payload_for_plan(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
    ) -> Result<IcebergMetadataMaintenancePlanPayloadV1, ConnectorError> {
        let payload: IcebergMetadataMaintenancePlanPayloadV1 =
            decode_canonical_json(plan.provider_payload(), "Iceberg metadata maintenance plan")?;
        validate_payload_for_plan(plan, &payload)?;
        Ok(payload)
    }

    fn marker(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        _payload: &IcebergMetadataMaintenancePlanPayloadV1,
    ) -> IcebergMetadataMaintenanceMarkerV1 {
        IcebergMetadataMaintenanceMarkerV1 {
            version: MARKER_VERSION,
            identity_digest_hex: hex_encode(identity_digest(&self.descriptor, &self.key, plan)),
            incarnation_hex: hex_encode(self.key.incarnation.to_bytes()),
            operation_id_hex: hex_encode(plan.operation_id().to_bytes()),
            operation_kind: plan.operation_kind().to_string(),
            request_digest_hex: hex_encode(plan.request_digest()),
            plan_digest_hex: hex_encode(plan.plan_digest()),
            intent_digest_hex: hex_encode(plan.intent_digest()),
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
            intent_digest_hex: hex_encode(plan.intent_digest()),
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

    async fn replay_terminal(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        original: ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>,
        context: &ConnectorRequestContext,
        execution: &Weak<MaintenanceExecution>,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } = &original else {
            return Ok(original);
        };
        let Some((journal, entry)) =
            PublicationJournal::<MaintenancePublication>::from_evidence(evidence)?
        else {
            return Ok(original);
        };
        if let Some(execution) = execution.upgrade() {
            execution.retain_journal(journal.clone());
        }
        if evidence.operation_id() != plan.operation_id()
            || evidence.incarnation() != self.key.incarnation
            || evidence.descriptor() != &self.descriptor
            || evidence.operation_kind() != plan.operation_kind()
        {
            return Err(invalid(
                "Maintenance replay evidence differs from the exact operation owner",
            ));
        }
        if matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::KnownCommitted { .. }
        ) {
            return Ok(public_outcome(journal.catalog_entry_outcome(entry)));
        }
        let payload = self.payload_for_plan(plan)?;
        let descriptor = journal
            .entry_descriptor(entry)
            .ok_or_else(|| invalid("Maintenance replay lost the exact descriptor"))?;
        let target = CatalogTableName::new(payload.namespace.clone(), payload.table.clone());
        let binding = self
            .runtime
            .resources()
            .planning_binding()
            .for_request(context.clone());
        let catalog = Arc::clone(self.runtime.novarocks_catalog());
        let read_target = target.clone();
        let observed = match catalog
            .load_complete_metadata_for_read(read_target, binding, None)
            .await
        {
            Ok(crate::catalog::CatalogMetadataLoad::Complete { base, .. }) => *base,
            _ => return Ok(original),
        };
        let Some(observation) = descriptor
            .attribution
            .observe_table_metadata(&observed.metadata)
        else {
            return Ok(original);
        };
        if !descriptor.attribution.matches_observation(&observation) {
            return Ok(original);
        }
        journal
            .append_positive_proof_with_finalization(
                descriptor.key,
                observation,
                self.runtime
                    .novarocks_catalog()
                    .positive_attribution_finalization(),
            )
            .map_err(iceberg_error)?;
        if let ExternalMutationOutcome::KnownCommitted {
            receipt,
            effect: ExternalMutationEffect::Applied,
            ..
        } = journal.catalog_entry_outcome(entry)
        {
            if let Some((operation, roots)) = receipt.artifact_cleanup {
                // IRU-5 owned-output cleanup is separate from snapshot GC. It
                // retains the original operation and exact frozen roots.
                let report = operation.cleanup_after_commit_in(&roots, context).await;
                journal.record_entry_cleanup_report(entry, report);
            }
            if let Some(retired) = receipt.retired {
                let mut proof = match journal.commit_outcome(entry) {
                    crate::catalog::error::CatalogOutcome::KnownCommitted { receipt, .. } => {
                        receipt
                    }
                    _ => unreachable!("positive proof is recorded"),
                };
                // This is a fresh diagnostic chain starting point, never a
                // substitute verdict or a planning-base predecessor claim.
                proof.metadata_location = Some(Arc::from(observed.metadata_location));
                let marker = String::from_utf8(
                    canonical_json(&self.marker(plan, &payload), "maintenance marker")?.to_vec(),
                )
                .map_err(|e| internal(e.to_string()))?;
                let owner = Arc::clone(&self.runtime);
                let gc_context = context.clone();
                let result = crate::commit::snapshot_lifecycle_helpers::reclamation::reclaim(
                    owner.as_ref(),
                    target,
                    &retired,
                    &marker,
                    &proof,
                    &gc_context,
                )
                .await;
                record_reclamation_result(&journal, entry, &receipt.reclamation, Ok(result));
            }
        }
        self.runtime
            .control_state()
            .invalidate_table_cache(&payload.namespace, &payload.table);
        Ok(public_outcome(journal.catalog_entry_outcome(entry)))
    }

    // Design: ADR-0243 (docs/adr/ADR-0243-statement-authorized-iceberg-metadata-maintenance.md)
    async fn execute_once(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        context: &ConnectorRequestContext,
        execution: &Weak<MaintenanceExecution>,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        context.check_active()?;
        let token = OperationToken::from_mutation(plan.operation_id());
        let runtime = self.runtime.resources().catalog_runtime();
        let initial = runtime
            .observe_preparation_async(
                async {
                    self.runtime
                        .observe_table_classified_for_request_with_snapshots_async(
                            &payload.namespace,
                            &payload.table,
                            context,
                            maintenance_snapshot_loading(plan.operation_kind()),
                        )
                        .await
                        .map_err(|(kind, message)| ConnectorError::new(kind, message))
                },
                connector_failure,
            )
            .await?;
        if initial.table.metadata().uuid().to_string() != payload.table_uuid {
            return Ok(rejected(invalid(
                "Iceberg maintenance table UUID was replaced",
            )));
        }
        if plan.operation_kind() == novarocks_spi::connector::REWRITE_METADATA_LAYOUT_KIND {
            crate::document_storage::keyed::view::management_marker(initial.table.metadata())
                .map_err(crate::document_storage::observation::keyed_read_error)?;
        }
        let policy = RetryPolicy::from_properties(initial.table.metadata().properties())
            .map_err(iceberg_error)?;
        let context = context.clone().with_earlier_deadline(
            Instant::now()
                .checked_add(policy.total_timeout())
                .ok_or_else(|| invalid("Maintenance retry deadline overflows monotonic time"))?,
        );
        let context = &context;
        let marker = String::from_utf8(
            canonical_json(&self.marker(plan, payload), "maintenance marker")?.to_vec(),
        )
        .map_err(|error| internal(error.to_string()))?;
        let ident = initial.table.identifier().clone();
        let operation = IcebergCommitOperation::new(
            token,
            initial.table.metadata().location(),
            self.runtime.resources().planning_binding().clone(),
            context.clone(),
            runtime.clone(),
            OperationLimits::default(),
        )
        .map_err(iceberg_error)?;
        let journal = PublicationJournal::for_runtime(token, runtime);
        if let Some(execution) = execution.upgrade() {
            execution.retain_operation(operation.clone(), journal.clone());
        }
        if plan.operation_kind() == novarocks_spi::connector::EXPIRE_TABLE_VERSIONS_KIND {
            return self
                .execute_expire(
                    plan, payload, context, &operation, &journal, policy, ident, marker,
                )
                .await;
        }
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: ident.clone(),
                uuid: Some(initial.table.metadata().uuid()),
            },
            target_ref: "main".into(),
            start: None,
            changes: FileChanges::default(),
            dependencies: vec![Dependency::NoRead],
            isolation: IsolationLevel::Serializable,
            shape: RequestShape::SnapshotProducing,
            summary: BTreeMap::from([(MARKER_PROPERTY.into(), marker)]),
            token,
        })
        .map_err(iceberg_error)?
        .with_snapshot_loading(SnapshotLoading::Refs);
        let evidence = self.evidence(plan, payload)?;
        let seed = ReceiptSeed {
            descriptor: self.descriptor.clone(),
            incarnation: self.key.incarnation,
            plan: plan.clone(),
        };
        let preflight_journal = journal.clone();
        let sql_execute = payload.sql_execute;
        let publisher = journal.observe_prepared(Arc::new(CatalogPublisher {
            expected_table_uuid: None,
            catalog_operation: CatalogOperation::RewriteManifests,
            catalog: Arc::clone(self.runtime.novarocks_catalog()),
            ident,
            target_ref: "main".into(),
            operation: token,
            recovery_preflight: Arc::new(move |request, operation| {
                let receipt = seed
                    .for_request(request, rewrite_summary(request, sql_execute)?)
                    .map_err(as_iceberg_error)?;
                preflight_journal.record(
                    evidence.clone(),
                    MaintenancePublication {
                        receipt,
                        retired: None,
                        artifact_cleanup: Some((
                            operation.clone(),
                            request.artifacts().references().iter().cloned().collect(),
                        )),
                        reclamation: Arc::new(Mutex::new(None)),
                    },
                    request,
                )
            }),
        }));
        let preparer = RewriteManifestsPreparer::for_request(context.clone(), sql_execute);
        let report = attempt::run(
            &operation,
            &intent,
            &PreparedChange::default(),
            &[&preparer],
            &publisher,
            policy,
        )
        .await;
        let outcome = public_outcome(journal.project(report));
        let outcome = if sql_execute {
            project_commit_failure(outcome, MaintenanceCommitProcedure::OptimizeManifests)
        } else {
            outcome
        };
        self.runtime
            .control_state()
            .invalidate_table_cache(&payload.namespace, &payload.table);
        Ok(outcome)
    }

    // Design: ADR-0243 (docs/adr/ADR-0243-statement-authorized-iceberg-metadata-maintenance.md)
    async fn execute_expire(
        &self,
        plan: &ConnectorMetadataMaintenancePlan,
        payload: &IcebergMetadataMaintenancePlanPayloadV1,
        context: &ConnectorRequestContext,
        operation: &IcebergCommitOperation,
        journal: &PublicationJournal<MaintenancePublication>,
        policy: RetryPolicy,
        ident: crate::iceberg::TableIdent,
        marker: String,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        let started = Instant::now();
        let runtime = self.runtime.resources().catalog_runtime();
        let mut ordinal = 0;
        let mut retained_decisions = 0u64;
        loop {
            if started.elapsed() >= policy.total_timeout() {
                return Ok(rejected(deadline("Maintenance retry budget expired")));
            }
            let prepared = runtime
                .observe_preparation_async(
                    async {
                        context.check_active()?;
                        let (guard, loaded) = self
                            .runtime
                            .coordinate_table_for_request_async(
                                &payload.namespace,
                                &payload.table,
                                context,
                            )
                            .await?;
                        let metadata = loaded.table.metadata();
                        if metadata.uuid().to_string() != payload.table_uuid {
                            return Err(invalid("Iceberg expiration table UUID was replaced"));
                        }
                        let mut decision = RetentionPlan::owned_decision_bytes(metadata).map_err(iceberg_error)?;
                        if payload.clean_expired_metadata {
                            decision = decision.checked_add(expiry_metadata::dictionary_backing_bytes(&loaded.table)?)
                                .ok_or_else(|| invalid("Retention dictionary backing overflow"))?;
                        }
                        retained_decisions = retained_decisions.checked_add(decision).ok_or_else(||invalid("retention journal backing overflow"))?;
                        let initial = 2*(novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES+novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_MARKER_BYTES) as u64;
                        let required = retained_decisions.checked_add(initial).and_then(std::num::NonZeroU64::new).ok_or_else(||invalid("retention journal backing overflow"))?;
                        // The source reserves old journal decisions plus the new
                        // decision before any owned retention clone is formed.
                        context.reserve_owned_metadata(required).await?;
                        let retention = if payload.sql_execute {
                            let cutoff = payload.older_than_ms.ok_or_else(|| invalid("Explicit expiration has no frozen cutoff"))?;
                            let keep = payload.retain_last.ok_or_else(|| invalid("Explicit expiration has no frozen retention count"))?;
                            RetentionPlan::compute_execute(metadata, payload.evaluation_time_ms, cutoff, keep)
                        } else {
                            RetentionPlan::compute(
                                metadata,
                                payload.evaluation_time_ms,
                                payload.older_than_ms,
                                payload.retain_last,
                            )
                        }.map_err(iceberg_error)?;
                        let mut updates = retention.updates();
                        if payload.clean_expired_metadata {
                            updates.extend(expiry_metadata::unused_dictionary_updates(
                                &loaded.table,
                                &retention,
                                context,
                                &self.descriptor,
                                self.key.incarnation,
                                required,
                            ).await?);
                        }
                        if !updates.is_empty() {
                            updates.push(crate::iceberg::TableUpdate::SetProperties {
                                updates: HashMap::from([(MARKER_PROPERTY.into(), marker.clone())]),
                            });
                        }
                        let location = loaded.table.metadata_location().ok_or_else(|| {
                            invalid("Expiration requires an authoritative metadata location")
                        })?;
                        let request = crate::commit::staging::freeze_existing_change(
                            ident.clone(),
                            "main",
                            metadata,
                            location,
                            RequestShape::RefRetention,
                            AttemptToken::new(operation.token(), ordinal),
                            PreparedChange {
                                requirements: retention.requirements.clone(),
                                updates,
                            },
                            context,
                        )
                        .map_err(iceberg_error)?;
                        Ok((guard, retention, request))
                    },
                    connector_failure,
                )
                .await;
            let (guard, retention, request) = match prepared {
                Ok(prepared) => prepared,
                Err(error) => return Ok(rejected(error)),
            };
            if started.elapsed() >= policy.total_timeout() {
                return Ok(rejected(deadline(
                    "Maintenance retry budget expired after preparation",
                )));
            }
            let reclaims_files = !payload.sql_execute || !retention.removed_snapshots.is_empty();
            let receipt = ReceiptSeed {
                descriptor: self.descriptor.clone(),
                incarnation: self.key.incarnation,
                plan: plan.clone(),
            }
            .for_request(
                &request,
                ConnectorMetadataMaintenanceReceiptSummary {
                    affected_versions: retention.removed_snapshots.len() as u64,
                    ..Default::default()
                },
            )?;
            journal
                .record(
                    self.evidence(plan, payload)?,
                    MaintenancePublication {
                        receipt,
                        retired: reclaims_files.then(|| Arc::new(retention.clone())),
                        artifact_cleanup: None,
                        reclamation: Arc::new(Mutex::new(None)),
                    },
                    &request,
                )
                .map_err(iceberg_error)?;
            let catalog = Arc::clone(self.runtime.novarocks_catalog());
            let dispatch_journal = journal.clone();
            let preparation = context.clone();
            let evidence_policy = catalog.evidence_policy();
            let published = async move {
                dispatch_journal
                    .publish_once(
                        preparation,
                        evidence_policy,
                        move |descriptor, generation, preparation| async move {
                            catalog
                                .prepare_frozen_publication(
                                    request,
                                    CatalogOperation::ExpireSnapshots,
                                    None,
                                    descriptor,
                                    generation,
                                    preparation,
                                )
                                .await
                        },
                    )
                    .await
            }
            .await;
            // The in-process table lock covers this round's fresh decision
            // and dispatch conclusion. It never covers backoff or reclamation.
            drop(guard);
            let outcome = match published {
                Ok(entry) => public_outcome(journal.catalog_entry_outcome(entry)),
                Err(error) => public_outcome(journal.catalog_bridge_failure(error.to_string())),
            };
            if let ExternalMutationOutcome::KnownUncommitted { ref failure, .. } = outcome
                && failure.kind() == ConnectorMutationFailureKind::Conflict
                && ordinal < policy.retries()
            {
                let wait = policy.backoff(ordinal);
                if wait >= policy.total_timeout().saturating_sub(started.elapsed()) {
                    return Ok(if payload.sql_execute {
                        project_commit_failure(outcome, MaintenanceCommitProcedure::ExpireSnapshots)
                    } else {
                        outcome
                    });
                }
                if let Err(error) = operation.pause(wait).await.map_err(iceberg_error) {
                    return Ok(rejected(error));
                }
                ordinal += 1;
                continue;
            }
            self.runtime
                .control_state()
                .invalidate_table_cache(&payload.namespace, &payload.table);
            // Reclamation consumes only the exact Applied entry; it owns an
            // independent finite grant and cannot alter the publication fact.
            if reclaims_files
                && matches!(
                    outcome,
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        ..
                    }
                )
            {
                let entry = journal
                    .current_entry()
                    .expect("Applied expiration has an entry");
                let proof = match journal.commit_outcome(entry) {
                    crate::catalog::error::CatalogOutcome::KnownCommitted { receipt, .. } => {
                        receipt
                    }
                    _ => unreachable!("Applied entry retains its proof"),
                };
                let report_slot = match journal.catalog_entry_outcome(entry) {
                    ExternalMutationOutcome::KnownCommitted { receipt, .. } => receipt.reclamation,
                    _ => unreachable!("Applied entry retains its receipt"),
                };
                let reclamation_owner = Arc::clone(&self.runtime);
                let target =
                    CatalogTableName::new(payload.namespace.clone(), payload.table.clone());
                let reclamation_context = context.clone();
                let result = crate::commit::snapshot_lifecycle_helpers::reclamation::reclaim(
                    reclamation_owner.as_ref(),
                    target,
                    &retention,
                    &marker,
                    &proof,
                    &reclamation_context,
                )
                .await;
                record_reclamation_result(journal, entry, &report_slot, Ok(result));
                return Ok(public_outcome(journal.catalog_entry_outcome(entry)));
            }
            return Ok(if payload.sql_execute {
                project_commit_failure(outcome, MaintenanceCommitProcedure::ExpireSnapshots)
            } else {
                outcome
            });
        }
    }
}

/// Pure first-party fixed geometry from the same metadata execution factory.
/// No catalog instance, route, credential, lease, Work, or network is created.
pub fn metadata_maintenance_io_geometry()
-> Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry, ConnectorError> {
    // Exact original io_requests, not the cleanup object bound or a new
    // semaphore. Header/F costs exclude borrowed SDK/cache/private graphs.
    let io = std::num::NonZeroUsize::new(OperationLimits::default().io_requests as usize)
        .ok_or_else(|| internal("metadata operation I/O inventory is empty"))?;
    let fixed = std::mem::size_of::<MaintenanceEngine>()
        .checked_add(std::mem::size_of::<MetadataExecutionInput>())
        .and_then(|n| n.checked_add(metadata_execution_future_bytes(make_metadata_execution)))
        .and_then(|n| n.checked_add(execution::fixed_backing_bytes()))
        .and_then(|n| n.checked_add(observation::fixed_backing_bytes()?))
        .and_then(|n| n.checked_add(IcebergCommitOperation::fixed_backing_bytes().ok()?))
        .and_then(|n| {
            n.checked_add(
                crate::fs_io::governed_writer_backing_bytes()
                    .ok()?
                    .checked_mul(io.get())?,
            )
        })
        .ok_or_else(|| internal("metadata source geometry overflow"))?;
    // Canonical plan/evidence String+Bytes and old/new conversion coexist.
    // Variable retention/journal plans are reserved from observed metadata
    // at their actual formation points, not hidden in this initial bound.
    let initial = 2usize
        .checked_mul(
            novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES,
        )
        .and_then(|n| {
            n.checked_add(
                2 * novarocks_spi::connector::MAX_CONNECTOR_METADATA_MAINTENANCE_MARKER_BYTES,
            )
        })
        .ok_or_else(|| internal("metadata initial payload geometry overflow"))?;
    Ok(
        novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry::new(
            io,
            std::num::NonZeroU64::new(
                u64::try_from(fixed).map_err(|_| internal("metadata source geometry overflow"))?,
            )
            .unwrap(),
            std::num::NonZeroU64::new(
                u64::try_from(initial)
                    .map_err(|_| internal("metadata initial payload overflow"))?,
            )
            .unwrap(),
        ),
    )
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
    ) -> ConnectorMetadataMaintenanceFuture<'_, ConnectorMetadataMaintenancePlan> {
        Box::pin(async move {
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
            let (payload, intent_digest, summary) = self.plan_payload(&request).await?;
            let provider_payload = canonical_json(&payload, "Iceberg metadata maintenance plan")?;
            let plan = ConnectorMetadataMaintenancePlan::try_new(
                &request,
                intent_digest,
                summary,
                provider_payload,
            )?;
            Ok(plan)
        })
    }

    fn io_profile(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile, ConnectorError>
    {
        let geometry = metadata_maintenance_io_geometry()?;
        let dependencies = self.runtime.metadata_action_initial_dependencies()?;
        novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile::from_geometry(
            geometry,
            dependencies,
        )
    }

    fn begin_execute(
        &self,
        request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ConnectorMetadataMaintenanceExecution, ConnectorError> {
        request.plan.validate()?;
        self.ensure_owner(request.plan.owner())?;
        let mut active = self
            .active
            .lock()
            .map_err(|_| internal("Maintenance active set poisoned"))?;
        if active.contains_key(&request.plan.operation_id()) {
            return Err(invalid(
                "Maintenance operation already has an active execution",
            ));
        }
        let terminal = self
            .terminal
            .lock()
            .map_err(|_| internal("Maintenance terminal set poisoned"))?
            .get(&request.plan.operation_id())
            .cloned();
        if let Some(terminal) = &terminal
            && terminal.plan_digest != request.plan.plan_digest()
        {
            return Err(invalid("Maintenance replay changes the fixed plan"));
        }
        let payload = if terminal.is_none() {
            Some(self.payload_for_plan(&request.plan)?)
        } else {
            None
        };
        let row = Arc::new(MaintenanceExecution::new(
            request.plan.operation_id(),
            request.plan.plan_digest(),
            Arc::downgrade(&self.active),
            Arc::clone(&self.terminal),
        ));
        // The original active row exists before any physical future can be polled.
        let weak = Arc::downgrade(&row);
        let engine = Arc::clone(&self.engine);
        let future = Box::pin(make_metadata_execution(MetadataExecutionInput {
            engine,
            request,
            terminal,
            payload,
            weak,
        }));
        row.install(future);
        active.insert(row.operation, Arc::clone(&row));
        drop(active);
        Ok(ConnectorMetadataMaintenanceExecution::from_source(row))
    }

    fn begin_read_max_compactable_data_files(
        &self,
        request: ConnectorMaxCompactableDataFilesRequest,
        cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> Result<novarocks_spi::connector::ConnectorMaxCompactableDataFilesExecution, ConnectorError>
    {
        if request.table.owner() != &self.key.instance_id {
            return Err(invalid("compaction observation has a foreign table owner"));
        }
        Ok(
            novarocks_spi::connector::ConnectorMaxCompactableDataFilesExecution::from_source(
                observation::ObservationSource::new(Arc::clone(&self.engine), request, cpu),
            ),
        )
    }

    fn read_max_compactable_data_files(
        &self,
        request: ConnectorMaxCompactableDataFilesRequest,
    ) -> ConnectorMetadataMaintenanceFuture<'_, ConnectorMaxCompactableDataFiles> {
        Box::pin(async move {
            let (namespace, table_name) = decode_base_table_target(&request.table)?;
            // The observation feeds a maintenance decision, so it must see the
            // current table state rather than a cached generation snapshot.
            self.runtime
                .control_state()
                .invalidate_table_cache(&namespace, &table_name);
            let physical = self
                .runtime
                .load_table_for_request_async(&namespace, &table_name, &request.context)
                .await
                .map_err(|error| {
                    unavailable(format!("load Iceberg table for observation: {error}"))
                })?;
            let preserve_row_lineage =
                crate::schema_facts::row_lineage_enabled(physical.table.metadata());
            let table = physical.into_table();
            let stats = crate::commit::current_live_data_file_compaction_stats(
                &table,
                table.file_io(),
                preserve_row_lineage,
            )
            .await
            .map_err(|error| {
                if error.kind() == ConnectorErrorKind::Unsupported {
                    error
                } else {
                    unavailable(format!(
                        "observe Iceberg table {namespace}.{table_name} compaction groups: {error}"
                    ))
                }
            })?;
            let value = u64::try_from(stats.max_compactable_data_files).map_err(|_| {
                internal(format!(
                    "Iceberg table {namespace}.{table_name} compactable file count overflow"
                ))
            })?;
            Ok(ConnectorMaxCompactableDataFiles::new(Some(value)))
        })
    }
}

struct MetadataExecutionInput {
    engine: Arc<MaintenanceEngine>,
    request: ConnectorMetadataMaintenanceExecuteRequest,
    terminal: Option<TerminalRecord>,
    payload: Option<IcebergMetadataMaintenancePlanPayloadV1>,
    weak: Weak<MaintenanceExecution>,
}
async fn make_metadata_execution(
    input: MetadataExecutionInput,
) -> Result<ConnectorMetadataMaintenanceOutcome, ConnectorError> {
    let MetadataExecutionInput {
        engine,
        request,
        terminal,
        payload,
        weak,
    } = input;
    if let Some(terminal) = terminal {
        engine
            .replay_terminal(&request.plan, terminal.outcome, &request.context, &weak)
            .await
    } else {
        engine
            .execute_once(
                &request.plan,
                payload
                    .as_ref()
                    .expect("new execution has a validated payload"),
                &request.context,
                &weak,
            )
            .await
    }
}
fn metadata_execution_future_bytes<I, F>(_: fn(I) -> F) -> usize {
    std::mem::size_of::<F>()
}

#[path = "metadata_maintenance/execution.rs"]
mod execution;
use execution::MaintenanceExecution;

// The original journal retains candidate descriptors even for Unknown. This
// state is process-local and is never reconstructed from a wire-only marker.
#[derive(Clone)]
pub(crate) struct MaintenancePublication {
    receipt: ConnectorMetadataMaintenanceReceipt,
    retired: Option<Arc<RetentionPlan>>,
    artifact_cleanup: Option<(
        IcebergCommitOperation,
        Vec<crate::commit::model::ObjectIdentity>,
    )>,
    reclamation: Arc<Mutex<Option<ReclamationReport>>>,
}
fn project_commit_failure(
    outcome: ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>,
    procedure: MaintenanceCommitProcedure,
) -> ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt> {
    match outcome {
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            ExternalMutationOutcome::KnownUncommitted {
                failure: commit_failure(
                    procedure,
                    CommitFailureDisposition::KnownUncommitted,
                    failure,
                ),
                cleanup,
            }
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            ExternalMutationOutcome::CommitUnknown {
                failure: commit_failure(procedure, CommitFailureDisposition::Unknown, failure),
                evidence,
            }
        }
        committed => committed,
    }
}

fn public_outcome(
    outcome: ExternalMutationOutcome<MaintenancePublication>,
) -> ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt> {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            cleanup,
            effect,
            receipt,
            finalization,
        } => {
            let mut cleanup = cleanup;
            let report = receipt
                .reclamation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let mut public = receipt.receipt;
            if let Some(report) = report {
                let mut summary = public.summary();
                summary.cleanup_succeeded = report.deleted as u64;
                summary.cleanup_failed = report.failed as u64;
                match ConnectorMetadataMaintenanceReceipt::try_new(
                    public.descriptor().clone(),
                    public.incarnation(),
                    public.operation_id(),
                    public.operation_kind(),
                    public.request_digest(),
                    public.plan_digest(),
                    public.intent_digest(),
                    summary,
                    public.provider_payload().clone(),
                ) {
                    Ok(enriched) => public = enriched,
                    Err(error) => {
                        if matches!(cleanup, ExternalMutationFinalization::Complete) {
                            cleanup =
                                ExternalMutationFinalization::Failed(connector_failure(&error));
                        }
                    }
                }
            }
            ExternalMutationOutcome::KnownCommitted {
                cleanup,
                effect,
                receipt: public,
                finalization,
            }
        }
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            ExternalMutationOutcome::CommitUnknown { failure, evidence }
        }
    }
}

fn record_reclamation_result(
    journal: &PublicationJournal<MaintenancePublication>,
    entry: crate::commit::attempt::observation::JournalEntryRef,
    slot: &Mutex<Option<ReclamationReport>>,
    result: std::result::Result<ReclamationReport, String>,
) {
    let failure = match result {
        Ok(report) => {
            let failure = report.diagnostic_failure();
            *slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(report);
            failure
        }
        Err(message) => Some(unavailable(format!(
            "Reclamation bridge exited without a complete progress report: {message}"
        ))),
    };
    if let Some(error) = failure {
        journal.record_entry_cleanup(
            entry,
            ExternalMutationFinalization::Failed(connector_failure(&error)),
        );
    }
}

struct ReceiptSeed {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: novarocks_spi::connector::ProviderBindingEpoch,
    plan: ConnectorMetadataMaintenancePlan,
}
impl ReceiptSeed {
    fn for_request(
        &self,
        request: &FrozenRequest,
        summary: ConnectorMetadataMaintenanceReceiptSummary,
    ) -> Result<ConnectorMetadataMaintenanceReceipt, ConnectorError> {
        let BaseIdentity::Existing {
            parent,
            metadata_location,
            ..
        } = request.base()
        else {
            return Err(invalid("Maintenance requires an existing table base"));
        };
        let descriptor =
            crate::commit::publication_descriptor::PublicationDescriptor::frozen(request)
                .map_err(iceberg_error)?;
        let attempt = request.artifacts().attempt();
        ConnectorMetadataMaintenanceReceipt::try_new(
            self.descriptor.clone(),
            self.incarnation,
            self.plan.operation_id(),
            self.plan.operation_kind(),
            self.plan.request_digest(),
            self.plan.plan_digest(),
            self.plan.intent_digest(),
            summary,
            canonical_json(
                &IcebergMetadataMaintenanceReceiptV1 {
                    version: PAYLOAD_VERSION,
                    snapshot_id: request.ref_snapshot_after("main").or(*parent),
                    attempt_id_hex: hex_encode(attempt.nonce_bytes()),
                    attempt_ordinal: attempt.ordinal(),
                    base_snapshot_id: *parent,
                    metadata_location_digest_hex: hex_encode(metadata_location_digest(Some(
                        metadata_location,
                    ))),
                    frozen_request_digest_hex: hex_encode(descriptor.fingerprint),
                },
                "maintenance receipt",
            )?,
        )
    }
}
/// Current manifest layout uses ref heads; expiry and positive historical
/// adjudication continue to use the complete representation.
fn maintenance_snapshot_loading(operation_kind: &str) -> SnapshotLoading {
    if operation_kind == novarocks_spi::connector::REWRITE_METADATA_LAYOUT_KIND {
        SnapshotLoading::Refs
    } else {
        SnapshotLoading::All
    }
}

fn rewrite_summary(
    request: &FrozenRequest,
    sql_execute: bool,
) -> crate::iceberg::Result<ConnectorMetadataMaintenanceReceiptSummary> {
    let mut summary = ConnectorMetadataMaintenanceReceiptSummary::default();
    for update in request.updates() {
        if let crate::iceberg::TableUpdate::AddSnapshot { snapshot } = update {
            let properties = &snapshot.summary().additional_properties;
            if sql_execute {
                let metrics = execute_metrics(properties)?;
                summary.rewritten_items = metrics.rewritten;
                summary.added_items = metrics.added;
                summary.manifest_rewrite_metrics = Some(metrics);
                continue;
            }
            summary.rewritten_items = properties
                .get("replaced-manifests-count")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            summary.added_items = properties
                .get("added-manifests-count")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    Ok(summary)
}
fn connector_failure(error: &ConnectorError) -> ConnectorMutationFailure {
    attempt::before_dispatch_failure(&as_iceberg_error(error.clone()))
}
fn rejected(error: ConnectorError) -> ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: connector_failure(&error),
        cleanup: ExternalMutationFinalization::Complete,
    }
}
fn as_iceberg_error(error: ConnectorError) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, error.to_string())
        .with_source(error)
}
fn iceberg_error(error: crate::iceberg::Error) -> ConnectorError {
    // Preserve typed cancellation/deadline facts instead of classifying text.
    let failure = attempt::before_dispatch_failure(&error);
    let kind = match failure.kind() {
        ConnectorMutationFailureKind::Cancelled => ConnectorErrorKind::Cancelled,
        ConnectorMutationFailureKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        ConnectorMutationFailureKind::Unsupported => ConnectorErrorKind::Unsupported,
        ConnectorMutationFailureKind::InvalidRequest | ConnectorMutationFailureKind::Conflict => {
            ConnectorErrorKind::InvalidRequest
        }
        _ => ConnectorErrorKind::Unavailable,
    };
    ConnectorError::new(kind, error.to_string())
}
fn deadline(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::DeadlineExceeded, message)
}
fn now_ms() -> Result<i64, ConnectorError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| internal(e.to_string()))?
            .as_millis(),
    )
    .map_err(|_| internal("Maintenance clock overflow"))
}

fn decode_base_table_target(
    handle: &novarocks_spi::connector::ConnectorTableHandle,
) -> Result<(String, String), ConnectorError> {
    let target: IcebergTablePayload = crate::metadata_json::from_slice(handle.payload())
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
    if payload.version != PLAN_PAYLOAD_VERSION
        || (payload.clean_expired_metadata && !payload.sql_execute)
        || (payload.operation_kind == novarocks_spi::connector::EXPIRE_TABLE_VERSIONS_KIND
            && payload.sql_execute
            && (payload.older_than_ms.is_none()
                || !payload.retain_last.is_some_and(|count| count > 0)))
        || payload.operation_kind != plan.operation_kind()
        || payload.evaluation_time_ms <= 0
        || intent_digest(payload)? != plan.intent_digest()
    {
        return Err(invalid(
            "Iceberg metadata maintenance plan payload does not match SPI plan",
        ));
    }
    Ok(())
}

fn intent_digest(
    payload: &IcebergMetadataMaintenancePlanPayloadV1,
) -> Result<[u8; 32], ConnectorError> {
    let mut hasher = Sha256::new();
    hasher.update(STATE_DOMAIN);
    hasher.update(canonical_json(payload, "maintenance immutable intent")?);
    Ok(hasher.finalize().into())
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

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
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

#[cfg(test)]
fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if value.len() & 1 != 0 {
        return None;
    }
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    let mut decoded = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks(2) {
        decoded.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Some(decoded)
}

#[cfg(test)]
#[path = "metadata_maintenance/tests/mod.rs"]
mod tests;
