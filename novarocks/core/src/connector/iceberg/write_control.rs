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

//! FE-owned Iceberg control adapter for the provider-neutral writer SPI.
//!
//! The adapter deliberately contains no catalog client, object-store
//! credential, or BE writer state.  Composition supplies an
//! [`IcebergWriteControlBackend`] that turns canonical Iceberg reports into the
//! existing commit runner's input.  This keeps the generic FE transaction
//! bridge independent from Iceberg while preserving operation/report
//! idempotency at the provider boundary.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ConnectorStagedReport, ConnectorWriteAbortOutcome, ConnectorWriteAbortRequest,
    ConnectorWriteAttemptCompletion, ConnectorWriteCohortId, ConnectorWriteCommitRequest,
    ConnectorWriteControl, ConnectorWriteExecutionId, ConnectorWriteOperationId,
    ConnectorWritePlan, ConnectorWritePlanningRequest, ConnectorWriteReceipt,
    ConnectorWriteReconcileRequest, ConnectorWriterTerminalState, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
};

use super::commit::{CommitOutcome, CommitServiceError, RecoveryEvidence};
use super::write_contract::connector_write_receipt;

const ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION: u16 = 2;
const ICEBERG_WRITE_OPERATION_KIND: &str = "iceberg.connector_write.v2";
const ICEBERG_WRITE_PLAN_PAYLOAD_VERSION: u16 = 1;
const ICEBERG_FIRST_REFRESH_WRITE_PLAN_PAYLOAD_VERSION: u16 = 2;
const MAX_FIRST_REFRESH_STAGING_PATH_BYTES: usize = 4 * 1024;

/// Canonical, secret-free FE control payload.  The planner may use `target`
/// to describe a catalog table/ref, but must never put a catalog client or
/// storage credentials here.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergWritePlanPayloadV1 {
    pub version: u16,
    pub target: String,
    pub target_ref: String,
}

impl IcebergWritePlanPayloadV1 {
    pub(crate) fn encode(&self) -> Result<Bytes, ConnectorError> {
        if self.version != ICEBERG_WRITE_PLAN_PAYLOAD_VERSION
            || self.target.is_empty()
            || self.target_ref.is_empty()
        {
            return Err(invalid("invalid Iceberg write plan payload"));
        }
        canonical_json(self, "Iceberg write plan payload")
    }

    pub(crate) fn decode(payload: &[u8]) -> Result<Self, ConnectorError> {
        let decoded: Self = decode_canonical_json(payload, "Iceberg write plan payload")?;
        if decoded.version != ICEBERG_WRITE_PLAN_PAYLOAD_VERSION
            || decoded.target.is_empty()
            || decoded.target_ref.is_empty()
        {
            return Err(invalid(
                "unsupported or incomplete Iceberg write plan payload",
            ));
        }
        if canonical_json(&decoded, "Iceberg write plan payload")?.as_ref() != payload {
            return Err(invalid(
                "Iceberg write plan payload is not canonical JSON v1",
            ));
        }
        Ok(decoded)
    }
}

/// Provider-private facts for a first-refresh append. The application layer
/// retains this only as an opaque payload; validation and provenance handling
/// remain inside the Iceberg control binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergFirstRefreshWritePlanPayloadV2 {
    pub version: u16,
    pub target: String,
    pub target_ref: String,
    pub expected_snapshot_id: Option<i64>,
    pub staging_path: String,
    pub provenance_properties: BTreeMap<String, String>,
}

impl IcebergFirstRefreshWritePlanPayloadV2 {
    pub(crate) fn encode(&self) -> Result<Bytes, ConnectorError> {
        self.validate()?;
        canonical_json(self, "Iceberg first-refresh write plan payload")
    }

    pub(crate) fn decode(payload: &[u8]) -> Result<Self, ConnectorError> {
        let decoded: Self =
            decode_canonical_json(payload, "Iceberg first-refresh write plan payload")?;
        decoded.validate()?;
        if canonical_json(&decoded, "Iceberg first-refresh write plan payload")?.as_ref() != payload
        {
            return Err(invalid(
                "Iceberg first-refresh write plan payload is not canonical JSON v2",
            ));
        }
        Ok(decoded)
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.version != ICEBERG_FIRST_REFRESH_WRITE_PLAN_PAYLOAD_VERSION
            || self.target.is_empty()
            || self.target_ref.is_empty()
            || self.staging_path.is_empty()
            || self.staging_path.len() > MAX_FIRST_REFRESH_STAGING_PATH_BYTES
            || self
                .expected_snapshot_id
                .is_some_and(|snapshot_id| snapshot_id < 0)
            || self
                .provenance_properties
                .iter()
                .any(|(key, value)| key.is_empty() || value.is_empty())
        {
            return Err(invalid(
                "unsupported or incomplete Iceberg first-refresh write plan payload",
            ));
        }
        Ok(())
    }
}

/// Provider-owned planning result.  The backend uses
/// `write_contract::writer_handle_from_sink_plan` to create each handle from
/// its private sink plan; this adapter only validates the generic manifest.
#[derive(Clone)]
pub(crate) struct IcebergWriteControlPlan {
    pub handles: Vec<novarocks_spi::connector::ConnectorWriterHandle>,
    pub control_payload: Bytes,
}

/// Narrow adapter over the existing Iceberg planning/commit/cleanup code.
/// It is intentionally synchronous because the SPI contract is synchronous;
/// the eventual composition root supplies the existing transaction runner.
pub(crate) trait IcebergWriteControlBackend: Send + Sync {
    fn plan(
        &self,
        request: &ConnectorWritePlanningRequest,
    ) -> Result<IcebergWriteControlPlan, ConnectorError>;

    fn commit(
        &self,
        request: &ConnectorWriteCommitRequest,
    ) -> Result<CommitOutcome, CommitServiceError>;

    fn abort(
        &self,
        request: &ConnectorWriteAbortRequest,
    ) -> Result<ExternalMutationFinalization, ConnectorError>;

    /// `Ok(Some(outcome))` means the external snapshot is known committed;
    /// `Ok(None)` means reconciliation proved it uncommitted.
    fn reconcile(
        &self,
        evidence: &IcebergWriteReconcileEvidenceV1,
    ) -> Result<Option<CommitOutcome>, CommitServiceError>;

    /// Returns the committed table row count only when the operation has an
    /// MV provenance contract. The value is derived from the committed
    /// snapshot's `total-records`, never from frontend-visible staged input.
    fn resulting_row_count(
        &self,
        _: ConnectorWriteOperationId,
        _: &CommitOutcome,
    ) -> Result<Option<u64>, CommitServiceError> {
        Ok(None)
    }
}

#[derive(Clone)]
pub(crate) struct IcebergWriteControlAdapter {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    backend: Arc<dyn IcebergWriteControlBackend>,
    operations: Arc<Mutex<HashMap<ConnectorWriteOperationId, IcebergWriteOperationRecord>>>,
    aborts: Arc<Mutex<HashMap<ConnectorWriteOperationId, IcebergWriteAbortRecord>>>,
    plans: Arc<Mutex<HashMap<ConnectorWriteOperationId, IcebergWriteOperationPlans>>>,
}

#[derive(Clone)]
struct CachedPlan {
    attempt_digest: [u8; 32],
    plan: ConnectorWritePlan,
}

#[derive(Clone)]
struct IcebergWriteCohortPlans {
    stable_digest: [u8; 32],
    attempts: HashMap<ConnectorWriteExecutionId, CachedPlan>,
}

#[derive(Clone, Default)]
struct IcebergWriteOperationPlans {
    cohorts: HashMap<ConnectorWriteCohortId, IcebergWriteCohortPlans>,
}

#[derive(Clone)]
struct IcebergWriteOperationRecord {
    cohort_set_digest: [u8; 32],
    aggregate_digest: [u8; 32],
    outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
}

#[derive(Clone)]
struct IcebergWriteAbortRecord {
    cohort_set_digest: [u8; 32],
    aggregate_digest: [u8; 32],
    outcome: ConnectorWriteAbortOutcome,
}

/// Provider payload inside [`ExternalMutationEvidence`].  The outer envelope
/// supplies the exact connector descriptor/incarnation; these fields bind the
/// evidence to the write operation and report set.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergWriteReconcileEvidenceV1 {
    version: u16,
    operation_id_base64: String,
    cohort_set_digest_base64: String,
    aggregate_digest_base64: String,
    recovery: IcebergRecoveryEvidenceV1,
}

impl IcebergWriteReconcileEvidenceV1 {
    pub(crate) fn operation_id(&self) -> Result<ConnectorWriteOperationId, ConnectorError> {
        Ok(ConnectorWriteOperationId::from_bytes(decode_16(
            &self.operation_id_base64,
            "operation id",
        )?))
    }

    pub(crate) fn aggregate_digest(&self) -> Result<[u8; 32], ConnectorError> {
        decode_32(&self.aggregate_digest_base64, "aggregate digest")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergRecoveryEvidenceV1 {
    table_ident: String,
    op_kind: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: i64,
    staging_dir: String,
}

impl IcebergWriteControlAdapter {
    pub(crate) fn new(
        key: ConnectorExecutionBindingKey,
        backend: Arc<dyn IcebergWriteControlBackend>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        Ok(Self {
            key,
            descriptor,
            backend,
            operations: Arc::new(Mutex::new(HashMap::new())),
            aborts: Arc::new(Mutex::new(HashMap::new())),
            plans: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn ensure_owner(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg write request does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    fn receipt(
        &self,
        operation_id: ConnectorWriteOperationId,
        outcome: &CommitOutcome,
    ) -> Result<ConnectorWriteReceipt, ConnectorError> {
        let resulting_row_count = self
            .backend
            .resulting_row_count(operation_id, outcome)
            .map_err(|error| internal(format!("read Iceberg committed row count: {error:?}")))?;
        connector_write_receipt(outcome.new_snapshot_id, resulting_row_count)
            .map_err(|error| internal(format!("encode Iceberg write receipt: {error}")))
    }

    fn commit_outcome(
        &self,
        request: &ConnectorWriteCommitRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        match self.backend.commit(request) {
            Ok(outcome) => {
                let receipt = self.receipt(request.operation_id(), &outcome)?;
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            Err(CommitServiceError::KnownUncommitted { message, .. }) => {
                Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: failure(ConnectorMutationFailureKind::Conflict, message),
                })
            }
            Err(CommitServiceError::FinalizeFailedKnownCommitted {
                outcome,
                finalize_error,
                ..
            }) => {
                let receipt = outcome
                    .as_ref()
                    .map(|outcome| self.receipt(request.operation_id(), outcome))
                    .transpose()?
                    .ok_or_else(|| {
                        internal(
                            "Iceberg known-committed write finalization has no committed snapshot",
                        )
                    })?;
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Failed(failure(
                        ConnectorMutationFailureKind::Internal,
                        finalize_error,
                    )),
                })
            }
            Err(CommitServiceError::Unknown { message, evidence }) => {
                Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: failure(ConnectorMutationFailureKind::Unavailable, message),
                    evidence: self.evidence(request, evidence)?,
                })
            }
            Err(CommitServiceError::InvalidInput { message }) => {
                Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: failure(ConnectorMutationFailureKind::InvalidRequest, message),
                })
            }
        }
    }

    fn evidence(
        &self,
        request: &ConnectorWriteCommitRequest,
        recovery: RecoveryEvidence,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let payload = canonical_json(
            &IcebergWriteReconcileEvidenceV1 {
                version: ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION,
                operation_id_base64: base64_encode(request.operation_id().to_bytes()),
                cohort_set_digest_base64: base64_encode(request.sealed().digest()),
                aggregate_digest_base64: base64_encode(request.aggregate_digest()),
                recovery: IcebergRecoveryEvidenceV1 {
                    table_ident: recovery.table_ident,
                    op_kind: format!("{:?}", recovery.op_kind),
                    base_snapshot_id: recovery.base_snapshot_id,
                    base_sequence_number: recovery.base_sequence_number,
                    staging_dir: recovery.staging_dir,
                },
            },
            "Iceberg write reconciliation evidence",
        )?;
        ExternalMutationEvidence::try_new(
            ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.key.incarnation,
            ConnectorMutationOperationId::from_bytes(request.operation_id().to_bytes()),
            ICEBERG_WRITE_OPERATION_KIND,
            payload,
        )
    }

    fn decode_evidence(
        &self,
        evidence: &ExternalMutationEvidence,
    ) -> Result<IcebergWriteReconcileEvidenceV1, ConnectorError> {
        if evidence.schema_version() != ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION
            || evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.key.incarnation
            || evidence.operation_kind() != ICEBERG_WRITE_OPERATION_KIND
        {
            return Err(invalid(
                "Iceberg write reconciliation evidence has a foreign connector generation",
            ));
        }
        let decoded: IcebergWriteReconcileEvidenceV1 = decode_canonical_json(
            evidence.provider_payload(),
            "Iceberg write reconciliation evidence",
        )?;
        if decoded.version != ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION
            || canonical_json(&decoded, "Iceberg write reconciliation evidence")?.as_ref()
                != evidence.provider_payload().as_ref()
        {
            return Err(invalid(
                "Iceberg write reconciliation evidence is not canonical v1",
            ));
        }
        let operation_id = decode_16(&decoded.operation_id_base64, "operation id")?;
        if evidence.operation_id() != ConnectorMutationOperationId::from_bytes(operation_id) {
            return Err(invalid(
                "Iceberg write reconciliation operation ID mismatch",
            ));
        }
        decode_32(&decoded.cohort_set_digest_base64, "cohort set digest")?;
        decode_32(&decoded.aggregate_digest_base64, "aggregate digest")?;
        Ok(decoded)
    }
}

impl ConnectorWriteControl for IcebergWriteControlAdapter {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_write(
        &self,
        request: ConnectorWritePlanningRequest,
    ) -> Result<ConnectorWritePlan, ConnectorError> {
        self.ensure_owner(
            request
                .expected_writers
                .first()
                .map(|writer| writer.binding_key())
                .unwrap_or(&self.key),
        )?;
        request.validate(&self.key)?;
        let stable_digest = request.stable_digest(&self.key)?;
        let attempt_digest = planning_attempt_digest(&request);
        let mut plans = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg write plan lock: {error}")))?;
        let operation = plans.entry(request.operation_id).or_default();
        let cohort = operation
            .cohorts
            .entry(request.cohort_id)
            .or_insert_with(|| IcebergWriteCohortPlans {
                stable_digest,
                attempts: HashMap::new(),
            });
        if cohort.stable_digest != stable_digest {
            return Err(invalid(
                "Iceberg write cohort was replanned with different stable inputs",
            ));
        }
        if let Some(cached) = cohort.attempts.get(&request.execution_id) {
            if cached.attempt_digest == attempt_digest {
                return Ok(cached.plan.clone());
            }
            return Err(invalid(
                "Iceberg write execution attempt was replayed with different writer inputs",
            ));
        }
        let plan = self.backend.plan(&request)?;
        let plan = ConnectorWritePlan::try_new(
            self.key.clone(),
            request.operation_id,
            request.cohort_id,
            request.execution_id,
            plan.handles,
            plan.control_payload,
        )?;
        cohort.attempts.insert(
            request.execution_id,
            CachedPlan {
                attempt_digest,
                plan: plan.clone(),
            },
        );
        Ok(plan)
    }

    fn commit(
        &self,
        request: ConnectorWriteCommitRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.ensure_owner(request.owner())?;
        self.validate_aggregate_plans(request.sealed(), request.cohorts(), true)?;
        if self
            .aborts
            .lock()
            .map_err(|error| internal(format!("Iceberg write abort lock: {error}")))?
            .contains_key(&request.operation_id())
        {
            return Err(invalid(
                "Iceberg write operation cannot commit after a known-uncommitted abort",
            ));
        }
        let mut operations = self
            .operations
            .lock()
            .map_err(|error| internal(format!("Iceberg write operation lock: {error}")))?;
        if let Some(record) = operations.get(&request.operation_id()) {
            if record.cohort_set_digest == request.sealed().digest()
                && record.aggregate_digest == request.aggregate_digest()
            {
                return Ok(record.outcome.clone());
            }
            return Err(invalid(
                "Iceberg write operation was committed with a different sealed set or aggregate",
            ));
        }
        let outcome = self.commit_outcome(&request)?;
        operations.insert(
            request.operation_id(),
            IcebergWriteOperationRecord {
                cohort_set_digest: request.sealed().digest(),
                aggregate_digest: request.aggregate_digest(),
                outcome: outcome.clone(),
            },
        );
        Ok(outcome)
    }

    fn abort(
        &self,
        request: ConnectorWriteAbortRequest,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        self.ensure_owner(&request.owner)?;
        self.validate_aggregate_plans(&request.sealed, &request.cohorts, false)?;
        if let Some(record) = self
            .operations
            .lock()
            .map_err(|error| internal(format!("Iceberg write operation lock: {error}")))?
            .get(&request.operation_id())
            .cloned()
        {
            if record.cohort_set_digest != request.sealed.digest()
                || record.aggregate_digest != request.aggregate_digest
            {
                return Err(invalid(
                    "Iceberg write abort does not match the recorded operation aggregate",
                ));
            }
            match record.outcome {
                ExternalMutationOutcome::KnownCommitted {
                    receipt,
                    finalization,
                    ..
                } => {
                    return Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                        receipt,
                        finalization,
                    });
                }
                ExternalMutationOutcome::KnownUncommitted { .. } => {}
                ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
                    return Ok(ConnectorWriteAbortOutcome::CommitUnknown { failure, evidence });
                }
            }
        }
        let mut aborts = self
            .aborts
            .lock()
            .map_err(|error| internal(format!("Iceberg write abort lock: {error}")))?;
        if let Some(record) = aborts.get(&request.operation_id()) {
            if record.cohort_set_digest == request.sealed.digest()
                && record.aggregate_digest == request.aggregate_digest
            {
                return Ok(record.outcome.clone());
            }
            return Err(invalid(
                "Iceberg write operation was aborted with a different sealed set or aggregate",
            ));
        }
        let cleanup = self.backend.abort(&request)?;
        let outcome = ConnectorWriteAbortOutcome::KnownUncommitted { cleanup };
        aborts.insert(
            request.operation_id(),
            IcebergWriteAbortRecord {
                cohort_set_digest: request.sealed.digest(),
                aggregate_digest: request.aggregate_digest,
                outcome: outcome.clone(),
            },
        );
        Ok(outcome)
    }

    fn reconcile(
        &self,
        request: ConnectorWriteReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.ensure_owner(&request.owner)?;
        let evidence = self.decode_evidence(&request.evidence)?;
        let operation_id = ConnectorWriteOperationId::from_bytes(decode_16(
            &evidence.operation_id_base64,
            "operation id",
        )?);
        if request.operation_id != operation_id {
            return Err(invalid(
                "Iceberg write reconciliation request operation does not match its evidence",
            ));
        }
        let cohort_set_digest = decode_32(&evidence.cohort_set_digest_base64, "cohort set digest")?;
        let aggregate_digest = decode_32(&evidence.aggregate_digest_base64, "aggregate digest")?;
        if request.cohort_set_digest != cohort_set_digest
            || request.aggregate_digest != aggregate_digest
        {
            return Err(invalid(
                "Iceberg write reconciliation request digests do not match its evidence",
            ));
        }
        let mut operations = self
            .operations
            .lock()
            .map_err(|error| internal(format!("Iceberg write operation lock: {error}")))?;
        let record = operations.get(&operation_id).cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg write reconciliation has no known operation",
            )
        })?;
        if record.cohort_set_digest != cohort_set_digest
            || record.aggregate_digest != aggregate_digest
        {
            return Err(invalid(
                "Iceberg write reconciliation does not match the recorded operation",
            ));
        }
        if !matches!(
            record.outcome,
            ExternalMutationOutcome::CommitUnknown { .. }
        ) {
            return Ok(record.outcome);
        }
        let reconciled = match self.backend.reconcile(&evidence) {
            Ok(Some(outcome)) => ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(operation_id, &outcome)?,
                finalization: ExternalMutationFinalization::Complete,
            },
            Ok(None) => ExternalMutationOutcome::KnownUncommitted {
                failure: failure(
                    ConnectorMutationFailureKind::Internal,
                    "Iceberg reconcile proved the write uncommitted",
                ),
            },
            Err(CommitServiceError::Unknown { message, .. }) => {
                ExternalMutationOutcome::CommitUnknown {
                    failure: failure(ConnectorMutationFailureKind::Unavailable, message),
                    // The original envelope binds the exact generation,
                    // operation, execution, and report digest.  Reusing it
                    // keeps another uncertain reconciliation fail-closed.
                    evidence: request.evidence.clone(),
                }
            }
            Err(CommitServiceError::KnownUncommitted { message, .. })
            | Err(CommitServiceError::InvalidInput { message }) => {
                ExternalMutationOutcome::KnownUncommitted {
                    failure: failure(ConnectorMutationFailureKind::Internal, message),
                }
            }
            Err(CommitServiceError::FinalizeFailedKnownCommitted {
                outcome,
                finalize_error,
                ..
            }) => {
                let receipt = outcome
                    .as_ref()
                    .map(|outcome| self.receipt(operation_id, outcome))
                    .transpose()?
                    .ok_or_else(|| {
                        internal(
                            "Iceberg reconciled known-committed write finalization has no committed snapshot",
                        )
                    })?;
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Failed(failure(
                        ConnectorMutationFailureKind::Internal,
                        finalize_error,
                    )),
                }
            }
        };
        operations.insert(
            operation_id,
            IcebergWriteOperationRecord {
                cohort_set_digest,
                aggregate_digest,
                outcome: reconciled.clone(),
            },
        );
        Ok(reconciled)
    }
}

impl IcebergWriteControlAdapter {
    fn frozen_plan(
        &self,
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        execution_id: ConnectorWriteExecutionId,
        control_payload: &Bytes,
    ) -> Result<ConnectorWritePlan, ConnectorError> {
        let plans = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg write plan lock: {error}")))?;
        let Some(operation) = plans.get(&operation_id) else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg write operation has no frozen write plans",
            ));
        };
        let plan = operation
            .cohorts
            .get(&cohort_id)
            .and_then(|cohort| cohort.attempts.get(&execution_id))
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "Iceberg write cohort attempt has no frozen write plan",
                )
            })?;
        if plan.plan.control_payload() != control_payload {
            return Err(invalid(
                "Iceberg write commit control payload does not match the frozen plan",
            ));
        }
        Ok(plan.plan.clone())
    }

    fn validate_aggregate_plans(
        &self,
        sealed: &novarocks_spi::connector::ConnectorSealedWriteCohortSet,
        cohorts: &[novarocks_spi::connector::ConnectorWriteCohortCompletion],
        require_all_plans: bool,
    ) -> Result<(), ConnectorError> {
        let plans = self
            .plans
            .lock()
            .map_err(|error| internal(format!("Iceberg write plan lock: {error}")))?;
        let Some(operation) = plans.get(&sealed.operation_id()) else {
            if !require_all_plans && cohorts.is_empty() {
                return Ok(());
            }
            return Err(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg write operation has no frozen write plans",
            ));
        };
        for descriptor in sealed.cohorts() {
            let Some(cohort) = operation.cohorts.get(&descriptor.cohort_id()) else {
                if require_all_plans {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "Iceberg sealed cohort has no frozen planning parent",
                    ));
                }
                continue;
            };
            if cohort.stable_digest != descriptor.planning_digest() {
                return Err(invalid(
                    "Iceberg sealed cohort planning digest does not match its frozen parent",
                ));
            }
        }
        drop(plans);
        for cohort in cohorts {
            for attempt in cohort.accepted().into_iter().chain(cohort.superseded()) {
                self.validate_attempt_plan(attempt)?;
            }
        }
        Ok(())
    }

    fn validate_attempt_plan(
        &self,
        attempt: &ConnectorWriteAttemptCompletion,
    ) -> Result<(), ConnectorError> {
        let plan = self.frozen_plan(
            attempt.operation_id(),
            attempt.cohort_id(),
            attempt.execution_id(),
            attempt.control_payload(),
        )?;
        reports_digest(
            attempt.owner(),
            attempt.operation_id(),
            attempt.cohort_id(),
            attempt.execution_id(),
            Some(plan.handles()),
            attempt.reports(),
        )?;
        Ok(())
    }
}

fn reports_digest(
    owner: &ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    expected_handles: Option<&[novarocks_spi::connector::ConnectorWriterHandle]>,
    reports: &[ConnectorStagedReport],
) -> Result<[u8; 32], ConnectorError> {
    if reports.is_empty() {
        return Err(invalid(
            "Iceberg write commit requires at least one staged report",
        ));
    }
    let mut canonical_reports = BTreeMap::new();
    for report in reports {
        report.validate()?;
        if report.state() != ConnectorWriterTerminalState::Staged {
            return Err(invalid(
                "Iceberg write commit received a non-staged writer report",
            ));
        }
        let writer = report.writer();
        if writer.operation_id() != operation_id
            || writer.cohort_id() != cohort_id
            || writer.execution_id() != execution_id
            || writer.binding_key() != owner
        {
            return Err(invalid(
                "Iceberg staged report does not match the exact write operation and generation",
            ));
        }
        if canonical_reports.insert(writer.clone(), report).is_some() {
            return Err(invalid(
                "Iceberg write commit contains a duplicate logical writer report",
            ));
        }
    }
    if let Some(handles) = expected_handles {
        let expected = handles
            .iter()
            .map(|handle| handle.writer().clone())
            .collect::<BTreeSet<_>>();
        let actual = canonical_reports.keys().cloned().collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(invalid(
                "Iceberg write commit reports do not exactly cover the frozen writer manifest",
            ));
        }
    }
    let mut hasher = Sha256::new();
    for (writer, report) in canonical_reports {
        hasher.update(writer.operation_id().to_bytes());
        hasher.update(writer.cohort_id().to_bytes());
        hasher.update(writer.execution_id().query_id());
        hasher.update(writer.execution_id().attempt_id().to_be_bytes());
        hasher.update(writer.fragment_instance_id());
        hasher.update(writer.fragment_id().to_be_bytes());
        hasher.update(writer.backend_num().to_be_bytes());
        hasher.update(writer.sink_ordinal().to_be_bytes());
        hasher.update(writer.binding_key().instance_id.as_str().as_bytes());
        hasher.update(writer.binding_key().incarnation.to_bytes());
        hasher.update(report.version().to_be_bytes());
        let summary = report.summary();
        hasher.update(summary.input_rows.to_be_bytes());
        hasher.update(summary.staged_bytes.to_be_bytes());
        hasher.update(summary.artifact_count.to_be_bytes());
        hasher.update(report.payload_digest());
    }
    Ok(hasher.finalize().into())
}

fn planning_attempt_digest(request: &ConnectorWritePlanningRequest) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg-write-attempt-plan.v1\0");
    hasher.update(request.operation_id.to_bytes());
    hasher.update(request.cohort_id.to_bytes());
    hasher.update(request.execution_id.query_id());
    hasher.update(request.execution_id.attempt_id().to_be_bytes());
    for writer in request.expected_writers.iter().collect::<BTreeSet<_>>() {
        hasher.update(writer.operation_id().to_bytes());
        hasher.update(writer.cohort_id().to_bytes());
        hasher.update(writer.execution_id().query_id());
        hasher.update(writer.execution_id().attempt_id().to_be_bytes());
        hasher.update(writer.fragment_instance_id());
        hasher.update(writer.fragment_id().to_be_bytes());
        hasher.update(writer.backend_num().to_be_bytes());
        hasher.update(writer.sink_ordinal().to_be_bytes());
        hasher.update(writer.binding_key().instance_id.as_str().as_bytes());
        hasher.update(writer.binding_key().incarnation.to_bytes());
    }
    hasher.finalize().into()
}

fn canonical_json<T: Serialize>(value: &T, subject: &str) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| internal(format!("encode {subject}: {error}")))
}

fn decode_canonical_json<T: for<'de> Deserialize<'de>>(
    payload: &[u8],
    subject: &str,
) -> Result<T, ConnectorError> {
    serde_json::from_slice(payload).map_err(|error| invalid(format!("decode {subject}: {error}")))
}

fn base64_encode(bytes: impl AsRef<[u8]>) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn decode_16(value: &str, subject: &str) -> Result<[u8; 16], ConnectorError> {
    decode_fixed(value, subject)
}

fn decode_32(value: &str, subject: &str) -> Result<[u8; 32], ConnectorError> {
    decode_fixed(value, subject)
}

fn decode_fixed<const N: usize>(value: &str, subject: &str) -> Result<[u8; N], ConnectorError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| invalid(format!("decode Iceberg write {subject}: {error}")))?;
    bytes
        .try_into()
        .map_err(|_| invalid(format!("Iceberg write {subject} has invalid length")))
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

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorRequestContext, ConnectorSealedWriteCohortSet, ConnectorTableHandle,
        ConnectorWriteAttemptCompletion, ConnectorWriteCohortCompletion,
        ConnectorWriteCohortDescriptor, ConnectorWriteExecutionId, ConnectorWriteIntent,
        ConnectorWriteOperationCompletion, ConnectorWriterIdentity,
    };

    use super::*;

    #[derive(Default)]
    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct Backend {
        key: ConnectorExecutionBindingKey,
        commits: AtomicUsize,
        aborts: AtomicUsize,
        unknown: AtomicBool,
    }

    impl IcebergWriteControlBackend for Backend {
        fn plan(
            &self,
            request: &ConnectorWritePlanningRequest,
        ) -> Result<IcebergWriteControlPlan, ConnectorError> {
            let handles = request
                .expected_writers
                .iter()
                .cloned()
                .map(|writer| {
                    novarocks_spi::connector::ConnectorWriterHandle::try_new(
                        self.key.clone(),
                        writer,
                        1,
                        Bytes::from_static(b"iceberg-v1"),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(IcebergWriteControlPlan {
                handles,
                control_payload: IcebergWritePlanPayloadV1 {
                    version: 1,
                    target: "db.t".to_string(),
                    target_ref: "main".to_string(),
                }
                .encode()?,
            })
        }

        fn commit(
            &self,
            _: &ConnectorWriteCommitRequest,
        ) -> Result<CommitOutcome, CommitServiceError> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            if self.unknown.load(Ordering::SeqCst) {
                return Err(CommitServiceError::unknown(
                    "commit response lost".to_string(),
                    RecoveryEvidence {
                        table_ident: "db.t".to_string(),
                        op_kind: super::super::commit::CommitOpKind::FastAppend,
                        base_snapshot_id: None,
                        base_sequence_number: 0,
                        staging_dir: "file:///warehouse/db/t/data".to_string(),
                    },
                ));
            }
            Ok(CommitOutcome {
                new_snapshot_id: 7,
                written_manifest_paths: Vec::new(),
            })
        }

        fn abort(
            &self,
            _: &ConnectorWriteAbortRequest,
        ) -> Result<ExternalMutationFinalization, ConnectorError> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(ExternalMutationFinalization::Complete)
        }
        fn reconcile(
            &self,
            _: &IcebergWriteReconcileEvidenceV1,
        ) -> Result<Option<CommitOutcome>, CommitServiceError> {
            Ok(Some(CommitOutcome {
                new_snapshot_id: 7,
                written_manifest_paths: Vec::new(),
            }))
        }
    }

    fn key(incarnation: u8) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg.control").expect("id"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([incarnation; 16]),
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("context")
    }

    fn writer(
        key: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        execution_id: ConnectorWriteExecutionId,
    ) -> novarocks_spi::connector::ConnectorWriterIdentity {
        ConnectorWriterIdentity::new(operation_id, cohort_id, execution_id, [3; 16], 1, 2, 0, key)
    }

    fn planning(
        key: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        execution_id: ConnectorWriteExecutionId,
        payload: &'static [u8],
    ) -> ConnectorWritePlanningRequest {
        ConnectorWritePlanningRequest {
            operation_id,
            cohort_id,
            execution_id,
            table: ConnectorTableHandle::try_new(
                key.instance_id.clone(),
                Bytes::from_static(b"table"),
            )
            .expect("table"),
            intent: ConnectorWriteIntent::Append,
            input_schema: Arc::new(Schema::empty()),
            expected_writers: vec![writer(key, operation_id, cohort_id, execution_id)],
            provider_payload: Bytes::from_static(payload),
            context: context(),
        }
    }

    fn staged_report(writer: ConnectorWriterIdentity) -> ConnectorStagedReport {
        ConnectorStagedReport::try_new(
            writer,
            1,
            ConnectorWriterTerminalState::Staged,
            Default::default(),
            Bytes::from_static(b"reports"),
        )
        .expect("report")
    }

    fn completion(
        key: ConnectorExecutionBindingKey,
        requests: &[ConnectorWritePlanningRequest],
        plans: &[ConnectorWritePlan],
    ) -> ConnectorWriteOperationCompletion {
        let descriptors = requests
            .iter()
            .map(|request| {
                ConnectorWriteCohortDescriptor::new(
                    request.cohort_id,
                    request.intent,
                    request.stable_digest(&key).expect("stable digest"),
                )
            })
            .collect();
        let sealed = ConnectorSealedWriteCohortSet::try_new(requests[0].operation_id, descriptors)
            .expect("sealed cohorts");
        let cohorts = requests
            .iter()
            .zip(plans)
            .map(|(request, plan)| {
                let attempt = ConnectorWriteAttemptCompletion::try_new(
                    key.clone(),
                    request.operation_id,
                    request.cohort_id,
                    request.execution_id,
                    [8; 32],
                    vec![staged_report(plan.handles()[0].writer().clone())],
                    plan.control_payload().clone(),
                )
                .expect("attempt completion");
                ConnectorWriteCohortCompletion::try_new(
                    request.cohort_id,
                    Some(attempt),
                    Vec::new(),
                )
                .expect("cohort completion")
            })
            .collect();
        ConnectorWriteOperationCompletion::try_new(key, sealed, cohorts)
            .expect("operation completion")
    }

    fn backend(key: &ConnectorExecutionBindingKey) -> Arc<Backend> {
        Arc::new(Backend {
            key: key.clone(),
            commits: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
            unknown: AtomicBool::new(false),
        })
    }

    #[test]
    fn plans_are_scoped_by_operation_cohort_and_attempt() {
        let key = key(1);
        let backend = backend(&key);
        let adapter =
            IcebergWriteControlAdapter::new(key.clone(), backend.clone()).expect("adapter");
        let operation_id = ConnectorWriteOperationId::new();
        let first_cohort = ConnectorWriteCohortId::primary(operation_id);
        let second_cohort =
            ConnectorWriteCohortId::derive(operation_id, b"rewrite", [9; 32]).expect("cohort");
        let first = planning(
            key.clone(),
            operation_id,
            first_cohort,
            ConnectorWriteExecutionId::new([4; 16], 1),
            b"plan-a",
        );
        let retry = planning(
            key.clone(),
            operation_id,
            first_cohort,
            ConnectorWriteExecutionId::new([5; 16], 2),
            b"plan-a",
        );
        let second = planning(
            key.clone(),
            operation_id,
            second_cohort,
            ConnectorWriteExecutionId::new([6; 16], 1),
            b"plan-b",
        );
        let first_plan = adapter.plan_write(first.clone()).expect("first plan");
        assert_eq!(
            adapter
                .plan_write(first.clone())
                .expect("idempotent replay"),
            first_plan
        );
        assert!(adapter.plan_write(retry).is_ok());
        assert!(adapter.plan_write(second).is_ok());
        let mut conflicting = first;
        conflicting.provider_payload = Bytes::from_static(b"changed");
        assert!(adapter.plan_write(conflicting).is_err());
    }

    #[test]
    fn aggregate_commit_and_abort_are_operation_idempotent() {
        let key = key(1);
        let backend = backend(&key);
        let adapter =
            IcebergWriteControlAdapter::new(key.clone(), backend.clone()).expect("adapter");
        let operation_id = ConnectorWriteOperationId::new();
        let requests = vec![
            planning(
                key.clone(),
                operation_id,
                ConnectorWriteCohortId::primary(operation_id),
                ConnectorWriteExecutionId::new([4; 16], 1),
                b"plan-a",
            ),
            planning(
                key.clone(),
                operation_id,
                ConnectorWriteCohortId::derive(operation_id, b"rewrite", [7; 32]).expect("cohort"),
                ConnectorWriteExecutionId::new([5; 16], 1),
                b"plan-b",
            ),
        ];
        let plans = requests
            .iter()
            .cloned()
            .map(|request| adapter.plan_write(request).expect("plan"))
            .collect::<Vec<_>>();
        let commit_completion = completion(key.clone(), &requests, &plans);
        let commit = ConnectorWriteCommitRequest {
            completion: commit_completion,
            context: context(),
        };
        let first = adapter.commit(commit.clone()).expect("commit");
        let retry = adapter.commit(commit).expect("retry");
        assert_eq!(first, retry);
        assert_eq!(backend.commits.load(Ordering::SeqCst), 1);

        let abort_operation = ConnectorWriteOperationId::new();
        let abort_request = planning(
            key.clone(),
            abort_operation,
            ConnectorWriteCohortId::primary(abort_operation),
            ConnectorWriteExecutionId::new([6; 16], 1),
            b"abort-plan",
        );
        let abort_plan = adapter
            .plan_write(abort_request.clone())
            .expect("abort plan");
        let abort_completion = completion(key.clone(), &[abort_request], &[abort_plan]);
        let abort = ConnectorWriteAbortRequest::try_new(
            key,
            abort_completion.sealed().clone(),
            abort_completion.cohorts().to_vec(),
            context(),
        )
        .expect("abort request");
        assert_eq!(
            adapter.abort(abort.clone()).expect("abort"),
            adapter.abort(abort).expect("retry")
        );
        assert_eq!(backend.aborts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unknown_evidence_binds_sealed_and_aggregate_digests() {
        let key = key(1);
        let backend = backend(&key);
        backend.unknown.store(true, Ordering::SeqCst);
        let adapter =
            IcebergWriteControlAdapter::new(key.clone(), backend.clone()).expect("adapter");
        let operation_id = ConnectorWriteOperationId::new();
        let request = planning(
            key.clone(),
            operation_id,
            ConnectorWriteCohortId::primary(operation_id),
            ConnectorWriteExecutionId::new([4; 16], 1),
            b"plan",
        );
        let plan = adapter.plan_write(request.clone()).expect("plan");
        let completion = completion(key.clone(), std::slice::from_ref(&request), &[plan]);
        let cohort_set_digest = completion.sealed().digest();
        let aggregate_digest = completion.aggregate_digest();
        let commit = ConnectorWriteCommitRequest {
            completion,
            context: context(),
        };
        let evidence = match adapter.commit(commit).expect("unknown") {
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => evidence,
            _ => panic!("commit unknown"),
        };
        backend.unknown.store(false, Ordering::SeqCst);
        let reconciled = adapter
            .reconcile(ConnectorWriteReconcileRequest {
                owner: key.clone(),
                operation_id,
                cohort_set_digest,
                aggregate_digest,
                evidence: evidence.clone(),
                context: context(),
            })
            .expect("reconcile");
        assert!(matches!(
            reconciled,
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        let error = adapter
            .reconcile(ConnectorWriteReconcileRequest {
                owner: key,
                operation_id,
                cohort_set_digest: [9; 32],
                aggregate_digest,
                evidence,
                context: context(),
            })
            .expect_err("digest mismatch");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn canonical_plan_payload_rejects_noncanonical_form() {
        let payload = IcebergWritePlanPayloadV1 {
            version: 1,
            target: "db.t".to_string(),
            target_ref: "main".to_string(),
        }
        .encode()
        .expect("encode");
        assert_eq!(
            IcebergWritePlanPayloadV1::decode(&payload)
                .expect("decode")
                .target,
            "db.t"
        );
        assert!(
            IcebergWritePlanPayloadV1::decode(
                br#"{\"target_ref\":\"main\",\"target\":\"db.t\",\"version\":1}"#
            )
            .is_err()
        );
    }

    #[test]
    fn first_refresh_payload_is_canonical_and_rejects_unsafe_facts() {
        let payload = IcebergFirstRefreshWritePlanPayloadV2 {
            version: 2,
            target: "db.mv_target".to_string(),
            target_ref: "refresh-staging".to_string(),
            expected_snapshot_id: Some(7),
            staging_path: "s3://warehouse/db/mv_target/data/_staging/attempt".to_string(),
            provenance_properties: BTreeMap::from([(
                "novarocks.mv.refresh.id".to_string(),
                "42".to_string(),
            )]),
        }
        .encode()
        .expect("encode first-refresh payload");
        assert_eq!(
            IcebergFirstRefreshWritePlanPayloadV2::decode(&payload)
                .expect("decode first-refresh payload")
                .expected_snapshot_id,
            Some(7)
        );
        let error = IcebergFirstRefreshWritePlanPayloadV2 {
            version: 2,
            target: "db.mv_target".to_string(),
            target_ref: "refresh-staging".to_string(),
            expected_snapshot_id: Some(-1),
            staging_path: "s3://warehouse/db/mv_target/data/_staging/attempt".to_string(),
            provenance_properties: BTreeMap::new(),
        }
        .encode()
        .expect_err("negative snapshot must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
