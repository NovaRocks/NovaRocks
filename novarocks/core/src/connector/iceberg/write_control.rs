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
    ConnectorRowMutationActivationRequest, ConnectorRowMutationExecutionPlan,
    ConnectorRowMutationPreparationOutcome, ConnectorRowMutationPreparationRequest,
    ConnectorStagedReport, ConnectorWriteAbortOutcome, ConnectorWriteAbortRequest,
    ConnectorWriteActivation, ConnectorWriteActivationRequest, ConnectorWriteAttemptCompletion,
    ConnectorWriteCohortId, ConnectorWriteCommitRequest, ConnectorWriteControl,
    ConnectorWriteExecutionId, ConnectorWriteOperationId, ConnectorWritePlan,
    ConnectorWritePlanningRequest, ConnectorWritePreparationOutcome,
    ConnectorWritePreparationRequest, ConnectorWriteReceipt, ConnectorWriteReconcileRequest,
    ConnectorWriterTerminalState, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use super::commit::{CommitServiceError, RecoveryEvidence};
use novarocks_connector_iceberg::commit::{CommitOpKind, CommitOutcome};
use novarocks_connector_iceberg::write_codec::connector_write_receipt;

const ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION: u16 = 2;
const ICEBERG_WRITE_OPERATION_KIND: &str = "iceberg.connector_write.v2";
use novarocks_connector_iceberg::write_payload::{
    IcebergFirstRefreshWritePlanPayloadV2, IcebergWritePlanPayloadV1,
};

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
    /// Reserve any provider-private operation service implied by activation.
    /// The default supports routes whose service was already registered by
    /// their provider-side binder. Managed publication overrides this so the
    /// application never constructs a provider commit driver.
    fn activate(&self, _request: &ConnectorWriteActivationRequest) -> Result<(), ConnectorError> {
        Ok(())
    }

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

    fn build_staged_create_action(
        &self,
        _completion: &novarocks_spi::connector::ConnectorWriteOperationCompletion,
        abort_handle: &Arc<novarocks_connector_iceberg::commit::AbortLog>,
    ) -> Result<
        super::commit::StagedFastAppendAction,
        super::write_service::StagedCreateActionBuildFailure,
    > {
        Err(super::write_service::StagedCreateActionBuildFailure {
            error: CommitServiceError::invalid_input(
                "Iceberg write backend does not support atomic staged-table publication"
                    .to_string(),
            ),
            abort_handle: Arc::clone(abort_handle),
        })
    }

    fn abort_staged_create_action(
        &self,
        _completion: &novarocks_spi::connector::ConnectorWriteOperationCompletion,
        _abort_handle: &Arc<novarocks_connector_iceberg::commit::AbortLog>,
    ) -> Result<ExternalMutationFinalization, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg write backend does not support staged-table cleanup",
        ))
    }
}

/// Provider-owned admission hook.  The control adapter deliberately cannot
/// decode an Iceberg table handle: only the provider which admitted that table
/// can turn it into base-version, input-token, and managed-MV facts.
pub(crate) type IcebergWritePreparationFactory = dyn Fn(
        ConnectorWritePreparationRequest,
        &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorWritePreparationOutcome, ConnectorError>
    + Send
    + Sync;

/// Row-mutation admission is deliberately a separate provider factory. It is
/// invoked by the same exact write control, but cannot silently route through
/// ordinary append/overwrite preparation.
pub(crate) type IcebergRowMutationPreparationFactory = dyn Fn(
        ConnectorRowMutationPreparationRequest,
        &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorRowMutationPreparationOutcome, ConnectorError>
    + Send
    + Sync;

/// Activation stays paired with row-mutation admission.  In particular, it
/// cannot fall through to `plan_write`: the only value it may consume is the
/// provider-signed preparation returned by the factory above, retained by
/// this exact control generation.
pub(crate) type IcebergRowMutationActivationFactory = dyn Fn(
        ConnectorRowMutationActivationRequest,
        &ConnectorExecutionBindingKey,
    ) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError>
    + Send
    + Sync;

#[derive(Clone)]
pub(crate) struct IcebergWriteControlAdapter {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    backend: Arc<dyn IcebergWriteControlBackend>,
    prepare: Arc<IcebergWritePreparationFactory>,
    prepare_row_mutation: Arc<IcebergRowMutationPreparationFactory>,
    activate_row_mutation: Arc<IcebergRowMutationActivationFactory>,
    activations:
        Arc<novarocks_connector_iceberg::write_activation::IcebergWriteActivationReservations>,
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
        Self::new_with_preparation(key, backend, Arc::new(default_prepare))
    }

    pub(crate) fn new_with_preparation(
        key: ConnectorExecutionBindingKey,
        backend: Arc<dyn IcebergWriteControlBackend>,
        prepare: Arc<IcebergWritePreparationFactory>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        Ok(Self {
            key,
            descriptor,
            backend,
            prepare,
            prepare_row_mutation: Arc::new(default_prepare_row_mutation),
            activate_row_mutation: Arc::new(default_activate_row_mutation),
            activations: Arc::new(
                novarocks_connector_iceberg::write_activation::IcebergWriteActivationReservations::default(),
            ),
            operations: Arc::new(Mutex::new(HashMap::new())),
            aborts: Arc::new(Mutex::new(HashMap::new())),
            plans: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub(crate) fn with_row_mutation_preparation(
        mut self,
        prepare_row_mutation: Arc<IcebergRowMutationPreparationFactory>,
    ) -> Self {
        self.prepare_row_mutation = prepare_row_mutation;
        self
    }

    pub(crate) fn with_row_mutation_activation(
        mut self,
        activate_row_mutation: Arc<IcebergRowMutationActivationFactory>,
    ) -> Self {
        self.activate_row_mutation = activate_row_mutation;
        self
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

    fn prepare_write(
        &self,
        request: ConnectorWritePreparationRequest,
    ) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
        request.validate(&self.key)?;
        (self.prepare)(request, &self.key)
    }

    fn prepare_row_mutation(
        &self,
        request: ConnectorRowMutationPreparationRequest,
    ) -> Result<ConnectorRowMutationPreparationOutcome, ConnectorError> {
        request.validate(&self.key)?;
        (self.prepare_row_mutation)(request, &self.key)
    }

    fn activate_row_mutation(
        &self,
        request: ConnectorRowMutationActivationRequest,
    ) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
        request.validate(&self.key)?;
        (self.activate_row_mutation)(request, &self.key)
    }

    fn activate_write(
        &self,
        request: ConnectorWriteActivationRequest,
    ) -> Result<ConnectorWriteActivation, ConnectorError> {
        request.validate(&self.key)?;
        self.backend.activate(&request)?;
        self.activations.activate(&self.key, &request)
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
                let outcome = record.outcome.clone();
                drop(operations);
                self.release_activation_if_known(request.operation_id(), &outcome)?;
                return Ok(outcome);
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
        drop(operations);
        self.release_activation_if_known(request.operation_id(), &outcome)?;
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
                    self.release_activation(request.operation_id())?;
                    return Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                        receipt,
                        finalization,
                    });
                }
                ExternalMutationOutcome::KnownUncommitted { .. } => {
                    self.release_activation(request.operation_id())?;
                }
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
        drop(aborts);
        self.release_activation(request.operation_id())?;
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
            let outcome = record.outcome;
            drop(operations);
            self.release_activation_if_known(operation_id, &outcome)?;
            return Ok(outcome);
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
        drop(operations);
        self.release_activation_if_known(operation_id, &reconciled)?;
        Ok(reconciled)
    }
}

impl IcebergWriteControlAdapter {
    fn release_activation(
        &self,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<(), ConnectorError> {
        self.activations.release(operation_id)
    }

    fn release_activation_if_known(
        &self,
        operation_id: ConnectorWriteOperationId,
        outcome: &ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Result<(), ConnectorError> {
        if !matches!(outcome, ExternalMutationOutcome::CommitUnknown { .. }) {
            self.release_activation(operation_id)?;
        }
        Ok(())
    }

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

/// Translate the remaining in-process Iceberg commit runner into the sealed
/// connector terminal contract before it reaches a frontend application
/// owner.  The legacy runner remains provider-private during C1; its snapshot
/// and recovery structures never cross this reverse port.
pub(crate) fn terminal_outcome_from_iceberg_commit(
    owner: &ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    result: Result<CommitOutcome, CommitServiceError>,
) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
    let receipt = |outcome: &CommitOutcome| {
        connector_write_receipt(outcome.new_snapshot_id, None).map_err(|error| error.to_string())
    };
    let evidence = |recovery: RecoveryEvidence| {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .map_err(|error| error.to_string())?,
            instance_id: owner.instance_id.clone(),
        };
        let payload = canonical_json(
            &LegacyIcebergTerminalEvidenceV1 {
                version: 1,
                table_ident: recovery.table_ident,
                op_kind: format!("{:?}", recovery.op_kind),
                base_snapshot_id: recovery.base_snapshot_id,
                base_sequence_number: recovery.base_sequence_number,
                staging_dir: recovery.staging_dir,
            },
            "legacy Iceberg terminal evidence",
        )
        .map_err(|error| error.to_string())?;
        ExternalMutationEvidence::try_new(
            ICEBERG_WRITE_CONTROL_EVIDENCE_VERSION,
            descriptor,
            owner.incarnation,
            novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                operation_id.to_bytes(),
            ),
            ICEBERG_WRITE_OPERATION_KIND,
            payload,
        )
        .map_err(|error| error.to_string())
    };
    match result {
        Ok(outcome) => Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: receipt(&outcome)?,
            finalization: ExternalMutationFinalization::Complete,
        }),
        Err(CommitServiceError::KnownUncommitted { message, .. })
        | Err(CommitServiceError::InvalidInput { message }) => {
            Ok(ExternalMutationOutcome::KnownUncommitted {
                failure: failure(ConnectorMutationFailureKind::Conflict, message),
            })
        }
        Err(CommitServiceError::Unknown {
            message,
            evidence: recovery,
        }) => Ok(ExternalMutationOutcome::CommitUnknown {
            failure: failure(ConnectorMutationFailureKind::Unavailable, message),
            evidence: evidence(recovery)?,
        }),
        Err(CommitServiceError::FinalizeFailedKnownCommitted {
            outcome,
            finalize_error,
            ..
        }) => {
            let outcome = outcome.ok_or_else(|| {
                "Iceberg known-committed finalization failure did not retain a receipt".to_string()
            })?;
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: receipt(&outcome)?,
                finalization: ExternalMutationFinalization::Failed(failure(
                    ConnectorMutationFailureKind::Internal,
                    finalize_error,
                )),
            })
        }
    }
}

#[derive(Serialize)]
struct LegacyIcebergTerminalEvidenceV1 {
    version: u16,
    table_ident: String,
    op_kind: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: i64,
    staging_dir: String,
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn default_prepare(
    _: ConnectorWritePreparationRequest,
    _: &ConnectorExecutionBindingKey,
) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "Iceberg write control was constructed without a provider admission factory",
    ))
}

fn default_prepare_row_mutation(
    _: ConnectorRowMutationPreparationRequest,
    _: &ConnectorExecutionBindingKey,
) -> Result<ConnectorRowMutationPreparationOutcome, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "Iceberg write control was constructed without a row-mutation admission factory",
    ))
}

fn default_activate_row_mutation(
    _: ConnectorRowMutationActivationRequest,
    _: &ConnectorExecutionBindingKey,
) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "Iceberg write control was constructed without a row-mutation activation factory",
    ))
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorRequestContext, ConnectorSealedWriteCohortSet, ConnectorTableHandle,
        ConnectorWriteActivation, ConnectorWriteActivationIntent, ConnectorWriteActivationRequest,
        ConnectorWriteActivationSource, ConnectorWriteAttemptCompletion, ConnectorWriteBaseVersion,
        ConnectorWriteCohortCompletion, ConnectorWriteCohortDescriptor, ConnectorWriteExecutionId,
        ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
        ConnectorWriteIntent, ConnectorWriteOperationCompletion, ConnectorWritePreparation,
        ConnectorWriterIdentity,
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
        activations: AtomicUsize,
        commits: AtomicUsize,
        aborts: AtomicUsize,
        unknown: AtomicBool,
    }

    impl IcebergWriteControlBackend for Backend {
        fn activate(&self, _: &ConnectorWriteActivationRequest) -> Result<(), ConnectorError> {
            self.activations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

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
                        op_kind: CommitOpKind::FastAppend,
                        base_snapshot_id: None,
                        base_sequence_number: 0,
                        staging_dir: "file:///warehouse/db/t/data".to_string(),
                        manifest_cleanup_token: None,
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
        let preparation = ConnectorWritePreparation::try_new(
            key.clone(),
            ConnectorTableHandle::try_new(key.instance_id.clone(), Bytes::from_static(b"table"))
                .expect("table"),
            novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::Append,
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).expect("base version"),
            ConnectorWriteInputShape::Data {
                fields: vec![ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("value", DataType::Int64, true),
                )],
            },
            Bytes::from_static(payload),
        )
        .expect("preparation");
        let context = context();
        let activation = ConnectorWriteActivation::try_new(
            key.clone(),
            &ConnectorWriteActivationRequest {
                operation_id,
                source: ConnectorWriteActivationSource::Prepared(preparation.clone()),
                intent: ConnectorWriteActivationIntent::Ordinary,
                context: context.clone(),
            },
            vec![(cohort_id, preparation)],
        )
        .expect("activation");
        ConnectorWritePlanningRequest {
            operation_id,
            cohort_id,
            execution_id,
            activation: activation.cohort(cohort_id).expect("cohort"),
            expected_writers: vec![writer(key, operation_id, cohort_id, execution_id)],
            context,
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
                    request.activation.preparation().intent(),
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
            activations: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
            aborts: AtomicUsize::new(0),
            unknown: AtomicBool::new(false),
        })
    }

    #[test]
    fn activation_reservation_is_idempotent_and_rejects_conflicts() {
        let key = key(1);
        let backend = backend(&key);
        let adapter =
            IcebergWriteControlAdapter::new(key.clone(), backend.clone()).expect("adapter");
        let operation_id = ConnectorWriteOperationId::new();
        let request = planning(
            key.clone(),
            operation_id,
            ConnectorWriteCohortId::primary(operation_id),
            ConnectorWriteExecutionId::new([9; 16], 1),
            b"activation-a",
        );
        let activation_request = ConnectorWriteActivationRequest {
            operation_id,
            source: ConnectorWriteActivationSource::Prepared(
                request.activation.preparation().clone(),
            ),
            intent: ConnectorWriteActivationIntent::Ordinary,
            context: request.context.clone(),
        };
        let first = adapter
            .activate_write(activation_request.clone())
            .expect("first activation");
        let replay = adapter
            .activate_write(activation_request)
            .expect("idempotent activation replay");
        assert_eq!(first.digest(), replay.digest());
        assert_eq!(backend.activations.load(Ordering::SeqCst), 2);

        let conflicting = planning(
            key,
            operation_id,
            ConnectorWriteCohortId::primary(operation_id),
            ConnectorWriteExecutionId::new([9; 16], 2),
            b"activation-b",
        );
        assert!(
            adapter
                .activate_write(ConnectorWriteActivationRequest {
                    operation_id,
                    source: ConnectorWriteActivationSource::Prepared(
                        conflicting.activation.preparation().clone(),
                    ),
                    intent: ConnectorWriteActivationIntent::Ordinary,
                    context: conflicting.context.clone(),
                })
                .is_err()
        );
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
        let conflicting = planning(
            key.clone(),
            operation_id,
            first_cohort,
            ConnectorWriteExecutionId::new([4; 16], 1),
            b"changed",
        );
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
