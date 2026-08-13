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

//! Bounded current-generation CTAS takeover recovery.
//!
//! One cycle holds the top-level StateStore authority, raises the catalog
//! fence before inspecting visibility, persists the typed observation, and
//! performs cleanup only from a proof-bound unpublished disposition. It never
//! reconstructs an ordinary foreground staged handle.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks::engine::ctas_engine::CtasEngine;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorCtasActionId, ConnectorCtasAdvanceFenceRequest,
    ConnectorCtasFailure, ConnectorCtasOperationId, ConnectorCtasProofPurpose,
    ConnectorCtasPublicationFence, ConnectorCtasPublicationFenceReceipt,
    ConnectorCtasPublicationProof, ConnectorCtasStagedLocator, ConnectorExecutionBindingKey,
    ConnectorHistoricalCtasAction, ConnectorHistoricalCtasCheckpoint,
    ConnectorHistoricalCtasCleanupRequest, ConnectorHistoricalCtasDescriptor,
    ConnectorHistoricalCtasDispatchState, ConnectorHistoricalCtasDisposition,
    ConnectorHistoricalCtasObservation, ConnectorHistoricalWriteCheckpoint,
    ConnectorHistoricalWriteCleanupRequest, ConnectorHistoricalWriteDescriptor,
    ConnectorHistoricalWriteDispatchState, ConnectorHistoricalWriteDisposition,
    ConnectorHistoricalWriteFence, ConnectorHistoricalWriteFenceFacts,
    ConnectorHistoricalWriteFenceRaiseRequest, ConnectorHistoricalWriteIdentity,
    ConnectorHistoricalWritePhase, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorWriteIntent,
    ConnectorWriteOperationId, ConnectorWriteTargetRef, CreatePolicy, ExternalMutationFinalization,
    ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    connector_action_id, ctas_record, decode_digest, durable_conflict_kind,
    durable_historical_disposition, historical_observation_fact, observation_checkpoint_identity,
};
use crate::dml::coordination::ActiveDmlOperation;
use crate::dml::error::{DmlError, DmlErrorKind};
use crate::dml::model::{
    ConnectorWriteFailureKind, ConnectorWriteFailureRecord, CtasSagaPhase,
    DML_CTAS_RECOVERY_ENCODED_LIMIT, DML_HISTORICAL_WRITE_RECOVERY_CODEC_VERSION,
    DmlCtasActionKind, DmlCtasCatalogFenceRecord, DmlCtasCleanupReceiptRecord,
    DmlCtasCleanupRetention, DmlCtasDispatchCertainty, DmlCtasDispatchCheckpointRecord,
    DmlCtasHistoricalObservationRecord, DmlCtasRecoveryRecord, DmlHistoricalCleanupState,
    DmlHistoricalDispatchCertainty, DmlHistoricalRecoveryPhase, DmlHistoricalWriteRecoveryRecord,
    DmlHistoricalWriteRequestRecord, DmlOpaquePayload, DurableExternalFact, ExternalFactOutcome,
    ExternalMutationEvidenceWire, OperationKind, OperationPayload, OperationState,
    StatementNextAction,
};
use crate::dml::reconcile::{external_fence_receipt_record_parts, historical_write_result_record};
use crate::dml::write_recovery::{
    HistoricalWriteRecoveryLedger, HistoricalWriteRecoveryResolver,
    reconstruct_historical_write_fence, validate_historical_response,
};

const CTAS_RECOVERY_ACTION_DEADLINE: Duration = Duration::from_secs(30);
const CTAS_RECOVERY_RETRY_DELAY_MS: i64 = 5_000;
const CTAS_RECOVERY_MANUAL_DELAY_MS: i64 = 30_000;
const MAX_CTAS_FENCE_HISTORY: usize = 64;
const MAX_CTAS_HISTORICAL_OBSERVATIONS: usize = 256;
const HISTORICAL_ABORT_DOMAIN: &[u8] = b"novarocks.frontend.ctas-historical-abort.v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CtasRecoveryProgress {
    Published,
    NoOp,
    Aborted,
    Conflict,
    CleanupCompleted,
    Unresolved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteCleanupDecision {
    Authorized,
    InspectHistorically,
    Denied,
}

pub(crate) struct CtasRecoveryProfile {
    engine: Arc<dyn CtasEngine>,
    write_recovery: Option<Arc<dyn HistoricalWriteRecoveryResolver>>,
}

impl CtasRecoveryProfile {
    pub(crate) fn new(
        engine: Arc<dyn CtasEngine>,
        write_recovery: Option<Arc<dyn HistoricalWriteRecoveryResolver>>,
    ) -> Self {
        Self {
            engine,
            write_recovery,
        }
    }

    pub(crate) fn drive(
        &self,
        active: &mut ActiveDmlOperation,
        now_ms: i64,
    ) -> Result<CtasRecoveryProgress, DmlError> {
        if active.stored.operation_kind != OperationKind::CreateTableAsSelect {
            return Err(operation_error(
                active,
                DmlErrorKind::JournalCorruption,
                "CTAS recovery profile received another operation family",
            ));
        }
        let mut recovery = active
            .journal
            .load_ctas_recovery(active.operation_id())?
            .ok_or_else(|| {
                operation_error(
                    active,
                    DmlErrorKind::JournalCorruption,
                    "CTAS recovery side record is missing",
                )
            })?;
        let saga = ctas_record(&active.stored)?;
        let context = request_context()?;
        let fence = self.raise_current_fence(active, &mut recovery, &context, now_ms)?;
        ensure_abort_checkpoint(&mut recovery, &saga, &fence)?;
        persist(active, &mut recovery, Some(now_ms))?;

        let descriptor = match historical_descriptor(&recovery, &saga, fence) {
            Ok(descriptor) => descriptor,
            Err(error) => {
                park(active, &mut recovery, now_ms, true)?;
                return Err(error);
            }
        };
        active.check_before_dispatch()?;
        let observation = match self
            .engine
            .inspect_historical_ctas(descriptor.clone(), context.clone())
        {
            Ok(observation) => observation,
            Err(failure) => {
                persist_typed_catalog_failure(
                    active,
                    &mut recovery,
                    &failure,
                    now_ms,
                    "inspection",
                )?;
                return Ok(CtasRecoveryProgress::Unresolved);
            }
        };
        persist_observation(active, &mut recovery, &descriptor, &observation, now_ms)?;

        match observation.disposition {
            ConnectorHistoricalCtasDisposition::Published => {
                recovery.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
                recovery.next_action = StatementNextAction::None;
                persist(active, &mut recovery, None)?;
                finish_success(active, &observation, false)?;
                Ok(CtasRecoveryProgress::Published)
            }
            ConnectorHistoricalCtasDisposition::NoOp if observation.locator.is_none() => {
                recovery.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
                recovery.next_action = StatementNextAction::None;
                persist(active, &mut recovery, None)?;
                finish_success(active, &observation, true)?;
                Ok(CtasRecoveryProgress::NoOp)
            }
            ConnectorHistoricalCtasDisposition::NotCreated
            | ConnectorHistoricalCtasDisposition::Aborted => {
                recovery.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
                recovery.next_action = StatementNextAction::None;
                persist(active, &mut recovery, None)?;
                finish_aborted(active, &observation)?;
                Ok(CtasRecoveryProgress::Aborted)
            }
            ConnectorHistoricalCtasDisposition::Staged
            | ConnectorHistoricalCtasDisposition::NoOp => {
                if !self.write_cleanup_authorized(active, &saga, &recovery, now_ms)? {
                    park(active, &mut recovery, now_ms, false)?;
                    return Ok(CtasRecoveryProgress::Unresolved);
                }
                self.cleanup_staging(
                    active,
                    &mut recovery,
                    &descriptor,
                    observation,
                    context,
                    now_ms,
                )
            }
            ConnectorHistoricalCtasDisposition::Conflict => {
                recovery.cleanup_retention = if recovery.staged_locator.is_some() {
                    DmlCtasCleanupRetention::ManualRetention
                } else {
                    DmlCtasCleanupRetention::NotRequired
                };
                recovery.next_action = recovery
                    .requires_recovery_scan()
                    .then_some(StatementNextAction::ManualInspect)
                    .unwrap_or(StatementNextAction::None);
                let due = recovery
                    .requires_recovery_scan()
                    .then_some(now_ms.saturating_add(CTAS_RECOVERY_MANUAL_DELAY_MS));
                persist(active, &mut recovery, due)?;
                finish_conflict(active, &observation, due)?;
                Ok(CtasRecoveryProgress::Conflict)
            }
            ConnectorHistoricalCtasDisposition::Ambiguous
            | ConnectorHistoricalCtasDisposition::Unsupported => {
                park(active, &mut recovery, now_ms, true)?;
                finish_unresolved(active, &observation, now_ms)?;
                Ok(CtasRecoveryProgress::Unresolved)
            }
        }
    }

    fn raise_current_fence(
        &self,
        active: &mut ActiveDmlOperation,
        recovery: &mut DmlCtasRecoveryRecord,
        context: &ConnectorRequestContext,
        now_ms: i64,
    ) -> Result<ConnectorCtasPublicationFence, DmlError> {
        let proposal = active.external_fence()?;
        let table = table_identity(active)?;
        let generic = proposal
            .seal(
                ConnectorWriteOperationId::from_bytes(*active.operation_id().as_uuid().as_bytes()),
                table.clone(),
                ConnectorWriteTargetRef::main(),
            )
            .map_err(DmlError::executor)?;
        let fence = ConnectorCtasPublicationFence::try_new(
            generic.cluster(),
            generic.generation(),
            ConnectorCtasOperationId::try_from_bytes(*active.operation_id().as_uuid().as_bytes())
                .map_err(DmlError::executor)?,
            table,
        )
        .map_err(DmlError::executor)?;
        let action_id =
            ConnectorCtasActionId::try_from_bytes(*proposal.coordination_attempt_id().as_bytes())
                .map_err(DmlError::executor)?;
        let request =
            ConnectorCtasAdvanceFenceRequest::try_new(fence.clone(), action_id, context.clone())
                .map_err(DmlError::executor)?;

        let same_attempt = recovery.catalog_fence.as_ref().is_some_and(|current| {
            current.generation == proposal.generation()
                && current.action_id == proposal.coordination_attempt_id()
                && current.request_digest == hex::encode(request.input_digest)
        });
        if !same_attempt {
            let projected_size = projected_takeover_size(
                recovery,
                proposal.generation(),
                proposal.coordination_attempt_id(),
                &request,
            )?;
            if recovery.catalog_fence_history.len() >= MAX_CTAS_FENCE_HISTORY
                || projected_size > DML_CTAS_RECOVERY_ENCODED_LIMIT
            {
                park_exhausted_history(active, recovery, &proposal, now_ms)?;
                return Err(operation_error(
                    active,
                    DmlErrorKind::Commit,
                    "CTAS catalog fence history exhausted its bounded durable retention",
                ));
            }
            if let Some(previous) = recovery.catalog_fence.take() {
                if previous.generation >= proposal.generation() {
                    return Err(operation_error(
                        active,
                        DmlErrorKind::CoordinationLost,
                        "current CTAS recovery authority did not mint a higher catalog fence",
                    ));
                }
                recovery.catalog_fence_history.push(previous);
            }
            if recovery.recovery_attempt_id != proposal.coordination_attempt_id() {
                recovery.recovery_cycle = recovery.recovery_cycle.saturating_add(1);
                recovery.recovery_attempt_id = proposal.coordination_attempt_id();
            }
            recovery.catalog_fence = Some(DmlCtasCatalogFenceRecord {
                generation: proposal.generation(),
                action_id: proposal.coordination_attempt_id(),
                request_digest: hex::encode(request.input_digest),
                dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
                dispatched_at_ms: None,
                fence_digest: None,
                receipt_digest: None,
                receipt_payload: None,
                established_at_ms: None,
            });
            recovery.next_action = StatementNextAction::ManualInspect;
            persist(active, recovery, Some(now_ms))?;
        }

        let complete = recovery
            .catalog_fence
            .as_ref()
            .is_some_and(|current| current.receipt_payload.is_some());
        if complete {
            let stored = recovery.catalog_fence.as_ref().expect("current CTAS fence");
            let expected_digest = hex::encode(fence.digest());
            let receipt = ConnectorCtasPublicationFenceReceipt::try_new(
                &request,
                Bytes::copy_from_slice(
                    stored
                        .receipt_payload
                        .as_ref()
                        .expect("complete receipt payload")
                        .as_bytes(),
                ),
            )
            .map_err(DmlError::journal_corruption)?;
            if stored.fence_digest.as_deref() != Some(expected_digest.as_str())
                || stored.receipt_digest.as_deref() != Some(hex::encode(receipt.digest()).as_str())
            {
                return Err(operation_error(
                    active,
                    DmlErrorKind::JournalCorruption,
                    "durable CTAS catalog fence receipt does not seal its exact request",
                ));
            }
            return Ok(fence);
        }
        {
            let current = recovery.catalog_fence.as_mut().expect("current CTAS fence");
            current.dispatch_certainty = DmlCtasDispatchCertainty::PossiblyDispatched;
            current.dispatched_at_ms.get_or_insert(now_ms);
        }
        persist(active, recovery, Some(now_ms))?;
        active.check_before_dispatch()?;
        // One same-generation retry closes the common response-loss window.
        // A second uncertain response remains durable and the next holder must
        // supersede it with a strictly higher fence.
        let receipt = match self.engine.advance_historical_ctas_fence(request.clone()) {
            Ok(receipt) => Ok(receipt),
            Err(
                ConnectorCtasFailure::PossiblyDispatched(_)
                | ConnectorCtasFailure::CommittedResponseInvalid(_)
                | ConnectorCtasFailure::Ambiguous(_),
            ) => self.engine.advance_historical_ctas_fence(request.clone()),
            Err(failure) => Err(failure),
        };
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(failure) => {
                persist_typed_catalog_failure(active, recovery, &failure, now_ms, "fence advance")?;
                return Err(operation_error(
                    active,
                    DmlErrorKind::Commit,
                    "CTAS catalog fence advance remains unresolved",
                ));
            }
        };
        let current = recovery.catalog_fence.as_mut().expect("current CTAS fence");
        current.fence_digest = Some(hex::encode(receipt.fence_digest()));
        current.receipt_digest = Some(hex::encode(receipt.digest()));
        current.receipt_payload = Some(
            DmlOpaquePayload::try_new(receipt.payload().to_vec())
                .map_err(DmlError::journal_corruption)?,
        );
        current.established_at_ms = Some(now_ms);
        persist(active, recovery, Some(now_ms))?;
        Ok(fence)
    }

    /// Prove that the historical distributed writer cannot still mutate the
    /// retained staged target. Catalog visibility alone is insufficient: a
    /// possibly-dispatched writer must first be fenced and classified by the
    fn write_cleanup_authorized(
        &self,
        active: &mut ActiveDmlOperation,
        saga: &crate::dml::model::CtasSagaRecord,
        recovery: &DmlCtasRecoveryRecord,
        now_ms: i64,
    ) -> Result<bool, DmlError> {
        let write_checkpoint = recovery
            .dispatch_checkpoints
            .iter()
            .rev()
            .find(|checkpoint| checkpoint.action == DmlCtasActionKind::Write);
        if durable_write_cleanup_decision(saga, write_checkpoint, None)
            == WriteCleanupDecision::Authorized
        {
            return Ok(true);
        }
        let Some(resolver) = self.write_recovery.as_ref() else {
            return Ok(false);
        };
        let Some(cohort_digest) = saga.write_cohort_set_digest.as_deref() else {
            return Ok(false);
        };
        let cohort_digest = decode_digest(cohort_digest, "CTAS writer cohort set")?;
        let write_operation_id =
            ConnectorWriteOperationId::from_bytes(*saga.write_operation_id.as_bytes());
        let table = table_identity(active)?;
        let target_ref = ConnectorWriteTargetRef::main();
        let historical_binding = historical_binding(saga)?;
        let evidence = saga
            .write_fact
            .as_ref()
            .and_then(|fact| fact.evidence.as_deref())
            .map(decode_ctas_write_evidence)
            .transpose()?;
        let aggregate_digest = saga
            .aggregate_write_digest
            .as_deref()
            .map(|digest| decode_digest(digest, "CTAS aggregate write"))
            .transpose()?;
        let old_fence_record = active.journal.load_external_fence(active.operation_id())?;
        let historical_fence = old_fence_record
            .as_ref()
            .map(|record| {
                reconstruct_historical_write_fence(
                    record,
                    write_operation_id,
                    table.clone(),
                    target_ref.clone(),
                )
            })
            .transpose()
            .map_err(DmlError::journal_corruption)?
            .unwrap_or(ConnectorHistoricalWriteFence::NotEstablished);
        let proposal = active.external_fence()?;
        let raised = proposal
            .seal(write_operation_id, table.clone(), target_ref.clone())
            .map_err(DmlError::executor)?;
        let request = historical_write_request(
            saga,
            recovery,
            cohort_digest,
            aggregate_digest,
            evidence.as_ref(),
            old_fence_record.as_ref(),
        )?;
        let existing = active
            .journal
            .load_historical_write_recovery(active.operation_id())?;
        let mut cycle = match existing {
            Some(existing) => {
                if existing.request != request {
                    return Err(operation_error(
                        active,
                        DmlErrorKind::JournalCorruption,
                        "durable CTAS historical writer request changed",
                    ));
                }
                existing
            }
            None => {
                let requested = DmlHistoricalWriteRecoveryRecord {
                    codec_version: DML_HISTORICAL_WRITE_RECOVERY_CODEC_VERSION,
                    phase: DmlHistoricalRecoveryPhase::Requested,
                    recovery_attempt_id: proposal.coordination_attempt_id(),
                    recovery_cycle: 1,
                    request: request.clone(),
                    raised_fence: None,
                    result: None,
                    next_action: StatementNextAction::Reconcile,
                    requested_at_ms: now_ms,
                    updated_at_ms: now_ms,
                };
                HistoricalWriteRecoveryLedger::persist_recovery(
                    active,
                    requested.clone(),
                    Some(now_ms),
                )?;
                requested
            }
        };
        match durable_write_cleanup_decision(saga, write_checkpoint, cycle.result.as_ref()) {
            WriteCleanupDecision::Authorized => return Ok(true),
            WriteCleanupDecision::Denied => return Ok(false),
            WriteCleanupDecision::InspectHistorically => {}
        }
        let handle = resolver
            .resolve(&table.instance_id)
            .map_err(DmlError::executor)?;
        active.check_before_dispatch()?;
        let receipt = handle
            .facet()
            .raise_external_fence(ConnectorHistoricalWriteFenceRaiseRequest {
                historical_binding: historical_binding.clone(),
                observed: historical_fence.clone(),
                raised: raised.clone(),
                context: request_context()?,
            })
            .map_err(DmlError::executor)?;
        if !receipt.matches(&raised) {
            return Ok(false);
        }
        let raised_record = external_fence_receipt_record_parts(&raised, &receipt)
            .map_err(DmlError::journal_corruption)?;
        cycle.phase = match cycle.result.as_ref().map(|result| result.cleanup) {
            Some(DmlHistoricalCleanupState::Pending) => DmlHistoricalRecoveryPhase::CleanupPending,
            Some(_) => DmlHistoricalRecoveryPhase::Inspected,
            None => DmlHistoricalRecoveryPhase::FenceRaised,
        };
        if cycle.recovery_attempt_id != proposal.coordination_attempt_id() {
            cycle.recovery_cycle = cycle.recovery_cycle.saturating_add(1);
            cycle.recovery_attempt_id = proposal.coordination_attempt_id();
        }
        cycle.raised_fence = Some(raised_record);
        cycle.updated_at_ms = now_ms;
        HistoricalWriteRecoveryLedger::persist_recovery(active, cycle.clone(), Some(now_ms))?;

        let descriptor = ConnectorHistoricalWriteDescriptor::try_new(
            ConnectorHistoricalWriteIdentity {
                historical_binding,
                table,
                target_ref,
                operation_id: write_operation_id,
                intent: ConnectorWriteIntent::Append,
                cohort_set_digest: cohort_digest,
                aggregate_digest,
            },
            ConnectorHistoricalWriteFenceFacts {
                historical_fence,
                raised_fence: raised,
                raised_fence_receipt_digest: receipt.digest(),
            },
            historical_write_checkpoints(saga, recovery),
            evidence,
        )
        .map_err(DmlError::executor)?;
        active.check_before_dispatch()?;
        let observation = handle
            .facet()
            .inspect(descriptor.clone(), request_context()?)
            .map_err(DmlError::executor)?;
        validate_historical_response(&observation, &descriptor, descriptor.raised_fence.digest())
            .map_err(DmlError::executor)?;
        let mut result =
            historical_write_result_record(&observation).map_err(DmlError::journal_corruption)?;
        cycle.result = Some(result.clone());
        cycle.phase = if result.cleanup == DmlHistoricalCleanupState::Pending {
            DmlHistoricalRecoveryPhase::CleanupPending
        } else if matches!(
            result.disposition,
            crate::dml::model::DmlHistoricalWriteDisposition::Ambiguous
                | crate::dml::model::DmlHistoricalWriteDisposition::Unsupported
        ) {
            DmlHistoricalRecoveryPhase::Unresolved
        } else {
            DmlHistoricalRecoveryPhase::Resolved
        };
        cycle.next_action = if cycle.phase == DmlHistoricalRecoveryPhase::Resolved {
            StatementNextAction::None
        } else {
            StatementNextAction::Reconcile
        };
        cycle.updated_at_ms = now_ms;
        HistoricalWriteRecoveryLedger::persist_recovery(active, cycle.clone(), Some(now_ms))?;

        match observation.disposition {
            ConnectorHistoricalWriteDisposition::NotApplied
            | ConnectorHistoricalWriteDisposition::NotDispatched => Ok(true),
            ConnectorHistoricalWriteDisposition::Staged => {
                let expected_descriptor_digest = descriptor.digest();
                let expected_observation_digest = observation.digest();
                active.check_before_dispatch()?;
                let cleanup = handle
                    .facet()
                    .cleanup(ConnectorHistoricalWriteCleanupRequest {
                        operation_id: write_operation_id,
                        descriptor_digest: expected_descriptor_digest,
                        observation,
                        context: request_context()?,
                    })
                    .map_err(DmlError::executor)?;
                let cleanup = match cleanup {
                    ExternalMutationOutcome::CommitUnknown { ref evidence, .. } => handle
                        .facet()
                        .reconcile_cleanup(write_operation_id, evidence.clone(), request_context()?)
                        .unwrap_or(cleanup),
                    _ => cleanup,
                };
                let completed = match historical_write_cleanup_completed(
                    &cleanup,
                    expected_descriptor_digest,
                    expected_observation_digest,
                ) {
                    Ok(completed) => completed,
                    Err(message) => {
                        result.failure = Some(ConnectorWriteFailureRecord {
                            kind: ConnectorWriteFailureKind::CorruptData,
                            message,
                        });
                        cycle.result = Some(result);
                        cycle.phase = DmlHistoricalRecoveryPhase::CleanupPending;
                        cycle.next_action = StatementNextAction::ManualInspect;
                        cycle.updated_at_ms = now_ms;
                        HistoricalWriteRecoveryLedger::persist_recovery(
                            active,
                            cycle,
                            Some(now_ms),
                        )?;
                        return Ok(false);
                    }
                };
                if completed {
                    result.cleanup = DmlHistoricalCleanupState::Completed;
                    cycle.result = Some(result);
                    cycle.phase = DmlHistoricalRecoveryPhase::Resolved;
                    cycle.next_action = StatementNextAction::None;
                    cycle.updated_at_ms = now_ms;
                    HistoricalWriteRecoveryLedger::persist_recovery(active, cycle, Some(now_ms))?;
                }
                Ok(completed)
            }
            ConnectorHistoricalWriteDisposition::Applied
            | ConnectorHistoricalWriteDisposition::Conflict
            | ConnectorHistoricalWriteDisposition::Ambiguous
            | ConnectorHistoricalWriteDisposition::Unsupported => Ok(false),
        }
    }

    fn cleanup_staging(
        &self,
        active: &mut ActiveDmlOperation,
        recovery: &mut DmlCtasRecoveryRecord,
        descriptor: &ConnectorHistoricalCtasDescriptor,
        observation: ConnectorHistoricalCtasObservation,
        context: ConnectorRequestContext,
        now_ms: i64,
    ) -> Result<CtasRecoveryProgress, DmlError> {
        let abort_id = ctas_record(&active.stored)?.abort_staging_operation_id;
        let checkpoint = recovery
            .dispatch_checkpoints
            .iter_mut()
            .find(|checkpoint| {
                checkpoint.action == DmlCtasActionKind::Abort
                    && checkpoint.child_operation_id == abort_id
            })
            .ok_or_else(|| {
                operation_error(
                    active,
                    DmlErrorKind::JournalCorruption,
                    "CTAS cleanup has no durable abort checkpoint",
                )
            })?;
        if checkpoint.dispatch_certainty == DmlCtasDispatchCertainty::ConfirmedNotDispatched {
            checkpoint.dispatch_certainty = DmlCtasDispatchCertainty::PossiblyDispatched;
            checkpoint.dispatched_at_ms = Some(now_ms);
            persist(active, recovery, Some(now_ms))?;
        }
        let request = ConnectorHistoricalCtasCleanupRequest {
            descriptor: descriptor.clone(),
            observation: observation.clone(),
            context,
        };
        active.check_before_dispatch()?;
        let receipt = match self.engine.cleanup_historical_ctas(request) {
            Ok(receipt) => receipt,
            Err(failure) => {
                persist_typed_catalog_failure(
                    active,
                    recovery,
                    &failure,
                    now_ms,
                    "guarded cleanup",
                )?;
                return Ok(CtasRecoveryProgress::Unresolved);
            }
        };
        let proof_wire = receipt.proof.try_to_wire_v1().map_err(DmlError::executor)?;
        recovery.cleanup_receipt = Some(DmlCtasCleanupReceiptRecord {
            descriptor_digest: hex::encode(receipt.descriptor_digest),
            observation_digest: hex::encode(receipt.observation_digest),
            locator_digest: hex::encode(receipt.locator_digest),
            receipt_digest: hex::encode(receipt.digest()),
            proof_digest: hex::encode(receipt.proof.digest()),
            proof_payload: DmlOpaquePayload::try_new(proof_wire.to_vec())
                .map_err(DmlError::journal_corruption)?,
            completed_at_ms: now_ms,
        });
        recovery.cleanup_retention = DmlCtasCleanupRetention::Completed;
        recovery.next_action = StatementNextAction::None;
        persist(active, recovery, None)?;
        if observation.disposition == ConnectorHistoricalCtasDisposition::NoOp {
            finish_success(active, &observation, true)?;
        } else {
            finish_cleanup_aborted(active, &receipt)?;
        }
        Ok(CtasRecoveryProgress::CleanupCompleted)
    }
}

fn historical_write_cleanup_completed(
    outcome: &ExternalMutationOutcome<
        novarocks_spi::connector::ConnectorHistoricalWriteCleanupReceipt,
    >,
    expected_descriptor_digest: [u8; 32],
    expected_observation_digest: [u8; 32],
) -> Result<bool, String> {
    let ExternalMutationOutcome::KnownCommitted {
        receipt,
        finalization: ExternalMutationFinalization::Complete,
        ..
    } = outcome
    else {
        return Ok(false);
    };
    if receipt.descriptor_digest != expected_descriptor_digest
        || receipt.observation_digest != expected_observation_digest
    {
        return Err(
            "historical write cleanup receipt does not match the exact descriptor and observation"
                .to_string(),
        );
    }
    Ok(true)
}

fn durable_write_cleanup_decision(
    saga: &crate::dml::model::CtasSagaRecord,
    checkpoint: Option<&DmlCtasDispatchCheckpointRecord>,
    historical: Option<&crate::dml::model::DmlHistoricalWriteResultRecord>,
) -> WriteCleanupDecision {
    if saga
        .write_fact
        .as_ref()
        .is_some_and(|fact| fact.outcome == ExternalFactOutcome::KnownUncommitted)
        || checkpoint.is_none_or(|checkpoint| {
            checkpoint.dispatch_certainty == DmlCtasDispatchCertainty::ConfirmedNotDispatched
        })
    {
        return WriteCleanupDecision::Authorized;
    }
    let Some(historical) = historical else {
        return WriteCleanupDecision::InspectHistorically;
    };
    match historical.disposition {
        crate::dml::model::DmlHistoricalWriteDisposition::NotApplied
        | crate::dml::model::DmlHistoricalWriteDisposition::NotDispatched => {
            WriteCleanupDecision::Authorized
        }
        crate::dml::model::DmlHistoricalWriteDisposition::Staged
            if historical.cleanup == DmlHistoricalCleanupState::Completed =>
        {
            WriteCleanupDecision::Authorized
        }
        crate::dml::model::DmlHistoricalWriteDisposition::Staged => {
            WriteCleanupDecision::InspectHistorically
        }
        crate::dml::model::DmlHistoricalWriteDisposition::Applied
        | crate::dml::model::DmlHistoricalWriteDisposition::Conflict
        | crate::dml::model::DmlHistoricalWriteDisposition::Ambiguous
        | crate::dml::model::DmlHistoricalWriteDisposition::Unsupported => {
            WriteCleanupDecision::Denied
        }
    }
}

fn persist_typed_catalog_failure(
    active: &mut ActiveDmlOperation,
    recovery: &mut DmlCtasRecoveryRecord,
    failure: &ConnectorCtasFailure,
    now_ms: i64,
    stage: &str,
) -> Result<(), DmlError> {
    let manual = matches!(
        failure,
        ConnectorCtasFailure::CommittedResponseInvalid(_)
            | ConnectorCtasFailure::Ambiguous(_)
            | ConnectorCtasFailure::Conflict { .. }
    );
    park(active, recovery, now_ms, manual)?;
    let mut saga = ctas_record(&active.stored)?;
    if matches!(failure, ConnectorCtasFailure::Conflict { .. }) {
        saga.phase = CtasSagaPhase::Conflict;
    }
    saga.abort_staging_fact = Some(super::connector_failure_fact(failure));
    saga.next_action = StatementNextAction::ManualInspect;
    active.mutate_statement(
        active.stored.state,
        OperationPayload::CtasSaga(saga),
        Some(now_ms.saturating_add(if manual {
            CTAS_RECOVERY_MANUAL_DELAY_MS
        } else {
            CTAS_RECOVERY_RETRY_DELAY_MS
        })),
    )?;
    tracing::debug!(operation_id = %active.operation_id(), stage, failure = ?failure, "CTAS historical catalog action did not converge");
    Ok(())
}

fn projected_takeover_size(
    recovery: &DmlCtasRecoveryRecord,
    generation: crate::dml::model::DmlExternalFenceGeneration,
    attempt_id: Uuid,
    request: &ConnectorCtasAdvanceFenceRequest,
) -> Result<usize, DmlError> {
    let mut projected = recovery.clone();
    if let Some(previous) = projected.catalog_fence.take() {
        projected.catalog_fence_history.push(previous);
    }
    projected.recovery_attempt_id = attempt_id;
    projected.recovery_cycle = projected.recovery_cycle.saturating_add(1);
    projected.catalog_fence = Some(DmlCtasCatalogFenceRecord {
        generation,
        action_id: attempt_id,
        request_digest: hex::encode(request.input_digest),
        dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
        dispatched_at_ms: None,
        fence_digest: None,
        receipt_digest: None,
        receipt_payload: None,
        established_at_ms: None,
    });
    serde_json::to_vec(&projected)
        .map(|encoded| encoded.len())
        .map_err(DmlError::journal_corruption)
}

fn park_exhausted_history(
    active: &mut ActiveDmlOperation,
    recovery: &mut DmlCtasRecoveryRecord,
    proposal: &crate::dml::coordination::DmlExternalFenceProposal,
    now_ms: i64,
) -> Result<(), DmlError> {
    if recovery.recovery_attempt_id != proposal.coordination_attempt_id() {
        recovery.recovery_cycle = recovery.recovery_cycle.saturating_add(1);
        recovery.recovery_attempt_id = proposal.coordination_attempt_id();
    }
    recovery.cleanup_retention = DmlCtasCleanupRetention::ManualRetention;
    recovery.next_action = StatementNextAction::ManualInspect;
    persist(
        active,
        recovery,
        Some(now_ms.saturating_add(CTAS_RECOVERY_MANUAL_DELAY_MS)),
    )
}

fn historical_descriptor(
    recovery: &DmlCtasRecoveryRecord,
    saga: &crate::dml::model::CtasSagaRecord,
    fence: ConnectorCtasPublicationFence,
) -> Result<ConnectorHistoricalCtasDescriptor, DmlError> {
    let current = recovery.catalog_fence.as_ref().ok_or_else(|| {
        DmlError::journal_corruption("CTAS recovery has no current catalog fence")
    })?;
    let fence_receipt_digest = decode_digest(
        current.receipt_digest.as_deref().ok_or_else(|| {
            DmlError::journal_corruption("CTAS recovery current fence has no receipt")
        })?,
        "CTAS recovery current fence receipt",
    )?;
    let locator = recovery
        .staged_locator
        .as_ref()
        .map(|wire| ConnectorCtasStagedLocator::try_from_wire_v1(wire.as_bytes()))
        .transpose()
        .map_err(DmlError::executor)?;
    let staged_proof = recovery
        .staged_proof
        .as_ref()
        .map(|wire| ConnectorCtasPublicationProof::try_from_wire_v1(wire.as_bytes()))
        .transpose()
        .map_err(DmlError::executor)?;
    let target_digest = match (&recovery.staged_target_digest, &locator) {
        (Some(digest), _) => decode_digest(digest, "CTAS staged target")?,
        (None, Some(locator)) => locator.target_digest(),
        (None, None) => {
            return Err(DmlError::journal_corruption(
                "CTAS recovery cannot rebuild the original staged target digest",
            ));
        }
    };
    let historical_binding = locator
        .as_ref()
        .map(|locator| locator.issuance_owner().clone())
        .or_else(|| staged_proof.as_ref().map(|proof| proof.issuer().clone()))
        .unwrap_or(historical_binding(saga)?);
    let mut checkpoints = vec![ConnectorHistoricalCtasCheckpoint {
        action_id: connector_action_id(current.action_id)?,
        action: ConnectorHistoricalCtasAction::AdvanceFence,
        dispatch: ConnectorHistoricalCtasDispatchState::Completed,
        input_digest: decode_digest(&current.request_digest, "CTAS advance-fence request")?,
        evidence_digest: Some(fence_receipt_digest),
    }];
    for action_kind in [
        DmlCtasActionKind::Stage,
        DmlCtasActionKind::Publish,
        DmlCtasActionKind::Abort,
    ] {
        let base = match action_kind {
            DmlCtasActionKind::AdvanceFence => unreachable!("advance fence is added separately"),
            DmlCtasActionKind::Stage => saga.prepare_operation_id,
            DmlCtasActionKind::Publish => saga.publish_operation_id,
            DmlCtasActionKind::Abort => saga.abort_staging_operation_id,
            DmlCtasActionKind::Write => unreachable!("write is inspected by CP-3B"),
        };
        let Some(checkpoint) = latest_checkpoint_for_action(recovery, action_kind, base)? else {
            continue;
        };
        let action = match checkpoint.action {
            DmlCtasActionKind::AdvanceFence => continue,
            DmlCtasActionKind::Stage => ConnectorHistoricalCtasAction::Stage,
            DmlCtasActionKind::Publish => ConnectorHistoricalCtasAction::Publish,
            DmlCtasActionKind::Abort => ConnectorHistoricalCtasAction::Abort,
            DmlCtasActionKind::Write => continue,
        };
        let evidence_digest = if checkpoint.action == DmlCtasActionKind::Stage {
            staged_proof.as_ref().and_then(|proof| {
                (proof.purpose() == ConnectorCtasProofPurpose::Stage
                    && proof.action_id()
                        == Some(connector_action_id(checkpoint.child_operation_id).ok()?))
                .then_some(proof.digest())
            })
        } else {
            recovery
                .historical_observations
                .iter()
                .rev()
                .find(|observation| {
                    observation.action == checkpoint.action
                        && observation.child_operation_id == checkpoint.child_operation_id
                })
                .and_then(|observation| observation.proof_digest.as_deref())
                .map(|digest| decode_digest(digest, "CTAS checkpoint evidence"))
                .transpose()?
        };
        checkpoints.push(ConnectorHistoricalCtasCheckpoint {
            action_id: connector_action_id(checkpoint.child_operation_id)?,
            action,
            dispatch: if evidence_digest.is_some() {
                ConnectorHistoricalCtasDispatchState::Completed
            } else if checkpoint.dispatch_certainty
                == DmlCtasDispatchCertainty::ConfirmedNotDispatched
            {
                ConnectorHistoricalCtasDispatchState::NotDispatched
            } else {
                ConnectorHistoricalCtasDispatchState::Unknown
            },
            input_digest: decode_digest(&checkpoint.request_digest, "CTAS action request")?,
            evidence_digest,
        });
    }
    let evidence = staged_proof.filter(|proof| proof.purpose() == ConnectorCtasProofPurpose::Stage);
    ConnectorHistoricalCtasDescriptor::try_new(
        historical_binding,
        fence,
        fence_receipt_digest,
        target_digest,
        create_policy(saga)?,
        locator,
        checkpoints,
        evidence.clone(),
    )
    .map_err(DmlError::executor)
}

fn latest_checkpoint_for_action<'a>(
    recovery: &'a DmlCtasRecoveryRecord,
    action: DmlCtasActionKind,
    base: Uuid,
) -> Result<Option<&'a DmlCtasDispatchCheckpointRecord>, DmlError> {
    let mut leaf = base;
    for _ in 0..=recovery.child_supersessions.len() {
        let successors = recovery
            .child_supersessions
            .iter()
            .filter(|edge| edge.action == action && edge.predecessor_child_operation_id == leaf)
            .collect::<Vec<_>>();
        match successors.as_slice() {
            [] => break,
            [edge] => leaf = edge.successor_child_operation_id,
            _ => {
                return Err(DmlError::journal_corruption(
                    "CTAS child supersession lineage branches",
                ));
            }
        }
    }
    Ok(recovery
        .dispatch_checkpoints
        .iter()
        .find(|checkpoint| checkpoint.action == action && checkpoint.child_operation_id == leaf))
}

fn persist_observation(
    active: &mut ActiveDmlOperation,
    recovery: &mut DmlCtasRecoveryRecord,
    descriptor: &ConnectorHistoricalCtasDescriptor,
    observation: &ConnectorHistoricalCtasObservation,
    now_ms: i64,
) -> Result<(), DmlError> {
    let mut next = recovery.clone();
    let (action, child_operation_id) = observation_checkpoint_identity(recovery, observation)?;
    let proof_payload = observation
        .proof
        .as_ref()
        .map(|proof| {
            proof
                .try_to_wire_v1()
                .map_err(DmlError::executor)
                .and_then(|wire| {
                    DmlOpaquePayload::try_new(wire.to_vec()).map_err(DmlError::journal_corruption)
                })
        })
        .transpose()?;
    if next.staged_locator.is_none()
        && let (Some(locator), Some(proof), Some(proof_payload)) = (
            observation.locator.as_ref(),
            observation.proof.as_ref(),
            proof_payload.clone(),
        )
    {
        next.staged_target_digest = Some(hex::encode(locator.target_digest()));
        next.staged_locator = Some(
            DmlOpaquePayload::try_new(
                locator
                    .try_to_wire_v1()
                    .map_err(DmlError::executor)?
                    .to_vec(),
            )
            .map_err(DmlError::journal_corruption)?,
        );
        next.staged_locator_digest = Some(hex::encode(locator.digest()));
        next.staged_proof_digest = Some(hex::encode(proof.digest()));
        next.staged_proof = Some(proof_payload);
    }
    if observation.disposition.may_cleanup() && observation.locator.is_some() {
        next.cleanup_retention = DmlCtasCleanupRetention::Pending;
    }
    let durable = DmlCtasHistoricalObservationRecord {
        action,
        child_operation_id,
        disposition: durable_historical_disposition(observation.disposition),
        descriptor_digest: hex::encode(descriptor.digest()),
        descriptor_locator_digest: descriptor
            .locator
            .as_ref()
            .map(|locator| hex::encode(locator.digest()))
            .or_else(|| {
                (observation.disposition == ConnectorHistoricalCtasDisposition::NoOp
                    && observation.locator.is_none())
                .then(|| recovery.staged_locator_digest.clone())
                .flatten()
            }),
        observation_digest: hex::encode(observation.digest()),
        locator_digest: observation
            .locator
            .as_ref()
            .map(|locator| hex::encode(locator.digest())),
        proof_digest: observation
            .proof
            .as_ref()
            .map(|proof| hex::encode(proof.digest())),
        proof_payload,
        conflict_kind: observation.conflict_kind.map(durable_conflict_kind),
        failure: observation
            .failure
            .as_ref()
            .map(|failure| format!("{:?}: {}", failure.kind(), failure.message())),
        observed_at_ms: now_ms,
    };
    if recovery
        .historical_observations
        .iter()
        .any(|existing| existing.observation_digest == durable.observation_digest)
    {
        return Ok(());
    }
    next.historical_observations.push(durable);
    let projected_size = serde_json::to_vec(&next)
        .map_err(DmlError::journal_corruption)?
        .len();
    if recovery.historical_observations.len() >= MAX_CTAS_HISTORICAL_OBSERVATIONS
        || projected_size > DML_CTAS_RECOVERY_ENCODED_LIMIT
    {
        recovery.cleanup_retention = DmlCtasCleanupRetention::ManualRetention;
        recovery.next_action = StatementNextAction::ManualInspect;
        persist(
            active,
            recovery,
            Some(now_ms.saturating_add(CTAS_RECOVERY_MANUAL_DELAY_MS)),
        )?;
        return Err(operation_error(
            active,
            DmlErrorKind::Commit,
            "CTAS historical observation retention reached its durable bound",
        ));
    }
    *recovery = next;
    recovery.next_action = StatementNextAction::ManualInspect;
    persist(active, recovery, Some(now_ms))
}

fn ensure_abort_checkpoint(
    recovery: &mut DmlCtasRecoveryRecord,
    saga: &crate::dml::model::CtasSagaRecord,
    fence: &ConnectorCtasPublicationFence,
) -> Result<(), DmlError> {
    if recovery
        .dispatch_checkpoints
        .iter()
        .any(|checkpoint| checkpoint.action == DmlCtasActionKind::Abort)
    {
        return Ok(());
    }
    let target_digest = recovery
        .staged_target_digest
        .as_deref()
        .map(|digest| decode_digest(digest, "CTAS staged target"))
        .transpose()?
        .unwrap_or(fence.digest());
    let mut digest = Sha256::new();
    digest.update(HISTORICAL_ABORT_DOMAIN);
    digest.update(fence.operation_id().to_bytes());
    digest.update(saga.abort_staging_operation_id.as_bytes());
    digest.update(target_digest);
    recovery
        .dispatch_checkpoints
        .push(DmlCtasDispatchCheckpointRecord {
            action: DmlCtasActionKind::Abort,
            child_operation_id: saga.abort_staging_operation_id,
            request_digest: hex::encode(digest.finalize()),
            dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
            dispatched_at_ms: None,
        });
    Ok(())
}

fn decode_ctas_write_evidence(
    encoded: &str,
) -> Result<novarocks_spi::connector::ExternalMutationEvidence, DmlError> {
    let wire = hex::decode(encoded).map_err(DmlError::journal_corruption)?;
    ExternalMutationEvidenceWire::try_from_wire(wire)
        .and_then(|wire| wire.try_decode())
        .map_err(DmlError::journal_corruption)
}

fn historical_write_request(
    saga: &crate::dml::model::CtasSagaRecord,
    recovery: &DmlCtasRecoveryRecord,
    cohort_set_digest: [u8; 32],
    aggregate_digest: Option<[u8; 32]>,
    evidence: Option<&novarocks_spi::connector::ExternalMutationEvidence>,
    old_fence: Option<&crate::dml::model::DmlExternalFenceReceiptRecord>,
) -> Result<DmlHistoricalWriteRequestRecord, DmlError> {
    let checkpoint = recovery
        .dispatch_checkpoints
        .iter()
        .rev()
        .find(|checkpoint| checkpoint.action == DmlCtasActionKind::Write)
        .ok_or_else(|| DmlError::journal_corruption("CTAS historical writer has no checkpoint"))?;
    let old_attempt = old_fence
        .map(|fence| fence.identity.coordination_attempt_id)
        .or_else(|| {
            recovery
                .catalog_fence_history
                .first()
                .or(recovery.catalog_fence.as_ref())
                .map(|fence| fence.action_id)
        });
    let mut request = DmlHistoricalWriteRequestRecord {
        old_provider_id: saga
            .provider_id
            .clone()
            .ok_or_else(|| DmlError::journal_corruption("CTAS writer provider is missing"))?,
        old_connector_instance_id: saga.connector_instance_id.clone().ok_or_else(|| {
            DmlError::journal_corruption("CTAS writer connector instance is missing")
        })?,
        old_connector_incarnation: saga.connector_incarnation.clone().ok_or_else(|| {
            DmlError::journal_corruption("CTAS writer connector incarnation is missing")
        })?,
        old_coordination_attempt_id: old_attempt,
        old_fence: old_fence.cloned(),
        write_operation_id: saga.write_operation_id,
        cohort_set_digest: hex::encode(cohort_set_digest),
        aggregate_write_digest: aggregate_digest.map(hex::encode),
        dispatch_certainty: match checkpoint.dispatch_certainty {
            DmlCtasDispatchCertainty::ConfirmedNotDispatched => {
                DmlHistoricalDispatchCertainty::ConfirmedNotDispatched
            }
            DmlCtasDispatchCertainty::PossiblyDispatched => {
                DmlHistoricalDispatchCertainty::PossiblyDispatched
            }
        },
        writer_output_checkpointed: aggregate_digest.is_some(),
        commit_dispatched_at_ms: checkpoint.dispatched_at_ms,
        request_digest: String::new(),
    };
    let mut digest = Sha256::new();
    digest.update(b"novarocks.frontend.ctas-historical-write-request.v1\0");
    for bytes in [
        request.old_provider_id.as_bytes(),
        request.old_connector_instance_id.as_bytes(),
        request.old_connector_incarnation.as_bytes(),
        request.write_operation_id.as_bytes(),
        request.cohort_set_digest.as_bytes(),
        request
            .aggregate_write_digest
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    ] {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    digest.update([match request.dispatch_certainty {
        DmlHistoricalDispatchCertainty::ConfirmedNotDispatched => 0,
        DmlHistoricalDispatchCertainty::PossiblyDispatched => 1,
        DmlHistoricalDispatchCertainty::ConfirmedDispatched => 2,
    }]);
    digest.update([u8::from(request.writer_output_checkpointed)]);
    digest.update([u8::from(old_fence.is_some())]);
    if let Some(old_fence) = old_fence {
        digest.update(old_fence.identity.coordination_attempt_id.as_bytes());
        digest.update(
            old_fence
                .identity
                .generation
                .control_plane_incarnation
                .to_be_bytes(),
        );
        digest.update(old_fence.identity.generation.resource_epoch.to_be_bytes());
        digest.update(old_fence.identity.generation.fence_generation.to_be_bytes());
        digest.update(old_fence.fence_digest.as_bytes());
        digest.update(old_fence.receipt_digest.as_bytes());
    }
    digest.update(
        evidence
            .map(|evidence| evidence.digest())
            .unwrap_or([0; 32]),
    );
    request.request_digest = hex::encode(digest.finalize());
    Ok(request)
}

fn historical_write_checkpoints(
    saga: &crate::dml::model::CtasSagaRecord,
    recovery: &DmlCtasRecoveryRecord,
) -> Vec<ConnectorHistoricalWriteCheckpoint> {
    let dispatch = recovery
        .dispatch_checkpoints
        .iter()
        .rev()
        .find(|checkpoint| checkpoint.action == DmlCtasActionKind::Write)
        .map(|checkpoint| match checkpoint.dispatch_certainty {
            DmlCtasDispatchCertainty::ConfirmedNotDispatched => {
                ConnectorHistoricalWriteDispatchState::NotDispatched
            }
            DmlCtasDispatchCertainty::PossiblyDispatched => {
                ConnectorHistoricalWriteDispatchState::Unknown
            }
        })
        .unwrap_or(ConnectorHistoricalWriteDispatchState::NotDispatched);
    vec![
        ConnectorHistoricalWriteCheckpoint {
            phase: ConnectorHistoricalWritePhase::Activated,
            state: ConnectorHistoricalWriteDispatchState::Completed,
            evidence_digest: None,
        },
        ConnectorHistoricalWriteCheckpoint {
            phase: ConnectorHistoricalWritePhase::WritersDispatched,
            state: dispatch,
            evidence_digest: saga
                .write_fact
                .as_ref()
                .and_then(|fact| fact.evidence.as_ref())
                .map(|evidence| Sha256::digest(evidence.as_bytes()).into()),
        },
        ConnectorHistoricalWriteCheckpoint {
            phase: ConnectorHistoricalWritePhase::WritersCompleted,
            state: if saga.aggregate_write_digest.is_some() {
                ConnectorHistoricalWriteDispatchState::Completed
            } else {
                ConnectorHistoricalWriteDispatchState::Unknown
            },
            evidence_digest: saga
                .aggregate_write_digest
                .as_deref()
                .and_then(|digest| decode_digest(digest, "CTAS aggregate write").ok()),
        },
    ]
}

fn finish_success(
    active: &mut ActiveDmlOperation,
    observation: &ConnectorHistoricalCtasObservation,
    no_op: bool,
) -> Result<(), DmlError> {
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = if no_op {
        CtasSagaPhase::NoOp
    } else {
        CtasSagaPhase::Committed
    };
    saga.publish_fact = Some(historical_observation_fact(observation));
    saga.next_action = StatementNextAction::None;
    match active.stored.state {
        OperationState::Finalized => active.mutate_statement(
            OperationState::Finalized,
            OperationPayload::CtasSaga(saga),
            None,
        ),
        OperationState::Committed => active.mutate_statement(
            OperationState::Finalized,
            OperationPayload::CtasSaga(saga),
            None,
        ),
        OperationState::CommitUnknown | OperationState::Aborting => {
            active.mutate_statement(
                OperationState::Committed,
                OperationPayload::CtasSaga(saga.clone()),
                None,
            )?;
            active.mutate_statement(
                OperationState::Finalized,
                OperationPayload::CtasSaga(saga),
                None,
            )
        }
        OperationState::Preparing | OperationState::Writing | OperationState::Collecting => {
            active.mutate_statement(
                OperationState::Committing,
                OperationPayload::CtasSaga(saga.clone()),
                None,
            )?;
            active.mutate_statement(
                OperationState::Committed,
                OperationPayload::CtasSaga(saga.clone()),
                None,
            )?;
            active.mutate_statement(
                OperationState::Finalized,
                OperationPayload::CtasSaga(saga),
                None,
            )
        }
        OperationState::Committing => {
            active.mutate_statement(
                OperationState::Committed,
                OperationPayload::CtasSaga(saga.clone()),
                None,
            )?;
            active.mutate_statement(
                OperationState::Finalized,
                OperationPayload::CtasSaga(saga),
                None,
            )
        }
        OperationState::FinalizeFailedKnownCommitted => {
            active.mutate_statement(
                OperationState::Finalizing,
                OperationPayload::CtasSaga(saga.clone()),
                None,
            )?;
            active.mutate_statement(
                OperationState::Finalized,
                OperationPayload::CtasSaga(saga),
                None,
            )
        }
        state => Err(operation_error(
            active,
            DmlErrorKind::JournalCorruption,
            format!("published CTAS truth conflicts with terminal state {state:?}"),
        )),
    }
}

fn finish_aborted(
    active: &mut ActiveDmlOperation,
    observation: &ConnectorHistoricalCtasObservation,
) -> Result<(), DmlError> {
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::Failed;
    saga.abort_staging_fact = Some(historical_observation_fact(observation));
    saga.next_action = StatementNextAction::None;
    match active.stored.state {
        OperationState::Aborting => active.mutate_statement(
            OperationState::Aborted,
            OperationPayload::CtasSaga(saga),
            None,
        ),
        OperationState::Aborted | OperationState::FailedKnownUncommitted => {
            active.mutate_statement(active.stored.state, OperationPayload::CtasSaga(saga), None)
        }
        OperationState::Preparing
        | OperationState::Writing
        | OperationState::Collecting
        | OperationState::Committing
        | OperationState::CommitUnknown => active.mutate_statement(
            OperationState::FailedKnownUncommitted,
            OperationPayload::CtasSaga(saga),
            None,
        ),
        state => Err(operation_error(
            active,
            DmlErrorKind::JournalCorruption,
            format!("aborted CTAS truth conflicts with terminal state {state:?}"),
        )),
    }
}

fn finish_cleanup_aborted(
    active: &mut ActiveDmlOperation,
    receipt: &novarocks_spi::connector::ConnectorHistoricalCtasCleanupReceipt,
) -> Result<(), DmlError> {
    let evidence = receipt
        .proof
        .try_to_wire_v1()
        .map_err(DmlError::executor)
        .map(hex::encode)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::Failed;
    saga.abort_staging_fact = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::KnownCommitted,
        receipt: Some(hex::encode(receipt.digest())),
        evidence: Some(evidence),
        finalization_failure: None,
        failure: None,
    });
    saga.next_action = StatementNextAction::None;
    match active.stored.state {
        OperationState::FailedKnownUncommitted | OperationState::Aborted => {
            active.mutate_statement(active.stored.state, OperationPayload::CtasSaga(saga), None)
        }
        OperationState::Aborting => active.mutate_statement(
            OperationState::Aborted,
            OperationPayload::CtasSaga(saga),
            None,
        ),
        _ => {
            active.mutate_statement(
                OperationState::Aborting,
                OperationPayload::CtasSaga(saga.clone()),
                None,
            )?;
            active.mutate_statement(
                OperationState::Aborted,
                OperationPayload::CtasSaga(saga),
                None,
            )
        }
    }
}

fn finish_conflict(
    active: &mut ActiveDmlOperation,
    observation: &ConnectorHistoricalCtasObservation,
    due: Option<i64>,
) -> Result<(), DmlError> {
    let mut saga = ctas_record(&active.stored)?;
    if matches!(
        active.stored.state,
        OperationState::FailedKnownUncommitted | OperationState::Aborted
    ) {
        saga.abort_staging_fact = Some(historical_observation_fact(observation));
        saga.next_action = StatementNextAction::ManualInspect;
        return active.mutate_statement(active.stored.state, OperationPayload::CtasSaga(saga), due);
    }
    saga.phase = CtasSagaPhase::Conflict;
    saga.publish_fact = Some(historical_observation_fact(observation));
    saga.next_action = if due.is_some() {
        StatementNextAction::ManualInspect
    } else {
        StatementNextAction::None
    };
    match active.stored.state {
        OperationState::Finalized | OperationState::Committed => Err(operation_error(
            active,
            DmlErrorKind::JournalCorruption,
            "conflicting CTAS observation arrived after committed visibility",
        )),
        _ => active.mutate_statement(
            OperationState::FailedKnownUncommitted,
            OperationPayload::CtasSaga(saga),
            due,
        ),
    }
}

fn finish_unresolved(
    active: &mut ActiveDmlOperation,
    observation: &ConnectorHistoricalCtasObservation,
    now_ms: i64,
) -> Result<(), DmlError> {
    let mut saga = ctas_record(&active.stored)?;
    saga.next_action = StatementNextAction::ManualInspect;
    let due = Some(now_ms.saturating_add(CTAS_RECOVERY_MANUAL_DELAY_MS));
    match active.stored.state {
        OperationState::Committed
        | OperationState::Finalized
        | OperationState::Aborted
        | OperationState::FailedKnownUncommitted
        | OperationState::FinalizeFailedKnownCommitted => {
            // A cleanup-only ambiguity cannot erase an already durable user
            // terminal. Keep the success/failure fact and attach the latest
            // inspection to the staged-abort slot.
            saga.abort_staging_fact = Some(historical_observation_fact(observation));
            active.mutate_statement(active.stored.state, OperationPayload::CtasSaga(saga), due)
        }
        _ => {
            saga.phase =
                if observation.disposition == ConnectorHistoricalCtasDisposition::Unsupported {
                    CtasSagaPhase::Unsupported
                } else {
                    CtasSagaPhase::PublishUnknown
                };
            saga.publish_fact = Some(historical_observation_fact(observation));
            active.mutate_statement(
                OperationState::CommitUnknown,
                OperationPayload::CtasSaga(saga),
                due,
            )
        }
    }
}

fn park(
    active: &mut ActiveDmlOperation,
    recovery: &mut DmlCtasRecoveryRecord,
    now_ms: i64,
    manual: bool,
) -> Result<(), DmlError> {
    recovery.next_action = StatementNextAction::ManualInspect;
    if recovery.staged_locator.is_some() {
        // Ambiguity is not a terminal retention decision. A later exact
        // Staged/NoOp observation must still be able to authorize cleanup.
        recovery.cleanup_retention = DmlCtasCleanupRetention::Pending;
    }
    persist(
        active,
        recovery,
        Some(now_ms.saturating_add(if manual {
            CTAS_RECOVERY_MANUAL_DELAY_MS
        } else {
            CTAS_RECOVERY_RETRY_DELAY_MS
        })),
    )
}

fn persist(
    active: &mut ActiveDmlOperation,
    recovery: &mut DmlCtasRecoveryRecord,
    due: Option<i64>,
) -> Result<(), DmlError> {
    recovery.updated_at_ms = crate::dml::now_unix_millis();
    active.record_ctas_recovery(recovery.clone(), due)
}

fn table_identity(active: &ActiveDmlOperation) -> Result<ConnectorTableIdentity, DmlError> {
    Ok(ConnectorTableIdentity {
        instance_id: ConnectorInstanceId::parse(&active.stored.target.catalog)
            .map_err(DmlError::executor)?,
        namespace: active.stored.target.namespace.clone().into(),
        table: active.stored.target.table.clone().into(),
    })
}

fn historical_binding(
    saga: &crate::dml::model::CtasSagaRecord,
) -> Result<ConnectorExecutionBindingKey, DmlError> {
    let instance_id = ConnectorInstanceId::parse(
        saga.connector_instance_id
            .as_deref()
            .ok_or_else(|| DmlError::journal_corruption("CTAS saga has no connector instance"))?,
    )
    .map_err(DmlError::executor)?;
    let incarnation =
        hex::decode(saga.connector_incarnation.as_deref().ok_or_else(|| {
            DmlError::journal_corruption("CTAS saga has no connector incarnation")
        })?)
        .map_err(DmlError::journal_corruption)?;
    let bytes: [u8; 16] = incarnation
        .try_into()
        .map_err(|_| DmlError::journal_corruption("CTAS connector incarnation is not 16 bytes"))?;
    Ok(ConnectorExecutionBindingKey {
        instance_id,
        incarnation: ConnectorInstanceIncarnation::from_bytes(bytes),
    })
}

fn create_policy(saga: &crate::dml::model::CtasSagaRecord) -> Result<CreatePolicy, DmlError> {
    match saga.create_policy.as_str() {
        crate::dml::model::CTAS_CREATE_POLICY_FAIL_IF_EXISTS => Ok(CreatePolicy::FailIfExists),
        crate::dml::model::CTAS_CREATE_POLICY_NO_OP_IF_EXISTS => Ok(CreatePolicy::NoOpIfExists),
        value => Err(DmlError::journal_corruption(format!(
            "unknown durable CTAS create policy {value}"
        ))),
    }
}

fn request_context() -> Result<ConnectorRequestContext, DmlError> {
    ConnectorRequestContext::try_new(
        Instant::now() + CTAS_RECOVERY_ACTION_DEADLINE,
        Arc::new(NeverCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(DmlError::executor)
}

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn operation_error(
    active: &ActiveDmlOperation,
    kind: DmlErrorKind,
    message: impl std::fmt::Display,
) -> DmlError {
    DmlError::new(kind, message)
        .with_operation_id(active.operation_id())
        .with_next_action(StatementNextAction::ManualInspect)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};

    use super::*;
    use async_trait::async_trait;
    use novarocks::engine::ctas_engine::*;
    use novarocks_spi::connector::{
        ConnectorClusterIdentity, ConnectorCtasAbortResult, ConnectorCtasPublishResult,
        ConnectorCtasStagedLocator, ConnectorError, ConnectorErrorKind,
        ConnectorExecutionBindingKey, ConnectorExternalFenceGeneration,
        ConnectorExternalFenceReceipt, ConnectorHistoricalCtasCleanupReceipt,
        ConnectorHistoricalWriteCleanupReceipt, ConnectorHistoricalWriteCleanupRequest,
        ConnectorHistoricalWriteDescriptor, ConnectorHistoricalWriteDisposition,
        ConnectorHistoricalWriteFenceRaiseRequest, ConnectorHistoricalWriteObservation,
        ConnectorHistoricalWriteOutcomeFacts, ConnectorHistoricalWriteProof,
        ConnectorHistoricalWriteRecovery, ConnectorInstanceDescriptor, ConnectorMutationFailure,
        ConnectorMutationFailureKind, ConnectorMutationOperationId, ConnectorProviderId,
        ConnectorWriteOperationCompletion, ExternalMutationEffect, ExternalMutationEvidence,
        ExternalMutationFinalization, ExternalMutationOutcome,
    };
    use novarocks_spi::state_store::WriteTransaction;

    use crate::dml::coordination::{ActiveDmlOperation, DmlExternalFenceProposal};
    use crate::dml::journal::{
        DmlMutationAuthority, DmlMutationAuthorityValidator, OperationJournal,
    };
    use crate::dml::model::{
        DML_OPERATION_SCHEMA_VERSION, DmlCtasRecoveryMutationRequest,
        DmlHistoricalWriteRecoveryMutationRequest, DmlHistoricalWriteRecoveryRecord,
        DmlOperationId, OperationMutationRequest, OperationState, OperationTarget, StoredOperation,
        validate_ctas_recovery_transition, validate_historical_write_recovery_transition,
    };
    use crate::dml::write_recovery::HistoricalWriteRecoveryHandle;

    struct AlwaysCurrent;

    #[async_trait]
    impl DmlMutationAuthorityValidator for AlwaysCurrent {
        async fn validate_in(
            &self,
            _transaction: &mut dyn WriteTransaction,
        ) -> Result<(), DmlError> {
            Ok(())
        }
    }

    struct RecoveryDriveJournal {
        operation: Mutex<StoredOperation>,
        recovery: Mutex<DmlCtasRecoveryRecord>,
        write_recovery: Mutex<Option<DmlHistoricalWriteRecoveryRecord>>,
    }

    impl OperationJournal for RecoveryDriveJournal {
        fn create_preparing(
            &self,
            _request: crate::dml::model::CreatePreparingRequest,
        ) -> Result<DmlOperationId, DmlError> {
            unreachable!("recovery drive does not create operations")
        }

        fn transition(
            &self,
            _operation_id: DmlOperationId,
            _to: OperationState,
        ) -> Result<(), DmlError> {
            unreachable!("fenced recovery uses authorized statement mutation")
        }

        fn record_fact(
            &self,
            _operation_id: DmlOperationId,
            _fact: crate::dml::model::OperationFact,
        ) -> Result<(), DmlError> {
            unreachable!("CTAS recovery records statement facts")
        }

        fn load(&self, operation_id: DmlOperationId) -> Result<Option<StoredOperation>, DmlError> {
            let operation = self.operation.lock().expect("operation lock");
            Ok((operation.operation_id == operation_id).then(|| operation.clone()))
        }

        fn load_ctas_recovery(
            &self,
            operation_id: DmlOperationId,
        ) -> Result<Option<DmlCtasRecoveryRecord>, DmlError> {
            if self.operation.lock().expect("operation lock").operation_id != operation_id {
                return Ok(None);
            }
            Ok(Some(self.recovery.lock().expect("recovery lock").clone()))
        }

        fn load_historical_write_recovery(
            &self,
            operation_id: DmlOperationId,
        ) -> Result<Option<DmlHistoricalWriteRecoveryRecord>, DmlError> {
            if self.operation.lock().expect("operation lock").operation_id != operation_id {
                return Ok(None);
            }
            Ok(self
                .write_recovery
                .lock()
                .expect("write recovery lock")
                .clone())
        }

        fn load_external_fence(
            &self,
            operation_id: DmlOperationId,
        ) -> Result<Option<crate::dml::model::DmlExternalFenceReceiptRecord>, DmlError> {
            if self.operation.lock().expect("operation lock").operation_id != operation_id {
                return Ok(None);
            }
            Ok(None)
        }

        fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
            Ok(vec![self.operation.lock().expect("operation lock").clone()])
        }

        fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError> {
            self.list_operations()
        }

        fn preflight_ctas_recovery(
            &self,
            request: &DmlCtasRecoveryMutationRequest,
        ) -> Result<(), DmlError> {
            let existing = self.recovery.lock().expect("recovery lock");
            validate_ctas_recovery_transition(Some(&existing), &request.recovery)
                .map_err(DmlError::journal_corruption)
        }

        fn preflight_historical_write_recovery(
            &self,
            request: &DmlHistoricalWriteRecoveryMutationRequest,
        ) -> Result<(), DmlError> {
            let existing = self.write_recovery.lock().expect("write recovery lock");
            validate_historical_write_recovery_transition(existing.as_ref(), &request.recovery)
                .map_err(DmlError::journal_corruption)
        }

        fn record_ctas_recovery_authorized(
            &self,
            request: DmlCtasRecoveryMutationRequest,
            recovery_due_at_ms: Option<i64>,
            authority: DmlMutationAuthority,
        ) -> Result<StoredOperation, DmlError> {
            let mut operation = self.operation.lock().expect("operation lock");
            if operation.operation_id != request.operation_id
                || operation.revision != request.expected_revision
                || authority.coordination_attempt_id() != request.recovery.recovery_attempt_id
            {
                return Err(DmlError::journal_unresolved("stale CTAS recovery mutation"));
            }
            let mut existing = self.recovery.lock().expect("recovery lock");
            validate_ctas_recovery_transition(Some(&existing), &request.recovery)
                .map_err(DmlError::journal_corruption)?;
            *existing = request.recovery;
            operation.revision += 1;
            operation.last_mutation_id = request.mutation_id;
            operation.recovery_due_at_ms = recovery_due_at_ms;
            Ok(operation.clone())
        }

        fn record_historical_write_recovery_authorized(
            &self,
            request: DmlHistoricalWriteRecoveryMutationRequest,
            recovery_due_at_ms: Option<i64>,
            authority: DmlMutationAuthority,
        ) -> Result<StoredOperation, DmlError> {
            let mut operation = self.operation.lock().expect("operation lock");
            let recovery_attempt = self
                .recovery
                .lock()
                .expect("recovery lock")
                .recovery_attempt_id;
            if operation.operation_id != request.operation_id
                || operation.revision != request.expected_revision
                || authority.coordination_attempt_id() != recovery_attempt
            {
                return Err(DmlError::journal_unresolved(
                    "stale CTAS historical writer mutation",
                ));
            }
            let mut existing = self.write_recovery.lock().expect("write recovery lock");
            validate_historical_write_recovery_transition(existing.as_ref(), &request.recovery)
                .map_err(DmlError::journal_corruption)?;
            *existing = Some(request.recovery);
            operation.revision += 1;
            operation.last_mutation_id = request.mutation_id;
            operation.recovery_due_at_ms = recovery_due_at_ms;
            Ok(operation.clone())
        }

        fn mutate_statement_operation_authorized(
            &self,
            request: OperationMutationRequest,
            recovery_due_at_ms: Option<i64>,
            authority: DmlMutationAuthority,
        ) -> Result<StoredOperation, DmlError> {
            let recovery_attempt = self
                .recovery
                .lock()
                .expect("recovery lock")
                .recovery_attempt_id;
            let mut operation = self.operation.lock().expect("operation lock");
            if operation.operation_id != request.operation_id
                || operation.revision != request.expected_revision
                || authority.coordination_attempt_id() != recovery_attempt
            {
                return Err(DmlError::journal_unresolved("stale CTAS terminal mutation"));
            }
            operation.state = request.state;
            operation.payload = request.payload;
            operation.revision += 1;
            operation.last_mutation_id = request.mutation_id;
            operation.recovery_due_at_ms = recovery_due_at_ms;
            Ok(operation.clone())
        }

        fn mutate_statement_operation(
            &self,
            request: OperationMutationRequest,
        ) -> Result<StoredOperation, DmlError> {
            let mut operation = self.operation.lock().expect("operation lock");
            if operation.operation_id != request.operation_id
                || operation.revision != request.expected_revision
            {
                return Err(DmlError::journal_unresolved("stale CTAS terminal mutation"));
            }
            operation.state = request.state;
            operation.payload = request.payload;
            operation.revision += 1;
            operation.last_mutation_id = request.mutation_id;
            if request.state.is_finished()
                && !self
                    .recovery
                    .lock()
                    .expect("recovery lock")
                    .requires_recovery_scan()
            {
                operation.recovery_due_at_ms = None;
            }
            Ok(operation.clone())
        }
    }

    struct RecoveryDriveEngine {
        disposition: ConnectorHistoricalCtasDisposition,
        inspect_calls: AtomicUsize,
        cleanup_calls: AtomicUsize,
        inspect_entered: Option<Arc<Barrier>>,
        inspect_release: Option<Arc<Barrier>>,
    }

    impl RecoveryDriveEngine {
        fn new(disposition: ConnectorHistoricalCtasDisposition) -> Arc<Self> {
            Arc::new(Self {
                disposition,
                inspect_calls: AtomicUsize::new(0),
                cleanup_calls: AtomicUsize::new(0),
                inspect_entered: None,
                inspect_release: None,
            })
        }

        fn blocked(
            disposition: ConnectorHistoricalCtasDisposition,
            inspect_entered: Arc<Barrier>,
            inspect_release: Arc<Barrier>,
        ) -> Arc<Self> {
            Arc::new(Self {
                disposition,
                inspect_calls: AtomicUsize::new(0),
                cleanup_calls: AtomicUsize::new(0),
                inspect_entered: Some(inspect_entered),
                inspect_release: Some(inspect_release),
            })
        }
    }

    impl CtasEngine for RecoveryDriveEngine {
        fn classify_ctas(&self, _sql: &str) -> Result<Option<CtasCommand>, String> {
            unreachable!("recovery never classifies SQL")
        }

        fn preflight_ctas_target(
            &self,
            _command: &CtasCommand,
            _current_catalog: Option<&str>,
            _current_database: &str,
        ) -> Result<CtasTargetPreflightOutcome, CtasFailure> {
            unreachable!("recovery never uses ordinary preflight")
        }

        fn prepare_ctas_source(
            &self,
            _preflight: &dyn CtasPreparedTargetPreflight,
            _request: PrepareCtasSourceRequest,
        ) -> Result<PreparedCtasSource, CtasFailure> {
            unreachable!("recovery never prepares source")
        }

        fn prepare_ctas_fence_advance(
            &self,
            _preflight: &dyn CtasPreparedTargetPreflight,
            _fence: ConnectorCtasPublicationFence,
            _action_id: ConnectorCtasActionId,
        ) -> Result<PreparedCtasCatalogAction, CtasFailure> {
            unreachable!("recovery uses the historical facet")
        }

        fn advance_ctas_fence(
            &self,
            _action: &dyn CtasPreparedCatalogAction,
        ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
            unreachable!("recovery uses the historical facet")
        }

        fn prepare_ctas_target(
            &self,
            _source: &dyn CtasPreparedSource,
            _fence: ConnectorCtasPublicationFence,
            _stage_action_id: ConnectorCtasActionId,
            _policy: CreatePolicy,
        ) -> Result<PreparedCtasCatalogAction, CtasFailure> {
            unreachable!("recovery never stages")
        }

        fn stage_ctas_target(
            &self,
            _action: &dyn CtasPreparedCatalogAction,
        ) -> Result<CtasTargetStageResult, ConnectorCtasFailure> {
            unreachable!("recovery never stages")
        }

        fn prepare_ctas_write(
            &self,
            _source: &dyn CtasPreparedSource,
            _target: &dyn CtasPreparedTarget,
            _write_operation_id: ConnectorWriteOperationId,
        ) -> Result<PreparedCtasWrite, CtasFailure> {
            unreachable!("recovery never prepares writers")
        }

        fn execute_ctas_write(&self, _prepared: &dyn CtasPreparedWrite) -> CtasWriteOutcome {
            unreachable!("recovery never executes writers")
        }

        fn reconcile_ctas_write(
            &self,
            _prepared: &dyn CtasPreparedWrite,
            _evidence: ExternalMutationEvidence,
        ) -> CtasWriteOutcome {
            unreachable!("recovery never uses an old ordinary writer")
        }

        fn prepare_publish_ctas(
            &self,
            _target: &dyn CtasPreparedTarget,
            _action_id: ConnectorCtasActionId,
            _completion: ConnectorWriteOperationCompletion,
        ) -> Result<PreparedCtasCatalogAction, CtasFailure> {
            unreachable!("recovery never publishes through ordinary authority")
        }

        fn publish_ctas(
            &self,
            _action: &dyn CtasPreparedCatalogAction,
        ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure> {
            unreachable!("recovery never publishes through ordinary authority")
        }

        fn prepare_abort_ctas(
            &self,
            _target: &dyn CtasPreparedTarget,
            _action_id: ConnectorCtasActionId,
        ) -> Result<PreparedCtasCatalogAction, CtasFailure> {
            unreachable!("recovery never aborts through ordinary authority")
        }

        fn abort_ctas(
            &self,
            _action: &dyn CtasPreparedCatalogAction,
        ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure> {
            unreachable!("recovery never aborts through ordinary authority")
        }

        fn inspect_historical_ctas(
            &self,
            descriptor: ConnectorHistoricalCtasDescriptor,
            _context: ConnectorRequestContext,
        ) -> Result<ConnectorHistoricalCtasObservation, ConnectorCtasFailure> {
            self.inspect_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(entered) = &self.inspect_entered {
                entered.wait();
            }
            if let Some(release) = &self.inspect_release {
                release.wait();
            }
            let binding = descriptor.historical_binding.clone();
            let locator = if self.disposition == ConnectorHistoricalCtasDisposition::Staged {
                let stage = descriptor
                    .checkpoints
                    .iter()
                    .find(|checkpoint| checkpoint.action == ConnectorHistoricalCtasAction::Stage)
                    .expect("staged recovery checkpoint");
                Some(
                    ConnectorCtasStagedLocator::try_new(
                        binding.clone(),
                        &descriptor.fence,
                        stage.action_id,
                        descriptor.target_digest,
                        Bytes::from_static(b"full-drive-staged-locator"),
                    )
                    .expect("historical staged locator"),
                )
            } else {
                None
            };
            let (proof, failure) = match self.disposition {
                ConnectorHistoricalCtasDisposition::Ambiguous => (
                    None,
                    Some(ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "injected ambiguous catalog truth",
                    )),
                ),
                ConnectorHistoricalCtasDisposition::Published
                | ConnectorHistoricalCtasDisposition::Staged => (
                    Some(
                        ConnectorCtasPublicationProof::try_new(
                            binding.clone(),
                            &descriptor.fence,
                            match self.disposition {
                                ConnectorHistoricalCtasDisposition::Published => {
                                    ConnectorCtasProofPurpose::HistoricalPublished
                                }
                                ConnectorHistoricalCtasDisposition::Staged => {
                                    ConnectorCtasProofPurpose::HistoricalStaged
                                }
                                _ => unreachable!("conclusive proof purpose"),
                            },
                            None,
                            descriptor.digest(),
                            locator.as_ref(),
                            Bytes::from_static(b"full-drive-proof"),
                        )
                        .expect("historical proof"),
                    ),
                    None,
                ),
                _ => unreachable!("unsupported full-drive catalog disposition"),
            };
            ConnectorHistoricalCtasObservation::try_new(
                binding,
                &descriptor,
                self.disposition,
                locator,
                proof,
                None,
                failure,
            )
            .map_err(|error| {
                ConnectorCtasFailure::CommittedResponseInvalid(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::CorruptData,
                    error.to_string(),
                ))
            })
        }

        fn advance_historical_ctas_fence(
            &self,
            request: ConnectorCtasAdvanceFenceRequest,
        ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
            ConnectorCtasPublicationFenceReceipt::try_new(
                &request,
                Bytes::from_static(b"full-drive-fence-receipt"),
            )
            .map_err(|error| {
                ConnectorCtasFailure::CommittedResponseInvalid(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::CorruptData,
                    error.to_string(),
                ))
            })
        }

        fn cleanup_historical_ctas(
            &self,
            request: ConnectorHistoricalCtasCleanupRequest,
        ) -> Result<ConnectorHistoricalCtasCleanupReceipt, ConnectorCtasFailure> {
            self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
            if self.disposition != ConnectorHistoricalCtasDisposition::Staged {
                return Err(ConnectorCtasFailure::KnownNotDispatched(
                    ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::InvalidRequest,
                        "cleanup was not expected",
                    ),
                ));
            }
            request.validate().map_err(|error| {
                ConnectorCtasFailure::CommittedResponseInvalid(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::CorruptData,
                    error.to_string(),
                ))
            })?;
            let locator = request.observation.locator.as_ref();
            let proof = ConnectorCtasPublicationProof::try_new(
                request.observation.inspection_binding.clone(),
                &request.descriptor.fence,
                ConnectorCtasProofPurpose::HistoricalCleanup,
                None,
                request.observation.digest(),
                locator,
                Bytes::from_static(b"full-drive-cleanup-proof"),
            )
            .map_err(|error| {
                ConnectorCtasFailure::CommittedResponseInvalid(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::CorruptData,
                    error.to_string(),
                ))
            })?;
            ConnectorHistoricalCtasCleanupReceipt::try_new(&request, proof).map_err(|error| {
                ConnectorCtasFailure::CommittedResponseInvalid(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::CorruptData,
                    error.to_string(),
                ))
            })
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RecoveryWriterPlan {
        StagedCleanupLost,
        StagedCleanupForeignDirectReceipt,
        StagedCleanupForeignReconcileReceipt,
        Ambiguous,
    }

    struct RecoveryWriteFacet {
        key: ConnectorExecutionBindingKey,
        plan: RecoveryWriterPlan,
        raised: Mutex<Option<novarocks_spi::connector::ConnectorExternalOperationFence>>,
        issued: Mutex<Vec<[u8; 32]>>,
        cleanup_binding: Mutex<Option<([u8; 32], [u8; 32])>>,
        cleanup_calls: AtomicUsize,
        reconcile_calls: AtomicUsize,
    }

    impl RecoveryWriteFacet {
        fn new(plan: RecoveryWriterPlan) -> Arc<Self> {
            Arc::new(Self {
                key: ConnectorExecutionBindingKey {
                    instance_id: ConnectorInstanceId::parse("rest").expect("instance"),
                    incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
                },
                plan,
                raised: Mutex::new(None),
                issued: Mutex::new(Vec::new()),
                cleanup_binding: Mutex::new(None),
                cleanup_calls: AtomicUsize::new(0),
                reconcile_calls: AtomicUsize::new(0),
            })
        }
    }

    impl ConnectorHistoricalWriteRecovery for RecoveryWriteFacet {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn raise_external_fence(
            &self,
            request: ConnectorHistoricalWriteFenceRaiseRequest,
        ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
            request.validate()?;
            let receipt = ConnectorExternalFenceReceipt::try_new(
                &request.raised,
                Bytes::from_static(b"full-drive-writer-fence"),
            )?;
            *self.raised.lock().expect("raised fence lock") = Some(request.raised);
            Ok(receipt)
        }

        fn inspect(
            &self,
            descriptor: ConnectorHistoricalWriteDescriptor,
            _context: ConnectorRequestContext,
        ) -> Result<ConnectorHistoricalWriteObservation, ConnectorError> {
            descriptor.validate()?;
            let raised = self.raised.lock().expect("raised fence lock");
            if raised.as_ref().map(|fence| fence.digest()) != Some(descriptor.raised_fence.digest())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "historical writer inspection ran before its exact fence raise",
                ));
            }
            let (disposition, cleanup_required) = match self.plan {
                RecoveryWriterPlan::StagedCleanupLost
                | RecoveryWriterPlan::StagedCleanupForeignDirectReceipt
                | RecoveryWriterPlan::StagedCleanupForeignReconcileReceipt => {
                    (ConnectorHistoricalWriteDisposition::Staged, true)
                }
                RecoveryWriterPlan::Ambiguous => {
                    (ConnectorHistoricalWriteDisposition::Ambiguous, false)
                }
            };
            let observation = ConnectorHistoricalWriteObservation::try_new(
                &descriptor,
                disposition,
                ConnectorHistoricalWriteOutcomeFacts {
                    cleanup_required,
                    ..ConnectorHistoricalWriteOutcomeFacts::default()
                },
                ConnectorHistoricalWriteProof::try_new(Bytes::from_static(
                    b"full-drive-writer-proof",
                ))?,
            )?;
            self.issued
                .lock()
                .expect("issued observation lock")
                .push(observation.digest());
            Ok(observation)
        }

        fn cleanup(
            &self,
            request: ConnectorHistoricalWriteCleanupRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>
        {
            self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
            if !self
                .issued
                .lock()
                .expect("issued observation lock")
                .contains(&request.observation.digest())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "cleanup observation was not issued by this writer facet",
                ));
            }
            *self.cleanup_binding.lock().expect("cleanup binding lock") =
                Some((request.descriptor_digest, request.observation.digest()));
            if self.plan == RecoveryWriterPlan::StagedCleanupForeignDirectReceipt {
                return Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: ConnectorHistoricalWriteCleanupReceipt {
                        descriptor_digest: [0; 32],
                        observation_digest: [0; 32],
                    },
                    finalization: ExternalMutationFinalization::Complete,
                });
            }
            Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "injected lost writer cleanup response",
                ),
                evidence: ExternalMutationEvidence::try_new(
                    1,
                    ConnectorInstanceDescriptor {
                        provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                        instance_id: self.key.instance_id.clone(),
                    },
                    self.key.incarnation,
                    ConnectorMutationOperationId::from_bytes(request.operation_id.to_bytes()),
                    "historical-write-cleanup",
                    Bytes::from_static(b"full-drive-writer-cleanup-evidence"),
                )?,
            })
        }

        fn reconcile_cleanup(
            &self,
            _operation_id: ConnectorWriteOperationId,
            _evidence: ExternalMutationEvidence,
            _context: ConnectorRequestContext,
        ) -> Result<ExternalMutationOutcome<ConnectorHistoricalWriteCleanupReceipt>, ConnectorError>
        {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            let (descriptor_digest, observation_digest) = self
                .cleanup_binding
                .lock()
                .expect("cleanup binding lock")
                .expect("cleanup binding before reconciliation");
            let (descriptor_digest, observation_digest) =
                if self.plan == RecoveryWriterPlan::StagedCleanupForeignReconcileReceipt {
                    ([0; 32], [0; 32])
                } else {
                    (descriptor_digest, observation_digest)
                };
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: ConnectorHistoricalWriteCleanupReceipt {
                    descriptor_digest,
                    observation_digest,
                },
                finalization: ExternalMutationFinalization::Complete,
            })
        }
    }

    struct RecoveryWriteResolver {
        facet: Arc<RecoveryWriteFacet>,
    }

    impl HistoricalWriteRecoveryResolver for RecoveryWriteResolver {
        fn resolve(
            &self,
            instance_id: &ConnectorInstanceId,
        ) -> Result<HistoricalWriteRecoveryHandle, ConnectorError> {
            if instance_id != &self.facet.key.instance_id {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "historical writer instance drifted",
                ));
            }
            Ok(HistoricalWriteRecoveryHandle::new(
                "iceberg".to_string(),
                self.facet.clone(),
            ))
        }
    }

    fn test_fence(operation_id: Uuid) -> ConnectorCtasPublicationFence {
        ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster").unwrap(),
            ConnectorExternalFenceGeneration::try_new(2, 3, 4).unwrap(),
            ConnectorCtasOperationId::try_from_bytes(*operation_id.as_bytes()).unwrap(),
            ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse("rest").unwrap(),
                namespace: "db".into(),
                table: "t".into(),
            },
        )
        .unwrap()
    }

    fn test_saga(stage: Uuid, abort: Uuid) -> crate::dml::model::CtasSagaRecord {
        crate::dml::model::CtasSagaRecord {
            phase: CtasSagaPhase::PrepareUnknown,
            prepare_operation_id: stage,
            write_operation_id: Uuid::now_v7(),
            publish_operation_id: Uuid::now_v7(),
            abort_staging_operation_id: abort,
            create_policy: crate::dml::model::CTAS_CREATE_POLICY_FAIL_IF_EXISTS.to_string(),
            provider_id: Some("iceberg".to_string()),
            connector_instance_id: Some("rest".to_string()),
            connector_incarnation: Some(hex::encode([9_u8; 16])),
            source_plan_digest: None,
            source_schema_digest: None,
            source_execution_identity: None,
            write_cohort_id: None,
            staged_handle_digest: None,
            write_cohort_set_digest: None,
            aggregate_write_digest: None,
            prepare_fact: None,
            write_fact: None,
            publish_fact: None,
            abort_staging_fact: None,
            next_action: StatementNextAction::ManualInspect,
        }
    }

    fn recovery_drive_harness(
        disposition: ConnectorHistoricalCtasDisposition,
    ) -> (
        Arc<RecoveryDriveJournal>,
        Arc<RecoveryDriveEngine>,
        ActiveDmlOperation,
    ) {
        let operation_id = DmlOperationId::new_v7();
        let attempt = Uuid::now_v7();
        let operation = StoredOperation {
            schema_version: DML_OPERATION_SCHEMA_VERSION,
            operation_id,
            revision: 1,
            last_mutation_id: Uuid::now_v7(),
            operation_kind: OperationKind::CreateTableAsSelect,
            operation_subkind: None,
            target: OperationTarget {
                catalog: "rest".to_string(),
                namespace: "db".to_string(),
                table: "t".to_string(),
                ref_name: None,
            },
            state: OperationState::CommitUnknown,
            attempt_id: "ctas-recovery".to_string(),
            base_snapshot_id: None,
            base_snapshot_map: BTreeMap::new(),
            staged_artifacts: Vec::new(),
            payload: OperationPayload::CtasSaga(test_saga(Uuid::now_v7(), Uuid::now_v7())),
            coordination_provenance: None,
            recovery_due_at_ms: Some(1),
            created_at_ms: 1,
            updated_at_ms: 1,
            finished_at_ms: None,
        };
        let recovery = DmlCtasRecoveryRecord {
            codec_version: crate::dml::model::DML_CTAS_RECOVERY_CODEC_VERSION,
            capability_version: 1,
            recovery_attempt_id: attempt,
            recovery_cycle: 1,
            catalog_fence_history: Vec::new(),
            catalog_fence: None,
            staged_target_digest: Some(hex::encode([4_u8; 32])),
            staged_locator: None,
            staged_locator_digest: None,
            staged_proof_digest: None,
            staged_proof: None,
            dispatch_checkpoints: Vec::new(),
            historical_observations: Vec::new(),
            child_supersessions: Vec::new(),
            cleanup_retention: DmlCtasCleanupRetention::NotRequired,
            cleanup_receipt: None,
            next_action: StatementNextAction::Reconcile,
            updated_at_ms: 1,
        };
        let journal = Arc::new(RecoveryDriveJournal {
            operation: Mutex::new(operation.clone()),
            recovery: Mutex::new(recovery),
            write_recovery: Mutex::new(None),
        });
        let proposal = DmlExternalFenceProposal::testing(
            operation_id,
            "cluster",
            attempt,
            crate::dml::model::DmlExternalFenceGeneration {
                control_plane_incarnation: 1,
                resource_epoch: 1,
                fence_generation: 1,
            },
        )
        .expect("testing fence");
        let active = ActiveDmlOperation::testing_fenced(
            journal.clone(),
            operation,
            proposal,
            Arc::new(AlwaysCurrent),
        );
        (journal, RecoveryDriveEngine::new(disposition), active)
    }

    fn configure_staged_writer_recovery(
        journal: &RecoveryDriveJournal,
        active: &mut ActiveDmlOperation,
    ) {
        let mut operation = journal.operation.lock().expect("operation lock");
        let OperationPayload::CtasSaga(saga) = &mut operation.payload else {
            panic!("expected CTAS saga");
        };
        saga.write_cohort_set_digest = Some(hex::encode([6_u8; 32]));
        saga.aggregate_write_digest = Some(hex::encode([7_u8; 32]));
        let saga = saga.clone();
        active.stored.payload = operation.payload.clone();
        drop(operation);

        let mut recovery = journal.recovery.lock().expect("recovery lock");
        let durable_operation_id = active.operation_id();
        let operation_id = durable_operation_id.as_uuid();
        let fence = ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster").expect("cluster"),
            ConnectorExternalFenceGeneration::try_new(1, 1, 1).expect("generation"),
            ConnectorCtasOperationId::try_from_bytes(*operation_id.as_bytes())
                .expect("CTAS operation"),
            ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse("rest").expect("instance"),
                namespace: "db".into(),
                table: "t".into(),
            },
        )
        .expect("catalog fence");
        let request = ConnectorCtasAdvanceFenceRequest::try_new(
            fence.clone(),
            ConnectorCtasActionId::try_from_bytes(*recovery.recovery_attempt_id.as_bytes())
                .expect("fence action"),
            request_context().expect("request context"),
        )
        .expect("fence request");
        let receipt = ConnectorCtasPublicationFenceReceipt::try_new(
            &request,
            Bytes::from_static(b"preexisting-full-drive-fence"),
        )
        .expect("fence receipt");
        recovery.catalog_fence = Some(DmlCtasCatalogFenceRecord {
            generation: crate::dml::model::DmlExternalFenceGeneration {
                control_plane_incarnation: 1,
                resource_epoch: 1,
                fence_generation: 1,
            },
            action_id: recovery.recovery_attempt_id,
            request_digest: hex::encode(request.input_digest),
            dispatch_certainty: DmlCtasDispatchCertainty::PossiblyDispatched,
            dispatched_at_ms: Some(2),
            fence_digest: Some(hex::encode(fence.digest())),
            receipt_digest: Some(hex::encode(receipt.digest())),
            receipt_payload: Some(
                DmlOpaquePayload::try_new(receipt.payload().to_vec()).expect("receipt payload"),
            ),
            established_at_ms: Some(2),
        });
        recovery.dispatch_checkpoints = vec![
            DmlCtasDispatchCheckpointRecord {
                action: DmlCtasActionKind::Stage,
                child_operation_id: saga.prepare_operation_id,
                request_digest: hex::encode([8_u8; 32]),
                dispatch_certainty: DmlCtasDispatchCertainty::PossiblyDispatched,
                dispatched_at_ms: Some(2),
            },
            DmlCtasDispatchCheckpointRecord {
                action: DmlCtasActionKind::Write,
                child_operation_id: saga.write_operation_id,
                request_digest: hex::encode([9_u8; 32]),
                dispatch_certainty: DmlCtasDispatchCertainty::PossiblyDispatched,
                dispatched_at_ms: Some(3),
            },
            DmlCtasDispatchCheckpointRecord {
                action: DmlCtasActionKind::Abort,
                child_operation_id: saga.abort_staging_operation_id,
                request_digest: hex::encode([10_u8; 32]),
                dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
                dispatched_at_ms: None,
            },
        ];
    }

    #[test]
    fn historical_abort_digest_is_stable_and_operation_bound() {
        let operation = Uuid::now_v7();
        let action = Uuid::now_v7();
        let target = [7_u8; 32];
        let mut first = Sha256::new();
        first.update(HISTORICAL_ABORT_DOMAIN);
        first.update(operation.as_bytes());
        first.update(action.as_bytes());
        first.update(target);
        let first = first.finalize();
        let mut replay = Sha256::new();
        replay.update(HISTORICAL_ABORT_DOMAIN);
        replay.update(operation.as_bytes());
        replay.update(action.as_bytes());
        replay.update(target);
        assert_eq!(first.as_slice(), replay.finalize().as_slice());
    }

    #[test]
    fn descriptor_uses_only_the_latest_checkpoint_per_action() {
        let operation = Uuid::now_v7();
        let old_stage = Uuid::now_v7();
        let current_stage = Uuid::now_v7();
        let abort = Uuid::now_v7();
        let fence_action = Uuid::now_v7();
        let fence = test_fence(operation);
        let recovery = DmlCtasRecoveryRecord {
            codec_version: crate::dml::model::DML_CTAS_RECOVERY_CODEC_VERSION,
            capability_version: 1,
            recovery_attempt_id: fence_action,
            recovery_cycle: 2,
            catalog_fence_history: Vec::new(),
            catalog_fence: Some(DmlCtasCatalogFenceRecord {
                generation: crate::dml::model::DmlExternalFenceGeneration {
                    control_plane_incarnation: 2,
                    resource_epoch: 3,
                    fence_generation: 4,
                },
                action_id: fence_action,
                request_digest: hex::encode([1_u8; 32]),
                dispatch_certainty: DmlCtasDispatchCertainty::PossiblyDispatched,
                dispatched_at_ms: Some(1),
                fence_digest: Some(hex::encode(fence.digest())),
                receipt_digest: Some(hex::encode([2_u8; 32])),
                receipt_payload: Some(DmlOpaquePayload::try_new(vec![3]).unwrap()),
                established_at_ms: Some(2),
            }),
            staged_target_digest: Some(hex::encode([4_u8; 32])),
            staged_locator: None,
            staged_locator_digest: None,
            staged_proof_digest: None,
            staged_proof: None,
            dispatch_checkpoints: vec![
                DmlCtasDispatchCheckpointRecord {
                    action: DmlCtasActionKind::Stage,
                    child_operation_id: old_stage,
                    request_digest: hex::encode([5_u8; 32]),
                    dispatch_certainty: DmlCtasDispatchCertainty::PossiblyDispatched,
                    dispatched_at_ms: Some(3),
                },
                DmlCtasDispatchCheckpointRecord {
                    action: DmlCtasActionKind::Stage,
                    child_operation_id: current_stage,
                    request_digest: hex::encode([6_u8; 32]),
                    dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
                    dispatched_at_ms: None,
                },
                DmlCtasDispatchCheckpointRecord {
                    action: DmlCtasActionKind::Abort,
                    child_operation_id: abort,
                    request_digest: hex::encode([7_u8; 32]),
                    dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
                    dispatched_at_ms: None,
                },
            ],
            historical_observations: Vec::new(),
            child_supersessions: Vec::new(),
            cleanup_retention: DmlCtasCleanupRetention::NotRequired,
            cleanup_receipt: None,
            next_action: StatementNextAction::ManualInspect,
            updated_at_ms: 3,
        };

        let descriptor = historical_descriptor(&recovery, &test_saga(current_stage, abort), fence)
            .expect("descriptor");
        let stages = descriptor
            .checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.action == ConnectorHistoricalCtasAction::Stage)
            .collect::<Vec<_>>();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].action_id.to_bytes(), *current_stage.as_bytes());
        assert!(descriptor.checkpoints.iter().any(|checkpoint| {
            checkpoint.action == ConnectorHistoricalCtasAction::Abort
                && checkpoint.action_id.to_bytes() == *abort.as_bytes()
        }));
    }

    #[test]
    fn catalog_cleanup_requires_a_conclusive_writer_verdict() {
        use crate::dml::model::{DmlHistoricalWriteDisposition, DmlHistoricalWriteResultRecord};

        let stage = Uuid::now_v7();
        let mut saga = test_saga(stage, Uuid::now_v7());
        let possible = DmlCtasDispatchCheckpointRecord {
            action: DmlCtasActionKind::Write,
            child_operation_id: saga.write_operation_id,
            request_digest: hex::encode([8_u8; 32]),
            dispatch_certainty: DmlCtasDispatchCertainty::PossiblyDispatched,
            dispatched_at_ms: Some(1),
        };
        let not_dispatched = DmlCtasDispatchCheckpointRecord {
            dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
            dispatched_at_ms: None,
            ..possible.clone()
        };
        let result = |disposition, cleanup| DmlHistoricalWriteResultRecord {
            disposition,
            observation_digest: hex::encode([9_u8; 32]),
            evidence_payload: None,
            proof_payload: None,
            continuation_payload: None,
            cleanup,
            failure: None,
            observed_at_ms: 2,
        };

        assert_eq!(
            durable_write_cleanup_decision(&saga, Some(&not_dispatched), None),
            WriteCleanupDecision::Authorized
        );
        assert_eq!(
            durable_write_cleanup_decision(&saga, Some(&possible), None),
            WriteCleanupDecision::InspectHistorically
        );

        for disposition in [
            DmlHistoricalWriteDisposition::NotApplied,
            DmlHistoricalWriteDisposition::NotDispatched,
        ] {
            assert_eq!(
                durable_write_cleanup_decision(
                    &saga,
                    Some(&possible),
                    Some(&result(disposition, DmlHistoricalCleanupState::NotRequired)),
                ),
                WriteCleanupDecision::Authorized
            );
        }
        assert_eq!(
            durable_write_cleanup_decision(
                &saga,
                Some(&possible),
                Some(&result(
                    DmlHistoricalWriteDisposition::Staged,
                    DmlHistoricalCleanupState::Completed,
                )),
            ),
            WriteCleanupDecision::Authorized
        );
        assert_eq!(
            durable_write_cleanup_decision(
                &saga,
                Some(&possible),
                Some(&result(
                    DmlHistoricalWriteDisposition::Staged,
                    DmlHistoricalCleanupState::Pending,
                )),
            ),
            WriteCleanupDecision::InspectHistorically
        );
        for disposition in [
            DmlHistoricalWriteDisposition::Applied,
            DmlHistoricalWriteDisposition::Conflict,
            DmlHistoricalWriteDisposition::Ambiguous,
            DmlHistoricalWriteDisposition::Unsupported,
        ] {
            assert_eq!(
                durable_write_cleanup_decision(
                    &saga,
                    Some(&possible),
                    Some(&result(disposition, DmlHistoricalCleanupState::NotRequired)),
                ),
                WriteCleanupDecision::Denied
            );
        }

        saga.write_fact = Some(DurableExternalFact {
            outcome: ExternalFactOutcome::KnownUncommitted,
            receipt: None,
            evidence: None,
            finalization_failure: None,
            failure: None,
        });
        assert_eq!(
            durable_write_cleanup_decision(&saga, Some(&possible), None),
            WriteCleanupDecision::Authorized
        );
    }

    #[test]
    fn full_drive_publishes_or_parks_ambiguity_without_catalog_cleanup() {
        for disposition in [
            ConnectorHistoricalCtasDisposition::Published,
            ConnectorHistoricalCtasDisposition::Ambiguous,
        ] {
            let (journal, engine, mut active) = recovery_drive_harness(disposition);
            let progress = CtasRecoveryProfile::new(engine.clone(), None)
                .drive(&mut active, 10)
                .expect("full recovery drive");
            assert_eq!(engine.inspect_calls.load(Ordering::SeqCst), 1);
            assert_eq!(engine.cleanup_calls.load(Ordering::SeqCst), 0);

            let operation = journal.operation.lock().expect("operation lock").clone();
            let recovery = journal.recovery.lock().expect("recovery lock").clone();
            match disposition {
                ConnectorHistoricalCtasDisposition::Published => {
                    assert_eq!(progress, CtasRecoveryProgress::Published);
                    assert_eq!(operation.state, OperationState::Finalized);
                    assert_eq!(operation.recovery_due_at_ms, None);
                    assert_eq!(
                        recovery.cleanup_retention,
                        DmlCtasCleanupRetention::NotRequired
                    );
                    assert_eq!(recovery.next_action, StatementNextAction::None);
                }
                ConnectorHistoricalCtasDisposition::Ambiguous => {
                    assert_eq!(progress, CtasRecoveryProgress::Unresolved);
                    assert_eq!(operation.state, OperationState::CommitUnknown);
                    assert!(operation.recovery_due_at_ms.is_some());
                    assert_eq!(recovery.next_action, StatementNextAction::ManualInspect);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn full_drive_reconciles_lost_writer_cleanup_before_catalog_cleanup() {
        let (journal, engine, mut active) =
            recovery_drive_harness(ConnectorHistoricalCtasDisposition::Staged);
        configure_staged_writer_recovery(&journal, &mut active);
        let writer = RecoveryWriteFacet::new(RecoveryWriterPlan::StagedCleanupLost);
        let resolver = Arc::new(RecoveryWriteResolver {
            facet: writer.clone(),
        });

        let progress = CtasRecoveryProfile::new(engine.clone(), Some(resolver))
            .drive(&mut active, 20)
            .expect("staged full recovery drive");

        assert_eq!(progress, CtasRecoveryProgress::CleanupCompleted);
        assert_eq!(writer.cleanup_calls.load(Ordering::SeqCst), 1);
        assert_eq!(writer.reconcile_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.cleanup_calls.load(Ordering::SeqCst), 1);
        let write_recovery = journal
            .write_recovery
            .lock()
            .expect("write recovery lock")
            .clone()
            .expect("durable historical writer result");
        assert_eq!(
            write_recovery.result.expect("writer result").cleanup,
            DmlHistoricalCleanupState::Completed
        );
        assert_eq!(
            journal
                .recovery
                .lock()
                .expect("recovery lock")
                .cleanup_retention,
            DmlCtasCleanupRetention::Completed
        );
    }

    #[test]
    fn full_drive_rejects_foreign_writer_cleanup_receipts_before_catalog_cleanup() {
        for plan in [
            RecoveryWriterPlan::StagedCleanupForeignDirectReceipt,
            RecoveryWriterPlan::StagedCleanupForeignReconcileReceipt,
        ] {
            let (journal, engine, mut active) =
                recovery_drive_harness(ConnectorHistoricalCtasDisposition::Staged);
            configure_staged_writer_recovery(&journal, &mut active);
            let writer = RecoveryWriteFacet::new(plan);
            let resolver = Arc::new(RecoveryWriteResolver {
                facet: writer.clone(),
            });

            let progress = CtasRecoveryProfile::new(engine.clone(), Some(resolver))
                .drive(&mut active, 25)
                .expect("foreign writer cleanup receipt must park recovery");

            assert_eq!(progress, CtasRecoveryProgress::Unresolved);
            assert_eq!(writer.cleanup_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                writer.reconcile_calls.load(Ordering::SeqCst),
                usize::from(plan == RecoveryWriterPlan::StagedCleanupForeignReconcileReceipt)
            );
            assert_eq!(engine.cleanup_calls.load(Ordering::SeqCst), 0);
            let write_recovery = journal
                .write_recovery
                .lock()
                .expect("write recovery lock")
                .clone()
                .expect("durable historical writer result");
            let result = write_recovery.result.expect("writer result");
            assert_eq!(result.cleanup, DmlHistoricalCleanupState::Pending);
            assert_eq!(
                result.failure.expect("typed corrupt receipt").kind,
                ConnectorWriteFailureKind::CorruptData
            );
            assert!(
                journal
                    .operation
                    .lock()
                    .expect("operation lock")
                    .recovery_due_at_ms
                    .is_some()
            );
        }
    }

    #[test]
    fn full_drive_never_cleans_catalog_staging_for_ambiguous_writer_truth() {
        let (journal, engine, mut active) =
            recovery_drive_harness(ConnectorHistoricalCtasDisposition::Staged);
        configure_staged_writer_recovery(&journal, &mut active);
        let writer = RecoveryWriteFacet::new(RecoveryWriterPlan::Ambiguous);
        let resolver = Arc::new(RecoveryWriteResolver {
            facet: writer.clone(),
        });

        let progress = CtasRecoveryProfile::new(engine.clone(), Some(resolver))
            .drive(&mut active, 30)
            .expect("ambiguous writer full recovery drive");

        assert_eq!(progress, CtasRecoveryProgress::Unresolved);
        assert_eq!(writer.cleanup_calls.load(Ordering::SeqCst), 0);
        assert_eq!(writer.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.cleanup_calls.load(Ordering::SeqCst), 0);
        assert!(
            journal
                .operation
                .lock()
                .expect("operation lock")
                .recovery_due_at_ms
                .is_some()
        );
    }

    #[test]
    fn active_ctas_recovery_keeps_due_while_the_top_level_saga_is_unfinished() {
        let (journal, _, mut active) =
            recovery_drive_harness(ConnectorHistoricalCtasDisposition::Published);
        let mut recovery = journal.recovery.lock().expect("recovery lock").clone();
        recovery.next_action = StatementNextAction::None;

        active
            .record_ctas_recovery(recovery, None)
            .expect("unfinished CTAS keeps the foreground recovery due");

        assert!(active.stored.recovery_due_at_ms.is_some());
        assert!(
            journal
                .operation
                .lock()
                .expect("operation lock")
                .recovery_due_at_ms
                .is_some()
        );
    }

    #[test]
    fn superseded_holder_cannot_persist_a_late_catalog_observation() {
        let (journal, _, active) =
            recovery_drive_harness(ConnectorHistoricalCtasDisposition::Ambiguous);
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let old_engine = RecoveryDriveEngine::blocked(
            ConnectorHistoricalCtasDisposition::Ambiguous,
            entered.clone(),
            release.clone(),
        );
        let old = std::thread::spawn(move || {
            let mut active = active;
            CtasRecoveryProfile::new(old_engine, None).drive(&mut active, 20)
        });
        entered.wait();

        let next_attempt = Uuid::now_v7();
        let operation = journal.operation.lock().expect("operation lock").clone();
        let proposal = DmlExternalFenceProposal::testing(
            operation.operation_id,
            "cluster",
            next_attempt,
            crate::dml::model::DmlExternalFenceGeneration {
                control_plane_incarnation: 1,
                resource_epoch: 2,
                fence_generation: 1,
            },
        )
        .expect("higher testing fence");
        let mut current = ActiveDmlOperation::testing_fenced(
            journal.clone(),
            operation,
            proposal,
            Arc::new(AlwaysCurrent),
        );
        let current_engine =
            RecoveryDriveEngine::new(ConnectorHistoricalCtasDisposition::Published);
        assert_eq!(
            CtasRecoveryProfile::new(current_engine, None)
                .drive(&mut current, 21)
                .expect("current holder recovery"),
            CtasRecoveryProgress::Published
        );
        let revision_after_current = journal.operation.lock().expect("operation lock").revision;

        release.wait();
        let error = old
            .join()
            .expect("old holder thread")
            .expect_err("old holder must lose its late writeback");
        assert!(matches!(
            error.kind(),
            DmlErrorKind::JournalUnresolved | DmlErrorKind::JournalCorruption
        ));
        let operation = journal.operation.lock().expect("operation lock").clone();
        assert_eq!(operation.revision, revision_after_current);
        assert_eq!(operation.state, OperationState::Finalized);
        assert_eq!(
            journal
                .recovery
                .lock()
                .expect("recovery lock")
                .recovery_attempt_id,
            next_attempt
        );
    }
}
