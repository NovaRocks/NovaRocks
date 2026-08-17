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

//! Frontend-owned durable CTAS application saga.
//!
//! Core retains the admitted source artifact and provider-private staged
//! handles. The frontend owns only bounded neutral facts, stable child
//! operation IDs and the durable ordering barriers around every external
//! effect.

pub(crate) mod recovery;

use novarocks::query_execution::dml::ctas::{
    CtasCommand, CtasEngine, CtasFailure, CtasFailureKind, CtasTargetFacts,
    CtasTargetPreflightOutcome, CtasWriteOutcome, PrepareCtasSourceRequest, PreparedCtasSource,
    PreparedCtasTarget, PreparedCtasWrite,
};
use novarocks::query_execution::request_context::RequestContext;
use novarocks_protocol::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorCtasAbortDisposition, ConnectorCtasActionId, ConnectorCtasConflictKind,
    ConnectorCtasFailure, ConnectorCtasOperationId, ConnectorCtasPublicationFence,
    ConnectorCtasPublicationProof, ConnectorCtasPublicationReceipt,
    ConnectorCtasPublishDisposition, ConnectorCtasStagedLocator, ConnectorHistoricalCtasAction,
    ConnectorHistoricalCtasCheckpoint, ConnectorHistoricalCtasDescriptor,
    ConnectorHistoricalCtasDispatchState, ConnectorHistoricalCtasDisposition,
    ConnectorHistoricalCtasObservation, ConnectorMutationFailure, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorWriteOperationCompletion, ConnectorWriteOperationId,
    ConnectorWriteTargetRef, CreatePolicy, ExternalMutationEvidence,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dml::coordination::ActiveDmlOperation;
use crate::dml::error::{DmlError, DmlErrorKind};
use crate::dml::model::{
    CTAS_CREATE_POLICY_FAIL_IF_EXISTS, CTAS_CREATE_POLICY_NO_OP_IF_EXISTS,
    CreateStatementOperationRequest, CtasSagaPhase, CtasSagaRecord, DML_CTAS_FACT_ENCODED_LIMIT,
    DML_CTAS_RECOVERY_CODEC_VERSION, DmlCtasActionKind, DmlCtasCatalogFenceRecord,
    DmlCtasCleanupRetention, DmlCtasConflictKind, DmlCtasDispatchCertainty,
    DmlCtasDispatchCheckpointRecord, DmlCtasHistoricalDisposition,
    DmlCtasHistoricalObservationRecord, DmlCtasRecoveryRecord, DmlOpaquePayload, DmlOperationId,
    DurableExternalFact, ExternalFactOutcome, OperationKind, OperationPayload, OperationState,
    OperationTarget, StatementNextAction, StoredOperation,
};
use crate::dml::service::DmlService;

const DURABLE_CTAS_FACT_VERSION: u8 = 1;
const DURABLE_FAILURE_PREFIX_BYTES: usize = 2 * 1024;

#[derive(Serialize)]
struct DurableCtasWriteCompletionV1 {
    version: u8,
    instance_id: String,
    incarnation: String,
    operation_id: String,
    cohort_id: String,
    cohort_set_digest: String,
    aggregate_digest: String,
}

#[derive(Serialize)]
struct DurableCtasFailureV1<'a> {
    version: u8,
    kind: &'static str,
    message_prefix: &'a str,
    message_truncated: bool,
    original_message_bytes: usize,
    original_message_sha256: String,
}

impl DmlService {
    /// Recognize and execute CTAS through the frontend durable saga owner.
    ///
    /// `Ok(None)` is reserved for non-CTAS SQL. Once classified, the core
    /// fallback is never called, including on failures.
    pub fn try_execute_ctas(
        &self,
        engine: &dyn CtasEngine,
        sql: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<Option<()>, DmlError> {
        let Some(command) = engine.classify_ctas(sql).map_err(DmlError::executor)? else {
            return Ok(None);
        };
        let session = context.session();
        let operation_id = DmlOperationId::new_v7();
        let prepare_operation_id = Uuid::now_v7();
        let write_operation_id = Uuid::now_v7();
        let publish_operation_id = Uuid::now_v7();
        let abort_staging_operation_id = Uuid::now_v7();
        let policy = if command.if_not_exists {
            CreatePolicy::NoOpIfExists
        } else {
            CreatePolicy::FailIfExists
        };
        let initial = CtasSagaRecord {
            phase: CtasSagaPhase::PreparingSource,
            prepare_operation_id,
            write_operation_id,
            publish_operation_id,
            abort_staging_operation_id,
            create_policy: policy_name(policy).to_string(),
            provider_id: None,
            connector_instance_id: None,
            connector_incarnation: None,
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
            next_action: StatementNextAction::None,
        };
        let mut active = self
            .begin_statement_operation(CreateStatementOperationRequest {
                operation_id,
                mutation_id: Uuid::now_v7(),
                operation_kind: OperationKind::CreateTableAsSelect,
                target: syntactic_target(
                    &command.target_parts,
                    session.current_catalog(),
                    session.current_database(),
                ),
                attempt_id: operation_id.to_string(),
                payload: OperationPayload::CtasSaga(initial),
                created_at_ms: crate::dml::now_unix_millis(),
            })
            .map_err(|error| journal_error(error, operation_id))?;

        let result = execute_ctas_operation(
            engine,
            context,
            query_options,
            command,
            prepare_operation_id,
            policy,
            &mut active,
        );
        let _ = active.release();
        result.map(|()| Some(()))
    }
}

fn execute_ctas_operation(
    engine: &dyn CtasEngine,
    context: &RequestContext,
    query_options: Option<&QueryOptions>,
    command: CtasCommand,
    _prepare_operation_id: Uuid,
    policy: CreatePolicy,
    active: &mut ActiveDmlOperation,
) -> Result<(), DmlError> {
    let session = context.session();
    active.check_before_dispatch()?;
    let preflight = match engine.preflight_ctas_target(
        &command,
        session.current_catalog(),
        session.current_database(),
    ) {
        Ok(CtasTargetPreflightOutcome::ExistsNoOp) => {
            let mut record = ctas_record(&active.stored)?;
            record.phase = CtasSagaPhase::NoOp;
            record.next_action = StatementNextAction::None;
            active.mutate_statement(
                OperationState::Committing,
                OperationPayload::CtasSaga(record.clone()),
                None,
            )?;
            active.mutate_statement(
                OperationState::Committed,
                OperationPayload::CtasSaga(record.clone()),
                None,
            )?;
            active.mutate_statement(
                OperationState::Finalized,
                OperationPayload::CtasSaga(record),
                None,
            )?;
            return Ok(());
        }
        Ok(CtasTargetPreflightOutcome::Ready(preflight)) => preflight,
        Err(failure) => return finish_source_failure(active, active.stored.clone(), failure),
    };
    validate_preflight_facts(&active.stored, &preflight.facts)?;

    let proposal = active.external_fence()?;
    let table = ConnectorTableIdentity {
        instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(
            &preflight.facts.instance_id,
        )
        .map_err(DmlError::executor)?,
        namespace: preflight.facts.target_namespace.clone().into(),
        table: preflight.facts.target_table.clone().into(),
    };
    let generic_fence = proposal
        .seal(
            ConnectorWriteOperationId::from_bytes(*active.operation_id().as_uuid().as_bytes()),
            table.clone(),
            ConnectorWriteTargetRef::main(),
        )
        .map_err(DmlError::executor)?;
    let fence = ConnectorCtasPublicationFence::try_new(
        generic_fence.cluster(),
        generic_fence.generation(),
        ConnectorCtasOperationId::try_from_bytes(*active.operation_id().as_uuid().as_bytes())
            .map_err(DmlError::executor)?,
        table,
    )
    .map_err(DmlError::executor)?;
    let fence_action_id =
        ConnectorCtasActionId::try_from_bytes(*proposal.coordination_attempt_id().as_bytes())
            .map_err(DmlError::executor)?;
    let fence_action = match engine.prepare_ctas_fence_advance(
        preflight.handle.as_ref(),
        fence.clone(),
        fence_action_id,
    ) {
        Ok(action) => action,
        Err(failure) => {
            return finish_source_failure(active, active.stored.clone(), failure);
        }
    };
    let mut recovery = new_recovery_record(
        &preflight.facts,
        proposal.coordination_attempt_id(),
        proposal.generation(),
        fence_action_id,
        fence_action.input_digest,
    )?;
    record_recovery(active, &recovery)?;
    mark_fence_dispatched(&mut recovery);
    record_recovery(active, &recovery)?;
    active.check_before_dispatch()?;
    let fence_receipt = match engine.advance_ctas_fence(fence_action.handle.as_ref()) {
        Ok(receipt) => receipt,
        Err(failure) if catalog_failure_is_terminal(&failure) => {
            return finish_catalog_terminal_failure(active, recovery, FactSlot::Prepare, failure);
        }
        Err(failure) => {
            return finish_catalog_unknown(
                active,
                recovery,
                CtasSagaPhase::PrepareUnknown,
                FactSlot::Prepare,
                failure,
            );
        }
    };
    recovery
        .catalog_fence
        .as_mut()
        .expect("fence record")
        .fence_digest = Some(hex::encode(fence.digest()));
    recovery
        .catalog_fence
        .as_mut()
        .expect("fence record")
        .receipt_digest = Some(hex::encode(fence_receipt.digest()));
    recovery
        .catalog_fence
        .as_mut()
        .expect("fence record")
        .receipt_payload = Some(
        DmlOpaquePayload::try_new(fence_receipt.payload().to_vec())
            .map_err(DmlError::journal_corruption)?,
    );
    recovery
        .catalog_fence
        .as_mut()
        .expect("fence record")
        .established_at_ms = Some(crate::dml::now_unix_millis());
    recovery.staged_target_digest = Some(hex::encode(fence.digest()));
    recovery.next_action = StatementNextAction::None;
    record_recovery(active, &recovery)?;

    active.check_before_dispatch()?;
    let source = match engine.prepare_ctas_source(
        preflight.handle.as_ref(),
        PrepareCtasSourceRequest {
            command,
            current_catalog: session.current_catalog().map(ToOwned::to_owned),
            current_database: session.current_database().to_string(),
            query_options: query_options.cloned(),
            execution: context.execution().clone(),
        },
    ) {
        Ok(source) => source,
        Err(failure) => return finish_source_failure(active, active.stored.clone(), failure),
    };
    validate_source_facts(
        &active.stored,
        &source,
        session.current_catalog(),
        session.current_database(),
    )?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::PreparingStagedTable;
    saga.provider_id = Some(preflight.facts.provider_id.clone());
    saga.connector_instance_id = Some(preflight.facts.instance_id.clone());
    saga.connector_incarnation = Some(hex::encode(preflight.facts.incarnation));
    saga.source_plan_digest = Some(hex::encode(source.facts.plan_digest));
    saga.source_schema_digest = Some(hex::encode(source.facts.schema_digest));
    saga.source_execution_identity = Some(hex::encode(source.facts.execution_identity));
    active.mutate_statement(
        OperationState::Preparing,
        OperationPayload::CtasSaga(saga),
        None,
    )?;

    let stage_uuid = ctas_record(&active.stored)?.prepare_operation_id;
    let stage_action_id = connector_action_id(stage_uuid)?;
    let stage_action = match engine.prepare_ctas_target(
        source.handle.as_ref(),
        fence.clone(),
        stage_action_id,
        policy,
    ) {
        Ok(action) => action,
        Err(failure) => {
            return finish_local_catalog_preparation_failure(
                active,
                recovery,
                FactSlot::Prepare,
                failure,
            );
        }
    };
    append_checkpoint(
        &mut recovery,
        DmlCtasActionKind::Stage,
        stage_uuid,
        stage_action.input_digest,
    );
    record_recovery(active, &recovery)?;
    mark_checkpoint_dispatched(&mut recovery, DmlCtasActionKind::Stage, stage_uuid);
    record_recovery(active, &recovery)?;
    active.check_before_dispatch()?;
    let stage = match engine.stage_ctas_target(stage_action.handle.as_ref()) {
        Ok(stage) => stage,
        Err(failure) if catalog_failure_is_terminal(&failure) => {
            return finish_catalog_terminal_failure(active, recovery, FactSlot::Prepare, failure);
        }
        Err(failure) => {
            return finish_catalog_unknown(
                active,
                recovery,
                CtasSagaPhase::PrepareUnknown,
                FactSlot::Prepare,
                failure,
            );
        }
    };
    validate_target_facts_v2(
        &active.stored,
        &preflight.facts,
        &fence,
        &stage.target.facts,
        &stage.locator,
        stage_uuid,
    )?;
    recovery.staged_locator = Some(
        DmlOpaquePayload::try_new(
            stage
                .locator
                .try_to_wire_v1()
                .map_err(DmlError::executor)?
                .to_vec(),
        )
        .map_err(DmlError::journal_corruption)?,
    );
    recovery.staged_locator_digest = Some(hex::encode(stage.locator.digest()));
    recovery.staged_proof_digest = Some(hex::encode(stage.proof.digest()));
    recovery.staged_proof = Some(
        DmlOpaquePayload::try_new(
            stage
                .proof
                .try_to_wire_v1()
                .map_err(DmlError::executor)?
                .to_vec(),
        )
        .map_err(DmlError::journal_corruption)?,
    );
    recovery.cleanup_retention = DmlCtasCleanupRetention::Pending;
    recovery.next_action = StatementNextAction::AbortStaging;
    record_recovery(active, &recovery)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::Staged;
    saga.staged_handle_digest = Some(hex::encode(stage.target.facts.locator_digest));
    saga.prepare_fact = Some(publication_fact(
        ExternalFactOutcome::KnownCommitted,
        &stage.receipt,
    ));
    saga.next_action = StatementNextAction::None;
    active.mutate_statement(
        OperationState::Writing,
        OperationPayload::CtasSaga(saga),
        None,
    )?;
    let historical_context = novarocks::connector::connector_request_context_for_execution(
        query_options,
        context.execution(),
    )
    .map_err(DmlError::executor)?;
    execute_foreground_write(
        engine,
        active,
        recovery,
        source,
        stage.target,
        ForegroundStageAuthority {
            locator: stage.locator,
            proof: stage.proof,
            create_policy: policy,
            historical_context,
        },
    )
}

struct ForegroundStageAuthority {
    locator: ConnectorCtasStagedLocator,
    proof: ConnectorCtasPublicationProof,
    create_policy: CreatePolicy,
    historical_context: ConnectorRequestContext,
}

fn execute_foreground_write(
    engine: &dyn CtasEngine,
    active: &mut ActiveDmlOperation,
    mut recovery: DmlCtasRecoveryRecord,
    source: PreparedCtasSource,
    target: PreparedCtasTarget,
    authority: ForegroundStageAuthority,
) -> Result<(), DmlError> {
    let write_uuid = ctas_record(&active.stored)?.write_operation_id;
    let prepared = match engine.prepare_ctas_write(
        source.handle.as_ref(),
        target.handle.as_ref(),
        ConnectorWriteOperationId::from_bytes(*write_uuid.as_bytes()),
    ) {
        Ok(prepared) => prepared,
        Err(failure) => {
            return abort_foreground(
                engine,
                active,
                recovery,
                &target,
                &authority,
                failure_fact(&failure),
                format_failure("CTAS write preparation failed", &failure),
            );
        }
    };
    let native_bundle = match prepared.handle.native_encoding() {
        Ok(encoding) => match encoding.input() {
            Ok(input) => match crate::native::fragment_encoder::encode_native_fragment_bundle(
                input.encoding_view(),
            ) {
                Ok(bundle) => bundle,
                Err(message) => {
                    let failure = CtasFailure {
                        kind: CtasFailureKind::Internal,
                        message,
                    };
                    return abort_foreground(
                        engine,
                        active,
                        recovery,
                        &target,
                        &authority,
                        failure_fact(&failure),
                        format_failure("CTAS native write assembly failed", &failure),
                    );
                }
            },
            Err(failure) => {
                return abort_foreground(
                    engine,
                    active,
                    recovery,
                    &target,
                    &authority,
                    failure_fact(&failure),
                    format_failure("CTAS native write assembly failed", &failure),
                );
            }
        },
        Err(failure) => {
            return abort_foreground(
                engine,
                active,
                recovery,
                &target,
                &authority,
                failure_fact(&failure),
                format_failure("CTAS native write assembly failed", &failure),
            );
        }
    };
    if let Err(failure) =
        engine.bind_ctas_write_native_bundle(prepared.handle.as_ref(), native_bundle)
    {
        return abort_foreground(
            engine,
            active,
            recovery,
            &target,
            &authority,
            failure_fact(&failure),
            format_failure("CTAS native write assembly failed", &failure),
        );
    }
    validate_prepared_write(&active.stored, &source, &target, &prepared)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.write_cohort_set_digest = Some(hex::encode(prepared.cohort_set_digest));
    active.mutate_statement(
        active.stored.state,
        OperationPayload::CtasSaga(saga),
        active.stored.recovery_due_at_ms,
    )?;
    append_checkpoint(
        &mut recovery,
        DmlCtasActionKind::Write,
        write_uuid,
        prepared.execution_identity,
    );
    record_recovery(active, &recovery)?;
    mark_checkpoint_dispatched(&mut recovery, DmlCtasActionKind::Write, write_uuid);
    record_recovery(active, &recovery)?;
    active.check_before_dispatch()?;
    match engine.execute_ctas_write(prepared.handle.as_ref()) {
        CtasWriteOutcome::Completed {
            completion,
            execution_identity,
            established_fence,
        } => {
            if let Some(established) = established_fence {
                let record = crate::dml::reconcile::external_fence_receipt_record(&established)
                    .map_err(DmlError::journal_corruption)?;
                active.record_external_fence(record, Some(crate::dml::now_unix_millis()))?;
            }
            validate_completion(
                &active.stored,
                &source,
                &target,
                &prepared,
                &completion,
                execution_identity,
            )?;
            let (encoded, cohort) = encode_write_completion(&completion)?;
            let mut saga = ctas_record(&active.stored)?;
            saga.phase = CtasSagaPhase::Publishing;
            saga.write_cohort_id = Some(cohort);
            saga.aggregate_write_digest = Some(hex::encode(completion.aggregate_digest()));
            saga.write_fact = Some(DurableExternalFact {
                outcome: ExternalFactOutcome::KnownCommitted,
                receipt: Some(encoded),
                evidence: None,
                finalization_failure: None,
                failure: None,
            });
            active.mutate_statement(
                OperationState::Committing,
                OperationPayload::CtasSaga(saga),
                None,
            )?;
            publish_foreground(engine, active, recovery, target, authority, completion)
        }
        CtasWriteOutcome::KnownUncommitted { failure } => abort_foreground(
            engine,
            active,
            recovery,
            &target,
            &authority,
            failure_fact(&failure),
            format_failure("CTAS writer is known uncommitted", &failure),
        ),
        CtasWriteOutcome::CommitUnknown {
            failure,
            evidence,
            established_fence,
        } => {
            if let Some(established) = established_fence {
                let record = crate::dml::reconcile::external_fence_receipt_record(&established)
                    .map_err(DmlError::journal_corruption)?;
                active.record_external_fence(record, Some(crate::dml::now_unix_millis()))?;
            }
            let mut saga = ctas_record(&active.stored)?;
            saga.phase = CtasSagaPhase::WriteUnknown;
            saga.write_fact = Some(DurableExternalFact {
                outcome: ExternalFactOutcome::CommitUnknown,
                receipt: None,
                evidence: encode_evidence(&evidence).ok(),
                finalization_failure: None,
                failure: Some(encode_failure(&failure)),
            });
            saga.next_action = StatementNextAction::ManualInspect;
            recovery.next_action = StatementNextAction::ManualInspect;
            record_recovery(active, &recovery)?;
            active.mutate_statement(
                OperationState::CommitUnknown,
                OperationPayload::CtasSaga(saga),
                Some(crate::dml::now_unix_millis()),
            )?;
            Err(unknown_error(
                active.operation_id(),
                "CTAS writer",
                &failure,
            ))
        }
    }
}

fn publish_foreground(
    engine: &dyn CtasEngine,
    active: &mut ActiveDmlOperation,
    mut recovery: DmlCtasRecoveryRecord,
    target: PreparedCtasTarget,
    authority: ForegroundStageAuthority,
    completion: ConnectorWriteOperationCompletion,
) -> Result<(), DmlError> {
    let publish_uuid = ctas_record(&active.stored)?.publish_operation_id;
    let action = match engine.prepare_publish_ctas(
        target.handle.as_ref(),
        connector_action_id(publish_uuid)?,
        completion,
    ) {
        Ok(action) => action,
        Err(failure) => {
            let fact = failure_fact(&failure);
            let cause = format_failure("CTAS publish preparation failed", &failure);
            return abort_foreground(engine, active, recovery, &target, &authority, fact, cause);
        }
    };
    append_checkpoint(
        &mut recovery,
        DmlCtasActionKind::Publish,
        publish_uuid,
        action.input_digest,
    );
    record_recovery(active, &recovery)?;
    mark_checkpoint_dispatched(&mut recovery, DmlCtasActionKind::Publish, publish_uuid);
    record_recovery(active, &recovery)?;
    active.check_before_dispatch()?;
    let result = match engine.publish_ctas(action.handle.as_ref()) {
        Ok(result) => result,
        Err(failure) if catalog_failure_is_terminal(&failure) => {
            let fact = connector_failure_fact(&failure);
            let cause = format!("CTAS publish was rejected: {}", failure.failure());
            return abort_foreground(engine, active, recovery, &target, &authority, fact, cause);
        }
        Err(failure) => {
            return finish_catalog_unknown(
                active,
                recovery,
                CtasSagaPhase::PublishUnknown,
                FactSlot::Publish,
                failure,
            );
        }
    };
    let disposition = match result.disposition {
        ConnectorCtasPublishDisposition::Published => DmlCtasHistoricalDisposition::Published,
        ConnectorCtasPublishDisposition::NoOp => DmlCtasHistoricalDisposition::NoOp,
    };
    recovery
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action: DmlCtasActionKind::Publish,
            child_operation_id: publish_uuid,
            disposition,
            descriptor_digest: hex::encode(target.facts.fence_digest),
            descriptor_locator_digest: Some(hex::encode(target.facts.locator_digest)),
            observation_digest: hex::encode(result.digest()),
            locator_digest: (disposition == DmlCtasHistoricalDisposition::NoOp)
                .then(|| hex::encode(target.facts.locator_digest)),
            proof_digest: Some(hex::encode(result.proof.digest())),
            proof_payload: Some(
                DmlOpaquePayload::try_new(
                    result
                        .proof
                        .try_to_wire_v1()
                        .map_err(DmlError::executor)?
                        .to_vec(),
                )
                .map_err(DmlError::journal_corruption)?,
            ),
            conflict_kind: None,
            failure: None,
            observed_at_ms: crate::dml::now_unix_millis(),
        });
    let mut saga = ctas_record(&active.stored)?;
    saga.publish_fact = Some(publication_fact(
        if result.disposition == ConnectorCtasPublishDisposition::NoOp {
            ExternalFactOutcome::NoOp
        } else {
            ExternalFactOutcome::KnownCommitted
        },
        &result.receipt,
    ));
    saga.phase = if result.disposition == ConnectorCtasPublishDisposition::NoOp {
        CtasSagaPhase::NoOp
    } else {
        CtasSagaPhase::Committed
    };
    saga.next_action = StatementNextAction::None;
    if result.disposition == ConnectorCtasPublishDisposition::NoOp {
        recovery.cleanup_retention = DmlCtasCleanupRetention::Pending;
        recovery.next_action = StatementNextAction::AbortStaging;
    } else {
        recovery.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
        recovery.next_action = StatementNextAction::None;
    }
    record_recovery(active, &recovery)?;
    active.mutate_statement(
        OperationState::Committed,
        OperationPayload::CtasSaga(saga.clone()),
        None,
    )?;
    active.mutate_statement(
        OperationState::Finalized,
        OperationPayload::CtasSaga(saga),
        None,
    )?;
    Ok(())
}

fn inspect_foreground_visibility(
    engine: &dyn CtasEngine,
    active: &mut ActiveDmlOperation,
    recovery: &mut DmlCtasRecoveryRecord,
    authority: &ForegroundStageAuthority,
) -> Result<(ConnectorHistoricalCtasObservation, String), DmlError> {
    let descriptor = foreground_historical_descriptor(recovery, authority)?;
    active.check_before_dispatch()?;
    let observation = match engine
        .inspect_historical_ctas(descriptor.clone(), authority.historical_context.clone())
    {
        Ok(observation) => observation,
        Err(failure) => {
            return match finish_catalog_unknown(
                active,
                recovery.clone(),
                CtasSagaPhase::AbortUnknown,
                FactSlot::Abort,
                failure,
            ) {
                Err(error) => Err(error),
                Ok(()) => unreachable!("unresolved CTAS inspection cannot succeed"),
            };
        }
    };
    let (action, child_operation_id) = observation_checkpoint_identity(recovery, &observation)?;
    let disposition = durable_historical_disposition(observation.disposition);
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
    let descriptor_digest = hex::encode(descriptor.digest());
    recovery
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action,
            child_operation_id,
            disposition,
            descriptor_digest: descriptor_digest.clone(),
            descriptor_locator_digest: recovery.staged_locator_digest.clone(),
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
            observed_at_ms: crate::dml::now_unix_millis(),
        });
    recovery.next_action = StatementNextAction::ManualInspect;
    record_recovery(active, recovery)?;
    Ok((observation, descriptor_digest))
}

fn foreground_historical_descriptor(
    recovery: &DmlCtasRecoveryRecord,
    authority: &ForegroundStageAuthority,
) -> Result<ConnectorHistoricalCtasDescriptor, DmlError> {
    let fence = recovery
        .catalog_fence
        .as_ref()
        .ok_or_else(|| DmlError::journal_corruption("CTAS recovery has no catalog fence"))?;
    let fence_receipt_digest = decode_digest(
        fence
            .receipt_digest
            .as_deref()
            .ok_or_else(|| DmlError::journal_corruption("CTAS catalog fence has no receipt"))?,
        "CTAS catalog fence receipt",
    )?;
    let mut checkpoints = vec![ConnectorHistoricalCtasCheckpoint {
        action_id: connector_action_id(fence.action_id)?,
        action: ConnectorHistoricalCtasAction::AdvanceFence,
        dispatch: ConnectorHistoricalCtasDispatchState::Completed,
        input_digest: decode_digest(&fence.request_digest, "CTAS advance-fence request")?,
        evidence_digest: Some(fence_receipt_digest),
    }];
    for checkpoint in &recovery.dispatch_checkpoints {
        let action = match checkpoint.action {
            DmlCtasActionKind::AdvanceFence => continue,
            DmlCtasActionKind::Stage => ConnectorHistoricalCtasAction::Stage,
            DmlCtasActionKind::Publish => ConnectorHistoricalCtasAction::Publish,
            DmlCtasActionKind::Abort => ConnectorHistoricalCtasAction::Abort,
            DmlCtasActionKind::Write => continue,
        };
        let evidence_digest = if checkpoint.action == DmlCtasActionKind::Stage {
            Some(authority.proof.digest())
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
        let dispatch = if evidence_digest.is_some() {
            ConnectorHistoricalCtasDispatchState::Completed
        } else {
            match checkpoint.dispatch_certainty {
                DmlCtasDispatchCertainty::ConfirmedNotDispatched => {
                    ConnectorHistoricalCtasDispatchState::NotDispatched
                }
                DmlCtasDispatchCertainty::PossiblyDispatched => {
                    ConnectorHistoricalCtasDispatchState::Unknown
                }
            }
        };
        checkpoints.push(ConnectorHistoricalCtasCheckpoint {
            action_id: connector_action_id(checkpoint.child_operation_id)?,
            action,
            dispatch,
            input_digest: decode_digest(&checkpoint.request_digest, "CTAS action request")?,
            evidence_digest,
        });
    }
    ConnectorHistoricalCtasDescriptor::try_new(
        authority.locator.issuance_owner().clone(),
        authority.locator.issuance_fence().clone(),
        fence_receipt_digest,
        authority.locator.target_digest(),
        authority.create_policy,
        Some(authority.locator.clone()),
        checkpoints,
        Some(authority.proof.clone()),
    )
    .map_err(DmlError::executor)
}

fn observation_checkpoint_identity(
    recovery: &DmlCtasRecoveryRecord,
    observation: &ConnectorHistoricalCtasObservation,
) -> Result<(DmlCtasActionKind, Uuid), DmlError> {
    let preferred = match observation.disposition {
        ConnectorHistoricalCtasDisposition::Published => Some(DmlCtasActionKind::Publish),
        ConnectorHistoricalCtasDisposition::NoOp => Some(DmlCtasActionKind::Publish),
        ConnectorHistoricalCtasDisposition::Aborted => Some(DmlCtasActionKind::Abort),
        ConnectorHistoricalCtasDisposition::Staged => Some(DmlCtasActionKind::Stage),
        ConnectorHistoricalCtasDisposition::NotCreated
        | ConnectorHistoricalCtasDisposition::Conflict
        | ConnectorHistoricalCtasDisposition::Ambiguous
        | ConnectorHistoricalCtasDisposition::Unsupported => None,
    };
    let checkpoint = preferred.and_then(|preferred| {
        recovery
            .dispatch_checkpoints
            .iter()
            .rev()
            .find(|checkpoint| checkpoint.action == preferred)
    });
    if let Some(checkpoint) = checkpoint {
        return Ok((checkpoint.action, checkpoint.child_operation_id));
    }
    if observation.disposition == ConnectorHistoricalCtasDisposition::Staged {
        return Err(DmlError::journal_corruption(
            "staged CTAS inspection has no durable stage checkpoint",
        ));
    }
    let fence = recovery.catalog_fence.as_ref().ok_or_else(|| {
        DmlError::journal_corruption("CTAS inspection has no current catalog fence")
    })?;
    if fence.receipt_payload.is_none() {
        return Err(DmlError::journal_corruption(
            "CTAS inspection has no confirmed current catalog fence",
        ));
    }
    Ok((DmlCtasActionKind::AdvanceFence, fence.action_id))
}

const fn durable_historical_disposition(
    disposition: ConnectorHistoricalCtasDisposition,
) -> DmlCtasHistoricalDisposition {
    match disposition {
        ConnectorHistoricalCtasDisposition::NotCreated => DmlCtasHistoricalDisposition::Absent,
        ConnectorHistoricalCtasDisposition::Staged => DmlCtasHistoricalDisposition::Staged,
        ConnectorHistoricalCtasDisposition::Published => DmlCtasHistoricalDisposition::Published,
        ConnectorHistoricalCtasDisposition::NoOp => DmlCtasHistoricalDisposition::NoOp,
        ConnectorHistoricalCtasDisposition::Aborted => DmlCtasHistoricalDisposition::Aborted,
        ConnectorHistoricalCtasDisposition::Conflict => DmlCtasHistoricalDisposition::Conflict,
        ConnectorHistoricalCtasDisposition::Ambiguous => DmlCtasHistoricalDisposition::Ambiguous,
        ConnectorHistoricalCtasDisposition::Unsupported => {
            DmlCtasHistoricalDisposition::Unsupported
        }
    }
}

const fn durable_conflict_kind(kind: ConnectorCtasConflictKind) -> DmlCtasConflictKind {
    match kind {
        ConnectorCtasConflictKind::StaleFence => DmlCtasConflictKind::StaleFence,
        ConnectorCtasConflictKind::IdentityConflict => DmlCtasConflictKind::IdentityConflict,
        ConnectorCtasConflictKind::DigestConflict => DmlCtasConflictKind::DigestConflict,
        ConnectorCtasConflictKind::AlreadyPublished => DmlCtasConflictKind::AlreadyPublished,
        ConnectorCtasConflictKind::AlreadyAborted => DmlCtasConflictKind::AlreadyAborted,
        ConnectorCtasConflictKind::CreatePolicyConflict => {
            DmlCtasConflictKind::CreatePolicyConflict
        }
    }
}

fn decode_digest(value: &str, label: &str) -> Result<[u8; 32], DmlError> {
    let bytes = hex::decode(value).map_err(|error| {
        DmlError::journal_corruption(format!("{label} digest is not hexadecimal: {error}"))
    })?;
    bytes
        .try_into()
        .map_err(|_| DmlError::journal_corruption(format!("{label} digest is not 32 bytes")))
}

fn historical_observation_fact(
    observation: &ConnectorHistoricalCtasObservation,
) -> DurableExternalFact {
    let outcome = match observation.disposition {
        ConnectorHistoricalCtasDisposition::Published => ExternalFactOutcome::KnownCommitted,
        ConnectorHistoricalCtasDisposition::NoOp => ExternalFactOutcome::NoOp,
        ConnectorHistoricalCtasDisposition::NotCreated
        | ConnectorHistoricalCtasDisposition::Aborted
        | ConnectorHistoricalCtasDisposition::Staged => ExternalFactOutcome::KnownUncommitted,
        ConnectorHistoricalCtasDisposition::Conflict => ExternalFactOutcome::Conflict,
        ConnectorHistoricalCtasDisposition::Unsupported => ExternalFactOutcome::Unsupported,
        ConnectorHistoricalCtasDisposition::Ambiguous => ExternalFactOutcome::CommitUnknown,
    };
    DurableExternalFact {
        outcome,
        receipt: Some(hex::encode(observation.digest())),
        evidence: observation
            .proof
            .as_ref()
            .and_then(|proof| proof.try_to_wire_v1().ok())
            .map(hex::encode),
        finalization_failure: None,
        failure: observation
            .failure
            .as_ref()
            .map(|failure| format!("{:?}: {}", failure.kind(), failure.message())),
    }
}

fn abort_foreground(
    engine: &dyn CtasEngine,
    active: &mut ActiveDmlOperation,
    mut recovery: DmlCtasRecoveryRecord,
    target: &PreparedCtasTarget,
    authority: &ForegroundStageAuthority,
    cause_fact: DurableExternalFact,
    cause: String,
) -> Result<(), DmlError> {
    let (observation, descriptor_digest) =
        inspect_foreground_visibility(engine, active, &mut recovery, authority)?;
    match observation.disposition {
        ConnectorHistoricalCtasDisposition::Published => {
            recovery.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
            recovery.next_action = StatementNextAction::None;
            record_recovery(active, &recovery)?;
            let mut saga = ctas_record(&active.stored)?;
            saga.phase = CtasSagaPhase::Committed;
            saga.publish_fact = Some(historical_observation_fact(&observation));
            saga.next_action = StatementNextAction::None;
            if active.stored.state != OperationState::Committing {
                active.mutate_statement(
                    OperationState::Committing,
                    OperationPayload::CtasSaga(saga.clone()),
                    None,
                )?;
            }
            active.mutate_statement(
                OperationState::Committed,
                OperationPayload::CtasSaga(saga.clone()),
                None,
            )?;
            active.mutate_statement(
                OperationState::Finalized,
                OperationPayload::CtasSaga(saga),
                None,
            )?;
            return Ok(());
        }
        ConnectorHistoricalCtasDisposition::NotCreated
        | ConnectorHistoricalCtasDisposition::Aborted => {
            recovery.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
            recovery.next_action = StatementNextAction::None;
            record_recovery(active, &recovery)?;
            let mut saga = ctas_record(&active.stored)?;
            saga.phase = CtasSagaPhase::Failed;
            saga.write_fact = Some(cause_fact);
            saga.abort_staging_fact = Some(historical_observation_fact(&observation));
            saga.next_action = StatementNextAction::None;
            if active.stored.state != OperationState::Aborting {
                active.mutate_statement(
                    OperationState::Aborting,
                    OperationPayload::CtasSaga(saga.clone()),
                    None,
                )?;
            }
            active.mutate_statement(
                OperationState::Aborted,
                OperationPayload::CtasSaga(saga),
                None,
            )?;
            return Err(operation_error(
                DmlErrorKind::Executor,
                active.operation_id(),
                StatementNextAction::None,
                cause,
            ));
        }
        ConnectorHistoricalCtasDisposition::Staged | ConnectorHistoricalCtasDisposition::NoOp => {}
        ConnectorHistoricalCtasDisposition::Conflict
        | ConnectorHistoricalCtasDisposition::Ambiguous
        | ConnectorHistoricalCtasDisposition::Unsupported => {
            recovery.cleanup_retention = DmlCtasCleanupRetention::Pending;
            recovery.next_action = StatementNextAction::ManualInspect;
            record_recovery(active, &recovery)?;
            let mut saga = ctas_record(&active.stored)?;
            saga.phase = CtasSagaPhase::AbortUnknown;
            saga.write_fact = Some(cause_fact);
            saga.abort_staging_fact = Some(historical_observation_fact(&observation));
            saga.next_action = StatementNextAction::ManualInspect;
            active.mutate_statement(
                OperationState::CommitUnknown,
                OperationPayload::CtasSaga(saga),
                Some(crate::dml::now_unix_millis()),
            )?;
            return Err(operation_error(
                DmlErrorKind::Commit,
                active.operation_id(),
                StatementNextAction::ManualInspect,
                format!("CTAS visibility is unresolved; cleanup was not dispatched: {cause}"),
            ));
        }
    }
    let abort_uuid = ctas_record(&active.stored)?.abort_staging_operation_id;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::AbortingStaging;
    saga.write_fact = Some(cause_fact.clone());
    saga.next_action = StatementNextAction::AbortStaging;
    active.mutate_statement(
        OperationState::Aborting,
        OperationPayload::CtasSaga(saga),
        Some(crate::dml::now_unix_millis()),
    )?;
    let action =
        match engine.prepare_abort_ctas(target.handle.as_ref(), connector_action_id(abort_uuid)?) {
            Ok(action) => action,
            Err(failure) => {
                return finish_local_catalog_preparation_failure_with_cause(
                    active,
                    recovery,
                    FactSlot::Abort,
                    failure,
                    cause_fact,
                    cause,
                );
            }
        };
    append_checkpoint(
        &mut recovery,
        DmlCtasActionKind::Abort,
        abort_uuid,
        action.input_digest,
    );
    record_recovery(active, &recovery)?;
    mark_checkpoint_dispatched(&mut recovery, DmlCtasActionKind::Abort, abort_uuid);
    record_recovery(active, &recovery)?;
    active.check_before_dispatch()?;
    let result = match engine.abort_ctas(action.handle.as_ref()) {
        Ok(result) => result,
        Err(failure) if catalog_failure_is_terminal(&failure) => {
            return finish_catalog_terminal_failure_with_cause(
                active,
                recovery,
                FactSlot::Abort,
                failure,
                cause_fact,
                cause,
            );
        }
        Err(failure) => {
            return finish_catalog_unknown(
                active,
                recovery,
                CtasSagaPhase::AbortUnknown,
                FactSlot::Abort,
                failure,
            );
        }
    };
    debug_assert_eq!(result.disposition, ConnectorCtasAbortDisposition::Aborted);
    recovery
        .historical_observations
        .push(DmlCtasHistoricalObservationRecord {
            action: DmlCtasActionKind::Abort,
            child_operation_id: abort_uuid,
            disposition: DmlCtasHistoricalDisposition::Aborted,
            descriptor_digest,
            descriptor_locator_digest: Some(hex::encode(target.facts.locator_digest)),
            observation_digest: hex::encode(result.digest()),
            locator_digest: None,
            proof_digest: Some(hex::encode(result.proof.digest())),
            proof_payload: Some(
                DmlOpaquePayload::try_new(
                    result
                        .proof
                        .try_to_wire_v1()
                        .map_err(DmlError::executor)?
                        .to_vec(),
                )
                .map_err(DmlError::journal_corruption)?,
            ),
            conflict_kind: None,
            failure: None,
            observed_at_ms: crate::dml::now_unix_millis(),
        });
    recovery.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
    recovery.next_action = StatementNextAction::None;
    record_recovery(active, &recovery)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::Failed;
    saga.write_fact = Some(cause_fact);
    saga.abort_staging_fact = Some(publication_fact(
        ExternalFactOutcome::KnownCommitted,
        &result.receipt,
    ));
    saga.next_action = StatementNextAction::None;
    active.mutate_statement(
        OperationState::Aborted,
        OperationPayload::CtasSaga(saga),
        None,
    )?;
    Err(operation_error(
        DmlErrorKind::Executor,
        active.operation_id(),
        StatementNextAction::None,
        cause,
    ))
}

fn new_recovery_record(
    facts: &novarocks::query_execution::dml::ctas::CtasTargetPreflightFacts,
    attempt_id: Uuid,
    generation: crate::dml::model::DmlExternalFenceGeneration,
    action_id: ConnectorCtasActionId,
    request_digest: [u8; 32],
) -> Result<DmlCtasRecoveryRecord, DmlError> {
    if facts.capability_version != 1 {
        return Err(DmlError::executor(
            "CTAS requires exact capability version 1",
        ));
    }
    Ok(DmlCtasRecoveryRecord {
        codec_version: DML_CTAS_RECOVERY_CODEC_VERSION,
        capability_version: facts.capability_version,
        recovery_attempt_id: attempt_id,
        recovery_cycle: 1,
        catalog_fence_history: Vec::new(),
        catalog_fence: Some(DmlCtasCatalogFenceRecord {
            generation,
            action_id: Uuid::from_bytes(action_id.to_bytes()),
            request_digest: hex::encode(request_digest),
            dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
            dispatched_at_ms: None,
            fence_digest: None,
            receipt_digest: None,
            receipt_payload: None,
            established_at_ms: None,
        }),
        staged_target_digest: None,
        staged_locator: None,
        staged_locator_digest: None,
        staged_proof_digest: None,
        staged_proof: None,
        dispatch_checkpoints: Vec::new(),
        historical_observations: Vec::new(),
        child_supersessions: Vec::new(),
        cleanup_retention: DmlCtasCleanupRetention::NotRequired,
        cleanup_receipt: None,
        next_action: StatementNextAction::ManualInspect,
        updated_at_ms: crate::dml::now_unix_millis(),
    })
}

fn record_recovery(
    active: &mut ActiveDmlOperation,
    recovery: &DmlCtasRecoveryRecord,
) -> Result<(), DmlError> {
    let mut recovery = recovery.clone();
    recovery.updated_at_ms = crate::dml::now_unix_millis();
    let due = recovery
        .requires_recovery_scan()
        .then(crate::dml::now_unix_millis);
    active.record_ctas_recovery(recovery, due)
}

fn mark_fence_dispatched(recovery: &mut DmlCtasRecoveryRecord) {
    let fence = recovery.catalog_fence.as_mut().expect("CTAS fence record");
    fence.dispatch_certainty = DmlCtasDispatchCertainty::PossiblyDispatched;
    fence.dispatched_at_ms = Some(crate::dml::now_unix_millis());
}

fn append_checkpoint(
    recovery: &mut DmlCtasRecoveryRecord,
    action: DmlCtasActionKind,
    child_operation_id: Uuid,
    request_digest: [u8; 32],
) {
    recovery
        .dispatch_checkpoints
        .push(DmlCtasDispatchCheckpointRecord {
            action,
            child_operation_id,
            request_digest: hex::encode(request_digest),
            dispatch_certainty: DmlCtasDispatchCertainty::ConfirmedNotDispatched,
            dispatched_at_ms: None,
        });
    recovery.next_action = StatementNextAction::ManualInspect;
}

fn mark_checkpoint_dispatched(
    recovery: &mut DmlCtasRecoveryRecord,
    action: DmlCtasActionKind,
    child_operation_id: Uuid,
) {
    let checkpoint = recovery
        .dispatch_checkpoints
        .iter_mut()
        .find(|checkpoint| {
            checkpoint.action == action && checkpoint.child_operation_id == child_operation_id
        })
        .expect("CTAS dispatch checkpoint");
    checkpoint.dispatch_certainty = DmlCtasDispatchCertainty::PossiblyDispatched;
    checkpoint.dispatched_at_ms = Some(crate::dml::now_unix_millis());
}

const fn catalog_failure_is_terminal(failure: &ConnectorCtasFailure) -> bool {
    matches!(
        failure,
        ConnectorCtasFailure::KnownNotDispatched(_) | ConnectorCtasFailure::Conflict { .. }
    )
}

fn finish_local_catalog_preparation_failure(
    active: &mut ActiveDmlOperation,
    mut recovery: DmlCtasRecoveryRecord,
    slot: FactSlot,
    failure: CtasFailure,
) -> Result<(), DmlError> {
    recovery.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
    recovery.next_action = StatementNextAction::None;
    record_recovery(active, &recovery)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = if failure.kind == CtasFailureKind::Unsupported {
        CtasSagaPhase::Unsupported
    } else {
        CtasSagaPhase::Failed
    };
    install_fact(&mut saga, slot, failure_fact(&failure));
    saga.next_action = StatementNextAction::None;
    active.mutate_statement(
        OperationState::FailedKnownUncommitted,
        OperationPayload::CtasSaga(saga),
        None,
    )?;
    Err(source_failure_error(active.operation_id(), failure))
}

fn finish_local_catalog_preparation_failure_with_cause(
    active: &mut ActiveDmlOperation,
    mut recovery: DmlCtasRecoveryRecord,
    slot: FactSlot,
    failure: CtasFailure,
    cause_fact: DurableExternalFact,
    cause: String,
) -> Result<(), DmlError> {
    recovery.cleanup_retention = DmlCtasCleanupRetention::Pending;
    recovery.next_action = StatementNextAction::AbortStaging;
    record_recovery(active, &recovery)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::Failed;
    saga.write_fact = Some(cause_fact);
    install_fact(&mut saga, slot, failure_fact(&failure));
    saga.next_action = StatementNextAction::AbortStaging;
    active.mutate_statement(
        OperationState::FailedKnownUncommitted,
        OperationPayload::CtasSaga(saga),
        Some(crate::dml::now_unix_millis()),
    )?;
    Err(operation_error(
        DmlErrorKind::Executor,
        active.operation_id(),
        StatementNextAction::AbortStaging,
        format!(
            "{cause}; CTAS cleanup request could not be prepared: {}",
            failure.message
        ),
    ))
}

fn finish_catalog_terminal_failure(
    active: &mut ActiveDmlOperation,
    mut recovery: DmlCtasRecoveryRecord,
    slot: FactSlot,
    failure: ConnectorCtasFailure,
) -> Result<(), DmlError> {
    if recovery.staged_locator.is_some() {
        recovery.cleanup_retention = DmlCtasCleanupRetention::Pending;
        recovery.next_action = StatementNextAction::AbortStaging;
    } else {
        recovery.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
        recovery.next_action = StatementNextAction::None;
    }
    record_recovery(active, &recovery)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = if failure.failure().kind()
        == novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported
    {
        CtasSagaPhase::Unsupported
    } else {
        CtasSagaPhase::Failed
    };
    install_fact(&mut saga, slot, connector_failure_fact(&failure));
    saga.next_action = recovery.next_action;
    active.mutate_statement(
        OperationState::FailedKnownUncommitted,
        OperationPayload::CtasSaga(saga),
        recovery
            .requires_recovery_scan()
            .then(crate::dml::now_unix_millis),
    )?;
    Err(operation_error(
        DmlErrorKind::Executor,
        active.operation_id(),
        recovery.next_action,
        format!("CTAS catalog action was rejected: {}", failure.failure()),
    ))
}

fn finish_catalog_terminal_failure_with_cause(
    active: &mut ActiveDmlOperation,
    mut recovery: DmlCtasRecoveryRecord,
    slot: FactSlot,
    failure: ConnectorCtasFailure,
    cause_fact: DurableExternalFact,
    cause: String,
) -> Result<(), DmlError> {
    recovery.cleanup_retention = DmlCtasCleanupRetention::Pending;
    recovery.next_action = StatementNextAction::AbortStaging;
    record_recovery(active, &recovery)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = CtasSagaPhase::Failed;
    saga.write_fact = Some(cause_fact);
    install_fact(&mut saga, slot, connector_failure_fact(&failure));
    saga.next_action = StatementNextAction::AbortStaging;
    active.mutate_statement(
        OperationState::FailedKnownUncommitted,
        OperationPayload::CtasSaga(saga),
        Some(crate::dml::now_unix_millis()),
    )?;
    Err(operation_error(
        DmlErrorKind::Executor,
        active.operation_id(),
        StatementNextAction::AbortStaging,
        format!("{cause}; CTAS cleanup was rejected: {}", failure.failure()),
    ))
}

fn finish_catalog_unknown(
    active: &mut ActiveDmlOperation,
    mut recovery: DmlCtasRecoveryRecord,
    phase: CtasSagaPhase,
    slot: FactSlot,
    failure: ConnectorCtasFailure,
) -> Result<(), DmlError> {
    recovery.next_action = StatementNextAction::ManualInspect;
    if recovery.staged_locator.is_some() {
        recovery.cleanup_retention = DmlCtasCleanupRetention::Pending;
    }
    record_recovery(active, &recovery)?;
    let mut saga = ctas_record(&active.stored)?;
    saga.phase = phase;
    saga.next_action = StatementNextAction::ManualInspect;
    install_fact(&mut saga, slot, connector_failure_fact(&failure));
    active.mutate_statement(
        OperationState::CommitUnknown,
        OperationPayload::CtasSaga(saga),
        Some(crate::dml::now_unix_millis()),
    )?;
    Err(operation_error(
        DmlErrorKind::Commit,
        active.operation_id(),
        StatementNextAction::ManualInspect,
        format!("CTAS catalog outcome is unresolved: {}", failure.failure()),
    ))
}

fn connector_failure_fact(failure: &ConnectorCtasFailure) -> DurableExternalFact {
    let conflict = failure
        .conflict_kind()
        .map(|kind| format!("{kind:?}: "))
        .unwrap_or_default();
    let normalized = CtasFailure {
        kind: if failure.conflict_kind().is_some() {
            CtasFailureKind::Conflict
        } else {
            mutation_failure(failure.failure().clone()).kind
        },
        message: format!("{conflict}{}", failure.failure().message()),
    };
    DurableExternalFact {
        outcome: match failure {
            ConnectorCtasFailure::KnownNotDispatched(_) => ExternalFactOutcome::KnownUncommitted,
            ConnectorCtasFailure::Conflict { .. } => ExternalFactOutcome::Conflict,
            ConnectorCtasFailure::PossiblyDispatched(_)
            | ConnectorCtasFailure::CommittedResponseInvalid(_)
            | ConnectorCtasFailure::Ambiguous(_) => ExternalFactOutcome::CommitUnknown,
        },
        receipt: None,
        evidence: None,
        finalization_failure: None,
        failure: Some(encode_failure(&normalized)),
    }
}

fn publication_fact(
    outcome: ExternalFactOutcome,
    receipt: &ConnectorCtasPublicationReceipt,
) -> DurableExternalFact {
    DurableExternalFact {
        outcome,
        receipt: Some(hex::encode(receipt.digest())),
        evidence: None,
        finalization_failure: None,
        failure: None,
    }
}

fn connector_action_id(uuid: Uuid) -> Result<ConnectorCtasActionId, DmlError> {
    ConnectorCtasActionId::try_from_bytes(*uuid.as_bytes()).map_err(DmlError::executor)
}

fn validate_preflight_facts(
    stored: &StoredOperation,
    facts: &novarocks::query_execution::dml::ctas::CtasTargetPreflightFacts,
) -> Result<(), DmlError> {
    if facts.capability_version == 1
        && facts.instance_id == stored.target.catalog
        && facts.target_namespace == stored.target.namespace
        && facts.target_table == stored.target.table
        && !facts.provider_id.is_empty()
    {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Executor,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "CTAS preflight facts conflict with the durable statement target",
        ))
    }
}

fn validate_target_facts_v2(
    stored: &StoredOperation,
    preflight: &novarocks::query_execution::dml::ctas::CtasTargetPreflightFacts,
    fence: &ConnectorCtasPublicationFence,
    facts: &CtasTargetFacts,
    locator: &ConnectorCtasStagedLocator,
    stage_action_id: Uuid,
) -> Result<(), DmlError> {
    if facts.operation_id == *stored.operation_id.as_uuid().as_bytes()
        && facts.provider_id == preflight.provider_id
        && facts.instance_id == preflight.instance_id
        && facts.incarnation == preflight.incarnation
        && facts.fence_digest == fence.digest()
        && facts.locator_digest == locator.digest()
        && locator.issuance_fence().digest() == fence.digest()
        && locator.operation_id().to_bytes() == *stored.operation_id.as_uuid().as_bytes()
        && locator.stage_action_id().to_bytes() == *stage_action_id.as_bytes()
        && Uuid::from_bytes(stage_action_id.into_bytes()).get_version_num() == 7
    {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Commit,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "CTAS staged target facts conflict with durable operation identity",
        ))
    }
}

fn source_failure_error(operation_id: DmlOperationId, failure: CtasFailure) -> DmlError {
    operation_error(
        DmlErrorKind::Executor,
        operation_id,
        StatementNextAction::None,
        format_failure("CTAS request preparation failed", &failure),
    )
}

fn finish_source_failure(
    active: &mut ActiveDmlOperation,
    stored: StoredOperation,
    failure: CtasFailure,
) -> Result<(), DmlError> {
    let mut record = ctas_record(&stored)?;
    record.phase = if failure.kind == CtasFailureKind::Unsupported {
        CtasSagaPhase::Unsupported
    } else {
        CtasSagaPhase::Failed
    };
    record.prepare_fact = Some(failure_fact(&failure));
    record.next_action = StatementNextAction::None;
    active.mutate_statement(
        OperationState::FailedKnownUncommitted,
        OperationPayload::CtasSaga(record),
        None,
    )?;
    Err(source_failure_error(active.operation_id(), failure))
}

#[derive(Clone, Copy)]
enum FactSlot {
    Prepare,
    Publish,
    Abort,
}

fn validate_source_facts(
    stored: &StoredOperation,
    source: &PreparedCtasSource,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), DmlError> {
    let matches = source.handle.execution_identity() == source.facts.execution_identity
        && source.facts.target_catalog == stored.target.catalog
        && source.facts.target_namespace == stored.target.namespace
        && source.facts.target_table == stored.target.table
        && source.facts.source_catalog.as_deref() == current_catalog
        && source.facts.source_database == current_database
        && !source.facts.output_columns.is_empty();
    if matches {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Executor,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "CTAS prepared source facts conflict with the durable statement identity",
        ))
    }
}

fn validate_prepared_write(
    stored: &StoredOperation,
    source: &PreparedCtasSource,
    target: &PreparedCtasTarget,
    write: &PreparedCtasWrite,
) -> Result<(), DmlError> {
    let expected = ctas_record(stored)?.write_operation_id;
    if write.write_operation_id.to_bytes() == *expected.as_bytes()
        && write.execution_identity == source.facts.execution_identity
        && write.handle.execution_identity() == source.facts.execution_identity
        && write.target_facts == target.facts
    {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Executor,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "CTAS prepared write facts drifted from the source or staged target",
        ))
    }
}

fn validate_completion(
    stored: &StoredOperation,
    source: &PreparedCtasSource,
    target: &PreparedCtasTarget,
    prepared: &PreparedCtasWrite,
    completion: &ConnectorWriteOperationCompletion,
    execution_identity: [u8; 32],
) -> Result<(), DmlError> {
    let expected_write = ctas_record(stored)?.write_operation_id;
    let cohorts = completion.sealed().cohorts();
    let matching = execution_identity == source.facts.execution_identity
        && execution_identity == prepared.execution_identity
        && completion.owner().instance_id.as_str() == target.facts.instance_id
        && completion.owner().incarnation.to_bytes() == target.facts.incarnation
        && completion.sealed().operation_id().to_bytes() == *expected_write.as_bytes()
        && cohorts.len() == 1;
    if matching {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Commit,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "CTAS writer completion conflicts with durable source/target identity",
        ))
    }
}

fn failure_fact(failure: &CtasFailure) -> DurableExternalFact {
    DurableExternalFact {
        outcome: match failure.kind {
            CtasFailureKind::Unsupported => ExternalFactOutcome::Unsupported,
            CtasFailureKind::AlreadyExists | CtasFailureKind::Conflict => {
                ExternalFactOutcome::Conflict
            }
            _ => ExternalFactOutcome::KnownUncommitted,
        },
        receipt: None,
        evidence: None,
        finalization_failure: None,
        failure: Some(encode_failure(failure)),
    }
}

fn install_fact(record: &mut CtasSagaRecord, slot: FactSlot, fact: DurableExternalFact) {
    match slot {
        FactSlot::Prepare => record.prepare_fact = Some(fact),
        FactSlot::Publish => record.publish_fact = Some(fact),
        FactSlot::Abort => record.abort_staging_fact = Some(fact),
    }
}

fn encode_write_completion(
    completion: &ConnectorWriteOperationCompletion,
) -> Result<(String, String), DmlError> {
    let cohort = completion
        .sealed()
        .cohorts()
        .first()
        .ok_or_else(|| DmlError::commit("CTAS completion has no write cohort"))?;
    let cohort_id = hex::encode(cohort.cohort_id().to_bytes());
    let encoded = serde_json::to_string(&DurableCtasWriteCompletionV1 {
        version: DURABLE_CTAS_FACT_VERSION,
        instance_id: completion.owner().instance_id.as_str().to_string(),
        incarnation: hex::encode(completion.owner().incarnation.to_bytes()),
        operation_id: hex::encode(completion.sealed().operation_id().to_bytes()),
        cohort_id: cohort_id.clone(),
        cohort_set_digest: hex::encode(completion.sealed().digest()),
        aggregate_digest: hex::encode(completion.aggregate_digest()),
    })
    .map_err(DmlError::journal_corruption)?;
    ensure_fact_bound("CTAS writer completion", &encoded)?;
    Ok((encoded, cohort_id))
}

fn encode_evidence(evidence: &ExternalMutationEvidence) -> Result<String, DmlError> {
    let wire = evidence.try_to_wire_v1().map_err(DmlError::commit)?;
    let encoded = hex::encode(wire);
    ensure_fact_bound("CTAS evidence", &encoded)?;
    Ok(encoded)
}

fn encode_failure(failure: &CtasFailure) -> String {
    let original_message_bytes = failure.message.len();
    let mut prefix_end = original_message_bytes.min(DURABLE_FAILURE_PREFIX_BYTES);
    while !failure.message.is_char_boundary(prefix_end) {
        prefix_end -= 1;
    }
    serde_json::to_string(&DurableCtasFailureV1 {
        version: DURABLE_CTAS_FACT_VERSION,
        kind: failure_kind(failure.kind),
        message_prefix: &failure.message[..prefix_end],
        message_truncated: prefix_end < original_message_bytes,
        original_message_bytes,
        original_message_sha256: hex::encode(Sha256::digest(failure.message.as_bytes())),
    })
    .unwrap_or_else(|_| {
        r#"{"version":1,"kind":"INTERNAL","message_prefix":"failure encoding failed","message_truncated":true,"original_message_bytes":0,"original_message_sha256":""}"#.to_string()
    })
}

fn ensure_fact_bound(label: &str, value: &str) -> Result<(), DmlError> {
    if value.len() <= DML_CTAS_FACT_ENCODED_LIMIT {
        Ok(())
    } else {
        Err(DmlError::journal_unavailable(format!(
            "{label} encoded size {} exceeds CTAS fact limit {DML_CTAS_FACT_ENCODED_LIMIT}",
            value.len()
        )))
    }
}

fn ctas_record(stored: &StoredOperation) -> Result<CtasSagaRecord, DmlError> {
    match &stored.payload {
        OperationPayload::CtasSaga(record) => Ok(record.clone()),
        _ => Err(operation_error(
            DmlErrorKind::JournalCorruption,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "durable CTAS operation has the wrong payload kind",
        )),
    }
}

fn syntactic_target(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> OperationTarget {
    let (catalog, namespace, table) = match parts {
        [table] => (
            current_catalog.unwrap_or_default().to_string(),
            current_database.to_string(),
            table.clone(),
        ),
        [namespace, table] => (
            current_catalog.unwrap_or_default().to_string(),
            namespace.clone(),
            table.clone(),
        ),
        [catalog, namespace, table] => (catalog.clone(), namespace.clone(), table.clone()),
        _ => (
            current_catalog.unwrap_or_default().to_string(),
            current_database.to_string(),
            parts.join("."),
        ),
    };
    OperationTarget {
        catalog,
        namespace,
        table,
        ref_name: None,
    }
}

fn policy_name(policy: CreatePolicy) -> &'static str {
    match policy {
        CreatePolicy::FailIfExists => CTAS_CREATE_POLICY_FAIL_IF_EXISTS,
        CreatePolicy::NoOpIfExists => CTAS_CREATE_POLICY_NO_OP_IF_EXISTS,
    }
}

fn mutation_failure(failure: ConnectorMutationFailure) -> CtasFailure {
    use novarocks_spi::connector::ConnectorMutationFailureKind as Kind;
    let kind = match failure.kind() {
        Kind::InvalidRequest => CtasFailureKind::InvalidRequest,
        Kind::NotFound => CtasFailureKind::NotFound,
        Kind::AlreadyExists => CtasFailureKind::AlreadyExists,
        Kind::Conflict => CtasFailureKind::Conflict,
        Kind::Unsupported => CtasFailureKind::Unsupported,
        Kind::Cancelled => CtasFailureKind::Cancelled,
        Kind::DeadlineExceeded => CtasFailureKind::DeadlineExceeded,
        Kind::Unavailable => CtasFailureKind::Unavailable,
        _ => CtasFailureKind::Internal,
    };
    CtasFailure {
        kind,
        message: failure.message().to_string(),
    }
}

fn journal_error(error: DmlError, operation_id: DmlOperationId) -> DmlError {
    operation_error(
        error.kind(),
        operation_id,
        StatementNextAction::ManualInspect,
        error,
    )
}

fn unknown_error(operation_id: DmlOperationId, phase: &str, failure: &CtasFailure) -> DmlError {
    operation_error(
        DmlErrorKind::Commit,
        operation_id,
        StatementNextAction::ManualInspect,
        format_failure(&format!("{phase} remains unresolved"), failure),
    )
}

fn operation_error(
    kind: DmlErrorKind,
    operation_id: DmlOperationId,
    next_action: StatementNextAction,
    message: impl std::fmt::Display,
) -> DmlError {
    DmlError::new(kind, message)
        .with_operation_id(operation_id)
        .with_next_action(next_action)
}

fn format_failure(prefix: &str, failure: &CtasFailure) -> String {
    format!(
        "{prefix}: {}: {}",
        failure_kind(failure.kind),
        failure.message
    )
}

fn failure_kind(kind: CtasFailureKind) -> &'static str {
    match kind {
        CtasFailureKind::InvalidRequest => "INVALID_REQUEST",
        CtasFailureKind::NotFound => "NOT_FOUND",
        CtasFailureKind::AlreadyExists => "ALREADY_EXISTS",
        CtasFailureKind::Conflict => "CONFLICT",
        CtasFailureKind::Unsupported => "UNSUPPORTED",
        CtasFailureKind::Cancelled => "CANCELLED",
        CtasFailureKind::DeadlineExceeded => "DEADLINE_EXCEEDED",
        CtasFailureKind::Unavailable => "UNAVAILABLE",
        CtasFailureKind::Internal => "INTERNAL",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dml::model::{
        DmlExternalFenceGeneration, validate_ctas_recovery, validate_ctas_recovery_transition,
    };
    use novarocks::query_execution::dml::ctas::CtasTargetPreflightFacts;
    use novarocks_spi::connector::ConnectorMutationFailureKind;

    fn confirmed_recovery() -> DmlCtasRecoveryRecord {
        let action_id = ConnectorCtasActionId::try_from_bytes(*Uuid::now_v7().as_bytes()).unwrap();
        let facts = CtasTargetPreflightFacts {
            provider_id: "iceberg".to_string(),
            instance_id: "rest".to_string(),
            incarnation: [1; 16],
            capability_version: 1,
            target_namespace: "db".to_string(),
            target_table: "t".to_string(),
        };
        let mut recovery = new_recovery_record(
            &facts,
            Uuid::now_v7(),
            DmlExternalFenceGeneration {
                control_plane_incarnation: 1,
                resource_epoch: 1,
                fence_generation: 1,
            },
            action_id,
            [1; 32],
        )
        .unwrap();
        mark_fence_dispatched(&mut recovery);
        let fence = recovery.catalog_fence.as_mut().unwrap();
        fence.fence_digest = Some(hex::encode([2; 32]));
        fence.receipt_digest = Some(hex::encode([3; 32]));
        fence.receipt_payload = Some(DmlOpaquePayload::try_new(vec![4]).unwrap());
        fence.established_at_ms = Some(1);
        recovery.next_action = StatementNextAction::None;
        recovery
    }

    fn install_staging(recovery: &mut DmlCtasRecoveryRecord) -> Uuid {
        let stage = Uuid::now_v7();
        append_checkpoint(recovery, DmlCtasActionKind::Stage, stage, [5; 32]);
        assert_eq!(recovery.next_action, StatementNextAction::ManualInspect);
        mark_checkpoint_dispatched(recovery, DmlCtasActionKind::Stage, stage);
        recovery.staged_locator = Some(DmlOpaquePayload::try_new(vec![6]).unwrap());
        recovery.staged_locator_digest = Some(hex::encode([7; 32]));
        recovery.staged_proof_digest = Some(hex::encode([8; 32]));
        recovery.staged_proof = Some(DmlOpaquePayload::try_new(vec![9]).unwrap());
        recovery.staged_target_digest = Some(hex::encode([2; 32]));
        recovery.cleanup_retention = DmlCtasCleanupRetention::Pending;
        recovery.next_action = StatementNextAction::AbortStaging;
        stage
    }

    #[test]
    fn stage_checkpoint_is_durable_before_dispatch_and_retains_cleanup() {
        let mut recovery = confirmed_recovery();
        let stage = install_staging(&mut recovery);
        validate_ctas_recovery(&recovery).unwrap();
        let checkpoint = recovery.dispatch_checkpoints.first().unwrap();
        assert_eq!(checkpoint.child_operation_id, stage);
        assert_eq!(
            checkpoint.dispatch_certainty,
            DmlCtasDispatchCertainty::PossiblyDispatched
        );
        assert!(checkpoint.dispatched_at_ms.is_some());
        assert!(recovery.requires_recovery_scan());
    }

    #[test]
    fn proven_abort_closes_foreground_staging_without_cleanup_receipt() {
        let mut pending = confirmed_recovery();
        install_staging(&mut pending);
        validate_ctas_recovery(&pending).unwrap();

        let mut aborted = pending.clone();
        let action = Uuid::now_v7();
        append_checkpoint(&mut aborted, DmlCtasActionKind::Abort, action, [10; 32]);
        mark_checkpoint_dispatched(&mut aborted, DmlCtasActionKind::Abort, action);
        aborted
            .historical_observations
            .push(DmlCtasHistoricalObservationRecord {
                action: DmlCtasActionKind::Abort,
                child_operation_id: action,
                disposition: DmlCtasHistoricalDisposition::Aborted,
                descriptor_digest: hex::encode([11; 32]),
                descriptor_locator_digest: Some(hex::encode([7; 32])),
                observation_digest: hex::encode([12; 32]),
                locator_digest: None,
                proof_digest: Some(hex::encode([13; 32])),
                proof_payload: Some(DmlOpaquePayload::try_new(vec![14]).unwrap()),
                conflict_kind: None,
                failure: None,
                observed_at_ms: 2,
            });
        aborted.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
        aborted.next_action = StatementNextAction::None;

        validate_ctas_recovery_transition(Some(&pending), &aborted).unwrap();
        assert!(!aborted.requires_recovery_scan());
    }

    #[test]
    fn fence_reply_loss_remains_recovery_due_without_stage_facts() {
        let mut recovery = confirmed_recovery();
        let fence = recovery.catalog_fence.as_mut().unwrap();
        fence.fence_digest = None;
        fence.receipt_digest = None;
        fence.receipt_payload = None;
        fence.established_at_ms = None;
        recovery.next_action = StatementNextAction::ManualInspect;
        validate_ctas_recovery(&recovery).unwrap();
        assert!(recovery.requires_recovery_scan());
        assert!(recovery.staged_locator.is_none());
    }

    #[test]
    fn takeover_archives_one_fence_attempt_and_keeps_stage_authority() {
        let mut previous = confirmed_recovery();
        install_staging(&mut previous);
        validate_ctas_recovery(&previous).unwrap();

        let mut takeover = previous.clone();
        let superseded = takeover.catalog_fence.take().unwrap();
        takeover.catalog_fence_history.push(superseded);
        takeover.recovery_cycle += 1;
        takeover.recovery_attempt_id = Uuid::now_v7();
        takeover.catalog_fence = Some(DmlCtasCatalogFenceRecord {
            generation: DmlExternalFenceGeneration {
                control_plane_incarnation: 1,
                resource_epoch: 2,
                fence_generation: 2,
            },
            action_id: takeover.recovery_attempt_id,
            request_digest: hex::encode([20; 32]),
            dispatch_certainty: DmlCtasDispatchCertainty::PossiblyDispatched,
            dispatched_at_ms: Some(4),
            fence_digest: None,
            receipt_digest: None,
            receipt_payload: None,
            established_at_ms: None,
        });

        validate_ctas_recovery_transition(Some(&previous), &takeover).unwrap();
        assert_eq!(takeover.catalog_fence_history.len(), 1);
        assert_eq!(takeover.staged_target_digest, previous.staged_target_digest);

        let mut before_current_receipt = takeover.clone();
        append_checkpoint(
            &mut before_current_receipt,
            DmlCtasActionKind::Publish,
            Uuid::now_v7(),
            [21; 32],
        );
        assert!(
            validate_ctas_recovery_transition(Some(&takeover), &before_current_receipt).is_err()
        );

        let mut skipped = takeover.clone();
        skipped.catalog_fence_history.push(
            skipped
                .catalog_fence
                .as_ref()
                .expect("current fence")
                .clone(),
        );
        skipped.catalog_fence_history.push(
            skipped
                .catalog_fence
                .as_ref()
                .expect("current fence")
                .clone(),
        );
        assert!(validate_ctas_recovery_transition(Some(&takeover), &skipped).is_err());
    }

    #[test]
    fn only_definitely_undispatched_or_typed_conflict_is_terminal() {
        let failure = || {
            ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Conflict,
                "catalog rejected the request",
            )
        };
        let known_not_dispatched = ConnectorCtasFailure::KnownNotDispatched(failure());
        assert!(catalog_failure_is_terminal(&known_not_dispatched));
        assert_eq!(
            connector_failure_fact(&known_not_dispatched).outcome,
            ExternalFactOutcome::KnownUncommitted
        );
        let conflict = ConnectorCtasFailure::Conflict {
            kind: ConnectorCtasConflictKind::CreatePolicyConflict,
            failure: failure(),
        };
        assert!(catalog_failure_is_terminal(&conflict));
        assert_eq!(
            connector_failure_fact(&conflict).outcome,
            ExternalFactOutcome::Conflict
        );
        for uncertain in [
            ConnectorCtasFailure::PossiblyDispatched(failure()),
            ConnectorCtasFailure::CommittedResponseInvalid(failure()),
            ConnectorCtasFailure::Ambiguous(failure()),
        ] {
            assert!(!catalog_failure_is_terminal(&uncertain));
            assert_eq!(
                connector_failure_fact(&uncertain).outcome,
                ExternalFactOutcome::CommitUnknown
            );
        }
    }

    #[test]
    fn unrelated_terminal_observation_cannot_forget_staged_cleanup() {
        let mut pending = confirmed_recovery();
        install_staging(&mut pending);
        let abort = Uuid::now_v7();
        append_checkpoint(&mut pending, DmlCtasActionKind::Abort, abort, [10; 32]);
        mark_checkpoint_dispatched(&mut pending, DmlCtasActionKind::Abort, abort);
        pending
            .historical_observations
            .push(DmlCtasHistoricalObservationRecord {
                action: DmlCtasActionKind::Abort,
                child_operation_id: abort,
                disposition: DmlCtasHistoricalDisposition::Aborted,
                descriptor_digest: hex::encode([11; 32]),
                descriptor_locator_digest: None,
                observation_digest: hex::encode([12; 32]),
                locator_digest: None,
                proof_digest: Some(hex::encode([13; 32])),
                proof_payload: Some(DmlOpaquePayload::try_new(vec![14]).unwrap()),
                conflict_kind: None,
                failure: None,
                observed_at_ms: 2,
            });
        pending.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
        pending.next_action = StatementNextAction::None;
        assert!(validate_ctas_recovery(&pending).is_err());

        pending.historical_observations[0].descriptor_locator_digest =
            pending.staged_locator_digest.clone();
        validate_ctas_recovery(&pending).unwrap();
    }

    #[test]
    fn absent_stage_observation_without_locator_lineage_cannot_forget_cleanup() {
        let mut pending = confirmed_recovery();
        let stage = install_staging(&mut pending);
        pending
            .historical_observations
            .push(DmlCtasHistoricalObservationRecord {
                action: DmlCtasActionKind::Stage,
                child_operation_id: stage,
                disposition: DmlCtasHistoricalDisposition::Absent,
                descriptor_digest: hex::encode([15; 32]),
                descriptor_locator_digest: None,
                observation_digest: hex::encode([16; 32]),
                locator_digest: None,
                proof_digest: Some(hex::encode([17; 32])),
                proof_payload: Some(DmlOpaquePayload::try_new(vec![18]).unwrap()),
                conflict_kind: None,
                failure: None,
                observed_at_ms: 3,
            });
        pending.cleanup_retention = DmlCtasCleanupRetention::NotRequired;
        pending.next_action = StatementNextAction::None;

        assert!(validate_ctas_recovery(&pending).is_err());
    }
}
