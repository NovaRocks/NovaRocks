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

//! Frontend-owned `TRUNCATE TABLE` direct-mutation lifecycle.
//!
//! This use case deliberately has no writer, fragment, backend report or abort
//! carrier. The opaque core prepared handle retains the exact connector lease;
//! frontend only journals bounded provider-neutral facts around execute-once
//! and the optional same-session reconcile-once call.

use crate::common::admitted_query_context::RequestContext;
use crate::query_execution::dml::truncate::{
    PlanTruncateRequest, PreparedTruncate, TruncateCommand, TruncateDispatchState, TruncateEffect,
    TruncateEngine, TruncateEvidence, TruncateFailure, TruncateFailureKind, TruncateFinalization,
    TruncateOutcome, TruncatePlanError, TruncatePlanFacts, TruncateReceipt,
};
use bytes::Bytes;
use novarocks_proto::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorDataMutationPlanSummary, ConnectorDataMutationReceipt, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorMutationOperationId,
    ConnectorProviderId, ExternalMutationEvidence,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dml::coordination::ActiveDmlOperation;
use crate::dml::error::{DmlError, DmlErrorKind};
use crate::dml::model::{
    CreateStatementOperationRequest, DML_EXTERNAL_FACT_ENCODED_LIMIT, DmlDirectMutationKind,
    DmlOperationId, DurableExternalFact, DurableMutationSummary, ExternalFactOutcome,
    OperationKind, OperationPayload, OperationState, OperationTarget, StatementNextAction,
    StoredOperation, TruncateLifecyclePhase, TruncateLifecycleRecord,
};
use crate::dml::service::DmlService;

const DURABLE_TRUNCATE_RECEIPT_VERSION: u8 = 1;
const DURABLE_FAILURE_MESSAGE_PREFIX_BYTES: usize = 2 * 1024;

#[derive(Serialize)]
struct DurableTruncateReceiptV1<'a> {
    version: u8,
    effect: &'static str,
    provider_id: &'a str,
    instance_id: &'a str,
    incarnation: String,
    mutation_operation_id: String,
    operation_kind: &'a str,
    request_digest: String,
    plan_digest: String,
    state_digest: String,
    summary: DurableMutationSummary,
    opaque_payload: String,
    opaque_payload_digest: String,
}

#[derive(Serialize)]
struct DurableTruncateFailureV1<'a> {
    version: u8,
    kind: &'static str,
    message_prefix: &'a str,
    message_truncated: bool,
    original_message_bytes: usize,
    original_message_sha256: String,
}

impl DmlService {
    /// Executes one already-admitted typed TRUNCATE command through the
    /// durable direct-mutation lifecycle.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub fn execute_truncate(
        &self,
        engine: &dyn TruncateEngine,
        command: TruncateCommand,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        let operation_id = DmlOperationId::new_v7();
        let connector_operation_id = Uuid::now_v7();
        let session = context.session();
        let initial_record = TruncateLifecycleRecord {
            phase: TruncateLifecyclePhase::Preparing,
            connector_operation_id,
            provider_id: None,
            connector_instance_id: None,
            connector_incarnation: None,
            target_ref: command.target_ref.clone(),
            request_digest: None,
            plan_digest: None,
            state_digest: None,
            plan_summary: None,
            outcome: None,
            next_action: StatementNextAction::None,
        };
        let target = syntactic_target(
            &command.target_parts,
            session.current_catalog(),
            session.current_database(),
            &command.target_ref,
        );
        let mut active = self
            .begin_statement_operation(CreateStatementOperationRequest {
                operation_id,
                mutation_id: Uuid::now_v7(),
                operation_kind: OperationKind::Truncate,
                target,
                attempt_id: operation_id.to_string(),
                payload: OperationPayload::TruncateLifecycle(initial_record),
                created_at_ms: crate::dml::now_unix_millis(),
            })
            .map_err(|error| journal_error(error, operation_id))?;

        let result = execute_truncate_operation(
            engine,
            context,
            query_options,
            command,
            connector_operation_id,
            &mut active,
        );
        let _ = active.release();
        result
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn execute_truncate_operation(
    engine: &dyn TruncateEngine,
    context: &RequestContext,
    query_options: Option<&QueryOptions>,
    command: TruncateCommand,
    connector_operation_id: Uuid,
    active: &mut ActiveDmlOperation,
) -> Result<(), DmlError> {
    let session = context.session();
    let mut stored = active.stored.clone();

    active.check_before_dispatch()?;
    let prepared = match engine.plan_truncate(PlanTruncateRequest {
        command,
        current_catalog: session.current_catalog().map(ToOwned::to_owned),
        current_database: session.current_database().to_string(),
        mutation_operation_id: connector_operation_id.into_bytes(),
        query_options: query_options.cloned(),
        execution: context.execution().clone(),
    }) {
        Ok(prepared) => prepared,
        Err(error) => match finish_plan_failure(active, stored, error) {
            Ok(()) => unreachable!("a failed TRUNCATE plan cannot produce success"),
            Err(error) => return Err(error),
        },
    };

    if let Err(failure) = validate_plan_facts(connector_operation_id, &stored, &prepared.facts) {
        return match persist_known_uncommitted(active, stored, failure) {
            Ok(()) => unreachable!("invalid TRUNCATE plan facts cannot produce success"),
            Err(error) => Err(error),
        };
    }
    let planned = planned_record(&prepared.facts, connector_operation_id);
    stored = persist(active, stored, OperationState::Committing, planned.clone())?;
    if let Err(error) = preflight_external_truth(active, &stored) {
        return match persist_known_uncommitted(
            active,
            stored,
            TruncateFailure {
                kind: TruncateFailureKind::ResourceExhausted,
                message: format!(
                    "TRUNCATE journal cannot retain the worst-case post-dispatch truth: {error}"
                ),
            },
        ) {
            Ok(()) => unreachable!("failed TRUNCATE preflight cannot produce success"),
            Err(error) => Err(error),
        };
    }
    stored = persist(
        active,
        stored,
        OperationState::Committing,
        TruncateLifecycleRecord {
            phase: TruncateLifecyclePhase::Executing,
            ..planned
        },
    )?;

    active.check_before_dispatch()?;

    // Establish the external fence before the destructive execute. TRUNCATE
    // removes table content, so a superseded owner's late execute has to be
    // refused at the catalog rather than reported after the fact. The frontend
    // seals it because only it holds the resource identity a fence binds; the
    // plan facts carry exactly the identity the provider signed.
    stored = establish_and_record_fence(engine, active, stored, &prepared)?;

    finish_outcome(
        engine,
        active,
        stored,
        &prepared,
        engine.execute_truncate(prepared.handle.as_ref()),
        true,
    )
}

/// Establish this attempt's external fence and durably journal its receipt,
/// before the destructive execute is dispatched.
///
/// Ordering, in full: mint the proposal from the *live* lease guard, refuse a
/// receipt this journal could never hold, ask the provider to publish the
/// marker, double-check that it acknowledged exactly the fence that was sealed,
/// then persist the receipt through the fenced journal. Only a completely
/// successful run licenses dispatch; every failure returns before `execute` is
/// reached and leaves the operation recoverable for historical data-mutation
/// recovery to classify.
#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn establish_and_record_fence(
    engine: &dyn TruncateEngine,
    active: &mut ActiveDmlOperation,
    stored: StoredOperation,
    prepared: &PreparedTruncate,
) -> Result<StoredOperation, DmlError> {
    let proposal = active.external_fence()?;
    // TRUNCATE owns no source set at all, so it binds no source scope.
    active.preflight_direct_mutation_fence(&proposal, DmlDirectMutationKind::Truncate, None)?;
    let fence = proposal
        .seal(
            novarocks_spi::connector::ConnectorWriteOperationId::from_bytes(
                prepared.facts.mutation_operation_id,
            ),
            novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(
                    prepared.facts.instance_id.as_str(),
                )
                .map_err(DmlError::executor)?,
                namespace: std::sync::Arc::from(prepared.facts.namespace.as_str()),
                table: std::sync::Arc::from(prepared.facts.table.as_str()),
            },
            novarocks_spi::connector::ConnectorWriteTargetRef::parse(
                prepared.facts.target_ref.as_str(),
            )
            .map_err(DmlError::executor)?,
        )
        .map_err(DmlError::executor)?;
    let receipt = engine
        .establish_truncate_external_fence(prepared.handle.as_ref(), fence.clone())
        .map_err(DmlError::executor)?;
    proposal.validate_established_receipt(&fence, &receipt)?;
    let record = crate::dml::reconcile::direct_mutation_fence_receipt_record(
        DmlDirectMutationKind::Truncate,
        &fence,
        &receipt,
        None,
    )
    .map_err(DmlError::journal_corruption)?;
    let recovery_due_at_ms = stored.recovery_due_at_ms;
    active
        .record_direct_mutation_fence(record, recovery_due_at_ms)
        .map_err(|error| journal_error(error, stored.operation_id))?;
    Ok(active.stored.clone())
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn finish_plan_failure(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    error: TruncatePlanError,
) -> Result<(), DmlError> {
    match error {
        TruncatePlanError::KnownUncommitted(failure) => {
            persist_known_uncommitted(journal, stored, failure)
        }
        TruncatePlanError::ContractFailure { failure, dispatch } => match dispatch {
            TruncateDispatchState::ConfirmedNotDispatched => {
                persist_known_uncommitted(journal, stored, failure)
            }
            TruncateDispatchState::PossiblyDispatched => {
                persist_possibly_dispatched(journal, stored, failure)
            }
        },
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn finish_outcome(
    engine: &dyn TruncateEngine,
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    prepared: &PreparedTruncate,
    outcome: TruncateOutcome,
    allow_reconcile: bool,
) -> Result<(), DmlError> {
    match outcome {
        TruncateOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
        } => persist_known_committed(
            journal,
            stored,
            &prepared.facts,
            effect,
            receipt,
            finalization,
        ),
        TruncateOutcome::KnownUncommitted { failure } => {
            persist_known_uncommitted(journal, stored, failure)
        }
        TruncateOutcome::CommitUnknown { failure, evidence } => {
            let first_evidence = durable_evidence(&stored)?;
            if let Err(error) = validate_evidence(stored.operation_id, &prepared.facts, &evidence) {
                return if let Some(first_evidence) = first_evidence {
                    persist_reconcile_corruption(journal, stored, first_evidence, error.to_string())
                } else {
                    persist_possibly_dispatched(
                        journal,
                        stored,
                        TruncateFailure {
                            kind: TruncateFailureKind::CorruptData,
                            message: error.to_string(),
                        },
                    )
                };
            }
            let evidence_hex = match encode_truncate_evidence_hex(&evidence) {
                Ok(evidence_hex) => evidence_hex,
                Err(message) => {
                    return if let Some(first_evidence) = first_evidence {
                        persist_reconcile_corruption(journal, stored, first_evidence, message)
                    } else {
                        persist_possibly_dispatched(
                            journal,
                            stored,
                            TruncateFailure {
                                kind: TruncateFailureKind::ResourceExhausted,
                                message,
                            },
                        )
                    };
                }
            };
            if let Some(first_evidence) = &first_evidence
                && first_evidence != &evidence_hex
            {
                return persist_reconcile_corruption(
                    journal,
                    stored,
                    first_evidence.clone(),
                    "TRUNCATE reconcile returned evidence different from the first durable evidence",
                );
            }
            let durable_evidence = first_evidence.unwrap_or(evidence_hex);
            let unknown_record = outcome_record(
                &stored,
                TruncateLifecyclePhase::CommitUnknown,
                if allow_reconcile {
                    StatementNextAction::Reconcile
                } else {
                    StatementNextAction::ManualInspect
                },
                DurableExternalFact {
                    outcome: ExternalFactOutcome::CommitUnknown,
                    receipt: None,
                    evidence: Some(durable_evidence),
                    finalization_failure: None,
                    failure: Some(encode_failure(&failure)),
                },
            )?;
            let stored = persist(
                journal,
                stored,
                OperationState::CommitUnknown,
                unknown_record,
            )?;
            if !allow_reconcile {
                return Err(operation_error(
                    DmlErrorKind::Commit,
                    stored.operation_id,
                    StatementNextAction::ManualInspect,
                    format_failure("TRUNCATE remains commit-unknown after reconcile", &failure),
                ));
            }

            let reconciling = outcome_record_without_new_fact(
                &stored,
                TruncateLifecyclePhase::Reconciling,
                StatementNextAction::Reconcile,
            )?;
            let stored = persist(journal, stored, OperationState::CommitUnknown, reconciling)?;
            journal.check_before_dispatch()?;
            let reconciled = engine.reconcile_truncate(prepared.handle.as_ref(), &evidence);
            finish_outcome(engine, journal, stored, prepared, reconciled, false)
        }
        TruncateOutcome::ContractFailure { failure, dispatch } => match dispatch {
            TruncateDispatchState::ConfirmedNotDispatched => {
                persist_known_uncommitted(journal, stored, failure)
            }
            TruncateDispatchState::PossiblyDispatched => {
                persist_possibly_dispatched(journal, stored, failure)
            }
        },
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn persist_known_committed(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    facts: &TruncatePlanFacts,
    effect: TruncateEffect,
    receipt: TruncateReceipt,
    finalization: TruncateFinalization,
) -> Result<(), DmlError> {
    let retained_evidence = durable_evidence(&stored)?;
    if let Err(failure) = validate_receipt(facts, &receipt) {
        return persist_invalid_committed_receipt(
            journal,
            stored,
            retained_evidence,
            failure,
            &finalization,
        );
    }
    let receipt_json = match encode_receipt(effect, &receipt) {
        Ok(receipt_json) => receipt_json,
        Err(message) => {
            return persist_invalid_committed_receipt(
                journal,
                stored,
                retained_evidence,
                TruncateFailure {
                    kind: TruncateFailureKind::ResourceExhausted,
                    message,
                },
                &finalization,
            );
        }
    };
    let finalization_failure = match &finalization {
        TruncateFinalization::Complete => None,
        TruncateFinalization::Failed(failure) => Some(encode_failure(failure)),
    };
    let committed_record = outcome_record(
        &stored,
        TruncateLifecyclePhase::Committed,
        match finalization {
            TruncateFinalization::Complete => StatementNextAction::None,
            TruncateFinalization::Failed(_) => StatementNextAction::RetryFinalize,
        },
        DurableExternalFact {
            outcome: ExternalFactOutcome::KnownCommitted,
            receipt: Some(receipt_json),
            evidence: retained_evidence,
            finalization_failure,
            failure: None,
        },
    )?;
    let stored = persist(journal, stored, OperationState::Committed, committed_record)?;
    match finalization {
        TruncateFinalization::Complete => {
            let finalized = outcome_record_without_new_fact(
                &stored,
                TruncateLifecyclePhase::Committed,
                StatementNextAction::None,
            )?;
            persist(journal, stored, OperationState::Finalized, finalized)?;
            Ok(())
        }
        TruncateFinalization::Failed(failure) => {
            let finalizing = outcome_record_without_new_fact(
                &stored,
                TruncateLifecyclePhase::Committed,
                StatementNextAction::RetryFinalize,
            )?;
            let stored = persist(journal, stored, OperationState::Finalizing, finalizing)?;
            let failed = outcome_record_without_new_fact(
                &stored,
                TruncateLifecyclePhase::Committed,
                StatementNextAction::RetryFinalize,
            )?;
            let stored = persist(
                journal,
                stored,
                OperationState::FinalizeFailedKnownCommitted,
                failed,
            )?;
            Err(operation_error(
                DmlErrorKind::CommittedButUnfinalized,
                stored.operation_id,
                StatementNextAction::RetryFinalize,
                format_failure("TRUNCATE committed but finalization failed", &failure),
            ))
        }
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn persist_invalid_committed_receipt(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    retained_evidence: Option<String>,
    failure: TruncateFailure,
    finalization: &TruncateFinalization,
) -> Result<(), DmlError> {
    let committed_record = outcome_record(
        &stored,
        TruncateLifecyclePhase::Committed,
        StatementNextAction::ManualInspect,
        DurableExternalFact {
            outcome: ExternalFactOutcome::KnownCommitted,
            receipt: None,
            evidence: retained_evidence,
            finalization_failure: match finalization {
                TruncateFinalization::Complete => None,
                TruncateFinalization::Failed(finalization_failure) => {
                    Some(encode_failure(finalization_failure))
                }
            },
            failure: Some(encode_failure(&failure)),
        },
    )?;
    let stored = persist(journal, stored, OperationState::Committed, committed_record)?;
    let finalizing = outcome_record_without_new_fact(
        &stored,
        TruncateLifecyclePhase::Committed,
        StatementNextAction::ManualInspect,
    )?;
    let stored = persist(journal, stored, OperationState::Finalizing, finalizing)?;
    let failed = outcome_record_without_new_fact(
        &stored,
        TruncateLifecyclePhase::Committed,
        StatementNextAction::ManualInspect,
    )?;
    let stored = persist(
        journal,
        stored,
        OperationState::FinalizeFailedKnownCommitted,
        failed,
    )?;
    Err(operation_error(
        DmlErrorKind::CommittedButUnfinalized,
        stored.operation_id,
        StatementNextAction::ManualInspect,
        format_failure(
            "TRUNCATE is known committed but its receipt is invalid",
            &failure,
        ),
    ))
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn persist_known_uncommitted(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    failure: TruncateFailure,
) -> Result<(), DmlError> {
    let retained_evidence = durable_evidence(&stored)?;
    let failed_record = outcome_record(
        &stored,
        TruncateLifecyclePhase::Failed,
        StatementNextAction::None,
        DurableExternalFact {
            outcome: fact_outcome(&failure),
            receipt: None,
            evidence: retained_evidence,
            finalization_failure: None,
            failure: Some(encode_failure(&failure)),
        },
    )?;
    let stored = persist(
        journal,
        stored,
        OperationState::FailedKnownUncommitted,
        failed_record,
    )?;
    Err(operation_error(
        DmlErrorKind::Executor,
        stored.operation_id,
        StatementNextAction::None,
        format_failure("TRUNCATE is known uncommitted", &failure),
    ))
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn persist_possibly_dispatched(
    journal: &mut ActiveDmlOperation,
    mut stored: StoredOperation,
    failure: TruncateFailure,
) -> Result<(), DmlError> {
    if stored.state == OperationState::Preparing {
        let dispatch_started = outcome_record_without_new_fact(
            &stored,
            TruncateLifecyclePhase::Executing,
            StatementNextAction::ManualInspect,
        )?;
        stored = persist(
            journal,
            stored,
            OperationState::Committing,
            dispatch_started,
        )?;
    }
    let retained_evidence = durable_evidence(&stored)?;
    let unknown_record = outcome_record(
        &stored,
        TruncateLifecyclePhase::CommitUnknown,
        StatementNextAction::ManualInspect,
        DurableExternalFact {
            outcome: ExternalFactOutcome::CommitUnknown,
            receipt: None,
            evidence: retained_evidence,
            finalization_failure: None,
            failure: Some(encode_failure(&failure)),
        },
    )?;
    let stored = persist(
        journal,
        stored,
        OperationState::CommitUnknown,
        unknown_record,
    )?;
    Err(operation_error(
        DmlErrorKind::Commit,
        stored.operation_id,
        StatementNextAction::ManualInspect,
        format_failure("TRUNCATE dispatch may have occurred", &failure),
    ))
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn persist_reconcile_corruption(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    first_evidence: String,
    message: impl Into<String>,
) -> Result<(), DmlError> {
    let failure = TruncateFailure {
        kind: TruncateFailureKind::CorruptData,
        message: message.into(),
    };
    let unknown_record = outcome_record(
        &stored,
        TruncateLifecyclePhase::CommitUnknown,
        StatementNextAction::ManualInspect,
        DurableExternalFact {
            outcome: ExternalFactOutcome::CommitUnknown,
            receipt: None,
            evidence: Some(first_evidence),
            finalization_failure: None,
            failure: Some(encode_failure(&failure)),
        },
    )?;
    let stored = persist(
        journal,
        stored,
        OperationState::CommitUnknown,
        unknown_record,
    )?;
    Err(operation_error(
        DmlErrorKind::Commit,
        stored.operation_id,
        StatementNextAction::ManualInspect,
        format_failure("TRUNCATE reconcile contract is corrupt", &failure),
    ))
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn persist(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    state: OperationState,
    record: TruncateLifecycleRecord,
) -> Result<StoredOperation, DmlError> {
    journal
        .mutate_statement(state, OperationPayload::TruncateLifecycle(record), None)
        .map_err(|error| journal_error(error, stored.operation_id))?;
    Ok(journal.stored.clone())
}

fn planned_record(
    facts: &TruncatePlanFacts,
    connector_operation_id: Uuid,
) -> TruncateLifecycleRecord {
    TruncateLifecycleRecord {
        phase: TruncateLifecyclePhase::Planned,
        connector_operation_id,
        provider_id: Some(facts.provider_id.clone()),
        connector_instance_id: Some(facts.instance_id.clone()),
        connector_incarnation: Some(hex::encode(facts.incarnation)),
        target_ref: facts.target_ref.clone(),
        request_digest: Some(hex::encode(facts.request_digest)),
        plan_digest: Some(hex::encode(facts.plan_digest)),
        state_digest: Some(hex::encode(facts.state_digest)),
        plan_summary: Some(DurableMutationSummary {
            file_count: facts.summary.file_count,
            row_count: facts.summary.row_count,
            total_bytes: facts.summary.total_bytes,
        }),
        outcome: None,
        next_action: StatementNextAction::None,
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn outcome_record(
    stored: &StoredOperation,
    phase: TruncateLifecyclePhase,
    next_action: StatementNextAction,
    outcome: DurableExternalFact,
) -> Result<TruncateLifecycleRecord, DmlError> {
    let mut record = truncate_record(stored)?;
    record.phase = phase;
    record.next_action = next_action;
    record.outcome = Some(outcome);
    Ok(record)
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn outcome_record_without_new_fact(
    stored: &StoredOperation,
    phase: TruncateLifecyclePhase,
    next_action: StatementNextAction,
) -> Result<TruncateLifecycleRecord, DmlError> {
    let mut record = truncate_record(stored)?;
    record.phase = phase;
    record.next_action = next_action;
    Ok(record)
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn truncate_record(stored: &StoredOperation) -> Result<TruncateLifecycleRecord, DmlError> {
    match &stored.payload {
        OperationPayload::TruncateLifecycle(record) => Ok(record.clone()),
        _ => Err(operation_error(
            DmlErrorKind::JournalCorruption,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "durable TRUNCATE operation has the wrong payload kind",
        )),
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn durable_evidence(stored: &StoredOperation) -> Result<Option<String>, DmlError> {
    Ok(truncate_record(stored)?
        .outcome
        .and_then(|outcome| outcome.evidence))
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn preflight_external_truth(
    journal: &mut ActiveDmlOperation,
    stored: &StoredOperation,
) -> Result<(), DmlError> {
    let representative_failure = encode_failure(&TruncateFailure {
        kind: TruncateFailureKind::ResourceExhausted,
        // Control characters maximize JSON escaping within the bounded prefix.
        message: "\0".repeat(DURABLE_FAILURE_MESSAGE_PREFIX_BYTES),
    });

    let mut unknown = stored.clone();
    unknown.state = OperationState::CommitUnknown;
    unknown.payload = OperationPayload::TruncateLifecycle(outcome_record(
        stored,
        TruncateLifecyclePhase::CommitUnknown,
        StatementNextAction::Reconcile,
        DurableExternalFact {
            outcome: ExternalFactOutcome::CommitUnknown,
            receipt: None,
            evidence: Some("00".repeat(DML_EXTERNAL_FACT_ENCODED_LIMIT / 2)),
            finalization_failure: None,
            failure: Some(representative_failure.clone()),
        },
    )?);
    journal.journal.preflight_statement_operation(&unknown)?;

    let mut committed = stored.clone();
    committed.state = OperationState::FinalizeFailedKnownCommitted;
    committed.payload = OperationPayload::TruncateLifecycle(outcome_record(
        stored,
        TruncateLifecyclePhase::Committed,
        StatementNextAction::RetryFinalize,
        DurableExternalFact {
            outcome: ExternalFactOutcome::KnownCommitted,
            // Quotes maximize escaping when the already-encoded receipt string
            // is embedded in the complete operation envelope.
            receipt: Some("\"".repeat(DML_EXTERNAL_FACT_ENCODED_LIMIT)),
            evidence: None,
            finalization_failure: Some(representative_failure),
            failure: None,
        },
    )?);
    journal.journal.preflight_statement_operation(&committed)
}

fn validate_plan_facts(
    connector_operation_id: Uuid,
    stored: &StoredOperation,
    facts: &TruncatePlanFacts,
) -> Result<(), TruncateFailure> {
    let expected_target_ref = match &stored.payload {
        OperationPayload::TruncateLifecycle(record) => record.target_ref.clone(),
        _ => {
            return Err(TruncateFailure {
                kind: TruncateFailureKind::CorruptData,
                message: "durable TRUNCATE operation has the wrong payload kind".to_string(),
            });
        }
    };
    let identity_matches = facts.mutation_operation_id == connector_operation_id.into_bytes()
        && facts.target_ref == expected_target_ref
        && facts.catalog == stored.target.catalog
        && facts.namespace == stored.target.namespace
        && facts.table == stored.target.table
        && !facts.provider_id.is_empty()
        && !facts.instance_id.is_empty();
    if identity_matches {
        Ok(())
    } else {
        Err(TruncateFailure {
            kind: TruncateFailureKind::CorruptData,
            message: "TRUNCATE plan facts do not match the durable statement identity".to_string(),
        })
    }
}

fn validate_receipt(
    facts: &TruncatePlanFacts,
    receipt: &TruncateReceipt,
) -> Result<(), TruncateFailure> {
    let matching = receipt.provider_id == facts.provider_id
        && receipt.instance_id == facts.instance_id
        && receipt.incarnation == facts.incarnation
        && receipt.mutation_operation_id == facts.mutation_operation_id
        && receipt.request_digest == facts.request_digest
        && receipt.plan_digest == facts.plan_digest
        && receipt.state_digest == facts.state_digest
        && receipt.summary == facts.summary
        && receipt.operation_kind == "truncate";
    if !matching {
        return Err(TruncateFailure {
            kind: TruncateFailureKind::CorruptData,
            message: "TRUNCATE committed receipt conflicts with the durable plan facts".to_string(),
        });
    }

    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse(&receipt.provider_id)
            .map_err(receipt_contract_failure)?,
        instance_id: ConnectorInstanceId::parse(&receipt.instance_id)
            .map_err(receipt_contract_failure)?,
    };
    let summary = ConnectorDataMutationPlanSummary::try_new(
        receipt.summary.file_count,
        receipt.summary.row_count,
        receipt.summary.total_bytes,
    )
    .map_err(receipt_contract_failure)?;
    let reconstructed = ConnectorDataMutationReceipt::try_new(
        descriptor,
        ConnectorInstanceIncarnation::from_bytes(receipt.incarnation),
        ConnectorMutationOperationId::from_bytes(receipt.mutation_operation_id),
        receipt.operation_kind.clone(),
        receipt.request_digest,
        receipt.plan_digest,
        receipt.state_digest,
        summary,
        Bytes::from(receipt.opaque_payload.clone()),
    )
    .map_err(receipt_contract_failure)?;
    if reconstructed.provider_payload_digest() != receipt.opaque_payload_digest {
        return Err(TruncateFailure {
            kind: TruncateFailureKind::CorruptData,
            message: "TRUNCATE committed receipt payload digest is invalid".to_string(),
        });
    }
    Ok(())
}

fn receipt_contract_failure(error: impl std::fmt::Display) -> TruncateFailure {
    TruncateFailure {
        kind: TruncateFailureKind::CorruptData,
        message: format!("invalid SPI TRUNCATE committed receipt: {error}"),
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn validate_evidence(
    operation_id: DmlOperationId,
    facts: &TruncatePlanFacts,
    evidence: &TruncateEvidence,
) -> Result<(), DmlError> {
    let decoded =
        ExternalMutationEvidence::try_from_wire_v1(&evidence.wire_bytes).map_err(|error| {
            operation_error(
                DmlErrorKind::Commit,
                operation_id,
                StatementNextAction::ManualInspect,
                format!("invalid SPI TRUNCATE evidence wire: {error}"),
            )
        })?;
    let matching = decoded.schema_version() == evidence.schema_version
        && decoded.digest() == evidence.digest
        && decoded.descriptor().provider_id.as_str() == facts.provider_id
        && decoded.descriptor().instance_id.as_str() == facts.instance_id
        && decoded.incarnation().to_bytes() == facts.incarnation
        && decoded.operation_id().to_bytes() == facts.mutation_operation_id
        && decoded.operation_kind() == "truncate";
    if matching {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Commit,
            operation_id,
            StatementNextAction::ManualInspect,
            "TRUNCATE commit-unknown evidence conflicts with the durable plan facts",
        ))
    }
}

fn encode_receipt(effect: TruncateEffect, receipt: &TruncateReceipt) -> Result<String, String> {
    let encoded = serde_json::to_string(&DurableTruncateReceiptV1 {
        version: DURABLE_TRUNCATE_RECEIPT_VERSION,
        effect: match effect {
            TruncateEffect::Applied => "APPLIED",
            TruncateEffect::NoOp => "NO_OP",
        },
        provider_id: &receipt.provider_id,
        instance_id: &receipt.instance_id,
        incarnation: hex::encode(receipt.incarnation),
        mutation_operation_id: hex::encode(receipt.mutation_operation_id),
        operation_kind: &receipt.operation_kind,
        request_digest: hex::encode(receipt.request_digest),
        plan_digest: hex::encode(receipt.plan_digest),
        state_digest: hex::encode(receipt.state_digest),
        summary: DurableMutationSummary {
            file_count: receipt.summary.file_count,
            row_count: receipt.summary.row_count,
            total_bytes: receipt.summary.total_bytes,
        },
        opaque_payload: hex::encode(&receipt.opaque_payload),
        opaque_payload_digest: hex::encode(receipt.opaque_payload_digest),
    })
    .map_err(|error| format!("failed to encode durable TRUNCATE receipt: {error}"))?;
    ensure_external_fact_bound("TRUNCATE receipt", &encoded)?;
    Ok(encoded)
}

fn encode_failure(failure: &TruncateFailure) -> String {
    let original_message_bytes = failure.message.len();
    let mut prefix_end = original_message_bytes.min(DURABLE_FAILURE_MESSAGE_PREFIX_BYTES);
    while !failure.message.is_char_boundary(prefix_end) {
        prefix_end -= 1;
    }
    let message_prefix = &failure.message[..prefix_end];
    let original_message_sha256 = hex::encode(Sha256::digest(failure.message.as_bytes()));
    let encoded = serde_json::to_string(&DurableTruncateFailureV1 {
        version: 1,
        kind: failure_kind(failure.kind),
        message_prefix,
        message_truncated: prefix_end < original_message_bytes,
        original_message_bytes,
        original_message_sha256,
    })
    .unwrap_or_else(|_| {
        r#"{"version":1,"kind":"INTERNAL","message_prefix":"failure encoding failed","message_truncated":true,"original_message_bytes":0,"original_message_sha256":""}"#.to_string()
    });
    debug_assert!(encoded.len() <= DML_EXTERNAL_FACT_ENCODED_LIMIT);
    encoded
}

/// Encode opaque SPI evidence for the journal without understanding it.
/// Lowercase hex is canonical and preserves the exact wire bytes.
pub fn encode_truncate_evidence_hex(evidence: &TruncateEvidence) -> Result<String, String> {
    let decoded = ExternalMutationEvidence::try_from_wire_v1(&evidence.wire_bytes)
        .map_err(|error| format!("invalid SPI TRUNCATE evidence wire: {error}"))?;
    if decoded.schema_version() != evidence.schema_version || decoded.digest() != evidence.digest {
        return Err(
            "TRUNCATE evidence schema version or digest does not match its wire bytes".to_string(),
        );
    }
    let encoded = hex::encode(&evidence.wire_bytes);
    ensure_external_fact_bound("TRUNCATE evidence", &encoded)?;
    let decoded = decode_truncate_evidence_hex(&encoded)?;
    if decoded != evidence.wire_bytes {
        return Err("durable TRUNCATE evidence did not round-trip losslessly".to_string());
    }
    Ok(encoded)
}

/// Decode journaled evidence bytes for recovery. SPI remains responsible for
/// parsing and validating the wire schema, identity and semantic digest.
pub fn decode_truncate_evidence_hex(encoded: &str) -> Result<Vec<u8>, String> {
    ensure_external_fact_bound("TRUNCATE evidence", encoded)?;
    let decoded = hex::decode(encoded)
        .map_err(|error| format!("invalid durable TRUNCATE evidence hex: {error}"))?;
    if hex::encode(&decoded) != encoded {
        return Err("durable TRUNCATE evidence is not canonical lowercase hex".to_string());
    }
    Ok(decoded)
}

fn ensure_external_fact_bound(label: &str, encoded: &str) -> Result<(), String> {
    if encoded.len() > DML_EXTERNAL_FACT_ENCODED_LIMIT {
        Err(format!(
            "{label} exceeds encoded limit {DML_EXTERNAL_FACT_ENCODED_LIMIT}"
        ))
    } else {
        Ok(())
    }
}

fn syntactic_target(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
    target_ref: &str,
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
        ref_name: (target_ref != "main").then(|| target_ref.to_string()),
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

fn fact_outcome(failure: &TruncateFailure) -> ExternalFactOutcome {
    match failure.kind {
        TruncateFailureKind::Unsupported => ExternalFactOutcome::Unsupported,
        TruncateFailureKind::Conflict => ExternalFactOutcome::Conflict,
        _ => ExternalFactOutcome::KnownUncommitted,
    }
}

fn format_failure(prefix: &str, failure: &TruncateFailure) -> String {
    format!(
        "{prefix}: {}: {}",
        failure_kind(failure.kind),
        failure.message
    )
}

const fn failure_kind(kind: TruncateFailureKind) -> &'static str {
    match kind {
        TruncateFailureKind::InvalidRequest => "INVALID_REQUEST",
        TruncateFailureKind::NotFound => "NOT_FOUND",
        TruncateFailureKind::AlreadyExists => "ALREADY_EXISTS",
        TruncateFailureKind::Conflict => "CONFLICT",
        TruncateFailureKind::Unauthenticated => "UNAUTHENTICATED",
        TruncateFailureKind::PermissionDenied => "PERMISSION_DENIED",
        TruncateFailureKind::Unsupported => "UNSUPPORTED",
        TruncateFailureKind::Cancelled => "CANCELLED",
        TruncateFailureKind::DeadlineExceeded => "DEADLINE_EXCEEDED",
        TruncateFailureKind::ResourceExhausted => "RESOURCE_EXHAUSTED",
        TruncateFailureKind::Unavailable => "UNAVAILABLE",
        TruncateFailureKind::CorruptData => "CORRUPT_DATA",
        TruncateFailureKind::Internal => "INTERNAL",
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorMutationOperationId, ConnectorProviderId,
    };

    use super::*;

    fn evidence(payload: &'static [u8]) -> TruncateEvidence {
        let evidence = ExternalMutationEvidence::try_new(
            1,
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            },
            ConnectorInstanceIncarnation::from_bytes([1; 16]),
            ConnectorMutationOperationId::from_bytes([2; 16]),
            "truncate",
            Bytes::from_static(payload),
        )
        .unwrap();
        TruncateEvidence {
            schema_version: evidence.schema_version(),
            digest: evidence.digest(),
            wire_bytes: evidence.try_to_wire_v1().unwrap().to_vec(),
        }
    }

    #[test]
    fn evidence_hex_codec_requires_one_canonical_lossless_representation() {
        let evidence = evidence(b"opaque");
        let encoded = encode_truncate_evidence_hex(&evidence).unwrap();
        assert_eq!(encoded, hex::encode(&evidence.wire_bytes));
        assert_eq!(
            decode_truncate_evidence_hex(&encoded).unwrap(),
            evidence.wire_bytes
        );
        assert!(decode_truncate_evidence_hex("00ABFF").is_err());
    }

    #[test]
    fn syntactic_target_preserves_branch_without_resolving_a_connector() {
        assert_eq!(
            syntactic_target(
                &["ice".into(), "db".into(), "orders".into()],
                Some("ignored"),
                "ignored",
                "audit",
            ),
            OperationTarget {
                catalog: "ice".into(),
                namespace: "db".into(),
                table: "orders".into(),
                ref_name: Some("audit".into()),
            }
        );
    }
}
