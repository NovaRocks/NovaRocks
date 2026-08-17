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

//! Frontend-owned `ALTER TABLE ... ADD FILES FROM ...` lifecycle.
//!
//! The frontend owns statement identity, durable public artifacts and source
//! ownership.  The opaque core handle owns exactly one admitted connector
//! session; it is never recreated after a durable plan or evidence barrier.

use novarocks::query_execution::dml::add_files::{
    AddFilesCommand, AddFilesDispatchState, AddFilesEffect, AddFilesEngine, AddFilesEvidence,
    AddFilesFailure, AddFilesFailureKind, AddFilesFinalization, AddFilesOutcome, AddFilesPlanError,
    AddFilesPlanFacts, AddFilesReceipt, PlanAddFilesRequest, PreparedAddFiles,
};
use novarocks::query_execution::request_context::RequestContext;
use novarocks_protocol::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorDataMutationSourceScopeKind, ExternalMutationEvidence, REGISTER_EXISTING_FILES_KIND,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dml::coordination::ActiveDmlOperation;
use crate::dml::error::{DmlError, DmlErrorKind};
#[cfg(test)]
use crate::dml::journal::OperationJournal;
use crate::dml::model::{
    AddFilesArtifact, AddFilesArtifactDescriptor, AddFilesArtifactKind, AddFilesDispatchCertainty,
    AddFilesLifecyclePhase, AddFilesLifecycleRecord, AddFilesMutationRequest, AddFilesSourceAction,
    CreateStatementOperationRequest, DmlDirectMutationKind, DmlOperationId, DurableExternalFact,
    DurableMutationSummary, ExternalFactOutcome, OperationKind, OperationMutationRequest,
    OperationPayload, OperationState, OperationTarget, SourceScopeOwnership, StatementNextAction,
    StoredOperation,
};
use crate::dml::service::DmlService;

const ADD_FILES_ARTIFACT_CODEC_VERSION: u16 = 1;

impl DmlService {
    /// Recognize and execute one ADD FILES statement through the frontend
    /// application owner.  `Ok(None)` means this is not ADD FILES; all errors
    /// after classification are terminal for the SQL router.
    pub fn try_execute_add_files(
        &self,
        engine: &dyn AddFilesEngine,
        sql: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<Option<u32>, DmlError> {
        let Some(command) = engine.classify_add_files(sql).map_err(DmlError::executor)? else {
            return Ok(None);
        };

        if !is_secret_free_source_location(&command.location) {
            return Err(DmlError::executor(
                "ADD FILES source location must not contain credentials or query parameters",
            ));
        }

        let operation_id = DmlOperationId::new_v7();
        let connector_operation_id = Uuid::now_v7();
        let session = context.session();
        let initial = AddFilesLifecycleRecord {
            phase: AddFilesLifecyclePhase::Preparing,
            connector_operation_id,
            provider_id: None,
            connector_instance_id: None,
            connector_incarnation: None,
            source_location: command.location.clone(),
            source_scope_version: None,
            source_scope_kind: None,
            source_scope_digest: None,
            request_digest: None,
            plan_digest: None,
            state_digest: None,
            plan_summary: None,
            plan_artifact: None,
            receipt_artifact: None,
            evidence_artifact: None,
            dispatch_certainty: AddFilesDispatchCertainty::ConfirmedNotDispatched,
            source_ownership: SourceScopeOwnership::Unclaimed,
            outcome: None,
            next_action: StatementNextAction::None,
        };
        let mut active = self
            .begin_statement_operation(CreateStatementOperationRequest {
                operation_id,
                mutation_id: Uuid::now_v7(),
                operation_kind: OperationKind::AddFiles,
                target: syntactic_target(
                    &command.table_parts,
                    session.current_catalog(),
                    session.current_database(),
                ),
                attempt_id: operation_id.to_string(),
                payload: OperationPayload::AddFilesLifecycle(initial),
                created_at_ms: crate::dml::now_unix_millis(),
            })
            .map_err(|error| journal_error(error, operation_id))?;

        let result = execute_add_files_operation(
            engine,
            context,
            query_options,
            command,
            connector_operation_id,
            &mut active,
        );
        let _ = active.release();
        result.map(Some)
    }
}

fn execute_add_files_operation(
    engine: &dyn AddFilesEngine,
    context: &RequestContext,
    query_options: Option<&QueryOptions>,
    command: AddFilesCommand,
    connector_operation_id: Uuid,
    active: &mut ActiveDmlOperation,
) -> Result<u32, DmlError> {
    let session = context.session();
    let mut stored = active.stored.clone();

    active.check_before_dispatch()?;
    let prepared = match engine.plan_add_files(PlanAddFilesRequest {
        command,
        current_catalog: session.current_catalog().map(ToOwned::to_owned),
        current_database: session.current_database().to_string(),
        mutation_operation_id: connector_operation_id.into_bytes(),
        query_options: query_options.cloned(),
        execution: context.execution().clone(),
    }) {
        Ok(prepared) => prepared,
        Err(error) => return finish_plan_failure(active, stored, error),
    };

    if let Err(failure) = validate_plan_facts(connector_operation_id, &stored, &prepared.facts) {
        return persist_known_uncommitted(active, stored, failure);
    }
    let plan_artifact = artifact(
        AddFilesArtifactKind::Plan,
        prepared.facts.public_plan_wire.clone(),
    )?;
    let planned = planned_record(
        &prepared.facts,
        connector_operation_id,
        plan_artifact.descriptor.clone(),
    );
    let reserve = AddFilesMutationRequest {
        operation: OperationMutationRequest {
            operation_id: stored.operation_id,
            expected_revision: stored.revision,
            mutation_id: Uuid::now_v7(),
            state: OperationState::Committing,
            payload: OperationPayload::AddFilesLifecycle(planned.clone()),
        },
        artifacts: vec![plan_artifact],
        source_action: Some(AddFilesSourceAction::Reserve {
            provider_id: prepared.facts.provider_id.clone(),
            scope_digest: scope_digest(&prepared.facts),
            ownership: SourceScopeOwnership::ReservedImmutable,
        }),
    };
    active
        .journal
        .preflight_add_files_mutation(&reserve)
        .map_err(|error| journal_error(error, stored.operation_id))?;
    stored = apply(active, reserve)?;

    // This second durable write is deliberately distinct from reservation:
    // the plan and ownership are visible before execution is admitted.
    let executing = AddFilesLifecycleRecord {
        phase: AddFilesLifecyclePhase::Executing,
        ..planned
    };
    stored = apply(
        active,
        mutation(
            stored,
            OperationState::Committing,
            executing,
            Vec::new(),
            None,
        ),
    )?;

    active.check_before_dispatch()?;

    // Establish the external fence before registering any file. ADD FILES
    // brings external files under the table's ownership, so a superseded
    // owner's late execute has to be refused at the catalog rather than
    // reported once the files are already claimed. ADD FILES always targets
    // main; it has no branch-qualified form.
    stored = establish_and_record_fence(engine, active, stored, &prepared)?;

    finish_outcome(
        engine,
        active,
        stored,
        &prepared,
        engine.execute_add_files(prepared.handle.as_ref()),
        true,
    )
}

/// Establish this attempt's external fence and durably journal its receipt,
/// before any file is registered.
///
/// Ordering, in full: mint the proposal from the *live* lease guard, refuse a
/// receipt this journal could never hold, ask the provider to publish the
/// marker, double-check that it acknowledged exactly the fence that was sealed,
/// then persist the receipt through the fenced journal. The record binds this
/// statement's immutable source scope, so a later owner can prove the fence was
/// minted for the very source set it is reasoning about.
fn establish_and_record_fence(
    engine: &dyn AddFilesEngine,
    active: &mut ActiveDmlOperation,
    stored: StoredOperation,
    prepared: &PreparedAddFiles,
) -> Result<StoredOperation, DmlError> {
    let proposal = active.external_fence()?;
    let source_scope_digest = scope_digest(&prepared.facts);
    active.preflight_direct_mutation_fence(
        &proposal,
        DmlDirectMutationKind::AddFiles,
        Some(source_scope_digest.clone()),
    )?;
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
            novarocks_spi::connector::ConnectorWriteTargetRef::main(),
        )
        .map_err(DmlError::executor)?;
    let receipt = engine
        .establish_add_files_external_fence(prepared.handle.as_ref(), fence.clone())
        .map_err(DmlError::executor)?;
    proposal.validate_established_receipt(&fence, &receipt)?;
    let record = crate::dml::reconcile::direct_mutation_fence_receipt_record(
        DmlDirectMutationKind::AddFiles,
        &fence,
        &receipt,
        Some(source_scope_digest),
    )
    .map_err(DmlError::journal_corruption)?;
    let recovery_due_at_ms = stored.recovery_due_at_ms;
    active
        .record_direct_mutation_fence(record, recovery_due_at_ms)
        .map_err(|error| journal_error(error, stored.operation_id))?;
    Ok(active.stored.clone())
}

fn finish_plan_failure(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    error: AddFilesPlanError,
) -> Result<u32, DmlError> {
    match error {
        AddFilesPlanError::KnownUncommitted(failure) => {
            persist_known_uncommitted(journal, stored, failure)
        }
        AddFilesPlanError::ContractFailure { failure, dispatch } => {
            persist_contract_failure(journal, stored, failure, dispatch)
        }
    }
}

fn finish_outcome(
    engine: &dyn AddFilesEngine,
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    prepared: &PreparedAddFiles,
    outcome: AddFilesOutcome,
    allow_reconcile: bool,
) -> Result<u32, DmlError> {
    match outcome {
        AddFilesOutcome::KnownCommitted {
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
        AddFilesOutcome::KnownUncommitted { failure } => {
            persist_known_uncommitted(journal, stored, failure)
        }
        AddFilesOutcome::ContractFailure { failure, dispatch } => {
            persist_contract_failure(journal, stored, failure, dispatch)
        }
        AddFilesOutcome::CommitUnknown { failure, evidence } => persist_unknown_then_reconcile(
            engine,
            journal,
            stored,
            prepared,
            failure,
            evidence,
            allow_reconcile,
        ),
    }
}

fn persist_unknown_then_reconcile(
    engine: &dyn AddFilesEngine,
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    prepared: &PreparedAddFiles,
    failure: AddFilesFailure,
    evidence: AddFilesEvidence,
    allow_reconcile: bool,
) -> Result<u32, DmlError> {
    if let Err(error) = validate_evidence(stored.operation_id, &prepared.facts, &evidence) {
        return persist_frozen_manual(
            journal,
            stored,
            failure_corrupt(format!(
                "invalid ADD FILES commit-unknown evidence: {error}"
            )),
            None,
        );
    }
    let mut record = add_files_record(&stored)?;
    let first_evidence = record.evidence_artifact.clone();
    let evidence_artifact =
        match artifact(AddFilesArtifactKind::Evidence, evidence.wire_bytes.clone()) {
            Ok(artifact) => artifact,
            Err(error) => {
                return persist_frozen_manual(
                    journal,
                    stored,
                    failure_corrupt(format!(
                        "cannot retain ADD FILES commit-unknown evidence: {error}"
                    )),
                    None,
                );
            }
        };
    if let Some(descriptor) = first_evidence.as_ref() {
        let existing = journal
            .journal
            .load_add_files_artifact(stored.operation_id, descriptor)
            .map_err(|error| journal_error(error, stored.operation_id))?;
        if existing.bytes != evidence.wire_bytes {
            return persist_frozen_manual(
                journal,
                stored,
                failure_corrupt(
                    "ADD FILES reconcile returned evidence different from durable evidence",
                ),
                None,
            );
        }
    }
    record.phase = AddFilesLifecyclePhase::CommitUnknown;
    record.evidence_artifact = Some(
        first_evidence
            .clone()
            .unwrap_or_else(|| evidence_artifact.descriptor.clone()),
    );
    record.dispatch_certainty = AddFilesDispatchCertainty::PossiblyDispatched;
    record.source_ownership = SourceScopeOwnership::Frozen;
    record.next_action = if allow_reconcile {
        StatementNextAction::Reconcile
    } else {
        StatementNextAction::ManualInspect
    };
    record.outcome = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::CommitUnknown,
        receipt: None,
        evidence: record.evidence_artifact.as_ref().map(artifact_reference),
        finalization_failure: None,
        failure: Some(encode_failure(&failure)),
    });
    let source_action = match first_evidence {
        Some(_) => None,
        None => Some(AddFilesSourceAction::Transition {
            provider_id: prepared.facts.provider_id.clone(),
            scope_digest: scope_digest(&prepared.facts),
            expected: SourceScopeOwnership::ReservedImmutable,
            ownership: SourceScopeOwnership::Frozen,
        }),
    };
    let artifacts = if source_action.is_some() {
        vec![evidence_artifact]
    } else {
        Vec::new()
    };
    let stored = apply(
        journal,
        mutation(
            stored,
            OperationState::CommitUnknown,
            record,
            artifacts,
            source_action,
        ),
    )?;
    if !allow_reconcile {
        return Err(operation_error(
            DmlErrorKind::Commit,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            format_failure("ADD FILES remains commit-unknown after reconcile", &failure),
        ));
    }
    let mut reconciling = add_files_record(&stored)?;
    reconciling.phase = AddFilesLifecyclePhase::Reconciling;
    reconciling.next_action = StatementNextAction::Reconcile;
    let stored = apply(
        journal,
        mutation(
            stored,
            OperationState::CommitUnknown,
            reconciling,
            Vec::new(),
            None,
        ),
    )?;
    journal.check_before_dispatch()?;
    let reconciled = engine.reconcile_add_files(prepared.handle.as_ref(), &evidence);
    finish_outcome(engine, journal, stored, prepared, reconciled, false)
}

fn persist_known_committed(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    facts: &AddFilesPlanFacts,
    _effect: AddFilesEffect,
    receipt: AddFilesReceipt,
    finalization: AddFilesFinalization,
) -> Result<u32, DmlError> {
    if let Err(error) = validate_receipt(facts, &receipt) {
        return persist_committed_manual(journal, stored, facts, error.to_string());
    }
    let receipt_artifact = match artifact(
        AddFilesArtifactKind::Receipt,
        receipt.public_receipt_wire.clone(),
    ) {
        Ok(artifact) => artifact,
        Err(error) => return persist_committed_manual(journal, stored, facts, error.to_string()),
    };
    let mut record = add_files_record(&stored)?;
    record.phase = AddFilesLifecyclePhase::Committed;
    record.receipt_artifact = Some(receipt_artifact.descriptor.clone());
    record.dispatch_certainty = AddFilesDispatchCertainty::PossiblyDispatched;
    record.source_ownership = SourceScopeOwnership::TableOwned;
    record.next_action = match finalization {
        AddFilesFinalization::Complete => StatementNextAction::None,
        AddFilesFinalization::Failed(_) => StatementNextAction::RetryFinalize,
    };
    record.outcome = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::KnownCommitted,
        receipt: Some(artifact_reference(&receipt_artifact.descriptor)),
        evidence: record.evidence_artifact.as_ref().map(artifact_reference),
        finalization_failure: match &finalization {
            AddFilesFinalization::Complete => None,
            AddFilesFinalization::Failed(failure) => Some(encode_failure(failure)),
        },
        failure: None,
    });
    let expected_ownership = source_ownership(&stored)?;
    let stored = apply(
        journal,
        mutation(
            stored,
            OperationState::Committed,
            record,
            vec![receipt_artifact],
            Some(AddFilesSourceAction::Transition {
                provider_id: facts.provider_id.clone(),
                scope_digest: scope_digest(facts),
                expected: expected_ownership,
                ownership: SourceScopeOwnership::TableOwned,
            }),
        ),
    )?;
    match finalization {
        AddFilesFinalization::Complete => {
            let record = add_files_record(&stored)?;
            let _stored = apply(
                journal,
                mutation(stored, OperationState::Finalized, record, Vec::new(), None),
            )?;
            Ok(receipt.summary.file_count)
        }
        AddFilesFinalization::Failed(failure) => {
            let record = add_files_record(&stored)?;
            let stored = apply(
                journal,
                mutation(stored, OperationState::Finalizing, record, Vec::new(), None),
            )?;
            let record = add_files_record(&stored)?;
            let stored = apply(
                journal,
                mutation(
                    stored,
                    OperationState::FinalizeFailedKnownCommitted,
                    record,
                    Vec::new(),
                    None,
                ),
            )?;
            Err(operation_error(
                DmlErrorKind::CommittedButUnfinalized,
                stored.operation_id,
                StatementNextAction::RetryFinalize,
                format_failure("ADD FILES committed but finalization failed", &failure),
            ))
        }
    }
}

/// A confirmed provider commit transfers the scope to the table even if the
/// frontend cannot retain its receipt.  Record that ownership truth and stop
/// at manual inspection; releasing or freezing this source would be false.
fn persist_committed_manual(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    facts: &AddFilesPlanFacts,
    message: String,
) -> Result<u32, DmlError> {
    let expected_ownership = source_ownership(&stored)?;
    let mut record = add_files_record(&stored)?;
    record.phase = AddFilesLifecyclePhase::Committed;
    record.source_ownership = SourceScopeOwnership::TableOwned;
    record.dispatch_certainty = AddFilesDispatchCertainty::PossiblyDispatched;
    record.next_action = StatementNextAction::ManualInspect;
    record.outcome = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::KnownCommitted,
        receipt: None,
        evidence: record.evidence_artifact.as_ref().map(artifact_reference),
        finalization_failure: None,
        failure: Some(message.clone()),
    });
    let stored = apply(
        journal,
        mutation(
            stored,
            OperationState::Committed,
            record,
            Vec::new(),
            Some(AddFilesSourceAction::Transition {
                provider_id: facts.provider_id.clone(),
                scope_digest: scope_digest(facts),
                expected: expected_ownership,
                ownership: SourceScopeOwnership::TableOwned,
            }),
        ),
    )?;
    let record = add_files_record(&stored)?;
    let stored = apply(
        journal,
        mutation(stored, OperationState::Finalizing, record, Vec::new(), None),
    )?;
    let record = add_files_record(&stored)?;
    let stored = apply(
        journal,
        mutation(
            stored,
            OperationState::FinalizeFailedKnownCommitted,
            record,
            Vec::new(),
            None,
        ),
    )?;
    Err(operation_error(
        DmlErrorKind::CommittedButUnfinalized,
        stored.operation_id,
        StatementNextAction::ManualInspect,
        format!("ADD FILES is known committed but its durable receipt is invalid: {message}"),
    ))
}

fn persist_known_uncommitted(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    failure: AddFilesFailure,
) -> Result<u32, DmlError> {
    let mut record = add_files_record(&stored)?;
    let source_action = if record.source_ownership == SourceScopeOwnership::ReservedImmutable {
        let provider_id = record
            .provider_id
            .clone()
            .ok_or_else(|| wrong_record(&stored))?;
        let scope_digest = record
            .source_scope_digest
            .clone()
            .ok_or_else(|| wrong_record(&stored))?;
        record.source_ownership = SourceScopeOwnership::Unclaimed;
        Some(AddFilesSourceAction::Release {
            provider_id,
            scope_digest,
        })
    } else {
        None
    };
    record.phase = AddFilesLifecyclePhase::Failed;
    record.next_action = StatementNextAction::None;
    record.outcome = Some(DurableExternalFact {
        outcome: fact_outcome(&failure),
        receipt: None,
        evidence: record.evidence_artifact.as_ref().map(artifact_reference),
        finalization_failure: None,
        failure: Some(encode_failure(&failure)),
    });
    let stored = apply(
        journal,
        mutation(
            stored,
            OperationState::FailedKnownUncommitted,
            record,
            Vec::new(),
            source_action,
        ),
    )?;
    Err(operation_error(
        DmlErrorKind::Executor,
        stored.operation_id,
        StatementNextAction::None,
        format_failure("ADD FILES is known uncommitted", &failure),
    ))
}

fn persist_contract_failure(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    failure: AddFilesFailure,
    dispatch: AddFilesDispatchState,
) -> Result<u32, DmlError> {
    match dispatch {
        AddFilesDispatchState::ConfirmedNotDispatched => {
            persist_known_uncommitted(journal, stored, failure)
        }
        AddFilesDispatchState::PossiblyDispatched => {
            persist_frozen_manual(journal, stored, failure, None)
        }
    }
}

fn persist_frozen_manual(
    journal: &mut ActiveDmlOperation,
    stored: StoredOperation,
    failure: AddFilesFailure,
    evidence: Option<AddFilesArtifact>,
) -> Result<u32, DmlError> {
    let mut record = add_files_record(&stored)?;
    let has_scope = record.source_scope_digest.is_some();
    let source_action =
        if has_scope && record.source_ownership == SourceScopeOwnership::ReservedImmutable {
            record.source_ownership = SourceScopeOwnership::Frozen;
            Some(AddFilesSourceAction::Transition {
                provider_id: record
                    .provider_id
                    .clone()
                    .ok_or_else(|| wrong_record(&stored))?,
                scope_digest: record
                    .source_scope_digest
                    .clone()
                    .ok_or_else(|| wrong_record(&stored))?,
                expected: SourceScopeOwnership::ReservedImmutable,
                ownership: SourceScopeOwnership::Frozen,
            })
        } else {
            record.source_ownership = SourceScopeOwnership::Frozen;
            None
        };
    if let Some(evidence) = evidence.as_ref() {
        record.evidence_artifact = Some(evidence.descriptor.clone());
    }
    record.phase = AddFilesLifecyclePhase::Failed;
    record.dispatch_certainty = AddFilesDispatchCertainty::PossiblyDispatched;
    record.next_action = StatementNextAction::ManualInspect;
    record.outcome = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::CommitUnknown,
        receipt: None,
        evidence: record.evidence_artifact.as_ref().map(artifact_reference),
        finalization_failure: None,
        failure: Some(encode_failure(&failure)),
    });
    let stored = apply(
        journal,
        mutation(
            stored,
            OperationState::CommitUnknown,
            record,
            evidence.into_iter().collect(),
            source_action,
        ),
    )?;
    Err(operation_error(
        DmlErrorKind::Commit,
        stored.operation_id,
        StatementNextAction::ManualInspect,
        format_failure("ADD FILES dispatch may have occurred", &failure),
    ))
}

fn apply(
    journal: &mut ActiveDmlOperation,
    request: AddFilesMutationRequest,
) -> Result<StoredOperation, DmlError> {
    let operation_id = request.operation.operation_id;
    journal
        .apply_add_files_mutation(request, None)
        .map_err(|error| journal_error(error, operation_id))?;
    Ok(journal.stored.clone())
}

fn mutation(
    stored: StoredOperation,
    state: OperationState,
    record: AddFilesLifecycleRecord,
    artifacts: Vec<AddFilesArtifact>,
    source_action: Option<AddFilesSourceAction>,
) -> AddFilesMutationRequest {
    AddFilesMutationRequest {
        operation: OperationMutationRequest {
            operation_id: stored.operation_id,
            expected_revision: stored.revision,
            mutation_id: Uuid::now_v7(),
            state,
            payload: OperationPayload::AddFilesLifecycle(record),
        },
        artifacts,
        source_action,
    }
}

fn planned_record(
    facts: &AddFilesPlanFacts,
    connector_operation_id: Uuid,
    plan_artifact: AddFilesArtifactDescriptor,
) -> AddFilesLifecycleRecord {
    AddFilesLifecycleRecord {
        phase: AddFilesLifecyclePhase::Planned,
        connector_operation_id,
        provider_id: Some(facts.provider_id.clone()),
        connector_instance_id: Some(facts.instance_id.clone()),
        connector_incarnation: Some(hex::encode(facts.incarnation)),
        source_location: facts.source_location.clone(),
        source_scope_version: Some(facts.source_scope.version()),
        source_scope_kind: Some(match facts.source_scope.kind() {
            ConnectorDataMutationSourceScopeKind::Directory => "DIRECTORY".to_string(),
        }),
        source_scope_digest: Some(scope_digest(facts)),
        request_digest: Some(hex::encode(facts.request_digest)),
        plan_digest: Some(hex::encode(facts.plan_digest)),
        state_digest: Some(hex::encode(facts.state_digest)),
        plan_summary: Some(DurableMutationSummary {
            file_count: facts.summary.file_count,
            row_count: facts.summary.row_count,
            total_bytes: facts.summary.total_bytes,
        }),
        plan_artifact: Some(plan_artifact),
        receipt_artifact: None,
        evidence_artifact: None,
        dispatch_certainty: AddFilesDispatchCertainty::ConfirmedNotDispatched,
        source_ownership: SourceScopeOwnership::ReservedImmutable,
        outcome: None,
        next_action: StatementNextAction::None,
    }
}

fn artifact(kind: AddFilesArtifactKind, bytes: Vec<u8>) -> Result<AddFilesArtifact, DmlError> {
    let total_length = u32::try_from(bytes.len())
        .map_err(|_| DmlError::journal_unavailable("ADD FILES artifact exceeds u32 length"))?;
    if bytes.is_empty() {
        return Err(DmlError::journal_corruption(
            "ADD FILES durable artifact is empty",
        ));
    }
    // StateStore uses an 8 KiB chunk; keep this local descriptor calculation in
    // lockstep with its bounded artifact contract without exposing a store type.
    let chunk_count = u16::try_from(bytes.len().div_ceil(8 * 1024))
        .map_err(|_| DmlError::journal_unavailable("ADD FILES artifact has too many chunks"))?;
    Ok(AddFilesArtifact {
        descriptor: AddFilesArtifactDescriptor {
            kind,
            codec_version: ADD_FILES_ARTIFACT_CODEC_VERSION,
            total_length,
            chunk_count,
            sha256: hex::encode(Sha256::digest(&bytes)),
        },
        bytes,
    })
}

fn validate_plan_facts(
    connector_operation_id: Uuid,
    stored: &StoredOperation,
    facts: &AddFilesPlanFacts,
) -> Result<(), AddFilesFailure> {
    let expected_source = add_files_record(stored)
        .map_err(|error| failure_corrupt(error.to_string()))?
        .source_location;
    let identity_matches = facts.mutation_operation_id == connector_operation_id.into_bytes()
        && facts.source_location == expected_source
        && facts.catalog == stored.target.catalog
        && facts.namespace == stored.target.namespace
        && facts.table == stored.target.table
        && !facts.provider_id.is_empty()
        && !facts.instance_id.is_empty()
        && !facts.public_plan_wire.is_empty()
        && facts.source_scope.validate().is_ok();
    if identity_matches {
        Ok(())
    } else {
        Err(failure_corrupt(
            "ADD FILES plan facts do not match the durable statement identity",
        ))
    }
}

fn validate_receipt(facts: &AddFilesPlanFacts, receipt: &AddFilesReceipt) -> Result<(), DmlError> {
    let matching = receipt.provider_id == facts.provider_id
        && receipt.instance_id == facts.instance_id
        && receipt.incarnation == facts.incarnation
        && receipt.mutation_operation_id == facts.mutation_operation_id
        && receipt.request_digest == facts.request_digest
        && receipt.plan_digest == facts.plan_digest
        && receipt.state_digest == facts.state_digest
        && receipt.summary == facts.summary
        && receipt.operation_kind == REGISTER_EXISTING_FILES_KIND
        && !receipt.public_receipt_wire.is_empty();
    if matching {
        Ok(())
    } else {
        Err(DmlError::journal_corruption(
            "ADD FILES committed receipt conflicts with durable plan facts",
        ))
    }
}

fn validate_evidence(
    operation_id: DmlOperationId,
    facts: &AddFilesPlanFacts,
    evidence: &AddFilesEvidence,
) -> Result<(), DmlError> {
    let decoded =
        ExternalMutationEvidence::try_from_wire_v1(&evidence.wire_bytes).map_err(|error| {
            operation_error(
                DmlErrorKind::Commit,
                operation_id,
                StatementNextAction::ManualInspect,
                format!("invalid SPI ADD FILES evidence wire: {error}"),
            )
        })?;
    let matching = decoded.schema_version() == evidence.schema_version
        && decoded.digest() == evidence.digest
        && decoded.descriptor().provider_id.as_str() == facts.provider_id
        && decoded.descriptor().instance_id.as_str() == facts.instance_id
        && decoded.incarnation().to_bytes() == facts.incarnation
        && decoded.operation_id().to_bytes() == facts.mutation_operation_id
        && decoded.operation_kind() == REGISTER_EXISTING_FILES_KIND;
    if !matching {
        return Err(operation_error(
            DmlErrorKind::Commit,
            operation_id,
            StatementNextAction::ManualInspect,
            "ADD FILES evidence conflicts with the durable plan facts",
        ));
    }
    Ok(())
}

fn add_files_record(stored: &StoredOperation) -> Result<AddFilesLifecycleRecord, DmlError> {
    match &stored.payload {
        OperationPayload::AddFilesLifecycle(record) => Ok(record.clone()),
        _ => Err(wrong_record(stored)),
    }
}

fn source_ownership(stored: &StoredOperation) -> Result<SourceScopeOwnership, DmlError> {
    Ok(add_files_record(stored)?.source_ownership)
}

fn scope_digest(facts: &AddFilesPlanFacts) -> String {
    hex::encode(facts.source_scope.digest())
}

fn artifact_reference(descriptor: &AddFilesArtifactDescriptor) -> String {
    format!("sha256:{}", descriptor.sha256)
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

fn is_secret_free_source_location(location: &str) -> bool {
    // User info and URI query parameters are the two grammar-level places
    // where credentials/signed URLs can occur.  Do not reject path segments
    // such as `.../secret-data/`: their spelling alone carries no credential
    // semantics and would turn a durable-safety check into an incompatibility.
    !location.contains('@') && !location.contains('?')
}

fn journal_error(error: DmlError, operation_id: DmlOperationId) -> DmlError {
    operation_error(
        error.kind(),
        operation_id,
        StatementNextAction::ManualInspect,
        error,
    )
}

fn wrong_record(stored: &StoredOperation) -> DmlError {
    operation_error(
        DmlErrorKind::JournalCorruption,
        stored.operation_id,
        StatementNextAction::ManualInspect,
        "durable ADD FILES operation has the wrong payload kind",
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

fn fact_outcome(failure: &AddFilesFailure) -> ExternalFactOutcome {
    match failure.kind {
        AddFilesFailureKind::Unsupported => ExternalFactOutcome::Unsupported,
        AddFilesFailureKind::Conflict => ExternalFactOutcome::Conflict,
        _ => ExternalFactOutcome::KnownUncommitted,
    }
}

fn failure_corrupt(message: impl Into<String>) -> AddFilesFailure {
    AddFilesFailure {
        kind: AddFilesFailureKind::CorruptData,
        message: message.into(),
    }
}

fn encode_failure(failure: &AddFilesFailure) -> String {
    format!("{}: {}", failure_kind(failure.kind), failure.message)
}

fn format_failure(prefix: &str, failure: &AddFilesFailure) -> String {
    format!("{prefix}: {}", encode_failure(failure))
}

const fn failure_kind(kind: AddFilesFailureKind) -> &'static str {
    match kind {
        AddFilesFailureKind::InvalidRequest => "INVALID_REQUEST",
        AddFilesFailureKind::NotFound => "NOT_FOUND",
        AddFilesFailureKind::AlreadyExists => "ALREADY_EXISTS",
        AddFilesFailureKind::Conflict => "CONFLICT",
        AddFilesFailureKind::Unauthenticated => "UNAUTHENTICATED",
        AddFilesFailureKind::PermissionDenied => "PERMISSION_DENIED",
        AddFilesFailureKind::Unsupported => "UNSUPPORTED",
        AddFilesFailureKind::Cancelled => "CANCELLED",
        AddFilesFailureKind::DeadlineExceeded => "DEADLINE_EXCEEDED",
        AddFilesFailureKind::ResourceExhausted => "RESOURCE_EXHAUSTED",
        AddFilesFailureKind::Unavailable => "UNAVAILABLE",
        AddFilesFailureKind::CorruptData => "CORRUPT_DATA",
        AddFilesFailureKind::Internal => "INTERNAL",
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks::query_execution::backend::BackendTopologySnapshot;
    use novarocks::query_execution::cancellation::QueryCancellationSource;
    use novarocks::query_execution::dml::add_files::{AddFilesCommand, AddFilesPrepared};
    use novarocks::query_execution::request_context::{RequestAdmission, RequestContext};
    use novarocks_spi::connector::{
        ConnectorDataMutationPlanSummary, ConnectorDataMutationReceipt,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorMutationOperationId, ConnectorProviderId,
    };
    use novarocks_types::ClusterRole;

    use super::*;
    use crate::dml::model::{DML_OPERATION_SCHEMA_VERSION, validate_operation_transition};

    #[derive(Clone)]
    enum Behavior {
        Committed,
        KnownUncommitted,
        Unknown,
        PossiblyDispatched,
    }

    struct FakePrepared;

    impl AddFilesPrepared for FakePrepared {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct FakeEngine {
        execute_behavior: Behavior,
        reconcile_behavior: Behavior,
        classify_calls: AtomicUsize,
        plan_calls: AtomicUsize,
        execute_calls: AtomicUsize,
        reconcile_calls: AtomicUsize,
        plan_is_durable: Arc<AtomicBool>,
        evidence_is_durable: Arc<AtomicBool>,
        facts: Mutex<Option<AddFilesPlanFacts>>,
    }

    impl FakeEngine {
        fn new(execute_behavior: Behavior, reconcile_behavior: Behavior) -> Self {
            Self {
                execute_behavior,
                reconcile_behavior,
                classify_calls: AtomicUsize::new(0),
                plan_calls: AtomicUsize::new(0),
                execute_calls: AtomicUsize::new(0),
                reconcile_calls: AtomicUsize::new(0),
                plan_is_durable: Arc::new(AtomicBool::new(false)),
                evidence_is_durable: Arc::new(AtomicBool::new(false)),
                facts: Mutex::new(None),
            }
        }

        fn counts(&self) -> (usize, usize, usize, usize) {
            (
                self.classify_calls.load(Ordering::SeqCst),
                self.plan_calls.load(Ordering::SeqCst),
                self.execute_calls.load(Ordering::SeqCst),
                self.reconcile_calls.load(Ordering::SeqCst),
            )
        }
    }

    impl AddFilesEngine for FakeEngine {
        /// Acknowledge the sealed fence the way a provider does: with a receipt
        /// that names exactly this fence. What these tests exercise is the
        /// establish-before-dispatch ordering, the frontend double check, and
        /// the journalled receipt around it.
        fn establish_add_files_external_fence(
            &self,
            _prepared: &dyn novarocks::query_execution::dml::add_files::AddFilesPrepared,
            fence: novarocks_spi::connector::ConnectorExternalOperationFence,
        ) -> Result<
            novarocks_spi::connector::ConnectorExternalFenceReceipt,
            novarocks_spi::connector::ConnectorError,
        > {
            novarocks_spi::connector::ConnectorExternalFenceReceipt::try_new(
                &fence,
                Bytes::from_static(b"add-files-fence-marker"),
            )
        }

        fn classify_add_files(&self, sql: &str) -> Result<Option<AddFilesCommand>, String> {
            self.classify_calls.fetch_add(1, Ordering::SeqCst);
            Ok((sql == "ADD").then(|| AddFilesCommand {
                table_parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()],
                location: "s3://bucket/source".to_string(),
            }))
        }

        fn plan_add_files(
            &self,
            request: PlanAddFilesRequest,
        ) -> Result<PreparedAddFiles, AddFilesPlanError> {
            self.plan_calls.fetch_add(1, Ordering::SeqCst);
            let facts = facts(request.mutation_operation_id);
            *self.facts.lock().unwrap() = Some(facts.clone());
            Ok(PreparedAddFiles {
                facts,
                handle: Arc::new(FakePrepared),
            })
        }

        fn execute_add_files(&self, _prepared: &dyn AddFilesPrepared) -> AddFilesOutcome {
            assert!(
                self.plan_is_durable.load(Ordering::SeqCst),
                "execute must not happen before the public plan and reservation are durable"
            );
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            behavior_outcome(
                &self.execute_behavior,
                self.facts.lock().unwrap().as_ref().unwrap(),
            )
        }

        fn reconcile_add_files(
            &self,
            _prepared: &dyn AddFilesPrepared,
            _evidence: &AddFilesEvidence,
        ) -> AddFilesOutcome {
            assert!(
                self.evidence_is_durable.load(Ordering::SeqCst),
                "reconcile must not happen before commit-unknown evidence is durable"
            );
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            behavior_outcome(
                &self.reconcile_behavior,
                self.facts.lock().unwrap().as_ref().unwrap(),
            )
        }
    }

    fn facts(operation_id: [u8; 16]) -> AddFilesPlanFacts {
        AddFilesPlanFacts {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            source_location: "s3://bucket/source".to_string(),
            provider_id: "iceberg".to_string(),
            instance_id: "ice".to_string(),
            incarnation: [0x11; 16],
            mutation_operation_id: operation_id,
            request_digest: [0x22; 32],
            plan_digest: [0x33; 32],
            state_digest: [0x44; 32],
            summary: novarocks::query_execution::dml::add_files::AddFilesPlanSummary {
                file_count: 3,
                row_count: 7,
                total_bytes: 101,
            },
            source_scope:
                novarocks_spi::connector::ConnectorDataMutationSourceScope::try_new_directory(
                    [0x55; 32],
                )
                .unwrap(),
            public_plan_wire: b"public-plan".to_vec(),
        }
    }

    fn behavior_outcome(behavior: &Behavior, facts: &AddFilesPlanFacts) -> AddFilesOutcome {
        match behavior {
            Behavior::Committed => AddFilesOutcome::KnownCommitted {
                effect: AddFilesEffect::Applied,
                receipt: receipt(facts),
                finalization: AddFilesFinalization::Complete,
            },
            Behavior::KnownUncommitted => AddFilesOutcome::KnownUncommitted {
                failure: failure(AddFilesFailureKind::Conflict),
            },
            Behavior::Unknown => AddFilesOutcome::CommitUnknown {
                failure: failure(AddFilesFailureKind::Unavailable),
                evidence: evidence(facts),
            },
            Behavior::PossiblyDispatched => AddFilesOutcome::ContractFailure {
                failure: failure(AddFilesFailureKind::Unavailable),
                dispatch: AddFilesDispatchState::PossiblyDispatched,
            },
        }
    }

    fn receipt(facts: &AddFilesPlanFacts) -> AddFilesReceipt {
        let payload = Bytes::from_static(b"receipt");
        let receipt = ConnectorDataMutationReceipt::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(&facts.provider_id).unwrap(),
                instance_id: ConnectorInstanceId::parse(&facts.instance_id).unwrap(),
            },
            ConnectorInstanceIncarnation::from_bytes(facts.incarnation),
            ConnectorMutationOperationId::from_bytes(facts.mutation_operation_id),
            REGISTER_EXISTING_FILES_KIND,
            facts.request_digest,
            facts.plan_digest,
            facts.state_digest,
            ConnectorDataMutationPlanSummary::try_new(
                facts.summary.file_count,
                facts.summary.row_count,
                facts.summary.total_bytes,
            )
            .unwrap(),
            payload,
        )
        .unwrap();
        AddFilesReceipt {
            provider_id: facts.provider_id.clone(),
            instance_id: facts.instance_id.clone(),
            incarnation: facts.incarnation,
            mutation_operation_id: facts.mutation_operation_id,
            operation_kind: REGISTER_EXISTING_FILES_KIND.to_string(),
            request_digest: facts.request_digest,
            plan_digest: facts.plan_digest,
            state_digest: facts.state_digest,
            summary: facts.summary,
            public_receipt_wire: receipt.try_to_wire_v1().unwrap().to_vec(),
        }
    }

    fn evidence(facts: &AddFilesPlanFacts) -> AddFilesEvidence {
        let evidence = ExternalMutationEvidence::try_new(
            1,
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(&facts.provider_id).unwrap(),
                instance_id: ConnectorInstanceId::parse(&facts.instance_id).unwrap(),
            },
            ConnectorInstanceIncarnation::from_bytes(facts.incarnation),
            ConnectorMutationOperationId::from_bytes(facts.mutation_operation_id),
            REGISTER_EXISTING_FILES_KIND,
            Bytes::from_static(b"evidence"),
        )
        .unwrap();
        AddFilesEvidence {
            schema_version: evidence.schema_version(),
            digest: evidence.digest(),
            wire_bytes: evidence.try_to_wire_v1().unwrap().to_vec(),
        }
    }

    fn failure(kind: AddFilesFailureKind) -> AddFilesFailure {
        AddFilesFailure {
            kind,
            message: "synthetic failure".to_string(),
        }
    }

    #[derive(Default)]
    struct FakeJournal {
        operations: Mutex<BTreeMap<Uuid, StoredOperation>>,
        artifacts: Mutex<Vec<(Uuid, AddFilesArtifact)>>,
        history: Mutex<Vec<StoredOperation>>,
        plan_is_durable: Arc<AtomicBool>,
        evidence_is_durable: Arc<AtomicBool>,
        preflight_calls: AtomicUsize,
        direct_mutation_fences: Mutex<Vec<crate::dml::model::DmlDirectMutationFenceReceiptRecord>>,
    }

    impl FakeJournal {
        fn only_operation(&self) -> StoredOperation {
            let operations = self.operations.lock().unwrap();
            assert_eq!(operations.len(), 1);
            operations.values().next().unwrap().clone()
        }

        fn recorded_direct_mutation_fences(
            &self,
        ) -> Vec<crate::dml::model::DmlDirectMutationFenceReceiptRecord> {
            self.direct_mutation_fences.lock().unwrap().clone()
        }
    }

    impl OperationJournal for FakeJournal {
        fn create_preparing(
            &self,
            _request: crate::dml::CreatePreparingRequest,
        ) -> Result<DmlOperationId, DmlError> {
            panic!("ADD FILES must use statement journal API")
        }
        fn transition(
            &self,
            _operation_id: DmlOperationId,
            _to: OperationState,
        ) -> Result<(), DmlError> {
            panic!("ADD FILES must use atomic journal API")
        }
        fn record_fact(
            &self,
            _operation_id: DmlOperationId,
            _fact: crate::dml::OperationFact,
        ) -> Result<(), DmlError> {
            panic!("ADD FILES must use atomic journal API")
        }
        fn load(&self, operation_id: DmlOperationId) -> Result<Option<StoredOperation>, DmlError> {
            Ok(self
                .operations
                .lock()
                .unwrap()
                .get(operation_id.as_uuid())
                .cloned())
        }
        fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
            Ok(self.operations.lock().unwrap().values().cloned().collect())
        }
        fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError> {
            Ok(self
                .operations
                .lock()
                .unwrap()
                .values()
                .filter(|operation| !operation.state.is_finished())
                .cloned()
                .collect())
        }
        /// The coordinated path admits and validates inside the journal
        /// transaction. This fake has no transaction to do that inside; the
        /// transactional guarantees are covered by the StateStore journal
        /// tests. Here these only need to let a coordinated operation proceed
        /// so ADD FILES routing runs under a real fence.
        fn create_statement_operation_admitted(
            &self,
            request: CreateStatementOperationRequest,
            _admission: Arc<dyn crate::dml::journal::DmlIntentAdmissionValidator>,
        ) -> Result<StoredOperation, DmlError> {
            self.create_statement_operation(request)
        }

        fn claim_operation_admitted(
            &self,
            request: crate::dml::model::DmlCoordinationClaimRequest,
            _admission: Arc<dyn crate::dml::journal::DmlIntentAdmissionValidator>,
            _authority: crate::dml::journal::DmlMutationAuthority,
        ) -> Result<StoredOperation, DmlError> {
            Ok(self
                .load(request.operation_id)?
                .expect("claimed DML operation must exist in this fake journal"))
        }

        fn mutate_statement_operation_authorized(
            &self,
            request: OperationMutationRequest,
            _recovery_due_at_ms: Option<i64>,
            _authority: crate::dml::journal::DmlMutationAuthority,
        ) -> Result<StoredOperation, DmlError> {
            self.mutate_statement_operation(request)
        }

        fn create_statement_operation(
            &self,
            request: CreateStatementOperationRequest,
        ) -> Result<StoredOperation, DmlError> {
            let stored = StoredOperation {
                schema_version: DML_OPERATION_SCHEMA_VERSION,
                operation_id: request.operation_id,
                revision: 1,
                last_mutation_id: request.mutation_id,
                operation_kind: request.operation_kind,
                operation_subkind: None,
                target: request.target,
                state: OperationState::Preparing,
                attempt_id: request.attempt_id,
                base_snapshot_id: None,
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: Vec::new(),
                payload: request.payload,
                coordination_provenance: None,
                recovery_due_at_ms: None,
                created_at_ms: request.created_at_ms,
                updated_at_ms: request.created_at_ms,
                finished_at_ms: None,
            };
            self.operations
                .lock()
                .unwrap()
                .insert(*stored.operation_id.as_uuid(), stored.clone());
            self.history.lock().unwrap().push(stored.clone());
            Ok(stored)
        }
        fn preflight_add_files_mutation(
            &self,
            _request: &AddFilesMutationRequest,
        ) -> Result<(), DmlError> {
            self.preflight_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn preflight_direct_mutation_fence(
            &self,
            request: &crate::dml::model::DmlDirectMutationFenceMutationRequest,
        ) -> Result<(), DmlError> {
            crate::dml::model::validate_direct_mutation_fence_receipt(&request.fence)
                .map_err(DmlError::journal_corruption)
        }

        fn record_direct_mutation_fence_authorized(
            &self,
            request: crate::dml::model::DmlDirectMutationFenceMutationRequest,
            _recovery_due_at_ms: Option<i64>,
            _authority: crate::dml::journal::DmlMutationAuthority,
        ) -> Result<StoredOperation, DmlError> {
            crate::dml::model::validate_direct_mutation_fence_receipt(&request.fence)
                .map_err(DmlError::journal_corruption)?;
            let mut operations = self.operations.lock().unwrap();
            let operation = operations
                .get_mut(request.operation_id.as_uuid())
                .expect("fenced DML operation must exist in this fake journal");
            assert_eq!(operation.revision, request.expected_revision);
            operation.revision += 1;
            operation.last_mutation_id = request.mutation_id;
            self.direct_mutation_fences
                .lock()
                .unwrap()
                .push(request.fence);
            Ok(operation.clone())
        }
        fn apply_add_files_mutation_authorized(
            &self,
            request: AddFilesMutationRequest,
            _recovery_due_at_ms: Option<i64>,
            _authority: crate::dml::journal::DmlMutationAuthority,
        ) -> Result<StoredOperation, DmlError> {
            self.apply_add_files_mutation(request)
        }

        fn apply_add_files_mutation(
            &self,
            request: AddFilesMutationRequest,
        ) -> Result<StoredOperation, DmlError> {
            let mut operations = self.operations.lock().unwrap();
            let operation = operations
                .get_mut(request.operation.operation_id.as_uuid())
                .unwrap();
            assert_eq!(operation.revision, request.operation.expected_revision);
            validate_operation_transition(operation.state, request.operation.state).unwrap();
            for artifact in request.artifacts {
                self.artifacts
                    .lock()
                    .unwrap()
                    .push((*request.operation.operation_id.as_uuid(), artifact));
            }
            operation.revision += 1;
            operation.last_mutation_id = request.operation.mutation_id;
            operation.state = request.operation.state;
            operation.payload = request.operation.payload;
            operation.updated_at_ms += 1;
            if operation.state.is_finished() {
                operation.finished_at_ms = Some(operation.updated_at_ms);
            }
            let stored = operation.clone();
            if let OperationPayload::AddFilesLifecycle(record) = &stored.payload {
                if record.plan_artifact.is_some()
                    && record.source_ownership == SourceScopeOwnership::ReservedImmutable
                {
                    self.plan_is_durable.store(true, Ordering::SeqCst);
                }
                if record.evidence_artifact.is_some()
                    && record.source_ownership == SourceScopeOwnership::Frozen
                {
                    self.evidence_is_durable.store(true, Ordering::SeqCst);
                }
            }
            drop(operations);
            self.history.lock().unwrap().push(stored.clone());
            Ok(stored)
        }
        fn load_add_files_artifact(
            &self,
            operation_id: DmlOperationId,
            descriptor: &AddFilesArtifactDescriptor,
        ) -> Result<AddFilesArtifact, DmlError> {
            self.artifacts
                .lock()
                .unwrap()
                .iter()
                .find(|(id, artifact)| {
                    id == operation_id.as_uuid() && artifact.descriptor == *descriptor
                })
                .map(|(_, artifact)| artifact.clone())
                .ok_or_else(|| DmlError::journal_corruption("missing artifact"))
        }
    }

    /// One runtime for the whole test binary: a per-harness runtime would be
    /// dropped with the service that borrowed it.
    fn shared_runtime() -> &'static tokio::runtime::Runtime {
        static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("test runtime")
        })
    }

    /// Real coordination over a temporary SQLite StateStore.
    ///
    /// Dispatch is fenced now, and a fence can only be minted from a live
    /// coordination lease, so a service composed without coordination cannot
    /// dispatch at all. These tests therefore stand up the genuine coordination
    /// runtime rather than reaching for a test-only fence -- the seam that
    /// would have given them one also disables the guard asserting that an
    /// operation without authority cannot dispatch.
    fn coordination() -> Arc<crate::coordination::FrontendCoordinationRuntime> {
        let dir = tempfile::tempdir().expect("temp dir").keep();
        let registry = novarocks_state_store::builtin_state_store_provider_registry()
            .expect("provider registry");
        let runtime = shared_runtime();
        let host = runtime
            .block_on(novarocks_state_store::StateStoreHost::open(
                &registry,
                novarocks_state_store::StateStoreHostConfig {
                    state_store: novarocks_state_store::StateStoreAppConfig {
                        store: novarocks_state_store::StateStoreConfig {
                            cluster_id: "add-files-focused-test".to_string(),
                            limits: novarocks_state_store::StateStoreLimitOverrides::default(),
                            provider: novarocks_state_store::StateStoreProviderConfig::Sqlite {
                                path: dir.join("state.sqlite"),
                                deployment_owner: "add-files-fe".to_string(),
                            },
                        },
                        mysql_client: None,
                    },
                    foundationdb_client: None,
                },
                novarocks_spi::state_store::FeDeploymentView {
                    active_fe_count: std::num::NonZeroUsize::new(1).unwrap(),
                    topology_revision: bytes::Bytes::from_static(b"add-files-topology"),
                },
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            ))
            .expect("open state store host");
        let store = host.state_store().expect("StateStore exposure");
        let coordination = runtime
            .block_on(crate::coordination::FrontendCoordinationRuntime::open(
                store,
            ))
            .expect("open frontend coordination");
        // The host owns the database; keep it alive for the process.
        std::mem::forget(host);
        Arc::new(coordination)
    }

    fn harness(engine: &mut FakeEngine) -> (DmlService, Arc<FakeJournal>) {
        let journal = Arc::new(FakeJournal::default());
        engine.plan_is_durable = Arc::clone(&journal.plan_is_durable);
        engine.evidence_is_durable = Arc::clone(&journal.evidence_is_durable);
        (
            DmlService::compose_with_coordination(
                Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
                Arc::new(crate::statistics::FrontendStatisticsService::new()),
                coordination(),
                shared_runtime().handle().clone(),
            ),
            journal,
        )
    }

    fn context() -> RequestContext {
        let cancellation = QueryCancellationSource::new();
        RequestContext::admit(RequestAdmission::new(
            Some("ice".to_string()),
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(1),
            Some(Instant::now() + Duration::from_secs(30)),
            cancellation.view(),
            Default::default(),
        ))
    }

    #[test]
    fn non_add_files_returns_none_without_journal_or_engine_side_effects() {
        let mut engine = FakeEngine::new(Behavior::Committed, Behavior::Committed);
        let (service, journal) = harness(&mut engine);
        assert_eq!(
            service
                .try_execute_add_files(&engine, "SELECT 1", &context(), None)
                .unwrap(),
            None
        );
        assert_eq!(engine.counts(), (1, 0, 0, 0));
        assert!(journal.history.lock().unwrap().is_empty());
    }

    #[test]
    fn success_persists_public_plan_and_receipt_and_executes_once() {
        let mut engine = FakeEngine::new(Behavior::Committed, Behavior::Committed);
        let (service, journal) = harness(&mut engine);
        assert_eq!(
            service
                .try_execute_add_files(&engine, "ADD", &context(), None)
                .unwrap(),
            Some(3)
        );
        assert_eq!(engine.counts(), (1, 1, 1, 0));
        assert_eq!(journal.preflight_calls.load(Ordering::SeqCst), 1);
        let operation = journal.only_operation();
        let OperationPayload::AddFilesLifecycle(record) = operation.payload else {
            panic!("ADD FILES payload")
        };
        assert_eq!(operation.state, OperationState::Finalized);
        assert_eq!(record.source_ownership, SourceScopeOwnership::TableOwned);
        assert!(record.plan_artifact.is_some());
        assert!(record.receipt_artifact.is_some());

        // The fence the provider acknowledged must be durable before any file
        // is registered, and an ADD FILES fence must bind the immutable source
        // scope it was minted for.
        let fences = journal.recorded_direct_mutation_fences();
        assert_eq!(fences.len(), 1, "one fence receipt per ADD FILES attempt");
        assert_eq!(fences[0].operation_kind, DmlDirectMutationKind::AddFiles);
        assert_eq!(
            fences[0].source_scope_digest.as_deref(),
            record.source_scope_digest.as_deref()
        );
    }

    #[test]
    fn unknown_evidence_is_durable_before_one_reconcile() {
        let mut engine = FakeEngine::new(Behavior::Unknown, Behavior::Committed);
        let (service, journal) = harness(&mut engine);
        assert_eq!(
            service
                .try_execute_add_files(&engine, "ADD", &context(), None)
                .unwrap(),
            Some(3)
        );
        assert_eq!(engine.counts(), (1, 1, 1, 1));
        let operation = journal.only_operation();
        let OperationPayload::AddFilesLifecycle(record) = operation.payload else {
            panic!("ADD FILES payload")
        };
        assert_eq!(operation.state, OperationState::Finalized);
        assert!(record.evidence_artifact.is_some());
        assert_eq!(record.source_ownership, SourceScopeOwnership::TableOwned);
    }

    #[test]
    fn a_second_unknown_stays_frozen_and_never_retries_execution_or_reconcile() {
        let mut engine = FakeEngine::new(Behavior::Unknown, Behavior::Unknown);
        let (service, journal) = harness(&mut engine);
        let error = service
            .try_execute_add_files(&engine, "ADD", &context(), None)
            .unwrap_err();
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::ManualInspect)
        );
        assert_eq!(engine.counts(), (1, 1, 1, 1));
        let operation = journal.only_operation();
        let OperationPayload::AddFilesLifecycle(record) = operation.payload else {
            panic!("ADD FILES payload")
        };
        assert_eq!(operation.state, OperationState::CommitUnknown);
        assert_eq!(record.source_ownership, SourceScopeOwnership::Frozen);
        assert_eq!(record.next_action, StatementNextAction::ManualInspect);
    }

    #[test]
    fn possibly_dispatched_failure_freezes_and_does_not_fallback() {
        let mut engine = FakeEngine::new(Behavior::PossiblyDispatched, Behavior::Committed);
        let (service, journal) = harness(&mut engine);
        let error = service
            .try_execute_add_files(&engine, "ADD", &context(), None)
            .unwrap_err();
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::ManualInspect)
        );
        assert_eq!(engine.counts(), (1, 1, 1, 0));
        let operation = journal.only_operation();
        let OperationPayload::AddFilesLifecycle(record) = operation.payload else {
            panic!("ADD FILES payload")
        };
        assert_eq!(record.source_ownership, SourceScopeOwnership::Frozen);
        assert_eq!(record.next_action, StatementNextAction::ManualInspect);
    }

    #[test]
    fn known_uncommitted_releases_reserved_scope() {
        let mut engine = FakeEngine::new(Behavior::KnownUncommitted, Behavior::Committed);
        let (service, journal) = harness(&mut engine);
        let error = service
            .try_execute_add_files(&engine, "ADD", &context(), None)
            .unwrap_err();
        assert_eq!(error.next_action(), Some(StatementNextAction::None));
        assert_eq!(engine.counts(), (1, 1, 1, 0));
        let operation = journal.only_operation();
        let OperationPayload::AddFilesLifecycle(record) = operation.payload else {
            panic!("ADD FILES payload")
        };
        assert_eq!(operation.state, OperationState::FailedKnownUncommitted);
        assert_eq!(record.source_ownership, SourceScopeOwnership::Unclaimed);
    }
}
