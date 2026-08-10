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

use novarocks::engine::ctas_engine::{
    CtasEngine, CtasFailure, CtasFailureKind, CtasTargetFacts, CtasTargetPrecheck,
    CtasTargetPrepareOutcome, CtasWriteOutcome, PrepareCtasSourceRequest, PreparedCtasSource,
    PreparedCtasTarget, PreparedCtasWrite,
};
use novarocks::query_execution::request_context::RequestContext;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationOperationId, ConnectorStagedCreateAbortOutcome,
    ConnectorStagedCreatePublishOutcome, ConnectorStagedCreateReceipt,
    ConnectorStagedCreateReceiptPhase, ConnectorStagedCreateReconcileOutcome,
    ConnectorStagedCreateReconcilePhase, ConnectorWriteOperationCompletion,
    ConnectorWriteOperationId, CreatePolicy, ExternalMutationEvidence,
    ExternalMutationFinalization,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dml::error::{DmlError, DmlErrorKind};
use crate::dml::journal::OperationJournal;
use crate::dml::model::{
    CTAS_CREATE_POLICY_FAIL_IF_EXISTS, CTAS_CREATE_POLICY_NO_OP_IF_EXISTS,
    CreateStatementOperationRequest, CtasSagaPhase, CtasSagaRecord, DML_CTAS_FACT_ENCODED_LIMIT,
    DmlOperationId, DurableExternalFact, ExternalFactOutcome, OperationKind,
    OperationMutationRequest, OperationPayload, OperationState, OperationTarget,
    StatementNextAction, StoredOperation,
};
use crate::dml::service::DmlService;

const DURABLE_CTAS_FACT_VERSION: u8 = 1;
const DURABLE_FAILURE_PREFIX_BYTES: usize = 2 * 1024;

#[derive(Clone, Copy)]
enum AbortDisposition {
    SuccessNoOp { retry_finalize: bool },
    Error(CtasSagaPhase),
}

#[derive(Serialize)]
struct DurableCtasReceiptV1<'a> {
    version: u8,
    phase: &'static str,
    effect: &'static str,
    instance_id: &'a str,
    incarnation: String,
    operation_id: String,
    provider_payload_bytes: usize,
    provider_payload_sha256: String,
}

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
        let precheck = engine.precheck_ctas_target(
            &command,
            session.current_catalog(),
            session.current_database(),
        );
        if matches!(precheck, Ok(CtasTargetPrecheck::ExistsNoOp)) {
            return Ok(Some(()));
        }

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
        let journal = self
            .require_journal()
            .map_err(|error| journal_error(error, operation_id))?;
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
            aggregate_write_digest: None,
            prepare_fact: None,
            write_fact: None,
            publish_fact: None,
            abort_staging_fact: None,
            next_action: StatementNextAction::None,
        };
        let mut stored = journal
            .create_statement_operation(CreateStatementOperationRequest {
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

        if let Err(failure) = precheck {
            return finish_source_failure(journal, stored, failure).map(|()| Some(()));
        }

        let source = match engine.prepare_ctas_source(PrepareCtasSourceRequest {
            command,
            current_catalog: session.current_catalog().map(ToOwned::to_owned),
            current_database: session.current_database().to_string(),
            query_options: query_options.cloned(),
            execution: context.execution().clone(),
        }) {
            Ok(source) => source,
            Err(failure) => {
                return finish_source_failure(journal, stored, failure).map(|()| Some(()));
            }
        };
        if let Err(error) = validate_source_facts(
            &stored,
            &source,
            session.current_catalog(),
            session.current_database(),
        ) {
            return finish_source_failure(
                journal,
                stored,
                CtasFailure {
                    kind: CtasFailureKind::Internal,
                    message: error.to_string(),
                },
            )
            .map(|()| Some(()));
        }
        let mut record = ctas_record(&stored)?;
        record.phase = CtasSagaPhase::PreparingStagedTable;
        record.source_plan_digest = Some(hex::encode(source.facts.plan_digest));
        record.source_schema_digest = Some(hex::encode(source.facts.schema_digest));
        record.source_execution_identity = Some(hex::encode(source.facts.execution_identity));
        stored = persist(journal, stored, OperationState::Preparing, record)?;
        if let Err(error) = preflight_external_truth(journal, &stored) {
            return finish_source_failure(
                journal,
                stored,
                CtasFailure {
                    kind: CtasFailureKind::Internal,
                    message: format!(
                        "CTAS journal cannot retain worst-case external truth: {error}"
                    ),
                },
            )
            .map(|()| Some(()));
        }

        let outcome = engine.prepare_ctas_target(
            source.handle.as_ref(),
            ConnectorMutationOperationId::from_bytes(*prepare_operation_id.as_bytes()),
            policy,
        );
        match outcome {
            Err(failure) => finish_source_failure(journal, stored, failure),
            Ok(CtasTargetPrepareOutcome::Prepared {
                target,
                receipt,
                finalization,
            }) => finish_prepared_target(
                engine,
                journal,
                stored,
                source,
                target,
                receipt,
                finalization,
            ),
            Ok(CtasTargetPrepareOutcome::Conflict { failure }) => {
                finish_prepare_conflict(journal, stored, failure)
            }
            Ok(CtasTargetPrepareOutcome::KnownUncommitted { failure }) => {
                finish_prepare_known_uncommitted(journal, stored, failure)
            }
            Ok(CtasTargetPrepareOutcome::CommitUnknown {
                target,
                failure,
                evidence,
            }) => finish_prepare_unknown(
                engine,
                journal,
                stored,
                source,
                target,
                failure,
                Some(evidence),
                true,
            ),
            Ok(CtasTargetPrepareOutcome::ContractUnknown { target, failure }) => {
                finish_prepare_unknown(
                    engine, journal, stored, source, target, failure, None, false,
                )
            }
        }?;
        Ok(Some(()))
    }
}

fn finish_source_failure(
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    failure: CtasFailure,
) -> Result<(), DmlError> {
    let unsupported = failure.kind == CtasFailureKind::Unsupported;
    let mut record = ctas_record(&stored)?;
    record.phase = if unsupported {
        CtasSagaPhase::Unsupported
    } else {
        CtasSagaPhase::Failed
    };
    record.prepare_fact = Some(failure_fact(&failure));
    record.next_action = StatementNextAction::None;
    let stored = persist(
        journal,
        stored,
        OperationState::FailedKnownUncommitted,
        record,
    )?;
    Err(operation_error(
        DmlErrorKind::Executor,
        stored.operation_id,
        StatementNextAction::None,
        format_failure(
            "CTAS source/target preparation is known uncommitted",
            &failure,
        ),
    ))
}

fn finish_prepare_conflict(
    journal: &dyn OperationJournal,
    mut stored: StoredOperation,
    failure: CtasFailure,
) -> Result<(), DmlError> {
    let mut record = ctas_record(&stored)?;
    record.prepare_fact = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::Conflict,
        receipt: None,
        evidence: None,
        finalization_failure: None,
        failure: Some(encode_failure(&failure)),
    });
    record.next_action = StatementNextAction::None;
    if record.create_policy == CTAS_CREATE_POLICY_NO_OP_IF_EXISTS {
        record.phase = CtasSagaPhase::NoOp;
        stored = persist(journal, stored, OperationState::Committing, record.clone())?;
        stored = persist(journal, stored, OperationState::Committed, record.clone())?;
        persist(journal, stored, OperationState::Finalized, record)?;
        Ok(())
    } else {
        record.phase = CtasSagaPhase::Conflict;
        let stored = persist(
            journal,
            stored,
            OperationState::FailedKnownUncommitted,
            record,
        )?;
        Err(operation_error(
            DmlErrorKind::Executor,
            stored.operation_id,
            StatementNextAction::None,
            format_failure("CTAS target conflicts with a concurrent creator", &failure),
        ))
    }
}

fn finish_prepare_known_uncommitted(
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    failure: CtasFailure,
) -> Result<(), DmlError> {
    let mut record = ctas_record(&stored)?;
    record.phase = CtasSagaPhase::Failed;
    record.prepare_fact = Some(failure_fact(&failure));
    record.next_action = StatementNextAction::None;
    let stored = persist(
        journal,
        stored,
        OperationState::FailedKnownUncommitted,
        record,
    )?;
    Err(operation_error(
        DmlErrorKind::Executor,
        stored.operation_id,
        StatementNextAction::None,
        format_failure("CTAS staged prepare is known uncommitted", &failure),
    ))
}

#[allow(clippy::too_many_arguments)]
fn finish_prepare_unknown(
    engine: &dyn CtasEngine,
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    source: PreparedCtasSource,
    target: PreparedCtasTarget,
    failure: CtasFailure,
    evidence: Option<ExternalMutationEvidence>,
    allow_reconcile: bool,
) -> Result<(), DmlError> {
    if let Err(error) = validate_target_facts(&stored, &target.facts) {
        return finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PrepareUnknown,
            FactSlot::Prepare,
            error.to_string(),
        );
    }
    let mut record = ctas_record(&stored)?;
    install_target_facts(&mut record, &target.facts)?;
    let durable_evidence = evidence.as_ref().and_then(|evidence| {
        validate_evidence(
            &target.facts,
            record.prepare_operation_id,
            "staged-create-prepare",
            evidence,
        )
        .and_then(|()| encode_evidence(evidence))
        .ok()
    });
    let can_reconcile = allow_reconcile && evidence.is_some() && durable_evidence.is_some();
    record.phase = CtasSagaPhase::PrepareUnknown;
    record.prepare_fact = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::CommitUnknown,
        receipt: None,
        evidence: durable_evidence,
        finalization_failure: None,
        failure: Some(encode_failure(&failure)),
    });
    record.next_action = if can_reconcile {
        StatementNextAction::Reconcile
    } else {
        StatementNextAction::ManualInspect
    };
    let stored = persist(journal, stored, OperationState::CommitUnknown, record)?;
    let Some(evidence) = evidence.filter(|_| can_reconcile) else {
        return Err(unknown_error(
            stored.operation_id,
            "CTAS staged prepare",
            &failure,
        ));
    };
    let stored = reconcile_barrier(journal, stored)?;
    match engine.reconcile_ctas(
        target.handle.as_ref(),
        ConnectorStagedCreateReconcilePhase::Prepare,
        evidence.clone(),
    ) {
        Ok(ConnectorStagedCreateReconcileOutcome::Prepared {
            handle,
            receipt,
            finalization,
        }) => {
            let mut target_facts = target.facts.clone();
            target_facts.handle_digest = Some(handle.digest());
            finish_prepared_target(
                engine,
                journal,
                stored,
                source,
                PreparedCtasTarget {
                    facts: target_facts,
                    handle: target.handle,
                },
                receipt,
                finalization,
            )
        }
        Ok(ConnectorStagedCreateReconcileOutcome::KnownUncommitted { failure }) => {
            finish_prepare_known_uncommitted(journal, stored, mutation_failure(failure))
        }
        Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
            failure,
            evidence: next,
        }) => finish_reconcile_still_unknown(
            journal,
            stored,
            CtasSagaPhase::PrepareUnknown,
            FactSlot::Prepare,
            mutation_failure(failure),
            &evidence,
            &next,
        ),
        Ok(_) => finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PrepareUnknown,
            FactSlot::Prepare,
            "CTAS prepare reconcile returned an outcome for another phase",
        ),
        Err(failure) => finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PrepareUnknown,
            FactSlot::Prepare,
            format_failure("CTAS prepare reconcile contract failed", &failure),
        ),
    }
}

fn finish_prepared_target(
    engine: &dyn CtasEngine,
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    source: PreparedCtasSource,
    target: PreparedCtasTarget,
    receipt: ConnectorStagedCreateReceipt,
    finalization: ExternalMutationFinalization,
) -> Result<(), DmlError> {
    if let Err(error) = validate_target_facts(&stored, &target.facts) {
        return finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PrepareUnknown,
            FactSlot::Prepare,
            error.to_string(),
        );
    }
    if let Err(error) = validate_receipt(
        &target.facts,
        ctas_record(&stored)?.prepare_operation_id,
        ConnectorStagedCreateReceiptPhase::Prepared,
        &receipt,
    ) {
        return finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PrepareUnknown,
            FactSlot::Prepare,
            error.to_string(),
        );
    }
    let mut record = ctas_record(&stored)?;
    install_target_facts(&mut record, &target.facts)?;
    record.phase = CtasSagaPhase::Staged;
    record.prepare_fact = Some(match committed_fact(&receipt, &finalization) {
        Ok(fact) => fact,
        Err(error) => {
            return finish_contract_unknown(
                journal,
                stored,
                CtasSagaPhase::PrepareUnknown,
                FactSlot::Prepare,
                error.to_string(),
            );
        }
    });
    record.next_action = finalization_action(&finalization);
    let stored = persist(journal, stored, OperationState::Writing, record)?;
    if let ExternalMutationFinalization::Failed(failure) = finalization {
        return Err(operation_error(
            DmlErrorKind::CommittedButUnfinalized,
            stored.operation_id,
            StatementNextAction::RetryFinalize,
            format!("CTAS staged prepare is known committed but finalization failed: {failure}"),
        ));
    }
    prepare_and_execute_write(engine, journal, stored, source, target)
}

fn prepare_and_execute_write(
    engine: &dyn CtasEngine,
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    source: PreparedCtasSource,
    target: PreparedCtasTarget,
) -> Result<(), DmlError> {
    let write_id = ctas_record(&stored)?.write_operation_id;
    let prepared = match engine.prepare_ctas_write(
        source.handle.as_ref(),
        target.handle.as_ref(),
        ConnectorWriteOperationId::from_bytes(*write_id.as_bytes()),
    ) {
        Ok(prepared) => prepared,
        Err(failure) => {
            return begin_abort(
                engine,
                journal,
                stored,
                &target,
                None,
                FactSlot::Write,
                failure_fact(&failure),
                AbortDisposition::Error(CtasSagaPhase::Failed),
                format_failure("CTAS write preparation is known uncommitted", &failure),
            );
        }
    };
    if let Err(error) = validate_prepared_write(&stored, &source, &target, &prepared) {
        return begin_abort(
            engine,
            journal,
            stored,
            &target,
            None,
            FactSlot::Write,
            failure_fact(&CtasFailure {
                kind: CtasFailureKind::Internal,
                message: error.to_string(),
            }),
            AbortDisposition::Error(CtasSagaPhase::Failed),
            error.to_string(),
        );
    }
    let mut record = ctas_record(&stored)?;
    record.phase = CtasSagaPhase::Writing;
    record.next_action = StatementNextAction::None;
    let stored = persist(journal, stored, OperationState::Writing, record)?;
    match engine.execute_ctas_write(prepared.handle.as_ref()) {
        CtasWriteOutcome::Completed {
            completion,
            execution_identity,
        } => finish_write_completed(
            engine,
            journal,
            stored,
            source,
            target,
            prepared,
            completion,
            execution_identity,
        ),
        CtasWriteOutcome::KnownUncommitted { failure } => begin_abort(
            engine,
            journal,
            stored,
            &target,
            None,
            FactSlot::Write,
            failure_fact(&failure),
            AbortDisposition::Error(CtasSagaPhase::Failed),
            format_failure("CTAS writer is known uncommitted", &failure),
        ),
        CtasWriteOutcome::CommitUnknown { failure, evidence } => finish_write_unknown(
            engine, journal, stored, source, target, prepared, failure, evidence, true,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_write_completed(
    engine: &dyn CtasEngine,
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    source: PreparedCtasSource,
    target: PreparedCtasTarget,
    prepared: PreparedCtasWrite,
    completion: ConnectorWriteOperationCompletion,
    execution_identity: [u8; 32],
) -> Result<(), DmlError> {
    if let Err(error) = validate_completion(
        &stored,
        &source,
        &target,
        &prepared,
        &completion,
        execution_identity,
    ) {
        return finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::WriteUnknown,
            FactSlot::Write,
            error.to_string(),
        );
    }
    let (write_receipt, cohort_id) = match encode_write_completion(&completion) {
        Ok(encoded) => encoded,
        Err(error) => {
            return finish_contract_unknown(
                journal,
                stored,
                CtasSagaPhase::WriteUnknown,
                FactSlot::Write,
                error.to_string(),
            );
        }
    };
    let mut record = ctas_record(&stored)?;
    record.write_cohort_id = Some(cohort_id);
    record.aggregate_write_digest = Some(hex::encode(completion.aggregate_digest()));
    record.write_fact = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::KnownCommitted,
        receipt: Some(write_receipt),
        evidence: None,
        finalization_failure: None,
        failure: None,
    });
    record.phase = CtasSagaPhase::Publishing;
    record.next_action = StatementNextAction::None;
    let stored = persist(journal, stored, OperationState::Committing, record)?;
    finish_publish(engine, journal, stored, target, completion)
}

#[allow(clippy::too_many_arguments)]
fn finish_write_unknown(
    engine: &dyn CtasEngine,
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    source: PreparedCtasSource,
    target: PreparedCtasTarget,
    prepared: PreparedCtasWrite,
    failure: CtasFailure,
    evidence: ExternalMutationEvidence,
    allow_reconcile: bool,
) -> Result<(), DmlError> {
    let write_id = ctas_record(&stored)?.write_operation_id;
    let durable_evidence =
        validate_evidence(&target.facts, write_id, "ctas-write-staging", &evidence)
            .and_then(|()| encode_evidence(&evidence))
            .ok();
    let can_reconcile = allow_reconcile && durable_evidence.is_some();
    let mut record = ctas_record(&stored)?;
    let first = retained_evidence(&record.write_fact);
    if first
        .as_ref()
        .is_some_and(|first| Some(first) != durable_evidence.as_ref())
    {
        return finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::WriteUnknown,
            FactSlot::Write,
            "CTAS write reconcile returned evidence different from the first durable evidence",
        );
    }
    record.phase = CtasSagaPhase::WriteUnknown;
    record.write_fact = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::CommitUnknown,
        receipt: None,
        evidence: first.or(durable_evidence),
        finalization_failure: None,
        failure: Some(encode_failure(&failure)),
    });
    record.next_action = if can_reconcile {
        StatementNextAction::Reconcile
    } else {
        StatementNextAction::ManualInspect
    };
    let stored = persist(journal, stored, OperationState::CommitUnknown, record)?;
    if !can_reconcile {
        return Err(unknown_error(stored.operation_id, "CTAS writer", &failure));
    }
    let stored = reconcile_barrier(journal, stored)?;
    match engine.reconcile_ctas_write(prepared.handle.as_ref(), evidence.clone()) {
        CtasWriteOutcome::Completed {
            completion,
            execution_identity,
        } => finish_write_completed(
            engine,
            journal,
            stored,
            source,
            target,
            prepared,
            completion,
            execution_identity,
        ),
        CtasWriteOutcome::KnownUncommitted { failure } => begin_abort(
            engine,
            journal,
            stored,
            &target,
            None,
            FactSlot::Write,
            failure_fact(&failure),
            AbortDisposition::Error(CtasSagaPhase::Failed),
            format_failure("CTAS writer reconcile proved uncommitted", &failure),
        ),
        CtasWriteOutcome::CommitUnknown {
            failure,
            evidence: next,
        } => finish_reconcile_still_unknown(
            journal,
            stored,
            CtasSagaPhase::WriteUnknown,
            FactSlot::Write,
            failure,
            &evidence,
            &next,
        ),
    }
}

fn finish_publish(
    engine: &dyn CtasEngine,
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    target: PreparedCtasTarget,
    completion: ConnectorWriteOperationCompletion,
) -> Result<(), DmlError> {
    let publish_id = ctas_record(&stored)?.publish_operation_id;
    match engine.publish_ctas(
        target.handle.as_ref(),
        ConnectorMutationOperationId::from_bytes(*publish_id.as_bytes()),
        completion.clone(),
    ) {
        Err(failure) => finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PublishUnknown,
            FactSlot::Publish,
            format_failure("CTAS publish may have been dispatched", &failure),
        ),
        Ok(ConnectorStagedCreatePublishOutcome::Applied {
            receipt,
            finalization,
        }) => finish_published(journal, stored, &target.facts, receipt, finalization),
        Ok(ConnectorStagedCreatePublishOutcome::NoOp {
            receipt,
            finalization,
        }) => {
            if let Err(error) = validate_receipt(
                &target.facts,
                publish_id,
                ConnectorStagedCreateReceiptPhase::Published,
                &receipt,
            ) {
                return finish_contract_unknown(
                    journal,
                    stored,
                    CtasSagaPhase::PublishUnknown,
                    FactSlot::Publish,
                    error.to_string(),
                );
            }
            let retry_finalize = matches!(finalization, ExternalMutationFinalization::Failed(_));
            let no_op = match no_op_fact(&receipt, &finalization) {
                Ok(fact) => fact,
                Err(error) => {
                    return finish_contract_unknown(
                        journal,
                        stored,
                        CtasSagaPhase::PublishUnknown,
                        FactSlot::Publish,
                        error.to_string(),
                    );
                }
            };
            begin_abort(
                engine,
                journal,
                stored,
                &target,
                Some(completion),
                FactSlot::Publish,
                no_op,
                AbortDisposition::SuccessNoOp { retry_finalize },
                "CTAS publish was a no-op".to_string(),
            )
        }
        Ok(ConnectorStagedCreatePublishOutcome::Conflict { failure }) => begin_abort(
            engine,
            journal,
            stored,
            &target,
            Some(completion),
            FactSlot::Publish,
            mutation_failure_fact(ExternalFactOutcome::Conflict, &failure),
            AbortDisposition::Error(CtasSagaPhase::Conflict),
            format!("CTAS publish conflicted with a concurrent creator: {failure}"),
        ),
        Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted { failure }) => begin_abort(
            engine,
            journal,
            stored,
            &target,
            Some(completion),
            FactSlot::Publish,
            mutation_failure_fact(ExternalFactOutcome::KnownUncommitted, &failure),
            AbortDisposition::Error(CtasSagaPhase::Failed),
            format!("CTAS publish is known uncommitted: {failure}"),
        ),
        Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, evidence }) => {
            finish_publish_unknown(
                engine,
                journal,
                stored,
                target,
                completion,
                mutation_failure(failure),
                evidence,
                true,
            )
        }
    }
}

fn finish_published(
    journal: &dyn OperationJournal,
    mut stored: StoredOperation,
    target_facts: &CtasTargetFacts,
    receipt: ConnectorStagedCreateReceipt,
    finalization: ExternalMutationFinalization,
) -> Result<(), DmlError> {
    let publish_id = ctas_record(&stored)?.publish_operation_id;
    if let Err(error) = validate_receipt(
        target_facts,
        publish_id,
        ConnectorStagedCreateReceiptPhase::Published,
        &receipt,
    ) {
        return finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PublishUnknown,
            FactSlot::Publish,
            error.to_string(),
        );
    }
    let mut record = ctas_record(&stored)?;
    record.phase = CtasSagaPhase::Committed;
    record.publish_fact = Some(match committed_fact(&receipt, &finalization) {
        Ok(fact) => fact,
        Err(error) => {
            return finish_contract_unknown(
                journal,
                stored,
                CtasSagaPhase::PublishUnknown,
                FactSlot::Publish,
                error.to_string(),
            );
        }
    });
    record.next_action = finalization_action(&finalization);
    stored = persist(journal, stored, OperationState::Committed, record.clone())?;
    match finalization {
        ExternalMutationFinalization::Complete => {
            persist(journal, stored, OperationState::Finalized, record)?;
            Ok(())
        }
        ExternalMutationFinalization::Failed(failure) => {
            stored = persist(journal, stored, OperationState::Finalizing, record.clone())?;
            let stored = persist(
                journal,
                stored,
                OperationState::FinalizeFailedKnownCommitted,
                record,
            )?;
            Err(operation_error(
                DmlErrorKind::CommittedButUnfinalized,
                stored.operation_id,
                StatementNextAction::RetryFinalize,
                format!("CTAS publish is known committed but finalization failed: {failure}"),
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_publish_unknown(
    engine: &dyn CtasEngine,
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    target: PreparedCtasTarget,
    completion: ConnectorWriteOperationCompletion,
    failure: CtasFailure,
    evidence: ExternalMutationEvidence,
    allow_reconcile: bool,
) -> Result<(), DmlError> {
    let publish_id = ctas_record(&stored)?.publish_operation_id;
    let durable_evidence = validate_evidence(
        &target.facts,
        publish_id,
        "staged-create-publish",
        &evidence,
    )
    .and_then(|()| encode_evidence(&evidence))
    .ok();
    let can_reconcile = allow_reconcile && durable_evidence.is_some();
    let mut record = ctas_record(&stored)?;
    let first = retained_evidence(&record.publish_fact);
    if first
        .as_ref()
        .is_some_and(|first| Some(first) != durable_evidence.as_ref())
    {
        return finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PublishUnknown,
            FactSlot::Publish,
            "CTAS publish reconcile returned evidence different from the first durable evidence",
        );
    }
    record.phase = CtasSagaPhase::PublishUnknown;
    record.publish_fact = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::CommitUnknown,
        receipt: None,
        evidence: first.or(durable_evidence),
        finalization_failure: None,
        failure: Some(encode_failure(&failure)),
    });
    record.next_action = if can_reconcile {
        StatementNextAction::Reconcile
    } else {
        StatementNextAction::ManualInspect
    };
    let stored = persist(journal, stored, OperationState::CommitUnknown, record)?;
    if !can_reconcile {
        return Err(unknown_error(stored.operation_id, "CTAS publish", &failure));
    }
    let stored = reconcile_barrier(journal, stored)?;
    match engine.reconcile_ctas(
        target.handle.as_ref(),
        ConnectorStagedCreateReconcilePhase::Publish,
        evidence.clone(),
    ) {
        Ok(ConnectorStagedCreateReconcileOutcome::Published {
            receipt,
            finalization,
        }) => finish_published(journal, stored, &target.facts, receipt, finalization),
        Ok(ConnectorStagedCreateReconcileOutcome::KnownUncommitted { failure }) => begin_abort(
            engine,
            journal,
            stored,
            &target,
            Some(completion),
            FactSlot::Publish,
            mutation_failure_fact(ExternalFactOutcome::KnownUncommitted, &failure),
            AbortDisposition::Error(CtasSagaPhase::Failed),
            format!("CTAS publish reconcile proved uncommitted: {failure}"),
        ),
        Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
            failure,
            evidence: next,
        }) => finish_reconcile_still_unknown(
            journal,
            stored,
            CtasSagaPhase::PublishUnknown,
            FactSlot::Publish,
            mutation_failure(failure),
            &evidence,
            &next,
        ),
        Ok(_) => finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PublishUnknown,
            FactSlot::Publish,
            "CTAS publish reconcile returned an outcome for another phase",
        ),
        Err(failure) => finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::PublishUnknown,
            FactSlot::Publish,
            format_failure("CTAS publish reconcile contract failed", &failure),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn begin_abort(
    engine: &dyn CtasEngine,
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    target: &PreparedCtasTarget,
    completion: Option<ConnectorWriteOperationCompletion>,
    slot: FactSlot,
    cause_fact: DurableExternalFact,
    disposition: AbortDisposition,
    cause_message: String,
) -> Result<(), DmlError> {
    let mut record = ctas_record(&stored)?;
    install_fact(&mut record, slot, cause_fact);
    record.phase = CtasSagaPhase::AbortingStaging;
    record.next_action = StatementNextAction::AbortStaging;
    let stored = persist(journal, stored, OperationState::Aborting, record)?;
    let abort_id = ctas_record(&stored)?.abort_staging_operation_id;
    match engine.abort_ctas(
        target.handle.as_ref(),
        ConnectorMutationOperationId::from_bytes(*abort_id.as_bytes()),
        completion,
    ) {
        Err(failure) => finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::AbortUnknown,
            FactSlot::Abort,
            format_failure("CTAS staged abort may have been dispatched", &failure),
        ),
        Ok(ConnectorStagedCreateAbortOutcome::Aborted {
            receipt,
            finalization,
        }) => finish_aborted(
            journal,
            stored,
            &target.facts,
            receipt,
            finalization,
            disposition,
            cause_message,
        ),
        Ok(ConnectorStagedCreateAbortOutcome::KnownUncommitted { failure }) => {
            let mut record = ctas_record(&stored)?;
            record.abort_staging_fact = Some(mutation_failure_fact(
                ExternalFactOutcome::KnownUncommitted,
                &failure,
            ));
            record.next_action = StatementNextAction::AbortStaging;
            let stored = persist(journal, stored, OperationState::Aborting, record)?;
            Err(operation_error(
                DmlErrorKind::Executor,
                stored.operation_id,
                StatementNextAction::AbortStaging,
                format!("{cause_message}; staged abort is known uncommitted: {failure}"),
            ))
        }
        Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown { failure, evidence }) => {
            finish_abort_unknown(
                engine,
                journal,
                stored,
                target,
                mutation_failure(failure),
                evidence,
                disposition,
                cause_message,
                true,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_abort_unknown(
    engine: &dyn CtasEngine,
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    target: &PreparedCtasTarget,
    failure: CtasFailure,
    evidence: ExternalMutationEvidence,
    disposition: AbortDisposition,
    cause_message: String,
    allow_reconcile: bool,
) -> Result<(), DmlError> {
    let abort_id = ctas_record(&stored)?.abort_staging_operation_id;
    let durable_evidence =
        validate_evidence(&target.facts, abort_id, "staged-create-abort", &evidence)
            .and_then(|()| encode_evidence(&evidence))
            .ok();
    let can_reconcile = allow_reconcile && durable_evidence.is_some();
    let mut record = ctas_record(&stored)?;
    let first = retained_evidence(&record.abort_staging_fact);
    if first
        .as_ref()
        .is_some_and(|first| Some(first) != durable_evidence.as_ref())
    {
        return finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::AbortUnknown,
            FactSlot::Abort,
            "CTAS abort reconcile returned evidence different from the first durable evidence",
        );
    }
    record.phase = CtasSagaPhase::AbortUnknown;
    record.abort_staging_fact = Some(DurableExternalFact {
        outcome: ExternalFactOutcome::CommitUnknown,
        receipt: None,
        evidence: first.or(durable_evidence),
        finalization_failure: None,
        failure: Some(encode_failure(&failure)),
    });
    record.next_action = if can_reconcile {
        StatementNextAction::Reconcile
    } else {
        StatementNextAction::ManualInspect
    };
    let stored = persist(journal, stored, OperationState::CommitUnknown, record)?;
    if !can_reconcile {
        return Err(unknown_error(
            stored.operation_id,
            "CTAS staged abort",
            &failure,
        ));
    }
    let stored = reconcile_barrier(journal, stored)?;
    match engine.reconcile_ctas(
        target.handle.as_ref(),
        ConnectorStagedCreateReconcilePhase::Abort,
        evidence.clone(),
    ) {
        Ok(ConnectorStagedCreateReconcileOutcome::Aborted {
            receipt,
            finalization,
        }) => {
            let mut stored = stored;
            if stored.state == OperationState::CommitUnknown {
                let record = ctas_record(&stored)?;
                stored = persist(journal, stored, OperationState::Aborting, record)?;
            }
            finish_aborted(
                journal,
                stored,
                &target.facts,
                receipt,
                finalization,
                disposition,
                cause_message,
            )
        }
        Ok(ConnectorStagedCreateReconcileOutcome::KnownUncommitted { failure }) => {
            let mut record = ctas_record(&stored)?;
            record.phase = CtasSagaPhase::AbortUnknown;
            record.next_action = StatementNextAction::ManualInspect;
            let retained = record
                .abort_staging_fact
                .as_ref()
                .and_then(|fact| fact.evidence.clone());
            record.abort_staging_fact = Some(DurableExternalFact {
                outcome: ExternalFactOutcome::CommitUnknown,
                receipt: None,
                evidence: retained,
                finalization_failure: None,
                failure: Some(encode_failure(&mutation_failure(failure.clone()))),
            });
            let stored = persist(journal, stored, OperationState::CommitUnknown, record)?;
            Err(operation_error(
                DmlErrorKind::Commit,
                stored.operation_id,
                StatementNextAction::ManualInspect,
                format!(
                    "{cause_message}; staged abort remains unresolved because the reconcile request was not dispatched: {failure}"
                ),
            ))
        }
        Ok(ConnectorStagedCreateReconcileOutcome::CommitUnknown {
            failure,
            evidence: next,
        }) => finish_reconcile_still_unknown(
            journal,
            stored,
            CtasSagaPhase::AbortUnknown,
            FactSlot::Abort,
            mutation_failure(failure),
            &evidence,
            &next,
        ),
        Ok(_) => finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::AbortUnknown,
            FactSlot::Abort,
            "CTAS abort reconcile returned an outcome for another phase",
        ),
        Err(failure) => finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::AbortUnknown,
            FactSlot::Abort,
            format_failure("CTAS abort reconcile contract failed", &failure),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_aborted(
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    target_facts: &CtasTargetFacts,
    receipt: ConnectorStagedCreateReceipt,
    finalization: ExternalMutationFinalization,
    disposition: AbortDisposition,
    cause_message: String,
) -> Result<(), DmlError> {
    let abort_id = ctas_record(&stored)?.abort_staging_operation_id;
    if let Err(error) = validate_receipt(
        target_facts,
        abort_id,
        ConnectorStagedCreateReceiptPhase::Aborted,
        &receipt,
    ) {
        return finish_contract_unknown(
            journal,
            stored,
            CtasSagaPhase::AbortUnknown,
            FactSlot::Abort,
            error.to_string(),
        );
    }
    let mut record = ctas_record(&stored)?;
    record.abort_staging_fact = Some(match committed_fact(&receipt, &finalization) {
        Ok(fact) => fact,
        Err(error) => {
            return finish_contract_unknown(
                journal,
                stored,
                CtasSagaPhase::AbortUnknown,
                FactSlot::Abort,
                error.to_string(),
            );
        }
    });
    match finalization {
        ExternalMutationFinalization::Failed(failure) => {
            record.phase = CtasSagaPhase::AbortingStaging;
            record.next_action = StatementNextAction::RetryFinalize;
            let stored = persist(journal, stored, OperationState::Aborting, record)?;
            Err(operation_error(
                DmlErrorKind::CommittedButUnfinalized,
                stored.operation_id,
                StatementNextAction::RetryFinalize,
                format!(
                    "{cause_message}; staged abort committed but finalization failed: {failure}"
                ),
            ))
        }
        ExternalMutationFinalization::Complete => {
            let retry_finalize = matches!(
                disposition,
                AbortDisposition::SuccessNoOp {
                    retry_finalize: true
                }
            );
            record.phase = match disposition {
                AbortDisposition::SuccessNoOp { .. } => CtasSagaPhase::NoOp,
                AbortDisposition::Error(phase) => phase,
            };
            record.next_action = if retry_finalize {
                StatementNextAction::RetryFinalize
            } else {
                StatementNextAction::None
            };
            if retry_finalize {
                let stored = persist(journal, stored, OperationState::Aborting, record)?;
                return Err(operation_error(
                    DmlErrorKind::CommittedButUnfinalized,
                    stored.operation_id,
                    StatementNextAction::RetryFinalize,
                    format!("{cause_message}; publish no-op finalization failed"),
                ));
            }
            let stored = persist(journal, stored, OperationState::Aborted, record)?;
            match disposition {
                AbortDisposition::SuccessNoOp { .. } => Ok(()),
                AbortDisposition::Error(_) => Err(operation_error(
                    DmlErrorKind::Executor,
                    stored.operation_id,
                    StatementNextAction::None,
                    cause_message,
                )),
            }
        }
    }
}

#[derive(Clone, Copy)]
enum FactSlot {
    Prepare,
    Write,
    Publish,
    Abort,
}

fn finish_reconcile_still_unknown(
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    phase: CtasSagaPhase,
    slot: FactSlot,
    failure: CtasFailure,
    first: &ExternalMutationEvidence,
    next: &ExternalMutationEvidence,
) -> Result<(), DmlError> {
    if first != next {
        return finish_contract_unknown(
            journal,
            stored,
            phase,
            slot,
            "CTAS reconcile changed the exact provider evidence",
        );
    }
    let mut record = ctas_record(&stored)?;
    record.phase = phase;
    record.next_action = StatementNextAction::ManualInspect;
    install_fact(
        &mut record,
        slot,
        DurableExternalFact {
            outcome: ExternalFactOutcome::CommitUnknown,
            receipt: None,
            evidence: encode_evidence(first).ok(),
            finalization_failure: None,
            failure: Some(encode_failure(&failure)),
        },
    );
    let stored = persist(journal, stored, OperationState::CommitUnknown, record)?;
    Err(unknown_error(
        stored.operation_id,
        "CTAS reconcile",
        &failure,
    ))
}

fn finish_contract_unknown(
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    phase: CtasSagaPhase,
    slot: FactSlot,
    message: impl Into<String>,
) -> Result<(), DmlError> {
    let message = message.into();
    let mut record = ctas_record(&stored)?;
    let evidence = match slot {
        FactSlot::Prepare => retained_evidence(&record.prepare_fact),
        FactSlot::Write => retained_evidence(&record.write_fact),
        FactSlot::Publish => retained_evidence(&record.publish_fact),
        FactSlot::Abort => retained_evidence(&record.abort_staging_fact),
    };
    record.phase = phase;
    record.next_action = StatementNextAction::ManualInspect;
    install_fact(
        &mut record,
        slot,
        DurableExternalFact {
            outcome: ExternalFactOutcome::CommitUnknown,
            receipt: None,
            evidence,
            finalization_failure: None,
            failure: Some(encode_failure(&CtasFailure {
                kind: CtasFailureKind::Internal,
                message: message.clone(),
            })),
        },
    );
    let stored = persist(journal, stored, OperationState::CommitUnknown, record)?;
    Err(operation_error(
        DmlErrorKind::Commit,
        stored.operation_id,
        StatementNextAction::ManualInspect,
        message,
    ))
}

fn reconcile_barrier(
    journal: &dyn OperationJournal,
    stored: StoredOperation,
) -> Result<StoredOperation, DmlError> {
    let record = ctas_record(&stored)?;
    persist(journal, stored, OperationState::CommitUnknown, record)
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

fn validate_target_facts(
    stored: &StoredOperation,
    facts: &CtasTargetFacts,
) -> Result<(), DmlError> {
    let expected = ctas_record(stored)?.prepare_operation_id;
    if facts.operation_id == *expected.as_bytes()
        && !facts.provider_id.is_empty()
        && !facts.instance_id.is_empty()
        && facts.handle_digest.is_some()
    {
        Ok(())
    } else {
        Err(operation_error(
            DmlErrorKind::Executor,
            stored.operation_id,
            StatementNextAction::ManualInspect,
            "CTAS staged target facts conflict with the durable prepare identity",
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

fn install_target_facts(
    record: &mut CtasSagaRecord,
    facts: &CtasTargetFacts,
) -> Result<(), DmlError> {
    if facts.provider_id.is_empty() || facts.instance_id.is_empty() {
        return Err(DmlError::journal_corruption("CTAS target owner is empty"));
    }
    record.provider_id = Some(facts.provider_id.clone());
    record.connector_instance_id = Some(facts.instance_id.clone());
    record.connector_incarnation = Some(hex::encode(facts.incarnation));
    record.staged_handle_digest = facts.handle_digest.map(hex::encode);
    Ok(())
}

fn validate_receipt(
    facts: &CtasTargetFacts,
    operation_id: Uuid,
    phase: ConnectorStagedCreateReceiptPhase,
    receipt: &ConnectorStagedCreateReceipt,
) -> Result<(), DmlError> {
    let matching = receipt.owner().instance_id.as_str() == facts.instance_id
        && receipt.owner().incarnation.to_bytes() == facts.incarnation
        && receipt.operation_id().to_bytes() == *operation_id.as_bytes()
        && receipt.phase() == phase;
    if matching {
        Ok(())
    } else {
        Err(DmlError::commit(
            "CTAS provider receipt conflicts with durable child identity",
        ))
    }
}

fn validate_evidence(
    facts: &CtasTargetFacts,
    operation_id: Uuid,
    operation_kind: &str,
    evidence: &ExternalMutationEvidence,
) -> Result<(), DmlError> {
    let wire = evidence.try_to_wire_v1().map_err(DmlError::commit)?;
    let decoded = ExternalMutationEvidence::try_from_wire_v1(&wire).map_err(DmlError::commit)?;
    let matching = decoded == *evidence
        && evidence.descriptor().provider_id.as_str() == facts.provider_id
        && evidence.descriptor().instance_id.as_str() == facts.instance_id
        && evidence.incarnation().to_bytes() == facts.incarnation
        && evidence.operation_id().to_bytes() == *operation_id.as_bytes()
        && evidence.operation_kind() == operation_kind;
    if matching {
        Ok(())
    } else {
        Err(DmlError::commit(
            "CTAS provider evidence conflicts with durable child identity",
        ))
    }
}

fn committed_fact(
    receipt: &ConnectorStagedCreateReceipt,
    finalization: &ExternalMutationFinalization,
) -> Result<DurableExternalFact, DmlError> {
    Ok(DurableExternalFact {
        outcome: ExternalFactOutcome::KnownCommitted,
        receipt: Some(encode_receipt(receipt)?),
        evidence: None,
        finalization_failure: encode_finalization_failure(finalization),
        failure: None,
    })
}

fn no_op_fact(
    receipt: &ConnectorStagedCreateReceipt,
    finalization: &ExternalMutationFinalization,
) -> Result<DurableExternalFact, DmlError> {
    Ok(DurableExternalFact {
        outcome: ExternalFactOutcome::NoOp,
        receipt: Some(encode_receipt(receipt)?),
        evidence: None,
        finalization_failure: encode_finalization_failure(finalization),
        failure: None,
    })
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

fn mutation_failure_fact(
    outcome: ExternalFactOutcome,
    failure: &ConnectorMutationFailure,
) -> DurableExternalFact {
    DurableExternalFact {
        outcome,
        receipt: None,
        evidence: None,
        finalization_failure: None,
        failure: Some(encode_failure(&mutation_failure(failure.clone()))),
    }
}

fn install_fact(record: &mut CtasSagaRecord, slot: FactSlot, fact: DurableExternalFact) {
    match slot {
        FactSlot::Prepare => record.prepare_fact = Some(fact),
        FactSlot::Write => record.write_fact = Some(fact),
        FactSlot::Publish => record.publish_fact = Some(fact),
        FactSlot::Abort => record.abort_staging_fact = Some(fact),
    }
}

fn retained_evidence(fact: &Option<DurableExternalFact>) -> Option<String> {
    fact.as_ref().and_then(|fact| fact.evidence.clone())
}

fn encode_receipt(receipt: &ConnectorStagedCreateReceipt) -> Result<String, DmlError> {
    let payload = receipt.provider_payload();
    let encoded = serde_json::to_string(&DurableCtasReceiptV1 {
        version: DURABLE_CTAS_FACT_VERSION,
        phase: receipt_phase_name(receipt.phase()),
        effect: match receipt.effect() {
            novarocks_spi::connector::ExternalMutationEffect::Applied => "APPLIED",
            novarocks_spi::connector::ExternalMutationEffect::NoOp => "NO_OP",
        },
        instance_id: receipt.owner().instance_id.as_str(),
        incarnation: hex::encode(receipt.owner().incarnation.to_bytes()),
        operation_id: hex::encode(receipt.operation_id().to_bytes()),
        provider_payload_bytes: payload.len(),
        provider_payload_sha256: hex::encode(Sha256::digest(payload)),
    })
    .map_err(DmlError::journal_corruption)?;
    ensure_fact_bound("CTAS receipt", &encoded)?;
    Ok(encoded)
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

fn encode_finalization_failure(finalization: &ExternalMutationFinalization) -> Option<String> {
    match finalization {
        ExternalMutationFinalization::Complete => None,
        ExternalMutationFinalization::Failed(failure) => {
            Some(encode_failure(&mutation_failure(failure.clone())))
        }
    }
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

fn preflight_external_truth(
    journal: &dyn OperationJournal,
    stored: &StoredOperation,
) -> Result<(), DmlError> {
    let maximal = maximal_committed_fact();
    let mut operation = stored.clone();
    let mut record = ctas_record(stored)?;
    record.prepare_fact = Some(maximal.clone());
    record.write_fact = Some(maximal.clone());
    record.publish_fact = Some(maximal.clone());
    record.abort_staging_fact = Some(maximal);
    operation.payload = OperationPayload::CtasSaga(record);
    journal.preflight_statement_operation(&operation)
}

fn maximal_committed_fact() -> DurableExternalFact {
    let mut payload_len = DML_CTAS_FACT_ENCODED_LIMIT;
    loop {
        let fact = DurableExternalFact {
            outcome: ExternalFactOutcome::KnownCommitted,
            receipt: Some("x".repeat(payload_len)),
            evidence: None,
            finalization_failure: None,
            failure: None,
        };
        if serde_json::to_vec(&fact)
            .is_ok_and(|encoded| encoded.len() <= DML_CTAS_FACT_ENCODED_LIMIT)
        {
            return fact;
        }
        payload_len -= 1;
    }
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

fn persist(
    journal: &dyn OperationJournal,
    stored: StoredOperation,
    state: OperationState,
    record: CtasSagaRecord,
) -> Result<StoredOperation, DmlError> {
    journal
        .mutate_statement_operation(OperationMutationRequest {
            operation_id: stored.operation_id,
            expected_revision: stored.revision,
            mutation_id: Uuid::now_v7(),
            state,
            payload: OperationPayload::CtasSaga(record),
        })
        .map_err(|error| journal_error(error, stored.operation_id))
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

fn finalization_action(finalization: &ExternalMutationFinalization) -> StatementNextAction {
    match finalization {
        ExternalMutationFinalization::Complete => StatementNextAction::None,
        ExternalMutationFinalization::Failed(_) => StatementNextAction::RetryFinalize,
    }
}

fn receipt_phase_name(phase: ConnectorStagedCreateReceiptPhase) -> &'static str {
    match phase {
        ConnectorStagedCreateReceiptPhase::Prepared => "PREPARED",
        ConnectorStagedCreateReceiptPhase::Published => "PUBLISHED",
        ConnectorStagedCreateReceiptPhase::Aborted => "ABORTED",
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
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use novarocks::common::app_config::ClusterRole;
    use novarocks::engine::statistics::EmptyStatisticsService;
    use novarocks::query_execution::backend::BackendTopologySnapshot;
    use novarocks::query_execution::cancellation::QueryCancellationSource;
    use novarocks::query_execution::request_context::{RequestAdmission, RequestContext};
    use novarocks_spi::connector::{
        CONNECTOR_WRITE_CONTRACT_VERSION, ConnectorColumnDefinition, ConnectorDataType,
        ConnectorExecutionBindingKey, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorProviderId, ConnectorSealedWriteCohortSet,
        ConnectorStagedReport, ConnectorStagedReportSummary, ConnectorWriteAttemptCompletion,
        ConnectorWriteCohortCompletion, ConnectorWriteCohortDescriptor, ConnectorWriteCohortId,
        ConnectorWriteExecutionId, ConnectorWriteIntent, ConnectorWriterIdentity,
        ConnectorWriterTerminalState, ExternalMutationEffect,
    };

    use super::*;
    use crate::dml::journal::testing::InMemoryOperationJournal;

    struct FakeSource([u8; 32]);

    impl novarocks::engine::ctas_engine::CtasPreparedSource for FakeSource {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn execution_identity(&self) -> [u8; 32] {
            self.0
        }
    }

    struct FakeTarget;

    impl novarocks::engine::ctas_engine::CtasPreparedTarget for FakeTarget {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct FakeWrite {
        operation_id: ConnectorWriteOperationId,
        execution_identity: [u8; 32],
    }

    impl novarocks::engine::ctas_engine::CtasPreparedWrite for FakeWrite {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn execution_identity(&self) -> [u8; 32] {
            self.execution_identity
        }
    }

    #[derive(Clone, Copy)]
    enum PrepareMode {
        Prepared,
        Unsupported,
        ContractUnknown,
        MalformedContractUnknown,
    }

    #[derive(Clone, Copy)]
    enum WriteMode {
        Completed,
        CommitUnknown,
    }

    #[derive(Clone, Copy)]
    enum PublishMode {
        Applied,
        AppliedFinalizeFailed,
        NoOp,
        Conflict,
        OuterError,
    }

    #[derive(Clone, Copy)]
    enum AbortMode {
        Aborted,
        FinalizeFailed,
        CommitUnknown,
    }

    #[derive(Clone, Copy)]
    enum ReconcileMode {
        OuterError,
        AbortKnownUncommitted,
    }

    struct FakeEngine {
        precheck_exists: bool,
        precheck_failure: bool,
        source_session_drift: bool,
        prepare_mode: PrepareMode,
        write_mode: WriteMode,
        publish_mode: PublishMode,
        abort_mode: AbortMode,
        reconcile_mode: ReconcileMode,
        classify_calls: AtomicUsize,
        source_prepare_calls: AtomicUsize,
        target_prepare_calls: AtomicUsize,
        write_prepare_calls: AtomicUsize,
        write_execute_calls: AtomicUsize,
        write_reconcile_calls: AtomicUsize,
        publish_calls: AtomicUsize,
        abort_calls: AtomicUsize,
        reconcile_calls: AtomicUsize,
        child_ids: Mutex<Vec<[u8; 16]>>,
    }

    impl Default for FakeEngine {
        fn default() -> Self {
            Self {
                precheck_exists: false,
                precheck_failure: false,
                source_session_drift: false,
                prepare_mode: PrepareMode::Prepared,
                write_mode: WriteMode::Completed,
                publish_mode: PublishMode::Applied,
                abort_mode: AbortMode::Aborted,
                reconcile_mode: ReconcileMode::OuterError,
                classify_calls: AtomicUsize::new(0),
                source_prepare_calls: AtomicUsize::new(0),
                target_prepare_calls: AtomicUsize::new(0),
                write_prepare_calls: AtomicUsize::new(0),
                write_execute_calls: AtomicUsize::new(0),
                write_reconcile_calls: AtomicUsize::new(0),
                publish_calls: AtomicUsize::new(0),
                abort_calls: AtomicUsize::new(0),
                reconcile_calls: AtomicUsize::new(0),
                child_ids: Mutex::new(Vec::new()),
            }
        }
    }

    impl FakeEngine {
        fn owner() -> ConnectorExecutionBindingKey {
            ConnectorExecutionBindingKey {
                instance_id: ConnectorInstanceId::parse("ice").unwrap(),
                incarnation: ConnectorInstanceIncarnation::from_bytes([0x11; 16]),
            }
        }

        fn target_facts(operation_id: [u8; 16]) -> CtasTargetFacts {
            CtasTargetFacts {
                provider_id: "iceberg".to_string(),
                instance_id: "ice".to_string(),
                incarnation: [0x11; 16],
                operation_id,
                handle_digest: Some([0x22; 32]),
            }
        }

        fn target(operation_id: [u8; 16]) -> PreparedCtasTarget {
            PreparedCtasTarget {
                facts: Self::target_facts(operation_id),
                handle: Arc::new(FakeTarget),
            }
        }

        fn receipt(
            operation_id: ConnectorMutationOperationId,
            phase: ConnectorStagedCreateReceiptPhase,
            effect: ExternalMutationEffect,
        ) -> ConnectorStagedCreateReceipt {
            ConnectorStagedCreateReceipt::try_new(
                Self::owner(),
                operation_id,
                phase,
                effect,
                Bytes::from_static(b"receipt"),
            )
            .unwrap()
        }

        fn evidence(
            operation_id: ConnectorMutationOperationId,
            operation_kind: &'static str,
        ) -> ExternalMutationEvidence {
            ExternalMutationEvidence::try_new(
                1,
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("ice").unwrap(),
                },
                ConnectorInstanceIncarnation::from_bytes([0x11; 16]),
                operation_id,
                operation_kind,
                Bytes::from_static(b"evidence"),
            )
            .unwrap()
        }

        fn failure(kind: CtasFailureKind, message: &str) -> CtasFailure {
            CtasFailure {
                kind,
                message: message.to_string(),
            }
        }

        fn finalization_failure() -> ConnectorMutationFailure {
            ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                "finalization failed",
            )
        }
    }

    impl CtasEngine for FakeEngine {
        fn classify_ctas(
            &self,
            sql: &str,
        ) -> Result<Option<novarocks::engine::ctas_engine::CtasCommand>, String> {
            self.classify_calls.fetch_add(1, Ordering::SeqCst);
            if !sql.trim().to_ascii_uppercase().starts_with("CREATE TABLE") {
                return Ok(None);
            }
            Ok(Some(novarocks::engine::ctas_engine::CtasCommand {
                target_parts: vec!["ice".to_string(), "db".to_string(), "dst".to_string()],
                if_not_exists: sql.to_ascii_uppercase().contains("IF NOT EXISTS"),
                source_sql: "SELECT 1 AS x".to_string(),
                partitioning: Vec::new(),
                properties: Default::default(),
            }))
        }

        fn precheck_ctas_target(
            &self,
            command: &novarocks::engine::ctas_engine::CtasCommand,
            _current_catalog: Option<&str>,
            _current_database: &str,
        ) -> Result<CtasTargetPrecheck, CtasFailure> {
            if self.precheck_failure {
                return Err(Self::failure(
                    CtasFailureKind::Unavailable,
                    "precheck failed",
                ));
            }
            if self.precheck_exists && command.if_not_exists {
                Ok(CtasTargetPrecheck::ExistsNoOp)
            } else {
                Ok(CtasTargetPrecheck::Absent)
            }
        }

        fn prepare_ctas_source(
            &self,
            request: PrepareCtasSourceRequest,
        ) -> Result<PreparedCtasSource, CtasFailure> {
            self.source_prepare_calls.fetch_add(1, Ordering::SeqCst);
            let identity = [0x33; 32];
            Ok(PreparedCtasSource {
                facts: novarocks::engine::ctas_engine::CtasPreparedSourceFacts {
                    target_catalog: request.command.target_parts[0].clone(),
                    target_namespace: request.command.target_parts[1].clone(),
                    target_table: request.command.target_parts[2].clone(),
                    source_catalog: if self.source_session_drift {
                        Some("drifted".to_string())
                    } else {
                        request.current_catalog
                    },
                    source_database: if self.source_session_drift {
                        "drifted".to_string()
                    } else {
                        request.current_database
                    },
                    plan_digest: [0x44; 32],
                    schema_digest: [0x55; 32],
                    execution_identity: identity,
                    output_columns: vec![ConnectorColumnDefinition {
                        name: Arc::from("x"),
                        data_type: ConnectorDataType::Int,
                        nullable: false,
                        aggregation: None,
                        default: None,
                    }],
                },
                handle: Arc::new(FakeSource(identity)),
            })
        }

        fn prepare_ctas_target(
            &self,
            _source: &dyn novarocks::engine::ctas_engine::CtasPreparedSource,
            operation_id: ConnectorMutationOperationId,
            _policy: CreatePolicy,
        ) -> Result<CtasTargetPrepareOutcome, CtasFailure> {
            self.target_prepare_calls.fetch_add(1, Ordering::SeqCst);
            self.child_ids.lock().unwrap().push(operation_id.to_bytes());
            match self.prepare_mode {
                PrepareMode::Prepared => Ok(CtasTargetPrepareOutcome::Prepared {
                    target: Self::target(operation_id.to_bytes()),
                    receipt: Self::receipt(
                        operation_id,
                        ConnectorStagedCreateReceiptPhase::Prepared,
                        ExternalMutationEffect::Applied,
                    ),
                    finalization: ExternalMutationFinalization::Complete,
                }),
                PrepareMode::Unsupported => Err(Self::failure(
                    CtasFailureKind::Unsupported,
                    "staged publication unsupported",
                )),
                PrepareMode::ContractUnknown => Ok(CtasTargetPrepareOutcome::ContractUnknown {
                    target: Self::target(operation_id.to_bytes()),
                    failure: Self::failure(
                        CtasFailureKind::Unavailable,
                        "prepare contract unknown",
                    ),
                }),
                PrepareMode::MalformedContractUnknown => {
                    Ok(CtasTargetPrepareOutcome::ContractUnknown {
                        target: Self::target([0xff; 16]),
                        failure: Self::failure(
                            CtasFailureKind::Unavailable,
                            "malformed prepare contract unknown",
                        ),
                    })
                }
            }
        }

        fn prepare_ctas_write(
            &self,
            source: &dyn novarocks::engine::ctas_engine::CtasPreparedSource,
            target: &dyn novarocks::engine::ctas_engine::CtasPreparedTarget,
            operation_id: ConnectorWriteOperationId,
        ) -> Result<PreparedCtasWrite, CtasFailure> {
            self.write_prepare_calls.fetch_add(1, Ordering::SeqCst);
            self.child_ids.lock().unwrap().push(operation_id.to_bytes());
            let _ = target.as_any().downcast_ref::<FakeTarget>().unwrap();
            let source = source.as_any().downcast_ref::<FakeSource>().unwrap();
            Ok(PreparedCtasWrite {
                target_facts: Self::target_facts(self.child_ids.lock().unwrap()[0]),
                write_operation_id: operation_id,
                execution_identity: source.0,
                handle: Arc::new(FakeWrite {
                    operation_id,
                    execution_identity: source.0,
                }),
            })
        }

        fn execute_ctas_write(
            &self,
            prepared: &dyn novarocks::engine::ctas_engine::CtasPreparedWrite,
        ) -> CtasWriteOutcome {
            self.write_execute_calls.fetch_add(1, Ordering::SeqCst);
            let prepared = prepared.as_any().downcast_ref::<FakeWrite>().unwrap();
            match self.write_mode {
                WriteMode::Completed => CtasWriteOutcome::Completed {
                    completion: completion(Self::owner(), prepared.operation_id),
                    execution_identity: prepared.execution_identity,
                },
                WriteMode::CommitUnknown => CtasWriteOutcome::CommitUnknown {
                    failure: Self::failure(CtasFailureKind::Unavailable, "write unknown"),
                    evidence: Self::evidence(
                        ConnectorMutationOperationId::from_bytes(prepared.operation_id.to_bytes()),
                        "ctas-write-staging",
                    ),
                },
            }
        }

        fn reconcile_ctas_write(
            &self,
            prepared: &dyn novarocks::engine::ctas_engine::CtasPreparedWrite,
            _evidence: ExternalMutationEvidence,
        ) -> CtasWriteOutcome {
            self.write_reconcile_calls.fetch_add(1, Ordering::SeqCst);
            let prepared = prepared.as_any().downcast_ref::<FakeWrite>().unwrap();
            CtasWriteOutcome::Completed {
                completion: completion(Self::owner(), prepared.operation_id),
                execution_identity: prepared.execution_identity,
            }
        }

        fn publish_ctas(
            &self,
            _target: &dyn novarocks::engine::ctas_engine::CtasPreparedTarget,
            operation_id: ConnectorMutationOperationId,
            _completion: ConnectorWriteOperationCompletion,
        ) -> Result<ConnectorStagedCreatePublishOutcome, CtasFailure> {
            self.publish_calls.fetch_add(1, Ordering::SeqCst);
            self.child_ids.lock().unwrap().push(operation_id.to_bytes());
            match self.publish_mode {
                PublishMode::Applied => Ok(ConnectorStagedCreatePublishOutcome::Applied {
                    receipt: Self::receipt(
                        operation_id,
                        ConnectorStagedCreateReceiptPhase::Published,
                        ExternalMutationEffect::Applied,
                    ),
                    finalization: ExternalMutationFinalization::Complete,
                }),
                PublishMode::AppliedFinalizeFailed => {
                    Ok(ConnectorStagedCreatePublishOutcome::Applied {
                        receipt: Self::receipt(
                            operation_id,
                            ConnectorStagedCreateReceiptPhase::Published,
                            ExternalMutationEffect::Applied,
                        ),
                        finalization: ExternalMutationFinalization::Failed(
                            Self::finalization_failure(),
                        ),
                    })
                }
                PublishMode::NoOp => Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                    receipt: Self::receipt(
                        operation_id,
                        ConnectorStagedCreateReceiptPhase::Published,
                        ExternalMutationEffect::NoOp,
                    ),
                    finalization: ExternalMutationFinalization::Complete,
                }),
                PublishMode::Conflict => Ok(ConnectorStagedCreatePublishOutcome::Conflict {
                    failure: ConnectorMutationFailure::new(
                        novarocks_spi::connector::ConnectorMutationFailureKind::Conflict,
                        "publish conflict",
                    ),
                }),
                PublishMode::OuterError => Err(Self::failure(
                    CtasFailureKind::Unavailable,
                    "publish outer error",
                )),
            }
        }

        fn abort_ctas(
            &self,
            _target: &dyn novarocks::engine::ctas_engine::CtasPreparedTarget,
            operation_id: ConnectorMutationOperationId,
            _completion: Option<ConnectorWriteOperationCompletion>,
        ) -> Result<ConnectorStagedCreateAbortOutcome, CtasFailure> {
            self.abort_calls.fetch_add(1, Ordering::SeqCst);
            self.child_ids.lock().unwrap().push(operation_id.to_bytes());
            match self.abort_mode {
                AbortMode::Aborted | AbortMode::FinalizeFailed => {
                    Ok(ConnectorStagedCreateAbortOutcome::Aborted {
                        receipt: Self::receipt(
                            operation_id,
                            ConnectorStagedCreateReceiptPhase::Aborted,
                            ExternalMutationEffect::Applied,
                        ),
                        finalization: match self.abort_mode {
                            AbortMode::Aborted => ExternalMutationFinalization::Complete,
                            AbortMode::FinalizeFailed => {
                                ExternalMutationFinalization::Failed(Self::finalization_failure())
                            }
                            AbortMode::CommitUnknown => unreachable!(),
                        },
                    })
                }
                AbortMode::CommitUnknown => Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                        "abort unknown",
                    ),
                    evidence: Self::evidence(operation_id, "staged-create-abort"),
                }),
            }
        }

        fn reconcile_ctas(
            &self,
            _target: &dyn novarocks::engine::ctas_engine::CtasPreparedTarget,
            phase: ConnectorStagedCreateReconcilePhase,
            _evidence: ExternalMutationEvidence,
        ) -> Result<ConnectorStagedCreateReconcileOutcome, CtasFailure> {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            match self.reconcile_mode {
                ReconcileMode::OuterError => Err(Self::failure(
                    CtasFailureKind::Unavailable,
                    "unused reconcile",
                )),
                ReconcileMode::AbortKnownUncommitted => {
                    assert_eq!(phase, ConnectorStagedCreateReconcilePhase::Abort);
                    Ok(ConnectorStagedCreateReconcileOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                            "reconcile request not dispatched",
                        ),
                    })
                }
            }
        }
    }

    fn completion(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
    ) -> ConnectorWriteOperationCompletion {
        let cohort_id = ConnectorWriteCohortId::primary(operation_id);
        let execution_id = ConnectorWriteExecutionId::new([0x66; 16], 1);
        let writer = ConnectorWriterIdentity::new(
            operation_id,
            cohort_id,
            execution_id,
            [0x77; 16],
            1,
            0,
            0,
            owner.clone(),
        );
        let report = ConnectorStagedReport::try_new(
            writer,
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from_static(b"report"),
        )
        .unwrap();
        let accepted = ConnectorWriteAttemptCompletion::try_new(
            owner.clone(),
            operation_id,
            cohort_id,
            execution_id,
            [0x88; 32],
            vec![report],
            Bytes::new(),
        )
        .unwrap();
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![ConnectorWriteCohortDescriptor::new(
                cohort_id,
                ConnectorWriteIntent::Append,
                [0x99; 32],
            )],
        )
        .unwrap();
        ConnectorWriteOperationCompletion::try_new(
            owner,
            sealed,
            vec![
                ConnectorWriteCohortCompletion::try_new(cohort_id, Some(accepted), vec![]).unwrap(),
            ],
        )
        .unwrap()
    }

    fn service() -> (DmlService, Arc<InMemoryOperationJournal>) {
        let journal = Arc::new(InMemoryOperationJournal::default());
        (
            DmlService::new(Arc::clone(&journal) as Arc<dyn OperationJournal>),
            journal,
        )
    }

    fn admitted_context() -> RequestContext {
        let cancellation = QueryCancellationSource::new();
        RequestContext::admit(RequestAdmission::new(
            Some("ice".to_string()),
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(83),
            None,
            cancellation.view(),
            Default::default(),
        ))
    }

    fn operation_record(operation: &StoredOperation) -> &CtasSagaRecord {
        match &operation.payload {
            OperationPayload::CtasSaga(record) => record,
            other => panic!("expected CTAS record, got {other:?}"),
        }
    }

    #[test]
    fn worst_case_fact_fills_but_does_not_exceed_the_complete_envelope_limit() {
        let fact = maximal_committed_fact();
        let encoded = serde_json::to_vec(&fact).unwrap();
        assert!(encoded.len() <= DML_CTAS_FACT_ENCODED_LIMIT);
        let mut larger = fact;
        larger.receipt.as_mut().unwrap().push('x');
        assert!(serde_json::to_vec(&larger).unwrap().len() > DML_CTAS_FACT_ENCODED_LIMIT);
    }

    #[test]
    fn failure_projection_is_bounded_and_preserves_the_original_digest() {
        let failure = CtasFailure {
            kind: CtasFailureKind::Unavailable,
            message: "x".repeat(64 * 1024),
        };
        let encoded = encode_failure(&failure);
        assert!(encoded.len() < DML_CTAS_FACT_ENCODED_LIMIT);
        assert!(encoded.contains("UNAVAILABLE"));
        assert!(encoded.contains(&hex::encode(Sha256::digest(failure.message.as_bytes()))));
    }

    #[test]
    fn syntactic_target_uses_the_admitted_session_resolution() {
        assert_eq!(
            syntactic_target(&["orders".to_string()], Some("ice"), "sales"),
            OperationTarget {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
                ref_name: None,
            }
        );
        assert_eq!(
            syntactic_target(
                &["rest".to_string(), "db".to_string(), "t".to_string()],
                None,
                "ignored",
            ),
            OperationTarget {
                catalog: "rest".to_string(),
                namespace: "db".to_string(),
                table: "t".to_string(),
                ref_name: None,
            }
        );
    }

    #[test]
    fn non_ctas_and_ifne_existing_do_not_create_a_saga_or_prepare_the_source() {
        let engine = FakeEngine::default();
        let no_journal = DmlService::compose(None, Arc::new(EmptyStatisticsService));
        let context = admitted_context();
        assert_eq!(
            no_journal
                .try_execute_ctas(&engine, "SELECT 1", &context, None)
                .unwrap(),
            None
        );
        assert_eq!(engine.source_prepare_calls.load(Ordering::SeqCst), 0);

        let engine = FakeEngine {
            precheck_exists: true,
            ..Default::default()
        };
        assert_eq!(
            no_journal
                .try_execute_ctas(
                    &engine,
                    "CREATE TABLE IF NOT EXISTS ice.db.dst AS SELECT 1",
                    &context,
                    None,
                )
                .unwrap(),
            Some(())
        );
        assert_eq!(engine.source_prepare_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.target_prepare_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn happy_path_uses_stable_child_ids_and_executes_the_source_once() {
        let engine = FakeEngine::default();
        let (service, journal) = service();
        let context = admitted_context();
        assert_eq!(
            service
                .try_execute_ctas(
                    &engine,
                    "CREATE TABLE ice.db.dst AS SELECT 1",
                    &context,
                    None,
                )
                .unwrap(),
            Some(())
        );
        let operation = journal.only_operation();
        let record = operation_record(&operation);
        assert_eq!(operation.state, OperationState::Finalized);
        assert_eq!(record.phase, CtasSagaPhase::Committed);
        assert_eq!(record.next_action, StatementNextAction::None);
        assert!(record.prepare_fact.is_some());
        assert!(record.write_fact.is_some());
        assert!(record.publish_fact.is_some());
        let ids = [
            record.prepare_operation_id,
            record.write_operation_id,
            record.publish_operation_id,
            record.abort_staging_operation_id,
        ];
        assert!(ids.iter().all(|id| !id.is_nil()));
        assert_eq!(
            ids.iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            4
        );
        assert_eq!(engine.source_prepare_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.write_execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.publish_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            engine.child_ids.lock().unwrap().as_slice(),
            &[
                *record.prepare_operation_id.as_bytes(),
                *record.write_operation_id.as_bytes(),
                *record.publish_operation_id.as_bytes(),
            ]
        );
    }

    #[test]
    fn unsupported_target_fails_before_source_execution_and_external_prepare_progress() {
        let engine = FakeEngine {
            prepare_mode: PrepareMode::Unsupported,
            ..Default::default()
        };
        let (service, journal) = service();
        let error = service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap_err();
        assert_eq!(error.next_action(), Some(StatementNextAction::None));
        let operation = journal.only_operation();
        assert_eq!(operation.state, OperationState::FailedKnownUncommitted);
        assert_eq!(
            operation_record(&operation).phase,
            CtasSagaPhase::Unsupported
        );
        assert_eq!(engine.source_prepare_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.write_execute_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.publish_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepare_contract_unknown_is_durable_and_never_aborts() {
        let engine = FakeEngine {
            prepare_mode: PrepareMode::ContractUnknown,
            ..Default::default()
        };
        let (service, journal) = service();
        let error = service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap_err();
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::ManualInspect)
        );
        let operation = journal.only_operation();
        let record = operation_record(&operation);
        assert_eq!(operation.state, OperationState::CommitUnknown);
        assert_eq!(record.phase, CtasSagaPhase::PrepareUnknown);
        assert_eq!(record.prepare_fact.as_ref().unwrap().evidence, None);
        assert_eq!(engine.write_execute_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.reconcile_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn write_unknown_reconciles_without_reexecuting_the_source() {
        let engine = FakeEngine {
            write_mode: WriteMode::CommitUnknown,
            ..Default::default()
        };
        let (service, journal) = service();
        service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap();
        assert_eq!(journal.only_operation().state, OperationState::Finalized);
        assert_eq!(engine.write_execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.write_reconcile_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.publish_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn publish_finalization_failure_preserves_known_committed_truth_without_abort() {
        let engine = FakeEngine {
            publish_mode: PublishMode::AppliedFinalizeFailed,
            ..Default::default()
        };
        let (service, journal) = service();
        let error = service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap_err();
        assert_eq!(error.kind(), DmlErrorKind::CommittedButUnfinalized);
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::RetryFinalize)
        );
        let operation = journal.only_operation();
        assert_eq!(
            operation.state,
            OperationState::FinalizeFailedKnownCommitted
        );
        assert_eq!(operation_record(&operation).phase, CtasSagaPhase::Committed);
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn publish_noop_aborts_only_the_opaque_staging_and_returns_success() {
        let engine = FakeEngine {
            publish_mode: PublishMode::NoOp,
            ..Default::default()
        };
        let (service, journal) = service();
        service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE IF NOT EXISTS ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap();
        let operation = journal.only_operation();
        assert_eq!(operation.state, OperationState::Aborted);
        assert_eq!(operation_record(&operation).phase, CtasSagaPhase::NoOp);
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn publish_outer_error_is_unknown_and_forbids_abort() {
        let engine = FakeEngine {
            publish_mode: PublishMode::OuterError,
            ..Default::default()
        };
        let (service, journal) = service();
        let error = service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap_err();
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::ManualInspect)
        );
        let operation = journal.only_operation();
        assert_eq!(operation.state, OperationState::CommitUnknown);
        assert_eq!(
            operation_record(&operation).phase,
            CtasSagaPhase::PublishUnknown
        );
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn abort_finalization_failure_remains_unfinished_and_is_not_retried() {
        let engine = FakeEngine {
            publish_mode: PublishMode::Conflict,
            abort_mode: AbortMode::FinalizeFailed,
            ..Default::default()
        };
        let (service, journal) = service();
        let error = service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap_err();
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::RetryFinalize)
        );
        let operation = journal.only_operation();
        let record = operation_record(&operation);
        assert_eq!(operation.state, OperationState::Aborting);
        assert_eq!(record.phase, CtasSagaPhase::AbortingStaging);
        assert_eq!(record.next_action, StatementNextAction::RetryFinalize);
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 1);
        assert_eq!(journal.list_unfinished().unwrap().len(), 1);
    }

    #[test]
    fn precheck_failure_is_journaled_and_returns_an_operation_identity() {
        let engine = FakeEngine {
            precheck_failure: true,
            ..Default::default()
        };
        let (service, journal) = service();
        let error = service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap_err();
        let operation = journal.only_operation();
        assert_eq!(error.operation_id(), Some(operation.operation_id));
        assert_eq!(error.next_action(), Some(StatementNextAction::None));
        assert_eq!(operation.state, OperationState::FailedKnownUncommitted);
        assert_eq!(operation_record(&operation).phase, CtasSagaPhase::Failed);
        assert_eq!(engine.source_prepare_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.target_prepare_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepared_source_must_preserve_the_admitted_session_identity() {
        let engine = FakeEngine {
            source_session_drift: true,
            ..Default::default()
        };
        let (service, journal) = service();
        service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap_err();
        let operation = journal.only_operation();
        assert_eq!(operation.state, OperationState::FailedKnownUncommitted);
        assert_eq!(engine.target_prepare_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.write_execute_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepare_unknown_rejects_target_identity_drift_before_reconcile() {
        let engine = FakeEngine {
            prepare_mode: PrepareMode::MalformedContractUnknown,
            ..Default::default()
        };
        let (service, journal) = service();
        let error = service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap_err();
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::ManualInspect)
        );
        let operation = journal.only_operation();
        assert_eq!(operation.state, OperationState::CommitUnknown);
        assert_eq!(
            operation_record(&operation).phase,
            CtasSagaPhase::PrepareUnknown
        );
        assert_eq!(engine.reconcile_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.write_execute_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn maximal_provider_receipt_uses_a_bounded_digest_projection() {
        let receipt = ConnectorStagedCreateReceipt::try_new(
            FakeEngine::owner(),
            ConnectorMutationOperationId::from_bytes([0xaa; 16]),
            ConnectorStagedCreateReceiptPhase::Published,
            ExternalMutationEffect::Applied,
            Bytes::from(vec![0xbb; 64 * 1024]),
        )
        .unwrap();
        let encoded = encode_receipt(&receipt).unwrap();
        assert!(encoded.len() < DML_CTAS_FACT_ENCODED_LIMIT);
        assert!(encoded.contains("\"provider_payload_bytes\":65536"));
        assert!(!encoded.contains(&"bb".repeat(1024)));
    }

    #[test]
    fn abort_reconcile_known_uncommitted_preserves_the_original_unknown_truth() {
        let engine = FakeEngine {
            publish_mode: PublishMode::Conflict,
            abort_mode: AbortMode::CommitUnknown,
            reconcile_mode: ReconcileMode::AbortKnownUncommitted,
            ..Default::default()
        };
        let (service, journal) = service();
        let error = service
            .try_execute_ctas(
                &engine,
                "CREATE TABLE ice.db.dst AS SELECT 1",
                &admitted_context(),
                None,
            )
            .unwrap_err();
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::ManualInspect)
        );
        let operation = journal.only_operation();
        let record = operation_record(&operation);
        assert_eq!(operation.state, OperationState::CommitUnknown);
        assert_eq!(record.phase, CtasSagaPhase::AbortUnknown);
        assert_eq!(record.next_action, StatementNextAction::ManualInspect);
        let abort_fact = record.abort_staging_fact.as_ref().unwrap();
        assert_eq!(abort_fact.outcome, ExternalFactOutcome::CommitUnknown);
        assert!(abort_fact.evidence.is_some());
        assert_eq!(engine.abort_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.reconcile_calls.load(Ordering::SeqCst), 1);
        assert_eq!(journal.list_unfinished().unwrap().len(), 1);
    }
}
