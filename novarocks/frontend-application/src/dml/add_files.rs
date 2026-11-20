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

//! Statement-local `ALTER TABLE ... ADD FILES` publication flow.
//!
//! Source data is caller-owned. This flow never creates a cleanup or delete
//! intent for it, including after an unknown or a negative adjudication.

use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadByteBudget, ConnectorPayloadRetentionGuard,
    LakePublicationFamily, LakePublicationId, LakePublicationStatementTag, LakePublicationTarget,
    LakePublicationTerminal,
};

use crate::dml::attempt::{
    DmlPublicationAdjudication, DmlPublicationAdjudicationOutcome, DmlPublicationAttempt,
    DmlPublicationFinalization,
};
use crate::dml::error::DmlError;
use crate::dml::service::DmlService;
use crate::query_execution::dml::add_files::{
    AddFilesCommand, AddFilesDispatchState, AddFilesEngine, AddFilesFailure, AddFilesFinalization,
    AddFilesOutcome, AddFilesPlanError, AddFilesPlanFacts, PlanAddFilesRequest, PreparedAddFiles,
    ResolvedAddFiles,
};
use novarocks_query_application::admitted_query_context::RequestContext;
use novarocks_workload_control::{CancellationReason, ResultWindowAlias, WorkError};
use std::sync::Arc;

struct RegistrationByteAuthor(ResultWindowAlias);

impl ConnectorPayloadByteBudget for RegistrationByteAuthor {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        let bytes = u64::try_from(bytes)
            .map_err(|_| registration_work_error(WorkError::ArithmeticOverflow))?;
        let receipt = self
            .0
            .reserve_payload_bytes(bytes)
            .map_err(registration_work_error)?;
        Ok(ConnectorPayloadRetentionGuard::new(receipt))
    }
}

pub(crate) fn registration_byte_author(
    window: ResultWindowAlias,
) -> ConnectorPayloadRetentionGuard {
    ConnectorPayloadRetentionGuard::from_byte_budget(Arc::new(RegistrationByteAuthor(window)))
}

fn registration_work_error(error: WorkError) -> ConnectorError {
    let kind = match &error {
        WorkError::Capacity(_) | WorkError::CapacityWaitTimeout | WorkError::ArithmeticOverflow => {
            ConnectorErrorKind::ResourceExhausted
        }
        WorkError::Cancelled(
            CancellationReason::DeadlineExceeded
            | CancellationReason::FrontendDrainDeadlineExceeded,
        ) => ConnectorErrorKind::DeadlineExceeded,
        WorkError::Cancelled(_) | WorkError::Closed | WorkError::Released => {
            ConnectorErrorKind::Cancelled
        }
        _ => ConnectorErrorKind::Internal,
    };
    ConnectorError::from_domain(kind, error)
}

/// A planned ADD FILES operation whose provider dispatch has not begun.
///
/// It retains the exact provider plan and publication identity, so a caller
/// may cancel before dispatch without turning a non-effect into a retryable
/// post-dispatch operation.
pub(crate) struct PreparedAddFilesAttempt {
    prepared: PreparedAddFiles,
    attempt: DmlPublicationAttempt,
}

impl DmlService {
    #[cfg(test)]
    fn prepare_add_files(
        &self,
        engine: &dyn AddFilesEngine,
        command: AddFilesCommand,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<PreparedAddFilesAttempt, DmlError> {
        self.prepare_add_files_request(engine, command, None, context, query_options)
    }

    /// Use the exact target retained during TABLE EXECUTE argument binding.
    pub(crate) fn prepare_resolved_add_files(
        &self,
        engine: &dyn AddFilesEngine,
        resolved: ResolvedAddFiles,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<PreparedAddFilesAttempt, DmlError> {
        let command = AddFilesCommand {
            table_parts: vec![
                resolved.target.catalog.clone(),
                resolved.target.namespace.clone(),
                resolved.target.table.clone(),
            ],
            arguments: resolved.arguments,
        };
        self.prepare_add_files_request(
            engine,
            command,
            Some((resolved.target, resolved.planning)),
            context,
            query_options,
        )
    }

    fn prepare_add_files_request(
        &self,
        engine: &dyn AddFilesEngine,
        command: AddFilesCommand,
        retained: Option<(
            crate::catalog_application::resolver::TargetBackend,
            novarocks_spi::connector::ConnectorControlPlanningPin,
        )>,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<PreparedAddFilesAttempt, DmlError> {
        let publication_id = LakePublicationId::new_v7();
        let prepared = engine
            .plan_add_files(PlanAddFilesRequest {
                command,
                retained,
                mutation_operation_id: publication_id.to_bytes(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
            })
            .map_err(plan_error)?;

        let attempt = attempt(&prepared.facts, publication_id)?;
        Ok(PreparedAddFilesAttempt { prepared, attempt })
    }

    /// Crosses the one-shot provider dispatch boundary for planned ADD FILES.
    pub(crate) fn execute_prepared_add_files(
        &self,
        engine: &dyn AddFilesEngine,
        prepared: PreparedAddFilesAttempt,
        byte_author: ConnectorPayloadRetentionGuard,
    ) -> Result<u32, DmlError> {
        let PreparedAddFilesAttempt {
            prepared,
            mut attempt,
        } = prepared;
        attempt
            .mark_dispatch_possible()
            .map_err(DmlError::executor)?;
        finish(engine, prepared, &mut attempt, byte_author)
    }
}

fn attempt(
    facts: &AddFilesPlanFacts,
    publication_id: LakePublicationId,
) -> Result<DmlPublicationAttempt, DmlError> {
    let target = LakePublicationTarget::try_new(
        facts.catalog.clone(),
        facts.namespace.clone(),
        Some(facts.table.clone()),
        None,
    )
    .map_err(DmlError::executor)?;
    let tag = LakePublicationStatementTag::try_new("add_files".to_string())
        .map_err(DmlError::executor)?;
    Ok(DmlPublicationAttempt::new(
        publication_id,
        LakePublicationFamily::DataMutation,
        target,
        Some(tag),
    ))
}

fn finish(
    engine: &dyn AddFilesEngine,
    prepared: PreparedAddFiles,
    attempt: &mut DmlPublicationAttempt,
    byte_author: ConnectorPayloadRetentionGuard,
) -> Result<u32, DmlError> {
    match engine.execute_add_files(prepared.handle.as_ref(), byte_author) {
        AddFilesOutcome::KnownCommitted {
            receipt,
            finalization,
            ..
        } => committed(attempt, receipt.summary.file_count, finalization),
        AddFilesOutcome::CommitUnknown {
            failure: _,
            evidence,
        } => {
            let capability = attempt.begin_adjudication().map_err(DmlError::executor)?;
            match engine.adjudicate_add_files(prepared.handle.as_ref(), &evidence) {
                AddFilesOutcome::KnownCommitted {
                    receipt,
                    finalization,
                    ..
                } => committed_after_adjudication(
                    attempt,
                    capability,
                    receipt.summary.file_count,
                    finalization,
                ),
                AddFilesOutcome::CommitUnknown { failure, .. }
                | AddFilesOutcome::KnownUncommitted { failure }
                | AddFilesOutcome::ContractFailure { failure, .. } => {
                    unknown_after_adjudication(attempt, capability, failure)
                }
            }
        }
        AddFilesOutcome::KnownUncommitted { failure }
        | AddFilesOutcome::ContractFailure {
            failure,
            dispatch: AddFilesDispatchState::ConfirmedNotDispatched,
        } => failed(attempt.terminal_known_uncommitted(), failure),
        AddFilesOutcome::ContractFailure {
            failure,
            dispatch: AddFilesDispatchState::PossiblyDispatched,
        } => failed(attempt.terminal_commit_unknown(), failure),
    }
}

fn committed(
    attempt: &mut DmlPublicationAttempt,
    file_count: u32,
    finalization: AddFilesFinalization,
) -> Result<u32, DmlError> {
    let terminal = attempt
        .terminal_known_committed(finalization_state(&finalization))
        .map_err(DmlError::executor)?
        .clone();
    finalization_error(terminal, finalization, file_count)
}

fn committed_after_adjudication(
    attempt: &mut DmlPublicationAttempt,
    capability: DmlPublicationAdjudication,
    file_count: u32,
    finalization: AddFilesFinalization,
) -> Result<u32, DmlError> {
    let terminal = attempt
        .finish_adjudication(
            capability,
            DmlPublicationAdjudicationOutcome::KnownCommitted,
            finalization_state(&finalization),
        )
        .map_err(DmlError::executor)?
        .clone();
    finalization_error(terminal, finalization, file_count)
}

fn failed(
    terminal: Result<&LakePublicationTerminal, crate::dml::attempt::DmlPublicationAttemptError>,
    failure: AddFilesFailure,
) -> Result<u32, DmlError> {
    let terminal = terminal.map_err(DmlError::executor)?.clone();
    Err(DmlError::from_application(failure.into_service()).with_publication_terminal(terminal))
}

fn unknown_after_adjudication(
    attempt: &mut DmlPublicationAttempt,
    capability: DmlPublicationAdjudication,
    failure: AddFilesFailure,
) -> Result<u32, DmlError> {
    failed(
        attempt.finish_adjudication(
            capability,
            DmlPublicationAdjudicationOutcome::CommitUnknown,
            DmlPublicationFinalization::NotApplicable,
        ),
        failure,
    )
}

fn finalization_state(finalization: &AddFilesFinalization) -> DmlPublicationFinalization {
    match finalization {
        AddFilesFinalization::Complete => DmlPublicationFinalization::Succeeded,
        AddFilesFinalization::Failed(_) => DmlPublicationFinalization::Failed,
    }
}

fn finalization_error(
    terminal: LakePublicationTerminal,
    finalization: AddFilesFinalization,
    file_count: u32,
) -> Result<u32, DmlError> {
    match finalization {
        AddFilesFinalization::Complete => Ok(file_count),
        AddFilesFinalization::Failed(failure) => {
            Err(DmlError::from_application(failure.into_service())
                .with_publication_terminal(terminal))
        }
    }
}

fn plan_error(error: AddFilesPlanError) -> DmlError {
    match error {
        AddFilesPlanError::KnownUncommitted(failure)
        | AddFilesPlanError::ContractFailure { failure, .. } => {
            DmlError::from_application(failure.into_service())
        }
    }
}

#[cfg(test)]
mod tests;
