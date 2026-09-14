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
    AddFilesCommand, AddFilesEngine, AddFilesFailure, AddFilesFinalization, AddFilesOutcome,
    AddFilesPlanError, AddFilesPlanFacts, PlanAddFilesRequest, PreparedAddFiles,
};
use novarocks_query_application::admitted_query_context::RequestContext;

impl DmlService {
    /// Executes an admitted ADD FILES statement as one non-durable attempt.
    #[allow(clippy::result_large_err)]
    pub fn execute_add_files(
        &self,
        engine: &dyn AddFilesEngine,
        command: AddFilesCommand,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<u32, DmlError> {
        if !is_secret_free_source_location(&command.location) {
            return Err(DmlError::executor(
                "ADD FILES source location must not contain credentials or query parameters",
            ));
        }
        let publication_id = LakePublicationId::new_v7();
        let session = context.session();
        let prepared = engine
            .plan_add_files(PlanAddFilesRequest {
                command,
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                mutation_operation_id: publication_id.to_bytes(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
            })
            .map_err(plan_error)?;

        let mut attempt = attempt(&prepared.facts, publication_id)?;
        attempt
            .mark_dispatch_possible()
            .map_err(DmlError::executor)?;
        finish(engine, prepared, &mut attempt)
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
) -> Result<u32, DmlError> {
    match engine.execute_add_files(prepared.handle.as_ref()) {
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
        // This boundary is deliberate: a plan was retained and execute was
        // invoked, so even an adapter-local failure cannot grant a retry.
        AddFilesOutcome::KnownUncommitted { failure }
        | AddFilesOutcome::ContractFailure { failure, .. } => unknown(attempt, failure),
    }
}

fn committed(
    attempt: &mut DmlPublicationAttempt,
    file_count: u32,
    finalization: AddFilesFinalization,
) -> Result<u32, DmlError> {
    let finalization = finalization_state(&finalization);
    let terminal = attempt
        .terminal_known_committed(finalization)
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
    let finalization = finalization_state(&finalization);
    let terminal = attempt
        .finish_adjudication(
            capability,
            DmlPublicationAdjudicationOutcome::KnownCommitted,
            finalization,
        )
        .map_err(DmlError::executor)?
        .clone();
    finalization_error(terminal, finalization, file_count)
}

fn unknown(attempt: &mut DmlPublicationAttempt, failure: AddFilesFailure) -> Result<u32, DmlError> {
    let terminal = attempt
        .terminal_commit_unknown()
        .map_err(DmlError::executor)?
        .clone();
    Err(DmlError::commit(failure.message).with_publication_terminal(terminal))
}

fn unknown_after_adjudication(
    attempt: &mut DmlPublicationAttempt,
    capability: DmlPublicationAdjudication,
    failure: AddFilesFailure,
) -> Result<u32, DmlError> {
    let terminal = attempt
        .finish_adjudication(
            capability,
            DmlPublicationAdjudicationOutcome::CommitUnknown,
            DmlPublicationFinalization::NotApplicable,
        )
        .map_err(DmlError::executor)?
        .clone();
    Err(DmlError::commit(failure.message).with_publication_terminal(terminal))
}

fn finalization_state(finalization: &AddFilesFinalization) -> DmlPublicationFinalization {
    match finalization {
        AddFilesFinalization::Complete => DmlPublicationFinalization::Succeeded,
        AddFilesFinalization::Failed(_) => DmlPublicationFinalization::Failed,
    }
}

fn finalization_error(
    terminal: LakePublicationTerminal,
    finalization: DmlPublicationFinalization,
    file_count: u32,
) -> Result<u32, DmlError> {
    if finalization == DmlPublicationFinalization::Failed {
        return Err(DmlError::known_committed_finalization_failed(
            terminal,
            "ADD FILES finalization failed",
        ));
    }
    Ok(file_count)
}

fn plan_error(error: AddFilesPlanError) -> DmlError {
    match error {
        AddFilesPlanError::KnownUncommitted(failure)
        | AddFilesPlanError::ContractFailure { failure, .. } => DmlError::executor(failure.message),
    }
}

fn is_secret_free_source_location(location: &str) -> bool {
    !location.contains('@') && !location.contains('?')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caller_source_validation_does_not_treat_path_spelling_as_credentials() {
        assert!(is_secret_free_source_location(
            "s3://bucket/secret-data/files"
        ));
        assert!(!is_secret_free_source_location(
            "s3://key:secret@bucket/files"
        ));
        assert!(!is_secret_free_source_location(
            "s3://bucket/files?token=secret"
        ));
    }
}
