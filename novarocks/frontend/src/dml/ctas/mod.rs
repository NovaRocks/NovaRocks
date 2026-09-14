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

//! Statement-local CTAS publication orchestration.
//!
//! The staged-create provider owns every external effect, including cleanup
//! and no-op decisions. Frontend retains one attempt only for the admitted
//! statement and may perform one read-only adjudication after its exact
//! publication evidence reports `CommitUnknown`.

use crate::dml::attempt::{
    DmlPublicationAdjudicationOutcome, DmlPublicationAttempt, DmlPublicationAttemptError,
    DmlPublicationFinalization,
};
use crate::dml::error::DmlError;
use crate::dml::service::DmlService;
use crate::query_execution::dml::ctas::{
    CtasCommand, CtasEngine, CtasFailure, CtasFailureKind, CtasTargetPreflightFacts,
    CtasTargetPreflightOutcome, PrepareCtasSourceRequest, PreparedCtasSource,
    PreparedStandardCtasTarget, PreparedStandardCtasWrite,
    StandardCtasPublicationAdjudicationOutcome, StandardCtasPublishOutcome,
    StandardCtasStageOutcome, StandardCtasTargetFacts, StandardCtasWriteOutcome,
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::admitted_query_context::RequestContext;
use novarocks_query_application::engine_error::EngineErrorCode;
use novarocks_query_application::sql::dml_admission::DmlAdmissionError;
use novarocks_spi::connector::{
    CreatePolicy, ExternalMutationFinalization, LakePublicationFamily, LakePublicationId,
    LakePublicationStatementTag, LakePublicationTarget,
};

impl DmlService {
    /// Execute CTAS as one statement-local staged-create attempt.
    ///
    /// This intentionally does not create a DML journal operation: a retained
    /// frontend record cannot safely replay, abort, clean up, or adjudicate a
    /// provider-owned staged-create publication after the originating process
    /// has gone away.
    #[allow(
        clippy::result_large_err,
        reason = "Preserves the frozen DML error contract without a broad ABI migration."
    )]
    pub fn try_execute_ctas(
        &self,
        engine: &dyn CtasEngine,
        statement: &novarocks_parser::ast::CreateTableAsSelect,
        source: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        let _ = self;
        let command = CtasCommand::from_typed(statement, source).map_err(|error| {
            DmlError::admit(DmlAdmissionError::CreateTableUnsupportedForm.to_user_error(
                source,
                error.span,
                error.message,
            ))
        })?;
        let session = context.session();
        let policy = if command.if_not_exists {
            CreatePolicy::NoOpIfExists
        } else {
            CreatePolicy::FailIfExists
        };
        execute_standard_ctas_operation(
            engine,
            statement,
            source,
            context,
            query_options,
            command,
            policy,
            session.current_catalog(),
            session.current_database(),
        )
    }
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
#[expect(
    clippy::too_many_arguments,
    reason = "The statement boundary keeps its source and session inputs explicit."
)]
fn execute_standard_ctas_operation(
    engine: &dyn CtasEngine,
    statement: &novarocks_parser::ast::CreateTableAsSelect,
    source_text: &str,
    context: &RequestContext,
    query_options: Option<&QueryOptions>,
    command: CtasCommand,
    policy: CreatePolicy,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), DmlError> {
    let preflight = match engine.preflight_standard_ctas_target(
        statement,
        source_text,
        &command,
        current_catalog,
        current_database,
    ) {
        Ok(CtasTargetPreflightOutcome::Ready(preflight)) => preflight,
        Err(failure) => return Err(ctas_failure(source_text, failure)),
    };
    let publication_id = LakePublicationId::new_v7();
    let mut attempt = new_attempt(publication_id, &preflight.facts)?;

    let prepared_source = match engine.prepare_standard_ctas_source(
        preflight.handle.as_ref(),
        PrepareCtasSourceRequest {
            command,
            current_catalog: current_catalog.map(ToOwned::to_owned),
            current_database: current_database.to_string(),
            query_options: query_options.cloned(),
            execution: context.execution().clone(),
        },
    ) {
        Ok(source) => source,
        Err(failure) => return Err(pre_dispatch_failure(&mut attempt, source_text, failure)),
    };
    if let Err(error) = validate_source_facts(
        &prepared_source,
        &preflight.facts,
        current_catalog,
        current_database,
    ) {
        return Err(pre_dispatch_failure(&mut attempt, source_text, error));
    }

    let target = match engine.prepare_standard_ctas_target(
        prepared_source.handle.as_ref(),
        publication_id,
        policy,
    ) {
        Ok(action) => match engine.stage_standard_ctas_target(action.handle.as_ref()) {
            Ok(StandardCtasStageOutcome::Prepared { target, .. }) => target,
            Ok(StandardCtasStageOutcome::KnownUncommitted { failure })
            | Ok(StandardCtasStageOutcome::CommitUnknown { failure, .. })
            | Err(failure) => return Err(pre_dispatch_failure(&mut attempt, source_text, failure)),
        },
        Err(failure) => return Err(pre_dispatch_failure(&mut attempt, source_text, failure)),
    };
    if let Err(error) = validate_target_facts(&preflight.facts, &target.facts, publication_id) {
        return Err(pre_dispatch_failure(&mut attempt, source_text, error));
    }

    let prepared_write = match engine
        .prepare_standard_ctas_write(prepared_source.handle.as_ref(), target.handle.as_ref())
    {
        Ok(write) => write,
        Err(failure) => return Err(pre_dispatch_failure(&mut attempt, source_text, failure)),
    };
    if let Err(error) = validate_prepared_write(&prepared_source, &target, &prepared_write) {
        return Err(pre_dispatch_failure(&mut attempt, source_text, error));
    }
    let native_bundle = match standard_native_bundle(&prepared_write) {
        Ok(bundle) => bundle,
        Err(failure) => return Err(pre_dispatch_failure(&mut attempt, source_text, failure)),
    };
    if let Err(failure) =
        engine.bind_standard_ctas_write_native_bundle(prepared_write.handle.as_ref(), native_bundle)
    {
        return Err(pre_dispatch_failure(&mut attempt, source_text, failure));
    }

    let write = match engine.execute_standard_ctas_write(prepared_write.handle.as_ref()) {
        StandardCtasWriteOutcome::Completed {
            write,
            execution_identity,
        } => {
            if let Err(error) =
                validate_sealed_write(&prepared_source, &prepared_write, execution_identity)
            {
                return Err(pre_dispatch_failure(&mut attempt, source_text, error));
            }
            write
        }
        StandardCtasWriteOutcome::KnownUncommitted { failure }
        | StandardCtasWriteOutcome::CommitUnknown { failure, .. } => {
            return Err(pre_dispatch_failure(&mut attempt, source_text, failure));
        }
    };

    let publish =
        match engine.prepare_standard_publish_ctas(target.handle.as_ref(), publication_id, write) {
            Ok(publish) => publish,
            Err(failure) => return Err(pre_dispatch_failure(&mut attempt, source_text, failure)),
        };
    finish_standard_publication(engine, &mut attempt, target, publish)
}

#[allow(
    clippy::result_large_err,
    reason = "Preserves the frozen DML error contract without a broad ABI migration."
)]
fn finish_standard_publication(
    engine: &dyn CtasEngine,
    attempt: &mut DmlPublicationAttempt,
    target: PreparedStandardCtasTarget,
    publish: crate::query_execution::dml::ctas::PreparedStandardCtasCatalogAction,
) -> Result<(), DmlError> {
    match engine.publish_standard_ctas(publish.handle.as_ref()) {
        Ok(StandardCtasPublishOutcome::KnownUncommitted { failure }) => {
            Err(pre_dispatch_failure(attempt, "", failure))
        }
        Ok(StandardCtasPublishOutcome::Applied { finalization, .. })
        | Ok(StandardCtasPublishOutcome::NoOp { finalization, .. }) => {
            mark_dispatch_possible(attempt)?;
            finish_known_committed(attempt, finalization)
        }
        Ok(StandardCtasPublishOutcome::CommitUnknown {
            failure: _,
            evidence,
        }) => {
            mark_dispatch_possible(attempt)?;
            let adjudication = attempt.begin_adjudication().map_err(attempt_error)?;
            match engine.adjudicate_standard_ctas_publication(target.handle.as_ref(), evidence) {
                Ok(StandardCtasPublicationAdjudicationOutcome::Published {
                    finalization, ..
                }) => {
                    let terminal = attempt
                        .finish_adjudication(
                            adjudication,
                            DmlPublicationAdjudicationOutcome::KnownCommitted,
                            finalization_state(&finalization),
                        )
                        .map_err(attempt_error)?
                        .clone();
                    finalization_error_or_ok(terminal, finalization)
                }
                Ok(StandardCtasPublicationAdjudicationOutcome::CommitUnknown { failure }) => {
                    let terminal = attempt
                        .finish_adjudication(
                            adjudication,
                            DmlPublicationAdjudicationOutcome::CommitUnknown,
                            DmlPublicationFinalization::NotApplicable,
                        )
                        .map_err(attempt_error)?
                        .clone();
                    Err(unknown_failure(
                        terminal,
                        "CTAS publication adjudication",
                        failure,
                    ))
                }
                Err(error) => {
                    let terminal = attempt
                        .finish_adjudication(
                            adjudication,
                            DmlPublicationAdjudicationOutcome::CommitUnknown,
                            DmlPublicationFinalization::NotApplicable,
                        )
                        .map_err(attempt_error)?
                        .clone();
                    Err(DmlError::commit(format!(
                        "CTAS publication adjudication remains unresolved: {}",
                        error.message
                    ))
                    .with_engine_error_code(EngineErrorCode::CommitUnknown)
                    .with_publication_terminal(terminal))
                }
            }
        }
        Err(error) => {
            mark_dispatch_possible(attempt)?;
            let terminal = attempt
                .terminal_after_outer_failure()
                .map_err(attempt_error)?
                .clone();
            Err(DmlError::commit(format!(
                "CTAS publication dispatch remains unresolved: {}",
                error.message
            ))
            .with_engine_error_code(EngineErrorCode::CommitUnknown)
            .with_publication_terminal(terminal))
        }
    }
}

fn new_attempt(
    publication_id: LakePublicationId,
    facts: &CtasTargetPreflightFacts,
) -> Result<DmlPublicationAttempt, DmlError> {
    let target = LakePublicationTarget::try_new(
        facts.instance_id.clone(),
        facts.target_namespace.clone(),
        Some(facts.target_table.clone()),
        None,
    )
    .map_err(DmlError::executor)?;
    let tag =
        LakePublicationStatementTag::try_new("ctas".to_string()).map_err(DmlError::executor)?;
    Ok(DmlPublicationAttempt::new(
        publication_id,
        LakePublicationFamily::Ctas,
        target,
        Some(tag),
    ))
}

fn mark_dispatch_possible(attempt: &mut DmlPublicationAttempt) -> Result<(), DmlError> {
    attempt.mark_dispatch_possible().map_err(attempt_error)
}

fn finish_known_committed(
    attempt: &mut DmlPublicationAttempt,
    finalization: ExternalMutationFinalization,
) -> Result<(), DmlError> {
    let terminal = attempt
        .terminal_known_committed(finalization_state(&finalization))
        .map_err(attempt_error)?
        .clone();
    finalization_error_or_ok(terminal, finalization)
}

fn finalization_error_or_ok(
    terminal: novarocks_spi::connector::LakePublicationTerminal,
    finalization: ExternalMutationFinalization,
) -> Result<(), DmlError> {
    match finalization {
        ExternalMutationFinalization::Complete => Ok(()),
        ExternalMutationFinalization::Failed(failure) => Err(
            DmlError::known_committed_finalization_failed(terminal, failure.to_string())
                .with_engine_error_code(EngineErrorCode::CommitKnownCommittedFinalizeFailed),
        ),
    }
}

fn finalization_state(finalization: &ExternalMutationFinalization) -> DmlPublicationFinalization {
    match finalization {
        ExternalMutationFinalization::Complete => DmlPublicationFinalization::Succeeded,
        ExternalMutationFinalization::Failed(_) => DmlPublicationFinalization::Failed,
    }
}

fn pre_dispatch_failure(
    attempt: &mut DmlPublicationAttempt,
    source_text: &str,
    failure: CtasFailure,
) -> DmlError {
    let terminal = attempt
        .terminal_pre_dispatch_uncommitted()
        .expect("CTAS pre-publication path assigns one terminal")
        .clone();
    ctas_failure(source_text, failure).with_publication_terminal(terminal)
}

fn unknown_failure(
    terminal: novarocks_spi::connector::LakePublicationTerminal,
    label: &str,
    failure: CtasFailure,
) -> DmlError {
    DmlError::commit(format!("{label} remains unresolved: {}", failure.message))
        .with_engine_error_code(EngineErrorCode::CommitUnknown)
        .with_publication_terminal(terminal)
}

fn ctas_failure(source_text: &str, failure: CtasFailure) -> DmlError {
    if let Some(error) = failure.user_error(Some(source_text)) {
        return DmlError::admit(error);
    }
    DmlError::executor(format!("CTAS request failed: {}", failure.message))
        .with_engine_error_code(EngineErrorCode::CommitKnownUncommitted)
}

fn attempt_error(error: DmlPublicationAttemptError) -> DmlError {
    DmlError::executor(format!(
        "invalid CTAS publication attempt transition: {error}"
    ))
}

fn validate_source_facts(
    source: &PreparedCtasSource,
    preflight: &CtasTargetPreflightFacts,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), CtasFailure> {
    let matching = source.handle.execution_identity() == source.facts.execution_identity
        && source.facts.target_catalog == preflight.instance_id
        && source.facts.target_namespace == preflight.target_namespace
        && source.facts.target_table == preflight.target_table
        && source.facts.source_catalog.as_deref() == current_catalog
        && source.facts.source_database == current_database
        && !source.facts.output_columns.is_empty();
    if matching {
        Ok(())
    } else {
        Err(internal_failure(
            "CTAS prepared source facts conflict with its exact preflight target",
        ))
    }
}

fn validate_target_facts(
    preflight: &CtasTargetPreflightFacts,
    target: &StandardCtasTargetFacts,
    publication_id: LakePublicationId,
) -> Result<(), CtasFailure> {
    if target.publication_id == publication_id
        && target.provider_id == preflight.provider_id
        && target.instance_id == preflight.instance_id
        && target.control_runtime_id == preflight.control_runtime_id
    {
        Ok(())
    } else {
        Err(internal_failure(
            "standard CTAS staged target facts conflict with the statement publication identity",
        ))
    }
}

fn validate_prepared_write(
    source: &PreparedCtasSource,
    target: &PreparedStandardCtasTarget,
    write: &PreparedStandardCtasWrite,
) -> Result<(), CtasFailure> {
    if write.execution_identity == source.facts.execution_identity
        && write.handle.execution_identity() == source.facts.execution_identity
        && write.target_facts == target.facts
    {
        Ok(())
    } else {
        Err(internal_failure(
            "standard CTAS prepared write facts drifted from source or staged target",
        ))
    }
}

/// Check that the sealed write came from the execution this statement started.
///
/// There is nothing else left to check here. The write proof is a receipt and a
/// row count; whether it belongs to *this* staged target is decided by the
/// target itself, which bound one receipt and refuses to publish another. This
/// layer knows only which execution produced it.
fn validate_sealed_write(
    source: &PreparedCtasSource,
    prepared: &PreparedStandardCtasWrite,
    execution_identity: [u8; 32],
) -> Result<(), CtasFailure> {
    if execution_identity == source.facts.execution_identity
        && execution_identity == prepared.execution_identity
    {
        Ok(())
    } else {
        Err(internal_failure(
            "standard CTAS sealed write came from a different execution",
        ))
    }
}

fn standard_native_bundle(
    prepared: &PreparedStandardCtasWrite,
) -> Result<crate::query_execution::native_fragment::NativeFragmentAttachment, CtasFailure> {
    let encoding = prepared.handle.native_encoding()?;
    let input = encoding.input()?;
    crate::native::fragment_encoder::encode_native_fragment_bundle_for_input(input).map_err(
        |message| CtasFailure {
            kind: CtasFailureKind::Internal,
            message,
            user_error: None,
        },
    )
}

fn internal_failure(message: impl Into<String>) -> CtasFailure {
    CtasFailure {
        kind: CtasFailureKind::Internal,
        message: message.into(),
        user_error: None,
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ExternalMutationEvidence, LakePublicationDisposition,
        ProviderBindingEpoch,
    };

    use super::*;

    struct TestTarget;

    impl crate::query_execution::dml::ctas::CtasPreparedTarget for TestTarget {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct TestAction;

    impl crate::query_execution::dml::ctas::CtasPreparedCatalogAction for TestAction {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct PublishUnknownEngine {
        publishes: AtomicUsize,
        adjudications: AtomicUsize,
    }

    impl PublishUnknownEngine {
        fn evidence() -> ExternalMutationEvidence {
            ExternalMutationEvidence::try_new(
                1,
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                    instance_id: ConnectorInstanceId::parse("iceberg").expect("instance"),
                },
                ProviderBindingEpoch::new(),
                ConnectorMutationOperationId::from_bytes([7; 16]),
                "staged-create-publish",
                Bytes::from_static(b"opaque"),
            )
            .expect("evidence")
        }
    }

    impl CtasEngine for PublishUnknownEngine {
        fn publish_standard_ctas(
            &self,
            _action: &dyn crate::query_execution::dml::ctas::CtasPreparedCatalogAction,
        ) -> Result<StandardCtasPublishOutcome, CtasFailure> {
            self.publishes.fetch_add(1, Ordering::SeqCst);
            Ok(StandardCtasPublishOutcome::CommitUnknown {
                failure: internal_failure("publish response lost"),
                evidence: Self::evidence(),
            })
        }

        fn adjudicate_standard_ctas_publication(
            &self,
            _target: &dyn crate::query_execution::dml::ctas::CtasPreparedTarget,
            _evidence: ExternalMutationEvidence,
        ) -> Result<StandardCtasPublicationAdjudicationOutcome, CtasFailure> {
            self.adjudications.fetch_add(1, Ordering::SeqCst);
            Ok(StandardCtasPublicationAdjudicationOutcome::CommitUnknown {
                failure: internal_failure("exact publication remains unproven"),
            })
        }
    }

    fn attempt() -> DmlPublicationAttempt {
        new_attempt(
            LakePublicationId::new_v7(),
            &CtasTargetPreflightFacts {
                provider_id: "iceberg".to_string(),
                instance_id: "iceberg".to_string(),
                control_runtime_id: [1; 16],
                capability_version: 1,
                target_namespace: "db".to_string(),
                target_table: "target".to_string(),
            },
        )
        .expect("attempt")
    }

    #[test]
    fn unknown_publication_adjudicates_once_without_a_follow_up_mutation() {
        let engine = PublishUnknownEngine {
            publishes: AtomicUsize::new(0),
            adjudications: AtomicUsize::new(0),
        };
        let mut attempt = attempt();
        let target = PreparedStandardCtasTarget {
            facts: StandardCtasTargetFacts {
                provider_id: "iceberg".to_string(),
                instance_id: "iceberg".to_string(),
                control_runtime_id: [1; 16],
                publication_id: attempt.header().publication_id(),
                target_handle_digest: [2; 32],
            },
            handle: Arc::new(TestTarget),
        };
        let publish = crate::query_execution::dml::ctas::PreparedStandardCtasCatalogAction {
            input_digest: [3; 32],
            handle: Arc::new(TestAction),
        };

        let error = finish_standard_publication(&engine, &mut attempt, target, publish)
            .expect_err("unproven publication remains unknown");

        assert_eq!(engine.publishes.load(Ordering::SeqCst), 1);
        assert_eq!(engine.adjudications.load(Ordering::SeqCst), 1);
        assert_eq!(
            error
                .publication_terminal()
                .expect("terminal")
                .disposition(),
            LakePublicationDisposition::CommitUnknown
        );
    }

    #[test]
    fn finalization_failure_keeps_known_committed_terminal() {
        let mut attempt = attempt();
        attempt.mark_dispatch_possible().expect("dispatch possible");

        let error = finish_known_committed(
            &mut attempt,
            ExternalMutationFinalization::Failed(
                novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    "local finalization failed",
                ),
            ),
        )
        .expect_err("finalization failure is reported");

        assert_eq!(
            error
                .publication_terminal()
                .expect("terminal")
                .disposition(),
            LakePublicationDisposition::KnownCommitted
        );
    }
}
