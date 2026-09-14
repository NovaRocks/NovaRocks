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

//! Statement-local `TRUNCATE TABLE` publication flow.
//!
//! Planning is read-only. Once this module marks dispatch possible, no error
//! or negative observation can authorize a follow-up mutation: the only
//! allowed follow-up is one read-only adjudication on the retained session.

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
use crate::query_execution::dml::truncate::{
    PlanTruncateRequest, PreparedTruncate, TruncateCommand, TruncateEngine, TruncateFailure,
    TruncateFinalization, TruncateOutcome, TruncatePlanError, TruncatePlanFacts,
};
use novarocks_query_application::admitted_query_context::RequestContext;

impl DmlService {
    /// Executes an admitted TRUNCATE as one non-durable statement attempt.
    #[allow(clippy::result_large_err)]
    pub fn execute_truncate(
        &self,
        engine: &dyn TruncateEngine,
        command: TruncateCommand,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<(), DmlError> {
        let publication_id = LakePublicationId::new_v7();
        let session = context.session();
        let prepared = engine
            .plan_truncate(PlanTruncateRequest {
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
    facts: &TruncatePlanFacts,
    publication_id: LakePublicationId,
) -> Result<DmlPublicationAttempt, DmlError> {
    let target = LakePublicationTarget::try_new(
        facts.catalog.clone(),
        facts.namespace.clone(),
        Some(facts.table.clone()),
        Some(facts.target_ref.clone()),
    )
    .map_err(DmlError::executor)?;
    let tag =
        LakePublicationStatementTag::try_new("truncate".to_string()).map_err(DmlError::executor)?;
    Ok(DmlPublicationAttempt::new(
        publication_id,
        LakePublicationFamily::DataMutation,
        target,
        Some(tag),
    ))
}

fn finish(
    engine: &dyn TruncateEngine,
    prepared: PreparedTruncate,
    attempt: &mut DmlPublicationAttempt,
) -> Result<(), DmlError> {
    match engine.execute_truncate(prepared.handle.as_ref()) {
        TruncateOutcome::KnownCommitted { finalization, .. } => committed(attempt, finalization),
        TruncateOutcome::CommitUnknown {
            failure: _,
            evidence,
        } => {
            let capability = attempt.begin_adjudication().map_err(DmlError::executor)?;
            match engine.adjudicate_truncate(prepared.handle.as_ref(), &evidence) {
                TruncateOutcome::KnownCommitted { finalization, .. } => {
                    committed_after_adjudication(attempt, capability, finalization)
                }
                TruncateOutcome::CommitUnknown { failure, .. }
                | TruncateOutcome::KnownUncommitted { failure }
                | TruncateOutcome::ContractFailure { failure, .. } => {
                    unknown_after_adjudication(attempt, capability, failure)
                }
            }
        }
        // Dispatch was fenced before the call. A provider's claimed
        // known-uncommitted result cannot relax that conservative boundary.
        TruncateOutcome::KnownUncommitted { failure }
        | TruncateOutcome::ContractFailure { failure, .. } => unknown(attempt, failure),
    }
}

fn committed(
    attempt: &mut DmlPublicationAttempt,
    finalization: TruncateFinalization,
) -> Result<(), DmlError> {
    let finalization = finalization_state(&finalization);
    let terminal = attempt
        .terminal_known_committed(finalization)
        .map_err(DmlError::executor)?
        .clone();
    finalization_error(terminal, finalization)
}

fn committed_after_adjudication(
    attempt: &mut DmlPublicationAttempt,
    capability: DmlPublicationAdjudication,
    finalization: TruncateFinalization,
) -> Result<(), DmlError> {
    let finalization = finalization_state(&finalization);
    let terminal = attempt
        .finish_adjudication(
            capability,
            DmlPublicationAdjudicationOutcome::KnownCommitted,
            finalization,
        )
        .map_err(DmlError::executor)?
        .clone();
    finalization_error(terminal, finalization)
}

fn unknown(attempt: &mut DmlPublicationAttempt, failure: TruncateFailure) -> Result<(), DmlError> {
    let terminal = attempt
        .terminal_commit_unknown()
        .map_err(DmlError::executor)?
        .clone();
    Err(DmlError::commit(failure.message).with_publication_terminal(terminal))
}

fn unknown_after_adjudication(
    attempt: &mut DmlPublicationAttempt,
    capability: DmlPublicationAdjudication,
    failure: TruncateFailure,
) -> Result<(), DmlError> {
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

fn finalization_state(finalization: &TruncateFinalization) -> DmlPublicationFinalization {
    match finalization {
        TruncateFinalization::Complete => DmlPublicationFinalization::Succeeded,
        TruncateFinalization::Failed(_) => DmlPublicationFinalization::Failed,
    }
}

fn finalization_error(
    terminal: LakePublicationTerminal,
    finalization: DmlPublicationFinalization,
) -> Result<(), DmlError> {
    if finalization == DmlPublicationFinalization::Failed {
        return Err(DmlError::known_committed_finalization_failed(
            terminal,
            "TRUNCATE finalization failed",
        ));
    }
    Ok(())
}

fn plan_error(error: TruncatePlanError) -> DmlError {
    match error {
        TruncatePlanError::KnownUncommitted(failure)
        | TruncatePlanError::ContractFailure { failure, .. } => DmlError::executor(failure.message),
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::query_execution::dml::truncate::{
        PreparedTruncate, TruncateEffect, TruncateEvidence, TruncateFailureKind,
        TruncatePlanSummary, TruncatePrepared, TruncateReceipt,
    };
    use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
    use novarocks_query_application::api::BackendTopologySnapshot;
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use novarocks_spi::connector::LakePublicationDisposition;
    use novarocks_types::ClusterRole;

    #[derive(Clone, Copy)]
    enum Mode {
        Committed,
        CommittedFinalizationFailed,
        CommitUnknown,
        KnownUncommitted,
    }

    struct FakePrepared;

    impl TruncatePrepared for FakePrepared {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct FakeTruncateEngine {
        execute: Mode,
        adjudicate: Mode,
        plan_calls: AtomicUsize,
        execute_calls: AtomicUsize,
        adjudicate_calls: AtomicUsize,
    }

    impl FakeTruncateEngine {
        fn new(execute: Mode, adjudicate: Mode) -> Self {
            Self {
                execute,
                adjudicate,
                plan_calls: AtomicUsize::new(0),
                execute_calls: AtomicUsize::new(0),
                adjudicate_calls: AtomicUsize::new(0),
            }
        }

        fn outcome(&self, mode: Mode, facts: &TruncatePlanFacts) -> TruncateOutcome {
            match mode {
                Mode::Committed | Mode::CommittedFinalizationFailed => {
                    TruncateOutcome::KnownCommitted {
                        effect: TruncateEffect::Applied,
                        receipt: receipt(facts),
                        finalization: if matches!(mode, Mode::CommittedFinalizationFailed) {
                            TruncateFinalization::Failed(failure("cache invalidation failed"))
                        } else {
                            TruncateFinalization::Complete
                        },
                    }
                }
                Mode::CommitUnknown => TruncateOutcome::CommitUnknown {
                    failure: failure("catalog response lost"),
                    evidence: TruncateEvidence {
                        schema_version: 1,
                        digest: [7; 32],
                        wire_bytes: vec![1, 2, 3],
                    },
                },
                Mode::KnownUncommitted => TruncateOutcome::KnownUncommitted {
                    failure: failure("catalog rejected mutation"),
                },
            }
        }
    }

    impl TruncateEngine for FakeTruncateEngine {
        fn plan_truncate(
            &self,
            request: PlanTruncateRequest,
        ) -> Result<PreparedTruncate, TruncatePlanError> {
            self.plan_calls.fetch_add(1, Ordering::SeqCst);
            Ok(PreparedTruncate {
                facts: TruncatePlanFacts {
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "orders".to_string(),
                    target_ref: request.command.target_ref,
                    provider_id: "iceberg".to_string(),
                    instance_id: "ice".to_string(),
                    incarnation: [1; 16],
                    mutation_operation_id: request.mutation_operation_id,
                    request_digest: [2; 32],
                    plan_digest: [3; 32],
                    state_digest: [4; 32],
                    summary: TruncatePlanSummary {
                        file_count: 3,
                        row_count: 5,
                        total_bytes: 8,
                    },
                },
                handle: Arc::new(FakePrepared),
            })
        }

        fn execute_truncate(&self, _prepared: &dyn TruncatePrepared) -> TruncateOutcome {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            self.outcome(self.execute, &facts())
        }

        fn adjudicate_truncate(
            &self,
            _prepared: &dyn TruncatePrepared,
            _evidence: &TruncateEvidence,
        ) -> TruncateOutcome {
            self.adjudicate_calls.fetch_add(1, Ordering::SeqCst);
            self.outcome(self.adjudicate, &facts())
        }
    }

    fn facts() -> TruncatePlanFacts {
        TruncatePlanFacts {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            target_ref: "main".to_string(),
            provider_id: "iceberg".to_string(),
            instance_id: "ice".to_string(),
            incarnation: [1; 16],
            mutation_operation_id: [0; 16],
            request_digest: [2; 32],
            plan_digest: [3; 32],
            state_digest: [4; 32],
            summary: TruncatePlanSummary {
                file_count: 3,
                row_count: 5,
                total_bytes: 8,
            },
        }
    }

    fn receipt(facts: &TruncatePlanFacts) -> TruncateReceipt {
        TruncateReceipt {
            provider_id: facts.provider_id.clone(),
            instance_id: facts.instance_id.clone(),
            incarnation: facts.incarnation,
            mutation_operation_id: facts.mutation_operation_id,
            operation_kind: "truncate".to_string(),
            request_digest: facts.request_digest,
            plan_digest: facts.plan_digest,
            state_digest: facts.state_digest,
            summary: facts.summary,
            opaque_payload: vec![9],
            opaque_payload_digest: [10; 32],
        }
    }

    fn failure(message: &str) -> TruncateFailure {
        TruncateFailure {
            kind: TruncateFailureKind::Unavailable,
            message: message.to_string(),
        }
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

    fn command() -> TruncateCommand {
        TruncateCommand {
            target_parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()],
            target_ref: "main".to_string(),
        }
    }

    #[test]
    fn direct_truncate_uses_data_mutation_marker_family() {
        assert_eq!(
            LakePublicationFamily::DataMutation.as_str(),
            "data_mutation"
        );
    }

    #[test]
    fn unknown_is_adjudicated_once_and_exact_positive_commits_without_cleanup() {
        let engine = FakeTruncateEngine::new(Mode::CommitUnknown, Mode::Committed);
        DmlService::new()
            .execute_truncate(&engine, command(), &context(), None)
            .expect("exact positive adjudication commits");
        assert_eq!(engine.plan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unknown_or_negative_adjudication_never_retries_or_mutates() {
        let engine = FakeTruncateEngine::new(Mode::CommitUnknown, Mode::KnownUncommitted);
        let error = DmlService::new()
            .execute_truncate(&engine, command(), &context(), None)
            .expect_err("negative adjudication remains unknown");
        let terminal = error.publication_terminal().expect("explicit terminal");
        assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::CommitUnknown
        );
        assert!(terminal.do_not_retry());
        assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn finalization_failure_preserves_known_committed_terminal() {
        let engine = FakeTruncateEngine::new(Mode::CommittedFinalizationFailed, Mode::Committed);
        let error = DmlService::new()
            .execute_truncate(&engine, command(), &context(), None)
            .expect_err("finalization failure is visible");
        let terminal = error.publication_terminal().expect("explicit terminal");
        assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::KnownCommitted
        );
        assert!(terminal.do_not_retry());
        assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 0);
    }
}
