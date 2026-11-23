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

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::*;
use crate::query_execution::dml::truncate::{
    PreparedTruncate, TruncateEffect, TruncateEvidence, TruncateFailureKind, TruncatePlanSummary,
    TruncatePrepared, TruncateReceipt,
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
    AuthoredUnknown,
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
    authored_unknown: novarocks_failure_contract::SharedFailureCause,
    plan_calls: AtomicUsize,
    execute_calls: AtomicUsize,
    adjudicate_calls: AtomicUsize,
}

impl FakeTruncateEngine {
    fn new(execute: Mode, adjudicate: Mode) -> Self {
        Self {
            execute,
            adjudicate,
            authored_unknown: authored_unknown(),
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
            Mode::CommitUnknown | Mode::AuthoredUnknown => TruncateOutcome::CommitUnknown {
                failure: if matches!(mode, Mode::AuthoredUnknown) {
                    TruncateFailure {
                        kind: TruncateFailureKind::AuthoredCommit(self.authored_unknown.clone()),
                        message: "catalog response lost".into(),
                    }
                } else {
                    failure("catalog response lost")
                },
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
                target_ref: match request.command.target {
                    TruncateTarget::Main => "main".into(),
                    TruncateTarget::WholeBranch(name) => name,
                },
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
        novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        Some(
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    parametric_interval: false,
                    variant: true,
                    variant_binary: true,
                },
            ),
        ),
    ))
}

fn command() -> TruncateCommand {
    TruncateCommand {
        target_parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()],
        target: TruncateTarget::Main,
    }
}

fn execute_test_truncate(engine: &FakeTruncateEngine) -> Result<(), DmlError> {
    let service = DmlService::new();
    let prepared = service.prepare_truncate(engine, command(), &context(), None)?;
    service.execute_prepared_truncate(engine, prepared)
}

#[test]
fn direct_truncate_uses_data_mutation_marker_family() {
    assert_eq!(
        LakePublicationFamily::DataMutation.as_str(),
        "data_mutation"
    );
}

#[test]
fn planned_truncate_does_not_dispatch_until_the_second_edge() {
    let engine = FakeTruncateEngine::new(Mode::Committed, Mode::Committed);
    let prepared = DmlService::new()
        .prepare_truncate(&engine, command(), &context(), None)
        .expect("plan truncate without dispatch");

    assert_eq!(engine.plan_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 0);

    DmlService::new()
        .execute_prepared_truncate(&engine, prepared)
        .expect("explicit dispatch succeeds");
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn unknown_is_adjudicated_once_and_exact_positive_commits_without_cleanup() {
    let engine = FakeTruncateEngine::new(Mode::CommitUnknown, Mode::Committed);
    execute_test_truncate(&engine).expect("exact positive adjudication commits");
    assert_eq!(engine.plan_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn unknown_or_negative_adjudication_never_retries_or_mutates() {
    let engine = FakeTruncateEngine::new(Mode::CommitUnknown, Mode::KnownUncommitted);
    let error = execute_test_truncate(&engine).expect_err("negative adjudication remains unknown");
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
    let error = execute_test_truncate(&engine).expect_err("finalization failure is visible");
    let terminal = error.publication_terminal().expect("explicit terminal");
    assert_eq!(
        terminal.disposition(),
        LakePublicationDisposition::KnownCommitted
    );
    assert!(terminal.do_not_retry());
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn known_uncommitted_truncate_is_definite_and_full_branch_delete_stays_conservative() {
    let cases = [
        (
            TruncateTarget::Main,
            LakePublicationDisposition::KnownUncommitted,
        ),
        (
            TruncateTarget::WholeBranch("main".into()),
            LakePublicationDisposition::CommitUnknown,
        ),
    ];
    let service = DmlService::new();
    for (target, disposition) in cases {
        let engine = FakeTruncateEngine::new(Mode::KnownUncommitted, Mode::Committed);
        let prepared = service
            .prepare_truncate(
                &engine,
                TruncateCommand {
                    target,
                    ..command()
                },
                &context(),
                None,
            )
            .expect("prepare");
        let error = service
            .execute_prepared_truncate(&engine, prepared)
            .unwrap_err();
        assert_eq!(
            error.publication_terminal().unwrap().disposition(),
            disposition
        );
        assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
        assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 0);
    }
}

fn authored_unknown() -> novarocks_failure_contract::SharedFailureCause {
    use novarocks_failure_contract::{
        BoundedDetail, FailureClass, FailureFact, FailureProjection, SharedFailureCause,
        connector_reason::ICEBERG_COMMIT_STATE_UNKNOWN,
    };
    #[derive(Debug)]
    struct LostCommit;
    impl std::fmt::Display for LostCommit {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("catalog response lost")
        }
    }
    impl std::error::Error for LostCommit {}
    impl FailureProjection for LostCommit {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(FailureClass::Dependency, ICEBERG_COMMIT_STATE_UNKNOWN)
        }
    }
    SharedFailureCause::from_prefixed_public_detail(
        LostCommit,
        "Failed to commit during truncate: ",
        "[CommitUnknown] ",
        BoundedDetail::truncating("catalog response lost"),
    )
}

#[test]
fn truncate_adjudication_keeps_the_original_typed_unknown_and_does_not_redispatch() {
    let engine = FakeTruncateEngine::new(Mode::AuthoredUnknown, Mode::KnownUncommitted);
    let error = execute_test_truncate(&engine).unwrap_err();
    let original = error.application_error().unwrap().failure_cause().unwrap();
    assert_eq!(original, &engine.authored_unknown);
    assert_eq!(
        original.prefixed_public_detail(),
        Some("Failed to commit during truncate: [CommitUnknown] catalog response lost")
    );
    let terminal = error.publication_terminal().unwrap();
    assert_eq!(
        terminal.disposition(),
        LakePublicationDisposition::CommitUnknown
    );
    assert!(terminal.do_not_retry());
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 1);
}
