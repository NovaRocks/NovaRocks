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

//! Product-owner process-runtime integration checks.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{
    StatisticsAttemptError, StatisticsColumns, StatisticsFailure, StatisticsJob,
    StatisticsJobConclusion, StatisticsJobCreate, StatisticsJobPhase, StatisticsJobRepository,
    StatisticsJobState, StatisticsPublicationFact, StatisticsPublicationOutcome, StatisticsTarget,
    StatisticsWorker,
};
use novarocks_workload_control::{
    ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};

fn create(at_ms: i64) -> StatisticsJobCreate {
    StatisticsJobCreate {
        target: StatisticsTarget {
            catalog: Arc::from("iceberg"),
            namespace: Arc::from("db"),
            table: Arc::from("t"),
            object_id: Arc::from(&b"table-object"[..]),
        },
        columns: StatisticsColumns::All,
        submitted_at_ms: at_ms,
    }
}

async fn root() -> crate::StatisticsJobAdmission {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 16 * 1024 * 1024,
            control_bytes: 1024 * 1024,
            per_scope_bytes: 8 * 1024 * 1024,
        },
    )
    .unwrap();
    control
        .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .root_admission()
        .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
        .unwrap();
    let (permit, window) = root
        .owner
        .scope()
        .admit_query_with_result(novarocks_workload_control::ResultWindowClass::Internal)
        .unwrap()
        .await
        .unwrap();
    crate::StatisticsJobAdmission::try_new(root.owner, permit, window).unwrap()
}

struct PublishExecutor {
    publications: AtomicUsize,
    outcome: StatisticsPublicationFact,
    finalization_failure: bool,
    cleanup_failure: bool,
}
impl crate::StatisticsPhaseFixture for PublishExecutor {
    fn prepare(
        &self,
        _job: &StatisticsJob,
        context: &crate::StatisticsAttemptContext,
    ) -> Result<(), StatisticsAttemptError> {
        context.stage_scope().check().map_err(failed)
    }
    fn collect(
        &self,
        _job: &StatisticsJob,
        context: &crate::StatisticsAttemptContext,
    ) -> Result<(), StatisticsAttemptError> {
        context.stage_scope().check().map_err(failed)
    }
    fn publish(
        &self,
        _job: &StatisticsJob,
        context: &crate::StatisticsAttemptContext,
    ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError> {
        context.stage_scope().check().map_err(failed)?;
        self.publications.fetch_add(1, Ordering::SeqCst);
        let finalization_failure = self
            .finalization_failure
            .then(|| crate::test_publication_failure("finalization projection failed"));
        let cleanup_failure = self
            .cleanup_failure
            .then(|| crate::test_publication_failure("original cleanup failed"));
        Ok(match self.outcome {
            StatisticsPublicationFact::NoOp => StatisticsPublicationOutcome::NoOp {
                finalization_failure,
                cleanup_failure,
            },
            StatisticsPublicationFact::KnownCommitted => {
                StatisticsPublicationOutcome::KnownCommitted {
                    finalization_failure,
                    cleanup_failure,
                }
            }
            StatisticsPublicationFact::KnownUncommitted => {
                StatisticsPublicationOutcome::KnownUncommitted {
                    failure: crate::test_publication_failure("original publication refused"),
                    cleanup_failure,
                }
            }
            StatisticsPublicationFact::CommitUnknown => {
                StatisticsPublicationOutcome::CommitUnknown {
                    failure: crate::test_publication_failure("original publication unknown"),
                }
            }
            StatisticsPublicationFact::NotStarted => {
                panic!("phase fixture requires a provider verdict")
            }
        })
    }
}
fn failed(error: novarocks_workload_control::WorkError) -> StatisticsAttemptError {
    StatisticsAttemptError::Failed(StatisticsFailure {
        compile_control: None,
        message: Arc::from(error.to_string()),
    })
}

#[tokio::test]
async fn process_runtime_exposes_distinct_process_local_identities() {
    let repository = StatisticsJobRepository::new();
    let submitted = repository
        .create_admitted(create(1), root().await)
        .await
        .expect("create");
    let claimed = repository.claim_next(2).await.expect("claim").expect("job");
    assert_eq!(submitted.id.as_uuid().get_version_num(), 7);
    assert_eq!(submitted.publication_id.as_uuid().get_version_num(), 7);
    assert_ne!(
        submitted.id.as_uuid(),
        claimed.logical_execution_id.unwrap().as_uuid()
    );
    assert_ne!(
        claimed.logical_execution_id.unwrap().as_uuid(),
        claimed.query_attempt_id.unwrap().as_uuid()
    );
    assert_ne!(
        submitted.publication_id.as_uuid(),
        claimed.query_attempt_id.unwrap().as_uuid()
    );
    assert!(
        StatisticsJobRepository::new()
            .get(submitted.id)
            .await
            .expect("get")
            .is_none()
    );
}

#[tokio::test]
async fn commit_unknown_is_terminal_and_not_redispatched() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(PublishExecutor {
        publications: AtomicUsize::new(0),
        outcome: StatisticsPublicationFact::CommitUnknown,
        finalization_failure: false,
        cleanup_failure: false,
    });
    let worker = StatisticsWorker::new(repository.clone(), executor.clone());
    let job = repository
        .create_admitted(create(1), root().await)
        .await
        .expect("create");
    let terminal = worker.run_one(2).await.expect("run").expect("terminal");
    assert_eq!(
        terminal.state,
        StatisticsJobState::Terminal(StatisticsJobConclusion::CommitUnknown)
    );
    assert_eq!(
        terminal.publication,
        StatisticsPublicationFact::CommitUnknown
    );
    assert_eq!(executor.publications.load(Ordering::SeqCst), 1);
    assert!(worker.run_one(3).await.expect("idle").is_none());
    assert_eq!(executor.publications.load(Ordering::SeqCst), 1);
    assert_eq!(
        repository.get(job.id).await.expect("get").unwrap().state,
        terminal.state
    );
}

#[tokio::test]
async fn known_commit_finalization_failure_retains_the_provider_fact() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(PublishExecutor {
        publications: AtomicUsize::new(0),
        outcome: StatisticsPublicationFact::KnownCommitted,
        finalization_failure: true,
        cleanup_failure: false,
    });
    let worker = StatisticsWorker::new(repository.clone(), executor);
    repository
        .create_admitted(create(1), root().await)
        .await
        .expect("create");
    let terminal = worker.run_one(2).await.expect("run").expect("terminal");
    assert_eq!(
        terminal.state,
        StatisticsJobState::Terminal(StatisticsJobConclusion::Failed)
    );
    assert_eq!(
        terminal.publication,
        StatisticsPublicationFact::KnownCommitted
    );
    assert_eq!(
        terminal
            .publication_finalization_failure
            .unwrap()
            .local_diagnostic(),
        "finalization projection failed"
    );
    assert!(terminal.convergence.is_complete());
}

#[tokio::test]
async fn known_uncommitted_cleanup_failure_remains_a_failed_definite_publication() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(PublishExecutor {
        publications: AtomicUsize::new(0),
        outcome: StatisticsPublicationFact::KnownUncommitted,
        finalization_failure: false,
        cleanup_failure: true,
    });
    let worker = StatisticsWorker::new(repository.clone(), executor.clone());
    repository
        .create_admitted(create(1), root().await)
        .await
        .unwrap();
    let terminal = worker.run_one(2).await.unwrap().unwrap();
    assert_eq!(
        terminal.state,
        StatisticsJobState::Terminal(StatisticsJobConclusion::Failed)
    );
    assert_eq!(
        terminal.publication,
        StatisticsPublicationFact::KnownUncommitted
    );
    assert_eq!(
        terminal
            .publication_failure
            .as_ref()
            .unwrap()
            .local_diagnostic(),
        "original publication refused"
    );
    assert!(terminal.failure.is_none());
    assert_eq!(
        terminal.publication_failure.unwrap().fact(),
        crate::test_publication_failure("original publication refused").fact()
    );
    assert!(terminal.publication_finalization_failure.is_none());
    assert_eq!(
        terminal
            .publication_cleanup_failure
            .unwrap()
            .local_diagnostic(),
        "original cleanup failed"
    );
    assert_eq!(executor.publications.load(Ordering::SeqCst), 1);
    assert!(worker.run_one(3).await.unwrap().is_none());
}

#[tokio::test]
async fn submitted_is_a_phase_not_a_success_conclusion() {
    let repository = StatisticsJobRepository::new();
    let job = repository
        .create_admitted(create(1), root().await)
        .await
        .expect("create");
    assert_eq!(
        job.state,
        StatisticsJobState::Active(StatisticsJobPhase::Submitted)
    );
    assert!(!job.state.is_terminal());
}

#[cfg(novarocks_dev_controls)]
mod statement_observation_tests {
    use super::*;
    use novarocks_failpoint::target_binding::StatementWorkNomination;
    use std::sync::Mutex;

    fn nomination() -> Arc<StatementWorkNomination> {
        Arc::new(StatementWorkNomination {
            target_session_id: "statistics-owner-test".into(),
            statement_run_token: "original-analyze".into(),
            frontend_process_id: "019200aa-0000-7000-8000-0000000000ab".into(),
            connection_id: 1,
            session_key: 2,
            statement_generation: 3,
        })
    }

    struct NominatedExecutor {
        inner: PublishExecutor,
        seen: Mutex<Vec<(crate::StatisticsJobId, Option<StatementWorkNomination>)>>,
        fail_before_take: bool,
    }
    impl crate::StatisticsPhaseFixture for NominatedExecutor {
        fn prepare(
            &self,
            job: &StatisticsJob,
            context: &crate::StatisticsAttemptContext,
        ) -> Result<(), StatisticsAttemptError> {
            context.stage_scope().check().map_err(failed)?;
            if self.fail_before_take {
                return Err(StatisticsAttemptError::Failed(StatisticsFailure {
                    compile_control: None,
                    message: Arc::from("preparation refused before request construction"),
                }));
            }
            let nomination = context.take_test_statement_nomination().unwrap();
            self.seen
                .lock()
                .unwrap()
                .push((job.id, nomination.as_deref().cloned()));
            assert!(context.take_test_statement_nomination().unwrap().is_none());
            Ok(())
        }
        fn collect(
            &self,
            job: &StatisticsJob,
            context: &crate::StatisticsAttemptContext,
        ) -> Result<(), StatisticsAttemptError> {
            assert!(context.take_test_statement_nomination().unwrap().is_none());
            self.inner.collect(job, context)
        }
        fn publish(
            &self,
            job: &StatisticsJob,
            context: &crate::StatisticsAttemptContext,
        ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError> {
            assert!(context.take_test_statement_nomination().unwrap().is_none());
            self.inner.publish(job, context)
        }
    }

    #[tokio::test]
    async fn nomination_moves_once_to_exact_preparation_and_never_to_an_unrelated_job() {
        for fail_before_take in [false, true] {
            let repository = StatisticsJobRepository::new();
            let original = nomination();
            let weak = Arc::downgrade(&original);
            let expected = original.as_ref().clone();
            let nominated = repository
                .create_admitted(
                    create(1),
                    root()
                        .await
                        .with_test_statement_nomination(original)
                        .unwrap(),
                )
                .await
                .unwrap();
            let unrelated = repository
                .create_admitted(create(2), root().await)
                .await
                .unwrap();
            assert!(
                weak.upgrade().is_some(),
                "queued job must retain its own nomination"
            );
            let executor = Arc::new(NominatedExecutor {
                inner: PublishExecutor {
                    publications: AtomicUsize::new(0),
                    outcome: StatisticsPublicationFact::KnownCommitted,
                    finalization_failure: false,
                    cleanup_failure: false,
                },
                seen: Mutex::new(Vec::new()),
                fail_before_take,
            });
            let worker = StatisticsWorker::new(repository.clone(), executor.clone());
            let terminal = worker.run_one(3).await.unwrap().unwrap();
            assert_eq!(terminal.id, nominated.id);
            assert!(terminal.convergence.is_complete());
            assert!(
                weak.upgrade().is_none(),
                "terminal history must not retain preparation evidence"
            );
            let terminal = worker.run_one(4).await.unwrap().unwrap();
            assert_eq!(terminal.id, unrelated.id);
            let seen = executor.seen.lock().unwrap();
            if fail_before_take {
                assert!(seen.is_empty());
                assert_eq!(executor.inner.publications.load(Ordering::SeqCst), 0);
            } else {
                assert_eq!(
                    *seen,
                    [(nominated.id, Some(expected)), (unrelated.id, None)]
                );
                assert_eq!(executor.inner.publications.load(Ordering::SeqCst), 2);
            }
        }
    }

    #[tokio::test]
    async fn queued_cancel_shutdown_and_actual_convergence_release_unconsumed_nomination() {
        for path in 0..3 {
            let repository = StatisticsJobRepository::new();
            let original = nomination();
            let weak = Arc::downgrade(&original);
            let job = repository
                .create_admitted(
                    create(1),
                    root()
                        .await
                        .with_test_statement_nomination(original)
                        .unwrap(),
                )
                .await
                .unwrap();
            match path {
                0 => {
                    repository.request_cancel(job.id, 2).await.unwrap();
                }
                1 => {
                    repository.request_stop_for_process_exit(2).unwrap();
                }
                _ => {
                    repository.claim_next(2).await.unwrap().unwrap();
                    repository
                        .conclude(
                            job.id,
                            StatisticsJobPhase::Preparing,
                            StatisticsJobConclusion::Failed,
                            StatisticsPublicationFact::NotStarted,
                            None,
                            3,
                        )
                        .await
                        .unwrap();
                    assert!(
                        weak.upgrade().is_some(),
                        "business conclusion cannot invent runtime convergence"
                    );
                    repository
                        .record_convergence(
                            job.id,
                            crate::StatisticsConvergence {
                                collection_stopped: true,
                                execution_resources_released: true,
                                provider_session_closed: true,
                            },
                            4,
                        )
                        .await
                        .unwrap();
                }
            }
            assert!(
                repository
                    .get(job.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .convergence
                    .is_complete()
            );
            assert!(
                weak.upgrade().is_none(),
                "retained terminal row is observation only"
            );
        }
    }

    #[tokio::test]
    async fn admission_rejects_invalid_and_foreign_statement_evidence() {
        assert!(
            root()
                .await
                .with_test_statement_nomination(Arc::new(StatementWorkNomination::default()))
                .is_err()
        );
        let original = nomination();
        let admission = root()
            .await
            .with_test_statement_nomination(original.clone())
            .unwrap()
            .with_test_statement_nomination(original.clone())
            .unwrap();
        let mut foreign = original.as_ref().clone();
        foreign.statement_generation += 1;
        assert!(
            admission
                .with_test_statement_nomination(Arc::new(foreign))
                .is_err()
        );
    }
}

#[tokio::test]
async fn known_committed_cleanup_failure_does_not_change_business_success() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(PublishExecutor {
        publications: AtomicUsize::new(0),
        outcome: StatisticsPublicationFact::KnownCommitted,
        finalization_failure: false,
        cleanup_failure: true,
    });
    let worker = StatisticsWorker::new(repository.clone(), executor.clone());
    repository
        .create_admitted(create(1), root().await)
        .await
        .unwrap();
    let terminal = worker.run_one(2).await.unwrap().unwrap();
    assert_eq!(
        terminal.state,
        StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
    );
    assert_eq!(
        terminal.publication,
        StatisticsPublicationFact::KnownCommitted
    );
    assert!(terminal.failure.is_none());
    assert!(terminal.publication_failure.is_none());
    assert!(terminal.publication_finalization_failure.is_none());
    assert_eq!(
        terminal
            .publication_cleanup_failure
            .unwrap()
            .local_diagnostic(),
        "original cleanup failed"
    );
    assert_eq!(executor.publications.load(Ordering::SeqCst), 1);
    assert!(worker.run_one(3).await.unwrap().is_none());
}
