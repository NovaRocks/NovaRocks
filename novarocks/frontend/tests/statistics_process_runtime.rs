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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use novarocks_frontend::FrontendServingLifecycle;
use novarocks_frontend::statistics_jobs::application::StatisticsColumnIntent;
use novarocks_frontend::statistics_jobs::model::{
    StatisticsJob, StatisticsJobCreate, StatisticsJobErrorKind, StatisticsJobState,
    StatisticsJobTarget,
};
use novarocks_frontend::statistics_jobs::repository::{
    MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS, MAX_RECENT_TERMINAL_STATISTICS_JOBS,
    StatisticsJobRepository, StatisticsJobRepositoryErrorKind,
};
use novarocks_frontend::statistics_jobs::worker::{
    StatisticsAnalyzeWorker, StatisticsAttemptError, StatisticsAttemptExecutor,
};

fn create(at_ms: i64) -> StatisticsJobCreate {
    StatisticsJobCreate {
        target: StatisticsJobTarget {
            catalog: "iceberg".into(),
            namespace: "db".into(),
            table: "t".into(),
        },
        connector_instance_id: "iceberg".into(),
        object_id: b"table-object".to_vec(),
        columns: StatisticsColumnIntent::AllColumns,
        submitted_at_ms: at_ms,
    }
}

struct CommitUnknownExecutor {
    publishes: AtomicUsize,
}

struct CancelAwareExecutor {
    started: AtomicBool,
    finishes: AtomicUsize,
}

struct SuccessfulExecutor {
    finishes: AtomicUsize,
}

impl StatisticsAttemptExecutor for CancelAwareExecutor {
    fn execute(
        &self,
        _job: &StatisticsJob,
        cancellation: novarocks_frontend::common::query_cancellation::QueryCancellationView,
    ) -> Result<(), StatisticsAttemptError> {
        self.started.store(true, Ordering::Release);
        while !cancellation.is_cancelled() {
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(StatisticsAttemptError::permanent(
            StatisticsJobErrorKind::Cancelled,
            "cancelled before provider finish",
        ))
    }
}

impl StatisticsAttemptExecutor for CommitUnknownExecutor {
    fn execute(
        &self,
        _job: &StatisticsJob,
        _cancellation: novarocks_frontend::common::query_cancellation::QueryCancellationView,
    ) -> Result<(), StatisticsAttemptError> {
        self.publishes.fetch_add(1, Ordering::SeqCst);
        Err(StatisticsAttemptError::publication(
            novarocks_frontend::statistics_jobs::application::StatisticsPublicationTerminal::CommitUnknown,
            "connector outcome is unknown",
        ))
    }
}

impl StatisticsAttemptExecutor for SuccessfulExecutor {
    fn execute(
        &self,
        _job: &StatisticsJob,
        _cancellation: novarocks_frontend::common::query_cancellation::QueryCancellationView,
    ) -> Result<(), StatisticsAttemptError> {
        self.finishes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn wait_terminal(repository: &StatisticsJobRepository, job_id: uuid::Uuid) -> StatisticsJob {
    for _ in 0..200 {
        let job = repository.get(job_id).await.unwrap().unwrap();
        if job.state.is_terminal() {
            return job;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("statistics job did not reach a terminal state")
}

#[tokio::test]
async fn process_runtime_uses_v7_identities_and_never_recovers_another_incarnation() {
    let first = StatisticsJobRepository::new();
    let job = first.create(create(1)).await.unwrap();
    assert_eq!(job.job_id.get_version_num(), 7);
    assert_eq!(job.operation_id.as_uuid().get_version_num(), 7);
    assert!(first.get(job.job_id).await.unwrap().is_some());

    let restarted = StatisticsJobRepository::new();
    assert!(restarted.get(job.job_id).await.unwrap().is_none());
    assert!(restarted.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn active_and_queued_jobs_are_bounded_without_evicting_live_work() {
    let repository = StatisticsJobRepository::new();
    for index in 0..MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS {
        repository.create(create(index as i64 + 1)).await.unwrap();
    }
    let error = repository.create(create(10_000)).await.unwrap_err();
    assert_eq!(error.kind(), StatisticsJobRepositoryErrorKind::Capacity);
    assert_eq!(
        repository.list().await.unwrap().len(),
        MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS
    );
}

#[tokio::test]
async fn terminal_history_is_count_bounded_while_active_jobs_remain_visible() {
    let repository = StatisticsJobRepository::new();
    let base = now_ms();
    for index in 0..=MAX_RECENT_TERMINAL_STATISTICS_JOBS {
        let job = repository
            .create(create(base + index as i64))
            .await
            .unwrap();
        let claimed = repository
            .claim_next(base + index as i64)
            .await
            .unwrap()
            .unwrap();
        repository
            .transition(
                claimed.job_id,
                StatisticsJobState::Preparing,
                StatisticsJobState::Running,
                base + index as i64,
                None,
            )
            .await
            .unwrap();
        repository
            .transition(
                claimed.job_id,
                StatisticsJobState::Running,
                StatisticsJobState::Failed,
                base + index as i64,
                None,
            )
            .await
            .unwrap();
        assert_eq!(job.job_id, claimed.job_id);
    }
    let jobs = repository.list().await.unwrap();
    assert_eq!(jobs.len(), MAX_RECENT_TERMINAL_STATISTICS_JOBS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_unknown_is_terminal_and_never_dispatches_another_mutation() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(CommitUnknownExecutor {
        publishes: AtomicUsize::new(0),
    });
    let lifecycle = FrontendServingLifecycle::new();
    lifecycle.mark_ready().expect("mark frontend ready");
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        repository.clone(),
        executor.clone(),
        lifecycle,
    )
    .await
    .unwrap();
    let job = repository.create(create(now_ms())).await.unwrap();
    let terminal = wait_terminal(&repository, job.job_id).await;
    assert_eq!(terminal.state, StatisticsJobState::CommitUnknown);
    assert_eq!(
        terminal.error.unwrap().kind,
        StatisticsJobErrorKind::CommitUnknown
    );
    assert_eq!(executor.publishes.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(executor.publishes.load(Ordering::SeqCst), 1);
    worker
        .shutdown_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_finish_succeeds_once_and_late_cancel_cannot_rewrite_the_terminal() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(SuccessfulExecutor {
        finishes: AtomicUsize::new(0),
    });
    let lifecycle = FrontendServingLifecycle::new();
    lifecycle.mark_ready().expect("mark frontend ready");
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        repository.clone(),
        executor.clone(),
        lifecycle,
    )
    .await
    .unwrap();
    let job = repository.create(create(now_ms())).await.unwrap();
    let terminal = wait_terminal(&repository, job.job_id).await;
    assert_eq!(terminal.state, StatisticsJobState::Succeeded);
    assert_eq!(executor.finishes.load(Ordering::SeqCst), 1);

    let error = repository
        .request_cancel(job.job_id, now_ms())
        .await
        .expect_err("a terminal job is no longer a cancellable active attempt");
    assert_eq!(error.kind(), StatisticsJobRepositoryErrorKind::NotFound);
    assert_eq!(
        repository.get(job.job_id).await.unwrap().unwrap().state,
        StatisticsJobState::Succeeded
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(executor.finishes.load(Ordering::SeqCst), 1);
    worker
        .shutdown_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_cancel_reaches_the_attempt_and_blocks_provider_finish() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(CancelAwareExecutor {
        started: AtomicBool::new(false),
        finishes: AtomicUsize::new(0),
    });
    let lifecycle = FrontendServingLifecycle::new();
    lifecycle.mark_ready().expect("mark frontend ready");
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        repository.clone(),
        executor.clone(),
        lifecycle,
    )
    .await
    .unwrap();
    let job = repository.create(create(now_ms())).await.unwrap();
    for _ in 0..200 {
        if executor.started.load(Ordering::Acquire) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(executor.started.load(Ordering::Acquire));
    repository
        .request_cancel(job.job_id, now_ms())
        .await
        .unwrap();
    let terminal = wait_terminal(&repository, job.job_id).await;
    assert_eq!(terminal.state, StatisticsJobState::Cancelled);
    assert_eq!(executor.finishes.load(Ordering::SeqCst), 0);
    worker
        .shutdown_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn explicit_cancel_of_a_queued_job_is_terminal_and_process_local() {
    let repository = StatisticsJobRepository::new();
    let job = repository.create(create(now_ms())).await.unwrap();
    let cancelled = repository
        .request_cancel(job.job_id, now_ms())
        .await
        .unwrap();
    assert_eq!(cancelled.state, StatisticsJobState::Cancelled);
    assert!(cancelled.cancel_requested);
    assert_eq!(
        cancelled.error.unwrap().kind,
        StatisticsJobErrorKind::Cancelled
    );
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
