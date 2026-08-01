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

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_frontend::statistics_jobs::model::{
    StatisticsJobCreate, StatisticsJobError, StatisticsJobErrorKind, StatisticsJobState,
    StatisticsJobTablePin, StatisticsJobTarget,
};
use novarocks_frontend::statistics_jobs::repository::{
    FenceValidator, StatisticsJobRepository, StatisticsJobRepositoryErrorKind,
};
use novarocks_frontend::statistics_jobs::service::{
    AnalyzeTableStatement, CancelAnalyzeStatement, ShowAnalyzeJobsStatement,
    ShowTableStatsStatement, StatisticsApplicationErrorKind, StatisticsApplicationService,
    StatisticsJobTargetResolver, StatisticsStatement, StatisticsStatementResult,
    StatisticsTableStatRow, TableStatisticsReader,
};
use novarocks_frontend::statistics_jobs::worker::{
    StatisticsAnalyzeWorker, StatisticsAttemptError, StatisticsAttemptExecutor,
    StatisticsCollectedAttempt,
};
use novarocks_spi::connector::{
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorMutationOperationId, ConnectorProviderId, ExternalMutationEvidence,
};
use novarocks_spi::state_store::{
    Direction, FeDeploymentView, Key, KeyRange, RangeRequest, StateStore,
};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use tempfile::TempDir;

const PREFIX: &str = "novarocks/frontend/statistics/v2/";

fn publication_evidence(
    job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
) -> ExternalMutationEvidence {
    ExternalMutationEvidence::try_new(
        1,
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("statistics-test").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("statistics-test").expect("instance ID"),
        },
        ConnectorInstanceIncarnation::default(),
        ConnectorMutationOperationId::from_bytes(*job.operation_id.as_bytes()),
        "statistics-publish",
        Bytes::from_static(b"operation-evidence"),
    )
    .expect("test evidence")
}

fn sqlite_config(path: &Path) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: "statistics-repository-test".to_string(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Sqlite {
            path: path.to_path_buf(),
            deployment_owner: "statistics-repository-fe".to_string(),
        },
    }
}

async fn fixture() -> (TempDir, Arc<dyn StateStore>, StatisticsJobRepository) {
    let temp = TempDir::new().expect("create temp directory");
    let registry = builtin_state_store_provider_registry().expect("built-in provider registry");
    let store = StateStoreHost::open(
        &registry,
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: sqlite_config(&temp.path().join("state.sqlite")),
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"statistics-repository-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open SQLite state store")
    .state_store()
    .expect("SQLite state store exposure");
    let repository = StatisticsJobRepository::open(Arc::clone(&store))
        .await
        .expect("open statistics repository");
    (temp, store, repository)
}

fn request(table: &str, submitted_at_ms: i64) -> StatisticsJobCreate {
    StatisticsJobCreate {
        target: StatisticsJobTarget {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: table.to_string(),
        },
        table_pin: StatisticsJobTablePin {
            connector_instance_id: "ice".to_string(),
            table_handle: format!("table:{table}").into_bytes(),
            data_version: b"snapshot:1".to_vec(),
            columns: vec!["v".to_string()],
        },
        metric_names: vec!["row_count".to_string(), "ndv".to_string()],
        submitted_at_ms,
    }
}

fn always_valid_fence() -> FenceValidator {
    Arc::new(|_| Box::pin(async { Ok(()) }))
}

async fn stored_payloads(store: &dyn StateStore) -> Vec<String> {
    let prefix = Key::try_from(Bytes::from(PREFIX)).expect("valid statistics prefix");
    let range = KeyRange::for_prefix(prefix).expect("valid range");
    let mut transaction = store.begin_read().await.expect("begin raw read");
    let mut request = RangeRequest {
        range,
        direction: Direction::Forward,
        page_size: store.limits().max_page_size,
        continuation: None,
    };
    let mut payloads = Vec::new();
    loop {
        let page = transaction.range(&request).await.expect("read raw page");
        for record in page.records {
            payloads.push(String::from_utf8_lossy(record.value.as_bytes()).into_owned());
        }
        let Some(continuation) = page.continuation else {
            break;
        };
        request.continuation = Some(continuation);
    }
    transaction.abort().await.expect("finish raw read");
    payloads
}

#[tokio::test]
async fn records_are_versioned_durable_and_identical_analyze_requests_remain_distinct() {
    let (_temp, store, repository) = fixture().await;
    let first = repository
        .create(request("orders", 10))
        .await
        .expect("create first job");
    let second = repository
        .create(request("orders", 11))
        .await
        .expect("create second job");

    assert_ne!(first.job_id, second.job_id);
    assert_ne!(first.operation_id, second.operation_id);
    assert_eq!(first.table_pin.connector_instance_id, "ice");
    assert_eq!(first.table_pin.table_handle, b"table:orders");
    assert_eq!(first.table_pin.data_version, b"snapshot:1");
    assert_eq!(first.job_id.get_version_num(), 7);
    assert_eq!(first.operation_id.get_version_num(), 7);
    assert_eq!(
        repository
            .list_by_state(StatisticsJobState::Submitted)
            .await
            .unwrap()
            .len(),
        2
    );

    let payloads = stored_payloads(store.as_ref()).await;
    assert!(
        payloads
            .iter()
            .any(|payload| payload.contains("\"schema_version\":2"))
    );
    for forbidden in ["artifact", "sketch", "runtime_handle", "record_batch"] {
        assert!(payloads.iter().all(|payload| !payload.contains(forbidden)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_creates_retry_sqlite_snapshot_conflicts_with_stable_identities() {
    let (_temp, _store, repository) = fixture().await;
    let first_repository = repository.clone();
    let second_repository = repository.clone();
    let (first, second) = tokio::join!(
        first_repository.create(request("concurrent_orders", 10)),
        second_repository.create(request("concurrent_orders", 11)),
    );
    let first = first.expect("first concurrent create");
    let second = second.expect("second concurrent create");

    assert_ne!(first.job_id, second.job_id);
    assert_ne!(first.operation_id, second.operation_id);
    assert_eq!(
        repository
            .list_by_state(StatisticsJobState::Submitted)
            .await
            .expect("list submitted jobs")
            .len(),
        2
    );
}

struct SucceedingStatisticsExecutor {
    collected: AtomicUsize,
    published: AtomicUsize,
}

struct TransientlyFailingStatisticsExecutor {
    attempts: AtomicUsize,
}

impl StatisticsAttemptExecutor for TransientlyFailingStatisticsExecutor {
    fn collect(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        Err(StatisticsAttemptError::transient(
            StatisticsJobErrorKind::Collection,
            "temporary collector outage",
        ))
    }

    fn prepare_publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsAttemptError> {
        panic!("collection failure must not enter publish")
    }

    fn publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        panic!("collection failure must not enter publish")
    }

    fn reconcile(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        panic!("collection failure must not enter publish")
    }
}

impl StatisticsAttemptExecutor for SucceedingStatisticsExecutor {
    fn collect(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError> {
        self.collected.fetch_add(1, Ordering::AcqRel);
        Ok(Box::new(()))
    }

    fn prepare_publish(
        &self,
        job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsAttemptError> {
        Ok(publication_evidence(job))
    }

    fn publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        self.published.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn reconcile(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        self.published.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_claims_collects_and_publishes_under_the_fenced_lease() {
    let (_temp, _store, repository) = fixture().await;
    let job = repository
        .create(request("worker_orders", 10))
        .await
        .expect("create job");
    let concrete_executor = Arc::new(SucceedingStatisticsExecutor {
        collected: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        Arc::clone(&executor),
    )
    .await
    .expect("start worker");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = repository
                .get(job.job_id)
                .await
                .expect("read job")
                .expect("durable job");
            if current.state == StatisticsJobState::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must finish job");
    worker.shutdown().expect("shutdown worker");

    assert_eq!(concrete_executor.collected.load(Ordering::Acquire), 1);
    assert_eq!(concrete_executor.published.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_reconciles_publishing_without_recollecting() {
    let (_temp, _store, repository) = fixture().await;
    let fence = always_valid_fence();
    let created = repository
        .create(request("publishing_orders", 10))
        .await
        .expect("create job");
    repository
        .claim(created.job_id, 11, &fence)
        .await
        .expect("claim job");
    repository
        .transition(
            created.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Running,
            12,
            None,
            &fence,
        )
        .await
        .expect("run job");
    repository
        .begin_publishing(
            created.job_id,
            13,
            publication_evidence(&created)
                .try_to_wire_v1()
                .expect("encode test evidence"),
            &fence,
        )
        .await
        .expect("begin publish");
    let publishing_before_recovery = repository
        .get(created.job_id)
        .await
        .expect("read publishing job")
        .expect("durable publishing job");
    let stored_evidence = ExternalMutationEvidence::try_from_wire_v1(
        publishing_before_recovery
            .publication_evidence
            .as_deref()
            .expect("publishing job evidence"),
    )
    .expect("decode publishing job evidence");
    assert_eq!(
        stored_evidence.operation_id().to_bytes(),
        *created.operation_id.as_bytes()
    );

    let concrete_executor = Arc::new(SucceedingStatisticsExecutor {
        collected: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        Arc::clone(&executor),
    )
    .await
    .expect("start worker");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if repository
                .get(created.job_id)
                .await
                .expect("read job")
                .expect("durable job")
                .state
                == StatisticsJobState::Succeeded
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must reconcile publishing job");
    worker.shutdown().expect("shutdown worker");
    assert_eq!(concrete_executor.collected.load(Ordering::Acquire), 0);
    assert_eq!(concrete_executor.published.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_retries_transient_collection_at_most_three_times_with_one_operation() {
    let (_temp, _store, repository) = fixture().await;
    let created = repository
        .create(request("retry_orders", 10))
        .await
        .expect("create job");
    let concrete_executor = Arc::new(TransientlyFailingStatisticsExecutor {
        attempts: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        Arc::clone(&executor),
    )
    .await
    .expect("start worker");

    let failed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = repository
                .get(created.job_id)
                .await
                .expect("read job")
                .expect("durable job");
            if current.state == StatisticsJobState::Failed {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must exhaust retries");
    worker.shutdown().expect("shutdown worker");
    assert_eq!(failed.operation_id, created.operation_id);
    assert_eq!(failed.attempt, 3);
    assert_eq!(concrete_executor.attempts.load(Ordering::Acquire), 3);
}

#[tokio::test]
async fn claim_transitions_with_fence_and_cancel_observes_publish_boundary() {
    let (_temp, _store, repository) = fixture().await;
    let fence = always_valid_fence();
    let created = repository
        .create(request("orders", 10))
        .await
        .expect("create job");
    let preparing = repository
        .claim(created.job_id, 11, &fence)
        .await
        .expect("claim job")
        .expect("submitted job claimed");
    assert_eq!(preparing.state, StatisticsJobState::Preparing);
    assert_eq!(preparing.attempt, 1);
    let running = repository
        .transition(
            created.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Running,
            12,
            None,
            &fence,
        )
        .await
        .expect("start job");
    assert_eq!(running.state, StatisticsJobState::Running);
    let publishing = repository
        .begin_publishing(
            created.job_id,
            13,
            publication_evidence(&created)
                .try_to_wire_v1()
                .expect("encode test evidence"),
            &fence,
        )
        .await
        .expect("publish job");
    assert_eq!(publishing.state, StatisticsJobState::Publishing);
    let conflict = repository
        .cancel(created.job_id, 14, &fence)
        .await
        .unwrap_err();
    assert_eq!(conflict.kind(), StatisticsJobRepositoryErrorKind::Conflict);
    let succeeded = repository
        .transition(
            created.job_id,
            StatisticsJobState::Publishing,
            StatisticsJobState::Succeeded,
            15,
            None,
            &fence,
        )
        .await
        .expect("complete job");
    assert_eq!(succeeded.completed_at_ms, Some(15));

    let failed = repository
        .create(request("lineitem", 20))
        .await
        .expect("create failed job");
    let _ = repository
        .claim(failed.job_id, 21, &fence)
        .await
        .expect("claim failed job");
    let failed = repository
        .transition(
            failed.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Failed,
            22,
            Some(StatisticsJobError {
                kind: StatisticsJobErrorKind::Collection,
                message: "connector timed out".to_string(),
            }),
            &fence,
        )
        .await
        .expect("fail job");
    assert_eq!(failed.state, StatisticsJobState::Failed);
    assert_eq!(
        failed.error.unwrap().kind,
        StatisticsJobErrorKind::Collection
    );
}

struct StaticTableStatistics;

struct StaticStatisticsTargetResolver;

impl StatisticsJobTargetResolver for StaticStatisticsTargetResolver {
    fn resolve_table_pin(
        &self,
        target: &StatisticsJobTarget,
    ) -> Result<StatisticsJobTablePin, String> {
        Ok(StatisticsJobTablePin {
            connector_instance_id: target.catalog.clone(),
            table_handle: format!("table:{}:{}", target.namespace, target.table).into_bytes(),
            data_version: b"snapshot:1".to_vec(),
            columns: vec!["v".to_string()],
        })
    }
}

impl TableStatisticsReader for StaticTableStatistics {
    fn show_table_stats(
        &self,
        target: &StatisticsJobTarget,
    ) -> Result<Vec<StatisticsTableStatRow>, String> {
        Ok(vec![StatisticsTableStatRow {
            metric_name: format!(
                "{}.{}.{}.row_count",
                target.catalog, target.namespace, target.table
            ),
            value: Some("1".to_string()),
            status: "AVAILABLE".to_string(),
        }])
    }
}

#[tokio::test]
async fn typed_application_never_reparses_sql_and_keeps_reads_available_without_state_store() {
    let table_statistics = StaticTableStatistics;
    let target = request("orders", 10).target;
    let unavailable = StatisticsApplicationService::unavailable();
    let read = unavailable
        .execute(
            StatisticsStatement::ShowTableStats(ShowTableStatsStatement {
                target: target.clone(),
            }),
            10,
            &table_statistics,
        )
        .await
        .expect("read-only table statistics remain available without StateStore");
    assert!(matches!(read, StatisticsStatementResult::TableStats(_)));
    let error = unavailable
        .execute(
            StatisticsStatement::AnalyzeTable(AnalyzeTableStatement {
                target: target.clone(),
                metric_names: Vec::new(),
            }),
            10,
            &table_statistics,
        )
        .await
        .expect_err("ANALYZE requires a durable StateStore");
    assert_eq!(
        error.kind(),
        StatisticsApplicationErrorKind::StateStoreRequired
    );

    let (_temp, _store, repository) = fixture().await;
    let service = StatisticsApplicationService::with_repository_and_target_resolver(
        repository,
        Arc::new(StaticStatisticsTargetResolver),
    );
    let submitted = service
        .execute(
            StatisticsStatement::AnalyzeTable(AnalyzeTableStatement {
                target: target.clone(),
                metric_names: Vec::new(),
            }),
            11,
            &table_statistics,
        )
        .await
        .expect("typed ANALYZE creates a job");
    assert!(matches!(
        submitted,
        StatisticsStatementResult::JobSubmitted(ref job)
            if job.metric_names == vec!["v".to_string()]
    ));
    let listed = service
        .execute(
            StatisticsStatement::ShowAnalyzeJobs(ShowAnalyzeJobsStatement {
                target: Some(target),
            }),
            12,
            &table_statistics,
        )
        .await
        .expect("typed SHOW ANALYZE JOBS reads durable jobs");
    assert!(matches!(listed, StatisticsStatementResult::AnalyzeJobs(jobs) if jobs.len() == 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_cancel_records_intent_and_the_fenced_worker_transitions_it() {
    let (_temp, _store, repository) = fixture().await;
    let service = StatisticsApplicationService::with_repository(repository.clone());
    let table_statistics = StaticTableStatistics;
    let created = repository
        .create(request("cancelled_orders", 10))
        .await
        .expect("create durable job");

    let requested = service
        .execute(
            StatisticsStatement::CancelAnalyze(CancelAnalyzeStatement {
                job_id: created.job_id,
            }),
            11,
            &table_statistics,
        )
        .await
        .expect("record cancellation request");
    assert!(matches!(
        requested,
        StatisticsStatementResult::JobCancellationRequested(job)
            if job.cancel_requested && job.state == StatisticsJobState::Submitted
    ));

    let concrete_executor = Arc::new(SucceedingStatisticsExecutor {
        collected: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        Arc::clone(&executor),
    )
    .await
    .expect("start worker");
    let cancelled = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = repository
                .get(created.job_id)
                .await
                .expect("read job")
                .expect("durable job");
            if current.state == StatisticsJobState::Cancelled {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must consume cancellation intent");
    worker.shutdown().expect("shutdown worker");
    assert!(!cancelled.cancel_requested);
    assert_eq!(concrete_executor.collected.load(Ordering::Acquire), 0);
    assert_eq!(concrete_executor.published.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn failover_requeues_preparing_and_running_before_publish() {
    let (_temp, _store, repository) = fixture().await;
    let fence = always_valid_fence();
    let job = repository
        .create(request("orders", 10))
        .await
        .expect("create job");
    let preparing = repository
        .claim(job.job_id, 11, &fence)
        .await
        .expect("claim job")
        .expect("job claimed");
    assert_eq!(preparing.attempt, 1);
    let requeued = repository
        .requeue_incomplete(job.job_id, 12, &fence)
        .await
        .expect("requeue PREPARING job")
        .expect("PREPARING is replayable");
    assert_eq!(requeued.state, StatisticsJobState::Submitted);
    let preparing = repository
        .claim(job.job_id, 13, &fence)
        .await
        .expect("claim replayed job")
        .expect("job reclaimed");
    assert_eq!(preparing.attempt, 2);
    let running = repository
        .transition(
            job.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Running,
            14,
            None,
            &fence,
        )
        .await
        .expect("begin collection");
    assert_eq!(running.state, StatisticsJobState::Running);
    let requeued = repository
        .requeue_incomplete(job.job_id, 15, &fence)
        .await
        .expect("requeue RUNNING job")
        .expect("RUNNING is replayable");
    assert_eq!(requeued.state, StatisticsJobState::Submitted);
}
