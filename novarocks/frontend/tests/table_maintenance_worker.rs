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

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks::engine::table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceRequestContext,
    MaintenanceTarget, OptimizeJobState, OptimizeSubmission, TableMaintenanceEngine,
    TableMaintenanceService,
};
use novarocks_frontend::table_maintenance::FrontendTableMaintenanceService;
use novarocks_frontend::table_maintenance::model::{OptimizeJob, OptimizeJobCreate};
use novarocks_frontend::table_maintenance::repository::OptimizeJobRepository;
use novarocks_frontend::table_maintenance::worker::OptimizeWorker;
use novarocks_spi::state_store::{
    CommitOutcome, FeDeploymentView, Key, Precondition, StateStore, TransactionId, Value,
};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

fn rewrite_outcome() -> MaintenanceActionOutcome {
    MaintenanceActionOutcome::RewriteDataFiles {
        target_snapshot_id: Some(900),
        rewritten_data_files_count: 4,
        added_data_files_count: 2,
        rewritten_bytes_count: 8192,
        failed_data_files_count: 0,
        removed_delete_files_count: 3,
        output_record_count: 88,
    }
}

struct ExecutionGate {
    blocked_call: usize,
    released: Mutex<bool>,
    release: Condvar,
}

impl ExecutionGate {
    fn new(blocked_call: usize) -> Self {
        Self {
            blocked_call,
            released: Mutex::new(false),
            release: Condvar::new(),
        }
    }

    fn wait_if_blocked(&self, call: usize) {
        if call != self.blocked_call {
            return;
        }
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.release.wait(released).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}

struct FakeMaintenanceEngine {
    requests: Mutex<Vec<MaintenanceActionRequest>>,
    results: Mutex<VecDeque<Result<MaintenanceActionOutcome, String>>>,
    gate: Option<Arc<ExecutionGate>>,
    dropped: Option<Arc<AtomicBool>>,
}

impl FakeMaintenanceEngine {
    fn succeeding() -> Self {
        Self::with_results(Vec::new())
    }

    fn with_results(results: Vec<Result<MaintenanceActionOutcome, String>>) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            results: Mutex::new(results.into()),
            gate: None,
            dropped: None,
        }
    }

    fn gated(gate: Arc<ExecutionGate>) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            results: Mutex::new(VecDeque::new()),
            gate: Some(gate),
            dropped: None,
        }
    }

    fn with_drop_flag(dropped: Arc<AtomicBool>) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            results: Mutex::new(VecDeque::new()),
            gate: None,
            dropped: Some(dropped),
        }
    }

    fn requests(&self) -> Vec<MaintenanceActionRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for FakeMaintenanceEngine {
    fn drop(&mut self) {
        if let Some(dropped) = &self.dropped {
            dropped.store(true, Ordering::SeqCst);
        }
    }
}

impl TableMaintenanceEngine for FakeMaintenanceEngine {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, String> {
        match name_parts {
            [table] => Ok(target(
                context.current_catalog.unwrap_or("default_catalog"),
                context.current_database,
                table,
            )),
            [namespace, table] => Ok(target(
                context.current_catalog.unwrap_or("default_catalog"),
                namespace,
                table,
            )),
            [catalog, namespace, table] => Ok(target(catalog, namespace, table)),
            _ => Err(format!(
                "unsupported table name with {} parts",
                name_parts.len()
            )),
        }
    }

    fn reject_user_action_on_mv(&self, _target: &MaintenanceTarget) -> Result<(), String> {
        Ok(())
    }

    fn current_snapshot_id(&self, _target: &MaintenanceTarget) -> Result<i64, String> {
        Ok(777)
    }

    fn execute_action(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        let call = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request);
            requests.len()
        };
        if let Some(gate) = &self.gate {
            gate.wait_if_blocked(call);
        }
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Ok(rewrite_outcome()))
    }
}

fn target(catalog: &str, namespace: &str, table: &str) -> MaintenanceTarget {
    MaintenanceTarget {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
    }
}

fn sqlite_config(path: &Path) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: "table-maintenance-worker-test".to_string(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Sqlite {
            path: path.to_path_buf(),
            deployment_owner: "table-maintenance-worker-fe".to_string(),
        },
    }
}

async fn open_sqlite(path: &Path) -> Arc<dyn StateStore> {
    let registry = builtin_state_store_provider_registry().expect("built-in provider registry");
    StateStoreHost::open(
        &registry,
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: sqlite_config(path),
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"table-maintenance-worker-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open SQLite state store")
    .state_store()
    .expect("SQLite state store exposure")
}

async fn fixture() -> (
    TempDir,
    Arc<dyn StateStore>,
    Arc<OptimizeJobRepository>,
    Arc<FrontendTableMaintenanceService>,
) {
    let temp = TempDir::new().expect("create temp directory");
    let store = open_sqlite(&temp.path().join("state.sqlite")).await;
    let repository = Arc::new(
        OptimizeJobRepository::open(Arc::clone(&store))
            .await
            .expect("open optimize job repository"),
    );
    let service = Arc::new(
        FrontendTableMaintenanceService::open(
            Some(Arc::clone(&store)),
            tokio::runtime::Handle::current(),
        )
        .await
        .expect("open table-maintenance service"),
    );
    (temp, store, repository, service)
}

async fn create_job(
    repository: &OptimizeJobRepository,
    table: &str,
    base_snapshot_id: i64,
    created_at_ms: i64,
) -> OptimizeJob {
    repository
        .create(OptimizeJobCreate {
            target: target("ice", "db", table),
            base_snapshot_id,
            created_at_ms,
        })
        .await
        .expect("create optimize job")
}

fn start_service(service: &FrontendTableMaintenanceService, engine: &Arc<FakeMaintenanceEngine>) {
    let engine: Arc<dyn TableMaintenanceEngine> = engine.clone();
    service.start(engine).expect("start maintenance worker");
}

async fn wait_for_request_count(engine: &FakeMaintenanceEngine, expected: usize) {
    timeout(Duration::from_secs(5), async {
        loop {
            if engine.requests().len() >= expected {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker did not execute expected requests");
}

async fn wait_for_terminal_jobs(
    repository: &OptimizeJobRepository,
    expected: usize,
) -> Vec<OptimizeJob> {
    timeout(Duration::from_secs(5), async {
        loop {
            let jobs = repository.list().await.expect("list optimize jobs");
            if jobs.len() == expected
                && jobs.iter().all(|job| {
                    matches!(
                        job.state,
                        OptimizeJobState::Finished | OptimizeJobState::Failed
                    )
                })
            {
                return jobs;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("worker did not terminalize expected jobs")
}

#[tokio::test(flavor = "multi_thread")]
async fn start_reconciles_running_before_processing_pending_in_job_id_order() {
    let (_temp, _store, repository, service) = fixture().await;
    let running = create_job(&repository, "running", 10, 100).await;
    repository
        .claim(running.job_id, 150)
        .await
        .expect("claim running job")
        .expect("pending job was claimable");
    let first_pending = create_job(&repository, "first", 20, 200).await;
    let second_pending = create_job(&repository, "second", 30, 300).await;
    let engine = Arc::new(FakeMaintenanceEngine::succeeding());

    start_service(&service, &engine);

    let jobs = wait_for_terminal_jobs(&repository, 3).await;
    assert_eq!(jobs[0].job_id, running.job_id);
    assert_eq!(jobs[0].state, OptimizeJobState::Failed);
    assert!(
        jobs[0]
            .error_message
            .as_deref()
            .unwrap()
            .contains("frontend restart reconciliation")
    );
    assert_eq!(jobs[1].state, OptimizeJobState::Finished);
    assert_eq!(jobs[2].state, OptimizeJobState::Finished);
    assert_eq!(
        jobs[1].outcome.as_ref().unwrap(),
        &novarocks_frontend::table_maintenance::model::OptimizeJobOutcome {
            target_snapshot_id: Some(900),
            rewritten_data_files: 4,
            deleted_data_files: 3,
            added_data_files: 2,
            output_record_count: 88,
        }
    );
    assert_eq!(
        engine.requests(),
        vec![
            MaintenanceActionRequest::RewriteDataFiles {
                target: first_pending.target,
                base_snapshot_id: 20,
                job_id: Some(first_pending.job_id),
                options: BTreeMap::new(),
                branch: None,
                where_clause: None,
            },
            MaintenanceActionRequest::RewriteDataFiles {
                target: second_pending.target,
                base_snapshot_id: 30,
                job_id: Some(second_pending.job_id),
                options: BTreeMap::new(),
                branch: None,
                where_clause: None,
            },
        ]
    );
    service.shutdown().expect("shutdown maintenance worker");
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_claims_before_execute_and_terminalizes_before_claiming_the_next_job() {
    let (_temp, _store, repository, service) = fixture().await;
    let first = create_job(&repository, "first", 41, 100).await;
    let second = create_job(&repository, "second", 42, 200).await;
    let gate = Arc::new(ExecutionGate::new(1));
    let engine = Arc::new(FakeMaintenanceEngine::gated(Arc::clone(&gate)));

    start_service(&service, &engine);
    wait_for_request_count(&engine, 1).await;

    let jobs = repository.list().await.expect("list jobs during execution");
    assert_eq!(jobs[0].job_id, first.job_id);
    assert_eq!(jobs[0].state, OptimizeJobState::Running);
    assert_eq!(jobs[1].job_id, second.job_id);
    assert_eq!(jobs[1].state, OptimizeJobState::Pending);
    assert_eq!(
        engine.requests()[0],
        MaintenanceActionRequest::RewriteDataFiles {
            target: first.target,
            base_snapshot_id: 41,
            job_id: Some(first.job_id),
            options: BTreeMap::new(),
            branch: None,
            where_clause: None,
        }
    );

    gate.release();
    let jobs = wait_for_terminal_jobs(&repository, 2).await;
    assert_eq!(jobs[0].state, OptimizeJobState::Finished);
    assert_eq!(jobs[1].state, OptimizeJobState::Finished);
    service.shutdown().expect("shutdown maintenance worker");
}

#[tokio::test(flavor = "multi_thread")]
async fn execution_failure_fails_the_job_preserves_message_and_keeps_worker_running() {
    let (_temp, _store, repository, service) = fixture().await;
    let failed = create_job(&repository, "failed", 51, 100).await;
    let succeeded = create_job(&repository, "succeeded", 52, 200).await;
    let engine = Arc::new(FakeMaintenanceEngine::with_results(vec![
        Err("rewrite engine exploded".to_string()),
        Ok(rewrite_outcome()),
    ]));

    start_service(&service, &engine);

    let jobs = wait_for_terminal_jobs(&repository, 2).await;
    assert_eq!(jobs[0].job_id, failed.job_id);
    assert_eq!(jobs[0].state, OptimizeJobState::Failed);
    assert_eq!(
        jobs[0].error_message.as_deref(),
        Some("rewrite engine exploded")
    );
    assert_eq!(jobs[1].job_id, succeeded.job_id);
    assert_eq!(jobs[1].state, OptimizeJobState::Finished);
    service
        .shutdown()
        .expect("persisted job failure is not a worker failure");
}

#[tokio::test(flavor = "multi_thread")]
async fn service_start_accepts_an_engine_exactly_once() {
    let (_temp, _store, _repository, service) = fixture().await;
    let engine = Arc::new(FakeMaintenanceEngine::succeeding());
    let first: Arc<dyn TableMaintenanceEngine> = engine.clone();
    let second: Arc<dyn TableMaintenanceEngine> = engine.clone();

    service.start(first).expect("first start succeeds");
    assert_eq!(
        service.start(second).unwrap_err(),
        "table maintenance service is already started"
    );
    service.shutdown().expect("shutdown maintenance worker");

    let after_shutdown: Arc<dyn TableMaintenanceEngine> = engine;
    assert_eq!(
        service.start(after_shutdown).unwrap_err(),
        "table maintenance service cannot be restarted after shutdown"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_wakes_and_joins_worker_and_prevents_later_claims() {
    let (_temp, _store, repository, service) = fixture().await;
    let engine = Arc::new(FakeMaintenanceEngine::succeeding());
    start_service(&service, &engine);
    sleep(Duration::from_millis(100)).await;

    let shutdown_service = Arc::clone(&service);
    timeout(
        Duration::from_millis(300),
        tokio::task::spawn_blocking(move || shutdown_service.shutdown()),
    )
    .await
    .expect("shutdown should wake before the 500ms poll interval")
    .expect("join shutdown task")
    .expect("shutdown maintenance worker");

    let pending = create_job(&repository, "after-shutdown", 61, 100).await;
    sleep(Duration::from_millis(600)).await;
    let jobs = repository.list().await.expect("list jobs after shutdown");
    assert_eq!(jobs[0].job_id, pending.job_id);
    assert_eq!(jobs[0].state, OptimizeJobState::Pending);
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_engine_weak_reference_ends_worker_without_a_reference_cycle() {
    let (_temp, _store, _repository, service) = fixture().await;
    let dropped = Arc::new(AtomicBool::new(false));
    let engine = Arc::new(FakeMaintenanceEngine::with_drop_flag(Arc::clone(&dropped)));
    let engine_port: Arc<dyn TableMaintenanceEngine> = engine.clone();
    service.start(engine_port.clone()).expect("start worker");

    drop(engine_port);
    drop(engine);
    timeout(Duration::from_secs(2), async {
        while !dropped.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("service or worker retained a strong engine reference");

    service
        .shutdown()
        .expect("join worker after engine expiration");
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_worker_exits_autonomously_before_shutdown_when_engine_expires() {
    let (_temp, _store, repository, _service) = fixture().await;
    create_job(&repository, "idle-exit", 71, 100).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let engine = Arc::new(FakeMaintenanceEngine::with_drop_flag(Arc::clone(&dropped)));
    let engine_port: Arc<dyn TableMaintenanceEngine> = engine.clone();
    let engine_weak = Arc::downgrade(&engine_port);
    let mut worker = OptimizeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::clone(&repository),
        engine_weak,
    )
    .expect("start worker");

    let jobs = wait_for_terminal_jobs(&repository, 1).await;
    assert_eq!(jobs[0].state, OptimizeJobState::Finished);
    drop(engine_port);
    drop(engine);
    timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&repository) != 1 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("idle worker retained its repository after engine expiration");
    assert!(dropped.load(Ordering::SeqCst));

    worker.shutdown().expect("join already-finished worker");
}

#[tokio::test(flavor = "multi_thread")]
async fn successful_submit_wakes_an_idle_worker() {
    let (_temp, _store, repository, service) = fixture().await;
    let engine = Arc::new(FakeMaintenanceEngine::succeeding());
    start_service(&service, &engine);
    sleep(Duration::from_millis(100)).await;

    assert_eq!(
        service
            .submit_automatic_optimize(engine.as_ref(), target("ice", "db", "submitted"))
            .expect("submit optimize"),
        OptimizeSubmission::Submitted { job_id: 1 }
    );
    timeout(
        Duration::from_millis(300),
        wait_for_request_count(&engine, 1),
    )
    .await
    .expect("submit should wake before the 500ms poll interval");
    let jobs = wait_for_terminal_jobs(&repository, 1).await;
    assert_eq!(jobs[0].state, OptimizeJobState::Finished);
    service.shutdown().expect("shutdown maintenance worker");
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_shutdown_is_idempotent() {
    let (_temp, _store, _repository, service) = fixture().await;
    let engine = Arc::new(FakeMaintenanceEngine::succeeding());
    start_service(&service, &engine);

    service.shutdown().expect("first shutdown");
    service.shutdown().expect("second shutdown");
}

async fn write_raw(store: &dyn StateStore, key: Key, value: Value) {
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "write worker corruption test record",
        )
        .await
        .expect("begin raw write");
    transaction
        .put(key, value, Precondition::Absent)
        .await
        .expect("put raw record");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn repository_corruption_is_reported_by_shutdown() {
    let (_temp, store, _repository, service) = fixture().await;
    let pending_key = Key::try_from(Bytes::from_static(
        b"novarocks/frontend/table-maintenance/v1/state/pending/0000000000000001",
    ))
    .unwrap();
    let corrupt_value = Value::try_from(Bytes::from_static(b"bad")).unwrap();
    write_raw(store.as_ref(), pending_key, corrupt_value).await;
    let engine = Arc::new(FakeMaintenanceEngine::succeeding());

    start_service(&service, &engine);
    sleep(Duration::from_millis(100)).await;

    let error = service.shutdown().unwrap_err();
    assert!(error.contains("list pending optimize jobs"), "{error}");
    assert!(error.contains("non-canonical id"), "{error}");
}
