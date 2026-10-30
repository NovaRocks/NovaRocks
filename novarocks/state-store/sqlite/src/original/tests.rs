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

use super::*;
use novarocks_query_application::cpu::{
    QueryBlockingExecutorConfig, QueryBlockingExecutorOwner, QueryBlockingOriginal,
    QueryBlockingOriginalExecutor, QueryBlockingPreparedJob, QueryBlockingReceipt,
    QueryBlockingReservation,
};
use novarocks_state_store_api::{
    AttemptOutcome, CommitOutcome, Key, Precondition, StateStore, StateStoreBlockingOrigin,
    StateStoreExecutionPort, StateStoreLimits, StateStoreOpenRequest, StateStoreOriginalOpen,
    StateStoreProviderFactory, StateStoreProviderInstance, Value,
};
use std::{
    future::Future,
    num::NonZeroUsize,
    pin::pin,
    task::Waker,
    time::{Duration, Instant},
};

// This is a test-only receiving fixture, not the production Root/Host port.
// Each phase uses the real existing QB reservation, prepared call and actual
// exit receipt. No test thread, alternate worker, exit timer or grant is made.
struct Call {
    original: Arc<dyn Original>,
    request: Request,
    panic_after_run: std::sync::atomic::AtomicBool,
    ran_on: Mutex<Option<std::thread::ThreadId>>,
}
impl QueryBlockingOriginal for Call {
    fn run_original(&self) {
        *lock(&self.ran_on) = Some(std::thread::current().id());
        self.original.run_original_phase(self.request);
        assert!(
            !self
                .panic_after_run
                .swap(false, std::sync::atomic::Ordering::AcqRel),
            "injected physical panic after the same original phase"
        );
    }
}
#[derive(Default)]
struct Observation {
    result: Option<(Request, Result<(), Failure>)>,
    exit: Option<(Request, Exit)>,
}
struct Cell {
    original: Arc<dyn Original>,
    observation: Arc<Mutex<Observation>>,
    // Stable outer receiving: result and actual exit precede this handoff.
    health: Mutex<Option<Failure>>,
}
struct Borrower {
    observation: Arc<Mutex<Observation>>,
}
impl Slot for Borrower {
    fn poll_phase(&mut self, _: &mut Context<'_>, request: Request) -> Poll<Result<(), Failure>> {
        match &lock(&self.observation).result {
            Some((r, result)) if *r == request => Poll::Ready(result.clone()),
            _ => Poll::Pending,
        }
    }
    fn poll_exit(&mut self, _: &mut Context<'_>, request: Request) -> Poll<Result<Exit, Failure>> {
        match lock(&self.observation).exit {
            Some((r, exit)) if r == request => Poll::Ready(Ok(exit)),
            _ => Poll::Pending,
        }
    }
}
struct Receiving {
    cells: Mutex<Vec<Arc<Cell>>>,
    origin: StateStoreBlockingOrigin,
    progress_notifications: std::sync::atomic::AtomicUsize,
}
impl StateStoreExecutionPort for Receiving {
    fn bind_original_in(
        &self,
        original: &Arc<dyn Original>,
        _: Instant,
        target: &mut Option<Box<dyn Slot>>,
    ) -> Result<(), Failure> {
        assert!(target.is_none());
        assert_eq!(original.origin(), self.origin);
        let vocabulary = vocabulary();
        let actual = original.as_ref().type_id();
        assert!(
            actual == vocabulary.open_type()
                || actual == vocabulary.transaction_type()
                || actual == vocabulary.evidence_type()
        );
        let observation = Arc::new(Mutex::new(Observation::default()));
        lock(&self.cells).push(Arc::new(Cell {
            original: original.clone(),
            observation: observation.clone(),
            health: Mutex::new(None),
        }));
        *target = Some(Box::new(Borrower { observation }));
        Ok(())
    }
    fn request_progress(&self) {
        self.progress_notifications
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    } // Tests explicitly poll the stable receiving below.
}
struct Prepared {
    cell: Arc<Cell>,
    request: Request,
    call: Arc<Call>,
    reservation: Option<QueryBlockingReservation>,
    prepared: Option<QueryBlockingPreparedJob>,
    receipt: Option<QueryBlockingReceipt<()>>,
}
impl Prepared {
    fn record(&self, result: Result<(), Failure>, exit: Exit) {
        lock(&self.cell.observation).result = Some((self.request, result.clone()));
        self.cell.original.receive_result(self.request, result);
        lock(&self.cell.observation).exit = Some((self.request, exit));
        self.cell.original.receive_exit(self.request, Ok(exit));
    }
    fn withdraw(mut self) {
        // The same unpublished physical input/reservation really return here.
        drop(self.prepared.take());
        drop(self.reservation.take());
        self.record(
            Err(error(
                StateStoreErrorKind::Cancelled,
                "test original was withdrawn before publication",
            )),
            Exit::NeverPublishedReturned,
        );
        assert!(matches!(
            self.receipt
                .as_ref()
                .unwrap()
                .poll_actual_exit(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }
    async fn publish(mut self, executor: &QueryBlockingOriginalExecutor<Call>) {
        executor
            .publish_in(&mut self.reservation, &mut self.prepared)
            .expect("same original publication")
            .notify();
        let result = std::future::poll_fn(|cx| self.receipt.as_mut().unwrap().poll_result(cx))
            .await
            .map_err(failure);
        lock(&self.cell.observation).result = Some((self.request, result.clone()));
        self.cell.original.receive_result(self.request, result);
        // A result alone never enables the next source phase.
        assert_eq!(self.cell.original.requested(), Some(self.request));
        std::future::poll_fn(|cx| self.receipt.as_ref().unwrap().poll_actual_exit(cx))
            .await
            .expect("actual original exit");
        lock(&self.cell.observation).exit = Some((self.request, Exit::ActualExited));
        self.cell
            .original
            .receive_exit(self.request, Ok(Exit::ActualExited));
        // A successful business result/complete fact cannot bypass the raw
        // health receiving immediately after this same actual exit.
        if let Some(failure) = self.cell.original.local_failure() {
            let mut health = lock(&self.cell.health);
            if health.is_none() {
                *health = Some(failure);
            }
        }
        assert_eq!(self.call.request, self.request);
    }
}
struct Rig {
    owner: QueryBlockingExecutorOwner,
    executor: QueryBlockingOriginalExecutor<Call>,
    receiving: Arc<Receiving>,
}
impl Rig {
    fn new(origin: StateStoreBlockingOrigin) -> Self {
        let owner = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .expect("same ordinary QB");
        let executor = owner
            .executor()
            .original_executor::<Call>()
            .expect("closed test purpose");
        Self {
            owner,
            executor,
            receiving: Arc::new(Receiving {
                cells: Mutex::new(Vec::new()),
                origin,
                progress_notifications: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }
    fn context(&self, deadline: Instant) -> Execution {
        let port: Arc<dyn StateStoreExecutionPort> = self.receiving.clone();
        Execution::new(&port, deadline)
    }
    async fn prepare_next(&self) -> Prepared {
        let (cell, request) = lock(&self.receiving.cells)
            .iter()
            .find_map(|cell| {
                cell.original
                    .requested()
                    .map(|request| (cell.clone(), request))
            })
            .expect("one original request is pending");
        self.prepare_selected(cell, request).await
    }
    async fn prepare_selected(&self, cell: Arc<Cell>, request: Request) -> Prepared {
        let reservation = self.executor.reserve().await.expect("real queue slot");
        let call = Arc::new(Call {
            original: cell.original.clone(),
            request,
            panic_after_run: std::sync::atomic::AtomicBool::new(false),
            ran_on: Mutex::new(None),
        });
        let mut receipt = None;
        let prepared = self
            .executor
            .prepare_in(&reservation, &call, &mut receipt)
            .expect("stable receiver before mark");
        cell.original
            .before_publish(request)
            .expect("same source prepublication");
        Prepared {
            cell,
            request,
            call,
            reservation: Some(reservation),
            prepared: Some(prepared),
            receipt,
        }
    }
    async fn run<F: Future>(&self, future: F) -> F::Output {
        let mut future = pin!(future);
        loop {
            if let Poll::Ready(result) = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                return result;
            }
            self.prepare_next().await.publish(&self.executor).await;
        }
    }
    async fn drain(&self) {
        while lock(&self.receiving.cells)
            .iter()
            .any(|cell| !cell.original.is_complete())
        {
            self.prepare_next().await.publish(&self.executor).await;
        }
    }
    async fn stop_workers_with_retained_health(mut self) {
        let retained = lock(&self.receiving.cells).clone();
        assert!(!retained.is_empty());
        for cell in &retained {
            assert!(!cell.original.is_complete());
            assert!(cell.original.local_failure().is_some());
            assert!(lock(&cell.health).is_some());
            // No positive Source close is invented: only this original QB
            // footer is proven, and the failed Source remains in receiving.
            assert!(matches!(
                lock(&cell.observation).exit,
                Some((_, Exit::ActualExited))
            ));
        }
        drop(self.executor);
        self.owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .expect("same physical workers exit with failed sources still retained");
        assert_eq!(lock(&self.receiving.cells).len(), retained.len());
        // Test teardown releases the stable failed receiving after worker join;
        // this is not a positive Source-complete or production cleanup receipt.
        lock(&self.receiving.cells).clear();
        drop(retained);
    }
    async fn stop(mut self) {
        assert!(
            lock(&self.receiving.cells)
                .iter()
                .all(|cell| cell.original.is_complete())
        );
        lock(&self.receiving.cells).clear();
        drop(self.executor);
        self.owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .expect("same workers exit");
    }
}
fn request() -> StateStoreOpenRequest {
    StateStoreOpenRequest {
        cluster_id: "original-custody".into(),
        limits: StateStoreLimits::default(),
        deadline: Instant::now() + Duration::from_secs(5),
    }
}
async fn store(path: std::path::PathBuf) -> crate::SqliteStateStore {
    // Explicit legacy setup only. Every tested transaction below uses *_in.
    crate::SqliteStateStore::open(path, request())
        .await
        .expect("fixture store")
}
fn key() -> Key {
    Key::try_from(bytes::Bytes::from_static(b"same-original")).unwrap()
}
fn value() -> Value {
    Value::try_from(bytes::Bytes::from_static(b"applied-once")).unwrap()
}

#[tokio::test]
async fn undelivered_open_remains_in_original_until_same_worker_close_exits() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("original.sqlite");
    let rig = Rig::new(StateStoreBlockingOrigin::Opening(
        crate::SQLITE_STATE_STORE_PROVIDER_ID,
    ));
    let request = request();
    let context = rig.context(request.deadline);
    let factory = Box::new(crate::SqliteStateStoreProviderFactory::new(path.clone()));
    assert_eq!(factory.blocking_source_vocabulary(), Some(vocabulary()));
    let mut opening: Option<Box<dyn StateStoreOriginalOpen>> = None;
    factory
        .prepare_open_in(request, &context, &mut opening)
        .unwrap();
    rig.prepare_next().await.publish(&rig.executor).await;
    assert!(matches!(
        opening
            .as_mut()
            .unwrap()
            .poll_ready(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    assert!(crate::acquire_owner_lock(&path).is_err());
    drop(opening); // No outer F or opening borrower remains to own the store.
    assert!(!lock(&rig.receiving.cells)[0].original.is_complete());
    assert!(crate::acquire_owner_lock(&path).is_err());
    rig.drain().await;
    assert!(crate::acquire_owner_lock(&path).is_ok());
    rig.stop().await;
}

#[tokio::test]
async fn original_open_moves_instance_only_after_real_exit_into_original_target() {
    let temp = tempfile::tempdir().unwrap();
    let rig = Rig::new(StateStoreBlockingOrigin::Opening(
        crate::SQLITE_STATE_STORE_PROVIDER_ID,
    ));
    let request = request();
    let mut opening = None;
    Box::new(crate::SqliteStateStoreProviderFactory::new(
        temp.path().join("store.sqlite"),
    ))
    .prepare_open_in(
        request.clone(),
        &rig.context(request.deadline),
        &mut opening,
    )
    .unwrap();
    let mut instance: Option<Box<dyn StateStoreProviderInstance>> = None;
    assert!(
        opening
            .as_mut()
            .unwrap()
            .take_instance_in(&mut instance)
            .is_err()
    );
    assert!(instance.is_none());
    rig.prepare_next().await.publish(&rig.executor).await;
    opening
        .as_mut()
        .unwrap()
        .take_instance_in(&mut instance)
        .unwrap();
    assert_eq!(
        instance.as_ref().unwrap().blocking_source_vocabulary(),
        Some(vocabulary())
    );
    assert!(
        instance
            .as_ref()
            .unwrap()
            .blocking_instance_scope()
            .is_some()
    );
    drop(opening);
    instance
        .as_mut()
        .unwrap()
        .shutdown(request.deadline)
        .await
        .unwrap();
    rig.stop().await;
}

#[tokio::test]
async fn withdrawn_begin_retains_its_original_refusal_after_actual_close() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path().join("withdrawn-begin.sqlite")).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(Instant::now() + Duration::from_secs(5));
    let mut begin = Box::pin(store.begin_original_read_in(&execution));
    assert!(
        begin
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let prepared = rig.prepare_next().await;
    let cell = prepared.cell.clone();
    assert_eq!(prepared.request.phase, Phase::BeginRead);
    prepared.withdraw();
    let original = lock(&cell.observation)
        .result
        .as_ref()
        .unwrap()
        .1
        .as_ref()
        .unwrap_err()
        .clone();
    assert_eq!(cell.original.requested().unwrap().phase, Phase::Close);
    assert!(!cell.original.is_complete());
    assert!(cell.original.local_failure().is_none());
    // Delay the borrower until the same real QB Close has entirely returned.
    rig.drain().await;
    assert!(cell.original.is_complete());
    assert!(matches!(
        lock(&cell.observation).exit,
        Some((_, Exit::ActualExited))
    ));
    let selected = match begin.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(failure)) => failure,
        _ => panic!("actual Close cannot erase the original unpublished Begin refusal"),
    };
    assert_eq!(selected, original);
    assert_eq!(selected.failure_fact(), original.failure_fact());
    assert!(cell.original.local_failure().is_none());
    drop(begin);
    drop(cell);
    rig.stop().await;
}

#[tokio::test]
async fn withdrawn_get_retains_its_original_refusal_after_actual_close() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path().join("withdrawn-get.sqlite")).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(Instant::now() + Duration::from_secs(5));
    let mut transaction = rig
        .run(store.begin_original_read_in(&execution))
        .await
        .unwrap();
    let item = key();
    let mut get = Box::pin(transaction.get_in(&item));
    assert!(
        get.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let prepared = rig.prepare_next().await;
    let cell = prepared.cell.clone();
    assert_eq!(prepared.request.phase, Phase::Get);
    prepared.withdraw();
    let original = lock(&cell.observation)
        .result
        .as_ref()
        .unwrap()
        .1
        .as_ref()
        .unwrap_err()
        .clone();
    assert_eq!(cell.original.requested().unwrap().phase, Phase::Close);
    assert!(!cell.original.is_complete());
    assert!(cell.original.local_failure().is_none());
    rig.drain().await;
    assert!(cell.original.is_complete());
    let selected = match get.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(failure)) => failure,
        _ => panic!("actual Close cannot erase the original unpublished Get refusal"),
    };
    assert_eq!(selected, original);
    assert_eq!(selected.failure_fact(), original.failure_fact());
    assert!(cell.original.local_failure().is_none());
    drop(get);
    drop(transaction);
    drop(cell);
    rig.stop().await;
}

#[tokio::test]
async fn marked_never_published_commit_settles_only_after_same_rollback_actual_exit() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path().join("store.sqlite")).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(request().deadline);
    let (attempt, observation) = store.attempts.reserve().unwrap();
    let mut transaction = rig
        .run(store.begin_original_write_in(attempt, "same original", &execution))
        .await
        .unwrap();
    rig.run(transaction.put_in(key(), value(), Precondition::Any))
        .await
        .unwrap();
    let mut commit = Box::pin(transaction.commit_in());
    assert!(
        commit
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let prepared = rig.prepare_next().await;
    assert_eq!(prepared.request.phase, Phase::Commit);
    assert_eq!(
        observation.outcome().await.unwrap(),
        AttemptOutcome::Unresolved
    );
    prepared.withdraw();
    // A normal unpublished return is not itself a connection-close proof.
    assert_eq!(
        observation.outcome().await.unwrap(),
        AttemptOutcome::Unresolved
    );
    let close = rig.prepare_next().await;
    assert_eq!(close.request.phase, Phase::Close);
    close.publish(&rig.executor).await;
    assert_eq!(
        observation.outcome().await.unwrap(),
        AttemptOutcome::NotCommitted
    );
    assert!(matches!(
        rig.run(commit).await.unwrap(),
        CommitOutcome::DefiniteFailure(_)
    ));
    assert!(
        transaction
            .original_failure()
            .unwrap()
            .downcast_ref::<StateStoreError>()
            .is_some()
    );
    drop(transaction);
    rig.drain().await;
    let mut read = rig
        .run(store.begin_original_read_in(&execution))
        .await
        .unwrap();
    assert!(rig.run(read.get_in(&key())).await.unwrap().is_none());
    drop(read);
    rig.drain().await;
    rig.stop().await;
}

#[tokio::test]
async fn dropped_borrower_keeps_original_connection_and_file_until_rollback_exit() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("store.sqlite");
    let store = store(path.clone()).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(request().deadline);
    let mut read = rig
        .run(store.begin_original_read_in(&execution))
        .await
        .unwrap();
    rig.run(read.get_in(&key())).await.unwrap();
    drop(read);
    drop(store);
    assert!(crate::acquire_owner_lock(&path).is_err());
    assert!(!lock(&rig.receiving.cells)[0].original.is_complete());
    rig.drain().await;
    assert!(crate::acquire_owner_lock(&path).is_ok());
    rig.stop().await;
}

#[tokio::test]
async fn original_commit_retains_same_physical_panic_while_actual_evidence_keeps_applied() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path().join("store.sqlite")).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(request().deadline);
    let (attempt, observation) = store.attempts.reserve().unwrap();
    let mut transaction = rig
        .run(store.begin_original_write_in(attempt, "panic after apply", &execution))
        .await
        .unwrap();
    rig.run(transaction.put_in(key(), value(), Precondition::Any))
        .await
        .unwrap();
    store.test_hooks.inject_commit_after_apply_panic();
    let outcome = rig.run(transaction.commit_in()).await.unwrap();
    let CommitOutcome::Committed(receipt) = outcome else {
        panic!("same operation must retain its committed receipt");
    };
    assert_eq!(
        observation.outcome().await.unwrap(),
        AttemptOutcome::Committed(receipt)
    );
    let first = transaction
        .original_failure()
        .expect("original physical panic");
    assert!(
        first
            .downcast_ref::<novarocks_query_application::cpu::QueryBlockingFailure>()
            .is_some()
    );
    assert_eq!(first, transaction.original_failure().unwrap());
    let cell = lock(&rig.receiving.cells)[0].clone();
    // Close has returned and the committed outcome is known, but the same
    // original EvidenceRelease still must run before deferred health is visible.
    assert!(!cell.original.is_complete());
    assert!(cell.original.local_failure().is_none());
    assert!(lock(&cell.health).is_none());
    assert_eq!(
        cell.original.requested().unwrap().phase,
        Phase::EvidenceRelease
    );
    drop(transaction);
    rig.drain().await;
    assert!(cell.original.is_complete());
    assert_eq!(cell.original.local_failure().unwrap(), first);
    assert_eq!(lock(&cell.health).as_ref().unwrap(), &first);
    assert_eq!(
        first.failure_fact().class(),
        novarocks_failure_contract::FailureClass::Unexpected
    );
    drop(cell);
    let mut read = rig
        .run(store.begin_original_read_in(&execution))
        .await
        .unwrap();
    assert_eq!(
        rig.run(read.get_in(&key())).await.unwrap().unwrap().value,
        value()
    );
    drop(read);
    rig.drain().await;
    rig.stop().await;
}

#[tokio::test]
async fn withdrawn_cancel_cannot_hide_later_close_or_evidence_physical_panic() {
    for phase in [Phase::Close, Phase::EvidenceRelease] {
        let temp = tempfile::tempdir().unwrap();
        let store = store(temp.path().join("store.sqlite")).await;
        let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
        let execution = rig.context(request().deadline);
        let (attempt, _) = store.attempts.reserve().unwrap();
        let mut transaction = rig
            .run(store.begin_original_write_in(
                attempt,
                "withdraw then settlement panic",
                &execution,
            ))
            .await
            .unwrap();
        rig.run(transaction.put_in(key(), value(), Precondition::Any))
            .await
            .unwrap();
        let mut commit = Box::pin(transaction.commit_in());
        assert!(
            commit
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let withdrawn = rig.prepare_next().await;
        assert_eq!(withdrawn.request.phase, Phase::Commit);
        withdrawn.withdraw();
        drop(commit);
        let ordinary = transaction.original_failure().unwrap();
        assert_eq!(
            ordinary.downcast_ref::<StateStoreError>().unwrap().kind(),
            StateStoreErrorKind::Cancelled
        );
        let cell = lock(&rig.receiving.cells)[0].clone();
        assert!(cell.original.local_failure().is_none());
        if phase == Phase::EvidenceRelease {
            let close = rig.prepare_next().await;
            assert_eq!(close.request.phase, Phase::Close);
            close.publish(&rig.executor).await;
        }
        let failed = rig.prepare_next().await;
        assert_eq!(failed.request.phase, phase);
        failed
            .call
            .panic_after_run
            .store(true, std::sync::atomic::Ordering::Release);
        failed.publish(&rig.executor).await;
        let health = cell
            .original
            .local_failure()
            .expect("settlement raw health is mandatory");
        assert!(
            health
                .downcast_ref::<novarocks_query_application::cpu::QueryBlockingFailure>()
                .is_some()
        );
        assert_eq!(
            health.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Unexpected
        );
        assert_ne!(health, ordinary);
        assert_eq!(transaction.original_failure().unwrap(), ordinary);
        assert_eq!(lock(&cell.health).as_ref().unwrap(), &health);
        assert!(!cell.original.is_complete());
        assert!(cell.original.requested().is_none());
        cell.original.request_close();
        assert!(
            cell.original.requested().is_none(),
            "health cannot auto-retry settlement"
        );
        assert_eq!(cell.original.local_failure().unwrap(), health);
        let failure = rig.run(transaction.close_in()).await.unwrap_err();
        assert_eq!(failure, health);
        drop(transaction);
        drop(cell);
        rig.stop_workers_with_retained_health().await;
    }
}

#[tokio::test]
async fn release_failure_is_idle_retained_until_explicit_same_source_close_retry() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path().join("store.sqlite")).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(request().deadline);
    let (attempt, observation) = store.attempts.reserve().unwrap();
    let mut transaction = rig
        .run(store.begin_original_write_in(attempt, "release retry", &execution))
        .await
        .unwrap();
    rig.run(transaction.put_in(key(), value(), Precondition::Any))
        .await
        .unwrap();
    store.evidence.fail_next_release();
    let CommitOutcome::Committed(receipt) = rig.run(transaction.commit_in()).await.unwrap() else {
        panic!("commit is already known");
    };
    assert_eq!(
        observation.outcome().await.unwrap(),
        AttemptOutcome::Committed(receipt)
    );
    let release = rig.prepare_next().await;
    assert_eq!(release.request.phase, Phase::EvidenceRelease);
    release.publish(&rig.executor).await;
    let source = lock(&rig.receiving.cells)[0].original.clone();
    assert!(!source.is_complete());
    assert!(source.requested().is_none());
    assert!(
        source.requested().is_none(),
        "observation alone must not busy-retry debt"
    );
    // close_in is the existing explicit same-source close observation, not a
    // new service, timer, attempt, dispatch or permission to repeat commit.
    let failure = rig.run(transaction.close_in()).await.unwrap_err();
    assert_eq!(
        failure.downcast_ref::<StateStoreError>().unwrap().kind(),
        StateStoreErrorKind::ProviderUnavailable
    );
    assert!(source.is_complete());
    drop(transaction);
    rig.stop().await;
}

#[tokio::test]
async fn dropped_method_exposes_original_stop_while_real_same_queue_is_full() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path().join("store.sqlite")).await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(store.attempts.scope()));
    let execution = rig.context(request().deadline);
    assert!(execution.shares_owner(&execution.with_deadline(Instant::now())));
    assert_eq!(
        execution
            .with_deadline(execution.deadline() + Duration::from_secs(1))
            .deadline(),
        execution.deadline()
    );
    let mut transaction = rig
        .run(store.begin_original_read_in(&execution))
        .await
        .unwrap();
    let held = rig
        .executor
        .try_reserve()
        .expect("fill same original queue");
    let mut queue_wait = Box::pin(rig.executor.reserve());
    assert!(
        queue_wait
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let item = key();
    let mut get = Box::pin(transaction.get_in(&item));
    assert!(
        get.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let cell = lock(&rig.receiving.cells)[0].clone();
    let request = cell.original.requested().unwrap();
    assert_eq!(request.phase, Phase::Get);
    assert!(cell.original.publication_refusal(request).is_none());
    drop(get);
    assert_eq!(
        cell.original
            .publication_refusal(request)
            .unwrap()
            .downcast_ref::<StateStoreError>()
            .unwrap()
            .kind(),
        StateStoreErrorKind::Cancelled
    );
    // This exact reservation F never acquired a permit or constructed a call.
    // Only its normal return is reported as unpublished, never actual worker exit.
    drop(queue_wait);
    let refusal = cell.original.publication_refusal(request).unwrap();
    lock(&cell.observation).result = Some((request, Err(refusal.clone())));
    cell.original.receive_result(request, Err(refusal));
    lock(&cell.observation).exit = Some((request, Exit::NeverPublishedReturned));
    cell.original
        .receive_exit(request, Ok(Exit::NeverPublishedReturned));
    let close = cell.original.requested().unwrap();
    assert_eq!(close.phase, Phase::Close);
    assert!(
        cell.original.publication_refusal(close).is_none(),
        "stop does not disable settlement"
    );
    assert!(!cell.original.is_complete());
    drop(held);
    drop(transaction);
    rig.drain().await;
    rig.stop().await;
}

struct HeaderDropProbe {
    core: std::sync::Weak<Core<()>>,
    unlocked: Arc<std::sync::atomic::AtomicBool>,
}
impl std::fmt::Debug for HeaderDropProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeaderDropProbe")
    }
}
impl std::fmt::Display for HeaderDropProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("header reentry destructor probe")
    }
}
impl std::error::Error for HeaderDropProbe {}
impl novarocks_failure_contract::FailureProjection for HeaderDropProbe {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        StateStoreError::new(StateStoreErrorKind::Internal, "probe").failure_fact()
    }
}
impl Drop for HeaderDropProbe {
    fn drop(&mut self) {
        let core = self
            .core
            .upgrade()
            .expect("same original header remains alive");
        self.unlocked.store(
            core.header.try_lock().is_ok(),
            std::sync::atomic::Ordering::SeqCst,
        );
    }
}
fn header_drop_core() -> (Arc<Core<()>>, Arc<Receiving>) {
    let receiving = Arc::new(Receiving {
        cells: Mutex::new(Vec::new()),
        origin: StateStoreBlockingOrigin::Opening(crate::SQLITE_STATE_STORE_PROVIDER_ID),
        progress_notifications: std::sync::atomic::AtomicUsize::new(0),
    });
    let port: Arc<dyn StateStoreExecutionPort> = receiving.clone();
    let core = Arc::new(Core::new(
        (),
        Execution::new(&port, Instant::now() + Duration::from_secs(5)),
    ));
    (core, receiving)
}
#[test]
fn rejected_health_payload_is_destroyed_outside_original_header() {
    let (core, _receiving) = header_drop_core();
    let first = invalid("first accepted health");
    core.fail(first.clone());
    let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    core.fail(failure(HeaderDropProbe {
        core: Arc::downgrade(&core),
        unlocked: unlocked.clone(),
    }));
    assert!(unlocked.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(core.health().unwrap(), first);
}
#[test]
fn rejected_exit_payload_is_destroyed_outside_original_header() {
    let (core, _receiving) = header_drop_core();
    // Header acceptance unit only: no physical call or positive exit is claimed.
    let request = core.queue(Phase::Close).unwrap();
    core.result(request, Ok(()));
    let first = invalid("first accepted health");
    core.fail(first.clone());
    let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    assert!(
        core.exit(
            request,
            Err(failure(HeaderDropProbe {
                core: Arc::downgrade(&core),
                unlocked: unlocked.clone(),
            }))
        )
        .is_none()
    );
    assert!(unlocked.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(core.health().unwrap(), first);
    assert!(!core.is_complete());
}

async fn instance_close_fixture() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    Box<dyn StateStoreProviderInstance>,
    std::sync::Weak<crate::SqliteInstanceLifetime>,
) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("instance-close.sqlite");
    // Explicit legacy setup only; every close phase below uses the same QB.
    let store = store(path.clone()).await;
    let lifetime = Arc::downgrade(&store.lifetime);
    let descriptor = novarocks_state_store_api::StateStoreProviderDescriptor::new(
        crate::SQLITE_STATE_STORE_PROVIDER_ID,
        novarocks_state_store_api::MAX_KEY_BYTES,
    );
    (
        temp,
        path,
        crate::provider::instance_from_store(descriptor, store),
        lifetime,
    )
}

#[tokio::test]
async fn instance_close_drops_store_evidence_and_file_in_three_actual_close_phases() {
    use novarocks_state_store_api::StateStoreProviderLifecycle;
    let (_temp, path, mut instance, lifetime) = instance_close_fixture().await;
    let scope = instance.blocking_instance_scope().unwrap();
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(scope));
    let execution = rig.context(request().deadline);
    let external = instance.state_store().unwrap();
    let weak_store = Arc::downgrade(&external);
    let mut closing = None;
    instance.prepare_close_in(&execution, &mut closing).unwrap();
    assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Draining);
    assert!(instance.state_store().is_none());
    let cell = lock(&rig.receiving.cells)[0].clone();
    assert_eq!(
        cell.original.origin(),
        StateStoreBlockingOrigin::Instance(scope)
    );
    assert_eq!(cell.original.as_ref().type_id(), vocabulary().open_type());
    assert!(
        closing
            .as_mut()
            .unwrap()
            .poll_closed(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        cell.original.requested().is_none(),
        "an external Store alias prevents dispatch"
    );
    assert!(crate::acquire_owner_lock(&path).is_err());
    drop(external);
    let close_store = rig.prepare_next().await;
    assert_eq!(close_store.request.phase, Phase::Close);
    let first_revision = close_store.request.revision;
    close_store.publish(&rig.executor).await;
    assert!(weak_store.upgrade().is_none());
    assert!(
        lifetime.upgrade().is_some(),
        "File lease needs its separate worker"
    );
    assert!(!cell.original.is_complete());
    assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Draining);
    assert!(crate::acquire_owner_lock(&path).is_err());
    let close_evidence = rig.prepare_next().await;
    assert_eq!(close_evidence.request.phase, Phase::Close);
    assert!(close_evidence.request.revision > first_revision);
    let evidence_revision = close_evidence.request.revision;
    close_evidence.publish(&rig.executor).await;
    assert!(!cell.original.is_complete());
    assert!(lifetime.upgrade().is_some());
    assert!(crate::acquire_owner_lock(&path).is_err());
    let close_file = rig.prepare_next().await;
    assert_eq!(close_file.request.phase, Phase::Close);
    assert!(close_file.request.revision > evidence_revision);
    close_file.publish(&rig.executor).await;
    assert!(matches!(
        closing
            .as_mut()
            .unwrap()
            .poll_closed(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    assert!(cell.original.is_complete());
    assert!(cell.original.local_failure().is_none());
    assert!(lifetime.upgrade().is_none());
    assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Stopped);
    assert!(crate::acquire_owner_lock(&path).is_ok());
    let reopened = store(path).await;
    drop(reopened);
    drop(closing);
    drop(cell);
    drop(instance);
    rig.stop().await;
}

#[tokio::test]
async fn external_observation_cannot_drop_evidence_connection_outside_close_worker() {
    use novarocks_state_store_api::StateStoreProviderLifecycle;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("observation-close.sqlite");
    let value_store = store(path.clone()).await;
    let lifetime = Arc::downgrade(&value_store.lifetime);
    let connection_exit = value_store.evidence.connection_exit_thread();
    let weak_evidence = Arc::downgrade(&value_store.evidence);
    let descriptor = novarocks_state_store_api::StateStoreProviderDescriptor::new(
        crate::SQLITE_STATE_STORE_PROVIDER_ID,
        novarocks_state_store_api::MAX_KEY_BYTES,
    );
    let mut instance = crate::provider::instance_from_store(descriptor, value_store);
    let external = instance.state_store().unwrap();
    let (attempt, observation) = external.attempts().reserve().unwrap();
    attempt.cancel_before_dispatch().unwrap();
    drop(attempt);
    let scope = instance.blocking_instance_scope().unwrap();
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(scope));
    let execution = rig.context(request().deadline);
    let mut closing = None;
    instance.prepare_close_in(&execution, &mut closing).unwrap();
    drop(external);
    let close_store = rig.prepare_next().await;
    let store_revision = close_store.request.revision;
    close_store.publish(&rig.executor).await;
    let source = lock(&rig.receiving.cells)[0].original.clone();
    assert!(!source.is_complete());
    for _ in 0..3 {
        assert!(source.requested().is_none());
        assert!(
            closing
                .as_mut()
                .unwrap()
                .poll_closed(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Draining);
    assert!(lifetime.upgrade().is_some());
    assert!(crate::acquire_owner_lock(&path).is_err());
    assert!(observation.peek().unwrap().is_none());
    let caller_thread = std::thread::current().id();
    drop(observation);
    // The same source owns Evidence; Observation is no longer its last alias.
    // This marker drops AFTER the real Connection field, not at Drop entry.
    assert!(lock(&connection_exit).is_none());
    assert!(weak_evidence.upgrade().is_some());
    let selected = rig.prepare_next().await;
    assert_eq!(selected.request.phase, Phase::Close);
    assert!(selected.request.revision > store_revision);
    let selected_revision = selected.request.revision;
    let raced = weak_evidence
        .upgrade()
        .expect("same original Evidence alias");
    let before = rig
        .receiving
        .progress_notifications
        .load(std::sync::atomic::Ordering::SeqCst);
    selected.publish(&rig.executor).await;
    assert_eq!(
        rig.receiving
            .progress_notifications
            .load(std::sync::atomic::Ordering::SeqCst),
        before
    );
    assert!(!source.is_complete());
    assert!(source.requested().is_none());
    assert!(lock(&connection_exit).is_none());
    drop(raced);
    assert!(lock(&connection_exit).is_none());
    let close_evidence = rig.prepare_next().await;
    assert!(close_evidence.request.revision > selected_revision);
    let evidence_revision = close_evidence.request.revision;
    let physical = close_evidence.call.clone();
    close_evidence.publish(&rig.executor).await;
    let worker_thread = lock(&physical.ran_on).expect("same real QB worker ran");
    assert_ne!(worker_thread, caller_thread);
    assert_eq!(*lock(&connection_exit), Some(worker_thread));
    assert!(weak_evidence.upgrade().is_none());
    assert!(!source.is_complete());
    assert!(lifetime.upgrade().is_some());
    assert!(crate::acquire_owner_lock(&path).is_err());
    let close_file = rig.prepare_next().await;
    assert_eq!(close_file.request.phase, Phase::Close);
    assert!(close_file.request.revision > evidence_revision);
    close_file.publish(&rig.executor).await;
    assert!(matches!(
        closing
            .as_mut()
            .unwrap()
            .poll_closed(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Stopped);
    assert!(lifetime.upgrade().is_none());
    assert!(crate::acquire_owner_lock(&path).is_ok());
    drop(physical);
    drop(source);
    drop(closing);
    drop(instance);
    rig.stop().await;
}

#[tokio::test]
async fn unpublished_instance_close_and_dropped_handle_keep_the_same_source_for_actual_close() {
    use novarocks_state_store_api::StateStoreProviderLifecycle;
    let (_temp, path, mut instance, _) = instance_close_fixture().await;
    let scope = instance.blocking_instance_scope().unwrap();
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(scope));
    let execution = rig.context(request().deadline);
    let mut closing = None;
    instance.prepare_close_in(&execution, &mut closing).unwrap();
    assert!(
        closing
            .as_mut()
            .unwrap()
            .poll_closed(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let source = lock(&rig.receiving.cells)[0].original.clone();
    let withdrawn = rig.prepare_next().await;
    assert_eq!(withdrawn.request.phase, Phase::Close);
    withdrawn.withdraw();
    assert!(!source.is_complete());
    assert!(crate::acquire_owner_lock(&path).is_err());
    drop(closing.take());
    assert_eq!(lock(&rig.receiving.cells).len(), 1);
    let foreign = Arc::new(Receiving {
        cells: Mutex::new(Vec::new()),
        origin: StateStoreBlockingOrigin::Instance(scope),
        progress_notifications: std::sync::atomic::AtomicUsize::new(0),
    });
    let foreign_port: Arc<dyn StateStoreExecutionPort> = foreign.clone();
    let foreign_context = Execution::new(&foreign_port, request().deadline);
    assert!(
        instance
            .prepare_close_in(&foreign_context, &mut closing)
            .is_err()
    );
    assert!(closing.is_none());
    assert!(lock(&foreign.cells).is_empty());
    instance.prepare_close_in(&execution, &mut closing).unwrap();
    assert_eq!(lock(&rig.receiving.cells).len(), 1);
    assert!(Arc::ptr_eq(
        &source,
        &lock(&rig.receiving.cells)[0].original
    ));
    rig.drain().await;
    assert!(matches!(
        closing
            .as_mut()
            .unwrap()
            .poll_closed(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Stopped);
    assert!(crate::acquire_owner_lock(&path).is_ok());
    drop(source);
    drop(closing);
    drop(instance);
    rig.stop().await;
}

#[tokio::test]
async fn instance_close_physical_panic_keeps_raw_health_and_cannot_report_stopped() {
    use novarocks_state_store_api::StateStoreProviderLifecycle;
    let (_temp, path, mut instance, _) = instance_close_fixture().await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(
        instance.blocking_instance_scope().unwrap(),
    ));
    let execution = rig.context(request().deadline);
    let mut closing = None;
    instance.prepare_close_in(&execution, &mut closing).unwrap();
    let failed = rig.prepare_next().await;
    failed
        .call
        .panic_after_run
        .store(true, std::sync::atomic::Ordering::Release);
    failed.publish(&rig.executor).await;
    let cell = lock(&rig.receiving.cells)[0].clone();
    let original = cell.original.local_failure().unwrap();
    assert!(
        original
            .downcast_ref::<novarocks_query_application::cpu::QueryBlockingFailure>()
            .is_some()
    );
    let Poll::Ready(Err(error)) = closing
        .as_mut()
        .unwrap()
        .poll_closed(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("same original physical health must reach close borrower");
    };
    assert_eq!(error, original);
    assert_eq!(lock(&cell.health).as_ref().unwrap(), &original);
    assert!(!cell.original.is_complete());
    assert!(cell.original.requested().is_none());
    assert_eq!(instance.lifecycle(), StateStoreProviderLifecycle::Draining);
    assert!(crate::acquire_owner_lock(&path).is_err());
    drop(closing);
    drop(instance);
    drop(cell);
    rig.stop_workers_with_retained_health().await;
}

#[tokio::test]
async fn weak_upgrade_after_close_selection_restores_same_store_without_self_retry() {
    let (_temp, path, mut instance, _) = instance_close_fixture().await;
    let rig = Rig::new(StateStoreBlockingOrigin::Instance(
        instance.blocking_instance_scope().unwrap(),
    ));
    let execution = rig.context(request().deadline);
    let external = instance.state_store().unwrap();
    let weak = Arc::downgrade(&external);
    let mut closing = None;
    instance.prepare_close_in(&execution, &mut closing).unwrap();
    drop(external);
    let selected = rig.prepare_next().await;
    assert_eq!(selected.request.phase, Phase::Close);
    // Select at strong==1, then introduce a genuine alias through Weak before
    // the selected physical worker runs. A count is not a last-drop proof.
    let raced = weak.upgrade().expect("same selected Store still exists");
    let before = rig
        .receiving
        .progress_notifications
        .load(std::sync::atomic::Ordering::SeqCst);
    selected.publish(&rig.executor).await;
    assert_eq!(
        rig.receiving
            .progress_notifications
            .load(std::sync::atomic::Ordering::SeqCst),
        before
    );
    let source = lock(&rig.receiving.cells)[0].original.clone();
    assert!(source.requested().is_none());
    assert!(!source.is_complete());
    assert!(weak.upgrade().is_some());
    assert!(crate::acquire_owner_lock(&path).is_err());
    drop(raced);
    // Host's explicit next observation retries the SAME restored Arc/Source.
    rig.drain().await;
    assert!(matches!(
        closing
            .as_mut()
            .unwrap()
            .poll_closed(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    assert!(weak.upgrade().is_none());
    assert!(crate::acquire_owner_lock(&path).is_ok());
    drop(source);
    drop(closing);
    drop(instance);
    rig.stop().await;
}

#[tokio::test]
async fn cancelled_legacy_evidence_workers_keep_actual_connection_and_file_until_their_exit() {
    for release_evidence in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("legacy-tail.sqlite");
        let value_store = store(path.clone()).await;
        let evidence = value_store.evidence.clone();
        let lifetime = Arc::downgrade(&value_store.lifetime);
        let descriptor = novarocks_state_store_api::StateStoreProviderDescriptor::new(
            crate::SQLITE_STATE_STORE_PROVIDER_ID,
            novarocks_state_store_api::MAX_KEY_BYTES,
        );
        let mut instance = crate::provider::instance_from_store(descriptor, value_store);
        let external = instance.state_store().unwrap();
        let rig = Rig::new(StateStoreBlockingOrigin::Instance(
            instance.blocking_instance_scope().unwrap(),
        ));
        let execution = rig.context(request().deadline);
        let (attempt, observation) = external.attempts().reserve().unwrap();
        let id = attempt.id();
        let mut transaction = rig
            .run(external.begin_original_write_in(attempt, "legacy evidence tail", &execution))
            .await
            .unwrap();
        rig.run(transaction.put_in(key(), value(), Precondition::Any))
            .await
            .unwrap();
        let mut commit = Some(Box::pin(transaction.commit_in()));
        assert!(
            commit
                .as_mut()
                .unwrap()
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let applied = rig.prepare_next().await;
        assert_eq!(applied.request.phase, Phase::Commit);
        applied.publish(&rig.executor).await;
        // There is a real finished commit worker, not a fabricated liveness guard.
        if release_evidence {
            assert!(matches!(
                rig.run(commit.take().unwrap()).await.unwrap(),
                CommitOutcome::Committed(_)
            ));
        }
        let (entered, release) = evidence.pause_next_legacy_worker();
        let mut resolving: std::pin::Pin<Box<dyn Future<Output = ()> + '_>> = if release_evidence {
            Box::pin(async {
                let _ = novarocks_state_store_api::InDoubtAdjudicator::release_evidence(
                    evidence.as_ref(),
                    id,
                )
                .await;
            })
        } else {
            Box::pin(async {
                let _ = observation.outcome().await;
            })
        };
        assert!(
            resolving
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .expect("real legacy worker starts")
            .unwrap();
        drop(resolving); // Cancels only the awaiter; the actual worker is held.
        if !release_evidence {
            assert!(matches!(
                rig.run(commit.take().unwrap()).await.unwrap(),
                CommitOutcome::Committed(_)
            ));
        }
        drop(commit);
        drop(transaction);
        rig.drain().await;
        assert!(matches!(
            observation.peek().unwrap(),
            Some(AttemptOutcome::Committed(_))
        ));
        drop(observation);
        drop(external);
        drop(evidence);
        let mut closing = None;
        instance.prepare_close_in(&execution, &mut closing).unwrap();
        rig.prepare_next().await.publish(&rig.executor).await;
        let close_evidence = rig.prepare_next().await;
        assert_eq!(close_evidence.request.phase, Phase::Close);
        close_evidence.publish(&rig.executor).await;
        let source = lock(&rig.receiving.cells).last().unwrap().original.clone();
        assert!(
            source.requested().is_none(),
            "cancelled legacy worker still owns its connection/file bundle"
        );
        assert!(!source.is_complete());
        assert!(lifetime.upgrade().is_some());
        assert!(crate::acquire_owner_lock(&path).is_err());
        assert!(
            closing
                .as_mut()
                .unwrap()
                .poll_closed(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        release.send(()).unwrap();
        // Test-only bounded Host observation. The timer/yield proves nothing:
        // the same Source still checks actual pins and try_unwrap on its worker.
        tokio::time::timeout(Duration::from_secs(5), async {
            while source.requested().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("original File pin actually leaves the legacy worker");
        rig.prepare_next().await.publish(&rig.executor).await;
        assert!(matches!(
            closing
                .as_mut()
                .unwrap()
                .poll_closed(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(
            instance.lifecycle(),
            novarocks_state_store_api::StateStoreProviderLifecycle::Stopped
        );
        assert!(lifetime.upgrade().is_none());
        assert!(crate::acquire_owner_lock(&path).is_ok());
        drop(source);
        drop(closing);
        drop(instance);
        rig.stop().await;
    }
}

mod evidence_tests;
