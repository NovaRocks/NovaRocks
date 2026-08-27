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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use rusqlite::{Connection, InterruptHandle, OptionalExtension, ffi, params};

use novarocks_spi::state_store::{
    CommitOutcome, CommitReceipt, CommitResolution, Key, Precondition, RangePage, RangeRequest,
    ReadTransaction, StateRecord, StateStoreError, StateStoreErrorKind, StateStoreLimits,
    StateStoreOperation, StateStoreOutcome, StoreRevision, TransactionId, Value, VersionToken,
    WriteTransaction,
};

use novarocks_spi::state_store::StateStoreMetrics;

use super::{SqliteHistoryRetentionConfig, SqliteStateStore, history, open_connection, schema};

const MUTATION_KIND_BYTES: usize = 1;
const PRECONDITION_KIND_BYTES: usize = 1;
const PERSISTED_VERSION_BYTES: usize = size_of::<u64>();
const CHANGE_REVISION_BYTES: usize = size_of::<u64>();
const CHANGE_SEQUENCE_BYTES: usize = size_of::<u32>();
const COMMIT_TRANSACTION_ID_BYTES: usize = 16;
const COMMIT_REVISION_BYTES: usize = size_of::<u64>();
const COMMIT_TIMESTAMP_BYTES: usize = size_of::<i64>();
const CURRENT_REVISION_BYTES: usize = size_of::<u64>();
const TRANSACTION_ENVELOPE_BYTES: usize = COMMIT_TRANSACTION_ID_BYTES
    + COMMIT_REVISION_BYTES
    + COMMIT_TIMESTAMP_BYTES
    + CURRENT_REVISION_BYTES;
const SQLITE_BUSY_SNAPSHOT: i32 = ffi::SQLITE_BUSY_SNAPSHOT;
const PROVISIONAL_VERSION_TAG: &[u8] = b"sqlite-provisional-v1\0";
const SQLITE_BUSY_RETRY_LIMIT: Duration = Duration::from_millis(50);
const SQLITE_BUSY_RETRY_DELAY: Duration = Duration::from_millis(1);

pub(super) type CommitRegistry = Arc<Mutex<HashMap<TransactionId, CommitRegistryState>>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CommitRegistryState {
    InFlight,
    Committed(CommitReceipt),
    NotCommitted,
}

struct RecoveryReservation {
    // Commit attempts and other resolvers only observe this InFlight entry. The blocking
    // resolver closure that created the guard is the sole terminal publisher.
    registry: CommitRegistry,
    transaction_id: TransactionId,
    active: bool,
}

impl RecoveryReservation {
    fn new(registry: &CommitRegistry, transaction_id: TransactionId) -> Self {
        Self {
            registry: Arc::clone(registry),
            transaction_id,
            active: true,
        }
    }

    fn publish(
        mut self,
        terminal: CommitRegistryState,
    ) -> Result<CommitResolution, StateStoreError> {
        let resolution = registry_resolution(&terminal);
        let mut registry = lock_registry(&self.registry)?;
        if !matches!(
            registry.get(&self.transaction_id),
            Some(CommitRegistryState::InFlight)
        ) {
            return Err(internal_error());
        }
        registry.insert(self.transaction_id, terminal);
        self.active = false;
        Ok(resolution)
    }
}

impl Drop for RecoveryReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut registry) = self.registry.lock()
            && matches!(
                registry.get(&self.transaction_id),
                Some(CommitRegistryState::InFlight)
            )
        {
            registry.remove(&self.transaction_id);
        }
    }
}

pub(super) fn new_commit_registry() -> CommitRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(super) type TestHooks = Arc<TestHookState>;

#[cfg(test)]
#[derive(Default)]
pub(super) struct TestHookState {
    resolve_after_lookup: Mutex<Option<TestGate>>,
    commit_after_inflight: Mutex<Option<TestGate>>,
    range_after_refill: Mutex<Option<TestGate>>,
    range_refill_count: AtomicUsize,
    fail_next_operation_worker: AtomicBool,
    panic_next_commit_before_apply: AtomicBool,
    panic_next_commit_after_apply: AtomicBool,
}

#[cfg(test)]
#[derive(Clone)]
pub(super) struct TestGate {
    reached: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}

#[cfg(test)]
impl TestGate {
    fn new() -> Self {
        Self {
            reached: Arc::new(std::sync::Barrier::new(2)),
            release: Arc::new(std::sync::Barrier::new(2)),
        }
    }

    fn pause(&self) {
        self.reached.wait();
        self.release.wait();
    }

    async fn wait_reached(&self) {
        let reached = Arc::clone(&self.reached);
        tokio::task::spawn_blocking(move || reached.wait())
            .await
            .expect("test gate reach worker");
    }

    async fn release(&self) {
        let release = Arc::clone(&self.release);
        tokio::task::spawn_blocking(move || release.wait())
            .await
            .expect("test gate release worker");
    }
}

#[cfg(test)]
impl TestHookState {
    fn pause_resolve_after_lookup(&self) {
        if let Some(gate) = self
            .resolve_after_lookup
            .lock()
            .expect("resolve test hook")
            .take()
        {
            gate.pause();
        }
    }

    fn pause_commit_after_inflight(&self) {
        if let Some(gate) = self
            .commit_after_inflight
            .lock()
            .expect("commit test hook")
            .take()
        {
            gate.pause();
        }
    }

    fn panic_commit_before_apply(&self) {
        if self
            .panic_next_commit_before_apply
            .swap(false, Ordering::AcqRel)
        {
            panic!("injected SQLite commit worker failure before apply");
        }
    }

    fn panic_commit_after_apply(&self) {
        if self
            .panic_next_commit_after_apply
            .swap(false, Ordering::AcqRel)
        {
            panic!("injected SQLite commit worker failure after apply");
        }
    }

    fn range_refill_started(&self) {
        self.range_refill_count.fetch_add(1, Ordering::AcqRel);
    }

    fn pause_range_after_refill(&self) {
        if let Some(gate) = self
            .range_after_refill
            .lock()
            .expect("range test hook")
            .take()
        {
            gate.pause();
        }
    }
}

#[cfg(test)]
pub(super) fn new_test_hooks() -> TestHooks {
    Arc::new(TestHookState::default())
}

#[derive(Clone, Debug)]
pub(super) enum Mutation {
    Put {
        value: Value,
        precondition: Precondition,
        provisional_version: VersionToken,
    },
    Delete {
        precondition: Precondition,
    },
}

pub(super) struct SqliteTxnState {
    pub(super) connection: Connection,
    pub(super) overlay: BTreeMap<Key, Mutation>,
    mutations: Vec<(Key, Mutation)>,
    operation_count: usize,
    accounted_bytes: usize,
    limits: StateStoreLimits,
    pub(super) deadline: Instant,
    pub(super) cancelled: Arc<AtomicBool>,
    pub(super) range_frozen: bool,
    pub(super) interrupt_handle: Arc<InterruptHandle>,
    pub(super) snapshot_established: bool,
    #[cfg(test)]
    test_hooks: TestHooks,
    active: bool,
}

#[derive(Clone)]
struct TxnOwner {
    state: Arc<Mutex<SqliteTxnState>>,
    limits: StateStoreLimits,
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    interrupt_handle: Arc<InterruptHandle>,
    metrics: Arc<StateStoreMetrics>,
}

pub(super) struct SqliteReadTransaction {
    owner: Option<TxnOwner>,
    metrics: Arc<StateStoreMetrics>,
}

pub(super) struct SqliteWriteTransaction {
    owner: Option<TxnOwner>,
    metrics: Arc<StateStoreMetrics>,
    transaction_id: TransactionId,
    path: PathBuf,
    history_retention: SqliteHistoryRetentionConfig,
    commit_registry: CommitRegistry,
    #[cfg(test)]
    test_hooks: TestHooks,
}

impl SqliteStateStore {
    pub(super) async fn begin_read(&self) -> Result<SqliteReadTransaction, StateStoreError> {
        let started = Instant::now();
        let result = begin_transaction(
            self.path.clone(),
            self.limits.clone(),
            Arc::clone(&self.metrics),
            #[cfg(test)]
            Arc::clone(&self.test_hooks),
        )
        .await;
        record_result(&self.metrics, StateStoreOperation::Begin, started, &result);
        Ok(SqliteReadTransaction {
            owner: Some(result?),
            metrics: Arc::clone(&self.metrics),
        })
    }

    pub(super) async fn begin_write(
        &self,
        transaction_id: TransactionId,
    ) -> Result<SqliteWriteTransaction, StateStoreError> {
        let started = Instant::now();
        let result = match validate_transaction_envelope(&self.limits) {
            Ok(()) => {
                begin_transaction(
                    self.path.clone(),
                    self.limits.clone(),
                    Arc::clone(&self.metrics),
                    #[cfg(test)]
                    Arc::clone(&self.test_hooks),
                )
                .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Begin, started, &result);
        let owner = result?;
        let transaction_id_bytes = *transaction_id.as_uuid().as_bytes();
        if let Err(error) = run_operation(&owner, move |state| {
            if history::transaction_id_is_retired(&state.connection, &transaction_id_bytes)? {
                return Err(retired_transaction_error());
            }
            Ok(())
        })
        .await
        {
            schedule_rollback(owner);
            return Err(error);
        }
        Ok(SqliteWriteTransaction {
            owner: Some(owner),
            metrics: Arc::clone(&self.metrics),
            transaction_id,
            path: self.path.clone(),
            history_retention: self.history_retention.clone(),
            commit_registry: Arc::clone(&self.commit_registry),
            #[cfg(test)]
            test_hooks: Arc::clone(&self.test_hooks),
        })
    }

    pub(super) async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        let path = self.path.clone();
        let registry = Arc::clone(&self.commit_registry);
        #[cfg(test)]
        let test_hooks = Arc::clone(&self.test_hooks);
        let transaction_id = *transaction_id;
        tokio::task::spawn_blocking(move || {
            let reservation = {
                let mut registry_guard = lock_registry(&registry)?;
                if let Some(state) = registry_guard.get(&transaction_id) {
                    return Ok(registry_resolution(state));
                }
                registry_guard.insert(transaction_id, CommitRegistryState::InFlight);
                RecoveryReservation::new(&registry, transaction_id)
            };

            let terminal = match lookup_commit_resolution(&path, transaction_id)? {
                CommitResolution::Committed(receipt) => CommitRegistryState::Committed(receipt),
                CommitResolution::NotCommitted => CommitRegistryState::NotCommitted,
                CommitResolution::Unresolved => CommitRegistryState::InFlight,
            };
            #[cfg(test)]
            test_hooks.pause_resolve_after_lookup();
            reservation.publish(terminal)
        })
        .await
        .map_err(|_| worker_error())?
    }
}

impl SqliteReadTransaction {
    pub(super) async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = Instant::now();
        let result = match self.owner().and_then(|owner| {
            validate_key_value(key, None, &owner.limits)?;
            Ok(owner.clone())
        }) {
            Ok(owner) => get(&owner, key.clone()).await,
            Err(error) => Err(error),
        };
        record_read_result(&self.metrics, StateStoreOperation::Get, started, &result);
        result
    }

    pub(super) async fn range(
        &mut self,
        request: &RangeRequest,
    ) -> Result<RangePage, StateStoreError> {
        let started = Instant::now();
        let result = match self.owner().and_then(|owner| {
            validate_range_request(request, &owner.limits)?;
            Ok(owner.clone())
        }) {
            Ok(owner) => {
                let request = request.clone();
                run_operation(&owner, move |state| {
                    super::range::range_page(state, &request)
                })
                .await
            }
            Err(error) => Err(error),
        };
        record_range_result(&self.metrics, started, &result);
        result
    }

    pub(super) async fn abort(mut self) -> Result<(), StateStoreError> {
        let owner = self.take_owner()?;
        run_operation(&owner, rollback).await
    }

    fn owner(&self) -> Result<&TxnOwner, StateStoreError> {
        self.owner.as_ref().ok_or_else(transaction_finished)
    }

    fn take_owner(&mut self) -> Result<TxnOwner, StateStoreError> {
        self.owner.take().ok_or_else(transaction_finished)
    }
}

impl Drop for SqliteReadTransaction {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            schedule_rollback(owner);
        }
    }
}

impl SqliteWriteTransaction {
    pub(super) async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = Instant::now();
        let result = match self.owner().and_then(|owner| {
            validate_key_value(key, None, &owner.limits)?;
            Ok(owner.clone())
        }) {
            Ok(owner) => get(&owner, key.clone()).await,
            Err(error) => Err(error),
        };
        record_read_result(&self.metrics, StateStoreOperation::Get, started, &result);
        result
    }

    pub(super) async fn range(
        &mut self,
        request: &RangeRequest,
    ) -> Result<RangePage, StateStoreError> {
        let started = Instant::now();
        let result = match self.owner().and_then(|owner| {
            validate_range_request(request, &owner.limits)?;
            Ok(owner.clone())
        }) {
            Ok(owner) => {
                let request = request.clone();
                run_operation(&owner, move |state| {
                    let page = super::range::range_page(state, &request)?;
                    if page.continuation.is_some() {
                        state.range_frozen = true;
                    }
                    Ok(page)
                })
                .await
            }
            Err(error) => Err(error),
        };
        record_range_result(&self.metrics, started, &result);
        result
    }

    pub(super) async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = Instant::now();
        let transaction_id = self.transaction_id;
        let setup = self.owner().and_then(|owner| {
            validate_key_value(&key, Some(&value), &owner.limits)?;
            let measured_bytes = accounted_mutation_bytes(
                &key,
                &Mutation::Put {
                    value: value.clone(),
                    precondition: precondition.clone(),
                    provisional_version: provisional_version(transaction_id, 1),
                },
            )?;
            Ok((owner.clone(), measured_bytes))
        });
        let result = match setup {
            Ok((owner, measured_bytes)) => {
                run_operation(&owner, move |state| {
                    let next_operation = next_operation_count(state)?;
                    let provisional_version = provisional_version(transaction_id, next_operation);
                    stage_mutation(
                        state,
                        key,
                        Mutation::Put {
                            value,
                            precondition,
                            provisional_version,
                        },
                        next_operation,
                    )?;
                    Ok(measured_bytes)
                })
                .await
            }
            Err(error) => Err(error),
        };
        record_write_result(&self.metrics, StateStoreOperation::Put, started, &result);
        result.map(|_| ())
    }

    pub(super) async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = Instant::now();
        let setup = self.owner().and_then(|owner| {
            validate_key_value(&key, None, &owner.limits)?;
            let measured_bytes = accounted_mutation_bytes(
                &key,
                &Mutation::Delete {
                    precondition: precondition.clone(),
                },
            )?;
            Ok((owner.clone(), measured_bytes))
        });
        let result = match setup {
            Ok((owner, measured_bytes)) => {
                run_operation(&owner, move |state| {
                    let next_operation = next_operation_count(state)?;
                    stage_mutation(
                        state,
                        key,
                        Mutation::Delete { precondition },
                        next_operation,
                    )?;
                    Ok(measured_bytes)
                })
                .await
            }
            Err(error) => Err(error),
        };
        record_write_result(&self.metrics, StateStoreOperation::Delete, started, &result);
        result.map(|_| ())
    }

    pub(super) async fn abort(mut self) -> Result<(), StateStoreError> {
        let owner = self.take_owner()?;
        run_operation(&owner, rollback).await
    }

    pub(super) async fn commit(self) -> CommitOutcome {
        let metrics = Arc::clone(&self.metrics);
        let started = Instant::now();
        let outcome = self.commit_inner().await;
        let metric_outcome = commit_metric_outcome(&outcome);
        metrics.record_operation(
            StateStoreOperation::Commit,
            metric_outcome,
            started.elapsed(),
        );
        outcome
    }

    async fn commit_inner(mut self) -> CommitOutcome {
        let owner = match self.take_owner() {
            Ok(owner) => owner,
            Err(error) => return CommitOutcome::DefiniteFailure(error),
        };

        match register_inflight(&self.commit_registry, self.transaction_id) {
            Ok(RegisterOutcome::AlreadyCommitted(receipt)) => {
                schedule_rollback(owner);
                return CommitOutcome::Committed(receipt);
            }
            Ok(RegisterOutcome::Registered) => {}
            Ok(RegisterOutcome::NotCommitted) => {
                schedule_rollback(owner);
                return CommitOutcome::DefiniteFailure(StateStoreError::new(
                    StateStoreErrorKind::InvalidRequest,
                    "SQLite transaction id is terminally not committed",
                ));
            }
            Ok(RegisterOutcome::InFlight) => {
                schedule_rollback(owner);
                return CommitOutcome::CommitUnknown(StateStoreError::new(
                    StateStoreErrorKind::Conflict,
                    "SQLite transaction id commit is already in flight",
                ));
            }
            Err(error) => {
                schedule_rollback(owner);
                return CommitOutcome::CommitUnknown(error);
            }
        }

        let state = Arc::clone(&owner.state);
        let registry = Arc::clone(&self.commit_registry);
        let transaction_id = self.transaction_id;
        let path = self.path.clone();
        let recovery_registry = Arc::clone(&registry);
        let recovery_path = path.clone();
        let history_retention = self.history_retention.clone();
        #[cfg(test)]
        let test_hooks = Arc::clone(&self.test_hooks);
        let mut cancel_guard = CancelOnDrop::new(&owner);
        let mut worker = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            {
                test_hooks.pause_commit_after_inflight();
                test_hooks.panic_commit_before_apply();
            }
            let outcome = match state.lock() {
                Ok(mut state) => {
                    commit_blocking(&mut state, transaction_id, &path, &history_retention)
                }
                Err(_) => CommitOutcome::CommitUnknown(internal_error()),
            };
            #[cfg(test)]
            test_hooks.panic_commit_after_apply();
            finalize_registry(&registry, transaction_id, &outcome);
            outcome
        });

        let deadline = tokio::time::Instant::from_std(owner.deadline);
        let outcome = match tokio::time::timeout_at(deadline, &mut worker).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => {
                owner.metrics.record_blocking_failure();
                cancel_guard.disarm();
                drop(owner);
                recover_commit_after_worker_failure(
                    recovery_path,
                    recovery_registry,
                    transaction_id,
                )
                .await;
                CommitOutcome::CommitUnknown(worker_error())
            }
            Err(_) => {
                cancel_guard.cancel();
                match worker.await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        owner.metrics.record_blocking_failure();
                        cancel_guard.disarm();
                        drop(owner);
                        recover_commit_after_worker_failure(
                            recovery_path,
                            recovery_registry,
                            transaction_id,
                        )
                        .await;
                        return CommitOutcome::CommitUnknown(worker_error());
                    }
                }
            }
        };
        cancel_guard.disarm();
        outcome
    }

    fn owner(&self) -> Result<&TxnOwner, StateStoreError> {
        self.owner.as_ref().ok_or_else(transaction_finished)
    }

    fn take_owner(&mut self) -> Result<TxnOwner, StateStoreError> {
        self.owner.take().ok_or_else(transaction_finished)
    }
}

async fn recover_commit_after_worker_failure(
    path: PathBuf,
    registry: CommitRegistry,
    transaction_id: TransactionId,
) {
    let recovery_registry = Arc::clone(&registry);
    let recovery = tokio::task::spawn_blocking(move || {
        let terminal = match lookup_commit(&path, transaction_id) {
            Ok(Some(receipt)) => Some(CommitRegistryState::Committed(receipt)),
            Ok(None) => Some(CommitRegistryState::NotCommitted),
            Err(_) => None,
        };
        if let Ok(mut registry) = recovery_registry.lock()
            && matches!(
                registry.get(&transaction_id),
                Some(CommitRegistryState::InFlight)
            )
        {
            match terminal {
                Some(terminal) => {
                    registry.insert(transaction_id, terminal);
                }
                None => {
                    registry.remove(&transaction_id);
                }
            }
        }
    })
    .await;
    if recovery.is_err()
        && let Ok(mut registry) = registry.lock()
        && matches!(
            registry.get(&transaction_id),
            Some(CommitRegistryState::InFlight)
        )
    {
        registry.remove(&transaction_id);
    }
}

impl Drop for SqliteWriteTransaction {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            schedule_rollback(owner);
        }
    }
}

async fn begin_transaction(
    path: PathBuf,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
    #[cfg(test)] test_hooks: TestHooks,
) -> Result<TxnOwner, StateStoreError> {
    let deadline = Instant::now() + limits.transaction_deadline;
    let cancelled = Arc::new(AtomicBool::new(false));
    let interrupt_slot = Arc::new(Mutex::new(None));
    let worker_cancelled = Arc::clone(&cancelled);
    let worker_interrupt_slot = Arc::clone(&interrupt_slot);
    let mut cancel_guard =
        BeginCancelOnDrop::new(Arc::clone(&cancelled), Arc::clone(&interrupt_slot));
    let mut worker = tokio::task::spawn_blocking(move || {
        let connection = open_connection(&path)?;
        let interrupt_handle = Arc::new(connection.get_interrupt_handle());
        *worker_interrupt_slot.lock().map_err(|_| internal_error())? =
            Some(Arc::clone(&interrupt_handle));
        if worker_cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(deadline_error());
        }
        connection
            .busy_timeout(remaining(deadline))
            .map_err(|error| operation_error(&error, "failed to configure SQLite transaction"))?;
        connection
            .execute_batch("BEGIN DEFERRED")
            .map_err(|error| {
                operation_error(&error, "failed to begin SQLite deferred transaction")
            })?;
        if worker_cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            connection.execute_batch("ROLLBACK").map_err(|error| {
                operation_error(&error, "failed to roll back timed out SQLite begin")
            })?;
            return Err(deadline_error());
        }
        Ok::<_, StateStoreError>(SqliteTxnState {
            connection,
            overlay: BTreeMap::new(),
            mutations: Vec::new(),
            operation_count: 0,
            accounted_bytes: TRANSACTION_ENVELOPE_BYTES,
            limits,
            deadline,
            cancelled: worker_cancelled,
            range_frozen: false,
            interrupt_handle,
            snapshot_established: false,
            #[cfg(test)]
            test_hooks,
            active: true,
        })
    });
    let state = match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), &mut worker)
        .await
    {
        Ok(joined) => joined.map_err(|_| {
            metrics.record_blocking_failure();
            worker_error()
        })??,
        Err(_) => {
            cancel_guard.cancel();
            if let Ok(Ok(mut state)) = worker.await {
                tokio::task::spawn_blocking(move || rollback(&mut state))
                    .await
                    .map_err(|_| worker_error())??;
            }
            cancel_guard.disarm();
            return Err(deadline_error());
        }
    };
    cancel_guard.disarm();

    let limits = state.limits.clone();
    let cancelled = Arc::clone(&state.cancelled);
    let interrupt_handle = Arc::clone(&state.interrupt_handle);
    Ok(TxnOwner {
        state: Arc::new(Mutex::new(state)),
        limits,
        deadline,
        cancelled,
        interrupt_handle,
        metrics,
    })
}

pub(super) fn range_refill_started(state: &SqliteTxnState) {
    #[cfg(test)]
    state.test_hooks.range_refill_started();
    #[cfg(not(test))]
    let _ = state;
}

pub(super) fn range_refill_completed(state: &SqliteTxnState) {
    #[cfg(test)]
    state.test_hooks.pause_range_after_refill();
    #[cfg(not(test))]
    let _ = state;
}

struct BeginCancelOnDrop {
    cancelled: Arc<AtomicBool>,
    interrupt_slot: Arc<Mutex<Option<Arc<InterruptHandle>>>>,
    armed: bool,
}

impl BeginCancelOnDrop {
    fn new(
        cancelled: Arc<AtomicBool>,
        interrupt_slot: Arc<Mutex<Option<Arc<InterruptHandle>>>>,
    ) -> Self {
        Self {
            cancelled,
            interrupt_slot,
            armed: true,
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Ok(interrupt_slot) = self.interrupt_slot.lock()
            && let Some(interrupt_handle) = interrupt_slot.as_ref()
        {
            interrupt_handle.interrupt();
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BeginCancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancel();
        }
    }
}

async fn get(owner: &TxnOwner, key: Key) -> Result<Option<StateRecord>, StateStoreError> {
    let owner = owner.clone();
    run_operation(&owner, move |state| {
        if let Some(mutation) = state.overlay.get(&key).cloned() {
            return match mutation {
                Mutation::Delete { .. } => Ok(None),
                Mutation::Put {
                    value,
                    provisional_version,
                    ..
                } => Ok(Some(StateRecord {
                    key,
                    value,
                    version: provisional_version,
                })),
            };
        }
        let record = load_record(&state.connection, &key)?;
        state.snapshot_established = true;
        Ok(record)
    })
    .await
}

async fn run_operation<T, F>(owner: &TxnOwner, operation: F) -> Result<T, StateStoreError>
where
    T: Send + 'static,
    F: FnOnce(&mut SqliteTxnState) -> Result<T, StateStoreError> + Send + 'static,
{
    let state = Arc::clone(&owner.state);
    let timeout_state = Arc::clone(&owner.state);
    let cancelled = Arc::clone(&owner.cancelled);
    let deadline = owner.deadline;
    let mut cancel_guard = CancelOnDrop::new(owner);
    let mut worker = tokio::task::spawn_blocking(move || {
        let mut state = state.lock().map_err(|_| internal_error())?;
        #[cfg(test)]
        if state
            .test_hooks
            .fail_next_operation_worker
            .swap(false, Ordering::AcqRel)
        {
            panic!("injected SQLite operation worker failure");
        }
        if !state.active {
            return Err(transaction_finished());
        }
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            rollback(&mut state)?;
            return Err(deadline_error());
        }
        state
            .connection
            .busy_timeout(remaining(deadline))
            .map_err(|error| operation_error(&error, "failed to configure SQLite transaction"))?;
        let result = operation(&mut state);
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            rollback(&mut state)?;
            return Err(deadline_error());
        }
        result
    });

    let result = match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        &mut worker,
    )
    .await
    {
        Ok(joined) => joined.map_err(|_| {
            owner.metrics.record_blocking_failure();
            worker_error()
        })?,
        Err(_) => {
            cancel_guard.cancel();
            let _ = worker.await.map_err(|_| worker_error())?;
            tokio::task::spawn_blocking(move || {
                let mut state = timeout_state.lock().map_err(|_| internal_error())?;
                rollback(&mut state)
            })
            .await
            .map_err(|_| worker_error())??;
            Err(deadline_error())
        }
    };
    cancel_guard.disarm();
    result
}

struct CancelOnDrop {
    cancelled: Arc<AtomicBool>,
    interrupt_handle: Arc<InterruptHandle>,
    armed: bool,
}

impl CancelOnDrop {
    fn new(owner: &TxnOwner) -> Self {
        Self {
            cancelled: Arc::clone(&owner.cancelled),
            interrupt_handle: Arc::clone(&owner.interrupt_handle),
            armed: true,
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.interrupt_handle.interrupt();
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancel();
        }
    }
}

fn schedule_rollback(owner: TxnOwner) {
    owner.cancelled.store(true, Ordering::Release);
    owner.interrupt_handle.interrupt();
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn_blocking(move || {
            if let Ok(mut state) = owner.state.lock() {
                let _ = rollback(&mut state);
            }
        });
    }
}

fn validate_key_value(
    key: &Key,
    value: Option<&Value>,
    limits: &StateStoreLimits,
) -> Result<(), StateStoreError> {
    if key.as_bytes().len() > limits.max_key_bytes {
        return Err(limit_error("key exceeds the configured byte limit"));
    }
    if value.is_some_and(|value| value.as_bytes().len() > limits.max_value_bytes) {
        return Err(limit_error("value exceeds the configured byte limit"));
    }
    Ok(())
}

fn validate_transaction_envelope(limits: &StateStoreLimits) -> Result<(), StateStoreError> {
    let accounted_bytes = checked_accounting_add(0, TRANSACTION_ENVELOPE_BYTES)?;
    if accounted_bytes > limits.max_transaction_bytes {
        return Err(limit_error("transaction byte limit exceeded"));
    }
    Ok(())
}

fn validate_range_request(
    request: &RangeRequest,
    limits: &StateStoreLimits,
) -> Result<(), StateStoreError> {
    request.validate(limits)?;
    validate_key_value(&request.range.start, None, limits)?;
    validate_key_value(&request.range.end, None, limits)?;
    if let Some(continuation) = &request.continuation {
        let last_key = continuation.resume_after(request)?;
        validate_key_value(&last_key, None, limits)?;
    }
    Ok(())
}

fn stage_mutation(
    state: &mut SqliteTxnState,
    key: Key,
    mutation: Mutation,
    next_operations: usize,
) -> Result<(), StateStoreError> {
    if state.range_frozen {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            "writes are frozen after paginated range reads",
        ));
    }
    let mutation_bytes = accounted_mutation_bytes(&key, &mutation)?;
    let next_bytes = state
        .accounted_bytes
        .checked_add(mutation_bytes)
        .ok_or_else(|| limit_error("transaction byte limit exceeded"))?;
    if next_bytes > state.limits.max_transaction_bytes {
        return Err(limit_error("transaction byte limit exceeded"));
    }

    state.operation_count = next_operations;
    state.accounted_bytes = next_bytes;
    state.overlay.insert(key.clone(), mutation.clone());
    state.mutations.push((key, mutation));
    Ok(())
}

fn next_operation_count(state: &SqliteTxnState) -> Result<usize, StateStoreError> {
    let next_operations = state
        .operation_count
        .checked_add(1)
        .ok_or_else(|| limit_error("transaction operation limit exceeded"))?;
    if next_operations > state.limits.max_transaction_operations {
        return Err(limit_error("transaction operation limit exceeded"));
    }
    Ok(next_operations)
}

fn accounted_mutation_bytes(key: &Key, mutation: &Mutation) -> Result<usize, StateStoreError> {
    let mut bytes = MUTATION_KIND_BYTES;
    bytes = checked_accounting_add(bytes, key.as_bytes().len())?;
    bytes = checked_accounting_add(bytes, PRECONDITION_KIND_BYTES)?;
    bytes = checked_accounting_add(bytes, precondition_bytes(mutation_precondition(mutation)))?;
    bytes = checked_accounting_add(bytes, key.as_bytes().len())?;
    bytes = checked_accounting_add(bytes, CHANGE_REVISION_BYTES)?;
    bytes = checked_accounting_add(bytes, CHANGE_SEQUENCE_BYTES)?;
    match mutation {
        Mutation::Put {
            value,
            provisional_version,
            ..
        } => {
            bytes = checked_accounting_add(bytes, value.as_bytes().len())?;
            bytes = checked_accounting_add(bytes, provisional_version.as_bytes().len())?;
            bytes = checked_accounting_add(bytes, PERSISTED_VERSION_BYTES)?;
        }
        Mutation::Delete { .. } => {}
    }
    Ok(bytes)
}

fn checked_accounting_add(total: usize, bytes: usize) -> Result<usize, StateStoreError> {
    total
        .checked_add(bytes)
        .ok_or_else(|| limit_error("transaction byte limit exceeded"))
}

fn precondition_bytes(precondition: &Precondition) -> usize {
    match precondition {
        Precondition::Version(version) => version.as_bytes().len(),
        _ => 0,
    }
}

fn record_result<T>(
    metrics: &StateStoreMetrics,
    operation: StateStoreOperation,
    started: Instant,
    result: &Result<T, StateStoreError>,
) {
    metrics.record_operation(
        operation,
        if result.is_ok() {
            StateStoreOutcome::Success
        } else {
            StateStoreOutcome::Error
        },
        started.elapsed(),
    );
}

fn record_read_result(
    metrics: &StateStoreMetrics,
    operation: StateStoreOperation,
    started: Instant,
    result: &Result<Option<StateRecord>, StateStoreError>,
) {
    record_result(metrics, operation, started, result);
    if let Ok(Some(record)) = result {
        let bytes = record
            .key
            .as_bytes()
            .len()
            .saturating_add(record.value.as_bytes().len())
            .saturating_add(record.version.as_bytes().len());
        metrics.record_bytes_read(u64::try_from(bytes).unwrap_or(u64::MAX));
    }
}

fn record_range_result(
    metrics: &StateStoreMetrics,
    started: Instant,
    result: &Result<RangePage, StateStoreError>,
) {
    record_result(metrics, StateStoreOperation::Range, started, result);
    if let Ok(page) = result {
        metrics.record_page_records(page.records.len() as u64);
        let bytes = page.records.iter().fold(0_usize, |total, record| {
            total
                .saturating_add(record.key.as_bytes().len())
                .saturating_add(record.value.as_bytes().len())
                .saturating_add(record.version.as_bytes().len())
        });
        metrics.record_bytes_read(u64::try_from(bytes).unwrap_or(u64::MAX));
    }
}

fn record_write_result(
    metrics: &StateStoreMetrics,
    operation: StateStoreOperation,
    started: Instant,
    result: &Result<usize, StateStoreError>,
) {
    record_result(metrics, operation, started, result);
    if let Ok(bytes) = result {
        metrics.record_bytes_written(u64::try_from(*bytes).unwrap_or(u64::MAX));
    }
}

fn commit_metric_outcome(outcome: &CommitOutcome) -> StateStoreOutcome {
    match outcome {
        CommitOutcome::Committed(_) => StateStoreOutcome::Success,
        CommitOutcome::Conflict(_) => StateStoreOutcome::Conflict,
        CommitOutcome::TransientBeforeCommit(_) => StateStoreOutcome::TransientBeforeCommit,
        CommitOutcome::DefiniteFailure(_) => StateStoreOutcome::DefiniteFailure,
        CommitOutcome::CommitUnknown(_) => StateStoreOutcome::CommitUnknown,
    }
}

fn commit_blocking(
    state: &mut SqliteTxnState,
    transaction_id: TransactionId,
    path: &Path,
    history_retention: &SqliteHistoryRetentionConfig,
) -> CommitOutcome {
    if !state.active {
        return CommitOutcome::DefiniteFailure(transaction_finished());
    }
    if state.cancelled.load(Ordering::Acquire) || Instant::now() >= state.deadline {
        return rollback_outcome(state, CommitOutcome::DefiniteFailure(deadline_error()));
    }

    if let Err(error) = state.connection.busy_timeout(remaining(state.deadline)) {
        return rollback_outcome(
            state,
            CommitOutcome::TransientBeforeCommit(operation_error(
                &error,
                "failed to configure SQLite commit",
            )),
        );
    }

    match lookup_commit_on_connection(&state.connection, transaction_id) {
        Ok(Some(receipt)) => {
            return authoritative_committed_outcome(state, receipt);
        }
        Ok(None) => {}
        Err(error) => {
            return rollback_outcome(state, classify_precommit_error(error));
        }
    }
    let transaction_id_bytes = *transaction_id.as_uuid().as_bytes();
    match history::transaction_id_is_retired(&state.connection, &transaction_id_bytes) {
        Ok(true) => {
            return rollback_outcome(
                state,
                CommitOutcome::DefiniteFailure(retired_transaction_error()),
            );
        }
        Ok(false) => {}
        Err(error) => return rollback_outcome(state, classify_precommit_error(error)),
    }

    let current_revision = match load_current_revision(&state.connection) {
        Ok(revision) => revision,
        Err(error) => {
            return rollback_outcome(state, classify_precommit_error(error));
        }
    };
    state.snapshot_established = true;
    let revision = match current_revision.checked_add(1) {
        Some(revision) if i64::try_from(revision).is_ok() => revision,
        _ => {
            return rollback_outcome(
                state,
                CommitOutcome::DefiniteFailure(StateStoreError::new(
                    StateStoreErrorKind::Corruption,
                    "SQLite state store revision is exhausted",
                )),
            );
        }
    };

    let mutations = state.mutations.clone();
    let mut changed_keys = Vec::new();
    let mut seen_changed_keys = HashSet::new();
    let mut logical_versions = HashMap::<Key, Option<VersionToken>>::new();
    for (key, mutation) in mutations {
        if state.cancelled.load(Ordering::Acquire) || Instant::now() >= state.deadline {
            return rollback_outcome(state, CommitOutcome::DefiniteFailure(deadline_error()));
        }
        if !logical_versions.contains_key(&key) {
            let existing_version = match load_version(&state.connection, &key) {
                Ok(version) => version.map(revision_version),
                Err(error) => {
                    return rollback_outcome(state, classify_precommit_error(error));
                }
            };
            logical_versions.insert(key.clone(), existing_version);
        }
        let existing_version = logical_versions.get(&key).and_then(Option::as_ref);
        if !precondition_matches(mutation_precondition(&mutation), existing_version) {
            return rollback_outcome(
                state,
                CommitOutcome::Conflict(StateStoreError::new(
                    StateStoreErrorKind::PreconditionFailed,
                    "SQLite transaction precondition failed",
                )),
            );
        }

        let apply_result = apply_mutation_with_busy_retry(
            &state.connection,
            &key,
            &mutation,
            revision,
            state.deadline,
            &state.cancelled,
        );
        let changed = match apply_result {
            Ok(changed) => changed,
            Err(outcome) => return rollback_outcome(state, outcome),
        };
        if changed && seen_changed_keys.insert(key.clone()) {
            changed_keys.push(key.clone());
        }
        match &mutation {
            Mutation::Put {
                provisional_version,
                ..
            } => {
                logical_versions.insert(key, Some(provisional_version.clone()));
            }
            Mutation::Delete { .. } => {
                logical_versions.insert(key, None);
            }
        }
    }

    let revision_i64 = i64::try_from(revision).expect("revision checked above");
    let committed_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    for (sequence, key) in changed_keys.iter().enumerate() {
        if let Err(error) = state.connection.execute(
            "INSERT INTO state_store_changes(revision, sequence, key, committed_at_ms) VALUES (?1, ?2, ?3, ?4)",
            params![revision_i64, sequence as i64, key.as_bytes(), committed_at_ms],
        ) {
            let outcome = classify_apply_error(&error);
            return rollback_outcome(state, outcome);
        }
    }

    if let Err(error) = state.connection.execute(
        "INSERT INTO state_store_commits(transaction_id, revision, committed_at_ms) VALUES (?1, ?2, ?3)",
        params![transaction_id.as_uuid().as_bytes(), revision_i64, committed_at_ms],
    ) {
        let outcome = classify_apply_error(&error);
        return rollback_outcome(state, outcome);
    }
    match state.connection.execute(
        "UPDATE state_store_meta SET value = ?1 WHERE key = ?2",
        params![
            revision.to_be_bytes().as_slice(),
            schema::CURRENT_REVISION_KEY
        ],
    ) {
        Ok(1) => {}
        Ok(_) => {
            return rollback_outcome(
                state,
                CommitOutcome::DefiniteFailure(StateStoreError::new(
                    StateStoreErrorKind::Corruption,
                    "SQLite current revision metadata is missing",
                )),
            );
        }
        Err(error) => {
            let outcome = classify_apply_error(&error);
            return rollback_outcome(state, outcome);
        }
    }

    let reclaim_pending = match history::maintain_after_commit(
        &state.connection,
        history_retention,
        revision,
        committed_at_ms,
    ) {
        Ok(reclaim_pending) => reclaim_pending,
        Err(error) => return rollback_outcome(state, classify_precommit_error(error)),
    };

    match state.connection.execute_batch("COMMIT") {
        Ok(()) => {
            state.active = false;
            if let Err(error) =
                history::reclaim_after_commit(&state.connection, history_retention, reclaim_pending)
            {
                let _ = error;
            }
            CommitOutcome::Committed(CommitReceipt {
                transaction_id,
                revision: revision_token(revision),
            })
        }
        Err(error) => classify_commit_error(state, transaction_id, path, &error),
    }
}

fn apply_mutation(
    connection: &Connection,
    key: &Key,
    mutation: &Mutation,
    revision: u64,
) -> rusqlite::Result<bool> {
    let revision = i64::try_from(revision).expect("revision checked before apply");
    match mutation {
        Mutation::Put { value, .. } => {
            connection.execute(
                "INSERT INTO state_store_kv(key, value, version) VALUES (?1, ?2, ?3) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value, version = excluded.version",
                params![key.as_bytes(), value.as_bytes(), revision],
            )?;
            Ok(true)
        }
        Mutation::Delete { .. } => Ok(connection.execute(
            "DELETE FROM state_store_kv WHERE key = ?1",
            params![key.as_bytes()],
        )? > 0),
    }
}

fn apply_mutation_with_busy_retry(
    connection: &Connection,
    key: &Key,
    mutation: &Mutation,
    revision: u64,
    transaction_deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<bool, CommitOutcome> {
    let retry_deadline = transaction_deadline.min(Instant::now() + SQLITE_BUSY_RETRY_LIMIT);
    loop {
        match apply_mutation(connection, key, mutation, revision) {
            Ok(changed) => return Ok(changed),
            Err(error) if is_base_busy(&error) => {
                if cancelled.load(Ordering::Acquire) || Instant::now() >= transaction_deadline {
                    return Err(CommitOutcome::DefiniteFailure(deadline_error()));
                }
                if Instant::now() >= retry_deadline {
                    return Err(CommitOutcome::TransientBeforeCommit(operation_error(
                        &error,
                        "SQLite transaction remained busy before commit",
                    )));
                }
                std::thread::sleep(
                    SQLITE_BUSY_RETRY_DELAY
                        .min(retry_deadline.saturating_duration_since(Instant::now())),
                );
            }
            Err(error) => return Err(classify_apply_error(&error)),
        }
    }
}

fn mutation_precondition(mutation: &Mutation) -> &Precondition {
    match mutation {
        Mutation::Put { precondition, .. } | Mutation::Delete { precondition } => precondition,
    }
}

fn precondition_matches(
    precondition: &Precondition,
    existing_version: Option<&VersionToken>,
) -> bool {
    match precondition {
        Precondition::Any => true,
        Precondition::Absent => existing_version.is_none(),
        Precondition::Present => existing_version.is_some(),
        Precondition::Version(expected) => existing_version == Some(expected),
    }
}

fn load_record(connection: &Connection, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
    let row = connection
        .query_row(
            "SELECT value, version FROM state_store_kv WHERE key = ?1",
            params![key.as_bytes()],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|error| persisted_row_error(&error, "failed to read SQLite state record"))?;
    row.map(|(value, version)| {
        let version = u64::try_from(version).map_err(|_| corruption_error())?;
        Ok(StateRecord {
            key: key.clone(),
            value: persisted_value(value)?,
            version: revision_version(version),
        })
    })
    .transpose()
}

fn load_version(connection: &Connection, key: &Key) -> Result<Option<u64>, StateStoreError> {
    connection
        .query_row(
            "SELECT version FROM state_store_kv WHERE key = ?1",
            params![key.as_bytes()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|error| operation_error(&error, "failed to validate SQLite precondition"))?
        .map(|version| u64::try_from(version).map_err(|_| corruption_error()))
        .transpose()
}

pub(super) fn load_current_revision(connection: &Connection) -> Result<u64, StateStoreError> {
    let value = connection
        .query_row(
            "SELECT value FROM state_store_meta WHERE key = ?1",
            params![schema::CURRENT_REVISION_KEY],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map_err(|error| operation_error(&error, "failed to read SQLite store revision"))?;
    let bytes: [u8; 8] = value.try_into().map_err(|_| corruption_error())?;
    Ok(u64::from_be_bytes(bytes))
}

fn classify_apply_error(error: &rusqlite::Error) -> CommitOutcome {
    if is_busy_snapshot(error) {
        return CommitOutcome::Conflict(StateStoreError::new(
            StateStoreErrorKind::Conflict,
            "SQLite transaction snapshot conflicted",
        ));
    }
    match error.sqlite_error_code() {
        Some(ffi::ErrorCode::DatabaseBusy | ffi::ErrorCode::DatabaseLocked) => {
            CommitOutcome::TransientBeforeCommit(operation_error(
                error,
                "SQLite transaction was busy before commit",
            ))
        }
        _ => CommitOutcome::DefiniteFailure(operation_error(
            error,
            "SQLite transaction failed before commit",
        )),
    }
}

fn classify_commit_error(
    state: &mut SqliteTxnState,
    transaction_id: TransactionId,
    path: &Path,
    error: &rusqlite::Error,
) -> CommitOutcome {
    let mapped = operation_error(error, "SQLite transaction commit failed");
    if !state.connection.is_autocommit() {
        if rollback(state).is_ok() {
            return if mapped.kind() == StateStoreErrorKind::Transient {
                CommitOutcome::TransientBeforeCommit(mapped)
            } else {
                CommitOutcome::DefiniteFailure(mapped)
            };
        }
        return CommitOutcome::CommitUnknown(mapped);
    }
    state.active = false;
    match lookup_commit(path, transaction_id) {
        Ok(Some(receipt)) => CommitOutcome::Committed(receipt),
        Ok(None) => CommitOutcome::DefiniteFailure(mapped),
        Err(_) => CommitOutcome::CommitUnknown(mapped),
    }
}

fn rollback(state: &mut SqliteTxnState) -> Result<(), StateStoreError> {
    if !state.active {
        return Ok(());
    }
    state
        .connection
        .execute_batch("ROLLBACK")
        .map_err(|error| operation_error(&error, "failed to roll back SQLite transaction"))?;
    state.active = false;
    Ok(())
}

fn rollback_outcome(state: &mut SqliteTxnState, outcome: CommitOutcome) -> CommitOutcome {
    match rollback(state) {
        Ok(()) => outcome,
        Err(error) => CommitOutcome::CommitUnknown(error),
    }
}

fn authoritative_committed_outcome(
    state: &mut SqliteTxnState,
    receipt: CommitReceipt,
) -> CommitOutcome {
    let _ = rollback(state);
    state.active = false;
    CommitOutcome::Committed(receipt)
}

enum RegisterOutcome {
    Registered,
    AlreadyCommitted(CommitReceipt),
    InFlight,
    NotCommitted,
}

fn register_inflight(
    registry: &CommitRegistry,
    transaction_id: TransactionId,
) -> Result<RegisterOutcome, StateStoreError> {
    let mut registry = lock_registry(registry)?;
    match registry.get(&transaction_id) {
        Some(CommitRegistryState::Committed(receipt)) => {
            Ok(RegisterOutcome::AlreadyCommitted(receipt.clone()))
        }
        Some(CommitRegistryState::InFlight) => Ok(RegisterOutcome::InFlight),
        Some(CommitRegistryState::NotCommitted) => Ok(RegisterOutcome::NotCommitted),
        None => {
            registry.insert(transaction_id, CommitRegistryState::InFlight);
            Ok(RegisterOutcome::Registered)
        }
    }
}

fn finalize_registry(
    registry: &CommitRegistry,
    transaction_id: TransactionId,
    outcome: &CommitOutcome,
) {
    if let Ok(mut registry) = registry.lock() {
        match outcome {
            CommitOutcome::Committed(receipt) => {
                registry.insert(
                    transaction_id,
                    CommitRegistryState::Committed(receipt.clone()),
                );
            }
            CommitOutcome::Conflict(_)
            | CommitOutcome::TransientBeforeCommit(_)
            | CommitOutcome::DefiniteFailure(_) => {
                registry.insert(transaction_id, CommitRegistryState::NotCommitted);
            }
            CommitOutcome::CommitUnknown(_) => {
                registry.remove(&transaction_id);
            }
        }
    }
}

fn lookup_commit(
    path: &Path,
    transaction_id: TransactionId,
) -> Result<Option<CommitReceipt>, StateStoreError> {
    let connection = open_connection(path)?;
    lookup_commit_on_connection(&connection, transaction_id)
}

fn lookup_commit_resolution(
    path: &Path,
    transaction_id: TransactionId,
) -> Result<CommitResolution, StateStoreError> {
    let connection = open_connection(path)?;
    match lookup_commit_on_connection(&connection, transaction_id)? {
        Some(receipt) => Ok(CommitResolution::Committed(receipt)),
        None if history::transaction_id_is_retired(
            &connection,
            transaction_id.as_uuid().as_bytes(),
        )? =>
        {
            Ok(CommitResolution::Unresolved)
        }
        None => Ok(CommitResolution::NotCommitted),
    }
}

fn lookup_commit_on_connection(
    connection: &Connection,
    transaction_id: TransactionId,
) -> Result<Option<CommitReceipt>, StateStoreError> {
    let revision = connection
        .query_row(
            "SELECT revision FROM state_store_commits WHERE transaction_id = ?1",
            params![transaction_id.as_uuid().as_bytes()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|error| operation_error(&error, "failed to resolve SQLite commit"))?;
    revision
        .map(|revision| {
            let revision = u64::try_from(revision).map_err(|_| corruption_error())?;
            Ok(CommitReceipt {
                transaction_id,
                revision: revision_token(revision),
            })
        })
        .transpose()
}

fn classify_precommit_error(error: StateStoreError) -> CommitOutcome {
    match error.kind() {
        StateStoreErrorKind::Transient
        | StateStoreErrorKind::ProviderUnavailable
        | StateStoreErrorKind::DeadlineExceeded => CommitOutcome::TransientBeforeCommit(error),
        _ => CommitOutcome::DefiniteFailure(error),
    }
}

fn lock_registry(
    registry: &CommitRegistry,
) -> Result<std::sync::MutexGuard<'_, HashMap<TransactionId, CommitRegistryState>>, StateStoreError>
{
    registry.lock().map_err(|_| internal_error())
}

fn registry_resolution(state: &CommitRegistryState) -> CommitResolution {
    match state {
        CommitRegistryState::InFlight => CommitResolution::Unresolved,
        CommitRegistryState::Committed(receipt) => CommitResolution::Committed(receipt.clone()),
        CommitRegistryState::NotCommitted => CommitResolution::NotCommitted,
    }
}

const fn retired_transaction_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "SQLite transaction id is within retired history bounds",
    )
}

fn is_busy_snapshot(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(error, _) if error.extended_code == SQLITE_BUSY_SNAPSHOT
    )
}

fn is_base_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(error, _) if error.extended_code == ffi::SQLITE_BUSY
    )
}

pub(super) fn operation_error(error: &rusqlite::Error, message: &'static str) -> StateStoreError {
    let kind = match error.sqlite_error_code() {
        Some(ffi::ErrorCode::OperationInterrupted) => StateStoreErrorKind::Cancelled,
        Some(ffi::ErrorCode::DatabaseBusy | ffi::ErrorCode::DatabaseLocked) => {
            StateStoreErrorKind::Transient
        }
        Some(ffi::ErrorCode::DatabaseCorrupt | ffi::ErrorCode::NotADatabase) => {
            StateStoreErrorKind::Corruption
        }
        Some(
            ffi::ErrorCode::CannotOpen
            | ffi::ErrorCode::SystemIoFailure
            | ffi::ErrorCode::ReadOnly
            | ffi::ErrorCode::DiskFull
            | ffi::ErrorCode::PermissionDenied,
        ) => StateStoreErrorKind::ProviderUnavailable,
        _ => StateStoreErrorKind::Internal,
    };
    StateStoreError::new(kind, message)
}

pub(super) fn persisted_row_error(
    error: &rusqlite::Error,
    message: &'static str,
) -> StateStoreError {
    if matches!(
        error,
        rusqlite::Error::InvalidColumnType(..)
            | rusqlite::Error::FromSqlConversionFailure(..)
            | rusqlite::Error::IntegralValueOutOfRange(..)
    ) {
        return persisted_corruption();
    }
    operation_error(error, message)
}

pub(super) fn persisted_key(bytes: Vec<u8>) -> Result<Key, StateStoreError> {
    Key::try_from(Bytes::from(bytes)).map_err(|_| persisted_corruption())
}

pub(super) fn persisted_value(bytes: Vec<u8>) -> Result<Value, StateStoreError> {
    Value::try_from(Bytes::from(bytes)).map_err(|_| persisted_corruption())
}

pub(super) fn revision_token(revision: u64) -> StoreRevision {
    StoreRevision::try_from(Bytes::copy_from_slice(&revision.to_be_bytes()))
        .expect("u64 revision is non-empty")
}

pub(super) fn revision_version(revision: u64) -> VersionToken {
    VersionToken::try_from(Bytes::copy_from_slice(&revision.to_be_bytes()))
        .expect("u64 version is non-empty")
}

fn provisional_version(transaction_id: TransactionId, operation: usize) -> VersionToken {
    let mut bytes = Vec::with_capacity(PROVISIONAL_VERSION_TAG.len() + 16 + 8);
    bytes.extend_from_slice(PROVISIONAL_VERSION_TAG);
    bytes.extend_from_slice(transaction_id.as_uuid().as_bytes());
    bytes.extend_from_slice(&(operation as u64).to_be_bytes());
    VersionToken::try_from(Bytes::from(bytes)).expect("provisional version is non-empty")
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

const fn transaction_finished() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Cancelled,
        "SQLite transaction is no longer active",
    )
}

const fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "SQLite transaction deadline exceeded",
    )
}

const fn worker_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Internal,
        "SQLite transaction blocking worker failed",
    )
}

const fn internal_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Internal,
        "SQLite transaction state is unavailable",
    )
}

const fn corruption_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "SQLite state store revision is malformed",
    )
}

const fn persisted_corruption() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "SQLite persisted state record is malformed",
    )
}

const fn limit_error(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::LimitExceeded, message)
}

#[async_trait]
impl ReadTransaction for SqliteReadTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        SqliteReadTransaction::get(self, key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        SqliteReadTransaction::range(self, request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        SqliteReadTransaction::abort(*self).await
    }
}

#[async_trait]
impl ReadTransaction for SqliteWriteTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        SqliteWriteTransaction::get(self, key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        SqliteWriteTransaction::range(self, request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        SqliteWriteTransaction::abort(*self).await
    }
}

#[async_trait]
impl WriteTransaction for SqliteWriteTransaction {
    fn transaction_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        SqliteWriteTransaction::put(self, key, value, precondition).await
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        SqliteWriteTransaction::delete(self, key, precondition).await
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        SqliteWriteTransaction::commit(*self).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::TempDir;
    use tokio::sync::Barrier;
    use uuid::Uuid;

    use super::super::{SqliteHistoryRetentionConfig, SqliteStateStore};
    use super::*;
    use novarocks_spi::state_store::{
        CommitOutcome, CommitReceipt, CommitResolution, Direction, Key, KeyRange, Precondition,
        RangeRequest, StateRecord, StateStoreErrorKind, StateStoreOpenRequest, TransactionId,
        Value, VersionToken,
    };

    #[derive(Default)]
    struct StateStoreLimitOverrides {
        max_key_bytes: Option<usize>,
        max_transaction_bytes: Option<usize>,
        max_transaction_operations: Option<usize>,
        transaction_deadline_ms: Option<u64>,
    }

    fn resolve_state_store_limits(limits: &StateStoreLimitOverrides) -> StateStoreLimits {
        let mut resolved = StateStoreLimits::default();
        if let Some(value) = limits.max_key_bytes {
            resolved.max_key_bytes = value;
        }
        if let Some(value) = limits.max_transaction_bytes {
            resolved.max_transaction_bytes = value;
        }
        if let Some(value) = limits.max_transaction_operations {
            resolved.max_transaction_operations = value;
        }
        if let Some(value) = limits.transaction_deadline_ms {
            resolved.transaction_deadline = std::time::Duration::from_millis(value);
        }
        resolved
    }

    fn key(value: &'static [u8]) -> Key {
        Key::try_from(Bytes::from_static(value)).expect("valid key")
    }

    fn value(value: &'static [u8]) -> Value {
        Value::try_from(Bytes::from_static(value)).expect("valid value")
    }

    fn transaction_id() -> TransactionId {
        Uuid::now_v7().into()
    }

    async fn store(temp: &TempDir) -> Arc<SqliteStateStore> {
        store_with_limits(temp, StateStoreLimitOverrides::default()).await
    }

    async fn store_with_limits(
        temp: &TempDir,
        limits: StateStoreLimitOverrides,
    ) -> Arc<SqliteStateStore> {
        Arc::new(
            SqliteStateStore::open(
                temp.path().join("state-store.sqlite"),
                super::super::SqliteHistoryRetentionConfig::default(),
                StateStoreOpenRequest {
                    cluster_id: "cluster-a".to_owned(),
                    limits: resolve_state_store_limits(&limits),
                    deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
                },
            )
            .await
            .expect("open SQLite store"),
        )
    }

    async fn store_with_policy(
        temp: &TempDir,
        limits: StateStoreLimitOverrides,
        policy: SqliteHistoryRetentionConfig,
    ) -> Arc<SqliteStateStore> {
        Arc::new(
            SqliteStateStore::open(
                temp.path().join("state-store.sqlite"),
                policy,
                StateStoreOpenRequest {
                    cluster_id: "cluster-a".to_owned(),
                    limits: resolve_state_store_limits(&limits),
                    deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
                },
            )
            .await
            .expect("open SQLite store"),
        )
    }

    async fn durable_counts(store: &SqliteStateStore) -> (u64, i64, i64) {
        let path = store.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path).expect("inspection connection");
            let revision = load_current_revision(&connection).expect("current revision");
            let changes = connection
                .query_row("SELECT COUNT(*) FROM state_store_changes", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("change count");
            let commits = connection
                .query_row("SELECT COUNT(*) FROM state_store_commits", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("commit count");
            (revision, changes, commits)
        })
        .await
        .expect("inspection worker")
    }

    async fn durable_state_counts(store: &SqliteStateStore) -> (u64, i64, i64, i64) {
        let path = store.path.clone();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(&path).expect("inspection connection");
            let revision = load_current_revision(&connection).expect("current revision");
            let kv = connection
                .query_row("SELECT COUNT(*) FROM state_store_kv", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("KV count");
            let changes = connection
                .query_row("SELECT COUNT(*) FROM state_store_changes", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("change count");
            let commits = connection
                .query_row("SELECT COUNT(*) FROM state_store_commits", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("commit count");
            (revision, kv, changes, commits)
        })
        .await
        .expect("inspection worker")
    }

    fn committed(outcome: CommitOutcome) -> CommitReceipt {
        match outcome {
            CommitOutcome::Committed(receipt) => receipt,
            other => panic!("expected committed outcome, got {other:?}"),
        }
    }

    fn assert_conflict(outcome: CommitOutcome) {
        match outcome {
            CommitOutcome::Conflict(error) => assert!(matches!(
                error.kind(),
                StateStoreErrorKind::Conflict | StateStoreErrorKind::PreconditionFailed
            )),
            other => panic!("expected conflict outcome, got {other:?}"),
        }
    }

    async fn put_committed(store: &SqliteStateStore, key: Key, value: Value) -> CommitReceipt {
        let mut transaction = store
            .begin_write(transaction_id())
            .await
            .expect("begin write");
        transaction
            .put(key, value, Precondition::Any)
            .await
            .expect("stage put");
        committed(transaction.commit().await)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_metrics_record_deterministic_blocking_worker_failure() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let mut reader = store.begin_read().await.expect("begin worker-failure read");
        let before = store.metrics.snapshot();
        store
            .test_hooks
            .fail_next_operation_worker
            .store(true, Ordering::Release);

        let error = reader
            .get(&key(b"worker-failure"))
            .await
            .expect_err("injected blocking worker must fail");

        assert_eq!(error.kind(), StateStoreErrorKind::Internal);
        let after = store.metrics.snapshot();
        assert_eq!(
            after.blocking_failure_count,
            before.blocking_failure_count + 1
        );
        assert_eq!(
            after.operation_outcome_count(StateStoreOperation::Get, StateStoreOutcome::Error),
            before.operation_outcome_count(StateStoreOperation::Get, StateStoreOutcome::Error) + 1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_exact_byte_accounting_accepts_boundary_and_rejects_overage() {
        let item = key(b"exact-budget");
        let id = transaction_id();
        let exact_mutation = Mutation::Put {
            value: Value::try_from(Bytes::from(vec![7; 16])).expect("budget value"),
            precondition: Precondition::Any,
            provisional_version: provisional_version(id, 1),
        };
        let exact_budget = TRANSACTION_ENVELOPE_BYTES
            .checked_add(accounted_mutation_bytes(&item, &exact_mutation).expect("exact bytes"))
            .expect("exact budget");

        for (value_bytes, should_succeed, label) in [
            (15_usize, true, "boundary minus one"),
            (16_usize, true, "boundary"),
            (17_usize, false, "boundary plus one"),
        ] {
            let temp = TempDir::new().expect("budget temp dir");
            let store = store_with_limits(
                &temp,
                StateStoreLimitOverrides {
                    max_transaction_bytes: Some(exact_budget),
                    ..StateStoreLimitOverrides::default()
                },
            )
            .await;
            let mut transaction = store.begin_write(id).await.expect("begin budget write");
            let result = transaction
                .put(
                    item.clone(),
                    Value::try_from(Bytes::from(vec![7; value_bytes])).expect("budget value"),
                    Precondition::Any,
                )
                .await;
            if should_succeed {
                result.unwrap_or_else(|error| panic!("{label} must fit: {error}"));
            } else {
                assert_eq!(
                    result.expect_err("over-budget mutation must fail").kind(),
                    StateStoreErrorKind::LimitExceeded
                );
                assert_eq!(durable_counts(&store).await, (0, 0, 0));
            }
            transaction.abort().await.expect("abort budget transaction");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_envelope_is_checked_before_provider_io() {
        let under_budget = TempDir::new().expect("under-budget temp dir");
        let under_budget_store = store_with_limits(
            &under_budget,
            StateStoreLimitOverrides {
                max_transaction_bytes: Some(TRANSACTION_ENVELOPE_BYTES - 1),
                ..StateStoreLimitOverrides::default()
            },
        )
        .await;
        let durable_before = durable_state_counts(&under_budget_store).await;
        let metrics_before = under_budget_store.metrics.snapshot();
        let error = match under_budget_store.begin_write(transaction_id()).await {
            Ok(_) => panic!("transaction envelope must fit before provider I/O"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StateStoreErrorKind::LimitExceeded);
        let metrics_after = under_budget_store.metrics.snapshot();
        assert_eq!(
            metrics_after.begin_count,
            metrics_before.begin_count + 1,
            "rejected envelope must count one begin attempt"
        );
        assert_eq!(
            metrics_after
                .operation_outcome_count(StateStoreOperation::Begin, StateStoreOutcome::Error),
            metrics_before
                .operation_outcome_count(StateStoreOperation::Begin, StateStoreOutcome::Error)
                + 1
        );
        assert_eq!(
            metrics_after.operation_duration_observations(StateStoreOperation::Begin),
            metrics_before.operation_duration_observations(StateStoreOperation::Begin) + 1
        );
        assert_eq!(
            durable_state_counts(&under_budget_store).await,
            durable_before
        );

        let exact = TempDir::new().expect("exact-envelope temp dir");
        let exact_store = store_with_limits(
            &exact,
            StateStoreLimitOverrides {
                max_transaction_bytes: Some(TRANSACTION_ENVELOPE_BYTES),
                ..StateStoreLimitOverrides::default()
            },
        )
        .await;
        let exact_before = durable_state_counts(&exact_store).await;
        let exact_receipt = committed(
            exact_store
                .begin_write(transaction_id())
                .await
                .expect("exact envelope begins")
                .commit()
                .await,
        );
        let exact_after = durable_state_counts(&exact_store).await;
        assert_eq!(exact_after.0, exact_before.0 + 1);
        assert_eq!(exact_after.1, exact_before.1);
        assert_eq!(exact_after.2, exact_before.2);
        assert_eq!(exact_after.3, exact_before.3 + 1);
        assert_eq!(exact_receipt.revision, revision_token(exact_after.0));

        let one_byte = TempDir::new().expect("one-byte mutation budget temp dir");
        let one_byte_store = store_with_limits(
            &one_byte,
            StateStoreLimitOverrides {
                max_transaction_bytes: Some(TRANSACTION_ENVELOPE_BYTES + 1),
                ..StateStoreLimitOverrides::default()
            },
        )
        .await;
        let one_byte_before = durable_state_counts(&one_byte_store).await;
        let mut transaction = one_byte_store
            .begin_write(transaction_id())
            .await
            .expect("envelope plus one begins");
        assert_eq!(
            transaction
                .delete(key(b"x"), Precondition::Any)
                .await
                .expect_err("one byte cannot fund a delete mutation")
                .kind(),
            StateStoreErrorKind::LimitExceeded
        );
        transaction
            .abort()
            .await
            .expect("abort one-byte transaction");
        assert_eq!(durable_state_counts(&one_byte_store).await, one_byte_before);
    }

    async fn read_value(store: &SqliteStateStore, key: &Key) -> Option<StateRecord> {
        let mut transaction = store.begin_read().await.expect("begin read");
        let value = transaction.get(key).await.expect("read key");
        transaction.abort().await.expect("abort read");
        value
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_repeatable_point_read() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let item = key(b"repeatable");
        put_committed(&store, item.clone(), value(b"v1")).await;

        let mut reader = store.begin_read().await.expect("begin read");
        let first = reader
            .get(&item)
            .await
            .expect("first read")
            .expect("record");

        put_committed(&store, item.clone(), value(b"v2")).await;

        let second = reader
            .get(&item)
            .await
            .expect("second read")
            .expect("record");
        assert_eq!(first, second);
        reader.abort().await.expect("abort read");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_reads_own_ordered_mutations() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let item = key(b"overlay");
        let mut transaction = store
            .begin_write(transaction_id())
            .await
            .expect("begin write");

        transaction
            .put(item.clone(), value(b"v1"), Precondition::Absent)
            .await
            .expect("stage first put");
        let first_overlay = transaction
            .get(&item)
            .await
            .expect("read overlay")
            .expect("overlay record");
        assert_eq!(first_overlay.value, value(b"v1"));
        assert_ne!(
            first_overlay.version.as_bytes().len(),
            std::mem::size_of::<i64>(),
            "transaction-local versions must not collide with persisted revisions"
        );
        transaction
            .delete(item.clone(), Precondition::Version(first_overlay.version))
            .await
            .expect("stage delete");
        assert_eq!(transaction.get(&item).await.expect("read delete"), None);
        transaction
            .put(item.clone(), value(b"v2"), Precondition::Absent)
            .await
            .expect("stage second put");

        let receipt = committed(transaction.commit().await);
        assert_eq!(
            read_value(&store, &item)
                .await
                .expect("committed record")
                .value,
            value(b"v2")
        );

        let revision = u64::from_be_bytes(
            receipt
                .revision
                .as_bytes()
                .try_into()
                .expect("SQLite revision encoding"),
        );
        let path = store.path.clone();
        let transaction_id = receipt.transaction_id;
        let item_bytes = item.as_bytes().to_vec();
        let (kv_version, ledger_revision, change_count, current_revision) =
            tokio::task::spawn_blocking(move || {
                let connection = open_connection(&path).expect("inspection connection");
                let kv_version = connection
                    .query_row(
                        "SELECT version FROM state_store_kv WHERE key = ?1",
                        params![item_bytes],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("KV version");
                let ledger_revision = connection
                    .query_row(
                        "SELECT revision FROM state_store_commits WHERE transaction_id = ?1",
                        params![transaction_id.as_uuid().as_bytes()],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("ledger revision");
                let change_count = connection
                    .query_row(
                        "SELECT COUNT(*) FROM state_store_changes WHERE revision = ?1",
                        params![revision as i64],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("change rows");
                let current_revision = connection
                    .query_row(
                        "SELECT value FROM state_store_meta WHERE key = ?1",
                        params![schema::CURRENT_REVISION_KEY],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .expect("current revision");
                (kv_version, ledger_revision, change_count, current_revision)
            })
            .await
            .expect("inspection worker");
        assert_eq!(kv_version, revision as i64);
        assert_eq!(ledger_revision, revision as i64);
        assert_eq!(change_count, 1, "same-key changes must be deduplicated");
        assert_eq!(current_revision, revision.to_be_bytes());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_rolls_back_all_keys_on_precondition_failure() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let guarded = key(b"guarded");
        let partial = key(b"must-not-commit");
        put_committed(&store, guarded.clone(), value(b"original")).await;
        let durable_before = durable_counts(&store).await;

        let mut transaction = store
            .begin_write(transaction_id())
            .await
            .expect("begin write");
        transaction
            .put(partial.clone(), value(b"partial"), Precondition::Any)
            .await
            .expect("stage unguarded put");
        transaction
            .put(guarded.clone(), value(b"wrong"), Precondition::Absent)
            .await
            .expect("stage failing put");
        assert_conflict(transaction.commit().await);

        assert_eq!(read_value(&store, &partial).await, None);
        assert_eq!(
            read_value(&store, &guarded)
                .await
                .expect("guarded record")
                .value,
            value(b"original")
        );
        assert_eq!(
            durable_counts(&store).await,
            durable_before,
            "rollback must preserve revision, change rows, and commit ledger"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_enforces_all_preconditions() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let item = key(b"preconditions");

        let mut absent = store
            .begin_write(transaction_id())
            .await
            .expect("begin absent");
        absent
            .put(item.clone(), value(b"v1"), Precondition::Absent)
            .await
            .expect("stage absent put");
        committed(absent.commit().await);

        let mut present = store
            .begin_write(transaction_id())
            .await
            .expect("begin present");
        present
            .put(item.clone(), value(b"v2"), Precondition::Present)
            .await
            .expect("stage present put");
        committed(present.commit().await);

        let record = read_value(&store, &item).await.expect("versioned record");
        let mut versioned = store
            .begin_write(transaction_id())
            .await
            .expect("begin versioned");
        versioned
            .put(
                item.clone(),
                value(b"v3"),
                Precondition::Version(record.version),
            )
            .await
            .expect("stage versioned put");
        committed(versioned.commit().await);

        let mut stale = store
            .begin_write(transaction_id())
            .await
            .expect("begin stale");
        stale
            .delete(
                item.clone(),
                Precondition::Version(
                    VersionToken::try_from(Bytes::from_static(b"wrong-version"))
                        .expect("non-empty version"),
                ),
            )
            .await
            .expect("stage stale delete");
        assert_conflict(stale.commit().await);

        let mut missing = store
            .begin_write(transaction_id())
            .await
            .expect("begin missing");
        missing
            .delete(key(b"missing"), Precondition::Present)
            .await
            .expect("stage missing delete");
        assert_conflict(missing.commit().await);

        let mut any = store
            .begin_write(transaction_id())
            .await
            .expect("begin any");
        any.delete(item.clone(), Precondition::Any)
            .await
            .expect("stage any delete");
        committed(any.commit().await);
        assert_eq!(read_value(&store, &item).await, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_same_key_snapshot_conflict_has_one_winner() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let item = key(b"same-key");
        put_committed(&store, item.clone(), value(b"initial")).await;
        let barrier = Arc::new(Barrier::new(2));

        let writers = [value(b"writer-a"), value(b"writer-b")]
            .into_iter()
            .map(|next| {
                let store = Arc::clone(&store);
                let item = item.clone();
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    let mut transaction = store
                        .begin_write(transaction_id())
                        .await
                        .expect("begin concurrent write");
                    transaction
                        .get(&item)
                        .await
                        .expect("establish snapshot")
                        .expect("initial record");
                    barrier.wait().await;
                    transaction
                        .put(item, next, Precondition::Any)
                        .await
                        .expect("stage concurrent put");
                    transaction.commit().await
                })
            })
            .collect::<Vec<_>>();

        let mut committed_count = 0;
        let mut conflict_count = 0;
        for writer in writers {
            match writer.await.expect("writer task") {
                CommitOutcome::Committed(_) => committed_count += 1,
                CommitOutcome::Conflict(_) => conflict_count += 1,
                other => panic!("unexpected concurrent outcome: {other:?}"),
            }
        }
        assert_eq!((committed_count, conflict_count), (1, 1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_write_skew_snapshot_conflict_has_one_winner() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let left = key(b"doctor-left");
        let right = key(b"doctor-right");
        put_committed(&store, left.clone(), value(b"on-call")).await;
        put_committed(&store, right.clone(), value(b"on-call")).await;
        let barrier = Arc::new(Barrier::new(2));

        let writers = [left.clone(), right.clone()]
            .into_iter()
            .map(|delete_key| {
                let store = Arc::clone(&store);
                let left = left.clone();
                let right = right.clone();
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    let mut transaction = store
                        .begin_write(transaction_id())
                        .await
                        .expect("begin skew write");
                    transaction
                        .get(&left)
                        .await
                        .expect("read left")
                        .expect("left present");
                    transaction
                        .get(&right)
                        .await
                        .expect("read right")
                        .expect("right present");
                    barrier.wait().await;
                    transaction
                        .delete(delete_key, Precondition::Any)
                        .await
                        .expect("stage skew delete");
                    transaction.commit().await
                })
            })
            .collect::<Vec<_>>();

        let mut committed_count = 0;
        let mut conflict_count = 0;
        for writer in writers {
            match writer.await.expect("writer task") {
                CommitOutcome::Committed(_) => committed_count += 1,
                CommitOutcome::Conflict(_) => conflict_count += 1,
                other => panic!("unexpected skew outcome: {other:?}"),
            }
        }
        assert_eq!((committed_count, conflict_count), (1, 1));
        let survivors = usize::from(read_value(&store, &left).await.is_some())
            + usize::from(read_value(&store, &right).await.is_some());
        assert_eq!(survivors, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_resolves_inflight_committed_and_not_committed_ids() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let committed_id = transaction_id();
        let mut transaction = store
            .begin_write(committed_id)
            .await
            .expect("begin committed transaction");
        transaction
            .put(key(b"ledger"), value(b"value"), Precondition::Any)
            .await
            .expect("stage ledger put");
        let receipt = committed(transaction.commit().await);
        assert_eq!(
            store
                .resolve_commit(&committed_id)
                .await
                .expect("resolve registry commit"),
            CommitResolution::Committed(receipt.clone())
        );

        store
            .commit_registry
            .lock()
            .expect("commit registry")
            .remove(&committed_id);
        assert_eq!(
            store
                .resolve_commit(&committed_id)
                .await
                .expect("resolve ledger commit"),
            CommitResolution::Committed(receipt)
        );

        let missing_id = transaction_id();
        assert_eq!(
            store
                .resolve_commit(&missing_id)
                .await
                .expect("resolve missing transaction"),
            CommitResolution::NotCommitted
        );
        assert!(matches!(
            store
                .commit_registry
                .lock()
                .expect("commit registry")
                .get(&missing_id),
            Some(CommitRegistryState::NotCommitted)
        ));

        let inflight_id = transaction_id();
        store
            .commit_registry
            .lock()
            .expect("commit registry")
            .insert(inflight_id, CommitRegistryState::InFlight);
        assert_eq!(
            store
                .resolve_commit(&inflight_id)
                .await
                .expect("resolve in-flight transaction"),
            CommitResolution::Unresolved
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_not_committed_id_cannot_be_reused() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let reused_id = transaction_id();
        let item = key(b"must-stay-absent");

        assert_eq!(
            store
                .resolve_commit(&reused_id)
                .await
                .expect("resolve missing transaction"),
            CommitResolution::NotCommitted
        );

        let mut transaction = store
            .begin_write(reused_id)
            .await
            .expect("begin reused transaction");
        transaction
            .put(item.clone(), value(b"forbidden"), Precondition::Any)
            .await
            .expect("stage reused transaction");
        match transaction.commit().await {
            CommitOutcome::DefiniteFailure(error) => {
                assert_eq!(error.kind(), StateStoreErrorKind::InvalidRequest)
            }
            other => panic!("expected definite invalid-request failure, got {other:?}"),
        }

        assert_eq!(read_value(&store, &item).await, None);
        assert_eq!(
            store
                .resolve_commit(&reused_id)
                .await
                .expect("resolve terminal transaction"),
            CommitResolution::NotCommitted
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_restart_duplicate_id_returns_original_receipt_without_mutating() {
        let temp = TempDir::new().expect("temp dir");
        let original_key = key(b"original-key");
        let duplicate_key = key(b"duplicate-key");
        let duplicate_id = transaction_id();

        let initial_store = store(&temp).await;
        let mut original = initial_store
            .begin_write(duplicate_id)
            .await
            .expect("begin original transaction");
        original
            .put(original_key.clone(), value(b"original"), Precondition::Any)
            .await
            .expect("stage original transaction");
        let original_receipt = committed(original.commit().await);
        let durable_before = durable_counts(&initial_store).await;
        drop(initial_store);

        let reopened = store(&temp).await;
        let mut duplicate = reopened
            .begin_write(duplicate_id)
            .await
            .expect("begin duplicate transaction");
        duplicate
            .put(
                duplicate_key.clone(),
                value(b"must-not-apply"),
                Precondition::Any,
            )
            .await
            .expect("stage duplicate transaction");
        assert_eq!(
            committed(duplicate.commit().await),
            original_receipt,
            "the persistent ledger receipt must remain authoritative"
        );

        assert_eq!(read_value(&reopened, &duplicate_key).await, None);
        assert_eq!(
            read_value(&reopened, &original_key)
                .await
                .expect("original record")
                .value,
            value(b"original")
        );
        assert_eq!(durable_counts(&reopened).await, durable_before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_recovery_reservation_rejects_commit_without_blocking_publish() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let raced_id = transaction_id();
        let item = key(b"resolver-race");
        let mut transaction = store
            .begin_write(raced_id)
            .await
            .expect("begin raced transaction");
        transaction
            .put(item.clone(), value(b"must-not-commit"), Precondition::Any)
            .await
            .expect("stage raced mutation");

        let resolver_gate = TestGate::new();
        *store
            .test_hooks
            .resolve_after_lookup
            .lock()
            .expect("resolve test hook") = Some(resolver_gate.clone());
        let resolver_store = Arc::clone(&store);
        let resolver = tokio::spawn(async move { resolver_store.resolve_commit(&raced_id).await });
        resolver_gate.wait_reached().await;

        let second_resolver_store = Arc::clone(&store);
        let mut second_resolver =
            tokio::spawn(async move { second_resolver_store.resolve_commit(&raced_id).await });
        let mut commit = tokio::spawn(async move { transaction.commit().await });
        let prepublish = tokio::time::timeout(Duration::from_secs(1), async {
            let second_resolution = (&mut second_resolver).await;
            let commit_outcome = (&mut commit).await;
            (second_resolution, commit_outcome)
        })
        .await;
        resolver_gate.release().await;
        assert_eq!(
            resolver
                .await
                .expect("resolver task")
                .expect("resolve raced transaction"),
            CommitResolution::NotCommitted
        );
        let (second_resolution, commit_outcome) = match prepublish {
            Ok(results) => results,
            Err(_) => {
                let _ = second_resolver.await;
                let _ = commit.await;
                panic!("resolver reservation blocked concurrent registry operations")
            }
        };
        assert_eq!(
            second_resolution
                .expect("second resolver task")
                .expect("resolve recovery reservation"),
            CommitResolution::Unresolved
        );
        match commit_outcome.expect("commit task") {
            CommitOutcome::CommitUnknown(error) => {
                assert_eq!(error.kind(), StateStoreErrorKind::Conflict)
            }
            other => panic!("expected uncertain recovery-reservation rejection, got {other:?}"),
        }
        assert_eq!(read_value(&store, &item).await, None);
        assert_eq!(
            store
                .resolve_commit(&raced_id)
                .await
                .expect("resolve terminal race"),
            CommitResolution::NotCommitted
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_real_commit_worker_transitions_inflight_to_terminal() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let committed_id = transaction_id();
        let mut transaction = store
            .begin_write(committed_id)
            .await
            .expect("begin transaction");
        transaction
            .put(key(b"inflight"), value(b"value"), Precondition::Any)
            .await
            .expect("stage transaction");

        let commit_gate = TestGate::new();
        *store
            .test_hooks
            .commit_after_inflight
            .lock()
            .expect("commit test hook") = Some(commit_gate.clone());
        let commit = tokio::spawn(async move { transaction.commit().await });
        commit_gate.wait_reached().await;
        assert_eq!(
            store
                .resolve_commit(&committed_id)
                .await
                .expect("resolve in-flight commit"),
            CommitResolution::Unresolved
        );

        let duplicate_key = key(b"duplicate-inflight");
        let mut duplicate = store
            .begin_write(committed_id)
            .await
            .expect("begin duplicate transaction");
        duplicate
            .put(
                duplicate_key.clone(),
                value(b"must-not-apply"),
                Precondition::Any,
            )
            .await
            .expect("stage duplicate transaction");
        match duplicate.commit().await {
            CommitOutcome::CommitUnknown(error) => {
                assert_eq!(error.kind(), StateStoreErrorKind::Conflict)
            }
            other => panic!("expected uncertain in-flight duplicate, got {other:?}"),
        }
        assert_eq!(read_value(&store, &duplicate_key).await, None);

        commit_gate.release().await;
        let receipt = committed(commit.await.expect("commit task"));
        assert_eq!(
            store
                .resolve_commit(&committed_id)
                .await
                .expect("resolve committed transaction"),
            CommitResolution::Committed(receipt)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_commit_worker_panic_before_apply_recovers_not_committed() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let transaction_id = transaction_id();
        let item = key(b"panic-before-apply");
        let durable_before = durable_state_counts(&store).await;
        let mut transaction = store
            .begin_write(transaction_id)
            .await
            .expect("begin transaction");
        transaction
            .put(item.clone(), value(b"must-not-apply"), Precondition::Any)
            .await
            .expect("stage transaction");
        store
            .test_hooks
            .panic_next_commit_before_apply
            .store(true, Ordering::Release);

        assert!(matches!(
            transaction.commit().await,
            CommitOutcome::CommitUnknown(_)
        ));
        for _ in 0..3 {
            assert_eq!(
                store
                    .resolve_commit(&transaction_id)
                    .await
                    .expect("resolve failed commit worker"),
                CommitResolution::NotCommitted
            );
        }
        assert_eq!(read_value(&store, &item).await, None);
        assert_eq!(durable_state_counts(&store).await, durable_before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_commit_worker_panic_after_apply_recovers_committed() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let transaction_id = transaction_id();
        let item = key(b"panic-after-apply");
        let mut transaction = store
            .begin_write(transaction_id)
            .await
            .expect("begin transaction");
        transaction
            .put(item.clone(), value(b"must-apply-once"), Precondition::Any)
            .await
            .expect("stage transaction");
        store
            .test_hooks
            .panic_next_commit_after_apply
            .store(true, Ordering::Release);

        assert!(matches!(
            transaction.commit().await,
            CommitOutcome::CommitUnknown(_)
        ));
        let receipt = match store
            .resolve_commit(&transaction_id)
            .await
            .expect("resolve committed worker failure")
        {
            CommitResolution::Committed(receipt) => receipt,
            other => panic!("committed worker failure must recover immediately: {other:?}"),
        };
        for _ in 0..3 {
            assert_eq!(
                store
                    .resolve_commit(&transaction_id)
                    .await
                    .expect("repeat committed resolution"),
                CommitResolution::Committed(receipt.clone())
            );
        }
        assert_eq!(
            read_value(&store, &item)
                .await
                .expect("committed row")
                .value,
            value(b"must-apply-once")
        );
        assert_eq!(durable_state_counts(&store).await, (1, 1, 1, 1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_cancelled_commit_worker_rolls_back_before_not_committed() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let cancelled_id = transaction_id();
        let cancelled_key = key(b"cancelled-commit");
        let durable_before = durable_counts(&store).await;
        let mut transaction = store
            .begin_write(cancelled_id)
            .await
            .expect("begin transaction");
        transaction
            .put(
                cancelled_key.clone(),
                value(b"must-not-apply"),
                Precondition::Any,
            )
            .await
            .expect("stage transaction");

        let commit_gate = TestGate::new();
        *store
            .test_hooks
            .commit_after_inflight
            .lock()
            .expect("commit test hook") = Some(commit_gate.clone());
        let commit = tokio::spawn(async move { transaction.commit().await });
        commit_gate.wait_reached().await;
        commit.abort();
        assert!(
            commit
                .await
                .expect_err("commit task must be cancelled")
                .is_cancelled(),
            "commit task cancellation must drop the commit future"
        );
        assert_eq!(
            store
                .resolve_commit(&cancelled_id)
                .await
                .expect("resolve paused cancelled commit"),
            CommitResolution::Unresolved
        );

        commit_gate.release().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match store
                    .resolve_commit(&cancelled_id)
                    .await
                    .expect("resolve cancelled commit")
                {
                    CommitResolution::Unresolved => tokio::task::yield_now().await,
                    CommitResolution::NotCommitted => break,
                    CommitResolution::Committed(receipt) => {
                        panic!("cancelled commit became committed: {receipt:?}")
                    }
                }
            }
        })
        .await
        .expect("cancelled commit must reach a terminal state");

        assert_eq!(read_value(&store, &cancelled_key).await, None);
        assert_eq!(durable_counts(&store).await, durable_before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_authoritative_receipt_survives_cleanup_failure() {
        let temp = TempDir::new().expect("temp dir");
        let duplicate_id = transaction_id();
        let duplicate_key = key(b"cleanup-failure");
        let initial_store = store(&temp).await;
        let mut original = initial_store
            .begin_write(duplicate_id)
            .await
            .expect("begin original transaction");
        original
            .put(key(b"committed"), value(b"value"), Precondition::Any)
            .await
            .expect("stage original transaction");
        let original_receipt = committed(original.commit().await);
        let durable_before = durable_counts(&initial_store).await;
        drop(initial_store);

        let reopened = store(&temp).await;
        let mut duplicate = reopened
            .begin_write(duplicate_id)
            .await
            .expect("begin duplicate transaction");
        duplicate
            .put(
                duplicate_key.clone(),
                value(b"must-not-apply"),
                Precondition::Any,
            )
            .await
            .expect("stage duplicate transaction");
        let duplicate_state = Arc::clone(
            &duplicate
                .owner()
                .expect("duplicate transaction owner")
                .state,
        );
        tokio::task::spawn_blocking(move || {
            let state = duplicate_state.lock().expect("duplicate transaction state");
            state
                .connection
                .execute_batch("ROLLBACK")
                .expect("force cleanup failure precondition");
            assert!(state.active, "test must leave the owner marked active");
        })
        .await
        .expect("cleanup fault worker");

        assert_eq!(
            committed(duplicate.commit().await),
            original_receipt,
            "the authoritative ledger receipt must survive cleanup failure"
        );
        assert_eq!(read_value(&reopened, &duplicate_key).await, None);
        assert_eq!(durable_counts(&reopened).await, durable_before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_provider_key_limit_precedes_sqlite_io() {
        let temp = TempDir::new().expect("temp dir");
        let store = store_with_limits(
            &temp,
            StateStoreLimitOverrides {
                max_key_bytes: Some(3),
                ..StateStoreLimitOverrides::default()
            },
        )
        .await;
        let oversized = key(b"four");

        let mut reader = store.begin_read().await.expect("begin read");
        let error = reader.get(&oversized).await.expect_err("read key limit");
        assert_eq!(error.kind(), StateStoreErrorKind::LimitExceeded);
        assert!(
            !reader
                .owner()
                .expect("reader owner")
                .state
                .lock()
                .expect("reader state")
                .snapshot_established,
            "rejected get must not establish a SQLite snapshot"
        );
        reader.abort().await.expect("abort read");

        let mut writer = store
            .begin_write(transaction_id())
            .await
            .expect("begin write");
        let error = writer
            .put(oversized, value(b"value"), Precondition::Any)
            .await
            .expect_err("write key limit");
        assert_eq!(error.kind(), StateStoreErrorKind::LimitExceeded);
        {
            let state = writer
                .owner()
                .expect("writer owner")
                .state
                .lock()
                .expect("writer state");
            assert_eq!(state.operation_count, 0);
            assert!(state.overlay.is_empty());
            assert!(!state.snapshot_established);
        }
        writer.abort().await.expect("abort write");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_deadline_interrupts_blocking_sql() {
        let temp = TempDir::new().expect("temp dir");
        let store = store_with_limits(
            &temp,
            StateStoreLimitOverrides {
                transaction_deadline_ms: Some(50),
                ..StateStoreLimitOverrides::default()
            },
        )
        .await;
        let item = key(b"deadline-blocked");
        let mut transaction = store
            .begin_write(transaction_id())
            .await
            .expect("begin deadline transaction");
        transaction
            .put(item.clone(), value(b"must-not-commit"), Precondition::Any)
            .await
            .expect("stage deadline mutation");

        let blocker = open_connection(&store.path).expect("open deterministic SQL blocker");
        blocker
            .execute_batch("BEGIN IMMEDIATE")
            .expect("hold SQLite writer lock");
        let outcome = tokio::time::timeout(Duration::from_secs(2), transaction.commit())
            .await
            .expect("transaction deadline must interrupt blocking SQL");
        blocker
            .execute_batch("ROLLBACK")
            .expect("release SQLite writer lock");

        match outcome {
            CommitOutcome::DefiniteFailure(error) => {
                assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded)
            }
            other => panic!("expected definite deadline failure, got {other:?}"),
        }
        assert_eq!(read_value(&store, &item).await, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_cancelled_range_stops_before_another_refill() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let rows = (0_u8..32)
            .map(|number| {
                (
                    Key::try_from(Bytes::from(vec![number])).expect("valid key"),
                    value(b"base"),
                )
            })
            .collect::<Vec<_>>();
        let mut seed = store
            .begin_write(transaction_id())
            .await
            .expect("begin seed write");
        for (key, value) in &rows {
            seed.put(key.clone(), value.clone(), Precondition::Any)
                .await
                .expect("stage seed row");
        }
        committed(seed.commit().await);
        let durable_before = durable_counts(&store).await;

        let gate = TestGate::new();
        *store
            .test_hooks
            .range_after_refill
            .lock()
            .expect("install range gate") = Some(gate.clone());
        store
            .test_hooks
            .range_refill_count
            .store(0, Ordering::Release);

        let mut writer = store
            .begin_write(transaction_id())
            .await
            .expect("begin overlay write");
        for (key, _) in &rows {
            writer
                .delete(key.clone(), Precondition::Any)
                .await
                .expect("stage overlay delete");
        }
        let owner = writer.owner().expect("writer owner").clone();
        let task = tokio::spawn(async move {
            let result = writer
                .range(&RangeRequest {
                    range: KeyRange::new(
                        Key::try_from(Bytes::from(vec![0_u8])).expect("range start"),
                        Key::try_from(Bytes::from(vec![0xff])).expect("range end"),
                    )
                    .expect("bounded range"),
                    direction: Direction::Forward,
                    page_size: 1,
                    continuation: None,
                })
                .await;
            (writer, result)
        });

        gate.wait_reached().await;
        assert_eq!(
            store.test_hooks.range_refill_count.load(Ordering::Acquire),
            1
        );
        owner.cancelled.store(true, Ordering::Release);
        owner.interrupt_handle.interrupt();
        gate.release().await;

        let (writer, result) = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("cancelled range worker must terminate")
            .expect("range task must join");
        assert_eq!(
            result.expect_err("cancelled range must fail").kind(),
            StateStoreErrorKind::DeadlineExceeded
        );
        assert_eq!(
            store.test_hooks.range_refill_count.load(Ordering::Acquire),
            1,
            "cancelled range must not issue another bounded refill"
        );
        assert!(
            !owner.state.lock().expect("transaction state").active,
            "cancelled range must roll back before returning"
        );
        drop(writer);
        assert_eq!(durable_counts(&store).await, durable_before);
        for (key, expected) in rows {
            assert_eq!(
                read_value(&store, &key)
                    .await
                    .expect("seed row must remain durable")
                    .value,
                expected
            );
        }
    }

    #[tokio::test]
    async fn sqlite_history_capacity_pruning_keeps_resolution_conservative() {
        let temp = TempDir::new().expect("temporary directory");
        let store = store_with_policy(
            &temp,
            StateStoreLimitOverrides {
                max_transaction_operations: Some(1),
                ..StateStoreLimitOverrides::default()
            },
            SqliteHistoryRetentionConfig {
                max_age_secs: 60 * 60,
                max_change_rows: 1,
                max_commit_receipts: 1,
                maintenance_interval_commits: 64,
                incremental_vacuum_pages: 1,
            },
        )
        .await;
        let first = transaction_id();
        let second = transaction_id();
        for (transaction_id, key) in [(first, key(b"history-a")), (second, key(b"history-b"))] {
            let mut transaction = store
                .begin_write(transaction_id)
                .await
                .expect("begin retained-history transaction");
            transaction
                .put(key, value(b"value"), Precondition::Any)
                .await
                .expect("stage retained-history mutation");
            committed(transaction.commit().await);
        }

        assert_eq!(durable_counts(&store).await, (2, 1, 1));
        drop(store);
        let store = store_with_policy(
            &temp,
            StateStoreLimitOverrides {
                max_transaction_operations: Some(1),
                ..StateStoreLimitOverrides::default()
            },
            SqliteHistoryRetentionConfig {
                max_age_secs: 60 * 60,
                max_change_rows: 1,
                max_commit_receipts: 1,
                maintenance_interval_commits: 64,
                incremental_vacuum_pages: 1,
            },
        )
        .await;
        assert!(matches!(
            store
                .resolve_commit(&first)
                .await
                .expect("resolve retired id"),
            CommitResolution::Unresolved
        ));
        match store.begin_write(first).await {
            Ok(_) => panic!("retired transaction id must not be reusable"),
            Err(error) => assert_eq!(error.kind(), StateStoreErrorKind::InvalidRequest),
        }
        assert!(matches!(
            store
                .resolve_commit(&Uuid::now_v7().into())
                .await
                .expect("resolve unknown id"),
            CommitResolution::NotCommitted
        ));
    }

    #[test]
    fn sqlite_transaction_busy_classifier_only_treats_snapshot_code_as_conflict() {
        let base_busy = rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_BUSY),
            Some("base busy".to_owned()),
        );
        let locked = rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_LOCKED),
            Some("locked".to_owned()),
        );
        let busy_snapshot = rusqlite::Error::SqliteFailure(
            ffi::Error::new(SQLITE_BUSY_SNAPSHOT),
            Some("busy snapshot".to_owned()),
        );

        assert!(matches!(
            classify_apply_error(&base_busy),
            CommitOutcome::TransientBeforeCommit(_)
        ));
        assert!(matches!(
            classify_apply_error(&locked),
            CommitOutcome::TransientBeforeCommit(_)
        ));
        assert!(matches!(
            classify_apply_error(&busy_snapshot),
            CommitOutcome::Conflict(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_transaction_persistent_writer_contention_is_transient_not_conflict() {
        let temp = TempDir::new().expect("temp dir");
        let store = store(&temp).await;
        let item = key(b"contended");
        let mut transaction = store
            .begin_write(transaction_id())
            .await
            .expect("begin transaction");
        assert_eq!(
            transaction.get(&item).await.expect("establish snapshot"),
            None
        );
        transaction
            .put(item, value(b"value"), Precondition::Any)
            .await
            .expect("stage mutation");

        let blocker = open_connection(&store.path).expect("blocker connection");
        blocker
            .execute_batch("BEGIN IMMEDIATE")
            .expect("hold SQLite writer lock");
        let outcome = transaction.commit().await;
        blocker
            .execute_batch("ROLLBACK")
            .expect("release writer lock");
        assert!(matches!(outcome, CommitOutcome::TransientBeforeCommit(_)));
    }
}
