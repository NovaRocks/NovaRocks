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

//! Test-only in-memory StateStore support. This module is deliberately gated
//! behind `state-store-conformance` and is not a production provider.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use super::{
    ChangeCursor, ChangeHint, ChangePage, ChangePollRequest, CommitOutcome, CommitReceipt,
    CommitResolution, Direction, Key, Precondition, RangePage, RangeRequest, ReadTransaction,
    StateRecord, StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits,
    StateStoreMetrics, StateStoreMetricsSnapshot, StateStoreOperation, StateStoreOutcome,
    StoreIdentity, StoreRevision, TransactionId, Value, VersionToken, WriteTransaction,
};
use super::{
    StateStoreOpenRequest, StateStoreProviderDescriptor, StateStoreProviderFactory,
    StateStoreProviderInstance, StateStoreProviderLifecycle,
};

const IN_MEMORY_PROVIDER_ID: super::StateStoreProviderId =
    super::StateStoreProviderId::new("in-memory-test");

/// A deterministic, serializable reference implementation for consumer tests.
pub struct InMemoryStateStore {
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
    inner: Arc<Mutex<Inner>>,
    post_dispatch_hold: Arc<Mutex<Option<Arc<InMemoryCommitHold>>>>,
}

struct Inner {
    identity: StoreIdentity,
    revision: u64,
    records: BTreeMap<Key, StateRecord>,
    changes: Vec<Change>,
    commits: HashMap<TransactionId, CommitResolution>,
}

#[derive(Clone)]
struct Change {
    revision: u64,
    sequence: u32,
    key: Key,
}

struct InMemoryCommitHold {
    progress: watch::Sender<bool>,
}

impl InMemoryCommitHold {
    #[allow(dead_code)]
    fn new() -> Arc<Self> {
        let (progress, _) = watch::channel(false);
        Arc::new(Self { progress })
    }

    async fn wait_for_progress(&self) {
        let mut progress = self.progress.subscribe();
        progress
            .wait_for(|allowed| *allowed)
            .await
            .expect("in-memory progress sender");
    }

    #[allow(dead_code)]
    fn allow_provider_progress(&self) {
        self.progress.send_replace(true);
    }
}

#[derive(Clone)]
enum Mutation {
    Put {
        key: Key,
        value: Value,
        precondition: Precondition,
    },
    Delete {
        key: Key,
        precondition: Precondition,
    },
}

impl InMemoryStateStore {
    pub fn new(cluster_id: impl Into<String>) -> Self {
        Self::with_limits(cluster_id, StateStoreLimits::default())
    }

    pub fn with_limits(cluster_id: impl Into<String>, limits: StateStoreLimits) -> Self {
        Self {
            limits,
            metrics: Arc::new(StateStoreMetrics::new(IN_MEMORY_PROVIDER_ID)),
            inner: Arc::new(Mutex::new(Inner {
                identity: StoreIdentity {
                    store_id: Uuid::now_v7(),
                    cluster_id: cluster_id.into(),
                },
                revision: 0,
                records: BTreeMap::new(),
                changes: Vec::new(),
                commits: HashMap::new(),
            })),
            post_dispatch_hold: Arc::new(Mutex::new(None)),
        }
    }

    fn begin_snapshot(&self) -> (u64, BTreeMap<Key, StateRecord>) {
        let inner = self.inner.lock().expect("in-memory state store");
        (inner.revision, inner.records.clone())
    }

    #[cfg(test)]
    fn arm_post_dispatch(&self) -> Arc<InMemoryCommitHold> {
        let hold = InMemoryCommitHold::new();
        *self
            .post_dispatch_hold
            .lock()
            .expect("in-memory post-dispatch hold") = Some(Arc::clone(&hold));
        hold
    }
}

/// Test-only provider adapter for Frontend consumer tests. It deliberately
/// lives with the SPI reference store so consumer crates never depend on a
/// concrete production provider merely to exercise host lifecycle behavior.
pub struct InMemoryStateStoreProviderFactory {
    descriptor: StateStoreProviderDescriptor,
}

impl InMemoryStateStoreProviderFactory {
    pub const fn new(descriptor: StateStoreProviderDescriptor) -> Self {
        Self { descriptor }
    }
}

#[async_trait]
impl StateStoreProviderFactory for InMemoryStateStoreProviderFactory {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
        if std::time::Instant::now() >= request.deadline {
            return Err(StateStoreError::new(
                StateStoreErrorKind::DeadlineExceeded,
                "in-memory test provider deadline exceeded",
            ));
        }
        Ok(Box::new(InMemoryStateStoreProviderInstance {
            descriptor: self.descriptor,
            state_store: Some(Arc::new(InMemoryStateStore::with_limits(
                request.cluster_id,
                request.limits,
            ))),
        }))
    }
}

struct InMemoryStateStoreProviderInstance {
    descriptor: StateStoreProviderDescriptor,
    state_store: Option<Arc<dyn StateStore>>,
}

#[async_trait]
impl StateStoreProviderInstance for InMemoryStateStoreProviderInstance {
    fn descriptor(&self) -> &StateStoreProviderDescriptor {
        &self.descriptor
    }

    fn lifecycle(&self) -> StateStoreProviderLifecycle {
        if self.state_store.is_some() {
            StateStoreProviderLifecycle::Ready
        } else {
            StateStoreProviderLifecycle::Stopped
        }
    }

    fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        self.state_store.clone()
    }

    async fn shutdown(&mut self, _deadline: std::time::Instant) -> Result<(), StateStoreError> {
        self.state_store.take();
        Ok(())
    }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.metrics.snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        let started = std::time::Instant::now();
        self.metrics.record_operation(
            StateStoreOperation::Begin,
            StateStoreOutcome::Success,
            started.elapsed(),
        );
        Ok(Box::new(InMemoryReadTransaction {
            snapshot: None,
            limits: self.limits.clone(),
            metrics: Arc::clone(&self.metrics),
            inner: Arc::clone(&self.inner),
        }))
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        let started = std::time::Instant::now();
        let (base_revision, snapshot) = self.begin_snapshot();
        let mut inner = self.inner.lock().expect("in-memory state store");
        inner
            .commits
            .entry(transaction_id)
            .or_insert(CommitResolution::Unresolved);
        drop(inner);
        self.metrics.record_operation(
            StateStoreOperation::Begin,
            StateStoreOutcome::Success,
            started.elapsed(),
        );
        Ok(Box::new(InMemoryWriteTransaction {
            transaction_id,
            base_revision,
            snapshot,
            mutations: Vec::new(),
            mutation_bytes: 0,
            range_frozen: false,
            completed: false,
            limits: self.limits.clone(),
            metrics: Arc::clone(&self.metrics),
            inner: Arc::clone(&self.inner),
            post_dispatch_hold: Arc::clone(&self.post_dispatch_hold),
        }))
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        let started = std::time::Instant::now();
        request.validate(&self.limits)?;
        let inner = self.inner.lock().expect("in-memory state store");
        let after = request
            .after
            .as_ref()
            .map(|cursor| cursor.decode(inner.identity.store_id))
            .transpose()?;
        let (after_revision, after_sequence) = after
            .as_ref()
            .map(|(revision, sequence)| (parse_revision(revision), *sequence))
            .unwrap_or((0, 0));
        let selected = inner
            .changes
            .iter()
            .filter(|change| {
                change.revision > after_revision
                    || (change.revision == after_revision && change.sequence > after_sequence)
            })
            .take(request.page_size)
            .cloned()
            .collect::<Vec<_>>();
        let revision = revision_token(inner.revision);
        let next_cursor = match selected.last() {
            Some(change) => ChangeCursor::new(
                inner.identity.store_id,
                revision_token(change.revision),
                change.sequence,
            )?,
            None => ChangeCursor::new(inner.identity.store_id, revision, 0)?,
        };
        let hints = selected
            .iter()
            .map(|change| ChangeHint {
                revision: revision_token(change.revision),
                key: change.key.clone(),
            })
            .collect::<Vec<_>>();
        let bytes = hints.iter().fold(0_u64, |total, hint| {
            total
                .saturating_add((hint.key.as_bytes().len() + hint.revision.as_bytes().len()) as u64)
        });
        self.metrics.record_page_records(hints.len() as u64);
        self.metrics.record_bytes_read(bytes);
        self.metrics.record_operation(
            StateStoreOperation::Range,
            StateStoreOutcome::Success,
            started.elapsed(),
        );
        Ok(ChangePage {
            hints,
            next_cursor,
            high_watermark: revision_token(inner.revision),
            resync_required: false,
        })
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        Ok(self
            .inner
            .lock()
            .expect("in-memory state store")
            .identity
            .clone())
    }

    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        Ok(self
            .inner
            .lock()
            .expect("in-memory state store")
            .commits
            .get(transaction_id)
            .cloned()
            .unwrap_or(CommitResolution::NotCommitted))
    }
}

struct InMemoryReadTransaction {
    snapshot: Option<BTreeMap<Key, StateRecord>>,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
    inner: Arc<Mutex<Inner>>,
}

impl InMemoryReadTransaction {
    fn snapshot(&mut self) -> &BTreeMap<Key, StateRecord> {
        self.snapshot.get_or_insert_with(|| {
            self.inner
                .lock()
                .expect("in-memory state store")
                .records
                .clone()
        })
    }
}

#[async_trait]
impl ReadTransaction for InMemoryReadTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = std::time::Instant::now();
        validate_store_value(&self.limits, key, None)?;
        let result = self.snapshot().get(key).cloned();
        self.metrics.record_bytes_read(
            result
                .as_ref()
                .map(|record| record.key.as_bytes().len() + record.value.as_bytes().len())
                .unwrap_or_default() as u64,
        );
        self.metrics.record_operation(
            StateStoreOperation::Get,
            StateStoreOutcome::Success,
            started.elapsed(),
        );
        Ok(result)
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        request.validate(&self.limits)?;
        let snapshot = self.snapshot().clone();
        range_page(&snapshot, &self.limits, request, &self.metrics)
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        Ok(())
    }
}

struct InMemoryWriteTransaction {
    transaction_id: TransactionId,
    base_revision: u64,
    snapshot: BTreeMap<Key, StateRecord>,
    mutations: Vec<Mutation>,
    mutation_bytes: usize,
    range_frozen: bool,
    completed: bool,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
    inner: Arc<Mutex<Inner>>,
    post_dispatch_hold: Arc<Mutex<Option<Arc<InMemoryCommitHold>>>>,
}

impl InMemoryWriteTransaction {
    fn staged_records(&self) -> BTreeMap<Key, StateRecord> {
        let mut records = self.snapshot.clone();
        for mutation in &self.mutations {
            match mutation {
                Mutation::Put { key, value, .. } => {
                    records.insert(
                        key.clone(),
                        StateRecord {
                            key: key.clone(),
                            value: value.clone(),
                            version: version_token(0),
                        },
                    );
                }
                Mutation::Delete { key, .. } => {
                    records.remove(key);
                }
            }
        }
        records
    }

    fn stage(&mut self, mutation: Mutation) -> Result<(), StateStoreError> {
        if self.range_frozen {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "write transaction is frozen after paginated range read",
            ));
        }
        let additional_bytes = match &mutation {
            Mutation::Put { key, value, .. } => key.as_bytes().len() + value.as_bytes().len(),
            Mutation::Delete { key, .. } => key.as_bytes().len(),
        };
        if self.mutations.len() >= self.limits.max_transaction_operations
            || self
                .mutation_bytes
                .checked_add(additional_bytes)
                .is_none_or(|bytes| bytes > self.limits.max_transaction_bytes)
        {
            return Err(StateStoreError::new(
                StateStoreErrorKind::LimitExceeded,
                "transaction mutation envelope exceeds configured limits",
            ));
        }
        match &mutation {
            Mutation::Put { key, value, .. } => {
                validate_store_value(&self.limits, key, Some(value))?
            }
            Mutation::Delete { key, .. } => validate_store_value(&self.limits, key, None)?,
        }
        self.mutation_bytes += additional_bytes;
        self.mutations.push(mutation);
        Ok(())
    }
}

impl Drop for InMemoryWriteTransaction {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut inner = self.inner.lock().expect("in-memory state store");
        if matches!(
            inner.commits.get(&self.transaction_id),
            Some(CommitResolution::Unresolved)
        ) {
            inner
                .commits
                .insert(self.transaction_id, CommitResolution::NotCommitted);
        }
    }
}

#[async_trait]
impl ReadTransaction for InMemoryWriteTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = std::time::Instant::now();
        validate_store_value(&self.limits, key, None)?;
        let result = self.staged_records().get(key).cloned();
        self.metrics.record_operation(
            StateStoreOperation::Get,
            StateStoreOutcome::Success,
            started.elapsed(),
        );
        Ok(result)
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let page = range_page(&self.staged_records(), &self.limits, request, &self.metrics)?;
        self.range_frozen |= page.continuation.is_some();
        Ok(page)
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        self.completed = true;
        self.inner
            .lock()
            .expect("in-memory state store")
            .commits
            .insert(self.transaction_id, CommitResolution::NotCommitted);
        Ok(())
    }
}

#[async_trait]
impl WriteTransaction for InMemoryWriteTransaction {
    fn transaction_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = std::time::Instant::now();
        let result = self.stage(Mutation::Put {
            key,
            value,
            precondition,
        });
        self.metrics.record_operation(
            StateStoreOperation::Put,
            if result.is_ok() {
                StateStoreOutcome::Success
            } else {
                StateStoreOutcome::Error
            },
            started.elapsed(),
        );
        result
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = std::time::Instant::now();
        let result = self.stage(Mutation::Delete { key, precondition });
        self.metrics.record_operation(
            StateStoreOperation::Delete,
            if result.is_ok() {
                StateStoreOutcome::Success
            } else {
                StateStoreOutcome::Error
            },
            started.elapsed(),
        );
        result
    }

    async fn commit(mut self: Box<Self>) -> CommitOutcome {
        let started = std::time::Instant::now();
        let hold = self
            .post_dispatch_hold
            .lock()
            .expect("in-memory post-dispatch hold")
            .take();
        let outcome = if let Some(hold) = hold {
            self.commit_after_post_dispatch(hold).await
        } else {
            let mut inner = self.inner.lock().expect("in-memory state store");
            apply_commit(
                &mut inner,
                self.transaction_id,
                self.base_revision,
                &self.mutations,
            )
        };
        self.completed = true;
        let metric_outcome = match &outcome {
            CommitOutcome::Committed(_) => StateStoreOutcome::Success,
            CommitOutcome::Conflict(_) => StateStoreOutcome::Conflict,
            CommitOutcome::TransientBeforeCommit(_) => StateStoreOutcome::TransientBeforeCommit,
            CommitOutcome::DefiniteFailure(_) => StateStoreOutcome::DefiniteFailure,
            CommitOutcome::CommitUnknown(_) => StateStoreOutcome::CommitUnknown,
        };
        self.metrics.record_operation(
            StateStoreOperation::Commit,
            metric_outcome,
            started.elapsed(),
        );
        outcome
    }
}

impl InMemoryWriteTransaction {
    async fn commit_after_post_dispatch(&self, hold: Arc<InMemoryCommitHold>) -> CommitOutcome {
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut guard = CommitAbandonGuard {
            cancelled: Arc::clone(&cancelled),
            inner: Arc::clone(&self.inner),
            transaction_id: self.transaction_id,
            armed: true,
        };
        let (outcome_tx, outcome_rx) = oneshot::channel();
        let inner = Arc::clone(&self.inner);
        let mutations = self.mutations.clone();
        let transaction_id = self.transaction_id;
        let base_revision = self.base_revision;
        tokio::spawn(async move {
            hold.wait_for_progress().await;
            if cancelled.load(Ordering::Acquire) {
                return;
            }
            let outcome = {
                let mut inner = inner.lock().expect("in-memory state store");
                apply_commit(&mut inner, transaction_id, base_revision, &mutations)
            };
            let _ = outcome_tx.send(outcome);
        });
        let outcome = outcome_rx.await.unwrap_or_else(|_| {
            CommitOutcome::DefiniteFailure(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "in-memory post-dispatch worker stopped",
            ))
        });
        guard.armed = false;
        outcome
    }
}

struct CommitAbandonGuard {
    cancelled: Arc<AtomicBool>,
    inner: Arc<Mutex<Inner>>,
    transaction_id: TransactionId,
    armed: bool,
}

impl Drop for CommitAbandonGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.cancelled.store(true, Ordering::Release);
        let mut inner = self.inner.lock().expect("in-memory state store");
        if matches!(
            inner.commits.get(&self.transaction_id),
            Some(CommitResolution::Unresolved)
        ) {
            inner
                .commits
                .insert(self.transaction_id, CommitResolution::NotCommitted);
        }
    }
}

fn apply_commit(
    inner: &mut Inner,
    transaction_id: TransactionId,
    base_revision: u64,
    mutations: &[Mutation],
) -> CommitOutcome {
    match inner.commits.get(&transaction_id) {
        Some(CommitResolution::Committed(receipt)) => {
            return CommitOutcome::Committed(receipt.clone());
        }
        Some(CommitResolution::NotCommitted) => {
            return CommitOutcome::DefiniteFailure(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "transaction id is terminally not committed",
            ));
        }
        Some(CommitResolution::Unresolved) | None => {}
    }
    if inner.revision != base_revision {
        inner
            .commits
            .insert(transaction_id, CommitResolution::NotCommitted);
        return CommitOutcome::Conflict(StateStoreError::new(
            StateStoreErrorKind::Conflict,
            "in-memory state store snapshot conflict",
        ));
    }
    if !preconditions_hold(&inner.records, mutations) {
        inner
            .commits
            .insert(transaction_id, CommitResolution::NotCommitted);
        return CommitOutcome::Conflict(StateStoreError::new(
            StateStoreErrorKind::PreconditionFailed,
            "in-memory state store precondition failed",
        ));
    }
    inner.revision = inner.revision.saturating_add(1);
    let revision = revision_token(inner.revision);
    let revision_number = inner.revision;
    for (index, mutation) in mutations.iter().enumerate() {
        let key = match mutation {
            Mutation::Put { key, value, .. } => {
                inner.records.insert(
                    key.clone(),
                    StateRecord {
                        key: key.clone(),
                        value: value.clone(),
                        version: version_token(revision_number),
                    },
                );
                key.clone()
            }
            Mutation::Delete { key, .. } => {
                inner.records.remove(key);
                key.clone()
            }
        };
        inner.changes.push(Change {
            revision: revision_number,
            sequence: u32::try_from(index + 1).unwrap_or(u32::MAX),
            key,
        });
    }
    let receipt = CommitReceipt {
        transaction_id,
        revision,
    };
    inner
        .commits
        .insert(transaction_id, CommitResolution::Committed(receipt.clone()));
    CommitOutcome::Committed(receipt)
}

fn range_page(
    records: &BTreeMap<Key, StateRecord>,
    limits: &StateStoreLimits,
    request: &RangeRequest,
    metrics: &StateStoreMetrics,
) -> Result<RangePage, StateStoreError> {
    let started = std::time::Instant::now();
    request.validate(limits)?;
    let resume_after = request
        .continuation
        .as_ref()
        .map(|continuation| continuation.resume_after(request))
        .transpose()?;
    let mut selected = records
        .range(request.range.start.clone()..request.range.end.clone())
        .filter(|(key, _)| match (&request.direction, &resume_after) {
            (Direction::Forward, Some(resume_after)) => *key > resume_after,
            (Direction::Reverse, Some(resume_after)) => *key < resume_after,
            (_, None) => true,
        })
        .map(|(_, record)| record.clone())
        .collect::<Vec<_>>();
    if matches!(request.direction, Direction::Reverse) {
        selected.reverse();
    }
    let has_more = selected.len() > request.page_size;
    selected.truncate(request.page_size);
    let continuation = if has_more {
        selected
            .last()
            .map(|record| request.continuation_after(&record.key))
            .transpose()?
    } else {
        None
    };
    let bytes = selected.iter().fold(0_u64, |total, record| {
        total.saturating_add((record.key.as_bytes().len() + record.value.as_bytes().len()) as u64)
    });
    metrics.record_bytes_read(bytes);
    metrics.record_page_records(selected.len() as u64);
    metrics.record_operation(
        StateStoreOperation::Range,
        StateStoreOutcome::Success,
        started.elapsed(),
    );
    Ok(RangePage {
        records: selected,
        continuation,
    })
}

fn preconditions_hold(records: &BTreeMap<Key, StateRecord>, mutations: &[Mutation]) -> bool {
    let mut working = records.clone();
    for mutation in mutations {
        let (key, precondition) = match mutation {
            Mutation::Put {
                key, precondition, ..
            }
            | Mutation::Delete { key, precondition } => (key, precondition),
        };
        let current = working.get(key);
        let accepted = match precondition {
            Precondition::Any => true,
            Precondition::Absent => current.is_none(),
            Precondition::Present => current.is_some(),
            Precondition::Version(version) => {
                current.is_some_and(|record| &record.version == version)
            }
        };
        if !accepted {
            return false;
        }
        match mutation {
            Mutation::Put { key, value, .. } => {
                working.insert(
                    key.clone(),
                    StateRecord {
                        key: key.clone(),
                        value: value.clone(),
                        version: version_token(0),
                    },
                );
            }
            Mutation::Delete { key, .. } => {
                working.remove(key);
            }
        }
    }
    true
}

fn validate_store_value(
    limits: &StateStoreLimits,
    key: &Key,
    value: Option<&Value>,
) -> Result<(), StateStoreError> {
    if key.as_bytes().len() > limits.max_key_bytes
        || value.is_some_and(|value| value.as_bytes().len() > limits.max_value_bytes)
    {
        return Err(StateStoreError::new(
            StateStoreErrorKind::LimitExceeded,
            "value exceeds configured state store limits",
        ));
    }
    Ok(())
}

fn revision_token(revision: u64) -> StoreRevision {
    StoreRevision::try_from(Bytes::from(format!("r{revision:020}")))
        .expect("in-memory revision token")
}

fn version_token(revision: u64) -> VersionToken {
    VersionToken::try_from(Bytes::from(format!("v{revision:020}")))
        .expect("in-memory version token")
}

fn parse_revision(revision: &StoreRevision) -> u64 {
    std::str::from_utf8(revision.as_bytes())
        .ok()
        .and_then(|value| value.strip_prefix('r'))
        .and_then(|value| value.parse().ok())
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;
    use crate::state_store::conformance::{
        FaultGate, FaultInjectingStateStore, PostDispatchControl, PostDispatchController,
        PostDispatchScenario, StateStoreConformanceFixture, StateStoreFactory,
        run_state_store_conformance,
    };

    struct InMemoryPostDispatchController {
        fault: Arc<FaultInjectingStateStore>,
        store: Arc<InMemoryStateStore>,
    }

    #[async_trait]
    impl PostDispatchController for InMemoryPostDispatchController {
        async fn arm(&self, scenario: PostDispatchScenario) -> Box<dyn PostDispatchControl> {
            let gate = FaultGate::new();
            let hold = self.store.arm_post_dispatch();
            match scenario {
                PostDispatchScenario::CancelWaiterBeforeApply => {
                    self.fault.pause_next_post_dispatch(gate.clone())
                }
                PostDispatchScenario::LoseCommittedResponse => {
                    self.fault.lose_next_post_dispatch_response(gate.clone())
                }
            }
            Box::new(InMemoryPostDispatchControl { gate, hold })
        }
    }

    struct InMemoryPostDispatchControl {
        gate: FaultGate,
        hold: Arc<InMemoryCommitHold>,
    }

    #[async_trait]
    impl PostDispatchControl for InMemoryPostDispatchControl {
        async fn wait_dispatched(&self) {
            self.gate.wait_reached().await;
            self.gate.wait_armed().await;
        }

        async fn wait_waiter_cancelled(&self) {
            self.gate.wait_cancelled().await;
        }

        async fn allow_provider_progress(&self) {
            self.hold.allow_provider_progress();
        }

        async fn release_response(&self) {
            self.gate.release().await;
        }

        async fn wait_inner_dropped(&self) {
            self.gate.wait_inner_dropped().await;
        }
    }

    fn factory() -> StateStoreFactory {
        Rc::new(|| {
            Box::pin(async {
                let in_memory = Arc::new(InMemoryStateStore::with_limits(
                    "test-cluster",
                    StateStoreLimits {
                        max_key_bytes: 64,
                        max_value_bytes: 64,
                        max_page_size: 10,
                        max_transaction_operations: 8,
                        max_transaction_bytes: 300,
                        ..StateStoreLimits::default()
                    },
                ));
                let store: Arc<dyn StateStore> = in_memory.clone();
                let fault = FaultInjectingStateStore::new(store);
                let controller: Arc<dyn PostDispatchController> =
                    Arc::new(InMemoryPostDispatchController {
                        fault: Arc::clone(&fault),
                        store: in_memory,
                    });
                Ok(StateStoreConformanceFixture::new(fault, controller))
            })
        })
    }

    #[tokio::test]
    async fn reference_store_conforms_to_the_spi_contract() {
        run_state_store_conformance(factory()).await;
    }

    #[test]
    fn default_limits_are_not_relaxed() {
        let store = InMemoryStateStore::new("test-cluster");
        assert_eq!(store.limits(), &StateStoreLimits::default());
    }
}
