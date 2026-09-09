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

//! Test-only in-memory StateStore support. It lives in a test-only crate and
//! is not a production provider.
//!
//! # Evidence, and why absence proves nothing
//!
//! The fake keeps one private map from [`AttemptId`] to [`AttemptOutcome`].
//! That map is *evidence*, not an answer sheet: an entry is written when a
//! write attempt begins, replaced by a terminal outcome once one is proven,
//! and removed only when the supervisor says the terminal is safely published.
//!
//! A missing entry therefore means "this store cannot prove anything", which
//! adjudicates to [`AttemptOutcome::Unresolved`] and never to
//! [`AttemptOutcome::NotCommitted`]. The fake is held to the same rule as a
//! real provider on purpose: it and the SQLite provider run the same basic and
//! attempt suites, so a reference store that answered denials from absence
//! would quietly license every provider to do the same.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use novarocks_state_store_api::{
    AttemptId, AttemptOutcome, AttemptSupervisor, CommitOutcome, CommitReceipt,
    DEFAULT_MAX_OUTSTANDING_ATTEMPTS, Direction, InDoubtAdjudicator, Key, Precondition, RangePage,
    RangeRequest, ReadTransaction, StateRecord, StateStore, StateStoreError, StateStoreErrorKind,
    StateStoreLimits, StoreIdentity, StoreRevision, Value, VersionToken, WriteAttempt,
    WriteTransaction,
};
use novarocks_state_store_api::{
    StateStoreOpenRequest, StateStoreProviderDescriptor, StateStoreProviderFactory,
    StateStoreProviderInstance, StateStoreProviderLifecycle,
};

/// A deterministic, serializable reference implementation for consumer tests.
pub struct InMemoryStateStore {
    limits: StateStoreLimits,
    attempts: AttemptSupervisor,
    inner: Arc<Mutex<Inner>>,
    post_dispatch_hold: Arc<Mutex<Option<Arc<InMemoryCommitHold>>>>,
}

struct Inner {
    identity: StoreIdentity,
    revision: u64,
    records: BTreeMap<Key, StateRecord>,
    /// Private commit evidence, keyed by the attempt that produced it. Only
    /// this store reads it, and its absence is deliberately not a verdict.
    commits: HashMap<AttemptId, AttemptOutcome>,
}

/// The store's in-doubt callback.
///
/// It is a separate object because [`AttemptSupervisor`] needs the adjudicator
/// at construction time while the adjudicator needs the same state the store
/// mutates; both therefore share [`Inner`] rather than one owning the other.
struct InMemoryEvidence {
    inner: Arc<Mutex<Inner>>,
}

#[async_trait]
impl InDoubtAdjudicator for InMemoryEvidence {
    async fn adjudicate(&self, attempt: AttemptId) -> Result<AttemptOutcome, StateStoreError> {
        // A read that cannot be performed is not proof that nothing happened.
        let Ok(inner) = self.inner.lock() else {
            return Ok(AttemptOutcome::Unresolved);
        };
        // Neither is a missing record. Only a recorded terminal decides.
        Ok(inner
            .commits
            .get(&attempt)
            .cloned()
            .unwrap_or(AttemptOutcome::Unresolved))
    }

    async fn release_evidence(&self, attempt: AttemptId) -> Result<(), StateStoreError> {
        let mut inner = self.inner.lock().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "in-memory state store lock is poisoned",
            )
        })?;
        inner.commits.remove(&attempt);
        Ok(())
    }
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
        Self::with_limits_and_capacity(cluster_id, limits, default_attempt_capacity())
    }

    /// Builds a store whose instance admits at most `outstanding_attempts`
    /// charged attempts at once.
    ///
    /// Tests that exercise saturation want a small ceiling; nothing else does,
    /// which is why the ordinary constructors keep the contract default.
    pub fn with_limits_and_capacity(
        cluster_id: impl Into<String>,
        limits: StateStoreLimits,
        outstanding_attempts: NonZeroUsize,
    ) -> Self {
        let inner = Arc::new(Mutex::new(Inner {
            identity: StoreIdentity {
                store_id: Uuid::now_v7(),
                cluster_id: cluster_id.into(),
            },
            revision: 0,
            records: BTreeMap::new(),
            commits: HashMap::new(),
        }));
        let adjudicator: Arc<dyn InDoubtAdjudicator> = Arc::new(InMemoryEvidence {
            inner: Arc::clone(&inner),
        });
        Self {
            limits,
            attempts: AttemptSupervisor::new(outstanding_attempts, adjudicator),
            inner,
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

fn default_attempt_capacity() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_MAX_OUTSTANDING_ATTEMPTS).expect("default attempt capacity")
}

/// Test-only provider adapter for Frontend consumer tests. It deliberately
/// lives with the reference store so consumer crates never depend on a
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

    fn attempts(&self) -> &AttemptSupervisor {
        &self.attempts
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        Ok(Box::new(InMemoryReadTransaction {
            snapshot: None,
            limits: self.limits.clone(),
            inner: Arc::clone(&self.inner),
        }))
    }

    async fn begin_write(
        &self,
        attempt: WriteAttempt,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        // Refuse a capability another instance issued before registering
        // anything, so a handle held across a reopen cannot address this store.
        // The attempt is simply dropped: it never reached storage here, and
        // this store is in no position to make statements about it.
        attempt.require_scope(self.attempts.scope())?;
        let (base_revision, snapshot) = self.begin_snapshot();
        self.inner
            .lock()
            .expect("in-memory state store")
            .commits
            .entry(attempt.id())
            .or_insert(AttemptOutcome::Unresolved);
        Ok(Box::new(InMemoryWriteTransaction {
            attempt,
            base_revision,
            snapshot,
            mutations: Vec::new(),
            mutation_bytes: 0,
            range_frozen: false,
            completed: false,
            limits: self.limits.clone(),
            inner: Arc::clone(&self.inner),
            post_dispatch_hold: Arc::clone(&self.post_dispatch_hold),
        }))
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        Ok(self
            .inner
            .lock()
            .expect("in-memory state store")
            .identity
            .clone())
    }
}

struct InMemoryReadTransaction {
    snapshot: Option<BTreeMap<Key, StateRecord>>,
    limits: StateStoreLimits,
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
        validate_store_value(&self.limits, key, None)?;
        Ok(self.snapshot().get(key).cloned())
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        request.validate(&self.limits)?;
        let snapshot = self.snapshot().clone();
        range_page(&snapshot, &self.limits, request)
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        Ok(())
    }
}

struct InMemoryWriteTransaction {
    attempt: WriteAttempt,
    base_revision: u64,
    snapshot: BTreeMap<Key, StateRecord>,
    mutations: Vec<Mutation>,
    mutation_bytes: usize,
    range_frozen: bool,
    completed: bool,
    limits: StateStoreLimits,
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
        // Never dispatched: the attempt is provably free of write effect, and
        // saying so is what lets the supervisor reclaim its slot. If it was
        // dispatched this call fails and is ignored -- an abandoned dispatch is
        // decided by evidence below, not by the handle going away.
        let _ = self.attempt.cancel_before_dispatch();
        abandon_evidence(&self.inner, self.attempt.id());
    }
}

#[async_trait]
impl ReadTransaction for InMemoryWriteTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        validate_store_value(&self.limits, key, None)?;
        Ok(self.staged_records().get(key).cloned())
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let page = range_page(&self.staged_records(), &self.limits, request)?;
        self.range_frozen |= page.continuation.is_some();
        Ok(page)
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        self.completed = true;
        let _ = self.attempt.cancel_before_dispatch();
        abandon_evidence(&self.inner, self.attempt.id());
        Ok(())
    }
}

#[async_trait]
impl WriteTransaction for InMemoryWriteTransaction {
    fn attempt(&self) -> AttemptId {
        self.attempt.id()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.stage(Mutation::Put {
            key,
            value,
            precondition,
        })
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.stage(Mutation::Delete { key, precondition })
    }

    async fn commit(mut self: Box<Self>) -> CommitOutcome {
        let hold = self
            .post_dispatch_hold
            .lock()
            .expect("in-memory post-dispatch hold")
            .take();
        // Dispatch is recorded before anything can touch storage: from here a
        // dropped handle is a cleanup debt, not a free slot.
        if let Err(error) = self.attempt.mark_dispatched() {
            return CommitOutcome::DefiniteFailure(error);
        }
        let outcome = if let Some(hold) = hold {
            self.commit_after_post_dispatch(hold).await
        } else {
            let mut inner = self.inner.lock().expect("in-memory state store");
            apply_commit(
                &mut inner,
                self.attempt.id(),
                self.base_revision,
                &self.mutations,
            )
        };
        self.completed = true;
        publish_witnessed_outcome(&self.inner, &self.attempt, &outcome);
        outcome
    }
}

impl InMemoryWriteTransaction {
    /// Applies the commit on a worker the caller does not own.
    ///
    /// The worker writes evidence; only the caller-facing future publishes a
    /// verdict. That split is the whole point: if the caller is cancelled or
    /// its answer is lost, the attempt stays in doubt and has to be adjudicated
    /// from evidence, exactly as it would across a real connection.
    async fn commit_after_post_dispatch(&self, hold: Arc<InMemoryCommitHold>) -> CommitOutcome {
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut guard = CommitAbandonGuard {
            cancelled: Arc::clone(&cancelled),
            inner: Arc::clone(&self.inner),
            attempt: self.attempt.id(),
            armed: true,
        };
        let (outcome_tx, outcome_rx) = oneshot::channel();
        let inner = Arc::clone(&self.inner);
        let mutations = self.mutations.clone();
        let attempt = self.attempt.id();
        let base_revision = self.base_revision;
        tokio::spawn(async move {
            hold.wait_for_progress().await;
            if cancelled.load(Ordering::Acquire) {
                return;
            }
            let outcome = {
                let mut inner = inner.lock().expect("in-memory state store");
                apply_commit(&mut inner, attempt, base_revision, &mutations)
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

/// Publishes the terminal the caller actually witnessed, then drops the
/// evidence that terminal was derived from.
///
/// Order matters and is the contract: the proof is published first, so a later
/// reader reads a recorded verdict rather than re-deriving one from evidence
/// that is on its way out. Releasing here rather than leaving it to
/// `AttemptSupervisor::drain_abandoned_attempts` is what keeps a provider's
/// private evidence bounded — abandonment is the exception, not the norm, and a
/// provider that only cleans up on abandonment grows forever.
///
/// [`CommitOutcome::CommitUnknown`] publishes nothing on purpose: an ambiguous
/// answer is not a verdict, and the attempt is resolved later from evidence,
/// which therefore must survive.
fn publish_witnessed_outcome(
    inner: &Mutex<Inner>,
    attempt: &WriteAttempt,
    outcome: &CommitOutcome,
) {
    let verdict = match outcome {
        CommitOutcome::Committed(receipt) => AttemptOutcome::Committed(receipt.clone()),
        // All three are statements that the write can no longer land: the
        // in-memory commit path either never applied or refused the mutation.
        CommitOutcome::Conflict(_)
        | CommitOutcome::TransientBeforeCommit(_)
        | CommitOutcome::DefiniteFailure(_) => AttemptOutcome::NotCommitted,
        CommitOutcome::CommitUnknown(_) => return,
    };
    attempt
        .settle(verdict)
        .expect("in-memory attempt settles its witnessed outcome exactly once");
    inner
        .lock()
        .expect("in-memory state store")
        .commits
        .remove(&attempt.id());
}

/// Records that an attempt can no longer commit, without ever overwriting a
/// proof that it already did.
fn abandon_evidence(inner: &Mutex<Inner>, attempt: AttemptId) {
    let mut inner = inner.lock().expect("in-memory state store");
    match inner.commits.get(&attempt) {
        Some(AttemptOutcome::Committed(_)) => {}
        _ => {
            inner.commits.insert(attempt, AttemptOutcome::NotCommitted);
        }
    }
}

struct CommitAbandonGuard {
    cancelled: Arc<AtomicBool>,
    inner: Arc<Mutex<Inner>>,
    attempt: AttemptId,
    armed: bool,
}

impl Drop for CommitAbandonGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Order matters only in one direction: whoever takes the lock first
        // wins, and `apply_commit` refuses to apply over a recorded denial, so
        // the worker cannot resurrect an abandoned attempt.
        self.cancelled.store(true, Ordering::Release);
        let mut inner = self.inner.lock().expect("in-memory state store");
        if matches!(
            inner.commits.get(&self.attempt),
            Some(AttemptOutcome::Unresolved)
        ) {
            inner
                .commits
                .insert(self.attempt, AttemptOutcome::NotCommitted);
        }
    }
}

fn apply_commit(
    inner: &mut Inner,
    attempt: AttemptId,
    base_revision: u64,
    mutations: &[Mutation],
) -> CommitOutcome {
    match inner.commits.get(&attempt) {
        Some(AttemptOutcome::Committed(receipt)) => {
            return CommitOutcome::Committed(receipt.clone());
        }
        Some(AttemptOutcome::NotCommitted) => {
            return CommitOutcome::DefiniteFailure(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "write attempt is terminally not committed",
            ));
        }
        Some(AttemptOutcome::Unresolved) | None => {}
    }
    if inner.revision != base_revision {
        inner.commits.insert(attempt, AttemptOutcome::NotCommitted);
        return CommitOutcome::Conflict(StateStoreError::new(
            StateStoreErrorKind::Conflict,
            "in-memory state store snapshot conflict",
        ));
    }
    if !preconditions_hold(&inner.records, mutations) {
        inner.commits.insert(attempt, AttemptOutcome::NotCommitted);
        return CommitOutcome::Conflict(StateStoreError::new(
            StateStoreErrorKind::PreconditionFailed,
            "in-memory state store precondition failed",
        ));
    }
    inner.revision = inner.revision.saturating_add(1);
    let revision = revision_token(inner.revision);
    let revision_number = inner.revision;
    for mutation in mutations {
        match mutation {
            Mutation::Put { key, value, .. } => {
                inner.records.insert(
                    key.clone(),
                    StateRecord {
                        key: key.clone(),
                        value: value.clone(),
                        version: version_token(revision_number),
                    },
                );
            }
            Mutation::Delete { key, .. } => {
                inner.records.remove(key);
            }
        }
    }
    let receipt = CommitReceipt { attempt, revision };
    inner
        .commits
        .insert(attempt, AttemptOutcome::Committed(receipt.clone()));
    CommitOutcome::Committed(receipt)
}

fn range_page(
    records: &BTreeMap<Key, StateRecord>,
    limits: &StateStoreLimits,
    request: &RangeRequest,
) -> Result<RangePage, StateStoreError> {
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

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;
    use crate::conformance::{
        FaultGate, FaultInjectingStateStore, FaultStateStoreFactory, PostDispatchControl,
        PostDispatchController, PostDispatchScenario, StateStoreFactory, StateStoreFaultFixture,
        run_attempt_suite, run_basic_suite, run_fault_suite,
    };

    /// Small on purpose: the attempt suite fills the instance to its ceiling,
    /// and a 1024-slot default would say nothing about the accounting.
    const CONFORMANCE_ATTEMPT_CAPACITY: usize = 4;

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

    fn conformance_limits() -> StateStoreLimits {
        StateStoreLimits {
            max_key_bytes: 64,
            max_value_bytes: 64,
            max_page_size: 10,
            max_transaction_operations: 8,
            max_transaction_bytes: 300,
            ..StateStoreLimits::default()
        }
    }

    fn reference_store() -> Arc<InMemoryStateStore> {
        Arc::new(InMemoryStateStore::with_limits_and_capacity(
            "test-cluster",
            conformance_limits(),
            NonZeroUsize::new(CONFORMANCE_ATTEMPT_CAPACITY).expect("conformance capacity"),
        ))
    }

    fn factory() -> StateStoreFactory {
        Rc::new(|| {
            Box::pin(async {
                let store: Arc<dyn StateStore> = reference_store();
                Ok(store)
            })
        })
    }

    fn fault_factory() -> FaultStateStoreFactory {
        Rc::new(|| {
            Box::pin(async {
                let in_memory = reference_store();
                let store: Arc<dyn StateStore> = in_memory.clone();
                let fault = FaultInjectingStateStore::new(store);
                let controller: Arc<dyn PostDispatchController> =
                    Arc::new(InMemoryPostDispatchController {
                        fault: Arc::clone(&fault),
                        store: in_memory,
                    });
                Ok(StateStoreFaultFixture::new(fault, controller))
            })
        })
    }

    #[tokio::test]
    async fn reference_store_satisfies_the_basic_suite() {
        run_basic_suite(&factory()).await;
    }

    #[tokio::test]
    async fn reference_store_satisfies_the_attempt_suite() {
        run_attempt_suite(&factory()).await;
    }

    #[tokio::test]
    async fn reference_store_satisfies_the_fault_suite() {
        run_fault_suite(&fault_factory()).await;
    }

    #[test]
    fn default_limits_are_not_relaxed() {
        let store = InMemoryStateStore::new("test-cluster");
        assert_eq!(store.limits(), &StateStoreLimits::default());
        assert_eq!(
            store.attempts().capacity(),
            DEFAULT_MAX_OUTSTANDING_ATTEMPTS
        );
    }

    /// The invariant the public surface cannot reach: once evidence is gone, an
    /// adjudicator must go back to saying it does not know.
    ///
    /// `release_evidence` only ever runs after every handle for an attempt is
    /// dropped, so no `CommitObservation` can be alive to ask afterwards. The
    /// rule is still real -- a provider that answers a denial from a missing
    /// row is wrong -- so it is pinned here, directly against the callback.
    /// A settled attempt must not leave evidence behind. Cleaning up only on
    /// abandonment would let the private evidence table grow for the entire
    /// life of an instance, which is one of the things this contract exists to
    /// stop.
    #[tokio::test]
    async fn a_settled_attempt_leaves_no_evidence_behind() {
        let store = InMemoryStateStore::new("evidence-bound");
        for _ in 0..8 {
            let (attempt, observation) = store.attempts().reserve().expect("reserve");
            let mut transaction = store
                .begin_write(attempt, "bounded evidence")
                .await
                .expect("begin");
            transaction
                .put(
                    Key::try_from(Bytes::from_static(b"k")).expect("key"),
                    Value::try_from(Bytes::from_static(b"v")).expect("value"),
                    Precondition::Any,
                )
                .await
                .expect("put");
            assert!(matches!(
                transaction.commit().await,
                CommitOutcome::Committed(_)
            ));
            assert!(observation.peek().expect("peek").is_some());
        }
        assert_eq!(
            store.inner.lock().expect("inner").commits.len(),
            0,
            "settled attempts must release their evidence rather than accumulate it"
        );
        assert_eq!(store.attempts().abandoned(), 0);
    }

    #[tokio::test]
    async fn absent_evidence_adjudicates_unresolved_never_not_committed() {
        let store = InMemoryStateStore::new("evidence-cluster");
        let evidence = InMemoryEvidence {
            inner: Arc::clone(&store.inner),
        };
        let (attempt, _observation) = store.attempts().reserve().expect("reserve");
        let id = attempt.id();

        // Never registered: nothing is known, and nothing may be claimed.
        assert_eq!(
            evidence.adjudicate(id).await.expect("adjudicate unknown"),
            AttemptOutcome::Unresolved
        );

        // Registered but undecided is still not a denial.
        let transaction = store
            .begin_write(attempt, "evidence probe")
            .await
            .expect("begin write");
        assert_eq!(transaction.attempt(), id);
        assert_eq!(
            evidence
                .adjudicate(id)
                .await
                .expect("adjudicate registered"),
            AttemptOutcome::Unresolved
        );

        // Aborting proves the denial, so now it may be stated.
        transaction.abort().await.expect("abort");
        assert_eq!(
            evidence.adjudicate(id).await.expect("adjudicate aborted"),
            AttemptOutcome::NotCommitted
        );

        // And releasing the proof takes the statement away again rather than
        // leaving a denial behind for the next reader to trust.
        evidence.release_evidence(id).await.expect("release");
        assert_eq!(
            evidence.adjudicate(id).await.expect("adjudicate released"),
            AttemptOutcome::Unresolved
        );
    }
}
