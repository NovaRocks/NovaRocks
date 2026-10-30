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

// Design: ADR-0143 (docs/adr/ADR-0143-state-store-answers-only-issued-attempts.md)

//! Write-attempt issuance, capacity, and commit observation.
//!
//! This module owns the one state machine every provider shares. A provider
//! implements physical transactions and an in-doubt adjudication callback; it
//! does not implement issuance, capacity accounting, or outcome publication,
//! because those are storage-independent and were previously reimplemented
//! per provider with subtly different rules.
//!
//! # Why identity is issued rather than supplied
//!
//! A consumer used to mint a transaction id from any UUID it liked, which made
//! two things possible that should not be: asking a store about an id it never
//! issued, and reusing an id across a restart as if it were a recovery ticket.
//! An [`AttemptId`] is issued by one open instance, carries that instance's
//! scope, and cannot be constructed from bytes. A capability from a previous
//! instance is rejected rather than silently answered.
//!
//! # What an outcome means
//!
//! [`AttemptOutcome::Committed`] and [`AttemptOutcome::NotCommitted`] are
//! terminal and never flip. `NotCommitted` is a proof obligation: a provider
//! may only return it once the attempt can no longer commit *and* the evidence
//! it based that on was readable at decision time. Absent evidence is not
//! absence of a commit. When nothing can be proven the honest answer is
//! [`AttemptOutcome::Unresolved`], which grants no right to run the work again.

use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use uuid::Uuid;

use super::contract::CommitReceipt;
use super::error::{StateStoreError, StateStoreErrorKind};
use super::execution::{
    StateStoreExecutionContext, StateStoreExecutionFailure, StateStoreOriginalUnsupported,
};

/// Default ceiling on attempts a single open instance keeps charged at once.
///
/// A slot is charged from reservation until the outcome is terminal and every
/// handle is gone; an attempt abandoned mid-flight stays charged until its
/// evidence is released. It is a resource bound, not a contract constant: a
/// provider may
/// choose another value, and tests routinely run at 1 or 2 to exercise
/// saturation.
pub const DEFAULT_MAX_OUTSTANDING_ATTEMPTS: usize = 1024;

/// Identifies one opened store instance.
///
/// Minted fresh on every open. There is deliberately no way to build one from
/// bytes or to persist and restore it, so a capability cannot outlive the
/// instance that issued it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InstanceScope(Uuid);

impl InstanceScope {
    /// Mints a scope for a newly opened instance.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for InstanceScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Identifies one write attempt inside one instance scope.
///
/// The sequence is a checked counter, not a clock reading: ordering by time is
/// never implied, and two attempts from different instances are never equal
/// even if their sequences match.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AttemptId {
    scope: InstanceScope,
    sequence: u64,
}

impl AttemptId {
    pub const fn scope(&self) -> InstanceScope {
        self.scope
    }

    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl AttemptId {
    /// Bytes a provider may key private evidence by.
    ///
    /// Fixed width and stable by contract. A provider that derived its own key
    /// from [`std::fmt::Display`] would be betting on the formatting of a UUID
    /// never changing, and a change would silently shift both its key width and
    /// any byte accounting built on it.
    pub const STORAGE_KEY_BYTES: usize = 57;

    pub fn storage_key(&self) -> String {
        // 36 (hyphenated UUID) + 1 + 20 (zero-padded u64) = STORAGE_KEY_BYTES.
        format!("{}:{:020}", self.scope.0.as_hyphenated(), self.sequence)
    }
}

impl std::fmt::Display for AttemptId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.scope, self.sequence)
    }
}

/// What is known about a write attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptOutcome {
    /// Proven committed. Terminal.
    Committed(CommitReceipt),
    /// Proven not committed, and it can never commit. Terminal.
    NotCommitted,
    /// Not enough evidence to decide. Not terminal, and not a licence to retry.
    Unresolved,
}

impl AttemptOutcome {
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Committed(_) | Self::NotCommitted)
    }
}

/// Decides attempts whose outcome the caller did not witness, and releases the
/// private evidence a decision rests on.
///
/// Implemented by the provider, driven by [`AttemptSupervisor`]. The provider
/// chooses its own evidence mechanism; this contract only fixes what the answer
/// has to mean.
#[async_trait::async_trait]
pub trait InDoubtAdjudicator: Send + Sync {
    /// Decides one dispatched attempt.
    ///
    /// Returning [`AttemptOutcome::NotCommitted`] asserts all of: the attempt
    /// can no longer commit, the physical worker or connection has finished
    /// whatever it was doing, and the evidence the decision rests on was
    /// readable at decision time. A failed read, released evidence, or work
    /// still in flight must produce [`AttemptOutcome::Unresolved`], never
    /// `NotCommitted`.
    async fn adjudicate(&self, attempt: AttemptId) -> Result<AttemptOutcome, StateStoreError>;

    /// Releases the private evidence for an attempt whose terminal outcome is
    /// already published.
    ///
    /// The supervisor calls this only after the outcome is safely recorded, so
    /// a later reader can never mistake released evidence for proof that
    /// nothing committed.
    async fn release_evidence(&self, attempt: AttemptId) -> Result<(), StateStoreError>;

    /// Uses the same installed original owner for evidence observation.
    /// Unsupported providers refuse explicitly; no legacy I/O is dispatched.
    async fn adjudicate_in(
        &self,
        _attempt: AttemptId,
        _execution: &StateStoreExecutionContext,
    ) -> Result<AttemptOutcome, StateStoreExecutionFailure> {
        Err(StateStoreOriginalUnsupported.into_original_failure())
    }

    /// Releases evidence through that same original owner. Cancellation of this
    /// borrower never removes the supervisor's outstanding release obligation.
    async fn release_evidence_in(
        &self,
        _attempt: AttemptId,
        _execution: &StateStoreExecutionContext,
    ) -> Result<(), StateStoreExecutionFailure> {
        Err(StateStoreOriginalUnsupported.into_original_failure())
    }
}

// Closed API selection, not a second attempt algorithm or execution owner.
// Returning the async-trait future directly adds no wrapper future allocation.
type EvidenceFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;
trait EvidenceMode: Sync {
    type Error: Send;
    fn from_store(error: StateStoreError) -> Self::Error;
    fn kind(error: &Self::Error) -> Option<StateStoreErrorKind>;
    fn adjudicate<'a>(
        &'a self,
        adjudicator: &'a dyn InDoubtAdjudicator,
        attempt: AttemptId,
    ) -> EvidenceFuture<'a, AttemptOutcome, Self::Error>;
    fn release<'a>(
        &'a self,
        adjudicator: &'a dyn InDoubtAdjudicator,
        attempt: AttemptId,
    ) -> EvidenceFuture<'a, (), Self::Error>;
}
struct LegacyEvidence;
impl EvidenceMode for LegacyEvidence {
    type Error = StateStoreError;
    fn from_store(error: StateStoreError) -> Self::Error {
        error
    }
    fn kind(error: &Self::Error) -> Option<StateStoreErrorKind> {
        Some(error.kind())
    }
    fn adjudicate<'a>(
        &'a self,
        adjudicator: &'a dyn InDoubtAdjudicator,
        attempt: AttemptId,
    ) -> EvidenceFuture<'a, AttemptOutcome, Self::Error> {
        adjudicator.adjudicate(attempt)
    }
    fn release<'a>(
        &'a self,
        adjudicator: &'a dyn InDoubtAdjudicator,
        attempt: AttemptId,
    ) -> EvidenceFuture<'a, (), Self::Error> {
        adjudicator.release_evidence(attempt)
    }
}
struct OriginalEvidence<'a>(&'a StateStoreExecutionContext);
impl EvidenceMode for OriginalEvidence<'_> {
    type Error = StateStoreExecutionFailure;
    fn from_store(error: StateStoreError) -> Self::Error {
        error.into_original_failure()
    }
    fn kind(error: &Self::Error) -> Option<StateStoreErrorKind> {
        // Only this API's original error owns Transient semantics. A foreign
        // fact, diagnostic, or nested source is not a retry classification.
        error
            .downcast_ref::<StateStoreError>()
            .map(StateStoreError::kind)
    }
    fn adjudicate<'a>(
        &'a self,
        adjudicator: &'a dyn InDoubtAdjudicator,
        attempt: AttemptId,
    ) -> EvidenceFuture<'a, AttemptOutcome, Self::Error> {
        adjudicator.adjudicate_in(attempt, self.0)
    }
    fn release<'a>(
        &'a self,
        adjudicator: &'a dyn InDoubtAdjudicator,
        attempt: AttemptId,
    ) -> EvidenceFuture<'a, (), Self::Error> {
        adjudicator.release_evidence_in(attempt, self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SlotState {
    /// Capacity is charged; nothing has touched storage.
    Reserved,
    /// A commit was dispatched. The outcome is unknown until settled.
    Dispatched,
    /// Terminal, published.
    Settled(AttemptOutcome),
    /// Never dispatched, so provably without write effect.
    CancelledBeforeDispatch,
}

struct Slot {
    id: AttemptId,
    state: Mutex<SlotState>,
    // Only the supervisor-adjudicated path owns this release. Provider-owned
    // settle paths retain their existing physical release responsibility.
    evidence_pending: AtomicBool,
    supervisor: Arc<SupervisorInner>,
}

impl Slot {
    fn read(&self) -> Result<SlotState, StateStoreError> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| internal("state store attempt slot lock is poisoned"))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if self.evidence_pending.load(Ordering::Acquire) {
            // The release guard transferred this original charge to debt
            // before its last strong alias could exit. A drain whose Weak
            // upgrade loses to this Drop is the sole remaining refund owner.
            return;
        }
        // The last handle for this attempt is gone. A settled or never
        // dispatched attempt owes nothing more and frees its slot. A dispatched
        // attempt nobody settled is a real debt: it may still be committing, so
        // the slot stays charged and the supervisor keeps it for cleanup rather
        // than silently reclaiming capacity.
        let state = self.state.get_mut().map(|state| state.clone());
        match state {
            Ok(SlotState::Settled(_)) | Ok(SlotState::CancelledBeforeDispatch) => {
                self.supervisor.release_slot();
            }
            Ok(SlotState::Reserved) => {
                // Reserved and dropped without ever being handed to a provider:
                // no storage was touched, so the capacity is free.
                self.supervisor.release_slot();
            }
            Ok(SlotState::Dispatched) => {
                self.supervisor.record_debt(CleanupDebt::abandoned(self.id))
            }
            Err(_) => self.supervisor.record_debt(CleanupDebt::abandoned(self.id)),
        }
    }
}

struct SupervisorInner {
    scope: InstanceScope,
    capacity: usize,
    next_sequence: AtomicU64,
    outstanding: Mutex<usize>,
    /// Attempts whose evidence still has to be released, or that were dropped
    /// while dispatched. They stay charged against capacity until drained.
    debt: Mutex<VecDeque<CleanupDebt>>,
    adjudicator: Arc<dyn InDoubtAdjudicator>,
}

impl SupervisorInner {
    fn release_slot(&self) {
        if let Ok(mut outstanding) = self.outstanding.lock() {
            *outstanding = outstanding.saturating_sub(1);
        }
    }

    fn record_debt(&self, entry: CleanupDebt) {
        if let Ok(mut debt) = self.debt.lock() {
            debt.push_back(entry);
        }
    }
}

// Each entry owns the original outstanding charge only after the Slot exits.
// A Weak points back to a still-observable adjudicated terminal without making
// a supervisor -> Slot -> supervisor ownership cycle.
struct CleanupDebt {
    id: AttemptId,
    slot: Weak<Slot>,
}

impl CleanupDebt {
    fn abandoned(id: AttemptId) -> Self {
        Self {
            id,
            slot: Weak::new(),
        }
    }

    fn released(self, supervisor: &SupervisorInner) {
        if let Some(slot) = self.slot.upgrade() {
            // Keep this strong alias until the flag is clear. The final Slot
            // Drop, possibly this one, is the only refund while aliases live.
            slot.evidence_pending.store(false, Ordering::Release);
            drop(slot);
        } else {
            // Slot::drop sees evidence_pending and does not refund or requeue.
            supervisor.release_slot();
        }
    }
}

// This guard borrows a strong original Slot. Its cancellation transfer always
// happens before that alias can be dropped; an answer alone is not a release.
struct AdjudicatedRelease<'a> {
    slot: &'a Arc<Slot>,
    pending: bool,
}

impl<'a> AdjudicatedRelease<'a> {
    fn new(slot: &'a Arc<Slot>) -> Self {
        slot.evidence_pending.store(true, Ordering::Release);
        Self {
            slot,
            pending: true,
        }
    }

    fn released(mut self) {
        self.slot.evidence_pending.store(false, Ordering::Release);
        self.pending = false;
    }
}

impl Drop for AdjudicatedRelease<'_> {
    fn drop(&mut self) {
        if self.pending {
            self.slot.supervisor.record_debt(CleanupDebt {
                id: self.slot.id,
                slot: Arc::downgrade(self.slot),
            });
        }
    }
}

// One drain exclusively owns its taken batch. The front remains in this guard
// across the provider await, so cancellation returns both current and tail.
struct PendingDebts<'a> {
    supervisor: &'a SupervisorInner,
    entries: VecDeque<CleanupDebt>,
}

impl Drop for PendingDebts<'_> {
    fn drop(&mut self) {
        if !self.entries.is_empty()
            && let Ok(mut debt) = self.supervisor.debt.lock()
        {
            debt.append(&mut self.entries);
        }
    }
}

/// Issues write attempts for one open instance and accounts for their capacity.
///
/// One supervisor belongs to one opened store. Providers construct it during
/// open and expose it through [`crate::StateStore::attempts`].
#[derive(Clone)]
pub struct AttemptSupervisor {
    inner: Arc<SupervisorInner>,
}

impl AttemptSupervisor {
    /// Builds a supervisor for a freshly opened instance.
    pub fn new(capacity: NonZeroUsize, adjudicator: Arc<dyn InDoubtAdjudicator>) -> Self {
        Self {
            inner: Arc::new(SupervisorInner {
                scope: InstanceScope::new(),
                capacity: capacity.get(),
                next_sequence: AtomicU64::new(1),
                outstanding: Mutex::new(0),
                debt: Mutex::new(VecDeque::new()),
                adjudicator,
            }),
        }
    }

    pub fn scope(&self) -> InstanceScope {
        self.inner.scope
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Attempts currently charged against capacity.
    pub fn outstanding(&self) -> usize {
        self.inner.outstanding.lock().map_or(0, |count| *count)
    }

    /// Queued attempts whose evidence still has to be released, including a
    /// cancelled or failed release after adjudication. See
    /// [`Self::drain_abandoned_attempts`].
    pub fn abandoned(&self) -> usize {
        self.inner.debt.lock().map_or(0, |debt| debt.len())
    }

    /// Reserves capacity for one write attempt.
    ///
    /// This touches no storage. It fails with [`StateStoreErrorKind::Saturated`]
    /// when the instance already holds its ceiling, which happens strictly
    /// before any write effect and is therefore safe for the caller to retry
    /// within its own budget.
    pub fn reserve(&self) -> Result<(WriteAttempt, CommitObservation), StateStoreError> {
        {
            let mut outstanding = self
                .inner
                .outstanding
                .lock()
                .map_err(|_| internal("state store attempt accounting lock is poisoned"))?;
            if *outstanding >= self.inner.capacity {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::Saturated,
                    "state store instance is at its outstanding attempt ceiling",
                ));
            }
            *outstanding += 1;
        }

        // `fetch_add` would wrap and start reissuing identities this instance
        // has already handed out, so the counter is advanced only while a next
        // value exists.
        let sequence = match self.inner.next_sequence.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| current.checked_add(1),
        ) {
            Ok(sequence) => sequence,
            Err(_) => {
                self.inner.release_slot();
                return Err(internal(
                    "state store instance exhausted its attempt sequence",
                ));
            }
        };

        let slot = Arc::new(Slot {
            id: AttemptId {
                scope: self.inner.scope,
                sequence,
            },
            state: Mutex::new(SlotState::Reserved),
            evidence_pending: AtomicBool::new(false),
            supervisor: Arc::clone(&self.inner),
        });

        Ok((
            WriteAttempt {
                slot: Arc::clone(&slot),
            },
            CommitObservation { slot },
        ))
    }

    /// Releases evidence for attempts that were dispatched and then abandoned.
    ///
    /// This is **not** the normal path. A provider that settles an attempt is
    /// already inside an async context and must release that attempt's evidence
    /// itself, immediately after publishing the outcome. Leaving it to this
    /// method would let a provider's private evidence grow without bound, which
    /// is one of the things the attempt contract exists to prevent.
    ///
    /// This also owns a failed or cancelled supervisor-adjudicated release.
    /// Otherwise it is the case with nobody left to do it: every handle for a
    /// dispatched attempt was dropped before anything settled. The slot stays
    /// charged until the release succeeds, because reclaiming it early would let
    /// new work evict evidence a possibly-committed attempt still needs.
    ///
    /// Cleanup is driven, never spawned: the supervisor owns no task, so a host
    /// calls this on a cadence it can account for, and at shutdown. Entries
    /// whose release fails stay queued and stay charged. Returns how many were
    /// released.
    pub async fn drain_abandoned_attempts(&self) -> Result<usize, StateStoreError> {
        self.drain_with(&LegacyEvidence).await
    }

    /// Borrows the installed original owner for the same debt pass. The context
    /// and its absolute deadline are unchanged; this does not spawn a worker or
    /// authorize legacy evidence I/O if original execution is unavailable.
    pub async fn drain_abandoned_attempts_in(
        &self,
        execution: &StateStoreExecutionContext,
    ) -> Result<usize, StateStoreExecutionFailure> {
        self.drain_with(&OriginalEvidence(execution)).await
    }

    async fn drain_with<M: EvidenceMode>(&self, mode: &M) -> Result<usize, M::Error> {
        let mut pending = PendingDebts {
            supervisor: &self.inner,
            entries: {
                let mut debt = self.inner.debt.lock().map_err(|_| {
                    M::from_store(internal("state store cleanup debt lock is poisoned"))
                })?;
                std::mem::take(&mut *debt)
            },
        };

        let mut released = 0_usize;
        let mut blocked: Option<M::Error> = None;
        while let Some(entry) = pending.entries.front() {
            let attempt = entry.id;
            match mode.release(self.inner.adjudicator.as_ref(), attempt).await {
                Ok(()) => {
                    // No await separates successful release, removal, and the
                    // original charge disposition.
                    pending.entries.pop_front().unwrap().released(&self.inner);
                    released += 1;
                }
                Err(error) => {
                    // Requeue: the slot stays charged, which is the point.
                    // Reclaiming capacity here would let new work evict evidence
                    // that may still be needed.
                    self.inner.record_debt(pending.entries.pop_front().unwrap());
                    // "Not releasable yet" is an ordinary state, not a fault: a
                    // provider says it while its own worker is still in flight.
                    // Stopping the pass on it would let one busy attempt hide
                    // every other release that was ready to happen.
                    if M::kind(&error) != Some(StateStoreErrorKind::Transient) && blocked.is_none()
                    {
                        blocked = Some(error);
                    }
                }
            }
        }

        match blocked {
            // Progress is reported even when something failed, so a caller can
            // see the queue moving rather than only that it errored.
            Some(error) if released == 0 => Err(error),
            _ => Ok(released),
        }
    }
}

impl std::fmt::Debug for AttemptSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AttemptSupervisor")
            .field("scope", &self.inner.scope)
            .field("capacity", &self.inner.capacity)
            .field("outstanding", &self.outstanding())
            .field("abandoned", &self.abandoned())
            .finish()
    }
}

/// The right to run exactly one write transaction body.
///
/// Not `Clone`: one reservation is one attempt. A provider consumes it when it
/// begins a transaction, and the resulting transaction settles it.
pub struct WriteAttempt {
    slot: Arc<Slot>,
}

impl WriteAttempt {
    pub fn id(&self) -> AttemptId {
        self.slot.id
    }

    /// Rejects a capability that a different instance issued.
    ///
    /// A provider calls this before touching storage, so a handle retained
    /// across a reopen fails loudly instead of addressing a fresh instance.
    pub fn require_scope(&self, scope: InstanceScope) -> Result<(), StateStoreError> {
        if self.slot.id.scope == scope {
            return Ok(());
        }
        Err(StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            "write attempt was issued by a different state store instance",
        ))
    }

    /// Records that the attempt may now have a durable effect.
    ///
    /// A provider must call this **before** the first operation that could
    /// leave any trace a later reader might see, not merely before the commit
    /// call. Until it is called, [`CommitObservation::outcome`] answers
    /// `NotCommitted` on the strength of "nothing reached storage"; a provider
    /// that writes first and marks second makes that answer a lie.
    ///
    /// From here on, dropping without settling is an abandoned attempt rather
    /// than a free slot.
    pub fn mark_dispatched(&self) -> Result<(), StateStoreError> {
        let mut state = self
            .slot
            .state
            .lock()
            .map_err(|_| internal("state store attempt slot lock is poisoned"))?;
        match *state {
            SlotState::Reserved => {
                *state = SlotState::Dispatched;
                Ok(())
            }
            SlotState::Dispatched => Ok(()),
            SlotState::Settled(_) | SlotState::CancelledBeforeDispatch => {
                Err(StateStoreError::new(
                    StateStoreErrorKind::InvalidRequest,
                    "write attempt has already reached a terminal state",
                ))
            }
        }
    }

    /// Publishes a proven terminal outcome.
    ///
    /// A terminal outcome is written once. A second, differing settle is a
    /// contract break and is rejected rather than allowed to overwrite proof.
    pub fn settle(&self, outcome: AttemptOutcome) -> Result<(), StateStoreError> {
        if !outcome.is_terminal() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "only a terminal outcome can settle a write attempt",
            ));
        }
        let mut state = self
            .slot
            .state
            .lock()
            .map_err(|_| internal("state store attempt slot lock is poisoned"))?;
        match &*state {
            SlotState::Reserved | SlotState::Dispatched => {
                *state = SlotState::Settled(outcome);
                Ok(())
            }
            SlotState::Settled(existing) if *existing == outcome => Ok(()),
            SlotState::Settled(_) => Err(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "write attempt outcome cannot be replaced once it is terminal",
            )),
            SlotState::CancelledBeforeDispatch => Err(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "a write attempt cancelled before dispatch cannot settle",
            )),
        }
    }

    /// Records that nothing was dispatched, so the attempt provably had no
    /// write effect.
    pub fn cancel_before_dispatch(&self) -> Result<(), StateStoreError> {
        let mut state = self
            .slot
            .state
            .lock()
            .map_err(|_| internal("state store attempt slot lock is poisoned"))?;
        match &*state {
            SlotState::Reserved => {
                *state = SlotState::CancelledBeforeDispatch;
                Ok(())
            }
            SlotState::CancelledBeforeDispatch => Ok(()),
            // The observer got here first and recorded the same fact. Agreeing
            // with a published `NotCommitted` is not a contract break, the same
            // way re-settling an identical outcome is not: both statements say
            // the attempt had no effect.
            SlotState::Settled(AttemptOutcome::NotCommitted) => Ok(()),
            SlotState::Dispatched | SlotState::Settled(_) => Err(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "a dispatched write attempt cannot be cancelled as having no effect",
            )),
        }
    }
}

impl std::fmt::Debug for WriteAttempt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WriteAttempt")
            .field("id", &self.slot.id)
            .finish()
    }
}

/// A retained view of one attempt's outcome.
///
/// Cloning shares the attempt's slot rather than charging a second one, so a
/// caller may hand a copy to whoever needs to learn the result. The slot is
/// freed once every clone is gone, the outcome is terminal, and any
/// supervisor-adjudicated evidence release has completed.
#[derive(Clone)]
pub struct CommitObservation {
    slot: Arc<Slot>,
}

/// A read-only snapshot of the issued attempt's own publication state.
/// NotDispatched is an instantaneous fact, never a terminal absence proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitObservationState {
    NotDispatched,
    Dispatched,
    Terminal(AttemptOutcome),
}

impl CommitObservation {
    /// Reads the original slot without I/O, adjudication or settlement.
    /// A later provider dispatch may follow NotDispatched; this grants no right
    /// to retire the owner or declare its operation uncommitted.
    pub fn publication_state(&self) -> Result<CommitObservationState, StateStoreError> {
        Ok(match self.slot.read()? {
            SlotState::Reserved => CommitObservationState::NotDispatched,
            SlotState::Dispatched => CommitObservationState::Dispatched,
            SlotState::CancelledBeforeDispatch => {
                CommitObservationState::Terminal(AttemptOutcome::NotCommitted)
            }
            SlotState::Settled(outcome) => CommitObservationState::Terminal(outcome),
        })
    }
    pub fn id(&self) -> AttemptId {
        self.slot.id
    }

    /// Reads an already published terminal outcome without any I/O.
    ///
    /// Returns `None` while the attempt is reserved, dispatched but unsettled,
    /// or cancelled before dispatch.
    pub fn peek(&self) -> Result<Option<AttemptOutcome>, StateStoreError> {
        match self.slot.read()? {
            SlotState::Settled(outcome) => Ok(Some(outcome)),
            _ => Ok(None),
        }
    }

    /// Resolves the attempt, adjudicating with the provider if the outcome was
    /// dispatched but never witnessed.
    ///
    /// A published terminal outcome is returned as-is and is never
    /// re-adjudicated, so a decision cannot be revisited after the evidence it
    /// rested on has been released.
    pub async fn outcome(&self) -> Result<AttemptOutcome, StateStoreError> {
        self.outcome_with(&LegacyEvidence).await
    }

    /// Resolves through the same installed original owner without a legacy
    /// fallback. Cached terminal proofs still require no provider I/O. A failed
    /// or cancelled evidence release retains the same Slot/debt responsibility.
    pub async fn outcome_in(
        &self,
        execution: &StateStoreExecutionContext,
    ) -> Result<AttemptOutcome, StateStoreExecutionFailure> {
        self.outcome_with(&OriginalEvidence(execution)).await
    }

    async fn outcome_with<M: EvidenceMode>(&self, mode: &M) -> Result<AttemptOutcome, M::Error> {
        // Reading `Reserved` and answering from it has to be one step. A
        // provider whose commit runs on a task the caller does not own can mark
        // the attempt dispatched at any moment, so a lock taken only to read
        // would let this answer `NotCommitted` and the attempt reach storage
        // immediately afterwards -- the terminal would then flip to
        // `Unresolved` on the next question. Settling here instead makes the
        // answer decisive: the later `mark_dispatched` fails, and every
        // provider already turns that failure into a definite failure rather
        // than writing.
        {
            let mut state = self.slot.state.lock().map_err(|_| {
                M::from_store(internal("state store attempt slot lock is poisoned"))
            })?;
            match &*state {
                SlotState::Settled(outcome) => return Ok(outcome.clone()),
                SlotState::CancelledBeforeDispatch => return Ok(AttemptOutcome::NotCommitted),
                SlotState::Reserved => {
                    // Nothing reached storage: the ordering rule on
                    // `WriteAttempt::mark_dispatched` says a provider marks
                    // before the first operation that could leave a trace, and
                    // this claims the slot before it can.
                    *state = SlotState::Settled(AttemptOutcome::NotCommitted);
                    return Ok(AttemptOutcome::NotCommitted);
                }
                SlotState::Dispatched => {}
            }
        }

        let decided = mode
            .adjudicate(self.slot.supervisor.adjudicator.as_ref(), self.slot.id)
            .await?;
        if !decided.is_terminal() {
            return Ok(AttemptOutcome::Unresolved);
        }

        // Record the proof before anything can release the evidence behind it.
        let (published, release) = {
            let mut state = self.slot.state.lock().map_err(|_| {
                M::from_store(internal("state store attempt slot lock is poisoned"))
            })?;
            match &*state {
                // Someone else already published; theirs stands and theirs owns
                // the release.
                SlotState::Settled(existing) => return Ok(existing.clone()),
                _ => {
                    let release = AdjudicatedRelease::new(&self.slot);
                    *state = SlotState::Settled(decided.clone());
                    (decided, release)
                }
            }
        };

        // The proof is recorded, so the evidence it came from has done its job.
        // Releasing here is what keeps a provider's evidence bounded on this
        // path: the provider released what it witnessed itself, but an outcome
        // recovered through adjudication is published by the supervisor, so
        // only the supervisor knows the evidence is now spent.
        //
        // A failed release is not a failed read. The caller asked what happened
        // and now knows; the uncollected evidence becomes accounted debt rather
        // than a lost answer or a silently leaked row.
        if mode
            .release(self.slot.supervisor.adjudicator.as_ref(), self.slot.id)
            .await
            .is_ok()
        {
            release.released();
        }
        // Err and cancellation retain the same original charge as debt. Other
        // observers keep the published answer without creating another debt.
        Ok(published)
    }
}

impl std::fmt::Debug for CommitObservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommitObservation")
            .field("id", &self.slot.id)
            .finish()
    }
}

fn internal(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use bytes::Bytes;

    use super::*;
    use crate::contract::StoreRevision;

    /// Records what the supervisor asked of it and answers a scripted verdict.
    struct ScriptedAdjudicator {
        verdict: Mutex<AttemptOutcome>,
        adjudicated: AtomicUsize,
        released: AtomicUsize,
        release_failure: Mutex<Option<StateStoreErrorKind>>,
        /// How many further releases refuse. `usize::MAX` means "every one".
        refusals_left: AtomicUsize,
    }

    impl ScriptedAdjudicator {
        fn new(verdict: AttemptOutcome) -> Arc<Self> {
            Arc::new(Self {
                verdict: Mutex::new(verdict),
                adjudicated: AtomicUsize::new(0),
                released: AtomicUsize::new(0),
                release_failure: Mutex::new(None),
                refusals_left: AtomicUsize::new(usize::MAX),
            })
        }
    }

    #[async_trait::async_trait]
    impl InDoubtAdjudicator for ScriptedAdjudicator {
        async fn adjudicate(&self, _: AttemptId) -> Result<AttemptOutcome, StateStoreError> {
            self.adjudicated.fetch_add(1, Ordering::Relaxed);
            Ok(self.verdict.lock().expect("verdict").clone())
        }

        async fn release_evidence(&self, _: AttemptId) -> Result<(), StateStoreError> {
            let scripted = *self.release_failure.lock().expect("release flag");
            if let Some(kind) = scripted {
                let remaining = self.refusals_left.load(Ordering::Relaxed);
                if remaining > 0 {
                    if remaining != usize::MAX {
                        self.refusals_left.fetch_sub(1, Ordering::Relaxed);
                    }
                    return Err(StateStoreError::new(kind, "scripted release refusal"));
                }
            }
            self.released.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn supervisor(capacity: usize) -> (AttemptSupervisor, Arc<ScriptedAdjudicator>) {
        let adjudicator = ScriptedAdjudicator::new(AttemptOutcome::Unresolved);
        let supervisor = AttemptSupervisor::new(
            NonZeroUsize::new(capacity).expect("capacity"),
            Arc::clone(&adjudicator) as Arc<dyn InDoubtAdjudicator>,
        );
        (supervisor, adjudicator)
    }

    fn receipt(attempt: AttemptId) -> CommitReceipt {
        CommitReceipt {
            attempt,
            revision: StoreRevision::try_from(Bytes::from_static(b"r1")).expect("revision"),
        }
    }

    #[test]
    fn publication_snapshot_reads_only_the_actual_issued_state() {
        let (supervisor, _) = supervisor(2);
        let (attempt, observation) = supervisor.reserve().unwrap();
        assert_eq!(
            observation.publication_state().unwrap(),
            CommitObservationState::NotDispatched
        );
        assert!(observation.peek().unwrap().is_none());
        // Reading NotDispatched did not consume the owner's dispatch right.
        attempt.mark_dispatched().unwrap();
        assert_eq!(
            observation.publication_state().unwrap(),
            CommitObservationState::Dispatched
        );
        assert!(observation.peek().unwrap().is_none());
        let actual = receipt(attempt.id());
        attempt
            .settle(AttemptOutcome::Committed(actual.clone()))
            .unwrap();
        assert_eq!(
            observation.publication_state().unwrap(),
            CommitObservationState::Terminal(AttemptOutcome::Committed(actual))
        );
        let (withdrawn, observed) = supervisor.reserve().unwrap();
        withdrawn.cancel_before_dispatch().unwrap();
        assert_eq!(
            observed.publication_state().unwrap(),
            CommitObservationState::Terminal(AttemptOutcome::NotCommitted)
        );
    }

    #[test]
    fn attempt_identity_is_issued_and_scoped_to_one_instance() {
        let (first, _) = supervisor(4);
        let (second, _) = supervisor(4);
        let (a, _) = first.reserve().expect("reserve");
        let (b, _) = second.reserve().expect("reserve");

        assert_ne!(first.scope(), second.scope());
        assert_ne!(a.id(), b.id());
        // Same sequence, different instance: still not the same attempt.
        assert_eq!(a.id().sequence(), b.id().sequence());

        a.require_scope(first.scope()).expect("own scope accepted");
        let rejected = a
            .require_scope(second.scope())
            .expect_err("a foreign scope must be rejected");
        assert_eq!(rejected.kind(), StateStoreErrorKind::InvalidRequest);
    }

    #[test]
    fn sequences_advance_without_reading_a_clock() {
        let (supervisor, _) = supervisor(8);
        let (first, _keep_first) = supervisor.reserve().expect("reserve");
        let (second, _keep_second) = supervisor.reserve().expect("reserve");
        assert_eq!(first.id().sequence() + 1, second.id().sequence());
    }

    #[test]
    fn the_sequence_refuses_to_wrap_rather_than_reissue_an_identity() {
        let (supervisor, _) = supervisor(4);
        // Park the counter at its last usable value. `fetch_add` would wrap here
        // and silently start reissuing identities this instance already handed
        // out; `u64::MAX` is deliberately never issued so no successor is needed.
        supervisor
            .inner
            .next_sequence
            .store(u64::MAX - 1, Ordering::Relaxed);

        let (last, _keep) = supervisor
            .reserve()
            .expect("the final identity is issuable");
        assert_eq!(last.id().sequence(), u64::MAX - 1);

        let exhausted = supervisor.reserve().expect_err("there is no next identity");
        assert_eq!(exhausted.kind(), StateStoreErrorKind::Internal);
        // The refused reservation must not leak a slot either.
        assert_eq!(supervisor.outstanding(), 1);
    }

    #[test]
    fn saturation_is_its_own_retryable_classification() {
        let (supervisor, _) = supervisor(1);
        let (_attempt, _observation) = supervisor.reserve().expect("first reserve");

        let error = supervisor.reserve().expect_err("must saturate at capacity");
        assert_eq!(error.kind(), StateStoreErrorKind::Saturated);
        // Not the permanent classification: a caller is allowed to back off.
        assert_ne!(error.kind(), StateStoreErrorKind::LimitExceeded);
    }

    #[test]
    fn observation_clones_share_one_slot() {
        let (supervisor, _) = supervisor(1);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        let _clone_a = observation.clone();
        let _clone_b = observation.clone();

        assert_eq!(supervisor.outstanding(), 1);
        assert_eq!(observation.id(), attempt.id());
        // Cloning a handle must not consume the ceiling.
        assert_eq!(
            supervisor
                .reserve()
                .expect_err("still one attempt outstanding")
                .kind(),
            StateStoreErrorKind::Saturated
        );
    }

    #[test]
    fn a_settled_attempt_frees_its_slot_once_every_handle_is_gone() {
        let (supervisor, _) = supervisor(1);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");
        attempt
            .settle(AttemptOutcome::Committed(receipt(attempt.id())))
            .expect("settle");

        assert_eq!(supervisor.outstanding(), 1, "handles still held");
        drop(attempt);
        assert_eq!(supervisor.outstanding(), 1, "observation still held");
        drop(observation);
        assert_eq!(supervisor.outstanding(), 0);
        supervisor.reserve().expect("capacity is back");
    }

    #[test]
    fn a_dispatched_attempt_dropped_unsettled_keeps_its_slot_charged() {
        let (supervisor, _) = supervisor(1);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");
        drop(attempt);
        drop(observation);

        // It may still be committing. Reclaiming the slot here would let new
        // work evict evidence that is still needed.
        assert_eq!(supervisor.outstanding(), 1);
        assert_eq!(supervisor.abandoned(), 1);
        assert_eq!(
            supervisor.reserve().expect_err("still charged").kind(),
            StateStoreErrorKind::Saturated
        );
    }

    /// "Not releasable yet" is an ordinary state a provider reports while its
    /// own worker is still in flight. It must keep the slot charged without
    /// looking like a fault, or a host driving cleanup on a cadence would treat
    /// normal operation as an error.
    #[tokio::test]
    async fn a_release_that_is_not_yet_possible_is_not_reported_as_a_fault() {
        let (supervisor, adjudicator) = supervisor(1);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");
        drop(attempt);
        drop(observation);

        *adjudicator.release_failure.lock().expect("flag") = Some(StateStoreErrorKind::Transient);
        assert_eq!(
            supervisor
                .drain_abandoned_attempts()
                .await
                .expect("a transient refusal is not a drain failure"),
            0
        );
        assert_eq!(supervisor.outstanding(), 1, "still charged");
        assert_eq!(supervisor.abandoned(), 1, "still queued");

        *adjudicator.release_failure.lock().expect("flag") = None;
        assert_eq!(
            supervisor.drain_abandoned_attempts().await.expect("drain"),
            1
        );
        assert_eq!(supervisor.outstanding(), 0);
        assert_eq!(supervisor.abandoned(), 0);
    }

    /// A real failure with no progress to report does surface.
    #[tokio::test]
    async fn a_release_failure_with_no_progress_surfaces() {
        let (supervisor, adjudicator) = supervisor(2);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");
        drop(attempt);
        drop(observation);

        *adjudicator.release_failure.lock().expect("flag") = Some(StateStoreErrorKind::Corruption);
        let error = supervisor
            .drain_abandoned_attempts()
            .await
            .expect_err("a real failure with nothing released must surface");
        assert_eq!(error.kind(), StateStoreErrorKind::Corruption);
        assert_eq!(supervisor.outstanding(), 1, "stays charged");
        assert_eq!(supervisor.abandoned(), 1, "stays queued");
    }

    /// One stuck entry must not hide the releases that were ready.
    #[tokio::test]
    async fn one_blocked_release_does_not_hide_the_rest_of_the_queue() {
        let (supervisor, adjudicator) = supervisor(4);
        for _ in 0..3 {
            let (attempt, observation) = supervisor.reserve().expect("reserve");
            attempt.mark_dispatched().expect("dispatch");
            drop(attempt);
            drop(observation);
        }
        assert_eq!(supervisor.abandoned(), 3);

        *adjudicator.release_failure.lock().expect("flag") = Some(StateStoreErrorKind::Transient);
        adjudicator.refusals_left.store(1, Ordering::Relaxed);

        // Two of the three were ready; the pass reports that progress instead of
        // stopping at the first refusal.
        assert_eq!(
            supervisor.drain_abandoned_attempts().await.expect("drain"),
            2
        );
        assert_eq!(supervisor.abandoned(), 1);
        assert_eq!(supervisor.outstanding(), 1);
    }

    /// A provider keys durable evidence by this, so its width is part of the
    /// contract rather than a bet on how a UUID happens to print.
    #[test]
    fn the_storage_key_is_stable_and_fixed_width() {
        let (supervisor, _) = supervisor(4);
        let (first, _keep_first) = supervisor.reserve().expect("reserve");
        let (second, _keep_second) = supervisor.reserve().expect("reserve");

        let key = first.id().storage_key();
        assert_eq!(key.len(), AttemptId::STORAGE_KEY_BYTES);
        assert_eq!(
            second.id().storage_key().len(),
            AttemptId::STORAGE_KEY_BYTES
        );
        assert_ne!(key, second.id().storage_key());
        // Same value every time: a provider may key persisted rows by it.
        assert_eq!(key, first.id().storage_key());
        // Zero padding keeps byte accounting constant as the counter carries.
        assert!(key.ends_with("00000000000000000001"));
    }

    #[test]
    fn state_progression_is_one_way() {
        let (supervisor, _) = supervisor(4);
        let (attempt, _observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");

        // A dispatched attempt cannot claim it had no effect.
        assert_eq!(
            attempt
                .cancel_before_dispatch()
                .expect_err("dispatched work is not effect-free")
                .kind(),
            StateStoreErrorKind::Internal
        );

        attempt
            .settle(AttemptOutcome::NotCommitted)
            .expect("settle");
        assert_eq!(
            attempt
                .mark_dispatched()
                .expect_err("terminal attempts do not reopen")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
    }

    #[test]
    fn a_terminal_outcome_never_flips() {
        let (supervisor, _) = supervisor(4);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");
        attempt
            .settle(AttemptOutcome::NotCommitted)
            .expect("settle");

        // Idempotent restatement is fine; contradiction is not.
        attempt
            .settle(AttemptOutcome::NotCommitted)
            .expect("same verdict again");
        assert_eq!(
            attempt
                .settle(AttemptOutcome::Committed(receipt(attempt.id())))
                .expect_err("a proven verdict cannot be replaced")
                .kind(),
            StateStoreErrorKind::Internal
        );
        assert_eq!(
            observation.peek().expect("peek"),
            Some(AttemptOutcome::NotCommitted)
        );
    }

    #[test]
    fn only_a_terminal_verdict_may_settle() {
        let (supervisor, _) = supervisor(4);
        let (attempt, _observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");
        assert_eq!(
            attempt
                .settle(AttemptOutcome::Unresolved)
                .expect_err("unknown is not a verdict")
                .kind(),
            StateStoreErrorKind::InvalidRequest
        );
    }

    #[tokio::test]
    async fn a_published_outcome_is_never_re_adjudicated() {
        let (supervisor, adjudicator) = supervisor(4);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");
        let committed = AttemptOutcome::Committed(receipt(attempt.id()));
        attempt.settle(committed.clone()).expect("settle");

        assert_eq!(observation.outcome().await.expect("outcome"), committed);
        assert_eq!(observation.outcome().await.expect("outcome"), committed);
        assert_eq!(
            adjudicator.adjudicated.load(Ordering::Relaxed),
            0,
            "a published proof must not be re-derived after its evidence may be gone"
        );
    }

    #[tokio::test]
    async fn an_unwitnessed_dispatch_is_adjudicated_once_and_then_cached() {
        let (supervisor, adjudicator) = supervisor(4);
        *adjudicator.verdict.lock().expect("verdict") = AttemptOutcome::NotCommitted;
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");

        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(adjudicator.adjudicated.load(Ordering::Relaxed), 1);
    }

    /// An outcome recovered through adjudication is published by the
    /// supervisor, so only the supervisor can tell the provider its evidence is
    /// spent. Without this the witnessed path cleans up after itself and the
    /// adjudicated path leaks one record per ambiguous commit, forever.
    #[tokio::test]
    async fn an_adjudicated_terminal_releases_the_evidence_it_came_from() {
        let (supervisor, adjudicator) = supervisor(4);
        *adjudicator.verdict.lock().expect("verdict") = AttemptOutcome::NotCommitted;
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");

        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(
            adjudicator.released.load(Ordering::Relaxed),
            1,
            "publishing a recovered terminal must retire the evidence behind it"
        );

        // Reading again is served from the published proof, so it neither
        // re-adjudicates nor releases a second time.
        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(adjudicator.adjudicated.load(Ordering::Relaxed), 1);
        assert_eq!(adjudicator.released.load(Ordering::Relaxed), 1);
    }

    /// A release that fails must not turn a known answer into an error, and
    /// must not vanish either.
    #[tokio::test]
    async fn a_failed_release_becomes_debt_not_a_lost_answer() {
        let (supervisor, adjudicator) = supervisor(4);
        *adjudicator.verdict.lock().expect("verdict") = AttemptOutcome::NotCommitted;
        *adjudicator.release_failure.lock().expect("flag") = Some(StateStoreErrorKind::Transient);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");

        assert_eq!(
            observation
                .outcome()
                .await
                .expect("the answer still arrives"),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(
            supervisor.abandoned(),
            1,
            "the uncollected evidence is owed"
        );
    }

    #[tokio::test]
    async fn an_undecidable_attempt_stays_unresolved_and_is_asked_again() {
        let (supervisor, adjudicator) = supervisor(4);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");

        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::Unresolved
        );
        assert_eq!(observation.peek().expect("peek"), None, "not terminal");
        // Unknown is not cached as a verdict, so later evidence can still settle it.
        *adjudicator.verdict.lock().expect("verdict") = AttemptOutcome::NotCommitted;
        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(adjudicator.adjudicated.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn an_attempt_that_never_reached_storage_is_not_committed() {
        let (supervisor, adjudicator) = supervisor(4);
        let (attempt, observation) = supervisor.reserve().expect("reserve");

        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::NotCommitted
        );
        attempt.cancel_before_dispatch().expect("cancel");
        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(
            adjudicator.adjudicated.load(Ordering::Relaxed),
            0,
            "nothing was dispatched, so there is nothing to ask the provider"
        );
    }

    /// A provider whose commit runs on a task the caller does not own can mark
    /// an attempt dispatched at any moment. If observing a reserved slot only
    /// read it, the observer could answer `NotCommitted` and the very next
    /// instant the attempt could reach storage -- so the next question would
    /// answer `Unresolved`, flipping a terminal. Two providers here work that
    /// way, so this is the real shape, not a contrived interleaving.
    #[tokio::test]
    async fn observing_a_reserved_attempt_shuts_the_door_on_a_later_dispatch() {
        let (supervisor, adjudicator) = supervisor(4);
        let (attempt, observation) = supervisor.reserve().expect("reserve");

        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::NotCommitted
        );

        // The worker wakes up after the answer was given. It must be refused,
        // because the alternative is a durable write behind a published
        // "nothing happened".
        let refused = attempt
            .mark_dispatched()
            .expect_err("a dispatch after the attempt was answered must be refused");
        assert_eq!(refused.kind(), StateStoreErrorKind::InvalidRequest);

        // And the answer stands, without asking the provider anything.
        assert_eq!(
            observation.outcome().await.expect("outcome again"),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(
            observation.peek().expect("peek"),
            Some(AttemptOutcome::NotCommitted),
            "the answer was published, not merely returned"
        );
        assert_eq!(
            adjudicator.adjudicated.load(Ordering::Relaxed),
            0,
            "nothing reached storage, so the provider was never asked"
        );
    }

    #[tokio::test]
    async fn observation_and_cleanup_still_progress_at_capacity() {
        let (supervisor, _) = supervisor(1);
        let (attempt, observation) = supervisor.reserve().expect("reserve");
        attempt.mark_dispatched().expect("dispatch");
        attempt
            .settle(AttemptOutcome::NotCommitted)
            .expect("settle");

        // New admission is refused, yet control paths keep working.
        assert_eq!(
            supervisor.reserve().expect_err("at capacity").kind(),
            StateStoreErrorKind::Saturated
        );
        assert_eq!(
            observation.outcome().await.expect("outcome"),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(
            supervisor.drain_abandoned_attempts().await.expect("drain"),
            0
        );
    }

    // The provider retains real per-attempt evidence until its release await
    // passes the gate. Cancelling a caller cannot itself delete this evidence.
    struct GatedEvidence {
        evidence: Mutex<std::collections::HashSet<AttemptId>>,
        released: Mutex<Vec<AttemptId>>,
        adjudicated: AtomicUsize,
        refuse: AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
    }

    impl GatedEvidence {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                evidence: Mutex::new(std::collections::HashSet::new()),
                released: Mutex::new(Vec::new()),
                adjudicated: AtomicUsize::new(0),
                refuse: AtomicBool::new(false),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Semaphore::new(0),
            })
        }

        fn reserve(&self, supervisor: &AttemptSupervisor) -> (WriteAttempt, CommitObservation) {
            let (attempt, observation) = supervisor.reserve().expect("original reservation");
            assert!(self.evidence.lock().unwrap().insert(attempt.id()));
            attempt.mark_dispatched().expect("original dispatch");
            (attempt, observation)
        }
    }

    #[async_trait::async_trait]
    impl InDoubtAdjudicator for GatedEvidence {
        async fn adjudicate(&self, attempt: AttemptId) -> Result<AttemptOutcome, StateStoreError> {
            assert!(self.evidence.lock().unwrap().contains(&attempt));
            self.adjudicated.fetch_add(1, Ordering::Relaxed);
            Ok(AttemptOutcome::NotCommitted)
        }

        async fn release_evidence(&self, attempt: AttemptId) -> Result<(), StateStoreError> {
            self.entered.notify_one();
            if self.refuse.load(Ordering::Acquire) {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::Transient,
                    "release not ready",
                ));
            }
            self.release.acquire().await.expect("release gate").forget();
            assert!(
                self.evidence.lock().unwrap().remove(&attempt),
                "evidence is released once"
            );
            self.released.lock().unwrap().push(attempt);
            Ok(())
        }
    }

    #[tokio::test]
    async fn cancelled_concurrent_drains_return_current_and_remaining_original_debt() {
        let evidence = GatedEvidence::new();
        let supervisor = AttemptSupervisor::new(NonZeroUsize::new(4).unwrap(), evidence.clone());
        let mut ids = std::collections::HashSet::new();
        for _ in 0..3 {
            let (attempt, observation) = evidence.reserve(&supervisor);
            ids.insert(attempt.id());
            drop((attempt, observation));
        }
        let first = tokio::spawn({
            let supervisor = supervisor.clone();
            async move { supervisor.drain_abandoned_attempts().await }
        });
        evidence.entered.notified().await;
        // The other drain gets only newly abandoned debt, never the first
        // drain's in-flight entry or its unvisited tail.
        let (attempt, observation) = evidence.reserve(&supervisor);
        ids.insert(attempt.id());
        drop((attempt, observation));
        let second = tokio::spawn({
            let supervisor = supervisor.clone();
            async move { supervisor.drain_abandoned_attempts().await }
        });
        evidence.entered.notified().await;
        first.abort();
        second.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(second.await.unwrap_err().is_cancelled());
        assert_eq!(supervisor.abandoned(), 4);
        assert_eq!(supervisor.outstanding(), 4);
        assert_eq!(*evidence.evidence.lock().unwrap(), ids);
        assert!(evidence.released.lock().unwrap().is_empty());

        evidence.release.add_permits(4);
        assert_eq!(supervisor.drain_abandoned_attempts().await.unwrap(), 4);
        assert_eq!(supervisor.abandoned(), 0);
        assert_eq!(supervisor.outstanding(), 0);
        assert!(evidence.evidence.lock().unwrap().is_empty());
        let released = evidence.released.lock().unwrap();
        assert_eq!(released.len(), 4);
        assert_eq!(
            released
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>(),
            ids
        );
    }

    #[tokio::test]
    async fn cancelled_adjudicated_release_keeps_the_terminal_and_original_charge() {
        let evidence = GatedEvidence::new();
        let supervisor = AttemptSupervisor::new(NonZeroUsize::new(1).unwrap(), evidence.clone());
        let (attempt, observation) = evidence.reserve(&supervisor);
        let id = attempt.id();
        let resolving = tokio::spawn({
            let observation = observation.clone();
            async move { observation.outcome().await }
        });
        evidence.entered.notified().await;
        assert_eq!(
            observation.peek().unwrap(),
            Some(AttemptOutcome::NotCommitted)
        );
        assert_eq!(
            observation.outcome().await.unwrap(),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(evidence.adjudicated.load(Ordering::Relaxed), 1);
        resolving.abort();
        assert!(resolving.await.unwrap_err().is_cancelled());
        assert_eq!(supervisor.abandoned(), 1);
        drop((attempt, observation));
        assert_eq!(
            supervisor.outstanding(),
            1,
            "unreleased evidence remains charged"
        );
        assert_eq!(
            supervisor.reserve().unwrap_err().kind(),
            StateStoreErrorKind::Saturated
        );
        assert!(evidence.evidence.lock().unwrap().contains(&id));

        evidence.release.add_permits(1);
        assert_eq!(supervisor.drain_abandoned_attempts().await.unwrap(), 1);
        assert_eq!(supervisor.outstanding(), 0);
        assert_eq!(*evidence.released.lock().unwrap(), vec![id]);
        let live = supervisor.reserve().unwrap();
        assert_eq!(supervisor.drain_abandoned_attempts().await.unwrap(), 0);
        assert_eq!(
            supervisor.outstanding(),
            1,
            "no second refund consumes the next attempt"
        );
        drop(live);
        assert_eq!(supervisor.outstanding(), 0);
    }

    #[tokio::test]
    async fn failed_adjudicated_release_refunds_once_on_either_side_of_last_alias_drop() {
        for drop_aliases_first in [false, true] {
            let evidence = GatedEvidence::new();
            let supervisor =
                AttemptSupervisor::new(NonZeroUsize::new(1).unwrap(), evidence.clone());
            let (attempt, observation) = evidence.reserve(&supervisor);
            let alias = observation.clone();
            evidence.refuse.store(true, Ordering::Release);
            assert_eq!(
                observation.outcome().await.unwrap(),
                AttemptOutcome::NotCommitted
            );
            assert_eq!(alias.outcome().await.unwrap(), AttemptOutcome::NotCommitted);
            assert_eq!(
                supervisor.abandoned(),
                1,
                "a cached terminal adds no second debt"
            );
            drop((attempt, observation));
            let mut last = Some(alias);
            if drop_aliases_first {
                drop(last.take());
            }
            assert_eq!(supervisor.outstanding(), 1);
            assert_eq!(
                supervisor.reserve().unwrap_err().kind(),
                StateStoreErrorKind::Saturated
            );

            evidence.refuse.store(false, Ordering::Release);
            evidence.release.add_permits(1);
            assert_eq!(supervisor.drain_abandoned_attempts().await.unwrap(), 1);
            assert_eq!(supervisor.outstanding(), usize::from(!drop_aliases_first));
            drop(last);
            assert_eq!(supervisor.outstanding(), 0);
            let live = supervisor.reserve().unwrap();
            assert_eq!(supervisor.drain_abandoned_attempts().await.unwrap(), 0);
            assert_eq!(supervisor.outstanding(), 1);
            drop(live);
            assert_eq!(supervisor.outstanding(), 0);
        }
    }

    #[tokio::test]
    async fn provider_settlement_retains_its_existing_release_owner() {
        let evidence = GatedEvidence::new();
        let supervisor = AttemptSupervisor::new(NonZeroUsize::new(1).unwrap(), evidence.clone());
        let (attempt, observation) = evidence.reserve(&supervisor);
        attempt.settle(AttemptOutcome::NotCommitted).unwrap();
        assert_eq!(
            observation.outcome().await.unwrap(),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(evidence.adjudicated.load(Ordering::Relaxed), 0);
        evidence.release.add_permits(1);
        evidence.release_evidence(attempt.id()).await.unwrap();
        assert_eq!(supervisor.abandoned(), 0);
        drop(attempt);
        assert_eq!(
            supervisor.outstanding(),
            1,
            "the original observation still owns its alias"
        );
        drop(observation);
        assert_eq!(supervisor.outstanding(), 0);
        assert_eq!(supervisor.drain_abandoned_attempts().await.unwrap(), 0);
    }

    // This test port never supplies a physical operation. These tests exercise
    // only the original API borrower, shared evidence algorithm and debt owner.
    struct NoPhysicalPort;
    impl crate::StateStoreExecutionPort for NoPhysicalPort {
        fn bind_original_in(
            &self,
            _: &Arc<dyn crate::StateStoreBlockingOriginal>,
            _: std::time::Instant,
            _: &mut Option<Box<dyn crate::StateStoreBlockingSlot>>,
        ) -> Result<(), StateStoreExecutionFailure> {
            panic!("evidence fixture must not fabricate a physical slot")
        }
        fn request_progress(&self) {}
    }
    fn original_context() -> (
        Arc<dyn crate::StateStoreExecutionPort>,
        StateStoreExecutionContext,
    ) {
        let port: Arc<dyn crate::StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
        let execution = StateStoreExecutionContext::new(
            &port,
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        );
        (port, execution)
    }

    struct OriginalEvidenceFixture {
        expected: StateStoreExecutionContext,
        evidence: Arc<GatedEvidence>,
        observe_error: Mutex<Option<StateStoreExecutionFailure>>,
        release_error: Mutex<Option<StateStoreExecutionFailure>>,
    }
    impl OriginalEvidenceFixture {
        fn new(execution: &StateStoreExecutionContext, evidence: Arc<GatedEvidence>) -> Arc<Self> {
            Arc::new(Self {
                expected: execution.clone(),
                evidence,
                observe_error: Mutex::new(None),
                release_error: Mutex::new(None),
            })
        }
        fn check(&self, execution: &StateStoreExecutionContext) {
            assert!(self.expected.shares_owner(execution));
            assert_eq!(self.expected.deadline(), execution.deadline());
        }
    }
    #[async_trait::async_trait]
    impl InDoubtAdjudicator for OriginalEvidenceFixture {
        async fn adjudicate(&self, _: AttemptId) -> Result<AttemptOutcome, StateStoreError> {
            panic!("original observation must not dispatch the legacy API")
        }
        async fn release_evidence(&self, _: AttemptId) -> Result<(), StateStoreError> {
            panic!("original release must not dispatch the legacy API")
        }
        async fn adjudicate_in(
            &self,
            attempt: AttemptId,
            execution: &StateStoreExecutionContext,
        ) -> Result<AttemptOutcome, StateStoreExecutionFailure> {
            self.check(execution);
            if let Some(error) = self.observe_error.lock().unwrap().clone() {
                return Err(error);
            }
            // Both fixture APIs share the same provider evidence set/gate;
            // this is not a production fallback or a second release record.
            self.evidence
                .adjudicate(attempt)
                .await
                .map_err(StateStoreError::into_original_failure)
        }
        async fn release_evidence_in(
            &self,
            attempt: AttemptId,
            execution: &StateStoreExecutionContext,
        ) -> Result<(), StateStoreExecutionFailure> {
            self.check(execution);
            if let Some(error) = self.release_error.lock().unwrap().clone() {
                return Err(error);
            }
            self.evidence
                .release_evidence(attempt)
                .await
                .map_err(StateStoreError::into_original_failure)
        }
    }

    #[tokio::test]
    async fn original_evidence_defaults_refuse_without_dispatching_legacy() {
        let (_port, execution) = original_context();
        let (supervisor, legacy) = supervisor(1);
        let (attempt, observation) = supervisor.reserve().unwrap();
        attempt.mark_dispatched().unwrap();
        let refusal = observation.outcome_in(&execution).await.unwrap_err();
        assert!(
            refusal
                .downcast_ref::<StateStoreOriginalUnsupported>()
                .is_some()
        );
        assert_eq!(observation.peek().unwrap(), None);
        drop((attempt, observation));
        let refusal = supervisor
            .drain_abandoned_attempts_in(&execution)
            .await
            .unwrap_err();
        assert!(
            refusal
                .downcast_ref::<StateStoreOriginalUnsupported>()
                .is_some()
        );
        assert_eq!(legacy.adjudicated.load(Ordering::Relaxed), 0);
        assert_eq!(legacy.released.load(Ordering::Relaxed), 0);
        assert_eq!(supervisor.abandoned(), 1);
        assert_eq!(supervisor.outstanding(), 1);
        // The explicit legacy entry still uses its existing provider contract.
        assert_eq!(supervisor.drain_abandoned_attempts().await.unwrap(), 1);
        assert_eq!(legacy.released.load(Ordering::Relaxed), 1);
        let (attempt, observation) = supervisor.reserve().unwrap();
        assert_eq!(
            observation.outcome_in(&execution).await.unwrap(),
            AttemptOutcome::NotCommitted
        );
        assert_eq!(
            attempt.mark_dispatched().unwrap_err().kind(),
            StateStoreErrorKind::InvalidRequest
        );
        assert_eq!(legacy.adjudicated.load(Ordering::Relaxed), 0);
    }

    #[derive(Debug)]
    struct ForeignTransient;
    impl std::fmt::Display for ForeignTransient {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Transient: release not ready")
        }
    }
    impl std::error::Error for ForeignTransient {}
    impl novarocks_failure_contract::FailureProjection for ForeignTransient {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            // Deliberately the same fact and diagnostic as the API-owned
            // Transient. Neither permits inferring the foreign retry policy.
            novarocks_failure_contract::FailureProjection::failure_fact(&StateStoreError::new(
                StateStoreErrorKind::Transient,
                "release not ready",
            ))
        }
    }

    #[tokio::test]
    async fn original_evidence_preserves_cause_and_only_own_transient_is_deferred() {
        use novarocks_failure_contract::FailureProjection;
        let (_port, execution) = original_context();
        let evidence = GatedEvidence::new();
        let original = OriginalEvidenceFixture::new(&execution, evidence.clone());
        let supervisor = AttemptSupervisor::new(NonZeroUsize::new(1).unwrap(), original.clone());
        let (attempt, observation) = evidence.reserve(&supervisor);
        let foreign = StateStoreExecutionFailure::new(ForeignTransient);
        *original.observe_error.lock().unwrap() = Some(foreign.clone());
        let returned = observation.outcome_in(&execution).await.unwrap_err();
        assert_eq!(returned, foreign, "same original cause identity");
        assert_eq!(returned.failure_fact(), foreign.failure_fact());
        assert!(returned.downcast_ref::<ForeignTransient>().is_some());
        assert_eq!(observation.peek().unwrap(), None);
        *original.observe_error.lock().unwrap() = None;
        drop((attempt, observation));
        *original.release_error.lock().unwrap() = Some(foreign.clone());
        assert_eq!(
            supervisor
                .drain_abandoned_attempts_in(&execution)
                .await
                .unwrap_err(),
            foreign
        );
        assert_eq!((supervisor.outstanding(), supervisor.abandoned()), (1, 1));
        *original.release_error.lock().unwrap() = Some(
            StateStoreError::new(StateStoreErrorKind::Transient, "release not ready")
                .into_original_failure(),
        );
        assert_eq!(
            supervisor
                .drain_abandoned_attempts_in(&execution)
                .await
                .unwrap(),
            0
        );
        assert_eq!((supervisor.outstanding(), supervisor.abandoned()), (1, 1));
        *original.release_error.lock().unwrap() = None;
        evidence.release.add_permits(1);
        assert_eq!(
            supervisor
                .drain_abandoned_attempts_in(&execution)
                .await
                .unwrap(),
            1
        );
        assert_eq!((supervisor.outstanding(), supervisor.abandoned()), (0, 0));
        assert_eq!(evidence.released.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn original_cancelled_release_returns_to_same_debt_and_context() {
        let (_port, execution) = original_context();
        let evidence = GatedEvidence::new();
        let original = OriginalEvidenceFixture::new(&execution, evidence.clone());
        let supervisor = AttemptSupervisor::new(NonZeroUsize::new(1).unwrap(), original);
        let (attempt, observation) = evidence.reserve(&supervisor);
        let id = attempt.id();
        let resolving = tokio::spawn({
            let observation = observation.clone();
            let execution = execution.clone();
            async move { observation.outcome_in(&execution).await }
        });
        evidence.entered.notified().await;
        assert_eq!(
            observation.outcome_in(&execution).await.unwrap(),
            AttemptOutcome::NotCommitted
        );
        resolving.abort();
        assert!(resolving.await.unwrap_err().is_cancelled());
        drop((attempt, observation));
        assert_eq!((supervisor.outstanding(), supervisor.abandoned()), (1, 1));
        let draining = tokio::spawn({
            let supervisor = supervisor.clone();
            let execution = execution.clone();
            async move { supervisor.drain_abandoned_attempts_in(&execution).await }
        });
        evidence.entered.notified().await;
        draining.abort();
        assert!(draining.await.unwrap_err().is_cancelled());
        assert_eq!((supervisor.outstanding(), supervisor.abandoned()), (1, 1));
        assert!(evidence.evidence.lock().unwrap().contains(&id));
        evidence.release.add_permits(1);
        assert_eq!(
            supervisor
                .drain_abandoned_attempts_in(&execution)
                .await
                .unwrap(),
            1
        );
        assert_eq!(evidence.adjudicated.load(Ordering::Relaxed), 1);
        assert_eq!(*evidence.released.lock().unwrap(), vec![id]);
        assert_eq!((supervisor.outstanding(), supervisor.abandoned()), (0, 0));
    }
}
