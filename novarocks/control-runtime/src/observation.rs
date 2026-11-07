// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Diagnostic observations from original runtime owners. They never grant
//! resources, select scheduling, infer exit, or reconstruct duties from history.

use crate::{
    actor_turn::Clock,
    sync::{Arc, Mutex, MutexGuard},
};
use novarocks_control_runtime_core::{
    capacity::CapacityError, identity::Tick, mailbox::InputClass, owned_storage::OwnedSlotNeed,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::{
    ops::{Deref, DerefMut},
    sync::{LockResult, PoisonError, TryLockError, TryLockResult},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DurationObservation {
    pub samples: u64,
    pub total_ns: u64,
    pub maximum_ns: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputClassObservation {
    pub class: InputClass,
    pub steps: u64,
    /// Original class credits held by inputs or prepaid reusable roles.
    pub input_credits_held: u64,
    /// Time from the handoff's pre-lock observation to the step's pre-lock
    /// observation. This includes handoff/lock waiting, not just mailbox time.
    pub handoff_to_step: Option<DurationObservation>,
    pub step_elapsed: Option<DurationObservation>,
    pub charged_work: u64,
    pub maximum_step_work: u64,
    pub panicked_steps: u64,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LedgerContentionObservation {
    pub lock_attempts: u64,
    pub contended_locks: u64,
    pub poisoned_locks: u64,
    pub unavailable_try_locks: u64,
}
/// Typed refusal categories. Ordinary capacity exhaustion is separately
/// observed backpressure; it is never counted as a runtime invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum CapacityRefusalKind {
    Exhausted,
    ScopeClosed,
    ScopeStopped,
    ScopeFailed,
    GenerationMismatch,
    IdentityOrArithmetic,
    WrongOwnerOrState,
    OwnedStorage,
}
impl CapacityRefusalKind {
    pub const ALL: [Self; 8] = [
        Self::Exhausted,
        Self::ScopeClosed,
        Self::ScopeStopped,
        Self::ScopeFailed,
        Self::GenerationMismatch,
        Self::IdentityOrArithmetic,
        Self::WrongOwnerOrState,
        Self::OwnedStorage,
    ];
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeObservationSnapshot {
    pub inputs: [InputClassObservation; 4],
    pub ledger: LedgerContentionObservation,
    pub capacity_operations: u64,
    pub capacity_refusals: [u64; 8],
    pub scope_failures: u64,
    pub invalid_clock_observations: u64,
}
/// A completed bounded traversal observes an interval, not a coherent instant.
/// Work phases are the canonical lease phases, not physical executor queues.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LedgerInventoryObservation {
    pub began_at: Option<Tick>,
    pub ended_at: Option<Tick>,
    pub records_visited: usize,
    pub scopes: usize,
    pub failed_scopes: usize,
    pub stopped_scopes: usize,
    pub admitted_work: usize,
    pub queued_work: usize,
    pub started_work: usize,
    pub body_exited_work: usize,
    pub fully_exited_work: usize,
    pub bound_storage_positions: usize,
    pub reports: usize,
    pub filled_reports: usize,
    /// Reports with positive body/capture receipts which still retain a
    /// payload or consumer obligation; this does not certify child exit.
    pub producer_exited_reports_awaiting_retirement: usize,
    pub exited_unconsumed_reports: usize,
    pub reservations: usize,
    pub unused_owned_slots: OwnedSlotNeed,
    pub unresolved_work: usize,
    pub live_by_label: BTreeMap<&'static str, usize>,
    pub unresolved_by_label: BTreeMap<&'static str, usize>,
}
/// One original Work header, including exited work awaiting its children,
/// reports or pins. This observation confers no exit or retirement authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkInventoryEntry {
    pub work: novarocks_control_runtime_core::identity::WorkId,
    pub parent: Option<novarocks_control_runtime_core::identity::WorkId>,
    pub kind: novarocks_control_runtime_core::scope::WorkKind,
    pub phase: novarocks_control_runtime_core::scope::WorkPhase,
    pub label: Option<&'static str>,
    pub body_exited: bool,
    pub captures_exited: bool,
    pub unresolved: bool,
}
/// A coherent, on-demand scope diagnostic. The application knows its own
/// control roles and can exclude them when displaying detachment blockers.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScopeWorkInventory {
    pub entries: Vec<WorkInventoryEntry>,
    pub live_by_label: BTreeMap<&'static str, usize>,
    pub unresolved_by_label: BTreeMap<&'static str, usize>,
}
impl crate::transaction::ScopeHandle {
    pub fn work_inventory(&self) -> Result<ScopeWorkInventory, crate::transaction::RuntimeError> {
        let state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope = state
            .scopes
            .get(&self.id)
            .ok_or(crate::transaction::RuntimeError::UnknownScope)?;
        let mut inventory = ScopeWorkInventory::default();
        for id in scope.work_ids() {
            let work = scope
                .work(id)
                .expect("work identity from same original scope");
            let label = state.work_labels.get(&(self.id, id)).copied();
            let unresolved = state.unresolved.contains(&(self.id, id));
            if let Some(label) = label {
                *inventory.live_by_label.entry(label).or_default() += 1;
                if unresolved {
                    *inventory.unresolved_by_label.entry(label).or_default() += 1;
                }
            }
            inventory.entries.push(WorkInventoryEntry {
                work: id,
                parent: work.parent,
                kind: work.kind,
                phase: work.phase,
                label,
                body_exited: work.body_exited,
                captures_exited: work.captures_exited,
                unresolved,
            });
        }
        Ok(inventory)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerInventoryPage {
    pub visited: usize,
    pub cumulative_visited: usize,
    /// Only the page which completes the traversal returns its interval.
    /// An incomplete or busy page never substitutes an older cached inventory.
    pub completed: Option<LedgerInventoryObservation>,
}

#[derive(Default)]
struct DurationCounters {
    samples: AtomicU64,
    total: AtomicU64,
    maximum: AtomicU64,
}
impl DurationCounters {
    fn observe(&self, value: u64, state: &ObservationState) {
        state.add(&self.samples, 1);
        state.add(&self.total, value);
        self.maximum.fetch_max(value, Ordering::Relaxed);
    }
    fn snapshot(&self) -> DurationObservation {
        DurationObservation {
            samples: self.samples.load(Ordering::Relaxed),
            total_ns: self.total.load(Ordering::Relaxed),
            maximum_ns: self.maximum.load(Ordering::Relaxed),
        }
    }
}
#[derive(Default)]
struct InputCounters {
    credits: AtomicU64,
    steps: AtomicU64,
    wait: DurationCounters,
    elapsed: DurationCounters,
    work: AtomicU64,
    maximum_work: AtomicU64,
    panics: AtomicU64,
}
struct ObservationState {
    clock: Option<Arc<dyn Clock>>,
    inputs: [InputCounters; 4],
    lock_attempts: AtomicU64,
    contended_locks: AtomicU64,
    poisoned_locks: AtomicU64,
    unavailable_try_locks: AtomicU64,
    capacity_operations: AtomicU64,
    capacity_refusals: [AtomicU64; 8],
    scope_failures: AtomicU64,
    invalid_time: AtomicU64,
    overflow: AtomicBool,
}
impl ObservationState {
    fn add(&self, counter: &AtomicU64, n: u64) {
        if counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_add(n))
            .is_err()
        {
            self.overflow.store(true, Ordering::Release);
        }
    }
}
/// Disabled is allocation-free. Enabling metrics allocates fixed diagnostic
/// counters; it does not enable or allocate debug history. A clock is optional.
/// Durations use that clock's domain and are not CPU-time measurements.
#[derive(Clone, Default)]
pub struct RuntimeObservation(Option<Arc<ObservationState>>);
impl RuntimeObservation {
    pub fn new(clock: Option<Arc<dyn Clock>>) -> Self {
        Self(Some(Arc::new(ObservationState {
            clock,
            inputs: std::array::from_fn(|_| InputCounters::default()),
            lock_attempts: AtomicU64::new(0),
            contended_locks: AtomicU64::new(0),
            poisoned_locks: AtomicU64::new(0),
            unavailable_try_locks: AtomicU64::new(0),
            capacity_operations: AtomicU64::new(0),
            capacity_refusals: std::array::from_fn(|_| AtomicU64::new(0)),
            scope_failures: AtomicU64::new(0),
            invalid_time: AtomicU64::new(0),
            overflow: AtomicBool::new(false),
        })))
    }
    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }
    /// Call only outside scheduler, inbox, ledger and pure-core locks.
    pub(crate) fn now(&self) -> Option<Tick> {
        self.0.as_ref()?.clock.as_ref().map(|c| c.now())
    }
    pub fn snapshot(&self) -> Option<RuntimeObservationSnapshot> {
        let state = self.0.as_ref()?;
        if state.overflow.load(Ordering::Acquire) {
            return None;
        }
        let result = RuntimeObservationSnapshot {
            inputs: std::array::from_fn(|i| {
                let c = &state.inputs[i];
                InputClassObservation {
                    class: InputClass::ALL[i],
                    steps: c.steps.load(Ordering::Relaxed),
                    input_credits_held: c.credits.load(Ordering::Relaxed),
                    handoff_to_step: state.clock.as_ref().map(|_| c.wait.snapshot()),
                    step_elapsed: state.clock.as_ref().map(|_| c.elapsed.snapshot()),
                    charged_work: c.work.load(Ordering::Relaxed),
                    maximum_step_work: c.maximum_work.load(Ordering::Relaxed),
                    panicked_steps: c.panics.load(Ordering::Relaxed),
                }
            }),
            ledger: LedgerContentionObservation {
                lock_attempts: state.lock_attempts.load(Ordering::Relaxed),
                contended_locks: state.contended_locks.load(Ordering::Relaxed),
                poisoned_locks: state.poisoned_locks.load(Ordering::Relaxed),
                unavailable_try_locks: state.unavailable_try_locks.load(Ordering::Relaxed),
            },
            capacity_operations: state.capacity_operations.load(Ordering::Relaxed),
            capacity_refusals: std::array::from_fn(|i| {
                state.capacity_refusals[i].load(Ordering::Relaxed)
            }),
            scope_failures: state.scope_failures.load(Ordering::Relaxed),
            invalid_clock_observations: state.invalid_time.load(Ordering::Relaxed),
        };
        (!state.overflow.load(Ordering::Acquire)).then_some(result)
    }
    pub(crate) fn step(
        &self,
        class: InputClass,
        queued: Option<Tick>,
        began: Option<Tick>,
        ended: Option<Tick>,
        work: usize,
        panicked: bool,
    ) {
        let Some(state) = &self.0 else { return };
        let c = &state.inputs[class.index()];
        state.add(&c.steps, 1);
        if let Ok(work) = u64::try_from(work) {
            state.add(&c.work, work);
            c.maximum_work.fetch_max(work, Ordering::Relaxed);
        } else {
            state.overflow.store(true, Ordering::Release);
        }
        if panicked {
            state.add(&c.panics, 1);
        }
        for (from, to, counter) in [(queued, began, &c.wait), (began, ended, &c.elapsed)] {
            if let (Some(from), Some(to)) = (from, to) {
                if let Some(ns) = to.0.checked_sub(from.0) {
                    counter.observe(ns, state);
                } else {
                    state.add(&state.invalid_time, 1);
                }
            }
        }
    }
    pub(crate) fn input_credit_taken(&self, class: InputClass) {
        if let Some(state) = &self.0 {
            state.add(&state.inputs[class.index()].credits, 1);
        }
    }
    pub(crate) fn input_credit_returned(&self, class: InputClass) {
        if let Some(state) = &self.0
            && state.inputs[class.index()]
                .credits
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_sub(1)
                })
                .is_err()
        {
            state.overflow.store(true, Ordering::Release);
        }
    }
    pub(crate) fn scope_failed(&self) {
        if let Some(s) = &self.0 {
            s.add(&s.scope_failures, 1);
        }
    }
    pub(crate) fn capacity_runtime<T>(&self, result: &Result<T, crate::transaction::RuntimeError>) {
        use crate::transaction::RuntimeError;
        let result = result.as_ref().map(|_| ()).map_err(|error| match error {
            RuntimeError::Capacity(error) => *error,
            RuntimeError::CapacityDenied(denial) => CapacityError::Exhausted(denial.dimension),
            RuntimeError::Scope(error) => CapacityError::Scope(*error),
            RuntimeError::OwnedStorage(error) => CapacityError::OwnedStorage(*error),
            RuntimeError::IdentityExhausted => CapacityError::Overflow,
            _ => CapacityError::WrongOwner,
        });
        self.capacity(&result);
    }
    pub(crate) fn capacity<T>(&self, result: &Result<T, CapacityError>) {
        let Some(s) = &self.0 else { return };
        s.add(&s.capacity_operations, 1);
        if let Err(error) = result {
            use novarocks_control_runtime_core::scope::ScopeError;
            let kind = match error {
                CapacityError::Exhausted(_) => CapacityRefusalKind::Exhausted,
                CapacityError::Scope(ScopeError::Closed) => CapacityRefusalKind::ScopeClosed,
                CapacityError::Scope(ScopeError::Stopped) => CapacityRefusalKind::ScopeStopped,
                CapacityError::Scope(ScopeError::Failed) => CapacityRefusalKind::ScopeFailed,
                CapacityError::Scope(ScopeError::GenerationMismatch) => {
                    CapacityRefusalKind::GenerationMismatch
                }
                CapacityError::Overflow
                | CapacityError::DuplicateDimension
                | CapacityError::UnknownDimension => CapacityRefusalKind::IdentityOrArithmetic,
                CapacityError::OwnedStorage(_) => CapacityRefusalKind::OwnedStorage,
                _ => CapacityRefusalKind::WrongOwnerOrState,
            };
            s.add(&s.capacity_refusals[kind as usize], 1);
        }
    }
}

/// The same original ledger mutex with diagnostic acquisition observations.
/// No clock, callback, history or resource policy runs while it is acquired.
pub(crate) struct ObservedMutex<T> {
    inner: Mutex<T>,
    observation: RuntimeObservation,
}
pub(crate) struct ObservedMutexGuard<'a, T>(MutexGuard<'a, T>);
impl<T> ObservedMutex<T> {
    pub(crate) fn new(value: T, observation: RuntimeObservation) -> Self {
        Self {
            inner: Mutex::new(value),
            observation,
        }
    }
    fn wrap<'a>(r: LockResult<MutexGuard<'a, T>>) -> LockResult<ObservedMutexGuard<'a, T>> {
        r.map(ObservedMutexGuard)
            .map_err(|e| PoisonError::new(ObservedMutexGuard(e.into_inner())))
    }
    pub(crate) fn lock(&self) -> LockResult<ObservedMutexGuard<'_, T>> {
        let Some(s) = &self.observation.0 else {
            return Self::wrap(self.inner.lock());
        };
        s.add(&s.lock_attempts, 1);
        match self.inner.try_lock() {
            Ok(guard) => Ok(ObservedMutexGuard(guard)),
            Err(TryLockError::Poisoned(error)) => {
                s.add(&s.poisoned_locks, 1);
                Err(PoisonError::new(ObservedMutexGuard(error.into_inner())))
            }
            Err(TryLockError::WouldBlock) => {
                s.add(&s.contended_locks, 1);
                let result = self.inner.lock();
                if result.is_err() {
                    s.add(&s.poisoned_locks, 1);
                }
                Self::wrap(result)
            }
        }
    }
    pub(crate) fn try_lock(&self) -> TryLockResult<ObservedMutexGuard<'_, T>> {
        let result = self.inner.try_lock();
        if let Some(s) = &self.observation.0 {
            match &result {
                Err(TryLockError::WouldBlock) => s.add(&s.unavailable_try_locks, 1),
                Err(TryLockError::Poisoned(_)) => s.add(&s.poisoned_locks, 1),
                _ => {}
            }
        }
        result.map(ObservedMutexGuard).map_err(|e| match e {
            TryLockError::WouldBlock => TryLockError::WouldBlock,
            TryLockError::Poisoned(e) => {
                TryLockError::Poisoned(PoisonError::new(ObservedMutexGuard(e.into_inner())))
            }
        })
    }
}
impl<T> Deref for ObservedMutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
impl<T> DerefMut for ObservedMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

#[derive(Default)]
pub(crate) struct InventoryCursor {
    round: Option<InventoryRound>,
}
struct InventoryRound {
    phase: u8,
    scope_until: Option<novarocks_control_runtime_core::identity::ScopeId>,
    scope_after: Option<novarocks_control_runtime_core::identity::ScopeId>,
    work: Option<(
        novarocks_control_runtime_core::identity::ScopeId,
        Option<novarocks_control_runtime_core::identity::WorkId>,
        Option<novarocks_control_runtime_core::identity::WorkId>,
    )>,
    slot_until: Option<novarocks_control_runtime_core::identity::SlotId>,
    slot_after: Option<novarocks_control_runtime_core::identity::SlotId>,
    reservation_until: Option<novarocks_control_runtime_core::identity::ReservationId>,
    reservation_after: Option<novarocks_control_runtime_core::identity::ReservationId>,
    sample: LedgerInventoryObservation,
}
impl crate::transaction::RuntimeLedger {
    /// Advances at most `budget` original records/probes. Neither acquiring a
    /// diagnostic cursor nor acquiring the ledger waits. IDs freeze the upper
    /// bounds; deletion/replacement during the traversal remains an interval
    /// observation, never proof of coherent zero or actual quiescence.
    pub fn try_observation_inventory(
        &self,
        budget: usize,
    ) -> Result<Option<LedgerInventoryPage>, crate::transaction::CapacityObservationError> {
        use crate::transaction::CapacityObservationError;
        use novarocks_control_runtime_core::{completion::SlotState, scope::WorkPhase};
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let Some(cursor) = &self.shared.inventory else {
            return Ok(None);
        };
        let began = self.shared.observation.now();
        let mut cursor = match cursor.try_lock() {
            Ok(cursor) => cursor,
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Poisoned(_)) => return Err(CapacityObservationError::Poisoned),
        };
        let state = match self.shared.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Poisoned(_)) => return Err(CapacityObservationError::Poisoned),
        };
        let round = cursor.round.get_or_insert_with(|| InventoryRound {
            phase: 0,
            scope_until: state.scopes.last_key_value().map(|(id, _)| *id),
            scope_after: None,
            work: None,
            slot_until: state.slots.last_key_value().map(|(id, _)| *id),
            slot_after: None,
            reservation_until: state.ledger.last_observed_reservation(),
            reservation_after: None,
            sample: LedgerInventoryObservation {
                began_at: began,
                ..Default::default()
            },
        });
        let mut visited = 0;
        for _ in 0..budget {
            match round.phase {
                0 => {
                    if let Some((id, after, until)) = round.work {
                        let next = until.and_then(|until| {
                            state.scopes.get(&id)?.observed_work_after(after, until)
                        });
                        if let Some((work, record)) = next {
                            round.work = Some((id, Some(work), until));
                            match record.phase {
                                WorkPhase::Admitted => round.sample.admitted_work += 1,
                                WorkPhase::Queued => round.sample.queued_work += 1,
                                WorkPhase::Started => round.sample.started_work += 1,
                                WorkPhase::BodyExited => round.sample.body_exited_work += 1,
                            }
                            if record.body_exited && record.captures_exited && record.children == 0
                            {
                                round.sample.fully_exited_work += 1;
                            }
                            round.sample.unresolved_work +=
                                usize::from(state.unresolved.contains(&(id, work)));
                            if let Some(label) = state.work_labels.get(&(id, work)) {
                                *round.sample.live_by_label.entry(label).or_default() += 1;
                                if state.unresolved.contains(&(id, work)) {
                                    *round.sample.unresolved_by_label.entry(label).or_default() +=
                                        1;
                                }
                            }
                            visited += 1;
                            round.sample.records_visited += 1;
                        } else {
                            round.work = None;
                        }
                    } else {
                        let next = round.scope_until.and_then(|until| {
                            state
                                .scopes
                                .range((
                                    round.scope_after.map_or(Unbounded, Excluded),
                                    Included(until),
                                ))
                                .next()
                        });
                        if let Some((id, scope)) = next {
                            round.scope_after = Some(*id);
                            round.work = Some((*id, None, scope.last_observed_work()));
                            round.sample.scopes += 1;
                            round.sample.failed_scopes += usize::from(scope.failed());
                            round.sample.stopped_scopes += usize::from(scope.stopped());
                            round.sample.bound_storage_positions += scope.storage_count();
                            visited += 1;
                            round.sample.records_visited += 1;
                        } else {
                            round.phase = 1;
                        }
                    }
                }
                1 => {
                    let next = round.slot_until.and_then(|until| {
                        state
                            .slots
                            .range((
                                round.slot_after.map_or(Unbounded, Excluded),
                                Included(until),
                            ))
                            .next()
                    });
                    if let Some((id, report)) = next {
                        round.slot_after = Some(*id);
                        round.sample.reports += 1;
                        round.sample.filled_reports +=
                            usize::from(report.state() == SlotState::Filled);
                        if report.body_and_captures_exited() && !report.retirable() {
                            round.sample.producer_exited_reports_awaiting_retirement += 1;
                        }
                        if report.body_and_captures_exited() && report.pending_consumers() != 0 {
                            round.sample.exited_unconsumed_reports += 1;
                        }
                        visited += 1;
                        round.sample.records_visited += 1;
                    } else {
                        round.phase = 2;
                    }
                }
                2 => {
                    let next = round.reservation_until.and_then(|until| {
                        state
                            .ledger
                            .observed_reservation_after(round.reservation_after, until)
                    });
                    if let Some((id, reservation)) = next {
                        round.reservation_after = Some(id);
                        round.sample.reservations += 1;
                        let aggregate = round
                            .sample
                            .unused_owned_slots
                            .checked_add(reservation.owned_slots());
                        let Ok(aggregate) = aggregate else {
                            // A failed page invalidates the whole interval.
                            // Its advanced cursor must never later produce a
                            // completed sample with the refused record omitted.
                            cursor.round = None;
                            return Err(CapacityObservationError::InventoryOverflow);
                        };
                        round.sample.unused_owned_slots = aggregate;
                        visited += 1;
                        round.sample.records_visited += 1;
                    } else {
                        round.phase = 3;
                    }
                }
                _ => break,
            }
        }
        let cumulative_visited = round.sample.records_visited;
        let completed = (round.phase == 3).then(|| {
            cursor
                .round
                .take()
                .expect("completed original observation")
                .sample
        });
        drop(state);
        drop(cursor);
        let completed = completed.map(|mut sample| {
            sample.ended_at = self.shared.observation.now();
            sample
        });
        Ok(Some(LedgerInventoryPage {
            visited,
            cumulative_visited,
            completed,
        }))
    }
    pub fn observation_snapshot(&self) -> Option<RuntimeObservationSnapshot> {
        self.shared.observation.snapshot()
    }
}
