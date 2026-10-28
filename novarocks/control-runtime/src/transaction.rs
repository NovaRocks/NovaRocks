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

//! Scope gates and capacity reservations use the same short transaction.

use crate::{
    lease::StepGuard,
    notify::Notify,
    sync::{Arc, Mutex},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, CapacityLedger, ReservationRequest, ResourceVector},
    completion::CompletionError,
    history::HistoryFact,
    identity::{DimensionId, Generation, ReservationId, ScopeId, SlotId, WorkId},
    scope::{ScopeError, ScopeState, WorkAdmission, WorkKind},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityDenial {
    pub dimension: DimensionId,
    pub requested: u64,
    pub committed: u64,
    pub limit: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    Capacity(CapacityError),
    CapacityDenied(CapacityDenial),
    Scope(ScopeError),
    Completion(CompletionError),
    UnknownScope,
    ScopeCapacity,
    DuplicateScope,
    UnknownSlot,
    DuplicateSlot,
    ScopeNotRetired,
    UnresolvedWork,
    IdentityExhausted,
}
impl From<CapacityError> for RuntimeError {
    fn from(value: CapacityError) -> Self {
        Self::Capacity(value)
    }
}
impl From<ScopeError> for RuntimeError {
    fn from(value: ScopeError) -> Self {
        Self::Scope(value)
    }
}
impl From<CompletionError> for RuntimeError {
    fn from(value: CompletionError) -> Self {
        Self::Completion(value)
    }
}
impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "control runtime failure: {self:?}")
    }
}
impl std::error::Error for RuntimeError {}

pub(crate) struct LedgerState {
    pub(crate) ledger: CapacityLedger,
    pub(crate) scopes: BTreeMap<ScopeId, ScopeState>,
    pub(crate) slots: BTreeMap<SlotId, novarocks_control_runtime_core::completion::CompletionSlot>,
    pub(crate) unresolved: BTreeSet<(ScopeId, WorkId)>,
    unresolved_by_scope: BTreeMap<ScopeId, usize>,
    pub(crate) abandoned_bundles: BTreeSet<ReservationId>,
    pub(crate) pending_retirement: BTreeSet<(ScopeId, WorkId)>,
    parent_links: BTreeMap<ScopeId, (ScopeId, WorkId)>,
    max_scopes: usize,
    last_scope: u64,
    last_slot: u64,
    external_open: bool,
}
impl LedgerState {
    pub(crate) fn note_unresolved(&mut self, scope: ScopeId, work: WorkId) {
        if self.unresolved.insert((scope, work)) {
            let count = self.unresolved_by_scope.entry(scope).or_default();
            *count = count
                .checked_add(1)
                .expect("unresolved work count fits admitted work");
        }
    }
    fn clear_unresolved(&mut self, scope: ScopeId, work: WorkId) {
        if self.unresolved.remove(&(scope, work)) {
            let count = self
                .unresolved_by_scope
                .get_mut(&scope)
                .expect("unresolved owner index");
            *count = count
                .checked_sub(1)
                .expect("unresolved work count underflow");
            if *count == 0 {
                self.unresolved_by_scope.remove(&scope);
            }
        }
    }
    pub(crate) fn next_slot_id(&self, owner: ScopeId) -> Result<SlotId, RuntimeError> {
        let local = self
            .scopes
            .get(&owner)
            .ok_or(RuntimeError::UnknownScope)?
            .next_slot_id()?;
        let global = self
            .last_slot
            .checked_add(1)
            .ok_or(RuntimeError::IdentityExhausted)?;
        SlotId::new(local.get().max(global)).ok_or(RuntimeError::IdentityExhausted)
    }
    pub(crate) fn reserve_slot(&mut self, owner: ScopeId, id: SlotId) -> Result<(), RuntimeError> {
        // Report handles retain this ledger and identity after retirement.
        // Neither another scope nor actor attachment may reissue that identity.
        if id.get() <= self.last_slot {
            return Err(RuntimeError::DuplicateSlot);
        }
        self.scopes
            .get_mut(&owner)
            .ok_or(RuntimeError::UnknownScope)?
            .reserve_slot(id)?;
        self.last_slot = self.last_slot.max(id.get());
        Ok(())
    }
    pub(crate) fn parts(
        &mut self,
        id: ScopeId,
    ) -> Result<(&mut CapacityLedger, &mut ScopeState), RuntimeError> {
        Ok((
            &mut self.ledger,
            self.scopes.get_mut(&id).ok_or(RuntimeError::UnknownScope)?,
        ))
    }
}

pub(crate) struct LedgerShared {
    pub(crate) state: Mutex<LedgerState>,
    pub(crate) changed: Notify,
    pub(crate) history: Option<crate::history::HistoryCapture>,
}
impl LedgerShared {
    pub(crate) fn record(&self, fact: HistoryFact) {
        if let Some(history) = &self.history {
            history.record(fact);
        }
    }
}
#[derive(Clone)]
pub struct RuntimeLedger {
    pub(crate) shared: Arc<LedgerShared>,
}
#[derive(Clone)]
pub struct ScopeHandle {
    pub(crate) runtime: RuntimeLedger,
    pub(crate) id: ScopeId,
}
/// A child scope is one actual parent work responsibility until full child retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeParent {
    pub scope: ScopeId,
    pub generation: Generation,
    pub work: WorkId,
    pub kind: WorkKind,
    pub admission: WorkAdmission,
}
/// Production child admission. The parent work identity is minted inside the
/// same ledger transaction as the new scope, rather than prefetched by a host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeParentAdmission {
    pub scope: ScopeId,
    pub generation: Generation,
    pub kind: WorkKind,
    pub admission: WorkAdmission,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeGates {
    pub generation: Generation,
    pub external_open: bool,
    pub stopped: bool,
    pub failed: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeSnapshot {
    pub id: ScopeId,
    pub generation: Generation,
    pub external_open: bool,
    pub stopped: bool,
    pub failed: bool,
    pub work: usize,
    pub storage: usize,
    pub join_ready: bool,
    pub retirement_ready: bool,
    pub unresolved: usize,
}
impl RuntimeLedger {
    pub fn new(limits: ResourceVector, max_reservations: usize, max_scopes: usize) -> Self {
        Self::build(limits, max_reservations, max_scopes, None)
    }
    pub fn with_history(
        limits: ResourceVector,
        max_reservations: usize,
        max_scopes: usize,
        history: crate::history::HistoryCapture,
    ) -> Self {
        Self::build(limits, max_reservations, max_scopes, Some(history))
    }
    fn build(
        limits: ResourceVector,
        max_reservations: usize,
        max_scopes: usize,
        history: Option<crate::history::HistoryCapture>,
    ) -> Self {
        Self {
            shared: Arc::new(LedgerShared {
                state: Mutex::new(LedgerState {
                    ledger: CapacityLedger::new(limits, max_reservations),
                    scopes: BTreeMap::new(),
                    slots: BTreeMap::new(),
                    unresolved: BTreeSet::new(),
                    unresolved_by_scope: BTreeMap::new(),
                    abandoned_bundles: BTreeSet::new(),
                    pending_retirement: BTreeSet::new(),
                    parent_links: BTreeMap::new(),
                    max_scopes,
                    last_scope: 0,
                    last_slot: 0,
                    external_open: true,
                }),
                changed: Notify::default(),
                history,
            }),
        }
    }
    pub fn open_scope(
        &self,
        id: ScopeId,
        parent: Option<ScopeParent>,
        generation: Generation,
        max_work: usize,
        max_slots: usize,
    ) -> Result<ScopeHandle, RuntimeError> {
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        self.open_scope_locked(&mut state, id, parent, generation, max_work, max_slots)
    }
    /// Allocates the scope identity and any parent responsibility atomically
    /// with admission. Explicit open_scope IDs require an already serialized owner.
    pub fn open_scope_next(
        &self,
        parent: Option<ScopeParentAdmission>,
        generation: Generation,
        max_work: usize,
        max_slots: usize,
    ) -> Result<ScopeHandle, RuntimeError> {
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        let id = ScopeId::new(
            state
                .last_scope
                .checked_add(1)
                .ok_or(RuntimeError::IdentityExhausted)?,
        )
        .ok_or(RuntimeError::IdentityExhausted)?;
        let parent = parent
            .map(|parent| {
                let work = state
                    .scopes
                    .get(&parent.scope)
                    .ok_or(RuntimeError::UnknownScope)?
                    .next_work_id()?;
                Ok::<_, RuntimeError>(ScopeParent {
                    scope: parent.scope,
                    generation: parent.generation,
                    work,
                    kind: parent.kind,
                    admission: parent.admission,
                })
            })
            .transpose()?;
        self.open_scope_locked(&mut state, id, parent, generation, max_work, max_slots)
    }
    /// Bootstrap one child scope and its original root from a live parent.
    /// Parent-link admission, root reservation and publication share one ledger
    /// transaction, including when external intake has already closed. The
    /// root owns a real zero-resource reservation; the caller retains the
    /// original capacity payer for this finite header inventory. No quota is
    /// acquired again or inferred from the parent lease alone.
    ///
    /// The child inherits the parent's closed/stopped/failed gates before it
    /// becomes observable. A failed parent permits only its normal cleanup
    /// kinds, and failing the new child advances `child_generation` once, just
    /// like an existing scope failure. The original root remains admitted and
    /// can bind for cleanup; it cannot derive new Business work after failure.
    pub fn open_child_scope_with_root(
        &self,
        parent: &crate::lease::WorkLease,
        child_generation: Generation,
        max_work: usize,
        max_slots: usize,
        root_kind: WorkKind,
    ) -> Result<(ScopeHandle, StepGuard), RuntimeError> {
        if !Arc::ptr_eq(&self.shared, &parent.scope().runtime.shared) {
            return Err(CapacityError::WrongOwner.into());
        }
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        if state.scopes.len() >= state.max_scopes {
            return Err(RuntimeError::ScopeCapacity);
        }
        let id = state
            .last_scope
            .checked_add(1)
            .and_then(ScopeId::new)
            .ok_or(RuntimeError::IdentityExhausted)?;
        let parent_scope = parent.scope().id();
        let (parent_generation, link, parent_closed, parent_stopped, parent_failed) = {
            let owner = state
                .scopes
                .get(&parent_scope)
                .ok_or(RuntimeError::UnknownScope)?;
            let link = owner.next_work_id()?;
            owner.check_admission(
                owner.generation(),
                link,
                root_kind,
                WorkAdmission::Derived(parent.id()),
            )?;
            (
                owner.generation(),
                link,
                !owner.external_open(),
                owner.stopped(),
                owner.failed(),
            )
        };
        // No fallible gate transition may remain after reserving the real
        // root. Inherited failure uses the existing generation transition.
        if parent_failed && child_generation.next().is_none() {
            return Err(ScopeError::IdentityExhausted.into());
        }
        let mut child = ScopeState::new(
            id,
            Some(parent_scope),
            child_generation,
            max_work,
            max_slots,
        );
        let root = child.next_work_id()?;
        let reservation = state.ledger.next_reservation_id()?;
        // The local scope is not published yet. Its initial root is authorized
        // by the exact parent preflight above, rather than by opening a live
        // external gate. A refusal mutates neither ledger nor parent scope.
        state.ledger.try_reserve_bundle(
            &mut child,
            ReservationRequest {
                id: reservation,
                generation: child_generation,
                guard: root,
                resources: ResourceVector::default(),
                admission: WorkAdmission::External,
                kind: root_kind,
            },
        )?;
        if parent_failed {
            child
                .fail()
                .expect("inherited child failure generation was preflighted");
        } else if parent_stopped {
            child.request_stop();
        } else if parent_closed || !state.external_open {
            child.close_external();
        }
        // Only the private child and capacity ledger changed after preflight;
        // the same locked parent transition cannot now ordinarily refuse.
        state
            .scopes
            .get_mut(&parent_scope)
            .expect("preflighted parent remains live")
            .admit(
                parent_generation,
                link,
                root_kind,
                WorkAdmission::Derived(parent.id()),
            )
            .expect("same-transaction parent link admission was preflighted");
        assert!(
            state
                .parent_links
                .insert(id, (parent_scope, link))
                .is_none()
        );
        let generation = child.generation();
        assert!(state.scopes.insert(id, child).is_none());
        state.last_scope = id.get();
        self.shared.record(HistoryFact::WorkAdmitted {
            scope: parent_scope,
            work: link,
        });
        self.shared.record(HistoryFact::ScopeOpened {
            scope: id,
            generation: child_generation,
        });
        self.shared.record(HistoryFact::Admitted {
            scope: id,
            work: root,
            reservation,
        });
        if parent_failed {
            self.shared.record(HistoryFact::OwnerFailed {
                scope: id,
                generation,
            });
        } else if parent_stopped {
            self.shared.record(HistoryFact::ScopeStopped { scope: id });
        } else if parent_closed || !state.external_open {
            self.shared.record(HistoryFact::ScopeClosed { scope: id });
        }
        let scope = ScopeHandle {
            runtime: self.clone(),
            id,
        };
        drop(state);
        self.shared.changed.notify();
        Ok((scope.clone(), StepGuard::new(scope, root, reservation)))
    }
    fn open_scope_locked(
        &self,
        state: &mut LedgerState,
        id: ScopeId,
        parent: Option<ScopeParent>,
        generation: Generation,
        max_work: usize,
        max_slots: usize,
    ) -> Result<ScopeHandle, RuntimeError> {
        if !state.external_open
            && !parent.is_some_and(|parent| matches!(parent.admission, WorkAdmission::Derived(_)))
        {
            return Err(ScopeError::Closed.into());
        }
        if id.get() <= state.last_scope || state.scopes.contains_key(&id) {
            return Err(RuntimeError::DuplicateScope);
        }
        if state.scopes.len() >= state.max_scopes {
            return Err(RuntimeError::ScopeCapacity);
        }
        if let Some(parent) = parent {
            let owner = state
                .scopes
                .get_mut(&parent.scope)
                .ok_or(RuntimeError::UnknownScope)?;
            owner.admit(
                parent.generation,
                parent.work,
                parent.kind,
                parent.admission,
            )?;
            state.parent_links.insert(id, (parent.scope, parent.work));
            self.shared.record(HistoryFact::WorkAdmitted {
                scope: parent.scope,
                work: parent.work,
            });
        }
        state.scopes.insert(
            id,
            ScopeState::new(
                id,
                parent.map(|parent| parent.scope),
                generation,
                max_work,
                max_slots,
            ),
        );
        state.last_scope = id.get();
        if !state.external_open {
            state
                .scopes
                .get_mut(&id)
                .expect("new child scope")
                .close_external();
        }
        self.shared.record(HistoryFact::ScopeOpened {
            scope: id,
            generation,
        });
        Ok(ScopeHandle {
            runtime: self.clone(),
            id,
        })
    }
    pub fn used(&self, dimension: DimensionId) -> u64 {
        self.shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .ledger
            .used(dimension)
    }
    pub fn unresolved_work(&self) -> Vec<(ScopeId, WorkId)> {
        self.shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .unresolved
            .iter()
            .copied()
            .collect()
    }
    pub fn abandoned_bundles(&self) -> Vec<ReservationId> {
        self.shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .abandoned_bundles
            .iter()
            .copied()
            .collect()
    }
    /// Closes only external admission. A healthy ledger still serves admitted
    /// parent responsibilities during normal drain and permitted cleanup.
    pub fn close_admission(&self) {
        self.shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .external_open = false;
        self.shared.changed.notify();
    }
    /// Shutdown may close the existing scope gates in finite slices. The global
    /// external gate above already excludes new external work without a scan.
    pub fn close_scopes(&self, after: Option<ScopeId>, budget: usize) -> Vec<ScopeId> {
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        let lower = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let ids: Vec<_> = state
            .scopes
            .range((lower, std::ops::Bound::Unbounded))
            .take(budget)
            .map(|(id, _)| *id)
            .collect();
        for id in &ids {
            state
                .scopes
                .get_mut(id)
                .expect("selected scope")
                .close_external();
            self.shared.record(HistoryFact::ScopeClosed { scope: *id });
        }
        drop(state);
        self.shared.changed.notify();
        ids
    }
    /// Each retirement is one budget unit. A ready parent is queued for a later bounded visit.
    pub fn reap_ready(&self, budget: usize) -> Result<usize, RuntimeError> {
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        let mut retired = 0;
        for _ in 0..budget {
            let Some((scope, work)) = state.pending_retirement.pop_first() else {
                break;
            };
            self.retire_work_locked(&mut state, scope, work)?;
            retired += 1;
        }
        drop(state);
        if retired != 0 {
            self.shared.changed.notify();
        }
        Ok(retired)
    }
    /// Reaps only this exact owner's ready work. Each operation is an indexed
    /// scope range lookup plus one retirement; other owners cannot consume the
    /// budget or delay a newly ready parent. Zero budget never mutates state.
    pub fn reap_scope_ready(&self, scope: ScopeId, budget: usize) -> Result<usize, RuntimeError> {
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        if !state.scopes.contains_key(&scope) {
            return Err(RuntimeError::UnknownScope);
        }
        let bounds = (scope, WorkId::new(1).expect("minimum work identity"))
            ..=(scope, WorkId::new(u64::MAX).expect("maximum work identity"));
        let mut retired = 0;
        for _ in 0..budget {
            let Some((owner, work)) = state
                .pending_retirement
                .range(bounds.clone())
                .next()
                .copied()
            else {
                break;
            };
            state.pending_retirement.remove(&(owner, work));
            self.retire_work_locked(&mut state, owner, work)?;
            retired += 1;
        }
        drop(state);
        if retired != 0 {
            self.shared.changed.notify();
        }
        Ok(retired)
    }
    fn retire_work_locked(
        &self,
        state: &mut LedgerState,
        scope: ScopeId,
        work: WorkId,
    ) -> Result<(), RuntimeError> {
        let parent = state
            .scopes
            .get_mut(&scope)
            .ok_or(RuntimeError::UnknownScope)?
            .retire_work(work)?;
        state.clear_unresolved(scope, work);
        self.shared.record(HistoryFact::WorkRetired { scope, work });
        if let Some(parent) = parent {
            state.pending_retirement.insert((scope, parent));
        }
        Ok(())
    }
    pub fn remove_scope(&self, id: ScopeId) -> Result<(), RuntimeError> {
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        let scope = state.scopes.get(&id).ok_or(RuntimeError::UnknownScope)?;
        if !scope.retirement_ready()
            || state.ledger.reservation_count_for(id) != 0
            || state
                .scopes
                .values()
                .any(|child| child.parent() == Some(id))
        {
            return Err(RuntimeError::ScopeNotRetired);
        }
        state.scopes.remove(&id);
        if let Some((parent, work)) = state.parent_links.remove(&id) {
            let parent_scope = state
                .scopes
                .get_mut(&parent)
                .expect("child scope retains its parent");
            parent_scope.body_exited(work)?;
            parent_scope.captures_exited(work)?;
            self.shared.record(HistoryFact::BodyExited {
                scope: parent,
                work,
            });
            self.shared.record(HistoryFact::CapturesExited {
                scope: parent,
                work,
            });
            if parent_scope.work_exited(work)? {
                state.pending_retirement.insert((parent, work));
            }
        }
        drop(state);
        self.shared.changed.notify();
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        let state = self.shared.state.lock().expect("runtime ledger lock");
        state.scopes.is_empty() && state.ledger.reservation_count() == 0 && state.slots.is_empty()
    }
    pub fn change_version(&self) -> u64 {
        self.shared.changed.version()
    }
    pub fn wait_after(&self, version: u64) {
        self.shared.changed.wait_after(version);
    }
}
impl ScopeHandle {
    pub const fn id(&self) -> ScopeId {
        self.id
    }
    /// Identity of the same original scope, independent of its current gates
    /// or generation. Matching numbers in another ledger do not match. This
    /// read grants no admission, start, publication or capacity authority.
    pub fn same_scope(&self, other: &Self) -> bool {
        self.id == other.id && Arc::ptr_eq(&self.runtime.shared, &other.runtime.shared)
    }

    pub(crate) fn attach_actor(&self, slot: SlotId) -> Result<(), RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if !state.external_open {
            return Err(ScopeError::Closed.into());
        }
        let scope = state
            .scopes
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownScope)?;
        if !scope.external_open() {
            return Err(ScopeError::Closed.into());
        }
        state.reserve_slot(self.id, slot)?;
        self.runtime.shared.record(HistoryFact::SlotReserved {
            scope: self.id,
            slot,
        });
        Ok(())
    }
    pub(crate) fn attach_actor_next(&self) -> Result<SlotId, RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if !state.external_open {
            return Err(ScopeError::Closed.into());
        }
        let scope = state
            .scopes
            .get(&self.id)
            .ok_or(RuntimeError::UnknownScope)?;
        if !scope.external_open() {
            return Err(ScopeError::Closed.into());
        }
        let slot = state.next_slot_id(self.id)?;
        state.reserve_slot(self.id, slot)?;
        self.runtime.shared.record(HistoryFact::SlotReserved {
            scope: self.id,
            slot,
        });
        Ok(slot)
    }
    /// Attach a pure actor under its original live failure-cleanup authority.
    /// External closure does not discard this existing responsibility. The
    /// current scope and Work are rechecked in the same transaction as slot
    /// admission, even if an earlier driver preflight already inspected them.
    pub(crate) fn attach_admitted_actor_next(
        &self,
        failure_work: WorkId,
    ) -> Result<SlotId, RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope = state
            .scopes
            .get(&self.id)
            .ok_or(RuntimeError::UnknownScope)?;
        if scope.failed() {
            return Err(ScopeError::Failed.into());
        }
        let work = scope.work(failure_work).ok_or(ScopeError::UnknownWork)?;
        if work.kind != WorkKind::Cleanup
            || work.unbound_guard
            || work.body_exited
            || work.captures_exited
        {
            return Err(ScopeError::InvalidTransition.into());
        }
        let slot = state.next_slot_id(self.id)?;
        state.reserve_slot(self.id, slot)?;
        self.runtime.shared.record(HistoryFact::SlotReserved {
            scope: self.id,
            slot,
        });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(slot)
    }
    pub(crate) fn detach_actor(&self, slot: SlotId) -> Result<(), RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state
            .scopes
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownScope)?
            .retire_slot(slot)?;
        self.runtime
            .shared
            .record(HistoryFact::SlotRetired { slot });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(())
    }
    /// Hot-path gates do not enumerate work, slots, unresolved exits or reservations.
    pub fn gates(&self) -> Result<ScopeGates, RuntimeError> {
        let state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope = state
            .scopes
            .get(&self.id)
            .ok_or(RuntimeError::UnknownScope)?;
        Ok(ScopeGates {
            generation: scope.generation(),
            external_open: state.external_open && scope.external_open(),
            stopped: scope.stopped(),
            failed: scope.failed(),
        })
    }
    pub fn join_ready(&self) -> Result<bool, RuntimeError> {
        let state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        Ok(state
            .scopes
            .get(&self.id)
            .ok_or(RuntimeError::UnknownScope)?
            .join_ready())
    }
    pub fn snapshot(&self) -> Result<ScopeSnapshot, RuntimeError> {
        let state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope = state
            .scopes
            .get(&self.id)
            .ok_or(RuntimeError::UnknownScope)?;
        Ok(ScopeSnapshot {
            id: self.id,
            generation: scope.generation(),
            external_open: state.external_open && scope.external_open(),
            stopped: scope.stopped(),
            failed: scope.failed(),
            work: scope.work_count(),
            storage: scope.storage_count(),
            join_ready: scope.join_ready(),
            retirement_ready: scope.retirement_ready()
                && state.ledger.reservation_count_for(self.id) == 0,
            unresolved: state
                .unresolved_by_scope
                .get(&self.id)
                .copied()
                .unwrap_or(0),
        })
    }
    /// Production allocation: identity selection and admission are one ledger
    /// transaction. A preempted producer never loses to a later external ID.
    pub fn reserve_next(
        &self,
        generation: Generation,
        resources: ResourceVector,
        kind: WorkKind,
        admission: WorkAdmission,
    ) -> Result<StepGuard, RuntimeError> {
        self.reserve_next_parent(generation, resources, kind, admission, None)
    }
    /// Derives from the actual work holding an existing bound reservation.
    /// Owner lookup, parent liveness, stop/generation checks, identity issuance
    /// and capacity admission are one transaction. No host map or copied
    /// parent identity participates. External closure permits existing drain;
    /// actual parent exit prevents zero-to-nonzero resurrection.
    pub fn reserve_child_next(
        &self,
        generation: Generation,
        parent: ReservationId,
        resources: ResourceVector,
        kind: WorkKind,
    ) -> Result<StepGuard, RuntimeError> {
        self.reserve_next_parent(
            generation,
            resources,
            kind,
            WorkAdmission::External,
            Some(parent),
        )
    }
    fn reserve_next_parent(
        &self,
        generation: Generation,
        resources: ResourceVector,
        kind: WorkKind,
        admission: WorkAdmission,
        parent: Option<ReservationId>,
    ) -> Result<StepGuard, RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let admission = if let Some(parent) = parent {
            let record = state
                .ledger
                .reservation(parent)
                .ok_or(CapacityError::UnknownReservation)?;
            if record.scope != self.id {
                return Err(CapacityError::WrongOwner.into());
            }
            if !record.bound {
                return Err(CapacityError::UnboundReservation.into());
            }
            WorkAdmission::Derived(record.work)
        } else {
            admission
        };
        if !state.external_open && admission == WorkAdmission::External {
            return Err(ScopeError::Closed.into());
        }
        let (ledger, scope) = state.parts(self.id)?;
        let work = scope.next_work_id()?;
        let reservation = ledger.next_reservation_id()?;
        // The fixed dimension inventory is plain metadata. Capture a denial
        // in this transaction, rather than sampling counters after unlocking
        // and attaching another thread's later state to the original refusal.
        let requested = resources.clone();
        let reserved = ledger.try_reserve_bundle(
            scope,
            ReservationRequest {
                id: reservation,
                generation,
                guard: work,
                resources,
                admission,
                kind,
            },
        );
        if let Err(CapacityError::Exhausted(dimension)) = reserved {
            return Err(RuntimeError::CapacityDenied(CapacityDenial {
                dimension,
                requested: requested.get(dimension),
                committed: ledger.used(dimension),
                limit: ledger.limit(dimension),
            }));
        }
        reserved?;
        self.runtime.shared.record(HistoryFact::Admitted {
            scope: self.id,
            work,
            reservation,
        });
        Ok(StepGuard::new(self.clone(), work, reservation))
    }
    /// Explicit identities are for deterministic tests or an already serialized
    /// identity owner. Concurrent production callers use reserve_next.
    pub fn reserve(&self, request: ReservationRequest) -> Result<StepGuard, RuntimeError> {
        let reservation = request.id;
        let work = request.guard;
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if !state.external_open && request.admission == WorkAdmission::External {
            return Err(ScopeError::Closed.into());
        }
        let (ledger, scope) = state.parts(self.id)?;
        ledger.try_reserve_bundle(scope, request)?;
        self.runtime.shared.record(HistoryFact::Admitted {
            scope: self.id,
            work,
            reservation,
        });
        Ok(StepGuard::new(self.clone(), work, reservation))
    }
    pub fn close_external(&self) -> Result<(), RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state
            .scopes
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownScope)?
            .close_external();
        self.runtime
            .shared
            .record(HistoryFact::ScopeClosed { scope: self.id });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(())
    }
    pub fn request_stop(&self) -> Result<(), RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state
            .scopes
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownScope)?
            .request_stop();
        self.runtime
            .shared
            .record(HistoryFact::ScopeStopped { scope: self.id });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(())
    }
    pub fn fail(&self) -> Result<Generation, RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let generation = state
            .scopes
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownScope)?
            .fail()?;
        self.runtime.shared.record(HistoryFact::OwnerFailed {
            scope: self.id,
            generation,
        });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(generation)
    }
    pub fn admit_work(
        &self,
        generation: Generation,
        id: WorkId,
        kind: WorkKind,
        admission: WorkAdmission,
    ) -> Result<crate::lease::WorkLease, RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if !state.external_open && admission == WorkAdmission::External {
            return Err(ScopeError::Closed.into());
        }
        state
            .scopes
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownScope)?
            .admit(generation, id, kind, admission)?;
        self.runtime.shared.record(HistoryFact::WorkAdmitted {
            scope: self.id,
            work: id,
        });
        Ok(crate::lease::WorkLease::new(self.clone(), id))
    }
}
