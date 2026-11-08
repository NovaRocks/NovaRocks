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

//! Non-cloneable work and capacity ownership. Drop never invents physical exit.

use crate::transaction::{RuntimeError, ScopeHandle};
use novarocks_control_runtime_core::{
    capacity::{KeyedQuota, ReservationRequest, ReservationResources, ResourceVector},
    history::{CapacityHistoryRequest, HistoryFact},
    identity::{Generation, ReservationId, ScopedWorkRef, SlotId, WorkId},
    owned_storage::{OwnedPosition, OwnedSlotKind, OwnedStorageError},
    scope::{ScopeError, WorkAdmission, WorkKind},
};

#[must_use = "A reservation must be bound to its actual holder or rolled back before it acquires a payload"]
pub struct StepRegistration {
    scope: ScopeHandle,
    work: WorkId,
    reservation: ReservationId,
    active: bool,
}
impl StepRegistration {
    pub(crate) fn new(scope: ScopeHandle, work: WorkId, reservation: ReservationId) -> Self {
        Self {
            scope,
            work,
            reservation,
            active: true,
        }
    }
    pub fn bind(
        mut self,
        kind: WorkKind,
    ) -> Result<(WorkObligation, ReservationObligation), RuntimeError> {
        {
            let mut state = self
                .scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            let (ledger, scope) = state.parts(self.scope.id)?;
            ledger.bind(scope, self.reservation, kind)?;
        }
        self.active = false;
        Ok((
            WorkObligation::new(self.scope.clone(), self.work),
            ReservationObligation {
                scope: self.scope.clone(),
                id: self.reservation,
                active: true,
            },
        ))
    }
    pub const fn work_id(&self) -> WorkId {
        self.work
    }
    pub const fn reservation_id(&self) -> ReservationId {
        self.reservation
    }
    pub fn scope(&self) -> &ScopeHandle {
        &self.scope
    }
    pub(crate) fn belongs_to(&self, scope: &ScopeHandle) -> bool {
        self.scope.id() == scope.id()
            && crate::sync::Arc::ptr_eq(&self.scope.runtime.shared, &scope.runtime.shared)
    }
}
impl Drop for StepRegistration {
    fn drop(&mut self) {
        if self.active {
            let mut state = self
                .scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            let parent = state
                .scopes
                .get(&self.scope.id)
                .and_then(|scope| scope.work(self.work))
                .expect("live unbound guard work")
                .parent;
            let released = (state.capacity_waiters.len() != 0).then(|| {
                state
                    .ledger
                    .reservation(self.reservation)
                    .expect("original unbound reservation")
                    .resources()
            });
            let (ledger, scope) = state
                .parts(self.scope.id)
                .expect("live scope retains its reservation");
            ledger
                .rollback_unbound(scope, self.reservation)
                .expect("only a pure unbound guard rolls back");
            // An exiting parent can have waited on precisely this child.
            // Rollback is its real unused-child retirement, so enqueue the
            // newly ready parent under the same transaction as that removal.
            let ready_parent = parent.filter(|parent| {
                scope
                    .work_header_retirement_ready(*parent)
                    .expect("unbound child retains its actual parent")
            });
            if let Some(parent) = ready_parent {
                state.note_header_ready(self.scope.id, parent);
            }
            self.scope
                .runtime
                .shared
                .record(|| HistoryFact::UnboundReservationRolledBack {
                    scope: self.scope.id,
                    work: self.work,
                    reservation: self.reservation,
                });
            let wakes = released.as_ref().map_or_else(Vec::new, |released| {
                self.scope.runtime.take_capacity_wakes(&mut state, released)
            });
            drop(state);
            self.scope.runtime.shared.changed.notify();
            self.scope.runtime.deliver_capacity_wakes(wakes);
        }
    }
}

/// An actual original header reference owned by a resident input/role/batch.
/// No Drop callback runs under driver locks; only its real retirement releases it.
#[must_use = "Retire the same original input header custody explicitly"]
pub(crate) struct WorkHeaderObligation {
    scope: ScopeHandle,
    original: novarocks_control_runtime_core::scope::WorkHeaderObligation,
}
impl WorkHeaderObligation {
    fn acquire_locked(
        scope: &ScopeHandle,
        work: WorkId,
        ledger: &mut crate::transaction::LedgerState,
    ) -> Result<Self, RuntimeError> {
        let original = ledger
            .scopes
            .get_mut(&scope.id())
            .ok_or(RuntimeError::UnknownScope)?
            .pin_work_header(work)?;
        ledger.pending_retirement.remove(&(scope.id(), work));
        Ok(Self {
            scope: scope.clone(),
            original,
        })
    }
    pub(crate) fn for_work_locked(
        work: &WorkObligation,
        ledger: &mut crate::transaction::LedgerState,
    ) -> Result<Self, RuntimeError> {
        Self::acquire_locked(work.scope(), work.id(), ledger)
    }
    pub(crate) fn for_guard_locked(
        guard: &StepRegistration,
        ledger: &mut crate::transaction::LedgerState,
    ) -> Result<Self, RuntimeError> {
        if !guard.active {
            return Err(ScopeError::InvalidTransition.into());
        }
        Self::acquire_locked(guard.scope(), guard.work_id(), ledger)
    }
    /// The driver has qualified this original binding at the actual input or
    /// batch boundary. A readonly clone alone does not acquire header custody.
    pub(crate) fn for_input_author_locked(
        author: &crate::actor_turn::InputAuthor,
        ledger: &mut crate::transaction::LedgerState,
    ) -> Result<Self, RuntimeError> {
        Self::acquire_locked(&author.scope, author.work().work, ledger)
    }
    pub(crate) fn reference(&self) -> novarocks_control_runtime_core::identity::ScopedWorkRef {
        novarocks_control_runtime_core::identity::ScopedWorkRef {
            scope: self.scope.id(),
            work: self.original.work(),
        }
    }
    pub(crate) fn belongs_to(&self, scope: &ScopeHandle) -> bool {
        self.scope.same_scope(scope)
    }
    pub(crate) fn retain_locked(
        &self,
        ledger: &mut crate::transaction::LedgerState,
    ) -> Result<Self, RuntimeError> {
        let original = ledger
            .scopes
            .get_mut(&self.scope.id())
            .ok_or(RuntimeError::UnknownScope)?
            .retain_work_header(&self.original)?;
        ledger
            .pending_retirement
            .remove(&(self.scope.id(), self.original.work()));
        Ok(Self {
            scope: self.scope.clone(),
            original,
        })
    }
    pub(crate) fn release_locked(self, ledger: &mut crate::transaction::LedgerState) {
        let work = self.original.work();
        let ready = ledger
            .scopes
            .get_mut(&self.scope.id())
            .expect("original input pin retains its Scope")
            .release_work_header(self.original)
            .expect("only the original actual holder releases its header pin");
        if ready {
            ledger.note_header_ready(self.scope.id(), work);
        }
        self.scope.runtime.shared.changed.notify();
    }
}
#[must_use = "Work remains counted until body, captures and children actually exit"]
pub struct WorkObligation {
    scope: ScopeHandle,
    id: WorkId,
    body_exited: bool,
    captures_exited: bool,
    header_readiness_issued: bool,
}
/// Derivation authority from one actual bound parent. Clones retain only
/// that original ledger identity: they pin no header, admit no Work, and do
/// not keep the parent alive. Every use rechecks its actual lifecycle.
#[derive(Clone)]
pub struct WorkChildAuthority {
    scope: ScopeHandle,
    parent: WorkId,
}
impl std::fmt::Debug for WorkChildAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkChildAuthority")
            .field("scope", &self.scope.id())
            .field("parent", &self.parent)
            .finish()
    }
}
/// A successful point-in-time admission check. It retains neither a ledger nor
/// derivation authority and does not prove an application identity. Identity
/// owners use it while issuing their domain-specific execution tokens.
#[derive(Clone, Debug)]
pub struct AdmissionProof {
    _private: (),
}
impl WorkChildAuthority {
    /// Verifies the same original bound parent without prolonging its lifetime.
    /// The proof grants no capacity, derivation or future lifecycle guarantee.
    pub fn verify_live(&self) -> Result<AdmissionProof, RuntimeError> {
        let state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope = state
            .scopes
            .get(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?;
        let work = scope.work(self.parent).ok_or(ScopeError::UnknownWork)?;
        if work.unbound_guard || work.body_exited || work.captures_exited {
            return Err(ScopeError::InvalidTransition.into());
        }
        if state.unresolved.contains(&(self.scope.id, self.parent)) {
            return Err(RuntimeError::UnresolvedWork);
        }
        Ok(AdmissionProof { _private: () })
    }
    pub(crate) fn scope(&self) -> &ScopeHandle {
        &self.scope
    }
    pub(crate) const fn parent(&self) -> WorkId {
        self.parent
    }
}
/// Read-only identity of an actually bound Work. It observes the common
/// reaper's removal of that exact header, independently of body exit, report
/// retirement and capacity release. It admits no Work and acknowledges no leg.
#[derive(Clone)]
pub struct WorkRetirementObserver {
    scope: ScopeHandle,
    work: WorkId,
}
impl std::fmt::Debug for WorkRetirementObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkRetirementObserver")
            .field("scope", &self.scope.id())
            .field("work", &self.work)
            .finish()
    }
}
impl WorkRetirementObserver {
    pub(crate) fn matches(
        &self,
        scope: &ScopeHandle,
        work: novarocks_control_runtime_core::identity::ScopedWorkRef,
    ) -> bool {
        self.scope.same_scope(scope) && self.scope.id() == work.scope && self.work == work.work
    }
    pub(crate) fn for_ledger(
        &self,
        ledger: &crate::transaction::RuntimeLedger,
    ) -> Option<(novarocks_control_runtime_core::identity::ScopeId, WorkId)> {
        crate::sync::Arc::ptr_eq(&self.scope.runtime.shared, &ledger.shared)
            .then_some((self.scope.id(), self.work))
    }
    /// Same original header's pin and child counts, without release authority.
    /// Absence observes true retirement of this originally bound header.
    pub fn header_holders(&self) -> Option<(usize, usize)> {
        let state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("original work header custody observation");
        let record = state.scopes.get(&self.scope.id())?.work(self.work)?;
        Some((record.header_pin_count(), record.children))
    }
    pub fn is_retired(&self) -> bool {
        let state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        // This identity can only come from a bound lease. Bound Work cannot
        // roll back, IDs never repeat, and removing its scope requires every
        // Work header to have retired. Absence therefore has exact provenance.
        state
            .scopes
            .get(&self.scope.id())
            .is_none_or(|scope| scope.work(self.work).is_none())
    }
}
impl WorkObligation {
    pub(crate) fn new(scope: ScopeHandle, id: WorkId) -> Self {
        Self {
            scope,
            id,
            body_exited: false,
            captures_exited: false,
            header_readiness_issued: false,
        }
    }
    /// Sets one opaque diagnostic label on an actually bound live Work.
    /// Labels never grant authority or participate in retirement decisions.
    pub fn set_diagnostic_label(&self, label: &'static str) -> Result<(), RuntimeError> {
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let work = state
            .scopes
            .get(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?
            .work(self.id)
            .ok_or(ScopeError::UnknownWork)?;
        if work.unbound_guard
            || work.body_exited
            || work.captures_exited
            || state.work_labels.contains_key(&(self.scope.id, self.id))
        {
            return Err(ScopeError::InvalidTransition.into());
        }
        state.work_labels.insert((self.scope.id, self.id), label);
        Ok(())
    }
    pub const fn id(&self) -> WorkId {
        self.id
    }
    /// Authorize future derivation from this actual bound parent. Issuance
    /// does not reserve capacity or prolong its body, captures or header.
    pub fn child_authority(&self) -> Result<WorkChildAuthority, RuntimeError> {
        let state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope = state
            .scopes
            .get(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?;
        let work = scope.work(self.id).ok_or(ScopeError::UnknownWork)?;
        if work.unbound_guard || work.body_exited || work.captures_exited {
            return Err(ScopeError::InvalidTransition.into());
        }
        if state.unresolved.contains(&(self.scope.id, self.id)) {
            return Err(RuntimeError::UnresolvedWork);
        }
        Ok(WorkChildAuthority {
            scope: self.scope.clone(),
            parent: self.id,
        })
    }
    /// Read the actual original header; this grants no start or exit right.
    pub fn kind(&self) -> Result<WorkKind, RuntimeError> {
        let state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        Ok(state
            .scopes
            .get(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?
            .work(self.id)
            .ok_or(RuntimeError::Scope(ScopeError::UnknownWork))?
            .kind)
    }
    /// The driver already observed and retired this same capture-free metadata
    /// Work under its original ledger transaction. This updates only the local
    /// owner's Drop bookkeeping; it never creates an exit or touches the ledger.
    pub(crate) fn note_metadata_exit_observed(&mut self) {
        self.body_exited = true;
        self.captures_exited = true;
    }
    /// Read-only address of this same admitted Work; it signs no authority.
    pub fn reference(&self) -> novarocks_control_runtime_core::identity::ScopedWorkRef {
        novarocks_control_runtime_core::identity::ScopedWorkRef {
            scope: self.scope.id(),
            work: self.id,
        }
    }
    pub fn scope(&self) -> &ScopeHandle {
        &self.scope
    }
    /// Read-only declared positions held by this actual role, not capacity.
    pub(crate) fn capture_positions(&self) -> Result<usize, RuntimeError> {
        let state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        Ok(state
            .scopes
            .get(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?
            .capture_positions(self.id)?)
    }
    /// Issue one original neutral readiness source before handing this Work to
    /// its exit owner. This neither pins its header nor changes physical exit.
    pub fn header_readiness_registration(
        &mut self,
    ) -> Result<crate::header_readiness::HeaderReadyRegistration, RuntimeError> {
        if self.header_readiness_issued {
            return Err(ScopeError::InvalidTransition.into());
        }
        {
            let state = self
                .scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            let work = state
                .scopes
                .get(&self.scope.id)
                .ok_or(RuntimeError::UnknownScope)?
                .work(self.id)
                .ok_or(ScopeError::UnknownWork)?;
            if work.unbound_guard || work.body_exited || work.captures_exited {
                return Err(ScopeError::InvalidTransition.into());
            }
            if state.unresolved.contains(&(self.scope.id, self.id)) {
                return Err(RuntimeError::UnresolvedWork);
            }
        }
        self.header_readiness_issued = true;
        Ok(crate::header_readiness::HeaderReadyRegistration::new(
            self.scope.clone(),
            self.reference(),
        ))
    }
    pub fn retirement_observer(&self) -> WorkRetirementObserver {
        WorkRetirementObserver {
            scope: self.scope.clone(),
            work: self.id,
        }
    }
    /// Actual exit includes the complete admitted child tree. A retained
    /// lease may outlive its pure record: this non-forgeable handle is created
    /// only after admission, and its local exit flags can become true only
    /// after that record accepted both actual observations. Bound work cannot
    /// roll back and IDs never repeat, so absence after those observations is
    /// known successful retirement (also after full scope removal).
    pub fn actual_exit_ready(&self) -> Result<bool, RuntimeError> {
        let state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let observed_exit = self.body_exited && self.captures_exited;
        let Some(scope) = state.scopes.get(&self.scope.id) else {
            return if observed_exit {
                Ok(true)
            } else {
                Err(RuntimeError::UnknownScope)
            };
        };
        match scope.work_exited(self.id) {
            Err(ScopeError::UnknownWork) if observed_exit => Ok(true),
            result => result.map_err(Into::into),
        }
    }
    pub fn start(&mut self) -> Result<(), RuntimeError> {
        self.start_guarded(None)
    }
    /// The host grants a delayed call start in the same transaction as scope
    /// failure. A rejected old generation has not started or lost its work.
    pub fn start_in_generation(&mut self, generation: Generation) -> Result<(), RuntimeError> {
        self.start_guarded(Some(generation))
    }
    /// A delayed child business call obeys its actual direct parent's original
    /// stop/failure generation. Cleanup may still use ordinary start afterward.
    pub fn start_in_parent_generation(
        &mut self,
        parent: &ScopeHandle,
        generation: Generation,
    ) -> Result<(), RuntimeError> {
        if !crate::sync::Arc::ptr_eq(&self.scope.runtime.shared, &parent.runtime.shared) {
            return Err(novarocks_control_runtime_core::capacity::CapacityError::WrongOwner.into());
        }
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state.validate_parent_start(self.scope.id, parent.id, generation)?;
        if state.unresolved.contains(&(self.scope.id, self.id)) {
            return Err(RuntimeError::UnresolvedWork);
        }
        state
            .scopes
            .get_mut(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?
            .start(self.id)?;
        self.scope.runtime.shared.record(|| HistoryFact::Started {
            scope: self.scope.id,
            work: self.id,
        });
        Ok(())
    }
    fn start_guarded(&mut self, generation: Option<Generation>) -> Result<(), RuntimeError> {
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope = state
            .scopes
            .get_mut(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?;
        if generation.is_some_and(|generation| generation != scope.generation()) {
            return Err(ScopeError::GenerationMismatch.into());
        }
        scope.start(self.id)?;
        self.scope.runtime.shared.record(|| HistoryFact::Started {
            scope: self.scope.id,
            work: self.id,
        });
        Ok(())
    }
    /// Records unknown exit while the actual custodian keeps this lease.
    /// Diagnostic responsibility is not a substitute for physical exit.
    pub fn mark_unresolved(&self) {
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if state.note_unresolved(self.scope.id, self.id) {
            self.scope
                .runtime
                .shared
                .record(|| HistoryFact::WorkUnresolved {
                    scope: self.scope.id,
                    work: self.id,
                });
        }
        drop(state);
        self.scope.runtime.shared.changed.notify();
    }
    /// Bind exactly the declared capture containers for this actual role.
    /// Future Source positions remain with the original bundle. The same
    /// WorkRecord owns this span until its real captures_exited observation.
    pub fn bind_owned_captures(
        &mut self,
        capacity: &mut ReservationObligation,
        count: usize,
    ) -> Result<(), RuntimeError> {
        if !self.scope.same_scope(&capacity.scope) || !capacity.active {
            return Err(OwnedStorageError::WrongOwner.into());
        }
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let funding = state
            .ledger
            .reservation(capacity.id)
            .ok_or(novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation)?;
        if funding.scope != self.scope.id || funding.work != self.id || !funding.bound {
            return Err(OwnedStorageError::WrongOwner.into());
        }
        if funding.owned_slots().captures < count {
            return Err(OwnedStorageError::MissingPosition(OwnedSlotKind::Capture).into());
        }
        if count == 0 {
            return Ok(());
        }
        let owner = state
            .scopes
            .get(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?;
        let work = owner.work(self.id).ok_or(ScopeError::UnknownWork)?;
        if work.unbound_guard
            || work.body_exited
            || work.captures_exited
            || owner.captures_bound(self.id)?
        {
            return Err(ScopeError::InvalidTransition.into());
        }
        let captures = state
            .ledger
            .take_owned_span(capacity.id, OwnedSlotKind::Capture, count)?
            .expect("preflighted original capture span");
        state
            .scopes
            .get_mut(&self.scope.id)
            .expect("original owner")
            .bind_captures(self.id, captures)
            .expect("same transaction capture binding");
        Ok(())
    }
    pub fn derive(
        &self,
        mut request: ReservationRequest,
    ) -> Result<StepRegistration, RuntimeError> {
        request.admission = WorkAdmission::Derived(self.id);
        self.scope.reserve(request)
    }
    pub fn body_exited(&mut self) -> Result<(), RuntimeError> {
        self.observe_body(None)
    }
    /// Publishes the pre-reserved body-exit fact in the same transaction as its observation.
    pub fn body_exited_with_report(
        &mut self,
        report: &crate::completion::ReportSlot,
    ) -> Result<(), RuntimeError> {
        self.observe_body(Some(report))
    }
    fn observe_body(
        &mut self,
        report: Option<&crate::completion::ReportSlot>,
    ) -> Result<(), RuntimeError> {
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if let Some(report) = report {
            report.validate_fill_for(&self.scope.runtime, &state, self.scope.id, self.id)?;
        }
        let scope = state
            .scopes
            .get_mut(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?;
        scope.body_exited(self.id)?;
        self.body_exited = true;
        let ready = scope.work_header_retirement_ready(self.id)?;
        self.scope
            .runtime
            .shared
            .record(|| HistoryFact::BodyExited {
                scope: self.scope.id,
                work: self.id,
            });
        if let Some(report) = report {
            report.fill_validated(&mut state);
        }
        if ready {
            state.note_header_ready(self.scope.id, self.id);
        }
        drop(state);
        self.scope.runtime.shared.changed.notify();
        Ok(())
    }
    pub fn captures_exited(&mut self) -> Result<(), RuntimeError> {
        self.observe_captures(None)
    }
    /// Publishes the pre-reserved capture-exit fact without a release/fill panic gap.
    pub fn captures_exited_with_report(
        &mut self,
        report: &crate::completion::ReportSlot,
    ) -> Result<(), RuntimeError> {
        self.observe_captures(Some(report))
    }
    fn observe_captures(
        &mut self,
        report: Option<&crate::completion::ReportSlot>,
    ) -> Result<(), RuntimeError> {
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if let Some(report) = report {
            report.validate_fill_for(&self.scope.runtime, &state, self.scope.id, self.id)?;
        }
        let scope = state
            .scopes
            .get_mut(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?;
        scope.captures_exited(self.id)?;
        self.captures_exited = true;
        let ready = scope.work_header_retirement_ready(self.id)?;
        self.scope
            .runtime
            .shared
            .record(|| HistoryFact::CapturesExited {
                scope: self.scope.id,
                work: self.id,
            });
        if let Some(report) = report {
            report.fill_validated(&mut state);
        }
        if ready {
            state.note_header_ready(self.scope.id, self.id);
        }
        drop(state);
        self.scope.runtime.shared.changed.notify();
        Ok(())
    }
}
impl Drop for WorkObligation {
    fn drop(&mut self) {
        if !self.body_exited || !self.captures_exited {
            self.mark_unresolved();
        }
    }
}

#[must_use = "Bound capacity follows its holder until an explicit release fact"]
pub struct ReservationObligation {
    pub(crate) scope: ScopeHandle,
    pub(crate) id: ReservationId,
    pub(crate) active: bool,
}
impl ReservationObligation {
    /// Validate only original ownership, under one original ledger lock. No
    /// lifecycle, generation, health or resource-release fact is inferred.
    pub(crate) fn validate_reported_work_binding(
        &self,
        work: &WorkObligation,
        report: &crate::completion::ReportSlot,
    ) -> Result<(), crate::reported_work::ReportedWorkBindingError> {
        use crate::{reported_work::ReportedWorkBindingError, sync::Arc};
        let state = work
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if !Arc::ptr_eq(&work.scope.runtime.shared, &self.scope.runtime.shared)
            || !Arc::ptr_eq(&work.scope.runtime.shared, &report.runtime.shared)
        {
            return Err(ReportedWorkBindingError::DifferentLedger);
        }
        if self.scope.id != work.scope.id {
            return Err(ReportedWorkBindingError::WrongScope);
        }
        let owner = state
            .scopes
            .get(&work.scope.id)
            .ok_or(ReportedWorkBindingError::UnknownScope)?;
        let original_work = owner
            .work(work.id)
            .ok_or(ReportedWorkBindingError::UnknownWork)?;
        if original_work.unbound_guard {
            return Err(ReportedWorkBindingError::UnboundWork);
        }
        if !self.active {
            return Err(ReportedWorkBindingError::InactiveCapacity);
        }
        let capacity = state
            .ledger
            .reservation(self.id)
            .ok_or(ReportedWorkBindingError::UnknownCapacity)?;
        if !capacity.bound {
            return Err(ReportedWorkBindingError::UnboundCapacity);
        }
        if capacity.scope != work.scope.id {
            return Err(ReportedWorkBindingError::WrongScope);
        }
        if capacity.work != work.id {
            return Err(ReportedWorkBindingError::CapacityWorkMismatch);
        }
        let original_report = state
            .slots
            .get(&report.id)
            .ok_or(ReportedWorkBindingError::UnknownReport)?;
        if original_report.storage_reservation().is_none() {
            return Err(ReportedWorkBindingError::UnknownReport);
        }
        if original_report.scope() != work.scope.id {
            return Err(ReportedWorkBindingError::WrongScope);
        }
        if original_report.producer() != work.id {
            return Err(ReportedWorkBindingError::ReportProducerMismatch);
        }
        Ok(())
    }
    pub const fn id(&self) -> ReservationId {
        self.id
    }
    /// This owned bundle has returned its entire reservation. A preparation
    /// remainder may be empty after releasing its last P/byte charges; it
    /// still carries the transfer receipt, but must not release them twice.
    /// This observes no body, capture, report or Work-header retirement.
    pub const fn is_released(&self) -> bool {
        !self.active
    }
    /// Exact observation only. A known fully moved/finished bundle has zero
    /// remaining resources; a missing active record is never a fallback.
    pub fn resources(&self) -> Result<ReservationResources, RuntimeError> {
        if !self.active {
            return Ok(ReservationResources::default());
        }
        let state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        Ok(state
            .ledger
            .reservation(self.id)
            .ok_or(novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation)?
            .resources())
    }
    pub fn release_quota(&mut self, resources: &ResourceVector) -> Result<(), RuntimeError> {
        self.release_report(resources, None, None)
    }
    pub fn release_quota_with_keyed(
        &mut self,
        resources: &ResourceVector,
        keyed: Option<&KeyedQuota>,
    ) -> Result<(), RuntimeError> {
        self.release_report(resources, keyed, None)
    }
    /// Capacity release and its reliable fact fill have one linearization point.
    /// Producer/capture retirement and each consumer acknowledgement remain separate.
    pub fn release_quota_with_report(
        &mut self,
        resources: &ResourceVector,
        report: &crate::completion::ReportSlot,
    ) -> Result<(), RuntimeError> {
        self.release_report(resources, None, Some(report))
    }
    /// Releases the same original process/keyed shares and fills their prepaid
    /// fact in one transaction. It does not establish producer or capture exit.
    pub fn release_quota_with_keyed_and_report(
        &mut self,
        resources: &ResourceVector,
        keyed: Option<&KeyedQuota>,
        report: &crate::completion::ReportSlot,
    ) -> Result<(), RuntimeError> {
        self.release_report(resources, keyed, Some(report))
    }
    fn release_report(
        &mut self,
        resources: &ResourceVector,
        keyed: Option<&KeyedQuota>,
        report: Option<&crate::completion::ReportSlot>,
    ) -> Result<(), RuntimeError> {
        let keyed_empty = keyed.is_none_or(|keyed| keyed.quota.is_empty());
        // Preserve the original local no-op/error fast path without history.
        // A successful idempotent operation has no ledger transition to trace.
        if !self.active
            && (self.scope.runtime.shared.history.is_none()
                || (resources.is_empty() && keyed_empty && report.is_none()))
        {
            let result = if resources.is_empty() && keyed_empty && report.is_none() {
                Ok(())
            } else {
                Err(
                    novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation
                        .into(),
                )
            };
            self.scope
                .runtime
                .shared
                .observation
                .capacity_runtime(&result);
            return result;
        }
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let refusal_intent = self
            .scope
            .runtime
            .shared
            .history
            .as_ref()
            .map(|_| match keyed {
                Some(keyed) => CapacityHistoryRequest::ReleaseKeyedQuota {
                    scope: self.scope.id,
                    source: self.id,
                    generation: state.ledger.reservation(self.id).map(|r| r.generation),
                    requested: resources.clone(),
                    keyed: keyed.clone(),
                },
                None => CapacityHistoryRequest::ReleaseQuota {
                    scope: self.scope.id,
                    source: self.id,
                    generation: state.ledger.reservation(self.id).map(|r| r.generation),
                    requested: resources.clone(),
                },
            });
        let mut notify = false;
        let mut wakes = Vec::new();
        let result = (|| {
            if !self.active {
                return if resources.is_empty() && keyed_empty && report.is_none() {
                    Ok(())
                } else {
                    Err(
                        novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation
                            .into(),
                    )
                };
            }

            if let Some(report) = report {
                let owner = state.ledger.reservation(self.id).ok_or(
                    novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation,
                )?;
                report.validate_fill_for(&self.scope.runtime, &state, owner.scope, owner.work)?;
            }
            state
                .ledger
                .release_quota_with_keyed(self.id, resources, keyed)?;
            self.scope
                .runtime
                .shared
                .record(|| HistoryFact::CapacityReleased {
                    reservation: self.id,
                });
            if let Some(report) = report {
                report.fill_validated(&mut state);
            }

            if state.capacity_waiters.len() != 0 {
                wakes = self.scope.runtime.take_capacity_wakes(
                    &mut state,
                    &ReservationResources {
                        quota: resources.clone(),
                        keyed: keyed.cloned(),
                        owned_slots: Default::default(),
                    },
                );
            }
            notify = true;
            Ok(())
        })();
        self.scope
            .runtime
            .shared
            .record_capacity_refusal(&result, || {
                refusal_intent.expect("enabled history retains the original capacity intent")
            });
        drop(state);
        if notify {
            self.scope.runtime.shared.changed.notify();
        }
        self.scope.runtime.deliver_capacity_wakes(wakes);
        self.scope
            .runtime
            .shared
            .observation
            .capacity_runtime(&result);
        result
    }
    pub fn split(
        &mut self,
        new_id: ReservationId,
        resources: &ReservationResources,
    ) -> Result<Self, RuntimeError> {
        self.split_inner(Some(new_id), resources)
    }
    /// Splits the original held units using an identity chosen in the same
    /// ledger transaction. No Work or capacity is admitted; the returned
    /// bundle retains this exact scope and bound owner until explicit transfer.
    pub fn split_next(&mut self, resources: &ReservationResources) -> Result<Self, RuntimeError> {
        self.split_inner(None, resources)
    }
    fn split_inner(
        &mut self,
        new_id: Option<ReservationId>,
        resources: &ReservationResources,
    ) -> Result<Self, RuntimeError> {
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let refusal_intent =
            self.scope
                .runtime
                .shared
                .history
                .as_ref()
                .map(|_| CapacityHistoryRequest::Split {
                    scope: self.scope.id,
                    source: self.id,
                    generation: state.ledger.reservation(self.id).map(|r| r.generation),
                    requested: resources.clone(),
                });
        let result = (|| {
            let new_id = match new_id {
                Some(id) => id,
                None => state.ledger.next_reservation_id()?,
            };
            state.ledger.split(self.id, new_id, resources)?;
            self.active = state.ledger.reservation(self.id).is_some();
            self.scope
                .runtime
                .shared
                .record(|| HistoryFact::CapacitySplit {
                    from: self.id,
                    to: new_id,
                });
            Ok(Self {
                scope: self.scope.clone(),
                id: new_id,
                active: true,
            })
        })();
        self.scope
            .runtime
            .shared
            .record_capacity_refusal(&result, || {
                refusal_intent.expect("enabled history retains the original capacity intent")
            });
        drop(state);
        self.scope
            .runtime
            .shared
            .observation
            .capacity_runtime(&result);
        result
    }
    /// Finish this original carrier only after its actual Work has exited.
    /// Bound positions already belong to their actual scope-pinned objects.
    /// Only never-bound unused inventory is destroyed by this operation.
    pub fn finish(&mut self) -> Result<(), RuntimeError> {
        if !self.active {
            let result = Ok(());
            self.scope
                .runtime
                .shared
                .observation
                .capacity_runtime(&result);
            return result;
        }
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let refusal_intent =
            self.scope
                .runtime
                .shared
                .history
                .as_ref()
                .map(|_| CapacityHistoryRequest::Finish {
                    scope: self.scope.id,
                    source: self.id,
                    generation: state.ledger.reservation(self.id).map(|r| r.generation),
                });
        let mut notify = false;
        let result = (|| {
            if !self.active {
                return Ok(());
            }

            let (ledger, scope) = state.parts(self.scope.id)?;
            ledger.finish(scope, self.id)?;
            self.scope
                .runtime
                .shared
                .record(|| HistoryFact::ReservationRetired {
                    reservation: self.id,
                });
            state.abandoned_bundles.remove(&self.id);
            self.active = false;

            notify = true;
            Ok(())
        })();
        self.scope
            .runtime
            .shared
            .record_capacity_refusal(&result, || {
                refusal_intent.expect("enabled history retains the original capacity intent")
            });
        drop(state);
        if notify {
            self.scope.runtime.shared.changed.notify();
        }
        self.scope
            .runtime
            .shared
            .observation
            .capacity_runtime(&result);
        result
    }
    /// Move a position into its actual object. Work and completion have their
    /// dedicated binders and can never bypass those real lifecycle gates.
    pub fn take_position(
        &mut self,
        kind: OwnedSlotKind,
    ) -> Result<OwnedPositionObligation, RuntimeError> {
        if matches!(kind, OwnedSlotKind::WorkHeader | OwnedSlotKind::Completion) {
            return Err(OwnedStorageError::WrongKind.into());
        }
        if !self.active {
            return Err(
                novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation.into(),
            );
        }
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let owner = state
            .ledger
            .reservation(self.id)
            .ok_or(novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation)?;
        if owner.scope != self.scope.id || !owner.bound {
            return Err(OwnedStorageError::WrongOwner.into());
        }
        if owner.owned_slots().get(kind) == 0 {
            return Err(OwnedStorageError::MissingPosition(kind).into());
        }
        let work = owner.work;
        let slot = state.next_slot_id(self.scope.id)?;
        state.reserve_slot(self.scope.id, slot)?;
        let position = state
            .ledger
            .take_owned(self.id, kind)
            .expect("preflighted original position");
        self.scope
            .runtime
            .shared
            .record(|| HistoryFact::SlotReserved {
                scope: self.scope.id,
                slot,
            });
        drop(state);
        self.scope.runtime.shared.changed.notify();
        Ok(OwnedPositionObligation {
            scope: self.scope.clone(),
            work,
            slot,
            position: Some(position),
            bound: false,
        })
    }
    pub fn transfer(&mut self, target: &WorkObligation) -> Result<(), RuntimeError> {
        let target_is_local =
            crate::sync::Arc::ptr_eq(&self.scope.runtime.shared, &target.scope.runtime.shared);
        if !target_is_local && self.scope.runtime.shared.history.is_none() {
            let result = Err(RuntimeError::Capacity(
                novarocks_control_runtime_core::capacity::CapacityError::WrongOwner,
            ));
            self.scope
                .runtime
                .shared
                .observation
                .capacity_runtime(&result);
            return result;
        }
        let runtime = self.scope.runtime.clone();
        let mut state = runtime.shared.state.lock().expect("runtime ledger lock");
        let refusal_intent =
            runtime
                .shared
                .history
                .as_ref()
                .map(|_| CapacityHistoryRequest::TransferWhole {
                    scope: self.scope.id,
                    source: self.id,
                    generation: state.ledger.reservation(self.id).map(|r| r.generation),
                    target: target.reference(),
                    target_is_local,
                    requested: state.ledger.reservation(self.id).map(|r| r.resources()),
                });
        let result = (|| {
            if !target_is_local {
                return Err(RuntimeError::Capacity(
                    novarocks_control_runtime_core::capacity::CapacityError::WrongOwner,
                ));
            }

            let original = state.ledger.reservation(self.id).ok_or(
                novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation,
            )?;
            let from = ScopedWorkRef {
                scope: original.scope,
                work: original.work,
            };
            let (ledger, scope) = state.parts(target.scope.id)?;
            ledger.transfer(self.id, scope, target.id)?;
            runtime
                .shared
                .record(|| HistoryFact::ReservationTransferred {
                    reservation: self.id,
                    from,
                    to: target.reference(),
                });

            self.scope = target.scope.clone();
            Ok(())
        })();
        runtime.shared.record_capacity_refusal(&result, || {
            refusal_intent.expect("enabled history retains the original capacity intent")
        });
        drop(state);
        self.scope
            .runtime
            .shared
            .observation
            .capacity_runtime(&result);
        result
    }
}
impl Drop for ReservationObligation {
    fn drop(&mut self) {
        if self.active {
            self.scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock")
                .abandoned_bundles
                .insert(self.id);
            self.scope.runtime.shared.changed.notify();
        }
    }
}

impl crate::completion::sealed::Sealed for StepRegistration {}
impl crate::completion::ReportStorageOwner for StepRegistration {
    fn report_storage(&self) -> (&ScopeHandle, ReservationId) {
        (&self.scope, self.reservation)
    }
}
impl crate::completion::sealed::Sealed for ReservationObligation {}
impl crate::completion::ReportStorageOwner for ReservationObligation {
    fn report_storage(&self) -> (&ScopeHandle, ReservationId) {
        (&self.scope, self.id)
    }
}

/// Unique storage held by the actual input, effect, cut, reply or capture.
/// A bound Drop never signals exit: its real scope slot remains unresolved.
#[must_use = "A bound position must reach its actual owner and exit explicitly"]
pub struct OwnedPositionObligation {
    scope: ScopeHandle,
    work: WorkId,
    slot: SlotId,
    position: Option<OwnedPosition>,
    bound: bool,
}
impl std::fmt::Debug for OwnedPositionObligation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedPositionObligation")
            .field("scope", &self.scope.id)
            .field("work", &self.work)
            .field("slot", &self.slot)
            .field("position", &self.position)
            .field("bound", &self.bound)
            .finish()
    }
}
impl OwnedPositionObligation {
    pub fn kind(&self) -> OwnedSlotKind {
        self.position.as_ref().expect("live owned position").kind()
    }
    pub fn belongs_to(&self, scope: &ScopeHandle) -> bool {
        self.scope.same_scope(scope)
    }
    /// Read-only claim provenance. Equal numbers grant no new position or Work.
    pub fn scope_id(&self) -> novarocks_control_runtime_core::identity::ScopeId {
        self.scope.id
    }
    pub fn owner_work(&self) -> WorkId {
        self.work
    }
    /// The original ledger, scope and Work must all agree. An unused parcel's
    /// later transfer never reparents this already claimed position.
    pub fn belongs_to_work(&self, work: &WorkObligation) -> bool {
        self.scope.same_scope(work.scope()) && self.work == work.id()
    }
    /// Bind before exposing a payload/capture. An already bound position is
    /// never converted back into unused inventory.
    pub fn bind(&mut self) -> Result<(), RuntimeError> {
        if self.bound || self.position.is_none() {
            return Err(OwnedStorageError::BoundPosition.into());
        }
        self.bound = true;
        Ok(())
    }
    fn retire(&mut self) -> Result<(), RuntimeError> {
        if self.position.is_none() {
            return Err(OwnedStorageError::WrongOwner.into());
        }
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state
            .scopes
            .get_mut(&self.scope.id)
            .ok_or(RuntimeError::UnknownScope)?
            .retire_slot(self.slot)?;
        self.scope
            .runtime
            .shared
            .record(|| HistoryFact::SlotRetired { slot: self.slot });
        self.position.take();
        drop(state);
        self.scope.runtime.shared.changed.notify();
        Ok(())
    }
    /// The actual object owner observed its captures and necessary consumers
    /// exit. No raw identity or storage count can call this on another owner.
    pub fn retire_after_exit(mut self) -> Result<(), (RuntimeError, Self)> {
        if !self.bound {
            return Err((OwnedStorageError::BoundPosition.into(), self));
        }
        match self.retire() {
            Ok(()) => Ok(()),
            Err(error) => Err((error, self)),
        }
    }
    pub fn revoke_unused(mut self) -> Result<(), (RuntimeError, Self)> {
        if self.bound {
            return Err((OwnedStorageError::BoundPosition.into(), self));
        }
        match self.retire() {
            Ok(()) => Ok(()),
            Err(error) => Err((error, self)),
        }
    }
}
impl Drop for OwnedPositionObligation {
    fn drop(&mut self) {
        // Only a genuinely unbound object can roll back. A lost bound lease
        // leaves its original scope position present, so join cannot lie.
        if self.position.is_some() && !self.bound {
            self.retire()
                .expect("unused owned position retains its real scope");
        }
    }
}
