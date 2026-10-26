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
    capacity::{ReservationRequest, ResourceVector},
    history::HistoryFact,
    identity::{ReservationId, WorkId},
    scope::{ScopeError, WorkAdmission, WorkKind},
};

#[must_use = "A reservation must be bound to its actual holder or rolled back before it acquires a payload"]
pub struct StepGuard {
    scope: ScopeHandle,
    work: WorkId,
    reservation: ReservationId,
    active: bool,
}
impl StepGuard {
    pub(crate) fn new(scope: ScopeHandle, work: WorkId, reservation: ReservationId) -> Self {
        Self {
            scope,
            work,
            reservation,
            active: true,
        }
    }
    pub fn bind(mut self, kind: WorkKind) -> Result<(WorkLease, CapacityBundle), RuntimeError> {
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
            WorkLease::new(self.scope.clone(), self.work),
            CapacityBundle {
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
    pub(crate) fn belongs_to(&self, scope: &ScopeHandle) -> bool {
        self.scope.id() == scope.id()
            && crate::sync::Arc::ptr_eq(&self.scope.runtime.shared, &scope.runtime.shared)
    }
}
impl Drop for StepGuard {
    fn drop(&mut self) {
        if self.active {
            let mut state = self
                .scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            let (ledger, scope) = state
                .parts(self.scope.id)
                .expect("live scope retains its reservation");
            ledger
                .rollback_unbound(scope, self.reservation)
                .expect("only a pure unbound guard rolls back");
            self.scope
                .runtime
                .shared
                .record(HistoryFact::UnboundReservationRolledBack {
                    scope: self.scope.id,
                    work: self.work,
                    reservation: self.reservation,
                });
            drop(state);
            self.scope.runtime.shared.changed.notify();
        }
    }
}

#[must_use = "Work remains counted until body, captures and children actually exit"]
pub struct WorkLease {
    scope: ScopeHandle,
    id: WorkId,
    body_exited: bool,
    captures_exited: bool,
}
impl WorkLease {
    pub(crate) fn new(scope: ScopeHandle, id: WorkId) -> Self {
        Self {
            scope,
            id,
            body_exited: false,
            captures_exited: false,
        }
    }
    pub const fn id(&self) -> WorkId {
        self.id
    }
    pub fn scope(&self) -> &ScopeHandle {
        &self.scope
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
            .start(self.id)?;
        self.scope.runtime.shared.record(HistoryFact::Started {
            scope: self.scope.id,
            work: self.id,
        });
        Ok(())
    }
    /// Records unknown exit while the actual custodian keeps this lease.
    /// Diagnostic responsibility is not a substitute for physical exit.
    pub(crate) fn mark_unresolved(&self) {
        self.scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .note_unresolved(self.scope.id, self.id);
        self.scope.runtime.shared.changed.notify();
    }
    pub fn derive(&self, mut request: ReservationRequest) -> Result<StepGuard, RuntimeError> {
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
        let ready = scope.work_exited(self.id)?;
        self.scope.runtime.shared.record(HistoryFact::BodyExited {
            scope: self.scope.id,
            work: self.id,
        });
        if let Some(report) = report {
            report.fill_validated(&mut state);
        }
        if ready {
            state.pending_retirement.insert((self.scope.id, self.id));
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
        let ready = scope.work_exited(self.id)?;
        self.scope
            .runtime
            .shared
            .record(HistoryFact::CapturesExited {
                scope: self.scope.id,
                work: self.id,
            });
        if let Some(report) = report {
            report.fill_validated(&mut state);
        }
        if ready {
            state.pending_retirement.insert((self.scope.id, self.id));
        }
        drop(state);
        self.scope.runtime.shared.changed.notify();
        Ok(())
    }
}
impl Drop for WorkLease {
    fn drop(&mut self) {
        if !self.body_exited || !self.captures_exited {
            self.scope
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock")
                .note_unresolved(self.scope.id, self.id);
            self.scope.runtime.shared.changed.notify();
        }
    }
}

#[must_use = "Bound capacity follows its holder until an explicit release fact"]
pub struct CapacityBundle {
    scope: ScopeHandle,
    id: ReservationId,
    active: bool,
}
impl CapacityBundle {
    pub const fn id(&self) -> ReservationId {
        self.id
    }
    pub fn resources(&self) -> Result<ResourceVector, RuntimeError> {
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
            .resources
            .clone())
    }
    pub fn release(&mut self, resources: &ResourceVector) -> Result<(), RuntimeError> {
        self.release_report(resources, None)
    }
    /// Capacity release and its reliable fact fill have one linearization point.
    /// Producer/capture retirement and each consumer acknowledgement remain separate.
    pub fn release_with_report(
        &mut self,
        resources: &ResourceVector,
        report: &crate::completion::ReportSlot,
    ) -> Result<(), RuntimeError> {
        self.release_report(resources, Some(report))
    }
    fn release_report(
        &mut self,
        resources: &ResourceVector,
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
            let owner = state.ledger.reservation(self.id).ok_or(
                novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation,
            )?;
            report.validate_fill_for(&self.scope.runtime, &state, owner.scope, owner.work)?;
        }
        state.ledger.release(self.id, resources)?;
        self.scope
            .runtime
            .shared
            .record(HistoryFact::CapacityReleased {
                reservation: self.id,
            });
        if let Some(report) = report {
            report.fill_validated(&mut state);
        }
        self.active = state.ledger.reservation(self.id).is_some();
        if !self.active {
            state.abandoned_bundles.remove(&self.id);
        }
        drop(state);
        self.scope.runtime.shared.changed.notify();
        Ok(())
    }
    pub fn split(
        &mut self,
        new_id: ReservationId,
        resources: &ResourceVector,
    ) -> Result<Self, RuntimeError> {
        let mut state = self
            .scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state.ledger.split(self.id, new_id, resources)?;
        self.scope
            .runtime
            .shared
            .record(HistoryFact::CapacitySplit {
                from: self.id,
                to: new_id,
            });
        Ok(Self {
            scope: self.scope.clone(),
            id: new_id,
            active: true,
        })
    }
    pub fn transfer(&mut self, target: &WorkLease) -> Result<(), RuntimeError> {
        if !crate::sync::Arc::ptr_eq(&self.scope.runtime.shared, &target.scope.runtime.shared) {
            return Err(RuntimeError::Capacity(
                novarocks_control_runtime_core::capacity::CapacityError::WrongOwner,
            ));
        }
        let runtime = self.scope.runtime.clone();
        let mut state = runtime.shared.state.lock().expect("runtime ledger lock");
        let (ledger, scope) = state.parts(target.scope.id)?;
        ledger.transfer(self.id, scope, target.id)?;
        runtime.shared.record(HistoryFact::Transferred {
            work: target.id,
            from: self.scope.id,
            to: target.scope.id,
        });
        drop(state);
        self.scope = target.scope.clone();
        Ok(())
    }
}
impl Drop for CapacityBundle {
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
