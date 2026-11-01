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

//! A non-cloneable carrier for one actually bound Work/capacity/report triple.

use crate::{
    completion::ReportSlot,
    lease::{ReservationBundle, WorkLease},
};

/// Rejection of mismatched original ownership, not an admission or exit fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReportedWorkBindingError {
    DifferentLedger,
    WrongScope,
    UnknownScope,
    UnknownWork,
    UnboundWork,
    WorkHeaderMismatch,
    InactiveCapacity,
    UnknownCapacity,
    UnboundCapacity,
    CapacityWorkMismatch,
    UnknownReport,
    ReportProducerMismatch,
    CaptureStorage(crate::transaction::RuntimeError),
}
impl std::fmt::Display for ReportedWorkBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reported Work binding failure: {self:?}")
    }
}
impl std::error::Error for ReportedWorkBindingError {}
/// Every refusal returns all three original owners without changing them.
#[must_use = "A refused binding still owns its Work, capacity and report"]
pub struct ReportedWorkRefusal {
    pub error: ReportedWorkBindingError,
    pub work: WorkLease,
    pub capacity: ReservationBundle,
    pub report: ReportSlot,
}
impl std::fmt::Debug for ReportedWorkRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReportedWorkRefusal")
            .field("error", &self.error)
            .field("scope", &self.work.scope().id())
            .field("work", &self.work.id())
            .field("reservation", &self.capacity.id())
            .field("report", &self.report.id())
            .finish()
    }
}
/// Only a validated carrier can enter either exit or continuation custody.
/// Fields are private; extracting them requires another validation before a
/// custodian can accept them again. This object signs no new responsibility.
#[must_use = "The original reported Work must reach its owner or be returned intact"]
pub struct ReportedWork {
    work: WorkLease,
    capacity: ReservationBundle,
    report: ReportSlot,
}
impl std::fmt::Debug for ReportedWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReportedWork")
            .field("scope", &self.work.scope().id())
            .field("work", &self.work.id())
            .field("reservation", &self.capacity.id())
            .field("report", &self.report.id())
            .finish()
    }
}
impl ReportedWork {
    /// One original ledger transaction checks identities and actual bindings.
    /// Current scope generation/health or Work execution phase do not cancel
    /// previously admitted ownership. No self pin, authority or credit is
    /// created on refusal, and no foreign callback or destructor runs here.
    pub fn try_new(
        mut work: WorkLease,
        mut capacity: ReservationBundle,
        report: ReportSlot,
    ) -> Result<Self, ReportedWorkRefusal> {
        if let Err(error) = capacity.validate_reported_work_binding(&work, &report) {
            return Err(ReportedWorkRefusal {
                error,
                work,
                capacity,
                report,
            });
        }
        let captures = match capacity.resources() {
            Ok(resources) => resources.owned_slots.captures,
            Err(error) => {
                return Err(ReportedWorkRefusal {
                    error: ReportedWorkBindingError::CaptureStorage(error),
                    work,
                    capacity,
                    report,
                });
            }
        };
        if let Err(error) = work.bind_owned_captures(&mut capacity, captures) {
            return Err(ReportedWorkRefusal {
                error: ReportedWorkBindingError::CaptureStorage(error),
                work,
                capacity,
                report,
            });
        }
        Ok(Self {
            work,
            capacity,
            report,
        })
    }
    /// Only the same-ledger atomic role constructor has already performed all
    /// real Work/header/report/capture bindings before exposing these owners.
    pub(crate) fn from_committed(
        work: WorkLease,
        capacity: ReservationBundle,
        report: ReportSlot,
    ) -> Self {
        Self {
            work,
            capacity,
            report,
        }
    }
    /// Read only the original bound storage, under one actual ledger lock.
    /// Unused inventory and quota are not evidence of a role's bound header,
    /// report or captures. This description grants no new position or Work.
    pub fn bound_storage_profile(
        &self,
    ) -> Result<
        novarocks_control_runtime_core::owned_storage::OwnedSlotNeed,
        ReportedWorkBindingError,
    > {
        use crate::sync::Arc;
        use novarocks_control_runtime_core::owned_storage::{OwnedSlotKind, OwnedSlotNeed};
        let original = self.work.scope();
        if !Arc::ptr_eq(&original.runtime.shared, &self.report.runtime.shared)
            || !Arc::ptr_eq(
                &original.runtime.shared,
                &self.capacity.scope.runtime.shared,
            )
        {
            return Err(ReportedWorkBindingError::DifferentLedger);
        }
        if !self.capacity.scope.same_scope(original) {
            return Err(ReportedWorkBindingError::WrongScope);
        }
        let state = original
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope = state
            .scopes
            .get(&original.id())
            .ok_or(ReportedWorkBindingError::UnknownScope)?;
        let work = scope
            .work(self.work.id())
            .ok_or(ReportedWorkBindingError::UnknownWork)?;
        if work.unbound_guard {
            return Err(ReportedWorkBindingError::UnboundWork);
        }
        let header = work.header_identity();
        if header.kind() != OwnedSlotKind::WorkHeader
            || header.allocation().is_none()
                && (header.scope() != Some(original.id())
                    || header.ordinal_or_work() != self.work.id().get())
        {
            return Err(ReportedWorkBindingError::WorkHeaderMismatch);
        }
        let report = state
            .slots
            .get(&self.report.id())
            .ok_or(ReportedWorkBindingError::UnknownReport)?;
        // ReportRecord's only constructor consumes a real Completion position.
        // Its current producer and Scope qualify that same original binding.
        if report.storage_reservation().is_none() {
            return Err(ReportedWorkBindingError::UnknownReport);
        }
        if report.scope() != original.id() {
            return Err(ReportedWorkBindingError::WrongScope);
        }
        if report.producer() != self.work.id() {
            return Err(ReportedWorkBindingError::ReportProducerMismatch);
        }
        let captures = scope
            .capture_positions(self.work.id())
            .map_err(|_| ReportedWorkBindingError::UnknownWork)?;
        Ok(OwnedSlotNeed {
            work_headers: 1,
            completions: 1,
            captures,
            ..OwnedSlotNeed::default()
        })
    }
    /// Sign the same original Work's sole neutral readiness source before
    /// transferring this carrier to its exit owner.
    pub fn header_readiness_registration(
        &mut self,
    ) -> Result<crate::header_readiness::HeaderReadyRegistration, crate::transaction::RuntimeError>
    {
        self.work.header_readiness_registration()
    }
    pub fn work(&self) -> &WorkLease {
        &self.work
    }
    pub fn report(&self) -> &ReportSlot {
        &self.report
    }
    pub fn into_parts(self) -> (WorkLease, ReservationBundle, ReportSlot) {
        (self.work, self.capacity, self.report)
    }
}
