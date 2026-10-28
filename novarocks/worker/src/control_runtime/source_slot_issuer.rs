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

//! Issue original SourceSlot holders from already paid storage. A refusal
//! retains only never-exposed empty originals; it is not source disposal for
//! a bound sender, a bank entry or an opaque producer.

use super::{
    admitted_role::{adopt_reported_work, reserve_reported_role},
    source_positions::{SOURCE_SLOT_STORAGE, SourceSlotAdmission, SourceSlotStorageError},
};
use novarocks_control_runtime::{
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::{CapacityBundle, WorkRetirementObserver},
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{Generation, ReservationId},
    scope::WorkKind,
};

#[derive(Debug)]
pub(crate) enum SourceSlotIssueError {
    Runtime(RuntimeError),
    Storage(SourceSlotStorageError),
    StorageUnavailable,
}
/// Retained owners keep their non-source units. A pure Accepted source pool
/// may instead hand off its entire final original bundle without adding a
/// reservation header or a dummy retained resource.
pub(crate) enum SourceSlotPayer<'a> {
    Retained(&'a mut CapacityBundle),
    Pool(&'a mut Option<CapacityBundle>),
    /// Explicit finite empty-vector protocol fixture. This variant is absent
    /// from production and never selected by inspecting a missing payment.
    #[cfg(test)]
    ProtocolModel,
}
impl SourceSlotPayer<'_> {
    fn unavailable(&self) -> bool {
        matches!(self, Self::Pool(pool) if pool.is_none())
    }
    fn take_slot(
        &mut self,
        resources: &ResourceVector,
    ) -> Result<CapacityBundle, SourceSlotIssueError> {
        match self {
            Self::Retained(payer) => payer.split_next(resources).map_err(Into::into),
            #[cfg(test)]
            Self::ProtocolModel => Err(SourceSlotIssueError::StorageUnavailable),
            Self::Pool(pool) => {
                let current = pool
                    .as_mut()
                    .ok_or(SourceSlotIssueError::StorageUnavailable)?;
                let remaining = current.resources()?;
                if &remaining == resources {
                    Ok(pool
                        .take()
                        .expect("the same last original source pool bundle"))
                } else {
                    current.split_next(resources).map_err(Into::into)
                }
            }
        }
    }
}

impl From<RuntimeError> for SourceSlotIssueError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
fn exit_error(error: ExitRoleError) -> SourceSlotIssueError {
    match error {
        ExitRoleError::Runtime(error) => SourceSlotIssueError::Runtime(error),
        // An unexpected phase cannot turn a possibly exposed original into
        // an unused exit. Its role and payment stay in the same partial.
        ExitRoleError::InvalidPhase(_) => {
            SourceSlotIssueError::Runtime(RuntimeError::UnresolvedWork)
        }
    }
}

struct UnusedSourceSlot {
    role: ExitRole,
    authority: Option<ExitAuthority>,
    retirement: WorkRetirementObserver,
    storage: Option<CapacityBundle>,
}
impl std::fmt::Debug for UnusedSourceSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnusedSourceSlot")
            .field("role", &self.role)
            .field("authority", &self.authority)
            .field("retirement", &self.retirement)
            .field("storage", &self.storage.as_ref().map(CapacityBundle::id))
            .finish()
    }
}
impl UnusedSourceSlot {
    fn new(reported: ReportedWork, storage: Option<CapacityBundle>) -> Self {
        let retirement = reported.work().retirement_observer();
        let (role, authority) = ExitRole::from_admitted(reported);
        Self {
            role,
            authority: Some(authority),
            retirement,
            storage,
        }
    }
}

/// Only actual unexposed admissions belong here. None payment means either
/// issuance never took a fee (still held by the payer), or an explicit test
/// model. It grants no production admission or replacement credit.
#[must_use = "Retain and retire the same unused Work, reports and split payments"]
pub(crate) struct SourceSlotIssuePartial {
    scope: ScopeHandle,
    slots: Vec<UnusedSourceSlot>,
}
impl std::fmt::Debug for SourceSlotIssuePartial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSlotIssuePartial")
            .field("scope", &self.scope.id())
            .field("slots", &self.slots)
            .finish()
    }
}
impl SourceSlotIssuePartial {
    /// The caller returns whole admissions refused before bank ownership.
    /// They must all belong to this same actual scope and its original ledger.
    /// No sender, payload or producer may have been attached to these holders.
    pub(crate) fn from_admissions(
        scope: &ScopeHandle,
        slots: impl IntoIterator<Item = SourceSlotAdmission>,
    ) -> Self {
        Self {
            scope: scope.clone(),
            slots: slots
                .into_iter()
                .map(|slot| {
                    let (reported, storage) = slot.into_originals();
                    UnusedSourceSlot::new(reported, storage)
                })
                .collect(),
        }
    }
    pub(crate) fn scope(&self) -> &ScopeHandle {
        &self.scope
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
    /// The caller supplies this scope's original ledger. One invocation reaps
    /// at most one real Work header, including when another ready sibling wins
    /// that turn. False requires the owner's existing Task/Context ready notice.
    pub(crate) fn try_retire(
        &mut self,
        ledger: &RuntimeLedger,
    ) -> Result<bool, SourceSlotIssueError> {
        for slot in &mut self.slots {
            if let Some(authority) = slot.authority.take() {
                // No capture, bank entry or sender ever left this partial.
                // Revocation is not consumption by either report observer.
                authority.revoke_unused_and_exit().map_err(exit_error)?;
            }
            match slot.role.phase() {
                ExitRolePhase::Retired => {}
                ExitRolePhase::Unresolved => {
                    return Err(SourceSlotIssueError::Runtime(RuntimeError::UnresolvedWork));
                }
                _ => {
                    slot.role.try_retire().map_err(exit_error)?;
                }
            }
        }
        if self.slots.iter().any(|slot| !slot.retirement.is_retired()) {
            ledger.reap_scope_ready(self.scope.id(), 1)?;
        }
        let mut complete = true;
        for slot in &mut self.slots {
            if slot.role.phase() != ExitRolePhase::Retired || !slot.retirement.is_retired() {
                complete = false;
                continue;
            }
            if let Some(storage) = &mut slot.storage {
                if !storage.is_released() {
                    let resources = storage.resources()?;
                    storage.release(&resources)?;
                }
                slot.storage = None;
            }
        }
        if complete {
            self.slots.clear();
        }
        Ok(complete)
    }
}

#[derive(Debug)]
#[must_use = "A failed issuer retains every admitted original and split payment"]
pub(crate) struct SourceSlotIssueRefusal {
    pub error: SourceSlotIssueError,
    pub partial: SourceSlotIssuePartial,
}

/// Reserve zero-resource source roles, then move their storage from the
/// caller's existing payment. N is the finite caller-owned source inventory;
/// this function introduces no independent capacity limit or second charge.
pub(crate) fn reserve_source_slots<const N: usize>(
    scope: &ScopeHandle,
    parent: ReservationId,
    core_generation: Generation,
    mut payer: SourceSlotPayer<'_>,
) -> Result<[SourceSlotAdmission; N], SourceSlotIssueRefusal> {
    let mut slots = Vec::with_capacity(N);
    let resources = SOURCE_SLOT_STORAGE
        .resources()
        .expect("fixed source slot storage fits its dimensions");
    for _ in 0..N {
        if payer.unavailable() {
            return Err(SourceSlotIssueRefusal {
                error: SourceSlotIssueError::StorageUnavailable,
                partial: SourceSlotIssuePartial::from_admissions(scope, slots),
            });
        }
        let admitted =
            match reserve_reported_role(scope, parent, WorkKind::Cleanup, Some(core_generation)) {
                Ok(admitted) => admitted,
                Err(error) => {
                    return Err(SourceSlotIssueRefusal {
                        error: SourceSlotIssueError::Runtime(error),
                        partial: SourceSlotIssuePartial::from_admissions(scope, slots),
                    });
                }
            };
        let reported = adopt_reported_work(admitted.work, admitted.capacity, admitted.report);
        #[cfg(test)]
        if matches!(&payer, SourceSlotPayer::ProtocolModel) {
            // The real scope/header/report limits still apply. No production
            // capacity is issued, split or refunded by this explicit model.
            slots.push(SourceSlotAdmission::protocol_model(reported));
            continue;
        }
        let storage = match payer.take_slot(&resources) {
            Ok(storage) => storage,
            Err(error) => {
                let mut partial = SourceSlotIssuePartial::from_admissions(scope, slots);
                partial.slots.push(UnusedSourceSlot::new(reported, None));
                return Err(SourceSlotIssueRefusal { error, partial });
            }
        };
        match SourceSlotAdmission::from_paid(reported, storage) {
            Ok(slot) => slots.push(slot),
            Err(refusal) => {
                let mut partial = SourceSlotIssuePartial::from_admissions(scope, slots);
                partial.slots.push(UnusedSourceSlot::new(
                    refusal.reported,
                    Some(refusal.storage),
                ));
                return Err(SourceSlotIssueRefusal {
                    error: SourceSlotIssueError::Storage(refusal.error),
                    partial,
                });
            }
        }
    }
    Ok(slots
        .try_into()
        .unwrap_or_else(|_| unreachable!("exactly N original SourceSlot admissions")))
}

#[cfg(test)]
mod tests;
