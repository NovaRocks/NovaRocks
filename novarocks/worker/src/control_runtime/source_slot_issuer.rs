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
    reported_work_admission::{
        adopt_reported_work, reserve_reported_work, reserve_reported_work_from_authority,
    },
    source_positions::{SOURCE_SLOT_STORAGE, SourceSlotAdmission, SourceSlotStorageError},
};
use novarocks_control_runtime::{
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    obligation::{
        ReservationObligation, WorkChildAuthority, WorkObligation, WorkRetirementObserver,
    },
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    identity::{Generation, ScopeId},
    scope::WorkKind,
};

#[derive(Debug)]
pub(crate) enum SourceSlotIssueError {
    Runtime(RuntimeError),
    Storage(SourceSlotStorageError),
    StorageUnavailable,
}
impl std::fmt::Display for SourceSlotIssueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "Source slot issuance failed: {error}"),
            Self::Storage(error) => write!(f, "Source slot storage was refused: {error}"),
            Self::StorageUnavailable => f.write_str("Source slot payer storage is unavailable"),
        }
    }
}
impl std::error::Error for SourceSlotIssueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::StorageUnavailable => None,
        }
    }
}
/// Retained owners keep their non-source units. A pure Accepted source pool
/// may instead hand off its entire final original bundle without adding a
/// reservation header or a dummy retained resource.
pub(crate) enum SourceSlotPayer<'a> {
    Retained(&'a mut ReservationObligation),
    Pool(&'a mut Option<ReservationObligation>),
}
impl SourceSlotPayer<'_> {
    fn payer(&mut self) -> Result<&mut ReservationObligation, SourceSlotIssueError> {
        match self {
            Self::Retained(payer) => Ok(payer),
            Self::Pool(pool) => pool
                .as_mut()
                .ok_or(SourceSlotIssueError::StorageUnavailable),
        }
    }
}

impl From<RuntimeError> for SourceSlotIssueError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
fn exit_error(error: ExitCustodyError) -> SourceSlotIssueError {
    match error {
        ExitCustodyError::Runtime(error) => SourceSlotIssueError::Runtime(error),
        // An unexpected phase cannot turn a possibly exposed original into
        // an unused exit. Its role and payment stay in the same partial.
        ExitCustodyError::InvalidPhase(_) => {
            SourceSlotIssueError::Runtime(RuntimeError::UnresolvedWork)
        }
    }
}

struct UnusedSourceSlot {
    exit_custody: ExitCustody,
    authority: Option<ExitObligation>,
    retirement: WorkRetirementObserver,
}
impl std::fmt::Debug for UnusedSourceSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnusedSourceSlot")
            .field("role", &self.exit_custody)
            .field("authority", &self.authority)
            .field("retirement", &self.retirement)
            .finish()
    }
}
impl UnusedSourceSlot {
    fn new(reported: ReportedWork) -> Self {
        let retirement = reported.work().retirement_observer();
        let (exit_custody, authority) = ExitCustody::from_admitted(reported);
        Self {
            exit_custody,
            authority: Some(authority),
            retirement,
        }
    }
}

/// Facts from this one bounded retirement attempt, not another owner or
/// permission. Progress comes only from an actual exit, phase change or reap.
#[must_use]
pub(crate) struct UnusedRetirementProgress {
    pub retired: bool,
    pub progressed: bool,
}

/// Only actual unexposed admissions belong here. Original unused positions
/// remain with the payer on refusal; this partial owns admitted roles only.
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
                .map(|slot| UnusedSourceSlot::new(slot.into_reported()))
                .collect(),
        }
    }
    #[cfg(test)]
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
        self.try_retire_progress(ledger)
            .map(|progress| progress.retired)
    }
    pub(crate) fn try_retire_progress(
        &mut self,
        ledger: &RuntimeLedger,
    ) -> Result<UnusedRetirementProgress, SourceSlotIssueError> {
        let mut progressed = false;
        for slot in &mut self.slots {
            let before = slot.exit_custody.phase();
            if let Some(authority) = slot.authority.take() {
                // No capture, bank entry or sender ever left this partial.
                // Revocation is not consumption by either report observer.
                authority.revoke_unused_and_exit().map_err(exit_error)?;
                progressed = true;
            }
            match slot.exit_custody.phase() {
                ExitCustodyPhase::Retired => {}
                ExitCustodyPhase::Unresolved => {
                    return Err(SourceSlotIssueError::Runtime(RuntimeError::UnresolvedWork));
                }
                _ => {
                    slot.exit_custody.try_retire().map_err(exit_error)?;
                }
            }
            progressed |= slot.exit_custody.phase() != before;
        }
        if self.slots.iter().any(|slot| !slot.retirement.is_retired()) {
            progressed |= ledger.reap_scope_ready(self.scope.id(), 1)? > 0;
        }
        let complete = self.slots.iter().all(|slot| {
            slot.exit_custody.phase() == ExitCustodyPhase::Retired && slot.retirement.is_retired()
        });
        if complete {
            self.slots.clear();
        }
        Ok(UnusedRetirementProgress {
            retired: complete,
            progressed,
        })
    }
}

#[derive(Debug)]
#[must_use = "A failed issuer retains every admitted original"]
pub(crate) struct SourceSlotIssueRefusal {
    pub error: SourceSlotIssueError,
    pub partial: SourceSlotIssuePartial,
}

/// Consume original unused positions and bind each Source's real role/report
/// in one ledger transaction. The caller transfers any cross-scope parcel to
/// `actual_parent` before calling; no existing Work is reparented here.
/// The Core consumer's explicit Scope/initial epoch may differ from this
/// financial Scope. The tuple describes a receipt target, not Work authority.
pub(crate) fn reserve_source_slots<const N: usize>(
    scope: &ScopeHandle,
    actual_parent: &WorkObligation,
    core_owner: (ScopeId, Generation),
    payer: SourceSlotPayer<'_>,
) -> Result<[SourceSlotAdmission; N], SourceSlotIssueRefusal> {
    reserve_source_slots_with_parent::<N>(
        scope,
        SourceParent::Work(actual_parent),
        core_owner,
        payer,
    )
}

/// External R's original Source parcel pays these roles, but their birth
/// derives from the original process baseline, never the R or Context Work.
pub(crate) fn reserve_source_slots_from_authority<const N: usize>(
    scope: &ScopeHandle,
    actual_parent: &WorkChildAuthority,
    core_owner: (ScopeId, Generation),
    payer: SourceSlotPayer<'_>,
) -> Result<[SourceSlotAdmission; N], SourceSlotIssueRefusal> {
    reserve_source_slots_with_parent::<N>(
        scope,
        SourceParent::Authority(actual_parent),
        core_owner,
        payer,
    )
}
enum SourceParent<'a> {
    Work(&'a WorkObligation),
    Authority(&'a WorkChildAuthority),
}
fn reserve_source_slots_with_parent<const N: usize>(
    scope: &ScopeHandle,
    actual_parent: SourceParent<'_>,
    core_owner: (ScopeId, Generation),
    mut payer: SourceSlotPayer<'_>,
) -> Result<[SourceSlotAdmission; N], SourceSlotIssueRefusal> {
    let mut slots = Vec::with_capacity(N);
    let resources: ReservationResources = SOURCE_SLOT_STORAGE
        .resources()
        .expect("fixed source position description fits");
    for _ in 0..N {
        let current = match payer.payer() {
            Ok(current) => current,
            Err(error) => {
                return Err(SourceSlotIssueRefusal {
                    error,
                    partial: SourceSlotIssuePartial::from_admissions(scope, slots),
                });
            }
        };
        let issued = match &actual_parent {
            SourceParent::Work(parent) => reserve_reported_work(
                scope,
                current,
                parent,
                resources.clone(),
                WorkKind::Cleanup,
                Some(core_owner),
            ),
            SourceParent::Authority(parent) => reserve_reported_work_from_authority(
                scope,
                current,
                parent,
                resources.clone(),
                WorkKind::Cleanup,
                Some(core_owner),
            ),
        };
        let admitted = match issued {
            Ok(admitted) => admitted,
            Err(error) => {
                return Err(SourceSlotIssueRefusal {
                    error: SourceSlotIssueError::Runtime(error),
                    partial: SourceSlotIssuePartial::from_admissions(scope, slots),
                });
            }
        };
        let reported = adopt_reported_work(admitted.work, admitted.capacity, admitted.report);
        match SourceSlotAdmission::from_admitted(reported) {
            Ok(slot) => slots.push(slot),
            Err(refusal) => {
                let mut partial = SourceSlotIssuePartial::from_admissions(scope, slots);
                partial.slots.push(UnusedSourceSlot::new(refusal.reported));
                return Err(SourceSlotIssueRefusal {
                    error: SourceSlotIssueError::Storage(refusal.error),
                    partial,
                });
            }
        }
    }
    Ok(slots
        .try_into()
        .unwrap_or_else(|_| unreachable!("exactly N original Source roles")))
}

#[cfg(test)]
mod tests;
