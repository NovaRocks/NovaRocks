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
//! Process and one keyed-family quota commit atomically; process exhaustion is
//! reported first. The `_or_wait` methods register that exact denial under the
//! same ledger lock. Each registrant merges the denied process/family dimension
//! domains until their first real release. Another key in the same domain may
//! cause one spurious wake; retry must repeat the original ledger transaction.
//! Registrant Drop cancels the logical wait; a wake grants no quota.

use crate::{
    notify::Notify,
    obligation::StepRegistration,
    sync::{Arc, Mutex},
};
use novarocks_control_runtime_core::{
    capacity::{
        CapacityError, CapacityLedger, CapacityWaitTarget, KeyedUsageSnapshot, ReservationRequest,
        ReservationResources, ResourceVector,
    },
    completion::CompletionError,
    history::{CapacityHistoryRefusal, CapacityHistoryRequest, HistoryFact},
    identity::{
        CapacityKey, DimensionId, FamilyId, Generation, ReservationId, ScopeId, ScopedWorkRef,
        SlotId, WorkId,
    },
    owned_storage::{OwnedSlotKind, OwnedStorageError},
    scope::{ScopeError, ScopeState, WorkAdmission, WorkKind},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityDenial {
    pub dimension: DimensionId,
    pub keyed: Option<(FamilyId, CapacityKey)>,
    pub requested: u64,
    pub committed: u64,
    pub limit: u64,
}

impl CapacityDenial {
    pub fn wait_target(self) -> CapacityWaitTarget {
        match self.keyed {
            Some((family, key)) => CapacityWaitTarget::Keyed {
                family,
                key,
                dimension: self.dimension,
            },
            None => CapacityWaitTarget::Process(self.dimension),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    Capacity(CapacityError),
    CapacityDenied(CapacityDenial),
    Scope(ScopeError),
    Completion(CompletionError),
    OwnedStorage(OwnedStorageError),
    UnknownScope,
    DuplicateScope,
    UnknownSlot,
    DuplicateSlot,
    ScopeNotRetired,
    UnresolvedWork,
    IdentityExhausted,
}
impl From<OwnedStorageError> for RuntimeError {
    fn from(value: OwnedStorageError) -> Self {
        Self::OwnedStorage(value)
    }
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
    pub(crate) capacity_waiters: crate::capacity_notification::CapacityWaiters,
    pub(crate) scopes: BTreeMap<ScopeId, ScopeState>,
    pub(crate) slots: BTreeMap<SlotId, crate::completion::ReportRecord>,
    pub(crate) unresolved: BTreeSet<(ScopeId, WorkId)>,
    pub(crate) work_labels: BTreeMap<(ScopeId, WorkId), &'static str>,
    unresolved_by_scope: BTreeMap<ScopeId, usize>,
    pub(crate) abandoned_bundles: BTreeSet<ReservationId>,
    pub(crate) pending_retirement: BTreeSet<(ScopeId, WorkId)>,
    pub(crate) header_readiness:
        BTreeMap<(ScopeId, WorkId), crate::header_readiness::HeaderBinding>,
    parent_links: BTreeMap<ScopeId, (ScopeId, WorkId)>,
    last_scope: u64,
    last_slot: u64,
    external_open: bool,
}
impl LedgerState {
    fn history_parent(&self, scope: ScopeId, work: WorkId) -> Option<ScopedWorkRef> {
        self.scopes
            .get(&scope)
            .expect("the actual admitted Scope")
            .work(work)
            .expect("the actual admitted Work")
            .parent
            .map(|work| ScopedWorkRef { scope, work })
    }
    /// Validate the real direct parent and both scopes before a delayed child
    /// begins business. Caller starts that same child under this ledger lock.
    pub(crate) fn validate_parent_start(
        &self,
        child: ScopeId,
        parent: ScopeId,
        generation: Generation,
    ) -> Result<(), RuntimeError> {
        let child_scope = self.scopes.get(&child).ok_or(RuntimeError::UnknownScope)?;
        let (owner, link) = self
            .parent_links
            .get(&child)
            .copied()
            .ok_or(CapacityError::WrongOwner)?;
        if owner != parent || child_scope.parent() != Some(parent) {
            return Err(CapacityError::WrongOwner.into());
        }
        let parent_scope = self.scopes.get(&parent).ok_or(RuntimeError::UnknownScope)?;
        if parent_scope.generation() != generation {
            return Err(ScopeError::GenerationMismatch.into());
        }
        if parent_scope.failed() || child_scope.failed() {
            return Err(ScopeError::Failed.into());
        }
        if parent_scope.stopped() || child_scope.stopped() {
            return Err(ScopeError::Stopped.into());
        }
        let record = parent_scope.work(link).ok_or(ScopeError::UnknownWork)?;
        for work in [Some(link), record.parent].into_iter().flatten() {
            let original = parent_scope.work(work).ok_or(ScopeError::UnknownWork)?;
            if original.unbound_guard || original.body_exited || original.captures_exited {
                return Err(ScopeError::InvalidTransition.into());
            }
            if self.unresolved.contains(&(parent, work)) {
                return Err(RuntimeError::UnresolvedWork);
            }
        }
        Ok(())
    }
    pub(crate) fn note_header_ready(&mut self, scope: ScopeId, work: WorkId) {
        self.pending_retirement.insert((scope, work));
        if let Some(binding) = self.header_readiness.get(&(scope, work)) {
            binding.ready();
        }
    }
    fn note_header_retired(&self, scope: ScopeId, work: WorkId) {
        if let Some(binding) = self.header_readiness.get(&(scope, work)) {
            binding.ready();
        }
    }
    pub(crate) fn note_unresolved(&mut self, scope: ScopeId, work: WorkId) -> bool {
        if self.unresolved.insert((scope, work)) {
            let count = self.unresolved_by_scope.entry(scope).or_default();
            *count = count
                .checked_add(1)
                .expect("unresolved work count fits admitted work");
            true
        } else {
            false
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
    pub(crate) state: crate::observation::ObservedMutex<LedgerState>,
    pub(crate) changed: Notify,
    pub(crate) pending_capacity_wakes: Arc<crate::capacity_notification::PendingCapacityWakes>,
    pub(crate) history: Option<crate::history::HistoryCapture>,
    pub(crate) observation: crate::observation::RuntimeObservation,
    pub(crate) inventory: Option<Mutex<crate::observation::InventoryCursor>>,
}
impl LedgerShared {
    pub(crate) fn record_capacity_refusal<T>(
        &self,
        result: &Result<T, RuntimeError>,
        request: impl FnOnce() -> CapacityHistoryRequest,
    ) {
        let Err(error) = result else {
            return;
        };
        self.record(|| HistoryFact::CapacityRefused {
            request: request(),
            reason: match *error {
                RuntimeError::Capacity(error) => CapacityHistoryRefusal::Capacity(error),
                RuntimeError::CapacityDenied(d) => CapacityHistoryRefusal::CapacityDenied {
                    dimension: d.dimension,
                    keyed: d.keyed,
                    requested: d.requested,
                    committed: d.committed,
                    limit: d.limit,
                },
                RuntimeError::Scope(error) => CapacityHistoryRefusal::Scope(error),
                RuntimeError::Completion(error) => CapacityHistoryRefusal::Completion(error),
                RuntimeError::OwnedStorage(error) => CapacityHistoryRefusal::OwnedStorage(error),
                RuntimeError::UnknownScope => CapacityHistoryRefusal::UnknownScope,
                RuntimeError::DuplicateScope => CapacityHistoryRefusal::DuplicateScope,
                RuntimeError::UnknownSlot => CapacityHistoryRefusal::UnknownSlot,
                RuntimeError::DuplicateSlot => CapacityHistoryRefusal::DuplicateSlot,
                RuntimeError::ScopeNotRetired => CapacityHistoryRefusal::ScopeNotRetired,
                RuntimeError::UnresolvedWork => CapacityHistoryRefusal::UnresolvedWork,
                RuntimeError::IdentityExhausted => CapacityHistoryRefusal::IdentityExhausted,
            },
        });
    }
    pub(crate) fn record(&self, fact: impl FnOnce() -> HistoryFact) {
        if let Some(history) = &self.history {
            history.record(fact());
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
/// Exact declaration of one actual role, including its original consumers.
pub struct DeclaredInput {
    pub generation: Generation,
    pub kind: WorkKind,
    pub resources: ReservationResources,
    pub consumers: Vec<(
        novarocks_control_runtime_core::identity::ConsumerId,
        novarocks_control_runtime_core::completion::ConsumerKind,
    )>,
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
/// Read-only actual capacity samples under one original ledger transaction.
/// Owner maxima require an explicitly configured same-transaction index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityUsageSnapshot<const N: usize> {
    pub used: [u64; N],
    pub limits: [u64; N],
    pub max_owner_usage: u64,
}
/// Exact process dimension values, with no optional owner statistic invented.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityDimensionsSnapshot<const N: usize> {
    pub used: [u64; N],
    pub limits: [u64; N],
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityObservationError {
    OwnerIndexNotConfigured(DimensionId),
    UnknownDimension(DimensionId),
    Poisoned,
    InventoryOverflow,
}
impl RuntimeLedger {
    pub fn new(limits: ResourceVector) -> Self {
        Self::build(CapacityLedger::new(limits), None, Default::default())
    }
    pub fn with_history(limits: ResourceVector, history: crate::history::HistoryCapture) -> Self {
        Self::build(
            CapacityLedger::new(limits),
            Some(history),
            Default::default(),
        )
    }
    pub fn with_owner_usage_dimensions(
        limits: ResourceVector,
        dimensions: impl IntoIterator<Item = DimensionId>,
    ) -> Result<Self, RuntimeError> {
        Self::with_owner_usage_dimensions_and_history(limits, dimensions, None)
    }
    pub fn with_owner_usage_dimensions_and_history(
        limits: ResourceVector,
        dimensions: impl IntoIterator<Item = DimensionId>,
        history: Option<crate::history::HistoryCapture>,
    ) -> Result<Self, RuntimeError> {
        Self::with_owner_usage_dimensions_and_observation(
            limits,
            dimensions,
            history,
            Default::default(),
        )
    }
    pub fn with_owner_usage_dimensions_and_observation(
        limits: ResourceVector,
        dimensions: impl IntoIterator<Item = DimensionId>,
        history: Option<crate::history::HistoryCapture>,
        observation: crate::observation::RuntimeObservation,
    ) -> Result<Self, RuntimeError> {
        let ledger = CapacityLedger::with_owner_usage_dimensions(limits, dimensions)?;
        Ok(Self::build(ledger, history, observation))
    }
    pub fn with_keyed_families(
        limits: ResourceVector,
        families: impl IntoIterator<Item = (FamilyId, ResourceVector)>,
    ) -> Result<Self, RuntimeError> {
        Self::with_keyed_families_and_observation(limits, families, [], None, Default::default())
    }
    pub fn with_keyed_families_and_observation(
        limits: ResourceVector,
        families: impl IntoIterator<Item = (FamilyId, ResourceVector)>,
        owner_dimensions: impl IntoIterator<Item = DimensionId>,
        history: Option<crate::history::HistoryCapture>,
        observation: crate::observation::RuntimeObservation,
    ) -> Result<Self, RuntimeError> {
        let mut ledger = CapacityLedger::with_owner_usage_dimensions(limits, owner_dimensions)?;
        ledger.configure_keyed_families(families)?;
        Ok(Self::build(ledger, history, observation))
    }
    /// Busy is absence, never a fabricated zero. Rows exist only for live keys.
    pub fn try_keyed_usage_snapshot(&self) -> Option<Vec<KeyedUsageSnapshot>> {
        self.shared
            .state
            .try_lock()
            .ok()
            .map(|state| state.ledger.keyed_usage_snapshot())
    }
    fn build(
        ledger: CapacityLedger,
        history: Option<crate::history::HistoryCapture>,
        observation: crate::observation::RuntimeObservation,
    ) -> Self {
        Self {
            shared: Arc::new(LedgerShared {
                state: crate::observation::ObservedMutex::new(
                    LedgerState {
                        ledger,
                        capacity_waiters: Default::default(),
                        scopes: BTreeMap::new(),
                        slots: BTreeMap::new(),
                        unresolved: BTreeSet::new(),
                        work_labels: BTreeMap::new(),
                        unresolved_by_scope: BTreeMap::new(),
                        abandoned_bundles: BTreeSet::new(),
                        pending_retirement: BTreeSet::new(),
                        header_readiness: BTreeMap::new(),
                        parent_links: BTreeMap::new(),
                        last_scope: 0,
                        last_slot: 0,
                        external_open: true,
                    },
                    observation.clone(),
                ),
                changed: Notify::default(),
                pending_capacity_wakes: Arc::new(Default::default()),
                history,
                inventory: observation
                    .is_enabled()
                    .then(|| Mutex::new(Default::default())),
                observation,
            }),
        }
    }
    pub fn open_scope(
        &self,
        id: ScopeId,
        parent: Option<ScopeParent>,
        generation: Generation,
    ) -> Result<ScopeHandle, RuntimeError> {
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        self.open_scope_locked(&mut state, id, parent, generation)
    }
    /// Allocates the scope identity and any parent responsibility atomically
    /// with admission. Explicit open_scope IDs require an already serialized owner.
    pub fn open_scope_next(
        &self,
        parent: Option<ScopeParentAdmission>,
        generation: Generation,
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
        self.open_scope_locked(&mut state, id, parent, generation)
    }
    /// Bootstrap one child scope and its original root from a live parent.
    /// Parent-link admission, root reservation and publication share one ledger
    /// transaction, including when external intake has already closed. The
    /// root owns a real zero-resource reservation. Metadata follows its actual
    /// lifetime; resource capacity remains with the original payer. No quota is
    /// acquired again or inferred from the parent lease alone.
    ///
    /// The child inherits the parent's closed/stopped/failed gates before it
    /// becomes observable. A failed parent permits only its normal cleanup
    /// kinds, and failing the new child advances `child_generation` once, just
    /// like an existing scope failure. The original root remains admitted and
    /// can bind for cleanup; it cannot derive new Business work after failure.
    pub fn open_child_scope_with_root(
        &self,
        parent: &crate::obligation::WorkObligation,
        child_generation: Generation,
        root_kind: WorkKind,
        resources: ReservationResources,
    ) -> Result<(ScopeHandle, StepRegistration), RuntimeError> {
        let parent_is_local = Arc::ptr_eq(&self.shared, &parent.scope().runtime.shared);
        if !parent_is_local && self.shared.history.is_none() {
            let result = Err(CapacityError::WrongOwner.into());
            self.shared.observation.capacity_runtime(&result);
            return result;
        }

        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        let refusal_intent =
            self.shared
                .history
                .as_ref()
                .map(|_| CapacityHistoryRequest::OpenChildScope {
                    parent: parent.reference(),
                    parent_is_local,
                    parent_generation: if Arc::ptr_eq(&self.shared, &parent.scope().runtime.shared)
                    {
                        state
                            .scopes
                            .get(&parent.scope().id())
                            .map(ScopeState::generation)
                    } else {
                        None
                    },
                    child_generation,
                    kind: root_kind,
                    requested: resources.clone(),
                });
        let mut notify = false;
        let result = (|| {
            if !parent_is_local {
                return Err(CapacityError::WrongOwner.into());
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
            let mut child = ScopeState::new(id, Some(parent_scope), child_generation);
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
                    resources,
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
            self.shared.record(|| HistoryFact::WorkAdmitted {
                scope: parent_scope,
                work: link,
                parent: state.history_parent(parent_scope, link),
            });
            self.shared.record(|| HistoryFact::ScopeOpened {
                scope: id,
                generation: child_generation,
                parent: Some(ScopedWorkRef {
                    scope: parent_scope,
                    work: link,
                }),
            });
            self.shared.record(|| HistoryFact::Admitted {
                scope: id,
                work: root,
                reservation,
                parent: state.history_parent(id, root),
            });
            if parent_failed {
                self.shared.observation.scope_failed();
                self.shared.record(|| HistoryFact::OwnerFailed {
                    scope: id,
                    generation,
                });
            } else if parent_stopped {
                self.shared
                    .record(|| HistoryFact::ScopeStopped { scope: id });
            } else if parent_closed || !state.external_open {
                self.shared
                    .record(|| HistoryFact::ScopeClosed { scope: id });
            }
            let scope = ScopeHandle {
                runtime: self.clone(),
                id,
            };

            notify = true;
            Ok((
                scope.clone(),
                StepRegistration::new(scope, root, reservation),
            ))
        })();
        self.shared.record_capacity_refusal(&result, || {
            refusal_intent.expect("enabled history retains the original capacity intent")
        });
        drop(state);
        if notify {
            self.shared.changed.notify();
        }
        self.shared.observation.capacity_runtime(&result);
        result
    }
    fn open_scope_locked(
        &self,
        state: &mut LedgerState,
        id: ScopeId,
        parent: Option<ScopeParent>,
        generation: Generation,
    ) -> Result<ScopeHandle, RuntimeError> {
        if !state.external_open
            && !parent.is_some_and(|parent| matches!(parent.admission, WorkAdmission::Derived(_)))
        {
            return Err(ScopeError::Closed.into());
        }
        if id.get() <= state.last_scope || state.scopes.contains_key(&id) {
            return Err(RuntimeError::DuplicateScope);
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
            self.shared.record(|| HistoryFact::WorkAdmitted {
                scope: parent.scope,
                work: parent.work,
                parent: state.history_parent(parent.scope, parent.work),
            });
        }
        state.scopes.insert(
            id,
            ScopeState::new(id, parent.map(|parent| parent.scope), generation),
        );
        state.last_scope = id.get();
        if !state.external_open {
            state
                .scopes
                .get_mut(&id)
                .expect("new child scope")
                .close_external();
        }
        self.shared.record(|| HistoryFact::ScopeOpened {
            scope: id,
            generation,
            parent: parent.map(|parent| ScopedWorkRef {
                scope: parent.scope,
                work: parent.work,
            }),
        });
        Ok(ScopeHandle {
            runtime: self.clone(),
            id,
        })
    }
    /// Nonblocking metadata-only read. No Scope, Work or reservation scan is
    /// performed, and neither missing configuration nor a busy lock means zero.
    pub fn try_capacity_usage<const N: usize>(
        &self,
        dimensions: [DimensionId; N],
        owner_dimension: DimensionId,
    ) -> Result<Option<CapacityUsageSnapshot<N>>, CapacityObservationError> {
        let state = match self.shared.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(CapacityObservationError::Poisoned);
            }
        };
        let max_owner_usage = state.ledger.max_owner_usage(owner_dimension).ok_or(
            CapacityObservationError::OwnerIndexNotConfigured(owner_dimension),
        )?;
        let mut used = [0; N];
        let mut limits = [0; N];
        for (index, dimension) in dimensions.into_iter().enumerate() {
            let (value, limit) = state
                .ledger
                .dimension_usage(dimension)
                .ok_or(CapacityObservationError::UnknownDimension(dimension))?;
            used[index] = value;
            limits[index] = limit;
        }
        Ok(Some(CapacityUsageSnapshot {
            used,
            limits,
            max_owner_usage,
        }))
    }
    /// Nonblocking process observations without an owner-usage index. Busy is
    /// unavailable, and an unknown dimension remains an explicit typed error.
    pub fn try_capacity_dimensions<const N: usize>(
        &self,
        dimensions: [DimensionId; N],
    ) -> Result<Option<CapacityDimensionsSnapshot<N>>, CapacityObservationError> {
        let state = match self.shared.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(CapacityObservationError::Poisoned);
            }
        };
        let mut used = [0; N];
        let mut limits = [0; N];
        for (index, dimension) in dimensions.into_iter().enumerate() {
            let (value, limit) = state
                .ledger
                .dimension_usage(dimension)
                .ok_or(CapacityObservationError::UnknownDimension(dimension))?;
            used[index] = value;
            limits[index] = limit;
        }
        Ok(Some(CapacityDimensionsSnapshot { used, limits }))
    }
    pub fn owner_usage_metadata_entries(&self, dimension: DimensionId) -> Option<usize> {
        self.shared
            .state
            .lock()
            .expect("runtime ledger metadata geometry")
            .ledger
            .owner_usage_metadata_entries(dimension)
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
            self.shared
                .record(|| HistoryFact::ScopeClosed { scope: *id });
        }
        drop(state);
        self.shared.changed.notify();
        ids
    }
    /// Each physically exited, unpinned header retirement is one budget unit.
    /// The last actual input holder queues its exact newly ready header; a
    /// ready parent is queued for a later bounded visit.
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
    /// Reap only the header named by this same ledger's original observer.
    /// Readiness still comes from canonical body/capture/child/pin gates.
    pub fn reap_observed_header(
        &self,
        observer: &crate::obligation::WorkRetirementObserver,
    ) -> Result<bool, RuntimeError> {
        let (scope, work) = observer
            .for_ledger(self)
            .ok_or(ScopeError::InvalidTransition)?;
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        let Some(owner) = state.scopes.get(&scope) else {
            return Ok(true);
        };
        if owner.work(work).is_none() {
            return Ok(true);
        }
        if !owner.work_header_retirement_ready(work)? {
            return Ok(false);
        }
        self.retire_work_locked(&mut state, scope, work)?;
        state.pending_retirement.remove(&(scope, work));
        drop(state);
        self.shared.changed.notify();
        Ok(true)
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
        state.work_labels.remove(&(scope, work));
        state.note_header_retired(scope, work);
        self.shared
            .record(|| HistoryFact::WorkRetired { scope, work });
        if let Some(parent) = parent {
            state.note_header_ready(scope, parent);
        }
        Ok(())
    }
    pub fn remove_scope(&self, id: ScopeId) -> Result<(), RuntimeError> {
        let mut state = self.shared.state.lock().expect("runtime ledger lock");
        let scope = state.scopes.get(&id).ok_or(RuntimeError::UnknownScope)?;
        // Every live child owns an intrinsic Work link in this Scope. That
        // link exits only when the actual child is removed, so the original
        // retirement proof already excludes children without a node-wide scan.
        if !scope.retirement_ready()
            || state.ledger.reservation_count_for(id) != 0
            || state
                .header_readiness
                .range((id, WorkId::new(1).expect("minimum Work identity"))..)
                .next()
                .is_some_and(|((owner, _), _)| *owner == id)
        {
            return Err(RuntimeError::ScopeNotRetired);
        }
        if let Some(&(parent, work)) = state.parent_links.get(&id) {
            let owner = state
                .scopes
                .get(&parent)
                .expect("the original child retains its parent Scope");
            let link = owner
                .work(work)
                .expect("the original Scope owns its intrinsic parent link");
            // Only this Scope owns the link. It has no asynchronous body,
            // capture, report or capacity reservation beyond that ownership.
            // Validate its actual record before removing either original.
            if link.children != 0
                || link.header_pin_count() != 0
                || link.body_exited
                || link.captures_exited
                || owner.captures_bound(work)?
            {
                return Err(RuntimeError::ScopeNotRetired);
            }
        }
        state.scopes.remove(&id);
        // Actor-owned roles can retire atomically with execution detachment.
        // Remove any remaining diagnostic labels without scanning other scopes.
        let labels = (id, WorkId::new(1).expect("minimum Work identity"))
            ..=(id, WorkId::new(u64::MAX).expect("maximum Work identity"));
        while let Some(key) = state
            .work_labels
            .range(labels.clone())
            .next()
            .map(|(key, _)| *key)
        {
            state.work_labels.remove(&key);
        }
        self.shared
            .record(|| HistoryFact::ScopeRemoved { scope: id });
        if let Some((parent, work)) = state.parent_links.remove(&id) {
            let parent_scope = state
                .scopes
                .get_mut(&parent)
                .expect("child scope retains its parent");
            parent_scope
                .body_exited(work)
                .expect("preflighted intrinsic parent link body");
            parent_scope
                .captures_exited(work)
                .expect("preflighted capture-free intrinsic parent link");
            self.shared.record(|| HistoryFact::BodyExited {
                scope: parent,
                work,
            });
            self.shared.record(|| HistoryFact::CapturesExited {
                scope: parent,
                work,
            });
            // Removing the actual child ends the link itself in this same
            // transaction. A cold actor never leaves this join obligation
            // pending on a later unrelated process-scope reaper turn.
            self.retire_work_locked(&mut state, parent, work)
                .expect("preflighted original child-free parent link");
        }
        drop(state);
        self.shared.changed.notify();
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        let state = self.shared.state.lock().expect("runtime ledger lock");
        state.scopes.is_empty()
            && state.ledger.reservation_count() == 0
            && state.slots.is_empty()
            && state.header_readiness.is_empty()
    }
    pub fn change_version(&self) -> u64 {
        self.shared.changed.version()
    }
    pub fn wait_after(&self, version: u64) {
        self.shared.changed.wait_after(version);
    }
}
impl ScopeHandle {
    /// Reap bounded real ready headers through this actual financial owner.
    /// The handle selects its original ledger; numeric ScopeId equality grants
    /// no authority to retire a different runtime's records.
    pub fn reap_ready(&self, budget: usize) -> Result<usize, RuntimeError> {
        self.runtime.reap_scope_ready(self.id, budget)
    }
    /// Observe one original Work record through its actual owning Scope.
    /// This neither recreates a retired header nor authorizes new input/work.
    pub fn work_lifecycle(
        &self,
        work: WorkId,
    ) -> Result<
        Option<(
            novarocks_control_runtime_core::scope::WorkPhase,
            bool,
            bool,
            usize,
        )>,
        RuntimeError,
    > {
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
        Ok(scope.work(work).map(|record| {
            (
                record.phase,
                record.body_exited,
                record.captures_exited,
                record.children,
            )
        }))
    }
    pub const fn id(&self) -> ScopeId {
        self.id
    }
    /// Identity of the same original scope, independent of its current gates
    /// or generation. Matching numbers in another ledger do not match. This
    /// read grants no admission, start, publication or capacity authority.
    pub fn same_scope(&self, other: &Self) -> bool {
        self.id == other.id && Arc::ptr_eq(&self.runtime.shared, &other.runtime.shared)
    }
    /// Read-only ledger identity across two original scopes. This grants no
    /// resource, publication, lifecycle or cross-scope access authority.
    pub fn same_ledger(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.runtime.shared, &other.runtime.shared)
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
        self.runtime.shared.record(|| HistoryFact::SlotReserved {
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
        self.runtime.shared.record(|| HistoryFact::SlotReserved {
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
        self.runtime.shared.record(|| HistoryFact::SlotReserved {
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
            .record(|| HistoryFact::SlotRetired { slot });
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
    /// Original positions and already-held quota move into exactly this real
    /// role. Every ordinary refusal precedes mutation, with no exposed partial.
    pub fn reserve_child_from(
        &self,
        payer: &mut crate::obligation::ReservationObligation,
        actual_parent: &crate::obligation::WorkObligation,
        role: DeclaredInput,
    ) -> Result<crate::reported_work::ReportedWork, RuntimeError> {
        self.reserve_child_from_parent(payer, actual_parent.scope(), actual_parent.id(), role)
    }
    /// Consume the same original payer under an authority issued by its real
    /// parent Work. The authority grants no storage or parent keepalive;
    /// lifecycle and Unknown-exit checks share the admission transaction.
    pub fn reserve_child_from_authority(
        &self,
        payer: &mut crate::obligation::ReservationObligation,
        authority: &crate::obligation::WorkChildAuthority,
        role: DeclaredInput,
    ) -> Result<crate::reported_work::ReportedWork, RuntimeError> {
        self.reserve_child_from_parent(payer, authority.scope(), authority.parent(), role)
    }
    fn reserve_child_from_parent(
        &self,
        payer: &mut crate::obligation::ReservationObligation,
        parent_scope: &ScopeHandle,
        parent: WorkId,
        role: DeclaredInput,
    ) -> Result<crate::reported_work::ReportedWork, RuntimeError> {
        let precheck =
            if !self.same_scope(parent_scope) || !self.same_scope(&payer.scope) || !payer.active {
                Some(OwnedStorageError::WrongOwner)
            } else if role.resources.owned_slots.completions == 0 {
                Some(OwnedStorageError::MissingPosition(
                    OwnedSlotKind::Completion,
                ))
            } else {
                None
            };
        if self.runtime.shared.history.is_none()
            && let Some(error) = precheck
        {
            let result = Err(error.into());
            self.runtime.shared.observation.capacity_runtime(&result);
            return result;
        }

        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let refusal_intent = self.runtime.shared.history.as_ref().map(|_| {
            CapacityHistoryRequest::ReserveChildRole {
                scope: self.id,
                generation: role.generation,
                payer: payer.id,
                payer_is_local: self.same_scope(&payer.scope),
                parent: ScopedWorkRef {
                    scope: parent_scope.id(),
                    work: parent,
                },
                parent_is_local: self.same_scope(parent_scope),
                kind: role.kind,
                requested: role.resources.clone(),
            }
        });
        let mut notify = false;
        let result = (|| {
            use crate::{
                completion::{ReportRecord, ReportSlot},
                obligation::{ReservationObligation, WorkObligation},
                reported_work::ReportedWork,
            };
            if let Some(error) = precheck {
                return Err(error.into());
            }

            if state.unresolved.contains(&(self.id, parent)) {
                return Err(RuntimeError::UnresolvedWork);
            }
            let scope = state
                .scopes
                .get(&self.id)
                .ok_or(RuntimeError::UnknownScope)?;
            let work = scope.next_work_id()?;
            scope.check_admission(
                role.generation,
                work,
                role.kind,
                WorkAdmission::Derived(parent),
            )?;
            let reservation = state.ledger.next_reservation_id()?;
            let report_id = state.next_slot_id(self.id)?;
            let report = novarocks_control_runtime_core::completion::CompletionSlot::new(
                report_id,
                self.id,
                role.generation,
                work,
                role.consumers.len(),
                role.consumers,
            )?;
            {
                let (ledger, scope) = state.parts(self.id)?;
                ledger.reserve_from(
                    scope,
                    payer.id,
                    ReservationRequest {
                        id: reservation,
                        generation: role.generation,
                        guard: work,
                        resources: role.resources,
                        admission: WorkAdmission::Derived(parent),
                        kind: role.kind,
                    },
                )?;
                ledger
                    .bind(scope, reservation, role.kind)
                    .expect("preflighted actual role binding");
            }
            state
                .reserve_slot(self.id, report_id)
                .expect("preflighted actual report position");
            let completion = state
                .ledger
                .take_owned(reservation, OwnedSlotKind::Completion)
                .expect("preflighted original completion");
            if let Some(captures) = state
                .ledger
                .take_owned_kind(reservation, OwnedSlotKind::Capture)
                .expect("committed original role inventory")
            {
                state
                    .scopes
                    .get_mut(&self.id)
                    .expect("committed original scope")
                    .bind_captures(work, captures)
                    .expect("new actual role owns its declared captures");
            }
            assert!(
                state
                    .slots
                    .insert(report_id, ReportRecord::new(report, completion))
                    .is_none()
            );
            payer.active = state.ledger.reservation(payer.id).is_some();
            self.runtime.shared.record(|| HistoryFact::Admitted {
                scope: self.id,
                work,
                reservation,
                parent: state.history_parent(self.id, work),
            });
            self.runtime.shared.record(|| HistoryFact::SlotReserved {
                scope: self.id,
                slot: report_id,
            });

            notify = true;
            Ok(ReportedWork::from_committed(
                WorkObligation::new(self.clone(), work),
                ReservationObligation {
                    scope: self.clone(),
                    id: reservation,
                    active: true,
                },
                ReportSlot {
                    runtime: self.runtime.clone(),
                    id: report_id,
                },
            ))
        })();
        self.runtime.shared.record_capacity_refusal(&result, || {
            refusal_intent.expect("enabled history retains the original capacity intent")
        });
        drop(state);
        if notify {
            self.runtime.shared.changed.notify();
        }
        self.runtime.shared.observation.capacity_runtime(&result);
        result
    }
    /// Production allocation: identity selection and admission are one ledger
    /// transaction. A preempted producer never loses to a later external ID.
    pub fn reserve_next(
        &self,
        generation: Generation,
        resources: ReservationResources,
        kind: WorkKind,
        admission: WorkAdmission,
    ) -> Result<StepRegistration, RuntimeError> {
        self.reserve_next_parent(generation, resources, kind, admission, None, None)
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
        resources: ReservationResources,
        kind: WorkKind,
    ) -> Result<StepRegistration, RuntimeError> {
        self.reserve_next_parent(
            generation,
            resources,
            kind,
            WorkAdmission::External,
            Some(parent),
            None,
        )
    }
    pub fn reserve_next_or_wait(
        &self,
        generation: Generation,
        resources: ReservationResources,
        kind: WorkKind,
        admission: WorkAdmission,
        registrant: &crate::capacity_notification::CapacityRegistrant,
    ) -> Result<StepRegistration, RuntimeError> {
        self.reserve_next_parent(
            generation,
            resources,
            kind,
            admission,
            None,
            Some(registrant),
        )
    }
    pub fn reserve_child_next_or_wait(
        &self,
        generation: Generation,
        parent: ReservationId,
        resources: ReservationResources,
        kind: WorkKind,
        registrant: &crate::capacity_notification::CapacityRegistrant,
    ) -> Result<StepRegistration, RuntimeError> {
        self.reserve_next_parent(
            generation,
            resources,
            kind,
            WorkAdmission::External,
            Some(parent),
            Some(registrant),
        )
    }
    fn reserve_next_parent(
        &self,
        generation: Generation,
        resources: ReservationResources,
        kind: WorkKind,
        admission: WorkAdmission,
        parent: Option<ReservationId>,
        registrant: Option<&crate::capacity_notification::CapacityRegistrant>,
    ) -> Result<StepRegistration, RuntimeError> {
        if registrant.is_some_and(|registrant| !registrant.belongs_to(&self.runtime)) {
            return Err(CapacityError::WrongOwner.into());
        }
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let refusal_intent = self.runtime.shared.history.as_ref().map(|_| match parent {
            Some(parent) => CapacityHistoryRequest::ReserveFromParent {
                scope: self.id,
                generation,
                parent,
                kind,
                requested: resources.clone(),
            },
            None => CapacityHistoryRequest::Reserve {
                scope: self.id,
                generation,
                admission,
                kind,
                requested: resources.clone(),
            },
        });
        let result = (|| {
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
            match reserved {
                Err(CapacityError::Exhausted(dimension)) => {
                    return Err(RuntimeError::CapacityDenied(CapacityDenial {
                        dimension,
                        keyed: None,
                        requested: requested.quota.get(dimension),
                        committed: ledger.used(dimension),
                        limit: ledger.limit(dimension),
                    }));
                }
                Err(CapacityError::ExhaustedKeyed {
                    family,
                    key,
                    dimension,
                }) => {
                    let (committed, limit) = ledger
                        .keyed_dimension_usage(family, key, dimension)
                        .expect("denied configured keyed dimension");
                    return Err(RuntimeError::CapacityDenied(CapacityDenial {
                        dimension,
                        keyed: Some((family, key)),
                        requested: requested
                            .keyed
                            .as_ref()
                            .expect("denied requested keyed quota")
                            .quota
                            .get(dimension),
                        committed,
                        limit,
                    }));
                }
                result => {
                    result?;
                }
            }
            self.runtime.shared.record(|| HistoryFact::Admitted {
                scope: self.id,
                work,
                reservation,
                parent: state.history_parent(self.id, work),
            });
            Ok(StepRegistration::new(self.clone(), work, reservation))
        })();
        self.runtime.shared.record_capacity_refusal(&result, || {
            refusal_intent.expect("enabled history retains the original capacity intent")
        });
        if let Some(registrant) = registrant
            && let Err(RuntimeError::CapacityDenied(denial)) = &result
            && state.capacity_waiters.register(registrant, *denial)
        {
            self.runtime
                .shared
                .record(|| HistoryFact::CapacityWaitRegistered {
                    registrant: registrant.id(),
                    target: denial.wait_target(),
                });
        }
        // A successful different candidate must retain an earlier wait:
        // the same actor may still have a refused target to revisit.
        // New denials merge bounded process/family dimension domains;
        // the first matching release or cancellation consumes all of them.
        drop(state);
        self.runtime.shared.observation.capacity_runtime(&result);
        result
    }
    /// Explicit identities are for deterministic tests or an already serialized
    /// identity owner. Concurrent production callers use reserve_next.
    pub fn reserve(&self, request: ReservationRequest) -> Result<StepRegistration, RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let refusal_intent =
            self.runtime
                .shared
                .history
                .as_ref()
                .map(|_| CapacityHistoryRequest::Reserve {
                    scope: self.id,
                    generation: request.generation,
                    admission: request.admission,
                    kind: request.kind,
                    requested: request.resources.clone(),
                });
        let result = (|| {
            let reservation = request.id;
            let work = request.guard;

            if !state.external_open && request.admission == WorkAdmission::External {
                return Err(ScopeError::Closed.into());
            }
            let (ledger, scope) = state.parts(self.id)?;
            ledger.try_reserve_bundle(scope, request)?;
            self.runtime.shared.record(|| HistoryFact::Admitted {
                scope: self.id,
                work,
                reservation,
                parent: state.history_parent(self.id, work),
            });
            Ok(StepRegistration::new(self.clone(), work, reservation))
        })();
        self.runtime.shared.record_capacity_refusal(&result, || {
            refusal_intent.expect("enabled history retains the original capacity intent")
        });
        drop(state);
        self.runtime.shared.observation.capacity_runtime(&result);
        result
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
            .record(|| HistoryFact::ScopeClosed { scope: self.id });
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
            .record(|| HistoryFact::ScopeStopped { scope: self.id });
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
        let scope = state
            .scopes
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownScope)?;
        let was_failed = scope.failed();
        let generation = scope.fail()?;
        if !was_failed {
            self.runtime.shared.observation.scope_failed();
            self.runtime.shared.record(|| HistoryFact::OwnerFailed {
                scope: self.id,
                generation,
            });
        }
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
    ) -> Result<crate::obligation::WorkObligation, RuntimeError> {
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
        self.runtime.shared.record(|| HistoryFact::WorkAdmitted {
            scope: self.id,
            work: id,
            parent: state.history_parent(self.id, id),
        });
        Ok(crate::obligation::WorkObligation::new(self.clone(), id))
    }
}

#[cfg(all(test, not(loom)))]
mod capacity_dimensions_tests {
    use super::*;
    #[test]
    fn dimension_observation_busy_is_unavailable_without_owner_index_or_zero_fallback() {
        let dimension = DimensionId::new(1).unwrap();
        let ledger = RuntimeLedger::new(ResourceVector::new([(dimension, 3)]).unwrap());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let held = scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::for_work(ResourceVector::new([(dimension, 2)]).unwrap()),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap();
        let lock = ledger.shared.state.lock().unwrap();
        assert_eq!(ledger.try_capacity_dimensions([dimension]).unwrap(), None);
        drop(lock);
        let sample = ledger
            .try_capacity_dimensions([dimension])
            .unwrap()
            .unwrap();
        assert_eq!(sample.used, [2]);
        assert_eq!(sample.limits, [3]);
        assert_eq!(
            ledger.try_capacity_dimensions([DimensionId::new(2).unwrap()]),
            Err(CapacityObservationError::UnknownDimension(
                DimensionId::new(2).unwrap()
            ))
        );
        drop(held);
        assert_eq!(
            ledger
                .try_capacity_dimensions([dimension])
                .unwrap()
                .unwrap()
                .used,
            [0]
        );
    }
}
