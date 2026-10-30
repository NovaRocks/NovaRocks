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

//! Capabilities and records in the original WC state. No proof runs here.

use super::*;
use crate::{
    WorkId, WorkOwner, WorkRequest, WorkScope, WorkloadControl,
    control_storage::{CancellationStorage, SlotId, SlotPhase},
    scope::{Inner, State},
};
use novarocks_resource_contract::SharedDependency;
use std::{
    collections::BTreeMap,
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll, Waker},
};

pub(crate) mod root_domains;
use root_domains::{GroupOrigin, RootDomainChange};
pub use root_domains::{
    RootDomainBinding, RootDomainInitialAdmission, RootDomainInitialProposal,
    RootDomainInitialSource,
};
mod typed_custody;
pub(crate) use paid_proposal::{capture_native_action, delegate_native_actions};
use typed_custody::ClaimCustody;
pub use typed_custody::{
    InternalComputationAlias, InternalComputationGrant, NativeAsyncActionGrant,
    NativeDispatchFrozen, NativeDispatchSource, NativeQueuedInputGrant, NativeReplyGrant,
    RetainedPayloadAlias, RetainedPayloadGrant, UnreceivedProductionAdmission,
    UnreceivedProductionGrant, UnreceivedProductionOwner, UnreceivedProductionQuote,
    UnreceivedProductionReady,
};
pub(crate) use typed_custody::{validate_native_computation_fence, validate_native_payload_fence};
mod control_binding;
pub use control_binding::ControlBindingProposal;
use control_binding::{validate_active_control_node, validate_control_binding_shape};
mod control_storage;
mod dependency_storage;
mod paid_proposal;
#[cfg(test)]
pub(crate) mod retained_claim;
pub use paid_proposal::{
    AsyncActionAdmission, AsyncActionGrant, AsyncActionSource, BlockingActionAdmission,
    BlockingActionSource, NativeAsyncActionAdmission, NativeAsyncActionAuthority,
    NativeAsyncActionQuote, NativeAsyncActionSource, NativeQueuedInputAdmission,
    NativeQueuedInputQuote, NativeQueuedInputReady, RootPayloadInitialAdmission,
    RootPayloadInitialSource,
};
use paid_proposal::{
    ControlActionFence, CpuActionFence, NativeActionFence, RootPayloadFence,
    validate_control_action, validate_control_productions, validate_cpu_action,
    validate_native_action, validate_root_payload, validate_root_stage,
};
pub use paid_proposal::{
    InternalCpuActionAdmission, InternalCpuActionSource, InternalCpuFactoryBinding,
    InternalResultClosedFactoryDescriptor,
};
pub use paid_proposal::{
    InternalManifestBinding, JointProposal, RootPayloadAdmission, RootPayloadProposal,
    RootPayloadReady, RootPayloadSource, RootStageAdmission, RootStageProposal, RootStageReady,
    RootStageSource, SourceCohort,
};
pub(crate) mod proposal_storage;
pub(crate) use control_storage::control_storage_base;
use dependency_storage::{DependencyRef, DependencyRegistry};
use proposal_storage::{
    ChangeBody, ProposalConstruction, ProposalKey, ProposalReservation, queue_proposal,
};
pub use proposal_storage::{
    JointGrantedClaims, JointProposalSlotSignal, JointProposalSlotWait, JointTryBegin,
};

pub(super) struct DependencyRecord {
    pub(super) ordinal: u64,
    pub(super) fact: SharedDependency,
}

/// A registry identity for an actual selected dependency, not an I/O permit.
#[cfg(test)]
pub struct JointDependency {
    inner: Arc<Inner>,
    pub(super) record: DependencyRef,
    _exit: crate::scope::ReclaimWakeOnExit,
}
#[cfg(test)]
impl Clone for JointDependency {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            record: self.record.clone(),
            _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
        }
    }
}
#[cfg(test)]
impl JointDependency {
    pub fn fact(&self) -> &SharedDependency {
        &self.record.fact
    }
}

#[derive(Clone)]
pub struct JointAdmissionHandle {
    pub(super) inner: Arc<Inner>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct FactKey {
    pub(super) origin: GroupOrigin,
    class: usize,
    lane: usize,
    dependencies: Vec<u64>,
    pub(super) source_kind: SourceKind,
}
pub(super) struct ResponsibilityFacts {
    pub(super) key: Arc<FactKey>,
    pub(super) class: WorkClass,
    pub(super) lane: JointLane,
    pub(super) dependencies: Arc<Vec<DependencyRef>>,
}
#[derive(Clone)]
pub(super) struct ProposedClaim {
    pub(super) facts: Arc<ResponsibilityFacts>,
    pub(super) amounts: JointResources,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClaimOwner {
    PhysicalWork(WorkId),
    ControlSlot(SlotId),
    #[cfg(test)]
    OwnerRetained(WorkId),
}

pub(super) struct ClaimRecord {
    internal_position: bool,
    computation_window: Option<crate::result_window::ClientComputationWindow>,
    /// Only closed Root initial/Stage Ready contains these uninstalled raws.
    /// No caller can extract them before the same atomic domain install.
    domain_birth: Option<u64>,
    native_dispatch: Option<typed_custody::NativeDispatchOrigin>,
    dispatch_staging: Option<u64>,
    custody: ClaimCustody,
    production: Option<typed_custody::unreceived::ProductionRecord>,
    pub(super) owner: ClaimOwner,
    /// Charged proposal, including any not-yet-installed delta and DI union.
    pub(super) facts: Arc<ResponsibilityFacts>,
    pub(super) amounts: JointResources,
    active_facts: Arc<ResponsibilityFacts>,
    active_amounts: JointResources,
    pending: Option<u64>,
    structure_revision: u64,
    release_revision: u64,
}
/// Sealed by the original holder; a numeric claim ID grants no authority.
#[derive(Clone, Copy)]
enum ClaimSource {
    Physical,
    Production {
        work: WorkId,
        epoch: u64,
    },
    Payload {
        epoch: u64,
    },
    Computation {
        epoch: u64,
    },
    NodeControl {
        work: WorkId,
        slot: SlotId,
        generation: u64,
    },
    PoolFree {
        slot: SlotId,
        generation: u64,
        parent: ParentControlFence,
        deadline: Option<tokio::time::Instant>,
    },
}
#[derive(Clone, Copy)]
struct ParentControlFence {
    work: WorkId,
    slot: SlotId,
    generation: u64,
    claim: u64,
    structure_revision: u64,
    release_revision: u64,
}
#[derive(Clone)]
pub struct JointClaimView {
    inner: Weak<Inner>,
    id: u64,
    source: ClaimSource,
    structure_revision: u64,
    release_revision: u64,
}
#[derive(Clone)]
pub(super) struct ChangeRecord {
    pub(super) old: Option<u64>,
    pub(super) original_amounts: Option<JointResources>,
    pub(super) original_facts: Option<Arc<ResponsibilityFacts>>,
    source: Option<JointClaimView>,
    pub(super) claim: ProposedClaim,
}
pub(super) enum RequestPhase {
    Waiting,
    Granted {
        new_ids: Vec<u64>,
    },
    /// The original reply retains these claims while its sole caller tries the
    /// selected local physical owner. Implicit Drop must never release them.
    LocalOffered {
        new_ids: Vec<u64>,
        failure: Option<WorkError>,
        // Set before arbitrary physical Drop; an unwind cannot be retried.
        exiting: bool,
    },
    /// New claims have crossed to their physical recipients. Withdrawal must
    /// never release them. Only the batch adjustment receipt remains here.
    Delivered,
    Failed(WorkError),
}
#[derive(Clone, Copy)]
struct RootStageFence {
    canonical: bool,
    stage: WorkId,
    payload: u64,
    epoch: u64,
    // None is the original New-compute mode; Some authenticates exactly one
    // existing typed stage claim without granting another computation.
    computation: Option<[u64; 2]>,
}
pub(super) struct RequestRecord {
    pub(super) root_domain: Option<RootDomainChange>,
    // Restricted by closed original sources, never caller-supplied identities.
    root_stage: Option<RootStageFence>,
    computation_window: Option<crate::result_window::ClientComputationWindow>,
    root_payload: Option<RootPayloadFence>,
    cpu_action: Option<CpuActionFence>,
    control_action: Option<ControlActionFence>,
    native_action: Option<NativeActionFence>,
    pub(super) owner: WorkId,
    pub(super) changes: ChangeBody,
    source_cohort: bool,
    proposal: Option<ProposalKey>,
    pub(super) phase: RequestPhase,
    /// Only the original service timer arms a new local observation pass.
    pub(super) local_blocked: bool,
    pub(super) waker: Option<Waker>,
}
pub(crate) struct JointLedger {
    pub(super) config: Arc<ResponsibilityConfig>,
    dependencies: Option<DependencyRegistry>,
    pub(super) claims: BTreeMap<u64, ClaimRecord>,
    pub(super) requests: BTreeMap<u64, RequestRecord>,
    /// A rollback can rekey a live charge behind a snapshot's ordered cursor.
    /// Such a change invalidates both positive and negative certificates.
    pub(super) facts_revision: u64,
    pub(super) driver_closed: bool,
    /// Count of local_blocked flags in this same original request inventory.
    pub(super) local_waiters: usize,
    control_empty_facts: [Option<Arc<ResponsibilityFacts>>; 5],
}
impl JointLedger {
    pub(super) fn has_reclaimable_dependencies(&self) -> bool {
        self.dependencies
            .as_ref()
            .is_some_and(|rows| rows.has_zero())
    }
    pub(super) fn arm_dependency_sweep(&mut self) {
        if let Some(rows) = &mut self.dependencies {
            rows.arm_sweep();
        }
    }
    pub(super) fn reap_dependency(&mut self) -> bool {
        self.dependencies
            .as_mut()
            .is_some_and(|rows| rows.reap_one())
    }
    pub(crate) fn is_drained(&self, storage: &CancellationStorage) -> bool {
        if !self.requests.is_empty()
            || self.local_waiters != 0
            || self
                .dependencies
                .as_ref()
                .is_some_and(|rows| !rows.is_empty())
        {
            return false;
        }
        if !storage.fixed {
            return self.claims.is_empty();
        }
        if !storage.drained() || self.claims.len() != storage.slots.len() {
            return false;
        }
        for (index, slot) in storage.slots.iter().enumerate() {
            if slot.phase != SlotPhase::Free {
                return false;
            }
            let Some(record) = slot.claim.and_then(|id| self.claims.get(&id)) else {
                return false;
            };
            let Some(empty) = &self.control_empty_facts[class_index(record.facts.class)] else {
                return false;
            };
            if record.owner != ClaimOwner::ControlSlot(SlotId(index))
                || record.pending.is_some()
                || record.amounts != slot.base
                || record.active_amounts != slot.base
                || record.facts.key != empty.key
                || record.active_facts.key != empty.key
            {
                return false;
            }
        }
        true
    }
    pub(super) fn released(&mut self) {
        self.layout_changed();
    }
    fn layout_changed(&mut self) {
        match self.facts_revision.checked_add(1) {
            Some(revision) => self.facts_revision = revision,
            None => self.driver_closed = true,
        }
    }
}
impl WorkloadControl {
    pub fn configure_responsibility(
        &self,
        config: ResponsibilityConfig,
    ) -> Result<(JointAdmissionHandle, JointProgressDriver), WorkError> {
        let config = Arc::new(config);
        self.inner.update(|state| {
            if state.ready
                || state.root_admission_is_closed()
                || !state.nodes.is_empty()
                || state.joint.is_some()
            {
                return Err(WorkError::Conflict);
            }
            state.joint = Some(JointLedger {
                config: Arc::clone(&config),
                dependencies: Some(DependencyRegistry::new()),
                claims: BTreeMap::new(),
                requests: BTreeMap::new(),
                facts_revision: 0,
                driver_closed: false,
                local_waiters: 0,
                control_empty_facts: std::array::from_fn(|_| None),
            });
            Ok((
                JointAdmissionHandle {
                    inner: Arc::clone(&self.inner),
                },
                JointProgressDriver::new(Arc::clone(&self.inner), config),
            ))
        })
    }
}
impl JointAdmissionHandle {
    #[cfg(test)]
    pub fn register_dependency(
        &self,
        fact: SharedDependency,
    ) -> Result<JointDependency, WorkError> {
        self.inner.update(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if state.closed || ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            let ledger = state.joint.as_mut().unwrap();
            let registry = ledger.dependencies.as_mut().ok_or(WorkError::NotReady)?;
            if let Some(record) = registry.lookup(&fact) {
                return Ok(JointDependency {
                    inner: Arc::clone(&self.inner),
                    record,
                    _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
                });
            }
            let ordinal = state.next_id()?;
            let record = state
                .joint
                .as_mut()
                .unwrap()
                .dependencies
                .as_mut()
                .unwrap()
                .insert(fact, ordinal)?;
            Ok(JointDependency {
                inner: Arc::clone(&self.inner),
                record,
                _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
            })
        })
    }

    /// Propose a same-class child using exact resident backing reserved by
    /// this original paid constructor. Capacity is granted only by its proof.
    pub fn request_child(
        &self,
        owner: &WorkOwner,
        request: WorkRequest,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        self.prepare_child(&owner.scope(), Some(request))
            .and_then(|prepared| self.queue_request(prepared))
            .map_err(|error| JointAdmissionFailure { error })
    }

    fn prepare_child(
        &self,
        scope: &WorkScope,
        request: Option<WorkRequest>,
    ) -> Result<PreparedRequest, WorkError> {
        let custody = self.reserve_proposal(scope)?;
        self.prepare_child_with_custody(scope, request, custody)
    }

    fn prepare_child_with_custody(
        &self,
        scope: &WorkScope,
        request: Option<WorkRequest>,
        mut custody: Option<ProposalConstruction>,
    ) -> Result<PreparedRequest, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let (source, from, base, to) = {
            let mut state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if state.closed || ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if !state.cancellations.fixed {
                return Err(WorkError::NotReady);
            }
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            node.check(&state.cancellations)?;
            if node.owner != crate::OwnerState::Active {
                return Err(WorkError::Conflict);
            }
            let request = request.unwrap_or_else(|| WorkRequest::new(node.class));
            if node.class != request.class {
                return Err(WorkError::Conflict);
            }
            let parent_slot = &state.cancellations.slots[node.cancellation.0];
            let parent_claim = parent_slot.claim.ok_or(WorkError::NotReady)?;
            let parent = &ledger.claims[&parent_claim];
            if parent.pending.is_some()
                || !root_domains::has_effective_domains(&state, &parent.active_facts, true)
                || parent.amounts != parent.active_amounts
                || parent.facts.key != parent.active_facts.key
            {
                return Err(WorkError::Conflict);
            }
            let slot = state
                .cancellations
                .free_head
                .ok_or(WorkError::Capacity("fixed control slots"))?;
            let cell = &state.cancellations.slots[slot.0];
            let claim = cell.claim.ok_or(WorkError::NotReady)?;
            let old = &ledger.claims[&claim];
            if cell.phase != SlotPhase::Free
                || cell.parent.is_some()
                || old.owner != ClaimOwner::ControlSlot(slot)
                || old.pending.is_some()
                || old.amounts != cell.base
                || old.active_amounts != cell.base
                || !old.facts.dependencies.is_empty()
                || !old.active_facts.dependencies.is_empty()
                || !old.active_facts.key.source_kind.is_empty()
            {
                return Err(WorkError::Conflict);
            }
            cell.generation
                .checked_add(2)
                .ok_or(WorkError::ArithmeticOverflow)?;
            old.structure_revision
                .checked_add(2)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let parent_fence = ParentControlFence {
                work: scope.id,
                slot: node.cancellation,
                generation: parent_slot.generation,
                claim: parent_claim,
                structure_revision: parent.structure_revision,
                release_revision: parent.release_revision,
            };
            let source = JointClaimView {
                inner: Arc::downgrade(&self.inner),
                id: claim,
                source: ClaimSource::PoolFree {
                    slot,
                    generation: cell.generation,
                    parent: parent_fence,
                    deadline: request.deadline,
                },
                structure_revision: old.structure_revision,
                release_revision: old.release_revision,
            };
            authenticate(old, &source)?;
            authenticate_parent(&state, parent_fence, scope.id)?;
            // Capture every fallible fence and immutable fact before taking
            // the resident position off the free chain. The paid constructor
            // is the sole rollback owner until the original ticket is queued.
            let result = (
                source,
                Arc::clone(&old.active_facts),
                cell.base,
                Arc::clone(&parent.active_facts),
            );
            let custody = custody.as_mut().ok_or(WorkError::NotReady)?;
            if custody.reserved_child.is_some() {
                return Err(WorkError::Conflict);
            }
            let next = state.cancellations.slots[slot.0].next_free.take();
            state.cancellations.free_head = next;
            state.cancellations.slots[slot.0].class = state.nodes[&scope.id].class;
            state.cancellations.slots[slot.0].phase = SlotPhase::ChildReserved { parent: scope.id };
            custody.reserved_child = Some(slot);
            result
        };
        // No cell Arc escapes: only the existing immutable profile facts do.
        let change = ChangeRecord {
            old: Some(source.id),
            original_amounts: Some(base),
            original_facts: Some(from),
            source: Some(source),
            claim: ProposedClaim {
                facts: to,
                amounts: base,
            },
        };
        Ok(PreparedRequest {
            root_domain: None,
            root_stage: None,
            computation_window: None,
            root_payload: None,
            cpu_action: None,
            control_action: None,
            native_action: None,
            scope: scope.clone(),
            changes: self.store_proposal(&custody, vec![change])?,
            custody,
            _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
        })
    }

    /// Queue New claims and/or growth of exact stable holders. No physical
    /// holder moves into this future. Growth authority starts only at install.
    #[cfg(test)]
    pub fn request(
        &self,
        scope: &WorkScope,
        changes: Vec<JointClaimChange>,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        self.prepare_request(scope, &changes)
            .and_then(|prepared| self.queue_request(prepared))
            .map_err(|error| JointAdmissionFailure { error })
    }

    /// Capture only immutable headers under State. Dependency union, sorting
    /// and record allocation occur outside the original authority's mutex.
    #[cfg(test)]
    pub(super) fn prepare_request(
        &self,
        scope: &WorkScope,
        changes: &[JointClaimChange],
    ) -> Result<PreparedRequest, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let custody = self.reserve_proposal(scope)?;
        let class = {
            let state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if changes.is_empty() {
                return Err(WorkError::Capacity("joint batch records"));
            }
            node.class
        };
        let mut records: Vec<ChangeRecord> = Vec::with_capacity(changes.len());
        for change in changes {
            let adjustment = match change {
                JointClaimChange::Grow {
                    source,
                    delta,
                    dependencies,
                } => Some((source, *delta, dependencies, false)),
                JointClaimChange::FirstBind {
                    source,
                    dependencies,
                } => Some((source, JointResources::ZERO, dependencies, true)),
                JointClaimChange::New { .. } => None,
            };
            let (source, original, lane, amounts, mut selected, additional) =
                if let Some((source, delta, dependencies, first_bind)) = adjustment {
                    if first_bind && dependencies.is_empty() {
                        return Err(WorkError::Conflict);
                    }
                    let (facts, old_amounts) =
                        self.capture_adjustment(scope, source, delta, first_bind)?;
                    if records.iter().any(|record| record.old == Some(source.id)) {
                        return Err(WorkError::Conflict);
                    }
                    let selected = facts.dependencies.to_vec();
                    (
                        Some(source.clone()),
                        Some((Arc::clone(&facts), old_amounts)),
                        facts.lane,
                        old_amounts.checked_add(delta)?,
                        selected,
                        dependencies,
                    )
                } else {
                    let JointClaimChange::New {
                        dependencies,
                        lane,
                        amounts,
                    } = change
                    else {
                        unreachable!("adjustment handled above")
                    };
                    (None, None, *lane, *amounts, Vec::new(), dependencies)
                };
            for dependency in additional {
                if !Arc::ptr_eq(&self.inner, &dependency.inner) {
                    return Err(WorkError::ForeignAuthority);
                }
                selected.push(dependency.record.clone());
            }
            selected.sort_unstable_by_key(|record| record.ordinal);
            if source.is_none() && selected.windows(2).any(|p| p[0].ordinal == p[1].ordinal) {
                return Err(WorkError::Conflict);
            }
            selected.dedup_by_key(|record| record.ordinal);
            let facts = Arc::new(ResponsibilityFacts {
                key: Arc::new(FactKey {
                    origin: GroupOrigin::Frozen,
                    class: class_index(class),
                    lane: lane as usize,
                    dependencies: selected.iter().map(|d| d.ordinal).collect(),
                    source_kind: SourceKind::Plain,
                }),
                class,
                lane,
                dependencies: Arc::new(selected),
            });
            records.push(ChangeRecord {
                old: source.as_ref().map(|source| source.id),
                original_amounts: original.as_ref().map(|(_, amounts)| *amounts),
                original_facts: original.map(|(facts, _)| facts),
                source,
                claim: ProposedClaim { facts, amounts },
            });
        }
        Ok(PreparedRequest {
            root_domain: None,
            root_stage: None,
            computation_window: None,
            root_payload: None,
            cpu_action: None,
            control_action: None,
            native_action: None,
            scope: scope.clone(),
            changes: self.store_proposal(&custody, records)?,
            custody,
            _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner)),
        })
    }

    /// Reauthenticate every source after the lock-free construction phase.
    /// Only this transaction sets pending and publishes the original reply.
    pub(super) fn queue_request(
        &self,
        prepared: PreparedRequest,
    ) -> Result<JointAdmission, WorkError> {
        self.queue_request_mode(prepared, false)
    }

    fn queue_request_mode(
        &self,
        mut prepared: PreparedRequest,
        control_only: bool,
    ) -> Result<JointAdmission, WorkError> {
        if !Arc::ptr_eq(&self.inner, &prepared.scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let scope = &prepared.scope;
        let records = &prepared.changes;
        let mut admission = JointAdmission {
            inner: Arc::clone(&self.inner),
            id: None,
        };
        self.inner.update(|state| {
            if let Some(change) = &prepared.root_domain {
                root_domains::validate_construction(
                    state,
                    change.root,
                    prepared.custody.as_ref().and_then(|c| c.key),
                )?;
                root_domains::validate_change(state, change, None)?;
            }
            validate_control_productions(state, records)?;
            if control_only {
                validate_control_binding_shape(state, scope, records)?;
            }
            if let Some(fence) = prepared.native_action.as_ref() {
                validate_native_action(state, scope.id, fence, records)?;
            }
            if let Some(fence) = prepared.control_action {
                validate_control_action(state, scope.id, fence, records)?;
            }
            if let Some(fence) = prepared.cpu_action {
                validate_cpu_action(state, scope.id, fence, records)?;
            } else if records.iter().any(|change| {
                !change.claim.facts.key.source_kind.is_empty()
                    || (state.internal_cpu_factory.is_some()
                        && change.claim.amounts.get(JointResource::InternalCpuJobs) != 0)
            }) {
                // Only the authenticated CPU action can publish this causal
                // exemption. A registered WC also forbids E CPU rows from
                // other constructors; backing custody is not source authority.
                return Err(WorkError::Conflict);
            }
            if let Some(fence) = prepared.root_payload {
                validate_root_payload(state, scope.id, fence, records)?;
            }
            if let Some(fence) = prepared.root_stage {
                validate_root_stage(state, scope.id, fence, records)?;
                state.nodes[&fence.stage]
                    .resource_waiters
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            for change in records.iter() {
                if let GroupOrigin::Inherited(root) = change.claim.facts.key.origin
                    && (node.root != root.work
                        || state
                            .nodes
                            .get(&root.work)
                            .and_then(|n| n.root_domains.as_ref())
                            .is_none()
                        || root_domains::origin(state, root.work)? != change.claim.facts.key.origin)
                {
                    return Err(WorkError::Conflict);
                }
                if change.claim.facts.class != node.class {
                    return Err(WorkError::Conflict);
                }
                if let Some(source) = &change.source {
                    let old = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                    authenticate_scope(
                        state,
                        old,
                        source,
                        source_work(scope.id, prepared.root_stage, source),
                    )?;
                    if old.pending.is_some() {
                        return Err(WorkError::Conflict);
                    }
                }
            }
            if requires_business_active(
                records,
                prepared.native_action.as_ref(),
                prepared.root_payload,
            ) {
                node.check(&state.cancellations)?;
                if state.closed {
                    return Err(WorkError::Closed);
                }
            }
            let waiters = node
                .resource_waiters
                .checked_add(usize::from(prepared.custody.is_none()))
                .ok_or(WorkError::ArithmeticOverflow)?;
            let id = state.next_id()?;
            let proposal = queue_proposal(state, &prepared.custody, id)?;
            if let Some(change) = &prepared.root_domain {
                root_domains::queue(state, change, id);
            }
            state.nodes.get_mut(&scope.id).unwrap().resource_waiters = waiters;
            if let Some(fence) = prepared.root_stage {
                state.nodes.get_mut(&fence.stage).unwrap().resource_waiters += 1;
            }
            let ledger = state.joint.as_mut().unwrap();
            for record in records.iter() {
                if let Some(old) = record.old
                    && !(prepared.root_domain.is_some() && root_domains::observation(record))
                {
                    ledger.claims.get_mut(&old).unwrap().pending = Some(id);
                }
            }
            ledger.requests.insert(
                id,
                RequestRecord {
                    root_domain: prepared.root_domain.take(),
                    root_stage: prepared.root_stage,
                    computation_window: prepared.computation_window,
                    root_payload: prepared.root_payload,
                    cpu_action: prepared.cpu_action,
                    control_action: prepared.control_action,
                    native_action: prepared.native_action,
                    owner: scope.id,
                    changes: records.clone(),
                    source_cohort: prepared
                        .custody
                        .as_ref()
                        .is_some_and(|custody| !custody.reserved_cohort.is_empty()),
                    proposal,
                    phase: RequestPhase::Waiting,
                    local_blocked: false,
                    waker: None,
                },
            );
            admission.id = Some(id);
            if let Some(custody) = &mut prepared.custody {
                if let Some(slot) = custody.reserved_child.take() {
                    assert_eq!(
                        state.cancellations.slots[slot.0].phase,
                        SlotPhase::ChildReserved { parent: scope.id }
                    );
                    state.cancellations.slots[slot.0].phase = SlotPhase::ChildQueued {
                        parent: scope.id,
                        ticket: id,
                    };
                }
                for slot in custody.reserved_cohort.drain(..) {
                    assert_eq!(
                        state.cancellations.slots[slot.0].phase,
                        SlotPhase::ChildReserved { parent: scope.id }
                    );
                    state.cancellations.slots[slot.0].phase = SlotPhase::ChildQueued {
                        parent: scope.id,
                        ticket: id,
                    };
                }
                custody.key = None;
            }
            Ok(())
        })?;
        Ok(admission)
    }
}

pub(super) struct PreparedRequest {
    root_domain: Option<RootDomainChange>,
    root_stage: Option<RootStageFence>,
    computation_window: Option<crate::result_window::ClientComputationWindow>,
    root_payload: Option<RootPayloadFence>,
    cpu_action: Option<CpuActionFence>,
    control_action: Option<ControlActionFence>,
    native_action: Option<NativeActionFence>,
    scope: WorkScope,
    changes: ChangeBody,
    custody: Option<ProposalConstruction>,
    _exit: crate::scope::ReclaimWakeOnExit,
}

#[cfg(test)]
pub enum JointClaimChange {
    /// Bind the complete resident control base to its first nonempty domain set.
    /// The source comes only from the original WorkOwner; no new guard is minted.
    FirstBind {
        source: JointClaimView,
        dependencies: Vec<JointDependency>,
    },
    New {
        dependencies: Vec<JointDependency>,
        lane: JointLane,
        amounts: JointResources,
    },
    Grow {
        source: JointClaimView,
        delta: JointResources,
        dependencies: Vec<JointDependency>,
    },
}
#[cfg(test)]
impl JointClaimChange {
    pub fn grow_by(
        source: JointClaimView,
        delta: JointResources,
        dependencies: Vec<JointDependency>,
    ) -> Self {
        Self::Grow {
            source,
            delta,
            dependencies,
        }
    }
}
fn source_work(root: WorkId, fence: Option<RootStageFence>, source: &JointClaimView) -> WorkId {
    match (fence, source.source) {
        (
            Some(fence),
            ClaimSource::NodeControl { work, .. } | ClaimSource::Production { work, .. },
        ) if work == fence.stage => fence.stage,
        (Some(fence), ClaimSource::Computation { epoch })
            if fence.computation == Some([source.id, epoch]) =>
        {
            fence.stage
        }
        _ => root,
    }
}
fn authenticate(record: &ClaimRecord, source: &JointClaimView) -> Result<(), WorkError> {
    let custody = match (source.source, record.custody) {
        (ClaimSource::Production { epoch, .. }, ClaimCustody::Production { epoch: original }) => {
            epoch == original && record.production.is_some()
        }
        (ClaimSource::Production { .. }, _) => false,
        (
            ClaimSource::Payload { epoch },
            ClaimCustody::Payload {
                epoch: original,
                pins,
            },
        ) => epoch == original && pins != 0,
        (ClaimSource::Payload { .. }, _) => false,
        (
            ClaimSource::Computation { epoch },
            ClaimCustody::Computation {
                epoch: original,
                pins,
            },
        ) => epoch == original && pins != 0,
        (ClaimSource::Computation { .. }, _) => false,
        (_, ClaimCustody::Raw) => true,
        _ => false,
    };
    if !custody
        || !record.active_facts.key.source_kind.is_empty()
        || record.structure_revision != source.structure_revision
        || record.release_revision != source.release_revision
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}
fn authenticate_parent(
    state: &State,
    fence: ParentControlFence,
    work: WorkId,
) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    let node = state.nodes.get(&work).ok_or(WorkError::Released)?;
    node.check(&state.cancellations)?;
    if node.owner != crate::OwnerState::Active {
        return Err(WorkError::Conflict);
    }
    let slot = state
        .cancellations
        .slots
        .get(fence.slot.0)
        .ok_or(WorkError::Released)?;
    let claim = ledger.claims.get(&fence.claim).ok_or(WorkError::Released)?;
    if work != fence.work
        || node.cancellation != fence.slot
        || slot.phase != SlotPhase::Node(work)
        || slot.generation != fence.generation
        || slot.claim != Some(fence.claim)
        || claim.owner != ClaimOwner::ControlSlot(fence.slot)
        || claim.structure_revision != fence.structure_revision
        || claim.release_revision != fence.release_revision
        || claim.pending.is_some()
        || !root_domains::has_effective_domains(state, &claim.active_facts, true)
        || claim.active_amounts != claim.amounts
        || claim.active_facts.key != claim.facts.key
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

fn authenticate_scope(
    state: &State,
    record: &ClaimRecord,
    source: &JointClaimView,
    work: WorkId,
) -> Result<(), WorkError> {
    authenticate(record, source)?;
    match source.source {
        ClaimSource::Physical | ClaimSource::Payload { .. } | ClaimSource::Computation { .. }
            if record.owner == ClaimOwner::PhysicalWork(work) =>
        {
            Ok(())
        }
        ClaimSource::Production { work: original, .. }
            if original == work && record.owner == ClaimOwner::PhysicalWork(work) =>
        {
            Ok(())
        }
        ClaimSource::NodeControl {
            work: original,
            slot,
            generation,
        } => {
            let node = state.nodes.get(&work).ok_or(WorkError::Released)?;
            let cell = state
                .cancellations
                .slots
                .get(slot.0)
                .ok_or(WorkError::Released)?;
            if original != work
                || !state.cancellations.fixed
                || node.cancellation != slot
                || cell.phase != SlotPhase::Node(work)
                || cell.generation != generation
                || cell.claim != Some(source.id)
                || record.owner != ClaimOwner::ControlSlot(slot)
            {
                return Err(WorkError::Conflict);
            }
            Ok(())
        }
        ClaimSource::PoolFree {
            slot,
            generation,
            parent,
            ..
        } => {
            authenticate_parent(state, parent, work)?;
            let cell = state
                .cancellations
                .slots
                .get(slot.0)
                .ok_or(WorkError::Released)?;
            if !state.cancellations.fixed
                || !matches!(cell.phase,
                    SlotPhase::ChildReserved { parent: owner }
                        | SlotPhase::ChildQueued { parent: owner, .. } if owner == work)
                || cell.generation != generation
                || cell.claim != Some(source.id)
                || cell.parent.is_some()
                || record.owner != ClaimOwner::ControlSlot(slot)
                || record.amounts != cell.base
                || record.active_amounts != cell.base
                || !record.facts.dependencies.is_empty()
                || !record.active_facts.dependencies.is_empty()
                || cell.next_free.is_some()
            {
                return Err(WorkError::Conflict);
            }
            Ok(())
        }
        _ => Err(WorkError::Conflict),
    }
}

pub(super) fn validate_waiting(
    state: &State,
    id: u64,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    if let Some(change) = state
        .joint
        .as_ref()
        .and_then(|l| l.requests.get(&id))
        .and_then(|r| r.root_domain.as_ref())
    {
        root_domains::validate_change(state, change, Some(id))?;
    }

    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
    let work = request.owner;
    validate_control_productions(state, changes)?;
    if let Some(fence) = request.native_action.as_ref() {
        validate_native_action(state, work, fence, changes)?;
    }
    if let Some(fence) = request.control_action {
        validate_control_action(state, work, fence, changes)?;
    }
    if let Some(fence) = request.cpu_action {
        validate_cpu_action(state, work, fence, changes)?;
    }
    if let Some(fence) = request.root_payload {
        validate_root_payload(state, work, fence, changes)?;
    }
    if let Some(fence) = request.root_stage {
        validate_root_stage(state, work, fence, changes)?;
    }
    for change in changes {
        if let Some(source) = &change.source {
            let record = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
            authenticate_scope(
                state,
                record,
                source,
                source_work(work, request.root_stage, source),
            )?;
            let expected = if request.root_domain.is_some() && root_domains::observation(change) {
                None
            } else {
                Some(id)
            };
            if record.pending != expected {
                return Err(WorkError::Conflict);
            }
        }
    }
    Ok(())
}

impl WorkScope {
    /// Request one same-class child with the parent's complete admitted
    /// dependency set. This uses the original fixed proposal and proof driver;
    /// it grants neither an I/O stage nor physical execution capacity.
    /// The original parent owner must remain Active through installation.
    pub fn request_joint_child(&self) -> Result<JointAdmission, JointAdmissionFailure> {
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&self.inner),
        };
        handle
            .prepare_child(self, None)
            .and_then(|prepared| handle.queue_request(prepared))
            .map_err(|error| JointAdmissionFailure { error })
    }
    /// Same child constructor and one finite proof. Full is only the original
    /// unacquired proposal slot; every other refusal retains its typed fact.
    pub fn try_request_joint_child(
        &self,
    ) -> Result<JointTryBegin<JointAdmission, JointProposalSlotSignal>, WorkError> {
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&self.inner),
        };
        match handle.try_reserve_proposal(self)? {
            ProposalReservation::Full(target) => Ok(JointTryBegin::Full(
                JointProposalSlotSignal::new(Arc::downgrade(&self.inner), target),
            )),
            ProposalReservation::Reserved(custody) => {
                let prepared = handle.prepare_child_with_custody(self, None, custody)?;
                handle.queue_request(prepared).map(JointTryBegin::Started)
            }
        }
    }
}

impl WorkOwner {
    /// Observe this exact Node's prepaid control claim. This view cannot keep
    /// the Node alive, complete it, or release its resident storage.
    pub fn control_claim_view(&self) -> Result<JointClaimView, WorkError> {
        control_claim_view_for_scope(&self.scope(), false).map(|(view, _)| view)
    }
}

// The owner path preserves its original observation semantics. Only the
// delegated control constructor additionally requires a live Active owner.
fn control_claim_view_for_scope(
    scope: &WorkScope,
    active: bool,
) -> Result<(JointClaimView, bool), WorkError> {
    let state = scope.inner.state.lock().unwrap();
    if !state.cancellations.fixed {
        return Err(WorkError::NotReady);
    }
    let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
    let slot = &state.cancellations.slots[node.cancellation.0];
    if slot.phase != SlotPhase::Node(scope.id) {
        return Err(WorkError::Conflict);
    }
    let id = slot.claim.ok_or(WorkError::NotReady)?;
    let record = state
        .joint
        .as_ref()
        .and_then(|ledger| ledger.claims.get(&id))
        .ok_or(WorkError::Released)?;
    if active {
        validate_active_control_node(&state, scope)?;
        if record.pending.is_some()
            || record.amounts != slot.base
            || record.active_amounts != slot.base
            || record.facts.key != record.active_facts.key
        {
            return Err(WorkError::Conflict);
        }
    }
    let unbound = record.active_facts.dependencies.is_empty();
    Ok((
        JointClaimView {
            inner: Arc::downgrade(&scope.inner),
            id,
            source: ClaimSource::NodeControl {
                work: scope.id,
                slot: node.cancellation,
                generation: slot.generation,
            },
            structure_revision: record.structure_revision,
            release_revision: record.release_revision,
        },
        unbound,
    ))
}

/// Physical custody remains here throughout any queued/granted adjustment.
/// Logical completion and adjustment cancellation must not drop this holder.
pub struct JointClaim {
    inner: Arc<Inner>,
    id: Option<u64>,
}
impl std::fmt::Debug for JointClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JointClaim")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
impl JointClaim {
    #[cfg(test)]
    pub(super) fn active_dependency_count(&self) -> usize {
        let state = self.inner.state.lock().unwrap();
        state.joint.as_ref().unwrap().claims[&self.id.unwrap()]
            .active_facts
            .dependencies
            .len()
    }
    pub fn view(&self) -> Result<JointClaimView, WorkError> {
        let id = self.id.ok_or(WorkError::Released)?;
        let state = self.inner.state.lock().unwrap();
        let record = state
            .joint
            .as_ref()
            .and_then(|l| l.claims.get(&id))
            .ok_or(WorkError::Released)?;
        if !record.custody.is_raw()
            || !record.active_facts.key.source_kind.is_empty()
            || !matches!(record.owner, ClaimOwner::PhysicalWork(_))
        {
            return Err(WorkError::Conflict);
        }
        Ok(JointClaimView {
            inner: Arc::downgrade(&self.inner),
            id,
            source: ClaimSource::Physical,
            structure_revision: record.structure_revision,
            release_revision: record.release_revision,
        })
    }
    /// Installed responsibility quantities only. Actual execution/allocation
    /// still requires the original executor, transport or result owner permit.
    pub fn amounts(&self) -> Result<JointResources, WorkError> {
        let id = self.id.ok_or(WorkError::Released)?;
        let state = self.inner.state.lock().unwrap();
        state
            .joint
            .as_ref()
            .and_then(|l| l.claims.get(&id))
            .ok_or(WorkError::Released)
            .and_then(|record| {
                if record.custody.is_raw() && matches!(record.owner, ClaimOwner::PhysicalWork(_)) {
                    Ok(record.active_amounts)
                } else {
                    Err(WorkError::Conflict)
                }
            })
    }
    /// Report an actual partial release. Granted delta remains separate; a
    /// queued/proving source becomes stale and receives typed Conflict.
    pub fn release_part(&mut self, released: JointResources) -> Result<(), WorkError> {
        let id = self.id.ok_or(WorkError::Released)?;
        self.inner.update(|state| {
            let ledger = state.joint.as_mut().ok_or(WorkError::NotReady)?;
            let record = ledger.claims.get_mut(&id).ok_or(WorkError::Released)?;
            if !record.custody.is_raw()
                || !record.active_facts.key.source_kind.is_empty()
                || !matches!(record.owner, ClaimOwner::PhysicalWork(_))
            {
                return Err(WorkError::Conflict);
            }
            let active = record.active_amounts.checked_sub(released)?;
            let charged = record.amounts.checked_sub(released)?;
            record.active_amounts = active;
            record.amounts = charged;
            match record.release_revision.checked_add(1) {
                Some(revision) => record.release_revision = revision,
                None => ledger.driver_closed = true,
            }
            ledger.released();
            Ok(())
        })
    }
}
impl Drop for JointClaim {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        let waker = self.inner.update(|state| release_current(state, id));
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
fn remove_claim(state: &mut State, id: u64) {
    let Some(record) = state.joint.as_mut().unwrap().claims.remove(&id) else {
        return;
    };
    let ledger = state.joint.as_mut().unwrap();
    ledger.released();
    let owner = match record.owner {
        ClaimOwner::PhysicalWork(owner) => owner,
        #[cfg(test)]
        ClaimOwner::OwnerRetained(owner) => owner,
        ClaimOwner::ControlSlot(_) => {
            unreachable!("fixed control claims are released only with their process inventory")
        }
    };
    if record.internal_position {
        release_internal_position(state, owner);
    }
    if record.amounts.get(JointResource::Computations) != 0
        && let Some(window) = &record.computation_window
    {
        window.release(state, id);
    }
    state.nodes.get_mut(&owner).unwrap().resource_holders -= 1;
    state.collect(owner);
}
/// Only actual holder Drop or withdrawal of a never-delivered New invokes this.
fn release_current(state: &mut State, id: u64) -> Option<Waker> {
    if !state
        .joint
        .as_ref()
        .and_then(|ledger| ledger.claims.get(&id))
        .is_some_and(|record| {
            record.custody.can_release_current()
                && matches!(record.owner, ClaimOwner::PhysicalWork(_))
        })
    {
        return None;
    }
    let pending = state
        .joint
        .as_ref()
        .and_then(|l| l.claims.get(&id))
        .and_then(|r| r.pending);
    let waker = pending.and_then(|ticket| fail_request(state, ticket, WorkError::Released));
    remove_claim(state, id);
    waker
}
fn remove_request(state: &mut State, id: u64) {
    if let Some(request) = state.joint.as_mut().unwrap().requests.remove(&id) {
        if request.local_blocked {
            state.joint.as_mut().unwrap().local_waiters -= 1;
        }
        for change in request.changes.iter() {
            if let Some(source) = &change.source
                && let ClaimSource::PoolFree { slot, parent, .. } = source.source
            {
                if request.source_cohort
                    && matches!(state.cancellations.slots[slot.0].phase,
                        SlotPhase::ChildQueued { ticket, .. } | SlotPhase::ChildGranted { ticket, .. } if ticket == id)
                {
                    state
                        .cancellations
                        .cell(parent.slot)
                        .release_fixed_children(1);
                }
                if matches!(state.cancellations.slots[slot.0].phase,
                        SlotPhase::ChildQueued { ticket, .. } if ticket == id)
                {
                    let record = state
                        .joint
                        .as_mut()
                        .unwrap()
                        .claims
                        .get_mut(&source.id)
                        .unwrap();
                    assert_eq!(record.pending, Some(id));
                    record.pending = None;
                    state.cancellations.withdraw_child_reservation(slot);
                } else if matches!(state.cancellations.slots[slot.0].phase,
                        SlotPhase::ChildGranted { ticket, .. } if ticket == id)
                {
                    let record = state
                        .joint
                        .as_mut()
                        .unwrap()
                        .claims
                        .get_mut(&source.id)
                        .unwrap();
                    assert_eq!(record.pending, Some(id));
                    record.pending = None;
                    state.cancellations.retire(slot);
                }
            }
        }
        if let Some(key) = request.proposal {
            root_domains::release_construction(state, request.owner, key);
        }
        if let Some(fence) = request.root_stage {
            state.nodes.get_mut(&fence.stage).unwrap().resource_waiters -= 1;
            state.collect(fence.stage);
        }
        state
            .nodes
            .get_mut(&request.owner)
            .unwrap()
            .resource_waiters -= 1;
        state.collect(request.owner);
    }
}
/// Shared cancellation path. The request retains its typed failure until its
/// future/receipt is consumed. It never restores a snapshot of an old amount.
pub(super) fn fail_request(state: &mut State, id: u64, error: WorkError) -> Option<Waker> {
    let ledger = state.joint.as_mut().unwrap();
    let request = ledger.requests.get_mut(&id)?;
    if let RequestPhase::LocalOffered { failure, .. } = &mut request.phase {
        // An external physical try may already own original resources. Record
        // cancellation, but retain the same ticket/Q/new_ids until real exit.
        if failure.is_none() {
            *failure = Some(error);
        }
        return request.waker.take();
    }
    if matches!(request.phase, RequestPhase::Failed(_)) {
        return request.waker.take();
    }
    if request.local_blocked {
        request.local_blocked = false;
        ledger.local_waiters -= 1;
    }
    let phase = std::mem::replace(&mut request.phase, RequestPhase::Failed(error));
    let changes = request.changes.clone();
    let source_cohort = request.source_cohort;
    let root_domain = request.root_domain.clone();
    let waker = request.waker.take();
    let charged = matches!(
        phase,
        RequestPhase::Granted { .. } | RequestPhase::Delivered
    );
    if root_domain.is_some() && charged {
        // Unlike ordinary Delivered New, these raw grants are sealed inside
        // RootDomainInitialAdmission/RootStageReady and cannot authorize a
        // physical effect. Withdraw them at the same cut BEFORE changing D.
        let mut after = std::ops::Bound::Unbounded;
        loop {
            let next = state
                .joint
                .as_ref()
                .unwrap()
                .claims
                .range((after, std::ops::Bound::Unbounded))
                .find_map(|(&claim, row)| (row.domain_birth == Some(id)).then_some(claim));
            let Some(claim) = next else {
                break;
            };
            after = std::ops::Bound::Excluded(claim);
            assert!(
                state.joint.as_ref().unwrap().claims[&claim]
                    .custody
                    .is_raw()
            );
            remove_claim(state, claim);
        }
    }
    let ledger = state.joint.as_mut().unwrap();
    for change in changes.iter() {
        if change
            .source
            .as_ref()
            .is_some_and(|source| matches!(source.source, ClaimSource::PoolFree { .. }))
        {
            if let Some(JointClaimView {
                source: ClaimSource::PoolFree { slot, parent, .. },
                id: claim,
                ..
            }) = change.source.as_ref()
                && matches!(state.cancellations.slots[slot.0].phase,
                    SlotPhase::ChildQueued { ticket, .. } if ticket == id)
            {
                if source_cohort {
                    state
                        .cancellations
                        .cell(parent.slot)
                        .release_fixed_children(1);
                }
                // A refused proof installed no domain grant or observer.
                // Return only its original uncommitted resident position.
                let record = ledger.claims.get_mut(claim).unwrap();
                assert_eq!(record.pending, Some(id));
                record.pending = None;
                state.cancellations.withdraw_child_reservation(*slot);
            }
            // A ChildGranted reply has actual committed domain liability;
            // retain that fee and pending ticket until its original reply exits.
            continue;
        }
        let Some(old) = change.old else {
            continue;
        };
        let Some(record) = ledger.claims.get_mut(&old) else {
            continue;
        };
        if record.pending != Some(id) {
            continue;
        }
        record.pending = None;
        if charged {
            let from = Arc::clone(&record.facts);
            let to = Arc::clone(&record.active_facts);
            let active = record.active_amounts;
            record.facts = Arc::clone(&to);
            record.amounts = active;
            match record.structure_revision.checked_add(1) {
                Some(revision) => record.structure_revision = revision,
                None => ledger.driver_closed = true,
            }
            if from.key != to.key {
                ledger.layout_changed();
            }
            ledger.released();
        }
    }
    if let Some(change) = &root_domain {
        // The active graph and the same original request retain both sides.
        // No detached snapshot amount is ever restored.
        let _retired = root_domains::rollback(state, change, id);
    }
    if let RequestPhase::Granted { new_ids } = phase {
        for id in new_ids {
            remove_claim(state, id);
        }
    }
    waker
}
fn withdraw_request(state: &mut State, id: u64) -> Option<Waker> {
    let waker = fail_request(state, id, WorkError::Released);
    if !state
        .joint
        .as_ref()
        .unwrap()
        .requests
        .get(&id)
        .is_some_and(|request| matches!(request.phase, RequestPhase::LocalOffered { .. }))
    {
        remove_request(state, id);
    }
    waker
}

/// Actual result positions belong to the original ResultCapacity author.
/// Waiting for another producer is backpressure, not a responsibility failure.
pub(super) fn request_compute_room(state: &State, id: u64) -> Result<bool, WorkError> {
    let request = state
        .joint
        .as_ref()
        .ok_or(WorkError::NotReady)?
        .requests
        .get(&id)
        .ok_or(WorkError::Released)?;
    if !request
        .root_stage
        .is_some_and(|fence| fence.computation.is_none())
    {
        return Ok(true);
    }
    if let Some(window) = &request.computation_window {
        return window.room();
    }
    let config = state.result_capacity.ok_or(WorkError::NotReady)?;
    Ok(state.result_windows.held_positions[2] < config.positions[2])
}
fn charge_internal_position(state: &mut State, owner: WorkId) -> Result<(), WorkError> {
    let config = state.result_capacity.ok_or(WorkError::NotReady)?;
    if state.result_windows.held_positions[2] >= config.positions[2] {
        return Err(WorkError::Capacity("Internal computation positions"));
    }
    state
        .nodes
        .get_mut(&owner)
        .ok_or(WorkError::Released)?
        .result_windows
        .held_positions[2] += 1;
    state.result_windows.held_positions[2] += 1;
    Ok(())
}
fn release_internal_position(state: &mut State, owner: WorkId) {
    state
        .nodes
        .get_mut(&owner)
        .expect("original compute owner")
        .result_windows
        .held_positions[2] -= 1;
    state.result_windows.held_positions[2] -= 1;
}

/// Called only by the unique responsibility driver after exact source validation.
/// This transfers recorded quantities, not physical or allocation permission.
pub(super) fn grant_request(state: &mut State, id: u64) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().unwrap();
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
    if !matches!(request.phase, RequestPhase::Waiting) {
        return Err(WorkError::Conflict);
    }
    validate_waiting(state, id, &request.changes)?;
    if !request_compute_room(state, id)? {
        return Err(WorkError::Capacity("Internal computation positions"));
    }
    let internal_compute = request
        .root_stage
        .is_some_and(|fence| fence.computation.is_none())
        && request.computation_window.is_none();
    let computation_window = request.computation_window.clone();
    let changes = request.changes.clone();
    let owner = request.owner;
    let root_domain = request.root_domain.clone();
    let physical_owner = request.root_stage.map_or(owner, |fence| fence.stage);
    let native_action = request.native_action.clone();
    let new_count = changes.iter().filter(|c| c.old.is_none()).count();
    for (index, change) in changes.iter().enumerate() {
        if change.old.is_none() {
            let holder = native_action
                .as_ref()
                .map_or(physical_owner, |fence| fence.physical_owner(index));
            state
                .nodes
                .get(&holder)
                .ok_or(WorkError::Released)?
                .resource_holders
                .checked_add(new_count)
                .ok_or(WorkError::ArithmeticOverflow)?;
        }
    }
    state
        .next_id
        .checked_add(new_count as u64)
        .ok_or(WorkError::ArithmeticOverflow)?;
    ledger
        .facts_revision
        .checked_add(changes.len() as u64)
        .and_then(|n| n.checked_add(u64::from(root_domain.is_some())))
        .ok_or(WorkError::ArithmeticOverflow)?;
    for change in changes.iter() {
        if let Some(source) = &change.source
            && let ClaimSource::PoolFree { slot, .. } = source.source
        {
            let cell = &mut state.cancellations.slots[slot.0];
            cell.generation
                .checked_add(2)
                .ok_or(WorkError::ArithmeticOverflow)?;
            Arc::get_mut(cell.cell.as_mut().unwrap()).ok_or(WorkError::Conflict)?;
        }
    }
    let mut new_ids = Vec::with_capacity(new_count);
    for (index, change) in changes.iter().enumerate() {
        let physical_owner = native_action
            .as_ref()
            .map_or(physical_owner, |fence| fence.physical_owner(index));
        if let Some(old) = change.old {
            if root_domain.is_some() && root_domains::observation(change) {
                continue;
            }
            let ledger = state.joint.as_mut().unwrap();
            let record = ledger.claims.get_mut(&old).unwrap();
            let rekey = record.facts.key != change.claim.facts.key;
            record.facts = Arc::clone(&change.claim.facts);
            record.amounts = change.claim.amounts;
            record.structure_revision += 1; // Reserved by request preflight.
            if let Some(source) = &change.source
                && let ClaimSource::PoolFree { slot, parent, .. } = source.source
            {
                record.active_facts = Arc::clone(&record.facts);
                record.active_amounts = record.amounts;
                record.pending = Some(id);
                let cell = &mut state.cancellations.slots[slot.0];
                assert_eq!(
                    cell.phase,
                    SlotPhase::ChildQueued {
                        parent: parent.work,
                        ticket: id,
                    }
                );
                cell.generation += 1;
                cell.phase = SlotPhase::ChildGranted {
                    parent: parent.work,
                    ticket: id,
                };
            }
            if rekey {
                ledger.layout_changed();
            }
        } else {
            let new_id = state.next_id().expect("prechecked claim ids");
            state.joint.as_mut().unwrap().claims.insert(
                new_id,
                ClaimRecord {
                    internal_position: internal_compute
                        && change.claim.amounts.get(JointResource::Computations) != 0,
                    computation_window: (change.claim.amounts.get(JointResource::Computations)
                        != 0)
                        .then(|| computation_window.clone())
                        .flatten(),
                    domain_birth: root_domain.as_ref().map(|_| id),
                    native_dispatch: native_action
                        .as_ref()
                        .map(|fence| fence.dispatch_origin(id, index)),
                    dispatch_staging: None,
                    custody: ClaimCustody::Raw,
                    production: None,
                    owner: ClaimOwner::PhysicalWork(physical_owner),
                    facts: Arc::clone(&change.claim.facts),
                    amounts: change.claim.amounts,
                    active_facts: Arc::clone(&change.claim.facts),
                    active_amounts: change.claim.amounts,
                    pending: None,
                    structure_revision: 0,
                    release_revision: 0,
                },
            );
            if change.claim.amounts.get(JointResource::Computations) != 0 {
                if internal_compute {
                    charge_internal_position(state, physical_owner)
                        .expect("original position and owner prechecked before mutation");
                }
                if let Some(window) = &computation_window {
                    window.acquire(new_id);
                }
            }
            state
                .nodes
                .get_mut(&physical_owner)
                .unwrap()
                .resource_holders += 1;
            new_ids.push(new_id);
        }
    }
    let ledger = state.joint.as_mut().unwrap();
    for change in changes.iter() {
        for dependency in change.claim.facts.dependencies.iter() {
            ledger
                .dependencies
                .as_mut()
                .unwrap()
                .mark_published(dependency);
        }
    }
    if let Some(change) = &root_domain {
        for dependency in change.candidate.iter() {
            ledger
                .dependencies
                .as_mut()
                .unwrap()
                .mark_published(dependency);
        }
    }
    ledger.requests.get_mut(&id).unwrap().phase = RequestPhase::Granted { new_ids };
    if let Some(change) = &root_domain {
        root_domains::charge(state, change);
    }
    Ok(())
}

#[derive(Debug)]
pub struct JointAdmissionFailure {
    pub error: WorkError,
}
#[derive(Debug)]
pub struct JointGrantedBatch {
    pub new_claims: JointGrantedClaims,
    pub adjustment: Option<JointAdjustmentReceipt>,
}
#[must_use = "Admission must be awaited or explicitly dropped"]
pub struct JointAdmission {
    inner: Arc<Inner>,
    id: Option<u64>,
}
fn requires_business_active(
    changes: &[ChangeRecord],
    native: Option<&NativeActionFence>,
    root_payload: Option<RootPayloadFence>,
) -> bool {
    root_payload.map_or_else(
        || {
            !native.is_some_and(NativeActionFence::permits_original_settlement)
                && changes
                    .iter()
                    .any(|change| change.claim.facts.lane != JointLane::Settlement)
        },
        RootPayloadFence::requires_business_active,
    )
}
impl RequestRecord {
    pub(super) fn requires_business_active(&self) -> bool {
        requires_business_active(
            &self.changes,
            self.native_action.as_ref(),
            self.root_payload,
        )
    }
}
impl Future for JointAdmission {
    type Output = Result<JointGrantedBatch, JointAdmissionFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = self.id.expect("completed joint admission polled again");
        let inner = Arc::clone(&self.inner);
        // Declared outside the transaction: every committed but unreceived ID
        // already has custody before any arbitrary wake callback can run.
        let mut delivery = ReplyDelivery {
            inner: Arc::clone(&inner),
            new_ids: Vec::new(),
            ticket: None,
            body: None,
            _exit: None,
        };
        // A pending reply only registers the original consumer. It does not
        // publish capacity progress or wake the same root's Q-slot waits.
        let mut incoming = Some(cx.waker().clone());
        let mut outgoing = None;
        let result = {
            let mut state = inner.state.lock().unwrap();
            let result = (|state: &mut State| {
                let closed = state.joint.as_ref().unwrap().driver_closed;
                let request = state
                    .joint
                    .as_mut()
                    .unwrap()
                    .requests
                    .get_mut(&id)
                    .expect("live reply");
                match &request.phase {
                    RequestPhase::Waiting => {
                        if request
                            .waker
                            .as_ref()
                            .is_none_or(|w| !w.will_wake(incoming.as_ref().unwrap()))
                        {
                            outgoing = request.waker.replace(incoming.take().unwrap());
                        }
                        return None;
                    }
                    RequestPhase::Failed(error) => {
                        let error = error.clone();
                        self.id = None;
                        remove_request(state, id);
                        return Some(Err(error));
                    }
                    RequestPhase::LocalOffered { .. } => {
                        // The original offer may still own physical resources.
                        // Do not convert its claims through the ordinary reply API.
                        return Some(Err(WorkError::Conflict));
                    }
                    RequestPhase::Delivered => unreachable!("one reply consumer"),
                    RequestPhase::Granted { .. } => {}
                }
                if closed {
                    fail_request(state, id, WorkError::Closed);
                    self.id = None;
                    remove_request(state, id);
                    return Some(Err(WorkError::Closed));
                }
                let control_action = request.control_action;
                let root_payload = request.root_payload;
                let changes = request.changes.clone();
                let owner = request.owner;
                let requires_business_active = request.requires_business_active();
                let source_valid = control_action
                    .map_or(Ok(()), |fence| {
                        validate_control_action(state, owner, fence, &changes)
                    })
                    .and_then(|()| {
                        root_payload.map_or(Ok(()), |fence| {
                            validate_root_payload(state, owner, fence, &changes)
                        })
                    });
                if let Err(error) = source_valid {
                    fail_request(state, id, error.clone());
                    self.id = None;
                    remove_request(state, id);
                    return Some(Err(error));
                }
                if requires_business_active
                    && let Err(error) = state
                        .nodes
                        .get(&owner)
                        .ok_or(WorkError::Released)
                        .and_then(|n| n.check(&state.cancellations))
                {
                    fail_request(state, id, error.clone());
                    self.id = None;
                    remove_request(state, id);
                    return Some(Err(error));
                }
                let request = state.joint.as_mut().unwrap().requests.get_mut(&id).unwrap();
                let RequestPhase::Granted { new_ids } =
                    std::mem::replace(&mut request.phase, RequestPhase::Delivered)
                else {
                    unreachable!()
                };
                let has_adjustment = request.root_domain.is_some()
                    || changes.iter().any(|change| change.old.is_some());
                // IDs are checked, monotonic and never reused in this exact
                // authority. Only this unique reply constructs physical guards.
                delivery.new_ids = new_ids;
                delivery.ticket = has_adjustment.then_some(id);
                delivery.body = Some(changes);
                delivery._exit = Some(crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(
                    &inner,
                )));
                self.id = None;
                if !has_adjustment {
                    remove_request(state, id);
                }
                Some(Ok(()))
            })(&mut state);
            if result.is_some() {
                crate::admission::dispatch(&mut state, &inner.config);
                crate::observation::fill_control(&mut state, &inner.config);
                state.advance_progress_revision();
            }
            result
        };
        drop(outgoing);
        drop(incoming);
        if result.is_some() {
            crate::admission::drain_wakes(&inner);
            inner.changed.notify_waiters();
        }
        let Some(result) = result else {
            return Poll::Pending;
        };
        if let Err(error) = result {
            return Poll::Ready(Err(JointAdmissionFailure { error }));
        }
        // A partial construction unwinds this later-declared Vec first, then
        // ReplyDelivery releases only IDs not yet held by real JointClaims.
        let mut claims = Vec::with_capacity(delivery.new_ids.len());
        while let Some(&id) = delivery.new_ids.last() {
            let claim = JointClaim {
                inner: Arc::clone(&inner),
                id: Some(id),
            };
            delivery.new_ids.pop();
            claims.push(claim);
        }
        claims.reverse();
        let adjustment = delivery.ticket.map(|id| JointAdjustmentReceipt {
            inner: Arc::clone(&inner),
            id: Some(id),
        });
        delivery.ticket = None;
        let new_claims = JointGrantedClaims::new(
            claims,
            delivery.body.take().expect("committed reply body"),
            &inner,
        );
        Poll::Ready(Ok(JointGrantedBatch {
            new_claims,
            adjustment,
        }))
    }
}

/// One observation of the same finite all-New request. This is a loan of the
/// original reply, not a grant and not another capacity or identity author.
///
/// No WC/Core/physical-owner mutex may be held while trying the real local
/// reservation. Dropping this loan does not undo its original claims: an
/// unwinding caller may still retain the physical value. Its original owner
/// must retain the admission and raw cause until explicit actual cleanup.
#[must_use = "Accept into the original stable receiver or explicitly dispose and reobserve"]
pub struct JointLocalOffer<'a> {
    admission: &'a mut JointAdmission,
    id: u64,
}

/// A stable receiver for the same physical holder and its original claims.
/// The physical holder must cover the entire admitted all-New batch. This
/// wrapper supplies no new capacity and grants no right to dispatch by itself.
/// Actual service/response/body ownership must continue this same custody.
#[must_use = "Retain the original physical holder through its actual exit"]
pub struct JointLocalGrant<P> {
    cpu_factory: Option<InternalResultClosedFactoryDescriptor>,
    physical: ManuallyDrop<P>,
    claims: ManuallyDrop<JointGrantedClaims>,
}
impl<P> JointLocalGrant<P> {
    /// A finite CPU grant may enter only the exact original registered pool and
    /// call factory. An ordinary E grant returns false for this closed seam.
    pub fn matches_internal_result_closed_factory(
        &self,
        descriptor: &InternalResultClosedFactoryDescriptor,
    ) -> bool {
        self.cpu_factory
            .as_ref()
            .is_some_and(|original| original.matches(descriptor))
    }
    pub fn is_internal_result_closed_factory(&self) -> bool {
        self.cpu_factory.is_some()
    }
    pub fn physical_mut(&mut self) -> &mut P {
        &mut self.physical
    }

    pub fn claims(&self) -> &JointGrantedClaims {
        &self.claims
    }

    /// Move both original values into empty slots of the same stable first-party
    /// physical owner. This calls no foreign code and performs no notification.
    /// That owner must continue the same claims through its response/body/actual
    /// physical exit, with explicit physical-before-claims destruction. A client
    /// or Endpoint return alone is not source exit. This transfer grants no new
    /// backing for input/future/pointees and is not an execution capability.
    pub fn transfer_in(
        self,
        physical: &mut Option<P>,
        claims: &mut Option<JointGrantedClaims>,
    ) -> Result<(), Self> {
        if physical.is_some() || claims.is_some() {
            return Err(self);
        }
        let mut original = ManuallyDrop::new(self);
        // SAFETY: both destinations were checked empty; moves execute no user
        // code, and the suppressed source destructor cannot read either field.
        *physical = Some(unsafe { ManuallyDrop::take(&mut original.physical) });
        *claims = Some(unsafe { ManuallyDrop::take(&mut original.claims) });
        drop(original.cpu_factory.take());
        Ok(())
    }
}
impl<P> Drop for JointLocalGrant<P> {
    fn drop(&mut self) {
        // SAFETY: each field is dropped once, in explicit custody order. If P's
        // real destructor unwinds, ManuallyDrop keeps all original claims
        // charged. No later field cleanup silently declares physical exit.
        unsafe {
            ManuallyDrop::drop(&mut self.physical);
            ManuallyDrop::drop(&mut self.claims);
        }
    }
}

impl JointAdmission {
    /// Observe the original finite all-New batch without releasing any physical
    /// claims to the caller. Existing adjustment requests use their original
    /// receipt path. The selected local try happens only after Ready(Ok).
    /// Waiting installs a Waker; local Pending is retried only by the original
    /// service's finite timer pass, using the remaining original proof budget.
    pub fn poll_local_offer(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<JointLocalOffer<'_>, JointAdmissionFailure>> {
        let Some(id) = self.id else {
            return Poll::Ready(Err(JointAdmissionFailure {
                error: WorkError::Released,
            }));
        };
        let inner = Arc::clone(&self.inner);
        // Both clone and destruction of a foreign Waker are outside State.
        let mut incoming = Some(cx.waker().clone());
        let mut outgoing = None;
        let result = {
            let mut state = inner.state.lock().unwrap();
            let result = (|state: &mut State| {
                let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
                let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
                if request.changes.iter().any(|change| change.old.is_some()) {
                    return Err(WorkError::Conflict);
                }
                match &request.phase {
                    RequestPhase::Waiting => {
                        let request = state.joint.as_mut().unwrap().requests.get_mut(&id).unwrap();
                        if request
                            .waker
                            .as_ref()
                            .is_none_or(|waker| !waker.will_wake(incoming.as_ref().unwrap()))
                        {
                            outgoing = request.waker.replace(incoming.take().unwrap());
                        }
                        return Ok(false);
                    }
                    RequestPhase::Failed(error) => {
                        let error = error.clone();
                        // Keep the original typed refusal and Q in the caller's
                        // stable admission until its ordinary Drop/withdraw path.
                        return Err(error);
                    }
                    RequestPhase::Granted { .. } => {}
                    RequestPhase::LocalOffered { .. } | RequestPhase::Delivered => {
                        return Err(WorkError::Conflict);
                    }
                }
                if let Some(fence) = request.native_action.as_ref() {
                    validate_native_action(state, request.owner, fence, &request.changes)?;
                }
                if request.root_payload.is_some() || request.root_domain.is_some() {
                    return Err(WorkError::Conflict);
                }
                if let Some(fence) = request.control_action {
                    validate_control_action(state, request.owner, fence, &request.changes)?;
                }
                if let Some(fence) = request.cpu_action {
                    validate_cpu_action(state, request.owner, fence, &request.changes)?;
                }
                if ledger.driver_closed {
                    return Err(WorkError::Closed);
                }
                if request.requires_business_active() {
                    state
                        .nodes
                        .get(&request.owner)
                        .ok_or(WorkError::Released)?
                        .check(&state.cancellations)?;
                    if state.closed {
                        return Err(WorkError::Closed);
                    }
                }
                let request = state.joint.as_mut().unwrap().requests.get_mut(&id).unwrap();
                let RequestPhase::Granted { new_ids } =
                    std::mem::replace(&mut request.phase, RequestPhase::Delivered)
                else {
                    unreachable!("validated original grant")
                };
                request.phase = RequestPhase::LocalOffered {
                    new_ids,
                    failure: None,
                    exiting: false,
                };
                Ok(true)
            })(&mut state);
            if matches!(result, Ok(true)) {
                state.advance_progress_revision();
            }
            result
        };
        drop(outgoing);
        drop(incoming);
        match result {
            Ok(false) => Poll::Pending,
            Ok(true) => Poll::Ready(Ok(JointLocalOffer {
                admission: self,
                id,
            })),
            Err(error) => Poll::Ready(Err(JointAdmissionFailure { error })),
        }
    }
}
impl JointLocalOffer<'_> {
    /// Dispose the original local physical attempt outside State, then return
    /// the same request/body/Q to Waiting. A Native Pending attempt passes None
    /// only after its original try has really dropped every partial holder.
    /// A destructor unwind leaves this original offer and its claims charged.
    /// No caller may retain an alias of P's reservation beyond this operation.
    /// Notify the returned token only after original source phase is stable.
    pub fn reobserve_after_drop<P>(
        &mut self,
        physical: &mut Option<P>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        let inner = Arc::clone(&self.admission.inner);
        inner.update_facts_silent(|state| {
            let request = state
                .joint
                .as_mut()
                .ok_or(WorkError::NotReady)?
                .requests
                .get_mut(&self.id)
                .ok_or(WorkError::Released)?;
            let RequestPhase::LocalOffered { exiting, .. } = &mut request.phase else {
                return Err(WorkError::Conflict);
            };
            if *exiting {
                return Err(WorkError::Conflict);
            }
            // Publish the no-repoll fence before entering foreign destruction.
            *exiting = true;
            Ok(())
        })?;
        drop(physical.take());
        inner.update_facts_silent(|state| {
            let request = state
                .joint
                .as_mut()
                .ok_or(WorkError::NotReady)?
                .requests
                .get_mut(&self.id)
                .ok_or(WorkError::Released)?;
            if !matches!(request.phase, RequestPhase::LocalOffered { .. }) {
                return Err(WorkError::Conflict);
            }
            let RequestPhase::LocalOffered {
                new_ids, failure, ..
            } = std::mem::replace(&mut request.phase, RequestPhase::Delivered)
            else {
                unreachable!()
            };
            // These were never exported. Roll back only the original New IDs;
            // there is no old-source mutation or second request/proof budget.
            for id in new_ids {
                remove_claim(state, id);
            }
            let request = state
                .joint
                .as_mut()
                .unwrap()
                .requests
                .get_mut(&self.id)
                .unwrap();
            request.phase = failure.map_or(RequestPhase::Waiting, RequestPhase::Failed);
            request.local_blocked = matches!(request.phase, RequestPhase::Waiting);
            if request.local_blocked {
                // Bounded by max_requests; this flag was false in LocalOffered.
                state.joint.as_mut().unwrap().local_waiters += 1;
            }
            Ok(())
        })?;
        Ok(crate::WorkloadProgressNotification::new(inner))
    }

    /// Publish into the original stable receiver before any Drop or wake can
    /// run. A nonempty destination, stopped Work or failed offer leaves the
    /// original physical Option and the entire provisional reply unchanged.
    /// This performs only checked internal moves under State, never user code.
    pub fn accept_in<P>(
        &mut self,
        physical: &mut Option<P>,
        destination: &mut Option<JointLocalGrant<P>>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        if destination.is_some() || physical.is_none() {
            return Err(WorkError::Conflict);
        }
        let inner = Arc::clone(&self.admission.inner);
        let count = {
            let state = inner.state.lock().unwrap();
            let request = state
                .joint
                .as_ref()
                .ok_or(WorkError::NotReady)?
                .requests
                .get(&self.id)
                .ok_or(WorkError::Released)?;
            let RequestPhase::LocalOffered { new_ids, .. } = &request.phase else {
                return Err(WorkError::Conflict);
            };
            new_ids.len()
        };
        // Uses the original quoted reply envelope. The empty vector cannot
        // execute any foreign destructor, including on a validation refusal.
        let mut claims = Vec::with_capacity(count);
        let mut outgoing = None;
        let result = inner.update_facts_silent(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let request = ledger.requests.get(&self.id).ok_or(WorkError::Released)?;
            let RequestPhase::LocalOffered {
                new_ids,
                failure,
                exiting,
            } = &request.phase
            else {
                return Err(WorkError::Conflict);
            };
            if *exiting {
                return Err(WorkError::Conflict);
            }
            if let Some(error) = failure {
                return Err(error.clone());
            }
            if let Some(fence) = request.native_action.as_ref() {
                validate_native_action(state, request.owner, fence, &request.changes)?;
            }
            if request.root_payload.is_some() {
                return Err(WorkError::Conflict);
            }
            if let Some(fence) = request.control_action {
                validate_control_action(state, request.owner, fence, &request.changes)?;
            }
            if let Some(fence) = request.cpu_action {
                validate_cpu_action(state, request.owner, fence, &request.changes)?;
            }
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if new_ids.len() != count || request.changes.iter().any(|change| change.old.is_some()) {
                return Err(WorkError::Conflict);
            }
            if request.requires_business_active() {
                state
                    .nodes
                    .get(&request.owner)
                    .ok_or(WorkError::Released)?
                    .check(&state.cancellations)?;
                if state.closed {
                    return Err(WorkError::Closed);
                }
            }
            // No fallible/foreign operation occurs after the first guard move.
            // The reserved Vec has exact space for every original New guard.
            for &id in new_ids {
                claims.push(JointClaim {
                    inner: Arc::clone(&inner),
                    id: Some(id),
                });
            }
            let body = request.changes.clone();
            let cpu_factory = request.cpu_action.and_then(|fence| fence.descriptor(state));
            let request = state
                .joint
                .as_mut()
                .unwrap()
                .requests
                .get_mut(&self.id)
                .unwrap();
            outgoing = request.waker.take();
            request.phase = RequestPhase::Delivered;
            *destination = Some(JointLocalGrant {
                cpu_factory,
                physical: ManuallyDrop::new(physical.take().unwrap()),
                claims: ManuallyDrop::new(JointGrantedClaims::new(
                    std::mem::take(&mut claims),
                    body,
                    &inner,
                )),
            });
            self.admission.id = None;
            remove_request(state, self.id);
            Ok(())
        });
        // On success the exact physical/claim bundle is already in its original
        // receiver before arbitrary Waker destruction or later notification.
        drop(outgoing);
        result?;
        Ok(crate::WorkloadProgressNotification::new(inner))
    }
}

/// Stack-owned handoff custody, never a State member or another request table.
/// Its body and final exit observer outlive the rollback's original transaction.
struct ReplyDelivery {
    inner: Arc<Inner>,
    new_ids: Vec<u64>,
    ticket: Option<u64>,
    body: Option<ChangeBody>,
    _exit: Option<crate::scope::ReclaimWakeOnExit>,
}
impl Drop for ReplyDelivery {
    fn drop(&mut self) {
        if self.new_ids.is_empty() && self.ticket.is_none() {
            return;
        }
        let ticket = self.ticket.take();
        let waker = self.inner.update(|state| {
            while let Some(id) = self.new_ids.pop() {
                remove_claim(state, id);
            }
            ticket.and_then(|id| withdraw_request(state, id))
        });
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Drop for JointAdmission {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let waker = self.inner.update(|state| withdraw_request(state, id));
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}
#[must_use = "Install against the exact stable holders or drop to withdraw the delta"]
pub struct JointAdjustmentReceipt {
    inner: Arc<Inner>,
    id: Option<u64>,
}
impl std::fmt::Debug for JointAdjustmentReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JointAdjustmentReceipt")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
impl JointAdjustmentReceipt {
    /// Synchronously adopt the whole batch into its original owners. No action
    /// may consume newly charged capacity before this operation succeeds.
    pub fn install(self, originals: &[&JointClaim]) -> Result<(), WorkError> {
        self.install_originals(originals, None)
    }

    /// Install the control binding only while borrowing its original owner.
    /// Physical claims in the same batch still require their original guards.
    pub fn install_control(
        self,
        owner: &WorkOwner,
        originals: &[&JointClaim],
    ) -> Result<(), WorkError> {
        self.install_originals(originals, Some(owner))
    }

    /// Publish the one child already committed by this exact reply. No second
    /// admission or physical-holder guard is created between grant and Node.
    pub fn install_child(mut self, owner: &WorkOwner) -> Result<WorkOwner, WorkError> {
        let mut child = None;
        self.install_child_in(&owner.scope(), &mut child)?.notify();
        Ok(child.expect("successful child publication installs the original owner"))
    }

    /// Install the original child in an existing stable receiver before any
    /// notification. Borrowing keeps this exact receipt with the caller on
    /// refusal; success disarms it in the same operation. Neither outcome
    /// invokes a callback. Notify success only after all receiver locks exit.
    pub fn install_child_in(
        &mut self,
        parent_scope: &WorkScope,
        destination: &mut Option<WorkOwner>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        if destination.is_some() {
            return Err(WorkError::Conflict);
        }
        if !Arc::ptr_eq(&parent_scope.inner, &self.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let id = self.id.ok_or(WorkError::Released)?;
        let result = self.inner.update_facts_silent(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
            match &request.phase {
                RequestPhase::Failed(error) => return Err(error.clone()),
                RequestPhase::Delivered => {}
                _ => return Err(WorkError::Conflict),
            }
            if state.closed || ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            if request.source_cohort
                || request.changes.len() != 1
                || request.owner != parent_scope.id
            {
                return Err(WorkError::Conflict);
            }
            let change = &request.changes[0];
            let source = change.source.as_ref().ok_or(WorkError::Conflict)?;
            let ClaimSource::PoolFree {
                slot,
                generation,
                parent,
                deadline,
            } = source.source
            else {
                return Err(WorkError::Conflict);
            };
            authenticate_parent(state, parent, parent_scope.id)?;
            let cell = &state.cancellations.slots[slot.0];
            let claim = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
            if cell.phase
                != (SlotPhase::ChildGranted {
                    parent: parent.work,
                    ticket: id,
                })
                || cell.generation
                    != generation
                        .checked_add(1)
                        .ok_or(WorkError::ArithmeticOverflow)?
                || cell.claim != Some(source.id)
                || claim.owner != ClaimOwner::ControlSlot(slot)
                || claim.pending != Some(id)
                || claim.structure_revision
                    != source
                        .structure_revision
                        .checked_add(1)
                        .ok_or(WorkError::ArithmeticOverflow)?
                || claim.release_revision != source.release_revision
                || claim.amounts != change.claim.amounts
                || claim.active_amounts != change.claim.amounts
                || claim.facts.key != change.claim.facts.key
                || claim.active_facts.key != change.claim.facts.key
            {
                return Err(WorkError::Conflict);
            }
            claim
                .structure_revision
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let claim_id = source.id;
            let child = state.install_bound_child(&self.inner, parent.work, slot, deadline)?;
            let claim = state
                .joint
                .as_mut()
                .unwrap()
                .claims
                .get_mut(&claim_id)
                .unwrap();
            claim.structure_revision += 1;
            claim.pending = None;
            remove_request(state, id);
            *destination = Some(child);
            Ok(())
        });
        result?;
        self.id = None;
        Ok(crate::WorkloadProgressNotification::new(Arc::clone(
            &self.inner,
        )))
    }

    fn install_originals(
        mut self,
        originals: &[&JointClaim],
        owner: Option<&WorkOwner>,
    ) -> Result<(), WorkError> {
        let control_scope = owner.map(WorkOwner::scope);
        let id = self.id.expect("live adjustment receipt");
        let result = self.inner.update(|state| {
            self.install_originals_state(state, id, originals, control_scope.as_ref(), false)
        });
        if result.is_ok() {
            self.id = None;
        }
        // Error leaves the RAII ticket armed; Drop rolls back only live delta.
        result
    }
    // Both public receipt routes use this exact original validation/commit.
    fn install_originals_state(
        &self,
        state: &mut State,
        id: u64,
        originals: &[&JointClaim],
        control_scope: Option<&WorkScope>,
        control_only: bool,
    ) -> Result<(), WorkError> {
        let ledger = state.joint.as_ref().unwrap();
        let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
        match &request.phase {
            RequestPhase::Failed(error) => return Err(error.clone()),
            RequestPhase::Delivered => {}
            _ => return Err(WorkError::Conflict),
        }
        if ledger.driver_closed {
            return Err(WorkError::Closed);
        }
        if request.root_payload.is_some() {
            return Err(WorkError::Conflict);
        }
        let changes = request.changes.clone();
        validate_control_productions(state, &changes)?;
        if control_only {
            let scope = control_scope.ok_or(WorkError::Conflict)?;
            if request.owner != scope.id {
                return Err(WorkError::Conflict);
            }
            validate_control_binding_shape(state, scope, &changes)?;
        }
        if request.requires_business_active() {
            state
                .nodes
                .get(&request.owner)
                .ok_or(WorkError::Released)?
                .check(&state.cancellations)?;
        }
        let sources = changes.iter().filter_map(|c| c.source.as_ref());
        if originals.len()
            != sources
                .clone()
                .filter(|source| matches!(source.source, ClaimSource::Physical))
                .count()
            || control_scope.is_some()
                != sources
                    .clone()
                    .any(|source| matches!(source.source, ClaimSource::NodeControl { .. }))
        {
            return Err(WorkError::Conflict);
        }
        for source in sources {
            match source.source {
                ClaimSource::Physical => {
                    let original = originals
                        .iter()
                        .find(|original| original.id == Some(source.id))
                        .ok_or(WorkError::Conflict)?;
                    if !Arc::ptr_eq(&original.inner, &self.inner) {
                        return Err(WorkError::ForeignAuthority);
                    }
                }
                ClaimSource::NodeControl { work, .. } => {
                    let original = control_scope.ok_or(WorkError::Conflict)?;
                    if !Arc::ptr_eq(&original.inner, &self.inner) {
                        return Err(WorkError::ForeignAuthority);
                    }
                    if original.id != work || original.id != request.owner {
                        return Err(WorkError::Conflict);
                    }
                }
                ClaimSource::Production { work, .. } => {
                    if control_scope.is_none_or(|scope| scope.id != work || work != request.owner) {
                        return Err(WorkError::Conflict);
                    }
                }
                ClaimSource::PoolFree { .. }
                | ClaimSource::Payload { .. }
                | ClaimSource::Computation { .. } => {
                    return Err(WorkError::Conflict);
                }
            }
            let record = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
            if record.pending != Some(id) {
                return Err(WorkError::Conflict);
            }
            if matches!(
                source.source,
                ClaimSource::NodeControl { .. } | ClaimSource::Production { .. }
            ) {
                // The exact pending ticket excludes another control grant,
                // and control partial release is not exposed. Its sole
                // intervening structural change is this original grant.
                let mut committed = source.clone();
                committed.structure_revision = committed
                    .structure_revision
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
                authenticate_scope(state, record, &committed, request.owner)?;
            } else if !record.custody.is_raw()
                || record.owner != ClaimOwner::PhysicalWork(request.owner)
            {
                return Err(WorkError::Conflict);
            }
            // Physical holders may report an actual partial release after
            // grant. Adopt their remaining live amount, never the snapshot.
            record
                .structure_revision
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
        }
        let ledger = state.joint.as_mut().unwrap();
        for change in changes.iter() {
            if let Some(old) = change.old {
                let record = ledger.claims.get_mut(&old).unwrap();
                record.active_facts = Arc::clone(&record.facts);
                record.active_amounts = record.amounts;
                record.pending = None;
                record.structure_revision += 1;
            }
        }
        remove_request(state, id);
        Ok(())
    }
}
impl Drop for JointAdjustmentReceipt {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let waker = self.inner.update(|state| withdraw_request(state, id));
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}
