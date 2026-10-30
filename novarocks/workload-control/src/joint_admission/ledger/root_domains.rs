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

//! Canonical Root domains in the original Node and original aggregate ledger.
//! This module owns no amount, count, registry, request queue or semaphore.
use super::*;
use std::num::NonZeroU64;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::joint_admission) enum GroupOrigin {
    Frozen,
    Inherited(CanonicalRootRef),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::joint_admission) struct CanonicalRootRef {
    pub(in crate::joint_admission) work: WorkId,
    slot: usize,
    generation: u64,
}
impl GroupOrigin {
    pub(in crate::joint_admission) fn is_root(self, root: WorkId) -> bool {
        matches!(self, Self::Inherited(original) if original.work == root)
    }
}
/// Dormant is an explicit relationship, not an external-effect grant.
/// Original child edges/claim holders keep this Node alive.
pub(crate) struct RootDomains {
    binding: CanonicalRootRef,
    pub(super) active: Arc<Vec<DependencyRef>>,
    pub(super) charged: Arc<Vec<DependencyRef>>,
    pub(super) epoch: u64,
    pub(super) pending: Option<u64>,
    pub(super) construction: Option<ProposalKey>,
    pub(super) writer_revision: u64,
}
#[derive(Clone)]
pub(in crate::joint_admission) struct RootDomainChange {
    pub(in crate::joint_admission) root: WorkId,
    pub(in crate::joint_admission) epoch: u64,
    pub(in crate::joint_admission) candidate: Arc<Vec<DependencyRef>>,
    pub(in crate::joint_admission) lane: JointLane,
}
/// An exact, non-owning view. Neither Dormant nor this view authorizes an effect.
#[derive(Clone)]
pub struct RootDomainBinding {
    inner: Weak<Inner>,
    root: WorkId,
    binding: CanonicalRootRef,
}
impl WorkScope {
    pub fn prepare_root_domains(&self) -> Result<RootDomainBinding, WorkError> {
        // No K-dependent allocation or final graph Drop runs under State.
        let empty: Arc<Vec<DependencyRef>> = Arc::new(Vec::new());
        let (source, amount, class, binding) = {
            let state = self.inner.state.lock().unwrap();
            validate_active_control_node(&state, self)?;
            let node = &state.nodes[&self.id];
            if node.parent.is_some() || node.root != self.id {
                return Err(WorkError::Conflict);
            }
            if let Some(domains) = &node.root_domains {
                return Ok(RootDomainBinding {
                    inner: Arc::downgrade(&self.inner),
                    root: self.id,
                    binding: domains.binding,
                });
            }
            if node.children != 0
                || node.resource_waiters != 0
                || state
                    .joint
                    .as_ref()
                    .unwrap()
                    .claims
                    .values()
                    .any(|r| r.owner == ClaimOwner::PhysicalWork(self.id))
            {
                return Err(WorkError::Conflict);
            }
            let id = state.cancellations.slots[node.cancellation.0]
                .claim
                .ok_or(WorkError::NotReady)?;
            let row = &state.joint.as_ref().unwrap().claims[&id];
            if row.pending.is_some() || row.facts.key != row.active_facts.key {
                return Err(WorkError::Conflict);
            }
            (
                Arc::clone(&row.active_facts),
                row.active_amounts,
                node.class,
                CanonicalRootRef {
                    work: self.id,
                    slot: node.cancellation.0,
                    generation: state.cancellations.slots[node.cancellation.0].generation,
                },
            )
        };
        // Dormant has no D. This is exactly the existing positive local S, not
        // a retroactive binding of any previously dispatched external action.
        let facts = Arc::new(ResponsibilityFacts {
            key: Arc::new(FactKey {
                origin: GroupOrigin::Inherited(binding),
                class: class_index(class),
                lane: source.lane as usize,
                dependencies: source.key.dependencies.clone(),
                source_kind: source.key.source_kind,
            }),
            class,
            lane: source.lane,
            dependencies: Arc::clone(&source.dependencies),
        });
        let retired = self.inner.update_facts_silent(|state| {
            validate_active_control_node(state, self)?;
            let node = &state.nodes[&self.id];
            if node.root_domains.is_some() {
                return Err(WorkError::Conflict);
            }
            if node.children != 0
                || node.resource_waiters != 0
                || state
                    .joint
                    .as_ref()
                    .unwrap()
                    .claims
                    .values()
                    .any(|r| r.owner == ClaimOwner::PhysicalWork(self.id))
            {
                return Err(WorkError::Conflict);
            }
            let id = state.cancellations.slots[node.cancellation.0]
                .claim
                .ok_or(WorkError::NotReady)?;
            let ledger = state.joint.as_mut().unwrap();
            let row = &ledger.claims[&id];
            if row.pending.is_some()
                || row.active_amounts != amount
                || !Arc::ptr_eq(&row.active_facts, &source)
            {
                return Err(WorkError::Conflict);
            }
            ledger
                .facts_revision
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let row = ledger.claims.get_mut(&id).unwrap();
            let retired = (
                std::mem::replace(&mut row.facts, Arc::clone(&facts)),
                std::mem::replace(&mut row.active_facts, Arc::clone(&facts)),
            );
            ledger.layout_changed();
            state.nodes.get_mut(&self.id).unwrap().root_domains = Some(RootDomains {
                binding,
                active: Arc::clone(&empty),
                charged: Arc::clone(&empty),
                epoch: 0,
                pending: None,
                construction: None,
                writer_revision: 0,
            });
            Ok(retired)
        })?;
        drop(retired);
        Ok(RootDomainBinding {
            inner: Arc::downgrade(&self.inner),
            root: self.id,
            binding,
        })
    }

    pub fn begin_inherited_control_binding(
        &self,
        root: &RootDomainBinding,
        local_s: &[SharedDependency],
    ) -> Result<ControlBindingProposal, WorkError> {
        root.authenticate(self)?;
        let mut proposal = self.begin_control_binding()?;
        let current = proposal
            .proposal
            .current
            .as_mut()
            .ok_or(WorkError::Conflict)?;
        if let GroupOrigin::Inherited(original) = current.origin
            && original != root.binding
        {
            return Err(WorkError::Conflict);
        }
        current.origin = GroupOrigin::Inherited(root.binding);
        for fact in local_s {
            proposal.push_dependency(fact)?;
        }
        Ok(proposal)
    }
}
impl RootDomainBinding {
    fn authenticate(&self, scope: &WorkScope) -> Result<(), WorkError> {
        if !Weak::ptr_eq(&self.inner, &Arc::downgrade(&scope.inner)) {
            return Err(WorkError::ForeignAuthority);
        }
        let state = scope.inner.state.lock().unwrap();
        let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
        if node.root != self.root
            || state
                .nodes
                .get(&self.root)
                .and_then(|n| n.root_domains.as_ref())
                .is_none()
        {
            return Err(WorkError::Conflict);
        }
        Ok(())
    }
    pub fn initial_payload_source(
        &self,
        root: &WorkScope,
        lane: JointLane,
    ) -> Result<RootDomainInitialSource, WorkError> {
        self.authenticate(root)?;
        if root.id != self.root || !matches!(lane, JointLane::Foreground | JointLane::Custody) {
            return Err(WorkError::Conflict);
        }
        let state = root.inner.state.lock().unwrap();
        validate_active_control_node(&state, root)?;
        let domains = state.nodes[&root.id].root_domains.as_ref().unwrap();
        if domains.epoch != 0 || domains.pending.is_some() {
            return Err(WorkError::Conflict);
        }
        Ok(RootDomainInitialSource {
            binding: self.clone(),
        })
    }
}

pub struct RootDomainInitialSource {
    binding: RootDomainBinding,
}
pub struct RootDomainInitialProposal {
    proposal: JointProposal,
    local: Arc<ResponsibilityFacts>,
}
pub struct RootDomainInitialAdmission {
    admission: JointAdmission,
    batch: Option<JointGrantedBatch>,
    raw: Option<JointClaim>,
}
impl RootDomainInitialSource {
    pub fn try_begin(
        &self,
        metadata_bytes: NonZeroU64,
        payload_bytes: NonZeroU64,
    ) -> Result<JointTryBegin<RootDomainInitialProposal>, JointAdmissionFailure> {
        let result = (|| {
            let inner = self.binding.inner.upgrade().ok_or(WorkError::Released)?;
            let scope = WorkScope {
                inner: Arc::clone(&inner),
                id: self.binding.root,
            };
            self.binding.authenticate(&scope)?;
            let handle = JointAdmissionHandle {
                inner: Arc::clone(&inner),
            };
            let mut proposal =
                match handle.try_begin_root_writer(scope.clone(), JointLane::Foreground, None)? {
                    JointTryBegin::Full(wait) => return Ok(JointTryBegin::Full(wait)),
                    JointTryBegin::Started(proposal) => proposal,
                };
            let local = {
                let state = inner.state.lock().unwrap();
                validate_active_control_node(&state, &scope)?;
                let node = &state.nodes[&scope.id];
                let domains = node.root_domains.as_ref().ok_or(WorkError::Conflict)?;
                if domains.epoch != 0 || domains.pending.is_some() {
                    return Err(WorkError::Conflict);
                }
                let id = state.cancellations.slots[node.cancellation.0]
                    .claim
                    .ok_or(WorkError::NotReady)?;
                Arc::clone(&state.joint.as_ref().unwrap().claims[&id].active_facts)
            };
            proposal.begin_new(
                JointLane::Custody,
                JointResources::ZERO
                    .with(JointResource::ClaimRecords, 1)
                    .with(JointResource::MetadataBytes, metadata_bytes.get())
                    .with(JointResource::PayloadBytes, payload_bytes.get()),
            )?;
            // The builder's single selected set is the candidate Root D. The
            // actual new payload later borrows this original local S graph.
            Ok(JointTryBegin::Started(RootDomainInitialProposal {
                proposal,
                local,
            }))
        })();
        result.map_err(|error| JointAdmissionFailure { error })
    }
}
impl RootDomainInitialProposal {
    pub fn push_dependency(&mut self, fact: &SharedDependency) -> Result<(), WorkError> {
        self.proposal.push_dependency(fact)
    }
    pub fn submit(mut self) -> Result<RootDomainInitialAdmission, JointAdmissionFailure> {
        let finish = (|| {
            self.proposal.end_change()?;
            let record = self
                .proposal
                .records
                .last_mut()
                .ok_or(WorkError::Conflict)?;
            if record.claim.facts.dependencies.is_empty() {
                return Err(WorkError::Conflict);
            }
            let candidate = Arc::clone(&record.claim.facts.dependencies);
            record.claim.facts = Arc::clone(&self.local);
            self.proposal.root_domain = Some(RootDomainChange {
                root: self.proposal.scope.id,
                epoch: 0,
                candidate,
                lane: JointLane::Foreground,
            });
            Ok(())
        })();
        finish.map_err(|error| JointAdmissionFailure { error })?;
        Ok(RootDomainInitialAdmission {
            admission: self.proposal.submit()?,
            batch: None,
            raw: None,
        })
    }
}
impl RootDomainInitialAdmission {
    pub fn poll_install_in(
        &mut self,
        cx: &mut Context<'_>,
        root: &WorkScope,
        target: &mut Option<RetainedPayloadGrant>,
    ) -> Poll<Result<crate::WorkloadProgressNotification, JointAdmissionFailure>> {
        if target.is_some() {
            return Poll::Ready(Err(JointAdmissionFailure {
                error: WorkError::Conflict,
            }));
        }
        if self.batch.is_none() {
            self.batch = Some(match Pin::new(&mut self.admission).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(batch)) => batch,
            });
        }
        let batch = self.batch.as_mut().unwrap();
        if self.raw.is_none() {
            if batch.new_claims.len() != 1 {
                return Poll::Ready(Err(JointAdmissionFailure {
                    error: WorkError::Conflict,
                }));
            }
            self.raw = batch.new_claims.pop();
        }
        Poll::Ready(
            typed_custody::adopt_initial_root_in(
                root,
                &mut self.raw,
                batch.adjustment.as_mut().ok_or(WorkError::Conflict),
                target,
            )
            .map_err(|error| JointAdmissionFailure { error }),
        )
    }
}

pub(super) fn validate_change(
    state: &State,
    change: &RootDomainChange,
    ticket: Option<u64>,
) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    let node = state.nodes.get(&change.root).ok_or(WorkError::Released)?;
    if node.parent.is_some() || node.owner != crate::OwnerState::Active {
        return Err(WorkError::Conflict);
    }
    if change.lane != JointLane::Settlement {
        node.check(&state.cancellations)?;
    }
    let domains = node.root_domains.as_ref().ok_or(WorkError::Conflict)?;
    if domains.epoch != change.epoch
        || domains.pending != ticket
        || change.candidate.is_empty()
        || !domains.active.iter().all(|old| {
            change
                .candidate
                .binary_search_by_key(&old.ordinal, |d| d.ordinal)
                .is_ok()
        })
    {
        return Err(WorkError::Conflict);
    }
    domains
        .epoch
        .checked_add(1)
        .ok_or(WorkError::ArithmeticOverflow)?;
    Ok(())
}
pub(super) fn queue(state: &mut State, change: &RootDomainChange, ticket: u64) {
    let domains = state
        .nodes
        .get_mut(&change.root)
        .unwrap()
        .root_domains
        .as_mut()
        .unwrap();
    domains.construction = None;
    domains.pending = Some(ticket);
}

pub(super) fn validate_construction(
    state: &State,
    root: WorkId,
    key: Option<ProposalKey>,
) -> Result<(), WorkError> {
    let domains = state
        .nodes
        .get(&root)
        .and_then(|n| n.root_domains.as_ref())
        .ok_or(WorkError::Released)?;
    if key.is_none() || domains.construction != key || domains.pending.is_some() {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

pub(super) fn release_construction(state: &mut State, root: WorkId, key: ProposalKey) {
    let Some(domains) = state
        .nodes
        .get_mut(&root)
        .and_then(|n| n.root_domains.as_mut())
    else {
        return;
    };
    if domains.construction == Some(key) {
        domains.construction = None;
        domains.writer_revision += 1; // Reserved by the same acquisition.
    }
}

/// Capture only the exact original precharge view while an official writer
/// owns it. No current charged revision becomes a source capability.
/// Waiting is allowed only for an original still-live source. A changed
/// official writer revision asks the caller to capture again; it does not
/// authorize a stale physical, release or ordinary structural revision.
pub(super) fn validate_captured(
    state: &State,
    root: WorkId,
    view: &JointClaimView,
    work: WorkId,
    observed: Option<u64>,
) -> Result<(), WorkError> {
    let row = state
        .joint
        .as_ref()
        .ok_or(WorkError::NotReady)?
        .claims
        .get(&view.id)
        .ok_or(WorkError::Released)?;
    let mut current = view.clone();
    current.structure_revision = row.structure_revision;
    current.release_revision = row.release_revision;
    authenticate_scope(state, row, &current, work)?;
    let original = capture_view(state, current, work)?;
    let domains = state
        .nodes
        .get(&root)
        .and_then(|n| n.root_domains.as_ref())
        .ok_or(WorkError::Released)?;
    if original.release_revision != view.release_revision
        || (observed == Some(domains.writer_revision)
            && original.structure_revision != view.structure_revision)
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

pub(super) fn capture_view(
    state: &State,
    view: JointClaimView,
    work: WorkId,
) -> Result<JointClaimView, WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    let row = ledger.claims.get(&view.id).ok_or(WorkError::Released)?;
    let Some(ticket) = row.pending else {
        return Ok(view);
    };
    let request = ledger.requests.get(&ticket).ok_or(WorkError::Conflict)?;
    let root = state.nodes.get(&work).ok_or(WorkError::Released)?.root;
    let domain = request.root_domain.as_ref().ok_or(WorkError::Conflict)?;
    if domain.root != root
        || state.nodes[&root]
            .root_domains
            .as_ref()
            .is_none_or(|d| d.pending != Some(ticket))
        || request.owner != root
        || matches!(request.phase, RequestPhase::Failed(_))
    {
        return Err(WorkError::Conflict);
    }
    let change = request
        .changes
        .iter()
        .find(|c| c.old == Some(view.id))
        .ok_or(WorkError::Conflict)?;
    let original = change.source.as_ref().ok_or(WorkError::Conflict)?;
    let same_source = match (original.source, view.source) {
        (ClaimSource::Payload { epoch: a }, ClaimSource::Payload { epoch: b })
        | (ClaimSource::Computation { epoch: a }, ClaimSource::Computation { epoch: b }) => a == b,
        (
            ClaimSource::NodeControl {
                work: a,
                slot: sa,
                generation: ga,
            },
            ClaimSource::NodeControl {
                work: b,
                slot: sb,
                generation: gb,
            },
        ) => a == b && sa == sb && ga == gb,
        _ => false,
    };
    if !same_source
        || !Weak::ptr_eq(&original.inner, &view.inner)
        || original.release_revision != row.release_revision
        || !(row.structure_revision == original.structure_revision
            || original.structure_revision.checked_add(1) == Some(row.structure_revision))
        || change.original_amounts != Some(row.active_amounts)
        || change
            .original_facts
            .as_ref()
            .is_none_or(|f| f.key != row.active_facts.key)
    {
        return Err(WorkError::Conflict);
    }
    Ok(original.clone())
}
pub(super) fn charge(state: &mut State, change: &RootDomainChange) {
    let domains = state
        .nodes
        .get_mut(&change.root)
        .unwrap()
        .root_domains
        .as_mut()
        .unwrap();
    domains.charged = Arc::clone(&change.candidate);
    state.joint.as_mut().unwrap().layout_changed();
}
pub(super) fn rollback(
    state: &mut State,
    change: &RootDomainChange,
    ticket: u64,
) -> Option<Arc<Vec<DependencyRef>>> {
    let domains = state.nodes.get_mut(&change.root)?.root_domains.as_mut()?;
    if domains.pending != Some(ticket) {
        return None;
    }
    domains.pending = None;
    domains.writer_revision += 1;
    let retired = std::mem::replace(&mut domains.charged, Arc::clone(&domains.active));
    state.joint.as_mut().unwrap().layout_changed();
    Some(retired)
}
pub(super) fn validate_install(state: &State, ticket: u64, root: WorkId) -> Result<(), WorkError> {
    let request = state
        .joint
        .as_ref()
        .unwrap()
        .requests
        .get(&ticket)
        .ok_or(WorkError::Released)?;
    if request.owner != root || !matches!(request.phase, RequestPhase::Delivered) {
        return Err(WorkError::Conflict);
    }
    state
        .joint
        .as_ref()
        .unwrap()
        .facts_revision
        .checked_add(1)
        .ok_or(WorkError::ArithmeticOverflow)?;
    let change = request.root_domain.as_ref().ok_or(WorkError::Conflict)?;
    validate_change(state, change, Some(ticket))
}
pub(super) fn install(state: &mut State, ticket: u64) -> Option<Arc<Vec<DependencyRef>>> {
    let change = state.joint.as_ref().unwrap().requests[&ticket]
        .root_domain
        .as_ref()
        .unwrap();
    let node = state.nodes.get_mut(&change.root).unwrap();
    let domains = node.root_domains.as_mut().unwrap();
    let changed = domains.active.len() != domains.charged.len()
        || domains
            .active
            .iter()
            .zip(domains.charged.iter())
            .any(|(a, b)| a.ordinal != b.ordinal);
    if changed {
        domains.epoch += 1;
    }
    domains.pending = None;
    domains.writer_revision += 1;
    let retired = std::mem::replace(&mut domains.active, Arc::clone(&domains.charged));
    state.joint.as_mut().unwrap().layout_changed();
    Some(retired)
}

/// At most one such header exists in the driver's record-at-a-time cursor.
/// Its old Root alias is dropped outside State before the next record capture.
pub(in crate::joint_admission) struct FactCapture {
    facts: Arc<ResponsibilityFacts>,
    root: Option<Arc<Vec<DependencyRef>>>,
    selected: Vec<DependencyRef>,
    ordinals: Vec<u64>,
    local_cursor: usize,
    root_cursor: usize,
    merge_capacity: usize,
    allocated: bool,
}
impl FactCapture {
    #[cfg(test)]
    pub(in crate::joint_admission) fn capture(
        state: &State,
        facts: Arc<ResponsibilityFacts>,
        candidate: Option<&RootDomainChange>,
    ) -> Result<Self, WorkError> {
        let root = match facts.key.origin {
            GroupOrigin::Frozen => None,
            GroupOrigin::Inherited(id) => Some(
                if let Some(change) = candidate.filter(|c| c.root == id.work) {
                    Arc::clone(&change.candidate)
                } else {
                    Arc::clone(&resolve(state, id).ok_or(WorkError::Released)?.charged)
                },
            ),
        };
        let merge_capacity = facts
            .dependencies
            .len()
            .checked_add(root.as_ref().map_or(0, |d| d.len()))
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(Self {
            facts,
            root,
            selected: Vec::new(),
            ordinals: Vec::new(),
            local_cursor: 0,
            root_cursor: 0,
            merge_capacity,
            allocated: false,
        })
    }
    pub(super) fn active(
        facts: Arc<ResponsibilityFacts>,
        root: Option<Arc<Vec<DependencyRef>>>,
    ) -> Result<Self, WorkError> {
        let merge_capacity = facts
            .dependencies
            .len()
            .checked_add(root.as_ref().map_or(0, |d| d.len()))
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(Self {
            facts,
            root,
            selected: Vec::new(),
            ordinals: Vec::new(),
            local_cursor: 0,
            root_cursor: 0,
            merge_capacity,
            allocated: false,
        })
    }
    /// One bounded merge step; allocation and membership clone occur lock-free.
    pub(in crate::joint_admission) fn step(
        &mut self,
    ) -> Result<Poll<Arc<ResponsibilityFacts>>, WorkError> {
        let Some(root) = &self.root else {
            return Ok(Poll::Ready(Arc::clone(&self.facts)));
        };
        if !self.allocated {
            self.selected
                .try_reserve_exact(self.merge_capacity)
                .map_err(|_| WorkError::Capacity("canonical effective D backing"))?;
            self.ordinals
                .try_reserve_exact(self.merge_capacity)
                .map_err(|_| WorkError::Capacity("canonical effective D key backing"))?;
            self.allocated = true;
            return Ok(Poll::Pending);
        }
        let a = self.facts.dependencies.get(self.local_cursor);
        let b = root.get(self.root_cursor);
        let next = match (a, b) {
            (Some(a), Some(b)) if a.ordinal == b.ordinal => {
                self.local_cursor += 1;
                self.root_cursor += 1;
                Some(a)
            }
            (Some(a), Some(b)) if a.ordinal < b.ordinal => {
                self.local_cursor += 1;
                Some(a)
            }
            (Some(_), Some(b)) => {
                self.root_cursor += 1;
                Some(b)
            }
            (Some(a), None) => {
                self.local_cursor += 1;
                Some(a)
            }
            (None, Some(b)) => {
                self.root_cursor += 1;
                Some(b)
            }
            (None, None) => None,
        };
        if let Some(next) = next {
            if self.selected.len() == self.merge_capacity {
                return Err(WorkError::Conflict);
            }
            self.ordinals.push(next.ordinal);
            self.selected.push(next.clone());
            return Ok(Poll::Pending);
        }
        let selected = std::mem::take(&mut self.selected);
        Ok(Poll::Ready(Arc::new(ResponsibilityFacts {
            key: Arc::new(FactKey {
                origin: GroupOrigin::Frozen,
                class: self.facts.key.class,
                lane: self.facts.key.lane,
                source_kind: self.facts.key.source_kind,
                dependencies: std::mem::take(&mut self.ordinals),
            }),
            class: self.facts.class,
            lane: self.facts.lane,
            dependencies: Arc::new(selected),
        })))
    }
}

/// Domain existence is read from the original Node; local S may be empty.
pub(super) fn has_effective_domains(
    state: &State,
    facts: &ResponsibilityFacts,
    active: bool,
) -> bool {
    if !facts.dependencies.is_empty() {
        return true;
    }
    match facts.key.origin {
        GroupOrigin::Frozen => false,
        GroupOrigin::Inherited(root) => resolve(state, root).is_some_and(|d| {
            if active {
                !d.active.is_empty()
            } else {
                !d.charged.is_empty()
            }
        }),
    }
}
/// Explicit source propagation combines only compatible original relationships.
pub(super) fn combined_origin(
    left: &ResponsibilityFacts,
    right: &ResponsibilityFacts,
) -> Result<GroupOrigin, WorkError> {
    match (left.key.origin, right.key.origin) {
        (GroupOrigin::Frozen, GroupOrigin::Frozen) => Ok(GroupOrigin::Frozen),
        (GroupOrigin::Inherited(a), GroupOrigin::Inherited(b)) if a == b => {
            Ok(GroupOrigin::Inherited(a))
        }
        _ => Err(WorkError::Conflict),
    }
}

pub(super) fn pending_payload_is_original(
    state: &State,
    root: WorkId,
    id: u64,
    epoch: u64,
) -> bool {
    let Some(ledger) = &state.joint else {
        return false;
    };
    let Some(row) = ledger.claims.get(&id) else {
        return false;
    };
    let Some(ticket) = row.pending else {
        return false;
    };
    let Some(request) = ledger.requests.get(&ticket) else {
        return false;
    };
    request.owner == root && request.root_domain.as_ref().is_some_and(|d| d.root == root && d.epoch != 0)
        && request.changes.iter().any(|c| c.old == Some(id)
            && c.source.as_ref().is_some_and(|s| matches!(s.source, ClaimSource::Payload { epoch: original } if original == epoch))
            && c.original_facts.as_ref().is_some_and(|f| f.key == row.active_facts.key)
            && c.original_amounts == Some(row.active_amounts))
        && !matches!(request.phase, RequestPhase::Failed(_))
}

pub(super) fn observation(change: &ChangeRecord) -> bool {
    change.old.is_some()
        && change.original_amounts == Some(change.claim.amounts)
        && change
            .original_facts
            .as_ref()
            .is_some_and(|facts| facts.key == change.claim.facts.key)
}

/// A new row may not be certified only under a charged, uninstalled D and
/// then inherit an unproved rollback D. Preserve the same queued request.
pub(in crate::joint_admission) fn request_blocked(state: &State, ticket: u64) -> bool {
    let Some(request) = state.joint.as_ref().and_then(|l| l.requests.get(&ticket)) else {
        return false;
    };
    request
        .changes
        .iter()
        .any(|change| match change.claim.facts.key.origin {
            GroupOrigin::Frozen => false,
            GroupOrigin::Inherited(root) => state
                .nodes
                .get(&root.work)
                .and_then(|n| n.root_domains.as_ref())
                .and_then(|d| d.pending)
                .is_some_and(|pending| pending != ticket),
        })
}

/// Exact inherited graph lookup also covers the original retired control
/// backing. The fixed slot cannot be reused before its actual reset; no scan,
/// registry, owning observation or new authority is introduced.
fn resolve(state: &State, original: CanonicalRootRef) -> Option<&RootDomains> {
    if let Some(domains) = state
        .nodes
        .get(&original.work)
        .and_then(|n| n.root_domains.as_ref())
    {
        return (domains.binding == original).then_some(domains);
    }
    let slot = state.cancellations.slots.get(original.slot)?;
    if slot.phase != SlotPhase::Retired || slot.generation != original.generation {
        return None;
    }
    slot.retired_root_domains
        .as_ref()
        .filter(|d| d.binding == original)
}
pub(super) fn origin(state: &State, root: WorkId) -> Result<GroupOrigin, WorkError> {
    let domains = state
        .nodes
        .get(&root)
        .and_then(|n| n.root_domains.as_ref())
        .ok_or(WorkError::Released)?;
    Ok(GroupOrigin::Inherited(domains.binding))
}

impl Drop for RootDomainInitialAdmission {
    fn drop(&mut self) {
        // This private raw never escapes. Retire it before its original
        // adjustment receipt may withdraw the charged initial Root domain.
        drop(self.raw.take());
        drop(self.batch.take());
    }
}
