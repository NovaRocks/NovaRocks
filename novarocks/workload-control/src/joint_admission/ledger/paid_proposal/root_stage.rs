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

//! One original paid request for retained Root payload and one direct stage.
use super::super::typed_custody::{
    install_computation_pin, payload_capture_view, payload_growth_view, same_payload_grant,
};
use super::*;

/// Pure capture from the positive original payload grant. No typed pin or Node
/// lifetime is retained, and no raw claim/view is exposed. Installation still
/// requires that same positive grant and the actual direct-child WorkOwner.
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::RootStageSource;
/// fn no_raw(source: RootStageSource) { source.view(); }
/// ```
#[must_use = "Capture is not a grant; construct outside receiver locks"]
pub struct RootStageSource {
    computation_window: Option<crate::result_window::ClientComputationWindow>,
    writer_revision: Option<u64>,
    inner: Weak<Inner>,
    root: WorkId,
    fence: RootStageFence,
    payload: JointClaimView,
    root_control: JointClaimView,
    stage_control: JointClaimView,
    computation: Option<JointClaimView>,
}
fn node_view(state: &State, scope: &WorkScope) -> Result<JointClaimView, WorkError> {
    validate_active_control_node(state, scope)?;
    let node = &state.nodes[&scope.id];
    let cell = &state.cancellations.slots[node.cancellation.0];
    let id = cell.claim.ok_or(WorkError::NotReady)?;
    let row = &state.joint.as_ref().unwrap().claims[&id];
    if cell.phase != SlotPhase::Node(scope.id)
        || !row.custody.is_raw()
        || row.owner != ClaimOwner::ControlSlot(node.cancellation)
        || row.active_amounts != cell.base
        || (row.pending.is_none()
            && (row.amounts != cell.base || row.facts.key != row.active_facts.key))
    {
        return Err(WorkError::Conflict);
    }
    root_domains::capture_view(
        state,
        JointClaimView {
            inner: Arc::downgrade(&scope.inner),
            id,
            source: ClaimSource::NodeControl {
                work: scope.id,
                slot: node.cancellation,
                generation: cell.generation,
            },
            structure_revision: row.structure_revision,
            release_revision: row.release_revision,
        },
        scope.id,
    )
}
fn validate_nodes(state: &State, root: WorkId, stage: WorkId) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    if state.closed || ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    if !state.cancellations.fixed {
        return Err(WorkError::NotReady);
    }
    let parent = state.nodes.get(&root).ok_or(WorkError::Released)?;
    let child = state.nodes.get(&stage).ok_or(WorkError::Released)?;
    parent.check(&state.cancellations)?;
    child.check(&state.cancellations)?;
    if parent.owner != crate::OwnerState::Active
        || child.owner != crate::OwnerState::Active
        || parent.parent.is_some()
        || child.parent != Some(root)
        || child.root != root
        || parent.class != child.class
        || root == stage
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}
impl RetainedPayloadGrant {
    /// Capture may run while the original finite receiver holds its own lock:
    /// only original State reads occur, with no constructors, wakes or pins.
    pub fn root_stage_source(
        &self,
        root: &WorkScope,
        stage: &WorkOwner,
    ) -> Result<RootStageSource, WorkError> {
        let child = stage.scope();
        if !Arc::ptr_eq(&root.inner, &child.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let state = root.inner.state.lock().unwrap();
        validate_nodes(&state, root.id, child.id)?;
        let payload = if state.nodes[&root.id].root_domains.is_some() {
            payload_capture_view(self, root, &state)?
        } else {
            payload_growth_view(self, root, &state)?
        };
        let ClaimSource::Payload { epoch } = payload.source else {
            unreachable!()
        };
        let root_control = node_view(&state, root)?;
        let stage_control = node_view(&state, &child)?;
        if state.joint.as_ref().unwrap().claims[&root_control.id]
            .active_facts
            .dependencies
            .is_empty()
            && state.nodes[&root.id]
                .root_domains
                .as_ref()
                .is_none_or(|d| d.epoch == 0)
        {
            return Err(WorkError::Conflict);
        }
        Ok(RootStageSource {
            computation_window: None,
            writer_revision: state.nodes[&root.id]
                .root_domains
                .as_ref()
                .map(|d| d.writer_revision),
            inner: Arc::downgrade(&root.inner),
            root: root.id,
            fence: RootStageFence {
                canonical: state.nodes[&root.id].root_domains.is_some(),
                stage: child.id,
                payload: payload.id,
                epoch,
                computation: None,
            },
            payload,
            root_control,
            stage_control,
            computation: None,
        })
    }
}
const ROOT_STAGE_BASE_CHANGES: usize = 4;
impl RootStageSource {
    /// Reuse the SAME actual Client computation window. A weak fence adds no
    /// second position; the caller must keep the original window alive.
    pub fn for_client_window(
        mut self,
        window: &crate::ResultWindowAlias,
    ) -> Result<Self, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        self.computation_window = Some(window.client_computation_window(&WorkScope {
            inner,
            id: self.root,
        })?);
        Ok(self)
    }

    /// The original Root and its distinct execution stage each retain one
    /// complete fixed control record until their actual owner exits.
    pub const fn minimum_control_records() -> usize {
        2
    }

    /// Actual closed batch shape before appending canonical Production rows.
    /// This is a pure backing fact; it neither captures a source nor grants.
    pub const fn minimum_change_records() -> usize {
        ROOT_STAGE_BASE_CHANGES
    }

    /// Exact original New record, shared by composition and end_stage. No
    /// CPU, physical job or queue resource is implied by this computation pin.
    pub fn computation_progress() -> JointResources {
        computation_amounts()
    }

    /// Actual current originals in this closed action, including every
    /// production row of both exact works. This is a fact, not maxL policy.
    pub fn required_change_records(&self) -> Result<usize, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let state = inner.state.lock().unwrap();
        validate_nodes(&state, self.root, self.fence.stage)?;
        if self.fence.canonical {
            // Already inherited groups follow the canonical Root without a
            // per-row request expansion. Only a first sealed Stage migration
            // needs its actually existing local production records.
            let slot = state.nodes[&self.fence.stage].cancellation;
            let id = state.cancellations.slots[slot.0]
                .claim
                .ok_or(WorkError::NotReady)?;
            if state.joint.as_ref().unwrap().claims[&id]
                .active_facts
                .key
                .origin
                .is_root(self.root)
            {
                Ok(ROOT_STAGE_BASE_CHANGES)
            } else {
                production_change_count(&state, &[self.fence.stage], ROOT_STAGE_BASE_CHANGES)
            }
        } else {
            production_change_count(
                &state,
                &[self.root, self.fence.stage],
                ROOT_STAGE_BASE_CHANGES,
            )
        }
    }
    /// Reserve the Root's original proposal cell before body allocation. Only
    /// PayloadBytes can grow here; no CPU/queue/physical geometry is guessed.
    pub fn begin(
        self,
        payload_delta_bytes: u64,
        lane: JointLane,
    ) -> Result<RootStageProposal, WorkError> {
        if !matches!(lane, JointLane::Foreground | JointLane::Settlement) {
            return Err(WorkError::Conflict);
        }
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        self.validate_writer_capture(&inner)?;
        let root = WorkScope {
            inner: Arc::clone(&inner),
            id: self.root,
        };
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&inner),
        };
        let proposal = if self.fence.canonical {
            match handle.try_begin_root_writer_signal(root, lane, self.writer_revision)? {
                JointTryBegin::Started(proposal) => proposal,
                JointTryBegin::Full(_) => {
                    return Err(WorkError::Capacity("original Root writer is busy"));
                }
            }
        } else {
            handle.begin_paid_proposal_for_scope(root)?
        };
        self.finish_begin(proposal, payload_delta_bytes, lane)
    }
    /// Same New-computation constructor with typed unacquired-Q backpressure.
    /// Full has no paid intent/ticket; callers keep the same delta and positive
    /// holders before re-capturing this pure source after the original signal.
    pub fn try_begin(
        self,
        payload_delta_bytes: u64,
        lane: JointLane,
    ) -> Result<JointTryBegin<RootStageProposal, JointProposalSlotSignal>, WorkError> {
        if !matches!(lane, JointLane::Foreground | JointLane::Settlement) {
            return Err(WorkError::Conflict);
        }
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        self.validate_writer_capture(&inner)?;
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&inner),
        };
        let root = WorkScope {
            inner,
            id: self.root,
        };
        let begin = if self.fence.canonical {
            handle.try_begin_root_writer_signal(root, lane, self.writer_revision)?
        } else {
            handle.try_begin_paid_proposal_for_scope_signal(root)?
        };
        match begin {
            JointTryBegin::Full(signal) => Ok(JointTryBegin::Full(signal)),
            JointTryBegin::Started(proposal) => self
                .finish_begin(proposal, payload_delta_bytes, lane)
                .map(JointTryBegin::Started),
        }
    }
    fn validate_writer_capture(&self, inner: &Arc<Inner>) -> Result<(), WorkError> {
        if !self.fence.canonical {
            return Ok(());
        }
        let state = inner.state.lock().unwrap();
        validate_nodes(&state, self.root, self.fence.stage)?;
        for (view, work) in [
            (&self.payload, self.root),
            (&self.root_control, self.root),
            (&self.stage_control, self.fence.stage),
        ]
        .into_iter()
        .chain(
            self.computation
                .as_ref()
                .map(|view| (view, self.fence.stage)),
        ) {
            root_domains::validate_captured(&state, self.root, view, work, self.writer_revision)?;
        }
        Ok(())
    }
    fn finish_begin(
        self,
        mut proposal: JointProposal,
        payload_delta_bytes: u64,
        lane: JointLane,
    ) -> Result<RootStageProposal, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let stage_facts = {
            let state = inner.state.lock().unwrap();
            validate_nodes(&state, self.root, self.fence.stage)?;
            if self.fence.canonical {
                root_domains::validate_construction(
                    &state,
                    self.root,
                    proposal.custody.as_ref().and_then(|c| c.key),
                )?;
            }
            let ledger = state.joint.as_ref().unwrap();
            for (source, work) in [
                (&self.payload, self.root),
                (&self.root_control, self.root),
                (&self.stage_control, self.fence.stage),
            ]
            .into_iter()
            .chain(
                self.computation
                    .as_ref()
                    .map(|source| (source, self.fence.stage)),
            ) {
                let row = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                authenticate_scope(&state, row, source, work)?;
                if row.pending.is_some() {
                    return Err(WorkError::Conflict);
                }
                row.structure_revision
                    .checked_add(2)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            (
                Arc::clone(&ledger.claims[&self.stage_control.id].active_facts),
                self.computation.as_ref().map(|source| {
                    (
                        Arc::clone(&ledger.claims[&self.root_control.id].active_facts),
                        Arc::clone(&ledger.claims[&source.id].active_facts),
                    )
                }),
            )
        };
        proposal.root_stage = Some(self.fence);
        proposal.computation_window = self.computation_window;
        proposal.begin_grow(
            self.payload,
            JointResources::ZERO.with(JointResource::PayloadBytes, payload_delta_bytes),
        )?;
        let (stage_facts, binding_facts) = stage_facts;
        if let Some((root_facts, compute_facts)) = binding_facts {
            // Binding the complete manifest is a full union for all four rows,
            // including old stage-only/compute facts before any new origin.
            merge(&mut proposal, &root_facts.dependencies)?;
            merge(&mut proposal, &stage_facts.dependencies)?;
            merge(&mut proposal, &compute_facts.dependencies)?;
        }
        Ok(RootStageProposal {
            proposal,
            root_control: self.root_control,
            stage_control: self.stage_control,
            stage_facts,
            computation: self.computation,
            lane,
            phase: 0,
        })
    }
}

/// Closed phases in one original paid body. The first group selects payload
/// dependencies; the second selects additional stage-only dependencies.
#[must_use = "Finish both closed phases and submit, or drop the same proposal"]
pub struct RootStageProposal {
    root_control: JointClaimView,
    stage_control: JointClaimView,
    stage_facts: Arc<ResponsibilityFacts>,
    computation: Option<JointClaimView>,
    lane: JointLane,
    phase: u8,
    // Its original final exit observer must follow all outer fact aliases.
    proposal: JointProposal,
}
impl RootStageProposal {
    fn require(&mut self, phase: u8) -> Result<(), WorkError> {
        self.proposal.healthy()?;
        if self.phase != phase {
            self.proposal.failed = Some(WorkError::Conflict);
            return Err(WorkError::Conflict);
        }
        Ok(())
    }
    pub fn push_payload_dependency(&mut self, fact: &SharedDependency) -> Result<(), WorkError> {
        self.require(0)?;
        self.proposal.push_dependency(fact)
    }
    pub fn push_payload_selected_network(
        &mut self,
        fact: SelectedNetworkOrigin<'_>,
    ) -> Result<(), WorkError> {
        self.require(0)?;
        self.proposal.push_selected_network(fact)
    }
    pub fn end_payload(&mut self) -> Result<(), WorkError> {
        self.require(0)?;
        self.proposal.arm()?;
        let result = (|| {
            self.proposal.end()?;
            self.proposal.begin(
                Some(self.root_control.clone()),
                JointLane::Custody,
                JointResources::ZERO,
                false,
            )?;
            let payload_facts = Arc::clone(&self.proposal.records[0].claim.facts);
            merge(&mut self.proposal, &payload_facts.dependencies)?;
            if !self.proposal.root_stage.unwrap().canonical {
                merge(&mut self.proposal, &self.stage_facts.dependencies)?;
            }
            if self.proposal.root_stage.unwrap().canonical {
                let active = {
                    let state = self.proposal.scope.inner.state.lock().unwrap();
                    let root = state.nodes[&self.proposal.scope.id]
                        .root_domains
                        .as_ref()
                        .ok_or(WorkError::Conflict)?;
                    if root.pending.is_some() {
                        return Err(WorkError::Conflict);
                    }
                    Arc::clone(&root.active)
                };
                merge(&mut self.proposal, &active)?;
            }
            self.phase = 1;
            Ok(())
        })();
        self.proposal.remember(result)
    }
    pub fn push_stage_dependency(&mut self, fact: &SharedDependency) -> Result<(), WorkError> {
        self.require(1)?;
        self.proposal.push_dependency(fact)
    }
    pub fn push_stage_selected_network(
        &mut self,
        fact: SelectedNetworkOrigin<'_>,
    ) -> Result<(), WorkError> {
        self.require(1)?;
        self.proposal.push_selected_network(fact)
    }
    pub fn end_stage(&mut self) -> Result<(), WorkError> {
        self.require(1)?;
        self.proposal.arm()?;
        let result = (|| {
            self.proposal.end()?;
            let full = Arc::clone(&self.proposal.records[1].claim.facts);
            let first = self.stage_facts.dependencies.is_empty();
            self.proposal.begin(
                Some(self.stage_control.clone()),
                JointLane::Custody,
                JointResources::ZERO,
                first,
            )?;
            merge(&mut self.proposal, &full.dependencies)?;
            self.proposal.end()?;
            match self.computation.as_ref() {
                Some(source) => self.proposal.begin(
                    Some(source.clone()),
                    self.lane,
                    JointResources::ZERO,
                    false,
                )?,
                None => self
                    .proposal
                    .begin(None, self.lane, computation_amounts(), false)?,
            }
            merge(&mut self.proposal, &full.dependencies)?;
            self.proposal.end()?;
            if self.proposal.root_stage.unwrap().canonical {
                let epoch = {
                    let state = self.proposal.scope.inner.state.lock().unwrap();
                    state.nodes[&self.proposal.scope.id]
                        .root_domains
                        .as_ref()
                        .ok_or(WorkError::Conflict)?
                        .epoch
                };
                self.proposal.root_domain = Some(RootDomainChange {
                    root: self.proposal.scope.id,
                    epoch,
                    candidate: Arc::clone(&full.dependencies),
                    lane: self.lane,
                });
                for index in [0usize, 1] {
                    let record = &mut self.proposal.records[index];
                    record.claim.facts = Arc::clone(record.original_facts.as_ref().unwrap());
                }
                // Birth preserves the actual stage-local S. Manifest routes
                // extend the Root once; they never rewrite an existing S.
                let root_origin = self.proposal.records[1].claim.facts.key.origin;
                let stage = &mut self.proposal.records[2];
                let local = stage.original_facts.as_ref().unwrap();
                stage.claim.facts = Arc::new(ResponsibilityFacts {
                    key: Arc::new(FactKey {
                        origin: root_origin,
                        class: local.key.class,
                        lane: local.key.lane,
                        source_kind: local.key.source_kind,
                        dependencies: local.key.dependencies.clone(),
                    }),
                    class: local.class,
                    lane: local.lane,
                    dependencies: Arc::clone(&local.dependencies),
                });
                let local = Arc::clone(&stage.claim.facts);
                let compute = &mut self.proposal.records[3];
                compute.claim.facts = if let Some(original) = &compute.original_facts {
                    Arc::clone(original)
                } else {
                    Arc::new(ResponsibilityFacts {
                        key: Arc::new(FactKey {
                            origin: local.key.origin,
                            class: local.key.class,
                            lane: self.lane as usize,
                            source_kind: SourceKind::Plain,
                            dependencies: local.key.dependencies.clone(),
                        }),
                        class: local.class,
                        lane: self.lane,
                        dependencies: Arc::clone(&local.dependencies),
                    })
                };
            }
            self.phase = 2;
            Ok(())
        })();
        self.proposal.remember(result)
    }
    pub fn submit(self) -> Result<RootStageAdmission, JointAdmissionFailure> {
        if self.phase != 2 {
            return Err(JointAdmissionFailure {
                error: WorkError::Conflict,
            });
        }
        let Self {
            proposal,
            root_control,
            stage_control,
            stage_facts,
            ..
        } = self;
        drop(root_control);
        drop(stage_control);
        drop(stage_facts);
        proposal
            .submit()
            .map(|admission| RootStageAdmission { admission })
    }
}
pub(super) fn merge(
    proposal: &mut JointProposal,
    references: &[DependencyRef],
) -> Result<(), WorkError> {
    let current = proposal.current.as_mut().ok_or(WorkError::Conflict)?;
    for reference in references {
        if let Err(position) = current
            .selected
            .binary_search_by_key(&reference.ordinal, |r| r.ordinal)
        {
            current.selected.insert(position, reference.clone());
        }
    }
    Ok(())
}
fn computation_amounts() -> JointResources {
    JointResources::ZERO
        .with(JointResource::ClaimRecords, 1)
        .with(JointResource::Computations, 1)
}
fn includes(full: &[DependencyRef], narrow: &[DependencyRef]) -> bool {
    narrow.iter().all(|fact| {
        full.binary_search_by_key(&fact.ordinal, |f| f.ordinal)
            .is_ok()
    })
}

// Shape is closed in the original request. Revisions/pending fences are checked
// by the original queue/proof/commit authors, including the actual child owner.
pub(in crate::joint_admission::ledger) fn validate_root_stage(
    state: &State,
    root: WorkId,
    fence: RootStageFence,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    validate_nodes(state, root, fence.stage)?;
    if changes.len() < ROOT_STAGE_BASE_CHANGES
        || changes.iter().skip(ROOT_STAGE_BASE_CHANGES).any(|change| {
            change
                .source
                .as_ref()
                .is_none_or(|source| !matches!(source.source, ClaimSource::Production { .. }))
        })
    {
        return Err(WorkError::Conflict);
    }
    validate_control_productions(state, changes)?;
    let ledger = state.joint.as_ref().unwrap();
    let class = state.nodes[&root].class;
    let payload = &changes[0];
    let source = payload.source.as_ref().ok_or(WorkError::Conflict)?;
    let row = ledger
        .claims
        .get(&fence.payload)
        .ok_or(WorkError::Released)?;
    if source.id != fence.payload
        || !matches!(source.source, ClaimSource::Payload { epoch } if epoch == fence.epoch)
        || payload.old != Some(source.id)
        || row.owner != ClaimOwner::PhysicalWork(root)
        || !matches!(row.custody, ClaimCustody::Payload { epoch, pins } if epoch == fence.epoch && pins != 0)
        || payload.claim.facts.lane != JointLane::Custody
    {
        return Err(WorkError::Conflict);
    }
    let old = payload.original_amounts.ok_or(WorkError::Conflict)?;
    let delta = payload.claim.amounts.checked_sub(old)?;
    if delta
        != JointResources::ZERO.with(
            JointResource::PayloadBytes,
            delta.get(JointResource::PayloadBytes),
        )
        || payload
            .original_facts
            .as_ref()
            .is_none_or(|f| !includes(&payload.claim.facts.dependencies, &f.dependencies))
    {
        return Err(WorkError::Conflict);
    }
    if !fence.canonical
        && fence.computation.is_some()
        && (delta != JointResources::ZERO
            || changes[0].claim.facts.key.dependencies != changes[1].claim.facts.key.dependencies)
    {
        return Err(WorkError::Conflict);
    }
    let full_index = if fence.canonical { 2 } else { 1 };
    let full = &changes[full_index].claim.facts.dependencies;
    if !fence.canonical && (full.is_empty() || !includes(full, &payload.claim.facts.dependencies)) {
        return Err(WorkError::Conflict);
    }
    for (change, work) in [(&changes[1], root), (&changes[2], fence.stage)] {
        let source = change.source.as_ref().ok_or(WorkError::Conflict)?;
        let ClaimSource::NodeControl {
            work: original,
            slot,
            generation,
        } = source.source
        else {
            return Err(WorkError::Conflict);
        };
        let node = &state.nodes[&work];
        let cell = &state.cancellations.slots[node.cancellation.0];
        if original != work
            || slot != node.cancellation
            || generation != cell.generation
            || cell.phase != SlotPhase::Node(work)
            || cell.claim != Some(source.id)
            || change.old != Some(source.id)
            || change.original_amounts != Some(cell.base)
            || change.claim.amounts != cell.base
            || change.claim.facts.lane != JointLane::Custody
            || if fence.canonical {
                change
                    .original_facts
                    .as_ref()
                    .is_none_or(|old| old.key.dependencies != change.claim.facts.key.dependencies)
                    || change.claim.facts.key.origin != root_domains::origin(state, root)?
            } else {
                change.claim.facts.key.dependencies != changes[1].claim.facts.key.dependencies
                    || change
                        .original_facts
                        .as_ref()
                        .is_none_or(|f| !includes(full, &f.dependencies))
            }
        {
            return Err(WorkError::Conflict);
        }
    }
    let compute = &changes[3];
    let compute_shape = match fence.computation {
        None => {
            compute.old.is_none()
                && compute.source.is_none()
                && compute.claim.amounts == computation_amounts()
        }
        Some([id, epoch]) => {
            let row = ledger.claims.get(&id).ok_or(WorkError::Released)?;
            compute.old == Some(id) && compute.source.as_ref().is_some_and(|source| {
                source.id == id
                    && matches!(source.source, ClaimSource::Computation { epoch: e } if e == epoch)
            })
                && matches!(row.custody, ClaimCustody::Computation { epoch: e, pins } if e == epoch && pins != 0)
                && row.owner == ClaimOwner::PhysicalWork(fence.stage)
                && compute.original_amounts == Some(compute.claim.amounts)
                && compute.claim.amounts == row.active_amounts
                && compute.original_facts.as_ref().is_some_and(|old| {
                    if fence.canonical {
                        old.key == compute.claim.facts.key
                    } else {
                        includes(full, &old.dependencies)
                    }
                })
        }
    };
    if !compute_shape
        || !matches!(
            compute.claim.facts.lane,
            JointLane::Foreground | JointLane::Settlement
        )
        || (!fence.canonical
            && compute.claim.facts.key.dependencies != changes[1].claim.facts.key.dependencies)
        || (fence.canonical && compute.claim.facts.key.origin != root_domains::origin(state, root)?)
        || changes.iter().any(|c| c.claim.facts.class != class)
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

/// A sealed future: the ordinary raw New claim/adjustment never escape.
#[must_use = "Keep the same original admission through its verdict"]
pub struct RootStageAdmission {
    admission: JointAdmission,
}
impl Future for RootStageAdmission {
    type Output = Result<RootStageReady, JointAdmissionFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.admission)
            .poll(cx)
            .map(|result| result.map(|batch| RootStageReady { batch }))
    }
}
/// Uninstalled capacity cannot be used or extracted. Drop withdraws its exact
/// provisional New and old deltas through the original RAII receipt authors.
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::RootStageReady;
/// fn no_extract(ready: RootStageReady) { let _ = ready.new_claims; }
/// ```
#[must_use = "Install atomically into the original receivers before use"]
pub struct RootStageReady {
    batch: JointGrantedBatch,
}
impl RootStageReady {
    pub fn install_in(
        &mut self,
        root: &WorkScope,
        payload: &RetainedPayloadGrant,
        stage: &WorkOwner,
        destination: &mut Option<InternalComputationGrant>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        if destination.is_some() {
            return Err(WorkError::Conflict);
        }
        let child = stage.scope();
        let receipt = self.batch.adjustment.as_mut().ok_or(WorkError::Released)?;
        let id = receipt.id.ok_or(WorkError::Released)?;
        let inner = Arc::clone(&receipt.inner);
        if !Arc::ptr_eq(&inner, &root.inner) || !Arc::ptr_eq(&inner, &child.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if self.batch.new_claims.len() != 1 {
            return Err(WorkError::Conflict);
        }
        let original = &mut self.batch.new_claims[0];
        let compute = original.id.ok_or(WorkError::Released)?;
        let retired_root = inner.update_facts_silent(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
            if let RequestPhase::Failed(error) = &request.phase {
                return Err(error.clone());
            }
            if !matches!(request.phase, RequestPhase::Delivered)
                || request.owner != root.id
                || request.waker.is_some()
            {
                return Err(WorkError::Conflict);
            }
            let fence = request.root_stage.ok_or(WorkError::Conflict)?;
            if fence.computation.is_some()
                || fence.stage != child.id
                || !same_payload_grant(payload, &inner, fence.payload, fence.epoch)
            {
                return Err(WorkError::Conflict);
            }
            let changes = request.changes.clone();
            validate_root_stage(state, root.id, fence, &changes)?;
            check_adjusted_sources(state, root.id, fence, &changes, id)?;
            if fence.canonical {
                root_domains::validate_install(state, id, root.id)?;
            }
            let row = ledger.claims.get(&compute).ok_or(WorkError::Released)?;
            let epoch = row
                .structure_revision
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            if !Arc::ptr_eq(&original.inner, &inner)
                || !row.custody.is_raw()
                || row.owner != ClaimOwner::PhysicalWork(child.id)
                || row.pending.is_some()
                || row.amounts != computation_amounts()
                || row.active_amounts != computation_amounts()
                || row.facts.key != changes[3].claim.facts.key
                || row.active_facts.key != row.facts.key
            {
                return Err(WorkError::Conflict);
            }
            if let Some(window) = &row.computation_window {
                window.validate_claim(compute)?;
            }
            // All fallible checks precede the first move. No callbacks, raw
            // aliases or old pin changes occur during this original commit.
            commit_adjusted_sources(state, &changes, fence.canonical);
            state
                .joint
                .as_mut()
                .unwrap()
                .claims
                .get_mut(&compute)
                .unwrap()
                .domain_birth = None;
            let grant = install_computation_pin(&inner, state, compute, epoch);
            *destination = Some(grant);
            original.id = None;
            let retired = if fence.canonical {
                root_domains::install(state, id)
            } else {
                None
            };
            remove_request(state, id);
            Ok(retired)
        })?;
        drop(retired_root);
        receipt.id = None;
        Ok(crate::WorkloadProgressNotification::new(inner))
    }
}

fn check_adjusted_sources(
    state: &State,
    root: WorkId,
    fence: RootStageFence,
    changes: &[ChangeRecord],
    id: u64,
) -> Result<(), WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    for change in changes.iter().filter(|change| change.old.is_some()) {
        let source = change.source.as_ref().unwrap();
        let row = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
        let observation = fence.canonical && root_domains::observation(change);
        let mut committed = source.clone();
        committed.structure_revision = committed
            .structure_revision
            .checked_add(u64::from(!observation))
            .ok_or(WorkError::ArithmeticOverflow)?;
        authenticate_scope(
            state,
            row,
            &committed,
            source_work(root, Some(fence), source),
        )?;
        if row.pending != if observation { None } else { Some(id) }
            || row.amounts != change.claim.amounts
            || row.facts.key != change.claim.facts.key
            || row.active_amounts != change.original_amounts.unwrap()
            || row.active_facts.key != change.original_facts.as_ref().unwrap().key
        {
            return Err(WorkError::Conflict);
        }
        row.structure_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    Ok(())
}
fn commit_adjusted_sources(state: &mut State, changes: &[ChangeRecord], canonical: bool) {
    for change in changes.iter().filter(|change| change.old.is_some()) {
        if canonical && root_domains::observation(change) {
            continue;
        }
        let row = state
            .joint
            .as_mut()
            .unwrap()
            .claims
            .get_mut(&change.old.unwrap())
            .unwrap();
        row.active_facts = Arc::clone(&row.facts);
        row.active_amounts = row.amounts;
        row.pending = None;
        row.structure_revision += 1;
    }
}

mod internal_binding;
pub use internal_binding::InternalManifestBinding;

#[cfg(test)]
pub(super) mod tests;
