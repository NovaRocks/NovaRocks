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

//! Original Root Native action and independent canonical-reply custody.
use super::*;
use crate::result_window::NativeCapacityFence;
use std::num::NonZeroU64;

/// First-party composition input from the original Native request/reply
/// builders. This describes requested backing and grants no capacity. It is
/// not a finite-source certificate and never changes causal protection.
#[derive(Clone, Copy, Debug)]
pub struct NativeAsyncActionQuote {
    items: NonZeroU64,
    action_metadata: NonZeroU64,
    input_bytes: NonZeroU64,
    queue_bytes: NonZeroU64,
    reply_metadata: NonZeroU64,
    reply_bytes: NonZeroU64,
}
impl NativeAsyncActionQuote {
    pub fn for_original_native_transport(
        items: NonZeroU64,
        action_metadata: NonZeroU64,
        input_bytes: NonZeroU64,
        queue_bytes: NonZeroU64,
        reply_metadata: NonZeroU64,
        reply_bytes: NonZeroU64,
    ) -> Self {
        Self {
            items,
            action_metadata,
            input_bytes,
            queue_bytes,
            reply_metadata,
            reply_bytes,
        }
    }
    pub fn action(self, lane: JointLane) -> Result<JointResources, WorkError> {
        if !matches!(lane, JointLane::Foreground | JointLane::Settlement) {
            return Err(WorkError::Conflict);
        }
        Ok(JointResources::ZERO
            .with(JointResource::ClaimRecords, 1)
            .with(JointResource::AsyncIo, 1)
            .with(JointResource::PhysicalQueueRecords, 1)
            .with(JointResource::MetadataBytes, self.action_metadata.get())
            .with(JointResource::PayloadBytes, self.input_bytes.get())
            .with(JointResource::PhysicalQueueBytes, self.queue_bytes.get()))
    }
    pub fn custody(self) -> JointResources {
        JointResources::ZERO
            .with(JointResource::ClaimRecords, 1)
            .with(JointResource::ReplyRecords, self.items.get())
            .with(JointResource::MetadataBytes, self.reply_metadata.get())
            .with(JointResource::PayloadBytes, self.reply_bytes.get())
    }
}
/// Actual original input builder's requested new backing. No physical or
/// execution position is acquired in this prequeue custody phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeQueuedInputQuote {
    metadata: NonZeroU64,
    payload: u64,
}
impl NativeQueuedInputQuote {
    pub fn for_original_native_input(metadata: NonZeroU64, payload: u64) -> Self {
        Self { metadata, payload }
    }
    pub fn custody(self) -> JointResources {
        JointResources::ZERO
            .with(JointResource::ClaimRecords, 1)
            .with(JointResource::QueueRecords, 1)
            .with(JointResource::MetadataBytes, self.metadata.get())
            .with(JointResource::PayloadBytes, self.payload)
    }
}
#[derive(Clone, Copy)]
enum NativeSourceQuote {
    Action(NativeAsyncActionQuote),
    Queued(NativeQueuedInputQuote),
}
#[derive(Clone)]
pub(in crate::joint_admission::ledger) struct NativeNodeFence {
    work: WorkId,
    claim: u64,
    slot: SlotId,
    generation: u64,
    structure_revision: u64,
    release_revision: u64,
}
#[derive(Clone)]
pub(in crate::joint_admission::ledger) struct NativeActionFence {
    root: NativeNodeFence,
    execution: NativeNodeFence,
    lane: JointLane,
    capacity: NativeCapacityFence,
    quote: NativeSourceQuote,
}
/// Reusable weak delegation from the actual original Root result owner.
/// Each new proposal captures current control revisions; an existing ticket
/// is never reminted or refreshed when the Root dependency union changes.
#[derive(Clone)]
pub struct NativeAsyncActionAuthority {
    inner: Weak<Inner>,
    owner: WorkId,
    execution: WorkId,
    capacity: NativeCapacityFence,
}
impl std::fmt::Debug for NativeAsyncActionAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginalNativeActionAuthority")
    }
}
impl NativeAsyncActionAuthority {
    /// Capture one action on this original Native source. The quote describes
    /// its actual owned backing and next increment; it does not grant capacity.
    pub fn action_source(
        &self,
        lane: JointLane,
        quote: NativeAsyncActionQuote,
    ) -> Result<NativeAsyncActionSource, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        capture_native_action(
            &WorkScope {
                inner,
                id: self.owner,
            },
            &WorkScope {
                inner: Arc::clone(&self.inner.upgrade().ok_or(WorkError::Released)?),
                id: self.execution,
            },
            self.capacity.clone(),
            lane,
            quote,
        )
    }
}
pub(crate) fn delegate_native_actions(
    scope: &WorkScope,
    execution: &WorkScope,
    capacity: NativeCapacityFence,
) -> Result<NativeAsyncActionAuthority, WorkError> {
    if !Arc::ptr_eq(&scope.inner, &execution.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    {
        let state = scope.inner.state.lock().unwrap();
        let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
        if node.owner != crate::OwnerState::Active || node.parent.is_some() || node.root != scope.id
        {
            return Err(WorkError::Conflict);
        }
        validate_execution(&state, scope.id, execution.id)?;
        capacity.validate(&state, scope.id)?;
    }
    Ok(NativeAsyncActionAuthority {
        inner: Arc::downgrade(&scope.inner),
        owner: scope.id,
        execution: execution.id,
        capacity,
    })
}
pub struct NativeAsyncActionSource {
    inner: Weak<Inner>,
    owner: WorkId,
    fence: NativeActionFence,
}
#[must_use = "Retain this same original paid ticket in the pending batch"]
pub struct NativeAsyncActionAdmission {
    admission: Option<JointAdmission>,
    scope: WorkScope,
    fence: NativeActionFence,
    slot_wait: Option<JointProposalSlotSignal>,
}
impl NativeAsyncActionAdmission {
    pub fn is_waiting_for_proposal_slot(&self) -> bool {
        self.admission.is_none()
    }

    fn poll_constructor(
        &mut self,
        cx: &mut Context<'_>,
        observe: &mut impl FnMut(
            &JointProposalSlotSignal,
            &mut Context<'_>,
        ) -> Poll<Result<(), WorkError>>,
    ) -> Poll<Result<(), JointAdmissionFailure>> {
        if self.admission.is_some() {
            return Poll::Ready(Ok(()));
        }
        if let Some(wait) = self.slot_wait.as_mut() {
            match observe(wait, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(JointAdmissionFailure { error }));
                }
                Poll::Ready(Ok(())) => {
                    self.slot_wait = None;
                }
            }
        }
        let source = NativeAsyncActionSource {
            inner: Arc::downgrade(&self.scope.inner),
            owner: self.scope.id,
            fence: self.fence.clone(),
        };
        match source.try_begin()? {
            JointTryBegin::Started(mut original) => {
                self.admission = original.admission.take();
                Poll::Ready(Ok(()))
            }
            JointTryBegin::Full(wait) => {
                self.slot_wait = Some(wait);
                match observe(self.slot_wait.as_ref().unwrap(), cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(error)) => Poll::Ready(Err(JointAdmissionFailure { error })),
                    Poll::Ready(Ok(())) => {
                        // A genuine event raced the failed reservation. Keep
                        // the same source, make only one attempt this turn.
                        self.slot_wait = None;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
        }
    }

    pub fn adopt_action_in(
        &self,
        source: &mut Option<JointClaim>,
        target: &mut Option<NativeAsyncActionGrant>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        typed_custody::adopt_native_action_in(
            &WorkScope {
                inner: Arc::clone(&self.scope.inner),
                id: self.fence.execution.work,
            },
            &self.fence,
            source,
            target,
        )
    }
    pub fn adopt_reply_in(
        &self,
        source: &mut Option<JointClaim>,
        target: &mut Option<NativeReplyGrant>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        typed_custody::adopt_native_reply_in(&self.scope, &self.fence, source, target)
    }
    pub fn poll_local_offer(
        &mut self,
        cx: &mut Context<'_>,
        observe: &mut impl FnMut(
            &JointProposalSlotSignal,
            &mut Context<'_>,
        ) -> Poll<Result<(), WorkError>>,
    ) -> Poll<Result<JointLocalOffer<'_>, JointAdmissionFailure>> {
        match self.poll_constructor(cx, observe) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        self.admission
            .as_mut()
            .expect("original Native constructor started")
            .poll_local_offer(cx)
    }
}
fn validate_execution(state: &State, root: WorkId, execution: WorkId) -> Result<(), WorkError> {
    let original = state.nodes.get(&root).ok_or(WorkError::Released)?;
    if original.owner != crate::OwnerState::Active
        || original.parent.is_some()
        || original.root != root
    {
        return Err(WorkError::Released);
    }
    let mut cursor = execution;
    for _ in 0..=state.nodes.len() {
        let node = state.nodes.get(&cursor).ok_or(WorkError::Released)?;
        if node.owner != crate::OwnerState::Active
            || node.root != root
            || node.class != original.class
        {
            return Err(WorkError::Conflict);
        }
        if cursor == root {
            return Ok(());
        }
        cursor = node.parent.ok_or(WorkError::Conflict)?;
    }
    Err(WorkError::Conflict)
}
fn capture_node(scope: &WorkScope) -> Result<NativeNodeFence, WorkError> {
    let (view, _) = control_claim_view_for_scope(scope, false)?;
    let ClaimSource::NodeControl {
        slot, generation, ..
    } = view.source
    else {
        return Err(WorkError::Conflict);
    };
    Ok(NativeNodeFence {
        work: scope.id,
        claim: view.id,
        slot,
        generation,
        structure_revision: view.structure_revision,
        release_revision: view.release_revision,
    })
}
fn node_facts<'a>(
    state: &'a State,
    node_fence: &NativeNodeFence,
    foreground: bool,
) -> Result<&'a Arc<ResponsibilityFacts>, WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    let owner = node_fence.work;
    let node = state.nodes.get(&owner).ok_or(WorkError::Released)?;
    let slot = state
        .cancellations
        .slots
        .get(node_fence.slot.0)
        .ok_or(WorkError::Released)?;
    let claim = ledger
        .claims
        .get(&node_fence.claim)
        .ok_or(WorkError::Released)?;
    if node.owner != crate::OwnerState::Active
        || !state.cancellations.fixed
        || node.cancellation != node_fence.slot
        || slot.phase != SlotPhase::Node(owner)
        || slot.generation != node_fence.generation
        || slot.claim != Some(node_fence.claim)
        || claim.owner != ClaimOwner::ControlSlot(node_fence.slot)
        || claim.structure_revision != node_fence.structure_revision
        || claim.release_revision != node_fence.release_revision
        || claim.pending.is_some()
        || claim.amounts != slot.base
        || claim.active_amounts != slot.base
        || claim.facts.key != claim.active_facts.key
        || claim.active_facts.class != node.class
        || !root_domains::has_effective_domains(state, &claim.active_facts, true)
        || !claim.active_facts.key.source_kind.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    if foreground {
        node.check(&state.cancellations)?;
        if state.closed {
            return Err(WorkError::Closed);
        }
    }
    Ok(&claim.active_facts)
}
fn source_facts<'a>(
    state: &'a State,
    owner: WorkId,
    fence: &NativeActionFence,
) -> Result<(&'a Arc<ResponsibilityFacts>, &'a Arc<ResponsibilityFacts>), WorkError> {
    if owner != fence.root.work {
        return Err(WorkError::Conflict);
    }
    validate_execution(state, owner, fence.execution.work)?;
    if fence.lane == JointLane::Foreground {
        fence.capacity.validate(state, owner)?;
    }
    // Settlement continues the original delegated responsibility after the
    // result holder leaves. It still needs both live original control nodes,
    // their exact slot/generation/revisions and a fresh paid Joint proof; it
    // neither retains nor reacquires the released result position.
    Ok((
        node_facts(state, &fence.root, fence.lane == JointLane::Foreground)?,
        node_facts(state, &fence.execution, fence.lane == JointLane::Foreground)?,
    ))
}
fn exact_union(
    facts: &ResponsibilityFacts,
    root: &ResponsibilityFacts,
    execution: &ResponsibilityFacts,
) -> bool {
    if root_domains::combined_origin(root, execution).ok() != Some(facts.key.origin) {
        return false;
    }
    let actual = &facts.key.dependencies;
    actual.iter().all(|key| {
        root.key.dependencies.binary_search(key).is_ok()
            || execution.key.dependencies.binary_search(key).is_ok()
    }) && root
        .key
        .dependencies
        .iter()
        .chain(execution.key.dependencies.iter())
        .all(|key| actual.binary_search(key).is_ok())
}
pub(in crate::joint_admission::ledger) fn validate_native_action(
    state: &State,
    owner: WorkId,
    fence: &NativeActionFence,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    let (facts, execution) = source_facts(state, owner, fence)?;
    let expected = match fence.quote {
        NativeSourceQuote::Action(quote) => [
            Some((quote.action(fence.lane)?, fence.lane)),
            Some((quote.custody(), JointLane::Custody)),
        ],
        NativeSourceQuote::Queued(quote) => [Some((quote.custody(), JointLane::Custody)), None],
    };
    if changes.len() != expected.iter().flatten().count() {
        return Err(WorkError::Conflict);
    }
    for (change, (amounts, lane)) in changes.iter().zip(expected.into_iter().flatten()) {
        if change.old.is_some()
            || change.source.is_some()
            || change.claim.amounts != amounts
            || change.claim.facts.class != facts.class
            || change.claim.facts.lane != lane
            || !exact_union(&change.claim.facts, facts, execution)
            || !change.claim.facts.key.source_kind.is_empty()
        {
            return Err(WorkError::Conflict);
        }
    }
    Ok(())
}
pub(crate) fn capture_native_action(
    scope: &WorkScope,
    execution: &WorkScope,
    capacity: NativeCapacityFence,
    lane: JointLane,
    quote: NativeAsyncActionQuote,
) -> Result<NativeAsyncActionSource, WorkError> {
    quote.action(lane)?;
    capture_native_source(
        scope,
        execution,
        capacity,
        lane,
        NativeSourceQuote::Action(quote),
    )
}
fn capture_native_source(
    scope: &WorkScope,
    execution: &WorkScope,
    capacity: NativeCapacityFence,
    lane: JointLane,
    quote: NativeSourceQuote,
) -> Result<NativeAsyncActionSource, WorkError> {
    if !matches!(lane, JointLane::Foreground | JointLane::Settlement) {
        return Err(WorkError::Conflict);
    }
    if !Arc::ptr_eq(&scope.inner, &execution.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    let fence = NativeActionFence {
        root: capture_node(scope)?,
        execution: capture_node(execution)?,
        lane,
        capacity,
        quote,
    };
    source_facts(&scope.inner.state.lock().unwrap(), scope.id, &fence)?;
    Ok(NativeAsyncActionSource {
        inner: Arc::downgrade(&scope.inner),
        owner: scope.id,
        fence,
    })
}
impl NativeAsyncActionSource {
    pub fn begin(self) -> Result<NativeAsyncActionAdmission, JointAdmissionFailure> {
        self.begin_inner()
            .map_err(|error| JointAdmissionFailure { error })
    }
    pub fn try_begin(
        &self,
    ) -> Result<
        JointTryBegin<NativeAsyncActionAdmission, JointProposalSlotSignal>,
        JointAdmissionFailure,
    > {
        let result = (|| {
            let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
            source_facts(&inner.state.lock().unwrap(), self.owner, &self.fence)?;
            let scope = WorkScope {
                inner: Arc::clone(&inner),
                id: self.owner,
            };
            let attempt = JointAdmissionHandle {
                inner: Arc::clone(&inner),
            }
            .try_begin_paid_proposal_for_scope_signal(scope.clone())?;
            source_facts(&inner.state.lock().unwrap(), self.owner, &self.fence)?;
            match attempt {
                JointTryBegin::Full(wait) => Ok(JointTryBegin::Full(wait)),
                JointTryBegin::Started(proposal) => self
                    .finish_begin(proposal, scope)
                    .map(JointTryBegin::Started),
            }
        })();
        result.map_err(|error| JointAdmissionFailure { error })
    }
    /// Keep the same source/fence before a ticket exists; only Q Full defers.
    pub fn begin_when_available(self) -> Result<NativeAsyncActionAdmission, JointAdmissionFailure> {
        match self.try_begin()? {
            JointTryBegin::Started(admission) => Ok(admission),
            JointTryBegin::Full(wait) => Ok(NativeAsyncActionAdmission {
                admission: None,
                scope: WorkScope {
                    inner: self.inner.upgrade().ok_or(JointAdmissionFailure {
                        error: WorkError::Released,
                    })?,
                    id: self.owner,
                },
                fence: self.fence,
                slot_wait: Some(wait),
            }),
        }
    }
    fn begin_inner(&self) -> Result<NativeAsyncActionAdmission, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        source_facts(&inner.state.lock().unwrap(), self.owner, &self.fence)?;
        let scope = WorkScope {
            inner: Arc::clone(&inner),
            id: self.owner,
        };
        let proposal = JointAdmissionHandle {
            inner: Arc::clone(&inner),
        }
        .begin_paid_proposal_for_scope(scope.clone())?;
        self.finish_begin(proposal, scope)
    }
    fn finish_begin(
        &self,
        mut proposal: JointProposal,
        scope: WorkScope,
    ) -> Result<NativeAsyncActionAdmission, WorkError> {
        let inner = Arc::clone(&scope.inner);
        let (facts, execution) = {
            let state = inner.state.lock().unwrap();
            let (root, stage) = source_facts(&state, self.owner, &self.fence)?;
            (Arc::clone(root), Arc::clone(stage))
        };
        let changes = match self.fence.quote {
            NativeSourceQuote::Action(quote) => [
                Some((self.fence.lane, quote.action(self.fence.lane)?)),
                Some((JointLane::Custody, quote.custody())),
            ],
            NativeSourceQuote::Queued(quote) => [Some((JointLane::Custody, quote.custody())), None],
        };
        proposal.native_action = Some(self.fence.clone());
        for (lane, amounts) in changes.into_iter().flatten() {
            proposal.begin_new(lane, amounts)?;
            let current = proposal.current.as_mut().expect("original Native change");
            current.origin = root_domains::combined_origin(&facts, &execution)?;
            for record in facts
                .dependencies
                .iter()
                .chain(execution.dependencies.iter())
            {
                if let Err(position) = current
                    .selected
                    .binary_search_by_key(&record.ordinal, |record| record.ordinal)
                {
                    current
                        .selected
                        .try_reserve(1)
                        .map_err(|_| WorkError::Capacity("Native action dependency allocation"))?;
                    current.selected.insert(position, record.clone());
                }
            }
            proposal.end_change()?;
        }
        Ok(NativeAsyncActionAdmission {
            admission: Some(proposal.submit().map_err(|e| e.error)?),
            scope,
            fence: self.fence.clone(),
            slot_wait: None,
        })
    }
}

/// Pure original-state validation used inside the same typed adoption lock.
pub(in crate::joint_admission::ledger) fn validate_native_fence(
    state: &State,
    scope: &WorkScope,
    fence: &NativeActionFence,
) -> Result<(), WorkError> {
    source_facts(state, fence.root.work, fence).and_then(|_| {
        if scope.id == fence.root.work {
            Ok(())
        } else {
            Err(WorkError::Conflict)
        }
    })
}
impl NativeAsyncActionAuthority {
    fn queued_source(
        &self,
        lane: JointLane,
        quote: NativeQueuedInputQuote,
    ) -> Result<NativeAsyncActionSource, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let scope = WorkScope {
            inner,
            id: self.owner,
        };
        capture_native_source(
            &scope,
            &WorkScope {
                inner: Arc::clone(&scope.inner),
                id: self.execution,
            },
            self.capacity.clone(),
            lane,
            NativeSourceQuote::Queued(quote),
        )
    }
    pub fn begin_queued_input(
        &self,
        lane: JointLane,
        quote: NativeQueuedInputQuote,
    ) -> Result<NativeQueuedInputAdmission, JointAdmissionFailure> {
        self.queued_source(lane, quote)
            .map_err(|error| JointAdmissionFailure { error })?
            .begin()
            .map(|original| NativeQueuedInputAdmission { original })
    }
    pub fn begin_queued_input_when_available(
        &self,
        lane: JointLane,
        quote: NativeQueuedInputQuote,
    ) -> Result<NativeQueuedInputAdmission, JointAdmissionFailure> {
        self.queued_source(lane, quote)
            .map_err(|error| JointAdmissionFailure { error })?
            .begin_when_available()
            .map(|original| NativeQueuedInputAdmission { original })
    }
}
#[must_use]
pub struct NativeQueuedInputAdmission {
    original: NativeAsyncActionAdmission,
}
impl NativeQueuedInputAdmission {
    pub fn is_waiting_for_proposal_slot(&self) -> bool {
        self.original.is_waiting_for_proposal_slot()
    }
    pub fn poll_with_slot_observer(
        &mut self,
        cx: &mut Context<'_>,
        observe: &mut impl FnMut(
            &JointProposalSlotSignal,
            &mut Context<'_>,
        ) -> Poll<Result<(), WorkError>>,
    ) -> Poll<Result<NativeQueuedInputReady, JointAdmissionFailure>> {
        match self.original.poll_constructor(cx, observe) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let result = Pin::new(self.original.admission.as_mut().unwrap()).poll(cx);
        result.map(|result| {
            result.map(|batch| NativeQueuedInputReady {
                original: None,
                batch,
                scope: self.original.scope.clone(),
                fence: self.original.fence.clone(),
            })
        })
    }
}
#[must_use]
pub struct NativeQueuedInputReady {
    original: Option<JointClaim>,
    batch: JointGrantedBatch,
    scope: WorkScope,
    fence: NativeActionFence,
}
impl NativeQueuedInputReady {
    pub fn install_in(
        &mut self,
        destination: &mut Option<NativeQueuedInputGrant>,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        if self.batch.adjustment.is_some() {
            return Err(WorkError::Conflict);
        }
        if self.original.is_none() {
            if self.batch.new_claims.len() != 1 {
                return Err(WorkError::Conflict);
            }
            self.original = self.batch.new_claims.pop();
        }
        typed_custody::adopt_native_input_in(
            &self.scope,
            &self.fence,
            &mut self.original,
            destination,
        )
    }
}

pub(in crate::joint_admission::ledger) fn validate_native_action_claim(
    state: &State,
    scope: &WorkScope,
    fence: &NativeActionFence,
    claim: &JointClaim,
    reply: bool,
) -> Result<(), WorkError> {
    let (root, execution) = source_facts(state, fence.root.work, fence)?;
    let NativeSourceQuote::Action(quote) = fence.quote else {
        return Err(WorkError::Conflict);
    };
    let row = state
        .joint
        .as_ref()
        .ok_or(WorkError::NotReady)?
        .claims
        .get(&claim.id.ok_or(WorkError::Released)?)
        .ok_or(WorkError::Released)?;
    let expected_owner = if reply {
        fence.root.work
    } else {
        fence.execution.work
    };
    let expected_amounts = if reply {
        quote.custody()
    } else {
        quote.action(fence.lane)?
    };
    if scope.id != expected_owner
        || row.owner != ClaimOwner::PhysicalWork(expected_owner)
        || row.active_amounts != expected_amounts
        || !exact_union(&row.active_facts, root, execution)
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}
impl NativeActionFence {
    pub(in crate::joint_admission::ledger) fn permits_original_settlement(&self) -> bool {
        self.lane == JointLane::Settlement
    }
    pub(in crate::joint_admission::ledger) fn physical_owner(&self, index: usize) -> WorkId {
        match (self.quote, index) {
            (NativeSourceQuote::Action(_), 0) => self.execution.work,
            _ => self.root.work,
        }
    }
}

impl NativeActionFence {
    pub(in crate::joint_admission::ledger) fn dispatch_origin(
        &self,
        ticket: u64,
        index: usize,
    ) -> typed_custody::NativeDispatchOrigin {
        let kind = match (self.quote, index) {
            (NativeSourceQuote::Action(_), 0) => typed_custody::NativeDispatchKind::Action,
            (NativeSourceQuote::Action(_), 1) => typed_custody::NativeDispatchKind::Reply,
            (NativeSourceQuote::Queued(_), 0) => typed_custody::NativeDispatchKind::Input,
            _ => unreachable!("validated original Native batch"),
        };
        typed_custody::NativeDispatchOrigin {
            root: self.root.work,
            execution: self.execution.work,
            ticket,
            kind,
            frozen: false,
            lane: self.lane,
        }
    }
}
