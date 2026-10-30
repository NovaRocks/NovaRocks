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

//! The original exact NodeControl source shared by blocking and async actions.

use super::*;
use std::num::NonZeroU64;

#[derive(Clone, Copy)]
pub(in crate::joint_admission::ledger) struct ControlActionFence {
    claim: u64,
    slot: SlotId,
    generation: u64,
    structure_revision: u64,
    release_revision: u64,
    lane: JointLane,
    kind: ControlActionKind,
    root_payload: Option<[u64; 2]>,
}

/// Permission to seek one E action, not a physical grant or finite CPU source.
/// Only the original active owner can capture its complete admitted D.
#[must_use = "Construct the paid action in the original stable job"]
pub(super) struct ControlActionSource {
    inner: Weak<Inner>,
    owner: WorkId,
    fence: ControlActionFence,
}
#[must_use = "Retain the same finite ticket through physical publication"]
pub(super) struct ControlActionAdmission {
    pub(super) admission: JointAdmission,
}

fn source_facts(
    state: &State,
    owner: WorkId,
    fence: ControlActionFence,
) -> Result<&Arc<ResponsibilityFacts>, WorkError> {
    let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
    if ledger.driver_closed {
        return Err(WorkError::Closed);
    }
    if let Some(key) = fence.root_payload {
        super::super::typed_custody::validate_native_payload_fence(state, owner, key)?;
    }
    let node = state.nodes.get(&owner).ok_or(WorkError::Released)?;
    if node.owner != crate::OwnerState::Active {
        return Err(WorkError::Released);
    }
    let slot = state
        .cancellations
        .slots
        .get(fence.slot.0)
        .ok_or(WorkError::Released)?;
    let claim = ledger.claims.get(&fence.claim).ok_or(WorkError::Released)?;
    if !state.cancellations.fixed
        || node.cancellation != fence.slot
        || slot.phase != SlotPhase::Node(owner)
        || slot.generation != fence.generation
        || slot.claim != Some(fence.claim)
        || claim.owner != ClaimOwner::ControlSlot(fence.slot)
        || claim.structure_revision != fence.structure_revision
        || claim.release_revision != fence.release_revision
        || claim.pending.is_some()
        || claim.amounts != slot.base
        || claim.active_amounts != slot.base
        || claim.facts.key != claim.active_facts.key
        || claim.active_facts.class != node.class
        || !root_domains::has_effective_domains(state, &claim.active_facts, true)
        || !claim.active_facts.key.source_kind.is_empty()
        || (matches!(fence.kind, ControlActionKind::Payload)
            && (node.parent.is_some() || fence.lane != JointLane::Foreground))
        || (!matches!(fence.kind, ControlActionKind::Payload)
            && !matches!(fence.lane, JointLane::Foreground | JointLane::Settlement))
    {
        return Err(WorkError::Conflict);
    }
    // This existing responsibility may settle after business cancellation.
    // No ordinary new business source can acquire the settlement policy.
    if fence.lane != JointLane::Settlement {
        node.check(&state.cancellations)?;
        if state.closed {
            return Err(WorkError::Closed);
        }
    }
    Ok(&claim.active_facts)
}
#[derive(Clone, Copy)]
pub(super) enum ControlActionKind {
    Blocking,
    Async,
    Payload,
}

impl WorkOwner {
    /// First-party lifecycle delegation. Settlement is only for this owner's
    /// already registered source/output/failure obligation, never new business.
    /// The original owner must remain Active through queue/offer/accept.
    pub(super) fn control_action_source(
        &self,
        lane: JointLane,
        kind: ControlActionKind,
    ) -> Result<ControlActionSource, WorkError> {
        let scope = self.scope();
        capture(&scope, lane, kind, None)
    }
}

fn capture(
    scope: &WorkScope,
    lane: JointLane,
    kind: ControlActionKind,
    root_payload: Option<[u64; 2]>,
) -> Result<ControlActionSource, WorkError> {
    let (view, _) = control_claim_view_for_scope(scope, false)?;
    let ClaimSource::NodeControl {
        slot, generation, ..
    } = view.source
    else {
        return Err(WorkError::Conflict);
    };
    let fence = ControlActionFence {
        claim: view.id,
        slot,
        generation,
        structure_revision: view.structure_revision,
        release_revision: view.release_revision,
        lane,
        kind,
        root_payload,
    };
    source_facts(&scope.inner.state.lock().unwrap(), scope.id, fence)?;
    Ok(ControlActionSource {
        inner: Arc::downgrade(&scope.inner),
        owner: scope.id,
        fence,
    })
}

pub(super) fn capture_payload_continuation(
    grant: &RetainedPayloadGrant,
    root: &WorkScope,
    lane: JointLane,
) -> Result<ControlActionSource, WorkError> {
    let state = root.inner.state.lock().unwrap();
    let payload = super::super::typed_custody::payload_growth_view(grant, root, &state)?;
    let ClaimSource::Payload { epoch } = payload.source else {
        return Err(WorkError::Conflict);
    };
    let key = [payload.id, epoch];
    drop(state);
    capture(root, lane, ControlActionKind::Async, Some(key))
}

pub(in crate::joint_admission::ledger) fn validate_control_action(
    state: &State,
    owner: WorkId,
    fence: ControlActionFence,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    let facts = source_facts(state, owner, fence)?;
    let [change] = changes else {
        return Err(WorkError::Conflict);
    };
    let amounts = change.claim.amounts;
    let expected = match fence.kind {
        ControlActionKind::Blocking => JointResources::ZERO
            .with(JointResource::BlockingJobs, 1)
            .with(JointResource::PhysicalQueueRecords, 1),
        ControlActionKind::Async => {
            JointResources::ZERO.with(JointResource::AsyncIo, amounts.get(JointResource::AsyncIo))
        }
        ControlActionKind::Payload => JointResources::ZERO.with(
            JointResource::PayloadBytes,
            amounts.get(JointResource::PayloadBytes),
        ),
    }
    .with(JointResource::ClaimRecords, 1)
    .with(
        JointResource::MetadataBytes,
        amounts.get(JointResource::MetadataBytes),
    )
    .with(
        JointResource::PhysicalQueueBytes,
        amounts.get(JointResource::PhysicalQueueBytes),
    );
    if change.old.is_some()
        || change.source.is_some()
        || amounts != expected
        || amounts.get(JointResource::MetadataBytes) == 0
        || match fence.kind {
            ControlActionKind::Blocking => amounts.get(JointResource::PhysicalQueueBytes) == 0,
            ControlActionKind::Async => {
                amounts.get(JointResource::AsyncIo) == 0
                    || amounts.get(JointResource::PhysicalQueueBytes) != 0
            }
            ControlActionKind::Payload => {
                amounts.get(JointResource::PayloadBytes) == 0
                    || amounts.get(JointResource::PhysicalQueueBytes) != 0
            }
        }
        || change.claim.facts.key.origin != facts.key.origin
        || change.claim.facts.key.class != facts.key.class
        || change.claim.facts.lane
            != match fence.kind {
                ControlActionKind::Payload => JointLane::Custody,
                _ => fence.lane,
            }
        || change.claim.facts.key.dependencies != facts.key.dependencies
        || !change.claim.facts.key.source_kind.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}
impl ControlActionSource {
    pub fn begin(
        self,
        metadata_bytes: NonZeroU64,
        position_or_queue_bytes: NonZeroU64,
    ) -> Result<ControlActionAdmission, JointAdmissionFailure> {
        self.begin_inner(metadata_bytes, position_or_queue_bytes)
            .map_err(|error| JointAdmissionFailure { error })
    }
    fn begin_inner(
        &self,
        metadata_bytes: NonZeroU64,
        position_or_queue_bytes: NonZeroU64,
    ) -> Result<ControlActionAdmission, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        {
            source_facts(&inner.state.lock().unwrap(), self.owner, self.fence)?;
        }
        let proposal = JointAdmissionHandle {
            inner: Arc::clone(&inner),
        }
        .begin_paid_proposal_for_scope(WorkScope {
            inner: Arc::clone(&inner),
            id: self.owner,
        })?;
        self.finish_begin(proposal, metadata_bytes, position_or_queue_bytes)
    }

    pub(super) fn try_begin(
        &self,
        metadata_bytes: NonZeroU64,
        position_or_queue_bytes: NonZeroU64,
    ) -> Result<JointTryBegin<ControlActionAdmission>, JointAdmissionFailure> {
        let result = (|| {
            let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
            source_facts(&inner.state.lock().unwrap(), self.owner, self.fence)?;
            let attempt = JointAdmissionHandle {
                inner: Arc::clone(&inner),
            }
            .try_begin_paid_proposal_for_scope(WorkScope {
                inner: Arc::clone(&inner),
                id: self.owner,
            })?;
            // Full is not permission to refresh this original source fence.
            source_facts(&inner.state.lock().unwrap(), self.owner, self.fence)?;
            match attempt {
                JointTryBegin::Full(wait) => Ok(JointTryBegin::Full(wait)),
                JointTryBegin::Started(proposal) => self
                    .finish_begin(proposal, metadata_bytes, position_or_queue_bytes)
                    .map(JointTryBegin::Started),
            }
        })();
        result.map_err(|error| JointAdmissionFailure { error })
    }

    fn finish_begin(
        &self,
        mut proposal: JointProposal,
        metadata_bytes: NonZeroU64,
        position_or_queue_bytes: NonZeroU64,
    ) -> Result<ControlActionAdmission, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        // Recheck after original Q reservation; no caller-built D or mask enters.
        let facts = {
            Arc::clone(source_facts(
                &inner.state.lock().unwrap(),
                self.owner,
                self.fence,
            )?)
        };
        proposal.control_action = Some(self.fence);
        let amounts = match self.fence.kind {
            ControlActionKind::Blocking => JointResources::ZERO
                .with(JointResource::BlockingJobs, 1)
                .with(JointResource::PhysicalQueueRecords, 1)
                .with(
                    JointResource::PhysicalQueueBytes,
                    position_or_queue_bytes.get(),
                ),
            ControlActionKind::Async => {
                JointResources::ZERO.with(JointResource::AsyncIo, position_or_queue_bytes.get())
            }
            ControlActionKind::Payload => JointResources::ZERO
                .with(JointResource::PayloadBytes, position_or_queue_bytes.get()),
        }
        .with(JointResource::ClaimRecords, 1)
        .with(JointResource::MetadataBytes, metadata_bytes.get());
        let lane = match self.fence.kind {
            ControlActionKind::Payload => JointLane::Custody,
            _ => self.fence.lane,
        };
        proposal.begin_new(lane, amounts)?;
        let current = proposal
            .current
            .as_mut()
            .expect("one original blocking action");
        current
            .selected
            .try_reserve(facts.dependencies.len())
            .map_err(|_| WorkError::Capacity("blocking action dependency allocation"))?;
        current.origin = facts.key.origin;
        current.selected.extend(facts.dependencies.iter().cloned());
        proposal.end_change()?;
        Ok(ControlActionAdmission {
            admission: proposal.submit().map_err(|failure| failure.error)?,
        })
    }
}
