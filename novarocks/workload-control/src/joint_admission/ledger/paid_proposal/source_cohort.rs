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

//! One finite original proof for the complete control custody of a closed Source.

use super::*;
use std::num::NonZeroUsize;

/// The original positive receipt retains every unused fixed position. Taking
/// a child installs that exact position without another proof or allocation
/// of cohort backing. Each installed child retains its own cancellation and
/// actual-exit lifetime. Dropping this lease retires only unused positions.
#[must_use = "Retain the original cohort through Source lifetime"]
pub struct SourceCohort {
    receipt: JointAdjustmentReceipt,
    remaining: usize,
}

impl WorkScope {
    /// Reserve all simultaneous child control bases before opening a Source,
    /// watch or Native RPC. The already-bound parent's exact complete D is
    /// inherited by every position. Count comes from the actual frozen Source
    /// recipe, including overlapping attempts, never a topology default.
    pub fn request_source_cohort(
        &self,
        count: NonZeroUsize,
    ) -> Result<JointAdmission, JointAdmissionFailure> {
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&self.inner),
        };
        let proposal = handle
            .begin_paid_proposal_for_scope(self.clone())
            .map_err(|error| JointAdmissionFailure { error })?;
        prepare(proposal, count).and_then(JointProposal::submit)
    }

    /// Full is only an unacquired original Q cell. It holds no fixed slots,
    /// request or positive right; the same Source reobserves its own signal.
    pub fn try_request_source_cohort(
        &self,
        count: NonZeroUsize,
    ) -> Result<JointTryBegin<JointAdmission, JointProposalSlotSignal>, WorkError> {
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&self.inner),
        };
        match handle.try_begin_paid_proposal_for_scope_signal(self.clone())? {
            JointTryBegin::Full(signal) => Ok(JointTryBegin::Full(signal)),
            JointTryBegin::Started(proposal) => prepare(proposal, count)
                .and_then(JointProposal::submit)
                .map(JointTryBegin::Started)
                .map_err(|failure| failure.error),
        }
    }
}

fn prepare(
    mut proposal: JointProposal,
    count: NonZeroUsize,
) -> Result<JointProposal, JointAdmissionFailure> {
    let result = (|| {
        let count = count.get();
        // The same original fixed-service author charges this bounded array.
        // Reserve it only after acquiring the original construction position.
        proposal
            .custody
            .as_mut()
            .ok_or(WorkError::NotReady)?
            .reserved_cohort
            .try_reserve_exact(count)
            .map_err(|_| WorkError::Capacity("paid Source cohort rollback backing"))?;
        let mut state = proposal.scope.inner.state.lock().unwrap();
        validate_active_control_node(&state, &proposal.scope)?;
        let node = &state.nodes[&proposal.scope.id];
        let parent_slot = node.cancellation;
        let parent_cell = &state.cancellations.slots[parent_slot.0];
        let parent_claim = parent_cell.claim.ok_or(WorkError::NotReady)?;
        let parent = &state.joint.as_ref().unwrap().claims[&parent_claim];
        let fence = ParentControlFence {
            work: proposal.scope.id,
            slot: parent_slot,
            generation: parent_cell.generation,
            claim: parent_claim,
            structure_revision: parent.structure_revision,
            release_revision: parent.release_revision,
        };
        authenticate_parent(&state, fence, proposal.scope.id)?;
        let facts = Arc::clone(&parent.active_facts);
        for _ in 0..count {
            let slot = state
                .cancellations
                .free_head
                .ok_or(WorkError::Capacity("fixed control slots"))?;
            let cell = &state.cancellations.slots[slot.0];
            let claim = cell.claim.ok_or(WorkError::NotReady)?;
            let old = &state.joint.as_ref().unwrap().claims[&claim];
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
            let source = JointClaimView {
                inner: Arc::downgrade(&proposal.scope.inner),
                id: claim,
                source: ClaimSource::PoolFree {
                    slot,
                    generation: cell.generation,
                    parent: fence,
                    deadline: None,
                },
                structure_revision: old.structure_revision,
                release_revision: old.release_revision,
            };
            authenticate(old, &source)?;
            let change = ChangeRecord {
                old: Some(claim),
                original_amounts: Some(cell.base),
                original_facts: Some(Arc::clone(&old.active_facts)),
                source: Some(source),
                claim: ProposedClaim {
                    facts: Arc::clone(&facts),
                    amounts: cell.base,
                },
            };
            // Both arrays were paid and bounded before removing this position.
            state
                .cancellations
                .cell(parent_slot)
                .reserve_fixed_children(1)?;
            proposal.records.push(change);
            proposal
                .custody
                .as_mut()
                .unwrap()
                .reserved_cohort
                .push(slot);
            let next = state.cancellations.slots[slot.0].next_free.take();
            state.cancellations.free_head = next;
            state.cancellations.slots[slot.0].class = facts.class;
            state.cancellations.slots[slot.0].phase = SlotPhase::ChildReserved {
                parent: proposal.scope.id,
            };
        }
        proposal.started = count;
        Ok(())
    })();
    result.map_err(|error| JointAdmissionFailure { error })?;
    Ok(proposal)
}

impl JointAdjustmentReceipt {
    /// Transfer this same delivered grant into its Source's stable field.
    /// No Node is installed, no callback runs and no positive right is cloned.
    pub fn install_source_cohort_in(
        &mut self,
        parent: &WorkScope,
        destination: &mut Option<SourceCohort>,
    ) -> Result<(), WorkError> {
        if destination.is_some() {
            return Err(WorkError::Conflict);
        }
        if !Arc::ptr_eq(&self.inner, &parent.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let id = self.id.ok_or(WorkError::Released)?;
        let remaining = {
            let state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
            if let RequestPhase::Failed(error) = &request.phase {
                return Err(error.clone());
            }
            if !matches!(request.phase, RequestPhase::Delivered)
                || request.owner != parent.id
                || !request.source_cohort
                || request.changes.is_empty()
                || request.changes.iter().any(|change| {
                    !matches!(
                        change.source.as_ref().map(|source| source.source),
                        Some(ClaimSource::PoolFree { .. })
                    )
                })
            {
                return Err(WorkError::Conflict);
            }
            validate_active_control_node(&state, parent)?;
            for change in request.changes.iter() {
                let source = change.source.as_ref().ok_or(WorkError::Conflict)?;
                let ClaimSource::PoolFree {
                    slot,
                    generation,
                    parent: fence,
                    ..
                } = source.source
                else {
                    return Err(WorkError::Conflict);
                };
                authenticate_parent(&state, fence, parent.id)?;
                let cell = &state.cancellations.slots[slot.0];
                let claim = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
                if cell.phase
                    != (SlotPhase::ChildGranted {
                        parent: fence.work,
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
            }
            request.changes.len()
        };
        *destination = Some(SourceCohort {
            receipt: JointAdjustmentReceipt {
                inner: Arc::clone(&self.inner),
                id: Some(id),
            },
            remaining,
        });
        self.id = None;
        Ok(())
    }
}

impl SourceCohort {
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    /// Write the original child into a stable receiver before returning its
    /// notice. Refusal leaves this same lease armed. Notify only after every
    /// receiver lock has exited. None means every reserved position was used.
    pub fn take_child_in(
        &mut self,
        parent: &WorkScope,
        destination: &mut Option<WorkOwner>,
    ) -> Result<Option<crate::WorkloadProgressNotification>, WorkError> {
        if destination.is_some() {
            return Err(WorkError::Conflict);
        }
        if !Arc::ptr_eq(&self.receipt.inner, &parent.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let id = self.receipt.id.ok_or(WorkError::Released)?;
        let last = self.remaining == 1;
        self.receipt.inner.update_facts_silent(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
            if let RequestPhase::Failed(error) = &request.phase {
                return Err(error.clone());
            }
            if !matches!(request.phase, RequestPhase::Delivered)
                || !request.source_cohort
                || request.owner != parent.id
            {
                return Err(WorkError::Conflict);
            }
            if state.closed || ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            let change = request
                .changes
                .iter()
                .find(|change| {
                    matches!(change.source.as_ref().map(|source| source.source),
                    Some(ClaimSource::PoolFree { slot, .. }) if matches!(
                        state.cancellations.slots[slot.0].phase,
                        SlotPhase::ChildGranted { ticket, .. } if ticket == id))
                })
                .ok_or(WorkError::Conflict)?;
            let source = change.source.as_ref().unwrap();
            let ClaimSource::PoolFree {
                slot,
                generation,
                parent: fence,
                deadline,
            } = source.source
            else {
                return Err(WorkError::Conflict);
            };
            authenticate_parent(state, fence, parent.id)?;
            let cell = &state.cancellations.slots[slot.0];
            let claim = ledger.claims.get(&source.id).ok_or(WorkError::Released)?;
            if cell.phase
                != (SlotPhase::ChildGranted {
                    parent: fence.work,
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
            state
                .cancellations
                .cell(fence.slot)
                .release_fixed_children(1);
            let child =
                match state.install_bound_child(&self.receipt.inner, fence.work, slot, deadline) {
                    Ok(child) => child,
                    Err(error) => {
                        state
                            .cancellations
                            .cell(fence.slot)
                            .reserve_fixed_children(1)
                            .expect("failed installation restores its original reserved edge");
                        return Err(error);
                    }
                };
            let claim = state
                .joint
                .as_mut()
                .unwrap()
                .claims
                .get_mut(&claim_id)
                .unwrap();
            claim.structure_revision += 1;
            claim.pending = None;
            if last {
                remove_request(state, id);
            }
            *destination = Some(child);
            Ok(())
        })?;
        self.remaining -= 1;
        if last {
            self.receipt.id = None;
        }
        Ok(Some(crate::WorkloadProgressNotification::new(Arc::clone(
            &self.receipt.inner,
        ))))
    }
}

#[cfg(test)]
mod tests;
