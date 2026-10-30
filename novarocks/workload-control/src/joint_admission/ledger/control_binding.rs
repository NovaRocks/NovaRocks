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

//! Restricted delegation of one existing Node's original control dependency set.

use super::*;
use novarocks_resource_contract::SelectedNetworkOrigin;

/// One paid original control-source adjustment. No Work owner, completion,
/// release, physical claim, arbitrary source or general builder is exposed.
/// The original Active WorkOwner must remain alive through installation.
///
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::{ControlBindingProposal, JointLane, JointResources};
/// fn no_new(proposal: &mut ControlBindingProposal) {
///     let _ = proposal.begin_new(JointLane::Foreground, JointResources::ZERO);
/// }
/// ```
///
/// ```compile_fail
/// use novarocks_workload_control::joint_admission::{ControlBindingProposal, JointClaimView, JointResources};
/// fn no_growth(proposal: &mut ControlBindingProposal, source: JointClaimView) {
///     let _ = proposal.begin_grow(source, JointResources::ZERO);
/// }
/// ```
#[must_use = "Finish and submit this original proposal, or drop it"]
pub struct ControlBindingProposal {
    pub(super) proposal: JointProposal,
}
impl ControlBindingProposal {
    pub fn push_dependency(&mut self, fact: &SharedDependency) -> Result<(), WorkError> {
        self.proposal.push_dependency(fact)
    }
    pub fn push_selected_network(
        &mut self,
        selected: SelectedNetworkOrigin<'_>,
    ) -> Result<(), WorkError> {
        self.proposal.push_selected_network(selected)
    }
    /// Finish the original control change. Its exact existing production rows
    /// follow the complete dependency union in this same paid batch at submit.
    /// Repeated end or a push after end poisons the original proposal.
    pub fn end_change(&mut self) -> Result<(), WorkError> {
        self.proposal.end_change()
    }
    pub fn submit(self) -> Result<JointAdmission, JointAdmissionFailure> {
        self.proposal.submit_control_binding()
    }
}

impl WorkScope {
    /// Reserve the original paid proposal cell before request allocation, then
    /// bind only this existing Node's exact control claim. The scope adds no
    /// ownership; the original paid proposal retains its existing construction
    /// pin. No owner, stage, physical claim or new WorkId is minted.
    pub fn begin_control_binding(&self) -> Result<ControlBindingProposal, WorkError> {
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&self.inner),
        };
        self.finish_control_binding(handle.begin_paid_proposal_for_scope(self.clone())?)
    }

    /// The same restricted constructor, with a typed unacquired-Q observation.
    /// Full has no request/body/ticket and grants no control or physical right.
    pub fn try_begin_control_binding(
        &self,
    ) -> Result<JointTryBegin<ControlBindingProposal, JointProposalSlotSignal>, WorkError> {
        let handle = JointAdmissionHandle {
            inner: Arc::clone(&self.inner),
        };
        match handle.try_begin_paid_proposal_for_scope_signal(self.clone())? {
            JointTryBegin::Full(signal) => Ok(JointTryBegin::Full(signal)),
            JointTryBegin::Started(proposal) => self
                .finish_control_binding(proposal)
                .map(JointTryBegin::Started),
        }
    }

    fn finish_control_binding(
        &self,
        mut proposal: JointProposal,
    ) -> Result<ControlBindingProposal, WorkError> {
        let (source, unbound) = control_claim_view_for_scope(self, true)?;
        if unbound {
            proposal.begin_first_bind(source)?;
        } else {
            proposal.begin_grow(source, JointResources::ZERO)?;
        }
        Ok(ControlBindingProposal { proposal })
    }
}

pub(super) fn validate_active_control_node(
    state: &State,
    scope: &WorkScope,
) -> Result<(), WorkError> {
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
    Ok(())
}

// This checks only the restricted delegation's shape/authority. Source
// revisions, pending ticket and the sole commit stay in the original authors.
pub(super) fn validate_control_binding_shape(
    state: &State,
    scope: &WorkScope,
    changes: &[ChangeRecord],
) -> Result<(), WorkError> {
    validate_active_control_node(state, scope)?;
    if changes.is_empty()
        || changes.iter().skip(1).any(|change| {
            change
                .source
                .as_ref()
                .is_none_or(|source| !matches!(source.source, ClaimSource::Production { .. }))
        })
    {
        return Err(WorkError::Conflict);
    }
    validate_control_productions(state, changes)?;
    let change = &changes[0];
    let source = change.source.as_ref().ok_or(WorkError::Conflict)?;
    let ClaimSource::NodeControl { work, slot, .. } = source.source else {
        return Err(WorkError::Conflict);
    };
    let node = &state.nodes[&scope.id];
    let cell = state
        .cancellations
        .slots
        .get(slot.0)
        .ok_or(WorkError::Released)?;
    let record = state
        .joint
        .as_ref()
        .unwrap()
        .claims
        .get(&source.id)
        .ok_or(WorkError::Released)?;
    if work != scope.id
        || node.cancellation != slot
        || change.old != Some(source.id)
        || cell.phase != SlotPhase::Node(scope.id)
        || cell.claim != Some(source.id)
        || record.owner != ClaimOwner::ControlSlot(slot)
        || record.amounts != cell.base
        || record.active_amounts != cell.base
        || change.original_amounts != Some(cell.base)
        || change.claim.amounts != cell.base
        || change.claim.facts.lane != JointLane::Custody
        || change.claim.facts.dependencies.is_empty()
    {
        return Err(WorkError::Conflict);
    }
    Ok(())
}

impl JointAdjustmentReceipt {
    /// Silently install the original NodeControl and all its production D while borrowing
    /// its non-owning scope. Failure keeps this same receipt armed; success
    /// disarms it before returning a token. Notify only after the caller's
    /// original phase is stable and every receiver/State lock has exited.
    pub fn install_control_scope_in(
        &mut self,
        scope: &WorkScope,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let id = self.id.ok_or(WorkError::Released)?;
        self.inner.update_facts_silent(|state| {
            self.install_originals_state(state, id, &[], Some(scope), true)
        })?;
        self.id = None;
        Ok(crate::WorkloadProgressNotification::new(Arc::clone(
            &self.inner,
        )))
    }
}

#[cfg(test)]
#[path = "control_binding_tests.rs"]
mod tests;
