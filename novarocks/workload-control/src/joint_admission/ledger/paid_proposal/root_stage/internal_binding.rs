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

//! Positive-grant-derived, zero-growth mode of the original Root/stage batch.
use super::super::super::typed_custody::{
    computation_capture_view, computation_growth_view, typed_binding_capture_view,
};
use super::*;

/// This is closed binding authority derived from two positive original grants,
/// never a retained pin, Work owner, raw view or arbitrary claim constructor.
/// Copies carry only the same weak incarnation fences and grant no lifetime.
#[derive(Clone)]
pub struct InternalManifestBinding {
    inner: Weak<Inner>,
    root: WorkId,
    stage: WorkId,
    payload: [u64; 2],
    computation: [u64; 2],
}
impl InternalManifestBinding {
    pub(crate) fn from_grants(
        root: &WorkScope,
        stage: &WorkOwner,
        payload: &RetainedPayloadGrant,
        computation: &InternalComputationGrant,
    ) -> Result<Self, WorkError> {
        let child = stage.scope();
        if !Arc::ptr_eq(&root.inner, &child.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let state = root.inner.state.lock().unwrap();
        validate_nodes(&state, root.id, child.id)?;
        let (payload, compute) = if state.nodes[&root.id].root_domains.is_some() {
            (
                payload_capture_view(payload, root, &state)?,
                computation_capture_view(computation, &child, &state)?,
            )
        } else {
            (
                payload_growth_view(payload, root, &state)?,
                computation_growth_view(computation, &child, &state)?,
            )
        };
        let ClaimSource::Payload {
            epoch: payload_epoch,
        } = payload.source
        else {
            unreachable!()
        };
        let ClaimSource::Computation {
            epoch: compute_epoch,
        } = compute.source
        else {
            unreachable!()
        };
        Ok(Self {
            inner: Arc::downgrade(&root.inner),
            root: root.id,
            stage: child.id,
            payload: [payload.id, payload_epoch],
            computation: [compute.id, compute_epoch],
        })
    }
    pub(crate) fn computation_identity(&self) -> (WorkId, [u64; 2]) {
        (self.stage, self.computation)
    }
    pub(crate) fn validate_alias(
        &self,
        root: &WorkScope,
        execution: &WorkScope,
        payload: [u64; 2],
    ) -> Result<(), WorkError> {
        if !Weak::ptr_eq(&self.inner, &Arc::downgrade(&root.inner))
            || !Arc::ptr_eq(&root.inner, &execution.inner)
        {
            return Err(WorkError::ForeignAuthority);
        }
        if self.root != root.id || self.stage != execution.id || self.payload != payload {
            return Err(WorkError::Conflict);
        }
        Ok(())
    }
    fn capture(&self, inner: &Arc<Inner>) -> Result<(RootStageSource, JointLane), WorkError> {
        let state = inner.state.lock().unwrap();
        validate_nodes(&state, self.root, self.stage)?;
        let root = WorkScope {
            inner: Arc::clone(inner),
            id: self.root,
        };
        let stage = WorkScope {
            inner: Arc::clone(inner),
            id: self.stage,
        };
        let payload = typed_binding_capture_view(&state, &root, self.payload, false)?;
        let computation = typed_binding_capture_view(&state, &stage, self.computation, true)?;
        let root_control = node_view(&state, &root)?;
        let stage_control = node_view(&state, &stage)?;
        let lane = state.joint.as_ref().unwrap().claims[&computation.id]
            .active_facts
            .lane;
        if !matches!(lane, JointLane::Foreground | JointLane::Settlement) {
            return Err(WorkError::Conflict);
        }
        Ok((
            RootStageSource {
                computation_window: None,
                writer_revision: state.nodes[&self.root]
                    .root_domains
                    .as_ref()
                    .map(|d| d.writer_revision),
                inner: Arc::downgrade(inner),
                root: self.root,
                fence: RootStageFence {
                    canonical: state.nodes[&self.root].root_domains.is_some(),
                    stage: self.stage,
                    payload: self.payload[0],
                    epoch: self.payload[1],
                    computation: Some(self.computation),
                },
                payload,
                root_control,
                stage_control,
                computation: Some(computation),
            },
            lane,
        ))
    }
    /// Before the original Q is acquired this creates no facts, pins, body or
    /// ticket. Full only asks the SAME stable Source to wait/reobserve.
    pub fn try_begin(
        &self,
    ) -> Result<JointTryBegin<RootStageProposal, JointProposalSlotSignal>, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let (source, lane) = self.capture(&inner)?;
        source.try_begin(0, lane)
    }
}
impl RootStageReady {
    /// Silently commit the zero-growth existing mode. The same positive
    /// incarnation must still have nonzero typed pins and Active exact nodes.
    /// No New claim, pin, owner or capacity is moved into a replacement slot.
    pub fn install_binding_in(
        &mut self,
        binding: &InternalManifestBinding,
    ) -> Result<crate::WorkloadProgressNotification, WorkError> {
        let receipt = self.batch.adjustment.as_mut().ok_or(WorkError::Released)?;
        let id = receipt.id.ok_or(WorkError::Released)?;
        let inner = Arc::clone(&receipt.inner);
        if !Weak::ptr_eq(&binding.inner, &Arc::downgrade(&inner)) {
            return Err(WorkError::ForeignAuthority);
        }
        if !self.batch.new_claims.is_empty() {
            return Err(WorkError::Conflict);
        }
        let retired_root = inner.update_facts_silent(|state| {
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            let request = ledger.requests.get(&id).ok_or(WorkError::Released)?;
            if let RequestPhase::Failed(error) = &request.phase {
                return Err(error.clone());
            }
            if !matches!(request.phase, RequestPhase::Delivered)
                || request.owner != binding.root
                || request.waker.is_some()
            {
                return Err(WorkError::Conflict);
            }
            let fence = request.root_stage.ok_or(WorkError::Conflict)?;
            if fence.stage != binding.stage
                || [fence.payload, fence.epoch] != binding.payload
                || fence.computation != Some(binding.computation)
            {
                return Err(WorkError::Conflict);
            }
            let changes = request.changes.clone();
            validate_root_stage(state, binding.root, fence, &changes)?;
            check_adjusted_sources(state, binding.root, fence, &changes, id)?;
            if fence.canonical {
                root_domains::validate_install(state, id, binding.root)?;
            }
            // Reuse exactly the original install author. Amounts and typed
            // custody/epochs/pins remain unchanged; only full-D facts advance.
            commit_adjusted_sources(state, &changes, fence.canonical);
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
