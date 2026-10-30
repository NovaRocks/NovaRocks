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

//! Pure observation of one original fixed control incarnation's reset.

use super::*;
use crate::control_storage::SlotPhase;

/// Non-owning observation of the original fixed cancellation/proposal backing.
///
/// It retains neither the Work nor a strong or weak cancellation reference,
/// and therefore cannot prevent the original exclusive reset. It grants no
/// completion, cancellation, admission, or release authority. Composition must
/// keep its finite observer values in the already funded receiving locations.
/// Losing the authority is an error, never evidence of physical reclamation.
#[must_use = "A retirement observer reports actual original backing reset"]
pub struct WorkRetirementObserver {
    inner: Weak<Inner>,
    slot: SlotId,
    generation: u64,
}

impl WorkOwner {
    /// Observe this exact published Node's fixed control incarnation, including
    /// after cancellation. Legacy on-demand control storage has no reset proof.
    pub fn retirement_observer(&self) -> Result<WorkRetirementObserver, WorkError> {
        let scope = self.scope.as_ref().ok_or(WorkError::Released)?;
        let state = scope.inner.state.lock().unwrap();
        if !state.cancellations.fixed {
            return Err(WorkError::NotReady);
        }
        let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
        let slot = state
            .cancellations
            .slots
            .get(node.cancellation.0)
            .ok_or(WorkError::Conflict)?;
        if slot.phase != SlotPhase::Node(scope.id) {
            return Err(WorkError::Conflict);
        }
        Ok(WorkRetirementObserver {
            inner: Arc::downgrade(&scope.inner),
            slot: node.cancellation,
            generation: slot.generation,
        })
    }
}

impl WorkRetirementObserver {
    /// Read the original reset boundary without driving it or registering a
    /// new waiter. Capture the owner's existing progress marker before this
    /// read, then wait on that marker if false; progress itself is not proof.
    pub fn is_reclaimed(&self) -> Result<bool, WorkError> {
        let inner = self.inner.upgrade().ok_or(WorkError::Released)?;
        let state = inner.state.lock().unwrap();
        if !state.cancellations.fixed {
            return Err(WorkError::NotReady);
        }
        let slot = state
            .cancellations
            .slots
            .get(self.slot.0)
            .ok_or(WorkError::Conflict)?;
        if slot.generation < self.generation {
            return Err(WorkError::Conflict);
        }
        // From an actual Node, every later generation requires its old reset.
        // MAX cannot advance, but finish_reaped still publishes actual Free;
        // subsequent positive operations refuse rather than wrapping to an ABA.
        Ok(slot.generation > self.generation || slot.phase == SlotPhase::Free)
    }
}

#[cfg(test)]
mod tests;
