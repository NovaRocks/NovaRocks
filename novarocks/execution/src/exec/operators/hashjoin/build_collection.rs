// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact E-slot handoff and one F-owned begin responsibility. I is the actual
//! caller-owned captured-run/payload receipt; this module invents no page data.

use super::{
    build_state::build_contract,
    join_completion::{
        JoinCompletion, PublishResult, RemainingDemand, arithmetic, funded_shared, funded_vec,
    },
};
use crate::{
    exec::{
        phased_work::{PhasedRangeWork, RangeLease},
        pipeline::operator::InputConstructionOwner,
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
};

enum InputSlot<I> {
    Pending,
    Normal(Option<I>),
    Withdrawn,
    Failed,
}
struct State<I> {
    inputs: Vec<InputSlot<I>>,
    normal: usize,
    withdrawn: usize,
    begin_completed: bool,
}
pub(crate) struct BuildCollection<I, A> {
    state: Mutex<State<I>>,
    begin: Arc<PhasedRangeWork<ExecutionFailure, ValueTypeStorageLifetime>>,
    completion: Arc<JoinCompletion<A>>,
    _storage: ValueTypeStorageLifetime,
}
/// This owns only the original metadata creator; every I keeps its own actual
/// sealed arrays/pages/capture creator. The protocol neither clones nor copies I.
pub(crate) struct CollectedInputs<I> {
    values: Vec<I>,
    _storage: ValueTypeStorageLifetime,
}
impl<I> CollectedInputs<I> {
    pub(crate) fn as_slice(&self) -> &[I] {
        &self.values
    }
    pub(crate) fn pop(&mut self) -> Option<I> {
        self.values.pop()
    }
}
pub(crate) enum BeginClaim<I, A> {
    Pending,
    Unavailable,
    InProgress,
    // Destruction of the original input container precedes lease abandonment.
    Claimed {
        inputs: CollectedInputs<I>,
        lease: BeginLease<I, A>,
    },
}
pub(crate) struct BeginLease<I, A> {
    owner: Arc<BuildCollection<I, A>>,
    lease: RangeLease<ExecutionFailure, ValueTypeStorageLifetime>,
}
impl<I, A> BuildCollection<I, A> {
    pub(crate) fn new(
        completion: Arc<JoinCompletion<A>>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Arc<Self>> {
        control.checkpoint(0)?;
        let count = completion.participants().inputs;
        let mut inputs = funded_vec(owner, count)?;
        for ordinal in 0..count {
            if ordinal.is_multiple_of(256) {
                control.checkpoint(0)?;
            }
            inputs.push(InputSlot::Pending);
        }
        let begin = funded_shared(
            owner,
            PhasedRangeWork::with_retention(
                build_contract("join begin lease was abandoned"),
                arithmetic(),
                owner.storage_lifetime(),
            ),
        )?;
        begin.begin(1, NonZeroUsize::new(1).unwrap())?;
        funded_shared(
            owner,
            Self {
                state: Mutex::new(State {
                    inputs,
                    normal: 0,
                    withdrawn: 0,
                    begin_completed: false,
                }),
                begin,
                completion,
                _storage: owner.storage_lifetime(),
            },
        )
    }
    fn state(&self) -> ExecutionResult<MutexGuard<'_, State<I>>> {
        match self.state.lock() {
            Ok(state) => Ok(state),
            Err(poisoned) => {
                drop(poisoned.into_inner());
                Err(self
                    .completion
                    .fail(build_contract("join input collection was poisoned")))
            }
        }
    }
    pub(crate) fn check(&self) -> ExecutionResult<()> {
        self.completion.check()?;
        self.begin
            .check()
            .map_err(|cause| self.completion.fail(cause))
    }
    /// E calls this only after actual EOS and its RF/input obligations finish.
    /// The last E only installs one receipt and notifies. It never runs begin.
    pub(crate) fn normal_input(&self, input: usize, value: I) -> ExecutionResult<()> {
        let notify = self.completion.observable().defer_notify();
        let result: ExecutionResult<_> = (|| {
            self.check()?;
            let mut state = self.state()?;
            let slot = state
                .inputs
                .get_mut(input)
                .ok_or_else(|| build_contract("join input has no installed E slot"))?;
            if !matches!(slot, InputSlot::Pending) {
                return Err(build_contract("join input settled more than once"));
            }
            *slot = InputSlot::Normal(Some(value));
            state.normal += 1;
            Ok(())
        })();
        match result {
            Ok(()) => {
                notify.arm();
                Ok(())
            }
            Err(cause) => Err(self.completion.fail(cause)),
        }
    }
    pub(crate) fn withdraw_input_if_pending(&self, input: usize) -> ExecutionResult<bool> {
        let notify = self.completion.observable().defer_notify();
        let result: ExecutionResult<_> = (|| {
            self.check()?;
            let mut state = self.state()?;
            let slot = state
                .inputs
                .get_mut(input)
                .ok_or_else(|| build_contract("join input exit has no installed E slot"))?;
            if !matches!(slot, InputSlot::Pending) {
                return Ok(false);
            }
            *slot = InputSlot::Withdrawn;
            state.withdrawn += 1;
            Ok(true)
        })();
        match result {
            Ok(changed) => {
                if changed {
                    notify.arm();
                }
                Ok(changed)
            }
            Err(cause) => Err(self.completion.fail(cause)),
        }
    }
    pub(crate) fn fail_input(&self, input: usize, cause: ExecutionFailure) -> ExecutionFailure {
        if let Ok(mut state) = self.state.lock() {
            if let Some(slot) = state.inputs.get_mut(input) {
                if matches!(slot, InputSlot::Pending) {
                    *slot = InputSlot::Failed;
                }
            }
        }
        self.completion.fail(cause)
    }
    pub(crate) fn inputs_sealed(&self) -> ExecutionResult<bool> {
        self.check()?;
        let state = self.state()?;
        Ok(state.normal == state.inputs.len())
    }
    pub(crate) fn claim_begin(
        self: &Arc<Self>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<BeginClaim<I, A>> {
        self.check()?;
        if self.completion.remaining_demand()? == RemainingDemand::Released {
            return Ok(BeginClaim::Unavailable);
        }
        let token = {
            let state = self.state()?;
            if state.withdrawn != 0 {
                return Ok(BeginClaim::Unavailable);
            }
            if state.normal != state.inputs.len() {
                return Ok(BeginClaim::Pending);
            }
            self.begin.claim()
        }
        .map_err(|cause| self.completion.fail(cause))?;
        let Some(token) = token else {
            return Ok(BeginClaim::InProgress);
        };
        let mut lease = BeginLease {
            owner: Arc::clone(self),
            lease: token,
        };
        // Only the winning F pays O(E) metadata and moves original receipts.
        // Resource/cancel refusal is latched before the unfinished token drops.
        let values: ExecutionResult<_> = (|| {
            control.checkpoint(0)?;
            let mut values = funded_vec(owner, self.completion.participants().inputs)?;
            let mut state = self.state()?;
            for (ordinal, slot) in state.inputs.iter_mut().enumerate() {
                if ordinal.is_multiple_of(256) {
                    control.checkpoint(0)?;
                }
                let InputSlot::Normal(value) = slot else {
                    return Err(build_contract("sealed join input changed before begin"));
                };
                values.push(
                    value
                        .take()
                        .ok_or_else(|| build_contract("join begin consumed an input twice"))?,
                );
            }
            Ok(CollectedInputs {
                values,
                _storage: owner.storage_lifetime(),
            })
        })();
        match values {
            Ok(inputs) => Ok(BeginClaim::Claimed { inputs, lease }),
            Err(cause) => Err(lease.fail(cause)),
        }
    }
    /// Call only after the winning F has installed the actual graph/finalizer
    /// state. This settles begin responsibility, not BuildPublished or Ready.
    pub(crate) fn complete_begin(&self, lease: &mut BeginLease<I, A>) -> ExecutionResult<()> {
        self.check()?;
        if !std::ptr::eq(self, Arc::as_ptr(&lease.owner)) {
            return Err(self
                .completion
                .fail(build_contract("join begin completion has a foreign owner")));
        }
        let mut state = self.state()?;
        if let Err(cause) = self.begin.complete(&mut lease.lease) {
            drop(state);
            return Err(self.completion.fail(cause));
        }
        state.begin_completed = true;
        drop(state);
        self.completion.finalized_step();
        Ok(())
    }
    /// Root's finalizer must independently hold real K Ready and the closed
    /// join range-phase barrier; no caller boolean substitutes for those facts.
    pub(crate) fn publish(
        &self,
        artifact: Arc<A>,
        rows: usize,
    ) -> ExecutionResult<PublishResult<A>> {
        self.check()?;
        let begun = { self.state()?.begin_completed };
        if !begun {
            return Err(self
                .completion
                .fail(build_contract("join published before begin completed")));
        }
        self.completion.publish(artifact, rows)
    }
}
impl<I, A> BeginLease<I, A> {
    pub(crate) fn fail(&mut self, cause: ExecutionFailure) -> ExecutionFailure {
        let cause = self.owner.begin.latch(cause);
        self.owner.completion.fail(cause)
    }
    /// A normal released-demand path retires this responsibility without
    /// creating a build publication, empty input, EOS or successful RF fact.
    pub(crate) fn withdraw_after_demand_released(&mut self) -> ExecutionResult<()> {
        self.owner.completion.check()?;
        if self.owner.completion.remaining_demand()? != RemainingDemand::Released {
            return Err(self.fail(build_contract(
                "join begin withdrawal still has output demand",
            )));
        }
        self.owner
            .begin
            .complete(&mut self.lease)
            .map_err(|cause| self.owner.completion.fail(cause))?;
        self.owner.completion.finalized_step();
        Ok(())
    }
}
impl<I, A> Drop for BeginLease<I, A> {
    fn drop(&mut self) {
        if !self.lease.is_settled() {
            let cause = self
                .owner
                .begin
                .check()
                .err()
                .unwrap_or_else(|| build_contract("join begin lease was abandoned"));
            self.owner.completion.fail(cause);
        }
    }
}

#[cfg(test)]
#[path = "build_collection_tests.rs"]
mod tests;
