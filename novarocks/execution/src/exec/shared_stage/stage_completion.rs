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

//! Exact installed stage participants, publication and probe completion facts.
//! This protocol owns no scheduler, CPU position, Task verdict or row storage.
// Design: ADR-0212 (docs/adr/ADR-0212-shared-join-stages-and-exact-demand-retirement.md)

use super::{ContractFailure, funded_shared, funded_vec};
use crate::{
    exec::{
        operators::local_exchanger::LocalExchanger, pipeline::operator::InputConstructionOwner,
    },
    runtime::{
        observable::{Observable, ObserverSubscription},
        task::{ExecutionFailure, ExecutionResult},
    },
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StageParticipants {
    pub(crate) inputs: usize,
    pub(crate) finalizers: NonZeroUsize,
    pub(crate) probes: usize,
    pub(crate) tails: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProbeState {
    Pending,
    NormalCompleted,
    Withdrawn,
    Failed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemainingDemand {
    Needed,
    Released,
}
/// The original operator supplies its immutable artifact.
/// A must retain its own real data/metadata creators; this wrapper grants none.
pub(crate) struct Published<A> {
    pub(crate) artifact: Arc<A>,
    pub(crate) rows: usize,
}
impl<A> Clone for Published<A> {
    fn clone(&self) -> Self {
        Self {
            artifact: Arc::clone(&self.artifact),
            rows: self.rows,
        }
    }
}
pub(crate) enum PublishResult<A> {
    Installed,
    NotNeeded(Arc<A>),
}
pub(crate) enum TailStatus<A> {
    Pending,
    Unavailable,
    Withdrawn,
    Ready(Published<A>),
}
struct CompletionState<A> {
    probes: Vec<ProbeState>,
    normal: usize,
    retired: usize,
    failure: Option<ExecutionFailure>,
    published: Option<Published<A>>,
}
/// Demand is owned by the original probe slots or the original Handoff.
enum OutputDemand {
    ProbeSlots,
    Handoff(Arc<LocalExchanger>),
}
pub(crate) struct StageCompletion<A> {
    participants: StageParticipants,
    state: Mutex<CompletionState<A>>,
    output: OutputDemand,
    contract: ContractFailure,
    changed: Arc<Observable>,
    activation: OnceLock<ExecutionResult<Option<ObserverSubscription>>>,
    _storage: ValueTypeStorageLifetime,
}
impl<A> StageCompletion<A> {
    /// The builder supplies its original funded Observable and, only for T>0,
    /// its actual unique Handoff. This does not register or allocate observers.
    pub(crate) fn new(
        participants: StageParticipants,
        output_producers: Option<usize>,
        handoff: Option<Arc<LocalExchanger>>,
        changed: Arc<Observable>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
        contract: ContractFailure,
    ) -> ExecutionResult<Arc<Self>> {
        control.checkpoint(0)?;
        let output = match (output_producers, handoff) {
            (None, None) if participants.tails == 0 => OutputDemand::ProbeSlots,
            (Some(expected), Some(handoff))
                if participants.tails != 0 && handoff.remaining_producers() == expected =>
            {
                OutputDemand::Handoff(handoff)
            }
            _ => {
                return Err(contract("output demand differs from installed producers"));
            }
        };
        let mut probes = funded_vec(owner, participants.probes)?;
        for ordinal in 0..participants.probes {
            if ordinal.is_multiple_of(256) {
                control.checkpoint(0)?;
            }
            probes.push(ProbeState::Pending);
        }
        funded_shared(
            owner,
            Self {
                participants,
                state: Mutex::new(CompletionState {
                    probes,
                    normal: 0,
                    retired: 0,
                    failure: None,
                    published: None,
                }),
                output,
                contract,
                changed,
                activation: OnceLock::new(),
                _storage: owner.storage_lifetime(),
            },
        )
    }
    pub(super) fn contract_failure(&self, detail: &'static str) -> ExecutionFailure {
        (self.contract)(detail)
    }
    pub(crate) fn participants(&self) -> StageParticipants {
        self.participants
    }
    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.changed)
    }
    pub(crate) fn handoff_closed_observable(&self) -> Option<Arc<Observable>> {
        match &self.output {
            OutputDemand::ProbeSlots => None,
            OutputDemand::Handoff(h) => Some(h.closed_observable()),
        }
    }
    /// Registration belongs to admitted operator activation. Preparation
    /// creates only dormant metadata, never a live subscription.
    pub(crate) fn activate(&self, owner: &dyn InputConstructionOwner) -> ExecutionResult<()> {
        self.activation
            .get_or_init(|| {
                let Some(closed) = self.handoff_closed_observable() else {
                    return Ok(None);
                };
                let changed = self.observable();
                let weak = Arc::downgrade(&changed);
                let retention = owner.storage_lifetime();
                let callback = funded_shared(owner, move || {
                    let _retention = &retention;
                    if let Some(changed) = weak.upgrade() {
                        changed.notify_observers();
                    }
                })?;
                Ok(Some(closed.subscribe(callback)))
            })
            .as_ref()
            .map(|_| ())
            .map_err(Clone::clone)
    }
    #[cfg(test)]
    pub(crate) fn activation_started(&self) -> bool {
        self.activation.get().is_some()
    }
    fn state(&self) -> ExecutionResult<MutexGuard<'_, CompletionState<A>>> {
        match self.state.lock() {
            Ok(state) => Ok(state),
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let cause = state
                    .failure
                    .get_or_insert_with(|| {
                        self.contract_failure("stage completion state was poisoned")
                    })
                    .clone();
                Err(cause)
            }
        }
    }
    fn check_in(state: &CompletionState<A>) -> ExecutionResult<()> {
        match &state.failure {
            Some(cause) => Err(cause.clone()),
            None => Ok(()),
        }
    }
    pub(crate) fn check(&self) -> ExecutionResult<()> {
        let state = self.state()?;
        Self::check_in(&state)
    }
    pub(crate) fn fail(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let notify = self.changed.defer_notify();
        let result = match self.state.lock() {
            Ok(mut state) => state.failure.get_or_insert(cause).clone(),
            Err(poisoned) => poisoned.into_inner().failure.get_or_insert(cause).clone(),
        };
        notify.arm();
        result
    }
    fn demand_in(&self, state: &CompletionState<A>) -> RemainingDemand {
        let released = match &self.output {
            OutputDemand::ProbeSlots => state.retired == self.participants.probes,
            OutputDemand::Handoff(handoff) => handoff.all_consumers_closed(),
        };
        if released {
            RemainingDemand::Released
        } else {
            RemainingDemand::Needed
        }
    }
    pub(crate) fn remaining_demand(&self) -> ExecutionResult<RemainingDemand> {
        let state = self.state()?;
        Self::check_in(&state)?;
        Ok(self.demand_in(&state))
    }
    /// Every relaxed bitmap write and every admitted candidate/output precedes
    /// this mutex release. Tail acquires the SAME mutex before reading marks.
    pub(crate) fn normal_after_output(&self, probe: usize) -> ExecutionResult<()> {
        self.settle(probe, ProbeState::NormalCompleted)
    }
    #[cfg(test)]
    pub(crate) fn withdraw(&self, probe: usize) -> ExecutionResult<()> {
        self.settle(probe, ProbeState::Withdrawn)
    }
    fn settle(&self, probe: usize, verdict: ProbeState) -> ExecutionResult<()> {
        let notify = self.changed.defer_notify();
        let result: ExecutionResult<_> = (|| {
            let mut state = self.state()?;
            Self::check_in(&state)?;
            let slot = state
                .probes
                .get_mut(probe)
                .ok_or_else(|| self.contract_failure("probe is not an installed participant"))?;
            if *slot != ProbeState::Pending {
                return Err(self.contract_failure("probe settled more than once"));
            }
            *slot = verdict;
            state.retired += 1;
            if verdict == ProbeState::NormalCompleted {
                state.normal += 1;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                notify.arm();
                Ok(())
            }
            Err(cause) => Err(self.fail(cause)),
        }
    }
    /// Driver begin_exit is not a verdict. Preserve an earlier normal/failure
    /// fact and settle only a still-pending participant as truly withdrawn.
    pub(crate) fn withdraw_if_pending(&self, probe: usize) -> ExecutionResult<bool> {
        let notify = self.changed.defer_notify();
        let mut state = self.state()?;
        Self::check_in(&state)?;
        let Some(slot) = state.probes.get_mut(probe) else {
            let cause = state
                .failure
                .get_or_insert_with(|| {
                    self.contract_failure("probe exit has no installed probe slot")
                })
                .clone();
            notify.arm();
            return Err(cause);
        };
        if *slot != ProbeState::Pending {
            return Ok(false);
        }
        *slot = ProbeState::Withdrawn;
        state.retired += 1;
        notify.arm();
        Ok(true)
    }
    pub(crate) fn fail_probe(&self, probe: usize, cause: ExecutionFailure) -> ExecutionFailure {
        let notify = self.changed.defer_notify();
        let result = match self.state.lock() {
            Ok(mut state) => {
                if let Some(slot) = state.probes.get_mut(probe)
                    && *slot == ProbeState::Pending
                {
                    *slot = ProbeState::Failed;
                }
                state.failure.get_or_insert(cause).clone()
            }
            Err(poisoned) => poisoned.into_inner().failure.get_or_insert(cause).clone(),
        };
        notify.arm();
        result
    }
    /// The actual finalizer must already hold K Ready and the closed
    /// phase barrier. This method installs data, it does not prove those facts.
    pub(crate) fn publish(
        &self,
        artifact: Arc<A>,
        rows: usize,
    ) -> ExecutionResult<PublishResult<A>> {
        let notify = self.changed.defer_notify();
        let mut state = self.state()?;
        Self::check_in(&state)?;
        if state.published.is_some() {
            let cause = state
                .failure
                .get_or_insert_with(|| self.contract_failure("build published more than once"))
                .clone();
            notify.arm();
            return Err(cause);
        }
        if self.demand_in(&state) == RemainingDemand::Released {
            return Ok(PublishResult::NotNeeded(artifact));
        }
        state.published = Some(Published { artifact, rows });
        notify.arm();
        Ok(PublishResult::Installed)
    }
    pub(crate) fn published(&self) -> ExecutionResult<Option<Published<A>>> {
        let state = self.state()?;
        Self::check_in(&state)?;
        Ok(state.published.clone())
    }
    pub(crate) fn tail_status(&self) -> ExecutionResult<TailStatus<A>> {
        let state = self.state()?;
        Self::check_in(&state)?;
        if self.participants.tails == 0 {
            return Err(self.contract_failure("stage has no tail source"));
        }
        if self.demand_in(&state) == RemainingDemand::Released {
            return Ok(TailStatus::Withdrawn);
        }
        if state.retired > state.normal {
            return Ok(TailStatus::Unavailable);
        }
        if state.normal != self.participants.probes {
            return Ok(TailStatus::Pending);
        }
        Ok(match &state.published {
            Some(published) => TailStatus::Ready(published.clone()),
            None => TailStatus::Pending,
        })
    }
    /// Caller invokes this AFTER each actual finalization step. The original
    /// Observable generation is the waiter's register/recheck witness.
    pub(crate) fn finalized_step(&self) {
        self.changed.notify_observers();
    }
}
#[cfg(test)]
pub(crate) mod tests;
