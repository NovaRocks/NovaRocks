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

//! Exact installed join participants, publication and probe completion facts.
//! This protocol owns no scheduler, CPU position, Task verdict or row storage.
// Design: ADR-0212 (docs/adr/ADR-0212-shared-join-stages-and-exact-demand-retirement.md)

use super::build_state::build_contract;
use crate::{
    exec::{
        operators::local_exchanger::LocalExchanger,
        pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    },
    runtime::{
        observable::Observable,
        task::{ExecutionFailure, ExecutionResult},
    },
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    alloc::Layout,
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JoinParticipants {
    pub(crate) inputs: usize,
    pub(crate) finalizers: NonZeroUsize,
    pub(crate) probes: usize,
    pub(crate) tails: usize,
}
impl JoinParticipants {
    pub(crate) fn handoff_producers(self) -> ExecutionResult<Option<usize>> {
        if self.tails == 0 {
            return Ok(None);
        }
        self.probes
            .checked_add(self.tails)
            .map(Some)
            .ok_or_else(arithmetic)
    }
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
/// Exact immutable artifact data is supplied by its actual P03/P07 author.
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
/// Probe-only joins retain no output exchange. The other variant is the SAME
/// original Q+T-producer Handoff, never a separate queue or demand counter.
enum OutputDemand {
    ProbeSlots,
    Handoff(Arc<LocalExchanger>),
}
pub(crate) struct JoinCompletion<A> {
    participants: JoinParticipants,
    state: Mutex<CompletionState<A>>,
    output: OutputDemand,
    changed: Arc<Observable>,
    _storage: ValueTypeStorageLifetime,
}
impl<A> JoinCompletion<A> {
    /// The builder supplies its original funded Observable and, only for T>0,
    /// its actual unique Handoff. This does not register or allocate observers.
    pub(crate) fn new(
        participants: JoinParticipants,
        handoff: Option<Arc<LocalExchanger>>,
        changed: Arc<Observable>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Arc<Self>> {
        control.checkpoint(0)?;
        let output = match (participants.handoff_producers()?, handoff) {
            (None, None) => OutputDemand::ProbeSlots,
            (Some(expected), Some(handoff)) if handoff.remaining_producers() == expected => {
                OutputDemand::Handoff(handoff)
            }
            _ => {
                return Err(build_contract(
                    "join output demand differs from installed Q/T participants",
                ));
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
                changed,
                _storage: owner.storage_lifetime(),
            },
        )
    }
    pub(crate) fn participants(&self) -> JoinParticipants {
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
    fn state(&self) -> ExecutionResult<MutexGuard<'_, CompletionState<A>>> {
        match self.state.lock() {
            Ok(state) => Ok(state),
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                let cause = state
                    .failure
                    .get_or_insert_with(|| build_contract("join completion state was poisoned"))
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
                .ok_or_else(|| build_contract("join probe is not an installed participant"))?;
            if *slot != ProbeState::Pending {
                return Err(build_contract("join probe settled more than once"));
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
                .get_or_insert_with(|| build_contract("join exit has no installed probe slot"))
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
    /// The actual finalizer must already hold K Ready and the closed join
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
                .get_or_insert_with(|| build_contract("join build published more than once"))
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
            return Err(build_contract("probe-only join has no tail source"));
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
    /// Caller invokes this AFTER each actual K/join complete. The original
    /// Observable generation is the waiter's register/recheck witness.
    pub(crate) fn finalized_step(&self) {
        self.changed.notify_observers();
    }
}
pub(super) fn arithmetic() -> ExecutionFailure {
    ConstructionFailure::from(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
    .into()
}
pub(super) fn funded_vec<T>(
    owner: &dyn InputConstructionOwner,
    count: usize,
) -> ExecutionResult<Vec<T>> {
    let layout = Layout::array::<T>(count).map_err(|_| arithmetic())?;
    if count != 0 {
        owner.request(layout)?;
    }
    let mut result = Vec::new();
    result.try_reserve_exact(count).map_err(|source| {
        ExecutionFailure::from_domain(novarocks_value_arrays::ValueError::Allocation {
            requested: layout.size(),
            source,
        })
    })?;
    Ok(result)
}
/// Value must retain its actual metadata creator until its last field exits.
pub(super) fn funded_shared<T>(
    owner: &dyn InputConstructionOwner,
    value: T,
) -> ExecutionResult<Arc<T>> {
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
            .map_err(ConstructionFailure::from)?,
    )?;
    Ok(Arc::new(value))
}

#[cfg(test)]
#[path = "join_completion_tests.rs"]
pub(super) mod tests;
