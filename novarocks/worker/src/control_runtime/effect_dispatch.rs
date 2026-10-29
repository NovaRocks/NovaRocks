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

//! Host routing indexes point into the runtime's actual committed batches.
//! Publications for one Task/route keep Core order; a slow body/capture exit
//! after publication does not prevent the next metadata publication.

use super::actors::{WorkerAction, WorkerActor};
use novarocks_control_runtime::driver::{ActorRuntime, DriverError, EffectTicket};
use novarocks_control_runtime_core::{
    effect::EffectState,
    identity::{ActionId, ActorId, BatchId, InputId},
};
use novarocks_execution_contract::{QueryContextRef, TaskIdentity};
use novarocks_worker_protocol::{context::ContextEffect, control::ControlEffect};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EffectRef {
    pub input: InputId,
    pub action: ActionId,
}

#[cfg(test)]
mod tests;
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum LaneKey {
    Task(TaskIdentity),
    Route(QueryContextRef),
    ControlMetadata,
    ContextMetadata,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Release {
    Started,
    Published,
    Completed,
}
#[derive(Clone, Copy)]
struct Entry {
    effect: EffectRef,
    release: Release,
}
#[derive(Default)]
struct Lane {
    entries: VecDeque<Entry>,
    in_flight: Option<EffectRef>,
}
#[derive(Clone, Copy)]
enum Ready {
    Direct(EffectRef),
    Lane(LaneKey),
}
#[derive(Debug)]
pub(crate) enum DispatchError {
    Runtime(DriverError),
    OwnerFailed,
    Capacity,
    InvalidPublication,
    MissingMetadata,
}
impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "Worker effect dispatch failed: {error}"),
            Self::OwnerFailed => f.write_str("Worker effect owner failed"),
            Self::Capacity => f.write_str("Worker effect routing exceeded its prepaid inventory"),
            Self::InvalidPublication => {
                f.write_str("Worker effect publication fact is missing or out of order")
            }
            Self::MissingMetadata => {
                f.write_str("Worker effect lost its original routing metadata")
            }
        }
    }
}
impl std::error::Error for DispatchError {}
impl From<DriverError> for DispatchError {
    fn from(error: DriverError) -> Self {
        Self::Runtime(error)
    }
}
#[derive(Debug, Default)]
pub(crate) struct IndexedBatches {
    /// Actual committed inputs. The host may now consume only their Core
    /// report leg; effect/capture/other-consumer retirement remains separate.
    pub inputs: Vec<InputId>,
    pub units: usize,
}

pub(crate) struct EffectDispatch {
    actor: ActorId,
    completion_runtime: Option<ActorRuntime<WorkerActor>>,
    control: bool,
    after: Option<BatchId>,
    lanes: BTreeMap<LaneKey, Lane>,
    ready: VecDeque<Ready>,
    indexed: usize,
    limit: usize,
}
impl EffectDispatch {
    /// Limit is the exact paid effect inventory from actor/role admission.
    pub(crate) fn new(actor: ActorId, limit: usize) -> Self {
        assert!(limit > 0, "effect routing positions are prepaid");
        Self {
            actor,
            completion_runtime: None,
            control: false,
            after: None,
            lanes: BTreeMap::new(),
            ready: VecDeque::new(),
            indexed: 0,
            limit,
        }
    }
    /// The root serializes only its metadata transitions. Starting a host I/O
    /// transfers the original ticket and releases this ordering position; its
    /// real body/capture exit remains independently owned by that host job.
    pub(crate) fn control(
        runtime: ActorRuntime<WorkerActor>,
        actor: ActorId,
        limit: usize,
    ) -> Self {
        let mut dispatch = Self::new(actor, limit);
        dispatch.completion_runtime = Some(runtime);
        dispatch.control = true;
        dispatch
    }
    /// Context-to-Control metadata keeps the producing actor's order across
    /// batches. Completion means the next owner accepted the neutral handoff;
    /// it never waits for a reply from that owner or for a host I/O body.
    pub(crate) fn context(
        runtime: ActorRuntime<WorkerActor>,
        actor: ActorId,
        limit: usize,
    ) -> Self {
        let mut dispatch = Self::new(actor, limit);
        dispatch.completion_runtime = Some(runtime);
        dispatch
    }
    fn classify(&self, action: &WorkerAction) -> Option<(LaneKey, Release)> {
        if self.control {
            let release = match action {
                WorkerAction::Control {
                    effect: ControlEffect::Route { .. },
                    ..
                } => Release::Published,
                WorkerAction::Control {
                    effect:
                        ControlEffect::AdmissionReply { .. }
                        | ControlEffect::DomainFailureReply { .. }
                        | ControlEffect::Reply { .. }
                        | ControlEffect::WakeInternal
                        | ControlEffect::RouteEstablish { .. }
                        | ControlEffect::CancelEstablishRoute { .. }
                        | ControlEffect::RetireReservation(_),
                    ..
                }
                | WorkerAction::ControlReply(_)
                | WorkerAction::AdmissionEpoch(_) => Release::Completed,
                WorkerAction::Control { .. } => Release::Started,
                _ => return None,
            };
            return Some((LaneKey::ControlMetadata, release));
        }
        match action {
            WorkerAction::Context(
                ContextEffect::EstablishProbed { .. }
                | ContextEffect::EstablishmentDecided { .. }
                | ContextEffect::RetainContext { .. },
            ) => Some((LaneKey::ContextMetadata, Release::Completed)),
            WorkerAction::Context(
                ContextEffect::PublishTask { task, .. }
                | ContextEffect::PublishMetrics { task, .. }
                | ContextEffect::PublishTaskConvergence { task, .. }
                | ContextEffect::SummaryConsumed { task, .. }
                | ContextEffect::PublishConclusion { task, .. }
                | ContextEffect::RetireTask { task, .. }
                | ContextEffect::FinalizeTaskExecution { task, .. }
                | ContextEffect::SettlementBarrier { task, .. },
            ) => Some((LaneKey::Task(*task), Release::Published)),
            WorkerAction::Context(ContextEffect::PrepareReadPayload { task, .. }) => {
                Some((LaneKey::Task(*task), Release::Completed))
            }
            // Only the preceding publications gate dispatch. The original
            // started stop job retains its own actual exit/capture liability.
            WorkerAction::Context(
                ContextEffect::StopTask { task, .. }
                | ContextEffect::DisposeSource { task, .. }
                | ContextEffect::EnqueuePreparation { task, .. }
                | ContextEffect::DisposeLoadedInput { task, .. }
                | ContextEffect::InitialPreparationReady { task, .. }
                | ContextEffect::ApplyTaskClose { task, .. },
            ) => Some((LaneKey::Task(*task), Release::Started)),
            // A later RPC reply observes the same Task publication order.
            // Starting its original delivery releases only this ordering
            // position; the actual reply body and wire holder exit separately.
            WorkerAction::Reply(reply) => reply
                .task
                .map(|task| (LaneKey::Task(task), Release::Started)),
            WorkerAction::Control {
                effect: ControlEffect::Route { context, .. },
                ..
            } => Some((LaneKey::Route(*context), Release::Published)),
            _ => None,
        }
    }
    pub(crate) fn index_committed(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        budget: usize,
    ) -> Result<IndexedBatches, DispatchError> {
        let runtime = self.completion_runtime.as_ref().unwrap_or(runtime).clone();
        if runtime.snapshot(self.actor)?.failed {
            return Err(DispatchError::OwnerFailed);
        }
        let mut result = IndexedBatches::default();
        while result.units < budget {
            let Some((ordinal, input)) = runtime
                .committed_batches(self.actor, self.after, 1)?
                .first()
                .copied()
            else {
                break;
            };
            let records = runtime.effect_inventory(
                self.actor,
                input,
                None,
                crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
            )?;
            let units = 1 + records.len();
            if units > budget - result.units {
                break;
            }
            if records.len() > self.limit - self.indexed {
                return Err(DispatchError::Capacity);
            }
            // These clones are neutral routing metadata. Original effect and
            // guard custody stay in the runtime until their actual host claim.
            for (descriptor, state) in records {
                if state != EffectState::Pending {
                    return Err(DispatchError::MissingMetadata);
                }
                let metadata = runtime
                    .pending_effect_metadata(self.actor, input, descriptor.id)?
                    .ok_or(DispatchError::MissingMetadata)?;
                let effect = EffectRef {
                    input,
                    action: descriptor.id,
                };
                if let Some((key, release)) = self.classify(&metadata) {
                    let lane = self.lanes.entry(key).or_default();
                    if lane.entries.is_empty() {
                        self.ready.push_back(Ready::Lane(key));
                    }
                    lane.entries.push_back(Entry { effect, release });
                } else {
                    self.ready.push_back(Ready::Direct(effect));
                }
                self.indexed += 1;
            }
            self.after = Some(ordinal);
            result.inputs.push(input);
            result.units += units;
        }
        Ok(result)
    }
    pub(crate) fn take_ready(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        budget: usize,
    ) -> Result<Option<EffectTicket<WorkerAction>>, DispatchError> {
        let runtime = self.completion_runtime.as_ref().unwrap_or(runtime).clone();
        if runtime.snapshot(self.actor)?.failed {
            return Err(DispatchError::OwnerFailed);
        }
        for _ in 0..budget.min(self.ready.len()) {
            let ready = self.ready.pop_front().expect("bounded ready metadata");
            let entry = match ready {
                Ready::Direct(effect) => Entry {
                    effect,
                    release: Release::Started,
                },
                Ready::Lane(key) => {
                    let lane = self.lanes.get(&key).expect("queued publication lane");
                    assert!(lane.in_flight.is_none());
                    *lane.entries.front().expect("queued lane head")
                }
            };
            let ticket =
                match runtime.take_effect(self.actor, entry.effect.input, entry.effect.action) {
                    Ok(ticket) => ticket,
                    Err(DriverError::Effect(
                        novarocks_control_runtime_core::effect::EffectError::NotReady,
                    )) => {
                        self.ready.push_back(ready);
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
            match ready {
                Ready::Direct(_) => self.indexed -= 1,
                Ready::Lane(key) if entry.release != Release::Started => {
                    self.lanes.get_mut(&key).unwrap().in_flight = Some(entry.effect);
                }
                Ready::Lane(key) => self.pop_lane(key, entry.effect),
            }
            return Ok(Some(ticket));
        }
        Ok(None)
    }
    fn pop_lane(&mut self, key: LaneKey, effect: EffectRef) {
        let lane = self.lanes.get_mut(&key).expect("live publication lane");
        assert_eq!(
            lane.entries.pop_front().map(|entry| entry.effect),
            Some(effect)
        );
        lane.in_flight = None;
        self.indexed -= 1;
        if lane.entries.is_empty() {
            self.lanes.remove(&key);
        } else {
            self.ready.push_back(Ready::Lane(key));
        }
    }
    /// Publication releases only ordering, never its body's capacity, sources,
    /// captures or input. The caller still owns and completes the original ticket.
    pub(crate) fn publication_committed(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), DispatchError> {
        let runtime = self.completion_runtime.as_ref().unwrap_or(runtime).clone();
        let Some((key, Release::Published)) = self.classify(&ticket.effect) else {
            return Err(DispatchError::InvalidPublication);
        };
        if ticket.actor != self.actor {
            return Err(DispatchError::InvalidPublication);
        }
        let effect = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        if self.lanes.get(&key).and_then(|lane| lane.in_flight) != Some(effect) {
            return Err(DispatchError::InvalidPublication);
        }
        let actual = runtime.effect_inventory(
            self.actor,
            ticket.input,
            ActionId::new(effect.action.get() - 1),
            1,
        )?;
        if actual.first().is_none_or(|(descriptor, state)| {
            descriptor.id != effect.action || *state != EffectState::Published
        }) {
            return Err(DispatchError::InvalidPublication);
        }
        // Worker adapters declare these edges as publication conditions.
        // Satisfy them from the actual runtime fact, independently of slow
        // notification, capture destruction or final effect completion.
        for (descriptor, state) in runtime.effect_inventory(
            self.actor,
            ticket.input,
            None,
            crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
        )? {
            if state == EffectState::Pending && descriptor.dependencies.contains(&effect.action) {
                runtime.acknowledge_dependency(
                    self.actor,
                    ticket.input,
                    descriptor.id,
                    effect.action,
                )?;
            }
        }
        self.pop_lane(key, effect);
        Ok(())
    }
    /// A synchronous metadata owner calls this after actual handoff completion,
    /// before retiring that input. An absent record is never a completion fact.
    pub(crate) fn completion_observed(&mut self, effect: EffectRef) -> Result<(), DispatchError> {
        let key = if self.control {
            LaneKey::ControlMetadata
        } else {
            LaneKey::ContextMetadata
        };
        self.complete_lane(key, effect)
    }
    /// Only the original synchronous preparation's actual completion releases
    /// this Task ordering position. Getter I/O and delivery captures keep their
    /// own original roles; this method neither pins payloads nor signs exits.
    pub(crate) fn task_preparation_completed(
        &mut self,
        task: TaskIdentity,
        effect: EffectRef,
    ) -> Result<(), DispatchError> {
        if self.control {
            return Err(DispatchError::InvalidPublication);
        }
        self.complete_lane(LaneKey::Task(task), effect)
    }
    fn complete_lane(&mut self, key: LaneKey, effect: EffectRef) -> Result<(), DispatchError> {
        let runtime = self
            .completion_runtime
            .as_ref()
            .ok_or(DispatchError::MissingMetadata)?;
        let lane = self.lanes.get(&key).ok_or(DispatchError::MissingMetadata)?;
        if lane.in_flight != Some(effect)
            || lane
                .entries
                .front()
                .is_none_or(|head| head.release != Release::Completed)
        {
            return Err(DispatchError::InvalidPublication);
        }
        let actual = runtime.effect_inventory(
            self.actor,
            effect.input,
            ActionId::new(effect.action.get() - 1),
            1,
        )?;
        if actual.first().is_none_or(|(descriptor, state)| {
            descriptor.id != effect.action || *state != EffectState::Completed
        }) {
            return Err(DispatchError::InvalidPublication);
        }
        // The actual completed predecessor, not caller-supplied success,
        // acknowledges the same committed batch's original reply dependency.
        for (descriptor, state) in runtime.effect_inventory(
            self.actor,
            effect.input,
            None,
            crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
        )? {
            if state == EffectState::Pending && descriptor.dependencies.contains(&effect.action) {
                runtime.acknowledge_dependency(
                    self.actor,
                    effect.input,
                    descriptor.id,
                    effect.action,
                )?;
            }
        }
        self.pop_lane(key, effect);
        Ok(())
    }
    /// Waiting lanes retain custody but do not make a host turn runnable.
    pub(crate) fn has_unindexed(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
    ) -> Result<bool, DispatchError> {
        Ok(!runtime
            .committed_batches(self.actor, self.after, 1)?
            .is_empty())
    }
    /// Count current routing markers for one bounded readiness scan. This is
    /// not evidence that a dependency or physical action has completed.
    pub(crate) fn ready_positions(&self) -> usize {
        self.ready.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.indexed == 0
    }
    /// Discard only neutral routing markers after the failed owner has really
    /// drained its original runtime batches. This supplies no action exit.
    pub(crate) fn drain_failed_index_turn(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        budget: usize,
    ) -> Result<bool, DispatchError> {
        if !runtime.snapshot(self.actor)?.failed {
            return Err(DispatchError::InvalidPublication);
        }
        for _ in 0..budget {
            if let Some(ready) = self.ready.pop_front() {
                if matches!(ready, Ready::Direct(_)) {
                    self.indexed -= 1;
                }
                continue;
            }
            let Some((&key, lane)) = self.lanes.first_key_value() else {
                break;
            };
            let empty = lane.entries.is_empty();
            if empty {
                self.lanes.remove(&key);
            } else {
                let lane = self.lanes.get_mut(&key).expect("original failed lane");
                lane.entries.pop_front();
                lane.in_flight = None;
                self.indexed -= 1;
            }
        }
        Ok(self.ready.is_empty() && self.lanes.is_empty() && self.indexed == 0)
    }
    /// Indexes hold only plain references. The failure owner recovers original
    /// runtime/resource custody; clearing these never supplies any exit proof.
    pub(crate) fn clear_failed_index(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
    ) -> Result<(), DispatchError> {
        if !runtime.snapshot(self.actor)?.failed {
            return Err(DispatchError::InvalidPublication);
        }
        self.lanes.clear();
        self.ready.clear();
        self.indexed = 0;
        Ok(())
    }
}
