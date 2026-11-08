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

//! Worker lane classification over the shared effect dispatch mechanism.

use super::actors::{WorkerAction, WorkerActor};
use novarocks_control_runtime::{
    actor_turn::{ActorRuntime, ActorTurnError, CompletedEffect, EffectTicket},
    effect_dispatch::{self, EffectLanePolicy, EffectRelease},
};
use novarocks_control_runtime_core::identity::ActorId;
use novarocks_execution_contract::{ParticipationRef, TaskIdentity};
use novarocks_worker_protocol::{control::ControlEffect, participation::ParticipationEffect};

pub(crate) use effect_dispatch::{EffectRef, IndexedBatches};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum LaneKey {
    Task(TaskIdentity),
    Route(ParticipationRef),
    ControlMetadata,
    ParticipationMetadata,
}

struct WorkerLanePolicy {
    control: bool,
}
impl EffectLanePolicy<WorkerActor> for WorkerLanePolicy {
    type LaneKey = LaneKey;
    fn classify(&self, action: &WorkerAction) -> Option<(LaneKey, EffectRelease)> {
        if self.control {
            let release = match action {
                WorkerAction::Control {
                    effect: ControlEffect::Route { .. },
                    ..
                } => EffectRelease::Published,
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
                | WorkerAction::AdmissionEpoch(_) => EffectRelease::Completed,
                WorkerAction::Control { .. } => EffectRelease::Started,
                _ => return None,
            };
            return Some((LaneKey::ControlMetadata, release));
        }
        match action {
            WorkerAction::Participation(
                ParticipationEffect::EstablishProbed { .. }
                | ParticipationEffect::EstablishmentDecided { .. }
                | ParticipationEffect::RetainParticipation { .. },
            ) => Some((LaneKey::ParticipationMetadata, EffectRelease::Completed)),
            WorkerAction::Participation(
                ParticipationEffect::PublishTask { task, .. }
                | ParticipationEffect::PublishMetrics { task, .. }
                | ParticipationEffect::PublishTaskConvergence { task, .. }
                | ParticipationEffect::SummaryConsumed { task, .. }
                | ParticipationEffect::PublishConclusion { task, .. }
                | ParticipationEffect::RetireTask { task, .. }
                | ParticipationEffect::FinalizeTaskExecution { task, .. }
                | ParticipationEffect::SettlementBarrier { task, .. },
            ) => Some((LaneKey::Task(*task), EffectRelease::Published)),
            WorkerAction::Participation(ParticipationEffect::PrepareReadPayload {
                task, ..
            }) => Some((LaneKey::Task(*task), EffectRelease::Completed)),
            // Only the preceding publications gate dispatch. The original
            // started stop job retains its own actual exit/capture liability.
            WorkerAction::Participation(
                ParticipationEffect::StopTask { task, .. }
                | ParticipationEffect::DisposeSource { task, .. }
                | ParticipationEffect::EnqueuePreparation { task, .. }
                | ParticipationEffect::DisposeLoadedInput { task, .. }
                | ParticipationEffect::InitialPreparationReady { task, .. }
                | ParticipationEffect::ApplyTaskClose { task, .. },
            ) => Some((LaneKey::Task(*task), EffectRelease::Started)),
            // A later RPC reply observes the same Task publication order.
            // Starting its original delivery releases only this ordering
            // position; the actual reply body and wire holder exit separately.
            WorkerAction::Reply(reply) => reply
                .task
                .map(|task| (LaneKey::Task(task), EffectRelease::Started)),
            WorkerAction::Control {
                effect: ControlEffect::Route { participation, .. },
                ..
            } => Some((LaneKey::Route(*participation), EffectRelease::Published)),
            _ => None,
        }
    }

    fn effect_inventory_limit(&self) -> usize {
        super::actors::ParticipationActor::MAX_STEP_ACTIONS
    }
}
#[derive(Debug)]
pub(crate) enum DispatchError {
    Runtime(ActorTurnError),
    OwnerFailed,
    Capacity,
    InvalidPublication,
    MissingMetadata,
    InvalidCandidate,
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
            Self::InvalidCandidate => {
                f.write_str("Worker effect candidate differs from its original ready head")
            }
        }
    }
}
impl std::error::Error for DispatchError {}
impl From<ActorTurnError> for DispatchError {
    fn from(error: ActorTurnError) -> Self {
        Self::Runtime(error)
    }
}

impl From<effect_dispatch::DispatchError> for DispatchError {
    fn from(error: effect_dispatch::DispatchError) -> Self {
        match error {
            effect_dispatch::DispatchError::Runtime(error) => Self::Runtime(error),
            effect_dispatch::DispatchError::OwnerFailed => Self::OwnerFailed,
            effect_dispatch::DispatchError::Capacity => Self::Capacity,
            effect_dispatch::DispatchError::InvalidPublication => Self::InvalidPublication,
            effect_dispatch::DispatchError::MissingMetadata => Self::MissingMetadata,
            effect_dispatch::DispatchError::InvalidCandidate => Self::InvalidCandidate,
        }
    }
}

pub(crate) struct EffectDispatch {
    shared: effect_dispatch::EffectDispatch<WorkerActor, WorkerLanePolicy>,
    control: bool,
}
impl EffectDispatch {
    #[cfg(test)]
    pub(crate) fn new(actor: ActorId, limit: usize) -> Self {
        Self {
            shared: effect_dispatch::EffectDispatch::new(
                actor,
                limit,
                WorkerLanePolicy { control: false },
            ),
            control: false,
        }
    }
    pub(crate) fn control(
        runtime: ActorRuntime<WorkerActor>,
        actor: ActorId,
        limit: usize,
    ) -> Self {
        Self {
            shared: effect_dispatch::EffectDispatch::with_completion_runtime(
                runtime,
                actor,
                limit,
                WorkerLanePolicy { control: true },
            ),
            control: true,
        }
    }
    pub(crate) fn participation(
        runtime: ActorRuntime<WorkerActor>,
        actor: ActorId,
        limit: usize,
    ) -> Self {
        Self {
            shared: effect_dispatch::EffectDispatch::with_completion_runtime(
                runtime,
                actor,
                limit,
                WorkerLanePolicy { control: false },
            ),
            control: false,
        }
    }
    fn completion_lane(&self) -> LaneKey {
        if self.control {
            LaneKey::ControlMetadata
        } else {
            LaneKey::ParticipationMetadata
        }
    }
    pub(crate) fn index_committed(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        budget: usize,
    ) -> Result<IndexedBatches, DispatchError> {
        self.shared
            .index_committed(runtime, budget)
            .map_err(Into::into)
    }
    pub(crate) fn take_ready(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        budget: usize,
    ) -> Result<Option<EffectTicket<WorkerAction>>, DispatchError> {
        self.shared.take_ready(runtime, budget).map_err(Into::into)
    }
    pub(crate) fn publication_committed(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), DispatchError> {
        self.shared
            .publication_committed(runtime, ticket)
            .map_err(Into::into)
    }
    pub(crate) fn requires_completion(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<bool, DispatchError> {
        self.shared.requires_completion(ticket).map_err(Into::into)
    }
    pub(crate) fn completion_observed(&mut self, effect: EffectRef) -> Result<(), DispatchError> {
        self.shared
            .completion_observed(self.completion_lane(), effect)
            .map_err(Into::into)
    }
    pub(crate) fn completion_observed_with_receipt(
        &mut self,
        completed: &CompletedEffect<WorkerActor>,
    ) -> Result<(), DispatchError> {
        self.shared
            .completion_observed_with_receipt(self.completion_lane(), completed)
            .map_err(Into::into)
    }
    pub(crate) fn task_preparation_completed(
        &mut self,
        task: TaskIdentity,
        effect: EffectRef,
    ) -> Result<(), DispatchError> {
        if self.control {
            return Err(DispatchError::InvalidPublication);
        }
        self.shared
            .completion_observed(LaneKey::Task(task), effect)
            .map_err(Into::into)
    }
    pub(crate) fn has_unindexed(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
    ) -> Result<bool, DispatchError> {
        self.shared.has_unindexed(runtime).map_err(Into::into)
    }
    pub(crate) fn ready_positions(&self) -> usize {
        self.shared.ready_positions()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }
    pub(crate) fn drain_failed_index_turn(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        budget: usize,
    ) -> Result<bool, DispatchError> {
        self.shared
            .drain_failed_index_turn(runtime, budget)
            .map_err(Into::into)
    }
    /// Test cleanup drops only neutral indexes through the same bounded drain.
    /// It does not consume runtime tickets or establish physical effect exit.
    #[cfg(test)]
    pub(crate) fn clear_failed_index(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
    ) -> Result<(), DispatchError> {
        while !self.drain_failed_index_turn(runtime, 1)? {}
        Ok(())
    }
}
