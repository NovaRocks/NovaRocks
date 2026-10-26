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
use novarocks_worker_protocol::{
    context::{ContextCore, ContextEffect},
    control::ControlEffect,
};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EffectRef {
    pub input: InputId,
    pub action: ActionId,
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum LaneKey {
    Task(TaskIdentity),
    Route(QueryContextRef),
}
#[derive(Clone, Copy)]
struct Entry {
    effect: EffectRef,
    publication: bool,
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
            after: None,
            lanes: BTreeMap::new(),
            ready: VecDeque::new(),
            indexed: 0,
            limit,
        }
    }
    fn classify(action: &WorkerAction) -> Option<(LaneKey, bool)> {
        match action {
            WorkerAction::Context(
                ContextEffect::PublishTask { task, .. }
                | ContextEffect::PublishMetrics { task, .. }
                | ContextEffect::PublishTaskConvergence { task, .. }
                | ContextEffect::PublishConclusion { task, .. },
            ) => Some((LaneKey::Task(*task), true)),
            // Only the preceding publications gate dispatch. The original
            // started stop job retains its own actual exit/capture liability.
            WorkerAction::Context(
                ContextEffect::StopTask { task, .. } | ContextEffect::RetireTask { task, .. },
            ) => Some((LaneKey::Task(*task), false)),
            WorkerAction::Control {
                effect: ControlEffect::Route { context, .. },
                ..
            } => Some((LaneKey::Route(*context), true)),
            _ => None,
        }
    }
    pub(crate) fn index_committed(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        budget: usize,
    ) -> Result<IndexedBatches, DispatchError> {
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
            let records =
                runtime.effect_inventory(self.actor, input, None, ContextCore::MAX_STEP_WORK)?;
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
                if let Some((key, publication)) = Self::classify(&metadata) {
                    let lane = self.lanes.entry(key).or_default();
                    if lane.entries.is_empty() {
                        self.ready.push_back(Ready::Lane(key));
                    }
                    lane.entries.push_back(Entry {
                        effect,
                        publication,
                    });
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
        if runtime.snapshot(self.actor)?.failed {
            return Err(DispatchError::OwnerFailed);
        }
        for _ in 0..budget.min(self.ready.len()) {
            let ready = self.ready.pop_front().expect("bounded ready metadata");
            let entry = match ready {
                Ready::Direct(effect) => Entry {
                    effect,
                    publication: false,
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
                Ready::Lane(key) if entry.publication => {
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
        let Some((key, true)) = Self::classify(&ticket.effect) else {
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
        for (descriptor, state) in
            runtime.effect_inventory(self.actor, ticket.input, None, ContextCore::MAX_STEP_WORK)?
        {
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
    pub(crate) fn is_empty(&self) -> bool {
        self.indexed == 0
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
