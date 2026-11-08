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

use crate::actor::Actor;
use crate::actor_turn::{
    ActorHostTurn, ActorRegistration, ActorRuntime, ActorTurnError, CompletedEffect, EffectTicket,
};
use crate::sync::Arc;
use novarocks_control_runtime_core::{
    effect::EffectState,
    identity::{ActionId, ActorId, BatchId, InputId},
};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EffectRef {
    pub input: InputId,
    pub action: ActionId,
}

#[cfg(test)]
mod tests;
/// The actual runtime fact that releases a neutral FIFO routing position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectRelease {
    Started,
    Published,
    Completed,
}

/// Application policy supplies only neutral lane keys and ordering facts.
/// It does not execute effects or own their payloads, captures or permits.
pub trait EffectLanePolicy<A: Actor> {
    type LaneKey: Copy + Ord;
    fn classify(&self, effect: &A::Effect) -> Option<(Self::LaneKey, EffectRelease)>;
    /// The actor's admitted maximum effect inventory for one atomic step.
    fn effect_inventory_limit(&self) -> usize;
}
#[derive(Clone, Copy)]
struct Entry {
    effect: EffectRef,
    release: EffectRelease,
}
#[derive(Default)]
struct Lane {
    entries: VecDeque<Entry>,
    in_flight: Option<EffectRef>,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Ready<K> {
    Direct(EffectRef),
    Lane(K),
}
/// A non-authorizing sample of one original ready marker. Its host borrow
/// retains the actual actor incarnation; only an exact claim can issue a
/// ticket. Metadata is absent when the original runtime start gate is blocked.
pub struct EffectCandidate<'h, A: Actor, K> {
    origin: Arc<()>,
    runtime: ActorRuntime<A>,
    host: &'h ActorHostTurn<A>,
    marker: Ready<K>,
    effect: EffectRef,
    metadata: Option<A::Effect>,
}
impl<A: Actor, K> EffectCandidate<'_, A, K> {
    pub fn effect_ref(&self) -> EffectRef {
        self.effect
    }
    /// None requires no Source loan: defer this same marker and inspect only
    /// another bounded candidate. Some is a sample, never a start guarantee.
    pub fn metadata(&self) -> Option<&A::Effect> {
        self.metadata.as_ref()
    }
}
#[derive(Debug)]
pub enum DispatchError {
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
            Self::Runtime(error) => write!(f, "effect dispatch failed: {error}"),
            Self::OwnerFailed => f.write_str("effect owner failed"),
            Self::Capacity => f.write_str("effect routing exceeded its prepaid inventory"),
            Self::InvalidPublication => {
                f.write_str("effect publication fact is missing or out of order")
            }
            Self::MissingMetadata => f.write_str("effect lost its original routing metadata"),
            Self::InvalidCandidate => {
                f.write_str("effect candidate is foreign or no longer the current ready head")
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
#[derive(Debug, Default)]
pub struct IndexedBatches {
    /// Actual committed inputs. The host may now consume only their Core
    /// report leg; effect/capture/other-consumer retirement remains separate.
    pub inputs: Vec<InputId>,
    pub units: usize,
}

/// Original routing index and one coherent runtime inbox observation. Ready
/// markers are not total pending effects; same-lane tails and unindexed batches
/// are counted by runtime.states.pending and runtime.unindexed_states.pending.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EffectDispatchObservation {
    pub indexed_routing_positions: usize,
    pub ready_markers: usize,
    pub runtime: crate::observation::EffectInventoryObservation,
}
pub struct EffectDispatch<A: Actor, P: EffectLanePolicy<A>> {
    actor: ActorId,
    origin: Arc<()>,
    completion_runtime: Option<ActorRuntime<A>>,
    policy: P,
    after: Option<BatchId>,
    lanes: BTreeMap<P::LaneKey, Lane>,
    ready: VecDeque<Ready<P::LaneKey>>,
    indexed: usize,
    limit: usize,
}
impl<A: Actor, P: EffectLanePolicy<A>> EffectDispatch<A, P>
where
    A::Effect: Clone,
{
    /// Limit is the exact paid effect inventory from actor/role admission.
    pub fn new(actor: ActorId, limit: usize, policy: P) -> Self {
        assert!(limit > 0, "effect routing positions are prepaid");
        Self {
            actor,
            origin: Arc::new(()),
            completion_runtime: None,
            policy,
            after: None,
            lanes: BTreeMap::new(),
            ready: VecDeque::new(),
            indexed: 0,
            limit,
        }
    }
    /// Bind the original runtime used to validate completion receipts.
    pub fn with_completion_runtime(
        runtime: ActorRuntime<A>,
        actor: ActorId,
        limit: usize,
        policy: P,
    ) -> Self {
        let mut dispatch = Self::new(actor, limit, policy);
        dispatch.completion_runtime = Some(runtime);
        dispatch
    }
    pub fn index_committed(
        &mut self,
        runtime: &ActorRuntime<A>,
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
                self.policy.effect_inventory_limit(),
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
                if let Some((key, release)) = self.policy.classify(&metadata) {
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
    /// Peek at exactly one indexed head, without taking or rotating it. The
    /// actual host registration and runtime start predicate qualify this sample
    /// before any Source loan is borrowed. Only eligible metadata is cloned,
    /// under the existing neutral, bounded Clone contract; no callback runs
    /// under a runtime lock. A::Effect must not hide a large body clone here.
    pub fn peek_candidate<'h>(
        &self,
        runtime: &ActorRuntime<A>,
        host: &'h ActorHostTurn<A>,
    ) -> Result<Option<EffectCandidate<'h, A, P::LaneKey>>, DispatchError> {
        self.validate_candidate_host(runtime, host)?;
        let Some(marker) = self.ready.front().copied() else {
            return Ok(None);
        };
        let entry = self.candidate_entry(marker)?;
        let metadata = if runtime.effect_ready_for(host, entry.effect.input, entry.effect.action)? {
            Some(
                runtime
                    .pending_effect_metadata(self.actor, entry.effect.input, entry.effect.action)?
                    .ok_or(DispatchError::MissingMetadata)?,
            )
        } else {
            None
        };
        Ok(Some(EffectCandidate {
            origin: self.origin.clone(),
            runtime: runtime.clone(),
            host,
            marker,
            effect: entry.effect,
            metadata,
        }))
    }
    /// Claim only the sampled original head. NotReady preserves its marker and
    /// returns None, never another ticket. The caller still owns its original
    /// Source loan on every refusal and must return it before deferring.
    pub fn claim_candidate(
        &mut self,
        runtime: &ActorRuntime<A>,
        candidate: &EffectCandidate<'_, A, P::LaneKey>,
    ) -> Result<Option<EffectTicket<A::Effect>>, DispatchError> {
        if !runtime.same_runtime(&candidate.runtime) {
            return Err(DispatchError::InvalidCandidate);
        }
        let entry = self.validate_candidate(candidate)?;
        let ticket = match runtime.take_effect(self.actor, entry.effect.input, entry.effect.action)
        {
            Ok(ticket) => ticket,
            Err(ActorTurnError::Effect(
                novarocks_control_runtime_core::effect::EffectError::NotReady,
            )) => {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        self.ready
            .pop_front()
            .expect("the validated original ready marker");
        match candidate.marker {
            Ready::Direct(_) => self.indexed -= 1,
            Ready::Lane(key) if entry.release != EffectRelease::Started => {
                self.lanes
                    .get_mut(&key)
                    .expect("the original lane")
                    .in_flight = Some(entry.effect);
            }
            Ready::Lane(key) => self.pop_lane(key, entry.effect),
        }
        Ok(Some(ticket))
    }
    /// Rotate only this current original ready marker once. The runtime effect
    /// remains Pending and a lane's tail cannot overtake its head. No ticket,
    /// Work, quota or second queue is created. Charge each visit/defer and retain
    /// a finite pass boundary across budget yields; all-blocked/all-Full passes
    /// must park for real readiness/loan-retirement changes, not self-notify.
    /// Actual executor handoff refusal remains the original host's custody duty.
    pub fn defer_candidate(
        &mut self,
        candidate: &EffectCandidate<'_, A, P::LaneKey>,
    ) -> Result<(), DispatchError> {
        self.validate_candidate(candidate)?;
        let marker = self
            .ready
            .pop_front()
            .expect("the validated original ready marker");
        self.ready.push_back(marker);
        Ok(())
    }
    fn validate_candidate_host(
        &self,
        runtime: &ActorRuntime<A>,
        host: &ActorHostTurn<A>,
    ) -> Result<(), DispatchError> {
        if host.actor() != self.actor
            || self
                .completion_runtime
                .as_ref()
                .is_some_and(|original| !original.same_runtime(runtime))
        {
            return Err(DispatchError::InvalidCandidate);
        }
        runtime.validate_registration(&host.registration())?;
        if runtime.snapshot(self.actor)?.failed {
            return Err(DispatchError::OwnerFailed);
        }
        Ok(())
    }
    fn candidate_entry(&self, marker: Ready<P::LaneKey>) -> Result<Entry, DispatchError> {
        match marker {
            Ready::Direct(effect) => Ok(Entry {
                effect,
                release: EffectRelease::Started,
            }),
            Ready::Lane(key) => {
                let lane = self
                    .lanes
                    .get(&key)
                    .ok_or(DispatchError::InvalidCandidate)?;
                if lane.in_flight.is_some() {
                    return Err(DispatchError::InvalidCandidate);
                }
                lane.entries
                    .front()
                    .copied()
                    .ok_or(DispatchError::InvalidCandidate)
            }
        }
    }
    fn validate_candidate(
        &self,
        candidate: &EffectCandidate<'_, A, P::LaneKey>,
    ) -> Result<Entry, DispatchError> {
        if !Arc::ptr_eq(&self.origin, &candidate.origin)
            || self.ready.front().copied() != Some(candidate.marker)
        {
            return Err(DispatchError::InvalidCandidate);
        }
        self.validate_candidate_host(&candidate.runtime, candidate.host)?;
        let entry = self.candidate_entry(candidate.marker)?;
        if entry.effect != candidate.effect {
            return Err(DispatchError::InvalidCandidate);
        }
        Ok(entry)
    }
    pub fn take_ready(
        &mut self,
        runtime: &ActorRuntime<A>,
        budget: usize,
    ) -> Result<Option<EffectTicket<A::Effect>>, DispatchError> {
        let runtime = self.completion_runtime.as_ref().unwrap_or(runtime).clone();
        if runtime.snapshot(self.actor)?.failed {
            return Err(DispatchError::OwnerFailed);
        }
        for _ in 0..budget.min(self.ready.len()) {
            let ready = self.ready.pop_front().expect("bounded ready metadata");
            let entry = match ready {
                Ready::Direct(effect) => Entry {
                    effect,
                    release: EffectRelease::Started,
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
                    Err(ActorTurnError::Effect(
                        novarocks_control_runtime_core::effect::EffectError::NotReady,
                    )) => {
                        self.ready.push_back(ready);
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
            match ready {
                Ready::Direct(_) => self.indexed -= 1,
                Ready::Lane(key) if entry.release != EffectRelease::Started => {
                    self.lanes.get_mut(&key).unwrap().in_flight = Some(entry.effect);
                }
                Ready::Lane(key) => self.pop_lane(key, entry.effect),
            }
            return Ok(Some(ticket));
        }
        Ok(None)
    }
    fn pop_lane(&mut self, key: P::LaneKey, effect: EffectRef) {
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
    pub fn publication_committed(
        &mut self,
        runtime: &ActorRuntime<A>,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<(), DispatchError> {
        let runtime = self.completion_runtime.as_ref().unwrap_or(runtime).clone();
        let Some((key, EffectRelease::Published)) = self.policy.classify(&ticket.effect) else {
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
        // Application adapters declare these edges as publication conditions.
        // Satisfy them from the actual runtime fact, independently of slow
        // notification, capture destruction or final effect completion.
        for (descriptor, state) in runtime.effect_inventory(
            self.actor,
            ticket.input,
            None,
            self.policy.effect_inventory_limit(),
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
    /// Read the same original dispatch policy that released or retained this
    /// taken ticket. A host completion cannot invent a second ordered ACK for
    /// an action whose lane already advanced at start or publication.
    pub fn requires_completion(
        &self,
        ticket: &EffectTicket<A::Effect>,
    ) -> Result<bool, DispatchError> {
        let runtime = self
            .completion_runtime
            .as_ref()
            .ok_or(DispatchError::MissingMetadata)?;
        // This is physical completion, including an already Published route.
        // Publication authority has ended at that point; its original actual
        // Started/Published dispatch still owns the right to report completion.
        let _original = runtime.effect_completion_observer(ticket)?;
        if ticket.actor != self.actor {
            return Err(DispatchError::MissingMetadata);
        }
        let (key, release) = self
            .policy
            .classify(&ticket.effect)
            .ok_or(DispatchError::MissingMetadata)?;
        if release != EffectRelease::Completed {
            return Ok(false);
        }
        let reference = EffectRef {
            input: ticket.input,
            action: ticket.descriptor.id,
        };
        let lane = self.lanes.get(&key).ok_or(DispatchError::MissingMetadata)?;
        if lane.in_flight != Some(reference)
            || lane
                .entries
                .front()
                .is_none_or(|entry| entry.release != EffectRelease::Completed)
        {
            return Err(DispatchError::InvalidPublication);
        }
        Ok(true)
    }
    /// A synchronous metadata owner calls this after actual handoff completion,
    /// before retiring that input. An absent record is never a completion fact.
    pub fn completion_observed(
        &mut self,
        key: P::LaneKey,
        effect: EffectRef,
    ) -> Result<(), DispatchError> {
        self.complete_lane(key, effect)
    }
    /// The actual completion receipt survives automatic input retirement.
    /// Its source identity and original lane head must both match; numeric
    /// absence or a caller's success flag never authorizes lane advancement.
    pub fn completion_observed_with_receipt(
        &mut self,
        key: P::LaneKey,
        completed: &CompletedEffect<A>,
    ) -> Result<(), DispatchError> {
        if completed.registration().actor() != self.actor {
            return Err(DispatchError::InvalidPublication);
        }
        let effect = EffectRef {
            input: completed.input(),
            action: completed.descriptor().id,
        };
        self.validate_completed_lane_head(key, effect)?;
        self.completion_runtime
            .as_ref()
            .ok_or(DispatchError::MissingMetadata)?
            .acknowledge_completed_effect(completed)?;
        self.pop_lane(key, effect);
        Ok(())
    }
    fn validate_completed_lane_head(
        &self,
        key: P::LaneKey,
        effect: EffectRef,
    ) -> Result<(), DispatchError> {
        let lane = self.lanes.get(&key).ok_or(DispatchError::MissingMetadata)?;
        if lane.in_flight != Some(effect)
            || lane
                .entries
                .front()
                .is_none_or(|head| head.release != EffectRelease::Completed)
        {
            return Err(DispatchError::InvalidPublication);
        }
        Ok(())
    }
    fn complete_lane(&mut self, key: P::LaneKey, effect: EffectRef) -> Result<(), DispatchError> {
        let runtime = self
            .completion_runtime
            .as_ref()
            .ok_or(DispatchError::MissingMetadata)?;
        self.validate_completed_lane_head(key, effect)?;
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
            self.policy.effect_inventory_limit(),
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
    pub fn has_unindexed(&self, runtime: &ActorRuntime<A>) -> Result<bool, DispatchError> {
        Ok(!runtime
            .committed_batches(self.actor, self.after, 1)?
            .is_empty())
    }
    /// Diagnostic only. Busy is unavailable, and an unknown/foreign original
    /// registration is typed failure rather than an invented empty inventory.
    pub fn try_observation(
        &self,
        runtime: &ActorRuntime<A>,
        registration: &ActorRegistration,
    ) -> Result<Option<EffectDispatchObservation>, crate::observation::EffectObservationError> {
        if registration.actor() != self.actor {
            return Err(crate::observation::EffectObservationError::WrongActor);
        }
        let runtime = self.completion_runtime.as_ref().unwrap_or(runtime);
        Ok(runtime
            .try_effect_inventory_observation(registration, self.after)?
            .map(|snapshot| EffectDispatchObservation {
                indexed_routing_positions: self.indexed,
                ready_markers: self.ready.len(),
                runtime: snapshot,
            }))
    }
    /// Count current routing markers for one bounded readiness scan. This is
    /// not evidence that a dependency or physical action has completed.
    pub fn ready_positions(&self) -> usize {
        self.ready.len()
    }
    pub fn is_empty(&self) -> bool {
        self.indexed == 0
    }
    /// Discard only neutral routing markers after the failed owner has really
    /// drained its original runtime batches. This supplies no action exit.
    pub fn drain_failed_index_turn(
        &mut self,
        runtime: &ActorRuntime<A>,
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
    #[cfg(test)]
    pub fn clear_failed_index(&mut self, runtime: &ActorRuntime<A>) -> Result<(), DispatchError> {
        if !runtime.snapshot(self.actor)?.failed {
            return Err(DispatchError::InvalidPublication);
        }
        self.lanes.clear();
        self.ready.clear();
        self.indexed = 0;
        Ok(())
    }
}
