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

mod consumer_ingress;
mod dedupe;
mod final_domain_completion;
mod inbound;
#[cfg(test)]
mod live_aggregate_conformance_tests;
#[cfg(test)]
mod m3a_tests;
#[cfg(test)]
mod m3b_tests;
#[cfg(test)]
mod m3c_tests;
#[cfg(test)]
mod m4_conformance_tests;
mod materialization;
mod memory;
mod native_execution;
mod producer;
mod registry;
mod reliable_transport;
mod subscription;

#[cfg(test)]
pub(crate) use self::subscription::{NativeAcquireGateGuard, install_native_acquire_gate_for_test};

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::ThreadId;
use std::time::Instant;

use crate::common::types::UniqueId;
use crate::runtime_filter::codec::artifact::{
    ArtifactDecodeExpectation, ArtifactWireCodecError, encode_artifact_bundle,
    encode_completed_without_artifact, encode_unavailable, max_encoded_len_for_artifact_budget,
};
use crate::runtime_filter::codec::producer::encode_producer_failure;
use crate::runtime_filter::core::channel::ChannelAction;
use crate::runtime_filter::model::contract::{BindingId, ChannelId, ConsumerActivation};
use crate::runtime_filter::port::artifact::ConsumerArtifactProfile;
use crate::runtime_filter::port::events::{
    RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink, TransportEventKind,
    TransportFailOpenReason, TransportRouteEventIdentity,
};
use crate::runtime_filter::port::identity::RouteEdgeId;
use crate::runtime_filter::port::install::RuntimeFilterParticipantInstall;
use crate::runtime_filter::port::producer::{
    FinalDomainProducerAdapter, InstallContractError, InstallContractErrorKind, InstallOutcome,
    OrderedBoundProducerAdapter, ProducerAdapter, ProducerFailureReason, ProducerHandle,
    ProducerHandleWeak, ProducerPortKind, RuntimeContractViolation, RuntimeContractViolationKind,
    SubmitOutcome, TopKSummaryProducerAdapter,
};
use crate::runtime_filter::port::routing::{
    RuntimeFilterDeliveryRouteIntent, RuntimeFilterProducerRouteIntent,
    RuntimeFilterRouteContractError, RuntimeFilterRouteDecision,
};
use crate::runtime_filter::port::subscription::{
    ArtifactDeliveryOutcome, LiveTerminal, SubscriptionHandle, SubscriptionKind,
};
use crate::runtime_filter::port::support::{RuntimeFilterClock, RuntimeFilterMemoryAccount};
use crate::runtime_filter::port::transport::{
    ProducerInstanceRouteIdentity, RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind,
    RuntimeFilterRouteIdentity,
};

pub(crate) use self::consumer_ingress::{
    InboundConsumerDispatchError, InboundConsumerDispatchErrorKind, InboundConsumerDispatchOutcome,
};
use self::dedupe::IngressDedupe;
use self::final_domain_completion::FinalDomainCompletionSessionRegistry;
pub(crate) use self::final_domain_completion::{
    FinalDomainCompletionSession, FinalDomainPartitionCommitter, FinalDomainServiceIssuancePermit,
};
pub(crate) use self::inbound::{
    InboundProducerDispatchError, InboundProducerDispatchErrorKind, InboundProducerDispatchOutcome,
};
#[cfg(test)]
use self::materialization::run_materialization_jobs;
use self::materialization::{
    ClaimedMaterializationJob, MaterializationWorkClaim, PublishCommitOutcome,
    claim_materialization_jobs, execute_materialization_jobs, take_materialization_launch_events,
};
pub(crate) use self::native_execution::{
    InstalledRuntimeFilterExecutionContract, NativeRuntimeFilterExecutionContext,
    ResolvedNativeConsumer, ResolvedNativeProducer,
};
use self::producer::{RemoteProducerAdapter, RemoteProducerState, ServiceProducerAdapter};
use self::registry::{DeploymentRegistry, InstalledDeployment};
pub(crate) use self::reliable_transport::ReliableTransportPolicy;
use self::reliable_transport::{
    ReliableEnvelopeTransport, ReliableFailedOpenWork, ReliableSendOutcome, TransportResourceLimit,
};

struct EventQueueState {
    draining: bool,
    draining_thread: Option<ThreadId>,
    next_batch_id: u64,
    batches: VecDeque<EventBatch>,
}

struct EventBatch {
    id: u64,
    ready: bool,
    events: VecDeque<RuntimeFilterEvent>,
    completion: Arc<EventBatchCompletion>,
}

struct EventBatchHandle {
    id: u64,
    completion: Arc<EventBatchCompletion>,
}

struct EventBatchCompletion {
    state: Mutex<EventBatchCompletionState>,
    changed: Condvar,
}

#[derive(Default)]
struct EventBatchCompletionState {
    completed: bool,
    callbacks: Vec<Box<dyn FnOnce() + Send>>,
}

impl Default for EventBatchCompletion {
    fn default() -> Self {
        Self {
            state: Mutex::new(EventBatchCompletionState::default()),
            changed: Condvar::new(),
        }
    }
}

impl EventBatchCompletion {
    fn wait(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while !state.completed {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    fn complete(&self) {
        let callbacks = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.completed {
                return;
            }
            state.completed = true;
            self.changed.notify_all();
            std::mem::take(&mut state.callbacks)
        };
        for callback in callbacks {
            callback();
        }
    }

    fn on_complete(&self, callback: impl FnOnce() + Send + 'static) {
        let mut callback = Some(Box::new(callback) as Box<dyn FnOnce() + Send>);
        let run_now = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.completed {
                true
            } else {
                state
                    .callbacks
                    .push(callback.take().expect("completion callback is present"));
                false
            }
        };
        if run_now {
            callback.expect("completed callback remains present")();
        }
    }

    fn completed() -> Arc<Self> {
        let completion = Arc::new(Self::default());
        completion.complete();
        completion
    }
}

struct EventEmitter {
    sink: Arc<dyn RuntimeFilterEventSink>,
    state: Mutex<EventQueueState>,
    #[cfg(test)]
    recorded: Mutex<Vec<RuntimeFilterEvent>>,
    #[cfg(test)]
    after_publish_ready: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl EventEmitter {
    fn new(sink: Arc<dyn RuntimeFilterEventSink>) -> Self {
        Self {
            sink,
            state: Mutex::new(EventQueueState {
                draining: false,
                draining_thread: None,
                next_batch_id: 0,
                batches: VecDeque::new(),
            }),
            #[cfg(test)]
            recorded: Mutex::new(Vec::new()),
            #[cfg(test)]
            after_publish_ready: Mutex::new(None),
        }
    }

    fn is_draining_on_current_thread(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .draining_thread
            == Some(std::thread::current().id())
    }

    fn prequeue(
        &self,
        events: impl IntoIterator<Item = RuntimeFilterEvent>,
    ) -> Option<EventBatchHandle> {
        let events = events.into_iter().collect::<VecDeque<_>>();
        if events.is_empty() {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let id = state.next_batch_id;
        state.next_batch_id = state
            .next_batch_id
            .checked_add(1)
            .expect("runtime filter event batch identity exhausted");
        let completion = Arc::new(EventBatchCompletion::default());
        state.batches.push_back(EventBatch {
            id,
            ready: false,
            events,
            completion: completion.clone(),
        });
        drop(state);
        self.drain();
        Some(EventBatchHandle { id, completion })
    }

    fn reserve_unready(
        &self,
        events: impl IntoIterator<Item = RuntimeFilterEvent>,
    ) -> Option<EventBatchHandle> {
        let events = events.into_iter().collect::<VecDeque<_>>();
        if events.is_empty() {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let id = state.next_batch_id;
        state.next_batch_id = state
            .next_batch_id
            .checked_add(1)
            .expect("runtime filter event batch identity exhausted");
        let completion = Arc::new(EventBatchCompletion::default());
        state.batches.push_back(EventBatch {
            id,
            ready: false,
            events,
            completion: completion.clone(),
        });
        Some(EventBatchHandle { id, completion })
    }

    fn publish(&self, batch: Option<EventBatchHandle>) {
        let Some(batch) = batch else {
            return;
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let queued = state
            .batches
            .iter_mut()
            .find(|queued| queued.id == batch.id)
            .expect("prequeued runtime filter event batch must remain pending");
        queued.ready = true;
        let reentrant = state.draining_thread == Some(std::thread::current().id());
        drop(state);
        #[cfg(test)]
        if let Some(hook) = self
            .after_publish_ready
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            hook();
        }
        self.drain();
        if !reentrant {
            batch.completion.wait();
        }
    }

    fn abort(&self, batch: Option<EventBatchHandle>) {
        let Some(batch) = batch else {
            return;
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let removed = state
            .batches
            .iter()
            .position(|queued| queued.id == batch.id)
            .and_then(|index| state.batches.remove(index));
        drop(state);
        if let Some(removed) = removed {
            debug_assert!(!removed.ready, "published event batches cannot be aborted");
            removed.completion.complete();
        }
        self.drain();
    }

    fn record_all(&self, events: impl IntoIterator<Item = RuntimeFilterEvent>) {
        let events = events.into_iter().collect::<VecDeque<_>>();
        if events.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let id = state.next_batch_id;
        state.next_batch_id = state
            .next_batch_id
            .checked_add(1)
            .expect("runtime filter event batch identity exhausted");
        let completion = Arc::new(EventBatchCompletion::default());
        state.batches.push_back(EventBatch {
            id,
            ready: true,
            events,
            completion: completion.clone(),
        });
        drop(state);
        self.drain();
    }

    fn drain(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.draining {
            return;
        }
        state.draining = true;
        state.draining_thread = Some(std::thread::current().id());
        loop {
            let next = match state.batches.front_mut() {
                Some(batch) if batch.ready => {
                    let event = batch.events.pop_front();
                    let completion = batch.events.is_empty().then(|| batch.completion.clone());
                    event.map(|event| (event, completion))
                }
                Some(_) | None => None,
            };
            let Some((event, completion)) = next else {
                state.draining = false;
                state.draining_thread = None;
                return;
            };
            if state
                .batches
                .front()
                .is_some_and(|batch| batch.events.is_empty())
            {
                state.batches.pop_front();
            }
            drop(state);
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #[cfg(test)]
                self.recorded
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push(event.clone());
                self.sink.record(event);
            }));
            if let Some(completion) = completion {
                completion.complete();
            }
            state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        }
    }

    #[cfg(test)]
    fn recorded_for_test(&self) -> Vec<RuntimeFilterEvent> {
        self.recorded
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

impl RuntimeFilterEventSink for EventEmitter {
    fn record(&self, event: RuntimeFilterEvent) {
        self.record_all([event]);
    }
}

struct ActionDispatcher {
    query_id: UniqueId,
    registry: Arc<DeploymentRegistry>,
    events: Arc<EventEmitter>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    reliable_transport: Arc<ReliableEnvelopeTransport>,
    channels: Mutex<BTreeMap<ChannelId, Arc<ChannelDispatchFlight>>>,
    #[cfg(test)]
    after_claim: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_materialization_admission: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_materialization_gate_claim: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_encode: Mutex<
        Option<Arc<dyn Fn(crate::runtime_filter::port::artifact::ConsumerProfileId) + Send + Sync>>,
    >,
    #[cfg(test)]
    after_encode: Mutex<
        Option<Arc<dyn Fn(crate::runtime_filter::port::artifact::ConsumerProfileId) + Send + Sync>>,
    >,
    #[cfg(test)]
    before_owner_finish: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_owner_finish: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

#[derive(Default)]
struct ChannelDispatchState {
    next_order: u64,
    draining: bool,
    // Blocks a later cancel core from entering the event FIFO until this action's
    // owner launch prefix has either been reserved or proven unnecessary.
    launch_prefix_pending: bool,
    active_completion: Option<Arc<EventBatchCompletion>>,
    pending: BTreeMap<u64, PendingDispatch>,
    completed_errors: BTreeMap<u64, RuntimeContractViolation>,
    publishing_completions: BTreeMap<u64, Arc<EventBatchCompletion>>,
    reserved_core: BTreeMap<u64, EventBatchHandle>,
}

struct PendingDispatch {
    action: ChannelAction,
    core_batch: Option<EventBatchHandle>,
    completion: Arc<EventBatchCompletion>,
    needs_drainer: bool,
}

#[derive(Default)]
struct ChannelDispatchFlight {
    state: Mutex<ChannelDispatchState>,
    changed: Condvar,
}

struct ClaimedActionMaterialization {
    installed: Arc<InstalledDeployment>,
    snapshot: Arc<crate::runtime_filter::port::value_domain::LogicalSnapshot>,
    jobs: Vec<ClaimedMaterializationJob>,
    launch_batch: Option<EventBatchHandle>,
}

fn action_needs_materialization_launch_prefix(action: &ChannelAction) -> bool {
    matches!(
        action,
        ChannelAction::VisibleSnapshot { .. } | ChannelAction::Completed { .. }
    )
}

impl ActionDispatcher {
    fn finish_publishing_action(
        flight: Arc<ChannelDispatchFlight>,
        order: u64,
        action_completion: Arc<EventBatchCompletion>,
    ) {
        action_completion.complete();
        let mut state = flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.publishing_completions.remove(&order);
        drop(state);
        flight.changed.notify_all();
    }

    #[cfg(test)]
    fn reserve_core_before_hook(&self, channel_id: ChannelId, action: &ChannelAction) {
        let Some(order) = action.dispatch_order() else {
            return;
        };
        let flight = self
            .channels
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(channel_id)
            .or_default()
            .clone();
        let mut state = flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if order == state.next_order && !state.reserved_core.contains_key(&order) {
            if let Some(batch) = self.events.reserve_unready(action.events().iter().cloned()) {
                state.reserved_core.insert(order, batch);
                state.launch_prefix_pending = action_needs_materialization_launch_prefix(action);
            }
        }
    }

    #[cfg(test)]
    fn pending_action_count(&self, channel_id: ChannelId) -> usize {
        self.channels
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&channel_id)
            .map(|flight| {
                flight
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .pending
                    .len()
            })
            .unwrap_or(0)
    }

    fn dispatch(
        &self,
        channel_id: ChannelId,
        action: ChannelAction,
    ) -> Result<(), RuntimeContractViolation> {
        self.dispatch_internal(channel_id, action, true).0
    }

    fn dispatch_nonblocking(
        &self,
        channel_id: ChannelId,
        action: ChannelAction,
    ) -> Arc<EventBatchCompletion> {
        self.dispatch_internal(channel_id, action, false).1
    }

    fn dispatch_internal(
        &self,
        channel_id: ChannelId,
        action: ChannelAction,
        wait: bool,
    ) -> (
        Result<(), RuntimeContractViolation>,
        Arc<EventBatchCompletion>,
    ) {
        let Some(order) = action.dispatch_order() else {
            return (Ok(()), EventBatchCompletion::completed());
        };
        let flight = self
            .channels
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(channel_id)
            .or_default()
            .clone();
        let mut incoming = Some(action);
        loop {
            let mut state = flight
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if order < state.next_order {
                let completion = state
                    .publishing_completions
                    .get(&order)
                    .cloned()
                    .unwrap_or_else(EventBatchCompletion::completed);
                drop(state);
                if wait && !self.events.is_draining_on_current_thread() {
                    completion.wait();
                }
                let result = flight
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .completed_errors
                    .get(&order)
                    .cloned()
                    .map_or(Ok(()), Err);
                return (result, completion);
            }
            if state.draining && order == state.next_order {
                let completion = state
                    .active_completion
                    .clone()
                    .expect("draining action owns an event completion");
                if !wait {
                    return (Ok(()), completion);
                }
                state = flight
                    .changed
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
                drop(state);
                continue;
            }
            let mut caller_completion = None;
            if let Some(action) = incoming.take() {
                let predecessor_reserved =
                    state.draining || state.reserved_core.contains_key(&state.next_order);
                let reserve_contiguous_cancel = predecessor_reserved
                    && !state.launch_prefix_pending
                    && order == state.next_order.saturating_add(1)
                    && matches!(&action, ChannelAction::Cancelled { .. });
                let previously_reserved = state.reserved_core.remove(&order);
                match state.pending.entry(order) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let core_batch = previously_reserved.or_else(|| {
                            reserve_contiguous_cancel
                                .then(|| {
                                    self.events.reserve_unready(action.events().iter().cloned())
                                })
                                .flatten()
                        });
                        let completion = Arc::new(EventBatchCompletion::default());
                        caller_completion = Some(completion.clone());
                        entry.insert(PendingDispatch {
                            action,
                            core_batch,
                            completion,
                            needs_drainer: !wait,
                        });
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if wait {
                            entry.get_mut().needs_drainer = false;
                        }
                        caller_completion = Some(entry.get().completion.clone());
                    }
                }
            }
            if state.draining || order != state.next_order {
                if !wait {
                    return (
                        Ok(()),
                        caller_completion.expect("queued action owns an event completion"),
                    );
                }
                state = flight
                    .changed
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
                drop(state);
                continue;
            }
            let next_order = state.next_order;
            let PendingDispatch {
                action,
                core_batch: reserved_core_batch,
                completion: action_completion,
                ..
            } = state
                .pending
                .remove(&next_order)
                .expect("next ordered runtime filter action is pending");
            let core_batch = reserved_core_batch
                .or_else(|| self.events.reserve_unready(action.events().iter().cloned()));
            state.draining = true;
            state.launch_prefix_pending = action_needs_materialization_launch_prefix(&action);
            state.active_completion = Some(action_completion.clone());
            drop(state);
            let caller_completion = caller_completion.unwrap_or_else(|| action_completion.clone());
            let result = self.process_claimed_action(
                channel_id,
                flight.clone(),
                action,
                core_batch,
                action_completion,
            );
            self.drain_ready_nonblocking(channel_id, flight);
            return (result, caller_completion);
        }
    }

    fn drain_ready_nonblocking(&self, channel_id: ChannelId, flight: Arc<ChannelDispatchFlight>) {
        loop {
            let mut state = flight
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.draining {
                return;
            }
            let next_order = state.next_order;
            if !state
                .pending
                .get(&next_order)
                .is_some_and(|pending| pending.needs_drainer)
            {
                return;
            }
            let PendingDispatch {
                action,
                core_batch,
                completion,
                ..
            } = state
                .pending
                .remove(&next_order)
                .expect("ready nonblocking dispatch remains pending");
            let core_batch =
                core_batch.or_else(|| self.events.reserve_unready(action.events().iter().cloned()));
            state.draining = true;
            state.launch_prefix_pending = action_needs_materialization_launch_prefix(&action);
            state.active_completion = Some(completion.clone());
            drop(state);
            let _ = self.process_claimed_action(
                channel_id,
                flight.clone(),
                action,
                core_batch,
                completion,
            );
        }
    }

    fn process_claimed_action(
        &self,
        channel_id: ChannelId,
        flight: Arc<ChannelDispatchFlight>,
        action: ChannelAction,
        core_batch: Option<EventBatchHandle>,
        action_completion: Arc<EventBatchCompletion>,
    ) -> Result<(), RuntimeContractViolation> {
        #[cfg(test)]
        if let Some(hook) = self
            .after_claim
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            hook();
        }
        let mut claimed = self.claim_action_materialization(channel_id, &action);
        let launch_batch = claimed
            .as_mut()
            .and_then(|claimed| claimed.launch_batch.take());
        let core_completion = core_batch.as_ref().map(|batch| batch.completion.clone());

        let mut state = flight
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.launch_prefix_pending = false;
        state.next_order = state
            .next_order
            .checked_add(1)
            .expect("runtime filter dispatch order exhausted");
        let completed_order = state.next_order - 1;
        state.active_completion = None;
        state.draining = false;
        state
            .publishing_completions
            .insert(completed_order, action_completion.clone());
        drop(state);
        flight.changed.notify_all();

        self.events.publish(core_batch);
        self.events.publish(launch_batch);
        #[cfg(test)]
        let before_materialization_admission = claimed.is_some().then(|| {
            self.before_materialization_admission
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
        });
        #[cfg(test)]
        if let Some(Some(hook)) = before_materialization_admission {
            hook();
        }
        let (artifact_batch, error) = self.route_and_prequeue(channel_id, &action, claimed);
        let result = error.map_or(Ok(()), Err);
        if let Err(error) = &result {
            flight
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .completed_errors
                .insert(completed_order, error.clone());
        }

        let final_event_completion = artifact_batch
            .as_ref()
            .map(|batch| batch.completion.clone())
            .or(core_completion);
        if let Some(event_completion) = final_event_completion {
            let flight = flight.clone();
            let action_completion = action_completion.clone();
            event_completion.on_complete(move || {
                Self::finish_publishing_action(flight, completed_order, action_completion);
            });
        } else {
            Self::finish_publishing_action(flight, completed_order, action_completion);
        }
        self.events.publish(artifact_batch);
        result
    }

    fn claim_action_materialization(
        &self,
        channel_id: ChannelId,
        action: &ChannelAction,
    ) -> Option<ClaimedActionMaterialization> {
        let snapshot = match action {
            ChannelAction::VisibleSnapshot { snapshot, .. }
            | ChannelAction::Completed { snapshot, .. } => snapshot.clone(),
            _ => return None,
        };
        let installed = self.registry.installation_for_dispatch()?;
        let plan = installed.artifact_plan(channel_id)?;
        let mut jobs =
            claim_materialization_jobs(plan, installed.publish_gate(), snapshot.version());
        #[cfg(test)]
        let after_materialization_gate_claim = jobs
            .iter()
            .any(|job| matches!(job, ClaimedMaterializationJob::Owner { .. }))
            .then(|| {
                self.after_materialization_gate_claim
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take()
            });
        #[cfg(test)]
        if let Some(Some(hook)) = after_materialization_gate_claim {
            hook();
        }
        let launch_batch = self
            .events
            .reserve_unready(take_materialization_launch_events(&mut jobs));
        Some(ClaimedActionMaterialization {
            installed,
            snapshot,
            jobs,
            launch_batch,
        })
    }

    fn route_and_prequeue(
        &self,
        channel_id: ChannelId,
        action: &ChannelAction,
        claimed: Option<ClaimedActionMaterialization>,
    ) -> (Option<EventBatchHandle>, Option<RuntimeContractViolation>) {
        let installed = claimed
            .as_ref()
            .map(|claimed| claimed.installed.clone())
            .or_else(|| self.registry.installation_for_dispatch());
        let Some(installed) = installed else {
            return (None, None);
        };
        let mut events = Vec::new();
        let mut deliveries = Vec::new();
        let mut remote_deliveries = Vec::new();
        let mut remote_terminals = Vec::new();
        let mut error = None;
        match action {
            ChannelAction::None | ChannelAction::Progress { .. } => {}
            ChannelAction::CompletedWithoutArtifact { .. } => {
                if let Some(plan) = installed.artifact_plan(channel_id) {
                    for group in plan.groups() {
                        deliveries.push((
                            group.route_edges().to_vec(),
                            None,
                            Some(LiveTerminal::CompletedWithoutArtifact),
                        ));
                        remote_terminals.push((
                            group.profile().clone(),
                            group.route_edges().to_vec(),
                            LiveTerminal::CompletedWithoutArtifact,
                        ));
                    }
                }
            }
            ChannelAction::DegradedLogical { reason, .. } => {
                if let Some(plan) = installed.artifact_plan(channel_id) {
                    for group in plan.groups() {
                        deliveries.push((
                            group.route_edges().to_vec(),
                            None,
                            Some(LiveTerminal::DegradedLogical(*reason)),
                        ));
                        remote_terminals.push((
                            group.profile().clone(),
                            group.route_edges().to_vec(),
                            LiveTerminal::DegradedLogical(*reason),
                        ));
                    }
                }
            }
            ChannelAction::VisibleSnapshot { snapshot, .. }
            | ChannelAction::Completed { snapshot, .. } => {
                let terminal = matches!(action, ChannelAction::Completed { .. })
                    .then_some(LiveTerminal::Completed);
                let Some(claimed) = claimed else {
                    return (None, None);
                };
                debug_assert!(Arc::ptr_eq(snapshot, &claimed.snapshot));
                let Some(plan) = installed.artifact_plan(channel_id) else {
                    return (None, None);
                };
                for work in execute_materialization_jobs(
                    plan,
                    snapshot,
                    self.memory_account.clone(),
                    {
                        #[cfg(test)]
                        {
                            self.before_encode
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .clone()
                        }
                        #[cfg(not(test))]
                        {
                            None
                        }
                    },
                    {
                        #[cfg(test)]
                        {
                            self.after_encode
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .clone()
                        }
                        #[cfg(not(test))]
                        {
                            None
                        }
                    },
                    claimed.jobs,
                ) {
                    let identity =
                        crate::runtime_filter::port::events::ArtifactMaterializationIdentity::new(
                            work.group.common(),
                            work.group.profile().id(),
                            snapshot.version(),
                        );
                    events.extend(work.events);
                    if let Some(violation) = work.contract_violation {
                        error.get_or_insert(violation);
                        continue;
                    }
                    let Some(outcome) = work.outcome else {
                        events.push(RuntimeFilterEvent::ArtifactPublishStaleSkipped { identity });
                        continue;
                    };
                    let mut delivered_before_notify = false;
                    let decision = match work.claim {
                        MaterializationWorkClaim::Owner(owner) => {
                            #[cfg(test)]
                            if let Some(hook) = self
                                .before_owner_finish
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .take()
                            {
                                hook();
                            }
                            let route_edges = work.group.route_edges().to_vec();
                            match owner.finish_after_delivery(
                                outcome.clone(),
                                |decision, committed| {
                                    if decision == PublishCommitOutcome::Published {
                                        installed.router().route_live(
                                            &route_edges,
                                            Some(committed),
                                            terminal,
                                        );
                                        delivered_before_notify = true;
                                    }
                                },
                            ) {
                                Ok(decision) => {
                                    #[cfg(test)]
                                    if let Some(hook) = self
                                        .after_owner_finish
                                        .lock()
                                        .unwrap_or_else(|error| error.into_inner())
                                        .take()
                                    {
                                        hook();
                                    }
                                    decision
                                }
                                Err(conflict) => {
                                    error.get_or_insert(conflict);
                                    continue;
                                }
                            }
                        }
                        MaterializationWorkClaim::Follower => PublishCommitOutcome::Idempotent,
                        MaterializationWorkClaim::Stale => PublishCommitOutcome::Stale,
                    };
                    let delivery_outcome = match decision {
                        PublishCommitOutcome::Published => {
                            if let ArtifactDeliveryOutcome::Published(bundle) = &outcome {
                                let (kind, _) = bundle
                                    .artifacts()
                                    .first()
                                    .expect("published artifact bundle is non-empty");
                                events.push(RuntimeFilterEvent::ArtifactPublished {
                                    identity,
                                    kind: *kind,
                                    bytes: bundle.encoded_bytes(),
                                    digest: bundle.canonical_digest(),
                                });
                            }
                            Some(outcome.clone())
                        }
                        PublishCommitOutcome::Stale => {
                            events
                                .push(RuntimeFilterEvent::ArtifactPublishStaleSkipped { identity });
                            continue;
                        }
                        PublishCommitOutcome::Cancelled => continue,
                        PublishCommitOutcome::Idempotent if terminal.is_none() => continue,
                        PublishCommitOutcome::Idempotent => None,
                    };
                    if delivery_outcome.is_some() {
                        for route_edge_id in work.group.route_edges() {
                            if installed.router().contains_route(*route_edge_id) {
                                events.extend(
                                    installed
                                        .route_event_identities(*route_edge_id)
                                        .iter()
                                        .copied()
                                        .map(|identity| RuntimeFilterEvent::LoopbackDelivered {
                                            identity,
                                            version: snapshot.version(),
                                        }),
                                );
                            }
                        }
                    }
                    let remote_outcome = if terminal == Some(LiveTerminal::Completed)
                        && matches!(outcome, ArtifactDeliveryOutcome::Published(_))
                    {
                        // A completion must always cross the wire as one atomic final
                        // artifact, including when the same version was published by an
                        // earlier VisibleSnapshot and this materialization is idempotent.
                        Some(&outcome)
                    } else {
                        delivery_outcome.as_ref()
                    };
                    if let Some(outcome) = remote_outcome {
                        let envelope_kind = match outcome {
                            ArtifactDeliveryOutcome::Published(_) => {
                                Some(if terminal == Some(LiveTerminal::Completed) {
                                    RuntimeFilterEnvelopeKind::FinalArtifact
                                } else {
                                    RuntimeFilterEnvelopeKind::Artifact
                                })
                            }
                            ArtifactDeliveryOutcome::Unavailable(_) => {
                                Some(RuntimeFilterEnvelopeKind::Unavailable)
                            }
                            ArtifactDeliveryOutcome::Unsupported(_)
                            | ArtifactDeliveryOutcome::Cancelled => None,
                        };
                        if let Some(envelope_kind) = envelope_kind {
                            remote_deliveries.push((
                                work.group.profile().clone(),
                                work.group.route_edges().to_vec(),
                                outcome.clone(),
                                envelope_kind,
                            ));
                        }
                    }
                    if !delivered_before_notify {
                        deliveries.push((
                            work.group.route_edges().to_vec(),
                            delivery_outcome,
                            terminal,
                        ));
                    }
                }
            }
            ChannelAction::Unavailable { reason, .. } => {
                if let Some(plan) = installed.artifact_plan(channel_id) {
                    for group in plan.groups() {
                        let outcome = ArtifactDeliveryOutcome::Unavailable(*reason);
                        deliveries.push((
                            group.route_edges().to_vec(),
                            Some(outcome.clone()),
                            Some(LiveTerminal::Unavailable(*reason)),
                        ));
                        remote_deliveries.push((
                            group.profile().clone(),
                            group.route_edges().to_vec(),
                            outcome,
                            RuntimeFilterEnvelopeKind::Unavailable,
                        ));
                    }
                }
            }
            ChannelAction::Cancelled { .. } => {
                // Gate cancellation owns direct cancellation delivery for pending keys.
            }
        }
        let batch = self.events.prequeue(events);
        for (route_edges, outcome, terminal) in deliveries {
            installed
                .router()
                .route_live(&route_edges, outcome.as_ref(), terminal);
        }
        for (profile, route_edges, outcome, envelope_kind) in remote_deliveries {
            if let Err(delivery_error) = self.deliver_remote_artifact(
                &installed,
                channel_id,
                &profile,
                route_edges,
                &outcome,
                envelope_kind,
            ) {
                error.get_or_insert_with(|| {
                    violation(
                        RuntimeContractViolationKind::ServiceUnavailable,
                        delivery_error.to_string(),
                    )
                });
            }
        }
        for (profile, route_edges, terminal) in remote_terminals {
            if let Err(delivery_error) = self.deliver_remote_terminal(
                &installed,
                channel_id,
                &profile,
                route_edges,
                terminal,
            ) {
                error.get_or_insert_with(|| {
                    violation(
                        RuntimeContractViolationKind::ServiceUnavailable,
                        delivery_error.to_string(),
                    )
                });
            }
        }
        (batch, error)
    }

    fn deliver_remote_terminal(
        &self,
        installed: &InstalledDeployment,
        channel_id: ChannelId,
        profile: &ConsumerArtifactProfile,
        route_edge_ids: Vec<RouteEdgeId>,
        terminal: LiveTerminal,
    ) -> Result<(), ArtifactDeliveryError> {
        let envelope_kind = match terminal {
            LiveTerminal::CompletedWithoutArtifact => {
                RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
            }
            LiveTerminal::DegradedLogical(_) => RuntimeFilterEnvelopeKind::DegradedLogical,
            LiveTerminal::Completed | LiveTerminal::Unavailable(_) | LiveTerminal::Cancelled => {
                return Err(ArtifactDeliveryError::UndeliverableOutcome);
            }
            LiveTerminal::DegradedArtifact(_) | LiveTerminal::DegradedDelivery(_) => {
                return Err(ArtifactDeliveryError::UndeliverableOutcome);
            }
        };
        let intent = RuntimeFilterDeliveryRouteIntent::new(
            installed.epoch(),
            channel_id,
            route_edge_ids,
            envelope_kind,
        )?;
        let decision = installed.role_router().route_delivery(intent)?;
        if decision.remote_routes().is_empty() {
            return Ok(());
        }
        let expectation = ArtifactDecodeExpectation::new(profile);
        let frame = Arc::new(match terminal {
            LiveTerminal::CompletedWithoutArtifact => {
                encode_completed_without_artifact(expectation)
            }
            LiveTerminal::DegradedLogical(reason) => {
                let max_encoded = max_encoded_len_for_artifact_budget(
                    installed
                        .artifact_plan(channel_id)
                        .ok_or(ArtifactDeliveryError::NotInstalled)?
                        .max_artifact_bytes(),
                )?;
                encode_unavailable(reason, expectation, max_encoded)?
            }
            LiveTerminal::Completed | LiveTerminal::Unavailable(_) | LiveTerminal::Cancelled => {
                unreachable!("non-deliverable terminals are rejected above")
            }
            LiveTerminal::DegradedArtifact(_) | LiveTerminal::DegradedDelivery(_) => {
                unreachable!("non-deliverable terminals are rejected above")
            }
        });
        let common = RuntimeFilterEventIdentity::new(
            self.query_id,
            installed.participant_id(),
            channel_id,
            installed.epoch(),
        );
        for route in decision.remote_routes() {
            let identity = TransportRouteEventIdentity::new(common, route.route_edge_id());
            if let ReliableSendOutcome::ResourceLimit(_limit) = self.reliable_transport.send_kind(
                route,
                Arc::clone(&frame),
                identity,
                envelope_kind,
            ) {
                self.events.record(RuntimeFilterEvent::TransportEnvelope {
                    identity,
                    kind: TransportEventKind::FailedOpen(TransportFailOpenReason::ResourceLimit),
                    bytes: frame.payload().len(),
                });
            }
        }
        Ok(())
    }

    fn deliver_remote_artifact(
        &self,
        installed: &InstalledDeployment,
        channel_id: ChannelId,
        profile: &ConsumerArtifactProfile,
        route_edge_ids: Vec<RouteEdgeId>,
        outcome: &ArtifactDeliveryOutcome,
        envelope_kind: RuntimeFilterEnvelopeKind,
    ) -> Result<(), ArtifactDeliveryError> {
        match (outcome, envelope_kind) {
            (
                ArtifactDeliveryOutcome::Published(_),
                RuntimeFilterEnvelopeKind::Artifact | RuntimeFilterEnvelopeKind::FinalArtifact,
            )
            | (ArtifactDeliveryOutcome::Unavailable(_), RuntimeFilterEnvelopeKind::Unavailable) => {
            }
            _ => return Err(ArtifactDeliveryError::UndeliverableOutcome),
        }
        let intent = RuntimeFilterDeliveryRouteIntent::new(
            installed.epoch(),
            channel_id,
            route_edge_ids,
            envelope_kind,
        )?;
        let decision = installed.role_router().route_delivery(intent)?;
        if decision.remote_routes().is_empty() {
            return Ok(());
        }
        let max_encoded = max_encoded_len_for_artifact_budget(
            installed
                .artifact_plan(channel_id)
                .ok_or(ArtifactDeliveryError::NotInstalled)?
                .max_artifact_bytes(),
        )?;
        let expectation = ArtifactDecodeExpectation::new(profile);
        let frame = Arc::new(match outcome {
            ArtifactDeliveryOutcome::Published(bundle) => {
                encode_artifact_bundle(bundle, expectation, max_encoded)?
            }
            ArtifactDeliveryOutcome::Unavailable(reason) => {
                encode_unavailable(*reason, expectation, max_encoded)?
            }
            ArtifactDeliveryOutcome::Unsupported(_) | ArtifactDeliveryOutcome::Cancelled => {
                unreachable!("non-deliverable outcomes are rejected above")
            }
        });
        let common = RuntimeFilterEventIdentity::new(
            self.query_id,
            installed.participant_id(),
            channel_id,
            installed.epoch(),
        );
        for route in decision.remote_routes() {
            let identity = TransportRouteEventIdentity::new(common, route.route_edge_id());
            if let ReliableSendOutcome::ResourceLimit(_limit) = self.reliable_transport.send_kind(
                route,
                Arc::clone(&frame),
                identity,
                envelope_kind,
            ) {
                self.events.record(RuntimeFilterEvent::TransportEnvelope {
                    identity,
                    kind: TransportEventKind::FailedOpen(TransportFailOpenReason::ResourceLimit),
                    bytes: frame.payload().len(),
                });
            }
        }
        Ok(())
    }
}

/// Failure modes of the outbound artifact-delivery bridge
/// ([`RuntimeFilterService::deliver_artifact`]).
///
/// The bridge fails fast: it never silently drops a route or best-effort skips an
/// edge. `Route` surfaces a Router authorization/topology rejection verbatim and
/// `Encode` surfaces a wire-codec rejection verbatim, so callers see exactly why a
/// delivery scope could not be honored.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactDeliveryError {
    /// No active installation owns the requested channel, or the channel carries
    /// no materialization plan on this participant.
    NotInstalled,
    /// The outcome is neither a published bundle nor an `Unavailable` sentinel, so
    /// it has no delivery envelope kind.
    UndeliverableOutcome,
    /// The delivery Router rejected the requested scope (unknown/forbidden edge,
    /// stale epoch, unknown channel).
    Route(RuntimeFilterRouteContractError),
    /// The wire codec rejected a remote-leg frame (over budget, non-canonical,
    /// profile mismatch).
    Encode(ArtifactWireCodecError),
}

impl std::fmt::Display for ArtifactDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInstalled => {
                write!(
                    formatter,
                    "runtime filter artifact delivery has no installed channel plan"
                )
            }
            Self::UndeliverableOutcome => {
                write!(
                    formatter,
                    "runtime filter artifact delivery outcome has no envelope kind"
                )
            }
            Self::Route(error) => {
                write!(formatter, "runtime filter artifact delivery route: {error}")
            }
            Self::Encode(error) => write!(
                formatter,
                "runtime filter artifact delivery encode: {error}"
            ),
        }
    }
}

impl std::error::Error for ArtifactDeliveryError {}

impl From<RuntimeFilterRouteContractError> for ArtifactDeliveryError {
    fn from(error: RuntimeFilterRouteContractError) -> Self {
        Self::Route(error)
    }
}

impl From<ArtifactWireCodecError> for ArtifactDeliveryError {
    fn from(error: ArtifactWireCodecError) -> Self {
        Self::Encode(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LifecyclePhase {
    Running,
    Closing,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CloseRole {
    Leader,
    Follower,
    Deferred,
    Closed,
}

struct LifecycleState {
    phase: LifecyclePhase,
    in_flight: usize,
    owners: HashMap<ThreadId, usize>,
    finalizer: Option<ThreadId>,
}

/// Reentrant-safe lifecycle barrier shared by Service and its reliable transport.
///
/// A permit covers one externally observable operation. Closing rejects new permits
/// and a non-reentrant closer waits on the condition variable until every admitted
/// operation has left. A closer invoked synchronously from its own callback is marked
/// deferred; the outer operation's guard claims finalization after releasing its last
/// permit, avoiding self-wait deadlocks without weakening the external close barrier.
pub(super) struct LifecycleBarrier {
    state: Mutex<LifecycleState>,
    changed: Condvar,
}

impl LifecycleBarrier {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(LifecycleState {
                phase: LifecyclePhase::Running,
                in_flight: 0,
                owners: HashMap::new(),
                finalizer: None,
            }),
            changed: Condvar::new(),
        }
    }

    pub(super) fn try_admit(&self) -> Option<LifecyclePermit<'_>> {
        let current = std::thread::current().id();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.phase != LifecyclePhase::Running {
            return None;
        }
        state.in_flight += 1;
        *state.owners.entry(current).or_insert(0) += 1;
        Some(LifecyclePermit { barrier: self })
    }

    pub(super) fn is_running(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .phase
            == LifecyclePhase::Running
    }

    pub(super) fn is_closing(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .phase
            == LifecyclePhase::Closing
    }

    pub(super) fn request_close(&self) -> CloseRole {
        let current = std::thread::current().id();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.phase == LifecyclePhase::Closed {
            return CloseRole::Closed;
        }
        state.phase = LifecyclePhase::Closing;
        if state.owners.get(&current).copied().unwrap_or(0) != 0
            || state.finalizer.as_ref() == Some(&current)
        {
            return CloseRole::Deferred;
        }
        if state.finalizer.is_none() {
            state.finalizer = Some(current);
            CloseRole::Leader
        } else {
            CloseRole::Follower
        }
    }

    pub(super) fn wait_for_quiescence(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while state.in_flight != 0 {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    pub(super) fn wait_until_closed(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while state.phase != LifecyclePhase::Closed {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    pub(super) fn mark_closed(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.phase = LifecyclePhase::Closed;
        state.finalizer = None;
        drop(state);
        self.changed.notify_all();
    }

    fn release(&self) {
        let current = std::thread::current().id();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let remove_owner = {
            let count = state
                .owners
                .get_mut(&current)
                .expect("lifecycle permit owner is registered");
            *count -= 1;
            *count == 0
        };
        if remove_owner {
            state.owners.remove(&current);
        }
        state.in_flight -= 1;
        drop(state);
        self.changed.notify_all();
    }
}

pub(super) struct LifecyclePermit<'a> {
    barrier: &'a LifecycleBarrier,
}

/// Guarantees that a lifecycle leader publishes `Closed` even when teardown code
/// panics. External callbacks are still allowed to propagate their first panic, but
/// duplicate closers must never remain parked behind a permanently `Closing` owner.
pub(super) struct FinalizerCompletion<'a> {
    barrier: &'a LifecycleBarrier,
}

impl<'a> FinalizerCompletion<'a> {
    pub(super) fn new(barrier: &'a LifecycleBarrier) -> Self {
        Self { barrier }
    }
}

impl Drop for FinalizerCompletion<'_> {
    fn drop(&mut self) {
        self.barrier.mark_closed();
    }
}

pub(super) type FinalizerPanic = Box<dyn std::any::Any + Send + 'static>;

/// Keep cleanup progressing after the first teardown panic. Discarding a later
/// payload normally would run its arbitrary `Drop` implementation inside finalizer
/// code, so leak it deliberately on this already-failing path instead.
pub(super) fn retain_first_finalizer_panic(
    first: &mut Option<FinalizerPanic>,
    payload: FinalizerPanic,
) {
    if first.is_none() {
        *first = Some(payload);
    } else {
        std::mem::forget(payload);
    }
}

/// Resume a teardown panic only when this finalizer did not start inside another
/// unwind. A deferred close commonly finalizes from a call guard's `Drop`; resuming a
/// secondary teardown panic there would abort the process and erase the outer panic.
pub(super) fn finish_finalizer_panic(first: Option<FinalizerPanic>, entered_while_panicking: bool) {
    let Some(payload) = first else {
        return;
    };
    if entered_while_panicking {
        // Panic payloads are arbitrary user values and their destructor may panic.
        // Leaking one payload while the thread is already unwinding is preferable to
        // a double-panic process abort.
        std::mem::forget(payload);
    } else {
        std::panic::resume_unwind(payload);
    }
}

impl Drop for LifecyclePermit<'_> {
    fn drop(&mut self) {
        self.barrier.release();
    }
}

pub(crate) struct RuntimeFilterService {
    _query_id: UniqueId,
    _clock: Arc<dyn RuntimeFilterClock>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    registry: Arc<DeploymentRegistry>,
    dispatcher: Arc<ActionDispatcher>,
    producer_handles: Mutex<BTreeMap<(BindingId, UniqueId), ProducerHandleWeak>>,
    remote_producer_states: Mutex<BTreeMap<(BindingId, UniqueId), Arc<RemoteProducerState>>>,
    final_domain_completion_sessions: FinalDomainCompletionSessionRegistry,
    #[cfg(test)]
    producer_test_handles: Mutex<BTreeMap<(BindingId, UniqueId), Weak<ServiceProducerAdapter>>>,
    // Deterministic inbound-lifecycle seams. Each is a one-shot hook taken and run
    // at a fixed point so a test can pin an admission/submit/cancel interleaving.
    // They are compiled and read only under `cfg(test)`; the production path never
    // observes them. Lock context matters: `after_inbound_open_admission` fires while the
    // `operation` lock is still held, so its installed closure MUST be non-blocking — a
    // blocking closure would deadlock a concurrent cancel/install that needs `operation`.
    // `before_inbound_typed_dispatch` and the cancel seam fire after the lock is released.
    #[cfg(test)]
    after_inbound_open_admission: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_inbound_typed_dispatch: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_registry_cancel_before_channel_cancel: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_deadline_dispatch: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_close_request_before_quiescence: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_resource_limit_event_admission: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    operation: Mutex<()>,
    lifecycle: LifecycleBarrier,
    // Unified, per-channel ingress dedupe + `(query, epoch)` tombstone (M3 Task 3).
    // Both ingress directions consult it; teardown (`cancel`) retires this query/epoch
    // into its tombstone. It subsumes the former `delivered_versions` ledger (the
    // consumer logical `(route_edge, version)` idempotency, M2C spec §7.7) and adds the
    // transport-identity dedupe keyed on the wire route identity incl. transport
    // sequence. See `dedupe.rs`.
    dedupe: IngressDedupe,
    // Sender-side reliable transport for the outbound remote leg: buffers each
    // wire-encoded remote frame for ack-release and bounded retry, failing open on
    // deadline. Query-scoped so its in-flight buffer persists across delivery calls.
    reliable_transport: Arc<ReliableEnvelopeTransport>,
}

pub(super) struct OpenedProducer {
    pub(super) handle: ProducerHandle,
    pub(super) outcome: SubmitOutcome,
}

struct ServiceCall<'a> {
    service: &'a RuntimeFilterService,
    permit: Option<LifecyclePermit<'a>>,
}

impl<'a> ServiceCall<'a> {
    fn admit(service: &'a RuntimeFilterService) -> Option<Self> {
        Some(Self {
            service,
            permit: Some(service.lifecycle.try_admit()?),
        })
    }
}

impl Drop for ServiceCall<'_> {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.service.finish_close_if_requested();
    }
}

impl RuntimeFilterService {
    fn new_with_dependencies(
        query_id: UniqueId,
        clock: Arc<dyn RuntimeFilterClock>,
        event_sink: Arc<dyn RuntimeFilterEventSink>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Self {
        let events = Arc::new(EventEmitter::new(event_sink));
        let registry = Arc::new(DeploymentRegistry::new(
            query_id,
            clock.clone(),
            memory_account.clone(),
            events.clone(),
        ));
        // The reliable transport emits its structured `TransportEnvelope` events through
        // the SAME `EventEmitter` the registry and dispatcher already use — one query-
        // scoped lifecycle sink, never a second registry.
        let reliable_transport = Arc::new(ReliableEnvelopeTransport::for_query(
            clock.clone(),
            events.clone(),
        ));
        let dispatcher = Arc::new(ActionDispatcher {
            query_id,
            registry: registry.clone(),
            events: events.clone(),
            memory_account: memory_account.clone(),
            reliable_transport: reliable_transport.clone(),
            channels: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            after_claim: Mutex::new(None),
            #[cfg(test)]
            before_materialization_admission: Mutex::new(None),
            #[cfg(test)]
            after_materialization_gate_claim: Mutex::new(None),
            #[cfg(test)]
            before_encode: Mutex::new(None),
            #[cfg(test)]
            after_encode: Mutex::new(None),
            #[cfg(test)]
            before_owner_finish: Mutex::new(None),
            #[cfg(test)]
            after_owner_finish: Mutex::new(None),
        });
        Self {
            _query_id: query_id,
            _clock: clock,
            memory_account,
            registry,
            dispatcher,
            reliable_transport,
            producer_handles: Mutex::new(BTreeMap::new()),
            remote_producer_states: Mutex::new(BTreeMap::new()),
            final_domain_completion_sessions: FinalDomainCompletionSessionRegistry::default(),
            #[cfg(test)]
            producer_test_handles: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            after_inbound_open_admission: Mutex::new(None),
            #[cfg(test)]
            before_inbound_typed_dispatch: Mutex::new(None),
            #[cfg(test)]
            after_registry_cancel_before_channel_cancel: Mutex::new(None),
            #[cfg(test)]
            before_deadline_dispatch: Mutex::new(None),
            #[cfg(test)]
            after_close_request_before_quiescence: Mutex::new(None),
            #[cfg(test)]
            before_resource_limit_event_admission: Mutex::new(None),
            operation: Mutex::new(()),
            lifecycle: LifecycleBarrier::new(),
            dedupe: IngressDedupe::new(query_id),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_lifecycle_test(
        query_id: UniqueId,
        clock: Arc<dyn RuntimeFilterClock>,
        event_sink: Arc<dyn RuntimeFilterEventSink>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Self {
        Self::new_with_dependencies(query_id, clock, event_sink, memory_account)
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_events_for_test(&self) -> Vec<RuntimeFilterEvent> {
        self.dispatcher.events.recorded_for_test()
    }

    pub(crate) fn install(
        &self,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<InstallOutcome, InstallContractError> {
        if !self.lifecycle.is_running() {
            return Err(InstallContractError::new(
                InstallContractErrorKind::ServiceClosed,
                "runtime filter service is terminal",
            ));
        }
        let result = self.registry.install(install)?;
        let outcome = result.outcome();
        Ok(outcome)
    }

    pub(crate) fn configure_transport(
        &self,
        policy: ReliableTransportPolicy,
    ) -> Result<(), String> {
        let Some(_call) = ServiceCall::admit(self) else {
            return Err("runtime filter service is terminal".to_string());
        };
        self.reliable_transport.configure_policy(policy)
    }

    #[cfg(test)]
    pub(crate) fn installed_participant_install_for_test(
        &self,
    ) -> Option<RuntimeFilterParticipantInstall> {
        self.registry
            .active_installation()
            .map(|installed| installed.participant_install_for_test())
    }

    pub(crate) fn open_producer(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
        requested: ProducerPortKind,
    ) -> Result<ProducerHandle, RuntimeContractViolation> {
        let _operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = self
            .registry
            .active_installation()
            .ok_or_else(service_cancelled)?;
        self.open_producer_with_install_locked(
            &installed,
            binding_id,
            fragment_instance_id,
            local_partition_count,
            requested,
        )
    }

    pub(crate) fn open_final_aggregate_producer(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
    ) -> Result<FinalDomainCompletionSession, RuntimeContractViolation> {
        let _operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.final_domain_completion_sessions
            .ensure_vacant(binding_id, fragment_instance_id)?;
        let installed = self
            .registry
            .active_installation()
            .ok_or_else(service_cancelled)?;
        let route = installed.producer(binding_id).ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "producer binding is not installed on this participant",
            )
        })?;
        let contract = route
            .final_domain_seed
            .as_ref()
            .ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "installed producer route has no final-domain completion authority",
                )
            })?
            .contract();
        let producer = self
            .open_producer_with_install_locked(
                &installed,
                binding_id,
                fragment_instance_id,
                local_partition_count,
                ProducerPortKind::FinalDomain,
            )?
            .into_final_domain()?;
        let session = FinalDomainCompletionSession::new(
            contract,
            binding_id,
            fragment_instance_id,
            producer,
            local_partition_count,
        )?;
        self.final_domain_completion_sessions.register(
            binding_id,
            fragment_instance_id,
            session.weak(),
        )?;
        Ok(session)
    }

    fn open_producer_with_install_locked(
        &self,
        installed: &Arc<InstalledDeployment>,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
        requested: ProducerPortKind,
    ) -> Result<ProducerHandle, RuntimeContractViolation> {
        let route = installed.producer(binding_id).ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "producer binding is not installed on this participant",
            )
        })?;
        if installed.producer_participant(route.channel_id(), binding_id, fragment_instance_id)
            != Some(installed.participant_id())
        {
            return Err(violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "producer fragment instance is owned by another participant",
            ));
        }
        if route.kind != requested {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "requested producer port does not match the installed channel contract",
            ));
        }
        route.channel.preflight_remote_open(
            binding_id,
            fragment_instance_id,
            local_partition_count,
            crate::runtime_filter::port::identity::PartitionId::new(0),
        )?;
        let producer_intent = RuntimeFilterProducerRouteIntent::new(
            installed.epoch(),
            route.channel_id(),
            binding_id,
            RuntimeFilterEnvelopeKind::Contribution,
        )
        .map_err(|error| {
            violation(
                RuntimeContractViolationKind::ServiceUnavailable,
                error.to_string(),
            )
        })?;
        match installed.role_router().route_producer(producer_intent) {
            Ok(decision)
                if decision.remote_routes().len() == 1
                    && decision.loopback_route_edge_ids().is_empty() =>
            {
                self.open_remote_producer_locked(
                    installed,
                    route,
                    decision.remote_routes()[0].clone(),
                    binding_id,
                    fragment_instance_id,
                    local_partition_count,
                    requested,
                )
            }
            Ok(decision)
                if decision.remote_routes().is_empty()
                    && decision.loopback_route_edge_ids().len() == 1 =>
            {
                Ok(self
                    .open_inbound_core_locked(
                        installed,
                        binding_id,
                        fragment_instance_id,
                        local_partition_count,
                        requested,
                    )?
                    .handle)
            }
            Err(RuntimeFilterRouteContractError::ForbiddenOutboundKind { .. }) => Ok(self
                .open_inbound_core_locked(
                    installed,
                    binding_id,
                    fragment_instance_id,
                    local_partition_count,
                    requested,
                )?
                .handle),
            Ok(_) | Err(_) => Err(violation(
                RuntimeContractViolationKind::ServiceUnavailable,
                "installed producer route does not resolve to one frozen dispatch target",
            )),
        }
    }

    fn open_remote_producer_locked(
        &self,
        installed: &Arc<InstalledDeployment>,
        route: &registry::ProducerRoute,
        remote_route: crate::runtime_filter::port::routing::RuntimeFilterRemoteRoute,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
        requested: ProducerPortKind,
    ) -> Result<ProducerHandle, RuntimeContractViolation> {
        let key = (binding_id, fragment_instance_id);
        let state = {
            let mut states = self
                .remote_producer_states
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(state) = states.get(&key) {
                state.validate_open(
                    installed.epoch(),
                    &remote_route,
                    requested,
                    local_partition_count,
                )?;
                Arc::clone(state)
            } else {
                let state = Arc::new(RemoteProducerState::new(
                    installed.epoch(),
                    remote_route.clone(),
                    requested,
                    local_partition_count,
                ));
                states.insert(key, Arc::clone(&state));
                state
            }
        };
        let mut handles = self
            .producer_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(cached) = handles.get(&key) {
            if cached.kind() != requested {
                return Err(violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "cached producer port does not match the requested channel contract",
                ));
            }
            if let Some(handle) = cached.upgrade() {
                return Ok(handle);
            }
        }
        let concrete = Arc::new(RemoteProducerAdapter::new(
            self._query_id,
            installed.participant_id(),
            route.channel_id(),
            installed.epoch(),
            remote_route,
            route.channel.clone(),
            binding_id,
            fragment_instance_id,
            local_partition_count,
            route.inbound_contract().clone(),
            self.reliable_transport.clone(),
            self.dispatcher.clone(),
            state,
        ));
        let handle = match requested {
            ProducerPortKind::Membership => {
                let typed: Arc<dyn ProducerAdapter> = concrete;
                ProducerHandle::Membership(typed)
            }
            ProducerPortKind::OrderedBound => {
                let typed: Arc<dyn OrderedBoundProducerAdapter> = concrete;
                ProducerHandle::OrderedBound(typed)
            }
            ProducerPortKind::TopKSummary => {
                let typed: Arc<dyn TopKSummaryProducerAdapter> = concrete;
                ProducerHandle::TopKSummary(typed)
            }
            ProducerPortKind::FinalDomain => {
                let typed: Arc<dyn FinalDomainProducerAdapter> = concrete;
                ProducerHandle::FinalDomain(typed)
            }
        };
        handles.insert(key, handle.downgrade());
        Ok(handle)
    }

    pub(super) fn open_inbound_core_locked(
        &self,
        installed: &Arc<InstalledDeployment>,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
        requested: ProducerPortKind,
    ) -> Result<OpenedProducer, RuntimeContractViolation> {
        let route = installed.producer(binding_id).ok_or_else(|| {
            violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "producer binding is not installed on this participant",
            )
        })?;
        if !route.expected_instances.contains(&fragment_instance_id) {
            return Err(violation(
                RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                "producer fragment instance is not installed for this binding",
            ));
        }
        if route.kind != requested {
            return Err(violation(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "requested producer port does not match the installed channel contract",
            ));
        }
        let outcome =
            route
                .channel
                .open_producer(binding_id, fragment_instance_id, local_partition_count)?;
        let key = (binding_id, fragment_instance_id);
        let mut handles = self
            .producer_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(cached) = handles.get(&key) {
            if cached.kind() != requested {
                return Err(violation(
                    RuntimeContractViolationKind::ProducerPortMismatch,
                    "cached producer port does not match the requested channel contract",
                ));
            }
            if let Some(handle) = cached.upgrade() {
                return Ok(OpenedProducer { handle, outcome });
            }
        }
        let final_domain_authorized =
            requested == ProducerPortKind::FinalDomain && route.final_domain_seed.is_some();
        #[cfg(test)]
        let final_domain_authority = if requested == ProducerPortKind::FinalDomain {
            Some(
                route
                    .final_domain_seed
                    .as_ref()
                    .ok_or_else(|| {
                        violation(
                            RuntimeContractViolationKind::ProducerPortMismatch,
                            "installed final-domain producer route is missing its private seed",
                        )
                    })?
                    .derive_test_authority(binding_id, fragment_instance_id)?,
            )
        } else {
            None
        };
        let concrete = Arc::new(ServiceProducerAdapter::new(
            route.channel_id,
            route.channel.clone(),
            binding_id,
            fragment_instance_id,
            self.memory_account.clone(),
            self.dispatcher.clone(),
            final_domain_authorized,
            #[cfg(test)]
            final_domain_authority,
        ));
        #[cfg(test)]
        self.producer_test_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key, Arc::downgrade(&concrete));
        let handle = match requested {
            ProducerPortKind::Membership => {
                let membership: Arc<dyn ProducerAdapter> = concrete;
                ProducerHandle::Membership(membership)
            }
            ProducerPortKind::OrderedBound => {
                let ordered: Arc<dyn OrderedBoundProducerAdapter> = concrete;
                ProducerHandle::OrderedBound(ordered)
            }
            ProducerPortKind::TopKSummary => {
                let summary: Arc<dyn TopKSummaryProducerAdapter> = concrete;
                ProducerHandle::TopKSummary(summary)
            }
            ProducerPortKind::FinalDomain => {
                let final_domain: Arc<dyn FinalDomainProducerAdapter> = concrete;
                ProducerHandle::FinalDomain(final_domain)
            }
        };
        handles.insert(key, handle.downgrade());
        Ok(OpenedProducer { handle, outcome })
    }

    pub(crate) fn subscribe(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        requested: SubscriptionKind,
    ) -> Result<SubscriptionHandle, RuntimeContractViolation> {
        let _operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = self
            .registry
            .active_installation()
            .ok_or_else(service_cancelled)?;
        let Some(activation) = installed.consumer_activation(binding_id) else {
            return Err(violation(
                RuntimeContractViolationKind::UnauthorizedBinding,
                "consumer binding is not installed on this participant",
            ));
        };
        let installed_kind = match activation {
            ConsumerActivation::BlockingSnapshot => SubscriptionKind::BlockingSnapshot,
            ConsumerActivation::NonBlockingLive { .. } => SubscriptionKind::NonBlockingLive,
        };
        if installed_kind != requested {
            return Err(violation(
                RuntimeContractViolationKind::SubscriptionActivationMismatch,
                "requested subscription kind does not match install-frozen consumer activation",
            ));
        }
        installed
            .subscription(binding_id, fragment_instance_id, requested)
            .ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                    "consumer fragment instance is not installed for this binding",
                )
            })
    }

    #[cfg(test)]
    fn subscribe_blocking(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Result<
        Arc<dyn crate::runtime_filter::port::subscription::BlockingSnapshotSubscription>,
        RuntimeContractViolation,
    > {
        self.subscribe(
            binding_id,
            fragment_instance_id,
            SubscriptionKind::BlockingSnapshot,
        )?
        .into_blocking()
    }

    /// Outbound delivery bridge: route a materialized artifact (or an `Unavailable`
    /// sentinel) for one consumer profile through the delivery Router into its
    /// loopback and/or remote edges.
    ///
    /// `route_edge_ids` is the profile's already-authorized delivery scope. The
    /// Router (`route_delivery`) is the sole fanout authority: it validates each
    /// edge against the installed routing shard and splits the scope into loopback
    /// edges (delivered in-process to the local subscriptions via the existing
    /// `LoopbackRouter`) and remote edges. Each remote edge is wire-encoded and
    /// handed to the service's sender-side [`ReliableEnvelopeTransport`], which owns
    /// ack-release, bounded retry, and deadline fail-open. The fanout is read
    /// entirely from the resulting decision — this method never widens by source role
    /// and never inspects the subscription map.
    ///
    /// `profile` is the consumer's install-owned artifact profile; it is the wire
    /// codec's contract authority for the remote leg. It is required even for a
    /// remote consumer whose profile is absent from this aggregator's local plan.
    ///
    /// Returns the Router decision so callers can observe the realized fanout.
    pub(crate) fn deliver_artifact(
        &self,
        channel_id: ChannelId,
        profile: &ConsumerArtifactProfile,
        route_edge_ids: Vec<RouteEdgeId>,
        outcome: ArtifactDeliveryOutcome,
    ) -> Result<RuntimeFilterRouteDecision, ArtifactDeliveryError> {
        let Some(call) = ServiceCall::admit(self) else {
            return Err(ArtifactDeliveryError::NotInstalled);
        };
        // Snapshot the installation under the operation lock, then release it: the
        // loopback leg delivers into subscriptions and must not run while holding a
        // lock a concurrent cancel/install could contend.
        let installed = {
            let _operation = self
                .operation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.registry.active_installation()
        }
        .ok_or(ArtifactDeliveryError::NotInstalled)?;

        let envelope_kind = match &outcome {
            ArtifactDeliveryOutcome::Published(_) => RuntimeFilterEnvelopeKind::Artifact,
            ArtifactDeliveryOutcome::Unavailable(_) => RuntimeFilterEnvelopeKind::Unavailable,
            ArtifactDeliveryOutcome::Unsupported(_) | ArtifactDeliveryOutcome::Cancelled => {
                return Err(ArtifactDeliveryError::UndeliverableOutcome);
            }
        };

        let intent = RuntimeFilterDeliveryRouteIntent::new(
            installed.epoch(),
            channel_id,
            route_edge_ids,
            envelope_kind,
        )?;
        let decision = installed.role_router().route_delivery(intent)?;

        // Loopback leg: reuse the existing publish->deliver path. `route` fans the
        // terminal outcome to each local subscription registered for the edge.
        installed
            .router()
            .route(decision.loopback_route_edge_ids(), &outcome);

        // Remote leg: wire-encode once against the consumer profile and share the
        // frame across every remote route (broadcast fans out one serialized frame),
        // handing each to the reliable transport for ack-release and bounded retry.
        // The bundle budget is the channel's installed `max_artifact_bytes` promoted
        // to its wire ceiling.
        let mut resource_limits = Vec::new();
        if !decision.remote_routes().is_empty() {
            let max_encoded = max_encoded_len_for_artifact_budget(
                installed
                    .artifact_plan(channel_id)
                    .ok_or(ArtifactDeliveryError::NotInstalled)?
                    .max_artifact_bytes(),
            )?;
            let expectation = ArtifactDecodeExpectation::new(profile);
            let frame = Arc::new(match &outcome {
                ArtifactDeliveryOutcome::Published(bundle) => {
                    encode_artifact_bundle(bundle, expectation, max_encoded)?
                }
                ArtifactDeliveryOutcome::Unavailable(reason) => {
                    encode_unavailable(*reason, expectation, max_encoded)?
                }
                ArtifactDeliveryOutcome::Unsupported(_) | ArtifactDeliveryOutcome::Cancelled => {
                    unreachable!("envelope kind rejected non-deliverable outcomes above")
                }
            });
            // The route-level event coordinates are shared across the fan-out: the same
            // query / local participant / channel / epoch, differing only by route edge.
            let common = RuntimeFilterEventIdentity::new(
                self._query_id,
                installed.participant_id(),
                channel_id,
                installed.epoch(),
            );
            for route in decision.remote_routes() {
                let identity = TransportRouteEventIdentity::new(common, route.route_edge_id());
                match self.reliable_transport.send_kind(
                    route,
                    Arc::clone(&frame),
                    identity,
                    envelope_kind,
                ) {
                    ReliableSendOutcome::Buffered(_identity) => {}
                    ReliableSendOutcome::ResourceLimit(limit) => {
                        // A self-owned transport ceiling tripped: the frame was neither
                        // buffered nor put on the wire. Degrade this route as an explicit
                        // resource-limit rejection (a first-class outcome, not a silent
                        // drop) and keep going. Runtime filters are fail-open at the query
                        // level, so the query neither errors nor panics.
                        resource_limits.push((identity, limit, frame.payload().len()));
                    }
                    ReliableSendOutcome::Shutdown => {
                        // Query teardown is terminal. A racing delivery is rejected
                        // without reviving transport state or failing the query.
                    }
                }
            }
        }

        // Resource-limit events use a fresh admission after the outbound operation.
        // This preserves parent-before-child ordering while giving a racing shutdown a
        // real linearization point at the event boundary: if close wins this gap, no
        // callback is emitted after terminal state.
        drop(call);
        for (identity, limit, bytes) in resource_limits {
            self.record_transport_resource_limit(identity, limit, bytes);
        }

        Ok(decision)
    }

    /// Seam for a remote delivery route degraded because the sender-side reliable
    /// transport hit a self-owned buffer ceiling (M3 Task 4). This is an EXPLICIT
    /// resource rejection, distinct from the deadline fail-open: the artifact exists
    /// but could not be buffered or transmitted under the transport's own limits.
    ///
    /// It deliberately does NOT deliver an `Unavailable(ResourceLimit)` sentinel to
    /// the consumer — that reason means "no artifact was produced", which is not the
    /// case here — and it deliberately does NOT touch the global MemTracker. Instead it
    /// emits a structured `TransportEnvelope` fail-open event through the SAME lifecycle
    /// sink the buffered transport steps use, so the resource-limit degradation is
    /// observable. `_limit` names which ceiling tripped for the call site; the event
    /// coarsens it to `ResourceLimit` (distinct from the deadline fail-open).
    fn record_transport_resource_limit(
        &self,
        identity: TransportRouteEventIdentity,
        _limit: TransportResourceLimit,
        bytes: usize,
    ) {
        #[cfg(test)]
        if let Some(hook) = self
            .before_resource_limit_event_admission
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .cloned()
        {
            hook();
        }
        let Some(_call) = ServiceCall::admit(self) else {
            return;
        };
        self.dispatcher
            .events
            .record(RuntimeFilterEvent::TransportEnvelope {
                identity,
                kind: TransportEventKind::FailedOpen(TransportFailOpenReason::ResourceLimit),
                bytes,
            });
    }

    pub(crate) fn expire_deadlines(&self, now: Instant) {
        let installed = {
            let _operation = self
                .operation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.registry.active_installation()
        };
        if let Some(installed) = installed {
            #[cfg(test)]
            if let Some(hook) = self
                .before_deadline_dispatch
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .cloned()
            {
                hook();
            }
            let Some(_call) = ServiceCall::admit(self) else {
                return;
            };
            for (channel_id, channel) in installed.channels() {
                let action = channel.expire_deadline(now);
                if !matches!(action, ChannelAction::None) {
                    let _ = self.dispatcher.dispatch(channel_id, action);
                }
            }
        }
    }

    /// Advance every time-owned Service subsystem from one manager-provided instant.
    /// Channel deadlines and reliable transport completions/retries share this sole
    /// production driver so their lifecycle cannot drift into separate timer owners.
    pub(crate) fn tick(&self, now: Instant) {
        let Some(_call) = ServiceCall::admit(self) else {
            return;
        };
        self.expire_deadlines(now);
        if !self.lifecycle.is_running() {
            return;
        }
        let tick = self.reliable_transport.drain_completions_and_drive(now);
        for work in tick.failed_open_work() {
            self.handle_failed_transport_open(work);
        }
    }

    fn handle_failed_transport_open(&self, work: &ReliableFailedOpenWork) {
        let producer = if let Some(identity) = work.envelope().route_identity().as_contribution() {
            Some((
                identity.producer_binding_id(),
                identity.fragment_instance_id(),
            ))
        } else {
            work.envelope()
                .route_identity()
                .as_producer_instance()
                .map(|identity| {
                    (
                        identity.producer_binding_id(),
                        identity.fragment_instance_id(),
                    )
                })
        };
        let Some((binding_id, fragment_instance_id)) = producer else {
            return;
        };
        if let Some(state) = self
            .remote_producer_states
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(binding_id, fragment_instance_id))
            .cloned()
        {
            state.mark_failed();
        }
        if let Some(installed) = self.registry.active_installation()
            && let Some(route) = installed.producer(binding_id)
            && let Ok(action) = route.channel.fail_instance(
                binding_id,
                fragment_instance_id,
                ProducerFailureReason::UpstreamUnavailable,
            )
        {
            let _ = self.dispatcher.dispatch(route.channel_id(), action);
        }
        if work.envelope().kind() == RuntimeFilterEnvelopeKind::ProducerUnavailable {
            return;
        }
        let Some(identity) = work.envelope().route_identity().as_contribution() else {
            return;
        };
        let Ok(producer_identity) = ProducerInstanceRouteIdentity::try_new(
            identity.producer_binding_id(),
            identity.fragment_instance_id(),
        ) else {
            return;
        };
        let common = work.event_identity().common();
        let Ok(envelope) = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::ProducerUnavailable,
            common.query_id(),
            common.channel_id(),
            common.epoch(),
            RuntimeFilterRouteIdentity::producer_instance(producer_identity),
            None,
            None,
            work.envelope().schema_digest(),
            encode_producer_failure(ProducerFailureReason::UpstreamUnavailable),
        ) else {
            return;
        };
        let _ = self.reliable_transport.send_envelope(
            work.route(),
            Arc::new(envelope),
            work.event_identity(),
        );
    }

    pub(crate) fn cancel(&self) {
        self.close_from_request();
    }

    fn finish_close_if_requested(&self) {
        if self.lifecycle.is_closing() {
            self.close_from_request();
        }
    }

    fn close_from_request(&self) {
        match self.lifecycle.request_close() {
            CloseRole::Closed | CloseRole::Deferred => return,
            CloseRole::Follower => {
                self.lifecycle.wait_until_closed();
                return;
            }
            CloseRole::Leader => {}
        }
        let entered_while_panicking = std::thread::panicking();
        let completion = FinalizerCompletion::new(&self.lifecycle);
        let mut first_panic = None;
        #[cfg(test)]
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.fire_after_close_request_before_quiescence();
        })) {
            retain_first_finalizer_panic(&mut first_panic, payload);
        }
        self.lifecycle.wait_for_quiescence();
        let installed = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _operation = self
                .operation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.registry.cancel()
        })) {
            Ok(installed) => installed,
            Err(payload) => {
                retain_first_finalizer_panic(&mut first_panic, payload);
                None
            }
        };
        // Terminalize outbound authority before any channel-cancellation callback can
        // reenter delivery. The transport waits for an already-admitted nonblocking
        // submission to finish, then closes its sink and releases every pending frame.
        // Repeated callers also pass through this idempotent barrier, so none can
        // return while another caller is still closing an admitted submission.
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.reliable_transport.shutdown();
        })) {
            retain_first_finalizer_panic(&mut first_panic, payload);
        }
        if let Some(cancelled) = installed {
            // Tombstone this (query, epoch) so a late/duplicate envelope arriving after
            // teardown is rejected without rebuilding context (M2B3 lookup-only). Both
            // terminal paths reach this line through `cancel()`: an explicit cancel
            // calls it directly, and normal completion reaches it via `shutdown()`,
            // which delegates to `cancel()`.
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.dedupe.retire_epoch(cancelled.installed().epoch());
            })) {
                retain_first_finalizer_panic(&mut first_panic, payload);
            }
            #[cfg(test)]
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.fire_after_registry_cancel_before_channel_cancel();
            })) {
                retain_first_finalizer_panic(&mut first_panic, payload);
            }
            for (channel_id, channel) in cancelled.installed().channels() {
                if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let locally_owned = cancelled
                        .installed()
                        .local_producer_instances(channel_id)
                        .into_iter()
                        .collect::<BTreeSet<_>>();
                    let action = channel.cancel_with_pending_producer_failures(&locally_owned);
                    let action = if matches!(action, ChannelAction::None) {
                        channel.terminal_action()
                    } else {
                        action
                    };
                    let barrier = self.dispatcher.dispatch_nonblocking(channel_id, action);
                    cancelled.arm_artifact_cancellation(channel_id, barrier);
                })) {
                    retain_first_finalizer_panic(&mut first_panic, payload);
                }
            }
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                cancelled.deliver_artifact_cancellation();
            })) {
                retain_first_finalizer_panic(&mut first_panic, payload);
            }
        }
        drop(completion);
        finish_finalizer_panic(first_panic, entered_while_panicking);
    }

    pub(crate) fn shutdown(&self) {
        self.cancel();
    }

    pub(crate) fn shutdown_transport(&self) {
        self.shutdown();
    }

    /// Point the outbound remote leg at a fake transport sink. Mirrors the other
    /// `#[cfg(test)]` seams: the service is built with the live production sink, and
    /// delivery tests override it here rather than threading a sink through every
    /// `new_with_dependencies` call site.
    #[cfg(test)]
    pub(crate) fn set_remote_sink_for_test(
        &self,
        sink: Arc<dyn crate::runtime_filter::router::remote::RuntimeFilterEnvelopeSink>,
    ) {
        self.reliable_transport.set_sink_for_test(sink);
    }

    /// Borrow the sender-side reliable transport so a fixture can synthesize acks and
    /// drive retry / deadline ticks directly against the production-wired instance.
    #[cfg(test)]
    fn reliable_transport(&self) -> &ReliableEnvelopeTransport {
        &self.reliable_transport
    }

    #[cfg(test)]
    pub(crate) fn seed_remote_transport_for_test(
        &self,
        route: &crate::runtime_filter::port::routing::RuntimeFilterRemoteRoute,
        frame: Arc<crate::runtime_filter::codec::artifact::EncodedArtifactFrame>,
        identity: TransportRouteEventIdentity,
    ) {
        assert!(matches!(
            self.reliable_transport.send(route, frame, identity),
            ReliableSendOutcome::Buffered(_)
        ));
    }

    #[cfg(test)]
    pub(crate) fn transport_pending_len_for_test(&self) -> usize {
        self.reliable_transport.pending_len()
    }

    #[cfg(test)]
    pub(crate) fn admitted_transport_envelopes_for_test(
        &self,
    ) -> Vec<(
        crate::runtime_filter::port::routing::RuntimeFilterRemoteRoute,
        Arc<crate::runtime_filter::port::transport::RuntimeFilterEnvelope>,
    )> {
        self.reliable_transport.admitted_envelopes_for_test()
    }

    #[cfg(test)]
    pub(crate) fn core_producer_handle_exists_for_test(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> bool {
        self.producer_test_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(&(binding_id, fragment_instance_id))
    }

    #[cfg(test)]
    pub(crate) fn producer_handle_is_live_for_test(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> bool {
        self.producer_test_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(binding_id, fragment_instance_id))
            .and_then(Weak::upgrade)
            .is_some()
    }

    #[cfg(test)]
    fn set_producer_before_dispatch_hook(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        if let Some(handle) = self
            .producer_test_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(binding_id, fragment_instance_id))
            .and_then(Weak::upgrade)
        {
            handle.set_before_dispatch(hook);
        }
    }

    #[cfg(test)]
    fn inject_final_domain_submit_failure_for_test(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: crate::runtime_filter::port::identity::PartitionId,
        sequence: crate::runtime_filter::port::identity::ProducerSequence,
    ) {
        self.producer_test_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(binding_id, fragment_instance_id))
            .and_then(Weak::upgrade)
            .expect("selected final-domain producer handle must be live")
            .inject_final_domain_submit_failure(partition_id, sequence);
    }

    #[cfg(test)]
    fn final_domain_test_issuer(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        open_drivers: u32,
    ) -> Option<crate::runtime_filter::port::final_domain::CollectingFinalDomainTestIssuer> {
        self.producer_test_handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(binding_id, fragment_instance_id))
            .and_then(Weak::upgrade)
            .and_then(|adapter| adapter.final_domain_test_issuer(open_drivers))
    }

    #[cfg(test)]
    fn set_dispatcher_after_claim_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .dispatcher
            .after_claim
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_before_materialization_admission_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .dispatcher
            .before_materialization_admission
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_after_materialization_gate_claim_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .dispatcher
            .after_materialization_gate_claim
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_before_encode_hook(
        &self,
        hook: Arc<dyn Fn(crate::runtime_filter::port::artifact::ConsumerProfileId) + Send + Sync>,
    ) {
        *self
            .dispatcher
            .before_encode
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_after_encode_hook(
        &self,
        hook: Arc<dyn Fn(crate::runtime_filter::port::artifact::ConsumerProfileId) + Send + Sync>,
    ) {
        *self
            .dispatcher
            .after_encode
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_before_owner_finish_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .dispatcher
            .before_owner_finish
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_after_owner_finish_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .dispatcher
            .after_owner_finish
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn dispatcher_pending_action_count(&self, channel_id: ChannelId) -> usize {
        self.dispatcher.pending_action_count(channel_id)
    }

    #[cfg(test)]
    fn set_before_commit_clock_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        self.registry.set_before_commit_clock_hook(hook);
    }

    #[cfg(test)]
    fn set_after_commit_before_publish_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        self.registry.set_after_commit_before_publish_hook(hook);
    }

    #[cfg(test)]
    fn set_after_inbound_open_admission_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .after_inbound_open_admission
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_before_inbound_typed_dispatch_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .before_inbound_typed_dispatch
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_after_registry_cancel_before_channel_cancel_hook(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        *self
            .after_registry_cancel_before_channel_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_before_deadline_dispatch_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .before_deadline_dispatch
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_after_close_request_before_quiescence_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .after_close_request_before_quiescence
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn set_before_resource_limit_event_admission_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self
            .before_resource_limit_event_admission
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn fire_after_inbound_open_admission(&self) {
        let hook = self
            .after_inbound_open_admission
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn fire_before_inbound_typed_dispatch(&self) {
        let hook = self
            .before_inbound_typed_dispatch
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn fire_after_registry_cancel_before_channel_cancel(&self) {
        let hook = self
            .after_registry_cancel_before_channel_cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn fire_after_close_request_before_quiescence(&self) {
        let hook = self
            .after_close_request_before_quiescence
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

impl Drop for RuntimeFilterService {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn service_cancelled() -> RuntimeContractViolation {
    violation(
        RuntimeContractViolationKind::ServiceUnavailable,
        "runtime filter service is uninstalled or cancelled",
    )
}

fn violation(
    kind: RuntimeContractViolationKind,
    detail: impl Into<String>,
) -> RuntimeContractViolation {
    RuntimeContractViolation::new(kind, detail)
}

// Shared production-compiler test fixture. It lives in its own module (rather than
// `mod tests`) so both `service::tests` and `service::inbound::tests` can build the
// same three-backend `AllOf` deployment from the real `deployment::compiler::compile`
// entry, never a hand-written routing shard.
#[cfg(test)]
pub(super) mod test_support {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::query_execution::backend::LiveBackendSnapshot;
    use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::deployment::compiler::compile;
    use crate::runtime_filter::deployment::{
        RuntimeFilterDeploymentPlan, RuntimeFilterDeploymentPolicy,
    };
    use crate::runtime_filter::port::identity::*;
    use crate::runtime_filter::port::install::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::planner::distributed::{
        DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
    };
    use crate::sql::planner::runtime_filter::contract::*;
    use crate::sql::planner::runtime_filter::coverage::Coverage;
    use crate::sql::planner::runtime_filter::graph::{
        ApplyPoint, ConsumerBindingTarget, ConsumerRequirement, PlanLocation,
        ProducerBindingTarget, ProducerRequirement, RuntimeFilterBindingRole,
        RuntimeFilterBindingSpec, RuntimeFilterChannelSpec, RuntimeFilterGraph,
    };

    pub(super) fn compiled_three_backend_all_of_plan() -> RuntimeFilterDeploymentPlan {
        let channel_id = ChannelId::new(5);
        let producer_binding = BindingId::new(10);
        let consumer_binding = BindingId::new(11);
        let witness = CoverageWitnessId::new(1);
        let coverage = Coverage::AllOf(vec![Coverage::Leaf(witness)]);
        let contributions = BTreeSet::from([
            ContributionKind::ValueDomainDelta,
            ContributionKind::ProducerClosed,
        ]);
        let capabilities = BTreeSet::from([
            ArtifactCapability::Membership,
            ArtifactCapability::EmptyDomain,
        ]);
        let expression = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        };
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_channel(RuntimeFilterChannelSpec {
                channel_id,
                logical_domain: RuntimeFilterLogicalDomain::Membership {
                    value_type: DataType::Int64,
                    null_semantics: NullSemantics::NeverMatches,
                },
                lifecycle: RuntimeFilterLifecycle::CompleteOnce,
                availability_coverage: coverage.clone(),
                terminal_coverage: coverage,
                reduction_requirement: ReductionRequirement::SetUnion,
                allowed_contribution_kinds: contributions.clone(),
                required_consumer_capabilities: capabilities.clone(),
                policy: RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 1024,
                    max_artifact_bytes: 1024,
                    deadline_ms: 100,
                    max_retries: 1,
                },
            })
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: producer_binding,
                channel_id,
                coverage_witness_id: Some(witness),
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(2),
                    node_id: PlanNodeId::new(1),
                },
                expression: expression.clone(),
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                    contribution_kinds: contributions,
                    completion_requirement: CompletionRequirement::ProducerClosed,
                    target: ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
                }),
            })
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: consumer_binding,
                channel_id,
                coverage_witness_id: None,
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(1),
                    node_id: PlanNodeId::new(2),
                },
                expression,
                apply_point: ApplyPoint::NodeInput,
                role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                    capabilities,
                    activation: ConsumerActivation::BlockingSnapshot,
                    target: ConsumerBindingTarget::SourceBoundary,
                }),
            })
            .unwrap();

        let placement = |fragment_id: u32,
                         instance_index: usize,
                         backend_idx: usize,
                         finst_id: UniqueId,
                         endpoint: &str| FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id,
            backend_idx,
            endpoint: RuntimeEndpoint::from_socket_addr(endpoint.parse().unwrap()),
            scan_ranges: BTreeMap::new(),
            connector_splits: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        };
        let local_producer = UniqueId::new(1, 3);
        let remote_producer = UniqueId::new(1, 4);
        let scheduling = SchedulingPlan {
            root_fragment_id: 1,
            by_fragment: BTreeMap::from([
                (
                    1,
                    vec![
                        placement(1, 0, 2, UniqueId::new(1, 1), "10.0.0.2:9060"),
                        placement(1, 1, 11, UniqueId::new(1, 2), "10.0.0.11:9060"),
                    ],
                ),
                (
                    2,
                    vec![
                        placement(2, 0, 2, local_producer, "10.0.0.2:9060"),
                        placement(2, 1, 7, remote_producer, "10.0.0.7:9060"),
                    ],
                ),
            ]),
            root_finst_id: UniqueId::new(1, 1),
            root_backend_idx: 2,
        };
        let edges = vec![FragmentEdge {
            source_fragment_id: 2,
            target_fragment_id: 1,
            target_exchange_node_id: 1,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }];
        let backends = LiveBackendSnapshot::new(vec![
            (2, "10.0.0.2:9060".parse().unwrap()),
            (7, "10.0.0.7:9060".parse().unwrap()),
            (11, "10.0.0.11:9060".parse().unwrap()),
        ]);
        let policy = RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(8192),
            replica_redundancy: 2,
            materialization: MaterializationPolicy::for_test(),
        };
        compile(
            &graph,
            &scheduling,
            &edges,
            &backends,
            &policy,
            DeploymentEpoch::new(9),
        )
        .unwrap()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Condvar, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::query_execution::backend::LiveBackendSnapshot;
    use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::codec::artifact::{
        ArtifactDecodeExpectation, EncodedArtifactFrame, decode_artifact_bundle, decode_unavailable,
    };
    use crate::runtime_filter::codec::producer::encode_producer_failure;
    use crate::runtime_filter::deployment::RuntimeFilterDeploymentPolicy;
    use crate::runtime_filter::deployment::compiler::compile;
    use crate::runtime_filter::deployment::extension::RuntimeFilterDeploymentExtension;
    use crate::runtime_filter::materializer::codec::{ArtifactDecodeExpectations, decode_leaf};
    use crate::runtime_filter::materializer::{MaterializationOutcome, Materializer};
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
        ConsumerProfileId, PhysicalArtifact,
    };
    use crate::runtime_filter::port::events::{
        ConsumerEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity,
        RuntimeFilterEventSink, TransportEventKind, TransportFailOpenReason,
        TransportRouteEventIdentity,
    };
    use crate::runtime_filter::port::final_domain::FinalDomainTestIssuerTransition;
    use crate::runtime_filter::port::identity::*;
    use crate::runtime_filter::port::install::*;
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedBoundUpdate, OrderedScalar, OrderedTuple,
        RuntimeOrderContract, comparator_digest_for_test,
    };
    use crate::runtime_filter::port::producer::{
        InstallOutcome, ProducerAdapter, ProducerFailureReason, ProducerHandle, ProducerPortKind,
        RuntimeContractViolation, RuntimeContractViolationKind, SubmitOutcome,
    };
    use crate::runtime_filter::port::routing::{
        RuntimeFilterChannelRoutingView, RuntimeFilterRemoteRoute, RuntimeFilterRouteEndpointView,
        RuntimeFilterRoutePeer, RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView,
        RuntimeFilterRoutingShard,
    };
    use crate::runtime_filter::port::subscription::{
        ArtifactAcquireOutcome, ArtifactDelivery, ArtifactDeliveryOutcome,
        BlockingSnapshotSubscription, LivePollOutcome, LiveTerminal, SubscriptionHandle,
        SubscriptionKind, UnavailableReason,
    };
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactScratchBudget, MemoryAccountError,
        RetainedMemoryReservation, RuntimeFilterClock, RuntimeFilterMemoryAccount,
        TemporaryContributionLease,
    };
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, ProducerInstanceRouteIdentity, ProducerOpenMetadata,
        RuntimeFilterAcceptStatus, RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind,
        RuntimeFilterRouteIdentity,
    };
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain, ValueDomainDelta,
    };
    use crate::runtime_filter::router::loopback::LoopbackRouter;
    use crate::runtime_filter::router::remote::{
        RuntimeFilterEnvelopeSink, SinkCompletion, SinkSubmitOutcome, SinkTransportError,
    };
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::planner::runtime_filter::{
        contract as sql_contract, coverage::Coverage as SqlCoverage, graph as sql_graph,
    };

    use super::materialization::MaterializationWorkClaim;
    use super::memory::MemTrackerMemoryAccount;
    use super::reliable_transport::{EnvelopeAckOutcome, TransportResourceLimit};
    use super::reliable_transport::{ReliableSendOutcome, ReliableTransportPolicy};
    use super::subscription::SubscriptionGroup;
    use super::{
        ActionDispatcher, ArtifactDeliveryError, ChannelAction, EventBatchCompletion, EventEmitter,
        PendingDispatch, RuntimeFilterService, run_materialization_jobs,
    };

    #[derive(Default)]
    struct Events(Mutex<Vec<RuntimeFilterEvent>>);

    impl RuntimeFilterEventSink for Events {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    struct BlockingResourceLimitEvents {
        entered: mpsc::SyncSender<()>,
        release: Mutex<Option<mpsc::Receiver<()>>>,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    impl RuntimeFilterEventSink for BlockingResourceLimitEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            if matches!(
                event,
                RuntimeFilterEvent::TransportEnvelope {
                    kind: TransportEventKind::FailedOpen(TransportFailOpenReason::ResourceLimit),
                    ..
                }
            ) {
                self.entered
                    .send(())
                    .expect("resource-limit callback entered");
                if let Some(release) = self
                    .release
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take()
                {
                    release.recv().expect("release resource-limit callback");
                }
            }
            self.recorded
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(event);
        }
    }

    struct ReentrantResourceLimitEvents {
        service: Mutex<Weak<RuntimeFilterService>>,
        observed_closed: AtomicBool,
        fired: AtomicBool,
    }

    impl RuntimeFilterEventSink for ReentrantResourceLimitEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            if matches!(
                event,
                RuntimeFilterEvent::TransportEnvelope {
                    kind: TransportEventKind::FailedOpen(TransportFailOpenReason::ResourceLimit),
                    ..
                }
            ) && !self.fired.swap(true, Ordering::AcqRel)
            {
                let service = self
                    .service
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .upgrade()
                    .expect("service alive during event callback");
                service.shutdown();
                self.observed_closed.store(
                    service
                        .lifecycle
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .phase
                        == super::LifecyclePhase::Closed,
                    Ordering::Release,
                );
            }
        }
    }

    #[derive(Default)]
    struct SameChannelReentryEvents {
        dispatcher: Mutex<Weak<ActionDispatcher>>,
        fired: AtomicBool,
    }

    impl RuntimeFilterEventSink for SameChannelReentryEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            if matches!(event, RuntimeFilterEvent::ChannelPlanned { .. })
                && !self.fired.swap(true, Ordering::SeqCst)
            {
                self.dispatcher
                    .lock()
                    .unwrap()
                    .upgrade()
                    .unwrap()
                    .dispatch(
                        ChannelId::new(1),
                        ChannelAction::Progress {
                            order: Some(0),
                            outcome: SubmitOutcome::Applied,
                            events: Vec::new(),
                        },
                    )
                    .unwrap();
            }
        }
    }

    struct CrossChannelReentryEvents {
        dispatcher: Mutex<Weak<ActionDispatcher>>,
        nested_dispatched: Mutex<Option<mpsc::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
        fired: AtomicBool,
    }

    impl RuntimeFilterEventSink for CrossChannelReentryEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            let RuntimeFilterEvent::ChannelPlanned { identity } = event else {
                return;
            };
            if identity.channel_id() != ChannelId::new(1) || self.fired.swap(true, Ordering::SeqCst)
            {
                return;
            }
            self.dispatcher
                .lock()
                .unwrap()
                .upgrade()
                .unwrap()
                .dispatch(
                    ChannelId::new(2),
                    ChannelAction::Progress {
                        order: Some(0),
                        outcome: SubmitOutcome::Applied,
                        events: vec![RuntimeFilterEvent::ChannelPlanned {
                            identity: RuntimeFilterEventIdentity::new(
                                uid(0),
                                RuntimeFilterParticipantId::new(3),
                                ChannelId::new(2),
                                DeploymentEpoch::new(9),
                            ),
                        }],
                    },
                )
                .unwrap();
            self.nested_dispatched
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
    }

    struct PanicOnceEvents {
        panicked: AtomicBool,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    struct MaterializationLifecycleEvents {
        subscription: Mutex<Option<Weak<dyn BlockingSnapshotSubscription>>>,
        panicked: AtomicBool,
        reentered: AtomicBool,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    impl RuntimeFilterEventSink for PanicOnceEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            if !self.panicked.swap(true, Ordering::SeqCst) {
                panic!("intentional event sink panic");
            }
            self.recorded.lock().unwrap().push(event);
        }
    }

    impl RuntimeFilterEventSink for MaterializationLifecycleEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            self.recorded.lock().unwrap().push(event.clone());
            if matches!(event, RuntimeFilterEvent::MaterializationStarted { .. })
                && !self.panicked.swap(true, Ordering::SeqCst)
            {
                panic!("intentional materialization event panic");
            }
            if matches!(event, RuntimeFilterEvent::ArtifactPublished { .. })
                && !self.reentered.swap(true, Ordering::SeqCst)
            {
                let subscription = self
                    .subscription
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(Weak::upgrade)
                    .expect("subscription remains live for materialization reentry");
                assert!(matches!(
                    subscription.acquire(Duration::ZERO),
                    ArtifactAcquireOutcome::Published(_)
                ));
            }
        }
    }

    struct Clock(Instant);

    impl RuntimeFilterClock for Clock {
        fn now(&self) -> Instant {
            self.0
        }
    }

    struct DynamicClock;

    impl RuntimeFilterClock for DynamicClock {
        fn now(&self) -> Instant {
            Instant::now()
        }
    }

    struct NearMaxClock;

    impl RuntimeFilterClock for NearMaxClock {
        fn now(&self) -> Instant {
            let base = Instant::now();
            let mut lower = 0u64;
            let mut upper = u64::MAX;
            while lower < upper {
                let midpoint = lower + (upper - lower) / 2 + 1;
                if base.checked_add(Duration::from_secs(midpoint)).is_some() {
                    lower = midpoint;
                } else {
                    upper = midpoint - 1;
                }
            }
            base.checked_add(Duration::from_secs(lower)).unwrap()
        }
    }

    #[derive(Default)]
    struct RejectingMemoryAccount {
        calls: AtomicUsize,
    }

    struct BlockingFirstRejectingMemoryAccount {
        calls: AtomicUsize,
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    #[derive(Default)]
    struct PanicWhenArmedMemoryAccount {
        armed: AtomicBool,
    }

    #[derive(Default)]
    struct RejectSecondWhenArmedMemoryAccount {
        armed: AtomicBool,
        armed_calls: AtomicUsize,
        current: AtomicUsize,
    }

    #[derive(Default)]
    struct ArmableRejectingMemoryAccount {
        armed: AtomicBool,
        current: AtomicUsize,
    }

    impl RuntimeFilterMemoryAccount for ArmableRejectingMemoryAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            if self.armed.load(Ordering::SeqCst) {
                return Err(MemoryAccountError::CapacityExceeded);
            }
            self.current.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.current.fetch_sub(bytes, Ordering::SeqCst);
            assert!(previous >= bytes);
        }
    }

    impl RuntimeFilterMemoryAccount for PanicWhenArmedMemoryAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            assert!(
                !self.armed.load(Ordering::SeqCst) || bytes <= 64,
                "intentional materialization memory panic"
            );
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    impl RuntimeFilterMemoryAccount for RejectSecondWhenArmedMemoryAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            if self.armed.load(Ordering::SeqCst)
                && self.armed_calls.fetch_add(1, Ordering::SeqCst) == 1
            {
                return Err(MemoryAccountError::CapacityExceeded);
            }
            self.current.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.current.fetch_sub(bytes, Ordering::SeqCst);
            assert!(previous >= bytes);
        }
    }

    impl RuntimeFilterMemoryAccount for BlockingFirstRejectingMemoryAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
                return Err(MemoryAccountError::CapacityExceeded);
            }
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    struct BlockingInstallEvents {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    struct BlockingLastInstallEvent {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    struct BlockingChannelDeadlineEvents {
        entered: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    impl RuntimeFilterEventSink for BlockingChannelDeadlineEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            if matches!(event, RuntimeFilterEvent::ChannelUnavailable { .. }) {
                self.entered.send(()).expect("channel callback entered");
                self.release
                    .lock()
                    .expect("channel callback release")
                    .recv_timeout(Duration::from_secs(1))
                    .expect("channel callback released");
            }
            self.recorded.lock().expect("recorded events").push(event);
        }
    }

    impl BlockingChannelDeadlineEvents {
        fn unavailable_count(&self) -> usize {
            self.recorded
                .lock()
                .expect("recorded events")
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::ChannelUnavailable { .. }))
                .count()
        }
    }

    impl RuntimeFilterEventSink for BlockingLastInstallEvent {
        fn record(&self, event: RuntimeFilterEvent) {
            if matches!(event, RuntimeFilterEvent::ChannelPlanned { .. }) {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
            self.recorded.lock().unwrap().push(event);
        }
    }

    impl RuntimeFilterEventSink for BlockingInstallEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            if matches!(event, RuntimeFilterEvent::DeploymentInstalled { .. }) {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
            self.recorded.lock().unwrap().push(event);
        }
    }

    impl RuntimeFilterMemoryAccount for RejectingMemoryAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(MemoryAccountError::CapacityExceeded)
        }

        fn release(&self, _bytes: usize) {}
    }

    struct ReentrantServiceClock {
        service: Mutex<Weak<RuntimeFilterService>>,
        now: Instant,
    }

    impl RuntimeFilterClock for ReentrantServiceClock {
        fn now(&self) -> Instant {
            let service = self
                .service
                .lock()
                .unwrap()
                .upgrade()
                .expect("service installed before clock use");
            assert_eq!(
                service
                    .subscribe_blocking(BindingId::new(30), uid(30))
                    .err()
                    .expect("service must remain unavailable during installation")
                    .kind(),
                RuntimeContractViolationKind::ServiceUnavailable
            );
            self.now
        }
    }

    fn uid(lo: i64) -> UniqueId {
        UniqueId::new(70, lo)
    }

    fn deployment(
        channel_id: u32,
        producer_binding: u32,
        consumer_binding: u32,
        route_edge: u32,
        producer_instances: impl IntoIterator<Item = i64>,
        consumer_instances: impl IntoIterator<Item = i64>,
        deadline_ms: u64,
    ) -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(channel_id + 100);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(channel_id),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms,
                max_retries: 2,
            },
            RuntimeFilterCoreBudget::new(8192),
            crate::runtime_filter::port::install::MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(producer_binding),
                ProducerDeployment::new(witness, producer_instances.into_iter().map(uid).collect()),
            )]),
            BTreeMap::from([(
                BindingId::new(consumer_binding),
                ConsumerDeployment::new(
                    ConsumerActivation::BlockingSnapshot,
                    BTreeSet::from([ArtifactCapability::Membership]),
                    BTreeSet::from([RouteEdgeId::new(route_edge)]),
                    consumer_instances.into_iter().map(uid).collect(),
                ),
            )]),
        )
    }

    pub(crate) fn installed_join_loopback_service_for_exec_test() -> (
        Arc<RuntimeFilterService>,
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    ) {
        let query_id = uid(0);
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            query_id,
            Arc::new(DynamicClock),
            Arc::new(Events::default()),
            MemTrackerMemoryAccount::new_root_for_test("native-join-loopback"),
        ));
        service
            .install(inbound_loopback_install_for_test(deployment(
                1,
                10,
                30,
                40,
                [10],
                [30],
                1_000,
            )))
            .expect("install native Join loopback deployment");
        (
            Arc::clone(&service),
            crate::runtime_filter::service::NativeRuntimeFilterExecutionContext::new(
                Arc::clone(&service),
                query_id,
                DeploymentEpoch::new(9),
                uid(10),
            ),
            crate::runtime_filter::service::NativeRuntimeFilterExecutionContext::new(
                Arc::clone(&service),
                query_id,
                DeploymentEpoch::new(9),
                uid(30),
            ),
        )
    }

    mod native_execution_resolution {
        use super::*;
        use crate::runtime_filter::service::native_execution::NativeRuntimeFilterExecutionContext;

        fn installed_membership_service() -> (
            Arc<RuntimeFilterService>,
            NativeRuntimeFilterExecutionContext,
        ) {
            let query = uid(0);
            let finst = uid(10);
            let channel = RuntimeFilterChannelDeployment::new(
                ChannelId::new(1),
                RuntimeFilterLogicalDomain::Membership {
                    value_type: DataType::Int64,
                    null_semantics: NullSemantics::NeverMatches,
                },
                RuntimeFilterLifecycle::CompleteOnce,
                Coverage::Leaf(CoverageWitnessId::new(101)),
                Coverage::Leaf(CoverageWitnessId::new(101)),
                ReductionRequirement::SetUnion,
                BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::ProducerClosed,
                RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 321,
                    max_artifact_bytes: 4_096,
                    deadline_ms: 100,
                    max_retries: 2,
                },
                RuntimeFilterCoreBudget::new(8_192),
                MaterializationPolicy::for_test(),
                BTreeMap::from([(
                    BindingId::new(10),
                    ProducerDeployment::new(CoverageWitnessId::new(101), BTreeSet::from([finst])),
                )]),
                BTreeMap::from([(
                    BindingId::new(30),
                    ConsumerDeployment::new(
                        ConsumerActivation::BlockingSnapshot,
                        BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                        BTreeSet::from([RouteEdgeId::new(40)]),
                        BTreeSet::from([uid(30)]),
                    ),
                )]),
            );
            let install = local_participant_install_for_test(RuntimeFilterInstallView::new(
                DeploymentEpoch::new(9),
                RuntimeFilterParticipantId::new(3),
                BTreeMap::from([(ChannelId::new(1), channel)]),
            ));
            let service = Arc::new(RuntimeFilterService::new_with_dependencies(
                query,
                Arc::new(Clock(Instant::now())),
                Arc::new(Events::default()),
                MemTrackerMemoryAccount::new_root_for_test("native-resolution"),
            ));
            service.install(install).expect("valid install");
            let context = NativeRuntimeFilterExecutionContext::new(
                Arc::clone(&service),
                query,
                DeploymentEpoch::new(9),
                finst,
            );
            (service, context)
        }

        #[test]
        fn resolved_producer_exposes_installed_max_contribution_bytes() {
            let (_service, context) = installed_membership_service();

            let resolved = context
                .resolve_producer(
                    BindingId::new(10),
                    ChannelId::new(1),
                    ProducerPortKind::Membership,
                )
                .expect("exact installed producer");

            assert_eq!(resolved.max_contribution_bytes(), 321);
            assert_ne!(resolved.max_contribution_bytes(), 4_096);
            assert_eq!(
                resolved.allowed_contribution_kinds(),
                &BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ])
            );
            assert_eq!(
                resolved.completion_requirement(),
                CompletionRequirement::ProducerClosed
            );
            assert_eq!(
                resolved.reduction_requirement(),
                ReductionRequirement::SetUnion
            );
        }

        #[test]
        fn execution_session_opens_only_the_exact_installed_producer_contract() {
            use novarocks_execution::runtime_filter::{
                RuntimeFilterBindOutcome, RuntimeFilterExecutionContract,
                RuntimeFilterProducerContract, RuntimeFilterProducerKind,
                RuntimeFilterProducerOpenRequest, RuntimeFilterSession,
            };

            let (_service, context) = installed_membership_service();
            let schema =
                ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches)
                    .expect("installed membership schema");
            let contract = RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            };
            let request = RuntimeFilterProducerOpenRequest::new(
                RuntimeFilterProducerContract::new(
                    novarocks_execution::runtime_filter::RuntimeFilterBindingId::new(10),
                    novarocks_execution::runtime_filter::RuntimeFilterChannelId::new(1),
                    RuntimeFilterProducerKind::Membership,
                    contract.clone(),
                ),
                1,
            );
            assert!(matches!(
                RuntimeFilterSession::open_producer(&context, request),
                Ok(RuntimeFilterBindOutcome::Bound(_))
            ));

            let mismatched = RuntimeFilterProducerOpenRequest::new(
                RuntimeFilterProducerContract::new(
                    novarocks_execution::runtime_filter::RuntimeFilterBindingId::new(10),
                    novarocks_execution::runtime_filter::RuntimeFilterChannelId::new(1),
                    RuntimeFilterProducerKind::Membership,
                    RuntimeFilterExecutionContract::Membership {
                        canonical_schema: Arc::from(schema.canonical_bytes()),
                        schema_digest: [0; 32],
                    },
                ),
                1,
            );
            let error = match RuntimeFilterSession::open_producer(&context, mismatched) {
                Err(error) => error,
                Ok(_) => panic!("execution session must reject a mismatched route contract"),
            };
            assert_eq!(
                error.kind(),
                novarocks_execution::runtime_filter::RuntimeFilterContractViolationKind::ContractMismatch
            );
        }

        #[test]
        fn execution_session_binds_only_the_exact_installed_consumer_contract() {
            use novarocks_execution::runtime_filter::{
                ConsumerActivation as ExecutionConsumerActivation, RuntimeFilterBindOutcome,
                RuntimeFilterConsumerContract, RuntimeFilterExecutionContract,
                RuntimeFilterSession, RuntimeFilterSubscriptionRequest,
            };

            let (_service, producer_context) = installed_membership_service();
            let context = NativeRuntimeFilterExecutionContext::new(
                Arc::clone(producer_context.service()),
                producer_context.query_id(),
                producer_context.epoch(),
                uid(30),
            );
            let schema =
                ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches)
                    .expect("installed membership schema");
            let contract = RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            };
            let request =
                RuntimeFilterSubscriptionRequest::new(RuntimeFilterConsumerContract::new(
                    novarocks_execution::runtime_filter::RuntimeFilterBindingId::new(30),
                    novarocks_execution::runtime_filter::RuntimeFilterChannelId::new(1),
                    ExecutionConsumerActivation::BlockingSnapshot,
                    contract.clone(),
                ));
            assert!(matches!(
                RuntimeFilterSession::subscribe(&context, request),
                Ok(RuntimeFilterBindOutcome::Bound(
                    novarocks_execution::runtime_filter::RuntimeFilterSubscriptionHandle::Blocking(
                        _
                    )
                ))
            ));

            let mismatched =
                RuntimeFilterSubscriptionRequest::new(RuntimeFilterConsumerContract::new(
                    novarocks_execution::runtime_filter::RuntimeFilterBindingId::new(30),
                    novarocks_execution::runtime_filter::RuntimeFilterChannelId::new(1),
                    ExecutionConsumerActivation::BlockingSnapshot,
                    RuntimeFilterExecutionContract::Membership {
                        canonical_schema: Arc::from(schema.canonical_bytes()),
                        schema_digest: [0; 32],
                    },
                ));
            let error = match RuntimeFilterSession::subscribe(&context, mismatched) {
                Err(error) => error,
                Ok(_) => panic!("execution session must reject a mismatched consumer contract"),
            };
            assert_eq!(
                error.kind(),
                novarocks_execution::runtime_filter::RuntimeFilterContractViolationKind::ContractMismatch
            );
        }

        #[test]
        fn native_binding_role_kind_and_contract_mismatch_fail_before_open() {
            let (service, context) = installed_membership_service();

            let role = context
                .resolve_producer(
                    BindingId::new(30),
                    ChannelId::new(1),
                    ProducerPortKind::Membership,
                )
                .expect_err("consumer binding cannot resolve as producer");
            assert!(role.to_string().contains("producer role"), "{role}");

            let kind = context
                .resolve_producer(
                    BindingId::new(10),
                    ChannelId::new(1),
                    ProducerPortKind::OrderedBound,
                )
                .expect_err("port-kind drift must fail before open");
            assert!(kind.to_string().contains("port kind"), "{kind}");

            let _operation = service
                .operation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let installed = service
                .registry
                .active_installation()
                .expect("installed deployment");
            let opened = service
                .open_inbound_core_locked(
                    &installed,
                    BindingId::new(10),
                    uid(10),
                    2,
                    ProducerPortKind::Membership,
                )
                .expect("failed resolutions had no producer-open side effect");
            assert_eq!(opened.outcome, SubmitOutcome::Applied);
        }

        #[test]
        fn native_pipeline_resolves_exact_fragment_instance() {
            let (_service, context) = installed_membership_service();
            context
                .resolve_producer(
                    BindingId::new(10),
                    ChannelId::new(1),
                    ProducerPortKind::Membership,
                )
                .expect("installed fragment instance");

            let wrong_context = NativeRuntimeFilterExecutionContext::new(
                Arc::clone(context.service()),
                context.query_id(),
                context.epoch(),
                uid(999),
            );
            let error = wrong_context
                .resolve_producer(
                    BindingId::new(10),
                    ChannelId::new(1),
                    ProducerPortKind::Membership,
                )
                .expect_err("uninstalled fragment instance");
            assert!(error.to_string().contains("fragment instance"), "{error}");
        }

        #[test]
        fn local_operator_cannot_open_remote_fragment_instance_on_aggregator() {
            let query = uid(0);
            let (install, binding_id, remote_finst) =
                super::compiled_three_backend_all_of_aggregator_install();
            let service = Arc::new(RuntimeFilterService::new_with_dependencies(
                query,
                Arc::new(Clock(Instant::now())),
                Arc::new(Events::default()),
                MemTrackerMemoryAccount::new_root_for_test("native-aggregator-resolution"),
            ));
            service.install(install).expect("aggregator install");

            let error = service
                .open_producer(binding_id, remote_finst, 1, ProducerPortKind::Membership)
                .expect_err("aggregator cannot impersonate a remote producer instance");
            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::UnauthorizedFragmentInstance
            );
            assert!(!service.core_producer_handle_exists_for_test(binding_id, remote_finst));
        }

        #[test]
        fn native_consumer_resolution_matches_activation_capability_and_fragment() {
            let (_service, producer_context) = installed_membership_service();
            let context = NativeRuntimeFilterExecutionContext::new(
                Arc::clone(producer_context.service()),
                producer_context.query_id(),
                producer_context.epoch(),
                uid(30),
            );
            let resolved = context
                .resolve_consumer(
                    BindingId::new(30),
                    ChannelId::new(1),
                    SubscriptionKind::BlockingSnapshot,
                )
                .expect("exact installed consumer");
            assert_eq!(
                resolved.capabilities(),
                &BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ])
            );
            let error = context
                .resolve_consumer(
                    BindingId::new(30),
                    ChannelId::new(1),
                    SubscriptionKind::NonBlockingLive,
                )
                .expect_err("activation drift");
            assert!(error.to_string().contains("activation"), "{error}");
        }

        #[test]
        fn native_consumer_rejects_uninstalled_fragment_instance() {
            let (_service, producer_context) = installed_membership_service();
            let wrong_context = NativeRuntimeFilterExecutionContext::new(
                Arc::clone(producer_context.service()),
                producer_context.query_id(),
                producer_context.epoch(),
                uid(999),
            );

            let error = wrong_context
                .resolve_consumer(
                    BindingId::new(30),
                    ChannelId::new(1),
                    SubscriptionKind::BlockingSnapshot,
                )
                .expect_err("consumer fragment instance is not installed");

            assert_eq!(
                error.kind(),
                RuntimeContractViolationKind::UnauthorizedFragmentInstance
            );
        }
    }

    fn fenced_final_deployment() -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(101);
        let coverage = Coverage::AllOf(vec![Coverage::Leaf(witness)]);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            coverage.clone(),
            coverage,
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 2,
            },
            RuntimeFilterCoreBudget::new(8192),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(10),
                ProducerDeployment::new(witness, BTreeSet::from([uid(10)])),
            )]),
            BTreeMap::from([(
                BindingId::new(30),
                ConsumerDeployment::with_profile(
                    ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
                    },
                    BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    ConsumerArtifactProfile::new(
                        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
                        None,
                    )
                    .unwrap(),
                    BTreeSet::from([RouteEdgeId::new(40)]),
                    BTreeSet::from([uid(30)]),
                ),
            )]),
        )
    }

    pub(super) fn compiled_fenced_final_install() -> RuntimeFilterParticipantInstall {
        let deployment = fenced_final_deployment();
        let expression = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        };
        let mut graph = sql_graph::RuntimeFilterGraph::default();
        graph
            .insert_channel(sql_graph::RuntimeFilterChannelSpec {
                channel_id: sql_contract::ChannelId::new(deployment.channel_id().get()),
                logical_domain: sql_contract::RuntimeFilterLogicalDomain::Membership {
                    value_type: DataType::Int64,
                    null_semantics: sql_contract::NullSemantics::NullSafeEqual,
                },
                lifecycle: sql_contract::RuntimeFilterLifecycle::CompleteOnce,
                availability_coverage: SqlCoverage::AllOf(vec![SqlCoverage::Leaf(
                    sql_contract::CoverageWitnessId::new(101),
                )]),
                terminal_coverage: SqlCoverage::AllOf(vec![SqlCoverage::Leaf(
                    sql_contract::CoverageWitnessId::new(101),
                )]),
                reduction_requirement: sql_contract::ReductionRequirement::SetUnion,
                allowed_contribution_kinds: BTreeSet::from([
                    sql_contract::ContributionKind::FinalDomainShard,
                    sql_contract::ContributionKind::ProducerClosed,
                ]),
                required_consumer_capabilities: BTreeSet::from([
                    sql_contract::ArtifactCapability::Membership,
                    sql_contract::ArtifactCapability::EmptyDomain,
                ]),
                policy: sql_contract::RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: deployment.policy().max_contribution_bytes,
                    max_artifact_bytes: deployment.policy().max_artifact_bytes,
                    deadline_ms: deployment.policy().deadline_ms,
                    max_retries: deployment.policy().max_retries,
                },
            })
            .unwrap();
        graph
            .insert_binding(sql_graph::RuntimeFilterBindingSpec {
                binding_id: sql_contract::BindingId::new(10),
                channel_id: sql_contract::ChannelId::new(1),
                coverage_witness_id: Some(sql_contract::CoverageWitnessId::new(101)),
                location: sql_graph::PlanLocation {
                    fragment_id: sql_contract::PlanFragmentId::new(0),
                    node_id: sql_contract::PlanNodeId::new(1),
                },
                expression: expression.clone(),
                apply_point: sql_graph::ApplyPoint::NodeOutput,
                role: sql_graph::RuntimeFilterBindingRole::Producer(
                    sql_graph::ProducerRequirement {
                        contribution_kinds: BTreeSet::from([
                            sql_contract::ContributionKind::FinalDomainShard,
                            sql_contract::ContributionKind::ProducerClosed,
                        ]),
                        completion_requirement:
                            sql_contract::CompletionRequirement::FencedFinalDomain(
                                sql_contract::CompletionFenceKind::CommittedDomainFrozen,
                            ),
                        target: sql_graph::ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
                    },
                ),
            })
            .unwrap();
        graph
            .insert_binding(sql_graph::RuntimeFilterBindingSpec {
                binding_id: sql_contract::BindingId::new(30),
                channel_id: sql_contract::ChannelId::new(1),
                coverage_witness_id: None,
                location: sql_graph::PlanLocation {
                    fragment_id: sql_contract::PlanFragmentId::new(0),
                    node_id: sql_contract::PlanNodeId::new(2),
                },
                expression,
                apply_point: sql_graph::ApplyPoint::NodeInput,
                role: sql_graph::RuntimeFilterBindingRole::Consumer(
                    sql_graph::ConsumerRequirement {
                        capabilities: BTreeSet::from([
                            sql_contract::ArtifactCapability::Membership,
                            sql_contract::ArtifactCapability::EmptyDomain,
                        ]),
                        activation: sql_contract::ConsumerActivation::NonBlockingLive {
                            late_apply: sql_contract::LateApplyGranularity::Batch,
                        },
                        target: sql_graph::ConsumerBindingTarget::SourceBoundary,
                    },
                ),
            })
            .unwrap();
        let placement = FragmentInstancePlacement {
            fragment_id: 0,
            instance_index: 0,
            finst_id: uid(10),
            backend_idx: 0,
            endpoint: RuntimeEndpoint::from_socket_addr("127.0.0.1:9060".parse().unwrap()),
            scan_ranges: BTreeMap::new(),
            connector_splits: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        };
        let scheduling = SchedulingPlan {
            root_fragment_id: 0,
            by_fragment: BTreeMap::from([(0, vec![placement])]),
            root_finst_id: uid(10),
            root_backend_idx: 0,
        };
        let backends = LiveBackendSnapshot::from_endpoints(vec!["127.0.0.1:9060".parse().unwrap()]);
        let policy = RuntimeFilterDeploymentPolicy {
            core_budget: deployment.core_budget(),
            replica_redundancy: 1,
            materialization: deployment.materialization_policy(),
        };
        let mut plan = compile(
            &graph,
            &scheduling,
            &[],
            &backends,
            &policy,
            DeploymentEpoch::new(9),
        )
        .unwrap();
        let participant = RuntimeFilterParticipantId::new(1);
        let core_view = plan
            .install_views
            .remove(&participant)
            .expect("compiler projects the colocated aggregate install view");
        let routing_shard = plan
            .routing_shards
            .remove(&participant)
            .expect("compiler projects the matching routing shard");
        RuntimeFilterParticipantInstall::new(core_view, routing_shard)
    }

    pub(super) fn compiled_three_backend_all_of_aggregator_install()
    -> (RuntimeFilterParticipantInstall, BindingId, UniqueId) {
        let channel_id = ChannelId::new(5);
        let producer_binding = BindingId::new(10);
        let remote_producer = UniqueId::new(1, 4);
        // The shared fixture places this producer on backend index 7. Participant
        // identities are deliberately nonzero, so the compiler projects backend N
        // as participant N + 1 rather than preserving the backend index verbatim.
        let remote_participant =
            crate::runtime_filter::deployment::participant_id_for_backend(7).unwrap();
        let mut plan = super::test_support::compiled_three_backend_all_of_plan();
        let aggregator = plan
            .routing_shards
            .iter()
            .find_map(|(participant, shard)| {
                shard
                    .channel(channel_id)
                    .filter(|channel| {
                        channel
                            .local_roles()
                            .contains(&RuntimeFilterRouteRole::Aggregator)
                    })
                    .map(|_| *participant)
            })
            .expect("AllOf compiler plan has an aggregator participant");
        let routing_channel = plan.routing_shards[&aggregator]
            .channel(channel_id)
            .unwrap();
        assert_eq!(
            routing_channel.producer_participant(producer_binding, remote_producer),
            Some(remote_participant)
        );
        assert_ne!(aggregator, remote_participant);
        let core_view = plan.install_views.remove(&aggregator).unwrap();
        let routing_shard = plan.routing_shards.remove(&aggregator).unwrap();
        (
            RuntimeFilterParticipantInstall::new(core_view, routing_shard),
            producer_binding,
            remote_producer,
        )
    }

    pub(super) fn inbound_loopback_install_for_test(
        channel: RuntimeFilterChannelDeployment,
    ) -> RuntimeFilterParticipantInstall {
        let mut grouped = BTreeMap::<
            crate::runtime_filter::port::artifact::ConsumerProfileId,
            (
                crate::runtime_filter::port::artifact::ConsumerArtifactProfile,
                BTreeSet<RouteEdgeId>,
            ),
        >::new();
        for consumer in channel.consumers().values() {
            grouped
                .entry(consumer.artifact_profile().id())
                .or_insert_with(|| (consumer.artifact_profile().clone(), BTreeSet::new()))
                .1
                .extend(consumer.route_edge_ids().iter().copied());
        }
        let channel = channel.with_outbound_materialization_groups(
            grouped
                .into_iter()
                .map(|(profile_id, (profile, routes))| {
                    (
                        profile_id,
                        OutboundMaterializationGroup::new(
                            OutboundMaterializationOwner::Aggregator,
                            profile,
                            routes,
                        ),
                    )
                })
                .collect(),
        );
        let epoch = DeploymentEpoch::new(9);
        let participant = RuntimeFilterParticipantId::new(3);
        let channel_id = channel.channel_id();
        let mut local_roles = BTreeSet::from([RuntimeFilterRouteRole::Aggregator]);
        let mut producer_instances = BTreeMap::new();
        let mut inbound_edges = Vec::new();
        let mut outbound_edges = Vec::new();
        for (index, (binding_id, producer)) in channel.producers().iter().enumerate() {
            local_roles.insert(RuntimeFilterRouteRole::Producer(*binding_id));
            for fragment_instance_id in producer.expected_fragment_instances() {
                producer_instances.insert((*binding_id, *fragment_instance_id), participant);
            }
            let edge = RuntimeFilterRoutingEdgeView::new(
                channel_id,
                RouteEdgeId::new(u32::try_from(index).unwrap() + 1),
                RuntimeFilterRouteEndpointView::new(
                    participant,
                    RuntimeFilterRouteRole::Producer(*binding_id),
                ),
                RuntimeFilterRouteEndpointView::new(
                    participant,
                    RuntimeFilterRouteRole::Aggregator,
                ),
                RuntimeFilterRoutePeer::Loopback,
                BTreeSet::from([
                    RuntimeFilterEnvelopeKind::Contribution,
                    RuntimeFilterEnvelopeKind::ProducerClosed,
                    RuntimeFilterEnvelopeKind::ProducerUnavailable,
                ]),
            )
            .unwrap();
            inbound_edges.push(edge.clone());
            outbound_edges.push(edge);
        }
        for (binding_id, consumer) in channel.consumers() {
            local_roles.insert(RuntimeFilterRouteRole::Consumer(*binding_id));
            for route_edge_id in consumer.route_edge_ids() {
                let edge = RuntimeFilterRoutingEdgeView::new(
                    channel_id,
                    *route_edge_id,
                    RuntimeFilterRouteEndpointView::new(
                        participant,
                        RuntimeFilterRouteRole::Aggregator,
                    ),
                    RuntimeFilterRouteEndpointView::new(
                        participant,
                        RuntimeFilterRouteRole::Consumer(*binding_id),
                    ),
                    RuntimeFilterRoutePeer::Loopback,
                    BTreeSet::from([
                        RuntimeFilterEnvelopeKind::Artifact,
                        RuntimeFilterEnvelopeKind::Unavailable,
                        RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                        RuntimeFilterEnvelopeKind::DegradedLogical,
                        RuntimeFilterEnvelopeKind::FinalArtifact,
                    ]),
                )
                .unwrap();
                inbound_edges.push(edge.clone());
                outbound_edges.push(edge);
            }
        }
        let routing_channel = RuntimeFilterChannelRoutingView::new(
            channel_id,
            local_roles,
            producer_instances,
            inbound_edges,
            outbound_edges,
        )
        .unwrap();
        let routing_shard = RuntimeFilterRoutingShard::new(
            epoch,
            participant,
            BTreeMap::from([(channel_id, routing_channel)]),
        )
        .unwrap();
        let core_view = RuntimeFilterInstallView::new(
            epoch,
            participant,
            BTreeMap::from([(channel_id, channel)]),
        );
        RuntimeFilterParticipantInstall::new(core_view, routing_shard)
    }

    fn view(
        channels: impl IntoIterator<Item = RuntimeFilterChannelDeployment>,
    ) -> RuntimeFilterParticipantInstall {
        local_participant_install_for_test(RuntimeFilterInstallView::new(
            DeploymentEpoch::new(9),
            RuntimeFilterParticipantId::new(3),
            channels
                .into_iter()
                .map(|channel| (channel.channel_id(), channel))
                .collect(),
        ))
    }

    fn deployment_with_profiles(
        consumers: impl IntoIterator<Item = (u32, u32, i64, ConsumerArtifactProfile)>,
    ) -> RuntimeFilterChannelDeployment {
        let consumers = consumers.into_iter().collect::<Vec<_>>();
        let max_concurrent_jobs = consumers
            .iter()
            .map(|(_, _, _, profile)| profile.id())
            .collect::<BTreeSet<_>>()
            .len();
        deployment_with_profiles_and_concurrency(consumers, max_concurrent_jobs)
    }

    fn deployment_with_profiles_and_concurrency(
        consumers: impl IntoIterator<Item = (u32, u32, i64, ConsumerArtifactProfile)>,
        max_concurrent_jobs: usize,
    ) -> RuntimeFilterChannelDeployment {
        deployment_with_profiles_concurrency_and_activation(
            consumers,
            max_concurrent_jobs,
            ConsumerActivation::BlockingSnapshot,
        )
    }

    fn deployment_with_profiles_concurrency_and_activation(
        consumers: impl IntoIterator<Item = (u32, u32, i64, ConsumerArtifactProfile)>,
        max_concurrent_jobs: usize,
        activation: ConsumerActivation,
    ) -> RuntimeFilterChannelDeployment {
        let base = match activation {
            ConsumerActivation::BlockingSnapshot => deployment(1, 10, 30, 40, [10], [30], 100),
            ConsumerActivation::NonBlockingLive { .. } => fenced_final_deployment(),
        };
        let consumers = consumers
            .into_iter()
            .map(|(binding, route, instance, profile)| {
                (
                    BindingId::new(binding),
                    ConsumerDeployment::with_profile(
                        activation,
                        BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                        profile,
                        BTreeSet::from([RouteEdgeId::new(route)]),
                        BTreeSet::from([uid(instance)]),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        RuntimeFilterChannelDeployment::new(
            base.channel_id(),
            base.logical_domain().clone(),
            base.lifecycle(),
            base.availability_coverage().clone(),
            base.terminal_coverage().clone(),
            base.reduction_requirement(),
            base.allowed_contribution_kinds().clone(),
            base.completion_requirement(),
            base.policy(),
            base.core_budget(),
            crate::runtime_filter::port::install::MaterializationPolicy::new(
                8,
                5,
                17,
                1,
                1 << 20,
                1 << 16,
                max_concurrent_jobs,
            )
            .unwrap(),
            base.producers().clone(),
            consumers,
        )
    }

    pub(super) struct Fixture {
        pub(super) service: Arc<RuntimeFilterService>,
        events: Arc<Events>,
        started: Instant,
        tracker: Arc<MemTrackerMemoryAccount>,
    }

    pub(super) fn fixture() -> Fixture {
        let events = Arc::new(Events::default());
        let started = Instant::now();
        let tracker = MemTrackerMemoryAccount::new_root_for_test("runtime-filter-test-query");
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(started)),
            events.clone(),
            tracker.clone(),
        ));
        Fixture {
            service,
            events,
            started,
            tracker,
        }
    }

    #[test]
    fn fenced_final_install_opens_only_typed_handle_and_completes_through_private_authority() {
        let fixture = fixture();
        assert_eq!(
            fixture
                .service
                .install(compiled_fenced_final_install())
                .unwrap(),
            InstallOutcome::Installed
        );
        for wrong in [
            ProducerPortKind::Membership,
            ProducerPortKind::OrderedBound,
            ProducerPortKind::TopKSummary,
        ] {
            assert_eq!(
                fixture
                    .service
                    .open_producer(BindingId::new(10), uid(10), 1, wrong)
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::ProducerPortMismatch
            );
        }
        let ProducerHandle::FinalDomain(producer) = fixture
            .service
            .open_producer(
                BindingId::new(10),
                uid(10),
                1,
                ProducerPortKind::FinalDomain,
            )
            .unwrap()
        else {
            panic!("fenced-final install must return the typed final-domain handle")
        };
        let collecting = fixture
            .service
            .final_domain_test_issuer(BindingId::new(10), uid(10), 1)
            .expect("service adapter privately owns the installed authority");
        let FinalDomainTestIssuerTransition::Frozen(issuer) = collecting.close_driver() else {
            panic!("last driver close freezes the committed domain")
        };
        let shard = issuer
            .issue_shard(
                ProducerStreamId::new(BindingId::new(10), uid(10), PartitionId::new(0)),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([7]), false),
            )
            .unwrap();
        assert_eq!(
            producer
                .complete(PartitionId::new(0), ProducerSequence::new(0), shard)
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            SubmitOutcome::Completed
        );
    }

    pub(super) fn installed_ordered_service_fixture() -> Arc<RuntimeFilterService> {
        installed_ordered_service_with_account(MemTrackerMemoryAccount::new_root_for_test(
            "ordered-runtime-filter-test-query",
        ))
        .0
    }

    pub(super) fn installed_ordered_service_with_account(
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> (Arc<RuntimeFilterService>, Arc<RuntimeOrderContract>) {
        installed_ordered_service_with_account_and_events(
            memory_account,
            Arc::new(Events::default()),
        )
    }

    fn installed_ordered_service_with_account_and_events(
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        events: Arc<Events>,
    ) -> (Arc<RuntimeFilterService>, Arc<RuntimeOrderContract>) {
        let started = Instant::now();
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(started)),
            events,
            memory_account,
        ));
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let plan = OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        };
        let contract = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
        let order_digest = contract.digest();
        let witness = CoverageWitnessId::new(1);
        let channel = RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::OrderedBound(plan),
            RuntimeFilterLifecycle::MonotonicUpdates,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::TightenOrderedBound,
            BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 1,
            },
            RuntimeFilterCoreBudget::new(4096),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(1),
                ProducerDeployment::new(witness, BTreeSet::from([uid(1)])),
            )]),
            BTreeMap::from([(
                BindingId::new(2),
                ConsumerDeployment::with_profile(
                    ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
                    },
                    BTreeSet::from([ArtifactCapability::OrderedRange]),
                    ConsumerArtifactProfile::new_ordered_range(order_digest).unwrap(),
                    BTreeSet::from([RouteEdgeId::new(1)]),
                    BTreeSet::from([uid(2)]),
                ),
            )]),
        );
        service.install(view([channel])).unwrap();
        (service, contract)
    }

    fn installed_ordered_allof_service_with_events(
        events: Arc<Events>,
    ) -> (Arc<RuntimeFilterService>, Arc<RuntimeOrderContract>) {
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            events,
            MemTrackerMemoryAccount::new_root_for_test("ordered-allof-observability-test"),
        ));
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let plan = OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        };
        let contract = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
        let order_digest = contract.digest();
        let witnesses = [CoverageWitnessId::new(1), CoverageWitnessId::new(2)];
        let coverage = Coverage::AllOf(witnesses.into_iter().map(Coverage::Leaf).collect());
        let channel = RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::OrderedBound(plan),
            RuntimeFilterLifecycle::MonotonicUpdates,
            coverage.clone(),
            coverage,
            ReductionRequirement::TightenOrderedBound,
            BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 1,
            },
            RuntimeFilterCoreBudget::new(4096),
            MaterializationPolicy::for_test(),
            BTreeMap::from([
                (
                    BindingId::new(1),
                    ProducerDeployment::new(witnesses[0], BTreeSet::from([uid(1)])),
                ),
                (
                    BindingId::new(3),
                    ProducerDeployment::new(witnesses[1], BTreeSet::from([uid(3)])),
                ),
            ]),
            BTreeMap::from([(
                BindingId::new(2),
                ConsumerDeployment::with_profile(
                    ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
                    },
                    BTreeSet::from([ArtifactCapability::OrderedRange]),
                    ConsumerArtifactProfile::new_ordered_range(order_digest).unwrap(),
                    BTreeSet::from([RouteEdgeId::new(1)]),
                    BTreeSet::from([uid(2)]),
                ),
            )]),
        );
        service.install(view([channel])).unwrap();
        (service, contract)
    }

    pub(super) fn ordered_update(
        contract: &RuntimeOrderContract,
        value: i64,
    ) -> OrderedBoundUpdate {
        OrderedBoundUpdate::new(
            contract,
            OrderedTuple::try_new(contract, [Some(OrderedScalar::Int64(value))]).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn ordered_visible_snapshot_materializes_range_without_waiting_for_terminal() {
        let events = Arc::new(Events::default());
        let (service, contract) = installed_ordered_service_with_account_and_events(
            MemTrackerMemoryAccount::new_root_for_test("ordered-range-materialization-test"),
            events.clone(),
        );
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };

        assert_eq!(
            producer
                .submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ordered_update(&contract, 40),
                )
                .unwrap(),
            SubmitOutcome::Published
        );
        assert!(events.0.lock().unwrap().iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::ArtifactMaterialized {
                kind: ArtifactKind::Range,
                ..
            }
        )));
    }

    #[test]
    fn ordered_updates_emit_applied_and_typed_rejected_events_before_return() {
        let events = Arc::new(Events::default());
        let (service, contract) = installed_ordered_allof_service_with_events(events.clone());
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };

        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            )
            .unwrap();
        let recorded = events.0.lock().unwrap().clone();
        assert!(recorded.iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::OrderedUpdateApplied { identity }
                if identity.stream().binding_id() == BindingId::new(1)
                    && identity.sequence() == ProducerSequence::new(0)
        )));
        assert!(
            !recorded
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::LogicalVersionPublished { .. }))
        );

        events.0.lock().unwrap().clear();
        let loosened = producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&contract, 101),
            )
            .unwrap_err();
        assert_eq!(
            loosened.kind(),
            RuntimeContractViolationKind::OrderedBoundLoosened
        );
        let recorded = events.0.lock().unwrap().clone();
        assert!(recorded.iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::OrderedUpdateRejected { identity, violation }
                if identity.sequence() == ProducerSequence::new(1)
                    && *violation == RuntimeContractViolationKind::OrderedBoundLoosened
        )));

        events.0.lock().unwrap().clear();
        let conflict = producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 90),
            )
            .unwrap_err();
        assert_eq!(
            conflict.kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
        let recorded = events.0.lock().unwrap().clone();
        assert!(recorded.iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::OrderedUpdateRejected { identity, violation }
                if identity.sequence() == ProducerSequence::new(0)
                    && *violation == RuntimeContractViolationKind::ConflictingReplay
        )));
    }

    #[test]
    fn ordered_authorize_rejection_emits_typed_event_before_return() {
        let events = Arc::new(Events::default());
        let account = Arc::new(ArmableRejectingMemoryAccount::default());
        let (service, contract) =
            installed_ordered_service_with_account_and_events(account.clone(), events.clone());
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };

        events.0.lock().unwrap().clear();
        let invalid_partition = producer
            .submit_bound(
                PartitionId::new(1),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            )
            .unwrap_err();
        assert_eq!(
            invalid_partition.kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert!(events.0.lock().unwrap().iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::OrderedUpdateRejected { identity, violation }
                if identity.stream().partition_id() == PartitionId::new(1)
                    && identity.sequence() == ProducerSequence::new(0)
                    && *violation == RuntimeContractViolationKind::InvalidPartition
        )));
    }

    #[test]
    fn ordered_resource_preflight_rejection_emits_typed_event_before_return() {
        let events = Arc::new(Events::default());
        let account = Arc::new(ArmableRejectingMemoryAccount::default());
        let (service, contract) =
            installed_ordered_service_with_account_and_events(account.clone(), events.clone());
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };

        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            )
            .unwrap();
        events.0.lock().unwrap().clear();
        account.armed.store(true, Ordering::SeqCst);
        let mismatched_keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }];
        let mismatched_contract = RuntimeOrderContract::try_from_plan(&OrderContract {
            comparator_digest: comparator_digest_for_test(
                &mismatched_keys,
                COMPARATOR_ALGORITHM_VERSION,
            ),
            keys: mismatched_keys,
            inclusive: true,
        })
        .unwrap();
        let contract_error = producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&mismatched_contract, 90),
            )
            .unwrap_err();
        assert_eq!(
            contract_error.kind(),
            RuntimeContractViolationKind::OrderedContractMismatch
        );
        assert!(events.0.lock().unwrap().iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::OrderedUpdateRejected { identity, violation }
                if identity.stream().partition_id() == PartitionId::new(0)
                    && identity.sequence() == ProducerSequence::new(1)
                    && *violation == RuntimeContractViolationKind::OrderedContractMismatch
        )));
    }

    #[test]
    fn ordered_rejection_waits_for_earlier_accepted_action_without_test_reservation() {
        let events = Arc::new(Events::default());
        let (service, contract) = installed_ordered_allof_service_with_events(events.clone());
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };
        let channel = service
            .registry
            .active_installation()
            .unwrap()
            .channels()
            .next()
            .unwrap()
            .1;
        events.0.lock().unwrap().clear();

        let accepted_update = ordered_update(&contract, 100);
        let accepted_bytes = accepted_update.canonical_contribution_bytes().unwrap();
        let accepted = channel
            .submit_ordered(
                BindingId::new(1),
                uid(1),
                PartitionId::new(0),
                ProducerSequence::new(0),
                accepted_update,
                TemporaryContributionLease::new(
                    MemTrackerMemoryAccount::new_root_for_test("ordered-event-order-test"),
                    accepted_bytes,
                ),
            )
            .unwrap();

        let (done_tx, done_rx) = mpsc::channel();
        let rejected_producer = producer.clone();
        let rejected_contract = contract.clone();
        let rejected = std::thread::spawn(move || {
            let error = rejected_producer
                .submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    ordered_update(&rejected_contract, 101),
                )
                .unwrap_err();
            done_tx.send(error.kind()).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while service.dispatcher.pending_action_count(ChannelId::new(1)) == 0 {
            assert!(
                Instant::now() < deadline,
                "later rejection never reached dispatcher"
            );
            std::thread::yield_now();
        }
        assert!(events.0.lock().unwrap().is_empty());

        service
            .dispatcher
            .dispatch(ChannelId::new(1), accepted)
            .unwrap();
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            RuntimeContractViolationKind::OrderedBoundLoosened
        );
        rejected.join().unwrap();
        let ordered_events = events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                RuntimeFilterEvent::OrderedUpdateApplied { identity } => {
                    Some((identity.sequence(), None))
                }
                RuntimeFilterEvent::OrderedUpdateRejected {
                    identity,
                    violation,
                } => Some((identity.sequence(), Some(*violation))),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ordered_events,
            vec![
                (ProducerSequence::new(0), None),
                (
                    ProducerSequence::new(1),
                    Some(RuntimeContractViolationKind::OrderedBoundLoosened),
                ),
            ]
        );
    }

    #[test]
    fn ordered_launch_events_follow_claim_order_while_newer_version_commits_first() {
        struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);

        impl ReleaseOnDrop {
            fn release(&self) {
                let (lock, changed) = &*self.0;
                *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
                changed.notify_all();
            }
        }

        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                self.release();
            }
        }

        let events = Arc::new(Events::default());
        let (service, contract) = installed_ordered_service_with_account_and_events(
            MemTrackerMemoryAccount::new_root_for_test("ordered-out-of-order-publish-test"),
            events.clone(),
        );
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };

        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let release_on_drop = ReleaseOnDrop(release.clone());
        let (older_claimed_tx, older_claimed_rx) = mpsc::channel();
        service.set_before_materialization_admission_hook(Arc::new({
            let release = release.clone();
            move || {
                older_claimed_tx.send(()).unwrap();
                let (lock, changed) = &*release;
                let mut released = lock.lock().unwrap_or_else(|error| error.into_inner());
                while !*released {
                    released = changed
                        .wait(released)
                        .unwrap_or_else(|error| error.into_inner());
                }
            }
        }));

        let (older_tx, older_rx) = mpsc::channel();
        let older_producer = producer.clone();
        let older_contract = contract.clone();
        let older = std::thread::spawn(move || {
            older_tx
                .send(older_producer.submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ordered_update(&older_contract, 40),
                ))
                .unwrap();
        });
        older_claimed_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("older version must advance before materialization admission");

        let (newer_tx, newer_rx) = mpsc::channel();
        let newer_producer = producer.clone();
        let newer_contract = contract.clone();
        let newer = std::thread::spawn(move || {
            newer_tx
                .send(newer_producer.submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    ordered_update(&newer_contract, 30),
                ))
                .unwrap();
        });
        assert_eq!(
            newer_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("newer version must publish before the older encode is released")
                .unwrap(),
            SubmitOutcome::Published
        );

        release_on_drop.release();
        assert_eq!(
            older_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Published
        );
        older.join().unwrap();
        newer.join().unwrap();

        let events = events.0.lock().unwrap();
        let started_versions = events
            .iter()
            .filter_map(|event| match event {
                RuntimeFilterEvent::MaterializationStarted { identity } => Some(identity.version()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            started_versions,
            [LogicalVersion::FIRST, LogicalVersion::new(2)],
            "owner launch events must remain in ordered action-claim order"
        );
        let newer_published = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    RuntimeFilterEvent::ArtifactPublished { identity, .. }
                        if identity.version() == LogicalVersion::new(2)
                )
            })
            .expect("newer version must publish");
        let older_stale = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    RuntimeFilterEvent::ArtifactPublishStaleSkipped { identity }
                        if identity.version() == LogicalVersion::FIRST
                )
            })
            .expect("older version must become stale");
        assert!(newer_published < older_stale);
    }

    #[test]
    fn claimed_owner_launch_prefix_precedes_contiguous_cancel_without_deadlock() {
        let fixture = fixture();
        let value_set = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let bitset = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        fixture
            .service
            .install(view([deployment_with_profiles([
                (30, 40, 30, value_set),
                (31, 41, 31, bitset),
            ])]))
            .unwrap();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();

        let (owner_claimed_tx, owner_claimed_rx) = mpsc::channel();
        let (claim_release_tx, claim_release_rx) = mpsc::channel();
        let claim_release_rx = Mutex::new(claim_release_rx);
        fixture
            .service
            .set_after_materialization_gate_claim_hook(Arc::new(move || {
                owner_claimed_tx.send(()).unwrap();
                claim_release_rx.lock().unwrap().recv().unwrap();
            }));

        let (complete_tx, complete_rx) = mpsc::channel();
        let completing_producer = producer.clone();
        let completing = std::thread::spawn(move || {
            complete(&completing_producer, 23);
            complete_tx.send(()).unwrap();
        });
        owner_claimed_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("completion must claim at least one materialization owner");

        let (cancel_started_tx, cancel_started_rx) = mpsc::channel();
        let (cancel_done_tx, cancel_done_rx) = mpsc::channel();
        let cancelling_dispatcher = fixture.service.dispatcher.clone();
        let cancelling = std::thread::spawn(move || {
            cancel_started_tx.send(()).unwrap();
            cancelling_dispatcher.dispatch_nonblocking(
                ChannelId::new(1),
                ChannelAction::Cancelled {
                    order: 2,
                    events: vec![RuntimeFilterEvent::ChannelCancelled {
                        identity: RuntimeFilterEventIdentity::new(
                            uid(0),
                            RuntimeFilterParticipantId::new(3),
                            ChannelId::new(1),
                            DeploymentEpoch::new(9),
                        ),
                    }],
                },
            );
            cancel_done_tx.send(()).unwrap();
        });
        cancel_started_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let cancel_completed_before_release = cancel_done_rx
            .recv_timeout(Duration::from_millis(200))
            .is_ok();
        claim_release_tx.send(()).unwrap();

        let completed_without_rescue = complete_rx.recv_timeout(Duration::from_millis(300)).is_ok();
        if !completed_without_rescue {
            let flight = fixture
                .service
                .dispatcher
                .channels
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&ChannelId::new(1))
                .cloned()
                .expect("channel dispatch flight must exist");
            fixture
                .service
                .dispatcher
                .drain_ready_nonblocking(ChannelId::new(1), flight);
            complete_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("rescue drain must release the old deadlock window");
        }
        if !cancel_completed_before_release {
            cancel_done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("cancel must finish after the launch prefix is reserved");
        }
        completing.join().unwrap();
        cancelling.join().unwrap();

        let events = fixture.events.0.lock().unwrap();
        let started = events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                matches!(event, RuntimeFilterEvent::MaterializationStarted { .. }).then_some(index)
            })
            .collect::<Vec<_>>();
        let cancelled = events
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .expect("contiguous cancel must emit its causal event");
        assert_eq!(
            started.len(),
            2,
            "both canonical profile owners must launch"
        );
        assert!(
            completed_without_rescue,
            "launch publication must not wait behind its contiguous cancel core"
        );
        assert!(started.into_iter().all(|index| index < cancelled));
    }

    fn install_one(fixture: &Fixture) {
        assert_eq!(
            fixture
                .service
                .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn membership_open_rejects_ordered_port_before_caching_handle() {
        let fixture = fixture();
        install_one(&fixture);
        let error = fixture
            .service
            .open_producer(
                BindingId::new(10),
                uid(10),
                1,
                ProducerPortKind::OrderedBound,
            )
            .unwrap_err();
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ProducerPortMismatch
        );
        assert!(matches!(
            fixture
                .service
                .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership,)
                .unwrap(),
            ProducerHandle::Membership(_)
        ));
    }

    fn open_and_subscribe(
        fixture: &Fixture,
    ) -> (
        Arc<dyn ProducerAdapter>,
        Arc<dyn BlockingSnapshotSubscription>,
    ) {
        let subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .unwrap();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        (producer, subscription)
    }

    fn complete(producer: &Arc<dyn ProducerAdapter>, value: i64) {
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([value]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            SubmitOutcome::Completed
        );
    }

    #[test]
    fn unpublished_causal_batch_blocks_later_subscription_event_until_publish() {
        let events = Arc::new(Events::default());
        let emitter = EventEmitter::new(events.clone());
        let common = RuntimeFilterEventIdentity::new(
            uid(0),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(1),
            DeploymentEpoch::new(9),
        );
        let batch = emitter.prequeue([RuntimeFilterEvent::ChannelCompleted {
            identity: common,
            version: LogicalVersion::FIRST,
        }]);
        emitter.record(RuntimeFilterEvent::SubscriptionAcquired {
            identity: ConsumerEventIdentity::new(common, BindingId::new(30), uid(30)),
            version: LogicalVersion::FIRST,
        });
        assert!(events.0.lock().unwrap().is_empty());
        emitter.publish(batch);
        assert!(matches!(
            events.0.lock().unwrap().as_slice(),
            [
                RuntimeFilterEvent::ChannelCompleted { .. },
                RuntimeFilterEvent::SubscriptionAcquired { .. }
            ]
        ));
    }

    #[test]
    fn each_pending_dispatch_order_is_drained_by_its_caller() {
        fn action(order: u64) -> ChannelAction {
            ChannelAction::Progress {
                order: Some(order),
                outcome: SubmitOutcome::Applied,
                events: vec![RuntimeFilterEvent::ChannelPlanned {
                    identity: RuntimeFilterEventIdentity::new(
                        uid(0),
                        RuntimeFilterParticipantId::new(3),
                        ChannelId::new(100 + u32::try_from(order).unwrap()),
                        DeploymentEpoch::new(9),
                    ),
                }],
            }
        }

        let fixture = fixture();
        install_one(&fixture);
        fixture.events.0.lock().unwrap().clear();
        let channel_id = ChannelId::new(1);
        let flight = fixture
            .service
            .dispatcher
            .channels
            .lock()
            .unwrap()
            .entry(channel_id)
            .or_default()
            .clone();
        {
            let mut state = flight.state.lock().unwrap();
            state.pending.insert(
                1,
                PendingDispatch {
                    action: action(1),
                    core_batch: None,
                    completion: Arc::new(EventBatchCompletion::default()),
                    needs_drainer: false,
                },
            );
            state.pending.insert(
                2,
                PendingDispatch {
                    action: action(2),
                    core_batch: None,
                    completion: Arc::new(EventBatchCompletion::default()),
                    needs_drainer: false,
                },
            );
        }
        fixture
            .service
            .dispatcher
            .dispatch_nonblocking(channel_id, action(2));
        fixture
            .service
            .dispatcher
            .dispatch(channel_id, action(0))
            .unwrap();

        let dispatcher = fixture.service.dispatcher.clone();
        let (second_tx, second_rx) = mpsc::channel();
        std::thread::spawn(move || {
            dispatcher.dispatch(channel_id, action(2)).unwrap();
            second_tx.send(()).unwrap();
        });
        let dispatcher = fixture.service.dispatcher.clone();
        let (first_tx, first_rx) = mpsc::channel();
        std::thread::spawn(move || {
            dispatcher.dispatch(channel_id, action(1)).unwrap();
            first_tx.send(()).unwrap();
        });
        first_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        second_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let state = flight.state.lock().unwrap();
        assert_eq!(state.next_order, 3);
        assert!(state.pending.is_empty());
        drop(state);
        assert!(
            fixture
                .service
                .dispatcher
                .events
                .state
                .lock()
                .unwrap()
                .batches
                .is_empty()
        );
        let channel_ids = fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|event| match event {
                RuntimeFilterEvent::ChannelPlanned { identity } => identity.channel_id().get(),
                other => panic!("unexpected event: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(channel_ids, [100, 101, 102]);
    }

    #[test]
    fn install_waits_when_another_drainer_owns_its_published_batch() {
        let (sink_entered_tx, sink_entered_rx) = mpsc::channel();
        let (sink_release_tx, sink_release_rx) = mpsc::channel();
        let sink = Arc::new(BlockingLastInstallEvent {
            entered: sink_entered_tx,
            release: Mutex::new(sink_release_rx),
            recorded: Mutex::new(Vec::new()),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("publish-completion-race"),
        ));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (publish_release_tx, publish_release_rx) = mpsc::channel();
        let publish_release_rx = Mutex::new(publish_release_rx);
        *service
            .dispatcher
            .events
            .after_publish_ready
            .lock()
            .unwrap() = Some(Arc::new(move || {
            ready_tx.send(()).unwrap();
            publish_release_rx.lock().unwrap().recv().unwrap();
        }));

        let install_view = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let install_service = service.clone();
        let (install_tx, install_rx) = mpsc::channel();
        std::thread::spawn(move || {
            install_tx
                .send(install_service.install(install_view))
                .unwrap()
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let emitter = service.dispatcher.events.clone();
        let later_identity = RuntimeFilterEventIdentity::new(
            uid(0),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(2),
            DeploymentEpoch::new(9),
        );
        let (drainer_tx, drainer_rx) = mpsc::channel();
        std::thread::spawn(move || {
            emitter.record(RuntimeFilterEvent::ChannelCancelled {
                identity: later_identity,
            });
            drainer_tx.send(()).unwrap();
        });
        sink_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        publish_release_tx.send(()).unwrap();

        let early_install = install_rx.recv_timeout(Duration::from_millis(50));
        sink_release_tx.send(()).unwrap();
        drainer_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        match early_install {
            Ok(result) => panic!("install returned before its last sink callback: {result:?}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => panic!("install result channel disconnected: {error}"),
        }
        assert_eq!(
            install_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::Installed
        );
        assert!(matches!(
            sink.recorded.lock().unwrap().as_slice(),
            [
                RuntimeFilterEvent::DeploymentInstalled { .. },
                RuntimeFilterEvent::ChannelPlanned { .. },
                RuntimeFilterEvent::ChannelCancelled { .. }
            ]
        ));
    }

    #[test]
    fn panicking_event_sink_is_contained_and_queue_keeps_draining() {
        let sink = Arc::new(PanicOnceEvents {
            panicked: AtomicBool::new(false),
            recorded: Mutex::new(Vec::new()),
        });
        let emitter = EventEmitter::new(sink.clone());
        let identity = RuntimeFilterEventIdentity::new(
            uid(0),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(1),
            DeploymentEpoch::new(9),
        );
        emitter.record_all([
            RuntimeFilterEvent::ChannelPlanned { identity },
            RuntimeFilterEvent::ChannelCancelled { identity },
        ]);
        emitter.record(RuntimeFilterEvent::ChannelPlanned { identity });
        assert_eq!(sink.recorded.lock().unwrap().len(), 2);
    }

    #[test]
    fn install_batch_is_reserved_before_installed_state_is_observable() {
        let fixture = fixture();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_after_commit_before_publish_hook(Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let service = fixture.service.clone();
        let (installed_tx, installed_rx) = mpsc::channel();
        std::thread::spawn(move || {
            installed_tx
                .send(service.install(view([deployment(1, 10, 30, 40, [10], [30], 100)])))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        let (submit_tx, submit_rx) = mpsc::channel();
        std::thread::spawn(move || {
            submit_tx
                .send(producer.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([1]), false),
                ))
                .unwrap();
        });
        assert!(submit_rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(fixture.events.0.lock().unwrap().is_empty());
        release_tx.send(()).unwrap();
        assert_eq!(
            installed_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::Installed
        );
        assert_eq!(
            submit_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Applied
        );
        let events = fixture.events.0.lock().unwrap();
        assert!(matches!(
            events[0],
            RuntimeFilterEvent::DeploymentInstalled { .. }
        ));
        assert!(matches!(
            events[1],
            RuntimeFilterEvent::ChannelPlanned { .. }
        ));
        assert!(matches!(
            events[2],
            RuntimeFilterEvent::DeltaAccepted { .. }
        ));
    }

    #[test]
    fn installing_exposes_neither_core_nor_role_router() {
        let fixture = fixture();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_before_commit_clock_hook(Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let service = fixture.service.clone();
        let (install_tx, install_rx) = mpsc::channel();
        std::thread::spawn(move || {
            install_tx
                .send(service.install(view([deployment(1, 10, 30, 40, [10], [30], 100)])))
                .unwrap();
        });

        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(fixture.service.registry.active_installation().is_none());
        assert!(
            fixture
                .service
                .registry
                .channel(ChannelId::new(1))
                .is_none()
        );

        release_tx.send(()).unwrap();
        assert_eq!(
            install_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn logical_commit_exposes_core_and_role_router_together_before_event_publish_returns() {
        let fixture = fixture();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_after_commit_before_publish_hook(Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let service = fixture.service.clone();
        let (install_tx, install_rx) = mpsc::channel();
        std::thread::spawn(move || {
            install_tx
                .send(service.install(view([deployment(1, 10, 30, 40, [10], [30], 100)])))
                .unwrap();
        });

        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let installed = fixture
            .service
            .registry
            .active_installation()
            .expect("logical commit exposes the installed snapshot");
        assert_eq!(installed.channels().count(), 1);
        let _role_router = installed.role_router();
        assert!(install_rx.recv_timeout(Duration::from_millis(50)).is_err());

        release_tx.send(()).unwrap();
        assert_eq!(
            install_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn candidate_failure_leaves_no_active_core_or_role_router() {
        let service = RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(NearMaxClock),
            Arc::new(Events::default()),
            MemTrackerMemoryAccount::new_root_for_test("failed-install-candidate"),
        );

        assert!(
            service
                .install(view([deployment(1, 10, 30, 40, [10], [30], 1000)]))
                .is_err()
        );
        assert!(service.registry.active_installation().is_none());
        assert!(service.registry.channel(ChannelId::new(1)).is_none());
    }

    #[test]
    fn cancel_during_install_leaves_no_half_installed_role_router() {
        let fixture = fixture();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_before_commit_clock_hook(Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let service = fixture.service.clone();
        let (install_tx, install_rx) = mpsc::channel();
        std::thread::spawn(move || {
            install_tx
                .send(service.install(view([deployment(1, 10, 30, 40, [10], [30], 100)])))
                .unwrap();
        });

        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        fixture.service.cancel();
        assert!(fixture.service.registry.active_installation().is_none());
        assert!(
            fixture
                .service
                .registry
                .channel(ChannelId::new(1))
                .is_none()
        );

        release_tx.send(()).unwrap();
        assert!(
            install_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .is_err()
        );
        assert!(fixture.service.registry.active_installation().is_none());
    }

    #[test]
    fn installed_role_router_authorizes_compiler_projected_remote_producer() {
        let (install, producer_binding, remote_producer) =
            compiled_three_backend_all_of_aggregator_install();
        let channel_id = ChannelId::new(5);
        let epoch = install.core_view().epoch();
        let service = RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            Arc::new(Events::default()),
            MemTrackerMemoryAccount::new_root_for_test("installed-role-router"),
        );

        assert_eq!(service.install(install).unwrap(), InstallOutcome::Installed);
        let installed = service.registry.active_installation().unwrap();
        assert!(
            installed
                .role_router()
                .authorize_contribution(
                    epoch,
                    channel_id,
                    producer_binding,
                    remote_producer,
                    RuntimeFilterEnvelopeKind::Contribution,
                )
                .is_ok()
        );
    }

    #[test]
    fn compiler_participant_installs_all_succeed() {
        let plan = super::test_support::compiled_three_backend_all_of_plan();
        let installs = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .expect("compiler projections pair into participant installs");

        assert_eq!(installs.len(), plan.install_views.len());
        for (participant, install) in installs {
            let service = RuntimeFilterService::new_with_dependencies(
                uid(0),
                Arc::new(Clock(Instant::now())),
                Arc::new(Events::default()),
                MemTrackerMemoryAccount::new_root_for_test("compiler-participant-install"),
            );
            service.install(install).unwrap_or_else(|error| {
                panic!(
                    "compiler participant {} install must succeed: {:?}",
                    participant.get(),
                    error.kind()
                )
            });
        }
    }

    fn install_with_extra_route_kind(
        install: RuntimeFilterParticipantInstall,
        route_edge_id: RouteEdgeId,
        extra: RuntimeFilterEnvelopeKind,
    ) -> RuntimeFilterParticipantInstall {
        let (core, shard) = install.into_parts();
        let channels = shard
            .channels()
            .iter()
            .map(|(channel_id, channel)| {
                let mutate = |edge: &RuntimeFilterRoutingEdgeView| {
                    let mut allowed = edge.allowed_kinds().clone();
                    if edge.route_edge_id() == route_edge_id {
                        allowed.insert(extra);
                    }
                    RuntimeFilterRoutingEdgeView::new(
                        *channel_id,
                        edge.route_edge_id(),
                        edge.source().clone(),
                        edge.target().clone(),
                        edge.peer().clone(),
                        allowed,
                    )
                    .unwrap()
                };
                (
                    *channel_id,
                    RuntimeFilterChannelRoutingView::new(
                        *channel_id,
                        channel.local_roles().clone(),
                        channel.producer_instances().clone(),
                        channel.inbound_edges().iter().map(&mutate).collect(),
                        channel.outbound_edges().iter().map(&mutate).collect(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        RuntimeFilterParticipantInstall::new(
            core,
            RuntimeFilterRoutingShard::new(
                shard.deployment_epoch(),
                shard.local_participant_id(),
                channels,
            )
            .unwrap(),
        )
    }

    #[test]
    fn shared_install_validator_rejects_cross_family_extra_allowed_kinds() {
        let direct = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let direct_route = direct.routing_shard().channels()[&ChannelId::new(1)].outbound_edges()
            [0]
        .route_edge_id();
        let direct = install_with_extra_route_kind(
            direct,
            direct_route,
            RuntimeFilterEnvelopeKind::Contribution,
        );
        crate::runtime_filter::deployment::install_validation::validate_participant_install(
            &direct,
        )
        .expect_err("direct delivery rejects contribution-family authority");

        let plan = super::test_support::compiled_three_backend_all_of_plan();
        for (_, install) in RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .expect("compiler projections pair")
        {
            let edges = install
                .routing_shard()
                .channels()
                .values()
                .flat_map(|channel| {
                    channel
                        .inbound_edges()
                        .iter()
                        .chain(channel.outbound_edges())
                })
                .map(|edge| {
                    let extra = if edge
                        .allowed_kinds()
                        .contains(&RuntimeFilterEnvelopeKind::Artifact)
                    {
                        RuntimeFilterEnvelopeKind::Contribution
                    } else {
                        RuntimeFilterEnvelopeKind::Artifact
                    };
                    (edge.route_edge_id(), extra)
                })
                .collect::<BTreeSet<_>>();
            for (route_edge_id, extra) in edges {
                let mutated = install_with_extra_route_kind(install.clone(), route_edge_id, extra);
                crate::runtime_filter::deployment::install_validation::validate_participant_install(
                    &mutated,
                )
                .expect_err("To/FromAggregator edges reject cross-family authority");
            }
        }
    }

    #[test]
    fn deadline_is_anchored_after_delayed_build_and_idempotent_install_does_not_reset_it() {
        let events = Arc::new(Events::default());
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            events,
            MemTrackerMemoryAccount::new_root_for_test("delayed-build-clock"),
        ));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        service.set_before_commit_clock_hook(Arc::new(move || {
            ready_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }));
        let install = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let install_thread = service.clone();
        let first_view = install.clone();
        let handle = std::thread::spawn(move || install_thread.install(first_view));
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let released_at = Instant::now();
        release_tx.send(()).unwrap();
        assert_eq!(handle.join().unwrap().unwrap(), InstallOutcome::Installed);
        let deadline = service.registry.deadline(ChannelId::new(1)).unwrap();
        assert!(deadline >= released_at + Duration::from_millis(90));
        assert_eq!(
            service.install(install).unwrap(),
            InstallOutcome::AlreadyInstalled
        );
        assert_eq!(service.registry.deadline(ChannelId::new(1)), Some(deadline));
    }

    #[test]
    fn install_clock_may_reenter_service_reads_without_deadlock() {
        let events = Arc::new(Events::default());
        let clock = Arc::new(ReentrantServiceClock {
            service: Mutex::new(Weak::new()),
            now: Instant::now(),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            clock.clone(),
            events,
            MemTrackerMemoryAccount::new_root_for_test("reentrant-service-clock"),
        ));
        *clock.service.lock().unwrap() = Arc::downgrade(&service);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            tx.send(service.install(view([deployment(1, 10, 30, 40, [10], [30], 100)])))
                .unwrap();
        });
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1))
                .expect("reentrant clock deadlocked service install")
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn loopback_never_bypasses_complete_once_publish_gate() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::TimedOut
        ));
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false)
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::TimedOut
        ));
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            SubmitOutcome::Completed
        );
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Published(_)
        ));
        let events = fixture.events.0.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. }))
                .count(),
            1
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::SubscriptionCancelled { .. }))
        );
    }

    #[test]
    fn loopback_delivers_completed_logical_snapshot_through_real_ports() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        complete(&producer, 7);
        let ArtifactAcquireOutcome::Published(snapshot) = subscription.acquire(Duration::ZERO)
        else {
            panic!("expected completed snapshot");
        };
        assert_eq!(snapshot.channel_id(), ChannelId::new(1));
        assert_eq!(snapshot.version(), LogicalVersion::FIRST);
        assert_eq!(snapshot.artifacts().len(), 1);
    }

    #[test]
    fn codec_hop_leaf_fixture_matches_direct_bundle_through_real_router_subscriptions() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, source_subscription) = open_and_subscribe(&fixture);
        complete(&producer, 37);
        let ArtifactAcquireOutcome::Published(direct_bundle) =
            source_subscription.acquire(Duration::ZERO)
        else {
            panic!("direct materialization must publish");
        };
        let installed = fixture.service.registry.active_installation().unwrap();
        let plan = installed.artifact_plan(ChannelId::new(1)).unwrap();
        let profile = plan.groups()[0].profile().clone();
        let max_artifact_bytes = plan.max_artifact_bytes();
        let retained_budget = plan.retained_budget();
        let direct_budget_baseline = retained_budget.retained_bytes();
        let direct_account_baseline = fixture.tracker.current();
        assert_eq!(
            direct_budget_baseline,
            direct_bundle.retained_memory_bytes()
        );
        drop(installed);
        let (kind, direct_artifact) = &direct_bundle.artifacts()[0];
        let decoded_artifact = decode_leaf(
            direct_artifact.canonical_bytes(),
            ArtifactDecodeExpectations {
                expected_kind: *kind,
                expected_schema_digest: direct_artifact.schema_digest(),
                expected_logical_version: direct_bundle.version(),
                expected_hash_contract: profile.bloom_hash_contract(),
            },
            max_artifact_bytes,
            retained_budget.clone(),
            fixture.tracker.clone(),
        )
        .unwrap();
        let decoded_leaf_retained = decoded_artifact.retained_memory_bytes();
        assert_eq!(
            retained_budget.retained_bytes(),
            direct_budget_baseline + decoded_leaf_retained
        );
        assert_eq!(
            fixture.tracker.current(),
            direct_account_baseline + i64::try_from(decoded_leaf_retained).unwrap()
        );
        let decoded_bundle = Arc::new(
            ArtifactBundle::new(
                direct_bundle.channel_id(),
                direct_bundle.version(),
                &profile,
                vec![(*kind, decoded_artifact)],
                max_artifact_bytes,
            )
            .unwrap(),
        );

        let common = RuntimeFilterEventIdentity::new(
            uid(0),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(1),
            DeploymentEpoch::new(9),
        );
        let direct_events = Arc::new(Events::default());
        let direct_group = Arc::new(SubscriptionGroup::new(
            common,
            BindingId::new(50),
            ConsumerActivation::BlockingSnapshot,
            [RouteEdgeId::new(70)],
            [uid(50)],
            direct_events,
        ));
        let direct_slot = direct_group
            .handle(uid(50), SubscriptionKind::BlockingSnapshot)
            .unwrap()
            .into_blocking()
            .unwrap();
        let direct_router = LoopbackRouter::new(BTreeMap::from([(
            RouteEdgeId::new(70),
            direct_group as Arc<dyn ArtifactDelivery>,
        )]));
        direct_router.route(
            &[RouteEdgeId::new(70)],
            &ArtifactDeliveryOutcome::Published(direct_bundle.clone()),
        );

        let codec_events = Arc::new(Events::default());
        let codec_group = Arc::new(SubscriptionGroup::new(
            common,
            BindingId::new(51),
            ConsumerActivation::BlockingSnapshot,
            [RouteEdgeId::new(71)],
            [uid(51)],
            codec_events,
        ));
        let codec_slot = codec_group
            .handle(uid(51), SubscriptionKind::BlockingSnapshot)
            .unwrap()
            .into_blocking()
            .unwrap();
        let codec_router = LoopbackRouter::new(BTreeMap::from([(
            RouteEdgeId::new(71),
            codec_group as Arc<dyn ArtifactDelivery>,
        )]));
        codec_router.route(
            &[RouteEdgeId::new(71)],
            &ArtifactDeliveryOutcome::Published(decoded_bundle.clone()),
        );

        let (
            ArtifactAcquireOutcome::Published(direct_acquired),
            ArtifactAcquireOutcome::Published(codec_acquired),
        ) = (
            direct_slot.acquire(Duration::ZERO),
            codec_slot.acquire(Duration::ZERO),
        )
        else {
            panic!("both leaf fixtures must deliver a published bundle");
        };
        assert_eq!(direct_acquired.channel_id(), codec_acquired.channel_id());
        assert_eq!(direct_acquired.version(), codec_acquired.version());
        assert_eq!(direct_acquired.profile_id(), codec_acquired.profile_id());
        assert_eq!(
            direct_acquired.artifacts()[0].0,
            codec_acquired.artifacts()[0].0
        );
        assert_eq!(
            direct_acquired.canonical_digest(),
            codec_acquired.canonical_digest()
        );
        assert_eq!(
            direct_acquired.artifacts()[0].1.canonical_digest(),
            codec_acquired.artifacts()[0].1.canonical_digest()
        );
        drop(codec_acquired);
        drop(codec_slot);
        drop(codec_router);
        drop(decoded_bundle);
        assert_eq!(retained_budget.retained_bytes(), direct_budget_baseline);
        assert_eq!(fixture.tracker.current(), direct_account_baseline);
    }

    #[test]
    fn same_profile_consumers_share_one_bundle_arc() {
        let fixture = fixture();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        fixture
            .service
            .install(view([deployment_with_profiles([
                (30, 40, 30, profile.clone()),
                (31, 41, 31, profile),
            ])]))
            .unwrap();
        let first = fixture
            .service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .unwrap();
        let second = fixture
            .service
            .subscribe_blocking(BindingId::new(31), uid(31))
            .unwrap();
        let installed = fixture.service.registry.active_installation().unwrap();
        let plan = installed.artifact_plan(ChannelId::new(1)).unwrap();
        assert_eq!(plan.groups().len(), 1);
        assert_eq!(plan.groups()[0].route_edges().len(), 2);
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        complete(&producer, 11);
        let (ArtifactAcquireOutcome::Published(first), ArtifactAcquireOutcome::Published(second)) = (
            first.acquire(Duration::ZERO),
            second.acquire(Duration::ZERO),
        ) else {
            panic!("expected both subscriptions completed");
        };
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.version(), LogicalVersion::FIRST);
    }

    #[test]
    fn concurrent_real_materialization_jobs_single_flight_one_encode_and_share_terminal_arc() {
        struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);

        impl ReleaseOnDrop {
            fn release(&self) {
                let (lock, changed) = &*self.0;
                *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                changed.notify_all();
            }
        }

        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                self.release();
            }
        }

        let fixture = fixture();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        fixture
            .service
            .install(view([deployment_with_profiles([(30, 40, 30, profile)])]))
            .unwrap();
        let installed = fixture.service.registry.active_installation().unwrap();
        let plan = installed.artifact_plan(ChannelId::new(1)).unwrap();
        let key = plan.groups()[0].key();
        let snapshot = Arc::new(LogicalSnapshot::first(
            ChannelId::new(1),
            ReducedMembershipDomain::new(MembershipValues::int64([41]), false),
            Default::default(),
        ));
        let start = Arc::new((Mutex::new(false), Condvar::new()));
        let encode_release = Arc::new((Mutex::new(false), Condvar::new()));
        let start_release = ReleaseOnDrop(start.clone());
        let encode_release_on_drop = ReleaseOnDrop(encode_release.clone());
        let encode_count = Arc::new(AtomicUsize::new(0));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (encode_entered_tx, encode_entered_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let before_encode: Arc<dyn Fn(ConsumerProfileId) + Send + Sync> = Arc::new({
            let encode_release = encode_release.clone();
            let encode_count = encode_count.clone();
            move |_| {
                encode_count.fetch_add(1, Ordering::SeqCst);
                encode_entered_tx.send(()).unwrap();
                let (lock, changed) = &*encode_release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = changed.wait(released).unwrap();
                }
            }
        });
        let threads = (0..2)
            .map(|_| {
                let installed = installed.clone();
                let snapshot = snapshot.clone();
                let account = fixture.tracker.clone();
                let start = start.clone();
                let ready_tx = ready_tx.clone();
                let result_tx = result_tx.clone();
                let before_encode = before_encode.clone();
                std::thread::spawn(move || {
                    ready_tx.send(()).unwrap();
                    let (lock, changed) = &*start;
                    let mut started = lock.lock().unwrap();
                    while !*started {
                        started = changed.wait(started).unwrap();
                    }
                    drop(started);
                    let plan = installed.artifact_plan(ChannelId::new(1)).unwrap();
                    let mut results = run_materialization_jobs(
                        plan,
                        installed.publish_gate(),
                        &snapshot,
                        account,
                        Some(before_encode),
                        None,
                    );
                    assert_eq!(results.len(), 1);
                    let work = results.pop().unwrap();
                    let started_events = work
                        .events
                        .iter()
                        .filter(|event| {
                            matches!(event, RuntimeFilterEvent::MaterializationStarted { .. })
                        })
                        .count();
                    let outcome = work.outcome.expect("same-key job must be terminal");
                    let owner = match work.claim {
                        MaterializationWorkClaim::Owner(owner) => {
                            assert_eq!(
                                owner.finish(outcome.clone()).unwrap(),
                                super::PublishCommitOutcome::Published
                            );
                            true
                        }
                        MaterializationWorkClaim::Follower => false,
                        MaterializationWorkClaim::Stale => {
                            panic!("same-version concurrent job cannot be stale")
                        }
                    };
                    result_tx.send((owner, outcome, started_events)).unwrap();
                })
            })
            .collect::<Vec<_>>();
        for _ in 0..2 {
            ready_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("both real job invocations must reach the start gate");
        }
        start_release.release();
        encode_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("one owner must enter the real encoder");
        let deadline = Instant::now() + Duration::from_secs(5);
        while installed
            .publish_gate()
            .in_flight_follower_count(key, LogicalVersion::FIRST)
            != 1
        {
            assert!(
                Instant::now() < deadline,
                "second real job invocation must claim the in-flight key as follower"
            );
            std::thread::yield_now();
        }
        encode_release_on_drop.release();
        let results = (0..2)
            .map(|_| {
                result_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("both real materialization invocations must finish")
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(encode_count.load(Ordering::SeqCst), 1);
        assert_eq!(results.iter().filter(|(owner, _, _)| *owner).count(), 1);
        assert_eq!(
            results
                .iter()
                .map(|(_, _, started_events)| *started_events)
                .sum::<usize>(),
            1
        );
        let published = results
            .iter()
            .map(|(_, outcome, _)| match outcome {
                ArtifactDeliveryOutcome::Published(bundle) => bundle,
                _ => panic!("both real jobs must observe Published"),
            })
            .collect::<Vec<_>>();
        assert!(Arc::ptr_eq(published[0], published[1]));
    }

    #[test]
    fn different_profiles_publish_same_logical_version_independently_in_canonical_order() {
        let fixture = fixture();
        let value_set = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let bitset = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        fixture
            .service
            .install(view([deployment_with_profiles([
                (30, 40, 30, value_set.clone()),
                (31, 41, 31, bitset.clone()),
            ])]))
            .unwrap();
        let value_set_subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .unwrap();
        let bitset_subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(31), uid(31))
            .unwrap();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        complete(&producer, 11);

        let ArtifactAcquireOutcome::Published(value_set_bundle) =
            value_set_subscription.acquire(Duration::ZERO)
        else {
            panic!("value-set profile must publish");
        };
        let ArtifactAcquireOutcome::Published(bitset_bundle) =
            bitset_subscription.acquire(Duration::ZERO)
        else {
            panic!("bitset profile must publish");
        };
        assert!(!Arc::ptr_eq(&value_set_bundle, &bitset_bundle));
        assert_eq!(value_set_bundle.version(), LogicalVersion::FIRST);
        assert_eq!(bitset_bundle.version(), LogicalVersion::FIRST);
        assert_eq!(value_set_bundle.profile_id(), value_set.id());
        assert_eq!(bitset_bundle.profile_id(), bitset.id());

        let events = fixture.events.0.lock().unwrap();
        let started_profiles = events
            .iter()
            .filter_map(|event| match event {
                RuntimeFilterEvent::MaterializationStarted { identity } => {
                    Some(identity.profile_id())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let completed_profiles = events
            .iter()
            .filter_map(|event| match event {
                RuntimeFilterEvent::ArtifactMaterialized { identity, .. }
                | RuntimeFilterEvent::ArtifactPublished { identity, .. } => {
                    Some(identity.profile_id())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut expected_profiles = vec![value_set.id(), bitset.id()];
        expected_profiles.sort_unstable();
        assert_eq!(started_profiles, expected_profiles);
        assert_eq!(
            completed_profiles,
            expected_profiles
                .into_iter()
                .flat_map(|profile| [profile; 2])
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn distinct_profiles_encode_concurrently_after_canonical_admission() {
        let fixture = fixture();
        let value_set = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let bitset = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        fixture
            .service
            .install(view([deployment_with_profiles([
                (30, 40, 30, value_set),
                (31, 41, 31, bitset),
            ])]))
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        fixture.service.set_before_encode_hook(Arc::new({
            let barrier = barrier.clone();
            let active = active.clone();
            let peak = peak.clone();
            move |_| {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                barrier.wait();
                active.fetch_sub(1, Ordering::SeqCst);
            }
        }));
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        complete(&producer, 19);
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn three_profiles_respect_job_bound_and_commit_canonically_after_reverse_completion() {
        let fixture = fixture();
        let profiles = [
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
                None,
            )
            .unwrap(),
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
                None,
            )
            .unwrap(),
            ConsumerArtifactProfile::new(
                BTreeSet::from([
                    ArtifactKind::ValueSet,
                    ArtifactKind::Bitset,
                    ArtifactKind::EmptyDomain,
                ]),
                None,
            )
            .unwrap(),
        ];
        fixture
            .service
            .install(view([deployment_with_profiles_and_concurrency(
                [
                    (30, 40, 30, profiles[0].clone()),
                    (31, 41, 31, profiles[1].clone()),
                    (32, 42, 32, profiles[2].clone()),
                ],
                2,
            )]))
            .unwrap();
        let installed = fixture.service.registry.active_installation().unwrap();
        let canonical = installed
            .artifact_plan(ChannelId::new(1))
            .unwrap()
            .groups()
            .iter()
            .map(|group| group.profile().id())
            .collect::<Vec<_>>();
        assert_eq!(canonical.len(), 3);

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Mutex::new(Vec::new()));
        let completed = Arc::new(Mutex::new(Vec::new()));
        let (second_started_tx, second_started_rx) = mpsc::channel();
        let second_started_rx = Arc::new(Mutex::new(second_started_rx));
        let (second_completed_tx, second_completed_rx) = mpsc::channel();
        let second_completed_rx = Arc::new(Mutex::new(second_completed_rx));
        let (first_completed_tx, first_completed_rx) = mpsc::channel();
        let first_completed_rx = Arc::new(Mutex::new(first_completed_rx));
        fixture.service.set_before_encode_hook(Arc::new({
            let active = active.clone();
            let peak = peak.clone();
            let started = started.clone();
            let canonical = canonical.clone();
            move |profile_id| {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                started.lock().unwrap().push(profile_id);
                if profile_id == canonical[1] {
                    second_started_tx.send(()).unwrap();
                } else if profile_id == canonical[0] {
                    second_started_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .expect("the second bounded worker must start");
                }
            }
        }));
        fixture.service.set_after_encode_hook(Arc::new({
            let active = active.clone();
            let completed = completed.clone();
            let canonical = canonical.clone();
            move |profile_id| {
                if profile_id == canonical[1] {
                    completed.lock().unwrap().push(profile_id);
                    second_completed_tx.send(()).unwrap();
                    first_completed_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .expect("the first worker must acknowledge reverse completion");
                } else if profile_id == canonical[0] {
                    second_completed_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .expect("the second worker must complete first");
                    completed.lock().unwrap().push(profile_id);
                    first_completed_tx.send(()).unwrap();
                } else {
                    completed.lock().unwrap().push(profile_id);
                }
                active.fetch_sub(1, Ordering::SeqCst);
            }
        }));

        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        complete(&producer, 19);
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        let starts = started.lock().unwrap().clone();
        assert_eq!(starts.len(), 3);
        assert_eq!(starts[2], canonical[2]);
        assert_eq!(
            BTreeSet::from([starts[0], starts[1]]),
            BTreeSet::from([canonical[0], canonical[1]])
        );
        assert_eq!(
            *completed.lock().unwrap(),
            vec![canonical[1], canonical[0], canonical[2]]
        );
        let events = fixture.events.0.lock().unwrap();
        let started_profiles = events
            .iter()
            .filter_map(|event| match event {
                RuntimeFilterEvent::MaterializationStarted { identity } => {
                    Some(identity.profile_id())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let completed_profiles = events
            .iter()
            .filter_map(|event| match event {
                RuntimeFilterEvent::ArtifactMaterialized { identity, .. }
                | RuntimeFilterEvent::ArtifactPublished { identity, .. } => {
                    Some(identity.profile_id())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(started_profiles, canonical);
        assert_eq!(
            completed_profiles,
            canonical
                .iter()
                .flat_map(|profile_id| [*profile_id; 2])
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn accepted_publish_before_cancel_remains_the_only_route_winner() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([21]), false),
            )
            .unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_after_owner_finish_hook(Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let (closed_tx, closed_rx) = mpsc::channel();
        std::thread::spawn(move || {
            closed_tx
                .send(producer.close_partition(PartitionId::new(0), ProducerSequence::new(1)))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        fixture.service.cancel();
        release_tx.send(()).unwrap();
        assert_eq!(
            closed_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Completed
        );
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Published(_)
        ));
        let events = fixture.events.0.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. }))
                .count(),
            1
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::SubscriptionCancelled { .. }))
        );
    }

    #[test]
    fn cancel_before_owner_finish_routes_only_cancelled() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([22]), false),
            )
            .unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_before_owner_finish_hook(Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let (closed_tx, closed_rx) = mpsc::channel();
        std::thread::spawn(move || {
            closed_tx
                .send(producer.close_partition(PartitionId::new(0), ProducerSequence::new(1)))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        fixture.service.cancel();
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Cancelled
        ));
        release_tx.send(()).unwrap();
        assert_eq!(
            closed_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Completed
        );
        let events = fixture.events.0.lock().unwrap();
        assert!(!events.iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::ArtifactPublished { .. }
                | RuntimeFilterEvent::LoopbackDelivered { .. }
        )));
    }

    #[test]
    fn shutdown_during_scoped_encode_invalidates_jobs_and_drop_leaves_no_orphan() {
        struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);

        impl ReleaseOnDrop {
            fn release(&self) {
                let (lock, changed) = &*self.0;
                *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                changed.notify_all();
            }
        }

        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                self.release();
            }
        }

        let Fixture {
            service,
            events,
            tracker,
            ..
        } = fixture();
        let value_set = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let bitset = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        service
            .install(view([deployment_with_profiles([
                (30, 40, 30, value_set.clone()),
                (31, 41, 31, bitset.clone()),
            ])]))
            .unwrap();
        let subscriptions = [
            service
                .subscribe_blocking(BindingId::new(30), uid(30))
                .unwrap(),
            service
                .subscribe_blocking(BindingId::new(31), uid(31))
                .unwrap(),
        ];
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([25]), false),
            )
            .unwrap();

        let profile_binding = BTreeMap::from([
            (value_set.id(), (BindingId::new(30), uid(30))),
            (bitset.id(), (BindingId::new(31), uid(31))),
        ]);
        let weak_service = Arc::downgrade(&service);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let release_on_drop = ReleaseOnDrop(release.clone());
        let active = Arc::new(AtomicUsize::new(0));
        let reentered = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = mpsc::channel();
        service.set_before_encode_hook(Arc::new({
            let weak_service = weak_service.clone();
            let release = release.clone();
            let active = active.clone();
            let reentered = reentered.clone();
            move |profile_id| {
                active.fetch_add(1, Ordering::SeqCst);
                let service = weak_service
                    .upgrade()
                    .expect("service is live before shutdown");
                let (binding_id, instance_id) = profile_binding[&profile_id];
                service.subscribe_blocking(binding_id, instance_id).unwrap();
                reentered.fetch_add(1, Ordering::SeqCst);
                drop(service);
                entered_tx.send(()).unwrap();
                let (lock, changed) = &*release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = changed.wait(released).unwrap();
                }
            }
        }));
        service.set_after_encode_hook(Arc::new({
            let active = active.clone();
            move |_| {
                active.fetch_sub(1, Ordering::SeqCst);
            }
        }));

        let (closed_tx, closed_rx) = mpsc::channel();
        let close_thread = std::thread::spawn(move || {
            closed_tx
                .send(producer.close_partition(PartitionId::new(0), ProducerSequence::new(1)))
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 2);
        assert_eq!(reentered.load(Ordering::SeqCst), 2);
        assert!(tracker.current() > 0);

        service.shutdown();
        for subscription in &subscriptions {
            assert!(matches!(
                subscription.acquire(Duration::ZERO),
                ArtifactAcquireOutcome::Cancelled
            ));
        }
        let service_dropped = Arc::downgrade(&service);
        let dispatcher_dropped = Arc::downgrade(&service.dispatcher);
        let registry_dropped = Arc::downgrade(&service.registry);
        drop(service);
        assert!(service_dropped.upgrade().is_none());
        assert_eq!(active.load(Ordering::SeqCst), 2);

        release_on_drop.release();
        assert_eq!(
            closed_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Completed
        );
        close_thread.join().unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(dispatcher_dropped.upgrade().is_none());
        assert!(registry_dropped.upgrade().is_none());
        assert!(!events.0.lock().unwrap().iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::ArtifactPublished { .. }
                | RuntimeFilterEvent::LoopbackDelivered { .. }
        )));
        drop(subscriptions);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn conflicting_gate_publish_is_returned_to_the_completing_producer() {
        let fixture = fixture();
        install_one(&fixture);
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([23]), false),
            )
            .unwrap();
        let installed = fixture.service.registry.active_installation().unwrap();
        let plan = installed.artifact_plan(ChannelId::new(1)).unwrap();
        let group = &plan.groups()[0];
        let artifact = Arc::new(PhysicalArtifact::new_test(
            ArtifactKind::ValueSet,
            plan.schema()
                .expect("membership artifact plan has schema")
                .digest(),
            LogicalVersion::FIRST,
            false,
            Arc::from([99]),
        ));
        let conflicting = Arc::new(
            ArtifactBundle::new(
                ChannelId::new(1),
                LogicalVersion::FIRST,
                group.profile(),
                vec![(ArtifactKind::ValueSet, artifact)],
                usize::MAX,
            )
            .unwrap(),
        );
        let gate = installed.publish_gate().clone();
        let key = group.key();
        let generation = gate.generation(key);
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_before_owner_finish_hook(Arc::new(move || {
                gate.commit_published(key, generation, conflicting.clone())
                    .unwrap();
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let (owner_tx, owner_rx) = mpsc::channel();
        std::thread::spawn(move || {
            owner_tx
                .send(producer.close_partition(PartitionId::new(0), ProducerSequence::new(1)))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let channel = fixture.service.registry.channel(ChannelId::new(1)).unwrap();
        let mut waiters = Vec::new();
        for _ in 0..2 {
            let dispatcher = fixture.service.dispatcher.clone();
            let action = channel.terminal_action();
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                tx.send(dispatcher.dispatch(ChannelId::new(1), action))
                    .unwrap();
            });
            waiters.push(rx);
        }
        release_tx.send(()).unwrap();
        let owner = owner_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            owner.unwrap_err().kind(),
            RuntimeContractViolationKind::ConflictingArtifactPublish
        );
        for waiter in waiters {
            assert_eq!(
                waiter
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .unwrap_err()
                    .kind(),
                RuntimeContractViolationKind::ConflictingArtifactPublish
            );
        }
        assert_eq!(
            fixture
                .service
                .dispatcher_pending_action_count(ChannelId::new(1)),
            0
        );
        assert!(
            fixture
                .events
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::ChannelCompleted { .. }))
        );
    }

    #[test]
    fn repeated_completed_action_does_not_redeliver_an_idempotent_gate_outcome() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        complete(&producer, 17);
        let ArtifactAcquireOutcome::Published(first) = subscription.acquire(Duration::ZERO) else {
            panic!("first completion must publish");
        };
        let channel = fixture.service.registry.channel(ChannelId::new(1)).unwrap();
        let repeated = channel.terminal_action();
        let claimed = fixture
            .service
            .dispatcher
            .claim_action_materialization(ChannelId::new(1), &repeated);
        let (batch, error) =
            fixture
                .service
                .dispatcher
                .route_and_prequeue(ChannelId::new(1), &repeated, claimed);
        assert!(error.is_none());
        fixture.service.dispatcher.events.publish(batch);
        let ArtifactAcquireOutcome::Published(second) = subscription.acquire(Duration::ZERO) else {
            panic!("published subscription remains terminal");
        };
        assert!(Arc::ptr_eq(&first, &second));
        let events = fixture.events.0.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn materialization_panic_finishes_owner_through_gate_and_routes_one_unavailable() {
        let account = Arc::new(PanicWhenArmedMemoryAccount::default());
        let events = Arc::new(Events::default());
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            events.clone(),
            account.clone(),
        ));
        service
            .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
            .unwrap();
        let subscription = service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .unwrap();
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([23]), false),
            )
            .unwrap();
        account.armed.store(true, Ordering::SeqCst);
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            SubmitOutcome::Completed
        );
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Unavailable(UnavailableReason::MaterializationFailed)
        ));
        assert_eq!(
            events
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn profile_local_memory_rejection_preserves_sibling_and_releases_every_failed_reservation() {
        let account = Arc::new(RejectSecondWhenArmedMemoryAccount::default());
        let events = Arc::new(Events::default());
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            events.clone(),
            account.clone(),
        ));
        let value_set = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let bitset = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        service
            .install(view([deployment_with_profiles([
                (30, 40, 30, value_set),
                (31, 41, 31, bitset),
            ])]))
            .unwrap();
        let subscriptions = [
            service
                .subscribe_blocking(BindingId::new(30), uid(30))
                .unwrap(),
            service
                .subscribe_blocking(BindingId::new(31), uid(31))
                .unwrap(),
        ];
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([29]), false),
            )
            .unwrap();
        let installed = service.registry.active_installation().unwrap();
        let plan = installed.artifact_plan(ChannelId::new(1)).unwrap();
        let retained_budget = plan.retained_budget();
        let scratch_budget = plan.scratch_budget();
        drop(installed);
        account.armed.store(true, Ordering::SeqCst);

        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            SubmitOutcome::Completed
        );
        let outcomes = subscriptions
            .iter()
            .map(|subscription| subscription.acquire(Duration::ZERO))
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    ArtifactAcquireOutcome::Unavailable(UnavailableReason::ResourceLimit)
                ))
                .count(),
            1
        );
        let published = outcomes
            .iter()
            .find_map(|outcome| match outcome {
                ArtifactAcquireOutcome::Published(bundle) => Some(bundle.clone()),
                _ => None,
            })
            .expect("one sibling profile must still publish");
        let logical_retained = service
            .registry
            .channel(ChannelId::new(1))
            .unwrap()
            .terminal_action()
            .snapshot()
            .unwrap()
            .retained_memory_bytes();
        assert_eq!(
            account.current.load(Ordering::SeqCst),
            logical_retained + published.retained_memory_bytes()
        );
        assert_eq!(scratch_budget.retained_bytes(), 0);
        assert_eq!(
            retained_budget.retained_bytes(),
            published.retained_memory_bytes()
        );
        let recorded = events.0.lock().unwrap();
        assert_eq!(
            recorded
                .iter()
                .filter(|event| matches!(
                    event,
                    RuntimeFilterEvent::ArtifactUnavailable {
                        reason: UnavailableReason::ResourceLimit,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert_eq!(
            recorded
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
                .count(),
            1
        );
        drop(recorded);

        drop(outcomes);
        drop(published);
        drop(subscriptions);
        drop(producer);
        drop(service);
        assert_eq!(scratch_budget.retained_bytes(), 0);
        assert_eq!(retained_budget.retained_bytes(), 0);
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn subscription_timeout_does_not_mark_channel_unavailable() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::TimedOut
        ));
        complete(&producer, 5);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Published(_)
        ));
    }

    #[test]
    fn expire_deadlines_marks_only_incomplete_channels_unavailable() {
        let fixture = fixture();
        fixture
            .service
            .install(view([
                deployment(1, 10, 30, 40, [10], [30], 100),
                deployment(2, 11, 31, 41, [11], [31], 100),
            ]))
            .unwrap();
        let completed_subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .unwrap();
        let incomplete_subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(31), uid(31))
            .unwrap();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        complete(&producer, 1);
        fixture
            .service
            .expire_deadlines(fixture.started + Duration::from_millis(100));
        assert!(matches!(
            completed_subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Published(_)
        ));
        assert!(matches!(
            incomplete_subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Unavailable(_)
        ));
    }

    #[test]
    fn shutdown_linearizes_before_a_channel_deadline_callback_can_return_late() {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let events = Arc::new(BlockingChannelDeadlineEvents {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            recorded: Mutex::new(Vec::new()),
        });
        let started = Instant::now();
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(started)),
            events.clone(),
            MemTrackerMemoryAccount::new_root_for_test("channel-deadline-lifecycle"),
        ));
        service
            .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
            .expect("install deadline channel");

        let (tick_done_tx, tick_done_rx) = mpsc::sync_channel(1);
        let tick_service = service.clone();
        let tick_thread = std::thread::spawn(move || {
            tick_service.expire_deadlines(started + Duration::from_millis(100));
            tick_done_tx.send(()).expect("deadline tick complete");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("channel deadline callback entered");

        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown_service = service.clone();
        let shutdown_events = events.clone();
        let shutdown_thread = std::thread::spawn(move || {
            shutdown_service.shutdown();
            shutdown_done_tx
                .send(shutdown_events.unavailable_count())
                .expect("shutdown complete");
        });
        let early_count = shutdown_done_rx
            .recv_timeout(Duration::from_millis(100))
            .ok();

        release_tx.send(()).expect("release channel callback");
        tick_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("deadline tick joined in time");
        let count_at_shutdown = early_count.unwrap_or_else(|| {
            shutdown_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown joined in time")
        });
        tick_thread.join().expect("deadline tick thread");
        shutdown_thread.join().expect("shutdown thread");

        assert!(
            early_count.is_none(),
            "shutdown returned while an admitted channel callback was still running"
        );
        assert_eq!(events.unavailable_count(), count_at_shutdown);
    }

    #[test]
    fn shutdown_hook_panic_closes_service_and_wakes_duplicate_shutdown() {
        let fixture = fixture();
        install_one(&fixture);
        let subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .expect("subscribe before shutdown");
        fixture
            .service
            .set_after_registry_cancel_before_channel_cancel_hook(Arc::new(|| {
                panic!("intentional service finalizer hook panic");
            }));

        let first = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fixture.service.shutdown();
        }));
        assert!(first.is_err(), "the finalizer must resume the hook panic");

        let (duplicate_done_tx, duplicate_done_rx) = mpsc::sync_channel(1);
        let duplicate_service = fixture.service.clone();
        let duplicate = std::thread::spawn(move || {
            duplicate_service.shutdown();
            duplicate_done_tx
                .send(())
                .expect("duplicate shutdown completion");
        });
        duplicate_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("duplicate shutdown must wake after a panicking finalizer");
        duplicate.join().expect("duplicate shutdown thread");

        assert_eq!(
            fixture
                .service
                .lifecycle
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .phase,
            super::LifecyclePhase::Closed,
        );
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Unavailable(_) | ArtifactAcquireOutcome::Cancelled
        ));
    }

    #[test]
    fn nested_service_unwind_preserves_outer_panic_in_subprocess() {
        let output = std::process::Command::new(
            std::env::current_exe().expect("current lib-test executable"),
        )
        .arg("nested_service_unwind_child")
        .arg("--ignored")
        .arg("--nocapture")
        .output()
        .expect("run isolated nested-unwind service regression");
        assert!(
            output.status.success(),
            "nested service unwind aborted the child process:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "isolated by nested_service_unwind_preserves_outer_panic_in_subprocess"]
    fn nested_service_unwind_child() {
        let fixture = fixture();
        install_one(&fixture);
        let subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .expect("subscribe before nested shutdown");
        fixture
            .service
            .set_after_registry_cancel_before_channel_cancel_hook(Arc::new(|| {
                panic!("secondary service teardown panic");
            }));

        let outer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _call =
                super::ServiceCall::admit(&fixture.service).expect("admit fake service callback");
            fixture.service.shutdown();
            panic!("outer service callback panic");
        }));
        let payload = match outer {
            Ok(_) => panic!("outer service callback must panic"),
            Err(payload) => payload,
        };
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("outer service callback panic"),
            "the original callback panic must survive nested finalization"
        );

        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let duplicate_service = fixture.service.clone();
        let duplicate = std::thread::spawn(move || {
            duplicate_service.shutdown();
            done_tx.send(()).expect("duplicate shutdown complete");
        });
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("duplicate shutdown must observe Closed");
        duplicate.join().expect("duplicate shutdown thread");
        assert_eq!(
            fixture
                .service
                .lifecycle
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .phase,
            super::LifecyclePhase::Closed,
        );
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Unavailable(_) | ArtifactAcquireOutcome::Cancelled
        ));
    }

    #[test]
    fn deadline_snapshot_before_shutdown_does_not_dispatch_after_terminal() {
        let events = Arc::new(Events::default());
        let started = Instant::now();
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(started)),
            events.clone(),
            MemTrackerMemoryAccount::new_root_for_test("deadline-snapshot-lifecycle"),
        ));
        service
            .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
            .expect("install deadline channel");
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let release_rx = Mutex::new(release_rx);
        let armed = Arc::new(AtomicBool::new(true));
        service.set_before_deadline_dispatch_hook(Arc::new(move || {
            if armed.swap(false, Ordering::AcqRel) {
                entered_tx.send(()).expect("deadline snapshot reached");
                release_rx
                    .lock()
                    .expect("deadline snapshot release")
                    .recv_timeout(Duration::from_secs(1))
                    .expect("deadline snapshot released");
            }
        }));

        let (tick_done_tx, tick_done_rx) = mpsc::sync_channel(1);
        let tick_service = service.clone();
        let tick_thread = std::thread::spawn(move || {
            tick_service.expire_deadlines(started + Duration::from_millis(100));
            tick_done_tx.send(()).expect("deadline tick complete");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("deadline snapshotted installation");

        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown_service = service.clone();
        let shutdown_thread = std::thread::spawn(move || {
            shutdown_service.shutdown();
            shutdown_done_tx.send(()).expect("shutdown complete");
        });
        let shutdown_completed_before_release = shutdown_done_rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok();

        release_tx.send(()).expect("release deadline snapshot");
        tick_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("deadline tick joined in time");
        if !shutdown_completed_before_release {
            shutdown_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown joined in time");
        }
        tick_thread.join().expect("deadline tick thread");
        shutdown_thread.join().expect("shutdown thread");

        assert!(
            shutdown_completed_before_release,
            "shutdown must close while an unadmitted channel dispatch is paused"
        );
        assert!(
            events
                .0
                .lock()
                .expect("recorded events")
                .iter()
                .all(|event| !matches!(event, RuntimeFilterEvent::ChannelUnavailable { .. }))
        );
    }

    #[test]
    fn unauthorized_binding_instance_or_partition_fails_before_mutation() {
        let fixture = fixture();
        install_one(&fixture);
        assert_eq!(
            fixture
                .service
                .open_producer(BindingId::new(99), uid(10), 1, ProducerPortKind::Membership)
                .err()
                .unwrap()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedBinding
        );
        assert_eq!(
            fixture
                .service
                .open_producer(BindingId::new(10), uid(99), 1, ProducerPortKind::Membership)
                .err()
                .unwrap()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedFragmentInstance
        );
        assert_eq!(
            fixture
                .service
                .subscribe_blocking(BindingId::new(99), uid(30))
                .err()
                .unwrap()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedBinding
        );
        assert_eq!(
            fixture
                .service
                .subscribe_blocking(BindingId::new(30), uid(99))
                .err()
                .unwrap()
                .kind(),
            RuntimeContractViolationKind::UnauthorizedFragmentInstance
        );
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        assert!(
            producer
                .submit(
                    PartitionId::new(1),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false)
                )
                .is_err()
        );
        assert_eq!(fixture.tracker.current(), 0);
        assert_eq!(fixture.tracker.peak(), 0);
    }

    #[test]
    fn local_preflight_contract_error_fails_synchronously() {
        let fixture = fixture();
        install_one(&fixture);

        let error = fixture
            .service
            .open_producer(BindingId::new(10), uid(99), 1, ProducerPortKind::Membership)
            .expect_err("an uninstalled local fragment instance must fail before dispatch");

        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::UnauthorizedFragmentInstance
        );
        assert_eq!(fixture.service.transport_pending_len_for_test(), 0);
        assert!(
            !fixture
                .service
                .core_producer_handle_exists_for_test(BindingId::new(10), uid(99))
        );
    }

    #[test]
    fn invalid_partition_precedes_rejecting_temporary_memory_account() {
        let account = Arc::new(RejectingMemoryAccount::default());
        let service = RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            Arc::new(Events::default()),
            account.clone(),
        );
        service
            .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
            .unwrap();
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(1),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert_eq!(account.calls.load(Ordering::SeqCst), 0);
        assert!(
            !service
                .registry
                .channel(ChannelId::new(1))
                .unwrap()
                .is_terminal()
        );
    }

    #[test]
    fn temporary_reservation_failure_revalidates_concurrent_duplicate() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let account = Arc::new(BlockingFirstRejectingMemoryAccount {
            calls: AtomicUsize::new(0),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            Arc::new(Events::default()),
            account,
        ));
        service
            .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
            .unwrap();
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        let first = producer.clone();
        let (first_tx, first_rx) = mpsc::channel();
        std::thread::spawn(move || {
            first_tx
                .send(first.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                ))
                .unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        release_tx.send(()).unwrap();
        assert_eq!(
            first_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Duplicate
        );
        assert!(
            !service
                .registry
                .channel(ChannelId::new(1))
                .unwrap()
                .is_terminal()
        );
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    ValueDomainDelta::new(MembershipValues::int64([8]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
    }

    #[test]
    fn install_follower_waits_for_reserved_batch_sink_completion() {
        let (sink_entered_tx, sink_entered_rx) = mpsc::channel();
        let (sink_release_tx, sink_release_rx) = mpsc::channel();
        let events = Arc::new(BlockingInstallEvents {
            entered: sink_entered_tx,
            release: Mutex::new(sink_release_rx),
            recorded: Mutex::new(Vec::new()),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            events.clone(),
            MemTrackerMemoryAccount::new_root_for_test("install-follower-publish"),
        ));
        let (commit_ready_tx, commit_ready_rx) = mpsc::channel();
        let (commit_release_tx, commit_release_rx) = mpsc::channel();
        let commit_release_rx = Mutex::new(commit_release_rx);
        service.set_before_commit_clock_hook(Arc::new(move || {
            commit_ready_tx.send(()).unwrap();
            commit_release_rx.lock().unwrap().recv().unwrap();
        }));
        let install = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let leader_service = service.clone();
        let leader_view = install.clone();
        let (leader_tx, leader_rx) = mpsc::channel();
        std::thread::spawn(move || leader_tx.send(leader_service.install(leader_view)).unwrap());
        commit_ready_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let follower_service = service.clone();
        let (follower_tx, follower_rx) = mpsc::channel();
        std::thread::spawn(move || follower_tx.send(follower_service.install(install)).unwrap());
        assert!(follower_rx.recv_timeout(Duration::from_millis(50)).is_err());
        commit_release_tx.send(()).unwrap();
        sink_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let follower_before_publish = follower_rx.recv_timeout(Duration::from_millis(50));
        sink_release_tx.send(()).unwrap();
        assert!(follower_before_publish.is_err());
        assert_eq!(
            leader_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::Installed
        );
        assert_eq!(
            follower_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            InstallOutcome::AlreadyInstalled
        );
        assert!(matches!(
            events.recorded.lock().unwrap().as_slice(),
            [
                RuntimeFilterEvent::DeploymentInstalled { .. },
                RuntimeFilterEvent::ChannelPlanned { .. }
            ]
        ));
    }

    #[test]
    fn duplicate_open_same_partition_count_is_idempotent() {
        let fixture = fixture();
        install_one(&fixture);
        let first = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 2, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        let second = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 2, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn conflicting_open_partition_count_fails() {
        let fixture = fixture();
        install_one(&fixture);
        fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        assert!(
            fixture
                .service
                .open_producer(BindingId::new(10), uid(10), 2, ProducerPortKind::Membership)
                .is_err()
        );
    }

    #[test]
    fn service_cancel_wakes_all_subscriptions_and_rejects_late_handles() {
        let fixture = fixture();
        install_one(&fixture);
        let subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            tx.send(subscription.acquire(Duration::from_secs(5)))
                .unwrap()
        });
        fixture.service.cancel();
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            ArtifactAcquireOutcome::Cancelled
        ));
        assert!(
            fixture
                .service
                .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
                .is_err()
        );
        assert!(
            fixture
                .service
                .subscribe_blocking(BindingId::new(30), uid(30))
                .is_err()
        );
        assert!(
            fixture
                .service
                .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
                .is_err()
        );
    }

    #[test]
    fn repeated_cancel_shutdown_and_drop_deliver_pending_cancellation_once() {
        let fixture = fixture();
        install_one(&fixture);
        let installed = fixture.service.registry.active_installation().unwrap();
        let callbacks = Arc::new(AtomicUsize::new(0));
        installed.set_subscription_delivery_hook(
            BindingId::new(30),
            Arc::new({
                let callbacks = callbacks.clone();
                move || {
                    callbacks.fetch_add(1, Ordering::SeqCst);
                }
            }),
        );
        fixture.service.cancel();
        fixture.service.cancel();
        fixture.service.shutdown();
        assert_eq!(
            installed.subscription_delivery_call_count(BindingId::new(30)),
            1
        );
        drop(installed);
        drop(fixture.service);
        assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancel_delivery_may_reenter_service_after_operation_lock_is_released() {
        let fixture = fixture();
        install_one(&fixture);
        let installed = fixture.service.registry.active_installation().unwrap();
        let weak_service = Arc::downgrade(&fixture.service);
        installed.set_subscription_delivery_hook(
            BindingId::new(30),
            Arc::new(move || {
                let service = weak_service.upgrade().unwrap();
                assert_eq!(
                    service
                        .subscribe_blocking(BindingId::new(30), uid(30))
                        .err()
                        .unwrap()
                        .kind(),
                    RuntimeContractViolationKind::ServiceUnavailable
                );
            }),
        );
        let service = fixture.service.clone();
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            service.cancel();
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("cancel delivery reentry deadlocked on the service operation lock");
    }

    #[test]
    fn cancel_invalidates_completed_logical_pending_artifact_before_dispatch() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([17]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture.service.set_producer_before_dispatch_hook(
            BindingId::new(10),
            uid(10),
            Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }),
        );
        let (outcome_tx, outcome_rx) = mpsc::channel();
        std::thread::spawn(move || {
            outcome_tx
                .send(producer.close_partition(PartitionId::new(0), ProducerSequence::new(1)))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        fixture.service.cancel();
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Cancelled
        ));
        release_tx.send(()).unwrap();
        assert_eq!(
            outcome_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Completed
        );
        let events = fixture.events.0.lock().unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. }))
                .count(),
            0
        );
        let position = |predicate: fn(&RuntimeFilterEvent) -> bool| {
            events
                .iter()
                .position(predicate)
                .expect("expected causal runtime-filter event")
        };
        let producer_closed =
            position(|event| matches!(event, RuntimeFilterEvent::ProducerInstanceClosed { .. }));
        let channel_completed =
            position(|event| matches!(event, RuntimeFilterEvent::ChannelCompleted { .. }));
        let subscription_cancelled =
            position(|event| matches!(event, RuntimeFilterEvent::SubscriptionCancelled { .. }));
        assert!(producer_closed < channel_completed);
        assert!(channel_completed < subscription_cancelled);
        for predicate in [
            (|event: &RuntimeFilterEvent| {
                matches!(event, RuntimeFilterEvent::ProducerInstanceClosed { .. })
            }) as fn(&RuntimeFilterEvent) -> bool,
            |event| matches!(event, RuntimeFilterEvent::ChannelCompleted { .. }),
        ] {
            assert_eq!(events.iter().filter(|event| predicate(event)).count(), 1);
        }
    }

    #[test]
    fn paused_progress_is_emitted_before_later_cancel_terminal() {
        let fixture = fixture();
        install_one(&fixture);
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture.service.set_producer_before_dispatch_hook(
            BindingId::new(10),
            uid(10),
            Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }),
        );
        let (submit_tx, submit_rx) = mpsc::channel();
        std::thread::spawn(move || {
            submit_tx
                .send(producer.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([9]), false),
                ))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let service = fixture.service.clone();
        let (cancel_tx, cancel_rx) = mpsc::channel();
        std::thread::spawn(move || {
            service.cancel();
            cancel_tx.send(()).unwrap();
        });
        cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(
            submit_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Applied
        );
        let events = fixture.events.0.lock().unwrap();
        let delta = events
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::DeltaAccepted { .. }))
            .unwrap();
        let producer_failed = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    RuntimeFilterEvent::ProducerInstanceFailed {
                        reason: ProducerFailureReason::Cancelled,
                        ..
                    }
                )
            })
            .unwrap();
        let cancelled = events
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .unwrap();
        assert!(delta < producer_failed);
        assert!(producer_failed < cancelled);
    }

    #[test]
    fn gapped_cancel_wakes_subscription_but_defers_event_until_all_core_orders_publish() {
        let fixture = fixture();
        install_one(&fixture);
        fixture.events.0.lock().unwrap().clear();
        let producer = fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        let subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .unwrap();

        let (ready0_tx, ready0_rx) = mpsc::channel();
        let (release0_tx, release0_rx) = mpsc::channel();
        let release0_rx = Mutex::new(release0_rx);
        fixture.service.set_producer_before_dispatch_hook(
            BindingId::new(10),
            uid(10),
            Arc::new(move || {
                ready0_tx.send(()).unwrap();
                release0_rx.lock().unwrap().recv().unwrap();
            }),
        );
        let first = producer.clone();
        let (first_tx, first_rx) = mpsc::channel();
        std::thread::spawn(move || {
            first_tx
                .send(first.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([31]), false),
                ))
                .unwrap();
        });
        ready0_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let (ready1_tx, ready1_rx) = mpsc::channel();
        let (release1_tx, release1_rx) = mpsc::channel();
        let release1_rx = Mutex::new(release1_rx);
        fixture.service.set_producer_before_dispatch_hook(
            BindingId::new(10),
            uid(10),
            Arc::new(move || {
                ready1_tx.send(()).unwrap();
                release1_rx.lock().unwrap().recv().unwrap();
            }),
        );
        let second = producer.clone();
        let (second_tx, second_rx) = mpsc::channel();
        std::thread::spawn(move || {
            second_tx
                .send(second.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    ValueDomainDelta::new(MembershipValues::int64([32]), false),
                ))
                .unwrap();
        });
        ready1_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let service = fixture.service.clone();
        let (cancel_tx, cancel_rx) = mpsc::channel();
        std::thread::spawn(move || {
            service.cancel();
            cancel_tx.send(()).unwrap();
        });
        cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (acquire_tx, acquire_rx) = mpsc::channel();
        std::thread::spawn(move || {
            acquire_tx
                .send(subscription.acquire(Duration::from_secs(5)))
                .unwrap();
        });
        assert!(matches!(
            acquire_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            ArtifactAcquireOutcome::Cancelled
        ));
        assert!(
            !fixture
                .events
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::SubscriptionCancelled { .. }))
        );

        release1_tx.send(()).unwrap();
        release0_tx.send(()).unwrap();
        assert_eq!(
            first_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            second_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Applied
        );
        let events = fixture.events.0.lock().unwrap();
        let deltas = events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                matches!(event, RuntimeFilterEvent::DeltaAccepted { .. }).then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(deltas.len(), 2);
        let channel_cancelled = events
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .unwrap();
        let subscription_cancelled = events
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::SubscriptionCancelled { .. }))
            .unwrap();
        assert!(deltas[0] < deltas[1]);
        assert!(deltas[1] < channel_cancelled);
        assert!(channel_cancelled < subscription_cancelled);
    }

    #[test]
    fn duplicate_terminal_dispatch_waits_for_claimed_route_and_notify() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([12]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_dispatcher_after_claim_hook(Arc::new(move || {
                ready_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }));
        let (close_tx, close_rx) = mpsc::channel();
        std::thread::spawn(move || {
            close_tx
                .send(producer.close_partition(PartitionId::new(0), ProducerSequence::new(1)))
                .unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let service = fixture.service.clone();
        let (cancel_tx, cancel_rx) = mpsc::channel();
        std::thread::spawn(move || {
            service.cancel();
            cancel_tx.send(()).unwrap();
        });
        cancel_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Cancelled
        ));
        release_tx.send(()).unwrap();
        assert_eq!(
            close_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            SubmitOutcome::Completed
        );
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Cancelled
        ));
        assert_eq!(
            fixture
                .service
                .dispatcher_pending_action_count(ChannelId::new(1)),
            0
        );
    }

    #[test]
    fn service_emits_stable_control_contribution_route_and_outcome_events() {
        let fixture = fixture();
        install_one(&fixture);
        let (producer, subscription) = open_and_subscribe(&fixture);
        complete(&producer, 3);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Published(_)
        ));
        let events = fixture.events.0.lock().unwrap();
        assert!(
            matches!(events.first(), Some(RuntimeFilterEvent::DeploymentInstalled { query_id, participant_id, epoch }) if *query_id == uid(0) && participant_id.get() == 3 && epoch.get() == 9)
        );
        assert!(events.iter().any(|event| matches!(event, RuntimeFilterEvent::DeltaAccepted { identity } if identity.query_id() == uid(0) && identity.participant_id().get() == 3 && identity.channel_id().get() == 1 && identity.epoch().get() == 9 && identity.stream().binding_id().get() == 10 && identity.stream().fragment_instance_id() == uid(10) && identity.stream().partition_id().get() == 0 && identity.sequence().get() == 0)));
        assert!(events.iter().any(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { identity, version } if identity.common().query_id() == uid(0) && identity.consumer_binding_id().get() == 30 && identity.fragment_instance_id() == uid(30) && identity.route_edge_id().get() == 40 && *version == LogicalVersion::FIRST)));
        assert!(events.iter().any(|event| matches!(event, RuntimeFilterEvent::SubscriptionAcquired { identity, version } if identity.consumer_binding_id().get() == 30 && *version == LogicalVersion::FIRST)));
    }

    #[test]
    fn materialization_event_panic_and_reentry_preserve_the_full_causal_chain() {
        let sink = Arc::new(MaterializationLifecycleEvents {
            subscription: Mutex::new(None),
            panicked: AtomicBool::new(false),
            reentered: AtomicBool::new(false),
            recorded: Mutex::new(Vec::new()),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("materialization-event-reentry"),
        ));
        service
            .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
            .unwrap();
        let subscription = service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .unwrap();
        *sink.subscription.lock().unwrap() = Some(Arc::downgrade(&subscription));
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        complete(&producer, 41);
        assert!(sink.panicked.load(Ordering::SeqCst));
        assert!(sink.reentered.load(Ordering::SeqCst));
        assert!(subscription.snapshot().is_some());

        let events = sink.recorded.lock().unwrap();
        let position = |predicate: fn(&RuntimeFilterEvent) -> bool| {
            events
                .iter()
                .position(predicate)
                .expect("expected materialization lifecycle event")
        };
        let channel_completed =
            position(|event| matches!(event, RuntimeFilterEvent::ChannelCompleted { .. }));
        let started =
            position(|event| matches!(event, RuntimeFilterEvent::MaterializationStarted { .. }));
        let materialized =
            position(|event| matches!(event, RuntimeFilterEvent::ArtifactMaterialized { .. }));
        let published =
            position(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }));
        let delivered =
            position(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. }));
        let acquired =
            position(|event| matches!(event, RuntimeFilterEvent::SubscriptionAcquired { .. }));
        assert!(channel_completed < started);
        assert!(started < materialized);
        assert!(materialized < published);
        assert!(published < delivered);
        assert!(delivered < acquired);
    }

    struct ReentrantSink {
        service: Mutex<Weak<RuntimeFilterService>>,
    }

    impl RuntimeFilterEventSink for ReentrantSink {
        fn record(&self, _event: RuntimeFilterEvent) {
            if let Some(service) = self.service.lock().unwrap().upgrade() {
                let _ = service.install(view([]));
            }
        }
    }

    struct NonemptyReentrantInstallSink {
        service: Mutex<Weak<RuntimeFilterService>>,
        view: Mutex<Option<RuntimeFilterParticipantInstall>>,
        outcome: mpsc::Sender<InstallOutcome>,
    }

    impl RuntimeFilterEventSink for NonemptyReentrantInstallSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::DeploymentInstalled { .. }) {
                return;
            }
            let Some(view) = self.view.lock().unwrap().take() else {
                return;
            };
            let Some(service) = self.service.lock().unwrap().upgrade() else {
                return;
            };
            self.outcome.send(service.install(view).unwrap()).unwrap();
        }
    }

    struct CrossThreadReentrantInstallSink {
        service: Mutex<Weak<RuntimeFilterService>>,
        view: Mutex<Option<RuntimeFilterParticipantInstall>>,
        outcome: mpsc::Sender<Option<InstallOutcome>>,
    }

    impl RuntimeFilterEventSink for CrossThreadReentrantInstallSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::DeploymentInstalled { .. }) {
                return;
            }
            let Some(view) = self.view.lock().unwrap().take() else {
                return;
            };
            let Some(service) = self.service.lock().unwrap().upgrade() else {
                return;
            };
            let (worker_tx, worker_rx) = mpsc::channel();
            std::thread::spawn(move || {
                let _ = worker_tx.send(service.install(view));
            });
            self.outcome
                .send(
                    worker_rx
                        .recv_timeout(Duration::from_secs(1))
                        .ok()
                        .and_then(Result::ok),
                )
                .unwrap();
        }
    }

    #[test]
    fn nonempty_reentrant_install_from_event_sink_does_not_wait_on_its_own_batch() {
        let install = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let (reentrant_tx, reentrant_rx) = mpsc::channel();
        let sink = Arc::new(NonemptyReentrantInstallSink {
            service: Mutex::new(Weak::new()),
            view: Mutex::new(Some(install.clone())),
            outcome: reentrant_tx,
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("nonempty-reentrant-install"),
        ));
        *sink.service.lock().unwrap() = Arc::downgrade(&service);
        let (outer_tx, outer_rx) = mpsc::channel();
        std::thread::spawn(move || outer_tx.send(service.install(install)).unwrap());
        assert_eq!(
            reentrant_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("event-sink install reentry deadlocked"),
            InstallOutcome::AlreadyInstalled
        );
        assert_eq!(
            outer_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("outer install did not finish")
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn cross_thread_reentrant_install_from_event_sink_observes_logical_commit() {
        let install = view([deployment(1, 10, 30, 40, [10], [30], 100)]);
        let (reentrant_tx, reentrant_rx) = mpsc::channel();
        let sink = Arc::new(CrossThreadReentrantInstallSink {
            service: Mutex::new(Weak::new()),
            view: Mutex::new(Some(install.clone())),
            outcome: reentrant_tx,
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(DynamicClock),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("cross-thread-reentrant-install"),
        ));
        *sink.service.lock().unwrap() = Arc::downgrade(&service);
        let (outer_tx, outer_rx) = mpsc::channel();
        std::thread::spawn(move || outer_tx.send(service.install(install)).unwrap());
        assert_eq!(
            reentrant_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("event-sink did not report cross-thread install outcome"),
            Some(InstallOutcome::AlreadyInstalled)
        );
        assert_eq!(
            outer_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("outer install did not finish")
                .unwrap(),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn reentrant_event_sink_does_not_deadlock_registry_or_channel_lock() {
        let sink = Arc::new(ReentrantSink {
            service: Mutex::new(Weak::new()),
        });
        let started = Instant::now();
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(started)),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("reentrant-query"),
        ));
        *sink.service.lock().unwrap() = Arc::downgrade(&service);
        assert_eq!(
            service
                .install(view([deployment(1, 10, 30, 40, [10], [30], 100)]))
                .unwrap(),
            InstallOutcome::Installed
        );
        let subscription = service
            .subscribe_blocking(BindingId::new(30), uid(30))
            .unwrap();
        let producer = service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap()
            .into_membership()
            .unwrap();
        complete(&producer, 7);
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Published(_)
        ));
    }

    #[test]
    fn same_channel_duplicate_reentry_during_core_publish_does_not_deadlock() {
        let sink = Arc::new(SameChannelReentryEvents::default());
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("same-channel-reentry"),
        ));
        *sink.dispatcher.lock().unwrap() = Arc::downgrade(&service.dispatcher);
        service
            .dispatcher
            .dispatch(
                ChannelId::new(1),
                ChannelAction::Progress {
                    order: Some(0),
                    outcome: SubmitOutcome::Applied,
                    events: vec![RuntimeFilterEvent::ChannelPlanned {
                        identity: RuntimeFilterEventIdentity::new(
                            uid(0),
                            RuntimeFilterParticipantId::new(3),
                            ChannelId::new(1),
                            DeploymentEpoch::new(9),
                        ),
                    }],
                },
            )
            .unwrap();
        assert!(sink.fired.load(Ordering::SeqCst));
    }

    #[test]
    fn cross_channel_reentry_keeps_duplicate_waiting_until_nested_event_publishes() {
        let (nested_tx, nested_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let sink = Arc::new(CrossChannelReentryEvents {
            dispatcher: Mutex::new(Weak::new()),
            nested_dispatched: Mutex::new(Some(nested_tx)),
            release: Mutex::new(release_rx),
            fired: AtomicBool::new(false),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            sink.clone(),
            MemTrackerMemoryAccount::new_root_for_test("cross-channel-reentry"),
        ));
        *sink.dispatcher.lock().unwrap() = Arc::downgrade(&service.dispatcher);

        let outer_dispatcher = service.dispatcher.clone();
        let outer = std::thread::spawn(move || {
            outer_dispatcher
                .dispatch(
                    ChannelId::new(1),
                    ChannelAction::Progress {
                        order: Some(0),
                        outcome: SubmitOutcome::Applied,
                        events: vec![RuntimeFilterEvent::ChannelPlanned {
                            identity: RuntimeFilterEventIdentity::new(
                                uid(0),
                                RuntimeFilterParticipantId::new(3),
                                ChannelId::new(1),
                                DeploymentEpoch::new(9),
                            ),
                        }],
                    },
                )
                .unwrap();
        });
        nested_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let duplicate_dispatcher = service.dispatcher.clone();
        let (duplicate_tx, duplicate_rx) = mpsc::channel();
        let duplicate = std::thread::spawn(move || {
            duplicate_dispatcher
                .dispatch(
                    ChannelId::new(2),
                    ChannelAction::Progress {
                        order: Some(0),
                        outcome: SubmitOutcome::Applied,
                        events: Vec::new(),
                    },
                )
                .unwrap();
            duplicate_tx.send(()).unwrap();
        });

        let early_return = duplicate_rx.recv_timeout(Duration::from_millis(50));
        release_tx.send(()).unwrap();
        duplicate_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        outer.join().unwrap();
        duplicate.join().unwrap();
        assert!(
            matches!(early_return, Err(mpsc::RecvTimeoutError::Timeout)),
            "concurrent duplicate returned before the nested event batch published"
        );
    }

    #[test]
    fn ordered_open_returns_ordered_handle_and_rejects_membership_request() {
        let service = installed_ordered_service_fixture();
        let error = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::Membership)
            .unwrap_err();
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ProducerPortMismatch
        );
        assert!(matches!(
            service
                .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
                .unwrap(),
            ProducerHandle::OrderedBound(_)
        ));
    }

    #[test]
    fn ordered_live_consumer_rejects_blocking_subscribe_without_cache_or_hang() {
        let service = installed_ordered_service_fixture();
        let subscribe_service = service.clone();
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let kind = match subscribe_service.subscribe_blocking(BindingId::new(2), uid(2)) {
                Ok(_) => None,
                Err(error) => Some(error.kind()),
            };
            done_tx.send(kind).unwrap();
        });
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Some(RuntimeContractViolationKind::SubscriptionActivationMismatch)
        );
        let installed = service.registry.active_installation().unwrap();
        assert!(
            installed
                .subscription(
                    BindingId::new(2),
                    uid(2),
                    SubscriptionKind::BlockingSnapshot
                )
                .is_none()
        );
        assert!(
            installed
                .subscription(BindingId::new(2), uid(2), SubscriptionKind::NonBlockingLive)
                .is_some()
        );
        assert_eq!(
            service
                .subscribe_blocking(BindingId::new(2), uid(2))
                .err()
                .unwrap()
                .kind(),
            RuntimeContractViolationKind::SubscriptionActivationMismatch
        );
    }

    #[test]
    fn ordered_rejected_lease_preserves_terminal_range_violation_precedence() {
        let account = Arc::new(ArmableRejectingMemoryAccount::default());
        let (service, contract) = installed_ordered_service_with_account(account.clone());
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };
        assert_eq!(
            producer
                .submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ordered_update(&contract, 40),
                )
                .unwrap(),
            SubmitOutcome::Published
        );
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            SubmitOutcome::Completed
        );
        account.armed.store(true, Ordering::SeqCst);

        let error = producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&contract, 30),
            )
            .err()
            .expect("terminal-range violation must precede rejected temporary lease");
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::SequenceOutsideTerminalRange
        );
    }

    #[test]
    fn ordered_rejected_lease_preserves_replay_and_contract_violation_precedence() {
        let account = Arc::new(ArmableRejectingMemoryAccount::default());
        let (service, contract) = installed_ordered_service_with_account(account.clone());
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };
        assert_eq!(
            producer
                .submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ordered_update(&contract, 40),
                )
                .unwrap(),
            SubmitOutcome::Published
        );
        let retained_before = account.current.load(Ordering::SeqCst);
        account.armed.store(true, Ordering::SeqCst);

        let replay_error = producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 30),
            )
            .err()
            .expect("conflicting replay must precede rejected temporary lease");
        assert_eq!(
            replay_error.kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );

        let mismatched_keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }];
        let mismatched_contract = RuntimeOrderContract::try_from_plan(&OrderContract {
            comparator_digest: comparator_digest_for_test(
                &mismatched_keys,
                COMPARATOR_ALGORITHM_VERSION,
            ),
            keys: mismatched_keys,
            inclusive: true,
        })
        .unwrap();
        let contract_error = producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&mismatched_contract, 30),
            )
            .err()
            .expect("order-contract violation must precede rejected temporary lease");
        assert_eq!(
            contract_error.kind(),
            RuntimeContractViolationKind::OrderedContractMismatch
        );
        assert_eq!(account.current.load(Ordering::SeqCst), retained_before);

        account.armed.store(false, Ordering::SeqCst);
        assert_eq!(
            producer
                .submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    ordered_update(&contract, 40),
                )
                .unwrap(),
            SubmitOutcome::SequenceAdvancedEqual
        );
    }

    #[test]
    fn ordered_rejected_lease_preserves_collecting_loosen_violation_and_state() {
        let account = Arc::new(ArmableRejectingMemoryAccount::default());
        let (service, contract) = installed_ordered_service_with_account(account.clone());
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };
        assert_eq!(
            producer
                .submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ordered_update(&contract, 40),
                )
                .unwrap(),
            SubmitOutcome::Published
        );
        let retained_before = account.current.load(Ordering::SeqCst);
        account.armed.store(true, Ordering::SeqCst);

        let error = producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&contract, 50),
            )
            .err()
            .expect("higher-sequence loosen violation must precede rejected temporary lease");
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::OrderedBoundLoosened
        );
        assert_eq!(account.current.load(Ordering::SeqCst), retained_before);

        account.armed.store(false, Ordering::SeqCst);
        assert_eq!(
            producer
                .submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    ordered_update(&contract, 30),
                )
                .unwrap(),
            SubmitOutcome::Published
        );
        let installed = service.registry.active_installation().unwrap();
        let snapshot = installed.channels().next().unwrap().1.snapshot().unwrap();
        assert_eq!(
            snapshot.version(),
            LogicalVersion::FIRST.checked_next().unwrap()
        );
    }

    #[test]
    fn ordered_rejected_lease_preserves_degraded_loosen_violation_and_version() {
        let account = Arc::new(ArmableRejectingMemoryAccount::default());
        let (service, contract) = installed_ordered_service_with_account(account.clone());
        let ProducerHandle::OrderedBound(producer) = service
            .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
            .unwrap()
        else {
            panic!("ordered fixture must return ordered producer")
        };
        assert_eq!(
            producer
                .submit_bound(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ordered_update(&contract, 40),
                )
                .unwrap(),
            SubmitOutcome::Published
        );
        producer
            .fail(ProducerFailureReason::ExecutionFailed)
            .unwrap();
        let retained_before = account.current.load(Ordering::SeqCst);
        account.armed.store(true, Ordering::SeqCst);

        let error = producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&contract, 50),
            )
            .err()
            .expect("degraded full reducer must preserve ordered loosen violation");
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::OrderedBoundLoosened
        );
        assert_eq!(account.current.load(Ordering::SeqCst), retained_before);
        let installed = service.registry.active_installation().unwrap();
        let snapshot = installed.channels().next().unwrap().1.snapshot().unwrap();
        assert_eq!(snapshot.version(), LogicalVersion::FIRST);
    }

    // ---- RFD-4/M2C Task 2: outbound artifact-delivery bridge ---------------------

    /// Injectable remote sink for the delivery Router's remote leg. It records every
    /// authorized route + wire frame it is handed so a test can decode and compare.
    /// M3 replaces this with the live network sender behind the same seam.
    #[derive(Default)]
    struct RecordingRemoteSink {
        envelopes: Mutex<
            Vec<(
                RouteEdgeId,
                crate::runtime_filter::port::transport::RuntimeFilterEnvelope,
            )>,
        >,
        completions: Mutex<VecDeque<SinkCompletion>>,
        before_send: Mutex<Option<Arc<dyn Fn(RuntimeFilterEnvelopeKind) + Send + Sync + 'static>>>,
    }

    impl RuntimeFilterEnvelopeSink for RecordingRemoteSink {
        fn try_send(
            &self,
            route: RuntimeFilterRemoteRoute,
            envelope: crate::runtime_filter::port::transport::RuntimeFilterTransportEnvelope,
        ) -> SinkSubmitOutcome {
            let envelope = envelope.envelope().clone();
            let hook = self
                .before_send
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .cloned();
            if let Some(hook) = hook {
                hook(envelope.kind());
            }
            self.envelopes
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push((route.route_edge_id(), envelope));
            SinkSubmitOutcome::Submitted
        }

        fn try_recv_completion(&self) -> Option<SinkCompletion> {
            self.completions
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pop_front()
        }

        fn shutdown(&self) {}
    }

    impl RecordingRemoteSink {
        fn set_before_send(
            &self,
            hook: Arc<dyn Fn(RuntimeFilterEnvelopeKind) + Send + Sync + 'static>,
        ) {
            *self
                .before_send
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(hook);
        }

        fn complete(&self, completion: SinkCompletion) {
            self.completions
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push_back(completion);
        }

        fn frames(&self) -> Vec<(RouteEdgeId, EncodedArtifactFrame)> {
            self.envelopes
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .map(|(edge, envelope)| {
                    (
                        *edge,
                        EncodedArtifactFrame::from_parts_for_test(
                            *envelope.schema_digest(),
                            envelope.payload().to_vec(),
                        ),
                    )
                })
                .collect()
        }

        fn envelopes(
            &self,
        ) -> Vec<(
            RouteEdgeId,
            crate::runtime_filter::port::transport::RuntimeFilterEnvelope,
        )> {
            self.envelopes
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
        }
    }

    struct ReentrantServiceShutdownSink {
        service: Mutex<Weak<RuntimeFilterService>>,
        send_entered: mpsc::SyncSender<()>,
        reentry_release: Mutex<Option<mpsc::Receiver<()>>>,
        reentry_returned: mpsc::SyncSender<()>,
        shutdown_entered: Option<mpsc::SyncSender<()>>,
        shutdown_release: Mutex<Option<mpsc::Receiver<()>>>,
        shutdown: AtomicBool,
    }

    impl RuntimeFilterEnvelopeSink for ReentrantServiceShutdownSink {
        fn try_send(
            &self,
            _route: RuntimeFilterRemoteRoute,
            _envelope: crate::runtime_filter::port::transport::RuntimeFilterTransportEnvelope,
        ) -> SinkSubmitOutcome {
            self.send_entered.send(()).expect("transport call entered");
            if let Some(release) = self
                .reentry_release
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                release.recv().expect("release service shutdown reentry");
            }
            self.service
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .upgrade()
                .expect("service remains alive during callback")
                .shutdown();
            self.reentry_returned
                .send(())
                .expect("service shutdown reentry returned");
            SinkSubmitOutcome::Submitted
        }

        fn try_recv_completion(&self) -> Option<SinkCompletion> {
            None
        }

        fn shutdown(&self) {
            if let Some(entered) = &self.shutdown_entered {
                entered.send(()).expect("sink shutdown entered");
            }
            if let Some(release) = self
                .shutdown_release
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                release.recv().expect("release sink shutdown");
            }
            self.shutdown.store(true, Ordering::Release);
        }
    }

    fn service_outbound_delivery_profile() -> ConsumerArtifactProfile {
        ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap()
    }

    /// Materialize a real membership bundle (valid canonical leaf encoding) for the
    /// given profile so the remote leg can genuinely round-trip through the codec.
    fn service_outbound_delivery_membership_bundle(
        profile: &ConsumerArtifactProfile,
        channel_id: ChannelId,
    ) -> Arc<ArtifactBundle> {
        let values = MembershipValues::int64([1, 2, 3]);
        let schema =
            ArtifactMembershipSchema::new(&values.data_type(), NullSemantics::NeverMatches)
                .unwrap();
        let snapshot = LogicalSnapshot::first(
            channel_id,
            ReducedMembershipDomain::new(values, false),
            RetainedMemoryReservation::empty(),
        );
        let plan = Materializer::plan(
            Arc::new(snapshot),
            &schema,
            profile,
            MaterializationPolicy::for_test(),
            1024,
        )
        .unwrap();
        match Materializer::materialize(
            plan,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(ArtifactScratchBudget::new(1 << 16, 1 << 16).unwrap()),
            MemTrackerMemoryAccount::new_root_for_test("service-outbound-delivery-bundle"),
        ) {
            MaterializationOutcome::Published(bundle) => bundle,
            other => panic!("membership fixture must publish a bundle, got {other:?}"),
        }
    }

    struct OutboundDeliveryRoutes {
        channel_id: ChannelId,
        profile: ConsumerArtifactProfile,
        loopback_edges: Vec<RouteEdgeId>,
        remote_edges: Vec<RouteEdgeId>,
    }

    impl OutboundDeliveryRoutes {
        fn all_edges(&self) -> Vec<RouteEdgeId> {
            self.loopback_edges
                .iter()
                .chain(&self.remote_edges)
                .copied()
                .collect()
        }
    }

    /// Install an aggregator (participant 3) on channel 1 with a local producer, a
    /// set of colocated (loopback) consumers, and a set of remote consumers. The
    /// routing shard is hand-built so the delivery Router (`route_delivery`) sees the
    /// exact loopback/remote FromAggregator edges; the shared consumer profile drives
    /// the codec. Returns the realized route-edge ids per peer class.
    fn install_outbound_delivery_aggregator(
        service: &RuntimeFilterService,
        local_consumers: &[(u32, u32, i64)],
        remote_consumers: &[(u32, u32, u32, &str)],
    ) -> OutboundDeliveryRoutes {
        install_outbound_delivery_aggregator_with_activation(
            service,
            local_consumers,
            remote_consumers,
            ConsumerActivation::BlockingSnapshot,
        )
    }

    fn install_outbound_delivery_aggregator_with_activation(
        service: &RuntimeFilterService,
        local_consumers: &[(u32, u32, i64)],
        remote_consumers: &[(u32, u32, u32, &str)],
        activation: ConsumerActivation,
    ) -> OutboundDeliveryRoutes {
        let epoch = DeploymentEpoch::new(9);
        let aggregator = RuntimeFilterParticipantId::new(3);
        let channel_id = ChannelId::new(1);
        let profile = service_outbound_delivery_profile();

        let local_profiles = local_consumers
            .iter()
            .map(|(binding, route, instance)| (*binding, *route, *instance, profile.clone()))
            .collect::<Vec<_>>();
        let max_concurrent_jobs = local_profiles
            .iter()
            .map(|(_, _, _, profile)| profile.id())
            .collect::<BTreeSet<_>>()
            .len();
        let channel = deployment_with_profiles_concurrency_and_activation(
            local_profiles,
            max_concurrent_jobs,
            activation,
        )
        .with_outbound_materialization_groups(BTreeMap::from([(
            profile.id(),
            OutboundMaterializationGroup::new(
                OutboundMaterializationOwner::Aggregator,
                profile.clone(),
                local_consumers
                    .iter()
                    .map(|(_, route, _)| RouteEdgeId::new(*route))
                    .chain(
                        remote_consumers
                            .iter()
                            .map(|(_, route, _, _)| RouteEdgeId::new(*route)),
                    )
                    .collect(),
            ),
        )]));
        let core_view = RuntimeFilterInstallView::new(
            epoch,
            aggregator,
            BTreeMap::from([(channel_id, channel)]),
        );

        // Local producer's ToAggregator edge is a loopback self-edge and must be
        // mirrored on both inbound and outbound sides.
        let producer_edge = RuntimeFilterRoutingEdgeView::new(
            channel_id,
            RouteEdgeId::new(1),
            RuntimeFilterRouteEndpointView::new(
                aggregator,
                RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            ),
            RuntimeFilterRouteEndpointView::new(aggregator, RuntimeFilterRouteRole::Aggregator),
            RuntimeFilterRoutePeer::Loopback,
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
            ]),
        )
        .unwrap();

        let mut local_roles = BTreeSet::from([
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
        ]);
        let producer_instances = BTreeMap::from([((BindingId::new(10), uid(10)), aggregator)]);
        let mut inbound_edges = vec![producer_edge.clone()];
        let mut outbound_edges = vec![producer_edge];
        let mut loopback_edges = Vec::new();
        for (binding, route, _instance) in local_consumers {
            local_roles.insert(RuntimeFilterRouteRole::Consumer(BindingId::new(*binding)));
            // A loopback FromAggregator edge is a self-edge: mirror it inbound+outbound.
            let edge = RuntimeFilterRoutingEdgeView::new(
                channel_id,
                RouteEdgeId::new(*route),
                RuntimeFilterRouteEndpointView::new(aggregator, RuntimeFilterRouteRole::Aggregator),
                RuntimeFilterRouteEndpointView::new(
                    aggregator,
                    RuntimeFilterRouteRole::Consumer(BindingId::new(*binding)),
                ),
                RuntimeFilterRoutePeer::Loopback,
                BTreeSet::from([
                    RuntimeFilterEnvelopeKind::Artifact,
                    RuntimeFilterEnvelopeKind::Unavailable,
                    RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                    RuntimeFilterEnvelopeKind::DegradedLogical,
                    RuntimeFilterEnvelopeKind::FinalArtifact,
                ]),
            )
            .unwrap();
            inbound_edges.push(edge.clone());
            outbound_edges.push(edge);
            loopback_edges.push(RouteEdgeId::new(*route));
        }
        let mut remote_edges = Vec::new();
        for (binding, route, participant, endpoint) in remote_consumers {
            let peer = RuntimeFilterParticipantId::new(*participant);
            let edge = RuntimeFilterRoutingEdgeView::new(
                channel_id,
                RouteEdgeId::new(*route),
                RuntimeFilterRouteEndpointView::new(aggregator, RuntimeFilterRouteRole::Aggregator),
                RuntimeFilterRouteEndpointView::new(
                    peer,
                    RuntimeFilterRouteRole::Consumer(BindingId::new(*binding)),
                ),
                RuntimeFilterRoutePeer::Remote {
                    participant_id: peer,
                    endpoint: RuntimeEndpoint::new(*endpoint, 9060).unwrap(),
                },
                BTreeSet::from([
                    RuntimeFilterEnvelopeKind::Artifact,
                    RuntimeFilterEnvelopeKind::Unavailable,
                    RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                    RuntimeFilterEnvelopeKind::DegradedLogical,
                    RuntimeFilterEnvelopeKind::FinalArtifact,
                ]),
            )
            .unwrap();
            outbound_edges.push(edge);
            remote_edges.push(RouteEdgeId::new(*route));
        }

        let routing_channel = RuntimeFilterChannelRoutingView::new(
            channel_id,
            local_roles,
            producer_instances,
            inbound_edges,
            outbound_edges,
        )
        .unwrap();
        let routing_shard = RuntimeFilterRoutingShard::new(
            epoch,
            aggregator,
            BTreeMap::from([(channel_id, routing_channel)]),
        )
        .unwrap();
        service
            .install(RuntimeFilterParticipantInstall::new(
                core_view,
                routing_shard,
            ))
            .unwrap();
        OutboundDeliveryRoutes {
            channel_id,
            profile,
            loopback_edges,
            remote_edges,
        }
    }

    #[test]
    fn service_outbound_delivery_loopback_edge_reaches_local_subscription() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator(&fixture.service, &[(20, 20, 200)], &[]);
        let bundle =
            service_outbound_delivery_membership_bundle(&routes.profile, routes.channel_id);
        let sink = Arc::new(RecordingRemoteSink::default());
        fixture.service.set_remote_sink_for_test(sink.clone());

        let decision = fixture
            .service
            .deliver_artifact(
                routes.channel_id,
                &routes.profile,
                routes.loopback_edges.clone(),
                ArtifactDeliveryOutcome::Published(bundle.clone()),
            )
            .unwrap();

        assert_eq!(decision.loopback_route_edge_ids(), &[RouteEdgeId::new(20)]);
        assert!(decision.remote_routes().is_empty());
        assert!(
            sink.frames().is_empty(),
            "loopback-only scope emits no wire frame"
        );

        let subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(20), uid(200))
            .unwrap();
        let acquired = subscription.acquire(Duration::ZERO);
        let ArtifactAcquireOutcome::Published(delivered) = acquired else {
            panic!("loopback subscription must acquire the published bundle, got {acquired:?}");
        };
        assert!(Arc::ptr_eq(&delivered, &bundle));
        assert_eq!(
            subscription.snapshot().unwrap().canonical_digest(),
            bundle.canonical_digest()
        );
    }

    #[test]
    fn service_outbound_delivery_remote_edge_emits_decodable_frame() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator(
            &fixture.service,
            &[(20, 20, 200)],
            &[(30, 30, 7, "10.0.0.7")],
        );
        let bundle =
            service_outbound_delivery_membership_bundle(&routes.profile, routes.channel_id);
        let sink = Arc::new(RecordingRemoteSink::default());
        fixture.service.set_remote_sink_for_test(sink.clone());

        let decision = fixture
            .service
            .deliver_artifact(
                routes.channel_id,
                &routes.profile,
                routes.remote_edges.clone(),
                ArtifactDeliveryOutcome::Published(bundle.clone()),
            )
            .unwrap();

        assert!(decision.loopback_route_edge_ids().is_empty());
        assert_eq!(decision.remote_routes().len(), 1);
        assert_eq!(
            decision.remote_routes()[0].route_edge_id(),
            RouteEdgeId::new(30)
        );

        let frames = sink.frames();
        assert_eq!(
            frames.len(),
            1,
            "the single remote edge emits one wire frame"
        );
        let (edge_id, frame) = &frames[0];
        assert_eq!(*edge_id, RouteEdgeId::new(30));
        assert_eq!(frame.profile_digest(), &bundle.profile_id().bytes());

        // Task 1 decode must reconstruct a bundle logically equal to the source.
        let decoded = decode_artifact_bundle(
            frame.payload(),
            frame.profile_digest(),
            ArtifactDecodeExpectation::new(&routes.profile),
            1 << 20,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            MemTrackerMemoryAccount::new_root_for_test("service-outbound-delivery-decode"),
        )
        .unwrap();
        assert_eq!(decoded.channel_id(), bundle.channel_id());
        assert_eq!(decoded.version(), bundle.version());
        assert_eq!(decoded.profile_id(), bundle.profile_id());
        assert_eq!(decoded.canonical_digest(), bundle.canonical_digest());
        assert_eq!(decoded.artifacts().len(), bundle.artifacts().len());
        assert_eq!(decoded.artifacts()[0].0, bundle.artifacts()[0].0);
        assert_eq!(
            decoded.artifacts()[0].1.canonical_bytes(),
            bundle.artifacts()[0].1.canonical_bytes()
        );
    }

    #[test]
    fn route_and_prequeue_sends_completed_without_artifact_to_remote_groups() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator(
            &fixture.service,
            &[(20, 20, 200)],
            &[(30, 30, 7, "10.0.0.7")],
        );
        let sink = Arc::new(RecordingRemoteSink::default());
        fixture.service.set_remote_sink_for_test(sink.clone());
        fixture
            .service
            .dispatcher
            .dispatch(
                routes.channel_id,
                ChannelAction::CompletedWithoutArtifact {
                    order: 0,
                    outcome: SubmitOutcome::CompletedWithoutArtifact,
                    events: Vec::new(),
                },
            )
            .unwrap();

        let envelopes = sink.envelopes();
        assert_eq!(envelopes.len(), 1);
        let (edge, envelope) = &envelopes[0];
        assert_eq!(*edge, routes.remote_edges[0]);
        assert_eq!(
            envelope.kind(),
            RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
        );
        assert_eq!(envelope.schema_digest(), &routes.profile.id().bytes());
        assert!(envelope.payload().is_empty());
    }

    #[test]
    fn route_and_prequeue_sends_degraded_logical_to_remote_groups() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator(
            &fixture.service,
            &[(20, 20, 200)],
            &[(30, 30, 7, "10.0.0.7")],
        );
        let sink = Arc::new(RecordingRemoteSink::default());
        fixture.service.set_remote_sink_for_test(sink.clone());
        let snapshot = Arc::new(LogicalSnapshot::first(
            routes.channel_id,
            ReducedMembershipDomain::new(MembershipValues::int64([1]), false),
            RetainedMemoryReservation::empty(),
        ));
        fixture
            .service
            .dispatcher
            .dispatch(
                routes.channel_id,
                ChannelAction::DegradedLogical {
                    order: 0,
                    outcome: SubmitOutcome::TerminalNoop,
                    reason: UnavailableReason::ProducerFailed,
                    snapshot,
                    events: Vec::new(),
                },
            )
            .unwrap();

        let envelopes = sink.envelopes();
        assert_eq!(envelopes.len(), 1);
        let (edge, envelope) = &envelopes[0];
        assert_eq!(*edge, routes.remote_edges[0]);
        assert_eq!(envelope.kind(), RuntimeFilterEnvelopeKind::DegradedLogical);
        assert_eq!(envelope.schema_digest(), &routes.profile.id().bytes());
        assert_eq!(
            decode_unavailable(
                envelope.payload(),
                envelope.schema_digest(),
                ArtifactDecodeExpectation::new(&routes.profile),
                1 << 20,
            )
            .unwrap(),
            UnavailableReason::ProducerFailed
        );
    }

    #[test]
    fn route_and_prequeue_sends_final_artifact_atomically_with_loopback_parity() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator_with_activation(
            &fixture.service,
            &[(20, 20, 200)],
            &[(30, 30, 7, "10.0.0.7")],
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
        );
        let sink = Arc::new(RecordingRemoteSink::default());
        fixture.service.set_remote_sink_for_test(sink.clone());
        let snapshot = Arc::new(LogicalSnapshot::first(
            routes.channel_id,
            ReducedMembershipDomain::new(MembershipValues::int64([1]), false),
            RetainedMemoryReservation::empty(),
        ));
        fixture
            .service
            .dispatcher
            .dispatch(
                routes.channel_id,
                ChannelAction::Completed {
                    order: 0,
                    outcome: SubmitOutcome::Completed,
                    snapshot,
                    events: Vec::new(),
                },
            )
            .unwrap();

        let SubscriptionHandle::Live(loopback_subscription) = fixture
            .service
            .subscribe(
                BindingId::new(20),
                uid(200),
                SubscriptionKind::NonBlockingLive,
            )
            .unwrap()
        else {
            panic!("completed loopback route must install a live subscription")
        };
        let LivePollOutcome::Updated {
            bundle: loopback,
            terminal: Some(LiveTerminal::Completed),
        } = loopback_subscription.poll_after(None)
        else {
            panic!("completed loopback route must atomically publish and complete")
        };
        let envelopes = sink.envelopes();
        assert_eq!(envelopes.len(), 1, "completion is one atomic wire message");
        let (edge, envelope) = &envelopes[0];
        assert_eq!(*edge, routes.remote_edges[0]);
        assert_eq!(envelope.kind(), RuntimeFilterEnvelopeKind::FinalArtifact);
        let remote = decode_artifact_bundle(
            envelope.payload(),
            envelope.schema_digest(),
            ArtifactDecodeExpectation::new(&routes.profile),
            1 << 20,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            MemTrackerMemoryAccount::new_root_for_test("final-artifact-parity"),
        )
        .unwrap();
        assert_eq!(remote.canonical_digest(), loopback.canonical_digest());
    }

    #[test]
    fn visible_then_completed_sends_artifact_then_atomic_final_artifact() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator(
            &fixture.service,
            &[(20, 20, 200)],
            &[(30, 30, 7, "10.0.0.7")],
        );
        let sink = Arc::new(RecordingRemoteSink::default());
        fixture.service.set_remote_sink_for_test(sink.clone());
        let snapshot = Arc::new(LogicalSnapshot::first(
            routes.channel_id,
            ReducedMembershipDomain::new(MembershipValues::int64([1]), false),
            RetainedMemoryReservation::empty(),
        ));

        fixture
            .service
            .dispatcher
            .dispatch(
                routes.channel_id,
                ChannelAction::VisibleSnapshot {
                    order: 0,
                    outcome: SubmitOutcome::Published,
                    version: snapshot.version(),
                    snapshot: snapshot.clone(),
                    events: Vec::new(),
                },
            )
            .unwrap();
        fixture
            .service
            .dispatcher
            .dispatch(
                routes.channel_id,
                ChannelAction::Completed {
                    order: 1,
                    outcome: SubmitOutcome::Completed,
                    snapshot,
                    events: Vec::new(),
                },
            )
            .unwrap();

        let envelopes = sink.envelopes();
        assert_eq!(envelopes.len(), 2);
        assert_eq!(
            envelopes
                .iter()
                .map(|(_, envelope)| envelope.kind())
                .collect::<Vec<_>>(),
            vec![
                RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::FinalArtifact,
            ],
        );
        assert_eq!(envelopes[0].1.payload(), envelopes[1].1.payload());
        assert_eq!(
            envelopes[0].1.schema_digest(),
            envelopes[1].1.schema_digest()
        );
    }

    #[test]
    fn reentrant_service_shutdown_waits_for_child_transport_teardown() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator(
            &fixture.service,
            &[(20, 20, 200)],
            &[(30, 30, 7, "10.0.0.7")],
        );
        let bundle =
            service_outbound_delivery_membership_bundle(&routes.profile, routes.channel_id);
        let (send_entered_tx, send_entered_rx) = mpsc::sync_channel(1);
        let (reentry_returned_tx, reentry_returned_rx) = mpsc::sync_channel(1);
        let (sink_shutdown_entered_tx, sink_shutdown_entered_rx) = mpsc::sync_channel(1);
        let (sink_shutdown_release_tx, sink_shutdown_release_rx) = mpsc::sync_channel(1);
        let sink = Arc::new(ReentrantServiceShutdownSink {
            service: Mutex::new(Arc::downgrade(&fixture.service)),
            send_entered: send_entered_tx,
            reentry_release: Mutex::new(None),
            reentry_returned: reentry_returned_tx,
            shutdown_entered: Some(sink_shutdown_entered_tx),
            shutdown_release: Mutex::new(Some(sink_shutdown_release_rx)),
            shutdown: AtomicBool::new(false),
        });
        fixture.service.set_remote_sink_for_test(sink.clone());

        let delivery_service = fixture.service.clone();
        let delivery_profile = routes.profile.clone();
        let delivery_edges = routes.remote_edges.clone();
        let delivery = std::thread::spawn(move || {
            delivery_service.deliver_artifact(
                routes.channel_id,
                &delivery_profile,
                delivery_edges,
                ArtifactDeliveryOutcome::Published(bundle),
            )
        });
        send_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("remote submission entered");
        reentry_returned_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("reentrant service shutdown returned");
        sink_shutdown_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("child transport teardown entered");

        let (duplicate_done_tx, duplicate_done_rx) = mpsc::sync_channel(1);
        let duplicate_service = fixture.service.clone();
        let duplicate = std::thread::spawn(move || {
            duplicate_service.shutdown();
            duplicate_done_tx
                .send(())
                .expect("duplicate service shutdown completion");
        });
        let duplicate_returned_before_child = duplicate_done_rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok();

        sink_shutdown_release_tx
            .send(())
            .expect("release child transport teardown");
        if !duplicate_returned_before_child {
            duplicate_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("duplicate shutdown joined after child teardown");
        }
        delivery.join().expect("delivery thread").expect("delivery");
        duplicate.join().expect("duplicate shutdown thread");

        assert!(
            !duplicate_returned_before_child,
            "service shutdown returned before child transport teardown"
        );
        assert!(sink.shutdown.load(Ordering::Acquire));
        assert_eq!(fixture.service.reliable_transport().pending_len(), 0);
    }

    #[test]
    fn parent_before_child_permit_order_breaks_cross_thread_shutdown_cycle() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator(
            &fixture.service,
            &[(20, 20, 200)],
            &[(30, 30, 7, "10.0.0.7")],
        );
        let bundle =
            service_outbound_delivery_membership_bundle(&routes.profile, routes.channel_id);
        let (send_entered_tx, send_entered_rx) = mpsc::sync_channel(1);
        let (reentry_release_tx, reentry_release_rx) = mpsc::sync_channel(1);
        let (reentry_returned_tx, reentry_returned_rx) = mpsc::sync_channel(1);
        let sink = Arc::new(ReentrantServiceShutdownSink {
            service: Mutex::new(Arc::downgrade(&fixture.service)),
            send_entered: send_entered_tx,
            reentry_release: Mutex::new(Some(reentry_release_rx)),
            reentry_returned: reentry_returned_tx,
            shutdown_entered: None,
            shutdown_release: Mutex::new(None),
            shutdown: AtomicBool::new(false),
        });
        fixture.service.set_remote_sink_for_test(sink.clone());

        let delivery_service = fixture.service.clone();
        let delivery_profile = routes.profile.clone();
        let delivery_edges = routes.remote_edges.clone();
        let delivery = std::thread::spawn(move || {
            delivery_service.deliver_artifact(
                routes.channel_id,
                &delivery_profile,
                delivery_edges,
                ArtifactDeliveryOutcome::Published(bundle),
            )
        });
        send_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("thread B holds the child transport permit");

        let (parent_close_requested_tx, parent_close_requested_rx) = mpsc::sync_channel(1);
        fixture
            .service
            .set_after_close_request_before_quiescence_hook(Arc::new(move || {
                parent_close_requested_tx
                    .send(())
                    .expect("parent close request observed");
            }));
        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown_service = fixture.service.clone();
        let shutdown = std::thread::spawn(move || {
            shutdown_service.shutdown();
            shutdown_done_tx
                .send(())
                .expect("thread A shutdown complete");
        });
        parent_close_requested_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("thread A requested parent close");

        reentry_release_tx
            .send(())
            .expect("release thread B into parent shutdown reentry");
        reentry_returned_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("thread B must defer parent shutdown instead of following thread A");
        shutdown_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("thread A completes after thread B releases parent and child permits");
        delivery.join().expect("delivery thread").expect("delivery");
        shutdown.join().expect("shutdown thread");

        assert!(sink.shutdown.load(Ordering::Acquire));
        assert_eq!(fixture.service.reliable_transport().pending_len(), 0);
    }

    #[test]
    fn service_outbound_delivery_fanout_is_read_from_the_route_decision() {
        // A 2-edge and an N-edge deployment exercise the identical delivery call; the
        // realized fanout is read from the Router decision, never a hardcoded count.
        fn deliver_scope(remote_consumers: &[(u32, u32, u32, &str)]) -> (usize, usize, usize) {
            let fixture = fixture();
            let routes = install_outbound_delivery_aggregator(
                &fixture.service,
                &[(20, 20, 200)],
                remote_consumers,
            );
            let bundle =
                service_outbound_delivery_membership_bundle(&routes.profile, routes.channel_id);
            let sink = Arc::new(RecordingRemoteSink::default());
            fixture.service.set_remote_sink_for_test(sink.clone());
            let decision = fixture
                .service
                .deliver_artifact(
                    routes.channel_id,
                    &routes.profile,
                    routes.all_edges(),
                    ArtifactDeliveryOutcome::Published(bundle),
                )
                .unwrap();
            (
                decision.loopback_route_edge_ids().len(),
                decision.remote_routes().len(),
                sink.frames().len(),
            )
        }

        // Two edges: one loopback consumer + one remote consumer.
        let (loopback, remote, frames) = deliver_scope(&[(30, 30, 7, "10.0.0.7")]);
        assert_eq!(loopback + remote, 2);
        assert_eq!(loopback, 1);
        assert_eq!(remote, 1);
        assert_eq!(frames, remote, "one wire frame per remote route");

        // Four edges: one loopback consumer + three remote consumers.
        let (loopback, remote, frames) = deliver_scope(&[
            (30, 30, 7, "10.0.0.7"),
            (40, 31, 11, "10.0.0.11"),
            (50, 32, 12, "10.0.0.12"),
        ]);
        assert_eq!(loopback + remote, 4);
        assert_eq!(loopback, 1);
        assert_eq!(remote, 3);
        assert_eq!(frames, remote, "one wire frame per remote route");
    }

    #[test]
    fn service_outbound_delivery_unavailable_flows_through_the_same_bridge() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator(
            &fixture.service,
            &[(20, 20, 200)],
            &[(30, 30, 7, "10.0.0.7")],
        );
        let sink = Arc::new(RecordingRemoteSink::default());
        fixture.service.set_remote_sink_for_test(sink.clone());

        let decision = fixture
            .service
            .deliver_artifact(
                routes.channel_id,
                &routes.profile,
                routes.all_edges(),
                ArtifactDeliveryOutcome::Unavailable(UnavailableReason::IncompleteCoverage),
            )
            .unwrap();

        assert_eq!(decision.loopback_route_edge_ids(), &[RouteEdgeId::new(20)]);
        assert_eq!(decision.remote_routes().len(), 1);
        // The remote leg still frames the Unavailable sentinel for its peer.
        assert_eq!(sink.frames().len(), 1);

        let subscription = fixture
            .service
            .subscribe_blocking(BindingId::new(20), uid(200))
            .unwrap();
        assert!(matches!(
            subscription.acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Unavailable(UnavailableReason::IncompleteCoverage)
        ));
    }

    #[test]
    fn service_outbound_delivery_rejects_an_unauthorized_edge() {
        let fixture = fixture();
        let routes = install_outbound_delivery_aggregator(&fixture.service, &[(20, 20, 200)], &[]);
        let bundle =
            service_outbound_delivery_membership_bundle(&routes.profile, routes.channel_id);
        let sink = Arc::new(RecordingRemoteSink::default());
        fixture.service.set_remote_sink_for_test(sink.clone());

        // An edge that is not in the installed routing shard must fail fast through
        // the Router rather than deliver on a best-effort basis.
        let error = fixture
            .service
            .deliver_artifact(
                routes.channel_id,
                &routes.profile,
                vec![RouteEdgeId::new(999)],
                ArtifactDeliveryOutcome::Published(bundle),
            )
            .unwrap_err();
        assert!(matches!(error, ArtifactDeliveryError::Route(_)));
        assert!(sink.frames().is_empty());
    }

    // ---- RFD-4/M2C Task 5 Part B: compiler-produced consumer delivery fixture ----

    // A compiler-produced consumer-delivery composite. The routing authority (delivery
    // route edge id, remote producer source) is projected entirely by the production
    // `deployment::compiler::compile`; only the consumer core-view entry the compiler
    // defers to RFD-4 (`project_install_views` is loopback-only) is supplied so the
    // authorized delivery can land in a real subscription.
    struct CompilerConsumerDeliveryFixture {
        install: RuntimeFilterParticipantInstall,
        consumer_participant: RuntimeFilterParticipantId,
        remote_source_participant: RuntimeFilterParticipantId,
        channel_id: ChannelId,
        consumer_binding: BindingId,
        delivery_route_edge: RouteEdgeId,
        consumer_finst: UniqueId,
        profile: ConsumerArtifactProfile,
    }

    fn compiler_consumer_delivery_fixture() -> CompilerConsumerDeliveryFixture {
        let plan = super::test_support::compiled_three_backend_all_of_plan();
        let installs = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .expect("compiler projections pair into participant installs");
        // The AllOf plan projects exactly one install view: the aggregator BE, which is
        // colocated with a consumer through a loopback FromAggregator delivery edge.
        let (consumer_participant, install) = installs
            .into_iter()
            .find(|(_, install)| {
                install.routing_shard().channels().values().any(|channel| {
                    channel
                        .local_roles()
                        .contains(&RuntimeFilterRouteRole::Aggregator)
                })
            })
            .expect("AllOf plan projects an aggregator install view");
        let (core_view, routing_shard) = install.into_parts();
        let channel_id = *core_view.channels().keys().next().expect("one channel");
        let routing_channel = routing_shard.channel(channel_id).expect("routing channel");

        // Locate the compiler's loopback aggregator -> consumer delivery edge.
        let delivery_edge = routing_channel
            .inbound_edges()
            .iter()
            .find(|edge| {
                edge.source().role() == RuntimeFilterRouteRole::Aggregator
                    && matches!(edge.target().role(), RuntimeFilterRouteRole::Consumer(_))
                    && matches!(edge.peer(), RuntimeFilterRoutePeer::Loopback)
            })
            .expect("aggregator hosts a loopback consumer delivery edge");
        let RuntimeFilterRouteRole::Consumer(consumer_binding) = delivery_edge.target().role()
        else {
            unreachable!("delivery edge target was filtered to a Consumer role");
        };
        let delivery_route_edge = delivery_edge.route_edge_id();

        // A remote producer instance is the cross-participant source that feeds the
        // aggregator whose artifact this consumer receives.
        let remote_source_participant = routing_channel
            .producer_instances()
            .values()
            .copied()
            .find(|participant| *participant != consumer_participant)
            .expect("the AllOf plan carries a remote producer source");

        // Add the RFD-4-deferred consumer on the compiler's loopback delivery edge.
        let template = core_view.channels().get(&channel_id).unwrap();
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let consumer_finst = UniqueId::new(1, 1);
        let mut consumers = template.consumers().clone();
        consumers.insert(
            consumer_binding,
            ConsumerDeployment::with_profile(
                ConsumerActivation::BlockingSnapshot,
                BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                profile.clone(),
                BTreeSet::from([delivery_route_edge]),
                BTreeSet::from([consumer_finst]),
            ),
        );
        let consumer_channel = RuntimeFilterChannelDeployment::new(
            template.channel_id(),
            template.logical_domain().clone(),
            template.lifecycle(),
            template.availability_coverage().clone(),
            template.terminal_coverage().clone(),
            template.reduction_requirement(),
            template.allowed_contribution_kinds().clone(),
            template.completion_requirement(),
            template.policy(),
            template.core_budget(),
            template.materialization_policy(),
            template.producers().clone(),
            consumers,
        )
        .with_outbound_materialization_groups(template.outbound_materialization_groups().clone());
        let mut channels = core_view.channels().clone();
        channels.insert(channel_id, consumer_channel);
        let augmented_core =
            RuntimeFilterInstallView::new(core_view.epoch(), consumer_participant, channels);
        CompilerConsumerDeliveryFixture {
            install: RuntimeFilterParticipantInstall::new(augmented_core, routing_shard),
            consumer_participant,
            remote_source_participant,
            channel_id,
            consumer_binding,
            delivery_route_edge,
            consumer_finst,
            profile,
        }
    }

    #[test]
    fn consumer_delivery_compiler_fixture_remote_source_reaches_subscription() {
        use crate::runtime::query_context::{QueryContextManager, QueryId};
        use crate::runtime_filter::codec::artifact::{
            encode_artifact_bundle, max_encoded_len_for_artifact_budget, semantic_artifact_bytes,
        };
        use crate::runtime_filter::port::transport::{
            DeliveryRouteIdentity, RuntimeFilterAcceptStatus, RuntimeFilterEnvelope,
            RuntimeFilterRouteIdentity,
        };
        use crate::service::runtime_filter_envelope_ingress::query_scoped_runtime_filter_envelope_ingress_with_manager;

        let fixture = compiler_consumer_delivery_fixture();
        // The compiled plan is genuinely distributed: the artifact's producer source is a
        // different backend than the consumer participant receiving the delivery.
        assert_ne!(
            fixture.remote_source_participant, fixture.consumer_participant,
            "the delivered artifact's source must be a remote participant"
        );

        const QUERY: QueryId = QueryId::new(71, 72);
        let query_uid = UniqueId::new(71, 72);
        let epoch = fixture.install.epoch();

        let manager = QueryContextManager::new_for_test();
        manager
            .get_or_register_native(
                QUERY,
                false,
                Duration::from_secs(30),
                Duration::from_secs(30),
            )
            .expect("register native query context");
        let service = manager
            .runtime_filter_service_for_ingress(QUERY)
            .expect("registered query exposes a runtime filter service");
        assert_eq!(
            service.install(fixture.install).unwrap(),
            InstallOutcome::Installed
        );

        // Materialize a real bundle for the consumer profile and encode it to a wire frame.
        let bundle =
            service_outbound_delivery_membership_bundle(&fixture.profile, fixture.channel_id);
        let ceiling =
            max_encoded_len_for_artifact_budget(semantic_artifact_bytes(&bundle).unwrap()).unwrap();
        let (digest, payload) = encode_artifact_bundle(
            &bundle,
            ArtifactDecodeExpectation::new(&fixture.profile),
            ceiling,
        )
        .unwrap()
        .into_parts();

        // Submit the remote Artifact envelope through the production query-scoped gRPC
        // ingress adapter (lookup -> dispatch_inbound_consumer).
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager.clone());
        let envelope = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::Artifact,
            query_uid,
            fixture.channel_id,
            epoch,
            RuntimeFilterRouteIdentity::delivery(
                DeliveryRouteIdentity::try_new(
                    fixture.delivery_route_edge,
                    ProducerSequence::new(1),
                )
                .unwrap(),
            ),
            None,
            None,
            &digest,
            payload,
        )
        .unwrap();
        assert_eq!(
            ingress.accept(envelope).accept_status(),
            RuntimeFilterAcceptStatus::Accepted,
        );

        // Route-authorization (against the compiler routing shard) -> decode -> deliver
        // formed one chain: the real subscription now retains the logically-equal artifact.
        let delivered = manager
            .runtime_filter_service_for_ingress(QUERY)
            .expect("installed query exposes a runtime filter service")
            .subscribe(
                fixture.consumer_binding,
                fixture.consumer_finst,
                SubscriptionKind::BlockingSnapshot,
            )
            .expect("compiler consumer binding is subscribable")
            .into_blocking()
            .expect("consumer activation is blocking-snapshot")
            .snapshot()
            .map(|delivered| delivered.canonical_digest());
        assert_eq!(
            delivered,
            Some(bundle.canonical_digest()),
            "the compiler-authorized delivery must land the logically-equal artifact"
        );
    }

    // ---- RFD-4/M3 Task 5: bounded at-least-once transport, fake-network fixture -----

    /// Project the structured transport events out of a recording lifecycle sink.
    fn transport_events_of(
        events: &Events,
    ) -> Vec<(TransportRouteEventIdentity, TransportEventKind, usize)> {
        events
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter_map(|event| match event {
                RuntimeFilterEvent::TransportEnvelope {
                    identity,
                    kind,
                    bytes,
                } => Some((*identity, *kind, *bytes)),
                _ => None,
            })
            .collect()
    }

    /// A service built on the PRODUCTION compiler's 1FE+3BE `AllOf` topology, with a
    /// recording lifecycle sink and an injected fake remote transport sink. The compiler
    /// (`compiled_three_backend_all_of_plan`) projects the aggregator's genuine remote
    /// consumer-delivery edge; the fixture never hand-builds a routing shard.
    struct ReliableTransportFixture {
        service: Arc<RuntimeFilterService>,
        events: Arc<Events>,
        remote_sink: Arc<RecordingRemoteSink>,
        profile: ConsumerArtifactProfile,
        channel_id: ChannelId,
        remote_delivery_edge: RouteEdgeId,
        started: Instant,
    }

    fn reliable_transport_fixture() -> ReliableTransportFixture {
        let base = compiler_consumer_delivery_fixture();
        // The genuine cross-backend delivery route the production compiler projected for
        // the aggregator: an aggregator -> consumer edge whose peer is Remote.
        let remote_delivery_edge = base
            .install
            .routing_shard()
            .channel(base.channel_id)
            .expect("compiler routing channel")
            .outbound_edges()
            .iter()
            .find(|edge| {
                edge.source().role() == RuntimeFilterRouteRole::Aggregator
                    && matches!(edge.target().role(), RuntimeFilterRouteRole::Consumer(_))
                    && matches!(edge.peer(), RuntimeFilterRoutePeer::Remote { .. })
            })
            .expect("the AllOf plan projects a remote aggregator -> consumer delivery edge")
            .route_edge_id();

        let events = Arc::new(Events::default());
        let started = Instant::now();
        let tracker = MemTrackerMemoryAccount::new_root_for_test("reliable-transport-fixture");
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(started)),
            events.clone(),
            tracker,
        ));
        assert_eq!(
            service.install(base.install).unwrap(),
            InstallOutcome::Installed
        );
        let remote_sink = Arc::new(RecordingRemoteSink::default());
        service.set_remote_sink_for_test(remote_sink.clone());
        ReliableTransportFixture {
            service,
            events,
            remote_sink,
            profile: base.profile,
            channel_id: base.channel_id,
            remote_delivery_edge,
            started,
        }
    }

    struct ProducerFailedOpenFixture {
        service: Arc<RuntimeFilterService>,
        events: Arc<Events>,
        remote_sink: Arc<RecordingRemoteSink>,
        route: RuntimeFilterRemoteRoute,
        event_identity: TransportRouteEventIdentity,
        started: Instant,
    }

    fn producer_failed_open_fixture() -> ProducerFailedOpenFixture {
        let fixture = fixture();
        assert_eq!(
            fixture
                .service
                .install(view([deployment(1, 10, 30, 40, [10], [30], 10_000)]))
                .unwrap(),
            InstallOutcome::Installed
        );
        fixture
            .service
            .configure_transport(ReliableTransportPolicy::new(
                Duration::from_millis(50),
                2,
                Duration::from_millis(150),
                16,
                4096,
            ))
            .unwrap();
        fixture
            .service
            .open_producer(BindingId::new(10), uid(10), 1, ProducerPortKind::Membership)
            .unwrap();
        let remote_sink = Arc::new(RecordingRemoteSink::default());
        fixture
            .service
            .set_remote_sink_for_test(remote_sink.clone());
        let route = RuntimeFilterRemoteRoute::new(
            RouteEdgeId::new(99),
            RuntimeFilterParticipantId::new(7),
            RuntimeEndpoint::new("10.0.0.7", 9060).unwrap(),
            RuntimeFilterRouteRole::Aggregator,
        )
        .unwrap();
        let event_identity = TransportRouteEventIdentity::new(
            RuntimeFilterEventIdentity::new(
                uid(0),
                RuntimeFilterParticipantId::new(3),
                ChannelId::new(1),
                DeploymentEpoch::new(9),
            ),
            route.route_edge_id(),
        );
        ProducerFailedOpenFixture {
            service: fixture.service,
            events: fixture.events,
            remote_sink,
            route,
            event_identity,
            started: fixture.started,
        }
    }

    fn remote_membership_install(
        value_type: DataType,
        max_contribution_bytes: usize,
    ) -> RuntimeFilterParticipantInstall {
        let channel_id = ChannelId::new(1);
        let binding_id = BindingId::new(10);
        let witness = CoverageWitnessId::new(101);
        let participant = RuntimeFilterParticipantId::new(3);
        let remote_participant = RuntimeFilterParticipantId::new(7);
        let channel = RuntimeFilterChannelDeployment::new(
            channel_id,
            RuntimeFilterLogicalDomain::Membership {
                value_type,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: u64::try_from(max_contribution_bytes).unwrap(),
                max_artifact_bytes: 1024,
                deadline_ms: 10_000,
                max_retries: 2,
            },
            RuntimeFilterCoreBudget::new(8192),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                binding_id,
                ProducerDeployment::new(witness, BTreeSet::from([uid(10)])),
            )]),
            BTreeMap::new(),
        );
        let producer_role = RuntimeFilterRouteRole::Producer(binding_id);
        let edge = RuntimeFilterRoutingEdgeView::new(
            channel_id,
            RouteEdgeId::new(99),
            RuntimeFilterRouteEndpointView::new(participant, producer_role),
            RuntimeFilterRouteEndpointView::new(
                remote_participant,
                RuntimeFilterRouteRole::Aggregator,
            ),
            RuntimeFilterRoutePeer::Remote {
                participant_id: remote_participant,
                endpoint: RuntimeEndpoint::new("10.0.0.7", 9060).unwrap(),
            },
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
            ]),
        )
        .unwrap();
        let routing_channel = RuntimeFilterChannelRoutingView::new(
            channel_id,
            BTreeSet::from([producer_role]),
            BTreeMap::from([((binding_id, uid(10)), participant)]),
            Vec::new(),
            vec![edge],
        )
        .unwrap();
        RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(
                DeploymentEpoch::new(9),
                participant,
                BTreeMap::from([(channel_id, channel)]),
            ),
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(9),
                participant,
                BTreeMap::from([(channel_id, routing_channel)]),
            )
            .unwrap(),
        )
    }

    struct RemoteProducerFixture {
        service: Arc<RuntimeFilterService>,
        sink: Arc<RecordingRemoteSink>,
        started: Instant,
    }

    fn remote_producer_fixture(
        value_type: DataType,
        max_contribution_bytes: usize,
    ) -> RemoteProducerFixture {
        let fixture = fixture();
        fixture
            .service
            .install(remote_membership_install(
                value_type,
                max_contribution_bytes,
            ))
            .unwrap();
        let sink = Arc::new(RecordingRemoteSink::default());
        fixture.service.set_remote_sink_for_test(sink.clone());
        RemoteProducerFixture {
            service: fixture.service,
            sink,
            started: fixture.started,
        }
    }

    fn open_remote_membership(
        fixture: &RemoteProducerFixture,
        local_partition_count: u32,
    ) -> Result<Arc<dyn ProducerAdapter>, RuntimeContractViolation> {
        fixture
            .service
            .open_producer(
                BindingId::new(10),
                uid(10),
                local_partition_count,
                ProducerPortKind::Membership,
            )
            .and_then(|handle| handle.into_membership())
    }

    #[test]
    fn remote_valid_oversized_utf8_membership_fails_open_without_query_error() {
        let fixture = remote_producer_fixture(DataType::Utf8, 8);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        let outcome = producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(
                    MembershipValues::utf8(["a structurally valid but oversized scalar"]),
                    false,
                ),
            )
            .expect("oversized legal contribution must fail only the optimization");
        assert_eq!(outcome, SubmitOutcome::TerminalNoop);
        assert_eq!(
            fixture
                .sink
                .envelopes()
                .iter()
                .map(|(_, envelope)| envelope.kind())
                .collect::<Vec<_>>(),
            vec![RuntimeFilterEnvelopeKind::ProducerUnavailable]
        );
        assert!(fixture.service.lifecycle.is_running());

        let invalid = remote_producer_fixture(DataType::Utf8, 1024);
        let producer = open_remote_membership(&invalid, 1).unwrap();
        let error = producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([7]), false),
            )
            .expect_err("schema/type mismatch remains a synchronous contract error");
        assert_eq!(error.kind(), RuntimeContractViolationKind::TypeMismatch);
        assert!(invalid.sink.envelopes().is_empty());
    }

    #[test]
    fn remote_final_encode_allocator_rejection_fails_open_without_query_error() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        let outcome = crate::runtime_filter::codec::contribution::with_rejecting_contribution_allocator_for_test(
            || {
                producer.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
            },
        )
        .expect("allocation rejection must fail only the optimization");
        assert_eq!(outcome, SubmitOutcome::TerminalNoop);
        assert_eq!(
            fixture
                .sink
                .envelopes()
                .iter()
                .map(|(_, envelope)| envelope.kind())
                .collect::<Vec<_>>(),
            vec![RuntimeFilterEnvelopeKind::ProducerUnavailable]
        );
        assert!(fixture.service.lifecycle.is_running());
    }

    #[test]
    fn remote_reopen_freezes_partition_count_even_after_handle_expires() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let first = open_remote_membership(&fixture, 1).unwrap();
        let live_error = open_remote_membership(&fixture, 2)
            .err()
            .expect("live reopen must fail");
        assert_eq!(
            live_error.kind(),
            RuntimeContractViolationKind::PartitionCountConflict
        );
        drop(first);
        let expired_error = open_remote_membership(&fixture, 2)
            .err()
            .expect("expired reopen must fail");
        assert_eq!(
            expired_error.kind(),
            RuntimeContractViolationKind::PartitionCountConflict
        );
    }

    #[test]
    fn remote_handle_close_then_submit_is_outside_terminal_range_without_enqueue() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(0))
                .unwrap(),
            SubmitOutcome::Applied
        );
        let sends = fixture.sink.envelopes().len();
        let error = producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([7]), false),
            )
            .expect_err("a contribution at the terminal sequence must be rejected");
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::SequenceOutsideTerminalRange
        );
        assert_eq!(fixture.sink.envelopes().len(), sends);
    }

    #[test]
    fn remote_handle_fail_then_submit_is_terminal_noop_without_enqueue() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        assert_eq!(
            producer
                .fail(ProducerFailureReason::ExecutionFailed)
                .unwrap(),
            SubmitOutcome::Applied
        );
        let sends = fixture.sink.envelopes().len();
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
                .unwrap(),
            SubmitOutcome::TerminalNoop
        );
        assert_eq!(fixture.sink.envelopes().len(), sends);
    }

    #[test]
    fn remote_handle_async_failed_open_then_submit_is_terminal_noop_without_enqueue() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([7]), false),
            )
            .unwrap();
        let contribution = fixture.sink.envelopes()[0].1.clone();
        fixture.sink.complete(SinkCompletion::Ack(
            contribution.route_identity().clone(),
            RuntimeFilterAcceptStatus::Rejected,
        ));
        fixture.service.tick(fixture.started);
        let sends = fixture.sink.envelopes().len();
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    ValueDomainDelta::new(MembershipValues::int64([8]), false),
                )
                .unwrap(),
            SubmitOutcome::TerminalNoop
        );
        assert_eq!(fixture.sink.envelopes().len(), sends);
    }

    #[test]
    fn remote_close_before_gap_allows_missing_sequences_to_fill() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(2))
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(1),
                    ValueDomainDelta::new(MembershipValues::int64([8]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
                .unwrap(),
            SubmitOutcome::Applied
        );
        assert_eq!(fixture.sink.envelopes().len(), 3);
    }

    #[test]
    fn remote_exact_close_replay_is_duplicate_without_enqueue() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap();
        let sends = fixture.sink.envelopes().len();
        assert_eq!(
            producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .unwrap(),
            SubmitOutcome::Duplicate
        );
        assert_eq!(fixture.sink.envelopes().len(), sends);
    }

    #[test]
    fn remote_conflicting_terminal_sequence_is_synchronous_error() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(2))
            .unwrap();
        let error = producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(3))
            .expect_err("terminal sequence is frozen on first close");
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ConflictingTerminalSequence
        );
    }

    #[test]
    fn remote_sequence_at_or_above_terminal_is_synchronous_error() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(2))
            .unwrap();
        let error = producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(2),
                ValueDomainDelta::new(MembershipValues::int64([7]), false),
            )
            .expect_err("terminal range is exclusive");
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::SequenceOutsideTerminalRange
        );
    }

    #[test]
    fn remote_terminal_still_rejects_invalid_partition_and_type_before_noop() {
        let invalid_partition = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&invalid_partition, 1).unwrap();
        producer
            .fail(ProducerFailureReason::ExecutionFailed)
            .unwrap();
        let error = producer
            .submit(
                PartitionId::new(1),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([7]), false),
            )
            .expect_err("partition preflight remains structural after terminal");
        assert_eq!(error.kind(), RuntimeContractViolationKind::InvalidPartition);

        let invalid_type = remote_producer_fixture(DataType::Utf8, 1024);
        let producer = open_remote_membership(&invalid_type, 1).unwrap();
        producer
            .fail(ProducerFailureReason::ExecutionFailed)
            .unwrap();
        let sends = invalid_type.sink.envelopes().len();
        let error = producer
            .submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([7]), false),
            )
            .expect_err("type preflight remains structural after terminal");
        assert_eq!(error.kind(), RuntimeContractViolationKind::TypeMismatch);
        assert_eq!(invalid_type.sink.envelopes().len(), sends);
    }

    #[test]
    fn remote_fail_linearizes_after_an_admitted_submit_send() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        fixture.sink.set_before_send({
            let released = Arc::clone(&released);
            Arc::new(move |kind| {
                if kind != RuntimeFilterEnvelopeKind::Contribution {
                    return;
                }
                entered_tx.send(()).expect("submit send entered");
                let (lock, wake) = &*released;
                let mut ready = lock.lock().unwrap_or_else(|error| error.into_inner());
                while !*ready {
                    ready = wake.wait(ready).unwrap_or_else(|error| error.into_inner());
                }
            })
        });

        let submit = {
            let producer = Arc::clone(&producer);
            std::thread::spawn(move || {
                producer.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
            })
        };
        entered_rx.recv().expect("submit reached the send barrier");
        let (fail_done_tx, fail_done_rx) = mpsc::sync_channel(1);
        let fail = {
            let producer = Arc::clone(&producer);
            std::thread::spawn(move || {
                let outcome = producer.fail(ProducerFailureReason::ExecutionFailed);
                fail_done_tx.send(outcome).expect("report fail outcome");
            })
        };
        assert!(
            fail_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "fail must wait behind the admitted submit linearization barrier"
        );
        {
            let (lock, wake) = &*released;
            *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
            wake.notify_all();
        }
        assert_eq!(submit.join().unwrap().unwrap(), SubmitOutcome::Applied);
        assert_eq!(
            fail_done_rx.recv().unwrap().unwrap(),
            SubmitOutcome::Applied
        );
        fail.join().unwrap();
        assert_eq!(
            fixture
                .sink
                .envelopes()
                .iter()
                .map(|(_, envelope)| envelope.kind())
                .collect::<Vec<_>>(),
            vec![
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
            ]
        );
    }

    #[test]
    fn remote_same_thread_reentrant_fail_does_not_deadlock_and_fails_open_once() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        let (reentrant_tx, reentrant_rx) = mpsc::sync_channel(1);
        fixture.sink.set_before_send({
            let producer = Arc::clone(&producer);
            Arc::new(move |kind| {
                if kind == RuntimeFilterEnvelopeKind::Contribution {
                    let result = producer.fail(ProducerFailureReason::ExecutionFailed);
                    reentrant_tx
                        .send(result)
                        .expect("report same-thread reentrant fail");
                }
            })
        });

        let (submit_tx, submit_rx) = mpsc::sync_channel(1);
        let submit = std::thread::spawn(move || {
            let result = producer.submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([7]), false),
            );
            submit_tx.send(result).expect("report outer submit");
        });

        assert_eq!(
            reentrant_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("same-thread reentrant fail must not deadlock")
                .unwrap(),
            SubmitOutcome::TerminalNoop
        );
        assert_eq!(
            submit_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("outer submit must complete after deferred fail-open")
                .unwrap(),
            SubmitOutcome::TerminalNoop
        );
        submit.join().unwrap();
        assert_eq!(
            fixture
                .sink
                .envelopes()
                .iter()
                .map(|(_, envelope)| envelope.kind())
                .collect::<Vec<_>>(),
            vec![
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
            ]
        );
        assert!(fixture.service.lifecycle.is_running());
    }

    #[test]
    fn remote_same_thread_async_mark_failed_is_deferred_without_deadlock() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        let state = fixture
            .service
            .remote_producer_states
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(BindingId::new(10), uid(10)))
            .cloned()
            .expect("open remote producer freezes shared state");
        let (marked_tx, marked_rx) = mpsc::sync_channel(1);
        fixture.sink.set_before_send(Arc::new(move |kind| {
            if kind == RuntimeFilterEnvelopeKind::Contribution {
                state.mark_failed();
                marked_tx
                    .send(())
                    .expect("report same-thread async failure mark");
            }
        }));

        let (submit_tx, submit_rx) = mpsc::sync_channel(1);
        let submit = std::thread::spawn(move || {
            let result = producer.submit(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ValueDomainDelta::new(MembershipValues::int64([7]), false),
            );
            submit_tx.send(result).expect("report outer submit");
        });
        marked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("same-thread async mark_failed must not wait on its owner");
        assert_eq!(
            submit_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("outer submit completes after deferred async failure")
                .unwrap(),
            SubmitOutcome::TerminalNoop
        );
        submit.join().unwrap();
        assert_eq!(
            fixture
                .sink
                .envelopes()
                .iter()
                .map(|(_, envelope)| envelope.kind())
                .collect::<Vec<_>>(),
            vec![RuntimeFilterEnvelopeKind::Contribution]
        );
        assert!(fixture.service.lifecycle.is_running());
    }

    #[test]
    fn remote_panicking_send_drops_permit_and_wakes_waiting_fail() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        let producer = open_remote_membership(&fixture, 1).unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        fixture.sink.set_before_send({
            let released = Arc::clone(&released);
            Arc::new(move |kind| {
                if kind != RuntimeFilterEnvelopeKind::Contribution {
                    return;
                }
                entered_tx.send(()).expect("submit send entered");
                let (lock, wake) = &*released;
                let mut ready = lock.lock().unwrap_or_else(|error| error.into_inner());
                while !*ready {
                    ready = wake.wait(ready).unwrap_or_else(|error| error.into_inner());
                }
                panic!("intentional remote sink panic");
            })
        });

        let submit = {
            let producer = Arc::clone(&producer);
            std::thread::spawn(move || {
                producer.submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
            })
        };
        entered_rx.recv().expect("submit reached the send barrier");
        let (fail_tx, fail_rx) = mpsc::sync_channel(1);
        let fail = std::thread::spawn(move || {
            fail_tx
                .send(producer.fail(ProducerFailureReason::ExecutionFailed))
                .expect("report waiting fail result");
        });
        assert!(
            fail_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "concurrent fail must wait while the operation permit is owned"
        );
        {
            let (lock, wake) = &*released;
            *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
            wake.notify_all();
        }
        assert!(
            submit.join().is_err(),
            "sink panic must unwind outer submit"
        );
        assert_eq!(
            fail_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("permit Drop must wake a waiting fail")
                .unwrap(),
            SubmitOutcome::TerminalNoop
        );
        fail.join().unwrap();
        assert!(fixture.sink.envelopes().is_empty());
    }

    #[test]
    fn remote_bloom_only_retirement_hit_fails_open_without_query_error_or_enqueue() {
        let fixture = remote_producer_fixture(DataType::Int64, 1024);
        fixture
            .service
            .reliable_transport()
            .saturate_retired_filter_for_test();
        let producer = open_remote_membership(&fixture, 1).unwrap();
        assert_eq!(
            producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int64([7]), false),
                )
                .expect("probabilistic retirement is never a query-facing error"),
            SubmitOutcome::TerminalNoop
        );
        assert!(fixture.sink.envelopes().is_empty());
        assert!(fixture.service.lifecycle.is_running());
    }

    fn producer_transport_envelope(kind: RuntimeFilterEnvelopeKind) -> Arc<RuntimeFilterEnvelope> {
        let (identity, producer_open, payload) = match kind {
            RuntimeFilterEnvelopeKind::Contribution => (
                RuntimeFilterRouteIdentity::contribution(
                    ContributionRouteIdentity::try_new(
                        BindingId::new(10),
                        uid(10),
                        PartitionId::new(0),
                        ProducerSequence::new(0),
                    )
                    .unwrap(),
                ),
                Some(ProducerOpenMetadata::try_new(1).unwrap()),
                vec![1],
            ),
            RuntimeFilterEnvelopeKind::ProducerUnavailable => (
                RuntimeFilterRouteIdentity::producer_instance(
                    ProducerInstanceRouteIdentity::try_new(BindingId::new(10), uid(10)).unwrap(),
                ),
                None,
                encode_producer_failure(ProducerFailureReason::UpstreamUnavailable),
            ),
            other => panic!("unsupported producer transport fixture kind {other:?}"),
        };
        Arc::new(
            RuntimeFilterEnvelope::try_new(
                kind,
                uid(0),
                ChannelId::new(1),
                DeploymentEpoch::new(9),
                identity,
                producer_open,
                None,
                &[7; 32],
                payload,
            )
            .unwrap(),
        )
    }

    fn assert_service_failed_open_once(fx: &ProducerFailedOpenFixture) {
        let channel = fx.service.registry.channel(ChannelId::new(1)).unwrap();
        assert!(channel.is_terminal());
        assert!(matches!(
            fx.service
                .subscribe(
                    BindingId::new(30),
                    uid(30),
                    SubscriptionKind::BlockingSnapshot,
                )
                .unwrap()
                .into_blocking()
                .unwrap()
                .acquire(Duration::ZERO),
            ArtifactAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed)
        ));
        let failed_events = fx
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ProducerInstanceFailed { .. }))
            .count();
        assert_eq!(failed_events, 1, "producer instance fails exactly once");
        assert!(fx.service.lifecycle.is_running());
        assert!(fx.service.registry.active_installation().is_some());
    }

    fn ack_synthesized_unavailable(fx: &ProducerFailedOpenFixture) {
        let unavailable = fx
            .remote_sink
            .envelopes()
            .into_iter()
            .map(|(_, envelope)| envelope)
            .find(|envelope| envelope.kind() == RuntimeFilterEnvelopeKind::ProducerUnavailable)
            .expect("failed contribution synthesizes producer unavailable");
        fx.remote_sink.complete(SinkCompletion::Ack(
            unavailable.route_identity().clone(),
            RuntimeFilterAcceptStatus::Accepted,
        ));
        fx.service.tick(fx.started + Duration::from_millis(201));
        assert_eq!(fx.service.transport_pending_len_for_test(), 0);
    }

    #[test]
    fn async_rejected_or_mismatched_ack_fails_route_open() {
        for mismatch in [false, true] {
            let fx = producer_failed_open_fixture();
            let envelope = producer_transport_envelope(RuntimeFilterEnvelopeKind::Contribution);
            let identity = envelope.route_identity().clone();
            assert!(matches!(
                fx.service.reliable_transport().send_envelope(
                    &fx.route,
                    envelope,
                    fx.event_identity,
                ),
                Ok(ReliableSendOutcome::Buffered(_))
            ));
            if mismatch {
                fx.remote_sink.complete(SinkCompletion::TransportFailure(
                    identity,
                    SinkTransportError::contract("runtime filter ACK identity mismatch"),
                ));
            } else {
                fx.remote_sink.complete(SinkCompletion::Ack(
                    identity,
                    RuntimeFilterAcceptStatus::Rejected,
                ));
            }

            fx.service.tick(fx.started);
            assert_service_failed_open_once(&fx);
            ack_synthesized_unavailable(&fx);
            fx.service.tick(fx.started + Duration::from_secs(1));
            assert_service_failed_open_once(&fx);
        }
    }

    #[test]
    fn contribution_deadline_degrades_filter_without_query_failure() {
        let fx = producer_failed_open_fixture();
        let envelope = producer_transport_envelope(RuntimeFilterEnvelopeKind::Contribution);
        assert!(matches!(
            fx.service
                .reliable_transport()
                .send_envelope(&fx.route, envelope, fx.event_identity,),
            Ok(ReliableSendOutcome::Buffered(_))
        ));

        fx.service.tick(fx.started + Duration::from_millis(200));
        assert_service_failed_open_once(&fx);
        ack_synthesized_unavailable(&fx);
        fx.service.tick(fx.started + Duration::from_secs(1));
        assert_service_failed_open_once(&fx);
    }

    #[test]
    fn producer_unavailable_send_failure_does_not_recurse() {
        let fx = producer_failed_open_fixture();
        let envelope = producer_transport_envelope(RuntimeFilterEnvelopeKind::ProducerUnavailable);
        let identity = envelope.route_identity().clone();
        assert!(matches!(
            fx.service
                .reliable_transport()
                .send_envelope(&fx.route, envelope, fx.event_identity,),
            Ok(ReliableSendOutcome::Buffered(_))
        ));
        fx.remote_sink.complete(SinkCompletion::TransportFailure(
            identity,
            SinkTransportError::contract("runtime filter ACK identity mismatch"),
        ));

        fx.service.tick(fx.started);
        assert_service_failed_open_once(&fx);
        assert_eq!(fx.service.transport_pending_len_for_test(), 0);
        assert_eq!(fx.remote_sink.envelopes().len(), 1);
        for step in 1..=3 {
            fx.service.tick(fx.started + Duration::from_secs(step));
        }
        assert_eq!(fx.service.transport_pending_len_for_test(), 0);
        assert_eq!(
            fx.remote_sink.envelopes().len(),
            1,
            "failed ProducerUnavailable must not synthesize another ProducerUnavailable"
        );
        assert_service_failed_open_once(&fx);
    }

    #[test]
    fn reliable_transport_fixture_send_then_ack_releases_and_records_the_lifecycle() {
        use crate::runtime_filter::port::transport::{
            DeliveryRouteIdentity, RuntimeFilterAcceptStatus, RuntimeFilterRouteIdentity,
        };

        let fx = reliable_transport_fixture();
        let bundle = service_outbound_delivery_membership_bundle(&fx.profile, fx.channel_id);

        // Deliver through the production bridge to the compiler-authorized remote edge.
        let decision = fx
            .service
            .deliver_artifact(
                fx.channel_id,
                &fx.profile,
                vec![fx.remote_delivery_edge],
                ArtifactDeliveryOutcome::Published(bundle),
            )
            .unwrap();

        // Exactly one remote route, one wire frame, one buffered in-flight entry.
        assert!(decision.loopback_route_edge_ids().is_empty());
        assert_eq!(decision.remote_routes().len(), 1);
        let frames = fx.remote_sink.frames();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0, fx.remote_delivery_edge);
        let wire_bytes = frames[0].1.payload().len();
        assert_eq!(fx.service.reliable_transport().pending_len(), 1);

        // A "sent" event flowed through the SAME lifecycle sink, keyed by the route and
        // carrying the serialized byte size.
        let sent = transport_events_of(&fx.events);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].1, TransportEventKind::Sent);
        assert_eq!(sent[0].2, wire_bytes);
        assert_eq!(sent[0].0.route_edge_id(), fx.remote_delivery_edge);
        assert_eq!(sent[0].0.common().query_id(), uid(0));
        assert_eq!(sent[0].0.common().channel_id(), fx.channel_id);

        // The transport stamps sequences from 1 on a fresh query, so the first (only)
        // remote send owns delivery sequence 1. Synthesize its Accepted ack.
        let identity = RuntimeFilterRouteIdentity::delivery(
            DeliveryRouteIdentity::try_new(fx.remote_delivery_edge, ProducerSequence::new(1))
                .unwrap(),
        );
        assert_eq!(
            fx.service
                .reliable_transport()
                .on_ack(&identity, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released,
        );
        assert_eq!(fx.service.reliable_transport().pending_len(), 0);

        // An "acked" event carrying the Accepted status was recorded.
        let acked: Vec<_> = transport_events_of(&fx.events)
            .into_iter()
            .filter(|(_, kind, _)| matches!(kind, TransportEventKind::Acked(_)))
            .collect();
        assert_eq!(acked.len(), 1);
        assert_eq!(
            acked[0].1,
            TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted),
        );

        // No hang: a bounded tick far in the future neither retries nor fails open.
        assert!(
            fx.service
                .reliable_transport()
                .drive_retries(fx.started + Duration::from_secs(60))
                .is_quiescent()
        );
    }

    #[test]
    fn reliable_transport_fixture_missing_ack_retries_then_deadline_fails_open() {
        let fx = reliable_transport_fixture();
        let bundle = service_outbound_delivery_membership_bundle(&fx.profile, fx.channel_id);
        fx.service
            .deliver_artifact(
                fx.channel_id,
                &fx.profile,
                vec![fx.remote_delivery_edge],
                ArtifactDeliveryOutcome::Published(bundle),
            )
            .unwrap();
        assert_eq!(fx.remote_sink.frames().len(), 1);
        assert_eq!(fx.service.reliable_transport().pending_len(), 1);

        // Never ack. Drive explicit retry ticks on the manual clock (no real waiting, no
        // hang). The default policy bounds the attempt count, so retries are capped.
        let mut retried = 0;
        for step in 1..=6u32 {
            let now = fx.started + Duration::from_millis(200 * u64::from(step));
            retried += fx.service.reliable_transport().drive_retries(now).retried();
        }
        // Bounded at-least-once: the initial send plus the capped retries, no storm.
        let total_sends = fx.remote_sink.frames().len();
        assert_eq!(retried, 4, "retries are bounded by the attempt count");
        assert_eq!(total_sends, 5, "initial send + 4 bounded retries");
        let retried_events = transport_events_of(&fx.events)
            .into_iter()
            .filter(|(_, kind, _)| matches!(kind, TransportEventKind::Retried))
            .count();
        assert_eq!(retried_events, 4);
        // Still buffered before the deadline; the query has NOT errored.
        assert_eq!(fx.service.reliable_transport().pending_len(), 1);

        // Cross the deadline: the frame is released and the route fails open — no panic,
        // no error surfaced to the query.
        let tick = fx
            .service
            .reliable_transport()
            .drive_retries(fx.started + Duration::from_secs(31));
        assert_eq!(tick.failed_open().len(), 1);
        assert_eq!(fx.service.reliable_transport().pending_len(), 0);
        let failed = transport_events_of(&fx.events)
            .into_iter()
            .filter(|(_, kind, _)| {
                matches!(
                    kind,
                    TransportEventKind::FailedOpen(TransportFailOpenReason::Deadline)
                )
            })
            .count();
        assert_eq!(failed, 1);

        // No hang: further ticks are quiescent and nothing is re-sent past the deadline.
        assert!(
            fx.service
                .reliable_transport()
                .drive_retries(fx.started + Duration::from_secs(120))
                .is_quiescent()
        );
        assert_eq!(fx.remote_sink.frames().len(), 5);
    }

    #[test]
    fn reliable_transport_fixture_duplicate_wire_identity_answers_duplicate() {
        use crate::runtime::query_context::{QueryContextManager, QueryId};
        use crate::runtime_filter::codec::artifact::{
            encode_artifact_bundle, max_encoded_len_for_artifact_budget, semantic_artifact_bytes,
        };
        use crate::runtime_filter::port::transport::{
            DeliveryRouteIdentity, RuntimeFilterAcceptStatus, RuntimeFilterEnvelope,
            RuntimeFilterRouteIdentity,
        };
        use crate::service::runtime_filter_envelope_ingress::query_scoped_runtime_filter_envelope_ingress_with_manager;

        let fixture = compiler_consumer_delivery_fixture();
        const QUERY: QueryId = QueryId::new(81, 82);
        let query_uid = UniqueId::new(81, 82);
        let epoch = fixture.install.epoch();

        let manager = QueryContextManager::new_for_test();
        manager
            .get_or_register_native(
                QUERY,
                false,
                Duration::from_secs(30),
                Duration::from_secs(30),
            )
            .expect("register native query context");
        let service = manager
            .runtime_filter_service_for_ingress(QUERY)
            .expect("registered query exposes a runtime filter service");
        assert_eq!(
            service.install(fixture.install).unwrap(),
            InstallOutcome::Installed
        );

        let bundle =
            service_outbound_delivery_membership_bundle(&fixture.profile, fixture.channel_id);
        let ceiling =
            max_encoded_len_for_artifact_budget(semantic_artifact_bytes(&bundle).unwrap()).unwrap();
        let (digest, payload) = encode_artifact_bundle(
            &bundle,
            ArtifactDecodeExpectation::new(&fixture.profile),
            ceiling,
        )
        .unwrap()
        .into_parts();

        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager.clone());
        // Same wire route identity (edge + transport sequence) each time.
        let envelope = || {
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::Artifact,
                query_uid,
                fixture.channel_id,
                epoch,
                RuntimeFilterRouteIdentity::delivery(
                    DeliveryRouteIdentity::try_new(
                        fixture.delivery_route_edge,
                        ProducerSequence::new(1),
                    )
                    .unwrap(),
                ),
                None,
                None,
                &digest,
                payload.clone(),
            )
            .unwrap()
        };

        // The first arrival is accepted; an exact at-least-once wire retry (identical
        // route edge + transport sequence) is absorbed as Duplicate, never re-applied.
        assert_eq!(
            ingress.accept(envelope()).accept_status(),
            RuntimeFilterAcceptStatus::Accepted,
        );
        assert_eq!(
            ingress.accept(envelope()).accept_status(),
            RuntimeFilterAcceptStatus::Duplicate,
        );
    }

    #[test]
    fn reliable_transport_fixture_out_of_order_arrival_absorbed_by_logical_identity() {
        use crate::runtime::query_context::{QueryContextManager, QueryId};
        use crate::runtime_filter::codec::artifact::{
            encode_artifact_bundle, max_encoded_len_for_artifact_budget, semantic_artifact_bytes,
        };
        use crate::runtime_filter::port::transport::{
            DeliveryRouteIdentity, RuntimeFilterAcceptStatus, RuntimeFilterEnvelope,
            RuntimeFilterRouteIdentity,
        };
        use crate::service::runtime_filter_envelope_ingress::query_scoped_runtime_filter_envelope_ingress_with_manager;

        let fixture = compiler_consumer_delivery_fixture();
        const QUERY: QueryId = QueryId::new(91, 92);
        let query_uid = UniqueId::new(91, 92);
        let epoch = fixture.install.epoch();

        let manager = QueryContextManager::new_for_test();
        manager
            .get_or_register_native(
                QUERY,
                false,
                Duration::from_secs(30),
                Duration::from_secs(30),
            )
            .expect("register native query context");
        let service = manager
            .runtime_filter_service_for_ingress(QUERY)
            .expect("registered query exposes a runtime filter service");
        assert_eq!(
            service.install(fixture.install).unwrap(),
            InstallOutcome::Installed
        );

        let bundle =
            service_outbound_delivery_membership_bundle(&fixture.profile, fixture.channel_id);
        let ceiling =
            max_encoded_len_for_artifact_budget(semantic_artifact_bytes(&bundle).unwrap()).unwrap();
        let (digest, payload) = encode_artifact_bundle(
            &bundle,
            ArtifactDecodeExpectation::new(&fixture.profile),
            ceiling,
        )
        .unwrap()
        .into_parts();

        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager.clone());
        // Same logical artifact (same `(route_edge, version)`) arriving under two DIFFERENT
        // transport sequences, out of order (the later sequence first).
        let arrival = |sequence: u64| {
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::Artifact,
                query_uid,
                fixture.channel_id,
                epoch,
                RuntimeFilterRouteIdentity::delivery(
                    DeliveryRouteIdentity::try_new(
                        fixture.delivery_route_edge,
                        ProducerSequence::new(sequence),
                    )
                    .unwrap(),
                ),
                None,
                None,
                &digest,
                payload.clone(),
            )
            .unwrap()
        };

        // Transport sequence 2 arrives first and is accepted.
        assert_eq!(
            ingress.accept(arrival(2)).accept_status(),
            RuntimeFilterAcceptStatus::Accepted,
        );
        // Transport sequence 1 arrives afterward (out of order): the transport-identity gate
        // admits the fresh sequence, but the stable logical `(route_edge, version)` identity
        // absorbs it as Duplicate — never delivered or applied twice.
        assert_eq!(
            ingress.accept(arrival(1)).accept_status(),
            RuntimeFilterAcceptStatus::Duplicate,
        );
    }

    #[test]
    fn transport_events_resource_limit_seam_emits_failed_open_through_the_lifecycle_sink() {
        // The Task-4 resource-limit seam now emits a structured FailedOpen(ResourceLimit)
        // event through the SAME lifecycle sink the buffered transport steps use — closing
        // Task 4's flagged no-op seam. The event coarsens the specific ceiling into
        // ResourceLimit (distinct from the deadline fail-open) and carries the frame bytes.
        let events = Arc::new(Events::default());
        let service = RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            events.clone(),
            MemTrackerMemoryAccount::new_root_for_test("transport-resource-limit-seam"),
        );
        let identity = TransportRouteEventIdentity::new(
            RuntimeFilterEventIdentity::new(
                uid(0),
                RuntimeFilterParticipantId::new(2),
                ChannelId::new(5),
                DeploymentEpoch::new(9),
            ),
            RouteEdgeId::new(4),
        );

        service.record_transport_resource_limit(
            identity,
            TransportResourceLimit::PendingEntries,
            128,
        );

        assert_eq!(
            transport_events_of(&events),
            vec![(
                identity,
                TransportEventKind::FailedOpen(TransportFailOpenReason::ResourceLimit),
                128,
            )],
        );
    }

    #[test]
    fn resource_limit_event_gap_suppresses_callback_after_shutdown_wins() {
        let fixture = fixture();
        fixture
            .service
            .configure_transport(super::ReliableTransportPolicy::new(
                Duration::from_millis(10),
                2,
                Duration::from_secs(1),
                1,
                1 << 20,
            ))
            .expect("one-entry transport policy");
        let routes = install_outbound_delivery_aggregator(
            &fixture.service,
            &[(20, 20, 200)],
            &[(30, 30, 7, "10.0.0.7"), (40, 31, 8, "10.0.0.8")],
        );
        fixture
            .service
            .set_remote_sink_for_test(Arc::new(RecordingRemoteSink::default()));
        let bundle =
            service_outbound_delivery_membership_bundle(&routes.profile, routes.channel_id);
        let (gap_entered_tx, gap_entered_rx) = mpsc::sync_channel(1);
        let (gap_release_tx, gap_release_rx) = mpsc::sync_channel(1);
        let gap_release_rx = Mutex::new(gap_release_rx);
        fixture
            .service
            .set_before_resource_limit_event_admission_hook(Arc::new(move || {
                gap_entered_tx.send(()).expect("resource event gap entered");
                gap_release_rx
                    .lock()
                    .expect("resource event gap")
                    .recv()
                    .expect("release resource event gap");
            }));

        let delivery_service = fixture.service.clone();
        let delivery_profile = routes.profile.clone();
        let delivery = std::thread::spawn(move || {
            delivery_service.deliver_artifact(
                routes.channel_id,
                &delivery_profile,
                routes.remote_edges,
                ArtifactDeliveryOutcome::Published(bundle),
            )
        });
        gap_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("resource-limit event reached pre-admission gap");

        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown_service = fixture.service.clone();
        let shutdown = std::thread::spawn(move || {
            shutdown_service.shutdown();
            shutdown_done_tx.send(()).expect("shutdown complete");
        });
        shutdown_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown wins before second event admission");
        gap_release_tx.send(()).expect("release event gap");
        delivery.join().expect("delivery thread").expect("delivery");
        shutdown.join().expect("shutdown thread");

        assert!(
            transport_events_of(&fixture.events)
                .iter()
                .all(|(_, kind, _)| {
                    !matches!(
                        kind,
                        TransportEventKind::FailedOpen(TransportFailOpenReason::ResourceLimit)
                    )
                }),
            "shutdown must suppress the not-yet-admitted resource-limit callback"
        );
    }

    #[test]
    fn shutdown_waits_for_admitted_resource_limit_event_callback() {
        let (callback_entered_tx, callback_entered_rx) = mpsc::sync_channel(1);
        let (callback_release_tx, callback_release_rx) = mpsc::sync_channel(1);
        let events = Arc::new(BlockingResourceLimitEvents {
            entered: callback_entered_tx,
            release: Mutex::new(Some(callback_release_rx)),
            recorded: Mutex::new(Vec::new()),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            events,
            MemTrackerMemoryAccount::new_root_for_test("resource-limit-admission"),
        ));
        let identity = TransportRouteEventIdentity::new(
            RuntimeFilterEventIdentity::new(
                uid(0),
                RuntimeFilterParticipantId::new(3),
                ChannelId::new(1),
                DeploymentEpoch::new(9),
            ),
            RouteEdgeId::new(30),
        );

        let delivery_service = service.clone();
        let delivery = std::thread::spawn(move || {
            delivery_service.record_transport_resource_limit(
                identity,
                TransportResourceLimit::PendingEntries,
                128,
            );
        });
        callback_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("resource-limit callback entered");
        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown_service = service.clone();
        let shutdown = std::thread::spawn(move || {
            shutdown_service.shutdown();
            shutdown_done_tx.send(()).expect("shutdown complete");
        });
        let shutdown_returned_early = shutdown_done_rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok();

        callback_release_tx
            .send(())
            .expect("release resource-limit callback");
        if !shutdown_returned_early {
            shutdown_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown joins after callback");
        }
        delivery.join().expect("delivery thread");
        shutdown.join().expect("shutdown thread");
        assert!(
            !shutdown_returned_early,
            "shutdown returned while an admitted resource-limit callback was active"
        );
    }

    #[test]
    fn resource_limit_event_reentrant_shutdown_is_deferred_by_service_permit() {
        let events = Arc::new(ReentrantResourceLimitEvents {
            service: Mutex::new(Weak::new()),
            observed_closed: AtomicBool::new(false),
            fired: AtomicBool::new(false),
        });
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            uid(0),
            Arc::new(Clock(Instant::now())),
            events.clone(),
            MemTrackerMemoryAccount::new_root_for_test("resource-limit-reentry"),
        ));
        *events
            .service
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Arc::downgrade(&service);
        let identity = TransportRouteEventIdentity::new(
            RuntimeFilterEventIdentity::new(
                uid(0),
                RuntimeFilterParticipantId::new(3),
                ChannelId::new(1),
                DeploymentEpoch::new(9),
            ),
            RouteEdgeId::new(30),
        );

        service.record_transport_resource_limit(
            identity,
            TransportResourceLimit::PendingEntries,
            128,
        );

        assert!(events.fired.load(Ordering::Acquire));
        assert!(
            !events.observed_closed.load(Ordering::Acquire),
            "reentrant shutdown must defer while the resource event owns a ServiceCall"
        );
        assert_eq!(
            service
                .lifecycle
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .phase,
            super::LifecyclePhase::Closed,
        );
    }
}
