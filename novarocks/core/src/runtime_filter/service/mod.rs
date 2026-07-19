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

mod materialization;
mod memory;
mod producer;
// `registry` is `pub(crate)` (rather than private) solely so RFD-2's
// deployment-compiler tests can reach `registry::validate_view_for_test`
// (see registry.rs) to prove compiler output satisfies the BE install
// contract. Item-level privacy inside `registry.rs` is unaffected.
#[cfg(test)]
mod m3a_tests;
#[cfg(test)]
mod m3b_tests;
#[cfg(test)]
mod m3c_tests;
#[cfg(test)]
mod m4_conformance_tests;
pub(crate) mod registry;
mod subscription;

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::ThreadId;
use std::time::Instant;

use crate::common::types::UniqueId;
use crate::runtime_filter::core::channel::ChannelAction;
use crate::runtime_filter::model::contract::{BindingId, ChannelId, ConsumerActivation};
use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
use crate::runtime_filter::port::install::RuntimeFilterParticipantInstall;
use crate::runtime_filter::port::producer::{
    FinalDomainProducerAdapter, InstallContractError, InstallOutcome, OrderedBoundProducerAdapter,
    ProducerAdapter, ProducerHandle, ProducerHandleWeak, ProducerPortKind,
    RuntimeContractViolation, RuntimeContractViolationKind, TopKSummaryProducerAdapter,
};
use crate::runtime_filter::port::subscription::{
    ArtifactDeliveryOutcome, LiveTerminal, SubscriptionHandle, SubscriptionKind,
};
use crate::runtime_filter::port::support::{RuntimeFilterClock, RuntimeFilterMemoryAccount};

#[cfg(test)]
use self::materialization::run_materialization_jobs;
use self::materialization::{
    ClaimedMaterializationJob, MaterializationWorkClaim, PublishCommitOutcome,
    claim_materialization_jobs, execute_materialization_jobs, take_materialization_launch_events,
};
use self::producer::ServiceProducerAdapter;
use self::registry::{DeploymentRegistry, InstalledDeployment};

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
                self.sink.record(event);
            }));
            if let Some(completion) = completion {
                completion.complete();
            }
            state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        }
    }
}

impl RuntimeFilterEventSink for EventEmitter {
    fn record(&self, event: RuntimeFilterEvent) {
        self.record_all([event]);
    }
}

struct ActionDispatcher {
    registry: Arc<DeploymentRegistry>,
    events: Arc<EventEmitter>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
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
                            Some(outcome)
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
                        deliveries.push((
                            group.route_edges().to_vec(),
                            Some(ArtifactDeliveryOutcome::Unavailable(*reason)),
                            Some(LiveTerminal::Unavailable(*reason)),
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
        (batch, error)
    }
}

pub(crate) struct RuntimeFilterService {
    _query_id: UniqueId,
    _clock: Arc<dyn RuntimeFilterClock>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    registry: Arc<DeploymentRegistry>,
    dispatcher: Arc<ActionDispatcher>,
    producer_handles: Mutex<BTreeMap<(BindingId, UniqueId), ProducerHandleWeak>>,
    #[cfg(test)]
    producer_test_handles: Mutex<BTreeMap<(BindingId, UniqueId), Weak<ServiceProducerAdapter>>>,
    operation: Mutex<()>,
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
        let dispatcher = Arc::new(ActionDispatcher {
            registry: registry.clone(),
            events: events.clone(),
            memory_account: memory_account.clone(),
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
            producer_handles: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            producer_test_handles: Mutex::new(BTreeMap::new()),
            operation: Mutex::new(()),
        }
    }

    pub(crate) fn install(
        &self,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<InstallOutcome, InstallContractError> {
        let result = self.registry.install(install)?;
        let outcome = result.outcome();
        Ok(outcome)
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
                return Ok(handle);
            }
        }
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
                    .derive(binding_id, fragment_instance_id)?,
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
        Ok(handle)
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

    pub(crate) fn expire_deadlines(&self, now: Instant) {
        let installed = {
            let _operation = self
                .operation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.registry.active_installation()
        };
        if let Some(installed) = installed {
            for (channel_id, channel) in installed.channels() {
                let action = channel.expire_deadline(now);
                if !matches!(action, ChannelAction::None) {
                    let _ = self.dispatcher.dispatch(channel_id, action);
                }
            }
        }
    }

    pub(crate) fn cancel(&self) {
        let installed = {
            let _operation = self
                .operation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.registry.cancel()
        };
        if let Some(cancelled) = installed {
            for (channel_id, channel) in cancelled.installed().channels() {
                let action = channel.cancel();
                let action = if matches!(action, ChannelAction::None) {
                    channel.terminal_action()
                } else {
                    action
                };
                let barrier = self.dispatcher.dispatch_nonblocking(channel_id, action);
                cancelled.arm_artifact_cancellation(channel_id, barrier);
            }
            cancelled.deliver_artifact_cancellation();
        }
    }

    pub(crate) fn shutdown(&self) {
        self.cancel();
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Condvar, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::coordinator::cluster::LiveBackendSnapshot;
    use crate::coordinator::scheduler::{FragmentInstancePlacement, SchedulingPlan};
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::deployment::compiler::compile;
    use crate::runtime_filter::deployment::extension::RuntimeFilterDeploymentExtension;
    use crate::runtime_filter::deployment::{
        RuntimeFilterDeploymentPlan, RuntimeFilterDeploymentPolicy,
    };
    use crate::runtime_filter::materializer::codec::{ArtifactDecodeExpectations, decode_leaf};
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::model::graph::{
        ApplyPoint, ConsumerRequirement, PlanLocation, ProducerRequirement,
        RuntimeFilterBindingRole, RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
        RuntimeFilterGraph,
    };
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ConsumerArtifactProfile, ConsumerProfileId, PhysicalArtifact,
    };
    use crate::runtime_filter::port::events::{
        ConsumerEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity,
        RuntimeFilterEventSink,
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
        RuntimeContractViolationKind, SubmitOutcome,
    };
    use crate::runtime_filter::port::routing::RuntimeFilterRouteRole;
    use crate::runtime_filter::port::subscription::{
        ArtifactAcquireOutcome, ArtifactDelivery, ArtifactDeliveryOutcome,
        BlockingSnapshotSubscription, SubscriptionKind, UnavailableReason,
    };
    use crate::runtime_filter::port::support::{
        MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
        TemporaryContributionLease,
    };
    use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain, ValueDomainDelta,
    };
    use crate::runtime_filter::router::loopback::LoopbackRouter;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::planner::distributed::{
        DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
    };

    use super::materialization::MaterializationWorkClaim;
    use super::memory::MemTrackerMemoryAccount;
    use super::subscription::SubscriptionGroup;
    use super::{
        ActionDispatcher, ChannelAction, EventBatchCompletion, EventEmitter, PendingDispatch,
        RuntimeFilterService, run_materialization_jobs,
    };

    #[derive(Default)]
    struct Events(Mutex<Vec<RuntimeFilterEvent>>);

    impl RuntimeFilterEventSink for Events {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0.lock().unwrap().push(event);
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
        UniqueId { hi: 70, lo }
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
                    RouteEdgeId::new(route_edge),
                    consumer_instances.into_iter().map(uid).collect(),
                ),
            )]),
        )
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
                    RouteEdgeId::new(40),
                    BTreeSet::from([uid(30)]),
                ),
            )]),
        )
    }

    fn compiled_fenced_final_install() -> RuntimeFilterParticipantInstall {
        let deployment = fenced_final_deployment();
        let expression = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        };
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_channel(RuntimeFilterChannelSpec {
                channel_id: deployment.channel_id(),
                logical_domain: deployment.logical_domain().clone(),
                lifecycle: deployment.lifecycle(),
                availability_coverage: deployment.availability_coverage().clone(),
                terminal_coverage: deployment.terminal_coverage().clone(),
                reduction_requirement: deployment.reduction_requirement(),
                allowed_contribution_kinds: deployment.allowed_contribution_kinds().clone(),
                required_consumer_capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                policy: deployment.policy(),
            })
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: BindingId::new(10),
                channel_id: ChannelId::new(1),
                coverage_witness_id: Some(CoverageWitnessId::new(101)),
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(0),
                    node_id: PlanNodeId::new(1),
                },
                expression: expression.clone(),
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                    contribution_kinds: BTreeSet::from([
                        ContributionKind::FinalDomainShard,
                        ContributionKind::ProducerClosed,
                    ]),
                    completion_requirement: CompletionRequirement::FencedFinalDomain(
                        CompletionFenceKind::CommittedDomainFrozen,
                    ),
                    join_key_ordinal: 0,
                }),
            })
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: BindingId::new(30),
                channel_id: ChannelId::new(1),
                coverage_witness_id: None,
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(0),
                    node_id: PlanNodeId::new(2),
                },
                expression,
                apply_point: ApplyPoint::NodeInput,
                role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                    capabilities: BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    activation: ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
                    },
                    target:
                        crate::runtime_filter::model::graph::ConsumerBindingTarget::SourceBoundary,
                }),
            })
            .unwrap();
        let placement = FragmentInstancePlacement {
            fragment_id: 0,
            instance_index: 0,
            finst_id: uid(10),
            backend_idx: 0,
            endpoint: RuntimeEndpoint::from_socket_addr("127.0.0.1:9060".parse().unwrap()),
            scan_ranges: BTreeMap::new(),
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
        let participant = RuntimeFilterParticipantId::new(0);
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

    fn compiled_three_backend_all_of_plan() -> RuntimeFilterDeploymentPlan {
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
                    join_key_ordinal: 0,
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
                    target:
                        crate::runtime_filter::model::graph::ConsumerBindingTarget::SourceBoundary,
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
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        };
        let local_producer = UniqueId { hi: 1, lo: 3 };
        let remote_producer = UniqueId { hi: 1, lo: 4 };
        let scheduling = SchedulingPlan {
            root_fragment_id: 1,
            by_fragment: BTreeMap::from([
                (
                    1,
                    vec![
                        placement(1, 0, 2, UniqueId { hi: 1, lo: 1 }, "10.0.0.2:9060"),
                        placement(1, 1, 11, UniqueId { hi: 1, lo: 2 }, "10.0.0.11:9060"),
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
            root_finst_id: UniqueId { hi: 1, lo: 1 },
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

    fn compiled_three_backend_all_of_aggregator_install()
    -> (RuntimeFilterParticipantInstall, BindingId, UniqueId) {
        let channel_id = ChannelId::new(5);
        let producer_binding = BindingId::new(10);
        let remote_producer = UniqueId { hi: 1, lo: 4 };
        let mut plan = compiled_three_backend_all_of_plan();
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
            Some(RuntimeFilterParticipantId::new(7))
        );
        assert_ne!(aggregator, RuntimeFilterParticipantId::new(7));
        let core_view = plan.install_views.remove(&aggregator).unwrap();
        let routing_shard = plan.routing_shards.remove(&aggregator).unwrap();
        (
            RuntimeFilterParticipantInstall::new(core_view, routing_shard),
            producer_binding,
            remote_producer,
        )
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
        let base = deployment(1, 10, 30, 40, [10], [30], 100);
        let consumers = consumers
            .into_iter()
            .map(|(binding, route, instance, profile)| {
                (
                    BindingId::new(binding),
                    ConsumerDeployment::with_profile(
                        ConsumerActivation::BlockingSnapshot,
                        BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                        profile,
                        RouteEdgeId::new(route),
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

    struct Fixture {
        service: Arc<RuntimeFilterService>,
        events: Arc<Events>,
        started: Instant,
        tracker: Arc<MemTrackerMemoryAccount>,
    }

    fn fixture() -> Fixture {
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
                max_retries: 0,
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
                    RouteEdgeId::new(1),
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
                max_retries: 0,
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
                    RouteEdgeId::new(1),
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
        let plan = compiled_three_backend_all_of_plan();
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
            RouteEdgeId::new(70),
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
            RouteEdgeId::new(71),
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
        let cancelled = events
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .unwrap();
        assert!(delta < cancelled);
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
}
