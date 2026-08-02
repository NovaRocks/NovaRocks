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

use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
#[cfg(test)]
use std::sync::{OnceLock, Weak};
use std::time::Duration;

use novarocks::runtime_filter_transition::port::events::{
    ConsumerEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink,
};
use novarocks::runtime_filter_transition::port::identity::RouteEdgeId;
use novarocks::runtime_filter_transition::port::subscription::{
    ArtifactAcquireOutcome, ArtifactDelivery, ArtifactDeliveryOutcome,
    BlockingSnapshotSubscription, LivePollOutcome, LiveTerminal, NonBlockingLiveSubscription,
    SubscriptionHandle, SubscriptionKind, UnavailableReason,
};
use novarocks_types::UniqueId;

use super::EventBatchCompletion;

enum SubscriptionState {
    Pending,
    Terminal(ArtifactDeliveryOutcome),
}

pub(super) struct SubscriptionSlot {
    identity: ConsumerEventIdentity,
    events: Arc<dyn RuntimeFilterEventSink>,
    state: Mutex<SubscriptionState>,
    cancellation_event_barrier: Mutex<Option<Arc<EventBatchCompletion>>>,
    changed: Condvar,
}

impl SubscriptionSlot {
    pub(super) fn new(
        identity: ConsumerEventIdentity,
        events: Arc<dyn RuntimeFilterEventSink>,
    ) -> Self {
        Self {
            identity,
            events,
            state: Mutex::new(SubscriptionState::Pending),
            cancellation_event_barrier: Mutex::new(None),
            changed: Condvar::new(),
        }
    }

    fn deliver(&self, outcome: ArtifactDeliveryOutcome) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if matches!(*state, SubscriptionState::Pending) {
            *state = SubscriptionState::Terminal(outcome);
        }
        drop(state);
        self.changed.notify_all();
    }

    fn current_outcome(state: &SubscriptionState) -> Option<ArtifactAcquireOutcome> {
        match state {
            SubscriptionState::Pending => None,
            SubscriptionState::Terminal(outcome) => Some(outcome.acquire_outcome()),
        }
    }

    fn arm_cancellation_event(&self, barrier: Arc<EventBatchCompletion>) {
        *self
            .cancellation_event_barrier
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(barrier);
    }

    fn emit_outcome(&self, outcome: &ArtifactAcquireOutcome) {
        let event = match outcome {
            ArtifactAcquireOutcome::Published(bundle) => RuntimeFilterEvent::SubscriptionAcquired {
                identity: self.identity,
                version: bundle.version(),
            },
            ArtifactAcquireOutcome::Unsupported(reason) => {
                RuntimeFilterEvent::SubscriptionUnsupported {
                    identity: self.identity,
                    reason: *reason,
                }
            }
            ArtifactAcquireOutcome::Unavailable(reason) => {
                RuntimeFilterEvent::SubscriptionUnavailable {
                    identity: self.identity,
                    reason: *reason,
                }
            }
            ArtifactAcquireOutcome::Cancelled => RuntimeFilterEvent::SubscriptionCancelled {
                identity: self.identity,
            },
            ArtifactAcquireOutcome::TimedOut => RuntimeFilterEvent::SubscriptionTimedOut {
                identity: self.identity,
            },
        };
        if matches!(outcome, ArtifactAcquireOutcome::Cancelled) {
            let barrier = self
                .cancellation_event_barrier
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            if let Some(barrier) = barrier {
                let events = self.events.clone();
                barrier.on_complete(move || events.record(event));
                return;
            }
        }
        self.events.record(event);
    }
}

impl BlockingSnapshotSubscription for SubscriptionSlot {
    fn acquire(&self, timeout: Duration) -> ArtifactAcquireOutcome {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        // Signal while holding `state`, immediately before the condvar wait. A
        // cancel observed after this point cannot acquire `state` until the wait
        // has atomically registered this waiter and released the mutex.
        #[cfg(test)]
        notify_native_acquire_waiter_registered_for_test(self.identity);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                matches!(state, SubscriptionState::Pending)
            })
            .unwrap_or_else(|error| error.into_inner());
        let outcome = Self::current_outcome(&state).unwrap_or(ArtifactAcquireOutcome::TimedOut);
        drop(state);
        self.emit_outcome(&outcome);
        outcome
    }

    fn snapshot(
        &self,
    ) -> Option<Arc<novarocks::runtime_filter_transition::port::artifact::ArtifactBundle>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            SubscriptionState::Terminal(ArtifactDeliveryOutcome::Published(bundle)) => {
                Some(bundle.clone())
            }
            SubscriptionState::Pending | SubscriptionState::Terminal(_) => None,
        }
    }
}

#[cfg(test)]
pub(crate) struct NativeAcquireGate {
    entered: Mutex<bool>,
    changed: Condvar,
}

#[cfg(test)]
impl NativeAcquireGate {
    fn wait_entered(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut entered = self.entered.lock().expect("native RF acquire gate lock");
        while !*entered {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, wait) = self
                .changed
                .wait_timeout(entered, remaining)
                .expect("native RF acquire gate lock");
            entered = next;
            if wait.timed_out() && !*entered {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
type NativeAcquireGateKey = (
    UniqueId,
    novarocks::runtime_filter_transition::model::contract::BindingId,
);

#[cfg(test)]
fn native_acquire_gates() -> &'static Mutex<BTreeMap<NativeAcquireGateKey, Weak<NativeAcquireGate>>>
{
    static GATES: OnceLock<Mutex<BTreeMap<NativeAcquireGateKey, Weak<NativeAcquireGate>>>> =
        OnceLock::new();
    GATES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) struct NativeAcquireGateGuard {
    key: NativeAcquireGateKey,
    gate: Arc<NativeAcquireGate>,
}

#[cfg(test)]
impl NativeAcquireGateGuard {
    pub(crate) fn wait_entered(&self, timeout: Duration) -> bool {
        self.gate.wait_entered(timeout)
    }
}

#[cfg(test)]
impl Drop for NativeAcquireGateGuard {
    fn drop(&mut self) {
        let mut gates = native_acquire_gates()
            .lock()
            .expect("native RF acquire gates lock");
        if gates
            .get(&self.key)
            .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&self.gate)))
        {
            gates.remove(&self.key);
        }
    }
}

#[cfg(test)]
pub(crate) fn install_native_acquire_gate_for_test(
    query_id: UniqueId,
    binding_id: novarocks::runtime_filter_transition::model::contract::BindingId,
) -> NativeAcquireGateGuard {
    let key = (query_id, binding_id);
    let gate = Arc::new(NativeAcquireGate {
        entered: Mutex::new(false),
        changed: Condvar::new(),
    });
    native_acquire_gates()
        .lock()
        .expect("native RF acquire gates lock")
        .insert(key, Arc::downgrade(&gate));
    NativeAcquireGateGuard { key, gate }
}

#[cfg(test)]
fn notify_native_acquire_waiter_registered_for_test(identity: ConsumerEventIdentity) {
    let key = (identity.common().query_id(), identity.consumer_binding_id());
    let gate = native_acquire_gates()
        .lock()
        .expect("native RF acquire gates lock")
        .get(&key)
        .and_then(Weak::upgrade);
    let Some(gate) = gate else {
        return;
    };
    let mut entered = gate.entered.lock().expect("native RF acquire gate lock");
    *entered = true;
    gate.changed.notify_all();
}

#[derive(Default)]
struct LiveSubscriptionState {
    latest: Option<Arc<novarocks::runtime_filter_transition::port::artifact::ArtifactBundle>>,
    terminal: Option<LiveTerminal>,
}

pub(super) struct LiveSubscriptionSlot {
    identity: ConsumerEventIdentity,
    events: Arc<dyn RuntimeFilterEventSink>,
    state: Mutex<LiveSubscriptionState>,
    cancellation_event_barrier: Mutex<Option<Arc<EventBatchCompletion>>>,
}

impl LiveSubscriptionSlot {
    pub(super) fn new(
        identity: ConsumerEventIdentity,
        events: Arc<dyn RuntimeFilterEventSink>,
    ) -> Self {
        Self {
            identity,
            events,
            state: Mutex::new(LiveSubscriptionState::default()),
            cancellation_event_barrier: Mutex::new(None),
        }
    }

    fn arm_cancellation_event(&self, barrier: Arc<EventBatchCompletion>) {
        *self
            .cancellation_event_barrier
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(barrier);
    }

    fn terminal_precedence(terminal: LiveTerminal) -> u8 {
        match terminal {
            LiveTerminal::Completed | LiveTerminal::CompletedWithoutArtifact => 0,
            LiveTerminal::DegradedArtifact(_) => 1,
            LiveTerminal::DegradedDelivery(_) => 2,
            LiveTerminal::DegradedLogical(_) => 3,
            LiveTerminal::Unavailable(_) => 4,
            LiveTerminal::Cancelled => 5,
        }
    }

    fn normalize_terminal(has_latest: bool, terminal: LiveTerminal) -> LiveTerminal {
        if has_latest {
            return terminal;
        }
        match terminal {
            LiveTerminal::DegradedArtifact(reason) | LiveTerminal::DegradedDelivery(reason) => {
                LiveTerminal::Unavailable(reason)
            }
            terminal => terminal,
        }
    }

    fn merge_terminal(current: &mut Option<LiveTerminal>, incoming: LiveTerminal) {
        if current.is_none()
            || Self::terminal_precedence(incoming)
                > Self::terminal_precedence(current.expect("live terminal is present"))
        {
            *current = Some(incoming);
        }
    }

    pub(super) fn deliver(
        &self,
        bundle: Arc<novarocks::runtime_filter_transition::port::artifact::ArtifactBundle>,
        terminal: Option<LiveTerminal>,
    ) {
        self.apply_delivery(Some(ArtifactDeliveryOutcome::Published(bundle)), terminal);
    }

    pub(super) fn terminal(&self, terminal: LiveTerminal) {
        self.apply_delivery(None, Some(terminal));
    }

    fn apply_delivery(
        &self,
        outcome: Option<ArtifactDeliveryOutcome>,
        terminal: Option<LiveTerminal>,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let previous_terminal = state.terminal;
        if let Some(outcome) = outcome {
            match outcome {
                ArtifactDeliveryOutcome::Published(bundle) => {
                    if state
                        .latest
                        .as_ref()
                        .is_none_or(|latest| bundle.version() > latest.version())
                    {
                        state.latest = Some(bundle);
                    }
                }
                ArtifactDeliveryOutcome::Unsupported(_) => {
                    let terminal = Self::normalize_terminal(
                        state.latest.is_some(),
                        LiveTerminal::DegradedArtifact(UnavailableReason::MaterializationFailed),
                    );
                    Self::merge_terminal(&mut state.terminal, terminal);
                }
                ArtifactDeliveryOutcome::Unavailable(UnavailableReason::RouteUnavailable) => {
                    let terminal = Self::normalize_terminal(
                        state.latest.is_some(),
                        LiveTerminal::DegradedDelivery(UnavailableReason::RouteUnavailable),
                    );
                    Self::merge_terminal(&mut state.terminal, terminal);
                }
                ArtifactDeliveryOutcome::Unavailable(reason) => {
                    let terminal = Self::normalize_terminal(
                        state.latest.is_some(),
                        LiveTerminal::DegradedArtifact(reason),
                    );
                    Self::merge_terminal(&mut state.terminal, terminal);
                }
                ArtifactDeliveryOutcome::Cancelled => {
                    Self::merge_terminal(&mut state.terminal, LiveTerminal::Cancelled);
                }
            }
        }
        if let Some(terminal) = terminal {
            let terminal = Self::normalize_terminal(state.latest.is_some(), terminal);
            Self::merge_terminal(&mut state.terminal, terminal);
        }
        let terminal_event = if state.terminal != previous_terminal {
            Some((
                state.terminal.expect("changed live terminal is present"),
                state.latest.as_ref().map(|bundle| bundle.version()),
            ))
        } else {
            None
        };
        drop(state);
        if let Some((terminal, retained_version)) = terminal_event {
            let event = RuntimeFilterEvent::LiveSubscriptionTerminal {
                identity: self.identity,
                terminal,
                retained_version,
            };
            if matches!(terminal, LiveTerminal::Cancelled) {
                let barrier = self
                    .cancellation_event_barrier
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                if let Some(barrier) = barrier {
                    let events = self.events.clone();
                    barrier.on_complete(move || events.record(event));
                    return;
                }
            }
            self.events.record(event);
        }
    }
}

impl NonBlockingLiveSubscription for LiveSubscriptionSlot {
    fn snapshot(
        &self,
    ) -> Option<Arc<novarocks::runtime_filter_transition::port::artifact::ArtifactBundle>> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .latest
            .clone()
    }

    fn poll_after(
        &self,
        observed: Option<novarocks::runtime_filter_transition::port::identity::LogicalVersion>,
    ) -> LivePollOutcome {
        let (latest, terminal) = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            (state.latest.clone(), state.terminal)
        };
        let outcome = match latest {
            Some(bundle) if observed.is_none_or(|version| bundle.version() > version) => {
                LivePollOutcome::Updated { bundle, terminal }
            }
            latest => LivePollOutcome::Idle {
                latest_version: latest.as_ref().map(|bundle| bundle.version()),
                terminal,
            },
        };
        let event = match &outcome {
            LivePollOutcome::Updated { bundle, terminal } => {
                RuntimeFilterEvent::LiveSubscriptionUpdated {
                    identity: self.identity,
                    version: bundle.version(),
                    terminal: *terminal,
                }
            }
            LivePollOutcome::Idle {
                latest_version,
                terminal,
            } => RuntimeFilterEvent::LiveSubscriptionIdle {
                identity: self.identity,
                latest_version: *latest_version,
                terminal: *terminal,
            },
        };
        self.events.record(event);
        outcome
    }
}

pub(super) struct SubscriptionGroup {
    route_edge_ids: BTreeSet<RouteEdgeId>,
    activation: novarocks::runtime_filter_transition::model::contract::ConsumerActivation,
    slots: BTreeMap<UniqueId, InstalledSubscriptionSlot>,
    #[cfg(test)]
    before_deliver: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    delivery_call_count: AtomicUsize,
}

enum InstalledSubscriptionSlot {
    Blocking(Arc<SubscriptionSlot>),
    Live(Arc<LiveSubscriptionSlot>),
}

impl SubscriptionGroup {
    pub(super) fn new(
        common: RuntimeFilterEventIdentity,
        binding_id: novarocks::runtime_filter_transition::model::contract::BindingId,
        activation: novarocks::runtime_filter_transition::model::contract::ConsumerActivation,
        route_edge_ids: impl IntoIterator<Item = RouteEdgeId>,
        instances: impl IntoIterator<Item = UniqueId>,
        events: Arc<dyn RuntimeFilterEventSink>,
    ) -> Self {
        let slots = instances
            .into_iter()
            .map(|instance| {
                (
                    instance,
                    match activation {
                        novarocks::runtime_filter_transition::model::contract::ConsumerActivation::BlockingSnapshot => {
                            InstalledSubscriptionSlot::Blocking(Arc::new(SubscriptionSlot::new(
                                ConsumerEventIdentity::new(common, binding_id, instance),
                                events.clone(),
                            )))
                        }
                        novarocks::runtime_filter_transition::model::contract::ConsumerActivation::NonBlockingLive { .. } => {
                            InstalledSubscriptionSlot::Live(Arc::new(LiveSubscriptionSlot::new(
                                ConsumerEventIdentity::new(common, binding_id, instance),
                                events.clone(),
                            )))
                        }
                    },
                )
            })
            .collect();
        Self {
            route_edge_ids: route_edge_ids.into_iter().collect(),
            activation,
            slots,
            #[cfg(test)]
            before_deliver: Mutex::new(None),
            #[cfg(test)]
            delivery_call_count: AtomicUsize::new(0),
        }
    }

    pub(super) fn handle(
        &self,
        instance: UniqueId,
        requested: SubscriptionKind,
    ) -> Option<SubscriptionHandle> {
        let installed = match self.activation {
            novarocks::runtime_filter_transition::model::contract::ConsumerActivation::BlockingSnapshot => {
                SubscriptionKind::BlockingSnapshot
            }
            novarocks::runtime_filter_transition::model::contract::ConsumerActivation::NonBlockingLive {
                ..
            } => SubscriptionKind::NonBlockingLive,
        };
        if installed != requested {
            return None;
        }
        let slot = self.slots.get(&instance)?;
        match (slot, requested) {
            (InstalledSubscriptionSlot::Blocking(slot), SubscriptionKind::BlockingSnapshot) => {
                Some(SubscriptionHandle::Blocking(slot.clone()))
            }
            (InstalledSubscriptionSlot::Live(slot), SubscriptionKind::NonBlockingLive) => {
                Some(SubscriptionHandle::Live(slot.clone()))
            }
            _ => None,
        }
    }

    pub(super) fn live_route_edge_ids(&self) -> Option<&BTreeSet<RouteEdgeId>> {
        matches!(
            self.activation,
            novarocks::runtime_filter_transition::model::contract::ConsumerActivation::NonBlockingLive { .. }
        )
        .then_some(&self.route_edge_ids)
    }

    pub(super) fn arm_cancellation_event(
        &self,
        route_edge_id: RouteEdgeId,
        barrier: Arc<EventBatchCompletion>,
    ) {
        if !self.route_edge_ids.contains(&route_edge_id) {
            return;
        }
        for slot in self.slots.values() {
            match slot {
                InstalledSubscriptionSlot::Blocking(slot) => {
                    slot.arm_cancellation_event(barrier.clone());
                }
                InstalledSubscriptionSlot::Live(slot) => {
                    slot.arm_cancellation_event(barrier.clone());
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn set_before_deliver_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.before_deliver.lock().unwrap() = Some(hook);
    }

    #[cfg(test)]
    pub(super) fn delivery_call_count(&self) -> usize {
        self.delivery_call_count.load(Ordering::SeqCst)
    }
}

impl ArtifactDelivery for SubscriptionGroup {
    fn deliver(&self, route_edge_id: RouteEdgeId, outcome: ArtifactDeliveryOutcome) {
        self.deliver_live(route_edge_id, Some(outcome), None);
    }

    fn deliver_live(
        &self,
        route_edge_id: RouteEdgeId,
        outcome: Option<ArtifactDeliveryOutcome>,
        terminal: Option<LiveTerminal>,
    ) {
        if !self.route_edge_ids.contains(&route_edge_id) {
            return;
        }
        #[cfg(test)]
        self.delivery_call_count.fetch_add(1, Ordering::SeqCst);
        #[cfg(test)]
        if let Some(hook) = self.before_deliver.lock().unwrap().take() {
            hook();
        }
        for slot in self.slots.values() {
            match slot {
                InstalledSubscriptionSlot::Blocking(slot) => {
                    if let Some(outcome) = outcome.clone() {
                        slot.deliver(outcome);
                    }
                }
                InstalledSubscriptionSlot::Live(slot) => {
                    slot.apply_delivery(outcome.clone(), terminal);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};

    use arrow::datatypes::DataType;

    use novarocks::runtime_filter_transition::model::contract::{
        BindingId, ChannelId, NullSemantics,
    };
    use novarocks::runtime_filter_transition::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactSchemaDigest, ConsumerArtifactProfile,
        PhysicalArtifact,
    };
    use novarocks::runtime_filter_transition::port::events::{
        ConsumerEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity,
        RuntimeFilterEventSink,
    };
    use novarocks::runtime_filter_transition::port::identity::{
        DeploymentEpoch, LogicalVersion, RuntimeFilterParticipantId,
    };
    use novarocks::runtime_filter_transition::port::subscription::{
        ArtifactAcquireOutcome, ArtifactDeliveryOutcome, BlockingSnapshotSubscription,
        LivePollOutcome, LiveTerminal, NonBlockingLiveSubscription, UnavailableReason,
    };
    use novarocks::runtime_filter_transition::port::support::{
        ArtifactRetainedBudget, ArtifactRetention, MemoryAccountError, RuntimeFilterMemoryAccount,
    };

    use super::{
        LiveSubscriptionSlot, SubscriptionSlot, install_native_acquire_gate_for_test,
        native_acquire_gates,
    };

    #[derive(Default)]
    struct NoopEvents(Mutex<Vec<RuntimeFilterEvent>>);

    impl RuntimeFilterEventSink for NoopEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    #[derive(Default)]
    struct StateCheckingEvents {
        events: Mutex<Vec<RuntimeFilterEvent>>,
        live: Mutex<Option<Weak<LiveSubscriptionSlot>>>,
    }

    impl RuntimeFilterEventSink for StateCheckingEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            if let Some(live) = self.live.lock().unwrap().as_ref().and_then(Weak::upgrade) {
                assert!(
                    live.state.try_lock().is_ok(),
                    "live event callback must run outside the subscription state lock"
                );
            }
            self.events.lock().unwrap().push(event);
        }
    }

    fn bundle(version: LogicalVersion, byte: u8) -> Arc<ArtifactBundle> {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let artifact = Arc::new(PhysicalArtifact::new_test(
            ArtifactKind::ValueSet,
            schema,
            version,
            false,
            Arc::from([byte]),
        ));
        Arc::new(
            ArtifactBundle::new(
                ChannelId::new(1),
                version,
                &profile,
                vec![(ArtifactKind::ValueSet, artifact)],
                usize::MAX,
            )
            .unwrap(),
        )
    }

    #[derive(Default)]
    struct CountingMemory(AtomicUsize);

    impl RuntimeFilterMemoryAccount for CountingMemory {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.0.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.0.fetch_sub(bytes, Ordering::SeqCst);
            assert!(previous >= bytes);
        }
    }

    fn retained_bundle(
        version: LogicalVersion,
        byte: u8,
    ) -> (
        Arc<ArtifactBundle>,
        Arc<ArtifactRetainedBudget>,
        Arc<CountingMemory>,
        usize,
    ) {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let encoded: Arc<[u8]> = Arc::from([byte]);
        let component_bytes =
            PhysicalArtifact::accounted_resident_component_bytes(encoded.len()).unwrap();
        let retained_bytes = ArtifactBundle::accounted_resident_overhead(&profile, 1)
            .unwrap()
            .checked_add(component_bytes)
            .unwrap();
        let budget = Arc::new(ArtifactRetainedBudget::new(retained_bytes));
        let memory = Arc::new(CountingMemory::default());
        let retention = Arc::new(
            ArtifactRetention::try_new(retained_bytes, budget.clone(), memory.clone()).unwrap(),
        );
        let artifact = Arc::new(
            PhysicalArtifact::from_shared_retained_bytes(
                ArtifactKind::ValueSet,
                schema,
                version,
                false,
                encoded,
                component_bytes,
                retained_bytes,
                retention.clone(),
            )
            .unwrap(),
        );
        let bundle = Arc::new(
            ArtifactBundle::new_retained(
                ChannelId::new(1),
                version,
                &profile,
                vec![(ArtifactKind::ValueSet, artifact)],
                usize::MAX,
                retention,
            )
            .unwrap(),
        );
        (bundle, budget, memory, retained_bytes)
    }

    fn slot() -> LiveSubscriptionSlot {
        let common = RuntimeFilterEventIdentity::new(
            novarocks_types::UniqueId::new(1, 2),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(1),
            DeploymentEpoch::new(1),
        );
        LiveSubscriptionSlot::new(
            ConsumerEventIdentity::new(
                common,
                BindingId::new(2),
                novarocks_types::UniqueId::new(4, 5),
            ),
            Arc::new(NoopEvents::default()),
        )
    }

    #[test]
    fn acquire_observer_signals_registered_waiter_and_guard_removes_registry_key() {
        let query_id = novarocks_types::UniqueId::new(91, 92);
        let binding_id = BindingId::new(93);
        let common = RuntimeFilterEventIdentity::new(
            query_id,
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(1),
            DeploymentEpoch::new(1),
        );
        let slot = Arc::new(SubscriptionSlot::new(
            ConsumerEventIdentity::new(common, binding_id, novarocks_types::UniqueId::new(94, 95)),
            Arc::new(NoopEvents::default()),
        ));
        let gate = install_native_acquire_gate_for_test(query_id, binding_id);
        let waiter = {
            let slot = slot.clone();
            std::thread::spawn(move || slot.acquire(std::time::Duration::from_secs(1)))
        };

        assert!(gate.wait_entered(std::time::Duration::from_secs(1)));
        slot.deliver(ArtifactDeliveryOutcome::Cancelled);
        assert!(matches!(
            waiter.join().unwrap(),
            ArtifactAcquireOutcome::Cancelled
        ));
        assert!(
            native_acquire_gates()
                .lock()
                .unwrap()
                .contains_key(&(query_id, binding_id))
        );
        drop(gate);
        assert!(
            !native_acquire_gates()
                .lock()
                .unwrap()
                .contains_key(&(query_id, binding_id))
        );
    }

    fn updated_version(outcome: LivePollOutcome) -> LogicalVersion {
        match outcome {
            LivePollOutcome::Updated { bundle, .. } => bundle.version(),
            other => panic!("expected live update, got {other:?}"),
        }
    }

    #[test]
    fn live_poll_none_then_v1_then_latest_v3_without_shared_cursor() {
        let live = slot();
        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: None
            }
        ));

        live.deliver(bundle(LogicalVersion::new(1), 100), None);
        assert_eq!(
            updated_version(live.poll_after(None)),
            LogicalVersion::new(1)
        );

        live.deliver(bundle(LogicalVersion::new(3), 70), None);
        assert_eq!(
            updated_version(live.poll_after(Some(LogicalVersion::new(1)))),
            LogicalVersion::new(3)
        );
        assert_eq!(
            updated_version(live.poll_after(None)),
            LogicalVersion::new(3)
        );
    }

    #[test]
    fn update_and_terminal_are_observed_atomically() {
        let live = slot();
        live.deliver(
            bundle(LogicalVersion::FIRST, 100),
            Some(LiveTerminal::Completed),
        );

        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Updated {
                terminal: Some(LiveTerminal::Completed),
                ..
            }
        ));
    }

    #[test]
    fn completed_without_artifact_is_not_unavailable_or_empty_domain() {
        let live = slot();
        live.terminal(LiveTerminal::CompletedWithoutArtifact);

        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: Some(LiveTerminal::CompletedWithoutArtifact)
            }
        ));
    }

    #[test]
    fn artifact_failure_retains_latest_bundle() {
        let live = slot();
        live.deliver(bundle(LogicalVersion::FIRST, 100), None);
        live.terminal(LiveTerminal::DegradedArtifact(
            UnavailableReason::MaterializationFailed,
        ));

        assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
        assert!(matches!(
            live.poll_after(Some(LogicalVersion::FIRST)),
            LivePollOutcome::Idle {
                latest_version: Some(LogicalVersion::FIRST),
                terminal: Some(LiveTerminal::DegradedArtifact(
                    UnavailableReason::MaterializationFailed
                ))
            }
        ));
    }

    #[test]
    fn live_snapshot_clones_share_one_retention_account() {
        let live = slot();
        let (bundle, budget, memory, retained_bytes) = retained_bundle(LogicalVersion::FIRST, 100);
        live.deliver(bundle.clone(), None);
        drop(bundle);
        assert_eq!(budget.retained_bytes(), retained_bytes);
        assert_eq!(memory.0.load(Ordering::SeqCst), retained_bytes);

        let first = live.snapshot().unwrap();
        let clones = (0..32).map(|_| first.clone()).collect::<Vec<_>>();
        assert_eq!(budget.retained_bytes(), retained_bytes);
        assert_eq!(memory.0.load(Ordering::SeqCst), retained_bytes);
        drop(clones);
        drop(first);
        assert_eq!(budget.retained_bytes(), retained_bytes);
        assert_eq!(memory.0.load(Ordering::SeqCst), retained_bytes);

        drop(live);
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(memory.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn profile_local_failure_retains_v1_while_sibling_route_publishes_v2() {
        let failed_profile = slot();
        let healthy_profile = slot();
        failed_profile.deliver(bundle(LogicalVersion::FIRST, 100), None);
        healthy_profile.deliver(bundle(LogicalVersion::FIRST, 100), None);

        failed_profile.terminal(LiveTerminal::DegradedArtifact(
            UnavailableReason::MaterializationFailed,
        ));
        healthy_profile.deliver(bundle(LogicalVersion::new(2), 70), None);

        assert!(matches!(
            failed_profile.poll_after(Some(LogicalVersion::FIRST)),
            LivePollOutcome::Idle {
                latest_version: Some(LogicalVersion::FIRST),
                terminal: Some(LiveTerminal::DegradedArtifact(
                    UnavailableReason::MaterializationFailed
                ))
            }
        ));
        assert!(matches!(
            healthy_profile.poll_after(Some(LogicalVersion::FIRST)),
            LivePollOutcome::Updated {
                bundle,
                terminal: None
            } if bundle.version() == LogicalVersion::new(2)
        ));
    }

    #[test]
    fn artifact_failure_without_latest_is_unavailable() {
        let live = slot();
        live.apply_delivery(
            Some(ArtifactDeliveryOutcome::Unavailable(
                UnavailableReason::MaterializationFailed,
            )),
            None,
        );

        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: Some(LiveTerminal::Unavailable(
                    UnavailableReason::MaterializationFailed
                ))
            }
        ));
    }

    #[test]
    fn delivery_failure_retains_latest_bundle() {
        let live = slot();
        live.deliver(bundle(LogicalVersion::FIRST, 100), None);
        live.apply_delivery(
            Some(ArtifactDeliveryOutcome::Unavailable(
                UnavailableReason::RouteUnavailable,
            )),
            None,
        );

        assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
        assert!(matches!(
            live.poll_after(Some(LogicalVersion::FIRST)),
            LivePollOutcome::Idle {
                latest_version: Some(LogicalVersion::FIRST),
                terminal: Some(LiveTerminal::DegradedDelivery(
                    UnavailableReason::RouteUnavailable
                ))
            }
        ));
    }

    #[test]
    fn delivery_failure_without_latest_is_unavailable() {
        let live = slot();
        live.apply_delivery(
            Some(ArtifactDeliveryOutcome::Unavailable(
                UnavailableReason::RouteUnavailable,
            )),
            None,
        );

        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: Some(LiveTerminal::Unavailable(
                    UnavailableReason::RouteUnavailable
                ))
            }
        ));
    }

    #[test]
    fn terminal_precedence_preserves_degradation_and_escalates_by_severity() {
        let live = slot();
        live.deliver(bundle(LogicalVersion::FIRST, 100), None);
        live.terminal(LiveTerminal::DegradedArtifact(
            UnavailableReason::MaterializationFailed,
        ));
        live.terminal(LiveTerminal::Completed);
        assert!(matches!(
            live.poll_after(Some(LogicalVersion::FIRST)),
            LivePollOutcome::Idle {
                terminal: Some(LiveTerminal::DegradedArtifact(
                    UnavailableReason::MaterializationFailed
                )),
                ..
            }
        ));

        live.terminal(LiveTerminal::DegradedDelivery(
            UnavailableReason::RouteUnavailable,
        ));
        live.terminal(LiveTerminal::DegradedLogical(
            UnavailableReason::ProducerFailed,
        ));
        live.terminal(LiveTerminal::Unavailable(
            UnavailableReason::IncompleteCoverage,
        ));
        assert!(matches!(
            live.poll_after(Some(LogicalVersion::FIRST)),
            LivePollOutcome::Idle {
                terminal: Some(LiveTerminal::Unavailable(
                    UnavailableReason::IncompleteCoverage
                )),
                ..
            }
        ));
    }

    #[test]
    fn cancelled_overrides_every_prior_terminal_and_retains_latest() {
        let prior_terminals = [
            LiveTerminal::Completed,
            LiveTerminal::CompletedWithoutArtifact,
            LiveTerminal::DegradedArtifact(UnavailableReason::MaterializationFailed),
            LiveTerminal::DegradedDelivery(UnavailableReason::RouteUnavailable),
            LiveTerminal::DegradedLogical(UnavailableReason::ProducerFailed),
            LiveTerminal::Unavailable(UnavailableReason::IncompleteCoverage),
        ];
        for prior in prior_terminals {
            let live = slot();
            live.deliver(bundle(LogicalVersion::FIRST, 100), Some(prior));
            live.terminal(LiveTerminal::Cancelled);
            assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
            assert!(matches!(
                live.poll_after(Some(LogicalVersion::FIRST)),
                LivePollOutcome::Idle {
                    terminal: Some(LiveTerminal::Cancelled),
                    ..
                }
            ));
        }
    }

    #[test]
    fn cancellation_retains_latest_bundle() {
        let live = slot();
        live.deliver(bundle(LogicalVersion::FIRST, 100), None);
        live.apply_delivery(Some(ArtifactDeliveryOutcome::Cancelled), None);

        assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
        assert!(matches!(
            live.poll_after(Some(LogicalVersion::FIRST)),
            LivePollOutcome::Idle {
                latest_version: Some(LogicalVersion::FIRST),
                terminal: Some(LiveTerminal::Cancelled)
            }
        ));
    }

    #[test]
    fn lower_delivery_is_rejected_without_rolling_back_latest() {
        let live = slot();
        live.deliver(bundle(LogicalVersion::new(3), 70), None);
        live.deliver(bundle(LogicalVersion::new(2), 80), None);

        assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::new(3));
    }

    #[test]
    fn live_poll_and_terminal_events_are_exhaustive_and_state_precedes_callback() {
        let common = RuntimeFilterEventIdentity::new(
            novarocks_types::UniqueId::new(1, 2),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(1),
            DeploymentEpoch::new(1),
        );
        let events = Arc::new(StateCheckingEvents::default());
        let live = Arc::new(LiveSubscriptionSlot::new(
            ConsumerEventIdentity::new(
                common,
                BindingId::new(2),
                novarocks_types::UniqueId::new(4, 5),
            ),
            events.clone(),
        ));
        *events.live.lock().unwrap() = Some(Arc::downgrade(&live));

        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Idle { .. }
        ));
        live.deliver(bundle(LogicalVersion::FIRST, 1), None);
        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Updated { .. }
        ));
        live.terminal(LiveTerminal::DegradedDelivery(
            UnavailableReason::RouteUnavailable,
        ));

        let recorded = events.events.lock().unwrap();
        assert!(matches!(
            recorded[0],
            RuntimeFilterEvent::LiveSubscriptionIdle {
                latest_version: None,
                terminal: None,
                ..
            }
        ));
        assert!(matches!(
            recorded[1],
            RuntimeFilterEvent::LiveSubscriptionUpdated {
                version: LogicalVersion::FIRST,
                terminal: None,
                ..
            }
        ));
        assert!(matches!(
            recorded[2],
            RuntimeFilterEvent::LiveSubscriptionTerminal {
                terminal: LiveTerminal::DegradedDelivery(UnavailableReason::RouteUnavailable),
                retained_version: Some(LogicalVersion::FIRST),
                ..
            }
        ));
    }
}
