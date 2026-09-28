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

//! Backend-owned publication and subscription state.
//!
//! A slot retains only immutable Execution snapshots. Blocking-snapshot
//! outcomes and live polling therefore share one publication path while
//! Execution remains the owner of snapshot/version semantics exposed to
//! operators. Nothing here blocks a caller: a blocking-snapshot consumer waits
//! on the slot's outcome observable and decides its own timeout.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_execution::runtime::observable::Observable;
use novarocks_execution::runtime_filter::{
    BlockingSnapshotSubscription, ConsumerActivation, LivePollOutcome, LiveTerminal,
    LogicalVersion, NonBlockingLiveSubscription, RuntimeFilterSnapshot,
    RuntimeFilterSubscriptionHandle, SnapshotAcquireOutcome, UnavailableReason,
};
use novarocks_types::UniqueId;

use super::{
    BackendChannelIdentity, BackendConsumerSubscriptionIdentity, BackendRouteEdgeId,
    BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendSubscriptionError {
    UnknownRoute(BackendRouteEdgeId),
    VersionRegression {
        observed: LogicalVersion,
        published: LogicalVersion,
    },
}

impl fmt::Display for BackendSubscriptionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter subscription: {self:?}"
        )
    }
}

impl std::error::Error for BackendSubscriptionError {}

enum BlockingState {
    Pending,
    Terminal(SnapshotAcquireOutcome),
}

pub struct BackendBlockingSubscription {
    identity: BackendConsumerSubscriptionIdentity,
    events: Arc<dyn BackendRuntimeFilterEventObserver>,
    state: Mutex<BlockingState>,
    /// Notified once, when the outcome is published.
    published: Arc<Observable>,
    /// Whether the consumer's final outcome was recorded.
    recorded: AtomicBool,
}

impl BackendBlockingSubscription {
    fn new(
        identity: BackendConsumerSubscriptionIdentity,
        events: Arc<dyn BackendRuntimeFilterEventObserver>,
    ) -> Self {
        Self {
            identity,
            events,
            state: Mutex::new(BlockingState::Pending),
            published: Arc::new(Observable::new()),
            recorded: AtomicBool::new(false),
        }
    }

    fn publish(&self, outcome: SnapshotAcquireOutcome) {
        let published = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if matches!(*state, BlockingState::Pending) {
                *state = BlockingState::Terminal(outcome);
                true
            } else {
                false
            }
        };
        if published {
            self.published.notify_observers();
        }
    }

    fn emit(&self, outcome: &SnapshotAcquireOutcome) {
        let event = match outcome {
            SnapshotAcquireOutcome::Published(snapshot) => {
                BackendRuntimeFilterEvent::SubscriptionAcquired {
                    identity: self.identity,
                    version: snapshot.logical_version(),
                }
            }
            SnapshotAcquireOutcome::Unsupported(reason) => {
                BackendRuntimeFilterEvent::SubscriptionUnsupported {
                    identity: self.identity,
                    reason: *reason,
                }
            }
            SnapshotAcquireOutcome::Unavailable(reason) => {
                BackendRuntimeFilterEvent::SubscriptionUnavailable {
                    identity: self.identity,
                    reason: *reason,
                }
            }
            SnapshotAcquireOutcome::Cancelled => BackendRuntimeFilterEvent::SubscriptionCancelled {
                identity: self.identity,
            },
            SnapshotAcquireOutcome::TimedOut => BackendRuntimeFilterEvent::SubscriptionTimedOut {
                identity: self.identity,
            },
        };
        self.events.record(event);
    }
}

impl BlockingSnapshotSubscription for BackendBlockingSubscription {
    fn try_outcome(&self) -> Option<SnapshotAcquireOutcome> {
        match &*self.state.lock().unwrap_or_else(|error| error.into_inner()) {
            BlockingState::Pending => None,
            BlockingState::Terminal(outcome) => Some(outcome.clone()),
        }
    }

    fn outcome_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.published)
    }

    fn record_consumer_outcome(&self, outcome: &SnapshotAcquireOutcome) {
        if self.recorded.swap(true, Ordering::AcqRel) {
            return;
        }
        self.emit(outcome);
    }

    fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        match &*state {
            BlockingState::Terminal(SnapshotAcquireOutcome::Published(snapshot)) => {
                Some(Arc::clone(snapshot))
            }
            BlockingState::Pending
            | BlockingState::Terminal(
                SnapshotAcquireOutcome::Unsupported(_)
                | SnapshotAcquireOutcome::Unavailable(_)
                | SnapshotAcquireOutcome::Cancelled
                | SnapshotAcquireOutcome::TimedOut,
            ) => None,
        }
    }
}

#[derive(Default)]
struct LiveState {
    latest: Option<Arc<RuntimeFilterSnapshot>>,
    terminal: Option<LiveTerminal>,
}

pub struct BackendLiveSubscription {
    identity: BackendConsumerSubscriptionIdentity,
    events: Arc<dyn BackendRuntimeFilterEventObserver>,
    state: Mutex<LiveState>,
}

impl BackendLiveSubscription {
    fn new(
        identity: BackendConsumerSubscriptionIdentity,
        events: Arc<dyn BackendRuntimeFilterEventObserver>,
    ) -> Self {
        Self {
            identity,
            events,
            state: Mutex::new(LiveState::default()),
        }
    }

    fn publish(
        &self,
        snapshot: Arc<RuntimeFilterSnapshot>,
        terminal: Option<LiveTerminal>,
    ) -> Result<(), BackendSubscriptionError> {
        let (emit_terminal, retained_version) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(latest) = &state.latest
                && snapshot.logical_version() < latest.logical_version()
            {
                return Err(BackendSubscriptionError::VersionRegression {
                    observed: latest.logical_version(),
                    published: snapshot.logical_version(),
                });
            }
            if state
                .latest
                .as_ref()
                .is_none_or(|latest| snapshot.logical_version() > latest.logical_version())
            {
                state.latest = Some(snapshot);
            }
            let previous_terminal = state.terminal;
            if let Some(terminal) = terminal {
                state.terminal = Some(merge_terminal(state.terminal, terminal));
            }
            (
                state
                    .terminal
                    .filter(|terminal| Some(*terminal) != previous_terminal),
                state
                    .latest
                    .as_ref()
                    .map(|snapshot| snapshot.logical_version()),
            )
        };
        if let Some(terminal) = emit_terminal {
            self.events
                .record(BackendRuntimeFilterEvent::LiveSubscriptionTerminal {
                    identity: self.identity,
                    terminal,
                    retained_version,
                });
        }
        Ok(())
    }

    fn terminal(&self, terminal: LiveTerminal) {
        let (changed, retained_version) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let next = merge_terminal(state.terminal, terminal);
            let changed = state.terminal != Some(next);
            state.terminal = Some(next);
            (
                changed.then_some(next),
                state
                    .latest
                    .as_ref()
                    .map(|snapshot| snapshot.logical_version()),
            )
        };
        if let Some(terminal) = changed {
            self.events
                .record(BackendRuntimeFilterEvent::LiveSubscriptionTerminal {
                    identity: self.identity,
                    terminal,
                    retained_version,
                });
        }
    }
}

impl NonBlockingLiveSubscription for BackendLiveSubscription {
    fn snapshot(&self) -> Option<Arc<RuntimeFilterSnapshot>> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .latest
            .clone()
    }

    fn poll_after(&self, observed: Option<LogicalVersion>) -> LivePollOutcome {
        let (latest, terminal) = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            (state.latest.clone(), state.terminal)
        };
        match latest {
            Some(snapshot)
                if observed.is_none_or(|version| snapshot.logical_version() > version) =>
            {
                self.events
                    .record(BackendRuntimeFilterEvent::LiveSubscriptionUpdated {
                        identity: self.identity,
                        version: snapshot.logical_version(),
                        terminal,
                    });
                LivePollOutcome::Updated { snapshot, terminal }
            }
            latest => {
                self.events
                    .record(BackendRuntimeFilterEvent::LiveSubscriptionIdle {
                        identity: self.identity,
                        latest_version: latest.as_ref().map(|snapshot| snapshot.logical_version()),
                        terminal,
                    });
                LivePollOutcome::Idle {
                    latest_version: latest.as_ref().map(|snapshot| snapshot.logical_version()),
                    terminal,
                }
            }
        }
    }
}

fn terminal_rank(terminal: LiveTerminal) -> u8 {
    match terminal {
        LiveTerminal::Completed | LiveTerminal::CompletedWithoutArtifact => 0,
        LiveTerminal::Unavailable(UnavailableReason::MaterializationFailed) => 1,
        LiveTerminal::Unavailable(UnavailableReason::RouteUnavailable) => 2,
        LiveTerminal::Unavailable(_) => 3,
        LiveTerminal::Cancelled => 4,
    }
}

fn merge_terminal(current: Option<LiveTerminal>, incoming: LiveTerminal) -> LiveTerminal {
    current
        .filter(|current| terminal_rank(*current) >= terminal_rank(incoming))
        .unwrap_or(incoming)
}

/// All Backend-local slots for a consumer binding and its authorized delivery
/// routes. The group does not materialize artifacts or make evaluator choices.
pub struct BackendSubscriptionGroup {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    activation: ConsumerActivation,
    route_edge_ids: BTreeSet<BackendRouteEdgeId>,
    slots: BTreeMap<UniqueId, BackendInstalledSubscriptionSlot>,
}

enum BackendInstalledSubscriptionSlot {
    Blocking(Arc<BackendBlockingSubscription>),
    Live(Arc<BackendLiveSubscription>),
}

impl BackendSubscriptionGroup {
    pub fn new(
        channel: BackendChannelIdentity,
        consumer_binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        activation: ConsumerActivation,
        route_edge_ids: impl IntoIterator<Item = BackendRouteEdgeId>,
        instances: impl IntoIterator<Item = UniqueId>,
        events: Arc<dyn BackendRuntimeFilterEventObserver>,
    ) -> Self {
        let slots = instances
            .into_iter()
            .map(|fragment_instance_id| {
                let identity = BackendConsumerSubscriptionIdentity::new(
                    channel,
                    consumer_binding_id,
                    fragment_instance_id,
                );
                let slot = match activation {
                    ConsumerActivation::BlockingSnapshot => {
                        BackendInstalledSubscriptionSlot::Blocking(Arc::new(
                            BackendBlockingSubscription::new(identity, Arc::clone(&events)),
                        ))
                    }
                    ConsumerActivation::NonBlockingLive { .. } => {
                        BackendInstalledSubscriptionSlot::Live(Arc::new(
                            BackendLiveSubscription::new(identity, Arc::clone(&events)),
                        ))
                    }
                };
                (fragment_instance_id, slot)
            })
            .collect();
        Self {
            activation,
            route_edge_ids: route_edge_ids.into_iter().collect(),
            slots,
        }
    }

    pub fn handle(
        &self,
        fragment_instance_id: UniqueId,
    ) -> Option<RuntimeFilterSubscriptionHandle> {
        match self.slots.get(&fragment_instance_id)? {
            BackendInstalledSubscriptionSlot::Blocking(slot) => {
                let slot: Arc<dyn BlockingSnapshotSubscription> = slot.clone();
                Some(RuntimeFilterSubscriptionHandle::Blocking(slot))
            }
            BackendInstalledSubscriptionSlot::Live(slot) => {
                let slot: Arc<dyn NonBlockingLiveSubscription> = slot.clone();
                Some(RuntimeFilterSubscriptionHandle::Live(slot))
            }
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn activation(&self) -> ConsumerActivation {
        self.activation
    }

    pub fn publish(
        &self,
        route_edge_id: BackendRouteEdgeId,
        outcome: SnapshotAcquireOutcome,
        terminal: Option<LiveTerminal>,
    ) -> Result<(), BackendSubscriptionError> {
        if !self.route_edge_ids.contains(&route_edge_id) {
            return Err(BackendSubscriptionError::UnknownRoute(route_edge_id));
        }
        for slot in self.slots.values() {
            match slot {
                BackendInstalledSubscriptionSlot::Blocking(slot) => slot.publish(outcome.clone()),
                BackendInstalledSubscriptionSlot::Live(slot) => match &outcome {
                    SnapshotAcquireOutcome::Published(snapshot) => {
                        slot.publish(Arc::clone(snapshot), terminal)?
                    }
                    SnapshotAcquireOutcome::Unavailable(reason) => {
                        slot.terminal(terminal.unwrap_or(LiveTerminal::Unavailable(*reason)))
                    }
                    SnapshotAcquireOutcome::Cancelled => slot.terminal(LiveTerminal::Cancelled),
                    SnapshotAcquireOutcome::Unsupported(_) | SnapshotAcquireOutcome::TimedOut => {
                        slot.terminal(LiveTerminal::Unavailable(
                            UnavailableReason::MaterializationFailed,
                        ))
                    }
                },
            }
        }
        Ok(())
    }

    pub fn publish_terminal(
        &self,
        route_edge_id: BackendRouteEdgeId,
        terminal: LiveTerminal,
    ) -> Result<(), BackendSubscriptionError> {
        if !self.route_edge_ids.contains(&route_edge_id) {
            return Err(BackendSubscriptionError::UnknownRoute(route_edge_id));
        }
        for slot in self.slots.values() {
            match slot {
                BackendInstalledSubscriptionSlot::Blocking(slot) => {
                    let outcome = match terminal {
                        LiveTerminal::Cancelled => SnapshotAcquireOutcome::Cancelled,
                        LiveTerminal::Unavailable(reason) => {
                            SnapshotAcquireOutcome::Unavailable(reason)
                        }
                        LiveTerminal::Completed | LiveTerminal::CompletedWithoutArtifact => {
                            SnapshotAcquireOutcome::Unavailable(
                                UnavailableReason::IncompleteCoverage,
                            )
                        }
                    };
                    slot.publish(outcome);
                }
                BackendInstalledSubscriptionSlot::Live(slot) => slot.terminal(terminal),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_execution::runtime_filter::{
        RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterLateApplyGranularity,
    };

    use super::*;
    use crate::runtime_filter::domain::{
        BackendParticipantIdentity, CollectingBackendRuntimeFilterEventObserver,
    };

    fn group(
        activation: ConsumerActivation,
    ) -> (
        BackendSubscriptionGroup,
        BackendRouteEdgeId,
        Arc<CollectingBackendRuntimeFilterEventObserver>,
    ) {
        let participant = BackendParticipantIdentity::new(UniqueId::new(1, 2), 3);
        let channel = BackendChannelIdentity::new(
            participant,
            RuntimeFilterBindingId::new(7),
            RuntimeFilterChannelId::new(9),
        );
        let route = BackendRouteEdgeId::new(11);
        let events = Arc::new(CollectingBackendRuntimeFilterEventObserver::default());
        (
            BackendSubscriptionGroup::new(
                channel,
                RuntimeFilterBindingId::new(7),
                activation,
                [route],
                [UniqueId::new(12, 13)],
                events.clone(),
            ),
            route,
            events,
        )
    }

    #[test]
    fn blocking_terminal_publication_wakes_without_an_artifact_evaluator() {
        let (group, route, events) = group(ConsumerActivation::BlockingSnapshot);
        group
            .publish(
                route,
                SnapshotAcquireOutcome::Unavailable(UnavailableReason::RouteUnavailable),
                None,
            )
            .unwrap();
        let RuntimeFilterSubscriptionHandle::Blocking(slot) =
            group.handle(UniqueId::new(12, 13)).unwrap()
        else {
            panic!("expected blocking slot")
        };
        let acquired = slot.try_outcome().expect("published outcome");
        assert!(matches!(
            acquired,
            SnapshotAcquireOutcome::Unavailable(UnavailableReason::RouteUnavailable)
        ));
        slot.record_consumer_outcome(&acquired);
        assert!(events.events().iter().any(|event| matches!(
            event,
            BackendRuntimeFilterEvent::SubscriptionUnavailable { .. }
        )));
    }

    #[test]
    fn live_slot_publishes_terminal_without_a_snapshot_or_arrow_input() {
        let (group, route, _) = group(ConsumerActivation::NonBlockingLive {
            late_apply: RuntimeFilterLateApplyGranularity::Row,
        });
        group
            .publish(
                route,
                SnapshotAcquireOutcome::Unavailable(UnavailableReason::MaterializationFailed),
                None,
            )
            .unwrap();
        let RuntimeFilterSubscriptionHandle::Live(slot) =
            group.handle(UniqueId::new(12, 13)).unwrap()
        else {
            panic!("expected live slot")
        };
        assert!(matches!(
            slot.poll_after(None),
            LivePollOutcome::Idle {
                terminal: Some(LiveTerminal::Unavailable(
                    UnavailableReason::MaterializationFailed
                )),
                ..
            }
        ));
    }
}
