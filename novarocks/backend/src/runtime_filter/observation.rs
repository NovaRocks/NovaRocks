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

//! Attempt-local runtime-filter observation owned by the Backend participant.
//!
//! The store retains one folded value for each identity authorized by the
//! sealed participant installation. Producer partitions become authorized
//! only after the matching installed producer instance freezes its partition
//! count. Raw events and scan-unit identifiers are never retained.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_execution::runtime_filter::{
    ArtifactUnsupportedReason, LiveTerminal, LogicalVersion, UnavailableReason,
    scan_domain::{RuntimeFilterScanUnitDecision, RuntimeFilterScanUnitNotEvaluatedReason},
};
use novarocks_types::UniqueId;

use super::domain::{
    BackendChannelIdentity, BackendConsumerSubscriptionIdentity, BackendEnvelopeKind,
    BackendParticipantIdentity, BackendParticipantInstall, BackendProducerStreamIdentity,
    BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver, BackendTransportEventIdentity,
    BackendTransportEventKind,
};

thread_local! {
    static OBSERVER_CALLBACK_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterObservationError {
    UnknownParticipant,
    UnknownChannel(BackendChannelIdentity),
    UnknownProducerInstance {
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
    },
    ProducerInstanceNotOpened {
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
    },
    ConflictingProducerPartitionCount {
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
    },
    UnknownProducerStream(BackendProducerStreamIdentity),
    ProducerPartitionCountExceeded,
    UnknownTransportRoute(BackendTransportEventIdentity),
    UnknownConsumer(BackendConsumerSubscriptionIdentity),
    IdentityMismatch,
    ConflictingChannelTerminal {
        channel: BackendChannelIdentity,
    },
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    DeliveryConflict,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    DeliveryResourceLimit,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    InvalidRowEffect,
    ProducerSequenceRegression {
        stream: BackendProducerStreamIdentity,
    },
    TransportAcknowledgementExceedsDelivery {
        route: BackendTransportEventIdentity,
    },
    ConsumerAppliedWithoutDelivery {
        consumer: BackendConsumerSubscriptionIdentity,
    },
    ConsumerAppliedVersionExceedsDelivery {
        consumer: BackendConsumerSubscriptionIdentity,
    },
    ConsumerOutcomeConflict {
        consumer: BackendConsumerSubscriptionIdentity,
    },
    ConsumerTerminalConflict {
        consumer: BackendConsumerSubscriptionIdentity,
    },
}

impl fmt::Display for RuntimeFilterObservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter observation: {self:?}"
        )
    }
}

impl std::error::Error for RuntimeFilterObservationError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterChannelObservation {
    identity: BackendChannelIdentity,
    latest_published_version: Option<LogicalVersion>,
    terminal: Option<RuntimeFilterChannelTerminal>,
    published: u64,
    completed: u64,
    unavailable: u64,
    cancelled: u64,
    terminal_conflicted: bool,
}

impl RuntimeFilterChannelObservation {
    pub(crate) const fn identity(&self) -> BackendChannelIdentity {
        self.identity
    }

    pub(crate) const fn latest_published_version(&self) -> Option<LogicalVersion> {
        self.latest_published_version
    }

    pub(crate) const fn terminal(&self) -> Option<RuntimeFilterChannelTerminal> {
        self.terminal
    }

    pub(crate) const fn published(&self) -> u64 {
        self.published
    }

    pub(crate) const fn completed(&self) -> u64 {
        self.completed
    }

    pub(crate) const fn unavailable(&self) -> u64 {
        self.unavailable
    }

    pub(crate) const fn cancelled(&self) -> u64 {
        self.cancelled
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterChannelTerminal {
    Completed(LogicalVersion),
    Unavailable(UnavailableReason),
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterProducerStreamObservation {
    identity: BackendProducerStreamIdentity,
    latest_accepted_sequence: Option<u64>,
    accepted: u64,
    duplicate: u64,
    stale: u64,
    conflict: u64,
    resource_limit: u64,
}

impl RuntimeFilterProducerStreamObservation {
    pub(crate) const fn identity(&self) -> BackendProducerStreamIdentity {
        self.identity
    }

    pub(crate) const fn latest_accepted_sequence(&self) -> Option<u64> {
        self.latest_accepted_sequence
    }

    pub(crate) const fn accepted(&self) -> u64 {
        self.accepted
    }

    pub(crate) const fn duplicate(&self) -> u64 {
        self.duplicate
    }

    pub(crate) const fn stale(&self) -> u64 {
        self.stale
    }

    pub(crate) const fn conflict(&self) -> u64 {
        self.conflict
    }

    pub(crate) const fn resource_limit(&self) -> u64 {
        self.resource_limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTransportObservation {
    identity: BackendTransportEventIdentity,
    sent: u64,
    retried: u64,
    acked: u64,
    failed_open: u64,
    sent_bytes: u64,
    retried_bytes: u64,
    acked_bytes: u64,
    failed_open_bytes: u64,
    bytes: u64,
}

impl RuntimeFilterTransportObservation {
    pub(crate) const fn identity(&self) -> BackendTransportEventIdentity {
        self.identity
    }

    pub(crate) const fn sent(&self) -> u64 {
        self.sent
    }

    pub(crate) const fn retried(&self) -> u64 {
        self.retried
    }

    pub(crate) const fn acked(&self) -> u64 {
        self.acked
    }

    pub(crate) const fn failed_open(&self) -> u64 {
        self.failed_open
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) const fn sent_bytes(&self) -> u64 {
        self.sent_bytes
    }

    pub(crate) const fn retried_bytes(&self) -> u64 {
        self.retried_bytes
    }

    pub(crate) const fn acked_bytes(&self) -> u64 {
        self.acked_bytes
    }

    pub(crate) const fn failed_open_bytes(&self) -> u64 {
        self.failed_open_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterConsumerObservation {
    identity: BackendConsumerSubscriptionIdentity,
    latest_delivered_version: Option<LogicalVersion>,
    latest_applied_version: Option<LogicalVersion>,
    outcome: Option<RuntimeFilterConsumerOutcome>,
    terminal: Option<LiveTerminal>,
    row_evaluations: u64,
    row_input: u64,
    row_output: u64,
    scan_evaluated: u64,
    scan_kept: u64,
    scan_pruned: u64,
    scan_not_evaluated: u64,
    scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedObservation,
    outcome_conflicted: bool,
    terminal_conflicted: bool,
}

impl RuntimeFilterConsumerObservation {
    pub(crate) const fn identity(&self) -> BackendConsumerSubscriptionIdentity {
        self.identity
    }

    pub(crate) const fn latest_delivered_version(&self) -> Option<LogicalVersion> {
        self.latest_delivered_version
    }

    pub(crate) const fn latest_applied_version(&self) -> Option<LogicalVersion> {
        self.latest_applied_version
    }

    pub(crate) const fn outcome(&self) -> Option<RuntimeFilterConsumerOutcome> {
        self.outcome
    }

    pub(crate) const fn terminal(&self) -> Option<LiveTerminal> {
        self.terminal
    }

    pub(crate) const fn row_evaluations(&self) -> u64 {
        self.row_evaluations
    }

    pub(crate) const fn row_input(&self) -> u64 {
        self.row_input
    }

    pub(crate) const fn row_output(&self) -> u64 {
        self.row_output
    }

    pub(crate) const fn scan_evaluated(&self) -> u64 {
        self.scan_evaluated
    }

    pub(crate) const fn scan_kept(&self) -> u64 {
        self.scan_kept
    }

    pub(crate) const fn scan_pruned(&self) -> u64 {
        self.scan_pruned
    }

    pub(crate) const fn scan_not_evaluated(&self) -> u64 {
        self.scan_not_evaluated
    }

    pub(crate) const fn scan_not_evaluated_reasons(
        &self,
    ) -> RuntimeFilterScanNotEvaluatedObservation {
        self.scan_not_evaluated_reasons
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterScanNotEvaluatedObservation {
    pub(crate) unit_facts_missing: u64,
    pub(crate) column_facts_missing: u64,
    pub(crate) data_type_unsupported: u64,
    pub(crate) predicate_capability_unsupported: u64,
    pub(crate) resource_unavailable: u64,
    pub(crate) snapshot_unavailable: u64,
    pub(crate) snapshot_timed_out: u64,
    pub(crate) snapshot_not_published: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterConsumerOutcome {
    Acquired,
    TimedOut,
    Unavailable(UnavailableReason),
    Unsupported(ArtifactUnsupportedReason),
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterObservationSnapshot {
    channels: Vec<RuntimeFilterChannelObservation>,
    producer_streams: Vec<RuntimeFilterProducerStreamObservation>,
    transport_routes: Vec<RuntimeFilterTransportObservation>,
    consumers: Vec<RuntimeFilterConsumerObservation>,
    anomalies: RuntimeFilterObservationAnomalies,
    correctness_error: Option<String>,
}

impl RuntimeFilterObservationSnapshot {
    pub(crate) fn channels(&self) -> &[RuntimeFilterChannelObservation] {
        &self.channels
    }

    pub(crate) fn producer_streams(&self) -> &[RuntimeFilterProducerStreamObservation] {
        &self.producer_streams
    }

    pub(crate) fn transport_routes(&self) -> &[RuntimeFilterTransportObservation] {
        &self.transport_routes
    }

    pub(crate) fn consumers(&self) -> &[RuntimeFilterConsumerObservation] {
        &self.consumers
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn anomalies(&self) -> &RuntimeFilterObservationAnomalies {
        &self.anomalies
    }

    pub(crate) fn correctness_error(&self) -> Option<&str> {
        self.correctness_error.as_deref()
    }
}

/// Diagnostics retained alongside the sealed observation manifest. Domain
/// violations also become first-wins correctness evidence at terminal capture;
/// late-after-seal observations remain diagnostic only.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterObservationAnomalies {
    unattributed: RuntimeFilterUnattributedObservations,
    conflicting_reports: RuntimeFilterConflictingReportObservations,
    saturated: u64,
    late_after_seal: u64,
    rejected: u64,
}

impl RuntimeFilterObservationAnomalies {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn unattributed(&self) -> RuntimeFilterUnattributedObservations {
        self.unattributed
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn conflicting_reports(&self) -> RuntimeFilterConflictingReportObservations {
        self.conflicting_reports
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn saturated(&self) -> u64 {
        self.saturated
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn late_after_seal(&self) -> u64 {
        self.late_after_seal
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn rejected(&self) -> u64 {
        self.rejected
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterUnattributedObservations {
    participant: u64,
    channel: u64,
    producer_instance: u64,
    producer_stream: u64,
    transport_route: u64,
    consumer: u64,
    identity_mismatch: u64,
}

impl RuntimeFilterUnattributedObservations {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn participant(&self) -> u64 {
        self.participant
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn channel(&self) -> u64 {
        self.channel
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn producer_instance(&self) -> u64 {
        self.producer_instance
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn producer_stream(&self) -> u64 {
        self.producer_stream
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn transport_route(&self) -> u64 {
        self.transport_route
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn consumer(&self) -> u64 {
        self.consumer
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn identity_mismatch(&self) -> u64 {
        self.identity_mismatch
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterConflictingReportObservations {
    channel_terminal: u64,
    consumer_outcome: u64,
    consumer_terminal: u64,
}

impl RuntimeFilterConflictingReportObservations {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn channel_terminal(&self) -> u64 {
        self.channel_terminal
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn consumer_outcome(&self) -> u64 {
        self.consumer_outcome
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) const fn consumer_terminal(&self) -> u64 {
        self.consumer_terminal
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ProducerInstanceIdentity {
    channel: BackendChannelIdentity,
    fragment_instance_id: UniqueId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProducerInstanceState {
    partition_count: Option<u32>,
}

struct ObservationState {
    participant: BackendParticipantIdentity,
    channels: BTreeMap<BackendChannelIdentity, RuntimeFilterChannelObservation>,
    producer_instances: BTreeMap<ProducerInstanceIdentity, ProducerInstanceState>,
    producer_streams:
        BTreeMap<BackendProducerStreamIdentity, RuntimeFilterProducerStreamObservation>,
    transport_routes: BTreeMap<BackendTransportEventIdentity, RuntimeFilterTransportObservation>,
    consumers: BTreeMap<BackendConsumerSubscriptionIdentity, RuntimeFilterConsumerObservation>,
    anomalies: RuntimeFilterObservationAnomalies,
    correctness_error: Option<RuntimeFilterObservationError>,
    sealed: Option<RuntimeFilterObservationSnapshot>,
}

// Design: ADR-0106 (docs/adr/ADR-0106-native-wire-layering-and-terminal-content-identity.md)
pub(crate) struct RuntimeFilterObservationStore {
    state: Mutex<ObservationState>,
    saturated: AtomicU64,
}

impl RuntimeFilterObservationStore {
    pub(crate) fn from_install(install: &BackendParticipantInstall) -> Self {
        let participant = install.participant();
        let mut channels = BTreeMap::new();
        let mut producer_instances = BTreeMap::new();
        let mut transport_routes = BTreeMap::new();
        let mut consumers = BTreeMap::new();
        for channel in install.channels().values() {
            let transport_binding_id = channel
                .producers()
                .keys()
                .next()
                .copied()
                .or_else(|| channel.consumers().keys().next().copied());
            for (binding_id, producer) in channel.producers() {
                let identity =
                    BackendChannelIdentity::new(participant, *binding_id, channel.channel_id());
                channels.insert(identity, channel_observation(identity));
                for fragment_instance_id in producer.expected_fragment_instances() {
                    producer_instances.insert(
                        ProducerInstanceIdentity {
                            channel: identity,
                            fragment_instance_id: *fragment_instance_id,
                        },
                        ProducerInstanceState {
                            partition_count: None,
                        },
                    );
                }
                for kind in [
                    BackendEnvelopeKind::Contribution,
                    BackendEnvelopeKind::ProducerClosed,
                    BackendEnvelopeKind::ProducerUnavailable,
                ] {
                    if let Ok(decision) =
                        install
                            .routing()
                            .route_producer(channel.channel_id(), *binding_id, kind)
                    {
                        for route_edge_id in decision
                            .loopback_route_edge_ids()
                            .iter()
                            .copied()
                            .chain(decision.remote_routes().iter().map(|route| route.edge_id()))
                        {
                            let route = BackendTransportEventIdentity::new(identity, route_edge_id);
                            transport_routes
                                .entry(route)
                                .or_insert_with(|| transport_observation(route));
                        }
                    }
                }
            }
            for (binding_id, consumer) in channel.consumers() {
                let channel_identity =
                    BackendChannelIdentity::new(participant, *binding_id, channel.channel_id());
                channels.insert(channel_identity, channel_observation(channel_identity));
                for fragment_instance_id in consumer.expected_fragment_instances() {
                    let identity = BackendConsumerSubscriptionIdentity::new(
                        channel_identity,
                        *binding_id,
                        *fragment_instance_id,
                    );
                    consumers.insert(identity, consumer_observation(identity));
                }
                for route_edge_id in consumer.route_edge_ids() {
                    let identity =
                        BackendTransportEventIdentity::new(channel_identity, *route_edge_id);
                    transport_routes.insert(identity, transport_observation(identity));
                }
            }
            if let Some(binding_id) = transport_binding_id {
                let channel_identity =
                    BackendChannelIdentity::new(participant, binding_id, channel.channel_id());
                for route_edge_id in channel
                    .outbound_materialization_groups()
                    .values()
                    .flat_map(|group| group.route_edge_ids().iter().copied())
                {
                    let identity =
                        BackendTransportEventIdentity::new(channel_identity, route_edge_id);
                    transport_routes
                        .entry(identity)
                        .or_insert_with(|| transport_observation(identity));
                }
            }
        }
        Self {
            state: Mutex::new(ObservationState {
                participant,
                channels,
                producer_instances,
                producer_streams: BTreeMap::new(),
                transport_routes,
                consumers,
                anomalies: RuntimeFilterObservationAnomalies::default(),
                correctness_error: None,
                sealed: None,
            }),
            saturated: AtomicU64::new(0),
        }
    }

    pub(crate) fn register_producer_instance(
        &self,
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
        partition_count: u32,
    ) {
        let mut state = self.lock();
        if state.sealed.is_some() {
            increment_field(&mut state.anomalies.late_after_seal, 1, &self.saturated);
            return;
        }
        let key = ProducerInstanceIdentity {
            channel,
            fragment_instance_id,
        };
        let result = match state.producer_instances.get_mut(&key) {
            None => Err(RuntimeFilterObservationError::UnknownProducerInstance {
                channel,
                fragment_instance_id,
            }),
            Some(_) if partition_count == 0 => Err(RuntimeFilterObservationError::IdentityMismatch),
            Some(_)
                if partition_count
                    > super::domain::MAX_RUNTIME_FILTER_PRODUCER_PARTITIONS_PER_INSTANCE =>
            {
                Err(RuntimeFilterObservationError::ProducerPartitionCountExceeded)
            }
            Some(instance) => match instance.partition_count {
                Some(existing) if existing != partition_count => Err(
                    RuntimeFilterObservationError::ConflictingProducerPartitionCount {
                        channel,
                        fragment_instance_id,
                    },
                ),
                Some(_) => Ok(()),
                None => {
                    instance.partition_count = Some(partition_count);
                    Ok(())
                }
            },
        };
        if let Err(error) = result {
            state.correctness_error.get_or_insert(error.clone());
            record_anomaly(&mut state, error, &self.saturated);
        }
    }

    pub(crate) fn fold(&self, event: &BackendRuntimeFilterEvent) {
        let mut state = self.lock();
        if state.sealed.is_some() {
            increment_field(&mut state.anomalies.late_after_seal, 1, &self.saturated);
            return;
        }
        if let Err(error) = fold_event(&mut state, event, &self.saturated) {
            state.correctness_error.get_or_insert(error.clone());
            record_anomaly(&mut state, error, &self.saturated);
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) fn reject(&self, error: RuntimeFilterObservationError) {
        let mut state = self.lock();
        if state.sealed.is_some() {
            increment_field(&mut state.anomalies.late_after_seal, 1, &self.saturated);
            return;
        }
        state.correctness_error.get_or_insert(error.clone());
        record_anomaly(&mut state, error, &self.saturated);
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) fn capture(&self) -> RuntimeFilterObservationSnapshot {
        let state = self.lock();
        state
            .sealed
            .clone()
            .unwrap_or_else(|| snapshot(&state, self.saturated.load(Ordering::Relaxed)))
    }

    /// Freezes the contribution that terminalization may retain. Later events
    /// are intentionally observable only as process-local anomaly metrics.
    pub(crate) fn seal(&self) -> RuntimeFilterObservationSnapshot {
        let mut state = self.lock();
        if let Some(snapshot) = &state.sealed {
            return snapshot.clone();
        }
        let frozen = snapshot(&state, self.saturated.load(Ordering::Relaxed));
        state.sealed = Some(frozen.clone());
        frozen
    }

    /// Atomically records cancellation for channels that have not reached a
    /// terminal result and freezes the terminal contribution. A query abort
    /// does not retroactively change an already completed or unavailable
    /// Runtime Filter channel into a cancelled one.
    fn cancel_open_channels_and_seal(
        &self,
    ) -> (
        RuntimeFilterObservationSnapshot,
        Vec<BackendRuntimeFilterEvent>,
    ) {
        let mut state = self.lock();
        if let Some(snapshot) = &state.sealed {
            return (snapshot.clone(), Vec::new());
        }
        let channels: Vec<_> = state
            .channels
            .values()
            .filter(|channel| channel.terminal.is_none() && !channel.terminal_conflicted)
            .map(|channel| channel.identity)
            .collect();
        for channel in &channels {
            let channel = state
                .channels
                .get_mut(channel)
                .expect("installed Runtime Filter channel remains present");
            channel.cancelled = increment(channel.cancelled, 1, &self.saturated);
            channel.terminal = Some(RuntimeFilterChannelTerminal::Cancelled);
        }
        let frozen = snapshot(&state, self.saturated.load(Ordering::Relaxed));
        state.sealed = Some(frozen.clone());
        (
            frozen,
            channels
                .into_iter()
                .map(|channel| BackendRuntimeFilterEvent::ChannelCancelled { channel })
                .collect(),
        )
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ObservationState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

pub(crate) struct RuntimeFilterObservationEmitter {
    store: Arc<RuntimeFilterObservationStore>,
    observer: Option<Arc<dyn BackendRuntimeFilterEventObserver>>,
}

impl RuntimeFilterObservationEmitter {
    pub(crate) fn from_install(
        install: &BackendParticipantInstall,
        observer: Option<Arc<dyn BackendRuntimeFilterEventObserver>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store: Arc::new(RuntimeFilterObservationStore::from_install(install)),
            observer,
        })
    }

    pub(crate) fn register_producer_instance(
        &self,
        channel: BackendChannelIdentity,
        fragment_instance_id: UniqueId,
        partition_count: u32,
    ) {
        self.store
            .register_producer_instance(channel, fragment_instance_id, partition_count);
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) fn capture(&self) -> RuntimeFilterObservationSnapshot {
        self.store.capture()
    }

    pub(crate) fn seal(&self) -> RuntimeFilterObservationSnapshot {
        self.store.seal()
    }

    /// Records the cancellation terminal fact for unresolved channels and
    /// seals the contribution as one store transaction. Observer notification
    /// follows the freeze because it is diagnostic-only and must not reopen
    /// the retained terminal proof.
    pub(crate) fn cancel_open_channels_and_seal(&self) -> RuntimeFilterObservationSnapshot {
        let (snapshot, events) = self.store.cancel_open_channels_and_seal();
        for event in events {
            self.notify(event);
        }
        snapshot
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub(crate) fn reject(&self, error: RuntimeFilterObservationError) {
        self.store.reject(error);
    }
}

impl BackendRuntimeFilterEventObserver for RuntimeFilterObservationEmitter {
    fn record(&self, event: BackendRuntimeFilterEvent) {
        if OBSERVER_CALLBACK_DEPTH.with(|depth| depth.get() != 0) {
            return;
        }
        self.store.fold(&event);
        self.notify(event);
    }
}

impl RuntimeFilterObservationEmitter {
    fn notify(&self, event: BackendRuntimeFilterEvent) {
        if OBSERVER_CALLBACK_DEPTH.with(|depth| depth.get() != 0) {
            return;
        }
        let Some(observer) = &self.observer else {
            return;
        };
        OBSERVER_CALLBACK_DEPTH.with(|depth| {
            let previous = depth.replace(depth.get().saturating_add(1));
            let _ = catch_unwind(AssertUnwindSafe(|| observer.record(event)));
            depth.set(previous);
        });
    }
}

fn fold_event(
    state: &mut ObservationState,
    event: &BackendRuntimeFilterEvent,
    saturated: &AtomicU64,
) -> Result<(), RuntimeFilterObservationError> {
    match event {
        BackendRuntimeFilterEvent::DeploymentInstalled { participant } => {
            if *participant != state.participant {
                return Err(RuntimeFilterObservationError::UnknownParticipant);
            }
        }
        BackendRuntimeFilterEvent::ChannelPlanned { channel } => {
            channel_mut(state, *channel)?;
        }
        BackendRuntimeFilterEvent::ContributionAccepted { stream, sequence } => {
            let identity = *stream;
            let stream = producer_stream_mut(state, *stream)?;
            if stream
                .latest_accepted_sequence
                .is_some_and(|latest| *sequence < latest)
            {
                return Err(RuntimeFilterObservationError::ProducerSequenceRegression {
                    stream: identity,
                });
            }
            stream.latest_accepted_sequence =
                max_option(stream.latest_accepted_sequence, *sequence);
            stream.accepted = increment(stream.accepted, 1, saturated);
        }
        BackendRuntimeFilterEvent::ContributionDuplicateIgnored { stream, .. } => {
            let stream = producer_stream_mut(state, *stream)?;
            stream.duplicate = increment(stream.duplicate, 1, saturated);
        }
        BackendRuntimeFilterEvent::ContributionStaleIgnored { stream, .. } => {
            let stream = producer_stream_mut(state, *stream)?;
            stream.stale = increment(stream.stale, 1, saturated);
        }
        BackendRuntimeFilterEvent::ContributionConflictRejected { stream, .. } => {
            let stream = producer_stream_mut(state, *stream)?;
            stream.conflict = increment(stream.conflict, 1, saturated);
        }
        BackendRuntimeFilterEvent::ContributionResourceLimitRejected { stream, .. } => {
            let stream = producer_stream_mut(state, *stream)?;
            stream.resource_limit = increment(stream.resource_limit, 1, saturated);
        }
        BackendRuntimeFilterEvent::LogicalVersionPublished { channel, version } => {
            let channel = channel_mut(state, *channel)?;
            channel.latest_published_version =
                max_option(channel.latest_published_version, *version);
            channel.published = increment(channel.published, 1, saturated);
        }
        BackendRuntimeFilterEvent::ChannelCompleted { channel, version } => {
            let conflicted = {
                let channel = channel_mut(state, *channel)?;
                channel.latest_published_version =
                    max_option(channel.latest_published_version, *version);
                channel.completed = increment(channel.completed, 1, saturated);
                join_channel_terminal(channel, RuntimeFilterChannelTerminal::Completed(*version))
            };
            if conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConflictingChannelTerminal { channel: *channel },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.channel_terminal,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::ChannelUnavailable { channel, reason } => {
            let conflicted = {
                let channel = channel_mut(state, *channel)?;
                channel.unavailable = increment(channel.unavailable, 1, saturated);
                join_channel_terminal(channel, RuntimeFilterChannelTerminal::Unavailable(*reason))
            };
            if conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConflictingChannelTerminal { channel: *channel },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.channel_terminal,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::ChannelCancelled { channel } => {
            let conflicted = {
                let channel = channel_mut(state, *channel)?;
                channel.cancelled = increment(channel.cancelled, 1, saturated);
                join_channel_terminal(channel, RuntimeFilterChannelTerminal::Cancelled)
            };
            if conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConflictingChannelTerminal { channel: *channel },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.channel_terminal,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::TransportEnvelope {
            identity,
            kind,
            bytes,
        } => {
            let bytes = u64::try_from(*bytes).unwrap_or(u64::MAX);
            let route = state.transport_routes.get_mut(identity).ok_or(
                RuntimeFilterObservationError::UnknownTransportRoute(*identity),
            )?;
            match kind {
                BackendTransportEventKind::Sent => {
                    route.sent = increment(route.sent, 1, saturated);
                    route.sent_bytes = increment(route.sent_bytes, bytes, saturated);
                }
                BackendTransportEventKind::Retried => {
                    route.retried = increment(route.retried, 1, saturated);
                    route.retried_bytes = increment(route.retried_bytes, bytes, saturated);
                }
                BackendTransportEventKind::Acked(_status) => {
                    let delivered_count = route.sent.checked_add(route.retried).ok_or(
                        RuntimeFilterObservationError::TransportAcknowledgementExceedsDelivery {
                            route: *identity,
                        },
                    )?;
                    let delivered_bytes = route.sent_bytes.checked_add(route.retried_bytes).ok_or(
                        RuntimeFilterObservationError::TransportAcknowledgementExceedsDelivery {
                            route: *identity,
                        },
                    )?;
                    if route.acked >= delivered_count
                        || route
                            .acked_bytes
                            .checked_add(bytes)
                            .is_none_or(|acked| acked > delivered_bytes)
                    {
                        return Err(
                            RuntimeFilterObservationError::TransportAcknowledgementExceedsDelivery {
                                route: *identity,
                            },
                        );
                    }
                    route.acked = increment(route.acked, 1, saturated);
                    route.acked_bytes = increment(route.acked_bytes, bytes, saturated);
                }
                BackendTransportEventKind::FailedOpen(_reason) => {
                    route.failed_open = increment(route.failed_open, 1, saturated);
                    route.failed_open_bytes = increment(route.failed_open_bytes, bytes, saturated);
                }
            }
            route.bytes = increment(route.bytes, bytes, saturated);
        }
        BackendRuntimeFilterEvent::SubscriptionAcquired { identity, version } => {
            let conflicted = {
                let consumer = consumer_mut(state, *identity)?;
                consumer.latest_delivered_version =
                    max_option(consumer.latest_delivered_version, *version);
                join_consumer_outcome(consumer, RuntimeFilterConsumerOutcome::Acquired)
            };
            if conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConsumerOutcomeConflict {
                        consumer: *identity,
                    },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.consumer_outcome,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::SubscriptionTimedOut { identity } => {
            let conflicted = join_consumer_outcome(
                consumer_mut(state, *identity)?,
                RuntimeFilterConsumerOutcome::TimedOut,
            );
            if conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConsumerOutcomeConflict {
                        consumer: *identity,
                    },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.consumer_outcome,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::SubscriptionUnavailable { identity, reason } => {
            let conflicted = join_consumer_outcome(
                consumer_mut(state, *identity)?,
                RuntimeFilterConsumerOutcome::Unavailable(*reason),
            );
            if conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConsumerOutcomeConflict {
                        consumer: *identity,
                    },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.consumer_outcome,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::SubscriptionUnsupported { identity, reason } => {
            let conflicted = join_consumer_outcome(
                consumer_mut(state, *identity)?,
                RuntimeFilterConsumerOutcome::Unsupported(*reason),
            );
            if conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConsumerOutcomeConflict {
                        consumer: *identity,
                    },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.consumer_outcome,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::SubscriptionCancelled { identity } => {
            let conflicted = join_consumer_outcome(
                consumer_mut(state, *identity)?,
                RuntimeFilterConsumerOutcome::Cancelled,
            );
            if conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConsumerOutcomeConflict {
                        consumer: *identity,
                    },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.consumer_outcome,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::LiveSubscriptionUpdated {
            identity,
            version,
            terminal,
        } => {
            let (outcome_conflicted, terminal_conflicted) = {
                let consumer = consumer_mut(state, *identity)?;
                consumer.latest_delivered_version =
                    max_option(consumer.latest_delivered_version, *version);
                let outcome_conflicted =
                    join_consumer_outcome(consumer, RuntimeFilterConsumerOutcome::Acquired);
                let terminal_conflicted =
                    terminal.is_some_and(|terminal| join_consumer_terminal(consumer, terminal));
                (outcome_conflicted, terminal_conflicted)
            };
            if outcome_conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConsumerOutcomeConflict {
                        consumer: *identity,
                    },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.consumer_outcome,
                    1,
                    saturated,
                );
            }
            if terminal_conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConsumerTerminalConflict {
                        consumer: *identity,
                    },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.consumer_terminal,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::LiveSubscriptionIdle {
            identity,
            latest_version,
            terminal,
        } => {
            let terminal_conflicted = {
                let consumer = consumer_mut(state, *identity)?;
                if let Some(version) = latest_version {
                    consumer.latest_delivered_version =
                        max_option(consumer.latest_delivered_version, *version);
                }
                terminal.is_some_and(|terminal| join_consumer_terminal(consumer, terminal))
            };
            if terminal_conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConsumerTerminalConflict {
                        consumer: *identity,
                    },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.consumer_terminal,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::LiveSubscriptionTerminal {
            identity,
            terminal,
            retained_version,
        } => {
            let terminal_conflicted = {
                let consumer = consumer_mut(state, *identity)?;
                if let Some(version) = retained_version {
                    consumer.latest_delivered_version =
                        max_option(consumer.latest_delivered_version, *version);
                }
                join_consumer_terminal(consumer, *terminal)
            };
            if terminal_conflicted {
                state.correctness_error.get_or_insert(
                    RuntimeFilterObservationError::ConsumerTerminalConflict {
                        consumer: *identity,
                    },
                );
                increment_field(
                    &mut state.anomalies.conflicting_reports.consumer_terminal,
                    1,
                    saturated,
                );
            }
        }
        BackendRuntimeFilterEvent::LoopbackDelivered {
            channel,
            consumer_binding_id,
            route_edge_id,
            version: _,
        } => {
            if channel.binding_id() != *consumer_binding_id {
                return Err(RuntimeFilterObservationError::IdentityMismatch);
            }
            channel_mut(state, *channel)?;
            let route = BackendTransportEventIdentity::new(*channel, *route_edge_id);
            if !state.transport_routes.contains_key(&route) {
                return Err(RuntimeFilterObservationError::UnknownTransportRoute(route));
            }
        }
        BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity,
            logical_version,
            input_rows,
            output_rows,
        } => {
            if output_rows > input_rows {
                return Err(RuntimeFilterObservationError::InvalidRowEffect);
            }
            let consumer = consumer_mut(state, *identity)?;
            let Some(delivered) = consumer.latest_delivered_version else {
                return Err(
                    RuntimeFilterObservationError::ConsumerAppliedWithoutDelivery {
                        consumer: *identity,
                    },
                );
            };
            if *logical_version > delivered {
                return Err(
                    RuntimeFilterObservationError::ConsumerAppliedVersionExceedsDelivery {
                        consumer: *identity,
                    },
                );
            }
            consumer.latest_applied_version =
                max_option(consumer.latest_applied_version, *logical_version);
            let evaluations = increment(consumer.row_evaluations, 1, saturated);
            let input = increment(consumer.row_input, *input_rows, saturated);
            let output = increment(consumer.row_output, *output_rows, saturated);
            consumer.row_evaluations = evaluations;
            consumer.row_input = input;
            consumer.row_output = output;
        }
        BackendRuntimeFilterEvent::ConsumerScanUnitEvaluated {
            identity,
            logical_version,
            decision,
        } => {
            let consumer = consumer_mut(state, *identity)?;
            let Some(delivered) = consumer.latest_delivered_version else {
                return Err(
                    RuntimeFilterObservationError::ConsumerAppliedWithoutDelivery {
                        consumer: *identity,
                    },
                );
            };
            if *logical_version > delivered {
                return Err(
                    RuntimeFilterObservationError::ConsumerAppliedVersionExceedsDelivery {
                        consumer: *identity,
                    },
                );
            }
            consumer.latest_applied_version =
                max_option(consumer.latest_applied_version, *logical_version);
            let evaluated = increment(consumer.scan_evaluated, 1, saturated);
            let (kept, pruned) = match decision {
                RuntimeFilterScanUnitDecision::Kept => (
                    increment(consumer.scan_kept, 1, saturated),
                    consumer.scan_pruned,
                ),
                RuntimeFilterScanUnitDecision::Pruned => (
                    consumer.scan_kept,
                    increment(consumer.scan_pruned, 1, saturated),
                ),
            };
            consumer.scan_evaluated = evaluated;
            consumer.scan_kept = kept;
            consumer.scan_pruned = pruned;
        }
        BackendRuntimeFilterEvent::ConsumerScanUnitNotEvaluated {
            identity,
            observed_version: _,
            reason,
        } => {
            let consumer = consumer_mut(state, *identity)?;
            consumer.scan_not_evaluated = increment(consumer.scan_not_evaluated, 1, saturated);
            let counter = match reason {
                RuntimeFilterScanUnitNotEvaluatedReason::UnitFactsMissing(_) => {
                    &mut consumer.scan_not_evaluated_reasons.unit_facts_missing
                }
                RuntimeFilterScanUnitNotEvaluatedReason::ColumnFactsMissing(_) => {
                    &mut consumer.scan_not_evaluated_reasons.column_facts_missing
                }
                RuntimeFilterScanUnitNotEvaluatedReason::DataTypeUnsupported => {
                    &mut consumer.scan_not_evaluated_reasons.data_type_unsupported
                }
                RuntimeFilterScanUnitNotEvaluatedReason::PredicateCapabilityUnsupported => {
                    &mut consumer
                        .scan_not_evaluated_reasons
                        .predicate_capability_unsupported
                }
                RuntimeFilterScanUnitNotEvaluatedReason::ResourceUnavailable => {
                    &mut consumer.scan_not_evaluated_reasons.resource_unavailable
                }
                RuntimeFilterScanUnitNotEvaluatedReason::SnapshotUnavailable => {
                    &mut consumer.scan_not_evaluated_reasons.snapshot_unavailable
                }
                RuntimeFilterScanUnitNotEvaluatedReason::SnapshotTimedOut => {
                    &mut consumer.scan_not_evaluated_reasons.snapshot_timed_out
                }
                RuntimeFilterScanUnitNotEvaluatedReason::SnapshotNotPublished => {
                    &mut consumer.scan_not_evaluated_reasons.snapshot_not_published
                }
            };
            *counter = increment(*counter, 1, saturated);
        }
    }
    Ok(())
}

fn channel_mut(
    state: &mut ObservationState,
    identity: BackendChannelIdentity,
) -> Result<&mut RuntimeFilterChannelObservation, RuntimeFilterObservationError> {
    state
        .channels
        .get_mut(&identity)
        .ok_or(RuntimeFilterObservationError::UnknownChannel(identity))
}

fn producer_stream_mut(
    state: &mut ObservationState,
    identity: BackendProducerStreamIdentity,
) -> Result<&mut RuntimeFilterProducerStreamObservation, RuntimeFilterObservationError> {
    if !state.producer_streams.contains_key(&identity) {
        let instance_key = ProducerInstanceIdentity {
            channel: identity.channel(),
            fragment_instance_id: identity.fragment_instance_id(),
        };
        let instance = state.producer_instances.get(&instance_key).ok_or(
            RuntimeFilterObservationError::UnknownProducerInstance {
                channel: identity.channel(),
                fragment_instance_id: identity.fragment_instance_id(),
            },
        )?;
        let partition_count = instance.partition_count.ok_or(
            RuntimeFilterObservationError::ProducerInstanceNotOpened {
                channel: identity.channel(),
                fragment_instance_id: identity.fragment_instance_id(),
            },
        )?;
        if identity.partition_id().get() >= partition_count {
            return Err(RuntimeFilterObservationError::UnknownProducerStream(
                identity,
            ));
        }
        state
            .producer_streams
            .insert(identity, producer_stream_observation(identity));
    }
    Ok(state
        .producer_streams
        .get_mut(&identity)
        .expect("authorized producer stream was inserted"))
}

fn consumer_mut(
    state: &mut ObservationState,
    identity: BackendConsumerSubscriptionIdentity,
) -> Result<&mut RuntimeFilterConsumerObservation, RuntimeFilterObservationError> {
    state
        .consumers
        .get_mut(&identity)
        .ok_or(RuntimeFilterObservationError::UnknownConsumer(identity))
}

fn max_option<T: Ord>(current: Option<T>, observed: T) -> Option<T> {
    Some(match current {
        Some(current) => current.max(observed),
        None => observed,
    })
}

fn increment(current: u64, delta: u64, saturated: &AtomicU64) -> u64 {
    let value = current.saturating_add(delta);
    if value == u64::MAX && (current != u64::MAX || delta != 0) {
        saturated.fetch_add(1, Ordering::Relaxed);
    }
    value
}

fn increment_field(field: &mut u64, delta: u64, saturated: &AtomicU64) {
    *field = increment(*field, delta, saturated);
}

fn snapshot(state: &ObservationState, saturated: u64) -> RuntimeFilterObservationSnapshot {
    let mut anomalies = state.anomalies;
    anomalies.saturated = saturated;
    RuntimeFilterObservationSnapshot {
        channels: state.channels.values().cloned().collect(),
        producer_streams: state.producer_streams.values().cloned().collect(),
        transport_routes: state.transport_routes.values().cloned().collect(),
        consumers: state.consumers.values().cloned().collect(),
        anomalies,
        correctness_error: state
            .correctness_error
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| {
                (saturated > 0).then(|| "runtime-filter observation counter overflow".to_owned())
            }),
    }
}

fn record_anomaly(
    state: &mut ObservationState,
    error: RuntimeFilterObservationError,
    saturated: &AtomicU64,
) {
    match error {
        RuntimeFilterObservationError::UnknownParticipant => {
            increment_field(&mut state.anomalies.unattributed.participant, 1, saturated);
        }
        RuntimeFilterObservationError::UnknownChannel(_) => {
            increment_field(&mut state.anomalies.unattributed.channel, 1, saturated);
        }
        RuntimeFilterObservationError::UnknownProducerInstance { .. }
        | RuntimeFilterObservationError::ProducerInstanceNotOpened { .. }
        | RuntimeFilterObservationError::ConflictingProducerPartitionCount { .. }
        | RuntimeFilterObservationError::ProducerPartitionCountExceeded => {
            increment_field(
                &mut state.anomalies.unattributed.producer_instance,
                1,
                saturated,
            );
        }
        RuntimeFilterObservationError::UnknownProducerStream(_) => {
            increment_field(
                &mut state.anomalies.unattributed.producer_stream,
                1,
                saturated,
            );
        }
        RuntimeFilterObservationError::UnknownTransportRoute(_) => {
            increment_field(
                &mut state.anomalies.unattributed.transport_route,
                1,
                saturated,
            );
        }
        RuntimeFilterObservationError::UnknownConsumer(_) => {
            increment_field(&mut state.anomalies.unattributed.consumer, 1, saturated);
        }
        RuntimeFilterObservationError::IdentityMismatch => {
            increment_field(
                &mut state.anomalies.unattributed.identity_mismatch,
                1,
                saturated,
            );
        }
        RuntimeFilterObservationError::ConflictingChannelTerminal { .. } => {
            increment_field(
                &mut state.anomalies.conflicting_reports.channel_terminal,
                1,
                saturated,
            );
        }
        RuntimeFilterObservationError::DeliveryConflict
        | RuntimeFilterObservationError::DeliveryResourceLimit
        | RuntimeFilterObservationError::InvalidRowEffect
        | RuntimeFilterObservationError::ProducerSequenceRegression { .. }
        | RuntimeFilterObservationError::TransportAcknowledgementExceedsDelivery { .. }
        | RuntimeFilterObservationError::ConsumerAppliedWithoutDelivery { .. }
        | RuntimeFilterObservationError::ConsumerAppliedVersionExceedsDelivery { .. }
        | RuntimeFilterObservationError::ConsumerOutcomeConflict { .. }
        | RuntimeFilterObservationError::ConsumerTerminalConflict { .. } => {
            increment_field(&mut state.anomalies.rejected, 1, saturated);
        }
    };
}

fn join_channel_terminal(
    channel: &mut RuntimeFilterChannelObservation,
    incoming: RuntimeFilterChannelTerminal,
) -> bool {
    if channel.terminal_conflicted {
        return false;
    }
    let Some(observed) = channel.terminal else {
        channel.terminal = Some(incoming);
        return false;
    };
    match join_channel_terminal_values(observed, incoming) {
        Some(joined) => {
            channel.terminal = Some(joined);
            false
        }
        None => {
            channel.terminal = None;
            channel.terminal_conflicted = true;
            true
        }
    }
}

fn join_channel_terminal_values(
    left: RuntimeFilterChannelTerminal,
    right: RuntimeFilterChannelTerminal,
) -> Option<RuntimeFilterChannelTerminal> {
    use RuntimeFilterChannelTerminal::{Cancelled, Completed, Unavailable};

    match (left, right) {
        (Completed(left), Completed(right)) => Some(Completed(left.max(right))),
        (Completed(version), Unavailable(UnavailableReason::IncompleteCoverage))
        | (Unavailable(UnavailableReason::IncompleteCoverage), Completed(version)) => {
            Some(Completed(version))
        }
        // A producer may complete after publishing a logical snapshot while a
        // consumer-specific materialization is unavailable. The unavailable
        // result is the channel's fail-open terminal, not a protocol conflict.
        (Completed(_), Unavailable(reason)) | (Unavailable(reason), Completed(_)) => {
            Some(Unavailable(reason))
        }
        // Cancellation may race with a terminal report on another driver. It
        // only describes that this observer stopped waiting; a specific
        // completed or unavailable result remains the authoritative terminal.
        (Cancelled, terminal) | (terminal, Cancelled) => Some(terminal),
        (left, right) if left == right => Some(left),
        _ => None,
    }
}

fn join_consumer_outcome(
    consumer: &mut RuntimeFilterConsumerObservation,
    incoming: RuntimeFilterConsumerOutcome,
) -> bool {
    if consumer.outcome_conflicted {
        return false;
    }
    match consumer.outcome {
        None => {
            consumer.outcome = Some(incoming);
            false
        }
        Some(observed) if observed == incoming => false,
        Some(_) => {
            consumer.outcome = None;
            consumer.outcome_conflicted = true;
            true
        }
    }
}

fn join_consumer_terminal(
    consumer: &mut RuntimeFilterConsumerObservation,
    incoming: LiveTerminal,
) -> bool {
    if consumer.terminal_conflicted {
        return false;
    }
    match consumer.terminal {
        None => {
            consumer.terminal = Some(incoming);
            false
        }
        Some(observed) if observed == incoming => false,
        Some(_) => {
            consumer.terminal = None;
            consumer.terminal_conflicted = true;
            true
        }
    }
}

fn channel_observation(identity: BackendChannelIdentity) -> RuntimeFilterChannelObservation {
    RuntimeFilterChannelObservation {
        identity,
        latest_published_version: None,
        terminal: None,
        published: 0,
        completed: 0,
        unavailable: 0,
        cancelled: 0,
        terminal_conflicted: false,
    }
}

fn producer_stream_observation(
    identity: BackendProducerStreamIdentity,
) -> RuntimeFilterProducerStreamObservation {
    RuntimeFilterProducerStreamObservation {
        identity,
        latest_accepted_sequence: None,
        accepted: 0,
        duplicate: 0,
        stale: 0,
        conflict: 0,
        resource_limit: 0,
    }
}

fn transport_observation(
    identity: BackendTransportEventIdentity,
) -> RuntimeFilterTransportObservation {
    RuntimeFilterTransportObservation {
        identity,
        sent: 0,
        retried: 0,
        acked: 0,
        failed_open: 0,
        sent_bytes: 0,
        retried_bytes: 0,
        acked_bytes: 0,
        failed_open_bytes: 0,
        bytes: 0,
    }
}

fn consumer_observation(
    identity: BackendConsumerSubscriptionIdentity,
) -> RuntimeFilterConsumerObservation {
    RuntimeFilterConsumerObservation {
        identity,
        latest_delivered_version: None,
        latest_applied_version: None,
        outcome: None,
        terminal: None,
        row_evaluations: 0,
        row_input: 0,
        row_output: 0,
        scan_evaluated: 0,
        scan_kept: 0,
        scan_pruned: 0,
        scan_not_evaluated: 0,
        scan_not_evaluated_reasons: RuntimeFilterScanNotEvaluatedObservation::default(),
        outcome_conflicted: false,
        terminal_conflicted: false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, Weak};
    use std::thread;

    use novarocks_execution::runtime_filter::{
        PartitionId, RuntimeFilterBindingId, RuntimeFilterConsumerContract,
        scan_domain::RuntimeFilterScanUnitNotEvaluatedReason,
    };

    use super::*;
    use crate::runtime_filter::artifact::{ArtifactKind, ConsumerArtifactProfile};
    use crate::runtime_filter::domain::{
        BackendAcceptStatus, BackendChannelInstall, BackendChannelLifecycle,
        BackendConsumerInstall, BackendCoverageWitnessId, BackendMaterializationPolicy,
        BackendProducerInstall, BackendRouteEdgeId, BackendRoutingShard,
    };
    use crate::runtime_filter::test_support::BackendRuntimeFilterFixture;

    struct Fixture {
        install: BackendParticipantInstall,
        producer_channel: BackendChannelIdentity,
        producer_instance: UniqueId,
        consumer: BackendConsumerSubscriptionIdentity,
        route: BackendTransportEventIdentity,
    }

    fn fixture() -> Fixture {
        let fixture = BackendRuntimeFilterFixture::membership();
        let participant = fixture.identity();
        let producer = fixture.producer_contract();
        let producer_instance = UniqueId::new(101, 102);
        let consumer_instance = UniqueId::new(201, 202);
        let consumer_contract = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(70),
            producer.channel_id(),
            producer.contract().clone(),
        )
        .expect("consumer contract");
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .expect("consumer profile");
        let route_edge_id = BackendRouteEdgeId::new(501);
        let channel = BackendChannelInstall::new(
            producer.channel_id(),
            producer.contract().clone(),
            BackendChannelLifecycle::CompleteOnce,
            fixture.coverage(),
            fixture.coverage(),
            BackendMaterializationPolicy::new(8, 3, 5, 1, 4096, 4096, 1)
                .expect("materialization policy"),
            4096,
            4096,
            [BackendProducerInstall::new(
                producer.clone(),
                BackendCoverageWitnessId::new(29),
                [producer_instance],
                4096,
            )
            .expect("producer install")],
            [BackendConsumerInstall::new(
                consumer_contract.clone(),
                profile,
                [route_edge_id],
                [consumer_instance],
            )
            .expect("consumer install")],
            [],
        )
        .expect("channel install");
        let routing = BackendRoutingShard::new(participant, 1, [])
            .expect("empty routing is sufficient for store tests");
        let install = BackendParticipantInstall::new(participant, 1, [channel], routing)
            .expect("participant install");
        let producer_channel =
            BackendChannelIdentity::new(participant, producer.binding_id(), producer.channel_id());
        let consumer_channel = BackendChannelIdentity::new(
            participant,
            consumer_contract.binding_id(),
            producer.channel_id(),
        );
        let consumer = BackendConsumerSubscriptionIdentity::new(
            consumer_channel,
            consumer_contract.binding_id(),
            consumer_instance,
        );
        Fixture {
            install,
            producer_channel,
            producer_instance,
            consumer,
            route: BackendTransportEventIdentity::new(consumer_channel, route_edge_id),
        }
    }

    fn record_delivery(
        emitter: &RuntimeFilterObservationEmitter,
        identity: BackendConsumerSubscriptionIdentity,
        version: LogicalVersion,
    ) {
        emitter.record(BackendRuntimeFilterEvent::SubscriptionAcquired { identity, version });
    }

    #[test]
    fn folds_only_authorized_identities_into_an_owned_snapshot() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        emitter.register_producer_instance(fixture.producer_channel, fixture.producer_instance, 2);
        let stream = BackendProducerStreamIdentity::new(
            fixture.producer_channel,
            fixture.producer_instance,
            PartitionId::new(1),
        );
        emitter.record(BackendRuntimeFilterEvent::ContributionAccepted {
            stream,
            sequence: 3,
        });
        emitter.record(BackendRuntimeFilterEvent::ContributionDuplicateIgnored {
            stream,
            sequence: 3,
        });
        emitter.record(BackendRuntimeFilterEvent::ContributionStaleIgnored {
            stream,
            sequence: 2,
        });
        for _ in 0..2 {
            emitter.record(BackendRuntimeFilterEvent::LogicalVersionPublished {
                channel: fixture.producer_channel,
                version: LogicalVersion::FIRST,
            });
            emitter.record(BackendRuntimeFilterEvent::ChannelCompleted {
                channel: fixture.producer_channel,
                version: LogicalVersion::FIRST,
            });
        }
        emitter.record(BackendRuntimeFilterEvent::TransportEnvelope {
            identity: fixture.route,
            kind: BackendTransportEventKind::Sent,
            bytes: 17,
        });
        emitter.record(BackendRuntimeFilterEvent::SubscriptionAcquired {
            identity: fixture.consumer,
            version: LogicalVersion::FIRST,
        });
        emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 100,
            output_rows: 20,
        });
        emitter.record(BackendRuntimeFilterEvent::ConsumerScanUnitEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            decision: RuntimeFilterScanUnitDecision::Pruned,
        });
        emitter.record(BackendRuntimeFilterEvent::ConsumerScanUnitNotEvaluated {
            identity: fixture.consumer,
            observed_version: Some(LogicalVersion::FIRST),
            reason: RuntimeFilterScanUnitNotEvaluatedReason::SnapshotUnavailable,
        });

        let frozen = emitter.capture();
        assert_eq!(frozen.producer_streams().len(), 1);
        assert_eq!(
            frozen.producer_streams()[0].latest_accepted_sequence(),
            Some(3)
        );
        assert_eq!(frozen.producer_streams()[0].accepted(), 1);
        assert_eq!(frozen.producer_streams()[0].duplicate(), 1);
        assert_eq!(frozen.producer_streams()[0].stale(), 1);
        let channel = frozen
            .channels()
            .iter()
            .find(|channel| channel.identity() == fixture.producer_channel)
            .expect("producer channel observation");
        assert_eq!(channel.published(), 2);
        assert_eq!(channel.completed(), 2);
        assert_eq!(frozen.transport_routes()[0].sent(), 1);
        assert_eq!(frozen.transport_routes()[0].bytes(), 17);
        assert_eq!(frozen.consumers()[0].row_input(), 100);
        assert_eq!(frozen.consumers()[0].row_output(), 20);
        assert_eq!(frozen.consumers()[0].scan_evaluated(), 1);
        assert_eq!(frozen.consumers()[0].scan_pruned(), 1);
        assert_eq!(frozen.consumers()[0].scan_not_evaluated(), 1);

        emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 10,
            output_rows: 5,
        });
        assert_eq!(frozen.consumers()[0].row_input(), 100);
        assert_eq!(emitter.capture().consumers()[0].row_input(), 110);
    }

    #[test]
    fn terminal_join_is_order_independent_and_keeps_event_counters_truthful() {
        for completed_first in [false, true] {
            let fixture = fixture();
            let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
            let incomplete = BackendRuntimeFilterEvent::ChannelUnavailable {
                channel: fixture.producer_channel,
                reason: UnavailableReason::IncompleteCoverage,
            };
            let completed = BackendRuntimeFilterEvent::ChannelCompleted {
                channel: fixture.producer_channel,
                version: LogicalVersion::FIRST,
            };
            if completed_first {
                emitter.record(completed);
                emitter.record(incomplete);
            } else {
                emitter.record(incomplete);
                emitter.record(completed);
            }

            let frozen = emitter.capture();
            let channel = frozen
                .channels()
                .iter()
                .find(|channel| channel.identity() == fixture.producer_channel)
                .expect("producer channel observation");
            assert_eq!(
                channel.terminal(),
                Some(RuntimeFilterChannelTerminal::Completed(
                    LogicalVersion::FIRST
                ))
            );
            assert_eq!(channel.completed(), 1);
            assert_eq!(channel.unavailable(), 1);
        }
    }

    #[test]
    fn cancellation_race_preserves_a_specific_channel_terminal() {
        for completed_first in [false, true] {
            let fixture = fixture();
            let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
            let completed = BackendRuntimeFilterEvent::ChannelCompleted {
                channel: fixture.producer_channel,
                version: LogicalVersion::FIRST,
            };
            let cancelled = BackendRuntimeFilterEvent::ChannelCancelled {
                channel: fixture.producer_channel,
            };
            if completed_first {
                emitter.record(completed);
                emitter.record(cancelled);
            } else {
                emitter.record(cancelled);
                emitter.record(completed);
            }

            let captured = emitter.capture();
            let channel = captured
                .channels()
                .iter()
                .find(|channel| channel.identity() == fixture.producer_channel)
                .expect("producer channel observation")
                .clone();
            assert_eq!(
                channel.terminal(),
                Some(RuntimeFilterChannelTerminal::Completed(
                    LogicalVersion::FIRST
                ))
            );
            assert_eq!(channel.completed(), 1);
            assert_eq!(channel.cancelled(), 1);
        }
    }

    #[test]
    fn materialization_unavailable_race_is_fail_open_and_order_independent() {
        for completed_first in [false, true] {
            let fixture = fixture();
            let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
            let completed = BackendRuntimeFilterEvent::ChannelCompleted {
                channel: fixture.producer_channel,
                version: LogicalVersion::FIRST,
            };
            let unavailable = BackendRuntimeFilterEvent::ChannelUnavailable {
                channel: fixture.producer_channel,
                reason: UnavailableReason::MaterializationFailed,
            };
            if completed_first {
                emitter.record(completed);
                emitter.record(unavailable);
            } else {
                emitter.record(unavailable);
                emitter.record(completed);
            }

            let captured = emitter.capture();
            let channel = captured
                .channels()
                .iter()
                .find(|channel| channel.identity() == fixture.producer_channel)
                .expect("producer channel observation");
            assert_eq!(
                channel.terminal(),
                Some(RuntimeFilterChannelTerminal::Unavailable(
                    UnavailableReason::MaterializationFailed
                ))
            );
            assert_eq!(channel.completed(), 1);
            assert_eq!(channel.unavailable(), 1);
        }
    }

    #[test]
    fn cross_driver_effect_reordering_keeps_the_highest_observed_version() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        record_delivery(&emitter, fixture.consumer, LogicalVersion::new(2));
        for version in [LogicalVersion::new(2), LogicalVersion::FIRST] {
            emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
                identity: fixture.consumer,
                logical_version: version,
                input_rows: 1,
                output_rows: 1,
            });
        }

        let captured = emitter.capture();
        let consumer = &captured.consumers()[0];
        assert_eq!(
            consumer.latest_applied_version(),
            Some(LogicalVersion::new(2))
        );
        assert_eq!(consumer.row_evaluations(), 2);
        assert_eq!(consumer.row_input(), 2);
        assert_eq!(consumer.row_output(), 2);
        assert_eq!(captured.anomalies().rejected(), 0);
    }

    #[test]
    fn unattributed_events_preserve_diagnostics_and_poison_terminal_correctness() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        let stream = BackendProducerStreamIdentity::new(
            fixture.producer_channel,
            fixture.producer_instance,
            PartitionId::new(0),
        );
        emitter.record(BackendRuntimeFilterEvent::ContributionAccepted {
            stream,
            sequence: 1,
        });
        emitter.reject(RuntimeFilterObservationError::DeliveryConflict);
        let captured = emitter.capture();
        assert_eq!(captured.anomalies().unattributed().producer_instance(), 1);
        assert_eq!(captured.anomalies().rejected(), 1);
        assert!(
            captured
                .correctness_error()
                .is_some_and(|detail| detail.contains("ProducerInstanceNotOpened"))
        );
    }

    #[test]
    fn sequence_regression_is_sticky_and_leaves_the_accepted_counter_unchanged() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        emitter.register_producer_instance(fixture.producer_channel, fixture.producer_instance, 1);
        let stream = BackendProducerStreamIdentity::new(
            fixture.producer_channel,
            fixture.producer_instance,
            PartitionId::new(0),
        );
        emitter.record(BackendRuntimeFilterEvent::ContributionAccepted {
            stream,
            sequence: 2,
        });
        emitter.record(BackendRuntimeFilterEvent::ContributionAccepted {
            stream,
            sequence: 1,
        });

        let captured = emitter.capture();
        assert_eq!(captured.producer_streams()[0].accepted(), 1);
        assert_eq!(
            captured.producer_streams()[0].latest_accepted_sequence(),
            Some(2)
        );
        assert!(
            captured
                .correctness_error()
                .is_some_and(|detail| detail.contains("ProducerSequenceRegression"))
        );
    }

    #[test]
    fn acknowledgement_cannot_exceed_the_recorded_delivery() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        emitter.record(BackendRuntimeFilterEvent::TransportEnvelope {
            identity: fixture.route,
            kind: BackendTransportEventKind::Acked(BackendAcceptStatus::Accepted),
            bytes: 17,
        });

        let captured = emitter.capture();
        assert_eq!(captured.transport_routes()[0].acked(), 0);
        assert!(
            captured
                .correctness_error()
                .is_some_and(|detail| detail.contains("TransportAcknowledgementExceedsDelivery"))
        );
    }

    #[test]
    fn applying_a_version_without_delivery_is_sticky_and_does_not_fold_rows() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 4,
            output_rows: 2,
        });

        let captured = emitter.capture();
        assert_eq!(captured.consumers()[0].row_evaluations(), 0);
        assert!(
            captured
                .correctness_error()
                .is_some_and(|detail| detail.contains("ConsumerAppliedWithoutDelivery"))
        );
    }

    #[test]
    fn producer_partition_bound_cannot_expand_after_open() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        emitter.register_producer_instance(fixture.producer_channel, fixture.producer_instance, 1);
        let out_of_bound = BackendProducerStreamIdentity::new(
            fixture.producer_channel,
            fixture.producer_instance,
            PartitionId::new(1),
        );
        emitter.record(BackendRuntimeFilterEvent::ContributionAccepted {
            stream: out_of_bound,
            sequence: 1,
        });
        let captured = emitter.capture();
        assert_eq!(captured.anomalies().unattributed().producer_stream(), 1);
    }

    struct PanickingObserver;

    impl BackendRuntimeFilterEventObserver for PanickingObserver {
        fn record(&self, _: BackendRuntimeFilterEvent) {
            panic!("diagnostic observer panic");
        }
    }

    #[test]
    fn diagnostic_observer_panic_cannot_erase_store_state() {
        let fixture = fixture();
        let observer: Arc<dyn BackendRuntimeFilterEventObserver> = Arc::new(PanickingObserver);
        let emitter =
            RuntimeFilterObservationEmitter::from_install(&fixture.install, Some(observer));
        record_delivery(&emitter, fixture.consumer, LogicalVersion::FIRST);
        emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 10,
            output_rows: 4,
        });
        assert_eq!(emitter.capture().consumers()[0].row_input(), 10);
    }

    struct ReentrantObserver {
        emitter: Mutex<Weak<RuntimeFilterObservationEmitter>>,
        event: BackendRuntimeFilterEvent,
    }

    impl BackendRuntimeFilterEventObserver for ReentrantObserver {
        fn record(&self, _: BackendRuntimeFilterEvent) {
            if let Some(emitter) = self.emitter.lock().unwrap().upgrade() {
                emitter.record(self.event.clone());
            }
        }
    }

    #[test]
    fn diagnostic_observer_reentrancy_cannot_double_fold() {
        let fixture = fixture();
        let event = BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 10,
            output_rows: 4,
        };
        let observer = Arc::new(ReentrantObserver {
            emitter: Mutex::new(Weak::new()),
            event: event.clone(),
        });
        let emitter =
            RuntimeFilterObservationEmitter::from_install(&fixture.install, Some(observer.clone()));
        *observer.emitter.lock().unwrap() = Arc::downgrade(&emitter);
        record_delivery(&emitter, fixture.consumer, LogicalVersion::FIRST);
        emitter.record(event);
        let captured = emitter.capture();
        let consumer = &captured.consumers()[0];
        assert_eq!(consumer.row_evaluations(), 1);
        assert_eq!(consumer.row_input(), 10);
    }

    #[test]
    fn concurrent_fold_and_capture_preserve_checked_totals() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        record_delivery(&emitter, fixture.consumer, LogicalVersion::FIRST);
        let mut workers = Vec::new();
        for _ in 0..4 {
            let emitter = Arc::clone(&emitter);
            let identity = fixture.consumer;
            workers.push(thread::spawn(move || {
                for _ in 0..100 {
                    emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
                        identity,
                        logical_version: LogicalVersion::FIRST,
                        input_rows: 1,
                        output_rows: 1,
                    });
                }
            }));
        }
        for _ in 0..10 {
            let _ = emitter.capture();
        }
        for worker in workers {
            worker.join().expect("worker");
        }
        let captured = emitter.capture();
        let consumer = &captured.consumers()[0];
        assert_eq!(consumer.row_evaluations(), 400);
        assert_eq!(consumer.row_input(), 400);
        assert_eq!(consumer.row_output(), 400);
    }

    #[test]
    fn counters_saturate_and_mark_the_observation() {
        let fixture = fixture();
        let store = RuntimeFilterObservationStore::from_install(&fixture.install);
        {
            let mut state = store.lock();
            state
                .consumers
                .get_mut(&fixture.consumer)
                .expect("installed consumer")
                .row_input = u64::MAX;
        }
        store.fold(&BackendRuntimeFilterEvent::SubscriptionAcquired {
            identity: fixture.consumer,
            version: LogicalVersion::FIRST,
        });
        store.fold(&BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 1,
            output_rows: 0,
        });
        let captured = store.capture();
        assert_eq!(captured.consumers()[0].row_input(), u64::MAX);
        assert_eq!(captured.anomalies().saturated(), 1);
        assert_eq!(
            captured.correctness_error(),
            Some("runtime-filter observation counter overflow")
        );
    }

    #[test]
    fn invalid_row_effect_is_a_first_wins_correctness_failure() {
        let fixture = fixture();
        let store = RuntimeFilterObservationStore::from_install(&fixture.install);
        store.fold(&BackendRuntimeFilterEvent::SubscriptionAcquired {
            identity: fixture.consumer,
            version: LogicalVersion::FIRST,
        });
        store.fold(&BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 1,
            output_rows: 2,
        });
        let captured = store.seal();
        assert_eq!(
            captured.correctness_error(),
            Some("invalid Backend runtime-filter observation: InvalidRowEffect")
        );
        assert_eq!(captured.consumers()[0].row_evaluations(), 0);
    }

    #[test]
    fn arrival_order_does_not_change_the_folded_snapshot() {
        let fixture = fixture();
        let first = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        let second = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        record_delivery(&first, fixture.consumer, LogicalVersion::new(2));
        record_delivery(&second, fixture.consumer, LogicalVersion::new(2));
        let events = [
            BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
                identity: fixture.consumer,
                logical_version: LogicalVersion::new(2),
                input_rows: 10,
                output_rows: 4,
            },
            BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
                identity: fixture.consumer,
                logical_version: LogicalVersion::FIRST,
                input_rows: 3,
                output_rows: 1,
            },
            BackendRuntimeFilterEvent::LogicalVersionPublished {
                channel: fixture.producer_channel,
                version: LogicalVersion::new(2),
            },
            BackendRuntimeFilterEvent::LogicalVersionPublished {
                channel: fixture.producer_channel,
                version: LogicalVersion::FIRST,
            },
        ];
        for event in &events {
            first.record(event.clone());
        }
        for event in events.into_iter().rev() {
            second.record(event);
        }
        assert_eq!(first.capture(), second.capture());
    }

    #[test]
    fn seal_freezes_the_contribution_and_marks_late_events() {
        let fixture = fixture();
        let emitter = RuntimeFilterObservationEmitter::from_install(&fixture.install, None);
        record_delivery(&emitter, fixture.consumer, LogicalVersion::FIRST);
        emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::FIRST,
            input_rows: 10,
            output_rows: 4,
        });
        let frozen = emitter.seal();
        emitter.record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
            identity: fixture.consumer,
            logical_version: LogicalVersion::new(2),
            input_rows: 5,
            output_rows: 2,
        });
        assert_eq!(frozen.consumers()[0].row_input(), 10);
        assert_eq!(emitter.capture(), frozen);
        assert_eq!(emitter.store.lock().anomalies.late_after_seal, 1);
    }
}
