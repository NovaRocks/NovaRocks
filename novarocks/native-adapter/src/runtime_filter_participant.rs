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

//! Native bridge for one attempt-scoped Worker runtime-filter participant.
//!
//! Fragment sessions resolve only sealed Execution contracts. Backend keeps
//! route authority, expected instances, reduction state, and subscriptions in
//! the installed channel sessions; no Core runtime-filter service is retained
//! by this participant.

use std::collections::{BTreeMap, VecDeque};
#[cfg(debug_assertions)]
use std::sync::Condvar;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use novarocks_execution::runtime_filter::{
    RuntimeFilterChannelId, RuntimeFilterContractViolation, RuntimeFilterContractViolationKind,
    RuntimeFilterSessionRef,
};
use novarocks_proto_codec::lifecycle::{QueryExecutionId, QueryTerminationReason};
use novarocks_types::UniqueId;
use prost::Message;

use crate::{
    BackendDataRuntime,
    runtime_filter_install::DecodedRuntimeFilterContribution,
    runtime_filter_rpc::{
        BackendNativeContributionRouteIdentity, BackendNativeDeliveryRouteIdentity,
        BackendNativeProducerInstanceRouteIdentity, BackendNativeRouteIdentity,
        BackendNativeRuntimeFilterEnvelope, BackendRuntimeFilterEnvelopeIngress,
        encode_runtime_filter_envelope,
    },
    runtime_filter_transport::{
        BackendNativeRuntimeFilterTransportEnvelope, BackendRuntimeFilterEnvelopeSink,
        BackendRuntimeFilterRetryPolicy, BackendRuntimeFilterSinkCompletion,
        BackendRuntimeFilterSinkSubmitOutcome, BackendRuntimeFilterTransportFailureReason,
        GrpcRuntimeFilterEnvelopeSink,
    },
};
use novarocks_worker::runtime_filter::domain::{
    BackendChannelIdentity, BackendEnvelopeKind, BackendFrontendFeedbackSink, BackendIngressResult,
    BackendMaterializedDelivery, BackendMaterializedDeliverySink, BackendParticipantInstall,
    BackendRouteDecision, BackendRoutingError, BackendRuntimeFilterEvent,
    BackendRuntimeFilterEventObserver, BackendTransportEventIdentity, BackendTransportEventKind,
    BackendTransportFailOpenReason,
};
use novarocks_worker::runtime_filter::execution_session::RuntimeFilterParticipantOutbound;
use novarocks_worker::runtime_filter::observation::{
    RuntimeFilterObservationEmitter, RuntimeFilterObservationSnapshot,
};
use novarocks_worker::runtime_filter::participant::WorkerRuntimeFilterParticipant;
use novarocks_worker::runtime_filter::participant_ingress;
use novarocks_worker::{
    RuntimeFilterContractError, RuntimeFilterContractErrorCode,
    runtime_filter::codec::producer as producer_codec,
};

const QUERY_UNAVAILABLE_REJECTION: &str = "runtime filter ingress rejected [query-unavailable]: runtime filter query is not active or in delivery grace";
const ACK_UNSUPPORTED_REJECTION: &str = "runtime filter ingress rejected [ack-unsupported]: runtime filter ack ingress is not supported";
const DELIVERY_REJECTION: &str = "runtime filter ingress rejected [artifact-delivery]: delivery violates the installed artifact contract";
/// Native construction bridge injected by the Backend attempt owner.
///
/// This factory materializes only the Worker participant's sealed Native
/// envelope/transport bridge. It owns neither the attempt registry nor the
/// exact-context lookup used to decide which participant may receive a frame.
pub struct NativeRuntimeFilterParticipantFactory {
    runtime: BackendDataRuntime,
}

impl NativeRuntimeFilterParticipantFactory {
    pub fn new(runtime: BackendDataRuntime) -> Self {
        Self { runtime }
    }

    // Design: ADR-0044 (docs/adr/ADR-0044-backend-runtime-filter-participant-domain.md)
    pub fn install(
        &self,
        execution_id: QueryExecutionId,
        contribution: DecodedRuntimeFilterContribution,
    ) -> Result<Arc<RuntimeFilterParticipant>, RuntimeFilterContractError> {
        let query_id = UniqueId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        );
        let lifecycle = contribution.lifecycle;
        let install = contribution.install;
        if install.participant().query_id() != query_id
            || install.participant().deployment_epoch() != execution_id.attempt_id().get()
        {
            return Err(RuntimeFilterContractError::invalid_contract(
                "runtime filter install does not match the query execution attempt",
            ));
        }
        let state = Arc::new(WorkerRuntimeFilterParticipant::from_install(
            install.clone(),
        )?);
        let transport_policy = BackendRuntimeFilterRetryPolicy::new(
            lifecycle.transport_retry_interval,
            lifecycle.transport_max_attempts,
            lifecycle.transport_deadline,
            lifecycle.transport_max_pending_entries,
            lifecycle.transport_max_pending_bytes,
        )
        .map_err(|error| {
            RuntimeFilterContractError::invalid_contract(format!(
                "invalid runtime filter transport policy: {error:?}"
            ))
        })?;
        RuntimeFilterParticipant::from_installed(
            execution_id,
            install,
            state,
            transport_policy,
            GrpcRuntimeFilterEnvelopeSink::new(self.runtime.clone()),
        )
    }
}

/// One sealed Backend participant for exactly one query execution attempt.
pub struct RuntimeFilterParticipant {
    execution_id: QueryExecutionId,
    state: Arc<WorkerRuntimeFilterParticipant>,
    outbound: Arc<BackendParticipantOutbound>,
    close_hook: RuntimeFilterParticipantCloseHook,
    #[cfg(debug_assertions)]
    accepted_contribution_retry_rendezvous: AcceptedContributionRetryRendezvous,
}

/// Runner-only synchronization for the Accepted-after-ACK-drop scenario.
///
/// The receiver must remain installed until the sender's exact retry reaches
/// its ordinary dedupe owner.  This lives on the participant rather than in a
/// registry or a transport fallback, so release builds and all non-faulted
/// attempts retain their normal release/no-op behavior.
#[cfg(debug_assertions)]
struct AcceptedContributionRetryRendezvous {
    state: Mutex<AcceptedContributionRetryRendezvousState>,
    changed: Condvar,
}

#[cfg(debug_assertions)]
#[derive(Copy, Clone, Eq, PartialEq)]
enum AcceptedContributionRetryRendezvousState {
    Idle,
    Armed {
        channel_id: RuntimeFilterChannelId,
        route: BackendNativeContributionRouteIdentity,
    },
    DuplicateObserved,
}

#[cfg(debug_assertions)]
impl AcceptedContributionRetryRendezvous {
    fn new() -> Self {
        Self {
            state: Mutex::new(AcceptedContributionRetryRendezvousState::Idle),
            changed: Condvar::new(),
        }
    }

    fn arm(
        &self,
        channel_id: RuntimeFilterChannelId,
        route: BackendNativeContributionRouteIdentity,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        *state = AcceptedContributionRetryRendezvousState::Armed { channel_id, route };
    }

    fn observe_duplicate(
        &self,
        kind: BackendEnvelopeKind,
        channel_id: RuntimeFilterChannelId,
        route: BackendNativeRouteIdentity,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let AcceptedContributionRetryRendezvousState::Armed {
            channel_id: armed_channel_id,
            route: armed_route,
        } = *state
        else {
            return;
        };
        if kind == BackendEnvelopeKind::Contribution
            && channel_id == armed_channel_id
            && route == BackendNativeRouteIdentity::Contribution(armed_route)
        {
            *state = AcceptedContributionRetryRendezvousState::DuplicateObserved;
            self.changed.notify_all();
        }
    }

    fn wait_for_duplicate(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while matches!(
            *state,
            AcceptedContributionRetryRendezvousState::Armed { .. }
        ) {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }
}

pub type RuntimeFilterParticipantCloseHook = Arc<
    dyn Fn(
            &RuntimeFilterParticipant,
            QueryTerminationReason,
        ) -> Result<(), RuntimeFilterContractError>
        + Send
        + Sync,
>;

impl RuntimeFilterParticipant {
    #[allow(clippy::too_many_arguments)]
    fn from_installed(
        execution_id: QueryExecutionId,
        install: BackendParticipantInstall,
        state: Arc<WorkerRuntimeFilterParticipant>,
        transport_policy: BackendRuntimeFilterRetryPolicy,
        transport_sink: Arc<dyn BackendRuntimeFilterEnvelopeSink>,
    ) -> Result<Arc<Self>, RuntimeFilterContractError> {
        let outbound = Arc::new(BackendParticipantOutbound::new(
            install.clone(),
            transport_policy,
            transport_sink,
            state.observation_emitter(),
        ));
        let sink = Arc::clone(&outbound) as Arc<dyn BackendMaterializedDeliverySink>;
        state.set_materialized_delivery_sink(sink);
        Ok(Arc::new(Self {
            execution_id,
            state,
            outbound,
            close_hook: Arc::new(|_, _| Ok(())),
            #[cfg(debug_assertions)]
            accepted_contribution_retry_rendezvous: AcceptedContributionRetryRendezvous::new(),
        }))
    }

    pub fn session_for_fragment(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        required: bool,
    ) -> Result<Option<RuntimeFilterSessionRef>, RuntimeFilterContractError> {
        if execution_id != self.execution_id {
            return Err(RuntimeFilterContractError::new(
                RuntimeFilterContractErrorCode::ParticipantClosed,
                "runtime filter participant does not belong to this execution attempt",
            ));
        }
        if !required {
            return Ok(None);
        }
        let outbound: Arc<dyn RuntimeFilterParticipantOutbound> = self.outbound.clone();
        Ok(Some(
            self.state
                .session_for_fragment(fragment_instance_id, outbound),
        ))
    }

    pub fn dispatch_envelope(
        &self,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> BackendIngressResult {
        let query_id = UniqueId::new(
            self.execution_id.query_id().high(),
            self.execution_id.query_id().low(),
        );
        if self.state.is_cancelled()
            || envelope.query_id() != query_id
            || envelope.deployment_epoch() != self.state.participant().deployment_epoch()
        {
            return rejected(QUERY_UNAVAILABLE_REJECTION);
        }
        let kind = envelope.kind();
        let channel_id = envelope.channel_id();
        let route = *envelope.route_identity();
        let result = match kind {
            BackendEnvelopeKind::Contribution | BackendEnvelopeKind::ProducerClosed => {
                self.dispatch_producer_envelope(envelope)
            }
            BackendEnvelopeKind::ProducerUnavailable => self.dispatch_producer_failure(envelope),
            BackendEnvelopeKind::Ack => rejected(ACK_UNSUPPORTED_REJECTION),
            BackendEnvelopeKind::Artifact
            | BackendEnvelopeKind::FinalArtifact
            | BackendEnvelopeKind::Unavailable
            | BackendEnvelopeKind::CompletedWithoutArtifact
            | BackendEnvelopeKind::DegradedLogical => self.dispatch_delivery_envelope(envelope),
        };
        #[cfg(debug_assertions)]
        if result.status()
            == novarocks_worker::runtime_filter::domain::BackendAcceptStatus::Duplicate
        {
            self.accepted_contribution_retry_rendezvous
                .observe_duplicate(kind, channel_id, route);
        }
        result
    }

    /// Arms the system-test-only receiver rendezvous after this participant
    /// has accepted the contribution whose unary response is being dropped.
    #[cfg(debug_assertions)]
    pub fn arm_accepted_contribution_retry_rendezvous(
        &self,
        channel_id: RuntimeFilterChannelId,
        route: BackendNativeContributionRouteIdentity,
    ) {
        self.accepted_contribution_retry_rendezvous
            .arm(channel_id, route);
    }

    /// Blocks only a debug-faulted participant's release until its exact
    /// duplicate retry arrives.  The ordinary participant never waits here.
    #[cfg(debug_assertions)]
    pub fn wait_for_accepted_contribution_retry_rendezvous(&self) {
        self.accepted_contribution_retry_rendezvous
            .wait_for_duplicate();
    }

    fn dispatch_delivery_envelope(
        &self,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> BackendIngressResult {
        let Some(identity) = envelope.route_identity().as_delivery() else {
            return rejected(DELIVERY_REJECTION);
        };
        self.state.dispatch_delivery(
            participant_ingress::DeliveryIngressRoute::new(
                envelope.channel_id(),
                identity.route_edge_id(),
                identity.sequence(),
            ),
            participant_ingress::DeliveryIngressFrame::new(
                envelope.kind(),
                *envelope.schema_digest(),
                envelope.payload(),
            ),
        )
    }

    fn dispatch_producer_envelope(
        &self,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> BackendIngressResult {
        let Some(identity) = envelope.route_identity().as_contribution() else {
            return rejected(
                "runtime filter ingress rejected [route-identity]: contribution route identity is required",
            );
        };
        let Some(open) = envelope.producer_open() else {
            return rejected(
                "runtime filter ingress rejected [producer-open]: producer open metadata is required",
            );
        };
        let route = participant_ingress::ProducerIngressRoute::new(
            envelope.channel_id(),
            identity.producer_binding_id(),
            identity.fragment_instance_id(),
            novarocks_execution::runtime_filter::PartitionId::new(identity.partition_id().get()),
            novarocks_execution::runtime_filter::ProducerSequence::new(identity.sequence().get()),
            open.local_partition_count().get(),
        );
        let command = match envelope.kind() {
            BackendEnvelopeKind::Contribution => {
                participant_ingress::ProducerIngressCommand::Contribution {
                    schema_digest: *envelope.schema_digest(),
                    payload: Arc::<[u8]>::from(envelope.payload()),
                }
            }
            BackendEnvelopeKind::ProducerClosed => {
                participant_ingress::ProducerIngressCommand::Closed
            }
            _ => unreachable!("caller selects producer envelope kinds"),
        };
        self.state.dispatch_producer(route, command)
    }

    fn dispatch_producer_failure(
        &self,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> BackendIngressResult {
        let Some(identity) = envelope.route_identity().as_producer_instance() else {
            return rejected(
                "runtime filter ingress rejected [route-identity]: producer-instance route identity is required",
            );
        };
        self.state.dispatch_producer_failure(
            envelope.channel_id(),
            identity.producer_binding_id(),
            identity.fragment_instance_id(),
        )
    }

    pub fn close(&self, reason: QueryTerminationReason) -> Result<(), RuntimeFilterContractError> {
        self.state.close();
        (self.close_hook)(self, reason)
    }

    /// The attempt owner installs a weak egress only after control attachment.
    /// A participant never owns that queue, so lifecycle retirement cannot form
    /// a query-retention cycle through a background feedback publisher.
    pub fn set_frontend_feedback_sink(&self, sink: Weak<dyn BackendFrontendFeedbackSink>) {
        self.state.set_frontend_feedback_sink(sink);
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn capture_runtime_filter_observation(&self) -> RuntimeFilterObservationSnapshot {
        self.state.capture_observation()
    }

    /// Drains sender-side completions, records the required terminal facts, and
    /// freezes the only runtime-filter observation snapshot eligible for query
    /// terminalization. Repeated calls intentionally return that same proof.
    pub fn prepare_terminal_capture(
        &self,
        reason: QueryTerminationReason,
    ) -> RuntimeFilterObservationSnapshot {
        self.outbound.drain_transport_completions();
        self.state.prepare_terminal_capture(
            reason == QueryTerminationReason::QueryTerminationCoordinatorFinalize,
        )
    }

    pub fn record_row_effect(
        &self,
        fragment_instance_id: UniqueId,
        effect: novarocks_execution::runtime_filter::RuntimeFilterRowEffect,
    ) {
        self.state.record_row_effect(fragment_instance_id, effect);
    }

    pub fn record_scan_unit_outcome(
        &self,
        fragment_instance_id: UniqueId,
        outcome: novarocks_execution::runtime_filter::scan_domain::RuntimeFilterScanUnitOutcome,
    ) {
        self.state
            .record_scan_unit_outcome(fragment_instance_id, outcome);
    }

    #[doc(hidden)]
    pub fn with_close_hook_for_test(
        &self,
        close_hook: RuntimeFilterParticipantCloseHook,
    ) -> Arc<Self> {
        Arc::new(Self {
            execution_id: self.execution_id,
            state: Arc::clone(&self.state),
            outbound: Arc::clone(&self.outbound),
            close_hook,
            #[cfg(debug_assertions)]
            accepted_contribution_retry_rendezvous: AcceptedContributionRetryRendezvous::new(),
        })
    }
}

impl BackendRuntimeFilterEnvelopeIngress for RuntimeFilterParticipant {
    fn accept(&self, envelope: BackendNativeRuntimeFilterEnvelope) -> BackendIngressResult {
        self.dispatch_envelope(envelope)
    }
}

struct BackendParticipantOutbound {
    install: BackendParticipantInstall,
    transport_policy: BackendRuntimeFilterRetryPolicy,
    transport_sink: Arc<dyn BackendRuntimeFilterEnvelopeSink>,
    next_delivery_sequence: AtomicU64,
    observation: Arc<RuntimeFilterObservationEmitter>,
    pending_transport: Mutex<
        BTreeMap<BackendNativeRouteIdentity, VecDeque<(BackendTransportEventIdentity, usize)>>,
    >,
}

impl BackendParticipantOutbound {
    fn new(
        install: BackendParticipantInstall,
        transport_policy: BackendRuntimeFilterRetryPolicy,
        transport_sink: Arc<dyn BackendRuntimeFilterEnvelopeSink>,
        observation: Arc<RuntimeFilterObservationEmitter>,
    ) -> Self {
        Self {
            install,
            transport_policy,
            transport_sink,
            next_delivery_sequence: AtomicU64::new(1),
            observation,
            pending_transport: Mutex::new(BTreeMap::new()),
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The contribution envelope is frozen from distinct backend routing facts at the native boundary."
    )]
    fn forward_producer_contribution(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: novarocks_execution::runtime_filter::PartitionId,
        sequence: novarocks_execution::runtime_filter::ProducerSequence,
        local_partition_count: u32,
        contribution: novarocks_execution::runtime_filter::RuntimeFilterContribution,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let envelope = BackendNativeRuntimeFilterEnvelope::new(
            BackendEnvelopeKind::Contribution,
            self.install.participant(),
            channel_id,
            BackendNativeRouteIdentity::contribution(BackendNativeContributionRouteIdentity::new(
                binding_id,
                fragment_instance_id,
                partition,
                novarocks_worker::runtime_filter::domain::BackendTransportSequence::new(
                    sequence.get(),
                ),
            )),
            Some(
                novarocks_worker::runtime_filter::domain::BackendProducerOpenMetadata::try_new(
                    local_partition_count,
                )
                .map_err(|error| outbound_violation(error.to_string()))?,
            ),
            None,
            contribution.contract_digest(),
            contribution.canonical_bytes().clone(),
        )
        .map_err(outbound_violation)?;
        self.forward_producer(
            channel_id,
            binding_id,
            BackendEnvelopeKind::Contribution,
            envelope,
        )
    }

    fn forward_producer_close(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: novarocks_execution::runtime_filter::PartitionId,
        sequence: novarocks_execution::runtime_filter::ProducerSequence,
        local_partition_count: u32,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let envelope = BackendNativeRuntimeFilterEnvelope::new(
            BackendEnvelopeKind::ProducerClosed,
            self.install.participant(),
            channel_id,
            BackendNativeRouteIdentity::contribution(BackendNativeContributionRouteIdentity::new(
                binding_id,
                fragment_instance_id,
                partition,
                novarocks_worker::runtime_filter::domain::BackendTransportSequence::new(
                    sequence.get(),
                ),
            )),
            Some(
                novarocks_worker::runtime_filter::domain::BackendProducerOpenMetadata::try_new(
                    local_partition_count,
                )
                .map_err(|error| outbound_violation(error.to_string()))?,
            ),
            None,
            [0; 32],
            Arc::<[u8]>::from([]),
        )
        .map_err(outbound_violation)?;
        self.forward_producer(
            channel_id,
            binding_id,
            BackendEnvelopeKind::ProducerClosed,
            envelope,
        )
    }

    fn forward_producer_failure(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        reason: novarocks_execution::runtime_filter::RuntimeFilterProducerFailure,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let envelope = BackendNativeRuntimeFilterEnvelope::new(
            BackendEnvelopeKind::ProducerUnavailable,
            self.install.participant(),
            channel_id,
            BackendNativeRouteIdentity::producer_instance(
                BackendNativeProducerInstanceRouteIdentity::new(binding_id, fragment_instance_id),
            ),
            None,
            None,
            [0; 32],
            Arc::<[u8]>::from(producer_codec::encode_producer_failure(reason)),
        )
        .map_err(outbound_violation)?;
        self.forward_producer(
            channel_id,
            binding_id,
            BackendEnvelopeKind::ProducerUnavailable,
            envelope,
        )
    }

    fn forward_producer(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        kind: BackendEnvelopeKind,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let decision = match self
            .install
            .routing()
            .route_producer(channel_id, binding_id, kind)
        {
            Ok(decision) => decision,
            Err(BackendRoutingError::ForbiddenOutboundKind { .. }) => return Ok(()),
            Err(error) => return Err(outbound_violation(error.to_string())),
        };
        self.dispatch_remote_envelope_decision(&decision, envelope, binding_id)
    }

    fn dispatch_materialized(
        &self,
        delivery: BackendMaterializedDelivery,
    ) -> Result<(), RuntimeFilterContractViolation> {
        let decision = self
            .install
            .routing()
            .route_delivery(
                delivery.channel_id(),
                delivery.route_edge_ids(),
                delivery.kind(),
            )
            .map_err(|error| outbound_violation(error.to_string()))?;
        let binding_id = self
            .transport_binding_id(delivery.channel_id())
            .ok_or_else(|| {
                outbound_violation("runtime-filter transport channel has no installed binding")
            })?;
        for route in decision.remote_routes() {
            let envelope =
                self.delivery_envelope(&delivery, route.edge_id(), self.next_delivery_sequence())?;
            self.submit_remote(route.clone(), envelope, binding_id);
        }
        Ok(())
    }

    fn delivery_envelope(
        &self,
        delivery: &BackendMaterializedDelivery,
        route_edge_id: novarocks_worker::runtime_filter::domain::BackendRouteEdgeId,
        sequence: novarocks_worker::runtime_filter::domain::BackendTransportSequence,
    ) -> Result<BackendNativeRuntimeFilterEnvelope, RuntimeFilterContractViolation> {
        BackendNativeRuntimeFilterEnvelope::new(
            delivery.kind(),
            self.install.participant(),
            delivery.channel_id(),
            BackendNativeRouteIdentity::delivery(BackendNativeDeliveryRouteIdentity::new(
                route_edge_id,
                sequence,
            )),
            None,
            None,
            delivery.schema_digest(),
            delivery.payload().clone(),
        )
        .map_err(outbound_violation)
    }

    fn next_delivery_sequence(
        &self,
    ) -> novarocks_worker::runtime_filter::domain::BackendTransportSequence {
        novarocks_worker::runtime_filter::domain::BackendTransportSequence::new(
            self.next_delivery_sequence.fetch_add(1, Ordering::Relaxed),
        )
    }

    fn dispatch_remote_envelope_decision(
        &self,
        decision: &BackendRouteDecision,
        envelope: BackendNativeRuntimeFilterEnvelope,
        binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
    ) -> Result<(), RuntimeFilterContractViolation> {
        for route in decision.remote_routes() {
            self.submit_remote(route.clone(), envelope.clone(), binding_id);
        }
        Ok(())
    }

    fn submit_remote(
        &self,
        route: novarocks_worker::runtime_filter::domain::BackendRemoteRoute,
        envelope: BackendNativeRuntimeFilterEnvelope,
        binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
    ) {
        self.drain_transport_completions();
        let identity = BackendTransportEventIdentity::new(
            BackendChannelIdentity::new(
                self.install.participant(),
                binding_id,
                envelope.channel_id(),
            ),
            route.edge_id(),
        );
        let bytes = encode_runtime_filter_envelope(&envelope).encoded_len();
        let route_identity = *envelope.route_identity();
        let Ok(envelope) = BackendNativeRuntimeFilterTransportEnvelope::new(
            Arc::new(envelope),
            self.transport_policy,
        ) else {
            self.record_transport(
                identity,
                BackendTransportEventKind::FailedOpen(
                    BackendTransportFailOpenReason::ContractRejected,
                ),
                bytes,
            );
            return;
        };
        match self.transport_sink.try_send(route, envelope) {
            BackendRuntimeFilterSinkSubmitOutcome::Submitted => {
                self.pending_transport
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .entry(route_identity)
                    .or_default()
                    .push_back((identity, bytes));
                self.record_transport(identity, BackendTransportEventKind::Sent, bytes);
            }
            BackendRuntimeFilterSinkSubmitOutcome::QueueFull
            | BackendRuntimeFilterSinkSubmitOutcome::Shutdown => {
                self.record_transport(
                    identity,
                    BackendTransportEventKind::FailedOpen(BackendTransportFailOpenReason::Deadline),
                    bytes,
                );
            }
        }
    }

    fn transport_binding_id(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
    ) -> Option<novarocks_execution::runtime_filter::RuntimeFilterBindingId> {
        let channel = self.install.channels().get(&channel_id)?;
        channel
            .producers()
            .keys()
            .next()
            .copied()
            .or_else(|| channel.consumers().keys().next().copied())
    }

    fn record_transport(
        &self,
        identity: BackendTransportEventIdentity,
        kind: BackendTransportEventKind,
        bytes: usize,
    ) {
        self.observation
            .record(BackendRuntimeFilterEvent::TransportEnvelope {
                identity,
                kind,
                bytes,
            });
    }

    fn drain_transport_completions(&self) {
        while let Some(completion) = self.transport_sink.try_recv_completion() {
            let (route_identity, kind, terminal) = match completion {
                BackendRuntimeFilterSinkCompletion::Ack(route_identity, status) => (
                    route_identity,
                    BackendTransportEventKind::Acked(status),
                    true,
                ),
                BackendRuntimeFilterSinkCompletion::Retried(route_identity) => {
                    (route_identity, BackendTransportEventKind::Retried, false)
                }
                BackendRuntimeFilterSinkCompletion::TransportFailure(route_identity, _, reason) => {
                    let reason = match reason {
                        BackendRuntimeFilterTransportFailureReason::Deadline => {
                            BackendTransportFailOpenReason::Deadline
                        }
                        BackendRuntimeFilterTransportFailureReason::AttemptsExhausted => {
                            BackendTransportFailOpenReason::AttemptsExhausted
                        }
                        BackendRuntimeFilterTransportFailureReason::ContractRejected => {
                            BackendTransportFailOpenReason::ContractRejected
                        }
                    };
                    (
                        route_identity,
                        BackendTransportEventKind::FailedOpen(reason),
                        true,
                    )
                }
            };
            let pending = &mut *self
                .pending_transport
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let Some(queue) = pending.get_mut(&route_identity) else {
                continue;
            };
            if !terminal {
                let Some((identity, bytes)) = queue.front().copied() else {
                    pending.remove(&route_identity);
                    continue;
                };
                self.record_transport(identity, kind, bytes);
                continue;
            }
            let Some((identity, bytes)) = queue.pop_front() else {
                pending.remove(&route_identity);
                continue;
            };
            if queue.is_empty() {
                pending.remove(&route_identity);
            }
            self.record_transport(identity, kind, bytes);
        }
    }
}

impl RuntimeFilterParticipantOutbound for BackendParticipantOutbound {
    fn forward_producer_contribution(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: novarocks_execution::runtime_filter::PartitionId,
        sequence: novarocks_execution::runtime_filter::ProducerSequence,
        local_partition_count: u32,
        contribution: novarocks_execution::runtime_filter::RuntimeFilterContribution,
    ) -> Result<(), RuntimeFilterContractViolation> {
        BackendParticipantOutbound::forward_producer_contribution(
            self,
            channel_id,
            binding_id,
            fragment_instance_id,
            partition,
            sequence,
            local_partition_count,
            contribution,
        )
    }

    fn forward_producer_close(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition: novarocks_execution::runtime_filter::PartitionId,
        sequence: novarocks_execution::runtime_filter::ProducerSequence,
        local_partition_count: u32,
    ) -> Result<(), RuntimeFilterContractViolation> {
        BackendParticipantOutbound::forward_producer_close(
            self,
            channel_id,
            binding_id,
            fragment_instance_id,
            partition,
            sequence,
            local_partition_count,
        )
    }

    fn forward_producer_failure(
        &self,
        channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
        binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        reason: novarocks_execution::runtime_filter::RuntimeFilterProducerFailure,
    ) -> Result<(), RuntimeFilterContractViolation> {
        BackendParticipantOutbound::forward_producer_failure(
            self,
            channel_id,
            binding_id,
            fragment_instance_id,
            reason,
        )
    }
}

impl BackendMaterializedDeliverySink for BackendParticipantOutbound {
    fn dispatch(
        &self,
        delivery: BackendMaterializedDelivery,
    ) -> Result<(), RuntimeFilterContractViolation> {
        self.dispatch_materialized(delivery)
    }
}

fn rejected(reason: &'static str) -> BackendIngressResult {
    BackendIngressResult::rejected(reason).expect("runtime-filter rejection reason is non-empty")
}

fn violation(
    kind: RuntimeFilterContractViolationKind,
    detail: impl Into<Arc<str>>,
) -> RuntimeFilterContractViolation {
    RuntimeFilterContractViolation::new(kind, detail)
}

fn outbound_violation(detail: impl Into<Arc<str>>) -> RuntimeFilterContractViolation {
    violation(RuntimeFilterContractViolationKind::ContractMismatch, detail)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use arrow::datatypes::DataType;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_execution::runtime::mem_tracker::MemTracker;
    use novarocks_execution::runtime_filter::{
        LogicalVersion, RuntimeFilterBindOutcome, RuntimeFilterChannelId,
        RuntimeFilterConsumerContract, RuntimeFilterExecutionContract, RuntimeFilterFinalDomain,
        RuntimeFilterFinalDomainCompletionHandle, RuntimeFilterFinalDomainOpenRequest,
        RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics, RuntimeFilterProducerContract,
        RuntimeFilterProducerFailure, RuntimeFilterProducerOpenRequest,
        RuntimeFilterSubscriptionHandle, RuntimeFilterSubscriptionRequest, SnapshotAcquireOutcome,
        UnavailableReason, contribution,
    };
    use novarocks_proto_codec::lifecycle::AttemptId;
    use novarocks_types::QueryId;

    use super::*;
    use crate::runtime_filter_transport::{
        BackendRuntimeFilterSinkCompletion, BackendRuntimeFilterSinkSubmitOutcome,
    };
    use novarocks_worker::runtime_filter::artifact::{ArtifactKind, ConsumerArtifactProfile};
    use novarocks_worker::runtime_filter::codec::artifact as artifact_codec;
    use novarocks_worker::runtime_filter::domain::{
        BackendChannelInstall, BackendChannelLifecycle, BackendConsumerInstall, BackendCoverage,
        BackendIngressDedupe, BackendMaterializationOwner, BackendMaterializationPolicy,
        BackendOutboundMaterializationGroup, BackendParticipantIdentity,
        BackendProducerOpenMetadata, BackendRemoteRoute, BackendRouteEdgeId, BackendRouteEndpoint,
        BackendRoutePeer, BackendRouteRole, BackendRoutingChannel, BackendRoutingEdge,
        BackendRoutingShard, BackendRuntimeFilterSession,
    };
    use novarocks_worker::runtime_filter::fixture::BackendRuntimeFilterFixture;

    impl RuntimeFilterParticipant {
        #[allow(clippy::too_many_arguments)]
        fn from_test_parts(
            execution_id: QueryExecutionId,
            install: BackendParticipantInstall,
            observation: Arc<RuntimeFilterObservationEmitter>,
            transport_policy: BackendRuntimeFilterRetryPolicy,
            producer_sessions: BTreeMap<
                novarocks_execution::runtime_filter::RuntimeFilterBindingId,
                Arc<BackendRuntimeFilterSession>,
            >,
            consumer_sessions: BTreeMap<
                novarocks_execution::runtime_filter::RuntimeFilterBindingId,
                Arc<BackendRuntimeFilterSession>,
            >,
            memory: Arc<MemTracker>,
            transport_sink: Arc<dyn BackendRuntimeFilterEnvelopeSink>,
        ) -> Result<Arc<Self>, RuntimeFilterContractError> {
            let state = Arc::new(WorkerRuntimeFilterParticipant::new(
                install.clone(),
                observation,
                producer_sessions,
                consumer_sessions,
                Arc::new(BackendIngressDedupe::new(16_384)),
                Arc::new(AtomicBool::new(false)),
                memory,
            ));
            Self::from_installed(
                execution_id,
                install,
                state,
                transport_policy,
                transport_sink,
            )
        }
    }

    struct ForwardingSink {
        target: Arc<RuntimeFilterParticipant>,
    }

    impl BackendRuntimeFilterEnvelopeSink for ForwardingSink {
        fn try_send(
            &self,
            _route: BackendRemoteRoute,
            envelope: BackendNativeRuntimeFilterTransportEnvelope,
        ) -> BackendRuntimeFilterSinkSubmitOutcome {
            let (envelope, _) = envelope.into_parts();
            let result = self.target.dispatch_envelope((*envelope).clone());
            assert!(
                matches!(
                    result.status(),
                    novarocks_worker::runtime_filter::domain::BackendAcceptStatus::Accepted
                        | novarocks_worker::runtime_filter::domain::BackendAcceptStatus::Duplicate
                ),
                "remote envelope rejected: {:?}",
                result.rejection_reason()
            );
            let replay = self.target.dispatch_envelope((*envelope).clone());
            assert!(
                matches!(
                    replay.status(),
                    novarocks_worker::runtime_filter::domain::BackendAcceptStatus::Accepted
                        | novarocks_worker::runtime_filter::domain::BackendAcceptStatus::Duplicate
                ),
                "replayed remote envelope rejected: {:?}",
                replay.rejection_reason()
            );
            BackendRuntimeFilterSinkSubmitOutcome::Submitted
        }

        fn try_recv_completion(&self) -> Option<BackendRuntimeFilterSinkCompletion> {
            None
        }

        fn shutdown(&self) {}
    }

    struct DiscardSink;

    impl BackendRuntimeFilterEnvelopeSink for DiscardSink {
        fn try_send(
            &self,
            _route: BackendRemoteRoute,
            _envelope: BackendNativeRuntimeFilterTransportEnvelope,
        ) -> BackendRuntimeFilterSinkSubmitOutcome {
            BackendRuntimeFilterSinkSubmitOutcome::Submitted
        }

        fn try_recv_completion(&self) -> Option<BackendRuntimeFilterSinkCompletion> {
            None
        }

        fn shutdown(&self) {}
    }

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(17, 19),
            AttemptId::new(23).expect("nonzero attempt"),
        )
        .expect("valid execution id")
    }

    fn transport_policy() -> BackendRuntimeFilterRetryPolicy {
        BackendRuntimeFilterRetryPolicy::new(
            Duration::from_millis(1),
            3,
            Duration::from_secs(1),
            64,
            64 * 1024,
        )
        .expect("test transport policy")
    }

    fn endpoint(port: i32) -> RuntimeEndpoint {
        RuntimeEndpoint::new("127.0.0.1", port).expect("valid endpoint")
    }

    fn final_domain_participant(
        max_contribution_bytes: usize,
    ) -> (
        Arc<RuntimeFilterParticipant>,
        RuntimeFilterProducerContract,
        UniqueId,
    ) {
        let participant_identity = BackendParticipantIdentity::new(UniqueId::new(701, 703), 23);
        let fragment_instance = UniqueId::new(709, 711);
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .expect("Int64 membership schema is supported");
        let contract = RuntimeFilterExecutionContract::Membership(schema);
        let producer = RuntimeFilterProducerContract::final_domain(
            novarocks_execution::runtime_filter::RuntimeFilterBindingId::new(71),
            RuntimeFilterChannelId::new(73),
            contract.clone(),
        )
        .expect("FinalDomain producer contract is valid");
        let coverage_witness =
            novarocks_worker::runtime_filter::domain::BackendCoverageWitnessId::new(79);
        let coverage = BackendCoverage::witness(coverage_witness);
        let channel = BackendChannelInstall::new(
            producer.channel_id(),
            contract,
            BackendChannelLifecycle::CompleteOnce,
            coverage.clone(),
            coverage,
            BackendMaterializationPolicy::new(8, 3, 5, 1, 4096, 4096, 1)
                .expect("materialization policy"),
            max_contribution_bytes,
            4096,
            [
                novarocks_worker::runtime_filter::domain::BackendProducerInstall::new(
                    producer.clone(),
                    coverage_witness,
                    [fragment_instance],
                    max_contribution_bytes,
                )
                .expect("FinalDomain producer install"),
            ],
            [],
            [],
        )
        .expect("FinalDomain channel install");
        let routing = BackendRoutingShard::new(
            participant_identity,
            1,
            [BackendRoutingChannel::new(
                producer.channel_id(),
                [BackendRouteRole::Producer(producer.binding_id())],
                [],
                [],
                [((producer.binding_id(), fragment_instance), 1)],
            )
            .expect("FinalDomain routing channel")],
        )
        .expect("FinalDomain routing install");
        let install = BackendParticipantInstall::new(participant_identity, 1, [channel], routing)
            .expect("FinalDomain participant install");
        let observation = RuntimeFilterObservationEmitter::from_install(&install, None);
        let session = Arc::new(
            BackendRuntimeFilterSession::from_channel_install(
                participant_identity,
                install.channels()[&producer.channel_id()].clone(),
                observation.clone(),
            )
            .expect("FinalDomain Backend session"),
        );
        let participant = RuntimeFilterParticipant::from_test_parts(
            execution_id(),
            install,
            observation,
            transport_policy(),
            BTreeMap::from([(producer.binding_id(), session)]),
            BTreeMap::new(),
            MemTracker::new_root("runtime_filter_final_domain_completion_test"),
            Arc::new(DiscardSink),
        )
        .expect("FinalDomain participant");
        (participant, producer, fragment_instance)
    }

    fn open_final_domain_completion(
        participant: &Arc<RuntimeFilterParticipant>,
        producer: RuntimeFilterProducerContract,
        fragment_instance: UniqueId,
        local_partition_count: u32,
    ) -> RuntimeFilterFinalDomainCompletionHandle {
        let session = participant
            .session_for_fragment(execution_id(), fragment_instance, true)
            .expect("participant session")
            .expect("required participant session");
        let RuntimeFilterBindOutcome::Bound(completion) = session
            .open_final_domain_completion(RuntimeFilterFinalDomainOpenRequest::new(
                producer,
                local_partition_count,
            ))
            .expect("FinalDomain completion binding")
        else {
            panic!("installed FinalDomain completion must bind");
        };
        completion
    }

    fn final_domain(
        values: impl IntoIterator<Item = i64>,
        digest: [u8; 32],
        max_canonical_bytes: usize,
    ) -> RuntimeFilterFinalDomain {
        RuntimeFilterFinalDomain::from_value_domain(
            &contribution::ValueDomainDelta::new(
                contribution::MembershipValues::int64(values),
                false,
            ),
            digest,
            max_canonical_bytes,
        )
        .expect("FinalDomain payload is canonical")
    }

    #[test]
    fn final_domain_completion_requires_every_partition_to_seal_and_close() {
        let (participant, producer, fragment_instance) = final_domain_participant(1024);
        let completion = open_final_domain_completion(&participant, producer, fragment_instance, 2);

        let mut first = completion
            .claim_partition(novarocks_execution::runtime_filter::PartitionId::new(0))
            .expect("first partition claim");
        let error = first.close().expect_err("close before seal is rejected");
        assert_eq!(
            error.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
        first
            .seal(final_domain(
                [3, 9],
                completion.contract_digest(),
                completion.max_domain_canonical_bytes(),
            ))
            .expect("first partition seal");
        first.close().expect("first partition close");
        assert!(
            participant
                .capture_runtime_filter_observation()
                .channels()
                .iter()
                .all(|channel| channel.completed() == 0)
        );

        let mut second = completion
            .claim_partition(novarocks_execution::runtime_filter::PartitionId::new(1))
            .expect("second partition claim");
        second
            .seal(final_domain(
                [11],
                completion.contract_digest(),
                completion.max_domain_canonical_bytes(),
            ))
            .expect("second partition seal");
        second.close().expect("second partition close");

        let snapshot = participant.capture_runtime_filter_observation();
        assert!(
            snapshot
                .channels()
                .iter()
                .any(|channel| channel.completed() == 1)
        );
        assert!(snapshot.producer_streams().iter().all(|stream| {
            stream.latest_accepted_sequence() == Some(0) && stream.accepted() == 1
        }));
    }

    #[test]
    fn final_domain_completion_rejects_duplicate_and_out_of_range_claims() {
        let (participant, producer, fragment_instance) = final_domain_participant(1024);
        let completion = open_final_domain_completion(&participant, producer, fragment_instance, 2);

        completion
            .claim_partition(novarocks_execution::runtime_filter::PartitionId::new(0))
            .expect("first claim");
        let Err(duplicate) =
            completion.claim_partition(novarocks_execution::runtime_filter::PartitionId::new(0))
        else {
            panic!("duplicate partition claim must be rejected");
        };
        assert_eq!(
            duplicate.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
        let Err(out_of_range) =
            completion.claim_partition(novarocks_execution::runtime_filter::PartitionId::new(2))
        else {
            panic!("partition outside the declared count must be rejected");
        };
        assert_eq!(
            out_of_range.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
        assert!(
            participant
                .capture_runtime_filter_observation()
                .channels()
                .iter()
                .all(|channel| channel.completed() == 0)
        );
    }

    #[test]
    fn terminal_prepare_seals_one_idempotent_observation_proof() {
        let (participant, producer, _) = final_domain_participant(1024);
        let first = participant
            .prepare_terminal_capture(QueryTerminationReason::QueryTerminationLocalFailure);
        assert!(
            first
                .channels()
                .iter()
                .all(|channel| channel.cancelled() == 1),
            "local failure must record cancellation before sealing"
        );

        let late_channel = BackendChannelIdentity::new(
            participant.outbound.install.participant(),
            producer.binding_id(),
            producer.channel_id(),
        );
        participant
            .outbound
            .observation
            .record(BackendRuntimeFilterEvent::ChannelCancelled {
                channel: late_channel,
            });

        let second = participant
            .prepare_terminal_capture(QueryTerminationReason::QueryTerminationLocalFailure);
        assert_eq!(second, first, "terminal proof is frozen exactly once");
        assert_eq!(
            participant.capture_runtime_filter_observation(),
            first,
            "late writes cannot alter the retained terminal proof"
        );
    }

    #[test]
    fn terminal_prepare_preserves_completed_channel_during_cancellation() {
        let (participant, producer, _) = final_domain_participant(1024);
        let channel = BackendChannelIdentity::new(
            participant.outbound.install.participant(),
            producer.binding_id(),
            producer.channel_id(),
        );
        participant
            .outbound
            .observation
            .record(BackendRuntimeFilterEvent::ChannelCompleted {
                channel,
                version: LogicalVersion::FIRST,
            });

        let first = participant
            .prepare_terminal_capture(QueryTerminationReason::QueryTerminationLocalFailure);
        let channel = first
            .channels()
            .iter()
            .find(|candidate| candidate.identity() == channel)
            .expect("installed channel is retained");
        assert_eq!(
            channel.terminal(),
            Some(
                novarocks_worker::runtime_filter::observation::RuntimeFilterChannelTerminal::Completed(
                    LogicalVersion::FIRST
                )
            )
        );
        assert_eq!(channel.completed(), 1);
        assert_eq!(channel.cancelled(), 0);

        let second = participant
            .prepare_terminal_capture(QueryTerminationReason::QueryTerminationLocalFailure);
        assert_eq!(
            second, first,
            "repeated cancellation capture preserves the same completed proof"
        );
    }

    #[test]
    fn final_domain_completion_rejects_double_seal_and_schema_or_digest_drift() {
        let (participant, producer, fragment_instance) = final_domain_participant(1024);
        let completion = open_final_domain_completion(&participant, producer, fragment_instance, 3);

        let mut schema_drift = completion
            .claim_partition(novarocks_execution::runtime_filter::PartitionId::new(0))
            .expect("schema-drift partition claim");
        let wrong_schema = RuntimeFilterFinalDomain::from_value_domain(
            &contribution::ValueDomainDelta::new(contribution::MembershipValues::int32([3]), false),
            completion.contract_digest(),
            completion.max_domain_canonical_bytes(),
        )
        .expect("Int32 FinalDomain payload is canonical");
        let error = schema_drift
            .seal(wrong_schema)
            .expect_err("schema drift is rejected");
        assert_eq!(
            error.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
        schema_drift
            .seal(final_domain(
                [3],
                completion.contract_digest(),
                completion.max_domain_canonical_bytes(),
            ))
            .expect("valid seal after schema rejection");
        let double_seal = schema_drift
            .seal(final_domain(
                [5],
                completion.contract_digest(),
                completion.max_domain_canonical_bytes(),
            ))
            .expect_err("second seal is rejected");
        assert_eq!(
            double_seal.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
        schema_drift.close().expect("schema-drift partition close");

        let mut digest_drift = completion
            .claim_partition(novarocks_execution::runtime_filter::PartitionId::new(1))
            .expect("digest-drift partition claim");
        let error = digest_drift
            .seal(final_domain(
                [7],
                [99; 32],
                completion.max_domain_canonical_bytes(),
            ))
            .expect_err("digest drift is rejected");
        assert_eq!(
            error.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
        digest_drift
            .seal(final_domain(
                [7],
                completion.contract_digest(),
                completion.max_domain_canonical_bytes(),
            ))
            .expect("valid seal after digest rejection");
        digest_drift.close().expect("digest-drift partition close");

        assert!(
            participant
                .capture_runtime_filter_observation()
                .channels()
                .iter()
                .all(|channel| channel.completed() == 0)
        );
    }

    #[test]
    fn final_domain_completion_rejects_payload_above_installed_canonical_budget() {
        let (participant, producer, fragment_instance) = final_domain_participant(64);
        let completion = open_final_domain_completion(&participant, producer, fragment_instance, 1);
        let mut partition = completion
            .claim_partition(novarocks_execution::runtime_filter::PartitionId::new(0))
            .expect("partition claim");
        let oversized = final_domain(0_i64..128, completion.contract_digest(), 16 * 1024);
        assert!(oversized.canonical_bytes().len() > completion.max_domain_canonical_bytes());
        let error = partition
            .seal(oversized)
            .expect_err("payload above installed canonical budget is rejected");
        assert_eq!(
            error.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
        assert!(
            participant
                .capture_runtime_filter_observation()
                .channels()
                .iter()
                .all(|channel| channel.completed() == 0)
        );
    }

    #[test]
    fn final_domain_completion_fail_opens_after_failure_or_participant_cancellation() {
        let (participant, producer, fragment_instance) = final_domain_participant(1024);
        let completion =
            open_final_domain_completion(&participant, producer.clone(), fragment_instance, 1);
        assert_eq!(
            completion
                .fail(RuntimeFilterProducerFailure::Cancelled)
                .expect("producer failure is admitted"),
            novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome::CompletedWithoutArtifact
        );

        participant
            .close(QueryTerminationReason::QueryTerminationLocalFailure)
            .expect("participant cancellation");
        let session = participant
            .session_for_fragment(execution_id(), fragment_instance, true)
            .expect("participant session")
            .expect("required participant session");
        assert!(matches!(
            session
                .open_final_domain_completion(RuntimeFilterFinalDomainOpenRequest::new(producer, 1))
                .expect("cancelled participant fails open"),
            RuntimeFilterBindOutcome::Unavailable(UnavailableReason::RouteUnavailable)
        ));
    }

    #[test]
    fn direct_source_materialization_reaches_remote_blocking_consumer() {
        let fixture = BackendRuntimeFilterFixture::membership();
        let identity = fixture.identity();
        let source_instance = UniqueId::new(101, 102);
        let consumer_instance = UniqueId::new(201, 202);
        let producer = fixture.producer_contract();
        let execution_contract = producer.contract().clone();
        let consumer_contract = RuntimeFilterConsumerContract::membership_blocking(
            novarocks_execution::runtime_filter::RuntimeFilterBindingId::new(70),
            producer.channel_id(),
            execution_contract.clone(),
        )
        .expect("consumer contract");
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .expect("membership profile");
        let policy = BackendMaterializationPolicy::new(8, 3, 5, 1, 4096, 4096, 1)
            .expect("materialization policy");
        let edge_id = BackendRouteEdgeId::new(501);
        let source_endpoint =
            BackendRouteEndpoint::new(1, BackendRouteRole::Producer(producer.binding_id()))
                .expect("source endpoint");
        let target_endpoint = BackendRouteEndpoint::new(
            2,
            BackendRouteRole::Consumer(consumer_contract.binding_id()),
        )
        .expect("target endpoint");
        let source_edge = BackendRoutingEdge::new(
            edge_id,
            source_endpoint.clone(),
            target_endpoint.clone(),
            BackendRoutePeer::Remote {
                participant_id: 2,
                endpoint: endpoint(9072),
            },
            [
                BackendEnvelopeKind::Artifact,
                BackendEnvelopeKind::FinalArtifact,
            ],
        )
        .expect("source route");
        let target_edge = BackendRoutingEdge::new(
            edge_id,
            source_endpoint,
            target_endpoint,
            BackendRoutePeer::Remote {
                participant_id: 1,
                endpoint: endpoint(9071),
            },
            [
                BackendEnvelopeKind::Artifact,
                BackendEnvelopeKind::FinalArtifact,
            ],
        )
        .expect("target route");
        let source_channel = BackendChannelInstall::new(
            producer.channel_id(),
            execution_contract.clone(),
            BackendChannelLifecycle::CompleteOnce,
            fixture.coverage(),
            fixture.coverage(),
            policy.clone(),
            4096,
            4096,
            [
                novarocks_worker::runtime_filter::domain::BackendProducerInstall::new(
                    producer.clone(),
                    novarocks_worker::runtime_filter::domain::BackendCoverageWitnessId::new(29),
                    [source_instance],
                    4096,
                )
                .expect("source producer"),
            ],
            [],
            [BackendOutboundMaterializationGroup::new(
                BackendMaterializationOwner::DirectSource,
                profile.clone(),
                [edge_id],
            )
            .expect("direct materialization group")],
        )
        .expect("source channel");
        let target_channel = BackendChannelInstall::new(
            producer.channel_id(),
            execution_contract,
            BackendChannelLifecycle::CompleteOnce,
            BackendCoverage::witness(
                novarocks_worker::runtime_filter::domain::BackendCoverageWitnessId::new(29),
            ),
            BackendCoverage::witness(
                novarocks_worker::runtime_filter::domain::BackendCoverageWitnessId::new(29),
            ),
            policy,
            4096,
            4096,
            [],
            [BackendConsumerInstall::new(
                consumer_contract.clone(),
                profile,
                [edge_id],
                [consumer_instance],
            )
            .expect("target consumer")],
            [],
        )
        .expect("target channel");
        let source_routing = BackendRoutingShard::new(
            identity,
            1,
            [BackendRoutingChannel::new(
                producer.channel_id(),
                [BackendRouteRole::Producer(producer.binding_id())],
                [],
                [source_edge],
                [((producer.binding_id(), source_instance), 1)],
            )
            .expect("source routing channel")],
        )
        .expect("source routing");
        let target_routing = BackendRoutingShard::new(
            identity,
            2,
            [BackendRoutingChannel::new(
                producer.channel_id(),
                [BackendRouteRole::Consumer(consumer_contract.binding_id())],
                [target_edge],
                [],
                [],
            )
            .expect("target routing channel")],
        )
        .expect("target routing");
        let source_install =
            BackendParticipantInstall::new(identity, 1, [source_channel], source_routing)
                .expect("source install");
        let target_install =
            BackendParticipantInstall::new(identity, 2, [target_channel], target_routing)
                .expect("target install");
        let target_observation =
            RuntimeFilterObservationEmitter::from_install(&target_install, None);
        let target_session = Arc::new(
            BackendRuntimeFilterSession::from_channel_install(
                identity,
                target_install.channels()[&producer.channel_id()].clone(),
                target_observation.clone(),
            )
            .expect("target session"),
        );
        let target = RuntimeFilterParticipant::from_test_parts(
            execution_id(),
            target_install,
            target_observation,
            transport_policy(),
            BTreeMap::new(),
            BTreeMap::from([(consumer_contract.binding_id(), target_session)]),
            MemTracker::new_root("runtime_filter_remote_consumer_test"),
            Arc::new(DiscardSink),
        )
        .expect("target participant");
        let source_observation =
            RuntimeFilterObservationEmitter::from_install(&source_install, None);
        let source_session = Arc::new(
            BackendRuntimeFilterSession::from_channel_install(
                identity,
                source_install.channels()[&producer.channel_id()].clone(),
                source_observation.clone(),
            )
            .expect("source session"),
        );
        let source = RuntimeFilterParticipant::from_test_parts(
            execution_id(),
            source_install,
            source_observation,
            transport_policy(),
            BTreeMap::from([(producer.binding_id(), source_session)]),
            BTreeMap::new(),
            MemTracker::new_root("runtime_filter_remote_source_test"),
            Arc::new(ForwardingSink {
                target: Arc::clone(&target),
            }),
        )
        .expect("source participant");

        let target_context = target
            .session_for_fragment(execution_id(), consumer_instance, true)
            .expect("target context")
            .expect("required target context");
        let RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Blocking(
            subscription,
        )) = target_context
            .subscribe(RuntimeFilterSubscriptionRequest::new(consumer_contract))
            .expect("target subscription")
        else {
            panic!("target consumer must bind a blocking subscription");
        };
        let source_context = source
            .session_for_fragment(execution_id(), source_instance, true)
            .expect("source context")
            .expect("required source context");
        let RuntimeFilterBindOutcome::Bound(producer_handle) = source_context
            .open_producer(RuntimeFilterProducerOpenRequest::new(producer, 1))
            .expect("source producer")
        else {
            panic!("source producer must bind");
        };
        producer_handle
            .submit(
                novarocks_execution::runtime_filter::PartitionId::new(0),
                novarocks_execution::runtime_filter::ProducerSequence::new(0),
                fixture.membership_contribution(),
            )
            .expect("source contribution");
        producer_handle
            .close_partition(
                novarocks_execution::runtime_filter::PartitionId::new(0),
                novarocks_execution::runtime_filter::ProducerSequence::new(1),
            )
            .expect("source close");

        assert!(matches!(
            subscription.acquire(Duration::from_millis(1)),
            SnapshotAcquireOutcome::Published(_)
        ));
        let source_snapshot = source.capture_runtime_filter_observation();
        assert_eq!(source_snapshot.producer_streams().len(), 1);
        assert_eq!(
            source_snapshot.producer_streams()[0].latest_accepted_sequence(),
            Some(0)
        );
        assert_eq!(source_snapshot.producer_streams()[0].accepted(), 1);
        assert!(
            source_snapshot
                .channels()
                .iter()
                .any(|channel| channel.completed() == 1)
        );
        assert!(
            source_snapshot
                .transport_routes()
                .iter()
                .any(|route| route.sent() == 1 && route.sent_bytes() > 0)
        );
        let target_snapshot = target.capture_runtime_filter_observation();
        assert!(
            target_snapshot
                .channels()
                .iter()
                .any(|channel| channel.completed() == 1)
        );
        assert!(target_snapshot.consumers().iter().any(|consumer| {
            consumer.latest_delivered_version().is_some()
                && matches!(
                    consumer.outcome(),
                    Some(novarocks_worker::runtime_filter::observation::RuntimeFilterConsumerOutcome::Acquired)
                )
        }));
    }

    #[test]
    fn inbound_contribution_retry_is_folded_as_the_same_producer_stream_duplicate() {
        let fixture = BackendRuntimeFilterFixture::membership();
        let identity = fixture.identity();
        let producer = fixture.producer_contract();
        let source_instance = UniqueId::new(101, 102);
        let witness = novarocks_worker::runtime_filter::domain::BackendCoverageWitnessId::new(29);
        let source_endpoint =
            BackendRouteEndpoint::new(1, BackendRouteRole::Producer(producer.binding_id()))
                .expect("source endpoint");
        let target_endpoint =
            BackendRouteEndpoint::new(2, BackendRouteRole::Aggregator).expect("target endpoint");
        let edge_id = BackendRouteEdgeId::new(501);
        let inbound_edge = BackendRoutingEdge::new(
            edge_id,
            source_endpoint,
            target_endpoint,
            BackendRoutePeer::Remote {
                participant_id: 1,
                endpoint: endpoint(9071),
            },
            [BackendEnvelopeKind::Contribution],
        )
        .expect("inbound contribution route");
        let channel = BackendChannelInstall::new(
            producer.channel_id(),
            producer.contract().clone(),
            BackendChannelLifecycle::CompleteOnce,
            BackendCoverage::witness(witness),
            BackendCoverage::witness(witness),
            BackendMaterializationPolicy::new(8, 3, 5, 1, 4096, 4096, 1)
                .expect("materialization policy"),
            4096,
            4096,
            [
                novarocks_worker::runtime_filter::domain::BackendProducerInstall::new(
                    producer.clone(),
                    witness,
                    [source_instance],
                    4096,
                )
                .expect("producer install"),
            ],
            [],
            [],
        )
        .expect("aggregator channel");
        let routing = BackendRoutingShard::new(
            identity,
            2,
            [BackendRoutingChannel::new(
                producer.channel_id(),
                [BackendRouteRole::Aggregator],
                [inbound_edge],
                [],
                [((producer.binding_id(), source_instance), 1)],
            )
            .expect("aggregator routing channel")],
        )
        .expect("aggregator routing");
        let install = BackendParticipantInstall::new(identity, 2, [channel], routing)
            .expect("aggregator install");
        let observation = RuntimeFilterObservationEmitter::from_install(&install, None);
        let session = Arc::new(
            BackendRuntimeFilterSession::from_channel_install(
                identity,
                install.channels()[&producer.channel_id()].clone(),
                observation.clone(),
            )
            .expect("aggregator session"),
        );
        let execution_id = QueryExecutionId::new(
            QueryId::new(identity.query_id().high(), identity.query_id().low()),
            AttemptId::new(identity.deployment_epoch()).expect("deployment epoch"),
        )
        .expect("execution id");
        let participant = RuntimeFilterParticipant::from_test_parts(
            execution_id,
            install,
            observation,
            transport_policy(),
            BTreeMap::from([(producer.binding_id(), session)]),
            BTreeMap::new(),
            MemTracker::new_root("runtime_filter_inbound_contribution_retry_test"),
            Arc::new(DiscardSink),
        )
        .expect("aggregator participant");
        let contribution = fixture.membership_contribution();
        let envelope = || {
            BackendNativeRuntimeFilterEnvelope::new(
                BackendEnvelopeKind::Contribution,
                identity,
                producer.channel_id(),
                BackendNativeRouteIdentity::contribution(
                    BackendNativeContributionRouteIdentity::new(
                        producer.binding_id(),
                        source_instance,
                        novarocks_execution::runtime_filter::PartitionId::new(0),
                        novarocks_worker::runtime_filter::domain::BackendTransportSequence::new(0),
                    ),
                ),
                Some(BackendProducerOpenMetadata::try_new(1).expect("producer open")),
                None,
                contribution.contract_digest(),
                contribution.canonical_bytes().clone(),
            )
            .expect("contribution envelope")
        };

        assert_eq!(
            participant.dispatch_envelope(envelope()).status(),
            novarocks_worker::runtime_filter::domain::BackendAcceptStatus::Accepted
        );
        assert_eq!(
            participant.dispatch_envelope(envelope()).status(),
            novarocks_worker::runtime_filter::domain::BackendAcceptStatus::Duplicate
        );

        let snapshot = participant.capture_runtime_filter_observation();
        assert_eq!(snapshot.producer_streams().len(), 1);
        let stream = &snapshot.producer_streams()[0];
        assert_eq!(stream.identity().fragment_instance_id(), source_instance);
        assert_eq!(stream.latest_accepted_sequence(), Some(0));
        assert_eq!(stream.accepted(), 1);
        assert_eq!(stream.duplicate(), 1);
    }

    #[test]
    fn unavailable_artifact_frame_reaches_remote_blocking_consumer() {
        let fixture = BackendRuntimeFilterFixture::membership();
        let identity = fixture.identity();
        let consumer_instance = UniqueId::new(201, 202);
        let producer = fixture.producer_contract();
        let consumer_contract = RuntimeFilterConsumerContract::membership_blocking(
            novarocks_execution::runtime_filter::RuntimeFilterBindingId::new(70),
            producer.channel_id(),
            producer.contract().clone(),
        )
        .expect("consumer contract");
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .expect("membership profile");
        let policy = BackendMaterializationPolicy::new(8, 3, 5, 1, 4096, 4096, 1)
            .expect("materialization policy");
        let edge_id = BackendRouteEdgeId::new(501);
        let source_endpoint =
            BackendRouteEndpoint::new(1, BackendRouteRole::Producer(producer.binding_id()))
                .expect("source endpoint");
        let target_endpoint = BackendRouteEndpoint::new(
            2,
            BackendRouteRole::Consumer(consumer_contract.binding_id()),
        )
        .expect("target endpoint");
        let target_edge = BackendRoutingEdge::new(
            edge_id,
            source_endpoint,
            target_endpoint,
            BackendRoutePeer::Remote {
                participant_id: 1,
                endpoint: endpoint(9071),
            },
            [BackendEnvelopeKind::Unavailable],
        )
        .expect("target route");
        let target_channel = BackendChannelInstall::new(
            producer.channel_id(),
            producer.contract().clone(),
            BackendChannelLifecycle::CompleteOnce,
            BackendCoverage::witness(
                novarocks_worker::runtime_filter::domain::BackendCoverageWitnessId::new(29),
            ),
            BackendCoverage::witness(
                novarocks_worker::runtime_filter::domain::BackendCoverageWitnessId::new(29),
            ),
            policy,
            4096,
            4096,
            [],
            [BackendConsumerInstall::new(
                consumer_contract.clone(),
                profile.clone(),
                [edge_id],
                [consumer_instance],
            )
            .expect("target consumer")],
            [],
        )
        .expect("target channel");
        let target_routing = BackendRoutingShard::new(
            identity,
            2,
            [BackendRoutingChannel::new(
                producer.channel_id(),
                [BackendRouteRole::Consumer(consumer_contract.binding_id())],
                [target_edge],
                [],
                [],
            )
            .expect("target routing channel")],
        )
        .expect("target routing");
        let target_install =
            BackendParticipantInstall::new(identity, 2, [target_channel], target_routing)
                .expect("target install");
        let target_observation =
            RuntimeFilterObservationEmitter::from_install(&target_install, None);
        let target_session = Arc::new(
            BackendRuntimeFilterSession::from_channel_install(
                identity,
                target_install.channels()[&producer.channel_id()].clone(),
                target_observation.clone(),
            )
            .expect("target session"),
        );
        let target = RuntimeFilterParticipant::from_test_parts(
            execution_id(),
            target_install,
            target_observation,
            transport_policy(),
            BTreeMap::new(),
            BTreeMap::from([(consumer_contract.binding_id(), target_session)]),
            MemTracker::new_root("runtime_filter_remote_unavailable_test"),
            Arc::new(DiscardSink),
        )
        .expect("target participant");
        let target_context = target
            .session_for_fragment(execution_id(), consumer_instance, true)
            .expect("target context")
            .expect("required target context");
        let RuntimeFilterBindOutcome::Bound(RuntimeFilterSubscriptionHandle::Blocking(
            subscription,
        )) = target_context
            .subscribe(RuntimeFilterSubscriptionRequest::new(consumer_contract))
            .expect("target subscription")
        else {
            panic!("target consumer must bind a blocking subscription");
        };
        let frame = artifact_codec::encode_unavailable(
            novarocks_execution::runtime_filter::UnavailableReason::MaterializationFailed,
            &profile,
            4096,
        )
        .expect("unavailable artifact frame");
        let envelope = BackendNativeRuntimeFilterEnvelope::new(
            BackendEnvelopeKind::Unavailable,
            identity,
            producer.channel_id(),
            BackendNativeRouteIdentity::delivery(BackendNativeDeliveryRouteIdentity::new(
                edge_id,
                novarocks_worker::runtime_filter::domain::BackendTransportSequence::new(1),
            )),
            None,
            None,
            *frame.profile_digest(),
            Arc::<[u8]>::from(frame.payload()),
        )
        .expect("delivery envelope");

        assert!(matches!(
            target.dispatch_envelope(envelope).status(),
            novarocks_worker::runtime_filter::domain::BackendAcceptStatus::Accepted
        ));
        assert!(matches!(
            subscription.acquire(Duration::from_millis(1)),
            SnapshotAcquireOutcome::Unavailable(
                novarocks_execution::runtime_filter::UnavailableReason::MaterializationFailed
            )
        ));
    }
}
