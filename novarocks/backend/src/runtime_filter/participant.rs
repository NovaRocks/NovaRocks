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

//! Attempt-scoped Backend runtime-filter participant ownership.
//!
//! Fragment sessions resolve only sealed Execution contracts. Backend keeps
//! route authority, expected instances, reduction state, and subscriptions in
//! the installed channel sessions; no Core runtime-filter service is retained
//! by this participant.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks::query_execution::lifecycle::{
    QueryExecutionId, QueryLifecycleError, QueryLifecycleErrorCode, QueryTerminationReason,
};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime_filter::{
    RuntimeFilterBindOutcome, RuntimeFilterContractViolation, RuntimeFilterContractViolationKind,
    RuntimeFilterExecutionContract, RuntimeFilterFinalDomain, RuntimeFilterFinalDomainCompletion,
    RuntimeFilterFinalDomainCompletionHandle, RuntimeFilterFinalDomainOpenRequest,
    RuntimeFilterFinalDomainPartition, RuntimeFilterFinalDomainPartitionHandle,
    RuntimeFilterProducer, RuntimeFilterProducerOpenRequest, RuntimeFilterSession,
    RuntimeFilterSessionRef, RuntimeFilterSnapshot, RuntimeFilterSubscriptionHandle,
    RuntimeFilterSubscriptionRequest,
};
use novarocks_types::UniqueId;

use super::domain::{
    BackendEnvelopeKind, BackendIngressResult, BackendMaterializedDelivery,
    BackendMaterializedDeliverySink, BackendParticipantInstall, BackendRouteDecision,
    BackendRoutingError, BackendRuntimeFilterEventObserver, BackendRuntimeFilterSession,
    DiscardBackendRuntimeFilterEventObserver,
};
use crate::native::runtime_filter_adapter::{
    BackendNativeContributionRouteIdentity, BackendNativeDeliveryRouteIdentity,
    BackendNativeProducerInstanceRouteIdentity, BackendNativeRouteIdentity,
    BackendNativeRuntimeFilterEnvelope, BackendRuntimeFilterEnvelopeIngress,
};
use crate::native::runtime_filter_install::DecodedRuntimeFilterContribution;
use crate::native::runtime_filter_sender::{
    BackendNativeRuntimeFilterTransportEnvelope, BackendRuntimeFilterEnvelopeSink,
    GrpcRuntimeFilterEnvelopeSink,
};
use crate::runtime_filter::artifact_query::BackendRuntimeFilterArtifactQuery;
use crate::runtime_filter::codec::{artifact as artifact_codec, producer as producer_codec};

const QUERY_UNAVAILABLE_REJECTION: &str = "runtime filter ingress rejected [query-unavailable]: runtime filter query is not active or in delivery grace";
const ACK_UNSUPPORTED_REJECTION: &str = "runtime filter ingress rejected [ack-unsupported]: runtime filter ack ingress is not supported";
const DELIVERY_REJECTION: &str = "runtime filter ingress rejected [artifact-delivery]: delivery violates the installed artifact contract";

/// Backend-private factory injected into the lifecycle registry. The entry
/// owns the attempt lifetime; it cannot recover a participant by query id.
pub(crate) trait RuntimeFilterParticipantFactory: Send + Sync + 'static {
    fn install(
        &self,
        execution_id: QueryExecutionId,
        contribution: DecodedRuntimeFilterContribution,
    ) -> Result<Arc<RuntimeFilterParticipant>, QueryLifecycleError>;
}

#[derive(Default)]
pub(crate) struct BackendRuntimeFilterParticipantFactory;

impl RuntimeFilterParticipantFactory for BackendRuntimeFilterParticipantFactory {
    // Design: ADR-0044 (docs/adr/ADR-0044-backend-runtime-filter-participant-domain.md)
    fn install(
        &self,
        execution_id: QueryExecutionId,
        contribution: DecodedRuntimeFilterContribution,
    ) -> Result<Arc<RuntimeFilterParticipant>, QueryLifecycleError> {
        let query_id = UniqueId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        );
        let lifecycle = contribution.lifecycle;
        let install = contribution.install;
        if install.participant().query_id() != query_id
            || install.participant().deployment_epoch() != execution_id.attempt_id().get()
        {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                "runtime filter install does not match the query execution attempt",
            ));
        }
        let events: Arc<dyn BackendRuntimeFilterEventObserver> =
            Arc::new(DiscardBackendRuntimeFilterEventObserver);
        let mut producers = BTreeMap::new();
        let mut consumers = BTreeMap::new();
        for channel in install.channels().values() {
            let session = Arc::new(
                BackendRuntimeFilterSession::from_channel_install(
                    install.participant(),
                    channel.clone(),
                    Arc::clone(&events),
                )
                .map_err(|error| {
                    QueryLifecycleError::new(
                        QueryLifecycleErrorCode::InvalidManifest,
                        error.to_string(),
                    )
                })?,
            );
            for binding_id in channel.producers().keys() {
                if producers
                    .insert(*binding_id, Arc::clone(&session))
                    .is_some()
                {
                    return Err(QueryLifecycleError::new(
                        QueryLifecycleErrorCode::InvalidManifest,
                        "runtime filter producer binding is installed by multiple channels",
                    ));
                }
            }
            for binding_id in channel.consumers().keys() {
                if consumers
                    .insert(*binding_id, Arc::clone(&session))
                    .is_some()
                {
                    return Err(QueryLifecycleError::new(
                        QueryLifecycleErrorCode::InvalidManifest,
                        "runtime filter consumer binding is installed by multiple channels",
                    ));
                }
            }
        }
        let memory = MemTracker::new_root(format!(
            "runtime_filter_participant_{:x}_{:x}_{}",
            query_id.high(),
            query_id.low(),
            execution_id.attempt_id().get()
        ));
        RuntimeFilterParticipant::from_installed(
            execution_id,
            install,
            lifecycle.transport_deadline,
            producers,
            consumers,
            memory,
            GrpcRuntimeFilterEnvelopeSink::new(),
        )
    }
}

/// One sealed Backend participant for exactly one query execution attempt.
pub(crate) struct RuntimeFilterParticipant {
    execution_id: QueryExecutionId,
    install: BackendParticipantInstall,
    producer_sessions: BTreeMap<
        novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        Arc<BackendRuntimeFilterSession>,
    >,
    consumer_sessions: BTreeMap<
        novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        Arc<BackendRuntimeFilterSession>,
    >,
    outbound: Arc<BackendParticipantOutbound>,
    cancelled: Arc<AtomicBool>,
    _memory: Arc<MemTracker>,
    close_hook: RuntimeFilterParticipantCloseHook,
}

pub(crate) type RuntimeFilterParticipantCloseHook = Arc<
    dyn Fn(&RuntimeFilterParticipant, QueryTerminationReason) -> Result<(), QueryLifecycleError>
        + Send
        + Sync,
>;

impl RuntimeFilterParticipant {
    #[allow(clippy::too_many_arguments)]
    fn from_installed(
        execution_id: QueryExecutionId,
        install: BackendParticipantInstall,
        transport_deadline: Duration,
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
    ) -> Result<Arc<Self>, QueryLifecycleError> {
        let outbound = Arc::new(BackendParticipantOutbound::new(
            install.clone(),
            transport_deadline,
            transport_sink,
        ));
        let sink = Arc::clone(&outbound) as Arc<dyn BackendMaterializedDeliverySink>;
        for session in producer_sessions.values() {
            session.set_materialized_delivery_sink(Arc::clone(&sink));
        }
        Ok(Arc::new(Self {
            execution_id,
            install,
            producer_sessions,
            consumer_sessions,
            outbound,
            cancelled: Arc::new(AtomicBool::new(false)),
            _memory: memory,
            close_hook: Arc::new(|_, _| Ok(())),
        }))
    }

    pub(crate) fn session_for_fragment(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        required: bool,
    ) -> Result<Option<RuntimeFilterSessionRef>, QueryLifecycleError> {
        if execution_id != self.execution_id {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Terminated,
                "runtime filter participant does not belong to this execution attempt",
            ));
        }
        if !required {
            return Ok(None);
        }
        Ok(Some(Arc::new(BackendRuntimeFilterExecutionContext {
            fragment_instance_id,
            producers: self.producer_sessions.clone(),
            consumers: self.consumer_sessions.clone(),
            outbound: Arc::clone(&self.outbound),
            cancelled: Arc::clone(&self.cancelled),
        }) as RuntimeFilterSessionRef))
    }

    pub(crate) fn dispatch_envelope(
        &self,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> BackendIngressResult {
        let query_id = UniqueId::new(
            self.execution_id.query_id().high(),
            self.execution_id.query_id().low(),
        );
        if self.cancelled.load(Ordering::Acquire)
            || envelope.query_id() != query_id
            || envelope.deployment_epoch() != self.install.participant().deployment_epoch()
        {
            return rejected(QUERY_UNAVAILABLE_REJECTION);
        }
        match envelope.kind() {
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
        }
    }

    fn dispatch_delivery_envelope(
        &self,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> BackendIngressResult {
        let Some(identity) = envelope.route_identity().as_delivery() else {
            return rejected(DELIVERY_REJECTION);
        };
        let route_edge_id = identity.route_edge_id();
        if self
            .install
            .routing()
            .authorize_delivery(envelope.channel_id(), route_edge_id, envelope.kind())
            .is_err()
        {
            return rejected(
                "runtime filter ingress rejected [artifact-delivery]: route is not authorized for this delivery",
            );
        }
        let Some((binding_id, session)) =
            self.consumer_sessions
                .iter()
                .find_map(|(binding_id, session)| {
                    session
                        .channel()
                        .consumers()
                        .get(binding_id)
                        .filter(|consumer| consumer.route_edge_ids().contains(&route_edge_id))
                        .map(|_| (*binding_id, Arc::clone(session)))
                })
        else {
            return rejected(
                "runtime filter ingress rejected [artifact-delivery]: no installed consumer owns this route",
            );
        };
        if session.channel().channel_id() != envelope.channel_id() {
            return rejected(
                "runtime filter ingress rejected [artifact-delivery]: route resolves to a different channel",
            );
        }
        let Some(consumer) = session.channel().consumers().get(&binding_id) else {
            return rejected(
                "runtime filter ingress rejected [artifact-delivery]: consumer install disappeared",
            );
        };
        let outcome = match envelope.kind() {
            BackendEnvelopeKind::Artifact | BackendEnvelopeKind::FinalArtifact => {
                let placeholder = match execution_placeholder_membership_schema() {
                    Ok(schema) => schema,
                    Err(()) => return rejected(DELIVERY_REJECTION),
                };
                let (schema, order_contract, contract_digest) = match consumer.contract().contract()
                {
                    RuntimeFilterExecutionContract::Membership(schema) => {
                        (schema, None, schema.digest())
                    }
                    RuntimeFilterExecutionContract::Ordered(order) => {
                        let Some(key) = order.keys().first() else {
                            return rejected(DELIVERY_REJECTION);
                        };
                        let _ = key;
                        // The schema field is not consulted for an ordered Range
                        // artifact; its contract digest is checked through
                        // `order_contract` by the strict NRFA decoder.
                        (&placeholder, Some(order.as_ref()), order.digest())
                    }
                };
                let bundle = artifact_codec::decode_artifact_bundle(
                    envelope.payload(),
                    envelope.schema_digest(),
                    artifact_codec::ArtifactDecodeExpectation {
                        profile: consumer.profile(),
                        schema,
                        order_contract,
                    },
                    session.channel().max_artifact_bytes(),
                );
                let Ok(bundle) = bundle else {
                    return rejected(
                        "runtime filter ingress rejected [artifact-delivery]: artifact frame violates the installed profile or contract",
                    );
                };
                let Some((_, _artifact)) = bundle.artifacts().first() else {
                    return rejected(
                        "runtime filter ingress rejected [artifact-delivery]: artifact frame contains no physical artifact",
                    );
                };
                let query = match consumer.contract().contract() {
                    RuntimeFilterExecutionContract::Membership(schema) => {
                        BackendRuntimeFilterArtifactQuery::membership(
                            &bundle,
                            schema.data_type().clone(),
                            schema.null_semantics(),
                        )
                    }
                    RuntimeFilterExecutionContract::Ordered(order) => {
                        BackendRuntimeFilterArtifactQuery::ordered(&bundle, Arc::clone(order))
                    }
                };
                let Ok(query) = query else {
                    return rejected(
                        "runtime filter ingress rejected [artifact-delivery]: artifact does not provide the installed evaluator",
                    );
                };
                novarocks_execution::runtime_filter::SnapshotAcquireOutcome::Published(Arc::new(
                    RuntimeFilterSnapshot::new(
                        binding_id,
                        bundle.version(),
                        contract_digest,
                        Arc::new(query),
                    ),
                ))
            }
            BackendEnvelopeKind::Unavailable => {
                let Ok(reason) = artifact_codec::decode_unavailable(
                    envelope.payload(),
                    envelope.schema_digest(),
                    consumer.profile(),
                    session.channel().max_artifact_bytes(),
                ) else {
                    return rejected(
                        "runtime filter ingress rejected [artifact-delivery]: unavailable frame violates the installed profile",
                    );
                };
                novarocks_execution::runtime_filter::SnapshotAcquireOutcome::Unavailable(reason)
            }
            BackendEnvelopeKind::DegradedLogical => {
                if producer_codec::decode_producer_failure(envelope.payload()).is_err() {
                    return rejected(
                        "runtime filter ingress rejected [artifact-delivery]: degraded frame is malformed",
                    );
                }
                novarocks_execution::runtime_filter::SnapshotAcquireOutcome::Unavailable(
                    novarocks_execution::runtime_filter::UnavailableReason::ProducerFailed,
                )
            }
            BackendEnvelopeKind::CompletedWithoutArtifact => {
                novarocks_execution::runtime_filter::SnapshotAcquireOutcome::Unavailable(
                    novarocks_execution::runtime_filter::UnavailableReason::IncompleteCoverage,
                )
            }
            _ => {
                return rejected(
                    "runtime filter ingress rejected [artifact-delivery]: envelope kind is not a delivery",
                );
            }
        };
        let terminal = match envelope.kind() {
            BackendEnvelopeKind::FinalArtifact => {
                Some(novarocks_execution::runtime_filter::LiveTerminal::Completed)
            }
            BackendEnvelopeKind::CompletedWithoutArtifact => {
                Some(novarocks_execution::runtime_filter::LiveTerminal::CompletedWithoutArtifact)
            }
            _ => None,
        };
        match session.publish_materialized(route_edge_id, outcome, terminal) {
            Ok(()) => BackendIngressResult::accepted(),
            Err(_) => rejected(
                "runtime filter ingress rejected [artifact-delivery]: subscription publication rejected the delivery",
            ),
        }
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
        let binding_id = identity.producer_binding_id();
        let channel_id = envelope.channel_id();
        let kind = envelope.kind();
        if self
            .install
            .routing()
            .authorize_contribution(
                channel_id,
                binding_id,
                identity.fragment_instance_id(),
                kind,
            )
            .is_err()
        {
            return rejected(
                "runtime filter ingress rejected [route-authority]: producer route is not installed",
            );
        }
        let Some(session) = self.producer_sessions.get(&binding_id) else {
            return rejected(
                "runtime filter ingress rejected [producer-binding]: producer binding is not installed",
            );
        };
        if session.channel().channel_id() != channel_id {
            return rejected(
                "runtime filter ingress rejected [producer-binding]: producer binding is installed for a different channel",
            );
        }
        let Some(install) = session.channel().producers().get(&binding_id) else {
            return rejected(
                "runtime filter ingress rejected [producer-binding]: producer binding is not installed in its channel",
            );
        };
        let Some(open) = envelope.producer_open() else {
            return rejected(
                "runtime filter ingress rejected [producer-open]: producer open metadata is required",
            );
        };
        if session
            .open_producer(
                identity.fragment_instance_id(),
                RuntimeFilterProducerOpenRequest::new(
                    install.contract().clone(),
                    open.local_partition_count().get(),
                ),
            )
            .is_err()
        {
            return rejected(
                "runtime filter ingress rejected [producer-open]: producer open does not match the installed binding",
            );
        }
        match envelope.kind() {
            BackendEnvelopeKind::Contribution => {
                let contribution =
                    novarocks_execution::runtime_filter::RuntimeFilterContribution::new(
                        contribution_kind(install.contract().kind()),
                        *envelope.schema_digest(),
                        Arc::<[u8]>::from(envelope.payload()),
                    );
                match session.submit(
                    binding_id,
                    identity.fragment_instance_id(),
                    novarocks_execution::runtime_filter::PartitionId::new(identity.partition_id().get()),
                    novarocks_execution::runtime_filter::ProducerSequence::new(identity.sequence().get()),
                    contribution,
                ) {
                    Ok(submission) if matches!(submission.outcome(), novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome::Duplicate | novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome::Stale) => BackendIngressResult::duplicate(),
                    Ok(_) => BackendIngressResult::accepted(),
                    Err(_) => rejected("runtime filter ingress rejected [contribution]: contribution violates the installed execution contract"),
                }
            }
            BackendEnvelopeKind::ProducerClosed => match session.close_partition(
                binding_id,
                identity.fragment_instance_id(),
                novarocks_execution::runtime_filter::PartitionId::new(
                    identity.partition_id().get(),
                ),
                novarocks_execution::runtime_filter::ProducerSequence::new(
                    identity.sequence().get(),
                ),
            ) {
                Ok(
                    novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome::TerminalNoop,
                ) => BackendIngressResult::duplicate(),
                Ok(_) => BackendIngressResult::accepted(),
                Err(_) => rejected(
                    "runtime filter ingress rejected [producer-close]: close violates the installed producer route",
                ),
            },
            _ => unreachable!("caller selects producer envelope kinds"),
        }
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
        let binding_id = identity.producer_binding_id();
        let channel_id = envelope.channel_id();
        if self
            .install
            .routing()
            .authorize_contribution(
                channel_id,
                binding_id,
                identity.fragment_instance_id(),
                BackendEnvelopeKind::ProducerUnavailable,
            )
            .is_err()
        {
            return rejected(
                "runtime filter ingress rejected [route-authority]: producer failure route is not installed",
            );
        }
        let Some(session) = self.producer_sessions.get(&binding_id) else {
            return rejected(
                "runtime filter ingress rejected [producer-binding]: producer binding is not installed",
            );
        };
        if session.channel().channel_id() != channel_id {
            return rejected(
                "runtime filter ingress rejected [producer-binding]: producer binding is installed for a different channel",
            );
        }
        match session.fail(
            binding_id,
            identity.fragment_instance_id(),
            novarocks_execution::runtime_filter::RuntimeFilterProducerFailure::UpstreamUnavailable,
        ) {
            Ok(novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome::TerminalNoop) => {
                BackendIngressResult::duplicate()
            }
            Ok(_) => BackendIngressResult::accepted(),
            Err(_) => rejected(
                "runtime filter ingress rejected [producer-failure]: failure violates the installed producer route",
            ),
        }
    }

    pub(crate) fn close(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError> {
        self.cancelled.store(true, Ordering::Release);
        (self.close_hook)(self, reason)
    }

    #[cfg(test)]
    pub(crate) fn with_close_hook_for_test(
        &self,
        close_hook: RuntimeFilterParticipantCloseHook,
    ) -> Arc<Self> {
        Arc::new(Self {
            execution_id: self.execution_id,
            install: self.install.clone(),
            producer_sessions: self.producer_sessions.clone(),
            consumer_sessions: self.consumer_sessions.clone(),
            outbound: Arc::clone(&self.outbound),
            cancelled: Arc::clone(&self.cancelled),
            _memory: Arc::clone(&self._memory),
            close_hook,
        })
    }
}

impl BackendRuntimeFilterEnvelopeIngress for RuntimeFilterParticipant {
    fn accept(&self, envelope: BackendNativeRuntimeFilterEnvelope) -> BackendIngressResult {
        self.dispatch_envelope(envelope)
    }
}

struct BackendParticipantRuntimeFilterProducer {
    local: novarocks_execution::runtime_filter::RuntimeFilterProducerHandle,
    outbound: Arc<BackendParticipantOutbound>,
    binding_id: novarocks_execution::runtime_filter::RuntimeFilterBindingId,
    channel_id: novarocks_execution::runtime_filter::RuntimeFilterChannelId,
    fragment_instance_id: UniqueId,
    local_partition_count: u32,
}

impl RuntimeFilterProducer for BackendParticipantRuntimeFilterProducer {
    fn max_contribution_bytes(&self) -> usize {
        self.local.max_contribution_bytes()
    }

    fn submit(
        &self,
        partition: novarocks_execution::runtime_filter::PartitionId,
        sequence: novarocks_execution::runtime_filter::ProducerSequence,
        contribution: novarocks_execution::runtime_filter::RuntimeFilterContribution,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome,
        RuntimeFilterContractViolation,
    > {
        let outcome = self
            .local
            .submit(partition, sequence, contribution.clone())?;
        self.outbound.forward_producer_contribution(
            self.channel_id,
            self.binding_id,
            self.fragment_instance_id,
            partition,
            sequence,
            self.local_partition_count,
            contribution,
        )?;
        Ok(outcome)
    }

    fn close_partition(
        &self,
        partition: novarocks_execution::runtime_filter::PartitionId,
        terminal: novarocks_execution::runtime_filter::ProducerSequence,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome,
        RuntimeFilterContractViolation,
    > {
        let outcome = self.local.close_partition(partition, terminal)?;
        self.outbound.forward_producer_close(
            self.channel_id,
            self.binding_id,
            self.fragment_instance_id,
            partition,
            terminal,
            self.local_partition_count,
        )?;
        Ok(outcome)
    }

    fn fail(
        &self,
        reason: novarocks_execution::runtime_filter::RuntimeFilterProducerFailure,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome,
        RuntimeFilterContractViolation,
    > {
        let outcome = self.local.fail(reason)?;
        self.outbound.forward_producer_failure(
            self.channel_id,
            self.binding_id,
            self.fragment_instance_id,
            reason,
        )?;
        Ok(outcome)
    }
}

struct BackendParticipantOutbound {
    install: BackendParticipantInstall,
    transport_deadline: Duration,
    transport_sink: Arc<dyn BackendRuntimeFilterEnvelopeSink>,
    next_delivery_sequence: AtomicU64,
}

impl BackendParticipantOutbound {
    fn new(
        install: BackendParticipantInstall,
        transport_deadline: Duration,
        transport_sink: Arc<dyn BackendRuntimeFilterEnvelopeSink>,
    ) -> Self {
        Self {
            install,
            transport_deadline,
            transport_sink,
            next_delivery_sequence: AtomicU64::new(1),
        }
    }

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
                super::domain::BackendTransportSequence::new(sequence.get()),
            )),
            Some(
                super::domain::BackendProducerOpenMetadata::try_new(local_partition_count)
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
                super::domain::BackendTransportSequence::new(sequence.get()),
            )),
            Some(
                super::domain::BackendProducerOpenMetadata::try_new(local_partition_count)
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
        self.dispatch_remote_envelope_decision(&decision, envelope)
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
        for route in decision.remote_routes() {
            let envelope =
                self.delivery_envelope(&delivery, route.edge_id(), self.next_delivery_sequence())?;
            self.submit_remote(route.clone(), envelope);
        }
        Ok(())
    }

    fn delivery_envelope(
        &self,
        delivery: &BackendMaterializedDelivery,
        route_edge_id: super::domain::BackendRouteEdgeId,
        sequence: super::domain::BackendTransportSequence,
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

    fn next_delivery_sequence(&self) -> super::domain::BackendTransportSequence {
        super::domain::BackendTransportSequence::new(
            self.next_delivery_sequence.fetch_add(1, Ordering::Relaxed),
        )
    }

    fn dispatch_remote_envelope_decision(
        &self,
        decision: &BackendRouteDecision,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) -> Result<(), RuntimeFilterContractViolation> {
        for route in decision.remote_routes() {
            self.submit_remote(route.clone(), envelope.clone());
        }
        Ok(())
    }

    fn submit_remote(
        &self,
        route: super::domain::BackendRemoteRoute,
        envelope: BackendNativeRuntimeFilterEnvelope,
    ) {
        let Ok(envelope) = BackendNativeRuntimeFilterTransportEnvelope::new(
            Arc::new(envelope),
            self.transport_deadline,
        ) else {
            return;
        };
        let _ = self.transport_sink.try_send(route, envelope);
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

struct BackendRuntimeFilterExecutionContext {
    fragment_instance_id: UniqueId,
    producers: BTreeMap<
        novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        Arc<BackendRuntimeFilterSession>,
    >,
    consumers: BTreeMap<
        novarocks_execution::runtime_filter::RuntimeFilterBindingId,
        Arc<BackendRuntimeFilterSession>,
    >,
    outbound: Arc<BackendParticipantOutbound>,
    cancelled: Arc<AtomicBool>,
}

impl RuntimeFilterSession for BackendRuntimeFilterExecutionContext {
    fn open_producer(
        &self,
        request: RuntimeFilterProducerOpenRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<novarocks_execution::runtime_filter::RuntimeFilterProducerHandle>,
        RuntimeFilterContractViolation,
    > {
        if self.cancelled.load(Ordering::Acquire) {
            return Ok(RuntimeFilterBindOutcome::Unavailable(
                novarocks_execution::runtime_filter::UnavailableReason::RouteUnavailable,
            ));
        }
        let binding_id = request.contract().binding_id();
        let session = self.producers.get(&binding_id).ok_or_else(|| {
            violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "producer binding is not installed for this Backend fragment",
            )
        })?;
        let local_partition_count = request.local_partition_count();
        match session.open_producer(self.fragment_instance_id, request)? {
            RuntimeFilterBindOutcome::Bound(local) => Ok(RuntimeFilterBindOutcome::Bound(
                Arc::new(BackendParticipantRuntimeFilterProducer {
                    local,
                    outbound: Arc::clone(&self.outbound),
                    binding_id,
                    channel_id: session.channel().channel_id(),
                    fragment_instance_id: self.fragment_instance_id,
                    local_partition_count,
                }),
            )),
            RuntimeFilterBindOutcome::Unavailable(reason) => {
                Ok(RuntimeFilterBindOutcome::Unavailable(reason))
            }
        }
    }

    fn subscribe(
        &self,
        request: RuntimeFilterSubscriptionRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<RuntimeFilterSubscriptionHandle>,
        RuntimeFilterContractViolation,
    > {
        if self.cancelled.load(Ordering::Acquire) {
            return Ok(RuntimeFilterBindOutcome::Unavailable(
                novarocks_execution::runtime_filter::UnavailableReason::RouteUnavailable,
            ));
        }
        let binding_id = request.contract().binding_id();
        let session = self.consumers.get(&binding_id).ok_or_else(|| {
            violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "consumer binding is not installed for this Backend fragment",
            )
        })?;
        session.subscribe(self.fragment_instance_id, request)
    }

    fn open_final_domain_completion(
        &self,
        request: RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        RuntimeFilterBindOutcome<RuntimeFilterFinalDomainCompletionHandle>,
        RuntimeFilterContractViolation,
    > {
        if self.cancelled.load(Ordering::Acquire) {
            return Ok(RuntimeFilterBindOutcome::Unavailable(
                novarocks_execution::runtime_filter::UnavailableReason::RouteUnavailable,
            ));
        }
        let contract = request.contract().clone();
        if contract.kind()
            != novarocks_execution::runtime_filter::RuntimeFilterProducerKind::FinalDomain
        {
            return Err(violation(
                RuntimeFilterContractViolationKind::RoleMismatch,
                "final-domain completion request does not carry a FinalDomain producer contract",
            ));
        }
        let RuntimeFilterExecutionContract::Membership(schema) = contract.contract() else {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain completion requires a membership execution contract",
            ));
        };
        let session = self.producers.get(&contract.binding_id()).ok_or_else(|| {
            violation(
                RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "FinalDomain producer binding is not installed for this Backend fragment",
            )
        })?;
        let producer = match self.open_producer(RuntimeFilterProducerOpenRequest::new(
            contract.clone(),
            request.local_partition_count(),
        ))? {
            RuntimeFilterBindOutcome::Bound(producer) => producer,
            RuntimeFilterBindOutcome::Unavailable(reason) => {
                return Ok(RuntimeFilterBindOutcome::Unavailable(reason));
            }
        };
        Ok(RuntimeFilterBindOutcome::Bound(Arc::new(
            BackendFinalDomainCompletion {
                producer,
                data_type: schema.data_type().clone(),
                contract_digest: schema.digest(),
                max_domain_canonical_bytes: session.policy().max_contribution_bytes(),
                local_partition_count: request.local_partition_count(),
                claimed: Mutex::new(BTreeMap::new()),
            },
        )))
    }
}

struct BackendFinalDomainCompletion {
    producer: novarocks_execution::runtime_filter::RuntimeFilterProducerHandle,
    data_type: arrow::datatypes::DataType,
    contract_digest: [u8; 32],
    max_domain_canonical_bytes: usize,
    local_partition_count: u32,
    claimed: Mutex<BTreeMap<novarocks_execution::runtime_filter::PartitionId, ()>>,
}

struct BackendFinalDomainPartition {
    completion: Arc<BackendFinalDomainCompletion>,
    partition: novarocks_execution::runtime_filter::PartitionId,
    sealed: bool,
}

impl RuntimeFilterFinalDomainCompletion for BackendFinalDomainCompletion {
    fn membership_key_type(&self) -> arrow::datatypes::DataType {
        self.data_type.clone()
    }

    fn max_domain_canonical_bytes(&self) -> usize {
        self.max_domain_canonical_bytes
    }

    fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }

    fn claim_partition(
        self: &Self,
        partition: novarocks_execution::runtime_filter::PartitionId,
    ) -> Result<RuntimeFilterFinalDomainPartitionHandle, RuntimeFilterContractViolation> {
        if partition.get() >= self.local_partition_count {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain partition is outside its declared local partition count",
            ));
        }
        let mut claimed = self
            .claimed
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if claimed.insert(partition, ()).is_some() {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain partition was claimed more than once",
            ));
        }
        Ok(Box::new(BackendFinalDomainPartition {
            completion: Arc::new(self.clone_for_partition()),
            partition,
            sealed: false,
        }))
    }

    fn fail(
        &self,
        reason: novarocks_execution::runtime_filter::RuntimeFilterProducerFailure,
    ) -> Result<
        novarocks_execution::runtime_filter::RuntimeFilterSubmitOutcome,
        RuntimeFilterContractViolation,
    > {
        self.producer.fail(reason)
    }
}

impl BackendFinalDomainCompletion {
    fn clone_for_partition(&self) -> Self {
        Self {
            producer: Arc::clone(&self.producer),
            data_type: self.data_type.clone(),
            contract_digest: self.contract_digest,
            max_domain_canonical_bytes: self.max_domain_canonical_bytes,
            local_partition_count: self.local_partition_count,
            claimed: Mutex::new(BTreeMap::new()),
        }
    }
}

impl RuntimeFilterFinalDomainPartition for BackendFinalDomainPartition {
    fn seal(
        &mut self,
        domain: RuntimeFilterFinalDomain,
    ) -> Result<(), RuntimeFilterContractViolation> {
        if self.sealed {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain partition was sealed twice",
            ));
        }
        if domain.data_type() != &self.completion.data_type
            || domain.contract_digest() != self.completion.contract_digest
        {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain payload does not match the installed membership contract",
            ));
        }
        let value_domain = novarocks_execution::runtime_filter::contribution::decode_value_domain(
            domain.canonical_bytes(),
            &self.completion.data_type,
            self.completion.max_domain_canonical_bytes,
        )
        .map_err(|_| {
            violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain canonical payload is invalid",
            )
        })?;
        let encoded = novarocks_execution::runtime_filter::contribution::encode_contribution(
            &novarocks_execution::runtime_filter::contribution::RuntimeFilterContribution::final_domain(
                novarocks_execution::runtime_filter::contribution::FinalDomainShard::new(
                    self.completion.contract_digest,
                    value_domain,
                ),
            ),
            novarocks_execution::runtime_filter::contribution::ContributionCodecExpectation::final_domain(
                &self.completion.data_type,
                self.completion.contract_digest,
            ),
            self.completion.max_domain_canonical_bytes,
        ).map_err(|_| violation(RuntimeFilterContractViolationKind::ContractMismatch, "FinalDomain contribution cannot be encoded canonically"))?;
        self.completion.producer.submit(
            self.partition,
            novarocks_execution::runtime_filter::ProducerSequence::new(0),
            novarocks_execution::runtime_filter::RuntimeFilterContribution::new(
                novarocks_execution::runtime_filter::RuntimeFilterContributionKind::FinalDomain,
                *encoded.schema_digest(),
                encoded.into_parts().1,
            ),
        )?;
        self.sealed = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), RuntimeFilterContractViolation> {
        if !self.sealed {
            return Err(violation(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "FinalDomain partition closed before seal",
            ));
        }
        self.completion.producer.close_partition(
            self.partition,
            novarocks_execution::runtime_filter::ProducerSequence::new(1),
        )?;
        Ok(())
    }
}

fn contribution_kind(
    kind: novarocks_execution::runtime_filter::RuntimeFilterProducerKind,
) -> novarocks_execution::runtime_filter::RuntimeFilterContributionKind {
    match kind {
        novarocks_execution::runtime_filter::RuntimeFilterProducerKind::Membership => {
            novarocks_execution::runtime_filter::RuntimeFilterContributionKind::Membership
        }
        novarocks_execution::runtime_filter::RuntimeFilterProducerKind::OrderedBound => {
            novarocks_execution::runtime_filter::RuntimeFilterContributionKind::OrderedBound
        }
        novarocks_execution::runtime_filter::RuntimeFilterProducerKind::TopKSummary => {
            novarocks_execution::runtime_filter::RuntimeFilterContributionKind::TopKSummary
        }
        novarocks_execution::runtime_filter::RuntimeFilterProducerKind::FinalDomain => {
            novarocks_execution::runtime_filter::RuntimeFilterContributionKind::FinalDomain
        }
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
    use std::time::Duration;

    use novarocks::query_execution::lifecycle::AttemptId;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_execution::runtime_filter::{
        RuntimeFilterBindOutcome, RuntimeFilterConsumerContract, RuntimeFilterSubscriptionHandle,
        RuntimeFilterSubscriptionRequest, SnapshotAcquireOutcome,
    };
    use novarocks_types::QueryId;

    use super::*;
    use crate::native::runtime_filter_sender::{
        BackendRuntimeFilterSinkCompletion, BackendRuntimeFilterSinkSubmitOutcome,
    };
    use crate::runtime_filter::artifact::{ArtifactKind, ConsumerArtifactProfile};
    use crate::runtime_filter::domain::{
        BackendChannelInstall, BackendChannelLifecycle, BackendConsumerInstall, BackendCoverage,
        BackendMaterializationOwner, BackendMaterializationPolicy,
        BackendOutboundMaterializationGroup, BackendRemoteRoute, BackendRouteEdgeId,
        BackendRouteEndpoint, BackendRoutePeer, BackendRouteRole, BackendRoutingChannel,
        BackendRoutingEdge, BackendRoutingShard,
    };
    use crate::runtime_filter::test_support::BackendRuntimeFilterFixture;

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
                    super::super::domain::BackendAcceptStatus::Accepted
                        | super::super::domain::BackendAcceptStatus::Duplicate
                ),
                "remote envelope rejected: {:?}",
                result.rejection_reason()
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

    fn endpoint(port: i32) -> RuntimeEndpoint {
        RuntimeEndpoint::new("127.0.0.1", port).expect("valid endpoint")
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
            [super::super::domain::BackendProducerInstall::new(
                producer.clone(),
                super::super::domain::BackendCoverageWitnessId::new(29),
                [source_instance],
                4096,
            )
            .expect("source producer")],
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
            BackendCoverage::witness(super::super::domain::BackendCoverageWitnessId::new(29)),
            BackendCoverage::witness(super::super::domain::BackendCoverageWitnessId::new(29)),
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
        let target_session = Arc::new(
            BackendRuntimeFilterSession::from_channel_install(
                identity,
                target_install.channels()[&producer.channel_id()].clone(),
                Arc::new(DiscardBackendRuntimeFilterEventObserver),
            )
            .expect("target session"),
        );
        let target = RuntimeFilterParticipant::from_installed(
            execution_id(),
            target_install,
            Duration::from_secs(1),
            BTreeMap::new(),
            BTreeMap::from([(consumer_contract.binding_id(), target_session)]),
            MemTracker::new_root("runtime_filter_remote_consumer_test"),
            Arc::new(DiscardSink),
        )
        .expect("target participant");
        let source_session = Arc::new(
            BackendRuntimeFilterSession::from_channel_install(
                identity,
                source_install.channels()[&producer.channel_id()].clone(),
                Arc::new(DiscardBackendRuntimeFilterEventObserver),
            )
            .expect("source session"),
        );
        let source = RuntimeFilterParticipant::from_installed(
            execution_id(),
            source_install,
            Duration::from_secs(1),
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
                novarocks_execution::runtime_filter::ProducerSequence::new(1),
                fixture.membership_contribution(),
            )
            .expect("source contribution");
        producer_handle
            .close_partition(
                novarocks_execution::runtime_filter::PartitionId::new(0),
                novarocks_execution::runtime_filter::ProducerSequence::new(2),
            )
            .expect("source close");

        assert!(matches!(
            subscription.acquire(Duration::from_millis(1)),
            SnapshotAcquireOutcome::Published(_)
        ));
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
            BackendCoverage::witness(super::super::domain::BackendCoverageWitnessId::new(29)),
            BackendCoverage::witness(super::super::domain::BackendCoverageWitnessId::new(29)),
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
        let target_session = Arc::new(
            BackendRuntimeFilterSession::from_channel_install(
                identity,
                target_install.channels()[&producer.channel_id()].clone(),
                Arc::new(DiscardBackendRuntimeFilterEventObserver),
            )
            .expect("target session"),
        );
        let target = RuntimeFilterParticipant::from_installed(
            execution_id(),
            target_install,
            Duration::from_secs(1),
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
                super::super::domain::BackendTransportSequence::new(1),
            )),
            None,
            None,
            *frame.profile_digest(),
            Arc::<[u8]>::from(frame.payload()),
        )
        .expect("delivery envelope");

        assert!(matches!(
            target.dispatch_envelope(envelope).status(),
            super::super::domain::BackendAcceptStatus::Accepted
        ));
        assert!(matches!(
            subscription.acquire(Duration::from_millis(1)),
            SnapshotAcquireOutcome::Unavailable(
                novarocks_execution::runtime_filter::UnavailableReason::MaterializationFailed
            )
        ));
    }
}

fn execution_placeholder_membership_schema()
-> Result<novarocks_execution::runtime_filter::RuntimeFilterMembershipSchema, ()> {
    novarocks_execution::runtime_filter::RuntimeFilterMembershipSchema::new(
        &arrow::datatypes::DataType::Boolean,
        novarocks_execution::runtime_filter::RuntimeFilterNullSemantics::NeverMatches,
    )
    .map_err(|_| ())
}
