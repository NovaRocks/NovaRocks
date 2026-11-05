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

//! Worker-owned runtime-filter ingress verdicts after Native route decoding.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_execution::runtime_filter::{
    CanonicalContributionBytes, LiveTerminal, PartitionId, ProducerSequence,
    RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterContribution,
    RuntimeFilterContributionKind, RuntimeFilterExecutionContract, RuntimeFilterMembershipSchema,
    RuntimeFilterNullSemantics, RuntimeFilterProducerFailure, RuntimeFilterProducerKind,
    RuntimeFilterProducerOpenRequest, RuntimeFilterSnapshot, RuntimeFilterSubmitOutcome,
    SnapshotAcquireOutcome, UnavailableReason,
};
use novarocks_types::UniqueId;
use sha2::{Digest, Sha256};

use super::artifact_query::BackendRuntimeFilterArtifactQuery;
use super::codec::{artifact as artifact_codec, producer as producer_codec};
use super::domain::{
    BackendChannelIdentity, BackendDeliveryAdmission, BackendDeliveryRouteIdentity,
    BackendEnvelopeKind, BackendIngressDedupe, BackendIngressResult, BackendParticipantIdentity,
    BackendParticipantInstall, BackendProducerStreamIdentity, BackendRouteEdgeId,
    BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver, BackendRuntimeFilterSession,
    BackendTransportSequence,
};
use super::observation::RuntimeFilterObservationEmitter;

const DELIVERY_REJECTION: &str = "runtime filter ingress rejected [artifact-delivery]: delivery violates the installed artifact contract";

/// Exact delivery route recovered from the Native envelope. It deliberately
/// lacks a consumer binding: that binding is resolved only from Worker-owned
/// installed consumer state.
pub struct DeliveryIngressRoute {
    channel_id: RuntimeFilterChannelId,
    route_edge_id: BackendRouteEdgeId,
    sequence: BackendTransportSequence,
}

impl DeliveryIngressRoute {
    pub const fn new(
        channel_id: RuntimeFilterChannelId,
        route_edge_id: BackendRouteEdgeId,
        sequence: BackendTransportSequence,
    ) -> Self {
        Self {
            channel_id,
            route_edge_id,
            sequence,
        }
    }
}

/// Decoded artifact-delivery facts whose contract interpretation belongs to Worker.
pub struct DeliveryIngressFrame<'a> {
    kind: BackendEnvelopeKind,
    schema_digest: [u8; 32],
    payload: &'a [u8],
}

impl<'a> DeliveryIngressFrame<'a> {
    pub const fn new(
        kind: BackendEnvelopeKind,
        schema_digest: [u8; 32],
        payload: &'a [u8],
    ) -> Self {
        Self {
            kind,
            schema_digest,
            payload,
        }
    }
}

/// Applies a decoded artifact-delivery frame against Worker-owned consumer state.
pub fn dispatch_delivery_frame(
    install: &BackendParticipantInstall,
    consumer_sessions: &BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    delivery_dedupe: &BackendIngressDedupe,
    route: DeliveryIngressRoute,
    frame: DeliveryIngressFrame<'_>,
) -> BackendIngressResult {
    if install
        .routing()
        .authorize_delivery(route.channel_id, route.route_edge_id, frame.kind)
        .is_err()
    {
        return rejected(
            "runtime filter ingress rejected [artifact-delivery]: route is not authorized for this delivery",
        );
    }
    let Some((binding_id, session)) = consumer_sessions.iter().find_map(|(binding_id, session)| {
        session
            .channel()
            .consumers()
            .get(binding_id)
            .filter(|consumer| consumer.route_edge_ids().contains(&route.route_edge_id))
            .map(|_| (*binding_id, Arc::clone(session)))
    }) else {
        return rejected(
            "runtime filter ingress rejected [artifact-delivery]: no installed consumer owns this route",
        );
    };
    if session.channel().channel_id() != route.channel_id {
        return rejected(
            "runtime filter ingress rejected [artifact-delivery]: route resolves to a different channel",
        );
    }
    let Some(consumer) = session.channel().consumers().get(&binding_id) else {
        return rejected(
            "runtime filter ingress rejected [artifact-delivery]: consumer install disappeared",
        );
    };
    let outcome = match frame.kind {
        BackendEnvelopeKind::Artifact | BackendEnvelopeKind::FinalArtifact => {
            let placeholder = match execution_placeholder_membership_schema() {
                Ok(schema) => schema,
                Err(()) => return rejected(DELIVERY_REJECTION),
            };
            let (schema, order_contract, contract_digest) = match consumer.contract().contract() {
                RuntimeFilterExecutionContract::Membership(schema) => {
                    (schema, None, schema.digest())
                }
                RuntimeFilterExecutionContract::Ordered(order) => {
                    let Some(key) = order.keys().first() else {
                        return rejected(DELIVERY_REJECTION);
                    };
                    let _ = key;
                    (&placeholder, Some(order.as_ref()), order.digest())
                }
            };
            let bundle = artifact_codec::decode_artifact_bundle(
                frame.payload,
                &frame.schema_digest,
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
                    BackendRuntimeFilterArtifactQuery::ordered(&bundle, order)
                }
            };
            let Ok(query) = query else {
                return rejected(
                    "runtime filter ingress rejected [artifact-delivery]: artifact does not provide the installed evaluator",
                );
            };
            SnapshotAcquireOutcome::Published(Arc::new(RuntimeFilterSnapshot::new(
                binding_id,
                bundle.version(),
                contract_digest,
                Arc::new(query),
            )))
        }
        BackendEnvelopeKind::Unavailable => {
            let Ok(reason) = artifact_codec::decode_unavailable(
                frame.payload,
                &frame.schema_digest,
                consumer.profile(),
                session.channel().max_artifact_bytes(),
            ) else {
                return rejected(
                    "runtime filter ingress rejected [artifact-delivery]: unavailable frame violates the installed profile",
                );
            };
            SnapshotAcquireOutcome::Unavailable(reason)
        }
        BackendEnvelopeKind::DegradedLogical => {
            if producer_codec::decode_producer_failure(frame.payload).is_err() {
                return rejected(
                    "runtime filter ingress rejected [artifact-delivery]: degraded frame is malformed",
                );
            }
            SnapshotAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed)
        }
        BackendEnvelopeKind::CompletedWithoutArtifact => {
            SnapshotAcquireOutcome::Unavailable(UnavailableReason::IncompleteCoverage)
        }
        _ => {
            return rejected(
                "runtime filter ingress rejected [artifact-delivery]: envelope kind is not a delivery",
            );
        }
    };
    let terminal = match frame.kind {
        BackendEnvelopeKind::FinalArtifact => Some(LiveTerminal::Completed),
        BackendEnvelopeKind::CompletedWithoutArtifact => {
            Some(LiveTerminal::CompletedWithoutArtifact)
        }
        _ => None,
    };
    let version = match &outcome {
        SnapshotAcquireOutcome::Published(snapshot) => Some(snapshot.logical_version()),
        _ => None,
    };
    let delivery_route = BackendDeliveryRouteIdentity::new(
        BackendChannelIdentity::new(install.participant(), binding_id, route.channel_id),
        route.route_edge_id,
        route.sequence,
    );
    let (exact_digest, content_digest) = delivery_digests(&frame);
    match delivery_dedupe.reserve_delivery(
        delivery_route,
        version,
        frame.kind == BackendEnvelopeKind::FinalArtifact,
        exact_digest,
        content_digest,
    ) {
        BackendDeliveryAdmission::Fresh => {}
        BackendDeliveryAdmission::Duplicate => return BackendIngressResult::duplicate(),
        BackendDeliveryAdmission::Conflict => {
            return rejected(
                "runtime filter ingress rejected [artifact-delivery]: replay content conflicts with an admitted delivery",
            );
        }
        BackendDeliveryAdmission::ResourceLimit => {
            return rejected(
                "runtime filter ingress rejected [artifact-delivery]: delivery replay identity limit exceeded",
            );
        }
    }
    match session.publish_materialized(route.route_edge_id, outcome, terminal) {
        Ok(()) => {
            delivery_dedupe.commit_delivery(
                delivery_route,
                version,
                frame.kind == BackendEnvelopeKind::FinalArtifact,
                exact_digest,
                content_digest,
            );
            BackendIngressResult::accepted()
        }
        Err(_) => {
            delivery_dedupe.abort_delivery(delivery_route, version);
            rejected(
                "runtime filter ingress rejected [artifact-delivery]: subscription publication rejected the delivery",
            )
        }
    }
}

/// Exact producer coordinates recovered by the Native ingress adapter.
pub struct ProducerIngressRoute {
    channel_id: RuntimeFilterChannelId,
    binding_id: RuntimeFilterBindingId,
    task_key: UniqueId,
    partition: PartitionId,
    sequence: ProducerSequence,
    local_partition_count: u32,
}

impl ProducerIngressRoute {
    pub const fn new(
        channel_id: RuntimeFilterChannelId,
        binding_id: RuntimeFilterBindingId,
        task_key: UniqueId,
        partition: PartitionId,
        sequence: ProducerSequence,
        local_partition_count: u32,
    ) -> Self {
        Self {
            channel_id,
            binding_id,
            task_key,
            partition,
            sequence,
            local_partition_count,
        }
    }
}

/// Decoded producer command whose interpretation belongs to the Worker.
pub enum ProducerIngressCommand {
    Contribution {
        schema_digest: [u8; 32],
        payload: CanonicalContributionBytes,
    },
    Closed,
}

/// Applies a decoded contribution or close frame against one installed participant.
///
/// The Native adapter extracts the exact wire facts, but Worker owns the
/// installed-route decision, producer-open validation, local session mutation,
/// and its observation evidence.
pub fn dispatch_producer_frame(
    install: &BackendParticipantInstall,
    producer_sessions: &BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    observation: &RuntimeFilterObservationEmitter,
    route: ProducerIngressRoute,
    command: ProducerIngressCommand,
) -> BackendIngressResult {
    let envelope_kind = match command {
        ProducerIngressCommand::Contribution { .. } => BackendEnvelopeKind::Contribution,
        ProducerIngressCommand::Closed => BackendEnvelopeKind::ProducerClosed,
    };
    if install
        .routing()
        .authorize_contribution(
            route.channel_id,
            route.binding_id,
            route.task_key,
            envelope_kind,
        )
        .is_err()
    {
        return rejected(
            "runtime filter ingress rejected [route-authority]: producer route is not installed",
        );
    }
    let Some(session) = producer_sessions.get(&route.binding_id) else {
        return rejected(
            "runtime filter ingress rejected [producer-binding]: producer binding is not installed",
        );
    };
    if session.channel().channel_id() != route.channel_id {
        return rejected(
            "runtime filter ingress rejected [producer-binding]: producer binding is installed for a different channel",
        );
    }
    let Some(channel_install) = session.channel().producers().get(&route.binding_id) else {
        return rejected(
            "runtime filter ingress rejected [producer-binding]: producer binding is not installed in its channel",
        );
    };
    if session
        .open_producer(
            route.task_key,
            RuntimeFilterProducerOpenRequest::new(
                channel_install.contract().clone(),
                route.local_partition_count,
            ),
        )
        .is_err()
    {
        return rejected(
            "runtime filter ingress rejected [producer-open]: producer open does not match the installed binding",
        );
    }
    observation.register_producer_instance(
        BackendChannelIdentity::new(install.participant(), route.binding_id, route.channel_id),
        route.task_key,
        route.local_partition_count,
    );
    match command {
        ProducerIngressCommand::Contribution {
            schema_digest,
            payload,
        } => match session.submit(
            route.binding_id,
            route.task_key,
            route.partition,
            route.sequence,
            RuntimeFilterContribution::from_shared(
                contribution_kind(channel_install.contract().kind()),
                schema_digest,
                payload,
            ),
        ) {
            Ok(submission) => {
                record_contribution_outcome(
                    observation,
                    install.participant(),
                    route.binding_id,
                    route.channel_id,
                    route.task_key,
                    route.partition,
                    route.sequence,
                    submission.outcome(),
                );
                if matches!(
                    submission.outcome(),
                    RuntimeFilterSubmitOutcome::Duplicate | RuntimeFilterSubmitOutcome::Stale
                ) {
                    BackendIngressResult::duplicate()
                } else {
                    BackendIngressResult::accepted()
                }
            }
            Err(_) => rejected(
                "runtime filter ingress rejected [contribution]: contribution violates the installed execution contract",
            ),
        },
        ProducerIngressCommand::Closed => match session.close_partition(
            route.binding_id,
            route.task_key,
            route.partition,
            route.sequence,
        ) {
            Ok(RuntimeFilterSubmitOutcome::TerminalNoop) => BackendIngressResult::duplicate(),
            Ok(_) => BackendIngressResult::accepted(),
            Err(_) => rejected(
                "runtime filter ingress rejected [producer-close]: close violates the installed producer route",
            ),
        },
    }
}

/// Records the Worker-owned outcome of an already-authorized contribution.
pub fn record_contribution_outcome(
    observation: &RuntimeFilterObservationEmitter,
    participant: BackendParticipantIdentity,
    binding_id: RuntimeFilterBindingId,
    channel_id: RuntimeFilterChannelId,
    task_key: UniqueId,
    partition: PartitionId,
    sequence: ProducerSequence,
    outcome: RuntimeFilterSubmitOutcome,
) {
    let stream = BackendProducerStreamIdentity::new(
        BackendChannelIdentity::new(participant, binding_id, channel_id),
        task_key,
        partition,
    );
    observation.record(match outcome {
        RuntimeFilterSubmitOutcome::Duplicate => {
            BackendRuntimeFilterEvent::ContributionDuplicateIgnored {
                stream,
                sequence: sequence.get(),
            }
        }
        RuntimeFilterSubmitOutcome::Stale => BackendRuntimeFilterEvent::ContributionStaleIgnored {
            stream,
            sequence: sequence.get(),
        },
        _ => BackendRuntimeFilterEvent::ContributionAccepted {
            stream,
            sequence: sequence.get(),
        },
    });
}

/// Applies a decoded producer-failure frame against one installed participant.
///
/// Native decoding deliberately remains outside this function. Once the
/// adapter has supplied exact route coordinates, installed-route authorization
/// and the local session verdict are Worker responsibilities.
pub fn dispatch_producer_failure(
    install: &BackendParticipantInstall,
    producer_sessions: &BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    channel_id: RuntimeFilterChannelId,
    binding_id: RuntimeFilterBindingId,
    task_key: UniqueId,
) -> BackendIngressResult {
    if install
        .routing()
        .authorize_contribution(
            channel_id,
            binding_id,
            task_key,
            BackendEnvelopeKind::ProducerUnavailable,
        )
        .is_err()
    {
        return rejected(
            "runtime filter ingress rejected [route-authority]: producer failure route is not installed",
        );
    }
    let Some(session) = producer_sessions.get(&binding_id) else {
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
        task_key,
        RuntimeFilterProducerFailure::UpstreamUnavailable,
    ) {
        Ok(RuntimeFilterSubmitOutcome::TerminalNoop) => BackendIngressResult::duplicate(),
        Ok(_) => BackendIngressResult::accepted(),
        Err(_) => rejected(
            "runtime filter ingress rejected [producer-failure]: failure violates the installed producer route",
        ),
    }
}

fn rejected(reason: &'static str) -> BackendIngressResult {
    BackendIngressResult::rejected(reason).expect("runtime-filter rejection reason is non-empty")
}

fn contribution_kind(kind: RuntimeFilterProducerKind) -> RuntimeFilterContributionKind {
    match kind {
        RuntimeFilterProducerKind::Membership => RuntimeFilterContributionKind::Membership,
        RuntimeFilterProducerKind::OrderedBound => RuntimeFilterContributionKind::OrderedBound,
        RuntimeFilterProducerKind::TopKSummary => RuntimeFilterContributionKind::TopKSummary,
        RuntimeFilterProducerKind::FinalDomain => RuntimeFilterContributionKind::FinalDomain,
    }
}

fn delivery_digests(frame: &DeliveryIngressFrame<'_>) -> ([u8; 32], [u8; 32]) {
    let mut content = Sha256::new();
    content.update(frame.schema_digest);
    content.update(frame.payload);
    let content_digest: [u8; 32] = content.finalize().into();

    let mut exact = Sha256::new();
    exact.update([delivery_kind_tag(frame.kind)]);
    exact.update(content_digest);
    (exact.finalize().into(), content_digest)
}

fn delivery_kind_tag(kind: BackendEnvelopeKind) -> u8 {
    match kind {
        BackendEnvelopeKind::Artifact => 1,
        BackendEnvelopeKind::FinalArtifact => 2,
        BackendEnvelopeKind::Unavailable => 3,
        BackendEnvelopeKind::CompletedWithoutArtifact => 4,
        BackendEnvelopeKind::DegradedLogical => 5,
        BackendEnvelopeKind::Contribution => 6,
        BackendEnvelopeKind::ProducerClosed => 7,
        BackendEnvelopeKind::ProducerUnavailable => 8,
        BackendEnvelopeKind::Ack => 9,
    }
}

fn execution_placeholder_membership_schema() -> Result<RuntimeFilterMembershipSchema, ()> {
    RuntimeFilterMembershipSchema::new(&DataType::Boolean, RuntimeFilterNullSemantics::NeverMatches)
        .map_err(|_| ())
}
