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

//! Query-scoped production ingress for inbound runtime-filter envelopes.
//!
//! This is the thin seam between the gRPC `RuntimeFilterEnvelope` wire adapter
//! and a live query's `RuntimeFilterService`. It does exactly three things and
//! reaches no further into the runtime-filter internals:
//!
//! 1. look up the query's already-installed service (lookup-only: it never
//!    creates, revives, or renews a query),
//! 2. dispatch the decoded envelope into that service by its `kind()` —
//!    producer-direction envelopes (`Contribution` / `ProducerClosed`) go to
//!    `dispatch_inbound_producer`, consumer-direction envelopes (`Artifact` /
//!    `Unavailable`) go to `dispatch_inbound_consumer`, and `Ack` (an M3 concern)
//!    is rejected here, and
//! 3. map the typed dispatch result back onto the transport-level
//!    `RuntimeFilterIngressResult`.
//!
//! On a lookup miss it answers with a stable adapter-owned `[query-unavailable]`
//! rejection whose shape matches the typed Core dispatch-error taxonomy, so
//! callers observe one uniform rejection surface. The adapter imports only the
//! two dispatch entrypoints and the result mapping — never the registry, Channel,
//! codec, or subscription internals.

use std::sync::Arc;

use crate::runtime::query_context::{QueryContextManager, QueryId, query_context_manager};
use crate::runtime_filter::port::transport::{
    RuntimeFilterEnvelope, RuntimeFilterEnvelopeIngress, RuntimeFilterEnvelopeKind,
    RuntimeFilterIngressResult,
};
use crate::runtime_filter::service::{
    InboundConsumerDispatchError, InboundConsumerDispatchOutcome, InboundProducerDispatchError,
    InboundProducerDispatchOutcome,
};

// Adapter-owned rejection for a query that is neither active nor within delivery
// grace. It mirrors the typed dispatch-error Display shape
// (`runtime filter ingress rejected [<prefix>]: <detail>`) so the query-miss case
// and the six typed Core dispatch errors surface under one rejection taxonomy.
const QUERY_UNAVAILABLE_REJECTION: &str = "runtime filter ingress rejected [query-unavailable]: \
     runtime filter query is not active or in delivery grace";

// Adapter-owned rejection for an `Ack` envelope: acknowledgement ingress is an M3
// concern, so the kind reaches neither the producer nor the consumer dispatch path.
// It mirrors the dispatch-error Display shape so `Ack` surfaces under the same
// `runtime filter ingress rejected [<prefix>]` rejection taxonomy.
const ACK_UNSUPPORTED_REJECTION: &str = "runtime filter ingress rejected [ack-unsupported]: \
     runtime filter ack ingress is not supported";

/// Production ingress bound to the process-global query context manager.
pub(crate) fn query_scoped_runtime_filter_envelope_ingress() -> Arc<dyn RuntimeFilterEnvelopeIngress>
{
    Arc::new(QueryScopedRuntimeFilterEnvelopeIngress {
        manager: query_context_manager(),
    })
}

/// Component-test constructor that binds the ingress to an isolated manager so a
/// test can register and install its own query without touching global state.
#[cfg(test)]
pub(crate) fn query_scoped_runtime_filter_envelope_ingress_with_manager(
    manager: Arc<QueryContextManager>,
) -> Arc<dyn RuntimeFilterEnvelopeIngress> {
    Arc::new(QueryScopedRuntimeFilterEnvelopeIngress { manager })
}

struct QueryScopedRuntimeFilterEnvelopeIngress {
    manager: Arc<QueryContextManager>,
}

impl RuntimeFilterEnvelopeIngress for QueryScopedRuntimeFilterEnvelopeIngress {
    fn accept(&self, envelope: RuntimeFilterEnvelope) -> RuntimeFilterIngressResult {
        let query_id = QueryId::new(envelope.query_id().high(), envelope.query_id().low());
        let Some(service) = self.manager.runtime_filter_service_for_ingress(query_id) else {
            return RuntimeFilterIngressResult::rejected(QUERY_UNAVAILABLE_REJECTION)
                .expect("query-unavailable reason is non-empty");
        };
        // Dispatch by direction: producer-contribution kinds to the producer path,
        // artifact-delivery kinds to the consumer path, `Ack` (M3) rejected here.
        match envelope.kind() {
            RuntimeFilterEnvelopeKind::Contribution
            | RuntimeFilterEnvelopeKind::ProducerClosed
            | RuntimeFilterEnvelopeKind::ProducerUnavailable => {
                ingress_result_for_producer_dispatch(service.dispatch_inbound_producer(envelope))
            }
            RuntimeFilterEnvelopeKind::Artifact
            | RuntimeFilterEnvelopeKind::FinalArtifact
            | RuntimeFilterEnvelopeKind::Unavailable
            | RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
            | RuntimeFilterEnvelopeKind::DegradedLogical => {
                ingress_result_for_consumer_dispatch(service.dispatch_inbound_consumer(envelope))
            }
            RuntimeFilterEnvelopeKind::Ack => {
                RuntimeFilterIngressResult::rejected(ACK_UNSUPPORTED_REJECTION)
                    .expect("ack-unsupported reason is non-empty")
            }
        }
    }
}

/// Maps a typed inbound producer dispatch result onto the transport ingress
/// result. The stable producer dispatch-error prefixes flow through unchanged via
/// the error's `Display`.
fn ingress_result_for_producer_dispatch(
    dispatched: Result<InboundProducerDispatchOutcome, InboundProducerDispatchError>,
) -> RuntimeFilterIngressResult {
    match dispatched {
        Ok(InboundProducerDispatchOutcome::Accepted) => RuntimeFilterIngressResult::accepted(),
        Ok(InboundProducerDispatchOutcome::Duplicate) => RuntimeFilterIngressResult::duplicate(),
        Err(error) => RuntimeFilterIngressResult::rejected(error.to_string())
            .expect("typed inbound dispatch error has a non-empty reason"),
    }
}

/// Maps a typed inbound consumer-delivery dispatch result onto the transport
/// ingress result. Mirrors the producer mapping; the stable consumer
/// dispatch-error prefixes flow through unchanged via the error's `Display`.
fn ingress_result_for_consumer_dispatch(
    dispatched: Result<InboundConsumerDispatchOutcome, InboundConsumerDispatchError>,
) -> RuntimeFilterIngressResult {
    match dispatched {
        Ok(InboundConsumerDispatchOutcome::Accepted) => RuntimeFilterIngressResult::accepted(),
        Ok(InboundConsumerDispatchOutcome::Duplicate) => RuntimeFilterIngressResult::duplicate(),
        Err(error) => RuntimeFilterIngressResult::rejected(error.to_string())
            .expect("typed inbound consumer dispatch error has a non-empty reason"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::datatypes::DataType;

    use super::{
        ingress_result_for_consumer_dispatch, ingress_result_for_producer_dispatch,
        query_scoped_runtime_filter_envelope_ingress_with_manager,
    };
    use crate::common::types::UniqueId;
    use crate::proto;
    use crate::runtime::query_context::{QueryContextManager, QueryId};
    use crate::runtime_filter::codec::artifact::{
        ArtifactDecodeExpectation, encode_artifact_bundle, encode_unavailable,
        max_encoded_len_for_artifact_budget, semantic_artifact_bytes,
    };
    use crate::runtime_filter::codec::contribution::{
        ContributionCodecExpectation, RuntimeFilterContribution, encode_contribution,
    };
    use crate::runtime_filter::materializer::{MaterializationOutcome, Materializer};
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, NullSemantics, ReductionRequirement,
        RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactMembershipSchema, ConsumerArtifactProfile,
    };
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, PartitionId, ProducerSequence, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::install::{
        ConsumerDeployment, MaterializationPolicy, OutboundMaterializationGroup,
        OutboundMaterializationOwner, ProducerDeployment, RuntimeFilterChannelDeployment,
        RuntimeFilterCoreBudget, RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
    };
    use crate::runtime_filter::port::producer::InstallOutcome;
    use crate::runtime_filter::port::routing::{
        RuntimeFilterChannelRoutingView, RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer,
        RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
    };
    use crate::runtime_filter::port::subscription::{
        ArtifactAcquireOutcome, SubscriptionKind, UnavailableReason,
    };
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactScratchBudget, MemoryAccountError,
        RetainedMemoryReservation, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, DeliveryRouteIdentity, ProducerOpenMetadata,
        RuntimeFilterAcceptStatus, RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind,
        RuntimeFilterIngressResult, RuntimeFilterRouteIdentity,
    };
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain, ValueDomainDelta,
    };
    use crate::runtime_filter::service::{
        InboundConsumerDispatchError, InboundConsumerDispatchErrorKind,
        InboundProducerDispatchError, InboundProducerDispatchErrorKind,
    };
    use crate::service::grpc_runtime_filter_adapter::handle_runtime_filter_envelope;

    // The registered query the adapter looks up. Its `UniqueId` projection is the
    // envelope query id; `hi`/`lo` are arbitrary non-zero coordinates.
    const QUERY: QueryId = QueryId::new(71, 72);
    const QUERY_UID: UniqueId = UniqueId::new(71, 72);
    // Loopback install coordinates (fixed epoch 9 / participant 3 mirror the
    // `runtime_filter::service` loopback-install fixture).
    const EPOCH: u64 = 9;
    const CHANNEL: u32 = 1;
    const PRODUCER_BINDING: u32 = 1;
    const CONSUMER_BINDING: u32 = 2;
    // The consumer's loopback delivery route edge; consumer-direction envelopes
    // (`Artifact` / `Unavailable`) address this edge.
    const CONSUMER_ROUTE: u32 = 40;
    const WITNESS: u32 = 11;
    const PRODUCER_FINST: UniqueId = UniqueId::new(1, 2);
    const CONSUMER_FINST: UniqueId = UniqueId::new(1, 3);

    const QUERY_UNAVAILABLE_REASON: &str = "runtime filter ingress rejected [query-unavailable]: \
         runtime filter query is not active or in delivery grace";

    // --- manager / install scaffolding --------------------------------------------------------

    fn registered_manager() -> Arc<QueryContextManager> {
        let manager = QueryContextManager::new_for_test();
        manager
            .get_or_register_native(
                QUERY,
                false,
                Duration::from_secs(30),
                Duration::from_secs(30),
            )
            .expect("register native query context");
        manager
    }

    fn installed_manager() -> Arc<QueryContextManager> {
        let manager = registered_manager();
        let service = manager
            .runtime_filter_service_for_ingress(QUERY)
            .expect("registered query exposes a runtime filter service");
        assert_eq!(
            service
                .install(loopback_membership_install(membership_deployment(4096)))
                .expect("valid loopback install"),
            InstallOutcome::Installed,
        );
        manager
    }

    fn membership_consumer() -> ConsumerDeployment {
        ConsumerDeployment::new(
            ConsumerActivation::BlockingSnapshot,
            BTreeSet::from([ArtifactCapability::Membership]),
            BTreeSet::from([RouteEdgeId::new(CONSUMER_ROUTE)]),
            BTreeSet::from([CONSUMER_FINST]),
        )
    }

    fn membership_deployment(max_contribution_bytes: u64) -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(WITNESS);
        let consumer = membership_consumer();
        let profile = consumer.artifact_profile().clone();
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(CHANNEL),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
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
                max_contribution_bytes,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
            RuntimeFilterCoreBudget::new(1 << 20),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(PRODUCER_BINDING),
                ProducerDeployment::new(witness, BTreeSet::from([PRODUCER_FINST])),
            )]),
            BTreeMap::from([(BindingId::new(CONSUMER_BINDING), consumer)]),
        )
        .with_outbound_materialization_groups(BTreeMap::from([(
            profile.id(),
            OutboundMaterializationGroup::new(
                OutboundMaterializationOwner::Aggregator,
                profile,
                BTreeSet::from([RouteEdgeId::new(CONSUMER_ROUTE)]),
            ),
        )]))
    }

    // Production-shaped local loopback install: an explicit producer -> aggregator
    // inbound edge per producer binding, which is what lets `dispatch_inbound_producer`
    // authorize a contribution and reach the real Core.
    fn loopback_membership_install(
        channel: RuntimeFilterChannelDeployment,
    ) -> RuntimeFilterParticipantInstall {
        let epoch = DeploymentEpoch::new(EPOCH);
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
        // Each consumer gets a loopback aggregator -> consumer delivery edge keyed by
        // its own route edge set, admitting the delivery kinds. This is what
        // lets `dispatch_inbound_consumer` authorize an `Artifact` / `Unavailable`
        // envelope and reach the target subscription (mirrors the M2C consumer-ingress
        // install fixture).
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

    // --- contribution / envelope builders -----------------------------------------------------

    fn membership_schema() -> ArtifactMembershipSchema {
        ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NullSafeEqual).unwrap()
    }

    fn membership_contribution(value: i64) -> RuntimeFilterContribution {
        RuntimeFilterContribution::Membership(ValueDomainDelta::new(
            MembershipValues::int64([value]),
            false,
        ))
    }

    fn encode_membership(value: i64) -> ([u8; 32], Vec<u8>) {
        encode_contribution(
            &membership_contribution(value),
            ContributionCodecExpectation::Membership(&membership_schema()),
            usize::MAX,
        )
        .unwrap()
        .into_parts()
    }

    #[allow(clippy::too_many_arguments)]
    fn producer_envelope(
        kind: RuntimeFilterEnvelopeKind,
        epoch: u64,
        binding: u32,
        fragment_instance_id: UniqueId,
        partition: u32,
        sequence: u64,
        producer_open: Option<u32>,
        schema_digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        let route = ContributionRouteIdentity::try_new(
            BindingId::new(binding),
            fragment_instance_id,
            PartitionId::new(partition),
            ProducerSequence::new(sequence),
        )
        .expect("valid contribution route identity");
        RuntimeFilterEnvelope::try_new(
            kind,
            QUERY_UID,
            ChannelId::new(CHANNEL),
            DeploymentEpoch::new(epoch),
            RuntimeFilterRouteIdentity::contribution(route),
            producer_open.map(|count| {
                ProducerOpenMetadata::try_new(count).expect("nonzero partition count")
            }),
            None,
            &schema_digest,
            payload,
        )
        .expect("valid producer envelope")
    }

    fn contribution_envelope(
        partition: u32,
        sequence: u64,
        count: u32,
        digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        producer_envelope(
            RuntimeFilterEnvelopeKind::Contribution,
            EPOCH,
            PRODUCER_BINDING,
            PRODUCER_FINST,
            partition,
            sequence,
            Some(count),
            digest,
            payload,
        )
    }

    fn closed_envelope(
        epoch: u64,
        binding: u32,
        partition: u32,
        sequence: u64,
        count: u32,
        digest: [u8; 32],
    ) -> RuntimeFilterEnvelope {
        producer_envelope(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            epoch,
            binding,
            PRODUCER_FINST,
            partition,
            sequence,
            Some(count),
            digest,
            Vec::new(),
        )
    }

    fn assert_rejected_prefix(result: &RuntimeFilterIngressResult, prefix: &str) {
        assert_eq!(
            result.accept_status(),
            RuntimeFilterAcceptStatus::Rejected,
            "expected a rejection carrying {prefix}"
        );
        let reason = result
            .rejection_reason()
            .expect("a rejected ingress result carries a reason");
        assert!(
            reason.starts_with("runtime filter ingress rejected "),
            "reason {reason:?} must carry the ingress rejection shape"
        );
        assert!(
            reason.contains(prefix),
            "reason {reason:?} must carry the {prefix} prefix"
        );
    }

    // The consumer dispatch error `Display` carries a distinct `consumer ingress`
    // shape, so a consumer rejection surfacing through the adapter must start with
    // that exact prefix (not the producer `runtime filter ingress rejected ` shape).
    fn assert_rejected_consumer_prefix(result: &RuntimeFilterIngressResult, prefix: &str) {
        assert_eq!(
            result.accept_status(),
            RuntimeFilterAcceptStatus::Rejected,
            "expected a consumer rejection carrying {prefix}"
        );
        let reason = result
            .rejection_reason()
            .expect("a rejected ingress result carries a reason");
        assert!(
            reason.starts_with("runtime filter consumer ingress rejected "),
            "reason {reason:?} must carry the consumer ingress rejection shape"
        );
        assert!(
            reason.contains(prefix),
            "reason {reason:?} must carry the {prefix} prefix"
        );
    }

    // --- consumer delivery fixtures -----------------------------------------------------------

    // Test-only memory account for the local bundle materialization; the live
    // dispatch uses the service's own account supplied by the query context.
    struct Memory;
    impl RuntimeFilterMemoryAccount for Memory {
        fn try_consume(&self, _: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }
        fn release(&self, _: usize) {}
    }

    // The consumer artifact profile the installed membership consumer owns. It is
    // exactly the `[ValueSet, EmptyDomain]` profile `ConsumerDeployment::new` freezes,
    // so an envelope encoded against it matches the installed profile digest.
    fn consumer_profile() -> ConsumerArtifactProfile {
        ConsumerArtifactProfile::m1_test_default()
    }

    fn membership_bundle(profile: &ConsumerArtifactProfile) -> Arc<ArtifactBundle> {
        let values = MembershipValues::int64([1, 2, 3]);
        let schema =
            ArtifactMembershipSchema::new(&values.data_type(), NullSemantics::NeverMatches)
                .unwrap();
        let snapshot = LogicalSnapshot::first(
            ChannelId::new(CHANNEL),
            ReducedMembershipDomain::new(values, false),
            RetainedMemoryReservation::empty(),
        );
        let plan = Materializer::plan(
            Arc::new(snapshot),
            &schema,
            profile,
            MaterializationPolicy::for_test(),
            4096,
        )
        .unwrap();
        match Materializer::materialize(
            plan,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(ArtifactScratchBudget::new(1 << 16, 1 << 16).unwrap()),
            Arc::new(Memory),
        ) {
            MaterializationOutcome::Published(bundle) => bundle,
            other => panic!("membership fixture must publish a bundle, got {other:?}"),
        }
    }

    fn encode_bundle(
        bundle: &ArtifactBundle,
        profile: &ConsumerArtifactProfile,
    ) -> ([u8; 32], Vec<u8>) {
        let ceiling =
            max_encoded_len_for_artifact_budget(semantic_artifact_bytes(bundle).unwrap()).unwrap();
        encode_artifact_bundle(bundle, ArtifactDecodeExpectation::new(profile), ceiling)
            .unwrap()
            .into_parts()
    }

    fn delivery_envelope(
        kind: RuntimeFilterEnvelopeKind,
        route_edge: u32,
        sequence: u64,
        digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        RuntimeFilterEnvelope::try_new(
            kind,
            QUERY_UID,
            ChannelId::new(CHANNEL),
            DeploymentEpoch::new(EPOCH),
            RuntimeFilterRouteIdentity::delivery(
                DeliveryRouteIdentity::try_new(
                    RouteEdgeId::new(route_edge),
                    ProducerSequence::new(sequence),
                )
                .expect("valid delivery route identity"),
            ),
            None,
            None,
            &digest,
            payload,
        )
        .expect("valid delivery envelope")
    }

    fn artifact_envelope(
        sequence: u64,
        digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        delivery_envelope(
            RuntimeFilterEnvelopeKind::Artifact,
            CONSUMER_ROUTE,
            sequence,
            digest,
            payload,
        )
    }

    // --- tests --------------------------------------------------------------------------------

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_query_miss_is_rejected_query_unavailable() {
        // Empty manager: QUERY is never registered.
        let manager = QueryContextManager::new_for_test();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        let result = ingress.accept(closed_envelope(EPOCH, PRODUCER_BINDING, 0, 0, 1, [0; 32]));

        assert_eq!(result.accept_status(), RuntimeFilterAcceptStatus::Rejected);
        assert_eq!(result.rejection_reason(), Some(QUERY_UNAVAILABLE_REASON));
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_installed_query_accepts_membership_contribution()
     {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);
        let (digest, payload) = encode_membership(7);

        let result = ingress.accept(contribution_envelope(0, 0, 1, digest, payload));

        assert_eq!(result.accept_status(), RuntimeFilterAcceptStatus::Accepted);
        assert_eq!(result.rejection_reason(), None);
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_exact_replay_is_duplicate() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);
        let (digest, payload) = encode_membership(7);

        assert_eq!(
            ingress
                .accept(contribution_envelope(0, 0, 1, digest, payload.clone()))
                .accept_status(),
            RuntimeFilterAcceptStatus::Accepted,
        );
        assert_eq!(
            ingress
                .accept(contribution_envelope(0, 0, 1, digest, payload))
                .accept_status(),
            RuntimeFilterAcceptStatus::Duplicate,
        );
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_uninstalled_query_is_deployment_unavailable() {
        // Registered but never installed: dispatch reaches the service and fails
        // fast under the deployment-unavailable prefix.
        let manager = registered_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        let result = ingress.accept(contribution_envelope(0, 0, 1, [0; 32], vec![1]));

        assert_rejected_prefix(&result, "[deployment-unavailable]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_stale_epoch_prefix() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        let result = ingress.accept(closed_envelope(
            EPOCH - 1,
            PRODUCER_BINDING,
            0,
            0,
            1,
            [0; 32],
        ));

        assert_rejected_prefix(&result, "[stale-epoch]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_route_contract_prefix() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        // Unknown producer binding: authorization rejects before the codec step.
        let result = ingress.accept(closed_envelope(
            EPOCH,
            PRODUCER_BINDING + 100,
            0,
            0,
            1,
            [0; 32],
        ));

        assert_rejected_prefix(&result, "[route-contract]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_codec_contract_prefix() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        // Correctly-routed contribution with a non-NRFC (bad-magic) payload frame.
        let result = ingress.accept(contribution_envelope(0, 0, 1, [0; 32], vec![0u8; 20]));

        assert_rejected_prefix(&result, "[codec-contract]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_producer_contract_prefix() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);
        // The producer-close digest matches the installed membership contract, so
        // dispatch clears the codec gate and rejects on the invalid partition id.
        let (digest, _payload) = encode_membership(7);

        let result = ingress.accept(closed_envelope(EPOCH, PRODUCER_BINDING, 5, 0, 1, digest));

        assert_rejected_prefix(&result, "[producer-contract]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_service_unavailable_prefix() {
        // ServiceUnavailable is unreachable through the live producer-submit path
        // (dispatch holds an active installation), so exercise the same mapping the
        // adapter performs on any typed dispatch error.
        let result = ingress_result_for_producer_dispatch(Err(InboundProducerDispatchError::new(
            InboundProducerDispatchErrorKind::ServiceUnavailable,
            "runtime filter service is uninstalled or cancelled",
        )));

        assert_rejected_prefix(&result, "[service-unavailable]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_consumer_service_unavailable_prefix() {
        // Mirror of the producer mapping check on the consumer path: any typed consumer
        // dispatch error surfaces through the adapter with its stable consumer-ingress
        // prefix unchanged.
        let result = ingress_result_for_consumer_dispatch(Err(InboundConsumerDispatchError::new(
            InboundConsumerDispatchErrorKind::ServiceUnavailable,
            "runtime filter service is cancelled or shut down",
        )));

        assert_rejected_consumer_prefix(&result, "[service-unavailable]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_wire_malformed_is_invalid_argument_before_lookup()
     {
        // A wire-malformed protobuf (default => Unspecified kind) fails wire
        // validation before the adapter is consulted. If the query-scoped adapter
        // (bound to an empty manager) were reached, it would return a normal
        // query-unavailable rejection response instead of a gRPC error.
        let manager = QueryContextManager::new_for_test();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);

        let error = handle_runtime_filter_envelope(
            ingress,
            proto::filter::RuntimeFilterEnvelope::default(),
        )
        .expect_err("malformed wire envelope must be an InvalidArgument gRPC error");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_installed_query_accepts_artifact_delivery() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager.clone());
        let profile = consumer_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        let result = ingress.accept(artifact_envelope(1, digest, payload));

        assert_eq!(result.accept_status(), RuntimeFilterAcceptStatus::Accepted);
        assert_eq!(result.rejection_reason(), None);

        // The artifact must have reached the real consumer subscription through
        // `dispatch_inbound_consumer`, not the producer path.
        let snapshot = manager
            .runtime_filter_service_for_ingress(QUERY)
            .expect("installed query exposes a runtime filter service")
            .subscribe(
                BindingId::new(CONSUMER_BINDING),
                CONSUMER_FINST,
                SubscriptionKind::BlockingSnapshot,
            )
            .expect("membership consumer is subscribable")
            .into_blocking()
            .expect("consumer activation is blocking-snapshot")
            .snapshot()
            .map(|delivered| delivered.canonical_digest());
        assert_eq!(
            snapshot,
            Some(bundle.canonical_digest()),
            "the target subscription must receive the logically-equal artifact"
        );
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_installed_query_accepts_unavailable_delivery() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager.clone());
        let profile = consumer_profile();
        let (digest, payload) = encode_unavailable(
            UnavailableReason::IncompleteCoverage,
            ArtifactDecodeExpectation::new(&profile),
            1 << 20,
        )
        .unwrap()
        .into_parts();

        let result = ingress.accept(delivery_envelope(
            RuntimeFilterEnvelopeKind::Unavailable,
            CONSUMER_ROUTE,
            1,
            digest,
            payload,
        ));

        assert_eq!(result.accept_status(), RuntimeFilterAcceptStatus::Accepted);

        let acquired = manager
            .runtime_filter_service_for_ingress(QUERY)
            .expect("installed query exposes a runtime filter service")
            .subscribe(
                BindingId::new(CONSUMER_BINDING),
                CONSUMER_FINST,
                SubscriptionKind::BlockingSnapshot,
            )
            .expect("membership consumer is subscribable")
            .into_blocking()
            .expect("consumer activation is blocking-snapshot")
            .acquire(Duration::ZERO);
        assert!(
            matches!(
                acquired,
                ArtifactAcquireOutcome::Unavailable(UnavailableReason::IncompleteCoverage)
            ),
            "the consumer subscription must observe the Unavailable reason, got {acquired:?}"
        );
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_consumer_exact_replay_is_duplicate() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);
        let profile = consumer_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        assert_eq!(
            ingress
                .accept(artifact_envelope(1, digest, payload.clone()))
                .accept_status(),
            RuntimeFilterAcceptStatus::Accepted,
        );
        assert_eq!(
            ingress
                .accept(artifact_envelope(1, digest, payload))
                .accept_status(),
            RuntimeFilterAcceptStatus::Duplicate,
        );
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_consumer_codec_contract_prefix() {
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);
        // A correctly-routed artifact whose profile digest does not match the installed
        // consumer profile: consumer dispatch clears routing and rejects at the digest
        // gate, carrying the consumer-ingress `[codec-contract]` prefix.
        let correct = consumer_profile().id().bytes();
        let wrong = [0x5A_u8; 32];
        assert_ne!(wrong, correct);

        let result = ingress.accept(artifact_envelope(1, wrong, vec![1]));

        assert_rejected_consumer_prefix(&result, "[codec-contract]");
    }

    #[test]
    fn query_scoped_runtime_filter_envelope_ingress_ack_is_rejected() {
        // `Ack` belongs to M3: the adapter dispatches neither to the producer nor the
        // consumer path and answers a stable adapter-owned rejection.
        let manager = installed_manager();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager);
        let ack = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::Ack,
            QUERY_UID,
            ChannelId::new(CHANNEL),
            DeploymentEpoch::new(EPOCH),
            RuntimeFilterRouteIdentity::delivery(
                DeliveryRouteIdentity::try_new(
                    RouteEdgeId::new(CONSUMER_ROUTE),
                    ProducerSequence::new(1),
                )
                .unwrap(),
            ),
            None,
            Some(RuntimeFilterAcceptStatus::Accepted),
            &[0; 32],
            Vec::new(),
        )
        .expect("valid ack envelope");

        let result = ingress.accept(ack);

        assert_eq!(result.accept_status(), RuntimeFilterAcceptStatus::Rejected);
        let reason = result
            .rejection_reason()
            .expect("ack rejection carries a reason");
        assert!(
            reason.contains("[ack-unsupported]"),
            "ack rejection {reason:?} must carry the [ack-unsupported] prefix"
        );
    }
}
