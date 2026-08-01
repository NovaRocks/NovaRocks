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

//! Inbound consumer-delivery ingress: the mirror dual of `inbound.rs`'s
//! `dispatch_inbound_producer`.
//!
//! A remote artifact, final-artifact, unavailable, or typed terminal envelope arriving
//! at a consumer/relay participant is looked up to its live query, authorized and
//! decoded against the consumer's install-owned `ConsumerArtifactProfile`, and
//! delivered into the target subscription. The pipeline follows a fixed
//! validation-before-delivery
//! order (route identity -> admission -> route authorization -> profile digest ->
//! strict decode -> release lock -> deliver) so no partial state is ever exposed:
//! any earlier failure rejects before the delivery and leaves the subscription
//! untouched. The `operation` lock is never held across the delivery, mirroring
//! the producer ingress.

use std::error::Error;
use std::fmt;

use crate::runtime_filter::codec::artifact::{
    ArtifactDecodeExpectation, ArtifactWireCodecError, decode_artifact_bundle, decode_unavailable,
    max_encoded_len_for_artifact_budget,
};
use crate::runtime_filter::port::identity::LogicalVersion;
use crate::runtime_filter::port::routing::RuntimeFilterRouteContractError;
use crate::runtime_filter::port::subscription::{ArtifactDeliveryOutcome, LiveTerminal};
use crate::runtime_filter::port::transport::{RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind};

use super::RuntimeFilterService;
use super::dedupe::{DeliveredVersionKind, DeliveryAdmission, TombstoneVerdict};
use super::registry::DispatchAdmission;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundConsumerDispatchOutcome {
    Accepted,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundConsumerDispatchErrorKind {
    DeploymentUnavailable,
    StaleEpoch,
    RouteContract,
    CodecContract,
    ServiceUnavailable,
    ResourceLimit,
}

impl InboundConsumerDispatchErrorKind {
    pub(crate) const fn prefix(self) -> &'static str {
        match self {
            Self::DeploymentUnavailable => "[deployment-unavailable]",
            Self::StaleEpoch => "[stale-epoch]",
            Self::RouteContract => "[route-contract]",
            Self::CodecContract => "[codec-contract]",
            Self::ServiceUnavailable => "[service-unavailable]",
            Self::ResourceLimit => "[resource-limit]",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InboundConsumerDispatchError {
    kind: InboundConsumerDispatchErrorKind,
    detail: String,
}

impl InboundConsumerDispatchError {
    pub(crate) fn new(kind: InboundConsumerDispatchErrorKind, detail: impl Into<String>) -> Self {
        let detail = detail.into();
        assert!(
            !detail.is_empty(),
            "inbound consumer rejection detail must not be empty"
        );
        Self { kind, detail }
    }

    pub(crate) const fn kind(&self) -> InboundConsumerDispatchErrorKind {
        self.kind
    }
}

impl fmt::Display for InboundConsumerDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime filter consumer ingress rejected {}: {}",
            self.kind.prefix(),
            self.detail
        )
    }
}

impl Error for InboundConsumerDispatchError {}

impl RuntimeFilterService {
    /// Inbound consumer-delivery dispatch: decode and deliver a remote `Artifact` /
    /// `Unavailable` envelope into the target subscription. Dual of
    /// `dispatch_inbound_producer`.
    ///
    /// The validation-before-delivery order is fixed and inlined (not hidden in
    /// helpers) so the "reject before delivery" invariant is auditable:
    ///
    /// 1. route identity (`as_delivery`) — producer-direction kinds carry a
    ///    contribution identity and are rejected here;
    /// 2. `operation` lock + `dispatch_admission` (Cancelled -> ServiceUnavailable,
    ///    Absent -> DeploymentUnavailable, Active -> the installed snapshot);
    /// 3. `authorize_delivery` — the Router proves the route edge exists, targets
    ///    this participant's Consumer/Relay role, and admits the kind;
    /// 4. take the install-owned `ConsumerArtifactProfile` for that edge and verify
    ///    the envelope profile digest against it;
    /// 5. strict decode against that profile (`Artifact` -> bundle, `Unavailable` ->
    ///    reason);
    /// 6. release the `operation` lock (never held across the delivery);
    /// 7. deliver, idempotent by the stable `(route_edge, version)` identity — an
    ///    exact replay is answered `Duplicate` and is never re-delivered.
    pub(crate) fn dispatch_inbound_consumer(
        &self,
        envelope: RuntimeFilterEnvelope,
    ) -> Result<InboundConsumerDispatchOutcome, InboundConsumerDispatchError> {
        // Step 1: an artifact-delivery envelope must carry a delivery route identity.
        // Contribution / ProducerClosed carry a contribution identity and land here.
        let route = envelope.route_identity().as_delivery().ok_or_else(|| {
            ingress_error(
                InboundConsumerDispatchErrorKind::RouteContract,
                "consumer envelope requires a delivery route identity",
            )
        })?;

        // (query, epoch) tombstone: a late delivery for a retired/stale epoch is rejected
        // without rebuilding context (M2B3 lookup-only). Consulted before admission so a
        // stale epoch after cancel is reported as StaleEpoch, not masked as a bare
        // service-unavailable.
        match self.dedupe.tombstone_verdict(envelope.deployment_epoch()) {
            TombstoneVerdict::Live => {}
            TombstoneVerdict::Retired => {
                return Err(ingress_error(
                    InboundConsumerDispatchErrorKind::ServiceUnavailable,
                    "runtime filter query/epoch is retired",
                ));
            }
            TombstoneVerdict::StaleEpoch => {
                return Err(ingress_error(
                    InboundConsumerDispatchErrorKind::StaleEpoch,
                    "runtime filter envelope epoch is older than a retired epoch",
                ));
            }
        }

        // Step 2: classify the registry under the operation lock so a concurrent
        // cancel/install cannot expose a torn active-then-cancelled view.
        let operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = match self.registry.dispatch_admission() {
            DispatchAdmission::Active(installed) => installed,
            DispatchAdmission::Cancelled => {
                return Err(ingress_error(
                    InboundConsumerDispatchErrorKind::ServiceUnavailable,
                    "runtime filter service is cancelled or shut down",
                ));
            }
            DispatchAdmission::Absent => {
                return Err(ingress_error(
                    InboundConsumerDispatchErrorKind::DeploymentUnavailable,
                    "runtime filter deployment is not active",
                ));
            }
        };

        // Step 3: the Router is the sole delivery authority. It proves the route edge
        // exists, targets this participant's Consumer/Relay role, and admits the kind
        // (Artifact / Unavailable). `Ack` and every other kind are rejected here.
        installed
            .role_router()
            .authorize_delivery(
                envelope.deployment_epoch(),
                envelope.channel_id(),
                route.route_edge_id(),
                envelope.kind(),
            )
            .map_err(map_route_error)?;

        // Step 4: recover the install-owned consumer profile for this edge and verify
        // the envelope's profile digest before decoding. The profile is the codec's
        // sole contract authority; the wire never derives contract facts.
        let plan = installed
            .artifact_plan(envelope.channel_id())
            .ok_or_else(|| {
                ingress_error(
                    InboundConsumerDispatchErrorKind::RouteContract,
                    "delivery channel has no installed artifact plan",
                )
            })?;
        let profile = installed
            .profile_for_route(envelope.channel_id(), route.route_edge_id())
            .ok_or_else(|| {
                ingress_error(
                    InboundConsumerDispatchErrorKind::RouteContract,
                    "delivery route has no installed consumer profile",
                )
            })?;
        if envelope.schema_digest() != &profile.id().bytes() {
            return Err(ingress_error(
                InboundConsumerDispatchErrorKind::CodecContract,
                "delivery profile digest does not match the installed consumer profile",
            ));
        }

        // Step 5: strict decode against the install-owned profile. The wire ceiling is
        // the channel's installed artifact byte budget promoted to its frame length.
        let max_encoded = max_encoded_len_for_artifact_budget(plan.max_artifact_bytes())
            .map_err(map_codec_error)?;
        let expectation = ArtifactDecodeExpectation::new(profile);
        let (outcome, terminal, version) = match envelope.kind() {
            RuntimeFilterEnvelopeKind::Artifact => {
                let bundle = decode_artifact_bundle(
                    envelope.payload(),
                    envelope.schema_digest(),
                    expectation,
                    max_encoded,
                    plan.retained_budget(),
                    self.memory_account.clone(),
                )
                .map_err(map_codec_error)?;
                let version = bundle.version();
                (
                    Some(ArtifactDeliveryOutcome::Published(bundle)),
                    None,
                    Some((version, DeliveredVersionKind::NonFinal)),
                )
            }
            RuntimeFilterEnvelopeKind::FinalArtifact => {
                let bundle = decode_artifact_bundle(
                    envelope.payload(),
                    envelope.schema_digest(),
                    expectation,
                    max_encoded,
                    plan.retained_budget(),
                    self.memory_account.clone(),
                )
                .map_err(map_codec_error)?;
                let version = bundle.version();
                (
                    Some(ArtifactDeliveryOutcome::Published(bundle)),
                    Some(LiveTerminal::Completed),
                    Some((version, DeliveredVersionKind::FinalArtifact)),
                )
            }
            RuntimeFilterEnvelopeKind::Unavailable => {
                let reason = decode_unavailable(
                    envelope.payload(),
                    envelope.schema_digest(),
                    expectation,
                    max_encoded,
                )
                .map_err(map_codec_error)?;
                // The Unavailable sentinel is versionless on the wire; it commits to the
                // reserved zero logical version (never a real bundle version >= FIRST) for
                // delivery-identity idempotency.
                (
                    Some(ArtifactDeliveryOutcome::Unavailable(reason)),
                    None,
                    Some((LogicalVersion::new(0), DeliveredVersionKind::NonFinal)),
                )
            }
            RuntimeFilterEnvelopeKind::CompletedWithoutArtifact => {
                (None, Some(LiveTerminal::CompletedWithoutArtifact), None)
            }
            RuntimeFilterEnvelopeKind::DegradedLogical => {
                let reason = decode_unavailable(
                    envelope.payload(),
                    envelope.schema_digest(),
                    expectation,
                    max_encoded,
                )
                .map_err(map_codec_error)?;
                (None, Some(LiveTerminal::DegradedLogical(reason)), None)
            }
            _ => {
                // Unreachable: `authorize_delivery` already rejected every non-delivery
                // kind. Kept as a defensive route-contract rejection, never a panic.
                return Err(ingress_error(
                    InboundConsumerDispatchErrorKind::RouteContract,
                    "envelope kind is not valid for consumer ingress",
                ));
            }
        };

        // Step 6: never hold `operation` across the delivery — a concurrent cancel must
        // be free to linearize against the subscription state, not this mutex.
        drop(operation);

        // Step 7: deliver, gated by the unified dedupe. Both gates must hold before the
        // delivery is fanned into the subscription; either one answering `Duplicate`
        // short-circuits without re-delivering.
        //   (a) transport-identity gate: an exact wire retry (same route edge + transport
        //       sequence) is absorbed regardless of its logical content;
        //   (b) absorbed logical `(route_edge, version)` gate (M2C spec §7.7): the same
        //       logical version is never delivered twice, even via a distinct transport
        //       sequence.
        let route_edge_id = route.route_edge_id();
        match self.dedupe.admit_delivery(envelope.channel_id(), route) {
            DeliveryAdmission::Fresh => {}
            DeliveryAdmission::Duplicate => {
                return Ok(InboundConsumerDispatchOutcome::Duplicate);
            }
            DeliveryAdmission::ResourceLimit => return Err(resource_limit_error()),
        }
        if let Some((version, kind)) = version {
            match self.dedupe.admit_delivered_version(
                envelope.channel_id(),
                route_edge_id,
                version,
                kind,
            ) {
                DeliveryAdmission::Fresh => {}
                DeliveryAdmission::Duplicate => {
                    return Ok(InboundConsumerDispatchOutcome::Duplicate);
                }
                DeliveryAdmission::ResourceLimit => return Err(resource_limit_error()),
            }
        }
        installed
            .router()
            .route_live(&[route_edge_id], outcome.as_ref(), terminal);
        Ok(InboundConsumerDispatchOutcome::Accepted)
    }
}

// A genuinely-new delivery identity beyond this channel's self-owned dedupe ceiling:
// an explicit first-class resource rejection, not a silent drop. The transport gate
// is checked first, so with a shared ceiling the logical gate is only reached once
// the transport gate has admitted (it never grows faster), and the two gates never
// leave inconsistent state at the cap. Rejected before the delivery is fanned into
// the subscription, so no partial delivery ever occurs.
fn resource_limit_error() -> InboundConsumerDispatchError {
    ingress_error(
        InboundConsumerDispatchErrorKind::ResourceLimit,
        "runtime filter consumer dedupe set is at its per-channel resource ceiling",
    )
}

fn ingress_error(
    kind: InboundConsumerDispatchErrorKind,
    detail: impl Into<String>,
) -> InboundConsumerDispatchError {
    InboundConsumerDispatchError::new(kind, detail)
}

fn map_route_error(error: RuntimeFilterRouteContractError) -> InboundConsumerDispatchError {
    let kind = if matches!(error, RuntimeFilterRouteContractError::StaleEpoch { .. }) {
        InboundConsumerDispatchErrorKind::StaleEpoch
    } else {
        InboundConsumerDispatchErrorKind::RouteContract
    };
    ingress_error(kind, error.to_string())
}

fn map_codec_error(error: ArtifactWireCodecError) -> InboundConsumerDispatchError {
    ingress_error(
        InboundConsumerDispatchErrorKind::CodecContract,
        error.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::codec::artifact::{
        ArtifactDecodeExpectation, encode_artifact_bundle, encode_unavailable,
        max_encoded_len_for_artifact_budget, semantic_artifact_bytes,
    };
    use crate::runtime_filter::core::ordered_reducer::OrderedBoundDomain;
    use crate::runtime_filter::materializer::bloom::BloomHashContract;
    use crate::runtime_filter::materializer::range::{
        RangeMaterializationOutcome, RangeMaterializer,
    };
    use crate::runtime_filter::materializer::{MaterializationOutcome, Materializer};
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder, NullSemantics,
        OrderContract, OrderKeyContract, ReductionRequirement, RuntimeFilterLifecycle,
        RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement, SortDirection,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
    };
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence, RouteEdgeId,
        RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::install::{
        ConsumerDeployment, MaterializationPolicy, OutboundMaterializationGroup,
        OutboundMaterializationOwner, ProducerDeployment, RuntimeFilterChannelDeployment,
        RuntimeFilterCoreBudget, RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
    };
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedScalar, OrderedTuple, RuntimeOrderContract,
        comparator_digest_for_test,
    };
    use crate::runtime_filter::port::producer::InstallOutcome;
    use crate::runtime_filter::port::routing::{
        RuntimeFilterChannelRoutingView, RuntimeFilterRemoteRoute, RuntimeFilterRouteContractError,
        RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer, RuntimeFilterRouteRole,
        RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
    };
    use crate::runtime_filter::port::subscription::{
        ArtifactAcquireOutcome, ArtifactDeliveryOutcome, LivePollOutcome, LiveTerminal,
        SubscriptionKind, UnavailableReason,
    };
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactScratchBudget, MemoryAccountError,
        RetainedMemoryReservation, RuntimeFilterClock, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, DeliveryRouteIdentity, ProducerOpenMetadata,
        RuntimeFilterAcceptStatus, RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind,
        RuntimeFilterRouteIdentity,
    };
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain,
    };

    use crate::runtime_filter::router::remote::{
        RuntimeFilterEnvelopeSink, SinkCompletion, SinkSubmitOutcome,
    };

    use super::InboundConsumerDispatchErrorKind::{
        CodecContract, DeploymentUnavailable, ResourceLimit, RouteContract, ServiceUnavailable,
        StaleEpoch,
    };
    use super::{
        InboundConsumerDispatchError, InboundConsumerDispatchErrorKind,
        InboundConsumerDispatchOutcome, RuntimeFilterService, map_codec_error, map_route_error,
    };

    const EPOCH: u64 = 9;
    const CHANNEL: u32 = 1;
    const PRODUCER_BINDING: u32 = 10;
    const CONSUMER_BINDING: u32 = 30;
    const CONSUMER_ROUTE: u32 = 40;
    const RANGE_CONSUMER_BINDING: u32 = 31;
    const RANGE_ROUTE: u32 = 41;
    const WITNESS: u32 = 101;
    const QID: UniqueId = UniqueId::new(70, 7);
    const ROOMY: usize = 1 << 20;

    fn uid(lo: i64) -> UniqueId {
        UniqueId::new(70, lo)
    }

    fn producer_finst() -> UniqueId {
        uid(10)
    }

    fn consumer_finst() -> UniqueId {
        uid(30)
    }

    fn range_consumer_finst() -> UniqueId {
        uid(31)
    }

    // --- dependency stubs ---------------------------------------------------------------------

    struct Clock;
    impl RuntimeFilterClock for Clock {
        fn now(&self) -> Instant {
            Instant::now()
        }
    }

    struct Memory;
    impl RuntimeFilterMemoryAccount for Memory {
        fn try_consume(&self, _: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }
        fn release(&self, _: usize) {}
    }

    #[derive(Default)]
    struct RecordingEvents(Mutex<Vec<RuntimeFilterEvent>>);
    impl RuntimeFilterEventSink for RecordingEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    fn service() -> Arc<RuntimeFilterService> {
        Arc::new(RuntimeFilterService::new_with_dependencies(
            QID,
            Arc::new(Clock),
            Arc::new(RecordingEvents::default()),
            Arc::new(Memory),
        ))
    }

    // --- deployment builders ------------------------------------------------------------------

    fn membership_profile() -> ConsumerArtifactProfile {
        ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap()
    }

    fn membership_channel(max_artifact_bytes: u64) -> RuntimeFilterChannelDeployment {
        membership_channel_profiled(max_artifact_bytes, membership_profile())
    }

    // Membership channel whose consumer carries an explicit physical profile. Task 5
    // uses this to install a consumer that accepts Bitset/Bloom artifacts so the
    // loopback-equals-remote proof can cover every membership physical repr.
    fn membership_channel_profiled(
        max_artifact_bytes: u64,
        profile: ConsumerArtifactProfile,
    ) -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(WITNESS);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(CHANNEL),
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
                max_contribution_bytes: max_artifact_bytes,
                max_artifact_bytes,
                deadline_ms: 1000,
                max_retries: 1,
            },
            RuntimeFilterCoreBudget::new(1 << 20),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(PRODUCER_BINDING),
                ProducerDeployment::new(witness, BTreeSet::from([producer_finst()])),
            )]),
            BTreeMap::from([(
                BindingId::new(CONSUMER_BINDING),
                ConsumerDeployment::with_profile(
                    ConsumerActivation::BlockingSnapshot,
                    BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    profile,
                    BTreeSet::from([RouteEdgeId::new(CONSUMER_ROUTE)]),
                    BTreeSet::from([consumer_finst()]),
                ),
            )]),
        )
    }

    fn order_plan() -> OrderContract {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let comparator_digest = comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION);
        OrderContract {
            keys,
            inclusive: true,
            comparator_digest,
        }
    }

    fn order_contract() -> RuntimeOrderContract {
        RuntimeOrderContract::try_from_plan(&order_plan()).unwrap()
    }

    fn range_profile() -> ConsumerArtifactProfile {
        ConsumerArtifactProfile::new_ordered_range(order_contract().digest()).unwrap()
    }

    fn ordered_channel() -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(WITNESS);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(CHANNEL),
            RuntimeFilterLogicalDomain::OrderedBound(order_plan()),
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
                max_contribution_bytes: 4096,
                max_artifact_bytes: 4096,
                deadline_ms: 1000,
                max_retries: 1,
            },
            RuntimeFilterCoreBudget::new(1 << 20),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(PRODUCER_BINDING),
                ProducerDeployment::new(witness, BTreeSet::from([producer_finst()])),
            )]),
            BTreeMap::from([(
                BindingId::new(RANGE_CONSUMER_BINDING),
                ConsumerDeployment::with_profile(
                    ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
                    },
                    BTreeSet::from([ArtifactCapability::OrderedRange]),
                    range_profile(),
                    BTreeSet::from([RouteEdgeId::new(RANGE_ROUTE)]),
                    BTreeSet::from([range_consumer_finst()]),
                ),
            )]),
        )
    }

    fn without_local_producers(
        channel: RuntimeFilterChannelDeployment,
    ) -> RuntimeFilterChannelDeployment {
        RuntimeFilterChannelDeployment::new(
            channel.channel_id(),
            channel.logical_domain().clone(),
            channel.lifecycle(),
            channel.availability_coverage().clone(),
            channel.terminal_coverage().clone(),
            channel.reduction_requirement(),
            channel.allowed_contribution_kinds().clone(),
            channel.completion_requirement(),
            channel.policy(),
            channel.core_budget(),
            channel.materialization_policy(),
            BTreeMap::new(),
            channel.consumers().clone(),
        )
    }

    fn install_consumer_only(
        service: &RuntimeFilterService,
        channel: RuntimeFilterChannelDeployment,
    ) {
        let epoch = DeploymentEpoch::new(EPOCH);
        let participant = RuntimeFilterParticipantId::new(3);
        let remote_aggregator = RuntimeFilterParticipantId::new(4);
        let channel_id = channel.channel_id();
        let mut local_roles = BTreeSet::new();
        let mut inbound_edges = Vec::new();
        for (binding_id, consumer) in channel.consumers() {
            local_roles.insert(RuntimeFilterRouteRole::Consumer(*binding_id));
            for route_edge_id in consumer.route_edge_ids() {
                inbound_edges.push(
                    RuntimeFilterRoutingEdgeView::new(
                        channel_id,
                        *route_edge_id,
                        RuntimeFilterRouteEndpointView::new(
                            remote_aggregator,
                            RuntimeFilterRouteRole::Aggregator,
                        ),
                        RuntimeFilterRouteEndpointView::new(
                            participant,
                            RuntimeFilterRouteRole::Consumer(*binding_id),
                        ),
                        RuntimeFilterRoutePeer::Remote {
                            participant_id: remote_aggregator,
                            endpoint: crate::runtime::endpoint::RuntimeEndpoint::new(
                                "remote-aggregator",
                                9060,
                            )
                            .unwrap(),
                        },
                        BTreeSet::from([
                            RuntimeFilterEnvelopeKind::Artifact,
                            RuntimeFilterEnvelopeKind::Unavailable,
                            RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                            RuntimeFilterEnvelopeKind::DegradedLogical,
                            RuntimeFilterEnvelopeKind::FinalArtifact,
                        ]),
                    )
                    .unwrap(),
                );
            }
        }
        let routing_channel = RuntimeFilterChannelRoutingView::new(
            channel_id,
            local_roles,
            BTreeMap::new(),
            inbound_edges,
            Vec::new(),
        )
        .unwrap();
        let install = RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(
                epoch,
                participant,
                BTreeMap::from([(channel_id, channel)]),
            ),
            RuntimeFilterRoutingShard::new(
                epoch,
                participant,
                BTreeMap::from([(channel_id, routing_channel)]),
            )
            .unwrap(),
        );
        assert_eq!(service.install(install).unwrap(), InstallOutcome::Installed);
    }

    // Install participant 3 as a producer/aggregator plus a consumer receiving from
    // a remote aggregator. Consumer ingress owns only inbound delivery-profile
    // authority; it does not imply local outbound materialization authority.
    fn install(service: &RuntimeFilterService, channel: RuntimeFilterChannelDeployment) {
        let consumer_route_kinds = channel
            .consumers()
            .values()
            .flat_map(|consumer| consumer.route_edge_ids().iter().copied())
            .map(|route_edge_id| {
                (
                    route_edge_id,
                    BTreeSet::from([
                        RuntimeFilterEnvelopeKind::Artifact,
                        RuntimeFilterEnvelopeKind::Unavailable,
                        RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                        RuntimeFilterEnvelopeKind::DegradedLogical,
                        RuntimeFilterEnvelopeKind::FinalArtifact,
                    ]),
                )
            })
            .collect();
        install_with_consumer_route_kinds(service, channel, consumer_route_kinds, false);
    }

    fn install_with_loopback_delivery(
        service: &RuntimeFilterService,
        channel: RuntimeFilterChannelDeployment,
    ) {
        let consumer_route_kinds = channel
            .consumers()
            .values()
            .flat_map(|consumer| consumer.route_edge_ids().iter().copied())
            .map(|route_edge_id| {
                (
                    route_edge_id,
                    BTreeSet::from([
                        RuntimeFilterEnvelopeKind::Artifact,
                        RuntimeFilterEnvelopeKind::Unavailable,
                        RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                        RuntimeFilterEnvelopeKind::DegradedLogical,
                        RuntimeFilterEnvelopeKind::FinalArtifact,
                    ]),
                )
            })
            .collect();
        install_with_consumer_route_kinds(service, channel, consumer_route_kinds, true);
    }

    fn install_with_consumer_route_kinds(
        service: &RuntimeFilterService,
        channel: RuntimeFilterChannelDeployment,
        consumer_route_kinds: BTreeMap<RouteEdgeId, BTreeSet<RuntimeFilterEnvelopeKind>>,
        loopback_delivery: bool,
    ) {
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
        for (binding_id, consumer) in channel.consumers() {
            local_roles.insert(RuntimeFilterRouteRole::Consumer(*binding_id));
            for route_edge_id in consumer.route_edge_ids() {
                let source_participant = if loopback_delivery {
                    participant
                } else {
                    RuntimeFilterParticipantId::new(4)
                };
                let edge = RuntimeFilterRoutingEdgeView::new(
                    channel_id,
                    *route_edge_id,
                    RuntimeFilterRouteEndpointView::new(
                        source_participant,
                        RuntimeFilterRouteRole::Aggregator,
                    ),
                    RuntimeFilterRouteEndpointView::new(
                        participant,
                        RuntimeFilterRouteRole::Consumer(*binding_id),
                    ),
                    if loopback_delivery {
                        RuntimeFilterRoutePeer::Loopback
                    } else {
                        RuntimeFilterRoutePeer::Remote {
                            participant_id: source_participant,
                            endpoint: crate::runtime::endpoint::RuntimeEndpoint::new(
                                "remote-aggregator",
                                9060,
                            )
                            .unwrap(),
                        }
                    },
                    consumer_route_kinds.get(route_edge_id).cloned().unwrap(),
                )
                .unwrap();
                inbound_edges.push(edge.clone());
                if loopback_delivery {
                    outbound_edges.push(edge);
                }
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
        let channel = if loopback_delivery {
            let grouped =
                channel
                    .consumers()
                    .values()
                    .fold(BTreeMap::new(), |mut groups, consumer| {
                        groups
                            .entry(consumer.artifact_profile().id())
                            .or_insert_with(|| {
                                (consumer.artifact_profile().clone(), BTreeSet::new())
                            })
                            .1
                            .extend(consumer.route_edge_ids().iter().copied());
                        groups
                    });
            let groups = grouped
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
                .collect();
            channel.with_outbound_materialization_groups(groups)
        } else {
            channel
        };
        let core_view = RuntimeFilterInstallView::new(
            epoch,
            participant,
            BTreeMap::from([(channel_id, channel)]),
        );
        assert_eq!(
            service
                .install(RuntimeFilterParticipantInstall::new(
                    core_view,
                    routing_shard
                ))
                .unwrap(),
            InstallOutcome::Installed,
        );
    }

    // --- artifact fixtures --------------------------------------------------------------------

    fn membership_bundle(profile: &ConsumerArtifactProfile) -> Arc<ArtifactBundle> {
        membership_bundle_values(profile, MembershipValues::int64([1, 2, 3]), 4096)
    }

    // Materialize a membership bundle for arbitrary values / byte budget so Task 5 can
    // drive the materializer to each physical repr (ValueSet, Bitset, Bloom, EmptyDomain)
    // against the matching consumer profile.
    fn membership_bundle_values(
        profile: &ConsumerArtifactProfile,
        values: MembershipValues,
        max_artifact_bytes: usize,
    ) -> Arc<ArtifactBundle> {
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
            max_artifact_bytes,
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

    fn two_artifact_membership_bundle(profile: &ConsumerArtifactProfile) -> Arc<ArtifactBundle> {
        let value_set = membership_bundle(profile);
        let empty = {
            let values = MembershipValues::int64([]);
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
                other => panic!("empty-domain fixture must publish a bundle, got {other:?}"),
            }
        };
        Arc::new(
            ArtifactBundle::new(
                ChannelId::new(CHANNEL),
                LogicalVersion::FIRST,
                profile,
                vec![
                    value_set.artifacts()[0].clone(),
                    empty.artifacts()[0].clone(),
                ],
                ROOMY,
            )
            .unwrap(),
        )
    }

    fn range_bundle(profile: &ConsumerArtifactProfile) -> Arc<ArtifactBundle> {
        let contract = Arc::new(order_contract());
        let tuple = OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(11))]).unwrap();
        let snapshot = Arc::new(LogicalSnapshot::ordered(
            ChannelId::new(CHANNEL),
            LogicalVersion::new(5),
            Arc::new(OrderedBoundDomain::new(contract.clone(), tuple)),
            RetainedMemoryReservation::empty(),
        ));
        match RangeMaterializer::materialize(
            snapshot,
            profile,
            usize::MAX,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(ArtifactScratchBudget::new(1 << 20, 1 << 20).unwrap()),
            Arc::new(Memory),
        ) {
            RangeMaterializationOutcome::Published(bundle) => bundle,
            other => panic!("range fixture must publish a bundle, got {other:?}"),
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

    // --- envelope builders --------------------------------------------------------------------

    fn delivery_env(
        kind: RuntimeFilterEnvelopeKind,
        channel: u32,
        epoch: u64,
        route_edge: u32,
        sequence: u64,
        digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        RuntimeFilterEnvelope::try_new(
            kind,
            QID,
            ChannelId::new(channel),
            DeploymentEpoch::new(epoch),
            RuntimeFilterRouteIdentity::delivery(
                DeliveryRouteIdentity::try_new(
                    RouteEdgeId::new(route_edge),
                    ProducerSequence::new(sequence),
                )
                .unwrap(),
            ),
            None,
            None,
            &digest,
            payload,
        )
        .unwrap()
    }

    fn artifact_env(
        route_edge: u32,
        sequence: u64,
        digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        delivery_env(
            RuntimeFilterEnvelopeKind::Artifact,
            CHANNEL,
            EPOCH,
            route_edge,
            sequence,
            digest,
            payload,
        )
    }

    // --- read-only subscription observation ---------------------------------------------------

    // Snapshot of everything a legal delivery would touch: the blocking consumer's
    // retained bundle digest (present only once a bundle is delivered) and the
    // SubscriptionGroup delivery call count. Negative dispatch cases assert this is
    // identical before and after the call, proving no delivery occurred.
    #[derive(Debug, PartialEq)]
    struct DeliveryObservation {
        snapshot_digest: Option<[u8; 32]>,
        delivery_calls: usize,
    }

    fn observe(service: &RuntimeFilterService) -> DeliveryObservation {
        let installed = service
            .registry
            .active_installation()
            .expect("service is installed");
        let handle = service
            .subscribe_blocking(BindingId::new(CONSUMER_BINDING), consumer_finst())
            .expect("membership consumer is subscribable");
        DeliveryObservation {
            snapshot_digest: handle.snapshot().map(|bundle| bundle.canonical_digest()),
            delivery_calls: installed
                .subscription_delivery_call_count(BindingId::new(CONSUMER_BINDING)),
        }
    }

    fn err(
        result: Result<InboundConsumerDispatchOutcome, InboundConsumerDispatchError>,
    ) -> InboundConsumerDispatchError {
        result.expect_err("dispatch must reject")
    }

    fn assert_prefix(error: &InboundConsumerDispatchError, kind: InboundConsumerDispatchErrorKind) {
        assert_eq!(error.kind(), kind);
        assert!(
            error.to_string().contains(kind.prefix()),
            "{} must carry the {} prefix",
            error,
            kind.prefix()
        );
    }

    // ==========================================================================================
    // Positive matrix: a decoded artifact reaches the target subscription logically equal.
    // ==========================================================================================

    #[test]
    fn inbound_consumer_dispatch_membership_artifact_reaches_subscription() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        let before = observe(&service);
        assert_eq!(before.snapshot_digest, None);
        assert_eq!(before.delivery_calls, 0);

        assert_eq!(
            service
                .dispatch_inbound_consumer(artifact_env(CONSUMER_ROUTE, 1, digest, payload))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );

        let after = observe(&service);
        assert_eq!(
            after.snapshot_digest,
            Some(bundle.canonical_digest()),
            "the target subscription must receive the logically-equal artifact"
        );
        assert_eq!(after.delivery_calls, 1);
    }

    #[test]
    fn inbound_consumer_dispatch_range_artifact_reaches_subscription() {
        let service = service();
        install(&service, ordered_channel());
        let profile = range_profile();
        let bundle = range_bundle(&profile);
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::Range);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        assert_eq!(
            service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::Artifact,
                    CHANNEL,
                    EPOCH,
                    RANGE_ROUTE,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );

        let delivered = service
            .subscribe(
                BindingId::new(RANGE_CONSUMER_BINDING),
                range_consumer_finst(),
                SubscriptionKind::NonBlockingLive,
            )
            .unwrap()
            .into_live()
            .unwrap()
            .snapshot()
            .expect("the live range subscription must retain the delivered artifact");
        assert_eq!(
            delivered.canonical_digest(),
            bundle.canonical_digest(),
            "the range subscription must receive the logically-equal artifact"
        );
    }

    #[test]
    fn remote_consumer_only_completed_without_artifact_is_typed_and_idempotent() {
        let service = service();
        install_consumer_only(&service, without_local_producers(ordered_channel()));
        assert!(
            service
                .open_producer(
                    BindingId::new(PRODUCER_BINDING),
                    producer_finst(),
                    1,
                    crate::runtime_filter::port::producer::ProducerPortKind::OrderedBound,
                )
                .is_err(),
            "consumer-only installation must not rely on a local producer"
        );
        let profile = range_profile();
        let envelope = || {
            delivery_env(
                RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                CHANNEL,
                EPOCH,
                RANGE_ROUTE,
                1,
                profile.id().bytes(),
                Vec::new(),
            )
        };
        assert_eq!(
            service.dispatch_inbound_consumer(envelope()).unwrap(),
            InboundConsumerDispatchOutcome::Accepted
        );
        assert_eq!(
            service.dispatch_inbound_consumer(envelope()).unwrap(),
            InboundConsumerDispatchOutcome::Duplicate
        );
        let live = service
            .subscribe(
                BindingId::new(RANGE_CONSUMER_BINDING),
                range_consumer_finst(),
                SubscriptionKind::NonBlockingLive,
            )
            .unwrap()
            .into_live()
            .unwrap();
        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: Some(LiveTerminal::CompletedWithoutArtifact),
            }
        ));
    }

    #[test]
    fn remote_consumer_only_degraded_logical_retains_prior_artifact() {
        let service = service();
        install_consumer_only(&service, without_local_producers(ordered_channel()));
        let profile = range_profile();
        let bundle = range_bundle(&profile);
        let (artifact_digest, artifact_payload) = encode_bundle(&bundle, &profile);
        assert_eq!(
            service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::Artifact,
                    CHANNEL,
                    EPOCH,
                    RANGE_ROUTE,
                    1,
                    artifact_digest,
                    artifact_payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted
        );
        let (terminal_digest, terminal_payload) = encode_unavailable(
            UnavailableReason::ProducerFailed,
            ArtifactDecodeExpectation::new(&profile),
            ROOMY,
        )
        .unwrap()
        .into_parts();
        let terminal = || {
            delivery_env(
                RuntimeFilterEnvelopeKind::DegradedLogical,
                CHANNEL,
                EPOCH,
                RANGE_ROUTE,
                2,
                terminal_digest,
                terminal_payload.clone(),
            )
        };
        assert_eq!(
            service.dispatch_inbound_consumer(terminal()).unwrap(),
            InboundConsumerDispatchOutcome::Accepted
        );
        assert_eq!(
            service.dispatch_inbound_consumer(terminal()).unwrap(),
            InboundConsumerDispatchOutcome::Duplicate
        );
        let live = service
            .subscribe(
                BindingId::new(RANGE_CONSUMER_BINDING),
                range_consumer_finst(),
                SubscriptionKind::NonBlockingLive,
            )
            .unwrap()
            .into_live()
            .unwrap();
        assert_eq!(
            live.snapshot().unwrap().canonical_digest(),
            bundle.canonical_digest(),
            "logical degradation must retain the last published artifact"
        );
        assert!(matches!(
            live.poll_after(Some(bundle.version())),
            LivePollOutcome::Idle {
                latest_version: Some(version),
                terminal: Some(LiveTerminal::DegradedLogical(
                    UnavailableReason::ProducerFailed
                )),
            } if version == bundle.version()
        ));
    }

    #[test]
    fn remote_consumer_only_final_artifact_sets_latest_and_completed_atomically() {
        let service = service();
        install_consumer_only(&service, without_local_producers(ordered_channel()));
        let profile = range_profile();
        let bundle = range_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);
        let envelope = || {
            delivery_env(
                RuntimeFilterEnvelopeKind::FinalArtifact,
                CHANNEL,
                EPOCH,
                RANGE_ROUTE,
                1,
                digest,
                payload.clone(),
            )
        };

        assert_eq!(
            service.dispatch_inbound_consumer(envelope()).unwrap(),
            InboundConsumerDispatchOutcome::Accepted
        );
        assert_eq!(
            service.dispatch_inbound_consumer(envelope()).unwrap(),
            InboundConsumerDispatchOutcome::Duplicate
        );
        let live = service
            .subscribe(
                BindingId::new(RANGE_CONSUMER_BINDING),
                range_consumer_finst(),
                SubscriptionKind::NonBlockingLive,
            )
            .unwrap()
            .into_live()
            .unwrap();
        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Updated {
                bundle: delivered,
                terminal: Some(LiveTerminal::Completed),
            } if delivered.canonical_digest() == bundle.canonical_digest()
        ));
    }

    #[test]
    fn same_version_artifact_then_final_artifact_upgrades_completed_once() {
        let service = service();
        install_consumer_only(&service, without_local_producers(ordered_channel()));
        let profile = range_profile();
        let bundle = range_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        assert_eq!(
            service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::Artifact,
                    CHANNEL,
                    EPOCH,
                    RANGE_ROUTE,
                    1,
                    digest,
                    payload.clone(),
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted
        );
        let final_artifact = || {
            delivery_env(
                RuntimeFilterEnvelopeKind::FinalArtifact,
                CHANNEL,
                EPOCH,
                RANGE_ROUTE,
                2,
                digest,
                payload.clone(),
            )
        };
        assert_eq!(
            service.dispatch_inbound_consumer(final_artifact()).unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
            "the final envelope must merge completion into an existing version"
        );
        assert_eq!(
            service.dispatch_inbound_consumer(final_artifact()).unwrap(),
            InboundConsumerDispatchOutcome::Duplicate
        );

        let live = service
            .subscribe(
                BindingId::new(RANGE_CONSUMER_BINDING),
                range_consumer_finst(),
                SubscriptionKind::NonBlockingLive,
            )
            .unwrap()
            .into_live()
            .unwrap();
        assert!(matches!(
            live.poll_after(Some(bundle.version())),
            LivePollOutcome::Idle {
                latest_version: Some(version),
                terminal: Some(LiveTerminal::Completed),
            } if version == bundle.version()
        ));
    }

    #[test]
    fn final_artifact_rejects_profile_payload_and_route_contract_drift() {
        let service = service();
        install_consumer_only(&service, without_local_producers(ordered_channel()));
        let profile = range_profile();
        let bundle = range_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        assert_prefix(
            &err(service.dispatch_inbound_consumer(delivery_env(
                RuntimeFilterEnvelopeKind::FinalArtifact,
                CHANNEL,
                EPOCH,
                RANGE_ROUTE,
                1,
                [0x5a; 32],
                payload.clone(),
            ))),
            CodecContract,
        );
        assert_prefix(
            &err(service.dispatch_inbound_consumer(delivery_env(
                RuntimeFilterEnvelopeKind::FinalArtifact,
                CHANNEL,
                EPOCH,
                RANGE_ROUTE,
                2,
                digest,
                b"not-an-artifact-frame".to_vec(),
            ))),
            CodecContract,
        );
        assert_prefix(
            &err(service.dispatch_inbound_consumer(delivery_env(
                RuntimeFilterEnvelopeKind::FinalArtifact,
                CHANNEL,
                EPOCH,
                999,
                3,
                digest,
                payload,
            ))),
            RouteContract,
        );
    }

    #[test]
    fn remote_final_artifact_keeps_blocking_snapshot_semantics() {
        let service = service();
        install_consumer_only(&service, without_local_producers(membership_channel(4096)));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        assert_eq!(
            service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::FinalArtifact,
                    CHANNEL,
                    EPOCH,
                    CONSUMER_ROUTE,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted
        );
        let ArtifactAcquireOutcome::Published(delivered) = service
            .subscribe_blocking(BindingId::new(CONSUMER_BINDING), consumer_finst())
            .unwrap()
            .acquire(Duration::ZERO)
        else {
            panic!("final artifact must publish the blocking snapshot")
        };
        assert_eq!(delivered.canonical_digest(), bundle.canonical_digest());
    }

    #[test]
    fn remote_terminal_rejects_wrong_profile_and_malformed_reason() {
        let service = service();
        install_consumer_only(&service, without_local_producers(ordered_channel()));
        let wrong_profile = delivery_env(
            RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
            CHANNEL,
            EPOCH,
            RANGE_ROUTE,
            1,
            [0x5a; 32],
            Vec::new(),
        );
        assert_prefix(
            &err(service.dispatch_inbound_consumer(wrong_profile)),
            CodecContract,
        );
        let malformed = delivery_env(
            RuntimeFilterEnvelopeKind::DegradedLogical,
            CHANNEL,
            EPOCH,
            RANGE_ROUTE,
            2,
            range_profile().id().bytes(),
            b"not-a-canonical-reason-frame".to_vec(),
        );
        assert_prefix(
            &err(service.dispatch_inbound_consumer(malformed)),
            CodecContract,
        );

        let profile = range_profile();
        let (digest, mut unknown_reason_payload) = encode_unavailable(
            UnavailableReason::ProducerFailed,
            ArtifactDecodeExpectation::new(&profile),
            ROOMY,
        )
        .unwrap()
        .into_parts();
        *unknown_reason_payload
            .last_mut()
            .expect("unavailable reason frame has a body") = u8::MAX;
        let unknown_reason = delivery_env(
            RuntimeFilterEnvelopeKind::DegradedLogical,
            CHANNEL,
            EPOCH,
            RANGE_ROUTE,
            3,
            digest,
            unknown_reason_payload,
        );
        assert_prefix(
            &err(service.dispatch_inbound_consumer(unknown_reason)),
            CodecContract,
        );
    }

    #[test]
    fn remote_live_terminal_does_not_complete_blocking_snapshot() {
        let service = service();
        install_consumer_only(&service, without_local_producers(membership_channel(4096)));
        let profile = membership_profile();
        assert_eq!(
            service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                    CHANNEL,
                    EPOCH,
                    CONSUMER_ROUTE,
                    1,
                    profile.id().bytes(),
                    Vec::new(),
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted
        );
        assert!(matches!(
            service
                .subscribe_blocking(BindingId::new(CONSUMER_BINDING), consumer_finst())
                .unwrap()
                .acquire(Duration::ZERO),
            ArtifactAcquireOutcome::TimedOut
        ));
    }

    // ==========================================================================================
    // Unavailable delivery: Accepted, observed as Unavailable(reason), never revokes an artifact.
    // ==========================================================================================

    #[test]
    fn inbound_consumer_dispatch_unavailable_reaches_subscription() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let (digest, payload) = encode_unavailable(
            UnavailableReason::IncompleteCoverage,
            ArtifactDecodeExpectation::new(&profile),
            ROOMY,
        )
        .unwrap()
        .into_parts();

        assert_eq!(
            service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::Unavailable,
                    CHANNEL,
                    EPOCH,
                    CONSUMER_ROUTE,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );

        let acquired = service
            .subscribe_blocking(BindingId::new(CONSUMER_BINDING), consumer_finst())
            .unwrap()
            .acquire(Duration::ZERO);
        assert!(
            matches!(
                acquired,
                ArtifactAcquireOutcome::Unavailable(UnavailableReason::IncompleteCoverage)
            ),
            "the subscription must observe the Unavailable reason, got {acquired:?}"
        );
    }

    #[test]
    fn multiple_canonical_consumer_routes_dispatch_through_same_subscription() {
        let service = service();
        let channel = membership_channel(4096);
        let consumer = channel
            .consumers()
            .get(&BindingId::new(CONSUMER_BINDING))
            .unwrap();
        let mut consumers = channel.consumers().clone();
        consumers.insert(
            BindingId::new(CONSUMER_BINDING),
            ConsumerDeployment::with_profile(
                consumer.activation(),
                consumer.capabilities().clone(),
                consumer.artifact_profile().clone(),
                BTreeSet::from([RouteEdgeId::new(CONSUMER_ROUTE), RouteEdgeId::new(41)]),
                consumer.expected_fragment_instances().clone(),
            ),
        );
        let channel = RuntimeFilterChannelDeployment::new(
            channel.channel_id(),
            channel.logical_domain().clone(),
            channel.lifecycle(),
            channel.availability_coverage().clone(),
            channel.terminal_coverage().clone(),
            channel.reduction_requirement(),
            channel.allowed_contribution_kinds().clone(),
            channel.completion_requirement(),
            channel.policy(),
            channel.core_budget(),
            channel.materialization_policy(),
            channel.producers().clone(),
            consumers,
        );
        install_with_consumer_route_kinds(
            &service,
            channel,
            BTreeMap::from([
                (
                    RouteEdgeId::new(CONSUMER_ROUTE),
                    BTreeSet::from([
                        RuntimeFilterEnvelopeKind::Artifact,
                        RuntimeFilterEnvelopeKind::Unavailable,
                        RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                        RuntimeFilterEnvelopeKind::DegradedLogical,
                        RuntimeFilterEnvelopeKind::FinalArtifact,
                    ]),
                ),
                (
                    RouteEdgeId::new(41),
                    BTreeSet::from([
                        RuntimeFilterEnvelopeKind::Artifact,
                        RuntimeFilterEnvelopeKind::Unavailable,
                        RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                        RuntimeFilterEnvelopeKind::DegradedLogical,
                        RuntimeFilterEnvelopeKind::FinalArtifact,
                    ]),
                ),
            ]),
            false,
        );

        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (artifact_digest, artifact_payload) = encode_bundle(&bundle, &profile);
        let (unavailable_digest, unavailable_payload) = encode_unavailable(
            UnavailableReason::ProducerFailed,
            ArtifactDecodeExpectation::new(&profile),
            ROOMY,
        )
        .unwrap()
        .into_parts();

        assert_eq!(
            service
                .dispatch_inbound_consumer(artifact_env(
                    CONSUMER_ROUTE,
                    1,
                    artifact_digest,
                    artifact_payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );
        assert_eq!(
            service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::Unavailable,
                    CHANNEL,
                    EPOCH,
                    41,
                    2,
                    unavailable_digest,
                    unavailable_payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );

        let observation = observe(&service);
        assert_eq!(observation.snapshot_digest, Some(bundle.canonical_digest()));
        assert_eq!(observation.delivery_calls, 2);
    }

    #[test]
    fn inbound_consumer_dispatch_unavailable_does_not_revoke_delivered_artifact() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (artifact_digest, artifact_payload) = encode_bundle(&bundle, &profile);
        let (unavailable_digest, unavailable_payload) = encode_unavailable(
            UnavailableReason::ProducerFailed,
            ArtifactDecodeExpectation::new(&profile),
            ROOMY,
        )
        .unwrap()
        .into_parts();

        assert_eq!(
            service
                .dispatch_inbound_consumer(artifact_env(
                    CONSUMER_ROUTE,
                    1,
                    artifact_digest,
                    artifact_payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );
        assert_eq!(
            observe(&service).snapshot_digest,
            Some(bundle.canonical_digest())
        );

        // A later Unavailable is Accepted but must not revoke the retained artifact.
        assert_eq!(
            service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::Unavailable,
                    CHANNEL,
                    EPOCH,
                    CONSUMER_ROUTE,
                    2,
                    unavailable_digest,
                    unavailable_payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );
        assert_eq!(
            observe(&service).snapshot_digest,
            Some(bundle.canonical_digest()),
            "an Unavailable delivery must not revoke the already-delivered artifact"
        );
    }

    // ==========================================================================================
    // Replay: an exact `(route_edge, version)` replay is Duplicate and never re-delivered.
    // ==========================================================================================

    #[test]
    fn inbound_consumer_dispatch_exact_replay_is_duplicate() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);
        let env = || artifact_env(CONSUMER_ROUTE, 1, digest, payload.clone());

        assert_eq!(
            service.dispatch_inbound_consumer(env()).unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );
        let seeded = observe(&service);
        assert_eq!(seeded.snapshot_digest, Some(bundle.canonical_digest()));
        assert_eq!(seeded.delivery_calls, 1);

        assert_eq!(
            service.dispatch_inbound_consumer(env()).unwrap(),
            InboundConsumerDispatchOutcome::Duplicate,
        );
        let replayed = observe(&service);
        assert_eq!(
            replayed.delivery_calls, 1,
            "an exact replay must not re-deliver into the subscription"
        );
        assert_eq!(replayed.snapshot_digest, seeded.snapshot_digest);
    }

    // ==========================================================================================
    // Negatives: each proves no delivery occurred (subscription observation unchanged).
    // ==========================================================================================

    #[test]
    fn inbound_consumer_dispatch_stale_epoch_is_rejected() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        let before = observe(&service);
        let error = err(service.dispatch_inbound_consumer(delivery_env(
            RuntimeFilterEnvelopeKind::Artifact,
            CHANNEL,
            EPOCH - 1,
            CONSUMER_ROUTE,
            1,
            digest,
            payload,
        )));
        assert_prefix(&error, StaleEpoch);
        assert_eq!(before, observe(&service));
    }

    #[test]
    fn inbound_consumer_dispatch_requires_active_installed_deployment() {
        // With no installation, dispatch fails fast under the deployment-unavailable
        // prefix and never touches any subscription.
        let service = service();
        let error = err(service.dispatch_inbound_consumer(artifact_env(
            CONSUMER_ROUTE,
            1,
            [0; 32],
            vec![1],
        )));
        assert_prefix(&error, DeploymentUnavailable);
    }

    #[test]
    fn inbound_consumer_dispatch_cancelled_service_is_rejected() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        // Capture the installation and subscription handle before cancelling, since a
        // cancelled service no longer exposes an active installation to re-subscribe
        // against. Cancel itself delivers a `Cancelled` sentinel to the subscription, so
        // the dispatch's effect is isolated by measuring the delta across the dispatch
        // call alone (after cancel has already run).
        let installed = service.registry.active_installation().unwrap();
        let handle = service
            .subscribe_blocking(BindingId::new(CONSUMER_BINDING), consumer_finst())
            .unwrap();
        service.cancel();
        let after_cancel_calls =
            installed.subscription_delivery_call_count(BindingId::new(CONSUMER_BINDING));
        let after_cancel_snapshot = handle.snapshot().map(|bundle| bundle.canonical_digest());

        let error = err(service.dispatch_inbound_consumer(artifact_env(
            CONSUMER_ROUTE,
            1,
            digest,
            payload,
        )));
        assert_prefix(&error, ServiceUnavailable);
        assert_eq!(
            handle.snapshot().map(|bundle| bundle.canonical_digest()),
            after_cancel_snapshot,
            "a rejected dispatch must not deliver the artifact"
        );
        assert_eq!(
            installed.subscription_delivery_call_count(BindingId::new(CONSUMER_BINDING)),
            after_cancel_calls,
            "a rejected dispatch must not deliver into the subscription"
        );
    }

    #[test]
    fn inbound_consumer_dispatch_unknown_route_edge_is_rejected() {
        let service = service();
        install(&service, membership_channel(4096));

        let before = observe(&service);
        let error = err(service.dispatch_inbound_consumer(artifact_env(
            CONSUMER_ROUTE + 900,
            1,
            [0; 32],
            vec![1],
        )));
        assert_prefix(&error, RouteContract);
        assert_eq!(before, observe(&service));
    }

    // A validly-constructed routing shard can never hold an inbound delivery edge that
    // targets a remote participant (`RuntimeFilterRoutingShard::new` rejects it), so the
    // non-local target is exercised through the dispatch error mapping instead: the
    // Router's `InboundTargetMismatch` classifies as a route contract.
    #[test]
    fn inbound_consumer_dispatch_maps_non_local_target_to_route_contract() {
        let mapped = map_route_error(RuntimeFilterRouteContractError::InboundTargetMismatch {
            channel: ChannelId::new(CHANNEL),
            edge: RouteEdgeId::new(CONSUMER_ROUTE),
            local_participant: RuntimeFilterParticipantId::new(3),
        });
        assert_eq!(mapped.kind(), RouteContract);
        assert_eq!(RouteContract.prefix(), "[route-contract]");
        assert!(mapped.to_string().contains("[route-contract]"));
    }

    #[test]
    fn inbound_consumer_dispatch_forbidden_envelope_kinds_are_rejected() {
        let service = service();
        install(&service, membership_channel(4096));
        let before = observe(&service);

        // Contribution / ProducerClosed carry a contribution route identity and are
        // rejected at the route-identity gate (they are the producer direction).
        let contribution_route = || {
            RuntimeFilterRouteIdentity::contribution(
                ContributionRouteIdentity::try_new(
                    BindingId::new(PRODUCER_BINDING),
                    producer_finst(),
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                )
                .unwrap(),
            )
        };
        let contribution = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::Contribution,
            QID,
            ChannelId::new(CHANNEL),
            DeploymentEpoch::new(EPOCH),
            contribution_route(),
            Some(ProducerOpenMetadata::try_new(1).unwrap()),
            None,
            &[0; 32],
            vec![1],
        )
        .unwrap();
        assert_prefix(
            &err(service.dispatch_inbound_consumer(contribution)),
            RouteContract,
        );

        let producer_closed = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            QID,
            ChannelId::new(CHANNEL),
            DeploymentEpoch::new(EPOCH),
            contribution_route(),
            Some(ProducerOpenMetadata::try_new(1).unwrap()),
            None,
            &[0; 32],
            Vec::new(),
        )
        .unwrap();
        assert_prefix(
            &err(service.dispatch_inbound_consumer(producer_closed)),
            RouteContract,
        );

        // Ack carries a delivery route identity (so it reaches the Router) but is not a
        // delivery kind: `authorize_delivery` rejects it as a route contract (M3).
        let ack = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::Ack,
            QID,
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
        .unwrap();
        assert_prefix(&err(service.dispatch_inbound_consumer(ack)), RouteContract);

        assert_eq!(before, observe(&service));
    }

    #[test]
    fn inbound_consumer_dispatch_wrong_profile_digest_is_rejected() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (correct, payload) = encode_bundle(&bundle, &profile);
        let wrong = [0x5A_u8; 32];
        assert_ne!(wrong, correct);

        let before = observe(&service);
        let error =
            err(service.dispatch_inbound_consumer(artifact_env(CONSUMER_ROUTE, 1, wrong, payload)));
        assert_prefix(&error, CodecContract);
        assert_eq!(before, observe(&service));
    }

    #[test]
    fn inbound_consumer_dispatch_noncanonical_payload_is_rejected() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = two_artifact_membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);
        // Swapping the two artifact records yields a frame that decodes (both leaves are
        // valid) but re-encodes to the canonical sorted order, so the codec's canonical
        // re-encode equality check must reject it.
        let swapped = swap_first_two_records(&payload);
        assert_ne!(swapped, payload);

        let before = observe(&service);
        let error = err(service.dispatch_inbound_consumer(artifact_env(
            CONSUMER_ROUTE,
            1,
            digest,
            swapped,
        )));
        assert_prefix(&error, CodecContract);
        assert_eq!(before, observe(&service));
    }

    #[test]
    fn inbound_consumer_dispatch_wire_oversize_is_rejected() {
        // Install a channel whose artifact byte budget is one, so its wire ceiling is a
        // bare frame header — smaller than any real bundle frame.
        let service = service();
        install(&service, membership_channel(1));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);
        assert!(payload.len() > 1);

        let before = observe(&service);
        let error = err(service.dispatch_inbound_consumer(artifact_env(
            CONSUMER_ROUTE,
            1,
            digest,
            payload,
        )));
        assert_prefix(&error, CodecContract);
        assert_eq!(before, observe(&service));
    }

    #[test]
    fn inbound_consumer_dispatch_error_taxonomy_prefixes_are_stable() {
        for (kind, prefix) in [
            (DeploymentUnavailable, "[deployment-unavailable]"),
            (StaleEpoch, "[stale-epoch]"),
            (RouteContract, "[route-contract]"),
            (CodecContract, "[codec-contract]"),
            (ServiceUnavailable, "[service-unavailable]"),
            (ResourceLimit, "[resource-limit]"),
        ] {
            assert_eq!(kind.prefix(), prefix);
        }
        // The codec-error mapping always lands under the codec-contract prefix.
        assert_eq!(
            map_codec_error(
                crate::runtime_filter::codec::artifact::ArtifactWireCodecError::Malformed
            )
            .kind(),
            CodecContract,
        );
    }

    // ==========================================================================================
    // RFD-4/M2C Task 5 Part A: loopback-equals-remote equivalence (umbrella §10).
    //
    // The SAME materialized artifact must land logically equal whether it reaches a
    // subscription through the in-process loopback leg of the outbound delivery bridge
    // (`deliver_artifact`, no wire hop) or through a fake-remote encode -> inbound decode
    // (`encode_artifact_bundle` -> `dispatch_inbound_consumer`). Each repr is proven on two
    // isolated services installed from the identical channel so the two legs never share
    // subscription state.
    // ==========================================================================================

    // A loopback-only delivery scope must never reach the remote sink; if the Router ever
    // classified a loopback edge as remote this panics rather than silently wire-encoding.
    struct NoopRemoteSink;
    impl RuntimeFilterEnvelopeSink for NoopRemoteSink {
        fn try_send(
            &self,
            _route: RuntimeFilterRemoteRoute,
            _envelope: crate::runtime_filter::port::transport::RuntimeFilterTransportEnvelope,
        ) -> SinkSubmitOutcome {
            panic!("a loopback-only delivery scope must not reach the remote sink");
        }

        fn try_recv_completion(&self) -> Option<SinkCompletion> {
            None
        }

        fn shutdown(&self) {}
    }

    fn bitset_profile() -> ConsumerArtifactProfile {
        ConsumerArtifactProfile::new(
            BTreeSet::from([
                ArtifactKind::ValueSet,
                ArtifactKind::Bitset,
                ArtifactKind::EmptyDomain,
            ]),
            None,
        )
        .unwrap()
    }

    fn bloom_profile() -> ConsumerArtifactProfile {
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let contract = BloomHashContract::new(&schema, MaterializationPolicy::for_test())
            .unwrap()
            .digest();
        ConsumerArtifactProfile::new(
            BTreeSet::from([
                ArtifactKind::ValueSet,
                ArtifactKind::Bitset,
                ArtifactKind::Bloom,
                ArtifactKind::EmptyDomain,
            ]),
            Some(contract),
        )
        .unwrap()
    }

    // How a delivered artifact is observed from its install-frozen subscription.
    #[derive(Clone, Copy)]
    enum DeliveryObserve {
        Blocking { binding: u32, finst: UniqueId },
        Live { binding: u32, finst: UniqueId },
    }

    fn observe_delivered(
        service: &RuntimeFilterService,
        observe: DeliveryObserve,
    ) -> Arc<ArtifactBundle> {
        match observe {
            DeliveryObserve::Blocking { binding, finst } => service
                .subscribe_blocking(BindingId::new(binding), finst)
                .unwrap()
                .snapshot()
                .expect("the blocking subscription must retain the delivered artifact"),
            DeliveryObserve::Live { binding, finst } => service
                .subscribe(
                    BindingId::new(binding),
                    finst,
                    SubscriptionKind::NonBlockingLive,
                )
                .unwrap()
                .into_live()
                .unwrap()
                .snapshot()
                .expect("the live subscription must retain the delivered artifact"),
        }
    }

    fn assert_bundles_logically_equal(left: &ArtifactBundle, right: &ArtifactBundle) {
        assert_eq!(left.canonical_digest(), right.canonical_digest());
        assert_eq!(left.channel_id(), right.channel_id());
        assert_eq!(left.version(), right.version());
        assert_eq!(left.profile_id(), right.profile_id());
        assert_eq!(left.artifacts().len(), right.artifacts().len());
        for ((left_kind, left_artifact), (right_kind, right_artifact)) in
            left.artifacts().iter().zip(right.artifacts())
        {
            assert_eq!(
                left_kind, right_kind,
                "physical repr must match on both legs"
            );
            assert_eq!(
                left_artifact.canonical_bytes(),
                right_artifact.canonical_bytes(),
                "membership set / range bound bytes must match on both legs"
            );
            assert_eq!(
                left_artifact.schema_digest(),
                right_artifact.schema_digest()
            );
            assert_eq!(left_artifact.version(), right_artifact.version());
        }
    }

    // Core acceptance: deliver `bundle` to `route_edge` on two isolated services, once via
    // the loopback leg and once via the fake-remote wire hop, and prove both subscriptions
    // observe the logically-equal artifact.
    fn assert_loopback_equals_remote(
        channel: impl Fn() -> RuntimeFilterChannelDeployment,
        profile: &ConsumerArtifactProfile,
        bundle: &Arc<ArtifactBundle>,
        route_edge: u32,
        observe: DeliveryObserve,
    ) {
        // (a) loopback: the outbound bridge routes the in-memory bundle straight into the
        //     local subscription with no wire encode.
        let loopback_service = service();
        install_with_loopback_delivery(&loopback_service, channel());
        // The panicking sink asserts the loopback-only scope never reaches the remote
        // transport, even though nothing is expected to be transmitted.
        loopback_service.set_remote_sink_for_test(Arc::new(NoopRemoteSink));
        let decision = loopback_service
            .deliver_artifact(
                ChannelId::new(CHANNEL),
                profile,
                vec![RouteEdgeId::new(route_edge)],
                ArtifactDeliveryOutcome::Published(bundle.clone()),
            )
            .expect("loopback delivery must route into the local subscription");
        assert!(
            decision.remote_routes().is_empty(),
            "a loopback-only scope must not emit a remote frame"
        );
        assert_eq!(
            decision.loopback_route_edge_ids(),
            &[RouteEdgeId::new(route_edge)]
        );
        let via_loopback = observe_delivered(&loopback_service, observe);

        // (b) remote: encode the identical bundle to a wire frame and feed it back through
        //     the inbound consumer dispatch (decode -> deliver).
        let remote_service = service();
        install(&remote_service, channel());
        let (digest, payload) = encode_bundle(bundle, profile);
        assert_eq!(
            remote_service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::Artifact,
                    CHANNEL,
                    EPOCH,
                    route_edge,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );
        let via_remote = observe_delivered(&remote_service, observe);

        // Both legs deliver the same logical artifact as the source materialization.
        assert_eq!(via_loopback.canonical_digest(), bundle.canonical_digest());
        assert_eq!(via_remote.canonical_digest(), bundle.canonical_digest());
        assert_bundles_logically_equal(&via_loopback, &via_remote);
    }

    #[test]
    fn consumer_delivery_loopback_equals_remote_membership_value_set() {
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::ValueSet);
        assert_loopback_equals_remote(
            || membership_channel(4096),
            &profile,
            &bundle,
            CONSUMER_ROUTE,
            DeliveryObserve::Blocking {
                binding: CONSUMER_BINDING,
                finst: consumer_finst(),
            },
        );
    }

    #[test]
    fn consumer_delivery_loopback_equals_remote_membership_empty_domain() {
        let profile = membership_profile();
        let bundle = membership_bundle_values(&profile, MembershipValues::int64([]), 4096);
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::EmptyDomain);
        assert_loopback_equals_remote(
            || membership_channel(4096),
            &profile,
            &bundle,
            CONSUMER_ROUTE,
            DeliveryObserve::Blocking {
                binding: CONSUMER_BINDING,
                finst: consumer_finst(),
            },
        );
    }

    #[test]
    fn consumer_delivery_loopback_equals_remote_membership_two_artifacts() {
        let profile = membership_profile();
        let bundle = two_artifact_membership_bundle(&profile);
        assert_eq!(bundle.artifacts().len(), 2);
        assert_loopback_equals_remote(
            || membership_channel(4096),
            &profile,
            &bundle,
            CONSUMER_ROUTE,
            DeliveryObserve::Blocking {
                binding: CONSUMER_BINDING,
                finst: consumer_finst(),
            },
        );
    }

    #[test]
    fn consumer_delivery_loopback_equals_remote_membership_bitset() {
        let profile = bitset_profile();
        let bundle = membership_bundle_values(&profile, MembershipValues::int64(100..164), 4096);
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::Bitset);
        assert_loopback_equals_remote(
            || membership_channel_profiled(4096, bitset_profile()),
            &profile,
            &bundle,
            CONSUMER_ROUTE,
            DeliveryObserve::Blocking {
                binding: CONSUMER_BINDING,
                finst: consumer_finst(),
            },
        );
    }

    #[test]
    fn consumer_delivery_loopback_equals_remote_membership_bloom() {
        let profile = bloom_profile();
        let bundle = membership_bundle_values(
            &profile,
            MembershipValues::int64((0..128).map(|value| value * 1_000_000)),
            512,
        );
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::Bloom);
        assert_loopback_equals_remote(
            || membership_channel_profiled(512, bloom_profile()),
            &profile,
            &bundle,
            CONSUMER_ROUTE,
            DeliveryObserve::Blocking {
                binding: CONSUMER_BINDING,
                finst: consumer_finst(),
            },
        );
    }

    #[test]
    fn consumer_delivery_loopback_equals_remote_range() {
        let profile = range_profile();
        let bundle = range_bundle(&profile);
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::Range);
        assert_loopback_equals_remote(
            ordered_channel,
            &profile,
            &bundle,
            RANGE_ROUTE,
            DeliveryObserve::Live {
                binding: RANGE_CONSUMER_BINDING,
                finst: range_consumer_finst(),
            },
        );
    }

    // Canonical-order record swap, mirroring the artifact codec's own test helper.
    fn swap_first_two_records(payload: &[u8]) -> Vec<u8> {
        // HEADER(56) + channel_id(4) + schema_digest(32) + artifact_count(2).
        let records_start = 56 + 4 + 32 + 2;
        let first_kind = records_start;
        let first_len =
            u64::from_be_bytes(payload[first_kind + 1..first_kind + 9].try_into().unwrap())
                as usize;
        let first_end = first_kind + 9 + first_len;
        let second_kind = first_end;
        let second_len = u64::from_be_bytes(
            payload[second_kind + 1..second_kind + 9]
                .try_into()
                .unwrap(),
        ) as usize;
        let second_end = second_kind + 9 + second_len;

        let mut swapped = Vec::with_capacity(payload.len());
        swapped.extend_from_slice(&payload[..records_start]);
        swapped.extend_from_slice(&payload[second_kind..second_end]);
        swapped.extend_from_slice(&payload[first_kind..first_end]);
        swapped.extend_from_slice(&payload[second_end..]);
        swapped
    }

    // ==========================================================================================
    // RFD-4/M3 Task 3: unified ingress dedupe + (query, epoch) tombstone (consumer side).
    // ==========================================================================================

    // The consumer transport-identity gate absorbs a wire retry keyed on
    // (route edge + transport sequence), independent of the logical (route, version)
    // gate. Delivering an artifact and then an Unavailable at the SAME transport identity
    // (they carry DIFFERENT logical versions, so the logical gate alone would not catch
    // it) proves the transport gate: the second is Duplicate and never re-delivered.
    #[test]
    fn ingress_dedupe_consumer_transport_retry_is_duplicate_not_redelivered() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);
        let (un_digest, un_payload) = encode_unavailable(
            UnavailableReason::ProducerFailed,
            ArtifactDecodeExpectation::new(&profile),
            ROOMY,
        )
        .unwrap()
        .into_parts();

        assert_eq!(
            service
                .dispatch_inbound_consumer(artifact_env(CONSUMER_ROUTE, 1, digest, payload))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );
        let seeded = observe(&service);
        assert_eq!(seeded.delivery_calls, 1);
        assert_eq!(seeded.snapshot_digest, Some(bundle.canonical_digest()));

        // Same route edge + same transport sequence (1) as the artifact, but an Unavailable
        // payload whose logical version (0) differs from the artifact's: the logical gate
        // alone would deliver it, but the transport-identity gate absorbs it first.
        assert_eq!(
            service
                .dispatch_inbound_consumer(delivery_env(
                    RuntimeFilterEnvelopeKind::Unavailable,
                    CHANNEL,
                    EPOCH,
                    CONSUMER_ROUTE,
                    1,
                    un_digest,
                    un_payload,
                ))
                .unwrap(),
            InboundConsumerDispatchOutcome::Duplicate,
        );
        let after = observe(&service);
        assert_eq!(
            after.delivery_calls, 1,
            "a transport retry must not re-deliver into the subscription"
        );
        assert_eq!(after.snapshot_digest, seeded.snapshot_digest);
    }

    // TRAP A: the absorbed logical (route edge, version) gate must survive the unified
    // dedupe. Re-delivering the SAME artifact version via a DIFFERENT transport sequence
    // (so the transport gate does NOT catch it) must still be Duplicate — a transport-only
    // dedupe would wrongly re-deliver the same logical version.
    #[test]
    fn ingress_dedupe_consumer_logical_version_replay_stays_duplicate() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        assert_eq!(
            service
                .dispatch_inbound_consumer(artifact_env(CONSUMER_ROUTE, 1, digest, payload.clone()))
                .unwrap(),
            InboundConsumerDispatchOutcome::Accepted,
        );
        let seeded = observe(&service);
        assert_eq!(seeded.delivery_calls, 1);

        // Same (route edge, version) but a distinct transport sequence (2): the transport
        // identity is fresh, yet the logical gate still absorbs the replay.
        assert_eq!(
            service
                .dispatch_inbound_consumer(artifact_env(CONSUMER_ROUTE, 2, digest, payload))
                .unwrap(),
            InboundConsumerDispatchOutcome::Duplicate,
        );
        assert_eq!(
            observe(&service).delivery_calls,
            1,
            "a logical version replay must not re-deliver into the subscription"
        );
    }

    // (query, epoch) tombstone: after cancel/completion, a late delivery for an epoch
    // OLDER than the retired epoch is reported as a stale epoch (not masked as a bare
    // service-unavailable), and the cancelled registry is never revived (M2B3
    // lookup-only).
    #[test]
    fn ingress_dedupe_consumer_tombstone_stale_epoch_after_cancel_is_rejected() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);
        service.cancel();

        let error = err(service.dispatch_inbound_consumer(delivery_env(
            RuntimeFilterEnvelopeKind::Artifact,
            CHANNEL,
            EPOCH - 1,
            CONSUMER_ROUTE,
            1,
            digest,
            payload,
        )));
        assert_prefix(&error, StaleEpoch);
        assert!(
            service.registry.active_installation().is_none(),
            "the tombstone must not revive the cancelled registry"
        );
    }

    // A late delivery for the retired epoch itself is rejected as service-unavailable
    // without reviving the cancelled registry or delivering into the subscription.
    #[test]
    fn ingress_dedupe_consumer_tombstone_retired_epoch_after_cancel_does_not_revive() {
        let service = service();
        install(&service, membership_channel(4096));
        let profile = membership_profile();
        let bundle = membership_bundle(&profile);
        let (digest, payload) = encode_bundle(&bundle, &profile);

        let installed = service.registry.active_installation().unwrap();
        let handle = service
            .subscribe_blocking(BindingId::new(CONSUMER_BINDING), consumer_finst())
            .unwrap();
        service.cancel();
        let after_cancel_calls =
            installed.subscription_delivery_call_count(BindingId::new(CONSUMER_BINDING));

        let error = err(service.dispatch_inbound_consumer(artifact_env(
            CONSUMER_ROUTE,
            1,
            digest,
            payload,
        )));
        assert_prefix(&error, ServiceUnavailable);
        assert!(
            service.registry.active_installation().is_none(),
            "the tombstone must not revive the cancelled registry"
        );
        assert_eq!(
            installed.subscription_delivery_call_count(BindingId::new(CONSUMER_BINDING)),
            after_cancel_calls,
            "a tombstoned dispatch must not deliver into the subscription"
        );
        let _ = handle;
    }
}
