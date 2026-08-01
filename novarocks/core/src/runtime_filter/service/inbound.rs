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

use std::error::Error;
use std::fmt;

use sha2::{Digest, Sha256};

use crate::runtime_filter::codec::contribution::{
    ContributionCodecExpectation, RuntimeFilterContribution, decode_contribution,
    semantic_contribution_bytes,
};
use crate::runtime_filter::codec::producer::decode_producer_failure;
use crate::runtime_filter::port::identity::ProducerStreamId;
use crate::runtime_filter::port::producer::{
    ProducerHandle, RuntimeContractViolation, RuntimeContractViolationKind, SubmitOutcome,
};
use crate::runtime_filter::port::routing::RuntimeFilterRouteContractError;
use crate::runtime_filter::port::transport::{RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind};

use super::dedupe::{ContributionAdmission, TombstoneVerdict};
use super::registry::{DispatchAdmission, InboundProducerContract};
use super::{OpenedProducer, RuntimeFilterService};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundProducerDispatchOutcome {
    Accepted,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InboundProducerDispatchErrorKind {
    DeploymentUnavailable,
    StaleEpoch,
    RouteContract,
    CodecContract,
    ProducerContract,
    ServiceUnavailable,
    ResourceLimit,
}

impl InboundProducerDispatchErrorKind {
    pub(crate) const fn prefix(self) -> &'static str {
        match self {
            Self::DeploymentUnavailable => "[deployment-unavailable]",
            Self::StaleEpoch => "[stale-epoch]",
            Self::RouteContract => "[route-contract]",
            Self::CodecContract => "[codec-contract]",
            Self::ProducerContract => "[producer-contract]",
            Self::ServiceUnavailable => "[service-unavailable]",
            Self::ResourceLimit => "[resource-limit]",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InboundProducerDispatchError {
    kind: InboundProducerDispatchErrorKind,
    detail: String,
}

impl InboundProducerDispatchError {
    pub(crate) fn new(kind: InboundProducerDispatchErrorKind, detail: impl Into<String>) -> Self {
        let detail = detail.into();
        assert!(
            !detail.is_empty(),
            "inbound rejection detail must not be empty"
        );
        Self { kind, detail }
    }

    pub(crate) const fn kind(&self) -> InboundProducerDispatchErrorKind {
        self.kind
    }
}

impl fmt::Display for InboundProducerDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "runtime filter ingress rejected {}: {}",
            self.kind.prefix(),
            self.detail
        )
    }
}

impl Error for InboundProducerDispatchError {}

impl RuntimeFilterService {
    pub(crate) fn dispatch_inbound_producer(
        &self,
        envelope: RuntimeFilterEnvelope,
    ) -> Result<InboundProducerDispatchOutcome, InboundProducerDispatchError> {
        let contribution_route = envelope.route_identity().as_contribution();
        let producer_instance_route = envelope.route_identity().as_producer_instance();
        let (producer_binding_id, fragment_instance_id) = match envelope.kind() {
            RuntimeFilterEnvelopeKind::Contribution | RuntimeFilterEnvelopeKind::ProducerClosed => {
                let route = contribution_route.ok_or_else(|| {
                    ingress_error(
                        InboundProducerDispatchErrorKind::RouteContract,
                        "contribution or close envelope requires contribution identity",
                    )
                })?;
                (route.producer_binding_id(), route.fragment_instance_id())
            }
            RuntimeFilterEnvelopeKind::ProducerUnavailable => {
                let route = producer_instance_route.ok_or_else(|| {
                    ingress_error(
                        InboundProducerDispatchErrorKind::RouteContract,
                        "producer-unavailable envelope requires producer-instance identity",
                    )
                })?;
                (route.producer_binding_id(), route.fragment_instance_id())
            }
            _ => {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::RouteContract,
                    "envelope kind is not valid for producer ingress",
                ));
            }
        };

        // (query, epoch) tombstone: a late contribution for a retired/stale epoch is
        // rejected without rebuilding context (M2B3 lookup-only). Consulted before
        // admission so a stale epoch after cancel is reported as StaleEpoch, not masked as
        // a bare service-unavailable.
        match self.dedupe.tombstone_verdict(envelope.deployment_epoch()) {
            TombstoneVerdict::Live => {}
            TombstoneVerdict::Retired => {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::ServiceUnavailable,
                    "runtime filter query/epoch is retired",
                ));
            }
            TombstoneVerdict::StaleEpoch => {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::StaleEpoch,
                    "runtime filter envelope epoch is older than a retired epoch",
                ));
            }
        }

        let operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = match self.registry.dispatch_admission() {
            DispatchAdmission::Active(installed) => installed,
            DispatchAdmission::Cancelled => {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::ServiceUnavailable,
                    "runtime filter service is cancelled or shut down",
                ));
            }
            DispatchAdmission::Absent => {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::DeploymentUnavailable,
                    "runtime filter deployment is not active",
                ));
            }
        };
        installed
            .role_router()
            .authorize_contribution(
                envelope.deployment_epoch(),
                envelope.channel_id(),
                producer_binding_id,
                fragment_instance_id,
                envelope.kind(),
            )
            .map_err(map_route_error)?;
        let producer = installed.producer(producer_binding_id).ok_or_else(|| {
            ingress_error(
                InboundProducerDispatchErrorKind::RouteContract,
                "producer binding is not installed",
            )
        })?;
        if producer.channel_id() != envelope.channel_id() {
            return Err(ingress_error(
                InboundProducerDispatchErrorKind::RouteContract,
                "producer binding belongs to another channel",
            ));
        }

        let contract = producer.inbound_contract();
        if envelope.kind() == RuntimeFilterEnvelopeKind::ProducerUnavailable {
            if envelope.schema_digest() != &contract.schema_digest() {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::CodecContract,
                    "producer-unavailable digest does not match the installed producer contract",
                ));
            }
            let reason = decode_producer_failure(envelope.payload()).map_err(|error| {
                ingress_error(
                    InboundProducerDispatchErrorKind::CodecContract,
                    error.to_string(),
                )
            })?;
            let route = producer_instance_route.expect("kind and identity were validated together");
            match self.dedupe.admit_producer_instance(
                envelope.channel_id(),
                route,
                envelope_fingerprint(&envelope),
            ) {
                ContributionAdmission::DuplicateRetry => {
                    drop(operation);
                    return Ok(InboundProducerDispatchOutcome::Duplicate);
                }
                ContributionAdmission::Conflict => {
                    drop(operation);
                    return Err(ingress_error(
                        InboundProducerDispatchErrorKind::ProducerContract,
                        "producer-unavailable identity was replayed with different content",
                    ));
                }
                ContributionAdmission::ResourceLimit => {
                    drop(operation);
                    return Err(ingress_error(
                        InboundProducerDispatchErrorKind::ResourceLimit,
                        "runtime filter producer-instance dedupe set is at its per-channel resource ceiling",
                    ));
                }
                ContributionAdmission::Fresh => {}
            }
            let action = producer
                .channel
                .fail_instance(producer_binding_id, fragment_instance_id, reason)
                .map_err(map_producer_error)?;
            let outcome = action.outcome();
            drop(operation);
            self.dispatcher
                .dispatch(envelope.channel_id(), action)
                .map_err(map_producer_error)?;
            return Ok(dispatch_outcome(outcome));
        }

        let route = contribution_route.expect("contribution kinds require contribution identity");

        let local_partition_count = envelope
            .producer_open()
            .ok_or_else(|| {
                ingress_error(
                    InboundProducerDispatchErrorKind::RouteContract,
                    "producer envelope is missing open metadata",
                )
            })?
            .local_partition_count()
            .get();
        let contribution = match envelope.kind() {
            RuntimeFilterEnvelopeKind::Contribution => {
                let expectation = contribution_expectation(
                    contract,
                    route.producer_binding_id(),
                    route.fragment_instance_id(),
                    route.partition_id(),
                    route.sequence(),
                );
                let contribution = decode_contribution(
                    envelope.payload(),
                    envelope.schema_digest(),
                    expectation,
                    contract.limits().max_encoded_bytes(),
                )
                .map_err(map_codec_error)?;
                let semantic_bytes =
                    semantic_contribution_bytes(&contribution).map_err(map_codec_error)?;
                if semantic_bytes > contract.limits().max_contribution_bytes() {
                    return Err(ingress_error(
                        InboundProducerDispatchErrorKind::CodecContract,
                        "contribution exceeds the installed semantic byte budget",
                    ));
                }
                Some(contribution)
            }
            RuntimeFilterEnvelopeKind::ProducerClosed => {
                if !envelope.payload().is_empty() {
                    return Err(ingress_error(
                        InboundProducerDispatchErrorKind::CodecContract,
                        "producer-close envelope must not carry a payload",
                    ));
                }
                if envelope.schema_digest() != &contract.schema_digest() {
                    return Err(ingress_error(
                        InboundProducerDispatchErrorKind::CodecContract,
                        "producer-close digest does not match the installed producer contract",
                    ));
                }
                None
            }
            _ => {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::RouteContract,
                    "envelope kind is not valid for producer ingress",
                ));
            }
        };

        producer
            .channel
            .preflight_remote_open(
                route.producer_binding_id(),
                route.fragment_instance_id(),
                local_partition_count,
                route.partition_id(),
            )
            .map_err(map_producer_error)?;

        // Transport-identity dedupe: a byte-identical at-least-once retry of this
        // contribution identity is absorbed as `Duplicate` before any Core mutation. A
        // same-identity arrival carrying different content is NOT a valid retry -- it
        // flows to the Core, whose content-aware sequence dedupe rejects it as a
        // conflicting replay. The content witness is the encoded contribution payload;
        // for a `ProducerClosed` (empty payload) it collapses to the empty-payload digest,
        // so its retries are absorbed too.
        let content_digest = envelope_fingerprint(&envelope);
        match self
            .dedupe
            .admit_contribution(envelope.channel_id(), route, content_digest)
        {
            ContributionAdmission::Fresh => {}
            ContributionAdmission::Conflict => {
                drop(operation);
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::ProducerContract,
                    "producer contribution identity was replayed with different content",
                ));
            }
            ContributionAdmission::DuplicateRetry => {
                drop(operation);
                return Ok(InboundProducerDispatchOutcome::Duplicate);
            }
            ContributionAdmission::ResourceLimit => {
                // A genuinely-new contribution identity beyond this channel's self-owned
                // dedupe ceiling: an explicit first-class resource rejection, not a
                // silent drop. Reject before any Core mutation.
                drop(operation);
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::ResourceLimit,
                    "runtime filter producer dedupe set is at its per-channel resource ceiling",
                ));
            }
        }

        let OpenedProducer { handle, outcome } = self
            .open_inbound_core_locked(
                &installed,
                route.producer_binding_id(),
                route.fragment_instance_id(),
                local_partition_count,
                contract.port_kind(),
            )
            .map_err(map_producer_error)?;
        #[cfg(test)]
        self.fire_after_inbound_open_admission();
        if outcome == SubmitOutcome::TerminalNoop {
            drop(operation);
            return Ok(InboundProducerDispatchOutcome::Accepted);
        }
        let partition_id = route.partition_id();
        let sequence = route.sequence();
        // Never hold `operation` across the typed submit/close: cancel/shutdown
        // must be free to linearize against the Channel lock, not this mutex.
        drop(operation);
        #[cfg(test)]
        self.fire_before_inbound_typed_dispatch();

        let outcome = match (handle, contribution) {
            (
                ProducerHandle::Membership(handle),
                Some(RuntimeFilterContribution::Membership(delta)),
            ) => handle.submit(partition_id, sequence, delta),
            (
                ProducerHandle::OrderedBound(handle),
                Some(RuntimeFilterContribution::OrderedBound(update)),
            ) => handle.submit_bound(partition_id, sequence, update),
            (
                ProducerHandle::TopKSummary(handle),
                Some(RuntimeFilterContribution::TopKSummary(summary)),
            ) => handle.submit_summary(partition_id, sequence, summary),
            (
                ProducerHandle::FinalDomain(handle),
                Some(RuntimeFilterContribution::FinalDomain(shard)),
            ) => handle.complete(partition_id, sequence, shard),
            (ProducerHandle::Membership(handle), None) => {
                handle.close_partition(partition_id, sequence)
            }
            (ProducerHandle::OrderedBound(handle), None) => {
                handle.close_partition(partition_id, sequence)
            }
            (ProducerHandle::TopKSummary(handle), None) => {
                handle.close_partition(partition_id, sequence)
            }
            (ProducerHandle::FinalDomain(handle), None) => {
                handle.close_partition(partition_id, sequence)
            }
            _ => {
                return Err(ingress_error(
                    InboundProducerDispatchErrorKind::ProducerContract,
                    "decoded contribution does not match the installed producer port",
                ));
            }
        }
        .map_err(map_producer_error)?;
        Ok(dispatch_outcome(outcome))
    }
}

fn envelope_fingerprint(envelope: &RuntimeFilterEnvelope) -> [u8; 32] {
    let kind_tag = match envelope.kind() {
        RuntimeFilterEnvelopeKind::Contribution => 1,
        RuntimeFilterEnvelopeKind::Artifact => 2,
        RuntimeFilterEnvelopeKind::ProducerClosed => 3,
        RuntimeFilterEnvelopeKind::ProducerUnavailable => 4,
        RuntimeFilterEnvelopeKind::Unavailable => 5,
        RuntimeFilterEnvelopeKind::Ack => 6,
        RuntimeFilterEnvelopeKind::CompletedWithoutArtifact => 7,
        RuntimeFilterEnvelopeKind::DegradedLogical => 8,
        RuntimeFilterEnvelopeKind::FinalArtifact => 9,
    };
    let mut digest = Sha256::new();
    digest.update([kind_tag]);
    match envelope.producer_open() {
        Some(open) => {
            digest.update([1]);
            digest.update(open.local_partition_count().get().to_le_bytes());
        }
        None => digest.update([0]),
    }
    digest.update(envelope.schema_digest());
    digest.update(envelope.payload());
    digest.finalize().into()
}

fn contribution_expectation<'a>(
    contract: &'a InboundProducerContract,
    binding_id: crate::runtime_filter::model::contract::BindingId,
    fragment_instance_id: crate::common::types::UniqueId,
    partition_id: crate::runtime_filter::port::identity::PartitionId,
    sequence: crate::runtime_filter::port::identity::ProducerSequence,
) -> ContributionCodecExpectation<'a> {
    match contract {
        InboundProducerContract::Membership { schema, .. } => {
            ContributionCodecExpectation::Membership(schema)
        }
        InboundProducerContract::OrderedBound { contract, .. } => {
            ContributionCodecExpectation::OrderedBound(contract)
        }
        InboundProducerContract::TopKSummary { contract, .. } => {
            ContributionCodecExpectation::TopKSummary(contract)
        }
        InboundProducerContract::FinalDomain { contract, .. } => {
            ContributionCodecExpectation::FinalDomain {
                contract,
                stream: ProducerStreamId::new(binding_id, fragment_instance_id, partition_id),
                sequence,
            }
        }
    }
}

fn ingress_error(
    kind: InboundProducerDispatchErrorKind,
    detail: impl Into<String>,
) -> InboundProducerDispatchError {
    InboundProducerDispatchError::new(kind, detail)
}

fn map_route_error(error: RuntimeFilterRouteContractError) -> InboundProducerDispatchError {
    let kind = if matches!(error, RuntimeFilterRouteContractError::StaleEpoch { .. }) {
        InboundProducerDispatchErrorKind::StaleEpoch
    } else {
        InboundProducerDispatchErrorKind::RouteContract
    };
    ingress_error(kind, error.to_string())
}

fn map_codec_error(
    error: crate::runtime_filter::codec::contribution::ContributionCodecError,
) -> InboundProducerDispatchError {
    ingress_error(
        InboundProducerDispatchErrorKind::CodecContract,
        error.to_string(),
    )
}

fn map_producer_error(error: RuntimeContractViolation) -> InboundProducerDispatchError {
    let kind = if error.kind() == RuntimeContractViolationKind::ServiceUnavailable {
        InboundProducerDispatchErrorKind::ServiceUnavailable
    } else {
        InboundProducerDispatchErrorKind::ProducerContract
    };
    ingress_error(kind, error.to_string())
}

const fn dispatch_outcome(outcome: SubmitOutcome) -> InboundProducerDispatchOutcome {
    if matches!(outcome, SubmitOutcome::Duplicate) {
        InboundProducerDispatchOutcome::Duplicate
    } else {
        InboundProducerDispatchOutcome::Accepted
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime::query_context::{QueryContextManager, QueryId};
    use crate::runtime_filter::codec::contribution::{
        ContributionCodecExpectation, RuntimeFilterContribution, encode_contribution,
        semantic_contribution_bytes,
    };
    use crate::runtime_filter::codec::producer::encode_producer_failure;
    use crate::runtime_filter::core::channel::{ProducerIngressCoreSnapshot, RuntimeFilterChannel};
    use crate::runtime_filter::core::coverage::CoverageProgress;
    use crate::runtime_filter::core::state::TerminalProgress;
    use crate::runtime_filter::deployment::extension::RuntimeFilterDeploymentExtension;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
        ConsumerActivation, ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder,
        NullSemantics, OrderContract, OrderKeyContract, ReductionRequirement,
        RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
        SortDirection, TopKSummaryRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::artifact::{
        ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
    };
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::final_domain::{
        CollectingFinalDomainTestIssuer, CompletionFenceAuthority, FinalDomainShard,
        FinalDomainTestIssuerTransition, RuntimeCompletionFenceContract,
    };
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, PartitionId, ProducerSequence, ProducerStreamId, RouteEdgeId,
        RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::install::{
        ConsumerDeployment, MaterializationPolicy, ProducerDeployment,
        RuntimeFilterChannelDeployment, RuntimeFilterCoreBudget, RuntimeFilterParticipantInstall,
    };
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedScalar, OrderedTuple, RuntimeOrderContract,
        comparator_digest_for_test,
    };
    use crate::runtime_filter::port::producer::{
        InstallOutcome, ProducerFailureReason, RuntimeContractViolation,
        RuntimeContractViolationKind,
    };
    use crate::runtime_filter::port::routing::RuntimeFilterRouteContractError;
    use crate::runtime_filter::port::routing::RuntimeFilterRouteRole;
    use crate::runtime_filter::port::support::{
        MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::topk_summary::{RuntimeTopKSummaryContract, TopKSummary};
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, ProducerInstanceRouteIdentity, ProducerOpenMetadata,
        RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind, RuntimeFilterRouteIdentity,
    };
    use crate::runtime_filter::port::transport::{
        RuntimeFilterAcceptStatus, RuntimeFilterEnvelopeIngress,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};
    use crate::runtime_filter::service::registry::InboundProducerContract as InstalledInboundContract;
    use crate::runtime_filter::service::test_support::compiled_three_backend_all_of_plan;
    use crate::runtime_filter::service::tests::{
        inbound_loopback_install_for_test, ordered_update,
    };
    use crate::service::runtime_filter_envelope_ingress::query_scoped_runtime_filter_envelope_ingress_with_manager;

    use super::InboundProducerDispatchErrorKind::{
        CodecContract, DeploymentUnavailable, ProducerContract, RouteContract, ServiceUnavailable,
        StaleEpoch,
    };
    use super::{
        InboundProducerDispatchError, InboundProducerDispatchErrorKind,
        InboundProducerDispatchOutcome, RuntimeFilterService, map_producer_error, map_route_error,
    };

    // Loopback installs from `inbound_loopback_install_for_test` are fixed at epoch 9 and
    // participant 3; every envelope in this matrix must speak that deployment epoch.
    const EPOCH: u64 = 9;
    const CHANNEL: u32 = 1;
    const PRODUCER_BINDING: u32 = 1;
    const PRODUCER_B_BINDING: u32 = 3;
    const CONSUMER_BINDING: u32 = 2;
    const WITNESS: u32 = 11;
    const WITNESS_B: u32 = 12;
    // The final-domain fence contract is derived from the service query id during install, so the
    // service and the shard we encode must agree on it.
    const SERVICE_QID: UniqueId = UniqueId::new(7, 7);
    const PRODUCER_FINST: UniqueId = UniqueId::new(1, 2);
    const PRODUCER_B_FINST: UniqueId = UniqueId::new(1, 4);
    const CONSUMER_FINST: UniqueId = UniqueId::new(1, 3);
    // MAGIC(4) + version(2) + kind(1) + flags(1) + schema digest(32) + body length(8).
    const FRAME_HEADER_LEN: usize = 48;

    struct Clock;
    impl RuntimeFilterClock for Clock {
        fn now(&self) -> Instant {
            Instant::now()
        }
    }

    #[derive(Default)]
    struct RecordingEvents(Mutex<Vec<RuntimeFilterEvent>>);
    impl RuntimeFilterEventSink for RecordingEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0.lock().unwrap().push(event);
        }
    }
    impl RecordingEvents {
        fn len(&self) -> usize {
            self.0.lock().unwrap().len()
        }
    }

    struct Memory;
    impl RuntimeFilterMemoryAccount for Memory {
        fn try_consume(&self, _: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }
        fn release(&self, _: usize) {}
    }

    // --- service / install helpers ------------------------------------------------------------

    fn service(events: Arc<RecordingEvents>) -> RuntimeFilterService {
        RuntimeFilterService::new_with_dependencies(
            SERVICE_QID,
            Arc::new(Clock),
            events,
            Arc::new(Memory),
        )
    }

    fn install(
        deployment: RuntimeFilterChannelDeployment,
    ) -> (RuntimeFilterService, Arc<RecordingEvents>) {
        let events = Arc::new(RecordingEvents::default());
        let service = service(events.clone());
        assert_eq!(
            service
                .install(inbound_loopback_install_for_test(deployment))
                .unwrap(),
            InstallOutcome::Installed,
        );
        (service, events)
    }

    fn channel_of(service: &RuntimeFilterService, binding: u32) -> Arc<RuntimeFilterChannel> {
        service
            .registry
            .active_installation()
            .expect("service is installed")
            .producer(BindingId::new(binding))
            .expect("producer binding is installed")
            .channel
            .clone()
    }

    fn installed_digest(service: &RuntimeFilterService, binding: u32) -> [u8; 32] {
        service
            .registry
            .active_installation()
            .expect("service is installed")
            .producer(BindingId::new(binding))
            .expect("producer binding is installed")
            .inbound_contract()
            .schema_digest()
    }

    // --- deployment builders ------------------------------------------------------------------

    fn membership_consumer() -> ConsumerDeployment {
        ConsumerDeployment::new(
            ConsumerActivation::BlockingSnapshot,
            BTreeSet::from([ArtifactCapability::Membership]),
            BTreeSet::from([RouteEdgeId::new(40)]),
            BTreeSet::from([CONSUMER_FINST]),
        )
    }

    fn policy(max_contribution_bytes: u64) -> RuntimeFilterPolicyRequirement {
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes,
            max_artifact_bytes: 4096,
            deadline_ms: 1000,
            max_retries: 1,
        }
    }

    fn membership_deployment(max_contribution_bytes: u64) -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(WITNESS);
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
            policy(max_contribution_bytes),
            RuntimeFilterCoreBudget::new(1 << 20),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(PRODUCER_BINDING),
                ProducerDeployment::new(witness, BTreeSet::from([PRODUCER_FINST])),
            )]),
            BTreeMap::from([(BindingId::new(CONSUMER_BINDING), membership_consumer())]),
        )
    }

    // Two producers over an `AnyOf` terminal coverage: closing producer A alone drives the
    // channel terminal while producer B remains installed but never opened, which is the only
    // way for `open_producer` to answer a fresh producer with `TerminalNoop`.
    fn membership_anyof_deployment() -> RuntimeFilterChannelDeployment {
        let witness_a = CoverageWitnessId::new(WITNESS);
        let witness_b = CoverageWitnessId::new(WITNESS_B);
        let coverage = Coverage::AnyOf(vec![Coverage::Leaf(witness_a), Coverage::Leaf(witness_b)]);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(CHANNEL),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            coverage.clone(),
            coverage,
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            policy(4096),
            RuntimeFilterCoreBudget::new(1 << 20),
            MaterializationPolicy::for_test(),
            BTreeMap::from([
                (
                    BindingId::new(PRODUCER_BINDING),
                    ProducerDeployment::new(witness_a, BTreeSet::from([PRODUCER_FINST])),
                ),
                (
                    BindingId::new(PRODUCER_B_BINDING),
                    ProducerDeployment::new(witness_b, BTreeSet::from([PRODUCER_B_FINST])),
                ),
            ]),
            BTreeMap::from([(BindingId::new(CONSUMER_BINDING), membership_consumer())]),
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

    fn topk_contract() -> RuntimeTopKSummaryContract {
        RuntimeTopKSummaryContract::try_from_plan(
            &order_plan(),
            TopKSummaryRequirement::try_new(4).unwrap(),
        )
        .unwrap()
    }

    fn ordered_consumer() -> ConsumerDeployment {
        ConsumerDeployment::with_profile(
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            BTreeSet::from([ArtifactCapability::OrderedRange]),
            ConsumerArtifactProfile::new_ordered_range(order_contract().digest()).unwrap(),
            BTreeSet::from([RouteEdgeId::new(40)]),
            BTreeSet::from([CONSUMER_FINST]),
        )
    }

    fn ordered_deployment(max_contribution_bytes: u64) -> RuntimeFilterChannelDeployment {
        ordered_family_deployment(
            ReductionRequirement::TightenOrderedBound,
            ContributionKind::OrderedBoundUpdate,
            max_contribution_bytes,
        )
    }

    fn topk_deployment(max_contribution_bytes: u64) -> RuntimeFilterChannelDeployment {
        ordered_family_deployment(
            ReductionRequirement::MergeTopKSummary(TopKSummaryRequirement::try_new(4).unwrap()),
            ContributionKind::TopKSummary,
            max_contribution_bytes,
        )
    }

    fn ordered_family_deployment(
        reduction: ReductionRequirement,
        contribution_kind: ContributionKind,
        max_contribution_bytes: u64,
    ) -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(WITNESS);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(CHANNEL),
            RuntimeFilterLogicalDomain::OrderedBound(order_plan()),
            RuntimeFilterLifecycle::MonotonicUpdates,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            reduction,
            BTreeSet::from([contribution_kind, ContributionKind::ProducerClosed]),
            CompletionRequirement::ProducerClosed,
            policy(max_contribution_bytes),
            RuntimeFilterCoreBudget::new(1 << 20),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(PRODUCER_BINDING),
                ProducerDeployment::new(witness, BTreeSet::from([PRODUCER_FINST])),
            )]),
            BTreeMap::from([(BindingId::new(CONSUMER_BINDING), ordered_consumer())]),
        )
    }

    fn fence_contract() -> RuntimeCompletionFenceContract {
        RuntimeCompletionFenceContract::try_from_install(
            SERVICE_QID,
            DeploymentEpoch::new(EPOCH),
            ChannelId::new(CHANNEL),
            CompletionFenceKind::CommittedDomainFrozen,
            &membership_schema(),
        )
        .unwrap()
    }

    fn final_deployment(max_contribution_bytes: u64) -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(WITNESS);
        let coverage = Coverage::AllOf(vec![Coverage::Leaf(witness)]);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(CHANNEL),
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
            policy(max_contribution_bytes),
            RuntimeFilterCoreBudget::new(1 << 20),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(PRODUCER_BINDING),
                ProducerDeployment::new(witness, BTreeSet::from([PRODUCER_FINST])),
            )]),
            BTreeMap::from([(
                BindingId::new(CONSUMER_BINDING),
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
                    BTreeSet::from([CONSUMER_FINST]),
                ),
            )]),
        )
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

    fn ordered_contribution(value: i64) -> RuntimeFilterContribution {
        RuntimeFilterContribution::OrderedBound(ordered_update(&order_contract(), value))
    }

    fn topk_contribution(value: i64) -> RuntimeFilterContribution {
        let contract = topk_contract();
        let candidate =
            OrderedTuple::try_new(contract.order(), [Some(OrderedScalar::Int64(value))]).unwrap();
        RuntimeFilterContribution::TopKSummary(
            TopKSummary::try_new(&contract, vec![candidate]).unwrap(),
        )
    }

    fn final_stream() -> ProducerStreamId {
        ProducerStreamId::new(
            BindingId::new(PRODUCER_BINDING),
            PRODUCER_FINST,
            PartitionId::new(0),
        )
    }

    fn final_shard(sequence: u64, value: i64) -> FinalDomainShard {
        let stream = final_stream();
        let authority = CompletionFenceAuthority::try_new(
            Arc::new(fence_contract()),
            stream.binding_id(),
            stream.fragment_instance_id(),
        )
        .unwrap();
        let FinalDomainTestIssuerTransition::Frozen(issuer) =
            CollectingFinalDomainTestIssuer::new(authority, 1).close_driver()
        else {
            panic!("a single-driver collecting issuer must freeze on the only close")
        };
        issuer
            .issue_shard(
                stream,
                ProducerSequence::new(sequence),
                ValueDomainDelta::new(MembershipValues::int64([value]), false),
            )
            .unwrap()
    }

    fn final_contribution(sequence: u64, value: i64) -> RuntimeFilterContribution {
        RuntimeFilterContribution::FinalDomain(final_shard(sequence, value))
    }

    fn encode(
        contribution: &RuntimeFilterContribution,
        expectation: ContributionCodecExpectation,
    ) -> ([u8; 32], Vec<u8>) {
        encode_contribution(contribution, expectation, usize::MAX)
            .unwrap()
            .into_parts()
    }

    #[allow(clippy::too_many_arguments)]
    fn envelope_full(
        kind: RuntimeFilterEnvelopeKind,
        channel: u32,
        epoch: u64,
        binding: u32,
        finst: UniqueId,
        partition: u32,
        sequence: u64,
        producer_open: Option<u32>,
        digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        let route = ContributionRouteIdentity::try_new(
            BindingId::new(binding),
            finst,
            PartitionId::new(partition),
            ProducerSequence::new(sequence),
        )
        .unwrap();
        RuntimeFilterEnvelope::try_new(
            kind,
            SERVICE_QID,
            ChannelId::new(channel),
            DeploymentEpoch::new(epoch),
            RuntimeFilterRouteIdentity::contribution(route),
            producer_open.map(|count| ProducerOpenMetadata::try_new(count).unwrap()),
            None,
            &digest,
            payload,
        )
        .unwrap()
    }

    fn contribution_env(
        binding: u32,
        finst: UniqueId,
        partition: u32,
        sequence: u64,
        count: u32,
        digest: [u8; 32],
        payload: Vec<u8>,
    ) -> RuntimeFilterEnvelope {
        envelope_full(
            RuntimeFilterEnvelopeKind::Contribution,
            CHANNEL,
            EPOCH,
            binding,
            finst,
            partition,
            sequence,
            Some(count),
            digest,
            payload,
        )
    }

    fn closed_env(
        binding: u32,
        finst: UniqueId,
        partition: u32,
        sequence: u64,
        count: u32,
        digest: [u8; 32],
    ) -> RuntimeFilterEnvelope {
        envelope_full(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            CHANNEL,
            EPOCH,
            binding,
            finst,
            partition,
            sequence,
            Some(count),
            digest,
            Vec::new(),
        )
    }

    // --- read-only state observation ----------------------------------------------------------

    // Snapshot of everything a legal open/Core mutation would touch. Negative dispatch cases
    // assert this is byte-for-byte identical before and after the call: the producer instance
    // partition count, materialized partition coverage, the visible domain / reducer state, the
    // channel terminal, and the recording event count.
    #[derive(Debug, PartialEq)]
    struct DispatchStateSnapshot {
        ingress: ProducerIngressCoreSnapshot,
        // Presence only, not content: sound for this matrix because membership negatives capture
        // reducer content via `ingress.membership_values`, and ordered/topk negatives reject before
        // the producer is opened. Strengthen to capture snapshot content if reused for a
        // post-materialization ordered/topk no-mutation case.
        visible_snapshot: bool,
        availability: CoverageProgress,
        terminal: bool,
        events: usize,
    }

    fn capture(
        service: &RuntimeFilterService,
        binding: u32,
        finst: UniqueId,
        events: &RecordingEvents,
    ) -> DispatchStateSnapshot {
        let channel = channel_of(service, binding);
        DispatchStateSnapshot {
            ingress: channel.producer_ingress_core_snapshot(BindingId::new(binding), finst),
            visible_snapshot: channel.snapshot().is_some(),
            availability: channel.availability_progress(),
            terminal: channel.is_terminal(),
            events: events.len(),
        }
    }

    fn err(
        result: Result<InboundProducerDispatchOutcome, InboundProducerDispatchError>,
    ) -> InboundProducerDispatchError {
        result.expect_err("dispatch must reject")
    }

    fn assert_prefix(error: &InboundProducerDispatchError, kind: InboundProducerDispatchErrorKind) {
        assert_eq!(error.kind(), kind);
        assert!(
            error.to_string().contains(kind.prefix()),
            "{} must carry the {} prefix",
            error,
            kind.prefix()
        );
    }

    // ==========================================================================================
    // Positive matrix: one canonical contribution per variant reaches the real Core.
    // ==========================================================================================

    #[test]
    fn inbound_producer_dispatch_membership_contribution_reaches_core() {
        let (service, events) = install(membership_deployment(4096));
        let (digest, payload) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&membership_schema()),
        );
        assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));

        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        let after = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_ne!(
            before, after,
            "a canonical contribution must mutate the Core"
        );
        assert_eq!(after.ingress.local_partition_count, Some(1));
        assert_eq!(after.ingress.materialized_partition_count, 1);
        assert_eq!(
            after.ingress.membership_values,
            Some(MembershipValues::int64([7])),
            "membership submit must reach the reducer domain"
        );
        assert!(after.events > before.events);
    }

    #[test]
    fn inbound_producer_dispatch_ordered_bound_contribution_reaches_core() {
        let (service, events) = install(ordered_deployment(4096));
        let contract = order_contract();
        let (digest, payload) = encode(
            &ordered_contribution(40),
            ContributionCodecExpectation::OrderedBound(&contract),
        );
        assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));

        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        let after = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_ne!(before, after);
        assert_eq!(after.ingress.local_partition_count, Some(1));
        assert!(
            after.visible_snapshot,
            "an ordered bound submit must materialize a visible range"
        );
        assert!(after.events > before.events);
    }

    #[test]
    fn inbound_producer_dispatch_topk_summary_contribution_reaches_core() {
        let (service, events) = install(topk_deployment(4096));
        let contract = topk_contract();
        let (digest, payload) = encode(
            &topk_contribution(40),
            ContributionCodecExpectation::TopKSummary(&contract),
        );
        assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));

        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        let after = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_ne!(before, after);
        assert_eq!(after.ingress.local_partition_count, Some(1));
        assert!(
            after.events > before.events,
            "a top-k summary submit must reach the summary reducer and emit progress"
        );
    }

    #[test]
    fn inbound_producer_dispatch_final_domain_contribution_reaches_core() {
        let (service, events) = install(final_deployment(4096));
        let fence = fence_contract();
        let (digest, payload) = encode(
            &final_contribution(0, 7),
            ContributionCodecExpectation::FinalDomain {
                contract: &fence,
                stream: final_stream(),
                sequence: ProducerSequence::new(0),
            },
        );
        assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));

        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        let after = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_ne!(before, after);
        assert_eq!(after.ingress.local_partition_count, Some(1));
        assert_eq!(after.ingress.materialized_partition_count, 1);
        assert!(after.events > before.events);
    }

    // ==========================================================================================
    // Legal boundary: a contribution whose semantic bytes are exactly equal to the installed
    // budget is accepted (the semantic gate rejects only strictly-greater).
    // ==========================================================================================

    #[test]
    fn inbound_producer_dispatch_membership_semantic_budget_boundary_is_accepted() {
        let contribution = membership_contribution(7);
        let semantic = semantic_contribution_bytes(&contribution).unwrap();
        let (digest, payload) = encode(
            &contribution,
            ContributionCodecExpectation::Membership(&membership_schema()),
        );
        let (service, _events) = install(membership_deployment(semantic as u64));
        assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
    }

    #[test]
    fn inbound_producer_dispatch_ordered_semantic_budget_boundary_is_accepted() {
        let contribution = ordered_contribution(40);
        let semantic = semantic_contribution_bytes(&contribution).unwrap();
        let contract = order_contract();
        let (digest, payload) = encode(
            &contribution,
            ContributionCodecExpectation::OrderedBound(&contract),
        );
        let (service, _events) = install(ordered_deployment(semantic as u64));
        assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
    }

    #[test]
    fn inbound_producer_dispatch_topk_semantic_budget_boundary_is_accepted() {
        let contribution = topk_contribution(40);
        let semantic = semantic_contribution_bytes(&contribution).unwrap();
        let contract = topk_contract();
        let (digest, payload) = encode(
            &contribution,
            ContributionCodecExpectation::TopKSummary(&contract),
        );
        let (service, _events) = install(topk_deployment(semantic as u64));
        assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
    }

    #[test]
    fn inbound_producer_dispatch_final_semantic_budget_boundary_is_accepted() {
        let contribution = final_contribution(0, 7);
        let semantic = semantic_contribution_bytes(&contribution).unwrap();
        let fence = fence_contract();
        let (digest, payload) = encode(
            &contribution,
            ContributionCodecExpectation::FinalDomain {
                contract: &fence,
                stream: final_stream(),
                sequence: ProducerSequence::new(0),
            },
        );
        let (service, _events) = install(final_deployment(semantic as u64));
        assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
    }

    // ==========================================================================================
    // Each of the four ports' `ProducerClosed` advances partition coverage via `close_partition`.
    // ==========================================================================================

    // A bare `ProducerClosed` from the sole producer drives a `ProducerClosed`-completion channel
    // terminal via the port's `close_partition` Core call. The channel-level terminal + a freshly
    // opened producer instance are the uniform "partition coverage advanced" signals; the internal
    // per-partition bookkeeping differs by port (membership materializes a partition, the ordered /
    // top-k reducers track the close without a membership partition entry).
    fn assert_producer_closed_advances(deployment: RuntimeFilterChannelDeployment) {
        let (service, events) = install(deployment);
        let digest = installed_digest(&service, PRODUCER_BINDING);
        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert!(!before.terminal);
        assert_eq!(before.ingress.local_partition_count, None);

        assert_eq!(
            service
                .dispatch_inbound_producer(closed_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );

        let after = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_ne!(before, after);
        assert_eq!(
            after.ingress.local_partition_count,
            Some(1),
            "the producer-close opens the producer instance"
        );
        assert!(
            after.terminal,
            "closing the sole producer drives the ProducerClosed channel terminal"
        );
    }

    #[test]
    fn inbound_producer_dispatch_membership_producer_closed_advances_partition_coverage() {
        assert_producer_closed_advances(membership_deployment(4096));
    }

    #[test]
    fn inbound_producer_dispatch_ordered_producer_closed_advances_partition_coverage() {
        assert_producer_closed_advances(ordered_deployment(4096));
    }

    #[test]
    fn inbound_producer_dispatch_topk_producer_closed_advances_partition_coverage() {
        assert_producer_closed_advances(topk_deployment(4096));
    }

    // A fenced-final partition cannot close before its final-domain sequence zero, so the close
    // follows the committed domain and then finalizes the channel.
    #[test]
    fn inbound_producer_dispatch_final_producer_closed_advances_partition_coverage() {
        let (service, events) = install(final_deployment(4096));
        let close_digest = installed_digest(&service, PRODUCER_BINDING);
        let fence = fence_contract();
        let (digest, payload) = encode(
            &final_contribution(0, 7),
            ContributionCodecExpectation::FinalDomain {
                contract: &fence,
                stream: final_stream(),
                sequence: ProducerSequence::new(0),
            },
        );
        // Commit the final domain at sequence zero.
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert!(
            !before.terminal,
            "a committed-but-unclosed final domain is not yet terminal"
        );

        // The producer-close at sequence one finalizes the fenced-final partition coverage.
        assert_eq!(
            service
                .dispatch_inbound_producer(closed_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    1,
                    1,
                    close_digest
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        let after = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_ne!(before, after);
        assert!(
            after.terminal,
            "the producer-close finalizes the fenced-final channel"
        );
    }

    // ==========================================================================================
    // Replay / conflict / partition-count / terminal-noop dispatch semantics.
    // ==========================================================================================

    #[test]
    fn inbound_producer_dispatch_exact_replay_is_duplicate() {
        let (service, _events) = install(membership_deployment(4096));
        let schema = membership_schema();
        let (digest, payload) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&schema),
        );
        let env = || {
            contribution_env(
                PRODUCER_BINDING,
                PRODUCER_FINST,
                0,
                0,
                1,
                digest,
                payload.clone(),
            )
        };

        assert_eq!(
            service.dispatch_inbound_producer(env()).unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        let seeded = channel_of(&service, PRODUCER_BINDING)
            .producer_ingress_core_snapshot(BindingId::new(PRODUCER_BINDING), PRODUCER_FINST)
            .membership_values;

        assert_eq!(
            service.dispatch_inbound_producer(env()).unwrap(),
            InboundProducerDispatchOutcome::Duplicate,
        );
        let replayed = channel_of(&service, PRODUCER_BINDING)
            .producer_ingress_core_snapshot(BindingId::new(PRODUCER_BINDING), PRODUCER_FINST)
            .membership_values;
        assert_eq!(
            seeded, replayed,
            "an exact replay must not change the reducer domain"
        );
    }

    #[test]
    fn inbound_producer_dispatch_conflicting_replay_is_rejected() {
        let (service, events) = install(membership_deployment(4096));
        let schema = membership_schema();
        let (digest, payload7) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&schema),
        );
        let (digest8, payload8) = encode(
            &membership_contribution(8),
            ContributionCodecExpectation::Membership(&schema),
        );
        // Identical schema digest, different membership payload.
        assert_eq!(digest, digest8);

        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload7,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        assert_eq!(
            service
                .dispatch_inbound_producer(closed_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    1,
                    1,
                    digest
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );

        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        let error = err(service.dispatch_inbound_producer(contribution_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            1,
            digest,
            payload8,
        )));
        assert_prefix(&error, ProducerContract);
        assert_eq!(
            before,
            capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events),
            "a conflicting replay must not mutate the Core"
        );
    }

    #[test]
    fn inbound_producer_dispatch_partition_count_conflict_takes_priority() {
        let (service, events) = install(membership_deployment(4096));
        let schema = membership_schema();
        let (digest, payload) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&schema),
        );
        // Open the instance with a local partition count of 2.
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    2,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );

        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_eq!(before.ingress.local_partition_count, Some(2));

        // Reopen with count 1 and a partition id that is also invalid for count 1: the partition
        // count conflict must be reported before (and instead of) the invalid partition.
        let error = err(service.dispatch_inbound_producer(closed_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            5,
            0,
            1,
            digest,
        )));
        assert_prefix(&error, ProducerContract);
        assert!(
            error.to_string().contains("different partition count"),
            "the count conflict must take priority over the invalid partition: {error}"
        );

        let after = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        assert_eq!(before, after);
        assert_eq!(
            after.ingress.local_partition_count,
            Some(2),
            "the original partition count must be preserved"
        );
    }

    #[test]
    fn inbound_producer_dispatch_terminal_noop_short_circuits_to_accepted() {
        let (service, events) = install(membership_anyof_deployment());
        let digest_a = installed_digest(&service, PRODUCER_BINDING);
        let digest_b = installed_digest(&service, PRODUCER_B_BINDING);

        // Closing producer A satisfies the `AnyOf` terminal coverage and drives the channel
        // terminal while producer B remains installed but unopened.
        assert_eq!(
            service
                .dispatch_inbound_producer(closed_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest_a
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        assert!(
            channel_of(&service, PRODUCER_B_BINDING).is_terminal(),
            "closing producer A must satisfy the AnyOf terminal coverage"
        );

        let before = capture(&service, PRODUCER_B_BINDING, PRODUCER_B_FINST, &events);
        assert_eq!(before.ingress.local_partition_count, None);
        assert_eq!(
            service
                .dispatch_inbound_producer(closed_env(
                    PRODUCER_B_BINDING,
                    PRODUCER_B_FINST,
                    0,
                    0,
                    1,
                    digest_b,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        let after = capture(&service, PRODUCER_B_BINDING, PRODUCER_B_FINST, &events);
        assert_eq!(
            before, after,
            "an open-level terminal noop must not mutate the Core"
        );
        assert_eq!(
            after.ingress.local_partition_count, None,
            "producer B must remain unopened after the terminal noop"
        );
    }

    // ==========================================================================================
    // Codec-contract rejections: all go THROUGH dispatch_inbound_producer, never a codec helper.
    // ==========================================================================================

    #[test]
    fn inbound_producer_dispatch_malformed_frame_is_rejected() {
        let (service, events) = install(membership_deployment(4096));
        let digest = installed_digest(&service, PRODUCER_BINDING);
        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        // A payload that is not a well-formed NRFC frame (bad magic).
        let error = err(service.dispatch_inbound_producer(contribution_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            1,
            digest,
            vec![0u8; 20],
        )));
        assert_prefix(&error, CodecContract);
        assert_eq!(
            before,
            capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
        );
    }

    #[test]
    fn inbound_producer_dispatch_wrong_frame_kind_is_rejected() {
        let (service, events) = install(membership_deployment(4096));
        let schema = membership_schema();
        let (digest, mut payload) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&schema),
        );
        // Flip the frame kind tag (byte index 6) from Membership(1) to OrderedBound(2).
        payload[6] = 2;
        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        let error = err(service.dispatch_inbound_producer(contribution_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            1,
            digest,
            payload,
        )));
        assert_prefix(&error, CodecContract);
        assert_eq!(
            before,
            capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
        );
    }

    #[test]
    fn inbound_producer_dispatch_wrong_digest_is_rejected() {
        let (service, events) = install(membership_deployment(4096));
        let schema = membership_schema();
        let (correct, payload) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&schema),
        );
        let wrong = [0x5A_u8; 32];
        assert_ne!(wrong, correct);
        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);

        // Contribution whose envelope digest disagrees with the framed / installed digest.
        let contribution = err(service.dispatch_inbound_producer(contribution_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            1,
            wrong,
            payload,
        )));
        assert_prefix(&contribution, CodecContract);

        // Producer-close whose digest disagrees with the installed producer contract.
        let closed = err(service.dispatch_inbound_producer(closed_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            1,
            wrong,
        )));
        assert_prefix(&closed, CodecContract);

        assert_eq!(
            before,
            capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
        );
    }

    #[test]
    fn inbound_producer_dispatch_noncanonical_payload_is_rejected() {
        let (service, events) = install(membership_deployment(4096));
        let schema = membership_schema();
        let (digest, mut payload) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&schema),
        );
        // Corrupt the trailing contains-null flag to a non-canonical value.
        *payload.last_mut().unwrap() = 2;
        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        let error = err(service.dispatch_inbound_producer(contribution_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            1,
            digest,
            payload,
        )));
        assert_prefix(&error, CodecContract);
        assert_eq!(
            before,
            capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
        );
    }

    // Wire ceiling: `payload.len() > max_encoded_bytes` for every port's budget matrix.
    #[test]
    fn inbound_producer_dispatch_wire_oversize_is_rejected_for_all_variants() {
        // Membership
        {
            let (service, events) = install(membership_deployment(1));
            let (digest, payload) = encode(
                &membership_contribution(7),
                ContributionCodecExpectation::Membership(&membership_schema()),
            );
            assert!(payload.len() > FRAME_HEADER_LEN + 1);
            let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
            let error = err(service.dispatch_inbound_producer(contribution_env(
                PRODUCER_BINDING,
                PRODUCER_FINST,
                0,
                0,
                1,
                digest,
                payload,
            )));
            assert_prefix(&error, CodecContract);
            assert_eq!(
                before,
                capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
            );
        }
        // OrderedBound
        {
            let contract = order_contract();
            let (service, events) = install(ordered_deployment(1));
            let (digest, payload) = encode(
                &ordered_contribution(40),
                ContributionCodecExpectation::OrderedBound(&contract),
            );
            assert!(payload.len() > FRAME_HEADER_LEN + 1);
            let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
            let error = err(service.dispatch_inbound_producer(contribution_env(
                PRODUCER_BINDING,
                PRODUCER_FINST,
                0,
                0,
                1,
                digest,
                payload,
            )));
            assert_prefix(&error, CodecContract);
            assert_eq!(
                before,
                capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
            );
        }
        // TopKSummary
        {
            let contract = topk_contract();
            let (service, events) = install(topk_deployment(1));
            let (digest, payload) = encode(
                &topk_contribution(40),
                ContributionCodecExpectation::TopKSummary(&contract),
            );
            assert!(payload.len() > FRAME_HEADER_LEN + 1);
            let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
            let error = err(service.dispatch_inbound_producer(contribution_env(
                PRODUCER_BINDING,
                PRODUCER_FINST,
                0,
                0,
                1,
                digest,
                payload,
            )));
            assert_prefix(&error, CodecContract);
            assert_eq!(
                before,
                capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
            );
        }
        // FinalDomain
        {
            let fence = fence_contract();
            let (service, events) = install(final_deployment(1));
            let (digest, payload) = encode(
                &final_contribution(0, 7),
                ContributionCodecExpectation::FinalDomain {
                    contract: &fence,
                    stream: final_stream(),
                    sequence: ProducerSequence::new(0),
                },
            );
            assert!(payload.len() > FRAME_HEADER_LEN + 1);
            let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
            let error = err(service.dispatch_inbound_producer(contribution_env(
                PRODUCER_BINDING,
                PRODUCER_FINST,
                0,
                0,
                1,
                digest,
                payload,
            )));
            assert_prefix(&error, CodecContract);
            assert_eq!(
                before,
                capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
            );
        }
    }

    // The wire frame fits under the encoded ceiling, but the decoded contribution's semantic byte
    // accounting exceeds the installed budget. Ordered / top-k accounting is strictly larger than
    // the wire body, so they exercise this gate (membership's semantic bytes equal the wire body).
    #[test]
    fn inbound_producer_dispatch_wire_fits_but_semantic_oversize_is_rejected() {
        // OrderedBound
        {
            let contribution = ordered_contribution(40);
            let contract = order_contract();
            let (digest, payload) = encode(
                &contribution,
                ContributionCodecExpectation::OrderedBound(&contract),
            );
            let semantic = semantic_contribution_bytes(&contribution).unwrap();
            let wire_body = payload.len() - FRAME_HEADER_LEN;
            assert!(
                wire_body < semantic,
                "ordered semantic accounting must exceed the wire body"
            );
            // max_encoded_bytes == HEADER + wire_body == payload.len(): the frame just fits.
            let (service, events) = install(ordered_deployment(wire_body as u64));
            assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));
            let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
            let error = err(service.dispatch_inbound_producer(contribution_env(
                PRODUCER_BINDING,
                PRODUCER_FINST,
                0,
                0,
                1,
                digest,
                payload,
            )));
            assert_prefix(&error, CodecContract);
            assert!(
                error.to_string().contains("semantic byte budget"),
                "must reject at the semantic gate, not the wire ceiling: {error}"
            );
            assert_eq!(
                before,
                capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
            );
        }
        // TopKSummary
        {
            let contribution = topk_contribution(40);
            let contract = topk_contract();
            let (digest, payload) = encode(
                &contribution,
                ContributionCodecExpectation::TopKSummary(&contract),
            );
            let semantic = semantic_contribution_bytes(&contribution).unwrap();
            let wire_body = payload.len() - FRAME_HEADER_LEN;
            assert!(
                wire_body < semantic,
                "top-k semantic accounting must exceed the wire body"
            );
            let (service, events) = install(topk_deployment(wire_body as u64));
            assert_eq!(digest, installed_digest(&service, PRODUCER_BINDING));
            let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
            let error = err(service.dispatch_inbound_producer(contribution_env(
                PRODUCER_BINDING,
                PRODUCER_FINST,
                0,
                0,
                1,
                digest,
                payload,
            )));
            assert_prefix(&error, CodecContract);
            assert!(
                error.to_string().contains("semantic byte budget"),
                "must reject at the semantic gate, not the wire ceiling: {error}"
            );
            assert_eq!(
                before,
                capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
            );
        }
    }

    #[test]
    fn inbound_producer_dispatch_invalid_partition_is_rejected() {
        let (service, events) = install(membership_deployment(4096));
        let digest = installed_digest(&service, PRODUCER_BINDING);
        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        // partition 5 is outside a declared local partition count of 1.
        let error = err(service.dispatch_inbound_producer(closed_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            5,
            0,
            1,
            digest,
        )));
        assert_prefix(&error, ProducerContract);
        assert!(
            error
                .to_string()
                .contains("outside the declared local partition count"),
            "{error}"
        );
        assert_eq!(
            before,
            capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events)
        );
    }

    // ==========================================================================================
    // Route-contract and stale-epoch rejections (identity / topology).
    // ==========================================================================================

    #[test]
    fn inbound_producer_dispatch_stale_epoch_is_rejected() {
        let (service, _events) = install(membership_deployment(4096));
        let digest = installed_digest(&service, PRODUCER_BINDING);
        let error = err(service.dispatch_inbound_producer(envelope_full(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            CHANNEL,
            EPOCH - 1,
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            Some(1),
            digest,
            Vec::new(),
        )));
        assert_prefix(&error, StaleEpoch);
    }

    #[test]
    fn inbound_producer_dispatch_unknown_channel_is_rejected() {
        let (service, _events) = install(membership_deployment(4096));
        let digest = installed_digest(&service, PRODUCER_BINDING);
        let error = err(service.dispatch_inbound_producer(envelope_full(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            CHANNEL + 1,
            EPOCH,
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            Some(1),
            digest,
            Vec::new(),
        )));
        assert_prefix(&error, RouteContract);
    }

    #[test]
    fn inbound_producer_dispatch_unknown_binding_is_rejected() {
        let (service, _events) = install(membership_deployment(4096));
        let digest = installed_digest(&service, PRODUCER_BINDING);
        let error = err(service.dispatch_inbound_producer(closed_env(
            PRODUCER_BINDING + 100,
            PRODUCER_FINST,
            0,
            0,
            1,
            digest,
        )));
        assert_prefix(&error, RouteContract);
    }

    #[test]
    fn inbound_producer_dispatch_unknown_finst_is_rejected() {
        let (service, _events) = install(membership_deployment(4096));
        let digest = installed_digest(&service, PRODUCER_BINDING);
        let error = err(service.dispatch_inbound_producer(closed_env(
            PRODUCER_BINDING,
            UniqueId::new(9, 9),
            0,
            0,
            1,
            digest,
        )));
        assert_prefix(&error, RouteContract);
    }

    #[test]
    fn inbound_producer_dispatch_forbidden_envelope_kind_is_rejected() {
        let (service, _events) = install(membership_deployment(4096));
        let digest = installed_digest(&service, PRODUCER_BINDING);
        // Ack accepts a contribution route identity (so it reaches Service authorization) but is
        // not a producer ingress kind: authorization rejects it as a route contract, rather than
        // the wire adapter rejecting it earlier.
        let route = ContributionRouteIdentity::try_new(
            BindingId::new(PRODUCER_BINDING),
            PRODUCER_FINST,
            PartitionId::new(0),
            ProducerSequence::new(0),
        )
        .unwrap();
        let envelope = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::Ack,
            SERVICE_QID,
            ChannelId::new(CHANNEL),
            DeploymentEpoch::new(EPOCH),
            RuntimeFilterRouteIdentity::contribution(route),
            None,
            Some(RuntimeFilterAcceptStatus::Accepted),
            &digest,
            Vec::new(),
        )
        .unwrap();
        assert_prefix(
            &err(service.dispatch_inbound_producer(envelope)),
            RouteContract,
        );
    }

    // A validly-installed deployment can never present an inbound producer edge whose Aggregator
    // target is a remote participant: install enforces exactly one local-aggregator inbound edge
    // per authorized producer source (see `validate_channel_routing_contract`), and the router
    // check itself is covered by `role_graph.rs`. Assert instead that the dispatch error mapping
    // classifies the RoleRouter's `InboundTargetMismatch` as a route contract.
    #[test]
    fn inbound_producer_dispatch_maps_non_local_target_to_route_contract() {
        let mapped = map_route_error(RuntimeFilterRouteContractError::InboundTargetMismatch {
            channel: ChannelId::new(CHANNEL),
            edge: RouteEdgeId::new(1),
            local_participant: RuntimeFilterParticipantId::new(3),
        });
        assert_eq!(mapped.kind(), RouteContract);
        assert_eq!(RouteContract.prefix(), "[route-contract]");
        assert!(mapped.to_string().contains("[route-contract]"));
    }

    // ServiceUnavailable has two production sources: the dispatch admission gate maps a
    // Cancelled deployment to it (see dispatch_inbound_producer), and the open/submit layer
    // mints it via `service_cancelled` on the typed submit/preflight/open path. This test
    // covers the latter mapping (map_producer_error) under the stable prefix; race 1
    // (remote_producer_ingress_lifecycle_*) covers the admission-gate source end to end.
    #[test]
    fn inbound_producer_dispatch_maps_service_unavailable_to_service_contract() {
        let mapped = map_producer_error(RuntimeContractViolation::new(
            RuntimeContractViolationKind::ServiceUnavailable,
            "runtime filter service is uninstalled or cancelled",
        ));
        assert_eq!(mapped.kind(), ServiceUnavailable);
        assert_eq!(ServiceUnavailable.prefix(), "[service-unavailable]");
        assert!(mapped.to_string().contains("[service-unavailable]"));
    }

    #[test]
    fn inbound_producer_dispatch_requires_an_active_installed_route() {
        // With no installation, dispatch fails fast under the deployment-unavailable prefix and
        // never opens or mutates Core state.
        let service = service(Arc::new(RecordingEvents::default()));
        let route = ContributionRouteIdentity::try_new(
            BindingId::new(PRODUCER_BINDING),
            PRODUCER_FINST,
            PartitionId::new(0),
            ProducerSequence::new(0),
        )
        .unwrap();
        let envelope = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::Contribution,
            SERVICE_QID,
            ChannelId::new(CHANNEL),
            DeploymentEpoch::new(EPOCH),
            RuntimeFilterRouteIdentity::contribution(route),
            Some(ProducerOpenMetadata::try_new(1).unwrap()),
            None,
            &[0; 32],
            vec![1],
        )
        .unwrap();
        assert_prefix(
            &err(service.dispatch_inbound_producer(envelope)),
            DeploymentUnavailable,
        );
    }

    #[test]
    fn inbound_producer_dispatch_error_taxonomy_prefixes_are_stable() {
        for (kind, prefix) in [
            (DeploymentUnavailable, "[deployment-unavailable]"),
            (StaleEpoch, "[stale-epoch]"),
            (RouteContract, "[route-contract]"),
            (CodecContract, "[codec-contract]"),
            (ProducerContract, "[producer-contract]"),
            (ServiceUnavailable, "[service-unavailable]"),
        ] {
            assert_eq!(kind.prefix(), prefix);
        }
    }

    // ==========================================================================================
    // Task 5: compiler-produced remote producer proof + deterministic lifecycle/concurrency.
    // ==========================================================================================

    // Bounded rendezvous budget: a missed cross-thread signal fails the test instead of hanging
    // the whole suite.
    const LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(5);

    // Production-shaped 1FE+3BE remote producer proof. The real deployment compiler projects an
    // aggregator install whose inbound edge authorizes a contribution from a *remote* producer
    // source; that composite is installed into a `new_for_test` query manager and driven through
    // the production query-scoped ingress. There is no live network sender (that is M3); the
    // producer envelope is handed to the aggregator's in-process ingress.
    struct ThreeBackendIngressFixture {
        ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
        service: Arc<RuntimeFilterService>,
        channel: Arc<RuntimeFilterChannel>,
        source_participant: RuntimeFilterParticipantId,
        aggregator_participant: RuntimeFilterParticipantId,
        transport_query_id: UniqueId,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        channel_id: ChannelId,
        deployment_epoch: DeploymentEpoch,
        schema_digest: [u8; 32],
        contribution_payload: Vec<u8>,
    }

    impl ThreeBackendIngressFixture {
        fn compile_and_install() -> Self {
            let manager_query_id = QueryId::new(92, 1);
            let transport_query_id = UniqueId::new(92, 1);
            let channel_id = ChannelId::new(5);
            let remote_binding_id = BindingId::new(10);
            let remote_finst = UniqueId::new(1, 4);

            let plan = compiled_three_backend_all_of_plan();
            let deployment_epoch = plan.epoch;
            let installs = RuntimeFilterDeploymentExtension::new()
                .participant_installs(&plan)
                .expect("compiler projections pair into participant installs");
            let aggregator_participant = plan
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
                .expect("compiler produced an aggregator participant");
            let source_participant = plan.routing_shards[&aggregator_participant]
                .channel(channel_id)
                .unwrap()
                .producer_participant(remote_binding_id, remote_finst)
                .expect("compiler routed the remote producer source");
            assert_ne!(
                source_participant, aggregator_participant,
                "the compiler must route a remote producer source distinct from the aggregator"
            );
            let (_, aggregator_install) = installs
                .into_iter()
                .find(|(participant, _)| *participant == aggregator_participant)
                .expect("aggregator participant owns a composite install");

            let manager = QueryContextManager::new_for_test();
            manager
                .ensure_native_context(
                    manager_query_id,
                    false,
                    Duration::from_secs(10),
                    Duration::from_secs(10),
                )
                .expect("register the native query context");
            let service = manager
                .runtime_filter_service_for_ingress(manager_query_id)
                .expect("registered query exposes a runtime filter service");
            assert_eq!(
                service.install(aggregator_install).unwrap(),
                InstallOutcome::Installed,
            );

            let installed = service
                .registry
                .active_installation()
                .expect("aggregator install is active");
            let producer = installed
                .producer(remote_binding_id)
                .expect("aggregator installs the remote producer binding");
            let channel = Arc::clone(&producer.channel);
            let schema = match producer.inbound_contract() {
                InstalledInboundContract::Membership { schema, .. } => schema.clone(),
                _ => panic!("the compiler fixture installs a membership producer"),
            };
            let max_encoded = producer.inbound_contract().limits().max_encoded_bytes();
            let (schema_digest, contribution_payload) = encode_contribution(
                &RuntimeFilterContribution::Membership(ValueDomainDelta::new(
                    MembershipValues::int64([4242]),
                    false,
                )),
                ContributionCodecExpectation::Membership(&schema),
                max_encoded,
            )
            .unwrap()
            .into_parts();

            let ingress =
                query_scoped_runtime_filter_envelope_ingress_with_manager(Arc::clone(&manager));
            Self {
                ingress,
                service,
                channel,
                source_participant,
                aggregator_participant,
                transport_query_id,
                binding_id: remote_binding_id,
                fragment_instance_id: remote_finst,
                channel_id,
                deployment_epoch,
                schema_digest,
                contribution_payload,
            }
        }

        fn envelope(
            &self,
            kind: RuntimeFilterEnvelopeKind,
            sequence: u64,
            payload: Vec<u8>,
        ) -> RuntimeFilterEnvelope {
            let route = ContributionRouteIdentity::try_new(
                self.binding_id,
                self.fragment_instance_id,
                PartitionId::new(0),
                ProducerSequence::new(sequence),
            )
            .unwrap();
            RuntimeFilterEnvelope::try_new(
                kind,
                self.transport_query_id,
                self.channel_id,
                self.deployment_epoch,
                RuntimeFilterRouteIdentity::contribution(route),
                Some(ProducerOpenMetadata::try_new(1).unwrap()),
                None,
                &self.schema_digest,
                payload,
            )
            .unwrap()
        }

        fn contribution(&self) -> RuntimeFilterEnvelope {
            self.envelope(
                RuntimeFilterEnvelopeKind::Contribution,
                0,
                self.contribution_payload.clone(),
            )
        }

        fn producer_closed(&self) -> RuntimeFilterEnvelope {
            self.envelope(RuntimeFilterEnvelopeKind::ProducerClosed, 1, Vec::new())
        }

        fn producer_unavailable(&self, reason: ProducerFailureReason) -> RuntimeFilterEnvelope {
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
                self.transport_query_id,
                self.channel_id,
                self.deployment_epoch,
                RuntimeFilterRouteIdentity::producer_instance(
                    ProducerInstanceRouteIdentity::try_new(
                        self.binding_id,
                        self.fragment_instance_id,
                    )
                    .unwrap(),
                ),
                None,
                None,
                &self.schema_digest,
                encode_producer_failure(reason),
            )
            .unwrap()
        }

        fn ingress_snapshot(&self) -> ProducerIngressCoreSnapshot {
            self.channel
                .producer_ingress_core_snapshot(self.binding_id, self.fragment_instance_id)
        }
    }

    // Part A: the whole chain (remote finst union -> RoleRouter authority -> installed expectation
    // -> open -> dispatch) forms one path through the production query-scoped ingress.
    #[test]
    fn authorized_inbound_can_open_remote_fragment_instance_core() {
        let fixture = ThreeBackendIngressFixture::compile_and_install();
        assert_ne!(fixture.source_participant, fixture.aggregator_participant);

        assert_eq!(
            fixture
                .ingress
                .accept(fixture.contribution())
                .accept_status(),
            RuntimeFilterAcceptStatus::Accepted,
        );
        let after_contribution = fixture.ingress_snapshot();
        assert_eq!(after_contribution.local_partition_count, Some(1));
        assert_eq!(after_contribution.materialized_partition_count, 1);
        assert_eq!(
            after_contribution.membership_values,
            Some(MembershipValues::int64([4242])),
            "the remote finst contribution must reach the aggregator reducer domain"
        );

        assert_eq!(
            fixture
                .ingress
                .accept(fixture.producer_closed())
                .accept_status(),
            RuntimeFilterAcceptStatus::Accepted,
        );
        assert_eq!(
            fixture.ingress_snapshot().terminal_progress,
            TerminalProgress::Satisfied,
            "the producer-close must advance the producer instance to Satisfied"
        );
    }

    #[test]
    fn remote_producer_unavailable_is_authorized_and_fails_instance() {
        let fixture = ThreeBackendIngressFixture::compile_and_install();
        assert_eq!(fixture.ingress_snapshot().local_partition_count, None);

        let first = fixture
            .ingress
            .accept(fixture.producer_unavailable(ProducerFailureReason::ExecutionFailed));
        assert_eq!(first.accept_status(), RuntimeFilterAcceptStatus::Accepted);
        let failed = fixture.ingress_snapshot();
        assert_eq!(
            failed.local_partition_count, None,
            "unavailable must never open a stream"
        );
        assert_eq!(failed.terminal_progress, TerminalProgress::Impossible);

        let duplicate = fixture
            .ingress
            .accept(fixture.producer_unavailable(ProducerFailureReason::ExecutionFailed));
        assert_eq!(
            duplicate.accept_status(),
            RuntimeFilterAcceptStatus::Duplicate
        );

        let conflict = fixture
            .ingress
            .accept(fixture.producer_unavailable(ProducerFailureReason::UpstreamUnavailable));
        assert_eq!(
            conflict.accept_status(),
            RuntimeFilterAcceptStatus::Rejected
        );
        assert!(
            conflict
                .rejection_reason()
                .is_some_and(|reason| reason.contains("different content")),
            "same producer-instance identity with another reason must be rejected"
        );
    }

    // Part B, race 1: cancel wins before admission -> Rejected([service-unavailable]).
    #[test]
    fn remote_producer_ingress_lifecycle_cancel_before_admission_is_service_unavailable() {
        let fixture = ThreeBackendIngressFixture::compile_and_install();
        // Cancel fully lands (registry Cancelled + channels cancelled) before any dispatch.
        fixture.service.cancel();

        let result = fixture.ingress.accept(fixture.contribution());
        assert_eq!(result.accept_status(), RuntimeFilterAcceptStatus::Rejected);
        let reason = result
            .rejection_reason()
            .expect("a rejected ingress result carries a reason");
        assert!(
            reason.contains("[service-unavailable]"),
            "cancel-before-admission must reject as service-unavailable: {reason}"
        );
        assert_eq!(
            fixture.ingress_snapshot().local_partition_count,
            None,
            "a rejected admission must never open the producer instance"
        );
    }

    // Part B, race 2: admission done, submit linearizes before channel cancel -> Accepted.
    #[test]
    fn remote_producer_ingress_lifecycle_submit_before_channel_cancel_is_accepted() {
        let fixture = ThreeBackendIngressFixture::compile_and_install();

        let (admitted_tx, admitted_rx) = mpsc::channel();
        fixture
            .service
            .set_after_inbound_open_admission_hook(Arc::new(move || {
                admitted_tx.send(()).unwrap();
            }));
        // Hold the per-channel cancel until the submit has linearized, so the channel is still
        // collecting when the contribution reaches Core.
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_after_registry_cancel_before_channel_cancel_hook(Arc::new(move || {
                let _ = release_rx.lock().unwrap().recv_timeout(LIFECYCLE_TIMEOUT);
            }));

        let (status_tx, status_rx) = mpsc::channel();
        let dispatch = {
            let ingress = Arc::clone(&fixture.ingress);
            let contribution = fixture.contribution();
            std::thread::spawn(move || {
                status_tx
                    .send(ingress.accept(contribution).accept_status())
                    .unwrap();
            })
        };

        admitted_rx
            .recv_timeout(LIFECYCLE_TIMEOUT)
            .expect("dispatch admission never completed");
        let cancel = {
            let service = Arc::clone(&fixture.service);
            std::thread::spawn(move || service.cancel())
        };

        assert_eq!(
            status_rx
                .recv_timeout(LIFECYCLE_TIMEOUT)
                .expect("the submit never completed"),
            RuntimeFilterAcceptStatus::Accepted,
        );
        assert_eq!(
            fixture.ingress_snapshot().membership_values,
            Some(MembershipValues::int64([4242])),
            "a submit that linearizes before the channel cancel must reach the reducer domain"
        );

        release_tx.send(()).unwrap();
        cancel.join().expect("cancel thread panicked");
        dispatch.join().expect("dispatch thread panicked");
    }

    // Part B, race 3: admission done, submit blocked until channel shutdown completes ->
    // Accepted(TerminalNoop).
    #[test]
    fn remote_producer_ingress_lifecycle_submit_after_channel_shutdown_is_terminal_noop() {
        let fixture = ThreeBackendIngressFixture::compile_and_install();

        let (admitted_tx, admitted_rx) = mpsc::channel();
        fixture
            .service
            .set_after_inbound_open_admission_hook(Arc::new(move || {
                admitted_tx.send(()).unwrap();
            }));
        // Park the typed submit until the channel shutdown has fully completed.
        let (cancelled_tx, cancelled_rx) = mpsc::channel::<()>();
        let cancelled_rx = Mutex::new(cancelled_rx);
        fixture
            .service
            .set_before_inbound_typed_dispatch_hook(Arc::new(move || {
                let _ = cancelled_rx.lock().unwrap().recv_timeout(LIFECYCLE_TIMEOUT);
            }));

        let (status_tx, status_rx) = mpsc::channel();
        let dispatch = {
            let ingress = Arc::clone(&fixture.ingress);
            let contribution = fixture.contribution();
            std::thread::spawn(move || {
                status_tx
                    .send(ingress.accept(contribution).accept_status())
                    .unwrap();
            })
        };

        admitted_rx
            .recv_timeout(LIFECYCLE_TIMEOUT)
            .expect("dispatch admission never completed");
        let cancel = {
            let service = Arc::clone(&fixture.service);
            std::thread::spawn(move || {
                service.cancel();
                cancelled_tx.send(()).unwrap();
            })
        };

        assert_eq!(
            status_rx
                .recv_timeout(LIFECYCLE_TIMEOUT)
                .expect("the submit never resumed after channel shutdown"),
            RuntimeFilterAcceptStatus::Accepted,
        );
        let after = fixture.ingress_snapshot();
        assert_eq!(
            after.local_partition_count,
            Some(1),
            "admission opened the producer instance before the shutdown"
        );
        assert_ne!(
            after.membership_values,
            Some(MembershipValues::int64([4242])),
            "a submit after channel shutdown is a terminal no-op and must not reach the reducer"
        );
        assert!(
            fixture.channel.is_terminal(),
            "the channel is terminal once the shutdown has completed"
        );
        cancel.join().expect("cancel thread panicked");
        dispatch.join().expect("dispatch thread panicked");
    }

    // Part B, race 4: registry already Cancelled but the per-channel cancel has not finished ->
    // no registry revival, no delivery past the cancelled publish gate, no deadlock. The window is
    // NOT forced to a terminal no-op: admission classifies it as service-unavailable.
    #[test]
    fn remote_producer_ingress_lifecycle_registry_cancelled_channel_cancel_pending_window() {
        let fixture = ThreeBackendIngressFixture::compile_and_install();

        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        fixture
            .service
            .set_after_registry_cancel_before_channel_cancel_hook(Arc::new(move || {
                parked_tx.send(()).unwrap();
                let _ = release_rx.lock().unwrap().recv_timeout(LIFECYCLE_TIMEOUT);
            }));

        let (returned_tx, returned_rx) = mpsc::channel();
        let cancel = {
            let service = Arc::clone(&fixture.service);
            std::thread::spawn(move || {
                service.cancel();
                returned_tx.send(()).unwrap();
            })
        };

        parked_rx
            .recv_timeout(LIFECYCLE_TIMEOUT)
            .expect("cancel never reached the per-channel window");
        assert!(
            fixture.service.registry.active_installation().is_none(),
            "the registry stays cancelled inside the window"
        );
        let result = fixture.ingress.accept(fixture.contribution());
        assert_eq!(
            result.accept_status(),
            RuntimeFilterAcceptStatus::Rejected,
            "a dispatch inside the cancelled window must not be delivered"
        );
        assert!(
            result
                .rejection_reason()
                .is_some_and(|reason| reason.contains("[service-unavailable]")),
            "the cancelled window rejects as service-unavailable, never a forced terminal no-op"
        );
        assert_eq!(
            fixture.ingress_snapshot().local_partition_count,
            None,
            "the cancelled window neither revives the registry nor opens a producer instance"
        );

        release_tx.send(()).unwrap();
        returned_rx
            .recv_timeout(LIFECYCLE_TIMEOUT)
            .expect("cancel deadlocked finishing the per-channel window");
        cancel.join().expect("cancel thread panicked");
    }

    // Part B, race 5: a legal dispatch admitted during Publishing still sees the install's
    // DeploymentInstalled event published before its own Core contribution event, because both
    // flow through the single existing EventEmitter FIFO.
    #[test]
    fn remote_producer_ingress_lifecycle_publishing_dispatch_orders_install_before_core_events() {
        let events = Arc::new(RecordingEvents::default());
        let service = Arc::new(RuntimeFilterService::new_with_dependencies(
            SERVICE_QID,
            Arc::new(Clock),
            events.clone(),
            Arc::new(Memory),
        ));
        let (digest, payload) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&membership_schema()),
        );

        let (admitted_tx, admitted_rx) = mpsc::channel();
        service.set_after_inbound_open_admission_hook(Arc::new(move || {
            admitted_tx.send(()).unwrap();
        }));
        let (publishing_tx, publishing_rx) = mpsc::channel();
        let admitted_rx = Mutex::new(admitted_rx);
        service.set_after_commit_before_publish_hook(Arc::new(move || {
            // Publishing is committed; release the racing dispatch and wait until it has admitted
            // (its Core batch is reserved behind the install batch) before publishing.
            publishing_tx.send(()).unwrap();
            let _ = admitted_rx.lock().unwrap().recv_timeout(LIFECYCLE_TIMEOUT);
        }));

        let (result_tx, result_rx) = mpsc::channel();
        let dispatch = {
            let service = Arc::clone(&service);
            std::thread::spawn(move || {
                if publishing_rx.recv_timeout(LIFECYCLE_TIMEOUT).is_err() {
                    return;
                }
                let outcome = service.dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ));
                let _ = result_tx.send(outcome);
            })
        };

        assert_eq!(
            service
                .install(inbound_loopback_install_for_test(membership_deployment(
                    4096
                )))
                .unwrap(),
            InstallOutcome::Installed,
        );
        assert_eq!(
            result_rx
                .recv_timeout(LIFECYCLE_TIMEOUT)
                .expect("the publishing-window dispatch never completed")
                .expect("the publishing-window dispatch returned a contract error"),
            InboundProducerDispatchOutcome::Accepted,
        );
        dispatch.join().expect("dispatch thread panicked");

        let recorded = events.0.lock().unwrap();
        let installed_at = recorded
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::DeploymentInstalled { .. }))
            .expect("the install publishes a DeploymentInstalled event");
        let core_at = recorded
            .iter()
            .position(|event| matches!(event, RuntimeFilterEvent::DeltaAccepted { .. }))
            .expect("the racing contribution publishes a Core delta event");
        assert!(
            installed_at < core_at,
            "DeploymentInstalled must publish before the Core contribution event \
             (install {installed_at} vs core {core_at})"
        );
    }

    // Part B, race 6: identical reentrant installs from an event sink never deadlock, on the same
    // thread and while waiting on a cross-thread install.
    struct ReentrantIdenticalInstallSink {
        service: Mutex<Weak<RuntimeFilterService>>,
        install: Mutex<Option<RuntimeFilterParticipantInstall>>,
        outcome: mpsc::Sender<InstallOutcome>,
    }

    impl RuntimeFilterEventSink for ReentrantIdenticalInstallSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::DeploymentInstalled { .. }) {
                return;
            }
            let Some(install) = self.install.lock().unwrap().take() else {
                return;
            };
            let Some(service) = self.service.lock().unwrap().upgrade() else {
                return;
            };
            self.outcome
                .send(service.install(install).unwrap())
                .unwrap();
        }
    }

    struct CrossThreadIdenticalInstallSink {
        service: Mutex<Weak<RuntimeFilterService>>,
        install: Mutex<Option<RuntimeFilterParticipantInstall>>,
        outcome: mpsc::Sender<Option<InstallOutcome>>,
    }

    impl RuntimeFilterEventSink for CrossThreadIdenticalInstallSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::DeploymentInstalled { .. }) {
                return;
            }
            let Some(install) = self.install.lock().unwrap().take() else {
                return;
            };
            let Some(service) = self.service.lock().unwrap().upgrade() else {
                return;
            };
            let (worker_tx, worker_rx) = mpsc::channel();
            std::thread::spawn(move || {
                let _ = worker_tx.send(service.install(install));
            });
            self.outcome
                .send(
                    worker_rx
                        .recv_timeout(LIFECYCLE_TIMEOUT)
                        .ok()
                        .and_then(Result::ok),
                )
                .unwrap();
        }
    }

    #[test]
    fn remote_producer_ingress_lifecycle_reentrant_identical_install_does_not_deadlock() {
        // Same-thread: the event sink re-installs the identical composite while the install's own
        // DeploymentInstalled event drains on this thread; the reentry is idempotent and must not
        // wait on its own batch.
        {
            let composite = inbound_loopback_install_for_test(membership_deployment(4096));
            let (reentrant_tx, reentrant_rx) = mpsc::channel();
            let sink = Arc::new(ReentrantIdenticalInstallSink {
                service: Mutex::new(Weak::new()),
                install: Mutex::new(Some(composite.clone())),
                outcome: reentrant_tx,
            });
            let service = Arc::new(RuntimeFilterService::new_with_dependencies(
                SERVICE_QID,
                Arc::new(Clock),
                sink.clone(),
                Arc::new(Memory),
            ));
            *sink.service.lock().unwrap() = Arc::downgrade(&service);
            let (outer_tx, outer_rx) = mpsc::channel();
            {
                let service = Arc::clone(&service);
                std::thread::spawn(move || outer_tx.send(service.install(composite)).unwrap());
            }
            assert_eq!(
                reentrant_rx
                    .recv_timeout(LIFECYCLE_TIMEOUT)
                    .expect("same-thread reentrant identical install deadlocked"),
                InstallOutcome::AlreadyInstalled,
            );
            assert_eq!(
                outer_rx
                    .recv_timeout(LIFECYCLE_TIMEOUT)
                    .expect("outer install never finished")
                    .unwrap(),
                InstallOutcome::Installed,
            );
        }

        // Cross-thread: the event sink blocks on a different thread performing the identical
        // install; that install observes the logical commit and returns idempotently.
        {
            let composite = inbound_loopback_install_for_test(membership_deployment(4096));
            let (reentrant_tx, reentrant_rx) = mpsc::channel();
            let sink = Arc::new(CrossThreadIdenticalInstallSink {
                service: Mutex::new(Weak::new()),
                install: Mutex::new(Some(composite.clone())),
                outcome: reentrant_tx,
            });
            let service = Arc::new(RuntimeFilterService::new_with_dependencies(
                SERVICE_QID,
                Arc::new(Clock),
                sink.clone(),
                Arc::new(Memory),
            ));
            *sink.service.lock().unwrap() = Arc::downgrade(&service);
            let (outer_tx, outer_rx) = mpsc::channel();
            {
                let service = Arc::clone(&service);
                std::thread::spawn(move || outer_tx.send(service.install(composite)).unwrap());
            }
            assert_eq!(
                reentrant_rx
                    .recv_timeout(LIFECYCLE_TIMEOUT)
                    .expect("cross-thread identical install wait deadlocked"),
                Some(InstallOutcome::AlreadyInstalled),
            );
            assert_eq!(
                outer_rx
                    .recv_timeout(LIFECYCLE_TIMEOUT)
                    .expect("outer install never finished")
                    .unwrap(),
                InstallOutcome::Installed,
            );
        }
    }

    // Part B, race 7: an AnyOf channel already driven terminal by another producer answers a
    // never-opened late producer's Contribution AND ProducerClosed with open-level TerminalNoop
    // (Accepted) and installs no partition count (the instance stays unopened).
    #[test]
    fn remote_producer_ingress_lifecycle_anyof_terminal_late_producer_is_terminal_noop() {
        let (service, _events) = install(membership_anyof_deployment());
        let digest_a = installed_digest(&service, PRODUCER_BINDING);
        let digest_b = installed_digest(&service, PRODUCER_B_BINDING);

        // Closing producer A satisfies the AnyOf terminal coverage; producer B is never opened.
        assert_eq!(
            service
                .dispatch_inbound_producer(closed_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest_a,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        assert!(channel_of(&service, PRODUCER_B_BINDING).is_terminal());

        let b_partition_count = || {
            channel_of(&service, PRODUCER_B_BINDING)
                .producer_ingress_core_snapshot(
                    BindingId::new(PRODUCER_B_BINDING),
                    PRODUCER_B_FINST,
                )
                .local_partition_count
        };

        // A late Contribution to the never-opened producer B short-circuits at open-level
        // TerminalNoop: Accepted, instance stays unopened, no submit continues.
        let (digest, payload) = encode(
            &membership_contribution(9),
            ContributionCodecExpectation::Membership(&membership_schema()),
        );
        assert_eq!(digest, digest_b);
        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_B_BINDING,
                    PRODUCER_B_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        assert_eq!(
            b_partition_count(),
            None,
            "a late Contribution must not open producer B"
        );

        // The same holds for a late ProducerClosed to producer B.
        assert_eq!(
            service
                .dispatch_inbound_producer(closed_env(
                    PRODUCER_B_BINDING,
                    PRODUCER_B_FINST,
                    0,
                    1,
                    1,
                    digest_b,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        assert_eq!(
            b_partition_count(),
            None,
            "a late ProducerClosed must not open producer B"
        );
    }

    // ==========================================================================================
    // RFD-4/M3 Task 3: unified ingress dedupe + (query, epoch) tombstone (producer side).
    // ==========================================================================================

    // A byte-identical at-least-once retry of a contribution identity is absorbed as
    // `Duplicate` BEFORE the producer is opened. The `after_inbound_open_admission` seam
    // is re-armed after the first open, so it fires again only if the retry reaches the
    // Core open path; the transport-identity gate short-circuits it, so the seam stays
    // silent and the reducer domain is never mutated twice.
    #[test]
    fn ingress_dedupe_producer_transport_retry_short_circuits_before_core() {
        let (service, _events) = install(membership_deployment(4096));
        let (digest, payload) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&membership_schema()),
        );
        let env = || {
            contribution_env(
                PRODUCER_BINDING,
                PRODUCER_FINST,
                0,
                0,
                1,
                digest,
                payload.clone(),
            )
        };

        // The open-admission seam is one-shot, so re-arm the same counting closure before
        // each dispatch: it fires once per reached open. A retry that short-circuits at the
        // transport gate never reaches the open, so the count stays at one.
        let opens = Arc::new(AtomicUsize::new(0));
        let arm = || {
            let opens = Arc::clone(&opens);
            service.set_after_inbound_open_admission_hook(Arc::new(move || {
                opens.fetch_add(1, Ordering::SeqCst);
            }));
        };

        arm();
        assert_eq!(
            service.dispatch_inbound_producer(env()).unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        let seeded = channel_of(&service, PRODUCER_BINDING)
            .producer_ingress_core_snapshot(BindingId::new(PRODUCER_BINDING), PRODUCER_FINST)
            .membership_values;
        assert_eq!(opens.load(Ordering::SeqCst), 1);

        // The byte-identical retry is absorbed at the transport-identity gate: Duplicate,
        // no second open (the re-armed seam stays silent), no second Core mutation.
        arm();
        assert_eq!(
            service.dispatch_inbound_producer(env()).unwrap(),
            InboundProducerDispatchOutcome::Duplicate,
        );
        assert_eq!(
            opens.load(Ordering::SeqCst),
            1,
            "a transport retry must short-circuit before opening the producer"
        );
        let replayed = channel_of(&service, PRODUCER_BINDING)
            .producer_ingress_core_snapshot(BindingId::new(PRODUCER_BINDING), PRODUCER_FINST)
            .membership_values;
        assert_eq!(
            seeded, replayed,
            "a transport retry must not mutate the reducer domain twice"
        );
    }

    // The transport-identity gate must NOT mask the Core's conflicting-replay detection.
    // A same-identity arrival carrying different content is not a valid at-least-once
    // retry, so it flows past the content-guarded gate into the Core, which rejects it as
    // a producer contract violation (never silently absorbed as a duplicate).
    #[test]
    fn ingress_dedupe_producer_same_identity_different_content_is_conflict_not_duplicate() {
        let (service, events) = install(membership_deployment(4096));
        let schema = membership_schema();
        let (digest, payload7) = encode(
            &membership_contribution(7),
            ContributionCodecExpectation::Membership(&schema),
        );
        let (digest8, payload8) = encode(
            &membership_contribution(8),
            ContributionCodecExpectation::Membership(&schema),
        );
        assert_eq!(digest, digest8);

        assert_eq!(
            service
                .dispatch_inbound_producer(contribution_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    0,
                    1,
                    digest,
                    payload7,
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );
        assert_eq!(
            service
                .dispatch_inbound_producer(closed_env(
                    PRODUCER_BINDING,
                    PRODUCER_FINST,
                    0,
                    1,
                    1,
                    digest
                ))
                .unwrap(),
            InboundProducerDispatchOutcome::Accepted,
        );

        // Same (binding, finst, partition, sequence) as the first contribution but a
        // different membership payload: the content guard refuses to short-circuit, and
        // the Core rejects the conflicting replay.
        let before = capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events);
        let error = err(service.dispatch_inbound_producer(contribution_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            1,
            digest,
            payload8,
        )));
        assert_prefix(&error, ProducerContract);
        assert_eq!(
            before,
            capture(&service, PRODUCER_BINDING, PRODUCER_FINST, &events),
            "a conflicting replay masked as a duplicate would have mutated the Core"
        );
    }

    // (query, epoch) tombstone: after cancel/completion, a late envelope for an epoch
    // OLDER than the retired epoch is reported as a stale epoch (not masked as a bare
    // service-unavailable), and the cancelled registry is never revived (M2B3
    // lookup-only).
    #[test]
    fn ingress_dedupe_producer_tombstone_stale_epoch_after_cancel_is_rejected() {
        let (service, _events) = install(membership_deployment(4096));
        let digest = installed_digest(&service, PRODUCER_BINDING);
        service.cancel();

        let error = err(service.dispatch_inbound_producer(envelope_full(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            CHANNEL,
            EPOCH - 1,
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            Some(1),
            digest,
            Vec::new(),
        )));
        assert_prefix(&error, StaleEpoch);
        assert!(
            service.registry.active_installation().is_none(),
            "the tombstone must not revive the cancelled registry"
        );
    }

    // A late envelope for the retired epoch itself is rejected as service-unavailable
    // without reviving the cancelled registry.
    #[test]
    fn ingress_dedupe_producer_tombstone_retired_epoch_after_cancel_does_not_revive() {
        let (service, _events) = install(membership_deployment(4096));
        let digest = installed_digest(&service, PRODUCER_BINDING);
        service.cancel();

        let error = err(service.dispatch_inbound_producer(closed_env(
            PRODUCER_BINDING,
            PRODUCER_FINST,
            0,
            0,
            1,
            digest,
        )));
        assert_prefix(&error, ServiceUnavailable);
        assert!(
            service.registry.active_installation().is_none(),
            "the tombstone must not revive the cancelled registry"
        );
    }
}
