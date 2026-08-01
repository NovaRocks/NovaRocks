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

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, ReductionRequirement, RuntimeFilterLifecycle,
    RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;

use super::artifact::{ConsumerArtifactProfile, ConsumerProfileId};
use super::identity::{DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId};
use super::routing::RuntimeFilterRoutingShard;
#[cfg(test)]
use super::routing::{
    RuntimeFilterChannelRoutingView, RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer,
    RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView,
};
#[cfg(test)]
use super::transport::RuntimeFilterEnvelopeKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterCoreBudget {
    max_reducer_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaterializationPolicy {
    bloom_bits_per_key: u64,
    bloom_hash_count: u32,
    bloom_seed: u64,
    bloom_algorithm_version: u16,
    max_total_retained_bytes: u64,
    max_scratch_bytes_per_job: u64,
    max_concurrent_jobs: usize,
}

impl MaterializationPolicy {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        bloom_bits_per_key: u64,
        bloom_hash_count: u32,
        bloom_seed: u64,
        bloom_algorithm_version: u16,
        max_total_retained_bytes: u64,
        max_scratch_bytes_per_job: u64,
        max_concurrent_jobs: usize,
    ) -> Result<Self, MaterializationPolicyError> {
        if bloom_bits_per_key == 0 || bloom_hash_count == 0 || bloom_algorithm_version != 1 {
            return Err(MaterializationPolicyError::InvalidBloomContract);
        }
        if max_total_retained_bytes == 0
            || max_scratch_bytes_per_job == 0
            || max_concurrent_jobs == 0
        {
            return Err(MaterializationPolicyError::ZeroResourceLimit);
        }
        usize::try_from(max_total_retained_bytes)
            .map_err(|_| MaterializationPolicyError::PlatformSizeOverflow)?;
        usize::try_from(max_scratch_bytes_per_job)
            .map_err(|_| MaterializationPolicyError::PlatformSizeOverflow)?;
        let aggregate_scratch = max_scratch_bytes_per_job
            .checked_mul(
                u64::try_from(max_concurrent_jobs)
                    .map_err(|_| MaterializationPolicyError::PlatformSizeOverflow)?,
            )
            .ok_or(MaterializationPolicyError::AggregateScratchOverflow)?;
        max_total_retained_bytes
            .checked_add(aggregate_scratch)
            .ok_or(MaterializationPolicyError::AggregateScratchOverflow)?;
        Ok(Self {
            bloom_bits_per_key,
            bloom_hash_count,
            bloom_seed,
            bloom_algorithm_version,
            max_total_retained_bytes,
            max_scratch_bytes_per_job,
            max_concurrent_jobs,
        })
    }

    pub(crate) const fn bloom_bits_per_key(self) -> u64 {
        self.bloom_bits_per_key
    }
    pub(crate) const fn bloom_hash_count(self) -> u32 {
        self.bloom_hash_count
    }
    pub(crate) const fn bloom_seed(self) -> u64 {
        self.bloom_seed
    }
    pub(crate) const fn bloom_algorithm_version(self) -> u16 {
        self.bloom_algorithm_version
    }
    pub(crate) const fn max_total_retained_bytes(self) -> u64 {
        self.max_total_retained_bytes
    }
    pub(crate) const fn max_scratch_bytes_per_job(self) -> u64 {
        self.max_scratch_bytes_per_job
    }
    pub(crate) const fn max_concurrent_jobs(self) -> usize {
        self.max_concurrent_jobs
    }

    pub(crate) fn with_max_concurrent_jobs(
        self,
        max_concurrent_jobs: usize,
    ) -> Result<Self, MaterializationPolicyError> {
        Self::new(
            self.bloom_bits_per_key,
            self.bloom_hash_count,
            self.bloom_seed,
            self.bloom_algorithm_version,
            self.max_total_retained_bytes,
            self.max_scratch_bytes_per_job,
            max_concurrent_jobs,
        )
    }

    pub(crate) fn aggregate_scratch_bytes(self) -> Result<usize, MaterializationPolicyError> {
        let total = self
            .max_scratch_bytes_per_job
            .checked_mul(
                u64::try_from(self.max_concurrent_jobs)
                    .map_err(|_| MaterializationPolicyError::PlatformSizeOverflow)?,
            )
            .ok_or(MaterializationPolicyError::AggregateScratchOverflow)?;
        usize::try_from(total).map_err(|_| MaterializationPolicyError::PlatformSizeOverflow)
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(8, 5, 17, 1, 1 << 20, 1 << 16, 1)
            .expect("built-in materialization test policy is valid")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaterializationPolicyError {
    InvalidBloomContract,
    ZeroResourceLimit,
    PlatformSizeOverflow,
    AggregateScratchOverflow,
}

impl RuntimeFilterCoreBudget {
    pub(crate) const fn new(max_reducer_bytes: u64) -> Self {
        Self { max_reducer_bytes }
    }

    pub(crate) const fn max_reducer_bytes(self) -> u64 {
        self.max_reducer_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProducerDeployment {
    coverage_witness_id: CoverageWitnessId,
    expected_fragment_instances: BTreeSet<UniqueId>,
}

impl ProducerDeployment {
    pub(crate) fn new(
        coverage_witness_id: CoverageWitnessId,
        expected_fragment_instances: BTreeSet<UniqueId>,
    ) -> Self {
        Self {
            coverage_witness_id,
            expected_fragment_instances,
        }
    }

    pub(crate) const fn coverage_witness_id(&self) -> CoverageWitnessId {
        self.coverage_witness_id
    }

    pub(crate) const fn expected_fragment_instances(&self) -> &BTreeSet<UniqueId> {
        &self.expected_fragment_instances
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerDeployment {
    activation: ConsumerActivation,
    capabilities: BTreeSet<ArtifactCapability>,
    artifact_profile: ConsumerArtifactProfile,
    route_edge_ids: BTreeSet<RouteEdgeId>,
    expected_fragment_instances: BTreeSet<UniqueId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboundMaterializationOwner {
    DirectSource,
    Aggregator,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutboundMaterializationGroup {
    owner: OutboundMaterializationOwner,
    profile: ConsumerArtifactProfile,
    route_edge_ids: BTreeSet<RouteEdgeId>,
}

impl OutboundMaterializationGroup {
    pub(crate) fn new(
        owner: OutboundMaterializationOwner,
        profile: ConsumerArtifactProfile,
        route_edge_ids: BTreeSet<RouteEdgeId>,
    ) -> Self {
        Self {
            owner,
            profile,
            route_edge_ids,
        }
    }

    pub(crate) const fn owner(&self) -> OutboundMaterializationOwner {
        self.owner
    }

    pub(crate) const fn profile(&self) -> &ConsumerArtifactProfile {
        &self.profile
    }

    pub(crate) const fn route_edge_ids(&self) -> &BTreeSet<RouteEdgeId> {
        &self.route_edge_ids
    }
}

impl ConsumerDeployment {
    #[cfg(test)]
    pub(crate) fn new(
        activation: ConsumerActivation,
        mut capabilities: BTreeSet<ArtifactCapability>,
        route_edge_ids: BTreeSet<RouteEdgeId>,
        expected_fragment_instances: BTreeSet<UniqueId>,
    ) -> Self {
        if capabilities.contains(&ArtifactCapability::Membership) {
            capabilities.insert(ArtifactCapability::EmptyDomain);
        }
        Self::with_profile(
            activation,
            capabilities,
            ConsumerArtifactProfile::m1_test_default(),
            route_edge_ids,
            expected_fragment_instances,
        )
    }

    pub(crate) fn with_profile(
        activation: ConsumerActivation,
        capabilities: BTreeSet<ArtifactCapability>,
        artifact_profile: ConsumerArtifactProfile,
        route_edge_ids: BTreeSet<RouteEdgeId>,
        expected_fragment_instances: BTreeSet<UniqueId>,
    ) -> Self {
        Self {
            activation,
            capabilities,
            artifact_profile,
            route_edge_ids,
            expected_fragment_instances,
        }
    }

    pub(crate) const fn activation(&self) -> ConsumerActivation {
        self.activation
    }

    pub(crate) const fn capabilities(&self) -> &BTreeSet<ArtifactCapability> {
        &self.capabilities
    }

    pub(crate) const fn artifact_profile(&self) -> &ConsumerArtifactProfile {
        &self.artifact_profile
    }

    pub(crate) const fn route_edge_ids(&self) -> &BTreeSet<RouteEdgeId> {
        &self.route_edge_ids
    }

    pub(crate) const fn expected_fragment_instances(&self) -> &BTreeSet<UniqueId> {
        &self.expected_fragment_instances
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterChannelDeployment {
    channel_id: ChannelId,
    logical_domain: RuntimeFilterLogicalDomain,
    lifecycle: RuntimeFilterLifecycle,
    availability_coverage: Coverage,
    terminal_coverage: Coverage,
    reduction_requirement: ReductionRequirement,
    allowed_contribution_kinds: BTreeSet<ContributionKind>,
    completion_requirement: CompletionRequirement,
    policy: RuntimeFilterPolicyRequirement,
    core_budget: RuntimeFilterCoreBudget,
    materialization_policy: MaterializationPolicy,
    producers: BTreeMap<BindingId, ProducerDeployment>,
    consumers: BTreeMap<BindingId, ConsumerDeployment>,
    outbound_materialization_groups: BTreeMap<ConsumerProfileId, OutboundMaterializationGroup>,
}

impl RuntimeFilterChannelDeployment {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        channel_id: ChannelId,
        logical_domain: RuntimeFilterLogicalDomain,
        lifecycle: RuntimeFilterLifecycle,
        availability_coverage: Coverage,
        terminal_coverage: Coverage,
        reduction_requirement: ReductionRequirement,
        allowed_contribution_kinds: BTreeSet<ContributionKind>,
        completion_requirement: CompletionRequirement,
        policy: RuntimeFilterPolicyRequirement,
        core_budget: RuntimeFilterCoreBudget,
        materialization_policy: MaterializationPolicy,
        producers: BTreeMap<BindingId, ProducerDeployment>,
        consumers: BTreeMap<BindingId, ConsumerDeployment>,
    ) -> Self {
        Self {
            channel_id,
            logical_domain,
            lifecycle,
            availability_coverage,
            terminal_coverage,
            reduction_requirement,
            allowed_contribution_kinds,
            completion_requirement,
            policy,
            core_budget,
            materialization_policy,
            producers,
            consumers,
            outbound_materialization_groups: BTreeMap::new(),
        }
    }

    pub(crate) fn with_outbound_materialization_groups(
        mut self,
        groups: BTreeMap<ConsumerProfileId, OutboundMaterializationGroup>,
    ) -> Self {
        self.outbound_materialization_groups = groups;
        self
    }

    pub(crate) const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }
    pub(crate) const fn logical_domain(&self) -> &RuntimeFilterLogicalDomain {
        &self.logical_domain
    }
    pub(crate) const fn lifecycle(&self) -> RuntimeFilterLifecycle {
        self.lifecycle
    }
    pub(crate) const fn availability_coverage(&self) -> &Coverage {
        &self.availability_coverage
    }
    pub(crate) const fn terminal_coverage(&self) -> &Coverage {
        &self.terminal_coverage
    }
    pub(crate) const fn reduction_requirement(&self) -> ReductionRequirement {
        self.reduction_requirement
    }
    pub(crate) const fn allowed_contribution_kinds(&self) -> &BTreeSet<ContributionKind> {
        &self.allowed_contribution_kinds
    }
    pub(crate) const fn completion_requirement(&self) -> CompletionRequirement {
        self.completion_requirement
    }
    pub(crate) const fn policy(&self) -> RuntimeFilterPolicyRequirement {
        self.policy
    }
    pub(crate) const fn core_budget(&self) -> RuntimeFilterCoreBudget {
        self.core_budget
    }
    pub(crate) const fn materialization_policy(&self) -> MaterializationPolicy {
        self.materialization_policy
    }
    pub(crate) const fn producers(&self) -> &BTreeMap<BindingId, ProducerDeployment> {
        &self.producers
    }
    pub(crate) const fn consumers(&self) -> &BTreeMap<BindingId, ConsumerDeployment> {
        &self.consumers
    }
    pub(crate) const fn outbound_materialization_groups(
        &self,
    ) -> &BTreeMap<ConsumerProfileId, OutboundMaterializationGroup> {
        &self.outbound_materialization_groups
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterInstallView {
    epoch: DeploymentEpoch,
    local_participant_id: RuntimeFilterParticipantId,
    channels: BTreeMap<ChannelId, RuntimeFilterChannelDeployment>,
}

impl RuntimeFilterInstallView {
    pub(crate) fn new(
        epoch: DeploymentEpoch,
        local_participant_id: RuntimeFilterParticipantId,
        channels: BTreeMap<ChannelId, RuntimeFilterChannelDeployment>,
    ) -> Self {
        Self {
            epoch,
            local_participant_id,
            channels,
        }
    }

    pub(crate) const fn epoch(&self) -> DeploymentEpoch {
        self.epoch
    }
    pub(crate) const fn local_participant_id(&self) -> RuntimeFilterParticipantId {
        self.local_participant_id
    }
    pub(crate) const fn channels(&self) -> &BTreeMap<ChannelId, RuntimeFilterChannelDeployment> {
        &self.channels
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterParticipantInstall {
    core_view: RuntimeFilterInstallView,
    routing_shard: RuntimeFilterRoutingShard,
}

impl RuntimeFilterParticipantInstall {
    pub(crate) fn new(
        core_view: RuntimeFilterInstallView,
        routing_shard: RuntimeFilterRoutingShard,
    ) -> Self {
        Self {
            core_view,
            routing_shard,
        }
    }

    pub(crate) const fn epoch(&self) -> DeploymentEpoch {
        self.core_view.epoch()
    }

    pub(crate) const fn local_participant_id(&self) -> RuntimeFilterParticipantId {
        self.core_view.local_participant_id()
    }

    pub(crate) const fn core_view(&self) -> &RuntimeFilterInstallView {
        &self.core_view
    }

    pub(crate) const fn routing_shard(&self) -> &RuntimeFilterRoutingShard {
        &self.routing_shard
    }

    pub(crate) fn into_parts(self) -> (RuntimeFilterInstallView, RuntimeFilterRoutingShard) {
        (self.core_view, self.routing_shard)
    }
}

#[cfg(test)]
/// Builds a local-only composite for Service unit tests.
///
/// This helper maps every expected producer instance to the view's local participant.
/// It must not be used for aggregator or compiler-conformance tests, which require the
/// compiler-produced remote-aware routing shard.
pub(crate) fn local_participant_install_for_test(
    core_view: RuntimeFilterInstallView,
) -> RuntimeFilterParticipantInstall {
    let participant = core_view.local_participant_id();
    let mut channels = BTreeMap::new();
    for (channel_id, deployment) in core_view.channels() {
        let local_roles = deployment
            .producers()
            .keys()
            .copied()
            .map(RuntimeFilterRouteRole::Producer)
            .chain(
                deployment
                    .consumers()
                    .keys()
                    .copied()
                    .map(RuntimeFilterRouteRole::Consumer),
            )
            .collect();
        let producer_instances = deployment
            .producers()
            .iter()
            .flat_map(|(binding_id, producer)| {
                producer.expected_fragment_instances().iter().copied().map(
                    move |fragment_instance_id| ((*binding_id, fragment_instance_id), participant),
                )
            })
            .collect();
        let mut inbound_edges = Vec::new();
        let mut outbound_edges = Vec::new();
        if let Some(producer_binding_id) = deployment.producers().keys().next().copied() {
            for (consumer_binding_id, consumer) in deployment.consumers() {
                for route_edge_id in consumer.route_edge_ids() {
                    let edge = RuntimeFilterRoutingEdgeView::new(
                        *channel_id,
                        *route_edge_id,
                        RuntimeFilterRouteEndpointView::new(
                            participant,
                            RuntimeFilterRouteRole::Producer(producer_binding_id),
                        ),
                        RuntimeFilterRouteEndpointView::new(
                            participant,
                            RuntimeFilterRouteRole::Consumer(*consumer_binding_id),
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
                    .expect("test install view produces a valid consumer route");
                    inbound_edges.push(edge.clone());
                    outbound_edges.push(edge);
                }
            }
        }
        let channel = RuntimeFilterChannelRoutingView::new(
            *channel_id,
            local_roles,
            producer_instances,
            inbound_edges,
            outbound_edges,
        )
        .expect("test install view produces a valid routing channel");
        channels.insert(*channel_id, channel);
    }
    let routing_shard = RuntimeFilterRoutingShard::new(core_view.epoch(), participant, channels)
        .expect("test install view produces a valid routing shard");
    let projected_channels = core_view
        .channels()
        .iter()
        .map(|(channel_id, deployment)| {
            let mut grouped = BTreeMap::<
                ConsumerProfileId,
                (ConsumerArtifactProfile, BTreeSet<RouteEdgeId>),
            >::new();
            if !deployment.producers().is_empty() {
                for consumer in deployment.consumers().values() {
                    let entry = grouped
                        .entry(consumer.artifact_profile().id())
                        .or_insert_with(|| (consumer.artifact_profile().clone(), BTreeSet::new()));
                    entry.1.extend(consumer.route_edge_ids().iter().copied());
                }
            }
            let groups = grouped
                .into_iter()
                .map(|(profile_id, (profile, routes))| {
                    (
                        profile_id,
                        OutboundMaterializationGroup::new(
                            OutboundMaterializationOwner::DirectSource,
                            profile,
                            routes,
                        ),
                    )
                })
                .collect();
            (
                *channel_id,
                deployment
                    .clone()
                    .with_outbound_materialization_groups(groups),
            )
        })
        .collect();
    RuntimeFilterParticipantInstall::new(
        RuntimeFilterInstallView::new(core_view.epoch(), participant, projected_channels),
        routing_shard,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::identity::*;
    use crate::runtime_filter::port::routing::RuntimeFilterRoutingShard;

    use super::*;

    #[test]
    fn participant_install_keeps_core_and_routing_authority_together() {
        let epoch = DeploymentEpoch::new(6);
        let participant = RuntimeFilterParticipantId::new(7);
        let core_view = RuntimeFilterInstallView::new(epoch, participant, BTreeMap::new());
        let routing_shard =
            RuntimeFilterRoutingShard::new(epoch, participant, BTreeMap::new()).unwrap();

        let install =
            RuntimeFilterParticipantInstall::new(core_view.clone(), routing_shard.clone());

        assert_eq!(install.epoch(), epoch);
        assert_eq!(install.local_participant_id(), participant);
        assert_eq!(install.core_view(), &core_view);
        assert_eq!(install.routing_shard(), &routing_shard);
        assert_eq!(install.into_parts(), (core_view, routing_shard));
    }

    #[test]
    fn install_view_keeps_expected_producer_and_consumer_instances() {
        let channel_id = ChannelId::new(1);
        let producer_binding_id = BindingId::new(2);
        let consumer_binding_id = BindingId::new(3);
        let witness_id = CoverageWitnessId::new(4);
        let producer_instances = BTreeSet::from([UniqueId::new(10, 11), UniqueId::new(12, 13)]);
        let consumer_instances = BTreeSet::from([UniqueId::new(14, 15)]);

        let deployment = RuntimeFilterChannelDeployment::new(
            channel_id,
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness_id),
            Coverage::Leaf(witness_id),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 128,
                max_artifact_bytes: 256,
                deadline_ms: 1_000,
                max_retries: 7,
            },
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                producer_binding_id,
                ProducerDeployment::new(witness_id, producer_instances.clone()),
            )]),
            BTreeMap::from([(
                consumer_binding_id,
                ConsumerDeployment::new(
                    ConsumerActivation::BlockingSnapshot,
                    BTreeSet::from([ArtifactCapability::Membership]),
                    BTreeSet::from([RouteEdgeId::new(5)]),
                    consumer_instances.clone(),
                ),
            )]),
        );
        let view = RuntimeFilterInstallView::new(
            DeploymentEpoch::new(6),
            RuntimeFilterParticipantId::new(7),
            BTreeMap::from([(channel_id, deployment)]),
        );

        let installed = view.channels().get(&channel_id).unwrap();
        assert_eq!(
            installed
                .producers()
                .get(&producer_binding_id)
                .unwrap()
                .expected_fragment_instances(),
            &producer_instances
        );
        assert_eq!(
            installed
                .consumers()
                .get(&consumer_binding_id)
                .unwrap()
                .expected_fragment_instances(),
            &consumer_instances
        );
        assert_eq!(installed.policy().max_retries, 7);
        assert_eq!(installed.core_budget().max_reducer_bytes(), 512);
    }
}
