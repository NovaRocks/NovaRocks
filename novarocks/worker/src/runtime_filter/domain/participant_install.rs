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

//! Backend-owned sealed runtime-filter installation.
//!
//! This is the participant's decoded deployment authority.  It deliberately
//! separates Execution contracts from Backend-local coverage, physical
//! materialization, expected-instance, and routing facts.  In particular it
//! does not retain a Core installation view or a protocol DTO after strict
//! native decoding completes.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use novarocks_execution::runtime_filter::{
    RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterConsumerContract,
    RuntimeFilterExecutionContract, RuntimeFilterProducerContract,
};
use novarocks_types::UniqueId;

use crate::runtime_filter::artifact::{ConsumerArtifactProfile, ConsumerProfileId};

use super::{
    BackendCoverage, BackendCoverageWitnessId, BackendParticipantIdentity, BackendRouteEdgeId,
    BackendRoutingShard,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendChannelLifecycle {
    CompleteOnce,
    MonotonicUpdates,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendMaterializationOwner {
    DirectSource,
    Aggregator,
}

/// Sealed authority for one best-effort Frontend feedback publication.  This
/// stays in the Backend install domain rather than retaining the protobuf
/// carrier after native decoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendFrontendFeedbackPublication {
    publisher_owner: BackendMaterializationOwner,
    contract_digest: [u8; 32],
    max_encoded_domain_bytes: usize,
}

impl BackendFrontendFeedbackPublication {
    pub fn new(
        publisher_owner: BackendMaterializationOwner,
        contract_digest: [u8; 32],
        max_encoded_domain_bytes: usize,
    ) -> Result<Self, BackendParticipantInstallError> {
        if max_encoded_domain_bytes == 0 || max_encoded_domain_bytes > 64 * 1024 {
            return Err(BackendParticipantInstallError::InvalidFeedbackPublication);
        }
        Ok(Self {
            publisher_owner,
            contract_digest,
            max_encoded_domain_bytes,
        })
    }

    pub const fn publisher_owner(&self) -> BackendMaterializationOwner {
        self.publisher_owner
    }

    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }

    pub const fn max_encoded_domain_bytes(&self) -> usize {
        self.max_encoded_domain_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendMaterializationPolicy {
    bloom_bits_per_key: u32,
    bloom_hash_count: u32,
    bloom_seed: u64,
    bloom_algorithm_version: u16,
    max_total_retained_bytes: usize,
    max_scratch_bytes_per_job: usize,
    max_concurrent_jobs: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendParticipantInstallError {
    ZeroBudget,
    ZeroConcurrentJobs,
    AggregateScratchOverflow,
    DuplicateChannel,
    DuplicateBinding,
    DuplicateConsumerRoute,
    EmptyExpectedInstances,
    EmptyConsumerRoutes,
    InvalidFeedbackPublication,
}

impl fmt::Display for BackendParticipantInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter participant install: {self:?}"
        )
    }
}

impl std::error::Error for BackendParticipantInstallError {}

impl BackendMaterializationPolicy {
    pub fn new(
        bloom_bits_per_key: u32,
        bloom_hash_count: u32,
        bloom_seed: u64,
        bloom_algorithm_version: u16,
        max_total_retained_bytes: usize,
        max_scratch_bytes_per_job: usize,
        max_concurrent_jobs: usize,
    ) -> Result<Self, BackendParticipantInstallError> {
        if max_total_retained_bytes == 0 || max_scratch_bytes_per_job == 0 {
            return Err(BackendParticipantInstallError::ZeroBudget);
        }
        if max_concurrent_jobs == 0 {
            return Err(BackendParticipantInstallError::ZeroConcurrentJobs);
        }
        max_scratch_bytes_per_job
            .checked_mul(max_concurrent_jobs)
            .ok_or(BackendParticipantInstallError::AggregateScratchOverflow)?;
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

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn bloom_bits_per_key(&self) -> u32 {
        self.bloom_bits_per_key
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn bloom_hash_count(&self) -> u32 {
        self.bloom_hash_count
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn bloom_seed(&self) -> u64 {
        self.bloom_seed
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn bloom_algorithm_version(&self) -> u16 {
        self.bloom_algorithm_version
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn max_total_retained_bytes(&self) -> usize {
        self.max_total_retained_bytes
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn max_scratch_bytes_per_job(&self) -> usize {
        self.max_scratch_bytes_per_job
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn max_concurrent_jobs(&self) -> usize {
        self.max_concurrent_jobs
    }
    pub fn aggregate_scratch_bytes(&self) -> Result<usize, BackendParticipantInstallError> {
        self.max_scratch_bytes_per_job
            .checked_mul(self.max_concurrent_jobs)
            .ok_or(BackendParticipantInstallError::AggregateScratchOverflow)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendOutboundMaterializationGroup {
    owner: BackendMaterializationOwner,
    profile: ConsumerArtifactProfile,
    route_edge_ids: BTreeSet<BackendRouteEdgeId>,
}

impl BackendOutboundMaterializationGroup {
    pub fn new(
        owner: BackendMaterializationOwner,
        profile: ConsumerArtifactProfile,
        route_edge_ids: impl IntoIterator<Item = BackendRouteEdgeId>,
    ) -> Result<Self, BackendParticipantInstallError> {
        let route_edge_ids: BTreeSet<BackendRouteEdgeId> = route_edge_ids.into_iter().collect();
        if route_edge_ids.is_empty() {
            return Err(BackendParticipantInstallError::EmptyConsumerRoutes);
        }
        Ok(Self {
            owner,
            profile,
            route_edge_ids,
        })
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn owner(&self) -> BackendMaterializationOwner {
        self.owner
    }
    pub const fn profile(&self) -> &ConsumerArtifactProfile {
        &self.profile
    }
    pub const fn route_edge_ids(&self) -> &BTreeSet<BackendRouteEdgeId> {
        &self.route_edge_ids
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendProducerInstall {
    contract: RuntimeFilterProducerContract,
    coverage_witness: BackendCoverageWitnessId,
    expected_tasks: BTreeSet<UniqueId>,
    max_contribution_bytes: usize,
}

impl BackendProducerInstall {
    pub fn new(
        contract: RuntimeFilterProducerContract,
        coverage_witness: BackendCoverageWitnessId,
        expected_tasks: impl IntoIterator<Item = UniqueId>,
        max_contribution_bytes: usize,
    ) -> Result<Self, BackendParticipantInstallError> {
        let expected_tasks: BTreeSet<UniqueId> = expected_tasks.into_iter().collect();
        if expected_tasks.is_empty() {
            return Err(BackendParticipantInstallError::EmptyExpectedInstances);
        }
        if max_contribution_bytes == 0 {
            return Err(BackendParticipantInstallError::ZeroBudget);
        }
        Ok(Self {
            contract,
            coverage_witness,
            expected_tasks,
            max_contribution_bytes,
        })
    }

    pub const fn contract(&self) -> &RuntimeFilterProducerContract {
        &self.contract
    }
    pub const fn coverage_witness(&self) -> BackendCoverageWitnessId {
        self.coverage_witness
    }
    pub const fn expected_tasks(&self) -> &BTreeSet<UniqueId> {
        &self.expected_tasks
    }
    pub const fn max_contribution_bytes(&self) -> usize {
        self.max_contribution_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendConsumerInstall {
    contract: RuntimeFilterConsumerContract,
    profile: ConsumerArtifactProfile,
    route_edge_ids: BTreeSet<BackendRouteEdgeId>,
    expected_tasks: BTreeSet<UniqueId>,
}

impl BackendConsumerInstall {
    pub fn new(
        contract: RuntimeFilterConsumerContract,
        profile: ConsumerArtifactProfile,
        route_edge_ids: impl IntoIterator<Item = BackendRouteEdgeId>,
        expected_tasks: impl IntoIterator<Item = UniqueId>,
    ) -> Result<Self, BackendParticipantInstallError> {
        let route_edge_ids: BTreeSet<BackendRouteEdgeId> = route_edge_ids.into_iter().collect();
        let expected_tasks: BTreeSet<UniqueId> = expected_tasks.into_iter().collect();
        if route_edge_ids.is_empty() {
            return Err(BackendParticipantInstallError::EmptyConsumerRoutes);
        }
        if expected_tasks.is_empty() {
            return Err(BackendParticipantInstallError::EmptyExpectedInstances);
        }
        Ok(Self {
            contract,
            profile,
            route_edge_ids,
            expected_tasks,
        })
    }

    pub const fn contract(&self) -> &RuntimeFilterConsumerContract {
        &self.contract
    }
    pub const fn profile(&self) -> &ConsumerArtifactProfile {
        &self.profile
    }
    pub const fn route_edge_ids(&self) -> &BTreeSet<BackendRouteEdgeId> {
        &self.route_edge_ids
    }
    pub const fn expected_tasks(&self) -> &BTreeSet<UniqueId> {
        &self.expected_tasks
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendChannelInstall {
    channel_id: RuntimeFilterChannelId,
    execution_contract: RuntimeFilterExecutionContract,
    lifecycle: BackendChannelLifecycle,
    availability_coverage: BackendCoverage,
    terminal_coverage: BackendCoverage,
    materialization_policy: BackendMaterializationPolicy,
    max_reducer_bytes: usize,
    max_artifact_bytes: usize,
    producers: BTreeMap<RuntimeFilterBindingId, BackendProducerInstall>,
    consumers: BTreeMap<RuntimeFilterBindingId, BackendConsumerInstall>,
    outbound_materialization_groups:
        BTreeMap<ConsumerProfileId, BackendOutboundMaterializationGroup>,
    frontend_feedback_publication: Option<BackendFrontendFeedbackPublication>,
}

impl BackendChannelInstall {
    #[expect(
        clippy::too_many_arguments,
        reason = "Construction validates each frozen runtime-filter install fact and its cross-field relationships; a second public input schema adds no distinct invariant."
    )]
    pub fn new(
        channel_id: RuntimeFilterChannelId,
        execution_contract: RuntimeFilterExecutionContract,
        lifecycle: BackendChannelLifecycle,
        availability_coverage: BackendCoverage,
        terminal_coverage: BackendCoverage,
        materialization_policy: BackendMaterializationPolicy,
        max_reducer_bytes: usize,
        max_artifact_bytes: usize,
        producers: impl IntoIterator<Item = BackendProducerInstall>,
        consumers: impl IntoIterator<Item = BackendConsumerInstall>,
        outbound_materialization_groups: impl IntoIterator<Item = BackendOutboundMaterializationGroup>,
    ) -> Result<Self, BackendParticipantInstallError> {
        if max_reducer_bytes == 0 || max_artifact_bytes == 0 {
            return Err(BackendParticipantInstallError::ZeroBudget);
        }
        let mut producer_map = BTreeMap::new();
        for producer in producers {
            if producer_map
                .insert(producer.contract().binding_id(), producer)
                .is_some()
            {
                return Err(BackendParticipantInstallError::DuplicateBinding);
            }
        }
        let mut consumer_map = BTreeMap::new();
        let mut routes = BTreeSet::new();
        for consumer in consumers {
            let binding_id = consumer.contract().binding_id();
            if producer_map.contains_key(&binding_id)
                || consumer_map.insert(binding_id, consumer.clone()).is_some()
            {
                return Err(BackendParticipantInstallError::DuplicateBinding);
            }
            if consumer
                .route_edge_ids()
                .iter()
                .any(|route| !routes.insert(*route))
            {
                return Err(BackendParticipantInstallError::DuplicateConsumerRoute);
            }
        }
        let mut groups = BTreeMap::new();
        for group in outbound_materialization_groups {
            if groups.insert(group.profile().id(), group).is_some() {
                return Err(BackendParticipantInstallError::DuplicateConsumerRoute);
            }
        }
        Ok(Self {
            channel_id,
            execution_contract,
            lifecycle,
            availability_coverage,
            terminal_coverage,
            materialization_policy,
            max_reducer_bytes,
            max_artifact_bytes,
            producers: producer_map,
            consumers: consumer_map,
            outbound_materialization_groups: groups,
            frontend_feedback_publication: None,
        })
    }

    pub fn with_frontend_feedback_publication(
        mut self,
        publication: BackendFrontendFeedbackPublication,
    ) -> Result<Self, BackendParticipantInstallError> {
        let RuntimeFilterExecutionContract::Membership(schema) = &self.execution_contract else {
            return Err(BackendParticipantInstallError::InvalidFeedbackPublication);
        };
        if self.lifecycle != BackendChannelLifecycle::CompleteOnce
            || self.producers.is_empty()
            || schema.digest() != publication.contract_digest()
            || !self
                .outbound_materialization_groups
                .values()
                .any(|group| group.owner() == publication.publisher_owner())
        {
            return Err(BackendParticipantInstallError::InvalidFeedbackPublication);
        }
        self.frontend_feedback_publication = Some(publication);
        Ok(self)
    }

    pub const fn channel_id(&self) -> RuntimeFilterChannelId {
        self.channel_id
    }
    pub const fn execution_contract(&self) -> &RuntimeFilterExecutionContract {
        &self.execution_contract
    }
    pub const fn lifecycle(&self) -> BackendChannelLifecycle {
        self.lifecycle
    }
    pub const fn availability_coverage(&self) -> &BackendCoverage {
        &self.availability_coverage
    }
    pub const fn terminal_coverage(&self) -> &BackendCoverage {
        &self.terminal_coverage
    }
    pub const fn materialization_policy(&self) -> &BackendMaterializationPolicy {
        &self.materialization_policy
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn max_reducer_bytes(&self) -> usize {
        self.max_reducer_bytes
    }
    pub const fn max_artifact_bytes(&self) -> usize {
        self.max_artifact_bytes
    }
    pub const fn producers(&self) -> &BTreeMap<RuntimeFilterBindingId, BackendProducerInstall> {
        &self.producers
    }
    pub const fn consumers(&self) -> &BTreeMap<RuntimeFilterBindingId, BackendConsumerInstall> {
        &self.consumers
    }
    pub const fn outbound_materialization_groups(
        &self,
    ) -> &BTreeMap<ConsumerProfileId, BackendOutboundMaterializationGroup> {
        &self.outbound_materialization_groups
    }
    pub const fn frontend_feedback_publication(
        &self,
    ) -> Option<&BackendFrontendFeedbackPublication> {
        self.frontend_feedback_publication.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendParticipantInstall {
    participant: BackendParticipantIdentity,
    local_participant_id: u32,
    channels: BTreeMap<RuntimeFilterChannelId, BackendChannelInstall>,
    routing: BackendRoutingShard,
}

impl BackendParticipantInstall {
    pub fn new(
        participant: BackendParticipantIdentity,
        local_participant_id: u32,
        channels: impl IntoIterator<Item = BackendChannelInstall>,
        routing: BackendRoutingShard,
    ) -> Result<Self, BackendParticipantInstallError> {
        let mut by_id = BTreeMap::new();
        for channel in channels {
            if by_id.insert(channel.channel_id(), channel).is_some() {
                return Err(BackendParticipantInstallError::DuplicateChannel);
            }
        }
        Ok(Self {
            participant,
            local_participant_id,
            channels: by_id,
            routing,
        })
    }

    pub const fn participant(&self) -> BackendParticipantIdentity {
        self.participant
    }

    pub const fn local_participant_id(&self) -> u32 {
        self.local_participant_id
    }
    pub const fn channels(&self) -> &BTreeMap<RuntimeFilterChannelId, BackendChannelInstall> {
        &self.channels
    }
    pub const fn routing(&self) -> &BackendRoutingShard {
        &self.routing
    }
}
