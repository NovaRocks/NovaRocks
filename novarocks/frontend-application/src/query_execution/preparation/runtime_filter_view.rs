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

//! Borrow-only runtime-filter facts for the Frontend semantic encoder.

use crate::query_execution::attempt_runtime_filter_facts as attempt_facts;
use crate::query_execution::schedule::SchedulingPlan;

pub enum RuntimeFilterLogicalDomainFacts {
    Membership {
        value_type: novarocks_physical_plan::ValueType,
        null_semantics: RuntimeFilterNullSemantics,
    },
    Ordered {
        keys: Vec<RuntimeFilterOrderKeyFacts>,
        inclusive: bool,
        comparator_digest: [u8; 32],
    },
}

impl RuntimeFilterLogicalDomainFacts {
    fn from_attempt(value: &attempt_facts::AttemptRuntimeFilterLogicalDomainFacts) -> Self {
        match value {
            attempt_facts::AttemptRuntimeFilterLogicalDomainFacts::Membership {
                value_type,
                null_semantics,
            } => Self::Membership {
                value_type: value_type.clone(),
                null_semantics: RuntimeFilterNullSemantics::from_attempt(*null_semantics),
            },
            attempt_facts::AttemptRuntimeFilterLogicalDomainFacts::Ordered {
                keys,
                inclusive,
                comparator_digest,
            } => Self::Ordered {
                keys: keys
                    .iter()
                    .map(|key| RuntimeFilterOrderKeyFacts {
                        value_type: key.value_type.clone(),
                        direction: RuntimeFilterSortDirection::from_attempt(key.direction),
                        null_order: RuntimeFilterNullOrder::from_attempt(key.null_order),
                    })
                    .collect(),
                inclusive: *inclusive,
                comparator_digest: *comparator_digest,
            },
        }
    }
}

pub struct RuntimeFilterOrderKeyFacts {
    pub value_type: novarocks_physical_plan::ValueType,
    pub direction: RuntimeFilterSortDirection,
    pub null_order: RuntimeFilterNullOrder,
}

pub enum RuntimeFilterReductionFacts {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: u32 },
}

impl RuntimeFilterReductionFacts {
    fn from_attempt(value: attempt_facts::AttemptRuntimeFilterReductionFacts) -> Self {
        match value {
            attempt_facts::AttemptRuntimeFilterReductionFacts::SetUnion => Self::SetUnion,
            attempt_facts::AttemptRuntimeFilterReductionFacts::TightenOrderedBound => {
                Self::TightenOrderedBound
            }
            attempt_facts::AttemptRuntimeFilterReductionFacts::MergeTopKSummary { k } => {
                Self::MergeTopKSummary { k }
            }
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterContributionKind {
    ValueDomainDelta,
    FinalDomainShard,
    OrderedBoundUpdate,
    TopKSummary,
    ProducerClosed,
}

impl RuntimeFilterContributionKind {
    fn from_attempt(value: attempt_facts::AttemptRuntimeFilterContributionKind) -> Self {
        use attempt_facts::AttemptRuntimeFilterContributionKind as ContributionKind;
        match value {
            ContributionKind::ValueDomainDelta => Self::ValueDomainDelta,
            ContributionKind::FinalDomainShard => Self::FinalDomainShard,
            ContributionKind::OrderedBoundUpdate => Self::OrderedBoundUpdate,
            ContributionKind::TopKSummary => Self::TopKSummary,
            ContributionKind::ProducerClosed => Self::ProducerClosed,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterCompletionRequirement {
    ProducerClosed,
    FencedCommittedDomainFrozen,
}

impl RuntimeFilterCompletionRequirement {
    fn from_attempt(value: attempt_facts::AttemptRuntimeFilterCompletionRequirement) -> Self {
        use attempt_facts::AttemptRuntimeFilterCompletionRequirement as CompletionRequirement;
        match value {
            CompletionRequirement::ProducerClosed => Self::ProducerClosed,
            CompletionRequirement::FencedCommittedDomainFrozen => Self::FencedCommittedDomainFrozen,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterArtifactCapability {
    Membership,
    OrderedRange,
    EmptyDomain,
}

impl RuntimeFilterArtifactCapability {
    fn from_attempt(value: attempt_facts::AttemptRuntimeFilterArtifactCapability) -> Self {
        use attempt_facts::AttemptRuntimeFilterArtifactCapability as ArtifactCapability;
        match value {
            ArtifactCapability::Membership => Self::Membership,
            ArtifactCapability::OrderedRange => Self::OrderedRange,
            ArtifactCapability::EmptyDomain => Self::EmptyDomain,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive(RuntimeFilterLateApplyGranularity),
}

impl RuntimeFilterConsumerActivation {
    fn from_attempt(value: attempt_facts::AttemptRuntimeFilterConsumerActivation) -> Self {
        use attempt_facts::{
            AttemptRuntimeFilterConsumerActivation as ConsumerActivation,
            AttemptRuntimeFilterLateApplyGranularity as LateApplyGranularity,
        };
        match value {
            ConsumerActivation::BlockingSnapshot => Self::BlockingSnapshot,
            ConsumerActivation::NonBlockingLive(late_apply) => {
                Self::NonBlockingLive(match late_apply {
                    LateApplyGranularity::Row => RuntimeFilterLateApplyGranularity::Row,
                    LateApplyGranularity::Batch => RuntimeFilterLateApplyGranularity::Batch,
                    LateApplyGranularity::RowGroup => RuntimeFilterLateApplyGranularity::RowGroup,
                    LateApplyGranularity::Split => RuntimeFilterLateApplyGranularity::Split,
                    LateApplyGranularity::File => RuntimeFilterLateApplyGranularity::File,
                })
            }
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterLateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterSortDirection {
    Ascending,
    Descending,
}

impl RuntimeFilterSortDirection {
    fn from_attempt(value: attempt_facts::AttemptRuntimeFilterSortDirection) -> Self {
        match value {
            attempt_facts::AttemptRuntimeFilterSortDirection::Ascending => Self::Ascending,
            attempt_facts::AttemptRuntimeFilterSortDirection::Descending => Self::Descending,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterNullOrder {
    First,
    Last,
}

impl RuntimeFilterNullOrder {
    fn from_attempt(value: attempt_facts::AttemptRuntimeFilterNullOrder) -> Self {
        match value {
            attempt_facts::AttemptRuntimeFilterNullOrder::First => Self::First,
            attempt_facts::AttemptRuntimeFilterNullOrder::Last => Self::Last,
        }
    }
}

/// Borrow-only deployment facts projected from the sealed SQL plan and the
/// already validated schedule. This is intentionally not a graph facade:
/// every public result is a narrow immutable fact value.
#[derive(Clone, Copy)]
pub struct RuntimeFilterDeploymentFactsView<'a> {
    runtime_filters:
        &'a crate::query_execution::attempt_runtime_filter_facts::AttemptRuntimeFilterFacts,
    fragment_edges: &'a [crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    schedule: &'a SchedulingPlan,
}

impl<'a> RuntimeFilterDeploymentFactsView<'a> {
    /// The three facts this view projects, rather than the plan carrier they
    /// happen to be reachable from today. A completed plan and a sealed plan
    /// both produce these, so deployment stops depending on which one built
    /// the execution -- the same reason scheduling reads
    /// `FragmentSchedulingFacts` instead of a plan representation.
    pub(crate) const fn new(
        runtime_filters: &'a crate::query_execution::attempt_runtime_filter_facts::AttemptRuntimeFilterFacts,
        fragment_edges: &'a [crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
        schedule: &'a SchedulingPlan,
    ) -> Self {
        Self {
            runtime_filters,
            fragment_edges,
            schedule,
        }
    }

    pub fn channels(self) -> impl Iterator<Item = RuntimeFilterChannelDeploymentFacts<'a>> + 'a {
        self.runtime_filters
            .channels()
            .iter()
            .map(|channel| RuntimeFilterChannelDeploymentFacts { channel })
    }

    pub fn bindings(self) -> impl Iterator<Item = RuntimeFilterDeploymentBindingFacts<'a>> + 'a {
        self.runtime_filters
            .deployment_bindings()
            .iter()
            .map(|binding| RuntimeFilterDeploymentBindingFacts { binding })
    }

    pub fn placements(self) -> impl Iterator<Item = RuntimeFilterValidatedPlacementFacts> + 'a {
        self.schedule
            .by_fragment
            .values()
            .flatten()
            .map(|placement| RuntimeFilterValidatedPlacementFacts {
                fragment_id: placement.fragment_id,
                task_key: placement.task_key,
                backend_idx: placement.backend_idx,
            })
    }

    pub fn fragment_edges(
        self,
    ) -> impl ExactSizeIterator<Item = RuntimeFilterFragmentEdgeFacts> + 'a {
        self.fragment_edges
            .iter()
            .map(RuntimeFilterFragmentEdgeFacts::from_fragment_edge)
    }

    /// Each producer tuple has at most one sealed proof or skip provenance.
    /// The source bindings are BTreeMap ordered, so this iterator is stable.
    pub fn join_progress(self) -> impl Iterator<Item = RuntimeFilterJoinProgressFacts> + 'a {
        self.runtime_filters
            .join_progress()
            .iter()
            .map(RuntimeFilterJoinProgressFacts::from_attempt)
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterChannelDeploymentFacts<'a> {
    channel: &'a attempt_facts::AttemptRuntimeFilterChannelFacts,
}

impl RuntimeFilterChannelDeploymentFacts<'_> {
    pub fn channel_id(self) -> u32 {
        self.channel.channel_id
    }

    pub fn logical_domain(self) -> RuntimeFilterLogicalDomainFacts {
        RuntimeFilterLogicalDomainFacts::from_attempt(&self.channel.logical_domain)
    }

    pub fn lifecycle(self) -> RuntimeFilterDeploymentLifecycleFacts {
        match self.channel.lifecycle {
            attempt_facts::AttemptRuntimeFilterLifecycleFacts::CompleteOnce => {
                RuntimeFilterDeploymentLifecycleFacts::CompleteOnce
            }
            attempt_facts::AttemptRuntimeFilterLifecycleFacts::MonotonicUpdates => {
                RuntimeFilterDeploymentLifecycleFacts::MonotonicUpdates
            }
        }
    }

    pub fn availability_coverage(self) -> RuntimeFilterCoverageFacts {
        RuntimeFilterCoverageFacts::from_attempt(&self.channel.availability_coverage)
    }

    pub fn terminal_coverage(self) -> RuntimeFilterCoverageFacts {
        RuntimeFilterCoverageFacts::from_attempt(&self.channel.terminal_coverage)
    }

    pub fn reduction(self) -> RuntimeFilterReductionFacts {
        RuntimeFilterReductionFacts::from_attempt(self.channel.reduction)
    }

    pub fn allowed_contribution_kinds(self) -> Vec<RuntimeFilterContributionKind> {
        self.channel
            .allowed_contribution_kinds
            .iter()
            .copied()
            .map(RuntimeFilterContributionKind::from_attempt)
            .collect()
    }

    pub fn required_consumer_capabilities(self) -> Vec<RuntimeFilterArtifactCapability> {
        self.channel
            .required_consumer_capabilities
            .iter()
            .copied()
            .map(RuntimeFilterArtifactCapability::from_attempt)
            .collect()
    }

    pub fn policy(self) -> RuntimeFilterPolicyFacts {
        RuntimeFilterPolicyFacts {
            max_contribution_bytes: self.channel.policy.max_contribution_bytes,
            max_artifact_bytes: self.channel.policy.max_artifact_bytes,
            deadline_ms: self.channel.policy.deadline_ms,
            max_retries: self.channel.policy.max_retries,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterNullSemantics {
    NeverMatches,
    NullSafeEqual,
}

impl RuntimeFilterNullSemantics {
    fn from_attempt(value: attempt_facts::AttemptRuntimeFilterNullSemantics) -> Self {
        match value {
            attempt_facts::AttemptRuntimeFilterNullSemantics::NeverMatches => Self::NeverMatches,
            attempt_facts::AttemptRuntimeFilterNullSemantics::NullSafeEqual => Self::NullSafeEqual,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterDeploymentLifecycleFacts {
    CompleteOnce,
    MonotonicUpdates,
}

pub enum RuntimeFilterCoverageFacts {
    LeafWitnessId(u32),
    AllOf(Vec<RuntimeFilterCoverageFacts>),
    AnyOf(Vec<RuntimeFilterCoverageFacts>),
}

impl RuntimeFilterCoverageFacts {
    fn from_attempt(coverage: &attempt_facts::AttemptRuntimeFilterCoverageFacts) -> Self {
        match coverage {
            attempt_facts::AttemptRuntimeFilterCoverageFacts::LeafWitnessId(witness) => {
                Self::LeafWitnessId(*witness)
            }
            attempt_facts::AttemptRuntimeFilterCoverageFacts::AllOf(children) => {
                Self::AllOf(children.iter().map(Self::from_attempt).collect())
            }
            attempt_facts::AttemptRuntimeFilterCoverageFacts::AnyOf(children) => {
                Self::AnyOf(children.iter().map(Self::from_attempt).collect())
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterPolicyFacts {
    pub max_contribution_bytes: u64,
    pub max_artifact_bytes: u64,
    pub deadline_ms: u64,
    pub max_retries: u32,
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterDeploymentBindingFacts<'a> {
    binding: &'a attempt_facts::AttemptRuntimeFilterDeploymentBindingFacts,
}

impl RuntimeFilterDeploymentBindingFacts<'_> {
    pub fn binding_id(self) -> u32 {
        self.binding.binding_id
    }

    pub fn channel_id(self) -> u32 {
        self.binding.channel_id
    }

    pub fn fragment_id(self) -> u32 {
        self.binding.fragment_id
    }

    pub fn node_id(self) -> i32 {
        self.binding.node_id
    }

    pub fn coverage_witness_id(self) -> Option<u32> {
        self.binding.coverage_witness_id
    }

    pub fn role(self) -> RuntimeFilterDeploymentBindingRoleFacts {
        match &self.binding.role {
            attempt_facts::AttemptRuntimeFilterBindingRoleFacts::Producer {
                completion_requirement,
                ..
            } => RuntimeFilterDeploymentBindingRoleFacts::Producer {
                completion_requirement: RuntimeFilterCompletionRequirement::from_attempt(
                    *completion_requirement,
                ),
            },
            attempt_facts::AttemptRuntimeFilterBindingRoleFacts::Consumer {
                capabilities,
                activation,
                feedback_scan_type,
                ..
            } => RuntimeFilterDeploymentBindingRoleFacts::Consumer {
                capabilities: capabilities
                    .iter()
                    .copied()
                    .map(RuntimeFilterArtifactCapability::from_attempt)
                    .collect(),
                activation: RuntimeFilterConsumerActivation::from_attempt(*activation),
                feedback_scan_type: feedback_scan_type.clone(),
            },
        }
    }
}

pub enum RuntimeFilterDeploymentBindingRoleFacts {
    Producer {
        completion_requirement: RuntimeFilterCompletionRequirement,
    },
    Consumer {
        capabilities: Vec<RuntimeFilterArtifactCapability>,
        activation: RuntimeFilterConsumerActivation,
        feedback_scan_type: Option<novarocks_physical_plan::ValueType>,
    },
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterValidatedPlacementFacts {
    fragment_id: u32,
    task_key: novarocks_types::UniqueId,
    backend_idx: usize,
}

impl RuntimeFilterValidatedPlacementFacts {
    pub const fn fragment_id(self) -> u32 {
        self.fragment_id
    }

    pub const fn task_key(self) -> novarocks_types::UniqueId {
        self.task_key
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterFragmentEdgeFacts {
    source_fragment_id: u32,
    target_fragment_id: u32,
    target_exchange_node_id: i32,
}

impl RuntimeFilterFragmentEdgeFacts {
    fn from_fragment_edge(
        edge: &crate::query_execution::attempt_plan_facts::AttemptEdgeFacts,
    ) -> Self {
        Self {
            source_fragment_id: edge.source_fragment_id,
            target_fragment_id: edge.target_fragment_id,
            target_exchange_node_id: edge.target_exchange_node_id,
        }
    }

    pub const fn source_fragment_id(self) -> u32 {
        self.source_fragment_id
    }

    pub const fn target_fragment_id(self) -> u32 {
        self.target_fragment_id
    }

    pub const fn target_exchange_node_id(self) -> i32 {
        self.target_exchange_node_id
    }
}

pub enum RuntimeFilterJoinProgressFacts {
    Proven {
        channel_id: u32,
        producer_binding_id: u32,
        producer_fragment_id: u32,
        join_node_id: i32,
        build_frontier: Vec<RuntimeFilterFrontierEdgeFacts>,
        non_build_inputs: Vec<RuntimeFilterFrontierEdgeFacts>,
    },
}

impl RuntimeFilterJoinProgressFacts {
    fn from_attempt(value: &attempt_facts::AttemptRuntimeFilterJoinProgressFacts) -> Self {
        match value {
            attempt_facts::AttemptRuntimeFilterJoinProgressFacts::Proven {
                channel_id,
                producer_binding_id,
                producer_fragment_id,
                join_node_id,
                build_frontier,
                non_build_inputs,
            } => Self::Proven {
                channel_id: *channel_id,
                producer_binding_id: *producer_binding_id,
                producer_fragment_id: *producer_fragment_id,
                join_node_id: *join_node_id,
                build_frontier: build_frontier
                    .iter()
                    .map(RuntimeFilterFrontierEdgeFacts::from_attempt)
                    .collect(),
                non_build_inputs: non_build_inputs
                    .iter()
                    .map(RuntimeFilterFrontierEdgeFacts::from_attempt)
                    .collect(),
            },
        }
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterFrontierEdgeFacts {
    pub source_fragment_id: u32,
    pub target_exchange_node_id: i32,
}

impl RuntimeFilterFrontierEdgeFacts {
    fn from_attempt(edge: &attempt_facts::AttemptRuntimeFilterFrontierEdgeFacts) -> Self {
        Self {
            source_fragment_id: edge.source_fragment_id,
            target_exchange_node_id: edge.target_exchange_node_id,
        }
    }
}
