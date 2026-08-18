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

use crate::query_execution::schedule::SchedulingPlan;
use arrow::datatypes::DataType;
use novarocks_sql::plan_read::FragmentEdge;
use novarocks_sql::plan_read::TypedExpr;
use novarocks_sql::planning::query_execution as sql_facts;

use super::projection::PreparedFragmentSet;

#[derive(Clone, Copy)]
pub struct RuntimeFilterBindingFactsView<'a> {
    prepared: &'a PreparedFragmentSet,
}

impl<'a> RuntimeFilterBindingFactsView<'a> {
    pub(crate) const fn new(prepared: &'a PreparedFragmentSet) -> Self {
        Self { prepared }
    }

    pub fn fragments(
        self,
    ) -> impl ExactSizeIterator<Item = RuntimeFilterBindingFragmentFactsView<'a>> + 'a {
        self.prepared
            .scheduling_view()
            .fragments()
            .map(|fragment| RuntimeFilterBindingFragmentFactsView { fragment })
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterBindingFragmentFactsView<'a> {
    fragment: &'a super::projection::PreparedFragment,
}

impl<'a> RuntimeFilterBindingFragmentFactsView<'a> {
    pub fn fragment_id(self) -> u32 {
        self.fragment.fragment_id()
    }

    pub fn bindings(self) -> impl ExactSizeIterator<Item = RuntimeFilterBindingFacts<'a>> + 'a {
        self.fragment
            .runtime_filter_bindings()
            .iter()
            .map(|binding| RuntimeFilterBindingFacts { binding })
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterBindingFacts<'a> {
    binding: &'a sql_facts::SqlRuntimeFilterBindingFacts,
}

impl<'a> RuntimeFilterBindingFacts<'a> {
    pub fn binding_id(self) -> u32 {
        self.binding.binding_id
    }

    pub fn channel_id(self) -> u32 {
        self.binding.channel_id
    }

    pub fn node_id(self) -> i32 {
        self.binding.node_id
    }

    pub fn apply_point(self) -> RuntimeFilterApplyPoint {
        match self.binding.apply_point {
            sql_facts::SqlRuntimeFilterApplyPoint::NodeInput => RuntimeFilterApplyPoint::NodeInput,
            sql_facts::SqlRuntimeFilterApplyPoint::NodeOutput => {
                RuntimeFilterApplyPoint::NodeOutput
            }
        }
    }

    /// The Frontend owns the generic TypedExpr-to-wire mapping.  Core exposes
    /// only the sealed typed fact and never an encoder context.
    pub fn expression(self) -> &'a TypedExpr {
        &self.binding.expression
    }

    pub fn logical_domain(self) -> RuntimeFilterLogicalDomainFacts {
        RuntimeFilterLogicalDomainFacts::from_sql(&self.binding.logical_domain)
    }

    pub fn reduction(self) -> RuntimeFilterReductionFacts {
        RuntimeFilterReductionFacts::from_sql(self.binding.reduction)
    }

    pub fn role(self) -> RuntimeFilterBindingRoleFacts {
        match &self.binding.role {
            sql_facts::SqlRuntimeFilterBindingRoleFacts::Producer {
                contribution_kinds,
                completion_requirement,
                target,
            } => RuntimeFilterBindingRoleFacts::Producer {
                contribution_kinds: contribution_kinds
                    .iter()
                    .copied()
                    .map(RuntimeFilterContributionKind::from_sql)
                    .collect(),
                completion_requirement: RuntimeFilterCompletionRequirement::from_sql(
                    *completion_requirement,
                ),
                target: RuntimeFilterProducerTarget::from_sql(*target),
            },
            sql_facts::SqlRuntimeFilterBindingRoleFacts::Consumer {
                capabilities,
                activation,
                target,
            } => RuntimeFilterBindingRoleFacts::Consumer {
                capabilities: capabilities
                    .iter()
                    .copied()
                    .map(RuntimeFilterArtifactCapability::from_sql)
                    .collect(),
                activation: RuntimeFilterConsumerActivation::from_sql(*activation),
                target: RuntimeFilterConsumerTarget::from_sql(target),
            },
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterApplyPoint {
    NodeInput,
    NodeOutput,
}

pub enum RuntimeFilterLogicalDomainFacts {
    Membership {
        value_type: DataType,
        null_semantics: RuntimeFilterNullSemantics,
    },
    Ordered {
        keys: Vec<RuntimeFilterOrderKeyFacts>,
        inclusive: bool,
        comparator_digest: [u8; 32],
    },
}

impl RuntimeFilterLogicalDomainFacts {
    fn from_sql(value: &sql_facts::SqlRuntimeFilterLogicalDomainFacts) -> Self {
        match value {
            sql_facts::SqlRuntimeFilterLogicalDomainFacts::Membership {
                value_type,
                null_semantics,
            } => Self::Membership {
                value_type: value_type.clone(),
                null_semantics: RuntimeFilterNullSemantics::from_sql(*null_semantics),
            },
            sql_facts::SqlRuntimeFilterLogicalDomainFacts::Ordered {
                keys,
                inclusive,
                comparator_digest,
            } => Self::Ordered {
                keys: keys
                    .iter()
                    .map(|key| RuntimeFilterOrderKeyFacts {
                        data_type: key.data_type.clone(),
                        direction: RuntimeFilterSortDirection::from_sql(key.direction),
                        null_order: RuntimeFilterNullOrder::from_sql(key.null_order),
                    })
                    .collect(),
                inclusive: *inclusive,
                comparator_digest: *comparator_digest,
            },
        }
    }
}

pub struct RuntimeFilterOrderKeyFacts {
    pub data_type: DataType,
    pub direction: RuntimeFilterSortDirection,
    pub null_order: RuntimeFilterNullOrder,
}

pub enum RuntimeFilterReductionFacts {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: u32 },
}

impl RuntimeFilterReductionFacts {
    fn from_sql(value: sql_facts::SqlRuntimeFilterReductionFacts) -> Self {
        match value {
            sql_facts::SqlRuntimeFilterReductionFacts::SetUnion => Self::SetUnion,
            sql_facts::SqlRuntimeFilterReductionFacts::TightenOrderedBound => {
                Self::TightenOrderedBound
            }
            sql_facts::SqlRuntimeFilterReductionFacts::MergeTopKSummary { k } => {
                Self::MergeTopKSummary { k }
            }
        }
    }
}

pub enum RuntimeFilterBindingRoleFacts {
    Producer {
        contribution_kinds: Vec<RuntimeFilterContributionKind>,
        completion_requirement: RuntimeFilterCompletionRequirement,
        target: RuntimeFilterProducerTarget,
    },
    Consumer {
        capabilities: Vec<RuntimeFilterArtifactCapability>,
        activation: RuntimeFilterConsumerActivation,
        target: RuntimeFilterConsumerTarget,
    },
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
    fn from_sql(value: sql_facts::SqlRuntimeFilterContributionKind) -> Self {
        use sql_facts::SqlRuntimeFilterContributionKind as ContributionKind;
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
    fn from_sql(value: sql_facts::SqlRuntimeFilterCompletionRequirement) -> Self {
        use sql_facts::SqlRuntimeFilterCompletionRequirement as CompletionRequirement;
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
    fn from_sql(value: sql_facts::SqlRuntimeFilterArtifactCapability) -> Self {
        use sql_facts::SqlRuntimeFilterArtifactCapability as ArtifactCapability;
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
    fn from_sql(value: sql_facts::SqlRuntimeFilterConsumerActivation) -> Self {
        use sql_facts::{
            SqlRuntimeFilterConsumerActivation as ConsumerActivation,
            SqlRuntimeFilterLateApplyGranularity as LateApplyGranularity,
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
pub enum RuntimeFilterProducerTarget {
    JoinBuildKey { ordinal: u32 },
    AggregateTopNKey { group_key_ordinal: u32, limit: u32 },
}

impl RuntimeFilterProducerTarget {
    fn from_sql(value: sql_facts::SqlRuntimeFilterProducerTarget) -> Self {
        match value {
            sql_facts::SqlRuntimeFilterProducerTarget::JoinBuildKey { ordinal } => {
                Self::JoinBuildKey { ordinal }
            }
            sql_facts::SqlRuntimeFilterProducerTarget::AggregateTopNKey {
                group_key_ordinal,
                limit,
            } => Self::AggregateTopNKey {
                group_key_ordinal,
                limit,
            },
        }
    }
}

#[derive(Clone)]
pub enum RuntimeFilterConsumerTarget {
    DirectInputOrdinal(u32),
    SourceBoundary {
        scan_domain_target: Option<RuntimeFilterScanDomainTarget>,
    },
}

impl RuntimeFilterConsumerTarget {
    fn from_sql(value: &sql_facts::SqlRuntimeFilterConsumerTarget) -> Self {
        match value {
            sql_facts::SqlRuntimeFilterConsumerTarget::DirectInput { input_ordinal } => {
                Self::DirectInputOrdinal(*input_ordinal)
            }
            sql_facts::SqlRuntimeFilterConsumerTarget::SourceBoundary { scan_domain } => {
                Self::SourceBoundary {
                    scan_domain_target: scan_domain.as_ref().map(|target| {
                        RuntimeFilterScanDomainTarget {
                            field_ordinal: target.field_ordinal,
                            data_type: target.data_type.clone(),
                            nullable: target.nullable,
                        }
                    }),
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct RuntimeFilterScanDomainTarget {
    pub field_ordinal: u32,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterSortDirection {
    Ascending,
    Descending,
}

impl RuntimeFilterSortDirection {
    fn from_sql(value: sql_facts::SqlRuntimeFilterSortDirection) -> Self {
        match value {
            sql_facts::SqlRuntimeFilterSortDirection::Ascending => Self::Ascending,
            sql_facts::SqlRuntimeFilterSortDirection::Descending => Self::Descending,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterNullOrder {
    First,
    Last,
}

impl RuntimeFilterNullOrder {
    fn from_sql(value: sql_facts::SqlRuntimeFilterNullOrder) -> Self {
        match value {
            sql_facts::SqlRuntimeFilterNullOrder::First => Self::First,
            sql_facts::SqlRuntimeFilterNullOrder::Last => Self::Last,
        }
    }
}

/// Borrow-only deployment facts projected from the sealed SQL plan and the
/// already validated schedule. This is intentionally not a graph facade:
/// every public result is a narrow immutable fact value.
#[derive(Clone, Copy)]
pub struct RuntimeFilterDeploymentFactsView<'a> {
    prepared: &'a PreparedFragmentSet,
    schedule: &'a SchedulingPlan,
}

impl<'a> RuntimeFilterDeploymentFactsView<'a> {
    pub(crate) const fn new(
        prepared: &'a PreparedFragmentSet,
        schedule: &'a SchedulingPlan,
    ) -> Self {
        Self { prepared, schedule }
    }

    pub fn channels(self) -> impl Iterator<Item = RuntimeFilterChannelDeploymentFacts<'a>> + 'a {
        self.prepared
            .runtime_filter_facts()
            .channels()
            .iter()
            .map(|channel| RuntimeFilterChannelDeploymentFacts { channel })
    }

    pub fn bindings(self) -> impl Iterator<Item = RuntimeFilterDeploymentBindingFacts<'a>> + 'a {
        self.prepared
            .runtime_filter_facts()
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
                instance_index: placement.instance_index,
                fragment_instance_id: placement.finst_id,
                backend_idx: placement.backend_idx,
            })
    }

    pub fn fragment_edges(
        self,
    ) -> impl ExactSizeIterator<Item = RuntimeFilterFragmentEdgeFacts> + 'a {
        self.prepared
            .scheduling_view()
            .edges()
            .iter()
            .map(RuntimeFilterFragmentEdgeFacts::from_fragment_edge)
    }

    /// Each producer tuple has at most one sealed proof or skip provenance.
    /// The source bindings are BTreeMap ordered, so this iterator is stable.
    pub fn join_progress(self) -> impl Iterator<Item = RuntimeFilterJoinProgressFacts> + 'a {
        self.prepared
            .runtime_filter_facts()
            .join_progress()
            .iter()
            .map(RuntimeFilterJoinProgressFacts::from_sql)
    }
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterChannelDeploymentFacts<'a> {
    channel: &'a sql_facts::SqlRuntimeFilterChannelFacts,
}

impl RuntimeFilterChannelDeploymentFacts<'_> {
    pub fn channel_id(self) -> u32 {
        self.channel.channel_id
    }

    pub fn logical_domain(self) -> RuntimeFilterLogicalDomainFacts {
        RuntimeFilterLogicalDomainFacts::from_sql(&self.channel.logical_domain)
    }

    pub fn lifecycle(self) -> RuntimeFilterDeploymentLifecycleFacts {
        match self.channel.lifecycle {
            sql_facts::SqlRuntimeFilterLifecycleFacts::CompleteOnce => {
                RuntimeFilterDeploymentLifecycleFacts::CompleteOnce
            }
            sql_facts::SqlRuntimeFilterLifecycleFacts::MonotonicUpdates => {
                RuntimeFilterDeploymentLifecycleFacts::MonotonicUpdates
            }
        }
    }

    pub fn availability_coverage(self) -> RuntimeFilterCoverageFacts {
        RuntimeFilterCoverageFacts::from_sql(&self.channel.availability_coverage)
    }

    pub fn terminal_coverage(self) -> RuntimeFilterCoverageFacts {
        RuntimeFilterCoverageFacts::from_sql(&self.channel.terminal_coverage)
    }

    pub fn reduction(self) -> RuntimeFilterReductionFacts {
        RuntimeFilterReductionFacts::from_sql(self.channel.reduction)
    }

    pub fn allowed_contribution_kinds(self) -> Vec<RuntimeFilterContributionKind> {
        self.channel
            .allowed_contribution_kinds
            .iter()
            .copied()
            .map(RuntimeFilterContributionKind::from_sql)
            .collect()
    }

    pub fn required_consumer_capabilities(self) -> Vec<RuntimeFilterArtifactCapability> {
        self.channel
            .required_consumer_capabilities
            .iter()
            .copied()
            .map(RuntimeFilterArtifactCapability::from_sql)
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
    fn from_sql(value: sql_facts::SqlRuntimeFilterNullSemantics) -> Self {
        match value {
            sql_facts::SqlRuntimeFilterNullSemantics::NeverMatches => Self::NeverMatches,
            sql_facts::SqlRuntimeFilterNullSemantics::NullSafeEqual => Self::NullSafeEqual,
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
    fn from_sql(coverage: &sql_facts::SqlRuntimeFilterCoverageFacts) -> Self {
        match coverage {
            sql_facts::SqlRuntimeFilterCoverageFacts::LeafWitnessId(witness) => {
                Self::LeafWitnessId(*witness)
            }
            sql_facts::SqlRuntimeFilterCoverageFacts::AllOf(children) => {
                Self::AllOf(children.iter().map(Self::from_sql).collect())
            }
            sql_facts::SqlRuntimeFilterCoverageFacts::AnyOf(children) => {
                Self::AnyOf(children.iter().map(Self::from_sql).collect())
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
    binding: &'a sql_facts::SqlRuntimeFilterDeploymentBindingFacts,
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
            sql_facts::SqlRuntimeFilterBindingRoleFacts::Producer {
                contribution_kinds,
                completion_requirement,
                target,
            } => RuntimeFilterDeploymentBindingRoleFacts::Producer {
                contribution_kinds: contribution_kinds
                    .iter()
                    .copied()
                    .map(RuntimeFilterContributionKind::from_sql)
                    .collect(),
                completion_requirement: RuntimeFilterCompletionRequirement::from_sql(
                    *completion_requirement,
                ),
                target: RuntimeFilterProducerTarget::from_sql(*target),
            },
            sql_facts::SqlRuntimeFilterBindingRoleFacts::Consumer {
                capabilities,
                activation,
                target,
            } => RuntimeFilterDeploymentBindingRoleFacts::Consumer {
                capabilities: capabilities
                    .iter()
                    .copied()
                    .map(RuntimeFilterArtifactCapability::from_sql)
                    .collect(),
                activation: RuntimeFilterConsumerActivation::from_sql(*activation),
                target: RuntimeFilterConsumerTarget::from_sql(target),
            },
        }
    }
}

pub enum RuntimeFilterDeploymentBindingRoleFacts {
    Producer {
        contribution_kinds: Vec<RuntimeFilterContributionKind>,
        completion_requirement: RuntimeFilterCompletionRequirement,
        target: RuntimeFilterProducerTarget,
    },
    Consumer {
        capabilities: Vec<RuntimeFilterArtifactCapability>,
        activation: RuntimeFilterConsumerActivation,
        target: RuntimeFilterConsumerTarget,
    },
}

#[derive(Clone, Copy)]
pub struct RuntimeFilterValidatedPlacementFacts {
    fragment_id: u32,
    instance_index: usize,
    fragment_instance_id: crate::common::types::UniqueId,
    backend_idx: usize,
}

impl RuntimeFilterValidatedPlacementFacts {
    pub const fn fragment_id(self) -> u32 {
        self.fragment_id
    }

    pub const fn instance_index(self) -> usize {
        self.instance_index
    }

    pub const fn fragment_instance_id(self) -> crate::common::types::UniqueId {
        self.fragment_instance_id
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
    fn from_fragment_edge(edge: &FragmentEdge) -> Self {
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
    Skipped {
        channel_id: u32,
        producer_binding_id: u32,
        producer_fragment_id: u32,
        join_node_id: i32,
        reason: RuntimeFilterJoinProgressSkipReason,
    },
}

impl RuntimeFilterJoinProgressFacts {
    fn from_sql(value: &sql_facts::SqlRuntimeFilterJoinProgressFacts) -> Self {
        match value {
            sql_facts::SqlRuntimeFilterJoinProgressFacts::Proven {
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
                    .map(RuntimeFilterFrontierEdgeFacts::from_sql)
                    .collect(),
                non_build_inputs: non_build_inputs
                    .iter()
                    .map(RuntimeFilterFrontierEdgeFacts::from_sql)
                    .collect(),
            },
            sql_facts::SqlRuntimeFilterJoinProgressFacts::Skipped {
                channel_id,
                producer_binding_id,
                producer_fragment_id,
                join_node_id,
                reason,
            } => Self::Skipped {
                channel_id: *channel_id,
                producer_binding_id: *producer_binding_id,
                producer_fragment_id: *producer_fragment_id,
                join_node_id: *join_node_id,
                reason: RuntimeFilterJoinProgressSkipReason::from_sql(*reason),
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
    fn from_sql(edge: &sql_facts::SqlRuntimeFilterFrontierEdgeFacts) -> Self {
        Self {
            source_fragment_id: edge.source_fragment_id,
            target_exchange_node_id: edge.target_exchange_node_id,
        }
    }
}

#[derive(Clone, Copy)]
pub enum RuntimeFilterJoinProgressSkipReason {
    NoRfSides,
    MissingChild,
    UnauditedNode { node_id: i32 },
}

impl RuntimeFilterJoinProgressSkipReason {
    fn from_sql(value: sql_facts::SqlRuntimeFilterJoinProgressSkipReason) -> Self {
        match value {
            sql_facts::SqlRuntimeFilterJoinProgressSkipReason::NoRfSides => Self::NoRfSides,
            sql_facts::SqlRuntimeFilterJoinProgressSkipReason::MissingChild => Self::MissingChild,
            sql_facts::SqlRuntimeFilterJoinProgressSkipReason::UnauditedNode { node_id } => {
                Self::UnauditedNode { node_id }
            }
        }
    }
}
