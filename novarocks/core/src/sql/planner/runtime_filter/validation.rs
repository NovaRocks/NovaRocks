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

use std::collections::BTreeSet;
use std::fmt;

use super::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
    ConsumerActivation, ContributionKind, CoverageWitnessId, NullSemantics, PlanNodeId,
    ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
    RuntimeFilterPolicyRequirement,
};
use super::coverage::CoverageShapeError;
use super::graph::{
    ApplyPoint, ConsumerRequirementData, ProducerRequirement, RuntimeFilterBindingRoleData,
    RuntimeFilterBindingSpecData, RuntimeFilterChannelSpec, RuntimeFilterGraphData,
};
use super::policy::{RuntimeFilterPolicyValidationError, validate_runtime_filter_policy};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyField {
    MaxContributionBytes,
    MaxArtifactBytes,
    DeadlineMs,
    MaxRetries,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphValidationErrorKind {
    ChannelIdMismatch {
        map_key: ChannelId,
        object_id: ChannelId,
    },
    AvailabilityCoverageShape(CoverageShapeError),
    TerminalCoverageShape(CoverageShapeError),
    ZeroPolicyValue(PolicyField),
    ContributionBytesExceedArtifactBytes,
    ArtifactBytesExceedLimit,
    DeadlineExceedsLimit,
    RetriesExceedLimit,
    CompleteOnceCoverageMismatch,
    EmptyOrderContract,
    DomainLifecycleMismatch,
    ProducerClosedContributionMissing,
    MembershipReductionMismatch,
    MembershipContributionMissing,
    MembershipContributionMismatch,
    FinalDomainShardRequiresNullSafeEqual,
    FencedFinalDomainCoverageMismatch,
    TopKSummaryCoverageMismatch,
    TopKSummaryMissingCoverageWitness(CoverageWitnessId),
    TopKSummaryConsumerCapabilityMismatch,
    OrderedBoundReductionMismatch,
    OrderedBoundContributionMissing,
    OrderedBoundContributionMismatch(ContributionKind),
    BindingIdMismatch {
        map_key: BindingId,
        object_id: BindingId,
    },
    InvalidPlanNodeId(PlanNodeId),
    RoleApplyPointMismatch,
    UnknownChannel,
    ProducerCoverageWitnessMissing,
    MissingProducer,
    MissingConsumer,
    DuplicateCoverageWitness(CoverageWitnessId),
    EmptyProducerContributions,
    UnsupportedProducerContribution(ContributionKind),
    RequiredProducerContributionMissing(ContributionKind),
    ProducerCompletionMismatch {
        expected: CompletionRequirement,
        actual: CompletionRequirement,
    },
    UnknownCoverageWitness(CoverageWitnessId),
    ConsumerOwnedCoverageWitness(CoverageWitnessId),
    UnsupportedConsumerCapability(ArtifactCapability),
    BlockingFeedbackConsumer,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphValidationError {
    pub channel_id: Option<ChannelId>,
    pub binding_id: Option<BindingId>,
    pub kind: GraphValidationErrorKind,
}

impl fmt::Display for GraphValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "runtime filter graph validation failed:")?;
        if let Some(channel_id) = self.channel_id {
            write!(formatter, " channel {channel_id:?},")?;
        }
        if let Some(binding_id) = self.binding_id {
            write!(formatter, " binding {binding_id:?},")?;
        }
        write!(formatter, " {:?}", self.kind)
    }
}

impl std::error::Error for GraphValidationError {}

pub trait ActivationContract {
    fn satisfies_required_non_blocking(&self) -> bool;
}

impl ActivationContract for ConsumerActivation {
    fn satisfies_required_non_blocking(&self) -> bool {
        !matches!(self, Self::BlockingSnapshot)
    }
}

impl<A: ActivationContract> RuntimeFilterGraphData<A> {
    pub fn validate(&self) -> Result<(), GraphValidationError> {
        self.validate_channels()?;
        self.validate_bindings()?;
        self.validate_channel_relationships()
    }

    fn validate_channels(&self) -> Result<(), GraphValidationError> {
        for (map_key, channel) in self.channel_entries() {
            let channel_id = channel.channel_id;
            if *map_key != channel_id {
                return Err(channel_error(
                    *map_key,
                    GraphValidationErrorKind::ChannelIdMismatch {
                        map_key: *map_key,
                        object_id: channel_id,
                    },
                ));
            }
            if let Err(error) = channel.availability_coverage.validate_shape() {
                return Err(channel_error(
                    channel_id,
                    GraphValidationErrorKind::AvailabilityCoverageShape(error),
                ));
            }
            if let Err(error) = channel.terminal_coverage.validate_shape() {
                return Err(channel_error(
                    channel_id,
                    GraphValidationErrorKind::TerminalCoverageShape(error),
                ));
            }
            validate_policy(channel_id, channel.policy)?;
            validate_channel_matrix(channel)?;
        }
        Ok(())
    }

    fn validate_bindings(&self) -> Result<(), GraphValidationError> {
        for (map_key, binding) in self.binding_entries() {
            if *map_key != binding.binding_id {
                return Err(binding_error(
                    binding,
                    GraphValidationErrorKind::BindingIdMismatch {
                        map_key: *map_key,
                        object_id: binding.binding_id,
                    },
                ));
            }
            if binding.location.node_id.get() < 0 {
                return Err(binding_error(
                    binding,
                    GraphValidationErrorKind::InvalidPlanNodeId(binding.location.node_id),
                ));
            }
            let apply_point_matches = matches!(
                (&binding.role, binding.apply_point),
                (
                    RuntimeFilterBindingRoleData::Producer(_),
                    ApplyPoint::NodeOutput
                ) | (
                    RuntimeFilterBindingRoleData::Consumer(_),
                    ApplyPoint::NodeInput
                )
            );
            if !apply_point_matches {
                return Err(binding_error(
                    binding,
                    GraphValidationErrorKind::RoleApplyPointMismatch,
                ));
            }
            match (&binding.role, binding.coverage_witness_id) {
                (RuntimeFilterBindingRoleData::Producer(_), None) => {
                    return Err(binding_error(
                        binding,
                        GraphValidationErrorKind::ProducerCoverageWitnessMissing,
                    ));
                }
                (RuntimeFilterBindingRoleData::Consumer(_), Some(witness_id)) => {
                    return Err(binding_error(
                        binding,
                        GraphValidationErrorKind::ConsumerOwnedCoverageWitness(witness_id),
                    ));
                }
                _ => {}
            }
            if self.channel(binding.channel_id).is_none() {
                return Err(binding_error(
                    binding,
                    GraphValidationErrorKind::UnknownChannel,
                ));
            }
        }
        Ok(())
    }

    fn validate_channel_relationships(&self) -> Result<(), GraphValidationError> {
        for channel in self.channels() {
            let bindings = self
                .bindings()
                .filter(|binding| binding.channel_id == channel.channel_id)
                .collect::<Vec<_>>();
            let producers = bindings
                .iter()
                .filter_map(|binding| match &binding.role {
                    RuntimeFilterBindingRoleData::Producer(requirement) => {
                        Some((*binding, requirement))
                    }
                    RuntimeFilterBindingRoleData::Consumer(_) => None,
                })
                .collect::<Vec<_>>();
            let consumers = bindings
                .iter()
                .filter_map(|binding| match &binding.role {
                    RuntimeFilterBindingRoleData::Consumer(requirement) => {
                        Some((*binding, requirement))
                    }
                    RuntimeFilterBindingRoleData::Producer(_) => None,
                })
                .collect::<Vec<_>>();

            if producers.is_empty() {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::MissingProducer,
                ));
            }
            if consumers.is_empty() {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::MissingConsumer,
                ));
            }

            let mut producer_witnesses = BTreeSet::new();
            for (binding, _) in &producers {
                let witness_id = binding
                    .coverage_witness_id
                    .expect("producer witness presence was validated before channel relationships");
                if !producer_witnesses.insert(witness_id) {
                    return Err(binding_error(
                        binding,
                        GraphValidationErrorKind::DuplicateCoverageWitness(witness_id),
                    ));
                }
            }
            for (binding, requirement) in &producers {
                validate_producer(channel, binding, requirement)?;
            }

            validate_coverage_ownership(channel, &producer_witnesses)?;

            for (binding, requirement) in consumers {
                validate_consumer(channel, binding, requirement)?;
            }
        }
        Ok(())
    }
}

fn validate_policy(
    channel_id: ChannelId,
    policy: RuntimeFilterPolicyRequirement,
) -> Result<(), GraphValidationError> {
    validate_runtime_filter_policy(policy).map_err(|error| {
        let kind = match error {
            RuntimeFilterPolicyValidationError::ZeroMaxContributionBytes => {
                GraphValidationErrorKind::ZeroPolicyValue(PolicyField::MaxContributionBytes)
            }
            RuntimeFilterPolicyValidationError::ZeroMaxArtifactBytes => {
                GraphValidationErrorKind::ZeroPolicyValue(PolicyField::MaxArtifactBytes)
            }
            RuntimeFilterPolicyValidationError::ZeroDeadlineMs => {
                GraphValidationErrorKind::ZeroPolicyValue(PolicyField::DeadlineMs)
            }
            RuntimeFilterPolicyValidationError::ZeroMaxRetries => {
                GraphValidationErrorKind::ZeroPolicyValue(PolicyField::MaxRetries)
            }
            RuntimeFilterPolicyValidationError::ContributionBytesExceedArtifactBytes => {
                GraphValidationErrorKind::ContributionBytesExceedArtifactBytes
            }
            RuntimeFilterPolicyValidationError::ArtifactBytesExceedLimit => {
                GraphValidationErrorKind::ArtifactBytesExceedLimit
            }
            RuntimeFilterPolicyValidationError::DeadlineExceedsLimit => {
                GraphValidationErrorKind::DeadlineExceedsLimit
            }
            RuntimeFilterPolicyValidationError::RetriesExceedLimit => {
                GraphValidationErrorKind::RetriesExceedLimit
            }
        };
        channel_error(channel_id, kind)
    })
}

fn validate_channel_matrix(channel: &RuntimeFilterChannelSpec) -> Result<(), GraphValidationError> {
    if channel.lifecycle == RuntimeFilterLifecycle::CompleteOnce
        && !channel
            .availability_coverage
            .is_canonically_equivalent_to(&channel.terminal_coverage)
    {
        return Err(channel_error(
            channel.channel_id,
            GraphValidationErrorKind::CompleteOnceCoverageMismatch,
        ));
    }

    if !channel
        .allowed_contribution_kinds
        .contains(&ContributionKind::ProducerClosed)
    {
        return Err(channel_error(
            channel.channel_id,
            GraphValidationErrorKind::ProducerClosedContributionMissing,
        ));
    }

    match &channel.logical_domain {
        RuntimeFilterLogicalDomain::Membership { null_semantics, .. } => {
            if channel.lifecycle != RuntimeFilterLifecycle::CompleteOnce {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::DomainLifecycleMismatch,
                ));
            }
            if channel.reduction_requirement != ReductionRequirement::SetUnion {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::MembershipReductionMismatch,
                ));
            }
            let has_value_delta = channel
                .allowed_contribution_kinds
                .contains(&ContributionKind::ValueDomainDelta);
            let has_final_shard = channel
                .allowed_contribution_kinds
                .contains(&ContributionKind::FinalDomainShard);
            if has_final_shard && *null_semantics != NullSemantics::NullSafeEqual {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::FinalDomainShardRequiresNullSafeEqual,
                ));
            }
            if has_final_shard && !channel.availability_coverage.is_all_of_only() {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::FencedFinalDomainCoverageMismatch,
                ));
            }
            if !has_value_delta && !has_final_shard {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::MembershipContributionMissing,
                ));
            }
            if has_value_delta && has_final_shard
                || channel
                    .allowed_contribution_kinds
                    .contains(&ContributionKind::OrderedBoundUpdate)
                || channel
                    .allowed_contribution_kinds
                    .contains(&ContributionKind::TopKSummary)
            {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::MembershipContributionMismatch,
                ));
            }
        }
        RuntimeFilterLogicalDomain::OrderedBound(order_contract) => {
            if order_contract.keys.is_empty() {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::EmptyOrderContract,
                ));
            }
            if channel.lifecycle != RuntimeFilterLifecycle::MonotonicUpdates {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::DomainLifecycleMismatch,
                ));
            }
            let required_contribution = match channel.reduction_requirement {
                ReductionRequirement::TightenOrderedBound => {
                    let expected = BTreeSet::from([
                        ContributionKind::OrderedBoundUpdate,
                        ContributionKind::ProducerClosed,
                    ]);
                    if let Some(forbidden) = channel
                        .allowed_contribution_kinds
                        .difference(&expected)
                        .next()
                    {
                        return Err(channel_error(
                            channel.channel_id,
                            GraphValidationErrorKind::OrderedBoundContributionMismatch(*forbidden),
                        ));
                    }
                    ContributionKind::OrderedBoundUpdate
                }
                ReductionRequirement::MergeTopKSummary(_) => {
                    if !channel.availability_coverage.is_all_of_only()
                        || !channel
                            .availability_coverage
                            .is_canonically_equivalent_to(&channel.terminal_coverage)
                    {
                        return Err(channel_error(
                            channel.channel_id,
                            GraphValidationErrorKind::TopKSummaryCoverageMismatch,
                        ));
                    }
                    if channel.required_consumer_capabilities
                        != BTreeSet::from([ArtifactCapability::OrderedRange])
                    {
                        return Err(channel_error(
                            channel.channel_id,
                            GraphValidationErrorKind::TopKSummaryConsumerCapabilityMismatch,
                        ));
                    }
                    let expected = BTreeSet::from([
                        ContributionKind::TopKSummary,
                        ContributionKind::ProducerClosed,
                    ]);
                    if let Some(forbidden) = channel
                        .allowed_contribution_kinds
                        .difference(&expected)
                        .next()
                    {
                        return Err(channel_error(
                            channel.channel_id,
                            GraphValidationErrorKind::OrderedBoundContributionMismatch(*forbidden),
                        ));
                    }
                    ContributionKind::TopKSummary
                }
                ReductionRequirement::SetUnion => {
                    return Err(channel_error(
                        channel.channel_id,
                        GraphValidationErrorKind::OrderedBoundReductionMismatch,
                    ));
                }
            };
            if let Some(forbidden) = [
                ContributionKind::ValueDomainDelta,
                ContributionKind::FinalDomainShard,
            ]
            .into_iter()
            .find(|kind| channel.allowed_contribution_kinds.contains(kind))
            {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::OrderedBoundContributionMismatch(forbidden),
                ));
            }
            if !channel
                .allowed_contribution_kinds
                .contains(&required_contribution)
            {
                return Err(channel_error(
                    channel.channel_id,
                    GraphValidationErrorKind::OrderedBoundContributionMissing,
                ));
            }
        }
    }
    Ok(())
}

fn validate_producer(
    channel: &RuntimeFilterChannelSpec,
    binding: &RuntimeFilterBindingSpecData<impl ActivationContract>,
    requirement: &ProducerRequirement,
) -> Result<(), GraphValidationError> {
    if requirement.contribution_kinds.is_empty() {
        return Err(binding_error(
            binding,
            GraphValidationErrorKind::EmptyProducerContributions,
        ));
    }
    if let Some(unsupported) = requirement
        .contribution_kinds
        .difference(&channel.allowed_contribution_kinds)
        .next()
    {
        return Err(binding_error(
            binding,
            GraphValidationErrorKind::UnsupportedProducerContribution(*unsupported),
        ));
    }

    let required_contributions = match &channel.logical_domain {
        RuntimeFilterLogicalDomain::Membership { .. }
            if channel
                .allowed_contribution_kinds
                .contains(&ContributionKind::FinalDomainShard) =>
        {
            BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ])
        }
        RuntimeFilterLogicalDomain::Membership { .. } => BTreeSet::from([
            ContributionKind::ValueDomainDelta,
            ContributionKind::ProducerClosed,
        ]),
        RuntimeFilterLogicalDomain::OrderedBound(_) => {
            let reduction_contribution = match channel.reduction_requirement {
                ReductionRequirement::TightenOrderedBound => ContributionKind::OrderedBoundUpdate,
                ReductionRequirement::MergeTopKSummary(_) => ContributionKind::TopKSummary,
                ReductionRequirement::SetUnion => unreachable!(
                    "ordered-bound reduction was validated before producer relationships"
                ),
            };
            BTreeSet::from([reduction_contribution, ContributionKind::ProducerClosed])
        }
    };
    if let Some(missing) = required_contributions
        .difference(&requirement.contribution_kinds)
        .next()
    {
        return Err(binding_error(
            binding,
            GraphValidationErrorKind::RequiredProducerContributionMissing(*missing),
        ));
    }

    let expected_completion = if channel
        .allowed_contribution_kinds
        .contains(&ContributionKind::FinalDomainShard)
    {
        CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen)
    } else {
        CompletionRequirement::ProducerClosed
    };
    if requirement.completion_requirement != expected_completion {
        return Err(binding_error(
            binding,
            GraphValidationErrorKind::ProducerCompletionMismatch {
                expected: expected_completion,
                actual: requirement.completion_requirement,
            },
        ));
    }
    Ok(())
}

fn validate_coverage_ownership(
    channel: &RuntimeFilterChannelSpec,
    producer_witnesses: &BTreeSet<CoverageWitnessId>,
) -> Result<(), GraphValidationError> {
    let mut witness_ids = BTreeSet::new();
    for coverage in [&channel.availability_coverage, &channel.terminal_coverage] {
        witness_ids.extend(coverage.witness_ids_in_order());
    }
    if let Some(witness_id) = witness_ids.difference(producer_witnesses).next() {
        return Err(channel_error(
            channel.channel_id,
            GraphValidationErrorKind::UnknownCoverageWitness(*witness_id),
        ));
    }
    if matches!(
        channel.reduction_requirement,
        ReductionRequirement::MergeTopKSummary(_)
    ) && let Some(witness_id) = producer_witnesses.difference(&witness_ids).next()
    {
        return Err(channel_error(
            channel.channel_id,
            GraphValidationErrorKind::TopKSummaryMissingCoverageWitness(*witness_id),
        ));
    }
    Ok(())
}

fn validate_consumer<A: ActivationContract>(
    channel: &RuntimeFilterChannelSpec,
    binding: &RuntimeFilterBindingSpecData<A>,
    requirement: &ConsumerRequirementData<A>,
) -> Result<(), GraphValidationError> {
    if matches!(
        channel.reduction_requirement,
        ReductionRequirement::MergeTopKSummary(_)
    ) && requirement.capabilities != BTreeSet::from([ArtifactCapability::OrderedRange])
    {
        return Err(binding_error(
            binding,
            GraphValidationErrorKind::TopKSummaryConsumerCapabilityMismatch,
        ));
    }
    if let Some(missing) = channel
        .required_consumer_capabilities
        .difference(&requirement.capabilities)
        .next()
    {
        return Err(binding_error(
            binding,
            GraphValidationErrorKind::UnsupportedConsumerCapability(*missing),
        ));
    }

    let requires_non_blocking = matches!(
        channel.logical_domain,
        RuntimeFilterLogicalDomain::OrderedBound(_)
    ) || channel
        .allowed_contribution_kinds
        .contains(&ContributionKind::FinalDomainShard);
    if requires_non_blocking && !requirement.activation.satisfies_required_non_blocking() {
        return Err(binding_error(
            binding,
            GraphValidationErrorKind::BlockingFeedbackConsumer,
        ));
    }
    Ok(())
}

fn channel_error(channel_id: ChannelId, kind: GraphValidationErrorKind) -> GraphValidationError {
    GraphValidationError {
        channel_id: Some(channel_id),
        binding_id: None,
        kind,
    }
}

fn binding_error(
    binding: &RuntimeFilterBindingSpecData<impl ActivationContract>,
    kind: GraphValidationErrorKind,
) -> GraphValidationError {
    GraphValidationError {
        channel_id: Some(binding.channel_id),
        binding_id: Some(binding.binding_id),
        kind,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;

    use super::super::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
        ConsumerActivation, ContributionKind, CoverageWitnessId, LateApplyGranularity,
        NullSemantics, PlanFragmentId, PlanNodeId, ReductionRequirement,
        RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement, TopKSummaryRequirement,
    };
    use super::super::coverage::{Coverage, CoverageShapeError};
    use super::super::graph::tests::*;
    use super::super::graph::{
        ProducerBindingTarget, RuntimeFilterBindingRole, RuntimeFilterGraph,
    };
    use super::super::policy::{MAX_ARTIFACT_BYTES, MAX_DEADLINE_MS, MAX_RETRIES};
    use super::*;

    fn join_graph() -> RuntimeFilterGraph {
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_channel(join_channel(ChannelId::new(1)))
            .unwrap();
        graph
            .insert_binding(join_producer_binding(
                BindingId::new(1),
                ChannelId::new(1),
                CoverageWitnessId::new(1),
            ))
            .unwrap();
        graph
            .insert_binding(join_consumer_binding(BindingId::new(2), ChannelId::new(1)))
            .unwrap();
        graph
    }

    fn topn_graph() -> RuntimeFilterGraph {
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_channel(topn_channel(ChannelId::new(1)))
            .unwrap();
        graph
            .insert_binding(producer_binding(
                BindingId::new(1),
                ChannelId::new(1),
                CoverageWitnessId::new(1),
                BTreeSet::from([
                    ContributionKind::OrderedBoundUpdate,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::ProducerClosed,
                ProducerBindingTarget::AggregateTopNKey {
                    group_key_ordinal: 0,
                    limit: NonZeroU32::new(10).unwrap(),
                },
            ))
            .unwrap();
        graph
            .insert_binding(consumer_binding(
                BindingId::new(2),
                ChannelId::new(1),
                BTreeSet::from([ArtifactCapability::OrderedRange]),
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
            ))
            .unwrap();
        graph
    }

    fn topk_summary_graph() -> RuntimeFilterGraph {
        let mut graph = topn_graph();
        let summary = TopKSummaryRequirement::try_new(3).unwrap();
        let channel = graph.channel_mut_for_test(ChannelId::new(1)).unwrap();
        channel.availability_coverage = Coverage::Leaf(CoverageWitnessId::new(1));
        channel.terminal_coverage = Coverage::Leaf(CoverageWitnessId::new(1));
        channel.reduction_requirement = ReductionRequirement::MergeTopKSummary(summary);
        channel.allowed_contribution_kinds = BTreeSet::from([
            ContributionKind::TopKSummary,
            ContributionKind::ProducerClosed,
        ]);
        if let RuntimeFilterBindingRole::Producer(requirement) =
            &mut graph.binding_mut_for_test(BindingId::new(1)).unwrap().role
        {
            requirement.contribution_kinds = BTreeSet::from([
                ContributionKind::TopKSummary,
                ContributionKind::ProducerClosed,
            ]);
        }
        graph
    }

    fn aggregate_graph() -> RuntimeFilterGraph {
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_channel(aggregate_channel(ChannelId::new(1)))
            .unwrap();
        graph
            .insert_binding(producer_binding(
                BindingId::new(1),
                ChannelId::new(1),
                CoverageWitnessId::new(1),
                BTreeSet::from([
                    ContributionKind::FinalDomainShard,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::FencedFinalDomain(
                    CompletionFenceKind::CommittedDomainFrozen,
                ),
                ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
            ))
            .unwrap();
        graph
            .insert_binding(consumer_binding(
                BindingId::new(2),
                ChannelId::new(1),
                BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::RowGroup,
                },
            ))
            .unwrap();
        graph
    }

    fn assert_kind(graph: &RuntimeFilterGraph, expected: GraphValidationErrorKind) {
        assert_eq!(graph.validate().unwrap_err().kind, expected);
    }

    #[test]
    fn validate_rejects_binding_to_unknown_channel() {
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_binding(join_producer_binding(
                BindingId::new(1),
                ChannelId::new(99),
                CoverageWitnessId::new(1),
            ))
            .unwrap();
        assert_kind(&graph, GraphValidationErrorKind::UnknownChannel);
    }

    #[test]
    fn validate_accepts_join_topn_and_aggregate_contracts() {
        for graph in [join_graph(), topn_graph(), aggregate_graph()] {
            graph.validate().unwrap();
        }

        topk_summary_graph().validate().unwrap();
    }

    #[test]
    fn validate_fenced_final_requires_exact_all_of_coverage() {
        aggregate_graph()
            .validate()
            .expect("Aggregate helper must produce valid AllOf-only coverage");

        let mut any_of = aggregate_graph();
        let channel = any_of.channel_mut_for_test(ChannelId::new(1)).unwrap();
        channel.availability_coverage =
            Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]);
        channel.terminal_coverage =
            Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]);

        assert_kind(
            &any_of,
            GraphValidationErrorKind::FencedFinalDomainCoverageMismatch,
        );

        let mut join_any_of = join_graph();
        let channel = join_any_of.channel_mut_for_test(ChannelId::new(1)).unwrap();
        channel.availability_coverage =
            Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]);
        channel.terminal_coverage =
            Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]);
        join_any_of
            .validate()
            .expect("ordinary Join membership may use equivalent AnyOf coverage");
    }

    #[test]
    fn validate_fenced_final_keeps_exact_contribution_completion_and_activation() {
        let mut mixed = aggregate_graph();
        mixed
            .channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .allowed_contribution_kinds
            .insert(ContributionKind::ValueDomainDelta);
        assert_kind(
            &mixed,
            GraphValidationErrorKind::MembershipContributionMismatch,
        );

        let mut wrong_completion = aggregate_graph();
        if let RuntimeFilterBindingRole::Producer(requirement) = &mut wrong_completion
            .binding_mut_for_test(BindingId::new(1))
            .unwrap()
            .role
        {
            requirement.completion_requirement = CompletionRequirement::ProducerClosed;
        }
        assert_kind(
            &wrong_completion,
            GraphValidationErrorKind::ProducerCompletionMismatch {
                expected: CompletionRequirement::FencedFinalDomain(
                    CompletionFenceKind::CommittedDomainFrozen,
                ),
                actual: CompletionRequirement::ProducerClosed,
            },
        );

        let mut blocking = aggregate_graph();
        if let RuntimeFilterBindingRole::Consumer(requirement) = &mut blocking
            .binding_mut_for_test(BindingId::new(2))
            .unwrap()
            .role
        {
            requirement.activation = ConsumerActivation::BlockingSnapshot;
        }
        assert_kind(
            &blocking,
            GraphValidationErrorKind::BlockingFeedbackConsumer,
        );
    }

    #[test]
    fn validate_top_k_summary_requires_exact_all_of_coverage() {
        let mut any_of = topk_summary_graph();
        let channel = any_of.channel_mut_for_test(ChannelId::new(1)).unwrap();
        channel.availability_coverage =
            Coverage::AllOf(vec![Coverage::AnyOf(vec![Coverage::Leaf(
                CoverageWitnessId::new(1),
            )])]);
        channel.terminal_coverage = Coverage::AllOf(vec![Coverage::AnyOf(vec![Coverage::Leaf(
            CoverageWitnessId::new(1),
        )])]);
        assert_kind(
            &any_of,
            GraphValidationErrorKind::TopKSummaryCoverageMismatch,
        );

        let mut mismatched = topk_summary_graph();
        mismatched
            .channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .terminal_coverage = Coverage::AllOf(vec![Coverage::Leaf(CoverageWitnessId::new(2))]);
        assert_kind(
            &mismatched,
            GraphValidationErrorKind::TopKSummaryCoverageMismatch,
        );
    }

    #[test]
    fn validate_top_k_summary_rejects_direct_contribution_mixing() {
        let mut channel_mixing = topk_summary_graph();
        channel_mixing
            .channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .allowed_contribution_kinds
            .insert(ContributionKind::OrderedBoundUpdate);
        assert_kind(
            &channel_mixing,
            GraphValidationErrorKind::OrderedBoundContributionMismatch(
                ContributionKind::OrderedBoundUpdate,
            ),
        );

        let mut producer_mixing = topk_summary_graph();
        if let RuntimeFilterBindingRole::Producer(requirement) = &mut producer_mixing
            .binding_mut_for_test(BindingId::new(1))
            .unwrap()
            .role
        {
            requirement
                .contribution_kinds
                .insert(ContributionKind::OrderedBoundUpdate);
        }
        assert_kind(
            &producer_mixing,
            GraphValidationErrorKind::UnsupportedProducerContribution(
                ContributionKind::OrderedBoundUpdate,
            ),
        );
    }

    #[test]
    fn validate_direct_ordered_bound_rejects_summary_contribution_mixing() {
        let mut channel_mixing = topn_graph();
        channel_mixing
            .channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .allowed_contribution_kinds
            .insert(ContributionKind::TopKSummary);
        assert_kind(
            &channel_mixing,
            GraphValidationErrorKind::OrderedBoundContributionMismatch(
                ContributionKind::TopKSummary,
            ),
        );

        let mut producer_mixing = topn_graph();
        if let RuntimeFilterBindingRole::Producer(requirement) = &mut producer_mixing
            .binding_mut_for_test(BindingId::new(1))
            .unwrap()
            .role
        {
            requirement
                .contribution_kinds
                .insert(ContributionKind::TopKSummary);
        }
        assert_kind(
            &producer_mixing,
            GraphValidationErrorKind::UnsupportedProducerContribution(
                ContributionKind::TopKSummary,
            ),
        );
    }

    #[test]
    fn validate_top_k_summary_requires_every_producer_witness() {
        let mut graph = topk_summary_graph();
        graph
            .insert_binding(producer_binding(
                BindingId::new(3),
                ChannelId::new(1),
                CoverageWitnessId::new(2),
                BTreeSet::from([
                    ContributionKind::TopKSummary,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::ProducerClosed,
                ProducerBindingTarget::AggregateTopNKey {
                    group_key_ordinal: 0,
                    limit: NonZeroU32::new(3).unwrap(),
                },
            ))
            .unwrap();

        assert_kind(
            &graph,
            GraphValidationErrorKind::TopKSummaryMissingCoverageWitness(CoverageWitnessId::new(2)),
        );
    }

    #[test]
    fn validate_top_k_summary_requires_exact_channel_consumer_capability() {
        let mut graph = topk_summary_graph();
        graph
            .channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .required_consumer_capabilities
            .insert(ArtifactCapability::Membership);

        assert_kind(
            &graph,
            GraphValidationErrorKind::TopKSummaryConsumerCapabilityMismatch,
        );
    }

    #[test]
    fn validate_top_k_summary_requires_exact_consumer_binding_capability() {
        for capabilities in [
            BTreeSet::from([ArtifactCapability::Membership]),
            BTreeSet::from([
                ArtifactCapability::OrderedRange,
                ArtifactCapability::Membership,
            ]),
        ] {
            let mut graph = topk_summary_graph();
            let RuntimeFilterBindingRole::Consumer(requirement) =
                &mut graph.binding_mut_for_test(BindingId::new(2)).unwrap().role
            else {
                unreachable!("top-k summary fixture has a consumer")
            };
            requirement.capabilities = capabilities;

            assert_kind(
                &graph,
                GraphValidationErrorKind::TopKSummaryConsumerCapabilityMismatch,
            );
        }
    }

    #[test]
    fn validate_top_k_summary_requires_non_blocking_consumer() {
        let mut graph = topk_summary_graph();
        let RuntimeFilterBindingRole::Consumer(requirement) =
            &mut graph.binding_mut_for_test(BindingId::new(2)).unwrap().role
        else {
            unreachable!("top-k summary fixture has a consumer")
        };
        requirement.activation = ConsumerActivation::BlockingSnapshot;

        assert_kind(&graph, GraphValidationErrorKind::BlockingFeedbackConsumer);
    }

    #[test]
    fn validate_accepts_exact_top_k_summary_consumer_matrix() {
        topk_summary_graph().validate().unwrap();
    }

    #[test]
    fn validate_rejects_missing_roles() {
        let mut missing_producer = RuntimeFilterGraph::default();
        missing_producer
            .insert_channel(join_channel(ChannelId::new(1)))
            .unwrap();
        missing_producer
            .insert_binding(join_consumer_binding(BindingId::new(1), ChannelId::new(1)))
            .unwrap();
        assert_kind(&missing_producer, GraphValidationErrorKind::MissingProducer);

        let mut missing_consumer = RuntimeFilterGraph::default();
        missing_consumer
            .insert_channel(join_channel(ChannelId::new(1)))
            .unwrap();
        missing_consumer
            .insert_binding(join_producer_binding(
                BindingId::new(1),
                ChannelId::new(1),
                CoverageWitnessId::new(1),
            ))
            .unwrap();
        assert_kind(&missing_consumer, GraphValidationErrorKind::MissingConsumer);
    }

    #[test]
    fn validate_rejects_map_key_object_id_mismatches() {
        let mut channel_mismatch = join_graph();
        channel_mismatch.insert_raw_channel(ChannelId::new(0), join_channel(ChannelId::new(9)));
        assert_kind(
            &channel_mismatch,
            GraphValidationErrorKind::ChannelIdMismatch {
                map_key: ChannelId::new(0),
                object_id: ChannelId::new(9),
            },
        );

        let mut binding_mismatch = join_graph();
        binding_mismatch.insert_raw_binding(
            BindingId::new(0),
            join_consumer_binding(BindingId::new(9), ChannelId::new(1)),
        );
        assert_kind(
            &binding_mismatch,
            GraphValidationErrorKind::BindingIdMismatch {
                map_key: BindingId::new(0),
                object_id: BindingId::new(9),
            },
        );
    }

    #[test]
    fn validate_rejects_invalid_coverage_shape_before_policy() {
        let mut graph = join_graph();
        let mut channel = join_channel(ChannelId::new(0));
        channel.availability_coverage = Coverage::AllOf(vec![]);
        channel.policy.max_contribution_bytes = 0;
        graph.insert_raw_channel(ChannelId::new(0), channel);
        assert_kind(
            &graph,
            GraphValidationErrorKind::AvailabilityCoverageShape(CoverageShapeError::EmptyAllOf),
        );
    }

    #[test]
    fn validate_rejects_complete_once_coverage_mismatch() {
        let mut graph = join_graph();
        let mut channel = join_channel(ChannelId::new(0));
        channel.terminal_coverage = Coverage::Leaf(CoverageWitnessId::new(2));
        graph.insert_raw_channel(ChannelId::new(0), channel);
        assert_kind(
            &graph,
            GraphValidationErrorKind::CompleteOnceCoverageMismatch,
        );
    }

    #[test]
    fn validate_accepts_reordered_equivalent_complete_once_coverage() {
        let mut graph = join_graph();
        let channel = graph.channel_mut_for_test(ChannelId::new(1)).unwrap();
        channel.availability_coverage = Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(1)),
            Coverage::Leaf(CoverageWitnessId::new(2)),
        ]);
        channel.terminal_coverage = Coverage::AllOf(vec![
            Coverage::Leaf(CoverageWitnessId::new(2)),
            Coverage::Leaf(CoverageWitnessId::new(1)),
        ]);
        graph
            .insert_binding(join_producer_binding(
                BindingId::new(3),
                ChannelId::new(1),
                CoverageWitnessId::new(2),
            ))
            .unwrap();

        graph.validate().unwrap();
    }

    #[test]
    fn validate_rejects_invalid_domain_matrices() {
        let mut ordered = topn_graph();
        let mut channel = topn_channel(ChannelId::new(0));
        channel.allowed_contribution_kinds = BTreeSet::from([ContributionKind::ProducerClosed]);
        ordered.insert_raw_channel(ChannelId::new(0), channel);
        assert_kind(
            &ordered,
            GraphValidationErrorKind::OrderedBoundContributionMissing,
        );

        let mut membership = join_graph();
        let mut channel = join_channel(ChannelId::new(0));
        channel.reduction_requirement = ReductionRequirement::TightenOrderedBound;
        membership.insert_raw_channel(ChannelId::new(0), channel);
        assert_kind(
            &membership,
            GraphValidationErrorKind::MembershipReductionMismatch,
        );

        let mut fenced = aggregate_graph();
        if let RuntimeFilterBindingRole::Producer(requirement) =
            &mut fenced.binding_mut_for_test(BindingId::new(1)).unwrap().role
        {
            requirement
                .contribution_kinds
                .remove(&ContributionKind::FinalDomainShard);
        }
        assert_kind(
            &fenced,
            GraphValidationErrorKind::RequiredProducerContributionMissing(
                ContributionKind::FinalDomainShard,
            ),
        );
    }

    #[test]
    fn validate_requires_nonempty_order_contract_keys() {
        let mut graph = topn_graph();
        let RuntimeFilterLogicalDomain::OrderedBound(order) = &mut graph
            .channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .logical_domain
        else {
            panic!("expected ordered-bound channel");
        };
        order.keys.clear();

        assert_kind(&graph, GraphValidationErrorKind::EmptyOrderContract);
    }

    #[test]
    fn validate_requires_channel_level_producer_closed() {
        for mut graph in [join_graph(), topn_graph(), aggregate_graph()] {
            graph
                .channel_mut_for_test(ChannelId::new(1))
                .unwrap()
                .allowed_contribution_kinds
                .remove(&ContributionKind::ProducerClosed);
            assert_kind(
                &graph,
                GraphValidationErrorKind::ProducerClosedContributionMissing,
            );
        }
    }

    #[test]
    fn validate_distinguishes_forbidden_ordered_membership_from_missing_ordered_update() {
        let mut forbidden = topn_graph();
        forbidden
            .channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .allowed_contribution_kinds = BTreeSet::from([
            ContributionKind::ValueDomainDelta,
            ContributionKind::ProducerClosed,
        ]);
        assert_kind(
            &forbidden,
            GraphValidationErrorKind::OrderedBoundContributionMismatch(
                ContributionKind::ValueDomainDelta,
            ),
        );

        let mut missing = topn_graph();
        missing
            .channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .allowed_contribution_kinds = BTreeSet::from([ContributionKind::ProducerClosed]);
        assert_kind(
            &missing,
            GraphValidationErrorKind::OrderedBoundContributionMissing,
        );
    }

    #[test]
    fn validate_requires_null_safe_equal_for_final_domain_shards() {
        let mut aggregate = aggregate_graph();
        aggregate
            .channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .logical_domain = RuntimeFilterLogicalDomain::Membership {
            value_type: arrow::datatypes::DataType::Int64,
            null_semantics: NullSemantics::NeverMatches,
        };
        assert_kind(
            &aggregate,
            GraphValidationErrorKind::FinalDomainShardRequiresNullSafeEqual,
        );

        let mut join = join_graph();
        join.channel_mut_for_test(ChannelId::new(1))
            .unwrap()
            .logical_domain = RuntimeFilterLogicalDomain::Membership {
            value_type: arrow::datatypes::DataType::Int64,
            null_semantics: NullSemantics::NullSafeEqual,
        };
        join.validate()
            .expect("value-domain Join may use null-safe equality");
    }

    #[test]
    fn validate_rejects_empty_and_invalid_policy() {
        let invalid = [
            (
                RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 0,
                    ..policy()
                },
                GraphValidationErrorKind::ZeroPolicyValue(PolicyField::MaxContributionBytes),
            ),
            (
                RuntimeFilterPolicyRequirement {
                    max_artifact_bytes: 0,
                    ..policy()
                },
                GraphValidationErrorKind::ZeroPolicyValue(PolicyField::MaxArtifactBytes),
            ),
            (
                RuntimeFilterPolicyRequirement {
                    deadline_ms: 0,
                    ..policy()
                },
                GraphValidationErrorKind::ZeroPolicyValue(PolicyField::DeadlineMs),
            ),
            (
                RuntimeFilterPolicyRequirement {
                    max_retries: 0,
                    ..policy()
                },
                GraphValidationErrorKind::ZeroPolicyValue(PolicyField::MaxRetries),
            ),
            (
                RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 4097,
                    max_artifact_bytes: 4096,
                    ..policy()
                },
                GraphValidationErrorKind::ContributionBytesExceedArtifactBytes,
            ),
            (
                RuntimeFilterPolicyRequirement {
                    max_artifact_bytes: MAX_ARTIFACT_BYTES + 1,
                    ..policy()
                },
                GraphValidationErrorKind::ArtifactBytesExceedLimit,
            ),
            (
                RuntimeFilterPolicyRequirement {
                    deadline_ms: MAX_DEADLINE_MS + 1,
                    ..policy()
                },
                GraphValidationErrorKind::DeadlineExceedsLimit,
            ),
            (
                RuntimeFilterPolicyRequirement {
                    max_retries: MAX_RETRIES + 1,
                    ..policy()
                },
                GraphValidationErrorKind::RetriesExceedLimit,
            ),
        ];

        for (policy, expected) in invalid {
            let mut graph = join_graph();
            let mut channel = join_channel(ChannelId::new(0));
            channel.policy = policy;
            graph.insert_raw_channel(ChannelId::new(0), channel);
            assert_kind(&graph, expected);
        }
    }

    #[test]
    fn validate_rejects_invalid_location_and_apply_point() {
        let mut invalid_location = RuntimeFilterGraph::default();
        invalid_location
            .insert_binding(join_producer_binding(
                BindingId::new(1),
                ChannelId::new(99),
                CoverageWitnessId::new(1),
            ))
            .unwrap();
        invalid_location
            .binding_mut_for_test(BindingId::new(1))
            .unwrap()
            .location
            .node_id = PlanNodeId::new(-1);
        assert_kind(
            &invalid_location,
            GraphValidationErrorKind::InvalidPlanNodeId(PlanNodeId::new(-1)),
        );

        let mut invalid_apply = RuntimeFilterGraph::default();
        let mut binding = join_producer_binding(
            BindingId::new(1),
            ChannelId::new(99),
            CoverageWitnessId::new(1),
        );
        binding.apply_point = ApplyPoint::NodeInput;
        invalid_apply.insert_binding(binding).unwrap();
        assert_kind(
            &invalid_apply,
            GraphValidationErrorKind::RoleApplyPointMismatch,
        );

        let mut valid_fragment = join_graph();
        valid_fragment
            .binding_mut_for_test(BindingId::new(1))
            .unwrap()
            .location
            .fragment_id = PlanFragmentId::new(u32::MAX);
        valid_fragment.validate().unwrap();
    }

    #[test]
    fn validate_rejects_duplicate_and_unowned_witnesses() {
        let mut duplicate = join_graph();
        duplicate
            .insert_binding(join_producer_binding(
                BindingId::new(3),
                ChannelId::new(1),
                CoverageWitnessId::new(1),
            ))
            .unwrap();
        assert_kind(
            &duplicate,
            GraphValidationErrorKind::DuplicateCoverageWitness(CoverageWitnessId::new(1)),
        );

        let mut unknown = join_graph();
        let channel = unknown.channel_mut_for_test(ChannelId::new(1)).unwrap();
        channel.availability_coverage = Coverage::Leaf(CoverageWitnessId::new(99));
        channel.terminal_coverage = Coverage::Leaf(CoverageWitnessId::new(99));
        assert_kind(
            &unknown,
            GraphValidationErrorKind::UnknownCoverageWitness(CoverageWitnessId::new(99)),
        );

        let mut consumer_owned = join_graph();
        consumer_owned
            .binding_mut_for_test(BindingId::new(2))
            .unwrap()
            .coverage_witness_id = Some(CoverageWitnessId::new(2));
        assert_kind(
            &consumer_owned,
            GraphValidationErrorKind::ConsumerOwnedCoverageWitness(CoverageWitnessId::new(2)),
        );

        let mut missing = join_graph();
        missing
            .binding_mut_for_test(BindingId::new(1))
            .unwrap()
            .coverage_witness_id = None;
        assert_kind(
            &missing,
            GraphValidationErrorKind::ProducerCoverageWitnessMissing,
        );
    }

    #[test]
    fn validate_rejects_invalid_producer_requirements() {
        let mut empty = join_graph();
        if let RuntimeFilterBindingRole::Producer(requirement) =
            &mut empty.binding_mut_for_test(BindingId::new(1)).unwrap().role
        {
            requirement.contribution_kinds.clear();
        }
        assert_kind(&empty, GraphValidationErrorKind::EmptyProducerContributions);

        let mut unsupported = join_graph();
        if let RuntimeFilterBindingRole::Producer(requirement) = &mut unsupported
            .binding_mut_for_test(BindingId::new(1))
            .unwrap()
            .role
        {
            requirement
                .contribution_kinds
                .insert(ContributionKind::OrderedBoundUpdate);
        }
        assert_kind(
            &unsupported,
            GraphValidationErrorKind::UnsupportedProducerContribution(
                ContributionKind::OrderedBoundUpdate,
            ),
        );

        let mut completion = join_graph();
        if let RuntimeFilterBindingRole::Producer(requirement) = &mut completion
            .binding_mut_for_test(BindingId::new(1))
            .unwrap()
            .role
        {
            requirement.completion_requirement = CompletionRequirement::FencedFinalDomain(
                CompletionFenceKind::CommittedDomainFrozen,
            );
        }
        assert_kind(
            &completion,
            GraphValidationErrorKind::ProducerCompletionMismatch {
                expected: CompletionRequirement::ProducerClosed,
                actual: CompletionRequirement::FencedFinalDomain(
                    CompletionFenceKind::CommittedDomainFrozen,
                ),
            },
        );
    }

    #[test]
    fn validate_rejects_producers_missing_matrix_required_contributions() {
        fn remove_contribution(graph: &mut RuntimeFilterGraph, contribution: ContributionKind) {
            if let RuntimeFilterBindingRole::Producer(requirement) =
                &mut graph.binding_mut_for_test(BindingId::new(1)).unwrap().role
            {
                requirement.contribution_kinds.remove(&contribution);
            }
        }

        let mut cases = Vec::new();

        for missing in [
            ContributionKind::ValueDomainDelta,
            ContributionKind::ProducerClosed,
        ] {
            let mut graph = join_graph();
            remove_contribution(&mut graph, missing);
            cases.push((graph, missing));
        }

        for missing in [
            ContributionKind::FinalDomainShard,
            ContributionKind::ProducerClosed,
        ] {
            let mut graph = aggregate_graph();
            remove_contribution(&mut graph, missing);
            cases.push((graph, missing));
        }

        for missing in [
            ContributionKind::OrderedBoundUpdate,
            ContributionKind::ProducerClosed,
        ] {
            let mut graph = topn_graph();
            remove_contribution(&mut graph, missing);
            cases.push((graph, missing));
        }

        for missing in [
            ContributionKind::TopKSummary,
            ContributionKind::ProducerClosed,
        ] {
            let mut graph = topn_graph();
            let channel = graph.channel_mut_for_test(ChannelId::new(1)).unwrap();
            channel.availability_coverage = Coverage::Leaf(CoverageWitnessId::new(1));
            channel.terminal_coverage = Coverage::Leaf(CoverageWitnessId::new(1));
            channel.reduction_requirement =
                ReductionRequirement::MergeTopKSummary(TopKSummaryRequirement::try_new(3).unwrap());
            channel.allowed_contribution_kinds = BTreeSet::from([
                ContributionKind::TopKSummary,
                ContributionKind::ProducerClosed,
            ]);
            if let RuntimeFilterBindingRole::Producer(requirement) =
                &mut graph.binding_mut_for_test(BindingId::new(1)).unwrap().role
            {
                requirement.contribution_kinds = BTreeSet::from([
                    ContributionKind::TopKSummary,
                    ContributionKind::ProducerClosed,
                ]);
            }
            remove_contribution(&mut graph, missing);
            cases.push((graph, missing));
        }

        for (graph, missing) in cases {
            assert_kind(
                &graph,
                GraphValidationErrorKind::RequiredProducerContributionMissing(missing),
            );
        }
    }

    #[test]
    fn witness_uniqueness_precedes_every_producer_matrix_error() {
        let mut graph = join_graph();
        if let RuntimeFilterBindingRole::Producer(requirement) =
            &mut graph.binding_mut_for_test(BindingId::new(1)).unwrap().role
        {
            requirement
                .contribution_kinds
                .insert(ContributionKind::OrderedBoundUpdate);
        }
        graph
            .insert_binding(join_producer_binding(
                BindingId::new(3),
                ChannelId::new(1),
                CoverageWitnessId::new(1),
            ))
            .unwrap();

        assert_kind(
            &graph,
            GraphValidationErrorKind::DuplicateCoverageWitness(CoverageWitnessId::new(1)),
        );
    }

    #[test]
    fn coverage_ownership_selects_lowest_unknown_witness_independent_of_child_order() {
        fn invalid_graph(reverse: bool) -> RuntimeFilterGraph {
            let mut graph = join_graph();
            let children = if reverse {
                vec![
                    Coverage::Leaf(CoverageWitnessId::new(99)),
                    Coverage::Leaf(CoverageWitnessId::new(3)),
                ]
            } else {
                vec![
                    Coverage::Leaf(CoverageWitnessId::new(3)),
                    Coverage::Leaf(CoverageWitnessId::new(99)),
                ]
            };
            let channel = graph.channel_mut_for_test(ChannelId::new(1)).unwrap();
            channel.availability_coverage = Coverage::AllOf(children.clone());
            channel.terminal_coverage = Coverage::AllOf(children);
            graph
        }

        let expected = GraphValidationError {
            channel_id: Some(ChannelId::new(1)),
            binding_id: None,
            kind: GraphValidationErrorKind::UnknownCoverageWitness(CoverageWitnessId::new(3)),
        };
        assert_eq!(invalid_graph(false).validate(), Err(expected.clone()));
        assert_eq!(invalid_graph(true).validate(), Err(expected));
    }

    #[test]
    fn validate_rejects_unsupported_capabilities_and_blocking_feedback() {
        let mut capabilities = join_graph();
        if let RuntimeFilterBindingRole::Consumer(requirement) = &mut capabilities
            .binding_mut_for_test(BindingId::new(2))
            .unwrap()
            .role
        {
            requirement
                .capabilities
                .remove(&ArtifactCapability::Membership);
        }
        assert_kind(
            &capabilities,
            GraphValidationErrorKind::UnsupportedConsumerCapability(ArtifactCapability::Membership),
        );

        for mut graph in [topn_graph(), aggregate_graph()] {
            if let RuntimeFilterBindingRole::Consumer(requirement) =
                &mut graph.binding_mut_for_test(BindingId::new(2)).unwrap().role
            {
                requirement.activation = ConsumerActivation::BlockingSnapshot;
            }
            assert_kind(&graph, GraphValidationErrorKind::BlockingFeedbackConsumer);
        }
    }

    #[test]
    fn validation_returns_the_same_first_error_for_reversed_insertion() {
        fn invalid_graph(reverse: bool) -> RuntimeFilterGraph {
            let mut graph = RuntimeFilterGraph::default();
            let mut low = join_channel(ChannelId::new(1));
            low.policy.deadline_ms = 0;
            let mut high = join_channel(ChannelId::new(2));
            high.policy.max_artifact_bytes = 0;
            let channels = if reverse { [high, low] } else { [low, high] };
            for channel in channels {
                graph.insert_channel(channel).unwrap();
            }
            graph
        }

        let expected = GraphValidationError {
            channel_id: Some(ChannelId::new(1)),
            binding_id: None,
            kind: GraphValidationErrorKind::ZeroPolicyValue(PolicyField::DeadlineMs),
        };
        assert_eq!(invalid_graph(false).validate(), Err(expected.clone()));
        assert_eq!(invalid_graph(true).validate(), Err(expected));

        fn invalid_bindings(reverse: bool) -> RuntimeFilterGraph {
            let mut graph = join_graph();
            let mut low = join_consumer_binding(BindingId::new(3), ChannelId::new(1));
            low.location.node_id = PlanNodeId::new(-1);
            let mut high = join_consumer_binding(BindingId::new(4), ChannelId::new(1));
            high.location.node_id = PlanNodeId::new(-2);
            let bindings = if reverse { [high, low] } else { [low, high] };
            for binding in bindings {
                graph.insert_binding(binding).unwrap();
            }
            graph
        }
        let expected = GraphValidationError {
            channel_id: Some(ChannelId::new(1)),
            binding_id: Some(BindingId::new(3)),
            kind: GraphValidationErrorKind::InvalidPlanNodeId(PlanNodeId::new(-1)),
        };
        assert_eq!(invalid_bindings(false).validate(), Err(expected.clone()));
        assert_eq!(invalid_bindings(true).validate(), Err(expected));
    }

    #[test]
    fn validation_error_display_contains_only_stable_model_values() {
        let error = GraphValidationError {
            channel_id: Some(ChannelId::new(7)),
            binding_id: Some(BindingId::new(9)),
            kind: GraphValidationErrorKind::UnknownChannel,
        };
        assert_eq!(
            error.to_string(),
            "runtime filter graph validation failed: channel ChannelId(7), binding BindingId(9), UnknownChannel"
        );
    }
}
