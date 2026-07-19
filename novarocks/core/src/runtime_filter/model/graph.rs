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

use crate::sql::analysis::TypedExpr;

use super::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, PlanFragmentId, PlanNodeId, ReductionRequirement,
    RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
};
use super::coverage::Coverage;

#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterChannelSpec {
    pub channel_id: ChannelId,
    pub logical_domain: RuntimeFilterLogicalDomain,
    pub lifecycle: RuntimeFilterLifecycle,
    pub availability_coverage: Coverage,
    pub terminal_coverage: Coverage,
    pub reduction_requirement: ReductionRequirement,
    pub allowed_contribution_kinds: BTreeSet<ContributionKind>,
    pub required_consumer_capabilities: BTreeSet<ArtifactCapability>,
    pub policy: RuntimeFilterPolicyRequirement,
}

#[derive(Clone, Debug)]
pub(crate) struct ProducerRequirement {
    pub contribution_kinds: BTreeSet<ContributionKind>,
    pub completion_requirement: CompletionRequirement,
    pub join_key_ordinal: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConsumerBindingTarget {
    DirectInput { input_ordinal: usize },
    SourceBoundary,
}

#[derive(Clone, Debug)]
pub(crate) struct ConsumerRequirement {
    pub capabilities: BTreeSet<ArtifactCapability>,
    pub activation: ConsumerActivation,
    pub target: ConsumerBindingTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PlanLocation {
    pub fragment_id: PlanFragmentId,
    pub node_id: PlanNodeId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApplyPoint {
    NodeInput,
    NodeOutput,
}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeFilterBindingRole {
    Producer(ProducerRequirement),
    Consumer(ConsumerRequirement),
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterBindingSpec {
    pub binding_id: BindingId,
    pub channel_id: ChannelId,
    pub coverage_witness_id: Option<CoverageWitnessId>,
    pub location: PlanLocation,
    pub expression: TypedExpr,
    pub apply_point: ApplyPoint,
    pub role: RuntimeFilterBindingRole,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeFilterGraph {
    channels: BTreeMap<ChannelId, RuntimeFilterChannelSpec>,
    bindings: BTreeMap<BindingId, RuntimeFilterBindingSpec>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GraphBuildError {
    DuplicateChannel(ChannelId),
    DuplicateBinding(BindingId),
}

impl RuntimeFilterGraph {
    pub(crate) fn insert_channel(
        &mut self,
        channel: RuntimeFilterChannelSpec,
    ) -> Result<(), GraphBuildError> {
        if self.channels.contains_key(&channel.channel_id) {
            return Err(GraphBuildError::DuplicateChannel(channel.channel_id));
        }
        self.channels.insert(channel.channel_id, channel);
        Ok(())
    }

    pub(crate) fn insert_binding(
        &mut self,
        binding: RuntimeFilterBindingSpec,
    ) -> Result<(), GraphBuildError> {
        if self.bindings.contains_key(&binding.binding_id) {
            return Err(GraphBuildError::DuplicateBinding(binding.binding_id));
        }
        self.bindings.insert(binding.binding_id, binding);
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.bindings.is_empty()
    }

    pub(crate) fn channel_count(&self) -> usize {
        self.channels.len()
    }

    pub(crate) fn binding_count(&self) -> usize {
        self.bindings.len()
    }

    pub(crate) fn channel(&self, channel_id: ChannelId) -> Option<&RuntimeFilterChannelSpec> {
        self.channels.get(&channel_id)
    }

    pub(crate) fn binding(&self, binding_id: BindingId) -> Option<&RuntimeFilterBindingSpec> {
        self.bindings.get(&binding_id)
    }

    pub(crate) fn channels(&self) -> impl Iterator<Item = &RuntimeFilterChannelSpec> {
        self.channels.values()
    }

    pub(crate) fn bindings(&self) -> impl Iterator<Item = &RuntimeFilterBindingSpec> {
        self.bindings.values()
    }

    pub(super) fn channel_entries(
        &self,
    ) -> impl Iterator<Item = (&ChannelId, &RuntimeFilterChannelSpec)> {
        self.channels.iter()
    }

    pub(super) fn binding_entries(
        &self,
    ) -> impl Iterator<Item = (&BindingId, &RuntimeFilterBindingSpec)> {
        self.bindings.iter()
    }

    #[cfg(test)]
    pub(super) fn insert_raw_channel(
        &mut self,
        map_key: ChannelId,
        channel: RuntimeFilterChannelSpec,
    ) {
        self.channels.insert(map_key, channel);
    }

    #[cfg(test)]
    pub(super) fn insert_raw_binding(
        &mut self,
        map_key: BindingId,
        binding: RuntimeFilterBindingSpec,
    ) {
        self.bindings.insert(map_key, binding);
    }

    #[cfg(test)]
    pub(super) fn channel_mut_for_test(
        &mut self,
        channel_id: ChannelId,
    ) -> Option<&mut RuntimeFilterChannelSpec> {
        self.channels.get_mut(&channel_id)
    }

    #[cfg(test)]
    pub(crate) fn binding_mut_for_test(
        &mut self,
        binding_id: BindingId,
    ) -> Option<&mut RuntimeFilterBindingSpec> {
        self.bindings.get_mut(&binding_id)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::BTreeSet;

    use arrow::datatypes::DataType;

    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};

    use super::super::contract::{
        ArtifactCapability, BindingId, ChannelId, ComparatorDigest, CompletionRequirement,
        ConsumerActivation, ContributionKind, CoverageWitnessId, NullOrder, NullSemantics,
        OrderContract, OrderKeyContract, PlanFragmentId, PlanNodeId, ReductionRequirement,
        RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
        SortDirection,
    };
    use super::super::coverage::Coverage;
    use super::*;

    pub(crate) fn expression() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    pub(crate) fn policy() -> RuntimeFilterPolicyRequirement {
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 4096,
            deadline_ms: 30_000,
            max_retries: 3,
        }
    }

    pub(crate) fn join_channel(channel_id: ChannelId) -> RuntimeFilterChannelSpec {
        RuntimeFilterChannelSpec {
            channel_id,
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            required_consumer_capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            policy: policy(),
        }
    }

    pub(crate) fn topn_channel(channel_id: ChannelId) -> RuntimeFilterChannelSpec {
        RuntimeFilterChannelSpec {
            channel_id,
            logical_domain: RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
                keys: vec![OrderKeyContract {
                    data_type: DataType::Int64,
                    direction: SortDirection::Descending,
                    null_order: NullOrder::First,
                }],
                inclusive: true,
                comparator_digest: ComparatorDigest::new([7; 32]),
            }),
            lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
            availability_coverage: Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]),
            terminal_coverage: Coverage::AllOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]),
            reduction_requirement: ReductionRequirement::TightenOrderedBound,
            allowed_contribution_kinds: BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            required_consumer_capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            policy: policy(),
        }
    }

    pub(crate) fn aggregate_channel(channel_id: ChannelId) -> RuntimeFilterChannelSpec {
        RuntimeFilterChannelSpec {
            channel_id,
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: Coverage::AllOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]),
            terminal_coverage: Coverage::AllOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]),
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ]),
            required_consumer_capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            policy: policy(),
        }
    }

    pub(crate) fn producer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
        witness_id: CoverageWitnessId,
        contribution_kinds: BTreeSet<ContributionKind>,
        completion_requirement: CompletionRequirement,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id,
            channel_id,
            coverage_witness_id: Some(witness_id),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(0),
                node_id: PlanNodeId::new(1),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                contribution_kinds,
                completion_requirement,
                join_key_ordinal: 0,
            }),
        }
    }

    pub(crate) fn consumer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
        capabilities: BTreeSet<ArtifactCapability>,
        activation: ConsumerActivation,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id,
            channel_id,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(0),
                node_id: PlanNodeId::new(2),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities,
                activation,
                target: ConsumerBindingTarget::SourceBoundary,
            }),
        }
    }

    pub(crate) fn join_producer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
        witness_id: CoverageWitnessId,
    ) -> RuntimeFilterBindingSpec {
        producer_binding(
            binding_id,
            channel_id,
            witness_id,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
        )
    }

    pub(crate) fn join_consumer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
    ) -> RuntimeFilterBindingSpec {
        consumer_binding(
            binding_id,
            channel_id,
            BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            ConsumerActivation::BlockingSnapshot,
        )
    }

    #[test]
    fn controlled_insert_rejects_duplicate_ids() {
        let mut graph = RuntimeFilterGraph::default();
        let channel = join_channel(ChannelId::new(1));
        graph.insert_channel(channel.clone()).unwrap();
        let mut conflicting_channel = channel;
        conflicting_channel.policy.deadline_ms = 1;
        assert_eq!(
            graph.insert_channel(conflicting_channel),
            Err(GraphBuildError::DuplicateChannel(ChannelId::new(1)))
        );
        assert_eq!(
            graph.channel(ChannelId::new(1)).unwrap().policy.deadline_ms,
            30_000
        );

        let binding = join_producer_binding(
            BindingId::new(1),
            ChannelId::new(1),
            CoverageWitnessId::new(1),
        );
        graph.insert_binding(binding.clone()).unwrap();
        let mut conflicting_binding = binding;
        conflicting_binding.location.node_id = PlanNodeId::new(99);
        assert_eq!(
            graph.insert_binding(conflicting_binding),
            Err(GraphBuildError::DuplicateBinding(BindingId::new(1)))
        );
        assert_eq!(
            graph.binding(BindingId::new(1)).unwrap().location.node_id,
            PlanNodeId::new(1)
        );
    }

    #[test]
    fn graph_exposes_read_only_deterministic_access() {
        let mut graph = RuntimeFilterGraph::default();
        graph
            .insert_channel(join_channel(ChannelId::new(2)))
            .unwrap();
        graph
            .insert_channel(join_channel(ChannelId::new(1)))
            .unwrap();
        graph
            .insert_binding(join_consumer_binding(BindingId::new(2), ChannelId::new(1)))
            .unwrap();
        graph
            .insert_binding(join_producer_binding(
                BindingId::new(1),
                ChannelId::new(1),
                CoverageWitnessId::new(1),
            ))
            .unwrap();

        assert_eq!(graph.channel_count(), 2);
        assert_eq!(graph.binding_count(), 2);
        assert!(!graph.is_empty());
        assert_eq!(
            graph
                .channels()
                .map(|channel| channel.channel_id)
                .collect::<Vec<_>>(),
            vec![ChannelId::new(1), ChannelId::new(2)]
        );
        assert_eq!(
            graph
                .bindings()
                .map(|binding| binding.binding_id)
                .collect::<Vec<_>>(),
            vec![BindingId::new(1), BindingId::new(2)]
        );
        assert_eq!(
            graph.channel(ChannelId::new(1)).unwrap().channel_id,
            ChannelId::new(1)
        );
        assert_eq!(
            graph.binding(BindingId::new(2)).unwrap().binding_id,
            BindingId::new(2)
        );
    }
}
