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
use std::num::NonZeroU32;

use crate::sql::analysis::TypedExpr;

use super::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, PlanFragmentId, PlanNodeId, ReductionRequirement,
    RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
};
use super::coverage::Coverage;

#[derive(Clone, Debug)]
pub struct RuntimeFilterChannelSpec {
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
pub struct ProducerRequirement {
    pub contribution_kinds: BTreeSet<ContributionKind>,
    pub completion_requirement: CompletionRequirement,
    pub target: ProducerBindingTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerBindingTarget {
    JoinBuildKey {
        ordinal: usize,
    },
    AggregateTopNKey {
        group_key_ordinal: usize,
        limit: NonZeroU32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerBindingTarget {
    DirectInput { input_ordinal: usize },
    SourceBoundary,
}

#[derive(Clone, Debug)]
pub struct ConsumerRequirementData<A> {
    pub capabilities: BTreeSet<ArtifactCapability>,
    pub activation: A,
    pub target: ConsumerBindingTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlanLocation {
    pub fragment_id: PlanFragmentId,
    pub node_id: PlanNodeId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyPoint {
    NodeInput,
    NodeOutput,
}

#[derive(Clone, Debug)]
pub enum RuntimeFilterBindingRoleData<A> {
    Producer(ProducerRequirement),
    Consumer(ConsumerRequirementData<A>),
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterBindingSpecData<A> {
    pub binding_id: BindingId,
    pub channel_id: ChannelId,
    pub coverage_witness_id: Option<CoverageWitnessId>,
    pub location: PlanLocation,
    pub expression: TypedExpr,
    pub apply_point: ApplyPoint,
    pub role: RuntimeFilterBindingRoleData<A>,
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterGraphData<A> {
    channels: BTreeMap<ChannelId, RuntimeFilterChannelSpec>,
    bindings: BTreeMap<BindingId, RuntimeFilterBindingSpecData<A>>,
}

impl<A> Default for RuntimeFilterGraphData<A> {
    fn default() -> Self {
        Self {
            channels: BTreeMap::new(),
            bindings: BTreeMap::new(),
        }
    }
}

pub type ConsumerRequirement = ConsumerRequirementData<ConsumerActivation>;
pub type RuntimeFilterBindingRole = RuntimeFilterBindingRoleData<ConsumerActivation>;
pub type RuntimeFilterBindingSpec = RuntimeFilterBindingSpecData<ConsumerActivation>;
pub type RuntimeFilterGraph = RuntimeFilterGraphData<ConsumerActivation>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphBuildError {
    DuplicateChannel(ChannelId),
    DuplicateBinding(BindingId),
}

impl<A> RuntimeFilterGraphData<A> {
    pub fn insert_channel(
        &mut self,
        channel: RuntimeFilterChannelSpec,
    ) -> Result<(), GraphBuildError> {
        if self.channels.contains_key(&channel.channel_id) {
            return Err(GraphBuildError::DuplicateChannel(channel.channel_id));
        }
        self.channels.insert(channel.channel_id, channel);
        Ok(())
    }

    pub fn insert_binding(
        &mut self,
        binding: RuntimeFilterBindingSpecData<A>,
    ) -> Result<(), GraphBuildError> {
        if self.bindings.contains_key(&binding.binding_id) {
            return Err(GraphBuildError::DuplicateBinding(binding.binding_id));
        }
        self.bindings.insert(binding.binding_id, binding);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.bindings.is_empty()
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    pub fn binding_count(&self) -> usize {
        self.bindings.len()
    }

    pub fn channel(&self, channel_id: ChannelId) -> Option<&RuntimeFilterChannelSpec> {
        self.channels.get(&channel_id)
    }

    pub fn binding(&self, binding_id: BindingId) -> Option<&RuntimeFilterBindingSpecData<A>> {
        self.bindings.get(&binding_id)
    }

    pub fn channels(&self) -> impl Iterator<Item = &RuntimeFilterChannelSpec> {
        self.channels.values()
    }

    pub fn bindings(&self) -> impl Iterator<Item = &RuntimeFilterBindingSpecData<A>> {
        self.bindings.values()
    }

    pub fn map_consumer_activations<B, E>(
        self,
        mut decide: impl FnMut(BindingId, ChannelId, PlanLocation, &A) -> Result<B, E>,
    ) -> Result<RuntimeFilterGraphData<B>, E> {
        let bindings = self
            .bindings
            .into_iter()
            .map(|(binding_id, binding)| {
                let role = match binding.role {
                    RuntimeFilterBindingRoleData::Producer(requirement) => {
                        RuntimeFilterBindingRoleData::Producer(requirement)
                    }
                    RuntimeFilterBindingRoleData::Consumer(requirement) => {
                        RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                            capabilities: requirement.capabilities,
                            activation: decide(
                                binding.binding_id,
                                binding.channel_id,
                                binding.location,
                                &requirement.activation,
                            )?,
                            target: requirement.target,
                        })
                    }
                };
                Ok((
                    binding_id,
                    RuntimeFilterBindingSpecData {
                        binding_id: binding.binding_id,
                        channel_id: binding.channel_id,
                        coverage_witness_id: binding.coverage_witness_id,
                        location: binding.location,
                        expression: binding.expression,
                        apply_point: binding.apply_point,
                        role,
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, E>>()?;
        Ok(RuntimeFilterGraphData {
            channels: self.channels,
            bindings,
        })
    }

    pub(super) fn channel_entries(
        &self,
    ) -> impl Iterator<Item = (&ChannelId, &RuntimeFilterChannelSpec)> {
        self.channels.iter()
    }

    pub(super) fn binding_entries(
        &self,
    ) -> impl Iterator<Item = (&BindingId, &RuntimeFilterBindingSpecData<A>)> {
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
        binding: RuntimeFilterBindingSpecData<A>,
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
    pub fn binding_mut_for_test(
        &mut self,
        binding_id: BindingId,
    ) -> Option<&mut RuntimeFilterBindingSpecData<A>> {
        self.bindings.get_mut(&binding_id)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;

    use arrow::datatypes::DataType;

    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};

    use super::super::contract::{
        ArtifactCapability, BindingId, ChannelId, ComparatorDigest, CompletionRequirement,
        ConsumerActivation, ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder,
        NullSemantics, OrderContract, OrderKeyContract, PlanFragmentId, PlanNodeId,
        ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
        RuntimeFilterPolicyRequirement, SortDirection,
    };
    use super::super::coverage::Coverage;
    use super::super::validation::ActivationContract;
    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TestActivation {
        Blocking,
        Live,
    }

    impl ActivationContract for TestActivation {
        fn satisfies_required_non_blocking(&self) -> bool {
            matches!(self, Self::Live)
        }
    }

    #[test]
    fn producer_binding_target_preserves_exact_owner_payloads() {
        let join = ProducerBindingTarget::JoinBuildKey { ordinal: 7 };
        let aggregate = ProducerBindingTarget::AggregateTopNKey {
            group_key_ordinal: 11,
            limit: NonZeroU32::new(19).unwrap(),
        };

        assert_eq!(join, ProducerBindingTarget::JoinBuildKey { ordinal: 7 });
        assert_eq!(
            aggregate,
            ProducerBindingTarget::AggregateTopNKey {
                group_key_ordinal: 11,
                limit: NonZeroU32::new(19).unwrap(),
            }
        );
    }

    #[test]
    fn producer_binding_target_variants_are_not_interchangeable() {
        fn join_ordinal(target: ProducerBindingTarget) -> Option<usize> {
            match target {
                ProducerBindingTarget::JoinBuildKey { ordinal } => Some(ordinal),
                ProducerBindingTarget::AggregateTopNKey { .. } => None,
            }
        }

        fn aggregate_key(target: ProducerBindingTarget) -> Option<(usize, NonZeroU32)> {
            match target {
                ProducerBindingTarget::JoinBuildKey { .. } => None,
                ProducerBindingTarget::AggregateTopNKey {
                    group_key_ordinal,
                    limit,
                } => Some((group_key_ordinal, limit)),
            }
        }

        let join = ProducerBindingTarget::JoinBuildKey { ordinal: 3 };
        let aggregate = ProducerBindingTarget::AggregateTopNKey {
            group_key_ordinal: 5,
            limit: NonZeroU32::new(13).unwrap(),
        };
        assert_eq!(join_ordinal(join), Some(3));
        assert_eq!(aggregate_key(join), None);
        assert_eq!(join_ordinal(aggregate), None);
        assert_eq!(
            aggregate_key(aggregate),
            Some((5, NonZeroU32::new(13).unwrap()))
        );
    }

    pub fn expression() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    pub fn policy() -> RuntimeFilterPolicyRequirement {
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 4096,
            deadline_ms: 30_000,
            max_retries: 3,
        }
    }

    pub fn join_channel(channel_id: ChannelId) -> RuntimeFilterChannelSpec {
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

    pub fn topn_channel(channel_id: ChannelId) -> RuntimeFilterChannelSpec {
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

    pub fn aggregate_channel(channel_id: ChannelId) -> RuntimeFilterChannelSpec {
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

    pub fn producer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
        witness_id: CoverageWitnessId,
        contribution_kinds: BTreeSet<ContributionKind>,
        completion_requirement: CompletionRequirement,
        target: ProducerBindingTarget,
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
                target,
            }),
        }
    }

    pub fn consumer_binding(
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

    pub fn join_producer_binding(
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
            ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
        )
    }

    pub fn join_consumer_binding(
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

    fn generic_join_graph(activation: TestActivation) -> RuntimeFilterGraphData<TestActivation> {
        let mut graph = RuntimeFilterGraphData::default();
        graph
            .insert_channel(join_channel(ChannelId::new(1)))
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpecData {
                binding_id: BindingId::new(1),
                channel_id: ChannelId::new(1),
                coverage_witness_id: Some(CoverageWitnessId::new(1)),
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(0),
                    node_id: PlanNodeId::new(1),
                },
                expression: expression(),
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                    contribution_kinds: BTreeSet::from([
                        ContributionKind::ValueDomainDelta,
                        ContributionKind::ProducerClosed,
                    ]),
                    completion_requirement: CompletionRequirement::ProducerClosed,
                    target: ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
                }),
            })
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpecData {
                binding_id: BindingId::new(2),
                channel_id: ChannelId::new(1),
                coverage_witness_id: None,
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(0),
                    node_id: PlanNodeId::new(2),
                },
                expression: expression(),
                apply_point: ApplyPoint::NodeInput,
                role: RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                    capabilities: BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    activation,
                    target: ConsumerBindingTarget::SourceBoundary,
                }),
            })
            .unwrap();
        graph
    }

    fn generic_required_live_graph(
        activation: TestActivation,
    ) -> RuntimeFilterGraphData<TestActivation> {
        let mut graph = RuntimeFilterGraphData::default();
        graph
            .insert_channel(topn_channel(ChannelId::new(1)))
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpecData {
                binding_id: BindingId::new(1),
                channel_id: ChannelId::new(1),
                coverage_witness_id: Some(CoverageWitnessId::new(1)),
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(0),
                    node_id: PlanNodeId::new(1),
                },
                expression: expression(),
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                    contribution_kinds: BTreeSet::from([
                        ContributionKind::OrderedBoundUpdate,
                        ContributionKind::ProducerClosed,
                    ]),
                    completion_requirement: CompletionRequirement::ProducerClosed,
                    target: ProducerBindingTarget::AggregateTopNKey {
                        group_key_ordinal: 0,
                        limit: NonZeroU32::new(10).unwrap(),
                    },
                }),
            })
            .unwrap();
        graph
            .insert_binding(RuntimeFilterBindingSpecData {
                binding_id: BindingId::new(2),
                channel_id: ChannelId::new(1),
                coverage_witness_id: None,
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(0),
                    node_id: PlanNodeId::new(2),
                },
                expression: expression(),
                apply_point: ApplyPoint::NodeInput,
                role: RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                    capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
                    activation,
                    target: ConsumerBindingTarget::SourceBoundary,
                }),
            })
            .unwrap();
        graph
    }

    #[test]
    fn generic_and_sealed_graphs_share_validation_contract() {
        let generic: RuntimeFilterGraphData<TestActivation> =
            generic_join_graph(TestActivation::Blocking);
        let sealed: RuntimeFilterGraph = generic_join_graph(TestActivation::Blocking)
            .map_consumer_activations(|_, _, _, _| {
                Ok::<_, std::convert::Infallible>(ConsumerActivation::BlockingSnapshot)
            })
            .expect("infallible test activation mapping");

        assert!(generic.validate().is_ok());
        assert!(sealed.validate().is_ok());
    }

    #[test]
    fn generic_graph_requires_live_activation_when_channel_contract_requires_it() {
        assert!(
            generic_required_live_graph(TestActivation::Blocking)
                .validate()
                .is_err()
        );
        assert!(
            generic_required_live_graph(TestActivation::Live)
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn mapping_consumer_activations_preserves_binding_identity() {
        let graph = generic_join_graph(TestActivation::Blocking);
        let mapped = graph
            .map_consumer_activations(|_, _, _, activation| match activation {
                TestActivation::Blocking => Ok::<_, ()>(ConsumerActivation::BlockingSnapshot),
                TestActivation::Live => Ok(ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                }),
            })
            .unwrap();

        let producer = mapped.binding(BindingId::new(1)).unwrap();
        let consumer = mapped.binding(BindingId::new(2)).unwrap();
        assert_eq!(producer.channel_id, ChannelId::new(1));
        assert_eq!(consumer.channel_id, ChannelId::new(1));
        assert_eq!(producer.binding_id, BindingId::new(1));
        assert_eq!(consumer.binding_id, BindingId::new(2));
        assert!(matches!(
            producer.role,
            RuntimeFilterBindingRole::Producer(_)
        ));
        assert!(matches!(
            consumer.role,
            RuntimeFilterBindingRole::Consumer(_)
        ));
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
