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
use crate::runtime_filter::deployment::role_graph::{RoleGraph, RouteKind};
use crate::runtime_filter::deployment::{BindingInstanceIndex, DeploymentError};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, ReductionRequirement, RuntimeFilterLifecycle,
    RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::artifact::{ArtifactKind, ConsumerArtifactProfile};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::{
    ConsumerDeployment, MaterializationPolicy, OutboundMaterializationGroup,
    OutboundMaterializationOwner, ProducerDeployment, RuntimeFilterChannelDeployment,
    RuntimeFilterCoreBudget, RuntimeFilterInstallView,
};
use crate::runtime_filter::port::ordered_bound::RuntimeOrderContract;

/// Channel-level facts the projection stamps into each shard (mirrors the model
/// channel spec, plus the per-channel completion requirement and producer→witness
/// map the compiler pre-computes from the global graph).
#[derive(Clone, Debug)]
pub struct ChannelProjectionSpec {
    pub channel_id: ChannelId,
    pub logical_domain: RuntimeFilterLogicalDomain,
    pub lifecycle: RuntimeFilterLifecycle,
    pub availability_coverage: Coverage,
    pub terminal_coverage: Coverage,
    pub reduction_requirement: ReductionRequirement,
    pub allowed_contribution_kinds: BTreeSet<ContributionKind>,
    pub completion_requirement: CompletionRequirement,
    pub policy: RuntimeFilterPolicyRequirement,
    pub producer_witness: BTreeMap<BindingId, CoverageWitnessId>,
}

/// Consumer activation + capabilities, looked up per binding.
#[derive(Clone, Debug)]
pub struct ConsumerBindingFacts {
    pub activation: ConsumerActivation,
    pub capabilities: BTreeSet<ArtifactCapability>,
}

/// Lower a consumer's semantic `ArtifactCapability` set into the physical
/// `ConsumerArtifactProfile` the M2 install contract requires (RFD-3/M2 §159-162):
/// `Membership` → `ValueSet`, `EmptyDomain` → `EmptyDomain`, and an M3A
/// `OrderedBound` contract with `OrderedRange` capability → an exact Range
/// profile carrying the validated order digest. Membership channels never map
/// `OrderedRange` to Range. Bloom/Bitset are not selected here.
fn consumer_artifact_profile(
    logical_domain: &RuntimeFilterLogicalDomain,
    capabilities: &BTreeSet<ArtifactCapability>,
) -> Result<ConsumerArtifactProfile, DeploymentError> {
    if let RuntimeFilterLogicalDomain::OrderedBound(plan) = logical_domain {
        let contract = RuntimeOrderContract::try_from_plan(plan).map_err(|_| {
            DeploymentError::InvalidArtifactProfile(
                crate::runtime_filter::port::artifact::ArtifactContractError::UnsupportedSchema,
            )
        })?;
        if capabilities.contains(&ArtifactCapability::OrderedRange) {
            return ConsumerArtifactProfile::new_ordered_range(contract.digest())
                .map_err(DeploymentError::InvalidArtifactProfile);
        }
    }
    let mut accepted = BTreeSet::new();
    if capabilities.contains(&ArtifactCapability::Membership) {
        accepted.insert(ArtifactKind::ValueSet);
    }
    if capabilities.contains(&ArtifactCapability::EmptyDomain) {
        accepted.insert(ArtifactKind::EmptyDomain);
    }
    ConsumerArtifactProfile::new(accepted, None).map_err(DeploymentError::InvalidArtifactProfile)
}

/// Project the role graph + placement into per-participant install views.
///
/// Every participant receives Core authority for its real local producer and
/// consumer roles. Aggregators additionally receive query-global producer
/// authority so reduction agrees with the routing shard.
///
/// PRECONDITION: the caller (RFD-2's `compile`) MUST supply `channel_specs`,
/// `producer_witness`, and `consumer_facts` entries covering every channel /
/// producer binding / consumer binding present in `role_graph`. A
/// missing entry is logged (`tracing::warn!`) and the offending binding/channel
/// is skipped rather than panicking, except for an Aggregator's producer
/// authority: every producer witness and placement must be present so the Core
/// view cannot silently disagree with routing authorization.
///
/// Fails with [`DeploymentError::InvalidArtifactProfile`] if a consumer's
/// semantic capabilities cannot form a valid M2 physical artifact profile.
pub fn project_install_views(
    epoch: DeploymentEpoch,
    role_graph: &RoleGraph,
    channel_specs: &BTreeMap<ChannelId, ChannelProjectionSpec>,
    consumer_facts: &BTreeMap<BindingId, ConsumerBindingFacts>,
    instances: &BindingInstanceIndex,
    core_budget: RuntimeFilterCoreBudget,
    materialization: MaterializationPolicy,
) -> Result<BTreeMap<RuntimeFilterParticipantId, RuntimeFilterInstallView>, DeploymentError> {
    // participant -> channel -> (producers, consumers)
    #[allow(clippy::type_complexity)]
    let mut per_participant: BTreeMap<
        RuntimeFilterParticipantId,
        BTreeMap<
            ChannelId,
            (
                BTreeMap<BindingId, ProducerDeployment>,
                BTreeMap<BindingId, ConsumerDeployment>,
            ),
        >,
    > = BTreeMap::new();
    #[allow(clippy::type_complexity)]
    let mut materialization_groups: BTreeMap<
        RuntimeFilterParticipantId,
        BTreeMap<
            ChannelId,
            BTreeMap<
                crate::runtime_filter::port::artifact::ConsumerProfileId,
                (
                    OutboundMaterializationOwner,
                    ConsumerArtifactProfile,
                    BTreeSet<RouteEdgeId>,
                ),
            >,
        >,
    > = BTreeMap::new();
    for (channel_id, cg) in &role_graph.channels {
        let Some(spec) = channel_specs.get(channel_id) else {
            tracing::warn!(
                channel = channel_id.get(),
                "RFD-2 projection: channel missing from channel_specs; skipped"
            );
            continue;
        };
        // Producers: every hosted producer binding on each participant.
        for (participant, bindings) in &cg.producers {
            for binding in bindings {
                let Some(witness) = spec.producer_witness.get(binding).copied() else {
                    tracing::warn!(
                        channel = channel_id.get(),
                        binding = binding.get(),
                        "RFD-2 projection: producer binding missing witness; skipped"
                    );
                    continue;
                };
                let expected = instances
                    .get(&(*channel_id, *binding, *participant))
                    .cloned()
                    .unwrap_or_default();
                per_participant
                    .entry(*participant)
                    .or_default()
                    .entry(*channel_id)
                    .or_default()
                    .0
                    .insert(*binding, ProducerDeployment::new(witness, expected));
            }
        }
        // Aggregators reduce contributions from every producer participant, so
        // their Core authority must contain the same query-global finst set as
        // the routing shard. Keep non-aggregator producer views local-only.
        if let Some(aggregator) = cg.aggregator {
            let mut aggregator_producers: BTreeMap<BindingId, BTreeSet<UniqueId>> = BTreeMap::new();
            for (participant, bindings) in &cg.producers {
                for binding in bindings {
                    let Some(_witness) = spec.producer_witness.get(binding) else {
                        return Err(DeploymentError::InvalidInstallProjection {
                            detail: format!(
                                "runtime filter aggregator projection missing producer witness \
                                 for channel {} binding {}",
                                channel_id.get(),
                                binding.get()
                            ),
                        });
                    };
                    let Some(expected) = instances.get(&(*channel_id, *binding, *participant))
                    else {
                        return Err(DeploymentError::InvalidInstallProjection {
                            detail: format!(
                                "runtime filter aggregator projection missing producer placement \
                                 for channel {} binding {} participant {}",
                                channel_id.get(),
                                binding.get(),
                                participant.get()
                            ),
                        });
                    };
                    if expected.is_empty() {
                        return Err(DeploymentError::InvalidInstallProjection {
                            detail: format!(
                                "runtime filter aggregator projection missing producer placement \
                                 for channel {} binding {} participant {}",
                                channel_id.get(),
                                binding.get(),
                                participant.get()
                            ),
                        });
                    }
                    aggregator_producers
                        .entry(*binding)
                        .or_default()
                        .extend(expected.iter().copied());
                }
            }
            for (binding, expected) in aggregator_producers {
                let witness = spec.producer_witness[&binding];
                per_participant
                    .entry(aggregator)
                    .or_default()
                    .entry(*channel_id)
                    .or_default()
                    .0
                    .insert(binding, ProducerDeployment::new(witness, expected));
            }
        }
        // Consumers own every inbound delivery edge authorized by routing.
        // Loopback, direct remote, and aggregator delivery use one stable set.
        let inbound_routes: BTreeMap<
            (RuntimeFilterParticipantId, BindingId),
            BTreeSet<RouteEdgeId>,
        > = cg
            .routes
            .iter()
            .filter(|route| {
                matches!(
                    route.kind,
                    RouteKind::Loopback | RouteKind::ReplicaDirect | RouteKind::FromAggregator
                )
            })
            .fold(BTreeMap::new(), |mut routes, route| {
                routes
                    .entry((route.to.participant, route.to.binding))
                    .or_default()
                    .insert(route.edge_id);
                routes
            });
        for (participant, bindings) in &cg.consumers {
            for binding in bindings {
                let Some(route_edge_ids) = inbound_routes.get(&(*participant, *binding)).cloned()
                else {
                    return Err(DeploymentError::InvalidInstallProjection {
                        detail: format!(
                            "runtime filter consumer projection missing inbound route for channel {} binding {} participant {}",
                            channel_id.get(),
                            binding.get(),
                            participant.get()
                        ),
                    });
                };
                let Some(facts) = consumer_facts.get(binding) else {
                    tracing::warn!(
                        channel = channel_id.get(),
                        binding = binding.get(),
                        "RFD-2 projection: consumer binding missing consumer_facts; skipped"
                    );
                    continue;
                };
                let profile = consumer_artifact_profile(&spec.logical_domain, &facts.capabilities)?;
                let expected = instances
                    .get(&(*channel_id, *binding, *participant))
                    .cloned()
                    .unwrap_or_default();
                per_participant
                    .entry(*participant)
                    .or_default()
                    .entry(*channel_id)
                    .or_default()
                    .1
                    .insert(
                        *binding,
                        ConsumerDeployment::with_profile(
                            facts.activation,
                            facts.capabilities.clone(),
                            profile,
                            route_edge_ids,
                            expected,
                        ),
                    );
            }
        }
        for route in &cg.routes {
            let owner = match route.kind {
                RouteKind::Loopback | RouteKind::ReplicaDirect => {
                    OutboundMaterializationOwner::DirectSource
                }
                RouteKind::FromAggregator => OutboundMaterializationOwner::Aggregator,
                RouteKind::ToAggregator => continue,
            };
            let facts = consumer_facts.get(&route.to.binding).ok_or_else(|| {
                DeploymentError::InvalidInstallProjection {
                    detail: format!(
                        "runtime filter materialization projection missing consumer facts for channel {} binding {}",
                        channel_id.get(),
                        route.to.binding.get()
                    ),
                }
            })?;
            let profile = consumer_artifact_profile(&spec.logical_domain, &facts.capabilities)?;
            let profile_id = profile.id();
            let group = materialization_groups
                .entry(route.from.participant)
                .or_default()
                .entry(*channel_id)
                .or_default()
                .entry(profile_id)
                .or_insert_with(|| (owner, profile.clone(), BTreeSet::new()));
            if group.0 != owner || group.1.canonical_bytes() != profile.canonical_bytes() {
                return Err(DeploymentError::InvalidInstallProjection {
                    detail: format!(
                        "runtime filter materialization profile collision for channel {} profile {:?}",
                        channel_id.get(),
                        profile_id
                    ),
                });
            }
            group.2.insert(route.edge_id);
        }
    }

    let mut views = BTreeMap::new();
    for (participant, channels) in per_participant {
        let mut channel_deployments = BTreeMap::new();
        for (channel_id, (producers, consumers)) in channels {
            let spec = &channel_specs[&channel_id];
            let groups: BTreeMap<_, _> = materialization_groups
                .get(&participant)
                .and_then(|channels| channels.get(&channel_id))
                .into_iter()
                .flat_map(|groups| groups.iter())
                .map(|(profile_id, (owner, profile, routes))| {
                    (
                        *profile_id,
                        OutboundMaterializationGroup::new(*owner, profile.clone(), routes.clone()),
                    )
                })
                .collect();
            let channel_materialization = if groups.is_empty() {
                materialization
            } else {
                materialization
                    .with_max_concurrent_jobs(
                        materialization.max_concurrent_jobs().min(groups.len()),
                    )
                    .map_err(|error| DeploymentError::InvalidInstallProjection {
                        detail: format!(
                            "runtime filter channel {} materialization concurrency projection failed: {error:?}",
                            channel_id.get()
                        ),
                    })?
            };
            channel_deployments.insert(
                channel_id,
                RuntimeFilterChannelDeployment::new(
                    channel_id,
                    spec.logical_domain.clone(),
                    spec.lifecycle,
                    spec.availability_coverage.clone(),
                    spec.terminal_coverage.clone(),
                    spec.reduction_requirement,
                    spec.allowed_contribution_kinds.clone(),
                    spec.completion_requirement,
                    spec.policy,
                    core_budget,
                    channel_materialization,
                    producers,
                    consumers,
                )
                .with_outbound_materialization_groups(groups),
            );
        }
        if channel_deployments.is_empty() {
            continue;
        }
        views.insert(
            participant,
            RuntimeFilterInstallView::new(epoch, participant, channel_deployments),
        );
    }
    Ok(views)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::types::UniqueId;
    use crate::query_execution::backend::LiveBackendSnapshot;
    use crate::runtime_filter::deployment::role_graph::*;
    use crate::runtime_filter::deployment::routing_shard::project_routing_shards;
    use crate::runtime_filter::model::contract::*;
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
    use crate::runtime_filter::port::routing::RuntimeFilterRouteRole;

    fn membership_channel(id: u32) -> ChannelProjectionSpec {
        ChannelProjectionSpec {
            channel_id: ChannelId::new(id),
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
            completion_requirement: CompletionRequirement::ProducerClosed,
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 64,
                max_artifact_bytes: 128,
                deadline_ms: 1000,
                max_retries: 3,
            },
            producer_witness: BTreeMap::from([(BindingId::new(10), CoverageWitnessId::new(1))]),
        }
    }

    fn top_k_summary_channel(id: u32) -> ChannelProjectionSpec {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }];
        ChannelProjectionSpec {
            channel_id: ChannelId::new(id),
            logical_domain: RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
                comparator_digest:
                    crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
                        &keys,
                        crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
                    ),
                keys,
                inclusive: true,
            }),
            lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            reduction_requirement: ReductionRequirement::MergeTopKSummary(
                TopKSummaryRequirement::try_new(3).unwrap(),
            ),
            allowed_contribution_kinds: BTreeSet::from([
                ContributionKind::TopKSummary,
                ContributionKind::ProducerClosed,
            ]),
            completion_requirement: CompletionRequirement::ProducerClosed,
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 64,
                max_artifact_bytes: 128,
                deadline_ms: 1000,
                max_retries: 3,
            },
            producer_witness: BTreeMap::from([(BindingId::new(10), CoverageWitnessId::new(1))]),
        }
    }

    /// M2 Membership consumers must declare both `Membership` and `EmptyDomain`
    /// semantics (RFD-3/M2 install收紧 §158); the derived profile then accepts
    /// `{ValueSet, EmptyDomain}`.
    fn membership_consumer_facts(binding: u32) -> (BindingId, ConsumerBindingFacts) {
        (
            BindingId::new(binding),
            ConsumerBindingFacts {
                activation: ConsumerActivation::BlockingSnapshot,
                capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
            },
        )
    }

    fn pid(raw: u32) -> RuntimeFilterParticipantId {
        RuntimeFilterParticipantId::new(raw)
    }

    fn finst(raw: i64) -> UniqueId {
        UniqueId::new(raw, raw + 100)
    }

    fn consumer_route_ids(consumer: &ConsumerDeployment) -> BTreeSet<RouteEdgeId> {
        consumer.route_edge_ids().clone()
    }

    fn projection_backends(participants: impl IntoIterator<Item = u32>) -> LiveBackendSnapshot {
        LiveBackendSnapshot::new(
            participants
                .into_iter()
                .map(|participant| {
                    (
                        usize::try_from(participant - 1).unwrap(),
                        format!("10.0.0.{participant}:9060").parse().unwrap(),
                    )
                })
                .collect(),
        )
    }

    fn direct_projection_fixture(
        reverse_routes: bool,
    ) -> (
        RoleGraph,
        BTreeMap<ChannelId, ChannelProjectionSpec>,
        BTreeMap<BindingId, ConsumerBindingFacts>,
        BindingInstanceIndex,
    ) {
        let channel_id = ChannelId::new(5);
        let producer_binding = BindingId::new(10);
        let consumer_binding = BindingId::new(11);
        let producer_a = pid(2);
        let producer_b = pid(7);
        let consumer = pid(11);
        let mut routes = vec![
            RouteEdge {
                channel: channel_id,
                edge_id: RouteEdgeId::new(9),
                kind: RouteKind::ReplicaDirect,
                from: RouteEndpoint {
                    participant: producer_a,
                    binding: producer_binding,
                },
                to: RouteEndpoint {
                    participant: consumer,
                    binding: consumer_binding,
                },
            },
            RouteEdge {
                channel: channel_id,
                edge_id: RouteEdgeId::new(4),
                kind: RouteKind::ReplicaDirect,
                from: RouteEndpoint {
                    participant: producer_b,
                    binding: producer_binding,
                },
                to: RouteEndpoint {
                    participant: consumer,
                    binding: consumer_binding,
                },
            },
        ];
        if reverse_routes {
            routes.reverse();
        }
        let mut channel = ChannelRoleGraph::empty(channel_id);
        channel
            .producers
            .insert(producer_a, BTreeSet::from([producer_binding]));
        channel
            .producers
            .insert(producer_b, BTreeSet::from([producer_binding]));
        channel
            .consumers
            .insert(consumer, BTreeSet::from([consumer_binding]));
        channel.routes = routes;
        (
            RoleGraph {
                channels: BTreeMap::from([(channel_id, channel)]),
            },
            BTreeMap::from([(channel_id, membership_channel(channel_id.get()))]),
            BTreeMap::from([membership_consumer_facts(consumer_binding.get())]),
            BTreeMap::from([
                (
                    (channel_id, producer_binding, producer_a),
                    BTreeSet::from([finst(2)]),
                ),
                (
                    (channel_id, producer_binding, producer_b),
                    BTreeSet::from([finst(7)]),
                ),
                (
                    (channel_id, consumer_binding, consumer),
                    BTreeSet::from([finst(11)]),
                ),
            ]),
        )
    }

    fn all_of_projection_fixture() -> (
        RoleGraph,
        BTreeMap<ChannelId, ChannelProjectionSpec>,
        BTreeMap<BindingId, ConsumerBindingFacts>,
        BindingInstanceIndex,
    ) {
        let channel_id = ChannelId::new(5);
        let producer_binding = BindingId::new(10);
        let second_producer_binding = BindingId::new(20);
        let consumer_binding = BindingId::new(11);
        let aggregator = pid(2);
        let remote_producer = pid(7);
        let second_remote_producer = pid(13);
        let remote_consumer = pid(11);

        let mut channel = ChannelRoleGraph::empty(channel_id);
        channel
            .producers
            .insert(aggregator, BTreeSet::from([producer_binding]));
        channel.producers.insert(
            remote_producer,
            BTreeSet::from([producer_binding, second_producer_binding]),
        );
        channel.producers.insert(
            second_remote_producer,
            BTreeSet::from([second_producer_binding]),
        );
        channel
            .consumers
            .insert(remote_consumer, BTreeSet::from([consumer_binding]));
        channel.aggregator = Some(aggregator);
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(1),
            kind: RouteKind::ToAggregator,
            from: RouteEndpoint {
                participant: aggregator,
                binding: producer_binding,
            },
            to: RouteEndpoint {
                participant: aggregator,
                binding: producer_binding,
            },
        });
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(2),
            kind: RouteKind::ToAggregator,
            from: RouteEndpoint {
                participant: remote_producer,
                binding: producer_binding,
            },
            to: RouteEndpoint {
                participant: aggregator,
                binding: producer_binding,
            },
        });
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(3),
            kind: RouteKind::ToAggregator,
            from: RouteEndpoint {
                participant: remote_producer,
                binding: second_producer_binding,
            },
            to: RouteEndpoint {
                participant: aggregator,
                binding: second_producer_binding,
            },
        });
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(4),
            kind: RouteKind::ToAggregator,
            from: RouteEndpoint {
                participant: second_remote_producer,
                binding: second_producer_binding,
            },
            to: RouteEndpoint {
                participant: aggregator,
                binding: second_producer_binding,
            },
        });
        channel.routes.push(RouteEdge {
            channel: channel_id,
            edge_id: RouteEdgeId::new(5),
            kind: RouteKind::FromAggregator,
            from: RouteEndpoint {
                participant: aggregator,
                binding: consumer_binding,
            },
            to: RouteEndpoint {
                participant: remote_consumer,
                binding: consumer_binding,
            },
        });

        let instances = BTreeMap::from([
            (
                (channel_id, producer_binding, aggregator),
                BTreeSet::from([finst(2)]),
            ),
            (
                (channel_id, producer_binding, remote_producer),
                BTreeSet::from([finst(7)]),
            ),
            (
                (channel_id, second_producer_binding, remote_producer),
                BTreeSet::from([finst(17)]),
            ),
            (
                (channel_id, second_producer_binding, second_remote_producer),
                BTreeSet::from([finst(13)]),
            ),
            (
                (channel_id, consumer_binding, remote_consumer),
                BTreeSet::from([finst(11)]),
            ),
        ]);
        let mut channel_spec = membership_channel(5);
        channel_spec
            .producer_witness
            .insert(second_producer_binding, CoverageWitnessId::new(2));

        (
            RoleGraph {
                channels: BTreeMap::from([(channel_id, channel)]),
            },
            BTreeMap::from([(channel_id, channel_spec)]),
            BTreeMap::from([membership_consumer_facts(11)]),
            instances,
        )
    }

    fn project_all_of_fixture()
    -> Result<BTreeMap<RuntimeFilterParticipantId, RuntimeFilterInstallView>, DeploymentError> {
        let (role_graph, channel_specs, consumer_facts, instances) = all_of_projection_fixture();
        project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
    }

    #[test]
    fn remote_direct_consumer_core_routes_match_routing_shard() {
        let (role_graph, channel_specs, consumer_facts, instances) =
            direct_projection_fixture(false);
        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("projection succeeds");
        let routing = project_routing_shards(
            DeploymentEpoch::new(9),
            &role_graph,
            &instances,
            &projection_backends([2, 7, 11]),
        )
        .expect("routing projection succeeds");
        let core_routes = consumer_route_ids(
            &views[&pid(11)].channels()[&ChannelId::new(5)].consumers()[&BindingId::new(11)],
        );
        let routing_routes = routing[&pid(11)].channels()[&ChannelId::new(5)]
            .inbound_edges()
            .iter()
            .map(|edge| edge.route_edge_id())
            .collect::<BTreeSet<_>>();

        assert_eq!(core_routes, routing_routes);
    }

    #[test]
    fn direct_sources_own_only_their_outbound_materialization_routes() {
        let (role_graph, channel_specs, consumer_facts, instances) =
            direct_projection_fixture(false);
        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("projection succeeds");

        let direct_a =
            views[&pid(2)].channels()[&ChannelId::new(5)].outbound_materialization_groups();
        let direct_b =
            views[&pid(7)].channels()[&ChannelId::new(5)].outbound_materialization_groups();
        assert_eq!(direct_a.len(), 1);
        assert_eq!(direct_b.len(), 1);
        assert_eq!(
            direct_a.values().next().unwrap().route_edge_ids(),
            &BTreeSet::from([RouteEdgeId::new(9)])
        );
        assert_eq!(
            direct_b.values().next().unwrap().route_edge_ids(),
            &BTreeSet::from([RouteEdgeId::new(4)])
        );
        assert!(
            views[&pid(11)].channels()[&ChannelId::new(5)]
                .outbound_materialization_groups()
                .is_empty(),
            "consumer-only participants never own materialization"
        );
    }

    #[test]
    fn projected_channel_clamps_query_concurrency_to_owned_profiles() {
        let (role_graph, channel_specs, consumer_facts, instances) =
            direct_projection_fixture(false);
        let query_policy = MaterializationPolicy::new(8, 5, 17, 1, 1 << 20, 1 << 16, 2)
            .expect("valid multi-channel query policy");
        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            query_policy,
        )
        .expect("projection succeeds");

        for participant in [pid(2), pid(7)] {
            let channel = &views[&participant].channels()[&ChannelId::new(5)];
            assert_eq!(channel.outbound_materialization_groups().len(), 1);
            assert_eq!(channel.materialization_policy().max_concurrent_jobs(), 1);
        }
    }

    #[test]
    fn projected_channel_preserves_concurrency_for_multiple_owned_profiles() {
        let query_policy = MaterializationPolicy::new(8, 5, 17, 1, 1 << 20, 1 << 16, 2)
            .expect("valid multi-channel query policy");

        let projected = query_policy
            .with_max_concurrent_jobs(query_policy.max_concurrent_jobs().min(2))
            .expect("valid projected policy");

        assert_eq!(projected.max_concurrent_jobs(), 2);
        assert_eq!(
            projected.bloom_bits_per_key(),
            query_policy.bloom_bits_per_key()
        );
        assert_eq!(
            projected.max_total_retained_bytes(),
            query_policy.max_total_retained_bytes()
        );
        assert_eq!(
            projected.max_scratch_bytes_per_job(),
            query_policy.max_scratch_bytes_per_job()
        );
    }

    #[test]
    fn only_allof_aggregator_owns_outbound_materialization() {
        let views = project_all_of_fixture().expect("projection succeeds");

        let aggregator =
            views[&pid(2)].channels()[&ChannelId::new(5)].outbound_materialization_groups();
        assert_eq!(aggregator.len(), 1);
        assert_eq!(
            aggregator.values().next().unwrap().route_edge_ids(),
            &BTreeSet::from([RouteEdgeId::new(5)])
        );
        for participant in [pid(7), pid(13), pid(11)] {
            assert!(
                views[&participant].channels()[&ChannelId::new(5)]
                    .outbound_materialization_groups()
                    .is_empty(),
                "contribution-only producers and consumer-only participants do not materialize"
            );
        }
    }

    #[test]
    fn aggregate_remote_consumer_core_routes_match_from_aggregator_edges() {
        let (role_graph, channel_specs, consumer_facts, instances) = all_of_projection_fixture();
        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("projection succeeds");
        let routing = project_routing_shards(
            DeploymentEpoch::new(9),
            &role_graph,
            &instances,
            &projection_backends([2, 7, 11, 13]),
        )
        .expect("routing projection succeeds");
        let core_routes = consumer_route_ids(
            &views[&pid(11)].channels()[&ChannelId::new(5)].consumers()[&BindingId::new(11)],
        );
        let routing_routes = routing[&pid(11)].channels()[&ChannelId::new(5)]
            .inbound_edges()
            .iter()
            .filter(|edge| edge.source().role() == RuntimeFilterRouteRole::Aggregator)
            .map(|edge| edge.route_edge_id())
            .collect::<BTreeSet<_>>();

        assert_eq!(core_routes, routing_routes);
    }

    #[test]
    fn consumer_with_multiple_inbound_routes_keeps_every_route() {
        let (role_graph, channel_specs, consumer_facts, instances) =
            direct_projection_fixture(false);
        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("projection succeeds");
        let consumer =
            &views[&pid(11)].channels()[&ChannelId::new(5)].consumers()[&BindingId::new(11)];
        assert_eq!(
            consumer_route_ids(consumer),
            BTreeSet::from([RouteEdgeId::new(4), RouteEdgeId::new(9)])
        );

        let (reversed_graph, reversed_specs, reversed_facts, reversed_instances) =
            direct_projection_fixture(true);
        let reversed = project_install_views(
            DeploymentEpoch::new(9),
            &reversed_graph,
            &reversed_specs,
            &reversed_facts,
            &reversed_instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("reordered projection succeeds");
        assert_eq!(
            views, reversed,
            "route input order must not affect install views"
        );
    }

    #[test]
    fn all_of_aggregator_projects_union_of_remote_producer_instances() {
        let views = project_all_of_fixture().expect("projection succeeds");

        let aggregator_producer =
            &views[&pid(2)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(10)];
        assert_eq!(
            aggregator_producer.expected_fragment_instances(),
            &BTreeSet::from([finst(2), finst(7)])
        );
        assert_eq!(
            views[&pid(2)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(20)]
                .expected_fragment_instances(),
            &BTreeSet::from([finst(13), finst(17)])
        );
    }

    #[test]
    fn aggregator_without_local_consumer_still_gets_core_channel() {
        let views = project_all_of_fixture().expect("projection succeeds");

        let channel = &views[&pid(2)].channels()[&ChannelId::new(5)];
        assert_eq!(
            channel.producers()[&BindingId::new(10)].expected_fragment_instances(),
            &BTreeSet::from([finst(2), finst(7)])
        );
        assert!(channel.consumers().is_empty());
    }

    #[test]
    fn source_only_non_aggregator_producer_gets_local_core_authority() {
        let views = project_all_of_fixture().expect("projection succeeds");

        assert_eq!(
            views[&pid(7)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(10)]
                .expected_fragment_instances(),
            &BTreeSet::from([finst(7)])
        );
        assert_eq!(
            views[&pid(13)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(20)]
                .expected_fragment_instances(),
            &BTreeSet::from([finst(13)])
        );
        assert_eq!(
            views[&pid(2)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(10)]
                .expected_fragment_instances(),
            &BTreeSet::from([finst(2), finst(7)])
        );
        assert_eq!(
            views[&pid(2)].channels()[&ChannelId::new(5)].producers()[&BindingId::new(20)]
                .expected_fragment_instances(),
            &BTreeSet::from([finst(13), finst(17)])
        );
    }

    #[test]
    fn remote_only_consumer_gets_from_aggregator_core_authority() {
        let views = project_all_of_fixture().expect("projection succeeds");

        assert_eq!(
            views[&pid(11)].channels()[&ChannelId::new(5)].consumers()[&BindingId::new(11)]
                .route_edge_ids(),
            &BTreeSet::from([RouteEdgeId::new(5)])
        );
    }

    #[test]
    fn all_of_aggregator_rejects_missing_producer_witness() {
        let (role_graph, mut channel_specs, consumer_facts, instances) =
            all_of_projection_fixture();
        channel_specs
            .get_mut(&ChannelId::new(5))
            .unwrap()
            .producer_witness
            .clear();

        let err = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            DeploymentError::InvalidInstallProjection { detail }
                if detail.contains("producer witness")
        ));
    }

    #[test]
    fn all_of_aggregator_rejects_missing_producer_placement() {
        let (role_graph, channel_specs, consumer_facts, mut instances) =
            all_of_projection_fixture();
        instances.remove(&(ChannelId::new(5), BindingId::new(10), pid(7)));

        let err = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            DeploymentError::InvalidInstallProjection { detail }
                if detail.contains("producer placement")
        ));
    }

    #[test]
    fn ordered_range_projector_emits_exact_profile_contract() {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let plan = OrderContract {
            comparator_digest:
                crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
                    &keys,
                    crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
                ),
            keys,
            inclusive: true,
        };
        let expected =
            crate::runtime_filter::port::ordered_bound::RuntimeOrderContract::try_from_plan(&plan)
                .unwrap()
                .digest();
        let profile = consumer_artifact_profile(
            &RuntimeFilterLogicalDomain::OrderedBound(plan),
            &BTreeSet::from([ArtifactCapability::OrderedRange]),
        )
        .unwrap();

        assert_eq!(
            profile.accepted_kinds(),
            &BTreeSet::from([ArtifactKind::Range])
        );
        assert_eq!(profile.order_contract_digest(), Some(expected));
    }

    #[test]
    fn projection_preserves_top_k_summary_requirement() {
        let participant = RuntimeFilterParticipantId::new(1);
        let mut channel_graph = ChannelRoleGraph::empty(ChannelId::new(5));
        channel_graph
            .producers
            .insert(participant, BTreeSet::from([BindingId::new(10)]));
        channel_graph.aggregator = Some(participant);
        let mut role_graph = RoleGraph::default();
        role_graph.channels.insert(ChannelId::new(5), channel_graph);

        let projected = top_k_summary_channel(5);
        assert_eq!(
            projected.reduction_requirement,
            ReductionRequirement::MergeTopKSummary(TopKSummaryRequirement::try_new(3).unwrap())
        );
        let channel_specs = BTreeMap::from([(ChannelId::new(5), projected)]);
        let instances = BTreeMap::from([(
            (ChannelId::new(5), BindingId::new(10), participant),
            BTreeSet::from([UniqueId::new(1, 2)]),
        )]);

        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &BTreeMap::new(),
            &instances,
            crate::runtime_filter::port::install::RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("projection succeeds");
        let deployment = &views[&participant].channels()[&ChannelId::new(5)];
        assert_eq!(
            deployment.reduction_requirement(),
            ReductionRequirement::MergeTopKSummary(TopKSummaryRequirement::try_new(3).unwrap())
        );
    }

    #[test]
    fn loopback_projection_passes_be_side_validate_view() {
        let part = RuntimeFilterParticipantId::new(1);
        let finst = UniqueId::new(1, 2);
        let mut cg = ChannelRoleGraph::empty(ChannelId::new(5));
        cg.producers
            .insert(part, BTreeSet::from([BindingId::new(10)]));
        cg.consumers
            .insert(part, BTreeSet::from([BindingId::new(11)]));
        cg.routes.push(RouteEdge {
            channel: ChannelId::new(5),
            edge_id: crate::runtime_filter::port::identity::RouteEdgeId::new(1),
            kind: RouteKind::Loopback,
            from: RouteEndpoint {
                participant: part,
                binding: BindingId::new(10),
            },
            to: RouteEndpoint {
                participant: part,
                binding: BindingId::new(11),
            },
        });
        let mut role_graph = RoleGraph::default();
        role_graph.channels.insert(ChannelId::new(5), cg);

        let mut instances: BTreeMap<
            (ChannelId, BindingId, RuntimeFilterParticipantId),
            BTreeSet<UniqueId>,
        > = BTreeMap::new();
        instances.insert(
            (ChannelId::new(5), BindingId::new(10), part),
            BTreeSet::from([finst]),
        );
        instances.insert(
            (ChannelId::new(5), BindingId::new(11), part),
            BTreeSet::from([finst]),
        );

        let mut channel_specs = BTreeMap::new();
        channel_specs.insert(ChannelId::new(5), membership_channel(5));

        let consumer_facts = BTreeMap::from([membership_consumer_facts(11)]);

        let views = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            crate::runtime_filter::port::install::RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .expect("projection succeeds");
        let view = views.get(&part).expect("participant has a view");
        // Reuse the BE-side validator to prove the shard is well-formed.
        crate::runtime_filter::deployment::install_validation::validate_install_view_contract_for_test(
            view,
        )
        .map_err(|error| error.to_string())
            .expect("compiler output must satisfy BE install contract");
    }

    #[test]
    fn consumer_without_authorized_inbound_route_is_rejected() {
        let producer_participant = RuntimeFilterParticipantId::new(1);
        let consumer_participant = RuntimeFilterParticipantId::new(2);
        let finst = UniqueId::new(1, 2);

        let mut cg = ChannelRoleGraph::empty(ChannelId::new(5));
        cg.producers
            .insert(producer_participant, BTreeSet::from([BindingId::new(10)]));
        cg.consumers
            .insert(consumer_participant, BTreeSet::from([BindingId::new(11)]));
        // Deliberately no RouteKind::Loopback edge to (consumer_participant, 11).

        let mut role_graph = RoleGraph::default();
        role_graph.channels.insert(ChannelId::new(5), cg);

        let mut instances: BTreeMap<
            (ChannelId, BindingId, RuntimeFilterParticipantId),
            BTreeSet<UniqueId>,
        > = BTreeMap::new();
        instances.insert(
            (ChannelId::new(5), BindingId::new(10), producer_participant),
            BTreeSet::from([finst]),
        );
        instances.insert(
            (ChannelId::new(5), BindingId::new(11), consumer_participant),
            BTreeSet::from([finst]),
        );

        let mut channel_specs = BTreeMap::new();
        channel_specs.insert(ChannelId::new(5), membership_channel(5));

        let consumer_facts = BTreeMap::from([membership_consumer_facts(11)]);

        let error = project_install_views(
            DeploymentEpoch::new(9),
            &role_graph,
            &channel_specs,
            &consumer_facts,
            &instances,
            crate::runtime_filter::port::install::RuntimeFilterCoreBudget::new(512),
            MaterializationPolicy::for_test(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DeploymentError::InvalidInstallProjection { detail }
                if detail.contains("consumer projection missing inbound route")
        ));
    }
}
