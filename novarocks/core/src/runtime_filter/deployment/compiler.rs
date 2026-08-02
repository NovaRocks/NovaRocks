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
use crate::query_execution::backend::LiveBackendSnapshot;
use crate::query_execution::schedule::SchedulingPlan;
use crate::runtime_filter::deployment::planning_adapter as planning;
use crate::runtime_filter::deployment::role_graph::{
    ChannelRoleInputs, ConsumerPlacement, ProducerPlacement, RoleGraph, RouteEdgeAllocator,
    build_channel_role_graph,
};
use crate::runtime_filter::deployment::routing_shard::project_routing_shards;
use crate::runtime_filter::deployment::shard::{
    ChannelProjectionSpec, ConsumerBindingFacts, project_install_views,
};
use crate::runtime_filter::deployment::wait_for::validate_wait_for;
use crate::runtime_filter::deployment::{
    DeploymentError, RuntimeFilterDeploymentPlan, RuntimeFilterDeploymentPolicy,
    participant_id_for_backend,
};
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, CompletionRequirement, CoverageWitnessId,
};
use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
use crate::sql::planner::distributed::{FragmentEdge, FragmentId};
use crate::sql::planner::runtime_filter::graph::{
    RuntimeFilterBindingRole as SqlRuntimeFilterBindingRole, RuntimeFilterGraph,
};
use crate::sql::planner::runtime_filter::progress::JoinBuildProgressCatalog;
use crate::sql::planner::runtime_filter::wait_graph::{
    ConsumerWaitBehavior, project_consumer_waits,
};

/// Compile a query-global [`RuntimeFilterGraph`] plus COOR-2 scheduling/placement
/// into a coordinator-side [`RuntimeFilterDeploymentPlan`]: a full role graph
/// (Producer/Aggregator/Relay/Consumer, all routes) and the per-participant
/// loopback `RuntimeFilterInstallView` shards each BE installs today.
///
/// Pipeline: `graph.validate()` -> build the fragment `ExecutionDependencyGraph`
/// -> resolve per-(channel,binding) participant placement -> reject
/// `BlockingSnapshot` feedback cycles via `validate_wait_for` -> build each
/// channel's role graph -> project loopback install views and remote-aware
/// routing shards under one epoch -> assemble the plan.
///
/// Pure and deterministic: never mutates `scheduling`, iterates only
/// `BTreeMap`/`BTreeSet`, and never hardcodes backend/replica counts (they are
/// read from `backends` and clamped by `policy.replica_redundancy`).
pub fn compile(
    graph: &RuntimeFilterGraph,
    scheduling: &SchedulingPlan,
    edges: &[FragmentEdge],
    backends: &LiveBackendSnapshot,
    policy: &RuntimeFilterDeploymentPolicy,
    epoch: DeploymentEpoch,
) -> Result<RuntimeFilterDeploymentPlan, DeploymentError> {
    compile_with_join_progress(
        graph,
        scheduling,
        edges,
        &JoinBuildProgressCatalog::default(),
        backends,
        policy,
        epoch,
    )
}

pub fn compile_with_join_progress(
    graph: &RuntimeFilterGraph,
    scheduling: &SchedulingPlan,
    edges: &[FragmentEdge],
    join_progress: &JoinBuildProgressCatalog,
    backends: &LiveBackendSnapshot,
    policy: &RuntimeFilterDeploymentPolicy,
    epoch: DeploymentEpoch,
) -> Result<RuntimeFilterDeploymentPlan, DeploymentError> {
    // 1. RFD-1 validation first: no downstream step may paper over a graph the
    // model itself already rejects.
    graph.validate().map_err(DeploymentError::GraphInvalid)?;

    // 2. Resolve per-(channel,binding) participant placement + the expected
    // finst instances from the scheduling plan. Participant identity is the
    // checked, nonzero deployment identity derived from `backend_idx`; the
    // original backend index remains the scheduling and endpoint-map key.
    let mut known_backends = BTreeSet::new();
    for (backend_idx, _) in backends.entries() {
        let _ = participant_id_for_backend(*backend_idx)?;
        if !known_backends.insert(*backend_idx) {
            return Err(DeploymentError::DuplicateBackend {
                backend_idx: *backend_idx,
            });
        }
    }
    let mut instances: BTreeMap<
        (ChannelId, BindingId, RuntimeFilterParticipantId),
        BTreeSet<UniqueId>,
    > = BTreeMap::new();
    let mut producer_placements: BTreeMap<ChannelId, Vec<ProducerPlacement>> = BTreeMap::new();
    let mut consumer_placements: BTreeMap<ChannelId, Vec<ConsumerPlacement>> = BTreeMap::new();
    let mut consumer_facts: BTreeMap<BindingId, ConsumerBindingFacts> = BTreeMap::new();
    let mut channel_completion: BTreeMap<ChannelId, CompletionRequirement> = BTreeMap::new();
    let mut producer_witness: BTreeMap<ChannelId, BTreeMap<BindingId, CoverageWitnessId>> =
        BTreeMap::new();

    for binding in graph.bindings() {
        let fragment: FragmentId = binding.location.fragment_id.get();
        let placements =
            scheduling
                .by_fragment
                .get(&fragment)
                .ok_or(DeploymentError::MissingPlacement {
                    fragment: planning::fragment_id(binding.location.fragment_id),
                })?;
        if placements.is_empty() {
            return Err(DeploymentError::MissingPlacement {
                fragment: planning::fragment_id(binding.location.fragment_id),
            });
        }
        let mut participants: BTreeSet<RuntimeFilterParticipantId> = BTreeSet::new();
        for p in placements {
            if !known_backends.contains(&p.backend_idx) {
                return Err(DeploymentError::UnknownBackend {
                    backend_idx: p.backend_idx,
                });
            }
            let participant = participant_id_for_backend(p.backend_idx)?;
            participants.insert(participant);
            instances
                .entry((
                    planning::channel_id(binding.channel_id),
                    planning::binding_id(binding.binding_id),
                    participant,
                ))
                .or_default()
                .insert(p.finst_id);
        }
        match &binding.role {
            SqlRuntimeFilterBindingRole::Producer(req) => {
                producer_placements
                    .entry(planning::channel_id(binding.channel_id))
                    .or_default()
                    .push(ProducerPlacement {
                        binding: planning::binding_id(binding.binding_id),
                        participants,
                    });
                channel_completion
                    .entry(planning::channel_id(binding.channel_id))
                    .or_insert(planning::completion(req.completion_requirement));
                if let Some(witness) = binding.coverage_witness_id {
                    producer_witness
                        .entry(planning::channel_id(binding.channel_id))
                        .or_default()
                        .insert(
                            planning::binding_id(binding.binding_id),
                            planning::witness_id(witness),
                        );
                }
            }
            SqlRuntimeFilterBindingRole::Consumer(req) => {
                consumer_placements
                    .entry(planning::channel_id(binding.channel_id))
                    .or_default()
                    .push(ConsumerPlacement {
                        binding: planning::binding_id(binding.binding_id),
                        participants,
                    });
                consumer_facts.insert(
                    planning::binding_id(binding.binding_id),
                    ConsumerBindingFacts {
                        activation: planning::activation(req.activation),
                        capabilities: planning::capabilities(&req.capabilities),
                    },
                );
            }
        }
    }

    // 3. Wait-for cycle validation: only `BlockingSnapshot` consumers add a
    // wait edge, and only a real execution-topology cycle is rejected.
    let refined_edges = edges
        .iter()
        .map(FragmentEdge::as_refined_runtime_filter_edge)
        .collect::<Vec<_>>();
    let consumer_waits = project_consumer_waits(graph, |activation| match activation {
        crate::sql::planner::runtime_filter::contract::ConsumerActivation::BlockingSnapshot => {
            ConsumerWaitBehavior::BlocksUntilComplete
        }
        crate::sql::planner::runtime_filter::contract::ConsumerActivation::NonBlockingLive {
            ..
        } => ConsumerWaitBehavior::NeverBlocks,
    });
    validate_wait_for(&refined_edges, &consumer_waits, join_progress)?;

    // 4. Role graph per channel + the per-channel projection spec the shard
    // projector needs. The completion requirement is precomputed here from
    // the channel's producer bindings, since the model channel spec itself
    // carries no completion field (`graph.validate()` guarantees every
    // channel has >=1 producer, all agreeing on this value).
    let mut alloc = RouteEdgeAllocator::new();
    let mut role_graph = RoleGraph::default();
    let mut channel_specs: BTreeMap<ChannelId, ChannelProjectionSpec> = BTreeMap::new();
    for channel in graph.channels() {
        let channel_id = planning::channel_id(channel.channel_id);
        let inputs = ChannelRoleInputs {
            channel_id,
            availability_coverage: planning::coverage(&channel.availability_coverage),
            producers: producer_placements
                .get(&channel_id)
                .cloned()
                .unwrap_or_default(),
            consumers: consumer_placements
                .get(&channel_id)
                .cloned()
                .unwrap_or_default(),
        };
        let channel_role_graph =
            build_channel_role_graph(&inputs, policy.replica_redundancy, &mut alloc);
        role_graph.channels.insert(channel_id, channel_role_graph);

        let completion =
            channel_completion
                .get(&channel_id)
                .copied()
                .ok_or(DeploymentError::EmptyCoverage {
                    channel: channel_id,
                })?;
        channel_specs.insert(
            channel_id,
            ChannelProjectionSpec {
                channel_id,
                logical_domain: planning::logical_domain(&channel.logical_domain),
                lifecycle: planning::lifecycle(channel.lifecycle),
                availability_coverage: planning::coverage(&channel.availability_coverage),
                terminal_coverage: planning::coverage(&channel.terminal_coverage),
                reduction_requirement: planning::reduction(channel.reduction_requirement),
                allowed_contribution_kinds: planning::contribution_kinds(
                    &channel.allowed_contribution_kinds,
                ),
                completion_requirement: completion,
                policy: planning::policy(channel.policy),
                producer_witness: producer_witness
                    .get(&channel_id)
                    .cloned()
                    .unwrap_or_default(),
            },
        );
    }

    // 6. Project loopback install views and remote-aware routing shards
    // atomically under the same epoch.
    let install_views = project_install_views(
        epoch,
        &role_graph,
        &channel_specs,
        &consumer_facts,
        &instances,
        policy.core_budget,
        policy.materialization,
    )?;
    let routing_shards = project_routing_shards(epoch, &role_graph, &instances, backends)?;

    let participants = known_backends
        .into_iter()
        .map(participant_id_for_backend)
        .collect::<Result<BTreeSet<_>, _>>()?;

    Ok(RuntimeFilterDeploymentPlan {
        epoch,
        participants,
        install_views,
        routing_shards,
        role_graph,
    })
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::query_execution::schedule::FragmentInstancePlacement;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::deployment::extension::RuntimeFilterDeploymentExtension;
    use crate::runtime_filter::deployment::role_graph::RouteKind;
    use crate::runtime_filter::port::install::{MaterializationPolicy, RuntimeFilterCoreBudget};
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::planner::distributed::{
        DataPartition, FragmentEdgeKind, FragmentStreamKind, PartitionKind,
    };
    use crate::sql::planner::runtime_filter::comparator::comparator_digest_for_plan;
    use crate::sql::planner::runtime_filter::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder, NullSemantics,
        OrderContract, OrderKeyContract, PlanFragmentId, PlanNodeId, ReductionRequirement,
        RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
        SortDirection, TopKSummaryRequirement,
    };
    use crate::sql::planner::runtime_filter::coverage::Coverage;
    use crate::sql::planner::runtime_filter::graph::{
        ApplyPoint, ConsumerBindingTarget, ConsumerRequirement, PlanLocation,
        ProducerBindingTarget, ProducerRequirement, RuntimeFilterBindingRole,
        RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
    };

    /// Minimal typed expression used by compiler fixtures.
    fn sample_typed_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn placement(
        fragment_id: u32,
        instance_index: usize,
        backend_idx: usize,
        finst: UniqueId,
    ) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id: finst,
            backend_idx,
            endpoint: RuntimeEndpoint::from_socket_addr("127.0.0.1:9060".parse().unwrap()),
            scan_ranges: BTreeMap::new(),
            connector_splits: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    fn pid(raw: u32) -> RuntimeFilterParticipantId {
        RuntimeFilterParticipantId::new(raw)
    }

    fn edge(source: u32, target: u32) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: source,
            target_fragment_id: target,
            target_exchange_node_id: 0,
            output_partition: DataPartition {
                kind: PartitionKind::Unpartitioned,
                exprs: Vec::new(),
            },
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    /// Membership/CompleteOnce/SetUnion Join-shaped channel fixture.
    fn channel_spec(id: u32) -> RuntimeFilterChannelSpec {
        RuntimeFilterChannelSpec {
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
            required_consumer_capabilities: BTreeSet::from([ArtifactCapability::Membership]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 64,
                max_artifact_bytes: 128,
                deadline_ms: 1000,
                max_retries: 3,
            },
        }
    }

    fn top_k_summary_channel_spec(id: u32) -> RuntimeFilterChannelSpec {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }];
        RuntimeFilterChannelSpec {
            channel_id: ChannelId::new(id),
            logical_domain: RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
                comparator_digest: comparator_digest_for_plan(&keys).expect("supported order"),
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
            required_consumer_capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 64,
                max_artifact_bytes: 128,
                deadline_ms: 1000,
                max_retries: 3,
            },
        }
    }

    fn producer_binding(binding: u32, channel: u32, fragment: u32) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id: BindingId::new(binding),
            channel_id: ChannelId::new(channel),
            coverage_witness_id: Some(CoverageWitnessId::new(1)),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment),
                node_id: PlanNodeId::new(1),
            },
            expression: sample_typed_expr(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                // Membership producers without FinalDomainShard must include
                // ValueDomainDelta and ProducerClosed.
                contribution_kinds: BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ]),
                completion_requirement: CompletionRequirement::ProducerClosed,
                target: ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
            }),
        }
    }

    fn top_k_summary_producer_binding(
        binding: u32,
        channel: u32,
        fragment: u32,
    ) -> RuntimeFilterBindingSpec {
        let mut binding = producer_binding(binding, channel, fragment);
        let RuntimeFilterBindingRole::Producer(requirement) = &mut binding.role else {
            unreachable!("producer_binding always returns a producer")
        };
        requirement.contribution_kinds = BTreeSet::from([
            ContributionKind::TopKSummary,
            ContributionKind::ProducerClosed,
        ]);
        binding
    }

    fn consumer_binding(
        binding: u32,
        channel: u32,
        fragment: u32,
        activation: ConsumerActivation,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id: BindingId::new(binding),
            channel_id: ChannelId::new(channel),
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment),
                node_id: PlanNodeId::new(2),
            },
            expression: sample_typed_expr(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                // M2 install requires Membership consumers to also declare
                // EmptyDomain (RFD-3/M2 §158 收紧); the derived physical profile
                // then accepts {ValueSet, EmptyDomain}.
                capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                activation,
                target: ConsumerBindingTarget::SourceBoundary,
            }),
        }
    }

    fn top_k_summary_consumer_binding(
        binding: u32,
        channel: u32,
        fragment: u32,
    ) -> RuntimeFilterBindingSpec {
        let mut binding = consumer_binding(
            binding,
            channel,
            fragment,
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
        );
        let RuntimeFilterBindingRole::Consumer(requirement) = &mut binding.role else {
            unreachable!("consumer_binding always returns a consumer")
        };
        requirement.capabilities = BTreeSet::from([ArtifactCapability::OrderedRange]);
        binding
    }

    fn deployment_policy(replica_redundancy: u32) -> RuntimeFilterDeploymentPolicy {
        RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(1024),
            replica_redundancy,
            materialization: MaterializationPolicy::for_test(),
        }
    }

    fn all_of_compiler_fixture() -> (
        RuntimeFilterGraph,
        SchedulingPlan,
        Vec<FragmentEdge>,
        LiveBackendSnapshot,
        RuntimeFilterDeploymentPolicy,
    ) {
        let mut channel = channel_spec(5);
        channel.availability_coverage =
            Coverage::AllOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]);
        channel.terminal_coverage =
            Coverage::AllOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]);
        let mut graph = RuntimeFilterGraph::default();
        graph.insert_channel(channel).unwrap();
        graph.insert_binding(producer_binding(10, 5, 2)).unwrap();
        graph
            .insert_binding(consumer_binding(
                11,
                5,
                1,
                ConsumerActivation::BlockingSnapshot,
            ))
            .unwrap();

        let mut by_fragment = BTreeMap::new();
        by_fragment.insert(
            1u32,
            vec![
                placement(1, 0, 1, UniqueId::new(1, 1)),
                placement(1, 1, 10, UniqueId::new(1, 2)),
            ],
        );
        by_fragment.insert(
            2u32,
            vec![
                placement(2, 0, 1, UniqueId::new(1, 3)),
                placement(2, 1, 6, UniqueId::new(1, 4)),
            ],
        );
        let scheduling = SchedulingPlan {
            root_fragment_id: 1,
            by_fragment,
            root_finst_id: UniqueId::new(1, 1),
            root_backend_idx: 1,
        };
        let backends = LiveBackendSnapshot::new(vec![
            (1, "10.0.0.2:9060".parse().unwrap()),
            (6, "10.0.0.7:9060".parse().unwrap()),
            (10, "10.0.0.11:9060".parse().unwrap()),
            (98, "10.0.0.99:9060".parse().unwrap()),
        ]);
        (
            graph,
            scheduling,
            vec![edge(2, 1)],
            backends,
            deployment_policy(2),
        )
    }

    fn any_of_five_backend_compiler_fixture() -> (
        RuntimeFilterGraph,
        SchedulingPlan,
        Vec<FragmentEdge>,
        LiveBackendSnapshot,
        RuntimeFilterDeploymentPolicy,
    ) {
        let mut channel = channel_spec(5);
        channel.availability_coverage =
            Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]);
        channel.terminal_coverage =
            Coverage::AnyOf(vec![Coverage::Leaf(CoverageWitnessId::new(1))]);
        let mut graph = RuntimeFilterGraph::default();
        graph.insert_channel(channel).unwrap();
        graph.insert_binding(producer_binding(10, 5, 2)).unwrap();
        graph
            .insert_binding(consumer_binding(
                11,
                5,
                1,
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
            ))
            .unwrap();

        let producer_placements = (0..5)
            .map(|backend_idx| {
                placement(
                    2,
                    backend_idx,
                    backend_idx,
                    UniqueId::new(2, backend_idx as i64),
                )
            })
            .collect();
        let mut by_fragment = BTreeMap::new();
        by_fragment.insert(1u32, vec![placement(1, 0, 4, UniqueId::new(1, 1))]);
        by_fragment.insert(2u32, producer_placements);
        let scheduling = SchedulingPlan {
            root_fragment_id: 1,
            by_fragment,
            root_finst_id: UniqueId::new(1, 1),
            root_backend_idx: 4,
        };
        let backends = LiveBackendSnapshot::new(
            (0..5)
                .map(|backend_idx| {
                    (
                        backend_idx,
                        format!("10.0.0.{}:9060", backend_idx + 1).parse().unwrap(),
                    )
                })
                .collect(),
        );
        (
            graph,
            scheduling,
            vec![edge(2, 1)],
            backends,
            deployment_policy(1),
        )
    }

    #[test]
    fn compiler_atomically_returns_install_and_routing_views_with_same_epoch() {
        let (graph, scheduling, edges, backends, policy) = all_of_compiler_fixture();
        let plan = compile(
            &graph,
            &scheduling,
            &edges,
            &backends,
            &policy,
            DeploymentEpoch::new(9),
        )
        .unwrap();
        assert!(!plan.install_views.is_empty());
        assert!(!plan.routing_shards.is_empty());
        for shard in plan.routing_shards.values() {
            assert_eq!(shard.deployment_epoch(), plan.epoch);
        }
        for view in plan.install_views.values() {
            assert_eq!(view.epoch(), plan.epoch);
        }
    }

    #[test]
    fn compiler_all_of_aggregator_core_view_matches_routing_authority() {
        let (graph, scheduling, edges, backends, policy) = all_of_compiler_fixture();
        let plan = compile(
            &graph,
            &scheduling,
            &edges,
            &backends,
            &policy,
            DeploymentEpoch::new(9),
        )
        .unwrap();
        let channel_id = crate::runtime_filter::model::contract::ChannelId::new(5);
        let producer_binding = crate::runtime_filter::model::contract::BindingId::new(10);
        let aggregator = pid(2);
        let remote_producer = pid(7);
        let remote_consumer = pid(11);

        let aggregator_channel = &plan.install_views[&aggregator].channels()[&channel_id];
        let core_instances =
            aggregator_channel.producers()[&producer_binding].expected_fragment_instances();
        let routing_channel = plan.routing_shards[&aggregator]
            .channel(channel_id)
            .expect("aggregator routing channel");
        let routing_instances = routing_channel
            .producer_instances()
            .keys()
            .filter_map(|(binding, finst)| (*binding == producer_binding).then_some(*finst))
            .collect::<BTreeSet<_>>();

        assert_eq!(core_instances, &routing_instances);
        assert_eq!(
            core_instances,
            &BTreeSet::from([UniqueId::new(1, 3), UniqueId::new(1, 4)])
        );
        assert!(
            aggregator_channel
                .consumers()
                .contains_key(&crate::runtime_filter::model::contract::BindingId::new(11))
        );
        assert!(plan.install_views.contains_key(&remote_producer));
        assert!(plan.routing_shards.contains_key(&remote_producer));
        assert!(plan.install_views.contains_key(&remote_consumer));
        assert!(plan.routing_shards.contains_key(&remote_consumer));

        let installs = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .expect("every core view has a matching routing shard");
        assert_eq!(installs.len(), plan.install_views.len());
        assert_eq!(
            installs
                .iter()
                .map(|(participant, _)| *participant)
                .collect::<BTreeSet<_>>(),
            plan.install_views.keys().copied().collect()
        );
        assert!(installs.iter().all(|(participant, install)| {
            install.core_view() == &plan.install_views[participant]
                && install.routing_shard() == &plan.routing_shards[participant]
        }));
    }

    #[test]
    fn compiler_any_of_fanout_follows_replica_redundancy_without_hardcoded_three() {
        for redundancy in [1, 2, 4] {
            let (graph, scheduling, edges, backends, mut policy) =
                any_of_five_backend_compiler_fixture();
            policy.replica_redundancy = redundancy;
            let plan = compile(
                &graph,
                &scheduling,
                &edges,
                &backends,
                &policy,
                DeploymentEpoch::new(9),
            )
            .unwrap();
            let replica_routes = plan.role_graph.channels
                [&crate::runtime_filter::model::contract::ChannelId::new(5)]
                .routes
                .iter()
                .filter(|route| route.kind == RouteKind::ReplicaDirect)
                .count();
            let projected_outbound_routes = plan
                .routing_shards
                .values()
                .filter_map(|shard| {
                    shard.channel(crate::runtime_filter::model::contract::ChannelId::new(5))
                })
                .map(|channel| channel.outbound_edges().len())
                .sum::<usize>();
            assert_eq!(replica_routes, redundancy as usize);
            assert_eq!(projected_outbound_routes, replica_routes);
        }
    }

    #[test]
    fn compiler_does_not_emit_empty_routing_shard_for_roleless_backend() {
        let (graph, scheduling, edges, backends, policy) = all_of_compiler_fixture();
        let plan = compile(
            &graph,
            &scheduling,
            &edges,
            &backends,
            &policy,
            DeploymentEpoch::new(9),
        )
        .unwrap();
        assert!(!plan.routing_shards.contains_key(&pid(99)));
    }

    #[test]
    fn compiler_rejects_duplicate_live_backend_ids_before_placement_projection() {
        let (graph, scheduling, edges, _, policy) = all_of_compiler_fixture();
        let backends = LiveBackendSnapshot::new(vec![
            (7, "10.0.0.7:9060".parse().unwrap()),
            (7, "10.0.0.8:9060".parse().unwrap()),
        ]);
        assert!(matches!(
            compile(
                &graph,
                &scheduling,
                &edges,
                &backends,
                &policy,
                DeploymentEpoch::new(9),
            ),
            Err(DeploymentError::DuplicateBackend { backend_idx: 7 })
        ));
    }

    #[test]
    fn compiler_rejects_fragment_with_empty_placements() {
        let (graph, mut scheduling, edges, backends, policy) = all_of_compiler_fixture();
        scheduling.by_fragment.insert(2, Vec::new());

        let err = compile(
            &graph,
            &scheduling,
            &edges,
            &backends,
            &policy,
            DeploymentEpoch::new(9),
        )
        .unwrap_err();

        assert_eq!(
            err,
            DeploymentError::MissingPlacement {
                fragment: crate::runtime_filter::model::contract::PlanFragmentId::new(2)
            }
        );
    }

    #[test]
    fn compiler_rejects_backend_id_out_of_range_before_placement_projection() {
        let backend_idx = usize::try_from(u32::MAX).expect("64-bit backend identity");
        let (graph, scheduling, edges, _, policy) = all_of_compiler_fixture();
        let backends = LiveBackendSnapshot::new(vec![
            (0, "10.0.0.1:9060".parse().unwrap()),
            (backend_idx, "10.0.0.2:9060".parse().unwrap()),
        ]);
        assert!(matches!(
            compile(
                &graph,
                &scheduling,
                &edges,
                &backends,
                &policy,
                DeploymentEpoch::new(9),
            ),
            Err(DeploymentError::BackendIdOutOfRange {
                backend_idx: rejected
            }) if rejected == backend_idx
        ));
    }

    #[test]
    fn compile_colocated_join_yields_one_loopback_view() {
        let mut graph = RuntimeFilterGraph::default();
        graph.insert_channel(channel_spec(5)).unwrap();
        graph.insert_binding(producer_binding(10, 5, 2)).unwrap();
        graph
            .insert_binding(consumer_binding(
                11,
                5,
                1,
                ConsumerActivation::BlockingSnapshot,
            ))
            .unwrap();

        // Data flow build(frag 2) -> probe(frag 1): consumer(1) depends on
        // producer(2). No cycle.
        let edges = vec![edge(2, 1)];
        // Both fragments scheduled onto backend 0 -> co-located -> loopback.
        let mut by_fragment = BTreeMap::new();
        by_fragment.insert(1u32, vec![placement(1, 0, 0, UniqueId::new(1, 1))]);
        by_fragment.insert(2u32, vec![placement(2, 0, 0, UniqueId::new(1, 2))]);
        let scheduling = SchedulingPlan {
            root_fragment_id: 1,
            by_fragment,
            root_finst_id: UniqueId::new(1, 1),
            root_backend_idx: 0,
        };
        let backends = LiveBackendSnapshot::from_endpoints(vec![
            "127.0.0.1:9060".parse::<SocketAddr>().unwrap(),
        ]);
        let policy = RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(1024),
            replica_redundancy: 1,
            materialization: MaterializationPolicy::for_test(),
        };

        let plan = compile(
            &graph,
            &scheduling,
            &edges,
            &backends,
            &policy,
            DeploymentEpoch::new(7),
        )
        .unwrap();
        assert_eq!(plan.participants.len(), 1);
        assert_eq!(plan.install_views.len(), 1);
        assert_eq!(plan.epoch.get(), 7);
    }

    #[test]
    fn compile_preserves_top_k_summary_requirement_in_install_view() {
        let mut graph = RuntimeFilterGraph::default();
        graph.insert_channel(top_k_summary_channel_spec(5)).unwrap();
        graph
            .insert_binding(top_k_summary_producer_binding(10, 5, 2))
            .unwrap();
        graph
            .insert_binding(top_k_summary_consumer_binding(11, 5, 1))
            .unwrap();

        let mut by_fragment = BTreeMap::new();
        by_fragment.insert(1u32, vec![placement(1, 0, 0, UniqueId::new(1, 1))]);
        by_fragment.insert(2u32, vec![placement(2, 0, 0, UniqueId::new(1, 2))]);
        let scheduling = SchedulingPlan {
            root_fragment_id: 1,
            by_fragment,
            root_finst_id: UniqueId::new(1, 1),
            root_backend_idx: 0,
        };
        let backends = LiveBackendSnapshot::from_endpoints(vec![
            "127.0.0.1:9060".parse::<SocketAddr>().unwrap(),
        ]);
        let policy = RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(1024),
            replica_redundancy: 1,
            materialization: MaterializationPolicy::for_test(),
        };

        let plan = compile(
            &graph,
            &scheduling,
            &[edge(2, 1)],
            &backends,
            &policy,
            DeploymentEpoch::new(7),
        )
        .unwrap();
        let deployment = &plan.install_views[&RuntimeFilterParticipantId::new(1)].channels()
            [&crate::runtime_filter::model::contract::ChannelId::new(5)];
        assert_eq!(
            deployment.reduction_requirement(),
            crate::runtime_filter::model::contract::ReductionRequirement::MergeTopKSummary(
                crate::runtime_filter::model::contract::TopKSummaryRequirement::try_new(3).unwrap(),
            )
        );
    }

    #[test]
    fn compile_missing_live_activation_decision_rejects_blocking_feedback_cycle() {
        let mut graph = RuntimeFilterGraph::default();
        // A Membership channel (not OrderedBound) is used deliberately: an
        // OrderedBound/FinalDomainShard channel forbids `BlockingSnapshot`
        // consumers at `graph.validate()` time (`BlockingFeedbackConsumer`),
        // which would fire before `compile`'s wait-for check ever runs. Using
        // Membership + an execution-topology cycle isolates the wait-for path.
        graph.insert_channel(channel_spec(5)).unwrap();
        graph.insert_binding(producer_binding(10, 5, 1)).unwrap();
        graph
            .insert_binding(consumer_binding(
                11,
                5,
                2,
                ConsumerActivation::BlockingSnapshot,
            ))
            .unwrap();

        // scan(2) -> topn(1): the producer's own fragment (1) depends on the
        // consumer's fragment (2), but the consumer blocks waiting for the
        // producer's first snapshot -> execution feedback cycle.
        let edges = vec![edge(2, 1)];
        let mut by_fragment = BTreeMap::new();
        by_fragment.insert(1u32, vec![placement(1, 0, 0, UniqueId::new(1, 1))]);
        by_fragment.insert(2u32, vec![placement(2, 0, 0, UniqueId::new(1, 2))]);
        let scheduling = SchedulingPlan {
            root_fragment_id: 2,
            by_fragment,
            root_finst_id: UniqueId::new(1, 2),
            root_backend_idx: 0,
        };
        let backends = LiveBackendSnapshot::from_endpoints(vec![
            "127.0.0.1:9060".parse::<SocketAddr>().unwrap(),
        ]);
        let policy = RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(1024),
            replica_redundancy: 1,
            materialization: MaterializationPolicy::for_test(),
        };

        let err = compile(
            &graph,
            &scheduling,
            &edges,
            &backends,
            &policy,
            DeploymentEpoch::new(7),
        )
        .unwrap_err();
        assert!(matches!(err, DeploymentError::BlockingFeedbackCycle { .. }));
    }
}
