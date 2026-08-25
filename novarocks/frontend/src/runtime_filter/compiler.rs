//! Production entrypoint for Frontend runtime-filter deployment compilation.
//!
//! Core supplies only sealed schedule and planner facts.  This module projects
//! those facts into the participant-local native install DTOs; it never calls
//! a Core runtime-filter compiler or reconstructs a SQL graph.

use std::collections::{BTreeMap, BTreeSet};

use crate::query_execution::artifact::RuntimeFilterScheduledView;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, RuntimeFilterLifecycleView,
};
use crate::query_execution::{
    RuntimeFilterArtifactCapability, RuntimeFilterCompletionRequirement,
    RuntimeFilterConsumerActivation, RuntimeFilterContributionKind, RuntimeFilterCoverageFacts,
    RuntimeFilterDeploymentBindingRoleFacts, RuntimeFilterDeploymentFactsView,
    RuntimeFilterDeploymentLifecycleFacts, RuntimeFilterLateApplyGranularity,
};
use novarocks_proto::{common, filter, plan};
use novarocks_types::UniqueId;
use sha2::{Digest, Sha256};

use super::deployment::{RuntimeFilterWaitEdge, RuntimeFilterWaitGraph};
use super::install_encoder::{EncodedRuntimeFilterDeployment, encode_install_contributions};
use super::model::{
    FrontendRuntimeFilterDeployment, FrontendRuntimeFilterDeploymentPolicy,
    FrontendRuntimeFilterLifecycle, FrontendRuntimeFilterParticipant, RuntimeFilterChannelPolicy,
};
use super::semantic_encoder;

/// Owner-local query configuration captured before the schedule is sealed.
/// The lifecycle is still validated for RF-off queries so enabling channels
/// later cannot introduce an unchecked transport policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrontendRuntimeFilterDeploymentCompilerConfig {
    lifecycle: FrontendRuntimeFilterLifecycle,
    runtime_worker_count: usize,
}

impl FrontendRuntimeFilterDeploymentCompilerConfig {
    pub(crate) const fn new(
        lifecycle: FrontendRuntimeFilterLifecycle,
        runtime_worker_count: usize,
    ) -> Self {
        Self {
            lifecycle,
            runtime_worker_count,
        }
    }

    pub(crate) fn from_query_lifecycle(
        lifecycle: RuntimeFilterLifecycleView,
        runtime_worker_count: usize,
    ) -> Result<Self, DistributedQueryError> {
        let delivery_expire_ms =
            u64::try_from(lifecycle.delivery_expire().as_millis()).map_err(|_| {
                compilation_error("runtime filter delivery expiry exceeds wire milliseconds")
            })?;
        let query_expire_ms =
            u64::try_from(lifecycle.query_expire().as_millis()).map_err(|_| {
                compilation_error("runtime filter query expiry exceeds wire milliseconds")
            })?;
        FrontendRuntimeFilterLifecycle::for_query(delivery_expire_ms, query_expire_ms)
            .map(|lifecycle| Self::new(lifecycle, runtime_worker_count))
            .map_err(|error| compilation_error(error.to_string()))
    }
}

fn compilation_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// Compile a deployment from one sealed SQL/schedule/topology handoff.
///
/// An empty graph deliberately produces no contribution. A nonempty graph
/// produces one typed install for every frozen live backend, including an
/// explicitly empty service-only install when it has no RF-local role.
pub(crate) fn compile_scheduled_runtime_filter_deployment(
    view: RuntimeFilterScheduledView<'_>,
    config: FrontendRuntimeFilterDeploymentCompilerConfig,
) -> Result<EncodedRuntimeFilterDeployment, DistributedQueryError> {
    if view.clone().has_runtime_filter_channels() {
        return compile_nonempty_deployment(view, config);
    }

    let deployment = FrontendRuntimeFilterDeployment::new(
        view.clone().artifact_id(),
        view.clone().query_id_wire(),
        view.clone().deployment_epoch(),
        config.lifecycle,
        view.frozen_live_backend_ids(),
        std::iter::empty(),
        &RuntimeFilterWaitGraph::default(),
    )
    .map_err(|error| compilation_error(error.to_string()))?;
    debug_assert!(deployment.is_empty());
    encode_install_contributions(&deployment)
}

#[derive(Clone)]
struct ChannelSpec {
    channel_id: u32,
    logical_domain: filter::RuntimeFilterLogicalDomain,
    lifecycle: i32,
    availability_coverage: filter::RuntimeFilterCoverage,
    terminal_coverage: filter::RuntimeFilterCoverage,
    reduction: plan::RuntimeFilterReductionContract,
    contribution_kinds: Vec<i32>,
    required_capabilities: BTreeSet<Capability>,
    policy: filter::RuntimeFilterPolicyRequirement,
}

#[derive(Clone)]
struct BindingSpec {
    binding_id: u32,
    channel_id: u32,
    fragment_id: u32,
    node_id: i32,
    witness: Option<u32>,
    role: BindingRole,
}

#[derive(Clone, Copy)]
struct FragmentEdgeSpec {
    source_fragment_id: u32,
    target_fragment_id: u32,
    target_exchange_node_id: i32,
}

#[derive(Clone)]
struct JoinProgressProof {
    channel_id: u32,
    producer_binding_id: u32,
    producer_fragment_id: u32,
    join_node_id: i32,
    build_frontier: Vec<(u32, i32)>,
    non_build_inputs: Vec<(u32, i32)>,
}

#[derive(Clone)]
enum BindingRole {
    Producer {
        completion: i32,
    },
    Consumer {
        capabilities: BTreeSet<Capability>,
        activation: plan::RuntimeFilterConsumerActivation,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Capability {
    Membership,
    OrderedRange,
    EmptyDomain,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RouteRole {
    Producer(u32),
    Aggregator,
    Consumer(u32),
}

#[derive(Clone, Copy)]
enum RouteKind {
    Direct,
    ToAggregator,
    FromAggregator,
}

#[derive(Clone, Copy)]
struct Route {
    edge_id: u32,
    kind: RouteKind,
    from_participant: u32,
    from_binding: u32,
    to_participant: u32,
    to_binding: u32,
}

#[derive(Default)]
struct RoleChannel {
    producers: BTreeMap<u32, BTreeSet<u32>>,
    consumers: BTreeMap<u32, BTreeSet<u32>>,
    aggregator: Option<u32>,
    routes: Vec<Route>,
}

type Instances = BTreeMap<(u32, u32, u32), BTreeSet<UniqueId>>;

/// Frontend-private, immutable deployment input copied from the sealed Core
/// view before placement, routing and liveness compilation begins.
struct DeploymentInput {
    topology: BTreeMap<usize, String>,
    fragment_edges: Vec<FragmentEdgeSpec>,
    join_progress: BTreeMap<(u32, u32, u32), JoinProgressProof>,
    channels: BTreeMap<u32, ChannelSpec>,
    policies: Vec<RuntimeFilterChannelPolicy>,
    bindings: BTreeMap<u32, BindingSpec>,
    instances: Instances,
}

impl DeploymentInput {
    fn from_sealed(view: RuntimeFilterScheduledView<'_>) -> Result<Self, DistributedQueryError> {
        let topology = sealed_topology(view.clone())?;
        let facts = view.deployment_facts();
        let fragment_edges = facts
            .fragment_edges()
            .map(|edge| FragmentEdgeSpec {
                source_fragment_id: edge.source_fragment_id(),
                target_fragment_id: edge.target_fragment_id(),
                target_exchange_node_id: edge.target_exchange_node_id(),
            })
            .collect();
        let join_progress = facts
            .join_progress()
            .filter_map(|progress| match progress {
                crate::query_execution::RuntimeFilterJoinProgressFacts::Proven {
                    channel_id,
                    producer_binding_id,
                    producer_fragment_id,
                    join_node_id,
                    build_frontier,
                    non_build_inputs,
                } => Some(JoinProgressProof {
                    channel_id,
                    producer_binding_id,
                    producer_fragment_id,
                    join_node_id,
                    build_frontier: build_frontier
                        .into_iter()
                        .map(|edge| (edge.source_fragment_id, edge.target_exchange_node_id))
                        .collect(),
                    non_build_inputs: non_build_inputs
                        .into_iter()
                        .map(|edge| (edge.source_fragment_id, edge.target_exchange_node_id))
                        .collect(),
                }),
                crate::query_execution::RuntimeFilterJoinProgressFacts::Skipped { .. } => None,
            })
            .map(|proof| {
                (
                    (
                        proof.channel_id,
                        proof.producer_binding_id,
                        proof.producer_fragment_id,
                    ),
                    proof,
                )
            })
            .collect();
        let (channels, policies) = materialize_channels(facts)?;
        let (bindings, instances) = materialize_bindings(facts, &channels, &topology)?;
        Ok(Self {
            topology,
            fragment_edges,
            join_progress,
            channels,
            policies,
            bindings,
            instances,
        })
    }
}

fn compile_nonempty_deployment(
    view: RuntimeFilterScheduledView<'_>,
    config: FrontendRuntimeFilterDeploymentCompilerConfig,
) -> Result<EncodedRuntimeFilterDeployment, DistributedQueryError> {
    let input = DeploymentInput::from_sealed(view.clone())?;
    if input.channels.is_empty() {
        return Err(compilation_error(
            "runtime filter nonempty handoff has no channel facts",
        ));
    }
    let roles = build_roles(
        &input.channels,
        &input.bindings,
        &input.instances,
        input.topology.len(),
    )?;
    let wait_graph =
        build_wait_graph(&input.bindings, &input.fragment_edges, &input.join_progress)?;

    let lifecycle_input = config.lifecycle.to_wire();
    let deployment_policy = FrontendRuntimeFilterDeploymentPolicy::derive(
        input.policies,
        input.topology.len(),
        config.runtime_worker_count,
        lifecycle_input.delivery_expire_ms,
        lifecycle_input.query_expire_ms,
    )
    .map_err(|error| compilation_error(error.to_string()))?;

    let mut participants = Vec::with_capacity(input.topology.len());
    for &backend_idx in input.topology.keys() {
        let participant_id = participant_id(backend_idx)?;
        let install = participant_install(
            participant_id,
            &input.channels,
            &input.bindings,
            &input.instances,
            &roles,
            &input.topology,
            deployment_policy,
        )?;
        let participant =
            if install.core_channels.is_empty() && install.routing_channels.is_empty() {
                FrontendRuntimeFilterParticipant::service_only(backend_idx)
            } else {
                FrontendRuntimeFilterParticipant::active(backend_idx, install)
            }
            .map_err(|error| compilation_error(error.to_string()))?;
        participants.push(participant);
    }
    let deployment = FrontendRuntimeFilterDeployment::new(
        view.clone().artifact_id(),
        view.clone().query_id_wire(),
        view.clone().deployment_epoch(),
        deployment_policy.lifecycle,
        input.topology.keys().copied(),
        participants,
        &wait_graph,
    )
    .map_err(|error| compilation_error(error.to_string()))?;
    encode_install_contributions(&deployment)
}

fn sealed_topology(
    view: RuntimeFilterScheduledView<'_>,
) -> Result<BTreeMap<usize, String>, DistributedQueryError> {
    let mut topology = BTreeMap::new();
    for entry in view.clone().frozen_live_backends() {
        if entry.start_epoch() == 0 {
            return Err(compilation_error(format!(
                "runtime filter backend {} has zero start epoch",
                entry.backend_idx()
            )));
        }
        if topology
            .insert(entry.backend_idx(), entry.endpoint().to_string())
            .is_some()
        {
            return Err(compilation_error(format!(
                "runtime filter frozen topology repeats backend {}",
                entry.backend_idx()
            )));
        }
    }
    if topology.is_empty() {
        return Err(compilation_error(
            "runtime filter deployment requires a nonempty frozen live-backend snapshot",
        ));
    }
    let ids = view.frozen_live_backend_ids().collect::<BTreeSet<_>>();
    if ids != topology.keys().copied().collect() {
        return Err(compilation_error(
            "runtime filter frozen topology id set does not match topology entries",
        ));
    }
    Ok(topology)
}

fn materialize_channels(
    facts: RuntimeFilterDeploymentFactsView<'_>,
) -> Result<(BTreeMap<u32, ChannelSpec>, Vec<RuntimeFilterChannelPolicy>), DistributedQueryError> {
    let mut channels = BTreeMap::new();
    let mut policies = Vec::new();
    for fact in facts.channels() {
        let channel_id = require_nonzero(fact.channel_id(), "channel id")?;
        let encoded_domain = semantic_encoder::encode_logical_domain(fact.logical_domain())?;
        let logical_domain = filter::RuntimeFilterLogicalDomain {
            value_type: Some(encoded_domain.value_type()),
            contract: Some(encoded_domain.contract()),
        };
        let lifecycle = match fact.lifecycle() {
            RuntimeFilterDeploymentLifecycleFacts::CompleteOnce => {
                filter::RuntimeFilterLifecycle::CompleteOnce as i32
            }
            RuntimeFilterDeploymentLifecycleFacts::MonotonicUpdates => {
                filter::RuntimeFilterLifecycle::MonotonicUpdates as i32
            }
        };
        let availability_coverage = coverage(fact.availability_coverage())?;
        let terminal_coverage = coverage(fact.terminal_coverage())?;
        let reduction = encoded_domain.encode_reduction(fact.reduction())?;
        let contribution_kinds = unique_contribution_kinds(fact.allowed_contribution_kinds())?;
        let required_capabilities = unique_capabilities(fact.required_consumer_capabilities())?;
        let policy = fact.policy();
        if policy.max_contribution_bytes == 0
            || policy.max_artifact_bytes == 0
            || policy.deadline_ms == 0
        {
            return Err(compilation_error(format!(
                "runtime filter channel {channel_id} has a zero resource or deadline limit"
            )));
        }
        let spec = ChannelSpec {
            channel_id,
            logical_domain,
            lifecycle,
            availability_coverage,
            terminal_coverage,
            reduction,
            contribution_kinds,
            required_capabilities,
            policy: filter::RuntimeFilterPolicyRequirement {
                max_contribution_bytes: policy.max_contribution_bytes,
                max_artifact_bytes: policy.max_artifact_bytes,
                deadline_ms: policy.deadline_ms,
                max_retries: policy.max_retries,
            },
        };
        if channels.insert(channel_id, spec).is_some() {
            return Err(compilation_error(format!(
                "runtime filter repeats channel {channel_id}"
            )));
        }
        policies.push(RuntimeFilterChannelPolicy {
            max_contribution_bytes: policy.max_contribution_bytes,
            max_artifact_bytes: policy.max_artifact_bytes,
            deadline_ms: policy.deadline_ms,
            max_retries: policy.max_retries,
        });
    }
    Ok((channels, policies))
}

fn materialize_bindings(
    facts: RuntimeFilterDeploymentFactsView<'_>,
    channels: &BTreeMap<u32, ChannelSpec>,
    topology: &BTreeMap<usize, String>,
) -> Result<(BTreeMap<u32, BindingSpec>, Instances), DistributedQueryError> {
    let mut placements: BTreeMap<u32, Vec<(usize, UniqueId)>> = BTreeMap::new();
    for placement in facts.placements() {
        if !topology.contains_key(&placement.backend_idx()) {
            return Err(compilation_error(format!(
                "scheduled backend {} is absent from frozen topology",
                placement.backend_idx()
            )));
        }
        let instance = placement.fragment_instance_id();
        if instance.high() == 0 && instance.low() == 0 {
            return Err(compilation_error(
                "runtime filter placement has a zero fragment instance id",
            ));
        }
        placements
            .entry(placement.fragment_id())
            .or_default()
            .push((placement.backend_idx(), instance));
    }
    let mut bindings = BTreeMap::new();
    let mut instances = Instances::new();
    for fact in facts.bindings() {
        let binding_id = require_nonzero(fact.binding_id(), "binding id")?;
        let channel_id = require_nonzero(fact.channel_id(), "binding channel id")?;
        if !channels.contains_key(&channel_id) {
            return Err(compilation_error(format!(
                "runtime filter binding {binding_id} references unknown channel {channel_id}"
            )));
        }
        let placements_for_fragment = placements.get(&fact.fragment_id()).ok_or_else(|| {
            compilation_error(format!(
                "runtime filter binding {binding_id} fragment {} has no validated placement",
                fact.fragment_id()
            ))
        })?;
        if placements_for_fragment.is_empty() {
            return Err(compilation_error(format!(
                "runtime filter binding {binding_id} fragment {} has an empty placement",
                fact.fragment_id()
            )));
        }
        let role = match fact.role() {
            RuntimeFilterDeploymentBindingRoleFacts::Producer {
                completion_requirement,
                ..
            } => {
                require_nonzero(
                    fact.coverage_witness_id().ok_or_else(|| {
                        compilation_error(format!(
                            "runtime filter producer binding {binding_id} has no coverage witness"
                        ))
                    })?,
                    "coverage witness id",
                )?;
                let completion = completion(completion_requirement);
                BindingRole::Producer { completion }
            }
            RuntimeFilterDeploymentBindingRoleFacts::Consumer {
                capabilities,
                activation,
                ..
            } => {
                if fact.coverage_witness_id().is_some() {
                    return Err(compilation_error(format!(
                        "runtime filter consumer binding {binding_id} unexpectedly carries a coverage witness"
                    )));
                }
                BindingRole::Consumer {
                    capabilities: unique_capabilities(capabilities)?,
                    activation: activation_wire(activation),
                }
            }
        };
        let spec = BindingSpec {
            binding_id,
            channel_id,
            fragment_id: fact.fragment_id(),
            node_id: fact.node_id(),
            witness: fact.coverage_witness_id(),
            role,
        };
        if bindings.insert(binding_id, spec).is_some() {
            return Err(compilation_error(format!(
                "runtime filter repeats binding {binding_id}"
            )));
        }
        for (backend_idx, instance) in placements_for_fragment {
            instances
                .entry((channel_id, binding_id, participant_id(*backend_idx)?))
                .or_default()
                .insert(*instance);
        }
    }
    validate_channel_bindings(channels, &bindings)?;
    Ok((bindings, instances))
}

fn validate_channel_bindings(
    channels: &BTreeMap<u32, ChannelSpec>,
    bindings: &BTreeMap<u32, BindingSpec>,
) -> Result<(), DistributedQueryError> {
    for (&channel_id, channel) in channels {
        let mut producers = BTreeSet::new();
        let mut consumers = BTreeSet::new();
        for binding in bindings
            .values()
            .filter(|binding| binding.channel_id == channel_id)
        {
            match &binding.role {
                BindingRole::Producer { .. } => {
                    producers.insert(binding.witness.expect("producer witness validated"));
                }
                BindingRole::Consumer { capabilities, .. } => {
                    if !channel.required_capabilities.is_subset(capabilities) {
                        return Err(compilation_error(format!(
                            "runtime filter consumer binding {} lacks a required channel capability",
                            binding.binding_id
                        )));
                    }
                    consumers.insert(binding.binding_id);
                }
            }
        }
        if producers.is_empty() || consumers.is_empty() {
            return Err(compilation_error(format!(
                "runtime filter channel {channel_id} lacks a producer or consumer binding"
            )));
        }
        let mut leaves = BTreeSet::new();
        coverage_leaves(&channel.availability_coverage, &mut leaves)?;
        if leaves != producers {
            return Err(compilation_error(format!(
                "runtime filter channel {channel_id} availability coverage does not match producer witnesses"
            )));
        }
        let mut terminal = BTreeSet::new();
        coverage_leaves(&channel.terminal_coverage, &mut terminal)?;
        if !terminal.is_subset(&producers) {
            return Err(compilation_error(format!(
                "runtime filter channel {channel_id} terminal coverage references an unknown witness"
            )));
        }
    }
    Ok(())
}

fn build_roles(
    channels: &BTreeMap<u32, ChannelSpec>,
    bindings: &BTreeMap<u32, BindingSpec>,
    instances: &Instances,
    replica_redundancy: usize,
) -> Result<BTreeMap<u32, RoleChannel>, DistributedQueryError> {
    let mut all = BTreeMap::new();
    let mut next_edge = 1u32;
    for (&channel_id, channel) in channels {
        let mut role = RoleChannel::default();
        for binding in bindings
            .values()
            .filter(|binding| binding.channel_id == channel_id)
        {
            let participants = instances
                .keys()
                .filter(|(c, b, _)| *c == channel_id && *b == binding.binding_id)
                .map(|(_, _, participant)| *participant)
                .collect::<BTreeSet<_>>();
            match binding.role {
                BindingRole::Producer { .. } => {
                    for participant in participants {
                        role.producers
                            .entry(participant)
                            .or_default()
                            .insert(binding.binding_id);
                    }
                }
                BindingRole::Consumer { .. } => {
                    for participant in participants {
                        role.consumers
                            .entry(participant)
                            .or_default()
                            .insert(binding.binding_id);
                    }
                }
            }
        }
        let producer_participants = role.producers.keys().copied().collect::<BTreeSet<_>>();
        let consumer_participants = role.consumers.keys().copied().collect::<BTreeSet<_>>();
        if producer_participants == consumer_participants && producer_participants.len() == 1 {
            let participant = *producer_participants
                .iter()
                .next()
                .expect("one participant");
            for producer in bindings.values().filter(|binding| {
                binding.channel_id == channel_id
                    && matches!(binding.role, BindingRole::Producer { .. })
            }) {
                for consumer in bindings.values().filter(|binding| {
                    binding.channel_id == channel_id
                        && matches!(binding.role, BindingRole::Consumer { .. })
                }) {
                    role.routes.push(Route {
                        edge_id: allocate_edge(&mut next_edge)?,
                        kind: RouteKind::Direct,
                        from_participant: participant,
                        from_binding: producer.binding_id,
                        to_participant: participant,
                        to_binding: consumer.binding_id,
                    });
                }
            }
        } else if coverage_is_any_of(&channel.availability_coverage) {
            let senders = producer_participants
                .iter()
                .copied()
                .take(replica_redundancy.max(1))
                .collect::<BTreeSet<_>>();
            for producer in bindings.values().filter(|binding| {
                binding.channel_id == channel_id
                    && matches!(binding.role, BindingRole::Producer { .. })
            }) {
                for sender in &senders {
                    if !role
                        .producers
                        .get(sender)
                        .is_some_and(|bindings| bindings.contains(&producer.binding_id))
                    {
                        continue;
                    }
                    for consumer in bindings.values().filter(|binding| {
                        binding.channel_id == channel_id
                            && matches!(binding.role, BindingRole::Consumer { .. })
                    }) {
                        for participant in
                            role.consumers.iter().filter_map(|(participant, hosted)| {
                                hosted
                                    .contains(&consumer.binding_id)
                                    .then_some(*participant)
                            })
                        {
                            role.routes.push(Route {
                                edge_id: allocate_edge(&mut next_edge)?,
                                kind: RouteKind::Direct,
                                from_participant: *sender,
                                from_binding: producer.binding_id,
                                to_participant: participant,
                                to_binding: consumer.binding_id,
                            });
                        }
                    }
                }
            }
        } else {
            let aggregator = *producer_participants.iter().next().ok_or_else(|| {
                compilation_error(format!(
                    "runtime filter channel {channel_id} has no producer participant"
                ))
            })?;
            role.aggregator = Some(aggregator);
            for producer in bindings.values().filter(|binding| {
                binding.channel_id == channel_id
                    && matches!(binding.role, BindingRole::Producer { .. })
            }) {
                for participant in role.producers.iter().filter_map(|(participant, hosted)| {
                    hosted
                        .contains(&producer.binding_id)
                        .then_some(*participant)
                }) {
                    role.routes.push(Route {
                        edge_id: allocate_edge(&mut next_edge)?,
                        kind: RouteKind::ToAggregator,
                        from_participant: participant,
                        from_binding: producer.binding_id,
                        to_participant: aggregator,
                        to_binding: producer.binding_id,
                    });
                }
            }
            for consumer in bindings.values().filter(|binding| {
                binding.channel_id == channel_id
                    && matches!(binding.role, BindingRole::Consumer { .. })
            }) {
                for participant in role.consumers.iter().filter_map(|(participant, hosted)| {
                    hosted
                        .contains(&consumer.binding_id)
                        .then_some(*participant)
                }) {
                    role.routes.push(Route {
                        edge_id: allocate_edge(&mut next_edge)?,
                        kind: RouteKind::FromAggregator,
                        from_participant: aggregator,
                        from_binding: consumer.binding_id,
                        to_participant: participant,
                        to_binding: consumer.binding_id,
                    });
                }
            }
        }
        all.insert(channel_id, role);
    }
    Ok(all)
}

fn participant_install(
    participant: u32,
    channels: &BTreeMap<u32, ChannelSpec>,
    bindings: &BTreeMap<u32, BindingSpec>,
    instances: &Instances,
    roles: &BTreeMap<u32, RoleChannel>,
    topology: &BTreeMap<usize, String>,
    deployment_policy: FrontendRuntimeFilterDeploymentPolicy,
) -> Result<filter::RuntimeFilterParticipantInstall, DistributedQueryError> {
    let mut core_channels = Vec::new();
    let mut routing_channels = Vec::new();
    for (&channel_id, channel) in channels {
        let role = roles
            .get(&channel_id)
            .expect("every channel has a role graph");
        let local_producers = role
            .producers
            .get(&participant)
            .cloned()
            .unwrap_or_default();
        let local_consumers = role
            .consumers
            .get(&participant)
            .cloned()
            .unwrap_or_default();
        let has_aggregator = role.aggregator == Some(participant);
        if !local_producers.is_empty() || !local_consumers.is_empty() || has_aggregator {
            core_channels.push(core_channel(
                participant,
                channel,
                bindings,
                instances,
                role,
                deployment_policy,
            )?);
        }
        if !local_producers.is_empty() || !local_consumers.is_empty() || has_aggregator {
            routing_channels.push(routing_channel(
                participant,
                channel_id,
                instances,
                role,
                topology,
            )?);
        }
    }
    Ok(filter::RuntimeFilterParticipantInstall {
        core_channels,
        routing_channels,
    })
}

fn core_channel(
    participant: u32,
    channel: &ChannelSpec,
    bindings: &BTreeMap<u32, BindingSpec>,
    instances: &Instances,
    role: &RoleChannel,
    policy: FrontendRuntimeFilterDeploymentPolicy,
) -> Result<filter::RuntimeFilterChannelDeployment, DistributedQueryError> {
    let mut producer_ids = role
        .producers
        .get(&participant)
        .cloned()
        .unwrap_or_default();
    if role.aggregator == Some(participant) {
        producer_ids.extend(role.producers.values().flatten().copied());
    }
    let mut producers = Vec::new();
    for binding_id in producer_ids {
        let binding = bindings
            .get(&binding_id)
            .expect("role bindings come from sealed bindings");
        let witness = binding.witness.ok_or_else(|| {
            compilation_error(format!(
                "runtime filter producer binding {binding_id} lacks a witness"
            ))
        })?;
        let mut expected = BTreeSet::new();
        if role.aggregator == Some(participant) {
            for source in role.producers.keys() {
                expected.extend(
                    instances
                        .get(&(channel.channel_id, binding_id, *source))
                        .cloned()
                        .unwrap_or_default(),
                );
            }
        } else {
            expected.extend(
                instances
                    .get(&(channel.channel_id, binding_id, participant))
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        if expected.is_empty() {
            return Err(compilation_error(format!(
                "runtime filter producer binding {binding_id} has no expected fragment instance"
            )));
        }
        producers.push(filter::RuntimeFilterProducerDeployment {
            binding_id,
            coverage_witness_id: witness,
            expected_fragment_instances: expected.into_iter().map(unique_id_wire).collect(),
        });
    }
    let mut consumers = Vec::new();
    let mut groups: BTreeMap<
        Vec<u8>,
        (
            i32,
            filter::RuntimeFilterConsumerArtifactProfile,
            BTreeSet<u32>,
        ),
    > = BTreeMap::new();
    for binding_id in role.consumers.get(&participant).into_iter().flatten() {
        let binding = bindings
            .get(binding_id)
            .expect("role bindings come from sealed bindings");
        let BindingRole::Consumer {
            capabilities,
            activation,
        } = &binding.role
        else {
            return Err(compilation_error(
                "runtime filter role graph consumer is not a consumer binding",
            ));
        };
        let expected = instances
            .get(&(channel.channel_id, *binding_id, participant))
            .cloned()
            .unwrap_or_default();
        if expected.is_empty() {
            return Err(compilation_error(format!(
                "runtime filter consumer binding {binding_id} has no expected fragment instance"
            )));
        }
        let route_ids = role
            .routes
            .iter()
            .filter(|route| {
                route.to_participant == participant
                    && route.to_binding == *binding_id
                    && matches!(route.kind, RouteKind::Direct | RouteKind::FromAggregator)
            })
            .map(|route| route.edge_id)
            .collect::<BTreeSet<_>>();
        if route_ids.is_empty() {
            return Err(compilation_error(format!(
                "runtime filter consumer binding {binding_id} has no inbound delivery route"
            )));
        }
        let profile = artifact_profile(channel, capabilities)?;
        consumers.push(filter::RuntimeFilterConsumerDeployment {
            binding_id: *binding_id,
            activation: Some(*activation),
            capabilities: capabilities.iter().copied().map(capability_wire).collect(),
            artifact_profile: Some(profile.clone()),
            route_edge_ids: route_ids.iter().copied().collect(),
            expected_fragment_instances: expected.into_iter().map(unique_id_wire).collect(),
        });
        for route in role.routes.iter().filter(|route| {
            route.to_binding == *binding_id
                && route.from_participant == participant
                && matches!(route.kind, RouteKind::Direct | RouteKind::FromAggregator)
        }) {
            let owner = if matches!(route.kind, RouteKind::Direct) {
                filter::RuntimeFilterOutboundMaterializationOwner::DirectSource as i32
            } else {
                filter::RuntimeFilterOutboundMaterializationOwner::Aggregator as i32
            };
            let entry = groups
                .entry(profile.profile_id.clone())
                .or_insert_with(|| (owner, profile.clone(), BTreeSet::new()));
            if entry.0 != owner {
                return Err(compilation_error(
                    "runtime filter materialization profile has conflicting owners",
                ));
            }
            entry.2.insert(route.edge_id);
        }
    }
    // A materialization group belongs to the participant that sends an artifact,
    // not necessarily to one that also hosts the consuming fragment.  In a
    // distributed direct route those roles can be disjoint, so derive the final
    // coverage from the sealed outbound routing facts rather than the local
    // consumer projection above.
    for route in role.routes.iter().filter(|route| {
        route.from_participant == participant
            && matches!(route.kind, RouteKind::Direct | RouteKind::FromAggregator)
    }) {
        let binding = bindings
            .get(&route.to_binding)
            .expect("role bindings come from sealed bindings");
        let BindingRole::Consumer { capabilities, .. } = &binding.role else {
            return Err(compilation_error(
                "runtime filter outbound artifact route does not target a consumer binding",
            ));
        };
        let owner = if matches!(route.kind, RouteKind::Direct) {
            filter::RuntimeFilterOutboundMaterializationOwner::DirectSource as i32
        } else {
            filter::RuntimeFilterOutboundMaterializationOwner::Aggregator as i32
        };
        let profile = artifact_profile(channel, capabilities)?;
        let entry = groups
            .entry(profile.profile_id.clone())
            .or_insert_with(|| (owner, profile, BTreeSet::new()));
        if entry.0 != owner {
            return Err(compilation_error(
                "runtime filter materialization profile has conflicting owners",
            ));
        }
        entry.2.insert(route.edge_id);
    }
    let max_concurrent_jobs = if groups.is_empty() {
        policy.max_concurrent_jobs
    } else {
        policy
            .max_concurrent_jobs
            .min(u64::try_from(groups.len()).map_err(|_| {
                compilation_error("runtime filter materialization group count exceeds wire width")
            })?)
    };

    Ok(filter::RuntimeFilterChannelDeployment {
        channel_id: channel.channel_id,
        logical_domain: Some(channel.logical_domain.clone()),
        lifecycle: channel.lifecycle,
        availability_coverage: Some(channel.availability_coverage.clone()),
        terminal_coverage: Some(channel.terminal_coverage.clone()),
        reduction: Some(channel.reduction.clone()),
        allowed_contribution_kinds: channel.contribution_kinds.clone(),
        completion_requirement: channel_completion(bindings, channel.channel_id)?,
        policy: Some(channel.policy),
        core_budget: Some(filter::RuntimeFilterCoreBudget {
            max_reducer_bytes: policy.core_budget_bytes,
        }),
        materialization_policy: Some(filter::RuntimeFilterMaterializationPolicy {
            bloom_bits_per_key: policy.bloom_bits_per_key,
            bloom_hash_count: policy.bloom_hash_count,
            bloom_seed: policy.bloom_seed,
            bloom_algorithm_version: policy.bloom_algorithm_version,
            max_total_retained_bytes: policy.max_total_retained_bytes,
            max_scratch_bytes_per_job: policy.max_scratch_bytes_per_job,
            max_concurrent_jobs,
        }),
        producers,
        consumers,
        outbound_materialization_groups: groups
            .into_values()
            .map(|(owner, artifact_profile, routes)| {
                filter::RuntimeFilterOutboundMaterializationGroup {
                    owner,
                    artifact_profile: Some(artifact_profile),
                    route_edge_ids: routes.into_iter().collect(),
                }
            })
            .collect(),
    })
}

fn routing_channel(
    participant: u32,
    channel_id: u32,
    instances: &Instances,
    role: &RoleChannel,
    topology: &BTreeMap<usize, String>,
) -> Result<filter::RuntimeFilterChannelRoutingView, DistributedQueryError> {
    let mut local_roles = BTreeSet::new();
    local_roles.extend(
        role.producers
            .get(&participant)
            .into_iter()
            .flatten()
            .copied()
            .map(RouteRole::Producer),
    );
    local_roles.extend(
        role.consumers
            .get(&participant)
            .into_iter()
            .flatten()
            .copied()
            .map(RouteRole::Consumer),
    );
    if role.aggregator == Some(participant) {
        local_roles.insert(RouteRole::Aggregator);
    }
    if local_roles.is_empty() {
        return Err(compilation_error(format!(
            "runtime filter participant {participant} has no local role for routing channel {channel_id}"
        )));
    }
    let mut producer_instances = BTreeMap::new();
    for ((channel, binding, producer), expected) in instances {
        if *channel != channel_id {
            continue;
        }
        if role
            .producers
            .get(producer)
            .is_some_and(|bindings| bindings.contains(binding))
        {
            for instance in expected {
                producer_instances.insert((*binding, *instance), *producer);
            }
        }
    }
    let inbound_edges = role
        .routes
        .iter()
        .filter(|route| route.to_participant == participant)
        .map(|route| routing_edge(route, false, topology))
        .collect::<Result<Vec<_>, _>>()?;
    let outbound_edges = role
        .routes
        .iter()
        .filter(|route| route.from_participant == participant)
        .map(|route| routing_edge(route, true, topology))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(filter::RuntimeFilterChannelRoutingView {
        channel_id,
        local_roles: local_roles.into_iter().map(route_role_wire).collect(),
        producer_instances: producer_instances
            .into_iter()
            .map(|((binding_id, fragment_instance_id), participant_id)| {
                filter::RuntimeFilterProducerInstanceRoute {
                    binding_id,
                    fragment_instance_id: Some(unique_id_wire(fragment_instance_id)),
                    participant_id,
                }
            })
            .collect(),
        inbound_edges,
        outbound_edges,
    })
}

fn routing_edge(
    route: &Route,
    outbound: bool,
    topology: &BTreeMap<usize, String>,
) -> Result<filter::RuntimeFilterRoutingEdgeView, DistributedQueryError> {
    let (source_role, target_role, allowed_kinds) = match route.kind {
        RouteKind::Direct => (
            RouteRole::Producer(route.from_binding),
            RouteRole::Consumer(route.to_binding),
            delivery_kinds(),
        ),
        RouteKind::ToAggregator => (
            RouteRole::Producer(route.from_binding),
            RouteRole::Aggregator,
            contribution_kinds(),
        ),
        RouteKind::FromAggregator => (
            RouteRole::Aggregator,
            RouteRole::Consumer(route.to_binding),
            delivery_kinds(),
        ),
    };
    let local = if outbound {
        route.from_participant
    } else {
        route.to_participant
    };
    let remote = if outbound {
        route.to_participant
    } else {
        route.from_participant
    };
    let peer = if local == remote {
        filter::RuntimeFilterRoutePeer {
            peer: Some(filter::runtime_filter_route_peer::Peer::Loopback(true)),
        }
    } else {
        let backend = usize::try_from(remote - 1).map_err(|_| {
            compilation_error("runtime filter participant id cannot map to a backend")
        })?;
        let endpoint = topology.get(&backend).ok_or_else(|| {
            compilation_error(format!(
                "runtime filter route references non-live participant {remote}"
            ))
        })?;
        filter::RuntimeFilterRoutePeer {
            peer: Some(filter::runtime_filter_route_peer::Peer::Remote(
                filter::RuntimeFilterRemotePeer {
                    participant_id: remote,
                    endpoint: endpoint.clone(),
                },
            )),
        }
    };
    Ok(filter::RuntimeFilterRoutingEdgeView {
        route_edge_id: route.edge_id,
        source: Some(route_endpoint(route.from_participant, source_role)),
        target: Some(route_endpoint(route.to_participant, target_role)),
        peer: Some(peer),
        allowed_kinds,
    })
}

fn build_wait_graph(
    bindings: &BTreeMap<u32, BindingSpec>,
    fragment_edges: &[FragmentEdgeSpec],
    join_progress: &BTreeMap<(u32, u32, u32), JoinProgressProof>,
) -> Result<RuntimeFilterWaitGraph, DistributedQueryError> {
    let dependencies = fragment_dependencies(fragment_edges)?;
    let mut edges = fragment_edges
        .iter()
        .map(|edge| {
            RuntimeFilterWaitEdge::new(edge.source_fragment_id, edge.target_fragment_id, true)
        })
        .collect::<Vec<_>>();
    let mut next_virtual_node = bindings
        .values()
        .map(|binding| binding.fragment_id)
        .chain(
            fragment_edges
                .iter()
                .flat_map(|edge| [edge.source_fragment_id, edge.target_fragment_id]),
        )
        .max()
        .unwrap_or_default()
        .checked_add(1)
        .ok_or_else(|| compilation_error("runtime filter wait graph node id overflow"))?;
    let mut build_ready_nodes = BTreeMap::new();
    let mut consumer_sources = Vec::new();

    for consumer in bindings
        .values()
        .filter(|binding| matches!(binding.role, BindingRole::Consumer { .. }))
    {
        let BindingRole::Consumer { activation, .. } = &consumer.role else {
            unreachable!()
        };
        let blocking = activation.kind.as_ref().is_some_and(|kind| {
            matches!(
                kind,
                plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true)
            )
        });
        if !blocking {
            continue;
        }

        let mut sources = Vec::new();
        for producer in bindings.values().filter(|binding| {
            binding.channel_id == consumer.channel_id
                && matches!(binding.role, BindingRole::Producer { .. })
        }) {
            let proof = join_progress
                .get(&(
                    producer.channel_id,
                    producer.binding_id,
                    producer.fragment_id,
                ))
                .filter(|proof| {
                    valid_join_progress_proof(
                        proof,
                        producer,
                        consumer.fragment_id,
                        fragment_edges,
                        &dependencies,
                    )
                });
            let source = if let Some(proof) = proof {
                let key = (proof.producer_fragment_id, proof.join_node_id);
                let node = if let Some(node) = build_ready_nodes.get(&key) {
                    *node
                } else {
                    let node = next_virtual_node;
                    next_virtual_node = next_virtual_node.checked_add(1).ok_or_else(|| {
                        compilation_error("runtime filter wait graph node id overflow")
                    })?;
                    build_ready_nodes.insert(key, node);
                    node
                };
                for &(source_fragment_id, _) in &proof.build_frontier {
                    edges.push(RuntimeFilterWaitEdge::new(source_fragment_id, node, true));
                }
                edges.push(RuntimeFilterWaitEdge::new(
                    node,
                    proof.producer_fragment_id,
                    true,
                ));
                node
            } else {
                producer.fragment_id
            };
            edges.push(RuntimeFilterWaitEdge::new(
                source,
                consumer.fragment_id,
                true,
            ));
            sources.push(source);
        }
        consumer_sources.push((consumer.fragment_id, sources));
    }

    let mut outgoing = BTreeMap::<u32, BTreeSet<(u32, i32)>>::new();
    for edge in fragment_edges {
        outgoing
            .entry(edge.source_fragment_id)
            .or_default()
            .insert((edge.target_fragment_id, edge.target_exchange_node_id));
    }
    for (multicast_fragment, branches) in outgoing {
        if branches.len() < 2 {
            continue;
        }
        for (target_fragment, _) in branches {
            for (consumer_fragment, sources) in &consumer_sources {
                if *consumer_fragment != target_fragment
                    && !dependencies
                        .get(consumer_fragment)
                        .is_some_and(|predecessors| predecessors.contains(&target_fragment))
                {
                    continue;
                }
                for source in sources {
                    edges.push(RuntimeFilterWaitEdge::new(
                        *source,
                        multicast_fragment,
                        true,
                    ));
                }
            }
        }
    }
    Ok(RuntimeFilterWaitGraph::new(edges))
}

fn fragment_dependencies(
    fragment_edges: &[FragmentEdgeSpec],
) -> Result<BTreeMap<u32, BTreeSet<u32>>, DistributedQueryError> {
    let mut direct = BTreeMap::<u32, BTreeSet<u32>>::new();
    let mut nodes = BTreeSet::new();
    for edge in fragment_edges {
        nodes.insert(edge.source_fragment_id);
        nodes.insert(edge.target_fragment_id);
        direct
            .entry(edge.target_fragment_id)
            .or_default()
            .insert(edge.source_fragment_id);
    }
    let mut dependencies = BTreeMap::new();
    for node in nodes {
        let mut closure = BTreeSet::new();
        let mut pending = direct
            .get(&node)
            .into_iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        while let Some(fragment) = pending.pop() {
            if closure.insert(fragment) {
                pending.extend(direct.get(&fragment).into_iter().flatten().copied());
            }
        }
        dependencies.insert(node, closure);
    }
    Ok(dependencies)
}

fn valid_join_progress_proof(
    proof: &JoinProgressProof,
    producer: &BindingSpec,
    consumer_fragment_id: u32,
    fragment_edges: &[FragmentEdgeSpec],
    dependencies: &BTreeMap<u32, BTreeSet<u32>>,
) -> bool {
    if proof.channel_id != producer.channel_id
        || proof.producer_binding_id != producer.binding_id
        || proof.producer_fragment_id != producer.fragment_id
        || proof.join_node_id != producer.node_id
        || !matches!(
            producer.role,
            BindingRole::Producer { completion }
                if completion == plan::RuntimeFilterCompletionRequirement::ProducerClosed as i32
        )
    {
        return false;
    }
    let sealed_inputs = fragment_edges
        .iter()
        .filter(|edge| edge.target_fragment_id == proof.producer_fragment_id)
        .map(|edge| (edge.source_fragment_id, edge.target_exchange_node_id))
        .collect::<BTreeSet<_>>();
    let frontier = proof
        .build_frontier
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let non_build = proof
        .non_build_inputs
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if frontier.len() != proof.build_frontier.len()
        || non_build.len() != proof.non_build_inputs.len()
        || !frontier.is_disjoint(&non_build)
        || frontier.union(&non_build).copied().collect::<BTreeSet<_>>() != sealed_inputs
    {
        return false;
    }
    consumer_fragment_id == proof.producer_fragment_id
        || non_build.iter().any(|(source_fragment_id, _)| {
            *source_fragment_id == consumer_fragment_id
                || dependencies
                    .get(source_fragment_id)
                    .is_some_and(|predecessors| predecessors.contains(&consumer_fragment_id))
        })
}

fn coverage(
    value: RuntimeFilterCoverageFacts,
) -> Result<filter::RuntimeFilterCoverage, DistributedQueryError> {
    use filter::runtime_filter_coverage::Kind;
    let kind = match value {
        RuntimeFilterCoverageFacts::LeafWitnessId(id) => {
            Kind::LeafWitnessId(require_nonzero(id, "coverage witness id")?)
        }
        RuntimeFilterCoverageFacts::AllOf(children) => {
            if children.is_empty() {
                return Err(compilation_error(
                    "runtime filter AllOf coverage must be nonempty",
                ));
            }
            Kind::AllOf(filter::RuntimeFilterCoverageAllOf {
                children: children
                    .into_iter()
                    .map(coverage)
                    .collect::<Result<_, _>>()?,
            })
        }
        RuntimeFilterCoverageFacts::AnyOf(children) => {
            if children.is_empty() {
                return Err(compilation_error(
                    "runtime filter AnyOf coverage must be nonempty",
                ));
            }
            Kind::AnyOf(filter::RuntimeFilterCoverageAnyOf {
                children: children
                    .into_iter()
                    .map(coverage)
                    .collect::<Result<_, _>>()?,
            })
        }
    };
    Ok(filter::RuntimeFilterCoverage { kind: Some(kind) })
}

fn coverage_leaves(
    coverage: &filter::RuntimeFilterCoverage,
    output: &mut BTreeSet<u32>,
) -> Result<(), DistributedQueryError> {
    match coverage
        .kind
        .as_ref()
        .ok_or_else(|| compilation_error("runtime filter coverage is missing its kind"))?
    {
        filter::runtime_filter_coverage::Kind::LeafWitnessId(id) => {
            if !output.insert(require_nonzero(*id, "coverage witness id")?) {
                return Err(compilation_error(format!(
                    "runtime filter coverage repeats witness {id}"
                )));
            }
        }
        filter::runtime_filter_coverage::Kind::AllOf(all) => {
            for child in &all.children {
                coverage_leaves(child, output)?;
            }
        }
        filter::runtime_filter_coverage::Kind::AnyOf(any) => {
            for child in &any.children {
                coverage_leaves(child, output)?;
            }
        }
    }
    Ok(())
}

fn coverage_is_any_of(coverage: &filter::RuntimeFilterCoverage) -> bool {
    matches!(
        coverage.kind,
        Some(filter::runtime_filter_coverage::Kind::AnyOf(_))
    )
}

fn unique_contribution_kinds(
    values: Vec<RuntimeFilterContributionKind>,
) -> Result<Vec<i32>, DistributedQueryError> {
    let values = values
        .into_iter()
        .map(|value| match value {
            RuntimeFilterContributionKind::ValueDomainDelta => {
                plan::RuntimeFilterContributionKind::ValueDomainDelta as i32
            }
            RuntimeFilterContributionKind::FinalDomainShard => {
                plan::RuntimeFilterContributionKind::FinalDomainShard as i32
            }
            RuntimeFilterContributionKind::OrderedBoundUpdate => {
                plan::RuntimeFilterContributionKind::OrderedBoundUpdate as i32
            }
            RuntimeFilterContributionKind::TopKSummary => {
                plan::RuntimeFilterContributionKind::TopkSummary as i32
            }
            RuntimeFilterContributionKind::ProducerClosed => {
                plan::RuntimeFilterContributionKind::ProducerClosed as i32
            }
        })
        .collect::<BTreeSet<_>>();
    if values.is_empty() {
        return Err(compilation_error(
            "runtime filter allowed contribution kinds must be nonempty",
        ));
    }
    Ok(values.into_iter().collect())
}

fn unique_capabilities(
    values: Vec<RuntimeFilterArtifactCapability>,
) -> Result<BTreeSet<Capability>, DistributedQueryError> {
    let values = values
        .into_iter()
        .map(|value| match value {
            RuntimeFilterArtifactCapability::Membership => Capability::Membership,
            RuntimeFilterArtifactCapability::OrderedRange => Capability::OrderedRange,
            RuntimeFilterArtifactCapability::EmptyDomain => Capability::EmptyDomain,
        })
        .collect::<BTreeSet<_>>();
    if values.is_empty() {
        return Err(compilation_error(
            "runtime filter consumer capabilities must be nonempty",
        ));
    }
    Ok(values)
}

fn completion(value: RuntimeFilterCompletionRequirement) -> i32 {
    match value {
        RuntimeFilterCompletionRequirement::ProducerClosed => {
            plan::RuntimeFilterCompletionRequirement::ProducerClosed as i32
        }
        RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen => {
            plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen as i32
        }
    }
}

fn activation_wire(
    value: RuntimeFilterConsumerActivation,
) -> plan::RuntimeFilterConsumerActivation {
    use plan::runtime_filter_consumer_activation::Kind;
    let kind = match value {
        RuntimeFilterConsumerActivation::BlockingSnapshot => Kind::BlockingSnapshot(true),
        RuntimeFilterConsumerActivation::NonBlockingLive(granularity) => {
            Kind::NonBlockingLive(match granularity {
                RuntimeFilterLateApplyGranularity::Row => {
                    plan::RuntimeFilterLateApplyGranularity::Row as i32
                }
                RuntimeFilterLateApplyGranularity::Batch => {
                    plan::RuntimeFilterLateApplyGranularity::Batch as i32
                }
                RuntimeFilterLateApplyGranularity::RowGroup => {
                    plan::RuntimeFilterLateApplyGranularity::RowGroup as i32
                }
                RuntimeFilterLateApplyGranularity::Split => {
                    plan::RuntimeFilterLateApplyGranularity::Split as i32
                }
                RuntimeFilterLateApplyGranularity::File => {
                    plan::RuntimeFilterLateApplyGranularity::File as i32
                }
            })
        }
    };
    plan::RuntimeFilterConsumerActivation { kind: Some(kind) }
}

fn channel_completion(
    bindings: &BTreeMap<u32, BindingSpec>,
    channel_id: u32,
) -> Result<i32, DistributedQueryError> {
    let mut completion = None;
    for binding in bindings
        .values()
        .filter(|binding| binding.channel_id == channel_id)
    {
        if let BindingRole::Producer { completion: value } = binding.role
            && let Some(previous) = completion.replace(value)
            && previous != value
        {
            return Err(compilation_error(format!(
                "runtime filter channel {channel_id} has incompatible producer completion requirements"
            )));
        }
    }
    completion.ok_or_else(|| {
        compilation_error(format!(
            "runtime filter channel {channel_id} has no producer completion requirement"
        ))
    })
}

fn artifact_profile(
    channel: &ChannelSpec,
    capabilities: &BTreeSet<Capability>,
) -> Result<filter::RuntimeFilterConsumerArtifactProfile, DistributedQueryError> {
    let (kinds, order) = match channel
        .logical_domain
        .contract
        .as_ref()
        .and_then(|contract| contract.kind.as_ref())
    {
        Some(plan::runtime_filter_contract::Kind::Ordered(ordered))
            if capabilities.contains(&Capability::OrderedRange) =>
        {
            (
                vec![filter::RuntimeFilterArtifactKind::Range as i32],
                Some(ordered.order_contract_digest.clone()),
            )
        }
        _ => {
            let mut kinds = Vec::new();
            if capabilities.contains(&Capability::Membership) {
                kinds.push(filter::RuntimeFilterArtifactKind::ValueSet as i32);
            }
            if capabilities.contains(&Capability::EmptyDomain) {
                kinds.push(filter::RuntimeFilterArtifactKind::EmptyDomain as i32);
            }
            (kinds, None)
        }
    };
    if kinds.is_empty() {
        return Err(compilation_error(format!(
            "runtime filter channel {} consumer profile has no supported artifact kind",
            channel.channel_id
        )));
    }
    let mut canonical = Vec::new();
    canonical.push(if order.is_some() { 2 } else { 1 });
    canonical.extend_from_slice(
        &u16::try_from(kinds.len())
            .map_err(|_| compilation_error("runtime filter profile kind count overflow"))?
            .to_be_bytes(),
    );
    canonical.extend(kinds.iter().map(|kind| {
        match filter::RuntimeFilterArtifactKind::try_from(*kind).expect("known artifact kind") {
            filter::RuntimeFilterArtifactKind::ValueSet => 1,
            filter::RuntimeFilterArtifactKind::EmptyDomain => 5,
            filter::RuntimeFilterArtifactKind::Range => 4,
            _ => unreachable!(),
        }
    }));
    canonical.push(0);
    if let Some(digest) = &order {
        if digest.len() != 32 {
            return Err(compilation_error(
                "runtime filter ordered profile digest must be 32 bytes",
            ));
        }
        canonical.push(1);
        canonical.extend_from_slice(digest);
    }
    let profile_id = Sha256::digest(&canonical).to_vec();
    Ok(filter::RuntimeFilterConsumerArtifactProfile {
        accepted_kinds: kinds,
        bloom_hash_contract: None,
        order_contract_digest: order,
        profile_id,
    })
}

fn capability_wire(value: Capability) -> i32 {
    match value {
        Capability::Membership => plan::RuntimeFilterArtifactCapability::Membership as i32,
        Capability::OrderedRange => plan::RuntimeFilterArtifactCapability::OrderedRange as i32,
        Capability::EmptyDomain => plan::RuntimeFilterArtifactCapability::EmptyDomain as i32,
    }
}
fn route_role_wire(value: RouteRole) -> filter::RuntimeFilterRouteRole {
    use filter::runtime_filter_route_role::Role;
    filter::RuntimeFilterRouteRole {
        role: Some(match value {
            RouteRole::Producer(binding) => Role::ProducerBindingId(binding),
            RouteRole::Aggregator => Role::Aggregator(true),
            RouteRole::Consumer(binding) => Role::ConsumerBindingId(binding),
        }),
    }
}
fn route_endpoint(participant_id: u32, role: RouteRole) -> filter::RuntimeFilterRouteEndpointView {
    filter::RuntimeFilterRouteEndpointView {
        participant_id,
        role: Some(route_role_wire(role)),
    }
}
fn delivery_kinds() -> Vec<i32> {
    vec![
        filter::RuntimeFilterEnvelopeKind::Artifact as i32,
        filter::RuntimeFilterEnvelopeKind::FinalArtifact as i32,
        filter::RuntimeFilterEnvelopeKind::Unavailable as i32,
        filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact as i32,
        filter::RuntimeFilterEnvelopeKind::DegradedLogical as i32,
    ]
}
fn contribution_kinds() -> Vec<i32> {
    vec![
        filter::RuntimeFilterEnvelopeKind::Contribution as i32,
        filter::RuntimeFilterEnvelopeKind::ProducerClosed as i32,
        filter::RuntimeFilterEnvelopeKind::ProducerUnavailable as i32,
    ]
}
fn participant_id(backend_idx: usize) -> Result<u32, DistributedQueryError> {
    let value = backend_idx.checked_add(1).ok_or_else(|| {
        compilation_error("runtime filter backend index overflows participant identity")
    })?;
    u32::try_from(value).map_err(|_| {
        compilation_error("runtime filter backend index exceeds participant identity width")
    })
}
fn unique_id_wire(value: UniqueId) -> common::UniqueId {
    common::UniqueId {
        hi: value.high(),
        lo: value.low(),
    }
}
fn require_nonzero(value: u32, label: &str) -> Result<u32, DistributedQueryError> {
    if value == 0 {
        Err(compilation_error(format!(
            "runtime filter {label} must be nonzero"
        )))
    } else {
        Ok(value)
    }
}
fn allocate_edge(next: &mut u32) -> Result<u32, DistributedQueryError> {
    let id = *next;
    *next = next.checked_add(1).ok_or_else(|| {
        compilation_error("runtime filter route edge identity space is exhausted")
    })?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producer(binding_id: u32, fragment_id: u32, node_id: i32) -> BindingSpec {
        BindingSpec {
            binding_id,
            channel_id: 1,
            fragment_id,
            node_id,
            witness: Some(binding_id),
            role: BindingRole::Producer {
                completion: plan::RuntimeFilterCompletionRequirement::ProducerClosed as i32,
            },
        }
    }

    fn consumer(binding_id: u32, fragment_id: u32, blocking: bool) -> BindingSpec {
        let kind = if blocking {
            plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true)
        } else {
            plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(
                plan::RuntimeFilterLateApplyGranularity::Batch as i32,
            )
        };
        BindingSpec {
            binding_id,
            channel_id: 1,
            fragment_id,
            node_id: 22,
            witness: None,
            role: BindingRole::Consumer {
                capabilities: BTreeSet::from([Capability::Membership]),
                activation: plan::RuntimeFilterConsumerActivation { kind: Some(kind) },
            },
        }
    }

    #[test]
    fn fragment_dependency_closure_is_stable_under_input_order() {
        let ordered = [
            FragmentEdgeSpec {
                source_fragment_id: 1,
                target_fragment_id: 3,
                target_exchange_node_id: 11,
            },
            FragmentEdgeSpec {
                source_fragment_id: 2,
                target_fragment_id: 3,
                target_exchange_node_id: 12,
            },
            FragmentEdgeSpec {
                source_fragment_id: 3,
                target_fragment_id: 4,
                target_exchange_node_id: 13,
            },
        ];
        let reordered = [ordered[2], ordered[0], ordered[1]];

        let dependencies = fragment_dependencies(&ordered).expect("sealed edges are valid");
        assert_eq!(dependencies, fragment_dependencies(&reordered).unwrap());
        assert_eq!(dependencies[&3], BTreeSet::from([1, 2]));
        assert_eq!(dependencies[&4], BTreeSet::from([1, 2, 3]));
    }

    #[test]
    fn join_progress_proof_rejects_mismatch_and_invalid_frontiers() {
        let producer = producer(7, 2, 100);
        let edges = [
            FragmentEdgeSpec {
                source_fragment_id: 1,
                target_fragment_id: 2,
                target_exchange_node_id: 11,
            },
            FragmentEdgeSpec {
                source_fragment_id: 3,
                target_fragment_id: 2,
                target_exchange_node_id: 13,
            },
        ];
        let dependencies = fragment_dependencies(&edges).unwrap();
        let valid = JoinProgressProof {
            channel_id: 1,
            producer_binding_id: 7,
            producer_fragment_id: 2,
            join_node_id: 100,
            build_frontier: vec![(1, 11)],
            non_build_inputs: vec![(3, 13)],
        };
        assert!(valid_join_progress_proof(
            &valid,
            &producer,
            3,
            &edges,
            &dependencies
        ));

        let mut wrong_binding = valid.clone();
        wrong_binding.producer_binding_id = 8;
        let mut incomplete = valid.clone();
        incomplete.non_build_inputs.clear();
        let mut overlap = valid.clone();
        overlap.non_build_inputs = vec![(1, 11), (3, 13)];
        let mut duplicate = valid.clone();
        duplicate.build_frontier.push((1, 11));

        for (name, proof) in [
            ("binding mismatch", wrong_binding),
            ("incomplete frontier", incomplete),
            ("overlapping frontier", overlap),
            ("duplicate frontier edge", duplicate),
        ] {
            assert!(
                !valid_join_progress_proof(&proof, &producer, 3, &edges, &dependencies),
                "{name} must be rejected"
            );
        }
    }

    #[test]
    fn blocking_consumer_cycle_is_rejected_but_live_apply_is_not() {
        let producer = producer(1, 1, 10);
        let edge = FragmentEdgeSpec {
            source_fragment_id: 2,
            target_fragment_id: 1,
            target_exchange_node_id: 20,
        };
        for (name, blocking, expected_live) in [
            ("blocking snapshot", true, false),
            ("nonblocking live apply", false, true),
        ] {
            let bindings = BTreeMap::from([
                (producer.binding_id, producer.clone()),
                (2, consumer(2, 2, blocking)),
            ]);
            let graph = build_wait_graph(&bindings, &[edge], &BTreeMap::new())
                .expect("sealed facts can construct a wait graph");
            assert_eq!(
                graph.validate().is_ok(),
                expected_live,
                "{name} must have the expected liveness result"
            );
        }
    }
}
