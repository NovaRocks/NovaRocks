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

//! Pure validation for a participant deployment contract.
//!
//! This module owns no service state. The deployment registry and native wire
//! codec both call the same validator so an install cannot be accepted by one
//! boundary and rejected by the other.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;
use crate::runtime_filter::materializer::bloom::BloomHashContract;
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, CompletionFenceKind, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, ReductionRequirement, RuntimeFilterLifecycle,
    RuntimeFilterLogicalDomain,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::model::policy::validate_runtime_filter_policy;
use crate::runtime_filter::port::artifact::{
    ArtifactKind, ArtifactMembershipSchema, ConsumerProfileId,
};
use crate::runtime_filter::port::identity::RuntimeFilterParticipantId;
use crate::runtime_filter::port::install::{
    OutboundMaterializationOwner, RuntimeFilterChannelDeployment, RuntimeFilterInstallView,
    RuntimeFilterParticipantInstall,
};
use crate::runtime_filter::port::ordered_bound::RuntimeOrderContract;
use crate::runtime_filter::port::producer::{InstallContractError, InstallContractErrorKind};
use crate::runtime_filter::port::routing::{
    RuntimeFilterChannelRoutingView, RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView,
    RuntimeFilterRoutingShard, canonical_route_allowed_kinds,
};
use crate::runtime_filter::port::topk_summary::RuntimeTopKSummaryContract;
use crate::runtime_filter::port::value_domain::MembershipValues;

pub fn validate_participant_install(
    install: &RuntimeFilterParticipantInstall,
) -> Result<(), InstallContractError> {
    validate_install_identity(install)?;
    if install.core_view().is_empty() && install.routing_shard().channels().is_empty() {
        return Ok(());
    }
    validate_view_with_routing(install.core_view(), Some(install.routing_shard()))
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
pub fn validate_install_view_contract_for_test(
    view: &RuntimeFilterInstallView,
) -> Result<(), InstallContractError> {
    validate_view_with_routing(view, None)
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
pub fn validate_channel_contract_for_test(
    channel: &RuntimeFilterChannelDeployment,
) -> Result<(), InstallContractError> {
    validate_channel(channel, &mut BTreeMap::new(), (true, true))
}

fn validate_install_identity(
    install: &RuntimeFilterParticipantInstall,
) -> Result<(), InstallContractError> {
    if install.core_view().epoch() != install.routing_shard().deployment_epoch() {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "participant install core and routing epochs differ",
        ));
    }
    if install.core_view().local_participant_id() != install.routing_shard().local_participant_id()
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "participant install core and routing participants differ",
        ));
    }
    Ok(())
}

fn validate_view_with_routing<'a>(
    view: &'a RuntimeFilterInstallView,
    routing_shard: Option<&RuntimeFilterRoutingShard>,
) -> Result<(), InstallContractError> {
    if view.epoch().get() == 0 {
        return Err(install_error(
            InstallContractErrorKind::InvalidEpoch,
            "deployment epoch must be non-zero",
        ));
    }

    if let Some(shard) = routing_shard {
        for (channel_id, routing) in shard.channels() {
            validate_route_family_contract(routing)?;
            let requires_core = routing.local_roles().iter().any(|role| {
                matches!(
                    role,
                    RuntimeFilterRouteRole::Producer(_)
                        | RuntimeFilterRouteRole::Aggregator
                        | RuntimeFilterRouteRole::Consumer(_)
                )
            });
            let relay_only =
                routing.local_roles() == &BTreeSet::from([RuntimeFilterRouteRole::Relay]);
            match (requires_core, view.channels().contains_key(channel_id)) {
                (true, false) => {
                    return Err(install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "routing channel {} requires Core authority for its local roles",
                            channel_id.get()
                        ),
                    ));
                }
                (false, true) => {
                    return Err(install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "routing-only channel {} must not carry fake Core authority",
                            channel_id.get()
                        ),
                    ));
                }
                (false, false) if !relay_only => {
                    return Err(install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "routing channel {} has no genuine local role",
                            channel_id.get()
                        ),
                    ));
                }
                _ => {}
            }
        }
    }

    validate_install_identities(view)?;
    let mut profile_encodings = BTreeMap::<ConsumerProfileId, &'a [u8]>::new();
    for channel in view.channels().values() {
        if channel
            .producers()
            .values()
            .any(|producer| producer.expected_fragment_instances().is_empty())
            || channel
                .consumers()
                .values()
                .any(|consumer| consumer.expected_fragment_instances().is_empty())
        {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "producer and consumer expected fragment instances must be non-empty",
            ));
        }
        let role_requirements = match routing_shard {
            Some(shard) => {
                let routing = shard.channel(channel.channel_id()).ok_or_else(|| {
                    install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "core channel {} is missing from routing shard",
                            channel.channel_id().get()
                        ),
                    )
                })?;
                validate_channel_routing_contract(view.local_participant_id(), channel, routing)?
            }
            None => (true, true),
        };
        validate_channel(channel, &mut profile_encodings, role_requirements)?;
    }
    Ok(())
}

fn validate_route_family_contract(
    routing: &RuntimeFilterChannelRoutingView,
) -> Result<(), InstallContractError> {
    for edge in routing
        .inbound_edges()
        .iter()
        .chain(routing.outbound_edges())
    {
        let Some(expected) =
            canonical_route_allowed_kinds(edge.source().role(), edge.target().role())
        else {
            return Err(invalid_route_family(
                edge,
                "endpoint role pair is not canonical",
            ));
        };
        if edge.allowed_kinds() != &expected {
            return Err(invalid_route_family(
                edge,
                "allowed kinds do not exactly match the endpoint route family",
            ));
        }
    }
    Ok(())
}

fn invalid_route_family(edge: &RuntimeFilterRoutingEdgeView, detail: &str) -> InstallContractError {
    install_error(
        InstallContractErrorKind::UnsupportedChannelContract,
        format!(
            "routing edge {} {detail}: source {:?}, target {:?}, allowed {:?}",
            edge.route_edge_id().get(),
            edge.source().role(),
            edge.target().role(),
            edge.allowed_kinds(),
        ),
    )
}

fn validate_install_identities(
    view: &RuntimeFilterInstallView,
) -> Result<(), InstallContractError> {
    let mut channel_ids = BTreeSet::new();
    let mut binding_ids = BTreeSet::new();
    let mut route_ids = BTreeSet::new();
    for (map_channel_id, channel) in view.channels() {
        if *map_channel_id != channel.channel_id() || !channel_ids.insert(channel.channel_id()) {
            return Err(install_error(
                InstallContractErrorKind::DuplicateIdentity,
                "channel map key and channel identity must match and be unique",
            ));
        }
        for binding_id in channel.producers().keys() {
            if !binding_ids.insert(*binding_id) {
                return Err(install_error(
                    InstallContractErrorKind::DuplicateIdentity,
                    "producer binding identities must be unique across the install view",
                ));
            }
        }
        for (binding_id, consumer) in channel.consumers() {
            if !binding_ids.insert(*binding_id) || consumer.route_edge_ids().is_empty() {
                return Err(install_error(
                    InstallContractErrorKind::DuplicateIdentity,
                    "consumer binding identities must be unique and route sets must be nonempty",
                ));
            }
            for route_edge_id in consumer.route_edge_ids() {
                if !route_ids.insert(*route_edge_id) {
                    return Err(install_error(
                        InstallContractErrorKind::DuplicateIdentity,
                        "consumer route identities must be unique across the install view",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_channel_routing_contract(
    local_participant_id: RuntimeFilterParticipantId,
    channel: &RuntimeFilterChannelDeployment,
    routing: &RuntimeFilterChannelRoutingView,
) -> Result<(bool, bool), InstallContractError> {
    let is_local_aggregator = routing
        .local_roles()
        .contains(&RuntimeFilterRouteRole::Aggregator);
    let local_producer_bindings = routing
        .local_roles()
        .iter()
        .filter_map(|role| match role {
            RuntimeFilterRouteRole::Producer(binding_id) => Some(*binding_id),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let local_consumer_bindings = routing
        .local_roles()
        .iter()
        .filter_map(|role| match role {
            RuntimeFilterRouteRole::Consumer(binding_id) => Some(*binding_id),
            _ => None,
        })
        .collect::<BTreeSet<_>>();

    for (binding_id, producer) in channel.producers() {
        for fragment_instance_id in producer.expected_fragment_instances() {
            let participant_id = routing
                .producer_participant(*binding_id, *fragment_instance_id)
                .ok_or_else(|| {
                    install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "producer binding {} instance {:?} is missing from routing producer index",
                            binding_id.get(), fragment_instance_id
                        ),
                    )
                })?;
            if !is_local_aggregator && participant_id != local_participant_id {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "non-aggregator producer binding {} instance {:?} maps to remote participant {:?}",
                        binding_id.get(),
                        fragment_instance_id,
                        participant_id
                    ),
                ));
            }
            if participant_id == local_participant_id
                && !routing
                    .local_roles()
                    .contains(&RuntimeFilterRouteRole::Producer(*binding_id))
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "local producer binding {} has no matching local Producer role",
                        binding_id.get()
                    ),
                ));
            }
        }
    }

    if is_local_aggregator {
        for ((binding_id, fragment_instance_id), _) in routing.producer_instances() {
            let installed = channel.producers().get(binding_id).is_some_and(|producer| {
                producer
                    .expected_fragment_instances()
                    .contains(fragment_instance_id)
            });
            if !installed {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "aggregator core is missing routing-authorized producer binding {} instance {:?}",
                        binding_id.get(),
                        fragment_instance_id
                    ),
                ));
            }
        }
    }

    let expected_producer_instances = routing
        .producer_instances()
        .iter()
        .filter(|(_, participant_id)| {
            is_local_aggregator || **participant_id == local_participant_id
        })
        .fold(
            BTreeMap::<BindingId, BTreeSet<UniqueId>>::new(),
            |mut expected, ((binding_id, fragment_instance_id), _)| {
                expected
                    .entry(*binding_id)
                    .or_default()
                    .insert(*fragment_instance_id);
                expected
            },
        );
    let installed_producer_instances = channel
        .producers()
        .iter()
        .map(|(binding_id, producer)| (*binding_id, producer.expected_fragment_instances().clone()))
        .collect::<BTreeMap<_, _>>();
    if installed_producer_instances != expected_producer_instances {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Core producer authority does not exactly match local routing producer roles",
        ));
    }
    if !is_local_aggregator
        && channel.producers().keys().copied().collect::<BTreeSet<_>>() != local_producer_bindings
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Core producer bindings do not exactly match local Producer roles",
        ));
    }

    if is_local_aggregator {
        validate_aggregator_edges(local_participant_id, routing)?;
    }
    if channel.consumers().keys().copied().collect::<BTreeSet<_>>() != local_consumer_bindings {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Core consumer bindings do not exactly match local Consumer roles",
        ));
    }
    for (binding_id, consumer) in channel.consumers() {
        let expected_routes = routing
            .inbound_edges()
            .iter()
            .filter(|edge| {
                edge.target().participant_id() == local_participant_id
                    && edge.target().role() == RuntimeFilterRouteRole::Consumer(*binding_id)
            })
            .map(|edge| edge.route_edge_id())
            .collect::<BTreeSet<_>>();
        if consumer.route_edge_ids() != &expected_routes || expected_routes.is_empty() {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                format!(
                    "consumer binding {} Core route authority does not exactly match inbound routing edges",
                    binding_id.get()
                ),
            ));
        }
    }
    validate_outbound_materialization_contract(local_participant_id, channel, routing)?;
    Ok((
        is_local_aggregator || !local_producer_bindings.is_empty(),
        !local_consumer_bindings.is_empty(),
    ))
}

fn validate_outbound_materialization_contract(
    local_participant_id: RuntimeFilterParticipantId,
    channel: &RuntimeFilterChannelDeployment,
    routing: &RuntimeFilterChannelRoutingView,
) -> Result<(), InstallContractError> {
    let mut expected = BTreeMap::new();
    for edge in routing
        .outbound_edges()
        .iter()
        .filter(|edge| matches!(edge.target().role(), RuntimeFilterRouteRole::Consumer(_)))
    {
        if edge.source().participant_id() != local_participant_id {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "outbound materialization edge must originate locally",
            ));
        }
        let owner = match edge.source().role() {
            RuntimeFilterRouteRole::Producer(_) => OutboundMaterializationOwner::DirectSource,
            RuntimeFilterRouteRole::Aggregator => OutboundMaterializationOwner::Aggregator,
            _ => {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "outbound materialization edge must originate from a Producer or Aggregator role",
                ));
            }
        };
        if expected.insert(edge.route_edge_id(), owner).is_some() {
            return Err(install_error(
                InstallContractErrorKind::DuplicateIdentity,
                "outbound materialization route identity is duplicated",
            ));
        }
    }

    let mut actual = BTreeMap::new();
    for (profile_id, group) in channel.outbound_materialization_groups() {
        if *profile_id != group.profile().id() || group.route_edge_ids().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::DuplicateIdentity,
                "outbound materialization profile key must match and own a nonempty route set",
            ));
        }
        let owner_role_present = match group.owner() {
            OutboundMaterializationOwner::DirectSource => routing
                .local_roles()
                .iter()
                .any(|role| matches!(role, RuntimeFilterRouteRole::Producer(_))),
            OutboundMaterializationOwner::Aggregator => routing
                .local_roles()
                .contains(&RuntimeFilterRouteRole::Aggregator),
        };
        if !owner_role_present {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "outbound materialization owner has no matching local routing role",
            ));
        }
        for route in group.route_edge_ids() {
            if actual.insert(*route, group.owner()).is_some() {
                return Err(install_error(
                    InstallContractErrorKind::DuplicateIdentity,
                    "outbound materialization route belongs to more than one profile group",
                ));
            }
        }
    }
    if actual != expected {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "outbound materialization groups do not exactly cover local Artifact/Unavailable edges",
        ));
    }
    for (binding_id, consumer) in channel.consumers() {
        for route in consumer.route_edge_ids() {
            let is_loopback_outbound = routing.outbound_edges().iter().any(|edge| {
                edge.route_edge_id() == *route
                    && edge.target().role() == RuntimeFilterRouteRole::Consumer(*binding_id)
            });
            if is_loopback_outbound {
                let Some(group) = channel
                    .outbound_materialization_groups()
                    .values()
                    .find(|group| group.route_edge_ids().contains(route))
                else {
                    return Err(install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        "loopback consumer route is missing materialization authority",
                    ));
                };
                if group.profile().canonical_bytes()
                    != consumer.artifact_profile().canonical_bytes()
                {
                    return Err(install_error(
                        InstallContractErrorKind::ConflictingDeployment,
                        "loopback consumer and materializer profiles differ",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_aggregator_edges(
    local_participant_id: RuntimeFilterParticipantId,
    routing: &RuntimeFilterChannelRoutingView,
) -> Result<(), InstallContractError> {
    let authorized_sources = routing
        .producer_instances()
        .iter()
        .map(|((binding_id, _), participant_id)| (*binding_id, *participant_id))
        .collect::<BTreeSet<_>>();
    for edge in routing.inbound_edges().iter().filter(|edge| {
        matches!(edge.source().role(), RuntimeFilterRouteRole::Producer(_))
            && edge.target().participant_id() == local_participant_id
            && edge.target().role() == RuntimeFilterRouteRole::Aggregator
    }) {
        let RuntimeFilterRouteRole::Producer(binding_id) = edge.source().role() else {
            unreachable!("inbound producer edges were filtered by source role")
        };
        if !authorized_sources.contains(&(binding_id, edge.source().participant_id())) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "aggregator inbound producer edge source has no authorized producer instance",
            ));
        }
    }
    for (binding_id, source_participant_id) in authorized_sources {
        let matching_edges = routing
            .inbound_edges()
            .iter()
            .filter(|edge| {
                edge.source().participant_id() == source_participant_id
                    && edge.source().role() == RuntimeFilterRouteRole::Producer(binding_id)
                    && edge.target().participant_id() == local_participant_id
                    && edge.target().role() == RuntimeFilterRouteRole::Aggregator
            })
            .collect::<Vec<_>>();
        if matching_edges.len() != 1 {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                format!(
                    "aggregator producer binding {} source participant {:?} requires exactly one inbound Producer-to-Aggregator edge",
                    binding_id.get(),
                    source_participant_id
                ),
            ));
        }
    }
    Ok(())
}

fn validate_channel<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
    role_requirements: (bool, bool),
) -> Result<(), InstallContractError> {
    if matches!(
        channel.logical_domain(),
        RuntimeFilterLogicalDomain::OrderedBound(_)
    ) {
        return validate_ordered_channel(channel, profile_encodings, role_requirements);
    }
    validate_membership_channel(channel, profile_encodings, role_requirements)
}

fn validate_membership_channel<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
    role_requirements: (bool, bool),
) -> Result<(), InstallContractError> {
    let RuntimeFilterLogicalDomain::Membership {
        value_type,
        null_semantics,
    } = channel.logical_domain()
    else {
        unreachable!("membership validator is called only for membership channels")
    };
    let ordinary = channel.lifecycle() == RuntimeFilterLifecycle::CompleteOnce
        && channel.reduction_requirement() == ReductionRequirement::SetUnion
        && channel.allowed_contribution_kinds()
            == &BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ])
        && channel.completion_requirement() == CompletionRequirement::ProducerClosed;
    let fenced_final = channel.lifecycle() == RuntimeFilterLifecycle::CompleteOnce
        && channel.reduction_requirement() == ReductionRequirement::SetUnion
        && channel.allowed_contribution_kinds()
            == &BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ])
        && channel.completion_requirement()
            == CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen)
        && *null_semantics == crate::runtime_filter::model::contract::NullSemantics::NullSafeEqual
        && channel.availability_coverage().is_all_of_only()
        && channel.terminal_coverage().is_all_of_only();
    if !ordinary && !fenced_final {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "channel does not match the CompleteOnce Membership SetUnion matrix",
        ));
    }
    if MembershipValues::empty_for_data_type(value_type).is_none() {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedMembershipType,
            "membership data type is not supported by the runtime filter port",
        ));
    }
    validate_common_channel(channel, role_requirements)?;

    let schema = ArtifactMembershipSchema::new(value_type, *null_semantics).map_err(|_| {
        install_error(
            InstallContractErrorKind::UnsupportedMembershipType,
            "membership schema has no canonical artifact encoding",
        )
    })?;
    validate_producer_coverage(channel)?;
    if !channel
        .availability_coverage()
        .is_canonically_equivalent_to(channel.terminal_coverage())
    {
        return Err(install_error(
            InstallContractErrorKind::InvalidCoverage,
            "CompleteOnce availability and terminal coverage must be canonically equivalent",
        ));
    }

    let mut unique_profiles = BTreeSet::new();
    for consumer in channel.consumers().values() {
        if ordinary && !consumer.activation().is_blocking_or_batch_live() {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "M1 consumers must use BlockingSnapshot or Batch NonBlockingLive activation",
            ));
        }
        if fenced_final
            && !matches!(
                consumer.activation(),
                ConsumerActivation::NonBlockingLive { .. }
            )
        {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "fenced-final consumers must use NonBlockingLive activation",
            ));
        }
        validate_membership_consumer(
            channel,
            consumer,
            &schema,
            fenced_final,
            &mut unique_profiles,
            profile_encodings,
        )?;
    }
    let mut materialization_profiles = BTreeSet::new();
    for group in channel.outbound_materialization_groups().values() {
        let profile = group.profile();
        if !profile.accepts(ArtifactKind::EmptyDomain) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "Membership materialization profile must accept EmptyDomain",
            ));
        }
        let value_set = profile.accepts(ArtifactKind::ValueSet);
        let bitset = profile.accepts(ArtifactKind::Bitset) && bitset_schema_is_feasible(value_type);
        let bloom = profile.accepts(ArtifactKind::Bloom);
        if !value_set && !bitset && !bloom || profile.accepts(ArtifactKind::Range) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "Membership materialization profile has no feasible membership representation",
            ));
        }
        if matches!(
            channel.logical_domain(),
            RuntimeFilterLogicalDomain::Membership {
                null_semantics:
                    crate::runtime_filter::model::contract::NullSemantics::NullSafeEqual,
                ..
            }
        ) && !value_set
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "NullSafeEqual Membership materialization profile must accept ValueSet",
            ));
        }
        if bloom {
            let expected = BloomHashContract::new(&schema, channel.materialization_policy())
                .map_err(|_| {
                    install_error(
                        InstallContractErrorKind::InvalidPolicy,
                        "materialization Bloom policy is not supported",
                    )
                })?
                .digest();
            if profile.bloom_hash_contract() != Some(expected) {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "Bloom materialization profile does not match channel schema and policy",
                ));
            }
        }
        validate_profile_identity(profile, profile_encodings)?;
        materialization_profiles.insert((profile.id(), profile.canonical_bytes()));
    }
    validate_materialization_concurrency(
        channel,
        !channel.outbound_materialization_groups().is_empty(),
        materialization_profiles.len(),
    )
}

fn validate_membership_consumer<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    consumer: &'a crate::runtime_filter::port::install::ConsumerDeployment,
    schema: &ArtifactMembershipSchema,
    fenced_final: bool,
    unique_profiles: &mut BTreeSet<(ConsumerProfileId, &'a [u8])>,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
) -> Result<(), InstallContractError> {
    if consumer.expected_fragment_instances().is_empty() {
        return Err(install_error(
            InstallContractErrorKind::EmptyExpectedInstances,
            "consumer expected fragment instance set must be non-empty",
        ));
    }
    let capabilities = consumer.capabilities();
    let profile = consumer.artifact_profile();
    unique_profiles.insert((profile.id(), profile.canonical_bytes()));
    if !capabilities.contains(&ArtifactCapability::Membership)
        || !capabilities.contains(&ArtifactCapability::EmptyDomain)
    {
        return Err(install_error(
            InstallContractErrorKind::MissingMembershipCapability,
            "M2 Membership consumers must declare Membership and EmptyDomain semantics",
        ));
    }
    if fenced_final
        && (capabilities
            != &BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ])
            || profile.accepted_kinds()
                != &BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]))
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "fenced-final consumers require exact Membership and EmptyDomain semantics",
        ));
    }
    if !profile.accepts(ArtifactKind::EmptyDomain) {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "M2 Membership profile must accept EmptyDomain",
        ));
    }
    let value_type = match channel.logical_domain() {
        RuntimeFilterLogicalDomain::Membership { value_type, .. } => value_type,
        RuntimeFilterLogicalDomain::OrderedBound(_) => unreachable!(),
    };
    let value_set = profile.accepts(ArtifactKind::ValueSet);
    let bitset = profile.accepts(ArtifactKind::Bitset) && bitset_schema_is_feasible(value_type);
    let bloom = profile.accepts(ArtifactKind::Bloom);
    if !value_set && !bitset && !bloom {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "M2 Membership profile has no statically feasible membership representation",
        ));
    }
    if matches!(
        channel.logical_domain(),
        RuntimeFilterLogicalDomain::Membership {
            null_semantics: crate::runtime_filter::model::contract::NullSemantics::NullSafeEqual,
            ..
        }
    ) && !value_set
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "NullSafeEqual Membership profile must accept ValueSet",
        ));
    }
    if profile.accepts(ArtifactKind::Range)
        && !capabilities.contains(&ArtifactCapability::OrderedRange)
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Range physical kind requires OrderedRange semantic capability",
        ));
    }
    if profile.accepted_kinds().iter().any(|kind| {
        matches!(
            kind,
            ArtifactKind::ValueSet | ArtifactKind::Bloom | ArtifactKind::Bitset
        )
    }) && !capabilities.contains(&ArtifactCapability::Membership)
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "membership physical kinds require Membership semantic capability",
        ));
    }
    validate_profile_identity(profile, profile_encodings)?;
    if profile.accepts(ArtifactKind::Bloom) {
        let expected = BloomHashContract::new(schema, channel.materialization_policy())
            .map_err(|_| {
                install_error(
                    InstallContractErrorKind::InvalidPolicy,
                    "materialization Bloom policy is not supported",
                )
            })?
            .digest();
        if profile.bloom_hash_contract() != Some(expected) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "Bloom profile hash contract does not match channel schema and policy",
            ));
        }
    }
    Ok(())
}

fn validate_ordered_channel<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
    role_requirements: (bool, bool),
) -> Result<(), InstallContractError> {
    let RuntimeFilterLogicalDomain::OrderedBound(plan) = channel.logical_domain() else {
        unreachable!("ordered validator is called only for ordered channels")
    };
    let contract = RuntimeOrderContract::try_from_plan(plan).map_err(|error| {
        install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            format!("ordered channel has an invalid order contract: {error:?}"),
        )
    })?;
    match channel.reduction_requirement() {
        ReductionRequirement::TightenOrderedBound => {
            if channel.lifecycle() != RuntimeFilterLifecycle::MonotonicUpdates
                || channel.allowed_contribution_kinds()
                    != &BTreeSet::from([
                        ContributionKind::OrderedBoundUpdate,
                        ContributionKind::ProducerClosed,
                    ])
                || channel.completion_requirement() != CompletionRequirement::ProducerClosed
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "channel does not match the MonotonicUpdates OrderedBound M3A matrix",
                ));
            }
        }
        ReductionRequirement::MergeTopKSummary(requirement) => {
            RuntimeTopKSummaryContract::try_from_plan(plan, requirement).map_err(|error| {
                install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!("ordered channel has an invalid top-k summary contract: {error:?}"),
                )
            })?;
            if !channel
                .availability_coverage()
                .is_canonically_equivalent_to(channel.terminal_coverage())
            {
                return Err(install_error(
                    InstallContractErrorKind::InvalidCoverage,
                    "top-k summary availability and terminal coverage must be canonically equivalent",
                ));
            }
            if channel.lifecycle() != RuntimeFilterLifecycle::MonotonicUpdates
                || channel.allowed_contribution_kinds()
                    != &BTreeSet::from([
                        ContributionKind::TopKSummary,
                        ContributionKind::ProducerClosed,
                    ])
                || channel.completion_requirement() != CompletionRequirement::ProducerClosed
                || !channel.availability_coverage().is_all_of_only()
                || !channel.terminal_coverage().is_all_of_only()
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "channel does not match the MonotonicUpdates OrderedBound TopKSummary M3B matrix",
                ));
            }
        }
        ReductionRequirement::SetUnion => {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered channel cannot use SetUnion reduction",
            ));
        }
    }
    validate_common_channel(channel, role_requirements)?;
    validate_producer_coverage(channel)?;

    let mut unique_profiles = BTreeSet::new();
    for consumer in channel.consumers().values() {
        if consumer.expected_fragment_instances().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "consumer expected fragment instance set must be non-empty",
            ));
        }
        if !matches!(
            consumer.activation(),
            ConsumerActivation::NonBlockingLive { .. }
        ) {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "ordered consumers must use NonBlockingLive activation",
            ));
        }
        if consumer.capabilities() != &BTreeSet::from([ArtifactCapability::OrderedRange]) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered consumers must declare exactly OrderedRange capability",
            ));
        }
        let profile = consumer.artifact_profile();
        if profile.accepted_kinds() != &BTreeSet::from([ArtifactKind::Range])
            || profile.order_contract_digest() != Some(contract.digest())
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered consumer profile must accept only Range with the channel order digest",
            ));
        }
        unique_profiles.insert((profile.id(), profile.canonical_bytes()));
        validate_profile_identity(profile, profile_encodings)?;
    }
    let mut materialization_profiles = BTreeSet::new();
    for group in channel.outbound_materialization_groups().values() {
        let profile = group.profile();
        if profile.accepted_kinds() != &BTreeSet::from([ArtifactKind::Range])
            || profile.order_contract_digest() != Some(contract.digest())
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered materialization profile must accept only Range with the channel order digest",
            ));
        }
        validate_profile_identity(profile, profile_encodings)?;
        materialization_profiles.insert((profile.id(), profile.canonical_bytes()));
    }
    validate_materialization_concurrency(
        channel,
        !channel.outbound_materialization_groups().is_empty(),
        materialization_profiles.len(),
    )
}

fn validate_common_channel(
    channel: &RuntimeFilterChannelDeployment,
    role_requirements: (bool, bool),
) -> Result<(), InstallContractError> {
    let (requires_producer, requires_consumer) = role_requirements;
    if channel.producers().is_empty() != !requires_producer
        || channel.consumers().is_empty() != !requires_consumer
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Core roles do not match the routing requirements",
        ));
    }
    validate_runtime_filter_policy(channel.policy()).map_err(|error| {
        install_error(
            InstallContractErrorKind::InvalidPolicy,
            format!("invalid runtime filter policy: {error:?}"),
        )
    })?;
    if channel.core_budget().max_reducer_bytes() == 0 {
        return Err(install_error(
            InstallContractErrorKind::InvalidBudget,
            "max reducer bytes must be non-zero",
        ));
    }
    let policy = channel.materialization_policy();
    usize::try_from(policy.max_total_retained_bytes()).map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization retained budget does not fit this platform",
        )
    })?;
    usize::try_from(policy.max_scratch_bytes_per_job()).map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization scratch budget does not fit this platform",
        )
    })?;
    policy.aggregate_scratch_bytes().map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization aggregate scratch budget overflows",
        )
    })?;
    Ok(())
}

fn validate_producer_coverage(
    channel: &RuntimeFilterChannelDeployment,
) -> Result<(), InstallContractError> {
    let mut witnesses = BTreeSet::new();
    for producer in channel.producers().values() {
        if !witnesses.insert(producer.coverage_witness_id()) {
            return Err(install_error(
                InstallContractErrorKind::DuplicateCoverageWitness,
                "producer witness identities must be unique within a channel",
            ));
        }
        if producer.expected_fragment_instances().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "producer expected fragment instance set must be non-empty",
            ));
        }
    }
    if !channel.producers().is_empty() {
        validate_coverage(channel.availability_coverage(), channel)?;
        validate_coverage(channel.terminal_coverage(), channel)?;
    } else {
        for coverage in [channel.availability_coverage(), channel.terminal_coverage()] {
            coverage.validate_shape().map_err(|error| {
                install_error(
                    InstallContractErrorKind::InvalidCoverage,
                    format!("invalid coverage shape: {error:?}"),
                )
            })?;
        }
    }
    Ok(())
}

fn validate_profile_identity<'a>(
    profile: &'a crate::runtime_filter::port::artifact::ConsumerArtifactProfile,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
) -> Result<(), InstallContractError> {
    if let Some(existing) = profile_encodings.insert(profile.id(), profile.canonical_bytes())
        && existing != profile.canonical_bytes()
    {
        return Err(install_error(
            InstallContractErrorKind::ConflictingDeployment,
            "consumer profile digest collision carried different canonical bytes",
        ));
    }
    Ok(())
}

fn validate_materialization_concurrency(
    channel: &RuntimeFilterChannelDeployment,
    owns_materialization: bool,
    unique_profiles: usize,
) -> Result<(), InstallContractError> {
    if owns_materialization
        && channel.materialization_policy().max_concurrent_jobs() > unique_profiles
    {
        return Err(install_error(
            InstallContractErrorKind::InvalidPolicy,
            "max concurrent materialization jobs exceeds normalized unique profile count",
        ));
    }
    Ok(())
}

fn bitset_schema_is_feasible(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Decimal128(1..=18, _)
    )
}

fn validate_coverage(
    coverage: &Coverage,
    channel: &RuntimeFilterChannelDeployment,
) -> Result<(), InstallContractError> {
    coverage.validate_shape().map_err(|error| {
        install_error(
            InstallContractErrorKind::InvalidCoverage,
            format!("invalid coverage shape: {error:?}"),
        )
    })?;
    let expected = channel
        .producers()
        .values()
        .map(|producer| producer.coverage_witness_id())
        .collect::<BTreeSet<_>>();
    let mut counts = BTreeMap::new();
    count_witnesses(coverage, &mut counts);
    if counts.keys().any(|witness| !expected.contains(witness)) {
        return Err(install_error(
            InstallContractErrorKind::UnknownCoverageWitness,
            "coverage references a witness without an installed producer",
        ));
    }
    if counts.values().any(|count| *count != 1) {
        return Err(install_error(
            InstallContractErrorKind::DuplicateCoverageWitness,
            "coverage must reference each producer witness exactly once",
        ));
    }
    if counts.keys().copied().collect::<BTreeSet<_>>() != expected {
        return Err(install_error(
            InstallContractErrorKind::UnknownCoverageWitness,
            "coverage must reference every installed producer witness",
        ));
    }
    Ok(())
}

fn count_witnesses(coverage: &Coverage, counts: &mut BTreeMap<CoverageWitnessId, usize>) {
    match coverage {
        Coverage::Leaf(witness) => *counts.entry(*witness).or_default() += 1,
        Coverage::AllOf(children) | Coverage::AnyOf(children) => {
            for child in children {
                count_witnesses(child, counts);
            }
        }
    }
}

fn install_error(
    kind: InstallContractErrorKind,
    detail: impl Into<String>,
) -> InstallContractError {
    InstallContractError::new(kind, detail)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, LateApplyGranularity, NullSemantics,
        ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
        RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::identity::RouteEdgeId;
    use crate::runtime_filter::port::install::{
        ConsumerDeployment, MaterializationPolicy, ProducerDeployment,
        RuntimeFilterChannelDeployment, RuntimeFilterCoreBudget,
    };

    use super::validate_channel_contract_for_test;

    fn ordinary_membership_channel(
        activation: ConsumerActivation,
    ) -> RuntimeFilterChannelDeployment {
        let witness = CoverageWitnessId::new(1);
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 3,
            },
            RuntimeFilterCoreBudget::new(4096),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(10),
                ProducerDeployment::new(witness, BTreeSet::from([UniqueId::new(1, 10)])),
            )]),
            BTreeMap::from([(
                BindingId::new(20),
                ConsumerDeployment::new(
                    activation,
                    BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    BTreeSet::from([RouteEdgeId::new(30)]),
                    BTreeSet::from([UniqueId::new(1, 20)]),
                ),
            )]),
        )
    }

    #[test]
    fn ordinary_membership_install_accepts_only_blocking_or_batch_live() {
        assert!(
            validate_channel_contract_for_test(&ordinary_membership_channel(
                ConsumerActivation::BlockingSnapshot,
            ))
            .is_ok()
        );
        assert!(
            validate_channel_contract_for_test(&ordinary_membership_channel(
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
            ))
            .is_ok()
        );

        for late_apply in [
            LateApplyGranularity::Row,
            LateApplyGranularity::RowGroup,
            LateApplyGranularity::Split,
            LateApplyGranularity::File,
        ] {
            assert!(
                validate_channel_contract_for_test(&ordinary_membership_channel(
                    ConsumerActivation::NonBlockingLive { late_apply },
                ))
                .is_err()
            );
        }
    }
}
