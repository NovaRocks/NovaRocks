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
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::proto::{common, filter};
use crate::protocol::common::error::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::deployment::install_validation::validate_participant_install;
use crate::runtime_filter::model::contract::{
    BindingId, ChannelId, CoverageWitnessId, RuntimeFilterLifecycle, RuntimeFilterPolicyRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::artifact::{
    ArtifactKind, ConsumerArtifactProfile, HashContractDigest,
};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::{
    ConsumerDeployment, MaterializationPolicy, OutboundMaterializationGroup,
    OutboundMaterializationOwner, ProducerDeployment, RuntimeFilterChannelDeployment,
    RuntimeFilterCoreBudget, RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
};
use crate::runtime_filter::port::ordered_bound::OrderContractDigest;
use crate::runtime_filter::port::routing::{
    RuntimeFilterChannelRoutingView, RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer,
    RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
};
use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;

use super::encode::plan::{
    encode_runtime_filter_activation, encode_runtime_filter_capability,
    encode_runtime_filter_completion, encode_runtime_filter_contribution_kind,
    encode_runtime_filter_logical_domain, encode_runtime_filter_reduction_requirement,
};
use super::runtime_filter_contract_codec::{
    decode_runtime_filter_activation, decode_runtime_filter_capability,
    decode_runtime_filter_completion, decode_runtime_filter_contribution_kind,
    decode_runtime_filter_logical_domain_and_reduction,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterQueryLifecycleOptions {
    pub(crate) delivery_expire: Duration,
    pub(crate) query_expire: Duration,
    pub(crate) transport_retry_interval: Duration,
    pub(crate) transport_max_attempts: u32,
    pub(crate) transport_deadline: Duration,
    pub(crate) transport_max_pending_entries: usize,
    pub(crate) transport_max_pending_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedRuntimeFilterParticipantInstall {
    pub(crate) query_id: UniqueId,
    pub(crate) lifecycle: RuntimeFilterQueryLifecycleOptions,
    pub(crate) install: RuntimeFilterParticipantInstall,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterDeploymentAbort {
    pub(crate) query_id: UniqueId,
    pub(crate) epoch: DeploymentEpoch,
}

type CodecResult<T> = Result<T, ProtocolError>;

fn codec_error(
    path: FieldPath,
    kind: ProtocolErrorKind,
    detail: impl Into<String>,
) -> ProtocolError {
    ProtocolError::new(ProtocolFamily::Native, path, kind, detail)
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    codec_error(path, ProtocolErrorKind::InvalidValue, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    codec_error(path, ProtocolErrorKind::MissingField, detail)
}

fn duplicate(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    codec_error(path, ProtocolErrorKind::DuplicateField, detail)
}

fn inconsistent(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    codec_error(path, ProtocolErrorKind::InconsistentFields, detail)
}

fn reject_zero(raw: u64, path: FieldPath, identity: &'static str) -> CodecResult<()> {
    if raw == 0 {
        Err(invalid(path, format!("{identity} must be nonzero")))
    } else {
        Ok(())
    }
}

fn encode_unique_id(value: UniqueId, path: FieldPath) -> CodecResult<common::UniqueId> {
    if value.high() == 0 && value.low() == 0 {
        return Err(invalid(path, "unique id must be nonzero"));
    }
    Ok(common::UniqueId {
        hi: value.high(),
        lo: value.low(),
    })
}

fn decode_unique_id(value: Option<&common::UniqueId>, path: FieldPath) -> CodecResult<UniqueId> {
    let value = value.ok_or_else(|| missing(path.clone(), "unique id is required"))?;
    let decoded = UniqueId::new(value.hi, value.lo);
    if decoded.high() == 0 && decoded.low() == 0 {
        return Err(invalid(path, "unique id must be nonzero"));
    }
    Ok(decoded)
}

fn duration_millis(duration: Duration, path: FieldPath) -> CodecResult<u64> {
    let millis = u64::try_from(duration.as_millis()).map_err(|_| {
        codec_error(
            path.clone(),
            ProtocolErrorKind::OutOfRange,
            "duration does not fit u64 milliseconds",
        )
    })?;
    reject_zero(millis, path, "duration")?;
    Ok(millis)
}

fn allocatable_usize(raw: u64, path: FieldPath, label: &'static str) -> CodecResult<usize> {
    let value = usize::try_from(raw).map_err(|_| {
        codec_error(
            path.clone(),
            ProtocolErrorKind::OutOfRange,
            format!("{label} does not fit usize"),
        )
    })?;
    if value > isize::MAX as usize {
        return Err(codec_error(
            path,
            ProtocolErrorKind::OutOfRange,
            format!("{label} exceeds the maximum allocatable size"),
        ));
    }
    Ok(value)
}

fn encode_allocatable_usize(
    value: usize,
    path: FieldPath,
    label: &'static str,
) -> CodecResult<u64> {
    if value > isize::MAX as usize {
        return Err(codec_error(
            path,
            ProtocolErrorKind::OutOfRange,
            format!("{label} exceeds the maximum allocatable size"),
        ));
    }
    u64::try_from(value).map_err(|_| {
        codec_error(
            path,
            ProtocolErrorKind::OutOfRange,
            format!("{label} does not fit u64"),
        )
    })
}

fn encode_lifecycle_options(
    options: RuntimeFilterQueryLifecycleOptions,
) -> CodecResult<filter::RuntimeFilterQueryLifecycleOptions> {
    let root = FieldPath::root("install_runtime_filter_deployment_request").field("lifecycle");
    reject_zero(
        u64::from(options.transport_max_attempts),
        root.clone().field("transport_max_attempts"),
        "transport max attempts",
    )?;
    let max_pending_entries = encode_allocatable_usize(
        options.transport_max_pending_entries,
        root.clone().field("transport_max_pending_entries"),
        "transport max pending entries",
    )?;
    reject_zero(
        max_pending_entries,
        root.clone().field("transport_max_pending_entries"),
        "transport max pending entries",
    )?;
    let max_pending_bytes = encode_allocatable_usize(
        options.transport_max_pending_bytes,
        root.clone().field("transport_max_pending_bytes"),
        "transport max pending bytes",
    )?;
    reject_zero(
        max_pending_bytes,
        root.clone().field("transport_max_pending_bytes"),
        "transport max pending bytes",
    )?;
    Ok(filter::RuntimeFilterQueryLifecycleOptions {
        delivery_expire_ms: duration_millis(
            options.delivery_expire,
            root.clone().field("delivery_expire_ms"),
        )?,
        query_expire_ms: duration_millis(
            options.query_expire,
            root.clone().field("query_expire_ms"),
        )?,
        transport_retry_interval_ms: duration_millis(
            options.transport_retry_interval,
            root.clone().field("transport_retry_interval_ms"),
        )?,
        transport_max_attempts: u64::from(options.transport_max_attempts),
        transport_deadline_ms: duration_millis(
            options.transport_deadline,
            root.clone().field("transport_deadline_ms"),
        )?,
        transport_max_pending_entries: max_pending_entries,
        transport_max_pending_bytes: max_pending_bytes,
    })
}

fn decode_lifecycle_options(
    options: Option<&filter::RuntimeFilterQueryLifecycleOptions>,
) -> CodecResult<RuntimeFilterQueryLifecycleOptions> {
    let root = FieldPath::root("install_runtime_filter_deployment_request").field("lifecycle");
    let options = options.ok_or_else(|| missing(root.clone(), "lifecycle options are required"))?;
    for (raw, field, label) in [
        (
            options.delivery_expire_ms,
            "delivery_expire_ms",
            "delivery expiry",
        ),
        (options.query_expire_ms, "query_expire_ms", "query expiry"),
        (
            options.transport_retry_interval_ms,
            "transport_retry_interval_ms",
            "transport retry interval",
        ),
        (
            options.transport_max_attempts,
            "transport_max_attempts",
            "transport max attempts",
        ),
        (
            options.transport_deadline_ms,
            "transport_deadline_ms",
            "transport deadline",
        ),
        (
            options.transport_max_pending_entries,
            "transport_max_pending_entries",
            "transport max pending entries",
        ),
        (
            options.transport_max_pending_bytes,
            "transport_max_pending_bytes",
            "transport max pending bytes",
        ),
    ] {
        reject_zero(raw, root.clone().field(field), label)?;
    }
    Ok(RuntimeFilterQueryLifecycleOptions {
        delivery_expire: Duration::from_millis(options.delivery_expire_ms),
        query_expire: Duration::from_millis(options.query_expire_ms),
        transport_retry_interval: Duration::from_millis(options.transport_retry_interval_ms),
        transport_max_attempts: u32::try_from(options.transport_max_attempts).map_err(|_| {
            codec_error(
                root.clone().field("transport_max_attempts"),
                ProtocolErrorKind::OutOfRange,
                "transport max attempts does not fit u32",
            )
        })?,
        transport_deadline: Duration::from_millis(options.transport_deadline_ms),
        transport_max_pending_entries: allocatable_usize(
            options.transport_max_pending_entries,
            root.clone().field("transport_max_pending_entries"),
            "transport max pending entries",
        )?,
        transport_max_pending_bytes: allocatable_usize(
            options.transport_max_pending_bytes,
            root.field("transport_max_pending_bytes"),
            "transport max pending bytes",
        )?,
    })
}

pub(crate) fn encode_participant_install(
    query_id: UniqueId,
    lifecycle: RuntimeFilterQueryLifecycleOptions,
    install: &RuntimeFilterParticipantInstall,
) -> CodecResult<filter::InstallRuntimeFilterDeploymentRequest> {
    let root = FieldPath::root("install_runtime_filter_deployment_request");
    reject_zero(
        install.epoch().get(),
        root.clone().field("deployment_epoch"),
        "deployment epoch",
    )?;
    reject_zero(
        u64::from(install.local_participant_id().get()),
        root.clone().field("participant_id"),
        "participant id",
    )?;
    if install.routing_shard().deployment_epoch() != install.epoch() {
        return Err(inconsistent(
            root.clone().field("install"),
            "core and routing deployment epochs differ",
        ));
    }
    if install.routing_shard().local_participant_id() != install.local_participant_id() {
        return Err(inconsistent(
            root.clone().field("install"),
            "core and routing participant identities differ",
        ));
    }
    validate_participant_install(install)
        .map_err(|error| invalid(root.clone().field("install"), error.to_string()))?;
    Ok(filter::InstallRuntimeFilterDeploymentRequest {
        query_id: Some(encode_unique_id(query_id, root.clone().field("query_id"))?),
        deployment_epoch: install.epoch().get(),
        participant_id: install.local_participant_id().get(),
        lifecycle: Some(encode_lifecycle_options(lifecycle)?),
        install: Some(encode_install(install)?),
    })
}

pub(crate) fn decode_participant_install(
    request: &filter::InstallRuntimeFilterDeploymentRequest,
) -> CodecResult<DecodedRuntimeFilterParticipantInstall> {
    let root = FieldPath::root("install_runtime_filter_deployment_request");
    let query_id = decode_unique_id(request.query_id.as_ref(), root.clone().field("query_id"))?;
    reject_zero(
        request.deployment_epoch,
        root.clone().field("deployment_epoch"),
        "deployment epoch",
    )?;
    let epoch = DeploymentEpoch::new(request.deployment_epoch);
    reject_zero(
        u64::from(request.participant_id),
        root.clone().field("participant_id"),
        "participant id",
    )?;
    let participant = RuntimeFilterParticipantId::new(request.participant_id);
    let lifecycle = decode_lifecycle_options(request.lifecycle.as_ref())?;
    let wire = request.install.as_ref().ok_or_else(|| {
        missing(
            root.clone().field("install"),
            "participant install is required",
        )
    })?;
    let install = decode_install(wire, epoch, participant, root.clone().field("install"))?;
    validate_participant_install(&install)
        .map_err(|error| invalid(root.field("install"), error.to_string()))?;
    Ok(DecodedRuntimeFilterParticipantInstall {
        query_id,
        lifecycle,
        install,
    })
}

pub(crate) fn encode_abort_runtime_filter_deployment(
    query_id: UniqueId,
    epoch: DeploymentEpoch,
) -> CodecResult<filter::AbortRuntimeFilterDeploymentRequest> {
    let root = FieldPath::root("abort_runtime_filter_deployment_request");
    reject_zero(
        epoch.get(),
        root.clone().field("deployment_epoch"),
        "deployment epoch",
    )?;
    Ok(filter::AbortRuntimeFilterDeploymentRequest {
        query_id: Some(encode_unique_id(query_id, root.field("query_id"))?),
        deployment_epoch: epoch.get(),
    })
}

pub(crate) fn decode_abort_runtime_filter_deployment(
    request: &filter::AbortRuntimeFilterDeploymentRequest,
) -> CodecResult<RuntimeFilterDeploymentAbort> {
    let root = FieldPath::root("abort_runtime_filter_deployment_request");
    reject_zero(
        request.deployment_epoch,
        root.clone().field("deployment_epoch"),
        "deployment epoch",
    )?;
    Ok(RuntimeFilterDeploymentAbort {
        query_id: decode_unique_id(request.query_id.as_ref(), root.field("query_id"))?,
        epoch: DeploymentEpoch::new(request.deployment_epoch),
    })
}

fn encode_install(
    install: &RuntimeFilterParticipantInstall,
) -> CodecResult<filter::RuntimeFilterParticipantInstall> {
    Ok(filter::RuntimeFilterParticipantInstall {
        core_channels: install
            .core_view()
            .channels()
            .values()
            .map(encode_core_channel)
            .collect::<CodecResult<Vec<_>>>()?,
        routing_channels: install
            .routing_shard()
            .channels()
            .values()
            .map(encode_routing_channel)
            .collect::<CodecResult<Vec<_>>>()?,
    })
}

fn decode_install(
    wire: &filter::RuntimeFilterParticipantInstall,
    epoch: DeploymentEpoch,
    participant: RuntimeFilterParticipantId,
    path: FieldPath,
) -> CodecResult<RuntimeFilterParticipantInstall> {
    let mut core_channels = BTreeMap::new();
    let mut binding_ids = BTreeSet::new();
    let mut consumer_route_ids = BTreeSet::new();
    for (index, channel) in wire.core_channels.iter().enumerate() {
        let item_path = path.clone().field("core_channels").index(index);
        let decoded = decode_core_channel(channel, item_path.clone())?;
        if core_channels
            .insert(decoded.channel_id(), decoded)
            .is_some()
        {
            return Err(duplicate(
                item_path.field("channel_id"),
                "duplicate core channel id",
            ));
        }
        let decoded = core_channels
            .get(&ChannelId::new(channel.channel_id))
            .expect("inserted core channel");
        for binding in decoded.producers().keys().chain(decoded.consumers().keys()) {
            if !binding_ids.insert(*binding) {
                return Err(duplicate(
                    item_path.clone(),
                    "duplicate producer or consumer binding id across core install",
                ));
            }
        }
        for route in decoded
            .consumers()
            .values()
            .flat_map(|consumer| consumer.route_edge_ids())
        {
            if !consumer_route_ids.insert(*route) {
                return Err(duplicate(
                    item_path.clone(),
                    "duplicate consumer route edge id across core install",
                ));
            }
        }
    }
    let mut routing_channels = BTreeMap::new();
    for (index, channel) in wire.routing_channels.iter().enumerate() {
        let item_path = path.clone().field("routing_channels").index(index);
        let decoded = decode_routing_channel(channel, participant, item_path.clone())?;
        if routing_channels
            .insert(decoded.channel_id(), decoded)
            .is_some()
        {
            return Err(duplicate(
                item_path.field("channel_id"),
                "duplicate routing channel id",
            ));
        }
    }
    let core = RuntimeFilterInstallView::new(epoch, participant, core_channels);
    let routing = RuntimeFilterRoutingShard::new(epoch, participant, routing_channels)
        .map_err(|error| invalid(path, error.to_string()))?;
    Ok(RuntimeFilterParticipantInstall::new(core, routing))
}

fn encode_core_channel(
    channel: &RuntimeFilterChannelDeployment,
) -> CodecResult<filter::RuntimeFilterChannelDeployment> {
    reject_zero(
        u64::from(channel.channel_id().get()),
        FieldPath::root("runtime_filter_install").field("core_channel_id"),
        "channel id",
    )?;
    let (value_type, contract) = encode_runtime_filter_logical_domain(channel.logical_domain())
        .map_err(|error| {
            invalid(
                FieldPath::root("runtime_filter_install").field("logical_domain"),
                error,
            )
        })?;
    Ok(filter::RuntimeFilterChannelDeployment {
        channel_id: channel.channel_id().get(),
        logical_domain: Some(filter::RuntimeFilterLogicalDomain {
            value_type: Some(value_type),
            contract: Some(contract),
        }),
        lifecycle: match channel.lifecycle() {
            RuntimeFilterLifecycle::CompleteOnce => {
                filter::RuntimeFilterLifecycle::CompleteOnce as i32
            }
            RuntimeFilterLifecycle::MonotonicUpdates => {
                filter::RuntimeFilterLifecycle::MonotonicUpdates as i32
            }
        },
        availability_coverage: Some(encode_coverage(channel.availability_coverage())?),
        terminal_coverage: Some(encode_coverage(channel.terminal_coverage())?),
        reduction: Some(
            encode_runtime_filter_reduction_requirement(
                channel.logical_domain(),
                channel.reduction_requirement(),
            )
            .map_err(|error| {
                invalid(
                    FieldPath::root("runtime_filter_install").field("reduction"),
                    error,
                )
            })?,
        ),
        allowed_contribution_kinds: channel
            .allowed_contribution_kinds()
            .iter()
            .copied()
            .map(encode_runtime_filter_contribution_kind)
            .collect(),
        completion_requirement: encode_runtime_filter_completion(channel.completion_requirement()),
        policy: Some(filter::RuntimeFilterPolicyRequirement {
            max_contribution_bytes: channel.policy().max_contribution_bytes,
            max_artifact_bytes: channel.policy().max_artifact_bytes,
            deadline_ms: channel.policy().deadline_ms,
            max_retries: channel.policy().max_retries,
        }),
        core_budget: Some(filter::RuntimeFilterCoreBudget {
            max_reducer_bytes: channel.core_budget().max_reducer_bytes(),
        }),
        materialization_policy: Some(encode_materialization_policy(
            channel.materialization_policy(),
        )?),
        producers: channel
            .producers()
            .iter()
            .map(|(binding, producer)| encode_producer(*binding, producer))
            .collect::<CodecResult<Vec<_>>>()?,
        consumers: channel
            .consumers()
            .iter()
            .map(|(binding, consumer)| encode_consumer(*binding, consumer))
            .collect::<CodecResult<Vec<_>>>()?,
        outbound_materialization_groups: channel
            .outbound_materialization_groups()
            .values()
            .map(encode_outbound_materialization_group)
            .collect::<CodecResult<Vec<_>>>()?,
    })
}

fn decode_core_channel(
    wire: &filter::RuntimeFilterChannelDeployment,
    path: FieldPath,
) -> CodecResult<RuntimeFilterChannelDeployment> {
    reject_zero(
        u64::from(wire.channel_id),
        path.clone().field("channel_id"),
        "channel id",
    )?;
    let logical = wire.logical_domain.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("logical_domain"),
            "logical domain is required",
        )
    })?;
    let (domain, reduction) = decode_runtime_filter_logical_domain_and_reduction(
        logical.value_type.as_ref(),
        logical.contract.as_ref(),
        wire.reduction.as_ref(),
        path.clone(),
    )?;
    let lifecycle = match filter::RuntimeFilterLifecycle::try_from(wire.lifecycle) {
        Ok(filter::RuntimeFilterLifecycle::CompleteOnce) => RuntimeFilterLifecycle::CompleteOnce,
        Ok(filter::RuntimeFilterLifecycle::MonotonicUpdates) => {
            RuntimeFilterLifecycle::MonotonicUpdates
        }
        Ok(filter::RuntimeFilterLifecycle::Unspecified) | Err(_) => {
            return Err(codec_error(
                path.clone().field("lifecycle"),
                ProtocolErrorKind::InvalidEnum,
                format!("invalid runtime filter lifecycle={}", wire.lifecycle),
            ));
        }
    };
    let availability = decode_coverage(
        wire.availability_coverage.as_ref(),
        path.clone().field("availability_coverage"),
    )?;
    let terminal = decode_coverage(
        wire.terminal_coverage.as_ref(),
        path.clone().field("terminal_coverage"),
    )?;
    let mut contributions = BTreeSet::new();
    for (index, raw) in wire.allowed_contribution_kinds.iter().copied().enumerate() {
        let item_path = path
            .clone()
            .field("allowed_contribution_kinds")
            .index(index);
        let contribution = decode_runtime_filter_contribution_kind(raw, item_path.clone())?;
        if !contributions.insert(contribution) {
            return Err(duplicate(item_path, "duplicate contribution kind"));
        }
    }
    if contributions.is_empty() {
        return Err(invalid(
            path.clone().field("allowed_contribution_kinds"),
            "allowed contribution kinds must be nonempty",
        ));
    }
    let completion = decode_runtime_filter_completion(
        wire.completion_requirement,
        path.clone().field("completion_requirement"),
    )?;
    let policy_wire = wire
        .policy
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("policy"), "policy is required"))?;
    let policy = RuntimeFilterPolicyRequirement {
        max_contribution_bytes: policy_wire.max_contribution_bytes,
        max_artifact_bytes: policy_wire.max_artifact_bytes,
        deadline_ms: policy_wire.deadline_ms,
        max_retries: policy_wire.max_retries,
    };
    let budget = wire
        .core_budget
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("core_budget"), "core budget is required"))?;
    reject_zero(
        budget.max_reducer_bytes,
        path.clone().field("core_budget").field("max_reducer_bytes"),
        "core reducer budget",
    )?;
    let materialization = decode_materialization_policy(
        wire.materialization_policy.as_ref(),
        path.clone().field("materialization_policy"),
    )?;
    let mut producers = BTreeMap::new();
    for (index, producer) in wire.producers.iter().enumerate() {
        let item_path = path.clone().field("producers").index(index);
        let (binding, deployment) = decode_producer(producer, item_path.clone())?;
        if producers.insert(binding, deployment).is_some() {
            return Err(duplicate(
                item_path.field("binding_id"),
                "duplicate producer binding",
            ));
        }
    }
    let mut consumers = BTreeMap::new();
    for (index, consumer) in wire.consumers.iter().enumerate() {
        let item_path = path.clone().field("consumers").index(index);
        let (binding, deployment) = decode_consumer(consumer, item_path.clone())?;
        if consumers.insert(binding, deployment).is_some() {
            return Err(duplicate(
                item_path.field("binding_id"),
                "duplicate consumer binding",
            ));
        }
    }
    let mut groups = BTreeMap::new();
    for (index, group) in wire.outbound_materialization_groups.iter().enumerate() {
        let item_path = path
            .clone()
            .field("outbound_materialization_groups")
            .index(index);
        let group = decode_outbound_materialization_group(group, item_path.clone())?;
        if groups.insert(group.profile().id(), group).is_some() {
            return Err(duplicate(
                item_path,
                "duplicate outbound materialization profile",
            ));
        }
    }
    Ok(RuntimeFilterChannelDeployment::new(
        ChannelId::new(wire.channel_id),
        domain,
        lifecycle,
        availability,
        terminal,
        reduction,
        contributions,
        completion,
        policy,
        RuntimeFilterCoreBudget::new(budget.max_reducer_bytes),
        materialization,
        producers,
        consumers,
    )
    .with_outbound_materialization_groups(groups))
}

fn encode_outbound_materialization_group(
    group: &OutboundMaterializationGroup,
) -> CodecResult<filter::RuntimeFilterOutboundMaterializationGroup> {
    if group.route_edge_ids().is_empty() {
        return Err(invalid(
            FieldPath::root("runtime_filter_install").field("outbound_materialization_groups"),
            "outbound materialization route set must be nonempty",
        ));
    }
    Ok(filter::RuntimeFilterOutboundMaterializationGroup {
        owner: match group.owner() {
            OutboundMaterializationOwner::DirectSource => {
                filter::RuntimeFilterOutboundMaterializationOwner::DirectSource as i32
            }
            OutboundMaterializationOwner::Aggregator => {
                filter::RuntimeFilterOutboundMaterializationOwner::Aggregator as i32
            }
        },
        artifact_profile: Some(encode_artifact_profile(group.profile())?),
        route_edge_ids: group
            .route_edge_ids()
            .iter()
            .map(|route| route.get())
            .collect(),
    })
}

fn decode_outbound_materialization_group(
    wire: &filter::RuntimeFilterOutboundMaterializationGroup,
    path: FieldPath,
) -> CodecResult<OutboundMaterializationGroup> {
    let owner = match filter::RuntimeFilterOutboundMaterializationOwner::try_from(wire.owner) {
        Ok(filter::RuntimeFilterOutboundMaterializationOwner::DirectSource) => {
            OutboundMaterializationOwner::DirectSource
        }
        Ok(filter::RuntimeFilterOutboundMaterializationOwner::Aggregator) => {
            OutboundMaterializationOwner::Aggregator
        }
        Ok(filter::RuntimeFilterOutboundMaterializationOwner::Unspecified) | Err(_) => {
            return Err(codec_error(
                path.clone().field("owner"),
                ProtocolErrorKind::InvalidEnum,
                format!("invalid outbound materialization owner={}", wire.owner),
            ));
        }
    };
    let profile = decode_artifact_profile(
        wire.artifact_profile.as_ref(),
        path.clone().field("artifact_profile"),
    )?;
    let mut routes = BTreeSet::new();
    for (index, raw) in wire.route_edge_ids.iter().copied().enumerate() {
        let item_path = path.clone().field("route_edge_ids").index(index);
        reject_zero(u64::from(raw), item_path.clone(), "route edge id")?;
        if !routes.insert(RouteEdgeId::new(raw)) {
            return Err(duplicate(
                item_path,
                "duplicate outbound materialization route edge id",
            ));
        }
    }
    if routes.is_empty() {
        return Err(invalid(
            path.field("route_edge_ids"),
            "outbound materialization route edge ids must be nonempty",
        ));
    }
    Ok(OutboundMaterializationGroup::new(owner, profile, routes))
}

fn encode_coverage(coverage: &Coverage) -> CodecResult<filter::RuntimeFilterCoverage> {
    use filter::runtime_filter_coverage::Kind;
    let kind = match coverage {
        Coverage::Leaf(witness) => {
            reject_zero(
                u64::from(witness.get()),
                FieldPath::root("runtime_filter_install").field("coverage_witness_id"),
                "coverage witness id",
            )?;
            Kind::LeafWitnessId(witness.get())
        }
        Coverage::AllOf(children) => Kind::AllOf(filter::RuntimeFilterCoverageAllOf {
            children: children
                .iter()
                .map(encode_coverage)
                .collect::<CodecResult<Vec<_>>>()?,
        }),
        Coverage::AnyOf(children) => Kind::AnyOf(filter::RuntimeFilterCoverageAnyOf {
            children: children
                .iter()
                .map(encode_coverage)
                .collect::<CodecResult<Vec<_>>>()?,
        }),
    };
    coverage.validate_shape().map_err(|error| {
        invalid(
            FieldPath::root("runtime_filter_install").field("coverage"),
            format!("invalid coverage: {error:?}"),
        )
    })?;
    Ok(filter::RuntimeFilterCoverage { kind: Some(kind) })
}

fn decode_coverage(
    wire: Option<&filter::RuntimeFilterCoverage>,
    path: FieldPath,
) -> CodecResult<Coverage> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "coverage is required"))?;
    let coverage = match wire
        .kind
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("kind"), "coverage kind is required"))?
    {
        filter::runtime_filter_coverage::Kind::LeafWitnessId(raw) => {
            reject_zero(
                u64::from(*raw),
                path.clone().field("leaf_witness_id"),
                "coverage witness id",
            )?;
            Coverage::Leaf(CoverageWitnessId::new(*raw))
        }
        filter::runtime_filter_coverage::Kind::AllOf(all) => Coverage::AllOf(
            all.children
                .iter()
                .enumerate()
                .map(|(index, child)| {
                    decode_coverage(
                        Some(child),
                        path.clone().field("all_of").field("children").index(index),
                    )
                })
                .collect::<CodecResult<Vec<_>>>()?,
        ),
        filter::runtime_filter_coverage::Kind::AnyOf(any) => Coverage::AnyOf(
            any.children
                .iter()
                .enumerate()
                .map(|(index, child)| {
                    decode_coverage(
                        Some(child),
                        path.clone().field("any_of").field("children").index(index),
                    )
                })
                .collect::<CodecResult<Vec<_>>>()?,
        ),
    };
    coverage
        .validate_shape()
        .map_err(|error| invalid(path, format!("invalid coverage: {error:?}")))?;
    Ok(coverage)
}

fn encode_materialization_policy(
    policy: MaterializationPolicy,
) -> CodecResult<filter::RuntimeFilterMaterializationPolicy> {
    Ok(filter::RuntimeFilterMaterializationPolicy {
        bloom_bits_per_key: policy.bloom_bits_per_key(),
        bloom_hash_count: policy.bloom_hash_count(),
        bloom_seed: policy.bloom_seed(),
        bloom_algorithm_version: u32::from(policy.bloom_algorithm_version()),
        max_total_retained_bytes: policy.max_total_retained_bytes(),
        max_scratch_bytes_per_job: policy.max_scratch_bytes_per_job(),
        max_concurrent_jobs: encode_allocatable_usize(
            policy.max_concurrent_jobs(),
            FieldPath::root("runtime_filter_install")
                .field("materialization_policy")
                .field("max_concurrent_jobs"),
            "max concurrent jobs",
        )?,
    })
}

fn decode_materialization_policy(
    wire: Option<&filter::RuntimeFilterMaterializationPolicy>,
    path: FieldPath,
) -> CodecResult<MaterializationPolicy> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "materialization policy is required"))?;
    let version = u16::try_from(wire.bloom_algorithm_version).map_err(|_| {
        codec_error(
            path.clone().field("bloom_algorithm_version"),
            ProtocolErrorKind::OutOfRange,
            "bloom algorithm version does not fit u16",
        )
    })?;
    let jobs = allocatable_usize(
        wire.max_concurrent_jobs,
        path.clone().field("max_concurrent_jobs"),
        "max concurrent jobs",
    )?;
    MaterializationPolicy::new(
        wire.bloom_bits_per_key,
        wire.bloom_hash_count,
        wire.bloom_seed,
        version,
        wire.max_total_retained_bytes,
        wire.max_scratch_bytes_per_job,
        jobs,
    )
    .map_err(|error| invalid(path, format!("invalid materialization policy: {error:?}")))
}

fn encode_producer(
    binding: BindingId,
    producer: &ProducerDeployment,
) -> CodecResult<filter::RuntimeFilterProducerDeployment> {
    reject_zero(
        u64::from(binding.get()),
        FieldPath::root("runtime_filter_install").field("producer_binding_id"),
        "producer binding id",
    )?;
    reject_zero(
        u64::from(producer.coverage_witness_id().get()),
        FieldPath::root("runtime_filter_install").field("coverage_witness_id"),
        "coverage witness id",
    )?;
    if producer.expected_fragment_instances().is_empty() {
        return Err(invalid(
            FieldPath::root("runtime_filter_install").field("expected_fragment_instances"),
            "producer expected fragment instances must be nonempty",
        ));
    }
    Ok(filter::RuntimeFilterProducerDeployment {
        binding_id: binding.get(),
        coverage_witness_id: producer.coverage_witness_id().get(),
        expected_fragment_instances: producer
            .expected_fragment_instances()
            .iter()
            .copied()
            .map(|id| {
                encode_unique_id(
                    id,
                    FieldPath::root("runtime_filter_install").field("fragment_instance_id"),
                )
            })
            .collect::<CodecResult<Vec<_>>>()?,
    })
}

fn decode_producer(
    wire: &filter::RuntimeFilterProducerDeployment,
    path: FieldPath,
) -> CodecResult<(BindingId, ProducerDeployment)> {
    reject_zero(
        u64::from(wire.binding_id),
        path.clone().field("binding_id"),
        "producer binding id",
    )?;
    reject_zero(
        u64::from(wire.coverage_witness_id),
        path.clone().field("coverage_witness_id"),
        "coverage witness id",
    )?;
    let instances = decode_unique_id_set(
        &wire.expected_fragment_instances,
        path.field("expected_fragment_instances"),
    )?;
    Ok((
        BindingId::new(wire.binding_id),
        ProducerDeployment::new(CoverageWitnessId::new(wire.coverage_witness_id), instances),
    ))
}

fn encode_consumer(
    binding: BindingId,
    consumer: &ConsumerDeployment,
) -> CodecResult<filter::RuntimeFilterConsumerDeployment> {
    reject_zero(
        u64::from(binding.get()),
        FieldPath::root("runtime_filter_install").field("consumer_binding_id"),
        "consumer binding id",
    )?;
    if consumer.route_edge_ids().is_empty() || consumer.expected_fragment_instances().is_empty() {
        return Err(invalid(
            FieldPath::root("runtime_filter_install").field("consumer"),
            "consumer routes and expected fragment instances must be nonempty",
        ));
    }
    Ok(filter::RuntimeFilterConsumerDeployment {
        binding_id: binding.get(),
        activation: Some(encode_runtime_filter_activation(consumer.activation())),
        capabilities: consumer
            .capabilities()
            .iter()
            .copied()
            .map(encode_runtime_filter_capability)
            .collect(),
        artifact_profile: Some(encode_artifact_profile(consumer.artifact_profile())?),
        route_edge_ids: consumer
            .route_edge_ids()
            .iter()
            .map(|id| id.get())
            .collect(),
        expected_fragment_instances: consumer
            .expected_fragment_instances()
            .iter()
            .copied()
            .map(|id| {
                encode_unique_id(
                    id,
                    FieldPath::root("runtime_filter_install").field("fragment_instance_id"),
                )
            })
            .collect::<CodecResult<Vec<_>>>()?,
    })
}

fn decode_consumer(
    wire: &filter::RuntimeFilterConsumerDeployment,
    path: FieldPath,
) -> CodecResult<(BindingId, ConsumerDeployment)> {
    reject_zero(
        u64::from(wire.binding_id),
        path.clone().field("binding_id"),
        "consumer binding id",
    )?;
    let activation = decode_runtime_filter_activation(
        wire.activation.as_ref(),
        path.clone().field("activation"),
    )?;
    let mut capabilities = BTreeSet::new();
    for (index, raw) in wire.capabilities.iter().copied().enumerate() {
        let item_path = path.clone().field("capabilities").index(index);
        let capability = decode_runtime_filter_capability(raw, item_path.clone())?;
        if !capabilities.insert(capability) {
            return Err(duplicate(item_path, "duplicate consumer capability"));
        }
    }
    if capabilities.is_empty() {
        return Err(invalid(
            path.clone().field("capabilities"),
            "consumer capabilities must be nonempty",
        ));
    }
    let profile = decode_artifact_profile(
        wire.artifact_profile.as_ref(),
        path.clone().field("artifact_profile"),
    )?;
    let mut routes = BTreeSet::new();
    for (index, raw) in wire.route_edge_ids.iter().copied().enumerate() {
        let item_path = path.clone().field("route_edge_ids").index(index);
        reject_zero(u64::from(raw), item_path.clone(), "route edge id")?;
        if !routes.insert(RouteEdgeId::new(raw)) {
            return Err(duplicate(item_path, "duplicate consumer route edge id"));
        }
    }
    if routes.is_empty() {
        return Err(invalid(
            path.clone().field("route_edge_ids"),
            "consumer route edge ids must be nonempty",
        ));
    }
    let instances = decode_unique_id_set(
        &wire.expected_fragment_instances,
        path.clone().field("expected_fragment_instances"),
    )?;
    Ok((
        BindingId::new(wire.binding_id),
        ConsumerDeployment::with_profile(activation, capabilities, profile, routes, instances),
    ))
}

fn decode_unique_id_set(
    wire: &[common::UniqueId],
    path: FieldPath,
) -> CodecResult<BTreeSet<UniqueId>> {
    let mut values = BTreeSet::new();
    for (index, item) in wire.iter().enumerate() {
        let item_path = path.clone().index(index);
        let value = decode_unique_id(Some(item), item_path.clone())?;
        if !values.insert(value) {
            return Err(duplicate(item_path, "duplicate unique id"));
        }
    }
    if values.is_empty() {
        return Err(invalid(path, "unique id set must be nonempty"));
    }
    Ok(values)
}

fn encode_artifact_profile(
    profile: &ConsumerArtifactProfile,
) -> CodecResult<filter::RuntimeFilterConsumerArtifactProfile> {
    Ok(filter::RuntimeFilterConsumerArtifactProfile {
        accepted_kinds: profile
            .accepted_kinds()
            .iter()
            .copied()
            .map(encode_artifact_kind)
            .collect(),
        bloom_hash_contract: profile
            .bloom_hash_contract()
            .map(|digest| digest.bytes().to_vec()),
        order_contract_digest: profile
            .order_contract_digest()
            .map(|digest| digest.bytes().to_vec()),
        profile_id: profile.id().bytes().to_vec(),
    })
}

fn decode_artifact_profile(
    wire: Option<&filter::RuntimeFilterConsumerArtifactProfile>,
    path: FieldPath,
) -> CodecResult<ConsumerArtifactProfile> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "artifact profile is required"))?;
    let mut kinds = BTreeSet::new();
    for (index, raw) in wire.accepted_kinds.iter().copied().enumerate() {
        let item_path = path.clone().field("accepted_kinds").index(index);
        let kind = decode_artifact_kind(raw, item_path.clone())?;
        if !kinds.insert(kind) {
            return Err(duplicate(item_path, "duplicate artifact kind"));
        }
    }
    let bloom = wire
        .bloom_hash_contract
        .as_deref()
        .map(|bytes| digest32(bytes, path.clone().field("bloom_hash_contract")))
        .transpose()?
        .map(HashContractDigest::new);
    let order = wire
        .order_contract_digest
        .as_deref()
        .map(|bytes| digest32(bytes, path.clone().field("order_contract_digest")))
        .transpose()?
        .map(OrderContractDigest::from_bytes_for_codec);
    let profile = match order {
        Some(order) => {
            if kinds != BTreeSet::from([ArtifactKind::Range]) || bloom.is_some() {
                return Err(invalid(
                    path.clone(),
                    "ordered artifact profile must contain only Range and no bloom digest",
                ));
            }
            ConsumerArtifactProfile::new_ordered_range(order)
        }
        None => ConsumerArtifactProfile::new(kinds, bloom),
    }
    .map_err(|error| invalid(path.clone(), format!("invalid artifact profile: {error:?}")))?;
    let profile_id = digest32(&wire.profile_id, path.clone().field("profile_id"))?;
    if profile.id().bytes() != profile_id {
        return Err(inconsistent(
            path.field("profile_id"),
            "artifact profile id does not match typed profile",
        ));
    }
    Ok(profile)
}

fn digest32(bytes: &[u8], path: FieldPath) -> CodecResult<[u8; 32]> {
    bytes.try_into().map_err(|_| {
        invalid(
            path,
            format!("digest must be exactly 32 bytes, got {}", bytes.len()),
        )
    })
}

fn encode_artifact_kind(kind: ArtifactKind) -> i32 {
    match kind {
        ArtifactKind::ValueSet => filter::RuntimeFilterArtifactKind::ValueSet as i32,
        ArtifactKind::Bloom => filter::RuntimeFilterArtifactKind::Bloom as i32,
        ArtifactKind::Bitset => filter::RuntimeFilterArtifactKind::Bitset as i32,
        ArtifactKind::Range => filter::RuntimeFilterArtifactKind::Range as i32,
        ArtifactKind::EmptyDomain => filter::RuntimeFilterArtifactKind::EmptyDomain as i32,
    }
}

fn decode_artifact_kind(raw: i32, path: FieldPath) -> CodecResult<ArtifactKind> {
    match filter::RuntimeFilterArtifactKind::try_from(raw) {
        Ok(filter::RuntimeFilterArtifactKind::ValueSet) => Ok(ArtifactKind::ValueSet),
        Ok(filter::RuntimeFilterArtifactKind::Bloom) => Ok(ArtifactKind::Bloom),
        Ok(filter::RuntimeFilterArtifactKind::Bitset) => Ok(ArtifactKind::Bitset),
        Ok(filter::RuntimeFilterArtifactKind::Range) => Ok(ArtifactKind::Range),
        Ok(filter::RuntimeFilterArtifactKind::EmptyDomain) => Ok(ArtifactKind::EmptyDomain),
        Ok(filter::RuntimeFilterArtifactKind::Unspecified) | Err(_) => Err(codec_error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("invalid runtime filter artifact kind={raw}"),
        )),
    }
}

fn encode_routing_channel(
    channel: &RuntimeFilterChannelRoutingView,
) -> CodecResult<filter::RuntimeFilterChannelRoutingView> {
    reject_zero(
        u64::from(channel.channel_id().get()),
        FieldPath::root("runtime_filter_install").field("routing_channel_id"),
        "channel id",
    )?;
    Ok(filter::RuntimeFilterChannelRoutingView {
        channel_id: channel.channel_id().get(),
        local_roles: channel
            .local_roles()
            .iter()
            .copied()
            .map(encode_route_role)
            .collect::<CodecResult<Vec<_>>>()?,
        producer_instances: channel
            .producer_instances()
            .iter()
            .map(|((binding, instance), participant)| {
                reject_zero(
                    u64::from(binding.get()),
                    FieldPath::root("runtime_filter_install").field("producer_binding_id"),
                    "producer binding id",
                )?;
                reject_zero(
                    u64::from(participant.get()),
                    FieldPath::root("runtime_filter_install").field("producer_participant_id"),
                    "producer participant id",
                )?;
                Ok(filter::RuntimeFilterProducerInstanceRoute {
                    binding_id: binding.get(),
                    fragment_instance_id: Some(encode_unique_id(
                        *instance,
                        FieldPath::root("runtime_filter_install").field("fragment_instance_id"),
                    )?),
                    participant_id: participant.get(),
                })
            })
            .collect::<CodecResult<Vec<_>>>()?,
        inbound_edges: channel
            .inbound_edges()
            .iter()
            .map(encode_routing_edge)
            .collect::<CodecResult<Vec<_>>>()?,
        outbound_edges: channel
            .outbound_edges()
            .iter()
            .map(encode_routing_edge)
            .collect::<CodecResult<Vec<_>>>()?,
    })
}

fn decode_routing_channel(
    wire: &filter::RuntimeFilterChannelRoutingView,
    local_participant: RuntimeFilterParticipantId,
    path: FieldPath,
) -> CodecResult<RuntimeFilterChannelRoutingView> {
    reject_zero(
        u64::from(wire.channel_id),
        path.clone().field("channel_id"),
        "channel id",
    )?;
    let mut roles = BTreeSet::new();
    for (index, role) in wire.local_roles.iter().enumerate() {
        let item_path = path.clone().field("local_roles").index(index);
        let role = decode_route_role(role, item_path.clone())?;
        if !roles.insert(role) {
            return Err(duplicate(item_path, "duplicate local route role"));
        }
    }
    if roles.is_empty() {
        return Err(invalid(
            path.clone().field("local_roles"),
            "local route roles must be nonempty",
        ));
    }
    let mut producer_instances = BTreeMap::new();
    for (index, route) in wire.producer_instances.iter().enumerate() {
        let item_path = path.clone().field("producer_instances").index(index);
        reject_zero(
            u64::from(route.binding_id),
            item_path.clone().field("binding_id"),
            "producer binding id",
        )?;
        let instance = decode_unique_id(
            route.fragment_instance_id.as_ref(),
            item_path.clone().field("fragment_instance_id"),
        )?;
        reject_zero(
            u64::from(route.participant_id),
            item_path.clone().field("participant_id"),
            "producer participant id",
        )?;
        if producer_instances
            .insert(
                (BindingId::new(route.binding_id), instance),
                RuntimeFilterParticipantId::new(route.participant_id),
            )
            .is_some()
        {
            return Err(duplicate(item_path, "duplicate producer instance route"));
        }
    }
    let inbound = wire
        .inbound_edges
        .iter()
        .enumerate()
        .map(|(index, edge)| {
            decode_routing_edge(
                edge,
                ChannelId::new(wire.channel_id),
                path.clone().field("inbound_edges").index(index),
            )
        })
        .collect::<CodecResult<Vec<_>>>()?;
    let outbound = wire
        .outbound_edges
        .iter()
        .enumerate()
        .map(|(index, edge)| {
            decode_routing_edge(
                edge,
                ChannelId::new(wire.channel_id),
                path.clone().field("outbound_edges").index(index),
            )
        })
        .collect::<CodecResult<Vec<_>>>()?;
    let channel = RuntimeFilterChannelRoutingView::new(
        ChannelId::new(wire.channel_id),
        roles,
        producer_instances,
        inbound,
        outbound,
    )
    .map_err(|error| invalid(path.clone(), error.to_string()))?;
    for edge in channel.inbound_edges() {
        if edge.target().participant_id() != local_participant {
            return Err(inconsistent(
                path.clone().field("inbound_edges"),
                "inbound edge target does not match request participant",
            ));
        }
    }
    for edge in channel.outbound_edges() {
        if edge.source().participant_id() != local_participant {
            return Err(inconsistent(
                path.clone().field("outbound_edges"),
                "outbound edge source does not match request participant",
            ));
        }
    }
    Ok(channel)
}

fn encode_route_role(role: RuntimeFilterRouteRole) -> CodecResult<filter::RuntimeFilterRouteRole> {
    use filter::runtime_filter_route_role::Role;
    let role = match role {
        RuntimeFilterRouteRole::Producer(binding) => {
            reject_zero(
                u64::from(binding.get()),
                FieldPath::root("runtime_filter_install").field("producer_binding_id"),
                "producer binding id",
            )?;
            Role::ProducerBindingId(binding.get())
        }
        RuntimeFilterRouteRole::Aggregator => Role::Aggregator(true),
        RuntimeFilterRouteRole::Relay => Role::Relay(true),
        RuntimeFilterRouteRole::Consumer(binding) => {
            reject_zero(
                u64::from(binding.get()),
                FieldPath::root("runtime_filter_install").field("consumer_binding_id"),
                "consumer binding id",
            )?;
            Role::ConsumerBindingId(binding.get())
        }
    };
    Ok(filter::RuntimeFilterRouteRole { role: Some(role) })
}

fn decode_route_role(
    wire: &filter::RuntimeFilterRouteRole,
    path: FieldPath,
) -> CodecResult<RuntimeFilterRouteRole> {
    match wire
        .role
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("role"), "route role is required"))?
    {
        filter::runtime_filter_route_role::Role::ProducerBindingId(raw) => {
            reject_zero(
                u64::from(*raw),
                path.field("producer_binding_id"),
                "producer binding id",
            )?;
            Ok(RuntimeFilterRouteRole::Producer(BindingId::new(*raw)))
        }
        filter::runtime_filter_route_role::Role::Aggregator(true) => {
            Ok(RuntimeFilterRouteRole::Aggregator)
        }
        filter::runtime_filter_route_role::Role::Relay(true) => Ok(RuntimeFilterRouteRole::Relay),
        filter::runtime_filter_route_role::Role::ConsumerBindingId(raw) => {
            reject_zero(
                u64::from(*raw),
                path.field("consumer_binding_id"),
                "consumer binding id",
            )?;
            Ok(RuntimeFilterRouteRole::Consumer(BindingId::new(*raw)))
        }
        filter::runtime_filter_route_role::Role::Aggregator(false) => Err(invalid(
            path.field("aggregator"),
            "aggregator marker must be true",
        )),
        filter::runtime_filter_route_role::Role::Relay(false) => {
            Err(invalid(path.field("relay"), "relay marker must be true"))
        }
    }
}

fn encode_routing_edge(
    edge: &RuntimeFilterRoutingEdgeView,
) -> CodecResult<filter::RuntimeFilterRoutingEdgeView> {
    Ok(filter::RuntimeFilterRoutingEdgeView {
        route_edge_id: edge.route_edge_id().get(),
        source: Some(encode_route_endpoint(edge.source())?),
        target: Some(encode_route_endpoint(edge.target())?),
        peer: Some(encode_route_peer(edge.peer())?),
        allowed_kinds: edge
            .allowed_kinds()
            .iter()
            .copied()
            .map(encode_envelope_kind)
            .collect(),
    })
}

fn decode_routing_edge(
    wire: &filter::RuntimeFilterRoutingEdgeView,
    channel_id: ChannelId,
    path: FieldPath,
) -> CodecResult<RuntimeFilterRoutingEdgeView> {
    reject_zero(
        u64::from(wire.route_edge_id),
        path.clone().field("route_edge_id"),
        "route edge id",
    )?;
    let source = decode_route_endpoint(wire.source.as_ref(), path.clone().field("source"))?;
    let target = decode_route_endpoint(wire.target.as_ref(), path.clone().field("target"))?;
    let peer = decode_route_peer(wire.peer.as_ref(), path.clone().field("peer"))?;
    let mut allowed = BTreeSet::new();
    for (index, raw) in wire.allowed_kinds.iter().copied().enumerate() {
        let item_path = path.clone().field("allowed_kinds").index(index);
        let kind = decode_envelope_kind(raw, item_path.clone())?;
        if !allowed.insert(kind) {
            return Err(duplicate(item_path, "duplicate allowed envelope kind"));
        }
    }
    RuntimeFilterRoutingEdgeView::new(
        channel_id,
        RouteEdgeId::new(wire.route_edge_id),
        source,
        target,
        peer,
        allowed,
    )
    .map_err(|error| invalid(path, error.to_string()))
}

fn encode_route_endpoint(
    endpoint: &RuntimeFilterRouteEndpointView,
) -> CodecResult<filter::RuntimeFilterRouteEndpointView> {
    reject_zero(
        u64::from(endpoint.participant_id().get()),
        FieldPath::root("runtime_filter_install").field("route_participant_id"),
        "route participant id",
    )?;
    Ok(filter::RuntimeFilterRouteEndpointView {
        participant_id: endpoint.participant_id().get(),
        role: Some(encode_route_role(endpoint.role())?),
    })
}

fn decode_route_endpoint(
    wire: Option<&filter::RuntimeFilterRouteEndpointView>,
    path: FieldPath,
) -> CodecResult<RuntimeFilterRouteEndpointView> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "route endpoint is required"))?;
    reject_zero(
        u64::from(wire.participant_id),
        path.clone().field("participant_id"),
        "route participant id",
    )?;
    Ok(RuntimeFilterRouteEndpointView::new(
        RuntimeFilterParticipantId::new(wire.participant_id),
        decode_route_role(
            wire.role.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("role"),
                    "route endpoint role is required",
                )
            })?,
            path.field("role"),
        )?,
    ))
}

fn encode_route_peer(peer: &RuntimeFilterRoutePeer) -> CodecResult<filter::RuntimeFilterRoutePeer> {
    use filter::runtime_filter_route_peer::Peer;
    let peer = match peer {
        RuntimeFilterRoutePeer::Loopback => Peer::Loopback(true),
        RuntimeFilterRoutePeer::Remote {
            participant_id,
            endpoint,
        } => {
            reject_zero(
                u64::from(participant_id.get()),
                FieldPath::root("runtime_filter_install").field("remote_participant_id"),
                "remote participant id",
            )?;
            Peer::Remote(filter::RuntimeFilterRemotePeer {
                participant_id: participant_id.get(),
                endpoint: endpoint.as_host_port(),
            })
        }
    };
    Ok(filter::RuntimeFilterRoutePeer { peer: Some(peer) })
}

fn decode_route_peer(
    wire: Option<&filter::RuntimeFilterRoutePeer>,
    path: FieldPath,
) -> CodecResult<RuntimeFilterRoutePeer> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "route peer is required"))?;
    match wire
        .peer
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("peer"), "route peer kind is required"))?
    {
        filter::runtime_filter_route_peer::Peer::Loopback(true) => {
            Ok(RuntimeFilterRoutePeer::Loopback)
        }
        filter::runtime_filter_route_peer::Peer::Loopback(false) => Err(invalid(
            path.field("loopback"),
            "loopback marker must be true",
        )),
        filter::runtime_filter_route_peer::Peer::Remote(remote) => {
            reject_zero(
                u64::from(remote.participant_id),
                path.clone().field("remote").field("participant_id"),
                "remote participant id",
            )?;
            Ok(RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(remote.participant_id),
                endpoint: RuntimeEndpoint::parse(&remote.endpoint)
                    .map_err(|error| invalid(path.field("remote").field("endpoint"), error))?,
            })
        }
    }
}

fn encode_envelope_kind(kind: RuntimeFilterEnvelopeKind) -> i32 {
    match kind {
        RuntimeFilterEnvelopeKind::Contribution => {
            filter::RuntimeFilterEnvelopeKind::Contribution as i32
        }
        RuntimeFilterEnvelopeKind::Artifact => filter::RuntimeFilterEnvelopeKind::Artifact as i32,
        RuntimeFilterEnvelopeKind::ProducerClosed => {
            filter::RuntimeFilterEnvelopeKind::ProducerClosed as i32
        }
        RuntimeFilterEnvelopeKind::ProducerUnavailable => {
            filter::RuntimeFilterEnvelopeKind::ProducerUnavailable as i32
        }
        RuntimeFilterEnvelopeKind::Unavailable => {
            filter::RuntimeFilterEnvelopeKind::Unavailable as i32
        }
        RuntimeFilterEnvelopeKind::Ack => filter::RuntimeFilterEnvelopeKind::Ack as i32,
        RuntimeFilterEnvelopeKind::CompletedWithoutArtifact => {
            filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact as i32
        }
        RuntimeFilterEnvelopeKind::DegradedLogical => {
            filter::RuntimeFilterEnvelopeKind::DegradedLogical as i32
        }
        RuntimeFilterEnvelopeKind::FinalArtifact => {
            filter::RuntimeFilterEnvelopeKind::FinalArtifact as i32
        }
    }
}

fn decode_envelope_kind(raw: i32, path: FieldPath) -> CodecResult<RuntimeFilterEnvelopeKind> {
    match filter::RuntimeFilterEnvelopeKind::try_from(raw) {
        Ok(filter::RuntimeFilterEnvelopeKind::Contribution) => {
            Ok(RuntimeFilterEnvelopeKind::Contribution)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Artifact) => Ok(RuntimeFilterEnvelopeKind::Artifact),
        Ok(filter::RuntimeFilterEnvelopeKind::ProducerClosed) => {
            Ok(RuntimeFilterEnvelopeKind::ProducerClosed)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::ProducerUnavailable) => {
            Ok(RuntimeFilterEnvelopeKind::ProducerUnavailable)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Unavailable) => {
            Ok(RuntimeFilterEnvelopeKind::Unavailable)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Ack) => Ok(RuntimeFilterEnvelopeKind::Ack),
        Ok(filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact) => {
            Ok(RuntimeFilterEnvelopeKind::CompletedWithoutArtifact)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::DegradedLogical) => {
            Ok(RuntimeFilterEnvelopeKind::DegradedLogical)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::FinalArtifact) => {
            Ok(RuntimeFilterEnvelopeKind::FinalArtifact)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Unspecified) | Err(_) => Err(codec_error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("invalid runtime filter envelope kind={raw}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::types::UniqueId;
    use crate::proto::filter;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
        ConsumerActivation, ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder,
        NullSemantics, OrderContract, OrderKeyContract, ReductionRequirement,
        RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
        SortDirection, TopKSummaryRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::model::policy::{MAX_ARTIFACT_BYTES, MAX_DEADLINE_MS, MAX_RETRIES};
    use crate::runtime_filter::port::artifact::{
        ArtifactKind, ConsumerArtifactProfile, HashContractDigest,
    };
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::install::{
        ConsumerDeployment, MaterializationPolicy, OutboundMaterializationGroup,
        OutboundMaterializationOwner, ProducerDeployment, RuntimeFilterChannelDeployment,
        RuntimeFilterCoreBudget, RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
    };
    use crate::runtime_filter::port::ordered_bound::RuntimeOrderContract;
    use crate::runtime_filter::port::routing::{
        RuntimeFilterChannelRoutingView, RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer,
        RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
    };
    use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;

    const QUERY: UniqueId = UniqueId::new(41, 42);
    const EPOCH: DeploymentEpoch = DeploymentEpoch::new(7);

    fn lifecycle_options() -> RuntimeFilterQueryLifecycleOptions {
        RuntimeFilterQueryLifecycleOptions {
            delivery_expire: Duration::from_secs(5),
            query_expire: Duration::from_secs(30),
            transport_retry_interval: Duration::from_millis(200),
            transport_max_attempts: 3,
            transport_deadline: Duration::from_secs(2),
            transport_max_pending_entries: 1024,
            transport_max_pending_bytes: 1 << 20,
        }
    }

    fn endpoint(participant: u32, role: RuntimeFilterRouteRole) -> RuntimeFilterRouteEndpointView {
        RuntimeFilterRouteEndpointView::new(RuntimeFilterParticipantId::new(participant), role)
    }

    fn edge(
        channel_id: u32,
        edge_id: u32,
        source: RuntimeFilterRouteEndpointView,
        target: RuntimeFilterRouteEndpointView,
        peer: RuntimeFilterRoutePeer,
        allowed_kinds: BTreeSet<RuntimeFilterEnvelopeKind>,
    ) -> RuntimeFilterRoutingEdgeView {
        RuntimeFilterRoutingEdgeView::new(
            ChannelId::new(channel_id),
            RouteEdgeId::new(edge_id),
            source,
            target,
            peer,
            allowed_kinds,
        )
        .expect("valid test edge")
    }

    fn membership_channel(
        channel_id: u32,
        producer_binding: u32,
        consumer_binding: Option<u32>,
        route_edge_ids: BTreeSet<RouteEdgeId>,
    ) -> RuntimeFilterChannelDeployment {
        let producer_instance = UniqueId::new(i64::from(channel_id), 1);
        let producers = BTreeMap::from([(
            BindingId::new(producer_binding),
            ProducerDeployment::new(
                CoverageWitnessId::new(channel_id),
                BTreeSet::from([producer_instance]),
            ),
        )]);
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .expect("valid membership profile");
        let consumers = consumer_binding
            .map(|binding| {
                BTreeMap::from([(
                    BindingId::new(binding),
                    ConsumerDeployment::with_profile(
                        ConsumerActivation::BlockingSnapshot,
                        BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                        profile.clone(),
                        route_edge_ids.clone(),
                        BTreeSet::from([UniqueId::new(i64::from(channel_id), 2)]),
                    ),
                )])
            })
            .unwrap_or_default();
        let materialization_groups = consumer_binding
            .map(|_| {
                BTreeMap::from([(
                    profile.id(),
                    OutboundMaterializationGroup::new(
                        OutboundMaterializationOwner::DirectSource,
                        profile,
                        route_edge_ids,
                    ),
                )])
            })
            .unwrap_or_default();
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(channel_id),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(CoverageWitnessId::new(channel_id)),
            Coverage::Leaf(CoverageWitnessId::new(channel_id)),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 2048,
                deadline_ms: 3000,
                max_retries: 2,
            },
            RuntimeFilterCoreBudget::new(4096),
            MaterializationPolicy::new(8, 5, 17, 1, 4096, 1024, 1)
                .expect("valid materialization policy"),
            producers,
            consumers,
        )
        .with_outbound_materialization_groups(materialization_groups)
    }

    fn direct_install() -> RuntimeFilterParticipantInstall {
        let participant = RuntimeFilterParticipantId::new(1);
        let channel_id = ChannelId::new(10);
        let producer = BindingId::new(11);
        let consumer = BindingId::new(12);
        let route = edge(
            channel_id.get(),
            100,
            endpoint(
                participant.get(),
                RuntimeFilterRouteRole::Producer(producer),
            ),
            endpoint(
                participant.get(),
                RuntimeFilterRouteRole::Consumer(consumer),
            ),
            RuntimeFilterRoutePeer::Loopback,
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::Unavailable,
                RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                RuntimeFilterEnvelopeKind::DegradedLogical,
                RuntimeFilterEnvelopeKind::FinalArtifact,
            ]),
        );
        let core = membership_channel(
            channel_id.get(),
            producer.get(),
            Some(consumer.get()),
            BTreeSet::from([RouteEdgeId::new(100)]),
        );
        let routing = RuntimeFilterChannelRoutingView::new(
            channel_id,
            BTreeSet::from([
                RuntimeFilterRouteRole::Producer(producer),
                RuntimeFilterRouteRole::Consumer(consumer),
            ]),
            BTreeMap::from([((producer, UniqueId::new(10, 1)), participant)]),
            vec![route.clone()],
            vec![route],
        )
        .expect("valid direct routing");
        RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(EPOCH, participant, BTreeMap::from([(channel_id, core)])),
            RuntimeFilterRoutingShard::new(
                EPOCH,
                participant,
                BTreeMap::from([(channel_id, routing)]),
            )
            .expect("valid direct shard"),
        )
    }

    fn replace_only_core_channel(
        install: RuntimeFilterParticipantInstall,
        replace: impl FnOnce(RuntimeFilterChannelDeployment) -> RuntimeFilterChannelDeployment,
    ) -> RuntimeFilterParticipantInstall {
        let (core, routing) = install.into_parts();
        let participant = core.local_participant_id();
        let epoch = core.epoch();
        let mut channels = core.channels().clone();
        let channel_id = *channels.keys().next().expect("one core channel");
        let channel = channels.remove(&channel_id).expect("core channel");
        channels.insert(channel_id, replace(channel));
        RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(epoch, participant, channels),
            routing,
        )
    }

    fn with_contributions(
        channel: RuntimeFilterChannelDeployment,
        contributions: BTreeSet<ContributionKind>,
    ) -> RuntimeFilterChannelDeployment {
        RuntimeFilterChannelDeployment::new(
            channel.channel_id(),
            channel.logical_domain().clone(),
            channel.lifecycle(),
            channel.availability_coverage().clone(),
            channel.terminal_coverage().clone(),
            channel.reduction_requirement(),
            contributions,
            channel.completion_requirement(),
            channel.policy(),
            channel.core_budget(),
            channel.materialization_policy(),
            channel.producers().clone(),
            channel.consumers().clone(),
        )
        .with_outbound_materialization_groups(channel.outbound_materialization_groups().clone())
    }

    fn with_core_budget(
        channel: RuntimeFilterChannelDeployment,
        budget: RuntimeFilterCoreBudget,
    ) -> RuntimeFilterChannelDeployment {
        RuntimeFilterChannelDeployment::new(
            channel.channel_id(),
            channel.logical_domain().clone(),
            channel.lifecycle(),
            channel.availability_coverage().clone(),
            channel.terminal_coverage().clone(),
            channel.reduction_requirement(),
            channel.allowed_contribution_kinds().clone(),
            channel.completion_requirement(),
            channel.policy(),
            budget,
            channel.materialization_policy(),
            channel.producers().clone(),
            channel.consumers().clone(),
        )
        .with_outbound_materialization_groups(channel.outbound_materialization_groups().clone())
    }

    fn with_policy(
        channel: RuntimeFilterChannelDeployment,
        policy: RuntimeFilterPolicyRequirement,
    ) -> RuntimeFilterChannelDeployment {
        RuntimeFilterChannelDeployment::new(
            channel.channel_id(),
            channel.logical_domain().clone(),
            channel.lifecycle(),
            channel.availability_coverage().clone(),
            channel.terminal_coverage().clone(),
            channel.reduction_requirement(),
            channel.allowed_contribution_kinds().clone(),
            channel.completion_requirement(),
            policy,
            channel.core_budget(),
            channel.materialization_policy(),
            channel.producers().clone(),
            channel.consumers().clone(),
        )
        .with_outbound_materialization_groups(channel.outbound_materialization_groups().clone())
    }

    fn with_empty_consumer_capabilities(
        channel: RuntimeFilterChannelDeployment,
    ) -> RuntimeFilterChannelDeployment {
        let consumers = channel
            .consumers()
            .iter()
            .map(|(binding, consumer)| {
                (
                    *binding,
                    ConsumerDeployment::with_profile(
                        consumer.activation(),
                        BTreeSet::new(),
                        consumer.artifact_profile().clone(),
                        consumer.route_edge_ids().clone(),
                        consumer.expected_fragment_instances().clone(),
                    ),
                )
            })
            .collect();
        let groups = channel.outbound_materialization_groups().clone();
        RuntimeFilterChannelDeployment::new(
            channel.channel_id(),
            channel.logical_domain().clone(),
            channel.lifecycle(),
            channel.availability_coverage().clone(),
            channel.terminal_coverage().clone(),
            channel.reduction_requirement(),
            channel.allowed_contribution_kinds().clone(),
            channel.completion_requirement(),
            channel.policy(),
            channel.core_budget(),
            channel.materialization_policy(),
            channel.producers().clone(),
            consumers,
        )
        .with_outbound_materialization_groups(groups)
    }

    fn with_empty_local_roles(
        install: RuntimeFilterParticipantInstall,
    ) -> RuntimeFilterParticipantInstall {
        let (core, routing) = install.into_parts();
        let channels = routing
            .channels()
            .iter()
            .map(|(channel_id, _channel)| {
                (
                    *channel_id,
                    RuntimeFilterChannelRoutingView::new(
                        *channel_id,
                        BTreeSet::new(),
                        BTreeMap::new(),
                        Vec::new(),
                        Vec::new(),
                    )
                    .expect("routing DTO permits validation-boundary role checks"),
                )
            })
            .collect();
        RuntimeFilterParticipantInstall::new(
            core,
            RuntimeFilterRoutingShard::new(
                routing.deployment_epoch(),
                routing.local_participant_id(),
                channels,
            )
            .expect("valid routing shard shape"),
        )
    }

    fn ordered_bound_channel(
        channel: RuntimeFilterChannelDeployment,
    ) -> RuntimeFilterChannelDeployment {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        }];
        let order = OrderContract {
            comparator_digest:
                crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
                    &keys,
                    crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
                ),
            keys,
            inclusive: true,
        };
        let digest = RuntimeOrderContract::try_from_plan(&order)
            .expect("valid order contract")
            .digest();
        let consumers = channel
            .consumers()
            .iter()
            .map(|(binding, consumer)| {
                (
                    *binding,
                    ConsumerDeployment::with_profile(
                        ConsumerActivation::NonBlockingLive {
                            late_apply: LateApplyGranularity::RowGroup,
                        },
                        BTreeSet::from([ArtifactCapability::OrderedRange]),
                        ConsumerArtifactProfile::new_ordered_range(digest)
                            .expect("valid range profile"),
                        consumer.route_edge_ids().clone(),
                        consumer.expected_fragment_instances().clone(),
                    ),
                )
            })
            .collect();
        let groups = channel
            .outbound_materialization_groups()
            .values()
            .map(|group| {
                let profile = ConsumerArtifactProfile::new_ordered_range(digest)
                    .expect("valid range profile");
                (
                    profile.id(),
                    OutboundMaterializationGroup::new(
                        group.owner(),
                        profile,
                        group.route_edge_ids().clone(),
                    ),
                )
            })
            .collect();
        RuntimeFilterChannelDeployment::new(
            channel.channel_id(),
            RuntimeFilterLogicalDomain::OrderedBound(order),
            RuntimeFilterLifecycle::MonotonicUpdates,
            channel.availability_coverage().clone(),
            channel.terminal_coverage().clone(),
            ReductionRequirement::TightenOrderedBound,
            BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            channel.policy(),
            channel.core_budget(),
            channel.materialization_policy(),
            channel.producers().clone(),
            consumers,
        )
        .with_outbound_materialization_groups(groups)
    }

    fn final_domain_channel(
        channel: RuntimeFilterChannelDeployment,
    ) -> RuntimeFilterChannelDeployment {
        let coverage = Coverage::AllOf(vec![channel.availability_coverage().clone()]);
        let consumers = channel
            .consumers()
            .iter()
            .map(|(binding, consumer)| {
                (
                    *binding,
                    ConsumerDeployment::with_profile(
                        ConsumerActivation::NonBlockingLive {
                            late_apply: LateApplyGranularity::File,
                        },
                        consumer.capabilities().clone(),
                        consumer.artifact_profile().clone(),
                        consumer.route_edge_ids().clone(),
                        consumer.expected_fragment_instances().clone(),
                    ),
                )
            })
            .collect();
        RuntimeFilterChannelDeployment::new(
            channel.channel_id(),
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            coverage.clone(),
            coverage,
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
            channel.policy(),
            channel.core_budget(),
            channel.materialization_policy(),
            channel.producers().clone(),
            consumers,
        )
        .with_outbound_materialization_groups(channel.outbound_materialization_groups().clone())
    }

    fn ordered_topk_channel(
        channel_id: u32,
        producer_binding: u32,
    ) -> RuntimeFilterChannelDeployment {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Descending,
            null_order: NullOrder::Last,
        }];
        let order = OrderContract {
            comparator_digest:
                crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
                    &keys,
                    crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
                ),
            keys,
            inclusive: true,
        };
        RuntimeFilterChannelDeployment::new(
            ChannelId::new(channel_id),
            RuntimeFilterLogicalDomain::OrderedBound(order),
            RuntimeFilterLifecycle::MonotonicUpdates,
            Coverage::AllOf(vec![Coverage::Leaf(CoverageWitnessId::new(channel_id))]),
            Coverage::AllOf(vec![Coverage::Leaf(CoverageWitnessId::new(channel_id))]),
            ReductionRequirement::MergeTopKSummary(
                TopKSummaryRequirement::try_new(3).expect("nonzero TopK"),
            ),
            BTreeSet::from([
                ContributionKind::TopKSummary,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 2048,
                deadline_ms: 3000,
                max_retries: 2,
            },
            RuntimeFilterCoreBudget::new(4096),
            MaterializationPolicy::new(8, 5, 17, 1, 4096, 1024, 1)
                .expect("valid materialization policy"),
            BTreeMap::from([(
                BindingId::new(producer_binding),
                ProducerDeployment::new(
                    CoverageWitnessId::new(channel_id),
                    BTreeSet::from([UniqueId::new(i64::from(channel_id), 1)]),
                ),
            )]),
            BTreeMap::new(),
        )
    }

    fn aggregate_install() -> RuntimeFilterParticipantInstall {
        let local = RuntimeFilterParticipantId::new(2);
        let producer_participant = RuntimeFilterParticipantId::new(1);
        let consumer_participant = RuntimeFilterParticipantId::new(3);
        let channel_id = ChannelId::new(20);
        let producer = BindingId::new(21);
        let consumer = BindingId::new(22);
        let producer_instance = UniqueId::new(20, 1);
        let inbound = edge(
            channel_id.get(),
            200,
            endpoint(
                producer_participant.get(),
                RuntimeFilterRouteRole::Producer(producer),
            ),
            endpoint(local.get(), RuntimeFilterRouteRole::Aggregator),
            RuntimeFilterRoutePeer::Remote {
                participant_id: producer_participant,
                endpoint: RuntimeEndpoint::new("be-1", 9060).expect("endpoint"),
            },
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
            ]),
        );
        let outbound = edge(
            channel_id.get(),
            201,
            endpoint(local.get(), RuntimeFilterRouteRole::Aggregator),
            endpoint(
                consumer_participant.get(),
                RuntimeFilterRouteRole::Consumer(consumer),
            ),
            RuntimeFilterRoutePeer::Remote {
                participant_id: consumer_participant,
                endpoint: RuntimeEndpoint::new("be-3", 9060).expect("endpoint"),
            },
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::Unavailable,
                RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                RuntimeFilterEnvelopeKind::DegradedLogical,
                RuntimeFilterEnvelopeKind::FinalArtifact,
            ]),
        );
        let core = ordered_topk_channel(channel_id.get(), producer.get());
        let RuntimeFilterLogicalDomain::OrderedBound(order) = core.logical_domain() else {
            unreachable!("TopK fixture is ordered")
        };
        let profile = ConsumerArtifactProfile::new_ordered_range(
            RuntimeOrderContract::try_from_plan(order)
                .expect("valid order contract")
                .digest(),
        )
        .expect("valid range profile");
        let core = core.with_outbound_materialization_groups(BTreeMap::from([(
            profile.id(),
            OutboundMaterializationGroup::new(
                OutboundMaterializationOwner::Aggregator,
                profile,
                BTreeSet::from([RouteEdgeId::new(201)]),
            ),
        )]));
        let routing = RuntimeFilterChannelRoutingView::new(
            channel_id,
            BTreeSet::from([RuntimeFilterRouteRole::Aggregator]),
            BTreeMap::from([((producer, producer_instance), producer_participant)]),
            vec![inbound],
            vec![outbound],
        )
        .expect("valid aggregator routing");
        RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(EPOCH, local, BTreeMap::from([(channel_id, core)])),
            RuntimeFilterRoutingShard::new(EPOCH, local, BTreeMap::from([(channel_id, routing)]))
                .expect("valid aggregator shard"),
        )
    }

    fn relay_install() -> RuntimeFilterParticipantInstall {
        let local = RuntimeFilterParticipantId::new(4);
        let channel_id = ChannelId::new(30);
        let routing = RuntimeFilterChannelRoutingView::new(
            channel_id,
            BTreeSet::from([RuntimeFilterRouteRole::Relay]),
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("valid relay routing");
        RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(EPOCH, local, BTreeMap::new()),
            RuntimeFilterRoutingShard::new(EPOCH, local, BTreeMap::from([(channel_id, routing)]))
                .expect("valid relay shard"),
        )
    }

    #[test]
    fn runtime_filter_install_round_trips_direct_aggregate_and_relay() {
        let ordered = replace_only_core_channel(direct_install(), ordered_bound_channel);
        let final_domain = replace_only_core_channel(direct_install(), final_domain_channel);
        for install in [
            direct_install(),
            ordered,
            aggregate_install(),
            final_domain,
            relay_install(),
        ] {
            let request = encode_participant_install(QUERY, lifecycle_options(), &install)
                .expect("encode participant install");
            let decoded = decode_participant_install(&request).expect("decode participant install");
            assert_eq!(decoded.query_id, QUERY);
            assert_eq!(decoded.lifecycle, lifecycle_options());
            assert_eq!(decoded.install, install);
        }
    }

    #[test]
    fn outbound_materialization_group_wire_contract_is_strict() {
        let request = encode_participant_install(QUERY, lifecycle_options(), &direct_install())
            .expect("encode direct install with materialization authority");
        let group =
            &request.install.as_ref().unwrap().core_channels[0].outbound_materialization_groups[0];
        assert_eq!(
            group.owner,
            filter::RuntimeFilterOutboundMaterializationOwner::DirectSource as i32
        );
        assert!(group.artifact_profile.is_some());
        assert_eq!(group.route_edge_ids, vec![100]);
        assert_eq!(
            decode_participant_install(&request).unwrap().install,
            direct_install()
        );

        let mut missing_profile = request.clone();
        missing_profile.install.as_mut().unwrap().core_channels[0]
            .outbound_materialization_groups[0]
            .artifact_profile = None;
        assert!(decode_participant_install(&missing_profile).is_err());

        let mut unspecified_owner = request.clone();
        unspecified_owner.install.as_mut().unwrap().core_channels[0]
            .outbound_materialization_groups[0]
            .owner = filter::RuntimeFilterOutboundMaterializationOwner::Unspecified as i32;
        assert!(decode_participant_install(&unspecified_owner).is_err());

        let mut unknown_owner = request.clone();
        unknown_owner.install.as_mut().unwrap().core_channels[0].outbound_materialization_groups
            [0]
        .owner = 999;
        assert!(decode_participant_install(&unknown_owner).is_err());

        let mut duplicate_profile = request.clone();
        let duplicate = duplicate_profile.install.as_ref().unwrap().core_channels[0]
            .outbound_materialization_groups[0]
            .clone();
        duplicate_profile.install.as_mut().unwrap().core_channels[0]
            .outbound_materialization_groups
            .push(duplicate);
        assert!(decode_participant_install(&duplicate_profile).is_err());

        let mut duplicate_route = request.clone();
        duplicate_route.install.as_mut().unwrap().core_channels[0].outbound_materialization_groups
            [0]
        .route_edge_ids
        .push(100);
        assert!(decode_participant_install(&duplicate_route).is_err());

        let mut edge_drift = request.clone();
        edge_drift.install.as_mut().unwrap().core_channels[0].outbound_materialization_groups[0]
            .route_edge_ids[0] = 101;
        assert!(decode_participant_install(&edge_drift).is_err());

        let mut missing_authority = request;
        missing_authority.install.as_mut().unwrap().core_channels[0]
            .outbound_materialization_groups
            .clear();
        assert!(decode_participant_install(&missing_authority).is_err());
    }

    #[test]
    fn routing_edge_wire_rejects_cross_family_extra_allowed_kind() {
        let mut request = encode_participant_install(QUERY, lifecycle_options(), &direct_install())
            .expect("encode direct install");
        let channel = &mut request.install.as_mut().unwrap().routing_channels[0];
        for edge in channel
            .inbound_edges
            .iter_mut()
            .chain(channel.outbound_edges.iter_mut())
        {
            edge.allowed_kinds
                .push(filter::RuntimeFilterEnvelopeKind::Contribution as i32);
        }

        decode_participant_install(&request)
            .expect_err("direct delivery edges must reject contribution-family authority");
    }

    #[test]
    fn runtime_filter_install_rejects_unspecified_zero_duplicate_and_bad_digest() {
        let request = encode_participant_install(QUERY, lifecycle_options(), &direct_install())
            .expect("encode participant install");

        let mut zero = request.clone();
        zero.deployment_epoch = 0;
        assert!(decode_participant_install(&zero).is_err());

        let mut zero_participant = request.clone();
        zero_participant.participant_id = 0;
        assert!(decode_participant_install(&zero_participant).is_err());

        let mut zero_route_participant = request.clone();
        zero_route_participant
            .install
            .as_mut()
            .expect("install")
            .routing_channels[0]
            .inbound_edges[0]
            .source
            .as_mut()
            .expect("source")
            .participant_id = 0;
        assert!(decode_participant_install(&zero_route_participant).is_err());

        let mut duplicate_channel = request.clone();
        let install = duplicate_channel.install.as_mut().expect("install");
        install.core_channels.push(install.core_channels[0].clone());
        assert!(decode_participant_install(&duplicate_channel).is_err());

        let mut duplicate_producer = request.clone();
        let channel = &mut duplicate_producer
            .install
            .as_mut()
            .expect("install")
            .core_channels[0];
        channel.producers.push(channel.producers[0].clone());
        assert!(decode_participant_install(&duplicate_producer).is_err());

        let mut duplicate_consumer = request.clone();
        let channel = &mut duplicate_consumer
            .install
            .as_mut()
            .expect("install")
            .core_channels[0];
        channel.consumers.push(channel.consumers[0].clone());
        assert!(decode_participant_install(&duplicate_consumer).is_err());

        let mut duplicate_route = request.clone();
        let channel = &mut duplicate_route
            .install
            .as_mut()
            .expect("install")
            .routing_channels[0];
        channel
            .producer_instances
            .push(channel.producer_instances[0].clone());
        assert!(decode_participant_install(&duplicate_route).is_err());

        let mut duplicate_edge = request.clone();
        let channel = &mut duplicate_edge
            .install
            .as_mut()
            .expect("install")
            .routing_channels[0];
        channel.inbound_edges.push(channel.inbound_edges[0].clone());
        assert!(decode_participant_install(&duplicate_edge).is_err());

        let mut unspecified = request.clone();
        unspecified
            .install
            .as_mut()
            .expect("install")
            .routing_channels[0]
            .inbound_edges[0]
            .allowed_kinds[0] = filter::RuntimeFilterEnvelopeKind::Unspecified as i32;
        assert!(decode_participant_install(&unspecified).is_err());

        let mut unknown = request.clone();
        unknown.install.as_mut().expect("install").routing_channels[0].inbound_edges[0]
            .allowed_kinds[0] = 99_999;
        assert!(decode_participant_install(&unknown).is_err());

        let mut unspecified_lifecycle = request.clone();
        unspecified_lifecycle
            .install
            .as_mut()
            .expect("install")
            .core_channels[0]
            .lifecycle = filter::RuntimeFilterLifecycle::Unspecified as i32;
        assert!(decode_participant_install(&unspecified_lifecycle).is_err());

        let mut unspecified_contribution = request.clone();
        unspecified_contribution
            .install
            .as_mut()
            .expect("install")
            .core_channels[0]
            .allowed_contribution_kinds[0] =
            crate::proto::plan::RuntimeFilterContributionKind::Unspecified as i32;
        assert!(decode_participant_install(&unspecified_contribution).is_err());

        let mut unspecified_completion = request.clone();
        unspecified_completion
            .install
            .as_mut()
            .expect("install")
            .core_channels[0]
            .completion_requirement =
            crate::proto::plan::RuntimeFilterCompletionRequirement::Unspecified as i32;
        assert!(decode_participant_install(&unspecified_completion).is_err());

        let mut unspecified_capability = request.clone();
        unspecified_capability
            .install
            .as_mut()
            .expect("install")
            .core_channels[0]
            .consumers[0]
            .capabilities[0] =
            crate::proto::plan::RuntimeFilterArtifactCapability::Unspecified as i32;
        assert!(decode_participant_install(&unspecified_capability).is_err());

        let mut unspecified_artifact = request.clone();
        unspecified_artifact
            .install
            .as_mut()
            .expect("install")
            .core_channels[0]
            .consumers[0]
            .artifact_profile
            .as_mut()
            .expect("profile")
            .accepted_kinds[0] = filter::RuntimeFilterArtifactKind::Unspecified as i32;
        assert!(decode_participant_install(&unspecified_artifact).is_err());

        let mut missing_activation = request.clone();
        missing_activation
            .install
            .as_mut()
            .expect("install")
            .core_channels[0]
            .consumers[0]
            .activation
            .as_mut()
            .expect("activation")
            .kind = None;
        assert!(decode_participant_install(&missing_activation).is_err());

        let mut missing_coverage = request.clone();
        missing_coverage
            .install
            .as_mut()
            .expect("install")
            .core_channels[0]
            .availability_coverage
            .as_mut()
            .expect("coverage")
            .kind = None;
        assert!(decode_participant_install(&missing_coverage).is_err());

        let mut missing_contract = request.clone();
        missing_contract
            .install
            .as_mut()
            .expect("install")
            .core_channels[0]
            .logical_domain
            .as_mut()
            .expect("logical domain")
            .contract
            .as_mut()
            .expect("contract")
            .kind = None;
        assert!(decode_participant_install(&missing_contract).is_err());

        let mut missing_reduction = request.clone();
        missing_reduction
            .install
            .as_mut()
            .expect("install")
            .core_channels[0]
            .reduction
            .as_mut()
            .expect("reduction")
            .kind = None;
        assert!(decode_participant_install(&missing_reduction).is_err());

        let mut missing_peer = request.clone();
        missing_peer
            .install
            .as_mut()
            .expect("install")
            .routing_channels[0]
            .inbound_edges[0]
            .peer
            .as_mut()
            .expect("peer")
            .peer = None;
        assert!(decode_participant_install(&missing_peer).is_err());

        let mut missing_oneof = request.clone();
        missing_oneof
            .install
            .as_mut()
            .expect("install")
            .routing_channels[0]
            .local_roles[0]
            .role = None;
        assert!(decode_participant_install(&missing_oneof).is_err());

        let mut bad_digest = request;
        bad_digest.install.as_mut().expect("install").core_channels[0].consumers[0]
            .artifact_profile
            .as_mut()
            .expect("profile")
            .profile_id = vec![0; 31];
        assert!(decode_participant_install(&bad_digest).is_err());
    }

    #[test]
    fn runtime_filter_install_rejects_epoch_or_participant_drift() {
        let mut epoch_drift = direct_install();
        let (core, routing) = epoch_drift.into_parts();
        epoch_drift = RuntimeFilterParticipantInstall::new(
            core,
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(EPOCH.get() + 1),
                routing.local_participant_id(),
                routing.channels().clone(),
            )
            .expect("drifted shard"),
        );
        assert!(encode_participant_install(QUERY, lifecycle_options(), &epoch_drift).is_err());

        let direct = direct_install();
        let (core, routing) = direct.into_parts();
        let participant_drift = RuntimeFilterParticipantInstall::new(
            core,
            RuntimeFilterRoutingShard::new(
                routing.deployment_epoch(),
                RuntimeFilterParticipantId::new(9),
                BTreeMap::new(),
            )
            .expect("drifted participant shard"),
        );
        assert!(
            encode_participant_install(QUERY, lifecycle_options(), &participant_drift).is_err()
        );

        let mut wire_participant_drift =
            encode_participant_install(QUERY, lifecycle_options(), &direct_install())
                .expect("encode participant install");
        wire_participant_drift.participant_id = 9;
        assert!(decode_participant_install(&wire_participant_drift).is_err());
    }

    #[test]
    fn runtime_filter_install_encoder_and_decoder_reject_same_invalid_contracts() {
        let domain_cases = vec![
            (
                "empty contribution set",
                replace_only_core_channel(direct_install(), |channel| {
                    with_contributions(channel, BTreeSet::new())
                }),
            ),
            (
                "zero core budget",
                replace_only_core_channel(direct_install(), |channel| {
                    with_core_budget(channel, RuntimeFilterCoreBudget::new(0))
                }),
            ),
            (
                "empty consumer capabilities",
                replace_only_core_channel(direct_install(), with_empty_consumer_capabilities),
            ),
            (
                "empty local role set",
                with_empty_local_roles(direct_install()),
            ),
            (
                "membership contribution matrix",
                replace_only_core_channel(direct_install(), |channel| {
                    with_contributions(
                        channel,
                        BTreeSet::from([
                            ContributionKind::FinalDomainShard,
                            ContributionKind::ProducerClosed,
                        ]),
                    )
                }),
            ),
            (
                "zero max retries",
                replace_only_core_channel(direct_install(), |channel| {
                    let policy = RuntimeFilterPolicyRequirement {
                        max_retries: 0,
                        ..channel.policy()
                    };
                    with_policy(channel, policy)
                }),
            ),
            (
                "contribution bytes exceed artifact bytes",
                replace_only_core_channel(direct_install(), |channel| {
                    let policy = RuntimeFilterPolicyRequirement {
                        max_contribution_bytes: channel.policy().max_artifact_bytes + 1,
                        ..channel.policy()
                    };
                    with_policy(channel, policy)
                }),
            ),
            (
                "artifact bytes exceed canonical limit",
                replace_only_core_channel(direct_install(), |channel| {
                    let policy = RuntimeFilterPolicyRequirement {
                        max_artifact_bytes: MAX_ARTIFACT_BYTES + 1,
                        ..channel.policy()
                    };
                    with_policy(channel, policy)
                }),
            ),
            (
                "deadline exceeds canonical limit",
                replace_only_core_channel(direct_install(), |channel| {
                    let policy = RuntimeFilterPolicyRequirement {
                        deadline_ms: MAX_DEADLINE_MS + 1,
                        ..channel.policy()
                    };
                    with_policy(channel, policy)
                }),
            ),
            (
                "retries exceed canonical limit",
                replace_only_core_channel(direct_install(), |channel| {
                    let policy = RuntimeFilterPolicyRequirement {
                        max_retries: MAX_RETRIES + 1,
                        ..channel.policy()
                    };
                    with_policy(channel, policy)
                }),
            ),
        ];
        for (name, invalid) in domain_cases {
            assert!(
                encode_participant_install(QUERY, lifecycle_options(), &invalid).is_err(),
                "encoder accepted invalid contract: {name}"
            );
        }

        type Request = filter::InstallRuntimeFilterDeploymentRequest;
        let mutations: [(&str, fn(&mut Request)); 10] = [
            ("empty contribution set", |request| {
                request.install.as_mut().expect("install").core_channels[0]
                    .allowed_contribution_kinds
                    .clear();
            }),
            ("zero core budget", |request| {
                request.install.as_mut().expect("install").core_channels[0]
                    .core_budget
                    .as_mut()
                    .expect("core budget")
                    .max_reducer_bytes = 0;
            }),
            ("empty consumer capabilities", |request| {
                request.install.as_mut().expect("install").core_channels[0].consumers[0]
                    .capabilities
                    .clear();
            }),
            ("empty local role set", |request| {
                request.install.as_mut().expect("install").routing_channels[0]
                    .local_roles
                    .clear();
            }),
            ("membership contribution matrix", |request| {
                request.install.as_mut().expect("install").core_channels[0]
                    .allowed_contribution_kinds = vec![
                    crate::proto::plan::RuntimeFilterContributionKind::FinalDomainShard as i32,
                    crate::proto::plan::RuntimeFilterContributionKind::ProducerClosed as i32,
                ];
            }),
            ("zero max retries", |request| {
                request.install.as_mut().expect("install").core_channels[0]
                    .policy
                    .as_mut()
                    .expect("policy")
                    .max_retries = 0;
            }),
            ("contribution bytes exceed artifact bytes", |request| {
                let policy = request.install.as_mut().expect("install").core_channels[0]
                    .policy
                    .as_mut()
                    .expect("policy");
                policy.max_contribution_bytes = policy.max_artifact_bytes + 1;
            }),
            ("artifact bytes exceed canonical limit", |request| {
                request.install.as_mut().expect("install").core_channels[0]
                    .policy
                    .as_mut()
                    .expect("policy")
                    .max_artifact_bytes = MAX_ARTIFACT_BYTES + 1;
            }),
            ("deadline exceeds canonical limit", |request| {
                request.install.as_mut().expect("install").core_channels[0]
                    .policy
                    .as_mut()
                    .expect("policy")
                    .deadline_ms = MAX_DEADLINE_MS + 1;
            }),
            ("retries exceed canonical limit", |request| {
                request.install.as_mut().expect("install").core_channels[0]
                    .policy
                    .as_mut()
                    .expect("policy")
                    .max_retries = MAX_RETRIES + 1;
            }),
        ];
        let valid = encode_participant_install(QUERY, lifecycle_options(), &direct_install())
            .expect("encode valid membership install");
        for (name, mutate) in mutations {
            let mut invalid = valid.clone();
            mutate(&mut invalid);
            assert!(
                decode_participant_install(&invalid).is_err(),
                "decoder accepted invalid contract: {name}"
            );
        }
    }

    #[test]
    fn runtime_filter_leaf_enums_and_profiles_round_trip_exhaustively() {
        let activations = [
            ConsumerActivation::BlockingSnapshot,
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Row,
            },
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::RowGroup,
            },
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Split,
            },
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::File,
            },
        ];
        for activation in activations {
            let wire = encode_runtime_filter_activation(activation);
            assert_eq!(
                decode_runtime_filter_activation(
                    Some(&wire),
                    FieldPath::root("test").field("activation"),
                )
                .expect("decode activation"),
                activation
            );
        }

        for contribution in [
            ContributionKind::ValueDomainDelta,
            ContributionKind::FinalDomainShard,
            ContributionKind::OrderedBoundUpdate,
            ContributionKind::TopKSummary,
            ContributionKind::ProducerClosed,
        ] {
            assert_eq!(
                decode_runtime_filter_contribution_kind(
                    encode_runtime_filter_contribution_kind(contribution),
                    FieldPath::root("test").field("contribution"),
                )
                .expect("decode contribution"),
                contribution
            );
        }

        for completion in [
            CompletionRequirement::ProducerClosed,
            CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
        ] {
            assert_eq!(
                decode_runtime_filter_completion(
                    encode_runtime_filter_completion(completion),
                    FieldPath::root("test").field("completion"),
                )
                .expect("decode completion"),
                completion
            );
        }

        for kind in [
            RuntimeFilterEnvelopeKind::Contribution,
            RuntimeFilterEnvelopeKind::Artifact,
            RuntimeFilterEnvelopeKind::ProducerClosed,
            RuntimeFilterEnvelopeKind::Unavailable,
            RuntimeFilterEnvelopeKind::Ack,
            RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
            RuntimeFilterEnvelopeKind::DegradedLogical,
            RuntimeFilterEnvelopeKind::FinalArtifact,
        ] {
            assert_eq!(
                decode_envelope_kind(
                    encode_envelope_kind(kind),
                    FieldPath::root("test").field("envelope_kind"),
                )
                .expect("decode envelope kind"),
                kind
            );
        }

        let profiles = [
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
                None,
            )
            .expect("value-set profile"),
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::Bloom, ArtifactKind::EmptyDomain]),
                Some(HashContractDigest::new([1; 32])),
            )
            .expect("bloom profile"),
            ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::Bitset, ArtifactKind::EmptyDomain]),
                None,
            )
            .expect("bitset profile"),
            ConsumerArtifactProfile::new_ordered_range(
                crate::runtime_filter::port::ordered_bound::OrderContractDigest::from_bytes_for_codec(
                    [2; 32],
                ),
            )
            .expect("range profile"),
        ];
        for profile in profiles {
            let wire = encode_artifact_profile(&profile).expect("encode artifact profile");
            assert_eq!(
                decode_artifact_profile(
                    Some(&wire),
                    FieldPath::root("test").field("artifact_profile"),
                )
                .expect("decode artifact profile"),
                profile
            );
        }
    }

    #[test]
    fn runtime_filter_coverage_round_trips_all_shapes_and_rejects_bad_composites() {
        let leaf = Coverage::Leaf(CoverageWitnessId::new(1));
        for coverage in [
            leaf.clone(),
            Coverage::AllOf(vec![
                leaf.clone(),
                Coverage::Leaf(CoverageWitnessId::new(2)),
            ]),
            Coverage::AnyOf(vec![
                leaf.clone(),
                Coverage::Leaf(CoverageWitnessId::new(2)),
            ]),
        ] {
            let wire = encode_coverage(&coverage).expect("encode coverage");
            assert_eq!(
                decode_coverage(Some(&wire), FieldPath::root("test").field("coverage"))
                    .expect("decode coverage"),
                coverage
            );
        }

        for invalid in [
            Coverage::AllOf(Vec::new()),
            Coverage::AnyOf(Vec::new()),
            Coverage::AllOf(vec![leaf.clone(), leaf.clone()]),
        ] {
            assert!(encode_coverage(&invalid).is_err());
        }

        let leaf_wire = encode_coverage(&leaf).expect("encode leaf coverage");
        let invalid_wires = [
            filter::RuntimeFilterCoverage {
                kind: Some(filter::runtime_filter_coverage::Kind::AllOf(
                    filter::RuntimeFilterCoverageAllOf {
                        children: Vec::new(),
                    },
                )),
            },
            filter::RuntimeFilterCoverage {
                kind: Some(filter::runtime_filter_coverage::Kind::AnyOf(
                    filter::RuntimeFilterCoverageAnyOf {
                        children: Vec::new(),
                    },
                )),
            },
            filter::RuntimeFilterCoverage {
                kind: Some(filter::runtime_filter_coverage::Kind::AllOf(
                    filter::RuntimeFilterCoverageAllOf {
                        children: vec![leaf_wire.clone(), leaf_wire],
                    },
                )),
            },
        ];
        for wire in invalid_wires {
            assert!(
                decode_coverage(Some(&wire), FieldPath::root("test").field("coverage")).is_err()
            );
        }
    }

    #[test]
    fn runtime_filter_lifecycle_options_reject_zero_and_overflow() {
        let request = encode_participant_install(QUERY, lifecycle_options(), &direct_install())
            .expect("encode participant install");
        for mutate in [
            |options: &mut filter::RuntimeFilterQueryLifecycleOptions| {
                options.delivery_expire_ms = 0
            },
            |options: &mut filter::RuntimeFilterQueryLifecycleOptions| {
                options.transport_max_attempts = 0
            },
            |options: &mut filter::RuntimeFilterQueryLifecycleOptions| {
                options.transport_max_pending_entries = u64::MAX
            },
            |options: &mut filter::RuntimeFilterQueryLifecycleOptions| {
                options.transport_max_attempts = u64::from(u32::MAX) + 1
            },
        ] {
            let mut invalid = request.clone();
            mutate(invalid.lifecycle.as_mut().expect("lifecycle"));
            assert!(decode_participant_install(&invalid).is_err());
        }
    }

    #[test]
    fn abort_contract_round_trips_query_and_epoch() {
        let request = encode_abort_runtime_filter_deployment(QUERY, EPOCH)
            .expect("encode runtime filter abort");
        assert_eq!(
            decode_abort_runtime_filter_deployment(&request).expect("decode runtime filter abort"),
            RuntimeFilterDeploymentAbort {
                query_id: QUERY,
                epoch: EPOCH,
            }
        );

        let mut zero_query = request.clone();
        zero_query.query_id = Some(Default::default());
        assert!(decode_abort_runtime_filter_deployment(&zero_query).is_err());
        let mut zero_epoch = request;
        zero_epoch.deployment_epoch = 0;
        assert!(decode_abort_runtime_filter_deployment(&zero_epoch).is_err());
    }
}
