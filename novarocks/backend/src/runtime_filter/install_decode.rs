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

//! Backend-owned runtime-filter install decoder.
//!
//! Core carries an opaque contribution DTO. This module is the only native
//! boundary that interprets its lifecycle/install/routing semantics and builds
//! the participant-local install consumed by the Backend service.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::runtime_filter::{
    ConsumerActivation, RuntimeFilterBindingId, RuntimeFilterChannelId,
    RuntimeFilterConsumerContract, RuntimeFilterExecutionContract,
    RuntimeFilterLateApplyGranularity, RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics,
    RuntimeFilterProducerContract, RuntimeFilterProducerKind, RuntimeFilterReduction, contribution,
};
use novarocks_proto::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto::{
    common, filter,
    lifecycle::{QueryExecutionId, RuntimeFilterContribution},
    plan,
};
use novarocks_types::UniqueId;
use prost::Message;
use sha2::Digest;

use crate::fragment::decode::type_decode::decode_type;
use crate::query_lifecycle::{QueryLifecycleError, QueryLifecycleErrorCode};
use crate::runtime_filter::artifact::{ArtifactKind, ConsumerArtifactProfile, HashContractDigest};
use crate::runtime_filter::domain::{
    BackendChannelInstall, BackendChannelLifecycle, BackendConsumerInstall, BackendCoverage,
    BackendCoverageWitnessId, BackendEnvelopeKind, BackendMaterializationOwner,
    BackendMaterializationPolicy, BackendOutboundMaterializationGroup, BackendParticipantIdentity,
    BackendParticipantInstall, BackendProducerInstall, BackendRouteEdgeId, BackendRouteEndpoint,
    BackendRoutePeer, BackendRouteRole, BackendRoutingChannel, BackendRoutingEdge,
    BackendRoutingShard,
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
    pub(crate) install: BackendParticipantInstall,
}

#[derive(Clone, Debug)]
pub(crate) struct DecodedRuntimeFilterContribution {
    pub(crate) lifecycle: RuntimeFilterQueryLifecycleOptions,
    pub(crate) install: BackendParticipantInstall,
}

type CodecResult<T> = Result<T, ProtocolError>;

const CONTRIBUTION_DIGEST_DOMAIN: &[u8] =
    b"novarocks.query-lifecycle.runtime-filter-contribution.v1\0";

pub(crate) fn decode_runtime_filter_contribution(
    execution_id: QueryExecutionId,
    contribution: &RuntimeFilterContribution,
) -> Result<DecodedRuntimeFilterContribution, QueryLifecycleError> {
    let wire = contribution.as_proto();
    let request = filter::InstallRuntimeFilterDeploymentRequest {
        query_id: Some(common::UniqueId {
            hi: execution_id.query_id().high(),
            lo: execution_id.query_id().low(),
        }),
        deployment_epoch: execution_id.attempt_id().get(),
        participant_id: wire.participant_id,
        lifecycle: wire.lifecycle,
        install: wire.install.clone(),
    };
    let mut hasher = sha2::Sha256::new();
    hasher.update(CONTRIBUTION_DIGEST_DOMAIN);
    hasher.update(request.encode_to_vec());
    if hasher.finalize().as_slice() != contribution.digest() {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            "runtime filter contribution digest does not match install DTO",
        ));
    }
    let decoded = decode_participant_install(&request).map_err(|error| {
        QueryLifecycleError::new(QueryLifecycleErrorCode::InvalidManifest, error.to_string())
    })?;
    if decoded.query_id
        != UniqueId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        )
    {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            "runtime filter install query id does not match execution attempt",
        ));
    }
    if decoded.install.participant().deployment_epoch() != execution_id.attempt_id().get() {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            "runtime filter install epoch does not match query execution attempt",
        ));
    }
    if decoded.install.local_participant_id() != contribution.participant_id() {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            "runtime filter install participant does not match manifest contribution",
        ));
    }
    Ok(DecodedRuntimeFilterContribution {
        lifecycle: decoded.lifecycle,
        install: decoded.install,
    })
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, kind, detail)
}

fn contract_missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    error(path, ProtocolErrorKind::MissingField, detail)
}

fn contract_invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    error(path, ProtocolErrorKind::InvalidValue, detail)
}

fn contract_inconsistent(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    error(path, ProtocolErrorKind::InconsistentFields, detail)
}

fn contract_digest32(binding_id: u32, field: &str, bytes: &[u8]) -> Result<[u8; 32], String> {
    bytes.try_into().map_err(|_| format!(
        "native runtime-filter binding_id={binding_id} {field} must be exactly 32 bytes, got {}",
        bytes.len()
    ))
}

#[derive(Clone)]
struct DecodedChannelContract {
    execution: RuntimeFilterExecutionContract,
    reduction: RuntimeFilterReduction,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DecodedArtifactCapability {
    Membership,
    OrderedRange,
    EmptyDomain,
}

fn decode_runtime_filter_logical_domain_and_reduction(
    wire_type: Option<&common::TypeDesc>,
    wire_contract: Option<&plan::RuntimeFilterContract>,
    wire_reduction: Option<&plan::RuntimeFilterReductionContract>,
    path: FieldPath,
) -> CodecResult<DecodedChannelContract> {
    let type_path = path.clone().field("value_type");
    let wire_type = wire_type.ok_or_else(|| {
        contract_missing(
            type_path.clone(),
            "runtime filter deployment logical domain is contract_missing value type",
        )
    })?;
    let value_type =
        decode_type(wire_type).map_err(|detail| contract_invalid(type_path, detail))?;
    let contract = decode_contract(
        0,
        &value_type,
        wire_contract,
        path.clone().field("contract"),
    )?;
    let reduction = decode_reduction(0, &contract, wire_reduction, path.field("reduction"))?;
    Ok(DecodedChannelContract {
        execution: contract,
        reduction,
    })
}

fn decode_runtime_filter_activation(
    wire: Option<&plan::RuntimeFilterConsumerActivation>,
    path: FieldPath,
) -> CodecResult<ConsumerActivation> {
    let wire = wire.ok_or_else(|| {
        contract_missing(
            path.clone(),
            "contract_missing runtime filter consumer activation",
        )
    })?;
    match wire.kind.as_ref().ok_or_else(|| {
        contract_missing(
            path.clone().field("kind"),
            "contract_missing runtime filter consumer activation kind",
        )
    })? {
        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true) => {
            Ok(ConsumerActivation::BlockingSnapshot)
        }
        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(false) => {
            Err(contract_invalid(
                path.field("kind").field("blocking_snapshot"),
                "runtime filter blocking activation marker must be true",
            ))
        }
        plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(raw) => {
            let late_apply = match plan::RuntimeFilterLateApplyGranularity::try_from(*raw) {
                Ok(plan::RuntimeFilterLateApplyGranularity::Row) => {
                    RuntimeFilterLateApplyGranularity::Row
                }
                Ok(plan::RuntimeFilterLateApplyGranularity::Batch) => {
                    RuntimeFilterLateApplyGranularity::Batch
                }
                Ok(plan::RuntimeFilterLateApplyGranularity::RowGroup) => {
                    RuntimeFilterLateApplyGranularity::RowGroup
                }
                Ok(plan::RuntimeFilterLateApplyGranularity::Split) => {
                    RuntimeFilterLateApplyGranularity::Split
                }
                Ok(plan::RuntimeFilterLateApplyGranularity::File) => {
                    RuntimeFilterLateApplyGranularity::File
                }
                Ok(plan::RuntimeFilterLateApplyGranularity::Unspecified) | Err(_) => {
                    return Err(error(
                        path.field("kind").field("non_blocking_live"),
                        ProtocolErrorKind::InvalidEnum,
                        format!("contract_invalid runtime filter late-apply granularity={raw}"),
                    ));
                }
            };
            Ok(ConsumerActivation::NonBlockingLive { late_apply })
        }
    }
}

fn decode_contract(
    binding_id: u32,
    expression_type: &arrow::datatypes::DataType,
    wire: Option<&plan::RuntimeFilterContract>,
    path: FieldPath,
) -> CodecResult<RuntimeFilterExecutionContract> {
    let wire = wire.ok_or_else(|| {
        contract_missing(
            path.clone(),
            format!("native runtime-filter binding_id={binding_id} contract_missing contract"),
        )
    })?;
    let kind = wire.kind.as_ref().ok_or_else(|| {
        contract_missing(
            path.clone().field("kind"),
            format!("native runtime-filter binding_id={binding_id} contract_missing contract kind"),
        )
    })?;
    match kind {
        plan::runtime_filter_contract::Kind::Membership(membership) => {
            let path = path.field("membership");
            if membership.canonical_schema.is_empty() {
                return Err(contract_invalid(
                    path.clone().field("canonical_schema"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema is empty"
                    ),
                ));
            }
            let digest = contract_digest32(
                binding_id,
                "membership schema_digest",
                &membership.schema_digest,
            )
            .map_err(|detail| contract_invalid(path.clone().field("schema_digest"), detail))?;
            let schema = RuntimeFilterMembershipSchema::from_canonical(
                membership.canonical_schema.clone(),
                digest,
            )
            .map_err(|reason| {
                contract_invalid(
                    path.clone().field("canonical_schema"),
                    format!("native runtime-filter binding_id={binding_id} membership schema is noncanonical: {reason}"),
                )
            })?;
            let expected =
                RuntimeFilterMembershipSchema::new(expression_type, schema.null_semantics())
                    .map_err(|error| {
                        contract_invalid(path.clone().field("canonical_schema"), error.to_string())
                    })?;
            if expected.canonical_bytes() != schema.canonical_bytes() {
                return Err(contract_inconsistent(
                    path.field("canonical_schema"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema does not match expression type"
                    ),
                ));
            }
            Ok(RuntimeFilterExecutionContract::Membership(schema))
        }
        plan::runtime_filter_contract::Kind::Ordered(ordered) => {
            let path = path.field("ordered");
            if ordered.keys.len() != 1 {
                return Err(contract_invalid(
                    path.clone().field("keys"),
                    format!(
                        "native runtime-filter binding_id={binding_id} ordered contract must contain exactly one key, got {}",
                        ordered.keys.len()
                    ),
                ));
            }
            let mut keys = Vec::with_capacity(ordered.keys.len());
            for (index, key) in ordered.keys.iter().enumerate() {
                let key_path = path.clone().field("keys").index(index);
                let wire_type = key.r#type.as_ref().ok_or_else(|| {
                    contract_missing(
                        key_path.clone().field("type"),
                        format!(
                            "native runtime-filter binding_id={binding_id} ordered key type contract_missing"
                        ),
                    )
                })?;
                let data_type = decode_type(wire_type)
                    .map_err(|detail| contract_invalid(key_path.clone().field("type"), detail))?;
                let direction = match plan::RuntimeFilterSortDirection::try_from(key.direction) {
                    Ok(plan::RuntimeFilterSortDirection::Ascending) => {
                        contribution::RuntimeOrderSortDirection::Ascending
                    }
                    Ok(plan::RuntimeFilterSortDirection::Descending) => {
                        contribution::RuntimeOrderSortDirection::Descending
                    }
                    Ok(plan::RuntimeFilterSortDirection::Unspecified) | Err(_) => {
                        return Err(error(
                            key_path.clone().field("direction"),
                            ProtocolErrorKind::InvalidEnum,
                            format!(
                                "native runtime-filter binding_id={binding_id} contract_invalid sort direction={}",
                                key.direction
                            ),
                        ));
                    }
                };
                let null_order = match plan::RuntimeFilterNullOrder::try_from(key.null_order) {
                    Ok(plan::RuntimeFilterNullOrder::First) => {
                        contribution::RuntimeOrderNullOrder::First
                    }
                    Ok(plan::RuntimeFilterNullOrder::Last) => {
                        contribution::RuntimeOrderNullOrder::Last
                    }
                    Ok(plan::RuntimeFilterNullOrder::Unspecified) | Err(_) => {
                        return Err(error(
                            key_path.field("null_order"),
                            ProtocolErrorKind::InvalidEnum,
                            format!(
                                "native runtime-filter binding_id={binding_id} contract_invalid null order={}",
                                key.null_order
                            ),
                        ));
                    }
                };
                keys.push(contribution::RuntimeOrderKey::with_order(
                    data_type, direction, null_order,
                ));
            }
            if keys[0].data_type() != expression_type {
                return Err(contract_inconsistent(
                    path.clone().field("keys").index(0).field("type"),
                    format!(
                        "native runtime-filter binding_id={binding_id} ordered key type {:?} does not match expression type {:?}",
                        keys[0].data_type(),
                        expression_type
                    ),
                ));
            }
            let comparator =
                contract_digest32(binding_id, "comparator_digest", &ordered.comparator_digest)
                    .map_err(|detail| {
                        contract_invalid(path.clone().field("comparator_digest"), detail)
                    })?;
            let order_digest = contract_digest32(
                binding_id,
                "order_contract_digest",
                &ordered.order_contract_digest,
            )
            .map_err(|detail| {
                contract_invalid(path.clone().field("order_contract_digest"), detail)
            })?;
            Ok(RuntimeFilterExecutionContract::Ordered(
                std::sync::Arc::new(contribution::RuntimeOrderContract::from_frozen(
                    keys,
                    comparator,
                    order_digest,
                )),
            ))
        }
    }
}

fn decode_reduction(
    binding_id: u32,
    contract: &RuntimeFilterExecutionContract,
    wire: Option<&plan::RuntimeFilterReductionContract>,
    path: FieldPath,
) -> CodecResult<RuntimeFilterReduction> {
    let wire = wire.ok_or_else(|| {
        contract_missing(
            path.clone(),
            format!(
                "native runtime-filter binding_id={binding_id} contract_missing reduction contract"
            ),
        )
    })?;
    let kind = wire.kind.as_ref().ok_or_else(|| {
        contract_missing(
            path.clone().field("kind"),
            format!(
                "native runtime-filter binding_id={binding_id} contract_missing reduction kind"
            ),
        )
    })?;
    match kind {
        plan::runtime_filter_reduction_contract::Kind::SetUnion(true) => {
            if !matches!(contract, RuntimeFilterExecutionContract::Membership(_)) {
                return Err(contract_inconsistent(
                    path.field("kind"),
                    "SetUnion reduction requires a membership contract",
                ));
            }
            Ok(RuntimeFilterReduction::SetUnion)
        }
        plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(true) => {
            if !matches!(contract, RuntimeFilterExecutionContract::Ordered(_)) {
                return Err(contract_inconsistent(
                    path.field("kind"),
                    "TightenOrderedBound reduction requires an ordered contract",
                ));
            }
            Ok(RuntimeFilterReduction::TightenOrderedBound)
        }
        plan::runtime_filter_reduction_contract::Kind::SetUnion(false)
        | plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(false) => {
            Err(contract_invalid(
                path.field("kind"),
                format!(
                    "native runtime-filter binding_id={binding_id} reduction marker must be true"
                ),
            ))
        }
        plan::runtime_filter_reduction_contract::Kind::MergeTopkSummary(topk) => {
            let topk_path = path.field("kind").field("merge_topk_summary");
            if topk.k == 0 {
                return Err(contract_invalid(
                    topk_path.clone().field("k"),
                    format!("native runtime-filter binding_id={binding_id} TopK K must be nonzero"),
                ));
            }
            let digest =
                contract_digest32(binding_id, "TopK contract_digest", &topk.contract_digest)
                    .map_err(|detail| {
                        contract_invalid(topk_path.clone().field("contract_digest"), detail)
                    })?;
            let RuntimeFilterExecutionContract::Ordered(order) = contract else {
                return Err(contract_inconsistent(
                    topk_path.clone(),
                    format!(
                        "native runtime-filter binding_id={binding_id} TopK reduction requires ordered contract"
                    ),
                ));
            };
            if order.digest() != digest {
                return Err(contract_inconsistent(
                    topk_path.field("contract_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} TopK contract digest mismatch"
                    ),
                ));
            }
            Ok(RuntimeFilterReduction::MergeTopKSummary {
                k: topk.k,
                contract_digest: digest,
            })
        }
    }
}

fn decode_runtime_filter_completion(raw: i32, path: FieldPath) -> CodecResult<bool> {
    match plan::RuntimeFilterCompletionRequirement::try_from(raw) {
        Ok(plan::RuntimeFilterCompletionRequirement::ProducerClosed) => Ok(false),
        Ok(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen) => Ok(true),
        Ok(plan::RuntimeFilterCompletionRequirement::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("contract_invalid runtime filter completion requirement={raw}"),
        )),
    }
}

fn decode_runtime_filter_capability(
    raw: i32,
    path: FieldPath,
) -> CodecResult<DecodedArtifactCapability> {
    match plan::RuntimeFilterArtifactCapability::try_from(raw) {
        Ok(plan::RuntimeFilterArtifactCapability::Membership) => {
            Ok(DecodedArtifactCapability::Membership)
        }
        Ok(plan::RuntimeFilterArtifactCapability::OrderedRange) => {
            Ok(DecodedArtifactCapability::OrderedRange)
        }
        Ok(plan::RuntimeFilterArtifactCapability::EmptyDomain) => {
            Ok(DecodedArtifactCapability::EmptyDomain)
        }
        Ok(plan::RuntimeFilterArtifactCapability::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("contract_invalid runtime filter artifact capability={raw}"),
        )),
    }
}

fn codec_error(
    path: FieldPath,
    kind: ProtocolErrorKind,
    detail: impl Into<String>,
) -> ProtocolError {
    ProtocolError::new(path, kind, detail)
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

fn decode_unique_id(value: Option<&common::UniqueId>, path: FieldPath) -> CodecResult<UniqueId> {
    let value = value.ok_or_else(|| missing(path.clone(), "unique id is required"))?;
    let decoded = UniqueId::new(value.hi, value.lo);
    if decoded.high() == 0 && decoded.low() == 0 {
        return Err(invalid(path, "unique id must be nonzero"));
    }
    Ok(decoded)
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
    reject_zero(
        u64::from(request.participant_id),
        root.clone().field("participant_id"),
        "participant id",
    )?;
    let participant = BackendParticipantIdentity::new(query_id, request.deployment_epoch);
    let lifecycle = decode_lifecycle_options(request.lifecycle.as_ref())?;
    let wire = request.install.as_ref().ok_or_else(|| {
        missing(
            root.clone().field("install"),
            "participant install is required",
        )
    })?;
    let install = decode_install(
        wire,
        participant,
        request.participant_id,
        root.clone().field("install"),
    )?;
    validate_participant_install(&install)
        .map_err(|detail| invalid(root.field("install"), detail))?;
    Ok(DecodedRuntimeFilterParticipantInstall {
        query_id,
        lifecycle,
        install,
    })
}

fn decode_install(
    wire: &filter::RuntimeFilterParticipantInstall,
    participant: BackendParticipantIdentity,
    local_participant_id: u32,
    path: FieldPath,
) -> CodecResult<BackendParticipantInstall> {
    let mut channels = BTreeMap::new();
    let mut binding_ids = BTreeSet::new();
    let mut consumer_route_ids = BTreeSet::new();
    for (index, channel) in wire.core_channels.iter().enumerate() {
        let item_path = path.clone().field("core_channels").index(index);
        let decoded = decode_core_channel(channel, participant, item_path.clone())?;
        if channels.insert(decoded.channel_id(), decoded).is_some() {
            return Err(duplicate(
                item_path.field("channel_id"),
                "duplicate Backend channel id",
            ));
        }
        let decoded = channels
            .get(&RuntimeFilterChannelId::new(channel.channel_id))
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
    let mut routing_channels = Vec::new();
    for (index, channel) in wire.routing_channels.iter().enumerate() {
        let item_path = path.clone().field("routing_channels").index(index);
        routing_channels.push(decode_routing_channel(
            channel,
            local_participant_id,
            item_path.clone(),
        )?);
    }
    let routing = BackendRoutingShard::new(participant, local_participant_id, routing_channels)
        .map_err(|error| invalid(path.clone(), error.to_string()))?;
    BackendParticipantInstall::new(
        participant,
        local_participant_id,
        channels.into_values(),
        routing,
    )
    .map_err(|error| invalid(path, error.to_string()))
}

fn decode_core_channel(
    wire: &filter::RuntimeFilterChannelDeployment,
    participant: BackendParticipantIdentity,
    path: FieldPath,
) -> CodecResult<BackendChannelInstall> {
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
    let decoded_contract = decode_runtime_filter_logical_domain_and_reduction(
        logical.value_type.as_ref(),
        logical.contract.as_ref(),
        wire.reduction.as_ref(),
        path.clone(),
    )?;
    let lifecycle = match filter::RuntimeFilterLifecycle::try_from(wire.lifecycle) {
        Ok(filter::RuntimeFilterLifecycle::CompleteOnce) => BackendChannelLifecycle::CompleteOnce,
        Ok(filter::RuntimeFilterLifecycle::MonotonicUpdates) => {
            BackendChannelLifecycle::MonotonicUpdates
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
        let contribution = plan::RuntimeFilterContributionKind::try_from(raw).map_err(|_| {
            error(
                item_path.clone(),
                ProtocolErrorKind::InvalidEnum,
                format!("contract_invalid runtime filter contribution kind={raw}"),
            )
        })?;
        if contribution == plan::RuntimeFilterContributionKind::Unspecified {
            return Err(error(
                item_path,
                ProtocolErrorKind::InvalidEnum,
                "contract_invalid runtime filter contribution kind=unspecified",
            ));
        }
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
    let fenced_final = decode_runtime_filter_completion(
        wire.completion_requirement,
        path.clone().field("completion_requirement"),
    )?;
    let policy_wire = wire
        .policy
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("policy"), "policy is required"))?;
    validate_runtime_filter_policy(policy_wire)?;
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
    let channel_id = RuntimeFilterChannelId::new(wire.channel_id);
    let producer_kind = producer_kind_for_matrix(
        &decoded_contract.execution,
        decoded_contract.reduction,
        lifecycle,
        fenced_final,
        &contributions,
        path.clone(),
    )?;
    let mut producers = Vec::new();
    for (index, producer) in wire.producers.iter().enumerate() {
        let item_path = path.clone().field("producers").index(index);
        producers.push(decode_producer(
            producer,
            channel_id,
            decoded_contract.execution.clone(),
            decoded_contract.reduction,
            producer_kind,
            usize::try_from(policy_wire.max_contribution_bytes).map_err(|_| {
                invalid(
                    item_path.clone().field("policy"),
                    "max contribution bytes does not fit usize",
                )
            })?,
            item_path,
        )?);
    }
    let mut consumers = Vec::new();
    for (index, consumer) in wire.consumers.iter().enumerate() {
        let item_path = path.clone().field("consumers").index(index);
        consumers.push(decode_consumer(
            consumer,
            channel_id,
            decoded_contract.execution.clone(),
            decoded_contract.reduction,
            item_path,
        )?);
    }
    let mut groups = Vec::new();
    for (index, group) in wire.outbound_materialization_groups.iter().enumerate() {
        let item_path = path
            .clone()
            .field("outbound_materialization_groups")
            .index(index);
        groups.push(decode_outbound_materialization_group(group, item_path)?);
    }
    let _ = participant;
    BackendChannelInstall::new(
        channel_id,
        decoded_contract.execution,
        lifecycle,
        availability,
        terminal,
        materialization,
        allocatable_usize(
            budget.max_reducer_bytes,
            path.clone().field("core_budget").field("max_reducer_bytes"),
            "core reducer budget",
        )?,
        allocatable_usize(
            policy_wire.max_artifact_bytes,
            path.clone().field("policy").field("max_artifact_bytes"),
            "max artifact bytes",
        )?,
        producers,
        consumers,
        groups,
    )
    .map_err(|error| invalid(path, error.to_string()))
}

fn decode_outbound_materialization_group(
    wire: &filter::RuntimeFilterOutboundMaterializationGroup,
    path: FieldPath,
) -> CodecResult<BackendOutboundMaterializationGroup> {
    let owner = match filter::RuntimeFilterOutboundMaterializationOwner::try_from(wire.owner) {
        Ok(filter::RuntimeFilterOutboundMaterializationOwner::DirectSource) => {
            BackendMaterializationOwner::DirectSource
        }
        Ok(filter::RuntimeFilterOutboundMaterializationOwner::Aggregator) => {
            BackendMaterializationOwner::Aggregator
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
        if !routes.insert(BackendRouteEdgeId::new(u64::from(raw))) {
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
    BackendOutboundMaterializationGroup::new(owner, profile, routes)
        .map_err(|error| invalid(path, error.to_string()))
}

fn decode_coverage(
    wire: Option<&filter::RuntimeFilterCoverage>,
    path: FieldPath,
) -> CodecResult<BackendCoverage> {
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
            BackendCoverage::witness(BackendCoverageWitnessId::new(*raw))
        }
        filter::runtime_filter_coverage::Kind::AllOf(all) => BackendCoverage::all_of(
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
        )
        .map_err(|error| invalid(path.clone(), error.to_string()))?,
        filter::runtime_filter_coverage::Kind::AnyOf(any) => BackendCoverage::any_of(
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
        )
        .map_err(|error| invalid(path.clone(), error.to_string()))?,
    };
    Ok(coverage)
}

fn decode_materialization_policy(
    wire: Option<&filter::RuntimeFilterMaterializationPolicy>,
    path: FieldPath,
) -> CodecResult<BackendMaterializationPolicy> {
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
    let retained = allocatable_usize(
        wire.max_total_retained_bytes,
        path.clone().field("max_total_retained_bytes"),
        "materialization retained budget",
    )?;
    let scratch = allocatable_usize(
        wire.max_scratch_bytes_per_job,
        path.clone().field("max_scratch_bytes_per_job"),
        "materialization scratch budget",
    )?;
    let bloom_bits_per_key = u32::try_from(wire.bloom_bits_per_key).map_err(|_| {
        codec_error(
            path.clone().field("bloom_bits_per_key"),
            ProtocolErrorKind::OutOfRange,
            "bloom bits per key does not fit u32",
        )
    })?;
    let bloom_hash_count = wire.bloom_hash_count;
    BackendMaterializationPolicy::new(
        bloom_bits_per_key,
        bloom_hash_count,
        wire.bloom_seed,
        version,
        retained,
        scratch,
        jobs,
    )
    .map_err(|error| invalid(path, format!("invalid materialization policy: {error:?}")))
}

fn decode_producer(
    wire: &filter::RuntimeFilterProducerDeployment,
    channel_id: RuntimeFilterChannelId,
    contract: RuntimeFilterExecutionContract,
    reduction: RuntimeFilterReduction,
    kind: RuntimeFilterProducerKind,
    max_contribution_bytes: usize,
    path: FieldPath,
) -> CodecResult<BackendProducerInstall> {
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
        path.clone().field("expected_fragment_instances"),
    )?;
    let binding_id = RuntimeFilterBindingId::new(wire.binding_id);
    let producer = match kind {
        RuntimeFilterProducerKind::Membership => {
            RuntimeFilterProducerContract::membership(binding_id, channel_id, contract)
        }
        RuntimeFilterProducerKind::OrderedBound => {
            RuntimeFilterProducerContract::ordered_bound(binding_id, channel_id, contract)
        }
        RuntimeFilterProducerKind::TopKSummary => {
            let RuntimeFilterReduction::MergeTopKSummary { k, .. } = reduction else {
                unreachable!("validated producer kind carries TopK reduction")
            };
            RuntimeFilterProducerContract::top_k_summary(binding_id, channel_id, k, contract)
        }
        RuntimeFilterProducerKind::FinalDomain => {
            RuntimeFilterProducerContract::final_domain(binding_id, channel_id, contract)
        }
    }
    .map_err(|error| contract_inconsistent(path.clone(), error.to_string()))?;
    BackendProducerInstall::new(
        producer,
        BackendCoverageWitnessId::new(wire.coverage_witness_id),
        instances,
        max_contribution_bytes,
    )
    .map_err(|error| invalid(path, error.to_string()))
}

fn decode_consumer(
    wire: &filter::RuntimeFilterConsumerDeployment,
    channel_id: RuntimeFilterChannelId,
    contract: RuntimeFilterExecutionContract,
    reduction: RuntimeFilterReduction,
    path: FieldPath,
) -> CodecResult<BackendConsumerInstall> {
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
        if !routes.insert(BackendRouteEdgeId::new(u64::from(raw))) {
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
    validate_consumer_capabilities(&contract, &capabilities, &profile, path.clone())?;
    let binding_id = RuntimeFilterBindingId::new(wire.binding_id);
    let consumer = match (contract, reduction, activation) {
        (
            membership @ RuntimeFilterExecutionContract::Membership(_),
            RuntimeFilterReduction::SetUnion,
            ConsumerActivation::BlockingSnapshot,
        ) => RuntimeFilterConsumerContract::membership_blocking(binding_id, channel_id, membership),
        (
            membership @ RuntimeFilterExecutionContract::Membership(_),
            RuntimeFilterReduction::SetUnion,
            ConsumerActivation::NonBlockingLive { late_apply },
        ) => RuntimeFilterConsumerContract::membership_live(
            binding_id, channel_id, late_apply, membership,
        ),
        (
            ordered @ RuntimeFilterExecutionContract::Ordered(_),
            RuntimeFilterReduction::TightenOrderedBound,
            ConsumerActivation::NonBlockingLive { late_apply },
        ) => {
            RuntimeFilterConsumerContract::ordered_live(binding_id, channel_id, late_apply, ordered)
        }
        (
            ordered @ RuntimeFilterExecutionContract::Ordered(_),
            RuntimeFilterReduction::MergeTopKSummary { k, .. },
            ConsumerActivation::NonBlockingLive { late_apply },
        ) => RuntimeFilterConsumerContract::top_k_live(
            binding_id, channel_id, late_apply, k, ordered,
        ),
        _ => {
            return Err(contract_inconsistent(
                path.clone(),
                "consumer activation/reduction does not match execution contract",
            ));
        }
    }
    .map_err(|error| contract_inconsistent(path.clone(), error.to_string()))?;
    BackendConsumerInstall::new(consumer, profile, routes, instances)
        .map_err(|error| invalid(path, error.to_string()))
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
        .transpose()?;
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

fn producer_kind_for_matrix(
    contract: &RuntimeFilterExecutionContract,
    reduction: RuntimeFilterReduction,
    lifecycle: BackendChannelLifecycle,
    fenced_final: bool,
    contributions: &BTreeSet<plan::RuntimeFilterContributionKind>,
    path: FieldPath,
) -> CodecResult<RuntimeFilterProducerKind> {
    use plan::RuntimeFilterContributionKind as Kind;
    let expected = if fenced_final {
        if !matches!(contract, RuntimeFilterExecutionContract::Membership(_))
            || reduction != RuntimeFilterReduction::SetUnion
            || lifecycle != BackendChannelLifecycle::CompleteOnce
        {
            return Err(contract_inconsistent(
                path,
                "fenced final-domain channel requires CompleteOnce membership SetUnion",
            ));
        }
        (
            RuntimeFilterProducerKind::FinalDomain,
            BTreeSet::from([Kind::FinalDomainShard, Kind::ProducerClosed]),
        )
    } else {
        match (contract, reduction, lifecycle) {
            (
                RuntimeFilterExecutionContract::Membership(_),
                RuntimeFilterReduction::SetUnion,
                BackendChannelLifecycle::CompleteOnce,
            )
            | (
                RuntimeFilterExecutionContract::Membership(_),
                RuntimeFilterReduction::SetUnion,
                BackendChannelLifecycle::MonotonicUpdates,
            ) => (
                RuntimeFilterProducerKind::Membership,
                BTreeSet::from([Kind::ValueDomainDelta, Kind::ProducerClosed]),
            ),
            (
                RuntimeFilterExecutionContract::Ordered(_),
                RuntimeFilterReduction::TightenOrderedBound,
                BackendChannelLifecycle::MonotonicUpdates,
            ) => (
                RuntimeFilterProducerKind::OrderedBound,
                BTreeSet::from([Kind::OrderedBoundUpdate, Kind::ProducerClosed]),
            ),
            (
                RuntimeFilterExecutionContract::Ordered(_),
                RuntimeFilterReduction::MergeTopKSummary { .. },
                BackendChannelLifecycle::MonotonicUpdates,
            ) => (
                RuntimeFilterProducerKind::TopKSummary,
                BTreeSet::from([Kind::TopkSummary, Kind::ProducerClosed]),
            ),
            _ => {
                return Err(contract_inconsistent(
                    path,
                    "channel lifecycle, contract, and reduction do not form a legal producer matrix",
                ));
            }
        }
    };
    if contributions != &expected.1 {
        return Err(contract_inconsistent(
            path.field("allowed_contribution_kinds"),
            "allowed contribution kinds do not exactly match the sealed producer contract",
        ));
    }
    Ok(expected.0)
}

fn validate_consumer_capabilities(
    contract: &RuntimeFilterExecutionContract,
    capabilities: &BTreeSet<DecodedArtifactCapability>,
    profile: &ConsumerArtifactProfile,
    path: FieldPath,
) -> CodecResult<()> {
    match contract {
        RuntimeFilterExecutionContract::Membership(schema) => {
            if !capabilities.contains(&DecodedArtifactCapability::Membership)
                || !capabilities.contains(&DecodedArtifactCapability::EmptyDomain)
                || !profile.accepts(ArtifactKind::EmptyDomain)
            {
                return Err(contract_inconsistent(
                    path,
                    "membership consumer requires Membership and EmptyDomain capability/profile support",
                ));
            }
            if schema.null_semantics() == RuntimeFilterNullSemantics::NullSafeEqual
                && !profile.accepts(ArtifactKind::ValueSet)
            {
                return Err(contract_inconsistent(
                    path,
                    "null-safe membership consumer requires ValueSet artifact support",
                ));
            }
        }
        RuntimeFilterExecutionContract::Ordered(order) => {
            if capabilities != &BTreeSet::from([DecodedArtifactCapability::OrderedRange])
                || !profile.accepts(ArtifactKind::Range)
                || profile.order_contract_digest() != Some(order.digest())
            {
                return Err(contract_inconsistent(
                    path,
                    "ordered consumer requires exact OrderedRange capability and matching Range profile",
                ));
            }
        }
    }
    Ok(())
}

fn decode_routing_channel(
    wire: &filter::RuntimeFilterChannelRoutingView,
    local_participant: u32,
    path: FieldPath,
) -> CodecResult<BackendRoutingChannel> {
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
                (RuntimeFilterBindingId::new(route.binding_id), instance),
                route.participant_id,
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
                RuntimeFilterChannelId::new(wire.channel_id),
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
                RuntimeFilterChannelId::new(wire.channel_id),
                path.clone().field("outbound_edges").index(index),
            )
        })
        .collect::<CodecResult<Vec<_>>>()?;
    let channel = BackendRoutingChannel::new(
        RuntimeFilterChannelId::new(wire.channel_id),
        roles,
        inbound,
        outbound,
        producer_instances,
    )
    .map_err(|error| invalid(path.clone(), error.to_string()))?;
    for edge in wire.inbound_edges.iter() {
        let target = edge.target.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("inbound_edges"),
                "route target is required",
            )
        })?;
        if target.participant_id != local_participant {
            return Err(inconsistent(
                path.clone().field("inbound_edges"),
                "inbound edge target does not match request participant",
            ));
        }
    }
    for edge in wire.outbound_edges.iter() {
        let source = edge.source.as_ref().ok_or_else(|| {
            missing(
                path.clone().field("outbound_edges"),
                "route source is required",
            )
        })?;
        if source.participant_id != local_participant {
            return Err(inconsistent(
                path.clone().field("outbound_edges"),
                "outbound edge source does not match request participant",
            ));
        }
    }
    Ok(channel)
}

fn decode_route_role(
    wire: &filter::RuntimeFilterRouteRole,
    path: FieldPath,
) -> CodecResult<BackendRouteRole> {
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
            Ok(BackendRouteRole::Producer(RuntimeFilterBindingId::new(
                *raw,
            )))
        }
        filter::runtime_filter_route_role::Role::Aggregator(true) => {
            Ok(BackendRouteRole::Aggregator)
        }
        filter::runtime_filter_route_role::Role::Relay(true) => Ok(BackendRouteRole::Relay),
        filter::runtime_filter_route_role::Role::ConsumerBindingId(raw) => {
            reject_zero(
                u64::from(*raw),
                path.field("consumer_binding_id"),
                "consumer binding id",
            )?;
            Ok(BackendRouteRole::Consumer(RuntimeFilterBindingId::new(
                *raw,
            )))
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

fn decode_routing_edge(
    wire: &filter::RuntimeFilterRoutingEdgeView,
    channel_id: RuntimeFilterChannelId,
    path: FieldPath,
) -> CodecResult<BackendRoutingEdge> {
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
    let _ = channel_id;
    BackendRoutingEdge::new(
        BackendRouteEdgeId::new(u64::from(wire.route_edge_id)),
        source,
        target,
        peer,
        allowed,
    )
    .map_err(|error| invalid(path, error.to_string()))
}

fn decode_route_endpoint(
    wire: Option<&filter::RuntimeFilterRouteEndpointView>,
    path: FieldPath,
) -> CodecResult<BackendRouteEndpoint> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "route endpoint is required"))?;
    reject_zero(
        u64::from(wire.participant_id),
        path.clone().field("participant_id"),
        "route participant id",
    )?;
    BackendRouteEndpoint::new(
        wire.participant_id,
        decode_route_role(
            wire.role.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("role"),
                    "route endpoint role is required",
                )
            })?,
            path.clone().field("role"),
        )?,
    )
    .map_err(|error| invalid(path, error.to_string()))
}

fn decode_route_peer(
    wire: Option<&filter::RuntimeFilterRoutePeer>,
    path: FieldPath,
) -> CodecResult<BackendRoutePeer> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "route peer is required"))?;
    match wire
        .peer
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("peer"), "route peer kind is required"))?
    {
        filter::runtime_filter_route_peer::Peer::Loopback(true) => Ok(BackendRoutePeer::Loopback),
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
            Ok(BackendRoutePeer::Remote {
                participant_id: remote.participant_id,
                endpoint: RuntimeEndpoint::parse(&remote.endpoint)
                    .map_err(|error| invalid(path.field("remote").field("endpoint"), error))?,
            })
        }
    }
}

fn decode_envelope_kind(raw: i32, path: FieldPath) -> CodecResult<BackendEnvelopeKind> {
    match filter::RuntimeFilterEnvelopeKind::try_from(raw) {
        Ok(filter::RuntimeFilterEnvelopeKind::Contribution) => {
            Ok(BackendEnvelopeKind::Contribution)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Artifact) => Ok(BackendEnvelopeKind::Artifact),
        Ok(filter::RuntimeFilterEnvelopeKind::ProducerClosed) => {
            Ok(BackendEnvelopeKind::ProducerClosed)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::ProducerUnavailable) => {
            Ok(BackendEnvelopeKind::ProducerUnavailable)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Unavailable) => Ok(BackendEnvelopeKind::Unavailable),
        Ok(filter::RuntimeFilterEnvelopeKind::Ack) => Ok(BackendEnvelopeKind::Ack),
        Ok(filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact) => {
            Ok(BackendEnvelopeKind::CompletedWithoutArtifact)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::DegradedLogical) => {
            Ok(BackendEnvelopeKind::DegradedLogical)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::FinalArtifact) => {
            Ok(BackendEnvelopeKind::FinalArtifact)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Unspecified) | Err(_) => Err(codec_error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("invalid runtime filter envelope kind={raw}"),
        )),
    }
}

const MAX_ARTIFACT_BYTES: u64 = 1 << 30;
const MAX_DEADLINE_MS: u64 = 86_400_000;
const MAX_RETRIES: u32 = 100;

fn validate_runtime_filter_policy(
    policy: &filter::RuntimeFilterPolicyRequirement,
) -> CodecResult<()> {
    if policy.max_contribution_bytes == 0
        || policy.max_artifact_bytes == 0
        || policy.deadline_ms == 0
        || policy.max_retries == 0
    {
        return Err(invalid(
            FieldPath::root("install_runtime_filter_deployment_request").field("policy"),
            "runtime filter policy fields must be non-zero",
        ));
    }
    if policy.max_contribution_bytes > policy.max_artifact_bytes
        || policy.max_artifact_bytes > MAX_ARTIFACT_BYTES
        || policy.deadline_ms > MAX_DEADLINE_MS
        || policy.max_retries > MAX_RETRIES
    {
        return Err(invalid(
            FieldPath::root("install_runtime_filter_deployment_request").field("policy"),
            "runtime filter policy exceeds its frozen bounds",
        ));
    }
    Ok(())
}

pub(crate) fn validate_participant_install(
    install: &BackendParticipantInstall,
) -> Result<(), String> {
    if install.local_participant_id() == 0 {
        return Err("participant id must be non-zero".to_string());
    }
    let mut bindings = BTreeSet::new();
    let mut routes = BTreeSet::new();
    for channel in install.channels().values() {
        let producer_witnesses = channel
            .producers()
            .values()
            .map(|producer| producer.coverage_witness())
            .collect::<BTreeSet<_>>();
        if producer_witnesses.len() != channel.producers().len() {
            return Err("producer coverage witness is duplicated within a channel".to_string());
        }
        if !producer_witnesses.is_subset(&channel.availability_coverage().witnesses()) {
            return Err(
                "availability coverage must reference every local producer witness".to_string(),
            );
        }
        for binding in channel.producers().keys().chain(channel.consumers().keys()) {
            if !bindings.insert(*binding) {
                return Err("binding id is duplicated across Backend install".to_string());
            }
        }
        for consumer in channel.consumers().values() {
            for route in consumer.route_edge_ids() {
                if !routes.insert(*route) {
                    return Err(
                        "consumer route edge is duplicated across Backend install".to_string()
                    );
                }
            }
        }
        channel
            .materialization_policy()
            .aggregate_scratch_bytes()
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::{
        CONTRIBUTION_DIGEST_DOMAIN, decode_runtime_filter_contribution,
        validate_participant_install,
    };
    use crate::runtime_filter::{
        domain::{
            BackendChannelInstall, BackendChannelLifecycle, BackendMaterializationPolicy,
            BackendParticipantInstall, BackendRoutingShard,
        },
        test_support::BackendRuntimeFilterFixture,
    };
    use novarocks_proto::{
        common, filter,
        lifecycle::{AttemptId, QueryExecutionId, RuntimeFilterContribution},
        novarocks as proto_novarocks,
    };
    use novarocks_types::QueryId;
    use prost::Message;
    use sha2::Digest;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(0x5246_4f34, 7),
            AttemptId::new(3).expect("nonzero attempt"),
        )
        .expect("nonzero execution id")
    }

    fn valid_empty_contribution(execution_id: QueryExecutionId) -> RuntimeFilterContribution {
        let lifecycle = filter::RuntimeFilterQueryLifecycleOptions {
            delivery_expire_ms: 1,
            query_expire_ms: 1,
            transport_retry_interval_ms: 1,
            transport_max_attempts: 1,
            transport_deadline_ms: 1,
            transport_max_pending_entries: 1,
            transport_max_pending_bytes: 1,
        };
        let install = filter::RuntimeFilterParticipantInstall::default();
        let envelope = filter::InstallRuntimeFilterDeploymentRequest {
            query_id: Some(common::UniqueId {
                hi: execution_id.query_id().high(),
                lo: execution_id.query_id().low(),
            }),
            deployment_epoch: execution_id.attempt_id().get(),
            participant_id: 3,
            lifecycle: Some(lifecycle),
            install: Some(install.clone()),
        };
        let mut digest = sha2::Sha256::new();
        digest.update(CONTRIBUTION_DIGEST_DOMAIN);
        digest.update(envelope.encode_to_vec());
        RuntimeFilterContribution::parse(proto_novarocks::RuntimeFilterContribution {
            participant_id: 3,
            lifecycle: Some(lifecycle),
            install: Some(install),
            contribution_digest: digest.finalize().to_vec(),
        })
        .expect("valid opaque contribution fixture")
    }

    #[test]
    fn decodes_backend_participant_domain_only_after_digest_validation() {
        let execution_id = execution_id();
        let contribution = valid_empty_contribution(execution_id);

        let decoded = decode_runtime_filter_contribution(execution_id, &contribution)
            .expect("backend decodes the valid participant install");

        assert_eq!(decoded.install.participant().deployment_epoch(), 3);
        assert_eq!(decoded.install.local_participant_id(), 3);
        assert!(decoded.install.channels().is_empty());
    }

    #[test]
    fn rejects_bad_digest_before_constructing_participant_domain() {
        let execution_id = execution_id();
        let contribution = valid_empty_contribution(execution_id);
        let mut wire = contribution.as_proto().clone();
        wire.contribution_digest[0] ^= 0x01;
        let malformed = RuntimeFilterContribution::parse(wire)
            .expect("length remains a generic carrier invariant");

        let error = decode_runtime_filter_contribution(execution_id, &malformed)
            .expect_err("bad digest is rejected before service installation");
        assert_eq!(
            error.to_string(),
            "InvalidManifest: runtime filter contribution digest does not match install DTO"
        );
    }

    #[test]
    fn accepts_global_coverage_on_participant_without_local_producer() {
        let fixture = BackendRuntimeFilterFixture::membership();
        let producer = fixture.producer_contract();
        let channel = BackendChannelInstall::new(
            producer.channel_id(),
            producer.contract().clone(),
            BackendChannelLifecycle::CompleteOnce,
            fixture.coverage(),
            fixture.coverage(),
            BackendMaterializationPolicy::new(8, 3, 17, 1, 4096, 1024, 1)
                .expect("valid materialization policy"),
            4096,
            4096,
            [],
            [],
            [],
        )
        .expect("valid participant-local channel projection");
        let routing = BackendRoutingShard::new(fixture.identity(), 1, [])
            .expect("valid empty local routing projection");
        let install = BackendParticipantInstall::new(fixture.identity(), 1, [channel], routing)
            .expect("valid participant install");

        validate_participant_install(&install)
            .expect("global coverage may include witnesses owned by remote participants");
    }
}
