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

//! Native runtime-filter install decoder.
//!
//! Core carries an opaque contribution DTO. This module is the only native
//! boundary that interprets its lifecycle/install/routing semantics and builds
//! the participant-local install consumed by the Worker service.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::runtime_filter::{
    ConsumerActivation, RuntimeFilterBindingId, RuntimeFilterChannelId,
    RuntimeFilterConsumerContract, RuntimeFilterExecutionContract,
    RuntimeFilterLateApplyGranularity, RuntimeFilterNullSemantics, RuntimeFilterProducerContract,
    RuntimeFilterProducerKind, RuntimeFilterReduction, contribution,
};
use novarocks_proto_codec::lifecycle::QueryExecutionId;
#[cfg(any(test, novarocks_dev_controls))]
use novarocks_proto_codec::lifecycle::RuntimeFilterContribution;
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{common, filter, plan};
use novarocks_types::{BackendProcessId, UniqueId};

use novarocks_worker::runtime_filter::domain::{
    BackendChannelInstall, BackendChannelLifecycle, BackendConsumerInstall, BackendCoverage,
    BackendCoverageWitnessId, BackendEnvelopeKind, BackendFrontendFeedbackPublication,
    BackendMaterializationOwner, BackendMaterializationPolicy, BackendOutboundMaterializationGroup,
    BackendParticipantIdentity, BackendParticipantInstall, BackendProducerInstall,
    BackendRouteEdgeId, BackendRouteEndpoint, BackendRoutePeer, BackendRouteRole,
    BackendRoutingChannel, BackendRoutingEdge, BackendRoutingShard,
};
use novarocks_worker::{
    RuntimeFilterContractError,
    runtime_filter::artifact::{ArtifactKind, ConsumerArtifactProfile, HashContractDigest},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterQueryLifecycleOptions {
    pub delivery_expire: Duration,
    pub query_expire: Duration,
    pub transport_retry_interval: Duration,
    pub transport_max_attempts: u32,
    pub transport_deadline: Duration,
    pub transport_max_pending_entries: usize,
    pub transport_max_pending_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedRuntimeFilterParticipantInstall {
    pub query_id: UniqueId,
    pub lifecycle: RuntimeFilterQueryLifecycleOptions,
    pub install: BackendParticipantInstall,
}

#[derive(Clone, Debug)]
pub struct DecodedRuntimeFilterContribution {
    pub lifecycle: RuntimeFilterQueryLifecycleOptions,
    pub install: BackendParticipantInstall,
}

type CodecResult<T> = Result<T, ProtocolError>;

use crate::native_type_construction::{NativeTypeConstruction, NativeTypeConstructionError};
use crate::native_type_decode::{
    NativeTypeDecodeError, NativeTypeDecodeResources, NativeTypeTableDecoder, clone_type,
    decode_type_owner, resolve_type,
};
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
use std::alloc::Layout;

type NativeResult<T> = Result<T, NativeTypeDecodeError>;
enum InstallTypeMode<'borrow, 'creator> {
    Strong(&'borrow mut NativeTypeDecodeResources<'creator>),
}

#[cfg(any(test, novarocks_dev_controls))]
fn with_fixture_resources<T>(
    operation: impl FnOnce(&mut NativeTypeDecodeResources<'_>) -> NativeResult<T>,
) -> NativeResult<T> {
    struct Control;
    impl novarocks_type_contract::PureCompileControl for Control {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    let tracker =
        novarocks_execution::runtime::mem_tracker::MemTracker::new_root("runtime-filter fixture");
    let creation = NativeTypeConstruction::try_with_control(tracker, &Control)?;
    let mut resources = NativeTypeDecodeResources::try_new(&creation)?;
    let result = operation(&mut resources)?;
    resources.finish()?;
    Ok(result)
}

#[cfg(any(test, novarocks_dev_controls))]
fn fixture_protocol(error: NativeTypeDecodeError) -> ProtocolError {
    match error {
        NativeTypeDecodeError::Protocol(error) => error,
        error => contract_invalid(FieldPath::root("runtime_filter"), error.to_string()),
    }
}

/// Rebuild the participant-local install envelope from the manifest
/// contribution and the admitted execution identity, then decode it.
///
/// The contribution carries content only. Its integrity is covered by the
/// enclosing participant manifest, whose identity both roles derive by
/// descriptor traversal and reconcile across `InitQueryResponse`; the
/// execution identity used here is the admitted one, not a restated copy.
/// Every structural and cross-field rejection below therefore still runs on
/// the exact bytes that were admitted.
#[cfg(any(test, novarocks_dev_controls))]
pub fn decode_runtime_filter_contribution(
    execution_id: QueryExecutionId,
    contribution: &RuntimeFilterContribution,
) -> Result<DecodedRuntimeFilterContribution, RuntimeFilterContractError> {
    with_fixture_resources(|resources| {
        let wire = contribution.as_proto();
        let decoded = decode_participant_parts(
            UniqueId::new(
                execution_id.query_id().high(),
                execution_id.query_id().low(),
            ),
            execution_id.attempt_id().get(),
            wire.participant_id,
            wire.lifecycle.as_ref(),
            wire.install.as_ref(),
            &mut InstallTypeMode::Strong(resources),
        )?;
        Ok(DecodedRuntimeFilterContribution {
            lifecycle: decoded.lifecycle,
            install: decoded.install,
        })
    })
    .map_err(|error: NativeTypeDecodeError| {
        RuntimeFilterContractError::invalid_contract(error.to_string())
    })
}

/// Borrow the admitted contribution directly; no generated install DTO or
/// lifecycle manifest is cloned before the actual BE construction observer.
pub(crate) fn decode_runtime_filter_contribution_with_type_creator(
    execution_id: QueryExecutionId,
    contribution: &novarocks_proto_models::novarocks::RuntimeFilterContribution,
    creator: &NativeTypeConstruction<'_>,
) -> NativeResult<DecodedRuntimeFilterContribution> {
    let mut resources = NativeTypeDecodeResources::try_new(creator)?;
    let decoded = decode_participant_parts(
        UniqueId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        ),
        execution_id.attempt_id().get(),
        contribution.participant_id,
        contribution.lifecycle.as_ref(),
        contribution.install.as_ref(),
        &mut InstallTypeMode::Strong(&mut resources),
    )?;
    resources.finish()?;
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
    mode: &mut InstallTypeMode<'_, '_>,
) -> NativeResult<DecodedChannelContract> {
    let InstallTypeMode::Strong(resources) = mode;
    resources
        .work
        .step()
        .map_err(NativeTypeConstructionError::Control)?;
    let wire_contract = wire_contract.ok_or_else(|| {
        contract_missing(
            path.clone().field("contract"),
            "runtime filter contract is required",
        )
    })?;
    let outer = wire_type.ok_or_else(|| {
        contract_missing(
            path.clone().field("value_type"),
            "runtime filter value reference is required",
        )
    })?;
    let contract =
        decode_typed_contract_domain(0, wire_contract, Some(outer), path.clone(), resources)?;
    let reduction = decode_reduction(0, &contract, wire_reduction, path.field("reduction"))?;
    Ok(DecodedChannelContract {
        execution: contract,
        reduction,
    })
}

/// Decode the RF namespace under its actual creator. Fragment endpoint refs
/// belong to a different owner and are compared only after this domain exists.
pub(crate) fn decode_typed_contract_domain(
    binding_id: u32,
    wire_contract: &plan::RuntimeFilterContract,
    outer: Option<&common::TypeDesc>,
    path: FieldPath,
    resources: &mut NativeTypeDecodeResources<'_>,
) -> NativeResult<RuntimeFilterExecutionContract> {
    let domain_path = path.clone().field("contract").field("logical_domain");
    let domain = wire_contract.logical_domain.as_ref().ok_or_else(|| {
        contract_missing(
            domain_path.clone(),
            "runtime filter requires its independent complete type owner",
        )
    })?;
    let table = domain.execution_types.as_ref().ok_or_else(|| {
        contract_missing(
            domain_path.clone().field("execution_types"),
            "runtime filter type owner is required",
        )
    })?;
    let desc = domain.value_type.as_ref().ok_or_else(|| {
        contract_missing(
            domain_path.clone().field("value_type"),
            "runtime filter complete value reference is required",
        )
    })?;
    let owner = decode_type_owner(
        table,
        domain_path.clone().field("execution_types"),
        resources,
    )?;
    let types = owner.execution;
    let source = resolve_type(
        &types,
        desc,
        domain_path.clone().field("value_type"),
        resources,
    )?;
    // The deployment's redundant reference must resolve in this same owner.
    let mut decoder = NativeTypeTableDecoder {
        types: &types,
        resources,
    };
    if let Some(outer) = outer {
        let outer_source = resolve_type(
            &types,
            outer,
            path.clone().field("value_type"),
            decoder.resources,
        )?;
        if !decoder.bindings_match(source.typed_binding(), outer_source.typed_binding(), false)? {
            return Err(contract_inconsistent(
                path.clone().field("value_type"),
                "runtime filter value reference differs from its declared logical domain",
            )
            .into());
        }
    }
    let contract = decode_contract_with_types(
        binding_id,
        source.data_type(),
        source,
        Some(wire_contract),
        path.clone().field("contract"),
        &mut decoder,
    )?;
    Ok(contract)
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

#[cfg(test)]
fn decode_contract(
    binding_id: u32,
    expression_type: &arrow::datatypes::DataType,
    wire_contract: Option<&plan::RuntimeFilterContract>,
    path: FieldPath,
) -> CodecResult<RuntimeFilterExecutionContract> {
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
    };
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
    with_fixture_resources(|resources| {
        let complete = novarocks_functions::FunctionValueType::new(expression_type.clone(), true);
        let roots = [ExecutionTypeRoot {
            id: 0,
            value: &complete,
        }];
        let mut work = CompileCheckpoints::try_new(resources.creation, CompilePhase::Encode)
            .map_err(NativeTypeConstructionError::Control)?;
        let limits = ExecutionTypeTableLimits::for_validated_root_count(roots.len())
            .map_err(NativeTypeConstructionError::Control)?;
        let wire = ExecutionTypeTableBuilder::new(&roots)
            .encode_observed(
                limits,
                &mut |layout| resources.creation.request(layout),
                &mut work,
            )
            .map_err(|cause| crate::native_type_decode::table_failure(cause, path.clone()))?
            .into_wire();
        work.finish()
            .map_err(NativeTypeConstructionError::Control)?;
        let owner = decode_type_owner(&wire, path.clone(), resources)?;
        let mut decoder = NativeTypeTableDecoder {
            types: &owner.execution,
            resources,
        };
        decode_contract_with_types(
            binding_id,
            expression_type,
            &complete,
            wire_contract,
            path,
            &mut decoder,
        )
    })
    .map_err(fixture_protocol)
}

fn decode_contract_with_types(
    binding_id: u32,
    expression_type: &arrow::datatypes::DataType,
    complete_type: &novarocks_functions::FunctionValueType,
    wire: Option<&plan::RuntimeFilterContract>,
    path: FieldPath,
    decoder: &mut NativeTypeTableDecoder<'_, '_, '_>,
) -> NativeResult<RuntimeFilterExecutionContract> {
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
            let path = path.field("membership").field("null_semantics");
            let semantics =
                crate::runtime_filter_membership::decode_membership_null_semantics(membership)
                    .map_err(|cause| {
                        error(
                            path.clone(),
                            ProtocolErrorKind::InvalidEnum,
                            cause.to_string(),
                        )
                    })?;
            let schema = {
                let source = complete_type;
                let resources = &mut *decoder.resources;
                let source = clone_type(source, resources)?;
                let scope = resources.creation.begin_scope()?;
                novarocks_execution::runtime_filter::RuntimeFilterMembershipSchema::new_value_typed_with_storage_observed::<NativeTypeDecodeError>(
                    source, semantics, scope.storage_lifetime(), |layout| scope.request_layout(layout).map_err(Into::into),
                    |units| { for _ in 0..units { resources.work.step().map_err(NativeTypeConstructionError::Control)?; } Ok(()) },
                )?
            };
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
                ).into());
            }
            {
                let resources = &mut *decoder.resources;
                resources.request(
                    Layout::array::<contribution::RuntimeOrderKey>(ordered.keys.len()).map_err(
                        |_| NativeTypeConstructionError::Resource(LayoutResourceError::Arithmetic),
                    )?,
                )?;
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
                let data_type = decoder.data_type(wire_type, key_path.clone().field("type"))?;
                let key_type = decoder.value_type(wire_type, key_path.clone().field("type"))?;
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
                        ).into());
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
                        ).into());
                    }
                };
                if let Some(actual) = &key_type {
                    let expected = complete_type;
                    if !decoder.bindings_match(
                        actual.typed_binding(),
                        expected.typed_binding(),
                        false,
                    )? {
                        return Err(contract_inconsistent(
                            key_path.field("type"),
                            "ordered key differs from complete runtime filter domain",
                        )
                        .into());
                    }
                }
                keys.push(match key_type {
                    Some(value) => {
                        contribution::RuntimeOrderKey::with_value_type(value, direction, null_order)
                    }
                    None => {
                        contribution::RuntimeOrderKey::with_order(data_type, direction, null_order)
                    }
                });
            }
            if keys[0].data_type() != expression_type {
                return Err(contract_inconsistent(
                    path.clone().field("keys").index(0).field("type"),
                    format!(
                        "native runtime-filter binding_id={binding_id} ordered key type {:?} does not match expression type {:?}",
                        keys[0].data_type(),
                        expression_type
                    ),
                ).into());
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
            contribution::RuntimeOrderContract::validate_fragment_keys_observed(
                &keys,
                comparator,
                order_digest,
                |units| {
                        let resources = &mut *decoder.resources;
                        if units == 0 {
                            resources
                                .work
                                .flush()
                                .map_err(NativeTypeConstructionError::Control)
                                .map_err(NativeTypeDecodeError::from)
                        } else {
                            for _ in 0..units {
                                resources
                                    .work
                                    .step()
                                    .map_err(NativeTypeConstructionError::Control)
                                    .map_err(NativeTypeDecodeError::from)?;
                            }
                            Ok(())
                        }
                },
            )
            .map_err(|failure| match failure {
                contribution::RuntimeOrderContractValidationFailure::Contract(cause) => {
                    contract_invalid(
                        path.clone(),
                        format!(
                            "native runtime-filter binding_id={binding_id} ordered contract is noncanonical: {cause:?}"
                        ),
                    )
                    .into()
                }
                contribution::RuntimeOrderContractValidationFailure::Schema(cause) => {
                    NativeTypeDecodeError::from_domain(path.clone(), cause)
                }
                contribution::RuntimeOrderContractValidationFailure::Work(cause) => cause,
            })?;
            {
                let resources = &mut *decoder.resources;
                resources.request(
                    arc_layout(Layout::new::<contribution::RuntimeOrderContract>())
                        .map_err(NativeTypeConstructionError::Resource)?,
                )?;
            }
            Ok(RuntimeFilterExecutionContract::Ordered(
                std::sync::Arc::new(contribution::RuntimeOrderContract::from_frozen_keys(
                    keys,
                    comparator,
                    order_digest,
                ))
                .into(),
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

#[cfg(any(test, novarocks_dev_controls))]
pub fn decode_participant_install(
    request: &filter::InstallRuntimeFilterDeploymentRequest,
) -> CodecResult<DecodedRuntimeFilterParticipantInstall> {
    with_fixture_resources(|resources| {
        let root = FieldPath::root("install_runtime_filter_deployment_request");
        let query_id = decode_unique_id(request.query_id.as_ref(), root.field("query_id"))?;
        decode_participant_parts(
            query_id,
            request.deployment_epoch,
            request.participant_id,
            request.lifecycle.as_ref(),
            request.install.as_ref(),
            &mut InstallTypeMode::Strong(resources),
        )
    })
    .map_err(fixture_protocol)
}

fn decode_participant_parts(
    query_id: UniqueId,
    deployment_epoch: u64,
    participant_id: u32,
    lifecycle: Option<&filter::RuntimeFilterQueryLifecycleOptions>,
    install: Option<&filter::RuntimeFilterParticipantInstall>,
    mode: &mut InstallTypeMode<'_, '_>,
) -> NativeResult<DecodedRuntimeFilterParticipantInstall> {
    let root = FieldPath::root("install_runtime_filter_deployment_request");
    {
        let InstallTypeMode::Strong(resources) = mode;
        resources
            .work
            .step()
            .map_err(NativeTypeConstructionError::Control)?;
    }
    reject_zero(
        deployment_epoch,
        root.clone().field("deployment_epoch"),
        "deployment epoch",
    )?;
    reject_zero(
        u64::from(participant_id),
        root.clone().field("participant_id"),
        "participant id",
    )?;
    let participant = BackendParticipantIdentity::new(query_id, deployment_epoch);
    let lifecycle = decode_lifecycle_options(lifecycle)?;
    let wire = install.ok_or_else(|| {
        missing(
            root.clone().field("install"),
            "participant install is required",
        )
    })?;
    let install = decode_install(
        wire,
        participant,
        participant_id,
        root.clone().field("install"),
        mode,
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
    mode: &mut InstallTypeMode<'_, '_>,
) -> NativeResult<BackendParticipantInstall> {
    let mut channels = BTreeMap::new();
    let mut binding_ids = BTreeSet::new();
    let mut consumer_route_ids = BTreeSet::new();
    for (index, channel) in wire.core_channels.iter().enumerate() {
        let item_path = path.clone().field("core_channels").index(index);
        let decoded = decode_core_channel(channel, participant, item_path.clone(), mode)?;
        if channels.insert(decoded.channel_id(), decoded).is_some() {
            return Err(duplicate(
                item_path.field("channel_id"),
                "duplicate Backend channel id",
            )
            .into());
        }
        let decoded = channels
            .get(&RuntimeFilterChannelId::new(channel.channel_id))
            .expect("inserted core channel");
        for binding in decoded.producers().keys().chain(decoded.consumers().keys()) {
            if !binding_ids.insert(*binding) {
                return Err(duplicate(
                    item_path.clone(),
                    "duplicate producer or consumer binding id across core install",
                )
                .into());
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
                )
                .into());
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
    .map_err(|error| invalid(path, error.to_string()).into())
}

fn decode_core_channel(
    wire: &filter::RuntimeFilterChannelDeployment,
    participant: BackendParticipantIdentity,
    path: FieldPath,
    mode: &mut InstallTypeMode<'_, '_>,
) -> NativeResult<BackendChannelInstall> {
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
        mode,
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
            )
            .into());
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
            )
            .into());
        }
        if !contributions.insert(contribution) {
            return Err(duplicate(item_path, "duplicate contribution kind").into());
        }
    }
    if contributions.is_empty() {
        return Err(invalid(
            path.clone().field("allowed_contribution_kinds"),
            "allowed contribution kinds must be nonempty",
        )
        .into());
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
    let mut install = BackendChannelInstall::new(
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
    .map_err(|error| invalid(path.clone(), error.to_string()))?;
    if let Some(publication) = wire.frontend_feedback_publication.as_ref() {
        let publication = decode_frontend_feedback_publication(publication, path.clone())?;
        install = install
            .with_frontend_feedback_publication(publication)
            .map_err(|error| invalid(path.clone(), error.to_string()))?;
    }
    let _ = participant;
    Ok(install)
}

fn decode_frontend_feedback_publication(
    wire: &filter::RuntimeFilterFrontendFeedbackPublication,
    path: FieldPath,
) -> CodecResult<BackendFrontendFeedbackPublication> {
    let publication_path = path.field("frontend_feedback_publication");
    let publisher_owner =
        match filter::RuntimeFilterOutboundMaterializationOwner::try_from(wire.publisher_owner) {
            Ok(filter::RuntimeFilterOutboundMaterializationOwner::DirectSource) => {
                BackendMaterializationOwner::DirectSource
            }
            Ok(filter::RuntimeFilterOutboundMaterializationOwner::Aggregator) => {
                BackendMaterializationOwner::Aggregator
            }
            Ok(filter::RuntimeFilterOutboundMaterializationOwner::Unspecified) | Err(_) => {
                return Err(codec_error(
                    publication_path.clone().field("publisher_owner"),
                    ProtocolErrorKind::InvalidEnum,
                    format!(
                        "invalid frontend feedback publisher owner={}",
                        wire.publisher_owner
                    ),
                ));
            }
        };
    let contract_digest: [u8; 32] = wire.contract_digest.as_slice().try_into().map_err(|_| {
        invalid(
            publication_path.clone().field("contract_digest"),
            "frontend feedback contract digest must be 32 bytes",
        )
    })?;
    let max_encoded_domain_bytes =
        usize::try_from(wire.max_encoded_domain_bytes).map_err(|_| {
            invalid(
                publication_path.clone().field("max_encoded_domain_bytes"),
                "frontend feedback domain budget does not fit usize",
            )
        })?;
    BackendFrontendFeedbackPublication::new(
        publisher_owner,
        contract_digest,
        max_encoded_domain_bytes,
    )
    .map_err(|error| invalid(publication_path, error.to_string()))
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
    let instances =
        decode_unique_id_set(&wire.expected_tasks, path.clone().field("expected_tasks"))?;
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
    let instances =
        decode_unique_id_set(&wire.expected_tasks, path.clone().field("expected_tasks"))?;
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
        let instance =
            decode_unique_id(route.task_key.as_ref(), item_path.clone().field("task_key"))?;
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
            let process_path = path.clone().field("remote").field("backend_process_id");
            let process_bytes = remote
                .backend_process_id
                .as_slice()
                .try_into()
                .map_err(|_| {
                    invalid(
                        process_path.clone(),
                        "backend process identity must be exactly 16 bytes",
                    )
                })?;
            let backend_process_id = BackendProcessId::try_from_bytes(process_bytes)
                .map_err(|error| invalid(process_path, error.to_string()))?;
            Ok(BackendRoutePeer::Remote {
                participant_id: remote.participant_id,
                backend_process_id,
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

pub fn validate_participant_install(install: &BackendParticipantInstall) -> Result<(), String> {
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
    use arrow::datatypes::DataType;

    use super::{
        decode_contract, decode_route_peer, decode_runtime_filter_contribution,
        validate_participant_install,
    };
    use novarocks_proto_codec::{
        FieldPath, ProtocolErrorKind,
        lifecycle::{AttemptId, QueryExecutionId, RuntimeFilterContribution},
    };
    use novarocks_proto_models::{filter, novarocks as proto_novarocks, plan};
    use novarocks_types::QueryId;
    use novarocks_worker::{
        RuntimeFilterContractError, RuntimeFilterContractErrorCode,
        runtime_filter::{
            domain::{
                BackendChannelInstall, BackendChannelLifecycle, BackendMaterializationPolicy,
                BackendParticipantInstall, BackendRoutingShard,
            },
            fixture::BackendRuntimeFilterFixture,
        },
    };

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(0x5246_4f34, 7),
            AttemptId::new(3).expect("nonzero attempt"),
        )
        .expect("nonzero execution id")
    }

    fn lifecycle_options() -> filter::RuntimeFilterQueryLifecycleOptions {
        filter::RuntimeFilterQueryLifecycleOptions {
            delivery_expire_ms: 1,
            query_expire_ms: 1,
            transport_retry_interval_ms: 1,
            transport_max_attempts: 1,
            transport_deadline_ms: 1,
            transport_max_pending_entries: 1,
            transport_max_pending_bytes: 1,
        }
    }

    fn empty_contribution_wire() -> proto_novarocks::RuntimeFilterContribution {
        proto_novarocks::RuntimeFilterContribution {
            participant_id: 3,
            lifecycle: Some(lifecycle_options()),
            install: Some(filter::RuntimeFilterParticipantInstall::default()),
        }
    }

    fn valid_empty_contribution() -> RuntimeFilterContribution {
        RuntimeFilterContribution::parse(empty_contribution_wire())
            .expect("valid opaque contribution fixture")
    }

    /// Assert the boundary rejection without pinning the protocol-error
    /// rendering: the load-bearing facts are the lifecycle error code, the
    /// rejected field path, and the reason.
    fn assert_invalid_contract(error: &RuntimeFilterContractError, path: &str, reason: &str) {
        assert_eq!(
            error.code(),
            RuntimeFilterContractErrorCode::InvalidContract
        );
        assert!(
            error.detail().contains(path),
            "rejection must name {path}, got {}",
            error.detail()
        );
        assert!(
            error.detail().contains(reason),
            "rejection must explain {reason}, got {}",
            error.detail()
        );
    }

    #[test]
    fn init_membership_null_semantics_rejects_unspecified_and_unknown_before_install() {
        for raw in [
            plan::RuntimeFilterMembershipNullSemantics::Unspecified as i32,
            99,
        ] {
            let error = decode_contract(
                7,
                &DataType::Int64,
                Some(&plan::RuntimeFilterContract {
                    logical_domain: None,
                    kind: Some(plan::runtime_filter_contract::Kind::Membership(
                        plan::RuntimeFilterMembershipContract {
                            null_semantics: raw,
                        },
                    )),
                }),
                FieldPath::root("logical_domain").field("contract"),
            )
            .expect_err("invalid membership null semantics must fail before participant install");
            assert_eq!(
                error.path().to_string(),
                "logical_domain.contract.membership.null_semantics"
            );
            assert_eq!(error.kind(), &ProtocolErrorKind::InvalidEnum);
        }
    }

    #[test]
    fn decodes_backend_participant_domain_from_the_admitted_execution_identity() {
        let execution_id = execution_id();
        let contribution = valid_empty_contribution();

        let decoded = decode_runtime_filter_contribution(execution_id, &contribution)
            .expect("backend decodes the valid participant install");

        assert_eq!(decoded.install.participant().deployment_epoch(), 3);
        assert_eq!(decoded.install.local_participant_id(), 3);
        assert!(decoded.install.channels().is_empty());
    }

    #[test]
    fn rejects_a_contribution_without_lifecycle_options() {
        let mut wire = empty_contribution_wire();
        wire.lifecycle = None;
        let malformed =
            RuntimeFilterContribution::parse(wire).expect("carrier shape stays structurally valid");

        let error = decode_runtime_filter_contribution(execution_id(), &malformed)
            .expect_err("a lifecycle-less contribution cannot be installed");
        assert_invalid_contract(
            &error,
            "install_runtime_filter_deployment_request.lifecycle",
            "lifecycle options are required",
        );
    }

    #[test]
    fn rejects_a_contribution_whose_lifecycle_option_is_zero() {
        let mut wire = empty_contribution_wire();
        wire.lifecycle
            .as_mut()
            .expect("lifecycle fixture")
            .query_expire_ms = 0;
        let malformed =
            RuntimeFilterContribution::parse(wire).expect("carrier shape stays structurally valid");

        let error = decode_runtime_filter_contribution(execution_id(), &malformed)
            .expect_err("a zero lifecycle bound cannot be installed");
        assert_invalid_contract(
            &error,
            "install_runtime_filter_deployment_request.lifecycle.query_expire_ms",
            "query expiry must be nonzero",
        );
    }

    #[test]
    fn rejects_a_contribution_without_a_participant_install() {
        let mut wire = empty_contribution_wire();
        wire.install = None;
        let malformed =
            RuntimeFilterContribution::parse(wire).expect("carrier shape stays structurally valid");

        let error = decode_runtime_filter_contribution(execution_id(), &malformed)
            .expect_err("an install-less contribution cannot be installed");
        assert_invalid_contract(
            &error,
            "install_runtime_filter_deployment_request.install",
            "participant install is required",
        );
    }

    #[test]
    fn rejects_a_contribution_whose_install_is_structurally_invalid() {
        let mut wire = empty_contribution_wire();
        wire.install
            .as_mut()
            .expect("install fixture")
            .core_channels
            .push(filter::RuntimeFilterChannelDeployment::default());
        let malformed =
            RuntimeFilterContribution::parse(wire).expect("carrier shape stays structurally valid");

        let error = decode_runtime_filter_contribution(execution_id(), &malformed)
            .expect_err("an unaddressed core channel cannot be installed");
        assert_invalid_contract(
            &error,
            "install_runtime_filter_deployment_request.install.core_channels[0].channel_id",
            "channel id must be nonzero",
        );
    }

    fn relay_role() -> filter::RuntimeFilterRouteRole {
        filter::RuntimeFilterRouteRole {
            role: Some(filter::runtime_filter_route_role::Role::Relay(true)),
        }
    }

    fn route_endpoint(participant_id: u32) -> filter::RuntimeFilterRouteEndpointView {
        filter::RuntimeFilterRouteEndpointView {
            participant_id,
            role: Some(relay_role()),
        }
    }

    #[test]
    fn remote_peer_retains_the_frozen_process_even_when_endpoint_is_reused() {
        use novarocks_worker::runtime_filter::domain::BackendRoutePeer;
        let original = novarocks_types::BackendProcessId::new_v7();
        let replacement = novarocks_types::BackendProcessId::new_v7();
        let peer = |process: novarocks_types::BackendProcessId| filter::RuntimeFilterRoutePeer {
            peer: Some(filter::runtime_filter_route_peer::Peer::Remote(
                filter::RuntimeFilterRemotePeer {
                    participant_id: 4,
                    endpoint: "127.0.0.1:19040".to_owned(),
                    backend_process_id: process.to_bytes().to_vec(),
                },
            )),
        };
        let first = decode_route_peer(Some(&peer(original)), FieldPath::root("peer")).unwrap();
        let next = decode_route_peer(Some(&peer(replacement)), FieldPath::root("peer")).unwrap();
        assert_ne!(first, next);
        let BackendRoutePeer::Remote {
            backend_process_id, ..
        } = first
        else {
            panic!("remote peer must stay remote");
        };
        assert_eq!(backend_process_id, original);
    }

    #[test]
    fn remote_peer_refuses_missing_wrong_width_nil_and_non_v7_process() {
        let mut non_v7 = novarocks_types::BackendProcessId::new_v7().to_bytes();
        non_v7[6] = (non_v7[6] & 0x0f) | 0x40;
        for bytes in [
            Vec::new(),
            vec![1; 15],
            vec![1; 17],
            vec![0; 16],
            non_v7.to_vec(),
        ] {
            let wire = filter::RuntimeFilterRoutePeer {
                peer: Some(filter::runtime_filter_route_peer::Peer::Remote(
                    filter::RuntimeFilterRemotePeer {
                        participant_id: 4,
                        endpoint: "127.0.0.1:19040".to_owned(),
                        backend_process_id: bytes,
                    },
                )),
            };
            let error = decode_route_peer(Some(&wire), FieldPath::root("peer"))
                .expect_err("unfenced remote process must be refused");
            assert_eq!(error.path().to_string(), "peer.remote.backend_process_id");
        }
    }

    #[test]
    fn rejects_a_routing_edge_that_disagrees_with_the_contribution_participant() {
        let mut wire = empty_contribution_wire();
        wire.install
            .as_mut()
            .expect("install fixture")
            .routing_channels
            .push(filter::RuntimeFilterChannelRoutingView {
                channel_id: 1,
                local_roles: vec![relay_role()],
                producer_instances: Vec::new(),
                inbound_edges: Vec::new(),
                outbound_edges: vec![filter::RuntimeFilterRoutingEdgeView {
                    route_edge_id: 1,
                    // The contribution addresses participant 3, so an outbound
                    // edge sourced at participant 9 is self-inconsistent.
                    source: Some(route_endpoint(9)),
                    target: Some(route_endpoint(4)),
                    peer: Some(filter::RuntimeFilterRoutePeer {
                        peer: Some(filter::runtime_filter_route_peer::Peer::Remote(
                            filter::RuntimeFilterRemotePeer {
                                participant_id: 4,
                                endpoint: "127.0.0.1:19040".to_string(),
                                backend_process_id: novarocks_types::BackendProcessId::new_v7()
                                    .to_bytes()
                                    .to_vec(),
                            },
                        )),
                    }),
                    allowed_kinds: vec![filter::RuntimeFilterEnvelopeKind::Contribution as i32],
                }],
            });
        let malformed =
            RuntimeFilterContribution::parse(wire).expect("carrier shape stays structurally valid");

        let error = decode_runtime_filter_contribution(execution_id(), &malformed)
            .expect_err("a self-inconsistent routing edge cannot be installed");
        assert_invalid_contract(
            &error,
            "install_runtime_filter_deployment_request.install.routing_channels[0].outbound_edges",
            "outbound edge source does not match request participant",
        );
    }

    #[test]
    fn rejects_an_unaddressed_contribution_at_the_manifest_carrier() {
        let mut wire = empty_contribution_wire();
        wire.participant_id = 0;

        let error = RuntimeFilterContribution::parse(wire)
            .expect_err("participant id remains a carrier invariant");
        assert_eq!(
            error.detail(),
            "runtime filter participant id must be nonzero"
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

#[cfg(test)]
mod typed_domain_creation_tests {
    mod canonical_backing_owner_tests {
        use crate::native_type_construction::NativeTypeFactory;
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/runtime_filter_install/canonical_backing_owner_tests.rs"
        ));
    }

    use super::*;
    use novarocks_execution::runtime::mem_tracker::MemTracker;
    use novarocks_functions::FunctionValueType;
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
    };
    use novarocks_spi::connector::ConnectorStopOwner;
    use novarocks_type_contract::{
        CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
    };
    use std::sync::Arc;

    struct SourceControl;
    impl PureCompileControl for SourceControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    fn domain(value: &FunctionValueType) -> plan::RuntimeFilterContract {
        let roots = [ExecutionTypeRoot { id: 0, value }];
        let mut work = CompileCheckpoints::try_new(&SourceControl, CompilePhase::Encode).unwrap();
        let table = ExecutionTypeTableBuilder::new(&roots)
            .encode_observed::<std::convert::Infallible>(
                ExecutionTypeTableLimits::for_validated_root_count(roots.len()).unwrap(),
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap()
            .into_wire();
        work.finish().unwrap();
        plan::RuntimeFilterContract {
            logical_domain: Some(plan::RuntimeFilterTypeDomain {
                value_type: Some(common::TypeDesc {
                    value_binding_id: Some(0),
                }),
                execution_types: Some(table),
            }),
            kind: Some(plan::runtime_filter_contract::Kind::Membership(
                plan::RuntimeFilterMembershipContract {
                    null_semantics: plan::RuntimeFilterMembershipNullSemantics::NeverMatches as i32,
                },
            )),
        }
    }
    fn creator(
        tracker: Arc<MemTracker>,
        control: &crate::backend_test_support::PureConnectorCompileControl,
    ) -> NativeTypeConstruction<'_> {
        NativeTypeConstruction::try_with_control(tracker, control).unwrap()
    }
    #[test]
    fn independently_decoded_runtime_filter_domain_retains_actual_creator_until_last_binding_reference()
     {
        let source = FunctionValueType::try_with_logical_type(
            arrow::datatypes::DataType::Utf8,
            true,
            ValueLogicalType::Json,
        )
        .unwrap();
        let wire = domain(&source);
        let tracker = MemTracker::new_root("RF complete domain fixture");
        tracker.install_limit_once(1024 * 1024).unwrap();
        let stop = ConnectorStopOwner::new();
        let control = crate::backend_test_support::PureConnectorCompileControl(stop.view());
        let creation = creator(tracker.clone(), &control);
        let mut resources = NativeTypeDecodeResources::try_new(&creation).unwrap();
        let decoded = decode_typed_contract_domain(
            7,
            &wire,
            None,
            FieldPath::root("channel"),
            &mut resources,
        )
        .unwrap();
        resources.finish().unwrap();
        let independent = decoded.value_type().unwrap().clone();
        assert_eq!(independent, source);
        let all_live = tracker.current();
        drop(decoded);
        drop(creation);
        drop(wire);
        let own = 2
            * crate::native_type_construction::NativeTypeScope::reservation_allocation_layout()
                .unwrap()
                .size()
            + novarocks_type_contract::ValueTypeRef::storage_allocation_layout()
                .unwrap()
                .size()
            + novarocks_type_contract::ValueLayout::storage_allocation_layout()
                .unwrap()
                .size();
        assert_eq!(tracker.current(), i64::try_from(own).unwrap());
        assert!(
            tracker.current() < all_live,
            "FVT does not retain the canonical or parent scope"
        );
        drop(independent);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn runtime_filter_domain_rejects_inline_or_other_owner_references() {
        let source = FunctionValueType::new(arrow::datatypes::DataType::Int32, false);
        for invalid in [
            common::TypeDesc {
                value_binding_id: Some(99),
            },
            common::TypeDesc {
                value_binding_id: None,
            },
        ] {
            let wire = domain(&source);
            let tracker = MemTracker::new_root("RF namespace fixture");
            tracker.install_limit_once(1024 * 1024).unwrap();
            let stop = ConnectorStopOwner::new();
            let control = crate::backend_test_support::PureConnectorCompileControl(stop.view());
            let creation = creator(tracker, &control);
            let mut resources = NativeTypeDecodeResources::try_new(&creation).unwrap();
            let error = decode_typed_contract_domain(
                1,
                &wire,
                Some(&invalid),
                FieldPath::root("channel"),
                &mut resources,
            )
            .unwrap_err();
            assert!(error.construction_cause().is_none());
        }
    }

    #[test]
    fn runtime_filter_scoped_shared_child_releases_unused_names_layouts_and_namespace_scratch() {
        use arrow::datatypes::{DataType, Field};
        use novarocks_proto_models::arrow_type::arrow_physical_type::Kind as PhysicalKind;
        use novarocks_proto_models::value_type::logical_type_definition::Kind as LogicalKind;
        let source = FunctionValueType::new(DataType::Int64, false);
        let unused = FunctionValueType::new(
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "unused shared child",
                    DataType::Int64,
                    false,
                ))]
                .into(),
            ),
            false,
        );
        let mut wire = domain(&source);
        let mut work = CompileCheckpoints::try_new(&SourceControl, CompilePhase::Encode).unwrap();
        let mut table = ExecutionTypeTableBuilder::new(&[
            ExecutionTypeRoot {
                id: 0,
                value: &source,
            },
            ExecutionTypeRoot {
                id: u32::MAX,
                value: &unused,
            },
        ])
        .encode_observed::<std::convert::Infallible>(
            ExecutionTypeTableLimits::for_validated_root_count(2).unwrap(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap()
        .into_wire();
        work.finish().unwrap();
        // This wire author shares the equal scalar child definition explicitly
        // in this namespace. Both complete declarations and layouts are the
        // original encoder's facts; no metadata or carrier authors a domain.
        let selected_id = table
            .value_bindings
            .iter()
            .find(|b| b.id == Some(0))
            .unwrap()
            .logical_type_id
            .unwrap();
        let unused_binding = table
            .value_bindings
            .iter_mut()
            .find(|b| b.id == Some(u32::MAX))
            .unwrap();
        let unused_id = unused_binding.logical_type_id.unwrap();
        let Some(PhysicalKind::StructType(layout)) =
            &mut unused_binding.value_layout.as_mut().unwrap().kind
        else {
            panic!("fixture Struct layout");
        };
        layout.fields[0].logical_type_id = Some(selected_id);
        let Some(LogicalKind::StructValue(semantic)) = &mut table
            .logical_types
            .iter_mut()
            .find(|d| d.id == Some(unused_id))
            .unwrap()
            .kind
        else {
            panic!("fixture Struct semantics");
        };
        semantic.fields[0].value_type_id = Some(selected_id);
        wire.logical_domain.as_mut().unwrap().execution_types = Some(table);
        let tracker = MemTracker::new_root("RF scoped child");
        let stop = ConnectorStopOwner::new();
        let control = crate::backend_test_support::PureConnectorCompileControl(stop.view());
        let creation = creator(tracker.clone(), &control);
        let mut resources = NativeTypeDecodeResources::try_new(&creation).unwrap();
        let contract = decode_typed_contract_domain(
            7,
            &wire,
            None,
            FieldPath::root("channel"),
            &mut resources,
        )
        .unwrap();
        let late_child = contract.value_type().unwrap().value_type().clone();
        resources.finish().unwrap();
        drop(contract);
        drop(creation);
        drop(stop);
        drop(wire);
        let own = crate::native_type_construction::NativeTypeScope::reservation_allocation_layout()
            .unwrap()
            .size()
            + novarocks_type_contract::ValueTypeRef::storage_allocation_layout()
                .unwrap()
                .size();
        assert_eq!(tracker.current(), i64::try_from(own).unwrap());
        drop(late_child);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn runtime_filter_canonical_clone_keeps_its_own_bytes_and_reused_layout_until_last_reference() {
        use arrow::datatypes::{DataType, TimeUnit};
        let source = FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        );
        let wire = domain(&source);
        let tracker = MemTracker::new_root("RF canonical scope");
        let stop = ConnectorStopOwner::new();
        let control = crate::backend_test_support::PureConnectorCompileControl(stop.view());
        let creation = creator(tracker.clone(), &control);
        let mut resources = NativeTypeDecodeResources::try_new(&creation).unwrap();
        let contract = decode_typed_contract_domain(
            7,
            &wire,
            None,
            FieldPath::root("channel"),
            &mut resources,
        )
        .unwrap();
        let RuntimeFilterExecutionContract::Membership(schema) = &contract else {
            panic!("fixture membership");
        };
        let canonical = schema.canonical_schema().clone();
        let digest = canonical.digest();
        let pure = novarocks_local_program::FilterMembershipSchema::try_new(
            source.data_type(),
            canonical.null_semantics(),
        )
        .unwrap();
        assert_eq!(
            canonical, pure,
            "storage identity does not alter the physical artifact identity"
        );
        resources.finish().unwrap();
        drop(contract);
        drop(creation);
        drop(stop);
        drop(wire);
        drop(source);
        assert!(
            tracker.current() > 0,
            "canonical owner retains actual construction and its borrowed timezone layout"
        );
        assert_eq!(canonical.digest(), digest);
        assert_eq!(canonical.canonical_bytes(), pure.canonical_bytes());
        drop(canonical);
        assert_eq!(tracker.current(), 0);
    }
}
