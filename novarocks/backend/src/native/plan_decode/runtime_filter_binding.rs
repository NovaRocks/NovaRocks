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

//! Strict fragment-local runtime-filter binding decoding and lookup.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use super::error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};
use novarocks::exec::node::runtime_filter::{
    ArtifactCapability, ArtifactMembershipSchema, ComparatorDigest, CompletionFenceKind,
    CompletionRequirement, ConsumerActivation, ContributionKind, LateApplyGranularity, NullOrder,
    OrderContract, OrderKeyContract, ReductionRequirement, RuntimeFilterLogicalDomain,
    RuntimeOrderContract, RuntimeOrderKey, RuntimeTopKSummaryContract, SortDirection,
    TopKSummaryRequirement,
};
use novarocks::protocol::{FieldPath, ProtocolErrorKind};
use novarocks_protocol::{expr, plan};

/// Backend-local producer attachment target decoded from the native fragment
/// binding table. It is translated into the corresponding neutral execution
/// constructor at the physical node boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProducerBindingTarget {
    JoinBuildKey { ordinal: usize },
    AggregateTopNKey { ordinal: usize, limit: NonZeroU32 },
}

#[derive(Clone, Debug)]
pub(crate) struct DecodedRuntimeFilterBinding {
    pub(crate) binding_id: u32,
    pub(crate) channel_id: u32,
    pub(crate) node_id: i32,
    pub(crate) apply_point: DecodedApplyPoint,
    pub(crate) expression: expr::Expr,
    pub(crate) expression_path: FieldPath,
    pub(crate) role: DecodedBindingRole,
    pub(crate) contract: DecodedRuntimeFilterContract,
    pub(crate) reduction: DecodedRuntimeFilterReduction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DecodedApplyPoint {
    NodeInput,
    NodeOutput,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DecodedBindingRole {
    Producer {
        contribution_kinds: BTreeSet<ContributionKind>,
        completion_requirement: CompletionRequirement,
        target: ProducerBindingTarget,
    },
    Consumer {
        capabilities: BTreeSet<ArtifactCapability>,
        activation: ConsumerActivation,
        target: DecodedConsumerBindingTarget,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DecodedConsumerBindingTarget {
    DirectInput { input_ordinal: usize },
    SourceBoundary,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DecodedRuntimeFilterContract {
    Membership {
        canonical_schema: Arc<[u8]>,
        schema_digest: [u8; 32],
    },
    Ordered {
        keys: Arc<[RuntimeOrderKey]>,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DecodedRuntimeFilterReduction {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary {
        k: NonZeroU32,
        contract_digest: [u8; 32],
    },
}

pub(crate) struct NativeRuntimeFilterDecodeLedger {
    fragment_id: u32,
    records: BTreeMap<u32, DecodedRuntimeFilterBinding>,
    consumed: BTreeMap<u32, ()>,
}

impl NativeRuntimeFilterDecodeLedger {
    pub(crate) fn decode(
        enclosing_fragment_id: u32,
        table: Option<&plan::RuntimeFilterBindingTable>,
    ) -> Result<Self, NativeFragmentDecodeError> {
        let path = FieldPath::root("plan_fragment").field("runtime_filter_bindings");
        let table = table.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                format!(
                    "native PlanFragment fragment_id={enclosing_fragment_id} requires runtime_filter_bindings"
                ),
            )
        })?;
        if table.fragment_id != enclosing_fragment_id {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("fragment_id"),
                format!(
                    "binding table fragment_id={} does not match enclosing fragment_id={enclosing_fragment_id}",
                    table.fragment_id
                ),
            ));
        }
        let mut records = BTreeMap::new();
        let mut previous_id = None;
        for (index, wire) in table.bindings.iter().enumerate() {
            let binding_path = path.clone().field("bindings").index(index);
            if previous_id.is_some_and(|previous| wire.binding_id <= previous) {
                return Err(NativeFragmentDecodeError::inconsistent(
                    binding_path.clone().field("binding_id"),
                    format!(
                        "bindings must be strictly ordered by binding_id; previous={previous_id:?} actual={}",
                        wire.binding_id
                    ),
                ));
            }
            let record = decode_binding(wire, binding_path.clone())?;
            if records.insert(record.binding_id, record).is_some() {
                return Err(NativeFragmentDecodeError::inconsistent(
                    binding_path.field("binding_id"),
                    format!("duplicate binding_id={}", wire.binding_id),
                ));
            }
            previous_id = Some(wire.binding_id);
        }
        Ok(Self {
            fragment_id: enclosing_fragment_id,
            records,
            consumed: BTreeMap::new(),
        })
    }

    fn lookup_for_node(
        &self,
        binding_id: u32,
        node_id: i32,
        node_fragment_id: u32,
    ) -> Result<&DecodedRuntimeFilterBinding, NativeFragmentLeafDecodeError> {
        if node_fragment_id != self.fragment_id {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "runtime_filter_binding_ids",
                format!(
                    "native node_id={node_id} fragment_id={node_fragment_id} cannot reference runtime-filter binding table fragment_id={}",
                    self.fragment_id
                ),
            ));
        }
        if self.consumed.contains_key(&binding_id) {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "runtime_filter_binding_ids",
                format!("native runtime-filter binding_id={binding_id} is attached more than once"),
            ));
        }
        let record = self.records.get(&binding_id).ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InconsistentFields, "runtime_filter_binding_ids", format!(
                "native node_id={node_id} references unknown runtime-filter binding_id={binding_id}"
            ))
        })?;
        if record.node_id != node_id {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "runtime_filter_binding_ids",
                format!(
                    "native runtime-filter binding_id={binding_id} belongs to node_id={}, not attachment node_id={node_id}",
                    record.node_id
                ),
            ));
        }
        Ok(record)
    }

    pub(super) fn peek_attached(
        &self,
        binding_ids: &[u32],
        node_id: i32,
        node_fragment_id: u32,
    ) -> Result<Vec<DecodedRuntimeFilterBinding>, NativeFragmentLeafDecodeError> {
        let mut seen = BTreeSet::new();
        binding_ids.iter().copied().map(|binding_id| {
            if !seen.insert(binding_id) {
                return Err(NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InconsistentFields, "runtime_filter_binding_ids", format!("native node_id={node_id} has duplicate runtime-filter binding attachment id={binding_id}")));
            }
            self.lookup_for_node(binding_id, node_id, node_fragment_id).cloned()
        }).collect()
    }

    fn commit_consumed(&mut self, binding_id: u32) -> Result<(), NativeFragmentLeafDecodeError> {
        if !self.records.contains_key(&binding_id) {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "runtime_filter_binding_ids",
                format!("cannot consume unknown runtime-filter binding_id={binding_id}"),
            ));
        }
        if self.consumed.insert(binding_id, ()).is_some() {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "runtime_filter_binding_ids",
                format!("runtime-filter binding_id={binding_id} consumed more than once"),
            ));
        }
        Ok(())
    }

    pub(super) fn commit_consumed_many(
        &mut self,
        binding_ids: &[u32],
    ) -> Result<(), NativeFragmentLeafDecodeError> {
        let mut unique = BTreeSet::new();
        for binding_id in binding_ids {
            if !unique.insert(*binding_id)
                || !self.records.contains_key(binding_id)
                || self.consumed.contains_key(binding_id)
            {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "runtime_filter_binding_ids",
                    format!("cannot atomically consume runtime-filter binding_id={binding_id}"),
                ));
            }
        }
        for binding_id in unique {
            self.consumed.insert(binding_id, ());
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<(), NativeFragmentDecodeError> {
        self.finish_impl().map_err(|error| {
            error.into_native(FieldPath::root("plan_fragment").field("runtime_filter_bindings"))
        })
    }

    fn finish_impl(self) -> Result<(), NativeFragmentLeafDecodeError> {
        if let Some(binding_id) = self
            .records
            .keys()
            .find(|id| !self.consumed.contains_key(id))
        {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "bindings",
                format!(
                    "native runtime-filter binding table fragment_id={} has unconsumed binding_id={binding_id}",
                    self.fragment_id
                ),
            ));
        }
        Ok(())
    }
}

fn decode_binding(
    wire: &plan::RuntimeFilterBinding,
    path: FieldPath,
) -> Result<DecodedRuntimeFilterBinding, NativeFragmentDecodeError> {
    let apply_point = plan::RuntimeFilterApplyPoint::try_from(wire.apply_point).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("apply_point"),
            format!("unknown runtime-filter apply_point={}", wire.apply_point),
        )
    })?;
    if apply_point == plan::RuntimeFilterApplyPoint::Unspecified {
        return Err(NativeFragmentDecodeError::invalid_enum(
            path.clone().field("apply_point"),
            "runtime-filter apply_point must be specified",
        ));
    }
    let decoded_apply_point = match apply_point {
        plan::RuntimeFilterApplyPoint::NodeInput => DecodedApplyPoint::NodeInput,
        plan::RuntimeFilterApplyPoint::NodeOutput => DecodedApplyPoint::NodeOutput,
        plan::RuntimeFilterApplyPoint::Unspecified => unreachable!("rejected above"),
    };
    let expression_path = path.clone().field("expression");
    let expression = wire.expression.clone().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            expression_path.clone(),
            "runtime-filter binding requires expression",
        )
    })?;
    crate::native::expression::validate_proto_expr_shape_at(&expression, expression_path.clone())
        .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))?;
    let expression_type =
        crate::native::type_decode::decode_type(expression.r#type.as_ref().expect("checked"))
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    expression_path.clone().field("type"),
                    error,
                )
            })?;
    let contract = decode_contract(
        wire.binding_id,
        &expression_type,
        wire.contract.as_ref(),
        path.clone().field("contract"),
    )?;
    let reduction = decode_reduction(
        wire.binding_id,
        &contract,
        wire.reduction.as_ref(),
        path.clone().field("reduction"),
    )?;
    let role = decode_role(
        wire.binding_id,
        wire.role.as_ref(),
        path.clone().field("role"),
    )?;
    match (&role, apply_point) {
        (DecodedBindingRole::Consumer { .. }, plan::RuntimeFilterApplyPoint::NodeInput)
        | (DecodedBindingRole::Producer { .. }, plan::RuntimeFilterApplyPoint::NodeOutput) => {}
        (DecodedBindingRole::Consumer { .. }, _) => {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("apply_point"),
                format!(
                    "runtime-filter consumer binding_id={} must use NodeInput",
                    wire.binding_id
                ),
            ));
        }
        (DecodedBindingRole::Producer { .. }, _) => {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("apply_point"),
                format!(
                    "runtime-filter producer binding_id={} must use NodeOutput",
                    wire.binding_id
                ),
            ));
        }
    }
    validate_role_contract(wire.binding_id, &contract, &reduction, &role)
        .map_err(|error| NativeFragmentDecodeError::inconsistent(path.clone(), error))?;
    Ok(DecodedRuntimeFilterBinding {
        binding_id: wire.binding_id,
        channel_id: wire.channel_id,
        node_id: wire.node_id,
        apply_point: decoded_apply_point,
        expression,
        expression_path,
        role,
        contract,
        reduction,
    })
}

fn digest32(binding_id: u32, field: &str, bytes: &[u8]) -> Result<[u8; 32], String> {
    bytes.try_into().map_err(|_| format!(
        "native runtime-filter binding_id={binding_id} {field} must be exactly 32 bytes, got {}",
        bytes.len()
    ))
}

#[allow(dead_code)] // Shared with the install codec before its Task 4 handler call site lands.
pub(crate) fn decode_runtime_filter_logical_domain_and_reduction(
    wire_type: Option<&novarocks_protocol::common::TypeDesc>,
    wire_contract: Option<&plan::RuntimeFilterContract>,
    wire_reduction: Option<&plan::RuntimeFilterReductionContract>,
    path: FieldPath,
) -> Result<(RuntimeFilterLogicalDomain, ReductionRequirement), NativeFragmentDecodeError> {
    let type_path = path.clone().field("value_type");
    let wire_type = wire_type.ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            type_path.clone(),
            "runtime filter deployment logical domain is missing value type",
        )
    })?;
    let value_type = crate::native::type_decode::decode_type(wire_type)
        .map_err(|error| NativeFragmentDecodeError::invalid_value(type_path, error))?;
    let decoded_contract = decode_contract(
        0,
        &value_type,
        wire_contract,
        path.clone().field("contract"),
    )?;
    let decoded_reduction = decode_reduction(
        0,
        &decoded_contract,
        wire_reduction,
        path.field("reduction"),
    )?;
    let domain = match decoded_contract {
        DecodedRuntimeFilterContract::Membership {
            canonical_schema, ..
        } => {
            let schema = ArtifactMembershipSchema::view(&canonical_schema).map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    FieldPath::root("runtime_filter_install")
                        .field("logical_domain")
                        .field("contract"),
                    format!("invalid membership schema: {error:?}"),
                )
            })?;
            RuntimeFilterLogicalDomain::Membership {
                value_type,
                null_semantics: schema.null_semantics(),
            }
        }
        DecodedRuntimeFilterContract::Ordered {
            keys,
            comparator_digest,
            ..
        } => RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
            keys: keys
                .iter()
                .map(|key| OrderKeyContract {
                    data_type: key.data_type().clone(),
                    direction: key.direction(),
                    null_order: key.null_order(),
                })
                .collect(),
            inclusive: true,
            comparator_digest: ComparatorDigest::new(comparator_digest),
        }),
    };
    let reduction = match decoded_reduction {
        DecodedRuntimeFilterReduction::SetUnion => ReductionRequirement::SetUnion,
        DecodedRuntimeFilterReduction::TightenOrderedBound => {
            ReductionRequirement::TightenOrderedBound
        }
        DecodedRuntimeFilterReduction::MergeTopKSummary { k, .. } => {
            ReductionRequirement::MergeTopKSummary(
                TopKSummaryRequirement::try_new(k.get()).expect("decoded TopK K is nonzero"),
            )
        }
    };
    Ok((domain, reduction))
}

pub(crate) fn decode_runtime_filter_contribution_kind(
    raw: i32,
    path: FieldPath,
) -> Result<ContributionKind, NativeFragmentDecodeError> {
    match plan::RuntimeFilterContributionKind::try_from(raw) {
        Ok(plan::RuntimeFilterContributionKind::ValueDomainDelta) => {
            Ok(ContributionKind::ValueDomainDelta)
        }
        Ok(plan::RuntimeFilterContributionKind::FinalDomainShard) => {
            Ok(ContributionKind::FinalDomainShard)
        }
        Ok(plan::RuntimeFilterContributionKind::OrderedBoundUpdate) => {
            Ok(ContributionKind::OrderedBoundUpdate)
        }
        Ok(plan::RuntimeFilterContributionKind::TopkSummary) => Ok(ContributionKind::TopKSummary),
        Ok(plan::RuntimeFilterContributionKind::ProducerClosed) => {
            Ok(ContributionKind::ProducerClosed)
        }
        Ok(plan::RuntimeFilterContributionKind::Unspecified) | Err(_) => {
            Err(NativeFragmentDecodeError::invalid_enum(
                path,
                format!("invalid runtime filter contribution kind={raw}"),
            ))
        }
    }
}

pub(crate) fn decode_runtime_filter_completion(
    raw: i32,
    path: FieldPath,
) -> Result<CompletionRequirement, NativeFragmentDecodeError> {
    match plan::RuntimeFilterCompletionRequirement::try_from(raw) {
        Ok(plan::RuntimeFilterCompletionRequirement::ProducerClosed) => {
            Ok(CompletionRequirement::ProducerClosed)
        }
        Ok(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen) => Ok(
            CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
        ),
        Ok(plan::RuntimeFilterCompletionRequirement::Unspecified) | Err(_) => {
            Err(NativeFragmentDecodeError::invalid_enum(
                path,
                format!("invalid runtime filter completion requirement={raw}"),
            ))
        }
    }
}

pub(crate) fn decode_runtime_filter_capability(
    raw: i32,
    path: FieldPath,
) -> Result<ArtifactCapability, NativeFragmentDecodeError> {
    match plan::RuntimeFilterArtifactCapability::try_from(raw) {
        Ok(plan::RuntimeFilterArtifactCapability::Membership) => Ok(ArtifactCapability::Membership),
        Ok(plan::RuntimeFilterArtifactCapability::OrderedRange) => {
            Ok(ArtifactCapability::OrderedRange)
        }
        Ok(plan::RuntimeFilterArtifactCapability::EmptyDomain) => {
            Ok(ArtifactCapability::EmptyDomain)
        }
        Ok(plan::RuntimeFilterArtifactCapability::Unspecified) | Err(_) => {
            Err(NativeFragmentDecodeError::invalid_enum(
                path,
                format!("invalid runtime filter artifact capability={raw}"),
            ))
        }
    }
}

pub(crate) fn decode_runtime_filter_activation(
    wire: Option<&plan::RuntimeFilterConsumerActivation>,
    path: FieldPath,
) -> Result<ConsumerActivation, NativeFragmentDecodeError> {
    let wire = wire.ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone(),
            "missing runtime filter consumer activation",
        )
    })?;
    match wire.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            "missing runtime filter consumer activation kind",
        )
    })? {
        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true) => {
            Ok(ConsumerActivation::BlockingSnapshot)
        }
        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(false) => {
            Err(NativeFragmentDecodeError::invalid_value(
                path.field("kind").field("blocking_snapshot"),
                "runtime filter blocking activation marker must be true",
            ))
        }
        plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(raw) => {
            let late_apply = match plan::RuntimeFilterLateApplyGranularity::try_from(*raw) {
                Ok(plan::RuntimeFilterLateApplyGranularity::Row) => LateApplyGranularity::Row,
                Ok(plan::RuntimeFilterLateApplyGranularity::Batch) => LateApplyGranularity::Batch,
                Ok(plan::RuntimeFilterLateApplyGranularity::RowGroup) => {
                    LateApplyGranularity::RowGroup
                }
                Ok(plan::RuntimeFilterLateApplyGranularity::Split) => LateApplyGranularity::Split,
                Ok(plan::RuntimeFilterLateApplyGranularity::File) => LateApplyGranularity::File,
                Ok(plan::RuntimeFilterLateApplyGranularity::Unspecified) | Err(_) => {
                    return Err(NativeFragmentDecodeError::invalid_enum(
                        path.field("kind").field("non_blocking_live"),
                        format!("invalid runtime filter late-apply granularity={raw}"),
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
) -> Result<DecodedRuntimeFilterContract, NativeFragmentDecodeError> {
    let wire = wire.ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone(),
            format!("native runtime-filter binding_id={binding_id} missing contract"),
        )
    })?;
    let kind = wire.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            format!("native runtime-filter binding_id={binding_id} missing contract kind"),
        )
    })?;
    match kind {
        plan::runtime_filter_contract::Kind::Membership(membership) => {
            let path = path.field("membership");
            if membership.canonical_schema.is_empty() {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("canonical_schema"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema is empty"
                    ),
                ));
            }
            let view = ArtifactMembershipSchema::view(&membership.canonical_schema).map_err(
                |error| {
                    NativeFragmentDecodeError::invalid_value(
                        path.clone().field("canonical_schema"),
                        format!(
                            "native runtime-filter binding_id={binding_id} membership schema is noncanonical: {error:?}"
                        ),
                    )
                },
            )?;
            let digest = digest32(
                binding_id,
                "membership schema_digest",
                &membership.schema_digest,
            )
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(path.clone().field("schema_digest"), error)
            })?;
            if view.digest().bytes() != digest {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.clone().field("schema_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema digest mismatch"
                    ),
                ));
            }
            let expected = ArtifactMembershipSchema::new(expression_type, view.null_semantics())
                .map_err(|error| {
                    NativeFragmentDecodeError::invalid_value(
                        path.clone().field("canonical_schema"),
                        format!(
                            "native runtime-filter binding_id={binding_id} expression type cannot form membership schema: {error:?}"
                        ),
                    )
                })?;
            if expected.canonical_bytes() != membership.canonical_schema {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.field("canonical_schema"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema does not match expression type"
                    ),
                ));
            }
            Ok(DecodedRuntimeFilterContract::Membership {
                canonical_schema: Arc::from(membership.canonical_schema.as_slice()),
                schema_digest: digest,
            })
        }
        plan::runtime_filter_contract::Kind::Ordered(ordered) => {
            let path = path.field("ordered");
            if ordered.keys.len() != 1 {
                return Err(NativeFragmentDecodeError::invalid_value(
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
                    NativeFragmentDecodeError::missing(
                        key_path.clone().field("type"),
                        format!(
                            "native runtime-filter binding_id={binding_id} ordered key type missing"
                        ),
                    )
                })?;
                let data_type =
                    crate::native::type_decode::decode_type(wire_type).map_err(|error| {
                        NativeFragmentDecodeError::invalid_value(
                            key_path.clone().field("type"),
                            error,
                        )
                    })?;
                let direction = match plan::RuntimeFilterSortDirection::try_from(key.direction) {
                    Ok(plan::RuntimeFilterSortDirection::Ascending) => SortDirection::Ascending,
                    Ok(plan::RuntimeFilterSortDirection::Descending) => SortDirection::Descending,
                    Ok(plan::RuntimeFilterSortDirection::Unspecified) | Err(_) => {
                        return Err(NativeFragmentDecodeError::invalid_enum(
                            key_path.clone().field("direction"),
                            format!(
                                "native runtime-filter binding_id={binding_id} invalid sort direction={}",
                                key.direction
                            ),
                        ));
                    }
                };
                let null_order = match plan::RuntimeFilterNullOrder::try_from(key.null_order) {
                    Ok(plan::RuntimeFilterNullOrder::First) => NullOrder::First,
                    Ok(plan::RuntimeFilterNullOrder::Last) => NullOrder::Last,
                    Ok(plan::RuntimeFilterNullOrder::Unspecified) | Err(_) => {
                        return Err(NativeFragmentDecodeError::invalid_enum(
                            key_path.field("null_order"),
                            format!(
                                "native runtime-filter binding_id={binding_id} invalid null order={}",
                                key.null_order
                            ),
                        ));
                    }
                };
                keys.push(RuntimeOrderKey::new(data_type, direction, null_order));
            }
            if keys[0].data_type() != expression_type {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.clone().field("keys").index(0).field("type"),
                    format!(
                        "native runtime-filter binding_id={binding_id} ordered key type {:?} does not match expression type {:?}",
                        keys[0].data_type(),
                        expression_type
                    ),
                ));
            }
            let comparator = digest32(binding_id, "comparator_digest", &ordered.comparator_digest)
                .map_err(|error| {
                    NativeFragmentDecodeError::invalid_value(
                        path.clone().field("comparator_digest"),
                        error,
                    )
                })?;
            let order_digest = digest32(
                binding_id,
                "order_contract_digest",
                &ordered.order_contract_digest,
            )
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    path.clone().field("order_contract_digest"),
                    error,
                )
            })?;
            let plan_contract = OrderContract {
                keys: keys
                    .iter()
                    .map(|key| OrderKeyContract {
                        data_type: key.data_type().clone(),
                        direction: key.direction(),
                        null_order: key.null_order(),
                    })
                    .collect(),
                inclusive: true,
                comparator_digest: ComparatorDigest::new(comparator),
            };
            let canonical = RuntimeOrderContract::try_from_plan(&plan_contract).map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    path.clone(),
                    format!(
                        "native runtime-filter binding_id={binding_id} ordered contract is noncanonical: {error:?}"
                    ),
                )
            })?;
            if canonical.digest().bytes() != order_digest {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.field("order_contract_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} order contract digest mismatch"
                    ),
                ));
            }
            Ok(DecodedRuntimeFilterContract::Ordered {
                keys: keys.into(),
                comparator_digest: comparator,
                order_contract_digest: order_digest,
            })
        }
    }
}

fn decode_reduction(
    binding_id: u32,
    contract: &DecodedRuntimeFilterContract,
    wire: Option<&plan::RuntimeFilterReductionContract>,
    path: FieldPath,
) -> Result<DecodedRuntimeFilterReduction, NativeFragmentDecodeError> {
    let wire = wire.ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone(),
            format!("native runtime-filter binding_id={binding_id} missing reduction contract"),
        )
    })?;
    let kind = wire.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            format!("native runtime-filter binding_id={binding_id} missing reduction kind"),
        )
    })?;
    match kind {
        plan::runtime_filter_reduction_contract::Kind::SetUnion(true) => {
            Ok(DecodedRuntimeFilterReduction::SetUnion)
        }
        plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(true) => {
            Ok(DecodedRuntimeFilterReduction::TightenOrderedBound)
        }
        plan::runtime_filter_reduction_contract::Kind::SetUnion(false)
        | plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(false) => {
            Err(NativeFragmentDecodeError::invalid_value(
                path.field("kind"),
                format!(
                    "native runtime-filter binding_id={binding_id} reduction marker must be true"
                ),
            ))
        }
        plan::runtime_filter_reduction_contract::Kind::MergeTopkSummary(topk) => {
            let topk_path = path.field("kind").field("merge_topk_summary");
            let k = NonZeroU32::new(topk.k).ok_or_else(|| {
                NativeFragmentDecodeError::invalid_value(
                    topk_path.clone().field("k"),
                    format!("native runtime-filter binding_id={binding_id} TopK K must be nonzero"),
                )
            })?;
            let digest = digest32(binding_id, "TopK contract_digest", &topk.contract_digest)
                .map_err(|error| {
                    NativeFragmentDecodeError::invalid_value(
                        topk_path.clone().field("contract_digest"),
                        error,
                    )
                })?;
            let DecodedRuntimeFilterContract::Ordered {
                keys,
                comparator_digest,
                ..
            } = contract
            else {
                return Err(NativeFragmentDecodeError::inconsistent(
                    topk_path.clone(),
                    format!(
                        "native runtime-filter binding_id={binding_id} TopK reduction requires ordered contract"
                    ),
                ));
            };
            let order = OrderContract {
                keys: keys
                    .iter()
                    .map(|key| OrderKeyContract {
                        data_type: key.data_type().clone(),
                        direction: key.direction(),
                        null_order: key.null_order(),
                    })
                    .collect(),
                inclusive: true,
                comparator_digest: ComparatorDigest::new(*comparator_digest),
            };
            let expected = RuntimeTopKSummaryContract::try_from_plan(
                &order,
                TopKSummaryRequirement::try_new(k.get()).expect("nonzero"),
            )
            .map_err(|error| {
                NativeFragmentDecodeError::invalid_value(
                    topk_path.clone(),
                    format!("native runtime-filter binding_id={binding_id} TopK contract is noncanonical: {error:?}"),
                )
            })?;
            if expected.digest().bytes() != digest {
                return Err(NativeFragmentDecodeError::inconsistent(
                    topk_path.field("contract_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} TopK contract digest mismatch"
                    ),
                ));
            }
            Ok(DecodedRuntimeFilterReduction::MergeTopKSummary {
                k,
                contract_digest: digest,
            })
        }
    }
}

fn decode_role(
    binding_id: u32,
    role: Option<&plan::runtime_filter_binding::Role>,
    path: FieldPath,
) -> Result<DecodedBindingRole, NativeFragmentDecodeError> {
    match role.ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone(),
            format!("native runtime-filter binding_id={binding_id} missing role"),
        )
    })? {
        plan::runtime_filter_binding::Role::Producer(producer) => {
            let producer_path = path.field("producer");
            let mut contribution_kinds = BTreeSet::new();
            for (index, raw) in producer.contribution_kinds.iter().copied().enumerate() {
                let item_path = producer_path
                    .clone()
                    .field("contribution_kinds")
                    .index(index);
                let kind = decode_runtime_filter_contribution_kind(raw, item_path.clone())?;
                if !contribution_kinds.insert(kind) {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        item_path,
                        format!(
                            "native runtime-filter binding_id={binding_id} duplicate contribution kind={raw}"
                        ),
                    ));
                }
            }
            if contribution_kinds.is_empty() {
                return Err(NativeFragmentDecodeError::invalid_value(
                    producer_path.clone().field("contribution_kinds"),
                    format!(
                        "native runtime-filter binding_id={binding_id} producer contribution kinds must be nonempty"
                    ),
                ));
            }
            let completion_requirement = decode_runtime_filter_completion(
                producer.completion_requirement,
                producer_path.clone().field("completion_requirement"),
            )?;
            let target_path = producer_path.field("target");
            let target = match producer.target.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    target_path.clone(),
                    format!(
                        "native runtime-filter producer binding_id={binding_id} missing target"
                    ),
                )
            })? {
                plan::runtime_filter_producer_role::Target::JoinBuildKey(join) => {
                    ProducerBindingTarget::JoinBuildKey {
                        ordinal: usize::try_from(join.ordinal).map_err(|_| {
                            NativeFragmentDecodeError::invalid_value(
                                target_path.clone().field("join_build_key").field("ordinal"),
                                format!(
                                    "native runtime-filter producer binding_id={binding_id} join build key ordinal does not fit usize"
                                ),
                            )
                        })?,
                    }
                }
                plan::runtime_filter_producer_role::Target::AggregateTopnKey(aggregate) => {
                    ProducerBindingTarget::AggregateTopNKey {
                        ordinal: usize::try_from(aggregate.group_key_ordinal).map_err(
                            |_| {
                                NativeFragmentDecodeError::invalid_value(
                                    target_path
                                        .clone()
                                        .field("aggregate_topn_key")
                                        .field("group_key_ordinal"),
                                    format!(
                                        "native runtime-filter producer binding_id={binding_id} aggregate TopN group key ordinal does not fit usize"
                                    ),
                                )
                            },
                        )?,
                        limit: NonZeroU32::new(aggregate.limit).ok_or_else(|| {
                            NativeFragmentDecodeError::invalid_value(
                                target_path
                                    .clone()
                                    .field("aggregate_topn_key")
                                    .field("limit"),
                                format!(
                                    "native runtime-filter producer binding_id={binding_id} aggregate TopN limit must be nonzero"
                                ),
                            )
                        })?,
                    }
                }
            };
            Ok(DecodedBindingRole::Producer {
                contribution_kinds,
                completion_requirement,
                target,
            })
        }
        plan::runtime_filter_binding::Role::Consumer(consumer) => {
            let consumer_path = path.field("consumer");
            let mut capabilities = BTreeSet::new();
            for (index, raw) in consumer.capabilities.iter().copied().enumerate() {
                let item_path = consumer_path.clone().field("capabilities").index(index);
                let capability = decode_runtime_filter_capability(raw, item_path.clone())?;
                if !capabilities.insert(capability) {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        item_path,
                        format!(
                            "native runtime-filter binding_id={binding_id} duplicate capability={raw}"
                        ),
                    ));
                }
            }
            if capabilities.is_empty() {
                return Err(NativeFragmentDecodeError::invalid_value(
                    consumer_path.clone().field("capabilities"),
                    format!(
                        "native runtime-filter binding_id={binding_id} consumer capabilities must be nonempty"
                    ),
                ));
            }
            let activation = decode_runtime_filter_activation(
                consumer.activation.as_ref(),
                consumer_path.clone().field("activation"),
            )?;
            let target_path = consumer_path.field("target");
            let target = match consumer.target.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    target_path.clone(),
                    format!(
                        "native runtime-filter consumer binding_id={binding_id} missing target"
                    ),
                )
            })? {
                plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(raw) => {
                    DecodedConsumerBindingTarget::DirectInput {
                        input_ordinal: usize::try_from(*raw).map_err(|_| {
                            NativeFragmentDecodeError::invalid_value(
                                target_path.clone().field("direct_input_ordinal"),
                                format!(
                                    "native runtime-filter consumer binding_id={binding_id} input ordinal does not fit usize"
                                ),
                            )
                        })?,
                    }
                }
                plan::runtime_filter_consumer_role::Target::SourceBoundary(true) => {
                    DecodedConsumerBindingTarget::SourceBoundary
                }
                plan::runtime_filter_consumer_role::Target::SourceBoundary(false) => {
                    return Err(NativeFragmentDecodeError::invalid_value(
                        target_path.field("source_boundary"),
                        format!(
                            "native runtime-filter consumer binding_id={binding_id} source boundary marker must be true"
                        ),
                    ));
                }
            };
            Ok(DecodedBindingRole::Consumer {
                capabilities,
                activation,
                target,
            })
        }
    }
}

fn validate_role_contract(
    binding_id: u32,
    contract: &DecodedRuntimeFilterContract,
    reduction: &DecodedRuntimeFilterReduction,
    role: &DecodedBindingRole,
) -> Result<(), String> {
    match (contract, reduction) {
        (
            DecodedRuntimeFilterContract::Membership { .. },
            DecodedRuntimeFilterReduction::SetUnion,
        )
        | (
            DecodedRuntimeFilterContract::Ordered { .. },
            DecodedRuntimeFilterReduction::TightenOrderedBound,
        )
        | (
            DecodedRuntimeFilterContract::Ordered { .. },
            DecodedRuntimeFilterReduction::MergeTopKSummary { .. },
        ) => {}
        _ => {
            return Err(format!(
                "native runtime-filter binding_id={binding_id} contract/reduction mismatch"
            ));
        }
    }
    match role {
        DecodedBindingRole::Consumer { capabilities, .. } => {
            let expected = match contract {
                DecodedRuntimeFilterContract::Membership { .. } => BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                DecodedRuntimeFilterContract::Ordered { .. } => {
                    BTreeSet::from([ArtifactCapability::OrderedRange])
                }
            };
            if capabilities != &expected {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} consumer capabilities do not match the canonical role contract"
                ));
            }
            if matches!(contract, DecodedRuntimeFilterContract::Ordered { .. })
                && matches!(
                    role,
                    DecodedBindingRole::Consumer {
                        activation: ConsumerActivation::BlockingSnapshot,
                        ..
                    }
                )
            {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} ordered consumer cannot block on feedback"
                ));
            }
        }
        DecodedBindingRole::Producer {
            contribution_kinds,
            completion_requirement,
            ..
        } => {
            let (expected, expected_completion) = match reduction {
                DecodedRuntimeFilterReduction::SetUnion
                    if contribution_kinds.contains(&ContributionKind::FinalDomainShard) =>
                {
                    (
                        BTreeSet::from([
                            ContributionKind::FinalDomainShard,
                            ContributionKind::ProducerClosed,
                        ]),
                        CompletionRequirement::FencedFinalDomain(
                            CompletionFenceKind::CommittedDomainFrozen,
                        ),
                    )
                }
                DecodedRuntimeFilterReduction::SetUnion => (
                    BTreeSet::from([
                        ContributionKind::ValueDomainDelta,
                        ContributionKind::ProducerClosed,
                    ]),
                    CompletionRequirement::ProducerClosed,
                ),
                DecodedRuntimeFilterReduction::TightenOrderedBound => (
                    BTreeSet::from([
                        ContributionKind::OrderedBoundUpdate,
                        ContributionKind::ProducerClosed,
                    ]),
                    CompletionRequirement::ProducerClosed,
                ),
                DecodedRuntimeFilterReduction::MergeTopKSummary { .. } => (
                    BTreeSet::from([
                        ContributionKind::TopKSummary,
                        ContributionKind::ProducerClosed,
                    ]),
                    CompletionRequirement::ProducerClosed,
                ),
            };
            if contribution_kinds != &expected || completion_requirement != &expected_completion {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} producer contribution/completion contract mismatch"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use prost::Message;

    use super::*;
    use novarocks::exec::node::runtime_filter::{
        ArtifactMembershipSchema, NullOrder, NullSemantics, OrderContract, OrderKeyContract,
        RuntimeOrderContract, RuntimeTopKSummaryContract, SortDirection, TopKSummaryRequirement,
        comparator_digest_for_plan,
    };
    use novarocks::protocol::ProtocolErrorKind;
    use novarocks_protocol::expr;

    fn int64_type() -> novarocks_protocol::common::TypeDesc {
        novarocks_protocol::common::TypeDesc {
            kind: Some(novarocks_protocol::common::type_desc::Kind::Scalar(
                novarocks_protocol::common::ScalarType {
                    r#type: novarocks_protocol::common::PrimitiveType::Bigint as i32,
                    ..Default::default()
                },
            )),
        }
    }

    fn expression(column_id: u32) -> expr::Expr {
        expr::Expr {
            r#type: Some(int64_type()),
            nullable: false,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    fn cast_expression(operand: expr::Expr) -> expr::Expr {
        expr::Expr {
            r#type: Some(int64_type()),
            nullable: false,
            kind: Some(expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(operand)),
                target: Some(int64_type()),
            }))),
        }
    }

    fn binary_expression(op: i32) -> expr::Expr {
        expr::Expr {
            r#type: Some(int64_type()),
            nullable: false,
            kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op,
                left: Some(Box::new(expression(1))),
                right: Some(Box::new(expression(2))),
            }))),
        }
    }

    fn membership_binding(binding_id: u32, node_id: i32) -> plan::RuntimeFilterBinding {
        let schema = ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches)
            .expect("schema");
        plan::RuntimeFilterBinding {
            binding_id,
            channel_id: 9,
            node_id,
            apply_point: i32::from(plan::RuntimeFilterApplyPoint::NodeInput),
            expression: Some(expression(1)),
            contract: Some(plan::RuntimeFilterContract {
                kind: Some(plan::runtime_filter_contract::Kind::Membership(
                    plan::RuntimeFilterMembershipContract {
                        canonical_schema: schema.canonical_bytes().to_vec(),
                        schema_digest: schema.digest().bytes().to_vec(),
                    },
                )),
            }),
            reduction: Some(plan::RuntimeFilterReductionContract {
                kind: Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                    true,
                )),
            }),
            role: Some(plan::runtime_filter_binding::Role::Consumer(
                plan::RuntimeFilterConsumerRole {
                    capabilities: vec![
                        i32::from(plan::RuntimeFilterArtifactCapability::Membership),
                        i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain),
                    ],
                    activation: Some(plan::RuntimeFilterConsumerActivation {
                        kind: Some(
                            plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true),
                        ),
                    }),
                    target: Some(plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(0)),
                },
            )),
        }
    }

    #[test]
    fn binding_table_decode_requires_exact_consumer_target() {
        let mut missing_target = membership_binding(1, 11);
        let Some(plan::runtime_filter_binding::Role::Consumer(role)) = missing_target.role.as_mut()
        else {
            panic!("consumer")
        };
        role.target = None;
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![missing_target])),)
                .is_err()
        );
    }

    fn producer_role_with_target(
        target: Option<plan::runtime_filter_producer_role::Target>,
    ) -> plan::runtime_filter_binding::Role {
        plan::runtime_filter_binding::Role::Producer(plan::RuntimeFilterProducerRole {
            contribution_kinds: vec![
                i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta),
                i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
            ],
            completion_requirement: i32::from(
                plan::RuntimeFilterCompletionRequirement::ProducerClosed,
            ),
            target,
        })
    }

    fn decode_producer_binding_target(
        target: Option<plan::runtime_filter_producer_role::Target>,
    ) -> Result<super::ProducerBindingTarget, super::NativeFragmentDecodeError> {
        let role = producer_role_with_target(target);
        let DecodedBindingRole::Producer { target, .. } =
            decode_role(17, Some(&role), FieldPath::root("binding").field("role"))?
        else {
            unreachable!("fixture always carries a producer role")
        };
        Ok(target)
    }

    #[test]
    fn producer_binding_target_proto_round_trips_both_variants_exactly() {
        let cases = [
            (
                super::ProducerBindingTarget::JoinBuildKey { ordinal: 7 },
                plan::runtime_filter_producer_role::Target::JoinBuildKey(
                    plan::RuntimeFilterJoinBuildKey { ordinal: 7 },
                ),
            ),
            (
                super::ProducerBindingTarget::AggregateTopNKey {
                    ordinal: 11,
                    limit: NonZeroU32::new(19).unwrap(),
                },
                plan::runtime_filter_producer_role::Target::AggregateTopnKey(
                    plan::RuntimeFilterAggregateTopNKey {
                        group_key_ordinal: 11,
                        limit: 19,
                    },
                ),
            ),
        ];

        for (expected, wire_target) in cases {
            let wire = match producer_role_with_target(Some(wire_target)) {
                plan::runtime_filter_binding::Role::Producer(wire) => wire,
                _ => unreachable!("helper always returns producer"),
            };
            let bytes = wire.encode_to_vec();
            let round_tripped = plan::RuntimeFilterProducerRole::decode(bytes.as_slice())
                .expect("proto round trip");
            let decoded = decode_producer_binding_target(round_tripped.target)
                .expect("typed producer target");
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn producer_binding_target_decode_rejects_zero_limit_and_missing_target() {
        let zero_limit = plan::runtime_filter_producer_role::Target::AggregateTopnKey(
            plan::RuntimeFilterAggregateTopNKey {
                group_key_ordinal: 1,
                limit: 0,
            },
        );
        assert!(decode_producer_binding_target(Some(zero_limit)).is_err());
        assert!(decode_producer_binding_target(None).is_err());
    }

    fn table(
        fragment_id: u32,
        bindings: Vec<plan::RuntimeFilterBinding>,
    ) -> plan::RuntimeFilterBindingTable {
        plan::RuntimeFilterBindingTable {
            fragment_id,
            bindings,
        }
    }

    fn ordered_topk_binding(binding_id: u32, node_id: i32) -> plan::RuntimeFilterBinding {
        ordered_topk_binding_with_keys(
            binding_id,
            node_id,
            vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Descending,
                null_order: NullOrder::First,
            }],
        )
    }

    fn ordered_topk_binding_with_keys(
        binding_id: u32,
        node_id: i32,
        keys: Vec<OrderKeyContract>,
    ) -> plan::RuntimeFilterBinding {
        let comparator = comparator_digest_for_plan(&keys).expect("comparator");
        let order = OrderContract {
            keys: keys.clone(),
            inclusive: true,
            comparator_digest: comparator,
        };
        let canonical_order = RuntimeOrderContract::try_from_plan(&order).expect("order");
        let canonical_topk = RuntimeTopKSummaryContract::try_from_plan(
            &order,
            TopKSummaryRequirement::try_new(13).expect("k"),
        )
        .expect("topk");
        plan::RuntimeFilterBinding {
            binding_id,
            channel_id: 10,
            node_id,
            apply_point: i32::from(plan::RuntimeFilterApplyPoint::NodeInput),
            expression: Some(expression(1)),
            contract: Some(plan::RuntimeFilterContract {
                kind: Some(plan::runtime_filter_contract::Kind::Ordered(
                    plan::RuntimeFilterOrderedContract {
                        keys: keys
                            .iter()
                            .map(|key| plan::RuntimeFilterOrderKey {
                                r#type: Some(int64_type()),
                                direction: i32::from(match key.direction {
                                    SortDirection::Ascending => {
                                        plan::RuntimeFilterSortDirection::Ascending
                                    }
                                    SortDirection::Descending => {
                                        plan::RuntimeFilterSortDirection::Descending
                                    }
                                }),
                                null_order: i32::from(match key.null_order {
                                    NullOrder::First => plan::RuntimeFilterNullOrder::First,
                                    NullOrder::Last => plan::RuntimeFilterNullOrder::Last,
                                }),
                            })
                            .collect(),
                        comparator_digest: comparator.get().to_vec(),
                        order_contract_digest: canonical_order.digest().bytes().to_vec(),
                    },
                )),
            }),
            reduction: Some(plan::RuntimeFilterReductionContract {
                kind: Some(
                    plan::runtime_filter_reduction_contract::Kind::MergeTopkSummary(
                        plan::RuntimeFilterTopKReduction {
                            k: 13,
                            contract_digest: canonical_topk.digest().bytes().to_vec(),
                        },
                    ),
                ),
            }),
            role: Some(plan::runtime_filter_binding::Role::Consumer(
                plan::RuntimeFilterConsumerRole {
                    capabilities: vec![i32::from(
                        plan::RuntimeFilterArtifactCapability::OrderedRange,
                    )],
                    activation: Some(plan::RuntimeFilterConsumerActivation {
                        kind: Some(
                            plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(
                                i32::from(plan::RuntimeFilterLateApplyGranularity::Batch),
                            ),
                        ),
                    }),
                    target: Some(plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(0)),
                },
            )),
        }
    }

    #[test]
    fn binding_table_decode_rejects_missing_duplicate_unknown_enum_and_wrong_fragment() {
        let valid = table(7, vec![membership_binding(1, 11)]);
        NativeRuntimeFilterDecodeLedger::decode(7, Some(&valid)).expect("valid table");
        assert!(NativeRuntimeFilterDecodeLedger::decode(7, None).is_err());
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(
                7,
                Some(&table(
                    7,
                    vec![membership_binding(1, 11), membership_binding(1, 11)]
                )),
            )
            .is_err()
        );
        let mut unknown = membership_binding(1, 11);
        unknown.apply_point = 99_999;
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![unknown]))).is_err()
        );
        assert!(NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(8, Vec::new()))).is_err());
    }

    #[test]
    fn binding_table_decode_rejects_nested_expression_missing_type_or_kind() {
        let mut missing_type = membership_binding(1, 11);
        let mut child = expression(1);
        child.r#type = None;
        missing_type.expression = Some(cast_expression(child));
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![missing_type])))
                .is_err(),
            "nested expression without type must be rejected during binding decode"
        );

        let mut missing_kind = membership_binding(1, 11);
        let mut child = expression(1);
        child.kind = None;
        missing_kind.expression = Some(cast_expression(child));
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![missing_kind])))
                .is_err(),
            "nested expression without kind must be rejected during binding decode"
        );
    }

    #[test]
    fn binding_table_decode_rejects_nested_expression_illegal_enum() {
        let mut binding = membership_binding(1, 11);
        binding.expression = Some(cast_expression(binary_expression(99_999)));
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding]))).is_err(),
            "nested illegal expression enum must be rejected during binding decode"
        );
    }

    #[test]
    fn binding_table_decode_rejects_window_frame_without_end() {
        let mut binding = membership_binding(1, 11);
        binding.expression = Some(expr::Expr {
            r#type: Some(int64_type()),
            nullable: false,
            kind: Some(expr::expr::Kind::WindowCall(expr::WindowCall {
                function_name: "row_number".to_string(),
                args: Vec::new(),
                distinct: false,
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: Some(expr::WindowFrame {
                    frame_type: i32::from(expr::WindowFrameType::Rows),
                    start: Some(expr::WindowBound {
                        bound: Some(expr::window_bound::Bound::CurrentRow(true)),
                    }),
                    end: None,
                }),
                ignore_nulls: false,
            })),
        });
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding]))).is_err(),
            "window frame without end must be rejected during binding decode"
        );
    }

    #[test]
    fn binding_table_decode_rejects_two_key_ordered_contract() {
        let binding = ordered_topk_binding_with_keys(
            1,
            11,
            vec![
                OrderKeyContract {
                    data_type: DataType::Int64,
                    direction: SortDirection::Descending,
                    null_order: NullOrder::First,
                },
                OrderKeyContract {
                    data_type: DataType::Utf8,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::Last,
                },
            ],
        );
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding]))).is_err(),
            "ordered binding contract must contain exactly one key"
        );
    }

    #[test]
    fn binding_table_decode_rejects_ordered_key_type_mismatch() {
        let binding = ordered_topk_binding_with_keys(
            1,
            11,
            vec![OrderKeyContract {
                data_type: DataType::Utf8,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }],
        );
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding]))).is_err(),
            "ordered key type must match binding expression type"
        );
    }

    #[test]
    fn ordered_contract_direction_error_uses_exact_path_and_kind() {
        let mut binding = ordered_topk_binding(1, 11);
        let plan::runtime_filter_contract::Kind::Ordered(ordered) = binding
            .contract
            .as_mut()
            .and_then(|contract| contract.kind.as_mut())
            .expect("ordered contract")
        else {
            panic!("ordered contract");
        };
        ordered.keys[0].direction = plan::RuntimeFilterSortDirection::Unspecified as i32;

        let error = match NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding])))
        {
            Ok(_) => panic!("unspecified direction must fail"),
            Err(error) => error,
        };
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.runtime_filter_bindings.bindings[0].contract.ordered.keys[0].direction"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidEnum);
    }

    #[test]
    fn topk_reduction_k_error_uses_exact_path_and_kind() {
        let mut binding = ordered_topk_binding(1, 11);
        let plan::runtime_filter_reduction_contract::Kind::MergeTopkSummary(topk) = binding
            .reduction
            .as_mut()
            .and_then(|reduction| reduction.kind.as_mut())
            .expect("topk reduction")
        else {
            panic!("topk reduction");
        };
        topk.k = 0;

        let error = match NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding])))
        {
            Ok(_) => panic!("zero TopK K must fail"),
            Err(error) => error,
        };
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.runtime_filter_bindings.bindings[0].reduction.kind.merge_topk_summary.k"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
    }

    #[test]
    fn missing_reduction_message_uses_message_path_and_kind() {
        let mut binding = membership_binding(1, 11);
        binding.reduction = None;

        let error = match NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding])))
        {
            Ok(_) => panic!("missing reduction message must fail"),
            Err(error) => error,
        };
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.runtime_filter_bindings.bindings[0].reduction"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn missing_activation_message_uses_message_path_and_kind() {
        let mut binding = membership_binding(1, 11);
        let plan::runtime_filter_binding::Role::Consumer(consumer) =
            binding.role.as_mut().expect("consumer role")
        else {
            panic!("consumer role");
        };
        consumer.activation = None;

        let error = match NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding])))
        {
            Ok(_) => panic!("missing activation message must fail"),
            Err(error) => error,
        };
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.runtime_filter_bindings.bindings[0].role.consumer.activation"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn missing_activation_kind_uses_oneof_path_and_kind() {
        let mut binding = membership_binding(1, 11);
        let plan::runtime_filter_binding::Role::Consumer(consumer) =
            binding.role.as_mut().expect("consumer role")
        else {
            panic!("consumer role");
        };
        consumer.activation = Some(plan::RuntimeFilterConsumerActivation::default());

        let error = match NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding])))
        {
            Ok(_) => panic!("missing activation kind must fail"),
            Err(error) => error,
        };
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.runtime_filter_bindings.bindings[0].role.consumer.activation.kind"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
    }

    #[test]
    fn producer_contribution_kind_error_uses_exact_path_and_kind() {
        let mut binding = membership_binding(1, 11);
        binding.role = Some(plan::runtime_filter_binding::Role::Producer(
            plan::RuntimeFilterProducerRole {
                contribution_kinds: vec![plan::RuntimeFilterContributionKind::Unspecified as i32],
                completion_requirement: plan::RuntimeFilterCompletionRequirement::ProducerClosed
                    as i32,
                target: Some(plan::runtime_filter_producer_role::Target::JoinBuildKey(
                    plan::RuntimeFilterJoinBuildKey { ordinal: 0 },
                )),
            },
        ));

        let error = match NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![binding])))
        {
            Ok(_) => panic!("unspecified contribution kind must fail"),
            Err(error) => error,
        };
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.runtime_filter_bindings.bindings[0].role.producer.contribution_kinds[0]"
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidEnum);
    }

    #[test]
    fn binding_table_decode_recomputes_membership_order_and_topk_digests() {
        let mut membership = membership_binding(1, 11);
        NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![membership.clone()])))
            .expect("valid membership");
        let plan::runtime_filter_contract::Kind::Membership(contract) = membership
            .contract
            .as_mut()
            .and_then(|contract| contract.kind.as_mut())
            .expect("membership")
        else {
            panic!("membership")
        };
        contract.schema_digest[0] ^= 1;
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![membership]))).is_err()
        );

        let ordered = ordered_topk_binding(2, 11);
        let ledger =
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![ordered.clone()])))
                .expect("single-key ordered TopK");
        let record = ledger.lookup_for_node(2, 11, 7).expect("binding");
        let DecodedRuntimeFilterContract::Ordered { keys, .. } = &record.contract else {
            panic!("ordered")
        };
        assert_eq!(keys.len(), 1);

        let mut wrong_comparator = ordered.clone();
        let plan::runtime_filter_contract::Kind::Ordered(contract) = wrong_comparator
            .contract
            .as_mut()
            .and_then(|contract| contract.kind.as_mut())
            .expect("ordered")
        else {
            panic!("ordered")
        };
        contract.comparator_digest[0] ^= 1;
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![wrong_comparator])))
                .is_err()
        );

        let mut wrong_order = ordered.clone();
        let plan::runtime_filter_contract::Kind::Ordered(contract) = wrong_order
            .contract
            .as_mut()
            .and_then(|contract| contract.kind.as_mut())
            .expect("ordered")
        else {
            panic!("ordered")
        };
        contract.order_contract_digest[0] ^= 1;
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![wrong_order]))).is_err()
        );

        let mut wrong_topk = ordered;
        let plan::runtime_filter_reduction_contract::Kind::MergeTopkSummary(topk) = wrong_topk
            .reduction
            .as_mut()
            .and_then(|reduction| reduction.kind.as_mut())
            .expect("topk")
        else {
            panic!("topk")
        };
        topk.contract_digest[0] ^= 1;
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![wrong_topk]))).is_err()
        );
    }

    #[test]
    fn node_lookup_rejects_unknown_duplicate_wrong_node_role_and_apply_point() {
        let valid = table(7, vec![membership_binding(1, 11)]);
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(7, Some(&valid)).expect("decode");
        assert!(ledger.lookup_for_node(999, 11, 7).is_err());
        assert!(ledger.lookup_for_node(1, 12, 7).is_err());
        ledger.lookup_for_node(1, 11, 7).expect("lookup");
        ledger.commit_consumed(1).expect("commit");
        assert!(ledger.lookup_for_node(1, 11, 7).is_err());

        let mut consumer_at_output = membership_binding(2, 11);
        consumer_at_output.apply_point = i32::from(plan::RuntimeFilterApplyPoint::NodeOutput);
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![consumer_at_output])),)
                .is_err()
        );

        let mut producer_at_input = membership_binding(3, 11);
        producer_at_input.role = Some(plan::runtime_filter_binding::Role::Producer(
            plan::RuntimeFilterProducerRole {
                contribution_kinds: vec![
                    i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta),
                    i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
                ],
                completion_requirement: i32::from(
                    plan::RuntimeFilterCompletionRequirement::ProducerClosed,
                ),
                target: Some(plan::runtime_filter_producer_role::Target::JoinBuildKey(
                    plan::RuntimeFilterJoinBuildKey { ordinal: 0 },
                )),
            },
        ));
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![producer_at_input])),)
                .is_err()
        );
    }

    #[test]
    fn node_lookup_rejects_matching_node_id_from_different_wire_fragment() {
        let valid = table(7, vec![membership_binding(1, 11)]);
        let ledger = NativeRuntimeFilterDecodeLedger::decode(7, Some(&valid)).expect("decode");
        assert!(ledger.lookup_for_node(1, 11, 8).is_err());
        assert!(ledger.peek_attached(&[1], 11, 8).is_err());
    }

    #[test]
    fn role_contract_rejects_missing_extra_and_wrong_completion() {
        let mut missing_consumer_capability = membership_binding(1, 11);
        let Some(plan::runtime_filter_binding::Role::Consumer(role)) =
            missing_consumer_capability.role.as_mut()
        else {
            panic!("consumer")
        };
        role.capabilities.pop();
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(
                7,
                Some(&table(7, vec![missing_consumer_capability])),
            )
            .is_err()
        );

        let mut extra_consumer_capability = membership_binding(1, 11);
        let Some(plan::runtime_filter_binding::Role::Consumer(role)) =
            extra_consumer_capability.role.as_mut()
        else {
            panic!("consumer")
        };
        role.capabilities.push(i32::from(
            plan::RuntimeFilterArtifactCapability::OrderedRange,
        ));
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(
                7,
                Some(&table(7, vec![extra_consumer_capability])),
            )
            .is_err()
        );

        let producer_role = |kinds, completion| {
            plan::runtime_filter_binding::Role::Producer(plan::RuntimeFilterProducerRole {
                contribution_kinds: kinds,
                completion_requirement: i32::from(completion),
                target: Some(plan::runtime_filter_producer_role::Target::JoinBuildKey(
                    plan::RuntimeFilterJoinBuildKey { ordinal: 0 },
                )),
            })
        };
        let mut extra_producer_kind = membership_binding(1, 11);
        extra_producer_kind.apply_point = i32::from(plan::RuntimeFilterApplyPoint::NodeOutput);
        extra_producer_kind.role = Some(producer_role(
            vec![
                i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta),
                i32::from(plan::RuntimeFilterContributionKind::FinalDomainShard),
                i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
            ],
            plan::RuntimeFilterCompletionRequirement::ProducerClosed,
        ));
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![extra_producer_kind])),)
                .is_err()
        );

        let mut wrong_completion = membership_binding(1, 11);
        wrong_completion.apply_point = i32::from(plan::RuntimeFilterApplyPoint::NodeOutput);
        wrong_completion.role = Some(producer_role(
            vec![
                i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta),
                i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
            ],
            plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen,
        ));
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![wrong_completion])),)
                .is_err()
        );

        let mut final_domain = membership_binding(1, 11);
        final_domain.apply_point = i32::from(plan::RuntimeFilterApplyPoint::NodeOutput);
        final_domain.role = Some(producer_role(
            vec![
                i32::from(plan::RuntimeFilterContributionKind::FinalDomainShard),
                i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
            ],
            plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen,
        ));
        NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![final_domain])))
            .expect("canonical final-domain producer");

        let mut blocking_ordered = ordered_topk_binding(1, 11);
        let Some(plan::runtime_filter_binding::Role::Consumer(role)) =
            blocking_ordered.role.as_mut()
        else {
            panic!("consumer")
        };
        role.activation = Some(plan::RuntimeFilterConsumerActivation {
            kind: Some(plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true)),
        });
        assert!(
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![blocking_ordered])),)
                .is_err()
        );
    }

    #[test]
    fn fragment_finish_rejects_unconsumed_binding() {
        let valid = table(7, vec![membership_binding(1, 11)]);
        let ledger = NativeRuntimeFilterDecodeLedger::decode(7, Some(&valid)).expect("decode");
        assert!(ledger.finish().is_err());
        NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, Vec::new())))
            .expect("empty")
            .finish()
            .expect("all consumed");
    }

    #[test]
    fn finish_succeeds_only_after_complete_consumption() {
        let consumer = membership_binding(1, 11);
        let mut producer = membership_binding(2, 12);
        producer.apply_point = i32::from(plan::RuntimeFilterApplyPoint::NodeOutput);
        producer.role = Some(plan::runtime_filter_binding::Role::Producer(
            plan::RuntimeFilterProducerRole {
                contribution_kinds: vec![
                    i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta),
                    i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
                ],
                completion_requirement: i32::from(
                    plan::RuntimeFilterCompletionRequirement::ProducerClosed,
                ),
                target: Some(plan::runtime_filter_producer_role::Target::JoinBuildKey(
                    plan::RuntimeFilterJoinBuildKey { ordinal: 0 },
                )),
            },
        ));
        let mut ledger =
            NativeRuntimeFilterDecodeLedger::decode(7, Some(&table(7, vec![consumer, producer])))
                .expect("decode");

        ledger.lookup_for_node(2, 12, 7).expect("producer lookup");
        ledger.commit_consumed(2).expect("producer consumed");
        ledger.lookup_for_node(1, 11, 7).expect("consumer lookup");
        ledger.commit_consumed(1).expect("consumer consumed");

        ledger.finish().expect("all bindings consumed");
    }
}
