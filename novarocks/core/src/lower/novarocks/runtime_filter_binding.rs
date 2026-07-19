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

use crate::proto::{expr, plan};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, ComparatorDigest, CompletionFenceKind, CompletionRequirement,
    ConsumerActivation, ContributionKind, LateApplyGranularity, NullOrder, OrderContract,
    OrderKeyContract, SortDirection, TopKSummaryRequirement,
};
use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
use crate::runtime_filter::port::ordered_bound::{RuntimeOrderContract, RuntimeOrderKey};
use crate::runtime_filter::port::topk_summary::RuntimeTopKSummaryContract;

#[derive(Clone, Debug)]
pub(crate) struct DecodedRuntimeFilterBinding {
    pub(crate) binding_id: u32,
    pub(crate) channel_id: u32,
    pub(crate) node_id: i32,
    pub(crate) apply_point: DecodedApplyPoint,
    pub(crate) expression: expr::Expr,
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
        join_key_ordinal: usize,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeRuntimeFilterDormancyRole {
    Producer,
    Consumer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NativeRuntimeFilterDormancyFact {
    pub(crate) binding_id: u32,
    pub(crate) channel_id: u32,
    pub(crate) node_id: i32,
    pub(crate) apply_point: DecodedApplyPoint,
    pub(crate) role: NativeRuntimeFilterDormancyRole,
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

pub(crate) struct RuntimeFilterBindingLookupLedger {
    fragment_id: u32,
    records: BTreeMap<u32, DecodedRuntimeFilterBinding>,
    consumed: BTreeMap<u32, ()>,
}

impl RuntimeFilterBindingLookupLedger {
    pub(crate) fn decode(
        enclosing_fragment_id: u32,
        table: Option<&plan::RuntimeFilterBindingTable>,
    ) -> Result<Self, String> {
        let table = table.ok_or_else(|| {
            format!("native PlanFragment fragment_id={enclosing_fragment_id} missing runtime_filter_bindings")
        })?;
        if table.fragment_id != enclosing_fragment_id {
            return Err(format!(
                "native runtime-filter binding table fragment_id={} does not match enclosing fragment_id={enclosing_fragment_id}",
                table.fragment_id
            ));
        }
        let mut records = BTreeMap::new();
        let mut previous_id = None;
        for wire in &table.bindings {
            if previous_id.is_some_and(|previous| wire.binding_id <= previous) {
                return Err(format!(
                    "native runtime-filter binding table fragment_id={enclosing_fragment_id} bindings must be strictly ordered by binding_id"
                ));
            }
            let record = decode_binding(wire)?;
            if records.insert(record.binding_id, record).is_some() {
                return Err(format!(
                    "native runtime-filter binding table fragment_id={enclosing_fragment_id} has duplicate binding_id={}",
                    wire.binding_id
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

    pub(crate) fn lookup_for_node(
        &self,
        binding_id: u32,
        node_id: i32,
        node_fragment_id: u32,
    ) -> Result<&DecodedRuntimeFilterBinding, String> {
        if node_fragment_id != self.fragment_id {
            return Err(format!(
                "native node_id={node_id} fragment_id={node_fragment_id} cannot reference runtime-filter binding table fragment_id={}",
                self.fragment_id
            ));
        }
        if self.consumed.contains_key(&binding_id) {
            return Err(format!(
                "native runtime-filter binding_id={binding_id} is attached more than once"
            ));
        }
        let record = self.records.get(&binding_id).ok_or_else(|| {
            format!(
                "native node_id={node_id} references unknown runtime-filter binding_id={binding_id}"
            )
        })?;
        if record.node_id != node_id {
            return Err(format!(
                "native runtime-filter binding_id={binding_id} belongs to node_id={}, not attachment node_id={node_id}",
                record.node_id
            ));
        }
        Ok(record)
    }

    pub(crate) fn peek_attached(
        &self,
        binding_ids: &[u32],
        node_id: i32,
        node_fragment_id: u32,
    ) -> Result<Vec<DecodedRuntimeFilterBinding>, String> {
        let mut seen = BTreeSet::new();
        binding_ids.iter().copied().map(|binding_id| {
            if !seen.insert(binding_id) {
                return Err(format!("native node_id={node_id} has duplicate runtime-filter binding attachment id={binding_id}"));
            }
            self.lookup_for_node(binding_id, node_id, node_fragment_id).cloned()
        }).collect()
    }

    pub(crate) fn commit_consumed(&mut self, binding_id: u32) -> Result<(), String> {
        if !self.records.contains_key(&binding_id) {
            return Err(format!(
                "cannot consume unknown runtime-filter binding_id={binding_id}"
            ));
        }
        if self.consumed.insert(binding_id, ()).is_some() {
            return Err(format!(
                "runtime-filter binding_id={binding_id} consumed more than once"
            ));
        }
        Ok(())
    }

    pub(crate) fn commit_consumed_many(&mut self, binding_ids: &[u32]) -> Result<(), String> {
        let mut unique = BTreeSet::new();
        for binding_id in binding_ids {
            if !unique.insert(*binding_id)
                || !self.records.contains_key(binding_id)
                || self.consumed.contains_key(binding_id)
            {
                return Err(format!(
                    "cannot atomically consume runtime-filter binding_id={binding_id}"
                ));
            }
        }
        for binding_id in unique {
            self.consumed.insert(binding_id, ());
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<NativeRuntimeFilterDormancyFact>, String> {
        if let Some(binding_id) = self
            .records
            .keys()
            .find(|id| !self.consumed.contains_key(id))
        {
            return Err(format!(
                "native runtime-filter binding table fragment_id={} has unconsumed binding_id={binding_id}",
                self.fragment_id
            ));
        }
        Ok(self
            .records
            .into_values()
            .map(|binding| NativeRuntimeFilterDormancyFact {
                binding_id: binding.binding_id,
                channel_id: binding.channel_id,
                node_id: binding.node_id,
                apply_point: binding.apply_point,
                role: match binding.role {
                    DecodedBindingRole::Producer { .. } => {
                        NativeRuntimeFilterDormancyRole::Producer
                    }
                    DecodedBindingRole::Consumer { .. } => {
                        NativeRuntimeFilterDormancyRole::Consumer
                    }
                },
            })
            .collect())
    }
}

fn decode_binding(
    wire: &plan::RuntimeFilterBinding,
) -> Result<DecodedRuntimeFilterBinding, String> {
    let apply_point = plan::RuntimeFilterApplyPoint::try_from(wire.apply_point).map_err(|_| {
        format!(
            "native runtime-filter binding_id={} has unknown apply_point={}",
            wire.binding_id, wire.apply_point
        )
    })?;
    if apply_point == plan::RuntimeFilterApplyPoint::Unspecified {
        return Err(format!(
            "native runtime-filter binding_id={} has unspecified apply_point",
            wire.binding_id
        ));
    }
    let decoded_apply_point = match apply_point {
        plan::RuntimeFilterApplyPoint::NodeInput => DecodedApplyPoint::NodeInput,
        plan::RuntimeFilterApplyPoint::NodeOutput => DecodedApplyPoint::NodeOutput,
        plan::RuntimeFilterApplyPoint::Unspecified => unreachable!("rejected above"),
    };
    let expression = wire.expression.clone().ok_or_else(|| {
        format!(
            "native runtime-filter binding_id={} missing expression",
            wire.binding_id
        )
    })?;
    super::expr::validate_proto_expr_shape(&expression).map_err(|error| {
        format!(
            "native runtime-filter binding_id={} expression is invalid: {error}",
            wire.binding_id
        )
    })?;
    let expression_type = super::decode_type(expression.r#type.as_ref().expect("checked"))
        .map_err(|error| {
            format!(
                "native runtime-filter binding_id={} expression type: {error}",
                wire.binding_id
            )
        })?;
    let contract = decode_contract(wire.binding_id, &expression_type, wire.contract.as_ref())?;
    let reduction = decode_reduction(wire.binding_id, &contract, wire.reduction.as_ref())?;
    let role = decode_role(wire.binding_id, wire.role.as_ref())?;
    match (&role, apply_point) {
        (DecodedBindingRole::Consumer { .. }, plan::RuntimeFilterApplyPoint::NodeInput)
        | (DecodedBindingRole::Producer { .. }, plan::RuntimeFilterApplyPoint::NodeOutput) => {}
        (DecodedBindingRole::Consumer { .. }, _) => {
            return Err(format!(
                "native runtime-filter consumer binding_id={} must use NodeInput",
                wire.binding_id
            ));
        }
        (DecodedBindingRole::Producer { .. }, _) => {
            return Err(format!(
                "native runtime-filter producer binding_id={} must use NodeOutput",
                wire.binding_id
            ));
        }
    }
    validate_role_contract(wire.binding_id, &contract, &reduction, &role)?;
    Ok(DecodedRuntimeFilterBinding {
        binding_id: wire.binding_id,
        channel_id: wire.channel_id,
        node_id: wire.node_id,
        apply_point: decoded_apply_point,
        expression,
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

fn decode_contract(
    binding_id: u32,
    expression_type: &arrow::datatypes::DataType,
    wire: Option<&plan::RuntimeFilterContract>,
) -> Result<DecodedRuntimeFilterContract, String> {
    let kind = wire
        .and_then(|wire| wire.kind.as_ref())
        .ok_or_else(|| format!("native runtime-filter binding_id={binding_id} missing contract"))?;
    match kind {
        plan::runtime_filter_contract::Kind::Membership(membership) => {
            if membership.canonical_schema.is_empty() {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} membership schema is empty"
                ));
            }
            let view = ArtifactMembershipSchema::view(&membership.canonical_schema)
                .map_err(|error| format!("native runtime-filter binding_id={binding_id} membership schema is noncanonical: {error:?}"))?;
            let digest = digest32(
                binding_id,
                "membership schema_digest",
                &membership.schema_digest,
            )?;
            if view.digest().bytes() != digest {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} membership schema digest mismatch"
                ));
            }
            let expected = ArtifactMembershipSchema::new(expression_type, view.null_semantics())
                .map_err(|error| format!("native runtime-filter binding_id={binding_id} expression type cannot form membership schema: {error:?}"))?;
            if expected.canonical_bytes() != membership.canonical_schema {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} membership schema does not match expression type"
                ));
            }
            Ok(DecodedRuntimeFilterContract::Membership {
                canonical_schema: Arc::from(membership.canonical_schema.as_slice()),
                schema_digest: digest,
            })
        }
        plan::runtime_filter_contract::Kind::Ordered(ordered) => {
            if ordered.keys.len() != 1 {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} ordered contract must contain exactly one key, got {}",
                    ordered.keys.len()
                ));
            }
            let keys = ordered.keys.iter().map(|key| {
                let data_type = super::decode_type(key.r#type.as_ref().ok_or_else(|| {
                    format!("native runtime-filter binding_id={binding_id} ordered key type missing")
                })?).map_err(|error| format!("native runtime-filter binding_id={binding_id} ordered key type: {error}"))?;
                let direction = match plan::RuntimeFilterSortDirection::try_from(key.direction)
                    .map_err(|_| format!("native runtime-filter binding_id={binding_id} unknown sort direction={}", key.direction))? {
                    plan::RuntimeFilterSortDirection::Ascending => SortDirection::Ascending,
                    plan::RuntimeFilterSortDirection::Descending => SortDirection::Descending,
                    plan::RuntimeFilterSortDirection::Unspecified => return Err(format!("native runtime-filter binding_id={binding_id} unspecified sort direction")),
                };
                let null_order = match plan::RuntimeFilterNullOrder::try_from(key.null_order)
                    .map_err(|_| format!("native runtime-filter binding_id={binding_id} unknown null order={}", key.null_order))? {
                    plan::RuntimeFilterNullOrder::First => NullOrder::First,
                    plan::RuntimeFilterNullOrder::Last => NullOrder::Last,
                    plan::RuntimeFilterNullOrder::Unspecified => return Err(format!("native runtime-filter binding_id={binding_id} unspecified null order")),
                };
                Ok(RuntimeOrderKey::from_codec(data_type, direction, null_order))
            }).collect::<Result<Vec<_>, String>>()?;
            if keys[0].data_type() != expression_type {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} ordered key type {:?} does not match expression type {:?}",
                    keys[0].data_type(),
                    expression_type
                ));
            }
            let comparator = digest32(binding_id, "comparator_digest", &ordered.comparator_digest)?;
            let order_digest = digest32(
                binding_id,
                "order_contract_digest",
                &ordered.order_contract_digest,
            )?;
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
            let canonical = RuntimeOrderContract::try_from_plan(&plan_contract)
                .map_err(|error| format!("native runtime-filter binding_id={binding_id} ordered contract is noncanonical: {error:?}"))?;
            if canonical.digest().bytes() != order_digest {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} order contract digest mismatch"
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
) -> Result<DecodedRuntimeFilterReduction, String> {
    let kind = wire.and_then(|wire| wire.kind.as_ref()).ok_or_else(|| {
        format!("native runtime-filter binding_id={binding_id} missing reduction contract")
    })?;
    match kind {
        plan::runtime_filter_reduction_contract::Kind::SetUnion(true) => {
            Ok(DecodedRuntimeFilterReduction::SetUnion)
        }
        plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(true) => {
            Ok(DecodedRuntimeFilterReduction::TightenOrderedBound)
        }
        plan::runtime_filter_reduction_contract::Kind::SetUnion(false)
        | plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(false) => Err(
            format!("native runtime-filter binding_id={binding_id} reduction marker must be true"),
        ),
        plan::runtime_filter_reduction_contract::Kind::MergeTopkSummary(topk) => {
            let k = NonZeroU32::new(topk.k).ok_or_else(|| {
                format!("native runtime-filter binding_id={binding_id} TopK K must be nonzero")
            })?;
            let digest = digest32(binding_id, "TopK contract_digest", &topk.contract_digest)?;
            let DecodedRuntimeFilterContract::Ordered {
                keys,
                comparator_digest,
                ..
            } = contract
            else {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} TopK reduction requires ordered contract"
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
            let expected = RuntimeTopKSummaryContract::try_from_plan(&order, TopKSummaryRequirement::try_new(k.get()).expect("nonzero"))
                .map_err(|error| format!("native runtime-filter binding_id={binding_id} TopK contract is noncanonical: {error:?}"))?;
            if expected.digest().bytes() != digest {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} TopK contract digest mismatch"
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
) -> Result<DecodedBindingRole, String> {
    match role
        .ok_or_else(|| format!("native runtime-filter binding_id={binding_id} missing role"))?
    {
        plan::runtime_filter_binding::Role::Producer(producer) => {
            let contribution_kinds = producer.contribution_kinds.iter().copied().map(|raw| {
                match plan::RuntimeFilterContributionKind::try_from(raw).map_err(|_| format!("native runtime-filter binding_id={binding_id} unknown contribution kind={raw}"))? {
                    plan::RuntimeFilterContributionKind::ValueDomainDelta => Ok(ContributionKind::ValueDomainDelta),
                    plan::RuntimeFilterContributionKind::FinalDomainShard => Ok(ContributionKind::FinalDomainShard),
                    plan::RuntimeFilterContributionKind::OrderedBoundUpdate => Ok(ContributionKind::OrderedBoundUpdate),
                    plan::RuntimeFilterContributionKind::TopkSummary => Ok(ContributionKind::TopKSummary),
                    plan::RuntimeFilterContributionKind::ProducerClosed => Ok(ContributionKind::ProducerClosed),
                    plan::RuntimeFilterContributionKind::Unspecified => Err(format!("native runtime-filter binding_id={binding_id} unspecified contribution kind")),
                }
            }).collect::<Result<BTreeSet<_>, String>>()?;
            if contribution_kinds.len() != producer.contribution_kinds.len()
                || contribution_kinds.is_empty()
            {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} producer contribution kinds must be unique and nonempty"
                ));
            }
            let completion_requirement = match plan::RuntimeFilterCompletionRequirement::try_from(producer.completion_requirement)
                .map_err(|_| format!("native runtime-filter binding_id={binding_id} unknown completion requirement={}", producer.completion_requirement))? {
                plan::RuntimeFilterCompletionRequirement::ProducerClosed => CompletionRequirement::ProducerClosed,
                plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen => CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
                plan::RuntimeFilterCompletionRequirement::Unspecified => return Err(format!("native runtime-filter binding_id={binding_id} unspecified completion requirement")),
            };
            let join_key_ordinal = usize::try_from(producer.join_key_ordinal.ok_or_else(|| {
                format!(
                    "native runtime-filter producer binding_id={binding_id} missing join_key_ordinal"
                )
            })?)
            .map_err(|_| {
                format!(
                    "native runtime-filter producer binding_id={binding_id} join_key_ordinal does not fit usize"
                )
            })?;
            Ok(DecodedBindingRole::Producer {
                contribution_kinds,
                completion_requirement,
                join_key_ordinal,
            })
        }
        plan::runtime_filter_binding::Role::Consumer(consumer) => {
            let capabilities = consumer
                .capabilities
                .iter()
                .copied()
                .map(|raw| {
                    match plan::RuntimeFilterArtifactCapability::try_from(raw).map_err(|_| {
                        format!(
                            "native runtime-filter binding_id={binding_id} unknown capability={raw}"
                        )
                    })? {
                        plan::RuntimeFilterArtifactCapability::Membership => {
                            Ok(ArtifactCapability::Membership)
                        }
                        plan::RuntimeFilterArtifactCapability::OrderedRange => {
                            Ok(ArtifactCapability::OrderedRange)
                        }
                        plan::RuntimeFilterArtifactCapability::EmptyDomain => {
                            Ok(ArtifactCapability::EmptyDomain)
                        }
                        plan::RuntimeFilterArtifactCapability::Unspecified => Err(format!(
                            "native runtime-filter binding_id={binding_id} unspecified capability"
                        )),
                    }
                })
                .collect::<Result<BTreeSet<_>, String>>()?;
            if capabilities.len() != consumer.capabilities.len() || capabilities.is_empty() {
                return Err(format!(
                    "native runtime-filter binding_id={binding_id} consumer capabilities must be unique and nonempty"
                ));
            }
            let activation = match consumer.activation.as_ref().and_then(|activation| activation.kind.as_ref())
                .ok_or_else(|| format!("native runtime-filter binding_id={binding_id} missing consumer activation"))? {
                plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true) => ConsumerActivation::BlockingSnapshot,
                plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(false) => return Err(format!("native runtime-filter binding_id={binding_id} blocking activation marker must be true")),
                plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(raw) => ConsumerActivation::NonBlockingLive { late_apply: match plan::RuntimeFilterLateApplyGranularity::try_from(*raw).map_err(|_| format!("native runtime-filter binding_id={binding_id} unknown late-apply granularity={raw}"))? {
                    plan::RuntimeFilterLateApplyGranularity::Row => LateApplyGranularity::Row,
                    plan::RuntimeFilterLateApplyGranularity::Batch => LateApplyGranularity::Batch,
                    plan::RuntimeFilterLateApplyGranularity::RowGroup => LateApplyGranularity::RowGroup,
                    plan::RuntimeFilterLateApplyGranularity::Split => LateApplyGranularity::Split,
                    plan::RuntimeFilterLateApplyGranularity::File => LateApplyGranularity::File,
                    plan::RuntimeFilterLateApplyGranularity::Unspecified => return Err(format!("native runtime-filter binding_id={binding_id} unspecified late-apply granularity")),
                }},
            };
            let target = match consumer.target.as_ref().ok_or_else(|| {
                format!(
                    "native runtime-filter consumer binding_id={binding_id} missing target"
                )
            })? {
                plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(raw) => {
                    DecodedConsumerBindingTarget::DirectInput {
                        input_ordinal: usize::try_from(*raw).map_err(|_| {
                            format!(
                                "native runtime-filter consumer binding_id={binding_id} input ordinal does not fit usize"
                            )
                        })?,
                    }
                }
                plan::runtime_filter_consumer_role::Target::SourceBoundary(true) => {
                    DecodedConsumerBindingTarget::SourceBoundary
                }
                plan::runtime_filter_consumer_role::Target::SourceBoundary(false) => {
                    return Err(format!(
                        "native runtime-filter consumer binding_id={binding_id} source boundary marker must be true"
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

    use super::*;
    use crate::proto::expr;
    use crate::runtime_filter::model::contract::{
        NullOrder, NullSemantics, OrderContract, OrderKeyContract, SortDirection,
        TopKSummaryRequirement,
    };
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, RuntimeOrderContract, comparator_digest_for_test,
    };
    use crate::runtime_filter::port::topk_summary::RuntimeTopKSummaryContract;
    use crate::types::native_proto::encode_type;

    fn expression(column_id: u32) -> expr::Expr {
        expr::Expr {
            r#type: Some(encode_type(&DataType::Int64).expect("type")),
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
            r#type: Some(encode_type(&DataType::Int64).expect("type")),
            nullable: false,
            kind: Some(expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(operand)),
                target: Some(encode_type(&DataType::Int64).expect("target type")),
            }))),
        }
    }

    fn binary_expression(op: i32) -> expr::Expr {
        expr::Expr {
            r#type: Some(encode_type(&DataType::Int64).expect("type")),
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![missing_target])),)
                .is_err()
        );
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
        let comparator = comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION);
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
                                r#type: Some(encode_type(&key.data_type).expect("type")),
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
        RuntimeFilterBindingLookupLedger::decode(7, Some(&valid)).expect("valid table");
        assert!(RuntimeFilterBindingLookupLedger::decode(7, None).is_err());
        assert!(
            RuntimeFilterBindingLookupLedger::decode(
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![unknown]))).is_err()
        );
        assert!(RuntimeFilterBindingLookupLedger::decode(7, Some(&table(8, Vec::new()))).is_err());
    }

    #[test]
    fn binding_table_decode_rejects_nested_expression_missing_type_or_kind() {
        let mut missing_type = membership_binding(1, 11);
        let mut child = expression(1);
        child.r#type = None;
        missing_type.expression = Some(cast_expression(child));
        assert!(
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![missing_type])))
                .is_err(),
            "nested expression without type must be rejected during binding decode"
        );

        let mut missing_kind = membership_binding(1, 11);
        let mut child = expression(1);
        child.kind = None;
        missing_kind.expression = Some(cast_expression(child));
        assert!(
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![missing_kind])))
                .is_err(),
            "nested expression without kind must be rejected during binding decode"
        );
    }

    #[test]
    fn binding_table_decode_rejects_nested_expression_illegal_enum() {
        let mut binding = membership_binding(1, 11);
        binding.expression = Some(cast_expression(binary_expression(99_999)));
        assert!(
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![binding]))).is_err(),
            "nested illegal expression enum must be rejected during binding decode"
        );
    }

    #[test]
    fn binding_table_decode_rejects_window_frame_without_end() {
        let mut binding = membership_binding(1, 11);
        binding.expression = Some(expr::Expr {
            r#type: Some(encode_type(&DataType::Int64).expect("type")),
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![binding]))).is_err(),
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![binding]))).is_err(),
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![binding]))).is_err(),
            "ordered key type must match binding expression type"
        );
    }

    #[test]
    fn binding_table_decode_recomputes_membership_order_and_topk_digests() {
        let mut membership = membership_binding(1, 11);
        RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![membership.clone()])))
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![membership]))).is_err()
        );

        let ordered = ordered_topk_binding(2, 11);
        let ledger =
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![ordered.clone()])))
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![wrong_comparator])))
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![wrong_order])))
                .is_err()
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![wrong_topk]))).is_err()
        );
    }

    #[test]
    fn node_lookup_rejects_unknown_duplicate_wrong_node_role_and_apply_point() {
        let valid = table(7, vec![membership_binding(1, 11)]);
        let mut ledger = RuntimeFilterBindingLookupLedger::decode(7, Some(&valid)).expect("decode");
        assert!(ledger.lookup_for_node(999, 11, 7).is_err());
        assert!(ledger.lookup_for_node(1, 12, 7).is_err());
        ledger.lookup_for_node(1, 11, 7).expect("lookup");
        ledger.commit_consumed(1).expect("commit");
        assert!(ledger.lookup_for_node(1, 11, 7).is_err());

        let mut consumer_at_output = membership_binding(2, 11);
        consumer_at_output.apply_point = i32::from(plan::RuntimeFilterApplyPoint::NodeOutput);
        assert!(
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![consumer_at_output])),)
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
                join_key_ordinal: Some(0),
            },
        ));
        assert!(
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![producer_at_input])),)
                .is_err()
        );
    }

    #[test]
    fn node_lookup_rejects_matching_node_id_from_different_wire_fragment() {
        let valid = table(7, vec![membership_binding(1, 11)]);
        let ledger = RuntimeFilterBindingLookupLedger::decode(7, Some(&valid)).expect("decode");
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
            RuntimeFilterBindingLookupLedger::decode(
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
            RuntimeFilterBindingLookupLedger::decode(
                7,
                Some(&table(7, vec![extra_consumer_capability])),
            )
            .is_err()
        );

        let producer_role = |kinds, completion| {
            plan::runtime_filter_binding::Role::Producer(plan::RuntimeFilterProducerRole {
                contribution_kinds: kinds,
                completion_requirement: i32::from(completion),
                join_key_ordinal: Some(0),
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
        assert!(RuntimeFilterBindingLookupLedger::decode(
            7,
            Some(&table(7, vec![extra_producer_kind])),
        )
        .is_err());

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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![wrong_completion])),)
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
        RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![final_domain])))
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
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![blocking_ordered])),)
                .is_err()
        );
    }

    #[test]
    fn fragment_finish_rejects_unconsumed_binding() {
        let valid = table(7, vec![membership_binding(1, 11)]);
        let ledger = RuntimeFilterBindingLookupLedger::decode(7, Some(&valid)).expect("decode");
        assert!(ledger.finish().is_err());
        RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, Vec::new())))
            .expect("empty")
            .finish()
            .expect("all consumed");
    }

    #[test]
    fn finish_returns_sorted_dormancy_facts_only_after_complete_consumption() {
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
                join_key_ordinal: Some(0),
            },
        ));
        let mut ledger =
            RuntimeFilterBindingLookupLedger::decode(7, Some(&table(7, vec![consumer, producer])))
                .expect("decode");

        ledger.lookup_for_node(2, 12, 7).expect("producer lookup");
        ledger.commit_consumed(2).expect("producer consumed");
        ledger.lookup_for_node(1, 11, 7).expect("consumer lookup");
        ledger.commit_consumed(1).expect("consumer consumed");

        let facts = ledger.finish().expect("all bindings consumed");
        assert_eq!(
            facts
                .iter()
                .map(|fact| (
                    fact.binding_id,
                    fact.channel_id,
                    fact.node_id,
                    fact.apply_point,
                    fact.role
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    1,
                    9,
                    11,
                    DecodedApplyPoint::NodeInput,
                    NativeRuntimeFilterDormancyRole::Consumer
                ),
                (
                    2,
                    9,
                    12,
                    DecodedApplyPoint::NodeOutput,
                    NativeRuntimeFilterDormancyRole::Producer
                ),
            ]
        );
    }
}
