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

use std::num::NonZeroU32;

use super::super::expr::encode_expr;
use crate::coordinator::prepare::runtime_filter_binding::{
    PreparedReductionContract, PreparedRuntimeFilterBinding, PreparedRuntimeFilterBindingRole,
    PreparedRuntimeFilterContract, RuntimeFilterBindingTable,
};
use crate::proto::plan;
use crate::runtime_filter::model::contract::{
    ArtifactCapability, ComparatorDigest, CompletionFenceKind, CompletionRequirement,
    ConsumerActivation, ContributionKind, LateApplyGranularity, NullOrder, OrderContract,
    OrderKeyContract, SortDirection, TopKSummaryRequirement,
};
use crate::runtime_filter::model::graph::ApplyPoint;
use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
use crate::runtime_filter::port::ordered_bound::{
    OrderContractDigest, RuntimeOrderContract, RuntimeOrderKey,
};
use crate::runtime_filter::port::topk_summary::{
    RuntimeTopKSummaryContract, TopKSummaryContractDigest,
};
use crate::types::native_proto::encode_type;

pub(super) fn encode_runtime_filter_binding_table(
    enclosing_fragment_id: crate::sql::planner::distributed::FragmentId,
    table: &RuntimeFilterBindingTable,
) -> Result<plan::RuntimeFilterBindingTable, String> {
    if table.fragment_id() != enclosing_fragment_id {
        return Err(format!(
            "native runtime filter binding table fragment mismatch: enclosing_fragment_id={enclosing_fragment_id} table_fragment_id={}",
            table.fragment_id()
        ));
    }
    let mut previous_binding_id = None;
    let mut bindings = Vec::with_capacity(table.bindings().len());
    for binding in table.bindings() {
        let binding_id = binding.binding_id().get();
        if previous_binding_id.is_some_and(|previous| previous >= binding_id) {
            return Err(format!(
                "native runtime filter binding table is not strictly ordered by binding id: previous={previous_binding_id:?} current={binding_id}"
            ));
        }
        previous_binding_id = Some(binding_id);
        bindings.push(encode_runtime_filter_binding(binding)?);
    }
    Ok(plan::RuntimeFilterBindingTable {
        fragment_id: table.fragment_id(),
        bindings,
    })
}

fn encode_runtime_filter_binding(
    binding: &PreparedRuntimeFilterBinding,
) -> Result<plan::RuntimeFilterBinding, String> {
    let contract = encode_runtime_filter_contract(binding)?;
    let reduction = encode_runtime_filter_reduction(binding)?;
    let role = Some(match binding.role() {
        PreparedRuntimeFilterBindingRole::Producer {
            contribution_kinds,
            completion_requirement,
            join_key_ordinal,
        } => plan::runtime_filter_binding::Role::Producer(plan::RuntimeFilterProducerRole {
            contribution_kinds: contribution_kinds
                .iter()
                .copied()
                .map(encode_runtime_filter_contribution_kind)
                .collect(),
            completion_requirement: encode_runtime_filter_completion(*completion_requirement),
            join_key_ordinal: Some(u32::try_from(*join_key_ordinal).map_err(|_| {
                format!(
                    "runtime-filter binding_id={} join key ordinal does not fit u32",
                    binding.binding_id().get()
                )
            })?),
        }),
        PreparedRuntimeFilterBindingRole::Consumer {
            capabilities,
            activation,
            target,
        } => plan::runtime_filter_binding::Role::Consumer(plan::RuntimeFilterConsumerRole {
            capabilities: capabilities
                .iter()
                .copied()
                .map(encode_runtime_filter_capability)
                .collect(),
            activation: Some(encode_runtime_filter_activation(*activation)),
            target: Some(match target {
                crate::runtime_filter::model::graph::ConsumerBindingTarget::DirectInput {
                    input_ordinal,
                } => plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(
                    u32::try_from(*input_ordinal).map_err(|_| {
                        format!(
                            "runtime-filter binding_id={} input ordinal does not fit u32",
                            binding.binding_id().get()
                        )
                    })?,
                ),
                crate::runtime_filter::model::graph::ConsumerBindingTarget::SourceBoundary => {
                    plan::runtime_filter_consumer_role::Target::SourceBoundary(true)
                }
            }),
        }),
    });
    Ok(plan::RuntimeFilterBinding {
        binding_id: binding.binding_id().get(),
        channel_id: binding.channel_id().get(),
        node_id: binding.node_id().get(),
        apply_point: encode_runtime_filter_apply_point(binding.apply_point()),
        expression: Some(encode_expr(binding.expression())?),
        contract: Some(contract),
        reduction: Some(reduction),
        role,
    })
}

fn encode_runtime_filter_contract(
    binding: &PreparedRuntimeFilterBinding,
) -> Result<plan::RuntimeFilterContract, String> {
    use plan::runtime_filter_contract::Kind;

    let kind = match binding.contract() {
        PreparedRuntimeFilterContract::Membership {
            canonical_schema,
            schema_digest,
        } => Kind::Membership(encode_runtime_filter_membership_contract(
            binding.binding_id().get(),
            canonical_schema,
            schema_digest.bytes(),
        )?),
        PreparedRuntimeFilterContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        } => Kind::Ordered(encode_runtime_filter_ordered_contract(
            binding.binding_id().get(),
            keys,
            *comparator_digest,
            *order_contract_digest,
        )?),
    };
    Ok(plan::RuntimeFilterContract { kind: Some(kind) })
}

pub(super) fn encode_runtime_filter_ordered_contract(
    binding_id: u32,
    keys: &[RuntimeOrderKey],
    comparator_digest: ComparatorDigest,
    order_contract_digest: OrderContractDigest,
) -> Result<plan::RuntimeFilterOrderedContract, String> {
    if keys.is_empty() {
        return Err(format!(
            "native runtime filter binding id={binding_id} has no canonical order keys"
        ));
    }
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
        comparator_digest,
    };
    let canonical = RuntimeOrderContract::try_from_plan(&plan_contract).map_err(|error| {
        format!(
            "native runtime filter binding id={binding_id} has a noncanonical ordered contract: {error:?}"
        )
    })?;
    if canonical.digest() != order_contract_digest {
        return Err(format!(
            "native runtime filter binding id={binding_id} order contract digest does not match typed keys"
        ));
    }
    Ok(plan::RuntimeFilterOrderedContract {
        keys: keys
            .iter()
            .map(|key| {
                Ok(plan::RuntimeFilterOrderKey {
                    r#type: Some(encode_type(key.data_type())?),
                    direction: encode_runtime_filter_sort_direction(key.direction()),
                    null_order: encode_runtime_filter_null_order(key.null_order()),
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        comparator_digest: comparator_digest.get().to_vec(),
        order_contract_digest: order_contract_digest.bytes().to_vec(),
    })
}

pub(super) fn encode_runtime_filter_membership_contract(
    binding_id: u32,
    canonical_schema: &[u8],
    schema_digest: [u8; 32],
) -> Result<plan::RuntimeFilterMembershipContract, String> {
    if canonical_schema.is_empty() {
        return Err(format!(
            "native runtime filter binding id={binding_id} has an empty canonical membership schema"
        ));
    }
    let canonical = ArtifactMembershipSchema::view(canonical_schema).map_err(|error| {
        format!(
            "native runtime filter binding id={binding_id} has a noncanonical membership schema: {error:?}"
        )
    })?;
    if canonical.digest().bytes() != schema_digest {
        return Err(format!(
            "native runtime filter binding id={binding_id} membership schema digest does not match canonical bytes"
        ));
    }
    Ok(plan::RuntimeFilterMembershipContract {
        canonical_schema: canonical_schema.to_vec(),
        schema_digest: schema_digest.to_vec(),
    })
}

fn encode_runtime_filter_reduction(
    binding: &PreparedRuntimeFilterBinding,
) -> Result<plan::RuntimeFilterReductionContract, String> {
    use plan::runtime_filter_reduction_contract::Kind;

    let kind = match binding.reduction() {
        PreparedReductionContract::SetUnion => Kind::SetUnion(true),
        PreparedReductionContract::TightenOrderedBound => Kind::TightenOrderedBound(true),
        PreparedReductionContract::MergeTopKSummary { k, contract_digest } => {
            let PreparedRuntimeFilterContract::Ordered {
                keys,
                comparator_digest,
                ..
            } = binding.contract()
            else {
                return Err(format!(
                    "native runtime filter binding id={} has TopK reduction without an ordered contract",
                    binding.binding_id().get()
                ));
            };
            Kind::MergeTopkSummary(encode_runtime_filter_topk_reduction(
                binding.binding_id().get(),
                keys,
                *comparator_digest,
                *k,
                *contract_digest,
            )?)
        }
    };
    Ok(plan::RuntimeFilterReductionContract { kind: Some(kind) })
}

pub(super) fn encode_runtime_filter_topk_reduction(
    binding_id: u32,
    keys: &[RuntimeOrderKey],
    comparator_digest: ComparatorDigest,
    k: NonZeroU32,
    contract_digest: TopKSummaryContractDigest,
) -> Result<plan::RuntimeFilterTopKReduction, String> {
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
        comparator_digest,
    };
    let requirement =
        TopKSummaryRequirement::try_new(k.get()).expect("prepared TopK K is nonzero by type");
    let canonical = RuntimeTopKSummaryContract::try_from_plan(&order, requirement).map_err(
        |error| {
            format!(
                "native runtime filter binding id={binding_id} has a noncanonical TopK contract: {error:?}"
            )
        },
    )?;
    if canonical.digest() != contract_digest {
        return Err(format!(
            "native runtime filter binding id={binding_id} TopK digest does not match typed order keys and K"
        ));
    }
    Ok(plan::RuntimeFilterTopKReduction {
        k: k.get(),
        contract_digest: contract_digest.bytes().to_vec(),
    })
}

pub(super) fn encode_runtime_filter_apply_point(value: ApplyPoint) -> i32 {
    match value {
        ApplyPoint::NodeInput => i32::from(plan::RuntimeFilterApplyPoint::NodeInput),
        ApplyPoint::NodeOutput => i32::from(plan::RuntimeFilterApplyPoint::NodeOutput),
    }
}

pub(super) fn encode_runtime_filter_contribution_kind(value: ContributionKind) -> i32 {
    match value {
        ContributionKind::ValueDomainDelta => {
            i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta)
        }
        ContributionKind::FinalDomainShard => {
            i32::from(plan::RuntimeFilterContributionKind::FinalDomainShard)
        }
        ContributionKind::OrderedBoundUpdate => {
            i32::from(plan::RuntimeFilterContributionKind::OrderedBoundUpdate)
        }
        ContributionKind::TopKSummary => {
            i32::from(plan::RuntimeFilterContributionKind::TopkSummary)
        }
        ContributionKind::ProducerClosed => {
            i32::from(plan::RuntimeFilterContributionKind::ProducerClosed)
        }
    }
}

pub(super) fn encode_runtime_filter_completion(value: CompletionRequirement) -> i32 {
    match value {
        CompletionRequirement::ProducerClosed => {
            i32::from(plan::RuntimeFilterCompletionRequirement::ProducerClosed)
        }
        CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen) => {
            i32::from(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen)
        }
    }
}

pub(super) fn encode_runtime_filter_capability(value: ArtifactCapability) -> i32 {
    match value {
        ArtifactCapability::Membership => {
            i32::from(plan::RuntimeFilterArtifactCapability::Membership)
        }
        ArtifactCapability::OrderedRange => {
            i32::from(plan::RuntimeFilterArtifactCapability::OrderedRange)
        }
        ArtifactCapability::EmptyDomain => {
            i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain)
        }
    }
}

pub(super) fn encode_runtime_filter_activation(
    value: ConsumerActivation,
) -> plan::RuntimeFilterConsumerActivation {
    use plan::runtime_filter_consumer_activation::Kind;

    plan::RuntimeFilterConsumerActivation {
        kind: Some(match value {
            ConsumerActivation::BlockingSnapshot => Kind::BlockingSnapshot(true),
            ConsumerActivation::NonBlockingLive { late_apply } => {
                Kind::NonBlockingLive(encode_runtime_filter_late_apply(late_apply))
            }
        }),
    }
}

fn encode_runtime_filter_late_apply(value: LateApplyGranularity) -> i32 {
    match value {
        LateApplyGranularity::Row => i32::from(plan::RuntimeFilterLateApplyGranularity::Row),
        LateApplyGranularity::Batch => i32::from(plan::RuntimeFilterLateApplyGranularity::Batch),
        LateApplyGranularity::RowGroup => {
            i32::from(plan::RuntimeFilterLateApplyGranularity::RowGroup)
        }
        LateApplyGranularity::Split => i32::from(plan::RuntimeFilterLateApplyGranularity::Split),
        LateApplyGranularity::File => i32::from(plan::RuntimeFilterLateApplyGranularity::File),
    }
}

fn encode_runtime_filter_sort_direction(value: SortDirection) -> i32 {
    match value {
        SortDirection::Ascending => i32::from(plan::RuntimeFilterSortDirection::Ascending),
        SortDirection::Descending => i32::from(plan::RuntimeFilterSortDirection::Descending),
    }
}

fn encode_runtime_filter_null_order(value: NullOrder) -> i32 {
    match value {
        NullOrder::First => i32::from(plan::RuntimeFilterNullOrder::First),
        NullOrder::Last => i32::from(plan::RuntimeFilterNullOrder::Last),
    }
}
