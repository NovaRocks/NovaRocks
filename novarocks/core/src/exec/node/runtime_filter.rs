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

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;

use crate::exec::expr::ExprId;
use crate::exec::node::ExecNode;
pub use crate::runtime_filter::model::contract::{
    ArtifactCapability, ComparatorDigest, CompletionFenceKind, CompletionRequirement,
    ConsumerActivation, ContributionKind, LateApplyGranularity, NullOrder, NullSemantics,
    OrderContract, OrderKeyContract, ReductionRequirement, RuntimeFilterLogicalDomain,
    SortDirection, TopKSummaryRequirement,
};
pub use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
pub use crate::runtime_filter::port::ordered_bound::{
    RuntimeOrderContract, RuntimeOrderKey, comparator_digest_for_plan,
};
pub use crate::runtime_filter::port::topk_summary::RuntimeTopKSummaryContract;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterExecutionContract {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterExecutionReduction {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary {
        k: NonZeroU32,
        contract_digest: [u8; 32],
    },
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterConsumerBinding {
    pub(crate) binding_id: u32,
    pub(crate) channel_id: u32,
    pub(crate) expr_id: ExprId,
    pub(crate) activation: ConsumerActivation,
    pub(crate) capabilities: BTreeSet<ArtifactCapability>,
    pub(crate) contract: RuntimeFilterExecutionContract,
    pub(crate) reduction: RuntimeFilterExecutionReduction,
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterConsumerNode {
    pub(crate) input: Box<ExecNode>,
    pub(crate) owner_node_id: i32,
    pub(crate) bindings: Vec<RuntimeFilterConsumerBinding>,
}

impl RuntimeFilterConsumerNode {
    pub fn new(
        input: ExecNode,
        owner_node_id: i32,
        bindings: Vec<RuntimeFilterConsumerBinding>,
    ) -> Self {
        Self {
            input: Box::new(input),
            owner_node_id,
            bindings,
        }
    }

    pub fn input(&self) -> &ExecNode {
        &self.input
    }

    pub fn input_mut(&mut self) -> &mut ExecNode {
        &mut self.input
    }
}

impl RuntimeFilterExecutionContract {
    pub fn membership(canonical_schema: Arc<[u8]>, schema_digest: [u8; 32]) -> Self {
        Self::Membership {
            canonical_schema,
            schema_digest,
        }
    }

    pub fn try_ordered(
        keys: Arc<[RuntimeOrderKey]>,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    ) -> Result<Self, String> {
        if keys.is_empty() {
            return Err("runtime-filter ordered contract requires at least one key".to_string());
        }
        Ok(Self::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        })
    }
}

impl RuntimeFilterExecutionReduction {
    pub const fn set_union() -> Self {
        Self::SetUnion
    }

    pub const fn tighten_ordered_bound() -> Self {
        Self::TightenOrderedBound
    }

    pub const fn merge_top_k(k: NonZeroU32, contract_digest: [u8; 32]) -> Self {
        Self::MergeTopKSummary { k, contract_digest }
    }
}

impl RuntimeFilterConsumerBinding {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        binding_id: u32,
        channel_id: u32,
        expr_id: ExprId,
        activation: ConsumerActivation,
        capabilities: BTreeSet<ArtifactCapability>,
        contract: RuntimeFilterExecutionContract,
        reduction: RuntimeFilterExecutionReduction,
    ) -> Result<Self, String> {
        let expected_capabilities = match &contract {
            RuntimeFilterExecutionContract::Membership { .. } => BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            RuntimeFilterExecutionContract::Ordered { .. } => {
                BTreeSet::from([ArtifactCapability::OrderedRange])
            }
        };
        if capabilities != expected_capabilities {
            return Err("runtime-filter consumer capabilities do not match contract".to_string());
        }
        if matches!(contract, RuntimeFilterExecutionContract::Membership { .. })
            != matches!(reduction, RuntimeFilterExecutionReduction::SetUnion)
        {
            return Err("runtime-filter consumer reduction does not match contract".to_string());
        }
        if matches!(contract, RuntimeFilterExecutionContract::Ordered { .. })
            && matches!(activation, ConsumerActivation::BlockingSnapshot)
        {
            return Err("runtime-filter ordered consumer cannot block on feedback".to_string());
        }
        Ok(Self {
            binding_id,
            channel_id,
            expr_id,
            activation,
            capabilities,
            contract,
            reduction,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::runtime_filter::model::contract::{CompletionRequirement, ContributionKind};

    #[test]
    fn execution_specs_carry_only_binding_contract_data() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let consumer = RuntimeFilterConsumerBinding {
            binding_id: 1,
            channel_id: 2,
            expr_id,
            activation: ConsumerActivation::BlockingSnapshot,
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from([1_u8]),
                schema_digest: [2; 32],
            },
            reduction: RuntimeFilterExecutionReduction::SetUnion,
        };

        let producer = crate::exec::node::join::JoinRuntimeFilterProducerBinding {
            binding_id: 3,
            channel_id: 4,
            build_expr_id: expr_id,
            build_key_index: 0,
            contribution_kinds: BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            completion_requirement: CompletionRequirement::ProducerClosed,
            contract: consumer.contract.clone(),
            reduction: consumer.reduction.clone(),
        };
        assert_eq!(producer.contract, consumer.contract);
        assert_eq!(producer.reduction, consumer.reduction);

        let aggregate_producer =
            crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding {
                binding_id: 5,
                channel_id: 6,
                group_key_expr_id: expr_id,
                group_key_ordinal: 0,
                limit: std::num::NonZeroU32::new(7).expect("nonzero limit"),
                contribution_kinds: BTreeSet::from([
                    ContributionKind::OrderedBoundUpdate,
                    ContributionKind::ProducerClosed,
                ]),
                completion_requirement: CompletionRequirement::ProducerClosed,
                contract: RuntimeFilterExecutionContract::Ordered {
                    keys: Arc::from([
                        crate::runtime_filter::port::ordered_bound::RuntimeOrderKey::new(
                            DataType::Int64,
                            crate::runtime_filter::model::contract::SortDirection::Ascending,
                            crate::runtime_filter::model::contract::NullOrder::Last,
                        ),
                    ]),
                    comparator_digest: [3; 32],
                    order_contract_digest: [4; 32],
                },
                reduction: RuntimeFilterExecutionReduction::TightenOrderedBound,
            };
        assert_eq!(aggregate_producer.group_key_expr_id, expr_id);
        assert_eq!(aggregate_producer.limit.get(), 7);
        assert_eq!(
            aggregate_producer.reduction,
            RuntimeFilterExecutionReduction::TightenOrderedBound
        );
    }
}
