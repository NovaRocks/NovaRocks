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

use arrow::datatypes::DataType;

macro_rules! model_id {
    ($name:ident, $raw:ty) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($raw);

        impl $name {
            pub const fn new(raw: $raw) -> Self {
                Self(raw)
            }

            pub const fn get(self) -> $raw {
                self.0
            }
        }
    };
}

model_id!(ChannelId, u32);
model_id!(BindingId, u32);
model_id!(CoverageWitnessId, u32);
model_id!(PlanFragmentId, u32);
model_id!(PlanNodeId, i32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ComparatorDigest([u8; 32]);

impl ComparatorDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn get(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeFilterLogicalDomain {
    Membership {
        value_type: DataType,
        null_semantics: NullSemantics,
    },
    OrderedBound(OrderContract),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NullSemantics {
    NeverMatches,
    NullSafeEqual,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NullOrder {
    First,
    Last,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderKeyContract {
    pub data_type: DataType,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderContract {
    pub keys: Vec<OrderKeyContract>,
    pub inclusive: bool,
    pub comparator_digest: ComparatorDigest,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ContributionKind {
    ValueDomainDelta,
    FinalDomainShard,
    OrderedBoundUpdate,
    TopKSummary,
    ProducerClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterLifecycle {
    CompleteOnce,
    MonotonicUpdates,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TopKSummaryRequirement(NonZeroU32);

impl TopKSummaryRequirement {
    pub fn try_new(k: u32) -> Option<Self> {
        NonZeroU32::new(k).map(Self)
    }

    pub const fn k(self) -> NonZeroU32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReductionRequirement {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary(TopKSummaryRequirement),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CompletionFenceKind {
    CommittedDomainFrozen,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CompletionRequirement {
    ProducerClosed,
    FencedFinalDomain(CompletionFenceKind),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ArtifactCapability {
    Membership,
    OrderedRange,
    EmptyDomain,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum LateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive { late_apply: LateApplyGranularity },
}

impl ConsumerActivation {
    pub fn is_blocking_or_batch_live(self) -> bool {
        matches!(
            self,
            Self::BlockingSnapshot
                | Self::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch
                }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterPolicyRequirement {
    pub max_contribution_bytes: u64,
    pub max_artifact_bytes: u64,
    pub deadline_ms: u64,
    pub max_retries: u32,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::datatypes::DataType;

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct ProducerMatrix {
        domain: RuntimeFilterLogicalDomain,
        lifecycle: RuntimeFilterLifecycle,
        contributions: BTreeSet<ContributionKind>,
        reduction: ReductionRequirement,
        completion: CompletionRequirement,
    }

    #[test]
    fn model_ids_keep_stable_ordering_debug_output_and_raw_values() {
        let ids = BTreeSet::from([ChannelId::new(2), ChannelId::new(1)]);
        assert_eq!(
            ids.into_iter().collect::<Vec<_>>(),
            vec![ChannelId::new(1), ChannelId::new(2)]
        );
        assert_eq!(format!("{:?}", ChannelId::new(7)), "ChannelId(7)");

        assert_eq!(ChannelId::new(1).get(), 1);
        assert_eq!(BindingId::new(2).get(), 2);
        assert_eq!(CoverageWitnessId::new(3).get(), 3);
        assert_eq!(PlanFragmentId::new(4).get(), 4);
        assert_eq!(PlanNodeId::new(-5).get(), -5);
    }

    #[test]
    fn comparator_digest_round_trips_its_stable_bytes() {
        assert_eq!(ComparatorDigest::new([7; 32]).get(), [7; 32]);
    }

    #[test]
    fn blocking_or_batch_live_activation_is_a_closed_set() {
        assert!(ConsumerActivation::BlockingSnapshot.is_blocking_or_batch_live());
        assert!(
            ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            }
            .is_blocking_or_batch_live()
        );

        for late_apply in [
            LateApplyGranularity::Row,
            LateApplyGranularity::RowGroup,
            LateApplyGranularity::Split,
            LateApplyGranularity::File,
        ] {
            assert!(
                !ConsumerActivation::NonBlockingLive { late_apply }.is_blocking_or_batch_live(),
                "{late_apply:?} must remain outside the Join activation contract"
            );
        }
    }

    #[test]
    fn top_k_summary_requirement_rejects_zero_and_keeps_k() {
        assert!(TopKSummaryRequirement::try_new(0).is_none());
        assert_eq!(TopKSummaryRequirement::try_new(7).unwrap().k().get(), 7);
    }

    #[test]
    fn ordered_bound_contract_keeps_full_comparator_semantics() {
        let contract = OrderContract {
            keys: vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }],
            inclusive: true,
            comparator_digest: ComparatorDigest::new([7; 32]),
        };
        assert_eq!(contract.keys.len(), 1);
        assert_eq!(contract.keys[0].data_type, DataType::Int64);
        assert_eq!(contract.keys[0].direction, SortDirection::Ascending);
        assert_eq!(contract.keys[0].null_order, NullOrder::Last);
        assert!(contract.inclusive);
        assert_eq!(contract.comparator_digest.get(), [7; 32]);
    }

    #[test]
    fn join_producer_matrix_preserves_contract_requirements() {
        let matrix = ProducerMatrix {
            domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            contributions: BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            reduction: ReductionRequirement::SetUnion,
            completion: CompletionRequirement::ProducerClosed,
        };

        let RuntimeFilterLogicalDomain::Membership {
            value_type,
            null_semantics,
        } = &matrix.domain
        else {
            panic!("Join requires a membership domain");
        };
        assert_eq!(value_type, &DataType::Int64);
        assert_eq!(*null_semantics, NullSemantics::NeverMatches);
        assert_eq!(matrix.lifecycle, RuntimeFilterLifecycle::CompleteOnce);
        assert_eq!(
            matrix.contributions,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ])
        );
        assert_eq!(matrix.reduction, ReductionRequirement::SetUnion);
        assert_eq!(matrix.completion, CompletionRequirement::ProducerClosed);
    }

    #[test]
    fn topn_producer_matrix_preserves_contract_requirements() {
        for (reduction, contributions) in [
            (
                ReductionRequirement::TightenOrderedBound,
                BTreeSet::from([
                    ContributionKind::OrderedBoundUpdate,
                    ContributionKind::ProducerClosed,
                ]),
            ),
            (
                ReductionRequirement::MergeTopKSummary(TopKSummaryRequirement::try_new(3).unwrap()),
                BTreeSet::from([
                    ContributionKind::TopKSummary,
                    ContributionKind::ProducerClosed,
                ]),
            ),
        ] {
            let matrix = ProducerMatrix {
                domain: RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
                    keys: vec![OrderKeyContract {
                        data_type: DataType::Int64,
                        direction: SortDirection::Descending,
                        null_order: NullOrder::First,
                    }],
                    inclusive: false,
                    comparator_digest: ComparatorDigest::new([9; 32]),
                }),
                lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
                contributions: contributions.clone(),
                reduction,
                completion: CompletionRequirement::ProducerClosed,
            };

            let RuntimeFilterLogicalDomain::OrderedBound(order) = &matrix.domain else {
                panic!("TopN requires an ordered-bound domain");
            };
            assert_eq!(order.keys.len(), 1);
            assert_eq!(order.keys[0].data_type, DataType::Int64);
            assert_eq!(order.keys[0].direction, SortDirection::Descending);
            assert_eq!(order.keys[0].null_order, NullOrder::First);
            assert!(!order.inclusive);
            assert_eq!(order.comparator_digest.get(), [9; 32]);
            assert_eq!(matrix.lifecycle, RuntimeFilterLifecycle::MonotonicUpdates);
            assert_eq!(matrix.contributions, contributions);
            assert_eq!(matrix.reduction, reduction);
            assert_eq!(matrix.completion, CompletionRequirement::ProducerClosed);
        }
    }

    #[test]
    fn aggregate_committed_domain_matrix_preserves_contract_requirements() {
        let matrix = ProducerMatrix {
            domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NullSafeEqual,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            contributions: BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ]),
            reduction: ReductionRequirement::SetUnion,
            completion: CompletionRequirement::FencedFinalDomain(
                CompletionFenceKind::CommittedDomainFrozen,
            ),
        };

        let RuntimeFilterLogicalDomain::Membership {
            value_type,
            null_semantics,
        } = &matrix.domain
        else {
            panic!("Aggregate committed-domain requires a membership domain");
        };
        assert_eq!(value_type, &DataType::Int64);
        assert_eq!(*null_semantics, NullSemantics::NullSafeEqual);
        assert_eq!(matrix.lifecycle, RuntimeFilterLifecycle::CompleteOnce);
        assert_eq!(
            matrix.contributions,
            BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ])
        );
        assert_eq!(matrix.reduction, ReductionRequirement::SetUnion);
        assert_eq!(
            matrix.completion,
            CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen)
        );
    }

    #[test]
    fn consumer_contract_types_keep_capabilities_activation_and_policy() {
        let capabilities = BTreeSet::from([
            ArtifactCapability::Membership,
            ArtifactCapability::OrderedRange,
            ArtifactCapability::EmptyDomain,
        ]);
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
        let policy = RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 1,
            max_artifact_bytes: 2,
            deadline_ms: 3,
            max_retries: 4,
        };

        assert_eq!(capabilities.len(), 3);
        assert_eq!(activations.len(), 6);
        assert_eq!(policy.max_contribution_bytes, 1);
        assert_eq!(policy.max_artifact_bytes, 2);
        assert_eq!(policy.deadline_ms, 3);
        assert_eq!(policy.max_retries, 4);
    }
}
