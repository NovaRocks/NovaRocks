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

//! Closed, immutable runtime-filter declarations. The session and its mutable
//! artifacts are bound separately when a task instance is installed.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use arrow_schema::DataType;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterNullSemantics {
    NeverMatches,
    NullSafeEqual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterSortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterNullOrder {
    First,
    Last,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilterOrderKey {
    pub data_type: DataType,
    pub direction: FilterSortDirection,
    pub null_order: FilterNullOrder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StaticFilterContract {
    Membership {
        data_type: DataType,
        null_semantics: FilterNullSemantics,
        digest: [u8; 32],
    },
    Ordered {
        keys: Arc<[FilterOrderKey]>,
        comparator_digest: [u8; 32],
        contract_digest: [u8; 32],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterReduction {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: NonZeroU32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterProducerKind {
    Membership,
    OrderedBound,
    TopKSummary,
    FinalDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterLateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive {
        late_apply: FilterLateApplyGranularity,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilterScanDomainTarget {
    pub field_ordinal: u32,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticFilterProducer {
    binding_id: u32,
    channel_id: u32,
    kind: FilterProducerKind,
    contract: StaticFilterContract,
    reduction: FilterReduction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticFilterConsumer {
    binding_id: u32,
    channel_id: u32,
    activation: FilterConsumerActivation,
    contract: StaticFilterContract,
    reduction: FilterReduction,
    scan_domain: Option<FilterScanDomainTarget>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticFilterError {
    EmptyOrderedKeys,
    KindContractMismatch,
    ReductionContractMismatch,
    BlockingOrderedConsumer,
}

impl fmt::Display for StaticFilterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid static runtime filter: {self:?}")
    }
}

impl std::error::Error for StaticFilterError {}

impl StaticFilterContract {
    fn validate(&self) -> Result<(), StaticFilterError> {
        if matches!(self, Self::Ordered { keys, .. } if keys.is_empty()) {
            return Err(StaticFilterError::EmptyOrderedKeys);
        }
        Ok(())
    }
}

impl StaticFilterProducer {
    pub fn try_new(
        binding_id: u32,
        channel_id: u32,
        kind: FilterProducerKind,
        contract: StaticFilterContract,
        reduction: FilterReduction,
    ) -> Result<Self, StaticFilterError> {
        contract.validate()?;
        let valid = matches!(
            (kind, &contract, reduction),
            (
                FilterProducerKind::Membership | FilterProducerKind::FinalDomain,
                StaticFilterContract::Membership { .. },
                FilterReduction::SetUnion,
            ) | (
                FilterProducerKind::OrderedBound,
                StaticFilterContract::Ordered { .. },
                FilterReduction::TightenOrderedBound,
            ) | (
                FilterProducerKind::TopKSummary,
                StaticFilterContract::Ordered { .. },
                FilterReduction::MergeTopKSummary { .. },
            )
        );
        if !valid {
            return Err(StaticFilterError::KindContractMismatch);
        }
        Ok(Self {
            binding_id,
            channel_id,
            kind,
            contract,
            reduction,
        })
    }

    pub const fn binding_id(&self) -> u32 {
        self.binding_id
    }

    pub const fn channel_id(&self) -> u32 {
        self.channel_id
    }

    pub const fn kind(&self) -> FilterProducerKind {
        self.kind
    }

    pub const fn contract(&self) -> &StaticFilterContract {
        &self.contract
    }

    pub const fn reduction(&self) -> FilterReduction {
        self.reduction
    }
}

impl StaticFilterConsumer {
    pub fn try_new(
        binding_id: u32,
        channel_id: u32,
        activation: FilterConsumerActivation,
        contract: StaticFilterContract,
        reduction: FilterReduction,
        scan_domain: Option<FilterScanDomainTarget>,
    ) -> Result<Self, StaticFilterError> {
        contract.validate()?;
        if matches!(activation, FilterConsumerActivation::BlockingSnapshot)
            && matches!(contract, StaticFilterContract::Ordered { .. })
        {
            return Err(StaticFilterError::BlockingOrderedConsumer);
        }
        let valid = matches!(
            (&contract, reduction),
            (
                StaticFilterContract::Membership { .. },
                FilterReduction::SetUnion,
            ) | (
                StaticFilterContract::Ordered { .. },
                FilterReduction::TightenOrderedBound | FilterReduction::MergeTopKSummary { .. },
            )
        );
        if !valid {
            return Err(StaticFilterError::ReductionContractMismatch);
        }
        Ok(Self {
            binding_id,
            channel_id,
            activation,
            contract,
            reduction,
            scan_domain,
        })
    }

    pub const fn binding_id(&self) -> u32 {
        self.binding_id
    }

    pub const fn channel_id(&self) -> u32 {
        self.channel_id
    }

    pub const fn activation(&self) -> FilterConsumerActivation {
        self.activation
    }

    pub const fn contract(&self) -> &StaticFilterContract {
        &self.contract
    }

    pub const fn reduction(&self) -> FilterReduction {
        self.reduction
    }

    pub const fn scan_domain(&self) -> Option<&FilterScanDomainTarget> {
        self.scan_domain.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_role_mismatch_before_binding_a_session() {
        let contract = StaticFilterContract::Membership {
            data_type: DataType::Int64,
            null_semantics: FilterNullSemantics::NeverMatches,
            digest: [3; 32],
        };
        assert!(matches!(
            StaticFilterProducer::try_new(
                1,
                2,
                FilterProducerKind::TopKSummary,
                contract,
                FilterReduction::MergeTopKSummary {
                    k: NonZeroU32::new(5).unwrap(),
                },
            ),
            Err(StaticFilterError::KindContractMismatch)
        ));
    }
}
