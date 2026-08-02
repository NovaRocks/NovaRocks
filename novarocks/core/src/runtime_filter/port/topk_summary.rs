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

use std::cmp::Ordering;
use std::num::NonZeroU32;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::runtime_filter::model::contract::{OrderContract, TopKSummaryRequirement};

use super::ordered_bound::{
    OrderContractError, OrderedTuple, OrderedTupleError, RuntimeOrderContract,
};
use super::value_domain::ContributionSizeError;

const TOPK_CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.top-k-summary-contract";
const TOPK_CONTRACT_VERSION: u16 = 1;
const TOPK_REPLAY_DOMAIN: &[u8] = b"novarocks.runtime-filter.top-k-summary-replay";
const TOPK_REPLAY_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopKSummaryContractDigest([u8; 32]);

impl TopKSummaryContractDigest {
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopKSummaryError {
    InvalidOrderContract(OrderContractError),
    KIndexOverflow,
    TooManyCandidates,
    CandidateContractMismatch(OrderedTupleError),
    NonCanonicalCandidates,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeTopKSummaryContract {
    order: Arc<RuntimeOrderContract>,
    k: NonZeroU32,
    digest: TopKSummaryContractDigest,
}

impl RuntimeTopKSummaryContract {
    pub fn try_from_plan(
        order: &OrderContract,
        requirement: TopKSummaryRequirement,
    ) -> Result<Self, TopKSummaryError> {
        let order = Arc::new(
            RuntimeOrderContract::try_from_plan(order)
                .map_err(TopKSummaryError::InvalidOrderContract)?,
        );
        let k = requirement.k();
        usize::try_from(k.get()).map_err(|_| TopKSummaryError::KIndexOverflow)?;
        let mut canonical = Sha256::new();
        canonical.update(TOPK_CONTRACT_DOMAIN);
        canonical.update(TOPK_CONTRACT_VERSION.to_be_bytes());
        canonical.update(order.digest().bytes());
        canonical.update(k.get().to_be_bytes());
        let digest = TopKSummaryContractDigest(canonical.finalize().into());
        Ok(Self { order, k, digest })
    }

    pub const fn order(&self) -> &Arc<RuntimeOrderContract> {
        &self.order
    }

    pub const fn k(&self) -> NonZeroU32 {
        self.k
    }

    pub const fn digest(&self) -> TopKSummaryContractDigest {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopKSummary {
    contract_digest: TopKSummaryContractDigest,
    candidates: Arc<[OrderedTuple]>,
    replay_digest: [u8; 32],
}

impl TopKSummary {
    pub fn try_new(
        contract: &RuntimeTopKSummaryContract,
        candidates: Vec<OrderedTuple>,
    ) -> Result<Self, TopKSummaryError> {
        let k =
            usize::try_from(contract.k().get()).map_err(|_| TopKSummaryError::KIndexOverflow)?;
        if candidates.len() > k {
            return Err(TopKSummaryError::TooManyCandidates);
        }
        for candidate in &candidates {
            contract
                .order()
                .compare(candidate, candidate)
                .map_err(TopKSummaryError::CandidateContractMismatch)?;
        }
        for pair in candidates.windows(2) {
            if contract
                .order()
                .compare(&pair[0], &pair[1])
                .map_err(TopKSummaryError::CandidateContractMismatch)?
                == Ordering::Greater
            {
                return Err(TopKSummaryError::NonCanonicalCandidates);
            }
        }

        let replay_digest = canonical_replay_digest(contract.digest(), &candidates);
        Ok(Self {
            contract_digest: contract.digest(),
            candidates: candidates.into(),
            replay_digest,
        })
    }

    pub const fn contract_digest(&self) -> TopKSummaryContractDigest {
        self.contract_digest
    }

    pub fn candidates(&self) -> &[OrderedTuple] {
        &self.candidates
    }

    pub fn shared_candidates(&self) -> Arc<[OrderedTuple]> {
        self.candidates.clone()
    }

    pub const fn replay_digest(&self) -> [u8; 32] {
        self.replay_digest
    }

    pub fn canonical_contribution_bytes(&self) -> Option<usize> {
        let mut bytes = TOPK_REPLAY_DOMAIN
            .len()
            .checked_add(size_of::<u16>())?
            .checked_add(32)?
            .checked_add(size_of::<u64>())?;
        for candidate in self.candidates.iter() {
            let mut candidate_bytes = Some(bytes);
            candidate.visit_canonical(|part| {
                candidate_bytes = candidate_bytes.and_then(|bytes| bytes.checked_add(part.len()));
            });
            bytes = candidate_bytes?;
        }
        Some(bytes)
    }

    pub fn canonical_body_len(&self) -> Result<usize, ContributionSizeError> {
        u64::try_from(self.candidates.len())
            .map_err(|_| ContributionSizeError::LengthExceedsCanonicalRange)?;
        self.candidates
            .iter()
            .try_fold(size_of::<u64>(), |bytes, candidate| {
                bytes
                    .checked_add(candidate.canonical_codec_len()?)
                    .ok_or(ContributionSizeError::SizeOverflow)
            })
    }

    pub fn encode_canonical_body_into(
        &self,
        output: &mut Vec<u8>,
    ) -> Result<(), ContributionSizeError> {
        let exact_len = self.canonical_body_len()?;
        let candidate_count = u64::try_from(self.candidates.len())
            .map_err(|_| ContributionSizeError::LengthExceedsCanonicalRange)?;
        let start = output.len();
        output.extend_from_slice(&candidate_count.to_be_bytes());
        for candidate in self.candidates.iter() {
            candidate.encode_canonical_into(output)?;
        }
        debug_assert_eq!(output.len() - start, exact_len);
        Ok(())
    }
}

fn canonical_replay_digest(
    digest: TopKSummaryContractDigest,
    candidates: &[OrderedTuple],
) -> [u8; 32] {
    let mut canonical = Sha256::new();
    canonical.update(TOPK_REPLAY_DOMAIN);
    canonical.update(TOPK_REPLAY_VERSION.to_be_bytes());
    canonical.update(digest.bytes());
    canonical.update((candidates.len() as u64).to_be_bytes());
    for candidate in candidates {
        candidate.visit_canonical(|part| canonical.update(part));
    }
    canonical.finalize().into()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::{
        NullOrder, OrderContract, OrderKeyContract, SortDirection, TopKSummaryRequirement,
    };
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedScalar, OrderedTuple, comparator_digest_for_test,
    };

    use super::{RuntimeTopKSummaryContract, TopKSummary};

    fn plan(data_type: DataType, direction: SortDirection) -> OrderContract {
        let keys = vec![OrderKeyContract {
            data_type,
            direction,
            null_order: NullOrder::Last,
        }];
        OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        }
    }

    fn contract(k: u32) -> RuntimeTopKSummaryContract {
        RuntimeTopKSummaryContract::try_from_plan(
            &plan(DataType::Int64, SortDirection::Ascending),
            TopKSummaryRequirement::try_new(k).unwrap(),
        )
        .unwrap()
    }

    fn tuple(contract: &RuntimeTopKSummaryContract, value: i64) -> OrderedTuple {
        OrderedTuple::try_new(contract.order(), [Some(OrderedScalar::Int64(value))]).unwrap()
    }

    #[test]
    fn summary_accepts_empty_and_rejects_oversized_or_non_canonical_candidates() {
        let contract = contract(2);
        assert!(TopKSummary::try_new(&contract, vec![]).is_ok());
        assert!(
            TopKSummary::try_new(
                &contract,
                vec![
                    tuple(&contract, 1),
                    tuple(&contract, 2),
                    tuple(&contract, 3)
                ],
            )
            .is_err()
        );
        assert!(
            TopKSummary::try_new(&contract, vec![tuple(&contract, 3), tuple(&contract, 1)])
                .is_err()
        );
    }

    #[test]
    fn summary_rejects_tuple_from_incompatible_order_contract() {
        let contract = contract(2);
        let utf8_contract = RuntimeTopKSummaryContract::try_from_plan(
            &plan(DataType::Utf8, SortDirection::Ascending),
            TopKSummaryRequirement::try_new(2).unwrap(),
        )
        .unwrap();
        let wrong = OrderedTuple::try_new(
            utf8_contract.order(),
            [Some(OrderedScalar::Utf8("wrong".into()))],
        )
        .unwrap();

        assert!(TopKSummary::try_new(&contract, vec![wrong]).is_err());
    }

    #[test]
    fn contract_digest_covers_order_and_k() {
        let k2 = contract(2);
        let k3 = contract(3);
        let descending = RuntimeTopKSummaryContract::try_from_plan(
            &plan(DataType::Int64, SortDirection::Descending),
            TopKSummaryRequirement::try_new(2).unwrap(),
        )
        .unwrap();

        assert_ne!(k2.digest(), k3.digest());
        assert_ne!(k2.digest(), descending.digest());
        assert_eq!(k2.k().get(), 2);
    }

    #[test]
    fn summary_preserves_duplicate_multiplicity_and_has_canonical_replay_bytes() {
        let contract = contract(3);
        let first = TopKSummary::try_new(
            &contract,
            vec![
                tuple(&contract, 1),
                tuple(&contract, 1),
                tuple(&contract, 3),
            ],
        )
        .unwrap();
        let same = TopKSummary::try_new(
            &contract,
            vec![
                tuple(&contract, 1),
                tuple(&contract, 1),
                tuple(&contract, 3),
            ],
        )
        .unwrap();
        let deduplicated =
            TopKSummary::try_new(&contract, vec![tuple(&contract, 1), tuple(&contract, 3)])
                .unwrap();

        assert_eq!(first.candidates().len(), 3);
        assert_eq!(first.replay_digest(), same.replay_digest());
        assert_ne!(first.replay_digest(), deduplicated.replay_digest());
        assert_eq!(first.contract_digest(), contract.digest());
        assert_eq!(first.canonical_contribution_bytes(), Some(138));

        let mut body = Vec::new();
        first.encode_canonical_body_into(&mut body).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&3_u64.to_be_bytes());
        for value in [1_i64, 1, 3] {
            expected.extend_from_slice(&1_u64.to_be_bytes());
            expected.push(1);
            expected.extend_from_slice(&value.to_be_bytes());
        }
        assert_eq!(body, expected);
        assert_eq!(first.canonical_body_len(), Ok(body.len()));
    }
}
