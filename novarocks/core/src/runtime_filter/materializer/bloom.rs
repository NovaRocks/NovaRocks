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

use sha2::{Digest, Sha256};

use crate::runtime_filter::port::artifact::{
    ArtifactMembershipSchema, ArtifactSchemaDigest, HashContractDigest,
};
use crate::runtime_filter::port::install::MaterializationPolicy;
use crate::runtime_filter::port::value_domain::{ContributionSizeError, MembershipValues};

const CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.bloom-contract";
const SCALAR_HASH_DOMAIN: &[u8] = b"novarocks.runtime-filter.bloom-scalar";
const SCALAR_FRAMING_VERSION: u16 = 1;
const BIG_ENDIAN_EXTRACTION: u8 = 1;
const LSB0_BIT_ORDER: u8 = 1;
pub const BLOOM_METADATA_BYTES: usize = 2 + 2 + 8 + 8 + 4 + 8 + 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BloomHashContract {
    algorithm_version: u16,
    scalar_framing_version: u16,
    schema_digest: ArtifactSchemaDigest,
    seed: u64,
    bits_per_key: u64,
    hash_count: u32,
    digest: HashContractDigest,
}

impl BloomHashContract {
    pub fn new(
        schema: &ArtifactMembershipSchema,
        policy: MaterializationPolicy,
    ) -> Result<Self, BloomError> {
        Self::from_fields(
            schema.digest(),
            policy.bloom_algorithm_version(),
            SCALAR_FRAMING_VERSION,
            policy.bloom_seed(),
            policy.bloom_bits_per_key(),
            policy.bloom_hash_count(),
        )
    }

    pub fn from_fields(
        schema_digest: ArtifactSchemaDigest,
        algorithm_version: u16,
        scalar_framing_version: u16,
        seed: u64,
        bits_per_key: u64,
        hash_count: u32,
    ) -> Result<Self, BloomError> {
        if algorithm_version != 1
            || scalar_framing_version != SCALAR_FRAMING_VERSION
            || bits_per_key == 0
            || hash_count == 0
        {
            return Err(BloomError::InvalidContract);
        }
        let mut contract = Self {
            algorithm_version,
            scalar_framing_version,
            schema_digest,
            seed,
            bits_per_key,
            hash_count,
            digest: HashContractDigest::new([0; 32]),
        };
        let mut canonical = Sha256::new();
        contract.update_canonical(&mut canonical);
        contract.digest = HashContractDigest::new(canonical.finalize().into());
        Ok(contract)
    }

    pub const fn algorithm_version(&self) -> u16 {
        self.algorithm_version
    }
    pub const fn scalar_framing_version(&self) -> u16 {
        self.scalar_framing_version
    }
    pub const fn schema_digest(&self) -> ArtifactSchemaDigest {
        self.schema_digest
    }
    pub const fn seed(&self) -> u64 {
        self.seed
    }
    pub const fn bits_per_key(&self) -> u64 {
        self.bits_per_key
    }
    pub const fn hash_count(&self) -> u32 {
        self.hash_count
    }
    pub const fn digest(&self) -> HashContractDigest {
        self.digest
    }

    fn update_canonical(&self, hash: &mut Sha256) {
        hash.update(CONTRACT_DOMAIN);
        hash.update(self.algorithm_version.to_be_bytes());
        hash.update(self.scalar_framing_version.to_be_bytes());
        hash.update(self.schema_digest.bytes());
        hash.update(self.seed.to_be_bytes());
        hash.update([BIG_ENDIAN_EXTRACTION]);
        hash.update([LSB0_BIT_ORDER]);
        hash.update(self.bits_per_key.to_be_bytes());
        hash.update(self.hash_count.to_be_bytes());
    }

    pub fn bit_count(&self, cardinality: usize) -> Result<u64, BloomError> {
        if cardinality == 0 {
            return Err(BloomError::EmptyDomain);
        }
        let cardinality = u64::try_from(cardinality).map_err(|_| BloomError::SizeOverflow)?;
        let raw = cardinality
            .checked_mul(self.bits_per_key)
            .ok_or(BloomError::SizeOverflow)?;
        raw.checked_add(63)
            .map(|value| value / 64 * 64)
            .filter(|value| *value != 0)
            .ok_or(BloomError::SizeOverflow)
    }

    pub fn bit_count_u64(&self, cardinality: u64) -> Result<u64, BloomError> {
        if cardinality == 0 {
            return Err(BloomError::EmptyDomain);
        }
        let raw = cardinality
            .checked_mul(self.bits_per_key)
            .ok_or(BloomError::SizeOverflow)?;
        raw.checked_add(63)
            .map(|value| value / 64 * 64)
            .filter(|value| *value != 0)
            .ok_or(BloomError::SizeOverflow)
    }

    pub fn payload_len(&self, cardinality: usize) -> Result<usize, BloomError> {
        let bytes = self.bit_count(cardinality)? / 8;
        usize::try_from(bytes)
            .map_err(|_| BloomError::SizeOverflow)?
            .checked_add(BLOOM_METADATA_BYTES)
            .ok_or(BloomError::SizeOverflow)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BloomError {
    InvalidContract,
    EmptyDomain,
    SizeOverflow,
}

impl From<ContributionSizeError> for BloomError {
    fn from(_error: ContributionSizeError) -> Self {
        Self::SizeOverflow
    }
}

pub fn build_bits(
    values: &MembershipValues,
    contract: &BloomHashContract,
    frame: &mut Vec<u8>,
) -> Result<(u64, Vec<u8>), BloomError> {
    let bit_count = contract.bit_count(values.len())?;
    let byte_count = usize::try_from(bit_count / 8).map_err(|_| BloomError::SizeOverflow)?;
    let mut bits = vec![0u8; byte_count];
    values.visit_canonical_scalar_frames(frame, |scalar| {
        for bit in probe_indices(contract, scalar, bit_count) {
            let byte = usize::try_from(bit / 8).expect("bit index fits allocated buffer");
            bits[byte] |= 1 << (bit % 8);
        }
    })?;
    Ok((bit_count, bits))
}

pub fn contains_scalar(
    contract: &BloomHashContract,
    bit_count: u64,
    bits: &[u8],
    scalar_frame: &[u8],
) -> bool {
    bit_count != 0
        && probe_indices(contract, scalar_frame, bit_count).all(|bit| {
            let byte = usize::try_from(bit / 8).expect("bit index fits platform");
            bits.get(byte)
                .is_some_and(|value| value & (1 << (bit % 8)) != 0)
        })
}

fn probe_indices<'a>(
    contract: &'a BloomHashContract,
    scalar_frame: &'a [u8],
    bit_count: u64,
) -> impl Iterator<Item = u64> + 'a {
    let mut hash = Sha256::new();
    hash.update(SCALAR_HASH_DOMAIN);
    contract.update_canonical(&mut hash);
    hash.update(scalar_frame);
    let digest: [u8; 32] = hash.finalize().into();
    let h1 = u64::from_be_bytes(digest[0..8].try_into().expect("eight hash bytes"));
    let h2 = u64::from_be_bytes(digest[8..16].try_into().expect("eight hash bytes"));
    (0..u64::from(contract.hash_count()))
        .map(move |index| h1.wrapping_add(index.wrapping_mul(h2)) % bit_count)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::NullSemantics;
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
    use crate::runtime_filter::port::install::MaterializationPolicy;
    use crate::runtime_filter::port::value_domain::MembershipValues;

    use super::{BloomHashContract, build_bits, contains_scalar};

    fn contract() -> BloomHashContract {
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        BloomHashContract::new(
            &schema,
            MaterializationPolicy::new(8, 5, 17, 1, 1 << 20, 1 << 16, 1).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn bloom_never_loses_inserted_canonical_values() {
        for round in 1..=32_i64 {
            let values = MembershipValues::int64(
                (0..round * 3).map(|value| value.wrapping_mul(7919).wrapping_sub(round * 13)),
            );
            let contract = contract();
            let mut frame = Vec::new();
            let (bit_count, bits) = build_bits(&values, &contract, &mut frame).unwrap();
            values
                .visit_canonical_scalar_frames(&mut frame, |scalar| {
                    assert!(contains_scalar(&contract, bit_count, &bits, scalar));
                })
                .unwrap();
            assert_eq!(bit_count % 64, 0);
            assert_eq!(bits.len(), usize::try_from(bit_count / 8).unwrap());
        }
    }

    #[test]
    fn bloom_bytes_ignore_input_order_and_duplicates() {
        let left = MembershipValues::int64([99, -7, 3, 99, 2]);
        let right = MembershipValues::int64([2, 3, -7, 99]);
        let contract = contract();
        let first = build_bits(&left, &contract, &mut Vec::new()).unwrap();
        let second = build_bits(&right, &contract, &mut Vec::new()).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn bloom_hash_contract_and_probe_vector_are_frozen() {
        let values = MembershipValues::int64([1, 7, 42]);
        let contract = contract();
        let (bit_count, bits) = build_bits(&values, &contract, &mut Vec::new()).unwrap();
        assert_eq!(
            contract.digest().bytes(),
            [
                0xc4, 0x3e, 0xe2, 0x64, 0x02, 0x7c, 0x8c, 0xb7, 0xbd, 0x33, 0xbf, 0xac, 0xb7, 0x97,
                0xb6, 0x40, 0xc2, 0x77, 0x5b, 0x91, 0xcc, 0xf6, 0x4b, 0x25, 0xe7, 0xdc, 0xd3, 0xe9,
                0x1b, 0xc7, 0x8f, 0x03,
            ]
        );
        assert_eq!(bit_count, 64);
        assert_eq!(bits, [0x00, 0x20, 0x90, 0x76, 0x21, 0x00, 0x08, 0xa8]);
    }
}
