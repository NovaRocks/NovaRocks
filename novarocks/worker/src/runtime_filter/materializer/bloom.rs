use novarocks_execution::runtime_filter::contribution::{
    CanonicalF32, CanonicalF64, MembershipValues,
};
use sha2::{Digest, Sha256};

use crate::runtime_filter::artifact::{ArtifactSchemaDigest, HashContractDigest};

const CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.bloom-contract";
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
const SCALAR_HASH_DOMAIN: &[u8] = b"novarocks.runtime-filter.bloom-scalar";
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub const METADATA_BYTES: usize = 40;

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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BloomError {
    InvalidContract,
    EmptyDomain,
    SizeOverflow,
}

impl BloomHashContract {
    pub fn from_fields(
        schema_digest: ArtifactSchemaDigest,
        algorithm_version: u16,
        scalar_framing_version: u16,
        seed: u64,
        bits_per_key: u64,
        hash_count: u32,
    ) -> Result<Self, BloomError> {
        if algorithm_version != 1
            || scalar_framing_version != 1
            || bits_per_key == 0
            || hash_count == 0
        {
            return Err(BloomError::InvalidContract);
        }
        let mut value = Self {
            algorithm_version,
            scalar_framing_version,
            schema_digest,
            seed,
            bits_per_key,
            hash_count,
            digest: HashContractDigest::new([0; 32]),
        };
        value.digest = HashContractDigest::new(value.canonical_digest());
        Ok(value)
    }
    pub const fn digest(self) -> HashContractDigest {
        self.digest
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn algorithm_version(self) -> u16 {
        self.algorithm_version
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn scalar_framing_version(self) -> u16 {
        self.scalar_framing_version
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn seed(self) -> u64 {
        self.seed
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn bits_per_key(self) -> u64 {
        self.bits_per_key
    }
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn hash_count(self) -> u32 {
        self.hash_count
    }
    pub fn bit_count(self, cardinality: usize) -> Result<u64, BloomError> {
        let cardinality = u64::try_from(cardinality).map_err(|_| BloomError::SizeOverflow)?;
        if cardinality == 0 {
            return Err(BloomError::EmptyDomain);
        }
        cardinality
            .checked_mul(self.bits_per_key)
            .and_then(|raw| raw.checked_add(63))
            .map(|value| value / 64 * 64)
            .filter(|value| *value != 0)
            .ok_or(BloomError::SizeOverflow)
    }
    fn canonical_digest(self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(CONTRACT_DOMAIN);
        hash.update(self.algorithm_version.to_be_bytes());
        hash.update(self.scalar_framing_version.to_be_bytes());
        hash.update(self.schema_digest.bytes());
        hash.update(self.seed.to_be_bytes());
        hash.update([1]);
        hash.update([1]);
        hash.update(self.bits_per_key.to_be_bytes());
        hash.update(self.hash_count.to_be_bytes());
        hash.finalize().into()
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub fn build_bits(
    values: &MembershipValues,
    contract: BloomHashContract,
) -> Result<(u64, Vec<u8>), BloomError> {
    let bit_count = contract.bit_count(value_count(values))?;
    let mut bits = vec![0; usize::try_from(bit_count / 8).map_err(|_| BloomError::SizeOverflow)?];
    for_each_frame(values, |frame| {
        let digest = scalar_digest(contract, frame);
        let h1 = u64::from_be_bytes(digest[0..8].try_into().expect("digest has eight bytes"));
        let h2 = u64::from_be_bytes(digest[8..16].try_into().expect("digest has eight bytes"));
        for index in 0..u64::from(contract.hash_count) {
            let bit = h1.wrapping_add(index.wrapping_mul(h2)) % bit_count;
            bits[usize::try_from(bit / 8).expect("bit index fits allocation")] |= 1 << (bit % 8);
        }
    });
    Ok((bit_count, bits))
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn scalar_digest(contract: BloomHashContract, frame: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(SCALAR_HASH_DOMAIN);
    hash.update(CONTRACT_DOMAIN);
    hash.update(contract.algorithm_version.to_be_bytes());
    hash.update(contract.scalar_framing_version.to_be_bytes());
    hash.update(contract.schema_digest.bytes());
    hash.update(contract.seed.to_be_bytes());
    hash.update([1]);
    hash.update([1]);
    hash.update(contract.bits_per_key.to_be_bytes());
    hash.update(contract.hash_count.to_be_bytes());
    hash.update(frame);
    hash.finalize().into()
}
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn value_count(values: &MembershipValues) -> usize {
    match values {
        MembershipValues::Boolean(v) => v.len(),
        MembershipValues::Int8(v) => v.len(),
        MembershipValues::Int16(v) => v.len(),
        MembershipValues::Int32(v) => v.len(),
        MembershipValues::Int64(v) => v.len(),
        MembershipValues::LargeInt(v) => v.len(),
        MembershipValues::Temporal { values, .. } => values.len(),
        MembershipValues::Float32(v) => v.len(),
        MembershipValues::Float64(v) => v.len(),
        MembershipValues::Utf8(v) => v.len(),
        MembershipValues::Date32(v) => v.len(),
        MembershipValues::Timestamp { values, .. } => values.len(),
        MembershipValues::Decimal128 { values, .. } => values.len(),
    }
}
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn frame(tag: u8, value: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(9 + value.len());
    frame.push(tag);
    frame.extend_from_slice(&(value.len() as u64).to_be_bytes());
    frame.extend_from_slice(value);
    frame
}
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn for_each_frame(values: &MembershipValues, mut visit: impl FnMut(&[u8])) {
    macro_rules! fixed {
        ($tag:expr, $values:expr, $encode:expr) => {
            for value in $values {
                visit(&frame($tag, &$encode(value)));
            }
        };
    }
    match values {
        MembershipValues::Boolean(v) => fixed!(1, v, |x: &bool| [u8::from(*x)]),
        MembershipValues::Int8(v) => fixed!(2, v, |x: &i8| x.to_be_bytes()),
        MembershipValues::Int16(v) => fixed!(3, v, |x: &i16| x.to_be_bytes()),
        MembershipValues::Int32(v) => fixed!(4, v, |x: &i32| x.to_be_bytes()),
        MembershipValues::Int64(v) => fixed!(5, v, |x: &i64| x.to_be_bytes()),
        MembershipValues::LargeInt(v) => fixed!(6, v, |x: &i128| x.to_be_bytes()),
        MembershipValues::Temporal { values, .. } => fixed!(13, values, |key: &[u8; 12]| *key),
        MembershipValues::Float32(v) => fixed!(7, v, |x: &CanonicalF32| x.bits().to_be_bytes()),
        MembershipValues::Float64(v) => fixed!(8, v, |x: &CanonicalF64| x.bits().to_be_bytes()),
        MembershipValues::Utf8(v) => {
            for value in v {
                visit(&frame(9, value.as_bytes()));
            }
        }
        MembershipValues::Date32(v) => fixed!(10, v, |x: &i32| x.to_be_bytes()),
        MembershipValues::Timestamp { values, .. } => fixed!(11, values, |x: &i64| x.to_be_bytes()),
        MembershipValues::Decimal128 { values, .. } => {
            fixed!(12, values, |x: &i128| x.to_be_bytes())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::{
        RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics,
    };
    #[test]
    fn bloom_contract_and_bits_are_stable() {
        let contract =
            BloomHashContract::from_fields(ArtifactSchemaDigest::new([7; 32]), 1, 1, 17, 8, 5)
                .unwrap();
        let (count, bits) = build_bits(&MembershipValues::int64([1, 7, 42]), contract).unwrap();
        assert_eq!(count, 64);
        assert_eq!(bits.len(), 8);
        assert_eq!(
            bits,
            build_bits(&MembershipValues::int64([42, 7, 1, 7]), contract)
                .unwrap()
                .1
        );
    }

    #[test]
    fn migrated_core_canonical_bloom_vector_is_byte_identical() {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let contract = BloomHashContract::from_fields(
            ArtifactSchemaDigest::new(schema.digest()),
            1,
            1,
            17,
            8,
            5,
        )
        .unwrap();
        let (bit_count, bits) = build_bits(&MembershipValues::int64([1, 7, 42]), contract).unwrap();
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
