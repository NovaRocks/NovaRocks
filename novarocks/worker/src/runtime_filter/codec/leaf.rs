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

//! Canonical `NRFL` membership leaf codec.
//!
//! Values are never decoded through a Worker copy of the logical domain
//! grammar.  The leaf payload is reconstructed into Execution's canonical
//! `ValueDomainDelta` body and validated there before a resident index is made.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use novarocks_execution::runtime_filter::{
    KernelEvaluationControl, KernelFailure, LogicalVersion, RuntimeFilterMembershipSchema,
    RuntimeFilterNullSemantics,
    contribution::{self, ValueDomainDelta},
};

use crate::runtime_filter::artifact::{
    ArtifactKind, ArtifactSchemaDigest, HashContractDigest, LEAF_CODEC_VERSION, PhysicalArtifact,
    ResidentFilterIndex, ResidentMembershipIndex,
};

const MAGIC: &[u8; 4] = b"NRFL";
const FLAG_CONTAINS_NULL: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactCodecError {
    ContractViolation,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    Malformed,
    Truncated,
    UnknownVersion,
    UnknownKind,
    InvalidFlags,
    InvalidHashContract,
    KindMismatch,
    SchemaMismatch,
    InvalidLogicalVersion,
    VersionMismatch,
    HashContractMismatch,
    LengthOverflow,
    TrailingBytes,
    NonCanonicalPayload,
    EncodedSizeExceeded,
    ResourceLimit,
}
impl fmt::Display for ArtifactCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid runtime filter leaf artifact: {self:?}")
    }
}
impl Error for ArtifactCodecError {}

#[derive(Clone, Copy, Debug)]
pub struct ArtifactDecodeExpectations<'a> {
    pub expected_kind: ArtifactKind,
    pub schema: &'a RuntimeFilterMembershipSchema,
    pub expected_logical_version: LogicalVersion,
    pub expected_hash_contract: Option<HashContractDigest>,
}

pub fn encode_membership_leaf(
    domain: &ValueDomainDelta,
    schema: &RuntimeFilterMembershipSchema,
    logical_version: LogicalVersion,
) -> Result<Vec<u8>, ArtifactCodecError> {
    if !domain.matches_data_type(schema.data_type()) {
        return Err(ArtifactCodecError::SchemaMismatch);
    }
    let contains_null = domain.contains_null();
    if contains_null && schema.null_semantics() != RuntimeFilterNullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let kind = if domain.values().is_empty() && !contains_null {
        ArtifactKind::EmptyDomain
    } else {
        ArtifactKind::ValueSet
    };
    let payload = if kind == ArtifactKind::ValueSet {
        value_payload(domain)?
    } else {
        Vec::new()
    };
    encode_physical_leaf(kind, schema, logical_version, contains_null, None, &payload)
}

pub fn encode_physical_leaf(
    kind: ArtifactKind,
    schema: &RuntimeFilterMembershipSchema,
    logical_version: LogicalVersion,
    contains_null: bool,
    hash_contract: Option<HashContractDigest>,
    payload: &[u8],
) -> Result<Vec<u8>, ArtifactCodecError> {
    if contains_null && schema.null_semantics() != RuntimeFilterNullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    if (kind == ArtifactKind::Bloom) != hash_contract.is_some() {
        return Err(ArtifactCodecError::InvalidHashContract);
    }
    if kind == ArtifactKind::EmptyDomain && (contains_null || !payload.is_empty()) {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    if kind == ArtifactKind::Range {
        return Err(ArtifactCodecError::ContractViolation);
    }
    let schema_len = u16::try_from(schema.canonical_bytes().len())
        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let payload_len =
        u64::try_from(payload.len()).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let mut encoded = Vec::with_capacity(
        4 + 2
            + 1
            + 32
            + 2
            + usize::from(schema_len)
            + 8
            + 2
            + hash_contract.map_or(0, |_| 32)
            + 8
            + payload.len(),
    );
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&LEAF_CODEC_VERSION.to_be_bytes());
    encoded.push(kind.tag());
    encoded.extend_from_slice(&schema.digest());
    encoded.extend_from_slice(&schema_len.to_be_bytes());
    encoded.extend_from_slice(schema.canonical_bytes());
    encoded.extend_from_slice(&logical_version.get().to_be_bytes());
    encoded.push(if contains_null { FLAG_CONTAINS_NULL } else { 0 });
    match hash_contract {
        Some(value) => {
            encoded.push(1);
            encoded.extend_from_slice(&value.bytes());
        }
        None => encoded.push(0),
    }
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

pub fn decode_leaf(
    encoded: &[u8],
    expectations: ArtifactDecodeExpectations<'_>,
    max_artifact_bytes: usize,
) -> Result<Arc<PhysicalArtifact>, ArtifactCodecError> {
    if encoded.len() > max_artifact_bytes {
        return Err(ArtifactCodecError::EncodedSizeExceeded);
    }
    let parsed = parse_header(encoded)?;
    if parsed.kind != expectations.expected_kind {
        return Err(ArtifactCodecError::KindMismatch);
    }
    if parsed.schema.digest() != expectations.schema.digest() {
        return Err(ArtifactCodecError::SchemaMismatch);
    }
    if parsed.version != expectations.expected_logical_version {
        return Err(ArtifactCodecError::VersionMismatch);
    }
    if parsed.hash_contract != expectations.expected_hash_contract {
        return Err(ArtifactCodecError::HashContractMismatch);
    }
    if parsed.schema.canonical_bytes() != expectations.schema.canonical_bytes() {
        return Err(ArtifactCodecError::SchemaMismatch);
    }
    if parsed.contains_null
        && parsed.schema.null_semantics() != RuntimeFilterNullSemantics::NullSafeEqual
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let index = match parsed.kind {
        ArtifactKind::EmptyDomain => {
            if parsed.contains_null || !parsed.payload.is_empty() {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
            Some(ResidentMembershipIndex::EmptyDomain)
        }
        ArtifactKind::ValueSet => {
            let domain =
                decode_domain_payload(parsed.payload, parsed.contains_null, expectations.schema)?;
            if domain.values().is_empty() && !parsed.contains_null {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
            Some(inspect_membership_index(
                encoded,
                parsed.payload_offset,
                parsed.payload,
            )?)
        }
        ArtifactKind::Bitset => {
            validate_bitset(parsed.payload, &parsed.schema)?;
            None
        }
        ArtifactKind::Bloom => {
            validate_bloom(parsed.payload, &parsed.schema, parsed.hash_contract)?;
            None
        }
        ArtifactKind::Range => return Err(ArtifactCodecError::ContractViolation),
    };
    let filter_index = match parsed.kind {
        ArtifactKind::Bitset => Some(ResidentFilterIndex::Bitset {
            min: i64::from_be_bytes(parsed.payload[1..9].try_into().expect("validated bitset")),
            bit_count: u64::from_be_bytes(
                parsed.payload[17..25].try_into().expect("validated bitset"),
            ),
            bits: parsed.payload_offset + 25..parsed.payload_offset + parsed.payload.len(),
        }),
        ArtifactKind::Bloom => Some(ResidentFilterIndex::Bloom {
            bit_count: u64::from_be_bytes(
                parsed.payload[32..40].try_into().expect("validated bloom"),
            ),
            bits: parsed.payload_offset + 40..parsed.payload_offset + parsed.payload.len(),
            hash_contract: parsed.hash_contract.expect("validated bloom hash contract"),
        }),
        _ => None,
    };
    let artifact = PhysicalArtifact::new(
        parsed.kind,
        ArtifactSchemaDigest::new(parsed.schema.digest()),
        parsed.version,
        parsed.contains_null,
        Arc::from(encoded),
        index,
    );
    Ok(Arc::new(match filter_index {
        Some(index) => artifact.with_filter_index(index),
        None => artifact,
    }))
}

fn validate_bitset(
    payload: &[u8],
    schema: &RuntimeFilterMembershipSchema,
) -> Result<(), ArtifactCodecError> {
    if payload.len() < 25 {
        return Err(ArtifactCodecError::Truncated);
    }
    let tag = payload[0];
    let expected = match schema.data_type() {
        arrow::datatypes::DataType::Boolean => 1,
        arrow::datatypes::DataType::Int8 => 2,
        arrow::datatypes::DataType::Int16 => 3,
        arrow::datatypes::DataType::Int32 => 4,
        arrow::datatypes::DataType::Int64 => 5,
        arrow::datatypes::DataType::Date32 => 10,
        arrow::datatypes::DataType::Decimal128(1..=18, _) => 12,
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    };
    if tag != expected {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let min = i64::from_be_bytes(
        payload[1..9]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let max = i64::from_be_bytes(
        payload[9..17]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let bit_count = u64::from_be_bytes(
        payload[17..25]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let expected_bits = u64::try_from(
        i128::from(max)
            .checked_sub(i128::from(min))
            .and_then(|span| span.checked_add(1))
            .ok_or(ArtifactCodecError::LengthOverflow)?,
    )
    .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let byte_count = usize::try_from(
        bit_count
            .checked_add(7)
            .ok_or(ArtifactCodecError::LengthOverflow)?
            / 8,
    )
    .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if min > max
        || bit_count == 0
        || bit_count != expected_bits
        || payload.len()
            != 25usize
                .checked_add(byte_count)
                .ok_or(ArtifactCodecError::LengthOverflow)?
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let bits = &payload[25..];
    if bits.first().is_none_or(|value| value & 1 == 0)
        || bits
            .last()
            .is_none_or(|value| value & (1 << ((bit_count - 1) % 8)) == 0)
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let used_in_last = bit_count % 8;
    if used_in_last != 0
        && bits
            .last()
            .is_some_and(|byte| byte & !((1 << used_in_last) - 1) != 0)
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    Ok(())
}

fn validate_bloom(
    payload: &[u8],
    schema: &RuntimeFilterMembershipSchema,
    expected: Option<HashContractDigest>,
) -> Result<(), ArtifactCodecError> {
    if payload.len() < 40 {
        return Err(ArtifactCodecError::Truncated);
    }
    let algorithm_version = u16::from_be_bytes(
        payload[0..2]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let framing_version = u16::from_be_bytes(
        payload[2..4]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let seed = u64::from_be_bytes(
        payload[4..12]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let bits_per_key = u64::from_be_bytes(
        payload[12..20]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let hash_count = u32::from_be_bytes(
        payload[20..24]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let cardinality = u64::from_be_bytes(
        payload[24..32]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let bit_count = u64::from_be_bytes(
        payload[32..40]
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)?,
    );
    let contract = crate::runtime_filter::materializer::bloom::BloomHashContract::from_fields(
        ArtifactSchemaDigest::new(schema.digest()),
        algorithm_version,
        framing_version,
        seed,
        bits_per_key,
        hash_count,
    )
    .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
    if Some(contract.digest()) != expected
        || cardinality == 0
        || bit_count
            != contract
                .bit_count(
                    usize::try_from(cardinality).map_err(|_| ArtifactCodecError::LengthOverflow)?,
                )
                .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?
        || payload.len()
            != 40usize
                .checked_add(
                    usize::try_from(bit_count / 8)
                        .map_err(|_| ArtifactCodecError::LengthOverflow)?,
                )
                .ok_or(ArtifactCodecError::LengthOverflow)?
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    Ok(())
}

struct ParsedHeader<'a> {
    kind: ArtifactKind,
    schema: RuntimeFilterMembershipSchema,
    version: LogicalVersion,
    contains_null: bool,
    hash_contract: Option<HashContractDigest>,
    payload_offset: usize,
    payload: &'a [u8],
}

fn parse_header(encoded: &[u8]) -> Result<ParsedHeader<'_>, ArtifactCodecError> {
    let mut reader = Reader::new(encoded);
    if reader.take(4)? != MAGIC {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    if reader.u16()? != LEAF_CODEC_VERSION {
        return Err(ArtifactCodecError::UnknownVersion);
    }
    let kind = ArtifactKind::from_tag(reader.u8()?).ok_or(ArtifactCodecError::UnknownKind)?;
    let digest = reader.array::<32>()?;
    let schema_len = usize::from(reader.u16()?);
    let schema = RuntimeFilterMembershipSchema::from_canonical(reader.take(schema_len)?, digest)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
    let version = LogicalVersion::try_new(reader.u64()?)
        .map_err(|_| ArtifactCodecError::InvalidLogicalVersion)?;
    let flags = reader.u8()?;
    if flags & !FLAG_CONTAINS_NULL != 0 {
        return Err(ArtifactCodecError::InvalidFlags);
    }
    let hash_contract = match reader.u8()? {
        0 => None,
        1 => Some(HashContractDigest::new(reader.array::<32>()?)),
        _ => return Err(ArtifactCodecError::InvalidHashContract),
    };
    if (kind == ArtifactKind::Bloom) != hash_contract.is_some() {
        return Err(ArtifactCodecError::InvalidHashContract);
    }
    let payload_len =
        usize::try_from(reader.u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let payload_offset = reader.offset();
    let payload = reader.take(payload_len)?;
    if !reader.empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    Ok(ParsedHeader {
        kind,
        schema,
        version,
        contains_null: flags != 0,
        hash_contract,
        payload_offset,
        payload,
    })
}

fn value_payload(domain: &ValueDomainDelta) -> Result<Vec<u8>, ArtifactCodecError> {
    let mut canonical = Vec::new();
    domain
        .encode_canonical_into(&mut canonical)
        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let prefix = 8usize
        .checked_add(contribution::FINGERPRINT_VERSION_TAG.len())
        .ok_or(ArtifactCodecError::LengthOverflow)?;
    if canonical.len() < prefix + 1
        || canonical[..8]
            != u64::try_from(contribution::FINGERPRINT_VERSION_TAG.len())
                .map_err(|_| ArtifactCodecError::LengthOverflow)?
                .to_be_bytes()
        || canonical[8..prefix] != *contribution::FINGERPRINT_VERSION_TAG
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    Ok(canonical[prefix..canonical.len() - 1].to_vec())
}

fn decode_domain_payload(
    payload: &[u8],
    contains_null: bool,
    schema: &RuntimeFilterMembershipSchema,
) -> Result<ValueDomainDelta, ArtifactCodecError> {
    let mut canonical =
        Vec::with_capacity(8 + contribution::FINGERPRINT_VERSION_TAG.len() + payload.len() + 1);
    canonical
        .extend_from_slice(&(contribution::FINGERPRINT_VERSION_TAG.len() as u64).to_be_bytes());
    canonical.extend_from_slice(contribution::FINGERPRINT_VERSION_TAG);
    canonical.extend_from_slice(payload);
    canonical.push(u8::from(contains_null));
    let domain = contribution::decode_value_domain(&canonical, schema.data_type(), canonical.len())
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
    if let contribution::MembershipValues::Temporal { values, .. } = domain.values() {
        let declaration = schema
            .value_type()
            .and_then(|value_type| value_type.value_type().temporal())
            .ok_or(ArtifactCodecError::ContractViolation)?;
        if values.iter().any(|&encoded| {
            novarocks_type_contract::TemporalRelationKey::from_order_bytes(declaration, encoded)
                .is_err()
        }) {
            return Err(ArtifactCodecError::NonCanonicalPayload);
        }
    }
    Ok(domain)
}

fn inspect_membership_index(
    encoded: &[u8],
    payload_offset: usize,
    payload: &[u8],
) -> Result<ResidentMembershipIndex, ArtifactCodecError> {
    let mut reader = Reader::new(payload);
    let tag = reader.u8()?;
    let count = usize::try_from(match tag {
        11 => {
            reader.u8()?;
            match reader.u8()? {
                0 => {}
                1 => {
                    let len = usize::try_from(reader.u64()?)
                        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
                    reader.take(len)?;
                }
                _ => return Err(ArtifactCodecError::NonCanonicalPayload),
            };
            reader.u64()?
        }
        12 => {
            reader.u8()?;
            reader.u8()?;
            reader.u64()?
        }
        13 => {
            if !matches!(reader.u8()?, 10 | 12 | 14) {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
            reader.u64()?
        }
        _ => reader.u64()?,
    })
    .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if tag == 9 {
        // Restart after the tag; UTF-8 starts with its own count and variable lengths.
        let mut strings = Reader::new(&payload[1..]);
        let count =
            usize::try_from(strings.u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(count)
            .map_err(|_| ArtifactCodecError::ResourceLimit)?;
        for _ in 0..count {
            offsets.push(payload_offset + 1 + strings.offset());
            let len =
                usize::try_from(strings.u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
            strings.take(len)?;
        }
        if !strings.empty() {
            return Err(ArtifactCodecError::TrailingBytes);
        }
        return Ok(ResidentMembershipIndex::Utf8 {
            payload: payload_offset + 1..payload_offset + payload.len(),
            length_offsets: offsets.into_boxed_slice(),
        });
    }
    let width = match tag {
        1 | 2 => 1,
        3 => 2,
        4 | 7 | 10 => 4,
        5 | 8 | 11 => 8,
        6 | 12 => 16,
        13 => 12,
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    };
    let values_start = payload_offset + reader.offset();
    let byte_len = count
        .checked_mul(width)
        .ok_or(ArtifactCodecError::LengthOverflow)?;
    if reader.take(byte_len).is_err() || !reader.empty() {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    if encoded.get(values_start..values_start + byte_len).is_none() {
        return Err(ArtifactCodecError::Truncated);
    }
    Ok(ResidentMembershipIndex::Fixed {
        tag,
        values: values_start..values_start + byte_len,
        count,
        width,
    })
}

/// Typed scalar probe over a Backend-resident membership index.  This is an
/// artifact primitive: callers supply an already validated Execution scalar
/// and receive only a boolean membership fact.  It deliberately has no Arrow
/// or scan-domain dependency.
#[derive(Clone, Copy, Debug)]
pub enum MembershipProbe<'a> {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Utf8(&'a str),
    Date32(i32),
    Timestamp(i64),
    Temporal([u8; 12]),
    Decimal128(i128),
}

/// The original codec refusal and interruption remain distinct at the
/// resident probe boundary. Neither branch carries a row outcome or effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlledMembershipProbeError {
    Codec(ArtifactCodecError),
    Kernel(KernelFailure),
}
impl From<ArtifactCodecError> for ControlledMembershipProbeError {
    fn from(error: ArtifactCodecError) -> Self {
        Self::Codec(error)
    }
}
impl From<KernelFailure> for ControlledMembershipProbeError {
    fn from(error: KernelFailure) -> Self {
        Self::Kernel(error)
    }
}

enum MembershipQueryMode<'a> {
    Legacy,
    Controlled(&'a dyn KernelEvaluationControl),
}
impl MembershipQueryMode<'_> {
    fn checkpoint(&self, units: u32) -> Result<(), ControlledMembershipProbeError> {
        if let Self::Controlled(control) = self {
            control.checkpoint(units)?;
        }
        Ok(())
    }
    fn compare_utf8(
        &self,
        encoded: &[u8],
        offset: usize,
        needle: &str,
    ) -> Result<Ordering, ControlledMembershipProbeError> {
        match self {
            Self::Legacy => Ok(read_indexed_utf8(encoded, offset)?.cmp(needle)),
            Self::Controlled(control) => {
                let bytes = read_indexed_utf8_bytes(encoded, offset)?;
                validate_utf8_controlled(bytes, *control)?;
                Ok(contribution::compare_bytes_controlled(
                    bytes,
                    needle.as_bytes(),
                    *control,
                )?)
            }
        }
    }
}

pub fn indexed_membership_contains(
    encoded: &[u8],
    index: &ResidentMembershipIndex,
    probe: MembershipProbe<'_>,
) -> Result<bool, ArtifactCodecError> {
    indexed_membership_contains_with(encoded, index, probe, MembershipQueryMode::Legacy).map_err(
        |error| match error {
            ControlledMembershipProbeError::Codec(error) => error,
            ControlledMembershipProbeError::Kernel(_) => {
                unreachable!("legacy membership has no refusing control")
            }
        },
    )
}

pub fn indexed_membership_contains_controlled(
    encoded: &[u8],
    index: &ResidentMembershipIndex,
    probe: MembershipProbe<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<bool, ControlledMembershipProbeError> {
    indexed_membership_contains_with(
        encoded,
        index,
        probe,
        MembershipQueryMode::Controlled(control),
    )
}

fn indexed_membership_contains_with(
    encoded: &[u8],
    index: &ResidentMembershipIndex,
    probe: MembershipProbe<'_>,
    mode: MembershipQueryMode<'_>,
) -> Result<bool, ControlledMembershipProbeError> {
    mode.checkpoint(0)?;
    match index {
        ResidentMembershipIndex::EmptyDomain => Ok(false),
        ResidentMembershipIndex::Utf8 { length_offsets, .. } => {
            let MembershipProbe::Utf8(needle) = probe else {
                return Err(ArtifactCodecError::ContractViolation.into());
            };
            let mut low = 0usize;
            let mut high = length_offsets.len();
            while low < high {
                mode.checkpoint(1)?;
                let middle = low + (high - low) / 2;
                match mode.compare_utf8(encoded, length_offsets[middle], needle)? {
                    Ordering::Less => low = middle + 1,
                    Ordering::Greater => high = middle,
                    Ordering::Equal => return Ok(true),
                }
            }
            Ok(false)
        }
        ResidentMembershipIndex::Fixed {
            tag,
            values,
            count,
            width,
        } => {
            let bytes = encoded
                .get(values.clone())
                .ok_or(ArtifactCodecError::Truncated)?;
            let expected_len = count
                .checked_mul(*width)
                .ok_or(ArtifactCodecError::LengthOverflow)?;
            if bytes.len() != expected_len {
                return Err(ArtifactCodecError::Truncated.into());
            }
            let needle = fixed_probe(*tag, probe)?;
            let mut low = 0usize;
            let mut high = *count;
            while low < high {
                mode.checkpoint(1)?;
                let middle = low + (high - low) / 2;
                let value = fixed_value_at(bytes, middle, *width)?;
                match compare_fixed(*tag, value, needle)? {
                    Ordering::Less => low = middle + 1,
                    Ordering::Greater => high = middle,
                    Ordering::Equal => return Ok(true),
                }
            }
            Ok(false)
        }
    }
}

/// Tests whether a sorted resident ValueSet has an entry in the inclusive
/// closed range.  This stays logarithmic and never rehydrates a logical
/// domain, preserving the Backend physical-artifact boundary.
pub fn indexed_membership_range_may_match(
    encoded: &[u8],
    index: &ResidentMembershipIndex,
    inclusive_min: MembershipProbe<'_>,
    inclusive_max: MembershipProbe<'_>,
) -> Result<bool, ArtifactCodecError> {
    match index {
        ResidentMembershipIndex::EmptyDomain => Ok(false),
        ResidentMembershipIndex::Utf8 { length_offsets, .. } => {
            let (MembershipProbe::Utf8(min), MembershipProbe::Utf8(max)) =
                (inclusive_min, inclusive_max)
            else {
                return Err(ArtifactCodecError::ContractViolation);
            };
            if min > max {
                return Err(ArtifactCodecError::ContractViolation);
            }
            let mut low = 0usize;
            let mut high = length_offsets.len();
            while low < high {
                let middle = low + (high - low) / 2;
                if read_indexed_utf8(encoded, length_offsets[middle])? < min {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            match length_offsets.get(low) {
                Some(offset) => Ok(read_indexed_utf8(encoded, *offset)? <= max),
                None => Ok(false),
            }
        }
        ResidentMembershipIndex::Fixed {
            tag,
            values,
            count,
            width,
        } => {
            let bytes = encoded
                .get(values.clone())
                .ok_or(ArtifactCodecError::Truncated)?;
            let expected_len = count
                .checked_mul(*width)
                .ok_or(ArtifactCodecError::LengthOverflow)?;
            if bytes.len() != expected_len {
                return Err(ArtifactCodecError::Truncated);
            }
            let min = fixed_probe(*tag, inclusive_min)?;
            let max = fixed_probe(*tag, inclusive_max)?;
            if compare_probe(*tag, min, max)? == Ordering::Greater {
                return Err(ArtifactCodecError::ContractViolation);
            }
            let mut low = 0usize;
            let mut high = *count;
            while low < high {
                let middle = low + (high - low) / 2;
                let value = fixed_value_at(bytes, middle, *width)?;
                if compare_fixed(*tag, value, min)? == Ordering::Less {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            match (low < *count).then(|| fixed_value_at(bytes, low, *width)) {
                Some(Ok(value)) => Ok(compare_fixed(*tag, value, max)? != Ordering::Greater),
                Some(Err(error)) => Err(error),
                None => Ok(false),
            }
        }
    }
}

fn read_indexed_utf8_bytes(encoded: &[u8], offset: usize) -> Result<&[u8], ArtifactCodecError> {
    let mut reader = Reader::new(encoded.get(offset..).ok_or(ArtifactCodecError::Truncated)?);
    let len = usize::try_from(reader.u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    reader.take(len)
}

fn read_indexed_utf8(encoded: &[u8], offset: usize) -> Result<&str, ArtifactCodecError> {
    std::str::from_utf8(read_indexed_utf8_bytes(encoded, offset)?)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)
}

// Validate bounded standard-library windows. An incomplete code point at a
// window boundary is carried into the next window by retaining its <=3-byte
// suffix. No full-value validation, decoding or temporary String is needed.
fn validate_utf8_controlled(
    bytes: &[u8],
    control: &dyn KernelEvaluationControl,
) -> Result<(), ControlledMembershipProbeError> {
    let quantum = novarocks_execution::runtime_filter::MAX_UNOBSERVED_KERNEL_WORK as usize;
    let mut offset = 0;
    while offset < bytes.len() {
        let end = offset.saturating_add(quantum).min(bytes.len());
        let window = &bytes[offset..end];
        let validated = std::str::from_utf8(window);
        control.checkpoint(window.len() as u32)?;
        match validated {
            Ok(_) => offset = end,
            Err(error) if error.error_len().is_none() && end < bytes.len() => {
                let valid = error.valid_up_to();
                if valid == 0 {
                    return Err(ArtifactCodecError::NonCanonicalPayload.into());
                }
                offset += valid;
            }
            Err(_) => return Err(ArtifactCodecError::NonCanonicalPayload.into()),
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum FixedProbe {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    Temporal([u8; 12]),
    U32(u32),
    U64(u64),
}

fn fixed_probe(tag: u8, probe: MembershipProbe<'_>) -> Result<FixedProbe, ArtifactCodecError> {
    Ok(match (tag, probe) {
        (1, MembershipProbe::Boolean(value)) => FixedProbe::Bool(value),
        (2, MembershipProbe::Int8(value)) => FixedProbe::I8(value),
        (3, MembershipProbe::Int16(value)) => FixedProbe::I16(value),
        (4, MembershipProbe::Int32(value)) | (10, MembershipProbe::Date32(value)) => {
            FixedProbe::I32(value)
        }
        (5, MembershipProbe::Int64(value)) | (11, MembershipProbe::Timestamp(value)) => {
            FixedProbe::I64(value)
        }
        (6, MembershipProbe::LargeInt(value)) | (12, MembershipProbe::Decimal128(value)) => {
            FixedProbe::I128(value)
        }
        (13, MembershipProbe::Temporal(value)) => FixedProbe::Temporal(value),
        (7, MembershipProbe::Float32(value)) => FixedProbe::U32(canonical_probe_f32(value)),
        (8, MembershipProbe::Float64(value)) => FixedProbe::U64(canonical_probe_f64(value)),
        _ => return Err(ArtifactCodecError::ContractViolation),
    })
}

fn compare_fixed(
    tag: u8,
    bytes: &[u8],
    needle: FixedProbe,
) -> Result<Ordering, ArtifactCodecError> {
    macro_rules! decode {
        ($ty:ty) => {
            <$ty>::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| ArtifactCodecError::Truncated)?,
            )
        };
    }
    Ok(match (tag, needle) {
        (13, FixedProbe::Temporal(needle)) => bytes.cmp(&needle),
        (1, FixedProbe::Bool(value)) => match bytes {
            [0] => false.cmp(&value),
            [1] => true.cmp(&value),
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        },
        (2, FixedProbe::I8(value)) => decode!(i8).cmp(&value),
        (3, FixedProbe::I16(value)) => decode!(i16).cmp(&value),
        (4 | 10, FixedProbe::I32(value)) => decode!(i32).cmp(&value),
        (5 | 11, FixedProbe::I64(value)) => decode!(i64).cmp(&value),
        (6 | 12, FixedProbe::I128(value)) => decode!(i128).cmp(&value),
        (7, FixedProbe::U32(value)) => {
            f32::from_bits(decode!(u32)).total_cmp(&f32::from_bits(value))
        }
        (8, FixedProbe::U64(value)) => {
            f64::from_bits(decode!(u64)).total_cmp(&f64::from_bits(value))
        }
        _ => return Err(ArtifactCodecError::ContractViolation),
    })
}

fn fixed_value_at(bytes: &[u8], index: usize, width: usize) -> Result<&[u8], ArtifactCodecError> {
    let start = index
        .checked_mul(width)
        .ok_or(ArtifactCodecError::LengthOverflow)?;
    let end = start
        .checked_add(width)
        .ok_or(ArtifactCodecError::LengthOverflow)?;
    bytes.get(start..end).ok_or(ArtifactCodecError::Truncated)
}

fn compare_probe(
    tag: u8,
    left: FixedProbe,
    right: FixedProbe,
) -> Result<Ordering, ArtifactCodecError> {
    match (tag, left, right) {
        (1, FixedProbe::Bool(left), FixedProbe::Bool(right)) => Ok(left.cmp(&right)),
        (2, FixedProbe::I8(left), FixedProbe::I8(right)) => Ok(left.cmp(&right)),
        (3, FixedProbe::I16(left), FixedProbe::I16(right)) => Ok(left.cmp(&right)),
        (4 | 10, FixedProbe::I32(left), FixedProbe::I32(right)) => Ok(left.cmp(&right)),
        (5 | 11, FixedProbe::I64(left), FixedProbe::I64(right)) => Ok(left.cmp(&right)),
        (6 | 12, FixedProbe::I128(left), FixedProbe::I128(right)) => Ok(left.cmp(&right)),
        (7, FixedProbe::U32(left), FixedProbe::U32(right)) => {
            Ok(f32::from_bits(left).total_cmp(&f32::from_bits(right)))
        }
        (8, FixedProbe::U64(left), FixedProbe::U64(right)) => {
            Ok(f64::from_bits(left).total_cmp(&f64::from_bits(right)))
        }
        _ => Err(ArtifactCodecError::ContractViolation),
    }
}

fn canonical_probe_f32(value: f32) -> u32 {
    let bits = value.to_bits();
    if value.is_nan() {
        0x7fc0_0000
    } else if bits == (-0.0_f32).to_bits() {
        0
    } else {
        bits
    }
}

fn canonical_probe_f64(value: f64) -> u64 {
    let bits = value.to_bits();
    if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else if bits == (-0.0_f64).to_bits() {
        0
    } else {
        bits
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn offset(&self) -> usize {
        self.offset
    }
    fn empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], ArtifactCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ArtifactCodecError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ArtifactCodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, ArtifactCodecError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ArtifactCodecError> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, ArtifactCodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], ArtifactCodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ArtifactCodecError::Truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::{
        RuntimeFilterNullSemantics,
        contribution::{MembershipValues, ValueDomainDelta},
    };

    #[test]
    fn value_set_leaf_round_trips_with_execution_canonical_domain() {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let domain = ValueDomainDelta::new(MembershipValues::int64([9, 3, 9]), false);
        let bytes = encode_membership_leaf(&domain, &schema, LogicalVersion::FIRST).unwrap();
        let decoded = decode_leaf(
            &bytes,
            ArtifactDecodeExpectations {
                expected_kind: ArtifactKind::ValueSet,
                schema: &schema,
                expected_logical_version: LogicalVersion::FIRST,
                expected_hash_contract: None,
            },
            4096,
        )
        .unwrap();
        assert_eq!(&bytes[..7], b"NRFL\0\x01\x01");
        assert!(matches!(
            decoded.membership_index(),
            Some(ResidentMembershipIndex::Fixed {
                tag: 5,
                count: 2,
                ..
            })
        ));
    }

    #[test]
    fn nrfl_membership_bytes_match_the_v1_golden() {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let backend = encode_membership_leaf(
            &ValueDomainDelta::new(MembershipValues::int64([3, 9]), false),
            &schema,
            LogicalVersion::FIRST,
        )
        .unwrap();
        let expected = hex_bytes(
            "4e52464c0001013e87cf7b4c695573789dcd308efb51da3696ddd9e900e417e9ec460463254f0a002b6e6f7661726f636b732e72756e74696d652d66696c7465722e61727469666163742d736368656d6101050100000000000000010000000000000000001905000000000000000200000000000000030000000000000009",
        );
        assert_eq!(backend, expected);
    }

    #[test]
    fn nrfl_rejects_zero_logical_version() {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let domain = ValueDomainDelta::new(MembershipValues::int64([3]), false);
        let mut bytes = encode_membership_leaf(&domain, &schema, LogicalVersion::FIRST).unwrap();
        let version_offset = 4 + 2 + 1 + 32 + 2 + schema.canonical_bytes().len();
        bytes[version_offset..version_offset + 8].fill(0);
        assert!(matches!(
            decode_leaf(
                &bytes,
                ArtifactDecodeExpectations {
                    expected_kind: ArtifactKind::ValueSet,
                    schema: &schema,
                    expected_logical_version: LogicalVersion::FIRST,
                    expected_hash_contract: None,
                },
                4096,
            ),
            Err(ArtifactCodecError::InvalidLogicalVersion)
        ));
    }

    fn hex_bytes(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0, "golden hex must have complete bytes");
        (0..hex.len())
            .step_by(2)
            .map(|offset| u8::from_str_radix(&hex[offset..offset + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn empty_domain_and_null_contract_are_canonical() {
        let normal = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let empty = ValueDomainDelta::new(MembershipValues::int64([]), false);
        let bytes = encode_membership_leaf(&empty, &normal, LogicalVersion::FIRST).unwrap();
        assert_eq!(bytes[6], ArtifactKind::EmptyDomain.tag());
        let null_domain = ValueDomainDelta::new(MembershipValues::int64([1]), true);
        assert_eq!(
            encode_membership_leaf(&null_domain, &normal, LogicalVersion::FIRST),
            Err(ArtifactCodecError::NonCanonicalPayload)
        );
    }

    #[test]
    fn resident_membership_queries_preserve_canonical_numeric_and_utf8_order() {
        let int_schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let int_bytes = encode_membership_leaf(
            &ValueDomainDelta::new(MembershipValues::int64([3, 9, 42]), false),
            &int_schema,
            LogicalVersion::FIRST,
        )
        .unwrap();
        let int_artifact = decode_leaf(
            &int_bytes,
            ArtifactDecodeExpectations {
                expected_kind: ArtifactKind::ValueSet,
                schema: &int_schema,
                expected_logical_version: LogicalVersion::FIRST,
                expected_hash_contract: None,
            },
            4096,
        )
        .unwrap();
        let int_index = int_artifact.membership_index().unwrap();
        assert!(
            indexed_membership_contains(&int_bytes, int_index, MembershipProbe::Int64(9)).unwrap()
        );
        assert!(
            !indexed_membership_contains(&int_bytes, int_index, MembershipProbe::Int64(8)).unwrap()
        );
        assert!(
            indexed_membership_range_may_match(
                &int_bytes,
                int_index,
                MembershipProbe::Int64(8),
                MembershipProbe::Int64(42),
            )
            .unwrap()
        );

        let utf8_schema = RuntimeFilterMembershipSchema::new(
            &DataType::Utf8,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let utf8_bytes = encode_membership_leaf(
            &ValueDomainDelta::new(MembershipValues::utf8(["b", "z"]), false),
            &utf8_schema,
            LogicalVersion::FIRST,
        )
        .unwrap();
        let utf8_artifact = decode_leaf(
            &utf8_bytes,
            ArtifactDecodeExpectations {
                expected_kind: ArtifactKind::ValueSet,
                schema: &utf8_schema,
                expected_logical_version: LogicalVersion::FIRST,
                expected_hash_contract: None,
            },
            4096,
        )
        .unwrap();
        assert!(
            indexed_membership_range_may_match(
                &utf8_bytes,
                utf8_artifact.membership_index().unwrap(),
                MembershipProbe::Utf8("c"),
                MembershipProbe::Utf8("z"),
            )
            .unwrap()
        );
    }

    #[test]
    fn bitset_leaf_decodes_only_a_canonical_exact_payload() {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let values = MembershipValues::int64([-3, 0, 7]);
        let plan = crate::runtime_filter::materializer::bitset::BitsetPlan::new(&values).unwrap();
        let bits = crate::runtime_filter::materializer::bitset::build_bits(&values, plan).unwrap();
        let mut payload = vec![plan.type_tag()];
        payload.extend_from_slice(&plan.min().to_be_bytes());
        payload.extend_from_slice(&plan.max().to_be_bytes());
        payload.extend_from_slice(&plan.bit_count().to_be_bytes());
        payload.extend_from_slice(&bits);
        let bytes = encode_physical_leaf(
            ArtifactKind::Bitset,
            &schema,
            LogicalVersion::FIRST,
            false,
            None,
            &payload,
        )
        .unwrap();
        assert!(
            decode_leaf(
                &bytes,
                ArtifactDecodeExpectations {
                    expected_kind: ArtifactKind::Bitset,
                    schema: &schema,
                    expected_logical_version: LogicalVersion::FIRST,
                    expected_hash_contract: None,
                },
                4096,
            )
            .is_ok()
        );
        let mut noncanonical = bytes;
        *noncanonical.last_mut().unwrap() |= 0b1000_0000;
        assert!(
            decode_leaf(
                &noncanonical,
                ArtifactDecodeExpectations {
                    expected_kind: ArtifactKind::Bitset,
                    schema: &schema,
                    expected_logical_version: LogicalVersion::FIRST,
                    expected_hash_contract: None
                },
                4096,
            )
            .is_err()
        );
    }

    #[test]
    fn bloom_leaf_binds_the_frozen_hash_contract() {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let contract = crate::runtime_filter::materializer::bloom::BloomHashContract::from_fields(
            ArtifactSchemaDigest::new(schema.digest()),
            1,
            1,
            17,
            8,
            5,
        )
        .unwrap();
        let values = MembershipValues::int64([1, 7, 42]);
        let (bit_count, bits) =
            crate::runtime_filter::materializer::bloom::build_bits(&values, contract).unwrap();
        let mut payload = Vec::new();
        payload.extend_from_slice(&contract.algorithm_version().to_be_bytes());
        payload.extend_from_slice(&contract.scalar_framing_version().to_be_bytes());
        payload.extend_from_slice(&contract.seed().to_be_bytes());
        payload.extend_from_slice(&contract.bits_per_key().to_be_bytes());
        payload.extend_from_slice(&contract.hash_count().to_be_bytes());
        payload.extend_from_slice(&(3_u64).to_be_bytes());
        payload.extend_from_slice(&bit_count.to_be_bytes());
        payload.extend_from_slice(&bits);
        let bytes = encode_physical_leaf(
            ArtifactKind::Bloom,
            &schema,
            LogicalVersion::FIRST,
            false,
            Some(contract.digest()),
            &payload,
        )
        .unwrap();
        assert!(
            decode_leaf(
                &bytes,
                ArtifactDecodeExpectations {
                    expected_kind: ArtifactKind::Bloom,
                    schema: &schema,
                    expected_logical_version: LogicalVersion::FIRST,
                    expected_hash_contract: Some(contract.digest())
                },
                4096
            )
            .is_ok()
        );
        assert!(matches!(
            decode_leaf(
                &bytes,
                ArtifactDecodeExpectations {
                    expected_kind: ArtifactKind::Bloom,
                    schema: &schema,
                    expected_logical_version: LogicalVersion::FIRST,
                    expected_hash_contract: Some(HashContractDigest::new([0; 32]))
                },
                4096
            ),
            Err(ArtifactCodecError::HashContractMismatch)
        ));
    }
}

#[cfg(test)]
#[path = "leaf_controlled_membership_tests.rs"]
mod controlled_membership_tests;
