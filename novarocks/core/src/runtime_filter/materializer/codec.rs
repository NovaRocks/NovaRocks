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
use std::error::Error;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use crate::runtime_filter::model::contract::NullSemantics;
use crate::runtime_filter::model::contract::{ComparatorDigest, NullOrder, SortDirection};
use crate::runtime_filter::port::artifact::{
    ArtifactKind, ArtifactMembershipSchema, ArtifactMembershipSchemaView, ArtifactSchemaDigest,
    HashContractDigest, LEAF_CODEC_VERSION, PhysicalArtifact, RangeArtifactData,
    RangeArtifactResidentLayout, ResidentMembershipIndex, ResidentMembershipIndexView,
};
use crate::runtime_filter::port::identity::LogicalVersion;
use crate::runtime_filter::port::ordered_bound::{
    OrderContractDigest, OrderedScalar, OrderedTuple, RuntimeOrderContract, RuntimeOrderKey,
};
use crate::runtime_filter::port::support::{
    ArtifactRetainedBudget, ArtifactRetention, RetainedReservationError,
    RuntimeFilterMemoryAccount, TemporaryContributionLease,
};
use crate::runtime_filter::port::value_domain::{ContributionSizeError, ReducedMembershipDomain};

use super::bloom::{BLOOM_METADATA_BYTES, BloomHashContract};

const MAGIC: &[u8; 4] = b"NRFL";
const RANGE_MAGIC: &[u8; 4] = b"NRRG";
const RANGE_CODEC_VERSION: u16 = 1;
const FLAG_CONTAINS_NULL: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipIndexPlan {
    kind: ArtifactKind,
    layout: MembershipIndexLayoutPlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MembershipIndexLayoutPlan {
    EmptyDomain,
    Fixed {
        tag: u8,
        values: Range<usize>,
        count: usize,
        width: usize,
    },
    Utf8 {
        payload: Range<usize>,
        count: usize,
    },
}

impl MembershipIndexPlan {
    pub fn heap_bytes(&self) -> Result<usize, ArtifactCodecError> {
        match self.layout {
            MembershipIndexLayoutPlan::Utf8 { count, .. } => count
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or(ArtifactCodecError::LengthOverflow),
            MembershipIndexLayoutPlan::EmptyDomain | MembershipIndexLayoutPlan::Fixed { .. } => {
                Ok(0)
            }
        }
    }
}

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
    Decimal128(i128),
}

pub fn inspect_membership_index(encoded: &[u8]) -> Result<MembershipIndexPlan, ArtifactCodecError> {
    let header = parse_header(encoded)?;
    let payload_start = header.payload.as_ptr() as usize - encoded.as_ptr() as usize;
    let layout = match header.kind {
        ArtifactKind::EmptyDomain => {
            if header.contains_null || !header.payload.is_empty() {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
            MembershipIndexLayoutPlan::EmptyDomain
        }
        ArtifactKind::ValueSet => {
            validate_value_set(header.payload, header.contains_null, &header.schema)?;
            inspect_value_set_layout(header.payload, payload_start)?
        }
        _ => return Err(ArtifactCodecError::UnsupportedKind),
    };
    Ok(MembershipIndexPlan {
        kind: header.kind,
        layout,
    })
}

fn inspect_value_set_layout(
    payload: &[u8],
    payload_start: usize,
) -> Result<MembershipIndexLayoutPlan, ArtifactCodecError> {
    let mut reader = Reader::new(payload);
    let tag = reader.read_u8()?;
    if tag == 9 {
        let count =
            usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
        let start = payload.len() - reader.remaining_len();
        return Ok(MembershipIndexLayoutPlan::Utf8 {
            payload: payload_start + start..payload_start + payload.len(),
            count,
        });
    }
    let width = match tag {
        1 | 2 => 1,
        3 => 2,
        4 | 7 | 10 => 4,
        5 | 8 => 8,
        6 => 16,
        11 => {
            reader.read_u8()?;
            match reader.read_u8()? {
                0 => {}
                1 => {
                    let len = usize::try_from(reader.read_u64()?)
                        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
                    reader.read_exact(len)?;
                }
                _ => return Err(ArtifactCodecError::NonCanonicalPayload),
            }
            8
        }
        12 => {
            reader.read_u8()?;
            reader.read_u8()?;
            16
        }
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    };
    let count =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let start = payload.len() - reader.remaining_len();
    Ok(MembershipIndexLayoutPlan::Fixed {
        tag,
        values: payload_start + start..payload_start + payload.len(),
        count,
        width,
    })
}

pub fn build_membership_index(
    encoded: &[u8],
    plan: &MembershipIndexPlan,
) -> Result<ResidentMembershipIndex, ArtifactCodecError> {
    Ok(match &plan.layout {
        MembershipIndexLayoutPlan::EmptyDomain => ResidentMembershipIndex::empty_domain(encoded),
        MembershipIndexLayoutPlan::Fixed {
            tag,
            values,
            count,
            width,
        } => ResidentMembershipIndex::fixed(encoded, *tag, values.clone(), *count, *width),
        MembershipIndexLayoutPlan::Utf8 { payload, count } => {
            let bytes = encoded
                .get(payload.clone())
                .ok_or(ArtifactCodecError::Truncated)?;
            let mut reader = Reader::new(bytes);
            let mut offsets = Vec::new();
            offsets
                .try_reserve_exact(*count)
                .map_err(|_| ArtifactCodecError::ResourceUnavailable)?;
            for _ in 0..*count {
                offsets.push(payload.start + bytes.len() - reader.remaining_len());
                let len = usize::try_from(reader.read_u64()?)
                    .map_err(|_| ArtifactCodecError::LengthOverflow)?;
                reader.read_exact(len)?;
            }
            if !reader.is_empty() {
                return Err(ArtifactCodecError::TrailingBytes);
            }
            ResidentMembershipIndex::utf8(encoded, payload.clone(), offsets.into_boxed_slice())
        }
    })
}

pub fn validate_membership_index_binding(
    encoded: &[u8],
    kind: ArtifactKind,
    index: &ResidentMembershipIndex,
) -> Result<(), ArtifactCodecError> {
    let plan = inspect_membership_index(encoded)?;
    if plan.kind != kind {
        return Err(ArtifactCodecError::KindMismatch);
    }
    match (&plan.layout, index.view()) {
        (MembershipIndexLayoutPlan::EmptyDomain, ResidentMembershipIndexView::EmptyDomain) => {
            Ok(())
        }
        (
            MembershipIndexLayoutPlan::Fixed {
                tag: expected_tag,
                values: expected_values,
                count: expected_count,
                width: expected_width,
            },
            ResidentMembershipIndexView::Fixed {
                tag,
                values,
                count,
                width,
            },
        ) if *expected_tag == tag
            && expected_values == values
            && *expected_count == count
            && *expected_width == width =>
        {
            Ok(())
        }
        (
            MembershipIndexLayoutPlan::Utf8 {
                payload: expected_payload,
                count,
            },
            ResidentMembershipIndexView::Utf8 {
                payload,
                length_offsets,
            },
        ) if expected_payload == payload && *count == length_offsets.len() => {
            let bytes = encoded
                .get(expected_payload.clone())
                .ok_or(ArtifactCodecError::Truncated)?;
            let mut reader = Reader::new(bytes);
            for expected_offset in length_offsets {
                let actual_offset = expected_payload.start + bytes.len() - reader.remaining_len();
                if actual_offset != *expected_offset {
                    return Err(ArtifactCodecError::ContractViolation);
                }
                let len = usize::try_from(reader.read_u64()?)
                    .map_err(|_| ArtifactCodecError::LengthOverflow)?;
                reader.read_exact(len)?;
            }
            if !reader.is_empty() {
                return Err(ArtifactCodecError::TrailingBytes);
            }
            Ok(())
        }
        _ => Err(ArtifactCodecError::ContractViolation),
    }
}

pub fn indexed_membership_contains(
    encoded: &[u8],
    index: &ResidentMembershipIndex,
    probe: MembershipProbe<'_>,
) -> Result<bool, ArtifactCodecError> {
    indexed_membership_contains_inner(encoded, index, probe, |_| {})
}

fn indexed_membership_contains_inner(
    encoded: &[u8],
    index: &ResidentMembershipIndex,
    probe: MembershipProbe<'_>,
    mut compared: impl FnMut(()),
) -> Result<bool, ArtifactCodecError> {
    match index.view() {
        ResidentMembershipIndexView::EmptyDomain => Ok(false),
        ResidentMembershipIndexView::Utf8 { length_offsets, .. } => {
            let MembershipProbe::Utf8(needle) = probe else {
                return Err(ArtifactCodecError::ContractViolation);
            };
            let mut low = 0usize;
            let mut high = length_offsets.len();
            while low < high {
                let middle = low + (high - low) / 2;
                compared(());
                match read_indexed_utf8(encoded, length_offsets[middle])?.cmp(needle) {
                    Ordering::Less => low = middle + 1,
                    Ordering::Greater => high = middle,
                    Ordering::Equal => return Ok(true),
                }
            }
            Ok(false)
        }
        ResidentMembershipIndexView::Fixed {
            tag,
            values,
            count,
            width,
        } => {
            let bytes = encoded
                .get(values.clone())
                .ok_or(ArtifactCodecError::Truncated)?;
            let expected_len = count
                .checked_mul(width)
                .ok_or(ArtifactCodecError::LengthOverflow)?;
            if bytes.len() != expected_len {
                return Err(ArtifactCodecError::Truncated);
            }
            let needle = fixed_probe(tag, probe)?;
            let mut low = 0usize;
            let mut high = count;
            while low < high {
                let middle = low + (high - low) / 2;
                let start = middle
                    .checked_mul(width)
                    .ok_or(ArtifactCodecError::LengthOverflow)?;
                let end = start
                    .checked_add(width)
                    .ok_or(ArtifactCodecError::LengthOverflow)?;
                let value = bytes.get(start..end).ok_or(ArtifactCodecError::Truncated)?;
                compared(());
                match compare_fixed(tag, value, needle)? {
                    Ordering::Less => low = middle + 1,
                    Ordering::Greater => high = middle,
                    Ordering::Equal => return Ok(true),
                }
            }
            Ok(false)
        }
    }
}

#[cfg(test)]
pub fn indexed_membership_contains_counted_for_test(
    encoded: &[u8],
    index: &ResidentMembershipIndex,
    probe: MembershipProbe<'_>,
) -> Result<(bool, usize), ArtifactCodecError> {
    let mut comparisons = 0usize;
    let found = indexed_membership_contains_inner(encoded, index, probe, |_| comparisons += 1)?;
    Ok((found, comparisons))
}

fn read_indexed_utf8(encoded: &[u8], offset: usize) -> Result<&str, ArtifactCodecError> {
    let mut reader = Reader::new(encoded.get(offset..).ok_or(ArtifactCodecError::Truncated)?);
    let len =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    std::str::from_utf8(reader.read_exact(len)?)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)
}

#[derive(Clone, Copy)]
enum FixedProbe {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U32(u32),
    U64(u64),
}

fn fixed_probe(tag: u8, probe: MembershipProbe<'_>) -> Result<FixedProbe, ArtifactCodecError> {
    Ok(match (tag, probe) {
        (1, MembershipProbe::Boolean(v)) => FixedProbe::Bool(v),
        (2, MembershipProbe::Int8(v)) => FixedProbe::I8(v),
        (3, MembershipProbe::Int16(v)) => FixedProbe::I16(v),
        (4, MembershipProbe::Int32(v)) | (10, MembershipProbe::Date32(v)) => FixedProbe::I32(v),
        (5, MembershipProbe::Int64(v)) | (11, MembershipProbe::Timestamp(v)) => FixedProbe::I64(v),
        (6, MembershipProbe::LargeInt(v)) | (12, MembershipProbe::Decimal128(v)) => {
            FixedProbe::I128(v)
        }
        (7, MembershipProbe::Float32(v)) => FixedProbe::U32(canonical_probe_f32(v)),
        (8, MembershipProbe::Float64(v)) => FixedProbe::U64(canonical_probe_f64(v)),
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
        (1, FixedProbe::Bool(v)) => match bytes {
            [0] => false.cmp(&v),
            [1] => true.cmp(&v),
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        },
        (2, FixedProbe::I8(v)) => decode!(i8).cmp(&v),
        (3, FixedProbe::I16(v)) => decode!(i16).cmp(&v),
        (4 | 10, FixedProbe::I32(v)) => decode!(i32).cmp(&v),
        (5 | 11, FixedProbe::I64(v)) => decode!(i64).cmp(&v),
        (6 | 12, FixedProbe::I128(v)) => decode!(i128).cmp(&v),
        (7, FixedProbe::U32(v)) => f32::from_bits(decode!(u32)).total_cmp(&f32::from_bits(v)),
        (8, FixedProbe::U64(v)) => f64::from_bits(decode!(u64)).total_cmp(&f64::from_bits(v)),
        _ => return Err(ArtifactCodecError::ContractViolation),
    })
}

fn canonical_probe_f32(value: f32) -> u32 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        0x7fc0_0000
    } else {
        value.to_bits()
    }
}

fn canonical_probe_f64(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        value.to_bits()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactDecodeExpectations {
    pub expected_kind: ArtifactKind,
    pub expected_schema_digest: ArtifactSchemaDigest,
    pub expected_logical_version: LogicalVersion,
    pub expected_hash_contract: Option<HashContractDigest>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeDecodeExpectations {
    pub expected_order_digest: OrderContractDigest,
    pub expected_logical_version: LogicalVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactCodecError {
    ContractViolation,
    Malformed,
    ResourceUnavailable,
    Truncated,
    UnknownVersion,
    UnknownKind,
    UnsupportedKind,
    InvalidFlags,
    InvalidHashContract,
    KindMismatch,
    SchemaMismatch,
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

impl From<ContributionSizeError> for ArtifactCodecError {
    fn from(error: ContributionSizeError) -> Self {
        match error {
            ContributionSizeError::LengthExceedsCanonicalRange
            | ContributionSizeError::SizeOverflow => Self::LengthOverflow,
        }
    }
}

impl From<RetainedReservationError> for ArtifactCodecError {
    fn from(_error: RetainedReservationError) -> Self {
        Self::ResourceLimit
    }
}

pub fn encode_membership_leaf(
    domain: &ReducedMembershipDomain,
    null_semantics: NullSemantics,
    logical_version: LogicalVersion,
) -> Result<Vec<u8>, ArtifactCodecError> {
    let contains_null = domain.contains_null();
    if contains_null && null_semantics != NullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let kind = if domain.values().is_empty() && !contains_null {
        ArtifactKind::EmptyDomain
    } else {
        ArtifactKind::ValueSet
    };
    let schema = ArtifactMembershipSchema::new(&domain.data_type(), null_semantics)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
    let mut payload = Vec::new();
    if kind == ArtifactKind::ValueSet {
        let payload_len = domain.values().canonical_encoded_len()?;
        payload
            .try_reserve_exact(payload_len)
            .map_err(|_| ArtifactCodecError::ResourceLimit)?;
        domain.values().encode_canonical_into(&mut payload)?;
    }
    encode_physical_leaf(
        kind,
        &schema,
        logical_version,
        contains_null,
        None,
        &payload,
    )
}

pub fn encoded_leaf_len(
    schema: &ArtifactMembershipSchema,
    hash_contract: Option<HashContractDigest>,
    payload_len: usize,
) -> Result<usize, ArtifactCodecError> {
    u16::try_from(schema.canonical_bytes().len())
        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    u64::try_from(payload_len).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    4usize
        .checked_add(2)
        .and_then(|size| {
            size.checked_add(
                1 + 32
                    + 2
                    + schema.canonical_bytes().len()
                    + 8
                    + 1
                    + 1
                    + hash_contract.map_or(0, |_| 32)
                    + 8,
            )
        })
        .and_then(|size| size.checked_add(payload_len))
        .ok_or(ArtifactCodecError::LengthOverflow)
}

pub fn encode_physical_leaf(
    kind: ArtifactKind,
    schema: &ArtifactMembershipSchema,
    logical_version: LogicalVersion,
    contains_null: bool,
    hash_contract: Option<HashContractDigest>,
    payload: &[u8],
) -> Result<Vec<u8>, ArtifactCodecError> {
    if contains_null && schema.null_semantics() != NullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    if matches!(kind, ArtifactKind::Bloom) != hash_contract.is_some() {
        return Err(ArtifactCodecError::InvalidHashContract);
    }
    if kind == ArtifactKind::EmptyDomain && (contains_null || !payload.is_empty()) {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let schema_len = u16::try_from(schema.canonical_bytes().len())
        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let capacity = encoded_leaf_len(schema, hash_contract, payload.len())?;
    let payload_len =
        u64::try_from(payload.len()).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&LEAF_CODEC_VERSION.to_be_bytes());
    encoded.push(kind.tag());
    encoded.extend_from_slice(&schema.digest().bytes());
    encoded.extend_from_slice(&schema_len.to_be_bytes());
    encoded.extend_from_slice(schema.canonical_bytes());
    encoded.extend_from_slice(&logical_version.get().to_be_bytes());
    encoded.push(u8::from(contains_null) * FLAG_CONTAINS_NULL);
    match hash_contract {
        Some(digest) => {
            encoded.push(1);
            encoded.extend_from_slice(&digest.bytes());
        }
        None => encoded.push(0),
    }
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(payload);
    debug_assert_eq!(encoded.len(), capacity);
    Ok(encoded)
}

pub fn decode_leaf(
    encoded: &[u8],
    expectations: ArtifactDecodeExpectations,
    max_artifact_bytes: usize,
    retained_budget: Arc<ArtifactRetainedBudget>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Result<Arc<PhysicalArtifact>, ArtifactCodecError> {
    if encoded.len() > max_artifact_bytes {
        return Err(ArtifactCodecError::EncodedSizeExceeded);
    }
    let header = parse_header(encoded)?;
    if header.kind != expectations.expected_kind {
        return Err(ArtifactCodecError::KindMismatch);
    }
    if header.schema_digest != expectations.expected_schema_digest {
        return Err(ArtifactCodecError::SchemaMismatch);
    }
    if header.logical_version != expectations.expected_logical_version {
        return Err(ArtifactCodecError::VersionMismatch);
    }
    if header.hash_contract != expectations.expected_hash_contract {
        return Err(ArtifactCodecError::HashContractMismatch);
    }
    if header.contains_null && header.schema.null_semantics() != NullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    match header.kind {
        ArtifactKind::EmptyDomain => {
            if header.contains_null || !header.payload.is_empty() {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
        }
        ArtifactKind::ValueSet => {
            validate_value_set(header.payload, header.contains_null, &header.schema)?
        }
        ArtifactKind::Bitset => {
            validate_bitset(header.payload, header.contains_null, header.schema)?
        }
        ArtifactKind::Bloom => validate_bloom(
            header.payload,
            header.contains_null,
            header.schema,
            header
                .hash_contract
                .ok_or(ArtifactCodecError::InvalidHashContract)?,
        )?,
        ArtifactKind::Range => return Err(ArtifactCodecError::UnsupportedKind),
    }

    let index_plan = match header.kind {
        ArtifactKind::ValueSet | ArtifactKind::EmptyDomain => {
            Some(inspect_membership_index(encoded)?)
        }
        ArtifactKind::Bitset | ArtifactKind::Bloom => None,
        ArtifactKind::Range => unreachable!("range artifacts returned above"),
    };
    let index_heap_bytes = index_plan
        .as_ref()
        .map_or(Ok(0), MembershipIndexPlan::heap_bytes)?;
    let accounted_resident_bytes = if index_plan.is_some() {
        PhysicalArtifact::accounted_indexed_resident_bytes(encoded.len(), index_heap_bytes)
    } else {
        PhysicalArtifact::accounted_resident_bytes(encoded.len())
    }
    .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let retention =
        ArtifactRetention::try_new(accounted_resident_bytes, retained_budget, memory_account)?;
    let index = index_plan
        .as_ref()
        .map(|plan| build_membership_index(encoded, plan))
        .transpose()?;
    let bytes: Arc<[u8]> = Arc::from(encoded);
    let artifact = if let Some(index) = index {
        PhysicalArtifact::from_indexed_retained_bytes(
            header.kind,
            header.schema_digest,
            header.logical_version,
            header.contains_null,
            bytes,
            index,
            accounted_resident_bytes,
            retention,
        )
    } else {
        PhysicalArtifact::from_retained_bytes(
            header.kind,
            header.schema_digest,
            header.logical_version,
            header.contains_null,
            bytes,
            accounted_resident_bytes,
            retention,
        )
    }
    .map_err(|_| ArtifactCodecError::ResourceLimit)?;
    Ok(Arc::new(artifact))
}

struct ParsedHeader<'a> {
    kind: ArtifactKind,
    schema_digest: ArtifactSchemaDigest,
    schema: ArtifactMembershipSchemaView<'a>,
    logical_version: LogicalVersion,
    contains_null: bool,
    hash_contract: Option<HashContractDigest>,
    payload: &'a [u8],
}

fn parse_header(encoded: &[u8]) -> Result<ParsedHeader<'_>, ArtifactCodecError> {
    let mut reader = Reader::new(encoded);
    if reader.read_exact(4)? != MAGIC {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let version = reader.read_u16()?;
    if version != LEAF_CODEC_VERSION {
        return Err(ArtifactCodecError::UnknownVersion);
    }
    let kind = ArtifactKind::from_tag(reader.read_u8()?).ok_or(ArtifactCodecError::UnknownKind)?;
    let schema_digest = ArtifactSchemaDigest::from_canonical_bytes(reader.read_array::<32>()?);
    let schema_len = usize::from(reader.read_u16()?);
    let schema = ArtifactMembershipSchema::view(reader.read_exact(schema_len)?)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
    if schema.digest() != schema_digest {
        return Err(ArtifactCodecError::SchemaMismatch);
    }
    let logical_version = LogicalVersion::new(reader.read_u64()?);
    let flags = reader.read_u8()?;
    if flags & !FLAG_CONTAINS_NULL != 0 {
        return Err(ArtifactCodecError::InvalidFlags);
    }
    let hash_contract = match reader.read_u8()? {
        0 => None,
        1 => Some(HashContractDigest::new(reader.read_array::<32>()?)),
        _ => return Err(ArtifactCodecError::InvalidHashContract),
    };
    if matches!(kind, ArtifactKind::Bloom) != hash_contract.is_some() {
        return Err(ArtifactCodecError::InvalidHashContract);
    }
    let payload_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let payload = reader.read_exact(payload_len)?;
    if !reader.is_empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    Ok(ParsedHeader {
        kind,
        schema_digest,
        schema,
        logical_version,
        contains_null: flags & FLAG_CONTAINS_NULL != 0,
        hash_contract,
        payload,
    })
}

fn validate_value_set(
    payload: &[u8],
    contains_null: bool,
    schema: &ArtifactMembershipSchemaView<'_>,
) -> Result<(), ArtifactCodecError> {
    let mut reader = Reader::new(payload);
    let tag = reader.read_u8()?;
    if tag != schema.payload_tag() {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    match tag {
        1 => validate_ordered(&mut reader, |reader| match reader.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ArtifactCodecError::NonCanonicalPayload),
        })?,
        2 => validate_ordered(&mut reader, |reader| Ok(reader.read_u8()? as i8))?,
        3 => validate_ordered(&mut reader, |reader| reader.read_i16())?,
        4 | 10 => validate_ordered(&mut reader, |reader| reader.read_i32())?,
        5 => validate_ordered(&mut reader, |reader| reader.read_i64())?,
        6 => validate_ordered(&mut reader, |reader| reader.read_i128())?,
        7 => validate_ordered_by(
            &mut reader,
            |reader| reader.read_u32(),
            |left, right| f32::from_bits(*left).total_cmp(&f32::from_bits(*right)),
            canonical_f32_bits,
        )?,
        8 => validate_ordered_by(
            &mut reader,
            |reader| reader.read_u64(),
            |left, right| f64::from_bits(*left).total_cmp(&f64::from_bits(*right)),
            canonical_f64_bits,
        )?,
        9 => validate_utf8(&mut reader)?,
        11 => validate_timestamp(&mut reader, *schema)?,
        12 => validate_decimal(&mut reader, *schema)?,
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    }
    if !reader.is_empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    let cardinality = cardinality_from_payload(payload)?;
    if cardinality == 0 && !contains_null {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    if contains_null && schema.null_semantics() != NullSemantics::NullSafeEqual {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    Ok(())
}

fn validate_bitset(
    payload: &[u8],
    _contains_null: bool,
    schema: ArtifactMembershipSchemaView<'_>,
) -> Result<(), ArtifactCodecError> {
    let mut reader = Reader::new(payload);
    let type_tag = reader.read_u8()?;
    if type_tag != schema.payload_tag()
        || !matches!(type_tag, 1 | 2 | 3 | 4 | 5 | 10 | 12)
        || (type_tag == 12 && !matches!(schema.decimal_contract(), Some((1..=18, _))))
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let min = reader.read_i64()?;
    let max = reader.read_i64()?;
    let endpoints_representable = match type_tag {
        1 => min >= 0 && max <= 1,
        2 => min >= i64::from(i8::MIN) && max <= i64::from(i8::MAX),
        3 => min >= i64::from(i16::MIN) && max <= i64::from(i16::MAX),
        4 | 10 => min >= i64::from(i32::MIN) && max <= i64::from(i32::MAX),
        5 => true,
        12 => schema.decimal_contract().is_some_and(|(precision, _)| {
            10_i64
                .checked_pow(u32::from(precision))
                .is_some_and(|limit| min > -limit && max < limit)
        }),
        _ => false,
    };
    if min > max || !endpoints_representable {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let bit_count = reader.read_u64()?;
    let expected = i128::from(max)
        .checked_sub(i128::from(min))
        .and_then(|span| span.checked_add(1))
        .and_then(|span| u64::try_from(span).ok())
        .ok_or(ArtifactCodecError::LengthOverflow)?;
    if bit_count == 0 || bit_count != expected {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let byte_count = usize::try_from(
        bit_count
            .checked_add(7)
            .ok_or(ArtifactCodecError::LengthOverflow)?
            / 8,
    )
    .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if reader.remaining_len() != byte_count {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let bits = reader.read_exact(byte_count)?;
    if bits.first().is_none_or(|byte| byte & 1 == 0) {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let last_index = bit_count - 1;
    let last_byte =
        usize::try_from(last_index / 8).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if bits[last_byte] & (1 << (last_index % 8)) == 0 {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let used_in_last = bit_count % 8;
    if used_in_last != 0 {
        let padding_mask = !((1u8 << used_in_last) - 1);
        if bits.last().is_some_and(|byte| byte & padding_mask != 0) {
            return Err(ArtifactCodecError::NonCanonicalPayload);
        }
    }
    Ok(())
}

fn validate_bloom(
    payload: &[u8],
    _contains_null: bool,
    schema: ArtifactMembershipSchemaView<'_>,
    expected_digest: HashContractDigest,
) -> Result<(), ArtifactCodecError> {
    if payload.len() < BLOOM_METADATA_BYTES {
        return Err(ArtifactCodecError::Truncated);
    }
    let mut reader = Reader::new(payload);
    let algorithm_version = reader.read_u16()?;
    let scalar_framing_version = reader.read_u16()?;
    let seed = reader.read_u64()?;
    let bits_per_key = reader.read_u64()?;
    let hash_count = reader.read_u32()?;
    let cardinality = reader.read_u64()?;
    let bit_count = reader.read_u64()?;
    let contract = BloomHashContract::from_fields(
        schema.digest(),
        algorithm_version,
        scalar_framing_version,
        seed,
        bits_per_key,
        hash_count,
    )
    .map_err(|_| ArtifactCodecError::InvalidHashContract)?;
    if contract.digest() != expected_digest
        || contract
            .bit_count_u64(cardinality)
            .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?
            != bit_count
        || bit_count % 64 != 0
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let byte_count =
        usize::try_from(bit_count / 8).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if reader.remaining_len() != byte_count {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let bits = reader.read_exact(byte_count)?;
    if bits.iter().all(|byte| *byte == 0) {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    Ok(())
}

fn cardinality_from_payload(payload: &[u8]) -> Result<u64, ArtifactCodecError> {
    let tag = *payload.first().ok_or(ArtifactCodecError::Truncated)?;
    let offset = match tag {
        11 => {
            let mut reader = Reader::new(&payload[1..]);
            reader.read_u8()?;
            match reader.read_u8()? {
                0 => {}
                1 => {
                    let len = usize::try_from(reader.read_u64()?)
                        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
                    reader.read_exact(len)?;
                }
                _ => return Err(ArtifactCodecError::NonCanonicalPayload),
            }
            payload.len() - reader.remaining_len()
        }
        12 => 3,
        _ => 1,
    };
    let bytes = payload
        .get(offset..offset + 8)
        .ok_or(ArtifactCodecError::Truncated)?;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("eight-byte slice"),
    ))
}

fn validate_ordered<T: Ord>(
    reader: &mut Reader<'_>,
    read: impl FnMut(&mut Reader<'_>) -> Result<T, ArtifactCodecError>,
) -> Result<(), ArtifactCodecError> {
    validate_ordered_by(reader, read, Ord::cmp, |_| true)
}

fn validate_ordered_by<T>(
    reader: &mut Reader<'_>,
    mut read: impl FnMut(&mut Reader<'_>) -> Result<T, ArtifactCodecError>,
    compare: impl Fn(&T, &T) -> Ordering,
    canonical: impl Fn(&T) -> bool,
) -> Result<(), ArtifactCodecError> {
    let count =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let mut previous = None;
    for _ in 0..count {
        let value = read(reader)?;
        if !canonical(&value)
            || previous
                .as_ref()
                .is_some_and(|old| compare(old, &value) != Ordering::Less)
        {
            return Err(ArtifactCodecError::NonCanonicalPayload);
        }
        previous = Some(value);
    }
    Ok(())
}

fn canonical_f32_bits(bits: &u32) -> bool {
    let value = f32::from_bits(*bits);
    (!value.is_nan() || *bits == 0x7fc0_0000) && (value != 0.0 || *bits == 0)
}

fn canonical_f64_bits(bits: &u64) -> bool {
    let value = f64::from_bits(*bits);
    (!value.is_nan() || *bits == 0x7ff8_0000_0000_0000) && (value != 0.0 || *bits == 0)
}

fn validate_utf8(reader: &mut Reader<'_>) -> Result<(), ArtifactCodecError> {
    let count =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let mut previous: Option<&str> = None;
    for _ in 0..count {
        let len =
            usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
        let value = std::str::from_utf8(reader.read_exact(len)?)
            .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
        if previous.is_some_and(|old| old >= value) {
            return Err(ArtifactCodecError::NonCanonicalPayload);
        }
        previous = Some(value);
    }
    Ok(())
}

fn validate_timestamp(
    reader: &mut Reader<'_>,
    expected: ArtifactMembershipSchemaView<'_>,
) -> Result<(), ArtifactCodecError> {
    let Some((expected_unit, expected_timezone)) = expected.timestamp_contract() else {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    };
    let unit = reader.read_u8()?;
    if unit != expected_unit {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    match reader.read_u8()? {
        0 if expected_timezone.is_none() => {}
        1 => {
            let len = usize::try_from(reader.read_u64()?)
                .map_err(|_| ArtifactCodecError::LengthOverflow)?;
            let timezone = std::str::from_utf8(reader.read_exact(len)?)
                .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
            if expected_timezone != Some(timezone) {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
        }
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    }
    validate_ordered(reader, |reader| reader.read_i64())
}

fn validate_decimal(
    reader: &mut Reader<'_>,
    expected: ArtifactMembershipSchemaView<'_>,
) -> Result<(), ArtifactCodecError> {
    let Some((expected_precision, expected_scale)) = expected.decimal_contract() else {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    };
    let precision = reader.read_u8()?;
    let scale = reader.read_u8()? as i8;
    if precision != expected_precision
        || scale != expected_scale
        || precision == 0
        || precision > arrow::datatypes::DECIMAL128_MAX_PRECISION
        || (scale > 0 && scale as u8 > precision)
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let bound = 10_i128
        .checked_pow(u32::from(precision))
        .ok_or(ArtifactCodecError::NonCanonicalPayload)?;
    validate_ordered_by(
        reader,
        |reader| reader.read_i128(),
        Ord::cmp,
        |value| *value > -bound && *value < bound,
    )
}

pub fn encoded_range_leaf_len(
    contract: &RuntimeOrderContract,
    bound: &OrderedTuple,
) -> Result<usize, ArtifactCodecError> {
    contract
        .compare(bound, bound)
        .map_err(|_| ArtifactCodecError::ContractViolation)?;
    let contract_len = encoded_order_contract_len(contract)?;
    let tuple_len = encoded_order_tuple_len(contract, bound)?;
    u32::try_from(contract_len).map_err(|_| ArtifactCodecError::ResourceUnavailable)?;
    u64::try_from(tuple_len).map_err(|_| ArtifactCodecError::ResourceUnavailable)?;
    4usize
        .checked_add(2 + 1 + 32 + 8 + 4)
        .and_then(|bytes| bytes.checked_add(contract_len))
        .and_then(|bytes| bytes.checked_add(8))
        .and_then(|bytes| bytes.checked_add(tuple_len))
        .ok_or(ArtifactCodecError::ResourceUnavailable)
}

pub fn encode_range_leaf(
    contract: &RuntimeOrderContract,
    bound: &OrderedTuple,
    logical_version: LogicalVersion,
) -> Result<Vec<u8>, ArtifactCodecError> {
    let capacity = encoded_range_leaf_len(contract, bound)?;
    let contract_len = encoded_order_contract_len(contract)?;
    let tuple_len = encoded_order_tuple_len(contract, bound)?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(RANGE_MAGIC);
    encoded.extend_from_slice(&RANGE_CODEC_VERSION.to_be_bytes());
    encoded.push(ArtifactKind::Range.tag());
    encoded.extend_from_slice(&contract.digest().bytes());
    encoded.extend_from_slice(&logical_version.get().to_be_bytes());
    encoded.extend_from_slice(&(contract_len as u32).to_be_bytes());
    encode_order_contract(contract, &mut encoded)?;
    encoded.extend_from_slice(&(tuple_len as u64).to_be_bytes());
    encode_order_tuple(contract, bound, &mut encoded)?;
    debug_assert_eq!(encoded.len(), capacity);
    Ok(encoded)
}

pub fn decode_range(
    encoded: &[u8],
    expectations: RangeDecodeExpectations,
    max_artifact_bytes: usize,
    retained_budget: Arc<ArtifactRetainedBudget>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Result<Arc<PhysicalArtifact>, ArtifactCodecError> {
    if encoded.len() > max_artifact_bytes {
        return Err(ArtifactCodecError::ResourceUnavailable);
    }
    let header = parse_range_header(encoded).map_err(|_| ArtifactCodecError::Malformed)?;
    if header.kind != ArtifactKind::Range
        || header.order_digest != expectations.expected_order_digest
        || header.logical_version != expectations.expected_logical_version
    {
        return Err(ArtifactCodecError::ContractViolation);
    }
    let resident_layout =
        validate_range_contract_and_tuple(header.contract, header.tuple, header.order_digest)
            .map_err(|_| ArtifactCodecError::Malformed)?;
    let component_bytes = PhysicalArtifact::accounted_range_resident_component_bytes_for_layout(
        encoded.len(),
        resident_layout,
    )
    .map_err(|_| ArtifactCodecError::ResourceUnavailable)?;
    let total_bytes = component_bytes
        .checked_add(std::mem::size_of::<ArtifactRetention>())
        .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
        .ok_or(ArtifactCodecError::ResourceUnavailable)?;
    let retention = Arc::new(
        ArtifactRetention::try_new(total_bytes, retained_budget, memory_account.clone())
            .map_err(|_| ArtifactCodecError::ResourceUnavailable)?,
    );
    let _temporary = TemporaryContributionLease::try_new(
        memory_account,
        resident_layout
            .decode_temporary_bytes()
            .map_err(|_| ArtifactCodecError::ResourceUnavailable)?,
    )
    .map_err(|_| ArtifactCodecError::ResourceUnavailable)?;

    let contract = Arc::new(
        decode_order_contract(header.contract, header.order_digest).map_err(range_decode_error)?,
    );
    if contract.digest() != header.order_digest {
        return Err(ArtifactCodecError::Malformed);
    }
    let bound = decode_order_tuple(&contract, header.tuple).map_err(range_decode_error)?;
    let data = RangeArtifactData::new(contract, bound, header.logical_version)
        .map_err(|_| ArtifactCodecError::Malformed)?;
    let bytes: Arc<[u8]> = Arc::from(encoded);
    let artifact = PhysicalArtifact::from_range_shared_retained(
        header.logical_version,
        data,
        bytes,
        component_bytes,
        total_bytes,
        retention,
    )
    .map_err(|_| ArtifactCodecError::ResourceUnavailable)?;
    Ok(Arc::new(artifact))
}

const fn range_decode_error(error: ArtifactCodecError) -> ArtifactCodecError {
    match error {
        ArtifactCodecError::ResourceUnavailable => ArtifactCodecError::ResourceUnavailable,
        _ => ArtifactCodecError::Malformed,
    }
}

struct ParsedRangeHeader<'a> {
    kind: ArtifactKind,
    order_digest: OrderContractDigest,
    logical_version: LogicalVersion,
    contract: &'a [u8],
    tuple: &'a [u8],
}

fn parse_range_header(encoded: &[u8]) -> Result<ParsedRangeHeader<'_>, ArtifactCodecError> {
    let mut reader = Reader::new(encoded);
    if reader.read_exact(4)? != RANGE_MAGIC || reader.read_u16()? != RANGE_CODEC_VERSION {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let kind = ArtifactKind::from_tag(reader.read_u8()?).ok_or(ArtifactCodecError::UnknownKind)?;
    let order_digest = OrderContractDigest::from_bytes_for_codec(reader.read_array::<32>()?);
    let logical_version = LogicalVersion::new(reader.read_u64()?);
    let contract_len =
        usize::try_from(reader.read_u32()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let contract = reader.read_exact(contract_len)?;
    let tuple_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    let tuple = reader.read_exact(tuple_len)?;
    if !reader.is_empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    Ok(ParsedRangeHeader {
        kind,
        order_digest,
        logical_version,
        contract,
        tuple,
    })
}

fn encoded_order_contract_len(
    contract: &RuntimeOrderContract,
) -> Result<usize, ArtifactCodecError> {
    contract
        .keys()
        .iter()
        .try_fold(4usize, |bytes, key| {
            bytes
                .checked_add(encoded_order_type_len(key.data_type())?)
                .and_then(|bytes| bytes.checked_add(2))
                .ok_or(ArtifactCodecError::ResourceUnavailable)
        })?
        .checked_add(32)
        .ok_or(ArtifactCodecError::ResourceUnavailable)
}

fn encoded_order_type_len(
    data_type: &arrow::datatypes::DataType,
) -> Result<usize, ArtifactCodecError> {
    use arrow::datatypes::DataType;
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::FixedSizeBinary(16)
        | DataType::Utf8
        | DataType::Date32 => Ok(1),
        DataType::Timestamp(_, timezone) => {
            let timezone_len = timezone
                .as_ref()
                .map_or(Some(0), |value| 4usize.checked_add(value.len()))
                .ok_or(ArtifactCodecError::ResourceUnavailable)?;
            3usize
                .checked_add(timezone_len)
                .ok_or(ArtifactCodecError::ResourceUnavailable)
        }
        DataType::Decimal128(_, _) => Ok(3),
        _ => Err(ArtifactCodecError::ContractViolation),
    }
}

fn encode_order_contract(
    contract: &RuntimeOrderContract,
    output: &mut Vec<u8>,
) -> Result<(), ArtifactCodecError> {
    output.extend_from_slice(
        &u32::try_from(contract.keys().len())
            .map_err(|_| ArtifactCodecError::ResourceUnavailable)?
            .to_be_bytes(),
    );
    for key in contract.keys() {
        encode_order_type(key.data_type(), output)?;
        output.push(match key.direction() {
            SortDirection::Ascending => 1,
            SortDirection::Descending => 2,
        });
        output.push(match key.null_order() {
            NullOrder::First => 1,
            NullOrder::Last => 2,
        });
    }
    output.extend_from_slice(&contract.plan_comparator_digest().get());
    Ok(())
}

fn encode_order_type(
    data_type: &arrow::datatypes::DataType,
    output: &mut Vec<u8>,
) -> Result<(), ArtifactCodecError> {
    use arrow::datatypes::{DataType, TimeUnit};
    match data_type {
        DataType::Boolean => output.push(1),
        DataType::Int8 => output.push(2),
        DataType::Int16 => output.push(3),
        DataType::Int32 => output.push(4),
        DataType::Int64 => output.push(5),
        DataType::FixedSizeBinary(16) => output.push(6),
        DataType::Utf8 => output.push(9),
        DataType::Date32 => output.push(10),
        DataType::Timestamp(unit, timezone) => {
            output.push(11);
            output.push(match unit {
                TimeUnit::Second => 1,
                TimeUnit::Millisecond => 2,
                TimeUnit::Microsecond => 3,
                TimeUnit::Nanosecond => 4,
            });
            match timezone {
                None => output.push(0),
                Some(value) => {
                    output.push(1);
                    output.extend_from_slice(
                        &u32::try_from(value.len())
                            .map_err(|_| ArtifactCodecError::ResourceUnavailable)?
                            .to_be_bytes(),
                    );
                    output.extend_from_slice(value.as_bytes());
                }
            }
        }
        DataType::Decimal128(precision, scale) => {
            output.extend_from_slice(&[12, *precision, *scale as u8]);
        }
        _ => return Err(ArtifactCodecError::ContractViolation),
    }
    Ok(())
}

fn encoded_order_tuple_len(
    contract: &RuntimeOrderContract,
    bound: &OrderedTuple,
) -> Result<usize, ArtifactCodecError> {
    contract
        .compare(bound, bound)
        .map_err(|_| ArtifactCodecError::ContractViolation)?;
    contract
        .keys()
        .iter()
        .zip(bound.values())
        .try_fold(4usize, |bytes, (key, value)| {
            let scalar = match value {
                None => 0,
                Some(OrderedScalar::Boolean(_) | OrderedScalar::Int8(_)) => 1,
                Some(OrderedScalar::Int16(_)) => 2,
                Some(OrderedScalar::Int32(_) | OrderedScalar::Date32(_)) => 4,
                Some(OrderedScalar::Int64(_) | OrderedScalar::Timestamp(_)) => 8,
                Some(OrderedScalar::LargeInt(_) | OrderedScalar::Decimal128(_)) => 16,
                Some(OrderedScalar::Utf8(value)) => 8usize
                    .checked_add(value.len())
                    .ok_or(ArtifactCodecError::ResourceUnavailable)?,
            };
            if value
                .as_ref()
                .is_some_and(|value| !scalar_matches_key(value, key.data_type()))
            {
                return Err(ArtifactCodecError::ContractViolation);
            }
            bytes
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(scalar))
                .ok_or(ArtifactCodecError::ResourceUnavailable)
        })
}

fn encode_order_tuple(
    contract: &RuntimeOrderContract,
    bound: &OrderedTuple,
    output: &mut Vec<u8>,
) -> Result<(), ArtifactCodecError> {
    let start = output.len();
    output.extend_from_slice(
        &u32::try_from(bound.values().len())
            .map_err(|_| ArtifactCodecError::ResourceUnavailable)?
            .to_be_bytes(),
    );
    for value in bound.values() {
        let Some(value) = value else {
            output.push(0);
            continue;
        };
        output.push(1);
        match value {
            OrderedScalar::Boolean(value) => output.push(u8::from(*value)),
            OrderedScalar::Int8(value) => output.push(*value as u8),
            OrderedScalar::Int16(value) => output.extend_from_slice(&value.to_be_bytes()),
            OrderedScalar::Int32(value) | OrderedScalar::Date32(value) => {
                output.extend_from_slice(&value.to_be_bytes())
            }
            OrderedScalar::Int64(value) | OrderedScalar::Timestamp(value) => {
                output.extend_from_slice(&value.to_be_bytes())
            }
            OrderedScalar::LargeInt(value) | OrderedScalar::Decimal128(value) => {
                output.extend_from_slice(&value.to_be_bytes())
            }
            OrderedScalar::Utf8(value) => {
                output.extend_from_slice(&(value.len() as u64).to_be_bytes());
                output.extend_from_slice(value.as_bytes());
            }
        }
    }
    debug_assert_eq!(
        output.len() - start,
        encoded_order_tuple_len(contract, bound)?
    );
    Ok(())
}

fn scalar_matches_key(value: &OrderedScalar, data_type: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;
    matches!(
        (value, data_type),
        (OrderedScalar::Boolean(_), DataType::Boolean)
            | (OrderedScalar::Int8(_), DataType::Int8)
            | (OrderedScalar::Int16(_), DataType::Int16)
            | (OrderedScalar::Int32(_), DataType::Int32)
            | (OrderedScalar::Int64(_), DataType::Int64)
            | (OrderedScalar::LargeInt(_), DataType::FixedSizeBinary(16))
            | (OrderedScalar::Utf8(_), DataType::Utf8)
            | (OrderedScalar::Date32(_), DataType::Date32)
            | (OrderedScalar::Timestamp(_), DataType::Timestamp(_, _))
            | (OrderedScalar::Decimal128(_), DataType::Decimal128(_, _))
    )
}

fn validate_range_contract_and_tuple(
    contract_bytes: &[u8],
    tuple_bytes: &[u8],
    expected_order_digest: OrderContractDigest,
) -> Result<RangeArtifactResidentLayout, ArtifactCodecError> {
    let mut contract = Reader::new(contract_bytes);
    let key_count =
        usize::try_from(contract.read_u32()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if key_count == 0 {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let mut tuple = Reader::new(tuple_bytes);
    let arity =
        usize::try_from(tuple.read_u32()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if arity != key_count {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let mut timezone_count = 0usize;
    let mut timezone_bytes = 0usize;
    let mut utf8_count = 0usize;
    let mut utf8_bytes = 0usize;
    for _ in 0..key_count {
        let (data_type, timezone_len) = scan_order_type(&mut contract)?;
        if let Some(timezone_len) = timezone_len {
            timezone_count = timezone_count
                .checked_add(1)
                .ok_or(ArtifactCodecError::LengthOverflow)?;
            timezone_bytes = timezone_bytes
                .checked_add(timezone_len)
                .ok_or(ArtifactCodecError::LengthOverflow)?;
        }
        if !matches!(contract.read_u8()?, 1 | 2) || !matches!(contract.read_u8()?, 1 | 2) {
            return Err(ArtifactCodecError::NonCanonicalPayload);
        }
        let null_flag = tuple.read_u8()?;
        if null_flag == 0 {
            continue;
        }
        if null_flag != 1 {
            return Err(ArtifactCodecError::NonCanonicalPayload);
        }
        if let Some(utf8_len) = scan_order_scalar(&mut tuple, data_type)? {
            utf8_count = utf8_count
                .checked_add(1)
                .ok_or(ArtifactCodecError::LengthOverflow)?;
            utf8_bytes = utf8_bytes
                .checked_add(utf8_len)
                .ok_or(ArtifactCodecError::LengthOverflow)?;
        }
    }
    let canonical_keys_len = contract_bytes
        .len()
        .checked_sub(contract.remaining_len())
        .ok_or(ArtifactCodecError::LengthOverflow)?;
    let comparator_digest = contract.read_array::<32>()?;
    if !contract.is_empty() || !tuple.is_empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    if RuntimeOrderContract::validate_codec_contract_digest(
        &contract_bytes[..canonical_keys_len],
        comparator_digest,
    )
    .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?
        != expected_order_digest
    {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    Ok(RangeArtifactResidentLayout {
        key_count,
        timezone_count,
        timezone_bytes,
        tuple_arity: arity,
        utf8_count,
        utf8_bytes,
    })
}

#[derive(Clone, Copy)]
enum RangeTypeView {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    LargeInt,
    Utf8,
    Date32,
    Timestamp,
    Decimal128 { precision: u8, scale: i8 },
}

fn scan_order_type<'a>(
    reader: &mut Reader<'a>,
) -> Result<(RangeTypeView, Option<usize>), ArtifactCodecError> {
    Ok(match reader.read_u8()? {
        1 => (RangeTypeView::Boolean, None),
        2 => (RangeTypeView::Int8, None),
        3 => (RangeTypeView::Int16, None),
        4 => (RangeTypeView::Int32, None),
        5 => (RangeTypeView::Int64, None),
        6 => (RangeTypeView::LargeInt, None),
        9 => (RangeTypeView::Utf8, None),
        10 => (RangeTypeView::Date32, None),
        11 => {
            if !matches!(reader.read_u8()?, 1..=4) {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
            let timezone_len = match reader.read_u8()? {
                0 => None,
                1 => {
                    let len = usize::try_from(reader.read_u32()?)
                        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
                    std::str::from_utf8(reader.read_exact(len)?)
                        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
                    Some(len)
                }
                _ => return Err(ArtifactCodecError::NonCanonicalPayload),
            };
            (RangeTypeView::Timestamp, timezone_len)
        }
        12 => {
            let precision = reader.read_u8()?;
            let scale = reader.read_u8()? as i8;
            if precision == 0
                || precision > arrow::datatypes::DECIMAL128_MAX_PRECISION
                || scale > arrow::datatypes::DECIMAL128_MAX_SCALE
                || (scale > 0 && scale as u8 > precision)
            {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
            (RangeTypeView::Decimal128 { precision, scale }, None)
        }
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    })
}

fn scan_order_scalar(
    reader: &mut Reader<'_>,
    data_type: RangeTypeView,
) -> Result<Option<usize>, ArtifactCodecError> {
    match data_type {
        RangeTypeView::Boolean => match reader.read_u8()? {
            0 | 1 => Ok(None),
            _ => Err(ArtifactCodecError::NonCanonicalPayload),
        },
        RangeTypeView::Int8 => {
            reader.read_u8()?;
            Ok(None)
        }
        RangeTypeView::Int16 => {
            reader.read_exact(2)?;
            Ok(None)
        }
        RangeTypeView::Int32 | RangeTypeView::Date32 => {
            reader.read_exact(4)?;
            Ok(None)
        }
        RangeTypeView::Int64 | RangeTypeView::Timestamp => {
            reader.read_exact(8)?;
            Ok(None)
        }
        RangeTypeView::LargeInt => {
            reader.read_exact(16)?;
            Ok(None)
        }
        RangeTypeView::Decimal128 { precision, scale } => {
            let value = reader.read_i128()?;
            let bound = 10_i128
                .checked_pow(u32::from(precision))
                .ok_or(ArtifactCodecError::NonCanonicalPayload)?;
            if value <= -bound || value >= bound {
                return Err(ArtifactCodecError::NonCanonicalPayload);
            }
            let _ = scale;
            Ok(None)
        }
        RangeTypeView::Utf8 => {
            let len = usize::try_from(reader.read_u64()?)
                .map_err(|_| ArtifactCodecError::LengthOverflow)?;
            std::str::from_utf8(reader.read_exact(len)?)
                .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
            Ok(Some(len))
        }
        _ => Err(ArtifactCodecError::NonCanonicalPayload),
    }
}

fn decode_order_contract(
    bytes: &[u8],
    order_contract_digest: OrderContractDigest,
) -> Result<RuntimeOrderContract, ArtifactCodecError> {
    let mut reader = Reader::new(bytes);
    let count =
        usize::try_from(reader.read_u32()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if count == 0 {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let mut keys = Vec::new();
    keys.try_reserve_exact(count)
        .map_err(|_| ArtifactCodecError::ResourceUnavailable)?;
    for _ in 0..count {
        let data_type = decode_order_type(&mut reader)?;
        let direction = match reader.read_u8()? {
            1 => SortDirection::Ascending,
            2 => SortDirection::Descending,
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        };
        let null_order = match reader.read_u8()? {
            1 => NullOrder::First,
            2 => NullOrder::Last,
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        };
        keys.push(RuntimeOrderKey::new(data_type, direction, null_order));
    }
    let comparator_digest = ComparatorDigest::new(reader.read_array::<32>()?);
    if !reader.is_empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    RuntimeOrderContract::from_codec(keys, comparator_digest, order_contract_digest)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)
}

fn decode_order_type(
    reader: &mut Reader<'_>,
) -> Result<arrow::datatypes::DataType, ArtifactCodecError> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match reader.read_u8()? {
        1 => DataType::Boolean,
        2 => DataType::Int8,
        3 => DataType::Int16,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::FixedSizeBinary(16),
        9 => DataType::Utf8,
        10 => DataType::Date32,
        11 => {
            let unit = match reader.read_u8()? {
                1 => TimeUnit::Second,
                2 => TimeUnit::Millisecond,
                3 => TimeUnit::Microsecond,
                4 => TimeUnit::Nanosecond,
                _ => return Err(ArtifactCodecError::NonCanonicalPayload),
            };
            let timezone = match reader.read_u8()? {
                0 => None,
                1 => {
                    let len = usize::try_from(reader.read_u32()?)
                        .map_err(|_| ArtifactCodecError::LengthOverflow)?;
                    Some(
                        std::str::from_utf8(reader.read_exact(len)?)
                            .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?
                            .into(),
                    )
                }
                _ => return Err(ArtifactCodecError::NonCanonicalPayload),
            };
            DataType::Timestamp(unit, timezone)
        }
        12 => DataType::Decimal128(reader.read_u8()?, reader.read_u8()? as i8),
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    })
}

fn decode_order_tuple(
    contract: &RuntimeOrderContract,
    bytes: &[u8],
) -> Result<OrderedTuple, ArtifactCodecError> {
    let mut reader = Reader::new(bytes);
    let count =
        usize::try_from(reader.read_u32()?).map_err(|_| ArtifactCodecError::LengthOverflow)?;
    if count != contract.keys().len() {
        return Err(ArtifactCodecError::NonCanonicalPayload);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| ArtifactCodecError::ResourceUnavailable)?;
    for key in contract.keys() {
        values.push(match reader.read_u8()? {
            0 => None,
            1 => Some(decode_order_scalar(&mut reader, key.data_type())?),
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        });
    }
    if !reader.is_empty() {
        return Err(ArtifactCodecError::TrailingBytes);
    }
    OrderedTuple::try_from_codec(contract, values)
        .map_err(|_| ArtifactCodecError::NonCanonicalPayload)
}

fn decode_order_scalar(
    reader: &mut Reader<'_>,
    data_type: &arrow::datatypes::DataType,
) -> Result<OrderedScalar, ArtifactCodecError> {
    use arrow::datatypes::DataType;
    Ok(match data_type {
        DataType::Boolean => OrderedScalar::Boolean(match reader.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(ArtifactCodecError::NonCanonicalPayload),
        }),
        DataType::Int8 => OrderedScalar::Int8(reader.read_u8()? as i8),
        DataType::Int16 => OrderedScalar::Int16(reader.read_i16()?),
        DataType::Int32 => OrderedScalar::Int32(reader.read_i32()?),
        DataType::Int64 => OrderedScalar::Int64(reader.read_i64()?),
        DataType::FixedSizeBinary(16) => OrderedScalar::LargeInt(reader.read_i128()?),
        DataType::Utf8 => {
            let len = usize::try_from(reader.read_u64()?)
                .map_err(|_| ArtifactCodecError::LengthOverflow)?;
            let value = std::str::from_utf8(reader.read_exact(len)?)
                .map_err(|_| ArtifactCodecError::NonCanonicalPayload)?;
            OrderedScalar::Utf8(value.into())
        }
        DataType::Date32 => OrderedScalar::Date32(reader.read_i32()?),
        DataType::Timestamp(_, _) => OrderedScalar::Timestamp(reader.read_i64()?),
        DataType::Decimal128(_, _) => OrderedScalar::Decimal128(reader.read_i128()?),
        _ => return Err(ArtifactCodecError::NonCanonicalPayload),
    })
}

struct Reader<'a> {
    remaining: &'a [u8],
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }
    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
    const fn remaining_len(&self) -> usize {
        self.remaining.len()
    }
    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ArtifactCodecError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(len)
            .ok_or(ArtifactCodecError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ArtifactCodecError> {
        Ok(self.read_exact(N)?.try_into().expect("exact array length"))
    }
    fn read_u8(&mut self) -> Result<u8, ArtifactCodecError> {
        Ok(self.read_exact(1)?[0])
    }
    fn read_u16(&mut self) -> Result<u16, ArtifactCodecError> {
        Ok(u16::from_be_bytes(self.read_exact(2)?.try_into().unwrap()))
    }
    fn read_u32(&mut self) -> Result<u32, ArtifactCodecError> {
        Ok(u32::from_be_bytes(self.read_exact(4)?.try_into().unwrap()))
    }
    fn read_u64(&mut self) -> Result<u64, ArtifactCodecError> {
        Ok(u64::from_be_bytes(self.read_exact(8)?.try_into().unwrap()))
    }
    fn read_i16(&mut self) -> Result<i16, ArtifactCodecError> {
        Ok(i16::from_be_bytes(self.read_exact(2)?.try_into().unwrap()))
    }
    fn read_i32(&mut self) -> Result<i32, ArtifactCodecError> {
        Ok(i32::from_be_bytes(self.read_exact(4)?.try_into().unwrap()))
    }
    fn read_i64(&mut self) -> Result<i64, ArtifactCodecError> {
        Ok(i64::from_be_bytes(self.read_exact(8)?.try_into().unwrap()))
    }
    fn read_i128(&mut self) -> Result<i128, ArtifactCodecError> {
        Ok(i128::from_be_bytes(
            self.read_exact(16)?.try_into().unwrap(),
        ))
    }
}

#[cfg(test)]
fn decode_leaf_unretained_for_test(
    encoded: &[u8],
    data_type: &arrow::datatypes::DataType,
    null_semantics: NullSemantics,
    logical_version: LogicalVersion,
) -> Result<Arc<PhysicalArtifact>, ArtifactCodecError> {
    struct Unlimited;
    impl RuntimeFilterMemoryAccount for Unlimited {
        fn try_consume(
            &self,
            _bytes: usize,
        ) -> Result<(), crate::runtime_filter::port::support::MemoryAccountError> {
            Ok(())
        }
        fn release(&self, _bytes: usize) {}
    }
    let header = parse_header(encoded)?;
    let index_heap_bytes = if matches!(
        header.kind,
        ArtifactKind::ValueSet | ArtifactKind::EmptyDomain
    ) {
        inspect_membership_index(encoded)?.heap_bytes()?
    } else {
        0
    };
    let retained_bytes = if matches!(
        header.kind,
        ArtifactKind::ValueSet | ArtifactKind::EmptyDomain
    ) {
        PhysicalArtifact::accounted_indexed_resident_bytes(encoded.len(), index_heap_bytes)
    } else {
        PhysicalArtifact::accounted_resident_bytes(encoded.len())
    }
    .map_err(|_| ArtifactCodecError::LengthOverflow)?;
    decode_leaf(
        encoded,
        ArtifactDecodeExpectations {
            expected_kind: header.kind,
            expected_schema_digest: ArtifactSchemaDigest::for_membership(data_type, null_semantics)
                .map_err(|_| ArtifactCodecError::SchemaMismatch)?,
            expected_logical_version: logical_version,
            expected_hash_contract: None,
        },
        encoded.len(),
        Arc::new(ArtifactRetainedBudget::new(retained_bytes)),
        Arc::new(Unlimited),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::NullSemantics;
    use crate::runtime_filter::model::contract::{
        ChannelId, NullOrder, OrderContract, OrderKeyContract, SortDirection,
    };
    use crate::runtime_filter::port::artifact::{
        ArtifactKind, ArtifactMembershipSchema, ArtifactSchemaDigest, HashContractDigest,
        PhysicalArtifact, ResidentMembershipIndexView,
    };
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::install::MaterializationPolicy;
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, ComparatorDigestV1, OrderedScalar, OrderedTuple,
        RuntimeOrderContract,
    };
    use crate::runtime_filter::port::support::RetainedMemoryReservation;
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactRetention, MemoryAccountError, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::OrderedBoundDomain;
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain,
    };

    use super::super::bloom::{BloomHashContract, build_bits};
    use super::{
        ArtifactCodecError, ArtifactDecodeExpectations, MembershipProbe, RangeDecodeExpectations,
        decode_leaf, decode_leaf_unretained_for_test, decode_range, encode_membership_leaf,
        encode_physical_leaf, encode_range_leaf, indexed_membership_contains,
        indexed_membership_contains_counted_for_test, inspect_membership_index,
    };

    fn range_codec_fixture() -> (
        Arc<LogicalSnapshot>,
        crate::runtime_filter::port::artifact::ConsumerArtifactProfile,
    ) {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let comparator =
            ComparatorDigestV1::for_contract(&keys, COMPARATOR_ALGORITHM_VERSION).unwrap();
        let contract = Arc::new(
            RuntimeOrderContract::try_from_plan(&OrderContract {
                keys,
                inclusive: true,
                comparator_digest: comparator,
            })
            .unwrap(),
        );
        let tuple = OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(11))]).unwrap();
        (
            Arc::new(LogicalSnapshot::ordered(
                ChannelId::new(9),
                LogicalVersion::new(5),
                Arc::new(OrderedBoundDomain::new(contract.clone(), tuple)),
                RetainedMemoryReservation::empty(),
            )),
            crate::runtime_filter::port::artifact::ConsumerArtifactProfile::new_ordered_range(
                contract.digest(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn range_codec_hop_preserves_contract_version_bound_and_semantic_digest() {
        let (snapshot, profile) = range_codec_fixture();
        let direct = crate::runtime_filter::materializer::range::RangeMaterializer::materialize(
            snapshot.clone(),
            &profile,
            usize::MAX,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(
                crate::runtime_filter::port::support::ArtifactScratchBudget::new(1 << 20, 1 << 20)
                    .unwrap(),
            ),
            Arc::new(CountingAccount::default()),
        );
        let crate::runtime_filter::materializer::range::RangeMaterializationOutcome::Published(
            bundle,
        ) = direct
        else {
            panic!("Range materialization must publish")
        };
        let direct = bundle.artifacts()[0].1.clone();
        let decoded = decode_range(
            direct.canonical_bytes(),
            RangeDecodeExpectations {
                expected_order_digest: snapshot.ordered_bound().unwrap().contract().digest(),
                expected_logical_version: snapshot.version(),
            },
            direct.canonical_bytes().len(),
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(CountingAccount::default()),
        )
        .unwrap();
        assert_eq!(
            direct.range().unwrap().semantic_digest(),
            decoded.range().unwrap().semantic_digest()
        );
        assert_eq!(
            direct.range().unwrap().bound(),
            decoded.range().unwrap().bound()
        );
        assert_eq!(decoded.version(), LogicalVersion::new(5));
    }

    #[test]
    fn range_codec_round_trips_every_supported_ordered_scalar_contract() {
        use arrow::datatypes::TimeUnit;

        let typed_values = vec![
            (DataType::Boolean, Some(OrderedScalar::Boolean(true))),
            (DataType::Int8, Some(OrderedScalar::Int8(-8))),
            (DataType::Int16, Some(OrderedScalar::Int16(-16))),
            (DataType::Int32, Some(OrderedScalar::Int32(-32))),
            (DataType::Int64, Some(OrderedScalar::Int64(-64))),
            (
                DataType::FixedSizeBinary(16),
                Some(OrderedScalar::LargeInt(-128)),
            ),
            (
                DataType::Utf8,
                Some(OrderedScalar::Utf8("ordered-bytes".into())),
            ),
            (DataType::Date32, Some(OrderedScalar::Date32(20_000))),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into())),
                Some(OrderedScalar::Timestamp(1_234_567_890)),
            ),
            (
                DataType::Decimal128(18, 3),
                Some(OrderedScalar::Decimal128(123_456)),
            ),
        ];
        let keys = typed_values
            .iter()
            .map(|(data_type, _)| OrderKeyContract {
                data_type: data_type.clone(),
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            })
            .collect::<Vec<_>>();
        let comparator =
            ComparatorDigestV1::for_contract(&keys, COMPARATOR_ALGORITHM_VERSION).unwrap();
        let contract = Arc::new(
            RuntimeOrderContract::try_from_plan(&OrderContract {
                keys,
                inclusive: true,
                comparator_digest: comparator,
            })
            .unwrap(),
        );
        let bound =
            OrderedTuple::try_new(&contract, typed_values.into_iter().map(|(_, value)| value))
                .unwrap();
        let encoded = encode_range_leaf(&contract, &bound, LogicalVersion::new(8)).unwrap();
        let header = super::parse_range_header(&encoded).unwrap();
        let layout = super::validate_range_contract_and_tuple(
            header.contract,
            header.tuple,
            contract.digest(),
        )
        .unwrap();
        let exact_bytes = PhysicalArtifact::accounted_range_resident_component_bytes_for_layout(
            encoded.len(),
            layout,
        )
        .unwrap()
            + std::mem::size_of::<ArtifactRetention>()
            + 2 * std::mem::size_of::<usize>();
        let exact_budget = Arc::new(ArtifactRetainedBudget::new(exact_bytes));
        let exact_account = Arc::new(CountingAccount::default());
        let decoded = decode_range(
            &encoded,
            RangeDecodeExpectations {
                expected_order_digest: contract.digest(),
                expected_logical_version: LogicalVersion::new(8),
            },
            encoded.len(),
            exact_budget.clone(),
            exact_account.clone(),
        )
        .unwrap();

        assert_eq!(
            decoded.range().unwrap().contract().as_ref(),
            contract.as_ref()
        );
        assert_eq!(decoded.range().unwrap().bound(), &bound);
        assert_eq!(exact_budget.retained_bytes(), exact_bytes);
        drop(decoded);
        assert_eq!(exact_budget.retained_bytes(), 0);
        assert_eq!(exact_account.retained.load(Ordering::SeqCst), 0);

        let one_under = Arc::new(ArtifactRetainedBudget::new(exact_bytes - 1));
        let one_under_account = Arc::new(CountingAccount::default());
        assert_eq!(
            decode_range(
                &encoded,
                RangeDecodeExpectations {
                    expected_order_digest: contract.digest(),
                    expected_logical_version: LogicalVersion::new(8),
                },
                encoded.len(),
                one_under.clone(),
                one_under_account.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::ResourceUnavailable
        );
        assert_eq!(one_under.retained_bytes(), 0);
        assert_eq!(one_under_account.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn range_codec_classifies_expected_mismatch_malformed_and_resource_failures() {
        let (snapshot, profile) = range_codec_fixture();
        let direct = crate::runtime_filter::materializer::range::RangeMaterializer::materialize(
            snapshot.clone(),
            &profile,
            usize::MAX,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(
                crate::runtime_filter::port::support::ArtifactScratchBudget::new(1 << 20, 1 << 20)
                    .unwrap(),
            ),
            Arc::new(CountingAccount::default()),
        );
        let crate::runtime_filter::materializer::range::RangeMaterializationOutcome::Published(
            bundle,
        ) = direct
        else {
            panic!("Range materialization must publish")
        };
        let encoded = bundle.artifacts()[0].1.canonical_bytes();
        let expectations = RangeDecodeExpectations {
            expected_order_digest: snapshot.ordered_bound().unwrap().contract().digest(),
            expected_logical_version: snapshot.version(),
        };
        let failure_budget = Arc::new(ArtifactRetainedBudget::new(1 << 20));
        let failure_account = Arc::new(CountingAccount::default());
        let decode = |bytes: &[u8], expected: RangeDecodeExpectations, max: usize| {
            decode_range(
                bytes,
                expected,
                max,
                failure_budget.clone(),
                failure_account.clone(),
            )
        };

        let mut wrong = expectations;
        wrong.expected_logical_version = LogicalVersion::new(99);
        assert_eq!(
            decode(encoded, wrong, encoded.len()).unwrap_err(),
            ArtifactCodecError::ContractViolation
        );
        let mut wrong = expectations;
        wrong.expected_order_digest =
            crate::runtime_filter::port::ordered_bound::OrderContractDigest::from_bytes_for_codec(
                [99; 32],
            );
        assert_eq!(
            decode(encoded, wrong, encoded.len()).unwrap_err(),
            ArtifactCodecError::ContractViolation
        );
        let mut wrong_kind = encoded.to_vec();
        wrong_kind[6] = ArtifactKind::ValueSet.tag();
        assert_eq!(
            decode(&wrong_kind, expectations, wrong_kind.len()).unwrap_err(),
            ArtifactCodecError::ContractViolation
        );
        assert_eq!(
            decode(&encoded[..encoded.len() - 1], expectations, encoded.len()).unwrap_err(),
            ArtifactCodecError::Malformed
        );
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert_eq!(
            decode(&trailing, expectations, trailing.len()).unwrap_err(),
            ArtifactCodecError::Malformed
        );
        assert_eq!(
            decode(encoded, expectations, encoded.len() - 1).unwrap_err(),
            ArtifactCodecError::ResourceUnavailable
        );

        for malformed in [
            {
                let mut bytes = encoded.to_vec();
                bytes[0] ^= 0xff;
                bytes
            },
            {
                let mut bytes = encoded.to_vec();
                bytes[4..6].copy_from_slice(&99_u16.to_be_bytes());
                bytes
            },
            {
                let mut bytes = encoded.to_vec();
                bytes[6] = 99;
                bytes
            },
            {
                let mut bytes = encoded.to_vec();
                bytes[47..51].copy_from_slice(&u32::MAX.to_be_bytes());
                bytes
            },
        ] {
            assert_eq!(
                decode(&malformed, expectations, malformed.len()).unwrap_err(),
                ArtifactCodecError::Malformed
            );
        }

        let contract_len = u32::from_be_bytes(encoded[47..51].try_into().unwrap()) as usize;
        let contract_start = 51;
        let tuple_len_offset = contract_start + contract_len;
        let tuple_start = tuple_len_offset + 8;
        for malformed in [
            {
                let mut bytes = encoded.to_vec();
                bytes[contract_start + 4] = 99;
                bytes
            },
            {
                let mut bytes = encoded.to_vec();
                bytes[tuple_start..tuple_start + 4].copy_from_slice(&2_u32.to_be_bytes());
                bytes
            },
            {
                let mut bytes = encoded.to_vec();
                bytes[tuple_start + 4] = 2;
                bytes
            },
            {
                let mut bytes = encoded.to_vec();
                bytes[tuple_len_offset..tuple_len_offset + 8]
                    .copy_from_slice(&u64::MAX.to_be_bytes());
                bytes
            },
        ] {
            assert_eq!(
                decode(&malformed, expectations, malformed.len()).unwrap_err(),
                ArtifactCodecError::Malformed
            );
        }

        let budget = Arc::new(ArtifactRetainedBudget::new(0));
        let account = Arc::new(CountingAccount::default());
        assert_eq!(
            decode_range(
                encoded,
                expectations,
                encoded.len(),
                budget.clone(),
                account.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::ResourceUnavailable
        );
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(account.retained.load(Ordering::SeqCst), 0);

        let budget = Arc::new(ArtifactRetainedBudget::new(1 << 20));
        let rejecting = Arc::new(CountingAccount {
            retained: AtomicUsize::new(0),
            reject: true,
        });
        assert_eq!(
            decode_range(
                encoded,
                expectations,
                encoded.len(),
                budget.clone(),
                rejecting.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::ResourceUnavailable
        );
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(rejecting.retained.load(Ordering::SeqCst), 0);
        assert_eq!(failure_budget.retained_bytes(), 0);
        assert_eq!(failure_account.retained.load(Ordering::SeqCst), 0);
    }

    #[derive(Default)]
    struct CountingAccount {
        retained: AtomicUsize,
        reject: bool,
    }

    impl RuntimeFilterMemoryAccount for CountingAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            if self.reject {
                return Err(MemoryAccountError::CapacityExceeded);
            }
            self.retained.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.retained.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    fn int64_leaf(values: impl IntoIterator<Item = i64>, contains_null: bool) -> Vec<u8> {
        encode_membership_leaf(
            &ReducedMembershipDomain::new(MembershipValues::int64(values), contains_null),
            if contains_null {
                NullSemantics::NullSafeEqual
            } else {
                NullSemantics::NeverMatches
            },
            LogicalVersion::FIRST,
        )
        .unwrap()
    }

    fn int64_expectations(contains_null: bool) -> ArtifactDecodeExpectations {
        ArtifactDecodeExpectations {
            expected_kind: ArtifactKind::ValueSet,
            expected_schema_digest: ArtifactSchemaDigest::for_membership(
                &DataType::Int64,
                if contains_null {
                    NullSemantics::NullSafeEqual
                } else {
                    NullSemantics::NeverMatches
                },
            )
            .unwrap(),
            expected_logical_version: LogicalVersion::FIRST,
            expected_hash_contract: None,
        }
    }

    #[test]
    fn null_only_membership_is_not_empty_domain() {
        let domain = ReducedMembershipDomain::new(MembershipValues::int64([]), true);
        let encoded =
            encode_membership_leaf(&domain, NullSemantics::NullSafeEqual, LogicalVersion::FIRST)
                .unwrap();
        let decoded = decode_leaf_unretained_for_test(
            &encoded,
            &DataType::Int64,
            NullSemantics::NullSafeEqual,
            LogicalVersion::FIRST,
        )
        .unwrap();

        assert_eq!(decoded.kind(), ArtifactKind::ValueSet);
        assert!(decoded.contains_null());
    }

    #[test]
    fn encoder_rejects_nulls_under_never_matches_semantics() {
        let domain = ReducedMembershipDomain::new(MembershipValues::int64([1]), true);

        assert_eq!(
            encode_membership_leaf(&domain, NullSemantics::NeverMatches, LogicalVersion::FIRST,)
                .unwrap_err(),
            ArtifactCodecError::NonCanonicalPayload
        );
    }

    #[test]
    fn value_set_round_trips_every_m1_membership_type() {
        let cases = vec![
            MembershipValues::boolean([false, true]),
            MembershipValues::int8([-1, 2]),
            MembershipValues::int16([-2, 3]),
            MembershipValues::int32([-3, 4]),
            MembershipValues::int64([-4, 5]),
            MembershipValues::large_int([-5, 6]),
            MembershipValues::float32([f32::NEG_INFINITY, -0.0, 0.0, f32::INFINITY, f32::NAN]),
            MembershipValues::float64([f64::NEG_INFINITY, -0.0, 0.0, f64::INFINITY, f64::NAN]),
            MembershipValues::utf8(["a", "z"]),
            MembershipValues::date32([-7, 8]),
            MembershipValues::timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("Asia/Shanghai".into()),
                [-9, 10],
            ),
            MembershipValues::decimal128(18, 3, [-11, 12]).unwrap(),
        ];
        for values in cases {
            let data_type = values.data_type();
            let domain = ReducedMembershipDomain::new(values, false);
            let encoded =
                encode_membership_leaf(&domain, NullSemantics::NeverMatches, LogicalVersion::FIRST)
                    .unwrap();
            let decoded = decode_leaf_unretained_for_test(
                &encoded,
                &data_type,
                NullSemantics::NeverMatches,
                LogicalVersion::FIRST,
            )
            .unwrap();
            assert_eq!(decoded.kind(), ArtifactKind::ValueSet);
            assert_eq!(decoded.canonical_bytes(), encoded);
        }
    }

    #[test]
    fn membership_index_heap_is_exactly_accounted_before_build() {
        let fixed = int64_leaf([1, 2, 3], false);
        assert_eq!(
            inspect_membership_index(&fixed)
                .unwrap()
                .heap_bytes()
                .unwrap(),
            0
        );

        let values = MembershipValues::utf8(["a", "bb", "ccc"]);
        let data_type = values.data_type();
        let encoded = encode_membership_leaf(
            &ReducedMembershipDomain::new(values, false),
            NullSemantics::NeverMatches,
            LogicalVersion::FIRST,
        )
        .unwrap();
        let heap_bytes = inspect_membership_index(&encoded)
            .unwrap()
            .heap_bytes()
            .unwrap();
        assert_eq!(heap_bytes, 3 * std::mem::size_of::<usize>());
        let footprint =
            PhysicalArtifact::accounted_indexed_resident_bytes(encoded.len(), heap_bytes).unwrap();
        let expectations = ArtifactDecodeExpectations {
            expected_kind: ArtifactKind::ValueSet,
            expected_schema_digest: ArtifactSchemaDigest::for_membership(
                &data_type,
                NullSemantics::NeverMatches,
            )
            .unwrap(),
            expected_logical_version: LogicalVersion::FIRST,
            expected_hash_contract: None,
        };
        let account = Arc::new(CountingAccount::default());
        let too_small = Arc::new(ArtifactRetainedBudget::new(footprint - 1));
        assert_eq!(
            decode_leaf(
                &encoded,
                expectations,
                encoded.len(),
                too_small.clone(),
                account.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::ResourceLimit
        );
        assert_eq!(too_small.retained_bytes(), 0);
        assert_eq!(account.retained.load(Ordering::SeqCst), 0);

        let exact = Arc::new(ArtifactRetainedBudget::new(footprint));
        let artifact = decode_leaf(
            &encoded,
            expectations,
            encoded.len(),
            exact.clone(),
            account.clone(),
        )
        .unwrap();
        assert_eq!(artifact.retained_memory_bytes(), footprint);
        assert_eq!(
            artifact.membership_index().unwrap().heap_bytes().unwrap(),
            heap_bytes
        );
        drop(artifact);
        assert_eq!(exact.retained_bytes(), 0);
        assert_eq!(account.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn malformed_resident_index_never_panics_or_swallows_lookup_errors() {
        let fixed_bytes = int64_leaf([1, 2], false);
        let fixed_plan = inspect_membership_index(&fixed_bytes).unwrap();
        let fixed = super::build_membership_index(&fixed_bytes, &fixed_plan).unwrap();
        assert_eq!(
            indexed_membership_contains(
                &fixed_bytes[..fixed_bytes.len() - 1],
                &fixed,
                MembershipProbe::Int64(1),
            ),
            Err(ArtifactCodecError::Truncated)
        );

        let values = MembershipValues::utf8(["x"]);
        let mut utf8_bytes = encode_membership_leaf(
            &ReducedMembershipDomain::new(values, false),
            NullSemantics::NeverMatches,
            LogicalVersion::FIRST,
        )
        .unwrap();
        let utf8_plan = inspect_membership_index(&utf8_bytes).unwrap();
        let utf8 = super::build_membership_index(&utf8_bytes, &utf8_plan).unwrap();
        let ResidentMembershipIndexView::Utf8 { length_offsets, .. } = utf8.view() else {
            panic!("Utf8 leaf must build an Utf8 index");
        };
        let offset = length_offsets[0];
        utf8_bytes[offset..offset + 8].copy_from_slice(&u64::MAX.to_be_bytes());
        assert!(matches!(
            indexed_membership_contains(&utf8_bytes, &utf8, MembershipProbe::Utf8("x")),
            Err(ArtifactCodecError::LengthOverflow | ArtifactCodecError::Truncated)
        ));
    }

    #[test]
    fn fixed_membership_probe_is_logarithmic_at_4096_values() {
        let encoded = int64_leaf(0..4096, false);
        let plan = inspect_membership_index(&encoded).unwrap();
        let index = super::build_membership_index(&encoded, &plan).unwrap();
        for (needle, expected) in [(0, true), (2048, true), (4095, true), (8192, false)] {
            let (found, comparisons) = indexed_membership_contains_counted_for_test(
                &encoded,
                &index,
                MembershipProbe::Int64(needle),
            )
            .unwrap();
            assert_eq!(found, expected);
            assert!(comparisons <= 13, "4096 values need at most 13 comparisons");
        }
    }

    #[test]
    fn true_empty_domain_has_distinct_strict_encoding() {
        let encoded = int64_leaf([], false);
        let decoded = decode_leaf_unretained_for_test(
            &encoded,
            &DataType::Int64,
            NullSemantics::NeverMatches,
            LogicalVersion::FIRST,
        )
        .unwrap();
        assert_eq!(decoded.kind(), ArtifactKind::EmptyDomain);
        assert!(!decoded.contains_null());
    }

    #[test]
    fn decode_requires_typed_kind_schema_version_and_hash_expectations() {
        let encoded = int64_leaf([1, 2], false);
        let budget = Arc::new(ArtifactRetainedBudget::new(
            PhysicalArtifact::accounted_resident_bytes(encoded.len()).unwrap() * 4,
        ));
        let account = Arc::new(CountingAccount::default());
        let baseline = int64_expectations(false);

        let mut wrong = baseline;
        wrong.expected_kind = ArtifactKind::EmptyDomain;
        assert_eq!(
            decode_leaf(
                &encoded,
                wrong,
                encoded.len(),
                budget.clone(),
                account.clone()
            )
            .unwrap_err(),
            ArtifactCodecError::KindMismatch
        );
        let mut wrong = baseline;
        wrong.expected_schema_digest =
            ArtifactSchemaDigest::for_membership(&DataType::Utf8, NullSemantics::NeverMatches)
                .unwrap();
        assert_eq!(
            decode_leaf(
                &encoded,
                wrong,
                encoded.len(),
                budget.clone(),
                account.clone()
            )
            .unwrap_err(),
            ArtifactCodecError::SchemaMismatch
        );
        let mut wrong = baseline;
        wrong.expected_logical_version = LogicalVersion::new(99);
        assert_eq!(
            decode_leaf(
                &encoded,
                wrong,
                encoded.len(),
                budget.clone(),
                account.clone()
            )
            .unwrap_err(),
            ArtifactCodecError::VersionMismatch
        );
        let mut wrong = baseline;
        wrong.expected_hash_contract = Some(HashContractDigest::new([7; 32]));
        assert_eq!(
            decode_leaf(
                &encoded,
                wrong,
                encoded.len(),
                budget.clone(),
                account.clone()
            )
            .unwrap_err(),
            ArtifactCodecError::HashContractMismatch
        );
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(account.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn decode_budget_and_memory_failures_roll_back_every_reservation() {
        let encoded = int64_leaf([1, 2], false);
        let expectations = int64_expectations(false);
        let account = Arc::new(CountingAccount::default());
        let footprint = PhysicalArtifact::accounted_resident_bytes(encoded.len()).unwrap();
        let budget = Arc::new(ArtifactRetainedBudget::new(footprint));
        assert_eq!(
            decode_leaf(
                &encoded,
                expectations,
                encoded.len() - 1,
                budget.clone(),
                account.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::EncodedSizeExceeded
        );
        let first = decode_leaf(
            &encoded,
            expectations,
            encoded.len(),
            budget.clone(),
            account.clone(),
        )
        .unwrap();
        assert_eq!(budget.retained_bytes(), footprint);
        assert_eq!(account.retained.load(Ordering::SeqCst), footprint);
        assert_eq!(
            decode_leaf(
                &encoded,
                expectations,
                encoded.len(),
                budget.clone(),
                account.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::ResourceLimit
        );
        drop(first);
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(account.retained.load(Ordering::SeqCst), 0);

        let rejecting = Arc::new(CountingAccount {
            retained: AtomicUsize::new(0),
            reject: true,
        });
        assert_eq!(
            decode_leaf(
                &encoded,
                expectations,
                encoded.len(),
                budget.clone(),
                rejecting.clone(),
            )
            .unwrap_err(),
            ArtifactCodecError::ResourceLimit
        );
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(rejecting.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn decode_rejects_truncated_trailing_unknown_and_noncanonical_values() {
        let encoded = int64_leaf([1, 2], false);
        let expectations = int64_expectations(false);
        let decode = |bytes: &[u8]| {
            decode_leaf(
                bytes,
                expectations,
                bytes.len() + 1,
                Arc::new(ArtifactRetainedBudget::new(bytes.len() + 1)),
                Arc::new(CountingAccount::default()),
            )
        };
        assert_eq!(
            decode(&encoded[..encoded.len() - 1]).unwrap_err(),
            ArtifactCodecError::Truncated
        );
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(
            decode(&trailing).unwrap_err(),
            ArtifactCodecError::TrailingBytes
        );
        let mut unknown = encoded.clone();
        unknown[4..6].copy_from_slice(&99_u16.to_be_bytes());
        assert_eq!(
            decode(&unknown).unwrap_err(),
            ArtifactCodecError::UnknownVersion
        );

        let schema_len = u16::from_be_bytes(encoded[39..41].try_into().unwrap()) as usize;
        let mut duplicate = encoded;
        let payload_start = 4 + 2 + 1 + 32 + 2 + schema_len + 8 + 1 + 1 + 8;
        let first_value = payload_start + 1 + 8;
        let second_value = first_value + 8;
        let duplicate_bytes: [u8; 8] = duplicate[first_value..first_value + 8].try_into().unwrap();
        duplicate[second_value..second_value + 8].copy_from_slice(&duplicate_bytes);
        assert_eq!(
            decode(&duplicate).unwrap_err(),
            ArtifactCodecError::NonCanonicalPayload
        );
    }

    #[test]
    fn decode_rejects_payload_type_spliced_under_an_expected_schema_digest() {
        let int64 = int64_leaf([1], false);
        let mut utf8 = encode_membership_leaf(
            &ReducedMembershipDomain::new(MembershipValues::utf8(["x"]), false),
            NullSemantics::NeverMatches,
            LogicalVersion::FIRST,
        )
        .unwrap();
        let schema_digest_offset = 4 + 2 + 1;
        utf8[schema_digest_offset..schema_digest_offset + 32]
            .copy_from_slice(&int64[schema_digest_offset..schema_digest_offset + 32]);

        assert!(
            decode_leaf(
                &utf8,
                int64_expectations(false),
                utf8.len(),
                Arc::new(ArtifactRetainedBudget::new(utf8.len())),
                Arc::new(CountingAccount::default()),
            )
            .is_err()
        );
    }

    fn decode_test_leaf(
        encoded: &[u8],
        kind: ArtifactKind,
        schema: &ArtifactMembershipSchema,
        hash_contract: Option<HashContractDigest>,
    ) -> Result<Arc<PhysicalArtifact>, ArtifactCodecError> {
        decode_leaf(
            encoded,
            ArtifactDecodeExpectations {
                expected_kind: kind,
                expected_schema_digest: schema.digest(),
                expected_logical_version: LogicalVersion::FIRST,
                expected_hash_contract: hash_contract,
            },
            encoded.len(),
            Arc::new(ArtifactRetainedBudget::new(
                PhysicalArtifact::accounted_resident_bytes(encoded.len()).unwrap(),
            )),
            Arc::new(CountingAccount::default()),
        )
    }

    #[test]
    fn bitset_decoder_rejects_span_padding_endpoint_and_schema_violations() {
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let payload = |min: i64, max: i64, bit_count: u64, bits: &[u8]| {
            let mut payload = vec![5];
            payload.extend_from_slice(&min.to_be_bytes());
            payload.extend_from_slice(&max.to_be_bytes());
            payload.extend_from_slice(&bit_count.to_be_bytes());
            payload.extend_from_slice(bits);
            payload
        };
        let encode = |payload: &[u8]| {
            encode_physical_leaf(
                ArtifactKind::Bitset,
                &schema,
                LogicalVersion::FIRST,
                false,
                None,
                payload,
            )
            .unwrap()
        };
        let valid = encode(&payload(5, 7, 3, &[0b0000_0101]));
        assert!(decode_test_leaf(&valid, ArtifactKind::Bitset, &schema, None).is_ok());
        for malformed in [
            payload(7, 5, 3, &[0b0000_0101]),
            payload(5, 7, 2, &[0b0000_0011]),
            payload(5, 7, 3, &[0b1000_0101]),
            payload(5, 7, 3, &[0b0000_0100]),
            payload(5, 7, 3, &[0b0000_0001]),
            payload(i64::MIN, i64::MAX, u64::MAX, &[1]),
        ] {
            let encoded = encode(&malformed);
            assert!(decode_test_leaf(&encoded, ArtifactKind::Bitset, &schema, None).is_err());
        }

        let boolean =
            ArtifactMembershipSchema::new(&DataType::Boolean, NullSemantics::NeverMatches).unwrap();
        let mut boolean_payload = vec![1];
        boolean_payload.extend_from_slice(&0_i64.to_be_bytes());
        boolean_payload.extend_from_slice(&2_i64.to_be_bytes());
        boolean_payload.extend_from_slice(&3_u64.to_be_bytes());
        boolean_payload.push(0b0000_0101);
        let encoded = encode_physical_leaf(
            ArtifactKind::Bitset,
            &boolean,
            LogicalVersion::FIRST,
            false,
            None,
            &boolean_payload,
        )
        .unwrap();
        assert!(decode_test_leaf(&encoded, ArtifactKind::Bitset, &boolean, None).is_err());

        let decimal = ArtifactMembershipSchema::new(
            &DataType::Decimal128(19, 0),
            NullSemantics::NeverMatches,
        )
        .unwrap();
        let mut decimal_payload = vec![12];
        decimal_payload.extend_from_slice(&0_i64.to_be_bytes());
        decimal_payload.extend_from_slice(&0_i64.to_be_bytes());
        decimal_payload.extend_from_slice(&1_u64.to_be_bytes());
        decimal_payload.push(1);
        let encoded = encode_physical_leaf(
            ArtifactKind::Bitset,
            &decimal,
            LogicalVersion::FIRST,
            false,
            None,
            &decimal_payload,
        )
        .unwrap();
        assert!(decode_test_leaf(&encoded, ArtifactKind::Bitset, &decimal, None).is_err());
    }

    #[test]
    fn bitset_decoder_rejects_endpoints_outside_lossless_schema_range() {
        let malformed = [
            (
                DataType::Int8,
                2,
                i64::from(i8::MAX),
                i64::from(i8::MAX) + 1,
            ),
            (
                DataType::Int16,
                3,
                i64::from(i16::MAX),
                i64::from(i16::MAX) + 1,
            ),
            (
                DataType::Int32,
                4,
                i64::from(i32::MAX),
                i64::from(i32::MAX) + 1,
            ),
            (
                DataType::Date32,
                10,
                i64::from(i32::MAX),
                i64::from(i32::MAX) + 1,
            ),
            (DataType::Decimal128(2, 0), 12, 99, 100),
            (DataType::Decimal128(2, 0), 12, -101, -99),
        ];
        for (data_type, type_tag, min, max) in malformed {
            let schema =
                ArtifactMembershipSchema::new(&data_type, NullSemantics::NeverMatches).unwrap();
            let bit_count = u64::try_from(i128::from(max) - i128::from(min) + 1).unwrap();
            let mut payload = vec![type_tag];
            payload.extend_from_slice(&min.to_be_bytes());
            payload.extend_from_slice(&max.to_be_bytes());
            payload.extend_from_slice(&bit_count.to_be_bytes());
            let mut bits = vec![0; usize::try_from((bit_count + 7) / 8).unwrap()];
            bits[0] |= 1;
            let last = bit_count - 1;
            bits[usize::try_from(last / 8).unwrap()] |= 1 << (last % 8);
            payload.extend_from_slice(&bits);
            let encoded = encode_physical_leaf(
                ArtifactKind::Bitset,
                &schema,
                LogicalVersion::FIRST,
                false,
                None,
                &payload,
            )
            .unwrap();

            assert!(
                decode_test_leaf(&encoded, ArtifactKind::Bitset, &schema, None).is_err(),
                "accepted out-of-range endpoints {min}..={max} for {data_type:?}"
            );
        }
    }

    #[test]
    fn bloom_decoder_rebuilds_and_validates_full_contract_metadata() {
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let policy = MaterializationPolicy::new(8, 5, 17, 1, 1 << 20, 1 << 16, 1).unwrap();
        let contract = BloomHashContract::new(&schema, policy).unwrap();
        let values = MembershipValues::int64([1, 7, 42]);
        let (bit_count, bits) = build_bits(&values, &contract, &mut Vec::new()).unwrap();
        let payload = |algorithm: u16, cardinality: u64, bit_count: u64, bits: &[u8]| {
            let mut payload = Vec::new();
            payload.extend_from_slice(&algorithm.to_be_bytes());
            payload.extend_from_slice(&contract.scalar_framing_version().to_be_bytes());
            payload.extend_from_slice(&contract.seed().to_be_bytes());
            payload.extend_from_slice(&contract.bits_per_key().to_be_bytes());
            payload.extend_from_slice(&contract.hash_count().to_be_bytes());
            payload.extend_from_slice(&cardinality.to_be_bytes());
            payload.extend_from_slice(&bit_count.to_be_bytes());
            payload.extend_from_slice(bits);
            payload
        };
        let encode = |payload: &[u8]| {
            encode_physical_leaf(
                ArtifactKind::Bloom,
                &schema,
                LogicalVersion::FIRST,
                false,
                Some(contract.digest()),
                payload,
            )
            .unwrap()
        };
        let valid = encode(&payload(1, 3, bit_count, &bits));
        assert!(
            decode_test_leaf(
                &valid,
                ArtifactKind::Bloom,
                &schema,
                Some(contract.digest())
            )
            .is_ok()
        );
        for malformed in [
            payload(2, 3, bit_count, &bits),
            payload(1, 0, bit_count, &bits),
            payload(1, 3, bit_count + 64, &bits),
            payload(1, 3, bit_count, &vec![0; bits.len()]),
            payload(1, 3, bit_count, &bits[..bits.len() - 1]),
        ] {
            let encoded = encode(&malformed);
            assert!(
                decode_test_leaf(
                    &encoded,
                    ArtifactKind::Bloom,
                    &schema,
                    Some(contract.digest())
                )
                .is_err()
            );
        }
    }
}
