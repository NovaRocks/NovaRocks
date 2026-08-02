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

use std::error::Error;
use std::fmt;

#[cfg(any(test, feature = "runtime-filter-test-support"))]
use std::cell::Cell;

use arrow::datatypes::{DataType, TimeUnit};

use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
use crate::runtime_filter::port::final_domain::{
    CompletionFence, FinalDomainError, FinalDomainShard, RuntimeCompletionFenceContract,
};
use crate::runtime_filter::port::identity::{ProducerSequence, ProducerStreamId};
use crate::runtime_filter::port::ordered_bound::{
    OrderedBoundUpdate, OrderedScalar, OrderedTuple, RuntimeOrderContract,
};
use crate::runtime_filter::port::producer::RuntimeContractViolationKind;
use crate::runtime_filter::port::topk_summary::{RuntimeTopKSummaryContract, TopKSummary};
use crate::runtime_filter::port::value_domain::{
    ContributionSizeError, FINGERPRINT_VERSION_TAG, MembershipValues, ValueDomainDelta,
};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;

const MAGIC: &[u8; 4] = b"NRFC";
const CODEC_VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 1 + 1 + 32 + 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WireContributionKind {
    Membership,
    OrderedBound,
    TopKSummary,
    FinalDomain,
}

impl WireContributionKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Membership => 1,
            Self::OrderedBound => 2,
            Self::TopKSummary => 3,
            Self::FinalDomain => 4,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Membership),
            2 => Some(Self::OrderedBound),
            3 => Some(Self::TopKSummary),
            4 => Some(Self::FinalDomain),
            _ => None,
        }
    }
}

fn encode_frame_header(payload: &mut Vec<u8>, kind: WireContributionKind) {
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&CODEC_VERSION.to_be_bytes());
    payload.push(kind.tag());
    payload.push(0);
}

fn decode_frame_header(
    reader: &mut Reader<'_>,
) -> Result<WireContributionKind, ContributionCodecError> {
    if reader.read_exact(MAGIC.len())? != MAGIC {
        return Err(ContributionCodecError::Malformed);
    }
    if reader.read_u16()? != CODEC_VERSION {
        return Err(ContributionCodecError::UnknownVersion);
    }
    let frame_kind = WireContributionKind::from_tag(reader.read_u8()?)
        .ok_or(ContributionCodecError::UnknownKind)?;
    if reader.read_u8()? != 0 {
        return Err(ContributionCodecError::InvalidFlags);
    }
    Ok(frame_kind)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterContribution {
    Membership(ValueDomainDelta),
    OrderedBound(OrderedBoundUpdate),
    TopKSummary(TopKSummary),
    FinalDomain(FinalDomainShard),
}

#[derive(Clone, Copy, Debug)]
pub enum ContributionCodecExpectation<'a> {
    Membership(&'a ArtifactMembershipSchema),
    OrderedBound(&'a RuntimeOrderContract),
    TopKSummary(&'a RuntimeTopKSummaryContract),
    FinalDomain {
        contract: &'a RuntimeCompletionFenceContract,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedContribution {
    schema_digest: [u8; 32],
    payload: Vec<u8>,
}

impl EncodedContribution {
    pub const fn schema_digest(&self) -> &[u8; 32] {
        &self.schema_digest
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_parts(self) -> ([u8; 32], Vec<u8>) {
        (self.schema_digest, self.payload)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContributionCodecError {
    Malformed,
    Truncated,
    UnknownVersion,
    UnknownKind,
    InvalidFlags,
    KindMismatch,
    SchemaMismatch,
    LengthOverflow,
    TrailingBytes,
    NonCanonicalPayload,
    EncodedSizeExceeded,
    ResourceLimit,
}

impl fmt::Display for ContributionCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid runtime filter contribution: {self:?}")
    }
}

impl Error for ContributionCodecError {}

impl From<ContributionSizeError> for ContributionCodecError {
    fn from(_error: ContributionSizeError) -> Self {
        Self::LengthOverflow
    }
}

pub fn max_encoded_len_for_contribution_budget(
    max_contribution_bytes: usize,
) -> Result<usize, ContributionCodecError> {
    HEADER_LEN
        .checked_add(max_contribution_bytes)
        .ok_or(ContributionCodecError::LengthOverflow)
}

pub fn semantic_contribution_bytes(
    contribution: &RuntimeFilterContribution,
) -> Result<usize, ContributionCodecError> {
    match contribution {
        RuntimeFilterContribution::Membership(delta) => delta
            .canonical_encoded_len()
            .map_err(ContributionCodecError::from),
        RuntimeFilterContribution::OrderedBound(update) => update
            .canonical_contribution_bytes()
            .ok_or(ContributionCodecError::LengthOverflow),
        RuntimeFilterContribution::TopKSummary(summary) => summary
            .canonical_contribution_bytes()
            .ok_or(ContributionCodecError::LengthOverflow),
        RuntimeFilterContribution::FinalDomain(shard) => shard
            .canonical_contribution_bytes()
            .ok_or(ContributionCodecError::LengthOverflow),
    }
}

pub fn encoded_contribution_len(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
) -> Result<usize, ContributionCodecError> {
    let body_len = match (contribution, expectation) {
        (
            RuntimeFilterContribution::Membership(delta),
            ContributionCodecExpectation::Membership(schema),
        ) => {
            if !delta.matches_data_type(schema.data_type()) {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            delta.canonical_encoded_len()?
        }
        (
            RuntimeFilterContribution::OrderedBound(update),
            ContributionCodecExpectation::OrderedBound(contract),
        ) => {
            if update.order_contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            update.canonical_contribution_len()?
        }
        (
            RuntimeFilterContribution::TopKSummary(summary),
            ContributionCodecExpectation::TopKSummary(contract),
        ) => {
            if summary.contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            summary.canonical_body_len()?
        }
        (
            RuntimeFilterContribution::FinalDomain(shard),
            ContributionCodecExpectation::FinalDomain {
                contract,
                stream,
                sequence,
            },
        ) => {
            verify_final_domain_scope(shard, contract, stream, sequence)?;
            size_of::<[u8; 32]>()
                .checked_add(shard.domain().canonical_encoded_len()?)
                .ok_or(ContributionCodecError::LengthOverflow)?
        }
        _ => return Err(ContributionCodecError::KindMismatch),
    };
    encoded_frame_len_from_body_len(body_len)
}

/// Validate only the typed contribution/installed-contract relationship. This does
/// not walk or allocate the contribution body, so callers can preserve structural
/// errors even after a producer becomes terminal without encoding a large payload.
pub fn validate_contribution_contract(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
) -> Result<(), ContributionCodecError> {
    match (contribution, expectation) {
        (
            RuntimeFilterContribution::Membership(delta),
            ContributionCodecExpectation::Membership(schema),
        ) if delta.matches_data_type(schema.data_type()) => Ok(()),
        (
            RuntimeFilterContribution::OrderedBound(update),
            ContributionCodecExpectation::OrderedBound(contract),
        ) if update.order_contract_digest() == contract.digest() => Ok(()),
        (
            RuntimeFilterContribution::TopKSummary(summary),
            ContributionCodecExpectation::TopKSummary(contract),
        ) if summary.contract_digest() == contract.digest() => Ok(()),
        (
            RuntimeFilterContribution::FinalDomain(shard),
            ContributionCodecExpectation::FinalDomain {
                contract,
                stream,
                sequence,
            },
        ) => verify_final_domain_scope(shard, contract, stream, sequence),
        (RuntimeFilterContribution::Membership(_), ContributionCodecExpectation::Membership(_))
        | (
            RuntimeFilterContribution::OrderedBound(_),
            ContributionCodecExpectation::OrderedBound(_),
        )
        | (
            RuntimeFilterContribution::TopKSummary(_),
            ContributionCodecExpectation::TopKSummary(_),
        ) => Err(ContributionCodecError::SchemaMismatch),
        _ => Err(ContributionCodecError::KindMismatch),
    }
}

pub fn encode_contribution(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
) -> Result<EncodedContribution, ContributionCodecError> {
    #[cfg(any(test, feature = "runtime-filter-test-support"))]
    if REJECT_CONTRIBUTION_ALLOCATION_FOR_TEST.with(Cell::get) {
        return encode_contribution_with_allocator(
            contribution,
            expectation,
            max_encoded_bytes,
            &AlwaysRejectContributionFrameAllocator,
        );
    }
    encode_contribution_with_allocator(
        contribution,
        expectation,
        max_encoded_bytes,
        &SystemContributionFrameAllocator,
    )
}

pub fn decode_contribution(
    payload: &[u8],
    envelope_schema_digest: &[u8; 32],
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
) -> Result<RuntimeFilterContribution, ContributionCodecError> {
    if payload.len() > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }

    let mut reader = Reader::new(payload);
    let frame_kind = decode_frame_header(&mut reader)?;
    let frame_digest = reader.read_array::<32>()?;
    let body_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    match body_len.cmp(&reader.remaining_len()) {
        std::cmp::Ordering::Less => return Err(ContributionCodecError::TrailingBytes),
        std::cmp::Ordering::Greater => return Err(ContributionCodecError::Truncated),
        std::cmp::Ordering::Equal => {}
    }
    let body = reader.read_exact(body_len)?;
    debug_assert!(reader.is_empty());

    if frame_kind != expectation_kind(expectation) {
        return Err(ContributionCodecError::KindMismatch);
    }
    let installed_digest = expectation_digest(expectation);
    if frame_digest != *envelope_schema_digest
        || frame_digest != installed_digest
        || *envelope_schema_digest != installed_digest
    {
        return Err(ContributionCodecError::SchemaMismatch);
    }

    let contribution = match (frame_kind, expectation) {
        (WireContributionKind::Membership, ContributionCodecExpectation::Membership(schema)) => {
            RuntimeFilterContribution::Membership(decode_membership_body(body, schema.data_type())?)
        }
        (
            WireContributionKind::OrderedBound,
            ContributionCodecExpectation::OrderedBound(contract),
        ) => RuntimeFilterContribution::OrderedBound(decode_ordered_bound_body(body, contract)?),
        (
            WireContributionKind::TopKSummary,
            ContributionCodecExpectation::TopKSummary(contract),
        ) => RuntimeFilterContribution::TopKSummary(decode_topk_body(body, contract)?),
        (
            WireContributionKind::FinalDomain,
            ContributionCodecExpectation::FinalDomain {
                contract,
                stream,
                sequence,
            },
        ) => RuntimeFilterContribution::FinalDomain(decode_final_domain_body(
            body, contract, stream, sequence,
        )?),
        _ => return Err(ContributionCodecError::KindMismatch),
    };
    let canonical = encode_contribution(&contribution, expectation, payload.len())?;
    if canonical.payload() != payload {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(contribution)
}

trait ContributionFrameAllocator {
    fn allocate(&self, exact_len: usize) -> Result<Vec<u8>, ContributionCodecError>;
}

struct SystemContributionFrameAllocator;

#[cfg(any(test, feature = "runtime-filter-test-support"))]
struct AlwaysRejectContributionFrameAllocator;

impl ContributionFrameAllocator for SystemContributionFrameAllocator {
    fn allocate(&self, exact_len: usize) -> Result<Vec<u8>, ContributionCodecError> {
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(exact_len)
            .map_err(|_| ContributionCodecError::ResourceLimit)?;
        Ok(payload)
    }
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
impl ContributionFrameAllocator for AlwaysRejectContributionFrameAllocator {
    fn allocate(&self, _exact_len: usize) -> Result<Vec<u8>, ContributionCodecError> {
        Err(ContributionCodecError::ResourceLimit)
    }
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
thread_local! {
    static REJECT_CONTRIBUTION_ALLOCATION_FOR_TEST: Cell<bool> = const { Cell::new(false) };
}

/// Run a real contribution encode through a deterministic rejecting allocator on this
/// test thread. The thread-local scope keeps parallel adapter tests isolated.
#[cfg(any(test, feature = "runtime-filter-test-support"))]
pub fn with_rejecting_contribution_allocator_for_test<T>(run: impl FnOnce() -> T) -> T {
    struct Reset(bool);

    impl Drop for Reset {
        fn drop(&mut self) {
            REJECT_CONTRIBUTION_ALLOCATION_FOR_TEST.with(|reject| reject.set(self.0));
        }
    }

    let previous = REJECT_CONTRIBUTION_ALLOCATION_FOR_TEST.with(|reject| reject.replace(true));
    let _reset = Reset(previous);
    run()
}

fn encode_contribution_with_allocator(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
    allocator: &impl ContributionFrameAllocator,
) -> Result<EncodedContribution, ContributionCodecError> {
    let exact_len = encoded_contribution_len(contribution, expectation)?;
    if exact_len > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }

    let (kind, schema_digest, body_len) = match (contribution, expectation) {
        (
            RuntimeFilterContribution::Membership(delta),
            ContributionCodecExpectation::Membership(schema),
        ) => (
            WireContributionKind::Membership,
            schema.digest().bytes(),
            delta.canonical_encoded_len()?,
        ),
        (
            RuntimeFilterContribution::OrderedBound(update),
            ContributionCodecExpectation::OrderedBound(contract),
        ) => {
            if update.order_contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            (
                WireContributionKind::OrderedBound,
                contract.digest().bytes(),
                update.canonical_contribution_len()?,
            )
        }
        (
            RuntimeFilterContribution::TopKSummary(summary),
            ContributionCodecExpectation::TopKSummary(contract),
        ) => {
            if summary.contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            (
                WireContributionKind::TopKSummary,
                contract.digest().bytes(),
                summary.canonical_body_len()?,
            )
        }
        (
            RuntimeFilterContribution::FinalDomain(shard),
            ContributionCodecExpectation::FinalDomain {
                contract,
                stream,
                sequence,
            },
        ) => {
            verify_final_domain_scope(shard, contract, stream, sequence)?;
            (
                WireContributionKind::FinalDomain,
                contract.digest().bytes(),
                size_of::<[u8; 32]>()
                    .checked_add(shard.domain().canonical_encoded_len()?)
                    .ok_or(ContributionCodecError::LengthOverflow)?,
            )
        }
        _ => return Err(ContributionCodecError::KindMismatch),
    };
    let body_len = u64::try_from(body_len).map_err(|_| ContributionCodecError::LengthOverflow)?;
    let mut payload = allocator.allocate(exact_len)?;
    encode_frame_header(&mut payload, kind);
    payload.extend_from_slice(&schema_digest);
    payload.extend_from_slice(&body_len.to_be_bytes());
    match contribution {
        RuntimeFilterContribution::Membership(delta) => {
            delta.encode_canonical_into(&mut payload)?;
        }
        RuntimeFilterContribution::OrderedBound(update) => {
            update.encode_bound_canonical_into(&mut payload)?;
        }
        RuntimeFilterContribution::TopKSummary(summary) => {
            summary.encode_canonical_body_into(&mut payload)?;
        }
        RuntimeFilterContribution::FinalDomain(shard) => {
            payload.extend_from_slice(&shard.fence_digest());
            shard.domain().encode_canonical_into(&mut payload)?;
        }
    }
    debug_assert_eq!(payload.len(), exact_len);
    Ok(EncodedContribution {
        schema_digest,
        payload,
    })
}

fn encoded_frame_len_from_body_len(body_len: usize) -> Result<usize, ContributionCodecError> {
    let exact_len = HEADER_LEN
        .checked_add(body_len)
        .ok_or(ContributionCodecError::LengthOverflow)?;
    u64::try_from(body_len).map_err(|_| ContributionCodecError::LengthOverflow)?;
    u64::try_from(exact_len).map_err(|_| ContributionCodecError::LengthOverflow)?;
    Ok(exact_len)
}

const fn expectation_kind(expectation: ContributionCodecExpectation<'_>) -> WireContributionKind {
    match expectation {
        ContributionCodecExpectation::Membership(_) => WireContributionKind::Membership,
        ContributionCodecExpectation::OrderedBound(_) => WireContributionKind::OrderedBound,
        ContributionCodecExpectation::TopKSummary(_) => WireContributionKind::TopKSummary,
        ContributionCodecExpectation::FinalDomain { .. } => WireContributionKind::FinalDomain,
    }
}

fn expectation_digest(expectation: ContributionCodecExpectation<'_>) -> [u8; 32] {
    match expectation {
        ContributionCodecExpectation::Membership(schema) => schema.digest().bytes(),
        ContributionCodecExpectation::OrderedBound(contract) => contract.digest().bytes(),
        ContributionCodecExpectation::TopKSummary(contract) => contract.digest().bytes(),
        ContributionCodecExpectation::FinalDomain { contract, .. } => contract.digest().bytes(),
    }
}

fn decode_membership_body(
    body: &[u8],
    expected_data_type: &DataType,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    decode_membership_body_with_policy(
        body,
        expected_data_type,
        ContributionCodecError::SchemaMismatch,
    )
}

fn decode_membership_body_with_policy(
    body: &[u8],
    expected_data_type: &DataType,
    schema_mismatch_error: ContributionCodecError,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    let mut reader = Reader::new(body);
    let version_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if reader.read_exact(version_len)? != FINGERPRINT_VERSION_TAG {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let values = match expected_data_type {
        DataType::Boolean => {
            expect_type_tag(&mut reader, 1, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 1)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(match reader.read_u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(ContributionCodecError::NonCanonicalPayload),
                });
            }
            MembershipValues::boolean(values)
        }
        DataType::Int8 => {
            expect_type_tag(&mut reader, 2, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 1)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i8()?);
            }
            MembershipValues::int8(values)
        }
        DataType::Int16 => {
            expect_type_tag(&mut reader, 3, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 2)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i16()?);
            }
            MembershipValues::int16(values)
        }
        DataType::Int32 => {
            expect_type_tag(&mut reader, 4, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 4)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i32()?);
            }
            MembershipValues::int32(values)
        }
        DataType::Int64 => {
            expect_type_tag(&mut reader, 5, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i64()?);
            }
            MembershipValues::int64(values)
        }
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            expect_type_tag(&mut reader, 6, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 16)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i128()?);
            }
            MembershipValues::large_int(values)
        }
        DataType::Float32 => {
            expect_type_tag(&mut reader, 7, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 4)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(f32::from_bits(reader.read_u32()?));
            }
            MembershipValues::float32(values)
        }
        DataType::Float64 => {
            expect_type_tag(&mut reader, 8, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(f64::from_bits(reader.read_u64()?));
            }
            MembershipValues::float64(values)
        }
        DataType::Utf8 => {
            expect_type_tag(&mut reader, 9, schema_mismatch_error)?;
            let count = read_count(&mut reader)?;
            ensure_count_bytes(&reader, count, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                let len = usize::try_from(reader.read_u64()?)
                    .map_err(|_| ContributionCodecError::LengthOverflow)?;
                let bytes = reader.read_exact(len)?;
                let value = std::str::from_utf8(bytes)
                    .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
                let mut owned = String::new();
                owned
                    .try_reserve_exact(len)
                    .map_err(|_| ContributionCodecError::ResourceLimit)?;
                owned.push_str(value);
                values.push(owned);
            }
            MembershipValues::utf8(values)
        }
        DataType::Date32 => {
            expect_type_tag(&mut reader, 10, schema_mismatch_error)?;
            let count = read_fixed_count(&mut reader, 4)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i32()?);
            }
            MembershipValues::date32(values)
        }
        DataType::Timestamp(unit, timezone) => {
            expect_type_tag(&mut reader, 11, schema_mismatch_error)?;
            let encoded_unit = reader.read_u8()?;
            if encoded_unit != time_unit_tag(unit) {
                return Err(if (1..=4).contains(&encoded_unit) {
                    schema_mismatch_error
                } else {
                    ContributionCodecError::NonCanonicalPayload
                });
            }
            match reader.read_u8()? {
                0 if timezone.is_none() => {}
                0 => return Err(schema_mismatch_error),
                1 => {
                    let len = usize::try_from(reader.read_u64()?)
                        .map_err(|_| ContributionCodecError::LengthOverflow)?;
                    let timezone_bytes = reader.read_exact(len)?;
                    std::str::from_utf8(timezone_bytes)
                        .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
                    let Some(expected_timezone) = timezone else {
                        return Err(schema_mismatch_error);
                    };
                    if timezone_bytes != expected_timezone.as_bytes() {
                        return Err(schema_mismatch_error);
                    }
                }
                _ => return Err(ContributionCodecError::NonCanonicalPayload),
            }
            let count = read_fixed_count(&mut reader, 8)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i64()?);
            }
            MembershipValues::timestamp(unit.clone(), timezone.clone(), values)
        }
        DataType::Decimal128(precision, scale) => {
            expect_type_tag(&mut reader, 12, schema_mismatch_error)?;
            let encoded_precision = reader.read_u8()?;
            let encoded_scale = reader.read_u8()? as i8;
            if encoded_precision != *precision || encoded_scale != *scale {
                return Err(
                    if decimal_metadata_is_valid(encoded_precision, encoded_scale) {
                        schema_mismatch_error
                    } else {
                        ContributionCodecError::NonCanonicalPayload
                    },
                );
            }
            let count = read_fixed_count(&mut reader, 16)?;
            let mut values = reserve_values(count)?;
            for _ in 0..count {
                values.push(reader.read_i128()?);
            }
            MembershipValues::decimal128(*precision, *scale, values)
                .map_err(|_| ContributionCodecError::NonCanonicalPayload)?
        }
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    };
    let contains_null = match reader.read_u8()? {
        0 => false,
        1 => true,
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    };
    if !reader.is_empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(ValueDomainDelta::new(values, contains_null))
}

fn decode_ordered_bound_body(
    body: &[u8],
    contract: &RuntimeOrderContract,
) -> Result<OrderedBoundUpdate, ContributionCodecError> {
    let mut reader = Reader::new(body);
    let tuple = decode_ordered_tuple(&mut reader, contract)?;
    if !reader.is_empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    OrderedBoundUpdate::new(contract, tuple)
        .map_err(|_| ContributionCodecError::NonCanonicalPayload)
}

fn decode_topk_body(
    body: &[u8],
    contract: &RuntimeTopKSummaryContract,
) -> Result<TopKSummary, ContributionCodecError> {
    let mut reader = Reader::new(body);
    let candidate_count =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    let installed_k =
        usize::try_from(contract.k().get()).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if candidate_count > installed_k {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let minimum_bytes =
        minimum_topk_tuple_prefix_bytes(candidate_count, contract.order().keys().len())?;
    if minimum_bytes > reader.remaining_len() {
        return Err(ContributionCodecError::Truncated);
    }
    let mut candidates = reserve_values(candidate_count)?;
    for _ in 0..candidate_count {
        candidates.push(decode_ordered_tuple(&mut reader, contract.order())?);
    }
    if !reader.is_empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    TopKSummary::try_new(contract, candidates)
        .map_err(|_| ContributionCodecError::NonCanonicalPayload)
}

fn decode_final_domain_body(
    body: &[u8],
    contract: &RuntimeCompletionFenceContract,
    stream: ProducerStreamId,
    sequence: ProducerSequence,
) -> Result<FinalDomainShard, ContributionCodecError> {
    let mut reader = Reader::new(body);
    let encoded_fence_digest = reader.read_array::<32>()?;
    let fence = CompletionFence::try_from_remote_codec(
        contract.digest(),
        stream,
        sequence,
        encoded_fence_digest,
    )
    .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
    let domain = decode_final_domain_membership_body(
        reader.read_exact(reader.remaining_len())?,
        contract.membership_schema().data_type(),
    )?;
    let shard =
        FinalDomainShard::try_new(contract, fence, domain).map_err(map_final_domain_error)?;
    verify_final_domain_scope(&shard, contract, stream, sequence)?;
    Ok(shard)
}

fn decode_final_domain_membership_body(
    body: &[u8],
    expected_data_type: &DataType,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    match decode_membership_body_with_policy(
        body,
        expected_data_type,
        ContributionCodecError::SchemaMismatch,
    ) {
        Ok(domain) => Ok(domain),
        Err(ContributionCodecError::SchemaMismatch) => {
            validate_alternate_membership_body_with_observer(
                body,
                &NoopAlternateMembershipMetadataObserver,
            )?;
            Err(ContributionCodecError::SchemaMismatch)
        }
        Err(error) => Err(error),
    }
}

trait AlternateMembershipMetadataObserver {
    fn borrowed_timezone(&self, _timezone: &str) {}
}

struct NoopAlternateMembershipMetadataObserver;

impl AlternateMembershipMetadataObserver for NoopAlternateMembershipMetadataObserver {}

fn validate_alternate_membership_body_with_observer(
    body: &[u8],
    observer: &impl AlternateMembershipMetadataObserver,
) -> Result<(), ContributionCodecError> {
    let mut reader = Reader::new(body);
    let version_len =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if reader.read_exact(version_len)? != FINGERPRINT_VERSION_TAG {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    match reader.read_u8()? {
        1 => validate_canonical_boolean_values(&mut reader)?,
        2 => validate_canonical_fixed_values(&mut reader, 1, |reader| reader.read_i8())?,
        3 => validate_canonical_fixed_values(&mut reader, 2, |reader| reader.read_i16())?,
        4 => validate_canonical_fixed_values(&mut reader, 4, |reader| reader.read_i32())?,
        5 => validate_canonical_fixed_values(&mut reader, 8, |reader| reader.read_i64())?,
        6 => validate_canonical_fixed_values(&mut reader, 16, |reader| reader.read_i128())?,
        7 => validate_canonical_float32_values(&mut reader)?,
        8 => validate_canonical_float64_values(&mut reader)?,
        9 => validate_canonical_utf8_values(&mut reader)?,
        10 => validate_canonical_fixed_values(&mut reader, 4, |reader| reader.read_i32())?,
        11 => {
            match reader.read_u8()? {
                1..=4 => {}
                _ => return Err(ContributionCodecError::NonCanonicalPayload),
            }
            match reader.read_u8()? {
                0 => {}
                1 => {
                    let len = usize::try_from(reader.read_u64()?)
                        .map_err(|_| ContributionCodecError::LengthOverflow)?;
                    let bytes = reader.read_exact(len)?;
                    let timezone = std::str::from_utf8(bytes)
                        .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
                    observer.borrowed_timezone(timezone);
                }
                _ => return Err(ContributionCodecError::NonCanonicalPayload),
            }
            validate_canonical_fixed_values(&mut reader, 8, |reader| reader.read_i64())?;
        }
        12 => {
            let precision = reader.read_u8()?;
            let scale = reader.read_u8()? as i8;
            if !decimal_metadata_is_valid(precision, scale) {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            validate_canonical_decimal_values(&mut reader, precision)?;
        }
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    }
    match reader.read_u8()? {
        0 | 1 => {}
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    }
    if !reader.is_empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(())
}

fn validate_canonical_boolean_values(
    reader: &mut Reader<'_>,
) -> Result<(), ContributionCodecError> {
    let count = read_fixed_count(reader, 1)?;
    let mut previous = None;
    for _ in 0..count {
        let value = match reader.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(ContributionCodecError::NonCanonicalPayload),
        };
        validate_strictly_increasing(&mut previous, value)?;
    }
    Ok(())
}

fn validate_canonical_fixed_values<T: Copy + Ord>(
    reader: &mut Reader<'_>,
    width: usize,
    mut read: impl FnMut(&mut Reader<'_>) -> Result<T, ContributionCodecError>,
) -> Result<(), ContributionCodecError> {
    let count = read_fixed_count(reader, width)?;
    let mut previous = None;
    for _ in 0..count {
        validate_strictly_increasing(&mut previous, read(reader)?)?;
    }
    Ok(())
}

fn validate_canonical_float32_values(
    reader: &mut Reader<'_>,
) -> Result<(), ContributionCodecError> {
    let count = read_fixed_count(reader, 4)?;
    let mut previous = None;
    for _ in 0..count {
        let bits = reader.read_u32()?;
        let value = f32::from_bits(bits);
        if (value == 0.0 && bits != 0) || (value.is_nan() && bits != 0x7fc0_0000) {
            return Err(ContributionCodecError::NonCanonicalPayload);
        }
        if previous.is_some_and(|previous: f32| previous.total_cmp(&value).is_ge()) {
            return Err(ContributionCodecError::NonCanonicalPayload);
        }
        previous = Some(value);
    }
    Ok(())
}

fn validate_canonical_float64_values(
    reader: &mut Reader<'_>,
) -> Result<(), ContributionCodecError> {
    let count = read_fixed_count(reader, 8)?;
    let mut previous = None;
    for _ in 0..count {
        let bits = reader.read_u64()?;
        let value = f64::from_bits(bits);
        if (value == 0.0 && bits != 0) || (value.is_nan() && bits != 0x7ff8_0000_0000_0000) {
            return Err(ContributionCodecError::NonCanonicalPayload);
        }
        if previous.is_some_and(|previous: f64| previous.total_cmp(&value).is_ge()) {
            return Err(ContributionCodecError::NonCanonicalPayload);
        }
        previous = Some(value);
    }
    Ok(())
}

fn validate_canonical_utf8_values(reader: &mut Reader<'_>) -> Result<(), ContributionCodecError> {
    let count = read_count(reader)?;
    ensure_count_bytes(reader, count, size_of::<u64>())?;
    let mut previous: Option<&str> = None;
    for _ in 0..count {
        let len = usize::try_from(reader.read_u64()?)
            .map_err(|_| ContributionCodecError::LengthOverflow)?;
        let value = std::str::from_utf8(reader.read_exact(len)?)
            .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
        if previous.is_some_and(|previous| previous >= value) {
            return Err(ContributionCodecError::NonCanonicalPayload);
        }
        previous = Some(value);
    }
    Ok(())
}

fn validate_canonical_decimal_values(
    reader: &mut Reader<'_>,
    precision: u8,
) -> Result<(), ContributionCodecError> {
    let count = read_fixed_count(reader, 16)?;
    let exclusive_bound = 10_i128
        .checked_pow(u32::from(precision))
        .ok_or(ContributionCodecError::LengthOverflow)?;
    let mut previous = None;
    for _ in 0..count {
        let value = reader.read_i128()?;
        if value <= -exclusive_bound || value >= exclusive_bound {
            return Err(ContributionCodecError::NonCanonicalPayload);
        }
        validate_strictly_increasing(&mut previous, value)?;
    }
    Ok(())
}

fn validate_strictly_increasing<T: Copy + Ord>(
    previous: &mut Option<T>,
    value: T,
) -> Result<(), ContributionCodecError> {
    if previous.is_some_and(|previous| previous >= value) {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    *previous = Some(value);
    Ok(())
}

fn verify_final_domain_scope(
    shard: &FinalDomainShard,
    contract: &RuntimeCompletionFenceContract,
    stream: ProducerStreamId,
    sequence: ProducerSequence,
) -> Result<(), ContributionCodecError> {
    shard
        .verify_scope(contract, stream, sequence)
        .map_err(|error| match error.kind() {
            RuntimeContractViolationKind::TypeMismatch => ContributionCodecError::SchemaMismatch,
            _ => ContributionCodecError::NonCanonicalPayload,
        })
}

fn map_final_domain_error(error: FinalDomainError) -> ContributionCodecError {
    match error {
        FinalDomainError::ContractMismatch | FinalDomainError::DomainSchemaMismatch => {
            ContributionCodecError::SchemaMismatch
        }
        _ => ContributionCodecError::NonCanonicalPayload,
    }
}

fn decode_ordered_tuple(
    reader: &mut Reader<'_>,
    contract: &RuntimeOrderContract,
) -> Result<OrderedTuple, ContributionCodecError> {
    let arity =
        usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if arity != contract.keys().len() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    if arity > reader.remaining_len() {
        return Err(ContributionCodecError::Truncated);
    }
    let mut values = reserve_values(arity)?;
    for key in contract.keys() {
        values.push(match reader.read_u8()? {
            0 => None,
            1 => Some(decode_ordered_scalar(reader, key.data_type())?),
            _ => return Err(ContributionCodecError::NonCanonicalPayload),
        });
    }
    OrderedTuple::try_from_codec(contract, values)
        .map_err(|_| ContributionCodecError::NonCanonicalPayload)
}

fn decode_ordered_scalar(
    reader: &mut Reader<'_>,
    data_type: &DataType,
) -> Result<OrderedScalar, ContributionCodecError> {
    Ok(match data_type {
        DataType::Boolean => OrderedScalar::Boolean(match reader.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(ContributionCodecError::NonCanonicalPayload),
        }),
        DataType::Int8 => OrderedScalar::Int8(reader.read_i8()?),
        DataType::Int16 => OrderedScalar::Int16(reader.read_i16()?),
        DataType::Int32 => OrderedScalar::Int32(reader.read_i32()?),
        DataType::Int64 => OrderedScalar::Int64(reader.read_i64()?),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            OrderedScalar::LargeInt(reader.read_i128()?)
        }
        DataType::Utf8 => {
            let len = usize::try_from(reader.read_u64()?)
                .map_err(|_| ContributionCodecError::LengthOverflow)?;
            let bytes = reader.read_exact(len)?;
            let value = std::str::from_utf8(bytes)
                .map_err(|_| ContributionCodecError::NonCanonicalPayload)?;
            let mut owned = String::new();
            owned
                .try_reserve_exact(len)
                .map_err(|_| ContributionCodecError::ResourceLimit)?;
            owned.push_str(value);
            OrderedScalar::Utf8(owned.into())
        }
        DataType::Date32 => OrderedScalar::Date32(reader.read_i32()?),
        DataType::Timestamp(_, _) => OrderedScalar::Timestamp(reader.read_i64()?),
        DataType::Decimal128(_, _) => OrderedScalar::Decimal128(reader.read_i128()?),
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    })
}

fn expect_type_tag(
    reader: &mut Reader<'_>,
    expected: u8,
    mismatch_error: ContributionCodecError,
) -> Result<(), ContributionCodecError> {
    let actual = reader.read_u8()?;
    if actual != expected {
        return Err(if (1..=12).contains(&actual) {
            mismatch_error
        } else {
            ContributionCodecError::NonCanonicalPayload
        });
    }
    Ok(())
}

fn decimal_metadata_is_valid(precision: u8, scale: i8) -> bool {
    (1..=38).contains(&precision) && scale <= 38 && (scale <= 0 || scale as u8 <= precision)
}

fn read_count(reader: &mut Reader<'_>) -> Result<usize, ContributionCodecError> {
    usize::try_from(reader.read_u64()?).map_err(|_| ContributionCodecError::LengthOverflow)
}

fn read_fixed_count(
    reader: &mut Reader<'_>,
    width: usize,
) -> Result<usize, ContributionCodecError> {
    let count = read_count(reader)?;
    ensure_count_bytes(reader, count, width)?;
    Ok(count)
}

fn ensure_count_bytes(
    reader: &Reader<'_>,
    count: usize,
    width: usize,
) -> Result<(), ContributionCodecError> {
    let required = count
        .checked_mul(width)
        .ok_or(ContributionCodecError::LengthOverflow)?;
    if required > reader.remaining_len() {
        return Err(ContributionCodecError::Truncated);
    }
    Ok(())
}

fn minimum_topk_tuple_prefix_bytes(
    candidate_count: usize,
    key_count: usize,
) -> Result<usize, ContributionCodecError> {
    let minimum_per_tuple = size_of::<u64>()
        .checked_add(key_count)
        .ok_or(ContributionCodecError::LengthOverflow)?;
    candidate_count
        .checked_mul(minimum_per_tuple)
        .ok_or(ContributionCodecError::LengthOverflow)
}

fn reserve_values<T>(count: usize) -> Result<Vec<T>, ContributionCodecError> {
    count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(ContributionCodecError::LengthOverflow)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| ContributionCodecError::ResourceLimit)?;
    Ok(values)
}

fn time_unit_tag(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    }
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

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ContributionCodecError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(len)
            .ok_or(ContributionCodecError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ContributionCodecError> {
        Ok(self.read_exact(N)?.try_into().expect("exact array length"))
    }

    fn read_u8(&mut self) -> Result<u8, ContributionCodecError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ContributionCodecError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, ContributionCodecError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, ContributionCodecError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_i8(&mut self) -> Result<i8, ContributionCodecError> {
        Ok(i8::from_be_bytes(self.read_array()?))
    }

    fn read_i16(&mut self) -> Result<i16, ContributionCodecError> {
        Ok(i16::from_be_bytes(self.read_array()?))
    }

    fn read_i32(&mut self) -> Result<i32, ContributionCodecError> {
        Ok(i32::from_be_bytes(self.read_array()?))
    }

    fn read_i64(&mut self) -> Result<i64, ContributionCodecError> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    fn read_i128(&mut self) -> Result<i128, ContributionCodecError> {
        Ok(i128::from_be_bytes(self.read_array()?))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, TimeUnit};

    use super::*;
    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{
        BindingId, ChannelId, CompletionFenceKind, NullOrder, NullSemantics, OrderContract,
        OrderKeyContract, SortDirection, TopKSummaryRequirement,
    };
    use crate::runtime_filter::port::final_domain::{
        CollectingFinalDomainTestIssuer, CompletionFenceAuthority, FinalDomainTestIssuerTransition,
    };
    use crate::runtime_filter::port::identity::{DeploymentEpoch, PartitionId};
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedScalar, OrderedTuple, RuntimeOrderContract,
        comparator_digest_for_test,
    };
    use crate::runtime_filter::port::value_domain::MembershipValues;
    use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;

    struct CountingAllocator {
        calls: Cell<usize>,
        exact_len: Cell<usize>,
    }

    struct RejectingContributionFrameAllocator {
        calls: Cell<usize>,
        exact_len: Cell<usize>,
    }

    struct ConformanceFixtures {
        membership: RuntimeFilterContribution,
        membership_schema: ArtifactMembershipSchema,
        ordered: RuntimeFilterContribution,
        ordered_contract: RuntimeOrderContract,
        topk: RuntimeFilterContribution,
        topk_contract: RuntimeTopKSummaryContract,
        final_domain: RuntimeFilterContribution,
        final_contract: RuntimeCompletionFenceContract,
        final_stream: ProducerStreamId,
        final_sequence: ProducerSequence,
    }

    struct BorrowedTimezoneObserver {
        calls: Cell<usize>,
        pointer: Cell<usize>,
        len: Cell<usize>,
    }

    impl BorrowedTimezoneObserver {
        fn new() -> Self {
            Self {
                calls: Cell::new(0),
                pointer: Cell::new(0),
                len: Cell::new(0),
            }
        }
    }

    impl AlternateMembershipMetadataObserver for BorrowedTimezoneObserver {
        fn borrowed_timezone(&self, timezone: &str) {
            self.calls.set(self.calls.get() + 1);
            self.pointer.set(timezone.as_ptr() as usize);
            self.len.set(timezone.len());
        }
    }

    impl CountingAllocator {
        fn new() -> Self {
            Self {
                calls: Cell::new(0),
                exact_len: Cell::new(0),
            }
        }
    }

    impl ContributionFrameAllocator for CountingAllocator {
        fn allocate(&self, exact_len: usize) -> Result<Vec<u8>, ContributionCodecError> {
            self.calls.set(self.calls.get() + 1);
            self.exact_len.set(exact_len);
            Ok(Vec::with_capacity(exact_len))
        }
    }

    impl RejectingContributionFrameAllocator {
        fn new() -> Self {
            Self {
                calls: Cell::new(0),
                exact_len: Cell::new(0),
            }
        }
    }

    impl ContributionFrameAllocator for RejectingContributionFrameAllocator {
        fn allocate(&self, exact_len: usize) -> Result<Vec<u8>, ContributionCodecError> {
            self.calls.set(self.calls.get() + 1);
            self.exact_len.set(exact_len);
            Err(ContributionCodecError::ResourceLimit)
        }
    }

    impl ConformanceFixtures {
        fn cases(&self) -> [(&RuntimeFilterContribution, ContributionCodecExpectation<'_>); 4] {
            [
                (
                    &self.membership,
                    ContributionCodecExpectation::Membership(&self.membership_schema),
                ),
                (
                    &self.ordered,
                    ContributionCodecExpectation::OrderedBound(&self.ordered_contract),
                ),
                (
                    &self.topk,
                    ContributionCodecExpectation::TopKSummary(&self.topk_contract),
                ),
                (
                    &self.final_domain,
                    ContributionCodecExpectation::FinalDomain {
                        contract: &self.final_contract,
                        stream: self.final_stream,
                        sequence: self.final_sequence,
                    },
                ),
            ]
        }
    }

    fn schema(data_type: &DataType, null_semantics: NullSemantics) -> ArtifactMembershipSchema {
        ArtifactMembershipSchema::new(data_type, null_semantics).unwrap()
    }

    fn membership(
        values: MembershipValues,
        contains_null: bool,
    ) -> (RuntimeFilterContribution, ArtifactMembershipSchema) {
        let schema = schema(&values.data_type(), NullSemantics::NullSafeEqual);
        (
            RuntimeFilterContribution::Membership(ValueDomainDelta::new(values, contains_null)),
            schema,
        )
    }

    fn encode_membership(
        values: MembershipValues,
        contains_null: bool,
    ) -> (
        RuntimeFilterContribution,
        ArtifactMembershipSchema,
        EncodedContribution,
    ) {
        let (contribution, schema) = membership(values, contains_null);
        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::Membership(&schema),
            usize::MAX,
        )
        .unwrap();
        (contribution, schema, encoded)
    }

    fn values_offset(payload: &[u8]) -> usize {
        let version_len =
            u64::from_be_bytes(payload[HEADER_LEN..HEADER_LEN + 8].try_into().unwrap()) as usize;
        HEADER_LEN + 8 + version_len
    }

    fn first_value_offset(payload: &[u8]) -> usize {
        values_offset(payload) + 1 + 8
    }

    fn assert_membership_round_trip(values: MembershipValues, contains_null: bool) {
        let (expected, schema, encoded) = encode_membership(values, contains_null);
        assert_eq!(encoded.schema_digest(), &schema.digest().bytes());
        assert_eq!(
            encoded_contribution_len(&expected, ContributionCodecExpectation::Membership(&schema)),
            Ok(encoded.payload().len())
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&schema),
                encoded.payload().len(),
            ),
            Ok(expected.clone())
        );
        assert_eq!(
            encode_contribution(
                &expected,
                ContributionCodecExpectation::Membership(&schema),
                encoded.payload().len(),
            ),
            Ok(encoded)
        );
    }

    fn order_contract(keys: Vec<OrderKeyContract>) -> RuntimeOrderContract {
        let comparator_digest = comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION);
        RuntimeOrderContract::try_from_plan(&OrderContract {
            keys,
            inclusive: true,
            comparator_digest,
        })
        .unwrap()
    }

    fn order_key(
        data_type: DataType,
        direction: SortDirection,
        null_order: NullOrder,
    ) -> OrderKeyContract {
        OrderKeyContract {
            data_type,
            direction,
            null_order,
        }
    }

    fn ordered_bound(
        contract: &RuntimeOrderContract,
        values: impl IntoIterator<Item = Option<OrderedScalar>>,
    ) -> RuntimeFilterContribution {
        let tuple = OrderedTuple::try_new(contract, values).unwrap();
        RuntimeFilterContribution::OrderedBound(OrderedBoundUpdate::new(contract, tuple).unwrap())
    }

    fn assert_ordered_bound_round_trip(
        contract: &RuntimeOrderContract,
        values: impl IntoIterator<Item = Option<OrderedScalar>>,
    ) -> EncodedContribution {
        let contribution = ordered_bound(contract, values);
        let expectation = ContributionCodecExpectation::OrderedBound(contract);
        let encoded = encode_contribution(&contribution, expectation, usize::MAX);
        let expected_len = encoded_contribution_len(&contribution, expectation);
        assert_eq!(
            encoded.as_ref().map(|encoded| encoded.payload().len()),
            expected_len.as_ref().map(|len| *len)
        );
        let encoded = encoded.unwrap();
        assert_eq!(encoded.schema_digest(), &contract.digest().bytes());
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                encoded.payload().len(),
            ),
            Ok(contribution)
        );
        encoded
    }

    fn topk_contract(keys: Vec<OrderKeyContract>, k: u32) -> RuntimeTopKSummaryContract {
        let order = OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        };
        RuntimeTopKSummaryContract::try_from_plan(
            &order,
            TopKSummaryRequirement::try_new(k).unwrap(),
        )
        .unwrap()
    }

    fn topk_summary(
        contract: &RuntimeTopKSummaryContract,
        candidates: impl IntoIterator<Item = Vec<Option<OrderedScalar>>>,
    ) -> RuntimeFilterContribution {
        let candidates = candidates
            .into_iter()
            .map(|values| OrderedTuple::try_new(contract.order(), values).unwrap())
            .collect();
        RuntimeFilterContribution::TopKSummary(TopKSummary::try_new(contract, candidates).unwrap())
    }

    fn assert_topk_round_trip(
        contract: &RuntimeTopKSummaryContract,
        candidates: impl IntoIterator<Item = Vec<Option<OrderedScalar>>>,
    ) -> EncodedContribution {
        let contribution = topk_summary(contract, candidates);
        let expectation = ContributionCodecExpectation::TopKSummary(contract);
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();
        assert_eq!(
            encoded_contribution_len(&contribution, expectation),
            Ok(encoded.payload().len())
        );
        assert_eq!(encoded.schema_digest(), &contract.digest().bytes());
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                encoded.payload().len(),
            ),
            Ok(contribution)
        );
        encoded
    }

    fn final_domain_contract(data_type: &DataType) -> RuntimeCompletionFenceContract {
        RuntimeCompletionFenceContract::try_from_install(
            UniqueId::new(101, 102),
            DeploymentEpoch::new(103),
            ChannelId::new(104),
            CompletionFenceKind::CommittedDomainFrozen,
            &schema(data_type, NullSemantics::NullSafeEqual),
        )
        .unwrap()
    }

    fn final_domain_stream(binding: u32, instance: UniqueId, partition: u32) -> ProducerStreamId {
        ProducerStreamId::new(
            BindingId::new(binding),
            instance,
            PartitionId::new(partition),
        )
    }

    fn final_domain_shard(
        contract: &RuntimeCompletionFenceContract,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
        domain: ValueDomainDelta,
    ) -> FinalDomainShard {
        let authority = CompletionFenceAuthority::try_new(
            Arc::new(contract.clone()),
            stream.binding_id(),
            stream.fragment_instance_id(),
        )
        .unwrap();
        let issuer = match CollectingFinalDomainTestIssuer::new(authority, 1).close_driver() {
            FinalDomainTestIssuerTransition::Frozen(issuer) => issuer,
            FinalDomainTestIssuerTransition::Collecting(_) => {
                panic!("the only open driver must freeze the test issuer")
            }
        };
        issuer.issue_shard(stream, sequence, domain).unwrap()
    }

    fn encode_final_domain(
        contract: &RuntimeCompletionFenceContract,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
        domain: ValueDomainDelta,
    ) -> (RuntimeFilterContribution, EncodedContribution) {
        let contribution = RuntimeFilterContribution::FinalDomain(final_domain_shard(
            contract, stream, sequence, domain,
        ));
        let expectation = ContributionCodecExpectation::FinalDomain {
            contract,
            stream,
            sequence,
        };
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();
        (contribution, encoded)
    }

    fn conformance_fixtures() -> ConformanceFixtures {
        let (membership, membership_schema) = membership(MembershipValues::int64([1, 2]), true);
        let ordered_contract = order_contract(vec![order_key(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let ordered = ordered_bound(&ordered_contract, [Some(OrderedScalar::Int64(7))]);
        let topk_contract = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            2,
        );
        let topk = topk_summary(
            &topk_contract,
            [
                vec![Some(OrderedScalar::Int64(3))],
                vec![Some(OrderedScalar::Int64(5))],
            ],
        );
        let final_contract = final_domain_contract(&DataType::Int64);
        let final_stream = final_domain_stream(603, UniqueId::new(601, 602), 604);
        let final_sequence = ProducerSequence::new(605);
        let final_domain = RuntimeFilterContribution::FinalDomain(final_domain_shard(
            &final_contract,
            final_stream,
            final_sequence,
            ValueDomainDelta::new(MembershipValues::int64([11, 13]), false),
        ));
        ConformanceFixtures {
            membership,
            membership_schema,
            ordered,
            ordered_contract,
            topk,
            topk_contract,
            final_domain,
            final_contract,
            final_stream,
            final_sequence,
        }
    }

    #[test]
    fn installed_inbound_producer_contract_codec_limits_cover_all_variants() {
        let fixtures = conformance_fixtures();

        for (contribution, expectation) in fixtures.cases() {
            let semantic_bytes = semantic_contribution_bytes(contribution).unwrap();
            assert!(semantic_bytes > 0);
            let wire_ceiling = max_encoded_len_for_contribution_budget(semantic_bytes).unwrap();
            let encoded = encode_contribution(contribution, expectation, wire_ceiling).unwrap();

            assert_eq!(wire_ceiling, HEADER_LEN + semantic_bytes);
            assert!(encoded.payload().len() <= wire_ceiling);
        }
    }

    #[test]
    fn installed_inbound_producer_contract_separates_wire_and_semantic_limits() {
        let fixtures = conformance_fixtures();
        let contribution = &fixtures.ordered;
        let expectation = ContributionCodecExpectation::OrderedBound(&fixtures.ordered_contract);
        let semantic_bytes = semantic_contribution_bytes(contribution).unwrap();
        let semantic_budget = semantic_bytes - 1;
        let wire_ceiling = max_encoded_len_for_contribution_budget(semantic_budget).unwrap();
        let encoded = encode_contribution(contribution, expectation, wire_ceiling).unwrap();

        assert!(encoded.payload().len() <= wire_ceiling);
        assert!(semantic_contribution_bytes(contribution).unwrap() > semantic_budget);
    }

    #[test]
    fn installed_inbound_producer_contract_checked_wire_ceiling_overflow_is_rejected() {
        assert_eq!(
            max_encoded_len_for_contribution_budget(usize::MAX),
            Err(ContributionCodecError::LengthOverflow)
        );
    }

    #[test]
    fn all_contribution_kinds_have_exact_known_frame_prefixes() {
        let fixtures = conformance_fixtures();
        for ((contribution, expectation), tag) in fixtures.cases().into_iter().zip([1_u8, 2, 3, 4])
        {
            let encoded = encode_contribution(contribution, expectation, usize::MAX).unwrap();
            assert_eq!(
                &encoded.payload()[..8],
                &[b'N', b'R', b'F', b'C', 0, 1, tag, 0]
            );
        }
    }

    #[test]
    fn unified_encode_rejects_contribution_expectation_kind_mismatch() {
        let fixtures = conformance_fixtures();
        for (contribution, _) in fixtures.cases() {
            for (_, expectation) in fixtures.cases() {
                if expectation_kind(expectation)
                    == match contribution {
                        RuntimeFilterContribution::Membership(_) => {
                            WireContributionKind::Membership
                        }
                        RuntimeFilterContribution::OrderedBound(_) => {
                            WireContributionKind::OrderedBound
                        }
                        RuntimeFilterContribution::TopKSummary(_) => {
                            WireContributionKind::TopKSummary
                        }
                        RuntimeFilterContribution::FinalDomain(_) => {
                            WireContributionKind::FinalDomain
                        }
                    }
                {
                    continue;
                }
                assert_eq!(
                    encoded_contribution_len(contribution, expectation),
                    Err(ContributionCodecError::KindMismatch)
                );
                let allocator = CountingAllocator::new();
                assert_eq!(
                    encode_contribution_with_allocator(
                        contribution,
                        expectation,
                        usize::MAX,
                        &allocator,
                    ),
                    Err(ContributionCodecError::KindMismatch)
                );
                assert_eq!(allocator.calls.get(), 0);
            }
        }
    }

    #[test]
    fn unified_decode_rejects_frame_expectation_kind_mismatch() {
        let fixtures = conformance_fixtures();
        for (contribution, source_expectation) in fixtures.cases() {
            let encoded =
                encode_contribution(contribution, source_expectation, usize::MAX).unwrap();
            for (_, target_expectation) in fixtures.cases() {
                if expectation_kind(source_expectation) == expectation_kind(target_expectation) {
                    continue;
                }
                assert_eq!(
                    decode_contribution(
                        encoded.payload(),
                        encoded.schema_digest(),
                        target_expectation,
                        usize::MAX,
                    ),
                    Err(ContributionCodecError::KindMismatch)
                );
            }
        }
    }

    #[test]
    fn all_variants_report_exact_encoded_length() {
        let fixtures = conformance_fixtures();
        for (contribution, expectation) in fixtures.cases() {
            let exact = encoded_contribution_len(contribution, expectation).unwrap();
            let encoded = encode_contribution(contribution, expectation, exact).unwrap();
            assert_eq!(encoded.payload().len(), exact);
            assert_eq!(
                encode_contribution(contribution, expectation, exact - 1),
                Err(ContributionCodecError::EncodedSizeExceeded)
            );
        }
    }

    #[test]
    fn all_variants_enforce_frame_envelope_expectation_digest_equality() {
        let fixtures = conformance_fixtures();
        for (contribution, expectation) in fixtures.cases() {
            let encoded = encode_contribution(contribution, expectation, usize::MAX).unwrap();
            let expected_digest = expectation_digest(expectation);
            assert_eq!(encoded.schema_digest(), &expected_digest);
            assert_eq!(&encoded.payload()[8..40], &expected_digest);
            assert_eq!(
                decode_contribution(encoded.payload(), &expected_digest, expectation, usize::MAX,),
                Ok(contribution.clone())
            );

            let mut wrong_frame = encoded.payload().to_vec();
            wrong_frame[8] ^= 1;
            assert_eq!(
                decode_contribution(
                    &wrong_frame,
                    encoded.schema_digest(),
                    expectation,
                    usize::MAX,
                ),
                Err(ContributionCodecError::SchemaMismatch)
            );
            let mut wrong_envelope = expected_digest;
            wrong_envelope[0] ^= 1;
            assert_eq!(
                decode_contribution(encoded.payload(), &wrong_envelope, expectation, usize::MAX,),
                Err(ContributionCodecError::SchemaMismatch)
            );

            let mut coherent_wrong = encoded.payload().to_vec();
            let mut wrong_digest = expected_digest;
            wrong_digest[0] ^= 1;
            coherent_wrong[8..40].copy_from_slice(&wrong_digest);
            assert_eq!(
                decode_contribution(&coherent_wrong, &wrong_digest, expectation, usize::MAX,),
                Err(ContributionCodecError::SchemaMismatch)
            );
        }

        let membership = encode_contribution(
            &fixtures.membership,
            ContributionCodecExpectation::Membership(&fixtures.membership_schema),
            usize::MAX,
        )
        .unwrap();
        let alternate_membership = schema(&DataType::Utf8, NullSemantics::NullSafeEqual);
        assert_eq!(
            decode_contribution(
                membership.payload(),
                membership.schema_digest(),
                ContributionCodecExpectation::Membership(&alternate_membership),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let ordered = encode_contribution(
            &fixtures.ordered,
            ContributionCodecExpectation::OrderedBound(&fixtures.ordered_contract),
            usize::MAX,
        )
        .unwrap();
        let alternate_ordered = order_contract(vec![order_key(
            DataType::Int64,
            SortDirection::Descending,
            NullOrder::Last,
        )]);
        assert_eq!(
            decode_contribution(
                ordered.payload(),
                ordered.schema_digest(),
                ContributionCodecExpectation::OrderedBound(&alternate_ordered),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let topk = encode_contribution(
            &fixtures.topk,
            ContributionCodecExpectation::TopKSummary(&fixtures.topk_contract),
            usize::MAX,
        )
        .unwrap();
        let alternate_topk = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            3,
        );
        assert_eq!(
            decode_contribution(
                topk.payload(),
                topk.schema_digest(),
                ContributionCodecExpectation::TopKSummary(&alternate_topk),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let final_domain = encode_contribution(
            &fixtures.final_domain,
            ContributionCodecExpectation::FinalDomain {
                contract: &fixtures.final_contract,
                stream: fixtures.final_stream,
                sequence: fixtures.final_sequence,
            },
            usize::MAX,
        )
        .unwrap();
        let alternate_final = RuntimeCompletionFenceContract::try_from_install(
            UniqueId::new(801, 802),
            DeploymentEpoch::new(803),
            ChannelId::new(804),
            CompletionFenceKind::CommittedDomainFrozen,
            &schema(&DataType::Int64, NullSemantics::NullSafeEqual),
        )
        .unwrap();
        assert_eq!(
            decode_contribution(
                final_domain.payload(),
                final_domain.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &alternate_final,
                    stream: fixtures.final_stream,
                    sequence: fixtures.final_sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn all_variants_reject_truncated_trailing_unknown_and_oversized_frames() {
        let fixtures = conformance_fixtures();
        for (contribution, expectation) in fixtures.cases() {
            let encoded = encode_contribution(contribution, expectation, usize::MAX).unwrap();

            let truncated = &encoded.payload()[..encoded.payload().len() - 1];
            assert_eq!(
                decode_contribution(truncated, encoded.schema_digest(), expectation, usize::MAX,),
                Err(ContributionCodecError::Truncated)
            );

            let mut trailing = encoded.payload().to_vec();
            trailing.push(0);
            assert_eq!(
                decode_contribution(&trailing, encoded.schema_digest(), expectation, usize::MAX,),
                Err(ContributionCodecError::TrailingBytes)
            );

            let mut unknown = encoded.payload().to_vec();
            unknown[6] = u8::MAX;
            assert_eq!(
                decode_contribution(&unknown, encoded.schema_digest(), expectation, usize::MAX,),
                Err(ContributionCodecError::UnknownKind)
            );

            assert_eq!(
                decode_contribution(
                    encoded.payload(),
                    encoded.schema_digest(),
                    expectation,
                    encoded.payload().len() - 1,
                ),
                Err(ContributionCodecError::EncodedSizeExceeded)
            );
        }
    }

    #[test]
    fn cross_kind_body_splicing_is_rejected() {
        let fixtures = conformance_fixtures();
        let membership = encode_contribution(
            &fixtures.membership,
            ContributionCodecExpectation::Membership(&fixtures.membership_schema),
            usize::MAX,
        )
        .unwrap();
        let ordered = encode_contribution(
            &fixtures.ordered,
            ContributionCodecExpectation::OrderedBound(&fixtures.ordered_contract),
            usize::MAX,
        )
        .unwrap();
        let topk_one_contract = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            1,
        );
        let topk_one_contribution =
            topk_summary(&topk_one_contract, [vec![Some(OrderedScalar::Int64(3))]]);
        let topk_one = encode_contribution(
            &topk_one_contribution,
            ContributionCodecExpectation::TopKSummary(&topk_one_contract),
            usize::MAX,
        )
        .unwrap();
        let final_domain = encode_contribution(
            &fixtures.final_domain,
            ContributionCodecExpectation::FinalDomain {
                contract: &fixtures.final_contract,
                stream: fixtures.final_stream,
                sequence: fixtures.final_sequence,
            },
            usize::MAX,
        )
        .unwrap();
        let (_, long_membership_schema, long_membership) =
            encode_membership(MembershipValues::utf8(["0123456789abcdef"]), false);

        for (target, source, expectation) in [
            (
                &membership,
                &ordered,
                ContributionCodecExpectation::Membership(&fixtures.membership_schema),
            ),
            (
                &ordered,
                &membership,
                ContributionCodecExpectation::OrderedBound(&fixtures.ordered_contract),
            ),
            (
                &topk_one,
                &membership,
                ContributionCodecExpectation::TopKSummary(&topk_one_contract),
            ),
            (
                &final_domain,
                &long_membership,
                ContributionCodecExpectation::FinalDomain {
                    contract: &fixtures.final_contract,
                    stream: fixtures.final_stream,
                    sequence: fixtures.final_sequence,
                },
            ),
        ] {
            let mut spliced = target.payload()[..HEADER_LEN].to_vec();
            spliced.extend_from_slice(&source.payload()[HEADER_LEN..]);
            let body_len = spliced.len() - HEADER_LEN;
            spliced[40..48].copy_from_slice(&(body_len as u64).to_be_bytes());
            assert_eq!(
                decode_contribution(&spliced, target.schema_digest(), expectation, usize::MAX,),
                Err(ContributionCodecError::NonCanonicalPayload)
            );
        }
        assert_eq!(
            long_membership.schema_digest(),
            &long_membership_schema.digest().bytes()
        );
    }

    #[test]
    fn allocation_failure_maps_to_resource_limit() {
        let fixtures = conformance_fixtures();
        for (contribution, expectation) in fixtures.cases() {
            let exact = encoded_contribution_len(contribution, expectation).unwrap();
            let allocator = RejectingContributionFrameAllocator::new();
            assert_eq!(
                encode_contribution_with_allocator(
                    contribution,
                    expectation,
                    exact - 1,
                    &allocator,
                ),
                Err(ContributionCodecError::EncodedSizeExceeded)
            );
            assert_eq!(allocator.calls.get(), 0);

            assert_eq!(
                encode_contribution_with_allocator(contribution, expectation, exact, &allocator,),
                Err(ContributionCodecError::ResourceLimit)
            );
            assert_eq!(allocator.calls.get(), 1);
            assert_eq!(allocator.exact_len.get(), exact);
        }
    }

    #[test]
    fn membership_data_type_mismatch_maps_to_schema_mismatch() {
        let contribution = RuntimeFilterContribution::Membership(ValueDomainDelta::new(
            MembershipValues::int64([1]),
            false,
        ));
        let wrong_schema = schema(&DataType::Utf8, NullSemantics::NullSafeEqual);
        assert_eq!(
            encode_contribution(
                &contribution,
                ContributionCodecExpectation::Membership(&wrong_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let installed_schema = schema(&DataType::Int64, NullSemantics::NullSafeEqual);
        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::Membership(&installed_schema),
            usize::MAX,
        )
        .unwrap();
        let wrong_digest = wrong_schema.digest().bytes();
        let mut forged_for_wrong_install = encoded.payload().to_vec();
        forged_for_wrong_install[8..40].copy_from_slice(&wrong_digest);
        assert_eq!(
            decode_contribution(
                &forged_for_wrong_install,
                &wrong_digest,
                ContributionCodecExpectation::Membership(&wrong_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn ordered_and_topk_contract_digest_mismatch_map_to_schema_mismatch() {
        let fixtures = conformance_fixtures();
        let wrong_ordered = order_contract(vec![order_key(
            DataType::Int64,
            SortDirection::Descending,
            NullOrder::Last,
        )]);
        assert_eq!(
            encode_contribution(
                &fixtures.ordered,
                ContributionCodecExpectation::OrderedBound(&wrong_ordered),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let wrong_topk = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            3,
        );
        assert_eq!(
            encode_contribution(
                &fixtures.topk,
                ContributionCodecExpectation::TopKSummary(&wrong_topk),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn final_domain_contract_mismatch_maps_to_schema_mismatch() {
        let fixtures = conformance_fixtures();
        let wrong_contract = RuntimeCompletionFenceContract::try_from_install(
            UniqueId::new(701, 702),
            DeploymentEpoch::new(703),
            ChannelId::new(704),
            CompletionFenceKind::CommittedDomainFrozen,
            &schema(&DataType::Int64, NullSemantics::NullSafeEqual),
        )
        .unwrap();
        assert_eq!(
            encode_contribution(
                &fixtures.final_domain,
                ContributionCodecExpectation::FinalDomain {
                    contract: &wrong_contract,
                    stream: fixtures.final_stream,
                    sequence: fixtures.final_sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn final_domain_route_scope_mismatch_maps_to_noncanonical_payload() {
        let fixtures = conformance_fixtures();
        for (stream, sequence) in [
            (
                final_domain_stream(
                    fixtures.final_stream.binding_id().get() + 1,
                    fixtures.final_stream.fragment_instance_id(),
                    fixtures.final_stream.partition_id().get(),
                ),
                fixtures.final_sequence,
            ),
            (
                fixtures.final_stream,
                ProducerSequence::new(fixtures.final_sequence.get() + 1),
            ),
        ] {
            assert_eq!(
                encode_contribution(
                    &fixtures.final_domain,
                    ContributionCodecExpectation::FinalDomain {
                        contract: &fixtures.final_contract,
                        stream,
                        sequence,
                    },
                    usize::MAX,
                ),
                Err(ContributionCodecError::NonCanonicalPayload)
            );
        }
    }

    #[test]
    fn membership_round_trip_is_deterministic_and_contract_driven() {
        let cases = vec![
            (MembershipValues::boolean([false, true]), false),
            (MembershipValues::int8([i8::MIN, 0, i8::MAX]), true),
            (MembershipValues::int16([i16::MIN, 0, i16::MAX]), false),
            (MembershipValues::int32([i32::MIN, 0, i32::MAX]), true),
            (MembershipValues::int64([i64::MIN, 0, i64::MAX]), false),
            (MembershipValues::large_int([i128::MIN, 0, i128::MAX]), true),
            (
                MembershipValues::float32([
                    -0.0,
                    0.0,
                    f32::from_bits(0x7fc0_1234),
                    f32::NEG_INFINITY,
                    f32::INFINITY,
                ]),
                false,
            ),
            (
                MembershipValues::float64([
                    -0.0,
                    0.0,
                    f64::from_bits(0x7ff8_0000_0000_1234),
                    f64::NEG_INFINITY,
                    f64::INFINITY,
                ]),
                true,
            ),
            (MembershipValues::utf8(["", "é", "東京"]), false),
            (MembershipValues::date32([i32::MIN, 0, i32::MAX]), true),
            (
                MembershipValues::timestamp(TimeUnit::Second, None, [i64::MIN, i64::MAX]),
                false,
            ),
            (
                MembershipValues::timestamp(
                    TimeUnit::Millisecond,
                    Some(Arc::from("UTC")),
                    [-1, 0, 1],
                ),
                true,
            ),
            (
                MembershipValues::timestamp(TimeUnit::Microsecond, None, [-1, 0, 1]),
                false,
            ),
            (
                MembershipValues::timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from("Asia/Shanghai")),
                    [-1, 0, 1],
                ),
                true,
            ),
            (
                MembershipValues::decimal128(38, 4, [-(10_i128.pow(38) - 1), 10_i128.pow(38) - 1])
                    .unwrap(),
                false,
            ),
            (MembershipValues::int64([]), false),
            (
                MembershipValues::empty_for_data_type(&DataType::FixedSizeBinary(
                    LARGEINT_BYTE_WIDTH,
                ))
                .unwrap(),
                true,
            ),
        ];

        for (values, contains_null) in cases {
            assert_membership_round_trip(values, contains_null);
        }

        let contribution = RuntimeFilterContribution::Membership(ValueDomainDelta::new(
            MembershipValues::int64([1]),
            true,
        ));
        let never_matches = schema(&DataType::Int64, NullSemantics::NeverMatches);
        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::Membership(&never_matches),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&never_matches),
                usize::MAX,
            ),
            Ok(contribution)
        );
    }

    #[test]
    fn membership_requires_frame_envelope_and_install_digest_match() {
        let (_, installed_schema, encoded) = encode_membership(MembershipValues::int64([7]), false);
        let wrong_schema = schema(&DataType::Int64, NullSemantics::NeverMatches);
        let mut wrong_frame = encoded.payload().to_vec();
        wrong_frame[8] ^= 1;
        let wrong_envelope = [0x55; 32];

        assert_eq!(
            decode_contribution(
                &wrong_frame,
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&installed_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                &wrong_envelope,
                ContributionCodecExpectation::Membership(&installed_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&wrong_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn membership_encode_rejects_data_type_mismatch() {
        let contribution = RuntimeFilterContribution::Membership(ValueDomainDelta::new(
            MembershipValues::int64([7]),
            false,
        ));
        let schema = schema(&DataType::Int32, NullSemantics::NeverMatches);

        assert_eq!(
            encode_contribution(
                &contribution,
                ContributionCodecExpectation::Membership(&schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn membership_rejects_bad_magic_version_kind_flags_and_body_lengths() {
        let (_, schema, encoded) = encode_membership(MembershipValues::int32([1]), false);
        let expectation = ContributionCodecExpectation::Membership(&schema);

        for (offset, value, error) in [
            (0, b'X', ContributionCodecError::Malformed),
            (5, 2, ContributionCodecError::UnknownVersion),
            (6, 99, ContributionCodecError::UnknownKind),
            (7, 1, ContributionCodecError::InvalidFlags),
        ] {
            let mut mutated = encoded.payload().to_vec();
            mutated[offset] = value;
            assert_eq!(
                decode_contribution(&mutated, encoded.schema_digest(), expectation, usize::MAX,),
                Err(error)
            );
        }

        let mut wrong_kind = encoded.payload().to_vec();
        wrong_kind[6] = 2;
        assert_eq!(
            decode_contribution(
                &wrong_kind,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::KindMismatch)
        );

        let body_len = encoded.payload().len() - HEADER_LEN;
        for (declared, error) in [
            (body_len - 1, ContributionCodecError::TrailingBytes),
            (body_len + 1, ContributionCodecError::Truncated),
        ] {
            let mut mutated = encoded.payload().to_vec();
            mutated[40..48].copy_from_slice(&(declared as u64).to_be_bytes());
            assert_eq!(
                decode_contribution(&mutated, encoded.schema_digest(), expectation, usize::MAX,),
                Err(error)
            );
        }
    }

    #[test]
    fn membership_rejects_noncanonical_values_and_trailing_bytes() {
        let (_, schema, encoded) = encode_membership(MembershipValues::int32([1, 2]), false);
        let expectation = ContributionCodecExpectation::Membership(&schema);
        let mut duplicate = encoded.payload().to_vec();
        let first = first_value_offset(&duplicate);
        duplicate[first + 4..first + 8].copy_from_slice(&1_i32.to_be_bytes());
        assert_eq!(
            decode_contribution(&duplicate, encoded.schema_digest(), expectation, usize::MAX,),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let mut invalid_null = encoded.payload().to_vec();
        *invalid_null.last_mut().unwrap() = 2;
        assert_eq!(
            decode_contribution(
                &invalid_null,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let (_, bool_schema, bool_encoded) =
            encode_membership(MembershipValues::boolean([true]), false);
        let mut invalid_bool = bool_encoded.payload().to_vec();
        let bool_value = first_value_offset(&invalid_bool);
        invalid_bool[bool_value] = 2;
        assert_eq!(
            decode_contribution(
                &invalid_bool,
                bool_encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&bool_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let mut trailing = encoded.payload().to_vec();
        trailing.push(0);
        assert_eq!(
            decode_contribution(&trailing, encoded.schema_digest(), expectation, usize::MAX,),
            Err(ContributionCodecError::TrailingBytes)
        );
    }

    #[test]
    fn membership_rejects_invalid_utf8_noncanonical_float_and_decimal_overflow() {
        let (_, utf8_schema, utf8) = encode_membership(MembershipValues::utf8(["a"]), false);
        let mut invalid_utf8 = utf8.payload().to_vec();
        let utf8_value = first_value_offset(&invalid_utf8) + 8;
        invalid_utf8[utf8_value] = 0xff;
        assert_eq!(
            decode_contribution(
                &invalid_utf8,
                utf8.schema_digest(),
                ContributionCodecExpectation::Membership(&utf8_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let (_, float_schema, float) =
            encode_membership(MembershipValues::float32([0.0, f32::NAN]), false);
        let mut negative_zero = float.payload().to_vec();
        let float_value = first_value_offset(&negative_zero);
        negative_zero[float_value..float_value + 4]
            .copy_from_slice(&(-0.0_f32).to_bits().to_be_bytes());
        assert_eq!(
            decode_contribution(
                &negative_zero,
                float.schema_digest(),
                ContributionCodecExpectation::Membership(&float_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut noncanonical_nan = float.payload().to_vec();
        noncanonical_nan[float_value + 4..float_value + 8]
            .copy_from_slice(&0x7fc0_0001_u32.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &noncanonical_nan,
                float.schema_digest(),
                ContributionCodecExpectation::Membership(&float_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let (_, decimal_schema, decimal) =
            encode_membership(MembershipValues::decimal128(3, 0, [999]).unwrap(), false);
        let mut overflow = decimal.payload().to_vec();
        let decimal_value = first_value_offset(&overflow) + 2;
        overflow[decimal_value..decimal_value + 16].copy_from_slice(&1000_i128.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &overflow,
                decimal.schema_digest(),
                ContributionCodecExpectation::Membership(&decimal_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
    }

    #[test]
    fn membership_decode_rejects_oversize_before_body_allocation() {
        let schema = schema(&DataType::Int64, NullSemantics::NeverMatches);
        assert_eq!(
            decode_contribution(
                &[0xff],
                &schema.digest().bytes(),
                ContributionCodecExpectation::Membership(&schema),
                0,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
    }

    #[test]
    fn membership_rejects_impossible_counts_before_value_allocation() {
        let (_, fixed_schema, fixed) = encode_membership(MembershipValues::int64([1]), false);
        let mut fixed_overflow = fixed.payload().to_vec();
        let fixed_count = values_offset(&fixed_overflow) + 1;
        fixed_overflow[fixed_count..fixed_count + 8].copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &fixed_overflow,
                fixed.schema_digest(),
                ContributionCodecExpectation::Membership(&fixed_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::LengthOverflow)
        );

        let (_, utf8_schema, utf8) = encode_membership(MembershipValues::utf8(["a"]), false);
        let mut utf8_overflow = utf8.payload().to_vec();
        let utf8_count = values_offset(&utf8_overflow) + 1;
        utf8_overflow[utf8_count..utf8_count + 8].copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &utf8_overflow,
                utf8.schema_digest(),
                ContributionCodecExpectation::Membership(&utf8_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::LengthOverflow)
        );
    }

    #[test]
    fn membership_encode_rejects_oversize_before_frame_allocation() {
        let (contribution, schema) = membership(MembershipValues::utf8(["large"]), false);
        let exact = encoded_contribution_len(
            &contribution,
            ContributionCodecExpectation::Membership(&schema),
        )
        .unwrap();
        let allocator = CountingAllocator::new();

        assert_eq!(
            encode_contribution_with_allocator(
                &contribution,
                ContributionCodecExpectation::Membership(&schema),
                exact - 1,
                &allocator,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        assert_eq!(allocator.calls.get(), 0);

        let encoded = encode_contribution_with_allocator(
            &contribution,
            ContributionCodecExpectation::Membership(&schema),
            exact,
            &allocator,
        )
        .unwrap();
        assert_eq!(allocator.calls.get(), 1);
        assert_eq!(allocator.exact_len.get(), exact);
        assert_eq!(encoded.payload().len(), exact);
    }

    #[test]
    fn membership_exact_limit_succeeds_and_limit_minus_one_fails() {
        let (contribution, schema) = membership(MembershipValues::int64([1, 2, 3]), true);
        let expectation = ContributionCodecExpectation::Membership(&schema);
        let exact = encoded_contribution_len(&contribution, expectation).unwrap();

        assert_eq!(
            encode_contribution(&contribution, expectation, exact - 1),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        let encoded = encode_contribution(&contribution, expectation, exact).unwrap();
        assert_eq!(encoded.payload().len(), exact);
    }

    #[test]
    fn membership_length_preflight_returns_error_without_panic_or_allocation() {
        assert_eq!(
            encoded_frame_len_from_body_len(usize::MAX),
            Err(ContributionCodecError::LengthOverflow)
        );
    }

    #[test]
    fn ordered_bound_round_trip_uses_installed_contract() {
        let contract = order_contract(vec![order_key(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let contribution = ordered_bound(&contract, [Some(OrderedScalar::Int64(42))]);
        let expectation = ContributionCodecExpectation::OrderedBound(&contract);

        assert_eq!(
            encode_contribution(&contribution, expectation, usize::MAX)
                .map(|encoded| encoded.payload().len()),
            Ok(HEADER_LEN + 8 + 1 + 8)
        );
        assert_ordered_bound_round_trip(&contract, [Some(OrderedScalar::Int64(42))]);
    }

    #[test]
    fn ordered_bound_covers_asc_desc_nulls_first_last_and_multikey() {
        let ascending = order_contract(vec![order_key(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::First,
        )]);
        let descending = order_contract(vec![order_key(
            DataType::Int64,
            SortDirection::Descending,
            NullOrder::Last,
        )]);
        let ascending_encoded =
            assert_ordered_bound_round_trip(&ascending, [Some(OrderedScalar::Int64(7))]);
        let descending_encoded =
            assert_ordered_bound_round_trip(&descending, [Some(OrderedScalar::Int64(7))]);
        assert_eq!(
            ascending_encoded.payload()[HEADER_LEN..],
            descending_encoded.payload()[HEADER_LEN..]
        );

        for (direction, null_order, value) in [
            (SortDirection::Ascending, NullOrder::First, None),
            (
                SortDirection::Ascending,
                NullOrder::Last,
                Some(OrderedScalar::Int64(1)),
            ),
            (
                SortDirection::Descending,
                NullOrder::First,
                Some(OrderedScalar::Int64(-1)),
            ),
            (SortDirection::Descending, NullOrder::Last, None),
        ] {
            let contract = order_contract(vec![order_key(DataType::Int64, direction, null_order)]);
            assert_ordered_bound_round_trip(&contract, [value]);
        }

        let contract = order_contract(vec![
            order_key(DataType::Int32, SortDirection::Ascending, NullOrder::Last),
            order_key(DataType::Utf8, SortDirection::Descending, NullOrder::First),
        ]);
        assert_ordered_bound_round_trip(
            &contract,
            [
                Some(OrderedScalar::Int32(7)),
                Some(OrderedScalar::Utf8(Arc::from("多键"))),
            ],
        );
    }

    #[test]
    fn ordered_bound_covers_utf8_decimal_timestamp_and_largeint() {
        let cases = [
            (
                DataType::Utf8,
                Some(OrderedScalar::Utf8(Arc::from("héllo-東京"))),
            ),
            (
                DataType::Decimal128(38, 6),
                Some(OrderedScalar::Decimal128(10_i128.pow(38) - 1)),
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                Some(OrderedScalar::Timestamp(i64::MIN)),
            ),
            (
                DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH),
                Some(OrderedScalar::LargeInt(i128::MAX)),
            ),
        ];

        for (data_type, value) in cases {
            let contract = order_contract(vec![order_key(
                data_type,
                SortDirection::Ascending,
                NullOrder::Last,
            )]);
            assert_ordered_bound_round_trip(&contract, [value]);
        }
    }

    #[test]
    fn ordered_bound_rejects_wrong_kind_digest_arity_type_and_noncanonical_scalar() {
        let contract = order_contract(vec![order_key(
            DataType::Boolean,
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let contribution = ordered_bound(&contract, [Some(OrderedScalar::Boolean(true))]);
        let expectation = ContributionCodecExpectation::OrderedBound(&contract);
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();

        let membership_schema = schema(&DataType::Boolean, NullSemantics::NeverMatches);
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::Membership(&membership_schema),
                usize::MAX,
            ),
            Err(ContributionCodecError::KindMismatch)
        );

        let mut wrong_digest = encoded.payload().to_vec();
        wrong_digest[8] ^= 1;
        assert_eq!(
            decode_contribution(
                &wrong_digest,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let other_contract = order_contract(vec![order_key(
            DataType::Boolean,
            SortDirection::Descending,
            NullOrder::Last,
        )]);
        assert_eq!(
            encode_contribution(
                &contribution,
                ContributionCodecExpectation::OrderedBound(&other_contract),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let mut wrong_arity = encoded.payload().to_vec();
        wrong_arity[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&2_u64.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &wrong_arity,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut impossible_arity = encoded.payload().to_vec();
        impossible_arity[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &impossible_arity,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let mut invalid_boolean = encoded.payload().to_vec();
        invalid_boolean[HEADER_LEN + 8 + 1] = 2;
        assert_eq!(
            decode_contribution(
                &invalid_boolean,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let utf8_contract = order_contract(vec![order_key(
            DataType::Utf8,
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let utf8_encoded = assert_ordered_bound_round_trip(
            &utf8_contract,
            [Some(OrderedScalar::Utf8("a".into()))],
        );
        let mut invalid_utf8 = utf8_encoded.payload().to_vec();
        invalid_utf8[HEADER_LEN + 8 + 1 + 8] = 0xff;
        assert_eq!(
            decode_contribution(
                &invalid_utf8,
                utf8_encoded.schema_digest(),
                ContributionCodecExpectation::OrderedBound(&utf8_contract),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut impossible_utf8 = utf8_encoded.payload().to_vec();
        impossible_utf8[HEADER_LEN + 8 + 1..HEADER_LEN + 8 + 1 + 8]
            .copy_from_slice(&u64::MAX.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &impossible_utf8,
                utf8_encoded.schema_digest(),
                ContributionCodecExpectation::OrderedBound(&utf8_contract),
                usize::MAX,
            ),
            Err(ContributionCodecError::Truncated)
        );

        let decimal_contract = order_contract(vec![order_key(
            DataType::Decimal128(3, 0),
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let decimal_encoded = assert_ordered_bound_round_trip(
            &decimal_contract,
            [Some(OrderedScalar::Decimal128(999))],
        );
        let mut decimal_overflow = decimal_encoded.payload().to_vec();
        decimal_overflow[HEADER_LEN + 8 + 1..HEADER_LEN + 8 + 1 + 16]
            .copy_from_slice(&1000_i128.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &decimal_overflow,
                decimal_encoded.schema_digest(),
                ContributionCodecExpectation::OrderedBound(&decimal_contract),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
    }

    #[test]
    fn ordered_bound_exact_limit_succeeds_and_limit_minus_one_fails() {
        let contract = order_contract(vec![order_key(
            DataType::Utf8,
            SortDirection::Ascending,
            NullOrder::Last,
        )]);
        let contribution =
            ordered_bound(&contract, [Some(OrderedScalar::Utf8(Arc::from("exact")))]);
        let expectation = ContributionCodecExpectation::OrderedBound(&contract);
        let exact = encoded_contribution_len(&contribution, expectation);
        assert_eq!(exact, Ok(HEADER_LEN + 8 + 1 + 8 + 5));
        let exact = exact.unwrap();

        assert_eq!(
            encode_contribution(&contribution, expectation, exact - 1),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        let encoded = encode_contribution(&contribution, expectation, exact).unwrap();
        assert_eq!(encoded.payload().len(), exact);
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact - 1,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact,
            ),
            Ok(contribution)
        );
    }

    #[test]
    fn topk_round_trip_accepts_empty_single_and_exact_k_candidates() {
        let contract = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            3,
        );

        let empty = assert_topk_round_trip(&contract, []);
        assert_eq!(empty.payload().len(), HEADER_LEN + 8);
        let single = assert_topk_round_trip(&contract, [vec![Some(OrderedScalar::Int64(7))]]);
        assert_eq!(single.payload().len(), HEADER_LEN + 8 + 8 + 1 + 8);
        assert_topk_round_trip(
            &contract,
            [
                vec![Some(OrderedScalar::Int64(1))],
                vec![Some(OrderedScalar::Int64(2))],
                vec![Some(OrderedScalar::Int64(3))],
            ],
        );
    }

    #[test]
    fn topk_covers_desc_null_order_multikey_utf8_and_decimal() {
        let descending = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Descending,
                NullOrder::First,
            )],
            3,
        );
        assert_topk_round_trip(
            &descending,
            [
                vec![None],
                vec![Some(OrderedScalar::Int64(9))],
                vec![Some(OrderedScalar::Int64(1))],
            ],
        );

        let multikey = topk_contract(
            vec![
                order_key(DataType::Utf8, SortDirection::Ascending, NullOrder::Last),
                order_key(
                    DataType::Decimal128(6, 2),
                    SortDirection::Descending,
                    NullOrder::First,
                ),
            ],
            3,
        );
        assert_topk_round_trip(
            &multikey,
            [
                vec![
                    Some(OrderedScalar::Utf8(Arc::from("a"))),
                    Some(OrderedScalar::Decimal128(9999)),
                ],
                vec![
                    Some(OrderedScalar::Utf8(Arc::from("a"))),
                    Some(OrderedScalar::Decimal128(-9999)),
                ],
                vec![Some(OrderedScalar::Utf8(Arc::from("多键"))), None],
            ],
        );
    }

    #[test]
    fn topk_rejects_over_k_unsorted_wrong_type_and_length_overflow() {
        let contract = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            2,
        );
        let encoded = assert_topk_round_trip(
            &contract,
            [
                vec![Some(OrderedScalar::Int64(1))],
                vec![Some(OrderedScalar::Int64(2))],
            ],
        );
        let expectation = ContributionCodecExpectation::TopKSummary(&contract);

        let mut over_k = encoded.payload().to_vec();
        over_k[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&3_u64.to_be_bytes());
        assert_eq!(
            decode_contribution(&over_k, encoded.schema_digest(), expectation, usize::MAX,),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let mut unsorted = encoded.payload().to_vec();
        let first_value = HEADER_LEN + 8 + 8 + 1;
        let second_value = first_value + 8 + 1 + 8;
        let first = unsorted[first_value..first_value + 8].to_vec();
        let second = unsorted[second_value..second_value + 8].to_vec();
        unsorted[first_value..first_value + 8].copy_from_slice(&second);
        unsorted[second_value..second_value + 8].copy_from_slice(&first);
        assert_eq!(
            decode_contribution(&unsorted, encoded.schema_digest(), expectation, usize::MAX,),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let boolean = topk_contract(
            vec![order_key(
                DataType::Boolean,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            1,
        );
        let boolean_encoded =
            assert_topk_round_trip(&boolean, [vec![Some(OrderedScalar::Boolean(true))]]);
        let mut wrong_type = boolean_encoded.payload().to_vec();
        wrong_type[HEADER_LEN + 8 + 8 + 1] = 2;
        assert_eq!(
            decode_contribution(
                &wrong_type,
                boolean_encoded.schema_digest(),
                ContributionCodecExpectation::TopKSummary(&boolean),
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        assert_eq!(
            minimum_topk_tuple_prefix_bytes(usize::MAX, 1),
            Err(ContributionCodecError::LengthOverflow)
        );

        let mut missing_presence_markers =
            encoded.payload()[..HEADER_LEN + 8 + (2 * (8 + 1)) - 1].to_vec();
        let body_len = missing_presence_markers.len() - HEADER_LEN;
        missing_presence_markers[40..48].copy_from_slice(&(body_len as u64).to_be_bytes());
        assert_eq!(
            decode_contribution(
                &missing_presence_markers,
                encoded.schema_digest(),
                expectation,
                usize::MAX,
            ),
            Err(ContributionCodecError::Truncated)
        );
    }

    #[test]
    fn topk_uses_install_frozen_k_and_digest() {
        let installed = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            2,
        );
        let different_k = topk_contract(
            vec![order_key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            3,
        );
        let contribution = topk_summary(&installed, [vec![Some(OrderedScalar::Int64(7))]]);
        assert_eq!(
            encode_contribution(
                &contribution,
                ContributionCodecExpectation::TopKSummary(&different_k),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::TopKSummary(&installed),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::TopKSummary(&different_k),
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn topk_exact_limit_succeeds_and_limit_minus_one_fails() {
        let contract = topk_contract(
            vec![order_key(
                DataType::Utf8,
                SortDirection::Ascending,
                NullOrder::Last,
            )],
            1,
        );
        let contribution = topk_summary(
            &contract,
            [vec![Some(OrderedScalar::Utf8(Arc::from("exact")))]],
        );
        let expectation = ContributionCodecExpectation::TopKSummary(&contract);
        let exact = encoded_contribution_len(&contribution, expectation).unwrap();

        assert_eq!(
            encode_contribution(&contribution, expectation, exact - 1),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        let encoded = encode_contribution(&contribution, expectation, exact).unwrap();
        assert_eq!(encoded.payload().len(), exact);
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact - 1,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact,
            ),
            Ok(contribution)
        );
    }

    #[test]
    fn final_domain_round_trip_reconstructs_exact_fence_scope() {
        let contract = final_domain_contract(&DataType::Int64);
        let instance = UniqueId::new(201, 202);
        let stream = final_domain_stream(203, instance, 204);
        let sequence = ProducerSequence::new(205);
        let (contribution, encoded) = encode_final_domain(
            &contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::int64([1, 2]), true),
        );
        let expectation = ContributionCodecExpectation::FinalDomain {
            contract: &contract,
            stream,
            sequence,
        };

        assert_eq!(encoded.schema_digest(), &contract.digest().bytes());
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                encoded.payload().len(),
            ),
            Ok(contribution)
        );
    }

    #[test]
    fn final_domain_body_contains_digest_but_not_route_identity() {
        let contract = final_domain_contract(&DataType::Int64);
        let stream = final_domain_stream(
            0xa1b2_c3d4,
            UniqueId::new(0x1122_3344_5566_7788, 0x2233_4455_6677_8899),
            0xb1c2_d3e4,
        );
        let sequence = ProducerSequence::new(0x3344_5566_7788_99aa);
        let domain = ValueDomainDelta::new(MembershipValues::int64([7]), false);
        let shard = final_domain_shard(&contract, stream, sequence, domain.clone());
        let contribution = RuntimeFilterContribution::FinalDomain(shard.clone());
        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::FinalDomain {
                contract: &contract,
                stream,
                sequence,
            },
            usize::MAX,
        )
        .unwrap();
        let mut expected_body = shard.fence_digest().to_vec();
        domain.encode_canonical_into(&mut expected_body).unwrap();

        assert_eq!(&encoded.payload()[HEADER_LEN..], expected_body);
    }

    #[test]
    fn final_domain_rejects_fence_digest_binding_finst_partition_sequence_mismatch() {
        let contract = final_domain_contract(&DataType::Int64);
        let instance = UniqueId::new(301, 302);
        let stream = final_domain_stream(303, instance, 304);
        let sequence = ProducerSequence::new(305);
        let (contribution, encoded) = encode_final_domain(
            &contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::int64([1]), false),
        );
        let mismatched = [
            (final_domain_stream(999, instance, 304), sequence),
            (
                final_domain_stream(303, UniqueId::new(999, 302), 304),
                sequence,
            ),
            (final_domain_stream(303, instance, 999), sequence),
            (stream, ProducerSequence::new(999)),
        ];
        for (other_stream, other_sequence) in mismatched {
            assert_eq!(
                encode_contribution(
                    &contribution,
                    ContributionCodecExpectation::FinalDomain {
                        contract: &contract,
                        stream: other_stream,
                        sequence: other_sequence,
                    },
                    usize::MAX,
                ),
                Err(ContributionCodecError::NonCanonicalPayload)
            );
            assert_eq!(
                decode_contribution(
                    encoded.payload(),
                    encoded.schema_digest(),
                    ContributionCodecExpectation::FinalDomain {
                        contract: &contract,
                        stream: other_stream,
                        sequence: other_sequence,
                    },
                    usize::MAX,
                ),
                Err(ContributionCodecError::NonCanonicalPayload)
            );
        }

        let mut bad_digest = encoded.payload().to_vec();
        bad_digest[HEADER_LEN] ^= 1;
        assert_eq!(
            decode_contribution(
                &bad_digest,
                encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
    }

    #[test]
    fn final_domain_rejects_membership_schema_and_spliced_body_mismatch() {
        let contract = final_domain_contract(&DataType::Int64);
        let utf8_contract = final_domain_contract(&DataType::Utf8);
        let stream = final_domain_stream(403, UniqueId::new(401, 402), 404);
        let sequence = ProducerSequence::new(405);
        let (contribution, encoded) = encode_final_domain(
            &contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::int64([1]), false),
        );
        assert_eq!(
            encode_contribution(
                &contribution,
                ContributionCodecExpectation::FinalDomain {
                    contract: &utf8_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &utf8_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let (_, utf8_encoded) = encode_final_domain(
            &utf8_contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::utf8(["spliced"]), false),
        );
        let mut spliced = encoded.payload().to_vec();
        spliced.truncate(HEADER_LEN + 32);
        spliced.extend_from_slice(&utf8_encoded.payload()[HEADER_LEN + 32..]);
        let body_len = spliced.len() - HEADER_LEN;
        spliced[40..48].copy_from_slice(&(body_len as u64).to_be_bytes());
        assert_eq!(
            decode_contribution(
                &spliced,
                encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );
    }

    #[test]
    fn final_domain_keeps_invalid_schema_metadata_noncanonical() {
        let stream = final_domain_stream(453, UniqueId::new(451, 452), 454);
        let sequence = ProducerSequence::new(455);

        let int_contract = final_domain_contract(&DataType::Int64);
        let (_, int_encoded) = encode_final_domain(
            &int_contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::int64([1]), false),
        );
        let type_tag_offset = HEADER_LEN + 32 + 8 + FINGERPRINT_VERSION_TAG.len();
        let mut invalid_tag = int_encoded.payload().to_vec();
        invalid_tag[type_tag_offset] = 99;
        assert_eq!(
            decode_contribution(
                &invalid_tag,
                int_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &int_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut missing_fence_byte = int_encoded.payload()[..HEADER_LEN + 31].to_vec();
        missing_fence_byte[40..48].copy_from_slice(&31_u64.to_be_bytes());
        assert_eq!(
            decode_contribution(
                &missing_fence_byte,
                int_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &int_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::Truncated)
        );
        let mut malformed_alternate_type = int_encoded.payload().to_vec();
        malformed_alternate_type[type_tag_offset] = 1;
        assert_eq!(
            decode_contribution(
                &malformed_alternate_type,
                int_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &int_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let timestamp_contract =
            final_domain_contract(&DataType::Timestamp(TimeUnit::Second, None));
        let (_, timestamp_encoded) = encode_final_domain(
            &timestamp_contract,
            stream,
            sequence,
            ValueDomainDelta::new(
                MembershipValues::timestamp(TimeUnit::Second, None, [1]),
                false,
            ),
        );
        let mut invalid_unit = timestamp_encoded.payload().to_vec();
        invalid_unit[type_tag_offset + 1] = 99;
        assert_eq!(
            decode_contribution(
                &invalid_unit,
                timestamp_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &timestamp_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut invalid_timezone_marker = timestamp_encoded.payload().to_vec();
        invalid_timezone_marker[type_tag_offset + 2] = 2;
        assert_eq!(
            decode_contribution(
                &invalid_timezone_marker,
                timestamp_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &timestamp_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let decimal_contract = final_domain_contract(&DataType::Decimal128(5, 2));
        let (_, decimal_encoded) = encode_final_domain(
            &decimal_contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::decimal128(5, 2, [1]).unwrap(), false),
        );
        for (precision, scale) in [(0, 2), (5, 39)] {
            let mut invalid_decimal = decimal_encoded.payload().to_vec();
            invalid_decimal[type_tag_offset + 1] = precision;
            invalid_decimal[type_tag_offset + 2] = scale as u8;
            assert_eq!(
                decode_contribution(
                    &invalid_decimal,
                    decimal_encoded.schema_digest(),
                    ContributionCodecExpectation::FinalDomain {
                        contract: &decimal_contract,
                        stream,
                        sequence,
                    },
                    usize::MAX,
                ),
                Err(ContributionCodecError::NonCanonicalPayload)
            );
        }
    }

    #[test]
    fn final_domain_alternate_timestamp_schema_is_validated_without_owned_metadata() {
        let contract = final_domain_contract(&DataType::Timestamp(
            TimeUnit::Millisecond,
            Some(Arc::from("Asia/Shanghai")),
        ));
        let stream = final_domain_stream(483, UniqueId::new(481, 482), 484);
        let sequence = ProducerSequence::new(485);
        let (_, encoded) = encode_final_domain(
            &contract,
            stream,
            sequence,
            ValueDomainDelta::new(
                MembershipValues::timestamp(
                    TimeUnit::Millisecond,
                    Some(Arc::from("Asia/Shanghai")),
                    [1, 2],
                ),
                false,
            ),
        );
        let body = &encoded.payload()[HEADER_LEN + 32..];
        let observer = BorrowedTimezoneObserver::new();

        assert_eq!(
            validate_alternate_membership_body_with_observer(body, &observer),
            Ok(())
        );
        assert_eq!(observer.calls.get(), 1);
        assert_eq!(observer.len.get(), "Asia/Shanghai".len());
        let body_range = body.as_ptr() as usize..body.as_ptr() as usize + body.len();
        assert!(body_range.contains(&observer.pointer.get()));

        let installed_contract = final_domain_contract(&DataType::Int64);
        let (_, installed_encoded) = encode_final_domain(
            &installed_contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::int64([1]), false),
        );
        let mut spliced = installed_encoded.payload().to_vec();
        spliced.truncate(HEADER_LEN + 32);
        spliced.extend_from_slice(body);
        let spliced_body_len = spliced.len() - HEADER_LEN;
        spliced[40..48].copy_from_slice(&(spliced_body_len as u64).to_be_bytes());
        assert_eq!(
            decode_contribution(
                &spliced,
                installed_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &installed_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::SchemaMismatch)
        );

        let mut malformed = body.to_vec();
        let timezone_offset = 8 + FINGERPRINT_VERSION_TAG.len() + 1 + 1 + 1 + 8;
        malformed[timezone_offset] = 0xff;
        assert_eq!(
            validate_alternate_membership_body_with_observer(
                &malformed,
                &BorrowedTimezoneObserver::new(),
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let mut malformed_frame = installed_encoded.payload().to_vec();
        malformed_frame.truncate(HEADER_LEN + 32);
        malformed_frame.extend_from_slice(&malformed);
        let malformed_body_len = malformed_frame.len() - HEADER_LEN;
        malformed_frame[40..48].copy_from_slice(&(malformed_body_len as u64).to_be_bytes());
        assert_eq!(
            decode_contribution(
                &malformed_frame,
                installed_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &installed_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
        let truncated = &body[..timezone_offset + "Asia/Shanghai".len() - 1];
        assert_eq!(
            validate_alternate_membership_body_with_observer(
                truncated,
                &BorrowedTimezoneObserver::new(),
            ),
            Err(ContributionCodecError::Truncated)
        );
        let mut truncated_frame = installed_encoded.payload().to_vec();
        truncated_frame.truncate(HEADER_LEN + 32);
        truncated_frame.extend_from_slice(truncated);
        let truncated_body_len = truncated_frame.len() - HEADER_LEN;
        truncated_frame[40..48].copy_from_slice(&(truncated_body_len as u64).to_be_bytes());
        assert_eq!(
            decode_contribution(
                &truncated_frame,
                installed_encoded.schema_digest(),
                ContributionCodecExpectation::FinalDomain {
                    contract: &installed_contract,
                    stream,
                    sequence,
                },
                usize::MAX,
            ),
            Err(ContributionCodecError::Truncated)
        );
    }

    #[test]
    fn alternate_membership_validator_covers_closed_tags_and_canonical_values() {
        let cases = vec![
            MembershipValues::boolean([false, true]),
            MembershipValues::int8([-1, 1]),
            MembershipValues::int16([-2, 2]),
            MembershipValues::int32([-3, 3]),
            MembershipValues::int64([-4, 4]),
            MembershipValues::large_int([-5, 5]),
            MembershipValues::float32([f32::NEG_INFINITY, 0.0, f32::INFINITY, f32::NAN]),
            MembershipValues::float64([f64::NEG_INFINITY, 0.0, f64::INFINITY, f64::NAN]),
            MembershipValues::utf8(["a", "b"]),
            MembershipValues::date32([-6, 6]),
            MembershipValues::timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")), [-7, 7]),
            MembershipValues::decimal128(5, 2, [-999, 999]).unwrap(),
        ];
        for values in cases {
            let (_, _, encoded) = encode_membership(values, true);
            assert_eq!(
                validate_alternate_membership_body_with_observer(
                    &encoded.payload()[HEADER_LEN..],
                    &NoopAlternateMembershipMetadataObserver,
                ),
                Ok(())
            );
        }

        let (_, _, int32) = encode_membership(MembershipValues::int32([1, 2]), false);
        let mut duplicate = int32.payload()[HEADER_LEN..].to_vec();
        let type_offset = 8 + FINGERPRINT_VERSION_TAG.len();
        let first_value = type_offset + 1 + 8;
        let first_bytes: [u8; 4] = duplicate[first_value..first_value + 4].try_into().unwrap();
        duplicate[first_value + 4..first_value + 8].copy_from_slice(&first_bytes);
        assert_eq!(
            validate_alternate_membership_body_with_observer(
                &duplicate,
                &NoopAlternateMembershipMetadataObserver,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let (_, _, float) = encode_membership(MembershipValues::float32([0.0, f32::NAN]), false);
        let mut noncanonical_float = float.payload()[HEADER_LEN..].to_vec();
        let float_value = type_offset + 1 + 8;
        noncanonical_float[float_value..float_value + 4]
            .copy_from_slice(&(-0.0_f32).to_bits().to_be_bytes());
        assert_eq!(
            validate_alternate_membership_body_with_observer(
                &noncanonical_float,
                &NoopAlternateMembershipMetadataObserver,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );

        let (_, _, decimal) =
            encode_membership(MembershipValues::decimal128(3, 0, [999]).unwrap(), false);
        let mut decimal_overflow = decimal.payload()[HEADER_LEN..].to_vec();
        let decimal_value = type_offset + 1 + 2 + 8;
        decimal_overflow[decimal_value..decimal_value + 16]
            .copy_from_slice(&1000_i128.to_be_bytes());
        assert_eq!(
            validate_alternate_membership_body_with_observer(
                &decimal_overflow,
                &NoopAlternateMembershipMetadataObserver,
            ),
            Err(ContributionCodecError::NonCanonicalPayload)
        );
    }

    #[test]
    fn final_domain_exact_limit_succeeds_and_limit_minus_one_fails() {
        let contract = final_domain_contract(&DataType::Utf8);
        let stream = final_domain_stream(503, UniqueId::new(501, 502), 504);
        let sequence = ProducerSequence::new(505);
        let contribution = RuntimeFilterContribution::FinalDomain(final_domain_shard(
            &contract,
            stream,
            sequence,
            ValueDomainDelta::new(MembershipValues::utf8(["exact"]), true),
        ));
        let expectation = ContributionCodecExpectation::FinalDomain {
            contract: &contract,
            stream,
            sequence,
        };
        let exact = encoded_contribution_len(&contribution, expectation).unwrap();

        assert_eq!(
            encode_contribution(&contribution, expectation, exact - 1),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        let encoded = encode_contribution(&contribution, expectation, exact).unwrap();
        assert_eq!(encoded.payload().len(), exact);
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact - 1,
            ),
            Err(ContributionCodecError::EncodedSizeExceeded)
        );
        assert_eq!(
            decode_contribution(
                encoded.payload(),
                encoded.schema_digest(),
                expectation,
                exact,
            ),
            Ok(contribution)
        );
    }
}
