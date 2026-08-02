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
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, TimeUnit};
use sha2::{Digest, Sha256};

use crate::runtime_filter::model::contract::{ChannelId, NullSemantics};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;

use super::identity::LogicalVersion;
use super::ordered_bound::{
    OrderContractDigest, OrderedScalar, OrderedTuple, RuntimeOrderContract, RuntimeOrderKey,
};
use super::support::ArtifactRetention;

const PROFILE_VERSION: u8 = 1;
const ORDERED_PROFILE_VERSION: u8 = 2;
const SCHEMA_VERSION: u8 = 1;
pub const LEAF_CODEC_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ArtifactKind {
    ValueSet,
    Bloom,
    Bitset,
    Range,
    EmptyDomain,
}

impl ArtifactKind {
    pub const fn tag(self) -> u8 {
        match self {
            Self::ValueSet => 1,
            Self::Bloom => 2,
            Self::Bitset => 3,
            Self::Range => 4,
            Self::EmptyDomain => 5,
        }
    }

    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::ValueSet),
            2 => Some(Self::Bloom),
            3 => Some(Self::Bitset),
            4 => Some(Self::Range),
            5 => Some(Self::EmptyDomain),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HashContractDigest([u8; 32]);

impl HashContractDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerProfileId([u8; 32]);

impl ConsumerProfileId {
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }

    #[cfg(any(test, feature = "runtime-filter-test-support"))]
    pub const fn for_test(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArtifactSchemaDigest([u8; 32]);

impl ArtifactSchemaDigest {
    pub const fn from_canonical_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn for_membership(
        data_type: &DataType,
        null_semantics: NullSemantics,
    ) -> Result<Self, ArtifactContractError> {
        Ok(ArtifactMembershipSchema::new(data_type, null_semantics)?.digest())
    }

    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactMembershipSchema {
    data_type: DataType,
    null_semantics: NullSemantics,
    canonical_bytes: Arc<[u8]>,
    digest: ArtifactSchemaDigest,
}

impl ArtifactMembershipSchema {
    pub fn new(
        data_type: &DataType,
        null_semantics: NullSemantics,
    ) -> Result<Self, ArtifactContractError> {
        let mut canonical = Vec::with_capacity(48);
        canonical.extend_from_slice(b"novarocks.runtime-filter.artifact-schema");
        canonical.push(SCHEMA_VERSION);
        encode_schema(data_type, &mut canonical)?;
        canonical.push(match null_semantics {
            NullSemantics::NeverMatches => 1,
            NullSemantics::NullSafeEqual => 2,
        });
        let digest = ArtifactSchemaDigest(Sha256::digest(&canonical).into());
        Ok(Self {
            data_type: data_type.clone(),
            null_semantics,
            canonical_bytes: canonical.into(),
            digest,
        })
    }

    pub fn view(
        canonical: &[u8],
    ) -> Result<ArtifactMembershipSchemaView<'_>, ArtifactContractError> {
        const DOMAIN: &[u8] = b"novarocks.runtime-filter.artifact-schema";
        let mut cursor = SchemaCursor::new(canonical);
        if cursor.take(DOMAIN.len())? != DOMAIN || cursor.u8()? != SCHEMA_VERSION {
            return Err(ArtifactContractError::NonCanonicalSchema);
        }
        let payload_tag = cursor.u8()?;
        let type_contract = match payload_tag {
            1..=10 => ArtifactMembershipTypeContract::Primitive,
            11 => {
                let unit = match cursor.u8()? {
                    unit @ 1..=4 => unit,
                    _ => return Err(ArtifactContractError::NonCanonicalSchema),
                };
                let timezone = match cursor.u8()? {
                    0 => None,
                    1 => {
                        let len = cursor.u32()? as usize;
                        let timezone = std::str::from_utf8(cursor.take(len)?)
                            .map_err(|_| ArtifactContractError::NonCanonicalSchema)?;
                        Some(timezone)
                    }
                    _ => return Err(ArtifactContractError::NonCanonicalSchema),
                };
                ArtifactMembershipTypeContract::Timestamp { unit, timezone }
            }
            12 => {
                let precision = cursor.u8()?;
                let scale = cursor.u8()? as i8;
                if precision == 0
                    || precision > DECIMAL128_MAX_PRECISION
                    || scale > DECIMAL128_MAX_SCALE
                    || (scale > 0 && scale as u8 > precision)
                {
                    return Err(ArtifactContractError::NonCanonicalSchema);
                }
                ArtifactMembershipTypeContract::Decimal { precision, scale }
            }
            _ => return Err(ArtifactContractError::NonCanonicalSchema),
        };
        let null_semantics = match cursor.u8()? {
            1 => NullSemantics::NeverMatches,
            2 => NullSemantics::NullSafeEqual,
            _ => return Err(ArtifactContractError::NonCanonicalSchema),
        };
        if !cursor.is_empty() {
            return Err(ArtifactContractError::NonCanonicalSchema);
        }
        Ok(ArtifactMembershipSchemaView {
            payload_tag,
            type_contract,
            null_semantics,
            digest: ArtifactSchemaDigest(Sha256::digest(canonical).into()),
        })
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn null_semantics(&self) -> NullSemantics {
        self.null_semantics
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub const fn digest(&self) -> ArtifactSchemaDigest {
        self.digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactMembershipTypeContract<'a> {
    Primitive,
    Timestamp { unit: u8, timezone: Option<&'a str> },
    Decimal { precision: u8, scale: i8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactMembershipSchemaView<'a> {
    payload_tag: u8,
    type_contract: ArtifactMembershipTypeContract<'a>,
    null_semantics: NullSemantics,
    digest: ArtifactSchemaDigest,
}

impl<'a> ArtifactMembershipSchemaView<'a> {
    pub const fn payload_tag(self) -> u8 {
        self.payload_tag
    }

    pub const fn timestamp_contract(self) -> Option<(u8, Option<&'a str>)> {
        match self.type_contract {
            ArtifactMembershipTypeContract::Timestamp { unit, timezone } => Some((unit, timezone)),
            _ => None,
        }
    }

    pub const fn decimal_contract(self) -> Option<(u8, i8)> {
        match self.type_contract {
            ArtifactMembershipTypeContract::Decimal { precision, scale } => {
                Some((precision, scale))
            }
            _ => None,
        }
    }

    pub const fn null_semantics(self) -> NullSemantics {
        self.null_semantics
    }

    pub const fn digest(self) -> ArtifactSchemaDigest {
        self.digest
    }
}

struct SchemaCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> SchemaCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ArtifactContractError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(len)
            .ok_or(ArtifactContractError::NonCanonicalSchema)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, ArtifactContractError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, ArtifactContractError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("four-byte schema field"),
        ))
    }
}

pub(super) fn encode_schema(
    data_type: &DataType,
    output: &mut Vec<u8>,
) -> Result<(), ArtifactContractError> {
    match data_type {
        DataType::Boolean => output.push(1),
        DataType::Int8 => output.push(2),
        DataType::Int16 => output.push(3),
        DataType::Int32 => output.push(4),
        DataType::Int64 => output.push(5),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => output.push(6),
        DataType::Float32 => output.push(7),
        DataType::Float64 => output.push(8),
        DataType::Utf8 => output.push(9),
        DataType::Date32 => output.push(10),
        DataType::Timestamp(unit, timezone) => {
            output.extend_from_slice(&[
                11,
                match unit {
                    TimeUnit::Second => 1,
                    TimeUnit::Millisecond => 2,
                    TimeUnit::Microsecond => 3,
                    TimeUnit::Nanosecond => 4,
                },
            ]);
            match timezone {
                Some(timezone) => {
                    output.push(1);
                    let len = u32::try_from(timezone.len())
                        .map_err(|_| ArtifactContractError::LengthOverflow)?;
                    output.extend_from_slice(&len.to_be_bytes());
                    output.extend_from_slice(timezone.as_bytes());
                }
                None => output.push(0),
            }
        }
        DataType::Decimal128(precision, scale)
            if *precision != 0
                && *precision <= DECIMAL128_MAX_PRECISION
                && *scale <= DECIMAL128_MAX_SCALE
                && (*scale <= 0 || (*scale as u8) <= *precision) =>
        {
            output.extend_from_slice(&[12, *precision, *scale as u8]);
        }
        _ => return Err(ArtifactContractError::UnsupportedSchema),
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerArtifactProfile {
    accepted_kinds: BTreeSet<ArtifactKind>,
    bloom_hash_contract: Option<HashContractDigest>,
    order_contract_digest: Option<OrderContractDigest>,
    canonical_bytes: Arc<[u8]>,
    id: ConsumerProfileId,
}

impl ConsumerArtifactProfile {
    pub fn new(
        accepted_kinds: BTreeSet<ArtifactKind>,
        bloom_hash_contract: Option<HashContractDigest>,
    ) -> Result<Self, ArtifactContractError> {
        Self::new_with_order_contract(accepted_kinds, bloom_hash_contract, None)
    }

    pub fn new_ordered_range(
        order_contract_digest: OrderContractDigest,
    ) -> Result<Self, ArtifactContractError> {
        Self::new_with_order_contract(
            BTreeSet::from([ArtifactKind::Range]),
            None,
            Some(order_contract_digest),
        )
    }

    fn new_with_order_contract(
        accepted_kinds: BTreeSet<ArtifactKind>,
        bloom_hash_contract: Option<HashContractDigest>,
        order_contract_digest: Option<OrderContractDigest>,
    ) -> Result<Self, ArtifactContractError> {
        if accepted_kinds.is_empty() {
            return Err(ArtifactContractError::EmptyProfile);
        }
        if accepted_kinds.contains(&ArtifactKind::Bloom) != bloom_hash_contract.is_some() {
            return Err(ArtifactContractError::BloomHashContractMismatch);
        }
        let count = u16::try_from(accepted_kinds.len())
            .map_err(|_| ArtifactContractError::LengthOverflow)?;
        let mut canonical = Vec::with_capacity(4 + accepted_kinds.len() + 32);
        canonical.extend_from_slice(&[if order_contract_digest.is_some() {
            ORDERED_PROFILE_VERSION
        } else {
            PROFILE_VERSION
        }]);
        canonical.extend_from_slice(&count.to_be_bytes());
        canonical.extend(accepted_kinds.iter().map(|kind| kind.tag()));
        match bloom_hash_contract {
            Some(digest) => {
                canonical.push(1);
                canonical.extend_from_slice(&digest.bytes());
            }
            None => canonical.push(0),
        }
        match order_contract_digest {
            Some(digest) => {
                if !accepted_kinds.contains(&ArtifactKind::Range) {
                    return Err(ArtifactContractError::RangeOrderContractMismatch);
                }
                canonical.push(1);
                canonical.extend_from_slice(&digest.bytes());
            }
            None => {}
        }
        let id = ConsumerProfileId(Sha256::digest(&canonical).into());
        Ok(Self {
            accepted_kinds,
            bloom_hash_contract,
            order_contract_digest,
            canonical_bytes: canonical.into(),
            id,
        })
    }

    #[cfg(any(test, feature = "runtime-filter-test-support"))]
    pub fn m1_test_default() -> Self {
        Self::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .expect("built-in test profile is valid")
    }

    pub const fn accepted_kinds(&self) -> &BTreeSet<ArtifactKind> {
        &self.accepted_kinds
    }

    pub fn accepts(&self, kind: ArtifactKind) -> bool {
        self.accepted_kinds.contains(&kind)
    }

    pub const fn bloom_hash_contract(&self) -> Option<HashContractDigest> {
        self.bloom_hash_contract
    }

    pub const fn order_contract_digest(&self) -> Option<OrderContractDigest> {
        self.order_contract_digest
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub const fn id(&self) -> ConsumerProfileId {
        self.id
    }

    #[cfg(any(test, feature = "runtime-filter-test-support"))]
    pub fn with_test_identity(mut self, id: ConsumerProfileId) -> Self {
        self.id = id;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactContractError {
    EmptyProfile,
    BloomHashContractMismatch,
    RangeOrderContractMismatch,
    UnsupportedSchema,
    NonCanonicalSchema,
    LengthOverflow,
    EmptyBundle,
    DuplicateKind,
    KindNotAccepted,
    KindMismatch,
    VersionMismatch,
    SchemaMismatch,
    EncodedSizeOverflow,
    EncodedSizeExceeded,
    RetentionSizeMismatch,
    ResidentSizeOverflow,
    InvalidMembershipIndex,
}

impl fmt::Display for ArtifactContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid runtime filter artifact contract: {self:?}"
        )
    }
}

impl Error for ArtifactContractError {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ArtifactSemanticDigest([u8; 32]);

impl ArtifactSemanticDigest {
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeArtifactData {
    contract: Arc<RuntimeOrderContract>,
    bound: OrderedTuple,
    semantic_digest: ArtifactSemanticDigest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeArtifactResidentLayout {
    pub key_count: usize,
    pub timezone_count: usize,
    pub timezone_bytes: usize,
    pub tuple_arity: usize,
    pub utf8_count: usize,
    pub utf8_bytes: usize,
}

impl RangeArtifactResidentLayout {
    pub fn from_data(
        contract: &RuntimeOrderContract,
        bound: &OrderedTuple,
    ) -> Result<Self, ArtifactContractError> {
        let (timezone_count, timezone_bytes) =
            contract
                .keys()
                .iter()
                .try_fold((0usize, 0usize), |(count, bytes), key| {
                    match key.data_type() {
                        DataType::Timestamp(_, Some(timezone)) => Ok((
                            count
                                .checked_add(1)
                                .ok_or(ArtifactContractError::ResidentSizeOverflow)?,
                            bytes
                                .checked_add(timezone.len())
                                .ok_or(ArtifactContractError::ResidentSizeOverflow)?,
                        )),
                        _ => Ok((count, bytes)),
                    }
                })?;
        let (utf8_count, utf8_bytes) = bound.values().iter().try_fold(
            (0usize, 0usize),
            |(count, bytes), value| match value {
                Some(OrderedScalar::Utf8(value)) => Ok((
                    count
                        .checked_add(1)
                        .ok_or(ArtifactContractError::ResidentSizeOverflow)?,
                    bytes
                        .checked_add(value.len())
                        .ok_or(ArtifactContractError::ResidentSizeOverflow)?,
                )),
                _ => Ok((count, bytes)),
            },
        )?;
        Ok(Self {
            key_count: contract.keys().len(),
            timezone_count,
            timezone_bytes,
            tuple_arity: bound.values().len(),
            utf8_count,
            utf8_bytes,
        })
    }

    pub fn decode_temporary_bytes(self) -> Result<usize, ArtifactContractError> {
        self.key_count
            .checked_mul(size_of::<RuntimeOrderKey>())
            .and_then(|bytes| {
                self.tuple_arity
                    .checked_mul(size_of::<Option<OrderedScalar>>())
                    .and_then(|tuple| bytes.checked_add(tuple))
            })
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }
}

impl RangeArtifactData {
    pub fn new(
        contract: Arc<RuntimeOrderContract>,
        bound: OrderedTuple,
        logical_version: LogicalVersion,
    ) -> Result<Self, ArtifactContractError> {
        contract
            .compare(&bound, &bound)
            .map_err(|_| ArtifactContractError::SchemaMismatch)?;
        let mut semantic = Sha256::new();
        semantic.update(b"novarocks.runtime-filter.range-semantic");
        semantic.update([1]);
        semantic.update(logical_version.get().to_be_bytes());
        semantic.update(contract.digest().bytes());
        semantic.update((bound.values().len() as u64).to_be_bytes());
        for value in bound.values() {
            match value {
                None => semantic.update([0]),
                Some(value) => {
                    semantic.update([1]);
                    hash_ordered_scalar(&mut semantic, value);
                }
            }
        }
        Ok(Self {
            contract,
            bound,
            semantic_digest: ArtifactSemanticDigest(semantic.finalize().into()),
        })
    }

    pub const fn contract(&self) -> &Arc<RuntimeOrderContract> {
        &self.contract
    }

    pub const fn bound(&self) -> &OrderedTuple {
        &self.bound
    }

    pub const fn semantic_digest(&self) -> ArtifactSemanticDigest {
        self.semantic_digest
    }

    pub fn accounted_resident_bytes(&self) -> Result<usize, ArtifactContractError> {
        Self::accounted_resident_bytes_for_layout(RangeArtifactResidentLayout::from_data(
            &self.contract,
            &self.bound,
        )?)
    }

    pub fn accounted_resident_bytes_for_layout(
        layout: RangeArtifactResidentLayout,
    ) -> Result<usize, ArtifactContractError> {
        let arc_header = 2usize
            .checked_mul(size_of::<usize>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
        size_of::<Self>()
            .checked_add(arc_header)
            .and_then(|bytes| bytes.checked_add(size_of::<RuntimeOrderContract>()))
            .and_then(|bytes| bytes.checked_add(arc_header))
            .and_then(|bytes| {
                layout
                    .key_count
                    .checked_mul(size_of::<RuntimeOrderKey>())
                    .and_then(|keys| bytes.checked_add(keys))
            })
            .and_then(|bytes| bytes.checked_add(arc_header))
            .and_then(|bytes| {
                layout
                    .timezone_count
                    .checked_mul(arc_header)
                    .and_then(|headers| bytes.checked_add(headers))
            })
            .and_then(|bytes| bytes.checked_add(layout.timezone_bytes))
            .and_then(|bytes| {
                layout
                    .tuple_arity
                    .checked_mul(size_of::<Option<OrderedScalar>>())
                    .and_then(|values| bytes.checked_add(values))
            })
            .and_then(|bytes| bytes.checked_add(arc_header))
            .and_then(|bytes| {
                layout
                    .utf8_count
                    .checked_mul(arc_header)
                    .and_then(|headers| bytes.checked_add(headers))
            })
            .and_then(|bytes| bytes.checked_add(layout.utf8_bytes))
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }
}

fn hash_ordered_scalar(digest: &mut Sha256, value: &OrderedScalar) {
    match value {
        OrderedScalar::Boolean(value) => digest.update([1, u8::from(*value)]),
        OrderedScalar::Int8(value) => digest.update([2, *value as u8]),
        OrderedScalar::Int16(value) => {
            digest.update([3]);
            digest.update(value.to_be_bytes());
        }
        OrderedScalar::Int32(value) => {
            digest.update([4]);
            digest.update(value.to_be_bytes());
        }
        OrderedScalar::Int64(value) => {
            digest.update([5]);
            digest.update(value.to_be_bytes());
        }
        OrderedScalar::LargeInt(value) => {
            digest.update([6]);
            digest.update(value.to_be_bytes());
        }
        OrderedScalar::Utf8(value) => {
            digest.update([9]);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        OrderedScalar::Date32(value) => {
            digest.update([10]);
            digest.update(value.to_be_bytes());
        }
        OrderedScalar::Timestamp(value) => {
            digest.update([11]);
            digest.update(value.to_be_bytes());
        }
        OrderedScalar::Decimal128(value) => {
            digest.update([12]);
            digest.update(value.to_be_bytes());
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidentMembershipIndex {
    kind: ArtifactKind,
    canonical_digest: [u8; 32],
    layout: ResidentMembershipIndexLayout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResidentMembershipIndexLayout {
    EmptyDomain,
    Fixed {
        tag: u8,
        values: Range<usize>,
        count: usize,
        width: usize,
    },
    Utf8 {
        payload: Range<usize>,
        length_offsets: Box<[usize]>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum ResidentMembershipIndexView<'a> {
    EmptyDomain,
    Fixed {
        tag: u8,
        values: &'a Range<usize>,
        count: usize,
        width: usize,
    },
    Utf8 {
        payload: &'a Range<usize>,
        length_offsets: &'a [usize],
    },
}

impl ResidentMembershipIndex {
    pub fn empty_domain(canonical_bytes: &[u8]) -> Self {
        Self {
            kind: ArtifactKind::EmptyDomain,
            canonical_digest: Sha256::digest(canonical_bytes).into(),
            layout: ResidentMembershipIndexLayout::EmptyDomain,
        }
    }

    pub fn fixed(
        canonical_bytes: &[u8],
        tag: u8,
        values: Range<usize>,
        count: usize,
        width: usize,
    ) -> Self {
        Self {
            kind: ArtifactKind::ValueSet,
            canonical_digest: Sha256::digest(canonical_bytes).into(),
            layout: ResidentMembershipIndexLayout::Fixed {
                tag,
                values,
                count,
                width,
            },
        }
    }

    pub fn utf8(
        canonical_bytes: &[u8],
        payload: Range<usize>,
        length_offsets: Box<[usize]>,
    ) -> Self {
        Self {
            kind: ArtifactKind::ValueSet,
            canonical_digest: Sha256::digest(canonical_bytes).into(),
            layout: ResidentMembershipIndexLayout::Utf8 {
                payload,
                length_offsets,
            },
        }
    }

    pub fn view(&self) -> ResidentMembershipIndexView<'_> {
        match &self.layout {
            ResidentMembershipIndexLayout::EmptyDomain => ResidentMembershipIndexView::EmptyDomain,
            ResidentMembershipIndexLayout::Fixed {
                tag,
                values,
                count,
                width,
            } => ResidentMembershipIndexView::Fixed {
                tag: *tag,
                values,
                count: *count,
                width: *width,
            },
            ResidentMembershipIndexLayout::Utf8 {
                payload,
                length_offsets,
            } => ResidentMembershipIndexView::Utf8 {
                payload,
                length_offsets,
            },
        }
    }

    pub fn heap_bytes(&self) -> Result<usize, ArtifactContractError> {
        match &self.layout {
            ResidentMembershipIndexLayout::Utf8 { length_offsets, .. } => length_offsets
                .len()
                .checked_mul(size_of::<usize>())
                .ok_or(ArtifactContractError::ResidentSizeOverflow),
            ResidentMembershipIndexLayout::EmptyDomain
            | ResidentMembershipIndexLayout::Fixed { .. } => Ok(0),
        }
    }

    fn validate_binding(
        &self,
        kind: ArtifactKind,
        canonical_bytes: &[u8],
    ) -> Result<(), ArtifactContractError> {
        if self.kind != kind
            || self.canonical_digest != <[u8; 32]>::from(Sha256::digest(canonical_bytes))
        {
            return Err(ArtifactContractError::InvalidMembershipIndex);
        }
        crate::runtime_filter::materializer::codec::validate_membership_index_binding(
            canonical_bytes,
            kind,
            self,
        )
        .map_err(|_| ArtifactContractError::InvalidMembershipIndex)
    }
}

#[derive(Clone)]
pub enum PhysicalArtifactPayload {
    Membership(Option<ResidentMembershipIndex>),
    Range(Arc<RangeArtifactData>),
}

pub struct PhysicalArtifact {
    kind: ArtifactKind,
    codec_version: u16,
    schema_digest: ArtifactSchemaDigest,
    version: LogicalVersion,
    contains_null: bool,
    canonical_bytes: Arc<[u8]>,
    canonical_digest: [u8; 32],
    payload: PhysicalArtifactPayload,
    retained_memory: Option<Arc<ArtifactRetention>>,
}

impl PhysicalArtifact {
    pub fn accounted_resident_component_bytes(
        encoded_bytes: usize,
    ) -> Result<usize, ArtifactContractError> {
        encoded_bytes
            .checked_add(size_of::<Self>())
            .and_then(|bytes| bytes.checked_add(size_of::<Arc<[u8]>>()))
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub fn accounted_resident_bytes(encoded_bytes: usize) -> Result<usize, ArtifactContractError> {
        Self::accounted_resident_component_bytes(encoded_bytes)?
            .checked_add(size_of::<ArtifactRetention>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub fn accounted_indexed_resident_component_bytes(
        encoded_bytes: usize,
        index_heap_bytes: usize,
    ) -> Result<usize, ArtifactContractError> {
        Self::accounted_resident_component_bytes(encoded_bytes)?
            .checked_add(index_heap_bytes)
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub fn accounted_indexed_resident_bytes(
        encoded_bytes: usize,
        index_heap_bytes: usize,
    ) -> Result<usize, ArtifactContractError> {
        Self::accounted_indexed_resident_component_bytes(encoded_bytes, index_heap_bytes)?
            .checked_add(size_of::<ArtifactRetention>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub fn from_retained_bytes(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
        accounted_resident_bytes: usize,
        retained_memory: ArtifactRetention,
    ) -> Result<Self, ArtifactContractError> {
        if accounted_resident_bytes != Self::accounted_resident_bytes(canonical_bytes.len())?
            || retained_memory.bytes() != accounted_resident_bytes
            || retained_memory.budget_bytes() != accounted_resident_bytes
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Ok(Self {
            kind,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            payload: PhysicalArtifactPayload::Membership(None),
            retained_memory: Some(Arc::new(retained_memory)),
        })
    }

    pub fn from_shared_retained_bytes(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
        accounted_resident_component_bytes: usize,
        total_accounted_resident_bytes: usize,
        retained_memory: Arc<ArtifactRetention>,
    ) -> Result<Self, ArtifactContractError> {
        if accounted_resident_component_bytes
            != Self::accounted_resident_component_bytes(canonical_bytes.len())?
            || accounted_resident_component_bytes > total_accounted_resident_bytes
            || retained_memory.bytes() != total_accounted_resident_bytes
            || retained_memory.budget_bytes() != total_accounted_resident_bytes
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Ok(Self {
            kind,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            payload: PhysicalArtifactPayload::Membership(None),
            retained_memory: Some(retained_memory),
        })
    }

    #[cfg(any(test, feature = "runtime-filter-test-support"))]
    pub fn new_test(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
    ) -> Self {
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Self {
            kind,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            payload: PhysicalArtifactPayload::Membership(None),
            retained_memory: None,
        }
    }

    #[cfg(test)]
    pub fn clone_with_test_codec_version(&self, codec_version: u16) -> Self {
        Self {
            kind: self.kind,
            codec_version,
            schema_digest: self.schema_digest,
            version: self.version,
            contains_null: self.contains_null,
            canonical_bytes: self.canonical_bytes.clone(),
            canonical_digest: self.canonical_digest,
            payload: self.payload.clone(),
            retained_memory: self.retained_memory.clone(),
        }
    }

    pub fn from_indexed_retained_bytes(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
        index: ResidentMembershipIndex,
        accounted_resident_bytes: usize,
        retained_memory: ArtifactRetention,
    ) -> Result<Self, ArtifactContractError> {
        index.validate_binding(kind, &canonical_bytes)?;
        let index_heap_bytes = index.heap_bytes()?;
        if accounted_resident_bytes
            != Self::accounted_indexed_resident_bytes(canonical_bytes.len(), index_heap_bytes)?
            || retained_memory.bytes() != accounted_resident_bytes
            || retained_memory.budget_bytes() != accounted_resident_bytes
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Ok(Self {
            kind,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            payload: PhysicalArtifactPayload::Membership(Some(index)),
            retained_memory: Some(Arc::new(retained_memory)),
        })
    }

    pub fn from_shared_indexed_retained_bytes(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
        index: ResidentMembershipIndex,
        accounted_resident_component_bytes: usize,
        total_accounted_resident_bytes: usize,
        retained_memory: Arc<ArtifactRetention>,
    ) -> Result<Self, ArtifactContractError> {
        index.validate_binding(kind, &canonical_bytes)?;
        let index_heap_bytes = index.heap_bytes()?;
        if accounted_resident_component_bytes
            != Self::accounted_indexed_resident_component_bytes(
                canonical_bytes.len(),
                index_heap_bytes,
            )?
            || accounted_resident_component_bytes > total_accounted_resident_bytes
            || retained_memory.bytes() != total_accounted_resident_bytes
            || retained_memory.budget_bytes() != total_accounted_resident_bytes
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Ok(Self {
            kind,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            payload: PhysicalArtifactPayload::Membership(Some(index)),
            retained_memory: Some(retained_memory),
        })
    }

    #[cfg(test)]
    pub fn new_indexed_test(
        kind: ArtifactKind,
        schema_digest: ArtifactSchemaDigest,
        version: LogicalVersion,
        contains_null: bool,
        canonical_bytes: Arc<[u8]>,
        index: ResidentMembershipIndex,
    ) -> Result<Self, ArtifactContractError> {
        index.validate_binding(kind, &canonical_bytes)?;
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Ok(Self {
            kind,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            payload: PhysicalArtifactPayload::Membership(Some(index)),
            retained_memory: None,
        })
    }

    pub fn accounted_range_resident_component_bytes(
        encoded_bytes: usize,
        data: &RangeArtifactData,
    ) -> Result<usize, ArtifactContractError> {
        Self::accounted_range_resident_component_bytes_for_layout(
            encoded_bytes,
            RangeArtifactResidentLayout::from_data(data.contract(), data.bound())?,
        )
    }

    pub fn accounted_range_resident_component_bytes_for_layout(
        encoded_bytes: usize,
        layout: RangeArtifactResidentLayout,
    ) -> Result<usize, ArtifactContractError> {
        let arc_header = 2usize
            .checked_mul(size_of::<usize>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
        size_of::<Self>()
            .checked_add(arc_header)
            .and_then(|bytes| bytes.checked_add(encoded_bytes))
            .and_then(|bytes| bytes.checked_add(arc_header))
            .and_then(|bytes| {
                RangeArtifactData::accounted_resident_bytes_for_layout(layout)
                    .ok()
                    .and_then(|data| bytes.checked_add(data))
            })
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub fn from_range_retained(
        version: LogicalVersion,
        data: RangeArtifactData,
        canonical_bytes: Arc<[u8]>,
        retained_memory: ArtifactRetention,
    ) -> Result<Self, ArtifactContractError> {
        let accounted =
            Self::accounted_range_resident_component_bytes(canonical_bytes.len(), &data)?
                .checked_add(size_of::<ArtifactRetention>())
                .and_then(|bytes| bytes.checked_add(2 * size_of::<usize>()))
                .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
        if retained_memory.bytes() != accounted || retained_memory.budget_bytes() != accounted {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        Self::from_range_shared_retained(
            version,
            data,
            canonical_bytes,
            accounted - size_of::<ArtifactRetention>() - 2 * size_of::<usize>(),
            accounted,
            Arc::new(retained_memory),
        )
    }

    pub fn from_range_shared_retained(
        version: LogicalVersion,
        data: RangeArtifactData,
        canonical_bytes: Arc<[u8]>,
        accounted_component_bytes: usize,
        total_accounted_bytes: usize,
        retained_memory: Arc<ArtifactRetention>,
    ) -> Result<Self, ArtifactContractError> {
        if accounted_component_bytes
            != Self::accounted_range_resident_component_bytes(canonical_bytes.len(), &data)?
            || accounted_component_bytes > total_accounted_bytes
            || retained_memory.bytes() != total_accounted_bytes
            || retained_memory.budget_bytes() != total_accounted_bytes
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        let schema_digest =
            ArtifactSchemaDigest::from_canonical_bytes(data.contract().digest().bytes());
        let contains_null = data.bound().values().iter().any(Option::is_none);
        let canonical_digest = Sha256::digest(&canonical_bytes).into();
        Ok(Self {
            kind: ArtifactKind::Range,
            codec_version: LEAF_CODEC_VERSION,
            schema_digest,
            version,
            contains_null,
            canonical_bytes,
            canonical_digest,
            payload: PhysicalArtifactPayload::Range(Arc::new(data)),
            retained_memory: Some(retained_memory),
        })
    }

    pub const fn kind(&self) -> ArtifactKind {
        self.kind
    }
    pub const fn codec_version(&self) -> u16 {
        self.codec_version
    }
    pub const fn schema_digest(&self) -> ArtifactSchemaDigest {
        self.schema_digest
    }
    pub const fn version(&self) -> LogicalVersion {
        self.version
    }
    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
    pub const fn canonical_digest(&self) -> [u8; 32] {
        self.canonical_digest
    }
    pub fn range(&self) -> Option<&RangeArtifactData> {
        match &self.payload {
            PhysicalArtifactPayload::Membership(_) => None,
            PhysicalArtifactPayload::Range(data) => Some(data),
        }
    }
    pub fn membership_index(&self) -> Option<&ResidentMembershipIndex> {
        match &self.payload {
            PhysicalArtifactPayload::Membership(index) => index.as_ref(),
            PhysicalArtifactPayload::Range(_) => None,
        }
    }
    pub fn retained_memory_bytes(&self) -> usize {
        self.retained_memory
            .as_ref()
            .map_or(0, |retention| retention.bytes())
    }

    fn shares_retention(&self, retention: &Arc<ArtifactRetention>) -> bool {
        self.retained_memory
            .as_ref()
            .is_some_and(|owned| Arc::ptr_eq(owned, retention))
    }

    fn accounted_component_bytes(&self) -> Result<usize, ArtifactContractError> {
        match &self.payload {
            PhysicalArtifactPayload::Membership(index) => {
                Self::accounted_indexed_resident_component_bytes(
                    self.canonical_bytes.len(),
                    index
                        .as_ref()
                        .map_or(Ok(0), ResidentMembershipIndex::heap_bytes)?,
                )
            }
            PhysicalArtifactPayload::Range(data) => {
                Self::accounted_range_resident_component_bytes(self.canonical_bytes.len(), data)
            }
        }
    }
}

impl fmt::Debug for PhysicalArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PhysicalArtifact")
            .field("kind", &self.kind)
            .field("codec_version", &self.codec_version)
            .field("schema_digest", &self.schema_digest)
            .field("version", &self.version)
            .field("contains_null", &self.contains_null)
            .field("canonical_bytes", &self.canonical_bytes.len())
            .field("canonical_digest", &self.canonical_digest)
            .field("retained_memory_bytes", &self.retained_memory_bytes())
            .finish()
    }
}

#[derive(Debug)]
pub struct ArtifactBundle {
    channel_id: ChannelId,
    version: LogicalVersion,
    profile_id: ConsumerProfileId,
    artifacts: Box<[(ArtifactKind, Arc<PhysicalArtifact>)]>,
    canonical_digest: [u8; 32],
    encoded_bytes: usize,
    retained_memory: Option<Arc<ArtifactRetention>>,
}

impl ArtifactBundle {
    const CANONICAL_HEADER_BYTES: usize = 4 + 1 + 8 + 8 + 32 + 2;

    pub fn canonical_encoded_len(
        artifacts: &[(ArtifactKind, Arc<PhysicalArtifact>)],
    ) -> Result<usize, ArtifactContractError> {
        u16::try_from(artifacts.len()).map_err(|_| ArtifactContractError::EncodedSizeOverflow)?;
        artifacts
            .iter()
            .try_fold(Self::CANONICAL_HEADER_BYTES, |encoded, (_, artifact)| {
                u64::try_from(artifact.canonical_bytes().len())
                    .map_err(|_| ArtifactContractError::EncodedSizeOverflow)?;
                encoded
                    .checked_add(1 + 8)
                    .and_then(|encoded| encoded.checked_add(artifact.canonical_bytes().len()))
                    .ok_or(ArtifactContractError::EncodedSizeOverflow)
            })
    }

    pub fn canonical_encoded_len_for_single_artifact(
        artifact_encoded_bytes: usize,
    ) -> Result<usize, ArtifactContractError> {
        Self::CANONICAL_HEADER_BYTES
            .checked_add(1 + 8)
            .and_then(|bytes| bytes.checked_add(artifact_encoded_bytes))
            .ok_or(ArtifactContractError::EncodedSizeOverflow)
    }

    pub fn accounted_resident_overhead(
        profile: &ConsumerArtifactProfile,
        artifact_count: usize,
    ) -> Result<usize, ArtifactContractError> {
        let refs = artifact_count
            .checked_mul(size_of::<(ArtifactKind, Arc<PhysicalArtifact>)>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
        size_of::<Self>()
            .checked_add(profile.canonical_bytes().len())
            .and_then(|bytes| bytes.checked_add(refs))
            .and_then(|bytes| bytes.checked_add(size_of::<ArtifactRetention>()))
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub fn accounted_range_resident_overhead(
        artifact_count: usize,
    ) -> Result<usize, ArtifactContractError> {
        let arc_header = 2usize
            .checked_mul(size_of::<usize>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
        let refs = artifact_count
            .checked_mul(size_of::<(ArtifactKind, Arc<PhysicalArtifact>)>())
            .ok_or(ArtifactContractError::ResidentSizeOverflow)?;
        size_of::<Self>()
            .checked_add(arc_header)
            .and_then(|bytes| bytes.checked_add(refs))
            .and_then(|bytes| bytes.checked_add(size_of::<ArtifactRetention>()))
            .and_then(|bytes| bytes.checked_add(arc_header))
            .ok_or(ArtifactContractError::ResidentSizeOverflow)
    }

    pub fn new(
        channel_id: ChannelId,
        version: LogicalVersion,
        profile: &ConsumerArtifactProfile,
        artifacts: Vec<(ArtifactKind, Arc<PhysicalArtifact>)>,
        max_artifact_bytes: usize,
    ) -> Result<Self, ArtifactContractError> {
        Self::new_inner(
            channel_id,
            version,
            profile,
            artifacts,
            max_artifact_bytes,
            None,
        )
    }

    pub fn new_retained(
        channel_id: ChannelId,
        version: LogicalVersion,
        profile: &ConsumerArtifactProfile,
        artifacts: Vec<(ArtifactKind, Arc<PhysicalArtifact>)>,
        max_artifact_bytes: usize,
        retained_memory: Arc<ArtifactRetention>,
    ) -> Result<Self, ArtifactContractError> {
        let overhead = if artifacts
            .iter()
            .all(|(_, artifact)| artifact.kind() == ArtifactKind::Range)
        {
            Self::accounted_range_resident_overhead(artifacts.len())?
        } else {
            Self::accounted_resident_overhead(profile, artifacts.len())?
        };
        let expected = artifacts
            .iter()
            .try_fold(overhead, |bytes, (_, artifact)| {
                bytes
                    .checked_add(artifact.accounted_component_bytes()?)
                    .ok_or(ArtifactContractError::ResidentSizeOverflow)
            })?;
        if retained_memory.bytes() != expected || retained_memory.budget_bytes() != expected {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        if artifacts
            .iter()
            .any(|(_, artifact)| !artifact.shares_retention(&retained_memory))
        {
            return Err(ArtifactContractError::RetentionSizeMismatch);
        }
        Self::new_inner(
            channel_id,
            version,
            profile,
            artifacts,
            max_artifact_bytes,
            Some(retained_memory),
        )
    }

    fn new_inner(
        channel_id: ChannelId,
        version: LogicalVersion,
        profile: &ConsumerArtifactProfile,
        mut artifacts: Vec<(ArtifactKind, Arc<PhysicalArtifact>)>,
        max_artifact_bytes: usize,
        retained_memory: Option<Arc<ArtifactRetention>>,
    ) -> Result<Self, ArtifactContractError> {
        if artifacts.is_empty() {
            return Err(ArtifactContractError::EmptyBundle);
        }
        artifacts.sort_unstable_by_key(|(kind, _)| *kind);
        let mut schema = None;
        let count = u16::try_from(artifacts.len())
            .map_err(|_| ArtifactContractError::EncodedSizeOverflow)?;
        for (index, (kind, artifact)) in artifacts.iter().enumerate() {
            if index != 0 && artifacts[index - 1].0 == *kind {
                return Err(ArtifactContractError::DuplicateKind);
            }
            if !profile.accepts(*kind) {
                return Err(ArtifactContractError::KindNotAccepted);
            }
            if artifact.kind() != *kind {
                return Err(ArtifactContractError::KindMismatch);
            }
            if artifact.version() != version {
                return Err(ArtifactContractError::VersionMismatch);
            }
            if schema
                .replace(artifact.schema_digest())
                .is_some_and(|old| old != artifact.schema_digest())
            {
                return Err(ArtifactContractError::SchemaMismatch);
            }
            u64::try_from(artifact.canonical_bytes().len())
                .map_err(|_| ArtifactContractError::EncodedSizeOverflow)?;
        }
        let encoded_bytes = Self::canonical_encoded_len(&artifacts)?;
        if encoded_bytes > max_artifact_bytes {
            return Err(ArtifactContractError::EncodedSizeExceeded);
        }
        let mut canonical = Sha256::new();
        canonical.update(b"NRFB");
        canonical.update([1]);
        canonical.update(channel_id.get().to_be_bytes());
        canonical.update(version.get().to_be_bytes());
        canonical.update(profile.id().bytes());
        canonical.update(count.to_be_bytes());
        for (kind, artifact) in &artifacts {
            canonical.update([kind.tag()]);
            canonical.update(
                u64::try_from(artifact.canonical_bytes().len())
                    .expect("artifact length was checked before hashing")
                    .to_be_bytes(),
            );
            canonical.update(artifact.canonical_bytes());
        }
        let canonical_digest = canonical.finalize().into();
        Ok(Self {
            channel_id,
            version,
            profile_id: profile.id(),
            artifacts: artifacts.into_boxed_slice(),
            canonical_digest,
            encoded_bytes,
            retained_memory,
        })
    }

    pub const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }
    pub const fn version(&self) -> LogicalVersion {
        self.version
    }
    pub const fn profile_id(&self) -> ConsumerProfileId {
        self.profile_id
    }
    pub const fn artifacts(&self) -> &[(ArtifactKind, Arc<PhysicalArtifact>)] {
        &self.artifacts
    }
    pub const fn canonical_digest(&self) -> [u8; 32] {
        self.canonical_digest
    }
    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    pub fn retained_memory_bytes(&self) -> usize {
        self.retained_memory
            .as_ref()
            .map_or(0, |retention| retention.bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::mem::size_of;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::{ArtifactCapability, ChannelId, NullSemantics};
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactRetention, MemoryAccountError, RuntimeFilterMemoryAccount,
    };

    use super::{
        ArtifactBundle, ArtifactContractError, ArtifactKind, ArtifactSchemaDigest,
        ConsumerArtifactProfile, PhysicalArtifact, ResidentMembershipIndex,
    };

    struct AcceptingMemoryAccount;

    impl RuntimeFilterMemoryAccount for AcceptingMemoryAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    #[test]
    fn indexed_artifact_constructor_revalidates_kind_digest_and_layout_binding() {
        let bytes: Arc<[u8]> = Arc::from([0_u8; 8]);
        let digest = ArtifactSchemaDigest::from_canonical_bytes([1; 32]);
        let version = LogicalVersion::FIRST;

        let wrong_kind = ResidentMembershipIndex::fixed(&bytes, 5, 0..8, 1, 8);
        assert_eq!(
            PhysicalArtifact::new_indexed_test(
                ArtifactKind::EmptyDomain,
                digest,
                version,
                false,
                bytes.clone(),
                wrong_kind,
            )
            .unwrap_err(),
            ArtifactContractError::InvalidMembershipIndex
        );

        let wrong_layout = ResidentMembershipIndex::fixed(&bytes, 5, 0..8, 2, 8);
        assert_eq!(
            PhysicalArtifact::new_indexed_test(
                ArtifactKind::ValueSet,
                digest,
                version,
                false,
                bytes.clone(),
                wrong_layout,
            )
            .unwrap_err(),
            ArtifactContractError::InvalidMembershipIndex
        );

        let stale_digest = ResidentMembershipIndex::fixed(&bytes, 5, 0..8, 1, 8);
        let mutated: Arc<[u8]> = Arc::from([1_u8; 8]);
        assert_eq!(
            PhysicalArtifact::new_indexed_test(
                ArtifactKind::ValueSet,
                digest,
                version,
                false,
                mutated,
                stale_digest,
            )
            .unwrap_err(),
            ArtifactContractError::InvalidMembershipIndex
        );
    }

    #[test]
    fn semantic_capability_and_physical_kind_remain_distinct() {
        let semantics = BTreeSet::from([
            ArtifactCapability::Membership,
            ArtifactCapability::EmptyDomain,
        ]);
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();

        assert!(semantics.contains(&ArtifactCapability::Membership));
        assert!(profile.accepts(ArtifactKind::ValueSet));
    }

    #[test]
    fn normalized_profile_is_order_independent_and_digest_stable() {
        let left = ConsumerArtifactProfile::new(
            [ArtifactKind::EmptyDomain, ArtifactKind::ValueSet]
                .into_iter()
                .collect(),
            None,
        )
        .unwrap();
        let right = ConsumerArtifactProfile::new(
            [ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]
                .into_iter()
                .collect(),
            None,
        )
        .unwrap();

        assert_eq!(left.canonical_bytes(), right.canonical_bytes());
        assert_eq!(left.id(), right.id());
    }

    #[test]
    fn membership_profile_preserves_v1_canonical_identity() {
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap();

        assert_eq!(
            profile.canonical_bytes(),
            &[
                super::PROFILE_VERSION,
                0,
                1,
                ArtifactKind::ValueSet.tag(),
                0,
            ]
        );
    }

    #[test]
    fn bundle_keeps_channel_version_profile_and_only_accepted_kinds() {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let artifact = Arc::new(PhysicalArtifact::new_test(
            ArtifactKind::ValueSet,
            schema,
            LogicalVersion::FIRST,
            false,
            Arc::from([1_u8, 2, 3]),
        ));
        let bundle = ArtifactBundle::new(
            ChannelId::new(7),
            LogicalVersion::FIRST,
            &profile,
            vec![(ArtifactKind::ValueSet, artifact)],
            1024,
        )
        .unwrap();

        assert_eq!(bundle.channel_id(), ChannelId::new(7));
        assert_eq!(bundle.version(), LogicalVersion::FIRST);
        assert_eq!(bundle.profile_id(), profile.id());
        assert_eq!(bundle.artifacts().len(), 1);
        assert_eq!(bundle.artifacts()[0].0, ArtifactKind::ValueSet);
        assert_eq!(
            bundle.encoded_bytes(),
            ArtifactBundle::canonical_encoded_len(bundle.artifacts()).unwrap()
        );
    }

    #[test]
    fn bundle_rejects_duplicate_unaccepted_mismatched_and_over_budget_artifacts() {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let value_set = Arc::new(PhysicalArtifact::new_test(
            ArtifactKind::ValueSet,
            schema,
            LogicalVersion::FIRST,
            false,
            Arc::from([1_u8]),
        ));
        assert_eq!(
            ArtifactBundle::new(
                ChannelId::new(7),
                LogicalVersion::FIRST,
                &profile,
                vec![
                    (ArtifactKind::ValueSet, value_set.clone()),
                    (ArtifactKind::ValueSet, value_set.clone()),
                ],
                1024,
            )
            .unwrap_err(),
            ArtifactContractError::DuplicateKind
        );
        assert_eq!(
            ArtifactBundle::new(
                ChannelId::new(7),
                LogicalVersion::FIRST,
                &profile,
                vec![(ArtifactKind::Bloom, value_set.clone())],
                1024,
            )
            .unwrap_err(),
            ArtifactContractError::KindNotAccepted
        );
        assert_eq!(
            ArtifactBundle::new(
                ChannelId::new(7),
                LogicalVersion::new(2),
                &profile,
                vec![(ArtifactKind::ValueSet, value_set.clone())],
                1024,
            )
            .unwrap_err(),
            ArtifactContractError::VersionMismatch
        );
        assert_eq!(
            ArtifactBundle::new(
                ChannelId::new(7),
                LogicalVersion::FIRST,
                &profile,
                vec![(ArtifactKind::ValueSet, value_set)],
                1,
            )
            .unwrap_err(),
            ArtifactContractError::EncodedSizeExceeded
        );
    }

    #[test]
    fn retained_artifact_bytes_cannot_exceed_the_bound_reservation() {
        let budget = Arc::new(ArtifactRetainedBudget::new(8));
        let retention =
            ArtifactRetention::try_new(1, budget.clone(), Arc::new(AcceptingMemoryAccount))
                .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let error = PhysicalArtifact::from_retained_bytes(
            ArtifactKind::ValueSet,
            schema,
            LogicalVersion::FIRST,
            false,
            Arc::from([1_u8, 2]),
            PhysicalArtifact::accounted_resident_bytes(2).unwrap(),
            retention,
        )
        .unwrap_err();

        assert_eq!(error, ArtifactContractError::RetentionSizeMismatch);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn accounted_artifact_footprint_includes_shared_retention_owner_metadata() {
        let encoded_bytes = 17;
        let accounted = PhysicalArtifact::accounted_resident_bytes(encoded_bytes).unwrap();
        assert!(
            accounted
                >= encoded_bytes + size_of::<PhysicalArtifact>() + size_of::<ArtifactRetention>()
        );
    }

    #[test]
    fn two_artifact_bundle_accounts_one_shared_owner_at_the_exact_boundary() {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let first_bytes: Arc<[u8]> = Arc::from([1_u8, 2]);
        let second_bytes: Arc<[u8]> = Arc::from([3_u8]);
        let first_component =
            PhysicalArtifact::accounted_resident_component_bytes(first_bytes.len()).unwrap();
        let second_component =
            PhysicalArtifact::accounted_resident_component_bytes(second_bytes.len()).unwrap();
        let total = ArtifactBundle::accounted_resident_overhead(&profile, 2)
            .unwrap()
            .checked_add(first_component)
            .and_then(|bytes| bytes.checked_add(second_component))
            .unwrap();
        let short_budget = Arc::new(ArtifactRetainedBudget::new(total - 1));
        assert!(
            ArtifactRetention::try_new(
                total,
                short_budget.clone(),
                Arc::new(AcceptingMemoryAccount)
            )
            .is_err()
        );
        assert_eq!(short_budget.retained_bytes(), 0);

        let budget = Arc::new(ArtifactRetainedBudget::new(total));
        let retention = Arc::new(
            ArtifactRetention::try_new(total, budget.clone(), Arc::new(AcceptingMemoryAccount))
                .unwrap(),
        );
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let first = Arc::new(
            PhysicalArtifact::from_shared_retained_bytes(
                ArtifactKind::ValueSet,
                schema,
                LogicalVersion::FIRST,
                false,
                first_bytes,
                first_component,
                total,
                retention.clone(),
            )
            .unwrap(),
        );
        let second = Arc::new(
            PhysicalArtifact::from_shared_retained_bytes(
                ArtifactKind::EmptyDomain,
                schema,
                LogicalVersion::FIRST,
                false,
                second_bytes,
                second_component,
                total,
                retention.clone(),
            )
            .unwrap(),
        );
        let bundle = ArtifactBundle::new_retained(
            ChannelId::new(8),
            LogicalVersion::FIRST,
            &profile,
            vec![
                (ArtifactKind::ValueSet, first.clone()),
                (ArtifactKind::EmptyDomain, second.clone()),
            ],
            usize::MAX,
            retention,
        )
        .unwrap();
        assert_eq!(bundle.retained_memory_bytes(), total);
        assert_eq!(budget.retained_bytes(), total);
        drop(bundle);
        assert_eq!(budget.retained_bytes(), total);
        drop(first);
        assert_eq!(budget.retained_bytes(), total);
        drop(second);
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn schema_digest_uses_explicit_timestamp_and_null_semantics() {
        let utc = ArtifactSchemaDigest::for_membership(
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            NullSemantics::NeverMatches,
        )
        .unwrap();
        let nullable = ArtifactSchemaDigest::for_membership(
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            NullSemantics::NullSafeEqual,
        )
        .unwrap();
        let no_tz = ArtifactSchemaDigest::for_membership(
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            NullSemantics::NeverMatches,
        )
        .unwrap();

        assert_ne!(utc, nullable);
        assert_ne!(utc, no_tz);
    }
}
