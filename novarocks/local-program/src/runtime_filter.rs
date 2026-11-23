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
//!
//! This module is also the single owner of the canonical membership artifact
//! schema: its bytes and SHA-256 digest are what a static membership contract
//! carries, and Execution derives its runtime schema from the same encoding.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, TimeUnit};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;
use sha2::{Digest, Sha256};

#[path = "runtime_filter/order_keys.rs"]
mod order_keys;
pub use order_keys::FilterOrderKeys;
#[path = "runtime_filter/sites.rs"]
mod sites;
pub use sites::{AggregateTopNFilters, ScanFilterConsumers};

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
    pub value_type: novarocks_type_contract::FunctionValueType,
    pub direction: FilterSortDirection,
    pub null_order: FilterNullOrder,
}

/// The original v1 single-key comparator/order framing. Each caller supplies
/// its existing type visitor and observes the same individual key bytes; the
/// fixed headers and trailer retain the original observation boundaries.
pub fn single_key_ordered_filter_digests_observed<E>(
    direction: FilterSortDirection,
    null_order: FilterNullOrder,
    visit_key_type: impl FnOnce(&mut dyn FnMut(&[u8]) -> Result<(), E>) -> Result<(), E>,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<([u8; 32], [u8; 32]), E> {
    let mut comparator = Sha256::new();
    comparator.update(b"novarocks.runtime-filter.comparator");
    comparator.update(1_u16.to_be_bytes());
    let mut order = Sha256::new();
    order.update(b"novarocks.runtime-filter.order-contract");
    order.update(1_u16.to_be_bytes());
    let mut emit = |bytes: &[u8]| {
        for byte in bytes {
            observe()?;
            comparator.update([*byte]);
            order.update([*byte]);
        }
        Ok::<_, E>(())
    };
    emit(&1_u32.to_be_bytes())?;
    visit_key_type(&mut emit)?;
    emit(&[match direction {
        FilterSortDirection::Ascending => 1,
        FilterSortDirection::Descending => 2,
    }])?;
    emit(&[match null_order {
        FilterNullOrder::First => 1,
        FilterNullOrder::Last => 2,
    }])?;
    let comparator: [u8; 32] = comparator.finalize().into();
    order.update([1]);
    order.update(comparator);
    order.update(1_u16.to_be_bytes());
    Ok((comparator, order.finalize().into()))
}

/// Static payload domain of an ordered Scan filter. This admits no
/// array provenance, allocation capacity, or interpretation of nominal values.
/// Preparation and optional planner placement must use this same contract.
pub fn is_ordered_filter_flat_payload_type(
    value_type: &novarocks_type_contract::FunctionValueType,
) -> bool {
    if value_type.value_type().temporal().is_some() {
        return true;
    }
    use novarocks_type_contract::ValueKind;
    if value_type.logical_type() == novarocks_type_contract::ValueLogicalType::LargeInt {
        return matches!(value_type.data_type(), DataType::FixedSizeBinary(16));
    }
    matches!(
        value_type.value_type().kind(),
        ValueKind::Scalar(_) | ValueKind::String | ValueKind::Binary
    ) && matches!(
        value_type.data_type(),
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    )
}

/// Physical encodings understood by the existing ordered scalar projection.
/// This predicate grants no nominal identity or source/storage authority.
pub fn is_aggregate_topn_key_carrier(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Timestamp(_, None)
            | DataType::Decimal128(_, _)
            | DataType::FixedSizeBinary(16)
    )
}

/// Installed single-key Aggregate TopN domain. A 16-byte carrier alone never
/// establishes LARGEINT; the complete declared semantic identity is required.
pub fn is_aggregate_topn_key_type(value_type: &novarocks_type_contract::FunctionValueType) -> bool {
    if value_type.value_type().temporal().is_some() {
        return true;
    }
    use novarocks_type_contract::ValueLogicalType;
    match value_type.logical_type() {
        ValueLogicalType::Physical => {
            is_aggregate_topn_key_carrier(value_type.data_type())
                && !matches!(value_type.data_type(), DataType::FixedSizeBinary(_))
        }
        ValueLogicalType::LargeInt => {
            matches!(value_type.data_type(), DataType::FixedSizeBinary(16))
        }
        _ => false,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StaticFilterContract {
    Membership {
        canonical: FilterMembershipSchema,
        value_type: novarocks_type_contract::FunctionValueType,
    },
    Ordered {
        keys: FilterOrderKeys,
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

/// CompleteOnce membership live delivery applies its sole complete snapshot
/// from the next batch boundary; MonotonicUpdates applies successive updates.
/// Activation does not change the channel's declared lifecycle or reduction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterConsumerActivation {
    BlockingSnapshot,
    NonBlockingLive {
        late_apply: FilterLateApplyGranularity,
    },
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
    /// The membership contract over `data_type` keys, digested by the
    /// canonical membership schema. This is the only way a compiler obtains
    /// the digest a runtime consumer and producer verify.
    pub fn membership(
        value_type: &novarocks_type_contract::FunctionValueType,
        null_semantics: FilterNullSemantics,
    ) -> Result<Self, FilterMembershipSchemaError> {
        if !is_semantic_filter_key_type(value_type) {
            return Err(FilterMembershipSchemaError::UnsupportedType);
        }
        let canonical = FilterMembershipSchema::try_new(value_type.data_type(), null_semantics)?;
        Ok(Self::Membership {
            canonical,
            value_type: value_type.clone(),
        })
    }

    /// Adopt the original immutable artifact and independent complete domain.
    /// No canonical bytes or type tree are rebuilt here.
    pub fn membership_from_canonical(
        canonical: FilterMembershipSchema,
        value_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, StaticFilterError> {
        let contract = Self::Membership {
            canonical,
            value_type,
        };
        contract.validate()?;
        Ok(contract)
    }

    fn validate(&self) -> Result<(), StaticFilterError> {
        if matches!(self, Self::Membership { canonical, value_type } if canonical.data_type() != value_type.data_type())
        {
            return Err(StaticFilterError::KindContractMismatch);
        }

        if matches!(self, Self::Ordered { keys, .. } if keys.is_empty()) {
            return Err(StaticFilterError::EmptyOrderedKeys);
        }
        Ok(())
    }
}

/// Domain tag that prefixes every canonical membership schema.
const MEMBERSHIP_SCHEMA_DOMAIN: &[u8] = b"novarocks.runtime-filter.artifact-schema";
/// The only canonical membership schema version.
const MEMBERSHIP_SCHEMA_VERSION: u8 = 1;

/// Why a membership schema could not be encoded or decoded. Each variant
/// carries the exact diagnostic Execution reports for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterMembershipSchemaError {
    UnsupportedType,
    TimezoneLengthOverflow {
        required: usize,
        source: std::num::TryFromIntError,
    },
    DigestMismatch,
    Truncated,
    UnknownPrefix,
    InvalidNullSemantics,
    TrailingBytes,
    InvalidTimeUnit,
    TimezoneLengthDecodeOverflow {
        required: u32,
        source: std::num::TryFromIntError,
    },
    TimezoneNotUtf8(std::str::Utf8Error),
    InvalidTimezoneMetadata,
    InvalidDecimal,
    UnsupportedTypeTag,
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
}

impl FilterMembershipSchemaError {
    pub const fn detail(self) -> &'static str {
        match self {
            Self::UnsupportedType => "unsupported membership schema type",
            Self::TimezoneLengthOverflow { .. } => {
                "membership schema timezone length overflows u32"
            }
            Self::DigestMismatch => "membership schema digest does not match canonical bytes",
            Self::Truncated => "truncated membership schema",
            Self::UnknownPrefix => "unknown canonical membership schema prefix",
            Self::InvalidNullSemantics => "invalid membership null semantics",
            Self::TrailingBytes => "membership schema has trailing bytes",
            Self::InvalidTimeUnit => "invalid timestamp time unit",
            Self::TimezoneLengthDecodeOverflow { .. } => "timestamp timezone length overflow",
            Self::TimezoneNotUtf8(_) => "timestamp timezone is not UTF-8",
            Self::InvalidTimezoneMetadata => "invalid timestamp timezone metadata",
            Self::InvalidDecimal => "invalid decimal schema metadata",
            Self::UnsupportedTypeTag => "unsupported membership schema type tag",
            Self::Resource(_) => "membership schema construction resource failure",
        }
    }
}

impl fmt::Display for FilterMembershipSchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.detail())
    }
}

impl std::error::Error for FilterMembershipSchemaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TimezoneLengthOverflow { source, .. }
            | Self::TimezoneLengthDecodeOverflow { source, .. } => Some(source),
            Self::TimezoneNotUtf8(source) => Some(source),
            Self::Resource(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for FilterMembershipSchemaError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, code) = match self {
            Self::UnsupportedType => (FailureClass::Invariant, 100),
            Self::TimezoneLengthOverflow { required, .. } => {
                return FailureFact::new(
                    FailureClass::Representation,
                    FailureReason::new(
                        domain::LOCAL_PROGRAM,
                        std::num::NonZeroU16::new(101).unwrap(),
                    ),
                )
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::LOCAL_PROGRAM,
                        code: std::num::NonZeroU16::new(101).unwrap(),
                    },
                    required: u64::try_from(*required).ok(),
                    extent_limit: Some(u32::MAX as u64),
                    representable_prefix: None,
                });
            }
            Self::DigestMismatch => (FailureClass::Data, 102),
            Self::Truncated => (FailureClass::Data, 103),
            Self::UnknownPrefix => (FailureClass::Data, 104),
            Self::InvalidNullSemantics => (FailureClass::Data, 105),
            Self::TrailingBytes => (FailureClass::Data, 106),
            Self::InvalidTimeUnit => (FailureClass::Data, 107),
            Self::TimezoneLengthDecodeOverflow { .. } => (FailureClass::Data, 108),
            Self::TimezoneNotUtf8(_) => (FailureClass::Data, 109),
            Self::InvalidTimezoneMetadata => (FailureClass::Data, 110),
            Self::InvalidDecimal => (FailureClass::Data, 111),
            Self::UnsupportedTypeTag => (FailureClass::Data, 112),
            Self::Resource(source) => return source.failure_fact(),
        };
        FailureFact::new(
            class,
            FailureReason::new(
                domain::LOCAL_PROGRAM,
                std::num::NonZeroU16::new(code).unwrap(),
            ),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for FilterMembershipSchemaError {}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for FilterMembershipSchemaError
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}

/// The canonical schema of one membership artifact: its key type and NULL
/// semantics, their canonical bytes, and the SHA-256 digest of those bytes.
#[derive(Clone, Debug)]
pub struct FilterMembershipSchema {
    data_type: DataType,
    null_semantics: FilterNullSemantics,
    canonical_bytes: Arc<[u8]>,
    digest: [u8; 32],
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl PartialEq for FilterMembershipSchema {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
            && self.null_semantics == other.null_semantics
            && self.canonical_bytes == other.canonical_bytes
            && self.digest == other.digest
    }
}
impl Eq for FilterMembershipSchema {}

impl FilterMembershipSchema {
    pub fn try_new(
        data_type: &DataType,
        null_semantics: FilterNullSemantics,
    ) -> Result<Self, FilterMembershipSchemaError> {
        Self::try_new_observed::<FilterMembershipSchemaError>(
            data_type,
            null_semantics,
            |_| Ok(()),
            |_| Ok(()),
        )
    }

    /// Construct the canonical artifact under the original caller's request
    /// and work observers. Artifact bytes describe only physical key encoding;
    /// the complete execution value binding is retained by its channel owner.
    pub fn try_new_observed<E>(
        data_type: &DataType,
        null_semantics: FilterNullSemantics,
        request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E>
    where
        E: From<FilterMembershipSchemaError>
            + From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
    {
        Self::try_new_with_storage_observed(
            data_type,
            null_semantics,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            request,
            work,
        )
    }

    /// The canonical bytes and reused carrier backing retain only their
    /// actual construction/source lifetimes, independently of a channel.
    pub fn try_new_with_storage_observed<E>(
        data_type: &DataType,
        null_semantics: FilterNullSemantics,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E>
    where
        E: From<FilterMembershipSchemaError>
            + From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
    {
        use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
        let mut length = MEMBERSHIP_SCHEMA_DOMAIN
            .len()
            .checked_add(2)
            .ok_or(LayoutResourceError::Arithmetic)?;
        work(MEMBERSHIP_SCHEMA_DOMAIN.len() + 2)?;
        visit_filter_key_type::<E>(data_type, |bytes| {
            work(bytes.len())?;
            length = length
                .checked_add(bytes.len())
                .ok_or(LayoutResourceError::Arithmetic)?;
            Ok(())
        })?;
        let backing =
            std::alloc::Layout::array::<u8>(length).map_err(|_| LayoutResourceError::Arithmetic)?;
        request(backing)?;
        let mut canonical = Vec::with_capacity(length);
        work(MEMBERSHIP_SCHEMA_DOMAIN.len() + 1)?;
        canonical.extend_from_slice(MEMBERSHIP_SCHEMA_DOMAIN);
        canonical.push(MEMBERSHIP_SCHEMA_VERSION);
        visit_filter_key_type::<E>(data_type, |bytes| {
            work(bytes.len())?;
            canonical.extend_from_slice(bytes);
            Ok(())
        })?;
        work(1)?;
        canonical.push(match null_semantics {
            FilterNullSemantics::NeverMatches => 1,
            FilterNullSemantics::NullSafeEqual => 2,
        });
        work(canonical.len())?;
        let digest = Sha256::digest(&canonical).into();
        request(arc_layout(backing)?)?;
        work(1)?;
        Ok(Self {
            data_type: data_type.clone(),
            null_semantics,
            canonical_bytes: canonical.into(),
            digest,
            _storage: storage,
        })
    }

    /// Strictly decode canonical bytes whose digest must be `expected_digest`.
    pub fn from_canonical(
        canonical_bytes: impl AsRef<[u8]>,
        expected_digest: [u8; 32],
    ) -> Result<Self, FilterMembershipSchemaError> {
        let canonical = canonical_bytes.as_ref();
        let digest: [u8; 32] = Sha256::digest(canonical).into();
        if digest != expected_digest {
            return Err(FilterMembershipSchemaError::DigestMismatch);
        }
        let mut cursor = SchemaCursor {
            remaining: canonical,
        };
        if cursor.take(MEMBERSHIP_SCHEMA_DOMAIN.len())? != MEMBERSHIP_SCHEMA_DOMAIN
            || cursor.u8()? != MEMBERSHIP_SCHEMA_VERSION
        {
            return Err(FilterMembershipSchemaError::UnknownPrefix);
        }
        let data_type = decode_filter_key_type(&mut cursor)?;
        let null_semantics = match cursor.u8()? {
            1 => FilterNullSemantics::NeverMatches,
            2 => FilterNullSemantics::NullSafeEqual,
            _ => return Err(FilterMembershipSchemaError::InvalidNullSemantics),
        };
        if !cursor.remaining.is_empty() {
            return Err(FilterMembershipSchemaError::TrailingBytes);
        }
        Ok(Self {
            data_type,
            null_semantics,
            canonical_bytes: Arc::from(canonical),
            digest,
            _storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        })
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn null_semantics(&self) -> FilterNullSemantics {
        self.null_semantics
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

struct SchemaCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> SchemaCursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], FilterMembershipSchemaError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or(FilterMembershipSchemaError::Truncated)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, FilterMembershipSchemaError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, FilterMembershipSchemaError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .expect("schema cursor guarantees four bytes"),
        ))
    }
}

/// Append the canonical encoding of one runtime-filter key type. Membership
/// schemas and ordered contract keys share this one type vocabulary.
pub fn encode_filter_key_type(
    data_type: &DataType,
    output: &mut Vec<u8>,
) -> Result<(), FilterMembershipSchemaError> {
    visit_filter_key_type(data_type, |bytes| {
        output.extend_from_slice(bytes);
        Ok(())
    })
}

/// Whether the canonical membership schema can represent this physical key.
/// Planner placement and preparation share the canonical encoder's domain.
// Design: ADR-0236 (docs/adr/ADR-0236-installed-optimization-domains-and-partition-rank-retention.md)
pub fn is_semantic_filter_key_type(
    value_type: &novarocks_type_contract::FunctionValueType,
) -> bool {
    if matches!(
        value_type.data_type(),
        DataType::FixedSizeBinary(10 | 12 | 14)
    ) {
        return value_type.value_type().temporal().is_some();
    }
    is_filter_key_type(value_type.data_type())
}

pub fn is_filter_key_type(data_type: &DataType) -> bool {
    visit_filter_key_type::<FilterMembershipSchemaError>(data_type, |_| Ok(())).is_ok()
}

/// Typed ordered comparators have a separate nominal tag. Equal-width
/// timestamp, time and interval payloads never share a comparator identity.
pub fn temporal_order_key_type_bytes(temporal: novarocks_type_contract::TemporalType) -> [u8; 5] {
    use novarocks_type_contract::TemporalType;
    match temporal {
        TemporalType::Time(p) => [14, 1, p.digits(), 0, 0],
        TemporalType::TimeWithTimeZone(p) => [14, 2, p.digits(), 0, 0],
        TemporalType::Timestamp(p) => [14, 3, p.digits(), 0, 0],
        TemporalType::TimestampWithTimeZone(p) => [14, 4, p.digits(), 0, 0],
        TemporalType::Interval(q) => [
            14,
            5 + q.start() as u8,
            q.end() as u8,
            q.leading_precision(),
            q.fractional_precision().digits(),
        ],
    }
}

pub fn visit_ordered_filter_key_type<E: From<FilterMembershipSchemaError>>(
    data_type: &DataType,
    temporal: Option<novarocks_type_contract::TemporalType>,
    mut write: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    match temporal {
        Some(declaration) => {
            if data_type != &declaration.carrier() {
                return Err(FilterMembershipSchemaError::UnsupportedType.into());
            }
            write(&temporal_order_key_type_bytes(declaration))
        }
        None if matches!(
            data_type,
            DataType::FixedSizeBinary(10 | 12 | 14) | DataType::Float32 | DataType::Float64
        ) =>
        {
            Err(FilterMembershipSchemaError::UnsupportedType.into())
        }
        None => visit_filter_key_type(data_type, write),
    }
}

/// Visit canonical physical key bytes without allocating a shadow buffer.
/// Membership and ordered artifact encoders share this exact vocabulary.
pub fn visit_filter_key_type<E: From<FilterMembershipSchemaError>>(
    data_type: &DataType,
    mut write: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    match data_type {
        DataType::Boolean => write(&[1])?,
        DataType::Int8 => write(&[2])?,
        DataType::Int16 => write(&[3])?,
        DataType::Int32 => write(&[4])?,
        DataType::Int64 => write(&[5])?,
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => write(&[6])?,
        DataType::FixedSizeBinary(width @ (10 | 12 | 14)) => write(&[13, *width as u8])?,
        DataType::Float32 => write(&[7])?,
        DataType::Float64 => write(&[8])?,
        DataType::Utf8 => write(&[9])?,
        DataType::Date32 => write(&[10])?,
        DataType::Timestamp(unit, timezone) => {
            write(&[
                11,
                match unit {
                    TimeUnit::Second => 1,
                    TimeUnit::Millisecond => 2,
                    TimeUnit::Microsecond => 3,
                    TimeUnit::Nanosecond => 4,
                },
            ])?;
            match timezone {
                Some(timezone) => {
                    write(&[1])?;
                    let length = u32::try_from(timezone.len()).map_err(|source| {
                        FilterMembershipSchemaError::TimezoneLengthOverflow {
                            required: timezone.len(),
                            source,
                        }
                    })?;
                    write(&length.to_be_bytes())?;
                    write(timezone.as_bytes())?;
                }
                None => write(&[0])?,
            }
        }
        DataType::Decimal128(precision, scale)
            if *precision != 0
                && *precision <= DECIMAL128_MAX_PRECISION
                && *scale <= DECIMAL128_MAX_SCALE
                && (*scale <= 0 || (*scale as u8) <= *precision) =>
        {
            write(&[12, *precision, *scale as u8])?;
        }
        _ => return Err(FilterMembershipSchemaError::UnsupportedType.into()),
    }
    Ok(())
}

fn decode_filter_key_type(
    cursor: &mut SchemaCursor<'_>,
) -> Result<DataType, FilterMembershipSchemaError> {
    match cursor.u8()? {
        1 => Ok(DataType::Boolean),
        2 => Ok(DataType::Int8),
        3 => Ok(DataType::Int16),
        4 => Ok(DataType::Int32),
        5 => Ok(DataType::Int64),
        6 => Ok(DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH)),
        13 => match cursor.u8()? {
            width @ (10 | 12 | 14) => Ok(DataType::FixedSizeBinary(i32::from(width))),
            _ => Err(FilterMembershipSchemaError::UnsupportedType),
        },
        7 => Ok(DataType::Float32),
        8 => Ok(DataType::Float64),
        9 => Ok(DataType::Utf8),
        10 => Ok(DataType::Date32),
        11 => {
            let unit = match cursor.u8()? {
                1 => TimeUnit::Second,
                2 => TimeUnit::Millisecond,
                3 => TimeUnit::Microsecond,
                4 => TimeUnit::Nanosecond,
                _ => return Err(FilterMembershipSchemaError::InvalidTimeUnit),
            };
            let timezone = match cursor.u8()? {
                0 => None,
                1 => {
                    let required = cursor.u32()?;
                    let length = usize::try_from(required).map_err(|source| {
                        FilterMembershipSchemaError::TimezoneLengthDecodeOverflow {
                            required,
                            source,
                        }
                    })?;
                    Some(
                        std::str::from_utf8(cursor.take(length)?)
                            .map_err(FilterMembershipSchemaError::TimezoneNotUtf8)?
                            .into(),
                    )
                }
                _ => return Err(FilterMembershipSchemaError::InvalidTimezoneMetadata),
            };
            Ok(DataType::Timestamp(unit, timezone))
        }
        12 => {
            let precision = cursor.u8()?;
            let scale = cursor.u8()? as i8;
            if precision == 0
                || precision > DECIMAL128_MAX_PRECISION
                || scale > DECIMAL128_MAX_SCALE
                || (scale > 0 && scale as u8 > precision)
            {
                return Err(FilterMembershipSchemaError::InvalidDecimal);
            }
            Ok(DataType::Decimal128(precision, scale))
        }
        _ => Err(FilterMembershipSchemaError::UnsupportedTypeTag),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_key_ordered_digests_match_frozen_utf8_literals() {
        // Frozen v1 bytes, also independently checked by the runtime's strict
        // contract validator. The compiler uses this original type visitor.
        let cases = [
            (
                FilterSortDirection::Ascending,
                FilterNullOrder::First,
                "77782fddf467caee0ce29343ed593a1acb8cf89a46ef1f6765730c4bdbabc51d",
                "5137b9bc01ebb44e0f02e78917ef2e76a655d5320b360f7ec68ff2566970ca62",
            ),
            (
                FilterSortDirection::Ascending,
                FilterNullOrder::Last,
                "c712d2a8677a0968f10c6e39490fcd77ab08fcd3ee9de29bb1df944a16223bae",
                "a26b9e26239e0842aa3fd6016bcaf0d41d0f69d17b6a078ea57633f17e1eba26",
            ),
            (
                FilterSortDirection::Descending,
                FilterNullOrder::First,
                "e75b9b677a5a21c827d830a9a56dc38d389f51314de51dc18cb6ce7dd186911f",
                "63fd0c611009b809d3e952d66ce73e4a9b966b7f68b395eae784f35ca8331627",
            ),
            (
                FilterSortDirection::Descending,
                FilterNullOrder::Last,
                "ea4e538a3a740e5c6813c79021f0b816aacd03a060a9741902504e4034bdcaae",
                "2efde53cbc305da924d5b722ff253591fa2d9dfe6c137f319531b0108af51d6f",
            ),
        ];
        fn digest(hex: &str) -> [u8; 32] {
            std::array::from_fn(|index| {
                u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).unwrap()
            })
        }
        for (direction, null_order, comparator, order) in cases {
            let actual = single_key_ordered_filter_digests_observed(
                direction,
                null_order,
                |emit| visit_filter_key_type(&DataType::Utf8, emit),
                || Ok::<_, FilterMembershipSchemaError>(()),
            )
            .unwrap();
            assert_eq!(actual, (digest(comparator), digest(order)));
        }
    }

    #[test]
    fn membership_key_predicate_uses_the_canonical_schema_domain() {
        for data_type in [
            DataType::Boolean,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::FixedSizeBinary(16),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Decimal128(38, 3),
            DataType::Binary,
            DataType::Decimal256(40, 0),
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
        ] {
            assert_eq!(
                is_filter_key_type(&data_type),
                encode_filter_key_type(&data_type, &mut Vec::new()).is_ok(),
                "{data_type:?}",
            );
        }
    }

    #[test]
    fn ordered_filter_flat_payload_keeps_the_declared_scalar_domain() {
        use novarocks_type_contract::{FunctionValueType as V, ValueLogicalType};
        let accepted = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::FixedSizeBinary(7),
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Shanghai".into())),
            DataType::Decimal128(20, 3),
        ];
        for nullable in [false, true] {
            for data_type in &accepted {
                let ty = V::new(data_type.clone(), nullable);
                assert!(is_ordered_filter_flat_payload_type(&ty), "{ty:?}");
                assert_eq!(ty.data_type(), data_type);
                assert_eq!(ty.nullable(), nullable);
            }
            for data_type in [
                DataType::Null,
                DataType::UInt32,
                DataType::LargeUtf8,
                DataType::LargeBinary,
                DataType::Utf8View,
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
            ] {
                let ty = V::new(data_type, nullable);
                assert!(!is_ordered_filter_flat_payload_type(&ty), "{ty:?}");
            }
            for (data_type, logical) in [
                (DataType::Utf8, ValueLogicalType::Json),
                (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
            ] {
                let ty = V::try_with_logical_type(data_type, nullable, logical).unwrap();
                assert!(!is_ordered_filter_flat_payload_type(&ty), "{ty:?}");
            }
            let largeint = V::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                nullable,
                ValueLogicalType::LargeInt,
            )
            .unwrap();
            assert!(is_ordered_filter_flat_payload_type(&largeint));
        }
    }

    #[test]
    fn complete_domain_and_original_canonical_are_paired_without_semantic_inference() {
        use novarocks_type_contract::{FunctionValueType as V, ValueLogicalType};
        let canonical =
            FilterMembershipSchema::try_new(&DataType::Utf8, FilterNullSemantics::NeverMatches)
                .unwrap();
        let text = StaticFilterContract::membership_from_canonical(
            canonical.clone(),
            V::new(DataType::Utf8, false),
        )
        .unwrap();
        let json = StaticFilterContract::membership_from_canonical(
            canonical.clone(),
            V::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json).unwrap(),
        )
        .unwrap();
        assert_ne!(
            text, json,
            "equal artifact bytes do not author complete execution domains"
        );
        assert!(matches!(
            StaticFilterContract::membership_from_canonical(
                canonical,
                V::new(DataType::Int64, false)
            ),
            Err(StaticFilterError::KindContractMismatch)
        ));
    }

    #[test]
    fn rejects_role_mismatch_before_binding_a_session() {
        let contract = StaticFilterContract::membership(
            &novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            FilterNullSemantics::NeverMatches,
        )
        .unwrap();
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

    #[test]
    fn membership_schema_pins_the_v1_canonical_bytes_and_digest() {
        let schema =
            FilterMembershipSchema::try_new(&DataType::Int32, FilterNullSemantics::NullSafeEqual)
                .expect("int32 null-safe schema is supported");
        assert_eq!(
            schema.canonical_bytes(),
            b"novarocks.runtime-filter.artifact-schema\x01\x04\x02"
        );
        assert_eq!(
            schema.digest(),
            [
                0x92, 0x5d, 0xde, 0xf2, 0x0b, 0x5b, 0xfa, 0x0e, 0xd3, 0xd6, 0xe3, 0x55, 0xf1, 0x18,
                0x79, 0xba, 0xeb, 0x2a, 0x0c, 0x7d, 0x31, 0x0b, 0x56, 0xe2, 0xcc, 0x03, 0x55, 0x57,
                0x10, 0xbf, 0x96, 0x41,
            ]
        );
        assert_eq!(
            FilterMembershipSchema::from_canonical(schema.canonical_bytes(), schema.digest()),
            Ok(schema.clone())
        );
        assert_eq!(
            StaticFilterContract::membership(
                &novarocks_type_contract::FunctionValueType::new(DataType::Int32, true),
                FilterNullSemantics::NullSafeEqual
            ),
            Ok(StaticFilterContract::Membership {
                canonical: schema.clone(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, true),
            })
        );
    }

    #[test]
    fn membership_schema_round_trips_parameterized_types_and_refuses_the_rest() {
        for data_type in [
            DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Shanghai".into())),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Decimal128(38, 10),
            DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH),
        ] {
            let schema =
                FilterMembershipSchema::try_new(&data_type, FilterNullSemantics::NeverMatches)
                    .expect("supported membership type");
            let decoded =
                FilterMembershipSchema::from_canonical(schema.canonical_bytes(), schema.digest())
                    .expect("exact canonical bytes decode");
            assert_eq!(decoded, schema);
        }
        assert_eq!(
            FilterMembershipSchema::try_new(
                &DataType::LargeUtf8,
                FilterNullSemantics::NeverMatches
            ),
            Err(FilterMembershipSchemaError::UnsupportedType)
        );
        let schema =
            FilterMembershipSchema::try_new(&DataType::Int64, FilterNullSemantics::NeverMatches)
                .unwrap();
        assert_eq!(
            FilterMembershipSchema::from_canonical(schema.canonical_bytes(), [0; 32]),
            Err(FilterMembershipSchemaError::DigestMismatch)
        );
        let mut trailing = schema.canonical_bytes().to_vec();
        trailing.push(0);
        let digest: [u8; 32] = Sha256::digest(&trailing).into();
        assert_eq!(
            FilterMembershipSchema::from_canonical(&trailing, digest),
            Err(FilterMembershipSchemaError::TrailingBytes)
        );
    }
}

#[cfg(test)]
mod typed_creation_tests {
    use super::*;
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;

    #[derive(Debug, PartialEq)]
    enum Refusal {
        Observer(usize),
        Schema(FilterMembershipSchemaError),
        Resource(LayoutResourceError),
    }
    impl From<FilterMembershipSchemaError> for Refusal {
        fn from(error: FilterMembershipSchemaError) -> Self {
            Self::Schema(error)
        }
    }
    impl From<LayoutResourceError> for Refusal {
        fn from(error: LayoutResourceError) -> Self {
            Self::Resource(error)
        }
    }
    #[test]
    fn canonical_observed_constructor_keeps_artifact_bytes_and_real_requests() {
        let timezone: Arc<str> = "Z".repeat(600).into();
        let ty = DataType::Timestamp(TimeUnit::Microsecond, Some(timezone));
        let pure =
            FilterMembershipSchema::try_new(&ty, FilterNullSemantics::NullSafeEqual).unwrap();
        let mut requests = Vec::new();
        let mut work_units = 0;
        let observed = FilterMembershipSchema::try_new_observed::<Refusal>(
            &ty,
            FilterNullSemantics::NullSafeEqual,
            |layout| {
                requests.push(layout);
                Ok(())
            },
            |units| {
                work_units += units;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(observed, pure);
        assert_eq!(requests.len(), 2);
        let bytes = std::alloc::Layout::array::<u8>(pure.canonical_bytes().len()).unwrap();
        assert_eq!(requests[0], bytes);
        assert_eq!(
            requests[1],
            novarocks_type_contract::owned_resources::layout::arc_layout(bytes).unwrap()
        );
        assert!(work_units >= 3 * 600);
    }
    #[test]
    fn canonical_constructor_returns_first_request_or_work_refusal_without_later_callbacks() {
        for refuse_at in 1..=2 {
            let mut calls = 0;
            let result = FilterMembershipSchema::try_new_observed::<Refusal>(
                &DataType::Int64,
                FilterNullSemantics::NeverMatches,
                |_| {
                    calls += 1;
                    if calls == refuse_at {
                        Err(Refusal::Observer(refuse_at))
                    } else {
                        Ok(())
                    }
                },
                |_| Ok(()),
            );
            assert_eq!(result.unwrap_err(), Refusal::Observer(refuse_at));
            assert_eq!(calls, refuse_at);
        }
        let mut calls = 0;
        let result = FilterMembershipSchema::try_new_observed::<Refusal>(
            &DataType::Utf8,
            FilterNullSemantics::NeverMatches,
            |_| {
                calls += 1;
                Ok(())
            },
            |_| Err(Refusal::Observer(7)),
        );
        assert_eq!(result.unwrap_err(), Refusal::Observer(7));
        assert_eq!(calls, 0);
    }
}

#[cfg(test)]
mod failure_fact_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    use std::error::Error;
    #[test]
    fn canonical_schema_authors_distinguish_construction_format_and_actual_invalid_bytes() {
        let unsupported = FilterMembershipSchema::try_new(
            &DataType::Decimal256(10, 2),
            FilterNullSemantics::NeverMatches,
        )
        .unwrap_err();
        assert_eq!(unsupported, FilterMembershipSchemaError::UnsupportedType);
        assert_eq!(unsupported.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(unsupported.failure_fact().reason().code(), 100);
        let original = FilterMembershipSchema::try_new(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            FilterNullSemantics::NeverMatches,
        )
        .unwrap();
        let mut bytes = original.canonical_bytes().to_vec();
        let position = bytes.windows(3).position(|x| x == b"UTC").unwrap();
        bytes[position] = 255;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        let failure = FilterMembershipSchema::from_canonical(&bytes, digest).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 109);
        let cause = failure
            .source()
            .unwrap()
            .downcast_ref::<std::str::Utf8Error>()
            .unwrap();
        assert_eq!(cause.valid_up_to(), 0);
        assert_eq!(cause.error_len(), Some(1));
        assert!(failure.failure_fact().resource().is_none());
        assert!(failure.failure_fact().representation().is_none());
        #[cfg(target_pointer_width = "64")]
        {
            let required = u32::MAX as usize + 1;
            let source = u32::try_from(required).unwrap_err();
            let failure = FilterMembershipSchemaError::TimezoneLengthOverflow { required, source };
            assert_eq!(failure.failure_fact().class(), FailureClass::Representation);
            assert!(failure.failure_fact().resource().is_none());
            let representation = failure.failure_fact().representation().unwrap();
            assert_eq!(
                representation.boundary,
                RepresentationBoundary::SingleOutput
            );
            assert_eq!(representation.required, Some(required as u64));
            assert_eq!(representation.extent_limit, Some(u32::MAX as u64));
            assert_eq!(representation.representable_prefix, None);
            assert!(failure.source().unwrap().is::<std::num::TryFromIntError>());
        }
    }
}

// Owner-local installed-domain qualification, distinct from physical payload
// copying. Literal semantic identities catch width-based nominal inference.
#[cfg(test)]
mod aggregate_topn_key_tests {
    use super::*;
    use novarocks_type_contract::{FunctionValueType as V, ValueLogicalType as L};
    #[test]
    fn typed_channels_aggregate_topn_key_preserves_nominal_and_timestamp_domains() {
        for nullable in [false, true] {
            for ty in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Utf8,
                DataType::Date32,
                DataType::Decimal128(18, 3),
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ] {
                assert!(is_aggregate_topn_key_type(&V::new(ty, nullable)));
            }
            for ty in [
                DataType::Boolean,
                DataType::Float64,
                DataType::Binary,
                DataType::FixedSizeBinary(16),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ] {
                assert!(!is_aggregate_topn_key_type(&V::new(ty, nullable)));
            }
            assert!(is_aggregate_topn_key_type(
                &V::try_with_logical_type(DataType::FixedSizeBinary(16), nullable, L::LargeInt)
                    .unwrap()
            ));
            assert!(!is_aggregate_topn_key_type(
                &V::try_with_logical_type(DataType::FixedSizeBinary(16), nullable, L::Uuid)
                    .unwrap()
            ));
            assert!(!is_aggregate_topn_key_type(
                &V::try_with_logical_type(DataType::Utf8, nullable, L::Json).unwrap()
            ));
        }
    }
}
