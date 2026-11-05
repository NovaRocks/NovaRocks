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

//! Execution-owned canonical runtime-filter contributions. This module has no
//! participant, connector, or delivery dependencies.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;
use sha2::{Digest, Sha256};

use super::{
    RuntimeFilterContractViolation, RuntimeFilterContractViolationKind,
    RuntimeFilterContribution as ExecutionContribution, RuntimeFilterContributionKind,
    RuntimeFilterExecutionContract, RuntimeFilterFinalDomain, RuntimeFilterProducerContract,
    RuntimeFilterProducerKind,
};

pub const FINGERPRINT_VERSION_TAG: &[u8] = b"novarocks.runtime-filter.value-domain-delta.v1";
const MAGIC: &[u8; 4] = b"NRFC";
const CODEC_VERSION: u16 = 1;
const HEADER_LEN: usize = 48;

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
    ArithmeticOverflow,
    TrailingBytes,
    NonCanonicalPayload,
    EncodedSizeExceeded,
    ResourceLimit,
}
impl fmt::Display for ContributionCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid runtime filter contribution: {self:?}")
    }
}
impl Error for ContributionCodecError {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContributionSizeError {
    LengthExceedsCanonicalRange,
    SizeOverflow,
}
impl fmt::Display for ContributionSizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "canonical contribution size error: {self:?}")
    }
}
impl Error for ContributionSizeError {}
impl From<ContributionSizeError> for ContributionCodecError {
    fn from(source: ContributionSizeError) -> Self {
        match source {
            ContributionSizeError::LengthExceedsCanonicalRange => Self::LengthOverflow,
            ContributionSizeError::SizeOverflow => Self::ArithmeticOverflow,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalF32(u32);
impl CanonicalF32 {
    pub fn new(value: f32) -> Self {
        Self(if value == 0.0 {
            0
        } else if value.is_nan() {
            0x7fc0_0000
        } else {
            value.to_bits()
        })
    }
    pub const fn bits(self) -> u32 {
        self.0
    }
}
impl Ord for CanonicalF32 {
    fn cmp(&self, other: &Self) -> Ordering {
        f32::from_bits(self.0).total_cmp(&f32::from_bits(other.0))
    }
}
impl PartialOrd for CanonicalF32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalF64(u64);
impl CanonicalF64 {
    pub fn new(value: f64) -> Self {
        Self(if value == 0.0 {
            0
        } else if value.is_nan() {
            0x7ff8_0000_0000_0000
        } else {
            value.to_bits()
        })
    }
    pub const fn bits(self) -> u64 {
        self.0
    }
}
impl Ord for CanonicalF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        f64::from_bits(self.0).total_cmp(&f64::from_bits(other.0))
    }
}
impl PartialOrd for CanonicalF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipValues {
    Boolean(BTreeSet<bool>),
    Int8(BTreeSet<i8>),
    Int16(BTreeSet<i16>),
    Int32(BTreeSet<i32>),
    Int64(BTreeSet<i64>),
    LargeInt(BTreeSet<i128>),
    Float32(BTreeSet<CanonicalF32>),
    Float64(BTreeSet<CanonicalF64>),
    Utf8(BTreeSet<String>),
    Date32(BTreeSet<i32>),
    Timestamp {
        unit: TimeUnit,
        timezone: Option<String>,
        values: BTreeSet<i64>,
    },
    Decimal128 {
        precision: u8,
        scale: i8,
        values: BTreeSet<i128>,
    },
}
macro_rules! set_constructor {
    ($name:ident, $variant:ident, $ty:ty) => {
        pub fn $name(values: impl IntoIterator<Item = $ty>) -> Self {
            Self::$variant(values.into_iter().collect())
        }
    };
}
impl MembershipValues {
    set_constructor!(boolean, Boolean, bool);
    set_constructor!(int8, Int8, i8);
    set_constructor!(int16, Int16, i16);
    set_constructor!(int32, Int32, i32);
    set_constructor!(int64, Int64, i64);
    set_constructor!(large_int, LargeInt, i128);
    set_constructor!(date32, Date32, i32);
    pub fn float32(values: impl IntoIterator<Item = f32>) -> Self {
        Self::Float32(values.into_iter().map(CanonicalF32::new).collect())
    }
    pub fn float64(values: impl IntoIterator<Item = f64>) -> Self {
        Self::Float64(values.into_iter().map(CanonicalF64::new).collect())
    }
    pub fn utf8<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Utf8(values.into_iter().map(Into::into).collect())
    }
    pub fn timestamp(
        unit: TimeUnit,
        timezone: Option<impl Into<String>>,
        values: impl IntoIterator<Item = i64>,
    ) -> Self {
        Self::Timestamp {
            unit,
            timezone: timezone.map(Into::into),
            values: values.into_iter().collect(),
        }
    }
    pub fn decimal128(
        precision: u8,
        scale: i8,
        values: impl IntoIterator<Item = i128>,
    ) -> Result<Self, ContributionCodecError> {
        let values = values.into_iter().collect();
        validate_decimal(precision, scale, &values)?;
        Ok(Self::Decimal128 {
            precision,
            scale,
            values,
        })
    }
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Boolean(_) => DataType::Boolean,
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::LargeInt(_) => DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH),
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Date32(_) => DataType::Date32,
            Self::Timestamp { unit, timezone, .. } => {
                DataType::Timestamp(*unit, timezone.clone().map(Into::into))
            }
            Self::Decimal128 {
                precision, scale, ..
            } => DataType::Decimal128(*precision, *scale),
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Boolean(values) => values.is_empty(),
            Self::Int8(values) => values.is_empty(),
            Self::Int16(values) => values.is_empty(),
            Self::Int32(values) => values.is_empty(),
            Self::Int64(values) => values.is_empty(),
            Self::LargeInt(values) => values.is_empty(),
            Self::Float32(values) => values.is_empty(),
            Self::Float64(values) => values.is_empty(),
            Self::Utf8(values) => values.is_empty(),
            Self::Date32(values) => values.is_empty(),
            Self::Timestamp { values, .. } => values.is_empty(),
            Self::Decimal128 { values, .. } => values.is_empty(),
        }
    }
    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), ContributionSizeError> {
        macro_rules! fixed {
            ($tag:expr, $values:expr, $encode:expr) => {{
                out.push($tag);
                push_count(out, $values.len())?;
                for v in $values {
                    out.extend_from_slice(&$encode(v));
                }
            }};
        }
        match self {
            Self::Boolean(v) => fixed!(1, v, |x: &bool| [u8::from(*x)]),
            Self::Int8(v) => fixed!(2, v, |x: &i8| x.to_be_bytes()),
            Self::Int16(v) => fixed!(3, v, |x: &i16| x.to_be_bytes()),
            Self::Int32(v) => fixed!(4, v, |x: &i32| x.to_be_bytes()),
            Self::Int64(v) => fixed!(5, v, |x: &i64| x.to_be_bytes()),
            Self::LargeInt(v) => fixed!(6, v, |x: &i128| x.to_be_bytes()),
            Self::Float32(v) => fixed!(7, v, |x: &CanonicalF32| x.bits().to_be_bytes()),
            Self::Float64(v) => fixed!(8, v, |x: &CanonicalF64| x.bits().to_be_bytes()),
            Self::Utf8(v) => {
                out.push(9);
                push_count(out, v.len())?;
                for x in v {
                    push_bytes(out, x.as_bytes())?;
                }
            }
            Self::Date32(v) => fixed!(10, v, |x: &i32| x.to_be_bytes()),
            Self::Timestamp {
                unit,
                timezone,
                values,
            } => {
                out.extend_from_slice(&[11, time_unit_tag(unit)]);
                match timezone {
                    None => out.push(0),
                    Some(tz) => {
                        out.push(1);
                        push_bytes(out, tz.as_bytes())?;
                    }
                };
                push_count(out, values.len())?;
                for x in values {
                    out.extend_from_slice(&x.to_be_bytes());
                }
            }
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => {
                out.extend_from_slice(&[12, *precision, *scale as u8]);
                push_count(out, values.len())?;
                for x in values {
                    out.extend_from_slice(&x.to_be_bytes());
                }
            }
        };
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValueDomainDelta {
    values: MembershipValues,
    contains_null: bool,
}
impl ValueDomainDelta {
    pub const fn new(values: MembershipValues, contains_null: bool) -> Self {
        Self {
            values,
            contains_null,
        }
    }
    pub const fn values(&self) -> &MembershipValues {
        &self.values
    }
    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }
    pub fn data_type(&self) -> DataType {
        self.values.data_type()
    }
    pub fn matches_data_type(&self, expected: &DataType) -> bool {
        self.data_type() == *expected
    }
    pub fn canonical_encoded_len(&self) -> Result<usize, ContributionSizeError> {
        let mut out = Vec::new();
        self.encode_canonical_into(&mut out)?;
        Ok(out.len())
    }
    pub fn encode_canonical_into(&self, out: &mut Vec<u8>) -> Result<(), ContributionSizeError> {
        push_bytes(out, FINGERPRINT_VERSION_TAG)?;
        self.values.encode_into(out)?;
        out.push(u8::from(self.contains_null));
        Ok(())
    }
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut canonical = Vec::new();
        self.encode_canonical_into(&mut canonical)
            .expect("canonical contribution lengths fit u64");
        Sha256::digest(canonical).into()
    }
}

/// Encodes one membership domain without a contribution envelope.
///
/// Final-domain completion transports exactly this canonical typed payload;
/// the completion fence and routing authority remain outside this value.
pub fn encode_value_domain(
    domain: &ValueDomainDelta,
    max_encoded_bytes: usize,
) -> Result<Vec<u8>, ContributionCodecError> {
    let mut bytes = Vec::new();
    domain.encode_canonical_into(&mut bytes)?;
    if bytes.len() > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }
    Ok(bytes)
}

/// Strictly decodes a canonical membership-domain payload without a frame.
pub fn decode_value_domain(
    payload: &[u8],
    data_type: &DataType,
    max_encoded_bytes: usize,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    if payload.len() > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }
    let domain = decode_membership(payload, data_type)?;
    if encode_value_domain(&domain, max_encoded_bytes)? != payload {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(domain)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderedScalar {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Utf8(String),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
}

/// Borrowed values use the same frozen scalar vocabulary as owned tuples.
/// UTF-8 data remains with its original input or immutable bound owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderedScalarRef<'a> {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Utf8(&'a str),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
}

impl OrderedScalarRef<'_> {
    /// Legacy owned tuples and inline numeric keys share this representation
    /// conversion. The controlled UTF8 host handles strings before this call.
    pub(crate) fn into_owned(self) -> OrderedScalar {
        match self {
            Self::Boolean(value) => OrderedScalar::Boolean(value),
            Self::Int8(value) => OrderedScalar::Int8(value),
            Self::Int16(value) => OrderedScalar::Int16(value),
            Self::Int32(value) => OrderedScalar::Int32(value),
            Self::Int64(value) => OrderedScalar::Int64(value),
            Self::LargeInt(value) => OrderedScalar::LargeInt(value),
            Self::Utf8(value) => OrderedScalar::Utf8(value.to_owned()),
            Self::Date32(value) => OrderedScalar::Date32(value),
            Self::Timestamp(value) => OrderedScalar::Timestamp(value),
            Self::Decimal128(value) => OrderedScalar::Decimal128(value),
        }
    }
}

impl OrderedScalar {
    pub fn as_borrowed(&self) -> OrderedScalarRef<'_> {
        match self {
            Self::Boolean(value) => OrderedScalarRef::Boolean(*value),
            Self::Int8(value) => OrderedScalarRef::Int8(*value),
            Self::Int16(value) => OrderedScalarRef::Int16(*value),
            Self::Int32(value) => OrderedScalarRef::Int32(*value),
            Self::Int64(value) => OrderedScalarRef::Int64(*value),
            Self::LargeInt(value) => OrderedScalarRef::LargeInt(*value),
            Self::Utf8(value) => OrderedScalarRef::Utf8(value),
            Self::Date32(value) => OrderedScalarRef::Date32(*value),
            Self::Timestamp(value) => OrderedScalarRef::Timestamp(*value),
            Self::Decimal128(value) => OrderedScalarRef::Decimal128(*value),
        }
    }
}

/// A one-key bound needs no temporary owned tuple or scalar allocation.
#[derive(Clone, Copy, Debug)]
pub enum OrderedTupleRef<'a> {
    Owned(&'a OrderedTuple),
    Single(Option<OrderedScalarRef<'a>>),
}

impl<'a> OrderedTupleRef<'a> {
    fn len(self) -> usize {
        match self {
            Self::Owned(tuple) => tuple.values().len(),
            Self::Single(_) => 1,
        }
    }
    fn value(self, ordinal: usize) -> Option<OrderedScalarRef<'a>> {
        match self {
            Self::Owned(tuple) => tuple.values()[ordinal]
                .as_ref()
                .map(OrderedScalar::as_borrowed),
            Self::Single(value) => {
                debug_assert_eq!(ordinal, 0);
                value
            }
        }
    }
}

/// A control refusal never becomes an immutable tuple/type mismatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlledOrderedTupleError {
    Tuple(OrderedTupleError),
    Kernel(novarocks_functions::KernelFailure),
}
impl From<OrderedTupleError> for ControlledOrderedTupleError {
    fn from(error: OrderedTupleError) -> Self {
        Self::Tuple(error)
    }
}
impl From<novarocks_functions::KernelFailure> for ControlledOrderedTupleError {
    fn from(error: novarocks_functions::KernelFailure) -> Self {
        Self::Kernel(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlledContributionCodecError {
    Codec(ContributionCodecError),
    Kernel(novarocks_functions::KernelFailure),
}
impl From<ContributionCodecError> for ControlledContributionCodecError {
    fn from(error: ContributionCodecError) -> Self {
        Self::Codec(error)
    }
}
impl From<novarocks_functions::KernelFailure> for ControlledContributionCodecError {
    fn from(error: novarocks_functions::KernelFailure) -> Self {
        Self::Kernel(error)
    }
}

fn legacy_codec_error(error: ControlledContributionCodecError) -> ContributionCodecError {
    match error {
        ControlledContributionCodecError::Codec(error) => error,
        ControlledContributionCodecError::Kernel(_) => {
            unreachable!("the explicit legacy codec control cannot refuse")
        }
    }
}

struct LegacyOrderControl;
impl novarocks_functions::KernelEvaluationControl for LegacyOrderControl {
    fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
        unreachable!("ordered comparison never waits")
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedTuple {
    values: Vec<Option<OrderedScalar>>,
}
impl OrderedTuple {
    pub fn new(values: impl IntoIterator<Item = Option<OrderedScalar>>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }
    pub fn values(&self) -> &[Option<OrderedScalar>] {
        &self.values
    }
    pub fn try_new(
        contract: &RuntimeOrderContract,
        values: impl IntoIterator<Item = Option<OrderedScalar>>,
    ) -> Result<Self, OrderedTupleError> {
        let tuple = Self::new(values);
        contract.validate_tuple(&tuple)?;
        Ok(tuple)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedBoundUpdate {
    contract_digest: [u8; 32],
    bound: OrderedTuple,
    replay_digest: [u8; 32],
}
impl OrderedBoundUpdate {
    pub fn new(contract_digest: [u8; 32], bound: OrderedTuple) -> Self {
        Self {
            contract_digest,
            replay_digest: ordered_replay_digest(contract_digest, &bound),
            bound,
        }
    }
    pub fn try_new(
        contract: &RuntimeOrderContract,
        bound: OrderedTuple,
    ) -> Result<Self, OrderedTupleError> {
        contract.validate_tuple(&bound)?;
        Ok(Self::new(contract.digest(), bound))
    }
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    pub const fn bound(&self) -> &OrderedTuple {
        &self.bound
    }
    pub const fn replay_digest(&self) -> [u8; 32] {
        self.replay_digest
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopKSummary {
    contract_digest: [u8; 32],
    candidates: Vec<OrderedTuple>,
    replay_digest: [u8; 32],
}
impl TopKSummary {
    pub fn new(
        contract_digest: [u8; 32],
        candidates: impl IntoIterator<Item = OrderedTuple>,
    ) -> Self {
        let candidates = candidates.into_iter().collect::<Vec<_>>();
        Self {
            contract_digest,
            replay_digest: topk_replay_digest(contract_digest, &candidates),
            candidates,
        }
    }
    pub fn try_new(
        contract: &RuntimeTopKSummaryContract,
        candidates: impl IntoIterator<Item = OrderedTuple>,
    ) -> Result<Self, TopKSummaryError> {
        let candidates = candidates.into_iter().collect::<Vec<_>>();
        if candidates.len() > contract.k() as usize {
            return Err(TopKSummaryError::TooManyCandidates);
        }
        for candidate in &candidates {
            contract
                .order()
                .validate_tuple(candidate)
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
        let digest = topk_replay_digest(contract.digest(), &candidates);
        Ok(Self {
            contract_digest: contract.digest(),
            candidates,
            replay_digest: digest,
        })
    }
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    pub fn candidates(&self) -> &[OrderedTuple] {
        &self.candidates
    }
    pub const fn replay_digest(&self) -> [u8; 32] {
        self.replay_digest
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalDomainShard {
    fence_digest: [u8; 32],
    domain: ValueDomainDelta,
    replay_digest: [u8; 32],
}
impl FinalDomainShard {
    pub fn new(fence_digest: [u8; 32], domain: ValueDomainDelta) -> Self {
        Self {
            fence_digest,
            replay_digest: final_domain_replay_digest(fence_digest, &domain),
            domain,
        }
    }
    pub const fn fence_digest(&self) -> [u8; 32] {
        self.fence_digest
    }
    pub const fn domain(&self) -> &ValueDomainDelta {
        &self.domain
    }
    pub const fn replay_digest(&self) -> [u8; 32] {
        self.replay_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterContribution {
    Membership(ValueDomainDelta),
    OrderedBound(OrderedBoundUpdate),
    TopKSummary(TopKSummary),
    FinalDomain(FinalDomainShard),
}
impl RuntimeFilterContribution {
    pub const fn membership(delta: ValueDomainDelta) -> Self {
        Self::Membership(delta)
    }
    pub const fn ordered_bound(update: OrderedBoundUpdate) -> Self {
        Self::OrderedBound(update)
    }
    pub const fn top_k_summary(summary: TopKSummary) -> Self {
        Self::TopKSummary(summary)
    }
    pub const fn final_domain(shard: FinalDomainShard) -> Self {
        Self::FinalDomain(shard)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeOrderKey {
    data_type: DataType,
    complete_type: Option<novarocks_functions::FunctionValueType>,
    direction: RuntimeOrderSortDirection,
    null_order: RuntimeOrderNullOrder,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeOrderSortDirection {
    Ascending,
    Descending,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeOrderNullOrder {
    First,
    Last,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderedTupleError {
    ArityMismatch,
    TypeMismatch,
}
impl fmt::Display for OrderedTupleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The existing boundary shell already presents this original Debug.
        fmt::Debug::fmt(self, f)
    }
}
impl Error for OrderedTupleError {}
impl novarocks_failure_contract::FailureProjection for OrderedTupleError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        let reason = match self {
            Self::ArityMismatch => ExecutionReason::FilterOrderedTupleArity,
            Self::TypeMismatch => ExecutionReason::FilterOrderedTupleType,
        };
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            reason.reason(),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopKSummaryError {
    TooManyCandidates,
    CandidateContractMismatch(OrderedTupleError),
    NonCanonicalCandidates,
}
impl RuntimeOrderKey {
    pub const fn new(data_type: DataType) -> Self {
        Self::with_order(
            data_type,
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
        )
    }
    pub const fn with_order(
        data_type: DataType,
        direction: RuntimeOrderSortDirection,
        null_order: RuntimeOrderNullOrder,
    ) -> Self {
        Self {
            data_type,
            complete_type: None,
            direction,
            null_order,
        }
    }
    pub fn with_value_type(
        value_type: novarocks_functions::FunctionValueType,
        direction: RuntimeOrderSortDirection,
        null_order: RuntimeOrderNullOrder,
    ) -> Self {
        Self {
            data_type: value_type.data_type().clone(),
            complete_type: Some(value_type),
            direction,
            null_order,
        }
    }
    pub fn value_type(&self) -> Option<&novarocks_functions::FunctionValueType> {
        self.complete_type.as_ref()
    }
    /// A data frame carries physical key encoding, not a new semantic author.
    /// Its channel retains and validates the complete execution binding.
    pub fn same_physical_key(&self, other: &Self) -> bool {
        self.data_type == other.data_type
            && self.direction == other.direction
            && self.null_order == other.null_order
    }
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }
    pub const fn direction(&self) -> RuntimeOrderSortDirection {
        self.direction
    }
    pub const fn null_order(&self) -> RuntimeOrderNullOrder {
        self.null_order
    }
}
#[derive(Clone)]
enum RuntimeOrderKeys {
    Legacy(Vec<RuntimeOrderKey>),
    Owned(super::owned::OwnedOrderKeys),
}
impl RuntimeOrderKeys {
    fn as_slice(&self) -> &[RuntimeOrderKey] {
        match self {
            Self::Legacy(keys) => keys,
            Self::Owned(keys) => keys.as_slice(),
        }
    }
}
impl fmt::Debug for RuntimeOrderKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}
impl PartialEq for RuntimeOrderKeys {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl Eq for RuntimeOrderKeys {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeOrderContract {
    keys: RuntimeOrderKeys,
    comparator_digest: [u8; 32],
    digest: [u8; 32],
}
/// Original contract rejection and the caller's work failure remain distinct.
#[derive(Debug)]
pub enum RuntimeOrderContractValidationFailure<H> {
    Contract(RuntimeFilterContractViolation),
    Schema(novarocks_local_program::FilterMembershipSchemaError),
    Work(H),
}
impl<H> From<novarocks_local_program::FilterMembershipSchemaError>
    for RuntimeOrderContractValidationFailure<H>
{
    fn from(cause: novarocks_local_program::FilterMembershipSchemaError) -> Self {
        Self::Schema(cause)
    }
}
impl RuntimeOrderContract {
    pub fn new(keys: impl IntoIterator<Item = RuntimeOrderKey>, digest: [u8; 32]) -> Self {
        Self::from_frozen(keys, digest, digest)
    }
    /// Construct from the comparator and order digests frozen by the fragment
    /// contract. The codec never recomputes either digest from mutable plan
    /// state.
    pub fn from_frozen(
        keys: impl IntoIterator<Item = RuntimeOrderKey>,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    ) -> Self {
        Self {
            keys: RuntimeOrderKeys::Legacy(keys.into_iter().collect()),
            comparator_digest,
            digest: order_contract_digest,
        }
    }

    pub(crate) fn from_owned_keys(
        keys: super::owned::OwnedOrderKeys,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    ) -> Self {
        Self {
            keys: RuntimeOrderKeys::Owned(keys),
            comparator_digest,
            digest: order_contract_digest,
        }
    }

    /// Take the already observed, exact key backing without recollecting it.
    pub fn from_frozen_keys(
        keys: Vec<RuntimeOrderKey>,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    ) -> Self {
        Self {
            keys: RuntimeOrderKeys::Legacy(keys),
            comparator_digest,
            digest: order_contract_digest,
        }
    }

    /// Strictly reconstruct an ordered contract frozen in the native fragment
    /// envelope.  The digests are verified from the canonical Execution key
    /// sequence rather than trusted from the caller.
    pub fn from_fragment_contract(
        keys: impl IntoIterator<Item = RuntimeOrderKey>,
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
    ) -> Result<Self, super::RuntimeFilterFailure> {
        let keys: Vec<_> = keys.into_iter().collect();
        Self::validate_fragment_keys_observed(
            &keys,
            comparator_digest,
            order_contract_digest,
            |_| Ok::<_, std::convert::Infallible>(()),
        )
        .map_err(|failure| -> super::RuntimeFilterFailure {
            match failure {
                RuntimeOrderContractValidationFailure::Contract(cause) => cause.into(),
                RuntimeOrderContractValidationFailure::Schema(cause) => cause.into(),
                RuntimeOrderContractValidationFailure::Work(never) => match never {},
            }
        })?;
        Ok(Self::from_frozen_keys(
            keys,
            comparator_digest,
            order_contract_digest,
        ))
    }

    /// Verify the original canonical contract directly from observed key backing.
    /// Positive units observe actual hash input; zero flushes pending work before
    /// a digest verdict. No canonical byte buffer or key clone is constructed.
    pub fn validate_fragment_keys_observed<H>(
        keys: &[RuntimeOrderKey],
        comparator_digest: [u8; 32],
        order_contract_digest: [u8; 32],
        mut work: impl FnMut(usize) -> Result<(), H>,
    ) -> Result<(), RuntimeOrderContractValidationFailure<H>> {
        use RuntimeOrderContractValidationFailure::{Contract, Work};
        const COMPARATOR_DOMAIN: &[u8] = b"novarocks.runtime-filter.comparator";
        const ORDER_CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.order-contract";
        const VERSION: u16 = 1;
        if keys.is_empty() {
            return Err(Contract(fragment_contract_error(
                "ordered contract has no keys",
            )));
        }
        let count = u32::try_from(keys.len()).map_err(|_| {
            Contract(fragment_contract_error(
                "ordered contract key count overflows",
            ))
        })?;
        let mut comparator = Sha256::new();
        work(COMPARATOR_DOMAIN.len()).map_err(Work)?;
        comparator.update(COMPARATOR_DOMAIN);
        work(2).map_err(Work)?;
        comparator.update(VERSION.to_be_bytes());
        let mut order = Sha256::new();
        work(ORDER_CONTRACT_DOMAIN.len()).map_err(Work)?;
        order.update(ORDER_CONTRACT_DOMAIN);
        work(2).map_err(Work)?;
        order.update(VERSION.to_be_bytes());
        let mut emit = |bytes: &[u8]| {
            if !bytes.is_empty() {
                work(bytes.len()).map_err(Work)?;
                comparator.update(bytes);
                work(bytes.len()).map_err(Work)?;
                order.update(bytes);
            }
            Ok::<_, RuntimeOrderContractValidationFailure<H>>(())
        };
        emit(&count.to_be_bytes())?;
        for key in keys {
            if !ordered_key_type_supported(key.data_type()) {
                return Err(Contract(fragment_contract_error(
                    "ordered contract has unsupported key type",
                )));
            }
            novarocks_local_program::visit_filter_key_type(key.data_type(), &mut emit)?;
            emit(&[match key.direction() {
                RuntimeOrderSortDirection::Ascending => 1,
                RuntimeOrderSortDirection::Descending => 2,
            }])?;
            emit(&[match key.null_order() {
                RuntimeOrderNullOrder::First => 1,
                RuntimeOrderNullOrder::Last => 2,
            }])?;
        }
        work(0).map_err(Work)?;
        if <[u8; 32]>::from(comparator.finalize()) != comparator_digest {
            return Err(Contract(fragment_contract_error(
                "ordered comparator digest mismatch",
            )));
        }
        work(1).map_err(Work)?;
        order.update([1]); // inclusive bounds are the only frozen v1 contract.
        work(comparator_digest.len()).map_err(Work)?;
        order.update(comparator_digest);
        work(2).map_err(Work)?;
        order.update(VERSION.to_be_bytes());
        work(0).map_err(Work)?;
        if <[u8; 32]>::from(order.finalize()) != order_contract_digest {
            return Err(Contract(fragment_contract_error(
                "ordered contract digest mismatch",
            )));
        }
        Ok(())
    }
    pub fn keys(&self) -> &[RuntimeOrderKey] {
        self.keys.as_slice()
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub const fn comparator_digest(&self) -> [u8; 32] {
        self.comparator_digest
    }
    pub fn validate_tuple(&self, tuple: &OrderedTuple) -> Result<(), OrderedTupleError> {
        self.validate_borrowed_tuple(OrderedTupleRef::Owned(tuple), || Ok(()))
    }
    fn validate_borrowed_tuple<E: From<OrderedTupleError>>(
        &self,
        tuple: OrderedTupleRef<'_>,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        if tuple.len() != self.keys().len() {
            return Err(OrderedTupleError::ArityMismatch.into());
        }
        for (ordinal, key) in self.keys().iter().enumerate() {
            observe()?;
            if tuple
                .value(ordinal)
                .is_some_and(|value| !scalar_ref_matches(value, key.data_type()))
            {
                return Err(OrderedTupleError::TypeMismatch.into());
            }
        }
        Ok(())
    }
    pub fn compare(
        &self,
        left: &OrderedTuple,
        right: &OrderedTuple,
    ) -> Result<Ordering, OrderedTupleError> {
        self.compare_borrowed(OrderedTupleRef::Owned(left), OrderedTupleRef::Owned(right))
    }

    /// The legacy no-refusal wrapper also accepts an allocation-free view.
    pub fn compare_borrowed(
        &self,
        left: OrderedTupleRef<'_>,
        right: OrderedTupleRef<'_>,
    ) -> Result<Ordering, OrderedTupleError> {
        match self.compare_borrowed_controlled(left, right, &LegacyOrderControl) {
            Ok(ordering) => Ok(ordering),
            Err(ControlledOrderedTupleError::Tuple(error)) => Err(error),
            Err(ControlledOrderedTupleError::Kernel(_)) => {
                unreachable!("the explicit legacy order control cannot refuse")
            }
        }
    }

    /// The original NULL/direction/scalar order with explicit interruption.
    /// Single-key callers borrow UTF-8 directly instead of copying a tuple.
    pub fn compare_borrowed_controlled(
        &self,
        left: OrderedTupleRef<'_>,
        right: OrderedTupleRef<'_>,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<Ordering, ControlledOrderedTupleError> {
        let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
        work.flush()?;
        self.validate_borrowed_tuple(left, || {
            work.step().map_err(ControlledOrderedTupleError::Kernel)
        })?;
        self.validate_borrowed_tuple(right, || {
            work.step().map_err(ControlledOrderedTupleError::Kernel)
        })?;
        for (ordinal, key) in self.keys().iter().enumerate() {
            work.step()?;
            let left = left.value(ordinal);
            let right = right.value(ordinal);
            let ordering = match (left, right) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => match key.null_order() {
                    RuntimeOrderNullOrder::First => Ordering::Less,
                    RuntimeOrderNullOrder::Last => Ordering::Greater,
                },
                (Some(_), None) => match key.null_order() {
                    RuntimeOrderNullOrder::First => Ordering::Greater,
                    RuntimeOrderNullOrder::Last => Ordering::Less,
                },
                (Some(left), Some(right)) => {
                    let ordering = compare_scalar_ref(left, right, control, &mut work)?;
                    match key.direction() {
                        RuntimeOrderSortDirection::Ascending => ordering,
                        RuntimeOrderSortDirection::Descending => ordering.reverse(),
                    }
                }
            };
            if ordering != Ordering::Equal {
                work.finish()?;
                return Ok(ordering);
            }
        }
        work.finish()?;
        Ok(Ordering::Equal)
    }
}

fn ordered_key_type_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    ) || matches!(data_type, DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH)
}

fn fragment_contract_error(detail: &'static str) -> RuntimeFilterContractViolation {
    RuntimeFilterContractViolation::new(
        RuntimeFilterContractViolationKind::ContractMismatch,
        detail,
    )
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeTopKSummaryContract {
    order: RuntimeOrderContract,
    k: u32,
    digest: [u8; 32],
}
impl RuntimeTopKSummaryContract {
    pub const fn new(order: RuntimeOrderContract, k: u32, digest: [u8; 32]) -> Self {
        Self { order, k, digest }
    }
    pub const fn order(&self) -> &RuntimeOrderContract {
        &self.order
    }
    pub const fn k(&self) -> u32 {
        self.k
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn validate_fragment_contract(
        order: &RuntimeOrderContract,
        k: u32,
        digest: [u8; 32],
    ) -> Result<(), RuntimeFilterContractViolation> {
        if k == 0 {
            return Err(fragment_contract_error("TopK contract has zero limit"));
        }
        let mut canonical = Sha256::new();
        canonical.update(b"novarocks.runtime-filter.top-k-summary-contract");
        canonical.update(1u16.to_be_bytes());
        canonical.update(order.digest());
        canonical.update(k.to_be_bytes());
        if <[u8; 32]>::from(canonical.finalize()) != digest {
            return Err(fragment_contract_error("TopK contract digest mismatch"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ContributionCodecExpectation<'a> {
    Membership {
        data_type: &'a DataType,
        digest: [u8; 32],
    },
    OrderedBound(&'a RuntimeOrderContract),
    TopKSummary(&'a RuntimeTopKSummaryContract),
    FinalDomain {
        data_type: &'a DataType,
        digest: [u8; 32],
    },
}
impl<'a> ContributionCodecExpectation<'a> {
    pub const fn membership(data_type: &'a DataType, digest: [u8; 32]) -> Self {
        Self::Membership { data_type, digest }
    }
    pub const fn final_domain(data_type: &'a DataType, digest: [u8; 32]) -> Self {
        Self::FinalDomain { data_type, digest }
    }
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

pub fn encode_contribution(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
) -> Result<EncodedContribution, ContributionCodecError> {
    let (kind, digest, body) = encode_body(contribution, expectation)?;
    let body_len = u64::try_from(body.len()).map_err(|_| ContributionCodecError::LengthOverflow)?;
    let total = HEADER_LEN
        .checked_add(body.len())
        .ok_or(ContributionCodecError::ArithmeticOverflow)?;
    if total > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(total)
        .map_err(|_| ContributionCodecError::ResourceLimit)?;
    visit_contribution_header::<ContributionCodecError>(kind, &digest, body_len, |part| {
        payload.extend_from_slice(part);
        Ok(())
    })?;
    payload.extend_from_slice(&body);
    Ok(EncodedContribution {
        schema_digest: digest,
        payload,
    })
}

fn visit_contribution_header<E>(
    kind: u8,
    digest: &[u8; 32],
    body_len: u64,
    mut write: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    write(MAGIC)?;
    write(&CODEC_VERSION.to_be_bytes())?;
    write(&[kind, 0])?;
    write(digest)?;
    write(&body_len.to_be_bytes())
}

/// Count the SAME ordered canonical visitor without allocating a body.
pub fn borrowed_ordered_bound_encoded_len(
    tuple: OrderedTupleRef<'_>,
    contract: &RuntimeOrderContract,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<usize, ControlledContributionCodecError> {
    let mut body_len = 0usize;
    visit_ordered_tuple(tuple, Some(contract), control, |part| {
        body_len = body_len
            .checked_add(part.len())
            .ok_or(ContributionCodecError::LengthOverflow)?;
        Ok(())
    })?;
    HEADER_LEN
        .checked_add(body_len)
        .ok_or_else(|| ContributionCodecError::LengthOverflow.into())
}

/// Write an ordered NRFC frame into the caller's actual fallible sink. This
/// API grants no storage: the sink owns each allocation and any escaped bytes.
pub fn visit_borrowed_ordered_bound(
    tuple: OrderedTupleRef<'_>,
    contract: &RuntimeOrderContract,
    max_encoded_bytes: usize,
    control: &dyn novarocks_functions::KernelEvaluationControl,
    mut write: impl FnMut(&[u8]) -> Result<(), ControlledContributionCodecError>,
) -> Result<(), ControlledContributionCodecError> {
    let total = borrowed_ordered_bound_encoded_len(tuple, contract, control)?;
    if total > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded.into());
    }
    let body_len =
        u64::try_from(total - HEADER_LEN).map_err(|_| ContributionCodecError::LengthOverflow)?;
    control.checkpoint(0)?;
    visit_contribution_header(2, &contract.digest(), body_len, &mut write)?;
    visit_ordered_tuple(tuple, Some(contract), control, write)
}

pub fn max_encoded_len_for_contribution_budget(
    max_contribution_bytes: usize,
) -> Result<usize, ContributionCodecError> {
    HEADER_LEN
        .checked_add(max_contribution_bytes)
        .ok_or(ContributionCodecError::ArithmeticOverflow)
}

pub fn semantic_contribution_bytes(
    contribution: &RuntimeFilterContribution,
) -> Result<usize, ContributionCodecError> {
    let mut body = Vec::new();
    match contribution {
        RuntimeFilterContribution::Membership(domain) => domain.encode_canonical_into(&mut body)?,
        RuntimeFilterContribution::OrderedBound(update) => {
            visit_tuple(update.bound(), |part| body.extend_from_slice(part));
        }
        RuntimeFilterContribution::TopKSummary(summary) => {
            push_count(&mut body, summary.candidates().len())?;
            for candidate in summary.candidates() {
                visit_tuple(candidate, |part| body.extend_from_slice(part));
            }
        }
        RuntimeFilterContribution::FinalDomain(shard) => {
            body.extend_from_slice(&shard.fence_digest());
            shard.domain().encode_canonical_into(&mut body)?;
        }
    }
    Ok(body.len())
}

pub fn encoded_contribution_len(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
) -> Result<usize, ContributionCodecError> {
    let (_, _, body) = encode_body(contribution, expectation)?;
    max_encoded_len_for_contribution_budget(body.len())
}

pub fn validate_contribution_contract(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
) -> Result<(), ContributionCodecError> {
    encode_body(contribution, expectation).map(|_| ())
}

pub fn decode_contribution(
    payload: &[u8],
    envelope_digest: &[u8; 32],
    expectation: ContributionCodecExpectation<'_>,
    max_encoded_bytes: usize,
) -> Result<RuntimeFilterContribution, ContributionCodecError> {
    if payload.len() > max_encoded_bytes {
        return Err(ContributionCodecError::EncodedSizeExceeded);
    }
    let mut r = Reader::new(payload);
    if r.take(4)? != MAGIC {
        return Err(ContributionCodecError::Malformed);
    }
    if r.u16()? != CODEC_VERSION {
        return Err(ContributionCodecError::UnknownVersion);
    }
    let kind = r.u8()?;
    if !(1..=4).contains(&kind) {
        return Err(ContributionCodecError::UnknownKind);
    }
    if r.u8()? != 0 {
        return Err(ContributionCodecError::InvalidFlags);
    }
    let digest = r.array::<32>()?;
    let body_len = usize::try_from(r.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if body_len < r.len() {
        return Err(ContributionCodecError::TrailingBytes);
    }
    if body_len > r.len() {
        return Err(ContributionCodecError::Truncated);
    }
    let body = r.take(body_len)?;
    if kind != expectation_kind(expectation) {
        return Err(ContributionCodecError::KindMismatch);
    }
    if digest != *envelope_digest || digest != expectation_digest(expectation) {
        return Err(ContributionCodecError::SchemaMismatch);
    }
    let decoded = match expectation {
        ContributionCodecExpectation::Membership { data_type, .. } => {
            RuntimeFilterContribution::Membership(decode_membership(body, data_type)?)
        }
        ContributionCodecExpectation::OrderedBound(contract) => {
            RuntimeFilterContribution::OrderedBound(OrderedBoundUpdate::new(
                contract.digest(),
                decode_tuple(body, contract)?,
            ))
        }
        ContributionCodecExpectation::TopKSummary(contract) => {
            let mut body = Reader::new(body);
            let count =
                usize::try_from(body.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
            if count > contract.k() as usize {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            let mut candidates = Vec::new();
            candidates
                .try_reserve_exact(count)
                .map_err(|_| ContributionCodecError::ResourceLimit)?;
            for _ in 0..count {
                candidates.push(decode_tuple_reader(&mut body, contract.order())?);
            }
            if !body.empty() {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            RuntimeFilterContribution::TopKSummary(TopKSummary::new(contract.digest(), candidates))
        }
        ContributionCodecExpectation::FinalDomain { data_type, .. } => {
            let mut body = Reader::new(body);
            let fence = body.array::<32>()?;
            RuntimeFilterContribution::FinalDomain(FinalDomainShard::new(
                fence,
                decode_membership(body.take(body.len())?, data_type)?,
            ))
        }
    };
    let canonical = encode_contribution(&decoded, expectation, payload.len())?;
    if canonical.payload != payload {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(decoded)
}

fn encode_body(
    contribution: &RuntimeFilterContribution,
    expectation: ContributionCodecExpectation<'_>,
) -> Result<(u8, [u8; 32], Vec<u8>), ContributionCodecError> {
    let mut body = Vec::new();
    let (kind, digest) = match (contribution, expectation) {
        (
            RuntimeFilterContribution::Membership(domain),
            ContributionCodecExpectation::Membership { data_type, digest },
        ) => {
            if !domain.matches_data_type(data_type) {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            domain.encode_canonical_into(&mut body)?;
            (1, digest)
        }
        (
            RuntimeFilterContribution::OrderedBound(update),
            ContributionCodecExpectation::OrderedBound(contract),
        ) => {
            if update.contract_digest() != contract.digest() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            encode_tuple(&update.bound, contract, &mut body)?;
            (2, contract.digest())
        }
        (
            RuntimeFilterContribution::TopKSummary(summary),
            ContributionCodecExpectation::TopKSummary(contract),
        ) => {
            if summary.contract_digest() != contract.digest()
                || summary.candidates().len() > contract.k() as usize
            {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            push_count(&mut body, summary.candidates().len())?;
            for candidate in summary.candidates() {
                encode_tuple(candidate, contract.order(), &mut body)?;
            }
            (3, contract.digest())
        }
        (
            RuntimeFilterContribution::FinalDomain(shard),
            ContributionCodecExpectation::FinalDomain { data_type, digest },
        ) => {
            if !shard.domain().matches_data_type(data_type) {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            body.extend_from_slice(&shard.fence_digest());
            shard.domain().encode_canonical_into(&mut body)?;
            (4, digest)
        }
        _ => return Err(ContributionCodecError::KindMismatch),
    };
    Ok((kind, digest, body))
}
fn expectation_kind(expectation: ContributionCodecExpectation<'_>) -> u8 {
    match expectation {
        ContributionCodecExpectation::Membership { .. } => 1,
        ContributionCodecExpectation::OrderedBound(_) => 2,
        ContributionCodecExpectation::TopKSummary(_) => 3,
        ContributionCodecExpectation::FinalDomain { .. } => 4,
    }
}
fn expectation_digest(expectation: ContributionCodecExpectation<'_>) -> [u8; 32] {
    match expectation {
        ContributionCodecExpectation::Membership { digest, .. }
        | ContributionCodecExpectation::FinalDomain { digest, .. } => digest,
        ContributionCodecExpectation::OrderedBound(contract) => contract.digest(),
        ContributionCodecExpectation::TopKSummary(contract) => contract.digest(),
    }
}

fn encode_tuple(
    tuple: &OrderedTuple,
    contract: &RuntimeOrderContract,
    out: &mut Vec<u8>,
) -> Result<(), ContributionCodecError> {
    visit_ordered_tuple(
        OrderedTupleRef::Owned(tuple),
        Some(contract),
        &LegacyOrderControl,
        |part| {
            out.extend_from_slice(part);
            Ok(())
        },
    )
    .map_err(legacy_codec_error)
}

fn decode_membership(
    bytes: &[u8],
    expected: &DataType,
) -> Result<ValueDomainDelta, ContributionCodecError> {
    let mut r = Reader::new(bytes);
    let version_len =
        usize::try_from(r.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
    if r.take(version_len)? != FINGERPRINT_VERSION_TAG {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let tag = r.u8()?;
    let values = match expected {
        DataType::Boolean => {
            MembershipValues::Boolean(read_set(&mut r, tag, 1, |r| match r.u8()? {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(ContributionCodecError::NonCanonicalPayload),
            })?)
        }
        DataType::Int8 => MembershipValues::Int8(read_set(&mut r, tag, 2, |r| r.i8())?),
        DataType::Int16 => MembershipValues::Int16(read_set(&mut r, tag, 3, |r| r.i16())?),
        DataType::Int32 => MembershipValues::Int32(read_set(&mut r, tag, 4, |r| r.i32())?),
        DataType::Int64 => MembershipValues::Int64(read_set(&mut r, tag, 5, |r| r.i64())?),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            MembershipValues::LargeInt(read_set(&mut r, tag, 6, |r| r.i128())?)
        }
        DataType::Float32 => MembershipValues::Float32(read_set(&mut r, tag, 7, |r| {
            let bits = r.u32()?;
            let x = f32::from_bits(bits);
            if (x == 0.0 && bits != 0) || (x.is_nan() && bits != 0x7fc0_0000) {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            Ok(CanonicalF32(bits))
        })?),
        DataType::Float64 => MembershipValues::Float64(read_set(&mut r, tag, 8, |r| {
            let bits = r.u64()?;
            let x = f64::from_bits(bits);
            if (x == 0.0 && bits != 0) || (x.is_nan() && bits != 0x7ff8_0000_0000_0000) {
                return Err(ContributionCodecError::NonCanonicalPayload);
            }
            Ok(CanonicalF64(bits))
        })?),
        DataType::Utf8 => {
            expect_tag(tag, 9)?;
            let count = r.count()?;
            let mut values = BTreeSet::new();
            for _ in 0..count {
                let len = usize::try_from(r.u64()?)
                    .map_err(|_| ContributionCodecError::LengthOverflow)?;
                let value = std::str::from_utf8(r.take(len)?)
                    .map_err(|_| ContributionCodecError::NonCanonicalPayload)?
                    .to_owned();
                if !values.insert(value) {
                    return Err(ContributionCodecError::NonCanonicalPayload);
                }
            }
            MembershipValues::Utf8(values)
        }
        DataType::Date32 => MembershipValues::Date32(read_set(&mut r, tag, 10, |r| r.i32())?),
        DataType::Timestamp(unit, timezone) => {
            expect_tag(tag, 11)?;
            if r.u8()? != time_unit_tag(unit) {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            let actual_timezone = match r.u8()? {
                0 => None,
                1 => {
                    let len = usize::try_from(r.u64()?)
                        .map_err(|_| ContributionCodecError::LengthOverflow)?;
                    Some(
                        std::str::from_utf8(r.take(len)?)
                            .map_err(|_| ContributionCodecError::NonCanonicalPayload)?
                            .to_owned(),
                    )
                }
                _ => return Err(ContributionCodecError::NonCanonicalPayload),
            };
            if actual_timezone.as_deref() != timezone.as_deref() {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            MembershipValues::Timestamp {
                unit: *unit,
                timezone: actual_timezone,
                values: read_set_count(&mut r, |r| r.i64())?,
            }
        }
        DataType::Decimal128(precision, scale) => {
            expect_tag(tag, 12)?;
            if r.u8()? != *precision || r.u8()? as i8 != *scale {
                return Err(ContributionCodecError::SchemaMismatch);
            }
            let values = read_set_count(&mut r, |r| r.i128())?;
            validate_decimal(*precision, *scale, &values)?;
            MembershipValues::Decimal128 {
                precision: *precision,
                scale: *scale,
                values,
            }
        }
        _ => return Err(ContributionCodecError::SchemaMismatch),
    };
    let contains_null = match r.u8()? {
        0 => false,
        1 => true,
        _ => return Err(ContributionCodecError::NonCanonicalPayload),
    };
    if !r.empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(ValueDomainDelta::new(values, contains_null))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipContributionEncodingUnavailable {
    ResourceOrSize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipContributionEncodingOutcome {
    Contributions(Vec<ExecutionContribution>),
    Unavailable(MembershipContributionEncodingUnavailable),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipContributionEncodingError {
    ContractViolation {
        kind: RuntimeFilterContractViolationKind,
        detail: novarocks_failure_contract::BoundedDetail,
    },
    TypeMismatch {
        expected: novarocks_failure_contract::BoundedDetail,
        actual: novarocks_failure_contract::BoundedDetail,
    },
    UnsupportedType(novarocks_failure_contract::BoundedDetail),
    InvalidArray {
        data_type: novarocks_failure_contract::BoundedDetail,
        detail: novarocks_failure_contract::BoundedDetail,
    },
    InvalidDecimal {
        precision: u8,
        scale: i8,
        source: ContributionCodecError,
    },
}

impl fmt::Display for MembershipContributionEncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContractViolation { kind, detail } => write!(
                formatter,
                "runtime-filter contract violation {kind:?}: {detail}"
            ),
            Self::TypeMismatch { expected, actual } => write!(
                formatter,
                "runtime-filter membership type mismatch: expected {expected}, got {actual}"
            ),
            Self::UnsupportedType(data_type) => write!(
                formatter,
                "unsupported runtime-filter membership type: {data_type}"
            ),
            Self::InvalidArray { data_type, detail } => write!(
                formatter,
                "invalid runtime-filter membership array for {data_type}: {detail}"
            ),
            Self::InvalidDecimal {
                precision,
                scale,
                source,
            } => write!(
                formatter,
                "invalid runtime-filter Decimal128({precision}, {scale}) membership value: {source}"
            ),
        }
    }
}
impl Error for MembershipContributionEncodingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidDecimal { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for MembershipContributionEncodingError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self {
            Self::ContractViolation { kind, .. } => return kind.failure_fact(),
            Self::InvalidDecimal { source, .. } => return source.failure_fact(),
            Self::TypeMismatch { .. } => ExecutionReason::FilterMembershipType,
            Self::UnsupportedType(_) => ExecutionReason::FilterMembershipUnsupported,
            Self::InvalidArray { .. } => ExecutionReason::FilterMembershipArray,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}

/// The result of deriving a canonical final-domain payload from a finalized
/// aggregate key column. The budget applies to exactly the canonical domain
/// bytes retained by `RuntimeFilterFinalDomain`; a fence is not serialized
/// into this payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueDomainArrayEncodingOutcome {
    ValueDomain(ValueDomainDelta),
    Unavailable(MembershipContributionEncodingUnavailable),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalDomainArrayEncodingOutcome {
    FinalDomain(RuntimeFilterFinalDomain),
    Unavailable(MembershipContributionEncodingUnavailable),
}

/// Derive one typed membership domain from an Arrow column under the frozen
/// data type. This has no producer/session authority and is therefore usable
/// by FinalDomain after its aggregate has been sealed.
pub fn derive_value_domain_from_array(
    data_type: &DataType,
    array: &ArrayRef,
    max_canonical_bytes: usize,
) -> Result<ValueDomainArrayEncodingOutcome, MembershipContributionEncodingError> {
    if array.data_type() != data_type {
        return Err(MembershipContributionEncodingError::TypeMismatch {
            expected: type_description(data_type),
            actual: type_description(array.data_type()),
        });
    }
    let domain = value_domain_from_array(array.as_ref(), data_type)?;
    match encode_value_domain(&domain, max_canonical_bytes) {
        Ok(_) => Ok(ValueDomainArrayEncodingOutcome::ValueDomain(domain)),
        Err(
            ContributionCodecError::EncodedSizeExceeded
            | ContributionCodecError::ResourceLimit
            | ContributionCodecError::LengthOverflow
            | ContributionCodecError::ArithmeticOverflow,
        ) => Ok(ValueDomainArrayEncodingOutcome::Unavailable(
            MembershipContributionEncodingUnavailable::ResourceOrSize,
        )),
        Err(error) => Err(contract_error(
            RuntimeFilterContractViolationKind::ContractMismatch,
            error.to_string(),
        )),
    }
}

/// Encode a sealed FinalDomain value directly from Arrow data. The returned
/// payload is canonical typed-domain bytes; `fence_digest` is intentionally
/// carried only by the execution final-domain wrapper and is validated by the
/// participant-owned completion authority.
pub fn encode_final_domain_from_array(
    data_type: &DataType,
    fence_digest: [u8; 32],
    array: &ArrayRef,
    max_canonical_bytes: usize,
) -> Result<FinalDomainArrayEncodingOutcome, MembershipContributionEncodingError> {
    let outcome = derive_value_domain_from_array(data_type, array, max_canonical_bytes)?;
    match outcome {
        ValueDomainArrayEncodingOutcome::Unavailable(reason) => {
            Ok(FinalDomainArrayEncodingOutcome::Unavailable(reason))
        }
        ValueDomainArrayEncodingOutcome::ValueDomain(domain) => {
            let typed = RuntimeFilterFinalDomain::from_value_domain(
                &domain,
                fence_digest,
                max_canonical_bytes,
            )
            .map_err(|error| {
                contract_error(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    error.to_string(),
                )
            })?;
            Ok(FinalDomainArrayEncodingOutcome::FinalDomain(typed))
        }
    }
}

/// Encode one complete Arrow membership build input against a frozen producer
/// contract. A contribution either fits in its entirety or produces the
/// typed unavailable outcome; the encoder never submits a partial domain.
pub fn encode_membership_contributions(
    producer: &RuntimeFilterProducerContract,
    array: &ArrayRef,
    max_encoded_bytes: usize,
) -> Result<MembershipContributionEncodingOutcome, MembershipContributionEncodingError> {
    if producer.kind() != RuntimeFilterProducerKind::Membership {
        return Err(contract_error(
            RuntimeFilterContractViolationKind::RoleMismatch,
            "membership encoder requires a membership producer contract",
        ));
    }
    let RuntimeFilterExecutionContract::Membership(schema) = producer.contract() else {
        return Err(contract_error(
            RuntimeFilterContractViolationKind::ContractMismatch,
            "membership producer does not carry a membership execution contract",
        ));
    };
    let expected = schema.data_type().clone();
    if array.data_type() != &expected {
        return Err(MembershipContributionEncodingError::TypeMismatch {
            expected: type_description(&expected),
            actual: type_description(array.data_type()),
        });
    }
    let domain = value_domain_from_array(array.as_ref(), &expected)?;
    if domain.values().is_empty() && !domain.contains_null() {
        return Ok(MembershipContributionEncodingOutcome::Contributions(
            Vec::new(),
        ));
    }
    let typed = RuntimeFilterContribution::membership(domain);
    let encoded = match encode_contribution(
        &typed,
        ContributionCodecExpectation::membership(&expected, schema.digest()),
        max_encoded_bytes,
    ) {
        Ok(encoded) => encoded,
        Err(
            ContributionCodecError::EncodedSizeExceeded
            | ContributionCodecError::ResourceLimit
            | ContributionCodecError::LengthOverflow
            | ContributionCodecError::ArithmeticOverflow,
        ) => {
            return Ok(MembershipContributionEncodingOutcome::Unavailable(
                MembershipContributionEncodingUnavailable::ResourceOrSize,
            ));
        }
        Err(error) => {
            return Err(contract_error(
                RuntimeFilterContractViolationKind::ContractMismatch,
                error.to_string(),
            ));
        }
    };
    let (contract_digest, canonical_bytes) = encoded.into_parts();
    Ok(MembershipContributionEncodingOutcome::Contributions(vec![
        ExecutionContribution::new(
            RuntimeFilterContributionKind::Membership,
            contract_digest,
            canonical_bytes,
        ),
    ]))
}

fn contract_error(
    kind: RuntimeFilterContractViolationKind,
    detail: impl Into<String>,
) -> MembershipContributionEncodingError {
    MembershipContributionEncodingError::ContractViolation {
        kind,
        detail: novarocks_failure_contract::BoundedDetail::truncating(&detail.into()),
    }
}

fn value_domain_from_array(
    array: &dyn Array,
    expected: &DataType,
) -> Result<ValueDomainDelta, MembershipContributionEncodingError> {
    let data = array.to_data();
    if data.data_type() != expected || data.len() != array.len() {
        return Err(invalid_array(
            expected,
            "Arrow physical array does not match its declared data type or length",
        ));
    }
    macro_rules! collect_fixed {
        ($width:expr, $convert:expr, $constructor:ident) => {{
            let mut values = Vec::new();
            let mut contains_null = false;
            for row in 0..data.len() {
                if row_is_null(&data, row, expected)? {
                    contains_null = true;
                } else {
                    values.push($convert(fixed_value::<$width>(&data, row, expected)?));
                }
            }
            ValueDomainDelta::new(MembershipValues::$constructor(values), contains_null)
        }};
    }
    Ok(match expected {
        DataType::Boolean => {
            let mut values = Vec::new();
            let mut contains_null = false;
            for row in 0..data.len() {
                if row_is_null(&data, row, expected)? {
                    contains_null = true;
                } else {
                    values.push(boolean_value(&data, row, expected)?);
                }
            }
            ValueDomainDelta::new(MembershipValues::boolean(values), contains_null)
        }
        DataType::Int8 => collect_fixed!(1, |bytes: [u8; 1]| i8::from_ne_bytes(bytes), int8),
        DataType::Int16 => collect_fixed!(2, |bytes: [u8; 2]| i16::from_ne_bytes(bytes), int16),
        DataType::Int32 => collect_fixed!(4, |bytes: [u8; 4]| i32::from_ne_bytes(bytes), int32),
        DataType::Int64 => collect_fixed!(8, |bytes: [u8; 8]| i64::from_ne_bytes(bytes), int64),
        DataType::Date32 => collect_fixed!(4, |bytes: [u8; 4]| i32::from_ne_bytes(bytes), date32),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            let mut collected = Vec::new();
            let mut contains_null = false;
            for row in 0..data.len() {
                if row_is_null(&data, row, expected)? {
                    contains_null = true;
                } else {
                    let bytes = fixed_value::<16>(&data, row, expected)?;
                    let value = novarocks_types::largeint::i128_from_be_bytes(&bytes)
                        .map_err(|error| invalid_array(expected, error.to_string()))?;
                    collected.push(value);
                }
            }
            ValueDomainDelta::new(MembershipValues::large_int(collected), contains_null)
        }
        DataType::Float32 => collect_fixed!(4, |bytes: [u8; 4]| f32::from_ne_bytes(bytes), float32),
        DataType::Float64 => collect_fixed!(8, |bytes: [u8; 8]| f64::from_ne_bytes(bytes), float64),
        DataType::Utf8 => {
            let mut values = Vec::new();
            let mut contains_null = false;
            for row in 0..data.len() {
                if row_is_null(&data, row, expected)? {
                    contains_null = true;
                } else {
                    values.push(utf8_value(&data, row, expected)?.to_string());
                }
            }
            ValueDomainDelta::new(MembershipValues::utf8(values), contains_null)
        }
        DataType::Timestamp(unit, timezone) => {
            let mut values = Vec::new();
            let mut contains_null = false;
            for row in 0..data.len() {
                if row_is_null(&data, row, expected)? {
                    contains_null = true;
                } else {
                    values.push(i64::from_ne_bytes(fixed_value::<8>(&data, row, expected)?));
                }
            }
            ValueDomainDelta::new(
                MembershipValues::timestamp(
                    *unit,
                    timezone.as_ref().map(|value| value.as_ref()),
                    values,
                ),
                contains_null,
            )
        }
        DataType::Decimal128(precision, scale) => {
            let mut collected = Vec::new();
            let mut contains_null = false;
            for row in 0..data.len() {
                if row_is_null(&data, row, expected)? {
                    contains_null = true;
                } else {
                    collected.push(i128::from_ne_bytes(fixed_value::<16>(
                        &data, row, expected,
                    )?));
                }
            }
            let membership =
                MembershipValues::decimal128(*precision, *scale, collected).map_err(|error| {
                    MembershipContributionEncodingError::InvalidDecimal {
                        precision: *precision,
                        scale: *scale,
                        source: error,
                    }
                })?;
            ValueDomainDelta::new(membership, contains_null)
        }
        other => {
            return Err(MembershipContributionEncodingError::UnsupportedType(
                type_description(other),
            ));
        }
    })
}

fn invalid_array(
    data_type: &DataType,
    detail: impl Into<String>,
) -> MembershipContributionEncodingError {
    MembershipContributionEncodingError::InvalidArray {
        data_type: type_description(data_type),
        detail: novarocks_failure_contract::BoundedDetail::truncating(&detail.into()),
    }
}

fn row_is_null(
    data: &arrow::array::ArrayData,
    row: usize,
    expected: &DataType,
) -> Result<bool, MembershipContributionEncodingError> {
    super::evaluator::row_is_null(data, row)
        .map_err(|error| invalid_array(expected, error.to_string()))
}

fn boolean_value(
    data: &arrow::array::ArrayData,
    row: usize,
    expected: &DataType,
) -> Result<bool, MembershipContributionEncodingError> {
    super::evaluator::boolean_value(data, row)
        .map_err(|error| invalid_array(expected, error.to_string()))
}

fn fixed_value<const WIDTH: usize>(
    data: &arrow::array::ArrayData,
    row: usize,
    expected: &DataType,
) -> Result<[u8; WIDTH], MembershipContributionEncodingError> {
    super::evaluator::fixed_value(data, row)
        .map_err(|error| invalid_array(expected, error.to_string()))
}

fn utf8_value<'a>(
    data: &'a arrow::array::ArrayData,
    row: usize,
    expected: &DataType,
) -> Result<&'a str, MembershipContributionEncodingError> {
    super::evaluator::utf8_value(data, row)
        .map_err(|error| invalid_array(expected, error.to_string()))
}

fn decode_tuple(
    bytes: &[u8],
    contract: &RuntimeOrderContract,
) -> Result<OrderedTuple, ContributionCodecError> {
    let mut reader = Reader::new(bytes);
    let tuple = decode_tuple_reader(&mut reader, contract)?;
    if !reader.empty() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(tuple)
}
fn decode_tuple_reader(
    reader: &mut Reader<'_>,
    contract: &RuntimeOrderContract,
) -> Result<OrderedTuple, ContributionCodecError> {
    let arity = reader.count()?;
    if arity != contract.keys().len() {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(arity)
        .map_err(|_| ContributionCodecError::ResourceLimit)?;
    for key in contract.keys() {
        values.push(match reader.u8()? {
            0 => None,
            1 => Some(decode_scalar(reader, key.data_type())?),
            _ => return Err(ContributionCodecError::NonCanonicalPayload),
        });
    }
    Ok(OrderedTuple::new(values))
}
fn decode_scalar(
    r: &mut Reader<'_>,
    data_type: &DataType,
) -> Result<OrderedScalar, ContributionCodecError> {
    Ok(match data_type {
        DataType::Boolean => OrderedScalar::Boolean(match r.u8()? {
            0 => false,
            1 => true,
            _ => return Err(ContributionCodecError::NonCanonicalPayload),
        }),
        DataType::Int8 => OrderedScalar::Int8(r.i8()?),
        DataType::Int16 => OrderedScalar::Int16(r.i16()?),
        DataType::Int32 => OrderedScalar::Int32(r.i32()?),
        DataType::Int64 => OrderedScalar::Int64(r.i64()?),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
            OrderedScalar::LargeInt(r.i128()?)
        }
        DataType::Utf8 => {
            let len =
                usize::try_from(r.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)?;
            OrderedScalar::Utf8(
                std::str::from_utf8(r.take(len)?)
                    .map_err(|_| ContributionCodecError::NonCanonicalPayload)?
                    .to_owned(),
            )
        }
        DataType::Date32 => OrderedScalar::Date32(r.i32()?),
        DataType::Timestamp(_, _) => OrderedScalar::Timestamp(r.i64()?),
        DataType::Decimal128(_, _) => OrderedScalar::Decimal128(r.i128()?),
        _ => return Err(ContributionCodecError::SchemaMismatch),
    })
}
fn scalar_ref_matches(value: OrderedScalarRef<'_>, data_type: &DataType) -> bool {
    match (value, data_type) {
        (OrderedScalarRef::Boolean(_), DataType::Boolean)
        | (OrderedScalarRef::Int8(_), DataType::Int8)
        | (OrderedScalarRef::Int16(_), DataType::Int16)
        | (OrderedScalarRef::Int32(_), DataType::Int32)
        | (OrderedScalarRef::Int64(_), DataType::Int64)
        | (OrderedScalarRef::Utf8(_), DataType::Utf8)
        | (OrderedScalarRef::Date32(_), DataType::Date32)
        | (OrderedScalarRef::Timestamp(_), DataType::Timestamp(_, _)) => true,
        (OrderedScalarRef::Decimal128(value), DataType::Decimal128(precision, _)) => 10_i128
            .checked_pow((*precision).into())
            .is_some_and(|limit| value > -limit && value < limit),
        (OrderedScalarRef::LargeInt(_), DataType::FixedSizeBinary(width)) => {
            *width == LARGEINT_BYTE_WIDTH
        }
        _ => false,
    }
}

fn read_set<T: Ord>(
    r: &mut Reader<'_>,
    tag: u8,
    expected_tag: u8,
    mut read: impl FnMut(&mut Reader<'_>) -> Result<T, ContributionCodecError>,
) -> Result<BTreeSet<T>, ContributionCodecError> {
    expect_tag(tag, expected_tag)?;
    read_set_count(r, |r| read(r))
}
fn read_set_count<T: Ord>(
    r: &mut Reader<'_>,
    mut read: impl FnMut(&mut Reader<'_>) -> Result<T, ContributionCodecError>,
) -> Result<BTreeSet<T>, ContributionCodecError> {
    let count = r.count()?;
    let mut values = BTreeSet::new();
    for _ in 0..count {
        let value = read(r)?;
        if !values.insert(value) {
            return Err(ContributionCodecError::NonCanonicalPayload);
        }
    }
    Ok(values)
}
fn expect_tag(actual: u8, expected: u8) -> Result<(), ContributionCodecError> {
    if actual == expected {
        Ok(())
    } else if (1..=12).contains(&actual) {
        Err(ContributionCodecError::SchemaMismatch)
    } else {
        Err(ContributionCodecError::NonCanonicalPayload)
    }
}
fn validate_decimal(
    precision: u8,
    scale: i8,
    values: &BTreeSet<i128>,
) -> Result<(), ContributionCodecError> {
    if !(1..=38).contains(&precision) || scale > 38 || (scale > 0 && scale as u8 > precision) {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    let bound = 10_i128
        .checked_pow(precision.into())
        .ok_or(ContributionCodecError::ArithmeticOverflow)?;
    if values
        .iter()
        .any(|value| *value <= -bound || *value >= bound)
    {
        return Err(ContributionCodecError::NonCanonicalPayload);
    }
    Ok(())
}
fn time_unit_tag(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    }
}
fn ordered_replay_digest(contract_digest: [u8; 32], tuple: &OrderedTuple) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.runtime-filter.ordered-bound-replay");
    digest.update(1_u16.to_be_bytes());
    digest.update(contract_digest);
    visit_tuple(tuple, |part| digest.update(part));
    digest.finalize().into()
}
fn topk_replay_digest(contract_digest: [u8; 32], candidates: &[OrderedTuple]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.runtime-filter.top-k-summary-replay");
    digest.update(1_u16.to_be_bytes());
    digest.update(contract_digest);
    digest.update((candidates.len() as u64).to_be_bytes());
    for candidate in candidates {
        visit_tuple(candidate, |part| digest.update(part));
    }
    digest.finalize().into()
}
fn final_domain_replay_digest(fence_digest: [u8; 32], domain: &ValueDomainDelta) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.runtime-filter.final-domain-shard");
    digest.update(1_u16.to_be_bytes());
    digest.update(32_u64.to_be_bytes());
    digest.update(fence_digest);
    digest.update(32_u64.to_be_bytes());
    digest.update(domain.fingerprint());
    digest.finalize().into()
}
fn visit_tuple(tuple: &OrderedTuple, mut visit: impl FnMut(&[u8])) {
    match visit_ordered_tuple(
        OrderedTupleRef::Owned(tuple),
        None,
        &LegacyOrderControl,
        |part| {
            visit(part);
            Ok(())
        },
    ) {
        Ok(()) => {}
        Err(_) => unreachable!("an in-memory legacy tuple fits its canonical scalar vocabulary"),
    }
}

fn visit_ordered_tuple(
    tuple: OrderedTupleRef<'_>,
    contract: Option<&RuntimeOrderContract>,
    control: &dyn novarocks_functions::KernelEvaluationControl,
    mut visit: impl FnMut(&[u8]) -> Result<(), ControlledContributionCodecError>,
) -> Result<(), ControlledContributionCodecError> {
    let mut work = novarocks_functions::EvaluationCheckpoints::new(control);
    work.flush()?;
    if contract.is_some_and(|contract| tuple.len() != contract.keys().len()) {
        return Err(ContributionCodecError::SchemaMismatch.into());
    }
    let arity = u64::try_from(tuple.len()).map_err(|_| ContributionCodecError::LengthOverflow)?;
    visit(&arity.to_be_bytes())?;
    for ordinal in 0..tuple.len() {
        work.step()?;
        match tuple.value(ordinal) {
            None => visit(&[0])?,
            Some(value) => {
                if contract.is_some_and(|contract| {
                    !scalar_ref_matches(value, contract.keys()[ordinal].data_type())
                }) {
                    return Err(ContributionCodecError::SchemaMismatch.into());
                }
                visit(&[1])?;
                visit_scalar_ref(value, control, &mut work, &mut visit)?;
            }
        }
    }
    work.finish()?;
    Ok(())
}

fn visit_scalar_ref(
    value: OrderedScalarRef<'_>,
    control: &dyn novarocks_functions::KernelEvaluationControl,
    work: &mut novarocks_functions::EvaluationCheckpoints<'_>,
    visit: &mut impl FnMut(&[u8]) -> Result<(), ControlledContributionCodecError>,
) -> Result<(), ControlledContributionCodecError> {
    match value {
        OrderedScalarRef::Boolean(value) => visit(&[u8::from(value)])?,
        OrderedScalarRef::Int8(value) => visit(&value.to_be_bytes())?,
        OrderedScalarRef::Int16(value) => visit(&value.to_be_bytes())?,
        OrderedScalarRef::Int32(value) | OrderedScalarRef::Date32(value) => {
            visit(&value.to_be_bytes())?
        }
        OrderedScalarRef::Int64(value) | OrderedScalarRef::Timestamp(value) => {
            visit(&value.to_be_bytes())?
        }
        OrderedScalarRef::LargeInt(value) | OrderedScalarRef::Decimal128(value) => {
            visit(&value.to_be_bytes())?
        }
        OrderedScalarRef::Utf8(value) => {
            let len =
                u64::try_from(value.len()).map_err(|_| ContributionCodecError::LengthOverflow)?;
            visit(&len.to_be_bytes())?;
            work.flush()?;
            for part in value
                .as_bytes()
                .chunks(novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize)
            {
                control.checkpoint(part.len() as u32)?;
                visit(part)?;
            }
        }
    }
    Ok(())
}
/// Compare borrowed bytes in bounded public work windows. This is the same
/// byte-order leaf used by ordered UTF8 and resident membership probes; it
/// neither validates UTF8 nor supplies a frozen comparison contract.
pub fn compare_bytes_controlled(
    a: &[u8],
    b: &[u8],
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<Ordering, novarocks_functions::KernelFailure> {
    let mut ordering = Ordering::Equal;
    let quantum = novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize;
    for (a, b) in a.chunks(quantum).zip(b.chunks(quantum)) {
        ordering = a.cmp(b);
        // Each public byte comparison has bounded input. Report its
        // actual work before exposing even an early unequal result.
        control.checkpoint(a.len().max(b.len()) as u32)?;
        if ordering != Ordering::Equal {
            break;
        }
    }
    Ok(if ordering == Ordering::Equal {
        a.len().cmp(&b.len())
    } else {
        ordering
    })
}

fn compare_scalar_ref(
    left: OrderedScalarRef<'_>,
    right: OrderedScalarRef<'_>,
    control: &dyn novarocks_functions::KernelEvaluationControl,
    work: &mut novarocks_functions::EvaluationCheckpoints<'_>,
) -> Result<Ordering, ControlledOrderedTupleError> {
    Ok(match (left, right) {
        (OrderedScalarRef::Boolean(a), OrderedScalarRef::Boolean(b)) => a.cmp(&b),
        (OrderedScalarRef::Int8(a), OrderedScalarRef::Int8(b)) => a.cmp(&b),
        (OrderedScalarRef::Int16(a), OrderedScalarRef::Int16(b)) => a.cmp(&b),
        (OrderedScalarRef::Int32(a), OrderedScalarRef::Int32(b)) => a.cmp(&b),
        (OrderedScalarRef::Int64(a), OrderedScalarRef::Int64(b)) => a.cmp(&b),
        (OrderedScalarRef::LargeInt(a), OrderedScalarRef::LargeInt(b)) => a.cmp(&b),
        (OrderedScalarRef::Utf8(a), OrderedScalarRef::Utf8(b)) => {
            work.flush()?;
            compare_bytes_controlled(a.as_bytes(), b.as_bytes(), control)?
        }
        (OrderedScalarRef::Date32(a), OrderedScalarRef::Date32(b)) => a.cmp(&b),
        (OrderedScalarRef::Timestamp(a), OrderedScalarRef::Timestamp(b)) => a.cmp(&b),
        (OrderedScalarRef::Decimal128(a), OrderedScalarRef::Decimal128(b)) => a.cmp(&b),
        _ => return Err(OrderedTupleError::TypeMismatch.into()),
    })
}
fn push_count(out: &mut Vec<u8>, count: usize) -> Result<(), ContributionSizeError> {
    out.extend_from_slice(
        &u64::try_from(count)
            .map_err(|_| ContributionSizeError::LengthExceedsCanonicalRange)?
            .to_be_bytes(),
    );
    Ok(())
}
fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ContributionSizeError> {
    push_count(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
}
impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    const fn len(&self) -> usize {
        self.bytes.len()
    }
    const fn empty(&self) -> bool {
        self.bytes.is_empty()
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], ContributionCodecError> {
        let (value, remaining) = self
            .bytes
            .split_at_checked(n)
            .ok_or(ContributionCodecError::Truncated)?;
        self.bytes = remaining;
        Ok(value)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], ContributionCodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ContributionCodecError::Truncated)
    }
    fn u8(&mut self) -> Result<u8, ContributionCodecError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ContributionCodecError> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, ContributionCodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, ContributionCodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn i8(&mut self) -> Result<i8, ContributionCodecError> {
        Ok(i8::from_be_bytes(self.array()?))
    }
    fn i16(&mut self) -> Result<i16, ContributionCodecError> {
        Ok(i16::from_be_bytes(self.array()?))
    }
    fn i32(&mut self) -> Result<i32, ContributionCodecError> {
        Ok(i32::from_be_bytes(self.array()?))
    }
    fn i64(&mut self) -> Result<i64, ContributionCodecError> {
        Ok(i64::from_be_bytes(self.array()?))
    }
    fn i128(&mut self) -> Result<i128, ContributionCodecError> {
        Ok(i128::from_be_bytes(self.array()?))
    }
    fn count(&mut self) -> Result<usize, ContributionCodecError> {
        usize::try_from(self.u64()?).map_err(|_| ContributionCodecError::LengthOverflow)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::runtime_filter::{
        RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterExecutionContract,
        RuntimeFilterMembershipSchema, RuntimeFilterProducerContract,
    };
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::DataType;

    /// Frozen NRFC header/tuple framing is independent of the shared visitor.
    #[test]
    fn ordered_topn_owned_borrowed_codec_keeps_literal_utf8_frames_and_exact_size() {
        let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [7; 32]);
        for value in [None, Some(""), Some("a\0"), Some("海港")] {
            let mut body = 1_u64.to_be_bytes().to_vec();
            match value {
                None => body.push(0),
                Some(value) => {
                    body.push(1);
                    body.extend_from_slice(&(value.len() as u64).to_be_bytes());
                    body.extend_from_slice(value.as_bytes());
                }
            }
            let mut expected = b"NRFC\0\x01\x02\0".to_vec();
            expected.extend_from_slice(&[7; 32]);
            expected.extend_from_slice(&(body.len() as u64).to_be_bytes());
            expected.extend_from_slice(&body);
            let tuple = OrderedTupleRef::Single(value.map(OrderedScalarRef::Utf8));
            assert_eq!(
                borrowed_ordered_bound_encoded_len(tuple, &contract, &LegacyOrderControl),
                Ok(expected.len())
            );
            let mut actual = Vec::new();
            visit_borrowed_ordered_bound(
                tuple,
                &contract,
                expected.len(),
                &LegacyOrderControl,
                |part| {
                    actual.extend_from_slice(part);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(actual, expected);
            let owned = OrderedTuple::new([value.map(|value| OrderedScalar::Utf8(value.into()))]);
            let update = OrderedBoundUpdate::try_new(&contract, owned).unwrap();
            let encoded = encode_contribution(
                &RuntimeFilterContribution::OrderedBound(update),
                ContributionCodecExpectation::OrderedBound(&contract),
                expected.len(),
            )
            .unwrap();
            assert_eq!(encoded.payload(), expected);
            let mut called = false;
            assert_eq!(
                visit_borrowed_ordered_bound(
                    tuple,
                    &contract,
                    expected.len() - 1,
                    &LegacyOrderControl,
                    |_| {
                        called = true;
                        Ok(())
                    }
                ),
                Err(ControlledContributionCodecError::Codec(
                    ContributionCodecError::EncodedSizeExceeded
                ))
            );
            assert!(!called, "size refusal must precede the actual sink");
        }
    }

    #[test]
    fn ordered_topn_owned_borrowed_codec_bounds_control_and_retains_sink_refusal() {
        use novarocks_functions::{
            KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
        };
        use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
        struct Control(AtomicU32);
        impl KernelEvaluationControl for Control {
            fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
                assert!(units <= MAX_UNOBSERVED_KERNEL_WORK);
                let total = self.0.fetch_add(units, AtomicOrdering::Relaxed) + units;
                if total >= 2 * MAX_UNOBSERVED_KERNEL_WORK {
                    Err(KernelFailure::Cancelled)
                } else {
                    Ok(())
                }
            }
            fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
                panic!("codec visitor never waits")
            }
        }
        let text = "x".repeat(1 << 20);
        let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [5; 32]);
        let tuple = OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(&text)));
        let control = Control(AtomicU32::new(0));
        assert_eq!(
            borrowed_ordered_bound_encoded_len(tuple, &contract, &control),
            Err(ControlledContributionCodecError::Kernel(
                KernelFailure::Cancelled
            ))
        );
        assert!(control.0.load(AtomicOrdering::Relaxed) < 4 * MAX_UNOBSERVED_KERNEL_WORK);
        let tuple = OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8("m")));
        let size =
            borrowed_ordered_bound_encoded_len(tuple, &contract, &LegacyOrderControl).unwrap();
        let mut calls = 0;
        let failure =
            visit_borrowed_ordered_bound(tuple, &contract, size, &LegacyOrderControl, |_| {
                calls += 1;
                Err(ControlledContributionCodecError::Kernel(
                    KernelFailure::ResourceExhausted,
                ))
            });
        assert_eq!(
            failure,
            Err(ControlledContributionCodecError::Kernel(
                KernelFailure::ResourceExhausted
            ))
        );
        assert_eq!(calls, 1, "a failed sink must not be called again");
        assert_eq!(
            borrowed_ordered_bound_encoded_len(
                OrderedTupleRef::Single(Some(OrderedScalarRef::Int64(7))),
                &contract,
                &LegacyOrderControl
            ),
            Err(ControlledContributionCodecError::Codec(
                ContributionCodecError::SchemaMismatch
            ))
        );
    }

    /// Owner-local contract checks: std byte ordering is the independent
    /// oracle; declared strings need no service, timer, thread or cleanup.
    #[test]
    fn ordered_topn_owned_borrowed_utf8_matches_legacy_and_byte_oracle() {
        let boundary_prefix = "x".repeat(novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize);
        let values = [
            None,
            Some(String::new()),
            Some("a".to_string()),
            Some("a\0".to_string()),
            Some("é".to_string()),
            Some("海港".to_string()),
            Some(boundary_prefix.clone()),
            Some(format!("{boundary_prefix}a")),
            Some(format!("{boundary_prefix}b")),
        ];
        for direction in [
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderSortDirection::Descending,
        ] {
            for null_order in [RuntimeOrderNullOrder::First, RuntimeOrderNullOrder::Last] {
                let contract = RuntimeOrderContract::new(
                    [RuntimeOrderKey::with_order(
                        DataType::Utf8,
                        direction,
                        null_order,
                    )],
                    [7; 32],
                );
                for left in &values {
                    for right in &values {
                        let expected = match (left, right) {
                            (None, None) => Ordering::Equal,
                            (None, Some(_)) => {
                                if null_order == RuntimeOrderNullOrder::First {
                                    Ordering::Less
                                } else {
                                    Ordering::Greater
                                }
                            }
                            (Some(_), None) => {
                                if null_order == RuntimeOrderNullOrder::First {
                                    Ordering::Greater
                                } else {
                                    Ordering::Less
                                }
                            }
                            (Some(left), Some(right)) => {
                                let ordering = left.as_bytes().cmp(right.as_bytes());
                                if direction == RuntimeOrderSortDirection::Descending {
                                    ordering.reverse()
                                } else {
                                    ordering
                                }
                            }
                        };
                        let owned_left = OrderedTuple::new([left.clone().map(OrderedScalar::Utf8)]);
                        let owned_right =
                            OrderedTuple::new([right.clone().map(OrderedScalar::Utf8)]);
                        assert_eq!(contract.compare(&owned_left, &owned_right), Ok(expected));
                        assert_eq!(
                            contract.compare_borrowed_controlled(
                                OrderedTupleRef::Single(
                                    left.as_deref().map(OrderedScalarRef::Utf8)
                                ),
                                OrderedTupleRef::Single(
                                    right.as_deref().map(OrderedScalarRef::Utf8)
                                ),
                                &LegacyOrderControl,
                            ),
                            Ok(expected),
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn ordered_topn_owned_borrowed_keeps_original_tuple_type_rules() {
        let cases = [
            (
                DataType::Boolean,
                OrderedScalar::Boolean(false),
                OrderedScalar::Boolean(true),
            ),
            (
                DataType::Int8,
                OrderedScalar::Int8(i8::MIN),
                OrderedScalar::Int8(i8::MAX),
            ),
            (
                DataType::Int16,
                OrderedScalar::Int16(i16::MIN),
                OrderedScalar::Int16(i16::MAX),
            ),
            (
                DataType::Int32,
                OrderedScalar::Int32(i32::MIN),
                OrderedScalar::Int32(i32::MAX),
            ),
            (
                DataType::Int64,
                OrderedScalar::Int64(i64::MIN),
                OrderedScalar::Int64(i64::MAX),
            ),
            (
                DataType::FixedSizeBinary(16),
                OrderedScalar::LargeInt(i128::MIN),
                OrderedScalar::LargeInt(i128::MAX),
            ),
            (
                DataType::Date32,
                OrderedScalar::Date32(-1),
                OrderedScalar::Date32(1),
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                OrderedScalar::Timestamp(-1),
                OrderedScalar::Timestamp(1),
            ),
            (
                DataType::Decimal128(2, 1),
                OrderedScalar::Decimal128(-99),
                OrderedScalar::Decimal128(99),
            ),
        ];
        for (data_type, left, right) in cases {
            let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(data_type)], [9; 32]);
            assert_eq!(
                contract.compare_borrowed_controlled(
                    OrderedTupleRef::Single(Some(left.as_borrowed())),
                    OrderedTupleRef::Single(Some(right.as_borrowed())),
                    &LegacyOrderControl,
                ),
                Ok(Ordering::Less),
            );
            assert_eq!(
                contract.compare(
                    &OrderedTuple::new([Some(left)]),
                    &OrderedTuple::new([Some(right)])
                ),
                Ok(Ordering::Less)
            );
        }
        for (data_type, value) in [
            (DataType::FixedSizeBinary(8), OrderedScalarRef::LargeInt(0)),
            (
                DataType::Decimal128(2, 1),
                OrderedScalarRef::Decimal128(100),
            ),
            (
                DataType::Decimal128(2, 1),
                OrderedScalarRef::Decimal128(-100),
            ),
            (DataType::Utf8, OrderedScalarRef::Int64(0)),
        ] {
            let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(data_type)], [9; 32]);
            assert_eq!(
                contract.compare_borrowed_controlled(
                    OrderedTupleRef::Single(Some(value)),
                    OrderedTupleRef::Single(None),
                    &LegacyOrderControl
                ),
                Err(ControlledOrderedTupleError::Tuple(
                    OrderedTupleError::TypeMismatch
                ))
            );
        }
        let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [9; 32]);
        let empty = OrderedTuple::new([]);
        assert_eq!(
            contract.compare(&empty, &empty),
            Err(OrderedTupleError::ArityMismatch)
        );
    }

    /// Interruption is driven through the real public control port, with no
    /// wall-clock timeout or allocator hook. An equal long prefix must stop.
    #[test]
    fn ordered_topn_owned_borrowed_utf8_checks_control_inside_long_prefix() {
        use novarocks_functions::{
            KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
        };
        use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
        struct Control {
            units: AtomicU32,
            cause: KernelFailure,
        }
        impl KernelEvaluationControl for Control {
            fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
                assert!(units <= MAX_UNOBSERVED_KERNEL_WORK);
                let observed = self.units.fetch_add(units, AtomicOrdering::Relaxed) + units;
                if observed >= 2 * MAX_UNOBSERVED_KERNEL_WORK {
                    Err(self.cause.clone())
                } else {
                    Ok(())
                }
            }
            fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
                panic!("ordered comparison never waits")
            }
        }
        let text = "x".repeat(1 << 20);
        let contract = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Utf8)], [1; 32]);
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let control = Control {
                units: AtomicU32::new(0),
                cause: cause.clone(),
            };
            assert_eq!(
                contract.compare_borrowed_controlled(
                    OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(&text))),
                    OrderedTupleRef::Single(Some(OrderedScalarRef::Utf8(&text))),
                    &control
                ),
                Err(ControlledOrderedTupleError::Kernel(cause))
            );
            let observed = control.units.load(AtomicOrdering::Relaxed);
            assert!(observed >= 2 * MAX_UNOBSERVED_KERNEL_WORK);
            assert!(observed < 4 * MAX_UNOBSERVED_KERNEL_WORK);
        }
    }

    #[test]
    fn arrow_membership_encoder_uses_the_frozen_contract_and_nrfc_frame() {
        let schema = membership_schema_for_test(&DataType::Int32);
        let digest: [u8; 32] = Sha256::digest(&schema).into();
        let producer = RuntimeFilterProducerContract::membership(
            RuntimeFilterBindingId::new(7),
            RuntimeFilterChannelId::new(9),
            RuntimeFilterExecutionContract::Membership(
                RuntimeFilterMembershipSchema::from_canonical(schema, digest).unwrap(),
            ),
        )
        .unwrap();
        let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), None, Some(1), Some(2)]));

        let MembershipContributionEncodingOutcome::Contributions(contributions) =
            encode_membership_contributions(&producer, &input, usize::MAX).unwrap()
        else {
            panic!("the complete canonical contribution fits");
        };
        assert_eq!(contributions.len(), 1);
        assert_eq!(contributions[0].contract_digest(), digest);
        let decoded = decode_contribution(
            contributions[0].canonical_bytes(),
            &digest,
            ContributionCodecExpectation::membership(&DataType::Int32, digest),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            decoded,
            RuntimeFilterContribution::membership(ValueDomainDelta::new(
                MembershipValues::int32([1, 2]),
                true
            ))
        );
    }

    #[test]
    fn arrow_membership_encoder_fails_open_without_emitting_a_partial_domain() {
        let schema = membership_schema_for_test(&DataType::Int64);
        let digest: [u8; 32] = Sha256::digest(&schema).into();
        let producer = RuntimeFilterProducerContract::membership(
            RuntimeFilterBindingId::new(7),
            RuntimeFilterChannelId::new(9),
            RuntimeFilterExecutionContract::Membership(
                RuntimeFilterMembershipSchema::from_canonical(schema, digest).unwrap(),
            ),
        )
        .unwrap();
        let input: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1_i64, 2, 3]));

        assert_eq!(
            encode_membership_contributions(&producer, &input, 1).unwrap(),
            MembershipContributionEncodingOutcome::Unavailable(
                MembershipContributionEncodingUnavailable::ResourceOrSize
            ),
        );
    }

    #[test]
    fn final_domain_array_encoder_uses_the_exact_canonical_domain_budget() {
        let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), None, Some(1), Some(2)]));

        let FinalDomainArrayEncodingOutcome::FinalDomain(final_domain) =
            encode_final_domain_from_array(&DataType::Int32, [4; 32], &input, usize::MAX).unwrap()
        else {
            panic!("the full final-domain payload fits");
        };
        let expected = encode_value_domain(
            &ValueDomainDelta::new(MembershipValues::int32([1, 2]), true),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(final_domain.canonical_bytes().as_ref(), expected.as_slice());
        assert_eq!(final_domain.contract_digest(), [4; 32]);

        assert_eq!(
            encode_final_domain_from_array(&DataType::Int32, [4; 32], &input, 1).unwrap(),
            FinalDomainArrayEncodingOutcome::Unavailable(
                MembershipContributionEncodingUnavailable::ResourceOrSize
            ),
        );
    }

    #[test]
    fn membership_frame_uses_the_v1_nrfc_fixture_bytes() {
        let contribution = RuntimeFilterContribution::membership(ValueDomainDelta::new(
            MembershipValues::int32([2, 1, 2]),
            true,
        ));
        let digest = [0xA5; 32];

        let encoded = encode_contribution(
            &contribution,
            ContributionCodecExpectation::membership(&DataType::Int32, digest),
            usize::MAX,
        )
        .expect("the canonical membership contribution encodes");

        let expected = [
            b'N', b'R', b'F', b'C', 0, 1, 1, 0, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0, 0, 0, 0, 0, 0, 0, 72, 0,
            0, 0, 0, 0, 0, 0, 46, b'n', b'o', b'v', b'a', b'r', b'o', b'c', b'k', b's', b'.', b'r',
            b'u', b'n', b't', b'i', b'm', b'e', b'-', b'f', b'i', b'l', b't', b'e', b'r', b'.',
            b'v', b'a', b'l', b'u', b'e', b'-', b'd', b'o', b'm', b'a', b'i', b'n', b'-', b'd',
            b'e', b'l', b't', b'a', b'.', b'v', b'1', 4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0,
            0, 2, 1,
        ];
        assert_eq!(encoded.payload(), expected);
    }

    #[test]
    fn contribution_round_trips_each_canonical_payload_kind() {
        let membership_type = DataType::Int64;
        let membership = RuntimeFilterContribution::membership(ValueDomainDelta::new(
            MembershipValues::int64([9, 3]),
            false,
        ));
        let membership_expectation =
            ContributionCodecExpectation::membership(&membership_type, [1; 32]);
        assert_round_trip(membership, membership_expectation, [1; 32]);

        let order = RuntimeOrderContract::new([RuntimeOrderKey::new(DataType::Int32)], [2; 32]);
        let ordered = RuntimeFilterContribution::ordered_bound(OrderedBoundUpdate::new(
            [2; 32],
            OrderedTuple::new([Some(OrderedScalar::Int32(7))]),
        ));
        assert_round_trip(
            ordered,
            ContributionCodecExpectation::OrderedBound(&order),
            [2; 32],
        );

        let topk_contract = RuntimeTopKSummaryContract::new(order.clone(), 2, [3; 32]);
        let topk = RuntimeFilterContribution::top_k_summary(TopKSummary::new(
            [3; 32],
            [OrderedTuple::new([Some(OrderedScalar::Int32(7))])],
        ));
        assert_round_trip(
            topk,
            ContributionCodecExpectation::TopKSummary(&topk_contract),
            [3; 32],
        );

        let final_domain = RuntimeFilterContribution::final_domain(FinalDomainShard::new(
            [4; 32],
            ValueDomainDelta::new(MembershipValues::int64([5]), true),
        ));
        assert_round_trip(
            final_domain,
            ContributionCodecExpectation::final_domain(&membership_type, [4; 32]),
            [4; 32],
        );
    }

    #[test]
    fn decoder_rejects_a_trailing_or_noncanonical_membership_frame() {
        let data_type = DataType::Int32;
        let contribution = RuntimeFilterContribution::membership(ValueDomainDelta::new(
            MembershipValues::int32([1, 2]),
            false,
        ));
        let expectation = ContributionCodecExpectation::membership(&data_type, [7; 32]);
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();

        let mut trailing = encoded.payload().to_vec();
        trailing.push(0);
        assert_eq!(
            decode_contribution(&trailing, &[7; 32], expectation, usize::MAX),
            Err(ContributionCodecError::TrailingBytes),
        );

        let mut duplicate = encoded.payload().to_vec();
        let body_len_offset = 40;
        let body_len = u64::from_be_bytes(duplicate[body_len_offset..48].try_into().unwrap());
        let first_value_offset = 48 + 8 + FINGERPRINT_VERSION_TAG.len() + 1 + 8;
        duplicate.splice(first_value_offset + 4..first_value_offset + 4, [0, 0, 0, 2]);
        duplicate[body_len_offset..48].copy_from_slice(&(body_len + 4).to_be_bytes());
        assert_eq!(
            decode_contribution(&duplicate, &[7; 32], expectation, usize::MAX),
            Err(ContributionCodecError::NonCanonicalPayload),
        );
    }

    #[test]
    fn ordered_topk_and_final_domain_use_the_v1_body_fixtures() {
        let order = RuntimeOrderContract::new(
            [RuntimeOrderKey::with_order(
                DataType::Int32,
                RuntimeOrderSortDirection::Descending,
                RuntimeOrderNullOrder::First,
            )],
            [2; 32],
        );
        let tuple = OrderedTuple::new([Some(OrderedScalar::Int32(7))]);
        let ordered = encode_contribution(
            &RuntimeFilterContribution::ordered_bound(
                OrderedBoundUpdate::try_new(&order, tuple.clone()).unwrap(),
            ),
            ContributionCodecExpectation::OrderedBound(&order),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            &ordered.payload()[..8],
            &[b'N', b'R', b'F', b'C', 0, 1, 2, 0]
        );
        assert_eq!(
            &ordered.payload()[48..],
            &[0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 7]
        );

        let topk_contract = RuntimeTopKSummaryContract::new(order.clone(), 2, [3; 32]);
        let topk = encode_contribution(
            &RuntimeFilterContribution::top_k_summary(
                TopKSummary::try_new(&topk_contract, [tuple]).unwrap(),
            ),
            ContributionCodecExpectation::TopKSummary(&topk_contract),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(&topk.payload()[..8], &[b'N', b'R', b'F', b'C', 0, 1, 3, 0]);
        assert_eq!(
            &topk.payload()[48..],
            &[
                0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 7
            ]
        );

        let membership_type = DataType::Int64;
        let final_domain = encode_contribution(
            &RuntimeFilterContribution::final_domain(FinalDomainShard::new(
                [9; 32],
                ValueDomainDelta::new(MembershipValues::int64([11, 13]), false),
            )),
            ContributionCodecExpectation::final_domain(&membership_type, [4; 32]),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            &final_domain.payload()[..8],
            &[b'N', b'R', b'F', b'C', 0, 1, 4, 0]
        );
        assert_eq!(&final_domain.payload()[48..80], &[9; 32]);
        assert_eq!(
            &final_domain.payload()[80..],
            &[
                0, 0, 0, 0, 0, 0, 0, 46, b'n', b'o', b'v', b'a', b'r', b'o', b'c', b'k', b's',
                b'.', b'r', b'u', b'n', b't', b'i', b'm', b'e', b'-', b'f', b'i', b'l', b't', b'e',
                b'r', b'.', b'v', b'a', b'l', b'u', b'e', b'-', b'd', b'o', b'm', b'a', b'i', b'n',
                b'-', b'd', b'e', b'l', b't', b'a', b'.', b'v', b'1', 5, 0, 0, 0, 0, 0, 0, 0, 2, 0,
                0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 13, 0,
            ][..]
        );
    }

    fn assert_round_trip(
        contribution: RuntimeFilterContribution,
        expectation: ContributionCodecExpectation<'_>,
        digest: [u8; 32],
    ) {
        let encoded = encode_contribution(&contribution, expectation, usize::MAX).unwrap();
        assert_eq!(
            decode_contribution(encoded.payload(), &digest, expectation, usize::MAX).unwrap(),
            contribution
        );
    }

    fn membership_schema_for_test(data_type: &DataType) -> Vec<u8> {
        let mut schema = b"novarocks.runtime-filter.artifact-schema".to_vec();
        schema.push(1);
        schema.push(match data_type {
            DataType::Boolean => 1,
            DataType::Int8 => 2,
            DataType::Int16 => 3,
            DataType::Int32 => 4,
            DataType::Int64 => 5,
            DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => 6,
            DataType::Float32 => 7,
            DataType::Float64 => 8,
            DataType::Utf8 => 9,
            DataType::Date32 => 10,
            _ => panic!("test schema only needs primitive membership types"),
        });
        schema.push(2);
        schema
    }

    // Independent v1 bytes: count2; Int64/ASC/NULLS_LAST; timestamp-us UTC/DESC/NULLS_FIRST.
    fn original_streaming_order_fixture() -> (Vec<RuntimeOrderKey>, [u8; 32], [u8; 32]) {
        let keys = vec![
            RuntimeOrderKey::with_value_type(
                novarocks_functions::FunctionValueType::new(DataType::Int64, false),
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderNullOrder::Last,
            ),
            RuntimeOrderKey::with_value_type(
                novarocks_functions::FunctionValueType::new(
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                RuntimeOrderSortDirection::Descending,
                RuntimeOrderNullOrder::First,
            ),
        ];
        let comparator = [
            0xdf, 0xe9, 0x6e, 0xae, 0xfe, 0x17, 0x10, 0xfe, 0xde, 0x48, 0x96, 0xe5, 0x52, 0xae,
            0x59, 0xd6, 0x4a, 0x34, 0x01, 0x34, 0x24, 0x8a, 0x64, 0x87, 0x46, 0xa1, 0x74, 0xc9,
            0x26, 0x6e, 0x77, 0x34,
        ];
        let order = [
            0x17, 0xb6, 0x73, 0x06, 0x05, 0x19, 0xf1, 0x38, 0xc8, 0x7d, 0xd8, 0xe7, 0x73, 0xff,
            0x3a, 0x3d, 0x66, 0x51, 0x8e, 0xa5, 0xea, 0x0f, 0x2b, 0xbb, 0x1c, 0x8c, 0x33, 0x62,
            0x06, 0x2b, 0x60, 0x6a,
        ];
        (keys, comparator, order)
    }

    #[test]
    fn streaming_fragment_order_preserves_original_bytes_full_keys_and_owned_backing() {
        let (keys, comparator, order) = original_streaming_order_fixture();
        let pure = RuntimeOrderContract::from_fragment_contract(keys.clone(), comparator, order)
            .expect("original strict entry and byte format");
        let original_backing = keys.as_ptr();
        let mut work = Vec::new();
        RuntimeOrderContract::validate_fragment_keys_observed(&keys, comparator, order, |units| {
            work.push(units);
            Ok::<_, ()>(())
        })
        .expect("observed canonical validation");
        let observed = RuntimeOrderContract::from_frozen_keys(keys, comparator, order);
        assert_eq!(observed, pure);
        assert_eq!(observed.keys().as_ptr(), original_backing);
        assert_eq!(observed.keys().len(), 2);
        assert!(!observed.keys()[0].value_type().unwrap().nullable());
        assert!(observed.keys()[1].value_type().unwrap().nullable());
        assert_eq!(
            observed.keys()[1].direction(),
            RuntimeOrderSortDirection::Descending
        );
        assert_eq!(
            observed.keys()[1].null_order(),
            RuntimeOrderNullOrder::First
        );
        assert_eq!(work.iter().filter(|units| **units == 0).count(), 2);
    }

    #[test]
    fn streaming_fragment_order_keeps_comparator_order_and_unsupported_first_causes() {
        let (keys, comparator, order) = original_streaming_order_fixture();
        let mut wrong_comparator = comparator;
        wrong_comparator[0] ^= 1;
        let mut wrong_order = order;
        wrong_order[0] ^= 1;
        let first = RuntimeOrderContract::validate_fragment_keys_observed(
            &keys,
            wrong_comparator,
            wrong_order,
            |_| Ok::<_, ()>(()),
        )
        .expect_err("comparator is checked before order");
        let RuntimeOrderContractValidationFailure::Contract(first) = first else {
            panic!("original contract failure");
        };
        assert_eq!(first.detail(), "ordered comparator digest mismatch");
        let second = RuntimeOrderContract::validate_fragment_keys_observed(
            &keys,
            comparator,
            wrong_order,
            |_| Ok::<_, ()>(()),
        )
        .expect_err("wrong order");
        let RuntimeOrderContractValidationFailure::Contract(second) = second else {
            panic!("original contract failure");
        };
        assert_eq!(second.detail(), "ordered contract digest mismatch");
        let empty = RuntimeOrderContract::from_fragment_contract([], comparator, order)
            .expect_err("original empty contract refusal");
        assert_eq!(empty.detail(), "ordered contract has no keys");
        let unsupported = RuntimeOrderContract::from_fragment_contract(
            [RuntimeOrderKey::new(DataType::Float64)],
            comparator,
            order,
        )
        .expect_err("membership vocabulary does not expand ordered support");
        assert_eq!(
            unsupported.detail(),
            "ordered contract has unsupported key type"
        );
    }

    #[test]
    fn streaming_fragment_order_returns_each_original_work_failure_without_tail() {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        struct Original;
        let (keys, comparator, order) = original_streaming_order_fixture();
        let mut successful = Vec::new();
        RuntimeOrderContract::validate_fragment_keys_observed(&keys, comparator, order, |units| {
            successful.push(units);
            Ok::<_, Original>(())
        })
        .expect("actual successful observations");
        for refused in 0..successful.len() {
            let mut prefix = Vec::new();
            let result = RuntimeOrderContract::validate_fragment_keys_observed(
                &keys,
                comparator,
                order,
                |units| {
                    prefix.push(units);
                    if prefix.len() == refused + 1 {
                        Err(Original)
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(matches!(
                result,
                Err(RuntimeOrderContractValidationFailure::Work(Original))
            ));
            assert_eq!(prefix, successful[..=refused]);
        }
        let boundary = successful.iter().position(|units| *units == 0).unwrap();
        let mut wrong_comparator = comparator;
        wrong_comparator[0] ^= 1;
        let mut prefix = Vec::new();
        let result = RuntimeOrderContract::validate_fragment_keys_observed(
            &keys,
            wrong_comparator,
            order,
            |units| {
                prefix.push(units);
                if units == 0 { Err(Original) } else { Ok(()) }
            },
        );
        assert!(matches!(
            result,
            Err(RuntimeOrderContractValidationFailure::Work(Original))
        ));
        assert_eq!(prefix, successful[..=boundary]);
    }
}

fn type_description(value: &DataType) -> novarocks_failure_contract::BoundedDetail {
    novarocks_failure_contract::BoundedDetail::truncating_display(format_args!("{value:?}"))
}
impl novarocks_failure_contract::FailureProjection for ContributionCodecError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, reason) = match self {
            Self::LengthOverflow => {
                return FailureFact::new(
                    FailureClass::Representation,
                    ExecutionReason::FilterContributionLength.reason(),
                )
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: std::num::NonZeroU16::new(14).unwrap(),
                    },
                    required: None,
                    extent_limit: None,
                    representable_prefix: None,
                });
            }
            Self::EncodedSizeExceeded | Self::ResourceLimit | Self::ArithmeticOverflow => (
                FailureClass::Resource,
                ExecutionReason::FilterContributionResource,
            ),
            _ => (
                FailureClass::Invariant,
                ExecutionReason::FilterContributionCodec,
            ),
        };
        FailureFact::new(class, reason.reason())
    }
}

#[cfg(test)]
mod failure_source_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::sync::Arc;
    #[test]
    fn actual_membership_type_refusal_does_not_pin_original_arrow_fields() {
        let field = Arc::new(arrow::datatypes::Field::new("value", DataType::Utf8, true));
        let weak = Arc::downgrade(&field);
        let expected = DataType::Struct(vec![field.clone()].into());
        let actual = Arc::new(arrow::array::Int32Array::from(vec![7])) as ArrayRef;
        let error = derive_value_domain_from_array(&expected, &actual, 4096).unwrap_err();
        drop(expected);
        drop(field);
        assert!(
            weak.upgrade().is_none(),
            "the failure keeps diagnostics, not unaccompanied metadata backing"
        );
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            error.failure_fact().reason(),
            crate::runtime::task::ExecutionReason::FilterMembershipType.reason()
        );
        assert!(error.to_string().contains("expected Struct"));
    }
}
