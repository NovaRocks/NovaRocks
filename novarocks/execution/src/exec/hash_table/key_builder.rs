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
//! The shared canonical component framing and typed decoder.
//!
//! Local equality indexing belongs to `key_index`. These canonical helpers
//! retain their original distributed FNV/CRC and complex-value contracts;
//! their bytes and hashes are not an instance-local key-index identity.

#[cfg(test)]
use arrow::array::Int32Array;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
    Decimal256Array, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray, ListArray, MapArray,
    StringArray, StringViewArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
#[cfg(test)]
use arrow_buffer::OffsetBuffer;
#[cfg(test)]
use novarocks_failure_contract::FailureProjection;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use crate::exec::hash_table::allocation::AggregateAllocator;
use crate::exec::hash_table::allocation::AggregateVec;
use novarocks_types::largeint;

use super::hash::{canonical_f32_bits, canonical_f64_bits};

use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
#[derive(Clone, Debug, Eq, PartialEq)]
struct GroupKeyEncodingContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for GroupKeyEncodingContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for GroupKeyEncodingContract {}
impl novarocks_failure_contract::FailureProjection for GroupKeyEncodingContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::GroupKeyEncodingContract.reason(),
        )
    }
}
fn group_key_encoding_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(GroupKeyEncodingContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
#[derive(Debug)]
struct GroupKeyComponentExtent {
    component: &'static str,
    required: usize,
    source: std::num::TryFromIntError,
}
impl std::fmt::Display for GroupKeyComponentExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "canonical group-key {} length {} exceeds UInt32",
            self.component, self.required
        )
    }
}
impl std::error::Error for GroupKeyComponentExtent {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for GroupKeyComponentExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        FailureFact::new(
            FailureClass::Representation,
            crate::runtime::fragment::ExecutionReason::GroupKeyComponentExtent.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::MinimumUnit,
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: std::num::NonZeroU16::new(17).unwrap(),
            },
            required: u64::try_from(self.required).ok(),
            extent_limit: Some(u64::from(u32::MAX)),
            representable_prefix: None,
        })
    }
}
pub(super) fn group_key_component_len(
    required: usize,
    component: &'static str,
) -> ExecutionResult<u32> {
    u32::try_from(required).map_err(|source| {
        ExecutionFailure::from_domain(GroupKeyComponentExtent {
            component,
            required,
            source,
        })
    })
}
pub(super) fn group_key_request_arithmetic() -> ExecutionFailure {
    ExecutionFailure::from_domain(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
}

#[derive(Debug)]
enum GroupKeyDecodeFailure {
    Contract(novarocks_failure_contract::BoundedDetail),
    Utf8(std::str::Utf8Error),
    Arrow(arrow::error::ArrowError),
}
impl std::fmt::Display for GroupKeyDecodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::Utf8(source) => write!(f, "invalid canonical utf8 key: {source}"),
            Self::Arrow(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for GroupKeyDecodeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(_) => None,
            Self::Utf8(source) => Some(source),
            Self::Arrow(source) => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for GroupKeyDecodeFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use arrow::error::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, reason) = match self {
            Self::Contract(_) => (
                FailureClass::Invariant,
                ExecutionReason::GroupKeyDecodeContract,
            ),
            Self::Utf8(_) => (FailureClass::Invariant, ExecutionReason::GroupKeyDecodeUtf8),
            Self::Arrow(ArrowError::MemoryError(_) | ArrowError::ArithmeticOverflow(_)) => (
                FailureClass::Resource,
                ExecutionReason::GroupKeyDecodeAllocation,
            ),
            Self::Arrow(ArrowError::OffsetOverflowError(_)) => (
                FailureClass::Representation,
                ExecutionReason::GroupKeyDecodeOffset,
            ),
            Self::Arrow(ArrowError::IoError(..) | ArrowError::ExternalError(_)) => (
                FailureClass::Unexpected,
                ExecutionReason::GroupKeyDecodeUnexpected,
            ),
            Self::Arrow(_) => (
                FailureClass::Invariant,
                ExecutionReason::GroupKeyDecodeArrow,
            ),
        };
        let fact = FailureFact::new(class, reason.reason());
        if matches!(self, Self::Arrow(ArrowError::OffsetOverflowError(_))) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: std::num::NonZeroU16::new(22).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
pub(super) fn group_key_decoding_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(GroupKeyDecodeFailure::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn group_key_decode_arrow(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(GroupKeyDecodeFailure::Arrow(source))
}
pub(super) fn group_key_decode_utf8(source: std::str::Utf8Error) -> ExecutionFailure {
    ExecutionFailure::from_domain(GroupKeyDecodeFailure::Utf8(source))
}
#[derive(Clone, Copy, Debug)]
pub(super) enum GroupKeyOutputKind {
    Utf8,
    Binary,
    List,
    Map,
}
impl GroupKeyOutputKind {
    fn layout(self) -> u16 {
        match self {
            Self::Utf8 => 18,
            Self::Binary => 19,
            Self::List => 20,
            Self::Map => 21,
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct GroupKeyOutputExtent {
    kind: GroupKeyOutputKind,
    total: u128,
    largest: u128,
}
impl std::fmt::Display for GroupKeyOutputExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "canonical group-key {:?} output extent {} (largest value {}) exceeds Int32 offsets",
            self.kind, self.total, self.largest
        )
    }
}
impl std::error::Error for GroupKeyOutputExtent {}
impl novarocks_failure_contract::FailureProjection for GroupKeyOutputExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (boundary, required) = if self.largest > i32::MAX as u128 {
            (RepresentationBoundary::MinimumUnit, self.largest)
        } else {
            (RepresentationBoundary::SingleOutput, self.total)
        };
        FailureFact::new(
            FailureClass::Representation,
            crate::runtime::fragment::ExecutionReason::GroupKeyOutputExtent.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary,
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: std::num::NonZeroU16::new(self.kind.layout()).unwrap(),
            },
            required: u64::try_from(required).ok(),
            extent_limit: Some(i32::MAX as u64),
            representable_prefix: None,
        })
    }
}
#[derive(Default)]
pub(super) struct GroupKeyOutputTally {
    total: u128,
    largest: u128,
}
impl GroupKeyOutputTally {
    pub(super) fn add(&mut self, length: usize) -> ExecutionResult<()> {
        self.total = self
            .total
            .checked_add(length as u128)
            .ok_or_else(group_key_request_arithmetic)?;
        self.largest = self.largest.max(length as u128);
        Ok(())
    }
    pub(super) fn finish(self, kind: GroupKeyOutputKind) -> ExecutionResult<()> {
        if self.total > i32::MAX as u128 {
            Err(ExecutionFailure::from_domain(GroupKeyOutputExtent {
                kind,
                total: self.total,
                largest: self.largest,
            }))
        } else {
            Ok(())
        }
    }
}
#[cfg(test)]
pub fn encode_group_key_row(array: &ArrayRef, row: usize) -> ExecutionResult<Option<Vec<u8>>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let mut out = Vec::new();
    encode_group_key_non_null_value(array, row, &mut out)?;
    Ok(Some(out))
}

#[cfg(test)]
pub(crate) fn encode_group_key_row_tracked(
    array: &ArrayRef,
    row: usize,
    allocator: AggregateAllocator,
) -> ExecutionResult<Option<AggregateVec<u8>>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let encoded_len = encoded_group_key_non_null_len(array, row)?;
    let mut out = AggregateVec::new_in(allocator.clone());
    out.try_reserve_exact(encoded_len).map_err(|source| {
        allocator.allocation_error("reserve canonical group-key bytes", source)
    })?;
    encode_group_key_non_null_value(array, row, &mut out)?;
    debug_assert_eq!(out.len(), encoded_len);
    Ok(Some(out))
}

/// Append one precisely framed component using the existing grouping author.
/// The complete tuple's exact column types are frozen separately by its owner.
pub(crate) fn append_canonical_nullable_tracked(
    array: &ArrayRef,
    row: usize,
    out: &mut AggregateVec<u8>,
) -> ExecutionResult<()> {
    let count = if array.is_null(row) {
        1
    } else {
        5usize
            .checked_add(encoded_group_key_non_null_len(array, row)?)
            .ok_or_else(|| group_key_encoding_contract("canonical tuple extent overflow"))?
    };
    out.try_reserve_exact(count).map_err(|source| {
        out.allocator()
            .allocation_error("reserve canonical tuple component", source)
    })?;
    encode_nullable_nested(array, row, out, "tuple component")
}

trait KeyByteSink {
    fn push_byte(&mut self, value: u8) -> ExecutionResult<()>;
    fn extend_bytes(&mut self, values: &[u8]) -> ExecutionResult<()>;
}

impl KeyByteSink for Vec<u8> {
    fn push_byte(&mut self, value: u8) -> ExecutionResult<()> {
        self.push(value);
        Ok(())
    }

    fn extend_bytes(&mut self, values: &[u8]) -> ExecutionResult<()> {
        self.extend_from_slice(values);
        Ok(())
    }
}

impl KeyByteSink for AggregateVec<u8> {
    fn push_byte(&mut self, value: u8) -> ExecutionResult<()> {
        if self.len() == self.capacity() {
            return Err(group_key_encoding_contract(
                "canonical group-key byte reservation was too small",
            ));
        }
        self.push(value);
        Ok(())
    }

    fn extend_bytes(&mut self, values: &[u8]) -> ExecutionResult<()> {
        if values.len() > self.capacity().saturating_sub(self.len()) {
            return Err(group_key_encoding_contract(
                "canonical group-key byte reservation was too small",
            ));
        }
        self.extend_from_slice(values);
        Ok(())
    }
}

struct ComparingKeyByteSink<'a> {
    expected: &'a [u8],
    offset: usize,
    equal: bool,
}

impl KeyByteSink for ComparingKeyByteSink<'_> {
    fn push_byte(&mut self, value: u8) -> ExecutionResult<()> {
        self.extend_bytes(&[value])
    }

    fn extend_bytes(&mut self, values: &[u8]) -> ExecutionResult<()> {
        let end = self.offset.saturating_add(values.len());
        if self.equal && self.expected.get(self.offset..end) != Some(values) {
            self.equal = false;
        }
        self.offset = end;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn canonical_group_key_equals(
    array: &ArrayRef,
    row: usize,
    expected: &[u8],
) -> ExecutionResult<bool> {
    if array.is_null(row) {
        return Ok(false);
    }
    let mut sink = ComparingKeyByteSink {
        expected,
        offset: 0,
        equal: true,
    };
    encode_group_key_non_null_value(array, row, &mut sink)?;
    Ok(sink.equal && sink.offset == expected.len())
}

/// Compare one normalized complex component using the original nullable
/// framing and cooperative canonical walker. Ordinary STRING/BINARY keys use
/// value-arrays batch equality instead of this nested-value author.
pub(crate) fn canonical_nullable_equals_observed(
    array: &ArrayRef,
    row: usize,
    expected: &[u8],
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<bool> {
    checkpoint()?;
    let mut sink = ComparingKeyByteSink {
        expected,
        offset: 0,
        equal: true,
    };
    let mut work = ObservedCanonicalKeyWork::new(checkpoint);
    encode_nullable_nested_with(array, row, &mut sink, "tuple component", &mut work)?;
    checkpoint()?;
    Ok(sink.equal && sink.offset == expected.len())
}

struct HashingKeyByteSink {
    hash: u64,
}

impl HashingKeyByteSink {
    fn new(seed: u64) -> Self {
        Self {
            hash: seed ^ 0xcbf29ce484222325,
        }
    }

    fn finish(self) -> u64 {
        self.hash
    }
}

impl KeyByteSink for HashingKeyByteSink {
    fn push_byte(&mut self, value: u8) -> ExecutionResult<()> {
        self.hash ^= value as u64;
        self.hash = self.hash.wrapping_mul(0x100000001b3);
        Ok(())
    }

    fn extend_bytes(&mut self, values: &[u8]) -> ExecutionResult<()> {
        for value in values {
            self.push_byte(*value)?;
        }
        Ok(())
    }
}

fn canonical_group_key_hash_with_seed(
    array: &ArrayRef,
    row: usize,
    seed: u64,
) -> ExecutionResult<Option<u64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let mut sink = HashingKeyByteSink::new(seed);
    encode_group_key_non_null_value(array, row, &mut sink)?;
    Ok(Some(sink.finish()))
}

pub(crate) fn canonical_group_key_fnv_hash(
    array: &ArrayRef,
    row: usize,
) -> ExecutionResult<Option<u64>> {
    canonical_group_key_hash_with_seed(array, row, 0)
}

struct Crc32KeyByteSink {
    crc: u32,
}

impl Crc32KeyByteSink {
    fn new() -> Self {
        Self { crc: 0xffff_ffff }
    }

    fn finish(self) -> u32 {
        self.crc ^ 0xffff_ffff
    }
}

impl KeyByteSink for Crc32KeyByteSink {
    fn push_byte(&mut self, value: u8) -> ExecutionResult<()> {
        self.crc ^= value as u32;
        for _ in 0..8 {
            self.crc = if self.crc & 1 != 0 {
                (self.crc >> 1) ^ 0xedb8_8320
            } else {
                self.crc >> 1
            };
        }
        Ok(())
    }

    fn extend_bytes(&mut self, values: &[u8]) -> ExecutionResult<()> {
        for value in values {
            self.push_byte(*value)?;
        }
        Ok(())
    }
}

pub(crate) fn canonical_group_key_crc32_hash(
    array: &ArrayRef,
    row: usize,
) -> ExecutionResult<Option<u32>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let mut sink = Crc32KeyByteSink::new();
    encode_group_key_non_null_value(array, row, &mut sink)?;
    Ok(Some(sink.finish()))
}

/// Legacy callers retain the same byte author without adding a runtime
/// control dependency. The observed entry points instantiate this same walk.
trait CanonicalKeyWork {
    fn step(&mut self) -> ExecutionResult<()>;
    fn push<S: KeyByteSink>(&mut self, out: &mut S, byte: u8) -> ExecutionResult<()> {
        out.push_byte(byte)
    }
    fn extend<S: KeyByteSink>(&mut self, out: &mut S, bytes: &[u8]) -> ExecutionResult<()>;
}
struct UnobservedCanonicalKeyWork;
impl CanonicalKeyWork for UnobservedCanonicalKeyWork {
    fn step(&mut self) -> ExecutionResult<()> {
        Ok(())
    }
    fn extend<S: KeyByteSink>(&mut self, out: &mut S, bytes: &[u8]) -> ExecutionResult<()> {
        out.extend_bytes(bytes)
    }
}
struct ObservedCanonicalKeyWork<'a> {
    checkpoint: &'a dyn Fn() -> ExecutionResult<()>,
    pending_nodes: usize,
    pending_bytes: usize,
}
impl<'a> ObservedCanonicalKeyWork<'a> {
    fn new(checkpoint: &'a dyn Fn() -> ExecutionResult<()>) -> Self {
        Self {
            checkpoint,
            pending_nodes: 0,
            pending_bytes: 0,
        }
    }
}
impl CanonicalKeyWork for ObservedCanonicalKeyWork<'_> {
    fn push<S: KeyByteSink>(&mut self, out: &mut S, byte: u8) -> ExecutionResult<()> {
        self.extend(out, &[byte])
    }
    fn step(&mut self) -> ExecutionResult<()> {
        self.pending_nodes += 1;
        if self.pending_nodes == 256 {
            self.pending_nodes = 0;
            self.pending_bytes = 0;
            (self.checkpoint)()?;
        }
        Ok(())
    }
    fn extend<S: KeyByteSink>(&mut self, out: &mut S, mut bytes: &[u8]) -> ExecutionResult<()> {
        while !bytes.is_empty() {
            let length = (64 * 1024 - self.pending_bytes).min(bytes.len());
            out.extend_bytes(&bytes[..length])?;
            self.pending_bytes += length;
            bytes = &bytes[length..];
            if self.pending_bytes == 64 * 1024 {
                (self.checkpoint)()?;
                self.pending_bytes = 0;
                self.pending_nodes = 0;
            }
        }
        Ok(())
    }
}

fn encode_group_key_non_null_value<S: KeyByteSink>(
    array: &ArrayRef,
    row: usize,
    out: &mut S,
) -> ExecutionResult<()> {
    encode_group_key_non_null_value_with(array, row, out, &mut UnobservedCanonicalKeyWork)
}
fn encode_nullable_nested<S: KeyByteSink>(
    array: &ArrayRef,
    row: usize,
    out: &mut S,
    context: &'static str,
) -> ExecutionResult<()> {
    encode_nullable_nested_with(array, row, out, context, &mut UnobservedCanonicalKeyWork)
}
fn encoded_group_key_non_null_len(array: &ArrayRef, row: usize) -> ExecutionResult<usize> {
    encoded_group_key_non_null_len_with(array, row, &mut UnobservedCanonicalKeyWork)
}

/// Cooperative control on the original FNV canonical writer, including the
/// lengths computed recursively while encoding nested components.
pub(crate) fn canonical_group_key_fnv_hash_observed(
    array: &ArrayRef,
    row: usize,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Option<u64>> {
    checkpoint()?;
    if array.is_null(row) {
        return Ok(None);
    }
    let mut sink = HashingKeyByteSink::new(0);
    let mut work = ObservedCanonicalKeyWork::new(checkpoint);
    encode_group_key_non_null_value_with(array, row, &mut sink, &mut work)?;
    checkpoint()?;
    Ok(Some(sink.finish()))
}

/// Exact old tuple bytes, with bounded sizing and writing work. Bounded fixed
/// scalar leaves keep the original fast path; byte/nested values use the same
/// canonical walker with actual caller control, never a replacement codec.
pub(crate) fn append_canonical_nullable_tracked_observed(
    array: &ArrayRef,
    row: usize,
    out: &mut AggregateVec<u8>,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    if matches!(
        array.data_type(),
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Timestamp(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::FixedSizeBinary(16)
    ) {
        append_canonical_nullable_tracked(array, row, out)?;
        return checkpoint();
    }
    let mut work = ObservedCanonicalKeyWork::new(checkpoint);
    let count = if array.is_null(row) {
        1
    } else {
        5usize
            .checked_add(encoded_group_key_non_null_len_with(array, row, &mut work)?)
            .ok_or_else(|| group_key_encoding_contract("canonical tuple extent overflow"))?
    };
    checkpoint()?;
    out.try_reserve_exact(count).map_err(|source| {
        out.allocator()
            .allocation_error("reserve canonical tuple component", source)
    })?;
    checkpoint()?;
    encode_nullable_nested_with(array, row, out, "tuple component", &mut work)?;
    checkpoint()
}

/// Borrow the original byte leaf value and its existing canonical frame.
/// Physical UTF8 offsets/view layouts share marker8; BinaryView shares the
/// original Binary marker20. LargeBinary deliberately keeps marker21.
/// Dictionary encoding is removed by the original typed preparation owner.
fn canonical_byte_value(
    array: &ArrayRef,
    row: usize,
) -> ExecutionResult<(&[u8], u8, &'static str)> {
    macro_rules! value {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "canonical byte value does not match its declared carrier",
                    )
                })?
                .value(row)
        };
    }
    Ok(match array.data_type() {
        DataType::Utf8 => (value!(StringArray).as_bytes(), 8, "Utf8"),
        DataType::LargeUtf8 => (value!(LargeStringArray).as_bytes(), 8, "LargeUtf8"),
        DataType::Utf8View => (value!(StringViewArray).as_bytes(), 8, "Utf8View"),
        DataType::Binary => (value!(BinaryArray), 20, "Binary"),
        DataType::BinaryView => (value!(BinaryViewArray), 20, "BinaryView"),
        DataType::LargeBinary => (value!(LargeBinaryArray), 21, "LargeBinary"),
        _ => {
            return Err(group_key_encoding_contract(
                "canonical byte value requires a byte carrier",
            ));
        }
    })
}

fn encode_group_key_non_null_value_with<S: KeyByteSink, W: CanonicalKeyWork>(
    array: &ArrayRef,
    row: usize,
    out: &mut S,
    work: &mut W,
) -> ExecutionResult<()> {
    work.step()?;
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast BooleanArray while encoding group key",
                    )
                })?;
            work.push(out, 1)?;
            work.push(out, arr.value(row) as u8)?;
            Ok(())
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast Int8Array while encoding group key",
                    )
                })?;
            work.push(out, 2)?;
            work.extend(out, &arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast Int16Array while encoding group key",
                    )
                })?;
            work.push(out, 3)?;
            work.extend(out, &arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast Int32Array while encoding group key",
                    )
                })?;
            work.push(out, 4)?;
            work.extend(out, &arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast Int64Array while encoding group key",
                    )
                })?;
            work.push(out, 5)?;
            work.extend(out, &arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast Float32Array while encoding group key",
                    )
                })?;
            work.push(out, 6)?;
            work.extend(out, &canonical_f32_bits(arr.value(row)).to_le_bytes())?;
            Ok(())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast Float64Array while encoding group key",
                    )
                })?;
            work.push(out, 7)?;
            work.extend(out, &canonical_f64_bits(arr.value(row)).to_le_bytes())?;
            Ok(())
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::BinaryView
        | DataType::LargeBinary => {
            let (value, marker, component) = canonical_byte_value(array, row)?;
            let len = group_key_component_len(value.len(), component)?;
            work.push(out, marker)?;
            work.extend(out, &len.to_le_bytes())?;
            work.extend(out, value)?;
            Ok(())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast Date32Array while encoding group key",
                    )
                })?;
            work.push(out, 9)?;
            work.extend(out, &arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Timestamp(unit, _) => {
            let marker = match unit {
                TimeUnit::Second => 10_u8,
                TimeUnit::Millisecond => 11_u8,
                TimeUnit::Microsecond => 12_u8,
                TimeUnit::Nanosecond => 13_u8,
            };
            let value = match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| {
                            group_key_encoding_contract(
                                "failed to downcast TimestampSecondArray while encoding group key",
                            )
                        })?;
                    arr.value(row)
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            group_key_encoding_contract("failed to downcast TimestampMillisecondArray while encoding group key")
                        })?;
                    arr.value(row)
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            group_key_encoding_contract("failed to downcast TimestampMicrosecondArray while encoding group key")
                        })?;
                    arr.value(row)
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            group_key_encoding_contract("failed to downcast TimestampNanosecondArray while encoding group key")
                        })?;
                    arr.value(row)
                }
            };
            work.push(out, marker)?;
            work.extend(out, &value.to_le_bytes())?;
            Ok(())
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast Decimal128Array while encoding group key",
                    )
                })?;
            work.push(out, 14)?;
            work.extend(out, &arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast Decimal256Array while encoding group key",
                    )
                })?;
            work.push(out, 19)?;
            work.extend(out, &arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast FixedSizeBinaryArray while encoding group key",
                    )
                })?;
            if arr.value_length() != 16 {
                return Err(group_key_encoding_contract(
                    "canonical LARGEINT key has a foreign physical width",
                ));
            }
            let fixed: [u8; 16] = arr
                .value(row)
                .try_into()
                .expect("checked canonical LARGEINT physical width");
            let value = i128::from_be_bytes(fixed);
            work.push(out, 15)?;
            work.extend(out, &value.to_le_bytes())?;
            Ok(())
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                group_key_encoding_contract("failed to downcast ListArray while encoding group key")
            })?;
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = list.values();
            work.push(out, 16)?;
            let len = group_key_component_len(end.saturating_sub(start), "List")?;
            work.extend(out, &len.to_le_bytes())?;
            for idx in start..end {
                encode_nullable_nested_with(values, idx, out, "group key list item", work)?;
            }
            Ok(())
        }
        DataType::Struct(_) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast StructArray while encoding group key",
                    )
                })?;
            work.push(out, 17)?;
            let field_count = group_key_component_len(struct_arr.num_columns(), "Struct")?;
            work.extend(out, &field_count.to_le_bytes())?;
            for column in struct_arr.columns() {
                encode_nullable_nested_with(column, row, out, "group key struct field", work)?;
            }
            Ok(())
        }
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                group_key_encoding_contract("failed to downcast MapArray while encoding group key")
            })?;
            let offsets = map.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let entries = map.entries();
            let keys = entries.column(0).clone();
            let values = entries.column(1).clone();
            work.push(out, 18)?;
            let len = group_key_component_len(end.saturating_sub(start), "Map")?;
            work.extend(out, &len.to_le_bytes())?;
            for idx in start..end {
                encode_nullable_nested_with(&keys, idx, out, "group key map key", work)?;
                encode_nullable_nested_with(&values, idx, out, "group key map value", work)?;
            }
            Ok(())
        }
        other => Err(group_key_encoding_contract(format_args!(
            "group key encode unsupported input type: {:?}",
            other
        ))),
    }
}

fn encode_nullable_nested_with<S: KeyByteSink, W: CanonicalKeyWork>(
    array: &ArrayRef,
    row: usize,
    out: &mut S,
    context: &'static str,
    work: &mut W,
) -> ExecutionResult<()> {
    work.step()?;
    if matches!(array.data_type(), DataType::Null) || array.is_null(row) {
        work.push(out, 0)?;
        return Ok(());
    }
    work.push(out, 1)?;
    let len = encoded_group_key_non_null_len_with(array, row, work)?;
    let len = group_key_component_len(len, context)?;
    work.extend(out, &len.to_le_bytes())?;
    encode_group_key_non_null_value_with(array, row, out, work)
}

fn encoded_group_key_non_null_len_with<W: CanonicalKeyWork>(
    array: &ArrayRef,
    row: usize,
    work: &mut W,
) -> ExecutionResult<usize> {
    work.step()?;
    let fixed = match array.data_type() {
        DataType::Boolean | DataType::Int8 => Some(2),
        DataType::Int16 => Some(3),
        DataType::Int32 | DataType::Float32 | DataType::Date32 => Some(5),
        DataType::Int64 | DataType::Float64 | DataType::Timestamp(_, _) => Some(9),
        DataType::Decimal128(_, _) => Some(17),
        DataType::Decimal256(_, _) => Some(33),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => Some(17),
        _ => None,
    };
    if let Some(fixed) = fixed {
        return Ok(fixed);
    }
    match array.data_type() {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::BinaryView
        | DataType::LargeBinary => {
            let (value, _, component) = canonical_byte_value(array, row)?;
            group_key_component_len(value.len(), component)?;
            checked_encoded_len_add(5, value.len())
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                group_key_encoding_contract("failed to downcast ListArray while sizing group key")
            })?;
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = list.values();
            nested_sequence_encoded_len_with(values, start..end, 5, work)
        }
        DataType::Struct(_) => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    group_key_encoding_contract(
                        "failed to downcast StructArray while sizing group key",
                    )
                })?;
            values.columns().iter().try_fold(5usize, |total, column| {
                nested_value_encoded_len_with(column, row, work)
                    .and_then(|len| checked_encoded_len_add(total, len))
            })
        }
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                group_key_encoding_contract("failed to downcast MapArray while sizing group key")
            })?;
            let offsets = map.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let entries = map.entries();
            let keys = entries.column(0);
            let values = entries.column(1);
            (start..end).try_fold(5usize, |total, index| {
                let key_len = nested_value_encoded_len_with(keys, index, work)?;
                let value_len = nested_value_encoded_len_with(values, index, work)?;
                checked_encoded_len_add(total, key_len)
                    .and_then(|total| checked_encoded_len_add(total, value_len))
            })
        }
        other => Err(group_key_encoding_contract(format_args!(
            "group key encode unsupported input type: {:?}",
            other
        ))),
    }
}

fn nested_sequence_encoded_len_with<W: CanonicalKeyWork>(
    values: &ArrayRef,
    mut rows: std::ops::Range<usize>,
    initial: usize,
    work: &mut W,
) -> ExecutionResult<usize> {
    rows.try_fold(initial, |total, row| {
        nested_value_encoded_len_with(values, row, work)
            .and_then(|len| checked_encoded_len_add(total, len))
    })
}

fn nested_value_encoded_len_with<W: CanonicalKeyWork>(
    array: &ArrayRef,
    row: usize,
    work: &mut W,
) -> ExecutionResult<usize> {
    work.step()?;
    if matches!(array.data_type(), DataType::Null) || array.is_null(row) {
        Ok(1)
    } else {
        encoded_group_key_non_null_len_with(array, row, work)?
            .checked_add(5)
            .ok_or_else(group_key_request_arithmetic)
    }
}

fn checked_encoded_len_add(left: usize, right: usize) -> ExecutionResult<usize> {
    left.checked_add(right)
        .ok_or_else(group_key_request_arithmetic)
}

mod decode;
#[cfg(test)]
pub(crate) use decode::decode_group_key_rows;
pub(crate) use decode::decode_group_key_rows_construction;

struct EncodedCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

/// Borrow the original nullable component frame without inventing a second
/// tuple parser in a key store. The payload remains the canonical author's
/// non-NULL group-key encoding, not an Arrow value or a physical value layout.
pub(crate) fn canonical_component_payload(bytes: &[u8]) -> ExecutionResult<Option<&[u8]>> {
    let mut cursor = EncodedCursor::new(bytes);
    let payload = cursor.read_nested()?;
    cursor.finish()?;
    Ok(payload)
}

impl<'a> EncodedCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_marker(&mut self, expected: u8) -> ExecutionResult<()> {
        let actual = self.read_u8()?;
        if actual != expected {
            return Err(group_key_decoding_contract(format_args!(
                "canonical group-key marker mismatch: expected {expected}, got {actual}"
            )));
        }
        Ok(())
    }

    fn read_u8(&mut self) -> ExecutionResult<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| group_key_decoding_contract("truncated canonical group key"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> ExecutionResult<u32> {
        let bytes = self.read_slice(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("u32 bytes")))
    }

    fn read_slice(&mut self, len: usize) -> ExecutionResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| group_key_decoding_contract("canonical group-key cursor overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| group_key_decoding_contract("truncated canonical group key"))?;
        self.offset = end;
        Ok(value)
    }

    fn read_nested(&mut self) -> ExecutionResult<Option<&'a [u8]>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let len = self.read_u32()? as usize;
                self.read_slice(len).map(Some)
            }
            marker => Err(group_key_decoding_contract(format_args!(
                "invalid canonical nested marker: {marker}"
            ))),
        }
    }

    fn finish(&self) -> ExecutionResult<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(group_key_decoding_contract(
                "trailing bytes in canonical group key",
            ))
        }
    }
}

#[cfg(test)]
#[path = "key_builder/nested_byte_tests.rs"]
mod nested_byte_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;
    use arrow::array::{
        ArrayRef, Int32Builder, Int64Array, MapBuilder, MapFieldNames, NullArray, StringBuilder,
    };
    use arrow::datatypes::{Field, Fields};
    use arrow_buffer::NullBuffer;
    use std::sync::Arc;

    #[test]
    fn encode_group_key_row_accepts_binary_arrays() {
        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(b"abc".as_slice()),
            None,
            Some(b"abc\0".as_slice()),
        ]));
        assert!(encode_group_key_row(&binary, 0).unwrap().is_some());
        assert_eq!(encode_group_key_row(&binary, 1).unwrap(), None);
        assert!(encode_group_key_row(&binary, 2).unwrap().is_some());

        let large_binary: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(b"abc".as_slice()),
            None,
            Some(b"def".as_slice()),
        ]));
        assert!(encode_group_key_row(&large_binary, 0).unwrap().is_some());
        assert_eq!(encode_group_key_row(&large_binary, 1).unwrap(), None);
        assert!(encode_group_key_row(&large_binary, 2).unwrap().is_some());
    }

    fn assert_canonical_round_trip(array: ArrayRef) {
        let tracker = MemTracker::new_root("canonical-key-test");
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        let encoded = (0..array.len())
            .map(|row| encode_group_key_row_tracked(&array, row, allocator.clone()))
            .collect::<Result<Vec<_>, _>>()
            .expect("encode canonical keys");
        let borrowed = encoded
            .iter()
            .map(|value| value.as_ref().map(|value| value.as_slice()))
            .collect::<Vec<_>>();
        let decoded = decode_group_key_rows(array.data_type(), &borrowed).expect("decode keys");
        assert_eq!(decoded.as_ref(), array.as_ref());
        for (row, encoded) in borrowed.iter().copied().enumerate() {
            if let Some(encoded) = encoded {
                assert!(
                    canonical_group_key_equals(&array, row, encoded)
                        .expect("compare canonical key")
                );
            }
        }
        drop(encoded);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn canonical_complex_keys_round_trip_list_struct_and_map() {
        let null_list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            OffsetBuffer::new(vec![0, 0, 1, 1].into()),
            Arc::new(NullArray::new(1)),
            Some(NullBuffer::from(vec![true, true, false])),
        )) as ArrayRef;
        assert_canonical_round_trip(null_list);

        let list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(vec![0, 2, 4, 4].into()),
            Arc::new(Int64Array::from(vec![Some(1), None, Some(1), None])),
            Some(NullBuffer::from(vec![true, true, false])),
        )) as ArrayRef;
        assert_canonical_round_trip(list);

        let fields = Fields::from(vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(Field::new("name", DataType::Utf8, true)),
        ]);
        let struct_array = Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![Some(7), Some(7), None])),
                Arc::new(StringArray::from(vec![Some("x"), Some("x"), None])),
            ],
            Some(NullBuffer::from(vec![true, true, false])),
        )) as ArrayRef;
        assert_canonical_round_trip(struct_array);

        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            Int32Builder::new(),
            StringBuilder::new(),
        );
        for row in [Some(&[(1, "a")][..]), Some(&[(1, "a")][..]), None] {
            if let Some(entries) = row {
                for (key, value) in entries {
                    builder.keys().append_value(*key);
                    builder.values().append_value(*value);
                }
                builder.append(true).expect("append map row");
            } else {
                builder.append(false).expect("append null map row");
            }
        }
        assert_canonical_round_trip(Arc::new(builder.finish()));
    }

    #[test]
    fn canonical_complex_key_rejects_before_allocating_bytes() {
        let fields = Fields::from(vec![Arc::new(Field::new("payload", DataType::Utf8, false))]);
        let array = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(StringArray::from(vec![
                "a payload too large for one byte",
            ]))],
            None,
        )) as ArrayRef;
        let tracker = MemTracker::new_root("canonical-key-limit");
        tracker.install_limit_once(1).expect("install limit");

        let error =
            encode_group_key_row_tracked(&array, 0, AggregateAllocator::new(Arc::clone(&tracker)))
                .expect_err("canonical bytes must be rejected before allocation");

        assert_eq!(
            error.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Resource
        );
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
mod encoding_extent_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, RepresentationBoundary};
    #[test]
    fn one_key_component_extent_is_distinct_from_a_row_batch_prefix() {
        assert_eq!(group_key_component_len(0, "empty").unwrap(), 0);
        assert_eq!(
            group_key_component_len(u32::MAX as usize, "largest").unwrap(),
            u32::MAX
        );
        if usize::BITS > 32 {
            let required = (u64::from(u32::MAX) + 1) as usize;
            let failure = group_key_component_len(required, "ResourceExhausted numeric overflow")
                .unwrap_err();
            let fact = failure.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            assert_eq!(fact.reason().code(), 366);
            let extent = fact.representation().unwrap();
            assert_eq!(extent.boundary, RepresentationBoundary::MinimumUnit);
            assert_eq!(extent.required, Some(u64::from(u32::MAX) + 1));
            assert_eq!(extent.extent_limit, Some(u64::from(u32::MAX)));
            let crate::runtime::fragment::ExecutionFailureCause::Domain(cause) = failure.cause()
            else {
                panic!("expected original encoding source")
            };
            let cause = cause.downcast_ref::<GroupKeyComponentExtent>().unwrap();
            assert!(
                std::error::Error::source(cause)
                    .unwrap()
                    .is::<std::num::TryFromIntError>()
            );
        }
    }
}

#[cfg(test)]
mod decoding_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, RepresentationBoundary};
    #[test]
    fn malformed_utf8_keeps_its_error_without_retaining_the_input_owner() {
        let bytes = Arc::new(vec![8, 1, 0, 0, 0, 0xff]);
        let weak = Arc::downgrade(&bytes);
        let failure =
            decode_group_key_rows(&DataType::Utf8, &[Some(bytes.as_slice())]).unwrap_err();
        drop(bytes);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 374);
        let crate::runtime::fragment::ExecutionFailureCause::Domain(cause) = failure.cause() else {
            panic!("expected canonical decoder source")
        };
        let source = cause.downcast_ref::<GroupKeyDecodeFailure>().unwrap();
        assert_eq!(
            std::error::Error::source(source)
                .unwrap()
                .downcast_ref::<std::str::Utf8Error>()
                .unwrap()
                .valid_up_to(),
            0
        );
    }
    #[test]
    fn malformed_nonnullable_canonical_children_return_arrow_causes() {
        use arrow::datatypes::Field;
        let item = Arc::new(Field::new("item", DataType::Int64, false));
        let fields: arrow::datatypes::Fields = vec![item.clone()].into();
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int64, false),
                    Field::new("value", DataType::Int64, true),
                ]
                .into(),
            ),
            false,
        ));
        for (ty, bytes) in [
            (DataType::List(item), vec![16, 1, 0, 0, 0, 0]),
            (DataType::Struct(fields), vec![17, 1, 0, 0, 0, 0]),
            (DataType::Map(entries, false), vec![18, 1, 0, 0, 0, 0, 0]),
        ] {
            let failure = decode_group_key_rows(&ty, &[Some(bytes.as_slice())]).unwrap_err();
            assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
            assert_eq!(failure.failure_fact().reason().code(), 373);
            let crate::runtime::fragment::ExecutionFailureCause::Domain(cause) = failure.cause()
            else {
                panic!("expected exact Arrow construction source")
            };
            let source = cause.downcast_ref::<GroupKeyDecodeFailure>().unwrap();
            assert!(
                std::error::Error::source(source)
                    .unwrap()
                    .is::<arrow::error::ArrowError>()
            );
        }
    }
    #[test]
    fn output_offset_extent_distinguishes_one_value_from_an_output_batch() {
        if usize::BITS <= 32 {
            return;
        }
        let limit = i32::MAX as usize;
        for (lengths, boundary, required) in [
            (
                vec![limit + 1],
                RepresentationBoundary::MinimumUnit,
                (limit + 1) as u64,
            ),
            (
                vec![limit / 2 + 1, limit / 2 + 1],
                RepresentationBoundary::SingleOutput,
                (limit + 1) as u64,
            ),
        ] {
            for kind in [
                GroupKeyOutputKind::Utf8,
                GroupKeyOutputKind::Binary,
                GroupKeyOutputKind::List,
                GroupKeyOutputKind::Map,
            ] {
                let mut tally = GroupKeyOutputTally::default();
                for length in &lengths {
                    tally.add(*length).unwrap();
                }
                let failure = tally.finish(kind).unwrap_err();
                let fact = failure.failure_fact();
                assert_eq!(fact.class(), FailureClass::Representation);
                assert_eq!(fact.reason().code(), 375);
                let extent = fact.representation().unwrap();
                assert_eq!(extent.boundary, boundary);
                assert_eq!(extent.required, Some(required));
                assert_eq!(extent.extent_limit, Some(i32::MAX as u64));
            }
        }
    }
}

#[cfg(test)]
mod cooperative_canonical_work_tests {
    use super::*;
    use arrow::array::{Int64Builder, ListBuilder};
    use std::cell::Cell;

    fn one_large_nested_row() -> ArrayRef {
        let mut outer = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
        for value in 0..4096 {
            outer.values().values().append_value(value);
        }
        outer.values().append(true);
        outer.append(true);
        let array = Arc::new(outer.finish()) as ArrayRef;
        assert_eq!(array.len(), 1);
        assert!(matches!(array.data_type(), DataType::List(item)
            if matches!(item.data_type(), DataType::List(child) if child.data_type() == &DataType::Int64)));
        array
    }
    fn original_stop() -> ExecutionFailure {
        novarocks_functions::KernelFailure::Operational(novarocks_functions::KernelDiagnostic::new(
            "canonical nested original stop",
        ))
        .into()
    }

    #[test]
    fn one_nested_row_observes_original_stop_during_recursive_sizing() {
        let array = one_large_nested_row();
        let calls = Cell::new(0usize);
        let original = original_stop();
        let checkpoint = || {
            calls.set(calls.get() + 1);
            if calls.get() == 2 {
                Err(original.clone())
            } else {
                Ok(())
            }
        };
        let mut work = ObservedCanonicalKeyWork::new(&checkpoint);
        let failure = encoded_group_key_non_null_len_with(&array, 0, &mut work).unwrap_err();
        assert_eq!(failure, original);
        assert_eq!(
            calls.get(),
            2,
            "the stop is observed before completing one row"
        );
    }

    #[test]
    fn one_nested_row_hash_observes_stop_inside_its_encoder_length_walk() {
        let array = one_large_nested_row();
        let calls = Cell::new(0usize);
        let original = original_stop();
        let failure = canonical_group_key_fnv_hash_observed(&array, 0, &|| {
            calls.set(calls.get() + 1);
            if calls.get() == 3 {
                Err(original.clone())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(failure, original);
        assert_eq!(calls.get(), 3);
    }

    struct MeasuringSink<'a> {
        output: Vec<u8>,
        since_checkpoint: &'a Cell<usize>,
        largest_write: &'a Cell<usize>,
    }
    impl KeyByteSink for MeasuringSink<'_> {
        fn push_byte(&mut self, value: u8) -> ExecutionResult<()> {
            self.extend_bytes(&[value])
        }
        fn extend_bytes(&mut self, value: &[u8]) -> ExecutionResult<()> {
            self.largest_write
                .set(self.largest_write.get().max(value.len()));
            self.since_checkpoint
                .set(self.since_checkpoint.get() + value.len());
            self.output.extend_from_slice(value);
            Ok(())
        }
    }

    #[test]
    fn encoding_has_an_observable_stop_after_output_started_before_nested_child_finishes() {
        let array = one_large_nested_row();
        let written = Cell::new(0usize);
        let largest = Cell::new(0usize);
        let original = original_stop();
        let mut sink = MeasuringSink {
            output: Vec::new(),
            since_checkpoint: &written,
            largest_write: &largest,
        };
        let checkpoint = || {
            if written.get() > 0 {
                Err(original.clone())
            } else {
                Ok(())
            }
        };
        let mut work = ObservedCanonicalKeyWork::new(&checkpoint);
        let failure =
            encode_group_key_non_null_value_with(&array, 0, &mut sink, &mut work).unwrap_err();
        assert_eq!(failure, original);
        assert!(!sink.output.is_empty());
        assert!(
            sink.output.len() < 4096,
            "control interrupts nested sizing within the encoder"
        );
    }

    #[test]
    fn observed_author_preserves_nested_bytes_and_hash_and_chunks_wide_bytes() {
        for array in [
            one_large_nested_row(),
            Arc::new(StringArray::from(vec!["x".repeat(200_000)])) as ArrayRef,
        ] {
            let expected = encode_group_key_row(&array, 0).unwrap().unwrap();
            let since = Cell::new(0usize);
            let largest = Cell::new(0usize);
            let mut sink = MeasuringSink {
                output: Vec::new(),
                since_checkpoint: &since,
                largest_write: &largest,
            };
            let checkpoint = || {
                assert!(
                    since.get() <= 64 * 1024,
                    "actual canonical bytes between checkpoints"
                );
                since.set(0);
                Ok(())
            };
            let mut work = ObservedCanonicalKeyWork::new(&checkpoint);
            encode_group_key_non_null_value_with(&array, 0, &mut sink, &mut work).unwrap();
            checkpoint().unwrap();
            assert_eq!(sink.output, expected);
            assert!(largest.get() <= 64 * 1024);
            assert_eq!(
                canonical_group_key_fnv_hash_observed(&array, 0, &|| Ok(())).unwrap(),
                canonical_group_key_fnv_hash(&array, 0).unwrap()
            );
        }
    }
}
