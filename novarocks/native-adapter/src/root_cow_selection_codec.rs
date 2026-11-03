// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! COW records under a preparation-installed recipe, independent of transport.
//!
//! No schema is sent or learned from a record. Each nonempty input emits one
//! complete CSA2 BATCH record: a 64-byte fixed header (the prior batch geometry
//! plus the exact recipe digest), followed by node lengths, buffer lengths and
//! the bounded source ranges. Segments and replay retain their original owner.
//! The closed source set is checked before dictionary hydration; the installed
//! recipe chooses a plain representation without admitting additional types.
//! Every emission step has the original byte/work bounds. Caller admission
//! precedes cursor, hydrated payload and decoding construction.

// Design: ADR-0214 (docs/adr/ADR-0214-schema-free-online-units-and-prepared-layout-recipes.md)
use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, RecordBatch, RecordBatchOptions, make_array,
};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use novarocks_result_contract::RootProfileV1;
use novarocks_result_render::{RenderTurn, RenderTurnStatus};
use novarocks_spi::connector::{
    ConnectorPayloadRetentionGuard, ConnectorRowMutationSourceArray,
    ConnectorRowMutationSourceBatch, ConnectorRowMutationSourceBuilder,
    MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES, MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES,
};

pub const COW_SELECTION_HEADER_BYTES: usize = 64;
pub const COW_SELECTION_TURN_BYTES: usize = 64 * 1024;
pub const COW_SELECTION_TURN_WORK: usize = 1024;
const MAGIC: [u8; 4] = *b"CSA2";
const KIND_BATCH: u8 = 2;

mod tag {
    pub const NULL: u8 = 0;
    pub const BOOLEAN: u8 = 1;
    pub const INT8: u8 = 2;
    pub const INT16: u8 = 3;
    pub const INT32: u8 = 4;
    pub const INT64: u8 = 5;
    pub const UINT8: u8 = 6;
    pub const UINT16: u8 = 7;
    pub const UINT32: u8 = 8;
    pub const UINT64: u8 = 9;
    pub const FLOAT32: u8 = 10;
    pub const FLOAT64: u8 = 11;
    pub const DECIMAL128: u8 = 12;
    pub const DECIMAL256: u8 = 13;
    pub const DATE32: u8 = 14;
    pub const DATE64: u8 = 15;
    pub const TIME32: u8 = 16;
    pub const TIME64: u8 = 17;
    pub const TIMESTAMP: u8 = 18;
    pub const FIXED_SIZE_BINARY: u8 = 19;
    pub const UTF8: u8 = 20;
    pub const LARGE_UTF8: u8 = 21;
    pub const BINARY: u8 = 22;
    pub const LARGE_BINARY: u8 = 23;
    pub const LIST: u8 = 24;
    pub const LARGE_LIST: u8 = 25;
    pub const STRUCT: u8 = 26;
    pub const DICTIONARY: u8 = 27;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CowSelectionCodecError {
    UnsupportedType,
    SchemaLimit,
    SchemaChanged,
    BatchLimit,
    RecordLimit,
    ScratchLimit,
    TruncatedHeader,
    HeaderVersion,
    RecordLength,
    TruncatedRecord,
    TrailingRecordBytes,
    RecipeMismatch,
    MalformedBatch,
    SourceLimit,
}
impl std::fmt::Display for CowSelectionCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UnsupportedType => "COW selection column type is outside the codec's closed set",
            Self::SchemaLimit => "COW selection schema exceeds its bounded profile",
            Self::SchemaChanged => "COW selection input differs from its frozen recipe",
            Self::BatchLimit => "COW selection exceeds its frozen batch count",
            Self::RecordLimit => "COW selection record size cannot be represented",
            Self::ScratchLimit => "COW selection cursor exceeds its prepaid scratch capacity",
            Self::TruncatedHeader => "COW selection record header is truncated",
            Self::HeaderVersion => "unsupported COW selection record header",
            Self::RecordLength => "COW selection record length disagrees with its declarations",
            Self::TruncatedRecord => "COW selection record is truncated",
            Self::TrailingRecordBytes => "COW selection record has trailing bytes",
            Self::RecipeMismatch => "COW selection record differs from its frozen recipe",
            Self::MalformedBatch => "malformed COW selection batch record",
            Self::SourceLimit => "COW selection source exceeds its bounded ownership profile",
        })
    }
}
impl std::error::Error for CowSelectionCodecError {}

/// Keep an original construction cause under the COW recipe boundary. This
/// wrapper never turns a foreign diagnostic into semantic type authority.
#[derive(Debug)]
pub struct CowRecipeConstructionFailure<E>(pub E);
impl<E: std::fmt::Display> std::fmt::Display for CowRecipeConstructionFailure<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl<E: std::error::Error + 'static> std::error::Error for CowRecipeConstructionFailure<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl<E> novarocks_execution_contract::failure::FailureProjection
    for CowRecipeConstructionFailure<E>
{
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        novarocks_execution_contract::failure::FailureProjection::failure_fact(
            &CowSelectionCodecError::RecipeMismatch,
        )
    }
}

/// Owned construction distinguishes immutable wire grammar from the actual
/// allocation/budget author. Pure codec errors remain the original Copy kind.
#[derive(Debug)]
pub enum CowOwnedDecodeError {
    Codec(CowSelectionCodecError),
    Source(novarocks_spi::connector::ConnectorError),
}
impl From<CowSelectionCodecError> for CowOwnedDecodeError {
    fn from(error: CowSelectionCodecError) -> Self {
        Self::Codec(error)
    }
}
impl From<novarocks_spi::connector::ConnectorError> for CowOwnedDecodeError {
    fn from(error: novarocks_spi::connector::ConnectorError) -> Self {
        Self::Source(error)
    }
}
impl std::fmt::Display for CowOwnedDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Codec(error) => error.fmt(f),
            Self::Source(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for CowOwnedDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Source(error) => Some(error),
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for CowOwnedDecodeError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        match self {
            Self::Codec(error) => error.failure_fact(),
            Self::Source(error) => error.failure_fact(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CowSelectionRecordKind {
    Batch,
}

/// One validated record declaration, read from its fixed prefix only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CowSelectionRecordHeader {
    kind: CowSelectionRecordKind,
    record_bytes: u64,
    rows: u64,
    nodes: u32,
    buffers: u32,
    recipe_digest: [u8; 32],
}
impl CowSelectionRecordHeader {
    pub fn parse(prefix: &[u8]) -> Result<Self, CowSelectionCodecError> {
        if prefix.len() < COW_SELECTION_HEADER_BYTES {
            return Err(CowSelectionCodecError::TruncatedHeader);
        }
        if prefix[..4] != MAGIC || prefix[5..8] != [0; 3] {
            return Err(CowSelectionCodecError::HeaderVersion);
        }
        let u64_at = |at: usize| u64::from_le_bytes(prefix[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(prefix[at..at + 4].try_into().unwrap());
        let header = Self {
            kind: match prefix[4] {
                KIND_BATCH => CowSelectionRecordKind::Batch,
                _ => return Err(CowSelectionCodecError::HeaderVersion),
            },
            record_bytes: u64_at(8),
            rows: u64_at(16),
            nodes: u32_at(24),
            buffers: u32_at(28),
            recipe_digest: prefix[32..64].try_into().unwrap(),
        };
        let nodes = header.nodes as usize;
        let shape_ok = match header.kind {
            CowSelectionRecordKind::Batch => {
                header.rows != 0 && (header.buffers as usize) <= 3 * nodes
            }
        };
        let minimum = (COW_SELECTION_HEADER_BYTES as u64)
            + 8 * (u64::from(header.buffers)
                + u64::from(header.nodes)
                    * u64::from(header.kind == CowSelectionRecordKind::Batch));
        if !shape_ok || nodes == 0 || header.record_bytes < minimum {
            return Err(CowSelectionCodecError::RecordLength);
        }
        Ok(header)
    }
    pub const fn kind(self) -> CowSelectionRecordKind {
        self.kind
    }
    pub const fn record_bytes(self) -> u64 {
        self.record_bytes
    }
    pub const fn rows(self) -> u64 {
        self.rows
    }
    pub fn validate_record_bytes(self, available: usize) -> Result<(), CowSelectionCodecError> {
        match (available as u64).cmp(&self.record_bytes) {
            std::cmp::Ordering::Less => Err(CowSelectionCodecError::TruncatedRecord),
            std::cmp::Ordering::Greater => Err(CowSelectionCodecError::TrailingRecordBytes),
            std::cmp::Ordering::Equal => Ok(()),
        }
    }
    fn bytes(self) -> [u8; COW_SELECTION_HEADER_BYTES] {
        let mut bytes = [0; COW_SELECTION_HEADER_BYTES];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4] = match self.kind {
            CowSelectionRecordKind::Batch => KIND_BATCH,
        };
        bytes[8..16].copy_from_slice(&self.record_bytes.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.rows.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.nodes.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.buffers.to_le_bytes());
        bytes[32..64].copy_from_slice(&self.recipe_digest);
        bytes
    }
}

/// An immutable preparation result. The complete logical declarations stay
/// separate from Arrow representation; root slot/name projections never become
/// a new source of semantic identity.
#[derive(Debug)]
pub struct CowSelectionRecipe {
    batch: novarocks_arrow_batch_codec::BatchRecipe,
    _metadata: Option<ConnectorPayloadRetentionGuard>,
}
impl CowSelectionRecipe {
    /// Both inputs were constructed under the caller's real author. This pure
    /// binding performs no allocation and grants no capacity.
    pub fn try_new(
        batch: novarocks_arrow_batch_codec::BatchRecipe,
        declarations: Vec<novarocks_type_contract::FunctionValueType>,
    ) -> Result<Self, CowSelectionCodecError> {
        Self::validate_supported(batch.schema())?;
        if batch.declarations() != declarations.as_slice()
            || batch
                .schema()
                .fields()
                .iter()
                .any(|field| !field.is_nullable())
        {
            return Err(CowSelectionCodecError::SchemaChanged);
        }
        Ok(Self {
            batch,
            _metadata: None,
        })
    }
    pub fn with_metadata_retention(mut self, retention: ConnectorPayloadRetentionGuard) -> Self {
        self._metadata = Some(retention);
        self
    }
    pub fn schema(&self) -> &SchemaRef {
        self.batch.schema()
    }
    pub fn digest(&self) -> &[u8; 32] {
        self.batch.digest()
    }
    pub fn declarations(&self) -> &[novarocks_type_contract::FunctionValueType] {
        self.batch.declarations()
    }
    /// Preserve the original source support set before an author expands any
    /// dictionary. Hydration itself supports a larger set and cannot admit it.
    pub fn validate_supported(
        schema: &arrow::datatypes::Schema,
    ) -> Result<(), CowSelectionCodecError> {
        let mut plan = Plan::default();
        for field in schema.fields() {
            plan.field(field.name(), field.data_type(), 1)?;
        }
        if plan.nodes == 0 {
            return Err(CowSelectionCodecError::SchemaLimit);
        }
        Ok(())
    }
    fn validate_digest(
        &self,
        header: CowSelectionRecordHeader,
    ) -> Result<(), CowSelectionCodecError> {
        if &header.recipe_digest == self.digest() {
            Ok(())
        } else {
            Err(CowSelectionCodecError::RecipeMismatch)
        }
    }
}

/// Completed records are progress only, never selection or exit evidence.
#[derive(Clone, Debug, Default)]
pub struct CowSelectionTotals {
    batches: usize,
    rows: u64,
}
impl CowSelectionTotals {
    pub const fn batches(&self) -> usize {
        self.batches
    }
    pub const fn rows(&self) -> u64 {
        self.rows
    }
}

/// Fixed-width carrier facts of one supported type, or its tag shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Shape {
    Null,
    Boolean,
    Fixed(usize),
    FixedBinary(usize),
    Bytes32,
    Bytes64,
    List32,
    List64,
    Struct,
    Dictionary,
}
fn shape(data_type: &DataType) -> Result<(u8, Shape), CowSelectionCodecError> {
    use DataType as D;
    use Shape as S;
    Ok(match data_type {
        D::Null => (tag::NULL, S::Null),
        D::Boolean => (tag::BOOLEAN, S::Boolean),
        D::Int8 => (tag::INT8, S::Fixed(1)),
        D::Int16 => (tag::INT16, S::Fixed(2)),
        D::Int32 => (tag::INT32, S::Fixed(4)),
        D::Int64 => (tag::INT64, S::Fixed(8)),
        D::UInt8 => (tag::UINT8, S::Fixed(1)),
        D::UInt16 => (tag::UINT16, S::Fixed(2)),
        D::UInt32 => (tag::UINT32, S::Fixed(4)),
        D::UInt64 => (tag::UINT64, S::Fixed(8)),
        D::Float32 => (tag::FLOAT32, S::Fixed(4)),
        D::Float64 => (tag::FLOAT64, S::Fixed(8)),
        D::Decimal128(_, _) => (tag::DECIMAL128, S::Fixed(16)),
        D::Decimal256(_, _) => (tag::DECIMAL256, S::Fixed(32)),
        D::Date32 => (tag::DATE32, S::Fixed(4)),
        D::Date64 => (tag::DATE64, S::Fixed(8)),
        D::Time32(TimeUnit::Second | TimeUnit::Millisecond) => (tag::TIME32, S::Fixed(4)),
        D::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => (tag::TIME64, S::Fixed(8)),
        D::Timestamp(_, _) => (tag::TIMESTAMP, S::Fixed(8)),
        D::FixedSizeBinary(width) if *width >= 0 => {
            (tag::FIXED_SIZE_BINARY, S::FixedBinary(*width as usize))
        }
        D::Utf8 => (tag::UTF8, S::Bytes32),
        D::LargeUtf8 => (tag::LARGE_UTF8, S::Bytes64),
        D::Binary => (tag::BINARY, S::Bytes32),
        D::LargeBinary => (tag::LARGE_BINARY, S::Bytes64),
        D::List(_) => (tag::LIST, S::List32),
        D::LargeList(_) => (tag::LARGE_LIST, S::List64),
        D::Struct(_) => (tag::STRUCT, S::Struct),
        D::Dictionary(key, value)
            if key.as_ref() == &D::Int32 && matches!(value.as_ref(), D::Utf8 | D::LargeUtf8) =>
        {
            (tag::DICTIONARY, S::Dictionary)
        }
        _ => return Err(CowSelectionCodecError::UnsupportedType),
    })
}
const fn buffer_count(shape: Shape) -> usize {
    match shape {
        Shape::Null | Shape::Struct => 1,
        Shape::Bytes32 | Shape::Bytes64 => 3,
        _ => 2,
    }
}

/// Counts and schema payload bytes of one field tree, without allocating.
#[derive(Default)]
struct Plan {
    nodes: usize,
    buffers: usize,
}
impl Plan {
    fn field(
        &mut self,
        name: &str,
        data_type: &DataType,
        depth: usize,
    ) -> Result<(), CowSelectionCodecError> {
        if depth > RootProfileV1::MAX_DEPTH {
            return Err(CowSelectionCodecError::SchemaLimit);
        }
        let (_, shape) = shape(data_type)?;
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or(CowSelectionCodecError::RecordLimit)?;
        self.buffers = self
            .buffers
            .checked_add(buffer_count(shape))
            .ok_or(CowSelectionCodecError::RecordLimit)?;
        if name.len() > RootProfileV1::MAX_NAME_BYTES {
            return Err(CowSelectionCodecError::SchemaLimit);
        }
        match data_type {
            DataType::List(item) | DataType::LargeList(item) => {
                self.field(item.name(), item.data_type(), depth + 1)
            }
            DataType::Struct(fields) => fields
                .iter()
                .try_for_each(|child| self.field(child.name(), child.data_type(), depth + 1)),
            // The closed string-dictionary wrapper is physical encoding, not
            // another SQL value level. Both nodes still consume directory work.
            DataType::Dictionary(_, value) => self.field("", value, depth),
            _ => Ok(()),
        }
    }
}
/// One type node of one batch: its array and the logical row range it covers.
struct Node {
    array: ArrayRef,
    start: usize,
    len: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    Validity,
    BooleanValues,
    Fixed(usize),
    Offsets32,
    Offsets64,
    Data32,
    Data64,
    DictionaryKeys,
}
#[derive(Clone, Copy)]
struct Spec {
    node: usize,
    source: Source,
    bytes: u64,
}

fn primitive_bytes(array: &dyn Array) -> &[u8] {
    macro_rules! values {
        ($ty:ty) => {
            array.as_primitive::<$ty>().values().inner().as_slice()
        };
    }
    match array.data_type() {
        DataType::Int8 => values!(Int8Type),
        DataType::Int16 => values!(Int16Type),
        DataType::Int32 => values!(Int32Type),
        DataType::Int64 => values!(Int64Type),
        DataType::UInt8 => values!(UInt8Type),
        DataType::UInt16 => values!(UInt16Type),
        DataType::UInt32 => values!(UInt32Type),
        DataType::UInt64 => values!(UInt64Type),
        DataType::Float32 => values!(Float32Type),
        DataType::Float64 => values!(Float64Type),
        DataType::Decimal128(_, _) => values!(Decimal128Type),
        DataType::Decimal256(_, _) => values!(Decimal256Type),
        DataType::Date32 => values!(Date32Type),
        DataType::Date64 => values!(Date64Type),
        DataType::Time32(TimeUnit::Second) => values!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => values!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => values!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => values!(Time64NanosecondType),
        DataType::Timestamp(TimeUnit::Second, _) => values!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => values!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => values!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => values!(TimestampNanosecondType),
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().value_data(),
        DataType::Dictionary(_, _) => array
            .as_dictionary::<Int32Type>()
            .keys()
            .values()
            .inner()
            .as_slice(),
        _ => unreachable!("fixed carriers are classified before emission"),
    }
}
fn offsets32(array: &dyn Array) -> &[i32] {
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value_offsets(),
        DataType::Binary => array.as_binary::<i32>().value_offsets(),
        DataType::List(_) => array.as_list::<i32>().value_offsets(),
        _ => unreachable!("32-bit offsets are classified before emission"),
    }
}
fn offsets64(array: &dyn Array) -> &[i64] {
    match array.data_type() {
        DataType::LargeUtf8 => array.as_string::<i64>().value_offsets(),
        DataType::LargeBinary => array.as_binary::<i64>().value_offsets(),
        DataType::LargeList(_) => array.as_list::<i64>().value_offsets(),
        _ => unreachable!("64-bit offsets are classified before emission"),
    }
}
fn data_bytes(array: &dyn Array) -> &[u8] {
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().values().as_slice(),
        DataType::Binary => array.as_binary::<i32>().values().as_slice(),
        DataType::LargeUtf8 => array.as_string::<i64>().values().as_slice(),
        DataType::LargeBinary => array.as_binary::<i64>().values().as_slice(),
        _ => unreachable!("byte data is classified before emission"),
    }
}
fn null_buffer(array: &dyn Array) -> Option<&NullBuffer> {
    match array.data_type() {
        DataType::Null => None,
        _ => array.nulls(),
    }
}
/// One output byte of a re-packed bitmap that starts at `bit_start`.
fn bitmap_byte(source: &[u8], bit_start: usize, bits: usize, index: usize) -> u8 {
    let first = bit_start + 8 * index;
    let (byte, shift) = (first / 8, first % 8);
    let mut value = source[byte] >> shift;
    if shift != 0 && byte + 1 < source.len() {
        value |= source[byte + 1] << (8 - shift);
    }
    let remaining = bits - 8 * index;
    if remaining < 8 {
        value &= (1_u8 << remaining) - 1;
    }
    value
}

fn build_nodes(
    nodes: &mut Vec<Node>,
    specs: &mut Vec<Spec>,
    array: &ArrayRef,
    start: usize,
    len: usize,
) {
    let (_, shape) = shape(array.data_type()).expect("types are planned first");
    let index = nodes.len();
    nodes.push(Node {
        array: Arc::clone(array),
        start,
        len,
    });
    let validity = null_buffer(array.as_ref()).map_or(0, |_| len.div_ceil(8));
    specs.push(Spec {
        node: index,
        source: Source::Validity,
        bytes: validity as u64,
    });
    let mut push = |source, bytes: usize| {
        specs.push(Spec {
            node: index,
            source,
            bytes: bytes as u64,
        })
    };
    match shape {
        Shape::Null | Shape::Struct => {}
        Shape::Boolean => push(Source::BooleanValues, len.div_ceil(8)),
        Shape::Fixed(width) | Shape::FixedBinary(width) => push(Source::Fixed(width), len * width),
        Shape::Dictionary => push(Source::DictionaryKeys, len * 4),
        Shape::Bytes32 => {
            let offsets = offsets32(array.as_ref());
            push(Source::Offsets32, (len + 1) * 4);
            push(
                Source::Data32,
                (offsets[start + len] - offsets[start]) as usize,
            );
        }
        Shape::Bytes64 => {
            let offsets = offsets64(array.as_ref());
            push(Source::Offsets64, (len + 1) * 8);
            push(
                Source::Data64,
                (offsets[start + len] - offsets[start]) as usize,
            );
        }
        Shape::List32 => push(Source::Offsets32, (len + 1) * 4),
        Shape::List64 => push(Source::Offsets64, (len + 1) * 8),
    }
    match shape {
        Shape::List32 => {
            let offsets = offsets32(array.as_ref());
            let (child_start, child_end) = (offsets[start] as usize, offsets[start + len] as usize);
            let values = Arc::clone(array.as_list::<i32>().values());
            build_nodes(nodes, specs, &values, child_start, child_end - child_start);
        }
        Shape::List64 => {
            let offsets = offsets64(array.as_ref());
            let (child_start, child_end) = (offsets[start] as usize, offsets[start + len] as usize);
            let values = Arc::clone(array.as_list::<i64>().values());
            build_nodes(nodes, specs, &values, child_start, child_end - child_start);
        }
        Shape::Struct => {
            for child in array.as_struct().columns() {
                build_nodes(nodes, specs, child, start, len);
            }
        }
        Shape::Dictionary => {
            let values = Arc::clone(array.as_dictionary::<Int32Type>().values());
            let count = values.len();
            build_nodes(nodes, specs, &values, 0, count);
        }
        _ => {}
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Prefix { offset: usize },
    Buffer { index: usize, offset: u64 },
    Complete,
}

pub struct CowSelectionEncoder {
    nodes: Vec<Node>,
    specs: Vec<Spec>,
    /// This complete batch's header and directory.
    prefix: Vec<u8>,
    rows: u64,
    phase: Phase,
    pending: CowSelectionTotals,
    totals: CowSelectionTotals,
    // Data fields precede credits: the last physical payload exits first.
    retained: Option<crate::root_result_session::CowNormalizationRetention>,
}
impl CowSelectionEncoder {
    /// Exact heap capacity of the cursor for this input, plus its inline
    /// storage. The caller prepays it before construction.
    pub fn scratch_bytes(
        batch: &RecordBatch,
        recipe: &CowSelectionRecipe,
    ) -> Result<usize, CowSelectionCodecError> {
        let plan = Self::plan(batch, recipe)?;
        Self::scratch_for(&plan, batch.num_rows())
    }
    fn plan(
        batch: &RecordBatch,
        recipe: &CowSelectionRecipe,
    ) -> Result<Plan, CowSelectionCodecError> {
        if recipe.schema().fields().len() != batch.num_columns()
            || recipe
                .schema()
                .fields()
                .iter()
                .zip(batch.columns())
                .any(|(field, column)| field.data_type() != column.data_type())
        {
            return Err(CowSelectionCodecError::SchemaChanged);
        }
        let mut plan = Plan::default();
        for field in batch.schema_ref().fields() {
            plan.field(field.name(), field.data_type(), 1)?;
        }
        if plan.nodes == 0 {
            return Err(CowSelectionCodecError::SchemaLimit);
        }
        // Directory counts are u32 in the wire header. Memory capacity is
        // granted by the original Root budget before allocating the cursor.
        u32::try_from(plan.nodes).map_err(|_| CowSelectionCodecError::RecordLimit)?;
        u32::try_from(plan.buffers).map_err(|_| CowSelectionCodecError::RecordLimit)?;
        Ok(plan)
    }
    fn prefix_bytes(plan: &Plan, rows: usize) -> Result<usize, CowSelectionCodecError> {
        if rows == 0 {
            Ok(0)
        } else {
            plan.nodes
                .checked_add(plan.buffers)
                .and_then(|count| count.checked_mul(8))
                .and_then(|bytes| bytes.checked_add(COW_SELECTION_HEADER_BYTES))
                .ok_or(CowSelectionCodecError::RecordLimit)
        }
    }
    fn scratch_for(plan: &Plan, rows: usize) -> Result<usize, CowSelectionCodecError> {
        let tables = if rows == 0 {
            0
        } else {
            plan.nodes
                .checked_mul(size_of::<Node>())
                .and_then(|nodes| {
                    plan.buffers
                        .checked_mul(size_of::<Spec>())
                        .and_then(|buffers| nodes.checked_add(buffers))
                })
                .ok_or(CowSelectionCodecError::RecordLimit)?
        };
        size_of::<Self>()
            .checked_add(tables)
            .and_then(|bytes| bytes.checked_add(Self::prefix_bytes(plan, rows).ok()?))
            .ok_or(CowSelectionCodecError::RecordLimit)
    }
    pub fn try_new(
        batch: &RecordBatch,
        recipe: &CowSelectionRecipe,
        totals: CowSelectionTotals,
        prepaid_scratch_capacity: usize,
    ) -> Result<Self, CowSelectionCodecError> {
        let plan = Self::plan(batch, recipe)?;
        let rows = batch.num_rows();
        if rows != 0 && totals.batches >= MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES {
            return Err(CowSelectionCodecError::BatchLimit);
        }
        if Self::scratch_for(&plan, rows)? > prepaid_scratch_capacity {
            return Err(CowSelectionCodecError::ScratchLimit);
        }
        let mut prefix = Vec::with_capacity(Self::prefix_bytes(&plan, rows)?);
        let mut pending = totals.clone();
        let (mut nodes, mut specs) = (Vec::new(), Vec::new());
        if rows != 0 {
            nodes.reserve_exact(plan.nodes);
            specs.reserve_exact(plan.buffers);
            for column in batch.columns() {
                build_nodes(&mut nodes, &mut specs, column, 0, rows);
            }
            debug_assert_eq!((nodes.len(), specs.len()), (plan.nodes, plan.buffers));
            let record = specs
                .iter()
                .try_fold(
                    u64::try_from(Self::prefix_bytes(&plan, rows)?)
                        .map_err(|_| CowSelectionCodecError::RecordLimit)?,
                    |total, spec| total.checked_add(spec.bytes),
                )
                .ok_or(CowSelectionCodecError::RecordLimit)?;
            prefix.extend_from_slice(
                &CowSelectionRecordHeader {
                    kind: CowSelectionRecordKind::Batch,
                    record_bytes: record,
                    rows: rows as u64,
                    nodes: plan.nodes as u32,
                    buffers: plan.buffers as u32,
                    recipe_digest: *recipe.digest(),
                }
                .bytes(),
            );
            for node in &nodes {
                prefix.extend_from_slice(&(node.len as u64).to_le_bytes());
            }
            for spec in &specs {
                prefix.extend_from_slice(&spec.bytes.to_le_bytes());
            }
            pending.batches += 1;
            pending.rows += rows as u64;
        }
        Ok(Self {
            nodes,
            specs,
            phase: if prefix.is_empty() {
                Phase::Complete
            } else {
                Phase::Prefix { offset: 0 }
            },
            prefix,
            rows: rows as u64,
            totals: if rows == 0 { pending.clone() } else { totals },
            pending,
            retained: None,
        })
    }
    pub(crate) fn retain_normalization(
        &mut self,
        retained: crate::root_result_session::CowNormalizationRetention,
    ) {
        self.retained = Some(retained);
    }
    /// Totals of records whose entire encoding has been emitted.
    pub fn totals(&self) -> CowSelectionTotals {
        self.totals.clone()
    }
    /// Every limit was checked at construction, so emission cannot fail.
    pub fn step(&mut self, output: &mut [u8]) -> RenderTurn {
        let mut turn = RenderTurn {
            emitted_bytes: 0,
            examined_bytes: 0,
            visited_cells: 0,
            completed_rows: 0,
            status: RenderTurnStatus::Yielded,
        };
        loop {
            if matches!(self.phase, Phase::Complete) {
                turn.status = RenderTurnStatus::InputComplete;
                return turn;
            }
            if turn.visited_cells == COW_SELECTION_TURN_WORK
                || turn.examined_bytes == COW_SELECTION_TURN_BYTES
            {
                return turn;
            }
            turn.visited_cells += 1;
            match self.phase {
                Phase::Prefix { offset } => {
                    if offset == self.prefix.len() {
                        self.next_buffer(0, &mut turn);
                        continue;
                    }
                    if turn.emitted_bytes == output.len() {
                        turn.status = RenderTurnStatus::NeedsOutput;
                        return turn;
                    }
                    let count = (self.prefix.len() - offset)
                        .min(output.len() - turn.emitted_bytes)
                        .min(COW_SELECTION_TURN_BYTES - turn.examined_bytes);
                    output[turn.emitted_bytes..turn.emitted_bytes + count]
                        .copy_from_slice(&self.prefix[offset..offset + count]);
                    turn.emitted_bytes += count;
                    turn.examined_bytes += count;
                    self.phase = Phase::Prefix {
                        offset: offset + count,
                    };
                }
                Phase::Buffer { index, offset } => {
                    let spec = self.specs[index];
                    if offset == spec.bytes {
                        self.next_buffer(index + 1, &mut turn);
                        continue;
                    }
                    if turn.emitted_bytes == output.len() {
                        turn.status = RenderTurnStatus::NeedsOutput;
                        return turn;
                    }
                    let limit = ((spec.bytes - offset) as usize)
                        .min(output.len() - turn.emitted_bytes)
                        .min(COW_SELECTION_TURN_BYTES - turn.examined_bytes);
                    let target = &mut output[turn.emitted_bytes..turn.emitted_bytes + limit];
                    fill(&self.nodes[spec.node], spec.source, offset as usize, target);
                    turn.emitted_bytes += limit;
                    turn.examined_bytes += limit;
                    self.phase = Phase::Buffer {
                        index,
                        offset: offset + limit as u64,
                    };
                }
                Phase::Complete => unreachable!("the terminal phase is checked first"),
            }
        }
    }
    fn next_buffer(&mut self, index: usize, turn: &mut RenderTurn) {
        if index < self.specs.len() {
            self.phase = Phase::Buffer { index, offset: 0 };
        } else {
            self.phase = Phase::Complete;
            self.totals = self.pending.clone();
            turn.completed_rows += self.rows;
        }
    }
}

/// Copy `target.len()` bytes of one logical buffer, starting at `offset`.
fn fill(node: &Node, source: Source, offset: usize, target: &mut [u8]) {
    let array = node.array.as_ref();
    match source {
        Source::Validity => {
            let nulls = null_buffer(array).expect("a validity buffer has a null source");
            let bit_start = nulls.offset() + node.start;
            for (at, byte) in target.iter_mut().enumerate() {
                *byte = bitmap_byte(nulls.buffer().as_slice(), bit_start, node.len, offset + at);
            }
        }
        Source::BooleanValues => {
            let values = array.as_boolean().values();
            let bit_start = values.offset() + node.start;
            for (at, byte) in target.iter_mut().enumerate() {
                *byte = bitmap_byte(values.values(), bit_start, node.len, offset + at);
            }
        }
        Source::Fixed(width) => {
            let base = node.start * width + offset;
            target.copy_from_slice(&primitive_bytes(array)[base..base + target.len()]);
        }
        Source::DictionaryKeys => {
            let base = node.start * 4 + offset;
            target.copy_from_slice(&primitive_bytes(array)[base..base + target.len()]);
        }
        Source::Offsets32 => {
            let offsets = offsets32(array);
            let base = offsets[node.start];
            for (at, byte) in target.iter_mut().enumerate() {
                let position = offset + at;
                let value = offsets[node.start + position / 4] - base;
                *byte = value.to_le_bytes()[position % 4];
            }
        }
        Source::Offsets64 => {
            let offsets = offsets64(array);
            let base = offsets[node.start];
            for (at, byte) in target.iter_mut().enumerate() {
                let position = offset + at;
                let value = offsets[node.start + position / 8] - base;
                *byte = value.to_le_bytes()[position % 8];
            }
        }
        Source::Data32 => {
            let base = offsets32(array)[node.start] as usize + offset;
            target.copy_from_slice(&data_bytes(array)[base..base + target.len()]);
        }
        Source::Data64 => {
            let base = offsets64(array)[node.start] as usize + offset;
            target.copy_from_slice(&data_bytes(array)[base..base + target.len()]);
        }
    }
}

/// Frontend-side decoding of complete, length-validated records.
enum SourceConstruction {
    /// Finite local source construction claims no Root byte authorization.
    LocalRetention(ConnectorPayloadRetentionGuard),
    /// Every allocation requests the same original Root's real byte author.
    Governed(ConnectorPayloadRetentionGuard),
}

pub struct CowSelectionDecoder;
impl CowSelectionDecoder {
    pub fn decode_batch(
        recipe: &CowSelectionRecipe,
        record: &[u8],
    ) -> Result<RecordBatch, CowSelectionCodecError> {
        let header = CowSelectionRecordHeader::parse(record)?;
        header.validate_record_bytes(record.len())?;
        recipe.validate_digest(header)?;
        let schema = recipe.schema();
        if header.kind != CowSelectionRecordKind::Batch {
            return Err(CowSelectionCodecError::MalformedBatch);
        }
        let malformed = CowSelectionCodecError::MalformedBatch;
        let nodes = header.nodes as usize;
        let buffers = header.buffers as usize;
        let directory = &record[COW_SELECTION_HEADER_BYTES..];
        let mut state = BatchState {
            lengths: &directory[..8 * nodes],
            sizes: &directory[8 * nodes..8 * (nodes + buffers)],
            payload: &record[COW_SELECTION_HEADER_BYTES + 8 * (nodes + buffers)..],
            node: 0,
            buffer: 0,
            position: 0,
        };
        let rows = usize::try_from(header.rows).map_err(|_| malformed)?;
        let mut columns = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let data = state.array(field.data_type())?;
            if data.len() != rows {
                return Err(malformed);
            }
            columns.push(make_array(data));
        }
        if state.node != nodes || state.buffer != buffers || state.position != state.payload.len() {
            return Err(malformed);
        }
        RecordBatch::try_new_with_options(
            Arc::clone(schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|_| malformed)
    }

    /// The bounded source path copies and assembles every allocation through
    /// the SPI owner. The receipt never adopts a caller-provided Arrow batch.
    pub fn decode_batch_owned(
        recipe: &Arc<CowSelectionRecipe>,
        record: &[u8],
    ) -> Result<ConnectorRowMutationSourceBatch, CowOwnedDecodeError> {
        let retention = ConnectorPayloadRetentionGuard::from_shared(Arc::clone(recipe));
        Self::decode_batch_owned_inner(
            recipe,
            record,
            SourceConstruction::LocalRetention(retention),
        )
    }

    pub fn decode_batch_owned_with_guard(
        recipe: &Arc<CowSelectionRecipe>,
        record: &[u8],
        guard: ConnectorPayloadRetentionGuard,
    ) -> Result<ConnectorRowMutationSourceBatch, CowOwnedDecodeError> {
        let retention = ConnectorPayloadRetentionGuard::from_shared(Arc::clone(recipe));
        Self::decode_batch_owned_inner(
            recipe,
            record,
            SourceConstruction::Governed(guard.with_lifetime_retention(retention)),
        )
    }

    fn decode_batch_owned_inner(
        recipe: &CowSelectionRecipe,
        record: &[u8],
        construction: SourceConstruction,
    ) -> Result<ConnectorRowMutationSourceBatch, CowOwnedDecodeError> {
        let header = CowSelectionRecordHeader::parse(record)?;
        header.validate_record_bytes(record.len())?;
        recipe.validate_digest(header)?;
        let schema = recipe.schema();
        if header.kind != CowSelectionRecordKind::Batch {
            return Err(CowSelectionCodecError::MalformedBatch.into());
        }
        if record.len() > MAX_CONNECTOR_ROW_MUTATION_SOURCE_BYTES
            || header.rows > RootProfileV1::MAX_ELEMENTS_PER_ROW as u64
        {
            return Err(CowSelectionCodecError::SourceLimit.into());
        }
        let malformed = CowSelectionCodecError::MalformedBatch;
        let nodes = header.nodes as usize;
        let buffers = header.buffers as usize;
        let directory = &record[COW_SELECTION_HEADER_BYTES..];
        // Borrow directory words instead of holding a second directory copy
        // alongside the source construction and complete assembled record.
        let mut state = BatchState {
            lengths: &directory[..8 * nodes],
            sizes: &directory[8 * nodes..8 * (nodes + buffers)],
            payload: &directory[8 * (nodes + buffers)..],
            node: 0,
            buffer: 0,
            position: 0,
        };
        let mut owner = match construction {
            SourceConstruction::Governed(author) => {
                ConnectorRowMutationSourceBuilder::try_new_with_guard(Arc::clone(schema), author)
            }
            SourceConstruction::LocalRetention(lifetime) => {
                ConnectorRowMutationSourceBuilder::try_new_with_retention(
                    Arc::clone(schema),
                    lifetime,
                )
            }
        }
        .map_err(CowOwnedDecodeError::Source)?;
        let rows = usize::try_from(header.rows).map_err(|_| malformed)?;
        let mut columns = owner
            .children(schema.fields().len())
            .map_err(CowOwnedDecodeError::Source)?;
        for field in schema.fields() {
            columns
                .push(state.array_owned(field.data_type(), &mut owner)?)
                .map_err(CowOwnedDecodeError::Source)?;
        }
        if state.node != nodes || state.buffer != buffers || state.position != state.payload.len() {
            return Err(malformed.into());
        }
        owner
            .finish(rows, columns)
            .map_err(CowOwnedDecodeError::Source)
    }
}

/// A whole selection uses the exact recipe installed before its first body.
/// No record can add, replace or infer that declaration.
#[derive(Debug)]
pub struct CowSelectionStreamDecoder {
    recipe: Arc<CowSelectionRecipe>,
    batches: usize,
}
impl CowSelectionStreamDecoder {
    pub fn new(recipe: Arc<CowSelectionRecipe>) -> Self {
        Self { recipe, batches: 0 }
    }
    pub fn schema(&self) -> &SchemaRef {
        self.recipe.schema()
    }
    pub fn recipe(&self) -> &Arc<CowSelectionRecipe> {
        &self.recipe
    }

    pub fn apply_record(
        &mut self,
        record: &[u8],
    ) -> Result<Option<RecordBatch>, CowSelectionCodecError> {
        if self.batches >= MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES {
            return Err(CowSelectionCodecError::BatchLimit);
        }
        let batch = CowSelectionDecoder::decode_batch(&self.recipe, record)?;
        self.batches += 1;
        Ok(Some(batch))
    }
    pub fn apply_owned_record(
        &mut self,
        record: &[u8],
    ) -> Result<Option<ConnectorRowMutationSourceBatch>, CowOwnedDecodeError> {
        self.apply_owned_record_inner(record, None)
    }
    pub fn apply_owned_record_with_guard(
        &mut self,
        record: &[u8],
        guard: ConnectorPayloadRetentionGuard,
    ) -> Result<Option<ConnectorRowMutationSourceBatch>, CowOwnedDecodeError> {
        self.apply_owned_record_inner(record, Some(guard))
    }
    fn apply_owned_record_inner(
        &mut self,
        record: &[u8],
        guard: Option<ConnectorPayloadRetentionGuard>,
    ) -> Result<Option<ConnectorRowMutationSourceBatch>, CowOwnedDecodeError> {
        if self.batches >= MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES {
            return Err(CowSelectionCodecError::BatchLimit.into());
        }
        let batch = match guard {
            Some(guard) => {
                CowSelectionDecoder::decode_batch_owned_with_guard(&self.recipe, record, guard)?
            }
            None => CowSelectionDecoder::decode_batch_owned(&self.recipe, record)?,
        };
        self.batches += 1;
        Ok(Some(batch))
    }
    pub fn finish(self) -> SchemaRef {
        Arc::clone(self.recipe.schema())
    }
}

struct BatchState<'a> {
    lengths: &'a [u8],
    sizes: &'a [u8],
    payload: &'a [u8],
    node: usize,
    buffer: usize,
    position: usize,
}
impl<'a> BatchState<'a> {
    fn buffer_bytes(
        &mut self,
        expected: Option<usize>,
    ) -> Result<Option<&'a [u8]>, CowSelectionCodecError> {
        let malformed = CowSelectionCodecError::MalformedBatch;
        let at = self.buffer.checked_mul(8).ok_or(malformed)?;
        let word = self.sizes.get(at..at + 8).ok_or(malformed)?;
        let size =
            usize::try_from(u64::from_le_bytes(word.try_into().unwrap())).map_err(|_| malformed)?;
        self.buffer += 1;
        if expected.is_some_and(|expected| expected != size) {
            return Err(malformed);
        }
        let end = self
            .position
            .checked_add(size)
            .filter(|end| *end <= self.payload.len())
            .ok_or(malformed)?;
        let bytes = &self.payload[self.position..end];
        self.position = end;
        Ok((size != 0 || expected.is_some()).then_some(bytes))
    }
    fn buffer(
        &mut self,
        expected: Option<usize>,
    ) -> Result<Option<Buffer>, CowSelectionCodecError> {
        Ok(self.buffer_bytes(expected)?.map(Buffer::from_slice_ref))
    }
    fn array(&mut self, data_type: &DataType) -> Result<ArrayData, CowSelectionCodecError> {
        let malformed = CowSelectionCodecError::MalformedBatch;
        let (_, shape) = shape(data_type).map_err(|_| malformed)?;
        let at = self.node.checked_mul(8).ok_or(malformed)?;
        let word = self.lengths.get(at..at + 8).ok_or(malformed)?;
        let len =
            usize::try_from(u64::from_le_bytes(word.try_into().unwrap())).map_err(|_| malformed)?;
        self.node += 1;
        let validity = self.buffer(None)?;
        // Non-nullable columns may still carry an all-valid bitmap; the
        // assembled RecordBatch rejects any actual null in them.
        if validity
            .as_ref()
            .is_some_and(|bytes| bytes.len() != len.div_ceil(8) || matches!(shape, Shape::Null))
        {
            return Err(malformed);
        }
        let mut builder = ArrayData::builder(data_type.clone())
            .len(len)
            .null_bit_buffer(validity);
        match shape {
            Shape::Null | Shape::Struct => {}
            Shape::Boolean => {
                builder = builder.add_buffer(self.buffer(Some(len.div_ceil(8)))?.unwrap())
            }
            Shape::Fixed(width) | Shape::FixedBinary(width) => {
                let bytes = len.checked_mul(width).ok_or(malformed)?;
                builder = builder.add_buffer(self.buffer(Some(bytes))?.unwrap());
            }
            Shape::Dictionary => {
                let bytes = len.checked_mul(4).ok_or(malformed)?;
                builder = builder.add_buffer(self.buffer(Some(bytes))?.unwrap());
            }
            Shape::Bytes32 | Shape::List32 | Shape::Bytes64 | Shape::List64 => {
                let width = if matches!(shape, Shape::Bytes32 | Shape::List32) {
                    4
                } else {
                    8
                };
                let bytes = len
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(width))
                    .ok_or(malformed)?;
                builder = builder.add_buffer(self.buffer(Some(bytes))?.unwrap());
                if matches!(shape, Shape::Bytes32 | Shape::Bytes64) {
                    let data = self
                        .buffer(None)?
                        .unwrap_or_else(|| Buffer::from_slice_ref([0_u8; 0]));
                    builder = builder.add_buffer(data);
                }
            }
        }
        match data_type {
            DataType::List(item) | DataType::LargeList(item) => {
                builder = builder.add_child_data(self.array(item.data_type())?);
            }
            DataType::Struct(fields) => {
                for child in fields {
                    let data = self.array(child.data_type())?;
                    if data.len() != len {
                        return Err(malformed);
                    }
                    builder = builder.add_child_data(data);
                }
            }
            DataType::Dictionary(_, value) => {
                builder = builder.add_child_data(self.array(value)?);
            }
            _ => {}
        }
        builder.build().map_err(|_| malformed)
    }

    fn array_owned(
        &mut self,
        data_type: &DataType,
        owner: &mut ConnectorRowMutationSourceBuilder,
    ) -> Result<ConnectorRowMutationSourceArray, CowOwnedDecodeError> {
        let malformed = CowSelectionCodecError::MalformedBatch;
        let (_, shape) = shape(data_type).map_err(|_| malformed)?;
        let node = self.node;
        let at = node.checked_mul(8).ok_or(malformed)?;
        let word = self.lengths.get(at..at + 8).ok_or(malformed)?;
        let len =
            usize::try_from(u64::from_le_bytes(word.try_into().unwrap())).map_err(|_| malformed)?;
        if len > RootProfileV1::MAX_ELEMENTS_PER_ROW {
            return Err(CowSelectionCodecError::SourceLimit.into());
        }
        self.node += 1;
        let copy = |bytes: Option<&[u8]>, owner: &mut ConnectorRowMutationSourceBuilder| {
            bytes
                .map(|bytes| {
                    owner
                        .copy_buffer(bytes)
                        .map_err(CowOwnedDecodeError::Source)
                })
                .transpose()
        };
        let validity_bytes = self.buffer_bytes(None)?;
        if validity_bytes
            .is_some_and(|bytes| bytes.len() != len.div_ceil(8) || matches!(shape, Shape::Null))
        {
            return Err(malformed.into());
        }
        let validity = copy(validity_bytes, owner)?;
        let (first, second) = match shape {
            Shape::Null | Shape::Struct => (None, None),
            Shape::Boolean => (
                copy(self.buffer_bytes(Some(len.div_ceil(8)))?, owner)?,
                None,
            ),
            Shape::Fixed(width) | Shape::FixedBinary(width) => (
                copy(
                    self.buffer_bytes(Some(len.checked_mul(width).ok_or(malformed)?))?,
                    owner,
                )?,
                None,
            ),
            Shape::Dictionary => (
                copy(
                    self.buffer_bytes(Some(len.checked_mul(4).ok_or(malformed)?))?,
                    owner,
                )?,
                None,
            ),
            Shape::Bytes32 | Shape::List32 | Shape::Bytes64 | Shape::List64 => {
                let width = if matches!(shape, Shape::Bytes32 | Shape::List32) {
                    4
                } else {
                    8
                };
                let bytes = len
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(width))
                    .ok_or(malformed)?;
                let first = copy(self.buffer_bytes(Some(bytes))?, owner)?;
                let second = if matches!(shape, Shape::Bytes32 | Shape::Bytes64) {
                    Some(
                        owner
                            .copy_buffer(self.buffer_bytes(None)?.unwrap_or(&[]))
                            .map_err(CowOwnedDecodeError::Source)?,
                    )
                } else {
                    None
                };
                (first, second)
            }
        };
        let count = match data_type {
            DataType::List(_) | DataType::LargeList(_) | DataType::Dictionary(_, _) => 1,
            DataType::Struct(fields) => fields.len(),
            _ => 0,
        };
        let mut children = owner.children(count).map_err(CowOwnedDecodeError::Source)?;
        match data_type {
            DataType::List(item) | DataType::LargeList(item) => children
                .push(self.array_owned(item.data_type(), owner)?)
                .map_err(CowOwnedDecodeError::Source)?,
            DataType::Struct(fields) => {
                for child in fields {
                    children
                        .push(self.array_owned(child.data_type(), owner)?)
                        .map_err(CowOwnedDecodeError::Source)?;
                }
            }
            DataType::Dictionary(_, value) => children
                .push(self.array_owned(value, owner)?)
                .map_err(CowOwnedDecodeError::Source)?,
            _ => {}
        }
        owner
            .array(node, len, validity, first, second, children)
            .map_err(CowOwnedDecodeError::Source)
    }
}

impl novarocks_execution_contract::failure::FailureProjection for CowSelectionCodecError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let (class, code) = match self {
            Self::UnsupportedType => (FailureClass::Invariant, 300),
            Self::SchemaLimit => (FailureClass::Resource, 301),
            Self::SchemaChanged => (FailureClass::Invariant, 302),
            Self::BatchLimit => (FailureClass::Resource, 303),
            Self::RecordLimit => (FailureClass::Resource, 304),
            Self::ScratchLimit => (FailureClass::Resource, 305),
            Self::TruncatedHeader => (FailureClass::Data, 306),
            Self::HeaderVersion => (FailureClass::Data, 307),
            Self::RecordLength => (FailureClass::Data, 308),
            Self::TruncatedRecord => (FailureClass::Data, 309),
            Self::TrailingRecordBytes => (FailureClass::Data, 310),
            Self::RecipeMismatch => (FailureClass::Data, 311),
            Self::MalformedBatch => (FailureClass::Data, 312),
            Self::SourceLimit => (FailureClass::Resource, 313),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

#[cfg(test)]
mod owned_source_tests {
    use super::*;
    use arrow::{
        array::{Int32Array, StringArray, StructArray},
        datatypes::{Field, Schema},
    };
    mod support {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/common/cow_recipe.rs"
        ));
    }
    fn encode(batch: &RecordBatch, recipe: &CowSelectionRecipe) -> Vec<u8> {
        let batch = support::normalized(batch, recipe);
        let mut encoder =
            CowSelectionEncoder::try_new(&batch, recipe, CowSelectionTotals::default(), usize::MAX)
                .unwrap();
        let mut bytes = Vec::new();
        let mut output = [0u8; 1024];
        loop {
            let turn = encoder.step(&mut output);
            bytes.extend_from_slice(&output[..turn.emitted_bytes]);
            if turn.status == RenderTurnStatus::InputComplete {
                return bytes;
            }
        }
    }
    #[test]
    fn header_checks_wire_geometry_without_a_record_or_node_quota() {
        let nodes = u32::try_from(2 * RootProfileV1::SCHEMA_TYPE_NODES + 1).unwrap();
        let header = CowSelectionRecordHeader {
            kind: CowSelectionRecordKind::Batch,
            record_bytes: 256 * 1024 * 1024 + u64::from(nodes) * 8,
            rows: 1,
            nodes,
            buffers: 0,
            recipe_digest: [7; 32],
        };
        assert_eq!(CowSelectionRecordHeader::parse(&header.bytes()), Ok(header));
        let malformed = CowSelectionRecordHeader {
            record_bytes: 64,
            ..header
        };
        assert_eq!(
            CowSelectionRecordHeader::parse(&malformed.bytes()),
            Err(CowSelectionCodecError::RecordLength)
        );
    }
    #[test]
    fn installed_recipe_decodes_first_batch_without_a_schema_record() {
        let child = Arc::new(Int32Array::from(vec![Some(7), None]));
        let fields = vec![Arc::new(Field::new("child", DataType::Int32, true))].into();
        let array = Arc::new(StructArray::new(fields, vec![child], None)) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "root",
                array.data_type().clone(),
                false,
            )])),
            vec![array],
        )
        .unwrap();
        let recipe = support::recipe(batch.schema_ref());
        let stream = encode(&batch, &recipe);
        let header = CowSelectionRecordHeader::parse(&stream).unwrap();
        assert_eq!(header.kind(), CowSelectionRecordKind::Batch);
        assert_eq!(header.record_bytes() as usize, stream.len());
        let mut decoder = CowSelectionStreamDecoder::new(Arc::clone(&recipe));
        let decoded = decoder.apply_owned_record(&stream).unwrap().unwrap();
        assert_eq!(decoded.batch().columns(), batch.columns());
        assert!(decoded.source_bytes() >= decoded.batch().get_array_memory_size());
    }
    #[test]
    fn digest_failure_precedes_array_allocation_and_schema_messages_are_rejected() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let recipe = support::recipe(batch.schema_ref());
        let mut stream = encode(&batch, &recipe);
        stream[32] ^= 1;
        let mut decoder = CowSelectionStreamDecoder::new(recipe);
        assert!(matches!(
            decoder.apply_record(&stream),
            Err(CowSelectionCodecError::RecipeMismatch)
        ));
        stream[4] = 1;
        assert!(matches!(
            CowSelectionRecordHeader::parse(&stream),
            Err(CowSelectionCodecError::HeaderVersion)
        ));
    }
    #[test]
    fn owned_source_keeps_recipe_metadata_until_the_last_batch_exits() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["original"]))],
        )
        .unwrap();
        let holder = Arc::new(());
        let weak = Arc::downgrade(&holder);
        let recipe = Arc::try_unwrap(support::recipe(batch.schema_ref()))
            .unwrap()
            .with_metadata_retention(ConnectorPayloadRetentionGuard::from_shared(holder));
        let recipe = Arc::new(recipe);
        let stream = encode(&batch, &recipe);
        let mut decoder = CowSelectionStreamDecoder::new(recipe);
        let source = decoder.apply_owned_record(&stream).unwrap().unwrap();
        drop(decoder);
        assert!(weak.upgrade().is_some());
        drop(source);
        assert!(weak.upgrade().is_none());
    }
}

mod preparation;
pub(crate) use preparation::prepare_recipe;
