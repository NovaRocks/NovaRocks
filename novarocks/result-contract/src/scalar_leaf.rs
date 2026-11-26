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

//! Borrowed typed ScalarValueV1 leaf records. The caller supplies an already
//! frozen semantic schema and protected value; this cursor allocates nothing.
//! Container values need their separate cursor and are not silently formatted.
//! Source admission, schema transport, collector and session commit are absent.

use crate::{
    ScalarField, ScalarOpaqueType, ScalarProfileV1, ScalarSchema, ScalarTimestampUnit,
    ScalarValueType,
};

pub const SCALAR_LEAF_HEADER_BYTES: usize = 24;
pub(crate) const NULL: u8 = 1;
pub(crate) const ABSENT: u8 = 2;
pub(crate) const CONTAINER_LIST: u8 = 15;
pub(crate) const CONTAINER_MAP: u8 = 16;
pub(crate) const CONTAINER_STRUCT: u8 = 17;
// Tag 12 was the retired UTF-8 JSON leaf.
const NATIVE_JSON: u8 = 18;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarLeafError {
    Type,
    Nullability,
    ValueLimit,
    InvalidValue,
    UnsupportedContainer,
    MalformedRecord,
}
impl std::fmt::Display for ScalarLeafError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Type => "scalar leaf differs from its frozen semantic type",
            Self::Nullability => "scalar value violates frozen nullability",
            Self::ValueLimit => "scalar value exceeds its admitted profile",
            Self::InvalidValue => "invalid scalar coefficient or representation",
            Self::UnsupportedContainer => "scalar container value requires the container cursor",
            Self::MalformedRecord => "malformed scalar leaf record",
        })
    }
}
impl std::error::Error for ScalarLeafError {}

/// Each tag is supplied by an exact source bridge, never guessed from binary
/// or text bytes. Float values are their raw IEEE bits; Decimal is unscaled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BorrowedScalarLeaf<'a> {
    NoRows,
    Null,
    Boolean(bool),
    SignedInteger {
        bits: u16,
        value: i64,
    },
    LargeInt(i128),
    Float32(u32),
    Float64(u64),
    Decimal128 {
        coefficient: i128,
        precision: u8,
        scale: u8,
    },
    Decimal256 {
        coefficient_le: [u8; 32],
        precision: u8,
        scale: u8,
    },
    String(&'a str),
    Binary(&'a [u8]),
    Date(i32),
    TimeMicros(i64),
    /// The exact source bridge checks the frozen timezone before creating this
    /// atom. The immutable paired schema retains it; no local clock is read.
    Timestamp {
        ticks: i64,
        unit: ScalarTimestampUnit,
    },
    /// The original v1 native document bytes, bounded by this record.
    /// A typed consumer opens the document through the native JSON value reader.
    Json(&'a [u8]),
    Variant(&'a [u8]),
    Opaque {
        kind: ScalarOpaqueType,
        bytes: &'a [u8],
    },
}

/// Validate one complete, already admitted record and expose its typed value
/// without copying variable payloads. This is not a streaming assembly owner:
/// the caller retains the covered backing until the borrowed value exits.
/// UTF-8 validation examines at most the frozen 64KiB single-value ceiling.
/// No collector publication or session commit follows from this validation.
impl<'a> BorrowedScalarLeaf<'a> {
    pub fn decode(schema: &ScalarSchema, record: &'a [u8]) -> Result<Self, ScalarLeafError> {
        let malformed = ScalarLeafError::MalformedRecord;
        let header = ScalarLeafHeader::decode(
            schema,
            record.get(..SCALAR_LEAF_HEADER_BYTES).ok_or(malformed)?,
        )?;
        if record.len() != header.record_bytes() {
            return Err(malformed);
        }
        let payload = &record[SCALAR_LEAF_HEADER_BYTES..];
        if header.flags == NULL | ABSENT {
            return Ok(Self::NoRows);
        }
        if header.flags == NULL {
            return Ok(Self::Null);
        }
        Self::decode_payload(schema.field(), payload)
    }

    pub(crate) fn decode_payload(
        field: &ScalarField,
        payload: &'a [u8],
    ) -> Result<Self, ScalarLeafError> {
        use ScalarValueType as T;
        let malformed = ScalarLeafError::MalformedRecord;
        let fixed = match field.value_type {
            T::Boolean => Some(1),
            T::SignedInteger(bits) => Some(usize::from(bits / 8)),
            T::LargeInt | T::Decimal { bits: 128, .. } => Some(16),
            T::Decimal { bits: 256, .. } => Some(32),
            T::Float32 | T::Date => Some(4),
            T::Float64 | T::TimeMicros | T::Timestamp { .. } => Some(8),
            _ => None,
        };
        if fixed.is_some_and(|len| payload.len() != len) {
            return Err(malformed);
        }
        let value = match &field.value_type {
            T::Null => return Err(malformed),
            T::Boolean => Self::Boolean(match payload[0] {
                0 => false,
                1 => true,
                _ => return Err(malformed),
            }),
            T::SignedInteger(bits) => {
                let value = match bits {
                    8 => i64::from(i8::from_le_bytes(payload.try_into().unwrap())),
                    16 => i64::from(i16::from_le_bytes(payload.try_into().unwrap())),
                    32 => i64::from(i32::from_le_bytes(payload.try_into().unwrap())),
                    64 => i64::from_le_bytes(payload.try_into().unwrap()),
                    _ => return Err(malformed),
                };
                Self::SignedInteger { bits: *bits, value }
            }
            T::LargeInt => Self::LargeInt(i128::from_le_bytes(payload.try_into().unwrap())),
            T::Float32 => Self::Float32(u32::from_le_bytes(payload.try_into().unwrap())),
            T::Float64 => Self::Float64(u64::from_le_bytes(payload.try_into().unwrap())),
            T::Decimal {
                bits: 128,
                precision,
                scale,
            } => Self::Decimal128 {
                coefficient: i128::from_le_bytes(payload.try_into().unwrap()),
                precision: *precision,
                scale: *scale,
            },
            T::Decimal {
                bits: 256,
                precision,
                scale,
            } => Self::Decimal256 {
                coefficient_le: payload.try_into().unwrap(),
                precision: *precision,
                scale: *scale,
            },
            T::String => Self::String(std::str::from_utf8(payload).map_err(|_| malformed)?),
            T::Binary => Self::Binary(payload),
            T::Date => Self::Date(i32::from_le_bytes(payload.try_into().unwrap())),
            T::TimeMicros => Self::TimeMicros(i64::from_le_bytes(payload.try_into().unwrap())),
            T::Timestamp { unit, .. } => Self::Timestamp {
                ticks: i64::from_le_bytes(payload.try_into().unwrap()),
                unit: *unit,
            },
            T::Json => Self::Json(payload),
            T::Variant => Self::Variant(payload),
            T::Opaque(kind) => Self::Opaque {
                kind: *kind,
                bytes: payload,
            },
            T::List(_) | T::Map { .. } | T::Struct(_) => {
                return Err(ScalarLeafError::UnsupportedContainer);
            }
            _ => return Err(malformed),
        };
        // Canonical encoder validation also checks coefficient precision. No
        // payload formatting, allocation or timezone conversion takes place.
        ScalarLeafCursor::try_new_for_field(field, value)?;
        Ok(value)
    }
}

/// Validated allocation declarations only. This grants no storage and validates
/// neither payload bytes nor execution success. An assembly owner must obtain
/// its covered capacity before accepting/copying the declared payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScalarLeafHeader {
    payload_bytes: u32,
    flags: u8,
}
impl ScalarLeafHeader {
    pub fn decode(schema: &ScalarSchema, header: &[u8]) -> Result<Self, ScalarLeafError> {
        let malformed = ScalarLeafError::MalformedRecord;
        if header.len() != SCALAR_LEAF_HEADER_BYTES || &header[..4] != b"SCV1" {
            return Err(malformed);
        }
        let total = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(header[8..12].try_into().unwrap());
        if length as usize > ScalarProfileV1::RECORD_PAYLOAD_BYTES {
            return Err(ScalarLeafError::ValueLimit);
        }
        if total != SCALAR_LEAF_HEADER_BYTES + length as usize {
            return Err(malformed);
        }
        // Check exact type parameters and every reserved byte before permitting
        // an assembly owner to allocate or copy any variable payload.
        let expected = ScalarLeafCursor::try_new(schema, BorrowedScalarLeaf::NoRows)?;
        if header[12] != expected.header[12] || header[14..24] != expected.header[14..24] {
            return Err(malformed);
        }
        let flags = header[13];
        match flags {
            flags if flags == NULL | ABSENT => {
                if length != 0 {
                    return Err(malformed);
                }
            }
            NULL => {
                if length != 0 {
                    return Err(malformed);
                }
                ScalarLeafCursor::try_new(schema, BorrowedScalarLeaf::Null)?;
            }
            0 => {
                let expected = match &schema.field().value_type {
                    ScalarValueType::Boolean => Some(1),
                    ScalarValueType::SignedInteger(bits) => Some(u32::from(bits / 8)),
                    ScalarValueType::LargeInt | ScalarValueType::Decimal { bits: 128, .. } => {
                        Some(16)
                    }
                    ScalarValueType::Float32 | ScalarValueType::Date => Some(4),
                    ScalarValueType::Float64
                    | ScalarValueType::TimeMicros
                    | ScalarValueType::Timestamp { .. } => Some(8),
                    ScalarValueType::Decimal { bits: 256, .. } => Some(32),
                    ScalarValueType::Null => return Err(malformed),
                    _ => None,
                };
                if expected.is_some_and(|expected| expected != length) {
                    return Err(malformed);
                }
            }
            _ => return Err(malformed),
        }
        Ok(Self {
            payload_bytes: length,
            flags,
        })
    }
    pub fn record_bytes(self) -> usize {
        SCALAR_LEAF_HEADER_BYTES + self.payload_bytes as usize
    }
    pub fn payload_bytes(self) -> usize {
        self.payload_bytes as usize
    }
    pub fn rows(self) -> u64 {
        u64::from(self.flags != NULL | ABSENT)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScalarLeafTurn {
    pub emitted_bytes: usize,
    pub complete: bool,
}

/// Immutable view is retained with the cursor, so a turn never reselects a
/// source cell. Fixed values are copied inline; variable bytes remain borrowed.
pub struct ScalarLeafCursor<'a> {
    header: [u8; SCALAR_LEAF_HEADER_BYTES],
    fixed: [u8; 32],
    variable: Option<&'a [u8]>,
    payload_len: usize,
    position: usize,
    rows: u64,
}
impl<'a> ScalarLeafCursor<'a> {
    pub fn try_new(
        schema: &ScalarSchema,
        value: BorrowedScalarLeaf<'a>,
    ) -> Result<Self, ScalarLeafError> {
        Self::try_new_for_field(schema.field(), value)
    }
    /// The same record for one field of an already admitted schema. Nested
    /// record writers use this so a leaf never clones its frozen type.
    pub(crate) fn try_new_for_field(
        field: &ScalarField,
        value: BorrowedScalarLeaf<'a>,
    ) -> Result<Self, ScalarLeafError> {
        use BorrowedScalarLeaf as V;
        use ScalarValueType as T;
        let mut cursor = Self {
            header: [0; SCALAR_LEAF_HEADER_BYTES],
            fixed: [0; 32],
            variable: None,
            payload_len: 0,
            position: 0,
            rows: 1,
        };
        cursor.header[..4].copy_from_slice(b"SCV1");
        let (kind, width, precision, scale, unit, opaque) = match &field.value_type {
            T::Null => (0, 0, 0, 0, 0, 0),
            T::Boolean => (1, 8, 0, 0, 0, 0),
            T::SignedInteger(bits) => (2, *bits, 0, 0, 0, 0),
            T::LargeInt => (3, 128, 0, 0, 0, 0),
            T::Float32 => (4, 32, 0, 0, 0, 0),
            T::Float64 => (5, 64, 0, 0, 0, 0),
            T::Decimal {
                bits,
                precision,
                scale,
            } => (6, *bits, *precision, *scale, 0, 0),
            T::String => (7, 0, 0, 0, 0, 0),
            T::Binary => (8, 0, 0, 0, 0, 0),
            T::Date => (9, 32, 0, 0, 0, 0),
            T::TimeMicros => (10, 64, 0, 0, 1, 0),
            T::Timestamp { unit, .. } => (11, 64, 0, 0, timestamp_unit(*unit), 0),
            T::Json => (NATIVE_JSON, 0, 0, 0, 0, 0),
            T::Variant => (13, 0, 0, 0, 0, 0),
            T::Opaque(kind) => (14, 0, 0, 0, 0, opaque_kind(*kind)),
            // Container records carry their payload through ScalarRecordWriter;
            // this cursor only frames their absent/null headers.
            T::List(_) => (CONTAINER_LIST, 0, 0, 0, 0, 0),
            T::Map { .. } => (CONTAINER_MAP, 0, 0, 0, 0, 0),
            T::Struct(_) => (CONTAINER_STRUCT, 0, 0, 0, 0, 0),
        };
        cursor.header[12] = kind;
        cursor.header[14..16].copy_from_slice(&width.to_le_bytes());
        cursor.header[16] = precision;
        cursor.header[17] = scale;
        cursor.header[18] = unit;
        cursor.header[19] = opaque;
        match (value, &field.value_type) {
            (V::NoRows, _) => {
                cursor.header[13] = NULL | ABSENT;
                cursor.rows = 0;
            }
            (V::Null, _) if field.nullable || matches!(field.value_type, T::Null) => {
                cursor.header[13] = NULL
            }
            (V::Null, _) => return Err(ScalarLeafError::Nullability),
            (_, T::List(_) | T::Map { .. } | T::Struct(_)) => {
                return Err(ScalarLeafError::UnsupportedContainer);
            }
            (V::Boolean(value), T::Boolean) => cursor.set_fixed(&[u8::from(value)]),
            (V::SignedInteger { bits, value }, T::SignedInteger(expected)) if bits == *expected => {
                let fits = match bits {
                    8 => i8::try_from(value).is_ok(),
                    16 => i16::try_from(value).is_ok(),
                    32 => i32::try_from(value).is_ok(),
                    64 => true,
                    _ => false,
                };
                if !fits {
                    return Err(ScalarLeafError::InvalidValue);
                }
                cursor.set_fixed(&value.to_le_bytes()[..usize::from(bits / 8)]);
            }
            (V::LargeInt(value), T::LargeInt) => cursor.set_fixed(&value.to_le_bytes()),
            (V::Float32(bits), T::Float32) => cursor.set_fixed(&bits.to_le_bytes()),
            (V::Float64(bits), T::Float64) => cursor.set_fixed(&bits.to_le_bytes()),
            (
                V::Decimal128 {
                    coefficient,
                    precision,
                    scale,
                },
                T::Decimal {
                    bits: 128,
                    precision: p,
                    scale: s,
                },
            ) if precision == *p && scale == *s => {
                if !decimal_in_range(&coefficient.to_le_bytes(), precision) {
                    return Err(ScalarLeafError::InvalidValue);
                }
                cursor.set_fixed(&coefficient.to_le_bytes());
            }
            (
                V::Decimal256 {
                    coefficient_le,
                    precision,
                    scale,
                },
                T::Decimal {
                    bits: 256,
                    precision: p,
                    scale: s,
                },
            ) if precision == *p && scale == *s => {
                if !decimal_in_range(&coefficient_le, precision) {
                    return Err(ScalarLeafError::InvalidValue);
                }
                cursor.set_fixed(&coefficient_le);
            }
            (V::String(bytes), T::String) => cursor.set_variable(bytes.as_bytes())?,
            (V::Json(bytes), T::Json) => cursor.set_variable(native_json_payload(bytes)?)?,
            (V::Binary(bytes), T::Binary) | (V::Variant(bytes), T::Variant) => {
                cursor.set_variable(bytes)?
            }
            (V::Opaque { kind, bytes }, T::Opaque(expected)) if kind == *expected => {
                cursor.set_variable(bytes)?
            }
            (V::Date(days), T::Date) => cursor.set_fixed(&days.to_le_bytes()),
            (V::TimeMicros(ticks), T::TimeMicros) => cursor.set_fixed(&ticks.to_le_bytes()),
            (V::Timestamp { ticks, unit }, T::Timestamp { unit: expected, .. })
                if unit == *expected =>
            {
                cursor.set_fixed(&ticks.to_le_bytes())
            }
            _ => return Err(ScalarLeafError::Type),
        }
        let total = SCALAR_LEAF_HEADER_BYTES
            .checked_add(cursor.payload_len)
            .ok_or(ScalarLeafError::ValueLimit)?;
        cursor.header[4..8].copy_from_slice(
            &u32::try_from(total)
                .map_err(|_| ScalarLeafError::ValueLimit)?
                .to_le_bytes(),
        );
        cursor.header[8..12].copy_from_slice(
            &u32::try_from(cursor.payload_len)
                .map_err(|_| ScalarLeafError::ValueLimit)?
                .to_le_bytes(),
        );
        Ok(cursor)
    }
    fn set_fixed(&mut self, bytes: &[u8]) {
        self.fixed[..bytes.len()].copy_from_slice(bytes);
        self.payload_len = bytes.len();
    }
    fn set_variable(&mut self, bytes: &'a [u8]) -> Result<(), ScalarLeafError> {
        if bytes.len() > ScalarProfileV1::RECORD_PAYLOAD_BYTES {
            return Err(ScalarLeafError::ValueLimit);
        }
        self.variable = Some(bytes);
        self.payload_len = bytes.len();
        Ok(())
    }
    pub fn rows(&self) -> u64 {
        self.rows
    }
    pub(crate) fn header(&self) -> [u8; SCALAR_LEAF_HEADER_BYTES] {
        self.header
    }
    pub fn encoded_len(&self) -> usize {
        SCALAR_LEAF_HEADER_BYTES + self.payload_len
    }
    /// One step copies at most the existing 64KiB turn allowance. No value,
    /// literal, output Vec or metadata allocation is performed here.
    pub fn step(&mut self, output: &mut [u8]) -> ScalarLeafTurn {
        let turn = self
            .copy_range(self.position, output)
            .expect("cursor position is within its immutable record");
        self.position += turn.emitted_bytes;
        turn
    }
    /// Read a bounded range of this immutable record without advancing its
    /// cursor. Native owners can retain a separate offset and their immutable
    /// Arrow source, then borrow that source anew each turn without constructing
    /// a self-referential source/cursor pair. Ordering and single-record cardinality
    /// remain the root producer's responsibility; this adds no stream frontier.
    pub fn copy_range(
        &self,
        position: usize,
        output: &mut [u8],
    ) -> Result<ScalarLeafTurn, ScalarLeafError> {
        let end = self.encoded_len();
        if position > end {
            return Err(ScalarLeafError::MalformedRecord);
        }
        let limit = output
            .len()
            .min(crate::RootProfileV1::EMIT_BYTES_PER_TURN)
            .min(end - position);
        let mut position = position;
        let mut written = 0;
        if position < SCALAR_LEAF_HEADER_BYTES {
            let count = limit.min(SCALAR_LEAF_HEADER_BYTES - position);
            output[..count].copy_from_slice(&self.header[position..position + count]);
            position += count;
            written += count;
        }
        if written < limit {
            let payload = self
                .variable
                .unwrap_or_else(|| &self.fixed[..self.payload_len]);
            let offset = position - SCALAR_LEAF_HEADER_BYTES;
            let count = limit - written;
            output[written..limit].copy_from_slice(&payload[offset..offset + count]);
            position += count;
            written += count;
        }
        Ok(ScalarLeafTurn {
            emitted_bytes: written,
            complete: position == end,
        })
    }
}

/// Check the version and exact root-item extent within the admitted frame.
pub(crate) fn native_json_payload(bytes: &[u8]) -> Result<&[u8], ScalarLeafError> {
    novarocks_json_value_format::root_item(bytes).map_err(|_| ScalarLeafError::MalformedRecord)?;
    Ok(bytes)
}
fn timestamp_unit(unit: ScalarTimestampUnit) -> u8 {
    match unit {
        ScalarTimestampUnit::Microsecond => 1,
        ScalarTimestampUnit::Nanosecond => 2,
    }
}
fn opaque_kind(kind: ScalarOpaqueType) -> u8 {
    match kind {
        ScalarOpaqueType::Hll => 1,
        ScalarOpaqueType::Bitmap => 2,
        ScalarOpaqueType::Object => 3,
        ScalarOpaqueType::Percentile => 4,
    }
}

// Fixed four-limb arithmetic, including Decimal256 precision 76, requires no
// decimal string, BigInt heap or client-format conversion. At most 304 limb
// multiplies plus fixed-size copies/comparisons fit one existing work turn.
pub(crate) fn decimal_in_range(bytes: &[u8], precision: u8) -> bool {
    let mut value = [0u64; 4];
    for (index, limb_bytes) in bytes.as_chunks::<8>().0.iter().enumerate() {
        value[index] = u64::from_le_bytes(*limb_bytes);
    }
    let limbs = bytes.len() / 8;
    if bytes[bytes.len() - 1] & 0x80 != 0 {
        let mut carry = 1u128;
        for value in &mut value[..limbs] {
            let next = u128::from(!*value) + carry;
            *value = next as u64;
            carry = next >> 64;
        }
    }
    let mut ceiling = [1u64, 0, 0, 0];
    for _ in 0..precision {
        let mut carry = 0u128;
        for value in &mut ceiling {
            let next = u128::from(*value) * 10 + carry;
            *value = next as u64;
            carry = next >> 64;
        }
    }
    for index in (0..4).rev() {
        if value[index] != ceiling[index] {
            return value[index] < ceiling[index];
        }
    }
    false
}

impl novarocks_failure_contract::FailureProjection for ScalarLeafError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let (class, code) = match self {
            Self::Type => (FailureClass::Invariant, 1000),
            Self::Nullability => (FailureClass::Invariant, 1001),
            Self::ValueLimit => (FailureClass::Representation, 1002),
            Self::InvalidValue => (FailureClass::Data, 1003),
            Self::UnsupportedContainer => (FailureClass::Invariant, 1004),
            Self::MalformedRecord => (FailureClass::Data, 1005),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::RESULT, std::num::NonZeroU16::new(code).unwrap()),
        );
        if matches!(self, Self::ValueLimit) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::RESULT,
                    code: std::num::NonZeroU16::new(1000).unwrap(),
                },
                required: None,
                extent_limit: Some(crate::SCALAR_RECORD_MAX_BYTES as u64),
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
