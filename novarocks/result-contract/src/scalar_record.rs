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

//! Complete ScalarValueV1 records, including List, Map and Struct values.
//!
//! A record is the 24-byte `SCV1` header followed by its payload. Leaf records
//! keep the leaf layout of [`crate::ScalarLeafCursor`]. A container record's
//! payload is the value body of its frozen type, where every nested value is a
//! node: one presence byte (`0` value, `1` null) followed by, for a value, its
//! body. Bodies are type driven and carry no type tags:
//!
//! | type | body |
//! |---|---|
//! | fixed-width leaf | the leaf's little-endian bytes, as in a leaf record |
//! | String, Json, Binary, Variant, Opaque | `u32` LE length, then the bytes |
//! | List(element) | `u32` LE count, then `count` element nodes |
//! | Map(key, value) | `u32` LE count, then `count` (key node, value node) pairs |
//! | Struct(fields) | one node per field, in frozen field order |
//!
//! The complete record, including its header, every nested child and its
//! length/count/presence bytes, is limited to
//! [`ScalarProfileV1::RECORD_BYTES`] (64 KiB). Nesting depth is limited
//! by the frozen schema depth. Neither writer nor decoder allocates beyond the
//! caller's buffer (writer) or the decoded value tree (decoder). Every owned
//! allocation in the decoded tree is checked against the shared
//! [`ScalarProfileV1::CHILD_BYTES`] ceiling before it is requested, including
//! container capacity: a small wire record can otherwise expand to many
//! large owned nodes.

use crate::scalar_leaf::{
    ABSENT, CONTAINER_LIST, CONTAINER_MAP, CONTAINER_STRUCT, NULL, decimal_in_range,
    native_json_payload,
};
use crate::{
    BorrowedScalarLeaf, SCALAR_LEAF_HEADER_BYTES, ScalarField, ScalarLeafCursor, ScalarLeafError,
    ScalarLeafHeader, ScalarOpaqueType, ScalarProfileV1, ScalarSchema, ScalarTimestampUnit,
    ScalarValueType,
};

const PRESENT: u8 = 0;
const NULL_NODE: u8 = 1;

/// The largest complete record, including its header and nested metadata.
pub const SCALAR_RECORD_MAX_BYTES: usize = ScalarProfileV1::RECORD_BYTES;

/// Writes one container record into a caller-provided buffer whose capacity
/// already covers [`SCALAR_RECORD_MAX_BYTES`]. Every append checks the complete
/// record ceiling first, so the buffer never grows past its prepaid capacity.
/// The caller walks its source in the frozen type order; [`Self::finish`]
/// writes the header once the payload length is known.
pub struct ScalarRecordWriter<'a> {
    output: &'a mut Vec<u8>,
}

impl<'a> ScalarRecordWriter<'a> {
    pub fn new(schema: &ScalarSchema, output: &'a mut Vec<u8>) -> Result<Self, ScalarLeafError> {
        if !matches!(
            schema.field().value_type,
            ScalarValueType::List(_) | ScalarValueType::Map { .. } | ScalarValueType::Struct(_)
        ) {
            return Err(ScalarLeafError::Type);
        }
        if output.capacity() < SCALAR_RECORD_MAX_BYTES {
            return Err(ScalarLeafError::ValueLimit);
        }
        output.clear();
        output.extend_from_slice(&[0; SCALAR_LEAF_HEADER_BYTES]);
        Ok(Self { output })
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), ScalarLeafError> {
        let payload = self.output.len() - SCALAR_LEAF_HEADER_BYTES;
        if payload
            .checked_add(bytes.len())
            .is_none_or(|total| total > ScalarProfileV1::RECORD_PAYLOAD_BYTES)
        {
            return Err(ScalarLeafError::ValueLimit);
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }

    /// A nested value's presence byte; `null` must be legal for its field.
    pub fn presence(&mut self, field: &ScalarField, null: bool) -> Result<(), ScalarLeafError> {
        if null && !field.nullable && !matches!(field.value_type, ScalarValueType::Null) {
            return Err(ScalarLeafError::Nullability);
        }
        self.append(&[if null { NULL_NODE } else { PRESENT }])
    }

    /// The body of a non-null leaf, encoded exactly as a leaf record payload.
    pub fn leaf(
        &mut self,
        field: &ScalarField,
        value: BorrowedScalarLeaf<'_>,
    ) -> Result<(), ScalarLeafError> {
        if matches!(value, BorrowedScalarLeaf::NoRows | BorrowedScalarLeaf::Null) {
            return Err(ScalarLeafError::Type);
        }
        if matches!(
            field.value_type,
            ScalarValueType::List(_) | ScalarValueType::Map { .. } | ScalarValueType::Struct(_)
        ) {
            return Err(ScalarLeafError::UnsupportedContainer);
        }
        let cursor = ScalarLeafCursor::try_new_for_field(field, value)?;
        let mut payload = [0u8; 32];
        let len = cursor.encoded_len() - SCALAR_LEAF_HEADER_BYTES;
        if variable_leaf(&field.value_type) {
            self.append(
                &u32::try_from(len)
                    .map_err(|_| ScalarLeafError::ValueLimit)?
                    .to_le_bytes(),
            )?;
            let mut position = SCALAR_LEAF_HEADER_BYTES;
            while position < cursor.encoded_len() {
                let turn = cursor.copy_range(position, &mut payload)?;
                self.append(&payload[..turn.emitted_bytes])?;
                position += turn.emitted_bytes;
            }
            Ok(())
        } else {
            let turn = cursor.copy_range(SCALAR_LEAF_HEADER_BYTES, &mut payload)?;
            self.append(&payload[..turn.emitted_bytes])
        }
    }

    /// A List or Map element count.
    pub fn count(&mut self, count: usize) -> Result<(), ScalarLeafError> {
        let count = u32::try_from(count).map_err(|_| ScalarLeafError::ValueLimit)?;
        self.append(&count.to_le_bytes())
    }

    /// Write the header and return the complete record length.
    pub fn finish(self, schema: &ScalarSchema) -> Result<usize, ScalarLeafError> {
        let payload = self.output.len() - SCALAR_LEAF_HEADER_BYTES;
        let mut header = ScalarLeafCursor::try_new(schema, BorrowedScalarLeaf::NoRows)?.header();
        header[13] = 0;
        let total = u32::try_from(self.output.len()).map_err(|_| ScalarLeafError::ValueLimit)?;
        header[4..8].copy_from_slice(&total.to_le_bytes());
        header[8..12].copy_from_slice(
            &u32::try_from(payload)
                .map_err(|_| ScalarLeafError::ValueLimit)?
                .to_le_bytes(),
        );
        self.output[..SCALAR_LEAF_HEADER_BYTES].copy_from_slice(&header);
        Ok(self.output.len())
    }
}

fn variable_leaf(value_type: &ScalarValueType) -> bool {
    matches!(
        value_type,
        ScalarValueType::String
            | ScalarValueType::Json
            | ScalarValueType::Binary
            | ScalarValueType::Variant
            | ScalarValueType::Opaque(_)
    )
}

/// An owned decoded ScalarValueV1 value. Its node count never exceeds the
/// payload byte count of the record it came from.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
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
    String(String),
    Binary(Vec<u8>),
    Date(i32),
    TimeMicros(i64),
    Timestamp {
        ticks: i64,
        unit: ScalarTimestampUnit,
    },
    Json(Vec<u8>),
    Variant(Vec<u8>),
    Opaque {
        kind: ScalarOpaqueType,
        bytes: Vec<u8>,
    },
    List(Vec<ScalarValue>),
    Map(Vec<(ScalarValue, ScalarValue)>),
    Struct(Vec<ScalarValue>),
}

impl ScalarValue {
    fn from_leaf(
        leaf: BorrowedScalarLeaf<'_>,
        owned: &mut OwnedValueBudget,
    ) -> Result<Self, ScalarLeafError> {
        use BorrowedScalarLeaf as L;
        match leaf {
            L::String(value) => owned.reserve::<u8>(value.len())?,
            L::Json(value)
            | L::Binary(value)
            | L::Variant(value)
            | L::Opaque { bytes: value, .. } => owned.reserve::<u8>(value.len())?,
            _ => {}
        }
        Ok(match leaf {
            L::NoRows | L::Null => Self::Null,
            L::Boolean(value) => Self::Boolean(value),
            L::SignedInteger { bits, value } => Self::SignedInteger { bits, value },
            L::LargeInt(value) => Self::LargeInt(value),
            L::Float32(bits) => Self::Float32(bits),
            L::Float64(bits) => Self::Float64(bits),
            L::Decimal128 {
                coefficient,
                precision,
                scale,
            } => Self::Decimal128 {
                coefficient,
                precision,
                scale,
            },
            L::Decimal256 {
                coefficient_le,
                precision,
                scale,
            } => Self::Decimal256 {
                coefficient_le,
                precision,
                scale,
            },
            L::String(value) => Self::String(value.to_owned()),
            L::Binary(value) => Self::Binary(value.to_vec()),
            L::Date(days) => Self::Date(days),
            L::TimeMicros(ticks) => Self::TimeMicros(ticks),
            L::Timestamp { ticks, unit } => Self::Timestamp { ticks, unit },
            L::Json(value) => Self::Json(value.to_vec()),
            L::Variant(value) => Self::Variant(value.to_vec()),
            L::Opaque { kind, bytes } => Self::Opaque {
                kind,
                bytes: bytes.to_vec(),
            },
        })
    }
}

/// One decoded record: either the producer saw no row, or exactly one value
/// (which may be SQL NULL). The two are never conflated.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarRecord {
    NoRows,
    Value(ScalarValue),
}

impl ScalarRecord {
    /// Optional owned materialization under the existing CHILD_BYTES ceiling.
    /// Production consumption uses BorrowedScalarRecord instead, so compact
    /// records do not acquire a second semantic limit from owned tree size.
    pub fn decode_owned(schema: &ScalarSchema, record: &[u8]) -> Result<Self, ScalarLeafError> {
        let malformed = ScalarLeafError::MalformedRecord;
        let header = ScalarLeafHeader::decode(
            schema,
            record.get(..SCALAR_LEAF_HEADER_BYTES).ok_or(malformed)?,
        )?;
        if record.len() != header.record_bytes() {
            return Err(malformed);
        }
        let flags = record[13];
        if flags == NULL | ABSENT {
            return Ok(Self::NoRows);
        }
        if flags == NULL {
            return Ok(Self::Value(ScalarValue::Null));
        }
        match record[12] {
            CONTAINER_LIST | CONTAINER_MAP | CONTAINER_STRUCT => {
                let mut reader = Reader {
                    payload: &record[SCALAR_LEAF_HEADER_BYTES..],
                    position: 0,
                    owned: OwnedValueBudget::new(),
                };
                let value = reader.body(schema.field(), 1)?;
                if reader.position != reader.payload.len() {
                    return Err(malformed);
                }
                Ok(Self::Value(value))
            }
            _ => Ok(Self::Value(ScalarValue::from_leaf(
                BorrowedScalarLeaf::decode(schema, record)?,
                &mut OwnedValueBudget::new(),
            )?)),
        }
    }
}

/// One ceiling for the complete owned tree. Charges are never returned during
/// decoding: parent containers and earlier children remain live while later
/// children are constructed. This observes capacity; it grants no funding.
struct OwnedValueBudget {
    remaining: usize,
}

impl OwnedValueBudget {
    fn new() -> Self {
        Self {
            remaining: ScalarProfileV1::CHILD_BYTES,
        }
    }

    fn reserve<T>(&mut self, capacity: usize) -> Result<(), ScalarLeafError> {
        let bytes = capacity
            .checked_mul(size_of::<T>())
            .ok_or(ScalarLeafError::ValueLimit)?;
        self.remaining = self
            .remaining
            .checked_sub(bytes)
            .ok_or(ScalarLeafError::ValueLimit)?;
        Ok(())
    }
}

struct Reader<'a> {
    payload: &'a [u8],
    position: usize,
    owned: OwnedValueBudget,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], ScalarLeafError> {
        let end = self
            .position
            .checked_add(len)
            .filter(|end| *end <= self.payload.len())
            .ok_or(ScalarLeafError::MalformedRecord)?;
        let bytes = &self.payload[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn u32(&mut self) -> Result<usize, ScalarLeafError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("four bytes")) as usize)
    }

    fn node(&mut self, field: &ScalarField, depth: usize) -> Result<ScalarValue, ScalarLeafError> {
        match self.take(1)?[0] {
            PRESENT => self.body(field, depth),
            NULL_NODE if field.nullable || matches!(field.value_type, ScalarValueType::Null) => {
                Ok(ScalarValue::Null)
            }
            NULL_NODE => Err(ScalarLeafError::Nullability),
            _ => Err(ScalarLeafError::MalformedRecord),
        }
    }

    fn body(&mut self, field: &ScalarField, depth: usize) -> Result<ScalarValue, ScalarLeafError> {
        use ScalarValueType as T;
        if depth > crate::RootProfileV1::MAX_DEPTH {
            return Err(ScalarLeafError::MalformedRecord);
        }
        let malformed = ScalarLeafError::MalformedRecord;
        Ok(match &field.value_type {
            T::Null => return Err(malformed),
            T::Boolean => ScalarValue::Boolean(match self.take(1)?[0] {
                0 => false,
                1 => true,
                _ => return Err(malformed),
            }),
            T::SignedInteger(bits) => {
                let bytes = self.take(usize::from(bits / 8))?;
                let value = match bits {
                    8 => i64::from(i8::from_le_bytes(bytes.try_into().unwrap())),
                    16 => i64::from(i16::from_le_bytes(bytes.try_into().unwrap())),
                    32 => i64::from(i32::from_le_bytes(bytes.try_into().unwrap())),
                    64 => i64::from_le_bytes(bytes.try_into().unwrap()),
                    _ => return Err(malformed),
                };
                ScalarValue::SignedInteger { bits: *bits, value }
            }
            T::LargeInt => {
                ScalarValue::LargeInt(i128::from_le_bytes(self.take(16)?.try_into().unwrap()))
            }
            T::Float32 => {
                ScalarValue::Float32(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
            }
            T::Float64 => {
                ScalarValue::Float64(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
            }
            T::Decimal {
                bits: 128,
                precision,
                scale,
            } => {
                let bytes = self.take(16)?;
                if !decimal_in_range(bytes, *precision) {
                    return Err(ScalarLeafError::InvalidValue);
                }
                ScalarValue::Decimal128 {
                    coefficient: i128::from_le_bytes(bytes.try_into().unwrap()),
                    precision: *precision,
                    scale: *scale,
                }
            }
            T::Decimal {
                bits: 256,
                precision,
                scale,
            } => {
                let bytes = self.take(32)?;
                if !decimal_in_range(bytes, *precision) {
                    return Err(ScalarLeafError::InvalidValue);
                }
                ScalarValue::Decimal256 {
                    coefficient_le: bytes.try_into().unwrap(),
                    precision: *precision,
                    scale: *scale,
                }
            }
            T::Decimal { .. } => return Err(malformed),
            T::Date => ScalarValue::Date(i32::from_le_bytes(self.take(4)?.try_into().unwrap())),
            T::TimeMicros => {
                ScalarValue::TimeMicros(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
            }
            T::Timestamp { unit, .. } => ScalarValue::Timestamp {
                ticks: i64::from_le_bytes(self.take(8)?.try_into().unwrap()),
                unit: *unit,
            },
            T::String => {
                let len = self.u32()?;
                let text = std::str::from_utf8(self.take(len)?).map_err(|_| malformed)?;
                self.owned.reserve::<u8>(len)?;
                ScalarValue::String(text.to_owned())
            }
            T::Json | T::Binary | T::Variant | T::Opaque(_) => {
                let len = self.u32()?;
                let source = self.take(len)?;
                if matches!(field.value_type, T::Json) {
                    native_json_payload(source)?;
                }
                self.owned.reserve::<u8>(len)?;
                let bytes = source.to_vec();
                match &field.value_type {
                    T::Json => ScalarValue::Json(bytes),
                    T::Binary => ScalarValue::Binary(bytes),
                    T::Variant => ScalarValue::Variant(bytes),
                    T::Opaque(kind) => ScalarValue::Opaque { kind: *kind, bytes },
                    _ => unreachable!(),
                }
            }
            T::List(element) => {
                let count = self.u32()?;
                // Each element is at least its presence byte.
                if count > self.payload.len() - self.position {
                    return Err(malformed);
                }
                self.owned.reserve::<ScalarValue>(count)?;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    items.push(self.node(element, depth + 1)?);
                }
                ScalarValue::List(items)
            }
            T::Map { key, value } => {
                let count = self.u32()?;
                if count > (self.payload.len() - self.position) / 2 {
                    return Err(malformed);
                }
                self.owned.reserve::<(ScalarValue, ScalarValue)>(count)?;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = self.node(key, depth + 1)?;
                    let value = self.node(value, depth + 1)?;
                    entries.push((key, value));
                }
                ScalarValue::Map(entries)
            }
            T::Struct(fields) => {
                self.owned.reserve::<ScalarValue>(fields.len())?;
                let mut values = Vec::with_capacity(fields.len());
                for named in fields {
                    values.push(self.node(&named.field, depth + 1)?);
                }
                ScalarValue::Struct(values)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NamedScalarField;

    fn field(value_type: ScalarValueType, nullable: bool) -> ScalarField {
        ScalarField {
            nullable,
            value_type,
        }
    }

    fn list_of_strings() -> ScalarSchema {
        ScalarSchema::try_new(field(
            ScalarValueType::List(Box::new(field(ScalarValueType::String, true))),
            true,
        ))
        .unwrap()
    }

    fn buffer() -> Vec<u8> {
        Vec::with_capacity(SCALAR_RECORD_MAX_BYTES)
    }

    #[test]
    fn list_record_round_trips_with_null_elements() {
        let schema = list_of_strings();
        let ScalarValueType::List(element) = &schema.field().value_type else {
            unreachable!()
        };
        let mut output = buffer();
        let mut writer = ScalarRecordWriter::new(&schema, &mut output).unwrap();
        writer.count(3).unwrap();
        writer.presence(element, false).unwrap();
        writer
            .leaf(element, BorrowedScalarLeaf::String("a"))
            .unwrap();
        writer.presence(element, true).unwrap();
        writer.presence(element, false).unwrap();
        writer
            .leaf(element, BorrowedScalarLeaf::String(""))
            .unwrap();
        let len = writer.finish(&schema).unwrap();
        assert_eq!(len, output.len());
        assert_eq!(
            ScalarRecord::decode_owned(&schema, &output).unwrap(),
            ScalarRecord::Value(ScalarValue::List(vec![
                ScalarValue::String("a".into()),
                ScalarValue::Null,
                ScalarValue::String(String::new()),
            ]))
        );
    }

    #[test]
    fn map_and_struct_round_trip_with_fixed_leaves() {
        let map = ScalarSchema::try_new(field(
            ScalarValueType::Map {
                key: Box::new(field(ScalarValueType::SignedInteger(32), false)),
                value: Box::new(field(
                    ScalarValueType::Struct(vec![
                        NamedScalarField {
                            name: "flag".into(),
                            field: field(ScalarValueType::Boolean, false),
                        },
                        NamedScalarField {
                            name: "amount".into(),
                            field: field(
                                ScalarValueType::Decimal {
                                    bits: 128,
                                    precision: 10,
                                    scale: 2,
                                },
                                true,
                            ),
                        },
                    ]),
                    true,
                )),
            },
            false,
        ))
        .unwrap();
        let ScalarValueType::Map { key, value } = &map.field().value_type else {
            unreachable!()
        };
        let ScalarValueType::Struct(fields) = &value.value_type else {
            unreachable!()
        };
        let mut output = buffer();
        let mut writer = ScalarRecordWriter::new(&map, &mut output).unwrap();
        writer.count(1).unwrap();
        writer.presence(key, false).unwrap();
        writer
            .leaf(
                key,
                BorrowedScalarLeaf::SignedInteger {
                    bits: 32,
                    value: -7,
                },
            )
            .unwrap();
        writer.presence(value, false).unwrap();
        writer.presence(&fields[0].field, false).unwrap();
        writer
            .leaf(&fields[0].field, BorrowedScalarLeaf::Boolean(true))
            .unwrap();
        writer.presence(&fields[1].field, false).unwrap();
        writer
            .leaf(
                &fields[1].field,
                BorrowedScalarLeaf::Decimal128 {
                    coefficient: 12345,
                    precision: 10,
                    scale: 2,
                },
            )
            .unwrap();
        writer.finish(&map).unwrap();
        assert_eq!(
            ScalarRecord::decode_owned(&map, &output).unwrap(),
            ScalarRecord::Value(ScalarValue::Map(vec![(
                ScalarValue::SignedInteger {
                    bits: 32,
                    value: -7
                },
                ScalarValue::Struct(vec![
                    ScalarValue::Boolean(true),
                    ScalarValue::Decimal128 {
                        coefficient: 12345,
                        precision: 10,
                        scale: 2
                    },
                ]),
            )]))
        );
    }

    #[test]
    fn whole_record_is_limited_to_64_kib_including_header_and_children() {
        let schema = list_of_strings();
        let ScalarValueType::List(element) = &schema.field().value_type else {
            unreachable!()
        };
        // Header (24) + count (4) + presence (1) + length (4) + bytes == 64 KiB.
        let exact = "x".repeat(ScalarProfileV1::RECORD_PAYLOAD_BYTES - 9);
        let mut output = buffer();
        let capacity = output.capacity();
        let mut writer = ScalarRecordWriter::new(&schema, &mut output).unwrap();
        writer.count(1).unwrap();
        writer.presence(element, false).unwrap();
        writer
            .leaf(element, BorrowedScalarLeaf::String(&exact))
            .unwrap();
        writer.finish(&schema).unwrap();
        assert_eq!(output.len(), 64 * 1024);
        assert_eq!(SCALAR_RECORD_MAX_BYTES, 64 * 1024);
        assert_eq!(
            output.capacity(),
            capacity,
            "no growth past the prepaid buffer"
        );
        assert!(matches!(
            ScalarRecord::decode_owned(&schema, &output).unwrap(),
            ScalarRecord::Value(ScalarValue::List(_))
        ));

        let over = "x".repeat(ScalarProfileV1::RECORD_PAYLOAD_BYTES - 8);
        let mut output = buffer();
        let mut writer = ScalarRecordWriter::new(&schema, &mut output).unwrap();
        writer.count(1).unwrap();
        writer.presence(element, false).unwrap();
        assert_eq!(
            writer.leaf(element, BorrowedScalarLeaf::String(&over)),
            Err(ScalarLeafError::ValueLimit)
        );
    }

    #[test]
    fn container_no_rows_and_null_are_distinct_header_only_records() {
        let schema = list_of_strings();
        let no_rows = ScalarLeafCursor::try_new(&schema, BorrowedScalarLeaf::NoRows).unwrap();
        let null = ScalarLeafCursor::try_new(&schema, BorrowedScalarLeaf::Null).unwrap();
        let mut a = [0u8; SCALAR_LEAF_HEADER_BYTES];
        let mut b = [0u8; SCALAR_LEAF_HEADER_BYTES];
        no_rows.copy_range(0, &mut a).unwrap();
        null.copy_range(0, &mut b).unwrap();
        assert_eq!(
            ScalarRecord::decode_owned(&schema, &a).unwrap(),
            ScalarRecord::NoRows
        );
        assert_eq!(
            ScalarRecord::decode_owned(&schema, &b).unwrap(),
            ScalarRecord::Value(ScalarValue::Null)
        );
    }

    #[test]
    fn non_nullable_children_and_truncated_payloads_are_rejected() {
        let schema = ScalarSchema::try_new(field(
            ScalarValueType::List(Box::new(field(ScalarValueType::Float64, false))),
            false,
        ))
        .unwrap();
        let ScalarValueType::List(element) = &schema.field().value_type else {
            unreachable!()
        };
        let mut output = buffer();
        let mut writer = ScalarRecordWriter::new(&schema, &mut output).unwrap();
        writer.count(1).unwrap();
        assert_eq!(
            writer.presence(element, true),
            Err(ScalarLeafError::Nullability)
        );
        writer.presence(element, false).unwrap();
        writer
            .leaf(element, BorrowedScalarLeaf::Float64(1.5f64.to_bits()))
            .unwrap();
        writer.finish(&schema).unwrap();
        // A forged null node for a non-nullable element is refused on decode.
        let mut forged = output.clone();
        forged[SCALAR_LEAF_HEADER_BYTES + 4] = NULL_NODE;
        assert!(ScalarRecord::decode_owned(&schema, &forged).is_err());
        // A count larger than the remaining payload is refused before allocation.
        let mut inflated = output.clone();
        inflated[SCALAR_LEAF_HEADER_BYTES..SCALAR_LEAF_HEADER_BYTES + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            ScalarRecord::decode_owned(&schema, &inflated),
            Err(ScalarLeafError::MalformedRecord)
        );
        // Trailing bytes after the value body are malformed.
        let mut trailing = output.clone();
        trailing.push(0);
        assert!(ScalarRecord::decode_owned(&schema, &trailing).is_err());
    }

    #[test]
    fn leaf_records_decode_through_the_same_entry_point() {
        let schema = ScalarSchema::try_new(field(ScalarValueType::Date, true)).unwrap();
        let cursor = ScalarLeafCursor::try_new(&schema, BorrowedScalarLeaf::Date(19000)).unwrap();
        let mut record = vec![0u8; cursor.encoded_len()];
        cursor.copy_range(0, &mut record).unwrap();
        assert_eq!(
            ScalarRecord::decode_owned(&schema, &record).unwrap(),
            ScalarRecord::Value(ScalarValue::Date(19000))
        );
    }

    fn null_container_record(schema: &ScalarSchema, count: usize, nodes: usize) -> Vec<u8> {
        let mut output = buffer();
        let mut writer = ScalarRecordWriter::new(schema, &mut output).unwrap();
        writer.count(count).unwrap();
        for _ in 0..nodes {
            writer.append(&[NULL_NODE]).unwrap();
        }
        writer.finish(schema).unwrap();
        output
    }

    #[test]
    fn null_lists_and_maps_refuse_small_wire_records_that_expand_past_the_owned_limit() {
        let null = field(ScalarValueType::Null, true);
        for (value_type, element_bytes, nodes_per_element) in [
            (
                ScalarValueType::List(Box::new(null.clone())),
                size_of::<ScalarValue>(),
                1,
            ),
            (
                ScalarValueType::Map {
                    key: Box::new(null.clone()),
                    value: Box::new(null),
                },
                size_of::<(ScalarValue, ScalarValue)>(),
                2,
            ),
        ] {
            let schema = ScalarSchema::try_new(field(value_type, false)).unwrap();
            let maximum = ScalarProfileV1::CHILD_BYTES / element_bytes;
            let exact = null_container_record(&schema, maximum, maximum * nodes_per_element);
            assert!(ScalarRecord::decode_owned(&schema, &exact).is_ok());
            let over =
                null_container_record(&schema, maximum + 1, (maximum + 1) * nodes_per_element);
            assert!(over.len() < ScalarProfileV1::RECORD_PAYLOAD_BYTES);
            assert_eq!(
                ScalarRecord::decode_owned(&schema, &over),
                Err(ScalarLeafError::ValueLimit)
            );
        }
    }

    #[test]
    fn nested_lists_count_parent_capacity_and_live_siblings_together() {
        let null = field(ScalarValueType::Null, true);
        let inner = field(ScalarValueType::List(Box::new(null)), false);
        let schema =
            ScalarSchema::try_new(field(ScalarValueType::List(Box::new(inner.clone())), false))
                .unwrap();
        let per_list = ScalarProfileV1::CHILD_BYTES / size_of::<ScalarValue>() / 2;
        // Either inner list fits by itself. Both together exceed the ceiling
        // because the outer list's two slots are still live as well.
        let mut output = buffer();
        let mut writer = ScalarRecordWriter::new(&schema, &mut output).unwrap();
        writer.count(2).unwrap();
        for _ in 0..2 {
            writer.presence(&inner, false).unwrap();
            writer.count(per_list).unwrap();
            for _ in 0..per_list {
                writer.append(&[NULL_NODE]).unwrap();
            }
        }
        writer.finish(&schema).unwrap();
        assert!(output.len() < ScalarProfileV1::RECORD_PAYLOAD_BYTES);
        assert_eq!(
            ScalarRecord::decode_owned(&schema, &output),
            Err(ScalarLeafError::ValueLimit)
        );
    }

    #[test]
    fn repeated_structs_charge_each_owned_field_vector() {
        let fields = (0..4)
            .map(|index| NamedScalarField {
                name: format!("f{index}"),
                field: field(ScalarValueType::Null, true),
            })
            .collect();
        let element = field(ScalarValueType::Struct(fields), false);
        let schema = ScalarSchema::try_new(field(
            ScalarValueType::List(Box::new(element.clone())),
            false,
        ))
        .unwrap();
        // One outer slot plus four struct slots per element.
        let count = ScalarProfileV1::CHILD_BYTES / (5 * size_of::<ScalarValue>()) + 1;
        let mut output = buffer();
        let mut writer = ScalarRecordWriter::new(&schema, &mut output).unwrap();
        writer.count(count).unwrap();
        for _ in 0..count {
            writer.presence(&element, false).unwrap();
            writer.append(&[NULL_NODE; 4]).unwrap();
        }
        writer.finish(&schema).unwrap();
        assert!(output.len() < ScalarProfileV1::RECORD_PAYLOAD_BYTES);
        assert_eq!(
            ScalarRecord::decode_owned(&schema, &output),
            Err(ScalarLeafError::ValueLimit)
        );
    }

    fn owned_bytes(value: &ScalarValue) -> usize {
        match value {
            ScalarValue::String(value) => value.capacity(),
            ScalarValue::Json(value)
            | ScalarValue::Binary(value)
            | ScalarValue::Variant(value)
            | ScalarValue::Opaque { bytes: value, .. } => value.capacity(),
            ScalarValue::List(values) | ScalarValue::Struct(values) => {
                values.capacity() * size_of::<ScalarValue>()
                    + values.iter().map(owned_bytes).sum::<usize>()
            }
            ScalarValue::Map(values) => {
                values.capacity() * size_of::<(ScalarValue, ScalarValue)>()
                    + values
                        .iter()
                        .map(|(key, value)| owned_bytes(key) + owned_bytes(value))
                        .sum::<usize>()
            }
            _ => 0,
        }
    }

    #[test]
    fn variable_leaf_storage_and_container_capacity_share_the_exact_owned_boundary() {
        let count = ScalarProfileV1::CHILD_BYTES / size_of::<ScalarValue>() - 1;
        let exact_bytes = ScalarProfileV1::CHILD_BYTES - count * size_of::<ScalarValue>();
        for value_type in [
            ScalarValueType::String,
            ScalarValueType::Json,
            ScalarValueType::Binary,
            ScalarValueType::Variant,
            ScalarValueType::Opaque(ScalarOpaqueType::Hll),
        ] {
            let element = field(value_type.clone(), true);
            let schema = ScalarSchema::try_new(field(
                ScalarValueType::List(Box::new(element.clone())),
                false,
            ))
            .unwrap();
            for extra in [0, 1] {
                let text = "x".repeat(exact_bytes + extra);
                let mut json = vec![1, 0x44, u8::try_from(text.len() - 3).unwrap()];
                assert!(json[2] < 128);
                json.resize(text.len(), b'x');
                let leaf = match value_type {
                    ScalarValueType::String => BorrowedScalarLeaf::String(&text),
                    ScalarValueType::Json => BorrowedScalarLeaf::Json(&json),
                    ScalarValueType::Binary => BorrowedScalarLeaf::Binary(text.as_bytes()),
                    ScalarValueType::Variant => BorrowedScalarLeaf::Variant(text.as_bytes()),
                    ScalarValueType::Opaque(kind) => BorrowedScalarLeaf::Opaque {
                        kind,
                        bytes: text.as_bytes(),
                    },
                    _ => unreachable!(),
                };
                let mut output = buffer();
                let mut writer = ScalarRecordWriter::new(&schema, &mut output).unwrap();
                writer.count(count).unwrap();
                for _ in 1..count {
                    writer.presence(&element, true).unwrap();
                }
                writer.presence(&element, false).unwrap();
                writer.leaf(&element, leaf).unwrap();
                writer.finish(&schema).unwrap();
                assert!(output.len() < ScalarProfileV1::RECORD_PAYLOAD_BYTES);
                if extra == 0 {
                    let ScalarRecord::Value(value) =
                        ScalarRecord::decode_owned(&schema, &output).unwrap()
                    else {
                        unreachable!()
                    };
                    assert_eq!(owned_bytes(&value), ScalarProfileV1::CHILD_BYTES);
                } else {
                    assert_eq!(
                        ScalarRecord::decode_owned(&schema, &output),
                        Err(ScalarLeafError::ValueLimit)
                    );
                }
            }
        }
    }

    #[test]
    fn an_owned_capacity_overflow_is_refused_without_charging_it() {
        let mut owned = OwnedValueBudget::new();
        owned.reserve::<u8>(1).unwrap();
        assert_eq!(
            owned.reserve::<ScalarValue>(usize::MAX),
            Err(ScalarLeafError::ValueLimit)
        );
        assert_eq!(owned.remaining, ScalarProfileV1::CHILD_BYTES - 1);
        assert_eq!(
            owned.reserve::<u8>(ScalarProfileV1::CHILD_BYTES),
            Err(ScalarLeafError::ValueLimit)
        );
        assert_eq!(owned.remaining, ScalarProfileV1::CHILD_BYTES - 1);
    }
}
