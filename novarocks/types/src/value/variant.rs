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
use arrow_schema::DataType;
#[cfg(test)]
use arrow_schema::TimeUnit;
use base64::Engine;
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, Timelike, Utc};

#[derive(Debug, PartialEq)]
pub struct VariantFailure(Box<VariantFailureCause>);
#[derive(Debug, PartialEq)]
enum VariantFailureCause {
    Encoding(novarocks_failure_contract::BoundedDetail),
    Conversion(novarocks_failure_contract::BoundedDetail),
    Path(novarocks_failure_contract::BoundedDetail),
    Utf8(std::str::Utf8Error),
    PathIndex(std::num::ParseIntError),
    Size { required: u128 },
    NumericDate { days: i32 },
    NumericTimestamp { micros: i64, offset_seconds: i32 },
    PathStorage(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
}
impl VariantFailure {
    fn encoding(detail: impl std::fmt::Display) -> Self {
        Self(Box::new(VariantFailureCause::Encoding(
            novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        )))
    }
    fn conversion(detail: impl std::fmt::Display) -> Self {
        Self(Box::new(VariantFailureCause::Conversion(
            novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        )))
    }
    fn path(detail: impl std::fmt::Display) -> Self {
        Self(Box::new(VariantFailureCause::Path(
            novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        )))
    }
    fn utf8(cause: std::str::Utf8Error) -> Self {
        Self(Box::new(VariantFailureCause::Utf8(cause)))
    }
    fn path_index(cause: std::num::ParseIntError) -> Self {
        Self(Box::new(VariantFailureCause::PathIndex(cause)))
    }
    fn size(required: u128) -> Self {
        Self(Box::new(VariantFailureCause::Size { required }))
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for VariantFailure
{
    fn from(cause: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self(Box::new(VariantFailureCause::PathStorage(cause)))
    }
}
impl std::fmt::Display for VariantFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            VariantFailureCause::Encoding(detail)
            | VariantFailureCause::Conversion(detail)
            | VariantFailureCause::Path(detail) => f.write_str(detail.as_str()),
            VariantFailureCause::Utf8(cause) => write!(f, "Variant UTF-8 decoding failed: {cause}"),
            VariantFailureCause::PathIndex(cause) => {
                write!(f, "Invalid Variant array index: {cause}")
            }
            VariantFailureCause::Size { required } => write!(
                f,
                "Variant value requires {required} bytes, maximum {VARIANT_MAX_SIZE}"
            ),
            VariantFailureCause::NumericDate { days } => write!(
                f,
                "Variant date {days} is outside the supported calendar range"
            ),
            VariantFailureCause::NumericTimestamp {
                micros,
                offset_seconds,
            } => write!(
                f,
                "Variant timestamp {micros} microseconds with offset {offset_seconds} seconds is outside the supported calendar range"
            ),
            VariantFailureCause::PathStorage(cause) => write!(f, "Variant path storage: {cause}"),
        }
    }
}
impl std::error::Error for VariantFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.0.as_ref() {
            VariantFailureCause::Utf8(cause) => Some(cause),
            VariantFailureCause::PathIndex(cause) => Some(cause),
            VariantFailureCause::PathStorage(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for VariantFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        if let VariantFailureCause::PathStorage(cause) = self.0.as_ref() {
            return cause.failure_fact();
        }
        if matches!(
            self.0.as_ref(),
            VariantFailureCause::NumericDate { .. } | VariantFailureCause::NumericTimestamp { .. }
        ) {
            return novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact();
        }
        let code = match self.0.as_ref() {
            VariantFailureCause::Encoding(_) => 60,
            VariantFailureCause::Conversion(_) => 61,
            VariantFailureCause::Path(_) | VariantFailureCause::PathIndex(_) => 62,
            VariantFailureCause::Utf8(_) => 63,
            VariantFailureCause::Size { .. } => 64,
            VariantFailureCause::NumericDate { .. }
            | VariantFailureCause::NumericTimestamp { .. } => {
                unreachable!("numeric returned above")
            }
            VariantFailureCause::PathStorage(_) => unreachable!(),
        };
        let fact = FailureFact::new(
            if matches!(self.0.as_ref(), VariantFailureCause::Size { .. }) {
                FailureClass::Representation
            } else {
                FailureClass::Data
            },
            FailureReason::new(
                domain::TYPE_CONTRACT,
                std::num::NonZeroU16::new(code).unwrap(),
            ),
        );
        if let VariantFailureCause::Size { required } = self.0.as_ref() {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::TYPE_CONTRACT,
                    code: std::num::NonZeroU16::new(2).unwrap(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: Some(VARIANT_MAX_SIZE as u64),
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for VariantFailure {}

const VARIANT_MAX_SIZE: usize = 16 * 1024 * 1024;
const METADATA_MIN_SIZE: usize = 3;
const HEADER_SIZE: usize = 1;
const VERSION_MASK: u8 = 0b0000_1111;
const SORTED_STRINGS_MASK: u8 = 0b0001_0000;
const OFFSET_SIZE_MASK: u8 = 0b1100_0000;
const OFFSET_SIZE_SHIFT: u8 = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum BasicType {
    Primitive = 0,
    ShortString = 1,
    Object = 2,
    Array = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum VariantPrimitiveType {
    Null = 0,
    BooleanTrue = 1,
    BooleanFalse = 2,
    Int8 = 3,
    Int16 = 4,
    Int32 = 5,
    Int64 = 6,
    Double = 7,
    Decimal4 = 8,
    Decimal8 = 9,
    Decimal16 = 10,
    Date = 11,
    TimestampTz = 12,
    TimestampNtz = 13,
    Float = 14,
    Binary = 15,
    String = 16,
    TimeNtz = 17,
    TimestampTzNanos = 18,
    TimestampNtzNanos = 19,
    Uuid = 20,
}

#[derive(Clone, Debug)]
pub struct VariantMetadata {
    raw: Vec<u8>,
    dict_size: u32,
    offset_size: u8,
    sorted: bool,
}

impl VariantMetadata {
    pub fn empty() -> Self {
        // Version 1, dict_size=0, offset list contains one 0 offset.
        Self::from_bytes(vec![0x01, 0x00, 0x00]).expect("valid empty metadata")
    }

    pub fn from_bytes(raw: Vec<u8>) -> Result<Self, VariantFailure> {
        validate_metadata(&raw)?;
        let header = raw[0];
        let offset_size = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
        let sorted = (header & SORTED_STRINGS_MASK) != 0;
        let dict_size = read_le_u32(&raw[HEADER_SIZE..], offset_size)?;
        Ok(Self {
            raw,
            dict_size,
            offset_size,
            sorted,
        })
    }

    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    pub fn dict_size(&self) -> u32 {
        self.dict_size
    }

    pub fn get_key(&self, index: u32) -> Result<String, VariantFailure> {
        if index >= self.dict_size {
            return Err(VariantFailure::encoding(format_args!(
                "Variant index out of range: {} >= {}",
                index, self.dict_size
            )));
        }
        let offset_size = self.offset_size as usize;
        let offset_start = HEADER_SIZE + offset_size + (index as usize) * offset_size;
        let value_offset = read_le_u32(&self.raw[offset_start..], self.offset_size)? as usize;
        let value_next_offset =
            read_le_u32(&self.raw[offset_start + offset_size..], self.offset_size)? as usize;
        let key_len = value_next_offset.saturating_sub(value_offset);
        let string_start = HEADER_SIZE + offset_size * (self.dict_size as usize + 2) + value_offset;
        if string_start + key_len > self.raw.len() {
            return Err(VariantFailure::encoding("Variant string out of range"));
        }
        let bytes = &self.raw[string_start..string_start + key_len];
        let key = std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(VariantFailure::utf8)?;
        Ok(key)
    }

    pub fn get_index(&self, key: &str) -> Vec<u32> {
        let mut indexes = Vec::new();
        let dict_size = self.dict_size as usize;
        if self.sorted && dict_size > 32 {
            let mut left = 0usize;
            let mut right = dict_size.saturating_sub(1);
            while left <= right {
                let mid = left + (right - left) / 2;
                let key_mid = match self.get_key(mid as u32) {
                    Ok(v) => v,
                    Err(_) => return indexes,
                };
                let cmp = key_mid.as_str().cmp(key);
                if cmp == std::cmp::Ordering::Equal {
                    indexes.push(mid as u32);
                }
                if cmp == std::cmp::Ordering::Less {
                    left = mid + 1;
                } else {
                    if mid == 0 {
                        break;
                    }
                    right = mid - 1;
                }
            }
            return indexes;
        }

        let offset_size = self.offset_size as usize;
        let key_start_offset = HEADER_SIZE + offset_size * (dict_size + 2);
        let mut prev_key_offset = 0usize;
        for i in 0..dict_size {
            let offset_start = HEADER_SIZE + (i + 1) * offset_size;
            let next_key_offset =
                match read_le_u32(&self.raw[offset_start + offset_size..], self.offset_size) {
                    Ok(v) => v as usize,
                    Err(_) => return indexes,
                };
            let key_len = next_key_offset.saturating_sub(prev_key_offset);
            let start = key_start_offset + prev_key_offset;
            if start + key_len > self.raw.len() {
                return indexes;
            }
            if let Ok(field_key) = std::str::from_utf8(&self.raw[start..start + key_len])
                && field_key == key
            {
                indexes.push(i as u32);
            }
            prev_key_offset = next_key_offset;
        }
        indexes
    }
}

/// Split the engine-internal serialized variant form
/// `[size:u32 LE | metadata | value]` into zero-copy `(metadata, value)`
/// slices. Validation mirrors the original `VariantValue::from_serialized` logic.
pub fn split_serialized(data: &[u8]) -> Result<(&[u8], &[u8]), VariantFailure> {
    if data.len() < 4 {
        return Err(VariantFailure::encoding(
            "Invalid variant slice: too small to contain size header",
        ));
    }
    let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if size > VARIANT_MAX_SIZE {
        return Err(VariantFailure::size(size as u128));
    }
    if size > data.len().saturating_sub(4) {
        return Err(VariantFailure::encoding(format_args!(
            "Invalid variant size: {} exceeds available data: {}",
            size,
            data.len().saturating_sub(4)
        )));
    }
    let payload = &data[4..4 + size];
    let metadata = load_metadata(payload)?;
    if metadata.len() > payload.len() {
        return Err(VariantFailure::encoding(
            "Metadata size exceeds variant size",
        ));
    }
    let metadata_len = metadata.len();
    Ok((&payload[..metadata_len], &payload[metadata_len..]))
}

#[derive(Clone, Debug)]
pub struct VariantValue {
    metadata: VariantMetadata,
    value: Vec<u8>,
}

impl VariantValue {
    pub fn create(metadata: &[u8], value: &[u8]) -> Result<Self, VariantFailure> {
        if metadata.is_empty() {
            return Ok(Self::null_value());
        }
        validate_metadata(metadata)?;
        let required = metadata.len() as u128 + value.len() as u128;
        if required > VARIANT_MAX_SIZE as u128 {
            return Err(VariantFailure::size(required));
        }

        Ok(Self {
            metadata: VariantMetadata::from_bytes(metadata.to_vec())?,
            value: value.to_vec(),
        })
    }

    pub fn from_serialized(data: &[u8]) -> Result<Self, VariantFailure> {
        let (metadata, value) = split_serialized(data)?;
        Self::create(metadata, value)
    }

    pub fn null_value() -> Self {
        let metadata = VariantMetadata::empty();
        let value = vec![primitive_header(VariantPrimitiveType::Null)];
        Self { metadata, value }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let total = (self.metadata.raw().len() + self.value.len()) as u32;
        let mut out = Vec::with_capacity(4 + total as usize);
        out.extend_from_slice(&total.to_le_bytes());
        out.extend_from_slice(self.metadata.raw());
        out.extend_from_slice(&self.value);
        out
    }

    pub fn metadata(&self) -> &VariantMetadata {
        &self.metadata
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Renders timestamp values in the canonical Trino UTC representation.
    /// Rendering never reads a session or process-local time zone.
    pub fn to_json(&self) -> Result<String, VariantFailure> {
        let mut out = String::new();
        let variant = VariantRef {
            metadata: &self.metadata,
            value: &self.value,
        };
        variant_to_json(&variant, &mut out)?;
        Ok(out)
    }
}

struct VariantRef<'a> {
    metadata: &'a VariantMetadata,
    value: &'a [u8],
}

impl<'a> VariantRef<'a> {
    fn basic_type(&self) -> Result<BasicType, VariantFailure> {
        if self.value.is_empty() {
            return Err(VariantFailure::encoding("Variant value is empty"));
        }
        let bt = self.value[0] & 0b11;
        match bt {
            0 => Ok(BasicType::Primitive),
            1 => Ok(BasicType::ShortString),
            2 => Ok(BasicType::Object),
            3 => Ok(BasicType::Array),
            _ => Err(VariantFailure::encoding("Invalid variant basic type")),
        }
    }

    fn value_header(&self) -> u8 {
        self.value[0] >> 2
    }

    fn primitive_type(&self) -> Result<VariantPrimitiveType, VariantFailure> {
        let header = self.value_header();
        Ok(match header {
            0 => VariantPrimitiveType::Null,
            1 => VariantPrimitiveType::BooleanTrue,
            2 => VariantPrimitiveType::BooleanFalse,
            3 => VariantPrimitiveType::Int8,
            4 => VariantPrimitiveType::Int16,
            5 => VariantPrimitiveType::Int32,
            6 => VariantPrimitiveType::Int64,
            7 => VariantPrimitiveType::Double,
            8 => VariantPrimitiveType::Decimal4,
            9 => VariantPrimitiveType::Decimal8,
            10 => VariantPrimitiveType::Decimal16,
            11 => VariantPrimitiveType::Date,
            12 => VariantPrimitiveType::TimestampTz,
            13 => VariantPrimitiveType::TimestampNtz,
            14 => VariantPrimitiveType::Float,
            15 => VariantPrimitiveType::Binary,
            16 => VariantPrimitiveType::String,
            17 => VariantPrimitiveType::TimeNtz,
            18 => VariantPrimitiveType::TimestampTzNanos,
            19 => VariantPrimitiveType::TimestampNtzNanos,
            20 => VariantPrimitiveType::Uuid,
            _ => {
                return Err(VariantFailure::encoding(format_args!(
                    "Unsupported variant primitive type: {header}"
                )));
            }
        })
    }

    fn primitive_bytes(&self, size: usize) -> Result<&'a [u8], VariantFailure> {
        if self.value.len() < HEADER_SIZE + size {
            return Err(VariantFailure::encoding(format_args!(
                "Variant value too short: expected {} bytes, got {}",
                HEADER_SIZE + size,
                self.value.len()
            )));
        }
        Ok(&self.value[HEADER_SIZE..HEADER_SIZE + size])
    }

    fn get_bool(&self) -> Result<bool, VariantFailure> {
        match self.primitive_type()? {
            VariantPrimitiveType::BooleanTrue => Ok(true),
            VariantPrimitiveType::BooleanFalse => Ok(false),
            other => Err(VariantFailure::conversion(format_args!(
                "Not a boolean variant type: {:?}",
                other
            ))),
        }
    }

    fn get_int8(&self) -> Result<i8, VariantFailure> {
        let bytes = self.primitive_bytes(1)?;
        Ok(i8::from_le_bytes([bytes[0]]))
    }

    fn get_int16(&self) -> Result<i16, VariantFailure> {
        let bytes = self.primitive_bytes(2)?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn get_int32(&self) -> Result<i32, VariantFailure> {
        let bytes = self.primitive_bytes(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn get_int64(&self) -> Result<i64, VariantFailure> {
        let bytes = self.primitive_bytes(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn get_float32(&self) -> Result<f32, VariantFailure> {
        let bytes = self.primitive_bytes(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn get_float64(&self) -> Result<f64, VariantFailure> {
        let bytes = self.primitive_bytes(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn get_decimal4(&self) -> Result<(i32, u8), VariantFailure> {
        let bytes = self.primitive_bytes(1 + 4)?;
        let scale = bytes[0];
        let val = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        Ok((val, scale))
    }

    fn get_decimal8(&self) -> Result<(i64, u8), VariantFailure> {
        let bytes = self.primitive_bytes(1 + 8)?;
        let scale = bytes[0];
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[1..9]);
        Ok((i64::from_le_bytes(buf), scale))
    }

    fn get_decimal16(&self) -> Result<(i128, u8), VariantFailure> {
        let bytes = self.primitive_bytes(1 + 16)?;
        let scale = bytes[0];
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&bytes[1..17]);
        let value = i128::from_le_bytes(buf);
        Ok((value, scale))
    }

    fn get_date(&self) -> Result<i32, VariantFailure> {
        self.get_int32()
    }

    fn get_timestamp_micros(&self) -> Result<i64, VariantFailure> {
        self.get_int64()
    }

    fn get_timestamp_micros_ntz(&self) -> Result<i64, VariantFailure> {
        self.get_int64()
    }

    fn get_time_micros_ntz(&self) -> Result<i64, VariantFailure> {
        self.get_int64()
    }

    fn get_uuid(&self) -> Result<[u8; 16], VariantFailure> {
        let bytes = self.primitive_bytes(16)?;
        let mut out = [0u8; 16];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    fn get_string(&self) -> Result<&'a [u8], VariantFailure> {
        match self.basic_type()? {
            BasicType::ShortString => {
                let len = self.value_header() as usize;
                if self.value.len() < HEADER_SIZE + len {
                    return Err(VariantFailure::encoding("Invalid short string length"));
                }
                Ok(&self.value[HEADER_SIZE..HEADER_SIZE + len])
            }
            BasicType::Primitive => {
                if self.value.len() < HEADER_SIZE + 4 {
                    return Err(VariantFailure::encoding("Invalid string value: too short"));
                }
                let len = read_le_u32(&self.value[HEADER_SIZE..], 4)? as usize;
                let start = HEADER_SIZE + 4;
                if self.value.len() < start + len {
                    return Err(VariantFailure::encoding(
                        "Invalid string value: length out of bounds",
                    ));
                }
                Ok(&self.value[start..start + len])
            }
            other => Err(VariantFailure::conversion(format_args!(
                "Expected string, got {:?}",
                other
            ))),
        }
    }

    fn get_binary(&self) -> Result<&'a [u8], VariantFailure> {
        if self.value.len() < HEADER_SIZE + 4 {
            return Err(VariantFailure::encoding("Invalid binary value: too short"));
        }
        let len = read_le_u32(&self.value[HEADER_SIZE..], 4)? as usize;
        let start = HEADER_SIZE + 4;
        if self.value.len() < start + len {
            return Err(VariantFailure::encoding(
                "Invalid binary value: length out of bounds",
            ));
        }
        Ok(&self.value[start..start + len])
    }

    fn object_info(&self) -> Result<ObjectInfo, VariantFailure> {
        if self.basic_type()? != BasicType::Object {
            return Err(VariantFailure::encoding("Not an object variant"));
        }
        let header = self.value_header();
        let field_offset_size = (header & 0b11) + 1;
        let field_id_size = ((header >> 2) & 0b11) + 1;
        let is_large = ((header >> 4) & 0b1) == 1;
        let num_elements_size = if is_large { 4 } else { 1 };
        if self.value.len() < HEADER_SIZE + num_elements_size as usize {
            return Err(VariantFailure::encoding("Object value too short"));
        }
        let num_elements = read_le_u32(
            &self.value[HEADER_SIZE..HEADER_SIZE + num_elements_size as usize],
            num_elements_size,
        )?;
        let id_start_offset = HEADER_SIZE + num_elements_size as usize;
        let offset_start_offset = id_start_offset + num_elements as usize * field_id_size as usize;
        let data_start_offset =
            offset_start_offset + (num_elements as usize + 1) * field_offset_size as usize;
        if data_start_offset > self.value.len() {
            return Err(VariantFailure::encoding("Invalid object value offsets"));
        }
        Ok(ObjectInfo {
            num_elements,
            id_start_offset,
            id_size: field_id_size,
            offset_start_offset,
            offset_size: field_offset_size,
            data_start_offset,
        })
    }

    fn array_info(&self) -> Result<ArrayInfo, VariantFailure> {
        if self.basic_type()? != BasicType::Array {
            return Err(VariantFailure::encoding("Not an array variant"));
        }
        let header = self.value_header();
        let field_offset_size = (header & 0b11) + 1;
        let is_large = ((header >> 2) & 0b1) == 1;
        let num_elements_size = if is_large { 4 } else { 1 };
        if self.value.len() < HEADER_SIZE + num_elements_size as usize {
            return Err(VariantFailure::encoding("Array value too short"));
        }
        let num_elements = read_le_u32(
            &self.value[HEADER_SIZE..HEADER_SIZE + num_elements_size as usize],
            num_elements_size,
        )?;
        let offset_start_offset = HEADER_SIZE + num_elements_size as usize;
        let data_start_offset =
            offset_start_offset + (num_elements as usize + 1) * field_offset_size as usize;
        if data_start_offset > self.value.len() {
            return Err(VariantFailure::encoding("Invalid array value offsets"));
        }
        Ok(ArrayInfo {
            num_elements,
            offset_size: field_offset_size,
            offset_start_offset,
            data_start_offset,
        })
    }

    fn get_object_by_key(&self, key: &str) -> Result<Option<&'a [u8]>, VariantFailure> {
        let info = self.object_info()?;
        let dict_indexes = self.metadata.get_index(key);
        if dict_indexes.is_empty() {
            return Ok(None);
        }
        for dict_index in dict_indexes {
            let mut found_index: Option<u32> = None;
            for i in 0..info.num_elements {
                let pos = info.id_start_offset + i as usize * info.id_size as usize;
                let field_id = read_le_u32(&self.value[pos..], info.id_size)?;
                if field_id == dict_index {
                    found_index = Some(i);
                    break;
                }
            }
            if let Some(field_index) = found_index {
                let offset_pos =
                    info.offset_start_offset + field_index as usize * info.offset_size as usize;
                let offset = read_le_u32(&self.value[offset_pos..], info.offset_size)? as usize;
                let start = info.data_start_offset + offset;
                if start >= self.value.len() {
                    return Err(VariantFailure::encoding("Object offset out of bounds"));
                }
                return Ok(Some(&self.value[start..]));
            }
        }
        Ok(None)
    }

    fn get_element_at_index(&self, index: u32) -> Result<Option<&'a [u8]>, VariantFailure> {
        let info = self.array_info()?;
        if index >= info.num_elements {
            return Ok(None);
        }
        let offset_pos = info.offset_start_offset + index as usize * info.offset_size as usize;
        let offset = read_le_u32(&self.value[offset_pos..], info.offset_size)? as usize;
        let start = info.data_start_offset + offset;
        if start >= self.value.len() {
            return Err(VariantFailure::encoding("Array offset out of bounds"));
        }
        Ok(Some(&self.value[start..]))
    }
}

pub fn is_variant_null(value: &VariantValue) -> Result<bool, VariantFailure> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Ok(false);
    }
    Ok(matches!(
        variant.primitive_type()?,
        VariantPrimitiveType::Null
    ))
}

/// TYPEOF's established nullable row policy applies only to malformed Variant
/// encoding. Extent, allocation and other domain refusals retain their source.
pub fn variant_typeof_serialized_nullable(data: &[u8]) -> Result<Option<String>, VariantFailure> {
    nullable_typeof_result(VariantValue::from_serialized(data))
}

/// The JSON converter exposes metadata and value separately. Apply the same
/// Variant-owned nullable row policy without a second serialized allocation.
pub fn variant_typeof_parts_nullable(
    metadata: &[u8],
    value: &[u8],
) -> Result<Option<String>, VariantFailure> {
    nullable_typeof_result(VariantValue::create(metadata, value))
}

fn nullable_typeof_result(
    value: Result<VariantValue, VariantFailure>,
) -> Result<Option<String>, VariantFailure> {
    match value.and_then(|value| variant_typeof(&value)) {
        Ok(label) => Ok(Some(label)),
        Err(source) if matches!(source.0.as_ref(), VariantFailureCause::Encoding(_)) => Ok(None),
        Err(source) => Err(source),
    }
}

pub fn variant_typeof(value: &VariantValue) -> Result<String, VariantFailure> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    match variant.basic_type()? {
        BasicType::Primitive => Ok(match variant.primitive_type()? {
            VariantPrimitiveType::Null => "Null",
            VariantPrimitiveType::BooleanTrue => "Boolean(true)",
            VariantPrimitiveType::BooleanFalse => "Boolean(false)",
            VariantPrimitiveType::Int8 => "Int8",
            VariantPrimitiveType::Int16 => "Int16",
            VariantPrimitiveType::Int32 => "Int32",
            VariantPrimitiveType::Int64 => "Int64",
            VariantPrimitiveType::Double => "Double",
            VariantPrimitiveType::Decimal4 => "Decimal4",
            VariantPrimitiveType::Decimal8 => "Decimal8",
            VariantPrimitiveType::Decimal16 => "Decimal16",
            VariantPrimitiveType::Date => "Date",
            VariantPrimitiveType::TimestampTz => "TimestampTz",
            VariantPrimitiveType::TimestampNtz => "TimestampNtz",
            VariantPrimitiveType::Float => "Float",
            VariantPrimitiveType::Binary => "Binary",
            VariantPrimitiveType::String => "String",
            VariantPrimitiveType::TimeNtz => "TimeNtz",
            VariantPrimitiveType::TimestampTzNanos => "TimestampTzNanos",
            VariantPrimitiveType::TimestampNtzNanos => "TimestampNtzNanos",
            VariantPrimitiveType::Uuid => "Uuid",
        }
        .to_string()),
        BasicType::ShortString => Ok("ShortString".to_string()),
        BasicType::Object => Ok("Object".to_string()),
        BasicType::Array => Ok("Array".to_string()),
    }
}

pub fn variant_to_bool(value: &VariantValue) -> Result<bool, VariantFailure> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    match variant.basic_type()? {
        BasicType::Primitive => match variant.primitive_type()? {
            VariantPrimitiveType::BooleanTrue | VariantPrimitiveType::BooleanFalse => {
                Ok(variant.get_bool()?)
            }
            VariantPrimitiveType::String => parse_bool_from_bytes(variant.get_string()?),
            other => Err(VariantFailure::conversion(format_args!(
                "Cannot cast variant {:?} to BOOLEAN",
                other
            ))),
        },
        BasicType::ShortString => parse_bool_from_bytes(variant.get_string()?),
        other => Err(VariantFailure::conversion(format_args!(
            "Cannot cast variant {:?} to BOOLEAN",
            other
        ))),
    }
}

pub fn variant_to_i64(value: &VariantValue) -> Result<i64, VariantFailure> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err(VariantFailure::conversion(
            "Cannot cast non-primitive variant to BIGINT",
        ));
    }
    match variant.primitive_type()? {
        VariantPrimitiveType::BooleanTrue | VariantPrimitiveType::BooleanFalse => {
            Ok(if variant.get_bool()? { 1 } else { 0 })
        }
        VariantPrimitiveType::Int8 => Ok(variant.get_int8()? as i64),
        VariantPrimitiveType::Int16 => Ok(variant.get_int16()? as i64),
        VariantPrimitiveType::Int32 => Ok(variant.get_int32()? as i64),
        VariantPrimitiveType::Int64 => Ok(variant.get_int64()?),
        VariantPrimitiveType::Float => Ok(variant.get_float32()? as i64),
        VariantPrimitiveType::Double => Ok(variant.get_float64()? as i64),
        VariantPrimitiveType::Decimal4 => {
            let (val, scale) = variant.get_decimal4()?;
            Ok(decimal_to_i64(val as i128, scale))
        }
        VariantPrimitiveType::Decimal8 => {
            let (val, scale) = variant.get_decimal8()?;
            Ok(decimal_to_i64(val as i128, scale))
        }
        VariantPrimitiveType::Decimal16 => {
            let (val, scale) = variant.get_decimal16()?;
            Ok(decimal_to_i64(val, scale))
        }
        other => Err(VariantFailure::conversion(format_args!(
            "Cannot cast variant {:?} to BIGINT",
            other
        ))),
    }
}

pub fn variant_to_f64(value: &VariantValue) -> Result<f64, VariantFailure> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err(VariantFailure::conversion(
            "Cannot cast non-primitive variant to DOUBLE",
        ));
    }
    match variant.primitive_type()? {
        VariantPrimitiveType::BooleanTrue | VariantPrimitiveType::BooleanFalse => {
            Ok(if variant.get_bool()? { 1.0 } else { 0.0 })
        }
        VariantPrimitiveType::Int8 => Ok(variant.get_int8()? as f64),
        VariantPrimitiveType::Int16 => Ok(variant.get_int16()? as f64),
        VariantPrimitiveType::Int32 => Ok(variant.get_int32()? as f64),
        VariantPrimitiveType::Int64 => Ok(variant.get_int64()? as f64),
        VariantPrimitiveType::Float => Ok(variant.get_float32()? as f64),
        VariantPrimitiveType::Double => Ok(variant.get_float64()?),
        VariantPrimitiveType::Decimal4 => {
            let (val, scale) = variant.get_decimal4()?;
            Ok(decimal_to_f64(val as i128, scale))
        }
        VariantPrimitiveType::Decimal8 => {
            let (val, scale) = variant.get_decimal8()?;
            Ok(decimal_to_f64(val as i128, scale))
        }
        VariantPrimitiveType::Decimal16 => {
            let (val, scale) = variant.get_decimal16()?;
            Ok(decimal_to_f64(val, scale))
        }
        other => Err(VariantFailure::conversion(format_args!(
            "Cannot cast variant {:?} to DOUBLE",
            other
        ))),
    }
}

pub fn variant_to_string(value: &VariantValue) -> Result<String, VariantFailure> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    match variant.basic_type()? {
        BasicType::ShortString => bytes_to_string(variant.get_string()?),
        BasicType::Primitive => match variant.primitive_type()? {
            VariantPrimitiveType::String => bytes_to_string(variant.get_string()?),
            VariantPrimitiveType::TimestampTz
            | VariantPrimitiveType::TimestampNtz
            | VariantPrimitiveType::TimestampTzNanos
            | VariantPrimitiveType::TimestampNtzNanos => {
                let primitive = variant.primitive_type()?;
                format_variant_timestamp(
                    variant.get_int64()?,
                    matches!(
                        primitive,
                        VariantPrimitiveType::TimestampTzNanos
                            | VariantPrimitiveType::TimestampNtzNanos
                    ),
                    matches!(
                        primitive,
                        VariantPrimitiveType::TimestampTz | VariantPrimitiveType::TimestampTzNanos
                    ),
                )
            }
            _ => value.to_json(),
        },
        _ => value.to_json(),
    }
}

pub fn variant_to_date_days(value: &VariantValue, tz: FixedOffset) -> Result<i32, VariantFailure> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err(VariantFailure::conversion(
            "Cannot cast non-primitive variant to DATE",
        ));
    }
    match variant.primitive_type()? {
        VariantPrimitiveType::Date => variant.get_date(),
        VariantPrimitiveType::TimestampTz => {
            let micros = variant.get_timestamp_micros()?;
            let date = timestamp_tz_to_local_naive(micros, tz)?.date();
            Ok(date.num_days_from_ce() - 719163)
        }
        other => Err(VariantFailure::conversion(format_args!(
            "Cannot cast variant {:?} to DATE",
            other
        ))),
    }
}

pub fn variant_to_datetime_micros(
    value: &VariantValue,
    tz: FixedOffset,
) -> Result<i64, VariantFailure> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err(VariantFailure::conversion(
            "Cannot cast non-primitive variant to DATETIME",
        ));
    }
    match variant.primitive_type()? {
        VariantPrimitiveType::TimestampNtz => variant.get_timestamp_micros_ntz(),
        VariantPrimitiveType::TimestampTz => {
            let micros = variant.get_timestamp_micros()?;
            let naive = timestamp_tz_to_local_naive(micros, tz)?;
            let utc = naive.and_utc();
            Ok(utc.timestamp() * 1_000_000 + utc.timestamp_subsec_micros() as i64)
        }
        other => Err(VariantFailure::conversion(format_args!(
            "Cannot cast variant {:?} to DATETIME",
            other
        ))),
    }
}

pub fn variant_to_time_micros(
    value: &VariantValue,
    tz: FixedOffset,
) -> Result<i64, VariantFailure> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err(VariantFailure::conversion(
            "Cannot cast non-primitive variant to TIME",
        ));
    }
    match variant.primitive_type()? {
        VariantPrimitiveType::TimeNtz => variant.get_time_micros_ntz(),
        VariantPrimitiveType::TimestampTz => {
            let micros = variant.get_timestamp_micros()?;
            let naive = timestamp_tz_to_local_naive(micros, tz)?;
            let time = naive.time();
            Ok(time.num_seconds_from_midnight() as i64 * 1_000_000
                + (time.nanosecond() / 1000) as i64)
        }
        other => Err(VariantFailure::conversion(format_args!(
            "Cannot cast variant {:?} to TIME",
            other
        ))),
    }
}

#[derive(Clone, Copy, Debug)]
struct ObjectInfo {
    num_elements: u32,
    id_start_offset: usize,
    id_size: u8,
    offset_start_offset: usize,
    offset_size: u8,
    data_start_offset: usize,
}

#[derive(Clone, Copy, Debug)]
struct ArrayInfo {
    num_elements: u32,
    offset_size: u8,
    offset_start_offset: usize,
    data_start_offset: usize,
}

#[derive(Clone, Debug)]
pub enum VariantPathSegment {
    ObjectKey(String),
    ArrayIndex(u32),
}

#[derive(Clone, Debug)]
pub struct VariantPath {
    pub segments: Vec<VariantPathSegment>,
}

/// A concrete path constructor operation, observed before it executes.
#[derive(Clone, Copy, Debug)]
pub enum VariantPathConstructionStep {
    Request(std::alloc::Layout),
    Work(usize),
}

/// An independently held parsed path. Borrowed segments cannot transfer an
/// unguarded owned key/vector; its temporary owner ends with this value.
#[derive(Debug)]
pub struct ObservedVariantPath {
    path: VariantPath,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl ObservedVariantPath {
    pub fn segments(&self) -> &[VariantPathSegment] {
        &self.path.segments
    }
}

type PathObserver<'a, E> = Option<&'a mut dyn FnMut(VariantPathConstructionStep) -> Result<(), E>>;
fn observe_path<E>(
    observe: &mut PathObserver<'_, E>,
    step: VariantPathConstructionStep,
) -> Result<(), E> {
    if let Some(observe) = observe {
        observe(step)?;
    }
    Ok(())
}
fn request_path_array<
    T,
    E: From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
>(
    count: usize,
    observe: &mut PathObserver<'_, E>,
) -> Result<(), E> {
    let layout = std::alloc::Layout::array::<T>(count).map_err(|_| {
        E::from(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)
    })?;
    if layout.size() != 0 {
        observe_path(observe, VariantPathConstructionStep::Request(layout))?;
    }
    Ok(())
}
fn push_path<T, E: From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>>(
    values: &mut Vec<T>,
    value: T,
    observe: &mut PathObserver<'_, E>,
) -> Result<(), E> {
    let facts =
        novarocks_type_contract::owned_resources::vec::push_growth(values).map_err(|error| {
            use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
            use novarocks_type_contract::{CompileControlError, ControlResourceError};
            E::from(match error {
                ControlResourceError::SourceModel(_) => LayoutResourceError::SourceModel,
                ControlResourceError::Control(CompileControlError::ResourceExhausted) => {
                    LayoutResourceError::Arithmetic
                }
                // push_growth is a pure checked geometry author, not a control
                // checkpoint; any caller-control source here violates its pure
                // geometry contract and is rejected as a source model error.
                ControlResourceError::Control(
                    CompileControlError::Cancelled
                    | CompileControlError::DeadlineExceeded
                    | CompileControlError::Source(_),
                ) => LayoutResourceError::SourceModel,
            })
        })?;
    if let Some(layout) = facts.requested_backing {
        observe_path(observe, VariantPathConstructionStep::Request(layout))?;
    }
    values.push(value);
    Ok(())
}

pub fn parse_variant_path(path: &str) -> Result<VariantPath, VariantFailure> {
    parse_variant_path_core::<VariantFailure>(path, &mut None)
}

pub fn parse_variant_path_with_storage_observed<E>(
    path: &str,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
    mut observe: impl FnMut(VariantPathConstructionStep) -> Result<(), E>,
) -> Result<ObservedVariantPath, E>
where
    E: From<VariantFailure>
        + From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
{
    let path = parse_variant_path_core(path, &mut Some(&mut observe))?;
    Ok(ObservedVariantPath {
        path,
        _storage: storage,
    })
}

fn parse_variant_path_core<E>(
    path: &str,
    observe: &mut PathObserver<'_, E>,
) -> Result<VariantPath, E>
where
    E: From<VariantFailure>
        + From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
{
    let normalized = normalize_variant_path(path, observe)?;
    PathParser::new(&normalized, observe).parse()
}

/// Map a `variant_get` type-string literal to its SQL Arrow type.
pub use novarocks_type_contract::variant_get_target_type;

fn normalize_variant_path<E>(path: &str, observe: &mut PathObserver<'_, E>) -> Result<String, E>
where
    E: From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
{
    observe_path(observe, VariantPathConstructionStep::Work(path.len()))?;
    let trimmed = path.trim();
    observe_path(observe, VariantPathConstructionStep::Work(trimmed.len()))?;
    let (prefix, suffix) = if trimmed.is_empty() {
        ("$", "")
    } else if trimmed.starts_with('$') {
        ("", "")
    } else if trimmed.starts_with('[') || trimmed.starts_with('.') {
        ("$", "")
    } else if trimmed.chars().all(|c| c.is_ascii_digit()) {
        ("$[", "]")
    } else {
        ("$.", "")
    };
    let len = trimmed
        .len()
        .checked_add(prefix.len())
        .and_then(|n| n.checked_add(suffix.len()))
        .ok_or_else(|| {
            E::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
    request_path_array::<u8, E>(len, observe)?;
    let mut normalized = String::with_capacity(len);
    normalized.push_str(prefix);
    normalized.push_str(trimmed);
    normalized.push_str(suffix);
    Ok(normalized)
}

/// Strict navigation requires a matching value. It shares the same physical
/// decoder with nullable navigation and never repairs malformed storage.
pub fn variant_query(
    value: &VariantValue,
    path: &VariantPath,
) -> Result<VariantValue, VariantFailure> {
    variant_query_nullable(value, path)?
        .ok_or_else(|| VariantFailure::conversion("Variant path has no matching value"))
}

/// Missing keys/indices and a path through another value kind are ordinary
/// absence. Encoded offsets, metadata, UTF-8 and representation failures remain
/// failures; no semantic class is used as a blanket NULL policy.
pub fn variant_query_nullable(
    value: &VariantValue,
    path: &VariantPath,
) -> Result<Option<VariantValue>, VariantFailure> {
    let mut current_value = value.value();
    for segment in &path.segments {
        let current = VariantRef {
            metadata: value.metadata(),
            value: current_value,
        };
        let next = match segment {
            VariantPathSegment::ObjectKey(key) => {
                if !matches!(current.basic_type()?, BasicType::Object) {
                    return Ok(None);
                }
                current.get_object_by_key(key)?
            }
            VariantPathSegment::ArrayIndex(index) => {
                if !matches!(current.basic_type()?, BasicType::Array) {
                    return Ok(None);
                }
                current.get_element_at_index(*index)?
            }
        };
        let Some(next) = next else { return Ok(None) };
        current_value = next;
    }
    VariantValue::create(value.metadata.raw(), current_value).map(Some)
}

/// Only this domain's declared conversion rejection is nullable. This is an
/// operation contract, not permission to suppress arbitrary Data failures.
fn nullable_conversion<T>(result: Result<T, VariantFailure>) -> Result<Option<T>, VariantFailure> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error)
            if matches!(
                error.0.as_ref(),
                VariantFailureCause::Conversion(_)
                    | VariantFailureCause::NumericDate { .. }
                    | VariantFailureCause::NumericTimestamp { .. }
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
/// Tolerant path syntax keeps resource/layout failures on the error channel.
pub fn parse_variant_path_nullable(path: &str) -> Result<Option<VariantPath>, VariantFailure> {
    match parse_variant_path(path) {
        Ok(path) => Ok(Some(path)),
        Err(error)
            if matches!(
                error.0.as_ref(),
                VariantFailureCause::Path(_) | VariantFailureCause::PathIndex(_)
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

struct PathParser<'a, 'observer, 'port, E> {
    input: &'a [u8],
    pos: usize,
    observe: &'observer mut PathObserver<'port, E>,
}

impl<'a, 'observer, 'port, E> PathParser<'a, 'observer, 'port, E>
where
    E: From<VariantFailure>
        + From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
{
    fn new(input: &'a str, observe: &'observer mut PathObserver<'port, E>) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
            observe,
        }
    }
    fn parse(&mut self) -> Result<VariantPath, E> {
        if !self.consume_char(b'$')? {
            return Err(E::from(VariantFailure::path("Path must start with '$'")));
        }
        let mut segments = Vec::new();
        while !self.is_end() {
            observe_path(self.observe, VariantPathConstructionStep::Work(1))?;
            if self.peek_char() == b'.' {
                self.advance()?;
                let key = self.parse_unquoted_key()?;
                if key.is_empty() {
                    return Err(E::from(VariantFailure::path("Expected key after '.'")));
                }
                push_path(
                    &mut segments,
                    VariantPathSegment::ObjectKey(key),
                    self.observe,
                )?;
            } else if self.peek_char() == b'[' {
                if let Some(seg) = self.parse_bracket_segment()? {
                    push_path(&mut segments, seg, self.observe)?;
                }
            } else {
                return Err(E::from(VariantFailure::path(format_args!(
                    "Unexpected character '{}' at position {}",
                    self.peek_char() as char,
                    self.pos
                ))));
            }
        }
        Ok(VariantPath { segments })
    }
    fn parse_bracket_segment(&mut self) -> Result<Option<VariantPathSegment>, E> {
        self.expect_char(b'[')?;
        if self.is_end() {
            return Err(E::from(VariantFailure::path("Unexpected end of path")));
        }
        let ch = self.peek_char();
        if ch == b'\'' || ch == b'"' {
            let quote = self.advance()?;
            let key = self.parse_quoted_string(quote)?;
            self.expect_char(quote)?;
            self.expect_char(b']')?;
            return Ok(Some(VariantPathSegment::ObjectKey(key)));
        }
        let number = self.parse_number()?;
        if number.is_empty() {
            return Err(E::from(VariantFailure::path("Expected array index")));
        }
        self.expect_char(b']')?;
        observe_path(
            self.observe,
            VariantPathConstructionStep::Work(number.len()),
        )?;
        let index = number
            .parse::<u32>()
            .map_err(|cause| E::from(VariantFailure::path_index(cause)))?;
        Ok(Some(VariantPathSegment::ArrayIndex(index)))
    }
    fn parse_unquoted_key(&mut self) -> Result<String, E> {
        let start = self.pos;
        while !self.is_end() {
            let c = self.peek_char();
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.advance()?;
            } else {
                break;
            }
        }
        let bytes = &self.input[start..self.pos];
        request_path_array::<u8, E>(bytes.len(), self.observe)?;
        observe_path(self.observe, VariantPathConstructionStep::Work(bytes.len()))?;
        Ok(std::str::from_utf8(bytes)
            .expect("unquoted path keys are ASCII")
            .to_owned())
    }
    fn parse_number(&mut self) -> Result<&'a str, E> {
        let start = self.pos;
        while !self.is_end() && self.peek_char().is_ascii_digit() {
            self.advance()?;
        }
        Ok(std::str::from_utf8(&self.input[start..self.pos]).expect("path indices are ASCII"))
    }
    fn parse_quoted_string(&mut self, quote: u8) -> Result<String, E> {
        let mut out = Vec::new();
        while !self.is_end() && self.peek_char() != quote {
            let c = self.advance()?;
            let byte = if c == b'\\' && !self.is_end() {
                match self.advance()? {
                    b'"' => b'"',
                    b'\'' => b'\'',
                    b'\\' => b'\\',
                    b'n' => b'\n',
                    b't' => b'\t',
                    b'r' => b'\r',
                    other => other,
                }
            } else {
                c
            };
            push_path(&mut out, byte, self.observe)?;
        }
        observe_path(self.observe, VariantPathConstructionStep::Work(out.len()))?;
        String::from_utf8(out).map_err(|cause| E::from(VariantFailure::utf8(cause.utf8_error())))
    }
    fn peek_char(&self) -> u8 {
        self.input.get(self.pos).copied().unwrap_or(0)
    }
    fn advance(&mut self) -> Result<u8, E> {
        observe_path(self.observe, VariantPathConstructionStep::Work(1))?;
        let ch = self.peek_char();
        self.pos = self.pos.saturating_add(1);
        Ok(ch)
    }
    fn consume_char(&mut self, expected: u8) -> Result<bool, E> {
        if self.peek_char() == expected {
            self.advance()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    fn expect_char(&mut self, expected: u8) -> Result<(), E> {
        if self.consume_char(expected)? {
            Ok(())
        } else {
            Err(E::from(VariantFailure::path(format_args!(
                "Expected '{}' at position {}",
                expected as char, self.pos
            ))))
        }
    }
    fn is_end(&self) -> bool {
        self.pos >= self.input.len()
    }
}

fn validate_metadata(metadata: &[u8]) -> Result<(), VariantFailure> {
    if metadata.len() < METADATA_MIN_SIZE {
        return Err(VariantFailure::encoding("Variant metadata is too short"));
    }
    let header = metadata[0];
    let version = header & VERSION_MASK;
    if version != 1 {
        return Err(VariantFailure::encoding(format_args!(
            "Unsupported variant version: {}",
            version
        )));
    }
    Ok(())
}

fn load_metadata(variant: &[u8]) -> Result<&[u8], VariantFailure> {
    if variant.is_empty() {
        return Err(VariantFailure::encoding("Variant is empty"));
    }
    if variant.len() > VARIANT_MAX_SIZE {
        return Err(VariantFailure::size(variant.len() as u128));
    }

    let header = variant[0];
    let version = header & VERSION_MASK;
    if version != 1 {
        return Err(VariantFailure::encoding(format_args!(
            "Unsupported variant version: {}",
            version
        )));
    }
    let offset_size = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    if !(1..=4).contains(&offset_size) {
        return Err(VariantFailure::encoding(format_args!(
            "Invalid offset size in variant metadata: {}",
            offset_size
        )));
    }
    if variant.len() < HEADER_SIZE + offset_size as usize {
        return Err(VariantFailure::encoding(
            "Variant too short to contain dict_size",
        ));
    }
    let dict_size = read_le_u32(&variant[HEADER_SIZE..], offset_size)? as usize;
    let offset_list_offset = HEADER_SIZE + offset_size as usize;
    let required_offset_list_size = (1 + dict_size) * offset_size as usize;
    let data_offset = offset_list_offset + required_offset_list_size;
    let last_offset_pos = offset_list_offset + dict_size * offset_size as usize;
    if last_offset_pos + offset_size as usize > variant.len() {
        return Err(VariantFailure::encoding(
            "Variant too short to contain all offsets",
        ));
    }
    let last_data_size = read_le_u32(&variant[last_offset_pos..], offset_size)? as usize;
    let end_offset = data_offset + last_data_size;
    if end_offset > variant.len() {
        return Err(VariantFailure::encoding(format_args!(
            "Variant metadata end offset exceeds variant size: {} > {}",
            end_offset,
            variant.len()
        )));
    }
    Ok(&variant[..end_offset])
}

fn primitive_header(primitive: VariantPrimitiveType) -> u8 {
    (primitive as u8) << 2
}

fn read_le_u32(data: &[u8], size: u8) -> Result<u32, VariantFailure> {
    if size == 0 || size > 4 {
        return Err(VariantFailure::encoding("Invalid little-endian size"));
    }
    if data.len() < size as usize {
        return Err(VariantFailure::encoding("Not enough bytes to read value"));
    }
    let mut out: u32 = 0;
    for (i, byte) in data.iter().copied().enumerate().take(size as usize) {
        out |= (byte as u32) << (8 * i);
    }
    Ok(out)
}

fn variant_to_json(variant: &VariantRef<'_>, out: &mut String) -> Result<(), VariantFailure> {
    match variant.basic_type()? {
        BasicType::Primitive => match variant.primitive_type()? {
            VariantPrimitiveType::Null => out.push_str("null"),
            VariantPrimitiveType::BooleanTrue | VariantPrimitiveType::BooleanFalse => {
                out.push_str(if variant.get_bool()? { "true" } else { "false" });
            }
            VariantPrimitiveType::Int8 => out.push_str(&variant.get_int8()?.to_string()),
            VariantPrimitiveType::Int16 => out.push_str(&variant.get_int16()?.to_string()),
            VariantPrimitiveType::Int32 => out.push_str(&variant.get_int32()?.to_string()),
            VariantPrimitiveType::Int64 => out.push_str(&variant.get_int64()?.to_string()),
            VariantPrimitiveType::Float => {
                let v = variant.get_float32()? as f64;
                out.push_str(&float_to_json_string(v));
            }
            VariantPrimitiveType::Double => {
                let v = variant.get_float64()?;
                out.push_str(&float_to_json_string(v));
            }
            VariantPrimitiveType::Decimal4 => {
                let (val, scale) = variant.get_decimal4()?;
                out.push_str(&remove_trailing_zeros(&decimal_to_string(
                    val as i128,
                    scale,
                )));
            }
            VariantPrimitiveType::Decimal8 => {
                let (val, scale) = variant.get_decimal8()?;
                out.push_str(&remove_trailing_zeros(&decimal_to_string(
                    val as i128,
                    scale,
                )));
            }
            VariantPrimitiveType::Decimal16 => {
                let (val, scale) = variant.get_decimal16()?;
                out.push_str(&remove_trailing_zeros(&decimal_to_string(val, scale)));
            }
            VariantPrimitiveType::String => {
                let s = variant.get_string()?;
                out.push('"');
                out.push_str(&escape_json_string(s));
                out.push('"');
            }
            VariantPrimitiveType::Binary => {
                let b = variant.get_binary()?;
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                out.push('"');
                out.push_str(&escape_json_string(encoded.as_bytes()));
                out.push('"');
            }
            VariantPrimitiveType::Uuid => {
                let uuid = variant.get_uuid()?;
                out.push('"');
                out.push_str(&uuid_to_string(uuid));
                out.push('"');
            }
            VariantPrimitiveType::Date => {
                let days = variant.get_date()?;
                let date = 719163_i32
                    .checked_add(days)
                    .and_then(NaiveDate::from_num_days_from_ce_opt)
                    .ok_or_else(|| {
                        VariantFailure(Box::new(VariantFailureCause::NumericDate { days }))
                    })?;
                out.push('"');
                out.push_str(&date.format("%Y-%m-%d").to_string());
                out.push('"');
            }
            VariantPrimitiveType::TimestampTz
            | VariantPrimitiveType::TimestampNtz
            | VariantPrimitiveType::TimestampTzNanos
            | VariantPrimitiveType::TimestampNtzNanos => {
                let primitive = variant.primitive_type()?;
                let ts = format_variant_timestamp(
                    variant.get_int64()?,
                    matches!(
                        primitive,
                        VariantPrimitiveType::TimestampTzNanos
                            | VariantPrimitiveType::TimestampNtzNanos
                    ),
                    matches!(
                        primitive,
                        VariantPrimitiveType::TimestampTz | VariantPrimitiveType::TimestampTzNanos
                    ),
                )?;
                out.push('"');
                out.push_str(&ts);
                out.push('"');
            }
            VariantPrimitiveType::TimeNtz => {
                return Err(VariantFailure::encoding("Unsupported variant type"));
            }
        },
        BasicType::ShortString => {
            let s = variant.get_string()?;
            out.push('"');
            out.push_str(&escape_json_string(s));
            out.push('"');
        }
        BasicType::Object => {
            let info = variant.object_info()?;
            out.push('{');
            for i in 0..info.num_elements {
                if i > 0 {
                    out.push(',');
                }
                let id_pos = info.id_start_offset + i as usize * info.id_size as usize;
                let field_id = read_le_u32(&variant.value[id_pos..], info.id_size)?;
                let key = variant.metadata.get_key(field_id)?;
                out.push('"');
                out.push_str(&escape_json_string(key.as_bytes()));
                out.push('"');
                out.push(':');

                let offset_pos = info.offset_start_offset + i as usize * info.offset_size as usize;
                let offset = read_le_u32(&variant.value[offset_pos..], info.offset_size)? as usize;
                let start = info.data_start_offset + offset;
                if start >= variant.value.len() {
                    return Err(VariantFailure::encoding("Invalid offset in object"));
                }
                let sub = VariantRef {
                    metadata: variant.metadata,
                    value: &variant.value[start..],
                };
                variant_to_json(&sub, out)?;
            }
            out.push('}');
        }
        BasicType::Array => {
            let info = variant.array_info()?;
            out.push('[');
            for i in 0..info.num_elements {
                if i > 0 {
                    out.push(',');
                }
                let offset_pos = info.offset_start_offset + i as usize * info.offset_size as usize;
                let offset = read_le_u32(&variant.value[offset_pos..], info.offset_size)? as usize;
                let start = info.data_start_offset + offset;
                if start >= variant.value.len() {
                    return Err(VariantFailure::encoding("Invalid offset in array"));
                }
                let sub = VariantRef {
                    metadata: variant.metadata,
                    value: &variant.value[start..],
                };
                variant_to_json(&sub, out)?;
            }
            out.push(']');
        }
    }
    Ok(())
}

fn decimal_to_string(value: i128, scale: u8) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let negative = value < 0;
    let abs = value.unsigned_abs();
    let digits = abs.to_string();
    let scale_usize = scale as usize;
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    if digits.len() <= scale_usize {
        out.push('0');
        out.push('.');
        for _ in 0..(scale_usize - digits.len()) {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        let split = digits.len() - scale_usize;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    out
}

fn bytes_to_string(value: &[u8]) -> Result<String, VariantFailure> {
    std::str::from_utf8(value)
        .map(|s| s.to_string())
        .map_err(VariantFailure::utf8)
}

fn parse_bool_from_bytes(value: &[u8]) -> Result<bool, VariantFailure> {
    let s = std::str::from_utf8(value).map_err(VariantFailure::utf8)?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(VariantFailure::conversion(
            "Failed to cast empty string to BOOLEAN",
        ));
    }
    if let Ok(parsed) = trimmed.parse::<i32>() {
        return Ok(parsed != 0);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Ok(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Ok(false);
    }
    Err(VariantFailure::conversion(format_args!(
        "Failed to cast string '{}' to BOOLEAN",
        s
    )))
}

fn decimal_to_i64(value: i128, scale: u8) -> i64 {
    if scale == 0 {
        return value as i64;
    }
    let mut v = value;
    for _ in 0..scale {
        v /= 10;
    }
    v as i64
}

fn decimal_to_f64(value: i128, scale: u8) -> f64 {
    if scale == 0 {
        return value as f64;
    }
    let divisor = 10_f64.powi(scale as i32);
    (value as f64) / divisor
}

fn timestamp_tz_to_local_naive(
    micros: i64,
    tz: FixedOffset,
) -> Result<NaiveDateTime, VariantFailure> {
    let seconds = micros.div_euclid(1_000_000);
    let micros_rem = micros.rem_euclid(1_000_000) as u32;
    let failure = || {
        VariantFailure(Box::new(VariantFailureCause::NumericTimestamp {
            micros,
            offset_seconds: tz.local_minus_utc(),
        }))
    };
    let dt_utc = DateTime::<Utc>::from_timestamp(seconds, micros_rem * 1000).ok_or_else(failure)?;
    dt_utc
        .naive_utc()
        .checked_add_signed(chrono::TimeDelta::seconds(i64::from(tz.local_minus_utc())))
        .ok_or_else(failure)
}

fn remove_trailing_zeros(input: &str) -> String {
    if let Some(dot) = input.find('.') {
        let mut end = input.len();
        while end > dot + 1 && input.as_bytes()[end - 1] == b'0' {
            end -= 1;
        }
        if end == dot + 1 {
            return format!("{}0", &input[..dot + 1]);
        }
        return input[..end].to_string();
    }
    input.to_string()
}

fn float_to_json_string(value: f64) -> String {
    if !value.is_finite() {
        return "null".to_string();
    }
    let mut s = value.to_string();
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        s.push_str(".0");
    }
    s
}

fn escape_json_string(input: &[u8]) -> String {
    let mut out = String::new();
    for &c in input {
        match c {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x08 => out.push_str("\\b"),
            0x0C => out.push_str("\\f"),
            c if c < 0x20 => out.push_str(&format!("\\u{:04x}", c)),
            _ => out.push(c as char),
        }
    }
    out
}

fn uuid_to_string(bytes: [u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

fn format_variant_timestamp(
    value: i64,
    nanos: bool,
    with_zone: bool,
) -> Result<String, VariantFailure> {
    let (units_per_second, width, nanos_per_unit) = if nanos {
        (1_000_000_000, 9, 1)
    } else {
        (1_000_000, 6, 1000)
    };
    let seconds = value.div_euclid(units_per_second);
    let fraction = value.rem_euclid(units_per_second) as u32;
    let dt =
        DateTime::<Utc>::from_timestamp(seconds, fraction * nanos_per_unit).ok_or_else(|| {
            VariantFailure(Box::new(VariantFailureCause::NumericTimestamp {
                micros: if nanos { value.div_euclid(1000) } else { value },
                offset_seconds: 0,
            }))
        })?;
    let base = dt.format("%Y-%m-%d %H:%M:%S");
    let suffix = if with_zone { " UTC" } else { "" };
    Ok(format!("{base}.{fraction:0width$}{suffix}"))
}

/// Whether an Arrow type can carry raw variant metadata or value bytes.
pub fn is_variant_binary_like(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

/// Whether an Arrow type is a supported Parquet VARIANT struct layout.
///
/// Accepts both the unshredded form (`metadata` + `value`) and the shredded
/// form (`metadata` + `typed_value`, optionally with `value`). Any other field
/// name disqualifies the struct.
pub fn is_variant_struct_data_type(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    if fields.is_empty() {
        return false;
    }
    let mut has_metadata = false;
    let mut has_value = false;
    let mut has_typed_value = false;
    for field in fields {
        match field.name().as_str() {
            "metadata" if is_variant_binary_like(field.data_type()) => has_metadata = true,
            "value" if is_variant_binary_like(field.data_type()) => has_value = true,
            "typed_value" => has_typed_value = true,
            _ => return false,
        }
    }
    has_metadata && (has_value || has_typed_value)
}

#[cfg(test)]
mod tests {
    #[test]
    fn variant_timestamp_rendering_matches_trino_utc_and_precision() {
        for (primitive, value, expected) in [
            (
                VariantPrimitiveType::TimestampTz,
                DateTime::parse_from_rfc3339("2026-07-01T08:00:00+08:00")
                    .unwrap()
                    .timestamp_micros(),
                "2026-07-01 00:00:00.000000 UTC",
            ),
            (
                VariantPrimitiveType::TimestampTz,
                DateTime::parse_from_rfc3339("2026-01-01T08:00:00.123-05:00")
                    .unwrap()
                    .timestamp_micros(),
                "2026-01-01 13:00:00.123000 UTC",
            ),
            (
                VariantPrimitiveType::TimestampNtz,
                DateTime::parse_from_rfc3339("2026-07-01T08:00:00Z")
                    .unwrap()
                    .timestamp_micros(),
                "2026-07-01 08:00:00.000000",
            ),
            (
                VariantPrimitiveType::TimestampTz,
                -1,
                "1969-12-31 23:59:59.999999 UTC",
            ),
            (
                VariantPrimitiveType::TimestampNtz,
                -1,
                "1969-12-31 23:59:59.999999",
            ),
            (
                VariantPrimitiveType::TimestampTzNanos,
                1,
                "1970-01-01 00:00:00.000000001 UTC",
            ),
            (
                VariantPrimitiveType::TimestampNtzNanos,
                -1,
                "1969-12-31 23:59:59.999999999",
            ),
        ] {
            let mut bytes = vec![primitive_header(primitive)];
            bytes.extend_from_slice(&value.to_le_bytes());
            let variant = VariantValue::create(&[1, 0, 0], &bytes).unwrap();
            assert_eq!(variant.to_json().unwrap(), format!("\"{expected}\""));
            assert_eq!(variant_to_string(&variant).unwrap(), expected);
            assert_eq!(
                try_variant_to_json(&variant).unwrap(),
                Some(format!("\"{expected}\""))
            );
        }
    }

    #[test]
    fn variant_temporal_extremes_refuse_without_epoch_fallback_or_panic() {
        use novarocks_failure_contract::FailureProjection;
        for days in [i32::MIN, i32::MAX] {
            let mut bytes = vec![primitive_header(VariantPrimitiveType::Date)];
            bytes.extend_from_slice(&days.to_le_bytes());
            let value = VariantValue::create(&[1, 0, 0], &bytes).unwrap();
            assert_eq!(
                value.to_json().unwrap_err().failure_fact(),
                novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
            );
        }
        let tz = FixedOffset::east_opt(0).unwrap();
        for primitive in [
            VariantPrimitiveType::TimestampTz,
            VariantPrimitiveType::TimestampNtz,
        ] {
            for micros in [i64::MIN, i64::MAX] {
                let mut bytes = vec![primitive_header(primitive)];
                bytes.extend_from_slice(&micros.to_le_bytes());
                let value = VariantValue::create(&[1, 0, 0], &bytes).unwrap();
                assert_eq!(
                    value.to_json().unwrap_err().failure_fact(),
                    novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
                );
                if matches!(primitive, VariantPrimitiveType::TimestampTz) {
                    assert!(variant_to_date_days(&value, tz).is_err());
                    assert!(variant_to_datetime_micros(&value, tz).is_err());
                    assert!(variant_to_time_micros(&value, tz).is_err());
                }
            }
        }
    }
    #[test]
    fn variant_temporal_checked_offset_preserves_negative_fraction_bytes() {
        assert_eq!(
            format_variant_timestamp(-1, false, true).unwrap(),
            "1969-12-31 23:59:59.999999 UTC"
        );
        assert_eq!(
            format_variant_timestamp(-1, false, false).unwrap(),
            "1969-12-31 23:59:59.999999"
        );
        let max = NaiveDateTime::MAX.and_utc().timestamp_micros();
        assert!(timestamp_tz_to_local_naive(max, FixedOffset::east_opt(86_399).unwrap()).is_err());
        assert_eq!(
            decimal_to_string(i128::MIN, 0),
            "-170141183460469231731687303715884105728"
        );
        assert_eq!(
            decimal_to_string(i128::MIN, 1),
            "-17014118346046923173168730371588410572.8"
        );
    }

    #[test]
    fn variant_source_retains_utf8_and_parse_errors_without_input_storage() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use std::error::Error;
        let bytes = std::sync::Arc::new(vec![0xff_u8]);
        let weak = std::sync::Arc::downgrade(&bytes);
        let error = bytes_to_string(bytes.as_slice()).unwrap_err();
        assert!(error.source().unwrap().is::<std::str::Utf8Error>());
        drop(bytes);
        assert!(weak.upgrade().is_none());
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        let index = parse_variant_path("$[4294967296]").unwrap_err();
        assert!(index.source().unwrap().is::<std::num::ParseIntError>());
    }
    #[test]
    fn variant_unit_size_is_exact_representation_without_allocating_payload() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
        let error = VariantFailure::size(VARIANT_MAX_SIZE as u128 + 1);
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(
            fact.representation().unwrap().boundary,
            RepresentationBoundary::MinimumUnit
        );
        assert_eq!(
            fact.representation().unwrap().required,
            Some(VARIANT_MAX_SIZE as u64 + 1)
        );
    }

    use super::*;
    #[derive(Debug, PartialEq)]
    enum PathFailure {
        Source(VariantFailure),
        Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
        Refused(std::sync::Arc<str>),
    }
    impl From<VariantFailure> for PathFailure {
        fn from(error: VariantFailure) -> Self {
            Self::Source(error)
        }
    }
    impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError> for PathFailure {
        fn from(
            error: novarocks_type_contract::owned_resources::layout::LayoutResourceError,
        ) -> Self {
            Self::Resource(error)
        }
    }
    fn step_key(step: VariantPathConstructionStep) -> (usize, usize, bool) {
        match step {
            VariantPathConstructionStep::Request(layout) => (layout.size(), layout.align(), true),
            VariantPathConstructionStep::Work(units) => (units, 0, false),
        }
    }
    #[test]
    fn observed_path_preserves_quoted_numeric_and_original_syntax_errors() {
        for input in [
            "",
            "a.b",
            "[0].a",
            "12",
            "$.a[2]['é']",
            "$['a\\n\\t'].nested[4294967295]",
        ] {
            let plain = parse_variant_path(input).unwrap();
            let observed = parse_variant_path_with_storage_observed::<PathFailure>(
                input,
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                |_| Ok(()),
            )
            .unwrap();
            assert_eq!(
                format!("{:?}", observed.segments()),
                format!("{:?}", plain.segments)
            );
        }
        for input in ["$.", "$[", "$['é'", "$[4294967296]", "$[x]", "$.a!"] {
            let original = parse_variant_path(input).unwrap_err();
            let observed = parse_variant_path_with_storage_observed::<PathFailure>(
                input,
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                |_| Ok(()),
            )
            .unwrap_err();
            assert_eq!(observed, PathFailure::Source(original));
        }
    }
    #[test]
    fn every_actual_path_constructor_and_work_refusal_keeps_original_payload_and_prefix() {
        let input = "  $['a\\nlongkey'].one.two.three.four.five[123]  ";
        let mut baseline = Vec::new();
        let parsed = parse_variant_path_with_storage_observed::<PathFailure>(
            input,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            |step| {
                baseline.push(step_key(step));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(parsed.segments().len(), 7);
        assert!(
            baseline
                .iter()
                .any(|(bytes, _, request)| *request && *bytes > 4)
        );
        for at in 0..baseline.len() {
            let origin: std::sync::Arc<str> = std::sync::Arc::from("original parser refusal");
            let mut trace = Vec::new();
            let result = parse_variant_path_with_storage_observed::<PathFailure>(
                input,
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                |step| {
                    assert!(trace.len() <= at, "no observer tail after refusal");
                    trace.push(step_key(step));
                    if trace.len() == at + 1 {
                        Err(PathFailure::Refused(origin.clone()))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
            let PathFailure::Refused(actual) = result else {
                panic!("original refusal changed")
            };
            assert!(std::sync::Arc::ptr_eq(&actual, &origin));
            assert_eq!(trace, baseline[..=at]);
        }
    }

    #[test]
    fn sqlx2_value_codec_variant_get_target_type_is_stable() {
        assert_eq!(variant_get_target_type("bigint"), Ok(DataType::Int64));
        assert_eq!(variant_get_target_type(" INT "), Ok(DataType::Int32));
        assert_eq!(variant_get_target_type("string"), Ok(DataType::Utf8));
        assert_eq!(
            variant_get_target_type("datetime"),
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
        assert!(variant_get_target_type("decimal(10,2)").is_err());
        assert!(variant_get_target_type("variant").is_err());
    }

    #[test]
    fn split_serialized_round_trips_create_inputs() {
        let metadata = VariantMetadata::empty();
        let value = vec![6u8 << 2, 123, 0, 0, 0, 0, 0, 0, 0]; // int64 primitive 123
        let v = VariantValue::create(metadata.raw(), &value).expect("create");
        let serialized = v.serialize();
        let (m, val) = split_serialized(&serialized).expect("split");
        assert_eq!(m, metadata.raw());
        assert_eq!(val, value.as_slice());
    }

    #[test]
    fn split_serialized_rejects_truncated_input() {
        assert!(split_serialized(&[0u8, 0, 0]).is_err());
        // size header claims more bytes than present
        assert!(split_serialized(&[200u8, 0, 0, 0, 1]).is_err());
    }
}

/// Nullable value conversion; malformed physical data remains an error.
pub fn try_variant_to_bool(value: &VariantValue) -> Result<Option<bool>, VariantFailure> {
    nullable_conversion(variant_to_bool(value))
}

/// Nullable value conversion; malformed physical data remains an error.
pub fn try_variant_to_i64(value: &VariantValue) -> Result<Option<i64>, VariantFailure> {
    nullable_conversion(variant_to_i64(value))
}

/// Nullable value conversion; malformed physical data remains an error.
pub fn try_variant_to_f64(value: &VariantValue) -> Result<Option<f64>, VariantFailure> {
    nullable_conversion(variant_to_f64(value))
}

/// Nullable value conversion; malformed physical data remains an error.
pub fn try_variant_to_string(value: &VariantValue) -> Result<Option<String>, VariantFailure> {
    nullable_conversion(variant_to_string(value))
}

/// Nullable temporal conversion; it preserves physical and resource failures.
pub fn try_variant_to_date_days(
    value: &VariantValue,
    tz: FixedOffset,
) -> Result<Option<i32>, VariantFailure> {
    nullable_conversion(variant_to_date_days(value, tz))
}

/// Nullable temporal conversion; it preserves physical and resource failures.
pub fn try_variant_to_datetime_micros(
    value: &VariantValue,
    tz: FixedOffset,
) -> Result<Option<i64>, VariantFailure> {
    nullable_conversion(variant_to_datetime_micros(value, tz))
}

/// Nullable temporal conversion; it preserves physical and resource failures.
pub fn try_variant_to_time_micros(
    value: &VariantValue,
    tz: FixedOffset,
) -> Result<Option<i64>, VariantFailure> {
    nullable_conversion(variant_to_time_micros(value, tz))
}

/// Nullable JSON rendering for a value that has no supported conversion.
/// Malformed storage and format/resource limits cannot become SQL NULL.
pub fn try_variant_to_json(value: &VariantValue) -> Result<Option<String>, VariantFailure> {
    nullable_conversion(value.to_json())
}

#[cfg(test)]
mod nullable_operation_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    fn value(json: &str) -> VariantValue {
        VariantValue::from_serialized(
            &super::super::variant_encode::encode_json_text_to_variant_bytes(json).unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn nullable_navigation_distinguishes_absence_from_damaged_storage() {
        let valid = value(r#"{"a": [1]}"#);
        for path in ["$.missing", "$.a[1]", "$.a[0].missing"] {
            let path = parse_variant_path(path).unwrap();
            assert!(variant_query_nullable(&valid, &path).unwrap().is_none());
            assert!(variant_query(&valid, &path).is_err());
        }
        let path = parse_variant_path("$.a[0]").unwrap();
        let selected = variant_query_nullable(&valid, &path).unwrap().unwrap();
        assert_eq!(variant_to_i64(&selected).unwrap(), 1);
        let malformed = VariantValue::create(VariantMetadata::empty().raw(), &[]).unwrap();
        let failure =
            variant_query_nullable(&malformed, &parse_variant_path("$.a").unwrap()).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 60);
    }
    #[test]
    fn nullable_conversion_does_not_consume_physical_or_representation_failures() {
        assert_eq!(
            try_variant_to_i64(&value(r#""not an integer""#)).unwrap(),
            None
        );
        assert_eq!(try_variant_to_bool(&value("[]")).unwrap(), None);
        let malformed = VariantValue::create(VariantMetadata::empty().raw(), &[]).unwrap();
        assert!(try_variant_to_bool(&malformed).is_err());
        let failure = VariantValue::from_serialized(&((VARIANT_MAX_SIZE + 1) as u32).to_le_bytes())
            .unwrap_err();
        let original = failure.failure_fact();
        let failure = nullable_conversion::<bool>(Err(failure)).unwrap_err();
        assert_eq!(failure.failure_fact(), original);
        assert_eq!(original.class(), FailureClass::Representation);
    }
    #[test]
    fn nullable_path_syntax_has_an_explicit_absence_result() {
        assert!(parse_variant_path_nullable("$.a[0]").unwrap().is_some());
        assert!(parse_variant_path_nullable("not a path").unwrap().is_none());
        assert!(
            parse_variant_path_nullable("$[4294967296]")
                .unwrap()
                .is_none()
        );
    }
}

#[cfg(test)]
mod nullable_typeof_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};

    #[test]
    fn nullable_typeof_retains_original_labels_for_both_actual_encodings() {
        let metadata = VariantMetadata::empty();
        let mut integer = vec![6 << 2];
        integer.extend_from_slice(&123_i64.to_le_bytes());
        for (payload, expected) in [(integer.as_slice(), "Int64"), (&[0][..], "Null")] {
            let serialized = VariantValue::create(metadata.raw(), payload)
                .unwrap()
                .serialize();
            assert_eq!(
                variant_typeof_serialized_nullable(&serialized)
                    .unwrap()
                    .as_deref(),
                Some(expected)
            );
            assert_eq!(
                variant_typeof_parts_nullable(metadata.raw(), payload)
                    .unwrap()
                    .as_deref(),
                Some(expected)
            );
        }
    }

    #[test]
    fn nullable_typeof_nulls_only_the_original_malformed_encoding_rows() {
        assert_eq!(variant_typeof_serialized_nullable(&[255]).unwrap(), None);
        assert_eq!(variant_typeof_parts_nullable(&[255], &[0]).unwrap(), None);
        assert_eq!(
            variant_typeof_parts_nullable(VariantMetadata::empty().raw(), &[63 << 2]).unwrap(),
            None
        );
    }

    #[test]
    fn nullable_typeof_preserves_the_real_decoder_extent_refusal() {
        // The real decoder checks the declared extent before touching payload.
        // Four bytes exercise its production refusal without a huge fixture.
        let header = ((VARIANT_MAX_SIZE + 1) as u32).to_le_bytes();
        let original = VariantValue::from_serialized(&header).unwrap_err();
        let refusal = variant_typeof_serialized_nullable(&header).unwrap_err();
        assert_eq!(refusal, original);
        let fact = refusal.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(fact.reason().code(), 64);
        let extent = fact
            .representation()
            .expect("actual Variant representation extent");
        assert_eq!(extent.required, Some(VARIANT_MAX_SIZE as u64 + 1));
        assert_eq!(extent.extent_limit, Some(VARIANT_MAX_SIZE as u64));
    }
}
