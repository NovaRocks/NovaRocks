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
use base64::Engine;
use chrono::{
    DateTime, Datelike, FixedOffset, Local, NaiveDate, NaiveDateTime, Offset, Timelike, Utc,
};

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

    pub fn from_bytes(raw: Vec<u8>) -> Result<Self, String> {
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

    pub fn get_key(&self, index: u32) -> Result<String, String> {
        if index >= self.dict_size {
            return Err(format!(
                "Variant index out of range: {} >= {}",
                index, self.dict_size
            ));
        }
        let offset_size = self.offset_size as usize;
        let offset_start = HEADER_SIZE + offset_size + (index as usize) * offset_size;
        let value_offset = read_le_u32(&self.raw[offset_start..], self.offset_size)? as usize;
        let value_next_offset =
            read_le_u32(&self.raw[offset_start + offset_size..], self.offset_size)? as usize;
        let key_len = value_next_offset.saturating_sub(value_offset);
        let string_start = HEADER_SIZE + offset_size * (self.dict_size as usize + 2) + value_offset;
        if string_start + key_len > self.raw.len() {
            return Err("Variant string out of range".to_string());
        }
        let bytes = &self.raw[string_start..string_start + key_len];
        let key = String::from_utf8(bytes.to_vec())
            .map_err(|_| "Variant metadata key is not valid UTF-8".to_string())?;
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
            if let Ok(field_key) = std::str::from_utf8(&self.raw[start..start + key_len]) {
                if field_key == key {
                    indexes.push(i as u32);
                }
            }
            prev_key_offset = next_key_offset;
        }
        indexes
    }
}

#[derive(Clone, Debug)]
pub struct VariantValue {
    metadata: VariantMetadata,
    value: Vec<u8>,
}

impl VariantValue {
    pub fn create(metadata: &[u8], value: &[u8]) -> Result<Self, String> {
        if metadata.is_empty() {
            return Ok(Self::null_value());
        }
        validate_metadata(metadata)?;
        if metadata.len() + value.len() > VARIANT_MAX_SIZE {
            return Err(format!(
                "Variant value size exceeds maximum limit: {} > {}",
                metadata.len() + value.len(),
                VARIANT_MAX_SIZE
            ));
        }
        Ok(Self {
            metadata: VariantMetadata::from_bytes(metadata.to_vec())?,
            value: value.to_vec(),
        })
    }

    pub fn from_serialized(data: &[u8]) -> Result<Self, String> {
        if data.len() < 4 {
            return Err("Invalid variant slice: too small to contain size header".to_string());
        }
        let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if size > VARIANT_MAX_SIZE {
            return Err(format!(
                "Variant size exceeds maximum limit: {} > {}",
                size, VARIANT_MAX_SIZE
            ));
        }
        if size > data.len().saturating_sub(4) {
            return Err(format!(
                "Invalid variant size: {} exceeds available data: {}",
                size,
                data.len().saturating_sub(4)
            ));
        }
        let payload = &data[4..4 + size];
        let metadata = load_metadata(payload)?;
        if metadata.len() > payload.len() {
            return Err("Metadata size exceeds variant size".to_string());
        }
        let metadata_bytes = metadata.to_vec();
        let value_bytes = payload[metadata.len()..].to_vec();
        Self::create(&metadata_bytes, &value_bytes)
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

    pub fn to_json_local(&self) -> Result<String, String> {
        let tz = Local::now().offset().fix();
        self.to_json(Some(tz))
    }

    pub fn to_json(&self, tz: Option<FixedOffset>) -> Result<String, String> {
        let mut out = String::new();
        let variant = VariantRef {
            metadata: &self.metadata,
            value: &self.value,
        };
        variant_to_json(&variant, &mut out, tz)?;
        Ok(out)
    }
}

struct VariantRef<'a> {
    metadata: &'a VariantMetadata,
    value: &'a [u8],
}

impl<'a> VariantRef<'a> {
    fn basic_type(&self) -> Result<BasicType, String> {
        if self.value.is_empty() {
            return Err("Variant value is empty".to_string());
        }
        let bt = self.value[0] & 0b11;
        match bt {
            0 => Ok(BasicType::Primitive),
            1 => Ok(BasicType::ShortString),
            2 => Ok(BasicType::Object),
            3 => Ok(BasicType::Array),
            _ => Err("Invalid variant basic type".to_string()),
        }
    }

    fn value_header(&self) -> u8 {
        self.value[0] >> 2
    }

    fn primitive_type(&self) -> Result<VariantPrimitiveType, String> {
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
            _ => return Err(format!("Unsupported variant primitive type: {header}")),
        })
    }

    fn primitive_bytes(&self, size: usize) -> Result<&'a [u8], String> {
        if self.value.len() < HEADER_SIZE + size {
            return Err(format!(
                "Variant value too short: expected {} bytes, got {}",
                HEADER_SIZE + size,
                self.value.len()
            ));
        }
        Ok(&self.value[HEADER_SIZE..HEADER_SIZE + size])
    }

    fn get_bool(&self) -> Result<bool, String> {
        match self.primitive_type()? {
            VariantPrimitiveType::BooleanTrue => Ok(true),
            VariantPrimitiveType::BooleanFalse => Ok(false),
            other => Err(format!("Not a boolean variant type: {:?}", other)),
        }
    }

    fn get_int8(&self) -> Result<i8, String> {
        let bytes = self.primitive_bytes(1)?;
        Ok(i8::from_le_bytes([bytes[0]]))
    }

    fn get_int16(&self) -> Result<i16, String> {
        let bytes = self.primitive_bytes(2)?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn get_int32(&self) -> Result<i32, String> {
        let bytes = self.primitive_bytes(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn get_int64(&self) -> Result<i64, String> {
        let bytes = self.primitive_bytes(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn get_float32(&self) -> Result<f32, String> {
        let bytes = self.primitive_bytes(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn get_float64(&self) -> Result<f64, String> {
        let bytes = self.primitive_bytes(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn get_decimal4(&self) -> Result<(i32, u8), String> {
        let bytes = self.primitive_bytes(1 + 4)?;
        let scale = bytes[0];
        let val = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        Ok((val, scale))
    }

    fn get_decimal8(&self) -> Result<(i64, u8), String> {
        let bytes = self.primitive_bytes(1 + 8)?;
        let scale = bytes[0];
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[1..9]);
        Ok((i64::from_le_bytes(buf), scale))
    }

    fn get_decimal16(&self) -> Result<(i128, u8), String> {
        let bytes = self.primitive_bytes(1 + 16)?;
        let scale = bytes[0];
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&bytes[1..17]);
        let value = i128::from_le_bytes(buf);
        Ok((value, scale))
    }

    fn get_date(&self) -> Result<i32, String> {
        self.get_int32()
    }

    fn get_timestamp_micros(&self) -> Result<i64, String> {
        self.get_int64()
    }

    fn get_timestamp_micros_ntz(&self) -> Result<i64, String> {
        self.get_int64()
    }

    fn get_time_micros_ntz(&self) -> Result<i64, String> {
        self.get_int64()
    }

    fn get_uuid(&self) -> Result<[u8; 16], String> {
        let bytes = self.primitive_bytes(16)?;
        let mut out = [0u8; 16];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    fn get_string(&self) -> Result<&'a [u8], String> {
        match self.basic_type()? {
            BasicType::ShortString => {
                let len = self.value_header() as usize;
                if self.value.len() < HEADER_SIZE + len {
                    return Err("Invalid short string length".to_string());
                }
                Ok(&self.value[HEADER_SIZE..HEADER_SIZE + len])
            }
            BasicType::Primitive => {
                if self.value.len() < HEADER_SIZE + 4 {
                    return Err("Invalid string value: too short".to_string());
                }
                let len = read_le_u32(&self.value[HEADER_SIZE..], 4)? as usize;
                let start = HEADER_SIZE + 4;
                if self.value.len() < start + len {
                    return Err("Invalid string value: length out of bounds".to_string());
                }
                Ok(&self.value[start..start + len])
            }
            other => Err(format!("Expected string, got {:?}", other)),
        }
    }

    fn get_binary(&self) -> Result<&'a [u8], String> {
        if self.value.len() < HEADER_SIZE + 4 {
            return Err("Invalid binary value: too short".to_string());
        }
        let len = read_le_u32(&self.value[HEADER_SIZE..], 4)? as usize;
        let start = HEADER_SIZE + 4;
        if self.value.len() < start + len {
            return Err("Invalid binary value: length out of bounds".to_string());
        }
        Ok(&self.value[start..start + len])
    }

    fn object_info(&self) -> Result<ObjectInfo, String> {
        if self.basic_type()? != BasicType::Object {
            return Err("Not an object variant".to_string());
        }
        let header = self.value_header();
        let field_offset_size = (header & 0b11) + 1;
        let field_id_size = ((header >> 2) & 0b11) + 1;
        let is_large = ((header >> 4) & 0b1) == 1;
        let num_elements_size = if is_large { 4 } else { 1 };
        if self.value.len() < HEADER_SIZE + num_elements_size as usize {
            return Err("Object value too short".to_string());
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
            return Err("Invalid object value offsets".to_string());
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

    fn array_info(&self) -> Result<ArrayInfo, String> {
        if self.basic_type()? != BasicType::Array {
            return Err("Not an array variant".to_string());
        }
        let header = self.value_header();
        let field_offset_size = (header & 0b11) + 1;
        let is_large = ((header >> 2) & 0b1) == 1;
        let num_elements_size = if is_large { 4 } else { 1 };
        if self.value.len() < HEADER_SIZE + num_elements_size as usize {
            return Err("Array value too short".to_string());
        }
        let num_elements = read_le_u32(
            &self.value[HEADER_SIZE..HEADER_SIZE + num_elements_size as usize],
            num_elements_size,
        )?;
        let offset_start_offset = HEADER_SIZE + num_elements_size as usize;
        let data_start_offset =
            offset_start_offset + (num_elements as usize + 1) * field_offset_size as usize;
        if data_start_offset > self.value.len() {
            return Err("Invalid array value offsets".to_string());
        }
        Ok(ArrayInfo {
            num_elements,
            offset_size: field_offset_size,
            offset_start_offset,
            data_start_offset,
        })
    }

    fn get_object_by_key(&self, key: &str) -> Result<&'a [u8], String> {
        let info = self.object_info()?;
        let dict_indexes = self.metadata.get_index(key);
        if dict_indexes.is_empty() {
            return Err(format!("Field key not found: {}", key));
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
                    return Err("Object offset out of bounds".to_string());
                }
                return Ok(&self.value[start..]);
            }
        }
        Err(format!("Field key not found: {}", key))
    }

    fn get_element_at_index(&self, index: u32) -> Result<&'a [u8], String> {
        let info = self.array_info()?;
        if index >= info.num_elements {
            return Err(format!(
                "Array index out of range: {} >= {}",
                index, info.num_elements
            ));
        }
        let offset_pos = info.offset_start_offset + index as usize * info.offset_size as usize;
        let offset = read_le_u32(&self.value[offset_pos..], info.offset_size)? as usize;
        let start = info.data_start_offset + offset;
        if start >= self.value.len() {
            return Err("Array offset out of bounds".to_string());
        }
        Ok(&self.value[start..])
    }
}

pub fn is_variant_null(value: &VariantValue) -> Result<bool, String> {
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

pub fn variant_typeof(value: &VariantValue) -> Result<String, String> {
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

pub fn variant_to_bool(value: &VariantValue) -> Result<bool, String> {
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
            other => Err(format!("Cannot cast variant {:?} to BOOLEAN", other)),
        },
        BasicType::ShortString => parse_bool_from_bytes(variant.get_string()?),
        other => Err(format!("Cannot cast variant {:?} to BOOLEAN", other)),
    }
}

pub fn variant_to_i64(value: &VariantValue) -> Result<i64, String> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err("Cannot cast non-primitive variant to BIGINT".to_string());
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
        other => Err(format!("Cannot cast variant {:?} to BIGINT", other)),
    }
}

pub fn variant_to_f64(value: &VariantValue) -> Result<f64, String> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err("Cannot cast non-primitive variant to DOUBLE".to_string());
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
        other => Err(format!("Cannot cast variant {:?} to DOUBLE", other)),
    }
}

pub fn variant_to_string(value: &VariantValue) -> Result<String, String> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    match variant.basic_type()? {
        BasicType::ShortString => bytes_to_string(variant.get_string()?),
        BasicType::Primitive => match variant.primitive_type()? {
            VariantPrimitiveType::String => bytes_to_string(variant.get_string()?),
            _ => value.to_json_local(),
        },
        _ => value.to_json_local(),
    }
}

pub fn variant_to_date_days(value: &VariantValue, tz: FixedOffset) -> Result<i32, String> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err("Cannot cast non-primitive variant to DATE".to_string());
    }
    match variant.primitive_type()? {
        VariantPrimitiveType::Date => variant.get_date(),
        VariantPrimitiveType::TimestampTz => {
            let micros = variant.get_timestamp_micros()?;
            let date = timestamp_tz_to_local_naive(micros, tz).date();
            Ok(date.num_days_from_ce() - 719163)
        }
        other => Err(format!("Cannot cast variant {:?} to DATE", other)),
    }
}

pub fn variant_to_datetime_micros(value: &VariantValue, tz: FixedOffset) -> Result<i64, String> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err("Cannot cast non-primitive variant to DATETIME".to_string());
    }
    match variant.primitive_type()? {
        VariantPrimitiveType::TimestampNtz => variant.get_timestamp_micros_ntz(),
        VariantPrimitiveType::TimestampTz => {
            let micros = variant.get_timestamp_micros()?;
            let naive = timestamp_tz_to_local_naive(micros, tz);
            let utc = naive.and_utc();
            Ok(utc.timestamp() * 1_000_000 + utc.timestamp_subsec_micros() as i64)
        }
        other => Err(format!("Cannot cast variant {:?} to DATETIME", other)),
    }
}

pub fn variant_to_time_micros(value: &VariantValue, tz: FixedOffset) -> Result<i64, String> {
    let variant = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    if variant.basic_type()? != BasicType::Primitive {
        return Err("Cannot cast non-primitive variant to TIME".to_string());
    }
    match variant.primitive_type()? {
        VariantPrimitiveType::TimeNtz => variant.get_time_micros_ntz(),
        VariantPrimitiveType::TimestampTz => {
            let micros = variant.get_timestamp_micros()?;
            let naive = timestamp_tz_to_local_naive(micros, tz);
            let time = naive.time();
            Ok(time.num_seconds_from_midnight() as i64 * 1_000_000
                + (time.nanosecond() / 1000) as i64)
        }
        other => Err(format!("Cannot cast variant {:?} to TIME", other)),
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

pub fn parse_variant_path(path: &str) -> Result<VariantPath, String> {
    let normalized = normalize_variant_path(path);
    let mut parser = PathParser::new(&normalized);
    parser.parse()
}

fn normalize_variant_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return "$".to_string();
    }
    if trimmed.starts_with('$') {
        return trimmed.to_string();
    }
    if trimmed.starts_with('[') {
        return format!("${}", trimmed);
    }
    if trimmed.starts_with('.') {
        return format!("${}", trimmed);
    }
    if trimmed.chars().all(|c| c.is_ascii_digit()) {
        return format!("$[{}]", trimmed);
    }
    format!("$.{}", trimmed)
}

pub fn variant_query(value: &VariantValue, path: &VariantPath) -> Result<VariantValue, String> {
    let mut current = VariantRef {
        metadata: value.metadata(),
        value: value.value(),
    };
    let mut current_value = current.value;
    for segment in &path.segments {
        let next_value = match segment {
            VariantPathSegment::ObjectKey(key) => {
                let v = VariantRef {
                    metadata: current.metadata,
                    value: current_value,
                };
                v.get_object_by_key(key)?
            }
            VariantPathSegment::ArrayIndex(index) => {
                let v = VariantRef {
                    metadata: current.metadata,
                    value: current_value,
                };
                v.get_element_at_index(*index)?
            }
        };
        current_value = next_value;
        current = VariantRef {
            metadata: current.metadata,
            value: current_value,
        };
    }
    VariantValue::create(value.metadata.raw(), current_value)
}

struct PathParser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> PathParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn parse(&mut self) -> Result<VariantPath, String> {
        if !self.consume_char(b'$') {
            return Err("Path must start with '$'".to_string());
        }
        let mut segments = Vec::new();
        while !self.is_end() {
            if self.peek_char() == b'.' {
                self.advance();
                let key = self.parse_unquoted_key()?;
                if key.is_empty() {
                    return Err("Expected key after '.'".to_string());
                }
                segments.push(VariantPathSegment::ObjectKey(key));
            } else if self.peek_char() == b'[' {
                if let Some(seg) = self.parse_bracket_segment()? {
                    segments.push(seg);
                }
            } else {
                return Err(format!(
                    "Unexpected character '{}' at position {}",
                    self.peek_char() as char,
                    self.pos
                ));
            }
        }
        Ok(VariantPath { segments })
    }

    fn parse_bracket_segment(&mut self) -> Result<Option<VariantPathSegment>, String> {
        self.expect_char(b'[')?;
        if self.is_end() {
            return Err("Unexpected end of path".to_string());
        }
        let ch = self.peek_char();
        if ch == b'\'' || ch == b'"' {
            let quote = self.advance();
            let key = self.parse_quoted_string(quote)?;
            self.expect_char(quote)?;
            self.expect_char(b']')?;
            return Ok(Some(VariantPathSegment::ObjectKey(key)));
        }
        let number = self.parse_number();
        if number.is_empty() {
            return Err("Expected array index".to_string());
        }
        self.expect_char(b']')?;
        let index = number
            .parse::<u32>()
            .map_err(|_| format!("Invalid array index: {}", number))?;
        Ok(Some(VariantPathSegment::ArrayIndex(index)))
    }

    fn parse_unquoted_key(&mut self) -> Result<String, String> {
        let mut out = String::new();
        while !self.is_end() {
            let c = self.peek_char();
            if c.is_ascii_alphanumeric() || c == b'_' {
                out.push(c as char);
                self.advance();
            } else {
                break;
            }
        }
        Ok(out)
    }

    fn parse_number(&mut self) -> String {
        let mut out = String::new();
        while !self.is_end() {
            let c = self.peek_char();
            if c.is_ascii_digit() {
                out.push(c as char);
                self.advance();
            } else {
                break;
            }
        }
        out
    }

    fn parse_quoted_string(&mut self, quote: u8) -> Result<String, String> {
        let mut out = String::new();
        while !self.is_end() && self.peek_char() != quote {
            let c = self.advance();
            if c == b'\\' && !self.is_end() {
                let escaped = self.advance();
                match escaped {
                    b'"' | b'\'' | b'\\' => out.push(escaped as char),
                    b'n' => out.push('\n'),
                    b't' => out.push('\t'),
                    b'r' => out.push('\r'),
                    other => out.push(other as char),
                }
            } else {
                out.push(c as char);
            }
        }
        Ok(out)
    }

    fn peek_char(&self) -> u8 {
        self.input.get(self.pos).copied().unwrap_or(0)
    }

    fn advance(&mut self) -> u8 {
        let ch = self.peek_char();
        self.pos = self.pos.saturating_add(1);
        ch
    }

    fn consume_char(&mut self, expected: u8) -> bool {
        if self.peek_char() == expected {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_char(&mut self, expected: u8) -> Result<(), String> {
        if self.consume_char(expected) {
            Ok(())
        } else {
            Err(format!(
                "Expected '{}' at position {}",
                expected as char, self.pos
            ))
        }
    }

    fn is_end(&self) -> bool {
        self.pos >= self.input.len()
    }
}

fn validate_metadata(metadata: &[u8]) -> Result<(), String> {
    if metadata.len() < METADATA_MIN_SIZE {
        return Err("Variant metadata is too short".to_string());
    }
    let header = metadata[0];
    let version = header & VERSION_MASK;
    if version != 1 {
        return Err(format!("Unsupported variant version: {}", version));
    }
    Ok(())
}

fn load_metadata(variant: &[u8]) -> Result<&[u8], String> {
    if variant.is_empty() {
        return Err("Variant is empty".to_string());
    }
    if variant.len() > VARIANT_MAX_SIZE {
        return Err(format!(
            "Variant size exceeds maximum limit: {} > {}",
            variant.len(),
            VARIANT_MAX_SIZE
        ));
    }
    let header = variant[0];
    let version = header & VERSION_MASK;
    if version != 1 {
        return Err(format!("Unsupported variant version: {}", version));
    }
    let offset_size = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    if offset_size < 1 || offset_size > 4 {
        return Err(format!(
            "Invalid offset size in variant metadata: {}",
            offset_size
        ));
    }
    if variant.len() < HEADER_SIZE + offset_size as usize {
        return Err("Variant too short to contain dict_size".to_string());
    }
    let dict_size = read_le_u32(&variant[HEADER_SIZE..], offset_size)? as usize;
    let offset_list_offset = HEADER_SIZE + offset_size as usize;
    let required_offset_list_size = (1 + dict_size) * offset_size as usize;
    let data_offset = offset_list_offset + required_offset_list_size;
    let last_offset_pos = offset_list_offset + dict_size * offset_size as usize;
    if last_offset_pos + offset_size as usize > variant.len() {
        return Err("Variant too short to contain all offsets".to_string());
    }
    let last_data_size = read_le_u32(&variant[last_offset_pos..], offset_size)? as usize;
    let end_offset = data_offset + last_data_size;
    if end_offset > variant.len() {
        return Err(format!(
            "Variant metadata end offset exceeds variant size: {} > {}",
            end_offset,
            variant.len()
        ));
    }
    Ok(&variant[..end_offset])
}

fn primitive_header(primitive: VariantPrimitiveType) -> u8 {
    (primitive as u8) << 2
}

fn read_le_u32(data: &[u8], size: u8) -> Result<u32, String> {
    if size == 0 || size > 4 {
        return Err("Invalid little-endian size".to_string());
    }
    if data.len() < size as usize {
        return Err("Not enough bytes to read value".to_string());
    }
    let mut out: u32 = 0;
    for i in 0..(size as usize) {
        out |= (data[i] as u32) << (8 * i);
    }
    Ok(out)
}

fn variant_to_json(
    variant: &VariantRef<'_>,
    out: &mut String,
    tz: Option<FixedOffset>,
) -> Result<(), String> {
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
                let date = NaiveDate::from_num_days_from_ce_opt(719163 + days)
                    .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                out.push('"');
                out.push_str(&date.format("%Y-%m-%d").to_string());
                out.push('"');
            }
            VariantPrimitiveType::TimestampTz => {
                let micros = variant.get_timestamp_micros()?;
                let tz = tz.unwrap_or_else(|| Local::now().offset().fix());
                let ts = format_timestamp_tz(micros, tz);
                out.push('"');
                out.push_str(&ts);
                out.push('"');
            }
            VariantPrimitiveType::TimestampNtz => {
                let micros = variant.get_timestamp_micros_ntz()?;
                let ts = format_timestamp_ntz(micros, 6);
                out.push('"');
                out.push_str(&ts);
                out.push('"');
            }
            VariantPrimitiveType::TimeNtz
            | VariantPrimitiveType::TimestampTzNanos
            | VariantPrimitiveType::TimestampNtzNanos => {
                return Err("Unsupported variant type".to_string());
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
                    return Err("Invalid offset in object".to_string());
                }
                let sub = VariantRef {
                    metadata: variant.metadata,
                    value: &variant.value[start..],
                };
                variant_to_json(&sub, out, tz)?;
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
                    return Err("Invalid offset in array".to_string());
                }
                let sub = VariantRef {
                    metadata: variant.metadata,
                    value: &variant.value[start..],
                };
                variant_to_json(&sub, out, tz)?;
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
    let abs = value.abs();
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

fn bytes_to_string(value: &[u8]) -> Result<String, String> {
    std::str::from_utf8(value)
        .map(|s| s.to_string())
        .map_err(|_| "Variant string is not valid UTF-8".to_string())
}

fn parse_bool_from_bytes(value: &[u8]) -> Result<bool, String> {
    let s =
        std::str::from_utf8(value).map_err(|_| "Variant string is not valid UTF-8".to_string())?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err("Failed to cast empty string to BOOLEAN".to_string());
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
    Err(format!("Failed to cast string '{}' to BOOLEAN", s))
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

fn timestamp_tz_to_local_naive(micros: i64, tz: FixedOffset) -> NaiveDateTime {
    let seconds = micros.div_euclid(1_000_000);
    let micros_rem = micros.rem_euclid(1_000_000) as u32;
    let dt_utc = DateTime::<Utc>::from_timestamp(seconds, micros_rem * 1000)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
    dt_utc.with_timezone(&tz).naive_local()
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

fn format_timestamp_tz(micros: i64, tz: FixedOffset) -> String {
    let seconds = micros.div_euclid(1_000_000);
    let micros_rem = micros.rem_euclid(1_000_000) as u32;
    let dt_utc = DateTime::<Utc>::from_timestamp(seconds, micros_rem * 1000)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
    let dt = dt_utc.with_timezone(&tz);
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    let frac = format_fraction_trim(micros_rem, 6);
    let offset = dt.format("%:z").to_string();
    if frac.is_empty() {
        format!("{}{}", base, offset)
    } else {
        format!("{}.{}{}", base, frac, offset)
    }
}

fn format_timestamp_ntz(micros: i64, width: usize) -> String {
    let seconds = micros.div_euclid(1_000_000);
    let micros_rem = micros.rem_euclid(1_000_000) as u32;
    let dt = DateTime::<Utc>::from_timestamp(seconds, micros_rem * 1000)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
        .naive_utc();
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    if width == 0 {
        return base;
    }
    let frac = format_fraction_fixed(micros_rem, width);
    format!("{}.{}", base, frac)
}

fn format_fraction_trim(value: u32, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let mut s = format!("{:0width$}", value, width = width);
    while s.ends_with('0') {
        s.pop();
    }
    s
}

fn format_fraction_fixed(value: u32, width: usize) -> String {
    format!("{:0width$}", value, width = width)
}
