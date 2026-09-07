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
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    DictionaryArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, ListArray, MapArray, NullArray,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Int32Type, TimeUnit};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, i256};
use std::sync::Arc;

use crate::exec::expr::agg::{AggregateAllocator, AggregateVec, FloatArrayView, IntArrayView};
use novarocks_types::largeint;

use super::hash::{
    canonical_f32_bits, canonical_f64_bits, combine_hash, hash_bytes_with_seed,
    hash_i128_with_seed, hash_null_with_seed, hash_u64_with_seed,
};
use super::key_layout::{CompressedKeyContext, compressed_key_is_valid};

pub enum DictionaryStringValues<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

impl<'a> DictionaryStringValues<'a> {
    fn len(&self) -> usize {
        match self {
            Self::Utf8(values) => values.len(),
            Self::LargeUtf8(values) => values.len(),
        }
    }

    fn is_null(&self, code: usize) -> bool {
        match self {
            Self::Utf8(values) => values.is_null(code),
            Self::LargeUtf8(values) => values.is_null(code),
        }
    }

    fn value(&self, code: usize) -> &'a str {
        match self {
            Self::Utf8(values) => values.value(code),
            Self::LargeUtf8(values) => values.value(code),
        }
    }
}

pub struct DictionaryGroupKeyView<'a> {
    dict: &'a DictionaryArray<Int32Type>,
    values: DictionaryStringValues<'a>,
    values_ptr: usize,
}

impl<'a> DictionaryGroupKeyView<'a> {
    pub fn new(dict: &'a DictionaryArray<Int32Type>) -> Result<Self, String> {
        let values = dict.values();
        let values_ptr = Arc::as_ptr(values) as *const () as usize;
        let values = match values.data_type() {
            DataType::Utf8 => {
                let values = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        "failed to downcast dictionary values to StringArray".to_string()
                    })?;
                if values.null_count() != 0 {
                    return Err("dictionary group key values must not contain nulls".to_string());
                }
                DictionaryStringValues::Utf8(values)
            }
            DataType::LargeUtf8 => {
                let values = values
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        "failed to downcast dictionary values to LargeStringArray".to_string()
                    })?;
                if values.null_count() != 0 {
                    return Err("dictionary group key values must not contain nulls".to_string());
                }
                DictionaryStringValues::LargeUtf8(values)
            }
            other => {
                return Err(format!(
                    "dictionary group key unsupported value type: {:?}",
                    other
                ));
            }
        };
        Ok(Self {
            dict,
            values,
            values_ptr,
        })
    }

    pub fn values_ptr(&self) -> usize {
        self.values_ptr
    }

    pub fn values_len(&self) -> usize {
        self.values.len()
    }

    pub fn code_at(&self, row: usize) -> Result<Option<usize>, String> {
        if self.dict.is_null(row) {
            return Ok(None);
        }
        let code = self.dict.keys().value(row);
        usize::try_from(code).map(Some).map_err(|_| {
            format!(
                "dictionary group key code must be non-negative, got {} at row {}",
                code, row
            )
        })
    }

    pub fn is_null(&self, row: usize) -> bool {
        self.dict.is_null(row)
    }

    pub fn value_as_bytes(&self, row: usize) -> Result<Option<&'a [u8]>, String> {
        let Some(code) = self.code_at(row)? else {
            return Ok(None);
        };
        self.value_bytes_for_code(code).map(Some)
    }

    pub fn value_bytes_for_code(&self, code: usize) -> Result<&'a [u8], String> {
        Ok(self.value_str_for_code(code)?.as_bytes())
    }

    pub fn value_str_for_code(&self, code: usize) -> Result<&'a str, String> {
        if code >= self.values_len() {
            return Err("dictionary group key code out of bounds".to_string());
        }
        if self.values.is_null(code) {
            return Err("dictionary group key value cannot be null when key is valid".to_string());
        }
        Ok(self.values.value(code))
    }
}

pub enum GroupKeyArrayView<'a> {
    Int(IntArrayView<'a>),
    Float(FloatArrayView<'a>),
    Boolean(&'a BooleanArray),
    Utf8(&'a StringArray),
    Dictionary(DictionaryGroupKeyView<'a>),
    Date32(&'a Date32Array),
    TimestampSecond(&'a TimestampSecondArray),
    TimestampMillisecond(&'a TimestampMillisecondArray),
    TimestampMicrosecond(&'a TimestampMicrosecondArray),
    TimestampNanosecond(&'a TimestampNanosecondArray),
    Decimal128(&'a Decimal128Array),
    Decimal256(&'a Decimal256Array),
    LargeIntBinary(&'a FixedSizeBinaryArray),
    ListUtf8 {
        list: &'a ListArray,
        values: &'a StringArray,
    },
    ListInt32 {
        list: &'a ListArray,
        values: &'a Int32Array,
    },
    Complex(&'a ArrayRef),
}

pub fn build_group_key_views<'a>(
    arrays: &'a [ArrayRef],
) -> Result<Vec<GroupKeyArrayView<'a>>, String> {
    let mut views = Vec::with_capacity(arrays.len());
    for array in arrays {
        let view = match array.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                GroupKeyArrayView::Int(IntArrayView::new(array)?)
            }
            DataType::Float32 | DataType::Float64 => {
                GroupKeyArrayView::Float(FloatArrayView::new(array)?)
            }
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                GroupKeyArrayView::Boolean(arr)
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
                GroupKeyArrayView::Utf8(arr)
            }
            DataType::Dictionary(key_type, value_type)
                if matches!(key_type.as_ref(), DataType::Int32)
                    && matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
            {
                let dict = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .ok_or_else(|| {
                        "failed to downcast to DictionaryArray<Int32Type>".to_string()
                    })?;
                GroupKeyArrayView::Dictionary(DictionaryGroupKeyView::new(dict)?)
            }
            DataType::Dictionary(key_type, value_type) => {
                return Err(format!(
                    "dictionary group key unsupported type: key={:?}, value={:?}",
                    key_type, value_type
                ));
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
                GroupKeyArrayView::Date32(arr)
            }
            DataType::Timestamp(unit, _tz) => match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                    GroupKeyArrayView::TimestampSecond(arr)
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMillisecondArray".to_string()
                        })?;
                    GroupKeyArrayView::TimestampMillisecond(arr)
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMicrosecondArray".to_string()
                        })?;
                    GroupKeyArrayView::TimestampMicrosecond(arr)
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampNanosecondArray".to_string()
                        })?;
                    GroupKeyArrayView::TimestampNanosecond(arr)
                }
            },
            DataType::Decimal128(_precision, _scale) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                GroupKeyArrayView::Decimal128(arr)
            }
            DataType::Decimal256(_precision, _scale) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
                GroupKeyArrayView::Decimal256(arr)
            }
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
                if arr.value_length() != largeint::LARGEINT_BYTE_WIDTH {
                    return Err(format!(
                        "group by type mismatch: expected FixedSizeBinary({}), got FixedSizeBinary({})",
                        largeint::LARGEINT_BYTE_WIDTH,
                        arr.value_length()
                    ));
                }
                GroupKeyArrayView::LargeIntBinary(arr)
            }
            DataType::List(field) if matches!(field.data_type(), DataType::Utf8) => {
                let list = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| "failed to downcast to ListArray".to_string())?;
                let values = list
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast list values to StringArray".to_string())?;
                GroupKeyArrayView::ListUtf8 { list, values }
            }
            DataType::List(field) if matches!(field.data_type(), DataType::Int32) => {
                let list = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| "failed to downcast to ListArray".to_string())?;
                let values = list
                    .values()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "failed to downcast list values to Int32Array".to_string())?;
                GroupKeyArrayView::ListInt32 { list, values }
            }
            DataType::Null => {
                return Err("group by type is null".to_string());
            }
            other => {
                let _ = other;
                GroupKeyArrayView::Complex(array)
            }
        };
        views.push(view);
    }
    Ok(views)
}

#[cfg(test)]
pub fn encode_group_key_row(array: &ArrayRef, row: usize) -> Result<Option<Vec<u8>>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    let mut out = Vec::new();
    encode_group_key_non_null_value(array, row, &mut out)?;
    Ok(Some(out))
}

pub(crate) fn encode_group_key_row_tracked(
    array: &ArrayRef,
    row: usize,
    allocator: AggregateAllocator,
) -> Result<Option<AggregateVec<u8>>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    let encoded_len = encoded_group_key_non_null_len(array, row)?;
    let mut out = AggregateVec::new_in(allocator.clone());
    out.try_reserve_exact(encoded_len)
        .map_err(|_| allocator.allocation_error("reserve canonical group-key bytes"))?;
    encode_group_key_non_null_value(array, row, &mut out)?;
    debug_assert_eq!(out.len(), encoded_len);
    Ok(Some(out))
}

trait KeyByteSink {
    fn push_byte(&mut self, value: u8) -> Result<(), String>;
    fn extend_bytes(&mut self, values: &[u8]) -> Result<(), String>;
}

impl KeyByteSink for Vec<u8> {
    fn push_byte(&mut self, value: u8) -> Result<(), String> {
        self.push(value);
        Ok(())
    }

    fn extend_bytes(&mut self, values: &[u8]) -> Result<(), String> {
        self.extend_from_slice(values);
        Ok(())
    }
}

impl KeyByteSink for AggregateVec<u8> {
    fn push_byte(&mut self, value: u8) -> Result<(), String> {
        if self.len() == self.capacity() {
            return Err("canonical group-key byte reservation was too small".to_string());
        }
        self.push(value);
        Ok(())
    }

    fn extend_bytes(&mut self, values: &[u8]) -> Result<(), String> {
        if values.len() > self.capacity().saturating_sub(self.len()) {
            return Err("canonical group-key byte reservation was too small".to_string());
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
    fn push_byte(&mut self, value: u8) -> Result<(), String> {
        self.extend_bytes(&[value])
    }

    fn extend_bytes(&mut self, values: &[u8]) -> Result<(), String> {
        let end = self.offset.saturating_add(values.len());
        if self.equal && self.expected.get(self.offset..end) != Some(values) {
            self.equal = false;
        }
        self.offset = end;
        Ok(())
    }
}

pub(crate) fn canonical_group_key_equals(
    array: &ArrayRef,
    row: usize,
    expected: &[u8],
) -> Result<bool, String> {
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
    fn push_byte(&mut self, value: u8) -> Result<(), String> {
        self.hash ^= value as u64;
        self.hash = self.hash.wrapping_mul(0x100000001b3);
        Ok(())
    }

    fn extend_bytes(&mut self, values: &[u8]) -> Result<(), String> {
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
) -> Result<Option<u64>, String> {
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
) -> Result<Option<u64>, String> {
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
    fn push_byte(&mut self, value: u8) -> Result<(), String> {
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

    fn extend_bytes(&mut self, values: &[u8]) -> Result<(), String> {
        for value in values {
            self.push_byte(*value)?;
        }
        Ok(())
    }
}

pub(crate) fn canonical_group_key_crc32_hash(
    array: &ArrayRef,
    row: usize,
) -> Result<Option<u32>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    let mut sink = Crc32KeyByteSink::new();
    encode_group_key_non_null_value(array, row, &mut sink)?;
    Ok(Some(sink.finish()))
}

fn encode_group_key_non_null_value<S: KeyByteSink>(
    array: &ArrayRef,
    row: usize,
    out: &mut S,
) -> Result<(), String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    "failed to downcast BooleanArray while encoding group key".to_string()
                })?;
            out.push_byte(1)?;
            out.push_byte(arr.value(row) as u8)?;
            Ok(())
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| {
                    "failed to downcast Int8Array while encoding group key".to_string()
                })?;
            out.push_byte(2)?;
            out.extend_bytes(&arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| {
                    "failed to downcast Int16Array while encoding group key".to_string()
                })?;
            out.push_byte(3)?;
            out.extend_bytes(&arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| {
                    "failed to downcast Int32Array while encoding group key".to_string()
                })?;
            out.push_byte(4)?;
            out.extend_bytes(&arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    "failed to downcast Int64Array while encoding group key".to_string()
                })?;
            out.push_byte(5)?;
            out.extend_bytes(&arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| {
                    "failed to downcast Float32Array while encoding group key".to_string()
                })?;
            out.push_byte(6)?;
            out.extend_bytes(&canonical_f32_bits(arr.value(row)).to_le_bytes())?;
            Ok(())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| {
                    "failed to downcast Float64Array while encoding group key".to_string()
                })?;
            out.push_byte(7)?;
            out.extend_bytes(&canonical_f64_bits(arr.value(row)).to_le_bytes())?;
            Ok(())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    "failed to downcast StringArray while encoding group key".to_string()
                })?;
            out.push_byte(8)?;
            let value = arr.value(row).as_bytes();
            let len = value.len() as u32;
            out.extend_bytes(&len.to_le_bytes())?;
            out.extend_bytes(value)?;
            Ok(())
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast BinaryArray while encoding group key".to_string()
                })?;
            out.push_byte(20)?;
            let value = arr.value(row);
            let len = u32::try_from(value.len())
                .map_err(|_| "group key Binary length overflow".to_string())?;
            out.extend_bytes(&len.to_le_bytes())?;
            out.extend_bytes(value)?;
            Ok(())
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast LargeBinaryArray while encoding group key".to_string()
                })?;
            out.push_byte(21)?;
            let value = arr.value(row);
            let len = u32::try_from(value.len())
                .map_err(|_| "group key LargeBinary length overflow".to_string())?;
            out.extend_bytes(&len.to_le_bytes())?;
            out.extend_bytes(value)?;
            Ok(())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    "failed to downcast Date32Array while encoding group key".to_string()
                })?;
            out.push_byte(9)?;
            out.extend_bytes(&arr.value(row).to_le_bytes())?;
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
                            "failed to downcast TimestampSecondArray while encoding group key"
                                .to_string()
                        })?;
                    arr.value(row)
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast TimestampMillisecondArray while encoding group key"
                                .to_string()
                        })?;
                    arr.value(row)
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast TimestampMicrosecondArray while encoding group key"
                                .to_string()
                        })?;
                    arr.value(row)
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast TimestampNanosecondArray while encoding group key"
                                .to_string()
                        })?;
                    arr.value(row)
                }
            };
            out.push_byte(marker)?;
            out.extend_bytes(&value.to_le_bytes())?;
            Ok(())
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    "failed to downcast Decimal128Array while encoding group key".to_string()
                })?;
            out.push_byte(14)?;
            out.extend_bytes(&arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    "failed to downcast Decimal256Array while encoding group key".to_string()
                })?;
            out.push_byte(19)?;
            out.extend_bytes(&arr.value(row).to_le_bytes())?;
            Ok(())
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast FixedSizeBinaryArray while encoding group key".to_string()
                })?;
            let value = largeint::i128_from_be_bytes(arr.value(row))
                .map_err(|e| format!("encode LARGEINT group key failed: {}", e))?;
            out.push_byte(15)?;
            out.extend_bytes(&value.to_le_bytes())?;
            Ok(())
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                "failed to downcast ListArray while encoding group key".to_string()
            })?;
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = list.values();
            out.push_byte(16)?;
            let len = u32::try_from(end.saturating_sub(start))
                .map_err(|_| "group key list length overflow".to_string())?;
            out.extend_bytes(&len.to_le_bytes())?;
            for idx in start..end {
                encode_nullable_nested(values, idx, out, "group key list item")?;
            }
            Ok(())
        }
        DataType::Struct(_) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    "failed to downcast StructArray while encoding group key".to_string()
                })?;
            out.push_byte(17)?;
            let field_count = u32::try_from(struct_arr.num_columns())
                .map_err(|_| "group key struct field count overflow".to_string())?;
            out.extend_bytes(&field_count.to_le_bytes())?;
            for column in struct_arr.columns() {
                encode_nullable_nested(column, row, out, "group key struct field")?;
            }
            Ok(())
        }
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                "failed to downcast MapArray while encoding group key".to_string()
            })?;
            let offsets = map.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let entries = map.entries();
            let keys = entries.column(0).clone();
            let values = entries.column(1).clone();
            out.push_byte(18)?;
            let len = u32::try_from(end.saturating_sub(start))
                .map_err(|_| "group key map length overflow".to_string())?;
            out.extend_bytes(&len.to_le_bytes())?;
            for idx in start..end {
                encode_nullable_nested(&keys, idx, out, "group key map key")?;
                encode_nullable_nested(&values, idx, out, "group key map value")?;
            }
            Ok(())
        }
        other => Err(format!(
            "group key encode unsupported input type: {:?}",
            other
        )),
    }
}

fn encode_nullable_nested<S: KeyByteSink>(
    array: &ArrayRef,
    row: usize,
    out: &mut S,
    context: &str,
) -> Result<(), String> {
    if matches!(array.data_type(), DataType::Null) || array.is_null(row) {
        out.push_byte(0)?;
        return Ok(());
    }
    out.push_byte(1)?;
    let len = encoded_group_key_non_null_len(array, row)?;
    let len = u32::try_from(len).map_err(|_| format!("{context} length overflow"))?;
    out.extend_bytes(&len.to_le_bytes())?;
    encode_group_key_non_null_value(array, row, out)
}

fn encoded_group_key_non_null_len(array: &ArrayRef, row: usize) -> Result<usize, String> {
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
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    "failed to downcast StringArray while sizing group key".to_string()
                })?;
            Ok(5usize.saturating_add(array.value(row).len()))
        }
        DataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast BinaryArray while sizing group key".to_string()
                })?;
            Ok(5usize.saturating_add(array.value(row).len()))
        }
        DataType::LargeBinary => {
            let array = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast LargeBinaryArray while sizing group key".to_string()
                })?;
            Ok(5usize.saturating_add(array.value(row).len()))
        }
        DataType::List(_) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "failed to downcast ListArray while sizing group key".to_string())?;
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = list.values();
            nested_sequence_encoded_len(values, start..end, 5)
        }
        DataType::Struct(_) => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    "failed to downcast StructArray while sizing group key".to_string()
                })?;
            values.columns().iter().try_fold(5usize, |total, column| {
                nested_value_encoded_len(column, row)
                    .and_then(|len| checked_encoded_len_add(total, len))
            })
        }
        DataType::Map(_, _) => {
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "failed to downcast MapArray while sizing group key".to_string())?;
            let offsets = map.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let entries = map.entries();
            let keys = entries.column(0);
            let values = entries.column(1);
            (start..end).try_fold(5usize, |total, index| {
                let key_len = nested_value_encoded_len(keys, index)?;
                let value_len = nested_value_encoded_len(values, index)?;
                checked_encoded_len_add(total, key_len)
                    .and_then(|total| checked_encoded_len_add(total, value_len))
            })
        }
        other => Err(format!(
            "group key encode unsupported input type: {:?}",
            other
        )),
    }
}

fn nested_sequence_encoded_len(
    values: &ArrayRef,
    mut rows: std::ops::Range<usize>,
    initial: usize,
) -> Result<usize, String> {
    rows.try_fold(initial, |total, row| {
        nested_value_encoded_len(values, row).and_then(|len| checked_encoded_len_add(total, len))
    })
}

fn nested_value_encoded_len(array: &ArrayRef, row: usize) -> Result<usize, String> {
    if matches!(array.data_type(), DataType::Null) || array.is_null(row) {
        Ok(1)
    } else {
        encoded_group_key_non_null_len(array, row)?
            .checked_add(5)
            .ok_or_else(|| "canonical group-key length overflow".to_string())
    }
}

fn checked_encoded_len_add(left: usize, right: usize) -> Result<usize, String> {
    left.checked_add(right)
        .ok_or_else(|| "canonical group-key length overflow".to_string())
}

pub(crate) fn decode_group_key_rows(
    data_type: &DataType,
    rows: &[Option<&[u8]>],
) -> Result<ArrayRef, String> {
    macro_rules! fixed_array {
        ($marker:expr, $size:expr, $array:ty, $decode:expr) => {{
            let values = rows
                .iter()
                .map(|row| {
                    row.as_ref()
                        .map(|row| decode_fixed_value(row, $marker, $size, $decode))
                        .transpose()
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Arc::new(<$array>::from(values)) as ArrayRef)
        }};
    }

    match data_type {
        DataType::Null => {
            if rows.iter().any(Option::is_some) {
                return Err("canonical Null group-key contains a non-null value".to_string());
            }
            Ok(Arc::new(NullArray::new(rows.len())))
        }
        DataType::Boolean => fixed_array!(1, 1, BooleanArray, |bytes: &[u8]| Ok(bytes[0] != 0)),
        DataType::Int8 => fixed_array!(2, 1, Int8Array, |bytes: &[u8]| Ok(bytes[0] as i8)),
        DataType::Int16 => fixed_array!(3, 2, Int16Array, |bytes: &[u8]| {
            Ok(i16::from_le_bytes(bytes.try_into().expect("i16 bytes")))
        }),
        DataType::Int32 => fixed_array!(4, 4, Int32Array, |bytes: &[u8]| {
            Ok(i32::from_le_bytes(bytes.try_into().expect("i32 bytes")))
        }),
        DataType::Int64 => fixed_array!(5, 8, Int64Array, |bytes: &[u8]| {
            Ok(i64::from_le_bytes(bytes.try_into().expect("i64 bytes")))
        }),
        DataType::Float32 => fixed_array!(6, 4, Float32Array, |bytes: &[u8]| {
            Ok(f32::from_bits(u32::from_le_bytes(
                bytes.try_into().expect("f32 bytes"),
            )))
        }),
        DataType::Float64 => fixed_array!(7, 8, Float64Array, |bytes: &[u8]| {
            Ok(f64::from_bits(u64::from_le_bytes(
                bytes.try_into().expect("f64 bytes"),
            )))
        }),
        DataType::Utf8 => {
            let values = decode_variable_rows(rows, 8)?;
            let values = values
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            std::str::from_utf8(value)
                                .map_err(|error| format!("invalid canonical utf8 key: {error}"))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Arc::new(StringArray::from(values)))
        }
        DataType::Binary => Ok(Arc::new(BinaryArray::from(decode_variable_rows(rows, 20)?))),
        DataType::LargeBinary => Ok(Arc::new(LargeBinaryArray::from(decode_variable_rows(
            rows, 21,
        )?))),
        DataType::Date32 => fixed_array!(9, 4, Date32Array, |bytes: &[u8]| {
            Ok(i32::from_le_bytes(bytes.try_into().expect("date32 bytes")))
        }),
        DataType::Timestamp(unit, timezone) => {
            let marker = match unit {
                TimeUnit::Second => 10,
                TimeUnit::Millisecond => 11,
                TimeUnit::Microsecond => 12,
                TimeUnit::Nanosecond => 13,
            };
            let values = rows
                .iter()
                .map(|row| {
                    row.as_ref()
                        .map(|row| {
                            decode_fixed_value(row, marker, 8, |bytes| {
                                Ok(i64::from_le_bytes(
                                    bytes.try_into().expect("timestamp bytes"),
                                ))
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, String>>()?;
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::from(values).with_timezone_opt(timezone.clone()))
                }
                TimeUnit::Millisecond => Arc::new(
                    TimestampMillisecondArray::from(values).with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::from(values).with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Nanosecond => Arc::new(
                    TimestampNanosecondArray::from(values).with_timezone_opt(timezone.clone()),
                ),
            };
            Ok(array)
        }
        DataType::Decimal128(precision, scale) => {
            let values = rows
                .iter()
                .map(|row| {
                    row.as_ref()
                        .map(|row| {
                            decode_fixed_value(row, 14, 16, |bytes| {
                                Ok(i128::from_le_bytes(
                                    bytes.try_into().expect("decimal128 bytes"),
                                ))
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| error.to_string())?,
            ))
        }
        DataType::Decimal256(precision, scale) => {
            let values = rows
                .iter()
                .map(|row| {
                    row.as_ref()
                        .map(|row| {
                            decode_fixed_value(row, 19, 32, |bytes| {
                                Ok(i256::from_le_bytes(
                                    bytes.try_into().expect("decimal256 bytes"),
                                ))
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Arc::new(
                Decimal256Array::from(values)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| error.to_string())?,
            ))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let values = rows
                .iter()
                .map(|row| {
                    row.as_ref()
                        .map(|row| {
                            decode_fixed_value(row, 15, 16, |bytes| {
                                Ok(i128::from_le_bytes(
                                    bytes.try_into().expect("largeint bytes"),
                                ))
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, String>>()?;
            largeint::array_from_i128(&values)
        }
        DataType::List(field) => decode_list_rows(field, rows),
        DataType::Struct(fields) => decode_struct_rows(fields, rows),
        DataType::Map(field, ordered) => decode_map_rows(field, *ordered, rows),
        other => Err(format!(
            "canonical group-key decode unsupported type: {other:?}"
        )),
    }
}

fn decode_fixed_value<T>(
    bytes: &[u8],
    marker: u8,
    payload_len: usize,
    decode: impl FnOnce(&[u8]) -> Result<T, String>,
) -> Result<T, String> {
    if bytes.first().copied() != Some(marker) || bytes.len() != payload_len.saturating_add(1) {
        return Err("invalid canonical fixed-width group key".to_string());
    }
    decode(&bytes[1..])
}

fn decode_variable_rows<'a>(
    rows: &'a [Option<&'a [u8]>],
    marker: u8,
) -> Result<Vec<Option<&'a [u8]>>, String> {
    rows.iter()
        .map(|row| {
            row.as_ref()
                .map(|row| decode_variable_value(row, marker))
                .transpose()
        })
        .collect()
}

fn decode_variable_value(bytes: &[u8], marker: u8) -> Result<&[u8], String> {
    let mut cursor = EncodedCursor::new(bytes);
    cursor.expect_marker(marker)?;
    let len = cursor.read_u32()? as usize;
    let value = cursor.read_slice(len)?;
    cursor.finish()?;
    Ok(value)
}

fn decode_list_rows(
    field: &Arc<arrow::datatypes::Field>,
    rows: &[Option<&[u8]>],
) -> Result<ArrayRef, String> {
    let mut offsets = Vec::with_capacity(rows.len().saturating_add(1));
    offsets.push(0_i32);
    let mut nulls = NullBufferBuilder::new(rows.len());
    let mut children = Vec::new();
    for row in rows {
        match row {
            None => nulls.append_null(),
            Some(row) => {
                nulls.append_non_null();
                let mut cursor = EncodedCursor::new(row);
                cursor.expect_marker(16)?;
                let count = cursor.read_u32()? as usize;
                for _ in 0..count {
                    children.push(cursor.read_nested()?);
                }
                cursor.finish()?;
            }
        }
        offsets.push(
            i32::try_from(children.len()).map_err(|_| "list key offset overflow".to_string())?,
        );
    }
    let child_array = decode_group_key_rows(field.data_type(), &children)?;
    Ok(Arc::new(ListArray::new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        child_array,
        nulls.finish(),
    )))
}

fn decode_struct_rows(
    fields: &arrow::datatypes::Fields,
    rows: &[Option<&[u8]>],
) -> Result<ArrayRef, String> {
    let mut columns: Vec<Vec<Option<&[u8]>>> = (0..fields.len())
        .map(|_| Vec::with_capacity(rows.len()))
        .collect();
    let mut nulls = NullBufferBuilder::new(rows.len());
    for row in rows {
        match row {
            None => {
                nulls.append_null();
                for column in &mut columns {
                    column.push(None);
                }
            }
            Some(row) => {
                nulls.append_non_null();
                let mut cursor = EncodedCursor::new(row);
                cursor.expect_marker(17)?;
                let count = cursor.read_u32()? as usize;
                if count != fields.len() {
                    return Err("canonical struct group-key field count mismatch".to_string());
                }
                for column in &mut columns {
                    column.push(cursor.read_nested()?);
                }
                cursor.finish()?;
            }
        }
    }
    let arrays = fields
        .iter()
        .zip(columns)
        .map(|(field, rows)| decode_group_key_rows(field.data_type(), &rows))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(StructArray::new(
        fields.clone(),
        arrays,
        nulls.finish(),
    )))
}

fn decode_map_rows(
    field: &Arc<arrow::datatypes::Field>,
    ordered: bool,
    rows: &[Option<&[u8]>],
) -> Result<ArrayRef, String> {
    let DataType::Struct(entry_fields) = field.data_type() else {
        return Err("map group key requires struct entry type".to_string());
    };
    if entry_fields.len() != 2 {
        return Err("map group key requires key/value entry fields".to_string());
    }
    let mut offsets = Vec::with_capacity(rows.len().saturating_add(1));
    offsets.push(0_i32);
    let mut nulls = NullBufferBuilder::new(rows.len());
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for row in rows {
        match row {
            None => nulls.append_null(),
            Some(row) => {
                nulls.append_non_null();
                let mut cursor = EncodedCursor::new(row);
                cursor.expect_marker(18)?;
                let count = cursor.read_u32()? as usize;
                for _ in 0..count {
                    keys.push(cursor.read_nested()?);
                    values.push(cursor.read_nested()?);
                }
                cursor.finish()?;
            }
        }
        offsets.push(i32::try_from(keys.len()).map_err(|_| "map key offset overflow".to_string())?);
    }
    let key_array = decode_group_key_rows(entry_fields[0].data_type(), &keys)?;
    let value_array = decode_group_key_rows(entry_fields[1].data_type(), &values)?;
    let entries = StructArray::new(entry_fields.clone(), vec![key_array, value_array], None);
    Ok(Arc::new(MapArray::new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        entries,
        nulls.finish(),
        ordered,
    )))
}

struct EncodedCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> EncodedCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_marker(&mut self, expected: u8) -> Result<(), String> {
        let actual = self.read_u8()?;
        if actual != expected {
            return Err(format!(
                "canonical group-key marker mismatch: expected {expected}, got {actual}"
            ));
        }
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8, String> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| "truncated canonical group key".to_string())?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, String> {
        let bytes = self.read_slice(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("u32 bytes")))
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| "canonical group-key cursor overflow".to_string())?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| "truncated canonical group key".to_string())?;
        self.offset = end;
        Ok(value)
    }

    fn read_nested(&mut self) -> Result<Option<&'a [u8]>, String> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let len = self.read_u32()? as usize;
                self.read_slice(len).map(Some)
            }
            marker => Err(format!("invalid canonical nested marker: {marker}")),
        }
    }

    fn finish(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing bytes in canonical group key".to_string())
        }
    }
}

pub fn list_utf8_row_value(
    list: &ListArray,
    values: &StringArray,
    row: usize,
) -> Option<Vec<Option<String>>> {
    if list.is_null(row) {
        return None;
    }
    let offsets = list.value_offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let mut out = Vec::with_capacity(end.saturating_sub(start));
    for idx in start..end {
        if values.is_null(idx) {
            out.push(None);
        } else {
            out.push(Some(values.value(idx).to_string()));
        }
    }
    Some(out)
}

pub fn list_int32_row_value(
    list: &ListArray,
    values: &Int32Array,
    row: usize,
) -> Option<Vec<Option<i32>>> {
    if list.is_null(row) {
        return None;
    }
    let offsets = list.value_offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let mut out = Vec::with_capacity(end.saturating_sub(start));
    for idx in start..end {
        if values.is_null(idx) {
            out.push(None);
        } else {
            out.push(Some(values.value(idx)));
        }
    }
    Some(out)
}

fn hash_list_utf8_row(list: &ListArray, values: &StringArray, row: usize, seed: u64) -> u64 {
    let offsets = list.value_offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let mut hash = hash_u64_with_seed(seed, (end.saturating_sub(start)) as u64);
    for idx in start..end {
        if values.is_null(idx) {
            let marker = hash_u64_with_seed(seed, 0);
            hash = combine_hash(hash, marker);
            continue;
        }
        let marker = hash_u64_with_seed(seed, 1);
        hash = combine_hash(hash, marker);
        let value_hash = hash_bytes_with_seed(seed, values.value(idx).as_bytes());
        hash = combine_hash(hash, value_hash);
    }
    hash
}

fn hash_list_int32_row(list: &ListArray, values: &Int32Array, row: usize, seed: u64) -> u64 {
    let offsets = list.value_offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let mut hash = hash_u64_with_seed(seed, (end.saturating_sub(start)) as u64);
    for idx in start..end {
        if values.is_null(idx) {
            let marker = hash_u64_with_seed(seed, 0);
            hash = combine_hash(hash, marker);
            continue;
        }
        let marker = hash_u64_with_seed(seed, 1);
        hash = combine_hash(hash, marker);
        let value_hash = hash_u64_with_seed(seed, values.value(idx) as i64 as u64);
        hash = combine_hash(hash, value_hash);
    }
    hash
}

fn largeint_row_value(arr: &FixedSizeBinaryArray, row: usize) -> Result<Option<i128>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    largeint::i128_from_be_bytes(arr.value(row))
        .map(Some)
        .map_err(|e| format!("group key LARGEINT decode failed at row {}: {}", row, e))
}

pub fn build_one_number_hashes(
    view: &GroupKeyArrayView<'_>,
    num_rows: usize,
    seed: u64,
) -> Result<Vec<u64>, String> {
    let mut hashes = Vec::with_capacity(num_rows);
    let null_hash = hash_null_with_seed(seed);
    match view {
        GroupKeyArrayView::Int(view) => match view {
            IntArrayView::Int64(arr) => {
                if arr.null_count() == 0 {
                    for value in arr.values() {
                        let value_hash = hash_u64_with_seed(seed, *value as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes.push(combine_hash(seed, null_hash));
                        } else {
                            let value = arr.value(row);
                            let value_hash = hash_u64_with_seed(seed, value as u64);
                            hashes.push(combine_hash(seed, value_hash));
                        }
                    }
                }
            }
            IntArrayView::Int32(arr) => {
                if arr.null_count() == 0 {
                    for value in arr.values() {
                        let value_hash = hash_u64_with_seed(seed, *value as i64 as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes.push(combine_hash(seed, null_hash));
                        } else {
                            let value = arr.value(row) as i64;
                            let value_hash = hash_u64_with_seed(seed, value as u64);
                            hashes.push(combine_hash(seed, value_hash));
                        }
                    }
                }
            }
            IntArrayView::Int16(arr) => {
                if arr.null_count() == 0 {
                    for value in arr.values() {
                        let value_hash = hash_u64_with_seed(seed, *value as i64 as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes.push(combine_hash(seed, null_hash));
                        } else {
                            let value = arr.value(row) as i64;
                            let value_hash = hash_u64_with_seed(seed, value as u64);
                            hashes.push(combine_hash(seed, value_hash));
                        }
                    }
                }
            }
            IntArrayView::Int8(arr) => {
                if arr.null_count() == 0 {
                    for value in arr.values() {
                        let value_hash = hash_u64_with_seed(seed, *value as i64 as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes.push(combine_hash(seed, null_hash));
                        } else {
                            let value = arr.value(row) as i64;
                            let value_hash = hash_u64_with_seed(seed, value as u64);
                            hashes.push(combine_hash(seed, value_hash));
                        }
                    }
                }
            }
        },
        GroupKeyArrayView::Float(view) => match view {
            FloatArrayView::Float64(arr) => {
                if arr.null_count() == 0 {
                    for value in arr.values() {
                        let bits = canonical_f64_bits(*value);
                        let value_hash = hash_u64_with_seed(seed, bits);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes.push(combine_hash(seed, null_hash));
                        } else {
                            let bits = canonical_f64_bits(arr.value(row));
                            let value_hash = hash_u64_with_seed(seed, bits);
                            hashes.push(combine_hash(seed, value_hash));
                        }
                    }
                }
            }
            FloatArrayView::Float32(arr) => {
                if arr.null_count() == 0 {
                    for value in arr.values() {
                        let bits = canonical_f32_bits(*value);
                        let value_hash = hash_u64_with_seed(seed, bits as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes.push(combine_hash(seed, null_hash));
                        } else {
                            let bits = canonical_f32_bits(arr.value(row));
                            let value_hash = hash_u64_with_seed(seed, bits as u64);
                            hashes.push(combine_hash(seed, value_hash));
                        }
                    }
                }
            }
        },
        GroupKeyArrayView::Boolean(arr) => {
            if arr.null_count() == 0 {
                for row in 0..num_rows {
                    let value = arr.value(row);
                    let hash_value = if value { 1u64 } else { 0u64 };
                    let value_hash = hash_u64_with_seed(seed, hash_value);
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes.push(combine_hash(seed, null_hash));
                    } else {
                        let value = arr.value(row);
                        let hash_value = if value { 1u64 } else { 0u64 };
                        let value_hash = hash_u64_with_seed(seed, hash_value);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                }
            }
        }
        GroupKeyArrayView::Date32(arr) => {
            if arr.null_count() == 0 {
                for value in arr.values() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes.push(combine_hash(seed, null_hash));
                    } else {
                        let value = arr.value(row);
                        let value_hash = hash_u64_with_seed(seed, value as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampSecond(arr) => {
            if arr.null_count() == 0 {
                for value in arr.values() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes.push(combine_hash(seed, null_hash));
                    } else {
                        let value = arr.value(row);
                        let value_hash = hash_u64_with_seed(seed, value as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampMillisecond(arr) => {
            if arr.null_count() == 0 {
                for value in arr.values() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes.push(combine_hash(seed, null_hash));
                    } else {
                        let value = arr.value(row);
                        let value_hash = hash_u64_with_seed(seed, value as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampMicrosecond(arr) => {
            if arr.null_count() == 0 {
                for value in arr.values() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes.push(combine_hash(seed, null_hash));
                    } else {
                        let value = arr.value(row);
                        let value_hash = hash_u64_with_seed(seed, value as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampNanosecond(arr) => {
            if arr.null_count() == 0 {
                for value in arr.values() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes.push(combine_hash(seed, null_hash));
                    } else {
                        let value = arr.value(row);
                        let value_hash = hash_u64_with_seed(seed, value as u64);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                }
            }
        }
        GroupKeyArrayView::Decimal128(arr) => {
            if arr.null_count() == 0 {
                for value in arr.values() {
                    let value_hash = hash_i128_with_seed(seed, *value);
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes.push(combine_hash(seed, null_hash));
                    } else {
                        let value = arr.value(row);
                        let value_hash = hash_i128_with_seed(seed, value);
                        hashes.push(combine_hash(seed, value_hash));
                    }
                }
            }
        }
        GroupKeyArrayView::Decimal256(arr) => {
            if arr.null_count() == 0 {
                for value in arr.values() {
                    let value_hash = hash_bytes_with_seed(seed, &value.to_le_bytes());
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes.push(combine_hash(seed, null_hash));
                    } else {
                        let value_hash = hash_bytes_with_seed(seed, &arr.value(row).to_le_bytes());
                        hashes.push(combine_hash(seed, value_hash));
                    }
                }
            }
        }
        GroupKeyArrayView::LargeIntBinary(arr) => {
            if arr.null_count() == 0 {
                for row in 0..num_rows {
                    let value = largeint_row_value(arr, row)?
                        .ok_or_else(|| "group key LARGEINT unexpected null".to_string())?;
                    let value_hash = hash_i128_with_seed(seed, value);
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if let Some(value) = largeint_row_value(arr, row)? {
                        let value_hash = hash_i128_with_seed(seed, value);
                        hashes.push(combine_hash(seed, value_hash));
                    } else {
                        hashes.push(combine_hash(seed, null_hash));
                    }
                }
            }
        }
        GroupKeyArrayView::Utf8(_)
        | GroupKeyArrayView::Dictionary(_)
        | GroupKeyArrayView::ListUtf8 { .. }
        | GroupKeyArrayView::ListInt32 { .. }
        | GroupKeyArrayView::Complex(_) => {
            return Err("one number key does not support variable-length types".to_string());
        }
    }
    Ok(hashes)
}

pub fn build_group_key_hashes(
    views: &[GroupKeyArrayView<'_>],
    num_rows: usize,
    seed: u64,
) -> Result<Vec<u64>, String> {
    let mut hashes = vec![seed; num_rows];
    for view in views {
        hash_column(view, num_rows, seed, &mut hashes)?;
    }
    Ok(hashes)
}

#[inline]
fn combine_hash_at(hashes: &mut [u64], row: usize, value_hash: u64) {
    hashes[row] = combine_hash(hashes[row], value_hash);
}

fn hash_column(
    view: &GroupKeyArrayView<'_>,
    num_rows: usize,
    seed: u64,
    hashes: &mut [u64],
) -> Result<(), String> {
    let null_hash = hash_null_with_seed(seed);
    match view {
        GroupKeyArrayView::Int(view) => match view {
            IntArrayView::Int64(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let value_hash = hash_u64_with_seed(seed, *value as u64);
                        combine_hash_at(hashes, row, value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            combine_hash_at(hashes, row, null_hash);
                        } else {
                            let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                            combine_hash_at(hashes, row, value_hash);
                        }
                    }
                }
            }
            IntArrayView::Int32(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let value_hash = hash_u64_with_seed(seed, *value as i64 as u64);
                        combine_hash_at(hashes, row, value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            combine_hash_at(hashes, row, null_hash);
                        } else {
                            let value_hash = hash_u64_with_seed(seed, arr.value(row) as i64 as u64);
                            combine_hash_at(hashes, row, value_hash);
                        }
                    }
                }
            }
            IntArrayView::Int16(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let value_hash = hash_u64_with_seed(seed, *value as i64 as u64);
                        combine_hash_at(hashes, row, value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            combine_hash_at(hashes, row, null_hash);
                        } else {
                            let value_hash = hash_u64_with_seed(seed, arr.value(row) as i64 as u64);
                            combine_hash_at(hashes, row, value_hash);
                        }
                    }
                }
            }
            IntArrayView::Int8(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let value_hash = hash_u64_with_seed(seed, *value as i64 as u64);
                        combine_hash_at(hashes, row, value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            combine_hash_at(hashes, row, null_hash);
                        } else {
                            let value_hash = hash_u64_with_seed(seed, arr.value(row) as i64 as u64);
                            combine_hash_at(hashes, row, value_hash);
                        }
                    }
                }
            }
        },
        GroupKeyArrayView::Float(view) => match view {
            FloatArrayView::Float64(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let bits = canonical_f64_bits(*value);
                        let value_hash = hash_u64_with_seed(seed, bits);
                        combine_hash_at(hashes, row, value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            combine_hash_at(hashes, row, null_hash);
                        } else {
                            let bits = canonical_f64_bits(arr.value(row));
                            let value_hash = hash_u64_with_seed(seed, bits);
                            combine_hash_at(hashes, row, value_hash);
                        }
                    }
                }
            }
            FloatArrayView::Float32(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let bits = canonical_f32_bits(*value) as u64;
                        let value_hash = hash_u64_with_seed(seed, bits);
                        combine_hash_at(hashes, row, value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            combine_hash_at(hashes, row, null_hash);
                        } else {
                            let bits = canonical_f32_bits(arr.value(row)) as u64;
                            let value_hash = hash_u64_with_seed(seed, bits);
                            combine_hash_at(hashes, row, value_hash);
                        }
                    }
                }
            }
        },
        GroupKeyArrayView::Boolean(arr) => {
            if arr.null_count() == 0 {
                for row in 0..num_rows {
                    let value = if arr.value(row) { 1u64 } else { 0u64 };
                    let value_hash = hash_u64_with_seed(seed, value);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value = if arr.value(row) { 1u64 } else { 0u64 };
                        let value_hash = hash_u64_with_seed(seed, value);
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Utf8(arr) => {
            if arr.null_count() == 0 {
                for row in 0..num_rows {
                    let value_hash = hash_bytes_with_seed(seed, arr.value(row).as_bytes());
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_bytes_with_seed(seed, arr.value(row).as_bytes());
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Dictionary(dict) => {
            let mut value_hashes = vec![None; dict.values_len()];
            for row in 0..num_rows {
                let Some(code) = dict.code_at(row)? else {
                    combine_hash_at(hashes, row, null_hash);
                    continue;
                };
                let value_hash = match value_hashes.get(code).copied().flatten() {
                    Some(hash) => hash,
                    None => {
                        let hash = hash_bytes_with_seed(seed, dict.value_bytes_for_code(code)?);
                        let slot = value_hashes
                            .get_mut(code)
                            .ok_or_else(|| "dictionary group key code out of bounds".to_string())?;
                        *slot = Some(hash);
                        hash
                    }
                };
                combine_hash_at(hashes, row, value_hash);
            }
        }
        GroupKeyArrayView::Date32(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampSecond(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampMillisecond(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampMicrosecond(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampNanosecond(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Decimal128(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_i128_with_seed(seed, *value);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_i128_with_seed(seed, arr.value(row));
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Decimal256(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_bytes_with_seed(seed, &value.to_le_bytes());
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_bytes_with_seed(seed, &arr.value(row).to_le_bytes());
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::LargeIntBinary(arr) => {
            if arr.null_count() == 0 {
                for row in 0..num_rows {
                    let value = largeint_row_value(arr, row)?
                        .ok_or_else(|| "group key LARGEINT unexpected null".to_string())?;
                    let value_hash = hash_i128_with_seed(seed, value);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if let Some(value) = largeint_row_value(arr, row)? {
                        let value_hash = hash_i128_with_seed(seed, value);
                        combine_hash_at(hashes, row, value_hash);
                    } else {
                        combine_hash_at(hashes, row, null_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::ListUtf8 { list, values } => {
            if list.null_count() == 0 {
                for row in 0..num_rows {
                    let value_hash = hash_list_utf8_row(list, values, row, seed);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if list.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_list_utf8_row(list, values, row, seed);
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::ListInt32 { list, values } => {
            if list.null_count() == 0 {
                for row in 0..num_rows {
                    let value_hash = hash_list_int32_row(list, values, row, seed);
                    combine_hash_at(hashes, row, value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if list.is_null(row) {
                        combine_hash_at(hashes, row, null_hash);
                    } else {
                        let value_hash = hash_list_int32_row(list, values, row, seed);
                        combine_hash_at(hashes, row, value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Complex(array) => {
            for row in 0..num_rows {
                match canonical_group_key_hash_with_seed(array, row, seed)? {
                    Some(value_hash) => combine_hash_at(hashes, row, value_hash),
                    None => {
                        combine_hash_at(hashes, row, null_hash);
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn build_compressed_flags(
    ctx: &CompressedKeyContext,
    views: &[GroupKeyArrayView<'_>],
    num_rows: usize,
) -> Result<Vec<bool>, String> {
    let mut flags = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        flags.push(compressed_key_is_valid(ctx, views, row)?);
    }
    Ok(flags)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;
    use arrow::array::{
        ArrayRef, DictionaryArray, Int32Builder, Int64Array, MapBuilder, MapFieldNames,
        StringBuilder,
    };
    use arrow::datatypes::{Field, Fields, Int32Type};
    use arrow_buffer::NullBuffer;
    use std::sync::Arc;

    fn dict_utf8(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(values.into_iter().collect::<DictionaryArray<Int32Type>>())
    }

    #[test]
    fn build_group_key_views_builds_dictionary_view_for_utf8_dictionary() {
        let array = dict_utf8(vec![Some("PAID"), None, Some("NEW"), Some("PAID")]);
        let arrays = [array];
        let views = build_group_key_views(&arrays).expect("views");
        let GroupKeyArrayView::Dictionary(dict) = &views[0] else {
            panic!("expected dictionary view");
        };
        assert_ne!(dict.values_ptr(), 0);
        assert_eq!(
            dict.value_as_bytes(0).expect("value"),
            Some(b"PAID".as_slice())
        );
        assert_eq!(dict.value_as_bytes(1).expect("null"), None);
        assert_eq!(
            dict.value_as_bytes(2).expect("value"),
            Some(b"NEW".as_slice())
        );
    }

    #[test]
    fn dictionary_group_key_hashes_by_logical_value_across_local_codes() {
        let first = dict_utf8(vec![Some("A"), Some("B")]);
        let second = dict_utf8(vec![Some("B"), Some("A")]);
        let first_arrays = [first];
        let second_arrays = [second];
        let first_views = build_group_key_views(&first_arrays).expect("first views");
        let second_views = build_group_key_views(&second_arrays).expect("second views");

        let first_hashes = build_group_key_hashes(&first_views, 2, 0x1234).expect("first hashes");
        let second_hashes =
            build_group_key_hashes(&second_views, 2, 0x1234).expect("second hashes");

        assert_eq!(
            first_hashes[0], second_hashes[1],
            "A must hash by value, not code"
        );
        assert_eq!(
            first_hashes[1], second_hashes[0],
            "B must hash by value, not code"
        );
    }

    #[test]
    fn build_group_key_views_rejects_dictionary_values_with_nulls() {
        let keys = Int32Array::from(vec![Some(1)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("A"), None]));
        let array: ArrayRef =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
        let arrays = [array];

        let err = match build_group_key_views(&arrays) {
            Ok(_) => panic!("expected dictionary values null error"),
            Err(err) => err,
        };

        assert_eq!(err, "dictionary group key values must not contain nulls");
    }

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

        assert!(error.contains("ResourceExhausted"), "{error}");
        assert_eq!(tracker.current(), 0);
    }
}
