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
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::common::largeint;
use crate::exec::expr::agg::{FloatArrayView, IntArrayView};

use super::hash::{
    canonical_f32_bits, canonical_f64_bits, combine_hash, hash_bytes_with_seed,
    hash_i128_with_seed, hash_null_with_seed, hash_u64_with_seed,
};
use super::key_layout::{CompressedKeyContext, compressed_key_is_valid};

pub(crate) enum GroupKeyArrayView<'a> {
    Int(IntArrayView<'a>),
    Float(FloatArrayView<'a>),
    Boolean(&'a BooleanArray),
    Utf8(&'a StringArray),
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

pub(crate) fn build_group_key_views<'a>(
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

pub(crate) fn encode_group_key_row(
    array: &ArrayRef,
    row: usize,
) -> Result<Option<Vec<u8>>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    let mut out = Vec::new();
    encode_group_key_non_null_value(array, row, &mut out)?;
    Ok(Some(out))
}

fn encode_group_key_non_null_value(
    array: &ArrayRef,
    row: usize,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    "failed to downcast BooleanArray while encoding group key".to_string()
                })?;
            out.push(1);
            out.push(arr.value(row) as u8);
            Ok(())
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| {
                    "failed to downcast Int8Array while encoding group key".to_string()
                })?;
            out.push(2);
            out.extend_from_slice(&arr.value(row).to_le_bytes());
            Ok(())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| {
                    "failed to downcast Int16Array while encoding group key".to_string()
                })?;
            out.push(3);
            out.extend_from_slice(&arr.value(row).to_le_bytes());
            Ok(())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| {
                    "failed to downcast Int32Array while encoding group key".to_string()
                })?;
            out.push(4);
            out.extend_from_slice(&arr.value(row).to_le_bytes());
            Ok(())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    "failed to downcast Int64Array while encoding group key".to_string()
                })?;
            out.push(5);
            out.extend_from_slice(&arr.value(row).to_le_bytes());
            Ok(())
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| {
                    "failed to downcast Float32Array while encoding group key".to_string()
                })?;
            out.push(6);
            out.extend_from_slice(&canonical_f32_bits(arr.value(row)).to_le_bytes());
            Ok(())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| {
                    "failed to downcast Float64Array while encoding group key".to_string()
                })?;
            out.push(7);
            out.extend_from_slice(&canonical_f64_bits(arr.value(row)).to_le_bytes());
            Ok(())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    "failed to downcast StringArray while encoding group key".to_string()
                })?;
            out.push(8);
            let value = arr.value(row).as_bytes();
            let len = value.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(value);
            Ok(())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    "failed to downcast Date32Array while encoding group key".to_string()
                })?;
            out.push(9);
            out.extend_from_slice(&arr.value(row).to_le_bytes());
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
            out.push(marker);
            out.extend_from_slice(&value.to_le_bytes());
            Ok(())
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    "failed to downcast Decimal128Array while encoding group key".to_string()
                })?;
            out.push(14);
            out.extend_from_slice(&arr.value(row).to_le_bytes());
            Ok(())
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    "failed to downcast Decimal256Array while encoding group key".to_string()
                })?;
            let encoded = arr.value(row).to_string();
            out.push(19);
            let bytes = encoded.as_bytes();
            let len = u32::try_from(bytes.len())
                .map_err(|_| "group key Decimal256 length overflow".to_string())?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
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
            out.push(15);
            out.extend_from_slice(&value.to_le_bytes());
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
            out.push(16);
            let len = u32::try_from(end.saturating_sub(start))
                .map_err(|_| "group key list length overflow".to_string())?;
            out.extend_from_slice(&len.to_le_bytes());
            for idx in start..end {
                match encode_group_key_row(&values, idx)? {
                    None => out.push(0),
                    Some(encoded) => {
                        out.push(1);
                        let item_len = u32::try_from(encoded.len())
                            .map_err(|_| "group key list item length overflow".to_string())?;
                        out.extend_from_slice(&item_len.to_le_bytes());
                        out.extend_from_slice(&encoded);
                    }
                }
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
            out.push(17);
            let field_count = u32::try_from(struct_arr.num_columns())
                .map_err(|_| "group key struct field count overflow".to_string())?;
            out.extend_from_slice(&field_count.to_le_bytes());
            for column in struct_arr.columns() {
                match encode_group_key_row(column, row)? {
                    None => out.push(0),
                    Some(encoded) => {
                        out.push(1);
                        let len = u32::try_from(encoded.len())
                            .map_err(|_| "group key struct field length overflow".to_string())?;
                        out.extend_from_slice(&len.to_le_bytes());
                        out.extend_from_slice(&encoded);
                    }
                }
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
            out.push(18);
            let len = u32::try_from(end.saturating_sub(start))
                .map_err(|_| "group key map length overflow".to_string())?;
            out.extend_from_slice(&len.to_le_bytes());
            for idx in start..end {
                match encode_group_key_row(&keys, idx)? {
                    None => out.push(0),
                    Some(key) => {
                        out.push(1);
                        let key_len = u32::try_from(key.len())
                            .map_err(|_| "group key map key length overflow".to_string())?;
                        out.extend_from_slice(&key_len.to_le_bytes());
                        out.extend_from_slice(&key);
                    }
                }
                match encode_group_key_row(&values, idx)? {
                    None => out.push(0),
                    Some(encoded) => {
                        out.push(1);
                        let value_len = u32::try_from(encoded.len())
                            .map_err(|_| "group key map value length overflow".to_string())?;
                        out.extend_from_slice(&value_len.to_le_bytes());
                        out.extend_from_slice(&encoded);
                    }
                }
            }
            Ok(())
        }
        other => Err(format!(
            "group key encode unsupported input type: {:?}",
            other
        )),
    }
}

pub(crate) fn list_utf8_row_value(
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

pub(crate) fn list_int32_row_value(
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

pub(crate) fn build_one_number_hashes(
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
                    let value_hash = hash_bytes_with_seed(seed, value.to_string().as_bytes());
                    hashes.push(combine_hash(seed, value_hash));
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes.push(combine_hash(seed, null_hash));
                    } else {
                        let value_hash =
                            hash_bytes_with_seed(seed, arr.value(row).to_string().as_bytes());
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
        | GroupKeyArrayView::ListUtf8 { .. }
        | GroupKeyArrayView::ListInt32 { .. }
        | GroupKeyArrayView::Complex(_) => {
            return Err("one number key does not support variable-length types".to_string());
        }
    }
    Ok(hashes)
}

pub(crate) fn build_group_key_hashes(
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
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes[row] = combine_hash(hashes[row], null_hash);
                        } else {
                            let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                            hashes[row] = combine_hash(hashes[row], value_hash);
                        }
                    }
                }
            }
            IntArrayView::Int32(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let value_hash = hash_u64_with_seed(seed, *value as i64 as u64);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes[row] = combine_hash(hashes[row], null_hash);
                        } else {
                            let value_hash = hash_u64_with_seed(seed, arr.value(row) as i64 as u64);
                            hashes[row] = combine_hash(hashes[row], value_hash);
                        }
                    }
                }
            }
            IntArrayView::Int16(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let value_hash = hash_u64_with_seed(seed, *value as i64 as u64);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes[row] = combine_hash(hashes[row], null_hash);
                        } else {
                            let value_hash = hash_u64_with_seed(seed, arr.value(row) as i64 as u64);
                            hashes[row] = combine_hash(hashes[row], value_hash);
                        }
                    }
                }
            }
            IntArrayView::Int8(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let value_hash = hash_u64_with_seed(seed, *value as i64 as u64);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes[row] = combine_hash(hashes[row], null_hash);
                        } else {
                            let value_hash = hash_u64_with_seed(seed, arr.value(row) as i64 as u64);
                            hashes[row] = combine_hash(hashes[row], value_hash);
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
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes[row] = combine_hash(hashes[row], null_hash);
                        } else {
                            let bits = canonical_f64_bits(arr.value(row));
                            let value_hash = hash_u64_with_seed(seed, bits);
                            hashes[row] = combine_hash(hashes[row], value_hash);
                        }
                    }
                }
            }
            FloatArrayView::Float32(arr) => {
                if arr.null_count() == 0 {
                    for (row, value) in arr.values().iter().enumerate() {
                        let bits = canonical_f32_bits(*value) as u64;
                        let value_hash = hash_u64_with_seed(seed, bits);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                } else {
                    for row in 0..num_rows {
                        if arr.is_null(row) {
                            hashes[row] = combine_hash(hashes[row], null_hash);
                        } else {
                            let bits = canonical_f32_bits(arr.value(row)) as u64;
                            let value_hash = hash_u64_with_seed(seed, bits);
                            hashes[row] = combine_hash(hashes[row], value_hash);
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
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value = if arr.value(row) { 1u64 } else { 0u64 };
                        let value_hash = hash_u64_with_seed(seed, value);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Utf8(arr) => {
            if arr.null_count() == 0 {
                for row in 0..num_rows {
                    let value_hash = hash_bytes_with_seed(seed, arr.value(row).as_bytes());
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash = hash_bytes_with_seed(seed, arr.value(row).as_bytes());
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Date32(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampSecond(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampMillisecond(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampMicrosecond(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::TimestampNanosecond(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_u64_with_seed(seed, *value as u64);
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash = hash_u64_with_seed(seed, arr.value(row) as u64);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Decimal128(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_i128_with_seed(seed, *value);
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash = hash_i128_with_seed(seed, arr.value(row));
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Decimal256(arr) => {
            if arr.null_count() == 0 {
                for (row, value) in arr.values().iter().enumerate() {
                    let value_hash = hash_bytes_with_seed(seed, value.to_string().as_bytes());
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if arr.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash =
                            hash_bytes_with_seed(seed, arr.value(row).to_string().as_bytes());
                        hashes[row] = combine_hash(hashes[row], value_hash);
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
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if let Some(value) = largeint_row_value(arr, row)? {
                        let value_hash = hash_i128_with_seed(seed, value);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    } else {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::ListUtf8 { list, values } => {
            if list.null_count() == 0 {
                for row in 0..num_rows {
                    let value_hash = hash_list_utf8_row(list, values, row, seed);
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if list.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash = hash_list_utf8_row(list, values, row, seed);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::ListInt32 { list, values } => {
            if list.null_count() == 0 {
                for row in 0..num_rows {
                    let value_hash = hash_list_int32_row(list, values, row, seed);
                    hashes[row] = combine_hash(hashes[row], value_hash);
                }
            } else {
                for row in 0..num_rows {
                    if list.is_null(row) {
                        hashes[row] = combine_hash(hashes[row], null_hash);
                    } else {
                        let value_hash = hash_list_int32_row(list, values, row, seed);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                }
            }
        }
        GroupKeyArrayView::Complex(array) => {
            for row in 0..num_rows {
                match encode_group_key_row(array, row)? {
                    Some(encoded) => {
                        let value_hash = hash_bytes_with_seed(seed, &encoded);
                        hashes[row] = combine_hash(hashes[row], value_hash);
                    }
                    None => {
                        hashes[row] = combine_hash(hashes[row], null_hash);
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
