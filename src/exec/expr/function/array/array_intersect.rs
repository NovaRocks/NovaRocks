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
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray,
    MapArray, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, make_array,
};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use crc32c::crc32c_append;
use hashbrown::HashMap;

use crate::common::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};
use crate::common::largeint;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

const CRC_HASH_SEED1: u32 = 0x811C9DC5;
const PHMAP_K: u64 = 0xde5fb9d2630458e9;

#[derive(Clone, Debug, PartialEq, Eq)]
enum IntersectValue {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    LargeInt(i128),
    LegacyDecimalV2(i128),
    F32(u32),
    F64(u64),
    Str(String),
    Date(i32),
    Decimal(i128),
    Ts(i64),
    GenericHash(u64),
}

impl Hash for IntersectValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Bool(v) => state.write_u8(u8::from(*v)),
            Self::I8(v) => state.write_i8(*v),
            Self::I16(v) => state.write_i16(*v),
            Self::I32(v) => state.write_i32(*v),
            Self::I64(v) => state.write_i64(*v),
            Self::LargeInt(v) | Self::Decimal(v) => state.write_i128(*v),
            Self::LegacyDecimalV2(v) => state.write_u32(decimalv2_std_hash(*v)),
            Self::F32(bits) => state.write_u32(*bits),
            Self::F64(bits) => state.write_u64(*bits),
            Self::Str(v) => state.write(v.as_bytes()),
            Self::Date(v) => state.write_i32(*v),
            Self::Ts(v) => state.write_i64(*v),
            Self::GenericHash(v) => state.write_u64(*v),
        }
    }
}

#[derive(Default)]
struct PhmapCompatHasher {
    hash: u64,
}

impl Hasher for PhmapCompatHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        self.hash = phmap_crc_hash_64(bytes);
    }

    fn write_u8(&mut self, i: u8) {
        self.hash = phmap_mix_u64(u64::from(i));
    }

    fn write_u16(&mut self, i: u16) {
        self.hash = phmap_mix_u64(u64::from(i));
    }

    fn write_u32(&mut self, i: u32) {
        self.hash = phmap_mix_u64(u64::from(i));
    }

    fn write_u64(&mut self, i: u64) {
        self.hash = phmap_mix_u64(i);
    }

    fn write_i8(&mut self, i: i8) {
        self.hash = phmap_mix_u64(i as i64 as u64);
    }

    fn write_i16(&mut self, i: i16) {
        self.hash = phmap_mix_u64(i as i64 as u64);
    }

    fn write_i32(&mut self, i: i32) {
        self.hash = phmap_mix_u64(i as i64 as u64);
    }

    fn write_i64(&mut self, i: i64) {
        self.hash = phmap_mix_u64(i as u64);
    }

    fn write_i128(&mut self, i: i128) {
        self.hash = phmap_hash_i128(i);
    }
}

fn phmap_mix_u64(value: u64) -> u64 {
    let product = (value as u128).wrapping_mul(PHMAP_K as u128);
    (product as u64).wrapping_add((product >> 64) as u64)
}

fn phmap_mix_u32(value: u32) -> u64 {
    let mixed = u64::from(value).wrapping_mul(0xcc9e2d51_u64);
    mixed ^ (mixed >> 32)
}

fn phmap_crc_hash_64(bytes: &[u8]) -> u64 {
    let unmixed = if bytes.len() < 8 {
        phmap_mix_u32(crc32c_append(CRC_HASH_SEED1, bytes))
    } else {
        let mut hash = u64::from(CRC_HASH_SEED1);
        let mut cursor = 0usize;
        while cursor + 8 <= bytes.len() {
            hash = u64::from(crc32c_append(hash as u32, &bytes[cursor..cursor + 8]));
            cursor += 8;
        }
        if cursor != bytes.len() {
            hash = u64::from(crc32c_append(hash as u32, &bytes[bytes.len() - 8..]));
        }
        hash
    };
    phmap_mix_u64(unmixed)
}

fn hash_combine_u64(seed: &mut u64, value: u64) {
    *seed ^= value
        .wrapping_add(0x9e3779b97f4a7c15_u64)
        .wrapping_add(*seed << 12)
        .wrapping_add(*seed >> 4);
}

fn phmap_hash_i128(value: i128) -> u64 {
    let mut seed = u64::from(CRC_HASH_SEED1);
    hash_combine_u64(&mut seed, value as u64);
    hash_combine_u64(&mut seed, (value >> 64) as u64);
    phmap_mix_u64(seed)
}

fn decimalv2_std_hash(value: i128) -> u32 {
    decimalv2_crc_hash(value)
}

fn decimalv2_crc_hash(value: i128) -> u32 {
    let bytes = value.to_ne_bytes();
    let mut hash = 0u32;
    let mut cursor = 0usize;
    while cursor + 4 <= bytes.len() {
        hash = crc32c_append(hash, &bytes[cursor..cursor + 4]);
        cursor += 4;
    }
    while cursor < bytes.len() {
        hash = crc32c_append(hash, &bytes[cursor..cursor + 1]);
        cursor += 1;
    }
    hash.rotate_left(16)
}

fn value_key(values: &ArrayRef, idx: usize) -> Result<IntersectValue, String> {
    match values.data_type() {
        DataType::Boolean => {
            let arr = values.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(IntersectValue::Bool(arr.value(idx)))
        }
        DataType::Int8 => {
            let arr = values.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(IntersectValue::I8(arr.value(idx)))
        }
        DataType::Int16 => {
            let arr = values.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(IntersectValue::I16(arr.value(idx)))
        }
        DataType::Int32 => {
            let arr = values.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(IntersectValue::I32(arr.value(idx)))
        }
        DataType::Int64 => {
            let arr = values.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(IntersectValue::I64(arr.value(idx)))
        }
        DataType::Float32 => {
            let arr = values.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(IntersectValue::F32(arr.value(idx).to_bits()))
        }
        DataType::Float64 => {
            let arr = values.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(IntersectValue::F64(arr.value(idx).to_bits()))
        }
        DataType::Utf8 => {
            let arr = values.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(IntersectValue::Str(arr.value(idx).to_string()))
        }
        DataType::Date32 => {
            let arr = values.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(IntersectValue::Date(arr.value(idx)))
        }
        DataType::Decimal128(precision, scale) => {
            let arr = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
            if *precision == LEGACY_DECIMALV2_PRECISION && *scale == LEGACY_DECIMALV2_SCALE {
                Ok(IntersectValue::LegacyDecimalV2(arr.value(idx)))
            } else {
                Ok(IntersectValue::Decimal(arr.value(idx)))
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(IntersectValue::Ts(arr.value(idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(IntersectValue::Ts(arr.value(idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(IntersectValue::Ts(arr.value(idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(IntersectValue::Ts(arr.value(idx)))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = values
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "array_intersect failed to downcast LARGEINT values".to_string())?;
            let decoded = largeint::i128_from_be_bytes(arr.value(idx))
                .map_err(|e| format!("array_intersect LARGEINT decode failed: {e}"))?;
            Ok(IntersectValue::LargeInt(decoded))
        }
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
            Ok(IntersectValue::GenericHash(hash_complex_value(values, idx)?))
        }
        other => Err(format!("array_intersect unsupported hash key type: {:?}", other)),
    }
}

fn hash_complex_value(values: &ArrayRef, idx: usize) -> Result<u64, String> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hash_complex_value_impl(values, idx, &mut hasher)?;
    Ok(hasher.finish())
}

fn hash_complex_value_impl(
    values: &ArrayRef,
    idx: usize,
    hasher: &mut std::collections::hash_map::DefaultHasher,
) -> Result<(), String> {
    if values.is_null(idx) {
        0u8.hash(hasher);
        return Ok(());
    }
    1u8.hash(hasher);
    values.data_type().hash(hasher);
    match values.data_type() {
        DataType::Boolean => {
            values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Int8 => {
            values
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Int16 => {
            values
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Int32 => {
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Int64 => {
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Float32 => {
            values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?
                .value(idx)
                .to_bits()
                .hash(hasher);
        }
        DataType::Float64 => {
            values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?
                .value(idx)
                .to_bits()
                .hash(hasher);
        }
        DataType::Utf8 => {
            values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Date32 => {
            values
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Decimal128(_, _) => {
            values
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            values
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            values
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampMillisecondArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            values
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampMicrosecondArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            values
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampNanosecondArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            values
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?
                .value(idx)
                .hash(hasher);
        }
        DataType::List(_) => {
            let list = values
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "failed to downcast to ListArray".to_string())?;
            let offsets = list.value_offsets();
            let start = offsets[idx] as usize;
            let end = offsets[idx + 1] as usize;
            end.saturating_sub(start).hash(hasher);
            let child_values = list.values();
            for i in start..end {
                hash_complex_value_impl(&child_values, i, hasher)?;
            }
        }
        DataType::Struct(_) => {
            let s = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
            s.num_columns().hash(hasher);
            for col_idx in 0..s.num_columns() {
                hash_complex_value_impl(s.column(col_idx), idx, hasher)?;
            }
        }
        DataType::Map(_, _) => {
            let map = values
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "failed to downcast to MapArray".to_string())?;
            let offsets = map.value_offsets();
            let start = offsets[idx] as usize;
            let end = offsets[idx + 1] as usize;
            end.saturating_sub(start).hash(hasher);
            let keys = map.keys();
            let vals = map.values();
            for i in start..end {
                hash_complex_value_impl(&keys, i, hasher)?;
                hash_complex_value_impl(&vals, i, hasher)?;
            }
        }
        other => return Err(format!("array_intersect hash unsupported type: {:?}", other)),
    }
    Ok(())
}

fn intersect_hash(value: &IntersectValue) -> u64 {
    let mut hasher = PhmapCompatHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}

pub fn eval_array_intersect(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() < 2 {
        return Err("array_intersect expects at least two arguments".to_string());
    }

    let mut list_arrays = Vec::with_capacity(args.len());
    for arg in args {
        let arr = arena.eval(*arg, chunk)?;
        arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            format!(
                "array_intersect expects ListArray, got {:?}",
                arr.data_type()
            )
        })?;
        list_arrays.push(arr);
    }

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list_arrays[0].data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_intersect output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let target_item_type = output_field.data_type().clone();

    let mut value_arrays: Vec<ArrayRef> = Vec::with_capacity(list_arrays.len());
    for arr in &list_arrays {
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        let mut values = list.values().clone();
        if values.data_type() != &target_item_type {
            values = super::common::cast_with_special_rules(
                &values,
                &target_item_type,
                "array_intersect",
            )?;
        }
        value_arrays.push(values);
    }

    let values_data: Vec<arrow_data::ArrayData> =
        value_arrays.iter().map(|v| v.to_data()).collect();
    let data_refs: Vec<&arrow_data::ArrayData> = values_data.iter().collect();
    let mut mutable = MutableArrayData::new(data_refs, false, 0);

    let mut offsets = Vec::with_capacity(chunk.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let mut row_null = false;
        for arr in &list_arrays {
            let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let idx = super::common::row_index(row, list.len());
            if list.is_null(idx) {
                row_null = true;
                break;
            }
        }
        if row_null {
            null_builder.append_null();
            offsets.push(current as i32);
            continue;
        }

        let first = list_arrays[0].as_any().downcast_ref::<ListArray>().unwrap();
        let first_row = super::common::row_index(row, first.len());
        let first_offsets = first.value_offsets();
        let first_start = first_offsets[first_row] as usize;
        let first_end = first_offsets[first_row + 1] as usize;

        let mut has_null_in_all = false;
        let mut row_map = HashMap::<
            IntersectValue,
            (usize, usize, u64),
            BuildHasherDefault<PhmapCompatHasher>,
        >::default();
        for idx in first_start..first_end {
            if value_arrays[0].is_null(idx) {
                has_null_in_all = true;
                continue;
            }
            let key = value_key(&value_arrays[0], idx)?;
            let order_key = intersect_hash(&key) & 31;
            row_map.entry(key).or_insert((0, idx, order_key));
        }

        for array_idx in 1..list_arrays.len() {
            let list = list_arrays[array_idx]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let list_row = super::common::row_index(row, list.len());
            let offsets = list.value_offsets();
            let start = offsets[list_row] as usize;
            let end = offsets[list_row + 1] as usize;
            let mut local_has_null = false;
            for idx in start..end {
                if value_arrays[array_idx].is_null(idx) {
                    local_has_null = true;
                    continue;
                }
                let key = value_key(&value_arrays[array_idx], idx)?;
                if let Some((overlap_times, _, _)) = row_map.get_mut(&key)
                    && *overlap_times < array_idx
                {
                    *overlap_times += 1;
                }
            }
            has_null_in_all = has_null_in_all && local_has_null;
        }

        let max_overlap_times = list_arrays.len() - 1;
        let mut matched = row_map
            .values()
            .filter(|(overlap_times, _, _)| *overlap_times == max_overlap_times)
            .map(|(_, first_idx, order_key)| (*order_key, *first_idx))
            .collect::<Vec<_>>();
        matched.sort_unstable();
        for (_, first_idx) in matched {
            mutable.extend(0, first_idx, first_idx + 1);
            current += 1;
        }
        if has_null_in_all {
            mutable.extend_nulls(1);
            current += 1;
        }
        if current > i32::MAX as i64 {
            return Err("array_intersect offset overflow".to_string());
        }
        offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let out_values = make_array(mutable.freeze());
    let list = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        out_values,
        null_builder.finish(),
    );
    Ok(Arc::new(list) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::{Array, Decimal128Array, Int16Array, Int64Array};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_intersect_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let a3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let b1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let b2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let arr1 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![a1, a2, a3],
            },
            list_type.clone(),
        );
        let arr2 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![b1, b2],
            },
            list_type,
        );

        let out =
            eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values.value(0), 2);
    }

    #[test]
    fn test_array_intersect_keeps_shared_null_once() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let an = typed_null(&mut arena, DataType::Int64);
        let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let bn = typed_null(&mut arena, DataType::Int64);
        let b2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let arr1 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![a1, an, a2],
            },
            list_type.clone(),
        );
        let arr2 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![bn, b2],
            },
            list_type,
        );

        let out =
            eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), 2);
        assert!(values.is_null(1));
    }

    #[test]
    fn test_array_intersect_preserves_starrocks_hash_order() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int16, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let v100_a = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(100)), DataType::Int16);
        let v200 = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(200)), DataType::Int16);
        let v300_a = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(300)), DataType::Int16);
        let v100_b = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(100)), DataType::Int16);
        let v300_b = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(300)), DataType::Int16);
        let v900 = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(900)), DataType::Int16);
        let arr1 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v100_a, v200, v300_a],
            },
            list_type.clone(),
        );
        let arr2 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v100_b, v300_b, v900],
            },
            list_type,
        );

        let out =
            eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(values.values(), &[300, 100]);
    }

    #[test]
    fn test_array_intersect_preserves_decimalv2_hash_order() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let decimal_type = DataType::Decimal128(27, 9);
        let list_type = DataType::List(Arc::new(Field::new("item", decimal_type.clone(), true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let v123_a = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 123_450_000_000,
                precision: 27,
                scale: 9,
            }),
            decimal_type.clone(),
        );
        let v456_a = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 456_780_000_000,
                precision: 27,
                scale: 9,
            }),
            decimal_type.clone(),
        );
        let v789_a = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 789_010_000_000,
                precision: 27,
                scale: 9,
            }),
            decimal_type.clone(),
        );
        let v123_b = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 123_450_000_000,
                precision: 27,
                scale: 9,
            }),
            decimal_type.clone(),
        );
        let v456_b = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 456_780_000_000,
                precision: 27,
                scale: 9,
            }),
            decimal_type.clone(),
        );
        let v789_b = arena.push_typed(
            ExprNode::Literal(LiteralValue::Decimal128 {
                value: 789_010_000_000,
                precision: 27,
                scale: 9,
            }),
            decimal_type.clone(),
        );
        let arr1 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v123_a, v456_a, v789_a],
            },
            list_type.clone(),
        );
        let arr2 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v123_b, v456_b, v789_b],
            },
            list_type,
        );

        let out =
            eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(
            values.values(),
            &[123_450_000_000, 789_010_000_000, 456_780_000_000]
        );
    }
}
