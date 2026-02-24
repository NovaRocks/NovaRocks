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
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, FixedSizeBinaryArray, Int8Array,
    Int16Array, Int32Array, Int64Array, Int64Builder, LargeBinaryArray, LargeStringArray,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;

use crate::common::largeint;
use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct BitmapUnionIntAgg;

type BitmapValues = BTreeSet<u64>;

const BITMAP_TYPE_EMPTY: u8 = 0;
const BITMAP_TYPE_SINGLE32: u8 = 1;
const BITMAP_TYPE_SINGLE64: u8 = 3;
const BITMAP_TYPE_SET: u8 = 10;

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

fn kind_from_name(name: &str) -> Option<AggKind> {
    match canonical_agg_name(name) {
        "bitmap_union_int" => Some(AggKind::BitmapUnionInt),
        "bitmap_agg" | "bitmap_union" => Some(AggKind::BitmapAgg),
        "bitmap_union_count" => Some(AggKind::BitmapUnionInt),
        _ => None,
    }
}

fn values_slot(ptr: *mut u8) -> *mut *mut BitmapValues {
    ptr as *mut *mut BitmapValues
}

unsafe fn get_or_init_values<'a>(ptr: *mut u8) -> &'a mut BitmapValues {
    let slot = values_slot(ptr);
    let raw = unsafe { *slot };
    if raw.is_null() {
        let boxed: Box<BitmapValues> = Box::default();
        let raw = Box::into_raw(boxed);
        unsafe {
            *slot = raw;
            &mut *raw
        }
    } else {
        unsafe { &mut *raw }
    }
}

unsafe fn get_values<'a>(ptr: *mut u8) -> Option<&'a BitmapValues> {
    let raw = unsafe { *values_slot(ptr) };
    if raw.is_null() {
        None
    } else {
        Some(unsafe { &*raw })
    }
}

unsafe fn take_values(ptr: *mut u8) -> Option<Box<BitmapValues>> {
    let slot = values_slot(ptr);
    let raw = unsafe { *slot };
    if raw.is_null() {
        None
    } else {
        unsafe {
            *slot = std::ptr::null_mut();
            Some(Box::from_raw(raw))
        }
    }
}

fn decode_varint_u64(bytes: &[u8]) -> Result<(u64, usize), String> {
    let mut out = 0u64;
    let mut shift = 0u32;
    for (idx, byte) in bytes.iter().enumerate() {
        out |= u64::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            return Ok((out, idx + 1));
        }
        shift += 7;
        if shift > 63 {
            return Err("bitmap_union_int decode varint overflow".to_string());
        }
    }
    Err("bitmap_union_int decode varint reached end of payload".to_string())
}

fn encode_bitmap(values: &BitmapValues) -> Result<Vec<u8>, String> {
    if values.is_empty() {
        return Ok(vec![BITMAP_TYPE_EMPTY]);
    }
    if values.len() == 1 {
        let value = values
            .first()
            .copied()
            .ok_or_else(|| "bitmap_union_int internal empty set".to_string())?;
        if let Ok(v32) = u32::try_from(value) {
            let mut out = Vec::with_capacity(5);
            out.push(BITMAP_TYPE_SINGLE32);
            out.extend_from_slice(&v32.to_le_bytes());
            return Ok(out);
        }
        let mut out = Vec::with_capacity(9);
        out.push(BITMAP_TYPE_SINGLE64);
        out.extend_from_slice(&value.to_le_bytes());
        return Ok(out);
    }

    let count = u32::try_from(values.len())
        .map_err(|_| format!("bitmap_union_int value count overflow: {}", values.len()))?;
    let mut out = Vec::with_capacity(1 + 4 + values.len() * 8);
    out.push(BITMAP_TYPE_SET);
    out.extend_from_slice(&count.to_le_bytes());
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn decode_bitmap(bytes: &[u8]) -> Result<BitmapValues, String> {
    if bytes.is_empty() {
        return Err("bitmap_union_int payload is empty".to_string());
    }
    match bytes[0] {
        BITMAP_TYPE_EMPTY => {
            if bytes.len() != 1 {
                return Err(format!(
                    "bitmap_union_int EMPTY payload length mismatch: expected=1 actual={}",
                    bytes.len()
                ));
            }
            Ok(BTreeSet::new())
        }
        BITMAP_TYPE_SINGLE32 => {
            if bytes.len() != 5 {
                return Err(format!(
                    "bitmap_union_int SINGLE32 payload length mismatch: expected=5 actual={}",
                    bytes.len()
                ));
            }
            let value = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "bitmap_union_int decode SINGLE32 value failed".to_string())?,
            );
            Ok(BTreeSet::from([u64::from(value)]))
        }
        BITMAP_TYPE_SINGLE64 => {
            if bytes.len() != 9 {
                return Err(format!(
                    "bitmap_union_int SINGLE64 payload length mismatch: expected=9 actual={}",
                    bytes.len()
                ));
            }
            let value = u64::from_le_bytes(
                bytes[1..9]
                    .try_into()
                    .map_err(|_| "bitmap_union_int decode SINGLE64 value failed".to_string())?,
            );
            Ok(BTreeSet::from([value]))
        }
        BITMAP_TYPE_SET => {
            if bytes.len() < 5 {
                return Err(format!(
                    "bitmap_union_int SET payload too short: actual={}",
                    bytes.len()
                ));
            }
            let count = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "bitmap_union_int decode SET count failed".to_string())?,
            ) as usize;
            let fixed_expected = 1usize + 4 + count.saturating_mul(8);
            if bytes.len() == fixed_expected {
                let mut values = BTreeSet::new();
                let mut offset = 5usize;
                for idx in 0..count {
                    let value =
                        u64::from_le_bytes(bytes[offset..offset + 8].try_into().map_err(|_| {
                            format!(
                                "bitmap_union_int decode SET fixed64 value failed at entry {}",
                                idx
                            )
                        })?);
                    offset += 8;
                    values.insert(value);
                }
                return Ok(values);
            }

            let mut offset = 5usize;
            let mut values = BTreeSet::new();
            for idx in 0..count {
                let (value, consumed) = decode_varint_u64(&bytes[offset..]).map_err(|e| {
                    format!(
                        "bitmap_union_int decode SET value failed at entry {}: {}",
                        idx, e
                    )
                })?;
                if consumed == 0 {
                    return Err(format!(
                        "bitmap_union_int decode SET consumed zero bytes at entry {}",
                        idx
                    ));
                }
                offset = offset.saturating_add(consumed);
                if offset > bytes.len() {
                    return Err(format!(
                        "bitmap_union_int SET payload overflow: offset={} len={}",
                        offset,
                        bytes.len()
                    ));
                }
                values.insert(value);
            }
            if offset != bytes.len() {
                return Err(format!(
                    "bitmap_union_int SET payload has trailing bytes: offset={} len={}",
                    offset,
                    bytes.len()
                ));
            }
            Ok(values)
        }
        other => Err(format!(
            "bitmap_union_int unsupported payload type code: {}",
            other
        )),
    }
}

impl AggregateFunction for BitmapUnionIntAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let kind = kind_from_name(func.name.as_str())
            .ok_or_else(|| format!("unsupported bitmap aggregate function: {}", func.name))?;
        if !input_is_intermediate {
            let dt =
                input_type.ok_or_else(|| "bitmap_union_int requires 1 argument".to_string())?;
            if !matches!(
                dt,
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::FixedSizeBinary(_)
            ) {
                return Err(format!(
                    "bitmap aggregate expects BOOLEAN/INTEGER/VARCHAR/BINARY input, got {:?}",
                    dt
                ));
            }
        }
        Ok(AggSpec {
            kind: kind.clone(),
            output_type: match kind {
                AggKind::BitmapAgg => DataType::Binary,
                AggKind::BitmapUnionInt => DataType::Int64,
                _ => unreachable!("unexpected bitmap kind"),
            },
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::BitmapAgg | AggKind::BitmapUnionInt => (
                std::mem::size_of::<*mut BitmapValues>(),
                std::mem::align_of::<*mut BitmapValues>(),
            ),
            other => unreachable!("unexpected kind for bitmap aggregate: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "bitmap_union_int input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "bitmap_union_int intermediate input missing".to_string())?;
        let binary = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(binary))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(values_slot(ptr), std::ptr::null_mut());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            let _ = take_values(ptr);
        }
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("bitmap_union_int batch input type mismatch".to_string());
        };
        let include_negative = matches!(spec.kind, AggKind::BitmapUnionInt);

        macro_rules! update_signed {
            ($arr_ty:ty) => {{
                let arr = array
                    .as_any()
                    .downcast_ref::<$arr_ty>()
                    .ok_or_else(|| "failed to downcast signed integer array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let raw = i64::from(arr.value(row));
                    if !include_negative && raw < 0 {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    values.insert(raw as u64);
                }
                Ok(())
            }};
        }

        match array.data_type() {
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    values.insert(if arr.value(row) { 1 } else { 0 });
                }
                Ok(())
            }
            DataType::Int8 => update_signed!(Int8Array),
            DataType::Int16 => update_signed!(Int16Array),
            DataType::Int32 => update_signed!(Int32Array),
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let raw = arr.value(row);
                    if !include_negative && raw < 0 {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    values.insert(raw as u64);
                }
                Ok(())
            }
            DataType::UInt8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or_else(|| "failed to downcast to UInt8Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    values.insert(u64::from(arr.value(row)));
                }
                Ok(())
            }
            DataType::UInt16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .ok_or_else(|| "failed to downcast to UInt16Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    values.insert(u64::from(arr.value(row)));
                }
                Ok(())
            }
            DataType::UInt32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| "failed to downcast to UInt32Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    values.insert(u64::from(arr.value(row)));
                }
                Ok(())
            }
            DataType::UInt64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| "failed to downcast to UInt64Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    values.insert(arr.value(row));
                }
                Ok(())
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let Ok(value) = arr.value(row).trim().parse::<i128>() else {
                        continue;
                    };
                    if !include_negative && value < 0 {
                        continue;
                    }
                    if value < i64::MIN as i128 || value > u64::MAX as i128 {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    if value < 0 {
                        values.insert((value as i64) as u64);
                    } else {
                        values.insert(value as u64);
                    }
                }
                Ok(())
            }
            DataType::LargeUtf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| "failed to downcast to LargeStringArray".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let Ok(value) = arr.value(row).trim().parse::<i128>() else {
                        continue;
                    };
                    if !include_negative && value < 0 {
                        continue;
                    }
                    if value < i64::MIN as i128 || value > u64::MAX as i128 {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    if value < 0 {
                        values.insert((value as i64) as u64);
                    } else {
                        values.insert(value as u64);
                    }
                }
                Ok(())
            }
            DataType::Binary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    if let Ok(decoded) = decode_bitmap(arr.value(row)) {
                        values.extend(decoded.into_iter());
                        continue;
                    }
                    let Ok(text) = std::str::from_utf8(arr.value(row)) else {
                        continue;
                    };
                    let Ok(value) = text.trim().parse::<i128>() else {
                        continue;
                    };
                    if !include_negative && value < 0 {
                        continue;
                    }
                    if value < i64::MIN as i128 || value > u64::MAX as i128 {
                        continue;
                    }
                    if value < 0 {
                        values.insert((value as i64) as u64);
                    } else {
                        values.insert(value as u64);
                    }
                }
                Ok(())
            }
            DataType::LargeBinary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    if let Ok(decoded) = decode_bitmap(arr.value(row)) {
                        values.extend(decoded.into_iter());
                        continue;
                    }
                    let Ok(text) = std::str::from_utf8(arr.value(row)) else {
                        continue;
                    };
                    let Ok(value) = text.trim().parse::<i128>() else {
                        continue;
                    };
                    if !include_negative && value < 0 {
                        continue;
                    }
                    if value < i64::MIN as i128 || value > u64::MAX as i128 {
                        continue;
                    }
                    if value < 0 {
                        values.insert((value as i64) as u64);
                    } else {
                        values.insert(value as u64);
                    }
                }
                Ok(())
            }
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let Ok(value) = largeint::i128_from_be_bytes(arr.value(row)) else {
                        continue;
                    };
                    if !include_negative && value < 0 {
                        continue;
                    }
                    if value < i64::MIN as i128 || value > u64::MAX as i128 {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let values = unsafe { get_or_init_values(ptr) };
                    if value < 0 {
                        values.insert((value as i64) as u64);
                    } else {
                        values.insert(value as u64);
                    }
                }
                Ok(())
            }
            other => Err(format!(
                "bitmap aggregate expects BOOLEAN/INTEGER/VARCHAR/BINARY input, got {:?}",
                other
            )),
        }
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Binary(arr) = input else {
            return Err("bitmap_union_int merge input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let decoded = decode_bitmap(arr.value(row))?;
            if decoded.is_empty() {
                continue;
            }
            let ptr = unsafe { (base as *mut u8).add(offset) };
            let values = unsafe { get_or_init_values(ptr) };
            values.extend(decoded.into_iter());
        }
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let payload = match unsafe { get_values(ptr) } {
                    Some(values) => encode_bitmap(values)?,
                    None => vec![BITMAP_TYPE_EMPTY],
                };
                builder.append_value(payload);
            }
            return Ok(Arc::new(builder.finish()));
        }

        match &spec.kind {
            AggKind::BitmapUnionInt => {
                let mut builder = Int64Builder::new();
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let value = match unsafe { get_values(ptr) } {
                        Some(values) => i64::try_from(values.len()).map_err(|_| {
                            format!("bitmap_union_int cardinality overflow: {}", values.len())
                        })?,
                        None => 0_i64,
                    };
                    builder.append_value(value);
                }
                Ok(Arc::new(builder.finish()))
            }
            AggKind::BitmapAgg => {
                let mut builder = BinaryBuilder::new();
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    match unsafe { get_values(ptr) } {
                        Some(values) => builder.append_value(encode_bitmap(values)?),
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            other => Err(format!("unexpected bitmap aggregate kind: {:?}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{decode_bitmap, encode_bitmap};

    #[test]
    fn bitmap_union_int_encodes_single32() {
        let values = BTreeSet::from([7_u64]);
        let encoded = encode_bitmap(&values).expect("encode");
        assert_eq!(encoded, vec![1_u8, 7, 0, 0, 0]);
        let decoded = decode_bitmap(&encoded).expect("decode");
        assert_eq!(decoded, values);
    }

    #[test]
    fn bitmap_union_int_round_trip_set() {
        let values = BTreeSet::from([1_u64, 300_u64, 1_000_000_u64]);
        let encoded = encode_bitmap(&values).expect("encode");
        let decoded = decode_bitmap(&encoded).expect("decode");
        assert_eq!(decoded, values);
    }
}
