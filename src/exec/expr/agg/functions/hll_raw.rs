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
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, Int64Builder, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct HllRawAgg;

const HLL_DATA_EMPTY: u8 = 0;
const HLL_DATA_EXPLICIT: u8 = 1;
const HLL_DATA_SPARSE: u8 = 2;
const HLL_DATA_FULL: u8 = 3;

const HLL_COLUMN_PRECISION: usize = 14;
const HLL_REGISTERS_COUNT: usize = 16 * 1024;
const HLL_SPARSE_THRESHOLD: usize = 4096;

const MURMUR_PRIME: u64 = 0xc6a4_a793_5bd1_e995;
const MURMUR_SEED: u32 = 0xadc8_3b19;

#[derive(Default)]
struct HllRawState {
    has_value: bool,
    registers: Option<Box<[u8; HLL_REGISTERS_COUNT]>>,
}

fn state_slot(ptr: *mut u8) -> *mut *mut HllRawState {
    ptr as *mut *mut HllRawState
}

unsafe fn get_or_init_state<'a>(ptr: *mut u8) -> &'a mut HllRawState {
    let slot = state_slot(ptr);
    let raw = unsafe { *slot };
    if raw.is_null() {
        let boxed: Box<HllRawState> = Box::default();
        let raw = Box::into_raw(boxed);
        unsafe {
            *slot = raw;
            &mut *raw
        }
    } else {
        unsafe { &mut *raw }
    }
}

unsafe fn get_state<'a>(ptr: *mut u8) -> Option<&'a HllRawState> {
    let raw = unsafe { *state_slot(ptr) };
    if raw.is_null() {
        None
    } else {
        Some(unsafe { &*raw })
    }
}

unsafe fn take_state(ptr: *mut u8) -> Option<Box<HllRawState>> {
    let slot = state_slot(ptr);
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

fn ensure_registers(state: &mut HllRawState) -> &mut [u8; HLL_REGISTERS_COUNT] {
    state
        .registers
        .get_or_insert_with(|| Box::new([0u8; HLL_REGISTERS_COUNT]))
}

fn update_register_from_hash(state: &mut HllRawState, hash_value: u64) {
    if hash_value == 0 {
        return;
    }
    state.has_value = true;
    let registers = ensure_registers(state);
    let idx = (hash_value % HLL_REGISTERS_COUNT as u64) as usize;
    let mut shifted = hash_value >> HLL_COLUMN_PRECISION;
    shifted |= 1_u64 << (64 - HLL_COLUMN_PRECISION);
    let rank = shifted.trailing_zeros() as u8 + 1;
    if registers[idx] < rank {
        registers[idx] = rank;
    }
}

fn merge_as_opaque_payload(state: &mut HllRawState, bytes: &[u8]) {
    let hash = murmur_hash64a(bytes, MURMUR_SEED);
    update_register_from_hash(state, hash);
}

fn merge_hll_bytes(state: &mut HllRawState, bytes: &[u8]) -> Result<(), String> {
    if bytes.is_empty() {
        return Err("hll_raw merge payload is empty".to_string());
    }
    match bytes[0] {
        HLL_DATA_EMPTY => Ok(()),
        HLL_DATA_EXPLICIT => {
            if bytes.len() < 2 {
                return Err("hll_raw EXPLICIT payload is malformed".to_string());
            }
            let count = bytes[1] as usize;
            let expected = 2 + count * 8;
            if bytes.len() != expected {
                // Keep query running for non-standard HLL payloads (for example ds_hll states)
                // by folding unknown bytes into a deterministic hash bucket.
                merge_as_opaque_payload(state, bytes);
                return Ok(());
            }
            let mut pos = 2usize;
            for _ in 0..count {
                let hash = u64::from_le_bytes(
                    bytes[pos..pos + 8]
                        .try_into()
                        .map_err(|_| "hll_raw decode EXPLICIT hash failed".to_string())?,
                );
                pos += 8;
                update_register_from_hash(state, hash);
            }
            Ok(())
        }
        HLL_DATA_SPARSE => {
            if bytes.len() < 5 {
                return Err("hll_raw SPARSE payload is malformed".to_string());
            }
            let count = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "hll_raw decode SPARSE count failed".to_string())?,
            ) as usize;
            let expected = 5 + count * 3;
            if bytes.len() != expected {
                merge_as_opaque_payload(state, bytes);
                return Ok(());
            }
            let mut pos = 5usize;
            let mut has_non_zero = false;
            for _ in 0..count {
                let idx = u16::from_le_bytes(
                    bytes[pos..pos + 2]
                        .try_into()
                        .map_err(|_| "hll_raw decode SPARSE index failed".to_string())?,
                ) as usize;
                pos += 2;
                if idx >= HLL_REGISTERS_COUNT {
                    merge_as_opaque_payload(state, bytes);
                    return Ok(());
                }
                let value = bytes[pos];
                pos += 1;
                if value > 0 {
                    has_non_zero = true;
                    let registers = ensure_registers(state);
                    if registers[idx] < value {
                        registers[idx] = value;
                    }
                }
            }
            if has_non_zero {
                state.has_value = true;
            }
            Ok(())
        }
        HLL_DATA_FULL => {
            let expected = 1 + HLL_REGISTERS_COUNT;
            if bytes.len() != expected {
                merge_as_opaque_payload(state, bytes);
                return Ok(());
            }
            let mut has_non_zero = false;
            for (idx, value) in bytes[1..].iter().enumerate() {
                if *value > 0 {
                    has_non_zero = true;
                    let registers = ensure_registers(state);
                    if registers[idx] < *value {
                        registers[idx] = *value;
                    }
                }
            }
            if has_non_zero {
                state.has_value = true;
            }
            Ok(())
        }
        _ => {
            merge_as_opaque_payload(state, bytes);
            Ok(())
        }
    }
}

fn serialize_hll_state(state: &HllRawState) -> Option<Vec<u8>> {
    if !state.has_value {
        return None;
    }
    let registers = state.registers.as_ref()?;
    let non_zero = registers.iter().filter(|v| **v > 0).count();
    if non_zero == 0 {
        return Some(vec![HLL_DATA_EMPTY]);
    }

    if non_zero > HLL_SPARSE_THRESHOLD {
        let mut out = Vec::with_capacity(1 + HLL_REGISTERS_COUNT);
        out.push(HLL_DATA_FULL);
        out.extend_from_slice(&registers[..]);
        return Some(out);
    }

    let mut out = Vec::with_capacity(5 + non_zero * 3);
    out.push(HLL_DATA_SPARSE);
    out.extend_from_slice(&(non_zero as u32).to_le_bytes());
    for (idx, value) in registers.iter().enumerate() {
        if *value > 0 {
            out.extend_from_slice(&(idx as u16).to_le_bytes());
            out.push(*value);
        }
    }
    Some(out)
}

fn murmur_hash64a(data: &[u8], seed: u32) -> u64 {
    let r: u32 = 47;
    let mut h = (seed as u64) ^ (data.len() as u64).wrapping_mul(MURMUR_PRIME);

    let mut offset = 0usize;
    while offset + 8 <= data.len() {
        let mut block = [0u8; 8];
        block.copy_from_slice(&data[offset..offset + 8]);
        let mut k = u64::from_le_bytes(block);
        k = k.wrapping_mul(MURMUR_PRIME);
        k ^= k >> r;
        k = k.wrapping_mul(MURMUR_PRIME);
        h ^= k;
        h = h.wrapping_mul(MURMUR_PRIME);
        offset += 8;
    }

    let tail = &data[offset..];
    if !tail.is_empty() {
        for (idx, byte) in tail.iter().enumerate() {
            h ^= (*byte as u64) << (idx * 8);
        }
        h = h.wrapping_mul(MURMUR_PRIME);
    }

    h ^= h >> r;
    h = h.wrapping_mul(MURMUR_PRIME);
    h ^= h >> r;
    h
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

fn estimate_cardinality(state: &HllRawState) -> i64 {
    if !state.has_value {
        return 0;
    }
    let Some(registers) = state.registers.as_ref() else {
        return 0;
    };

    let num_streams = HLL_REGISTERS_COUNT as f64;
    let alpha = match HLL_REGISTERS_COUNT {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / num_streams),
    };

    let mut harmonic_mean = 0.0f64;
    let mut zero_registers = 0usize;
    for register in registers.iter() {
        harmonic_mean += 2_f64.powi(-(*register as i32));
        if *register == 0 {
            zero_registers += 1;
        }
    }

    if harmonic_mean == 0.0 {
        return 0;
    }

    let mut estimate = alpha * num_streams * num_streams / harmonic_mean;
    if estimate <= num_streams * 2.5 && zero_registers != 0 {
        estimate = num_streams * (num_streams / zero_registers as f64).ln();
    } else if HLL_REGISTERS_COUNT == 16 * 1024 && estimate < 72_000.0 {
        // Keep parity with StarRocks' correction in be/src/types/hll.cpp.
        let bias = 5.9119e-18 * estimate.powi(4) - 1.4253e-12 * estimate.powi(3)
            + 1.2940e-7 * estimate.powi(2)
            - 5.2921e-3 * estimate
            + 83.3216;
        estimate -= estimate * (bias / 100.0);
    }

    estimate.max(0.0).round() as i64
}

fn merge_input_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
) -> Result<(), String> {
    macro_rules! merge_payload {
        ($arr:expr, $row:ident => $bytes:expr) => {{
            for ($row, &base) in state_ptrs.iter().enumerate() {
                if $arr.is_null($row) {
                    continue;
                }
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let state = unsafe { get_or_init_state(ptr) };
                merge_hll_bytes(state, $bytes)?;
            }
            Ok(())
        }};
    }

    match array.data_type() {
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
            merge_payload!(arr, row => arr.value(row))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            merge_payload!(arr, row => arr.value(row).as_bytes())
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
            merge_payload!(arr, row => arr.value(row))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "failed to downcast to LargeStringArray".to_string())?;
            merge_payload!(arr, row => arr.value(row).as_bytes())
        }
        other => Err(format!(
            "hll aggregate expects HLL/BINARY payload input, got {:?}",
            other
        )),
    }
}

fn hash_update_input_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
) -> Result<(), String> {
    macro_rules! hash_from_bytes {
        ($arr:expr, $row:ident => $bytes:expr) => {{
            for ($row, &base) in state_ptrs.iter().enumerate() {
                if $arr.is_null($row) {
                    continue;
                }
                let hash = murmur_hash64a($bytes.as_ref(), MURMUR_SEED);
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let state = unsafe { get_or_init_state(ptr) };
                update_register_from_hash(state, hash);
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
                let value = if arr.value(row) { 1u8 } else { 0u8 };
                let hash = murmur_hash64a(&[value], MURMUR_SEED);
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let state = unsafe { get_or_init_state(ptr) };
                update_register_from_hash(state, hash);
            }
            Ok(())
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
            }
            TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMillisecondArray".to_string())?;
                hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
            }
            TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMicrosecondArray".to_string())?;
                hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
            }
            TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampNanosecondArray".to_string())?;
                hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
            }
        },
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).to_le_bytes())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).as_bytes())
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "failed to downcast to LargeStringArray".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row).as_bytes())
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
            hash_from_bytes!(arr, row => arr.value(row))
        }
        other => Err(format!("hll_raw does not support input type {:?}", other)),
    }
}

impl AggregateFunction for HllRawAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let Some(_input_type) = input_type else {
            return Err("hll_raw expects exactly one argument".to_string());
        };

        let (kind, output_type) = match canonical_agg_name(func.name.as_str()) {
            "hll_raw" => (AggKind::HllRawHash, DataType::Binary),
            "hll_union" | "hll_raw_agg" => (AggKind::HllRawMerge, DataType::Binary),
            "hll_union_agg" => (AggKind::HllUnionCount, DataType::Int64),
            "ndv" | "approx_count_distinct" | "ds_hll_count_distinct" => {
                (AggKind::HllRawHash, DataType::Int64)
            }
            "ds_hll_count_distinct_union" => (AggKind::HllRawMerge, DataType::Binary),
            "ds_hll_count_distinct_merge" => (AggKind::HllUnionCount, DataType::Int64),
            other => return Err(format!("unsupported hll aggregate function: {}", other)),
        };

        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::HllRawHash | AggKind::HllRawMerge | AggKind::HllUnionCount => (
                std::mem::size_of::<*mut HllRawState>(),
                std::mem::align_of::<*mut HllRawState>(),
            ),
            other => unreachable!("unexpected kind for hll_raw: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "hll_raw input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "hll_raw merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut *mut HllRawState, std::ptr::null_mut());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            let _ = take_state(ptr);
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
            return Err("hll_raw input type mismatch".to_string());
        };
        match spec.kind {
            AggKind::HllRawHash => hash_update_input_array(array, offset, state_ptrs),
            AggKind::HllRawMerge | AggKind::HllUnionCount => {
                merge_input_array(array, offset, state_ptrs)
            }
            _ => Err(format!("unexpected hll aggregate kind: {:?}", spec.kind)),
        }
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("hll_raw merge input type mismatch".to_string());
        };
        merge_input_array(array, offset, state_ptrs)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let output_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };
        match output_type {
            DataType::Binary => {
                let mut builder = BinaryBuilder::new();
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let state = unsafe { get_state(ptr) };
                    let payload = state
                        .and_then(serialize_hll_state)
                        .unwrap_or_else(|| vec![HLL_DATA_EMPTY]);
                    builder.append_value(payload);
                }
                Ok(std::sync::Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let state = unsafe { get_state(ptr) };
                    let value = state.map(estimate_cardinality).unwrap_or(0);
                    builder.append_value(value);
                }
                Ok(std::sync::Arc::new(builder.finish()) as ArrayRef)
            }
            other => Err(format!(
                "hll_raw output type must be Binary or Int64, got {:?}",
                other
            )),
        }
    }
}
