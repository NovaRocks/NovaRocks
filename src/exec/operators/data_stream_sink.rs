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
//! Network stream sink for distributed exchange output.
//!
//! Responsibilities:
//! - Serializes chunks and sends exchange payloads to remote fragment instances over brpc/grpc channels.
//! - Applies batching, backpressure, and destination readiness handling during transmission.
//!
//! Key exported interfaces:
//! - Types: `DataStreamSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::common::config::exchange_max_transmit_batched_bytes;
use crate::common::ids::SlotId;
use crate::common::types::{UniqueId, format_uuid};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::Layout;
use crate::runtime::exchange;
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::service::exchange_sender::{ExchangeSendTask, ExchangeSendTracker, exchange_send_queue};
use crate::{data_sinks, internal_service, partitions, types};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};

use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::profile::clamp_u128_to_i64;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

const NEED_INPUT_LOG_EVERY: u64 = 1;

static NEED_INPUT_BLOCKED_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_need_input() -> bool {
    if NEED_INPUT_LOG_EVERY <= 1 {
        NEED_INPUT_BLOCKED_LOG_COUNT.fetch_add(1, Ordering::Relaxed);
        return true;
    }
    let every = NEED_INPUT_LOG_EVERY.max(2);
    NEED_INPUT_BLOCKED_LOG_COUNT
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(every)
}

// Hash partition implementation (vectorized, no row conversion)
mod data_stream_sink_hash_partition {
    use super::{Chunk, ExprArena, ExprId};
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
        FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        ListArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::compute::cast;
    use arrow::compute::take;
    use arrow::datatypes::{DataType, TimeUnit};
    use std::sync::Arc;

    use crate::common::largeint;
    use crate::exec::hash_table::key_builder::encode_group_key_row;

    // FNV hash constants (from StarRocks BE)
    const FNV_SEED: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    // FNV hash for a single value
    fn fnv_hash_value(value: &[u8]) -> u64 {
        let mut hash = FNV_SEED;
        for &byte in value {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    fn fnv_hash_list_utf8_row(list: &ListArray, values: &StringArray, row: usize) -> u64 {
        let offsets = list.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let mut hash = FNV_SEED;
        let count_bytes = (end.saturating_sub(start) as u64).to_le_bytes();
        hash ^= fnv_hash_value(&count_bytes);
        hash = hash.wrapping_mul(FNV_PRIME);
        for idx in start..end {
            if values.is_null(idx) {
                hash = hash.wrapping_mul(FNV_PRIME);
            } else {
                hash ^= fnv_hash_value(values.value(idx).as_bytes());
                hash = hash.wrapping_mul(FNV_PRIME);
            }
        }
        hash
    }

    fn fnv_hash_list_int32_row(list: &ListArray, values: &Int32Array, row: usize) -> u64 {
        let offsets = list.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let mut hash = FNV_SEED;
        let count_bytes = (end.saturating_sub(start) as u64).to_le_bytes();
        hash ^= fnv_hash_value(&count_bytes);
        hash = hash.wrapping_mul(FNV_PRIME);
        for idx in start..end {
            if values.is_null(idx) {
                hash = hash.wrapping_mul(FNV_PRIME);
            } else {
                let bytes = values.value(idx).to_le_bytes();
                hash ^= fnv_hash_value(&bytes);
                hash = hash.wrapping_mul(FNV_PRIME);
            }
        }
        hash
    }

    // Compute FNV hash for each row in an array
    fn compute_fnv_hash_array(array: &ArrayRef) -> Result<Vec<u64>, String> {
        let len = array.len();
        let mut hash_values = vec![FNV_SEED; len];

        match array.data_type() {
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = if arr.value(i) { 1u8 } else { 0u8 };
                        hash_values[i] ^= val as u64;
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_bits().to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_bits().to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        hash_values[i] ^= fnv_hash_value(val.as_bytes());
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Timestamp(unit, _tz) => {
                for i in 0..len {
                    if array.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = match unit {
                            TimeUnit::Second => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampSecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampSecondArray".to_string()
                                    })?;
                                arr.value(i) as i64
                            }
                            TimeUnit::Millisecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampMillisecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampMillisecondArray"
                                            .to_string()
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Microsecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampMicrosecondArray"
                                            .to_string()
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Nanosecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampNanosecondArray".to_string()
                                    })?;
                                arr.value(i)
                            }
                        };
                        let bytes = val.to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::Decimal256(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
                for i in 0..len {
                    if arr.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let value = largeint::i128_from_be_bytes(arr.value(i)).map_err(|e| {
                            format!("hash_partition: decode LARGEINT failed at row {}: {}", i, e)
                        })?;
                        let bytes = value.to_le_bytes();
                        hash_values[i] ^= fnv_hash_value(&bytes);
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
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
                for i in 0..len {
                    if list.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let row_hash = fnv_hash_list_utf8_row(list, values, i);
                        hash_values[i] ^= row_hash;
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
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
                for i in 0..len {
                    if list.is_null(i) {
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    } else {
                        let row_hash = fnv_hash_list_int32_row(list, values, i);
                        hash_values[i] ^= row_hash;
                        hash_values[i] = hash_values[i].wrapping_mul(FNV_PRIME);
                    }
                }
            }
            DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
                for (row, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    match encode_group_key_row(array, row)? {
                        Some(encoded) => {
                            *hash_value ^= fnv_hash_value(&encoded);
                            *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                        }
                        None => {
                            *hash_value = hash_value.wrapping_mul(FNV_PRIME);
                        }
                    }
                }
            }
            _ => {
                return Err(format!(
                    "hash_partition: unsupported array type for FNV hash: {:?}",
                    array.data_type()
                ));
            }
        }

        Ok(hash_values)
    }

    // Compute CRC32 hash for each row in an array (for BUCKET_SHUFFLE_HASH_PARTITIONED)
    fn compute_crc32_hash_array(array: &ArrayRef) -> Result<Vec<u32>, String> {
        let len = array.len();
        let mut hash_values = vec![0u32; len];

        fn crc32_hash_value(value: &[u8]) -> u32 {
            let mut crc: u32 = 0xffffffff;
            for &byte in value {
                crc ^= byte as u32;
                for _ in 0..8 {
                    if crc & 1 != 0 {
                        crc = (crc >> 1) ^ 0xedb88320;
                    } else {
                        crc >>= 1;
                    }
                }
            }
            crc ^ 0xffffffff
        }

        match array.data_type() {
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = if arr.value(i) { 1u8 } else { 0u8 };
                        hash_values[i] = crc32_hash_value(&[val]);
                    }
                }
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_bits().to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_bits().to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        hash_values[i] = crc32_hash_value(val.as_bytes());
                    }
                }
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Timestamp(unit, _tz) => {
                for i in 0..len {
                    if !array.is_null(i) {
                        let val = match unit {
                            TimeUnit::Second => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampSecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampSecondArray".to_string()
                                    })?;
                                arr.value(i) as i64
                            }
                            TimeUnit::Millisecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampMillisecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampMillisecondArray"
                                            .to_string()
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Microsecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampMicrosecondArray"
                                            .to_string()
                                    })?;
                                arr.value(i)
                            }
                            TimeUnit::Nanosecond => {
                                let arr = array
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()
                                    .ok_or_else(|| {
                                        "failed to downcast to TimestampNanosecondArray".to_string()
                                    })?;
                                arr.value(i)
                            }
                        };
                        let bytes = val.to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::Decimal256(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        let bytes = val.to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
            }
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
                for i in 0..len {
                    if !arr.is_null(i) {
                        let value = largeint::i128_from_be_bytes(arr.value(i)).map_err(|e| {
                            format!("hash_partition: decode LARGEINT failed at row {}: {}", i, e)
                        })?;
                        let bytes = value.to_le_bytes();
                        hash_values[i] = crc32_hash_value(&bytes);
                    }
                }
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
                for i in 0..len {
                    if list.is_null(i) {
                        continue;
                    }
                    let offsets = list.value_offsets();
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    let mut encoded = Vec::new();
                    encoded.extend_from_slice(&(end.saturating_sub(start) as u64).to_le_bytes());
                    for idx in start..end {
                        if values.is_null(idx) {
                            encoded.push(0);
                        } else {
                            encoded.push(1);
                            encoded.extend_from_slice(values.value(idx).as_bytes());
                        }
                    }
                    hash_values[i] = crc32_hash_value(&encoded);
                }
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
                for i in 0..len {
                    if list.is_null(i) {
                        continue;
                    }
                    let offsets = list.value_offsets();
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    let mut encoded = Vec::new();
                    encoded.extend_from_slice(&(end.saturating_sub(start) as u64).to_le_bytes());
                    for idx in start..end {
                        if values.is_null(idx) {
                            encoded.push(0);
                        } else {
                            encoded.push(1);
                            encoded.extend_from_slice(&values.value(idx).to_le_bytes());
                        }
                    }
                    hash_values[i] = crc32_hash_value(&encoded);
                }
            }
            DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
                for (row, hash_value) in hash_values.iter_mut().enumerate().take(len) {
                    if let Some(encoded) = encode_group_key_row(array, row)? {
                        *hash_value = crc32_hash_value(&encoded);
                    }
                }
            }
            _ => {
                return Err(format!(
                    "hash_partition: unsupported array type for CRC32 hash: {:?}",
                    array.data_type()
                ));
            }
        }

        Ok(hash_values)
    }

    fn canonicalize_hash_array(array: &ArrayRef) -> Result<ArrayRef, String> {
        match array.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 => cast(array, &DataType::Int64)
                .map_err(|e| format!("hash_partition: cast to Int64 failed: {e}")),
            DataType::Float32 => cast(array, &DataType::Float64)
                .map_err(|e| format!("hash_partition: cast to Float64 failed: {e}")),
            _ => Ok(array.clone()),
        }
    }

    // Partition a chunk based on hash values without converting to rows
    pub fn partition_chunk_by_hash(
        chunk: &Chunk,
        partition_exprs: &[ExprId],
        arena: &ExprArena,
        num_partitions: usize,
        use_crc32: bool,
    ) -> Result<Vec<Chunk>, String> {
        if chunk.is_empty() {
            return Ok(vec![Chunk::default(); num_partitions]);
        }

        // Evaluate partition expressions to get arrays
        let mut partition_arrays = Vec::with_capacity(partition_exprs.len());
        for expr_id in partition_exprs {
            let array = arena.eval(*expr_id, chunk)?;
            partition_arrays.push(array);
        }

        partition_chunk_by_hash_arrays(chunk, &partition_arrays, num_partitions, use_crc32)
    }

    pub fn partition_chunk_by_hash_arrays(
        chunk: &Chunk,
        partition_arrays: &[ArrayRef],
        num_partitions: usize,
        use_crc32: bool,
    ) -> Result<Vec<Chunk>, String> {
        if chunk.is_empty() {
            return Ok(vec![Chunk::default(); num_partitions]);
        }

        let partition_arrays = partition_arrays
            .iter()
            .map(canonicalize_hash_array)
            .collect::<Result<Vec<_>, _>>()?;

        let num_rows = chunk.len();

        // Compute hash values for each row
        let hash_values = if use_crc32 {
            // CRC32 hash for BUCKET_SHUFFLE_HASH_PARTITIONED
            let mut crc32_hashes = vec![0u32; num_rows];
            for array in &partition_arrays {
                let arr_hashes = compute_crc32_hash_array(array)?;
                for i in 0..num_rows {
                    crc32_hashes[i] = crc32_hashes[i].wrapping_add(arr_hashes[i]);
                }
            }
            crc32_hashes.iter().map(|&h| h as u64).collect()
        } else {
            // FNV hash for HASH_PARTITIONED
            let mut fnv_hashes = vec![FNV_SEED; num_rows];
            for array in &partition_arrays {
                let arr_hashes = compute_fnv_hash_array(array)?;
                for i in 0..num_rows {
                    fnv_hashes[i] ^= arr_hashes[i];
                    fnv_hashes[i] = fnv_hashes[i].wrapping_mul(FNV_PRIME);
                }
            }
            fnv_hashes
        };

        // Compute partition index for each row
        let partition_indices: Vec<usize> = hash_values
            .iter()
            .map(|&h| (h as usize) % num_partitions)
            .collect();

        // Build row index arrays for each partition (similar to StarRocks BE)
        let mut partition_row_indices: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
        for (row_idx, &part_idx) in partition_indices.iter().enumerate() {
            partition_row_indices[part_idx].push(row_idx as u32);
        }

        // Create chunks for each partition using Arrow take
        let mut partition_chunks = Vec::with_capacity(num_partitions);
        for part_idx in 0..num_partitions {
            if partition_row_indices[part_idx].is_empty() {
                partition_chunks.push(Chunk::default());
            } else {
                // Use Arrow take to select rows for this partition
                let indices =
                    arrow::array::UInt32Array::from(partition_row_indices[part_idx].clone());
                let indices_ref = Arc::new(indices) as arrow::array::ArrayRef;

                let mut new_columns = Vec::with_capacity(chunk.batch.num_columns());
                for col in chunk.batch.columns() {
                    let taken = take(col.as_ref(), &indices_ref, None)
                        .map_err(|e| format!("Arrow take failed: {}", e))?;
                    new_columns.push(taken);
                }

                let new_batch =
                    arrow::record_batch::RecordBatch::try_new(chunk.batch.schema(), new_columns)
                        .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;

                partition_chunks.push(Chunk::new(new_batch));
            }
        }

        Ok(partition_chunks)
    }
}

pub(crate) use data_stream_sink_hash_partition::partition_chunk_by_hash;
pub(crate) use data_stream_sink_hash_partition::partition_chunk_by_hash_arrays;

/// Factory for distributed stream sinks that serialize and transmit chunks to remote fragment instances.
pub(crate) struct DataStreamSinkFactory {
    name: String,
    init_error: Option<String>,
    sink: data_sinks::TDataStreamSink,
    exec_params: internal_service::TPlanFragmentExecParams,
    layout: Layout,
    plan_node_id: i32,
    output_columns: Vec<SlotId>,
    last_query_id: Option<String>,
    fe_addr: Option<types::TNetworkAddress>,
    finish_state: Arc<DataStreamSinkFinishState>,
    shared_sequence: Arc<AtomicI64>,
}

impl DataStreamSinkFactory {
    pub(crate) fn new(
        sink: data_sinks::TDataStreamSink,
        exec_params: internal_service::TPlanFragmentExecParams,
        layout: Layout,
        plan_node_id: i32,
        last_query_id: Option<String>,
        fe_addr: Option<types::TNetworkAddress>,
    ) -> Self {
        // Align with StarRocks FE ExplainAnalyzer: ExchangeSinkOperator uses the *upstream plan node id*
        // (not the destination exchange node id) as `plan_node_id`.
        let name = if plan_node_id >= 0 {
            format!("EXCHANGE_SINK (id={plan_node_id})")
        } else {
            "EXCHANGE_SINK".to_string()
        };
        let mut init_error = None;
        let mut output_columns = Vec::new();
        if let Some(cols) = sink.output_columns.as_ref() {
            let mut seen = std::collections::HashSet::new();
            output_columns.reserve(cols.len());
            for &cid in cols {
                let slot_id = match SlotId::try_from(cid) {
                    Ok(v) => v,
                    Err(e) => {
                        init_error = Some(format!(
                            "DATA_STREAM_SINK: invalid output_columns slot id: {e}"
                        ));
                        break;
                    }
                };
                if !seen.insert(slot_id) {
                    init_error = Some(format!(
                        "DATA_STREAM_SINK: duplicate output_columns slot id: {slot_id}"
                    ));
                    break;
                }
                output_columns.push(slot_id);
            }
        }

        Self {
            name,
            init_error,
            sink,
            exec_params,
            layout,
            plan_node_id,
            output_columns,
            last_query_id,
            fe_addr,
            finish_state: Arc::new(DataStreamSinkFinishState::default()),
            shared_sequence: Arc::new(AtomicI64::new(0)),
        }
    }
}

impl OperatorFactory for DataStreamSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        use crate::novarocks_logging::debug;

        let sender_id = self
            .exec_params
            .sender_id
            .unwrap_or_else(|| (self.exec_params.fragment_instance_id.lo as i32) & 0x7fffffff);
        let be_number = 0i32;

        if driver_id == 0 {
            let part_type = match self.sink.output_partition.type_ {
                partitions::TPartitionType::UNPARTITIONED => "UNPARTITIONED",
                partitions::TPartitionType::RANDOM => "RANDOM",
                partitions::TPartitionType::HASH_PARTITIONED => "HASH_PARTITIONED",
                partitions::TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED => {
                    "BUCKET_SHUFFLE_HASH_PARTITIONED"
                }
                _ => "UNKNOWN",
            };
            let dest_count = self
                .exec_params
                .destinations
                .as_ref()
                .map(|v| v.len())
                .unwrap_or(0);
            let dest_preview = self
                .exec_params
                .destinations
                .as_ref()
                .map(|v| {
                    v.iter()
                        .take(3)
                        .map(|d| format_uuid(d.fragment_instance_id.hi, d.fragment_instance_id.lo))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default();
            debug!(
                "DataStreamSink created: finst={} plan_node_id={} dest_node_id={} part_type={} dop={} sender_id={} be_number={} destinations={} dest_preview=[{}]",
                format_uuid(
                    self.exec_params.fragment_instance_id.hi,
                    self.exec_params.fragment_instance_id.lo
                ),
                self.plan_node_id,
                self.sink.dest_node_id,
                part_type,
                dop.max(1),
                sender_id,
                be_number,
                dest_count,
                dest_preview
            );
        }

        let mut arena = ExprArena::default();
        let mut init_error = self.init_error.clone();
        let expr_ids = if init_error.is_none()
            && matches!(
                self.sink.output_partition.type_,
                partitions::TPartitionType::HASH_PARTITIONED
                    | partitions::TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED
            ) {
            let exprs = self
                .sink
                .output_partition
                .partition_exprs
                .as_ref()
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            match exprs
                .iter()
                .map(|e| {
                    lower_t_expr(
                        e,
                        &mut arena,
                        &self.layout,
                        self.last_query_id.as_deref(),
                        self.fe_addr.as_ref(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(ids) => {
                    let mut has_variant = false;
                    for id in &ids {
                        if let Some(dt) = arena.data_type(*id) {
                            if matches!(dt, arrow::datatypes::DataType::LargeBinary) {
                                has_variant = true;
                                break;
                            }
                        }
                    }
                    if has_variant {
                        init_error = Some(
                            "VARIANT is not supported in HASH_PARTITIONED partition keys"
                                .to_string(),
                        );
                        Vec::new()
                    } else {
                        ids
                    }
                }
                Err(err) => {
                    init_error = Some(err);
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        let send_observable = Arc::new(Observable::new());
        exchange_send_queue().register_send_observer(&send_observable);

        Box::new(DataStreamSinkOperator {
            name: self.name.clone(),
            sink: self.sink.clone(),
            exec_params: self.exec_params.clone(),
            arena,
            expr_ids,
            init_error,
            driver_id,
            sender_id,
            be_number,
            output_columns: self.output_columns.clone(),
            shared_sequence: Arc::clone(&self.shared_sequence),
            random_next: 0,
            pending_per_dest: Vec::new(),
            pending_bytes_per_dest: Vec::new(),
            pending_payloads_per_dest: Vec::new(),
            max_transmit_batched_bytes: exchange_max_transmit_batched_bytes().max(1),
            finished: AtomicBool::new(false),
            finishing: AtomicBool::new(false),
            send_tracker: ExchangeSendTracker::new(),
            send_observable,
            error_state: None,
            finish_state: Arc::clone(&self.finish_state),
            profile_initialized: false,
            profiles: None,
            pending_chunks_mem_tracker: None,
            pending_payload_mem_tracker: None,
            send_queue_mem_tracker: None,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct PendingPayload {
    payload: Vec<u8>,
    payload_bytes: usize,
    encode_ns: u128,
    sequence: i64,
    eos: bool,
    accounting: Option<TrackedBytes>,
}

enum PayloadEnqueue {
    Enqueued,
    NoCapacity(PendingPayload),
}

struct DataStreamSinkOperator {
    name: String,
    sink: data_sinks::TDataStreamSink,
    exec_params: internal_service::TPlanFragmentExecParams,
    arena: ExprArena,
    expr_ids: Vec<ExprId>,
    init_error: Option<String>,
    driver_id: i32,
    sender_id: i32,
    be_number: i32,
    output_columns: Vec<SlotId>,
    shared_sequence: Arc<AtomicI64>,
    random_next: usize,
    pending_per_dest: Vec<VecDeque<Chunk>>,
    pending_bytes_per_dest: Vec<usize>,
    pending_payloads_per_dest: Vec<Option<PendingPayload>>,
    max_transmit_batched_bytes: usize,
    finished: AtomicBool,
    finishing: AtomicBool,
    send_tracker: Arc<ExchangeSendTracker>,
    send_observable: Arc<Observable>,
    error_state: Option<Arc<RuntimeErrorState>>,
    finish_state: Arc<DataStreamSinkFinishState>,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    pending_chunks_mem_tracker: Option<Arc<MemTracker>>,
    pending_payload_mem_tracker: Option<Arc<MemTracker>>,
    send_queue_mem_tracker: Option<Arc<MemTracker>>,
}

impl Operator for DataStreamSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let pending_chunks = MemTracker::new_child("PendingChunks", &tracker);
        self.pending_chunks_mem_tracker = Some(Arc::clone(&pending_chunks));
        for pending in self.pending_per_dest.iter_mut() {
            for chunk in pending.iter_mut() {
                chunk.transfer_to(&pending_chunks);
            }
        }

        let pending_payloads = MemTracker::new_child("PendingPayloads", &tracker);
        self.pending_payload_mem_tracker = Some(Arc::clone(&pending_payloads));
        for payload in self
            .pending_payloads_per_dest
            .iter_mut()
            .filter_map(|p| p.as_mut())
        {
            let bytes = payload.payload.capacity().max(payload.payload.len());
            match payload.accounting.as_mut() {
                Some(accounting) => accounting.transfer_to(Arc::clone(&pending_payloads)),
                None => {
                    payload.accounting =
                        Some(TrackedBytes::new(bytes, Arc::clone(&pending_payloads)));
                }
            }
        }

        let send_queue = MemTracker::new_child("SendQueuePayloads", &tracker);
        self.send_queue_mem_tracker = Some(send_queue);
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn prepare(&mut self) -> Result<(), String> {
        // Align with StarRocks: count actual sink drivers prepared, not planned DOP.
        self.finish_state.register_driver();
        crate::novarocks_logging::debug!(
            "DataStreamSink registered driver: finst={} driver_id={} dest_node_id={} sender_id={} remaining_drivers={}",
            format_uuid(
                self.exec_params.fragment_instance_id.hi,
                self.exec_params.fragment_instance_id.lo
            ),
            self.driver_id,
            self.sink.dest_node_id,
            self.sender_id,
            self.finish_state.remaining_drivers.load(Ordering::SeqCst)
        );
        Ok(())
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.maybe_mark_finished()
    }
}

impl DataStreamSinkOperator {
    fn pending_chunk_bytes_total(&self) -> usize {
        self.pending_bytes_per_dest.iter().sum()
    }

    fn pending_payload_bytes_total(&self) -> usize {
        self.pending_payloads_per_dest
            .iter()
            .map(|p| p.as_ref().map(|v| v.payload_bytes).unwrap_or(0))
            .sum()
    }

    fn pending_payload_count(&self) -> usize {
        self.pending_payloads_per_dest
            .iter()
            .filter(|p| p.is_some())
            .count()
    }

    fn has_pending_payloads(&self) -> bool {
        self.pending_payloads_per_dest.iter().any(|p| p.is_some())
    }

    fn has_pending_data(&self) -> bool {
        self.pending_chunk_bytes_total() > 0 || self.has_pending_payloads()
    }

    fn pending_payloads_can_send(&self) -> bool {
        let max_inflight = exchange_send_queue().max_inflight_bytes();
        for payload in self
            .pending_payloads_per_dest
            .iter()
            .filter_map(|p| p.as_ref())
        {
            if payload.payload_bytes > max_inflight {
                continue;
            }
            if !exchange_send_queue().can_reserve(payload.payload_bytes) {
                return false;
            }
        }
        true
    }

    fn pending_batch_reserve_bytes(&self) -> usize {
        self.pending_bytes_per_dest
            .iter()
            .filter(|bytes| **bytes > 0)
            .map(|bytes| (*bytes).min(self.max_transmit_batched_bytes))
            .max()
            .unwrap_or(0)
    }

    fn log_need_input_blocked(&self, reason: &str, reserve_bytes: usize) {
        if !should_log_need_input() {
            return;
        }
        use crate::novarocks_logging::debug;
        let inflight_bytes = exchange_send_queue().inflight_bytes();
        let max_inflight_bytes = exchange_send_queue().max_inflight_bytes();
        debug!(
            "DataStreamSink need_input blocked: reason={} finst={} driver_id={} sender_id={} pending_chunk_bytes={} pending_payloads={} pending_payload_bytes={} reserve_bytes={} inflight_bytes={} max_inflight_bytes={} finishing={} send_idle={} send_inflight_bytes={}",
            reason,
            format_uuid(
                self.exec_params.fragment_instance_id.hi,
                self.exec_params.fragment_instance_id.lo
            ),
            self.driver_id,
            self.sender_id,
            self.pending_chunk_bytes_total(),
            self.pending_payload_count(),
            self.pending_payload_bytes_total(),
            reserve_bytes,
            inflight_bytes,
            max_inflight_bytes,
            self.finishing.load(Ordering::Acquire),
            self.send_tracker.is_idle(),
            self.send_tracker.inflight_bytes()
        );
    }

    fn ensure_error_state(&mut self, state: &RuntimeState) {
        if self.error_state.is_none() {
            self.error_state = Some(state.error_state());
        }
    }

    fn current_error(&self) -> Option<String> {
        self.error_state.as_ref().and_then(|state| state.error())
    }

    fn maybe_mark_finished(&self) -> bool {
        if self.finished.load(Ordering::Acquire) {
            return true;
        }
        if self.finishing.load(Ordering::Acquire)
            && !self.has_pending_data()
            && self.send_tracker.is_idle()
        {
            self.finished.store(true, Ordering::Release);
            return true;
        }
        false
    }

    fn init_profile_if_needed(&mut self) {
        if self.profile_initialized {
            return;
        }
        self.profile_initialized = true;

        let channel_num = self.destinations().len() as u128;
        let dest_id = self.sink.dest_node_id;
        let part_type = match self.sink.output_partition.type_ {
            partitions::TPartitionType::UNPARTITIONED => "UNPARTITIONED",
            partitions::TPartitionType::RANDOM => "RANDOM",
            partitions::TPartitionType::HASH_PARTITIONED => "HASH_PARTITIONED",
            partitions::TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED => {
                "BUCKET_SHUFFLE_HASH_PARTITIONED"
            }
            _ => "UNKNOWN",
        };
        if let Some(profile) = self.profiles.as_ref() {
            profile
                .common
                .add_info_string("DestID", format!("{dest_id}"));
            profile.common.add_info_string("PartType", part_type);
            profile.common.counter_add(
                "ChannelNum",
                crate::metrics::TUnit::UNIT,
                clamp_u128_to_i64(channel_num),
            );
        }
    }

    fn destinations(&self) -> &[data_sinks::TPlanFragmentDestination] {
        self.exec_params
            .destinations
            .as_ref()
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    fn partition_chunk(&mut self, chunk: &Chunk) -> Result<Vec<Vec<Chunk>>, String> {
        // Get destinations first to avoid borrowing self
        let dests: Vec<data_sinks::TPlanFragmentDestination> = self.destinations().to_vec();
        if dests.is_empty() {
            return Ok(vec![]);
        }

        if chunk.is_empty() {
            return Ok(vec![Vec::new(); dests.len()]);
        }

        let n = dests.len();

        match self.sink.output_partition.type_ {
            partitions::TPartitionType::UNPARTITIONED => {
                // Broadcast to all destinations
                let mut per_dest_chunks: Vec<Vec<Chunk>> = Vec::with_capacity(n);
                for _ in 0..n {
                    per_dest_chunks.push(vec![chunk.clone()]);
                }
                Ok(per_dest_chunks)
            }
            partitions::TPartitionType::RANDOM => {
                // Random partition: use row indices
                let num_rows = chunk.len();
                let mut partition_row_indices: Vec<Vec<u32>> = vec![Vec::new(); n];

                for row_idx in 0..num_rows {
                    let part_idx = self.random_next % n;
                    self.random_next = self.random_next.wrapping_add(1);
                    partition_row_indices[part_idx].push(row_idx as u32);
                }

                // Create chunks for each partition using Arrow take
                let mut per_dest_chunks: Vec<Vec<Chunk>> = Vec::with_capacity(n);
                for part_idx in 0..n {
                    if partition_row_indices[part_idx].is_empty() {
                        per_dest_chunks.push(Vec::new());
                    } else {
                        let indices = arrow::array::UInt32Array::from(
                            partition_row_indices[part_idx].clone(),
                        );
                        let indices_ref = Arc::new(indices) as arrow::array::ArrayRef;

                        let mut new_columns = Vec::with_capacity(chunk.batch.num_columns());
                        for col in chunk.batch.columns() {
                            let taken = arrow::compute::take(col.as_ref(), &indices_ref, None)
                                .map_err(|e| format!("Arrow take failed: {}", e))?;
                            new_columns.push(taken);
                        }

                        let new_batch = arrow::record_batch::RecordBatch::try_new(
                            chunk.batch.schema(),
                            new_columns,
                        )
                        .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;

                        per_dest_chunks.push(vec![Chunk::new(new_batch)]);
                    }
                }
                Ok(per_dest_chunks)
            }
            partitions::TPartitionType::HASH_PARTITIONED
            | partitions::TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED => {
                if self.expr_ids.is_empty() {
                    return Err("HASH_PARTITIONED missing partition_exprs".to_string());
                }

                // Use vectorized hash partition without row conversion
                let use_crc32 = matches!(
                    self.sink.output_partition.type_,
                    partitions::TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED
                );

                let partition_chunks =
                    partition_chunk_by_hash(chunk, &self.expr_ids, &self.arena, n, use_crc32)
                        .map_err(|e| e.to_string())?;

                // Convert Vec<Chunk> to Vec<Vec<Chunk>>
                let mut per_dest_chunks: Vec<Vec<Chunk>> = Vec::with_capacity(n);
                for part_chunk in partition_chunks {
                    if part_chunk.is_empty() {
                        per_dest_chunks.push(Vec::new());
                    } else {
                        per_dest_chunks.push(vec![part_chunk]);
                    }
                }
                Ok(per_dest_chunks)
            }
            other => {
                return Err(format!(
                    "unsupported DATA_STREAM_SINK partition type: {:?}",
                    other
                ));
            }
        }
    }

    fn try_enqueue_payload(
        &mut self,
        dest: &data_sinks::TPlanFragmentDestination,
        mut pending: PendingPayload,
        allow_overflow: bool,
    ) -> Result<PayloadEnqueue, String> {
        let allow_overflow =
            allow_overflow || pending.payload_bytes > exchange_send_queue().max_inflight_bytes();
        let reserve_bytes = pending.payload_bytes.max(1);

        let addr = dest
            .brpc_server
            .as_ref()
            .or_else(|| dest.deprecated_server.as_ref())
            .ok_or_else(|| "missing destination brpc_server".to_string())?;
        let dest_finst_id = UniqueId {
            hi: dest.fragment_instance_id.hi,
            lo: dest.fragment_instance_id.lo,
        };
        let error_state = self
            .error_state
            .as_ref()
            .ok_or_else(|| "missing runtime error state".to_string())?;
        if !allow_overflow && !exchange_send_queue().try_reserve_bytes(reserve_bytes) {
            return Ok(PayloadEnqueue::NoCapacity(pending));
        }

        if let (Some(tracker), Some(accounting)) = (
            self.send_queue_mem_tracker.as_ref(),
            pending.accounting.as_mut(),
        ) {
            accounting.transfer_to(Arc::clone(tracker));
        }
        let dest_port = addr.port as u16;
        let task = ExchangeSendTask {
            dest_host: addr.hostname.to_string(),
            dest_port,
            finst_id: dest_finst_id,
            node_id: self.sink.dest_node_id,
            sender_id: self.sender_id,
            be_number: self.be_number,
            eos: pending.eos,
            sequence: pending.sequence,
            payload: pending.payload,
            payload_accounting: pending.accounting,
            encode_ns: pending.encode_ns,
            payload_bytes: pending.payload_bytes,
            profiles: self.profiles.clone(),
            notify: Arc::clone(&self.send_observable),
            error_state: Arc::clone(error_state),
            tracker: Arc::clone(&self.send_tracker),
        };
        if allow_overflow {
            exchange_send_queue().try_submit(task, true)?;
            return Ok(PayloadEnqueue::Enqueued);
        }
        exchange_send_queue().submit_reserved(task, reserve_bytes)?;
        Ok(PayloadEnqueue::Enqueued)
    }

    fn transmit_partition(
        &mut self,
        dest: &data_sinks::TPlanFragmentDestination,
        chunks: &[Chunk],
        eos: bool,
        allow_overflow: bool,
    ) -> Result<PayloadEnqueue, String> {
        use crate::novarocks_logging::debug;

        self.init_profile_if_needed();

        let dest_finst_id = UniqueId {
            hi: dest.fragment_instance_id.hi,
            lo: dest.fragment_instance_id.lo,
        };

        let row_count: usize = chunks.iter().map(|c| c.len()).sum();
        let sequence = self.shared_sequence.fetch_add(1, Ordering::SeqCst);
        debug!(
            "DataStreamSink::transmit_partition: dest_finst={} node_id={} sender_id={} chunks={} rows={} eos={} seq={}",
            dest_finst_id,
            self.sink.dest_node_id,
            self.sender_id,
            chunks.len(),
            row_count,
            eos,
            sequence
        );

        let projected_storage;
        let chunks = if self.output_columns.is_empty() || chunks.is_empty() {
            chunks
        } else {
            projected_storage = chunks
                .iter()
                .map(|c| project_chunk_by_slot_ids(c, &self.output_columns))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            projected_storage.as_slice()
        };

        let encode_start = std::time::Instant::now();
        let payload =
            exchange::encode_chunks(chunks).map_err(|e| format!("failed to encode chunks: {e}"))?;
        let encode_ns = encode_start.elapsed().as_nanos() as u128;
        let payload_bytes = payload.len();
        let payload_capacity_bytes = payload.capacity().max(payload_bytes);

        if let Some(profile) = self.profiles.as_ref() {
            profile.common.counter_add(
                "SerializeChunkTime",
                crate::metrics::TUnit::TIME_NS,
                clamp_u128_to_i64(encode_ns),
            );
            profile.common.counter_add(
                "SerializedBytes",
                crate::metrics::TUnit::BYTES,
                clamp_u128_to_i64(payload_bytes as u128),
            );
        }

        let accounting = self
            .pending_payload_mem_tracker
            .as_ref()
            .map(|tracker| TrackedBytes::new(payload_capacity_bytes, Arc::clone(tracker)));
        let payload = PendingPayload {
            payload,
            payload_bytes,
            encode_ns,
            sequence,
            eos,
            accounting,
        };

        match self.try_enqueue_payload(dest, payload, allow_overflow)? {
            PayloadEnqueue::Enqueued => {
                debug!(
                    "DataStreamSink::transmit_partition enqueued: dest_finst={} node_id={} eos={} seq={} bytes={}",
                    dest_finst_id, self.sink.dest_node_id, eos, sequence, payload_bytes
                );
                Ok(PayloadEnqueue::Enqueued)
            }
            PayloadEnqueue::NoCapacity(payload) => Ok(PayloadEnqueue::NoCapacity(payload)),
        }
    }

    fn ensure_pending_buffers_initialized(&mut self) {
        let dest_count = self.destinations().len();
        if self.pending_per_dest.len() == dest_count {
            return;
        }
        self.pending_per_dest = (0..dest_count).map(|_| VecDeque::new()).collect();
        self.pending_bytes_per_dest = vec![0; dest_count];
        self.pending_payloads_per_dest = (0..dest_count).map(|_| None).collect();
    }

    fn buffer_chunk(&mut self, chunk: Chunk) -> Result<(), String> {
        self.ensure_pending_buffers_initialized();
        let per_dest_chunks = self.partition_chunk(&chunk)?;
        for (i, mut chunks) in per_dest_chunks.into_iter().enumerate() {
            if chunks.is_empty() {
                continue;
            }
            if let Some(tracker) = self.pending_chunks_mem_tracker.as_ref() {
                for chunk in chunks.iter_mut() {
                    chunk.transfer_to(tracker);
                }
            }
            let bytes = chunks
                .iter()
                .map(|c| c.batch.get_array_memory_size())
                .sum::<usize>();
            self.pending_per_dest[i].extend(chunks);
            self.pending_bytes_per_dest[i] = self.pending_bytes_per_dest[i].saturating_add(bytes);
        }
        Ok(())
    }

    fn drain_pending_batch(&mut self, dest_idx: usize) -> Result<(Vec<Chunk>, usize), String> {
        let max_bytes = self.max_transmit_batched_bytes.max(1);
        let pending = self
            .pending_per_dest
            .get_mut(dest_idx)
            .ok_or_else(|| "pending buffer index out of range".to_string())?;
        if pending.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        while let Some(front) = pending.front() {
            let chunk_bytes = front.batch.get_array_memory_size();
            if !batch.is_empty() && batch_bytes.saturating_add(chunk_bytes) > max_bytes {
                break;
            }
            let chunk = pending
                .pop_front()
                .ok_or_else(|| "pending buffer empty unexpectedly".to_string())?;
            batch_bytes = batch_bytes.saturating_add(chunk_bytes);
            batch.push(chunk);
            if batch_bytes >= max_bytes {
                break;
            }
        }

        if let Some(bytes) = self.pending_bytes_per_dest.get_mut(dest_idx) {
            *bytes = bytes.saturating_sub(batch_bytes);
        }
        Ok((batch, batch_bytes))
    }

    fn flush_pending(&mut self, force: bool, allow_overflow: bool) -> Result<(), String> {
        self.ensure_pending_buffers_initialized();
        let dests: Vec<data_sinks::TPlanFragmentDestination> = self.destinations().to_vec();
        for (i, dest) in dests.iter().enumerate() {
            let pending_payload = self
                .pending_payloads_per_dest
                .get_mut(i)
                .ok_or_else(|| "pending payload index out of range".to_string())?
                .take();
            if let Some(payload) = pending_payload {
                match self.try_enqueue_payload(dest, payload, allow_overflow)? {
                    PayloadEnqueue::Enqueued => {}
                    PayloadEnqueue::NoCapacity(payload) => {
                        self.pending_payloads_per_dest[i] = Some(payload);
                        return Ok(());
                    }
                }
            }

            loop {
                let bytes = self.pending_bytes_per_dest.get(i).copied().unwrap_or(0);
                if bytes == 0 {
                    break;
                }
                if !force && bytes < self.max_transmit_batched_bytes {
                    break;
                }
                let (chunks, _batch_bytes) = self.drain_pending_batch(i)?;
                if chunks.is_empty() {
                    break;
                }
                match self.transmit_partition(dest, &chunks, false, allow_overflow)? {
                    PayloadEnqueue::Enqueued => {}
                    PayloadEnqueue::NoCapacity(payload) => {
                        self.pending_payloads_per_dest[i] = Some(payload);
                        return Ok(());
                    }
                }
                if !force {
                    break;
                }
            }
        }
        Ok(())
    }

    fn send_eos(&mut self) -> Result<(), String> {
        self.ensure_pending_buffers_initialized();
        let dests: Vec<data_sinks::TPlanFragmentDestination> = self.destinations().to_vec();
        for dest in dests.iter() {
            match self.transmit_partition(dest, &[], true, true)? {
                PayloadEnqueue::Enqueued => {}
                PayloadEnqueue::NoCapacity(_) => {
                    return Err("exchange send EOS unexpectedly blocked".to_string());
                }
            }
        }
        Ok(())
    }
}

impl ProcessorOperator for DataStreamSinkOperator {
    fn need_input(&self) -> bool {
        if self.maybe_mark_finished() {
            return false;
        }
        if self.finishing.load(Ordering::Acquire) {
            let ready = !self.has_pending_data() && self.send_tracker.is_idle();
            if !ready {
                self.log_need_input_blocked("finishing_wait", 0);
            }
            return ready;
        }
        if self.has_pending_payloads() && !self.pending_payloads_can_send() {
            self.log_need_input_blocked("pending_payloads", 0);
            return false;
        }
        let pending_reserve_bytes = self.pending_batch_reserve_bytes();
        if pending_reserve_bytes > 0 && !exchange_send_queue().can_reserve(pending_reserve_bytes) {
            self.log_need_input_blocked("reserve_bytes", pending_reserve_bytes);
            return false;
        }
        true
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
        }
        if self.finished.load(Ordering::Acquire) {
            return Ok(());
        }
        self.ensure_error_state(_state);
        if let Some(err) = self.current_error() {
            return Err(err);
        }
        self.init_profile_if_needed();
        self.flush_pending(false, false)?;
        if chunk.is_empty() {
            return Ok(());
        }
        self.buffer_chunk(chunk)?;
        self.flush_pending(false, false)?;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        use crate::novarocks_logging::debug;

        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
        }
        if self.finished.load(Ordering::Acquire) {
            return Ok(());
        }
        self.ensure_error_state(_state);
        if let Some(err) = self.current_error() {
            return Err(err);
        }
        self.init_profile_if_needed();
        self.finishing.store(true, Ordering::Release);

        debug!(
            "DataStreamSink set_finishing: finst={} driver_id={} dest_node_id={} sender_id={} be_number={} destinations={} pending_dests={} pending_chunk_bytes_total={} pending_payload_bytes_total={}",
            format_uuid(
                self.exec_params.fragment_instance_id.hi,
                self.exec_params.fragment_instance_id.lo
            ),
            self.driver_id,
            self.sink.dest_node_id,
            self.sender_id,
            self.be_number,
            self.destinations().len(),
            self.pending_per_dest.len(),
            self.pending_chunk_bytes_total(),
            self.pending_payload_bytes_total()
        );

        self.flush_pending(true, true)?;
        let is_last_driver = self.finish_state.driver_finished();
        debug!(
            "DataStreamSink finishing progressed: finst={} driver_id={} dest_node_id={} sender_id={} last_driver={} (only last driver sends EOS)",
            format_uuid(
                self.exec_params.fragment_instance_id.hi,
                self.exec_params.fragment_instance_id.lo
            ),
            self.driver_id,
            self.sink.dest_node_id,
            self.sender_id,
            is_last_driver
        );
        if !is_last_driver {
            return Ok(());
        }
        self.send_eos()?;
        self.maybe_mark_finished();
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(&self.send_observable))
    }
}

struct DataStreamSinkFinishState {
    remaining_drivers: AtomicUsize,
    force_eos_sent: AtomicBool,
}

impl Default for DataStreamSinkFinishState {
    fn default() -> Self {
        Self {
            remaining_drivers: AtomicUsize::new(0),
            force_eos_sent: AtomicBool::new(false),
        }
    }
}

impl DataStreamSinkFinishState {
    fn register_driver(&self) {
        self.remaining_drivers.fetch_add(1, Ordering::SeqCst);
    }

    fn driver_finished(&self) -> bool {
        let mut current = self.remaining_drivers.load(Ordering::SeqCst);
        loop {
            if current == 0 {
                // Should not happen; allow only one EOS to avoid hangs.
                return !self.force_eos_sent.swap(true, Ordering::SeqCst);
            }
            match self.remaining_drivers.compare_exchange(
                current,
                current - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return current == 1,
                Err(next) => current = next,
            }
        }
    }
}

fn project_chunk_by_slot_ids(chunk: &Chunk, slot_ids: &[SlotId]) -> Result<Chunk, String> {
    if slot_ids.is_empty() || chunk.is_empty() {
        return Ok(chunk.clone());
    }

    let schema = chunk.schema();
    let slot_map = chunk.slot_id_to_index();

    let mut fields = Vec::with_capacity(slot_ids.len());
    let mut cols = Vec::with_capacity(slot_ids.len());

    for slot_id in slot_ids {
        let idx = slot_map.get(slot_id).copied().ok_or_else(|| {
            format!(
                "output_columns slot id {} not found in chunk schema (slot_ids={:?})",
                slot_id,
                slot_map.keys().collect::<Vec<_>>()
            )
        })?;
        let field = schema
            .fields()
            .get(idx)
            .ok_or_else(|| format!("slot id {} mapped to invalid index {}", slot_id, idx))?
            .as_ref()
            .clone();
        let col =
            chunk.columns().get(idx).cloned().ok_or_else(|| {
                format!("slot id {} mapped to invalid column index {}", slot_id, idx)
            })?;
        fields.push(field);
        cols.push(col);
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let batch = arrow::record_batch::RecordBatch::try_new(new_schema, cols)
        .map_err(|e| format!("failed to build projected RecordBatch: {e}"))?;
    Chunk::try_new(batch)
}
